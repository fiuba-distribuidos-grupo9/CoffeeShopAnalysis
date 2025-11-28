from __future__ import annotations
import logging
import socket
import threading
import time
from typing import Optional, List, Callable

from .models import Config, Peer, Message, ControllerTarget
from .election import Election
from .dood import DockerReviver

class Node:
    """
    Handles ring communication and controller health-checking
    """

    def __init__(self, cfg : Config):
        self.cfg = cfg

        # Socket for ring messages
        self._election_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._election_socket.bind((cfg.listen_host, cfg.listen_port))
        self._election_socket.settimeout(1.0)

        # Socket for health-checking
        self._health_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._health_socket.bind((cfg.listen_host, cfg.health_listen_port))
        self._health_socket.settimeout(1.0)

        # Peers
        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda p: p.id)
        self._succesor_index = self._get_successor_index()

        # Election
        self.election = Election(cfg, self._send_to_successor_with_retry)

        # Docker reviver 
        self.reviver = DockerReviver(cfg.docker_host)

        self._running = False
        self._lock = threading.Lock()

        # Election Thread
        self._election_thread: Optional[threading.Thread] = None

        # Health-Checking Threads
        self._health_recv_thread: Optional[threading.Thread] = None
        self._health_leader_thread: Optional[threading.Thread] = None
        self._health_follower_thread: Optional[threading.Thread] = None

        # Follower thread state
        self._leader_check_failures = 0
        self._max_leader_check_failures = 3
        self._leader_check_lock = threading.Lock()

        logging.info(f"action: Node startup | result: success | node_id: {cfg.node_id}")

    # =========================================================================
    #                           PUBLIC METHODS
    # =========================================================================

    def start(self) -> None:
        with self._lock:
            if self._running:
                logging.warning(f"action: start_node | result: skipped | reason: node_already_started")
                return
            self._running = True

        self._election_thread = threading.Thread(
            target=self._election_recv_loop,
            name=f"Node-ElectionRecv-{self.cfg.node_id}",
            daemon=True
        )
        self._election_thread.start()

        self._health_recv_thread = threading.Thread(
            target=self._health_recv_loop,
            name=f"Node-HealthRecv-{self.cfg.node_id}",
            daemon=True
        )
        self._health_recv_thread.start()

        self._leader_health_thread = threading.Thread(
            target=self._leader_health_loop,
            name=f"Node-LeaderHealth-{self.cfg.node_id}",
            daemon=True
        )
        self._leader_health_thread.start()
        
        self._follower_health_thread = threading.Thread(
            target=self._follower_health_loop,
            name=f"Node-FollowerHealth-{self.cfg.node_id}",
            daemon=True
        )
        self._follower_health_thread.start()
        
        logging.info(
            f"action: start_node | result: success | "
            f"election_port: {self.cfg.listen_port} | "
            f"health_port: {self.cfg.health_listen_port}"
        )

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
        logging.info(f"action: stopping_node | status: in_progress")

        threads = [
            (self._election_thread, "Node-ElectionRecv"),
            (self._health_recv_thread, "Node-HealthRecv"),
            (self._leader_health_thread, "Node-LeaderHealth"),
            (self._follower_health_thread, "Node-FollowerHealth")
        ]
        
        for thread, name in threads:
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=2.0)
                    if thread.is_alive():
                        logging.warning(f"action: stop_thread | result: timeout | thread: {name}")
                except Exception as e:
                    logging.error(f"action: stop_thread | result: fail | thread: {name} | error: {e}")

        self._close_socket(self._election_socket, "election")
        self._close_socket(self._health_socket, "health")

        try:
            self.reviver.close()
        except Exception as e:
            logging.error(f"action: close_reviver | result: fail | error: {e}")

        logging.info(f"action: stopping_node | result: success")

    def run(self) -> None:
        self.start()

        if self._election_thread:
            try:
                self._election_thread.join()
            except KeyboardInterrupt:
                pass
    
    def is_leader(self) -> bool:
        return self.election.leader_id == self.cfg.node_id
    
    def get_leader_info(self) -> Optional[tuple[str,str]]:
        """
        Returns current leader's (host, health_port)
        """
        with self._lock:
            leader_id = self.election.leader_id
            if leader_id is None:
                return None

            if leader_id == self.cfg.node_id:
                return (self.cfg.listen_host, self.cfg.health_listen_port)
            
            for target in self.cfg.controller_targets:
                if target.name == f"hc_container_{leader_id}":
                    return (target.host, target.port)
                
            return None
        
    def notify_leadership_to(self, target_host: str, target_health_port: int) -> None:
        if not self.is_leader():
            return

        leader_id = self.election.leader_id
        if leader_id is None:
            return 
        
        msg = Message (
            kind="coordinator",
            src_id=self.cfg.node_id,
            src_name=self.cfg.node_name,
            payload={
                "leader_id": leader_id,
                "initiator_id": leader_id
            },
            notifying_revived=True
        )

        election_port = self._get_election_port_for_host(target_host)

        try:
            self._election_socket.sendto(msg.to_json().encode("utf-8"),(target_host, election_port))
            logging.info(f"action: notifyin_leader | result: success | sent_to:{target_host}:{election_port}")
        except Exception as e:
            logging.error(f"action: notifying_leader | result: fail | error: {e}")

        
    # =========================================================================
    #                               RING-METHODS
    # =========================================================================

    def successor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        if self._succesor_index >= len(self._peers):
            self._succesor_index = 0
        return self._peers[self._succesor_index]
    
    def _get_successor_index(self) -> int:
        if not self._peers:
            return 0
        for i, p in enumerate(self._peers):
            if p.id > self.cfg.node_id:
                return i
        return 0

    def _peer_by_id(self, pid: int) -> Optional[Peer]:
        with self._lock:
            for p in self._peers:
                if p.id == pid:
                    return p
            return None
    
    def _remove_peer(self, peer_id: int) -> None:
        self._peers = [p for p in self._peers if p.id != peer_id]
        self._peers.sort(key=lambda p: p.id)
        if self._peers:
            self._successor_index = self._get_successor_index()
        else:
            self._successor_index = 0
        logging.info(f"action: remove_peer | result: success | peer_id: {peer_id}")

    def _get_election_port_for_host(self, target_host: str) -> int:
        for peer in self._peers:
            if peer.host == target_host:
                return peer.port
        return self.cfg.listen_port
    
    def _send_to(self, peer: Peer, msg: Message) -> None:
        data = msg.to_json().encode("utf-8")
        self._election_socket.sendto(data, (peer.host, peer.port))

    def _send_to_successor_with_retry(self, msg: Message) -> bool:
        if not self._peers:
            logging.info(f"action: send_message | result: fail | reason: no_peers_available")
            return False
        
        attempts = 0
        max_attempts = len(self._peers)
        
        while attempts < max_attempts:
            with self._lock:
                suc = self.successor()
            
            if suc is None:
                logging.info(f"action: send_message | result: fail | reason: no_successor_available")
                return False
            
            try:
                self._send_to(suc, msg)
                logging.info(f"action: send_message | result: success | successor: {suc.name}")
                return True
            except Exception as e:
                logging.error(
                    f"action: send_message | result: fail | error: {e} | attempt: ({attempts}/{max_attempts})"
                )
                with self._lock:
                    self._remove_peer(suc.id)
                    attempts += 1
        
        return False
    
    def _election_recv_loop(self) -> None:
        logging.info(f"action: election_recv_loop startup | result: success")
        
        try:
            while self._running:
                try:
                    if not self._sock_valid(self._health_socket):
                        return
                    data, addr = self._election_socket.recvfrom(64 * 1024)
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self._running:
                        break
                    logging.error(f"action: election_recv | result: fail | error: {e}")
                    break
                
                try:
                    msg = Message.from_json(data.decode("utf-8"))
                    self._handle_election_message(msg)
                except Exception as e:
                    logging.error(f"action: election_recv | result: fail | error: {e}")
        finally:
            self._close_socket(self._election_socket, "election")
    
    def _handle_election_message(self, msg: Message) -> None:
        kind = msg.kind
        if kind == "election":
            self.election.handle_election(msg)
        
        elif kind == "coordinator":
            self.election.handle_coordinator(msg)
            logging.info(
                f"action: new_leader_elected | result: success | "
                f"new_leader: {self.election.leader_id}"
            )

    # =========================================================================
    #                          HEALTH-CHECKING METHODS
    # =========================================================================

    def _health_recv_loop(self) -> None:
        logging.info(f"action: health_recv_loop startup | result: success")
        
        while self._running:
            try:
                if not self._sock_valid(self._health_socket):
                    return
                data, addr = self._health_socket.recvfrom(64 * 1024)
            except socket.timeout:
                continue
            except OSError:
                if not self._running:
                    break
                continue
            
            try:
                msg = Message.from_json(data.decode("utf-8"))
                self._handle_health_message(msg, addr)
            except Exception as e:
                logging.error(f"action: health_recv | result: fail | error: {e}")
    
    def _handle_health_message(self, msg: Message, addr: tuple) -> None:
        kind = msg.kind
        
        if kind == "heartbeat":
            ack = Message(
                kind="heartbeat_ack",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={},
            )
            try:
                if not self._sock_valid(self._health_socket):
                    return
                self._health_socket.sendto(ack.to_json().encode("utf-8"), addr)
                logging.info(f"action: heartbeat_ack | result: success | address: {addr}")
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return
            except Exception as e:
                logging.error(f"action: heartbeat_ack | result: fail | error: {e}")
        
        elif kind == "heartbeat_ack":
            pass  
        elif kind == "is_leader_alive":
            if self.is_leader():
                ack = Message(
                    kind="leader_alive_ack",
                    src_id=self.cfg.node_id,
                    src_name=self.cfg.node_name,
                    payload={},
                )
                try:
                    self._health_socket.sendto(ack.to_json().encode("utf-8"), addr)
                    logging.info(f"action: leader_alive_ack | result: success | address: {addr}")
                except Exception as e:
                    logging.error(f"action: leader_alive_ack | result: fail | error: {e}")
        
        elif kind == "leader_alive_ack":
            pass  
    
    def _leader_health_loop(self) -> None:
        logging.info(f"action: leader_health_loop startup | result: success")
        interval_s = self.cfg.heartbeat_interval_ms / 1000.0
        
        while self._running:
            if not self.is_leader():
                time.sleep(1.0)
                continue
            
            logging.info(f"action: send_heartbeat | result: success")
            
            for target in self.cfg.controller_targets:
                if not self._running:
                    break
                
                success = self._send_heartbeat_with_retry(target)
                
                if not success:
                    logging.info(
                        f"action: revive_controller | status: in_progress | controller_down: {target.name}"
                    )
                    revived = self._revive_controller(target)
                    
                    if revived:
                        logging.info(
                            f"action: revive_controller | result: success | controller_revived: {target.name}"
                        )
                        time.sleep(2.0)
                        self.notify_leadership_to(target.host, target.port)
            
            time.sleep(interval_s)
    
    def _send_heartbeat_with_retry(self, target: ControllerTarget) -> bool:
        timeout_s = self.cfg.heartbeat_timeout_ms / 1000.0
        max_retries = self.cfg.heartbeat_max_retries
        
        msg = Message(
            kind="heartbeat",
            src_id=self.cfg.node_id,
            src_name=self.cfg.node_name,
            payload={},
        )
        
        for attempt in range(1, max_retries + 1):
            if not self._running:
                return False
            
            if not self._sock_valid(self._health_socket):
                return False
            
            try:
                self._health_socket.sendto(
                    msg.to_json().encode("utf-8"),
                    (target.host, target.port)
                )
                
                try:
                    self._health_socket.settimeout(timeout_s)
                except OSError as e:
                    if getattr(e, "errno", None) == 9:
                        return False
                
                start = time.monotonic()
                
                while time.monotonic() - start < timeout_s:
                    try:
                        data, addr = self._health_socket.recvfrom(64 * 1024)
                        ack_msg = Message.from_json(data.decode("utf-8"))
                        
                        if ack_msg.kind == "heartbeat_ack":
                            logging.info(
                                f"action: heartbeat_ack_received | result: success | "
                                f"controller_name: {target.name} | attempt: {attempt}"
                            )
                            try:
                                self._health_socket.settimeout(0.5)
                            except Exception:
                                pass
                            return True
                    except socket.timeout:
                        break
                    except Exception:
                        continue
                
                logging.info(
                    f"action: heartbeat_ack_received | result: fail | controller_name: {target.name} | retrying: ({attempt}/{max_retries})"
                )
            
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return False
            except Exception as e:
                logging.error(
                    f"action: sending_heartbeat | result: fail | controller_name: {target.name} | error: {e}"
                )
            
            if attempt < max_retries:
                time.sleep(1.0)
        
        try:
            if self._sock_valid(self._health_socket):
                self._health_socket.settimeout(0.5)
        except Exception:
            pass
        
        return False
    
    def _revive_controller(self, target: ControllerTarget) -> bool:
        success = self.reviver.revive_container(target.container_name)
        
        if success:
            return True
        else:
            logging.info(
                f"action: revive_controller | result: fail | controller_down: {target.container_name}"
            )
            return False
    
    def _follower_health_loop(self) -> None:
        logging.info(f"action: follower_health_loop startup | result: success")
        interval_s = self.cfg.leader_check_interval_ms / 1000.0
        
        while self._running:
            if self.is_leader():
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                time.sleep(1.0)
                continue
            
            time.sleep(interval_s)
            
            if not self.is_leader():
                self._check_leader_alive()
    
    def _check_leader_alive(self) -> None:
        logging.info(f"action: leader_check | status: in progress")
        leader_info = self.get_leader_info()
        
        if leader_info is None:
            logging.info(f"action: leader_check | status: completed | result: no leader found")
            with self._leader_check_lock:
                self._leader_check_failures = 0
            return
        
        host, port = leader_info
        timeout_s = self.cfg.leader_check_timeout_ms / 1000.0
        
        msg = Message(
            kind="is_leader_alive",
            src_id=self.cfg.node_id,
            src_name=self.cfg.node_name,
            payload={},
        )
        
        if not self._sock_valid(self._health_socket):
            return
        
        logging.info(f"action: sending is_leader_alive | status: in progress")
        
        try:
            try:
                self._health_socket.sendto(msg.to_json().encode("utf-8"), (host, port))
            except OSError as e:
                logging.error(f"action: sending is_leader_alive | result: fail | error: {e}")
                if getattr(e, "errno", None) == 9:
                    return
                raise
            
            try:
                self._health_socket.settimeout(timeout_s)
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return
                raise
            
            logging.info(f"action: sending is_leader_alive | result: success")
            
            start = time.monotonic()
            received_ack = False
            
            logging.info(f"action: receiving ack from leader | result: in progress")
            
            while time.monotonic() - start < timeout_s:
                try:
                    data, addr = self._health_socket.recvfrom(64 * 1024)
                    ack_msg = Message.from_json(data.decode("utf-8"))
                    
                    if ack_msg.kind == "leader_alive_ack":
                        logging.info(
                            f"action: receiving ack from leader | result: success | leader_status: alive"
                        )
                        self._health_socket.settimeout(0.5)
                        received_ack = True
                        with self._leader_check_lock:
                            self._leader_check_failures = 0
                        return
                except socket.timeout:
                    break
                except Exception:
                    continue
            
            if not received_ack:
                with self._leader_check_lock:
                    self._leader_check_failures += 1
                    failures = self._leader_check_failures
                
                logging.info(
                    f"action: receiving ack from leader | result: fail | attempt: {failures}/{self._max_leader_check_failures}"
                )
                
                if failures >= self._max_leader_check_failures:
                    self._health_socket.settimeout(0.5)
                    with self._leader_check_lock:
                        self._leader_check_failures = 0
                    self._on_leader_dead()
                else:
                    self._health_socket.settimeout(0.5)
        
        except OSError as e:
            with self._leader_check_lock:
                self._leader_check_failures += 1
                failures = self._leader_check_failures
            
            logging.info(
                f"action: receiving ack from leader | result: fail | error: {e} | attempt: ({failures}/{self._max_leader_check_failures})"
            )
            
            if failures >= self._max_leader_check_failures:
                self._health_socket.settimeout(0.5)
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                self._on_leader_dead()
            else:
                self._health_socket.settimeout(0.5)
        
        except Exception as e:
            logging.error(f"action: receiving ack from leader | result: fail | error: {e}")
            self._health_socket.settimeout(0.5)
    
    def _on_leader_dead(self) -> None:
        if self.election is not None:
            self.election.set_leader(None)
        try:
            self.election.start_election()
        except Exception as e:
            logging.error(f"action: start_election | result: fail | error: {e}")
    
    # =========================================================================
    #                           HELPER METHODS
    # =========================================================================
    
    def _sock_valid(self, sock: socket.socket) -> bool:
        try:
            if sock is None:
                return False
            if getattr(sock, "_closed", False):
                return False
            if sock.fileno() == -1:
                return False
            return True
        except Exception:
            return False
    
    def _close_socket(self, sock: socket.socket, name: str) -> None:
        try:
            if self._sock_valid(sock):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
                logging.info(f"action: close_socket | result: success | socket: {name}")
        except Exception as e:
            try:
                sock.close()
            except Exception:
                pass
            logging.error(f"action: close_socket | result: fail | socket: {name} | error: {e}")