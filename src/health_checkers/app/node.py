from __future__ import annotations

import logging
import threading
import time
from typing import Optional, List

from .models import Config, Peer, Message, ControllerTarget
from .election import Election
from .dood import DockerReviver
from .socket_manager import SocketManager, SocketConfig


class Node:
    """
    Logic for ring node communication and controller health-checking
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
        
        # Socket for leader election
        self._election_socket = SocketManager(
            config=SocketConfig(
                host=cfg.listen_host,
                port=cfg.listen_port,
                timeout_s=1.0
            ),
            name="election"
        )
        
        # Socket for controller health-checking
        self._health_socket = SocketManager(
            config=SocketConfig(
                host=cfg.listen_host,
                port=cfg.health_listen_port,
                timeout_s=0.5
            ),
            name="health"
        )
        
        # Peer and ring topology setup
        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda p: p.id)
        self._successor_index = self._get_successor_index()
        
        # Election
        self.election = Election(cfg, self._send_to_successor_with_retry)
        
        # Docker Reviver
        self._reviver = DockerReviver(cfg.docker_host)
        
        # Threading control
        self._running = False
        self._lock = threading.Lock()
        
        # Election Thread
        self._election_recv_thread: Optional[threading.Thread] = None
        
        # Health-Checking Threads
        self._health_recv_thread: Optional[threading.Thread] = None
        self._leader_health_thread: Optional[threading.Thread] = None
        self._follower_health_thread: Optional[threading.Thread] = None
        
        # State for follower health-checking thread
        self._leader_check_failures = 0
        self._max_leader_check_failures = 3
        self._leader_check_lock = threading.Lock()
        
        logging.info(f"action: Node startup | result: success | node_id: {cfg.node_id}")

    # =========================================================================
    #                            PUBLIC METHODS
    # =========================================================================
    
    def start(self) -> None:
        with self._lock:
            if self._running:
                logging.warning(f"action: start_node | result: skipped | reason: already_running")
                return
            self._running = True
        
        self._election_recv_thread = threading.Thread(
            target=self._election_recv_loop,
            name=f"Node-ElectionRecv-{self.cfg.node_id}",
            daemon=True
        )
        self._election_recv_thread.start()
        
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
            (self._election_recv_thread, "ElectionRecv"),
            (self._health_recv_thread, "HealthRecv"),
            (self._leader_health_thread, "LeaderHealth"),
            (self._follower_health_thread, "FollowerHealth")
        ]
        
        for thread, name in threads:
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=2.0)
                    if thread.is_alive():
                        logging.warning(f"action: stop_thread | result: timeout | thread: {name}")
                except Exception as e:
                    logging.error(f"action: stop_thread | result: fail | thread: {name} | error: {e}")
        
        self._election_socket.close()
        self._health_socket.close()
        
        try:
            self._reviver.close()
        except Exception as e:
            logging.error(f"action: close_reviver | result: fail | error: {e}")
        
        logging.info(f"action: stopping_node | result: success")
    
    def run(self) -> None:
        self.start()
        
        if self._election_recv_thread:
            try:
                self._election_recv_thread.join()
            except KeyboardInterrupt:
                pass
    
    def is_leader(self) -> bool:
        return self.election.leader_id == self.cfg.node_id
    
    def get_leader_info(self) -> Optional[tuple[str, int]]:
        leader_id = self.election.leader_id
        if leader_id is None:
            return None
        
        if leader_id == self.cfg.node_id:
            return (self.cfg.listen_host, self.cfg.health_listen_port)
        
        for target in self.cfg.controller_targets:
            try:
                if "hc" not in target.name:
                    continue
                target_id_str = target.name.replace("hc_", "")
                if target_id_str.isdigit():
                    target_id = int(target_id_str)
                    if target_id == leader_id:
                        return (target.host, target.port)
            except (ValueError, AttributeError) as e:
                logging.warning(
                    f"action: parse_target_id | result: fail | "
                    f"target: {target.name} | error: {e}"
                )
                continue
        
        logging.warning(
            f"action: get_leader_info | result: fail | "
            f"leader_id: {leader_id} | reason: not_found_in_targets"
        )
        return None
    
    def notify_leadership_to(self, target_host: str, target_health_port: int) -> None:
        if not self.is_leader():
            return
        
        leader_id = self.election.leader_id
        if leader_id is None:
            return
        
        msg = Message(
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
        
        success = self._election_socket.send_message(msg, (target_host, election_port))
        if success:
            logging.info(
                f"action: notifying_leader | result: success | "
                f"sent_to: {target_host}:{election_port}"
            )
        else:
            logging.error(
                f"action: notifying_leader | result: fail | "
                f"sent_to: {target_host}:{election_port}"
            )
    
    # =========================================================================
    #                        RING-COMMUNICATION METHODS
    # =========================================================================
    
    def successor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        if self._successor_index >= len(self._peers):
            self._successor_index = 0
        return self._peers[self._successor_index]
    
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
        old_count = len(self._peers)
        self._peers = [p for p in self._peers if p.id != peer_id]
        
        if len(self._peers) == old_count:
            return
        
        self._peers.sort(key=lambda p: p.id)
        
        if self._peers:
            self._successor_index = self._get_successor_index()
            if self._successor_index >= len(self._peers):
                self._successor_index = 0
        else:
            self._successor_index = 0
        
        logging.info(
            f"action: remove_peer | result: success | peer_id: {peer_id} | "
            f"remaining_peers: {len(self._peers)}"
        )
    
    def _get_election_port_for_host(self, target_host: str) -> int:
        for peer in self._peers:
            if peer.host == target_host:
                return peer.port
        return self.cfg.listen_port
    
    def _send_to_successor_with_retry(self, msg: Message) -> bool:
        with self._lock:
            if not self._peers:
                logging.info(f"action: send_message | result: fail | reason: no_peers_available")
                return False
            
            max_attempts = len(self._peers)
        
        attempts = 0
        
        while attempts < max_attempts:
            with self._lock:
                if not self._peers:
                    logging.info(f"action: send_message | result: fail | reason: all_peers_removed")
                    return False
                
                suc = self.successor()
            
            if suc is None:
                logging.info(f"action: send_message | result: fail | reason: no_successor_available")
                return False
            
            success = self._election_socket.send_message(msg, (suc.host, suc.port))
            
            if success:
                logging.info(f"action: send_message | result: success | successor: {suc.name}")
                return True
            else:
                logging.error(
                    f"action: send_message | result: fail | successor: {suc.name} | "
                    f"attempt: {attempts + 1}/{max_attempts}"
                )
                with self._lock:
                    self._remove_peer(suc.id)
                    attempts += 1
        
        logging.info(f"action: send_message | result: fail | reason: all_peers_exhausted")
        return False
    
    def _election_recv_loop(self) -> None:
        logging.info(f"action: election_recv_loop startup | result: success")
        
        try:
            while self._running:
                result = self._election_socket.receive_message()
                
                if result is None:
                    continue
                
                msg, addr = result
                
                try:
                    self._handle_election_message(msg)
                except Exception as e:
                    logging.error(f"action: election_recv | result: fail | error: {e}")
        finally:
            self._election_socket.close()
    
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
    #                         HEALTH-CHECKING METHODS
    # =========================================================================
    
    def _health_recv_loop(self) -> None:
        logging.info(f"action: health_recv_loop startup | result: success")
        
        while self._running:
            result = self._health_socket.receive_message()
            
            if result is None:
                continue
            
            msg, addr = result
            
            try:
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
            success = self._health_socket.send_message(ack, addr)
            if success:
                logging.info(f"action: heartbeat_ack | result: success | address: {addr}")
            else:
                logging.error(f"action: heartbeat_ack | result: fail | address: {addr}")
        
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
                success = self._health_socket.send_message(ack, addr)
                if success:
                    logging.info(f"action: leader_alive_ack | result: success | address: {addr}")
                else:
                    logging.error(f"action: leader_alive_ack | result: fail | address: {addr}")
        
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
                        f"action: revive_controller | status: in_progress | "
                        f"controller_down: {target.name}"
                    )
                    revived = self._revive_controller(target)
                    
                    if revived:
                        logging.info(
                            f"action: revive_controller | result: success | "
                            f"controller_revived: {target.name}"
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
        
        success = self._health_socket.send_with_ack(
            msg=msg,
            target=(target.host, target.port),
            expected_ack_kind="heartbeat_ack",
            timeout_s=timeout_s,
            max_retries=max_retries,
            retry_delay_s=1.0
        )
        
        if success:
            logging.info(
                f"action: heartbeat_completed | result: success | "
                f"controller_name: {target.name}"
            )
        else:
            logging.warning(
                f"action: heartbeat_completed | result: fail | "
                f"controller_name: {target.name}"
            )
        
        return success
    
    def _revive_controller(self, target: ControllerTarget) -> bool:
        success = self._reviver.revive_container(target.container_name)
        
        if success:
            return True
        else:
            logging.info(
                f"action: revive_controller | result: fail | "
                f"controller_down: {target.container_name}"
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
        
        logging.info(f"action: sending is_leader_alive | status: in progress")
        
        success = self._health_socket.send_with_ack(
            msg=msg,
            target=(host, port),
            expected_ack_kind="leader_alive_ack",
            timeout_s=timeout_s,
            max_retries=1,
            retry_delay_s=0.0
        )
        
        if success:
            logging.info(
                f"action: leader_check | result: success | leader_status: alive"
            )
            with self._leader_check_lock:
                self._leader_check_failures = 0
        else:
            with self._leader_check_lock:
                self._leader_check_failures += 1
                failures = self._leader_check_failures
            
            logging.info(
                f"action: leader_check | result: fail | "
                f"attempt: {failures}/{self._max_leader_check_failures}"
            )
            
            if failures >= self._max_leader_check_failures:
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                logging.info(
                    f"action: leader_check | result: leader_dead | "
                    f"starting_election"
                )
                self._on_leader_dead()
    
    def _on_leader_dead(self) -> None:
        if self.election is not None:
            self.election.set_leader(None)
        try:
            self.election.start_election()
        except Exception as e:
            logging.error(f"action: start_election | result: fail | error: {e}")
