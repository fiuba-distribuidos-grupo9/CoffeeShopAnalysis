from __future__ import annotations

import logging
import socket
import threading
from typing import Optional, List

from .models import Config, Peer, Message
from .election import Election


class RingNode:
    def __init__(self, cfg: Config):
        self.cfg = cfg

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((cfg.listen_host, cfg.listen_port))
        self.sock.settimeout(1.0)

        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda p: p.id)
        self._successor_index = self._get_succesor_index()

        self.election = Election(cfg, self._send_to_successor_with_retry)
        self._running: bool = True

        self._lock = threading.Lock()

        logging.info(f"action: RingNode startup | status: success")

    def _get_succesor_index(self) -> int:
        if not self._peers:
            return 0
        for i, p in enumerate(self._peers):
            if p.id > self.cfg.node_id:
                return i
        return 0

    def successor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        return self._peers[self._successor_index]

    def predecessor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        lower = [p for p in self._peers if p.id < self.cfg.node_id]
        return lower[-1] if lower else self._peers[-1]

    def _peer_by_id(self, pid: int) -> Optional[Peer]:
        for p in self._peers:
            if p.id == pid:
                return p
        return None

    def _remove_peer(self, peer_id: int) -> None:
        self._peers = [p for p in self._peers if p.id != peer_id]
        self._peers.sort(key=lambda p: p.id)
        if self._peers:
            self._successor_index = self._get_succesor_index()
        else:
            self._successor_index = 0
        logging.info(f"action: remove_peer | result: success | peer_id: {peer_id}")

    def is_leader(self) -> bool:
        return self.election.leader_id == self.cfg.node_id

    def get_leader_info(self) -> Optional[tuple[str, int]]:
        """Retorna (host, health_port) del líder actual."""
        with self._lock:
            leader_id = self.election.leader_id
            if leader_id is None:
                return None

            if leader_id == self.cfg.node_id:
                return (self.cfg.listen_host, self.cfg.health_listen_port)

            for target in self.cfg.controller_targets:
                if target.name == f"hc-{leader_id}":
                    return (target.host, target.port)

            return None

    def notify_leadership_to(self, target_host: str, target_election_port: int) -> None:
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
        
        try:
            self.sock.sendto(msg.to_json().encode("utf-8"), (target_host, election_port))
        except Exception as e:
            logging.error(f"action: notifying_leader | result: fail | sent_to: {target_host}:{election_port} | error: {e}")
        logging.info(f"action: notifying_leader | result: success | sent_to:{target_host}:{election_port}")

    def _get_election_port_for_host(self, target_host: str) -> int:
        for peer in self._peers:
            if peer.host == target_host:
                return peer.port
        
        return self.cfg.listen_port

    def _send_to(self, peer: Peer, msg: Message) -> None:
        data = msg.to_json().encode("utf-8")
        try:
            self.sock.sendto(data, (peer.host, peer.port))
        except OSError as e:
            raise
        except Exception as e:
            raise

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
                logging.error(f"action: send_message | result: fail | error: {e} | attempt: {attempts}/{max_attempts}")
                with self._lock:
                    self._remove_peer(suc.id)
                    attempts += 1
        
        return False

    def stop(self) -> None:
        if not self._running:
            return
        with self._lock:
            self._running = False
            try:
                self.sock.close()
            except Exception:
                pass

    def run(self) -> None:
        try:
            while self._running:
                try:
                    data, addr = self.sock.recvfrom(64 * 1024)
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self._running:
                        break
                    logging.error(f"action: socket.recv() | result: fail | error: {e}")
                    break

                try:
                    msg = Message.from_json(data.decode("utf-8"))
                except Exception as e:
                    logging.error(f"action: receive_message | result: fail | error: {e}")
                    continue

                self._handle_message(msg)
        finally:
            try:
                self.sock.close()
            except Exception:
                pass

    def _handle_message(self, msg: Message) -> None:
        kind = msg.kind

        if kind == "election":
            self.election.handle_election(msg)

        elif kind == "coordinator":
            self.election.handle_coordinator(msg)
            logging.info(f"action: new_leader_elected | result: success | new_leader: {self.election.leader_id}")
