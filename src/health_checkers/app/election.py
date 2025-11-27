from __future__ import annotations
import logging
import threading
import time
from typing import Optional, Callable, Dict
from .models import Message, Config
from .utils import jitter_ms


class Election:
    def __init__(self, cfg: Config, send_to_successor_with_retry: Callable[[Message], bool]):
        self.cfg = cfg
        self.send_to_successor = send_to_successor_with_retry
        self._leader_id: Optional[int] = None
        self._running = False
        self._lock = threading.Lock()
        
        self._active_elections: Dict[int, int] = {}

    @property
    def leader_id(self) -> Optional[int]:
        return self._leader_id

    def set_leader(self, leader_id: Optional[int]) -> None:
        with self._lock:
            self._leader_id = leader_id
            if leader_id is None:
                self._active_elections.clear()
            logging.info(f"action: set_leader | result: success | new_leader: {leader_id}")

    def start_election(self) -> None:
        """Inicia una elección si no hay otra en curso."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._active_elections.clear()
        
        logging.info(f"action: start_election | status: in progress | initiator_HC: {self.cfg.node_id}")
        
        try:
            msg = Message(
                kind="election",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={
                    "candidate_id": self.cfg.node_id,
                    "initiator_id": self.cfg.node_id
                },
            )

            time.sleep(jitter_ms(self.cfg.election_backoff_ms_min, self.cfg.election_backoff_ms_max))
            success = self.send_to_successor(msg)
            
            if success:
                logging.info(f"action: election_message_sent_to_successor | result: success | current_HC: {self.cfg.node_id}")
            else:
                logging.info(f"action: start_election | result: success | new_leader: {self.cfg.node_id} (No HC left)")
                with self._lock:
                    self._leader_id = self.cfg.node_id
        finally:
            with self._lock:
                self._running = False

    def handle_election(self, msg: Message) -> None:
        candidate_id = msg.payload.get("candidate_id")
        initiator_id = msg.payload.get("initiator_id", candidate_id)
        
        if candidate_id is None:
            return

        node_id = self.cfg.node_id
        
        if initiator_id == node_id and candidate_id == node_id:
            logging.info(f"action: election | result: success | new_leader: {node_id}")
            with self._lock:
                self._leader_id = node_id
                self._active_elections.clear()
            
            announce = Message(
                kind="coordinator",
                src_id=node_id,
                src_name=self.cfg.node_name,
                payload={
                    "leader_id": node_id,
                    "initiator_id": node_id
                },
            )
            self.send_to_successor(announce)
            return

        with self._lock:
            if initiator_id in self._active_elections:
                prev_candidate = self._active_elections[initiator_id]
                if candidate_id <= prev_candidate:
                    return
            self._active_elections[initiator_id] = candidate_id

        logging.info(f"action: election_message_received | result: success | candidate: {candidate_id}")

        if candidate_id > node_id:
            self.send_to_successor(msg)
        else:
            logging.info(f"action: better_candidate | result: success | new_candidate_ID: {node_id}")
            new_msg = Message(
                kind="election",
                src_id=node_id,
                src_name=self.cfg.node_name,
                payload={
                    "candidate_id": node_id,
                    "initiator_id": initiator_id
                },
            )
            self.send_to_successor(new_msg)

    def handle_coordinator(self, msg: Message) -> None:
        leader_id = msg.payload.get("leader_id")
        initiator_id = msg.payload.get("initiator_id", leader_id)
        is_notifying_revived = msg.notifying_revived
        
        if leader_id is None:
            return
        
        node_id = self.cfg.node_id
        
        logging.info(f"action: coordinator_message_received | result: success | leader_id_received: {leader_id}")
        
        if leader_id == node_id:
            logging.info(f"action: election_completed | result: success | new_leader: {leader_id}")
            with self._lock:
                self._leader_id = leader_id
                self._active_elections.clear()
            return
        
        if initiator_id == node_id:
            logging.info(f"action: coordinator_message_finished_ring | result: success | new_leader: {leader_id}")
            with self._lock:
                self._leader_id = leader_id
                self._active_elections.clear()
            return
        
        with self._lock:
            self._leader_id = leader_id
            self._active_elections.clear()
        
        if is_notifying_revived:
            logging.info(f"action: notifying_leader_to_revived_HC | result: success")
            return
        self.send_to_successor(msg)
