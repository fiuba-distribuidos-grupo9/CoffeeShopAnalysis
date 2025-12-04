# Imports.
from __future__ import annotations
import logging
import threading
import time
from typing import Optional, Callable, Dict, Set
from shared.models import Message, Config
from shared.utils import jitter_ms

# Election class implementing the ring election algorithm.
class Election:
    def __init__(self, cfg: Config, send_to_successor_with_retry: Callable[[Message], bool]):
        self.cfg = cfg
        self.send_to_successor = send_to_successor_with_retry
        self._leader_id: Optional[int] = None
        self._running = False
        self._lock = threading.Lock()
        self._active_elections: Dict[int, int] = {}        
        self._completed_elections: Set[int] = set()

    @property
    def leader_id(self) -> Optional[int]:
        with self._lock:
            return self._leader_id

    def set_leader(self, leader_id: Optional[int]) -> None:
        with self._lock:
            old_leader = self._leader_id
            if old_leader == leader_id:
                return
            self._leader_id = leader_id
            if leader_id is None:
                self._active_elections.clear()
                self._completed_elections.clear()
                logging.info(
                    f"action: set_leader | result: success | new_leader: {leader_id} | "
                    f"old_leader: {old_leader} | state_cleaned: True"
                )
            else:
                logging.info(f"action: set_leader | result: success | new_leader: {leader_id}")


    def start_election(self) -> None:
        with self._lock:
            if self._running:
                logging.info("action: start_election | result: skipped | reason: election_already_running")
                return
            
            if self._leader_id is not None:
                logging.info(f"action: start_election | result: skipped | reason: leader_exists | leader: {self._leader_id}")
                return
            
            self._running = True
        
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
            with self._lock:
                if self._leader_id is not None:
                    logging.info(f"action: start_election | result: cancelled | reason: leader_exists | leader: {self._leader_id}")
                    return
            
            success = self.send_to_successor(msg)
            if success:
                logging.info(f"action: election_message_sent_to_successor | result: success | current_HC: {self.cfg.node_id}")
            else:
                logging.info(f"action: start_election | result: success | new_leader: {self.cfg.node_id} (No HC left)")
                with self._lock:
                    self._leader_id = self.cfg.node_id
                    self._completed_elections.add(self.cfg.node_id)
        except Exception as e:
            logging.error(f"action: start_election | result: fail | error: {e}")
        finally:
            with self._lock:
                self._running = False

    def handle_election(self, msg: Message) -> None:
        candidate_id = msg.payload.get("candidate_id")
        initiator_id = msg.payload.get("initiator_id", candidate_id)
        if candidate_id is None or initiator_id is None:
            return

        node_id = self.cfg.node_id
        if initiator_id == node_id:
            with self._lock:
                if initiator_id in self._completed_elections:
                    logging.info(f"action: election_cycle_completed | result: ignored | reason: already_completed | initiator: {initiator_id}")
                    return
                
                self._completed_elections.add(initiator_id)
                if candidate_id == node_id:
                    self._leader_id = candidate_id
                    logging.info(f"action: election_cycle_completed | result: success | winner: {candidate_id}")
                elif candidate_id > node_id:
                    self._leader_id = candidate_id
                    logging.info(f"action: election_cycle_completed | result: success | winner: {candidate_id}")
                else:
                    logging.error(f"action: election_cycle_completed | result: error | candidate: {candidate_id} < initiator: {node_id} | This should not happen!")
                    self._running = False
                    return
                
                self._active_elections.clear()
                self._running = False
            
            announce = Message(
                kind="coordinator",
                src_id=node_id,
                src_name=self.cfg.node_name,
                payload={
                    "leader_id": self._leader_id,
                    "initiator_id": node_id
                },
            )

            self.send_to_successor(announce)
            return

        with self._lock:
            if self._leader_id is not None:
                logging.info(f"action: election_message_received | result: ignored | reason: leader_exists | leader: {self._leader_id} | candidate: {candidate_id}")
                return
            
            if initiator_id in self._completed_elections:
                logging.info(f"action: election_message_received | result: ignored | reason: election_completed | initiator: {initiator_id}")
                return
            
            if initiator_id in self._active_elections:
                prev_candidate = self._active_elections[initiator_id]
                if candidate_id < prev_candidate:
                    logging.info(f"action: election_message_received | result: ignored | reason: worse_candidate | candidate: {candidate_id} | best: {prev_candidate}")
                    return
                elif candidate_id == prev_candidate:
                    logging.info(f"action: election_message_received | result: ignored | reason: duplicate | candidate: {candidate_id}")
                    return
            
            self._active_elections[initiator_id] = max(candidate_id, self._active_elections.get(initiator_id, 0))

        logging.info(f"action: election_message_received | result: success | candidate: {candidate_id} | initiator: {initiator_id}")
        if candidate_id > node_id:
            logging.info(f"action: forwarding_election | candidate: {candidate_id}")
            self.send_to_successor(msg)
        elif candidate_id == node_id:
            logging.info(f"action: forwarding_election | candidate: {candidate_id}")
            self.send_to_successor(msg)
        else:
            logging.info(f"action: better_candidate | result: success | new_candidate_ID: {node_id} | old_candidate: {candidate_id}")
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
        logging.info(f"action: coordinator_message_received | result: success | leader_id_received: {leader_id} | initiator: {initiator_id}")
        with self._lock:
            if self._leader_id == leader_id:
                logging.info(f"action: coordinator_message_received | result: ignored | reason: already_set | leader: {leader_id}")
                if initiator_id == node_id:
                    return
                if not is_notifying_revived:
                    self.send_to_successor(msg)
                return
            
            self._leader_id = leader_id
            self._active_elections.clear()
            self._running = False
        
        if leader_id == node_id:
            logging.info(f"action: election_completed | result: success | new_leader: {leader_id}")
            return
        
        if initiator_id == node_id:
            logging.info(f"action: coordinator_message_finished_ring | result: success | new_leader: {leader_id}")
            return
        
        if is_notifying_revived:
            logging.info(f"action: notifying_leader_to_revived_HC | result: success | leader: {leader_id}")
            return
        
        self.send_to_successor(msg)
