# src/health_checkers/app/election.py
from __future__ import annotations
import threading
import time
from typing import Optional
from .models import Message, Config
from .utils import jitter_ms


class Election:
    """
    Implementación síncrona (basada en hilos) del algoritmo de Chang–Roberts.
    """

    def __init__(self, cfg: Config, send_to_successor):
        self.cfg = cfg
        self.send_to_successor = send_to_successor
        self._leader_id: Optional[int] = None
        self._running = False
        self._lock = threading.Lock()

    @property
    def leader_id(self) -> Optional[int]:
        return self._leader_id

    def set_leader(self, lid: Optional[int]) -> None:
        self._leader_id = lid

    def start_election(self) -> None:
        """Inicia una elección si no hay otra en curso."""
        with self._lock:
            if self._running:
                return
            self._running = True
        try:
            msg = Message(
                kind="election",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={"candidate_id": self.cfg.node_id},
            )
            # Backoff aleatorio para evitar tormentas simultáneas.
            time.sleep(jitter_ms(self.cfg.election_backoff_ms_min, self.cfg.election_backoff_ms_max))
            self.send_to_successor(msg)
        finally:
            with self._lock:
                self._running = False

    def handle_election(self, msg: Message) -> None:
        cid = msg.payload.get("candidate_id")
        if cid is None:
            return

        my = self.cfg.node_id

        if cid == my:
            # Gané
            self._leader_id = my
            announce = Message(
                kind="coordinator",
                src_id=my,
                src_name=self.cfg.node_name,
                payload={"leader_id": my},
            )
            self.send_to_successor(announce)
            return

        if cid > my:
            # Reenvío intacto
            self.send_to_successor(msg)
        else:
            # Me postulo yo
            new_msg = Message(
                kind="election",
                src_id=my,
                src_name=self.cfg.node_name,
                payload={"candidate_id": my},
            )
            self.send_to_successor(new_msg)

    def handle_coordinator(self, msg: Message) -> None:
        lid = msg.payload.get("leader_id")
        if lid is None:
            return
        self._leader_id = lid
        if lid != self.cfg.node_id:
            self.send_to_successor(msg)
