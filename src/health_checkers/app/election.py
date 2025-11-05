# src/health_checkers/app/election.py
from __future__ import annotations
import asyncio, json, random
from typing import Optional, Tuple
from .models import Message, Config
from .utils import jitter_ms

class Election:
    def __init__(self, cfg: Config, send_to_successor):
        self.cfg = cfg
        self.send_to_successor = send_to_successor
        self._leader_id: Optional[int] = None
        self._running = False
        self._lock = asyncio.Lock()

    @property
    def leader_id(self) -> Optional[int]:
        return self._leader_id

    def set_leader(self, lid: Optional[int]):
        self._leader_id = lid

    async def start_election(self):
        async with self._lock:
            if self._running:
                return
            self._running = True

        try:
            # In Chang-Roberts, each node starts by proposing its own ID.
            msg = Message(
                kind="election",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={"candidate_id": self.cfg.node_id}
            )

            # Random backoff to avoid storms.
            await asyncio.sleep(jitter_ms(self.cfg.election_backoff_ms_min, self.cfg.election_backoff_ms_max))
            await self.send_to_successor(msg)
        finally:
            async with self._lock:
                self._running = False

    async def handle_election(self, msg: Message):
        """
        If candidate_id > self.id → Resend.
        If candidate_id < self.id → Replaced it with my ID and resent it.
        If candidate_id == self.id → I am leader → coordinator announcement.
        """
        cid = msg.payload.get("candidate_id")
        if cid is None:
            return

        my = self.cfg.node_id
        if cid == my:
            # I won the race → I'm the leader.
            self._leader_id = my
            announce = Message(kind="coordinator", src_id=my, src_name=self.cfg.node_name, payload={"leader_id": my})
            await self.send_to_successor(announce)
            return

        if cid > my:
            # Resend intact.
            await self.send_to_successor(msg)
        elif cid < my:
            # I apply.
            new_msg = Message(kind="election", src_id=my, src_name=self.cfg.node_name, payload={"candidate_id": my})
            await self.send_to_successor(new_msg)

    async def handle_coordinator(self, msg: Message):
        lid = msg.payload.get("leader_id")
        if lid is None:
            return
        self._leader_id = lid
        if lid != self.cfg.node_id:
            # Spread.
            await self.send_to_successor(msg)
