# src/health_checkers/app/heartbeat.py
from __future__ import annotations
import asyncio, time
from typing import Optional, Callable
from .models import Config, Message

class Heartbeat:
    def __init__(
        self,
        cfg: Config,
        send_to_successor,
        on_successor_suspect: Callable[[], None],
        is_leader: Callable[[], bool],
    ):
        self.cfg = cfg
        self.send_to_successor = send_to_successor
        self.on_successor_suspect = on_successor_suspect
        self.is_leader = is_leader

        self._last_from_pred_ts: float = time.monotonic()
        self._monitor_task: Optional[asyncio.Task] = None
        self._running = asyncio.Event()

    def note_heartbeat_from_pred(self):
        self._last_from_pred_ts = time.monotonic()

    async def start(self):
        self._running.set()
        self._monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop(self):
        self._running.clear()
        if self._monitor_task:
            self._monitor_task.cancel()
            with contextlib.suppress(Exception):
                await self._monitor_task

    async def _monitor_loop(self):
        while self._running.is_set():
            # We sent heartbeat to successor.
            msg = Message(kind="heartbeat", src_id=self.cfg.node_id, src_name=self.cfg.node_name)
            await self.send_to_successor(msg)

            # We check if we stop receiving heartbeats from the predecessor.
            now = time.monotonic()
            silence = (now - self._last_from_pred_ts) * 1000.0
            if silence > self.cfg.heartbeat_timeout_ms:
                # We are warning of suspicion regarding the successor (possibly a broken ring).

                self.on_successor_suspect()

            await asyncio.sleep(self.cfg.heartbeat_interval_ms / 1000.0)
