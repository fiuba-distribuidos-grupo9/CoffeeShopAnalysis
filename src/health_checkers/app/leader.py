# src/health_checkers/app/leader.py
from __future__ import annotations
import asyncio
from .models import Config
from .utils import jitter_ms

class LeaderLoop:
    def __init__(self, cfg: Config, is_leader_callable):
        self.cfg = cfg
        self.is_leader = is_leader_callable
        self._task = None
        self._running = False

    async def start(self):
        if self._task:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self):
        while self._running:
            if self.is_leader():
                # Placeholder: The leader would "make global pings".
                # For now, it only sleeps randomly to simulate work.
                sleep_s = jitter_ms(self.cfg.leader_sleep_min_ms, self.cfg.leader_sleep_max_ms)
                print(f"[leader] (placeholder) dormitando {sleep_s:.2f}s antes del siguiente barrido…")
                await asyncio.sleep(sleep_s)
                # TODO: Implement global ping to everyone and revive whatever is necessary.
            else:
                await asyncio.sleep(0.5)
