# src/health_checkers/app/leader.py
from __future__ import annotations
import threading
import time
from typing import Optional
from .models import Config


class LeaderLoop:
    """
    Loop sencillo en un hilo:
      - Sólo ejecuta lógica de validación cuando este nodo es líder.
    """

    def __init__(self, cfg: Config, is_leader_callable):
        self.cfg = cfg
        self.is_leader = is_leader_callable
        self._thread: Optional[threading.Thread] = None
        self._running = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._running.set()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"LeaderLoop-{self.cfg.node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread = None

    def _loop(self) -> None:
        while self._running.is_set():
            if self.is_leader():
                # Acá va la lógica “real” de validación de nodos (por ahora es un print).
                print(f"[leader] Nodo líder {self.cfg.node_id} validando nodos del sistema (placeholder)…")
                time.sleep(5.0)
            else:
                time.sleep(1.0)
