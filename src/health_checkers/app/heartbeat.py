from __future__ import annotations
import threading
import time
from typing import Callable, Optional
from .models import Config, Message


class HeartbeatLoop:
    """
    Loop de heartbeat basado en hilos:
      - Envía heartbeat al sucesor cada intervalo.
      - Espera ACK.
      - Si pasa el timeout sin ACK → sucesor sospechado caído.
    """

    def __init__(
        self,
        cfg: Config,
        get_successor: Callable[[], Optional["Peer"]],
        send_to_successor: Callable[[Message], None],
        on_successor_suspected: Callable[[int], None],
    ) -> None:
        self.cfg = cfg
        self.get_successor = get_successor
        self.send_to_successor = send_to_successor
        self.on_successor_suspected = on_successor_suspected

        self._running = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._lock = threading.Lock()
        self._last_ack_ts: float = 0.0
        self._ever_ack: bool = False

    def start(self) -> None:
        if self._thread is not None:
            return
        self._running.set()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"Heartbeat-{self.cfg.node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread = None

    def notify_ack(self) -> None:
        """Llamado cuando recibimos un ACK desde el sucesor."""
        now = time.monotonic()
        with self._lock:
            self._last_ack_ts = now
            self._ever_ack = True

    def _loop(self) -> None:
        interval_s = max(self.cfg.heartbeat_interval_ms / 1000.0, 0.1)
        timeout_s = max(self.cfg.heartbeat_timeout_ms / 1000.0, interval_s * 2)

        while self._running.is_set():
            suc = self.get_successor()
            now = time.monotonic()

            if suc is not None:
                msg = Message(
                    kind="heartbeat",
                    src_id=self.cfg.node_id,
                    src_name=self.cfg.node_name,
                    payload={"ack": False},
                )
                self.send_to_successor(msg)

            with self._lock:
                ever = self._ever_ack
                last = self._last_ack_ts

            if suc is not None and ever:
                silence = now - last
                if silence > timeout_s:
                    self.on_successor_suspected(suc.id)
                    with self._lock:
                        self._ever_ack = False
                        self._last_ack_ts = now

            time.sleep(interval_s)
