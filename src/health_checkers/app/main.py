from __future__ import annotations

import os
import signal
import threading
from typing import Optional

from .utils import load_config_from_env
from .ring_node import RingNode
from .leader import LeaderLoop

_shutdown_event = threading.Event()
_current_node: Optional[RingNode] = None
_current_leader: Optional[LeaderLoop] = None


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return str(signum)


def _handle_signal(signum, frame):
    """
    Manejador de SIGTERM / SIGINT.
    Marca el shutdown y pide a los componentes que se detengan de forma ordenada.
    """
    global _current_node, _current_leader

    name = _signal_name(signum)
    print(f"[signal] Recibida señal {name} ({signum}). Iniciando apagado graceful...")
    _shutdown_event.set()

    if _current_node is not None:
        try:
            _current_node.stop()
        except Exception as e:
            print(f"[signal] Error al detener RingNode: {e}")

    if _current_leader is not None:
        try:
            _current_leader.stop()
        except Exception as e:
            print(f"[signal] Error al detener LeaderLoop: {e}")


def _run_auto() -> None:
    global _current_node, _current_leader

    cfg = load_config_from_env()
    print(f"[boot] {cfg.node_name} (id={cfg.node_id}) escuchando en {cfg.listen_host}:{cfg.listen_port}")
    peers_str = [f"{p.id}@{p.host}:{p.port}" for p in cfg.peers]
    print(f"[boot] Peers: {peers_str}")

    rn = RingNode(cfg)
    leader = LeaderLoop(cfg, rn.is_leader)

    _current_node = rn
    _current_leader = leader

    leader.start()

    try:
        rn.election.start_election()
    except Exception as e:
        print(f"[boot] Error iniciando elección: {e}")

    try:
        rn.run()
    finally:
        try:
            leader.stop()
        except Exception:
            pass
        try:
            rn.stop()
        except Exception:
            pass
        print("[boot] Nodo apagado graceful.")


def _print_topology_and_exit() -> None:
    cfg = load_config_from_env()
    print("=== HEALTH CHECKERS (ring) — MODO MANUAL ===")
    print(f"Nodo: {cfg.node_name} (id={cfg.node_id}) en {cfg.listen_host}:{cfg.listen_port}")
    for p in cfg.peers:
        print(f"  peer: {p.id}@{p.host}:{p.port} ({p.name})")
    print("No se inicia lógica automática (heartbeat/election/leader).")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    mode = os.getenv("MODE", "auto")
    if mode == "manual":
        _print_topology_and_exit()
        return

    try:
        _run_auto()
    except KeyboardInterrupt:
        print("[main] KeyboardInterrupt recibido. Apagando...")
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
