# src/health_checkers/app/main.py
from __future__ import annotations
import os, sys

from .utils import load_config_from_env
from .ring_node import RingNode
from .leader import LeaderLoop


def _run_auto() -> None:
    cfg = load_config_from_env()
    print(f"[boot] {cfg.node_name} (id={cfg.node_id}) escuchando en {cfg.listen_host}:{cfg.listen_port}")
    peers_str = [f"{p.id}@{p.host}:{p.port}" for p in cfg.peers]
    print(f"[boot] Peers: {peers_str}")

    rn = RingNode(cfg)
    leader = LeaderLoop(cfg, rn.is_leader)
    leader.start()

    # Disparamos una elección inicial para tener líder.
    try:
        rn.election.start_election()
    except Exception as e:
        print(f"[boot] Error iniciando elección: {e}")

    try:
        rn.run()
    finally:
        leader.stop()


def _print_topology_and_exit() -> None:
    cfg = load_config_from_env()
    print("=== HEALTH CHECKERS (ring) — MODO MANUAL ===")
    print(f"Nodo: {cfg.node_name} (id={cfg.node_id}) en {cfg.listen_host}:{cfg.listen_port}")
    for p in cfg.peers:
        print(f"  peer: {p.id}@{p.host}:{p.port} ({p.name})")
    print("No se inicia lógica automática (heartbeat/election).")


def main() -> None:
    mode = os.getenv("MODE", "auto")
    if mode == "manual":
        _print_topology_and_exit()
        return
    try:
        _run_auto()
    except KeyboardInterrupt:
        print("Bye")


if __name__ == "__main__":
    main()
