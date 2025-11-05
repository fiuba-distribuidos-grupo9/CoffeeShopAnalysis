# src/health_checkers/app/main.py
from __future__ import annotations
import asyncio, os, sys
try:
    import uvloop
    uvloop.install()
except Exception:
    pass

from .utils import load_config_from_env
from .ring_node import RingNode
from .leader import LeaderLoop

async def _run_auto():
    cfg = load_config_from_env()
    print(f"[boot] {cfg.node_name} (id={cfg.node_id}) escuchando en {cfg.listen_host}:{cfg.listen_port}")
    print(f"[boot] Peers: {[f'{p.id}@{p.host}:{p.port}' for p in cfg.peers]}")
    rn = RingNode(cfg)
    leader = LeaderLoop(cfg, rn.is_leader)

    # We both start: Ring + leader-loop (Leader dozes if not leader).
    await asyncio.gather(
        rn.run(),
        leader.start(),
    )

def _print_topology_and_exit():
    cfg = load_config_from_env()
    print("=== HEALTH CHECKERS (ring) — MODO MANUAL ===")
    print(f"Nodo: {cfg.node_name} (id={cfg.node_id}) en {cfg.listen_host}:{cfg.listen_port}")
    for p in cfg.peers:
        print(f"  peer: {p.id}@{p.host}:{p.port} ({p.name})")
    print("No se inicia lógica automática (heartbeat/election).")

def main():
    mode = os.getenv("MODE", "auto")
    if mode == "manual":
        _print_topology_and_exit()
        return
    try:
        asyncio.run(_run_auto())
    except KeyboardInterrupt:
        print("Bye")

if __name__ == "__main__":
    main()
