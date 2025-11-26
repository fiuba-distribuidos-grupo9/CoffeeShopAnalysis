from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path
from typing import Optional

from .utils import load_config_from_env, jitter_ms
from .ring_node import RingNode
from .health_checker import HealthChecker

_shutdown_event = threading.Event()
_current_node: Optional[RingNode] = None
_current_health: Optional[HealthChecker] = None

STATE_FILE = Path("/tmp/hc_started.flag")


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return str(signum)


def _handle_signal(signum, frame):
    global _current_node, _current_health

    name = _signal_name(signum)
    print(f"[signal] Recibida señal {name} ({signum}). Iniciando apagado graceful...")
    _shutdown_event.set()

    if _current_health is not None:
        try:
            _current_health.stop()
        except Exception as e:
            print(f"[signal] Error al detener HealthChecker: {e}")

    if _current_node is not None:
        try:
            _current_node.stop()
        except Exception as e:
            print(f"[signal] Error al detener RingNode: {e}")


def _is_first_start() -> bool:
    return not STATE_FILE.exists()


def _mark_started() -> None:
    try:
        STATE_FILE.touch()
    except Exception as e:
        print(f"[boot] Error creando archivo de estado: {e}")


def _smart_election_start(rn: RingNode, cfg) -> None:
    is_first_start = _is_first_start()
    
    if is_first_start:
        jitter = jitter_ms(500, 2000)
        print(f"[boot] Iniciando elección en {jitter:.2f}s...")
        _mark_started()
        time.sleep(jitter)
        
        if not _shutdown_event.is_set():
            try:
                rn.election.start_election()
            except Exception as e:
                print(f"[boot] Error iniciando elección: {e}")
    else:
        discovery_timeout = 6.0
        check_interval = 0.5
        elapsed = 0.0
        
        print(f"[discovery] Nodo revivido.")
        
        while elapsed < discovery_timeout and not _shutdown_event.is_set():
            if rn.election.leader_id is not None:
                print(f"[discovery] Líder descubierto: {rn.election.leader_id}")
                return
            time.sleep(check_interval)
            elapsed += check_interval
        
        if rn.election.leader_id is None:
            print("[discovery] No se detectó líder, iniciando elección...")
            try:
                rn.election.start_election()
            except Exception as e:
                print(f"[discovery] Error iniciando elección: {e}")


def _run_auto() -> None:
    global _current_node, _current_health

    cfg = load_config_from_env()
    print(f"[boot] {cfg.node_name} (id={cfg.node_id})")
    print(f"[boot]   - Elección: {cfg.listen_host}:{cfg.listen_port}")
    print(f"[boot]   - Health: {cfg.listen_host}:{cfg.health_listen_port}")
    
    peers_str = [f"{p.id}@{p.host}:{p.port}" for p in cfg.peers]
    print(f"[boot] Peers: {peers_str}")
    
    targets_str = [f"{t.name}@{t.host}:{t.port} ({t.container_name})" for t in cfg.controller_targets]
    print(f"[boot] Controller targets: {targets_str}")

    rn = RingNode(cfg)
    _current_node = rn

    def on_leader_dead():
        current_leader = rn.election.leader_id
        print(f"[health] Líder {current_leader} caído, iniciando elección...")
        if rn.election is not None:
            rn.election.set_leader(None)
        try:
            rn.election.start_election()
        except Exception as e:
            print(f"[health] Error iniciando elección: {e}")

    def notify_revived_node(target_host: str, target_health_port: int) -> None:
        rn.notify_leadership_to(target_host, target_health_port)

    health = HealthChecker(
        cfg=cfg,
        is_leader_callable=rn.is_leader,
        get_leader_info=rn.get_leader_info,
        on_leader_dead=on_leader_dead,
        notify_revived_node=notify_revived_node
    )
    _current_health = health

    health.start()

    election_thread = threading.Thread(
        target=_smart_election_start,
        args=(rn, cfg),
        name=f"ElectionStart-{cfg.node_id}",
        daemon=False
    )
    election_thread.start()

    try:
        rn.run()
    finally:
        if election_thread.is_alive():
            print("[boot] Esperando thread de elección inicial...")
            election_thread.join(timeout=3.0)
            if election_thread.is_alive():
                print("[boot] ⚠️  Thread de elección no terminó a tiempo")
        
        try:
            health.stop()
        except Exception as e:
            print(f"[boot] Error deteniendo health: {e}")
        
        try:
            rn.stop()
        except Exception as e:
            print(f"[boot] Error deteniendo ring: {e}")
        
        print("[boot] Nodo apagado completamente.")


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        _run_auto()
    except KeyboardInterrupt:
        print("[main] KeyboardInterrupt recibido. Apagando...")
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
