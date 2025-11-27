from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path
from typing import Optional

from .utils import load_config_from_env, jitter_ms
from .ring_node import RingNode
from .health_checker import HealthChecker
from .health_callbacks import HealthCallbacks
from shared import initializer

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
    logging.info(f"Recibida señal {name} ({signum}). Iniciando apagado graceful...")
    _shutdown_event.set()

    if _current_health is not None:
        try:
            _current_health.stop()
        except Exception as e:
            pass

    if _current_node is not None:
        try:
            _current_node.stop()
        except Exception as e:
            logging.error(f"Error al detener RingNode: {e}")


def _is_first_start() -> bool:
    return not STATE_FILE.exists()


def _mark_started() -> None:
    try:
        STATE_FILE.touch()
    except Exception as e:
        logging.error(f"Error creando archivo de estado: {e}")


def _smart_election_start(ring_node: RingNode, cfg) -> None:
    is_first_start = _is_first_start()
    
    if is_first_start:
        jitter = jitter_ms(500, 2000)
        logging.info(f"Iniciando elección")
        _mark_started()
        time.sleep(jitter)
        
        if not _shutdown_event.is_set():
            try:
                ring_node.election.start_election()
            except Exception as e:
                logging.error(f"Error iniciando elección: {e}")
    else:
        discovery_timeout = 6.0
        check_interval = 0.5
        elapsed = 0.0
        
        logging.info(f"Nodo revivido.")
        
        while elapsed < discovery_timeout and not _shutdown_event.is_set():
            if ring_node.election.leader_id is not None:
                logging.info(f"Líder descubierto: {ring_node.election.leader_id}")
                return
            time.sleep(check_interval)
            elapsed += check_interval
        
        if ring_node.election.leader_id is None:
            logging.info(f"No se detectó líder, iniciando elección...")
            try:
                ring_node.election.start_election()
            except Exception as e:
                logging.error(f"Error iniciando elección: {e}")


def _run() -> None:
    global _current_node, _current_health

    cfg = load_config_from_env()
    logging.info(f"{cfg.node_name} (id={cfg.node_id})")
    logging.info(f"  - Elección: {cfg.listen_host}:{cfg.listen_port}")
    logging.info(f"  - Health: {cfg.listen_host}:{cfg.health_listen_port}")
    
    peers_str = [f"{p.id}@{p.host}:{p.port}" for p in cfg.peers]
    logging.info(f"Peers: {peers_str}")
    
    targets_str = [f"{t.name}@{t.host}:{t.port} ({t.container_name})" for t in cfg.controller_targets]
    logging.info(f"Controller targets: {targets_str}")

    ring_node = RingNode(cfg)
    _current_node = ring_node

    callbacks = HealthCallbacks(ring_node)

    health = HealthChecker(
        cfg=cfg,
        is_leader_callable=ring_node.is_leader,
        get_leader_info=ring_node.get_leader_info,
        on_leader_dead=callbacks.on_leader_dead,
        notify_revived_node=callbacks.notify_revived_node
    )
    _current_health = health

    health.start()

    election_thread = threading.Thread(
        target=_smart_election_start,
        args=(ring_node, cfg),
        name=f"ElectionStart-{cfg.node_id}",
        daemon=False
    )
    election_thread.start()

    try:
        ring_node.run()
    finally:
        if election_thread.is_alive():
            logging.info(f"Esperando thread de elección inicial...")
            election_thread.join(timeout=3.0)
        try:
            health.stop()
        except Exception as e:
            logging.error(f"Error deteniendo health: {e}")
        
        try:
            ring_node.stop()
        except Exception as e:
            logging.error(f"Error deteniendo ring: {e}")
        
        logging.info("Nodo apagado completamente.")


def main() -> None:
    initializer.init_log("INFO")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        _run()
    except KeyboardInterrupt:
        logging.debug("KeyboardInterrupt recibido. Apagando...")
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
