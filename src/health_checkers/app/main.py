from __future__ import annotations

import logging
import signal
import threading
import time
from pathlib import Path
from typing import Optional

from .utils import load_config_from_env, jitter_ms
from .node import Node  
from shared import initializer

_shutdown_event = threading.Event()
_current_node: Optional[Node] = None  
STATE_FILE = Path("/tmp/hc_started.flag")


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return str(signum)


def _handle_signal(signum, frame):
    global _current_node

    name = _signal_name(signum)
    logging.info(f"action: signal_received | signal: {signum} ({name}) | result: success")
    _shutdown_event.set()

    if _current_node is not None:
        try:
            _current_node.stop()
        except Exception as e:
            logging.error(f"action: signal_received | result: fail | error: {e}")


def _is_first_start() -> bool:
    return not STATE_FILE.exists()


def _mark_started() -> None:
    try:
        STATE_FILE.touch()
    except Exception as e:
        pass


def _smart_election_start(node: Node, cfg) -> None:
    is_first_start = _is_first_start()
    
    if is_first_start:
        jitter = jitter_ms(500, 2000)
        logging.info(f"action: starting_first_election | status: in progress")
        _mark_started()
        time.sleep(jitter)
        
        if not _shutdown_event.is_set():
            try:
                node.election.start_election()
            except Exception as e:
                logging.error(f"action: first_election | result: fail | error: {e}")
    else:
        discovery_timeout = 6.0
        check_interval = 0.5
        elapsed = 0.0
        
        logging.info(f"---------------------------------------------")
        logging.info(f"action: controller_revived | result: success")
        
        while elapsed < discovery_timeout and not _shutdown_event.is_set():
            if node.election.leader_id is not None:
                logging.info(f"action: leader_discovered | result: success | new_leader: {node.election.leader_id}")
                return
            time.sleep(check_interval)
            elapsed += check_interval
        
        if node.election.leader_id is None:
            logging.info(f"action: leader_discovered | result: fail | new_action: start_election")
            try:
                node.election.start_election()
            except Exception as e:
                logging.error(f"action: start_election | result: fail | error: {e}")


def _run() -> None:
    global _current_node

    cfg = load_config_from_env()

    node = Node(cfg)
    _current_node = node

    node.start()

    election_thread = threading.Thread(
        target=_smart_election_start,
        args=(node, cfg),
        name=f"ElectionStart-{cfg.node_id}",
        daemon=False
    )
    election_thread.start()

    try:
        node.run()
    finally:
        if election_thread.is_alive():
            election_thread.join(timeout=3.0)
        
        try:
            node.stop()
        except Exception as e:
            pass
        
        logging.info(f"action: close_down | result: success")


def main() -> None:
    initializer.init_log("INFO")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        _run()
    except KeyboardInterrupt:
        logging.debug(f"KeyboardInterrupt recibido. Apagando...")
        _handle_signal(signal.SIGINT, None)


if __name__ == "__main__":
    main()
