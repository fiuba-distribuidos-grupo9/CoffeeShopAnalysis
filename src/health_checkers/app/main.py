# Imports.
from __future__ import annotations
import logging
import threading
import time
from pathlib import Path
from typing import Optional
from shared.utils import load_config_from_env, jitter_ms
from .node import Node  
from shared import initializer

STATE_FILE = Path("/tmp/hc_started.flag")

# Check if first start.
def _is_first_start() -> bool:
    return not STATE_FILE.exists()

# Mark that the application has started at least once.
def _mark_started() -> None:
    try:
        STATE_FILE.touch()
    except Exception as e:
        logging.warning(f"action: mark_started | result: fail | error: {e}")

# Smart election start logic.
def _smart_election_start(node: Node, cfg) -> None:
    is_first_start = _is_first_start()
    
    if is_first_start:
        jitter = jitter_ms(500, 2000)
        logging.info(f"action: starting_first_election | status: in progress")
        _mark_started()
        time.sleep(jitter)
        if not node.shutdown_requested():
            try:
                if node.cfg.node_id == 0:
                    node.election.start_election()
            except Exception as e:
                logging.error(f"action: first_election | result: fail | error: {e}")
    else:
        discovery_timeout = 6.0
        check_interval = 0.5
        elapsed = 0.0
        logging.info(f"---------------------------------------------")
        logging.info(f"action: controller_revived | result: success")
        while elapsed < discovery_timeout and not node.shutdown_requested():
            if node.election.leader_id is not None:
                logging.info(f"action: leader_discovered | result: success | new_leader: {node.election.leader_id}")
                return

            time.sleep(check_interval)
            elapsed += check_interval
        
        if node.election.leader_id is None and not node.shutdown_requested():
            logging.info(f"action: leader_discovered | result: fail | new_action: start_election")
            try:
                node.election.start_election()
            except Exception as e:
                logging.error(f"action: start_election | result: fail | error: {e}")

# Main run function.
def _run() -> None:
    cfg = load_config_from_env()
    node = Node(cfg)
    
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
            if election_thread.is_alive():
                logging.warning(f"action: join_election_thread | result: timeout")
        
        logging.info(f"action: main_shutdown | result: success")

# Main function.
def main() -> None:
    initializer.init_log("INFO")
    try:
        _run()
    except KeyboardInterrupt:
        logging.info(f"action: keyboard_interrupt | result: caught_in_main")
    except Exception as e:
        logging.error(f"action: main_exception | error: {e}")
        raise

# Entry point.
if __name__ == "__main__":
    main()
