from __future__ import annotations

import logging
import os, random
from typing import Dict, List
from .models import Config, Peer, ControllerTarget

def extract_id_from_hostname(hostname: str) -> int:
    parts = hostname.rstrip('_').split('_')
    for part in reversed(parts):
        if part.isdigit():  
            return int(part)
    raise ValueError(f"No ID in hostname")


def parse_peers(peers_env: str, current_node_id: int) -> List[Peer]:
    """
    Expected format: host:port,host:port,...
    host = name_id 
    Example --> hc_container_1:9101,hc_container_2:9101
    """
    peers: List[Peer] = []
    if not peers_env:
        return peers
    for item in peers_env.split(","):
        item = item.strip()
        if not item: 
            continue

        host, port = item.split(":",1)
        try:
            peer_id = extract_id_from_hostname(host)
        except ValueError as e:
            logging.warning(f"action: peers_startup | result: fail ({host}) | error: {e}")
            continue
        if peer_id == current_node_id:
            continue
        
        peers.append(Peer(
            id=peer_id,
            host=host,
            port=int(port),
            name=host
        ))
    return peers


def parse_controller_targets(targets_env: str) -> List[ControllerTarget]:
    """
    Expected format: host:port,host2:port2,
    Example: hc_container_1:9201,hc_container_2:9201
    """
    targets: List[ControllerTarget] = []
    if not targets_env:
        return targets
    
    for item in targets_env.split(","):
        item = item.strip()
        if not item:
            continue

        parts = item.split(":",1)
        
        if len(parts) != 2:
            continue
            
        host, port = parts
        host = host.strip()
        try:
            target_id = extract_id_from_hostname(host)
            name = host
        except ValueError:
            name = host

        targets.append(ControllerTarget(
            name=name,
            host=host,
            port=int(port),
            container_name=host
        ))
    
    return targets


def load_config_from_env() -> Config:
    node_id = int(os.getenv("NODE_ID", "1"))
    node_name = os.getenv("NODE_NAME", f"hc_container_{node_id}")
    
    peers = parse_peers(os.getenv("RING_PEERS", ""), node_id)
    controller_targets = parse_controller_targets(os.getenv("CONTROLLER_TARGETS", ""))
    
    cfg = Config(
        node_id=node_id,
        node_name=node_name,
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "9101")),
        health_listen_port=int(os.getenv("HEALTH_LISTEN_PORT", "9201")),
        peers=peers,
        controller_targets=controller_targets,

        heartbeat_interval_ms=int(os.getenv("HEARTBEAT_INTERVAL_MS", "800")),
        heartbeat_timeout_ms=int(os.getenv("HEARTBEAT_TIMEOUT_MS", "1000")),
        heartbeat_max_retries=int(os.getenv("HEARTBEAT_MAX_RETRIES", "5")),
        suspect_grace_ms=int(os.getenv("SUSPECT_GRACE_MS", "1200")),

        leader_check_interval_ms=int(os.getenv("LEADER_CHECK_INTERVAL_MS", "10000")),
        leader_check_timeout_ms=int(os.getenv("LEADER_CHECK_TIMEOUT_MS", "1000")),

        election_backoff_ms_min=int(os.getenv("ELECTION_BACKOFF_MS_MIN", "300")),
        election_backoff_ms_max=int(os.getenv("ELECTION_BACKOFF_MS_MAX", "900")),

        leader_sleep_min_ms=int(os.getenv("LEADER_RANDOM_SLEEP_MIN_MS", "1500")),
        leader_sleep_max_ms=int(os.getenv("LEADER_RANDOM_SLEEP_MAX_MS", "5000")),

        log_level=os.getenv("LOGGING_LEVEL", "INFO").upper(),

        docker_host=os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock"),
    )
    return cfg

def jitter_ms(min_ms: int, max_ms: int) -> float:
    return random.uniform(min_ms/1000.0, max_ms/1000.0)
