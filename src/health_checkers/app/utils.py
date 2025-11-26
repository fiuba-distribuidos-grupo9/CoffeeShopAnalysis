from __future__ import annotations

import os, random
from typing import Dict, List
from .models import Config, Peer, ControllerTarget

def parse_peers(peers_env: str) -> List[Peer]:
    """
    Expected format: id@host:port,id@host:port,...
    name = host if not explicitly provided.
    """
    peers: List[Peer] = []
    if not peers_env:
        return peers
    for item in peers_env.split(","):
        item = item.strip()
        if not item: 
            continue
        id_part, addr = item.split("@", 1)
        host, port = addr.split(":", 1)
        peers.append(Peer(id=int(id_part), host=host, port=int(port), name=host))
    peers.sort(key=lambda p: p.id)
    return peers


def parse_controller_targets(targets_env: str) -> List[ControllerTarget]:
    """
    Formato: name@host:port:container_name,name2@host2:port2:container_name2
    Ejemplo: hc-1@hc1_container:9201:hc1_container,hc-2@hc2_container:9201:hc2_container
    """
    targets: List[ControllerTarget] = []
    if not targets_env:
        return targets
    
    for item in targets_env.split(","):
        item = item.strip()
        if not item:
            continue
        
        name_part, rest = item.split("@", 1)
        parts = rest.split(":", 2)
        
        if len(parts) != 3:
            print(f"[config] Formato inválido para target: {item}")
            continue
            
        host, port, container_name = parts
        targets.append(ControllerTarget(
            name=name_part.strip(),
            host=host.strip(),
            port=int(port),
            container_name=container_name.strip()
        ))
    
    return targets


def load_config_from_env() -> Config:
    peers = parse_peers(os.getenv("RING_PEERS", ""))
    controller_targets = parse_controller_targets(os.getenv("CONTROLLER_TARGETS", ""))
    
    cfg = Config(
        node_id = int(os.getenv("NODE_ID", "1")),
        node_name = os.getenv("NODE_NAME", "hc-1"),
        listen_host = os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port = int(os.getenv("LISTEN_PORT", "9101")),
        health_listen_port = int(os.getenv("HEALTH_LISTEN_PORT", "9201")),
        peers = peers,
        controller_targets = controller_targets,

        heartbeat_interval_ms = int(os.getenv("HEARTBEAT_INTERVAL_MS", "800")),
        heartbeat_timeout_ms = int(os.getenv("HEARTBEAT_TIMEOUT_MS", "1000")),
        heartbeat_max_retries = int(os.getenv("HEARTBEAT_MAX_RETRIES", "5")),
        suspect_grace_ms = int(os.getenv("SUSPECT_GRACE_MS", "1200")),

        leader_check_interval_ms = int(os.getenv("LEADER_CHECK_INTERVAL_MS", "10000")),
        leader_check_timeout_ms = int(os.getenv("LEADER_CHECK_TIMEOUT_MS", "1000")),

        election_backoff_ms_min = int(os.getenv("ELECTION_BACKOFF_MS_MIN", "300")),
        election_backoff_ms_max = int(os.getenv("ELECTION_BACKOFF_MS_MAX", "900")),

        leader_sleep_min_ms = int(os.getenv("LEADER_RANDOM_SLEEP_MIN_MS", "1500")),
        leader_sleep_max_ms = int(os.getenv("LEADER_RANDOM_SLEEP_MAX_MS", "5000")),

        mode = os.getenv("MODE", "auto"),
        log_level = os.getenv("LOG_LEVEL", "INFO").upper(),

        docker_host = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock"),
    )
    return cfg

def jitter_ms(min_ms: int, max_ms: int) -> float:
    return random.uniform(min_ms/1000.0, max_ms/1000.0)
