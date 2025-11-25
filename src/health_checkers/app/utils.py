from __future__ import annotations

import os, random
from typing import Dict, List
from .models import Config, Peer

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

def parse_revive_targets(mapping_env: str) -> Dict[str, str]:
    """
    Formato: nodeName=containerName,nodeName2=containerName2
    """
    mapping: Dict[str, str] = {}
    if not mapping_env:
        return mapping
    for pair in mapping_env.split(","):
        pair = pair.strip()
        if not pair:
            continue
        k, v = pair.split("=", 1)
        mapping[k.strip()] = v.strip()
    return mapping

def load_config_from_env() -> Config:
    peers = parse_peers(os.getenv("RING_PEERS", ""))
    cfg = Config(
        node_id = int(os.getenv("NODE_ID", "1")),
        node_name = os.getenv("NODE_NAME", "hc-1"),
        listen_host = os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port = int(os.getenv("LISTEN_PORT", "9101")),
        peers = peers,

        heartbeat_interval_ms = int(os.getenv("HEARTBEAT_INTERVAL_MS", "800")),
        heartbeat_timeout_ms = int(os.getenv("HEARTBEAT_TIMEOUT_MS", "2500")),
        suspect_grace_ms = int(os.getenv("SUSPECT_GRACE_MS", "1200")),

        election_backoff_ms_min = int(os.getenv("ELECTION_BACKOFF_MS_MIN", "300")),
        election_backoff_ms_max = int(os.getenv("ELECTION_BACKOFF_MS_MAX", "900")),

        leader_sleep_min_ms = int(os.getenv("LEADER_RANDOM_SLEEP_MIN_MS", "1500")),
        leader_sleep_max_ms = int(os.getenv("LEADER_RANDOM_SLEEP_MAX_MS", "5000")),

        mode = os.getenv("MODE", "auto"),
        log_level = os.getenv("LOG_LEVEL", "INFO").upper(),

        revive_targets = parse_revive_targets(os.getenv("REVIVE_TARGETS", "")),
        docker_host = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock"),
    )
    return cfg

def jitter_ms(min_ms: int, max_ms: int) -> float:
    return random.uniform(min_ms/1000.0, max_ms/1000.0)
