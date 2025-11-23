from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Literal
from pydantic import BaseModel, Field, ValidationError

class Peer(BaseModel):
    id: int
    host: str
    port: int
    name: str

class Config(BaseModel):
    node_id: int
    node_name: str
    listen_host: str
    listen_port: int
    peers: List[Peer]

    heartbeat_interval_ms: int = 800
    heartbeat_timeout_ms: int = 2500
    suspect_grace_ms: int = 1200

    election_backoff_ms_min: int = 300
    election_backoff_ms_max: int = 900

    leader_sleep_min_ms: int = 1500
    leader_sleep_max_ms: int = 5000

    mode: Literal["auto","manual"] = "auto"
    log_level: Literal["DEBUG","INFO","WARNING","ERROR"] = "INFO"

    revive_targets: Dict[str, str] = Field(default_factory=dict)
    docker_host: Optional[str] = "unix:///var/run/docker.sock"

class Message(BaseModel):
    kind: Literal[
        "heartbeat",
        "election",
        "election_ok",
        "coordinator",
        "whois",
        "iam",
        "probe",
        "probe_ack"
    ]
    
    src_id: int
    src_name: str
    payload: Dict = Field(default_factory=dict)
