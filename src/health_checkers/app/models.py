from __future__ import annotations
import json
from typing import Dict, List, Optional, Literal, Any
from dataclasses import dataclass, field


@dataclass
class Peer:
    id: int
    host: str
    port: int
    name: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "name": self.name
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Peer:
        return cls(
            id=int(data["id"]),
            host=str(data["host"]),
            port=int(data["port"]),
            name=str(data["name"])
        )


@dataclass
class ControllerTarget:
    name: str
    host: str
    port: int
    container_name: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "container_name": self.container_name
        }


@dataclass
class Config:
    node_id: int
    node_name: str
    listen_host: str
    listen_port: int
    peers: List[Peer]
    
    health_listen_port: int = 9201
    

    controller_targets: List[ControllerTarget] = field(default_factory=list)
    
    heartbeat_interval_ms: int = 800
    heartbeat_timeout_ms: int = 1000  
    heartbeat_max_retries: int = 5
    suspect_grace_ms: int = 1200
    
    leader_check_interval_ms: int = 10000
    leader_check_timeout_ms: int = 1000
    
    election_backoff_ms_min: int = 300
    election_backoff_ms_max: int = 900
    
    leader_sleep_min_ms: int = 1500
    leader_sleep_max_ms: int = 5000
    
    log_level: str = "INFO"
    
    docker_host: Optional[str] = "unix:///var/run/docker.sock"


@dataclass
class Message:
    kind: str
    src_id: int
    src_name: str
    payload: Dict[str, Any] = field(default_factory=dict)
    notifying_revived: bool = False

    VALID_KINDS = {
        "heartbeat", "heartbeat_ack",
        "election", "election_ok", "coordinator",
        "whois", "iam", "probe", "probe_ack",
        "is_leader_alive", "leader_alive_ack"
    }

    def __post_init__(self):
        if self.kind not in self.VALID_KINDS:
            raise ValueError(f"kind inválido: '{self.kind}'")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "src_id": self.src_id,
            "src_name": self.src_name,
            "payload": self.payload,
            "notifying_revived": self.notifying_revived
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Message:
        data = json.loads(json_str)
        return cls(
            kind=str(data["kind"]),
            src_id=int(data["src_id"]),
            src_name=str(data["src_name"]),
            payload=dict(data.get("payload", {})),
            notifying_revived=bool(data.get("notifying_revived", False))
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Message:
        return cls(
            kind=str(data["kind"]),
            src_id=int(data["src_id"]),
            src_name=str(data["src_name"]),
            payload=dict(data.get("payload", {})),
            notifying_revived=bool(data.get("notifying_revived", False))
        )
