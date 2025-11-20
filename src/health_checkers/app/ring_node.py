# src/health_checkers/app/ring_node.py
from __future__ import annotations
import socket
import time
from typing import Optional, List

from .models import Config, Peer, Message
from .election import Election
from .dood import DockerReviver
from .heartbeat import HeartbeatLoop

NOW = lambda: time.monotonic()


class RingNode:
    def __init__(self, cfg: Config):
        self.cfg = cfg

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((cfg.listen_host, cfg.listen_port))
        self.sock.settimeout(1.0)

        # Peers excluyéndome a mí
        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda p: p.id)
        self._successor_index = self._compute_successor_index()

        self.election = Election(cfg, self._send_to_successor)
        self.reviver = DockerReviver(cfg.docker_host)

        # Loop de heartbeat (pings al sucesor).
        self.heartbeat = HeartbeatLoop(
            cfg=cfg,
            get_successor=self.successor,
            send_to_successor=self._send_to_successor,
            on_successor_suspected=self._on_successor_suspected,
        )
        self.heartbeat.start()

    # ---------- TOPOLOGÍA ----------

    def _compute_successor_index(self) -> int:
        if not self._peers:
            return 0
        higher = [i for i, p in enumerate(self._peers) if p.id > self.cfg.node_id]
        return higher[0] if higher else 0

    def successor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        return self._peers[self._successor_index]

    def predecessor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        lower = [p for p in self._peers if p.id < self.cfg.node_id]
        return lower[-1] if lower else self._peers[-1]

    def _peer_by_id(self, pid: int) -> Optional[Peer]:
        for p in self._peers:
            if p.id == pid:
                return p
        return None

    def _remove_peer(self, peer_id: int) -> None:
        self._peers = [p for p in self._peers if p.id != peer_id]
        self._peers.sort(key=lambda p: p.id)
        self._successor_index = self._compute_successor_index()

    # ---------- LÍDER ----------

    def is_leader(self) -> bool:
        return self.election.leader_id == self.cfg.node_id

    # ---------- ENVÍO ----------

    def _send_to(self, peer: Peer, msg: Message) -> None:
        data = msg.model_dump_json().encode("utf-8")
        try:
            self.sock.sendto(data, (peer.host, peer.port))
        except Exception as e:
            print(f"[send] Error enviando a {peer.name}@{peer.host}:{peer.port}: {e}")

    def _send_to_successor(self, msg: Message) -> None:
        suc = self.successor()
        if suc is not None:
            self._send_to(suc, msg)

    # ---------- CALLBACK HEARTBEAT ----------

    def _on_successor_suspected(self, successor_id: int) -> None:
        suc = self.successor()
        if suc is None or suc.id != successor_id:
            return
        print(f"[hb] Sucesor {suc.id} ({suc.name}) sospechado caído. Lo removemos del anillo.")
        self._remove_peer(suc.id)

        # Si el sucesor sospechado era el líder → nueva elección.
        if self.election.leader_id == suc.id:
            self.election.set_leader(None)
            try:
                self.election.start_election()
            except Exception as e:
                print(f"[hb] Error iniciando nueva elección: {e}")

        # Si quedé solo, me auto-proclamo líder.
        if not self._peers:
            if self.election.leader_id != self.cfg.node_id:
                self.election.set_leader(self.cfg.node_id)
                print("[ring] Soy líder (único en el anillo).")

    # ---------- LOOP PRINCIPAL ----------

    def run(self) -> None:
        print("[ring] Loop principal iniciado.")
        while True:
            try:
                data, addr = self.sock.recvfrom(64 * 1024)
            except socket.timeout:
                continue
            except OSError as e:
                print(f"[ring] Error de socket: {e}")
                break

            try:
                msg = Message.model_validate_json(data.decode("utf-8"))
            except Exception as e:
                print(f"[ring] Mensaje inválido recibido: {e}")
                continue

            self._handle_message(msg)

    def _handle_message(self, msg: Message) -> None:
        kind = msg.kind

        if kind == "election":
            self.election.handle_election(msg)

        elif kind == "coordinator":
            self.election.handle_coordinator(msg)
            print(f"[ring] Nuevo líder: {self.election.leader_id}")

        elif kind == "heartbeat":
            # heartbeat con payload {"ack": False} = ping; {"ack": True} = ack
            ack_flag = bool(msg.payload.get("ack", False))
            if ack_flag:
                # ACK desde el sucesor
                self.heartbeat.notify_ack()
            else:
                # Ping desde el predecesor → respondemos ACK directo a ese peer.
                peer = self._peer_by_id(msg.src_id)
                if peer is not None:
                    ack_msg = Message(
                        kind="heartbeat",
                        src_id=self.cfg.node_id,
                        src_name=self.cfg.node_name,
                        payload={"ack": True},
                    )
                    self._send_to(peer, ack_msg)

        elif kind == "probe":
            # soporte opcional para mensajes de "probe" del líder
            ack = Message(
                kind="probe_ack",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
            )
            self._send_to_successor(ack)
