from __future__ import annotations

import socket
import time
from typing import Optional, List

from .models import Config, Peer, Message
from .election import Election
from .dood import DockerReviver

try:
    from .heartbeat import HeartbeatLoop
except ImportError:
    HeartbeatLoop = None


class RingNode:
    """
    Nodo del anillo:
      - Mantiene la topología (peers, sucesor, predecesor).
      - Maneja mensajes vía UDP.
      - Integra elección de líder y (opcionalmente) heartbeat.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((cfg.listen_host, cfg.listen_port))
        self.sock.settimeout(1.0)

        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda p: p.id)
        self._successor_index = self._compute_successor_index()

        self.election = Election(cfg, self._send_to_successor)
        self.reviver = DockerReviver(cfg.docker_host)

        self._running: bool = True

        if HeartbeatLoop is not None:
            try:
                self.heartbeat = HeartbeatLoop(
                    cfg=cfg,
                    get_successor=self.successor,
                    send_to_successor=self._send_to_successor,
                    on_successor_suspected=self._on_successor_suspected,
                )
                self.heartbeat.start()
            except Exception as e:
                print(f"[ring] No se pudo iniciar HeartbeatLoop: {e}")
        else:
            self.heartbeat = None

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

    def is_leader(self) -> bool:
        return self.election.leader_id == self.cfg.node_id

    def _send_to(self, peer: Peer, msg: Message) -> None:
        data = msg.model_dump_json().encode("utf-8")
        try:
            self.sock.sendto(data, (peer.host, peer.port))
        except Exception as e:
            print(f"[send] Error enviando a {peer.name}@{peer.host}:{peer.port}: {e}")

    def _send_to_successor(self, msg: Message) -> None:
        suc = self.successor()
        if suc is None:
            return
        self._send_to(suc, msg)

    def _on_successor_suspected(self, successor_id: int) -> None:
        suc = self.successor()
        if suc is None or suc.id != successor_id:
            return
        
        print(f"[hb] Sucesor {suc.id} ({suc.name}) sospechado caído. Lo removemos del anillo.")
        self._remove_peer(suc.id)

        self._validate_and_clean_unreachable_peers()

        if not self._peers:
            if not self.is_leader():
                print("[ring] Soy el unico nodo restante, me auto-proclamo lider")
                self.election.set_leader(self.cfg.node_id)
            return

        if self.election.leader_id == suc.id:
            self.election.set_leader(None)
            try:
                self.election.start_election()
            except Exception as e:
                print(f"[hb] Error iniciando nueva elección: {e}")

    def _validate_and_clean_unreachable_peers(self) -> None:
        unreachable = []

        for peer in self._peers[:]:
            probe = Message(
                kind="probe",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={}
                )
            try:
                data = probe.model_dump_json().encode("utf-8")
                self.sock.sendto(data, (peer.host, peer.port))
            except socket.gaierror as e:
                print(f"[validate] Peer {peer.id} ({peer.name}) inalcanzable")
                unreachable.append(peer.id)
            except Exception as e:
                print(f"[validate] Error probando {peer.id}: {e}")
        
        for peer_id in unreachable:
            self._remove_peer(peer_id)

    def stop(self) -> None:
        """
        Detiene el loop principal y cierra recursos.
        Se llama desde el manejador de señales y desde el finally de main().
        """
        if not self._running:
            return
        print("[ring] stop() llamado. Cerrando socket y deteniendo heartbeat si aplica...")
        self._running = False
        try:
            self.sock.close()
        except Exception:
            pass

        hb = getattr(self, "heartbeat", None)
        if hb is not None and hasattr(hb, "stop"):
            try:
                hb.stop()
            except Exception as e:
                print(f"[ring] Error al detener HeartbeatLoop: {e}")

    def run(self) -> None:
        print("[ring] Loop principal iniciado.")
        try:
            while self._running:
                try:
                    data, addr = self.sock.recvfrom(64 * 1024)
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self._running:
                        break
                    print(f"[ring] Error de socket: {e}")
                    break

                try:
                    msg = Message.model_validate_json(data.decode("utf-8"))
                except Exception as e:
                    print(f"[ring] Mensaje inválido recibido: {e}")
                    continue

                self._handle_message(msg)
        finally:
            try:
                self.sock.close()
            except Exception:
                pass
            print("[ring] Loop principal terminado.")

    def _handle_message(self, msg: Message) -> None:
        kind = msg.kind

        if kind == "election":
            self.election.handle_election(msg)

        elif kind == "coordinator":
            self.election.handle_coordinator(msg)
            print(f"[ring] Nuevo líder: {self.election.leader_id}")

        elif kind == "heartbeat":
            is_ack = bool(msg.payload.get("ack"))
            if is_ack:
                hb = getattr(self, "heartbeat", None)
                if hb is not None:
                    try:
                        hb.notify_ack()
                    except Exception as e:
                        print(f"[ring] Error en heartbeat.notify_ack(): {e}")
            else:
                ack = Message(
                    kind="heartbeat",
                    src_id=self.cfg.node_id,
                    src_name=self.cfg.node_name,
                    payload={"ack": True},
                )
                peer = self._peer_by_id(msg.src_id)
                if peer is not None:
                    self._send_to(peer, ack)

        elif kind == "probe":
            ack = Message(
                kind="probe_ack",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
            )
            self._send_to_successor(ack)
