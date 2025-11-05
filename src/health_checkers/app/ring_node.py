# src/health_checkers/app/ring_node.py
from __future__ import annotations
import asyncio, json, socket, contextlib
from typing import Optional, List
from .models import Config, Peer, Message
from .election import Election
from .dood import DockerReviver

class RingNode:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((cfg.listen_host, cfg.listen_port))
        self.sock.setblocking(False)

        self._peers: List[Peer] = [p for p in cfg.peers if p.id != cfg.node_id]
        self._peers.sort(key=lambda x: x.id)
        self._successor_index = self._compute_successor_index()

        self.election = Election(cfg, self._send_to_successor)
        self.reviver = DockerReviver(cfg.docker_host)

        self._leader_id: Optional[int] = None  # cache local

    # ——— Topology ———

    def _compute_successor_index(self) -> int:
        """The successor is the peer with the smallest id > my_id; if there is none, the smallest of all."""
        higher = [i for i, p in enumerate(self._peers) if p.id > self.cfg.node_id]
        if higher:
            return higher[0]
        return 0 if self._peers else -1

    def successor(self) -> Optional[Peer]:
        if not self._peers or self._successor_index < 0:
            return None
        return self._peers[self._successor_index]

    def predecessor(self) -> Optional[Peer]:
        if not self._peers:
            return None
        # 'pred' is the immediately smaller by id (or the larger if I am the smaller)
        lower = [p for p in self._peers if p.id < self.cfg.node_id]
        return lower[-1] if lower else self._peers[-1]

    def _remove_peer(self, peer_id: int):
        self._peers = [p for p in self._peers if p.id != peer_id]
        self._peers.sort(key=lambda x: x.id)
        self._successor_index = self._compute_successor_index()

    # ——— Leader State ———
    def is_leader(self) -> bool:
        return (self.election.leader_id == self.cfg.node_id)

    # ——— IO ———
    async def _send_to(self, peer: Peer, msg: Message):
        data = msg.model_dump_json().encode("utf-8")
        loop = asyncio.get_running_loop()
        try:
            await loop.sock_sendto(self.sock, data, (peer.host, peer.port))
        except Exception as e:
            print(f"[send] Error enviando a {peer.name}@{peer.host}:{peer.port}: {e}")

    async def _send_to_successor(self, msg: Message):
        suc = self.successor()
        if suc:
            await self._send_to(suc, msg)

    # ——— Run ———
    async def run(self):
        loop = asyncio.get_running_loop()
        recv_task = asyncio.create_task(self._recv_loop())
        hb_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await asyncio.gather(recv_task, hb_task)
        finally:
            with contextlib.suppress(Exception):
                recv_task.cancel()
                hb_task.cancel()

    async def _recv_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            data, addr = await loop.sock_recvfrom(self.sock, 65535)
            try:
                msg = Message.model_validate_json(data)
            except Exception:
                continue
            await self._handle_message(msg)

    async def _handle_message(self, msg: Message):
        kind = msg.kind

        if kind == "heartbeat":
            # Incoming heartbeat (from our predecessor)
            # (The predictor is not known by address here; we only update the signal)
            # Nothing more to do; if we wanted, we could respond.
            # We use the arrival itself as proof of life for the predictor.
            # (The timeout logic resides on the sending side)
            return

        elif kind == "election":
            await self.election.handle_election(msg)

        elif kind == "coordinator":
            await self.election.handle_coordinator(msg)
            lid = self.election.leader_id
            print(f"[ring] Nuevo líder: {lid}")

        elif kind == "probe":
            # Global leader ping (to be implemented later)
            ack = Message(kind="probe_ack", src_id=self.cfg.node_id, src_name=self.cfg.node_name)
            await self._send_to_successor(ack)

    # ——— Heartbeats neighbor and fall detection ———
    async def _heartbeat_loop(self):
        """
        We periodically send heartbeats to the SUCCESSOR.
        If there is no successor (we are the only one), we can proclaim ourselves leader.
        If we detect that the successor is unresponsive (due to an IO error or persistent silence),
        we consider it down:
            1) We attempt to revive its container (if it is mapped).
            2) We remove it from the ring (rearrangement).
            3) If it was the leader, we trigger an election.
        """
        import time
        interval = self.cfg.heartbeat_interval_ms / 1000.0
        timeout_ms = self.cfg.heartbeat_timeout_ms

        # Window of consecutive silences.
        misses = 0
        while True:
            suc = self.successor()
            if not suc:
                # We are alone → leaders by default.
                if self.election.leader_id != self.cfg.node_id:
                    self.election.set_leader(self.cfg.node_id)
                    print(f"[ring] Soy líder (único en el anillo)")
                await asyncio.sleep(interval)
                continue

            # We send heartbeat.
            hb = Message(kind="heartbeat", src_id=self.cfg.node_id, src_name=self.cfg.node_name)
            try:
                await self._send_to(suc, hb)
                # If sending doesn't raise an error, we'll assume it's okay for now.
                misses = 0
            except Exception:
                misses += 1

            if misses * self.cfg.heartbeat_interval_ms > timeout_ms:
                print(f"[ring] Sucesor {suc.id} sospechoso/caído. Reacomodando anillo…")
                # Attempt to revive via DooD.
                container = self.cfg.revive_targets.get(suc.name)
                if container:
                    ok = self.reviver.revive_container(container)
                    print(f"[ring] revive({suc.name} -> {container}) = {ok}")

                # If the fallen one was the known leader → force an election.
                was_leader = (self.election.leader_id == suc.id)
                self._remove_peer(suc.id)
                misses = 0

                if was_leader:
                    print("[ring] El sucesor caído era líder → inicio elección")
                    await self.election.start_election()
                elif self.election.leader_id is None:
                    print("[ring] No conozco líder → inicio elección")
                    await self.election.start_election()

            await asyncio.sleep(interval)
