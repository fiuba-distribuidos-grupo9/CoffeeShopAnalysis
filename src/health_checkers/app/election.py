from __future__ import annotations
import threading
import time
from typing import Optional, Callable, Dict
from .models import Message, Config
from .utils import jitter_ms


class Election:
    def __init__(self, cfg: Config, send_to_successor_with_retry: Callable[[Message], bool]):
        self.cfg = cfg
        self.send_to_successor = send_to_successor_with_retry
        self._leader_id: Optional[int] = None
        self._running = False
        self._lock = threading.Lock()
        
        self._active_elections: Dict[int, int] = {}

    @property
    def leader_id(self) -> Optional[int]:
        return self._leader_id

    def set_leader(self, leader_id: Optional[int]) -> None:
        with self._lock:
            self._leader_id = leader_id
            if leader_id is None:
                self._active_elections.clear()

    def start_election(self) -> None:
        """Inicia una elección si no hay otra en curso."""
        with self._lock:
            if self._running:
                print(f"[election] Ya hay una elección en curso en nodo {self.cfg.node_id}")
                return
            self._running = True
            self._active_elections.clear()
        
        print(f"[election] Nodo {self.cfg.node_id} iniciando elección")
        
        try:
            msg = Message(
                kind="election",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={
                    "candidate_id": self.cfg.node_id,
                    "initiator_id": self.cfg.node_id
                },
            )

            time.sleep(jitter_ms(self.cfg.election_backoff_ms_min, self.cfg.election_backoff_ms_max))
            success = self.send_to_successor(msg)
            
            if success:
                print(f"[election] Nodo {self.cfg.node_id} envió election a sucesor")
            else:
                print(f"[election] Nodo {self.cfg.node_id} sin peers, auto-proclamándose líder")
                with self._lock:
                    self._leader_id = self.cfg.node_id
        finally:
            with self._lock:
                self._running = False

    def handle_election(self, msg: Message) -> None:
        cid = msg.payload.get("candidate_id")
        initiator_id = msg.payload.get("initiator_id", cid)
        
        if cid is None:
            return

        my = self.cfg.node_id
        
        if initiator_id == my and cid == my:
            print(f"[election] El mensaje dio la vuelta completa, {my} es nuevo lider")
            with self._lock:
                self._leader_id = my
                self._active_elections.clear()
            
            announce = Message(
                kind="coordinator",
                src_id=my,
                src_name=self.cfg.node_name,
                payload={
                    "leader_id": my,
                    "initiator_id": my
                },
            )
            self.send_to_successor(announce)
            return

        with self._lock:
            if initiator_id in self._active_elections:
                prev_cid = self._active_elections[initiator_id]
                if cid <= prev_cid:
                    print(f"[election] Nodo: {my} ya procesó candidate_id={prev_cid} de initiator={initiator_id}, ignorando {cid}")
                    return
            self._active_elections[initiator_id] = cid

        print(f"[election] Nodo {my} recibió election: candidate={cid}, initiator={initiator_id}")

        if cid > my:
            print(f"[election] ID recibido mayor: {cid}. Reenvio el mensaje")
            self.send_to_successor(msg)
        else:
            print(f"[election] ID recibido menor: {cid}. Envio mensaje con ID: {my}")
            new_msg = Message(
                kind="election",
                src_id=my,
                src_name=self.cfg.node_name,
                payload={
                    "candidate_id": my,
                    "initiator_id": initiator_id
                },
            )
            self.send_to_successor(new_msg)

    def handle_coordinator(self, msg: Message) -> None:
        leader_id = msg.payload.get("leader_id")
        initiator_id = msg.payload.get("initiator_id", leader_id)
        
        if leader_id is None:
            return
        
        my = self.cfg.node_id
        
        print(f"[election] Nodo {my} recibió mensaje coordinator: leader={leader_id}, initiator={initiator_id}")
        
        if leader_id == my:
            print(f"[election] Soy el líder {leader_id}, anuncio completado")
            with self._lock:
                self._leader_id = leader_id
                self._active_elections.clear()
            return
        
        if initiator_id == my:
            print(f"[election] ✓ Anuncio de coordinator completó el ciclo en nodo {my}")
            with self._lock:
                self._leader_id = leader_id
                self._active_elections.clear()
            return
        
        with self._lock:
            self._leader_id = leader_id
            self._active_elections.clear()
        
        print(f"[election] Nodo {my} acepta líder {leader_id} y reenvía")
        self.send_to_successor(msg)
