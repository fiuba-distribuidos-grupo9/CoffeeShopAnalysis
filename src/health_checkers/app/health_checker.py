from __future__ import annotations

import socket
import threading
import time
from typing import Optional, Callable

from .models import Config, Message, ControllerTarget
from .dood import DockerReviver


class HealthChecker:
    def __init__(
        self,
        cfg: Config,
        is_leader_callable: Callable[[], bool],
        get_leader_info: Callable[[], Optional[tuple[str, int]]],
        on_leader_dead: Callable[[], None],
        notify_revived_node: Callable[[str, int], None]
    ):
        self.cfg = cfg
        self.is_leader = is_leader_callable
        self.get_leader_info = get_leader_info
        self.on_leader_dead = on_leader_dead
        self.notify_revived_node = notify_revived_node

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((cfg.listen_host, cfg.health_listen_port))
        self.sock.settimeout(0.5)

        self.reviver = DockerReviver(cfg.docker_host)

        self._running = threading.Event()
        self._recv_thread: Optional[threading.Thread] = None
        self._leader_thread: Optional[threading.Thread] = None
        self._follower_thread: Optional[threading.Thread] = None

        self._leader_check_failures = 0
        self._max_leader_check_failures = 3
        self._leader_check_lock = threading.Lock()

    def start(self) -> None:
        if self._recv_thread is not None:
            return

        self._running.set()

        self._recv_thread = threading.Thread(
            target=self._recv_loop,
            name=f"HealthChecker-Recv-{self.cfg.node_id}",
            daemon=True
        )
        self._recv_thread.start()

        self._leader_thread = threading.Thread(
            target=self._leader_loop,
            name=f"HealthChecker-Leader-{self.cfg.node_id}",
            daemon=True
        )
        self._leader_thread.start()

        self._follower_thread = threading.Thread(
            target=self._follower_loop,
            name=f"HealthChecker-Follower-{self.cfg.node_id}",
            daemon=True
        )
        self._follower_thread.start()

        print(f"[health] HealthChecker iniciado en puerto {self.cfg.health_listen_port}")

    def stop(self) -> None:
        if not self._running.is_set():
            return

        print("[health] Deteniendo HealthChecker...")
        self._running.clear()

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        try:
            self.sock.close()
        except Exception:
            pass

        threads = [
            (self._recv_thread, "Recv"),
            (self._leader_thread, "Leader"),
            (self._follower_thread, "Follower")
        ]

        for thread, thread_name in threads:
            if thread and thread.is_alive():
                thread.join(timeout=2.0)
        
        try:
            self.reviver.close()
        except Exception as e:
            print(f"[health] Error cerrando reviver: {e}")

        self._recv_thread = None
        self._leader_thread = None
        self._follower_thread = None

    def _recv_loop(self) -> None:
        while self._running.is_set():
            try:
                data, addr = self.sock.recvfrom(64 * 1024)
            except socket.timeout:
                continue
            except OSError:
                if not self._running.is_set():
                    break
                continue

            try:
                msg = Message.from_json(data.decode("utf-8"))
                self._handle_health_message(msg, addr)
            except Exception as e:
                print(f"[health] Error procesando mensaje: {e}")

    def _handle_health_message(self, msg: Message, addr: tuple) -> None:
        kind = msg.kind

        if kind == "heartbeat":
            ack = Message(
                kind="heartbeat_ack",
                src_id=self.cfg.node_id,
                src_name=self.cfg.node_name,
                payload={}
            )
            try:
                self.sock.sendto(ack.to_json().encode("utf-8"), addr)
                print(f"[health] Respondido heartbeat_ack a {addr}")
            except Exception as e:
                print(f"[health] Error enviando ACK: {e}")

        elif kind == "heartbeat_ack":
            pass

        elif kind == "is_leader_alive":
            if self.is_leader():
                ack = Message(
                    kind="leader_alive_ack",
                    src_id=self.cfg.node_id,
                    src_name=self.cfg.node_name,
                    payload={}
                )
                try:
                    self.sock.sendto(ack.to_json().encode("utf-8"), addr)
                    print(f"[health] Respondido leader_alive_ack a {addr}")
                except Exception as e:
                    print(f"[health] Error enviando leader_alive_ack: {e}")

        elif kind == "leader_alive_ack":
            pass

    def _leader_loop(self) -> None:
        interval_s = self.cfg.heartbeat_interval_ms / 1000.0

        while self._running.is_set():
            if not self.is_leader():
                time.sleep(1.0)
                continue

            print(f"[health] Líder {self.cfg.node_name} enviando heartbeats...")

            for target in self.cfg.controller_targets:
                if not self._running.is_set():
                    break

                was_dead = False
                success = self._send_heartbeat_with_retry(target)

                if not success:
                    print(f"[health] Controlador {target.name} no responde. Intentando revivir...")
                    was_dead = True
                    revived = self._revive_controller(target)
                    
                    if revived:
                        print(f"[health] Notificando a {target.name} sobre liderazgo...")
                        time.sleep(2.0)
                        self.notify_revived_node(target.host, target.port)

            time.sleep(interval_s)

    def _send_heartbeat_with_retry(self, target: ControllerTarget) -> bool:
        timeout_s = self.cfg.heartbeat_timeout_ms / 1000.0
        max_retries = self.cfg.heartbeat_max_retries

        msg = Message(
            kind="heartbeat",
            src_id=self.cfg.node_id,
            src_name=self.cfg.node_name,
            payload={}
        )
        for attempt in range(1, max_retries + 1):
            if not self._running.is_set():
                return False

            try:
                self.sock.sendto(
                    msg.to_json().encode("utf-8"),
                    (target.host, target.port)
                )

                self.sock.settimeout(timeout_s)
                start = time.monotonic()

                while time.monotonic() - start < timeout_s:
                    try:
                        data, addr = self.sock.recvfrom(64 * 1024)
                        ack_msg = Message.from_json(data.decode("utf-8"))

                        if ack_msg.kind == "heartbeat_ack":
                            print(f"[health] {target.name} respondió (intento {attempt})")
                            self.sock.settimeout(0.5)
                            return True
                    except socket.timeout:
                        break
                    except Exception:
                        continue

                print(f"[health] {target.name} no respondió (intento {attempt}/{max_retries})")

            except Exception as e:
                print(f"[health] Error enviando a {target.name}: {e}")

            if attempt < max_retries:
                time.sleep(1.0)

        self.sock.settimeout(0.5)
        return False

    def _revive_controller(self, target: ControllerTarget) -> bool:
        print(f"[health] Reviviendo contenedor: {target.container_name}")
        success = self.reviver.revive_container(target.container_name)

        if success:
            print(f"[health] Contenedor {target.container_name} revivido")
        else:
            print(f"[health] Falló al revivir {target.container_name}")
        
        return success

    def _follower_loop(self) -> None:
        interval_s = self.cfg.leader_check_interval_ms / 1000.0

        while self._running.is_set():
            if self.is_leader():
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                time.sleep(1.0)
                continue

            time.sleep(interval_s)

            if not self.is_leader():
                self._check_leader_alive()

    def _check_leader_alive(self) -> None:
        leader_info = self.get_leader_info()

        if leader_info is None:
            print("[health] No hay líder conocido para verificar")
            with self._leader_check_lock:
                self._leader_check_failures = 0
            return

        host, port = leader_info
        timeout_s = self.cfg.leader_check_timeout_ms / 1000.0


        msg = Message(
            kind="is_leader_alive",
            src_id=self.cfg.node_id,
            src_name=self.cfg.node_name,
            payload={}
        )

        try:
            self.sock.sendto(msg.to_json().encode("utf-8"), (host, port))
            self.sock.settimeout(timeout_s)

            start = time.monotonic()
            received_ack = False
            
            while time.monotonic() - start < timeout_s:
                try:
                    data, addr = self.sock.recvfrom(64 * 1024)
                    ack_msg = Message.from_json(data.decode("utf-8"))

                    if ack_msg.kind == "leader_alive_ack":
                        print(f"[health] Líder está vivo")
                        self.sock.settimeout(0.5)
                        received_ack = True
                        with self._leader_check_lock:
                            self._leader_check_failures = 0
                        return
                except socket.timeout:
                    break
                except Exception:
                    continue

            if not received_ack:
                with self._leader_check_lock:
                    self._leader_check_failures += 1
                    failures = self._leader_check_failures
                
                print(f"[health] Líder no responde (fallo {failures}/{self._max_leader_check_failures})")
                
                if failures >= self._max_leader_check_failures:
                    print("[health] Máximo de fallos alcanzado, iniciando elección...")
                    self.sock.settimeout(0.5)
                    with self._leader_check_lock:
                        self._leader_check_failures = 0
                    self.on_leader_dead()
                else:
                    self.sock.settimeout(0.5)

        except OSError as e:
            with self._leader_check_lock:
                self._leader_check_failures += 1
                failures = self._leader_check_failures
            
            print(f"[health] Error de red contactando líder: {e} (fallo {failures}/{self._max_leader_check_failures})")
            
            if failures >= self._max_leader_check_failures:
                print("[health] Líder caido, iniciando elección...")
                self.sock.settimeout(0.5)
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                self.on_leader_dead()
            else:
                self.sock.settimeout(0.5)
                
        except Exception as e:
            print(f"[health] Error inesperado verificando líder: {e}")
            self.sock.settimeout(0.5)
