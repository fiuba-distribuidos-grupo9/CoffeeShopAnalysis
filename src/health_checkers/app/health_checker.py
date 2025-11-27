from __future__ import annotations

import socket
import threading
import time
from typing import Optional, Callable
import logging

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

        logging.info(f"HealthChecker iniciado en puerto {self.cfg.health_listen_port}")

    def _sock_valid(self) -> bool:
        try:
            if self.sock is None:
                return False
            if getattr(self.sock, "_closed", False):
                return False
            if self.sock.fileno() == -1:
                return False
            return True
        except Exception:
            return False

    def stop(self) -> None:
        if not self._running.is_set():
            return

        logging.info("Deteniendo HealthChecker...")
        self._running.clear()

        threads = [
            (self._recv_thread, "Recv"),
            (self._leader_thread, "Leader"),
            (self._follower_thread, "Follower")
        ]

        for thread, thread_name in threads:
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=2.0)
                except Exception:
                    pass

        try:
            if self._sock_valid():
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self.sock.close()
                except Exception:
                    pass
        except Exception:
            try:
                self.sock.close()
            except Exception:
                pass
        
        try:
            self.reviver.close()
        except Exception as e:
            logging.error(f"Error cerrando reviver: {e}")

        self._recv_thread = None
        self._leader_thread = None
        self._follower_thread = None

    def _recv_loop(self) -> None:
        while self._running.is_set():
            try:
                if not self._sock_valid():
                    return

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
                logging.error(f"Error procesando mensaje: {e}")

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
                if not self._sock_valid():
                    return
                
                self.sock.sendto(ack.to_json().encode("utf-8"), addr)
                logging.info(f"Respondido heartbeat_ack a {addr}")
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return
            except Exception as e:
                logging.error(f"Error enviando ACK: {e}")

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
                    logging.info(f"Respondido leader_alive_ack a {addr}")
                except Exception as e:
                    logging.error(f"Error enviando leader_alive_ack: {e}")

        elif kind == "leader_alive_ack":
            pass

    def _leader_loop(self) -> None:
        interval_s = self.cfg.heartbeat_interval_ms / 1000.0

        while self._running.is_set():
            if not self.is_leader():
                time.sleep(1.0)
                continue

            logging.info(f"Líder {self.cfg.node_name} enviando heartbeats...")

            for target in self.cfg.controller_targets:
                if not self._running.is_set():
                    break

                success = self._send_heartbeat_with_retry(target)

                if not success:
                    logging.info(f"Controlador {target.name} no responde. Intentando revivir...")
                    revived = self._revive_controller(target)
                    
                    if revived:
                        logging.info(f"Notificando a {target.name} sobre liderazgo...")
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

            if not self._sock_valid():
                return False

            try:
                self.sock.sendto(
                    msg.to_json().encode("utf-8"),
                    (target.host, target.port)
                )
                try:
                    self.sock.settimeout(timeout_s)
                except OSError as e:
                    if getattr(e, "errno", None) == 9:
                        return False
                    
                start = time.monotonic()

                while time.monotonic() - start < timeout_s:
                    try:
                        data, addr = self.sock.recvfrom(64 * 1024)
                        ack_msg = Message.from_json(data.decode("utf-8"))

                        if ack_msg.kind == "heartbeat_ack":
                            logging.info(f"{target.name} respondió (intento {attempt})")
                            
                            try: 
                                self.sock.settimeout(0.5)
                            except Exception:
                                pass

                            return True
                    except socket.timeout:
                        break
                    except Exception:
                        continue

                logging.info(f"{target.name} no respondió (intento {attempt}/{max_retries})")

            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return False
                
            except Exception as e:
                logging.error(f"Error enviando a {target.name}: {e}")

            if attempt < max_retries:
                time.sleep(1.0)
        try:
            if self._sock_valid():
                self.sock.settimeout(0.5)
        except Exception:
            pass
        return False

    def _revive_controller(self, target: ControllerTarget) -> bool:
        logging.info(f"Reviviendo contenedor: {target.container_name}")
        success = self.reviver.revive_container(target.container_name)

        if success:
            logging.info(f"Contenedor {target.container_name} revivido")
        else:
            logging.info(f"Falló al revivir {target.container_name}")
        
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
            logging.info(f"No hay líder conocido para verificar")
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

        if not self._sock_valid():
            return

        try:
            try:
                self.sock.sendto(msg.to_json().encode("utf-8"), (host, port))
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return
                raise
            try:
                self.sock.settimeout(timeout_s)
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    return
                raise
                

            start = time.monotonic()
            received_ack = False
            
            while time.monotonic() - start < timeout_s:
                try:
                    data, addr = self.sock.recvfrom(64 * 1024)
                    ack_msg = Message.from_json(data.decode("utf-8"))

                    if ack_msg.kind == "leader_alive_ack":
                        logging.info(f"Líder está vivo")
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
                
                logging.info(f"Líder no responde (fallo {failures}/{self._max_leader_check_failures})")
                
                if failures >= self._max_leader_check_failures:
                    logging.info("Máximo de fallos alcanzado, iniciando elección...")
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
            
            logging.info(f"Error de red contactando líder: {e} (fallo {failures}/{self._max_leader_check_failures})")
            
            if failures >= self._max_leader_check_failures:
                logging.info("Líder caido, iniciando elección...")
                self.sock.settimeout(0.5)
                with self._leader_check_lock:
                    self._leader_check_failures = 0
                self.on_leader_dead()
            else:
                self.sock.settimeout(0.5)
                
        except Exception as e:
            logging.error(f"Error inesperado verificando líder: {e}")
            self.sock.settimeout(0.5)
