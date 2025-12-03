import logging
import os
import signal
import socket
from typing import Any, Callable
from .models import Message


class HeartbeatProcess:

    # ============================== INITIALIZE ============================== #

    def __init__(self, port: int) -> None:
        self._port = port

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("", port))

        self._set_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: {os.getpid()} | heartbeat_process")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: {os.getpid()} | heartbeat_process")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: {os.getpid()} | heartbeat_process")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._process_running

    def _set_as_not_running(self) -> None:
        self._process_running = False

    def _set_as_running(self) -> None:
        self._process_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info(f"action: sigterm_signal_handler | result: in_progress")

        self._set_as_not_running()

        self._health_socket.close()

        self._log_info(f"action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_as_running()
        self._log_info(f"action: heartbeat_process_running | result: success")

        while self._is_running():
            try:
                data, addr = self._sock.recvfrom(64 * 1024)
                msg = Message.from_json(data.decode("utf-8"))
                
                if msg.kind != "heartbeat":
                    continue

                ack = Message(
                    kind="heartbeat_ack",
                    src_id=0,
                    src_name="",
                    payload={}
                )

                data = ack.to_json().encode("utf-8")
                self._sock.sendto(data, addr)
                
            except socket.timeout:
                return None
            except OSError as e:
                errno = getattr(e, "errno", None)
                if errno == 9:
                    self._closed = True
                elif errno == -2:
                    logging.warning(
                        f"action: send_message | result: fail | reason: dns_error")
                else:
                    logging.error(
                        f"action: send_message | result: fail |  error: {e}"
                    )
                break
            except Exception as e:
                logging.error(
                    f"action: receive_message | result: fail | error: {e}"
                )
                break
            finally:
                self._close_socket()
    
    def _close_socket(self) -> None:
        try:
            if self._sock is None:
                return
            if self._closed:
                return
            if getattr(self._sock, "_closed", False):
                return
            if self._sock.fileno() == -1:
                return
            
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                    pass
            try:
                self._sock.close()
            except Exception:
                    pass
            return
        except Exception:
            return

    def _close_all(self) -> None:
        self._close_socket()

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(
                f"action: heartbeat_process_run | result: fail | error: {e}"
            )
            raise e
        finally:
            self._close_all()
            self._log_info(f"action: close_all | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info(f"action: heartbeat_process_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info(f"action: heartbeat_process_shutdown | result: success")
