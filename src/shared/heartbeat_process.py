import logging
import os
import signal
import time
from typing import Any, Callable


class HeartbeatProcess:

    # ============================== INITIALIZE ============================== #

    def __init__(self, port: int) -> None:
        self._port = port

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

        # TODO: we should have a socket to listen for UPD connections
        # we must close it when SIGTERM is received
        # self._client_socket.close()
        # self._log_debug(f"action: sigterm_client_socket_close | result: success")

        self._log_info(f"action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_as_running()
        self._log_info(f"action: heartbeat_process_running | result: success")

        while self._is_running():
            # this should be listening and then send ack
            sleep_interval = 10
            self._log_info(f"action: heartbeat! | interval: {sleep_interval}s")
            time.sleep(sleep_interval)

    def _close_all(self) -> None:
        pass

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
