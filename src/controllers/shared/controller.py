import logging
import multiprocessing
import os
import random
import signal
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from shared.heartbeat_process import HeartbeatProcess


class Controller(ABC):

    # ============================== INITIALIZE ============================== #

    def __init__(self, controller_id: int) -> None:
        self._controller_id = controller_id

        self.is_stopped = threading.Event()
        self._set_controller_as_stopped()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._heartbeat_process: Optional[multiprocessing.Process] = None
        self._heartbeat_process_port = 9201  # @TODO read from config

        self._random_exit_active = False

    # ============================== PRIVATE - EXIT ============================== #

    def _random_exit_with_error(self, message: str, prob: int = 1) -> None:
        if not self._random_exit_active:
            return
        random_value = random.randint(1, 1000)
        if random_value <= prob:
            self._log_error(f"action: exit_1 | result: error | message: {message}")
            os._exit(1)

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: main_process")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: main_process")

    def _log_warning(self, text: str) -> None:
        logging.warning(f"{text} | pid: main_process")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: main_process")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return not self.is_stopped.is_set()

    def _set_controller_as_stopped(self) -> None:
        self.is_stopped.set()

    def _set_controller_as_running(self) -> None:
        self.is_stopped.clear()

    # ============================== PRIVATE - HANDLE PROCESSES ============================== #

    def _terminate_all_processes(self) -> None:
        if self._heartbeat_process:
            process = self._heartbeat_process
            if process.is_alive():
                process.terminate()
                self._log_debug(
                    f"action: terminate_process | result: success | pid: {process.pid}"
                )

        self._log_info("action: all_processes_terminate | result: success")

    def _join_all_processes(self) -> None:
        if self._heartbeat_process:
            process = self._heartbeat_process
            process.join()
            pid = process.pid
            exitcode = process.exitcode
            self._log_info(
                f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
            )

    def _close_all_processes(self) -> list[tuple[int, int]]:
        uncaught_exceptions = []
        if self._heartbeat_process:
            process = self._heartbeat_process
            pid = process.pid
            exitcode = process.exitcode
            if exitcode != 0:
                uncaught_exceptions.append((pid, exitcode))

            process.close()
            self._log_debug(f"action: close_process | result: success | pid: {pid}")
        return uncaught_exceptions  # type: ignore

    def _join_non_alive_processes(self) -> None:
        if self._heartbeat_process:
            process = self._heartbeat_process
            if not process.is_alive():
                process.join()
                pid = process.pid
                exitcode = process.exitcode
                self._log_info(
                    f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
                )

                if exitcode != 0:
                    self._log_error(
                        f"action: process_error | result: error | pid: {pid} | exitcode: {exitcode}"
                    )
                else:
                    process.close()
                    self._heartbeat_process = None
                    self._log_info(
                        f"action: close_process | result: success | pid: {pid}"
                    )
                self._set_controller_as_stopped()
                self._stop()

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigchld_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info("action: sigchld_signal_handler | result: in_progress")

        self._join_non_alive_processes()

        self._log_info("action: sigchld_signal_handler | result: success")

    @abstractmethod
    def _stop(self) -> None:
        raise NotImplementedError("subclass resposability")

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info("action: sigterm_signal_handler | result: in_progress")

        self._set_controller_as_stopped()
        self._stop()

        self._log_info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - HEARTBEAT ============================== #

    def _handle_heartbeat(self) -> None:
        HeartbeatProcess(self._heartbeat_process_port).run()

    def _handle_heartbeat_spawning_process(self) -> None:
        process = multiprocessing.Process(target=self._handle_heartbeat, args=())
        process.start()
        self._heartbeat_process = process
        self._log_info(f"action: spawn_heartbeat_process | result: success")

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_controller_as_running()
        self._log_info("action: controller_running | result: success")
        self._handle_heartbeat_spawning_process()

    def _close_heartbeat_process(self) -> None:
        self._terminate_all_processes()
        self._join_all_processes()
        uncaught_exceptions = self._close_all_processes()
        if len(uncaught_exceptions) != 0:
            uncaught_exceptions_as_str = ", ".join(
                [
                    f"(pid: {pid}, exitcode: {exitcode})"
                    for pid, exitcode in uncaught_exceptions
                ]
            )
            raise Exception(
                f"Some processes exited with errors: {uncaught_exceptions_as_str}"
            )

    def _close_all(self) -> None:
        self._close_heartbeat_process()

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(f"action: controller_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all()
            self._log_info("action: close_all | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info("action: controller_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info("action: controller_shutdown | result: success")
