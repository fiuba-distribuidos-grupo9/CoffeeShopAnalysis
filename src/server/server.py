import logging
import multiprocessing
import signal
import socket
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional

from server.client_session_cleaner import ClientSessionCleaner
from server.client_session_handler import ClientSessionHandler
from shared.file_protocol.atomic_writer import AtomicWriter
from shared.file_protocol.metadata_sections.client_session_ids import ClientSessionIds
from shared.file_protocol.metadata_reader import MetadataReader
from shared.heartbeat_process import HeartbeatProcess


class Server:

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        rabbitmq_host: str,
        cleaners_data: dict,
        output_builders_data: dict,
    ) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        self._set_server_as_stopped()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)
        signal.signal(signal.SIGCHLD, self._sigchld_signal_handler)

        self._rabbitmq_host = rabbitmq_host
        self._cleaners_data = cleaners_data
        self._output_builders_data = output_builders_data

        self._client_spawned_processes: dict[str, multiprocessing.Process] = {}

        self._heartbeat_process: Optional[multiprocessing.Process] = None
        self._heartbeat_process_port = 9201  # @TODO read from config

        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

        self._metadata_file_name = Path("metadata.txt")

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: main_process")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: main_process")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: main_process")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._server_running

    def _set_server_as_stopped(self) -> None:
        self._server_running = False

    def _set_server_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - HANDLE PROCESSES ============================== #

    def _terminate_all_processes(self) -> None:
        for _, process in self._client_spawned_processes.items():
            if process.is_alive():
                process.terminate()
                self._log_debug(
                    f"action: terminate_process | result: success | pid: {process.pid}"
                )

        if self._heartbeat_process:
            process = self._heartbeat_process
            if process.is_alive():
                process.terminate()
                self._log_debug(
                    f"action: terminate_process | result: success | pid: {process.pid}"
                )

        self._log_info("action: all_processes_terminate | result: success")

    def _join_all_processes(self) -> None:
        for _, process in self._client_spawned_processes.items():
            process.join()
            pid = process.pid
            exitcode = process.exitcode
            self._log_info(
                f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
            )

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

        for _, process in self._client_spawned_processes.items():
            pid = process.pid
            exitcode = process.exitcode
            if exitcode != 0:
                uncaught_exceptions.append((pid, exitcode))

            process.close()
            self._log_debug(f"action: close_process | result: success | pid: {pid}")

        if self._heartbeat_process:
            process = self._heartbeat_process
            pid = process.pid
            exitcode = process.exitcode
            if exitcode != 0:
                uncaught_exceptions.append((pid, exitcode))

            process.close()
            self._log_debug(f"action: close_process | result: success | pid: {pid}")

        return uncaught_exceptions

    def _join_non_alive_processes(self) -> None:
        processes_to_close: list[tuple[str, multiprocessing.Process]] = []
        for session_id, process in self._client_spawned_processes.items():
            if not process.is_alive():
                process.join()
                pid = process.pid
                exitcode = process.exitcode
                self._log_info(
                    f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
                )

                processes_to_close.append((session_id, process))
                if exitcode != 0:
                    self._clean_up_zombie_session(session_id)
                    self._log_error(
                        f"action: process_error | result: error | pid: {pid} | exitcode: {exitcode}"
                    )
                else:
                    self._log_info(
                        f"action: close_process | result: success | pid: {pid}"
                    )

        for session_id, process in processes_to_close:
            process.close()
            self._client_spawned_processes.pop(session_id)
        self._save_current_state()

        if self._heartbeat_process:
            process = self._heartbeat_process
            if not process.is_alive():
                process.join()
                pid = process.pid
                exitcode = process.exitcode
                self._log_info(
                    f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
                )

                process.close()
                self._heartbeat_process = None
                if exitcode != 0:
                    self._log_error(
                        f"action: process_error | result: error | pid: {pid} | exitcode: {exitcode}"
                    )
                else:
                    self._log_info(
                        f"action: close_process | result: success | pid: {pid}"
                    )
                self._set_server_as_stopped()
                self._stop()

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigchld_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info("action: sigchld_signal_handler | result: in_progress")

        self._join_non_alive_processes()

        self._log_info("action: sigchld_signal_handler | result: success")

    def _stop(self) -> None:
        self._server_socket.close()
        self._log_debug("action: server_socket_close | result: success")

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info("action: sigterm_signal_handler | result: in_progress")

        self._set_server_as_stopped()
        self._stop()

        self._log_info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - ACCEPT CONNECTION ============================== #

    def _accept_new_connection(self) -> Optional[socket.socket]:
        client_connection: Optional[socket.socket] = None
        try:
            self._log_info(
                "action: accept_connections | result: in_progress",
            )
            client_connection, addr = self._server_socket.accept()
            self._log_info(
                f"action: accept_connections | result: success | ip: {addr[0]}",
            )
            return client_connection
        except OSError as e:
            if client_connection is not None:
                client_connection.shutdown(socket.SHUT_RDWR)
                client_connection.close()
                self._log_debug("action: client_connection_close | result: success")
            self._log_error(f"action: accept_connections | result: fail | error: {e}")
            return None

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _load_zombie_sessions(self) -> list[str]:
        session_ids = []

        path = self._metadata_file_name
        logging.info(
            f"action: load_zombie_sessions | result: in_progress | file: {path}"
        )

        metadata_sections = self._metadata_reader.read_from(path)
        for metadata_section in metadata_sections:
            if isinstance(metadata_section, ClientSessionIds):
                session_ids = metadata_section.client_session_ids()
            else:
                logging.warning(
                    f"action: unknown_metadata_section | result: error | section: {metadata_section}"
                )

        logging.info(
            f"action: load_zombie_sessions | result: success | file: {path}",
        )

        return session_ids

    def _save_new_session_on_current_state(self, new_session_id: str) -> None:
        client_session_ids = []
        client_session_ids.append(new_session_id)
        client_session_ids.extend(self._client_spawned_processes.keys())
        self._atomic_writer.write(
            self._metadata_file_name, str(ClientSessionIds(client_session_ids))
        )

    def _save_current_state(self) -> None:
        client_session_ids = []
        client_session_ids.extend(self._client_spawned_processes.keys())
        self._atomic_writer.write(
            self._metadata_file_name, str(ClientSessionIds(client_session_ids))
        )

    def _clean_up_zombie_session(self, session_id: str) -> None:
        self._log_info(
            f"action: clean_zombie_session | result: in_progress | session_id: {session_id}"
        )
        ClientSessionCleaner(
            session_id,
            self._rabbitmq_host,
            self._cleaners_data,
            self._output_builders_data,
        ).run()
        self._log_info(
            f"action: clean_zombie_session | result: success | session_id: {session_id}"
        )

    def _clean_up_zombie_sessions(self) -> None:
        session_ids = self._load_zombie_sessions()
        for session_id in session_ids:
            try:
                self._clean_up_zombie_session(session_id)
            except Exception as e:
                self._log_error(
                    f"action: clean_zombie_session | result: error | session_id: {session_id} | error: {e}"
                )
        self._save_current_state()

    def _clean_up_zombie_sessions_if_any(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            self._clean_up_zombie_sessions()
        else:
            logging.info(
                f"action: clean_up_zombie_sessions_skipped | result: success | file: {path}"
            )

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def _handle_client_connection(
        self, client_socket: socket.socket, session_id: str
    ) -> None:
        ClientSessionHandler(
            client_socket,
            session_id,
            self._rabbitmq_host,
            self._cleaners_data,
            self._output_builders_data,
        ).run()

    def _handle_client_connection_spawning_process(
        self, client_socket: socket.socket
    ) -> None:
        session_id = uuid.uuid4().hex
        self._save_new_session_on_current_state(session_id)
        process = multiprocessing.Process(
            target=self._handle_client_connection, args=(client_socket, session_id)
        )
        process.start()
        self._client_spawned_processes[session_id] = process
        self._log_info(f"action: spawn_client_connection_process | result: success")

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
        self._set_server_as_running()
        self._handle_heartbeat_spawning_process()
        self._clean_up_zombie_sessions_if_any()

        while self._is_running():
            client_socket = self._accept_new_connection()
            if client_socket is None:
                continue

            self._handle_client_connection_spawning_process(client_socket)

    def _deny_exitcode_with_error(
        self, exitcode: Optional[int], pid: Optional[int]
    ) -> None:
        if exitcode != 0:
            self._log_error(
                f"action: process_error | result: error | pid: {pid} | exitcode: {exitcode}"
            )
            raise Exception(f"Process {pid} exited with code {exitcode}")

    def _close_all(self) -> None:
        self._server_socket.close()
        self._log_debug("action: server_socket_close | result: success")

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

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all()
            self._log_info("action: close_all | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info("action: server_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info("action: server_shutdown | result: success")
