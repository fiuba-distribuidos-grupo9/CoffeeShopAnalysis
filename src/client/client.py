import logging
import signal
import socket
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Callable

from shared import constants, shell_cmd
from shared.communication_protocol import constants as cp_constants
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.handshake_message import HandshakeMessage, Message


class Client:

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        client_id: int,
        server_host: str,
        server_port: int,
        data_path: str,
        results_path: str,
        batch_max_size: int,
    ):
        self._client_id = client_id
        self._session_id = "<not_set>"

        self._server_host = server_host
        self._server_port = server_port

        self._data_path = Path(data_path)

        self._output_path = Path(results_path) / constants.QRS_FOLDER_NAME
        self._output_path.mkdir(parents=True, exist_ok=True)
        shell_cmd.shell_silent(f"rm -f {self._output_path}/*")

        self._batch_max_size = batch_max_size

        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self._set_client_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._temp_buffer = b""

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | session_id: {self._session_id}")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | session_id: {self._session_id}")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | session_id: {self._session_id}")

    # ============================== PRIVATE - RUNNING ============================== #

    def _is_running(self) -> bool:
        return self._client_running == True

    def _set_client_as_not_running(self) -> None:
        self._client_running = False

    def _set_client_as_running(self) -> None:
        self._client_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        self._log_info(f"action: sigterm_signal_handler | result: in_progress")

        self._set_client_as_not_running()

        self._client_socket.close()
        self._log_debug(f"action: sigterm_client_socket_close | result: success")

        self._log_info(f"action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - SEND/RECEIVE MESSAGES ============================== #

    def _socket_send_message(self, socket: socket.socket, message: str) -> None:
        self._log_debug(f"action: send_message | result: in_progress | msg: {message}")

        if self._is_running():
            socket.sendall(message.encode("utf-8"))
        else:
            logging.error(f"action: send_message_while_not_running | result: error")

        self._log_debug(f"action: send_message | result: success |  msg: {message}")

    def _socket_receive_message(self, socket: socket.socket) -> str:
        self._log_debug(f"action: receive_message | result: in_progress")

        buffsize = constants.KiB
        bytes_received = self._temp_buffer
        self._temp_buffer = b""

        all_data_received = False
        while not all_data_received:
            chunk = socket.recv(buffsize)
            if len(chunk) == 0:
                logging.error(
                    f"action: receive_message | result: fail | error: unexpected disconnection",
                )
                raise OSError("Unexpected disconnection of the server")

            self._log_debug(
                f"action: receive_chunk | result: success | chunk size: {len(chunk)}"
            )
            if chunk.endswith(cp_constants.MSG_END_DELIMITER.encode("utf-8")):
                all_data_received = True

            if cp_constants.MSG_END_DELIMITER.encode("utf-8") in chunk:
                index = chunk.rindex(cp_constants.MSG_END_DELIMITER.encode("utf-8"))
                bytes_received += chunk[: index + len(cp_constants.MSG_END_DELIMITER)]
                self._temp_buffer = chunk[index + len(cp_constants.MSG_END_DELIMITER) :]
                all_data_received = True
            else:
                bytes_received += chunk

        message = bytes_received.decode("utf-8")
        self._log_debug(f"action: receive_message | result: success | msg: {message}")
        return message

    # ============================== PRIVATE - READ CSV ============================== #

    def _parse_row_from_line(
        self, column_names: list[str], line: str
    ) -> dict[str, str]:
        row: dict[str, str] = {}

        fields = line.split(",")
        for i, column_name in enumerate(column_names):
            row[column_name] = fields[i]

        return row

    def _read_next_batch_from_file(
        self, file: TextIOWrapper, column_names: list[str]
    ) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        batch_size = 0
        eof_reached = False

        while not eof_reached and batch_size < self._batch_max_size:
            row = {}

            line = file.readline().strip()
            if not line:
                eof_reached = True
                continue

            row = self._parse_row_from_line(column_names, line)
            batch.append(row)
            batch_size += 1

        return batch

    # ============================== PRIVATE - PATHS SUPPORT ============================== #

    def _assert_is_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            raise ValueError(f"Data path error: {path} is not a file")

    def _assert_is_dir(self, path: Path) -> None:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Data path error: {path} is not a folder")

    def _folder_path(self, folder_name: str) -> Path:
        folder_path = self._data_path / folder_name
        self._assert_is_dir(folder_path)
        return folder_path

    # ============================== PRIVATE - SEND/RECV HANDSHAKE ============================== #

    def _send_handshake_message(self) -> None:
        handshake_message = HandshakeMessage(
            str(self._client_id), cp_constants.ALL_QUERIES
        )
        self._socket_send_message(self._client_socket, str(handshake_message))
        self._log_info(f"action: send_handshake | result: success")

    def _receive_handshake_ack_message(self) -> None:
        received_message = self._socket_receive_message(self._client_socket)

        handshake_message = HandshakeMessage.from_str(received_message)
        client_id = handshake_message.payload()
        if client_id != str(self._client_id):
            raise ValueError(
                f"Handshake ACK message error: expected client_id {self._client_id}, received {client_id}"
            )

        self._session_id = handshake_message.id()
        self._log_info(f"action: receive_handshake_ack | result: success")

    # ============================== PRIVATE - SEND DATA ============================== #

    def _send_data_from_file_using_batchs(
        self,
        folder_name: str,
        file: TextIOWrapper,
        message_type: str,
    ) -> None:
        column_names_line = file.readline().strip()
        column_names = column_names_line.split(",")

        batch_items = self._read_next_batch_from_file(file, column_names)
        while len(batch_items) != 0 and self._is_running():
            self._log_debug(f"action: {folder_name}_batch | result: in_progress")
            message = BatchMessage(
                message_type=message_type,
                session_id=self._session_id,
                message_id=None,
                controller_id=None,
                batch_items=batch_items,
            )
            self._socket_send_message(self._client_socket, str(message))
            self._log_debug(f"action: {folder_name}_batch | result: success")

            batch_items = self._read_next_batch_from_file(file, column_names)

    def _send_data_from_all_files_using_batchs(
        self,
        folder_name: str,
        message_type: str,
    ) -> None:
        for file_path in self._folder_path(folder_name).iterdir():
            if not file_path.name.lower().endswith(".csv"):
                logging.warning(
                    f"action: {folder_name}_file_skip | result: success | file: {file_path} | reason: not_csv"
                )
                continue
            self._assert_is_file(file_path)
            csv_file = open(file_path, "r", encoding="utf-8", buffering=constants.KiB)
            try:
                self._send_data_from_file_using_batchs(
                    folder_name,
                    csv_file,
                    message_type,
                )
            finally:
                csv_file.close()
                self._log_debug(
                    f"action: {folder_name}_file_close | result: success | file: {file_path}"
                )

        eof_message = EOFMessage(
            session_id=self._session_id,
            message_id=None,
            controller_id=None,
            batch_message_type=message_type,
        )
        self._socket_send_message(self._client_socket, str(eof_message))
        self._log_info(f"action: {folder_name}_all_files_sent | result: success")

    def _send_all_menu_items(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.MIT_FOLDER_NAME,
            cp_constants.MENU_ITEMS_BATCH_MSG_TYPE,
        )

    def _send_all_stores(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.STR_FOLDER_NAME,
            cp_constants.STORES_BATCH_MSG_TYPE,
        )

    def _send_all_transaction_items(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.TIT_FOLDER_NAME,
            cp_constants.TRANSACTION_ITEMS_BATCH_MSG_TYPE,
        )

    def _send_all_transactions(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.TRN_FOLDER_NAME,
            cp_constants.TRANSACTIONS_BATCH_MSG_TYPE,
        )

    def _send_all_users(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.USR_FOLDER_NAME,
            cp_constants.USERS_BATCH_MSG_TYPE,
        )

    def _send_all_data(self) -> None:
        # WARNING: do not modify order
        self._send_all_menu_items()
        self._send_all_stores()
        self._send_all_users()
        self._send_all_transactions()
        self._send_all_transaction_items()
        self._log_info(f"action: all_data_sent | result: success")

    # ============================== PRIVATE - SEND DATA ============================== #

    def _handle_query_result_message(self, message: BatchMessage) -> None:
        message_type = message.message_type()
        self._log_debug(
            f"action: {message_type}_receive_query_result | result: success"
        )
        file_name = (
            f"client_{self._client_id}__{self._session_id}__{message_type}_result.txt"
        )
        for batch_item in message.batch_items():
            shell_cmd.shell_silent(
                f"echo '{",".join(batch_item.values())}' >> {self._output_path / file_name}"
            )
            self._log_debug(
                f"action: {message_type}_save_query_result | result: success | file: {file_name}",
            )

    def _handle_query_result_eof_message(
        self, message: EOFMessage, all_eof_received: dict
    ) -> None:
        data_type = message.batch_message_type()
        if data_type not in all_eof_received:
            raise ValueError(f"Unknown EOF message type {data_type}")

        all_eof_received[data_type] = True
        self._log_info(
            f"action: eof_{data_type}_receive_query_result | result: success"
        )

    def _handle_server_message(self, message: Message, all_eof_received: dict) -> None:
        if isinstance(message, BatchMessage):
            self._handle_query_result_message(message)
        elif isinstance(message, EOFMessage):
            self._handle_query_result_eof_message(message, all_eof_received)

    def _with_each_message_do(
        self,
        received_message: str,
        callback: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        messages = received_message.split(cp_constants.MSG_END_DELIMITER)
        for message in messages:
            if not self._is_running():
                break
            if message == "":
                continue
            message += cp_constants.MSG_END_DELIMITER
            callback(Message.suitable_for_str(message), *args, **kwargs)

    def _receive_all_query_results_from_server(self) -> None:
        all_eof_received = {
            constants.QUERY_RESULT_1X: False,
            constants.QUERY_RESULT_21: False,
            constants.QUERY_RESULT_22: False,
            constants.QUERY_RESULT_3X: False,
            constants.QUERY_RESULT_4X: False,
        }

        while not all(all_eof_received.values()):
            if not self._is_running():
                return

            received_message = self._socket_receive_message(self._client_socket)
            self._with_each_message_do(
                received_message,
                self._handle_server_message,
                all_eof_received,
            )

        self._log_info(f"action: all_query_results_received | result: success")

    # ============================== PRIVATE - HANDLE SERVER CONNECTION ============================== #

    def _handle_server_connection(self) -> None:
        self._send_handshake_message()
        self._receive_handshake_ack_message()

        self._send_all_data()

        self._receive_all_query_results_from_server()

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info(f"action: client_startup | result: success")

        self._set_client_as_running()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._client_socket.connect((self._server_host, self._server_port))
            self._handle_server_connection()
        except Exception as e:
            logging.error(f"action: client_run | result: fail | error: {e}")
            raise e
        finally:
            server_socket.close()
            self._log_debug("action: server_socket_close | result: success")

        self._log_info(f"action: client_shutdown | result: success")
