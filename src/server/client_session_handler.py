import logging
import os
import signal
import socket
import uuid
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import constants
from shared.communication_protocol import constants as cp_constants
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.handshake_message import HandshakeMessage
from shared.communication_protocol.message import Message
from shared.simple_hash import simple_hash


class ClientSessionHandler:

    # ============================== INITIALIZE ============================== #

    def _init_cleaners_data(self, cleaners_data: dict) -> None:
        self._cleaners_data = cleaners_data

    def _init_output_builders_data(self, output_builders_data: dict) -> None:
        self._output_builders_data = output_builders_data

    def _init_client_data_batch_stats(self) -> None:
        self._client_eof_received = {
            cp_constants.MENU_ITEMS_BATCH_MSG_TYPE: False,
            cp_constants.STORES_BATCH_MSG_TYPE: False,
            cp_constants.TRANSACTION_ITEMS_BATCH_MSG_TYPE: False,
            cp_constants.TRANSACTIONS_BATCH_MSG_TYPE: False,
            cp_constants.USERS_BATCH_MSG_TYPE: False,
        }

    def _init_output_builders_data_stats(self) -> None:
        self._output_builders_eof_received = {
            constants.QUERY_RESULT_1X: 0,
            constants.QUERY_RESULT_21: 0,
            constants.QUERY_RESULT_22: 0,
            constants.QUERY_RESULT_3X: 0,
            constants.QUERY_RESULT_4X: 0,
        }

    def _init_mom_producers(self, rabbitmq_host: str) -> None:
        self._mom_cleaners_connections: dict[
            str, list[RabbitMQMessageMiddlewareQueue]
        ] = {}

        for data_type, cleaner_data in self._cleaners_data.items():
            workers_amount = cleaner_data[constants.WORKERS_AMOUNT]
            for id in range(workers_amount):
                queue_name = f"{cleaner_data[constants.QUEUE_PREFIX]}-{id}"
                queue_producer = RabbitMQMessageMiddlewareQueue(
                    rabbitmq_host, queue_name
                )

                if self._mom_cleaners_connections.get(data_type) is None:
                    self._mom_cleaners_connections[data_type] = []
                self._mom_cleaners_connections[data_type].append(queue_producer)

    def _init_mom_consumers(self, rabbitmq_host: str) -> None:
        (_, output_builder_data) = next(iter(self._output_builders_data.items()))
        queue_name = f"{output_builder_data[constants.QUEUE_PREFIX]}-{self._session_id}"
        queue_consumer = RabbitMQMessageMiddlewareQueue(rabbitmq_host, queue_name)
        self._mom_output_builders_connection = queue_consumer

    def __init__(
        self,
        client_socket: socket.socket,
        session_id: str,
        rabbitmq_host: str,
        cleaners_data: dict,
        output_builders_data: dict,
    ) -> None:
        self._client_socket = client_socket
        self._session_id = session_id
        self._controller_id = 0

        self._set_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._rabbitmq_host = rabbitmq_host

        self._init_cleaners_data(cleaners_data)
        self._init_output_builders_data(output_builders_data)

        self._init_client_data_batch_stats()
        self._init_output_builders_data_stats()

        self._init_mom_producers(rabbitmq_host)
        self._init_mom_consumers(rabbitmq_host)

        self._temp_buffer = b""

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

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

        self._client_socket.close()
        self._log_debug(f"action: sigterm_client_socket_close | result: success")

        self._mom_output_builders_connection.stop_consuming()
        self._log_debug(f"action: sigterm_mom_stop_consuming | result: success")

        self._log_info(f"action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - SOCKET SEND/RECEIVE MESSAGES ============================== #

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
                self._log_error(
                    f"action: receive_message | result: fail | error: unexpected disconnection",
                )
                raise OSError(f"Unexpected disconnection of the client")

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

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        data_type = message.message_type()
        mom_producers = self._mom_cleaners_connections[data_type]

        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(mom_producers)
        mom_producer = mom_producers[hash]
        mom_producer.send(str(message))

    def _mom_send_message_through_all_producers(self, message: EOFMessage) -> None:
        data_type = message.batch_message_type()

        mom_producers = self._mom_cleaners_connections[data_type]
        for mom_producer in mom_producers:
            message.update_message_id(uuid.UUID(int=0).hex)
            message.update_controller_id(str(self._controller_id))
            mom_producer.send(str(message))

    # ============================== PRIVATE - RECEIVE CLIENT HANDSHAKE ============================== #

    def _send_client_handshake_message(
        self, client_socket: socket.socket, client_id: str
    ) -> None:
        handshake_message = HandshakeMessage(self._session_id, client_id)
        self._socket_send_message(client_socket, str(handshake_message))
        self._log_info(
            f"action: handshake_response_sent | result: success | client_id: {client_id}"
        )

    def _accept_client_handshake_message(self, client_socket: socket.socket) -> None:
        received_message = self._socket_receive_message(client_socket)

        handshake_message = HandshakeMessage.from_str(received_message)
        client_id = handshake_message.id()
        if handshake_message.payload() != cp_constants.ALL_QUERIES:
            raise ValueError(
                f"Invalid handshake payload received from client: {handshake_message.payload()}"
            )
        self._log_info(
            f"action: handshake_received | result: success | client_id: {client_id}"
        )

        self._send_client_handshake_message(client_socket, client_id)

    # ============================== PRIVATE - RECEIVE CLIENT DATA ============================== #

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        message.update_message_id(uuid.uuid4().hex)
        message.update_controller_id(str(self._controller_id))

        self._mom_send_message_to_next(message)

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        data_type = message.batch_message_type()
        if data_type not in self._client_eof_received:
            raise ValueError(
                f'Invalid EOF message type received from client "{data_type}"'
            )
        self._client_eof_received[data_type] = True
        self._log_info(f"action: {data_type}_eof_received | result: success")

        self._mom_send_message_through_all_producers(message)
        self._log_info(f"action: {data_type}_eof_sent | result: success")

    def _handle_client_message(self, message: Message) -> None:
        if isinstance(message, BatchMessage):
            self._handle_data_batch_message(message)
        elif isinstance(message, EOFMessage):
            self._handle_data_batch_eof_message(message)

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

    def _receive_all_data_from_client(self, client_socket: socket.socket) -> None:
        while not all(self._client_eof_received.values()):
            if not self._is_running():
                return

            received_message = self._socket_receive_message(client_socket)
            self._with_each_message_do(
                received_message,
                self._handle_client_message,
            )

        self._log_info(f"action: all_data_received | result: success")

    # ============================== PRIVATE - RECEIVE RESULTS ============================== #

    def _all_eof_received_from_output_builders(self) -> bool:
        for data_type, eof_received in self._output_builders_eof_received.items():
            workers_amount = self._output_builders_data[data_type][
                constants.WORKERS_AMOUNT
            ]
            if eof_received < workers_amount:
                return False
        return True

    def _handle_query_result_batch_message(
        self, client_socket: socket.socket, message: BatchMessage
    ) -> None:
        self._socket_send_message(client_socket, str(message))

    def _handle_query_result_eof_message(
        self, client_socket: socket.socket, message: EOFMessage
    ) -> None:
        data_type = message.batch_message_type()
        if data_type not in self._output_builders_eof_received:
            raise ValueError(
                f'Invalid EOF message type received from output builder "{data_type}"'
            )
        self._output_builders_eof_received[data_type] += 1
        self._log_info(f"action: eof_{data_type}_result_received | result: success")

        workers_amount = self._output_builders_data[data_type][constants.WORKERS_AMOUNT]
        if self._output_builders_eof_received[data_type] == workers_amount:
            self._socket_send_message(client_socket, str(message))
            self._log_info(
                f"action: eof_{data_type}_results_to_client_sent | result: success"
            )

    def _handle_output_builder_message(self, client_socket: socket.socket) -> Callable:
        def _on_message_callback(message_as_bytes: bytes) -> None:
            message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
            if isinstance(message, BatchMessage):
                self._handle_query_result_batch_message(client_socket, message)
            elif isinstance(message, EOFMessage):
                self._handle_query_result_eof_message(client_socket, message)

            if self._all_eof_received_from_output_builders():
                self._mom_output_builders_connection.stop_consuming()
                self._log_info(f"action: all_results_received | result: success")

        return _on_message_callback

    def _receive_all_query_results_from_output_builders(
        self, client_socket: socket.socket
    ) -> None:
        self._mom_output_builders_connection.start_consuming(
            self._handle_output_builder_message(client_socket)
        )

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def _handle_client_connection(self, client_socket: socket.socket) -> None:
        self._accept_client_handshake_message(client_socket)
        self._receive_all_data_from_client(client_socket)
        self._receive_all_query_results_from_output_builders(client_socket)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_as_running()
        self._log_info(f"action: client_session_handler_running | result: success")

        self._handle_client_connection(self._client_socket)

    def _close_all(self) -> None:
        for mom_cleaner_connections in self._mom_cleaners_connections.values():
            for mom_cleaner_connection in mom_cleaner_connections:
                mom_cleaner_connection.close()
                self._log_debug(
                    f"action: mom_cleaner_connection_close | result: success"
                )

        self._mom_output_builders_connection.delete()
        self._mom_output_builders_connection.close()
        self._log_debug(
            f"action: mom_output_builder_connection_close | result: success"
        )

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(
                f"action: client_session_handler_run | result: fail | error: {e}"
            )
            raise e
        finally:
            self._client_socket.close()
            self._log_debug(f"action: client_socket_close | result: success")

            self._close_all()
            self._log_info(f"action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info(f"action: client_session_handler_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info(f"action: client_session_handler_shutdown | result: success")
