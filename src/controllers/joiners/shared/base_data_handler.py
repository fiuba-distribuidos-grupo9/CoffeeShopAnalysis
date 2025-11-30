import logging
import threading
from typing import Any, Callable, Optional

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message
from shared.duplicate_message_checker import DuplicateMessageChecker


class BaseDataHandler:

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._prev_controllers_eof_recv: dict[str, int] = {}
        self._prev_controllers_amount = consumers_config[
            "base_data_prev_controllers_amount"
        ]

        self._mom_consumer: RabbitMQMessageMiddlewareQueue = (
            self._build_mom_consumer_using(rabbitmq_host, consumers_config)
        )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        build_mom_consumer: Callable,
        base_data_by_session_id: dict[str, list[BatchMessage]],
        base_data_by_session_id_lock: Any,
        all_base_data_received: dict[str, bool],
        all_base_data_received_lock: Any,
        is_stopped: threading.Event,
    ) -> None:
        self._controller_id = controller_id

        self._build_mom_consumer_using = build_mom_consumer

        self._init_mom_consumers(rabbitmq_host, consumers_config)

        self._base_data_by_session_id = base_data_by_session_id
        self._base_data_by_session_id_lock = base_data_by_session_id_lock

        self._all_base_data_received = all_base_data_received
        self._all_base_data_received_lock = all_base_data_received_lock

        self.is_stopped = is_stopped

        self._prev_controllers_last_message: dict[int, Message] = {}
        self._duplicate_message_checker = DuplicateMessageChecker(self)

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | thread_name: {threading.current_thread().name}")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | thread_name: {threading.current_thread().name}")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | thread_name: {threading.current_thread().name}")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return not self.is_stopped.is_set()

    def mom_consumer(self) -> RabbitMQMessageMiddlewareQueue:
        return self._mom_consumer

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _load_last_state_if_exists(self) -> None:
        pass

    def _save_current_state(self) -> None:
        pass

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def is_duplicate_message(self, message: Message) -> bool:
        return self._duplicate_message_checker.is_duplicated(message)

    def _handle_base_data_batch_message(self, message: BatchMessage) -> None:
        session_id = message.session_id()
        with self._base_data_by_session_id_lock:
            self._base_data_by_session_id.setdefault(session_id, [])
            self._base_data_by_session_id[session_id].append(message)

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        del self._prev_controllers_eof_recv[session_id]

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_base_data_batch_eof(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        self._prev_controllers_eof_recv.setdefault(session_id, 0)
        self._prev_controllers_eof_recv[session_id] += 1
        self._log_debug(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if self._prev_controllers_eof_recv[session_id] == self._prev_controllers_amount:
            self._log_info(
                f"action: all_eofs_received | result: success | session_id: {session_id}"
            )

            with self._all_base_data_received_lock:
                self._all_base_data_received[session_id] = True

            self._clean_session_data_of(session_id)

    def _handle_base_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
            if isinstance(message, BatchMessage):
                self._handle_base_data_batch_message(message)
            elif isinstance(message, EOFMessage):
                self._handle_base_data_batch_eof(message)
            self._save_current_state()
        else:
            self._log_info(
                f"action: duplicate_message_ignored | result: success | message: {message}"
            )

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._log_info(f"action: handler_running | result: success")
        self._load_last_state_if_exists()
        self._mom_consumer.start_consuming(self._handle_base_data)

    def _close_all(self) -> None:
        self._mom_consumer.delete()
        self._mom_consumer.close()
        self._log_info(f"action: mom_consumer_close | result: success")

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(f"action: handler_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all()
            self._log_info(f"action: close_all | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info(f"action: handler_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info(f"action: handler_shutdown | result: success")
