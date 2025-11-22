import logging
import threading
from typing import Any, Callable, Union

from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message


class StreamDataHandler:

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._eof_recv_from_prev_controllers: dict[str, int] = {}
        self._prev_controllers_amount = consumers_config[
            "stream_data_prev_controllers_amount"
        ]

        self._mom_consumer: RabbitMQMessageMiddlewareQueue = (
            self._build_mom_consumer_using(rabbitmq_host, consumers_config)
        )

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers: list[MessageMiddleware] = []

        next_controllers_amount = producers_config["next_controllers_amount"]
        for producer_id in range(next_controllers_amount):
            mom_producer = self._build_mom_producer_using(
                rabbitmq_host, producers_config, producer_id
            )
            self._mom_producers.append(mom_producer)

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        build_mom_consumer: Callable,
        build_mom_producer: Callable,
        base_data_by_session_id: dict[str, list[dict[str, Any]]],
        base_data_by_session_id_lock: Any,
        all_base_data_received: dict[str, bool],
        all_base_data_received_lock: Any,
        join_key: str,
        transform_function: Callable,
        is_stopped: threading.Event,
    ) -> None:
        self._controller_id = controller_id

        self._build_mom_consumer_using = build_mom_consumer
        self._build_mom_producer_using = build_mom_producer

        self._init_mom_consumers(rabbitmq_host, consumers_config)
        self._init_mom_producers(rabbitmq_host, producers_config)

        self._join_key = join_key
        self._transform_function = transform_function

        self._stream_data_buffer_by_session_id: dict[str, list[BatchMessage]] = {}

        self._base_data_by_session_id = base_data_by_session_id
        self._base_data_by_session_id_lock = base_data_by_session_id_lock

        self._all_base_data_received = all_base_data_received
        self._all_base_data_received_lock = all_base_data_received_lock

        self.is_stopped = is_stopped

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | thread_name: {threading.current_thread().name}")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | thread_name: {threading.current_thread().name}")

    def _log_warning(self, text: str) -> None:
        logging.warning(f"{text} | thread_name: {threading.current_thread().name}")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | thread_name: {threading.current_thread().name}")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return not self.is_stopped.is_set()

    def mom_consumer(
        self,
    ) -> RabbitMQMessageMiddlewareQueue:
        return self._mom_consumer

    # ============================== PRIVATE - JOIN ============================== #

    def _should_be_joined(
        self, base_item: dict[str, str], stream_item: dict[str, str]
    ) -> bool:
        base_optional_value = base_item.get(self._join_key)
        stream_optional_value = stream_item.get(self._join_key)
        if base_optional_value is None or stream_optional_value is None:
            return False

        base_value = self._transform_function(base_optional_value)
        stream_value = self._transform_function(stream_optional_value)
        return base_value == stream_value  # type: ignore

    def _join_with_base_data(self, message: BatchMessage) -> BatchMessage:
        message_type = message.message_type()
        session_id = message.session_id()
        stream_batch_items = message.batch_items()
        joined_batch_items: list[dict[str, str]] = []
        for stream_batch_item in stream_batch_items:
            was_joined = False
            for base_batch_item in self._base_data_by_session_id[session_id]:
                if self._should_be_joined(base_batch_item, stream_batch_item):
                    joined_batch_item = {**stream_batch_item, **base_batch_item}
                    joined_batch_items.append(joined_batch_item)
                    was_joined = True
                    break
            if not was_joined:
                self._log_warning(
                    f"action: join_with_base_data | result: error | stream_item: {stream_batch_item}"
                )
        return BatchMessage(
            message_type=message_type,
            session_id=session_id,
            message_id=message.message_id(),
            controller_id=str(self._controller_id),
            batch_items=joined_batch_items,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        mom_cleaned_data_producer = self._mom_producers[self._current_producer_id]
        mom_cleaned_data_producer.send(str(message))

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def _send_all_buffered_messages(self, session_id: str) -> None:
        self._log_info(
            f"action: send_all_buffered_messages | result: success | session_id: {session_id}"
        )
        batch_messages = self._stream_data_buffer_by_session_id.get(session_id, [])
        for batch_message in batch_messages:
            joined_message = self._join_with_base_data(batch_message)
            if len(joined_message.batch_items()) > 0:
                self._mom_send_message_to_next(joined_message)
        self._stream_data_buffer_by_session_id[session_id] = []

    def _handle_data_batch_message_when_all_base_data_received(
        self, message: BatchMessage
    ) -> None:
        session_id = message.session_id()
        with self._all_base_data_received_lock:
            self._stream_data_buffer_by_session_id.setdefault(session_id, [])
            self._stream_data_buffer_by_session_id[session_id].append(message)
            if self._all_base_data_received.get(session_id, False):
                self._send_all_buffered_messages(session_id)
            else:
                self._log_debug(
                    f"action: stream_data_received_before_base_data | result: success | session_id: {session_id}"
                )

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        del self._eof_recv_from_prev_controllers[session_id]

        if session_id in self._stream_data_buffer_by_session_id:
            del self._stream_data_buffer_by_session_id[session_id]

        with self._all_base_data_received_lock:
            del self._all_base_data_received[session_id]
        with self._base_data_by_session_id_lock:
            del self._base_data_by_session_id[session_id]

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        self._eof_recv_from_prev_controllers.setdefault(session_id, 0)
        self._eof_recv_from_prev_controllers[session_id] += 1
        self._log_debug(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if (
            self._eof_recv_from_prev_controllers[session_id]
            == self._prev_controllers_amount
        ):
            with self._all_base_data_received_lock:
                all_base_data_received = self._all_base_data_received.get(
                    session_id, False
                )

            if all_base_data_received:
                self._log_info(
                    f"action: all_eofs_received | result: success | session_id: {session_id}"
                )
                self._send_all_buffered_messages(session_id)

                for mom_producer in self._mom_producers:
                    mom_producer.send(str(message))
                self._log_info(
                    f"action: eof_sent | result: success | session_id: {session_id}"
                )

                self._clean_session_data_of(session_id)
            else:
                self._log_debug(
                    f"action: all_eofs_received_before_base_data | result: success | session_id: {session_id}"
                )
                self._eof_recv_from_prev_controllers[session_id] -= 1
                self._mom_consumer.send(str(message))

    def _handle_stream_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if isinstance(message, BatchMessage):
            self._handle_data_batch_message_when_all_base_data_received(message)
        elif isinstance(message, EOFMessage):
            self._handle_data_batch_eof_message(message)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._log_info(f"action: handler_running | result: success")

        self._mom_consumer.start_consuming(self._handle_stream_data)

    def _close_all(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.close()
            self._log_info(f"action: mom_producer_close | result: success")

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
