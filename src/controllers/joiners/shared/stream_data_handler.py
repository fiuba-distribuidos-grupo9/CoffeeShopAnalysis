import logging
import threading
from pathlib import Path
from typing import Any, Callable, Optional

from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.clean_session_message import CleanSessionMessage
from shared.communication_protocol.duplicate_message_checker import (
    DuplicateMessageChecker,
)
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message
from shared.file_protocol.atomic_writer import AtomicWriter
from shared.file_protocol.metadata_reader import MetadataReader
from shared.file_protocol.metadata_sections.prev_controllers_eof_recv import (
    PrevControllersEOFRecv,
)
from shared.file_protocol.metadata_sections.prev_controllers_last_message import (
    PrevControllersLastMessage,
)
from shared.file_protocol.metadata_sections.session_batch_messages import (
    SessionBatchMessages,
)
from shared.simple_hash import simple_hash


class StreamDataHandler:

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._prev_controllers_eof_recv: dict[str, list[bool]] = {}
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
        base_data_by_session_id: dict[str, list[BatchMessage]],
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

        self._prev_controllers_last_message: dict[int, Message] = {}
        self._duplicate_message_checker = DuplicateMessageChecker(self)

        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

        self._metadata_file_name = Path("stream_metadata.txt")

        self._base_data_dir = Path("base_data")
        self._base_data_file_prefix = Path("base_data_")

        self._stream_data_dir = Path("stream_data")
        self._stream_data_dir.mkdir(parents=True, exist_ok=True)
        self._stream_data_file_prefix = Path("stream_data_")

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

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _file_exists(self, path: Path) -> bool:
        return path.exists() and path.is_file()

    def _assert_is_file(self, path: Path) -> None:
        if not self._file_exists(path):
            raise ValueError(f"Data path error: {path} is not a file")

    def _assert_is_dir(self, path: Path) -> None:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Data path error: {path} is not a folder")

    def _load_stream_data(self) -> None:
        self._log_info(f"action: load_stream_data | result: in_progress")

        for path in self._stream_data_dir.iterdir():
            self._assert_is_file(path)
            metadata_sections = self._metadata_reader.read_from(path)
            for metadata_section in metadata_sections:
                # @TODO: visitor pattern can be used here
                if isinstance(metadata_section, SessionBatchMessages):
                    session_id = path.stem.replace(
                        str(self._stream_data_file_prefix), ""
                    )
                    self._stream_data_buffer_by_session_id[session_id] = (
                        metadata_section.batch_messages()
                    )
                else:
                    self._log_warning(
                        f"action: unknown_metadata_section | result: error | section: {metadata_section}"
                    )

        self._log_info(f"action: load_stream_data | result: success")

    def _load_last_state(self) -> None:
        path = self._metadata_file_name
        self._log_info(f"action: load_last_state | result: in_progress | file: {path}")

        metadata_sections = self._metadata_reader.read_from(path)
        for metadata_section in metadata_sections:
            # @TODO: visitor pattern can be used here
            if isinstance(metadata_section, PrevControllersLastMessage):
                self._prev_controllers_last_message = (
                    metadata_section.prev_controllers_last_message()
                )
            elif isinstance(metadata_section, PrevControllersEOFRecv):
                self._prev_controllers_eof_recv = (
                    metadata_section.prev_controllers_eof_recv()
                )
            else:
                self._log_warning(
                    f"action: unknown_metadata_section | result: error | section: {metadata_section}"
                )

        self._load_stream_data()

        self._log_info(f"action: load_last_state | result: success | file: {path}")

    def _load_last_state_if_exists(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            self._load_last_state()
        else:
            self._log_info(
                f"action: load_last_state_skipped | result: success | file: {path}"
            )

    def _save_stream_data_section(self, session_id: str) -> None:
        batch_messages = self._stream_data_buffer_by_session_id.get(session_id, None)
        path = (
            self._stream_data_dir / f"{self._stream_data_file_prefix}{session_id}.txt"
        )
        if batch_messages:
            self._atomic_writer.write(
                path,
                str(SessionBatchMessages(batch_messages)),
            )

    def _save_current_state(self, session_id: str) -> None:
        self._save_stream_data_section(session_id)

        metadata_sections = [
            PrevControllersLastMessage(self._prev_controllers_last_message),
            PrevControllersEOFRecv(self._prev_controllers_eof_recv),
        ]
        metadata_sections_str = "".join([str(section) for section in metadata_sections])
        self._atomic_writer.write(self._metadata_file_name, metadata_sections_str)

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

    def _join_with_base_data(self, stream_message: BatchMessage) -> BatchMessage:
        joined_batch_items: list[dict[str, str]] = []
        with self._base_data_by_session_id_lock:
            for stream_batch_item in stream_message.batch_items():
                was_joined = False
                for base_messages in self._base_data_by_session_id.get(
                    stream_message.session_id(), []
                ):
                    for base_batch_item in base_messages.batch_items():
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
            message_type=stream_message.message_type(),
            session_id=stream_message.session_id(),
            message_id=stream_message.message_id(),
            controller_id=str(self._controller_id),
            batch_items=joined_batch_items,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def is_duplicate_message(self, message: Message) -> bool:
        return self._duplicate_message_checker.is_duplicated(message)

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(self._mom_producers)
        mom_producer = self._mom_producers[hash]
        mom_producer.send(str(message))

    def _send_all_buffered_messages(self, session_id: str) -> None:
        self._log_info(
            f"action: send_all_buffered_messages | result: in_progress | session_id: {session_id}"
        )

        while len(self._stream_data_buffer_by_session_id.get(session_id, [])) > 0:
            stream_message = self._stream_data_buffer_by_session_id[session_id].pop()
            joined_message = self._join_with_base_data(stream_message)
            if len(joined_message.batch_items()) == 0:
                self._log_warning(
                    f"action: empty_joined_message | result: error | session_id: {session_id}"
                )
                continue

            self._mom_send_message_to_next(joined_message)

            self._save_stream_data_section(session_id)

        self._log_info(
            f"action: send_all_buffered_messages | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_message_when_all_base_data_received(
        self, message: BatchMessage
    ) -> None:
        session_id = message.session_id()
        with self._all_base_data_received_lock:
            self._stream_data_buffer_by_session_id.setdefault(session_id, [])
            if message not in self._stream_data_buffer_by_session_id[session_id]:
                self._stream_data_buffer_by_session_id[session_id].append(message)
            if self._all_base_data_received.get(session_id, False):
                self._send_all_buffered_messages(session_id)
            else:
                self._log_debug(
                    f"action: stream_data_received_before_base_data | result: success | session_id: {session_id}"
                )

    def _clean_session_data_of(self, session_id: str) -> None:
        self._log_info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        self._prev_controllers_eof_recv.pop(session_id, None)
        self._stream_data_buffer_by_session_id.pop(session_id, None)

        path = (
            self._stream_data_dir / f"{self._stream_data_file_prefix}{session_id}.txt"
        )
        if self._file_exists(path):
            path.unlink()

        with self._all_base_data_received_lock:
            self._all_base_data_received.pop(session_id, None)
        with self._base_data_by_session_id_lock:
            self._base_data_by_session_id.pop(session_id, None)
            path = (
                self._base_data_dir / f"{self._base_data_file_prefix}{session_id}.txt"
            )
            if self._file_exists(path):
                path.unlink()

        self._log_info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        prev_controller_id = message.controller_id()
        self._prev_controllers_eof_recv.setdefault(
            session_id, [False for _ in range(self._prev_controllers_amount)]
        )
        self._prev_controllers_eof_recv[session_id][int(prev_controller_id)] = True
        self._log_debug(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if all(self._prev_controllers_eof_recv[session_id]):
            with self._all_base_data_received_lock:
                all_base_data_received = self._all_base_data_received.get(
                    session_id, False
                )

            if all_base_data_received:
                self._log_info(
                    f"action: all_eofs_received | result: success | session_id: {session_id}"
                )

                self._send_all_buffered_messages(session_id)

                self._mom_send_message_through_all_producers(
                    EOFMessage(
                        session_id=message.session_id(),
                        message_id=message.message_id(),
                        controller_id=str(self._controller_id),
                        batch_message_type=message.batch_message_type(),
                    )
                )
                self._log_info(
                    f"action: eof_sent | result: success | session_id: {session_id}"
                )

                self._clean_session_data_of(session_id)
            else:
                self._log_debug(
                    f"action: all_eofs_received_before_base_data | result: success | session_id: {session_id}"
                )
                # Requeue the EOF message until all base data is received
                # @TODO: base data handler should send a special message to notify that all base data has been received
                self._prev_controllers_eof_recv[session_id][
                    int(prev_controller_id)
                ] = False
                del self._prev_controllers_last_message[int(prev_controller_id)]
                self._mom_consumer.send(str(message))

    def _handle_clean_session_data_message(self, message: CleanSessionMessage) -> None:
        session_id = message.session_id()
        self._log_info(
            f"action: clean_session_message_received | result: success | session_id: {session_id}"
        )

        self._clean_session_data_of(session_id)

        self._mom_send_message_through_all_producers(
            CleanSessionMessage(
                session_id=message.session_id(),
                message_id=message.message_id(),
                controller_id=str(self._controller_id),
            )
        )
        self._log_info(
            f"action: clean_session_message_sent | result: success | session_id: {session_id}"
        )

    def _handle_stream_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
            if isinstance(message, BatchMessage):
                self._handle_data_batch_message_when_all_base_data_received(message)
                self._save_current_state(message.session_id())
            elif isinstance(message, EOFMessage):
                self._handle_data_batch_eof_message(message)
                self._save_current_state(message.session_id())
            elif isinstance(message, CleanSessionMessage):
                self._handle_clean_session_data_message(message)
                self._save_current_state(message.session_id())
        else:
            self._log_info(
                f"action: duplicate_message_ignored | result: success | message: {message.metadata()}"
            )

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._log_info(f"action: handler_running | result: success")
        self._load_last_state_if_exists()
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
