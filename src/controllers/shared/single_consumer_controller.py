from abc import abstractmethod
from pathlib import Path
from typing import Any, Optional

from controllers.shared.controller import Controller
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
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection
from shared.file_protocol.metadata_sections.prev_controllers_eof_recv import (
    PrevControllersEOFRecv,
)
from shared.file_protocol.metadata_sections.prev_controllers_last_message import (
    PrevControllersLastMessage,
)


class SingleConsumerController(Controller):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> RabbitMQMessageMiddlewareQueue:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    def _init_mom_consumer(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._prev_controllers_eof_recv: dict[str, list[bool]] = {}
        self._prev_controllers_amount = consumers_config["prev_controllers_amount"]
        self._mom_consumer = self._build_mom_consumer_using(
            rabbitmq_host, consumers_config
        )

    @abstractmethod
    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        raise NotImplementedError("subclass resposability")

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        health_listen_port: int,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        super().__init__(controller_id, health_listen_port)

        self._init_mom_consumer(rabbitmq_host, consumers_config)
        self._init_mom_producers(rabbitmq_host, producers_config)

        self._prev_controllers_last_message: dict[int, Message] = {}
        self._duplicate_message_checker = DuplicateMessageChecker(self)

        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

        self._metadata_file_name = Path("metadata.txt")

        # self._random_exit_active = True

    # ============================== PRIVATE - ACCESSING ============================== #

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - METADATA - VISITOR ============================== #

    def visit_prev_controllers_last_message(
        self, metadata_section: PrevControllersLastMessage
    ) -> None:
        self._prev_controllers_last_message = (
            metadata_section.prev_controllers_last_message()
        )

    def visit_prev_controllers_eof_recv(
        self, metadata_section: PrevControllersEOFRecv
    ) -> None:
        self._prev_controllers_eof_recv = metadata_section.prev_controllers_eof_recv()

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _file_exists(self, path: Path) -> bool:
        return path.exists() and path.is_file()

    def _assert_is_file(self, path: Path) -> None:
        if not self._file_exists(path):
            raise ValueError(f"Data path error: {path} is not a file")

    def _assert_is_dir(self, path: Path) -> None:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Data path error: {path} is not a folder")

    def _load_last_state(self) -> None:
        path = self._metadata_file_name
        self._log_info(f"action: load_last_state | result: in_progress | file: {path}")

        metadata_sections = self._metadata_reader.read_from(path)
        for metadata_section in metadata_sections:
            metadata_section.accept(self)

        self._log_info(f"action: load_last_state | result: success | file: {path}")

    def _load_last_state_if_exists(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            self._load_last_state()
        else:
            self._log_info(
                f"action: load_last_state_skipped | result: success | file: {path}"
            )

    def _metadata_sections(self) -> list[MetadataSection]:
        return [
            PrevControllersLastMessage(self._prev_controllers_last_message),
            PrevControllersEOFRecv(self._prev_controllers_eof_recv),
        ]

    def _save_current_state(self) -> None:
        metadata_sections = self._metadata_sections()
        metadata_sections_str = "".join([str(section) for section in metadata_sections])
        self._atomic_writer.write(self._metadata_file_name, metadata_sections_str)

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        self._log_info("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - MESSAGE - VISITOR ============================== #

    def visit_batch_message(self, message: BatchMessage) -> None:
        self._handle_data_batch_message(message)

    def visit_clean_session_message(self, message: CleanSessionMessage) -> None:
        self._handle_clean_session_data_message(message)

    def visit_eof_message(self, message: EOFMessage) -> None:
        self._handle_data_batch_eof_message(message)

    def visit_handshake_message(self, message: Message) -> None:
        raise ValueError("This message type should't be here")

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def is_duplicate_message(self, message: Message) -> bool:
        return self._duplicate_message_checker.is_duplicated(message)

    @abstractmethod
    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _handle_clean_session_data_message(self, message: CleanSessionMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        self._random_exit_with_error("before_message_processed")

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
            message.accept(self)

            self._random_exit_with_error("after_message_processed")
            self._save_current_state()
            self._random_exit_with_error("after_state_saved")
        else:
            self._log_info(
                f"action: duplicate_message_ignored | result: success | message: {message.metadata()}"
            )

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        super()._run()
        self._load_last_state_if_exists()
        self._mom_consumer.start_consuming(self._handle_received_data)

    @abstractmethod
    def _close_all_producers(self) -> None:
        raise NotImplementedError("subclass responsibility")

    def _close_all(self) -> None:
        super()._close_all()
        self._close_all_producers()

        self._mom_consumer.delete()
        self._mom_consumer.close()
        self._log_debug("action: mom_consumer_close | result: success")
