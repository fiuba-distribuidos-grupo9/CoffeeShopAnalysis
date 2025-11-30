import logging
from abc import abstractmethod
from pathlib import Path
from typing import Any, Optional

from controllers.shared.controller import Controller
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message
from shared.duplicate_message_checker import DuplicateMessageChecker
from shared.file_protocol.atomic_writer import AtomicWriter
from shared.file_protocol.metadata_reader import MetadataReader
from shared.file_protocol.prev_controllers_last_message import (
    PrevControllersLastMessage,
)


class Cleaner(Controller):

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name
        )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )
        self._prev_controllers_last_message: dict[int, Message] = {}
        self._duplicate_message_checker = DuplicateMessageChecker(self)

        self._metadata_file_name = Path("metadata.txt")
        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _columns_to_keep(self) -> list[str]:
        raise NotImplementedError("subclass responsibility")

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _load_last_state(self) -> None:
        metadata_sections = self._metadata_reader.read_from(self._metadata_file_name)
        for section in metadata_sections:
            # @TODO: visitor pattern can be used here
            if isinstance(section, PrevControllersLastMessage):
                self._prev_controllers_last_message = (
                    section.prev_controllers_last_message()
                )
            else:
                logging.warning(
                    f"action: unknown_metadata_section | result: warning | section: {section}"
                )

    def _load_last_state_if_exists(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            logging.info(
                f"action: load_last_state | result: in_progress | file: {path}"
            )

            self._load_last_state()

            logging.info(
                f"action: load_last_state | result: success | file: {path}",
            )
        else:
            logging.info(
                f"action: load_last_state_skipped | result: success | file: {path}"
            )

    def _save_current_state(self) -> None:
        metadata_sections = [
            PrevControllersLastMessage(self._prev_controllers_last_message)
        ]
        for metadata_section in metadata_sections:
            self._atomic_writer.write(self._metadata_file_name, str(metadata_section))

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.info("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - FILTER ============================== #

    def _transform_batch_item(self, batch_item: dict[str, str]) -> dict:
        modified_item_batch = {}
        for column in self._columns_to_keep():
            modified_item_batch[column] = batch_item[column]
        return modified_item_batch

    def _transform_batch_message(self, message: BatchMessage) -> BatchMessage:
        updated_batch_items = []
        for batch_item in message.batch_items():
            updated_batch_item = self._transform_batch_item(batch_item)
            updated_batch_items.append(updated_batch_item)
        message.update_batch_items(updated_batch_items)
        return message

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def is_duplicate_message(self, message: Message) -> bool:
        return self._duplicate_message_checker.is_duplicated(message)

    @abstractmethod
    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        updated_message = self._transform_batch_message(message)
        message.update_controller_id(str(self._controller_id))
        self._mom_send_message_to_next(updated_message)

    @abstractmethod
    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        raise NotImplementedError("subclass responsibility")

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        logging.info(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        message.update_controller_id(str(self._controller_id))
        self._mom_send_message_through_all_producers(message)
        logging.info(
            f"action: eof_sent | result: success | session_id: {session_id}",
        )

        self._clean_session_data_of(session_id)

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
            # @TODO: visitor pattern can be used here
            if isinstance(message, BatchMessage):
                self._handle_data_batch_message(message)
            elif isinstance(message, EOFMessage):
                self._handle_data_batch_eof_message(message)
            self._save_current_state()
        else:
            logging.info(
                f"action: duplicate_message_ignored | result: success | message: {message}"
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
        self._close_all_producers()

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
