import logging
import uuid
from abc import abstractmethod
from pathlib import Path
from typing import Any, Optional

from controllers.reducers.shared.reduced_data import ReducedData
from controllers.shared.controller import Controller
from middleware.middleware import MessageMiddleware
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.duplicate_message_checker import (
    DuplicateMessageChecker,
)
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message
from shared.file_protocol.atomic_writer import AtomicWriter
from shared.file_protocol.metadata_reader import MetadataReader
from shared.file_protocol.prev_controllers_eof_recv import PrevControllersEOFRecv
from shared.file_protocol.prev_controllers_last_message import (
    PrevControllersLastMessage,
)


class Reducer(Controller):

    # ============================== INITIALIZE ============================== #

    @abstractmethod
    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._prev_controllers_eof_recv = {}
        self._prev_controllers_amount = consumers_config["prev_controllers_amount"]
        self._mom_consumer = self._build_mom_consumer_using(
            rabbitmq_host, consumers_config
        )

    @abstractmethod
    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

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
        batch_max_size: int,
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._batch_max_size = batch_max_size

        self._reduced_data_by_session_id: dict[str, ReducedData] = {}

        self._prev_controllers_last_message: dict[int, Message] = {}
        self._duplicate_message_checker = DuplicateMessageChecker(self)

        self._metadata_file_name = Path("metadata.txt")
        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

    # ============================== PRIVATE - ACCESSING ============================== #

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _load_last_state(self) -> None:
        path = self._metadata_file_name
        logging.info(f"action: load_last_state | result: in_progress | file: {path}")

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
                logging.warning(
                    f"action: unknown_metadata_section | result: warning | section: {metadata_section}"
                )

        logging.info(f"action: load_last_state | result: success | file: {path}")

    def _load_last_state_if_exists(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            self._load_last_state()
        else:
            logging.info(
                f"action: load_last_state_skipped | result: success | file: {path}"
            )

    def _save_current_state(self) -> None:
        metadata_sections = [
            PrevControllersLastMessage(self._prev_controllers_last_message),
            PrevControllersEOFRecv(self._prev_controllers_eof_recv),
        ]
        metadata_sections_str = "".join([str(section) for section in metadata_sections])
        self._atomic_writer.write(self._metadata_file_name, metadata_sections_str)

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.info("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _keys(self) -> list[str]:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _accumulator_name(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _message_type(self) -> str:
        raise NotImplementedError("subclass responsibility")

    # ============================== PRIVATE - HANDLE DATA ============================== #

    @abstractmethod
    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        raise NotImplementedError("subclass responsibility")

    def _reduce_by_keys(self, session_id: str, batch_item: dict[str, str]) -> None:
        self._reduced_data_by_session_id.setdefault(
            session_id,
            ReducedData(self._keys(), self._accumulator_name()),
        ).reduce_using(batch_item, self._reduce_function)

    def _pop_next_batch_item(self, session_id: str) -> dict[str, str]:
        return self._reduced_data_by_session_id[session_id].pop_next_batch_item()

    def _take_next_batch(self, session_id: str) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []
        reduced_data = self._reduced_data_by_session_id.get(session_id)
        if reduced_data is None:
            logging.warning(
                f"action: no_reduced_data_for_session_id | result: warning | session_id: {session_id}"
            )
            return batch

        all_batchs_taken = False
        while not all_batchs_taken and len(batch) < self._batch_max_size:
            if reduced_data.is_empty():
                all_batchs_taken = True
                break

            batch_item = self._pop_next_batch_item(session_id)
            batch.append(batch_item)

        return batch

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def is_duplicate_message(self, message: Message) -> bool:
        return self._duplicate_message_checker.is_duplicated(message)

    @abstractmethod
    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    def _send_all_data_using_batchs(self, session_id: str) -> None:
        logging.debug(
            f"action: all_data_sent | result: in_progress | session_id: {session_id}"
        )

        batch_items = self._take_next_batch(session_id)
        while len(batch_items) != 0 and self._is_running():
            message = BatchMessage(
                message_type=self._message_type(),
                session_id=session_id,
                message_id=uuid.uuid4().hex,
                controller_id=str(self._controller_id),
                batch_items=batch_items,
            )
            self._mom_send_message_to_next(message)
            logging.debug(
                f"action: batch_sent | result: success | session_id: {session_id} | batch_size: {len(batch_items)}"
            )
            batch_items = self._take_next_batch(session_id)

        del self._reduced_data_by_session_id[session_id]
        logging.info(
            f"action: all_data_sent | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        session_id = message.session_id()
        for batch_item in message.batch_items():
            self._reduce_by_keys(session_id, batch_item)

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        del self._prev_controllers_eof_recv[session_id]

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        prev_controller_id = message.controller_id()
        self._prev_controllers_eof_recv.setdefault(
            session_id, [False for _ in range(self._prev_controllers_amount)]
        )
        self._prev_controllers_eof_recv[session_id][int(prev_controller_id)] = True
        logging.info(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if all(self._prev_controllers_eof_recv[session_id]):
            logging.info(
                f"action: all_eofs_received | result: success | session_id: {session_id}"
            )

            self._send_all_data_using_batchs(session_id)

            message.update_controller_id(str(self._controller_id))
            self._mom_send_message_through_all_producers(message)
            logging.info(
                f"action: eof_sent | result: success | session_id: {session_id}"
            )

            self._clean_session_data_of(session_id)

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
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

    def _close_all(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
