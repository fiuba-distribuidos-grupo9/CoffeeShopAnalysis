import uuid
from abc import abstractmethod
from pathlib import Path
from typing import Any, Optional

from controllers.shared.single_consumer_controller import SingleConsumerController
from controllers.sorters.shared.sorted_desc_data import SortedDescData
from middleware.middleware import MessageMiddleware
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.clean_session_message import CleanSessionMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection
from shared.file_protocol.metadata_sections.session_batch_messages import (
    SessionBatchMessages,
)
from shared.file_protocol.metadata_sections.sorted_desc_data_by_session_id import (
    SortedDescDataBySessionId,
)


class Sorter(SingleConsumerController):

    # ============================== INITIALIZE ============================== #

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
        health_listen_port: int,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        batch_max_size: int,
        amount_per_group: int,
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            health_listen_port,
            consumers_config,
            producers_config,
        )

        self._batch_max_size = batch_max_size
        self._amount_per_group = amount_per_group

        self._sorted_desc_data_by_session_id: dict[str, SortedDescData] = {}

        self._results_dir = Path("results")
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._results_file_prefix = Path("results_")

        # self._random_exit_active = True

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _grouping_key(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _primary_sort_key(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _secondary_sort_key(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _message_type(self) -> str:
        raise NotImplementedError("subclass responsibility")

    def update_last_message(self, message: Message, controller_id: int) -> None:
        self._prev_controllers_last_message[controller_id] = message

    def last_message_of(self, controller_id: int) -> Optional[Message]:
        return self._prev_controllers_last_message.get(controller_id)

    # ============================== PRIVATE - METADATA - VISITOR ============================== #

    def visit_sorted_desc_data_by_session_id(
        self, metadata_section: SortedDescDataBySessionId
    ) -> None:
        for (
            session_id,
            sorted_desc_data_dict,
        ) in metadata_section.sorted_desc_data_by_session_id().items():
            sorted_desc_data = SortedDescData(
                self._grouping_key(),
                self._primary_sort_key(),
                self._secondary_sort_key(),
                self._amount_per_group,
            )
            sorted_desc_data.replace(sorted_desc_data_dict)
            self._sorted_desc_data_by_session_id[session_id] = sorted_desc_data

    def visit_session_batch_messages(
        self, metadata_section: SessionBatchMessages
    ) -> list[BatchMessage]:
        return metadata_section.batch_messages()

    # ============================== PRIVATE - MANAGING STATE ============================== #

    def _load_results_to_be_sent(self, session_id: str) -> list[BatchMessage]:
        self._log_info(f"action: load_results_to_be_sent | result: in_progress")

        messages = []

        self._assert_is_dir(self._results_dir)

        path = self._results_dir / (f"{self._results_file_prefix}{session_id}.txt")
        self._assert_is_file(path)
        metadata_sections = self._metadata_reader.read_from(path)
        for metadata_section in metadata_sections:
            messages = metadata_section.accept(self)

        self._log_info(f"action: load_results_to_be_sent | result: success")

        return messages

    def _save_results_to_be_sent(
        self, session_id: str, messages: list[BatchMessage]
    ) -> None:
        self._atomic_writer.write(
            self._results_dir / (f"{self._results_file_prefix}{session_id}.txt"),
            str(SessionBatchMessages(messages)),
        )

    def _metadata_sections(self) -> list[MetadataSection]:
        metadata_sections = super()._metadata_sections()

        sorted_desc_data_by_session_id = {}
        for (
            session_id,
            sorted_desc_data,
        ) in self._sorted_desc_data_by_session_id.items():
            sorted_desc_data_by_session_id[session_id] = sorted_desc_data.to_dict()
        metadata_sections.append(
            SortedDescDataBySessionId(sorted_desc_data_by_session_id)
        )

        return metadata_sections

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _add_batch_item_keeping_sort_desc(
        self, session_id: str, batch_item: dict[str, str]
    ) -> None:
        self._sorted_desc_data_by_session_id.setdefault(
            session_id,
            SortedDescData(
                self._grouping_key(),
                self._primary_sort_key(),
                self._secondary_sort_key(),
                self._amount_per_group,
            ),
        ).add_batch_item_keeping_sort_desc(batch_item)

    def _pop_next_batch_item(self, session_id: str) -> dict[str, str]:
        return self._sorted_desc_data_by_session_id[session_id].pop_next_batch_item()

    def _take_next_batch(self, session_id: str) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []
        sorted_desc_by_grouping_key = self._sorted_desc_data_by_session_id.get(
            session_id
        )
        if sorted_desc_by_grouping_key is None:
            self._log_warning(
                f"action: no_sorted_data_for_session_id | result: warning | session_id: {session_id}"
            )
            return batch

        all_batchs_taken = False
        while not all_batchs_taken and len(batch) < self._batch_max_size:
            if sorted_desc_by_grouping_key.is_empty():
                all_batchs_taken = True
                break

            batch_item = self._pop_next_batch_item(session_id)
            batch.append(batch_item)

        return batch

    # ============================== PRIVATE - CLEAN SESSION ============================== #

    def _clean_session_data_of(self, session_id: str) -> None:
        self._log_info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        self._prev_controllers_eof_recv.pop(session_id, None)
        self._sorted_desc_data_by_session_id.pop(session_id, None)

        path = self._results_dir / f"{self._results_file_prefix}{session_id}.txt"
        if self._file_exists(path):
            path.unlink()

        self._log_info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    @abstractmethod
    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    def _mom_send_all_messages_to_next(self, session_id: str) -> None:
        messages = self._load_results_to_be_sent(session_id)

        while len(messages) > 0:
            message = messages.pop(0)
            batch_size = len(message.batch_items())
            self._random_exit_with_error("before_sending_batch", 10)
            self._mom_send_message_to_next(message)
            self._random_exit_with_error("after_sending_batch", 10)
            self._log_debug(
                f"action: batch_sent | result: success | session_id: {session_id} | batch_size: {batch_size}"
            )
            self._save_results_to_be_sent(session_id, messages)

    def _are_results_to_be_sent(self, session_id: str) -> bool:
        return self._file_exists(
            self._results_dir / (f"{self._results_file_prefix}{session_id}.txt")
        )

    def _send_all_data_using_batchs(self, session_id: str) -> None:
        self._log_debug(
            f"action: all_data_sent | result: in_progress | session_id: {session_id}"
        )

        if not self._are_results_to_be_sent(session_id):
            messages = []
            batch_items = self._take_next_batch(session_id)
            while len(batch_items) != 0 and self._is_running():
                message = BatchMessage(
                    message_type=self._message_type(),
                    session_id=session_id,
                    message_id=uuid.uuid4().hex,
                    controller_id=str(self._controller_id),
                    batch_items=batch_items,
                )
                messages.append(message)
                batch_items = self._take_next_batch(session_id)

            self._random_exit_with_error("before_saving_results_to_be_sent", 10)
            self._save_results_to_be_sent(session_id, messages)
            self._random_exit_with_error("after_saving_results_to_be_sent", 10)

        self._mom_send_all_messages_to_next(session_id)

        self._log_info(
            f"action: all_data_sent | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        session_id = message.session_id()
        for batch_item in message.batch_items():
            self._add_batch_item_keeping_sort_desc(session_id, batch_item)

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        prev_controller_id = message.controller_id()
        self._prev_controllers_eof_recv.setdefault(
            session_id, [False for _ in range(self._prev_controllers_amount)]
        )
        self._prev_controllers_eof_recv[session_id][int(prev_controller_id)] = True
        self._log_info(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if all(self._prev_controllers_eof_recv[session_id]):
            self._log_info(
                f"action: all_eofs_received | result: success | session_id: {session_id}"
            )

            self._send_all_data_using_batchs(session_id)

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

    # ============================== PRIVATE - RUN ============================== #

    def _close_all_producers(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.close()
            self._log_debug("action: mom_producer_close | result: success")
