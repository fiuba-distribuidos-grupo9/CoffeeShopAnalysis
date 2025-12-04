import uuid
from abc import abstractmethod
from pathlib import Path
from typing import Any, Optional

from controllers.reducers.shared.reduced_data import ReducedData
from controllers.shared.controller import Controller
from middleware.middleware import MessageMiddleware
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
from shared.file_protocol.metadata_sections.reduced_data_by_session_id import (
    ReducedDataBySessionId,
)
from shared.file_protocol.metadata_sections.session_batch_messages import (
    SessionBatchMessages,
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
        self._prev_controllers_eof_recv: dict[str, list[bool]] = {}
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

        self._metadata_reader = MetadataReader()
        self._atomic_writer = AtomicWriter()

        self._metadata_file_name = Path("metadata.txt")

        self._results_dir = Path("results")
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._results_file_prefix = Path("results_")

        # self._random_exit_active = True

    # ============================== PRIVATE - ACCESSING ============================== #

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

    def _load_results_to_be_sent(self, session_id: str) -> list[BatchMessage]:
        self._log_info(f"action: load_results_to_be_sent | result: in_progress")

        messages = []

        self._assert_is_dir(self._results_dir)

        path = self._results_dir / (f"{self._results_file_prefix}{session_id}.txt")
        self._assert_is_file(path)
        metadata_sections = self._metadata_reader.read_from(path)
        for metadata_section in metadata_sections:
            if isinstance(metadata_section, SessionBatchMessages):
                messages = metadata_section.batch_messages()
            else:
                self._log_warning(
                    f"action: unknown_metadata_section | result: error | section: {metadata_section}"
                )

        self._log_info(f"action: load_results_to_be_sent | result: success")

        return messages

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
            elif isinstance(metadata_section, ReducedDataBySessionId):
                for (
                    session_id,
                    reduced_data_dict,
                ) in metadata_section.reduced_data_by_session_id().items():
                    reduced_data = ReducedData(
                        self._keys(),
                        self._accumulator_name(),
                        self._reduce_function,
                    )
                    reduced_data.replace(reduced_data_dict)
                    self._reduced_data_by_session_id[session_id] = reduced_data
            else:
                self._log_warning(
                    f"action: unknown_metadata_section | result: error | section: {metadata_section}"
                )

        self._log_info(f"action: load_last_state | result: success | file: {path}")

    def _load_last_state_if_exists(self) -> None:
        path = self._metadata_file_name
        if path.exists() and path.is_file():
            self._load_last_state()
        else:
            self._log_info(
                f"action: load_last_state_skipped | result: success | file: {path}"
            )

    def _save_results_to_be_sent(
        self, session_id: str, messages: list[BatchMessage]
    ) -> None:
        self._atomic_writer.write(
            self._results_dir / (f"{self._results_file_prefix}{session_id}.txt"),
            str(SessionBatchMessages(messages)),
        )

    def _save_current_state(self) -> None:
        reduced_data_by_session_id_dict = {}
        for session_id, reduced_data in self._reduced_data_by_session_id.items():
            reduced_data_by_session_id_dict[session_id] = reduced_data.to_dict()

        metadata_sections = [
            PrevControllersLastMessage(self._prev_controllers_last_message),
            PrevControllersEOFRecv(self._prev_controllers_eof_recv),
            ReducedDataBySessionId(reduced_data_by_session_id_dict),
        ]
        metadata_sections_str = "".join([str(section) for section in metadata_sections])
        self._atomic_writer.write(self._metadata_file_name, metadata_sections_str)

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        self._log_info("action: sigterm_mom_stop_consuming | result: success")

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
            ReducedData(self._keys(), self._accumulator_name(), self._reduce_function),
        ).reduce(batch_item)

    def _pop_next_batch_item(self, session_id: str) -> dict[str, str]:
        return self._reduced_data_by_session_id[session_id].pop_next_batch_item()

    def _take_next_batch(self, session_id: str) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []
        reduced_data = self._reduced_data_by_session_id.get(session_id)
        if reduced_data is None:
            self._log_warning(
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
            self._reduce_by_keys(session_id, batch_item)

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    def _clean_session_data_of(self, session_id: str) -> None:
        self._log_info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        self._prev_controllers_eof_recv.pop(session_id, None)
        self._reduced_data_by_session_id.pop(session_id, None)

        path = self._results_dir / f"{self._results_file_prefix}{session_id}.txt"
        if self._file_exists(path):
            path.unlink()

        self._log_info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

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

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        self._random_exit_with_error("before_message_processed")
        message = Message.suitable_for_str(message_as_bytes.decode("utf-8"))
        if not self.is_duplicate_message(message):
            if isinstance(message, BatchMessage):
                self._handle_data_batch_message(message)
            elif isinstance(message, EOFMessage):
                self._handle_data_batch_eof_message(message)
            elif isinstance(message, CleanSessionMessage):
                self._handle_clean_session_data_message(message)
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

    def _close_all(self) -> None:
        super()._close_all()
        for mom_producer in self._mom_producers:
            mom_producer.close()
            self._log_debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        self._log_debug("action: mom_consumer_close | result: success")
