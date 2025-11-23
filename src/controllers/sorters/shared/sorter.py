import logging
import uuid
from abc import abstractmethod
from typing import Any

from controllers.shared.controller import Controller
from controllers.sorters.shared.sorted_desc_data import SortedDescData
from middleware.middleware import MessageMiddleware
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message


class Sorter(Controller):

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
        self._eof_recv_from_prev_controllers = {}
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
        amount_per_group: int,
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._batch_max_size = batch_max_size
        self._amount_per_group = amount_per_group
        self._sorted_desc_data_by_session_id: dict[str, SortedDescData] = {}

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.info("action: sigterm_mom_stop_consuming | result: success")

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
            logging.warning(
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

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

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

        del self._sorted_desc_data_by_session_id[session_id]
        logging.info(
            f"action: all_data_sent | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        session_id = message.session_id()
        for batch_item in message.batch_items():
            self._add_batch_item_keeping_sort_desc(session_id, batch_item)

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        del self._eof_recv_from_prev_controllers[session_id]

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        self._eof_recv_from_prev_controllers.setdefault(session_id, 0)
        self._eof_recv_from_prev_controllers[session_id] += 1
        logging.info(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if (
            self._eof_recv_from_prev_controllers[session_id]
            == self._prev_controllers_amount
        ):
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
        if isinstance(message, BatchMessage):
            self._handle_data_batch_message(message)
        elif isinstance(message, EOFMessage):
            self._handle_data_batch_eof_message(message)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        super()._run()
        self._mom_consumer.start_consuming(self._handle_received_data)

    def _close_all(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
