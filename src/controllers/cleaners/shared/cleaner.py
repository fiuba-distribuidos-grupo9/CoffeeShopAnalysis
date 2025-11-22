import logging
from abc import abstractmethod
from typing import Any

from controllers.shared.controller import Controller
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message


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

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _columns_to_keep(self) -> list[str]:
        raise NotImplementedError("subclass responsibility")

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

    @abstractmethod
    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        updated_message = self._transform_batch_message(message)
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

        self._mom_send_message_through_all_producers(message)
        logging.info(f"action: eof_sent | result: success | session_id: {session_id}")

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

    @abstractmethod
    def _close_all_producers(self) -> None:
        raise NotImplementedError("subclass responsibility")

    def _close_all(self) -> None:
        self._close_all_producers()

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
