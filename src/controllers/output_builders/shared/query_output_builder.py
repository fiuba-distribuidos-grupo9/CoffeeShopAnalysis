import logging
import uuid
from abc import abstractmethod
from typing import Any

from controllers.shared.controller import Controller
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message


class QueryOutputBuilder(Controller):

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._eof_recv_from_prev_controllers = {}
        self._prev_controllers_amount = consumers_config["prev_controllers_amount"]

        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name
        )

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        self._rabbitmq_host = rabbitmq_host
        self._queue_name_prefix = producers_config["queue_name_prefix"]

        self._mom_producers: dict[str, RabbitMQMessageMiddlewareQueue] = {}

    # ============================== PRIVATE - INTERFACE ============================== #

    @abstractmethod
    def _columns_to_keep(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def _output_message_type(self) -> str:
        raise NotImplementedError

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.info("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

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

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        session_id = message.session_id()
        updated_message = self._transform_batch_message(message)
        message.update_message_type(self._output_message_type())
        self._mom_producers.setdefault(
            session_id,
            RabbitMQMessageMiddlewareQueue(
                self._rabbitmq_host, f"{self._queue_name_prefix}-{session_id}"
            ),
        )
        message.update_controller_id(str(self._controller_id))
        self._mom_producers[session_id].send(str(updated_message))

    def _clean_session_data_of(self, session_id: str) -> None:
        logging.info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        del self._eof_recv_from_prev_controllers[session_id]

        mom_producer = self._mom_producers.pop(session_id, None)
        if mom_producer:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        logging.info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        self._eof_recv_from_prev_controllers.setdefault(session_id, 0)
        self._eof_recv_from_prev_controllers[session_id] += 1
        logging.debug(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if (
            self._eof_recv_from_prev_controllers[session_id]
            == self._prev_controllers_amount
        ):
            logging.info(
                f"action: all_eofs_received | result: success | session_id: {session_id}"
            )

            message = EOFMessage(
                session_id=session_id,
                message_id=uuid.UUID(int=0).hex,
                controller_id=str(self._controller_id),
                batch_message_type=self._output_message_type(),
            )
            self._mom_producers.setdefault(
                session_id,
                RabbitMQMessageMiddlewareQueue(
                    self._rabbitmq_host, f"{self._queue_name_prefix}-{session_id}"
                ),
            )
            message.update_controller_id(str(self._controller_id))
            self._mom_producers[session_id].send(str(message))
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
        for mom_producer in self._mom_producers.values():
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
