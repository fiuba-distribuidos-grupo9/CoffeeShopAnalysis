import logging
from typing import Any

from controllers.mappers.shared.mapper import Mapper
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.message import Message


class YearHalfCreatedAtTransactonsMapper(Mapper):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        queue_name_prefix = producers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{producer_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

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

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _transform_batch_item(self, batch_item: dict[str, str]) -> dict[str, str]:
        date = batch_item["created_at"].split(" ")[0]
        month = date.split("-")[1]
        if int(month) <= 6:
            semester = "H1"
        else:
            semester = "H2"
        year = date.split("-")[0]
        batch_item["year_half_created_at"] = f"{year}-{semester}"
        return batch_item

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        batch_items_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "store_id"

        for batch_item in message.batch_items():
            if batch_item[sharding_key] == "":
                # [IMPORTANT] If sharding value is empty, the hash will fail
                # but we are going to assign it to the first reducer anyway
                hash = 0
                batch_items_by_hash.setdefault(hash, [])
                batch_items_by_hash[hash].append(batch_item)
                continue
            sharding_value = int(float(batch_item[sharding_key]))
            batch_item[sharding_key] = str(sharding_value)

            hash = sharding_value % len(self._mom_producers)
            batch_items_by_hash.setdefault(hash, [])
            batch_items_by_hash[hash].append(batch_item)

        for hash, batch_items in batch_items_by_hash.items():
            mom_producer = self._mom_producers[hash]
            message = BatchMessage(
                message_type=message.message_type(),
                session_id=message.session_id(),
                message_id=message.message_id(),
                controller_id=str(self._controller_id),
                batch_items=batch_items,
            )
            mom_producer.send(str(message))

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.send(str(message))

    # ============================== PRIVATE - RUN ============================== #

    def _close_all_producers(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")
