import uuid
from typing import Any

from controllers.mappers.shared.mapper import Mapper
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage


class YearMonthCreatedAtTransactionItemsMapper(Mapper):

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
        count_queue_name = "count_items"
        sum_queue_name = "sum_items"
        count_queue = RabbitMQMessageMiddlewareQueue(host=rabbitmq_host,queue_name=(f"{queue_name_prefix}-{count_queue_name}-{producer_id}"))
        sum_queue = RabbitMQMessageMiddlewareQueue(host=rabbitmq_host,queue_name=(f"{queue_name_prefix}-{sum_queue_name}-{producer_id}"))
        return [count_queue, sum_queue]

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _transform_batch_item(self, batch_item: dict[str, str]) -> dict[str, str]:
        date = batch_item["created_at"].split(" ")[0]
        month = date.split("-")[1]
        year = date.split("-")[0]
        batch_item["year_month_created_at"] = f"{year}-{month}"
        return batch_item

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        batch_items_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "item_id"

        next_controllers = len(self._mom_producers) // 2

        for batch_items in message.batch_items():
            if batch_items[sharding_key] == "":
                # [IMPORTANT] If sharding value is empty, the hash will fail
                # but we are going to assign it to the first reducer anyway
                hash = 0
                batch_items_by_hash.setdefault(hash, [])
                batch_items_by_hash[hash].append(batch_items)
                continue
            sharding_value = int(float(batch_items[sharding_key]))
            batch_items[sharding_key] = str(sharding_value)

            hash = sharding_value % next_controllers
            batch_items_by_hash.setdefault(hash, [])
            batch_items_by_hash[hash].append(batch_items)

        for hash, batch_items in batch_items_by_hash.items():
            count_producer = self._mom_producers[hash*2]
            sum_producer = self._mom_producers[hash*2 + 1]
            # mom_producer = self._mom_producers[hash]
            message = BatchMessage(
                message_type=message.message_type(),
                session_id=message.session_id(),
                message_id=uuid.uuid4().hex,
                controller_id=str(self._controller_id),
                batch_items=batch_items,
            )
            count_producer.send(str(message))
            sum_producer.send(str(message))
