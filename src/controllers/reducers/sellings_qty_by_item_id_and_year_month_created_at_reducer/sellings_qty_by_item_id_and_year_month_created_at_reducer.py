import uuid
from typing import Any

from controllers.reducers.shared.reducer import Reducer
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol import constants
from shared.communication_protocol.batch_message import BatchMessage


class SellingsQtyByItemIdAndYearMonthCreatedAtReducer(Reducer):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name_type = consumers_config["queue_name_type"]
        queue_name = f"{queue_name_prefix}-{queue_name_type}-{self._controller_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host,queue_name=queue_name)

    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        queue_name_prefix = producers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{producer_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    # ============================== PRIVATE - ACCESSING ============================== #

    def _keys(self) -> list[str]:
        return ["item_id", "year_month_created_at"]

    def _accumulator_name(self) -> str:
        return "sellings_qty"

    def _message_type(self) -> str:
        return constants.TRANSACTION_ITEMS_BATCH_MSG_TYPE

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        return current_value + float(batch_item["quantity"])

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _simple_hash(self, value: str) -> int:
        hash_value = 0
        prime_multiplier = 31
        for char in value:
            char_value = ord(char)
            hash_value = (hash_value * prime_multiplier) + char_value
        return hash_value

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        batch_items_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "year_month_created_at"

        for batch_item in message.batch_items():
            if batch_item[sharding_key] == "":
                # [IMPORTANT] If sharding value is empty, the hash will fail
                # but we are going to assign it to the first reducer anyway
                hash = 0
                batch_items_by_hash.setdefault(hash, [])
                batch_items_by_hash[hash].append(batch_item)
                continue
            sharding_value = self._simple_hash(batch_item[sharding_key])

            hash = sharding_value % len(self._mom_producers)
            batch_items_by_hash.setdefault(hash, [])
            batch_items_by_hash[hash].append(batch_item)

        for hash, batch_items in batch_items_by_hash.items():
            mom_producer = self._mom_producers[hash]
            message = BatchMessage(
                message_type=message.message_type(),
                session_id=message.session_id(),
                message_id=uuid.uuid4().hex,
                controller_id=str(self._controller_id),
                batch_items=batch_items,
            )
            mom_producer.send(str(message))
