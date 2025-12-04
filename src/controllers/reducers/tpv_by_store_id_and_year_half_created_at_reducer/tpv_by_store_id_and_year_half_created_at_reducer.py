from typing import Any

from controllers.reducers.shared.reducer import Reducer
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol import constants
from shared.communication_protocol.batch_message import BatchMessage
from shared.simple_hash import simple_hash


class TpvByStoreIdAndYearHalfCreatedAtReducer(Reducer):

    # ============================== INITIALIZE ============================== #

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
        return ["store_id", "year_half_created_at"]

    def _accumulator_name(self) -> str:
        return "tpv"

    def _message_type(self) -> str:
        return constants.TRANSACTIONS_BATCH_MSG_TYPE

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        return current_value + float(batch_item["final_amount"])

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(self._mom_producers)
        mom_producer = self._mom_producers[hash]
        mom_producer.send(str(message))
