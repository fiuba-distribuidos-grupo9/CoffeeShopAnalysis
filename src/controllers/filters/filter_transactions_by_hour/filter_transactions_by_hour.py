import logging
import uuid
from typing import Any

from controllers.filters.shared.filter import Filter
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.message import Message
from shared.simple_hash import simple_hash


class FilterTransactionsByHour(Filter):

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
        self, rabbitmq_host: str, producers_config: dict[str, Any], producer_id: int
    ) -> tuple[MessageMiddleware, MessageMiddleware]:
        queue_name_prefix_1 = producers_config["queue_name_prefix_1"]
        queue_name_1 = f"{queue_name_prefix_1}-{producer_id}"
        mom_producer_1 = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name_1
        )

        queue_name_prefix_2 = producers_config["queue_name_prefix_2"]
        queue_name_2 = f"{queue_name_prefix_2}-{producer_id}"
        mom_producer_2 = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name_2
        )
        return (mom_producer_1, mom_producer_2)

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        self._mom_producers_1: list[MessageMiddleware] = []
        self._mom_producers_2: list[MessageMiddleware] = []

        self.next_controllers_amount_1 = producers_config["next_controllers_amount_1"]
        self.next_controllers_amount_2 = producers_config["next_controllers_amount_2"]
        for producer_id in range(self.next_controllers_amount_1):
            (mom_producer_1, mom_producer_2) = self._build_mom_producer_using(
                rabbitmq_host, producers_config, producer_id
            )
            self._mom_producers_1.append(mom_producer_1)
            self._mom_producers_2.append(mom_producer_2)

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        min_hour: int,
        max_hour: int,
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._min_hour = min_hour
        self._max_hour = max_hour

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _should_be_included(self, batch_item: dict[str, str]) -> bool:
        created_at = batch_item["created_at"]
        time = created_at.split(" ")[1]
        hour = int(time.split(":")[0])

        return self._min_hour <= hour and hour < self._max_hour

    # ============================== PRIVATE - SEND DATA ============================== #

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(self._mom_producers_1)
        mom_producer = self._mom_producers_1[hash]
        mom_producer.send(str(message))

        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(self._mom_producers_2)
        mom_producer = self._mom_producers_2[hash]
        mom_producer.send(str(message))

    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        for mom_producer in self._mom_producers_1:
            mom_producer.send(str(message))

        for mom_producer in self._mom_producers_2:
            mom_producer.send(str(message))

    # ============================== PRIVATE - RUN ============================== #

    def _close_all_producers(self) -> None:
        for mom_producer in self._mom_producers_1:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        for mom_producer in self._mom_producers_2:
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")
