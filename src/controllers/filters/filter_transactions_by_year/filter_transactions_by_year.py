import logging
from typing import Any

from controllers.filters.shared.filter import Filter
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.message import Message
from shared.simple_hash import simple_hash


class FilterTransactionsByYear(Filter):

    # ============================== INITIALIZE ============================== #

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
        health_listen_port: int,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        years_to_keep: list[int],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            health_listen_port,
            consumers_config,
            producers_config,
        )

        self._years_to_keep = set(years_to_keep)

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _should_be_included(self, batch_item: dict[str, str]) -> bool:
        created_at = batch_item["created_at"]
        date = created_at.split(" ")[0]
        year = int(date.split("-")[0])

        return year in self._years_to_keep

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next_fairly(
        self, message: BatchMessage, mom_producers: list[MessageMiddleware]
    ) -> None:
        sharding_value = simple_hash(message.message_id())
        hash = sharding_value % len(mom_producers)
        mom_producer = mom_producers[hash]
        mom_producer.send(str(message))

    def _mom_send_message_to_next_sharding(
        self, message: BatchMessage, mom_producers: list[MessageMiddleware]
    ) -> None:
        batch_items_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "user_id"

        counter = 0
        for batch_item in message.batch_items():
            if batch_item[sharding_key] == "":
                if counter == 0:
                    logging.warning(
                        f"action: invalid_{sharding_key} | {sharding_key}: {batch_item[sharding_key]} | result: skipped"
                    )
                counter = 0 if counter == 1000 else counter + 1
                continue
            sharding_value = int(float(batch_item[sharding_key]))
            batch_item[sharding_key] = str(sharding_value)

            hash = sharding_value % len(mom_producers)
            batch_items_by_hash.setdefault(hash, [])
            batch_items_by_hash[hash].append(batch_item)

        for hash, batch_items in batch_items_by_hash.items():
            mom_producer = mom_producers[hash]
            message = BatchMessage(
                message_type=message.message_type(),
                session_id=message.session_id(),
                message_id=message.message_id(),
                controller_id=str(self._controller_id),
                batch_items=batch_items,
            )
            mom_producer.send(str(message))

    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        self._mom_send_message_to_next_fairly(message, self._mom_producers_1)
        self._mom_send_message_to_next_sharding(message, self._mom_producers_2)

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
