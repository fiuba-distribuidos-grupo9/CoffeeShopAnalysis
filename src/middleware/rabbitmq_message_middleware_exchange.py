import logging
from typing import Callable, List

import pika
from pika.exceptions import AMQPConnectionError

from middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
)


class RabbitMQMessageMiddlewareExchange(MessageMiddlewareExchange):

    # ============================== PRIVATE - RABBIT INFO ============================== #

    def _rabbitmq_port(self) -> int:
        return 5672

    def _rabbitmq_user(self) -> str:
        return "guest"

    def _rabbitmq_password(self) -> str:
        return "guest"

    # ============================== PRIVATE - INITIALIZATION ============================== #

    def __init__(self, host: str, exchange_name: str, route_keys: List):
        super().__init__(host, exchange_name, route_keys)

        self._queue_name = ""
        self._exchange_name = exchange_name
        self._routing_keys = route_keys

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=self._rabbitmq_port(),
                    credentials=pika.PlainCredentials(
                        self._rabbitmq_user(), self._rabbitmq_password()
                    ),
                    heartbeat=3600,
                )
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="topic",  # type: ignore
                durable=True,
            )
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting to RabbitMQ server: {e}"
            )

    # ============================== PRIVATE - ACCESSING ============================== #

    def _pika_on_message_callback_wrapping(
        self, on_message_callback: Callable
    ) -> Callable:
        def pika_on_message_callback(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            try:
                on_message_callback(body)
                channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)  # type: ignore
            except Exception as e:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # type: ignore
                logging.error(f"action: receive_message | result: fail | error: {e}")
                raise e

        return pika_on_message_callback

    # ============================== PRIVATE - ASSERTIONS ============================== #

    def _assert_connection_is_open(self) -> None:
        if not self._connection.is_open or not self._channel.is_open:
            raise MessageMiddlewareDisconnectedError(
                "Error: Connection or channel is closed."
            )

    # ============================== PRIVATE - HANDLE EXCEPTIONS ============================== #

    def _handle_amqp_errors_during(
        self, callback: Callable, args=(), kwargs={}, exc_prefix: str = ""
    ) -> None:
        try:
            callback(*args, **kwargs)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"{exc_prefix} {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"{exc_prefix} {e}")

    # ============================== PRIVATE - SUPPORT ============================== #

    def _bind_queue_to_routing_keys(self, queue_name: str) -> None:
        for routing_key in self._routing_keys:
            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )

    def _start_consuming(self, on_message_callback: Callable) -> None:
        result = self._channel.queue_declare(
            queue=self._queue_name, exclusive=True, auto_delete=True, durable=True
        )
        queue_name = str(result.method.queue)
        self._bind_queue_to_routing_keys(queue_name)

        self._channel.basic_consume(
            queue_name,
            self._pika_on_message_callback_wrapping(on_message_callback),
            auto_ack=False,
        )
        self._channel.start_consuming()

    def _stop_consuming(self) -> None:
        self._channel.stop_consuming()

    def _send(self, message: str) -> None:
        for routing_key in self._routing_keys:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),  # type: ignore
            )

    # ============================== PUBLIC - INTERFACE ============================== #

    def start_consuming(self, on_message_callback: Callable) -> None:
        self._assert_connection_is_open()
        self._handle_amqp_errors_during(
            self._start_consuming,
            args=(on_message_callback,),
            exc_prefix="Error consuming messages:",
        )

    def stop_consuming(self) -> None:
        self._assert_connection_is_open()
        self._handle_amqp_errors_during(
            self._stop_consuming,
            exc_prefix="Error stopping consuming messages:",
        )

    def send(self, message: str) -> None:
        self._assert_connection_is_open()
        self._handle_amqp_errors_during(
            self._send,
            args=(message,),
            exc_prefix="Error sending message:",
        )

    def close(self) -> None:
        try:
            self._channel.close()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")

    def delete(self) -> None:
        try:
            self._channel.exchange_delete(exchange=self._exchange_name, if_unused=False)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error deleting queue: {e}")

    # ============================== PUBLIC - EXTRA ============================== #

    def schedule_stop_sonsuming(self) -> None:
        self._assert_connection_is_open()
        self._handle_amqp_errors_during(
            self._connection.add_callback_threadsafe,
            args=(self.stop_consuming,),
            exc_prefix="Error scheduling stop consuming:",
        )
