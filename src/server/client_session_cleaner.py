import logging
import os
import uuid
from typing import Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import constants
from shared.communication_protocol.clean_session_message import CleanSessionMessage


class ClientSessionCleaner:

    # ============================== INITIALIZE ============================== #

    def _init_cleaners_data(self, cleaners_data: dict) -> None:
        self._cleaners_data = cleaners_data

    def _init_output_builders_data(self, output_builders_data: dict) -> None:
        self._output_builders_data = output_builders_data

    def _init_mom_producers(self, rabbitmq_host: str) -> None:
        self._mom_cleaners_connections: dict[
            str, list[RabbitMQMessageMiddlewareQueue]
        ] = {}

        for data_type, cleaner_data in self._cleaners_data.items():
            workers_amount = cleaner_data[constants.WORKERS_AMOUNT]
            for id in range(workers_amount):
                queue_name = f"{cleaner_data[constants.QUEUE_PREFIX]}-{id}"
                queue_producer = RabbitMQMessageMiddlewareQueue(
                    rabbitmq_host, queue_name
                )

                if self._mom_cleaners_connections.get(data_type) is None:
                    self._mom_cleaners_connections[data_type] = []
                self._mom_cleaners_connections[data_type].append(queue_producer)

    def _init_mom_consumers(self, rabbitmq_host: str) -> None:
        (_, output_builder_data) = next(iter(self._output_builders_data.items()))
        queue_name = f"{output_builder_data[constants.QUEUE_PREFIX]}-{self._session_id}"
        queue_consumer = RabbitMQMessageMiddlewareQueue(rabbitmq_host, queue_name)
        self._mom_output_builders_connection = queue_consumer

    def __init__(
        self,
        session_id: str,
        rabbitmq_host: str,
        cleaners_data: dict,
        output_builders_data: dict,
    ) -> None:
        self._session_id = session_id
        self._controller_id = 0

        self._rabbitmq_host = rabbitmq_host

        self._init_cleaners_data(cleaners_data)
        self._init_output_builders_data(output_builders_data)

        self._init_mom_producers(rabbitmq_host)
        self._init_mom_consumers(rabbitmq_host)

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: {os.getpid()} | session_id: {self._session_id}")

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_through_all_producers(
        self, message: CleanSessionMessage
    ) -> None:
        for mom_producers in self._mom_cleaners_connections.values():
            for mom_producer in mom_producers:
                mom_producer.send(str(message))

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def _handle_client_connection(self) -> None:
        self._mom_send_message_through_all_producers(
            CleanSessionMessage(
                session_id=self._session_id,
                message_id=uuid.UUID(int=0).hex,
                controller_id=str(self._controller_id),
            )
        )
        self._log_info(f"action: clean_session_sent | result: success")
        pass

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._log_info(f"action: client_session_cleaner_running | result: success")

        self._handle_client_connection()

    def _close_all(self) -> None:
        for mom_cleaner_connections in self._mom_cleaners_connections.values():
            for mom_cleaner_connection in mom_cleaner_connections:
                mom_cleaner_connection.close()
                self._log_debug(
                    f"action: mom_cleaner_connection_close | result: success"
                )

        self._mom_output_builders_connection.delete()
        self._mom_output_builders_connection.close()
        self._log_debug(
            f"action: mom_output_builder_connection_close | result: success"
        )

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            self._log_error(
                f"action: client_session_cleaner_run | result: fail | error: {e}"
            )
            raise e
        finally:
            self._close_all()
            self._log_info(f"action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        self._log_info(f"action: client_session_cleaner_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        self._log_info(f"action: client_session_cleaner_shutdown | result: success")
