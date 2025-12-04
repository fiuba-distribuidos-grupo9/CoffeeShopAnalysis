from abc import abstractmethod
from typing import Any

from controllers.shared.single_consumer_controller import SingleConsumerController
from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.clean_session_message import CleanSessionMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.message import Message


class Filter(SingleConsumerController):

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        health_listen_port: int,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            health_listen_port,
            consumers_config,
            producers_config,
        )

        # self._random_exit_active = True

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    @abstractmethod
    def _should_be_included(self, batch_item: dict[str, str]) -> bool:
        raise NotImplementedError("subclass responsibility")

    def _transform_batch_message(self, message: BatchMessage) -> BatchMessage:
        updated_batch_items = []
        for batch_item in message.batch_items():
            if self._should_be_included(batch_item):
                updated_batch_items.append(batch_item)

        return BatchMessage(
            message_type=message.message_type(),
            session_id=message.session_id(),
            message_id=message.message_id(),
            controller_id=str(self._controller_id),
            batch_items=updated_batch_items,
        )

    # ============================== PRIVATE - CLEAN SESSION ============================== #

    def _clean_session_data_of(self, session_id: str) -> None:
        self._log_info(
            f"action: clean_session_data | result: in_progress | session_id: {session_id}"
        )

        self._prev_controllers_eof_recv.pop(session_id, None)

        self._log_info(
            f"action: clean_session_data | result: success | session_id: {session_id}"
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    @abstractmethod
    def _mom_send_message_to_next(self, message: BatchMessage) -> None:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _mom_send_message_through_all_producers(self, message: Message) -> None:
        raise NotImplementedError("subclass responsibility")

    def _handle_data_batch_message(self, message: BatchMessage) -> None:
        updated_message = self._transform_batch_message(message)
        if len(updated_message.batch_items()) > 0:
            self._mom_send_message_to_next(updated_message)

    def _handle_data_batch_eof_message(self, message: EOFMessage) -> None:
        session_id = message.session_id()
        prev_controller_id = message.controller_id()
        self._prev_controllers_eof_recv.setdefault(
            session_id, [False for _ in range(self._prev_controllers_amount)]
        )
        self._prev_controllers_eof_recv[session_id][int(prev_controller_id)] = True
        self._log_info(
            f"action: eof_received | result: success | session_id: {session_id}"
        )

        if all(self._prev_controllers_eof_recv[session_id]):
            self._log_info(
                f"action: all_eofs_received | result: success | session_id: {session_id}"
            )

            self._mom_send_message_through_all_producers(
                EOFMessage(
                    session_id=message.session_id(),
                    message_id=message.message_id(),
                    controller_id=str(self._controller_id),
                    batch_message_type=message.batch_message_type(),
                )
            )
            self._log_info(
                f"action: eof_sent | result: success | session_id: {session_id}"
            )

            self._clean_session_data_of(session_id)

    def _handle_clean_session_data_message(self, message: CleanSessionMessage) -> None:
        session_id = message.session_id()
        self._log_info(
            f"action: clean_session_message_received | result: success | session_id: {session_id}"
        )

        self._clean_session_data_of(session_id)

        self._mom_send_message_through_all_producers(
            CleanSessionMessage(
                session_id=message.session_id(),
                message_id=message.message_id(),
                controller_id=str(self._controller_id),
            )
        )
        self._log_info(
            f"action: clean_session_message_sent | result: success | session_id: {session_id}"
        )
