from typing import Any

from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.eof_message import EOFMessage
from shared.communication_protocol.handshake_message import HandshakeMessage
from shared.communication_protocol.message import Message


class DuplicateMessageChecker:

    # ============================== INITIALIZE ============================== #

    def __init__(self, context: Any) -> None:
        self._context = context

    # ============================== TESTING ============================== #

    def is_duplicated(self, message: Message) -> bool:
        return message.accept(self)

    # ============================== VISITOR ============================== #

    def visit_batch_message(self, message: BatchMessage) -> bool:
        controller_id = int(message.controller_id())

        last_message = self._context.last_message_of(controller_id)
        self._context.update_last_message(message, controller_id)
        if last_message is None:
            return False
        else:
            return last_message == message

    def visit_eof_message(self, message: EOFMessage) -> bool:
        controller_id = int(message.controller_id())

        last_message = self._context.last_message_of(controller_id)
        self._context.update_last_message(message, controller_id)
        if last_message is None:
            return False
        else:
            return last_message == message

    def visit_handshake_message(self, message: HandshakeMessage) -> bool:
        return False
