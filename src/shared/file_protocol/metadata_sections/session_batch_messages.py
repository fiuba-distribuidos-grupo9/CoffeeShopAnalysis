import logging
from typing import Any

from shared.communication_protocol.batch_message import BatchMessage
from shared.communication_protocol.message import Message
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection


class SessionBatchMessages(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "SessionBatchMessages"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        _, lines = row_section

        batch_messages = []

        for line in lines:
            try:
                message = Message.suitable_for_str(line)
            except ValueError as e:
                logging.error(
                    f"action: parse_session_batch_message_error | result: error | line: {line} | error: {e}"
                )
                raise
            batch_messages.append(message)

        return cls(batch_messages)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, batch_messages: list[BatchMessage]) -> None:
        self._batch_messages = batch_messages

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        payload = ""
        for message in self.batch_messages():
            if len(message.batch_items()) == 0:
                continue
            payload += str(message)
            payload += "\n"
        return payload

    def batch_messages(self) -> list[BatchMessage]:
        return self._batch_messages

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_session_batch_messages(self)
