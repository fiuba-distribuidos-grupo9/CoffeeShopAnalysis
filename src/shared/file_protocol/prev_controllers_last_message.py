from typing import Any

from shared.communication_protocol.message import Message
from shared.file_protocol import constants
from shared.file_protocol.metadata_section import MetadataSection


class PrevControllersLastMessage(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "PrevControllersLastMessage"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        _, lines = row_section

        prev_controllers_last_message = {}

        for line in lines:
            controller_id_str, message_str = line.split(constants.DICT_KEY_SEPARATOR, 1)

            controller_id = int(controller_id_str)
            message = Message.suitable_for_str(message_str)

            prev_controllers_last_message[controller_id] = message

        return cls(prev_controllers_last_message)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, prev_controllers_last_message: dict[int, Message]) -> None:
        self._prev_controllers_last_message = prev_controllers_last_message

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        payload = ""
        for controller_id, message in self.prev_controllers_last_message().items():
            payload += f"{controller_id}"
            payload += constants.DICT_KEY_SEPARATOR
            payload += f"{str(message)}"
            payload += "\n"
        return payload

    def prev_controllers_last_message(self) -> dict[int, Message]:
        return self._prev_controllers_last_message

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_prev_controllers_last_message(self)
