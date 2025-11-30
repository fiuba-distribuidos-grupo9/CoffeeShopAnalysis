from typing import Any

from shared.file_protocol import constants
from shared.file_protocol.metadata_section import MetadataSection


class PrevControllersEOFRecv(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "PrevControllersEOFRecv"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        _, lines = row_section

        prev_controllers_eof_recv = {}

        for line in lines:
            session_id, booleans_str = line.split(
                constants.KEY_VALUE_SECTION_SEPARATOR, 1
            )
            booleans_str = (
                booleans_str.strip()
                .lstrip(constants.LIST_START_DELIMITER)
                .rstrip(constants.LIST_END_DELIMITER)
            )

            booleans = [
                b_str.strip().lower() == str(True).lower()
                for b_str in booleans_str.split(constants.LIST_ITEMS_SEPARATOR)
            ]

            prev_controllers_eof_recv[session_id] = booleans

        return cls(prev_controllers_eof_recv)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, prev_controllers_eof_recv: dict[str, list[bool]]) -> None:
        self._prev_controllers_eof_recv = prev_controllers_eof_recv

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        payload = ""
        for session_id, booleans in self._prev_controllers_eof_recv.items():
            payload += session_id
            payload += constants.KEY_VALUE_SECTION_SEPARATOR
            payload += constants.LIST_START_DELIMITER
            payload += constants.LIST_ITEMS_SEPARATOR.join(
                [str(b).lower() for b in booleans]
            )
            payload += constants.LIST_END_DELIMITER
        return payload

    def prev_controllers_eof_recv(self) -> dict[str, list[bool]]:
        return self._prev_controllers_eof_recv

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_prev_controllers_eof_recv(self)
