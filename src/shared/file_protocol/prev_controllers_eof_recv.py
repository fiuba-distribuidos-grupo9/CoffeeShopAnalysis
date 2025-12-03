from typing import Any

from shared.file_protocol import constants
from shared.file_protocol.json_codec import JSONCodec
from shared.file_protocol.metadata_section import MetadataSection


class PrevControllersEOFRecv(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "PrevControllersEOFRecv"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        json_codec = JSONCodec()
        _, lines = row_section

        prev_controllers_eof_recv = {}

        for line in lines:
            session_id, booleans_str = line.split(constants.DICT_KEY_SEPARATOR, 1)
            booleans = json_codec.decode(booleans_str)
            prev_controllers_eof_recv[session_id] = booleans

        return cls(prev_controllers_eof_recv)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, prev_controllers_eof_recv: dict[str, list[bool]]) -> None:
        self._prev_controllers_eof_recv = prev_controllers_eof_recv

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        json_codec = JSONCodec()
        payload = ""
        for session_id, booleans in self.prev_controllers_eof_recv().items():
            payload += session_id
            payload += constants.DICT_KEY_SEPARATOR
            payload += json_codec.encode(booleans)
            payload += "\n"
        return payload

    def prev_controllers_eof_recv(self) -> dict[str, list[bool]]:
        return self._prev_controllers_eof_recv

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_prev_controllers_eof_recv(self)
