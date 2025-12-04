from typing import Any

from shared.file_protocol import constants
from shared.file_protocol.json_codec import JSONCodec
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection


class SortedDescDataBySessionId(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "SortedDescDataBySessionId"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        json_codec = JSONCodec()
        _, lines = row_section

        sorted_desc_data_by_session_id: dict[str, dict[str, list[dict[str, str]]]] = {}

        for line in lines:
            session_id, reduced_data_str = line.split(constants.DICT_KEY_SEPARATOR, 1)
            reduced_data = json_codec.decode(reduced_data_str)
            sorted_desc_data_by_session_id[session_id] = reduced_data

        return cls(sorted_desc_data_by_session_id)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(
        self, sorted_desc_data_by_session_id: dict[str, dict[str, list[dict[str, str]]]]
    ) -> None:
        self._sorted_desc_data_by_session_id = sorted_desc_data_by_session_id

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        json_codec = JSONCodec()
        # <session_id>:<sorted_desc_data_json>\n
        payload = ""
        for (
            session_id,
            sorted_desc_data,
        ) in self._sorted_desc_data_by_session_id.items():
            payload += f"{session_id}"
            payload += constants.DICT_KEY_SEPARATOR
            payload += json_codec.encode(sorted_desc_data)
            payload += "\n"
        return payload

    def sorted_desc_data_by_session_id(
        self,
    ) -> dict[str, dict[str, list[dict[str, str]]]]:
        return self._sorted_desc_data_by_session_id

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_sorted_desc_data_by_session_id(self)
