from typing import Any

from shared.file_protocol import constants
from shared.file_protocol.json_codec import JSONCodec
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection


class ReducedDataBySessionId(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "ReducedDataBySessionId"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        json_codec = JSONCodec()
        _, lines = row_section

        modified_reduced_data_by_session_id: dict[str, dict[str, float]] = {}
        for line in lines:
            session_id, reduced_data_str = line.split(constants.DICT_KEY_SEPARATOR, 1)
            modified_reduced_data: dict[str, float] = json_codec.decode(
                reduced_data_str
            )
            modified_reduced_data_by_session_id[session_id] = modified_reduced_data

        reduced_data_by_session_id: dict[str, dict[tuple, float]] = {}
        for (
            session_id,
            modified_reduced_data,
        ) in modified_reduced_data_by_session_id.items():
            reduced_data: dict[tuple, float] = {}
            for key_str, value in modified_reduced_data.items():
                key_str_clean = key_str.strip("()")
                key_tuple = tuple(item.strip() for item in key_str_clean.split("|"))
                reduced_data[key_tuple] = value
            reduced_data_by_session_id[session_id] = reduced_data

        return cls(reduced_data_by_session_id)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(
        self, reduced_data_by_session_id: dict[str, dict[tuple, float]]
    ) -> None:
        self._reduced_data_by_session_id = reduced_data_by_session_id

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        json_codec = JSONCodec()
        # <session_id>:<reduced_data_json>\n

        modified_reduced_data_by_session_id: dict[str, dict[str, float]] = {}
        for session_id, reduced_data in self._reduced_data_by_session_id.items():
            modified_reduced_data = {}
            for key in reduced_data.keys():
                new_key = "(" + "|".join(map(str, key)) + ")"
                modified_reduced_data[new_key] = reduced_data[key]
            modified_reduced_data_by_session_id[session_id] = modified_reduced_data

        payload = ""
        for session_id, reduced_data in modified_reduced_data_by_session_id.items():
            payload += f"{session_id}"
            payload += constants.DICT_KEY_SEPARATOR
            payload += json_codec.encode(reduced_data)
            payload += "\n"
        return payload

    def reduced_data_by_session_id(self) -> dict[str, dict[tuple, float]]:
        return self._reduced_data_by_session_id

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_reduced_data_by_session_id(self)
