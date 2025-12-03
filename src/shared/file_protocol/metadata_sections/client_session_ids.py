from typing import Any

from shared.file_protocol.metadata_sections.metadata_section import MetadataSection


class ClientSessionIds(MetadataSection):

    @classmethod
    def _section_description(cls) -> str:
        return "ClientSessionIds"

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        _, lines = row_section

        client_session_ids = []

        for line in lines:
            session_id = line
            client_session_ids.append(session_id)

        return cls(client_session_ids)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, client_session_ids: list[str]) -> None:
        self._client_session_ids = client_session_ids

    # ============================== ACCESSING ============================== #

    def _payload_for_file(self) -> str:
        payload = ""
        for client_session_id in self.client_session_ids():
            payload += client_session_id
            payload += "\n"
        return payload

    def client_session_ids(self) -> list[str]:
        return self._client_session_ids

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_client_session_ids(self)
