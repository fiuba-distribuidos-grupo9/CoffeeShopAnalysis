from typing import Any, Optional

from shared.communication_protocol import constants
from shared.communication_protocol.message import Message


class CleanSessionMessage(Message):

    @classmethod
    def _unique_available_message_type(cls) -> str:
        return constants.CLEAN_SESSION_MSG_TYPE

    @classmethod
    def _available_message_types(cls) -> list[str]:
        return [cls._unique_available_message_type()]

    # ============================== PRIVATE - ASSERTIONS ============================== #

    @classmethod
    def _assert_expected_message_type(
        cls, received_message_type: str, expected_message_types: list[str]
    ) -> None:
        if not received_message_type in expected_message_types:
            raise ValueError(f"Invalid message type: {received_message_type}")

    # ============================== PRIVATE - DECODE ============================== #

    @classmethod
    def _decode_metadata(cls, metadata: str) -> tuple[str, str, str]:
        metadata_fields = metadata.split(constants.METADATA_SEPARATOR)
        if len(metadata_fields) != 3:
            raise ValueError(f"Invalid metadata format: {metadata}")
        session_id, message_id, controller_id = metadata_fields
        return session_id, message_id, controller_id

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_str(cls, message_str: str) -> "CleanSessionMessage":
        cls._assert_expected_message_type(
            cls._message_type_from_str(message_str),
            cls._available_message_types(),
        )

        metadata = cls._metadata_from_str(message_str)
        (session_id, message_id, controller_id) = cls._decode_metadata(metadata)

        return cls(session_id, message_id, controller_id)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(
        self,
        session_id: str,
        message_id: Optional[str],
        controller_id: Optional[str],
    ) -> None:
        self._session_id = session_id
        self._message_id = message_id
        self._controller_id = controller_id

    # ============================== ACCESSING ============================== #

    def message_type(self) -> str:
        return self._unique_available_message_type()

    def metadata(self) -> str:
        return self._encode_metadata()

    def payload(self) -> str:
        return ""

    def session_id(self) -> str:
        return self._session_id

    def message_id(self) -> str:
        if self._message_id is None:
            raise ValueError("Message ID is not set")
        return self._message_id

    def controller_id(self) -> str:
        if self._controller_id is None:
            raise ValueError("Controller ID is not set")
        return self._controller_id

    # ============================== PRIVATE - ENCODE ============================== #

    def _encode_metadata(self) -> str:
        metadata_parts = [self._session_id]
        if self._message_id is not None:
            metadata_parts.append(self._message_id)
        if self._controller_id is not None:
            metadata_parts.append(self._controller_id)
        return constants.METADATA_SEPARATOR.join(metadata_parts)

    # ============================== UPDATING ============================== #

    def update_message_id(self, new_message_id: str) -> None:
        self._message_id = new_message_id

    def update_controller_id(self, new_controller_id: str) -> None:
        self._controller_id = new_controller_id

    # ============================== VISITOR ============================== #

    def accept(self, visitor: Any) -> Any:
        return visitor.visit_clean_session_message(self)

    # ============================== COMPARING ============================== #

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CleanSessionMessage):
            return False

        return (
            self.session_id() == other.session_id()
            and self.message_id() == other.message_id()
            and self.controller_id() == other.controller_id()
        )
