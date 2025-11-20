from typing import Optional

from shared.communication_protocol import constants
from shared.communication_protocol.message import Message


class EOFMessage(Message):

    @classmethod
    def _unique_available_message_type(cls) -> str:
        return constants.EOF_MSG_TYPE

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
    def _decode_metadata(
        cls, metadata: str
    ) -> tuple[str, Optional[str], Optional[str]]:
        metadata_fields = metadata.split(constants.METADATA_SEPARATOR)
        if len(metadata_fields) == 3:
            session_id, message_id, controller_id = metadata_fields
            return session_id, message_id, controller_id
        else:
            session_id = metadata_fields[0]
            return session_id, None, None

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_str(cls, message_str: str) -> "EOFMessage":
        cls._assert_expected_message_type(
            cls._message_type_from_str(message_str),
            cls._available_message_types(),
        )

        metadata = cls._metadata_from_str(message_str)
        (session_id, message_id, controller_id) = cls._decode_metadata(metadata)

        batch_message_type = cls._payload_from_str(message_str)

        return cls(session_id, message_id, controller_id, batch_message_type)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(
        self,
        session_id: str,
        message_id: Optional[str],
        controller_id: Optional[str],
        batch_message_type: str,
    ) -> None:
        self._session_id = session_id
        self._message_id = message_id
        self._controller_id = controller_id

        self._batch_message_type = batch_message_type

    # ============================== ACCESSING ============================== #

    def message_type(self) -> str:
        return self._unique_available_message_type()

    def metadata(self) -> str:
        return self._encode_metadata()

    def payload(self) -> str:
        return self._batch_message_type

    def session_id(self) -> str:
        return self._session_id

    def batch_message_type(self) -> str:
        return self._batch_message_type

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
