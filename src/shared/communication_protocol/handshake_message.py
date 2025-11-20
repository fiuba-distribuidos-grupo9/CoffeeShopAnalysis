from shared.communication_protocol import constants
from shared.communication_protocol.message import Message


class HandshakeMessage(Message):

    @classmethod
    def _unique_available_message_type(cls) -> str:
        return constants.HANDSHAKE_MSG_TYPE

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

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_str(cls, message_str: str) -> "HandshakeMessage":
        cls._assert_expected_message_type(
            cls._message_type_from_str(message_str),
            cls._available_message_types(),
        )

        id = cls._metadata_from_str(message_str)
        payload = cls._payload_from_str(message_str)

        return cls(id, payload)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(self, id: str, payload: str) -> None:
        self._id = id

        self._payload = payload

    # ============================== ACCESSING ============================== #

    def message_type(self) -> str:
        return self._unique_available_message_type()

    def metadata(self) -> str:
        return self.id()

    def payload(self) -> str:
        return self._payload

    def id(self) -> str:
        return self._id
