from typing import Optional

from shared.communication_protocol import constants
from shared.communication_protocol.message import Message


class BatchMessage(Message):

    @classmethod
    def _available_message_types(cls) -> list[str]:
        return [
            constants.MENU_ITEMS_BATCH_MSG_TYPE,
            constants.STORES_BATCH_MSG_TYPE,
            constants.TRANSACTION_ITEMS_BATCH_MSG_TYPE,
            constants.TRANSACTIONS_BATCH_MSG_TYPE,
            constants.USERS_BATCH_MSG_TYPE,
            constants.QUERY_RESULT_1X_MSG_TYPE,
            constants.QUERY_RESULT_21_MSG_TYPE,
            constants.QUERY_RESULT_22_MSG_TYPE,
            constants.QUERY_RESULT_3X_MSG_TYPE,
            constants.QUERY_RESULT_4X_MSG_TYPE,
        ]

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

    @classmethod
    def _decode_batch_item_field(cls, key_value_pair: str) -> tuple[str, str]:
        key, value = key_value_pair.split(":", 1)
        key = key.strip('"')
        value = value.strip('"')
        return key, value

    @classmethod
    def _decode_batch_item(cls, encoded_batch_item: str) -> dict[str, str]:
        encoded_batch_item = encoded_batch_item.strip(constants.BATCH_START_DELIMITER)
        encoded_batch_item = encoded_batch_item.strip(constants.BATCH_END_DELIMITER)

        key_value_pairs = encoded_batch_item.split(constants.BATCH_ITEM_FIELD_SEPARATOR)

        decoded_batch_item = {}

        for key_value_pair in key_value_pairs:
            key, value = cls._decode_batch_item_field(key_value_pair)
            decoded_batch_item[key] = value

        return decoded_batch_item

    @classmethod
    def _decode_batch_items(cls, payload: str) -> list[dict[str, str]]:
        encoded_batch_items = payload.split(constants.BATCH_ITEM_SEPARATOR)
        decoded_batch_items = []

        for encoded_batch_item in encoded_batch_items:
            decoded_batch_item = cls._decode_batch_item(encoded_batch_item)
            decoded_batch_items.append(decoded_batch_item)

        return decoded_batch_items

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def from_str(cls, message_str: str) -> "BatchMessage":
        message_type = cls._message_type_from_str(message_str)
        cls._assert_expected_message_type(
            message_type,
            cls._available_message_types(),
        )

        metadata = cls._metadata_from_str(message_str)
        (session_id, message_id, controller_id) = cls._decode_metadata(metadata)

        payload = cls._payload_from_str(message_str)
        batch_items = cls._decode_batch_items(payload)

        return cls(message_type, session_id, message_id, controller_id, batch_items)

    # ============================== PRIVATE - INITIALIZE ============================== #

    def __init__(
        self,
        message_type: str,
        session_id: str,
        message_id: Optional[str],
        controller_id: Optional[str],
        batch_items: list[dict[str, str]],
    ) -> None:
        self._message_type = message_type

        self._session_id = session_id
        self._message_id = message_id
        self._controller_id = controller_id

        self._batch_items = batch_items

    # ============================== ACCESSING ============================== #

    def message_type(self) -> str:
        return self._message_type

    def metadata(self) -> str:
        return self._encode_metadata()

    def payload(self) -> str:
        return self._encode_batch_items()

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

    def batch_items(self) -> list[dict[str, str]]:
        return self._batch_items

    # ============================== PRIVATE - ENCODE ============================== #

    def _encode_metadata(self) -> str:
        metadata_parts = [self._session_id]
        if self._message_id is not None:
            metadata_parts.append(self._message_id)
        if self._controller_id is not None:
            metadata_parts.append(self._controller_id)
        return constants.METADATA_SEPARATOR.join(metadata_parts)

    def _encode_batch_item_field(self, key: str, value: str) -> str:
        return f'"{key}":"{value}"'

    def _encode_batch_item(self, batch_item: dict[str, str]) -> str:
        encoded_batch_item_fields = [
            self._encode_batch_item_field(key, value)
            for key, value in batch_item.items()
        ]

        encoded_batch_item = constants.BATCH_START_DELIMITER
        encoded_batch_item += constants.BATCH_ITEM_FIELD_SEPARATOR.join(
            encoded_batch_item_fields
        )
        encoded_batch_item += constants.BATCH_END_DELIMITER

        return encoded_batch_item

    def _encode_batch_items(self) -> str:
        encoded_batch_items = []

        for batch_item in self.batch_items():
            encoded_batch_item = self._encode_batch_item(batch_item)
            encoded_batch_items.append(encoded_batch_item)

        return constants.BATCH_ITEM_SEPARATOR.join(encoded_batch_items)

    # ============================== UPDATING ============================== #

    def update_message_type(self, new_message_type: str) -> None:
        self._message_type = new_message_type

    def update_message_id(self, new_message_id: str) -> None:
        self._message_id = new_message_id

    def update_controller_id(self, new_controller_id: str) -> None:
        self._controller_id = new_controller_id

    def update_batch_items(self, new_batch_items: list[dict[str, str]]) -> None:
        self._batch_items = new_batch_items
