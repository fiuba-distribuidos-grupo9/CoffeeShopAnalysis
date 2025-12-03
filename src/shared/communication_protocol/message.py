from abc import ABC, abstractmethod
from typing import Any, Callable

from shared.communication_protocol import constants


class Message(ABC):

    @classmethod
    def _with_unique_suitable_subclass_for_str_do(
        cls,
        message_str: str,
        callback: Callable,
        no_unique_found_subclass_callback: Callable,
    ) -> Any:
        found_subclass = [
            subclass
            for subclass in cls.__subclasses__()
            if subclass._can_handle(message_str)
        ]
        if len(found_subclass) == 1:
            return callback(found_subclass[0])
        else:
            return no_unique_found_subclass_callback()

    @classmethod
    def _raise_no_suitable_subclass_found(cls, message_type: str) -> None:
        raise ValueError(f"No suitable subclass found for message type: {message_type}")

    @classmethod
    @abstractmethod
    def _available_message_types(cls) -> list[str]:
        raise NotImplementedError("subclass responsibility")

    @classmethod
    def _can_handle(cls, message_str: str) -> bool:
        return cls._message_type_from_str(message_str) in cls._available_message_types()

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def suitable_for_str(cls, message_str: str) -> "Message":
        message_class_found: Message = cls._with_unique_suitable_subclass_for_str_do(
            message_str,
            lambda subclass: subclass.from_str(message_str),
            lambda: cls._raise_no_suitable_subclass_found(message_str),
        )

        return message_class_found

    @classmethod
    @abstractmethod
    def from_str(cls, message_str: str) -> "Message":
        raise NotImplementedError("subclass responsibility")

    # ============================== PARSE ============================== #

    @classmethod
    def _message_type_from_str(cls, message_str: str) -> str:
        return message_str[: constants.MESSAGE_TYPE_LENGTH]

    @classmethod
    def _metadata_from_str(cls, message_str: str) -> str:
        start = message_str.index(constants.METADATA_DELIMITER)
        end = message_str.index(constants.MSG_START_DELIMITER, start)

        metadata = message_str[start + 1 : end]

        return metadata

    @classmethod
    def _payload_from_str(cls, message_str: str) -> str:
        start = message_str.index(constants.MSG_START_DELIMITER)
        end = message_str.index(constants.MSG_END_DELIMITER, start)

        payload = message_str[start + 1 : end]

        return payload

    # ============================== ACCESSING ============================== #

    @abstractmethod
    def message_type(cls) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def metadata(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def payload(self) -> str:
        raise NotImplementedError("subclass responsibility")

    # ============================== CONVERTING ============================== #

    def __str__(self) -> str:
        # format: <message_type>|<metadata>[<payload>]
        encoded_payload = self.message_type()
        encoded_payload += constants.METADATA_DELIMITER
        encoded_payload += self.metadata()
        encoded_payload += constants.MSG_START_DELIMITER
        encoded_payload += self.payload()
        encoded_payload += constants.MSG_END_DELIMITER
        return encoded_payload

    # ============================== VISITOR ============================== #

    @abstractmethod
    def accept(self, visitor: Any) -> Any:
        raise NotImplementedError("subclass responsibility")
