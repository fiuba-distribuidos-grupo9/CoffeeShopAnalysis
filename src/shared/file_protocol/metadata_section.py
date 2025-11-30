from abc import ABC, abstractmethod
from typing import Any, Callable

from shared.file_protocol import constants


class MetadataSection(ABC):

    @classmethod
    def _with_unique_suitable_subclass_for_str_do(
        cls,
        row_section: tuple[str, list],
        callback: Callable,
        no_unique_found_subclass_callback: Callable,
    ) -> Any:
        found_subclass = [
            subclass
            for subclass in cls.__subclasses__()
            if subclass._can_handle(row_section)
        ]
        if len(found_subclass) == 1:
            return callback(found_subclass[0])
        else:
            return no_unique_found_subclass_callback()

    @classmethod
    def _raise_no_suitable_subclass_found(cls, row_section: tuple[str, list]) -> None:
        raise ValueError(f"No suitable subclass found for section: {row_section}")

    @classmethod
    @abstractmethod
    def _section_description(cls) -> str:
        raise NotImplementedError("subclass responsibility")

    @classmethod
    def _can_handle(cls, row_section: tuple[str, list]) -> bool:
        return cls._section_description_from(row_section) == cls._section_description()

    # ============================== INSTANCE CREATION ============================== #

    @classmethod
    def suitable_for(cls, row_section: tuple[str, list]) -> "MetadataSection":
        message_class_found: MetadataSection = (
            cls._with_unique_suitable_subclass_for_str_do(
                row_section,
                lambda subclass: subclass.from_row_section(row_section),
                lambda: cls._raise_no_suitable_subclass_found(row_section),
            )
        )

        return message_class_found

    @classmethod
    @abstractmethod
    def from_row_section(cls, row_section: tuple[str, list[str]]) -> "MetadataSection":
        raise NotImplementedError("subclass responsibility")

    # ============================== PARSE ============================== #

    @classmethod
    def _section_description_from(cls, row_section: tuple[str, list]) -> str:
        section_description, _ = row_section
        return section_description

    # ============================== ACCESSING ============================== #

    @abstractmethod
    def _payload_for_file(self) -> str:
        raise NotImplementedError("subclass responsibility")

    # ============================== CONVERTING ============================== #

    def __str__(self) -> str:
        encoded_payload = constants.SECTION_START_DELIMITER
        encoded_payload += self._section_description()
        encoded_payload += constants.SECTION_END_DELIMITER
        encoded_payload += "\n"
        encoded_payload += self._payload_for_file()
        return encoded_payload

    # ============================== VISITOR ============================== #

    @abstractmethod
    def accept(self, visitor: Any) -> Any:
        raise NotImplementedError("subclass responsibility")
