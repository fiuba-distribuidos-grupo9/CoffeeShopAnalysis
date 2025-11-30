import logging
from typing import Callable


class ReducedData:

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        keys: list[str],
        accumulator_name: str,
        reduce_function: Callable,
    ) -> None:
        self._keys = keys
        self._accumulator_name = accumulator_name
        self._reduce_function = reduce_function

        self._reduced_data: dict[tuple, float] = {}

        self._logging_counter = 0

    # ============================== LOGGING ============================== #

    def _logging_warning_when_count_reached(self) -> None:
        if self._logging_counter >= 1000:
            logging.warning(
                f"action: empty_key_in_batch_item | result: error | total: {self._logging_counter}"
            )
            self._logging_counter = 0

    # ============================== MANAGING ============================== #

    def reduce(self, batch_item: dict[str, str]) -> None:
        for k in self._keys:
            if batch_item[k] == "":
                self._logging_counter += 1
                self._logging_warning_when_count_reached()
                return

        key = tuple(batch_item[k] for k in self._keys)
        if key not in self._reduced_data:
            self._reduced_data[key] = 0

        self._reduced_data[key] = self._reduce_function(
            self._reduced_data[key], batch_item
        )

    def replace(self, reduced_data_dict: dict[tuple, float]) -> None:
        self._reduced_data = reduced_data_dict

    # ============================== ACCESSING ============================== #

    def pop_next_batch_item(self) -> dict[str, str]:
        key, value = self._reduced_data.popitem()
        batch_item: dict[str, str] = {}
        for i, k in enumerate(self._keys):
            batch_item[k] = key[i]
        batch_item[self._accumulator_name] = str(value)
        return batch_item

    def to_dict(self) -> dict[tuple, float]:
        return self._reduced_data.copy()

    # ============================== TESTING ============================== #

    def is_empty(self) -> bool:
        return len(self._reduced_data) == 0
