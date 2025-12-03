class SortedDescData:

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        grouping_key: str,
        primary_sort_key: str,
        secondary_sort_key: str,
        amount_per_group: int,
    ):
        self._grouping_key = grouping_key

        self._primary_sort_key = primary_sort_key
        self._secondary_sort_key = secondary_sort_key

        self._amount_per_group = amount_per_group

        self._sorted_desc_by_grouping_key: dict[str, list[dict[str, str]]] = {}

    # ============================== MANAGING ============================== #

    def add_batch_item_keeping_sort_desc(self, batch_item: dict[str, str]) -> None:
        grouping_key_value = batch_item[self._grouping_key]
        primary_sort_value = batch_item[self._primary_sort_key]
        secondary_sort_value = batch_item[self._secondary_sort_key]

        sorted_desc_batch_items = self._sorted_desc_by_grouping_key.setdefault(
            grouping_key_value, []
        )

        index = 0
        while index < len(sorted_desc_batch_items):
            current_batch_item = sorted_desc_batch_items[index]
            current_primary_sort_value = current_batch_item[self._primary_sort_key]
            current_secondary_sort_value = current_batch_item[self._secondary_sort_key]

            if primary_sort_value > current_primary_sort_value:
                break
            if primary_sort_value == current_primary_sort_value:
                if secondary_sort_value > current_secondary_sort_value:
                    break
            index += 1

        sorted_desc_batch_items.insert(index, batch_item)
        if len(sorted_desc_batch_items) > self._amount_per_group:
            sorted_desc_batch_items.pop()

    def replace(
        self, sorted_desc_by_grouping_key: dict[str, list[dict[str, str]]]
    ) -> None:
        self._sorted_desc_by_grouping_key = sorted_desc_by_grouping_key

    # ============================== ACCESSING ============================== #

    def pop_next_batch_item(self) -> dict[str, str]:
        key = next(iter(self._sorted_desc_by_grouping_key))
        batch_item = self._sorted_desc_by_grouping_key[key].pop(0)
        if not self._sorted_desc_by_grouping_key[key]:
            del self._sorted_desc_by_grouping_key[key]
        return batch_item

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return self._sorted_desc_by_grouping_key.copy()

    # ============================== TESTING ============================== #

    def is_empty(self) -> bool:
        return len(self._sorted_desc_by_grouping_key.keys()) == 0
