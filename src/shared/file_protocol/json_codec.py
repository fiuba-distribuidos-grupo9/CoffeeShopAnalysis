from typing import Any


class JSONCodec:

    # ============================== ENCODE ============================== #

    def _encode_dict(self, data_dict: dict) -> str:
        items = []
        for key, value in data_dict.items():
            encoded_key = self._encode_recursive(key)
            encoded_value = self._encode_recursive(value)
            items.append(f"{encoded_key}:{encoded_value}")
        return "{" + ",".join(items) + "}"

    def _encode_list(self, data_list: list) -> str:
        items = []
        for item in data_list:
            encoded_item = self._encode_recursive(item)
            items.append(encoded_item)
        return "[" + ",".join(items) + "]"

    def _encode_tuple(self, data_tuple: tuple) -> str:
        items = []
        for item in data_tuple:
            encoded_item = self._encode_recursive(item)
            items.append(encoded_item)
        return "(" + ",".join(items) + ")"

    def _encode_str(self, data_str: str) -> str:
        return f'"{data_str}"'

    def _encode_number(self, data_num: Any) -> str:
        return str(data_num)

    def _encode_bool(self, data_bool: bool) -> str:
        return str(data_bool).lower()

    def _encode_recursive(self, data: Any) -> str:
        # Available types:
        # - dict
        # - list
        # - tuple
        # - string
        # - int/float
        # - bool

        if isinstance(data, dict):
            return self._encode_dict(data)
        elif isinstance(data, list):
            return self._encode_list(data)
        elif isinstance(data, tuple):
            return self._encode_tuple(data)
        elif isinstance(data, str):
            return self._encode_str(data)
        elif isinstance(data, (int, float)):
            return self._encode_number(data)
        elif isinstance(data, bool):
            return self._encode_bool(data)
        else:
            raise ValueError(f"Unsupported data type for encoding: {type(data)}")

    def encode(self, data: Any) -> str:
        return self._encode_recursive(data)

    # ============================== DECODE ============================== #

    def _decode_dict(self) -> dict:
        self._idx += 1
        result: dict = {}

        self._skip_spaces()
        if self._text[self._idx] == "}":
            self._idx += 1
            return result

        while True:
            key = self._decode_recursive()

            self._skip_spaces()
            if self._text[self._idx] != ":":
                raise ValueError("Expected ':'")
            self._idx += 1

            value = self._decode_recursive()
            result[key] = value

            self._skip_spaces()
            if self._text[self._idx] == "}":
                self._idx += 1
                return result
            elif self._text[self._idx] == ",":
                self._idx += 1
            else:
                raise ValueError("Expected ',' or '}'")

    def _decode_list(self) -> list:
        self._idx += 1
        result: list = []

        self._skip_spaces()
        if self._text[self._idx] == "]":
            self._idx += 1
            return result

        while True:
            value = self._decode_recursive()
            result.append(value)

            self._skip_spaces()
            if self._text[self._idx] == "]":
                self._idx += 1
                return result
            elif self._text[self._idx] == ",":
                self._idx += 1
            else:
                raise ValueError("Expected ',' or ']'")

    def _decode_tuple(self) -> tuple:
        self._idx += 1
        result: list = []

        self._skip_spaces()
        if self._text[self._idx] == ")":
            self._idx += 1
            return tuple(result)

        while True:
            value = self._decode_recursive()
            result.append(value)

            self._skip_spaces()
            if self._text[self._idx] == ")":
                self._idx += 1
                return tuple(result)
            elif self._text[self._idx] == ",":
                self._idx += 1
            else:
                raise ValueError("Expected ',' or ')'")

    def _decode_string(self) -> str:
        assert self._text[self._idx] == '"'
        self._idx += 1
        start = self._idx

        while self._text[self._idx] != '"':
            self._idx += 1

        value = self._text[start : self._idx]
        self._idx += 1
        return value

    def _decode_number(self) -> Any:
        start = self._idx
        while self._idx < len(self._text) and (
            self._text[self._idx].isdigit() or self._text[self._idx] in ".-+"
        ):
            self._idx += 1

        raw = self._text[start : self._idx]
        return float(raw) if "." in raw else int(raw)

    def _decode_bool(self, value: bool) -> bool:
        if value:
            self._idx += 4  # skip "true"
            return True
        else:
            self._idx += 5  # skip "false"
            return False

    def _skip_spaces(self) -> None:
        while self._idx < len(self._text) and self._text[self._idx].isspace():
            self._idx += 1

    def _decode_recursive(self) -> Any:
        self._skip_spaces()
        curr_char = self._text[self._idx]

        if curr_char == "{":
            return self._decode_dict()
        elif curr_char == "[":
            return self._decode_list()
        elif curr_char == "(":
            return self._decode_tuple()
        elif curr_char == '"':
            return self._decode_string()
        elif curr_char.isdigit() or curr_char == "-" or curr_char == "+":
            return self._decode_number()
        elif self._text.startswith("true", self._idx):
            return self._decode_bool(True)
        elif self._text.startswith("false", self._idx):
            return self._decode_bool(False)
        else:
            raise ValueError(f"Unexpected character: {curr_char}")

    def decode(self, text: str) -> Any:
        normalized = (text.replace("True", "true")).replace("False", "false").replace("None", "null")
        self._text = normalized
        self._idx = 0
        return self._decode_recursive()
