import json
from typing import Any


class JSONCodec:

    def decode(self, text: str) -> Any:
        return json.loads(text)

    def encode(self, data: Any) -> str:
        return json.dumps(data)
