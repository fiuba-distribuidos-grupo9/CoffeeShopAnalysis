import os
from pathlib import Path


class AtomicWriter:

    def write(self, path: Path, data: str) -> None:
        tmp_path = path.parent / f"{path.name}.tmp"

        try:
            with open(tmp_path, "w", encoding="utf-8") as file:
                n = file.write(data)
                if n < len(data):
                    raise IOError("Failed to write all data to temporary file")

                file.flush()
                os.fsync(file.fileno())

            os.replace(tmp_path, path)

        except Exception:
            if tmp_path.exists():
                os.remove(tmp_path)
            raise
