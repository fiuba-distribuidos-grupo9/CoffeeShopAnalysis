import os
from pathlib import Path


class AtomicWriter:

    def write(self, path: Path, data: str) -> None:
        tmp_path = Path(f"{path}.tmp")

        with open(tmp_path, "w+") as file:
            n = file.write(data)
            if n < len(data):
                raise IOError("Failed to write all data to temporary file")
            file.flush()
            # os.fsync(file.fileno())

        os.rename(tmp_path, path)
