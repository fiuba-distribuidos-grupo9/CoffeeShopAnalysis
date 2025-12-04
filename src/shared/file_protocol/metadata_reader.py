import logging
from pathlib import Path

from shared import constants
from shared.file_protocol import constants as fp_constants
from shared.file_protocol.metadata_sections.metadata_section import MetadataSection


class MetadataReader:

    def _is_section_header(self, line: str) -> bool:
        return line.startswith(fp_constants.SECTION_START_DELIMITER) and line.endswith(
            fp_constants.SECTION_END_DELIMITER
        )

    def _parse_section_description(self, line: str) -> str:
        return line[
            len(fp_constants.SECTION_START_DELIMITER) : -len(
                fp_constants.SECTION_END_DELIMITER
            )
        ]

    def _read_metadata_file(self, file_path: Path) -> dict[str, list[str]]:
        file = open(file_path, "r", encoding="utf-8", buffering=constants.KiB)
        try:
            row_sections: dict[str, list[str]] = {}
            curr_section = None

            eof_reached = False
            while not eof_reached:
                line = file.readline()
                if not line:
                    eof_reached = True
                    continue
                line = line.replace("\x00", "").strip()
                if not line:
                    continue

                if self._is_section_header(line):
                    curr_section = self._parse_section_description(line)
                else:
                    if curr_section is not None:
                        row_sections.setdefault(curr_section, [])
                        row_sections[curr_section].append(line)
                    else:
                        logging.error(
                            f"action: line_without_section | result: error | line: {line}"
                        )
                        raise ValueError("Line without section in metadata file")

            return row_sections
        finally:
            file.close()
            logging.debug(f"action: file_close | result: success | file: {file_path}")

    def read_from(self, file_path) -> list[MetadataSection]:
        row_sections = self._read_metadata_file(file_path)

        metadata = []

        for row_section in row_sections.items():
            metadata_section = MetadataSection.suitable_for(row_section)
            metadata.append(metadata_section)

        return metadata
