import json
import os
from dataclasses import dataclass
from threading import Lock
from typing import Callable

from mqtt_worker.utils.logger import get_logger


@dataclass(frozen=True)
class BufferResult:
    processed: int
    remaining: int


@dataclass(frozen=True)
class ProcessDecision:
    success: bool
    checkpoint_offset: int | None = None


class FileBuffer:
    def __init__(self, base_dir: str):
        self._base_dir = base_dir
        self._logger = get_logger(__name__)
        self._lock = Lock()
        os.makedirs(self._base_dir, exist_ok=True)
        self._bad_dir = os.path.join(self._base_dir, "bad")
        os.makedirs(self._bad_dir, exist_ok=True)

    def _file_path(self, device_code: str) -> str:
        return os.path.join(self._base_dir, f"{device_code}.jsonl")

    def _bad_path(self, device_code: str) -> str:
        return os.path.join(self._bad_dir, f"{device_code}.jsonl")

    def append(self, device_code: str, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False)
        path = self._file_path(device_code)
        with self._lock:
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def list_devices(self) -> list[str]:
        files = []
        for name in os.listdir(self._base_dir):
            if not name.endswith(".jsonl"):
                continue
            if name == "bad":
                continue
            files.append(name[:-6])
        return files

    def process(self, device_code: str, handler: Callable[[dict], ProcessDecision]) -> BufferResult:
        path = self._file_path(device_code)
        if not os.path.exists(path):
            return BufferResult(0, 0)

        with self._lock:
            with open(path, "r", encoding="utf-8") as handle:
                lines = handle.readlines()

            processed = 0
            safe_index = -1

            for index, line in enumerate(lines):
                raw = line.strip()
                if not raw:
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    with open(self._bad_path(device_code), "a", encoding="utf-8") as bad_handle:
                        bad_handle.write(raw + "\n")
                    self._logger.error("buffer_decode_failed", device_code=device_code)
                    continue

                try:
                    decision = handler(record)
                except Exception:
                    self._logger.exception("buffer_handler_failed", device_code=device_code)
                    decision = ProcessDecision(success=False)

                if not decision.success:
                    break

                processed += 1
                if decision.checkpoint_offset is not None:
                    checkpoint_index = index + decision.checkpoint_offset
                    if checkpoint_index >= safe_index:
                        safe_index = checkpoint_index

            remaining_lines = []
            start_index = max(safe_index + 1, 0)
            for tail in lines[start_index:]:
                remaining_lines.append(tail if tail.endswith("\n") else tail + "\n")

            if safe_index < 0:
                return BufferResult(processed, len(remaining_lines))

            if not remaining_lines:
                os.remove(path)
                return BufferResult(processed, 0)

            temp_path = path + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as temp_handle:
                temp_handle.writelines(remaining_lines)
            os.replace(temp_path, path)

            return BufferResult(processed, len(remaining_lines))