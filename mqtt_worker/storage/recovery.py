from typing import Callable

from mqtt_worker.storage.file_buffer import FileBuffer, ProcessDecision
from mqtt_worker.utils.logger import get_logger


class RecoveryManager:
    def __init__(self, buffer: FileBuffer):
        self._buffer = buffer
        self._logger = get_logger(__name__)

    def replay_all(self, handler_factory: Callable[[str], Callable[[dict], ProcessDecision]]) -> None:
        for device_code in self._buffer.list_devices():
            handler = handler_factory(device_code)
            result = self._buffer.process(device_code, handler)
            self._logger.info(
                "recovery_processed",
                device_code=device_code,
                processed=result.processed,
                remaining=result.remaining,
            )