from datetime import datetime

from mqtt_worker.db.repository import Repository
from mqtt_worker.utils.logger import get_logger


class RealtimeProcessor:
    def __init__(self, repository: Repository):
        self._repo = repository
        self._logger = get_logger(__name__)

    def handle(self, device_id: int, payload: dict, dt: datetime) -> bool:
        try:
            self._repo.upsert_realtime(device_id, payload, dt)
            self._repo.update_device_online(device_id, dt)
            return True
        except Exception:
            self._logger.exception("realtime_update_failed", device_id=device_id)
            return False