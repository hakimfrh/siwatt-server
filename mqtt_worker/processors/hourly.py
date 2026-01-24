from datetime import datetime

from mqtt_worker.db.repository import Repository
from mqtt_worker.utils.logger import get_logger


class HourlyProcessor:
    def __init__(self, repository: Repository):
        self._repo = repository
        self._logger = get_logger(__name__)

    def handle(
        self,
        device_id: int,
        hour_range_start: datetime,
        insert_dt: datetime,
        energy_value: float,
    ) -> tuple[bool, float | None]:
        try:
            aggregate = self._repo.get_hourly_legacy(device_id, hour_range_start)
            if not aggregate:
                self._logger.warning(
                    "hourly_no_data",
                    device_id=device_id,
                    hour_start=hour_range_start.isoformat(),
                )
                return True, None

            self._repo.upsert_hourly(
                device_id=device_id,
                dt=insert_dt,
                averages=aggregate["averages"],
                energy_last=energy_value,
                energy_delta=aggregate["energy_delta"],
            )
            return True, aggregate["energy_delta"]
        except Exception:
            self._logger.exception(
                "hourly_insert_failed",
                device_id=device_id,
                hour_start=hour_range_start.isoformat(),
            )
            return False, None