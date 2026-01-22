from dataclasses import dataclass
from datetime import datetime

from mqtt_worker.utils.datetime import floor_minute


FIELDS = ("voltage", "current", "power", "frequency", "pf")


@dataclass
class MinuteAggregate:
    minute_start: datetime
    averages: dict
    energy_last: float
    energy_delta: float


class MinuteAggregator:
    def __init__(self):
        self._minute_start: datetime | None = None
        self._count = 0
        self._sums = {field: 0.0 for field in FIELDS}
        self._energy_first: float | None = None
        self._energy_last: float | None = None

    def add(self, payload: dict, dt: datetime) -> MinuteAggregate | None:
        minute_start = floor_minute(dt)
        if self._minute_start is None:
            self._start_bucket(minute_start, payload)
            return None

        if minute_start == self._minute_start:
            self._accumulate(payload)
            return None

        aggregate = self._finalize()
        self._start_bucket(minute_start, payload)
        return aggregate

    def _start_bucket(self, minute_start: datetime, payload: dict) -> None:
        self._minute_start = minute_start
        self._count = 0
        self._sums = {field: 0.0 for field in FIELDS}
        self._energy_first = None
        self._energy_last = None
        self._accumulate(payload)

    def _accumulate(self, payload: dict) -> None:
        self._count += 1
        for field in FIELDS:
            self._sums[field] += float(payload[field])
        energy_value = float(payload["energy"])
        if self._energy_first is None:
            self._energy_first = energy_value
        self._energy_last = energy_value

    def _finalize(self) -> MinuteAggregate | None:
        if self._count == 0 or self._minute_start is None or self._energy_first is None or self._energy_last is None:
            return None

        averages = {field: self._sums[field] / self._count for field in FIELDS}
        energy_delta = self._energy_last - self._energy_first
        return MinuteAggregate(
            minute_start=self._minute_start,
            averages=averages,
            energy_last=self._energy_last,
            energy_delta=energy_delta,
        )