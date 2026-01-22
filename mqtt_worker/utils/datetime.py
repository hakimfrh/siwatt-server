from datetime import datetime


DATETIME_FORMAT = "%d-%m-%Y %H:%M:%S"


def parse_datetime(value: str) -> datetime:
    return datetime.strptime(value, DATETIME_FORMAT)


def floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def floor_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)