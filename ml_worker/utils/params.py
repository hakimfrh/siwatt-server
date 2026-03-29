from datetime import datetime, timezone
from typing import Any


def get_int_param(
    params: dict[str, Any],
    key: str,
    default: int,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw_value = params.get(key, default)

    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default

    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)

    return value


def get_bool_param(params: dict[str, Any], key: str, default: bool) -> bool:
    raw_value = params.get(key, default)
    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, str):
        value = raw_value.strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False

    if isinstance(raw_value, (int, float)):
        return bool(raw_value)

    return default


def get_choice_param(
    params: dict[str, Any],
    key: str,
    default: str,
    allowed: set[str],
) -> str:
    raw_value = params.get(key, default)
    if not isinstance(raw_value, str):
        return default

    normalized = raw_value.strip().lower()
    if normalized in allowed:
        return normalized
    return default


def parse_datetime_param(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    if not isinstance(value, str):
        raise ValueError("reference_end must be datetime string in ISO 8601 format")

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("reference_end must be datetime string in ISO 8601 format") from exc

    if parsed.tzinfo is None:
        return parsed

    return parsed.astimezone(timezone.utc).replace(tzinfo=None)
