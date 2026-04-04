import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int, min_value: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(min_value, value)


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _env_optional_int(name: str) -> int | None:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _env_csv(name: str, default_values: list[str]) -> tuple[str, ...]:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return tuple(default_values)

    values = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
    if not values:
        return tuple(default_values)
    return tuple(values)


def _resolve_path(raw_path: str, base_dir: Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


@dataclass(frozen=True)
class WorkerConfig:
    poll_interval_seconds: int
    max_jobs_per_cycle: int
    predictions_table: str
    hourly_model_path: Path
    daily_model_path: Path
    default_history_hours: int
    default_horizon_hourly: int
    default_horizon_daily: int
    default_fill_method: str
    default_smart_fill_weeks: int
    default_allow_partial_daily: bool
    enable_retrain: bool
    retrain_interval_days: int
    retrain_history_days: int
    retrain_model_types: tuple[str, ...]
    retrain_output_dir: Path
    retrain_train_log_table: str
    retrain_device_id: int | None
    retrain_hourly_epochs: int
    retrain_daily_epochs: int
    retrain_hourly_patience: int
    retrain_daily_patience: int
    retrain_batch_size: int
    retrain_min_hourly_rows: int
    retrain_min_daily_rows: int
    notify_daily_prediction: bool
    notify_url: str
    notify_api_secret: str
    notify_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        base_dir = Path(__file__).resolve().parent
        models_dir = base_dir / "models"

        fill_method = os.getenv("ML_DEFAULT_FILL_METHOD", "smart_fill").strip().lower()
        if fill_method not in {"smart_fill", "interpolate", "ffill"}:
            fill_method = "smart_fill"

        hourly_model_raw = os.getenv(
            "ML_HOURLY_MODEL_PATH",
            str(models_dir / "siwatt_lstm_hour-lag168_v2.2.keras"),
        )
        daily_model_raw = os.getenv(
            "ML_DAILY_MODEL_PATH",
            str(models_dir / "siwatt_lstm_day_v1.3.keras"),
        )
        retrain_output_raw = os.getenv("ML_RETRAIN_OUTPUT_DIR", str(models_dir / "retrained"))

        retrain_model_types = [
            item for item in _env_csv("ML_RETRAIN_MODEL_TYPES", ["hourly", "daily"])
            if item in {"hourly", "daily"}
        ]
        if not retrain_model_types:
            retrain_model_types = ["hourly", "daily"]

        return cls(
            poll_interval_seconds=_env_int("ML_POLL_INTERVAL_SECONDS", default=60, min_value=1),
            max_jobs_per_cycle=_env_int("ML_MAX_JOBS_PER_CYCLE", default=20, min_value=1),
            predictions_table=os.getenv("ML_PREDICTIONS_TABLE", "predictions").strip(),
            hourly_model_path=_resolve_path(hourly_model_raw, base_dir),
            daily_model_path=_resolve_path(daily_model_raw, base_dir),
            default_history_hours=_env_int("ML_DEFAULT_HISTORY_HOURS", default=24 * 120, min_value=24),
            default_horizon_hourly=_env_int("ML_DEFAULT_HORIZON_HOURLY", default=24, min_value=1),
            default_horizon_daily=_env_int("ML_DEFAULT_HORIZON_DAILY", default=14, min_value=1),
            default_fill_method=fill_method,
            default_smart_fill_weeks=_env_int("ML_DEFAULT_SMART_FILL_WEEKS", default=6, min_value=1),
            default_allow_partial_daily=_env_bool("ML_DEFAULT_ALLOW_PARTIAL_DAILY", default=False),
            enable_retrain=_env_bool("ML_ENABLE_RETRAIN", default=True),
            retrain_interval_days=_env_int("ML_RETRAIN_INTERVAL_DAYS", default=30, min_value=1),
            retrain_history_days=_env_int("ML_RETRAIN_HISTORY_DAYS", default=365, min_value=30),
            retrain_model_types=tuple(retrain_model_types),
            retrain_output_dir=_resolve_path(retrain_output_raw, base_dir),
            retrain_train_log_table=os.getenv("ML_TRAIN_LOG_TABLE", "train_log").strip(),
            retrain_device_id=_env_optional_int("ML_RETRAIN_DEVICE_ID"),
            retrain_hourly_epochs=_env_int("ML_RETRAIN_HOURLY_EPOCHS", default=100, min_value=1),
            retrain_daily_epochs=_env_int("ML_RETRAIN_DAILY_EPOCHS", default=200, min_value=1),
            retrain_hourly_patience=_env_int("ML_RETRAIN_HOURLY_PATIENCE", default=10, min_value=1),
            retrain_daily_patience=_env_int("ML_RETRAIN_DAILY_PATIENCE", default=15, min_value=1),
            retrain_batch_size=_env_int("ML_RETRAIN_BATCH_SIZE", default=16, min_value=1),
            retrain_min_hourly_rows=_env_int("ML_RETRAIN_MIN_HOURLY_ROWS", default=24 * 30, min_value=24),
            retrain_min_daily_rows=_env_int("ML_RETRAIN_MIN_DAILY_ROWS", default=90, min_value=14),
            notify_daily_prediction=_env_bool("ML_NOTIFY_DAILY_PREDICTION", default=True),
            notify_url=os.getenv("ML_NOTIFICATION_URL", "http://127.0.0.1:8000/notification/test").strip(),
            notify_api_secret=os.getenv("ML_NOTIFICATION_API_SECRET", os.getenv("TESTING_API_SECRET", "")).strip(),
            notify_timeout_seconds=_env_int("ML_NOTIFICATION_TIMEOUT_SECONDS", default=5, min_value=1),
        )
