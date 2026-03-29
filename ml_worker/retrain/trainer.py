from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import importlib
from pathlib import Path
import re
from typing import Any, Callable

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler

from ml_worker.config import WorkerConfig
from ml_worker.db.repository import PredictionRepository
from ml_worker.predictors.common import prepare_hourly_series
from ml_worker.predictors.daily import DailyPredictor
from ml_worker.predictors.hourly import HourlyPredictor


@dataclass(slots=True)
class RetrainOutput:
    model_path: Path
    epoch_count: int
    details: dict[str, Any]
    train_result: dict[str, Any]


def _create_dataset(data: np.ndarray, window: int, forecast: int) -> tuple[np.ndarray, np.ndarray]:
    x_values: list[np.ndarray] = []
    y_values: list[np.ndarray] = []

    for i in range(len(data) - window - forecast + 1):
        x_values.append(data[i : i + window, :])
        y_values.append(data[i + window : i + window + forecast, 0])

    if not x_values:
        return np.empty((0, window, data.shape[1])), np.empty((0, forecast))

    return np.array(x_values), np.array(y_values)


def _inverse_target(scaler: MinMaxScaler, values: np.ndarray, feature_count: int) -> np.ndarray:
    n_samples, horizon = values.shape
    dummy = np.zeros((n_samples * horizon, feature_count))
    dummy[:, 0] = values.reshape(-1)
    return scaler.inverse_transform(dummy)[:, 0].reshape(n_samples, horizon)


class AutoRetrainer:
    def __init__(self, config: WorkerConfig, repo: PredictionRepository, logger):
        self.config = config
        self.repo = repo
        self.logger = logger

    def get_latest_done_model_path(self, model_type: str) -> Path | None:
        try:
            path = self.repo.get_latest_done_model_path(model_type)
        except Exception:
            self.logger.exception("retrain_latest_model_query_failed", extra={"model_type": model_type})
            return None

        if path is None or str(path).strip() == "":
            return None

        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj

        return (Path(__file__).resolve().parents[1] / path_obj).resolve()

    def _is_due(self, model_type: str, now: datetime) -> bool:
        last_done = self.repo.get_last_done_train_time(model_type)
        if last_done is None:
            return True
        return now >= (last_done + timedelta(days=self.config.retrain_interval_days))

    @staticmethod
    def _normalize_output_stem(stem: str) -> str:
        normalized = re.sub(r"_retrain_\d{8}_\d{6}$", "", stem)
        normalized = re.sub(r"_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$", "", normalized)
        normalized = normalized.replace(":", "-")
        return normalized

    def _output_model_path(self, model_type: str, source_model_path: Path) -> Path:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        source_stem = self._normalize_output_stem(source_model_path.stem)
        out_dir = self.config.retrain_output_dir / model_type
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"{source_stem}_{stamp}.keras"

    @staticmethod
    def _get_model_shapes(model) -> tuple[int, int, int]:
        input_shape = model.input_shape
        if isinstance(input_shape, list):
            input_shape = input_shape[0]

        output_shape = model.output_shape
        if isinstance(output_shape, list):
            output_shape = output_shape[0]

        window = int(input_shape[1])
        feature_count = int(input_shape[2])
        forecast = int(output_shape[-1])
        return window, feature_count, forecast

    def _fit_model(
        self,
        model,
        x_train: np.ndarray,
        y_train: np.ndarray,
        x_val: np.ndarray,
        y_val: np.ndarray,
        model_type: str,
    ):
        callbacks_mod = importlib.import_module("tensorflow.keras.callbacks")
        losses_mod = importlib.import_module("tensorflow.keras.losses")
        optimizers_mod = importlib.import_module("tensorflow.keras.optimizers")

        EarlyStopping = getattr(callbacks_mod, "EarlyStopping")
        ReduceLROnPlateau = getattr(callbacks_mod, "ReduceLROnPlateau")
        Huber = getattr(losses_mod, "Huber")
        Adam = getattr(optimizers_mod, "Adam")

        model.compile(optimizer=Adam(learning_rate=0.001), loss=Huber())

        if model_type == "daily":
            patience = self.config.retrain_daily_patience
            epochs = self.config.retrain_daily_epochs
            callbacks = [
                EarlyStopping(patience=patience, restore_best_weights=True),
                ReduceLROnPlateau(factor=0.5, patience=max(2, patience // 2), min_lr=1e-5),
            ]
        else:
            patience = self.config.retrain_hourly_patience
            epochs = self.config.retrain_hourly_epochs
            callbacks = [EarlyStopping(patience=patience, restore_best_weights=True)]

        history = model.fit(
            x_train,
            y_train,
            validation_data=(x_val, y_val),
            epochs=epochs,
            batch_size=self.config.retrain_batch_size,
            callbacks=callbacks,
            verbose=0,
        )
        return history

    def _prepare_hourly_training_frame(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        return prepare_hourly_series(
            rows,
            fill_method=self.config.default_fill_method,
            smart_fill_weeks=self.config.default_smart_fill_weeks,
            reference_end=None,
        )

    def _prepare_daily_training_frame(self, hourly_df: pd.DataFrame) -> pd.DataFrame:
        hourly_series = hourly_df["energy_hour"]
        daily_sum = hourly_series.resample("D").sum(min_count=1)
        daily_count = hourly_series.resample("D").count()
        valid_days = daily_count[daily_count == 24].index
        daily = daily_sum[daily_sum.index.isin(valid_days)]
        return daily.dropna().to_frame(name="energy_hour")

    def _get_eligible_device_ids(
        self,
        start_datetime: datetime,
        minimum_source_rows: int,
    ) -> tuple[list[int], list[int], dict[int, int]]:
        minimum_source_rows = max(1, int(minimum_source_rows))
        candidates = self.repo.fetch_retrain_device_counts(
            start_datetime=start_datetime,
            device_id=self.config.retrain_device_id,
        )

        eligible: list[int] = []
        skipped: list[int] = []
        source_counts: dict[int, int] = {}

        for row in candidates:
            device_id = int(row["device_id"])
            row_count = int(row["row_count"])
            source_counts[device_id] = row_count
            if row_count >= minimum_source_rows:
                eligible.append(device_id)
            else:
                skipped.append(device_id)

        return eligible, skipped, source_counts

    @staticmethod
    def _fit_scaler_and_build_samples(
        frames: list[pd.DataFrame],
        device_ids: list[int],
        window: int,
        forecast: int,
    ) -> tuple[np.ndarray, np.ndarray, MinMaxScaler, dict[int, int]]:
        if not frames:
            raise ValueError("No per-device feature frames available")

        scaler = MinMaxScaler()
        combined = pd.concat(frames, axis=0)
        scaler.fit(combined)

        x_parts: list[np.ndarray] = []
        y_parts: list[np.ndarray] = []
        sample_counts: dict[int, int] = {}

        for device_id, frame in zip(device_ids, frames):
            scaled = scaler.transform(frame)
            x_part, y_part = _create_dataset(scaled, window, forecast)
            sample_counts[device_id] = int(len(x_part))
            if len(x_part) == 0:
                continue
            x_parts.append(x_part)
            y_parts.append(y_part)

        if not x_parts:
            raise ValueError("No training samples produced from per-device windows")

        return np.concatenate(x_parts, axis=0), np.concatenate(y_parts, axis=0), scaler, sample_counts

    def _report_running_task(
        self,
        train_id: int,
        running_details: dict[str, Any],
        task: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        updated = dict(running_details)
        updated["current_task"] = task
        updated["updated_at"] = datetime.now(timezone.utc).isoformat()
        if extra:
            updated.update(extra)

        try:
            self.repo.update_train_log_details(train_id=train_id, details=updated)
        except Exception:
            self.logger.exception("retrain_task_update_failed", extra={"train_id": train_id, "task": task})

        return updated

    def _retrain_hourly(
        self,
        source_model_path: Path,
        start_datetime: datetime,
        report_task: Callable[[str, dict[str, Any] | None], None] | None = None,
    ) -> RetrainOutput:
        if report_task is not None:
            report_task("loading_model", None)

        keras_models = importlib.import_module("tensorflow.keras.models")
        load_model = getattr(keras_models, "load_model")

        model = load_model(source_model_path)
        window, feature_count, forecast = self._get_model_shapes(model)

        feature_columns = HourlyPredictor._select_feature_columns(feature_count)

        minimum_rows = max(self.config.retrain_min_hourly_rows, window + forecast + 5)
        if report_task is not None:
            report_task("selecting_devices", {"minimum_rows_per_device": minimum_rows})

        eligible_devices, skipped_devices, source_counts = self._get_eligible_device_ids(
            start_datetime=start_datetime,
            minimum_source_rows=minimum_rows,
        )
        if not eligible_devices:
            raise ValueError("No device has enough hourly rows for retraining")

        frames: list[pd.DataFrame] = []
        used_devices: list[int] = []

        if report_task is not None:
            report_task("cleaning_data", {"candidate_device_count": len(eligible_devices)})

        for device_id in eligible_devices:
            rows = self.repo.fetch_retrain_hourly_series(start_datetime=start_datetime, device_id=device_id)
            if not rows:
                continue

            hourly_df = self._prepare_hourly_training_frame(rows)
            features = HourlyPredictor._engineer_features(hourly_df)
            frame = features[feature_columns]

            if len(frame) < minimum_rows:
                skipped_devices.append(device_id)
                continue

            frames.append(frame)
            used_devices.append(device_id)

        if not frames:
            raise ValueError("No eligible hourly device series after preprocessing")

        if report_task is not None:
            report_task("building_dataset", {"used_device_count": len(used_devices)})

        x_data, y_data, scaler, sample_counts = self._fit_scaler_and_build_samples(
            frames=frames,
            device_ids=used_devices,
            window=window,
            forecast=forecast,
        )
        if len(x_data) < 10:
            raise ValueError("Not enough hourly samples after windowing")

        # Shuffle gabungan sample lintas device agar split train/val tidak bias urutan device.
        permutation = np.random.default_rng(42).permutation(len(x_data))
        x_data = x_data[permutation]
        y_data = y_data[permutation]

        split = int(len(x_data) * 0.8)
        split = min(max(split, 1), len(x_data) - 1)
        x_train, x_val = x_data[:split], x_data[split:]
        y_train, y_val = y_data[:split], y_data[split:]

        if report_task is not None:
            report_task("training", {"train_samples": int(len(x_train)), "validation_samples": int(len(x_val))})

        history = self._fit_model(model, x_train, y_train, x_val, y_val, model_type="hourly")

        if report_task is not None:
            report_task("validating", None)

        pred_val = model.predict(x_val, verbose=0)
        pred_real = _inverse_target(scaler, pred_val, feature_count=x_data.shape[2])
        y_real = _inverse_target(scaler, y_val, feature_count=x_data.shape[2])

        if report_task is not None:
            report_task("saving_model", None)

        out_path = self._output_model_path("hourly", source_model_path=source_model_path)
        model.save(out_path)

        train_losses = history.history.get("loss", [])
        val_losses = history.history.get("val_loss", [])
        best_idx = int(np.argmin(val_losses)) if val_losses else max(0, len(train_losses) - 1)

        details = {
            "model_type": "hourly",
            "status": "done",
            "current_task": "done",
            "source_model_path": str(source_model_path),
            "output_model_path": str(out_path),
            "series_mode": "per_device",
            "history_start": min(frame.index.min() for frame in frames).to_pydatetime().isoformat(),
            "history_end": max(frame.index.max() for frame in frames).to_pydatetime().isoformat(),
            "history_points": int(sum(len(frame) for frame in frames)),
            "window_size": window,
            "forecast_size": forecast,
            "feature_columns": feature_columns,
            "train_samples": int(len(x_train)),
            "validation_samples": int(len(x_val)),
            "fill_method": self.config.default_fill_method,
            "smart_fill_weeks": self.config.default_smart_fill_weeks,
            "device_id": self.config.retrain_device_id,
            "device_count": int(len(used_devices)),
            "device_ids": used_devices,
            "skipped_device_ids": sorted(set(skipped_devices)),
            "minimum_rows_per_device": minimum_rows,
            "retrain_history_days": self.config.retrain_history_days,
        }

        train_result = {
            "best_epoch": best_idx + 1,
            "epochs_ran": len(train_losses),
            "loss_best": float(val_losses[best_idx]) if val_losses else None,
            "loss_train_at_best": float(train_losses[best_idx]) if train_losses else None,
            "mae": float(mean_absolute_error(y_real.reshape(-1), pred_real.reshape(-1))),
            "rmse": float(np.sqrt(mean_squared_error(y_real.reshape(-1), pred_real.reshape(-1)))),
            "mape": float(
                np.mean(
                    np.abs(
                        (y_real.reshape(-1) - pred_real.reshape(-1))
                        / np.clip(np.abs(y_real.reshape(-1)), 1e-6, None)
                    )
                )
                * 100
            ),
            "sample_count_by_device": sample_counts,
            "source_row_count_by_device": source_counts,
        }

        return RetrainOutput(
            model_path=out_path,
            epoch_count=len(train_losses),
            details=details,
            train_result=train_result,
        )

    def _retrain_daily(
        self,
        source_model_path: Path,
        start_datetime: datetime,
        report_task: Callable[[str, dict[str, Any] | None], None] | None = None,
    ) -> RetrainOutput:
        if report_task is not None:
            report_task("loading_model", None)

        keras_models = importlib.import_module("tensorflow.keras.models")
        load_model = getattr(keras_models, "load_model")

        model = load_model(source_model_path)
        window, feature_count, forecast = self._get_model_shapes(model)

        feature_columns = DailyPredictor._select_feature_columns(feature_count)

        minimum_daily_rows = max(self.config.retrain_min_daily_rows, window + forecast + 5)
        minimum_hourly_rows = max(self.config.retrain_min_hourly_rows, minimum_daily_rows * 24)

        if report_task is not None:
            report_task(
                "selecting_devices",
                {
                    "minimum_daily_rows_per_device": minimum_daily_rows,
                    "minimum_hourly_rows_per_device": minimum_hourly_rows,
                },
            )

        eligible_devices, skipped_devices, source_counts = self._get_eligible_device_ids(
            start_datetime=start_datetime,
            minimum_source_rows=minimum_hourly_rows,
        )
        if not eligible_devices:
            raise ValueError("No device has enough hourly history for daily retraining")

        frames: list[pd.DataFrame] = []
        used_devices: list[int] = []

        if report_task is not None:
            report_task("cleaning_data", {"candidate_device_count": len(eligible_devices)})

        for device_id in eligible_devices:
            rows = self.repo.fetch_retrain_hourly_series(start_datetime=start_datetime, device_id=device_id)
            if not rows:
                continue

            hourly_df = self._prepare_hourly_training_frame(rows)
            daily_df = self._prepare_daily_training_frame(hourly_df)
            features = DailyPredictor._engineer_features(daily_df)
            frame = features[feature_columns]

            if len(frame) < minimum_daily_rows:
                skipped_devices.append(device_id)
                continue

            frames.append(frame)
            used_devices.append(device_id)

        if not frames:
            raise ValueError("No eligible daily device series after preprocessing")

        if report_task is not None:
            report_task("building_dataset", {"used_device_count": len(used_devices)})

        x_data, y_data, scaler, sample_counts = self._fit_scaler_and_build_samples(
            frames=frames,
            device_ids=used_devices,
            window=window,
            forecast=forecast,
        )
        if len(x_data) < 10:
            raise ValueError("Not enough daily samples after windowing")

        # Shuffle gabungan sample lintas device agar split train/val tidak bias urutan device.
        permutation = np.random.default_rng(42).permutation(len(x_data))
        x_data = x_data[permutation]
        y_data = y_data[permutation]

        split = int(len(x_data) * 0.8)
        split = min(max(split, 1), len(x_data) - 1)
        x_train, x_val = x_data[:split], x_data[split:]
        y_train, y_val = y_data[:split], y_data[split:]

        if report_task is not None:
            report_task("training", {"train_samples": int(len(x_train)), "validation_samples": int(len(x_val))})

        history = self._fit_model(model, x_train, y_train, x_val, y_val, model_type="daily")

        if report_task is not None:
            report_task("validating", None)

        pred_val = model.predict(x_val, verbose=0)
        pred_real = _inverse_target(scaler, pred_val, feature_count=x_data.shape[2])
        y_real = _inverse_target(scaler, y_val, feature_count=x_data.shape[2])

        if report_task is not None:
            report_task("saving_model", None)

        out_path = self._output_model_path("daily", source_model_path=source_model_path)
        model.save(out_path)

        train_losses = history.history.get("loss", [])
        val_losses = history.history.get("val_loss", [])
        best_idx = int(np.argmin(val_losses)) if val_losses else max(0, len(train_losses) - 1)

        details = {
            "model_type": "daily",
            "status": "done",
            "current_task": "done",
            "source_model_path": str(source_model_path),
            "output_model_path": str(out_path),
            "series_mode": "per_device",
            "history_start": min(frame.index.min() for frame in frames).date().isoformat(),
            "history_end": max(frame.index.max() for frame in frames).date().isoformat(),
            "history_days": int(sum(len(frame) for frame in frames)),
            "window_size": window,
            "forecast_size": forecast,
            "feature_columns": feature_columns,
            "train_samples": int(len(x_train)),
            "validation_samples": int(len(x_val)),
            "fill_method": self.config.default_fill_method,
            "smart_fill_weeks": self.config.default_smart_fill_weeks,
            "device_id": self.config.retrain_device_id,
            "device_count": int(len(used_devices)),
            "device_ids": used_devices,
            "skipped_device_ids": sorted(set(skipped_devices)),
            "minimum_daily_rows_per_device": minimum_daily_rows,
            "minimum_hourly_rows_per_device": minimum_hourly_rows,
            "retrain_history_days": self.config.retrain_history_days,
        }

        train_result = {
            "best_epoch": best_idx + 1,
            "epochs_ran": len(train_losses),
            "loss_best": float(val_losses[best_idx]) if val_losses else None,
            "loss_train_at_best": float(train_losses[best_idx]) if train_losses else None,
            "mae": float(mean_absolute_error(y_real.reshape(-1), pred_real.reshape(-1))),
            "rmse": float(np.sqrt(mean_squared_error(y_real.reshape(-1), pred_real.reshape(-1)))),
            "mape": float(
                np.mean(
                    np.abs(
                        (y_real.reshape(-1) - pred_real.reshape(-1))
                        / np.clip(np.abs(y_real.reshape(-1)), 1e-6, None)
                    )
                )
                * 100
            ),
            "sample_count_by_device": sample_counts,
            "source_row_count_by_device": source_counts,
        }

        return RetrainOutput(
            model_path=out_path,
            epoch_count=len(train_losses),
            details=details,
            train_result=train_result,
        )

    def maybe_run(self, hourly_predictor: HourlyPredictor, daily_predictor: DailyPredictor) -> None:
        if not self.config.enable_retrain:
            return

        now = datetime.now()
        for model_type in self.config.retrain_model_types:
            train_id: int | None = None
            try:
                if self.repo.has_running_train(model_type):
                    continue

                if not self._is_due(model_type, now):
                    continue

                if model_type == "hourly":
                    predictor = hourly_predictor
                elif model_type == "daily":
                    predictor = daily_predictor
                else:
                    continue

                latest_path = self.get_latest_done_model_path(model_type)
                source_path = latest_path if latest_path is not None else predictor.model_path
                if not source_path.exists():
                    raise FileNotFoundError(f"Source model not found for retrain: {source_path}")

                train_id = self.repo.start_train_log(
                    model_type=model_type,
                    source_path=str(source_path),
                    details={
                        "status": "running",
                        "trigger": "schedule",
                        "current_task": "queued",
                        "interval_days": self.config.retrain_interval_days,
                    },
                )
                if not isinstance(train_id, int):
                    raise ValueError("Failed to create train log id")
                running_train_id = train_id

                running_details = {
                    "status": "running",
                    "trigger": "schedule",
                    "model_type": model_type,
                    "source_model_path": str(source_path),
                    "interval_days": self.config.retrain_interval_days,
                }

                running_details = self._report_running_task(
                    train_id=running_train_id,
                    running_details=running_details,
                    task="starting",
                )

                def report_task(task: str, extra: dict[str, Any] | None = None) -> None:
                    nonlocal running_details
                    running_details = self._report_running_task(
                        train_id=running_train_id,
                        running_details=running_details,
                        task=task,
                        extra=extra,
                    )

                start_datetime = now - timedelta(days=self.config.retrain_history_days)
                if model_type == "hourly":
                    output = self._retrain_hourly(
                        source_model_path=source_path,
                        start_datetime=start_datetime,
                        report_task=report_task,
                    )
                else:
                    output = self._retrain_daily(
                        source_model_path=source_path,
                        start_datetime=start_datetime,
                        report_task=report_task,
                    )

                output.details["updated_at"] = datetime.now(timezone.utc).isoformat()
                self.repo.finish_train_log_done(
                    train_id=running_train_id,
                    path=str(output.model_path),
                    epoch=output.epoch_count,
                    details=output.details,
                    train_result=output.train_result,
                )
                predictor.update_model_path(output.model_path)

                self.logger.info(
                    "retrain_done",
                    extra={
                        "model_type": model_type,
                        "train_id": train_id,
                        "model_path": str(output.model_path),
                        "epochs": output.epoch_count,
                        "mae": output.train_result.get("mae"),
                    },
                )
            except Exception as exc:
                self.logger.exception("retrain_failed", extra={"model_type": model_type})
                try:
                    if isinstance(train_id, int):
                        error_details = {
                            "status": "error",
                            "trigger": "schedule",
                            "current_task": "failed",
                            "model_type": model_type,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }
                        self.repo.finish_train_log_error(
                            train_id=train_id,
                            message=str(exc),
                            details=error_details,
                        )
                except Exception:
                    self.logger.exception("retrain_log_error_update_failed", extra={"model_type": model_type})
