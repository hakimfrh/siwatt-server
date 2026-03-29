from datetime import datetime, timezone
import importlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ml_worker.predictors.common import prepare_hourly_series
from ml_worker.utils.params import get_choice_param, get_int_param, parse_datetime_param


class HourlyPredictor:
    def __init__(
        self,
        model_path: Path,
        default_horizon: int,
        default_fill_method: str,
        default_smart_fill_weeks: int,
    ):
        self._model_path = model_path
        self._default_horizon = default_horizon
        self._default_fill_method = default_fill_method
        self._default_smart_fill_weeks = default_smart_fill_weeks
        self._model = None

    @property
    def model_path(self) -> Path:
        return self._model_path

    def update_model_path(self, model_path: Path) -> None:
        self._model_path = model_path
        self._model = None

    def _get_model(self):
        if self._model is None:
            keras_models = importlib.import_module("tensorflow.keras.models")
            load_model = getattr(keras_models, "load_model")

            if not self._model_path.exists():
                raise FileNotFoundError(f"Hourly model file not found: {self._model_path}")
            self._model = load_model(self._model_path)
        return self._model

    @staticmethod
    def _engineer_features(hourly_df: pd.DataFrame) -> pd.DataFrame:
        features = pd.DataFrame(index=hourly_df.index)
        dt_index = pd.DatetimeIndex(features.index)
        features["energy_hour"] = hourly_df["energy_hour"]
        features["hour"] = dt_index.hour.astype(float)
        features["hour_sin"] = np.sin(2 * np.pi * dt_index.hour / 24)
        features["hour_cos"] = np.cos(2 * np.pi * dt_index.hour / 24)
        features["dayofweek"] = dt_index.dayofweek.astype(float)
        features["day_sin"] = np.sin(2 * np.pi * dt_index.dayofweek / 7)
        features["day_cos"] = np.cos(2 * np.pi * dt_index.dayofweek / 7)
        features["lag_168"] = features["energy_hour"].shift(168)
        return features.dropna()

    @staticmethod
    def _select_feature_columns(feature_count: int) -> list[str]:
        ordered_columns = [
            "energy_hour",
            "hour",
            "hour_sin",
            "hour_cos",
            "dayofweek",
            "day_sin",
            "day_cos",
            "lag_168",
        ]

        if feature_count > len(ordered_columns):
            raise ValueError(
                f"Hourly model expects {feature_count} features, but max supported is {len(ordered_columns)}"
            )

        return ordered_columns[:feature_count]

    def predict(self, rows: list[dict[str, Any]], params: dict[str, Any]) -> dict[str, Any]:
        fill_method = get_choice_param(
            params,
            key="fill_method",
            default=self._default_fill_method,
            allowed={"smart_fill", "interpolate", "ffill"},
        )
        smart_fill_weeks = get_int_param(
            params,
            key="smart_fill_weeks",
            default=self._default_smart_fill_weeks,
            min_value=1,
            max_value=26,
        )
        reference_end = parse_datetime_param(params.get("reference_end"))

        hourly_df = prepare_hourly_series(
            rows,
            fill_method=fill_method,
            smart_fill_weeks=smart_fill_weeks,
            reference_end=reference_end,
        )
        features = self._engineer_features(hourly_df)

        if features.empty:
            raise ValueError("Not enough hourly history after lag feature engineering")

        model = self._get_model()

        input_shape = model.input_shape
        if isinstance(input_shape, list):
            input_shape = input_shape[0]
        if len(input_shape) != 3:
            raise ValueError("Hourly model must have input shape (batch, window, features)")

        output_shape = model.output_shape
        if isinstance(output_shape, list):
            output_shape = output_shape[0]
        if len(output_shape) < 2:
            raise ValueError("Hourly model output shape is invalid")

        window_size = int(input_shape[1])
        feature_count = int(input_shape[2])
        max_horizon = int(output_shape[-1])

        feature_columns = self._select_feature_columns(feature_count)
        model_frame = features[feature_columns]

        if len(model_frame) < window_size:
            raise ValueError(
                f"Insufficient data for hourly prediction. Need at least {window_size} points after preprocessing"
            )

        requested_horizon = get_int_param(
            params,
            key="horizon",
            default=self._default_horizon,
            min_value=1,
            max_value=max_horizon,
        )

        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(model_frame)

        input_seq = scaled[-window_size:]
        pred_scaled = model.predict(input_seq[np.newaxis, :, :], verbose=0)[0]
        pred_scaled = np.asarray(pred_scaled).reshape(-1)[:requested_horizon]

        dummy = np.zeros((requested_horizon, scaled.shape[1]))
        dummy[:, 0] = pred_scaled
        pred_real = scaler.inverse_transform(dummy)[:, 0]

        base_time = model_frame.index[-1]
        future_index = pd.date_range(start=base_time + pd.Timedelta(hours=1), periods=requested_horizon, freq="h")

        predictions = [
            {
                "datetime": ts.to_pydatetime().isoformat(),
                "energy_hour": float(value),
            }
            for ts, value in zip(future_index, pred_real)
        ]

        return {
            "model_type": "hourly",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_size": window_size,
            "horizon": requested_horizon,
            "max_horizon": max_horizon,
            "feature_columns": feature_columns,
            "history_start": model_frame.index[0].to_pydatetime().isoformat(),
            "history_end": model_frame.index[-1].to_pydatetime().isoformat(),
            "history_points": int(len(model_frame)),
            "fill_method": fill_method,
            "smart_fill_weeks": smart_fill_weeks,
            "predictions": predictions,
        }
