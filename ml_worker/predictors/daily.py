from datetime import datetime, timezone
import importlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ml_worker.predictors.common import prepare_hourly_series
from ml_worker.utils.params import (
    get_bool_param,
    get_choice_param,
    get_int_param,
    parse_datetime_param,
)


class DailyPredictor:
    def __init__(
        self,
        model_path: Path,
        default_horizon: int,
        default_fill_method: str,
        default_smart_fill_weeks: int,
        default_allow_partial_daily: bool,
    ):
        self._model_path = model_path
        self._default_horizon = default_horizon
        self._default_fill_method = default_fill_method
        self._default_smart_fill_weeks = default_smart_fill_weeks
        self._default_allow_partial_daily = default_allow_partial_daily
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
                raise FileNotFoundError(f"Daily model file not found: {self._model_path}")
            self._model = load_model(self._model_path)
        return self._model

    @staticmethod
    def _to_daily(hourly_df: pd.DataFrame, allow_partial_daily: bool) -> pd.DataFrame:
        hourly_series = hourly_df["energy_hour"]
        daily_sum = hourly_series.resample("D").sum(min_count=1)

        if allow_partial_daily:
            daily = daily_sum.dropna().to_frame(name="energy_hour")
            return daily

        daily_count = hourly_series.resample("D").count()
        valid_days = daily_count[daily_count == 24].index
        daily = daily_sum[daily_sum.index.isin(valid_days)]
        return daily.dropna().to_frame(name="energy_hour")

    @staticmethod
    def _engineer_features(daily_df: pd.DataFrame) -> pd.DataFrame:
        features = pd.DataFrame(index=daily_df.index)
        dt_index = pd.DatetimeIndex(features.index)
        features["energy_hour"] = daily_df["energy_hour"]
        features["dayofweek"] = dt_index.dayofweek.astype(float)
        features["dow_sin"] = np.sin(2 * np.pi * dt_index.dayofweek / 7)
        features["dow_cos"] = np.cos(2 * np.pi * dt_index.dayofweek / 7)
        features["dayofyear"] = dt_index.dayofyear.astype(float)
        features["doy_sin"] = np.sin(2 * np.pi * dt_index.dayofyear / 365)
        features["doy_cos"] = np.cos(2 * np.pi * dt_index.dayofyear / 365)
        return features.dropna()

    @staticmethod
    def _select_feature_columns(feature_count: int) -> list[str]:
        ordered_columns = [
            "energy_hour",
            "dayofweek",
            "dow_sin",
            "dow_cos",
            "dayofyear",
            "doy_sin",
            "doy_cos",
        ]

        if feature_count > len(ordered_columns):
            raise ValueError(
                f"Daily model expects {feature_count} features, but max supported is {len(ordered_columns)}"
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
        allow_partial_daily = get_bool_param(
            params,
            key="allow_partial_daily",
            default=self._default_allow_partial_daily,
        )
        reference_end = parse_datetime_param(params.get("reference_end"))

        hourly_df = prepare_hourly_series(
            rows,
            fill_method=fill_method,
            smart_fill_weeks=smart_fill_weeks,
            reference_end=reference_end,
        )
        daily_df = self._to_daily(hourly_df, allow_partial_daily=allow_partial_daily)
        if daily_df.empty:
            raise ValueError("No daily data available after aggregation")

        model = self._get_model()

        input_shape = model.input_shape
        if isinstance(input_shape, list):
            input_shape = input_shape[0]
        if len(input_shape) != 3:
            raise ValueError("Daily model must have input shape (batch, window, features)")

        output_shape = model.output_shape
        if isinstance(output_shape, list):
            output_shape = output_shape[0]
        if len(output_shape) < 2:
            raise ValueError("Daily model output shape is invalid")

        window_size = int(input_shape[1])
        feature_count = int(input_shape[2])
        max_horizon = int(output_shape[-1])

        feature_columns = self._select_feature_columns(feature_count)
        features = self._engineer_features(daily_df)
        if features.empty:
            raise ValueError("Not enough daily history after feature engineering")

        model_frame = features[feature_columns]

        if len(model_frame) < window_size:
            raise ValueError(
                f"Insufficient data for daily prediction. Need at least {window_size} points after preprocessing"
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

        base_date = model_frame.index[-1]
        future_dates = pd.date_range(start=base_date + pd.Timedelta(days=1), periods=requested_horizon, freq="D")

        predictions = [
            {
                "date": ts.date().isoformat(),
                "energy_day": float(value),
            }
            for ts, value in zip(future_dates, pred_real)
        ]

        return {
            "model_type": "daily",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_size": window_size,
            "horizon": requested_horizon,
            "max_horizon": max_horizon,
            "feature_columns": feature_columns,
            "history_start": model_frame.index[0].date().isoformat(),
            "history_end": model_frame.index[-1].date().isoformat(),
            "history_days": int(len(model_frame)),
            "allow_partial_daily": allow_partial_daily,
            "fill_method": fill_method,
            "smart_fill_weeks": smart_fill_weeks,
            "predictions": predictions,
        }
