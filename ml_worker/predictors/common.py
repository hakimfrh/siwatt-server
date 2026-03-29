from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd


def _smart_fill_series(series: pd.Series, weeks_limit: int) -> pd.Series:
    filled = series.copy()
    missing_times = filled[filled.isna()].index

    for t in missing_times:
        values: list[float] = []

        for h in (-1, 1):
            check_time = t + pd.Timedelta(hours=h)
            if check_time in filled.index:
                val = filled.loc[check_time]
                if pd.notna(val):
                    values.append(float(val))

        for d in (-1, 1):
            check_time = t + pd.Timedelta(days=d)
            if check_time in filled.index:
                val = filled.loc[check_time]
                if pd.notna(val):
                    values.append(float(val))

        for w in range(1, weeks_limit + 1):
            for sign in (-1, 1):
                check_time = t + pd.Timedelta(weeks=w * sign)
                if check_time in filled.index:
                    val = filled.loc[check_time]
                    if pd.notna(val):
                        values.append(float(val))

        if values:
            filled.loc[t] = float(np.median(values))

    return filled


def prepare_hourly_series(
    rows: list[dict[str, Any]],
    fill_method: str,
    smart_fill_weeks: int = 6,
    reference_end: datetime | None = None,
) -> pd.DataFrame:
    if not rows:
        raise ValueError("No data found in data_hourly for selected device")

    df = pd.DataFrame(rows)
    if "datetime" not in df.columns or "energy_hour" not in df.columns:
        raise ValueError("Hourly data must contain datetime and energy_hour columns")

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["energy_hour"] = pd.to_numeric(df["energy_hour"], errors="coerce")
    df["datetime"] = df["datetime"].dt.floor("h")
    df = df[df["energy_hour"].isna() | (df["energy_hour"] >= 0)]
    df = df.dropna(subset=["datetime"])
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")

    if reference_end is not None:
        df = df[df["datetime"] <= pd.Timestamp(reference_end)]

    if df.empty:
        raise ValueError("No data left after applying reference_end filter")

    df = df.set_index("datetime")
    df = df.asfreq("h")

    series = df["energy_hour"]
    if fill_method == "smart_fill":
        series = _smart_fill_series(series, weeks_limit=max(1, int(smart_fill_weeks)))
    elif fill_method == "ffill":
        series = series.ffill().bfill()
    elif fill_method == "interpolate":
        series = series.interpolate(method="time").ffill().bfill()
    else:
        raise ValueError("fill_method must be one of: smart_fill, interpolate, ffill")

    if series.isna().all():
        raise ValueError("energy_hour values are empty after preprocessing")

    df["energy_hour"] = series

    return df
