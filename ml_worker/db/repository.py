import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ml_worker.db.connection import get_connection


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(slots=True)
class PredictionJob:
    id: int
    user_id: int
    device_id: int
    job_type: str
    status: str
    params: dict[str, Any]
    created_at: datetime | None


class PredictionRepository:
    def __init__(self, predictions_table: str, train_log_table: str | None = None):
        table = predictions_table.strip()
        if not _TABLE_NAME_RE.fullmatch(table):
            raise ValueError("Invalid predictions table name")
        self._table = table

        if train_log_table is None or train_log_table.strip() == "":
            self._train_table = None
        else:
            parsed_train_table = train_log_table.strip()
            if not _TABLE_NAME_RE.fullmatch(parsed_train_table):
                raise ValueError("Invalid train log table name")
            self._train_table = parsed_train_table

    @staticmethod
    def _parse_params(raw_value: Any) -> dict[str, Any]:
        if raw_value is None:
            return {}

        if isinstance(raw_value, dict):
            return raw_value

        if isinstance(raw_value, (bytes, bytearray)):
            raw_value = raw_value.decode("utf-8", errors="ignore")

        if isinstance(raw_value, str):
            text = raw_value.strip()
            if text == "":
                return {}
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}

        return {}

    @staticmethod
    def _json_default(value: Any):
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @staticmethod
    def _progress_payload(percentage: int, info: str) -> str:
        payload = {
            "percentage": max(0, min(100, int(percentage))),
            "info": info,
        }
        return json.dumps(payload, ensure_ascii=False)

    def _require_train_table(self) -> str:
        if self._train_table is None:
            raise ValueError("Train log table is not configured")
        return self._train_table

    def claim_next_pending_job(self) -> PredictionJob | None:
        select_query = f"""
            SELECT id, user_id, device_id, type, status, params, created_at
            FROM {self._table}
            WHERE status = %s
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            FOR UPDATE
        """
        update_query = f"""
            UPDATE {self._table}
            SET status = %s,
                progress = %s,
                started_at = NOW(),
                finished_at = NULL,
                error_message = NULL
            WHERE id = %s
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, ("pending",))
                row = cursor.fetchone()
                if not row:
                    return None

                cursor.execute(
                    update_query,
                    (
                        "running",
                        self._progress_payload(5, "claimed"),
                        row["id"],
                    ),
                )

        return PredictionJob(
            id=int(row["id"]),
            user_id=int(row["user_id"]),
            device_id=int(row["device_id"]),
            job_type=str(row["type"]),
            status="running",
            params=self._parse_params(row.get("params")),
            created_at=row.get("created_at"),
        )

    def update_progress(self, job_id: int, percentage: int, info: str) -> None:
        query = f"""
            UPDATE {self._table}
            SET progress = %s
            WHERE id = %s
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (self._progress_payload(percentage, info), job_id))

    def mark_done(self, job_id: int, result_payload: dict[str, Any]) -> None:
        query = f"""
            UPDATE {self._table}
            SET status = %s,
                progress = %s,
                result = %s,
                error_message = NULL,
                finished_at = NOW()
            WHERE id = %s
        """
        payload = json.dumps(result_payload, ensure_ascii=False, default=self._json_default)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        "done",
                        self._progress_payload(100, "done"),
                        payload,
                        job_id,
                    ),
                )

    def mark_error(self, job_id: int, message: str, percentage: int = 100, info: str = "error") -> None:
        query = f"""
            UPDATE {self._table}
            SET status = %s,
                progress = %s,
                error_message = %s,
                finished_at = NOW()
            WHERE id = %s
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        "error",
                        self._progress_payload(percentage, info),
                        message[:2000],
                        job_id,
                    ),
                )

    def fetch_hourly_energy(
        self,
        device_id: int,
        limit_hours: int | None = None,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if start_datetime is not None and end_datetime is not None and start_datetime > end_datetime:
            raise ValueError("history_start cannot be greater than history_end")

        params: list[Any] = [device_id]
        where_clauses = ["device_id = %s", "energy_hour IS NOT NULL"]

        if start_datetime is not None:
            where_clauses.append("datetime >= %s")
            params.append(start_datetime)

        if end_datetime is not None:
            where_clauses.append("datetime <= %s")
            params.append(end_datetime)

        where_sql = " AND ".join(where_clauses)

        if limit_hours is None:
            query = f"""
                SELECT datetime, energy_hour
                FROM data_hourly
                WHERE {where_sql}
                ORDER BY datetime ASC
            """
        else:
            query = f"""
                SELECT datetime, energy_hour
                FROM data_hourly
                WHERE {where_sql}
                ORDER BY datetime DESC
                LIMIT %s
            """
            params.append(limit_hours)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, tuple(params))
                rows = cursor.fetchall() or []

        if limit_hours is not None:
            rows.reverse()
        return rows

    def get_device_context(self, device_id: int) -> dict[str, Any] | None:
        query = """
            SELECT d.id, d.user_id, d.device_code, d.device_name, u.username
            FROM devices d
            LEFT JOIN users u ON u.id = d.user_id
            WHERE d.id = %s
            LIMIT 1
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (device_id,))
                return cursor.fetchone()

    def get_last_done_train_time(self, model_type: str) -> datetime | None:
        train_table = self._require_train_table()
        query = f"""
            SELECT train_time
            FROM {train_table}
            WHERE model_type = %s
              AND status = %s
            ORDER BY train_time DESC, train_id DESC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (model_type, "done"))
                row = cursor.fetchone()
                return None if row is None else row["train_time"]

    def has_running_train(self, model_type: str) -> bool:
        train_table = self._require_train_table()
        query = f"""
            SELECT 1
            FROM {train_table}
            WHERE model_type = %s
              AND status = %s
            ORDER BY train_id DESC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (model_type, "running"))
                row = cursor.fetchone()
                return row is not None

    def get_latest_done_model_path(self, model_type: str) -> str | None:
        train_table = self._require_train_table()
        query = f"""
            SELECT path
            FROM {train_table}
            WHERE model_type = %s
              AND status = %s
              AND path IS NOT NULL
              AND path != ''
            ORDER BY train_time DESC, train_id DESC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (model_type, "done"))
                row = cursor.fetchone()
                return None if row is None else str(row["path"])

    def start_train_log(
        self,
        model_type: str,
        source_path: str,
        details: dict[str, Any] | None = None,
    ) -> int:
        train_table = self._require_train_table()
        query = f"""
            INSERT INTO {train_table}
                (model_type, status, path, train_time, epoch, details, train_result, error_message)
            VALUES
                (%s, %s, %s, NOW(), NULL, %s, NULL, NULL)
        """
        details_payload = json.dumps(details or {}, ensure_ascii=False, default=self._json_default)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (model_type, "running", source_path, details_payload))
                return int(cursor.lastrowid)

    def finish_train_log_done(
        self,
        train_id: int,
        path: str,
        epoch: int,
        details: dict[str, Any],
        train_result: dict[str, Any],
    ) -> None:
        train_table = self._require_train_table()
        query = f"""
            UPDATE {train_table}
            SET status = %s,
                path = %s,
                train_time = NOW(),
                epoch = %s,
                details = %s,
                train_result = %s,
                error_message = NULL
            WHERE train_id = %s
        """
        details_payload = json.dumps(details, ensure_ascii=False, default=self._json_default)
        result_payload = json.dumps(train_result, ensure_ascii=False, default=self._json_default)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, ("done", path, epoch, details_payload, result_payload, train_id))

    def update_train_log_details(self, train_id: int, details: dict[str, Any]) -> None:
        train_table = self._require_train_table()
        query = f"""
            UPDATE {train_table}
            SET details = %s
            WHERE train_id = %s
        """
        details_payload = json.dumps(details, ensure_ascii=False, default=self._json_default)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (details_payload, train_id))

    def finish_train_log_error(
        self,
        train_id: int,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        train_table = self._require_train_table()
        query = f"""
            UPDATE {train_table}
            SET status = %s,
                train_time = NOW(),
                details = %s,
                error_message = %s
            WHERE train_id = %s
        """
        details_payload = json.dumps(details or {}, ensure_ascii=False, default=self._json_default)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, ("error", details_payload, message[:2000], train_id))

    def fetch_retrain_device_counts(
        self,
        start_datetime: datetime,
        device_id: int | None = None,
    ) -> list[dict[str, Any]]:
        where_clauses = [
            "datetime >= %s",
            "energy_hour IS NOT NULL",
            "energy_hour >= 0",
        ]
        params: list[Any] = [start_datetime]

        if device_id is not None:
            where_clauses.append("device_id = %s")
            params.append(device_id)

        where_sql = " AND ".join(where_clauses)
        query = f"""
            SELECT device_id, COUNT(*) AS row_count
            FROM data_hourly
            WHERE {where_sql}
            GROUP BY device_id
            ORDER BY device_id ASC
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, tuple(params))
                return cursor.fetchall() or []

    def fetch_retrain_hourly_series(
        self,
        start_datetime: datetime,
        device_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if device_id is None:
            query = """
                SELECT datetime, AVG(energy_hour) AS energy_hour
                FROM data_hourly
                WHERE datetime >= %s
                  AND energy_hour IS NOT NULL
                  AND energy_hour >= 0
                GROUP BY datetime
                ORDER BY datetime ASC
            """
            params = (start_datetime,)
        else:
            query = """
                SELECT datetime, energy_hour
                FROM data_hourly
                WHERE device_id = %s
                  AND datetime >= %s
                  AND energy_hour IS NOT NULL
                  AND energy_hour >= 0
                ORDER BY datetime ASC
            """
            params = (device_id, start_datetime)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall() or []
