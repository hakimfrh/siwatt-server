import json
import os
import re
from datetime import datetime, timedelta

from mqtt_worker.db.connection import get_connection


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Repository:
    def __init__(self):
        table_name = os.getenv("ML_PREDICTIONS_TABLE", "predictions").strip()
        if not table_name or not _TABLE_NAME_RE.fullmatch(table_name):
            table_name = "predictions"
        self._predictions_table = table_name

    def get_device(self, username: str, device_code: str) -> dict | None:
        query = """
            SELECT d.id, d.device_code, d.user_id, u.username
            FROM devices d
            JOIN users u ON u.id = d.user_id
            WHERE u.username = %s AND d.device_code = %s
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (username, device_code))
                return cursor.fetchone()

    def update_device_online(self, device_id: int, dt: datetime, uptime: int = 0) -> None:
        query = """
            UPDATE devices
            SET last_online = %s,
                up_time = %s,
                is_active = 1
            WHERE id = %s
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (dt, uptime, device_id))

    def update_devices_offline_status(self, device_ids: list[int]) -> None:
        if not device_ids:
            return
        format_strings = ",".join(["%s"] * len(device_ids))
        query = f"""
            UPDATE devices
            SET is_active = 0
            WHERE id IN ({format_strings}) AND is_active = 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, tuple(device_ids))

    def get_active_device_ids(self) -> list[int]:
        query = "SELECT id FROM devices WHERE is_active = 1"
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return [row["id"] for row in cursor.fetchall()]

    def upsert_realtime(self, device_id: int, payload: dict, dt: datetime) -> None:
        query = """
            INSERT INTO data_realtime
                (device_id, voltage, current, power, energy, frequency, pf, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                voltage = VALUES(voltage),
                current = VALUES(current),
                power = VALUES(power),
                energy = VALUES(energy),
                frequency = VALUES(frequency),
                pf = VALUES(pf),
                updated_at = VALUES(updated_at)
        """
        values = (
            device_id,
            payload["voltage"],
            payload["current"],
            payload["power"],
            payload["energy"],
            payload["frequency"],
            payload["pf"],
            dt,
        )
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, values)

    def upsert_minutely(self, device_id: int, dt: datetime, averages: dict, energy_last: float, energy_delta: float) -> None:
        select_query = """
            SELECT id FROM data_minutely
            WHERE device_id = %s AND datetime = %s
        """
        update_query = """
            UPDATE data_minutely
            SET voltage = %s,
                current = %s,
                power = %s,
                energy = %s,
                frequency = %s,
                pf = %s,
                energy_minute = %s
            WHERE device_id = %s AND datetime = %s
        """
        insert_query = """
            INSERT INTO data_minutely
                (device_id, datetime, voltage, current, power, energy, frequency, pf, energy_minute)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (device_id, dt))
                exists = cursor.fetchone()
                if exists:
                    cursor.execute(
                        update_query,
                        (
                            averages["voltage"],
                            averages["current"],
                            averages["power"],
                            energy_last,
                            averages["frequency"],
                            averages["pf"],
                            energy_delta,
                            device_id,
                            dt,
                        ),
                    )
                else:
                    cursor.execute(
                        insert_query,
                        (
                            device_id,
                            dt,
                            averages["voltage"],
                            averages["current"],
                            averages["power"],
                            energy_last,
                            averages["frequency"],
                            averages["pf"],
                            energy_delta,
                        ),
                    )

    def get_last_minutely(self, device_id: int) -> dict | None:
        query = """
            SELECT datetime, energy
            FROM data_minutely
            WHERE device_id = %s
            ORDER BY datetime DESC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (device_id,))
                return cursor.fetchone()

    def get_hourly_from_minutely(self, device_id: int, hour_start: datetime) -> dict | None:
        hour_end = hour_start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        avg_query = """
            SELECT
                AVG(voltage) AS voltage,
                AVG(current) AS current,
                AVG(power) AS power,
                AVG(frequency) AS frequency,
                AVG(pf) AS pf,
                COUNT(*) AS count
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
        """
        first_query = """
            SELECT energy
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
            ORDER BY datetime ASC
            LIMIT 1
        """
        last_query = """
            SELECT energy
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
            ORDER BY datetime DESC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(avg_query, (device_id, hour_start, hour_end))
                avg_row = cursor.fetchone()
                if not avg_row or avg_row["count"] == 0:
                    return None

                cursor.execute(first_query, (device_id, hour_start, hour_end))
                first_row = cursor.fetchone()
                cursor.execute(last_query, (device_id, hour_start, hour_end))
                last_row = cursor.fetchone()

        if not first_row or not last_row:
            return None

        energy_first = float(first_row["energy"])
        energy_last = float(last_row["energy"])
        energy_delta = energy_last - energy_first
        averages = {
            "voltage": float(avg_row["voltage"]),
            "current": float(avg_row["current"]),
            "power": float(avg_row["power"]),
            "frequency": float(avg_row["frequency"]),
            "pf": float(avg_row["pf"]),
        }
        return {
            "averages": averages,
            "energy_last": energy_last,
            "energy_delta": energy_delta,
        }

    def get_hourly_legacy(self, device_id: int, hour_start: datetime) -> dict | None:
        prev_hour = hour_start - timedelta(hours=1)
        hour_end = hour_start + timedelta(hours=1)
        avg_query = """
            SELECT
                AVG(voltage) AS voltage,
                AVG(current) AS current,
                AVG(power) AS power,
                AVG(frequency) AS frequency,
                AVG(pf) AS pf,
                COUNT(*) AS count
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
        """
        prev_hourly_query = """
            SELECT energy
            FROM data_hourly
            WHERE device_id = %s AND datetime = %s
            LIMIT 1
        """
        prev_minutely_query = """
            SELECT energy
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
            ORDER BY datetime ASC
            LIMIT 1
        """
        curr_first_query = """
            SELECT energy
            FROM data_minutely
            WHERE device_id = %s AND datetime >= %s AND datetime < %s
            ORDER BY datetime ASC
            LIMIT 1
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(avg_query, (device_id, hour_start, hour_end))
                avg_row = cursor.fetchone()
                if not avg_row or avg_row["count"] == 0:
                    return None

                cursor.execute(prev_hourly_query, (device_id, prev_hour))
                prev_row = cursor.fetchone()
                if not prev_row:
                    cursor.execute(prev_minutely_query, (device_id, prev_hour, hour_start))
                    prev_row = cursor.fetchone()
                if not prev_row:
                    return None

                cursor.execute(curr_first_query, (device_id, hour_start, hour_end))
                curr_first = cursor.fetchone()
                if not curr_first:
                    return None

        energy_before = float(prev_row["energy"])
        energy_after = float(curr_first["energy"])
        energy_delta = round((energy_after - energy_before) * 1000) / 1000
        averages = {
            "voltage": float(avg_row["voltage"]),
            "current": float(avg_row["current"]),
            "power": float(avg_row["power"]),
            "frequency": float(avg_row["frequency"]),
            "pf": float(avg_row["pf"]),
        }
        return {
            "averages": averages,
            "energy_delta": energy_delta,
            "energy_after": energy_after,
        }

    def upsert_hourly(self, device_id: int, dt: datetime, averages: dict, energy_last: float, energy_delta: float) -> None:
        select_query = """
            SELECT id FROM data_hourly
            WHERE device_id = %s AND datetime = %s
        """
        update_query = """
            UPDATE data_hourly
            SET voltage = %s,
                current = %s,
                power = %s,
                energy = %s,
                frequency = %s,
                pf = %s,
                energy_hour = %s
            WHERE device_id = %s AND datetime = %s
        """
        insert_query = """
            INSERT INTO data_hourly
                (device_id, datetime, voltage, current, power, energy, frequency, pf, energy_hour)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (device_id, dt))
                exists = cursor.fetchone()
                if exists:
                    cursor.execute(
                        update_query,
                        (
                            averages["voltage"],
                            averages["current"],
                            averages["power"],
                            energy_last,
                            averages["frequency"],
                            averages["pf"],
                            energy_delta,
                            device_id,
                            dt,
                        ),
                    )
                else:
                    cursor.execute(
                        insert_query,
                        (
                            device_id,
                            dt,
                            averages["voltage"],
                            averages["current"],
                            averages["power"],
                            energy_last,
                            averages["frequency"],
                            averages["pf"],
                            energy_delta,
                        ),
                    )

    def decrement_token_balance(self, device_id: int, amount: float) -> None:
        query = """
            UPDATE devices
            SET token_balance = GREATEST(token_balance - %s, 0)
            WHERE id = %s
        """
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (amount, device_id))

    def enqueue_prediction_job(self, device_id: int, prediction_type: str, history_end: datetime) -> bool:
        if prediction_type not in ("hourly", "daily"):
            raise ValueError("prediction_type must be 'hourly' or 'daily'")

        history_end_value = history_end.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")
        params_payload = json.dumps({"history_end": history_end_value}, ensure_ascii=False, separators=(",", ":"))

        insert_query = f"""
            INSERT INTO {self._predictions_table}
                (user_id, device_id, type, status, params, created_at)
            SELECT d.user_id, d.id, %s, %s, %s, NOW()
            FROM devices d
            WHERE d.id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM {self._predictions_table}
                WHERE user_id = d.user_id
                  AND device_id = d.id
                  AND type = %s
                  AND params = %s
            )
        """

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    (
                        prediction_type,
                        "pending",
                        params_payload,
                        device_id,
                        prediction_type,
                        params_payload,
                    ),
                )
                return int(cursor.rowcount or 0) > 0