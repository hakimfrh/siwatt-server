import json
import time
from datetime import date, datetime, timezone
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from dotenv import load_dotenv

from ml_worker.config import WorkerConfig
from ml_worker.db.repository import PredictionJob, PredictionRepository
from ml_worker.predictors.daily import DailyPredictor
from ml_worker.predictors.hourly import HourlyPredictor
from ml_worker.retrain.trainer import AutoRetrainer
from ml_worker.utils.logger import get_logger
from ml_worker.utils.params import get_int_param, parse_datetime_param


load_dotenv()


class PredictionWorker:
    def __init__(self, config: WorkerConfig | None = None):
        self.config = config or WorkerConfig.from_env()
        self.logger = get_logger(__name__)
        self.repo = PredictionRepository(
            self.config.predictions_table,
            train_log_table=self.config.retrain_train_log_table,
        )
        self.hourly_predictor = HourlyPredictor(
            model_path=self.config.hourly_model_path,
            default_horizon=self.config.default_horizon_hourly,
            default_fill_method=self.config.default_fill_method,
            default_smart_fill_weeks=self.config.default_smart_fill_weeks,
        )
        self.daily_predictor = DailyPredictor(
            model_path=self.config.daily_model_path,
            default_horizon=self.config.default_horizon_daily,
            default_fill_method=self.config.default_fill_method,
            default_smart_fill_weeks=self.config.default_smart_fill_weeks,
            default_allow_partial_daily=self.config.default_allow_partial_daily,
        )
        self.retrainer = AutoRetrainer(self.config, self.repo, self.logger)
        self._sync_latest_models_from_train_log()

    @staticmethod
    def _calculate_estimated_days_from_daily_prediction(
        token_balance: float,
        prediction_result: dict[str, Any],
        reference_date: date,
    ) -> tuple[int, bool]:
        raw_predictions = prediction_result.get("predictions")
        if not isinstance(raw_predictions, list):
            return 0, False

        usable_predictions: list[tuple[date, float]] = []
        for item in raw_predictions:
            if not isinstance(item, dict):
                continue

            raw_date = item.get("date")
            raw_energy_day = item.get("energy_day")
            if raw_date is None or raw_energy_day is None:
                continue

            try:
                prediction_date = datetime.fromisoformat(str(raw_date)).date()
                energy_day = float(raw_energy_day)
            except (TypeError, ValueError):
                continue

            if prediction_date < reference_date or energy_day <= 0:
                continue

            usable_predictions.append((prediction_date, energy_day))

        if not usable_predictions:
            return 0, False

        usable_predictions.sort(key=lambda value: value[0])

        estimated_days = 0
        remaining_balance = token_balance
        for _, energy_day in usable_predictions:
            if remaining_balance < energy_day:
                break
            remaining_balance -= energy_day
            estimated_days += 1

        exceeded_prediction_horizon = estimated_days == len(usable_predictions) and remaining_balance > 0
        return estimated_days, exceeded_prediction_horizon

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _send_daily_prediction_notification_once(
        self,
        job: PredictionJob,
        prediction: dict[str, Any],
        device_context: dict[str, Any] | None,
    ) -> None:
        if not self.config.notify_daily_prediction:
            return

        if not self.config.notify_url:
            self.logger.warning("daily_notification_skipped_missing_url", extra={"job_id": job.id})
            return

        if not self.config.notify_api_secret:
            self.logger.warning("daily_notification_skipped_missing_secret", extra={"job_id": job.id})
            return

        if self.repo.is_daily_notification_sent(job.id):
            self.logger.info("daily_notification_already_sent", extra={"job_id": job.id})
            return

        token_balance = self._to_float((device_context or {}).get("token_balance"), default=0.0)
        estimated_days, exceeded_prediction_horizon = self._calculate_estimated_days_from_daily_prediction(
            token_balance=token_balance,
            prediction_result=prediction,
            reference_date=datetime.now().date(),
        )
        estimated_days_display = f"{estimated_days}+" if exceeded_prediction_horizon else str(estimated_days)

        device_name = str((device_context or {}).get("device_name") or f"Device {job.device_id}")
        title = "Prediksi Harian SiWatt"
        body = (
            f"Prediksi harian untuk {device_name} selesai. "
            f"Sisa token saat ini: {token_balance:.3f} kWh. "
            f"Perkiraan sisa masa aktif token: {estimated_days_display} hari."
        )
        payload = {
            "title": title,
            "body": body,
            "user_id": job.user_id,
            "data": {
                "type": "daily-prediction",
                "prediction_job_id": str(job.id),
                "device_id": str(job.device_id),
                "estimated_days": str(estimated_days),
                "estimated_days_display": estimated_days_display,
                "estimated_days_overflow": "1" if exceeded_prediction_horizon else "0",
                "remaining_token_kwh": f"{token_balance:.3f}",
                "token_balance": f"{token_balance:.3f}",
            },
        }

        request_body = json.dumps(payload).encode("utf-8")
        req = urlrequest.Request(
            self.config.notify_url,
            data=request_body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Api-Secret": self.config.notify_api_secret,
            },
        )

        try:
            with urlrequest.urlopen(req, timeout=self.config.notify_timeout_seconds) as response:
                response_text = response.read().decode("utf-8", errors="ignore")
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"Notification API returned {response.status}: {response_text}")
        except (urlerror.URLError, urlerror.HTTPError, RuntimeError):
            self.logger.exception(
                "daily_notification_failed",
                extra={
                    "job_id": job.id,
                    "url": self.config.notify_url,
                },
            )
            return

        self.repo.mark_daily_notification_sent(job.id)
        self.logger.info(
            "daily_notification_sent",
            extra={
                "job_id": job.id,
                "user_id": job.user_id,
                "device_id": job.device_id,
                "estimated_days": estimated_days,
                "estimated_days_display": estimated_days_display,
            },
        )

    def _sync_latest_models_from_train_log(self) -> None:
        try:
            hourly_latest = self.retrainer.get_latest_done_model_path("hourly")
            if hourly_latest is not None and hourly_latest.exists():
                self.hourly_predictor.update_model_path(hourly_latest)
                self.logger.info("hourly_model_updated_from_train_log", extra={"path": str(hourly_latest)})

            daily_latest = self.retrainer.get_latest_done_model_path("daily")
            if daily_latest is not None and daily_latest.exists():
                self.daily_predictor.update_model_path(daily_latest)
                self.logger.info("daily_model_updated_from_train_log", extra={"path": str(daily_latest)})
        except Exception:
            self.logger.exception("sync_latest_model_from_train_log_failed")

    def _run_predictor(
        self,
        job_type: str,
        rows: list[dict[str, Any]],
        params: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_type = job_type.strip().lower()
        if normalized_type == "hourly":
            return self.hourly_predictor.predict(rows, params)
        if normalized_type == "daily":
            return self.daily_predictor.predict(rows, params)
        raise ValueError(f"Unsupported prediction type: {job_type}")

    def _resolve_model_metadata(self, job_type: str) -> tuple[str, str]:
        normalized_type = job_type.strip().lower()
        if normalized_type == "hourly":
            return "hourly", str(self.hourly_predictor.model_path)
        if normalized_type == "daily":
            return "daily", str(self.daily_predictor.model_path)
        raise ValueError(f"Unsupported prediction type: {job_type}")

    @staticmethod
    def _build_result_payload(
        job: PredictionJob,
        device_context: dict[str, Any] | None,
        prediction: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "job": {
                "id": job.id,
                "type": job.job_type,
                "user_id": job.user_id,
                "device_id": job.device_id,
                "created_at": job.created_at.isoformat() if job.created_at else None,
            },
            "device": device_context,
            "prediction": prediction,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    def process_next_job(self) -> bool:
        job = self.repo.claim_next_pending_job()
        if job is None:
            return False

        progress_percentage = 5
        progress_info = "claimed"
        model_used = job.job_type.strip().lower()
        model_path: str | None = None

        self.logger.info(
            "prediction_job_claimed",
            extra={
                "job_id": job.id,
                "job_type": job.job_type,
                "user_id": job.user_id,
                "device_id": job.device_id,
            },
        )

        try:
            model_used, model_path = self._resolve_model_metadata(job.job_type)
            params = job.params or {}
            history_hours = get_int_param(
                params,
                key="history_hours",
                default=self.config.default_history_hours,
                min_value=24,
                max_value=24 * 365 * 5,
            )
            history_start_param = parse_datetime_param(params.get("history_start"))
            history_end_param = parse_datetime_param(params.get("history_end"))
            reference_end = parse_datetime_param(params.get("reference_end"))

            history_start = history_start_param
            history_end = history_end_param if history_end_param is not None else reference_end

            if history_start is not None and history_end is not None and history_start > history_end:
                raise ValueError("history_start cannot be greater than history_end")

            has_explicit_history_params = history_start_param is not None or history_end_param is not None

            if has_explicit_history_params:
                # If only history_end is provided, use history_hours as lookback window ending at history_end.
                if history_start_param is None and history_end_param is not None:
                    limit_hours = history_hours
                else:
                    # history_start only or history_start+history_end: use explicit range as provided.
                    limit_hours = None
            else:
                # Legacy mode: history_hours with optional reference_end anchor.
                limit_hours = history_hours

            progress_percentage = 20
            progress_info = "cleaning_data"
            self.repo.update_progress(
                job.id,
                progress_percentage,
                progress_info,
                model_used=model_used,
                model_path=model_path,
            )
            rows = self.repo.fetch_hourly_energy(
                device_id=job.device_id,
                limit_hours=limit_hours,
                start_datetime=history_start,
                end_datetime=history_end,
            )
            if not rows:
                raise ValueError("No records found in data_hourly for this device")

            progress_percentage = 60
            progress_info = "predicting"
            self.repo.update_progress(
                job.id,
                progress_percentage,
                progress_info,
                model_used=model_used,
                model_path=model_path,
            )
            prediction = self._run_predictor(job.job_type, rows, params)

            progress_percentage = 90
            progress_info = "saving_result"
            self.repo.update_progress(
                job.id,
                progress_percentage,
                progress_info,
                model_used=model_used,
                model_path=model_path,
            )
            device_context = self.repo.get_device_context(job.device_id)
            result_payload = self._build_result_payload(job, device_context, prediction)
            self.repo.mark_done(
                job.id,
                result_payload,
                model_used=model_used,
                model_path=model_path,
            )

            self.logger.info(
                "prediction_job_done",
                extra={
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "horizon": prediction.get("horizon"),
                },
            )

            if model_used == "daily":
                self._send_daily_prediction_notification_once(
                    job=job,
                    prediction=prediction,
                    device_context=device_context,
                )
        except Exception as exc:
            error_message = str(exc)
            self.logger.exception("prediction_job_failed", extra={"job_id": job.id})
            try:
                self.repo.mark_error(
                    job.id,
                    error_message,
                    percentage=progress_percentage,
                    info=f"error:{progress_info}",
                    model_used=model_used,
                    model_path=model_path,
                )
            except Exception:
                self.logger.exception("prediction_mark_error_failed", extra={"job_id": job.id})

        return True

    def run_forever(self) -> None:
        self.logger.info(
            "ml_worker_started",
            extra={
                "poll_interval_seconds": self.config.poll_interval_seconds,
                "max_jobs_per_cycle": self.config.max_jobs_per_cycle,
                "predictions_table": self.config.predictions_table,
                "hourly_model_path": str(self.config.hourly_model_path),
                "daily_model_path": str(self.config.daily_model_path),
                "notify_daily_prediction": self.config.notify_daily_prediction,
                "notify_url": self.config.notify_url,
            },
        )

        try:
            while True:
                processed = 0
                for _ in range(self.config.max_jobs_per_cycle):
                    has_more = self.process_next_job()
                    if not has_more:
                        break
                    processed += 1

                self.retrainer.maybe_run(self.hourly_predictor, self.daily_predictor)

                self.logger.info("ml_worker_cycle", extra={"processed": processed})
                time.sleep(self.config.poll_interval_seconds)
        except KeyboardInterrupt:
            self.logger.info("ml_worker_stopping")


def main() -> None:
    worker = PredictionWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()
