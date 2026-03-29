import time
from datetime import datetime, timezone
from typing import Any

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
            self.repo.update_progress(job.id, progress_percentage, progress_info)
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
            self.repo.update_progress(job.id, progress_percentage, progress_info)
            prediction = self._run_predictor(job.job_type, rows, params)

            progress_percentage = 90
            progress_info = "saving_result"
            self.repo.update_progress(job.id, progress_percentage, progress_info)
            device_context = self.repo.get_device_context(job.device_id)
            result_payload = self._build_result_payload(job, device_context, prediction)
            self.repo.mark_done(job.id, result_payload)

            self.logger.info(
                "prediction_job_done",
                extra={
                    "job_id": job.id,
                    "job_type": job.job_type,
                    "horizon": prediction.get("horizon"),
                },
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
