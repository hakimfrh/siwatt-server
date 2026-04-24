import json
import os
import time
from datetime import datetime
from typing import Callable, Optional, Tuple

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from mqtt_worker.db.repository import Repository
from mqtt_worker.mqtt.client import create_client
from mqtt_worker.mqtt.subscriber import Subscriber
from mqtt_worker.processors.hourly import HourlyProcessor
from mqtt_worker.processors.minute import MinuteAggregator
from mqtt_worker.processors.realtime import RealtimeProcessor
from mqtt_worker.storage.file_buffer import FileBuffer, ProcessDecision
from mqtt_worker.storage.recovery import RecoveryManager
from mqtt_worker.utils.datetime import floor_hour, parse_datetime
from mqtt_worker.utils.logger import get_logger


load_dotenv()

TOPIC_WILDCARD = os.getenv("MQTT_TOPIC_WILDCARD", "/siwatt-mqtt/+/swm-raw/+")
TOPIC_MODE = os.getenv("MQTT_TOPIC_MODE", "prefixed").lower()


def _is_enabled(value: str | None, default: bool = False) -> bool:
	if value is None:
		return default
	return value.strip().lower() in {"enable", "enabled", "true", "1", "yes", "on"}


def _parse_trigger_time(value: str | None, default_hour: int, default_minute: int) -> tuple[int, int]:
	if value is None:
		return default_hour, default_minute

	text = value.strip()
	if text == "":
		return default_hour, default_minute

	try:
		hour_text, minute_text = text.split(":", 1)
		hour = int(hour_text)
		minute = int(minute_text)
		if 0 <= hour <= 23 and 0 <= minute <= 59:
			return hour, minute
	except Exception:
		pass

	return default_hour, default_minute


def _parse_positive_float(value: str | None, default: float) -> float:
	if value is None:
		return default

	text = value.strip()
	if text == "":
		return default

	try:
		parsed = float(text)
		if parsed > 0:
			return parsed
	except Exception:
		pass

	return default


def _parse_min_int(value: str | None, default: int, minimum: int) -> int:
	if value is None:
		return default

	text = value.strip()
	if text == "":
		return default

	try:
		parsed = int(text)
		if parsed >= minimum:
			return parsed
	except Exception:
		pass

	return default


class AggregationPipeline:
	def __init__(
		self,
		repo: Repository,
		realtime: RealtimeProcessor,
		hourly: HourlyProcessor,
		logger,
		balance_mode: str,
		prediction_hourly_enabled: bool,
		prediction_hourly_trigger: tuple[int, int],
		prediction_daily_enabled: bool,
		prediction_daily_trigger: tuple[int, int],
		pzem_overflow_after_hourly_handler: Optional[Callable[[str, str, float], bool]] = None,
	):
		self._repo = repo
		self._realtime = realtime
		self._hourly = hourly
		self._logger = logger
		self._minute_agg = MinuteAggregator()
		self._last_processed_dt = None
		self._balance_mode = balance_mode
		self._prediction_hourly_enabled = prediction_hourly_enabled
		self._prediction_hourly_trigger = prediction_hourly_trigger
		self._prediction_daily_enabled = prediction_daily_enabled
		self._prediction_daily_trigger = prediction_daily_trigger
		self._pzem_overflow_after_hourly_handler = pzem_overflow_after_hourly_handler
		self._ignore_previous_energy_reference = False
		self._skip_hourly_transition_once = False

	@staticmethod
	def _is_trigger_match(hour_mark: datetime, trigger: tuple[int, int]) -> bool:
		return hour_mark.hour == trigger[0] and hour_mark.minute == trigger[1]

	def _enqueue_prediction_job(self, device_id: int, prediction_type: str, history_end: datetime) -> None:
		try:
			inserted = self._repo.enqueue_prediction_job(device_id, prediction_type, history_end)
			if inserted:
				self._logger.info(
					"prediction_job_enqueued",
					device_id=device_id,
					prediction_type=prediction_type,
					history_end=history_end.strftime("%Y-%m-%dT%H:%M:%S"),
				)
		except Exception:
			self._logger.exception(
				"prediction_job_enqueue_failed",
				device_id=device_id,
				prediction_type=prediction_type,
				history_end=history_end.strftime("%Y-%m-%dT%H:%M:%S"),
			)

	def reset_datetime_state(self):
		"""Reset referensi waktu di pipeline.
		Dipanggil setelah sync-rtc dikirim, agar data dengan waktu
		yang sudah dikoreksi tidak di-drop oleh pengecekan dt <= _last_processed_dt.
		"""
		self._last_processed_dt = None
		self._minute_agg = MinuteAggregator()

	def mark_energy_reset_event(self):
		"""Reset state agregasi setelah device reset energi ke 0 kWh.
		Ini mencegah pembacaan baru ikut baseline jam/menit sebelumnya.
		"""
		self._last_processed_dt = None
		self._minute_agg = MinuteAggregator()
		self._ignore_previous_energy_reference = True
		self._skip_hourly_transition_once = True

	def handle(self, record: dict) -> ProcessDecision:
		try:
			payload = record["payload"]
			dt = parse_datetime(payload["datetime"])
			device_id = record["device_id"]
			username = record["username"]
			device_code = record["device_code"]
		except Exception:
			self._logger.exception("record_parse_failed", record=record)
			return ProcessDecision(success=False)

		if self._last_processed_dt and dt <= self._last_processed_dt:
			return ProcessDecision(success=True)

		if not self._realtime.handle(device_id, payload, dt):
			return ProcessDecision(success=False)

		aggregate = self._minute_agg.add(payload, dt)
		self._last_processed_dt = dt

		if not aggregate:
			return ProcessDecision(success=True)

		energy_before = aggregate.energy_first
		if self._ignore_previous_energy_reference:
			self._ignore_previous_energy_reference = False
		else:
			try:
				last_row = self._repo.get_last_minutely(device_id)
				if last_row:
					last_dt = last_row["datetime"]
					# Use previous minute's energy as baseline if available
					# This captures consumption between the last sample of previous minute
					# and first sample of current minute
					if last_dt < aggregate.minute_mark:
						energy_before = float(last_row["energy"])
			except Exception:
				self._logger.exception("minutely_energy_before_failed", device_id=device_id)

		energy_delta = round((aggregate.energy_last - energy_before) * 1000) / 1000

		# Clamp: jika energy_delta negatif (misal PZEM di-reset), set 0
		# Tanpa ini, delta negatif bisa menambah saldo token
		if energy_delta < 0:
			self._logger.warning(
				"energy_delta_negative",
				device_id=device_id,
				energy_delta=energy_delta,
				energy_before=energy_before,
				energy_last=aggregate.energy_last,
			)
			energy_delta = 0.0

		try:
			self._repo.upsert_minutely(
				device_id=device_id,
				dt=aggregate.minute_mark,
				averages=aggregate.averages,
				energy_last=aggregate.energy_last,
				energy_delta=energy_delta,
			)
		except Exception:
			self._logger.exception("minutely_insert_failed", device_id=device_id)
			return ProcessDecision(success=False)

		if self._balance_mode == "minute":
			try:
				self._repo.decrement_token_balance(device_id, energy_delta)
			except Exception:
				self._logger.exception("balance_minute_update_failed", device_id=device_id)
				return ProcessDecision(success=False)

		current_hour = floor_hour(dt)
		if current_hour != aggregate.bucket_hour:
			hourly_saved = False
			if self._skip_hourly_transition_once:
				self._skip_hourly_transition_once = False
			else:
				success, energy_delta = self._hourly.handle(
					device_id,
					aggregate.bucket_hour,
					current_hour,
					aggregate.energy_last,
				)
				if not success:
					return ProcessDecision(success=False)
				hourly_saved = energy_delta is not None
				if self._balance_mode == "hour" and energy_delta is not None:
					try:
						self._repo.decrement_token_balance(device_id, energy_delta)
					except Exception:
						self._logger.exception("balance_hour_update_failed", device_id=device_id)
						return ProcessDecision(success=False)

				if energy_delta is not None:
					if self._prediction_hourly_enabled and self._is_trigger_match(current_hour, self._prediction_hourly_trigger):
						self._enqueue_prediction_job(device_id, "hourly", current_hour)

					if self._prediction_daily_enabled and self._is_trigger_match(current_hour, self._prediction_daily_trigger):
						self._enqueue_prediction_job(device_id, "daily", current_hour)

			if hourly_saved and self._pzem_overflow_after_hourly_handler:
				try:
					self._pzem_overflow_after_hourly_handler(username, device_code, aggregate.energy_last)
				except Exception:
					self._logger.exception("pzem_overflow_after_hourly_handler_failed", device_code=device_code)

		return ProcessDecision(success=True, checkpoint_offset=-1)


# Batas toleransi datetime dari device
_DATETIME_MAX_FUTURE_SECONDS = 120     # Maks 2 menit di depan waktu server
_DATETIME_MAX_PAST_SECONDS = 300       # Maks 5 menit di belakang waktu server
_DATETIME_MIN_YEAR = 2024              # Tahun minimum yang valid
_DATETIME_MAX_YEAR = 2027              # Tahun maksimum yang valid
_SYNC_COMMAND_COOLDOWN = 60            # Cooldown kirim sync-rtc (detik)
_DATETIME_BACKWARD_TOLERANCE = 5       # Toleransi waktu mundur (detik)


class Worker:
	def __init__(self):
		self._logger = get_logger(__name__)
		self._repo = Repository()
		base_dir = os.path.join(os.path.dirname(__file__), "data", "buffer")
		self._buffer = FileBuffer(base_dir)
		self._recovery = RecoveryManager(self._buffer)
		self._realtime = RealtimeProcessor(self._repo)
		self._hourly = HourlyProcessor(self._repo)
		self._pipelines: dict[str, AggregationPipeline] = {}
		self._last_seen: dict[int, float] = {}
		self._balance_mode = os.getenv("BALANCE_DECREASE_MODE", "minute").lower()
		if self._balance_mode not in ("minute", "hour"):
			self._balance_mode = "minute"

		self._prediction_hourly_enabled = _is_enabled(os.getenv("PREDICTION_HOURLY", "disable"))
		self._prediction_daily_enabled = _is_enabled(os.getenv("PREDICTION_DAILY", "disable"))
		self._prediction_hourly_trigger = _parse_trigger_time(
			os.getenv("PREDICTION_HOURLY_TRIGGER", "23:00"),
			23,
			0,
		)
		self._prediction_daily_trigger = _parse_trigger_time(
			os.getenv("PREDICTION_DAILY_TRIGGER", "00:00"),
			0,
			0,
		)
		self._auto_pzem_reset_enabled = _is_enabled(os.getenv("AUTO_PZEM_RESET_OVERFLOW", "disable"))
		self._auto_pzem_reset_threshold_kwh = _parse_positive_float(
			os.getenv("AUTO_PZEM_RESET_THRESHOLD_KWH", "999.99"),
			999.99,
		)
		self._auto_pzem_reset_cooldown_seconds = _parse_min_int(
			os.getenv("AUTO_PZEM_RESET_COOLDOWN_SECONDS", "60"),
			60,
			1,
		)

		self._mqtt_client: Optional[mqtt.Client] = None
		self._last_valid_dt: dict[str, datetime] = {}       # device_code → last valid datetime
		self._last_sync_cmd: dict[str, float] = {}           # device_code → last sync command time
		self._last_pzem_reset_cmd: dict[str, float] = {}     # device_code → last pzem reset command time

	@staticmethod
	def _parse_topic(topic: str) -> Optional[Tuple[str, str]]:
		parts = [part for part in topic.split("/") if part]
		if TOPIC_MODE == "simple":
			if len(parts) != 3:
				return None
			if parts[1] != "swm-raw":
				return None
			return parts[0], parts[2]

		if len(parts) != 4:
			return None
		if parts[0] != "siwatt-mqtt" or parts[2] != "swm-raw":
			return None
		return parts[1], parts[3]

	def _validate_device(self, username: str, device_code: str) -> Optional[dict]:
		device = self._repo.get_device(username, device_code)
		if not device:
			self._logger.warning(
				"device_not_found",
				username=username,
				device_code=device_code,
			)
			return None
		return device

	def _get_pipeline(self, device_code: str) -> AggregationPipeline:
		pipeline = self._pipelines.get(device_code)
		if pipeline:
			return pipeline
		pipeline = AggregationPipeline(
			self._repo,
			self._realtime,
			self._hourly,
			self._logger,
			self._balance_mode,
			self._prediction_hourly_enabled,
			self._prediction_hourly_trigger,
			self._prediction_daily_enabled,
			self._prediction_daily_trigger,
			self._handle_pzem_overflow_after_hourly,
		)
		self._pipelines[device_code] = pipeline
		return pipeline

	@staticmethod
	def _build_command_topic(username: str, device_code: str) -> str:
		if TOPIC_MODE == "simple":
			return f"{username}/swm-cmd/{device_code}"
		return f"/siwatt-mqtt/{username}/swm-cmd/{device_code}"

	def _handle_pzem_overflow_after_hourly(self, username: str, device_code: str, energy_kwh: float) -> bool:
		if not self._auto_pzem_reset_enabled:
			return False

		if energy_kwh <= self._auto_pzem_reset_threshold_kwh:
			return False

		if not self._mqtt_client:
			self._logger.warning("mqtt_client_not_ready_for_pzem_reset", device_code=device_code)
			return False

		now_ts = time.time()
		last_cmd_ts = self._last_pzem_reset_cmd.get(device_code, 0)
		if now_ts - last_cmd_ts < self._auto_pzem_reset_cooldown_seconds:
			self._logger.warning(
				"pzem_overflow_detected_in_cooldown",
				device_code=device_code,
				energy_kwh=energy_kwh,
				threshold_kwh=self._auto_pzem_reset_threshold_kwh,
			)
			return False

		cmd_topic = self._build_command_topic(username, device_code)

		try:
			self._mqtt_client.publish(cmd_topic, json.dumps({"cmd": "pzem-reset"}))
			self._mqtt_client.publish(cmd_topic, json.dumps({"cmd": "reboot"}))
			self._last_pzem_reset_cmd[device_code] = now_ts

			pipeline = self._pipelines.get(device_code)
			if pipeline:
				pipeline.mark_energy_reset_event()

			self._last_valid_dt.pop(device_code, None)

			self._logger.warning(
				"pzem_overflow_auto_reset_sent",
				device_code=device_code,
				energy_kwh=energy_kwh,
				threshold_kwh=self._auto_pzem_reset_threshold_kwh,
				topic=cmd_topic,
			)
			return True
		except Exception:
			self._logger.exception("pzem_overflow_auto_reset_failed", device_code=device_code)
			return False

	def _handle_message(self, topic: str, payload: dict) -> None:
		parsed = self._parse_topic(topic)
		if not parsed:
			self._logger.warning("topic_invalid", topic=topic)
			return

		username, device_code = parsed
		if payload.get("device_id") and payload.get("device_id") != device_code:
			self._logger.warning(
				"device_mismatch",
				topic=topic,
				payload_device_id=payload.get("device_id"),
				device_code=device_code,
			)
			return

		required_fields = ["datetime", "voltage", "current", "power", "energy", "frequency", "pf"]
		missing = [field for field in required_fields if field not in payload]
		if missing:
			self._logger.warning("payload_missing_fields", missing=missing, topic=topic)
			return

		device = self._validate_device(username, device_code)
		if not device:
			return

		# Validasi datetime dari device
		if not self._validate_device_datetime(username, device_code, payload):
			return

		self._last_seen[device["id"]] = time.time()

		record = {
			"username": username,
			"device_code": device_code,
			"device_id": device["id"],
			"payload": payload,
		}

		self._buffer.append(device_code, record)
		pipeline = self._get_pipeline(device_code)
		result = self._buffer.process(device_code, pipeline.handle)
		self._logger.info(
			"buffer_processed",
			device_code=device_code,
			mqtt_datetime=payload.get("datetime"),
			processed=result.processed,
			remaining=result.remaining,
		)

	def _validate_device_datetime(
		self, username: str, device_code: str, payload: dict
	) -> bool:
		"""Validasi datetime dari device. Return False jika abnormal (data di-drop + kirim sync-rtc)."""
		try:
			device_dt = parse_datetime(payload["datetime"])
		except Exception:
			self._logger.warning(
				"datetime_parse_failed",
				device_code=device_code,
				raw_datetime=payload.get("datetime"),
			)
			self._send_sync_rtc(username, device_code, "datetime parse failed")
			return False

		now = datetime.now()
		reason = None

		# Cek 1: Tahun di luar range wajar (contoh kasus: tahun 2036)
		if device_dt.year < _DATETIME_MIN_YEAR or device_dt.year > _DATETIME_MAX_YEAR:
			reason = f"year out of range: {device_dt.year} (valid: {_DATETIME_MIN_YEAR}-{_DATETIME_MAX_YEAR})"

		# Cek 2: Terlalu jauh di masa depan dibanding waktu server
		elif (device_dt - now).total_seconds() > _DATETIME_MAX_FUTURE_SECONDS:
			diff = (device_dt - now).total_seconds()
			reason = f"datetime {diff:.0f}s ahead of server time"

		# Cek 3: Terlalu jauh di masa lalu dibanding waktu server
		elif (now - device_dt).total_seconds() > _DATETIME_MAX_PAST_SECONDS:
			diff = (now - device_dt).total_seconds()
			reason = f"datetime {diff:.0f}s behind server time"

		# Cek 4: Waktu mundur dari data sebelumnya (RTC loncat ke belakang)
		elif device_code in self._last_valid_dt:
			prev_dt = self._last_valid_dt[device_code]
			if (prev_dt - device_dt).total_seconds() > _DATETIME_BACKWARD_TOLERANCE:
				diff = (prev_dt - device_dt).total_seconds()
				reason = f"datetime went backward by {diff:.0f}s (prev: {prev_dt}, now: {device_dt})"

		if reason:
			self._logger.warning(
				"datetime_abnormal",
				device_code=device_code,
				device_datetime=str(device_dt),
				server_datetime=str(now),
				reason=reason,
			)
			# Hapus referensi waktu lama agar setelah sync,
			# data dengan waktu yang sudah dikoreksi tidak dianggap "mundur"
			self._last_valid_dt.pop(device_code, None)
			self._send_sync_rtc(username, device_code, reason)
			return False

		# Datetime valid, simpan sebagai referensi
		self._last_valid_dt[device_code] = device_dt
		return True

	def _send_sync_rtc(self, username: str, device_code: str, reason: str) -> None:
		"""Kirim command sync-rtc ke device via MQTT (dengan cooldown)."""
		if not self._mqtt_client:
			return

		now = time.time()
		last_sent = self._last_sync_cmd.get(device_code, 0)

		if now - last_sent < _SYNC_COMMAND_COOLDOWN:
			return  # Masih dalam cooldown, jangan spam device

		cmd_topic = self._build_command_topic(username, device_code)

		cmd_payload = json.dumps({"cmd": "sync-rtc"})

		try:
			self._mqtt_client.publish(cmd_topic, cmd_payload)
			self._last_sync_cmd[device_code] = now

			# Reset pipeline agar data dengan waktu koreksi tidak di-drop
			# Scenario: device clock maju 90 detik (masih dalam toleransi)
			# → pipeline._last_processed_dt = waktu maju
			# → sync-rtc dikirim → device koreksi waktu mundur 90 detik
			# → tanpa reset, pipeline akan drop semua data selama 90 detik
			pipeline = self._pipelines.get(device_code)
			if pipeline:
				pipeline.reset_datetime_state()
				self._logger.info(
					"pipeline_datetime_reset",
					device_code=device_code,
				)

			self._logger.info(
				"sync_rtc_command_sent",
				device_code=device_code,
				topic=cmd_topic,
				reason=reason,
			)
		except Exception:
			self._logger.exception("sync_rtc_command_failed", device_code=device_code)

	def run(self) -> None:
		self._logger.info(
			"worker_starting",
			auto_pzem_reset_enabled=self._auto_pzem_reset_enabled,
			auto_pzem_reset_threshold_kwh=self._auto_pzem_reset_threshold_kwh,
			auto_pzem_reset_cooldown_seconds=self._auto_pzem_reset_cooldown_seconds,
		)
		self._recovery.replay_all(lambda device_code: self._get_pipeline(device_code).handle)

		client = create_client()
		self._mqtt_client = client  # Simpan referensi untuk publish command
		subscriber = Subscriber(TOPIC_WILDCARD, self._handle_message)
		client.on_connect = subscriber.on_connect
		client.on_message = subscriber.on_message

		host = os.getenv("MQTT_BROKER", "broker.emqx.io")
		port = int(os.getenv("MQTT_PORT", "1883"))
		client.connect(host, port, keepalive=60)
        
		client.loop_start()
		try:
			while True:
				try:
					active_ids = self._repo.get_active_device_ids()
					now = time.time()
					for device_id in active_ids:
						if device_id not in self._last_seen:
							self._last_seen[device_id] = now
					
					offline_ids = [
						device_id
						for device_id, last_seen in self._last_seen.items()
						if now - last_seen > 20
					]
					if offline_ids:
						self._repo.update_devices_offline_status(offline_ids)
						for device_id in offline_ids:
							self._last_seen.pop(device_id, None)
				except Exception:
					self._logger.exception("offline_status_update_failed")
				time.sleep(5)
		except KeyboardInterrupt:
			self._logger.info("worker_stopping")
			client.loop_stop()
if __name__ == "__main__":
	Worker().run()
