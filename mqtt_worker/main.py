import os
from typing import Optional, Tuple

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


class AggregationPipeline:
	def __init__(
		self,
		repo: Repository,
		realtime: RealtimeProcessor,
		hourly: HourlyProcessor,
		logger,
		balance_mode: str,
	):
		self._repo = repo
		self._realtime = realtime
		self._hourly = hourly
		self._logger = logger
		self._minute_agg = MinuteAggregator()
		self._last_processed_dt = None
		self._balance_mode = balance_mode

	def handle(self, record: dict) -> ProcessDecision:
		try:
			payload = record["payload"]
			dt = parse_datetime(payload["datetime"])
			device_id = record["device_id"]
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
			success, energy_delta = self._hourly.handle(
				device_id,
				aggregate.bucket_hour,
				current_hour,
				aggregate.energy_last,
			)
			if not success:
				return ProcessDecision(success=False)
			if self._balance_mode == "hour" and energy_delta is not None:
				try:
					self._repo.decrement_token_balance(device_id, energy_delta)
				except Exception:
					self._logger.exception("balance_hour_update_failed", device_id=device_id)
					return ProcessDecision(success=False)

		return ProcessDecision(success=True, checkpoint_offset=-1)


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
		self._balance_mode = os.getenv("BALANCE_DECREASE_MODE", "minute").lower()
		if self._balance_mode not in ("minute", "hour"):
			self._balance_mode = "minute"

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
		)
		self._pipelines[device_code] = pipeline
		return pipeline

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

	def run(self) -> None:
		self._logger.info("worker_starting")
		self._recovery.replay_all(lambda device_code: self._get_pipeline(device_code).handle)

		client = create_client()
		subscriber = Subscriber(TOPIC_WILDCARD, self._handle_message)
		client.on_connect = subscriber.on_connect
		client.on_message = subscriber.on_message

		host = os.getenv("MQTT_BROKER", "broker.emqx.io")
		port = int(os.getenv("MQTT_PORT", "1883"))
		client.connect(host, port, keepalive=60)
		client.loop_forever()


if __name__ == "__main__":
	Worker().run()
