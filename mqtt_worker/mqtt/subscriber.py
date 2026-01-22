import json
from typing import Callable

from mqtt_worker.utils.logger import get_logger


class Subscriber:
    def __init__(self, topic: str, handler: Callable[[str, dict], None]):
        self._topic = topic
        self._handler = handler
        self._logger = get_logger(__name__)

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self._logger.error("mqtt_connect_failed", rc=rc)
            return
        client.subscribe(self._topic)
        self._logger.info("mqtt_subscribed", topic=self._topic)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            self._logger.exception("mqtt_payload_invalid", topic=msg.topic)
            return

        try:
            self._handler(msg.topic, payload)
        except Exception:
            self._logger.exception("mqtt_handler_failed", topic=msg.topic)