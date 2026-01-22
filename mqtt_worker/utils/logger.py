import json
import logging
import os
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in ("msg", "args", "levelname", "levelno", "pathname", "filename",
                       "module", "exc_info", "exc_text", "stack_info", "lineno",
                       "funcName", "created", "msecs", "relativeCreated", "thread",
                       "threadName", "processName", "process", "name"):
                continue
            payload[key] = value

        return json.dumps(payload, ensure_ascii=False)


class ContextLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        reserved = {"exc_info", "stack_info", "stacklevel", "extra"}
        extra = dict(kwargs.get("extra", {}))
        for key, value in list(kwargs.items()):
            if key in reserved:
                continue
            extra[key] = value

        clean_kwargs = {key: value for key, value in kwargs.items() if key in reserved}
        clean_kwargs["extra"] = extra
        return msg, clean_kwargs


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return ContextLogger(logger, {})