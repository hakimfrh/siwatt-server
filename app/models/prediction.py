import os
import re
from datetime import datetime

from sqlalchemy import Column, BigInteger, DateTime, String, Text

from app.models.user import Base


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RAW_TABLE_NAME = os.getenv("ML_PREDICTIONS_TABLE", "predictions").strip()
_TABLE_NAME = _RAW_TABLE_NAME if _TABLE_NAME_RE.fullmatch(_RAW_TABLE_NAME) else "predictions"


class Prediction(Base):
    __tablename__ = _TABLE_NAME

    id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger)
    device_id = Column(BigInteger)
    job_type = Column("type", String(32))
    status = Column(String(32))
    params = Column(Text)
    progress = Column(Text)
    result = Column(Text)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)