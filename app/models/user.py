from sqlalchemy import Column, BigInteger, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True)
    username = Column(String(50), unique=True)
    email = Column(String(100), unique=True)
    password = Column(String(255))
    full_name = Column(String(100))
    firebase_uid = Column(String(128))
    fcm_token = Column(String(255))
    created_at = Column(DateTime)
    last_login = Column(DateTime)
