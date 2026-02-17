import random
from datetime import datetime, timedelta

def generate_otp():
    return str(random.randint(100000, 999999))

def otp_expiry():
    return datetime.now() + timedelta(minutes=5)
