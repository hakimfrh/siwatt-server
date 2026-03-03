import os
from mailjet_rest import Client
from dotenv import load_dotenv

load_dotenv()

MJ_APIKEY_PUBLIC = os.getenv("MJ_APIKEY_PUBLIC")
MJ_APIKEY_PRIVATE = os.getenv("MJ_APIKEY_PRIVATE")
MAILJET_SENDER_EMAIL = os.getenv("MAILJET_SENDER_EMAIL", "noreply@siwatt.com")
MAILJET_SENDER_NAME = os.getenv("MAILJET_SENDER_NAME", "SIWATT")

mailjet = Client(auth=(MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE), version='v3.1')
