import random
from datetime import datetime, timedelta


def generate_otp():
    return str(random.randint(100000, 999999))


def otp_expiry():
    return datetime.now() + timedelta(minutes=5)


def build_otp_html(otp_code: str) -> str:
    return f"""
    <div style="font-family: Arial, sans-serif; background-color: #f4f6f8; padding: 40px;">
      <div style="max-width: 500px; margin: auto; background: white; padding: 30px; border-radius: 12px;">
        <h2 style="color: #333;">Verifikasi Akun Kamu</h2>
        <p>Gunakan kode OTP berikut untuk melanjutkan:</p>

        <div style="text-align: center; margin: 30px 0;">
          <span style="
            font-size: 32px;
            letter-spacing: 6px;
            font-weight: bold;
            background: #111827;
            color: white;
            padding: 15px 25px;
            border-radius: 8px;
            display: inline-block;
          ">
            {otp_code}
          </span>
        </div>

        <p>Kode ini berlaku selama <b>5 menit</b>.</p>
        <p style="font-size: 12px; color: #888;">
          Jika kamu tidak merasa meminta kode ini, abaikan email ini.
        </p>
      </div>
    </div>
    """
