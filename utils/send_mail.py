import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()  # Load environment variables from .env file
def send_mail(subject: str, body: str, to_email: str):
    """
    Gửi email thông báo với tiêu đề và nội dung tùy chỉnh.
    Cấu hình SMTP theo Gmail (có thể đổi sang mail server khác).
    """
    # --- Thông tin tài khoản gửi ---
    sender_email = os.getenv("email")
    sender_password = os.getenv("app_password")  # Dùng App Password, không phải password Gmail thường vì bảo mật 2 lớp

    # --- Tạo nội dung email ---
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain", "utf-8"))

    # --- Gửi email ---
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"📧 Email sent successfully to {to_email}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
