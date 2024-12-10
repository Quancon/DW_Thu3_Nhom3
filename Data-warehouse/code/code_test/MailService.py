import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import json

def load_config(file_path='config.json'):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def send_email(subject, body, to_email, from_email, password):
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(from_email, password)
            server.send_message(msg)
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")

def notify_data_loaded():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = "Data Load Notification"
    body = f"Data has been successfully loaded into the database at {now}."
    config = load_config()
    email_config = config['email']
    from_email = email_config['from_email']
    password = email_config['password']
    to_email = email_config['to_email']
    
    send_email(subject, body, to_email, from_email, password)

if __name__ == "__main__":
    notify_data_loaded()