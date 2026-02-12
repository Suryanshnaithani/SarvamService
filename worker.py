import os
import time
import json
import threading
import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from http.server import BaseHTTPRequestHandler, HTTPServer
from sarvamai import SarvamAI

# =====================
# CONFIG
# =====================
SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")
EXCEL_URL = os.getenv("EXCEL_RAW_URL")

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

CHECK_INTERVAL = 300        # 5 minutes
BATCH_EMAIL_SIZE = 500
DOWNLOAD_DIR = "downloads"
STATE_FILE = "processed_state.json"

PORT = int(os.getenv("PORT", "10000"))

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =====================
# FAKE HTTP SERVER (Render Web Service)
# =====================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def start_http_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()

threading.Thread(target=start_http_server, daemon=True).start()

# =====================
# LOAD PERSISTENT STATE
# =====================
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        processed_state = json.load(f)
else:
    processed_state = {}

# =====================
# CLIENT
# =====================
client = SarvamAI(api_subscription_key=SARVAM_API_KEY)

# =====================
# HELPERS
# =====================
def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(processed_state, f, indent=2)

def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)

    print("📧 Email sent")

def download_excel():
    df = pd.read_excel(EXCEL_URL)
    return df

def download_file(url, filename):
    path = os.path.join(DOWNLOAD_DIR, filename)
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path

def process_document(filepath):
    job = client.document_intelligence.create_job(
        language="en-IN",
        output_format="md"
    )

    job.upload_file(filepath)
    job.start()

    return job.job_id

# =====================
# MAIN LOOP
# =====================
processed_total = len(processed_state)

print("🚀 Worker started (Web Service mode)")

while True:
    try:
        df = download_excel()

        newly_processed = 0
        unprocessed_found = False

        for _, row in df.iterrows():
            doc_name = row["document_name"]
            url = row["url"]

            if processed_state.get(doc_name):
                continue

            unprocessed_found = True

            print(f"⬇️ Downloading {doc_name}")
            file_path = download_file(url, doc_name)

            print(f"🧠 Processing {doc_name}")
            job_id = process_document(file_path)

            processed_state[doc_name] = {
                "job_id": job_id,
                "timestamp": time.time()
            }

            save_state()

            processed_total += 1
            newly_processed += 1

            print(f"✅ {doc_name} → Job ID: {job_id}")

            if processed_total % BATCH_EMAIL_SIZE == 0:
                send_email(
                    subject="SarvamAI Batch Update",
                    body=f"{processed_total} documents processed successfully."
                )

            time.sleep(2)  # API safety

        # ✅ Send final email if all docs processed but < 500
        if newly_processed > 0 and not unprocessed_found:
            send_email(
                subject="SarvamAI Processing Complete",
                body=(
                    f"All available documents have been processed.\n\n"
                    f"Total processed so far: {processed_total}"
                )
            )

        print("😴 Sleeping...")
        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(60)
