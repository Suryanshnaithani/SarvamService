import os
import time
import json
import threading
import requests
import pandas as pd
import smtplib
import base64
import io
from email.mime.text import MIMEText
from http.server import BaseHTTPRequestHandler, HTTPServer
from sarvamai import SarvamAI

# =====================================================
# ENV CONFIG
# =====================================================
SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_FILE_PATH = os.getenv("GITHUB_FILE_PATH")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

CHECK_INTERVAL = 300          # 5 minutes
BATCH_EMAIL_SIZE = 500
DOWNLOAD_DIR = "downloads"
STATE_FILE = "processed_state.json"

PORT = int(os.getenv("PORT", "10000"))

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

print("🔥 worker.py loaded", flush=True)

# =====================================================
# FAKE HTTP SERVER (Render health checks)
# =====================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        return

def start_http_server():
    print("🌐 Fake HTTP server started", flush=True)
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()

threading.Thread(target=start_http_server, daemon=True).start()

# =====================================================
# LOAD LOCAL STATE
# =====================================================
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        processed_state = json.load(f)
else:
    processed_state = {}

# =====================================================
# CLIENT
# =====================================================
client = SarvamAI(api_subscription_key=SARVAM_API_KEY)

# =====================================================
# GITHUB HELPERS
# =====================================================
GITHUB_API = (
    f"https://api.github.com/repos/"
    f"{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
)

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def read_excel_from_github():
    r = requests.get(GITHUB_API, headers=HEADERS, params={"ref": GITHUB_BRANCH})
    r.raise_for_status()
    data = r.json()
    content = base64.b64decode(data["content"])
    df = pd.read_excel(io.BytesIO(content), dtype=str)
    df = df.fillna("")
    return df, data["sha"]

def write_excel_to_github(df, sha, message):
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    payload = {
        "message": message,
        "content": base64.b64encode(buf.read()).decode(),
        "sha": sha,
        "branch": GITHUB_BRANCH
    }

    r = requests.put(GITHUB_API, headers=HEADERS, json=payload)
    r.raise_for_status()

# =====================================================
# HELPERS
# =====================================================
def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(processed_state, f, indent=2)

def send_email(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_TO

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        print(f"📧 Email sent: {subject}", flush=True)
    except Exception as e:
        print(f"❌ Email failed: {e}", flush=True)

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

# =====================================================
# MAIN LOOP
# =====================================================
processed_total = len(processed_state)

print("🚀 Worker started (Render Web Service)", flush=True)

# OPTIONAL STARTUP MAIL (enable once if needed)
# send_email("SarvamAI Worker Started", "Worker is live on Render.")

while True:
    try:
        print("🔄 New cycle started", flush=True)

        df, sha = read_excel_from_github()
        newly_processed = 0

        for idx, row in df.iterrows():
            doc_name = row["document_name"]
            url = row["url"]

            if doc_name in processed_state:
                continue

            print(f"⬇️ Downloading {doc_name}", flush=True)
            file_path = download_file(url, doc_name)

            print(f"🧠 Processing {doc_name}", flush=True)
            job_id = process_document(file_path)

            processed_state[doc_name] = {
                "job_id": job_id,
                "timestamp": time.time()
            }
            save_state()

            df.at[idx, "job_id"] = job_id
            df.at[idx, "status"] = "submitted"

            write_excel_to_github(
                df,
                sha,
                f"Processed {doc_name} → {job_id}"
            )

            df, sha = read_excel_from_github()

            processed_total += 1
            newly_processed += 1

            print(f"✅ {doc_name} → {job_id}", flush=True)

            if processed_total % BATCH_EMAIL_SIZE == 0:
                send_email(
                    "SarvamAI Batch Update",
                    f"{processed_total} documents processed successfully."
                )

            time.sleep(2)

        # 🔔 Cycle summary mail
        if newly_processed > 0:
            send_email(
                "SarvamAI Cycle Update",
                f"Processed {newly_processed} documents this cycle.\nTotal: {processed_total}"
            )

        # 🔔 Final completion mail
        pending = any(
            row["document_name"] not in processed_state
            for _, row in df.iterrows()
        )

        if newly_processed > 0 and not pending:
            send_email(
                "SarvamAI Processing Complete",
                f"All documents processed.\nTotal: {processed_total}"
            )

        print("😴 Sleeping...", flush=True)
        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print(f"❌ Error: {e}", flush=True)
        time.sleep(60)
