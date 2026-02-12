import os
import time
import requests
import pandas as pd
import smtplib
from email.mime.text import MIMEText
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

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =====================
# CLIENT
# =====================
client = SarvamAI(api_subscription_key=SARVAM_API_KEY)

# =====================
# HELPERS
# =====================
def send_email(processed_count):
    msg = MIMEText(f"{processed_count} documents have been processed successfully.")
    msg["Subject"] = "SarvamAI Batch Processing Update"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)

    print("📧 Email sent")

def download_excel():
    df = pd.read_excel(EXCEL_URL)

    if "processed" not in df.columns:
        df["processed"] = False
    if "job_id" not in df.columns:
        df["job_id"] = ""

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
processed_total = 0

print("🚀 Worker started")

while True:
    try:
        df = download_excel()
        updated = False

        for idx, row in df.iterrows():
            if bool(row["processed"]):
                continue

            doc_name = row["document_name"]
            url = row["url"]

            print(f"⬇️ Downloading {doc_name}")
            file_path = download_file(url, doc_name)

            print(f"🧠 Processing {doc_name}")
            job_id = process_document(file_path)

            df.at[idx, "processed"] = True
            df.at[idx, "job_id"] = job_id

            processed_total += 1
            updated = True

            print(f"✅ {doc_name} → Job ID: {job_id}")

            if processed_total % BATCH_EMAIL_SIZE == 0:
                send_email(processed_total)

            time.sleep(2)  # API safety

        if updated:
            # LOCAL SAVE (for debugging / safety)
            df.to_excel("last_processed_snapshot.xlsx", index=False)

        print("😴 Sleeping...")
        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(60)
