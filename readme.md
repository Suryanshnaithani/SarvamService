# SarvamService

SarvamService is an automated document processing and notification service that leverages SarvamAI for document intelligence. It periodically downloads documents from a provided Excel sheet, processes them using SarvamAI, and sends batch email notifications upon completion.

## Features
- Periodically downloads and processes documents listed in an Excel file
- Uses SarvamAI for document intelligence (output in Markdown)
- Sends batch email notifications after processing a set number of documents
- Maintains persistent state to avoid reprocessing
- Simple HTTP health check endpoint

## Folder Structure
- worker.py — Main service script
- requirements.txt — Python dependencies
- runtime.txt — (Optional) Python runtime version
- data/ — Contains input files (e.g., documents.xlsx)

## Setup Instructions
1. **Clone the repository**
2. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```
3. **Set environment variables:**
   - SARVAM_API_KEY: Your SarvamAI API key
   - EXCEL_RAW_URL: URL to the Excel file containing document info
   - EMAIL_USER: Email address to send notifications from
   - EMAIL_PASS: Password or app password for the email account
   - EMAIL_TO: Recipient email address
   - PORT: (Optional) Port for the health check server (default: 10000)

   You can use a `.env` file or set these in your environment.

4. **Run the worker:**
   ```
   python worker.py
   ```

## How It Works
- The service runs an infinite loop:
  1. Downloads the Excel file from EXCEL_RAW_URL
  2. For each unprocessed document:
     - Downloads the document
     - Processes it with SarvamAI
     - Updates persistent state
     - Sends a batch email every 500 documents
  3. Sleeps for 5 minutes between checks
- A health check endpoint is available at `http://localhost:<PORT>/`

## Environment Variables Example
```
SARVAM_API_KEY=your_sarvam_api_key
EXCEL_RAW_URL=https://example.com/documents.xlsx
EMAIL_USER=your_email@gmail.com
EMAIL_PASS=your_email_password
EMAIL_TO=recipient@example.com
PORT=10000
```

## Requirements
See `requirements.txt` for Python dependencies.

