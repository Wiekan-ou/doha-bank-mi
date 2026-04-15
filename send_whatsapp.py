import os
import json
import datetime
import requests
import time
import re
from supabase_client import get_supabase

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")


def load_active_numbers():
    sb = get_supabase()

    resp = (
        sb.table("recipients")
        .select("id, name, phone_number, tier")
        .eq("channel", "whatsapp")
        .eq("active", True)
        .execute()
    )

    rows = resp.data or []
    print(f"[INFO] Loaded {len(rows)} active WhatsApp recipients")
    return rows


def normalize_number(number: str):
    if not number:
        return ""

    number = number.strip().replace(" ", "")

    if not number.startswith("+"):
        return ""

    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""

    return number


def load_public_pdf_url():
    env_url = os.environ.get("PUBLIC_PDF_URL", "").strip()
    if env_url:
        print(f"[INFO] Using PUBLIC_PDF_URL from workflow env: {env_url}")
        return env_url

    if os.path.exists("public_pdf_url.json"):
        with open("public_pdf_url.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        file_url = data.get("public_url", "").strip()
        print(f"[INFO] Fallback public_pdf_url.json content: {file_url}")

        if file_url:
            return file_url

    print("[ERROR] No public PDF URL found in env or file")
    raise SystemExit(1)


def send():
    recipients = load_active_numbers()

    if not recipients:
        print("[WARN] No recipients found")
        return

    pdf_url = load_public_pdf_url()

    if "raw.githubusercontent.com" in pdf_url:
        print(f"[ERROR] Refusing to send old GitHub raw URL: {pdf_url}")
        raise SystemExit(1)

    caption = (
        f"Doha Bank Market Intelligence\n"
        f"{REPORT_DATE}\n\n"
        f"Please find attached today's market intelligence report covering "
        f"global indices, GCC markets, currencies, commodities, and latest news."
    )

    for r in recipients:
        name = r.get("name")
        number = normalize_number(r.get("phone_number"))

        if not number:
            print(f"[WARN] Skipping {name}, invalid number")
            continue

        payload = {
            "to": number,
            "name": name,
            "report_date": REPORT_DATE,
            "pdf_url": pdf_url,
            "caption": caption,
        }

        print(f"[INFO] Sending to {number}")
        print(f"[DEBUG] Payload: {payload}")

        res = requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=60)

        print(f"[INFO] Response: {res.status_code} {res.text}")

        time.sleep(1.5)


if __name__ == "__main__":
    send()
