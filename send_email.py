import os
import json
import base64
import datetime
import requests
from pathlib import Path
from supabase_client import get_supabase

RESEND_API_KEY = os.environ["RESEND_API_KEY"]
FROM_EMAIL = "Market Intelligence <updates@market-sigma.com>"
REPORT_DATE = datetime.date.today().strftime("%d %B %Y")
PDF_PATH = Path("report.pdf")
MARKET_DATA_PATH = Path("market_data.json")
ALLOWED_STATUSES = {"PASS"}


def assert_report_is_sendable() -> None:
    if not MARKET_DATA_PATH.exists():
        raise SystemExit("[BLOCKED] market_data.json not found. Email not sent.")

    with MARKET_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    status = str(data.get("report_status") or "").upper()
    allowed = bool(data.get("email_send_allowed", status in ALLOWED_STATUSES))
    issues = data.get("validation_issues") or []

    if status not in ALLOWED_STATUSES or not allowed:
        print(f"[BLOCKED] Report status is {status or 'UNKNOWN'}. Email not sent.")
        if issues:
            print("Validation issues:")
            for issue in issues:
                print(f"- {issue}")
        raise SystemExit(0)

    if not PDF_PATH.exists():
        raise SystemExit("[BLOCKED] report.pdf not found. Email not sent.")


def load_email_recipients() -> list[str]:
    sb = get_supabase()
    resp = (
        sb.table("recipients")
        .select("email")
        .eq("channel", "email")
        .eq("active", True)
        .execute()
    )
    rows = resp.data or []
    return [r["email"] for r in rows if r.get("email")]


def send():
    assert_report_is_sendable()

    recipients = load_email_recipients()

    if not recipients:
        print("[WARN] No active email recipients found in Supabase")
        return

    with PDF_PATH.open("rb") as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    payload = {
        "from": FROM_EMAIL,
        "to": recipients,
        "subject": f"Market Intelligence – {REPORT_DATE}",
        "html": f"""
            <p>Dear Team,</p>
            <p>Please find attached the approved <strong>Doha Bank Market Intelligence Report</strong>
            for <strong>{REPORT_DATE}</strong>.</p>
            <p>This report passed automated validation before distribution.</p>
        """,
        "attachments": [
            {
                "filename": f"Market-Intelligence-{REPORT_DATE}.pdf",
                "content": pdf_b64,
                "content_type": "application/pdf",
            }
        ],
    }

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    if resp.status_code in (200, 201):
        print(f"✓ Email sent to {recipients}")
    else:
        print(f"[ERROR] Resend API: {resp.status_code} – {resp.text}")
        raise SystemExit(1)


if __name__ == "__main__":
    send()
