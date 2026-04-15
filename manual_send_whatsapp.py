import os
import re
import sys
import json
import requests
from supabase_client import get_supabase

MAKE_WEBHOOK_URL = os.environ["MAKE_WEBHOOK_URL"]


def normalize_number(number: str) -> str:
    if not number:
        return ""
    number = number.strip().replace(" ", "")
    if not number.startswith("+"):
        return ""
    if not re.fullmatch(r"\+\d{8,15}", number):
        return ""
    return number


def get_report(report_id: str):
    sb = get_supabase()
    resp = (
        sb.table("reports")
        .select("*")
        .eq("id", report_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise ValueError("Report not found")
    return rows[0]


def get_recipient(recipient_id: str):
    sb = get_supabase()
    resp = (
        sb.table("recipients")
        .select("*")
        .eq("id", recipient_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        raise ValueError("Recipient not found")
    return rows[0]


def auto_approve_report(report_id: str, approved_by: str):
    sb = get_supabase()
    report = get_report(report_id)

    if report.get("status") != "approved":
        sb.table("reports").update({
            "status": "approved",
            "approved_by": approved_by,
            "approved_at": "now()"
        }).eq("id", report_id).execute()


def insert_dispatch_log(report_id, recipient, destination, triggered_by):
    sb = get_supabase()
    resp = (
        sb.table("dispatch_logs")
        .insert({
            "report_id": report_id,
            "recipient_id": recipient.get("id"),
            "recipient_name": recipient.get("name"),
            "channel": "whatsapp",
            "destination": destination,
            "action_type": "manual_single_send",
            "status": "pending",
            "triggered_by": triggered_by
        })
        .execute()
    )
    rows = resp.data or []
    return rows[0]["id"] if rows else None


def update_dispatch_log(log_id, status, response_text):
    if not log_id:
        return
    sb = get_supabase()
    sb.table("dispatch_logs").update({
        "status": status,
        "response_text": response_text,
        "sent_at": "now()" if status == "sent" else None
    }).eq("id", log_id).execute()


def main():
    if len(sys.argv) < 4:
        print("Usage: python manual_send_whatsapp.py REPORT_ID RECIPIENT_ID TRIGGERED_BY")
        raise SystemExit(1)

    report_id = sys.argv[1]
    recipient_id = sys.argv[2]
    triggered_by = sys.argv[3]

    sb = get_supabase()

    report = get_report(report_id)
    recipient = get_recipient(recipient_id)

    if recipient.get("channel") != "whatsapp":
        print("Recipient is not a WhatsApp channel row")
        raise SystemExit(1)

    number = normalize_number(recipient.get("phone_number", ""))
    if not number:
        print("Invalid phone number")
        raise SystemExit(1)

    auto_approve_report(report_id, triggered_by)

    caption = (
        f"Doha Bank Market Intelligence\n"
        f"{report.get('report_date')}\n\n"
        f"Please find attached the approved market intelligence report."
    )

    payload = {
        "to": number,
        "name": recipient.get("name", "Unknown"),
        "report_date": str(report.get("report_date")),
        "pdf_url": report.get("pdf_url"),
        "caption": caption,
    }

    log_id = insert_dispatch_log(report_id, recipient, number, triggered_by)

    print(f"[INFO] Sending payload: {json.dumps(payload)}")

    try:
        res = requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=60)
        if res.status_code == 200:
            update_dispatch_log(log_id, "sent", res.text)
            print("Sent successfully")
        else:
            update_dispatch_log(log_id, "failed", res.text)
            print(f"Failed: {res.status_code} {res.text}")
            raise SystemExit(1)
    except Exception as e:
        update_dispatch_log(log_id, "failed", str(e))
        print(f"Failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
