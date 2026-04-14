import json
import os
import datetime
from supabase_client import get_supabase

PDF_PATH = "report.pdf"
SIGNED_URL_OUTPUT = "signed_pdf_url.json"


def main():
    if not os.path.exists(PDF_PATH):
        print(f"[ERROR] PDF file not found: {PDF_PATH}")
        raise SystemExit(1)

    bucket = os.environ.get("SUPABASE_STORAGE_BUCKET", "reports")
    expires_in = int(os.environ.get("SIGNED_URL_EXPIRES_IN", "86400"))  # 24 hours

    today = datetime.date.today().strftime("%Y-%m-%d")
    storage_path = f"daily-reports/{today}/report.pdf"

    sb = get_supabase()

    print(f"[INFO] Uploading PDF to Supabase Storage bucket={bucket} path={storage_path}")

    # Try upload first, then update if file already exists
    try:
        with open(PDF_PATH, "rb") as f:
            sb.storage.from_(bucket).upload(
                path=storage_path,
                file=f,
                file_options={
                    "content-type": "application/pdf",
                    "cache-control": "3600",
                    "upsert": "false",
                },
            )
        print("[INFO] PDF uploaded successfully")
    except Exception as e:
        print(f"[WARN] Upload failed, trying update instead: {e}")
        with open(PDF_PATH, "rb") as f:
            sb.storage.from_(bucket).update(
                path=storage_path,
                file=f,
                file_options={
                    "content-type": "application/pdf",
                    "cache-control": "3600",
                },
            )
        print("[INFO] PDF updated successfully")

    print(f"[INFO] Creating signed URL, expires_in={expires_in} seconds")
    signed = sb.storage.from_(bucket).create_signed_url(
        storage_path,
        expires_in,
        {"download": True},
    )

    signed_url = None

    if isinstance(signed, dict):
        signed_url = signed.get("signedUrl") or signed.get("signed_url")
    else:
        signed_url = getattr(signed, "get", lambda *_: None)("signedUrl") or getattr(signed, "get", lambda *_: None)("signed_url")

    if not signed_url:
        print(f"[ERROR] Failed to create signed URL. Response: {signed}")
        raise SystemExit(1)

    with open(SIGNED_URL_OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"signed_url": signed_url}, f, indent=2)

    print(f"[INFO] Signed URL written to {SIGNED_URL_OUTPUT}")
    print(f"[INFO] Signed URL: {signed_url}")


if __name__ == "__main__":
    main()
