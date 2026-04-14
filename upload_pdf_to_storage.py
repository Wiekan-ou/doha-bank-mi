import json
import os
import datetime
from supabase_client import get_supabase

PDF_PATH = "report.pdf"
PUBLIC_URL_OUTPUT = "public_pdf_url.json"


def extract_public_url(resp):
    """
    Supabase python clients may return different shapes depending on version.
    Handle all common cases safely.
    """
    if resp is None:
        return None

    if isinstance(resp, str):
        return resp

    if isinstance(resp, dict):
        if resp.get("publicUrl"):
            return resp.get("publicUrl")
        if resp.get("public_url"):
            return resp.get("public_url")
        data = resp.get("data")
        if isinstance(data, dict):
            if data.get("publicUrl"):
                return data.get("publicUrl")
            if data.get("public_url"):
                return data.get("public_url")

    # object style fallback
    data = getattr(resp, "data", None)
    if isinstance(data, dict):
        if data.get("publicUrl"):
            return data.get("publicUrl")
        if data.get("public_url"):
            return data.get("public_url")

    return None


def main():
    if not os.path.exists(PDF_PATH):
        print(f"[ERROR] PDF file not found: {PDF_PATH}")
        raise SystemExit(1)

    bucket = os.environ.get("SUPABASE_PUBLIC_STORAGE_BUCKET", "reports-public")
    today = datetime.date.today().strftime("%Y-%m-%d")
    timestamp = datetime.datetime.utcnow().strftime("%H%M%S")
    storage_path = f"daily-reports/{today}/report-{timestamp}.pdf"

    sb = get_supabase()

    print(f"[INFO] Uploading PDF to public Supabase bucket={bucket} path={storage_path}")

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
        print(f"[ERROR] Upload failed: {e}")
        raise SystemExit(1)

    try:
        resp = sb.storage.from_(bucket).get_public_url(storage_path)
        public_url = extract_public_url(resp)

        # deterministic fallback if SDK response shape is odd
        if not public_url:
            supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
            public_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{storage_path}"

        with open(PUBLIC_URL_OUTPUT, "w", encoding="utf-8") as f:
            json.dump({"public_url": public_url}, f, indent=2)

        print(f"[INFO] Public URL written to {PUBLIC_URL_OUTPUT}")
        print(f"[INFO] Public URL: {public_url}")

    except Exception as e:
        print(f"[ERROR] Failed to build public URL: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
