import json
import re
import datetime
from typing import Optional

import requests


def _parse_number(s: str) -> float:
    return float(s.replace(",", "").strip())


def _headers() -> dict:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.investing.com/",
    }


def fetch_from_investing_historical() -> Optional[dict]:
    url = "https://www.investing.com/indices/qsi-historical-data"
    print(f"[QE_BACKUP] Fetching historical page: {url}")
    r = requests.get(url, headers=_headers(), timeout=20)
    print(f"[QE_BACKUP] Status: {r.status_code}")
    r.raise_for_status()
    text = r.text or ""

    row_pattern = re.compile(
        r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s*\|\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)\s*\|\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)\s*\|\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)\s*\|\s*"
        r"([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)\s*\|",
        re.MULTILINE,
    )

    rows = row_pattern.findall(text)
    parsed = []

    for row in rows[:40]:
        try:
            dt = datetime.datetime.strptime(row[0], "%b %d, %Y").date()
            price = _parse_number(row[1])
            parsed.append((dt, price))
        except Exception:
            continue

    if not parsed:
        print("[QE_BACKUP] No parsed historical rows found")
        return None

    parsed.sort(key=lambda x: x[0])
    px_last = parsed[-1][1]
    px_prev = parsed[-2][1] if len(parsed) >= 2 else None

    change_1d = "N/A"
    if px_prev not in (None, 0):
        pct = ((px_last - px_prev) / px_prev) * 100
        change_1d = f"{pct:+.2f}%"

    return {
        "name": "Qatar QE Index",
        "price": round(px_last, 2),
        "prev_close": round(px_prev, 2) if px_prev is not None else None,
        "change_1d": change_1d,
        "as_of": parsed[-1][0].strftime("%Y-%m-%d"),
        "source": "Investing.com historical data",
        "fetched_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


def fetch_from_investing_qsi_page() -> Optional[dict]:
    url = "https://www.investing.com/indices/qsi"
    print(f"[QE_BACKUP] Fetching QSI page: {url}")
    r = requests.get(url, headers=_headers(), timeout=20)
    print(f"[QE_BACKUP] Status: {r.status_code}")
    r.raise_for_status()
    text = r.text or ""

    price_match = re.search(
        r"QSI live stock price is\s*([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)",
        text,
        re.IGNORECASE,
    )
    prev_match = re.search(
        r"Prev\.\s*Close\s*([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)",
        text,
        re.IGNORECASE,
    )

    if not price_match:
        print("[QE_BACKUP] QSI page did not expose a price")
        return None

    px_last = _parse_number(price_match.group(1))
    px_prev = _parse_number(prev_match.group(1)) if prev_match else None

    change_1d = "N/A"
    if px_prev not in (None, 0):
        pct = ((px_last - px_prev) / px_prev) * 100
        change_1d = f"{pct:+.2f}%"

    return {
        "name": "Qatar QE Index",
        "price": round(px_last, 2),
        "prev_close": round(px_prev, 2) if px_prev is not None else None,
        "change_1d": change_1d,
        "as_of": datetime.date.today().strftime("%Y-%m-%d"),
        "source": "Investing.com QSI page",
        "fetched_at_utc": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


def main():
    result = None

    try:
        result = fetch_from_investing_historical()
    except Exception as e:
        print(f"[QE_BACKUP] Historical fetch failed: {e}")

    if result is None:
        try:
            result = fetch_from_investing_qsi_page()
        except Exception as e:
            print(f"[QE_BACKUP] QSI page fetch failed: {e}")

    if result is None:
        raise RuntimeError("Failed to fetch QE backup from Investing.com sources")

    with open("qe_backup.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("[QE_BACKUP] Wrote qe_backup.json successfully")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
