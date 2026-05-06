import json
import os
import re
import datetime
import calendar
from email.utils import parsedate_to_datetime
from typing import Optional, List, Dict, Any

import feedparser
import anthropic
import requests


CONFIG = {
    "client_name": "Doha Bank",
    "report_date": datetime.date.today().strftime("%d %B %Y"),
    "delivery_time_ast": "07:00",
    "sections": {
        "global_indices": True,
        "gcc_indices": True,
        "spot_currency": True,
        "qar_cross_rates": True,
        "fixed_income": True,
        "qatari_banks": True,
        "commodities": True,
        "global_news": True,
        "qatar_news": True,
    }
}


SUPABASE_TABLE = "market_indices_history"
EXPECTED_INSTRUMENT_COUNT = 34
STALE_DATA_WARNING_DAYS = 3
USD_QAR_SUSPICIOUS_MOVE_THRESHOLD = 0.25
MAX_NEWS_AGE_HOURS = 24
MIN_GLOBAL_NEWS_ITEMS = 4
MIN_QATAR_NEWS_ITEMS = 3

BUSINESS_NEWS_KEYWORDS = {
    "market", "markets", "stock", "stocks", "shares", "index", "indices",
    "bank", "banks", "banking", "finance", "financial", "investment",
    "investor", "investors", "economy", "economic", "trade", "exports",
    "imports", "inflation", "rates", "interest", "bond", "bonds",
    "oil", "gas", "energy", "lng", "sukuk", "debt", "fiscal",
    "qatar", "doha", "gcc", "gulf", "central bank", "treasury",
    "earnings", "profit", "revenue", "ipo", "merger", "acquisition",
    "real estate", "shipping", "commodities", "currency", "forex"
}

INSTRUMENT_RANGES = {
    "SPX": (3000, 8000),
    "FTSE100": (5000, 12000),
    "NIKKEI225": (20000, 70000),
    "DAX": (10000, 35000),
    "HSI": (10000, 40000),
    "SENSEX": (40000, 120000),
    "QE": (8000, 15000),
    "TASI": (8000, 16000),
    "DFMGI": (2500, 9000),
    "FADGI": (5000, 15000),
    "BKA": (5000, 13000),
    "BHSEASI": (1200, 3000),
    "DXY": (80, 130),
    "EURUSD": (0.90, 1.30),
    "GBPUSD": (1.00, 1.60),
    "USDJPY": (90, 220),
    "USDCNY": (5.5, 8.5),
    "USDQAR": (3.63, 3.66),
    "EURQAR": (3.2, 4.8),
    "GBPQAR": (4.0, 5.8),
    "CNYQAR": (0.40, 0.65),
    "UST5Y": (0.0, 10.0),
    "UST10Y": (0.0, 10.0),
    "DHBK": (1.0, 5.0),
    "QNBK": (8.0, 30.0),
    "QIBK": (10.0, 35.0),
    "CBQK": (2.0, 8.0),
    "QIIB": (5.0, 18.0),
    "MARK": (1.0, 5.0),
    "DUBK": (1.0, 7.0),
    "ABQK": (1.0, 6.0),
    "BRENT": (40.0, 150.0),
    "SILVER": (10.0, 100.0),
    "GOLDQAR": (8000.0, 30000.0),
}


REPORT_SECTION_TO_OUTPUT_KEY = {
    "GLOBAL INDICES": "global_indices",
    "GCC & REGIONAL INDICES": "gcc_indices",
    "SPOT CURRENCY": "spot_currency",
    "QAR CROSS RATES": "qar_cross_rates",
    "FIXED INCOME — UST YIELDS": "fixed_income",
    "FIXED INCOME - UST YIELDS": "fixed_income",
    "QATARI BANKS": "qatari_banks",
    "COMMODITIES & ENERGY": "commodities",
}


EXPECTED_INSTRUMENTS = [
    {
        "code": "SPX",
        "name": "US S&P 500",
        "symbol": "^GSPC",
        "report_section": "GLOBAL INDICES",
        "display_order": 1,
    },
    {
        "code": "FTSE100",
        "name": "UK FTSE 100",
        "symbol": "^FTSE",
        "report_section": "GLOBAL INDICES",
        "display_order": 2,
    },
    {
        "code": "NIKKEI225",
        "name": "Japan Nikkei",
        "symbol": "^N225",
        "report_section": "GLOBAL INDICES",
        "display_order": 3,
    },
    {
        "code": "DAX",
        "name": "Germany DAX",
        "symbol": "^GDAXI",
        "report_section": "GLOBAL INDICES",
        "display_order": 4,
    },
    {
        "code": "HSI",
        "name": "Hong Kong HSI",
        "symbol": "^HSI",
        "report_section": "GLOBAL INDICES",
        "display_order": 5,
    },
    {
        "code": "SENSEX",
        "name": "India Sensex",
        "symbol": "^BSESN",
        "report_section": "GLOBAL INDICES",
        "display_order": 6,
    },
    {
        "code": "QE",
        "name": "Qatar QE Index",
        "symbol": "^GNRI.QA",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 1,
    },
    {
        "code": "TASI",
        "name": "Saudi Tadawul",
        "symbol": "^TASI.SR",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 2,
    },
    {
        "code": "DFMGI",
        "name": "Dubai DFM",
        "symbol": "DFMGI",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 3,
    },
    {
        "code": "FADGI",
        "name": "Abu Dhabi ADX",
        "symbol": "FADGI",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 4,
    },
    {
        "code": "BKA",
        "name": "Kuwait Boursa",
        "symbol": "BKA",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 5,
    },
    {
        "code": "BHSEASI",
        "name": "Bahrain",
        "symbol": "BHSEASI",
        "report_section": "GCC & REGIONAL INDICES",
        "display_order": 6,
    },
    {
        "code": "DXY",
        "name": "USD Index",
        "symbol": "DXY",
        "report_section": "SPOT CURRENCY",
        "display_order": 1,
    },
    {
        "code": "EURUSD",
        "name": "EUR/USD",
        "symbol": "EURUSD",
        "report_section": "SPOT CURRENCY",
        "display_order": 2,
    },
    {
        "code": "GBPUSD",
        "name": "GBP/USD",
        "symbol": "GBPUSD",
        "report_section": "SPOT CURRENCY",
        "display_order": 3,
    },
    {
        "code": "USDJPY",
        "name": "USD/JPY",
        "symbol": "USDJPY",
        "report_section": "SPOT CURRENCY",
        "display_order": 4,
    },
    {
        "code": "USDCNY",
        "name": "USD/CNY",
        "symbol": "USDCNY",
        "report_section": "SPOT CURRENCY",
        "display_order": 5,
    },
    {
        "code": "USDQAR",
        "name": "USD/QAR",
        "symbol": "USDQAR",
        "report_section": "QAR CROSS RATES",
        "display_order": 1,
    },
    {
        "code": "EURQAR",
        "name": "EUR/QAR",
        "symbol": "EURQAR",
        "report_section": "QAR CROSS RATES",
        "display_order": 2,
    },
    {
        "code": "GBPQAR",
        "name": "GBP/QAR",
        "symbol": "GBPQAR",
        "report_section": "QAR CROSS RATES",
        "display_order": 3,
    },
    {
        "code": "CNYQAR",
        "name": "CNY/QAR",
        "symbol": "CNYQAR",
        "report_section": "QAR CROSS RATES",
        "display_order": 4,
    },
    {
        "code": "UST5Y",
        "name": "UST 5-Year",
        "symbol": "US5Y",
        "report_section": "FIXED INCOME — UST YIELDS",
        "display_order": 1,
    },
    {
        "code": "UST10Y",
        "name": "UST 10-Year",
        "symbol": "US10Y",
        "report_section": "FIXED INCOME — UST YIELDS",
        "display_order": 2,
    },
    {
        "code": "DHBK",
        "name": "Doha",
        "symbol": "DHBK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 1,
    },
    {
        "code": "QNBK",
        "name": "QNB",
        "symbol": "QNBK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 2,
    },
    {
        "code": "QIBK",
        "name": "QIB",
        "symbol": "QIBK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 3,
    },
    {
        "code": "CBQK",
        "name": "CBQ",
        "symbol": "CBQK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 4,
    },
    {
        "code": "QIIB",
        "name": "QIIB",
        "symbol": "QIIB.QA",
        "report_section": "QATARI BANKS",
        "display_order": 5,
    },
    {
        "code": "MARK",
        "name": "Al Rayan",
        "symbol": "MARK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 6,
    },
    {
        "code": "DUBK",
        "name": "Dukhan",
        "symbol": "DUBK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 7,
    },
    {
        "code": "ABQK",
        "name": "Ahli",
        "symbol": "ABQK.QA",
        "report_section": "QATARI BANKS",
        "display_order": 8,
    },
    {
        "code": "BRENT",
        "name": "Brent Crude",
        "symbol": "BZ=F",
        "report_section": "COMMODITIES & ENERGY",
        "display_order": 1,
    },
    {
        "code": "SILVER",
        "name": "Silver",
        "symbol": "XAGUSD",
        "report_section": "COMMODITIES & ENERGY",
        "display_order": 2,
    },
    {
        "code": "GOLDQAR",
        "name": "Gold (QAR)",
        "symbol": "XAUQAR",
        "report_section": "COMMODITIES & ENERGY",
        "display_order": 3,
    },
]


EXPECTED_BY_CODE = {item["code"]: item for item in EXPECTED_INSTRUMENTS}


NEWS_FEEDS = {
    "global": [
        {
            "source": "Reuters",
            "url": "https://feeds.reuters.com/reuters/businessNews",
            "max": 10,
        },
        {
            "source": "Bloomberg",
            "url": "https://feeds.bloomberg.com/markets/news.rss",
            "max": 10,
        },
    ],
    "qatar": [
        {
            "source": "The Peninsula",
            "url": "https://thepeninsulaqatar.com/rss/business",
            "max": 10,
        },
        {
            "source": "Qatar Tribune",
            "url": "https://www.qatar-tribune.com/rss",
            "max": 10,
        },
    ],
}


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("%", "").strip()
            if cleaned.lower() in ("", "n/a", "na", "null", "none"):
                return None
            return float(cleaned)
        return float(value)
    except Exception:
        return None


def _to_int(value: Any, default: int = 999) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _parse_date(value: Any) -> Optional[datetime.date]:
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    try:
        return datetime.datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _parse_news_datetime(entry: Any) -> Optional[datetime.datetime]:
    parsed_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if parsed_struct:
        try:
            return datetime.datetime(*parsed_struct[:6], tzinfo=datetime.timezone.utc)
        except Exception:
            pass

    for key in ("published", "updated", "created"):
        value = entry.get(key, "") if hasattr(entry, "get") else ""
        if not value:
            continue
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.astimezone(datetime.timezone.utc)
        except Exception:
            continue

    return None


def _age_hours(published_dt: Optional[datetime.datetime], now_utc: Optional[datetime.datetime] = None) -> Optional[float]:
    if not published_dt:
        return None
    now_utc = now_utc or datetime.datetime.now(datetime.timezone.utc)
    return round((now_utc - published_dt.astimezone(datetime.timezone.utc)).total_seconds() / 3600, 2)


def _is_recent_business_news(title: str, summary: str, age: Optional[float]) -> bool:
    if age is None or age < -1 or age > MAX_NEWS_AGE_HOURS:
        return False

    blob = f"{title} {summary}".lower()
    return any(keyword in blob for keyword in BUSINESS_NEWS_KEYWORDS)


def _format_price(value: Optional[float], digits: int = 2):
    if value is None:
        return "N/A"
    try:
        rounded = round(float(value), digits)
        return rounded
    except Exception:
        return "N/A"


def _fmt_pct_from_value(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.{digits}f}%"
    except Exception:
        return "N/A"


def _fmt_pct_number(current: Optional[float], base: Optional[float], digits: int = 2) -> str:
    if current is None or base in (None, 0):
        return "N/A"
    try:
        pct = ((current - base) / base) * 100
        return f"{pct:+.{digits}f}%"
    except Exception:
        return "N/A"


def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _supabase_headers() -> Dict[str, str]:
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_key:
        raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY environment variable")

    return {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }


def _supabase_base_url() -> str:
    supabase_url = os.environ.get("SUPABASE_URL")
    if not supabase_url:
        raise RuntimeError("Missing SUPABASE_URL environment variable")
    return supabase_url.rstrip("/")


def _supabase_get(path: str, params: Optional[Dict[str, str]] = None) -> Any:
    url = f"{_supabase_base_url()}/rest/v1/{path}"
    response = requests.get(
        url,
        headers=_supabase_headers(),
        params=params or {},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _get_latest_available_date(today: datetime.date) -> Optional[datetime.date]:
    params = {
        "select": "as_of_date",
        "order": "as_of_date.desc",
        "limit": "1",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)

    if not rows:
        return None

    return _parse_date(rows[0].get("as_of_date"))


def _get_rows_for_date(as_of_date: datetime.date) -> List[Dict[str, Any]]:
    params = {
        "select": "*",
        "as_of_date": f"eq.{as_of_date.isoformat()}",
        "order": "report_section.asc,display_order.asc,instrument_code.asc",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)
    return rows or []


def _get_history_rows_for_calculations(as_of_date: datetime.date) -> List[Dict[str, Any]]:
    year_start = datetime.date(as_of_date.year, 1, 1)
    history_start = year_start - datetime.timedelta(days=10)

    params = {
        "select": "instrument_code,px_last,change_1d_pct,as_of_date",
        "as_of_date": f"gte.{history_start.isoformat()}",
        "order": "instrument_code.asc,as_of_date.asc",
    }

    rows = _supabase_get(SUPABASE_TABLE, params=params)
    return rows or []


def _group_history_by_code(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        code = row.get("instrument_code")
        if not code:
            continue
        grouped.setdefault(code, []).append(row)

    for code in grouped:
        grouped[code].sort(key=lambda r: str(r.get("as_of_date", "")))

    return grouped


def _last_px_before_or_on(
    history: List[Dict[str, Any]],
    target_date: datetime.date,
) -> Optional[float]:
    found = None

    for row in history:
        row_date = _parse_date(row.get("as_of_date"))
        if row_date is None:
            continue
        if row_date <= target_date:
            px = _to_float(row.get("px_last"))
            if px is not None:
                found = px

    return found


def _previous_px_before_date(
    history: List[Dict[str, Any]],
    target_date: datetime.date,
) -> Optional[float]:
    found = None

    for row in history:
        row_date = _parse_date(row.get("as_of_date"))
        if row_date is None:
            continue
        if row_date < target_date:
            px = _to_float(row.get("px_last"))
            if px is not None:
                found = px

    return found


def _digits_for_code(code: str) -> int:
    if code in {
        "USDQAR",
        "EURQAR",
        "GBPQAR",
        "CNYQAR",
        "EURUSD",
        "GBPUSD",
        "USDCNY",
        "USDJPY",
    }:
        return 4

    if code in {"DHBK", "CBQK", "MARK", "DUBK", "ABQK", "QIIB"}:
        return 3

    if code in {"UST5Y", "UST10Y"}:
        return 4

    if code in {"GOLDQAR", "BRENT", "SILVER"}:
        return 2

    return 2


def _normalise_market_row(
    row: Dict[str, Any],
    history_by_code: Dict[str, List[Dict[str, Any]]],
    effective_date: datetime.date,
) -> Dict[str, Any]:
    code = row.get("instrument_code") or ""
    expected = EXPECTED_BY_CODE.get(code, {})

    px_last = _to_float(row.get("px_last"))
    change_1d_pct = _to_float(row.get("change_1d_pct"))

    history = history_by_code.get(code, [])
    prev_px = _previous_px_before_date(history, effective_date)

    month_start = datetime.date(effective_date.year, effective_date.month, 1)
    year_start = datetime.date(effective_date.year, 1, 1)

    month_base = _last_px_before_or_on(history, month_start - datetime.timedelta(days=1))
    year_base = _last_px_before_or_on(history, year_start - datetime.timedelta(days=1))

    if change_1d_pct is None and prev_px not in (None, 0):
        change_1d = _fmt_pct_number(px_last, prev_px, 2)
    else:
        change_1d = _fmt_pct_from_value(change_1d_pct, 2)

    mtd = _fmt_pct_number(px_last, month_base, 2)
    ytd = _fmt_pct_number(px_last, year_base, 2)

    report_section = row.get("report_section") or expected.get("report_section") or "UNKNOWN"

    return {
        "code": code,
        "name": row.get("instrument_name") or expected.get("name") or code,
        "ticker": row.get("symbol") or expected.get("symbol") or code,
        "px_last": _format_price(px_last, _digits_for_code(code)),
        "change_1d": change_1d,
        "mtd": mtd,
        "ytd": ytd,
        "as_of": str(row.get("as_of_date") or effective_date.isoformat()),
        "source": row.get("source") or "Supabase",
        "status": row.get("status") or "valid",
        "report_section": report_section,
        "display_order": _to_int(row.get("display_order"), expected.get("display_order", 999)),
    }


def fetch_market_data_from_supabase(today: datetime.date) -> tuple[Dict[str, List[Dict[str, Any]]], List[str], Optional[datetime.date]]:
    issues: List[str] = []

    latest_date = _get_latest_available_date(today)
    if latest_date is None:
        raise RuntimeError(f"No rows found in Supabase table {SUPABASE_TABLE}")

    effective_date = latest_date

    if effective_date != today:
        delta_days = (today - effective_date).days
        issues.append(f"Using latest available Supabase market date {effective_date.isoformat()}, not today {today.isoformat()}")

        if delta_days > STALE_DATA_WARNING_DAYS:
            issues.append(f"Supabase market data is stale by {delta_days} days")

    rows = _get_rows_for_date(effective_date)
    history_rows = _get_history_rows_for_calculations(effective_date)
    history_by_code = _group_history_by_code(history_rows)

    expected_codes = {item["code"] for item in EXPECTED_INSTRUMENTS}
    actual_codes = {row.get("instrument_code") for row in rows if row.get("instrument_code")}

    missing_codes = sorted(expected_codes - actual_codes)
    extra_codes = sorted(actual_codes - expected_codes)

    if missing_codes:
        issues.append(f"Missing instruments from Supabase: {', '.join(missing_codes)}")

    if extra_codes:
        issues.append(f"Unexpected instruments in Supabase: {', '.join(extra_codes)}")

    if len(actual_codes) != EXPECTED_INSTRUMENT_COUNT:
        issues.append(f"Expected {EXPECTED_INSTRUMENT_COUNT} instruments, found {len(actual_codes)}")

    output = {
        "global_indices": [],
        "gcc_indices": [],
        "spot_currency": [],
        "qar_cross_rates": [],
        "fixed_income": [],
        "qatari_banks": [],
        "commodities": [],
    }

    normalised_rows = [
        _normalise_market_row(row, history_by_code, effective_date)
        for row in rows
        if row.get("instrument_code") in expected_codes
    ]

    for row in normalised_rows:
        section_name = row.get("report_section", "")
        output_key = REPORT_SECTION_TO_OUTPUT_KEY.get(section_name)

        if not output_key:
            issues.append(f"Unknown report section for {row.get('code')}: {section_name}")
            continue

        clean_row = {
            "code": row["code"],
            "name": row["name"],
            "ticker": row["ticker"],
            "px_last": row["px_last"],
            "change_1d": row["change_1d"],
            "mtd": row["mtd"],
            "ytd": row["ytd"],
            "as_of": row["as_of"],
            "source": row["source"],
            "status": row["status"],
        }

        output[output_key].append((row["display_order"], clean_row))

    for key in output:
        output[key] = [
            row for _, row in sorted(output[key], key=lambda item: item[0])
        ]

    return output, issues, effective_date


def _find_row(rows: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    for row in rows:
        if row.get("name") == name:
            return row
    return None


def validate_market_data(data: Dict[str, Any]) -> List[str]:
    issues: List[str] = []

    for issue in data.get("_supabase_issues", []):
        issues.append(issue)

    market_sections = [
        "global_indices",
        "gcc_indices",
        "spot_currency",
        "qar_cross_rates",
        "fixed_income",
        "qatari_banks",
        "commodities",
    ]

    total_market_rows = sum(len(data.get(section, [])) for section in market_sections)

    if total_market_rows != EXPECTED_INSTRUMENT_COUNT:
        issues.append(f"CRITICAL: Market row count mismatch: expected {EXPECTED_INSTRUMENT_COUNT}, found {total_market_rows}")

    seen_codes = set()

    for section in market_sections:
        for row in data.get(section, []):
            code = str(row.get("code") or "").upper()
            name = row.get("name") or code
            px = _to_float(row.get("px_last"))
            seen_codes.add(code)

            if px is None:
                issues.append(f"CRITICAL: {name} missing PX Last")
                continue

            expected_range = INSTRUMENT_RANGES.get(code)
            if expected_range:
                lo, hi = expected_range
                if not (lo <= px <= hi):
                    issues.append(f"CRITICAL: {name} outside expected range {lo:g}-{hi:g}: {px:g}")

            for field in ("change_1d", "mtd", "ytd"):
                value = str(row.get(field, "")).strip()
                if value in ("", "N/A", "NA", "None", "null"):
                    issues.append(f"WARNING: {name} missing {field.upper()}")

            change_val = _to_float(row.get("change_1d"))
            if change_val is not None:
                if code == "USDQAR" and abs(change_val) > USD_QAR_SUSPICIOUS_MOVE_THRESHOLD:
                    issues.append(f"CRITICAL: USD/QAR daily change suspicious: {row.get('change_1d')}")
                elif code in {"BRENT", "SILVER", "GOLDQAR"} and abs(change_val) > 6:
                    issues.append(f"WARNING: {name} 1D move exceeds commodity review threshold: {row.get('change_1d')}")
                elif code not in {"UST5Y", "UST10Y", "USDQAR"} and abs(change_val) > 10:
                    issues.append(f"WARNING: {name} 1D move exceeds review threshold: {row.get('change_1d')}")

    expected_codes = {item["code"] for item in EXPECTED_INSTRUMENTS}
    missing_codes = sorted(expected_codes - seen_codes)
    if missing_codes:
        issues.append(f"CRITICAL: Missing expected instruments: {', '.join(missing_codes)}")

    return issues


def validate_news_data(data: Dict[str, Any]) -> List[str]:
    issues: List[str] = []

    def check_section(section_key: str, label: str, minimum: int):
        items = data.get(section_key, []) or []

        if len(items) < minimum:
            issues.append(f"CRITICAL: {label} has only {len(items)} valid recent business news items, minimum required {minimum}")

        forbidden = ["temporarily unavailable", "refresh pending", "awaiting source update", "stream incomplete", "market update"]

        for idx, item in enumerate(items, start=1):
            headline = str(item.get("headline") or "")
            summary = str(item.get("summary") or "")
            source = str(item.get("source") or "")
            url = str(item.get("url") or "")
            blob = f"{headline} {summary}".lower()

            if not headline or not source or not url:
                issues.append(f"CRITICAL: {label} item {idx} missing headline/source/url")

            if any(term in blob for term in forbidden):
                issues.append(f"CRITICAL: {label} item {idx} appears to be placeholder text")

    check_section("global_news", "Global news", MIN_GLOBAL_NEWS_ITEMS)
    check_section("qatar_news", "Qatar news", MIN_QATAR_NEWS_ITEMS)

    return issues


def classify_report_status(issues: List[str]) -> str:
    if any(str(issue).startswith("CRITICAL:") for issue in issues):
        return "FAIL"
    if issues:
        return "NEEDS_REVIEW"
    return "PASS"

def fetch_news(feed_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fetch only timestamped, recent, business-relevant RSS items.

    Rules:
    - No timestamp, no article.
    - Older than MAX_NEWS_AGE_HOURS, no article.
    - Not business, markets, economy, banking, investment, energy, or policy related, no article.
    - No placeholder or retry-status items are generated here.
    """
    items = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    for feed_cfg in feed_list:
        try:
            feed = feedparser.parse(feed_cfg["url"])
            print(f"    RSS {feed_cfg['source']} entries: {len(feed.entries)}")

            for entry in feed.entries[: feed_cfg["max"]]:
                title = _clean_text(entry.get("title", ""))
                summary = _clean_text(getattr(entry, "summary", ""))
                link = entry.get("link", "")
                published_dt = _parse_news_datetime(entry)
                age = _age_hours(published_dt, now_utc)

                if not title or not link:
                    continue

                if not _is_recent_business_news(title, summary, age):
                    continue

                items.append({
                    "source": feed_cfg["source"],
                    "title": title,
                    "summary": summary[:500],
                    "link": link,
                    "published": published_dt.isoformat() if published_dt else "",
                    "age_hours": age,
                    "validation_status": "PASS",
                })

        except Exception as e:
            print(f"[WARN] RSS {feed_cfg['source']}: {e}")

    return items

def dedupe_news(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []

    for item in items:
        key = re.sub(r"[^a-z0-9]+", "", item.get("title", "").lower())[:120]

        if not key or key in seen:
            continue

        seen.add(key)
        out.append(item)

    return out


def ensure_min_news(items: List[Dict[str, Any]], count: int, fallback_source: str) -> List[Dict[str, Any]]:
    # Legacy compatibility wrapper. Deliberately does not create placeholder news.
    return list(items)[:count]

def _fallback_summarise_news(raw_items: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
    fallback = []

    for item in raw_items[:count]:
        source = item.get("source", "Feed")
        title = item.get("title", "")[:120]
        summary = item.get("summary", "")[:240]

        blob = f"{title.lower()} {summary.lower()}"
        metric = source.upper()[:8] if source else "NEWS"
        metric_label = "Source"

        if "oil" in blob or "gas" in blob or "energy" in blob:
            metric = "ENERGY"
            metric_label = "Sector"
        elif "bank" in blob or "qnb" in blob or "qib" in blob or "cbq" in blob:
            metric = "BANK"
            metric_label = "Sector"
        elif "tax" in blob or "policy" in blob:
            metric = "POLICY"
            metric_label = "Theme"
        elif "qatar" in blob:
            metric = "QATAR"
            metric_label = "Domestic"

        fallback.append({
            "headline": title or "Market update",
            "summary": summary or "Latest development relevant to markets.",
            "source": source,
            "url": item.get("link", ""),
            "metric": metric,
            "metric_label": metric_label,
        })

    return fallback[:count]


def summarise_news(raw_items: List[Dict[str, Any]], scope: str, count: int) -> List[Dict[str, Any]]:
    if not raw_items:
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if not api_key:
        print("[WARN] ANTHROPIC_API_KEY not set, using fallback summarisation.")
        return _fallback_summarise_news(raw_items, count)

    client = anthropic.Anthropic(api_key=api_key)

    headlines_txt = "\n".join(
        f"[{item['source']}] {item['title']} — {item['summary']} (URL: {item['link']})"
        for item in raw_items
    )

    system = (
        "You are a financial news editor for a Gulf bank daily market intelligence report. "
        "Return only valid JSON. Select the most relevant stories and produce clean metric boxes."
    )

    prompt = f"""
From the following {scope} news items, select up to {count} most relevant stories.

Return a JSON array of up to {count} objects.

Each object must contain exactly these keys:
- headline
- summary
- source
- url
- metric
- metric_label

Rules:
- headline maximum 10 words
- summary maximum 40 words
- metric must be meaningful, short, and never just a dash unless absolutely impossible
- metric_label must explain the metric briefly
- no markdown fences
- no preamble

News:
{headlines_txt}
"""

    try:
        response = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
            max_tokens=1600,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        parsed = json.loads(text)

        if not isinstance(parsed, list):
            raise ValueError("Claude did not return a list")

        cleaned = []

        for item in parsed[:count]:
            cleaned.append({
                "headline": item.get("headline", "")[:120] or "Market update",
                "summary": item.get("summary", "")[:240] or "Latest development relevant to markets.",
                "source": item.get("source", "") or "Feed",
                "url": item.get("url", ""),
                "metric": item.get("metric", "")[:16] or "NEWS",
                "metric_label": item.get("metric_label", "")[:32] or "Signal",
            })

        return cleaned[:count]

    except Exception as e:
        print(f"[WARN] Claude summarisation failed ({scope}): {e}")
        return _fallback_summarise_news(raw_items, count)


def build_kpis(market_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    def px(rows: List[Dict[str, Any]], name: str):
        row = _find_row(rows, name)
        return row.get("px_last", "N/A") if row else "N/A"

    def chg(rows: List[Dict[str, Any]], name: str):
        row = _find_row(rows, name)
        return row.get("change_1d", "N/A") if row else "N/A"

    def ytd(rows: List[Dict[str, Any]], name: str):
        row = _find_row(rows, name)
        return row.get("ytd", "N/A") if row else "N/A"

    sp_1d = chg(market_data.get("global_indices", []), "US S&P 500")
    uk_1d = chg(market_data.get("global_indices", []), "UK FTSE 100")

    eq_label = "Positive"
    if isinstance(sp_1d, str) and isinstance(uk_1d, str):
        if sp_1d.startswith("-") or uk_1d.startswith("-"):
            eq_label = "Mixed"

    brent_px = px(market_data.get("commodities", []), "Brent Crude")
    brent_ytd = ytd(market_data.get("commodities", []), "Brent Crude")

    gold_qar = px(market_data.get("commodities", []), "Gold (QAR)")
    gold_ytd = ytd(market_data.get("commodities", []), "Gold (QAR)")

    qse_px = px(market_data.get("gcc_indices", []), "Qatar QE Index")
    qse_1d = chg(market_data.get("gcc_indices", []), "Qatar QE Index")
    qse_ytd = ytd(market_data.get("gcc_indices", []), "Qatar QE Index")

    ust10_px = px(market_data.get("fixed_income", []), "UST 10-Year")
    ust10_ytd = ytd(market_data.get("fixed_income", []), "UST 10-Year")

    def format_number(value):
        if isinstance(value, (int, float)):
            return f"{value:,.2f}"
        return str(value)

    return [
        {
            "value": eq_label,
            "label": "Global Equities",
            "sublabel": f"US {sp_1d} · UK {uk_1d}",
        },
        {
            "value": f"${format_number(brent_px)}",
            "label": "Brent Crude",
            "sublabel": f"{brent_ytd} Year-to-Date",
        },
        {
            "value": format_number(gold_qar),
            "label": "Gold (QAR)",
            "sublabel": f"{gold_ytd} YTD · Safe-haven demand",
        },
        {
            "value": format_number(qse_px),
            "label": "QSE Index",
            "sublabel": f"{qse_1d} today · {qse_ytd} YTD",
        },
        {
            "value": f"{format_number(ust10_px)}%",
            "label": "UST 10Y Yield",
            "sublabel": f"{ust10_ytd} YTD · Treasury yield curve",
        },
        {
            "value": "4.50%",
            "label": "QCB Sukuk Yield",
            "sublabel": "QR3bn · 2.7x oversubscribed",
        },
    ]


def run() -> Dict[str, Any]:
    today = datetime.date.today()
    cfg = CONFIG

    generated_at_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    data: Dict[str, Any] = {
        "config": cfg,
        "generated_at": generated_at_utc,
        "generated_display_time": cfg.get("delivery_time_ast", "07:00") + " AST",
    }

    print("▶ Fetching market data from Supabase ...")

    try:
        market_sections, supabase_issues, effective_date = fetch_market_data_from_supabase(today)

        for key, rows in market_sections.items():
            if not cfg["sections"].get(key, True):
                data[key] = []
            else:
                data[key] = rows

            print(f"  · {key}: {len(data[key])} rows")

        data["_supabase_issues"] = supabase_issues
        data["market_as_of_date"] = effective_date.isoformat() if effective_date else None

    except Exception as e:
        print(f"[ERROR] Supabase market fetch failed: {e}")

        for key in [
            "global_indices",
            "gcc_indices",
            "spot_currency",
            "qar_cross_rates",
            "fixed_income",
            "qatari_banks",
            "commodities",
        ]:
            data[key] = []

        data["_supabase_issues"] = [f"Supabase market fetch failed: {e}"]
        data["market_as_of_date"] = None

    if cfg["sections"].get("global_news", True):
        print("  · global news")
        raw_global = dedupe_news(fetch_news(NEWS_FEEDS["global"]))
        print(f"    Valid recent global business items found: {len(raw_global)}")
        data["global_news"] = summarise_news(raw_global, "regional and global", 6)
    else:
        data["global_news"] = []

    if cfg["sections"].get("qatar_news", True):
        print("  · qatar news")
        raw_qatar = dedupe_news(fetch_news(NEWS_FEEDS["qatar"]))
        print(f"    Valid recent Qatar business items found: {len(raw_qatar)}")
        data["qatar_news"] = summarise_news(raw_qatar, "qatar", 4)
    else:
        data["qatar_news"] = []

    data["kpis"] = build_kpis(data)

    validation_issues = validate_market_data(data) + validate_news_data(data)
    data["validation_issues"] = validation_issues
    data["report_status"] = classify_report_status(validation_issues)

    if "_supabase_issues" in data:
        del data["_supabase_issues"]

    print("✓ Fetch complete.")

    if validation_issues:
        print("⚠ Validation issues found:")
        for issue in validation_issues:
            print(f"   - {issue}")
    else:
        print("✓ Validation passed.")

    print(f"Report status: {data['report_status']}")

    return data


if __name__ == "__main__":
    result = run()

    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    print("✓ Data written to market_data.json")
