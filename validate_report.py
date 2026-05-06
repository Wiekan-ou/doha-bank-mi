import json
import os
import sys
from pathlib import Path

MARKET_DATA_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("market_data.json")
PASS_STATUSES = {"PASS", "OK"}


def write_github_output(status: str):
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"status={status}\n")


def main() -> int:
    if not MARKET_DATA_PATH.exists():
        status = "FAIL"
        print(f"CRITICAL: {MARKET_DATA_PATH} not found")
        write_github_output(status)
        return 0

    with MARKET_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    issues = list(data.get("validation_issues") or [])
    raw_status = str(data.get("report_status") or "").upper()

    if raw_status in PASS_STATUSES and not issues:
        status = "PASS"
    elif raw_status == "PASS" and issues:
        status = "NEEDS_REVIEW"
    elif raw_status in {"FAIL", "NEEDS_REVIEW"}:
        status = raw_status
    elif any(str(x).startswith("CRITICAL:") for x in issues):
        status = "FAIL"
    elif issues:
        status = "NEEDS_REVIEW"
    else:
        status = "PASS"

    data["report_status"] = status
    data["email_send_allowed"] = status == "PASS"

    with MARKET_DATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"Validation status: {status}")
    if issues:
        print("Validation issues:")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("No validation issues.")

    write_github_output(status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
