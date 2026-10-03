import argparse
import json
import logging
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "call_id",
    "agent_id",
    "customer_phone",
    "start_time",
    "end_time",
    "call_outcome",
    "language",
    "disposition_code",
    "retry_flag",
]
CALL_COLUMNS = [
    *REQUIRED_FIELDS[:-1],
    "amount_promised",
    "retry_flag",
    "call_duration_seconds",
    "call_hour",
    "call_date",
    "is_weekend",
    "duration_bucket",
    "is_amount_imputed",
    "delivery_timestamp",
]
ALLOWED_OUTCOMES = {"connected", "no_answer", "dropped", "callback_requested"}
ALLOWED_LANGUAGES = {"Hindi", "English", "Marathi"}


class RecordValidationError(ValueError):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the call-log ETL pipeline.")
    parser.add_argument("--input", type=str, default="data/raw_calls.json", help="Path to the raw JSON dataset.")
    parser.add_argument("--db-path", type=str, default="db/call_data.db", help="SQLite output database path.")
    parser.add_argument(
        "--rejected-log",
        type=str,
        default="data/rejected_log.json",
        help="Path to write rejected records and their reasons.",
    )
    return parser.parse_args()


def load_raw_json(path: str | Path) -> list[Any]:
    with Path(path).open("r", encoding="utf-8") as file_obj:
        records = json.load(file_obj)
    if not isinstance(records, list):
        raise ValueError("JSON file must contain a list of records.")
    return records


def parse_timestamp(value: str) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as error:
        raise RecordValidationError("malformed_timestamp", f"Invalid timestamp: {value!r}") from error
    if pd.isna(timestamp):
        raise RecordValidationError("malformed_timestamp", f"Invalid timestamp: {value!r}")
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("Asia/Kolkata")
    return timestamp.tz_convert("Asia/Kolkata")


def normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise RecordValidationError("bad_type", "Record must be a JSON object.")

    missing_fields = [
        field for field in REQUIRED_FIELDS
        if field not in record or record[field] is None
    ]
    if missing_fields:
        raise RecordValidationError("missing_field", f"Missing required fields: {', '.join(missing_fields)}")

    normalized = dict(record)
    string_fields = [
        "call_id",
        "agent_id",
        "customer_phone",
        "call_outcome",
        "language",
        "disposition_code",
    ]
    for field in string_fields:
        if not isinstance(record[field], str):
            raise RecordValidationError("bad_type", f"{field} must be a string.")
        normalized[field] = record[field].strip()
        if not normalized[field]:
            raise RecordValidationError("invalid_value", f"{field} cannot be empty.")

    normalized["call_outcome"] = normalized["call_outcome"].lower()
    if normalized["call_outcome"] not in ALLOWED_OUTCOMES:
        raise RecordValidationError("invalid_value", f"Unsupported call_outcome: {normalized['call_outcome']!r}")
    if normalized["language"] not in ALLOWED_LANGUAGES:
        raise RecordValidationError("invalid_value", f"Unsupported language: {normalized['language']!r}")

    for field in ("start_time", "end_time"):
        if not isinstance(record[field], str):
            raise RecordValidationError("bad_type", f"{field} must be a timestamp string.")
        try:
            normalized[field] = parse_timestamp(record[field]).isoformat(sep=" ")
        except RecordValidationError as error:
            raise RecordValidationError(error.reason, f"{field}: {error}") from error

    start_time = pd.Timestamp(normalized["start_time"])
    end_time = pd.Timestamp(normalized["end_time"])
    if end_time < start_time:
        raise RecordValidationError("invalid_value", "end_time cannot be before start_time.")

    retry_flag = record["retry_flag"]
    if isinstance(retry_flag, bool):
        normalized["retry_flag"] = retry_flag
    elif isinstance(retry_flag, int) and retry_flag in (0, 1):
        normalized["retry_flag"] = bool(retry_flag)
    elif isinstance(retry_flag, str) and retry_flag.strip().lower() in {"true", "false", "yes", "no", "0", "1"}:
        normalized["retry_flag"] = retry_flag.strip().lower() in {"true", "yes", "1"}
    else:
        raise RecordValidationError("bad_type", "retry_flag must be a boolean value.")

    amount = record.get("amount_promised")
    if amount is not None:
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise RecordValidationError("bad_type", "amount_promised must be a number or null.")
        if not math.isfinite(amount) or amount < 0:
            raise RecordValidationError("invalid_value", "amount_promised must be finite and non-negative.")
        normalized["amount_promised"] = float(amount)
    else:
        normalized["amount_promised"] = None

    return normalized


def ingest_records(raw_records: Iterable[Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    seen_records: set[str] = set()

    for raw_record in raw_records:
        if not isinstance(raw_record, dict):
            rejected.append({"record": raw_record, "reason": "bad_type", "detail": "Record must be a JSON object."})
            counts["bad_type"] += 1
            continue

        fingerprint = json.dumps(raw_record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        if fingerprint in seen_records:
            rejected.append({"record": raw_record, "reason": "duplicate", "detail": "Exact duplicate raw record."})
            counts["duplicate"] += 1
            continue
        seen_records.add(fingerprint)

        try:
            accepted.append(normalize_record(raw_record))
        except RecordValidationError as error:
            rejected.append({"record": raw_record, "reason": error.reason, "detail": str(error)})
            counts[error.reason] += 1

    return accepted, rejected, dict(counts)


def derive_duration_bucket(duration_seconds: float) -> str:
    if duration_seconds < 60:
        return "short"
    if duration_seconds <= 300:
        return "medium"
    return "long"


def _to_ist(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("Asia/Kolkata")
    return timestamp.tz_convert("Asia/Kolkata")


def transform_records(valid_records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(valid_records)
    if df.empty:
        return pd.DataFrame(columns=CALL_COLUMNS)

    df["start_time_ist"] = df["start_time"].map(_to_ist)
    df["end_time_ist"] = df["end_time"].map(_to_ist)
    df["call_duration_seconds"] = (df["end_time_ist"] - df["start_time_ist"]).dt.total_seconds()
    df["call_hour"] = df["start_time_ist"].dt.hour
    df["call_date"] = df["start_time_ist"].dt.strftime("%Y-%m-%d")
    df["is_weekend"] = df["start_time_ist"].dt.dayofweek.ge(5)
    df["is_amount_imputed"] = df["amount_promised"].isna()
    df["amount_promised"] = pd.to_numeric(df["amount_promised"], errors="coerce").fillna(0.0)
    df["duration_bucket"] = df["call_duration_seconds"].map(derive_duration_bucket)
    df["start_time"] = df["start_time_ist"].map(lambda value: value.isoformat(sep=" "))
    df["end_time"] = df["end_time_ist"].map(lambda value: value.isoformat(sep=" "))

    df = df.sort_values(["call_id", "start_time_ist"], ascending=[True, False], kind="stable")
    df = df.drop_duplicates(subset=["call_id"], keep="first")
    df["delivery_timestamp"] = datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds")
    df["call_duration_seconds"] = df["call_duration_seconds"].round(2)
    df["amount_promised"] = df["amount_promised"].round(2)
    return df[CALL_COLUMNS].reset_index(drop=True)


def create_sqlite_database(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS calls (
            call_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            customer_phone TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            call_outcome TEXT NOT NULL,
            language TEXT NOT NULL,
            disposition_code TEXT NOT NULL,
            amount_promised REAL NOT NULL,
            retry_flag INTEGER NOT NULL,
            call_duration_seconds REAL NOT NULL,
            call_hour INTEGER NOT NULL,
            call_date TEXT NOT NULL,
            is_weekend INTEGER NOT NULL,
            duration_bucket TEXT NOT NULL,
            is_amount_imputed INTEGER NOT NULL,
            delivery_timestamp TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT NOT NULL,
            records_processed INTEGER NOT NULL,
            rejected_count INTEGER NOT NULL,
            rejection_breakdown TEXT NOT NULL
        )
        """
    )
    return conn


def load_to_sqlite(
    conn: sqlite3.Connection,
    transformed: pd.DataFrame,
    raw_count: int,
    rejected_counts: dict[str, int],
) -> None:
    placeholders = ", ".join("?" for _ in CALL_COLUMNS)
    update_columns = [column for column in CALL_COLUMNS if column != "call_id"]
    updates = ", ".join(f"{column} = excluded.{column}" for column in update_columns)
    statement = (
        f"INSERT INTO calls ({', '.join(CALL_COLUMNS)}) VALUES ({placeholders}) "
        f"ON CONFLICT(call_id) DO UPDATE SET {updates}"
    )

    records = transformed.to_dict(orient="records")
    rows = []
    for record in records:
        rows.append(
            tuple(
                int(record[column]) if column in {"retry_flag", "is_weekend", "is_amount_imputed"} else record[column]
                for column in CALL_COLUMNS
            )
        )

    run_timestamp = datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds")
    rejected_count = sum(rejected_counts.values())
    with conn:
        if rows:
            conn.executemany(statement, rows)
        conn.execute(
            """
            INSERT INTO ingestion_log (run_timestamp, records_processed, rejected_count, rejection_breakdown)
            VALUES (?, ?, ?, ?)
            """,
            (run_timestamp, raw_count, rejected_count, json.dumps(rejected_counts, sort_keys=True)),
        )


def write_rejected_log(rejected_records: list[dict[str, Any]], path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(rejected_records, file_obj, indent=2, ensure_ascii=False)


def run_pipeline(
    input_path: str | Path,
    db_path: str | Path,
    rejected_log_path: str | Path | None = None,
) -> dict[str, Any]:
    raw_records = load_raw_json(input_path)
    valid_records, rejected, rejection_summary = ingest_records(raw_records)
    transformed = transform_records(valid_records)
    if rejected_log_path is not None:
        write_rejected_log(rejected, rejected_log_path)

    conn = create_sqlite_database(db_path)
    try:
        load_to_sqlite(conn, transformed, len(raw_records), rejection_summary)
    finally:
        conn.close()

    return {
        "total_raw_records": len(raw_records),
        "valid_records": len(valid_records),
        "rejected_records": sum(rejection_summary.values()),
        "rejection_summary": rejection_summary,
        "clean_records": len(transformed),
    }


def print_pipeline_summary(summary: dict[str, Any]) -> None:
    metrics = [
        ("Total raw records", summary["total_raw_records"]),
        ("Accepted records", summary["valid_records"]),
        ("Rejected records", summary["rejected_records"]),
        ("Transformed records", summary["clean_records"]),
    ]
    metric_width = max(len("Metric"), *(len(label) for label, _ in metrics))
    value_width = max(len("Count"), *(len(str(value)) for _, value in metrics))
    border = f"+-{'-' * metric_width}-+-{'-' * value_width}-+"

    print("\nETL PIPELINE SUMMARY")
    print(border)
    print(f"| {'Metric':<{metric_width}} | {'Count':>{value_width}} |")
    print(border)
    for label, value in metrics:
        print(f"| {label:<{metric_width}} | {value:>{value_width}} |")
    print(border)

    rejection_counts = summary["rejection_summary"]
    reason_width = max(len("Reason"), *(len(reason) for reason in rejection_counts)) if rejection_counts else len("Reason")
    count_width = max(len("Count"), *(len(str(count)) for count in rejection_counts.values())) if rejection_counts else len("Count")
    border = f"+-{'-' * reason_width}-+-{'-' * count_width}-+"
    print("\nREJECTION BREAKDOWN")
    print(border)
    print(f"| {'Reason':<{reason_width}} | {'Count':>{count_width}} |")
    print(border)
    for reason, count in sorted(rejection_counts.items()):
        print(f"| {reason:<{reason_width}} | {count:>{count_width}} |")
    if not rejection_counts:
        print(f"| {'None':<{reason_width}} | {0:>{count_width}} |")
    print(border)


def main() -> None:
    args = parse_args()
    summary = run_pipeline(args.input, args.db_path, args.rejected_log)
    print_pipeline_summary(summary)


if __name__ == "__main__":
    main()
