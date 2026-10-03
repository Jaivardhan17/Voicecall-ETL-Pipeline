import json
import sys
from pathlib import Path
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from generate import generate_raw_records
from pipeline import (
    derive_duration_bucket,
    ingest_records,
    normalize_record,
    run_pipeline,
    transform_records,
)


def make_record(**overrides):
    record = {
        "call_id": "CALL-001",
        "agent_id": "AGT-001",
        "customer_phone": "+919999999999",
        "start_time": "2026-10-01 10:00:00",
        "end_time": "2026-10-01 10:02:00",
        "call_outcome": "connected",
        "language": "English",
        "disposition_code": "D1",
        "amount_promised": 100.0,
        "retry_flag": False,
    }
    record.update(overrides)
    return record


def test_generator_writes_exactly_500_reproducible_records():
    first = generate_raw_records(num_records=500, seed=42)
    second = generate_raw_records(num_records=500, seed=42)

    assert len(first) == 500
    assert first == second


def test_generator_includes_exact_duplicate_records():
    records = generate_raw_records(num_records=500, seed=42)
    signatures = [json.dumps(record, sort_keys=True) for record in records]
    duplicate_count = len(signatures) - len(set(signatures))

    assert duplicate_count == 25


def test_generator_corruption_rates_are_as_requested():
    records = generate_raw_records(num_records=500, seed=42)
    missing_count = sum(
        any(field not in record or record[field] is None for field in (
            "call_id",
            "agent_id",
            "customer_phone",
            "start_time",
            "end_time",
            "call_outcome",
            "language",
            "disposition_code",
            "retry_flag",
        ))
        for record in records
    )
    malformed_count = sum(
        record.get("start_time") == "not-a-valid-timestamp"
        or record.get("end_time") == "not-a-valid-timestamp"
        for record in records
    )

    assert missing_count == 75
    assert malformed_count == 15


def test_ingest_rejects_missing_field():
    records = [{
        "call_id": "CALL-001",
        "agent_id": "AGT-001",
        "customer_phone": "+919999999999",
        "start_time": "2024-01-01 08:00:00",
        "end_time": "2024-01-01 08:10:00",
        "call_outcome": "connected",
        "language": "English",
        "disposition_code": "D1",
        "amount_promised": None,
        "retry_flag": False,
    }]
    records[0].pop("customer_phone")

    _, rejected, summary = ingest_records(records)
    assert summary.get("missing_field") == 1
    assert rejected[0]["reason"] == "missing_field"


def test_normalize_record_accepts_valid_data():
    record = {
        "call_id": "CALL-002",
        "agent_id": "AGT-002",
        "customer_phone": "+919876543210",
        "start_time": "2024-03-03 09:15:00",
        "end_time": "2024-03-03 09:45:00",
        "call_outcome": "callback_requested",
        "language": "Hindi",
        "disposition_code": "D4",
        "amount_promised": 123.45,
        "retry_flag": "true",
    }

    normalized = normalize_record(record)
    assert normalized["call_outcome"] == "callback_requested"
    assert normalized["retry_flag"] is True
    assert normalized["amount_promised"] == 123.45


def test_ingest_rejects_malformed_timestamp_with_specific_reason():
    accepted, rejected, summary = ingest_records([make_record(start_time="bad timestamp")])

    assert not accepted
    assert summary["malformed_timestamp"] == 1
    assert rejected[0]["reason"] == "malformed_timestamp"
    assert "start_time" in rejected[0]["detail"]


def test_ingest_distinguishes_bad_type_and_invalid_value():
    records = [
        make_record(retry_flag="sometimes"),
        make_record(call_outcome="unknown"),
    ]

    _, rejected, summary = ingest_records(records)

    assert summary == {"bad_type": 1, "invalid_value": 1}
    assert {item["reason"] for item in rejected} == {"bad_type", "invalid_value"}


def test_null_amount_is_valid_and_imputed():
    accepted, rejected, _ = ingest_records([make_record(amount_promised=None)])
    result = transform_records(accepted)

    assert not rejected
    assert result.iloc[0]["amount_promised"] == 0
    assert bool(result.iloc[0]["is_amount_imputed"])


def test_transform_keeps_latest_nonidentical_record_for_repeated_call_id():
    records = [
        make_record(call_id="CALL-300", start_time="2026-10-01 10:00:00", end_time="2026-10-01 10:20:00"),
        make_record(
            call_id="CALL-300",
            start_time="2026-10-01 11:00:00",
            end_time="2026-10-01 11:20:00",
            call_outcome="dropped",
            amount_promised=150,
            retry_flag=True,
        ),
    ]

    accepted, rejected, summary = ingest_records(records)
    result = transform_records(accepted)

    assert len(accepted) == 2
    assert not rejected
    assert not summary
    assert len(result) == 1
    assert result.iloc[0]["call_outcome"] == "dropped"
    assert result.iloc[0]["start_time"] == "2026-10-01 11:00:00+05:30"
    assert result.iloc[0]["call_hour"] == 11


def test_transform_converts_aware_timestamp_to_ist():
    result = transform_records([
        make_record(
            start_time="2026-10-01T10:00:00+00:00",
            end_time="2026-10-01T10:02:00+00:00",
        )
    ])

    assert result.iloc[0]["call_hour"] == 15
    assert result.iloc[0]["start_time"].endswith("+05:30")


def test_transform_calculates_duration_and_weekend_flag():
    result = transform_records([
        make_record(
            start_time="2026-10-03 10:00:00",
            end_time="2026-10-03 10:02:30",
        )
    ])

    assert result.iloc[0]["call_duration_seconds"] == 150
    assert bool(result.iloc[0]["is_weekend"])


def test_duration_bucket_classification():
    assert derive_duration_bucket(59.99) == "short"
    assert derive_duration_bucket(60) == "medium"
    assert derive_duration_bucket(300) == "medium"
    assert derive_duration_bucket(300.01) == "long"


def test_database_upsert_is_idempotent_and_log_persists(tmp_path):
    input_path = tmp_path / "calls.json"
    database_path = tmp_path / "calls.db"
    input_path.write_text(json.dumps([make_record()]), encoding="utf-8")

    first_summary = run_pipeline(input_path, database_path)
    second_summary = run_pipeline(input_path, database_path)

    with sqlite3.connect(database_path) as connection:
        call_count = connection.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
        unique_call_count = connection.execute("SELECT COUNT(DISTINCT call_id) FROM calls").fetchone()[0]
        log_count = connection.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]

    assert first_summary["clean_records"] == 1
    assert second_summary["clean_records"] == 1
    assert call_count == 1
    assert unique_call_count == 1
    assert log_count == 2


def test_database_retains_calls_absent_from_later_input(tmp_path):
    input_path = tmp_path / "calls.json"
    database_path = tmp_path / "calls.db"
    input_path.write_text(
        json.dumps([make_record(call_id="CALL-001"), make_record(call_id="CALL-002")]),
        encoding="utf-8",
    )
    run_pipeline(input_path, database_path)

    input_path.write_text(json.dumps([make_record(call_id="CALL-001")]), encoding="utf-8")
    run_pipeline(input_path, database_path)

    with sqlite3.connect(database_path) as connection:
        call_ids = {
            row[0] for row in connection.execute("SELECT call_id FROM calls").fetchall()
        }

    assert call_ids == {"CALL-001", "CALL-002"}
