import argparse
import json
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RANDOM_SEED = 42
REFERENCE_DATETIME = datetime(2026, 10, 2)
LANGUAGES = ["Hindi", "English", "Marathi"]
CALL_OUTCOMES = ["connected", "no_answer", "dropped", "callback_requested"]
DISPOSITION_CODES = ["D1", "D2", "D3", "D4", "D5", "D6"]
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


def build_base_record(rng: random.Random, index: int) -> dict[str, Any]:
    start_dt = REFERENCE_DATETIME - timedelta(
        days=rng.randint(0, 89),
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
    )
    end_dt = start_dt + timedelta(seconds=rng.randint(15, 900))

    return {
        "call_id": f"CALL-{index:04d}",
        "agent_id": f"AGT-{rng.randint(1, 20):03d}",
        "customer_phone": f"+91{rng.randint(7000000000, 9999999999)}",
        "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "call_outcome": rng.choice(CALL_OUTCOMES),
        "language": rng.choice(LANGUAGES),
        "disposition_code": rng.choice(DISPOSITION_CODES),
        "amount_promised": round(rng.uniform(100, 2000), 2) if rng.random() > 0.2 else None,
        "retry_flag": rng.choice([True, False]),
    }


def generate_raw_records(num_records: int = 500, seed: int = RANDOM_SEED) -> list[dict[str, Any]]:
    if num_records < 0:
        raise ValueError("num_records must be non-negative.")

    rng = random.Random(seed)
    duplicate_count = min(round(num_records * 0.05), num_records)
    unique_count = num_records - duplicate_count
    records = [build_base_record(rng, index) for index in range(1, unique_count + 1)]

    corruption_candidates = list(range(duplicate_count, unique_count))
    missing_count = min(round(num_records * 0.15), len(corruption_candidates))
    missing_indices = set(rng.sample(corruption_candidates, missing_count))
    timestamp_candidates = [index for index in corruption_candidates if index not in missing_indices]
    malformed_count = min(round(num_records * 0.03), len(timestamp_candidates))
    malformed_indices = set(rng.sample(timestamp_candidates, malformed_count))

    for index in missing_indices:
        field = rng.choice(REQUIRED_FIELDS)
        records[index][field] = None

    for index in malformed_indices:
        field = rng.choice(["start_time", "end_time"])
        records[index][field] = "not-a-valid-timestamp"

    duplicate_sources = records[:duplicate_count]
    records.extend(record.copy() for record in duplicate_sources)
    return records


def write_json_records(records: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(records, file_obj, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate noisy simulated call logs.")
    parser.add_argument("--output", type=str, default="data/raw_calls.json", help="Path to write the raw JSON dataset.")
    parser.add_argument("--count", type=int, default=500, help="Number of records in the final dataset.")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed for reproducibility.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    records = generate_raw_records(num_records=args.count, seed=args.seed)
    output_path = Path(args.output)
    write_json_records(records, output_path)
    logger.info("Generated %s raw records to %s", len(records), output_path)


if __name__ == "__main__":
    main()
