import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


QUERY_DEFINITIONS = {
    "connect_rate_by_language": {
        "sql": """
            SELECT
                language,
                COUNT(*) AS total_calls,
                ROUND(100.0 * SUM(CASE WHEN call_outcome = 'connected' THEN 1 ELSE 0 END) / COUNT(*), 2) AS connect_rate_pct
            FROM calls
            GROUP BY language
            ORDER BY connect_rate_pct DESC, language
        """,
        "filename": "connect_rate_by_language.csv",
    },
    "callback_rate_by_hour": {
        "sql": """
            SELECT
                call_hour,
                COUNT(*) AS total_calls,
                ROUND(100.0 * SUM(CASE WHEN call_outcome = 'callback_requested' THEN 1 ELSE 0 END) / COUNT(*), 2) AS callback_rate_pct
            FROM calls
            GROUP BY call_hour
            ORDER BY callback_rate_pct DESC, call_hour
        """,
        "filename": "callback_rate_by_hour.csv",
    },
    "long_duration_summary": {
        "sql": """
            SELECT
                ROUND(100.0 * SUM(CASE WHEN duration_bucket = 'long' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_long_calls,
                ROUND(AVG(CASE WHEN duration_bucket = 'long' THEN amount_promised END), 2) AS avg_amount_promised_for_long_calls
            FROM calls
        """,
        "filename": "long_duration_summary.csv",
    },
    "top_agents_by_calls": {
        "sql": """
            SELECT
                agent_id,
                COUNT(*) AS total_calls,
                SUM(CASE WHEN call_outcome = 'connected' THEN 1 ELSE 0 END) AS connected,
                SUM(CASE WHEN call_outcome = 'no_answer' THEN 1 ELSE 0 END) AS no_answer,
                SUM(CASE WHEN call_outcome = 'dropped' THEN 1 ELSE 0 END) AS dropped,
                SUM(CASE WHEN call_outcome = 'callback_requested' THEN 1 ELSE 0 END) AS callback_requested
            FROM calls
            GROUP BY agent_id
            ORDER BY total_calls DESC, agent_id
            LIMIT 3
        """,
        "filename": "top_agents_by_calls.csv",
    },
    "daily_volume_trend": {
        "sql": """
            SELECT
                call_date,
                COUNT(*) AS daily_calls
            FROM calls
            GROUP BY call_date
            ORDER BY call_date
        """,
        "filename": "daily_volume_trend.csv",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the analytics queries against the SQLite call database.")
    parser.add_argument("--db-path", type=str, default="db/call_data.db", help="SQLite database path.")
    parser.add_argument("--output-dir", type=str, default="output", help="Directory to write CSV outputs.")
    return parser.parse_args()


def run_queries(db_path: str, output_dir: str) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    for name, definition in QUERY_DEFINITIONS.items():
        df = pd.read_sql_query(definition["sql"], conn)
        target = output_path / definition["filename"]
        df.to_csv(target, index=False)
        logger.info("%s: %s rows -> %s", name, len(df), target)

    conn.close()


def main() -> None:
    args = parse_args()
    run_queries(args.db_path, args.output_dir)


if __name__ == "__main__":
    main()
