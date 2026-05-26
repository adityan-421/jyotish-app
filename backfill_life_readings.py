#!/usr/bin/env python3
"""
Backfill life readings for all saved charts that don't have one yet.

Usage:
    python backfill_life_readings.py [--dry-run]

Requires the same environment variables as the app (DB_HOST etc. + GCP creds).
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── DB helpers (inline to avoid importing Flask app) ──────────────────────────

import psycopg2
import psycopg2.extras


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", ""),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", ""),
        connect_timeout=10,
    )


def fetch_all_charts(conn):
    """Return all rows from saved_charts as a list of dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, user_id, input_data, chart_data FROM saved_charts ORDER BY id")
        return cur.fetchall()


def patch_chart_data(conn, chart_id, new_chart_data_json):
    """UPDATE saved_charts SET chart_data = %s WHERE id = %s"""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE saved_charts SET chart_data = %s WHERE id = %s",
            (new_chart_data_json, chart_id),
        )
    conn.commit()


# ── Prompt chain (minimal, no Flask dependency) ────────────────────────────────

def _load_prompts():
    path = Path(__file__).parent / "prompts.json"
    with open(path) as f:
        return json.load(f)


class _SafeFormatDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def _run_step(model, step, variables):
    prompt_text = step["prompt"]
    for key, value in variables.items():
        prompt_text = prompt_text.replace("{" + key + "}", str(value))
    response = model.generate_content(prompt_text)
    result_text = response.text.strip()

    if step["response_type"] == "json":
        cleaned = result_text
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
        try:
            result = json.loads(cleaned)
        except json.JSONDecodeError:
            result = step.get("json_fallback", {})
    else:
        result = result_text

    variables[step["output_var"]] = result
    if isinstance(result, (dict, list)):
        variables[step["output_var"]] = json.dumps(result, indent=2)

    if step.get("is_final"):
        return variables[step["output_var"]]
    return None


def generate_life_reading(chart_data: dict, model, steps: list) -> dict:
    """Call Vertex AI and return the life_reading dict."""
    trimmed = dict(chart_data)
    if "dasha" in trimmed:
        d = dict(trimmed["dasha"])
        d.pop("pratyantar", None)
        trimmed["dasha"] = d
    trimmed["current_date"] = datetime.now().strftime("%d-%b-%Y")

    variables = {
        "chart_data": json.dumps(trimmed, indent=2),
        "today": datetime.now().strftime("%d-%b-%Y"),
    }

    final = None
    for step in steps:
        final = _run_step(model, step, variables)

    # final is a JSON string (is_final serializes dicts)
    if isinstance(final, str):
        try:
            cleaned = final
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].strip()
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return steps[0].get("json_fallback", {})
    return final if isinstance(final, dict) else {}


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill life readings for saved charts.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be updated without writing to DB.")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds to wait between API calls (default: 2).")
    args = parser.parse_args()

    # Init Vertex AI
    import vertexai
    from vertexai.generative_models import GenerativeModel

    project = os.environ.get("GCP_PROJECT", "grahalogic")
    location = os.environ.get("GCP_LOCATION", "us-central1")
    vertexai.init(project=project, location=location)

    prompts_config = _load_prompts()
    model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))
    steps = prompts_config.get("life_reading_steps", [])
    if not steps:
        logger.error("life_reading_steps not found in prompts.json — aborting.")
        sys.exit(1)

    # Connect to DB
    logger.info("Connecting to database...")
    try:
        conn = _get_conn()
    except Exception as e:
        logger.error("DB connection failed: %s", e)
        sys.exit(1)

    rows = fetch_all_charts(conn)
    logger.info("Found %d total charts.", len(rows))

    to_process = []
    for row in rows:
        try:
            cd = json.loads(row["chart_data"])
        except (json.JSONDecodeError, TypeError):
            logger.warning("Chart %d: invalid chart_data JSON — skipping.", row["id"])
            continue
        if "life_reading" not in cd:
            to_process.append((row["id"], row["user_id"], cd))

    logger.info("%d charts need a life reading.", len(to_process))
    if not to_process:
        logger.info("Nothing to do.")
        conn.close()
        return

    ok = 0
    failed = 0
    for i, (chart_id, user_id, chart_data) in enumerate(to_process, 1):
        logger.info("[%d/%d] Chart %d (user %s)...", i, len(to_process), chart_id, user_id)

        if args.dry_run:
            logger.info("  [dry-run] would generate + patch chart %d", chart_id)
            continue

        try:
            reading = generate_life_reading(chart_data, model, steps)
            chart_data["life_reading"] = reading
            patch_chart_data(conn, chart_id, json.dumps(chart_data))
            logger.info("  ✓ patched (overall_theme: %s...)",
                        str(reading.get("overall_theme", ""))[:60])
            ok += 1
        except Exception as e:
            logger.error("  ✗ chart %d failed: %s", chart_id, e)
            failed += 1

        if i < len(to_process):
            time.sleep(args.delay)

    conn.close()
    logger.info("Done. %d updated, %d failed.", ok, failed)


if __name__ == "__main__":
    main()
