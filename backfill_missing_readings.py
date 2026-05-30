#!/usr/bin/env python3
"""
Backfill comprehensive AI readings for saved_charts that have NO reading yet
(reading IS NULL). Complements backfill_readings.py (which only regenerates
charts that already have a reading).

Usage (as Cloud Run Job args):
    python3 backfill_missing_readings.py count       # just report scope, no AI calls
    python3 backfill_missing_readings.py generate     # generate + store readings

Env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, GCP_PROJECT, GCP_LOCATION
"""
import json, os, sys, logging
from datetime import datetime

import psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

MODE = sys.argv[1] if len(sys.argv) > 1 else "count"
if MODE not in ("count", "generate"):
    log.error("Unknown mode %r — use 'count' or 'generate'", MODE)
    sys.exit(2)

# ── DB ──────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Charts needing a reading: never generated (NULL) OR stuck on the
# "Unable to generate reading..." fallback from a past failed generation.
MISSING_WHERE = ("reading IS NULL OR "
                 "reading::text LIKE '%Unable to generate reading%'")

# ── Scope report ────────────────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) AS c FROM saved_charts")
total = cur.fetchone()["c"]
cur.execute(f"SELECT COUNT(*) AS c FROM saved_charts WHERE {MISSING_WHERE}")
missing = cur.fetchone()["c"]
cur.execute(f"SELECT COUNT(DISTINCT user_id) AS c FROM saved_charts WHERE {MISSING_WHERE}")
missing_users = cur.fetchone()["c"]
log.info("Scope: total_charts=%d  missing_reading=%d  affected_users=%d",
         total, missing, missing_users)

if MODE == "count":
    cur.close(); conn.close()
    log.info("count mode — no readings generated.")
    sys.exit(0)

# ── generate mode: heavy imports only now ───────────────────────────────────
import vertexai
from vertexai.generative_models import GenerativeModel
from app import (_strip_chart_for_ai, _run_prompt_chain,
                 _format_transits_for_ai, load_prompts)
from jyotish_engine import compute_transits

cur.execute(f"""
    SELECT id, user_id, name, chart_data
    FROM saved_charts
    WHERE {MISSING_WHERE}
    ORDER BY id
""")
rows = cur.fetchall()
log.info("Generating readings for %d charts...", len(rows))

vertexai.init(
    project=os.environ.get("GCP_PROJECT", "grahalogic"),
    location=os.environ.get("GCP_LOCATION", "us-central1"),
)
prompts_config = load_prompts()
model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))
initial_reading_steps = prompts_config["initial_reading_steps"]

try:
    transits_now = compute_transits()
except Exception as e:
    log.warning("Could not compute transits: %s — using placeholder", e)
    transits_now = None

updated = errors = 0
for row in rows:
    chart_id, name = row["id"], row["name"]
    try:
        chart_data = json.loads(row["chart_data"]) if isinstance(row["chart_data"], str) else row["chart_data"]
        full_chart = _strip_chart_for_ai(chart_data, extra_charts=["D2", "D7", "D10", "D20"])
        full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

        natal_lagna_sign = chart_data.get("lagna_sign") or (chart_data.get("lagna") or {}).get("sign")
        transit_str = _format_transits_for_ai(transits_now, natal_lagna_sign) if transits_now else "(transit data unavailable)"

        variables = {
            "today": datetime.now().strftime("%d-%b-%Y"),
            "chart_data": json.dumps(full_chart, indent=2),
            "transit_data": transit_str,
        }
        raw = _run_prompt_chain(model, initial_reading_steps, variables,
                                prompts_config.get("default_thinking_budget"))
        reading_data = json.loads(raw) if isinstance(raw, str) else raw

        # Only store if it looks like a real reading (has categories), never a fallback
        if not (isinstance(reading_data, dict) and reading_data.get("categories")):
            log.error("Chart %s (%s): generated payload has no categories — skipping store", chart_id, name)
            errors += 1
            continue

        cur2 = conn.cursor()
        cur2.execute("UPDATE saved_charts SET reading = %s WHERE id = %s",
                     (json.dumps(reading_data), chart_id))
        cur2.close()
        conn.commit()
        log.info("Chart %s (%s): reading stored OK", chart_id, name)
        updated += 1
    except Exception as e:
        log.error("Chart %s (%s): ERROR — %s", chart_id, name, e)
        conn.rollback()
        errors += 1

cur.close(); conn.close()
log.info("Done. updated=%d  errors=%d  (started with %d missing)", updated, errors, missing)
