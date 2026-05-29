#!/usr/bin/env python3
"""
Regenerate comprehensive AI readings for all saved_charts that have an existing reading.
Uses the latest initial_reading_steps prompt from prompts.json.
Run as a Cloud Run Job after any prompt update.
"""
import json, os, logging
from datetime import datetime

import psycopg2, psycopg2.extras
import vertexai
from vertexai.generative_models import GenerativeModel

# Import helpers from app (safe — no code runs at module import level)
from app import (
    _strip_chart_for_ai,
    _run_prompt_chain,
    _format_transits_for_ai,
    load_prompts,
)
from jyotish_engine import compute_transits

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Only charts that already have a reading stored
cur.execute("""
    SELECT sc.id, sc.user_id, sc.name, sc.chart_data
    FROM saved_charts sc
    WHERE sc.reading IS NOT NULL
    ORDER BY sc.id
""")
rows = cur.fetchall()
log.info("Found %d charts with existing readings to regenerate", len(rows))

# ── Vertex AI ─────────────────────────────────────────────────────────────────
vertexai.init(
    project=os.environ.get("GCP_PROJECT", "grahalogic"),
    location=os.environ.get("GCP_LOCATION", "us-central1"),
)
prompts_config = load_prompts()
model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))
initial_reading_steps = prompts_config["initial_reading_steps"]

# ── Transits (computed once — same for all charts) ────────────────────────────
try:
    transits_now = compute_transits()
except Exception as e:
    log.warning("Could not compute transits: %s — will use placeholder", e)
    transits_now = None

updated = skipped = errors = 0

for row in rows:
    chart_id = row["id"]
    name = row["name"]
    try:
        chart_data = json.loads(row["chart_data"]) if isinstance(row["chart_data"], str) else row["chart_data"]

        # Build stripped chart (same as initial reading in api_ask)
        full_chart = _strip_chart_for_ai(chart_data, extra_charts=["D2", "D7", "D10"])
        full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

        # Format transits relative to this chart's lagna
        natal_lagna_sign = chart_data.get("lagna_sign") or (chart_data.get("lagna") or {}).get("sign")
        if transits_now:
            transit_str = _format_transits_for_ai(transits_now, natal_lagna_sign)
        else:
            transit_str = "(transit data unavailable)"

        variables = {
            "today": datetime.now().strftime("%d-%b-%Y"),
            "chart_data": json.dumps(full_chart, indent=2),
            "transit_data": transit_str,
        }

        raw = _run_prompt_chain(
            model, initial_reading_steps, variables,
            prompts_config.get("default_thinking_budget"),
        )
        reading_data = json.loads(raw) if isinstance(raw, str) else raw

        cur2 = conn.cursor()
        cur2.execute(
            "UPDATE saved_charts SET reading = %s WHERE id = %s",
            (json.dumps(reading_data), chart_id),
        )
        cur2.close()
        conn.commit()

        log.info("Chart %s (%s): reading regenerated OK", chart_id, name)
        updated += 1

    except Exception as e:
        log.error("Chart %s (%s): ERROR — %s", chart_id, name, e)
        conn.rollback()
        errors += 1

cur.close()
conn.close()
log.info("Done. updated=%d  skipped=%d  errors=%d", updated, skipped, errors)
