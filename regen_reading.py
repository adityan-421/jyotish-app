#!/usr/bin/env python3
"""
Regenerate the AI reading for saved charts matching a name pattern, in a chosen
tone mode, and store it (tagged with _mode). Prints a snippet for comparison.

  python3 regen_reading.py <mode> <name_pattern>
  e.g. python3 regen_reading.py layman "Aditya Namjoshi"

Env: DB_HOST..DB_PASSWORD, GCP_PROJECT, GCP_LOCATION.
"""
import os, sys, json
from datetime import datetime

import psycopg2, psycopg2.extras
import vertexai
from vertexai.generative_models import GenerativeModel

from app import (_strip_chart_for_ai, _run_prompt_chain, _format_transits_for_ai,
                 load_prompts, TONE_INSTRUCTIONS)
from jyotish_engine import compute_transits

mode = sys.argv[1] if len(sys.argv) > 1 else "layman"
pattern = sys.argv[2] if len(sys.argv) > 2 else ""
if mode not in TONE_INSTRUCTIONS:
    print(f"unknown mode {mode!r}"); sys.exit(2)
if not pattern:
    print("usage: regen_reading.py <mode> <name_pattern>"); sys.exit(2)

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, name, chart_data FROM saved_charts WHERE name ILIKE %s ORDER BY id",
            (f"%{pattern}%",))
rows = cur.fetchall()
print(f"matched {len(rows)} chart(s) for pattern {pattern!r}; mode={mode}")

vertexai.init(project=os.environ.get("GCP_PROJECT", "grahalogic"),
              location=os.environ.get("GCP_LOCATION", "us-central1"))
prompts = load_prompts()
model = GenerativeModel(prompts.get("model", "gemini-2.5-flash"))
steps = prompts["initial_reading_steps"]
try:
    transits = compute_transits()
except Exception:
    transits = None

for row in rows:
    cid, name = row["id"], row["name"]
    try:
        cd = json.loads(row["chart_data"]) if isinstance(row["chart_data"], str) else row["chart_data"]
        full = _strip_chart_for_ai(cd, extra_charts=["D2", "D7", "D10", "D20"])
        full["current_date"] = datetime.now().strftime("%d-%b-%Y")
        natal = cd.get("lagna_sign") or (cd.get("lagna") or {}).get("sign")
        tstr = _format_transits_for_ai(transits, natal) if transits else "(transit data unavailable)"
        variables = {
            "today": datetime.now().strftime("%d-%b-%Y"),
            "chart_data": json.dumps(full, indent=2),
            "transit_data": tstr,
            "tone_instructions": TONE_INSTRUCTIONS[mode],
        }
        raw = _run_prompt_chain(model, steps, variables, prompts.get("default_thinking_budget"))
        reading = json.loads(raw) if isinstance(raw, str) else raw
        if not (isinstance(reading, dict) and reading.get("categories")):
            print(f"  [{cid}] {name}: generation produced no categories — skipped"); continue
        reading["_mode"] = mode
        cur2 = conn.cursor()
        cur2.execute("UPDATE saved_charts SET reading = %s WHERE id = %s", (json.dumps(reading), cid))
        cur2.close(); conn.commit()
        gen = (reading.get("general") or "")[:600]
        cat0 = reading.get("categories", [{}])[0]
        print(f"\n===== [{cid}] {name} — {mode.upper()} =====")
        print("GENERAL:", gen)
        print(f"\n{cat0.get('title','?')}:", (cat0.get("reading") or "")[:500])
        print("===== stored OK =====")
    except Exception as e:
        print(f"  [{cid}] {name}: ERROR — {e}"); conn.rollback()

cur.close(); conn.close()
