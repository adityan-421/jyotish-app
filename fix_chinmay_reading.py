#!/usr/bin/env python3
"""One-off script: generate and save the AI reading for all Chinmay charts."""
import json, os, sys, logging, re, time
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import psycopg2, psycopg2.extras

FALLBACK_TEXT = "Unable to generate reading. Please try again."

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, user_id, name, input_data, chart_data FROM saved_charts WHERE lower(name) LIKE '%chinmay%'")
rows = cur.fetchall()

if not rows:
    logger.error("No chart found for Chinmay"); sys.exit(1)

for r in rows:
    logger.info("Found chart id=%s name=%s", r["id"], r["name"])

import vertexai
from vertexai.generative_models import GenerativeModel

vertexai.init(project=os.environ.get("GCP_PROJECT", "grahalogic"),
              location=os.environ.get("GCP_LOCATION", "us-central1"))

prompts_path = Path(__file__).parent / "prompts.json"
with open(prompts_path) as f:
    prompts_config = json.load(f)

model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

def is_fallback(data):
    """Return True if reading_data is the error fallback."""
    if not data:
        return True
    cats = data.get("categories", [])
    if cats and cats[0].get("reading", "") == FALLBACK_TEXT:
        return True
    if data.get("general", "") == FALLBACK_TEXT:
        return True
    return False

def _safe_sub(template, vars_):
    def _repl(m):
        k = m.group(1)
        v = vars_.get(k)
        if v is None: return m.group(0)
        return v if isinstance(v, str) else str(v)
    return re.sub(r'\{(\w+)\}', _repl, template)

def generate_reading(chart_data, attempt=1):
    full_chart = dict(chart_data)
    full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")
    if "dasha" in full_chart and "maha" in full_chart.get("dasha", {}):
        mahas = full_chart["dasha"]["maha"]
        today = datetime.now()
        def _parse_date(s):
            for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%Y/%m/%d"):
                try: return datetime.strptime(s, fmt)
                except (ValueError, TypeError): pass
            return today
        relevant = [m for m in mahas if _parse_date(m["end"]) >= today] if mahas else mahas
        full_chart["dasha"] = dict(full_chart["dasha"])
        full_chart["dasha"]["maha"] = relevant[:6] if len(relevant) > 6 else relevant

    variables = {"today": datetime.now().strftime("%d-%b-%Y"), "chart_data": json.dumps(full_chart, indent=2)}
    reading_data = None
    steps = prompts_config["initial_reading_steps"]
    for step in steps:
        prompt_text = _safe_sub(step["prompt"], variables)
        logger.info("  Attempt %d: Calling Gemini step=%s (%d chars)...", attempt, step.get("name","?"), len(prompt_text))
        response = model.generate_content(prompt_text)
        result_text = ""
        try:
            result_text = response.text.strip()
        except ValueError:
            try:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "text") and part.text:
                        result_text += part.text
            except Exception as e:
                logger.error("  Failed to extract parts: %s", e)
            result_text = result_text.strip()
        logger.info("  Got %d chars", len(result_text))
        if step["response_type"] == "json":
            cleaned = result_text
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                if cleaned.endswith("```"): cleaned = cleaned[:-3].strip()
            obj_start = next((i for i, ch in enumerate(cleaned) if ch in ('{','[')), -1)
            result = step.get("json_fallback", {})
            if obj_start != -1:
                try:
                    result, _ = json.JSONDecoder().raw_decode(cleaned, obj_start)
                    logger.info("  JSON OK keys=%s", list(result.keys()) if isinstance(result, dict) else type(result))
                except json.JSONDecodeError as e:
                    logger.error("  JSON fail: %s — will retry if needed", e)
                    result = step.get("json_fallback", {})
            variables[step["output_var"]] = result
            if step.get("is_final"):
                reading_data = result
    return reading_data

for row in rows:
    chart_id = row["id"]
    user_id = row["user_id"]
    chart_data = json.loads(row["chart_data"])
    logger.info("=== Processing chart id=%s name=%s ===", chart_id, row["name"])

    reading_data = None
    for attempt in range(1, 4):  # up to 3 tries
        reading_data = generate_reading(chart_data, attempt)
        if not is_fallback(reading_data):
            break
        logger.warning("  Got fallback on attempt %d — retrying in 5s...", attempt)
        time.sleep(5)

    if not is_fallback(reading_data):
        cur2 = conn.cursor()
        cur2.execute("UPDATE saved_charts SET reading=%s WHERE id=%s AND user_id=%s",
                     (json.dumps(reading_data), chart_id, user_id))
        conn.commit()
        cur2.close()
        logger.info("  Saved real reading for chart_id=%s", chart_id)
    else:
        logger.error("  All attempts failed for chart_id=%s — NOT saving fallback", chart_id)

cur.close()
conn.close()
logger.info("All done.")
