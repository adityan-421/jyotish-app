#!/usr/bin/env python3
"""One-off script: generate and save the AI reading for Ketan Jog."""
import json, os, sys, logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DB ────────────────────────────────────────────────────────────────────────
import psycopg2, psycopg2.extras, psycopg2.pool

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, user_id, name, input_data, chart_data, reading FROM saved_charts WHERE lower(name) LIKE '%ketan%'")
rows = cur.fetchall()

if not rows:
    logger.error("No chart found for Ketan Jog"); sys.exit(1)

for r in rows:
    logger.info("Found chart id=%s name=%s has_reading=%s", r["id"], r["name"], r["reading"] is not None)

row = rows[0]
chart_id = row["id"]
user_id = row["user_id"]
chart_data = json.loads(row["chart_data"])
logger.info("Using chart: %s (id=%s)", row["name"], chart_id)

# ── Vertex AI ─────────────────────────────────────────────────────────────────
import vertexai
from vertexai.generative_models import GenerativeModel

vertexai.init(project=os.environ.get("GCP_PROJECT", "grahalogic"),
              location=os.environ.get("GCP_LOCATION", "us-central1"))

prompts_path = Path(__file__).parent / "prompts.json"
with open(prompts_path) as f:
    prompts_config = json.load(f)

model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

# ── Build chart payload ───────────────────────────────────────────────────────
full_chart = dict(chart_data)
full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

# Trim dasha to relevant periods only
if "dasha" in full_chart and "maha" in full_chart.get("dasha", {}):
    mahas = full_chart["dasha"]["maha"]
    today = datetime.now()
    relevant = [m for m in mahas
                if datetime.strptime(m["end"], "%Y-%m-%d") >= today] if mahas else mahas
    full_chart["dasha"] = dict(full_chart["dasha"])
    full_chart["dasha"]["maha"] = relevant[:6] if len(relevant) > 6 else relevant

variables = {
    "today": datetime.now().strftime("%d-%b-%Y"),
    "chart_data": json.dumps(full_chart, indent=2),
}

# ── Run prompt chain ──────────────────────────────────────────────────────────
steps = prompts_config["initial_reading_steps"]
for step in steps:
    prompt_text = variables["chart_data"]  # simple substitution
    # Apply template substitution
    import string
    class SafeDict(dict):
        def __missing__(self, key): return "{" + key + "}"
    prompt_text = step["prompt"].format_map(SafeDict(variables))

    logger.info("Calling Gemini (%s chars prompt)...", len(prompt_text))
    response = model.generate_content(prompt_text)

    try:
        result_text = response.text.strip()
    except ValueError:
        logger.warning("response.text raised ValueError — extracting from candidates")
        result_text = ""
        try:
            for part in response.candidates[0].content.parts:
                if hasattr(part, "text") and part.text:
                    result_text += part.text
        except Exception as e:
            logger.error("Failed to extract from candidates: %s", e)
        result_text = result_text.strip()

    logger.info("Got response (%d chars)", len(result_text))

    if step["response_type"] == "json":
        cleaned = result_text
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
        try:
            result = json.loads(cleaned)
            logger.info("JSON parsed successfully, keys: %s", list(result.keys()) if isinstance(result, dict) else type(result))
        except json.JSONDecodeError as e:
            logger.error("JSON parse failed: %s", e)
            logger.error("Raw text (first 500): %s", result_text[:500])
            result = step.get("json_fallback", {})

        variables[step["output_var"]] = result
        if step.get("is_final"):
            reading_data = result

# ── Save to DB ────────────────────────────────────────────────────────────────
if reading_data and reading_data != steps[0].get("json_fallback", {}):
    logger.info("Saving reading to DB for chart_id=%s user_id=%s", chart_id, user_id)
    cur2 = conn.cursor()
    cur2.execute(
        "UPDATE saved_charts SET reading=%s WHERE id=%s AND user_id=%s",
        (json.dumps(reading_data), chart_id, user_id)
    )
    conn.commit()
    cur2.close()
    logger.info("Done! Reading saved successfully.")
else:
    logger.error("Reading generation failed — got fallback. Not saving.")
    sys.exit(1)

cur.close()
conn.close()
