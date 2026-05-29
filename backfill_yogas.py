#!/usr/bin/env python3
"""
Backfill yogas for all existing saved_charts.
Recomputes yogas (including new Parivartana + Graha Yuddha) from stored birth data.
"""
import json, os, logging
from datetime import datetime, timezone
import psycopg2, psycopg2.extras

from jyotish_engine import compute_chart

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, name, input_data, chart_data FROM saved_charts ORDER BY id")
rows = cur.fetchall()
log.info("Found %d charts to process", len(rows))

updated = skipped = errors = 0

for row in rows:
    chart_id = row["id"]
    try:
        inp = json.loads(row["input_data"])
        cd  = json.loads(row["chart_data"])

        year  = inp.get("year")  or inp.get("birth_year")
        month = inp.get("month") or inp.get("birth_month")
        day   = inp.get("day")   or inp.get("birth_day")
        hour  = inp.get("hour",  inp.get("birth_hour",  12))
        minute= inp.get("minute",inp.get("birth_minute", 0))
        lat   = inp.get("lat")
        lon   = inp.get("lon")
        tz    = inp.get("tz",    inp.get("tz_offset", 0))
        place = inp.get("place", inp.get("city", ""))

        if not all([year, month, day, lat is not None, lon is not None]):
            log.warning("Chart %s (%s): missing birth fields — skipping", chart_id, row["name"])
            skipped += 1
            continue

        new_chart = compute_chart(int(year), int(month), int(day),
                                  int(hour), int(minute),
                                  float(lat), float(lon), float(tz), place)
        new_yogas = new_chart.get("yogas", [])
        cd["yogas"] = new_yogas

        cur2 = conn.cursor()
        cur2.execute(
            "UPDATE saved_charts SET chart_data = %s WHERE id = %s",
            (json.dumps(cd), chart_id),
        )
        cur2.close()
        conn.commit()

        parivartan = [y for y in new_yogas if "Parivartana" in y["name"]]
        yuddha     = [y for y in new_yogas if "Yuddha" in y["name"]]
        log.info("Chart %s (%s): %d yogas total, %d parivartana, %d yuddha",
                 chart_id, row["name"], len(new_yogas), len(parivartan), len(yuddha))
        updated += 1

    except Exception as e:
        log.error("Chart %s (%s): ERROR — %s", chart_id, row["name"], e)
        conn.rollback()
        errors += 1

cur.close()
conn.close()
log.info("Done. updated=%d  skipped=%d  errors=%d", updated, skipped, errors)
