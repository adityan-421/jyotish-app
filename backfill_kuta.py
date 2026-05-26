#!/usr/bin/env python3
"""
Backfill kuta_profile into all existing saved_charts rows.
Reads Moon sign + nakshatra from stored chart_data, computes the
profile using the same lookup tables as the live engine, and patches
chart_data in place — no full chart recomputation needed.
"""
import json, os, logging
import psycopg2, psycopg2.extras

from jyotish_engine import (
    SIGNS, NAKSHATRAS,
    compute_kuta_profile,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"],
    connect_timeout=10,
)

cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, chart_data FROM saved_charts")
rows = cur.fetchall()
log.info("Found %d charts to process", len(rows))

updated = skipped = errors = 0

for row in rows:
    chart_id = row["id"]
    try:
        cd = json.loads(row["chart_data"])

        # Find Moon in planets list
        moon = next((p for p in cd.get("planets", []) if p["name"] == "Moon"), None)
        if not moon:
            log.warning("Chart %s: no Moon planet found — skipping", chart_id)
            skipped += 1
            continue

        # sign is 1-based in our schema; nakshatra is a name string
        moon_sign_idx = moon["sign"] - 1          # 0-based
        nak_name = moon["nakshatra"]
        if nak_name not in NAKSHATRAS:
            log.warning("Chart %s: unknown nakshatra '%s' — skipping", chart_id, nak_name)
            skipped += 1
            continue
        moon_nak_idx = NAKSHATRAS.index(nak_name)

        kuta = compute_kuta_profile(moon_sign_idx, moon_nak_idx)
        cd["kuta_profile"] = kuta

        cur2 = conn.cursor()
        cur2.execute(
            "UPDATE saved_charts SET chart_data = %s WHERE id = %s",
            (json.dumps(cd), chart_id)
        )
        cur2.close()
        conn.commit()
        log.info("Chart %s: updated — Moon in %s / %s → gana=%s nadi=%s yoni=%s varna=%s",
                 chart_id, kuta["moon_sign"], kuta["moon_nakshatra"],
                 kuta["gana"], kuta["nadi"], kuta["yoni"], kuta["varna"])
        updated += 1

    except Exception as e:
        log.error("Chart %s: ERROR — %s", chart_id, e)
        conn.rollback()
        errors += 1

cur.close()
conn.close()
log.info("Done. updated=%d skipped=%d errors=%d", updated, skipped, errors)
