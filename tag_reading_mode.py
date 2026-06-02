#!/usr/bin/env python3
"""
Tag every existing stored reading that has no _mode with _mode='expert'
(the original readings were generated in expert style). Readings that already
carry a _mode (e.g. the layman comparison regens) are left untouched.

  python3 tag_reading_mode.py
"""
import os, json
import psycopg2, psycopg2.extras

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT id, name, reading FROM saved_charts WHERE reading IS NOT NULL")
rows = cur.fetchall()
print(f"{len(rows)} chart(s) with a stored reading")

tagged = skipped = errors = 0
for r in rows:
    try:
        rd = json.loads(r["reading"]) if isinstance(r["reading"], str) else r["reading"]
        if not isinstance(rd, dict):
            skipped += 1; continue
        if rd.get("_mode"):
            print(f"  skip (already {rd['_mode']}): [{r['id']}] {r['name']}")
            skipped += 1; continue
        rd["_mode"] = "expert"
        cur2 = conn.cursor()
        cur2.execute("UPDATE saved_charts SET reading = %s WHERE id = %s", (json.dumps(rd), r["id"]))
        cur2.close(); conn.commit()
        tagged += 1
    except Exception as e:
        errors += 1; print(f"  [{r['id']}] {r['name']}: ERROR {e}"); conn.rollback()

print(f"DONE tagged_expert={tagged}  skipped(already tagged/non-dict)={skipped}  errors={errors}")
cur.close(); conn.close()
