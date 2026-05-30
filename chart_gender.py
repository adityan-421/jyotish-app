#!/usr/bin/env python3
"""
Manage the `gender` field on saved charts.

Modes (passed as argv[1]):
  dump            Print every chart's id, name, and current gender (if any).
  apply           Read GENDER_MAP env (JSON: {"<chart_id>": "male"|"female", ...})
                  and write gender into BOTH input_data and chart_data for each.

Env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, and (apply) GENDER_MAP.
"""
import os, sys, json
import psycopg2, psycopg2.extras

MODE = sys.argv[1] if len(sys.argv) > 1 else "dump"

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def _load(v):
    return json.loads(v) if isinstance(v, str) else (v or {})


if MODE == "dump":
    cur.execute("SELECT id, name, input_data FROM saved_charts ORDER BY id")
    rows = cur.fetchall()
    print(f"TOTAL {len(rows)}")
    for r in rows:
        inp = _load(r["input_data"])
        g = inp.get("gender", "")
        print(f"{r['id']}\t{r['name']}\t{g}")

elif MODE == "apply":
    gmap = json.loads(os.environ["GENDER_MAP"])
    updated = 0
    for cid, gender in gmap.items():
        if gender not in ("male", "female"):
            print(f"SKIP {cid}: invalid gender {gender!r}")
            continue
        cur.execute("SELECT input_data, chart_data FROM saved_charts WHERE id = %s", (int(cid),))
        row = cur.fetchone()
        if not row:
            print(f"SKIP {cid}: not found")
            continue
        inp = _load(row["input_data"]); cd = _load(row["chart_data"])
        inp["gender"] = gender
        cd["gender"] = gender
        cur.execute(
            "UPDATE saved_charts SET input_data = %s, chart_data = %s WHERE id = %s",
            (json.dumps(inp), json.dumps(cd), int(cid)),
        )
        conn.commit()
        print(f"OK {cid}: gender={gender}")
        updated += 1
    print(f"DONE updated={updated}")

else:
    print(f"unknown mode {MODE!r}")
    sys.exit(2)

cur.close()
conn.close()
