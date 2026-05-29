#!/usr/bin/env python3
"""
Clear cached AI reading for charts matching a name pattern.
Usage:
  python3 clear_reading_cache.py anant        # clears any chart with 'anant' in name
  python3 clear_reading_cache.py --all        # clears ALL cached readings
"""
import os, sys, psycopg2, psycopg2.extras

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

if "--all" in sys.argv:
    cur.execute("UPDATE saved_charts SET reading = NULL WHERE reading IS NOT NULL RETURNING id, name")
    rows = cur.fetchall()
    print(f"Cleared reading for {len(rows)} charts")
    for r in rows:
        print(f"  ID {r['id']}: {r['name']}")
else:
    pattern = sys.argv[1] if len(sys.argv) > 1 else ""
    if not pattern:
        print("Usage: clear_reading_cache.py <name_pattern> | --all")
        sys.exit(1)
    cur.execute(
        "UPDATE saved_charts SET reading = NULL WHERE name ILIKE %s AND reading IS NOT NULL RETURNING id, name",
        (f"%{pattern}%",)
    )
    rows = cur.fetchall()
    print(f"Cleared reading for {len(rows)} charts matching '{pattern}'")
    for r in rows:
        print(f"  ID {r['id']}: {r['name']}")

conn.commit()
cur.close()
conn.close()
