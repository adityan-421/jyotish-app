#!/usr/bin/env python3
"""
Audit / fix Supabase Row-Level Security on public-schema tables.

  python3 secure_rls.py audit    # report connecting role, per-table RLS, API grants
  python3 secure_rls.py enable   # ALTER TABLE ... ENABLE ROW LEVEL SECURITY on all public tables

The app connects as the `postgres` role (psycopg2, server-side) which bypasses
RLS, so enabling RLS denies the anon/authenticated REST API roles while leaving
the app fully working. `enable` aborts if the connecting role would NOT bypass
RLS, to avoid breaking the app.
"""
import os, sys
import psycopg2

mode = sys.argv[1] if len(sys.argv) > 1 else "audit"

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT current_user")
who = cur.fetchone()[0]
cur.execute("SELECT rolbypassrls, rolsuper FROM pg_roles WHERE rolname = current_user")
row = cur.fetchone()
bypassrls, is_super = (row if row else (False, False))
print(f"connected as: {who}  bypassrls={bypassrls}  superuser={is_super}")

cur.execute("SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
tables = cur.fetchall()
print(f"\npublic tables ({len(tables)}):")
for t, rls in tables:
    print(f"  {'RLS ON ' if rls else 'RLS OFF'}  {t}")

if mode == "audit":
    cur.execute("""
        SELECT table_name, grantee, string_agg(DISTINCT privilege_type, ',') AS privs
        FROM information_schema.role_table_grants
        WHERE table_schema='public' AND grantee IN ('anon', 'authenticated')
        GROUP BY table_name, grantee ORDER BY table_name, grantee
    """)
    grants = cur.fetchall()
    print(f"\nanon/authenticated grants on public tables ({len(grants)}):")
    for tn, g, p in grants:
        print(f"  {tn}: {g} -> {p}")
    print("\n(audit only — no changes made)")

elif mode == "enable":
    if not (bypassrls or is_super):
        print("\nABORT: connecting role does NOT bypass RLS — enabling RLS could break the app. No changes made.")
        sys.exit(1)
    enabled = 0
    for t, rls in tables:
        if not rls:
            cur.execute(f'ALTER TABLE public."{t}" ENABLE ROW LEVEL SECURITY')
            print(f"  enabled RLS: {t}")
            enabled += 1
    print(f"\nDONE: enabled RLS on {enabled} table(s); the rest already had it.")
    print("App uses a bypass-RLS role, so direct DB access is unaffected; anon/authenticated REST access is now denied.")
else:
    print(f"unknown mode {mode!r} — use 'audit' or 'enable'")
    sys.exit(2)

cur.close(); conn.close()
