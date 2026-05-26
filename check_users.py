import psycopg2, psycopg2.extras, os

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("SELECT COUNT(*) AS total_users, MIN(created_at) AS first_signup, MAX(created_at) AS latest_signup FROM users")
row = cur.fetchone()
print("=== User Summary ===")
for k, v in row.items():
    print(f"  {k}: {v}")

print("\n=== All Users ===")
cur.execute("SELECT id, email, name, created_at FROM users ORDER BY created_at DESC")
for r in cur.fetchall():
    print(f"  [{r['id']}] {r['name']} | {r['email']} | joined: {r['created_at']}")

print("\n=== Saved Charts per User ===")
cur.execute("""
    SELECT u.name, u.email, COUNT(sc.id) AS chart_count
    FROM users u
    LEFT JOIN saved_charts sc ON sc.user_id = u.id
    GROUP BY u.id, u.name, u.email
    ORDER BY chart_count DESC
""")
for r in cur.fetchall():
    print(f"  {r['name']} | {r['email']} | charts: {r['chart_count']}")

cur.close(); conn.close()
