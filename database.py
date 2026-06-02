"""PostgreSQL database layer for users and saved charts (Supabase)."""

import json
import os
import logging
from contextlib import contextmanager
from datetime import date

import psycopg2
import psycopg2.extras
import psycopg2.pool

DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
MAX_CHARTS = 20
UNLIMITED_CHART_EMAILS = {"adityan@gmail.com", "anu.namjoshi@gmail.com"}

logger = logging.getLogger(__name__)

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=0,
            maxconn=5,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        )
    return _pool


@contextmanager
def get_db():
    pool = _get_pool()
    conn = pool.getconn()
    # Discard already-closed (stale) connections and get a fresh one
    if conn.closed:
        pool.putconn(conn, close=True)
        conn = pool.getconn()
    ok = False
    try:
        yield conn
        ok = True
    finally:
        # Return healthy connections to pool; close broken ones so they aren't reused
        pool.putconn(conn, close=not ok)


def reset_pool():
    """Discard the connection pool so it is recreated on next use."""
    global _pool, _db_initialized
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
        _pool = None
    _db_initialized = False


_db_initialized = False


def init_db():
    """Create tables if they don't exist. Safe to call multiple times."""
    global _db_initialized
    if _db_initialized:
        return
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL,
                    name TEXT,
                    picture TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS saved_charts (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    name TEXT NOT NULL,
                    input_data TEXT NOT NULL,
                    chart_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("ALTER TABLE saved_charts ADD COLUMN IF NOT EXISTS reading TEXT DEFAULT NULL")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS own_chart_id INTEGER DEFAULT NULL")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS reading_mode TEXT DEFAULT 'expert'")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ai_questions (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    question TEXT NOT NULL,
                    category TEXT NOT NULL,
                    reading TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("ALTER TABLE ai_questions ADD COLUMN IF NOT EXISTS cost_usd DOUBLE PRECISION DEFAULT 0")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pending_readings (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    chart_id INTEGER,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    batch_name TEXT,
                    batch_index INTEGER,
                    reading_data TEXT,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pending_readings_status
                ON pending_readings(status);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_predictions (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    type TEXT NOT NULL,
                    period_start DATE NOT NULL,
                    prediction_text TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    batch_name TEXT,
                    batch_index INTEGER,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_predictions_unique
                ON user_predictions(user_id, type, period_start);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_predictions_status
                ON user_predictions(status);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_push_tokens (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, token)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_push_tokens_user_id
                ON user_push_tokens(user_id);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gem_usage (
                    user_id TEXT NOT NULL REFERENCES users(id),
                    month_start DATE NOT NULL,
                    used INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, month_start)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                INSERT INTO app_settings (key, value) VALUES ('weekly_email_enabled', 'false')
                ON CONFLICT (key) DO NOTHING;
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weekly_emails (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    week_start DATE NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    batch_name TEXT,
                    batch_index INTEGER,
                    ai_content TEXT,
                    sent_at TIMESTAMP,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, week_start)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_weekly_emails_status
                ON weekly_emails(status);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS email_unsubscribes (
                    user_id TEXT PRIMARY KEY REFERENCES users(id),
                    unsubscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS matchmaking_readings (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    chart_id_1 INTEGER NOT NULL,
                    chart_id_2 INTEGER NOT NULL,
                    chart_name_1 TEXT NOT NULL,
                    chart_name_2 TEXT NOT NULL,
                    score INTEGER,
                    result_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, chart_id_1, chart_id_2)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_matchmaking_user
                ON matchmaking_readings(user_id, created_at DESC);
            """)
            conn.commit()
            cur.close()
        _db_initialized = True
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.warning("init_db failed (will retry on first request): %s", e)


def upsert_user(user_id, email, name, picture):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO users (id, email, name, picture)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT(id) DO UPDATE SET email=EXCLUDED.email, name=EXCLUDED.name, picture=EXCLUDED.picture""",
            (user_id, email, name, picture),
        )
        conn.commit()
        cur.close()


def count_charts(user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT COUNT(*) as cnt FROM saved_charts WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
        return row["cnt"]


def save_chart(user_id, name, input_data, chart_data, user_email=None, reading=None):
    if user_email not in UNLIMITED_CHART_EMAILS and count_charts(user_id) >= MAX_CHARTS:
        return None, f"Limit reached: you can save up to {MAX_CHARTS} charts."
    # Only persist a reading that looks real (has categories), never a fallback/empty payload.
    reading_json = json.dumps(reading) if isinstance(reading, dict) and reading.get("categories") else None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO saved_charts (user_id, name, input_data, chart_data, reading) VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (user_id, name, json.dumps(input_data), json.dumps(chart_data), reading_json),
        )
        chart_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return chart_id, None


def update_chart(chart_id, user_id, input_data, chart_data):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE saved_charts SET input_data=%s, chart_data=%s, reading=NULL WHERE id=%s AND user_id=%s",
            (json.dumps(input_data), json.dumps(chart_data), chart_id, user_id),
        )
        updated = cur.rowcount
        conn.commit()
        cur.close()
        return updated > 0


def get_charts(user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT own_chart_id FROM users WHERE id = %s", (user_id,))
        user_row = cur.fetchone()
        own_id = user_row["own_chart_id"] if user_row else None
        cur.execute(
            """SELECT id, name, input_data, created_at FROM saved_charts
               WHERE user_id = %s ORDER BY created_at DESC""",
            (user_id,),
        )
        rows = cur.fetchall()
        cur.close()
    results = []
    for r in rows:
        inp = json.loads(r["input_data"])
        results.append({
            "id": r["id"],
            "name": r["name"],
            "place": inp.get("place", ""),
            "date": f"{inp.get('year')}-{inp.get('month', ''):02d}-{inp.get('day', ''):02d}" if inp.get("year") else "",
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "is_own_chart": r["id"] == own_id,
        })
    return results


def get_chart(chart_id, user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT own_chart_id FROM users WHERE id = %s", (user_id,))
        user_row = cur.fetchone()
        own_id = user_row["own_chart_id"] if user_row else None
        cur.execute(
            "SELECT * FROM saved_charts WHERE id = %s AND user_id = %s",
            (chart_id, user_id),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return None
    reading_raw = row.get("reading")
    reading = json.loads(reading_raw) if reading_raw else None
    return {
        "id": row["id"],
        "name": row["name"],
        "input_data": json.loads(row["input_data"]),
        "chart_data": json.loads(row["chart_data"]),
        "reading": reading,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "is_own_chart": row["id"] == own_id,
    }


def delete_chart(chart_id, user_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM saved_charts WHERE id = %s AND user_id = %s", (chart_id, user_id))
        deleted = cur.rowcount
        if deleted:
            # Clear own_chart_id if this was the user's own chart
            cur.execute(
                "UPDATE users SET own_chart_id = NULL WHERE id = %s AND own_chart_id = %s",
                (user_id, chart_id),
            )
        conn.commit()
        cur.close()
        return deleted > 0


def update_chart_reading(chart_id, user_id, reading):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE saved_charts SET reading=%s WHERE id=%s AND user_id=%s",
            (json.dumps(reading), chart_id, user_id),
        )
        conn.commit()
        cur.close()


def set_own_chart(user_id, chart_id):
    """Set a chart as the user's own birth chart. Pass None to clear."""
    with get_db() as conn:
        cur = conn.cursor()
        if chart_id is not None:
            # Verify chart belongs to user
            cur.execute("SELECT id FROM saved_charts WHERE id = %s AND user_id = %s", (chart_id, user_id))
            if not cur.fetchone():
                cur.close()
                return False
        cur.execute("UPDATE users SET own_chart_id = %s WHERE id = %s", (chart_id, user_id))
        conn.commit()
        cur.close()
        return True


def get_own_chart_id(user_id):
    """Return the user's own chart id, or None."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT own_chart_id FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
    return row["own_chart_id"] if row else None


def get_reading_mode(user_id):
    """Return the user's AI reading mode ('expert' or 'layman'); default 'expert'."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT reading_mode FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
        cur.close()
    mode = (row["reading_mode"] if row else None) or "expert"
    return mode if mode in ("expert", "layman") else "expert"


def set_reading_mode(user_id, mode):
    """Persist the user's AI reading mode. Ignores invalid values."""
    if mode not in ("expert", "layman"):
        return False
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET reading_mode = %s WHERE id = %s", (mode, user_id))
        conn.commit()
        cur.close()
    return True


def get_question_count_today(user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        today = date.today().isoformat()
        cur.execute(
            "SELECT COUNT(*) as cnt FROM ai_questions WHERE user_id = %s AND date(created_at) = %s",
            (user_id, today),
        )
        row = cur.fetchone()
        cur.close()
        return row["cnt"]


def save_ai_question(user_id, question, category, reading, cost_usd=0.0):
    """Store only telemetry (user_id + category + timestamp + est. cost).
    Question text and reading are not retained."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ai_questions (user_id, question, category, reading, cost_usd) VALUES (%s, %s, %s, %s, %s)",
            (user_id, "", category, "", float(cost_usd or 0.0)),
        )
        conn.commit()
        cur.close()


def get_all_charts_for_backfill():
    """Return all saved charts (id, user_id, input_data) for recomputation."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, user_id, input_data FROM saved_charts ORDER BY id")
        rows = cur.fetchall()
        cur.close()
    return [
        {"id": r["id"], "user_id": r["user_id"], "input_data": json.loads(r["input_data"])}
        for r in rows
    ]


def bulk_update_chart_data(chart_id, chart_data):
    """Update only chart_data for a given chart (no reading reset)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE saved_charts SET chart_data=%s WHERE id=%s",
            (json.dumps(chart_data), chart_id),
        )
        conn.commit()
        cur.close()


def get_stats():
    """Return aggregate stats: total users, total charts, charts per user."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT COUNT(*) as cnt FROM users")
        total_users = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) as cnt FROM saved_charts")
        total_charts = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(DISTINCT user_id) as cnt FROM saved_charts")
        users_with_charts = cur.fetchone()["cnt"]
        cur.execute("""
            SELECT u.name, u.email, COUNT(sc.id) as chart_count
            FROM users u
            LEFT JOIN saved_charts sc ON u.id = sc.user_id
            GROUP BY u.id, u.name, u.email
            ORDER BY chart_count DESC
        """)
        per_user = [{"name": r["name"], "email": r["email"], "chart_count": r["chart_count"]} for r in cur.fetchall()]
        cur.close()
    return {
        "total_users": total_users,
        "total_charts": total_charts,
        "users_with_charts": users_with_charts,
        "per_user": per_user,
    }


def get_admin_stats():
    """Full admin stats: per-user breakdown of charts, AI queries, and comparisons."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Per-user rollup
        cur.execute("""
            SELECT
                u.id, u.name, u.email, u.created_at,
                COUNT(DISTINCT sc.id) AS chart_count,
                COUNT(DISTINCT CASE WHEN aq.category != 'compatibility' THEN aq.id END) AS ai_count,
                COUNT(DISTINCT CASE WHEN aq.category = 'compatibility'  THEN aq.id END) AS compare_count,
                GREATEST(1.0, EXTRACT(EPOCH FROM (NOW() - u.created_at)) / 2592000.0) AS months_active
            FROM users u
            LEFT JOIN saved_charts sc ON sc.user_id = u.id
            LEFT JOIN ai_questions aq ON aq.user_id = u.id
            GROUP BY u.id, u.name, u.email, u.created_at
            ORDER BY u.created_at
        """)
        rows = cur.fetchall()

        # Per-user AI cost — aggregated separately so the charts×questions join
        # above doesn't multiply the summed cost.
        cur.execute("SELECT user_id, COALESCE(SUM(cost_usd), 0) AS cost FROM ai_questions GROUP BY user_id")
        cost_by_user = {r["user_id"]: float(r["cost"]) for r in cur.fetchall()}

        users = []
        for r in rows:
            m = float(r["months_active"])
            users.append({
                "name":               r["name"] or "",
                "email":              r["email"],
                "joined":             r["created_at"].strftime("%d %b %Y") if r["created_at"] else "—",
                "charts":             int(r["chart_count"]),
                "ai_queries":         int(r["ai_count"]),
                "ai_per_month":       round(r["ai_count"] / m, 1),
                "compares":           int(r["compare_count"]),
                "compares_per_month": round(r["compare_count"] / m, 1),
                "ai_cost":            round(cost_by_user.get(r["id"], 0.0), 4),
            })

        # Totals
        cur.execute("SELECT COUNT(*) AS cnt FROM users")
        total_users = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM saved_charts")
        total_charts = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM ai_questions WHERE category != 'compatibility'")
        total_queries = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM ai_questions WHERE category = 'compatibility'")
        total_compares = cur.fetchone()["cnt"]
        cur.execute("SELECT COALESCE(SUM(cost_usd), 0) AS total FROM ai_questions")
        total_cost = float(cur.fetchone()["total"])

        # Monthly trend — queries per calendar month (last 6 months)
        cur.execute("""
            SELECT TO_CHAR(DATE_TRUNC('month', created_at), 'Mon YYYY') AS month,
                   COUNT(*) FILTER (WHERE category != 'compatibility') AS queries,
                   COUNT(*) FILTER (WHERE category  = 'compatibility') AS compares
            FROM ai_questions
            WHERE created_at >= NOW() - INTERVAL '6 months'
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY DATE_TRUNC('month', created_at)
        """)
        monthly = [dict(r) for r in cur.fetchall()]

        cur.close()

    return {
        "users":          users,
        "total_users":    total_users,
        "total_charts":   total_charts,
        "total_queries":  total_queries,
        "total_compares": total_compares,
        "total_cost":     round(total_cost, 4),
        "monthly":        monthly,
    }


def create_pending_reading(reading_id, user_id, chart_id, prompt):
    """Insert a new pending reading request."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO pending_readings (id, user_id, chart_id, prompt, status)
               VALUES (%s, %s, %s, %s, 'pending')""",
            (reading_id, user_id, chart_id, prompt),
        )
        conn.commit()
        cur.close()
    return reading_id


def get_pending_readings_by_status(status):
    """Return all pending readings with the given status."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT * FROM pending_readings WHERE status = %s ORDER BY created_at",
            (status,),
        )
        rows = cur.fetchall()
        cur.close()
    return rows


def mark_readings_submitted(reading_ids, batch_name):
    """Update readings to submitted status with batch info."""
    with get_db() as conn:
        cur = conn.cursor()
        for idx, rid in enumerate(reading_ids):
            cur.execute(
                """UPDATE pending_readings
                   SET status='submitted', batch_name=%s, batch_index=%s, updated_at=CURRENT_TIMESTAMP
                   WHERE id=%s""",
                (batch_name, idx, rid),
            )
        conn.commit()
        cur.close()


def complete_reading(reading_id, reading_data_json):
    """Mark a reading as completed with its result."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """UPDATE pending_readings
               SET status='completed', reading_data=%s, updated_at=CURRENT_TIMESTAMP
               WHERE id=%s""",
            (reading_data_json, reading_id),
        )
        conn.commit()
        cur.close()


def fail_reading(reading_id, error_msg):
    """Mark a reading as failed."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """UPDATE pending_readings
               SET status='failed', error=%s, updated_at=CURRENT_TIMESTAMP
               WHERE id=%s""",
            (error_msg, reading_id),
        )
        conn.commit()
        cur.close()


def get_reading_status(reading_id):
    """Return status info for a pending reading."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, user_id, chart_id, status, reading_data, error, created_at FROM pending_readings WHERE id = %s",
            (reading_id,),
        )
        row = cur.fetchone()
        cur.close()
    return row


def get_cached_value(key, max_age_days=7):
    """Return cached value if it exists and is fresher than max_age_days, else None."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT value, created_at FROM app_cache WHERE key = %s",
            (key,),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return None
    from datetime import datetime, timezone
    age = datetime.now(timezone.utc) - row["created_at"].replace(tzinfo=timezone.utc)
    if age.total_seconds() > max_age_days * 86400:
        return None
    return json.loads(row["value"])


def set_cached_value(key, value):
    """Upsert a value into app_cache, resetting created_at."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO app_cache (key, value, created_at)
               VALUES (%s, %s, CURRENT_TIMESTAMP)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = CURRENT_TIMESTAMP""",
            (key, json.dumps(value)),
        )
        conn.commit()
        cur.close()


def get_ai_history(user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT id, question, category, reading, created_at FROM ai_questions
               WHERE user_id = %s AND created_at >= NOW() - INTERVAL '30 days'
               ORDER BY created_at DESC""",
            (user_id,),
        )
        rows = cur.fetchall()
        cur.close()
    return [
        {
            "id": r["id"],
            "question": r["question"],
            "category": r["category"],
            "reading": r["reading"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


# ── User Predictions ──────────────────────────────────────────────────────

def get_users_with_own_chart():
    """Return list of {user_id, chart_id, chart_data} for all users with own chart set."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT sc.id as chart_id, sc.user_id, sc.chart_data
            FROM saved_charts sc
            JOIN users u ON sc.id = u.own_chart_id AND sc.user_id = u.id
            WHERE u.own_chart_id IS NOT NULL
        """)
        rows = cur.fetchall()
        cur.close()
    return [
        {"user_id": r["user_id"], "chart_id": r["chart_id"], "chart_data": json.loads(r["chart_data"])}
        for r in rows
    ]


def insert_user_prediction(user_id, pred_type, period_start):
    """Insert a pending prediction row. Returns the new id, or None if already exists."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO user_predictions (user_id, type, period_start)
               VALUES (%s, %s, %s)
               ON CONFLICT (user_id, type, period_start) DO NOTHING
               RETURNING id""",
            (user_id, pred_type, period_start),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
    return row[0] if row else None


def get_pending_predictions():
    """Return all prediction rows with status 'pending'."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, user_id, type, period_start FROM user_predictions WHERE status = 'pending' ORDER BY id"
        )
        rows = cur.fetchall()
        cur.close()
    return rows


def mark_predictions_submitted(prediction_ids, batch_name):
    """Update predictions to submitted status with batch tracking info."""
    with get_db() as conn:
        cur = conn.cursor()
        for idx, pid in enumerate(prediction_ids):
            cur.execute(
                """UPDATE user_predictions
                   SET status='submitted', batch_name=%s, batch_index=%s
                   WHERE id=%s""",
                (batch_name, idx, pid),
            )
        conn.commit()
        cur.close()


def get_submitted_predictions():
    """Return all prediction rows with status 'submitted', including user email/name."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT p.id, p.user_id, p.type, p.period_start, p.batch_name, p.batch_index,
                      u.email, u.name
               FROM user_predictions p
               JOIN users u ON u.id = p.user_id
               WHERE p.status = 'submitted'
               ORDER BY p.id"""
        )
        rows = cur.fetchall()
        cur.close()
    return rows


def complete_prediction(prediction_id, text):
    """Mark a prediction as completed with its text."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE user_predictions SET status='completed', prediction_text=%s WHERE id=%s",
            (text, prediction_id),
        )
        conn.commit()
        cur.close()


def fail_prediction(prediction_id, error_msg):
    """Mark a prediction as failed."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE user_predictions SET status='failed', error=%s WHERE id=%s",
            (error_msg, prediction_id),
        )
        conn.commit()
        cur.close()


def get_user_predictions(user_id, week_start_str, month_start_str):
    """Return completed daily_week, weekly, and monthly predictions for a user."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT type, period_start, prediction_text
               FROM user_predictions
               WHERE user_id = %s
                 AND status = 'completed'
                 AND (
                   (type = 'daily_week' AND period_start = %s) OR
                   (type = 'weekly'     AND period_start = %s) OR
                   (type = 'monthly'    AND period_start = %s)
                 )""",
            (user_id, week_start_str, week_start_str, month_start_str),
        )
        rows = cur.fetchall()
        cur.close()
    return {r["type"]: r["prediction_text"] for r in rows}


def upsert_push_token(user_id, token):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO user_push_tokens (user_id, token)
               VALUES (%s, %s)
               ON CONFLICT (user_id, token) DO NOTHING""",
            (user_id, token),
        )
        cur.close()
        conn.commit()


def get_users_with_push_tokens_and_charts():
    """Return rows with user_id, chart_data, and their push tokens for users who have an own chart."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT u.id AS user_id, sc.chart_data,
                      array_agg(pt.token) AS push_tokens
               FROM users u
               JOIN saved_charts sc ON sc.id = u.own_chart_id
               JOIN user_push_tokens pt ON pt.user_id = u.id
               WHERE u.own_chart_id IS NOT NULL
               GROUP BY u.id, sc.chart_data"""
        )
        rows = cur.fetchall()
        cur.close()
    return rows


# ── GrahaGems ─────────────────────────────────────────────────────────────

GEMS_PER_MONTH = 20
UNLIMITED_GEM_EMAILS = {"adityan@gmail.com", "anu.namjoshi@gmail.com"}


def get_gem_balance(user_id, user_email=None):
    """Return {used, remaining, month_start, is_unlimited} for this month."""
    if user_email in UNLIMITED_GEM_EMAILS:
        return {"used": 0, "remaining": 999, "month_start": None, "is_unlimited": True}
    from datetime import date
    month_start = date.today().replace(day=1).isoformat()
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT used FROM gem_usage WHERE user_id = %s AND month_start = %s",
            (user_id, month_start),
        )
        row = cur.fetchone()
        cur.close()
    used = row["used"] if row else 0
    return {
        "used": used,
        "remaining": max(0, GEMS_PER_MONTH - used),
        "month_start": month_start,
        "is_unlimited": False,
    }


def use_gem(user_id, user_email=None, count=1):
    """Attempt to deduct `count` gems atomically. Returns True if successful, False if exhausted."""
    if user_email in UNLIMITED_GEM_EMAILS:
        return True
    from datetime import date
    month_start = date.today().replace(day=1).isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO gem_usage (user_id, month_start, used)
               VALUES (%s, %s, 0)
               ON CONFLICT (user_id, month_start) DO NOTHING""",
            (user_id, month_start),
        )
        cur.execute(
            """UPDATE gem_usage SET used = used + %s
               WHERE user_id = %s AND month_start = %s AND used + %s <= %s
               RETURNING used""",
            (count, user_id, month_start, count, GEMS_PER_MONTH),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
    return row is not None  # None means not enough gems remaining


# ── App Settings ───────────────────────────────────────────────────────────

def get_app_setting(key, default=None):
    """Return an app setting value by key, or default if not found."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
        row = cur.fetchone()
        cur.close()
    return row["value"] if row else default


def set_app_setting(key, value):
    """Upsert an app setting."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO app_settings (key, value, updated_at)
               VALUES (%s, %s, CURRENT_TIMESTAMP)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP""",
            (key, value),
        )
        conn.commit()
        cur.close()


# ── Weekly Emails ──────────────────────────────────────────────────────────

def get_users_for_weekly_email(week_start_str, test_email=None):
    """Return users with own_chart_id set, not unsubscribed, no row yet for this week.

    If test_email is given, only return that user (for test runs).
    Returns list of {user_id, email, name, chart_data}.
    """
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if test_email:
            cur.execute("""
                SELECT u.id AS user_id, u.email, u.name, sc.chart_data
                FROM users u
                JOIN saved_charts sc ON sc.id = u.own_chart_id AND sc.user_id = u.id
                WHERE u.email = %s
                  AND u.email IS NOT NULL
                  AND u.own_chart_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM email_unsubscribes eu WHERE eu.user_id = u.id)
                  AND NOT EXISTS (
                    SELECT 1 FROM weekly_emails we
                    WHERE we.user_id = u.id AND we.week_start = %s
                  )
            """, (test_email, week_start_str))
        else:
            cur.execute("""
                SELECT u.id AS user_id, u.email, u.name, sc.chart_data
                FROM users u
                JOIN saved_charts sc ON sc.id = u.own_chart_id AND sc.user_id = u.id
                WHERE u.email IS NOT NULL
                  AND u.own_chart_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM email_unsubscribes eu WHERE eu.user_id = u.id)
                  AND NOT EXISTS (
                    SELECT 1 FROM weekly_emails we
                    WHERE we.user_id = u.id AND we.week_start = %s
                  )
            """, (week_start_str,))
        rows = cur.fetchall()
        cur.close()
    return [
        {
            "user_id": r["user_id"],
            "email": r["email"],
            "name": r["name"],
            "chart_data": json.loads(r["chart_data"]) if isinstance(r["chart_data"], str) else r["chart_data"],
        }
        for r in rows
    ]


def insert_weekly_email(user_id, week_start):
    """Insert a pending weekly_email row. Returns new id or None if already exists."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO weekly_emails (user_id, week_start)
               VALUES (%s, %s)
               ON CONFLICT (user_id, week_start) DO NOTHING
               RETURNING id""",
            (user_id, week_start),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
    return row[0] if row else None


def mark_weekly_emails_submitted(id_index_pairs, batch_name):
    """Update weekly_email rows to submitted status with batch tracking info.
    id_index_pairs: list of (email_id, batch_index) tuples.
    """
    with get_db() as conn:
        cur = conn.cursor()
        for email_id, batch_index in id_index_pairs:
            cur.execute(
                """UPDATE weekly_emails
                   SET status='submitted', batch_name=%s, batch_index=%s
                   WHERE id=%s""",
                (batch_name, batch_index, email_id),
            )
        conn.commit()
        cur.close()


def get_submitted_weekly_emails():
    """Return all weekly_email rows with status='submitted'."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT id, user_id, week_start, batch_name, batch_index
               FROM weekly_emails WHERE status = 'submitted' ORDER BY id"""
        )
        rows = cur.fetchall()
        cur.close()
    return rows


def complete_weekly_email(email_id, ai_content_json):
    """Store AI result and mark as ai_ready."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE weekly_emails SET status='ai_ready', ai_content=%s WHERE id=%s",
            (ai_content_json, email_id),
        )
        conn.commit()
        cur.close()


def get_ai_ready_weekly_emails():
    """Return all weekly_email rows ready to send (ai_ready or failed-during-send)."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT we.id, we.user_id, we.week_start, we.ai_content,
                   u.email, u.name
            FROM weekly_emails we
            JOIN users u ON u.id = we.user_id
            WHERE we.status IN ('ai_ready', 'failed') AND we.ai_content IS NOT NULL
            ORDER BY we.id
        """)
        rows = cur.fetchall()
        cur.close()
    return rows


def mark_weekly_email_sent(email_id):
    """Mark a weekly_email as sent."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE weekly_emails SET status='sent', sent_at=CURRENT_TIMESTAMP WHERE id=%s",
            (email_id,),
        )
        conn.commit()
        cur.close()


def fail_weekly_email(email_id, error_msg):
    """Mark a weekly_email as failed."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE weekly_emails SET status='failed', error=%s WHERE id=%s",
            (error_msg, email_id),
        )
        conn.commit()
        cur.close()


def unsubscribe_user(user_id):
    """Add user to email_unsubscribes. Safe to call multiple times."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO email_unsubscribes (user_id)
               VALUES (%s)
               ON CONFLICT (user_id) DO NOTHING""",
            (user_id,),
        )
        conn.commit()
        cur.close()


# ── Matchmaking Readings ──────────────────────────────────────────────────────

def _canonical_chart_pair(chart_id_1, chart_id_2):
    """Return (smaller_id, larger_id) to normalize lookup regardless of order."""
    a, b = int(chart_id_1), int(chart_id_2)
    return (a, b) if a < b else (b, a)


def get_matchmaking(user_id, chart_id_1, chart_id_2):
    """Return cached matchmaking result for this chart pair, or None."""
    c1, c2 = _canonical_chart_pair(chart_id_1, chart_id_2)
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT id, chart_id_1, chart_id_2, chart_name_1, chart_name_2,
                      score, result_json, created_at
               FROM matchmaking_readings
               WHERE user_id = %s AND chart_id_1 = %s AND chart_id_2 = %s""",
            (user_id, c1, c2),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return None
    result = json.loads(row["result_json"])
    return {
        "id": row["id"],
        "chart_id_1": row["chart_id_1"],
        "chart_id_2": row["chart_id_2"],
        "chart_name_1": row["chart_name_1"],
        "chart_name_2": row["chart_name_2"],
        "score": row["score"],
        "result": result,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }


def save_matchmaking(user_id, chart_id_1, chart_id_2, name1, name2, score, result):
    """Upsert a matchmaking result. Names/score/result always reflect actual boy/girl order."""
    c1, c2 = _canonical_chart_pair(chart_id_1, chart_id_2)
    # If the canonical order swapped the charts, swap names too
    if c1 != int(chart_id_1):
        name1, name2 = name2, name1
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO matchmaking_readings
                   (user_id, chart_id_1, chart_id_2, chart_name_1, chart_name_2, score, result_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id, chart_id_1, chart_id_2)
               DO UPDATE SET chart_name_1=EXCLUDED.chart_name_1,
                             chart_name_2=EXCLUDED.chart_name_2,
                             score=EXCLUDED.score,
                             result_json=EXCLUDED.result_json,
                             created_at=CURRENT_TIMESTAMP""",
            (user_id, c1, c2, name1, name2, score, json.dumps(result)),
        )
        conn.commit()
        cur.close()


def get_matchmaking_history(user_id):
    """Return all matchmaking readings for a user, newest first."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT id, chart_id_1, chart_id_2, chart_name_1, chart_name_2,
                      score, result_json, created_at
               FROM matchmaking_readings
               WHERE user_id = %s
               ORDER BY created_at DESC""",
            (user_id,),
        )
        rows = cur.fetchall()
        cur.close()
    return [
        {
            "id": r["id"],
            "chart_id_1": r["chart_id_1"],
            "chart_id_2": r["chart_id_2"],
            "chart_name_1": r["chart_name_1"],
            "chart_name_2": r["chart_name_2"],
            "score": r["score"],
            "result": json.loads(r["result_json"]),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def delete_matchmaking(record_id, user_id):
    """Delete a matchmaking record (only if it belongs to this user)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM matchmaking_readings WHERE id = %s AND user_id = %s",
            (record_id, user_id),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        cur.close()
    return deleted
