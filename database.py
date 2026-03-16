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


def save_chart(user_id, name, input_data, chart_data):
    if count_charts(user_id) >= MAX_CHARTS:
        return None, f"Limit reached: you can save up to {MAX_CHARTS} charts."
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO saved_charts (user_id, name, input_data, chart_data) VALUES (%s, %s, %s, %s) RETURNING id",
            (user_id, name, json.dumps(input_data), json.dumps(chart_data)),
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
            "is_own": r["id"] == own_id,
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
        "is_own": row["id"] == own_id,
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


def save_ai_question(user_id, question, category, reading):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ai_questions (user_id, question, category, reading) VALUES (%s, %s, %s, %s)",
            (user_id, question, category, reading),
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
