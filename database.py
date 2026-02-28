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
        })
    return results


def get_chart(chart_id, user_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
    }


def delete_chart(chart_id, user_id):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM saved_charts WHERE id = %s AND user_id = %s", (chart_id, user_id))
        deleted = cur.rowcount
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
