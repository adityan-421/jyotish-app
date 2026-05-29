#!/usr/bin/env python3
"""
Send activation emails to registered users who have NOT saved their own chart.

Usage:
  python3 send_activation_emails.py --test    # send only to first eligible user
  python3 send_activation_emails.py           # send to ALL users without own chart
"""
import json, os, sys, logging
from datetime import datetime

import psycopg2, psycopg2.extras
import sendgrid
from sendgrid.helpers.mail import Mail
from jinja2 import Environment, FileSystemLoader
from itsdangerous import URLSafeTimedSerializer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

TEST_MODE = "--test" in sys.argv
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://jyotish-app-333157384151.us-central1.run.app")

_serializer = URLSafeTimedSerializer(os.environ["FLASK_SECRET_KEY"])
jinja_env = Environment(loader=FileSystemLoader("templates"), autoescape=False)

# ── DB ────────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Users who have signed in (have an email) but have NOT set an own chart
# Also exclude anyone who has unsubscribed
query = """
    SELECT u.id AS user_id, u.email, u.name
    FROM users u
    WHERE u.email IS NOT NULL
      AND (u.own_chart_id IS NULL)
      AND NOT EXISTS (
          SELECT 1 FROM email_unsubscribes eu WHERE eu.user_id = u.id
      )
    ORDER BY u.id
"""
if TEST_MODE:
    # Send only to admin (adityan@gmail.com) regardless of own_chart_id status
    query = """
        SELECT u.id AS user_id, u.email, u.name
        FROM users u
        WHERE u.email = 'adityan@gmail.com'
        LIMIT 1
    """
    log.info("TEST MODE — sending sample to adityan@gmail.com")

cur.execute(query)
rows = cur.fetchall()
log.info("Found %d users to email", len(rows))

def _send_activation_email(to_email, name):
    unsub_token = _serializer.dumps(to_email, salt="email-unsub")
    unsub_url = f"{APP_BASE_URL}/unsubscribe?token={unsub_token}"

    template = jinja_env.get_template("activation_email.html")
    html_body = template.render(
        name=name or "there",
        unsubscribe_url=unsub_url,
    )

    msg = Mail(
        from_email=("noreply@grahalogic.ai", "GrahaLogic"),
        to_emails=to_email,
        subject="Your Jyotish Reading Awaits — GrahaLogic",
        html_content=html_body,
    )
    sg = sendgrid.SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    resp = sg.send(msg)
    return resp.status_code < 300

sent = skipped = errors = 0

for row in rows:
    try:
        ok = _send_activation_email(row["email"], row["name"])
        if ok:
            log.info("Sent to %s (%s)", row["email"], row["name"] or "—")
            sent += 1
        else:
            log.error("SendGrid rejected for %s", row["email"])
            errors += 1
    except Exception as e:
        log.error("Error for %s: %s", row["email"], e)
        errors += 1

cur.close()
conn.close()
log.info("Done. sent=%d  skipped=%d  errors=%d", sent, skipped, errors)
