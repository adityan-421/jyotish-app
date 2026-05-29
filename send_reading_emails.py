#!/usr/bin/env python3
"""
Send personalized Jyotish reading emails to users whose own chart has a stored reading.

Usage:
  python3 send_reading_emails.py --test      # send only to admin (adityanamjoshi@gmail.com)
  python3 send_reading_emails.py             # send to ALL eligible users
"""
import json, os, sys, logging, re, markdown
from datetime import datetime

import psycopg2, psycopg2.extras
import sendgrid
from sendgrid.helpers.mail import Mail
from jinja2 import Environment, FileSystemLoader
from itsdangerous import URLSafeTimedSerializer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

TEST_MODE  = "--test"  in sys.argv
COUNT_ONLY = "--count" in sys.argv
ADMIN_EMAIL = "adityanamjoshi@gmail.com"
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://jyotish-app-333157384151.us-central1.run.app")

_serializer = URLSafeTimedSerializer(os.environ["FLASK_SECRET_KEY"])

# ── Jinja2 for email template ──────────────────────────────────────────────────
jinja_env = Environment(loader=FileSystemLoader("templates"), autoescape=False)

# ── DB ────────────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=os.environ.get("DB_PORT", "5432"),
    dbname=os.environ.get("DB_NAME", "postgres"),
    user=os.environ.get("DB_USER", "postgres"),
    password=os.environ["DB_PASSWORD"], connect_timeout=10,
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Fetch users with an own chart that has a reading, who have an email, not unsubscribed
query = """
    SELECT u.id AS user_id, u.email, u.name,
           sc.id AS chart_id, sc.name AS chart_name, sc.reading
    FROM users u
    JOIN saved_charts sc ON sc.id = u.own_chart_id
    WHERE u.email IS NOT NULL
      AND sc.reading IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM email_unsubscribes eu WHERE eu.user_id = u.id
      )
    ORDER BY sc.id
"""
if TEST_MODE:
    query += " LIMIT 1"
    log.info("TEST MODE — sending only to the first eligible user")

cur.execute(query)
rows = cur.fetchall()
log.info("Found %d users to email", len(rows))

if COUNT_ONLY:
    log.info("COUNT ONLY mode — exiting without sending")
    cur.close()
    conn.close()
    sys.exit(0)

# ── Helpers ───────────────────────────────────────────────────────────────────
def md_to_html(text):
    """Convert markdown to HTML, stripping outer <p> for inline use."""
    if not text:
        return ""
    return markdown.markdown(str(text), extensions=["nl2br"])

def _send_email(to_email, name, chart_name, reading):
    unsub_token = _serializer.dumps(to_email, salt="email-unsub")
    unsub_url = f"{APP_BASE_URL}/unsubscribe?token={unsub_token}"

    categories = []
    for cat in (reading.get("categories") or []):
        categories.append({
            "icon": cat.get("icon", ""),
            "title": cat.get("title", ""),
            "reading_html": md_to_html(cat.get("reading", "")),
        })

    template = jinja_env.get_template("reading_email.html")
    html_body = template.render(
        name=name or "there",
        chart_name=chart_name or "Birth Chart",
        generated_date=datetime.now().strftime("%d %b %Y"),
        general_html=md_to_html(reading.get("general", "")),
        categories=categories,
        dasha_html=md_to_html(reading.get("dasha_advice", "")),
        remedies_html=md_to_html(reading.get("remedies", "")),
        unsubscribe_url=unsub_url,
    )

    msg = Mail(
        from_email=("noreply@grahalogic.ai", "GrahaLogic"),
        to_emails=to_email,
        subject="Your Personalized Jyotish Reading — GrahaLogic",
        html_content=html_body,
    )
    sg = sendgrid.SendGridAPIClient(os.environ["SENDGRID_API_KEY"])
    resp = sg.send(msg)
    return resp.status_code < 300

# ── Main loop ─────────────────────────────────────────────────────────────────
sent = skipped = errors = 0

for row in rows:
    email = row["email"]
    name  = row["name"] or ""
    try:
        reading = json.loads(row["reading"]) if isinstance(row["reading"], str) else row["reading"]
        if not reading or not reading.get("general"):
            log.warning("User %s (%s): reading empty — skipping", row["user_id"], email)
            skipped += 1
            continue

        ok = _send_email(email, name, row["chart_name"], reading)
        if ok:
            log.info("Sent to %s (%s)", email, name)
            sent += 1
        else:
            log.error("SendGrid rejected for %s", email)
            errors += 1

    except Exception as e:
        log.error("Error for %s: %s", email, e)
        errors += 1

cur.close()
conn.close()
log.info("Done. sent=%d  skipped=%d  errors=%d", sent, skipped, errors)
