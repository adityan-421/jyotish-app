#!/usr/bin/env python3
"""Flask app for Vedic Jyotish chart generation."""

import os
import json
import functools
import logging
import requests as http_requests

from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from jyotish_engine import compute_chart, compute_btr, calculate_sadesati, compute_panchang, compute_transits, compute_transits_for_date, compute_ashta_kuta_scores, format_kuta_scores_for_ai
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from database import (
    init_db, reset_pool, upsert_user, save_chart, get_charts, get_chart, delete_chart,
    update_chart, update_chart_reading, count_charts, get_question_count_today, save_ai_question, get_ai_history,
    get_all_charts_for_backfill, bulk_update_chart_data, get_cached_value, set_cached_value, get_stats, get_admin_stats,
    create_pending_reading, get_pending_readings_by_status, mark_readings_submitted,
    complete_reading, fail_reading, get_reading_status,
    set_own_chart, get_own_chart_id,
    get_users_with_own_chart, insert_user_prediction, get_pending_predictions,
    mark_predictions_submitted, get_submitted_predictions, complete_prediction,
    fail_prediction, get_user_predictions,
    upsert_push_token, get_users_with_push_tokens_and_charts,
    get_gem_balance, use_gem,
    get_app_setting, set_app_setting,
    get_users_for_weekly_email, insert_weekly_email, mark_weekly_emails_submitted,
    get_submitted_weekly_emails, complete_weekly_email, get_ai_ready_weekly_emails,
    mark_weekly_email_sent, fail_weekly_email, unsubscribe_user,
)

from pathlib import Path

logger = logging.getLogger(__name__)

# --- Prompt chain loader with mtime caching ---
_prompts_cache = {"mtime": 0, "data": None}


def load_prompts():
    """Load prompts.json, re-reading only when the file has been modified."""
    path = Path(__file__).parent / "prompts.json"
    mtime = path.stat().st_mtime
    if mtime != _prompts_cache["mtime"]:
        with open(path) as f:
            _prompts_cache["data"] = json.load(f)
        _prompts_cache["mtime"] = mtime
    return _prompts_cache["data"]


def build_conv_context(conversation):
    """Format prior chat turns into a string for the prompt template."""
    if not conversation:
        return ""
    ctx = "PRIOR CONVERSATION:\n"
    for turn in conversation[-4:]:
        role = turn.get("role", "user").upper()
        ctx += f"{role}: {turn.get('text', '')}\n\n"
    ctx += "Continue the conversation naturally. Reference prior discussion where relevant.\n\n"
    return ctx


def _safe_substitute(template, variables):
    """Replace {key} placeholders with values from variables dict.

    Only replaces {word} tokens that exist in variables — leaves all other
    braces (JSON examples, nested objects, etc.) untouched.  This avoids the
    ValueError that str.format_map raises on literal JSON in prompt templates.
    """
    import re
    def _replacer(match):
        key = match.group(1)
        if key in variables:
            val = variables[key]
            return val if isinstance(val, str) else str(val)
        return match.group(0)
    return re.sub(r'\{(\w+)\}', _replacer, template)


def _run_prompt_chain(model, steps, variables, default_thinking_budget=None):
    """Execute a sequence of prompt steps, returning the final output."""
    final_output = None
    for step in steps:
        # Format the prompt template with current variables
        prompt_text = _safe_substitute(step["prompt"], variables)

        # Build generation config with thinking budget if specified
        thinking_budget = step.get("thinking_budget", default_thinking_budget)
        gen_kwargs = {}
        if thinking_budget:
            gen_kwargs["generation_config"] = {
                "thinking_config": {"thinking_budget": int(thinking_budget)}
            }

        response = model.generate_content(prompt_text, **gen_kwargs)
        # Build result_text from non-thinking parts only.
        result_text = ""
        try:
            for part in response.candidates[0].content.parts:
                if getattr(part, "thought", False):
                    continue
                if hasattr(part, "text") and part.text:
                    result_text += part.text
            result_text = result_text.strip()
        except Exception:
            pass
        if not result_text:
            try:
                result_text = response.text.strip()
            except ValueError:
                raise

        # Log token usage for cost tracking
        try:
            um = response.usage_metadata
            in_tok    = getattr(um, "prompt_token_count", 0)
            out_tok   = getattr(um, "candidates_token_count", 0)
            think_tok = getattr(um, "thoughts_token_count", 0)
            cost = (in_tok * 0.075 + out_tok * 0.30 + think_tok * 3.50) / 1_000_000
            print(f"TOKEN_USAGE step={step.get('name')} in={in_tok} out={out_tok} thinking={think_tok} est_cost=${cost:.5f}", flush=True)
        except Exception:
            pass

        logger.info("PROMPT_CHAIN step=%s response_len=%d result_text_preview=%r",
                    step.get("name"), len(result_text), result_text[:300])

        # Process response based on type
        if step["response_type"] == "json":
            # Find the start of the first JSON object or array, then use
            # raw_decode to parse exactly that object and ignore any trailing
            # text (e.g. thinking output that appears after the JSON).
            cleaned = result_text
            obj_start = -1
            for i, ch in enumerate(cleaned):
                if ch in ('{', '['):
                    obj_start = i
                    break
            result = step.get("json_fallback", {})
            if obj_start != -1:
                try:
                    result, _ = json.JSONDecoder().raw_decode(cleaned, obj_start)
                except json.JSONDecodeError as je:
                    logger.error("PROMPT_CHAIN json_decode_error step=%s err=%s preview=%r",
                                 step.get("name"), je, cleaned[obj_start:obj_start + 300])
                    result = step.get("json_fallback", {})
            else:
                logger.error("PROMPT_CHAIN no_json_found step=%s preview=%r",
                             step.get("name"), cleaned[:300])
            variables[step["output_var"]] = result
        else:
            result = result_text
            variables[step["output_var"]] = result

        # Post-processing hooks
        post = step.get("post_process")
        if post == "lowercase_validate_category":
            val = variables[step["output_var"]].strip().lower()
            if val not in LIFE_CATEGORIES:
                val = "other"
            variables[step["output_var"]] = val
        elif post == "extract_category_from_factors":
            factors_data = variables[step["output_var"]]
            if isinstance(factors_data, dict):
                cat = factors_data.pop("category", "other").strip().lower()
            else:
                cat = "other"
            if cat not in LIFE_CATEGORIES:
                cat = "other"
            variables["category"] = cat

        # After category is known, compute chart_data for subsequent steps
        if variables.get("category") and "chart_data" not in variables and "raw_chart_data" in variables:
            relevant = extract_relevant_chart_data(
                variables["raw_chart_data"], variables["category"]
            )
            variables["chart_data"] = json.dumps(relevant, indent=2)

        # Serialize dicts/lists so they can be injected into prompt templates
        cur = variables[step["output_var"]]
        if isinstance(cur, (dict, list)):
            variables[step["output_var"]] = json.dumps(cur, indent=2)

        if step.get("is_final"):
            final_output = variables[step["output_var"]]

    return final_output


class _SafeFormatDict(dict):
    """dict subclass that returns '{key}' for missing keys in str.format_map."""

    def __missing__(self, key):
        return "{" + key + "}"

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-fallback-secret-key")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
CORS(app)
_tf = None


def _get_tf():
    global _tf
    if _tf is None:
        from timezonefinder import TimezoneFinder
        _tf = TimezoneFinder()
    return _tf

# Google OAuth setup
oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)


from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

_token_serializer = None

def _get_serializer():
    global _token_serializer
    if _token_serializer is None:
        _token_serializer = URLSafeTimedSerializer(app.secret_key, salt="mobile-auth")
    return _token_serializer


def get_current_user():
    """Return the current user dict from session or Bearer token."""
    user = session.get("user")
    if user:
        return user
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        try:
            user = _get_serializer().loads(token, max_age=90 * 86400)  # 90 days
            return user
        except (BadSignature, SignatureExpired):
            return None
    return None


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({"error": "Login required"}), 401
        session["user"] = user  # populate session so downstream code works
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    return render_template("index.html")


ADMIN_EMAIL = "adityan@gmail.com"

@app.route("/admin")
def admin_page():
    user = session.get("user")
    if not user or user.get("email") != ADMIN_EMAIL:
        return "Access denied", 403
    stats = get_admin_stats()
    settings = {
        "weekly_email_enabled": get_app_setting("weekly_email_enabled", "false"),
    }
    return render_template("admin.html", stats=stats, settings=settings)


@app.route("/admin/settings", methods=["POST"])
def admin_settings():
    user = session.get("user")
    if not user or user.get("email") != ADMIN_EMAIL:
        return "Access denied", 403
    key = request.form.get("key", "").strip()
    value = request.form.get("value", "").strip()
    allowed_keys = {"weekly_email_enabled"}
    if key in allowed_keys:
        set_app_setting(key, value)
    return redirect("/admin")


@app.route("/auth/google")
def auth_google():
    redirect_uri = url_for("auth_callback", _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route("/auth/callback")
def auth_callback():
    try:
        token = google.authorize_access_token()
    except Exception as e:
        logger.error("OAuth token exchange failed: %s", e)
        return redirect("/")

    userinfo = token.get("userinfo")
    if not userinfo:
        return "Authentication failed", 400

    user_id = userinfo["sub"]
    email = userinfo.get("email", "")
    name = userinfo.get("name", "")
    picture = userinfo.get("picture", "")

    # Retry once — handles cold-start or stale-connection DB failures
    for attempt in range(2):
        try:
            upsert_user(user_id, email, name, picture)
            break
        except Exception as e:
            logger.warning("upsert_user attempt %d failed: %s", attempt + 1, e)
            if attempt == 0:
                reset_pool()
                init_db()
            else:
                logger.error("upsert_user failed after retry, aborting login")
                return "Login failed — please try again", 500

    session["user"] = {
        "id": user_id,
        "email": email,
        "name": name,
        "picture": picture,
    }
    return redirect("/")


@app.route("/auth/logout")
def auth_logout():
    session.pop("user", None)
    return redirect("/")


@app.route("/auth/mobile", methods=["POST"])
def auth_mobile():
    """Exchange a Google token or auth code for a signed Bearer token."""
    data = request.get_json() or {}
    id_token = data.get("id_token")
    access_token = data.get("access_token")
    auth_code = data.get("auth_code")

    if not id_token and not access_token and not auth_code:
        return jsonify({"error": "id_token, access_token, or auth_code is required"}), 400

    try:
        if auth_code:
            # Exchange auth code for tokens, then get user info
            token_resp = http_requests.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": auth_code,
                    "client_id": data.get("client_id", os.environ.get("GOOGLE_CLIENT_ID")),
                    "redirect_uri": data.get("redirect_uri", "grahalogic://"),
                    "grant_type": "authorization_code",
                },
                timeout=10,
            )
            if token_resp.status_code != 200:
                logger.error("Code exchange failed: %s", token_resp.text)
                return jsonify({"error": "Code exchange failed"}), 401
            tokens = token_resp.json()
            at = tokens.get("access_token")
            if not at:
                return jsonify({"error": "No access token returned"}), 401
            # Use the access token to get user info
            resp = http_requests.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {at}"},
                timeout=10,
            )
            if resp.status_code != 200:
                return jsonify({"error": "Failed to fetch user info"}), 401
            info = resp.json()
            user_id = info.get("id")
            email = info.get("email", "")
            name = info.get("name", email.split("@")[0] if email else "")
            picture = info.get("picture", "")
        elif id_token:
            # Verify via Google's tokeninfo endpoint
            resp = http_requests.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"id_token": id_token},
                timeout=10,
            )
            if resp.status_code != 200:
                return jsonify({"error": "Invalid Google token"}), 401
            info = resp.json()
            user_id = info.get("sub")
            email = info.get("email", "")
            name = info.get("name", email.split("@")[0] if email else "")
            picture = info.get("picture", "")
        else:
            # Verify access token via Google's userinfo endpoint
            resp = http_requests.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            if resp.status_code != 200:
                return jsonify({"error": "Invalid Google access token"}), 401
            info = resp.json()
            user_id = info.get("id")
            email = info.get("email", "")
            name = info.get("name", email.split("@")[0] if email else "")
            picture = info.get("picture", "")

        if not user_id:
            return jsonify({"error": "Invalid token payload"}), 401
    except Exception as e:
        logger.error("Google token verification failed: %s", e)
        return jsonify({"error": "Token verification failed"}), 500

    # Upsert user in database
    for attempt in range(2):
        try:
            upsert_user(user_id, email, name, picture)
            break
        except Exception as e:
            logger.warning("upsert_user attempt %d failed: %s", attempt + 1, e)
            if attempt == 0:
                reset_pool()
                init_db()
            else:
                return jsonify({"error": "Login failed — please try again"}), 500

    user = {"id": user_id, "email": email, "name": name, "picture": picture}
    token = _get_serializer().dumps(user)

    return jsonify({"token": token, "user": user})


@app.route("/api/me")
def api_me():
    user = get_current_user()
    if user:
        try:
            remaining = 25 - get_question_count_today(user["id"])
        except Exception:
            remaining = 25  # Assume full quota if DB is unreachable
        try:
            own_chart_id = get_own_chart_id(user["id"])
        except Exception:
            own_chart_id = None
        return jsonify({"user": user, "ai_remaining": max(remaining, 0), "own_chart_id": own_chart_id})
    return jsonify({"user": None})


@app.route("/api/charts/save", methods=["POST"])
@login_required
def api_save_chart():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Chart name is required"}), 400

    input_data = data.get("input_data")
    chart_data = data.get("chart_data")
    if not input_data or not chart_data:
        return jsonify({"error": "Missing input_data or chart_data"}), 400

    is_own = data.get("is_own", False)

    user = session["user"]
    user_id = user["id"]
    upsert_user(user_id, user.get("email", ""), user.get("name", ""), user.get("picture", ""))
    chart_id, error = save_chart(user_id, name, input_data, chart_data, user_email=user.get("email", ""))
    if error:
        return jsonify({"error": error}), 400

    if is_own:
        set_own_chart(user_id, chart_id)

    return jsonify({"id": chart_id, "message": "Chart saved"})


@app.route("/api/charts")
@login_required
def api_list_charts():
    user_id = session["user"]["id"]
    charts = get_charts(user_id)
    total = count_charts(user_id)
    user_email = session["user"].get("email", "")
    from database import UNLIMITED_CHART_EMAILS
    chart_limit = None if user_email in UNLIMITED_CHART_EMAILS else 20
    return jsonify({"charts": charts, "count": total, "limit": chart_limit})


@app.route("/api/charts/<int:chart_id>")
@login_required
def api_get_chart(chart_id):
    user_id = session["user"]["id"]
    chart = get_chart(chart_id, user_id)
    if not chart:
        return jsonify({"error": "Chart not found"}), 404
    # Backfill Sade Sati for charts saved before the feature was added
    cd = chart.get("chart_data")
    if cd and not cd.get("sadesati"):
        try:
            moon = next(p for p in cd["planets"] if p["name"] == "Moon")
            cd["sadesati"] = calculate_sadesati(moon["lon"], cd["birth"]["jd"])
        except Exception:
            logger.warning("Could not backfill sadesati for chart %s", chart_id)
    return jsonify(chart)


@app.route("/api/charts/<int:chart_id>", methods=["DELETE"])
@login_required
def api_delete_chart(chart_id):
    user_id = session["user"]["id"]
    if delete_chart(chart_id, user_id):
        return jsonify({"message": "Chart deleted"})
    return jsonify({"error": "Chart not found"}), 404


@app.route("/api/charts/<int:chart_id>", methods=["PUT"])
@login_required
def api_update_chart(chart_id):
    data = request.get_json()
    input_data = data.get("input_data")
    chart_data = data.get("chart_data")
    if not input_data or not chart_data:
        return jsonify({"error": "Missing input_data or chart_data"}), 400
    user_id = session["user"]["id"]
    if update_chart(chart_id, user_id, input_data, chart_data):
        return jsonify({"message": "Chart updated"})
    return jsonify({"error": "Chart not found"}), 404


@app.route("/api/charts/<int:chart_id>/set-own", methods=["PUT"])
@login_required
def api_set_own_chart(chart_id):
    """Mark this chart as the user's own (personal) chart, or unset if already marked."""
    user_id = session["user"]["id"]
    chart = get_chart(chart_id, user_id)
    if not chart:
        return jsonify({"error": "Chart not found"}), 404
    if chart.get("is_own_chart"):
        set_own_chart(user_id, None)
        return jsonify({"message": "Own chart unset", "own_chart_id": None})
    set_own_chart(user_id, chart_id)
    return jsonify({"message": "Own chart set", "own_chart_id": chart_id})


@app.route("/api/charts/compare", methods=["POST"])
@login_required
def api_charts_compare():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    id1 = data.get("chart_id_1")
    id2 = data.get("chart_id_2")
    if id1 is None or id2 is None:
        return jsonify({"error": "chart_id_1 and chart_id_2 are required"}), 400
    try:
        id1, id2 = int(id1), int(id2)
    except (TypeError, ValueError):
        return jsonify({"error": "chart_id_1 and chart_id_2 must be integers"}), 400
    if id1 == id2:
        return jsonify({"error": "Cannot compare a chart with itself"}), 400

    user_id = session["user"]["id"]

    chart1 = get_chart(id1, user_id)
    if not chart1:
        return jsonify({"error": "Chart 1 not found"}), 404
    chart2 = get_chart(id2, user_id)
    if not chart2:
        return jsonify({"error": "Chart 2 not found"}), 404

    if get_question_count_today(user_id) >= 25:
        return jsonify({"error": "Daily limit reached. You can ask 25 questions per day."}), 429

    # GrahaGem check — comparisons cost 1 gem
    user_email = session["user"].get("email", "")
    if not use_gem(user_id, user_email):
        from datetime import date
        today = date.today()
        reset_date = date(today.year + 1, 1, 1) if today.month == 12 else date(today.year, today.month + 1, 1)
        return jsonify({
            "error": f"You've used all your GrahaGems for this month. Your 10 gems reset on {reset_date.strftime('1 %b %Y')}.",
            "gems_exhausted": True,
            "reset_date": reset_date.isoformat(),
        }), 402

    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel

        project_id = os.environ.get("GCP_PROJECT", "grahalogic")
        location = os.environ.get("GCP_LOCATION", "us-central1")
        vertexai.init(project=project_id, location=location)

        prompts_config = load_prompts()
        model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

        slim1 = extract_synastry_data(chart1["chart_data"])
        slim2 = extract_synastry_data(chart2["chart_data"])

        try:
            transits_now = compute_transits()
            transit_str = _format_transits_for_ai(transits_now)
        except Exception as te:
            logger.warning("Could not compute transits for compare: %s", te)
            transit_str = "(transit data unavailable)"

        kp1 = chart1["chart_data"].get("kuta_profile", {})
        kp2 = chart2["chart_data"].get("kuta_profile", {})
        try:
            kuta = compute_ashta_kuta_scores(kp1, kp2)
            kuta_str = format_kuta_scores_for_ai(kuta)
        except Exception as ke:
            logger.warning("Could not compute kuta scores: %s", ke)
            kuta_str = "(kuta scores unavailable)"

        variables = {
            "chart_a_name": chart1["name"],
            "chart_b_name": chart2["name"],
            "chart_a_data": json.dumps(slim1, indent=2),
            "chart_b_data": json.dumps(slim2, indent=2),
            "today": datetime.now().strftime("%d-%b-%Y"),
            "transit_data": transit_str,
            "kuta_scores": kuta_str,
        }

        steps = prompts_config.get("compatibility_steps", [])
        if not steps:
            return jsonify({"error": "Compatibility prompts not configured"}), 500

        raw = _run_prompt_chain(model, steps, variables, prompts_config.get("default_thinking_budget"))
        result = json.loads(raw) if isinstance(raw, str) else raw

        # Attach authoritative Ashta Kuta scores (engine-computed, not AI-inferred)
        if isinstance(result, dict) and kuta_str != "(kuta scores unavailable)":
            result["kuta_detail"] = kuta["scores"]
            result["kuta_total"] = kuta["total"]

        # Apply score label if not set correctly by AI
        score_map = [(90, "Exceptional"), (76, "Strong"), (61, "Good"), (41, "Moderate"), (0, "Challenging")]
        if isinstance(result, dict) and "score" in result:
            score = result.get("score", 0)
            for threshold, label in score_map:
                if score >= threshold:
                    result["score_label"] = label
                    break

        save_ai_question(user_id, f"Compare: {chart1['name']} & {chart2['name']}", "compatibility",
                         json.dumps(result) if isinstance(result, dict) else str(result))

        remaining = 25 - get_question_count_today(user_id)
        return jsonify({"compatibility": result, "remaining": remaining})

    except Exception as e:
        error_type = type(e).__name__
        logger.error("Compare AI error (%s): %s", error_type, str(e))
        return jsonify({"error": "Failed to generate compatibility reading. Please try again later."}), 500


@app.route("/api/ai-history")
@login_required
def api_ai_history():
    user_id = session["user"]["id"]
    history = get_ai_history(user_id)
    return jsonify({"history": history})


@app.route("/api/chart", methods=["POST"])
def api_chart():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON body provided"}), 400

        year = int(data["year"])
        month = int(data["month"])
        day = int(data["day"])
        hour = int(data.get("hour", 12))
        minute = int(data.get("minute", 0))
        lat = float(data["lat"])
        lon = float(data["lon"])
        tz_offset = float(data.get("tz_offset", 5.5))
        place = str(data.get("place", ""))

        # Basic validation
        if not (1 <= month <= 12):
            return jsonify({"error": "Month must be 1-12"}), 400
        if not (1 <= day <= 31):
            return jsonify({"error": "Day must be 1-31"}), 400
        if not (-90 <= lat <= 90):
            return jsonify({"error": "Latitude must be between -90 and 90"}), 400
        if not (-180 <= lon <= 180):
            return jsonify({"error": "Longitude must be between -180 and 180"}), 400

        result = compute_chart(year, month, day, hour, minute, lat, lon, tz_offset, place)
        return jsonify(result)

    except KeyError as e:
        return jsonify({"error": f"Missing required field: {e}"}), 400
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid input: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/btr", methods=["POST"])
def api_btr():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON body provided"}), 400

        year = int(data["year"])
        month = int(data["month"])
        day = int(data["day"])
        hour = int(data.get("hour", 12))
        minute = int(data.get("minute", 0))
        lat = float(data["lat"])
        lon = float(data["lon"])
        tz_offset = float(data.get("tz_offset", 5.5))

        if not (1 <= month <= 12):
            return jsonify({"error": "Month must be 1-12"}), 400
        if not (1 <= day <= 31):
            return jsonify({"error": "Day must be 1-31"}), 400
        if not (-90 <= lat <= 90):
            return jsonify({"error": "Latitude must be between -90 and 90"}), 400
        if not (-180 <= lon <= 180):
            return jsonify({"error": "Longitude must be between -180 and 180"}), 400

        result = compute_btr(year, month, day, hour, minute, lat, lon, tz_offset)
        return jsonify(result)

    except KeyError as e:
        return jsonify({"error": f"Missing required field: {e}"}), 400
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid input: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/btr/ask", methods=["POST"])
@login_required
def api_btr_ask():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    mode = data.get("mode")
    if mode not in ("questions", "analyze", "followup"):
        return jsonify({"error": "Invalid mode. Must be 'questions', 'analyze', or 'followup'."}), 400

    chart_data = data.get("chart_data")
    btr_data = data.get("btr_data")
    if not chart_data or not btr_data:
        return jsonify({"error": "chart_data and btr_data are required"}), 400

    conversation = data.get("conversation") or []

    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel

        project_id = os.environ.get("GCP_PROJECT", "grahalogic")
        location = os.environ.get("GCP_LOCATION", "us-central1")
        vertexai.init(project=project_id, location=location)

        prompts_config = load_prompts()
        model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

        # Format conversation context
        conv_ctx = ""
        if conversation:
            conv_ctx = "PREVIOUS ROUNDS:\n"
            for turn in conversation[-6:]:
                role = turn.get("role", "unknown").upper()
                turn_data = turn.get("data", "")
                if isinstance(turn_data, (dict, list)):
                    turn_data = json.dumps(turn_data, indent=1)
                conv_ctx += f"{role}: {turn_data}\n\n"

        age = data.get("age")
        additional_info = (data.get("additional_info") or "").strip()

        age_context = f"PERSON'S AGE: {age} years old\n" if age is not None else ""
        additional_context = f"ADDITIONAL CONTEXT FROM PERSON:\n{additional_info}\n" if additional_info else ""

        variables = {
            "chart_data": json.dumps(chart_data, indent=2) if isinstance(chart_data, dict) else str(chart_data),
            "btr_data": json.dumps(btr_data, indent=2) if isinstance(btr_data, dict) else str(btr_data),
            "conversation": conv_ctx,
            "age_context": age_context,
            "additional_context": additional_context,
        }

        if mode == "questions":
            steps = prompts_config.get("btr_questions_steps", [])
            if not steps:
                return jsonify({"error": "BTR question prompts not configured"}), 500
            result = _run_prompt_chain(model, steps, variables, prompts_config.get("default_thinking_budget"))
            # Parse result back to list if it's a string
            if isinstance(result, str):
                try:
                    cleaned = result
                    if cleaned.startswith("```"):
                        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3].strip()
                    questions = json.loads(cleaned)
                except json.JSONDecodeError:
                    questions = []
            else:
                questions = result if isinstance(result, list) else []
            return jsonify({"questions": questions})

        elif mode == "analyze":
            qa_pairs = data.get("qa_pairs") or []
            if not qa_pairs:
                return jsonify({"error": "qa_pairs required for analyze mode"}), 400
            variables["qa_pairs"] = json.dumps(qa_pairs, indent=2)

            steps = prompts_config.get("btr_analyze_steps", [])
            if not steps:
                return jsonify({"error": "BTR analysis prompts not configured"}), 500
            result = _run_prompt_chain(model, steps, variables, prompts_config.get("default_thinking_budget"))
            # Parse result back to dict if it's a string
            if isinstance(result, str):
                try:
                    cleaned = result
                    if cleaned.startswith("```"):
                        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3].strip()
                    analysis = json.loads(cleaned)
                except json.JSONDecodeError:
                    analysis = {
                        "suggested_adjustment": "Unable to parse AI response",
                        "explanation": result,
                        "chart_changes": [],
                        "confidence": "low",
                        "additional_questions": [],
                    }
            else:
                analysis = result if isinstance(result, dict) else {}
            return jsonify({"analysis": analysis})

        else:  # followup
            user_message = (data.get("user_message") or "").strip()
            if not user_message:
                return jsonify({"error": "user_message required for followup mode"}), 400
            variables["user_message"] = user_message

            steps = prompts_config.get("btr_followup_steps", [])
            if not steps:
                return jsonify({"error": "BTR followup prompts not configured"}), 500
            result = _run_prompt_chain(model, steps, variables, prompts_config.get("default_thinking_budget"))
            if isinstance(result, str):
                try:
                    cleaned = result
                    if cleaned.startswith("```"):
                        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3].strip()
                    followup_result = json.loads(cleaned)
                except json.JSONDecodeError:
                    followup_result = {
                        "response_text": result,
                        "suggested_adjustment": "Still inconclusive — need more data",
                        "explanation": result,
                        "chart_changes": [],
                        "confidence": "low",
                        "followup_questions": [],
                    }
            else:
                followup_result = result if isinstance(result, dict) else {}
            return jsonify({"followup": followup_result})

    except Exception as e:
        error_type = type(e).__name__
        logger.error("BTR AI error (%s): %s", error_type, str(e))
        return jsonify({"error": "Failed to generate BTR analysis. Please try again later."}), 500


@app.route("/api/reading-status/<reading_id>")
@login_required
def api_reading_status(reading_id):
    """Poll for batch reading completion."""
    user_id = session["user"]["id"]
    reading = get_reading_status(reading_id)
    if not reading or reading["user_id"] != user_id:
        return jsonify({"error": "Reading not found"}), 404

    result = {"status": reading["status"], "reading_id": reading_id}
    if reading["status"] == "completed" and reading["reading_data"]:
        try:
            result["reading_data"] = json.loads(reading["reading_data"])
        except (json.JSONDecodeError, TypeError):
            result["reading_data"] = reading["reading_data"]
    elif reading["status"] == "failed":
        result["error"] = reading.get("error", "Reading generation failed")
    return jsonify(result)


# ── Prediction helpers ────────────────────────────────────────────────────

def _get_week_start(dt=None):
    """Return the ISO date string for Monday of the current (or given) week."""
    d = (dt or datetime.now()).date()
    return (d - timedelta(days=d.weekday())).isoformat()


def _get_month_start(dt=None):
    """Return the ISO date string for the 1st of the current (or given) month."""
    d = (dt or datetime.now()).date()
    return d.replace(day=1).isoformat()


def _find_current_dasha(dasha):
    """Return (maha_lord, antar_lord, antar_end_str) for today from dasha data."""
    today = datetime.now()
    maha_lord = antar_lord = antar_end = None
    for m in dasha.get("maha", []):
        try:
            if datetime.strptime(m["start"], "%d-%b-%Y") <= today <= datetime.strptime(m["end"], "%d-%b-%Y"):
                maha_lord = m["lord"]
                break
        except Exception:
            continue
    if maha_lord and maha_lord in dasha.get("antar", {}):
        for a in dasha["antar"][maha_lord]:
            try:
                if datetime.strptime(a["start"], "%d-%b-%Y") <= today <= datetime.strptime(a["end"], "%d-%b-%Y"):
                    antar_lord = a["lord"]
                    antar_end = a["end"]
                    break
            except Exception:
                continue
    return maha_lord, antar_lord, antar_end


def _build_natal_summary(chart_data):
    """Compact natal chart summary string for prediction prompts."""
    lagna = chart_data.get("lagna", {})
    planets = {p["name"]: p for p in chart_data.get("planets", [])}
    dasha = chart_data.get("dasha", {})
    sadesati = chart_data.get("sadesati", {})

    moon = planets.get("Moon", {})
    maha_lord, antar_lord, antar_end = _find_current_dasha(dasha)

    planet_lines = []
    for name in ["Sun", "Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Rahu", "Ketu"]:
        p = planets.get(name)
        if p:
            retro = "(R)" if p.get("retro") else ""
            planet_lines.append(
                f"  {name}: {p.get('sign_name','')} H{p.get('house','')} {p.get('nakshatra','')} {retro}".rstrip()
            )

    lines = [
        f"Ascendant: {lagna.get('sign_name','')} ({lagna.get('nakshatra','')}; lord: {lagna.get('sign_lord','')})",
        f"Natal Moon: {moon.get('sign_name','')} – {moon.get('nakshatra','')} nakshatra",
        "Planets:",
        *planet_lines,
    ]
    if maha_lord:
        dasha_line = f"Current Dasha: {maha_lord} Mahadasha"
        if antar_lord:
            dasha_line += f" → {antar_lord} Antardasha (until {antar_end})"
        lines.append(dasha_line)
    if sadesati and sadesati.get("active"):
        lines.append(
            f"Sade Sati ACTIVE — Phase: {sadesati.get('phase','')}, Saturn in {sadesati.get('sign','')}"
        )
    return "\n".join(lines)


def _format_transit_compact(transits):
    """One-line compact transit summary."""
    parts = []
    for t in transits:
        r = "(R)" if t.get("retrograde") and t["planet"] not in ("Rahu", "Ketu") else ""
        parts.append(f"{t['planet']}:{t['sign']}{r}")
    return " | ".join(parts)


def _format_transits_for_ai(transits, natal_lagna_sign=None):
    """Format current transits for AI prompt — sign, degree, nakshatra, house from natal lagna."""
    lines = []
    for t in transits:
        retro = " (R)" if t.get("retrograde") and t["planet"] not in ("Rahu", "Ketu") else ""
        deg = f"{t.get('deg_in_sign', 0):.1f}"
        nak = t.get("nakshatra", "")
        pada = t.get("nakshatra_pada", "")
        nak_str = f" | {nak} P{pada}" if nak else ""
        house_str = ""
        if natal_lagna_sign:
            # sign_idx is 0-based; natal_lagna_sign is 1-based
            house = ((t.get("sign_idx", 0) - (natal_lagna_sign - 1)) % 12) + 1
            house_str = f" → H{house}"
        lines.append(f"  {t['planet']}: {t['sign']} {deg}°{retro}{nak_str}{house_str}")
    return "\n".join(lines)


def _build_daily_week_prompt(natal_summary, week_dates, prompts_config):
    """Build the 7-day prediction prompt for one user."""
    template = prompts_config.get("daily_week_prediction", "")
    day_blocks = []
    for date_str in week_dates:
        try:
            transits = compute_transits_for_date(date_str)
            panchang = compute_panchang(date_str, "UTC")
        except Exception as e:
            logger.warning("Transit/panchang error for %s: %s", date_str, e)
            transits, panchang = [], {}
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day_name = dt.strftime("%A")
        transit_str = _format_transit_compact(transits)
        p_str = (
            f"Tithi:{panchang.get('tithi','?')} "
            f"Nak:{panchang.get('nakshatra','?')} "
            f"Yoga:{panchang.get('yoga','?')}"
        )
        day_blocks.append(f"{day_name} {date_str}: {transit_str} | {p_str}")

    week_range = f"{week_dates[0]} (Mon) to {week_dates[-1]} (Sun)"
    daily_data = "\n".join(day_blocks)
    return template.format(natal_summary=natal_summary, week_range=week_range, daily_data=daily_data)


def _build_weekly_prompt(natal_summary, week_dates, prompts_config):
    """Build the single weekly prediction prompt for one user."""
    template = prompts_config.get("weekly_prediction", "")
    # Compute transits for Mon, Wed, Sun to capture week arc
    weekly_lines = []
    for date_str in [week_dates[0], week_dates[2], week_dates[6]]:
        try:
            transits = compute_transits_for_date(date_str)
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            weekly_lines.append(f"{dt.strftime('%A')} {date_str}: {_format_transit_compact(transits)}")
        except Exception:
            pass
    week_range = f"{week_dates[0]} (Mon) to {week_dates[6]} (Sun)"
    return template.format(
        natal_summary=natal_summary,
        week_range=week_range,
        weekly_transits="\n".join(weekly_lines),
    )


def _build_monthly_prompt(natal_summary, month_start_str, prompts_config):
    """Build the monthly prediction prompt for one user."""
    template = prompts_config.get("monthly_prediction", "")
    from datetime import date as _date
    import calendar
    year, month = map(int, month_start_str.split("-")[:2])
    last_day = calendar.monthrange(year, month)[1]
    mid_str = f"{year}-{month:02d}-15"
    end_str = f"{year}-{month:02d}-{last_day}"
    transit_blocks = []
    for label, date_str in [("Start", month_start_str), ("Mid", mid_str), ("End", end_str)]:
        try:
            transits = compute_transits_for_date(date_str)
            transit_blocks.append(f"{label} ({date_str}): {_format_transit_compact(transits)}")
        except Exception:
            pass
    month_name = _date(year, month, 1).strftime("%B %Y")
    return template.format(
        natal_summary=natal_summary,
        month_name=month_name,
        month_transits="\n".join(transit_blocks),
    )


# ── Weekly email helpers ───────────────────────────────────────────────────

APP_BASE_URL = os.environ.get("APP_BASE_URL", "")

_DAY_LORD_COLORS = {
    "Sun":     "Red / Orange",
    "Moon":    "White / Silver",
    "Mars":    "Red / Coral",
    "Mercury": "Green",
    "Jupiter": "Yellow / Gold",
    "Venus":   "White / Pink",
    "Saturn":  "Blue / Black",
}
# Mon=0→Moon, Tue=1→Mars, Wed=2→Mercury, Thu=3→Jupiter, Fri=4→Venus, Sat=5→Saturn, Sun=6→Sun
_WEEKDAY_LORDS = ["Moon", "Mars", "Mercury", "Jupiter", "Venus", "Saturn", "Sun"]


def _get_email_serializer():
    """Return an itsdangerous serializer scoped to email unsubscribe tokens."""
    return URLSafeTimedSerializer(app.secret_key, salt="email-unsub")


def _build_lucky_colors_raw(week_dates):
    """Return (prompt_text, list_of_dicts) for 7 days based on day-lord rules."""
    rows = []
    for d_str in week_dates:
        d = datetime.strptime(d_str, "%Y-%m-%d")
        lord = _WEEKDAY_LORDS[d.weekday()]
        color = _DAY_LORD_COLORS[lord]
        rows.append({"date": d_str, "day": d.strftime("%A"), "ruler": lord, "color": color})
    text = "\n".join(f'{r["day"]} {r["date"]}: {r["ruler"]} — {r["color"]}' for r in rows)
    return text, rows


def _build_weekly_transit_table(week_dates):
    """Build a compact 7-row transit table string for the weekly email prompt."""
    lines = []
    for d_str in week_dates:
        try:
            transits = compute_transits_for_date(d_str)
            panchang = compute_panchang(d_str, "UTC")
        except Exception:
            transits, panchang = [], {}
        dt = datetime.strptime(d_str, "%Y-%m-%d")
        transit_str = _format_transit_compact(transits)
        yoga = panchang.get("yoga", "?")
        vara = panchang.get("vara", "?")
        lines.append(f"{dt.strftime('%A')} {d_str} | {transit_str} | Yoga:{yoga} | Vara:{vara}")
    return "\n".join(lines)


def _build_weekly_email_prompt(natal_summary, week_dates, prompts_config):
    """Build the weekly email AI prompt for one user."""
    template = prompts_config.get("weekly_email", "")
    if not template:
        return ""
    transit_table = _build_weekly_transit_table(week_dates)
    lucky_colors_raw, _ = _build_lucky_colors_raw(week_dates)
    week_range = f"{week_dates[0]} (Mon) to {week_dates[6]} (Sun)"
    return _safe_substitute(template, {
        "natal_summary": natal_summary,
        "week_range": week_range,
        "transit_table": transit_table,
        "lucky_colors_raw": lucky_colors_raw,
    })


def _send_weekly_email(to_email, name, ai_content, week_label, user_id):
    """Render and send the weekly email via SendGrid. Returns True on success."""
    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail
    except ImportError:
        logger.error("sendgrid package not installed")
        return False

    try:
        unsub_token = _get_email_serializer().dumps(user_id)
        unsub_url = f"{APP_BASE_URL}/unsubscribe?token={unsub_token}"
        html_body = render_template(
            "weekly_email.html",
            name=name or "there",
            week_label=week_label,
            content=ai_content,
            unsubscribe_url=unsub_url,
        )
        msg = Mail(
            from_email=("noreply@grahalogic.com", "GrahaLogic"),
            to_emails=to_email,
            subject=f"Your Weekly Jyotish Reading — {week_label}",
            html_content=html_body,
        )
        sg = sendgrid.SendGridAPIClient(os.environ.get("SENDGRID_API_KEY", ""))
        resp = sg.send(msg)
        return resp.status_code < 300
    except Exception as e:
        logger.error("SendGrid error for %s: %s", to_email, e)
        return False


# ── Prediction API endpoints ──────────────────────────────────────────────

@app.route("/api/gems")
@login_required
def api_get_gems():
    """Return the current user's GrahaGem balance for this month."""
    user = get_current_user()
    balance = get_gem_balance(user["id"], user.get("email", ""))
    return jsonify(balance)


@app.route("/api/predictions")
@login_required
def api_get_predictions():
    """Return this user's current daily/weekly/monthly predictions."""
    user_id = session["user"]["id"]
    today = datetime.now().date()
    week_start = _get_week_start()
    month_start = _get_month_start()

    raw = get_user_predictions(user_id, week_start, month_start)

    # Parse the daily_week JSON array → find today's entry
    daily_text = None
    daily_raw = raw.get("daily_week")
    if daily_raw:
        try:
            days = json.loads(daily_raw)
            today_str = today.isoformat()
            for entry in days:
                if entry.get("date") == today_str:
                    daily_text = entry.get("text")
                    break
        except Exception:
            pass

    own_chart_id = get_own_chart_id(user_id)
    return jsonify({
        "daily": daily_text,
        "weekly": raw.get("weekly"),
        "monthly": raw.get("monthly"),
        "week_start": week_start,
        "month_start": month_start,
        "own_chart_id": own_chart_id,
    })


@app.route("/api/push-token", methods=["POST"])
@login_required
def api_register_push_token():
    user = get_current_user()
    token = (request.json or {}).get("token", "").strip()
    if not token:
        return jsonify({"error": "token required"}), 400
    upsert_push_token(user["id"], token)
    return jsonify({"ok": True})


SIGNS_LIST = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
]


def _generate_daily_notification(chart_data, panchang, transits):
    """Call Gemini to produce a 3-line daily notification."""
    lagna_sign = SIGNS_LIST[chart_data.get("lagna_sign", 1) - 1]

    moon_sign = ""
    for p in chart_data.get("planets", []):
        if p.get("abbr") == "Mo":
            moon_sign = p.get("sign_name", "")
            break

    dasha = chart_data.get("dasha", {})
    maha_lord, antar_lord, _ = _find_current_dasha(dasha)

    key_transits = ", ".join(
        f"{t['planet']} in {t['sign']}"
        for t in transits
        if t["planet"] in ("Saturn", "Jupiter", "Mars", "Rahu")
    )

    vara = panchang.get("vara", "")
    vara_lord = panchang.get("vara_lord", "")
    nakshatra = panchang.get("nakshatra", "")
    tithi = panchang.get("tithi", "")

    prompt = (
        f"You are a Vedic astrology daily guide. Give a concise personalized daily reading.\n\n"
        f"NATAL CHART: Lagna {lagna_sign}, Moon in {moon_sign}, "
        f"Maha Dasha {maha_lord} / Antar Dasha {antar_lord}\n"
        f"TODAY'S PANCHANG: {vara} (lord: {vara_lord}), Nakshatra {nakshatra}, Tithi {tithi}\n"
        f"KEY TRANSITS: {key_transits}\n\n"
        f"Respond with EXACTLY 3 lines, nothing else:\n"
        f"Line 1: Lucky color to wear today and a brief reason (under 10 words)\n"
        f"Line 2: Mantra to chant with repetition count (e.g. Om Budhaya Namah — 17x)\n"
        f"Line 3: One specific thing to be watchful about today (under 12 words)\n\n"
        f"No line numbers, no labels, no preamble. Plain text only."
    )

    from google import genai as _genai
    client = _genai.Client(api_key=os.environ.get("GEMINI_API_KEY", ""))
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    return response.text.strip()


@app.route("/api/cron/daily-notifications", methods=["POST"])
def cron_daily_notifications():
    """Generate and send daily push notifications to all users with a personal chart."""
    try:
        users = get_users_with_push_tokens_and_charts()
    except Exception as e:
        logger.error("Failed to fetch users for notifications: %s", e)
        return jsonify({"error": str(e)}), 500

    panchang = compute_panchang()
    transits = compute_transits()

    expo_messages = []
    sent = errors = 0

    for row in users:
        try:
            chart_data = json.loads(row["chart_data"]) if isinstance(row["chart_data"], str) else row["chart_data"]
            text = _generate_daily_notification(chart_data, panchang, transits)
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            body = "\n".join(lines[:3])

            for token in (row["push_tokens"] or []):
                if token and token.startswith("ExponentPushToken"):
                    expo_messages.append({
                        "to": token,
                        "title": "Your Daily Reading \u2726",
                        "body": body,
                        "sound": "default",
                        "channelId": "daily-reading",
                    })
            sent += 1
        except Exception as e:
            logger.error("Notification error for user %s: %s", row["user_id"], e)
            errors += 1

    if expo_messages:
        try:
            http_requests.post(
                "https://exp.host/--/api/v2/push/send",
                json=expo_messages,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=30,
            )
        except Exception as e:
            logger.error("Expo push send error: %s", e)

    return jsonify({"sent": sent, "errors": errors, "push_count": len(expo_messages)})


@app.route("/api/cron/submit-predictions", methods=["POST"])
def cron_submit_predictions():
    """Generate and submit prediction batch for all users with own chart set.

    Query params:
      type: 'daily_week' | 'weekly' | 'monthly'  (default: all three)
    """
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    pred_type = request.args.get("type")  # None means all
    types_to_run = [pred_type] if pred_type else ["daily_week", "weekly", "monthly"]

    users = get_users_with_own_chart()
    if not users:
        return jsonify({"submitted": 0, "message": "No users with own chart"})

    prompts_config = load_prompts()

    # Calculate period dates
    now = datetime.now()
    week_start = _get_week_start(now)
    month_start = _get_month_start(now)
    week_dates = [
        (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]

    # Build prompts and insert pending rows
    pending_rows = []  # list of (db_id, prompt_text)
    for u in users:
        natal_summary = _build_natal_summary(u["chart_data"])
        for t in types_to_run:
            period = week_start if t in ("daily_week", "weekly") else month_start
            # Skip if already exists (unique constraint returns None)
            db_id = insert_user_prediction(u["user_id"], t, period)
            if db_id is None:
                continue  # already generated this period
            if t == "daily_week":
                prompt = _build_daily_week_prompt(natal_summary, week_dates, prompts_config)
            elif t == "weekly":
                prompt = _build_weekly_prompt(natal_summary, week_dates, prompts_config)
            else:
                prompt = _build_monthly_prompt(natal_summary, month_start, prompts_config)
            pending_rows.append((db_id, prompt))

    if not pending_rows:
        return jsonify({"submitted": 0, "message": "All predictions already generated for this period"})

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )
    model_name = prompts_config.get("batch_model", prompts_config.get("model", "gemini-2.5-flash"))

    BATCH_SIZE = 500
    total_submitted = 0

    for chunk_start in range(0, len(pending_rows), BATCH_SIZE):
        chunk = pending_rows[chunk_start:chunk_start + BATCH_SIZE]
        inline_requests = [
            {"contents": [{"parts": [{"text": prompt}], "role": "user"}]}
            for _, prompt in chunk
        ]
        db_ids = [db_id for db_id, _ in chunk]
        try:
            batch_job = client.batches.create(
                model=model_name,
                src=inline_requests,
                config={"display_name": f"predictions-{now.strftime('%Y%m%d-%H%M%S')}-{chunk_start}"},
            )
            mark_predictions_submitted(db_ids, batch_job.name)
            total_submitted += len(chunk)
        except Exception as e:
            logger.error("Prediction batch submit failed: %s", e)
            for db_id in db_ids:
                fail_prediction(db_id, str(e))

    return jsonify({"submitted": total_submitted, "users": len(users)})


@app.route("/api/cron/check-predictions", methods=["POST"])
def cron_check_predictions():
    """Poll submitted prediction batches and store completed results."""
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    submitted = get_submitted_predictions()
    if not submitted:
        return jsonify({"checked": 0, "completed": 0})

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )

    # Group by batch_name
    batches = {}
    for r in submitted:
        if r.get("batch_name"):
            batches.setdefault(r["batch_name"], []).append(r)

    completed_count = 0
    failed_count = 0

    for batch_name, preds in batches.items():
        try:
            batch_job = client.batches.get(name=batch_name)
        except Exception as e:
            logger.error("Failed to fetch prediction batch %s: %s", batch_name, e)
            continue

        state = batch_job.state.name if hasattr(batch_job.state, "name") else str(batch_job.state)
        if state not in ("JOB_STATE_SUCCEEDED", "SUCCEEDED"):
            if state in ("JOB_STATE_FAILED", "FAILED"):
                for p in preds:
                    fail_prediction(p["id"], "Batch job failed")
                    failed_count += 1
            continue

        responses = batch_job.dest.inlined_responses if batch_job.dest else []
        preds_sorted = sorted(preds, key=lambda r: r.get("batch_index") or 0)

        for i, p in enumerate(preds_sorted):
            try:
                resp = responses[i] if i < len(responses) else None
                if not resp or not resp.response:
                    fail_prediction(p["id"], "No response from batch")
                    failed_count += 1
                    continue

                result_text = resp.response.text.strip()
                # Strip markdown fences if present
                if result_text.startswith("```"):
                    result_text = result_text.split("\n", 1)[1] if "\n" in result_text else result_text[3:]
                    if result_text.endswith("```"):
                        result_text = result_text[:-3].strip()

                # daily_week expects a JSON array; weekly/monthly are plain text
                if p["type"] == "daily_week":
                    json.loads(result_text)  # validate JSON; raises if invalid

                complete_prediction(p["id"], result_text)
                completed_count += 1
            except Exception as e:
                logger.error("Failed to process prediction %s: %s", p["id"], e)
                fail_prediction(p["id"], str(e))
                failed_count += 1

    return jsonify({"checked": len(submitted), "completed": completed_count, "failed": failed_count})


@app.route("/api/cron/submit-readings", methods=["POST"])
def cron_submit_readings():
    """Collect pending readings and submit as Gemini batch job."""
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    pending = get_pending_readings_by_status("pending")
    if not pending:
        return jsonify({"submitted": 0})

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )
    prompts_config = load_prompts()
    model_name = prompts_config.get("batch_model", prompts_config.get("model", "gemini-2.5-flash"))

    inline_requests = []
    reading_ids = []
    for r in pending:
        inline_requests.append({
            "contents": [{"parts": [{"text": r["prompt"]}], "role": "user"}],
        })
        reading_ids.append(r["id"])

    try:
        batch_job = client.batches.create(
            model=model_name,
            src=inline_requests,
            config={"display_name": f"readings-{datetime.now().strftime('%Y%m%d-%H%M%S')}"},
        )
        mark_readings_submitted(reading_ids, batch_job.name)
        return jsonify({"submitted": len(reading_ids), "batch_name": batch_job.name})
    except Exception as e:
        logger.error("Batch submit failed: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/check-readings", methods=["POST"])
def cron_check_readings():
    """Check submitted batch jobs and store completed readings."""
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    submitted = get_pending_readings_by_status("submitted")
    if not submitted:
        return jsonify({"checked": 0, "completed": 0})

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )

    # Group by batch_name
    batches = {}
    for r in submitted:
        if r.get("batch_name"):
            batches.setdefault(r["batch_name"], []).append(r)

    completed_count = 0
    failed_count = 0

    for batch_name, readings in batches.items():
        try:
            batch_job = client.batches.get(name=batch_name)
        except Exception as e:
            logger.error("Failed to fetch batch %s: %s", batch_name, e)
            continue

        state = batch_job.state.name if hasattr(batch_job.state, "name") else str(batch_job.state)

        if state not in ("JOB_STATE_SUCCEEDED", "SUCCEEDED"):
            if state in ("JOB_STATE_FAILED", "FAILED"):
                for r in readings:
                    fail_reading(r["id"], "Batch job failed")
                    failed_count += 1
            continue

        # Extract results from inline responses
        responses = batch_job.dest.inlined_responses if batch_job.dest else []
        readings_sorted = sorted(readings, key=lambda r: r.get("batch_index", 0))

        for i, r in enumerate(readings_sorted):
            try:
                resp = responses[i] if i < len(responses) else None
                if not resp or not resp.response:
                    fail_reading(r["id"], "No response from batch")
                    failed_count += 1
                    continue

                result_text = resp.response.text.strip()
                cleaned = result_text
                if cleaned.startswith("```"):
                    cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                    if cleaned.endswith("```"):
                        cleaned = cleaned[:-3].strip()
                reading_data = json.loads(cleaned)

                complete_reading(r["id"], json.dumps(reading_data))
                completed_count += 1

                # Cache on saved chart if chart_id present
                if r.get("chart_id"):
                    try:
                        update_chart_reading(r["chart_id"], r["user_id"], reading_data)
                    except Exception as e:
                        logger.warning("Failed to cache reading for chart %s: %s", r["chart_id"], e)

            except Exception as e:
                logger.error("Failed to parse reading %s: %s", r["id"], e)
                fail_reading(r["id"], str(e))
                failed_count += 1

    # Mark stale pending readings (>30 min) as failed
    stale = get_pending_readings_by_status("pending")
    for r in stale:
        if r.get("created_at"):
            age = (datetime.now() - r["created_at"]).total_seconds()
            if age > 1800:
                fail_reading(r["id"], "Timed out waiting for batch submission")
                failed_count += 1

    return jsonify({"checked": len(submitted), "completed": completed_count, "failed": failed_count})


@app.route("/api/cron/submit-weekly-emails", methods=["POST"])
def cron_submit_weekly_emails():
    """Build and submit weekly email AI prompts as a Gemini batch job.

    Query params:
      test=1  — only process admin account (adityan@gmail.com) for testing
    """
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    if get_app_setting("weekly_email_enabled", "false") != "true":
        return jsonify({"skipped": "weekly email disabled"}), 200

    is_test = request.args.get("test") == "1"
    test_email = ADMIN_EMAIL if is_test else None

    now = datetime.now()
    week_start = _get_week_start(now)
    week_dates = [
        (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7)
    ]

    users = get_users_for_weekly_email(week_start, test_email=test_email)
    if not users:
        return jsonify({"submitted": 0, "message": "No eligible users"})

    prompts_config = load_prompts()
    # Pre-compute transit table once — same for all users
    transit_table = _build_weekly_transit_table(week_dates)
    lucky_colors_raw, _ = _build_lucky_colors_raw(week_dates)
    week_range = f"{week_dates[0]} (Mon) to {week_dates[6]} (Sun)"
    template = prompts_config.get("weekly_email", "")

    pending_rows = []  # list of (db_id, prompt_text, user_email, user_name)
    for u in users:
        db_id = insert_weekly_email(u["user_id"], week_start)
        if db_id is None:
            continue
        natal_summary = _build_natal_summary(u["chart_data"])
        prompt = _safe_substitute(template, {
            "natal_summary": natal_summary,
            "week_range": week_range,
            "transit_table": transit_table,
            "lucky_colors_raw": lucky_colors_raw,
        })
        pending_rows.append((db_id, prompt, u["email"], u["name"]))

    if not pending_rows:
        return jsonify({"submitted": 0, "message": "All already generated for this week"})

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )
    model_name = prompts_config.get("batch_model", prompts_config.get("model", "gemini-2.5-flash"))

    inline_requests = [
        {"contents": [{"parts": [{"text": prompt}], "role": "user"}]}
        for _, prompt, _, _ in pending_rows
    ]
    db_ids = [db_id for db_id, _, _, _ in pending_rows]

    try:
        tag = "test-" if is_test else ""
        batch_job = client.batches.create(
            model=model_name,
            src=inline_requests,
            config={"display_name": f"weekly-email-{tag}{now.strftime('%Y%m%d-%H%M%S')}"},
        )
        id_index_pairs = [(db_id, idx) for idx, db_id in enumerate(db_ids)]
        mark_weekly_emails_submitted(id_index_pairs, batch_job.name)
    except Exception as e:
        logger.error("Weekly email batch submit failed: %s", e)
        for db_id in db_ids:
            fail_weekly_email(db_id, str(e))
        return jsonify({"error": str(e)}), 500

    return jsonify({"submitted": len(pending_rows), "week_start": week_start, "test": is_test})


@app.route("/api/cron/send-weekly-emails", methods=["POST"])
def cron_send_weekly_emails():
    """Poll completed weekly email batches and send emails via SendGrid."""
    secret = request.headers.get("X-Cron-Secret", "")
    if secret != os.environ.get("CRON_SECRET", os.environ.get("BACKFILL_SECRET", "")):
        return jsonify({"error": "Unauthorized"}), 401

    submitted = get_submitted_weekly_emails()
    if not submitted:
        # Also check for any ai_ready rows that weren't sent yet
        pass

    from google import genai
    from google.genai.types import HttpOptions

    client = genai.Client(
        api_key=os.environ.get("GEMINI_API_KEY"),
        http_options=HttpOptions(timeout=60_000),
    )

    # Group by batch_name
    batches = {}
    for r in submitted:
        if r.get("batch_name"):
            batches.setdefault(r["batch_name"], []).append(r)

    completed_count = failed_count = 0

    for batch_name, rows in batches.items():
        try:
            batch_job = client.batches.get(name=batch_name)
        except Exception as e:
            logger.error("Failed to fetch weekly email batch %s: %s", batch_name, e)
            continue

        state = batch_job.state.name if hasattr(batch_job.state, "name") else str(batch_job.state)
        if state not in ("JOB_STATE_SUCCEEDED", "SUCCEEDED"):
            if state in ("JOB_STATE_FAILED", "FAILED"):
                for r in rows:
                    fail_weekly_email(r["id"], "Batch job failed")
                    failed_count += 1
            continue

        responses = batch_job.dest.inlined_responses if batch_job.dest else []
        rows_sorted = sorted(rows, key=lambda r: r.get("batch_index") or 0)

        for i, r in enumerate(rows_sorted):
            try:
                resp = responses[i] if i < len(responses) else None
                if not resp or not resp.response:
                    fail_weekly_email(r["id"], "No response from batch")
                    failed_count += 1
                    continue

                result_text = resp.response.text.strip()
                if result_text.startswith("```"):
                    result_text = result_text.split("\n", 1)[1] if "\n" in result_text else result_text[3:]
                    if result_text.endswith("```"):
                        result_text = result_text[:-3].strip()

                ai_content = json.loads(result_text)
                complete_weekly_email(r["id"], result_text)
                completed_count += 1
            except Exception as e:
                logger.error("Failed to process weekly email %s: %s", r["id"], e)
                fail_weekly_email(r["id"], str(e))
                failed_count += 1

    # Now send all ai_ready rows
    ai_ready_rows = get_ai_ready_weekly_emails()
    sent_count = send_failed_count = 0

    for r in ai_ready_rows:
        try:
            ai_content = json.loads(r["ai_content"])
            week_start_str = r["week_start"].isoformat() if hasattr(r["week_start"], "isoformat") else str(r["week_start"])
            week_label = f"Week of {datetime.strptime(week_start_str, '%Y-%m-%d').strftime('%d %b %Y')}"
            ok = _send_weekly_email(r["email"], r["name"], ai_content, week_label, r["user_id"])
            if ok:
                mark_weekly_email_sent(r["id"])
                sent_count += 1
            else:
                fail_weekly_email(r["id"], "SendGrid returned non-2xx")
                send_failed_count += 1
        except Exception as e:
            logger.error("Failed to send weekly email to %s: %s", r.get("email"), e)
            fail_weekly_email(r["id"], str(e))
            send_failed_count += 1

    return jsonify({
        "batch_completed": completed_count,
        "batch_failed": failed_count,
        "emails_sent": sent_count,
        "emails_failed": send_failed_count,
    })


@app.route("/unsubscribe")
def unsubscribe_email():
    """Handle one-click unsubscribe from weekly emails."""
    token = request.args.get("token", "")
    try:
        user_id = _get_email_serializer().loads(token, max_age=90 * 86400)
        unsubscribe_user(user_id)
        return render_template("unsubscribe.html", success=True)
    except (BadSignature, SignatureExpired):
        return render_template("unsubscribe.html", success=False, reason="Link expired or invalid")
    except Exception as e:
        logger.error("Unsubscribe error: %s", e)
        return render_template("unsubscribe.html", success=False, reason="An error occurred")


@app.route("/api/stats", methods=["POST"])
def api_stats():
    """Return aggregate usage stats (protected by backfill secret)."""
    secret = request.headers.get("X-Backfill-Secret", "")
    if secret != os.environ.get("BACKFILL_SECRET", ""):
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(get_stats())


@app.route("/api/backfill", methods=["POST"])
def api_backfill():
    """Recompute chart_data for all saved charts using the latest engine."""
    expected = os.environ.get("BACKFILL_SECRET")
    if expected:
        secret = request.headers.get("X-Backfill-Secret", "")
        if secret != expected:
            return jsonify({"error": "Unauthorized"}), 401

    charts = get_all_charts_for_backfill()
    updated = 0
    errors = []
    for chart in charts:
        inp = chart["input_data"]
        try:
            result = compute_chart(
                int(inp["year"]), int(inp["month"]), int(inp["day"]),
                int(inp.get("hour", 12)), int(inp.get("minute", 0)),
                float(inp["lat"]), float(inp["lon"]),
                float(inp.get("tz_offset", 5.5)),
                str(inp.get("place", "")),
            )
            bulk_update_chart_data(chart["id"], result)
            updated += 1
        except Exception as e:
            errors.append({"id": chart["id"], "error": str(e)})

    return jsonify({"updated": updated, "errors": errors, "total": len(charts)})


@app.route("/api/timezone", methods=["POST"])
def api_timezone():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON body provided"}), 400

        lat = float(data["lat"])
        lon = float(data["lon"])
        year = int(data.get("year", 2000))
        month = int(data.get("month", 1))
        day = int(data.get("day", 1))
        hour = int(data.get("hour", 12))
        minute = int(data.get("minute", 0))

        tz_name = _get_tf().timezone_at(lat=lat, lng=lon)
        if not tz_name:
            # Fallback: rough estimate from longitude
            offset = round(lon / 15 * 2) / 2
            return jsonify({"tz_name": None, "offset": offset})

        dt = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(tz_name))
        offset = dt.utcoffset().total_seconds() / 3600

        return jsonify({"tz_name": tz_name, "offset": offset})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def extract_relevant_chart_data(chart_data, category):
    """Filter chart JSON to only include data relevant to the category."""
    cat = category.lower()

    # Category → extra divisional charts beyond the D1+D9 default
    extra_charts = []
    if cat in ("career", "business"):
        extra_charts = ["D10"]
    elif cat == "finance":
        extra_charts = ["D2"]
    elif cat == "children":
        extra_charts = ["D7"]
    elif cat == "siblings":
        extra_charts = ["D3"]
    elif cat in ("spirituality", "other"):
        extra_charts = ["D60"]

    result = _strip_chart_for_ai(chart_data, extra_charts=extra_charts)

    # Ashtakavarga only for finance/career
    if cat not in ("finance", "gains/profits", "career", "business"):
        result.pop("ashtakavarga", None)

    result["current_date"] = datetime.now().strftime("%d-%b-%Y")
    return result


def extract_synastry_data(chart_data):
    """Filter chart JSON to only the fields relevant for Vedic synastry analysis."""
    # D9 is always critical for synastry; strip everything else
    result = _strip_chart_for_ai(chart_data, extra_charts=["D9"])
    # Synastry doesn't need ashtakavarga or doshas
    result.pop("ashtakavarga", None)
    result.pop("doshas", None)
    return result


def _relevant_maha_periods(maha_list):
    """Return the current maha dasha period plus its neighbours."""
    now = datetime.now()
    current_idx = None
    for i, m in enumerate(maha_list):
        try:
            end = datetime.strptime(m["end"], "%d-%b-%Y")
            start = datetime.strptime(m["start"], "%d-%b-%Y")
            if start <= now <= end:
                current_idx = i
                break
        except (ValueError, KeyError):
            continue
    if current_idx is not None:
        lo = max(0, current_idx - 1)
        hi = min(len(maha_list), current_idx + 2)
        return maha_list[lo:hi]
    return maha_list[-3:]


def _parse_date_safe(s):
    try:
        return datetime.strptime(s, "%d-%b-%Y")
    except Exception:
        return None


def _trim_dasha(dasha):
    """
    Trim dasha for AI consumption:
    - maha: current + 1 before + 2 after (existing behaviour)
    - antar: only the 3 relevant maha lords (not all 9)
    - pratyantar: only the current maha lord's 9 antar entries (covers next ~20 yrs
      of sub-sub periods; further-future pratyantar is too granular to be useful)
    """
    if not dasha:
        return dasha
    dasha = dict(dasha)

    trimmed_maha = _relevant_maha_periods(dasha.get("maha", []))
    dasha["maha"] = trimmed_maha
    relevant_lords = {m["lord"] for m in trimmed_maha}

    # Find current maha lord for pratyantar trimming
    now = datetime.now()
    current_maha_lord = None
    for m in trimmed_maha:
        try:
            if datetime.strptime(m["start"], "%d-%b-%Y") <= now <= datetime.strptime(m["end"], "%d-%b-%Y"):
                current_maha_lord = m["lord"]
                break
        except Exception:
            pass

    if "antar" in dasha:
        dasha["antar"] = {k: v for k, v in dasha["antar"].items() if k in relevant_lords}

    if "pratyantar" in dasha:
        if current_maha_lord and current_maha_lord in dasha["pratyantar"]:
            dasha["pratyantar"] = {current_maha_lord: dasha["pratyantar"][current_maha_lord]}
        else:
            dasha.pop("pratyantar", None)

    return dasha


def _slim_sadesati(ss):
    """Keep only current_status, moon_sign, and the active/nearest future cycle."""
    if not ss:
        return ss
    now = datetime.now()
    result = {k: ss[k] for k in ("moon_sign", "active", "current_status") if k in ss}

    # Keep only the active or next upcoming sade sati cycle
    kept = None
    for cycle in ss.get("cycles", []):
        end = _parse_date_safe(cycle.get("end", ""))
        if end and end >= now:
            kept = cycle
            break
    if kept:
        result["cycles"] = [kept]

    # Dhaiya periods within the next 5 years
    cutoff = datetime(now.year + 5, now.month, now.day)
    result["dhaiya"] = [
        d for d in ss.get("dhaiya", [])
        if _parse_date_safe(d.get("end", "")) and _parse_date_safe(d.get("end", "")) >= now
        and _parse_date_safe(d.get("start", "")) and _parse_date_safe(d.get("start", "")) <= cutoff
    ]
    return result


# Sign lords indexed 0=Aries … 11=Pisces (mirrors jyotish_engine.SIGN_LORDS)
_SIGN_LORDS_LIST = [
    "Mars", "Venus", "Mercury", "Moon", "Sun", "Mercury",
    "Venus", "Mars", "Jupiter", "Saturn", "Saturn", "Jupiter",
]


def _strip_chart_for_ai(chart_data, extra_charts=None):
    """
    Return a cleaned copy of chart_data suitable for AI prompts:
    - Strips computational/redundant fields (raw lons, speed, abbrs, jd, ayanamsa)
    - Keeps D1 + D9 always; adds extra_charts; drops D12, D20 always
    - Replaces verbose bhava array with compact house_lords dict
    - Trims dasha (maha/antar/pratyantar) and sadesati
    - Strips panchang display-only strings and karaka abbreviations
    """
    result = {}
    keep_charts = ({"D1", "D9"} | set(extra_charts or [])) - {"D12", "D20"}

    # birth — strip computation artifacts
    if "birth" in chart_data:
        b = dict(chart_data["birth"])
        b.pop("jd", None)
        b.pop("ayanamsa", None)
        result["birth"] = b

    # lagna — strip raw longitudes
    if "lagna" in chart_data:
        l = dict(chart_data["lagna"])
        l.pop("lon", None)
        l.pop("lon_fmt", None)
        result["lagna"] = l

    # planets — strip lon, full_lon, speed, abbr
    if "planets" in chart_data:
        result["planets"] = [
            {k: v for k, v in p.items() if k not in ("lon", "full_lon", "speed", "abbr")}
            for p in chart_data["planets"]
        ]

    # charts — keep only relevant, strip planet_degs from D1
    if "charts" in chart_data:
        filtered = {}
        for k, v in chart_data["charts"].items():
            if k not in keep_charts:
                continue
            if k == "D1":
                v = {ck: cv for ck, cv in v.items() if ck != "planet_degs"}
            filtered[k] = v
        result["charts"] = filtered

    # dignities — same chart filter
    if "dignities" in chart_data:
        result["dignities"] = {k: v for k, v in chart_data["dignities"].items() if k in keep_charts}

    # house_lords — compact replacement for bhava (12 entries vs ~400 tokens)
    lagna_sign = (chart_data.get("lagna") or {}).get("sign")
    if lagna_sign:
        idx = int(lagna_sign) - 1
        result["house_lords"] = {
            str(h): _SIGN_LORDS_LIST[(idx + h - 1) % 12]
            for h in range(1, 13)
        }

    # karakas — strip abbreviation fields
    if "karakas" in chart_data:
        result["karakas"] = [
            {k: v for k, v in kar.items() if k not in ("karaka_abbr", "planet_abbr")}
            for kar in chart_data["karakas"]
        ]

    # panchang — strip display-only strings (duplicates of structured fields)
    if "panchang" in chart_data:
        result["panchang"] = {
            lk: ({k: v for k, v in lv.items() if k != "display"} if isinstance(lv, dict) else lv)
            for lk, lv in chart_data["panchang"].items()
        }

    # dasha — trim maha/antar/pratyantar
    if "dasha" in chart_data:
        result["dasha"] = _trim_dasha(dict(chart_data["dasha"]))

    # sadesati — slim to current cycle only
    if "sadesati" in chart_data:
        result["sadesati"] = _slim_sadesati(chart_data["sadesati"])

    # pass-through fields (no stripping needed)
    for key in ("yogas", "doshas", "aspects", "ashtakavarga",
                "arudha_lagna", "combust_planets", "vargottama_planets",
                "kuta_profile", "current_date"):
        if key in chart_data:
            result[key] = chart_data[key]

    return result


LIFE_CATEGORIES = [
    "health", "relationship", "finance", "career", "spouse", "children",
    "siblings", "education", "spirituality", "travel", "property", "legal",
    "gains/profits", "friends", "business", "other",
]


@app.route("/api/ask", methods=["POST"])
@login_required
def api_ask():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    initial_reading = data.get("initial_reading", False)

    question = (data.get("question") or "").strip()
    if not question and not initial_reading:
        return jsonify({"error": "Question is required"}), 400
    if len(question) > 500:
        return jsonify({"error": "Question too long (max 500 characters)"}), 400

    conversation = data.get("conversation") or []  # prior turns for multi-turn

    chart_data = data.get("chart_data")
    if not chart_data:
        return jsonify({"error": "No chart data provided. Please generate or load a chart first."}), 400

    user = session["user"]
    user_id = user["id"]

    # Rate limit: 25 questions per day
    if get_question_count_today(user_id) >= 25:
        return jsonify({"error": "Daily limit reached. You can ask 25 questions per day."}), 429

    # GrahaGem check — initial readings are free; follow-up questions cost 1 gem
    user_email = user.get("email", "")
    if not initial_reading:
        if not use_gem(user_id, user_email):
            balance = get_gem_balance(user_id, user_email)
            import calendar
            from datetime import date
            today = date.today()
            last_day = calendar.monthrange(today.year, today.month)[1]
            reset_date = date(today.year, today.month, last_day + 1 if last_day < 31 else 1)
            # First day of next month
            if today.month == 12:
                reset_date = date(today.year + 1, 1, 1)
            else:
                reset_date = date(today.year, today.month + 1, 1)
            return jsonify({
                "error": f"You've used all your GrahaGems for this month. Your 10 gems reset on {reset_date.strftime('1 %b %Y')}.",
                "gems_exhausted": True,
                "reset_date": reset_date.isoformat(),
            }), 402

    # Ensure user exists in DB
    upsert_user(user_id, user.get("email", ""), user.get("name", ""), user.get("picture", ""))
    chart_id = data.get("chart_id")  # optional: cache reading back to saved chart

    try:
        prompts_config = load_prompts()

        if initial_reading and "initial_reading_steps" in prompts_config:
            # Run initial reading synchronously via Vertex AI
            import vertexai
            from vertexai.generative_models import GenerativeModel

            project_id = os.environ.get("GCP_PROJECT", "grahalogic")
            location = os.environ.get("GCP_LOCATION", "us-central1")
            vertexai.init(project=project_id, location=location)

            model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

            # Initial reading needs all major divisional charts
            full_chart = _strip_chart_for_ai(
                chart_data, extra_charts=["D2", "D7", "D10"]
            )
            full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

            try:
                transits_now = compute_transits()
                natal_lagna_sign = chart_data.get("lagna_sign") or (chart_data.get("lagna") or {}).get("sign")
                transit_str = _format_transits_for_ai(transits_now, natal_lagna_sign)
            except Exception as te:
                logger.warning("Could not compute transits for initial reading: %s", te)
                transit_str = "(transit data unavailable)"

            variables = {
                "today": datetime.now().strftime("%d-%b-%Y"),
                "chart_data": json.dumps(full_chart, indent=2),
                "transit_data": transit_str,
            }

            raw = _run_prompt_chain(
                model, prompts_config["initial_reading_steps"], variables,
                prompts_config.get("default_thinking_budget")
            )
            reading_data = json.loads(raw) if isinstance(raw, str) else raw

            save_ai_question(user_id, question or "Initial reading", "comprehensive",
                             json.dumps(reading_data) if isinstance(reading_data, dict) else str(reading_data))
            if chart_id:
                try:
                    update_chart_reading(chart_id, user_id, reading_data)
                except Exception as e:
                    logger.warning("Failed to cache reading for chart %s: %s", chart_id, e)

            remaining = 25 - get_question_count_today(user_id)
            return jsonify({"reading_data": reading_data, "remaining": remaining})
        else:
            # Follow-up / normal question — real-time via Vertex AI
            import vertexai
            from vertexai.generative_models import GenerativeModel

            project_id = os.environ.get("GCP_PROJECT", "grahalogic")
            location = os.environ.get("GCP_LOCATION", "us-central1")
            vertexai.init(project=project_id, location=location)

            model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

            full_chart = _strip_chart_for_ai(chart_data)
            full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

            try:
                transits_now = compute_transits()
                natal_lagna_sign = chart_data.get("lagna_sign") or (chart_data.get("lagna") or {}).get("sign")
                transit_str = _format_transits_for_ai(transits_now, natal_lagna_sign)
            except Exception as te:
                logger.warning("Could not compute transits for ask: %s", te)
                transit_str = "(transit data unavailable)"

            variables = {
                "question": question,
                "categories": ", ".join(LIFE_CATEGORIES),
                "today": datetime.now().strftime("%d-%b-%Y"),
                "conversation": build_conv_context(conversation),
                "raw_chart_data": chart_data,
                "transit_data": transit_str,
            }

            # For follow-ups (has conversation), skip full chart_data — let
            # extract_relevant_chart_data() populate it after categorization.
            # Also disable thinking to save tokens on follow-ups.
            if conversation:
                thinking_budget = 0
            else:
                variables["chart_data"] = json.dumps(full_chart, indent=2)
                thinking_budget = prompts_config.get("default_thinking_budget")

            reading = _run_prompt_chain(model, prompts_config["steps"], variables, thinking_budget)
            category = variables.get("category", "other")

        # Save to DB
        save_ai_question(user_id, question, category, reading)

        remaining = 25 - get_question_count_today(user_id)
        return jsonify({"category": category, "reading": reading, "remaining": remaining})

    except Exception as e:
        error_type = type(e).__name__
        logger.error("AI reading error (%s): %s", error_type, str(e))
        return jsonify({"error": "Failed to generate reading. Please try again later."}), 500


# Cosmic weather is cached in Postgres (app_cache table) with a 7-day TTL.


@app.route("/api/panchang")
def api_panchang():
    date_str = request.args.get("date")
    tz_str   = request.args.get("tz", "UTC")
    if not date_str:
        from datetime import date as _date
        from zoneinfo import ZoneInfo
        try:
            tz = ZoneInfo(tz_str)
        except Exception:
            tz = ZoneInfo("UTC")
        date_str = datetime.now(tz).strftime("%Y-%m-%d")
    try:
        data = compute_panchang(date_str, tz_str)
        return jsonify(data)
    except Exception as e:
        logger.error("Panchang error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/panchang/month")
def api_panchang_month():
    tz_str = request.args.get("tz", "UTC")
    try:
        year  = int(request.args.get("year",  datetime.now().year))
        month = int(request.args.get("month", datetime.now().month))
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid year/month"}), 400
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]
    result = []
    for day in range(1, days_in_month + 1):
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        try:
            result.append(compute_panchang(date_str, tz_str))
        except Exception as e:
            result.append({"date": date_str, "error": str(e)})
    return jsonify({"year": year, "month": month, "days": result})


@app.route("/api/transits")
def api_transits():
    try:
        data = compute_transits()
        return jsonify({"transits": data})
    except Exception as e:
        logger.error("Transits error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/cosmic-weather")
def api_cosmic_weather():
    # Check DB cache first (persists across container restarts and instances)
    try:
        cached = get_cached_value("cosmic_weather", max_age_days=7)
        if cached:
            return jsonify(cached)
    except Exception as e:
        logger.warning("Cosmic weather DB read failed: %s", e)

    try:
        transits = compute_transits()
        planet_summary = ", ".join(
            f"{t['planet']} in {t['sign']}" + (" (R)" if t['retrograde'] else "")
            for t in transits
        )
        today_str = datetime.now().strftime("%d %B %Y")

        import vertexai
        from vertexai.generative_models import GenerativeModel
        vertexai.init(project=os.environ.get("GCP_PROJECT", "grahalogic"),
                      location=os.environ.get("GCP_LOCATION", "us-central1"))
        prompts_config = load_prompts()
        model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

        prompt = (
            f"Today is {today_str}. Current planetary transits (sidereal/Vedic): {planet_summary}.\n\n"
            "As a Vedic astrologer, write a brief 'cosmic weather' update for this week in 2-3 sentences. "
            "Mention 1-2 of the most significant transits and what they mean for people in general. "
            "Keep it warm, insightful, and practical. Do not use markdown. Plain text only."
        )
        response = model.generate_content(prompt)
        text = response.text.strip()

        result = {"text": text, "generated_on": today_str, "transits_used": planet_summary}
        try:
            set_cached_value("cosmic_weather", result)
        except Exception as e:
            logger.warning("Cosmic weather DB write failed: %s", e)
        return jsonify(result)
    except Exception as e:
        logger.error("Cosmic weather error: %s", e)
        return jsonify({"text": "The cosmos is momentarily quiet. Check back soon.", "generated_on": "", "transits_used": ""}), 200


# Try to init DB at startup in a background thread so a stalled connection
# (e.g. SSL handshake hang) never blocks the gunicorn worker from booting.
import threading as _threading
_t = _threading.Thread(target=init_db, daemon=True)
_t.start()
_t.join(timeout=8)  # give up after 8 s; init_db will retry on first request


@app.before_request
def _ensure_db():
    """Lazily retry DB init if it failed at startup — skip for non-DB routes."""
    if request.endpoint in ("index",):
        return
    try:
        init_db()
    except Exception:
        pass


@app.after_request
def _add_cache_headers(response):
    """Add cache-control headers for the main page."""
    if request.path == "/" and response.status_code == 200:
        response.headers["Cache-Control"] = "public, max-age=300"
    return response


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 8080)))
