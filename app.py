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
from jyotish_engine import compute_chart, compute_btr, calculate_sadesati, compute_panchang, compute_transits
from zoneinfo import ZoneInfo
from datetime import datetime
from database import (
    init_db, reset_pool, upsert_user, save_chart, get_charts, get_chart, delete_chart,
    update_chart, update_chart_reading, count_charts, get_question_count_today, save_ai_question, get_ai_history,
    get_all_charts_for_backfill, bulk_update_chart_data, get_cached_value, set_cached_value, get_stats,
    create_pending_reading, get_pending_readings_by_status, mark_readings_submitted,
    complete_reading, fail_reading, get_reading_status,
    set_own_chart, get_own_chart_id,
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
        try:
            result_text = response.text.strip()
        except ValueError:
            # Gemini raises ValueError when finish_reason != STOP (e.g. RECITATION, MAX_TOKENS).
            # The content is still present in candidates — extract it directly.
            result_text = ""
            try:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "text") and part.text:
                        result_text += part.text
            except Exception:
                pass
            result_text = result_text.strip()
            if not result_text:
                raise

        # Process response based on type
        if step["response_type"] == "json":
            # Strip markdown code fences if present
            cleaned = result_text
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].strip()
            try:
                result = json.loads(cleaned)
            except json.JSONDecodeError:
                result = step.get("json_fallback", {})
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
    chart_id, error = save_chart(user_id, name, input_data, chart_data)
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
    return jsonify({"charts": charts, "count": total, "limit": 20})


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
    user_id = session["user"]["id"]
    if set_own_chart(user_id, chart_id):
        return jsonify({"message": "Chart set as your own birth chart"})
    return jsonify({"error": "Chart not found"}), 404


@app.route("/api/charts/clear-own", methods=["PUT"])
@login_required
def api_clear_own_chart():
    user_id = session["user"]["id"]
    set_own_chart(user_id, None)
    return jsonify({"message": "Own chart cleared"})


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
    result = {}

    # Always include birth info, lagna, and planets
    for key in ("birth", "lagna", "planets"):
        if key in chart_data:
            result[key] = chart_data[key]

    cat = category.lower()

    # Determine which divisional charts to include
    charts_to_include = ["D1", "D60"]
    if cat in ("spouse", "relationship"):
        charts_to_include.append("D9")
    elif cat in ("career", "business"):
        charts_to_include.append("D10")
    elif cat == "finance":
        charts_to_include.append("D2")
    elif cat == "children":
        charts_to_include.append("D7")
    elif cat == "siblings":
        charts_to_include.append("D3")

    if "charts" in chart_data:
        result["charts"] = {k: v for k, v in chart_data["charts"].items() if k in charts_to_include}

    # Determine which houses are most relevant
    house_map = {
        "health": [1, 6, 8],
        "relationship": [5, 7, 11],
        "spouse": [5, 7, 11],
        "finance": [2, 11],
        "career": [2, 6, 7, 10, 11],
        "business": [2, 6, 7, 10, 11],
        "children": [5, 9],
        "siblings": [3, 11],
        "education": [4, 5, 9],
        "spirituality": [5, 9, 12],
        "travel": [3, 9, 12],
        "property": [4],
        "legal": [6, 8, 12],
        "gains/profits": [2, 11],
        "friends": [11],
    }

    # Include dignities for selected charts
    if "dignities" in chart_data:
        result["dignities"] = {k: v for k, v in chart_data["dignities"].items() if k in charts_to_include}

    # Always include these if present
    for key in ("karakas", "panchang", "arudha_lagna"):
        if key in chart_data:
            result[key] = chart_data[key]

    # Include aspects
    if "aspects" in chart_data:
        result["aspects"] = chart_data["aspects"]

    # Include yogas for most categories
    if "yogas" in chart_data:
        result["yogas"] = chart_data["yogas"]

    # Include bhava
    if "bhava" in chart_data:
        relevant_houses = house_map.get(cat)
        if relevant_houses:
            result["bhava"] = [b for b in chart_data["bhava"] if b.get("house") in relevant_houses]
        else:
            result["bhava"] = chart_data["bhava"]

    # Include ashtakavarga for finance-related queries
    if cat in ("finance", "gains/profits", "career", "business") and "ashtakavarga" in chart_data:
        result["ashtakavarga"] = chart_data["ashtakavarga"]

    # Include dasha — send current maha period and neighbours for context
    if "dasha" in chart_data:
        dasha = dict(chart_data["dasha"])
        if "maha" in dasha:
            dasha["maha"] = _relevant_maha_periods(dasha["maha"])
        result["dasha"] = dasha

    # Fallback: for "other", send everything with D1
    if cat == "other":
        result = dict(chart_data)
        if "dasha" in result and "maha" in result["dasha"]:
            result["dasha"] = dict(result["dasha"])
            result["dasha"]["maha"] = _relevant_maha_periods(result["dasha"]["maha"])

    # Always include today's date so AI knows what's current
    result["current_date"] = datetime.now().strftime("%d-%b-%Y")

    return result


def _relevant_maha_periods(maha_list):
    """Return the current maha dasha period plus its neighbours."""
    from datetime import datetime
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
    # Fallback: return last 3 if current not found
    return maha_list[-3:]


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

    question = (data.get("question") or "").strip()
    if not question:
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

    # Ensure user exists in DB
    upsert_user(user_id, user.get("email", ""), user.get("name", ""), user.get("picture", ""))

    initial_reading = data.get("initial_reading", False)
    chart_id = data.get("chart_id")  # optional: cache reading back to saved chart

    try:
        prompts_config = load_prompts()

        if initial_reading and "initial_reading_steps" in prompts_config:
            # Queue initial reading for batch processing (50% cheaper)
            import uuid
            reading_id = str(uuid.uuid4())

            full_chart = dict(chart_data)
            if "dasha" in full_chart and "maha" in full_chart.get("dasha", {}):
                full_chart["dasha"] = dict(full_chart["dasha"])
                full_chart["dasha"]["maha"] = _relevant_maha_periods(full_chart["dasha"]["maha"])
            full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

            variables = {
                "today": datetime.now().strftime("%d-%b-%Y"),
                "chart_data": json.dumps(full_chart, indent=2),
            }
            step = prompts_config["initial_reading_steps"][0]
            prompt_text = _safe_substitute(step["prompt"], variables)

            create_pending_reading(reading_id, user_id, chart_id, prompt_text)
            save_ai_question(user_id, question or "Initial reading", "comprehensive", f"pending:{reading_id}")

            remaining = 25 - get_question_count_today(user_id)
            return jsonify({
                "status": "queued",
                "reading_id": reading_id,
                "remaining": remaining,
            })
        else:
            # Follow-up / normal question — real-time via Vertex AI
            import vertexai
            from vertexai.generative_models import GenerativeModel

            project_id = os.environ.get("GCP_PROJECT", "grahalogic")
            location = os.environ.get("GCP_LOCATION", "us-central1")
            vertexai.init(project=project_id, location=location)

            model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

            full_chart = dict(chart_data)
            if "dasha" in full_chart and "maha" in full_chart.get("dasha", {}):
                full_chart["dasha"] = dict(full_chart["dasha"])
                full_chart["dasha"]["maha"] = _relevant_maha_periods(full_chart["dasha"]["maha"])
            full_chart["current_date"] = datetime.now().strftime("%d-%b-%Y")

            variables = {
                "question": question,
                "categories": ", ".join(LIFE_CATEGORIES),
                "today": datetime.now().strftime("%d-%b-%Y"),
                "conversation": build_conv_context(conversation),
                "raw_chart_data": chart_data,
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
