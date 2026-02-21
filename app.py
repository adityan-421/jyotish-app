#!/usr/bin/env python3
"""Flask app for Vedic Jyotish chart generation."""

import os
import json
import functools
import logging

from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from jyotish_engine import compute_chart, compute_btr
from zoneinfo import ZoneInfo
from datetime import datetime
from database import (
    init_db, reset_pool, upsert_user, save_chart, get_charts, get_chart, delete_chart,
    update_chart, count_charts, get_question_count_today, save_ai_question, get_ai_history,
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
    for turn in conversation[-8:]:
        role = turn.get("role", "user").upper()
        ctx += f"{role}: {turn.get('text', '')}\n\n"
    ctx += "Continue the conversation naturally. Reference prior discussion where relevant.\n\n"
    return ctx


def _run_prompt_chain(model, steps, variables):
    """Execute a sequence of prompt steps, returning the final output."""
    final_output = None
    for step in steps:
        # Format the prompt template with current variables
        prompt_text = step["prompt"].format_map(_SafeFormatDict(variables))

        response = model.generate_content(prompt_text)
        result_text = response.text.strip()

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


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            return jsonify({"error": "Login required"}), 401
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


@app.route("/api/me")
def api_me():
    user = session.get("user")
    if user:
        remaining = 10 - get_question_count_today(user["id"])
        return jsonify({"user": user, "ai_remaining": max(remaining, 0)})
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

    user = session["user"]
    user_id = user["id"]
    upsert_user(user_id, user.get("email", ""), user.get("name", ""), user.get("picture", ""))
    chart_id, error = save_chart(user_id, name, input_data, chart_data)
    if error:
        return jsonify({"error": error}), 400

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
def api_btr_ask():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body provided"}), 400

    mode = data.get("mode")
    if mode not in ("questions", "analyze"):
        return jsonify({"error": "Invalid mode. Must be 'questions' or 'analyze'."}), 400

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
            result = _run_prompt_chain(model, steps, variables)
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

        else:  # analyze
            qa_pairs = data.get("qa_pairs") or []
            if not qa_pairs:
                return jsonify({"error": "qa_pairs required for analyze mode"}), 400
            variables["qa_pairs"] = json.dumps(qa_pairs, indent=2)

            steps = prompts_config.get("btr_analyze_steps", [])
            if not steps:
                return jsonify({"error": "BTR analysis prompts not configured"}), 500
            result = _run_prompt_chain(model, steps, variables)
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

    except Exception as e:
        error_type = type(e).__name__
        logger.error("BTR AI error (%s): %s", error_type, str(e))
        return jsonify({"error": "Failed to generate BTR analysis. Please try again later."}), 500


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
    charts_to_include = ["D1"]
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

    # Rate limit: 10 questions per day
    if get_question_count_today(user_id) >= 10:
        return jsonify({"error": "Daily limit reached. You can ask 10 questions per day."}), 429

    # Ensure user exists in DB
    upsert_user(user_id, user.get("email", ""), user.get("name", ""), user.get("picture", ""))

    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel

        project_id = os.environ.get("GCP_PROJECT", "grahalogic")
        location = os.environ.get("GCP_LOCATION", "us-central1")
        vertexai.init(project=project_id, location=location)

        prompts_config = load_prompts()
        model = GenerativeModel(prompts_config.get("model", "gemini-2.5-flash"))

        # Seed template variables
        variables = {
            "question": question,
            "categories": ", ".join(LIFE_CATEGORIES),
            "today": datetime.now().strftime("%d-%b-%Y"),
            "conversation": build_conv_context(conversation),
            "raw_chart_data": chart_data,
        }

        reading = _run_prompt_chain(model, prompts_config["steps"], variables)

        category = variables.get("category", "other")

        # Save to DB
        save_ai_question(user_id, question, category, reading)

        remaining = 10 - get_question_count_today(user_id)
        return jsonify({"category": category, "reading": reading, "remaining": remaining})

    except Exception as e:
        error_type = type(e).__name__
        logger.error("AI reading error (%s): %s", error_type, str(e))
        return jsonify({"error": "Failed to generate reading. Please try again later."}), 500


# Try to init DB at startup, but don't block if it fails —
# init_db() will be retried on the first DB-touching request.
try:
    init_db()
except Exception:
    pass


@app.before_request
def _ensure_db():
    """Lazily retry DB init if it failed at startup."""
    init_db()


@app.after_request
def _add_cache_headers(response):
    """Add cache-control headers for the main page."""
    if request.path == "/" and response.status_code == 200:
        response.headers["Cache-Control"] = "public, max-age=300"
    return response


if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 8080)))
