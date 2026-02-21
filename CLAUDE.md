# Jyotish App

## Project Overview
Vedic astrology (Jyotish) web application built with Flask and pyswisseph.

## Tech Stack
- **Backend:** Python 3.11, Flask, pyswisseph, gunicorn
- **Frontend:** Vanilla HTML/JS/CSS (single-page in `templates/index.html`)
- **Engine:** `jyotish_engine.py` — core calculation logic

## Key Files
- `app.py` — Flask routes and API endpoints
- `jyotish_engine.py` — Vedic astrology calculation engine
- `templates/index.html` — Full frontend (HTML + CSS + JS)
- `requirements.txt` — Python dependencies
- `Dockerfile` — Container config (Python 3.11-slim, gunicorn on port 8080)

## Deployment — Google Cloud Run

**GCP Project:** `grahalogic`

### Deploy Command
```bash
gcloud run deploy jyotish-app \
  --source . \
  --project grahalogic \
  --region us-central1 \
  --allow-unauthenticated
```

This builds the container using the Dockerfile and deploys to Cloud Run in one step.

### Dockerfile Details
- Base: `python:3.11-slim`
- Installs `build-essential` (needed for pyswisseph C compilation)
- Runs gunicorn with 1 worker, 2 threads, 60s timeout
- Listens on `PORT` env var (default 8080, Cloud Run sets this automatically)

### Manual Build & Deploy (alternative)
```bash
# Build and push to Artifact Registry
gcloud builds submit --tag gcr.io/grahalogic/jyotish-app --project grahalogic

# Deploy the image
gcloud run deploy jyotish-app \
  --image gcr.io/grahalogic/jyotish-app \
  --project grahalogic \
  --region us-central1 \
  --allow-unauthenticated
```

## Local Development
```bash
pip install -r requirements.txt
python app.py
```
