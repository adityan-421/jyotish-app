#!/usr/bin/env bash
set -euo pipefail

PROJECT=grahalogic
REGION=us-central1
SERVICE=jyotish-app
JOB=backfill-life-readings

echo "==> [1/4] Deploying latest code (bakes backfill script into the image)..."
gcloud run deploy "$SERVICE" \
  --source . \
  --project "$PROJECT" \
  --region "$REGION" \
  --allow-unauthenticated

echo ""
echo "==> [2/4] Reading image URL + env vars from deployed service..."
IMAGE=$(gcloud run services describe "$SERVICE" \
  --project "$PROJECT" \
  --region "$REGION" \
  --format="value(spec.template.spec.containers[0].image)")
echo "    Image: $IMAGE"

# Extract plain-value env vars (skips Secret Manager refs — handled below)
ENV_VARS=$(gcloud run services describe "$SERVICE" \
  --project "$PROJECT" \
  --region "$REGION" \
  --format=json \
  | python3 -c "
import json, sys
svc = json.load(sys.stdin)
envs = (svc.get('spec',{})
           .get('template',{})
           .get('spec',{})
           .get('containers',[{}])[0]
           .get('env',[]))
pairs = []
for e in envs:
    if 'value' in e:          # plain value — include
        v = e['value'].replace(',', '\,')   # escape commas
        pairs.append(e['name'] + '=' + v)
    elif 'valueFrom' in e:    # Secret Manager ref — warn
        import sys as _s
        print(f\"    [warn] Secret-Manager env var skipped: {e['name']} — add --set-secrets manually if needed\", file=_s.stderr)
print(','.join(pairs))
")
echo "    Env vars captured."

echo ""
echo "==> [3/4] Creating / updating Cloud Run job..."
if gcloud run jobs describe "$JOB" --project "$PROJECT" --region "$REGION" &>/dev/null; then
  echo "    Job exists — updating."
  gcloud run jobs update "$JOB" \
    --image "$IMAGE" \
    --project "$PROJECT" \
    --region "$REGION" \
    --command python3 \
    --args "backfill_life_readings.py" \
    --set-env-vars "$ENV_VARS" \
    --max-retries 0 \
    --task-timeout 3600
else
  echo "    Job not found — creating."
  gcloud run jobs create "$JOB" \
    --image "$IMAGE" \
    --project "$PROJECT" \
    --region "$REGION" \
    --command python3 \
    --args "backfill_life_readings.py" \
    --set-env-vars "$ENV_VARS" \
    --max-retries 0 \
    --task-timeout 3600
fi

echo ""
echo "==> [4/4] Executing job (--wait streams until completion)..."
gcloud run jobs execute "$JOB" \
  --project "$PROJECT" \
  --region "$REGION" \
  --wait

echo ""
echo "==> Backfill complete."
