#!/usr/bin/env bash
# Creates (or updates) the unattended hourly DLT watch for the dlq-medic agent.
# Usage: ./scripts/create-schedule.sh            # create/update the hourly schedule
#        ./scripts/create-schedule.sh --run-now  # ...and trigger one run immediately (for demos)
set -euo pipefail
cd "$(dirname "$0")/.."
TF=${TRUEFORGE_URL:-http://localhost:8790}
NAME=dlt-watch

MANIFEST=$(python3 <<'PY'
import json
task = """Scheduled DLT watch (unattended run; a human reviews this session later).
1. Call get_pipeline_health.
2. If dltUnhandled is 0 and openBatches is 0: reply in one line "All clear" with the orders.DLT total, dltUnhandled, dltParked and consumer lag. Stop there; do not load any skill.
3. If openBatches is greater than 0: an incident is already in progress and waiting for human approval. Do not stage anything new. Reply with the open batch ids and statuses, and stop.
4. Otherwise: load the dlq-triage skill and handle the unhandled messages. Stop at each approval card as usual; the on-call human will approve or deny here."""
# TrueForge's minimum schedule interval is one hour.
print(json.dumps({"task": task, "cron": "0 * * * *", "timezone": "Asia/Kolkata", "status": "active"}))
PY
)

ID=$(curl -sf "$TF/api/v1/schedules" | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; d=d if isinstance(d,list) else d.get('items',[]); print(next((s['id'] for s in d if s['name']=='$NAME'), ''))")
if [[ -n "$ID" ]]; then
  curl -sf -X PUT "$TF/api/v1/schedules/$ID" -H 'Content-Type: application/json' -d "{\"name\":\"$NAME\",\"manifest\":$MANIFEST}" > /dev/null
  echo "updated schedule $NAME ($ID): hourly, Asia/Kolkata"
else
  ID=$(curl -sf -X POST "$TF/api/v1/schedules" -H 'Content-Type: application/json' -d "{\"agent_name\":\"dlq-medic\",\"name\":\"$NAME\",\"manifest\":$MANIFEST}" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['id'])")
  echo "created schedule $NAME ($ID): hourly, Asia/Kolkata"
fi

if [[ "${1:-}" == "--run-now" ]]; then
  RUN=$(curl -sf -X POST "$TF/api/v1/schedules/runs" -H 'Content-Type: application/json' -d "{\"schedule_id\":\"$ID\"}" | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(d['id'], d['status'])")
  echo "triggered run: $RUN (look for a new 'Scheduled run' session in TrueForge)"
fi
