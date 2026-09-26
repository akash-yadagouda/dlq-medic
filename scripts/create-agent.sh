#!/usr/bin/env bash
# Creates (or replaces) the dlq-medic agent in a local TrueForge.
# Usage: MODEL=openai/gpt-5-6-terra SKILL_REF=main ./scripts/create-agent.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TF=${TRUEFORGE_URL:-http://localhost:8790}
MODEL=${MODEL:-openai/gpt-5-6-terra}
SKILL_REPO=${SKILL_REPO:-https://github.com/akash-yadagouda/dlq-medic}
SKILL_REF=${SKILL_REF:-main}   # pin a tag or commit SHA for production

# 1. Register the MCP server (idempotent PUT)
curl -sf -X PUT "$TF/api/v1/settings/mcp-servers" -H 'Content-Type: application/json' -d '{"manifest":{"type":"remote","name":"dlq-medic","url":"http://localhost:8081/mcp","description":"Kafka orders pipeline operations: pipeline health, dead-letter (orders.DLT) inspection, idempotency checks against the orders DB, staged replays with server-enforced canary, parking, audit log."}}' > /dev/null

# 2. Register the git-backed runbook skill (idempotent PUT); TrueForge materializes it in the sandbox on demand
python3 - "$SKILL_REPO" "$SKILL_REF" <<'PY' | curl -sf -X PUT "$TF/api/v1/settings/skills" -H 'Content-Type: application/json' -d @- > /dev/null
import json, re, sys
front = open("skills/dlq-triage/SKILL.md").read().split("---")[1]
description = re.search(r"^description:\s*(.+)$", front, re.M).group(1).strip()
print(json.dumps({"manifest": {"type": "git", "name": "dlq-triage", "url": sys.argv[1],
                               "path": "skills/dlq-triage", "ref": sys.argv[2], "description": description}}))
PY

# 3. Build the agent manifest: role + safety rules, the skill, only destructive tools gated, sandbox on
BODY=$(python3 - "$MODEL" <<'PY'
import json, sys
print(json.dumps({
  "name": "dlq-medic",
  "description": "Triages orders.DLT, repairs and safely replays dead-lettered orders with a human-approved canary.",
  "manifest": {
    "model": {"name": sys.argv[1]},
    "instructions": open("agent/instructions.md").read(),
    "mcp_servers": [{"name": "dlq-medic", "enable_tools": ["@all"], "require_approval_for_tools": ["@destructive"], "preload": True}],
    "skills": [{"name": "dlq-triage", "preload": False}],
    "config": {"sandbox": {"enabled": True}, "iteration_limit": 40}
  }
}))
PY
)

# 4. Create, or update if it already exists
ID=$(curl -sf "$TF/api/v1/agents" | python3 -c "import json,sys; print(next((a['id'] for a in json.load(sys.stdin)['data'] if a['name']=='dlq-medic'), ''))")
if [[ -n "$ID" ]]; then
  curl -sf -X PUT "$TF/api/v1/agents/$ID" -H 'Content-Type: application/json' -d "$(echo "$BODY" | python3 -c 'import json,sys; b=json.load(sys.stdin); b.pop("name"); print(json.dumps(b))')" > /dev/null
  echo "updated agent dlq-medic ($ID) on $MODEL with skill dlq-triage@$SKILL_REF"
else
  curl -sf -X POST "$TF/api/v1/agents" -H 'Content-Type: application/json' -d "$BODY" > /dev/null
  echo "created agent dlq-medic on $MODEL with skill dlq-triage@$SKILL_REF"
fi
