# Plan card, outcome card and incident files

All numbers come from tool results or sandbox output. Render cards with Generative UI (a fenced ```openui block; call `get_openui_instructions` first if you have not yet).

## 1. Error-type briefing (plan card, step 6b, before the first approval)
One Card titled "Error types found: <batchId>", so the developer sees every kind of failure before anything is replayed:
- a **donut PieChart** of the DLT messages by error type (label = short name, value = count)
- a **Table** with one row per error type, including the unfixable ones, with columns:
  Error type · Messages · Example (orderId: bad value → fixed value) · Action · In canary

Action is one of: `replay via <fix name>`, `park: <reason>`. Add one more row "Already processed" with its count and action `skip (would double-charge)`.
Take the fixable rows, their examples and canary counts from stage_replay `errorTypes`; take the unfixable rows from your sandbox classification (canary 0).
Always add a "Memory" line with the recall `headline` verbatim ("Seen before: incident #…" or "New incident: …").
Below it, one line: "Next: a canary of <canarySize> orders (2 from each fixable error type) needs your approval; the rest waits until every error type's canary is verified."

## 2. Outcome card (step 11)
One Card titled "Incident resolved: orders.DLT" with a row of KPI tiles:
DLT assessed · Replayed · Skipped (already processed) · Parked · Double charges (should be 0, taken from find_existing_orders charge counts)
plus a one-line root cause and the batch id.

## 3. Incident files (step 11)
Write both files in the sandbox, then list them in a ```sandbox_artifacts block.

**/tmp/dlq-medic/incident-report.md**
1. Title, date/time (UTC) and batch id
2. **Root cause:** the producer version and what changed in its messages (one line per error pattern, with counts)
3. **Outcome table:**

| Metric | Count |
|---|---:|
| DLT messages assessed | |
| Replayed (canary + bulk) | |
| Skipped as already processed | |
| Parked for the owning team | |
| Rejected at staging | |

4. **Safety checks:** the canary per error type (orderIds and charge counts), and the double-charge result
5. **Timeline:** one line per step (assess, classify, stage, canary approved, canary verified, bulk approved, parked, team emailed)
6. **Recommendation:** one change that would prevent a repeat (for example a contract test in the producer's CI)

**/tmp/dlq-medic/parked-messages.csv**: the handoff for the owning team.
Columns: `messageId,orderId,error,producerVersion,reason`, one row per parked message.
