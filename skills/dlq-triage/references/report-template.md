# Plan card, outcome card and incident files

All numbers come from tool results or sandbox output. Render cards with Generative UI (a fenced ```openui block; call `get_openui_instructions` first if you have not yet).

## 1. Plan card (step 6, before the first approval)
One Card titled "Replay plan: <batchId>" containing, side by side:
- a **donut PieChart** of the DLT messages by error pattern (label = short pattern name, value = count)
- a **Table** with columns: Error pattern · Messages · Producer version · Action

Action is one of: `replay via <fix name>`, `skip: already processed (<n>)`, `park: <reason>`.
If recall_similar_incidents found a match, add a "Seen before" line: incident id, date, similarity and what happened.
Below it, one line of text: "Next: a 5-message canary needs your approval; the rest waits until the canary is verified."

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

4. **Safety checks:** canary orderIds with their charge counts, and the double-charge result
5. **Timeline:** one line per step (assess, classify, stage, canary approved, canary verified, bulk approved, parked, team emailed)
6. **Recommendation:** one change that would prevent a repeat (for example a contract test in the producer's CI)

**/tmp/dlq-medic/parked-messages.csv**: the handoff for the owning team.
Columns: `messageId,orderId,error,producerVersion,reason`, one row per parked message.
