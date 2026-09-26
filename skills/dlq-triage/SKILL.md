---
name: dlq-triage
description: Runbook for a growing Kafka dead-letter topic (orders.DLT). Classify failures in the sandbox, rehearse fixes, prevent double charges, replay canary-first with human approval, park what cannot be fixed, and report. Use whenever orders.DLT is growing or a dead-letter incident is reported.
---

# DLT triage runbook

Work through these steps in order. The dlq-medic MCP tools are the only way to read or change Kafka and the orders database. Heavy data work happens in the sandbox.

Reference files in this skill (read them from the sandbox when a step points to them):
- `references/orders-contract.md`: the orders message contract and the catalogue of vetted server fixes
- `references/report-template.md`: the plan card, the outcome card and the incident files

## Visuals and files for the human
- **Generative UI:** call `get_openui_instructions` once, then render cards and charts in a fenced ```openui block. Use numbers you computed, never estimates.
- **Downloads:** write files under `/tmp/dlq-medic/` in the sandbox, then list them in a fenced ```sandbox_artifacts block, one `[label](/absolute/path)` per line.

## Sandbox Code Mode
In sandbox Python: `import asyncio` and `from mcp_client import call_tool`, then
`asyncio.run(call_tool('dlq-medic', '<tool>', {...}))` returns the parsed result.
Code Mode can call **read-only** tools only (get_pipeline_health, peek_dlt, find_existing_orders, get_audit_log). Call write tools (stage_replay, execute_replay, park_messages) yourself.

## Steps
1. **Assess.** Call get_pipeline_health. Report DLT size, consumer lag, and which producer versions are affected.
   Then **recall**: call recall_similar_incidents. If a match has similarity ≥ 0.5, tell the human in one line what happened last time (date, root cause, fix, facts, the human's decisions) and apply its lessons. Memory is advice, not proof: still run every check below.
2. **Load the unhandled DLT messages into the sandbox.** Write one Python script that pages through `peek_dlt` (fromIndex / nextIndex, 50 per page) with Code Mode and saves every message to /tmp/dlt.json, printing only the total. peek_dlt returns only unhandled messages by default (not yet staged, replayed or parked), so a repeat run never re-processes an old incident.
3. **Classify in the sandbox.** Group the messages by error pattern (the error text with concrete values removed). Print a table: pattern, count, producer versions, one example orderId.
4. **Rehearse the fix in the sandbox.** For each error pattern, write a Python transform, apply it to every message in that pattern, and validate each result against `references/orders-contract.md`. Map each fixable pattern to one vetted server fix from that file. A pattern with no vetted fix, or one that would need invented data, is unfixable: park it. Print counts of fixable (per fix), failed validation and unfixable.
5. **Idempotency check.** In the sandbox, call find_existing_orders (up to 500 ids per call) for all fixable orderIds. Exclude any that already exist and report which ones. Print the final messageIds per fix as one compact JSON object, e.g. {"amount_string_to_number": ["0:3", ...], "epoch_millis_to_iso8601": [...]}, plus the unfixable messageIds.
6. **Stage.** Call stage_replay yourself in a single call: fixes = [{fix, messageIds}] for every group, utcOffset, and a reason that states the root cause. The server applies the vetted fix and re-validates; check its samplePayloads match your sandbox results. Report staged, skipped and rejected counts. Before the first replay, render the **plan card** from `references/report-template.md` so the human sees exactly what they are approving.
7. **Canary.** Call execute_replay(batchId, 5). This pauses for human approval.
8. **Verify the canary.** Call find_existing_orders for the canary orderIds. Every one must exist with exactly 1 charge. If not, stop and report.
9. **Replay the rest.** Call execute_replay(batchId, <remaining count>). This pauses for human approval.
10. **Park.** Call park_messages for the unfixable messages with a reason the owning team can act on.
11. **Report.** Render the **outcome card**, write the **incident files** and list them for download, all as described in `references/report-template.md`.
12. **Remember.** Call record_incident with the batchId, the patterns (exact errorPattern values from peek_dlt, with count and action), the root cause, every approval or denial the human gave (with reasons), and 1-2 lessons that would make the next similar incident faster or safer. The server adds the verified facts.

Use short status lines between steps, not long explanations.
