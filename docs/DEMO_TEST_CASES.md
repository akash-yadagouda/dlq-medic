# DLQ Medic: demo test cases and recording script

Every test ends with `./scripts/verify.sh`, which reads the outcome straight from Kafka, SQL Server and Mailpit (not from the agent).

## Before recording (pre-flight)

- [ ] `docker compose up -d`: kafka, sqlserver and mailpit are healthy
- [ ] **order-consumer** and **dlq-medic-mcp** restarted on the latest code (`./mvnw spring-boot:run`; MCP log shows `Registered tools: 10`)
- [ ] TrueForge running with `OUTBOUND_URL_ALLOWED_HOSTS='["localhost"]'`
- [ ] Agent on the demo model: `MODEL=openai/gpt-5-6-terra ./scripts/create-agent.sh`
- [ ] Daytona warmed up (one small sandbox command in a scratch chat)
- [ ] Old sessions ignored: **only approve cards in the chat you are recording**
- [ ] Browser tabs ready: TrueForge (localhost:8790) · Mailpit (localhost:8025) · GitHub README · Offset Explorer · VS Code (SQL connection as `sa`, second connection as `dlq_medic`)
- [ ] **Never show secrets:** don't open TrueForge Settings (API keys), don't open `.env`

Reset between takes: stop order-consumer (Ctrl+C) → `./scripts/reset-demo.sh` (add `--forget` to wipe memory) → start order-consumer and wait until the `→ orders.DLT` lines stop.

## Test cases

| ID | What it proves | Rubric |
|---|---|---|
| TC-01 | Full job end to end on a fresh incident | Harness 30 · Working 25 |
| TC-02 | The agent remembers a past incident | Harness · Job worth delegating |
| TC-03 | Human denies an email draft, agent rewrites | Where it stops 20 |
| TC-04 | Human denies the bulk replay, nothing more is replayed | Where it stops 20 |
| TC-05 | Unattended run from a Schedule waits for a human | Harness · Job worth delegating |
| TC-06 | Unattended "All clear" when nothing is new | Harness |
| TC-07 | The agent's DB login cannot touch business data | Where it stops 20 |
| TC-08 | Canary gate: bulk refused until the canary lands (advanced) | Where it stops 20 |

### TC-01: fresh incident, full run
**Setup:** reset with `--forget`.
**Steps:** New chat → dlq-medic → `orders.DLT is filling up. Sort it out.` → approve the canary → approve the bulk → approve the email.
**Expect, in order:**
1. "No similar past incident"
2. the sandbox loads 214 messages and classifies them: 150 amount-as-string · 48 epoch createdAt · 16 missing customerId
3. the **error-type briefing card**: every type with a real example, action and canary count, plus "already processed: 12"
4. ⏸ canary card (about 4 orders = 2 per fixable type) → per-type check, 1 charge each
5. ⏸ bulk card → park 16 → ⏸ email draft to checkout-team
6. outcome card → download links for `incident-report.md` and `parked-messages.csv` → `record_incident`

**verify.sh:** orders 698 · charges 698 · **double charges 0 ✅** · replay items `SENT 186 · SKIPPED_ALREADY_PROCESSED 12` · canary `amount_string_to_number 2 · epoch_millis_to_iso8601 2` · parked 16 · 1 email (1 attachment) · memory 1.

### TC-02: it remembers
**Setup:** reset **without** `--forget` (after TC-01).
**Steps:** same prompt in a new chat.
**Expect:** right after assessing, "Seen before: incident #N (similarity 1.0) … same producer bug, the vetted fixes worked, 0 double charges". The briefing card shows a **Seen before** line. The rest as TC-01.
**verify.sh:** as TC-01, and memory goes up by 1.

### TC-03: deny the email, the agent rewrites it
**Steps:** run as TC-01. At the **email** card, click **Deny** with the reason *"Add a deadline of 24 hours and CC the on-call team in the text."*
**Expect:** the agent says what it changed and shows a new email card with the revised body → approve.
**verify.sh / Mailpit:** exactly **1** email, the revised one (nothing was sent on deny).

### TC-04: deny the bulk replay
**Steps:** run as TC-01. Approve the canary; at the **bulk** card, click **Deny** with the reason *"Stop after the canary: the producer team is still investigating."*
**Expect:** the agent stops replaying and reports what was done.
**verify.sh:** replay items show **SENT = the canary only** (about 4), and **double charges 0**. Reset afterwards.

### TC-05: unattended scheduled run (incident)
**Setup:** reset.
**Steps:** `./scripts/create-schedule.sh --run-now`. Don't type anything.
**Expect:** a new **Scheduled run** session appears in TrueForge. The agent triages on its own and **stops at the canary approval card**. Open the session and approve, as the on-call engineer.

### TC-06: unattended scheduled run (all clear)
**Setup:** after TC-01 or TC-02 has completed.
**Steps:** `./scripts/create-schedule.sh --run-now`.
**Expect:** one `get_pipeline_health` call, then **"All clear"** (dltUnhandled 0, parked 16, lag 0). It costs a fraction of a cent.

### TC-07: least privilege, live
**Steps:** in VS Code, connected as `dlq_medic`:
```sql
UPDATE dbo.orders SET amount = 0 WHERE order_id = 'ORD-10001';
DELETE FROM dbo.agent_audit_log;
```
**Expect:** both fail with *"The UPDATE/DELETE permission was denied"*.

### TC-08: canary gate (advanced, optional)
**Steps:** run as TC-01. After approving the canary, **stop order-consumer** before the agent verifies it.
**Expect:** the canary orders are not in the DB, so the agent's per-type check fails and it stops. If a bulk replay is attempted, the server refuses it: *"Canary not verified"*. Restart the consumer and reset afterwards.

## Recording script (about 5–6 minutes, record TC-02 as the main take)

A real run takes about 8 minutes with approvals. Record it all, then trim the waits or speed them up 2×.

| Time | Screen | Say |
|---|---|---|
| 0:00 | Offset Explorer: `orders.DLT`, 214 messages, open one → headers | "3 AM: producer 2.3.1 broke 214 orders. Skipping loses orders; a blind replay double-charges." |
| 0:25 | GitHub README: harness diagram | "DLQ Medic runs on TrueForge: MCP tools, a sandbox, a git-backed skill, approval gates." |
| 0:45 | TrueForge: new chat, prompt | "One sentence starts it; a Schedule can start it with no human at all." |
| 1:00 | Recall line: "Seen before…" | "It remembers the last incident, and the facts in memory come from the server." |
| 1:20 | Sandbox exec + classification | "It writes and runs its own Python in a Daytona sandbox; credentials never go there." |
| 1:50 | Error-type briefing card | "Before anything is replayed, I see every error type, with a real example and the fix." |
| 2:20 | ⏸ Canary card → approve → per-type check | "The server picks 2 orders from every error type. The bulk is blocked until they land exactly once." |
| 3:00 | ⏸ Bulk card → approve | "Only now does the rest go out." |
| 3:20 | ⏸ Email draft card → approve → Mailpit inbox + CSV | "Sending an email is irreversible, so I read the draft first. It can only email allow-listed teams." |
| 3:55 | Outcome card → download incident-report.md | |
| 4:15 | Terminal: `./scripts/verify.sh` | "698 orders, 0 double charges, checked from the database, not the agent." |
| 4:35 | VS Code as `dlq_medic`: UPDATE → permission denied | "Even a confused model can't touch business data: the database won't let it." |
| 4:55 | Terminal: `create-schedule.sh --run-now` → "All clear" | "Every hour it checks again, and costs a fraction of a cent when nothing is wrong." |
| 5:15 | Close on the README | "The model decides, the sandbox computes, the server enforces, TrueForge supervises, and I approve." |
