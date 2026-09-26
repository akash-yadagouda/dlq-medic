# Sample output: incident report written by the agent

This is `incident-report.md` exactly as DLQ Medic wrote it in its Daytona sandbox and offered as a download (run of 26 Sep 2026, batch `b-6dc4a698`, model gpt-5.6-luna). The agent also sent the owning team an email with `parked-messages.csv` attached, after human approval.

> **Improvement made after this run:** here the agent parked the 12 already-processed orders as "do-not-replay safety records", so they would stop showing as unhandled. Parking is meant for the owning team, so we changed the runbook and the server: already-processed orders now go through `stage_replay`, where the server records them as `SKIPPED_ALREADY_PROCESSED`, and `park_messages` refuses them. Current runs park only the 16 unfixable messages.

---

# orders.DLT incident report

- **UTC:** 2026-09-26T10:12:16.410Z
- **Batch:** `b-6dc4a698`

## Root cause
Producer **2.3.1** changed order serialization:
- 150 messages: `amount` was a JSON string instead of a number.
- 48 messages: `createdAt` was epoch milliseconds instead of an offset-bearing ISO-8601 string.
- 16 messages: required `customerId` was missing.

## Outcome
| Metric | Count |
|---|---:|
| DLT messages assessed | 214 |
| Replayed (canary + bulk) | 186 |
| Skipped as already processed | 12 |
| Parked for the owning team (missing customerId) | 16 |
| Parked as do-not-replay safety records | 12 |
| Rejected at staging | 0 |

## Safety checks
- Canary `amount_string_to_number`: ORD-10502 and ORD-10506 were present with exactly 1 charge each.
- Canary `epoch_millis_to_iso8601`: ORD-10558 and ORD-10566 were present with exactly 1 charge each.
- No double charges were observed in the canary. The 12 orders already present were never replayed.

## Timeline
- Assessed 214 unhandled records; consumer lag was 0.
- Classified and rehearsed vetted, format-only repairs.
- Excluded 12 processed orders and staged 186 orders in `b-6dc4a698`.
- Canary approved and sent: 4 orders.
- Canary verified: all 4 had exactly one charge.
- Bulk approved and completed: 182 orders.
- Parked 16 missing-customer records and 12 do-not-replay processed records.
- Emailed checkout-team with 16 source-data handoff records.

## Recommendation
Add producer contract tests in CI for numeric `amount`, offset-bearing ISO-8601 `createdAt`, and required `customerId` before deployment.
