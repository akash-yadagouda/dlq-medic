# DLQ Medic

**An on-call agent for Kafka dead-letter topics.** When orders pile up in `orders.DLT`, DLQ Medic works out why, repairs what can be repaired safely, replays it without double-charging anyone, and hands off what it can't fix. It stops for a human before anything irreversible.

Built on [TrueForge](https://github.com/truefoundry/trueforge) for the Polaris × TrueFoundry *Agents That Act* hackathon.

## The job

It's 3 AM. checkout-service v2.3.1 shipped a serialization bug, and 214 orders are in the dead-letter topic. The on-call engineer has two bad options:
- **Skip them:** lost orders.
- **Replay them blindly:** the order consumer isn't idempotent, so any order that was already re-sent gets **charged twice**.

DLQ Medic does the careful version of this job:

| Step | Where it runs |
|---|---|
| Check pipeline health, lag and the affected producer versions | MCP tool (Kafka Admin) |
| Load all 214 DLT messages, group them by error pattern | **Sandbox** (Python written by the agent; Code Mode reads through the harness) |
| Rehearse the fix and validate every result against the contract | **Sandbox** |
| Check for orders that were already processed | Sandbox → read-only MCP tool (SQL Server) |
| Stage the batch: the server applies a **vetted fix** and re-validates | MCP tool |
| **⏸ Canary: replay 5** | **Human approval** |
| Verify each canary order exists with exactly 1 charge | MCP tool |
| **⏸ Replay the rest** | **Human approval** |
| Park what can't be fixed (e.g. missing customerId) for the owning team | MCP tool |
| Report root cause, counts and a recommendation | Agent |

Result on the seeded incident: **186 replayed, 12 skipped as already processed, 16 parked, 0 double charges.**

## Architecture

```
            ┌──────────────────────── TrueForge (local, :8790) ───────────────────────┐
  you ────▶ │ agent loop · approval gates · sessions             ──▶ LLM provider API │
            └───────┬──────────────────────────────────────────────┬─────────────────┘
                    │ MCP (Streamable HTTP)                        │ sandbox tool
                    ▼                                              ▼
     dlq-medic-mcp  (Spring AI, :8081/mcp)           Daytona sandbox (cloud)
     Kafka Admin/Consumer/Producer + JDBC            Python written by the agent;
     vetted fixes · guardrails · audit log           read-only MCP calls bridged
                    │                                back through the harness
                    ▼
     docker-compose: Kafka (KRaft) :9092 · SQL Server :1433
                    ▲
     order-consumer (Spring Boot) ── contract violations ──▶ orders.DLT (kafka_dlt-* headers)
```

| Component | Role | Knows | Never sees |
|---|---|---|---|
| **LLM** (via TrueForge) | Decides the next step | Instructions, tool descriptions, tool results | DB/Kafka credentials. Can't act except through tools. |
| **TrueForge** | Runs the loop, pauses for approval, logs every event | The whole transcript; model and Daytona keys | DB/Kafka credentials |
| **Daytona sandbox** | Runs the agent's own code | The code and the data given to it | All credentials; can only call **read-only** tools |
| **dlq-medic-mcp** | The only way into Kafka and SQL Server; enforces the rules | Tool calls and arguments | The conversation, the model's reasoning |
| **Human** | Approves or denies anything irreversible | Chat, approval cards, session log | n/a |

## Where it stops

Safety is enforced **in the MCP server and the database**, not in the prompt.

| Tool | MCP annotation | Gate |
|---|---|---|
| `get_pipeline_health`, `peek_dlt`, `find_existing_orders`, `get_audit_log` | read-only | none (also callable from sandbox Code Mode) |
| `stage_replay` | write, non-destructive | none: it only writes our own staging tables |
| `park_messages` | write, non-destructive | none: it only adds data, and the DLT keeps its copy |
| **`execute_replay`** | **destructive** | **human approval, every call** |

**Server-side rules, independent of what the model says:**
- **Vetted fixes only.** The model picks a fix by name (`amount_string_to_number`, `epoch_millis_to_iso8601`) for a list of message IDs. The server applies it to the original payload, so **the model never writes the bytes that reach production**.
- Every staged payload is **re-validated** against the consumer's contract, and the **orderId can't change**.
- **Canary enforced by the server.** The first `execute_replay` on a batch sends at most 5. Further calls are **refused until every canary order is in the orders table**.
- **Duplicate check at stage time and again at send time.** Orders that were already processed are skipped.
- The replay target topic is a constant, not a parameter.
- **Not exposed at all:** delete topic, reset offsets, raw produce, arbitrary SQL.

**The database backs this up (SQL Server least privilege):**
- The `dlq_medic` login is **read-only** on `orders` and `payment_ledger`.
- It is **DENY UPDATE/DELETE** on its own `agent_audit_log`, so the agent cannot erase its trail.

**TrueForge backs this up too:**
- `require_approval_for_tools: ["@destructive"]`.
- Code Mode refuses any non-read-only tool, so writes can't be hidden inside a sandbox script.
- The SSRF guard stays on, with exactly one host allow-listed (`localhost`).

## Run it

**Prerequisites**
- Docker Desktop (4 GB is enough; on Apple Silicon turn on Rosetta, because SQL Server is amd64-only)
- Java 21
- Node 22.14+
- Python 3
- An LLM API key (OpenAI recommended)
- A [Daytona](https://daytona.io) API key with *Sandboxes* access and *Snapshots write* permission

**1. Infrastructure and the incident**
```bash
./scripts/init-env.sh        # generates local-only DB passwords into .env (gitignored)
docker compose up -d         # Kafka + SQL Server; creates topics, schema and least-privilege logins
./scripts/reset-demo.sh      # publishes the incident: 500 healthy + 214 broken + 12 hotfix re-sends
```

**2. Services** (two terminals)
```bash
cd order-consumer && ./mvnw spring-boot:run     # processes orders; 214 land in orders.DLT
cd dlq-medic-mcp  && ./mvnw spring-boot:run     # MCP server on http://localhost:8081/mcp
```

**3. TrueForge** (third terminal). The allowlist lets TrueForge reach the local MCP server; everything else stays blocked.
```bash
OUTBOUND_URL_ALLOWED_HOSTS='["localhost"]' npx --yes @truefoundry/trueforge@latest
```
Then open http://localhost:8790:
- **Settings → Models:** add your provider key.
- **Settings → Sandbox providers:** add your Daytona key.

**4. Create the agent**
```bash
MODEL=openai/gpt-5-6-terra ./scripts/create-agent.sh   # registers the MCP server + creates the dlq-medic agent
```

**5. Run it**
- In the UI: **New chat → dlq-medic →** `orders.DLT is filling up. Sort it out.` Approve the canary, then the bulk replay.
- Or headless, the same API an alerting system would call:
  ```bash
  python3 scripts/run-agent.py --msg "orders.DLT is filling up. Sort it out." --on-approval ask
  ```

**Reset between runs:** stop `order-consumer`, run `./scripts/reset-demo.sh`, then start it again.

## Repository

| Path | What |
|---|---|
| `docker-compose.yml`, `infra/sql/init.sql` | Kafka KRaft, SQL Server, topics, schema, least-privilege logins |
| `order-consumer/` | The service being protected: strict contract → DLT; non-idempotent upsert + charge |
| `dlq-medic-mcp/` | The MCP server: 7 tools, vetted fixes, guardrails, audit log |
| `agent/instructions.md` | The runbook the agent follows |
| `scripts/` | `init-env`, `seed`, `reset-demo`, `create-agent`, `run-agent` |

## AI assistance disclosure

Built with help from **Claude Code** (Anthropic) for design discussion, code generation and debugging. The architecture, the safety model and every line of code were reviewed, and can be explained, by the author.
