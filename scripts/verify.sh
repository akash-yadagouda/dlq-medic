#!/usr/bin/env bash
# Prints the outcome of a run from the systems themselves (not from the agent):
# orders, charges, double charges, replay items, parked messages, emails and memory.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

sql() {
  docker exec -e P="$MSSQL_SA_PASSWORD" sqlserver bash -c \
    "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"\$P\" -C -d orders_db -h -1 -W -Q \"SET NOCOUNT ON; $1\"" | tr -d '\r'
}
topic_count() {
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}

orders=$(sql "SELECT COUNT(*) FROM dbo.orders")
charges=$(sql "SELECT COUNT(*) FROM dbo.payment_ledger")
doubles=$(sql "SELECT COUNT(*) FROM (SELECT order_id FROM dbo.payment_ledger GROUP BY order_id HAVING COUNT(*) > 1) d")
items=$(sql "SELECT ISNULL(STRING_AGG(CONCAT(status, ' ', n), ' · '), 'none') FROM (SELECT status, COUNT(*) n FROM dbo.replay_item GROUP BY status) s")
canary=$(sql "SELECT ISNULL(STRING_AGG(CONCAT(fix, ' ', n), ' · '), 'none') FROM (SELECT fix, COUNT(*) n FROM dbo.replay_item WHERE sent_phase = 'CANARY' GROUP BY fix) c")
memory=$(sql "SELECT COUNT(*) FROM dbo.incident_memory")
emails=$(curl -s http://localhost:8025/api/v1/messages 2>/dev/null | python3 -c "
import json, sys
try:
    msgs = json.load(sys.stdin).get('messages', [])
except Exception:
    print('Mailpit not reachable'); sys.exit()
print(len(msgs), '|', '; '.join(f\"{m['To'][0]['Address']}: {m['Subject']} ({m.get('Attachments', 0)} attachment)\" for m in msgs[:3]) or 'none')")

check() { [[ "$1" == "0" ]] && echo "✅" || echo "❌"; }

echo "DLQ Medic: outcome check (read straight from Kafka, SQL Server and Mailpit)"
echo "  orders.DLT ............ $(topic_count orders.DLT)"
echo "  orders in DB .......... $orders"
echo "  payment charges ....... $charges"
echo "  double charges ........ $doubles $(check "$doubles")"
echo "  replay items .......... $items"
echo "  canary by fix ......... $canary"
echo "  orders.parked ......... $(topic_count orders.parked)"
echo "  emails (Mailpit) ...... $emails"
echo "  incident memory ....... $memory incident(s)"
