#!/usr/bin/env bash
# Wipes Kafka topics, the consumer group and all tables, then re-publishes the incident.
# Long-term incident memory survives the reset; pass --forget to wipe it too.
# Stop order-consumer first (the consumer group must be empty to reset it).
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

KAFKA="docker exec kafka /opt/kafka/bin"

if pgrep -f "order-consumer-.*\.jar|order-consumer.*spring-boot:run" >/dev/null; then
  echo "order-consumer is running: stop it first, then re-run this script." >&2
  exit 1
fi

echo "1/4 deleting topics"
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --if-exists --topic 'orders|orders\.DLT|orders\.parked'
for _ in $(seq 1 30); do
  [[ -z "$($KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E '^orders(\.DLT|\.parked)?$' || true)" ]] && break
  sleep 1
done

echo "2/4 recreating topics + resetting consumer group"
docker compose run --rm --no-deps kafka-init > /dev/null
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group order-service > /dev/null 2>&1 || true

echo "3/4 truncating tables"
docker exec -e P="$MSSQL_SA_PASSWORD" sqlserver bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$P" -C -b -d orders_db -Q "
  SET NOCOUNT ON;
  DELETE dbo.replay_item; DELETE dbo.replay_batch;
  TRUNCATE TABLE dbo.orders; TRUNCATE TABLE dbo.payment_ledger; TRUNCATE TABLE dbo.agent_audit_log;
  TRUNCATE TABLE dbo.notification_log;"'
if [[ "${1:-}" == "--forget" ]]; then
  docker exec -e P="$MSSQL_SA_PASSWORD" sqlserver bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$P" -C -b -d orders_db -Q "SET NOCOUNT ON; TRUNCATE TABLE dbo.incident_memory;"'
  echo "    incident memory wiped (--forget)"
fi

curl -s -X DELETE http://localhost:8025/api/v1/messages > /dev/null 2>&1 && echo "    Mailpit inbox emptied" || true

echo "4/4 publishing the incident"
./scripts/seed.sh 2>&1 | grep -v "deprecated"
echo "done: start order-consumer to process the backlog"
