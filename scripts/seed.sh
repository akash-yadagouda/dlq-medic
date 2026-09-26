#!/usr/bin/env bash
# Publishes the demo incident to the `orders` topic.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/seed.py | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders \
  --property parse.key=true --property parse.headers=true
