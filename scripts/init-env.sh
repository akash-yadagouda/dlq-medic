#!/usr/bin/env bash
# Creates .env with random local-only passwords (never committed). Safe to re-run: keeps an existing .env.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then echo ".env already exists, leaving it untouched"; exit 0; fi
pw() { echo "Pw$(openssl rand -hex 12)X9"; }   # upper + lower + digits satisfies SQL Server password policy
cat > .env <<ENV
MSSQL_SA_PASSWORD=$(pw)
ORDER_SVC_PASSWORD=$(pw)
DLQ_MEDIC_PASSWORD=$(pw)
ENV
echo "wrote .env"
