#!/usr/bin/env bash

set -u

pass=0
fail=0

check() {
    local name="$1"
    local cmd="$2"
    printf "  %-55s " "$name"
    if eval "$cmd" >/dev/null 2>&1; then
        echo "PASS"
        pass=$((pass + 1))
    else
        echo "FAIL"
        fail=$((fail + 1))
    fi
}

echo
echo "=== Service health ==="
check "Kafka broker reachable" \
    "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list"
check "Kafka topic trades-raw exists" \
    "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic trades-raw"
check "Kafka topic whale-alerts exists" \
    "docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic whale-alerts"
check "Cassandra reachable" \
    "docker compose exec -T cassandra cqlsh -e 'DESCRIBE KEYSPACES'"
check "Cassandra keyspace 'crypto' exists" \
    "docker compose exec -T cassandra cqlsh -e \"USE crypto; DESCRIBE TABLES\""
check "Postgres reachable" \
    "docker compose exec -T postgres psql -U crypto -d crypto -c 'SELECT 1'"
check "Postgres trades table exists" \
    "docker compose exec -T postgres psql -U crypto -d crypto -c '\\d trades'"
check "API /health returns OK" \
    "curl -fsS http://localhost:8000/health | grep -q ok"

echo
echo "=== Data flow ==="
check "Cassandra trades has rows" \
    "docker compose exec -T cassandra cqlsh -e 'SELECT count(*) FROM crypto.trades;' | grep -Eq '\\s+[1-9][0-9]*'"
check "Postgres trades has rows" \
    "docker compose exec -T postgres psql -U crypto -d crypto -tA -c 'SELECT count(*) > 0 FROM trades' | grep -q t"
check "market_momentum has rows (wait 1+ min after start)" \
    "docker compose exec -T cassandra cqlsh -e 'SELECT count(*) FROM crypto.market_momentum;' | grep -Eq '\\s+[1-9][0-9]*'"
check "hourly_reports has rows (wait for batch run)" \
    "docker compose exec -T postgres psql -U crypto -d crypto -tA -c 'SELECT count(*) > 0 FROM hourly_reports' | grep -q t"

echo
echo "Summary: ${pass} passed, ${fail} failed"
echo
exit $fail
