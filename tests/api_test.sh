#!/usr/bin/env bash

set -u

API="${API:-http://localhost:8000}"
SYMBOL="${SYMBOL:-XBTUSD}"
HEAD_BYTES="${HEAD_BYTES:-500}"

hit() {
    local title="$1"
    local url="$2"
    echo
    echo "────── ${title} ──────"
    echo "GET ${url}"
    local body
    body=$(curl -sS -w '\n[HTTP %{http_code}]' "$url" 2>&1)
    echo "$body" | head -c "$HEAD_BYTES"
    echo
}

echo "API base : $API"
echo "Symbol   : $SYMBOL"

hit "Health check"               "$API/health"

# сassandra-backed (streaming)
hit "Trade lookup"               "$API/api/trades?symbol=$SYMBOL&limit=3"
hit "Trade lookup (Buy only)"    "$API/api/trades?symbol=$SYMBOL&side=Buy&min_size=10000&limit=3"
hit "Recent momentum"            "$API/api/momentum/$SYMBOL?minutes=10"
hit "Recent whale alerts"        "$API/api/alerts/whale/$SYMBOL?limit=5"
hit "Recent volatility alerts"   "$API/api/alerts/volatility/$SYMBOL?limit=5"

# зostgres-backed (batch)
hit "Hourly trading report"      "$API/api/reports/hourly?symbol=$SYMBOL&hours=12"
hit "Trading patterns"           "$API/api/analytics/trading-patterns?symbol=$SYMBOL"
hit "Whale impact (24h)"         "$API/api/analytics/whale-impact?symbol=$SYMBOL&period=24h"

FROM=$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null \
    || date -u -v-1H '+%Y-%m-%dT%H:%M:%SZ')
TO=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
hit "OHLCV 1m (last hour)"       "$API/api/price/$SYMBOL?from=$FROM&to=$TO&interval=1m"
hit "OHLCV 5m (last hour)"       "$API/api/price/$SYMBOL?from=$FROM&to=$TO&interval=5m"

echo
echo "Done. Open ${API}/docs for the interactive Swagger UI."
