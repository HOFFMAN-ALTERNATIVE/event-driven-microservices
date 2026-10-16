#!/usr/bin/env bash
# End-to-end smoke test against the docker-compose stack.
set -euo pipefail

ORDERS=${ORDERS:-http://localhost:8081}
INVENTORY=${INVENTORY:-http://localhost:8082}
PAYMENTS=${PAYMENTS:-http://localhost:8083}
NOTIFIER=${NOTIFIER:-http://localhost:8084}
JAEGER=${JAEGER:-http://localhost:16686}

for url in "$ORDERS" "$INVENTORY" "$PAYMENTS" "$NOTIFIER"; do
  for _ in $(seq 1 60); do curl -fs "$url/readyz" >/dev/null && break; sleep 1; done
  curl -fs "$url/readyz" >/dev/null || { echo "not ready: $url"; exit 1; }
done

order() { # customer amount sku qty
  curl -fs -X POST "$ORDERS/orders" -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"$1\",\"amount_cents\":$2,\"items\":[{\"sku\":\"$3\",\"qty\":$4}]}" | jq -r .id
}

await() { # id status
  for _ in $(seq 1 100); do
    s=$(curl -fs "$ORDERS/orders/$1" | jq -r .status)
    [ "$s" = "$2" ] && { echo "order $1 -> $s"; return 0; }
    sleep 0.2
  done
  echo "order $1 stuck in $s, want $2"; curl -s "$ORDERS/orders/$1" | jq .; exit 1
}

ok=$(order smoke-alice 2599 sku-keyboard 1)
await "$ok" CONFIRMED

rejected=$(order smoke-bob 100 sku-gpu 99)
await "$rejected" REJECTED

curl -fs -X PUT "$PAYMENTS/accounts/smoke-carol" -d '{"balance_cents":1}' >/dev/null
cancelled=$(order smoke-carol 9999 sku-monitor 1)
await "$cancelled" CANCELLED

monitor=$(curl -fs "$INVENTORY/stock/sku-monitor" | jq -c .)
echo "sku-monitor after compensation: $monitor"
[ "$(echo "$monitor" | jq .reserved)" = 0 ] || { echo "compensation failed"; exit 1; }

for _ in $(seq 1 50); do
  n=$(curl -fs "$NOTIFIER/notifications" | jq 'map(select(.customer_id|startswith("smoke-")))|length')
  [ "$n" -ge 3 ] && break; sleep 0.2
done
echo "notifications: $n"; [ "$n" -ge 3 ]

# Traces: the happy-path order must appear as one trace touching all services.
for _ in $(seq 1 30); do
  services=$(curl -fs "$JAEGER/api/traces?service=orders&limit=50&lookback=1h" \
    | jq -r '[.data[] | select(any(.spans[]; .operationName=="POST /orders")) | .processes[].serviceName] | unique | join(",")' || true)
  case "$services" in *inventory*notifier*orders*payments*) break;; esac
  sleep 1
done
echo "services in traces: $services"
case "$services" in *inventory*notifier*orders*payments*) ;; *) echo "distributed trace incomplete"; exit 1;; esac
echo "SMOKE OK"
