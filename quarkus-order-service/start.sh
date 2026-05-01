#!/usr/bin/env bash
set -euo pipefail

# Resolve project root so the script works when called from any directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
  cat <<EOF
Usage: $0 [MODE] [JVM/app overrides...]

Modes:
  both       (default) HTTP endpoint + consumer threads on port 8001
  producer   HTTP endpoint only on port 8001 — consumer threads disabled
  consumer   Consumer threads only on port 8003 — HTTP returns 503

JVM/app overrides are passed directly to the JVM, e.g.:
  $0 producer -Dquarkus.datasource.jdbc.url=jdbc:postgresql://db:5432/orders_db
  $0 consumer -Dapp.num-shards=2 -Dapp.consumer-name=worker-2

Examples:
  ./start.sh                           # both producer + consumer on 8001
  ./start.sh producer                  # HTTP API only on 8001
  ./start.sh consumer                  # background consumer only on 8003
  ./start.sh both -Dapp.num-shards=8   # override shard count
EOF
}

MODE="${1:-both}"
case "$MODE" in
  both)     EXTRA_PROPS=() ;;
  producer) EXTRA_PROPS=(-Dquarkus.profile=producer -Dapp.consumer.enabled=false) ;;
  consumer) EXTRA_PROPS=(-Dquarkus.profile=consumer -Dapp.producer.enabled=false) ;;
  -h|--help) usage; exit 0 ;;
  *)
    echo "ERROR: Unknown mode '$MODE'"
    usage
    exit 1
    ;;
esac

# Shift mode argument off so remaining args are passed to the JVM
shift 2>/dev/null || true

JAR="target/quarkus-app/quarkus-run.jar"
if [[ ! -f "$JAR" ]]; then
  echo "ERROR: Quarkus runner not found at $SCRIPT_DIR/$JAR. Build first:"
  echo "  mvn -q package -DskipTests"
  exit 1
fi

CORES=$(nproc)

PORT="8001"
[[ "$MODE" == "consumer" ]] && PORT="8003"

echo "==> Quarkus order service [mode=$MODE, port=$PORT, cores=$CORES]"
echo "    JAR : $SCRIPT_DIR/$JAR"
echo "    Props: ${EXTRA_PROPS[*]:-<none>}"

exec java \
  -XX:+UseZGC -XX:+ZGenerational \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+AlwaysPreTouch \
  -Djdk.virtualThreadScheduler.parallelism="${CORES}" \
  -Djdk.virtualThreadScheduler.maxPoolSize=$(( CORES * 256 )) \
  "${EXTRA_PROPS[@]}" \
  -jar "$JAR" "$@"
