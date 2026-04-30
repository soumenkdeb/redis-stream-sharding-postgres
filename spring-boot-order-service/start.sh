#!/usr/bin/env bash
# Exit immediately on error, unset variable, or failed pipeline
set -euo pipefail

# Resolve project root so the script works when called from any directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
  cat <<EOF
Usage: $0 [MODE] [JVM/app overrides...]

Modes:
  both       (default) HTTP endpoint + consumer threads
  producer   HTTP endpoint only — consumer threads disabled
  consumer   Consumer threads only — HTTP returns 503

JVM/app overrides are passed directly to the JVM, e.g.:
  $0 producer --spring.datasource.url=jdbc:postgresql://db:5432/orders_db
  $0 consumer -Dapp.num-shards=2 -Dapp.consumer-name=worker-2

Examples:
  ./start.sh                           # both producer + consumer
  ./start.sh producer                  # HTTP API only
  ./start.sh consumer                  # background consumer only
  ./start.sh both -Dapp.num-shards=8   # override shard count
EOF
}

MODE="${1:-both}"
case "$MODE" in
  both)     EXTRA_PROPS=() ;;
  producer) EXTRA_PROPS=(-Dapp.consumer.enabled=false) ;;
  consumer) EXTRA_PROPS=(-Dapp.producer.enabled=false) ;;
  -h|--help) usage; exit 0 ;;
  *)
    echo "ERROR: Unknown mode '$MODE'"
    usage
    exit 1
    ;;
esac

# Shift mode argument off so remaining args are passed to the JVM
shift 2>/dev/null || true

JAR=$(ls target/spring-boot-order-service-*.jar 2>/dev/null | head -1)
if [[ -z "$JAR" ]]; then
  echo "ERROR: No JAR found in $SCRIPT_DIR/target/. Build first:"
  echo "  mvn -q package -DskipTests"
  exit 1
fi

CORES=$(nproc)

echo "==> Spring Boot order service [mode=$MODE, cores=$CORES]"
echo "    JAR : $JAR"
echo "    Props: ${EXTRA_PROPS[*]:-<none>}"

exec java \
  -XX:+UseZGC -XX:+ZGenerational \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:+AlwaysPreTouch \
  -Djdk.virtualThreadScheduler.parallelism="${CORES}" \
  -Djdk.virtualThreadScheduler.maxPoolSize=$(( CORES * 256 )) \
  -Dspring.jmx.enabled=false \
  "${EXTRA_PROPS[@]}" \
  -jar "$JAR" "$@"
