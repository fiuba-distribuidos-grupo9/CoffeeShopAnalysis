#!/bin/bash
set -e

IMAGE_NAME=tp-health-checker
NETWORK_NAME=ringnet

docker network create "$NETWORK_NAME" 2>/dev/null || true

for c in hc_container_1 hc_container_2 hc_container_3 hc_container_4 hc_container_5; do
  if docker ps -a --format '{{.Names}}' | grep -q "^${c}$"; then
    echo "Eliminando contenedor existente: $c"
    docker rm -f "$c" >/dev/null 2>&1 || true
  fi
done

echo "Levantando contenedores del ring..."

docker run -d --name hc_container_1 --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=1 -e NODE_NAME=hc_container_1 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="hc_container_2:9101,hc_container_3:9101,hc_container_4:9101,hc_container_5:9101" \
  -e CONTROLLER_TARGETS="hc_container_2:9201,hc_container_3:9201,hc_container_4:9201,hc_container_5:9201" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc_container_2 --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=2 -e NODE_NAME=hc_container_2 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="hc_container_1:9101,hc_container_3:9101,hc_container_4:9101,hc_container_5:9101" \
  -e CONTROLLER_TARGETS="hc_container_1:9201,hc_container_3:9201,hc_container_4:9201,hc_container_5:9201" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc_container_3 --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=3 -e NODE_NAME=hc_container_3 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="hc_container_1:9101,hc_container_2:9101,hc_container_4:9101,hc_container_5:9101" \
  -e CONTROLLER_TARGETS="hc_container_1:9201,hc_container_2:9201,hc_container_4:9201,hc_container_5:9201" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc_container_4 --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=4 -e NODE_NAME=hc_container_4 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="hc_container_1:9101,hc_container_2:9101,hc_container_3:9101,hc_container_5:9101" \
  -e CONTROLLER_TARGETS="hc_container_1:9201,hc_container_2:9201,hc_container_3:9201,hc_container_5:9201" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc_container_5 --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=5 -e NODE_NAME=hc_container_5 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="hc_container_1:9101,hc_container_2:9101,hc_container_3:9101,hc_container_4:9101" \
  -e CONTROLLER_TARGETS="hc_container_1:9201,hc_container_2:9201,hc_container_3:9201,hc_container_4:9201" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

echo "Todos los health checkers levantados."

exit 0
