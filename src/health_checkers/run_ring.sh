#!/bin/bash
set -e

IMAGE_NAME=tp-health-checker
NETWORK_NAME=ringnet

docker network create "$NETWORK_NAME" 2>/dev/null || true

for c in hc1_container hc2_container hc3_container hc4_container hc5_container; do
  if docker ps -a --format '{{.Names}}' | grep -q "^${c}$"; then
    echo "Eliminando contenedor existente: $c"
    docker rm -f "$c" >/dev/null 2>&1 || true
  fi
done

echo "Levantando contenedores del ring..."

docker run -d --name hc1_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=1 -e NODE_NAME=hc-1 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="2@hc2_container:9101,3@hc3_container:9101,4@hc4_container:9101,5@hc5_container:9101" \
  -e CONTROLLER_TARGETS="hc-2@hc2_container:9201:hc2_container,hc-3@hc3_container:9201:hc3_container,hc-4@hc4_container:9201:hc4_container,hc-5@hc5_container:9201:hc5_container" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc2_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=2 -e NODE_NAME=hc-2 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="1@hc1_container:9101,3@hc3_container:9101,4@hc4_container:9101,5@hc5_container:9101" \
  -e CONTROLLER_TARGETS="hc-1@hc1_container:9201:hc1_container,hc-3@hc3_container:9201:hc3_container,hc-4@hc4_container:9201:hc4_container,hc-5@hc5_container:9201:hc5_container" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc3_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=3 -e NODE_NAME=hc-3 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="1@hc1_container:9101,2@hc2_container:9101,4@hc4_container:9101,5@hc5_container:9101" \
  -e CONTROLLER_TARGETS="hc-1@hc1_container:9201:hc1_container,hc-2@hc2_container:9201:hc2_container,hc-4@hc4_container:9201:hc4_container,hc-5@hc5_container:9201:hc5_container" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

  docker run -d --name hc4_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=4 -e NODE_NAME=hc-4 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="1@hc1_container:9101,2@hc2_container:9101,3@hc3_container:9101,5@hc5_container:9101" \
  -e CONTROLLER_TARGETS="hc-1@hc1_container:9201:hc1_container,hc-2@hc2_container:9201:hc2_container,hc-3@hc3_container:9201:hc3_container,hc-5@hc5_container:9201:hc5_container" \
  -e HEARTBEAT_INTERVAL_MS=800 \
  -e HEARTBEAT_TIMEOUT_MS=1000 \
  -e HEARTBEAT_MAX_RETRIES=5 \
  -e LEADER_CHECK_INTERVAL_MS=10000 \
  -e LEADER_CHECK_TIMEOUT_MS=1000 \
  -e SUSPECT_GRACE_MS=1200 \
  -e ELECTION_BACKOFF_MS_MIN=300 \
  -e ELECTION_BACKOFF_MS_MAX=900 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc5_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=5 -e NODE_NAME=hc-5 \
  -e LISTEN_PORT=9101 \
  -e HEALTH_LISTEN_PORT=9201 \
  -e RING_PEERS="1@hc1_container:9101,2@hc2_container:9101,3@hc3_container:9101,4@hc4_container:9101" \
  -e CONTROLLER_TARGETS="hc-1@hc1_container:9201:hc1_container,hc-2@hc2_container:9201:hc2_container,hc-3@hc3_container:9201:hc3_container,hc-4@hc4_container:9201:hc4_container" \
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

# if command -v xterm >/dev/null 2>&1; then
#   echo "Abriendo ventanas xterm para cada nodo..."

#   xterm -T "hc1 logs" -e bash -lc "echo 'Logs hc1_container'; docker logs -f hc1_container" >/dev/null 2>&1 &
#   xterm -T "hc2 logs" -e bash -lc "echo 'Logs hc2_container'; docker logs -f hc2_container" >/dev/null 2>&1 &
#   xterm -T "hc3 logs" -e bash -lc "echo 'Logs hc3_container'; docker logs -f hc3_container" >/dev/null 2>&1 &
# else
#   echo "No se encontró xterm."
#   echo "Podés ver los logs manualmente con:"
#   echo "  docker logs -f hc1_container"
#   echo "  docker logs -f hc2_container"
#   echo "  docker logs -f hc3_container"
# fi

exit 0
