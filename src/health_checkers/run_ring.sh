#!/bin/bash
set -e

IMAGE_NAME=tp-health-checker
NETWORK_NAME=ringnet

docker network create "$NETWORK_NAME" 2>/dev/null || true

for c in hc1_container hc2_container hc3_container; do
  if docker ps -a --format '{{.Names}}' | grep -q "^${c}$"; then
    echo "Eliminando contenedor existente: $c"
    docker rm -f "$c" >/dev/null 2>&1 || true
  fi
done

echo "Levantando contenedores del ring..."

docker run -d --name hc1_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=1 -e NODE_NAME=hc-1 -e LISTEN_PORT=9101 \
  -e RING_PEERS="2@hc2_container:9102,3@hc3_container:9103" \
  -e HEARTBEAT_INTERVAL_MS=500 -e HEARTBEAT_TIMEOUT_MS=1500 \
  -e SUSPECT_GRACE_MS=5000 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc2_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=2 -e NODE_NAME=hc-2 -e LISTEN_PORT=9102 \
  -e RING_PEERS="1@hc1_container:9101,3@hc3_container:9103" \
  -e HEARTBEAT_INTERVAL_MS=500 -e HEARTBEAT_TIMEOUT_MS=1500 \
  -e SUSPECT_GRACE_MS=5000 \
  -e MODE=auto "$IMAGE_NAME"

docker run -d --name hc3_container --network "$NETWORK_NAME" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=3 -e NODE_NAME=hc-3 -e LISTEN_PORT=9103 \
  -e RING_PEERS="1@hc1_container:9101,2@hc2_container:9102" \
  -e HEARTBEAT_INTERVAL_MS=500 -e HEARTBEAT_TIMEOUT_MS=1500 \
  -e SUSPECT_GRACE_MS=5000 \
  -e MODE=auto "$IMAGE_NAME"

echo "Todos los health checkers levantados."

if command -v xterm >/dev/null 2>&1; then
  echo "Abriendo ventanas xterm para cada nodo..."

  xterm -T "hc1 logs" -e bash -lc "echo 'Logs hc1_container'; docker logs -f hc1_container" &
  xterm -T "hc2 logs" -e bash -lc "echo 'Logs hc2_container'; docker logs -f hc2_container" &
  xterm -T "hc3 logs" -e bash -lc "echo 'Logs hc3_container'; docker logs -f hc3_container" &
else
  echo "No se encontró xterm."
  echo "Podés ver los logs manualmente con:"
  echo "  docker logs -f hc1_container"
  echo "  docker logs -f hc2_container"
  echo "  docker logs -f hc3_container"
fi
