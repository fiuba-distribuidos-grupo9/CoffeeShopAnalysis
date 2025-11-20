#!/usr/bin/env bash

set -euo pipefail

source .env

QUEUE_NAME="trn-purchases-qty-by-user-id-&-store-id-0"

rabbitmq_container_id=$(
  docker ps \
    --filter name=rabbitmq-message-middleware \
    --quiet     
)

docker exec "$rabbitmq_container_id" \
  rabbitmqadmin \
    get queue=$QUEUE_NAME \
    ackmode=ack_requeue_true \
    count=25000 \
    --username="$RABBITMQ_USER" \
    --password="$RABBITMQ_PASS" > "${QUEUE_NAME}_queue_messages.json"