.PHONY: up down build logs0 logs1 logs2 logs3 logs4 stop0 stop1 stop2 stop3 stop4 

IMAGE_NAME := tp-health-checker
COMPOSE_FILE := docker-compose.yaml
ENV_FILE := .env

up: build
	@docker compose -f $(COMPOSE_FILE) up -d

down:
	@docker kill hc_0 hc_1 hc_2 hc_3 hc_4 2>/dev/null || true
	@docker rm -f hc_0 hc_1 hc_2 hc_3 hc_4 2>/dev/null || true
	@docker network rm ringnet 2>/dev/null || true

build:
	@docker build -t $(IMAGE_NAME):latest -f Dockerfile ..

logs:
	@docker compose -f $(COMPOSE_FILE) logs -f

logs0:
	@docker compose -f $(COMPOSE_FILE) logs -f hc_0

logs1:
	@docker compose -f $(COMPOSE_FILE) logs -f hc_1

logs2:
	@docker compose -f $(COMPOSE_FILE) logs -f hc_2

logs3:
	@docker compose -f $(COMPOSE_FILE) logs -f hc_3

logs4:
	@docker compose -f $(COMPOSE_FILE) logs -f hc_4


stop0:
	@docker kill hc_0

stop1:
	@docker kill hc_1

stop2:
	@docker kill hc_2

stop3:
	@docker kill hc_3

stop4:
	@docker kill hc_4

