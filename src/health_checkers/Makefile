.PHONY: up down build logs1 logs2 logs3 logs4 logs5 stop1 stop2 stop3 stop4 stop5

IMAGE_NAME := tp-health-checker
COMPOSE_FILE := docker-compose.yaml
ENV_FILE := .env


up: build
	@docker-compose -f $(COMPOSE_FILE) up -d

down:
	@docker kill hc_container_1 hc_container_2 hc_container_3 hc_container_4 hc_container_5 2>/dev/null || true
	@docker rm -f hc_container_1 hc_container_2 hc_container_3 hc_container_4 hc_container_5 2>/dev/null || true
	@docker network rm ringnet 2>/dev/null || true
	

build:
	@docker build -t $(IMAGE_NAME):latest -f Dockerfile ..

logs:
	@docker-compose -f $(COMPOSE_FILE) logs -f

logs1:
	@docker-compose -f $(COMPOSE_FILE) logs -f hc_container_1

logs2: 
	@docker-compose -f $(COMPOSE_FILE) logs -f hc_container_2

logs3: 
	@docker-compose -f $(COMPOSE_FILE) logs -f hc_container_3

logs4:
	@docker-compose -f $(COMPOSE_FILE) logs -f hc_container_4

logs5: 
	@docker-compose -f $(COMPOSE_FILE) logs -f hc_container_5

stop1: 
	@docker kill hc_container_1

stop2: 
	@docker kill hc_container_2

stop3: 
	@docker kill hc_container_3

stop4: 
	@docker kill hc_container_4

stop5:
	@docker kill hc_container_5