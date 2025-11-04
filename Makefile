SHELL := /bin/bash
PWD := $(shell pwd)

-include .env
export

DOCKER_COMPOSE_FILE := $(or $(COMPOSE_FILE), docker-compose.yaml)

# ============================== DOCKER COMPOSE FLOW ============================== #

DOCKERFILES := $(shell find ./src -name Dockerfile)
docker-build-image:
	@for dockerfile in $(DOCKERFILES); do \
		dir=$$(dirname $$dockerfile); \
		tag=$$(basename $$dir); \
		docker build --file $$dockerfile -t "$$tag:latest" .; \
	done

docker-compose-up: docker-build-image
	docker compose --file $(DOCKER_COMPOSE_FILE) up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker compose --file $(DOCKER_COMPOSE_FILE) stop --timeout 60
	docker compose --file $(DOCKER_COMPOSE_FILE) down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose --file $(DOCKER_COMPOSE_FILE) logs --follow
.PHONY: docker-compose-logs

docker-compose-restart:
	docker compose -f $(DOCKER_COMPOSE_FILE) stop $(if $(SERVICES),$(SERVICES))
	docker compose -f $(DOCKER_COMPOSE_FILE) rm -f $(if $(SERVICES),$(SERVICES))
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d $(if $(SERVICES),$(SERVICES))
.PHONY: docker-compose-restart


# ============================== TESTING ============================== #

docker-export-logs:
	@rm -rf logs
	@mkdir -p logs
	@for service in $$(docker-compose config --services); do \
		echo "Filtrando logs de $$service..."; \
		touch logs/$$service.log; \
		docker-compose logs --no-color $$service 2>&1 | grep 'eof' > logs/$$service.log; \
		if [ -s logs/$$service.log ]; then \
			lines=$$(wc -l < logs/$$service.log); \
		else \
			rm logs/$$service.log; \
		fi \
	done

test-all-eof-received: docker-export-logs
	@python3 eof_test.py

unit-tests:
	pytest --verbose
.PHONY: unit-tests

EXPECTED_VARIANT ?= reduced_data
EXPECTED_BASE := ./integration-tests/data/expected_output
EXPECTED_PATH := $(EXPECTED_BASE)/$(EXPECTED_VARIANT)

ACTUAL_DIR ?= ./integration-tests/data/query_results

ACTUAL_GLOB_DIR := ./.results/query_results

QUERY_IDS := 1X 21 22 3X 4X

RESULT_SUFFIX := _result.txt

integration-tests:
	@echo "==> Ejecutando tests de integración con variante: $(EXPECTED_VARIANT)"
	@echo "==> Usando expected outputs desde: $(EXPECTED_PATH)"
	@echo "==> Copiando resultados actuales normalizados a: $(ACTUAL_DIR)"
	@mkdir -p $(ACTUAL_DIR)
	@set -e; \
	for id in $(QUERY_IDS); do \
		pattern="$(ACTUAL_GLOB_DIR)/client_0__*__Q$${id}_result.txt"; \
		count=$$(ls -1 $$pattern 2>/dev/null | wc -l | tr -d ' ' || true); \
		if [ "$$count" = "0" ]; then \
			echo "[ADVERTENCIA] No se encontró ningún archivo para Q$${id} con patrón: $$pattern"; \
			continue; \
		fi; \
		if [ "$$count" -gt 1 ]; then \
			echo "[ADVERTENCIA] Se encontraron $$count archivos para Q$${id}; se usará el más reciente."; \
		fi; \
		file=$$(ls -t $$pattern 2>/dev/null | head -n1); \
		dest="$(ACTUAL_DIR)/client_Q$${id}_result.txt"; \
		echo "   - Q$${id}: $$file -> $$dest"; \
		sort "$$file" > "$$dest"; \
	done
	@python3 ./integration-tests/compare_results.py \
		--expected "$(EXPECTED_PATH)" \
		--actual   "$(ACTUAL_DIR)" \
		--suffix   "$(RESULT_SUFFIX)"

.PHONY: integration-tests
