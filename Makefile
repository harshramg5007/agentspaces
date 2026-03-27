.PHONY: help doctor up smoke down example-hello example-fault-tolerance build test run-postgres stop-postgres run-server docs-lint docs-check clean docker-up docker-down

COMPOSE := $(shell if command -v docker-compose >/dev/null 2>&1; then printf '%s' 'docker-compose'; else printf '%s' 'docker compose'; fi)
PYTHON ?= python3.11
SMOKE_VENV ?= .venv-smoke
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
ROOT_COMPOSE_FILE := $(ROOT_DIR)/docker-compose.yml
FAULT_TOLERANCE_DIR := $(ROOT_DIR)/examples/fault-tolerance
FAULT_TOLERANCE_COMPOSE_FILE := $(FAULT_TOLERANCE_DIR)/docker-compose.yml
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= 5433
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres
POSTGRES_DATABASE ?= agent_space
POSTGRES_QUEUE_CLAIM_MODE ?= strict

help:
	@echo "Public targets:"
	@echo "  doctor                  - Check Docker, Compose, and Python 3.11"
	@echo "  up                      - Start the supported local Docker stack"
	@echo "  smoke                   - Run the supported hello-world example end to end"
	@echo "  down                    - Stop the supported local Docker stack"
	@echo "  example-hello           - Alias for smoke"
	@echo "  example-fault-tolerance - Run the supported fault-tolerance example"
	@echo "  build                   - Build the supported server binary"
	@echo "  test                    - Run the supported Go test suite"
	@echo "  docs-lint docs-check    - Lint and validate the public markdown surface"
	@echo "  run-postgres run-server - Run the supported server against local Postgres"
	@echo "  clean                   - Remove local build artifacts and stop containers"

doctor:
	@command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is required"; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "ERROR: docker daemon is not running"; exit 1; }
	@if command -v docker-compose >/dev/null 2>&1; then docker-compose version >/dev/null; \
	elif docker compose version >/dev/null 2>&1; then true; \
	else echo "ERROR: docker compose or docker-compose is required"; exit 1; fi
	@command -v $(PYTHON) >/dev/null 2>&1 || { echo "ERROR: $(PYTHON) is required"; exit 1; }
	@$(PYTHON) -c 'import sys; assert sys.version_info[:2] == (3, 11), sys.version'
	@echo "PASS: Docker is available"
	@echo "PASS: Compose is available"
	@echo "PASS: Python 3.11 is available"
	@if command -v go >/dev/null 2>&1; then echo "INFO: $$(go version)"; else echo "INFO: Go not installed; only needed for contributor workflows"; fi

up:
	@$(COMPOSE) -f $(ROOT_COMPOSE_FILE) up -d --build postgres agent-server

$(SMOKE_VENV)/bin/python:
	@$(PYTHON) -m venv $(SMOKE_VENV)
	@$(SMOKE_VENV)/bin/python -m pip install --upgrade pip
	@$(SMOKE_VENV)/bin/python -m pip install -e sdk/python

smoke: $(SMOKE_VENV)/bin/python
	@AGENT_SPACE_URL=http://localhost:$${AGENT_SPACE_PORT:-8080}/api/v1 $(SMOKE_VENV)/bin/python examples/hello-world/hello_world.py

down:
	@$(COMPOSE) -f $(ROOT_COMPOSE_FILE) down --remove-orphans

example-hello: smoke

example-fault-tolerance:
	@cd $(FAULT_TOLERANCE_DIR); \
	set +e; \
	$(COMPOSE) -f $(FAULT_TOLERANCE_COMPOSE_FILE) up --build --abort-on-container-exit --exit-code-from validator; \
	status=$$?; \
	$(COMPOSE) -f $(FAULT_TOLERANCE_COMPOSE_FILE) down -v --remove-orphans; \
	exit $$status

build:
	go build -o bin/agent-server ./cmd/server

test:
	go test -v -race ./...

run-postgres:
	@$(COMPOSE) -f $(ROOT_COMPOSE_FILE) up -d postgres

stop-postgres:
	@$(COMPOSE) -f $(ROOT_COMPOSE_FILE) stop postgres || true

run-server: run-postgres
	STORE_TYPE=postgres \
	POSTGRES_HOST="$(POSTGRES_HOST)" \
	POSTGRES_PORT="$(POSTGRES_PORT)" \
	POSTGRES_USER="$(POSTGRES_USER)" \
	POSTGRES_PASSWORD="$(POSTGRES_PASSWORD)" \
	POSTGRES_DATABASE="$(POSTGRES_DATABASE)" \
	POSTGRES_QUEUE_CLAIM_MODE="$(POSTGRES_QUEUE_CLAIM_MODE)" \
	go run ./cmd/server

docs-lint:
	./scripts/docs_lint.sh

docs-check:
	./scripts/docs_check.sh

clean:
	rm -rf $(SMOKE_VENV) bin
	$(COMPOSE) -f $(ROOT_COMPOSE_FILE) down -v --remove-orphans || true

docker-up: up

docker-down: down
