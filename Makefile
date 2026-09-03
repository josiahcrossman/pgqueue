# DATABASE_URL is used by migrate/worker/loadtest. Override on the command line
# or via your environment; this default matches docker-compose.yml.
DATABASE_URL ?= postgres://pgqueue:pgqueue@localhost:5432/pgqueue?sslmode=disable
export DATABASE_URL

.PHONY: up down migrate migrate-down test test-integration loadtest build tidy

## up: start Postgres 16 and wait for it to be healthy
up:
	docker compose up -d
	@echo "waiting for postgres to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' pgqueue-postgres 2>/dev/null)" = "healthy" ]; do \
		sleep 1; \
	done
	@echo "postgres is healthy"

## down: stop Postgres and remove the data volume
down:
	docker compose down -v

## migrate: apply all up migrations
migrate:
	go run ./cmd/migrate up

## migrate-down: roll back the most recent migration
migrate-down:
	go run ./cmd/migrate down

## test: run unit tests (no database required)
test:
	go test ./...

## test-integration: run integration tests against Postgres (needs `make up && make migrate`)
test-integration:
	go test -tags=integration -count=1 ./test/...

## loadtest: seed N jobs, drain them, and report throughput + exactly-once
## usage: make loadtest N=10000
loadtest: N ?= 5000
loadtest:
	go run ./cmd/loadtest -n $(N)

## build: compile all binaries into ./bin
build:
	go build -o bin/worker   ./cmd/worker
	go build -o bin/migrate  ./cmd/migrate
	go build -o bin/loadtest ./cmd/loadtest

## tidy: sync go.mod/go.sum
tidy:
	go mod tidy
