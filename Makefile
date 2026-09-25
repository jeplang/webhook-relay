GO      ?= go
BINARY  := bin/relay
DB      ?= postgres://relay:relay@localhost:5432/relay?sslmode=disable

.PHONY: build test lint fmt run migrate integration compose-up compose-down

build:
	$(GO) build -o $(BINARY) ./cmd/relay

test:
	$(GO) test ./...

lint:
	gofmt -l .
	$(GO) vet ./...
	staticcheck ./...

fmt:
	gofmt -w .

run: build
	RELAY_DATABASE_URL=$(DB) ./$(BINARY)

migrate:
	psql "$(DB)" -f migrations/0001_init.sql

integration:
	RELAY_DATABASE_URL=$(DB) $(GO) test -tags integration ./internal/store/

compose-up:
	docker compose up -d

compose-down:
	docker compose down
