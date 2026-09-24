GO ?= go
BROKERS ?= localhost:19092

.PHONY: test integration lint image generate

test:
	$(GO) vet ./...
	$(GO) test -race ./...

# Needs the Kafka container from the e2e stand; starts it if it is not up.
integration:
	docker compose -f test/e2e/compose.yaml up -d --wait kafka
	KFKLEASE_BROKERS=$(BROKERS) $(GO) test -race -count=1 -run Integration -v ./lease/

lint:
	test -z "$$(gofmt -l .)"
	$(GO) vet ./...

image:
	docker build -t kfklease-scaler:dev .

generate:
	$(GO) generate ./...
