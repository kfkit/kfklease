GO ?= go
BROKERS ?= localhost:19092

.PHONY: test integration lint image generate

test:
	$(GO) vet ./...
	$(GO) test -race ./...

# Needs the Kafka container from the e2e stand; starts it if it is not up.
integration:
	docker compose -f test/e2e/compose.yaml up -d --wait kafka
	KFKLEASE_BROKERS=$(BROKERS) \
	KFKLEASE_SASL_BROKERS=localhost:19094 \
	KFKLEASE_MTLS_BROKERS=localhost:19095 KFKLEASE_CERTS_DIR=$(CURDIR)/test/e2e/.out/certs \
	$(GO) test -race -count=1 -run Integration -v ./lease/

lint:
	test -z "$$(gofmt -l .)"
	$(GO) vet ./...

image:
	docker build -t kfklease-scaler:dev .

generate:
	$(GO) generate ./...
