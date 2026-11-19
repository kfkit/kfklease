GO ?= go
# The stand's ports and image names: the file compose.yaml and the stand's
# scripts read as well.
include test/e2e/.env
BROKERS ?= localhost:$(KAFKA_HOST_PORT)

.PHONY: test integration lint image generate

test:
	$(GO) vet ./...
	$(GO) test -race ./...

# Needs the Kafka container from the e2e stand; starts it if it is not up.
integration:
	docker compose -f test/e2e/compose.yaml up -d --wait kafka
	KFKLEASE_BROKERS=$(BROKERS) \
	KFKLEASE_SASL_BROKERS=localhost:$(KAFKA_HOST_SASL_PORT) \
	KFKLEASE_MTLS_BROKERS=localhost:$(KAFKA_HOST_MTLS_PORT) KFKLEASE_CERTS_DIR=$(CURDIR)/test/e2e/.out/certs \
	$(GO) test -race -count=1 -run Integration -v ./lease/

# shellcheck is required in CI and optional on a developer's machine.
# SCRIPTDIR lets it follow `source ./lib.sh` from any working directory.
lint:
	test -z "$$(gofmt -l .)"
	$(GO) vet ./...
	@if command -v shellcheck >/dev/null; then \
		echo 'shellcheck -x -P SCRIPTDIR test/e2e/*.sh'; \
		shellcheck -x -P SCRIPTDIR test/e2e/*.sh; \
	elif [ -n "$$CI" ]; then \
		echo 'lint: shellcheck is not installed' >&2; exit 1; \
	else \
		echo 'lint: shellcheck is not installed, the scripts are NOT checked (CI checks them)' >&2; \
	fi
	test/e2e/selftest.sh

image:
	docker build -t $(SCALER_IMAGE) .

generate:
	$(GO) generate ./...
