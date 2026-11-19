GO ?= go
# The stand's ports and image names: the file compose.yaml and the stand's
# scripts read as well.
include test/e2e/.env
BROKERS ?= localhost:$(KAFKA_HOST_PORT)
COMPOSE = docker compose -f test/e2e/compose.yaml

.PHONY: test integration lint image generate chart-test e2e e2e-down

# CI keeps Go's cache between runs: no cached test results there.
test:
	$(GO) vet ./...
	$(GO) test -race $(if $(CI),-count=1) ./...

# Needs the Kafka container from the e2e stand; starts it if it is not up.
integration:
	$(COMPOSE) up -d --wait kafka
	KFKLEASE_BROKERS=$(BROKERS) \
	KFKLEASE_SASL_BROKERS=localhost:$(KAFKA_HOST_SASL_PORT) \
	KFKLEASE_SASL_USER=$(SASL_USER) KFKLEASE_SASL_PASSWORD=$(SASL_PASSWORD) \
	KFKLEASE_MTLS_BROKERS=localhost:$(KAFKA_HOST_MTLS_PORT) KFKLEASE_CERTS_DIR=$(CURDIR)/test/e2e/.out/certs \
	$(GO) test -race -count=1 -run Integration -v ./lease/

SCRIPTS = test/*.sh test/chart/*.sh test/e2e/*.sh

# shellcheck is required in CI and optional on a developer's machine.
# SCRIPTDIR lets it follow `source ./lib.sh` from any working directory.
lint:
	test -z "$$(gofmt -l .)"
	$(GO) vet ./...
	@if command -v shellcheck >/dev/null; then \
		echo 'shellcheck -x -P SCRIPTDIR $(SCRIPTS)'; \
		shellcheck -x -P SCRIPTDIR $(SCRIPTS); \
	elif [ -n "$$CI" ]; then \
		echo 'lint: shellcheck is not installed' >&2; exit 1; \
	else \
		echo 'lint: shellcheck is not installed, the scripts are NOT checked (CI checks them)' >&2; \
	fi
	test/e2e/selftest.sh
	test/changes-selftest.sh

image:
	docker build -t $(SCALER_IMAGE) .

# The chart's tests. Needs helm, or Docker for the alpine/helm image.
chart-test:
	test/chart/chart-test.sh

# The stand from nothing to "all scenarios passed"; SCENARIOS="crash pause"
# runs those two. Needs Docker, kubectl and envsubst. The stand stays up for
# its logs: e2e-down when done with it.
e2e:
	test/e2e/run.sh $(SCENARIOS)

e2e-down:
	$(COMPOSE) down -v

generate:
	$(GO) generate ./...
