GOFMT_FILES?=$$(find . -name '*.go' |grep -v vendor)
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
default: build

build:
	go build .

test:
	 go test ./kafka -v $(TESTARGS)

testacc:
	GODEBUG=x509ignoreCN=0 \
	KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
	KAFKA_CA_CERT=$(CURDIR)/secrets/ca.crt \
	KAFKA_CLIENT_CERT=$(CURDIR)/secrets/client.pem \
	KAFKA_CLIENT_KEY=$(CURDIR)/secrets/client.key \
	KAFKA_CLIENT_KEY_PASSPHRASE=test-pass \
	KAFKA_SKIP_VERIFY=false \
	KAFKA_ENABLE_TLS=true \
	TF_ACC=1 go test ./kafka -v $(TESTARGS) -timeout 9m -count=1

# SASL/GSSAPI against a throwaway MIT KDC + Kafka (KRaft) in Docker
test-gssapi-e2e:
	docker compose -f e2e/gssapi/docker-compose.yaml run --rm client; \
	rc=$$?; docker compose -f e2e/gssapi/docker-compose.yaml down -v; exit $$rc

.PHONY: build test testacc test-gssapi-e2e
