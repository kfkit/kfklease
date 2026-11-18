#!/bin/sh
# Runs in the certs service: a CA, a broker certificate for the mTLS
# listener (by IP on the kfk network and as localhost) and a client
# certificate. Kafka reads the PEM files directly; the tests read them from
# .out/certs. Kept while they are complete and valid for another day: the
# service runs on every `up` of kafka, and a broker that is already running
# keeps the CA it started with, so new certificates would break mTLS for
# every client that reads them (the integration tests run twice, say).
set -eu
cd /certs
if [ -s ca.crt ] && [ -s broker.pem ] && [ -s client.crt ] && [ -s client.key ] && [ -s jaas.conf ] \
  && openssl verify -CAfile ca.crt broker.pem client.crt >/dev/null 2>&1 \
  && openssl x509 -checkend 86400 -noout -in ca.crt >/dev/null \
  && openssl x509 -checkend 86400 -noout -in broker.pem >/dev/null \
  && openssl x509 -checkend 86400 -noout -in client.crt >/dev/null; then
  echo "certificates and jaas.conf kept"
  exit 0
fi
rm -f ./*.pem ./*.crt ./*.key ./*.csr
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 7 \
  -subj "/CN=kfklease e2e CA" -keyout ca.key -out ca.crt >/dev/null 2>&1
for name in broker client; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" >/dev/null 2>&1
done
printf 'subjectAltName=IP:172.30.0.10,IP:127.0.0.1,DNS:localhost,DNS:kafka\n' > broker.ext
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 7 -extfile broker.ext -out broker.crt >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 7 -out client.crt >/dev/null 2>&1
cat broker.key broker.crt > broker.pem
rm -f ./*.csr broker.ext ca.srl
# The broker's SASL users: PLAIN is static, SCRAM credentials live in the
# metadata log and are created by the tests.
cat > jaas.conf <<'JAAS'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
    user_kfklease="kfklease-secret";
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS
chmod 644 ./*
echo "certificates and jaas.conf written"
