#!/bin/sh
# Runs in the certs service: a CA, a broker certificate for the mTLS
# listener (by IP on the kfk network and as localhost) and a client
# certificate. Kafka reads the PEM files directly; the tests read them from
# .out/certs. Regenerated on every stand start.
set -eu
cd /certs
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
