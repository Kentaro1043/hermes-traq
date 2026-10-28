#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
suffix="$$"
network="hermes-traq-test-$suffix"
db="hermes-traq-db-$suffix"
server="hermes-traq-server-$suffix"
cleanup() {
    docker rm -f "$server" "$db" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
docker network create "$network" >/dev/null
docker run -d --name "$db" --network "$network" -e MARIADB_ROOT_PASSWORD=password -e MARIADB_DATABASE=traq mariadb:10.11 --character-set-server=utf8mb4 --collation-server=utf8mb4_general_ci >/dev/null
ready=0
for attempt in $(seq 1 60); do
    if docker exec "$db" healthcheck.sh --connect --innodb_initialized >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
done
if [ "$ready" != 1 ]; then docker logs "$db"; exit 1; fi
docker run -d --name "$server" --network "$network" --network-alias hermes-traq-test-server -e TRAQ_ALLOWSIGNUP=true -e TRAQ_ORIGIN=http://localhost:3000 -e TRAQ_MARIADB_HOST="$db" -e TRAQ_MARIADB_USERNAME=root -e TRAQ_MARIADB_PASSWORD=password -e TRAQ_MARIADB_DATABASE=traq -e TRAQ_STORAGE_TYPE=memory ghcr.io/traptitech/traq@sha256:e362c1eb05a107410e325314bdb4233a431cd302a7a4ae677553204ac1ff1de7 serve --dev --skip-init-emojis >/dev/null
docker run --rm --user hermes --network "$network" --entrypoint /opt/hermes/.venv/bin/python3 -v "$root/tests/docker_smoke.py:/tmp/smoke.py:ro" -v "$root/traq:/tmp/traq:ro" -w /opt/hermes nousresearch/hermes-agent@sha256:9c5561579f3ffdf333401797094ac3a743738d6c25d40f4331c7bf77b694238d /tmp/smoke.py
