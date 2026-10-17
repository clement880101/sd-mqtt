# Setup guide

From nothing to a working broker in a few minutes, then a cluster. Every
command here is the one the README's install section uses, and every
setting named has a page under [features](features/).

## 1. Run a broker

**With Docker:**

```bash
docker run -d --name sdmqtt \
  -p 1883:1883 -p 1884:1884 \
  -v sdmqtt-data:/data \
  -e SDMQTT_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  ghcr.io/clement880101/sd-mqtt:0.7.1
```

**Or with the binary**, on Linux x86-64 (the release has ARM Linux and
both macOS builds too):

```bash
curl -LO https://github.com/clement880101/sd-mqtt/releases/download/v0.7.1/sdmqtt-0.7.1-x86_64-unknown-linux-musl.tar.gz
curl -LO https://github.com/clement880101/sd-mqtt/releases/download/v0.7.1/sdmqtt-0.7.1-x86_64-unknown-linux-musl.tar.gz.sha256
sha256sum -c sdmqtt-0.7.1-x86_64-unknown-linux-musl.tar.gz.sha256
tar -xzf sdmqtt-0.7.1-x86_64-unknown-linux-musl.tar.gz
./sdmqtt-0.7.1-x86_64-unknown-linux-musl/sdmqtt serve --store-directory ./data
```

Either way: MQTT on port 1883, the admin interface and API on port 1884.
The binary keeps its sessions in `./data`; the container keeps them in
the `sdmqtt-data` volume. Without a store directory the broker runs but
forgets everything at restart, and says so in its log.

## 2. Talk to it

Any MQTT client works. With the Mosquitto command-line tools, in one
terminal:

```bash
mosquitto_sub -h 127.0.0.1 -t 'demo/#' -v
```

and in another:

```bash
mosquitto_pub -h 127.0.0.1 -t demo/hello -m 'hello from sd-mqtt'
```

The first terminal prints `demo/hello hello from sd-mqtt`.

## 3. Open the admin interface

<http://127.0.0.1:1884>. The Overview page shows live rates, Clients the
connected clients, Topics a retained-message browser and a live
publish-and-watch tool, Rules the connectors, Cluster the nodes, and
Settings the users, access control rules, API keys and listeners.

If you set `SDMQTT_ADMIN_TOKEN` (the container requires it), paste the
token at the top of the Settings page before changing anything; it is
kept for that browser tab only. Without a token the admin listener
answers on loopback only and needs no key.

The same things are a REST API under `/api/v1/`, metrics are at
`/metrics`, and `GET /health/ready` says whether the broker is up:

```bash
curl -s http://127.0.0.1:1884/health/ready
```

## 4. Write a configuration file

Everything so far was flags and environment variables. For more than a
few settings, write `sdmqtt.toml`:

```toml
[storage]
directory = "/var/lib/sdmqtt"

[listener]
address = "0.0.0.0:1883"

[admin]
address = "127.0.0.1:1884"

[auth]
anonymous = true
```

and start with `sdmqtt serve --config sdmqtt.toml`. Without `--config`
the broker looks for `sdmqtt.toml` in the directory it starts in. A
flag overrides the environment, which overrides the file. Every section
and every `SDMQTT_*` variable is in the
[configuration reference](../README.md#configuration-reference).

## 5. Require a login

Create a user through the API — the token is the one you set:

```bash
curl -X POST -H "Authorization: Bearer $SDMQTT_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"name":"alice","password":"a-real-password"}' \
     http://127.0.0.1:1884/api/v1/users
```

then turn off anonymous access with `anonymous = false` under `[auth]`
(or `SDMQTT_AUTH_ANONYMOUS=false`) and restart. Clients now connect with
a username and password:

```bash
mosquitto_sub -h 127.0.0.1 -u alice -P a-real-password -t 'demo/#' -v
```

Users replicate to every node of a cluster, so this is done once. The
[users and authentication](features/authentication.md) page covers
SCRAM, the failed-login limit and the anonymous switch;
[access control](features/access-control.md) covers who may publish and
subscribe where.

## 6. Run three nodes

A cluster is three containers that can reach each other. Save this as
`compose.yaml` and run `docker compose up -d`:

```yaml
x-node: &node
  image: ghcr.io/clement880101/sd-mqtt:0.7.1
  restart: unless-stopped
  environment: &env
    SDMQTT_ADMIN_TOKEN: ${SDMQTT_ADMIN_TOKEN:?set a token for the admin API}
    SDMQTT_CLUSTER_SECRET: ${SDMQTT_CLUSTER_SECRET:?set a secret the nodes share}
    SDMQTT_CLUSTER_ADDRESS: 0.0.0.0:7700
    SDMQTT_CLUSTER_EXPECTED_SIZE: "3"

services:
  alpha:
    <<: *node
    environment:
      <<: *env
      SDMQTT_NODE_NAME: alpha
      SDMQTT_CLUSTER_ADVERTISE: alpha:7700
      SDMQTT_CLUSTER_SEEDS: beta:7700,gamma:7700
    ports: ["1883:1883", "1884:1884"]
    volumes: ["alpha-data:/data"]
  beta:
    <<: *node
    environment:
      <<: *env
      SDMQTT_NODE_NAME: beta
      SDMQTT_CLUSTER_ADVERTISE: beta:7700
      SDMQTT_CLUSTER_SEEDS: alpha:7700,gamma:7700
    ports: ["1885:1883", "1886:1884"]
    volumes: ["beta-data:/data"]
  gamma:
    <<: *node
    environment:
      <<: *env
      SDMQTT_NODE_NAME: gamma
      SDMQTT_CLUSTER_ADVERTISE: gamma:7700
      SDMQTT_CLUSTER_SEEDS: alpha:7700,beta:7700
    ports: ["1887:1883", "1888:1884"]
    volumes: ["gamma-data:/data"]

volumes:
  alpha-data:
  beta-data:
  gamma-data:
```

```bash
export SDMQTT_ADMIN_TOKEN="$(openssl rand -hex 24)" SDMQTT_CLUSTER_SECRET="$(openssl rand -hex 24)"
docker compose up -d
curl -s http://127.0.0.1:1884/health/cluster
```

The health answer says `"status":"complete"` once alpha sees the other
two. Subscribe on one node and publish on another, and the message
crosses:

```bash
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'demo/#' -v &
mosquitto_pub -h 127.0.0.1 -p 1885 -t demo/across -m 'from beta'
```

A client that disconnects and reconnects to a different node finds its
session there; a node that dies loses no acknowledged message in
synchronous mode. The [clustering](features/cluster.md) and
[replication](features/replication.md) pages say how, and what each
setting trades.

## Where next

- [Features](README.md#features), one page each.
- [Every setting](../README.md#configuration-reference).
- [The design](design.md), for why.
