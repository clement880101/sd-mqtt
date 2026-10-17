# sd-mqtt

A distributed MQTT broker under the **Apache License 2.0, all of it**: clustering,
replicated queues, rules and connectors, authentication and the admin
interface are in the one binary, with no enterprise edition and no
open-core line. No external dependencies.

Status: **0.7.1**. [The design](docs/design.md) is what it was built
against; the questions it left open are settled, by measurement, in its
last two sections.
`CHANGELOG.md` is what to read before upgrading: before 1.0 the stored
format may move, and a store written by an earlier build is refused
rather than migrated.

## Features

| | |
|---|---|
| [Persistence](docs/features/persistence.md) | Sessions, subscriptions and queued messages survive a restart once the broker has a directory to keep them in. |
| [Rules and connectors](docs/features/rules.md) | Carry messages between MQTT and HTTP, poll an API, or bridge to another broker, with a durable outbox and dead letters. |
| [Users and authentication](docs/features/authentication.md) | Users with argon2 passwords, SCRAM-SHA-256 over MQTT 5's AUTH exchange, an anonymous switch, and a limit on failed logins. |
| [API keys](docs/features/api-keys.md) | Keys with read, write and admin scopes for the admin API, shown once and stored as digests. |
| [Access control](docs/features/access-control.md) | Rules that say who may publish and subscribe where, deny by default once one exists. |
| [Listeners](docs/features/listeners.md) | TCP, TLS, WebSocket and secure WebSocket listeners, each with its own mountpoint, maximum QoS, retain policy and publish cap. |
| [Clustering](docs/features/cluster.md) | Several nodes as one broker: identity, joining, failure detection, and what a partition does. |
| [Queue replication and failover](docs/features/replication.md) | Each session's queue on more than one node, synchronous or asynchronous, repaired when a node is gone and pulled from a copy when the owner dies. |
| [The admin API and interface](docs/features/admin-api.md) | A REST API, an events stream, Prometheus metrics, health endpoints and the embedded interface, on a listener guarded by where it listens and by API keys. |
| [The container image](docs/features/container.md) | A 10 MB scratch image of the static broker, run as a non-root user with its store in a volume. |

The [setup guide](docs/setup.md) takes you from nothing to a three-node
cluster; the [documentation index](docs/README.md) has the rest.

## Quick start

```bash
docker run -d --name sdmqtt \
  -p 1883:1883 -p 1884:1884 \
  -v sdmqtt-data:/data \
  -e SDMQTT_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  ghcr.io/clement880101/sd-mqtt:0.7.1
```

That is a broker on port 1883 with its admin interface on
<http://127.0.0.1:1884>, keeping its sessions in a volume. Or download a
binary from the [release](https://github.com/clement880101/sd-mqtt/releases/tag/v0.7.1):
static Linux builds for x86-64 and ARM, and macOS builds for Intel and
Apple silicon, each with its checksum, the licence and the notices for
everything compiled in. `sdmqtt serve --store-directory ./data` runs it.
The [setup guide](docs/setup.md) continues from here.

## Configuration reference

`sdmqtt serve` with no arguments and no configuration file starts a
working broker on the registered MQTT port. Everything below is
optional, and the keys are documented in full in
`services/broker/crates/sdmqtt/src/config.rs`. Three sources are layered
in this order: `sdmqtt.toml`, then `SDMQTT_*` in the environment, then a
flag.

The file's sections, each explained further down:

| Section | What it holds |
|---|---|
| `[node]` | this node's name |
| `[listener]`, `[[listeners]]` | the MQTT listeners: address, kind, TLS, mountpoint, maximum QoS, retain, durability, publish cap |
| `[broker]` | protocol limits: `max_packet_size`, `max_topic_depth`, `max_client_id_len`, `max_keep_alive`, `max_inflight`, `max_queue_messages`, `max_queue_bytes`, `max_hops`, `trie_shards` |
| `[storage]` | where and how durably the store is kept |
| `[admin]` | the admin listener: address, token, allowed hosts |
| `[ingest]` | the HTTP ingest listener for `http_in` rules |
| `[auth]` | anonymous access, SCRAM iterations, the failure limiter |
| `[[acl]]` | access control rules |
| `[[rules]]` | rules and their connectors |
| `[cluster]` | membership, replication and durability across nodes |
| `[log]` | `level`: `error`, `warn`, `info` (the default), `debug` or `trace` |

Every variable, which overrides the file and is overridden by a flag:

| Variable | Sets |
|---|---|
| `SDMQTT_CONFIG` | the configuration file to read |
| `SDMQTT_ADDRESS` | `listener.address`, the default MQTT listener |
| `SDMQTT_MAX_CONNECTIONS` | `listener.max_connections` |
| `SDMQTT_MOUNTPOINT` | `listener.mountpoint` |
| `SDMQTT_MAX_QOS` | `listener.max_qos` |
| `SDMQTT_RETAIN_AVAILABLE` | `listener.retain_available` |
| `SDMQTT_LISTENER_DURABILITY` | `listener.durability` |
| `SDMQTT_MAX_PUBLISH_RATE` | `listener.max_publish_rate` |
| `SDMQTT_PUBLISH_BURST` | `listener.publish_burst` |
| `SDMQTT_STORAGE_DIRECTORY` | `storage.directory`; empty is no store |
| `SDMQTT_STORAGE_SYNC` | `storage.sync`: `immediate` or `relaxed` |
| `SDMQTT_STORAGE_FLUSH_INTERVAL_MS` | `storage.flush_interval_ms` |
| `SDMQTT_STORAGE_FLUSH_RECORDS` | `storage.flush_records` |
| `SDMQTT_ADMIN_ADDRESS` | `admin.address` |
| `SDMQTT_ADMIN_ENABLED` | `admin.enabled`; `false` runs no admin listener |
| `SDMQTT_ADMIN_TOKEN` | `admin.token` |
| `SDMQTT_ADMIN_ALLOWED_HOSTS` | `admin.allowed_hosts`, comma separated |
| `SDMQTT_AUTH_ANONYMOUS` | `auth.anonymous` |
| `SDMQTT_AUTH_SCRAM_ITERATIONS` | `auth.scram_iterations` |
| `SDMQTT_AUTH_MAX_FAILURES_PER_MINUTE` | `auth.max_failures_per_minute` |
| `SDMQTT_LOG` | `log.level` |
| `SDMQTT_NODE_NAME` | `node.name` |
| `SDMQTT_CLUSTER_NAME` | `cluster.name`, bound into every handshake |
| `SDMQTT_CLUSTER_SECRET` | `cluster.secret`, compulsory once `cluster.address` is set |
| `SDMQTT_CLUSTER_ADDRESS` | `cluster.address`; setting it is what makes a node cluster |
| `SDMQTT_CLUSTER_ADVERTISE` | `cluster.advertise`, what peers dial |
| `SDMQTT_CLUSTER_SEEDS` | `cluster.seeds`, comma separated |
| `SDMQTT_CLUSTER_EXPECTED_SIZE` | `cluster.expected_size`, what `/health/cluster` calls complete |
| `SDMQTT_CLUSTER_REPLICATION_FACTOR` | `cluster.replication_factor` |
| `SDMQTT_CLUSTER_DURABILITY` | `cluster.durability`: `async` or `sync` |
| `SDMQTT_CLUSTER_SYNC_TIMEOUT_MS` | `cluster.sync_timeout_ms` |
| `SDMQTT_CLUSTER_STRICT_UNCONFIRMED` | `cluster.strict.unconfirmed` |
| `SDMQTT_CLUSTER_STRICT_CONNECTS` | `cluster.strict.connects`, `true` or `false` and nothing else |
| `SDMQTT_CLUSTER_STRICT_PUBLISHES` | `cluster.strict.publishes`, likewise |
| `SDMQTT_CLUSTER_STRICT_SUBSCRIBES` | `cluster.strict.subscribes`, likewise |
| `SDMQTT_CLUSTER_GONE_AFTER_MS` | `cluster.gone_after_ms`, the grace before a dead node is repaired away from |
| `SDMQTT_CLUSTER_REPAIR_SESSIONS_PER_SECOND` | `cluster.repair_sessions_per_second` |

The `[[listeners]]`, `[[acl]]` and `[[rules]]` tables are file-only.

## Licensing

sd-mqtt is licensed under the Apache License 2.0; see `LICENSE`.
Copyright 2026 Clement Chang. Releases made before the change were
published under the MIT licence and remain available under it; the
changelog says which. It includes third-party components under other
permissive licenses, and does not claim they are all Apache-2.0. Two of them are Apache-2.0 with no alternative branch this
project's allow list accepts, which asks for the license text to travel
with the binary.

It does. The attribution is compiled into `sdmqtt`, in two halves:

```bash
sdmqtt licenses            # everything in this binary, on standard output
```

and the same document at `/third-party-notices.txt` on the admin
listener, for anyone who has a browser pointed at it rather than a
shell.

The two halves are also in the repository, each checked in CI against
the thing it is generated from:

| File | Covers | Generated by |
|---|---|---|
| `services/broker/THIRD-PARTY-NOTICES.txt` | the crates compiled in | `scripts/rust-notices.py` |
| `services/web/THIRD-PARTY-NOTICES.txt` | the libraries in the bundle | `npm run build` |

**They are not generated the same way.** The browser half is read out
of the artifact: a plugin walks the bundler's post-tree-shaking module
graph, so a dependency that is installed and never imported is
correctly absent. The Rust half is generated from the dependency
graph, which is a manifest, and then `scripts/rust-notices.py --check`
reads the release binary and fails on any crate the binary names that
the file does not — rustc records the source path of every panic site,
so a crate that ships is usually named in the bytes. That is a
completeness assertion in one direction: a crate can be linked in and
leave no path string at all, so the check catches an omission and
never confirms the list is exactly right.

Neither of those checks runs in `cargo test`. Both are CI jobs, and a
green local suite says nothing about either.

The policy behind all of this — which code is distributed, which is
build tooling, and what each license asks for — is in
`docs/components.md`.
