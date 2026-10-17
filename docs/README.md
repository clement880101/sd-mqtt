# sd-mqtt documentation

Start with the [setup guide](setup.md): a broker running, a client talking
to it, the admin interface open, and a three-node cluster, in that order.

## Features

Each page says what the feature does, how to configure it, and what it
reports.

| Feature | What it does |
|---|---|
| [Persistence](features/persistence.md) | Sessions, subscriptions and queued messages survive a restart once the broker has a directory to keep them in. |
| [Rules and connectors](features/rules.md) | Carry messages between MQTT and HTTP, poll an API, or bridge to another broker, with a durable outbox and dead letters. |
| [Users and authentication](features/authentication.md) | Users with argon2 passwords, SCRAM-SHA-256 over MQTT 5's AUTH exchange, an anonymous switch, and a limit on failed logins. |
| [API keys](features/api-keys.md) | Keys with read, write and admin scopes for the admin API, shown once and stored as digests. |
| [Access control](features/access-control.md) | Rules that say who may publish and subscribe where, deny by default once one exists. |
| [Listeners](features/listeners.md) | TCP, TLS, WebSocket and secure WebSocket listeners, each with its own mountpoint, maximum QoS, retain policy and publish cap. |
| [Clustering](features/cluster.md) | Several nodes as one broker: identity, joining, failure detection, and what a partition does. |
| [Queue replication and failover](features/replication.md) | Each session's queue on more than one node, synchronous or asynchronous, repaired when a node is gone and pulled from a copy when the owner dies. |
| [The admin API and interface](features/admin-api.md) | A REST API, an events stream, Prometheus metrics, health endpoints and the embedded interface, on a listener guarded by where it listens and by API keys. |
| [The container image](features/container.md) | A 10 MB scratch image of the static broker, run as a non-root user with its store in a volume. |

## Reference

- [Every setting](../README.md#configuration-reference) — the file's
  sections and every `SDMQTT_*` variable, in the README, where a test
  holds it to what the broker reads.
- [Changelog](../CHANGELOG.md) — what each release changed and what an
  upgrade costs.
- [Development](development.md) — building, the conformance suite, the
  dev containers.

## Design

- [The design](superpowers/specs/2026-09-19-sd-mqtt-design.md) — the
  specification every milestone was built against, with as-built notes.
- [Decisions](decisions/README.md) — the questions settled by measurement.
- [Conventions](conventions.md) — the rules the project works by, each
  earned by a failure.
- [Research](research/) — the component selection and the brokers read
  before this one was designed.
