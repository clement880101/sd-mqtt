# sd-mqtt

A distributed MQTT broker with a built-in admin UI and UI-configurable
integrations. One binary, no external dependencies.

Status: in development. See `docs/superpowers/specs/` for the design and
`docs/superpowers/plans/` for the implementation plans.

## Build

```bash
cd services/broker
cargo build --release
```

## Configuration

`sdmqtt serve` with no arguments and no configuration file starts a
working broker on the registered MQTT port. Everything below is
optional, and the keys are documented in full in
`services/broker/crates/sdmqtt/src/config.rs`. Three sources are layered
in this order: `sdmqtt.toml`, then `SDMQTT_*` in the environment, then a
flag.

### Persistence is off unless you ask for it

```toml
[storage]
directory = "/var/lib/sdmqtt"   # no default: without it, nothing is stored
sync = "immediate"              # or "relaxed", which is for measuring only
flush_interval_ms = 2
flush_records = 1000
```

**With no `storage.directory` the broker writes nothing down.** Sessions,
their subscriptions and their queued messages last as long as the
process and no longer, and starting this way logs a warning saying so.
That is deliberate: refusing to start without a directory would break
the promise that no configuration is a working configuration, and
defaulting to one would drop a database file into whatever directory the
service happened to start in.

Give it a directory and a restart preserves durable sessions, their
subscriptions and their queued messages, and no acknowledgement is sent
to a publisher before its message is on the disk.

The directory is created if it is not there, and the store is the file
`sdmqtt.redb` inside it. One broker per directory: a second is refused
with the path it could not take.

## Development containers

Everything builds and tests in a **single container**. The per-service
containers exist for convenience, not necessity, and share one image.

Open the repository root in the container described by
`.devcontainer/root/devcontainer.json` and the whole workspace is available
at `/workspaces/repo`. Or without an editor:

```bash
docker build -f .devcontainer/base.Dockerfile -t sd-mqtt-dev .
```

```bash
docker run --rm -it -v "$PWD:/workspaces/repo" -w /workspaces/repo/services/broker sd-mqtt-dev bash
```

The `broker` and `web` containers in `.devcontainer/compose.yaml` are the
same image with a different working directory, for anyone who prefers one
editor window per service. Nothing requires them.

## Licensing

sd-mqtt is MIT licensed. It includes third-party components under other
permissive licenses; see `THIRD-PARTY-LICENSES.md` in a release artifact for
the full attribution.
