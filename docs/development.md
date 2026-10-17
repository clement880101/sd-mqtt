# Development

Building from source, the conformance suite, and the development containers.

## Build


```bash
cd services/broker
cargo build --release
```

## Conformance


`services/broker/conformance/run.sh` runs Eclipse Paho's interoperability
suite against a release build, over plain TCP, over WebSocket and — when
`openssl` is on the path to mint a certificate for the run — over TLS,
for both protocol versions, and fails on any difference from
`conformance/known-failures.txt` in either direction. `CONFORMANCE_MODES="tls"`
runs one layer while working on it.

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
