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
