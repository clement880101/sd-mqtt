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

## Licensing

sd-mqtt is MIT licensed. It includes third-party components under other
permissive licenses; see `THIRD-PARTY-LICENSES.md` in a release artifact for
the full attribution.
