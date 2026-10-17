# The container image

A 10 MB scratch image of the static broker, run as a non-root user with its store in a volume.

```bash
docker run -d --name sdmqtt \
  -p 1883:1883 -p 1884:1884 \
  -v sdmqtt-data:/data \
  -e SDMQTT_ADMIN_TOKEN="$(openssl rand -hex 24)" \
  ghcr.io/clement880101/sd-mqtt:latest
```

The image is the static broker on `scratch`: no shell, nothing else to
patch, about 10 MB. It runs as uid 65532 and writes only to `/data`, so it
runs with `--read-only` too. Its admin listener binds every interface,
because inside a container that is the only way to reach it, and so **it
refuses to start without `SDMQTT_ADMIN_TOKEN`**. `docker stop` is a clean
shutdown: the broker flushes its store on SIGTERM and exits inside Docker's
grace. Every setting is an `SDMQTT_*` variable, listed under
[Configuration](../../README.md#configuration-reference). Kubernetes or Compose health checks should
ask `GET /health/ready` on port 1884; the image has no client to run a
`HEALTHCHECK` of its own. `LICENSE` and both notices files are in
`/usr/share/doc/sdmqtt/`.

Build it from the repository root with
`docker build -f services/broker/Dockerfile -t sdmqtt .`, and test a build
with `python3 scripts/test_image.py`.
