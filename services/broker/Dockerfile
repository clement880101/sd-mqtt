# syntax=docker/dockerfile:1
#
# The sd-mqtt container image. Built from the repository root:
#
#     docker build -f services/broker/Dockerfile -t sdmqtt .
#     docker buildx build -f services/broker/Dockerfile \
#         --platform linux/amd64,linux/arm64 -t sdmqtt .
#
# Three stages. The interface is built once on the build machine; the
# broker is cross-compiled there too, as a static musl binary for the
# platform asked for, with cargo-zigbuild; and the image is `scratch`
# with the binary, its licence and both notices files, and nothing else
# — no shell, no package manager, nothing to patch (decision 0005).
#
# The same builder stage is what `scripts/package.sh` exports the Linux
# release archives from (target `export`), so the binary in an archive
# and the binary in the image are one build, not two that could drift.

ARG RUST_VERSION=1.98.1
ARG NODE_VERSION=22

# --- the interface -------------------------------------------------------
FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-bookworm-slim AS web
WORKDIR /src/services/web
COPY services/web/package.json services/web/package-lock.json ./
RUN npm ci
COPY services/web/ ./
RUN npm run build

# --- the broker, cross-compiled ------------------------------------------
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-slim-bookworm AS builder
ARG TARGETARCH
# Extra cargo flags, for a measurement build only; see scripts/package.sh.
ARG CARGO_FLAGS=""
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --break-system-packages --no-cache-dir ziglang==0.15.1 \
    && cargo install cargo-zigbuild --version 0.23.4 --locked
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-musl > /target ;; \
        arm64) echo aarch64-unknown-linux-musl > /target ;; \
        *) echo "no release target for $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && rustup target add "$(cat /target)"
WORKDIR /src
COPY . .
COPY --from=web /src/services/web/dist services/web/dist
# The registry and the target directory are cache mounts, so a rebuild
# after a source change recompiles what changed rather than everything;
# the binary is copied out in the same step because a cache mount is
# gone when it ends.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/services/broker/target \
    target="$(cat /target)" \
    && cd services/broker \
    && cargo zigbuild --release --locked -p sdmqtt --target "$target" $CARGO_FLAGS \
    && mkdir -p /out/doc /out/data \
    && cp "target/$target/release/sdmqtt" /out/sdmqtt \
    && cd /src \
    && python3 scripts/rust-notices.py --check --binary /out/sdmqtt \
    && cp LICENSE README.md /out/doc/ \
    && cp services/broker/THIRD-PARTY-NOTICES.txt /out/doc/THIRD-PARTY-NOTICES.txt \
    && cp services/web/THIRD-PARTY-NOTICES.txt /out/doc/web-THIRD-PARTY-NOTICES.txt

# --- what the release archives are made from -----------------------------
FROM scratch AS export
COPY --from=builder /out/sdmqtt /out/doc/ /

# --- the image -----------------------------------------------------------
FROM scratch AS image
COPY --from=builder /out/sdmqtt /sdmqtt
COPY --from=builder /out/doc/ /usr/share/doc/sdmqtt/
COPY --from=builder --chown=65532:65532 /out/data /data
USER 65532:65532
# The store is the volume; the admin listener binds every interface, so
# the broker refuses to start without SDMQTT_ADMIN_TOKEN, which is the
# point: a container is reached from outside it.
ENV SDMQTT_STORAGE_DIRECTORY=/data \
    SDMQTT_ADMIN_ADDRESS=0.0.0.0:1884
VOLUME /data
EXPOSE 1883 1884
ENTRYPOINT ["/sdmqtt"]
CMD ["serve"]
