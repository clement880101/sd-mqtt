FROM rust:1-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl jq ripgrep fd-find ca-certificates \
    build-essential pkg-config clang \
    && rm -rf /var/lib/apt/lists/*

# Node for the admin UI workspace
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy \
    && cargo install cargo-nextest cargo-deny --locked

WORKDIR /workspaces/repo
