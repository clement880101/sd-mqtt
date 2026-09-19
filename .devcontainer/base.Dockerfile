FROM rust:1.98.1-slim-bookworm

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

# /etc/profile assigns PATH wholesale rather than appending, so a login
# shell drops the toolchain the base image put there. Dev containers run
# lifecycle commands and terminals through login shells, so put it back
# for every shell kind rather than only the one docker run happens to use.
RUN printf '%s\n' 'export PATH="/usr/local/cargo/bin:$PATH"' \
      > /etc/profile.d/10-cargo.sh \
    && chmod 0644 /etc/profile.d/10-cargo.sh

WORKDIR /workspaces/repo
