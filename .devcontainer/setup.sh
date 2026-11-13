#!/usr/bin/env bash
# One-time devcontainer setup: system deps and Rust components needed by the build.
set -euo pipefail

# Trust corporate TLS-inspection CAs, if the host provides any (see README/devcontainer notes).
# Host dir ~/.devcontainer-ca is mounted at /opt/host-ca; every *.pem/*.crt in it is installed.
# Must run before anything else that downloads over HTTPS.
shopt -s nullglob
for f in /opt/host-ca/*.pem /opt/host-ca/*.crt; do
  [ -s "$f" ] && sudo cp "$f" "/usr/local/share/ca-certificates/host-$(basename "${f%.*}").crt"
done
sudo update-ca-certificates

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  protobuf-compiler pkg-config libssl-dev curl unzip

rustup component add rustfmt clippy

# DuckDB CLI (only needed for smoke tests) - best effort, never blocks setup
install_duckdb() {
  curl -fsSL https://install.duckdb.org | bash || return 1
  # The installer puts the binary in ~/.duckdb/cli/latest; expose it on PATH for all shells
  sudo ln -sf "$HOME/.duckdb/cli/latest/duckdb" /usr/local/bin/duckdb
}
if ! command -v duckdb >/dev/null 2>&1; then
  install_duckdb || echo "WARNING: DuckDB install failed; smoke tests will need it installed manually."
fi

# uv (per project convention for Python tooling)
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh || echo "WARNING: uv install failed"
  export PATH="$HOME/.local/bin:$PATH"
fi

# Pre-commit hooks
uvx pre-commit install || echo "pre-commit install skipped"

echo "Devcontainer ready. Try: make dev"
