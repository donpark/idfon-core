#!/bin/sh
set -eu

# The thin idfond binary links the C-FFI dylib (crates/idfond/build.rs),
# so it must exist before the workspace build.
cargo build --release -p iroh-c-ffi

# Workspace crates (protocol, daemon, CLI, media, client).
cargo test --workspace "$@"

# The C-FFI cdylib's own lib tests: IPC client FFI.
cargo test -p iroh-c-ffi --lib "$@"
