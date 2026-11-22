#!/bin/sh
set -eu

exec cargo test -p iroh-c-ffi --lib media:: "$@"
