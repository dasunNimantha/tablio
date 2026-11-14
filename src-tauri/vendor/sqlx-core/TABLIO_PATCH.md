# Vendored sqlx-core 0.9.0 (patched)

Unmodified copy of `sqlx-core` 0.9.0 from crates.io, except
`src/net/socket/mod.rs`: TCP sockets are created with `TCP_NODELAY`.

Why: sqlx never disables Nagle's algorithm. Connection setup (TLS
handshake, SASL / MySQL auth) is a series of small writes, and with Nagle
on each can wait for a delayed ACK (~40 ms on Linux). Measured against a
local Postgres with `ssl = on`: a new `sslmode=prefer` connection took
~98 ms, ~11 ms with `TCP_NODELAY`; MariaDB logins 57 ms → 14 ms.

Applied via `[patch.crates-io]` in `src-tauri/Cargo.toml` (and the fuzz
crate). Drop this directory and the patch entries once an upstream sqlx
release sets `TCP_NODELAY` itself.
