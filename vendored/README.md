# Vendored dependencies

## russh-sftp 2.0.6 (patched)

Verbatim copy of the `russh-sftp` 2.0.6 sources from crates.io, plus a few
additive public accessors needed by this binding's `SftpFile.readAt`/`writeAt`:

- `client::fs::File::raw_handle()` — the server-side handle string
- `client::fs::File::raw_session()` — the request-id-multiplexed raw session
- `client::RawSftpSession::configured_limits()` — the negotiated
  `limits@openssh.com` limits
- `client::RawSftpSession::pending_requests()` — the number of requests
  still awaiting a reply (lets the `writeAt` tests check that no request is
  left behind)

Applied via `[patch.crates-io]` in the workspace `Cargo.toml`. Should be
dropped once equivalent accessors land upstream
(https://github.com/AspectUnk/russh-sftp).
