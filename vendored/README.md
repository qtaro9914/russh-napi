# Vendored dependencies

## russh-sftp 2.0.6 (patched)

Verbatim copy of the `russh-sftp` 2.0.6 sources from crates.io, plus two
additive public accessors needed by this binding's `SftpFile.readAt`:

- `client::fs::File::raw_handle()` — the server-side handle string
- `client::fs::File::raw_session()` — the request-id-multiplexed raw session
- `client::RawSftpSession::configured_limits()` — the negotiated
  `limits@openssh.com` limits

Applied via `[patch.crates-io]` in the workspace `Cargo.toml`. Should be
dropped once equivalent accessors land upstream
(https://github.com/AspectUnk/russh-sftp).
