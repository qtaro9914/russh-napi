# N-API bindings for russh

This library contains neatly wrapped and typed native bindings for the client side of the [russh](https://github.com/warp-tech/russh) SSH protocol library.

Curently I'm only maintaining it as a dependency of [Tabby](https://github.com/eugeny/tabby) and not for general public use. However it is mostly stable, self-documented through type definitions and ready to use.

Run `yarn local:debug` to build a debug binding for your platform. GitHub CI builds bindings for all platforms and that's what gets published to npm.

## Cancellation

Create one `SshCancellation` for each connection attempt and pass it to both the
transport factory and `SSHClient.connect`:

```ts
const cancellation = new SshCancellation()
const transport = await SshTransport.newSocket('localhost:22', cancellation)
const client = await SSHClient.connect(transport, verifyHostKey, { cancellation })
// The tab/session teardown path can call this even before connect resolves.
cancellation.cancel()
```

Command, HTTP/SOCKS proxy and SSH jump-channel factories accept the token too.
Cancellation aborts the underlying I/O and pending host-key/authentication waits.
A cancelled token cannot be reused. Call `transport.close()` for a transport that
was created but will not be passed to `connect`. Failed/timed-out handshakes also
cancel their background session. `disconnect()` now aborts local I/O without
waiting for the authentication mutex or a peer acknowledgement. Proxy commands
are killed and asynchronously reaped; this applies to the direct child process,
not an arbitrary process tree.

## Bounded receive callbacks

Flow control is **opt-in**; existing consumers retain unthrottled delivery. On an
authenticated client, set a limit before starting output. Acknowledge each original
stdout/stderr array only after processing it:

```ts
const channel = await client.activateChannel(await client.openSessionChannel())
channel.setReceiveBufferLimit(256 * 1024)
const consume = (data: Uint8Array) => {
    Promise.resolve().then(() => render(data)).then(
        () => channel.acknowledgeData(data),
        () => channel.close().catch(() => client.disconnect()),
    )
}
channel.data$.subscribe(consume)
channel.extendedData$.subscribe(([, data]) => consume(data))
await channel.requestShell()
```

Both streams share the channel budget. One packet larger than the limit is
allowed when that channel has no outstanding data. Managed callbacks have an
additional connection-wide cap of 8 MiB and 1,024 pending packets; empty packets
consume one byte of credit. Duplicate ACKs, copies of arrays and ACKs after closing
cannot release credit twice. EOF/close callbacks wait for outstanding managed data
to be acknowledged. Local close discards remaining incoming channel data and
releases its credit. Disconnect/cancellation wake a blocked reader too.
`client.receiveBufferStats()` reports outstanding managed callbacks. The channel
count includes discarding channels waiting for a remote close; late/repeated closes
after that remote close do not recreate receive state.

**Limitation:** a full budget pauses packet dispatch for the entire shared SSH
connection, including other channels, keepalives and control packets. This is not
independent per-channel SSH window management. Consumers must ACK promptly or
close/cancel, and must consume both stdout and stderr. Keeping other channels
responsive while one is paused requires a separate russh core change. Zero
restores legacy delivery. Configure before requesting output: already queued
legacy callbacks cannot be bounded retroactively. The caps cover managed callback
payloads, not every native/socket buffer or the entire heap. Native callbacks that
have not yet reached JavaScript still count toward the managed budget.

## Regression checks

After `yarn local:debug`, run `yarn test:regression`. It builds a loopback-only Rust
SSH fixture and tests the actual native addon through Node. It covers stalled
SSH/proxy handshakes, unanswered host-key prompts, authentication and timeout
cancellation, bounded lossless stdout/stderr, oversized packets, duplicate ACKs,
flow-control disable, paused close/disconnect, and proxy-command child reaping.
No external SSH server is needed. Generated JavaScript, declarations and native
artifacts are build outputs; rebuild after changing Rust/TypeScript sources.
