const assert = require('node:assert/strict')
const { test, before, after } = require('node:test')
const { spawn } = require('node:child_process')
const net = require('node:net')
const { once } = require('node:events')
const path = require('node:path')
const { SSHClient, SshTransport, SshCancellation } = require('../lib')

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
async function deadline(promise, ms = 5000, label = 'operation') {
    let timer
    try {
        return await Promise.race([promise, new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`${label}: timed out after ${ms}ms`)), ms)
        })])
    } finally { clearTimeout(timer) }
}
const settled = promise => promise.then(value => ({ value }), error => ({ error }))
async function eventually(check, label = 'condition') {
    const end = Date.now() + 5000
    while (!check()) {
        if (Date.now() >= end) { throw new Error(`${label}: timed out`) }
        await delay(5)
    }
}

let serverProcess
let port
before(async () => {
    serverProcess = spawn(path.join(__dirname, '../target/debug/examples/receive_flow_server'), [], {
        stdio: ['ignore', 'pipe', 'inherit'],
    })
    const exited = new Promise((_, reject) => {
        serverProcess.once('error', reject)
        serverProcess.once('exit', code => reject(new Error(`SSH fixture exited: ${code}`)))
    })
    const output = await deadline(Promise.race([once(serverProcess.stdout, 'data'), exited]))
    port = Number(output[0].toString().trim())
    assert(port > 0)
})
after(async () => {
    if (serverProcess?.exitCode === null && serverProcess.pid) {
        const exited = once(serverProcess, 'exit')
        serverProcess.kill()
        await deadline(exited)
    }
})

async function stalledPeer(t) {
    const sockets = new Set()
    let accepted
    const connected = new Promise(resolve => { accepted = resolve })
    const server = net.createServer(socket => {
        sockets.add(socket)
        socket.on('error', () => {})
        socket.resume()
        socket.once('close', () => sockets.delete(socket))
        accepted(socket)
    })
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
    t.after(async () => {
        for (const socket of sockets) { socket.destroy() }
        await new Promise(resolve => server.close(resolve))
    })
    return { port: server.address().port, connected }
}
function cancellationFor(t) {
    const cancellation = new SshCancellation()
    t.after(() => cancellation.cancel())
    return cancellation
}
async function connect(config) {
    return SSHClient.connect(await SshTransport.newSocket(`127.0.0.1:${port}`), async () => true, config)
}
async function session(t) {
    const unauthenticated = await deadline(connect())
    t.after(() => unauthenticated.disconnect())
    const client = await deadline(unauthenticated.authenticateNone('test'))
    assert('openSessionChannel' in client)
    t.after(() => client.disconnect())
    const channel = await deadline(client.activateChannel(await client.openSessionChannel()))
    return { client, channel }
}

test('pre-cancelled transport is rejected; cancellation is idempotent', async () => {
    const cancellation = new SshCancellation()
    assert.equal(cancellation.isCancelled, false)
    cancellation.cancel()
    cancellation.cancel()
    assert.equal(cancellation.isCancelled, true)
    await assert.rejects(SshTransport.newSocket('127.0.0.1:1', cancellation), /cancelled/i)
})
test('cancel during SSH banner wait rejects and closes the socket', async t => {
    const peer = await stalledPeer(t)
    const cancellation = cancellationFor(t)
    const transport = await SshTransport.newSocket(`127.0.0.1:${peer.port}`, cancellation)
    const socket = await peer.connected
    const closed = once(socket, 'close')
    const result = settled(SSHClient.connect(transport, async () => true, { cancellation }))
    await delay(30)
    cancellation.cancel()
    assert.match(String((await deadline(result)).error), /cancelled/i)
    await deadline(closed)
})
test('unused transports can be closed explicitly', async t => {
    const peer = await stalledPeer(t)
    const transport = await SshTransport.newSocket(`127.0.0.1:${peer.port}`)
    const socket = await peer.connected
    const closed = once(socket, 'close')
    await transport.close()
    await transport.close()
    await deadline(closed)
})
for (const kind of ['HTTP', 'SOCKS']) {
    test(`cancel during ${kind} proxy handshake releases the socket`, async t => {
        const peer = await stalledPeer(t)
        const cancellation = cancellationFor(t)
        const factory = kind === 'HTTP' ? SshTransport.newHttpProxy : SshTransport.newSocksProxy
        const result = settled(factory('127.0.0.1', peer.port, 'example.invalid', 22, cancellation))
        const socket = await deadline(peer.connected)
        const closed = once(socket, 'close')
        cancellation.cancel()
        assert.match(String((await deadline(result)).error), /cancelled/i)
        await deadline(closed)
    })
}
test('cancel interrupts an unanswered host-key prompt', async t => {
    const cancellation = cancellationFor(t)
    let entered
    const prompting = new Promise(resolve => { entered = resolve })
    const transport = await SshTransport.newSocket(`127.0.0.1:${port}`)
    const result = settled(SSHClient.connect(transport, async () => {
        entered()
        return new Promise(() => {})
    }, { cancellation }))
    await deadline(prompting)
    cancellation.cancel()
    assert.match(String((await deadline(result)).error), /cancelled/i)
})
test('disconnect bypasses authentication holding the handle mutex', async t => {
    const client = await deadline(connect())
    t.after(() => client.disconnect())
    const authentication = settled(client.authenticateNone('stall'))
    await delay(50)
    await deadline(client.disconnect(), 1000)
    await deadline(authentication, 1000)
})
test('handshake timeout tears down an unanswered host-key prompt', async () => {
    const transport = await SshTransport.newSocket(`127.0.0.1:${port}`)
    await assert.rejects(deadline(SSHClient.connect(transport, async () => new Promise(() => {}), {
        connectionTimeoutSeconds: 1,
    }), 3000), /Connection timeout/)
})
test('bounded stdout/stderr resume without loss; ACKs cannot be duplicated', async t => {
    const { client, channel } = await session(t)
    channel.setReceiveBufferLimit(64 * 1024)
    const held = []
    const counts = new Array(256).fill(0)
    let bytes = 0
    let autoAck = false
    let invalidAcks = 0
    const consume = data => {
        bytes += data.length
        for (const byte of data) { counts[byte]++ }
        if (autoAck) {
            if (!channel.acknowledgeData(data)) { invalidAcks++ }
        } else { held.push(data) }
    }
    const subscriptions = [channel.data$.subscribe(consume), channel.extendedData$.subscribe(([, data]) => consume(data))]
    t.after(() => subscriptions.forEach(s => s.unsubscribe()))
    const closed = new Promise(resolve => channel.closed$.subscribe(resolve))
    await deadline(channel.requestShell())
    await eventually(() => bytes >= 64 * 1024)
    await delay(75)
    assert.equal(bytes, 64 * 1024)
    assert.deepEqual(client.receiveBufferStats(), { pendingBytes: 64 * 1024, pendingPackets: 4, managedChannels: 1 })
    assert.equal(channel.acknowledgeData(Uint8Array.from(held[0])), false)
    assert.equal(channel.acknowledgeData(held[0]), true)
    assert.equal(channel.acknowledgeData(held[0]), false)
    autoAck = true
    for (const chunk of held.slice(1)) { assert.equal(channel.acknowledgeData(chunk), true) }
    await deadline(closed)
    assert.equal(bytes, 8 * 1024 * 1024)
    const expected = new Array(256).fill(0)
    for (let i = 0; i < 512; i++) { expected[i % 251] += 16 * 1024 }
    assert.deepEqual(counts, expected)
    assert.equal(invalidAcks, 0)
    assert.deepEqual(client.receiveBufferStats(), { pendingBytes: 0, pendingPackets: 0, managedChannels: 0 })
    await deadline(channel.close())
    await deadline(channel.close())
    assert.equal(client.receiveBufferStats().managedChannels, 0, 'late close must not recreate receive state after remote close')
})
test('one oversized packet progresses; closing a paused channel releases credit', async t => {
    const { client, channel } = await session(t)
    channel.setReceiveBufferLimit(1024)
    const chunks = []
    const subscriptions = [channel.data$.subscribe(data => chunks.push(data)), channel.extendedData$.subscribe(([, data]) => chunks.push(data))]
    t.after(() => subscriptions.forEach(s => s.unsubscribe()))
    await deadline(channel.requestShell())
    await eventually(() => chunks.length > 0)
    await delay(30)
    assert.equal(chunks.length, 1)
    assert.equal(client.receiveBufferStats().pendingBytes, 16 * 1024)
    await deadline(channel.close())
    assert.equal(client.receiveBufferStats().pendingBytes, 0)
    assert.equal(channel.acknowledgeData(chunks[0]), false)
})
test('disconnect with full receive budget does not wait for ACK', async t => {
    const { client, channel } = await session(t)
    channel.setReceiveBufferLimit(1024)
    await deadline(channel.requestShell())
    await eventually(() => client.receiveBufferStats().pendingBytes > 0)
    await deadline(client.disconnect(), 1000)
    assert.equal(client.receiveBufferStats().pendingBytes, 0)
})
test('disabling flow control wakes a paused reader and preserves output', async t => {
    const { client, channel } = await session(t)
    assert.throws(() => channel.setReceiveBufferLimit(-1), RangeError)
    assert.throws(() => channel.setReceiveBufferLimit(1.5), RangeError)
    assert.throws(() => channel.setReceiveBufferLimit(8 * 1024 * 1024 + 1), RangeError)
    channel.setReceiveBufferLimit(1024)
    let bytes = 0
    const subscriptions = [channel.data$.subscribe(data => { bytes += data.length }), channel.extendedData$.subscribe(([, data]) => { bytes += data.length })]
    t.after(() => subscriptions.forEach(s => s.unsubscribe()))
    await deadline(channel.requestShell())
    await eventually(() => bytes > 0)
    channel.setReceiveBufferLimit(0)
    await eventually(() => bytes === 8 * 1024 * 1024)
    assert.equal(client.receiveBufferStats().pendingBytes, 0)
})
test('cancelling a proxy command terminates and reaps the child', async t => {
    const fs = require('node:fs')
    const os = require('node:os')
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'russh-cancel-'))
    const pidFile = path.join(directory, 'pid')
    const cancellation = cancellationFor(t)
    const transport = await SshTransport.newCommand(process.execPath, ['-e',
        'require("node:fs").writeFileSync(process.argv[1], String(process.pid)); setInterval(() => {}, 1000)', pidFile,
    ], cancellation)
    t.after(async () => {
        cancellation.cancel()
        await transport.close()
        fs.rmSync(directory, { recursive: true, force: true })
    })
    const result = settled(SSHClient.connect(transport, async () => true, { cancellation }))
    await eventually(() => fs.existsSync(pidFile), 'proxy child startup')
    const pid = Number(fs.readFileSync(pidFile, 'utf8'))
    cancellation.cancel()
    assert.match(String((await deadline(result)).error), /cancelled/i)
    await eventually(() => {
        try { process.kill(pid, 0); return false } catch (error) {
            assert.equal(error.code, 'ESRCH')
            return true
        }
    }, 'proxy child termination')
})
