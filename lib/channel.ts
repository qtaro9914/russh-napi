import * as russh from './native'
import { Observable } from 'rxjs'
import { Destructible } from './helpers'
import { ClientEventInterface } from './events'

export class NewChannel {
    constructor (private inner: russh.NewSshChannel | null) {}

    take (): russh.NewSshChannel {
        if (this.inner === null) {
            throw new Error('Channel already taken')
        }
        const inner = this.inner
        this.inner = null
        return inner
    }
}

export interface PTYSize {
    columns: number,
    rows: number,
    pixWidth: number,
    pixHeight: number,
}

export interface X11Options {
    singleConnection: boolean,
    authProtocol: string,
    authCookie: string,
    screenNumber: number,
}

export class Channel extends Destructible {
    readonly data$: Observable<Uint8Array>
    readonly extendedData$: Observable<[number, Uint8Array]>
    readonly eof$: Observable<void>
    readonly closed$: Observable<void>

    constructor(
        public readonly id: number,
        private inner: russh.SshChannel,
        private events: ClientEventInterface,
        private owner?: russh.SshClient,
    ) {
        super()
        this.data$ = events.data$.subscribe(id)
        this.extendedData$ = events.extendedData$.subscribe(id)
        this.eof$ = events.eof$.subscribe(id)
        this.closed$ = events.close$.subscribe(id)
    }

    async take(): Promise<russh.SshChannel> {
        this.assertNotDestructed()
        this.owner?.setReceiveBufferLimit(this.id, 0)
        this.destruct()
        return this.inner
    }

    async requestShell(): Promise<void> {
        this.assertNotDestructed()
        await this.inner.requestShell()
    }

    async requestExec(command: string): Promise<void> {
        this.assertNotDestructed()
        await this.inner.requestExec(command)
    }

    async requestAgentForwarding(): Promise<void> {
        this.assertNotDestructed()
        await this.inner.requestAgentForwarding()
    }

    async requestPTY(
        terminal: string,
        opts: PTYSize,
    ): Promise<void> {
        this.assertNotDestructed()
        await this.inner.requestPty(
            terminal,
            opts.columns,
            opts.rows,
            opts.pixWidth,
            opts.pixHeight
        )
    }

    async requestX11Forwarding(options: X11Options): Promise<void> {
        this.assertNotDestructed()
        await this.inner.requestX11Forwarding(
            options.singleConnection,
            options.authProtocol,
            options.authCookie,
            options.screenNumber
        )
    }

    async resizePTY(size: PTYSize): Promise<void> {
        this.assertNotDestructed()
        await this.inner.windowChange(
            size.columns,
            size.rows,
            size.pixWidth,
            size.pixHeight
        )
    }

    async write(data: Uint8Array): Promise<void> {
        this.assertNotDestructed()
        await this.inner.data(data)
    }

    async eof(): Promise<void> {
        await this.inner.eof()
    }

    async close(): Promise<void> {
        this.owner?.stopReceiving(this.id)
        await this.inner.close()
    }
    /**
     * Opt in before requesting output. ACK each original stdout/stderr chunk
     * after consumption. A full budget pauses the shared SSH connection.
     * Zero restores legacy unthrottled delivery.
     */
    setReceiveBufferLimit(bytes: number): void {
        this.assertNotDestructed()
        if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > 8 * 1024 * 1024) {
            throw new RangeError('Receive limit must be an integer between 0 and 8 MiB')
        }
        if (!this.owner) { throw new Error('Channel has no receive controller') }
        this.owner.setReceiveBufferLimit(this.id, bytes)
    }

    /** ACK exactly once, using the original received array. */
    acknowledgeData(data: Uint8Array): boolean {
        const ticket = this.events.takeDataTicket(this.id, data)
        return ticket !== null && (this.owner?.acknowledgeReceivedData(ticket) ?? false)
    }

}
