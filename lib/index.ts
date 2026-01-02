import { Observable, map } from 'rxjs'
import { Destructible } from './helpers'
import { SFTP } from './sftp'
import { Channel, NewChannel } from './channel'
import { ClientEventInterface } from './events'

import russh, { SshKeyPair, KeyboardInteractiveAuthenticationPrompt, SshClient, SshChannel, SshPublicKey, SshTransport, HashAlgorithm } from './native'
import { AgentConnectionSpec, makeRusshAgentConnection } from './agent'

export class KeyPair {
    private constructor(protected inner: SshKeyPair) { }

    get algorithm () {
        return this.inner.publicKey().algorithm()
    }

    static async parse(data: string, passphrase?: string): Promise<KeyPair> {
        return new KeyPair(await russh.parseKey(data, passphrase))
    }
}

export interface X11ChannelOpenEvent {
    readonly channel: NewChannel
    readonly clientAddress: string
    readonly clientPort: number
}

export interface TCPChannelOpenEvent {
    readonly channel: NewChannel
    readonly targetAddress: string
    readonly targetPort: number
    readonly clientAddress: string
    readonly clientPort: number
}


export type KeyboardInteractiveAuthenticationState = {
    state: 'failure'
    remainingMethods: string[]
} | {
    state: 'infoRequest'
    name: string
    instructions: string
    prompts: () => KeyboardInteractiveAuthenticationPrompt[]
}

export interface AuthFailure {
    remainingMethods: string[]
}

export interface Config {
    preferred?: {
        ciphers?: string[]
        kex?: string[],
        key?: string[],
        mac?: string[],
        compression?: string[],
    },
    connectionTimeoutSeconds?: number,
    keepaliveIntervalSeconds?: number,
    keepaliveCountMax?: number,
}

export class SSHClient extends Destructible {
    readonly disconnect$ = this.events.disconnect$.asObservable()
    readonly banner$ = this.events.banner$.asObservable()

    private constructor(
        private client: SshClient,
        private events: ClientEventInterface,
    ) { super() }

    static async connect(
        transport: SshTransport,
        serverKeyCallback: (key: SshPublicKey) => Promise<boolean>,
        config?: Config,
    ): Promise<SSHClient> {
        const eventInterface = new ClientEventInterface()
        const russhClient = await russh.connect(
            transport,
            config?.preferred?.ciphers,
            config?.preferred?.kex,
            config?.preferred?.key,
            config?.preferred?.mac,
            config?.preferred?.compression,
            config?.connectionTimeoutSeconds,
            config?.keepaliveIntervalSeconds,
            config?.keepaliveCountMax ?? 3,
            (_, k) => serverKeyCallback(k),
            eventInterface.dataCallback,
            eventInterface.extendedDataCallback,
            eventInterface.eofCallback,
            eventInterface.closeCallback,
            eventInterface.disconnectCallback,
            eventInterface.x11ChannelOpenCallback,
            eventInterface.tcpChannelOpenCallback,
            eventInterface.agentChannelOpenCallback,
            eventInterface.bannerCallback,
        )

        eventInterface.disconnect$.subscribe(() => {
            setTimeout(() => eventInterface.complete())
        })

        return new SSHClient(russhClient, eventInterface)
    }

    protected override destruct(): void {
        super.destruct()
    }

    async authenticateNone(username: string): Promise<AuthenticatedSSHClient | AuthFailure> {
        this.assertNotDestructed()
        const result = await this.client.authenticateNone(username)
        if (result.success) {
            return this.intoAuthenticated()
        }
        return result
    }

    async authenticateWithPassword(username: string, password: string): Promise<AuthenticatedSSHClient | AuthFailure> {
        this.assertNotDestructed()
        const result = await this.client.authenticatePassword(username, password)
        if (result.success) {
            return this.intoAuthenticated()
        }
        return result
    }

    private _hashAlg (hashAlgorithm: 'sha1' | 'sha256' | 'sha512' | null): HashAlgorithm | null {
        return hashAlgorithm ? {
            sha1: HashAlgorithm.Sha1,
            sha256: HashAlgorithm.Sha256,
            sha512: HashAlgorithm.Sha512,
        }[hashAlgorithm] : null
    }

    async authenticateWithKeyPair(username: string, keyPair: KeyPair, hashAlgorithm: 'sha1' | 'sha256' | 'sha512' | null): Promise<AuthenticatedSSHClient | AuthFailure> {
        this.assertNotDestructed()
        const result = await this.client.authenticatePublickey(username, keyPair['inner'], this._hashAlg(hashAlgorithm))
        if (result.success) {
            return this.intoAuthenticated()
        }
        return result
    }

    async startKeyboardInteractiveAuthentication(username: string): Promise<KeyboardInteractiveAuthenticationState> {
        this.assertNotDestructed()
        return await this.client.startKeyboardInteractiveAuthentication(username) as KeyboardInteractiveAuthenticationState
    }

    async continueKeyboardInteractiveAuthentication(responses: string[]): Promise<AuthenticatedSSHClient | KeyboardInteractiveAuthenticationState> {
        this.assertNotDestructed()
        const result = await this.client.respondToKeyboardInteractiveAuthentication(responses)
        if (result.state === 'success') {
            return this.intoAuthenticated()
        }
        return result as KeyboardInteractiveAuthenticationState
    }

    async authenticateWithAgent(
        username: string,
        connection: AgentConnectionSpec,
    ): Promise<AuthenticatedSSHClient | AuthFailure> {
        this.assertNotDestructed()
        const result = await this.client.authenticateAgent(
            username,
            makeRusshAgentConnection(connection),
        )
        if (result.success) {
            return this.intoAuthenticated()
        }
        return result
    }

    /**
     * Authenticate using SSH agent with a specific public key identity.
     * This is useful when you have multiple keys in the agent and want to use a specific one,
     * avoiding the server's authentication attempt limit.
     * @param username SSH username
     * @param connection Agent connection specification
     * @param publicKey The specific public key to use for authentication
     */
    async authenticateWithAgentIdentity(
        username: string,
        connection: AgentConnectionSpec,
        publicKey: SshPublicKey,
    ): Promise<AuthenticatedSSHClient | AuthFailure> {
        this.assertNotDestructed()
        const result = await this.client.authenticateAgentWithIdentity(
            username,
            makeRusshAgentConnection(connection),
            publicKey,
        )
        if (result.success) {
            return this.intoAuthenticated()
        }
        return result
    }

    async disconnect(): Promise<void> {
        this.destruct()
        await this.client.disconnect()
    }

    private intoAuthenticated(): AuthenticatedSSHClient {
        this.destruct()
        return new AuthenticatedSSHClient(this.client, this.events)
    }
}

export class AuthenticatedSSHClient extends Destructible {
    readonly disconnect$: Observable<void> = this.events.disconnect$
    readonly x11ChannelOpen$: Observable<X11ChannelOpenEvent> =
        this.events.x11ChannelOpen$.pipe(map(([ch, address, port]) => ({
            channel: new NewChannel(ch),
            clientAddress: address,
            clientPort: port,
        })))

    readonly tcpChannelOpen$: Observable<TCPChannelOpenEvent> = this.events.tcpChannelOpen$.pipe(map(([
        ch,
        targetAddress,
        targetPort,
        clientAddress,
        clientPort,
    ]) => ({
        channel: new NewChannel(ch),
        targetAddress,
        targetPort,
        clientAddress,
        clientPort,
    })))

    readonly agentChannelOpen$: Observable<NewChannel> = this.events.agentChannelOpen$.pipe(map(([ch]) => new NewChannel(ch)))

    constructor(
        private client: SshClient,
        private events: ClientEventInterface,
    ) { super() }

    async openSessionChannel(): Promise<NewChannel> {
        return await new NewChannel(await this.client.channelOpenSession())
    }

    async openTCPForwardChannel(options: {
        addressToConnectTo: string,
        portToConnectTo: number,
        originatorAddress: string,
        originatorPort: number,
    }): Promise<NewChannel> {
        return new NewChannel(await this.client.channelOpenDirectTcpip(
            options.addressToConnectTo,
            options.portToConnectTo,
            options.originatorAddress,
            options.originatorPort,
        ))
    }

    async forwardTCPPort(
        addressToBind: string,
        portToBind: number,
    ): Promise<number> {
        return await this.client.tcpipForward(
            addressToBind,
            portToBind,
        )
    }

    async stopForwardingTCPPort(
        addressToBind: string,
        portToBind: number,
    ): Promise<void> {
        await this.client.cancelTcpipForward(
            addressToBind,
            portToBind,
        )
    }

    async disconnect(): Promise<void> {
        this.destruct()
        await this.client.disconnect()
    }

    async activateChannel (ch: NewChannel): Promise<Channel> {
        let channel = await ch.take().activate()
        return this.wrapChannel(channel)
    }

    async activateSFTP (ch: NewChannel): Promise<SFTP> {
        let channel = await ch.take().activateSftp()
        return new SFTP(channel, this.events)
    }

    private async wrapChannel(channel: SshChannel): Promise<Channel> {
        let id = await channel.id()
        return new Channel(id, channel, this.events)
    }
}

export {
    KeyboardInteractiveAuthenticationPrompt,
    SshPublicKey,
    SshTransport,
    SshChannel,
    NewSshChannel,
    SftpFileType as SFTPFileType,
    supportedCiphers as getSupportedCiphers,
    supportedKexAlgorithms as getSupportedKexAlgorithms,
    supportedMacs as getSupportedMACs,
    supportedCompressionAlgorithms as getSupportedCompressionAlgorithms,
    supportedKeyTypes as getSupportedKeyTypes,
    OPEN_APPEND, OPEN_CREATE, OPEN_READ, OPEN_TRUNCATE, OPEN_WRITE,
    SftpFile as SFTPFile,
    isPageantRunning,
    HashAlgorithm,
    parsePublicKey,
} from './native'
export {
    SFTP, SFTPDirectoryEntry, SFTPMetadata,
} from './sftp'
export { AgentConnectionSpec, SSHAgentStream } from './agent'
export { Channel, NewChannel }
