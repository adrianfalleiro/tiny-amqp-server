import type { Duplex } from 'node:stream';
import type { Broker } from './broker/broker';
import { Channel, type ChannelHost } from './channel';
import { ConnectionError } from './errors';
import {
  constants,
  decodeFrame,
  encodeMethod,
  Heart,
  HEARTBEAT,
  HEARTBEAT_BUF,
  type MethodFields,
  type MethodFrame,
  methodName,
  methods,
  parseFrame,
  PROTOCOL_HEADER,
} from './wire';

export interface ConnectionOptions {
  /** Heartbeat interval in seconds offered in Connection.Tune. 0 disables (default). */
  heartbeat?: number;
  serverProperties?: MethodFields;
}

const FRAME_HEADER_BYTES = 7; // type + channel + size, enough to peek a frame's size
const DEFAULT_FRAME_MAX = 131072;
const DEFAULT_CHANNEL_MAX = 2047;
const CLOSE_OK_TIMEOUT_MS = 2000;

/** 0 means "no limit" on either side; otherwise the smaller value wins. */
function negotiate(server: number, client: number): number {
  if (server === 0 || client === 0) return Math.max(server, client);
  return Math.min(server, client);
}

type ConnectionState =
  | 'awaiting-protocol-header'
  | 'awaiting-start-ok'
  | 'awaiting-tune-ok'
  | 'awaiting-open'
  | 'open'
  | 'closing' // we sent Connection.Close, waiting for CloseOk
  | 'closed';

let connectionCounter = 0;

export class AmqpConnection implements ChannelHost {
  readonly connectionId = `conn-${++connectionCounter}`;
  frameMax = DEFAULT_FRAME_MAX;
  channelMax = DEFAULT_CHANNEL_MAX;
  heartbeatSeconds = 0;

  private state: ConnectionState = 'awaiting-protocol-header';
  private rest: Buffer = Buffer.alloc(0);
  private readonly channels = new Map<number, Channel>();
  private heart: Heart | null = null;
  private sentSinceCheck = false;
  private recvSinceCheck = false;

  constructor(
    // Anything duplex works — a net.Socket or an in-memory stream pair
    private readonly socket: Duplex,
    readonly broker: Broker,
    private readonly options: ConnectionOptions = {},
  ) {
    socket.on('data', (chunk) => this.onData(chunk));
    socket.on('error', () => socket.destroy());
    socket.on('close', () => this.teardown());
  }

  // ------------------------------------------------------------- ChannelHost

  send(buf: Buffer): void {
    if (this.state === 'closed' || this.socket.destroyed) return;
    this.sentSinceCheck = true;
    this.socket.write(buf);
  }

  channelClosed(channelNumber: number): void {
    this.channels.delete(channelNumber);
  }

  // ------------------------------------------------------------------ input

  private onData(chunk: Buffer): void {
    if (this.state === 'closed') return;
    this.recvSinceCheck = true;
    this.rest = this.rest.length === 0 ? chunk : Buffer.concat([this.rest, chunk]);

    if (this.state === 'awaiting-protocol-header') {
      if (this.rest.length < PROTOCOL_HEADER.length) return;
      const header = this.rest.subarray(0, PROTOCOL_HEADER.length).toString('latin1');
      if (header !== PROTOCOL_HEADER) {
        // Spec: reply with the protocol we do speak, then hang up
        this.socket.write(PROTOCOL_HEADER, 'latin1');
        this.socket.end();
        this.state = 'closed';
        return;
      }
      this.rest = this.rest.subarray(PROTOCOL_HEADER.length);
      this.sendMethod0(methods.ConnectionStart, {
        versionMajor: 0,
        versionMinor: 9,
        serverProperties: {
          product: 'tiny-amqp-server',
          platform: `Node.JS ${process.version}`,
          capabilities: {
            publisher_confirms: true,
            basic_nack: true,
            'basic.nack': true,
            consumer_cancel_notify: false,
            exchange_exchange_bindings: false,
          },
          ...this.options.serverProperties,
        },
        mechanisms: Buffer.from('PLAIN'),
        locales: Buffer.from('en_US'),
      });
      this.state = 'awaiting-start-ok';
    }

    try {
      this.parseLoop();
    } catch (err) {
      if (err instanceof ConnectionError) {
        this.failConnection(err);
      } else {
        // Malformed frame (bad frame-end byte, unknown method id, ...)
        this.failConnection(
          new ConnectionError(constants.FRAME_ERROR, (err as Error).message ?? 'framing error'),
        );
      }
    }
  }

  private parseLoop(): void {
    for (;;) {
      if (this.state === 'closed') return;
      if (this.rest.length >= FRAME_HEADER_BYTES) {
        // parseFrame does not bound the size — a bogus 4 GiB frame would
        // buffer forever, so enforce the negotiated frameMax up front
        const size = this.rest.readUInt32BE(3);
        if (size > this.frameMax - 8) {
          throw new ConnectionError(
            constants.FRAME_ERROR,
            `frame size ${size} exceeds frameMax ${this.frameMax}`,
          );
        }
      }
      const frame = parseFrame(this.rest);
      if (frame === false) return;
      this.rest = frame.rest;
      const decoded = decodeFrame(frame);
      if (decoded === HEARTBEAT) continue; // activity already recorded
      if (frame.channel === 0) {
        if (frame.type !== constants.FRAME_METHOD) {
          throw new ConnectionError(
            constants.UNEXPECTED_FRAME,
            `non-method frame of type ${frame.type} on channel 0`,
          );
        }
        this.acceptChannel0(decoded as MethodFrame);
      } else {
        this.acceptChannelFrame(frame.channel, decoded);
      }
    }
  }

  // ------------------------------------------------------- channel 0 methods

  private sendMethod0(id: number, fields: MethodFields): void {
    this.send(encodeMethod(id, 0, fields));
  }

  private acceptChannel0(frame: MethodFrame): void {
    const { id, fields } = frame;

    // The client may close at any point after the handshake starts
    if (id === methods.ConnectionClose) {
      this.shutdownChannels();
      this.sendMethod0(methods.ConnectionCloseOk, {});
      this.state = 'closed';
      this.socket.end();
      return;
    }
    if (id === methods.ConnectionCloseOk) {
      if (this.state === 'closing') {
        this.state = 'closed';
        this.socket.end();
      }
      return;
    }
    if (this.state === 'closing') return; // discard everything else until CloseOk

    switch (this.state) {
      case 'awaiting-start-ok':
        this.expectMethod(id, methods.ConnectionStartOk);
        // Test double: accept any credentials
        this.sendMethod0(methods.ConnectionTune, {
          channelMax: DEFAULT_CHANNEL_MAX,
          frameMax: DEFAULT_FRAME_MAX,
          heartbeat: this.options.heartbeat ?? 0,
        });
        this.state = 'awaiting-tune-ok';
        return;
      case 'awaiting-tune-ok': {
        this.expectMethod(id, methods.ConnectionTuneOk);
        const clientFrameMax = (fields.frameMax as number) ?? 0;
        const clientChannelMax = (fields.channelMax as number) ?? 0;
        if (clientFrameMax !== 0 && clientFrameMax < constants.FRAME_MIN_SIZE) {
          throw new ConnectionError(
            constants.NOT_ALLOWED,
            `frameMax ${clientFrameMax} is below the protocol minimum ${constants.FRAME_MIN_SIZE}`,
          );
        }
        this.frameMax = negotiate(DEFAULT_FRAME_MAX, clientFrameMax);
        this.channelMax = negotiate(DEFAULT_CHANNEL_MAX, clientChannelMax) || 0xffff;
        this.heartbeatSeconds = (fields.heartbeat as number) ?? 0;
        this.state = 'awaiting-open';
        return;
      }
      case 'awaiting-open':
        this.expectMethod(id, methods.ConnectionOpen);
        this.sendMethod0(methods.ConnectionOpenOk, {});
        this.state = 'open';
        if (this.heartbeatSeconds > 0) this.startHeartbeat();
        return;
      case 'open':
        throw new ConnectionError(
          constants.COMMAND_INVALID,
          `unexpected method ${methodName(id)} on channel 0`,
          id,
        );
      default:
        throw new ConnectionError(
          constants.COMMAND_INVALID,
          `method ${methodName(id)} in connection state ${this.state}`,
          id,
        );
    }
  }

  private expectMethod(got: number, want: number): void {
    if (got !== want) {
      throw new ConnectionError(
        constants.COMMAND_INVALID,
        `expected ${methodName(want)}, got ${methodName(got)}`,
        got,
      );
    }
  }

  // ---------------------------------------------------------- channels 1..N

  private acceptChannelFrame(channelNumber: number, decoded: ReturnType<typeof decodeFrame>): void {
    if (this.state !== 'open') {
      throw new ConnectionError(
        constants.COMMAND_INVALID,
        `frame on channel ${channelNumber} before connection is open`,
      );
    }
    const existing = this.channels.get(channelNumber);
    if ('fields' in decoded && 'id' in decoded && !('size' in decoded) && decoded.id === methods.ChannelOpen) {
      if (existing) {
        throw new ConnectionError(
          constants.CHANNEL_ERROR,
          `channel ${channelNumber} is already open`,
          methods.ChannelOpen,
        );
      }
      if (channelNumber > this.channelMax) {
        throw new ConnectionError(
          constants.CHANNEL_ERROR,
          `channel ${channelNumber} exceeds negotiated channelMax ${this.channelMax}`,
          methods.ChannelOpen,
        );
      }
      this.channels.set(channelNumber, new Channel(channelNumber, this));
      return;
    }
    if (!existing) {
      throw new ConnectionError(
        constants.CHANNEL_ERROR,
        `frame on unopened channel ${channelNumber}`,
      );
    }
    existing.accept(decoded);
  }

  // ------------------------------------------------------------- heartbeats

  private startHeartbeat(): void {
    this.heart = new Heart(
      this.heartbeatSeconds,
      () => {
        const sent = this.sentSinceCheck;
        this.sentSinceCheck = false;
        return sent;
      },
      () => {
        const received = this.recvSinceCheck;
        this.recvSinceCheck = false;
        return received;
      },
    );
    this.heart.on('beat', () => this.send(HEARTBEAT_BUF));
    this.heart.on('timeout', () => this.socket.destroy());
  }

  // ---------------------------------------------------------------- closing

  /** Server-initiated connection close for a fatal protocol error. */
  private failConnection(err: ConnectionError): void {
    if (this.state === 'closed' || this.socket.destroyed) return;
    if (this.state === 'closing') {
      this.socket.destroy();
      return;
    }
    this.shutdownChannels();
    // Mid-handshake failures can't be reported in-protocol reliably; just hang up
    if (this.state === 'awaiting-protocol-header') {
      this.state = 'closed';
      this.socket.destroy();
      return;
    }
    this.sendMethod0(methods.ConnectionClose, {
      replyCode: err.replyCode,
      replyText: err.replyText,
      classId: err.classId,
      methodId: err.methodId,
    });
    this.state = 'closing';
    const timer = setTimeout(() => this.socket.destroy(), CLOSE_OK_TIMEOUT_MS);
    timer.unref();
    this.socket.once('close', () => clearTimeout(timer));
  }

  /** Ask the client to go away (server.close()). */
  closeGracefully(): void {
    if (this.state !== 'open') {
      this.socket.destroy();
      return;
    }
    this.failConnection(
      new ConnectionError(constants.CONNECTION_FORCED, 'server is shutting down'),
    );
  }

  private shutdownChannels(): void {
    for (const channel of this.channels.values()) channel.shutdown();
    this.channels.clear();
  }

  /** Idempotent final cleanup once the socket is gone. */
  private teardown(): void {
    if (this.heart) {
      this.heart.clear();
      this.heart = null;
    }
    this.shutdownChannels();
    this.broker.cleanupConnection(this.connectionId);
    this.state = 'closed';
  }
}
