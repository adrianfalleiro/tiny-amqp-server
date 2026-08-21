import net from 'node:net';
import type { Duplex } from 'node:stream';
import { Broker } from './broker/broker';
import { AmqpConnection, type ConnectionOptions } from './connection';

export interface AmqpServerOptions extends ConnectionOptions {}

/**
 * A small in-memory AMQP 0-9-1 server. Point an amqplib client at
 * `amqp://localhost:<port>` and use it like a (very forgiving) RabbitMQ.
 */
export class AmqpServer {
  readonly broker = new Broker();
  private readonly netServer: net.Server;
  private readonly connections = new Set<AmqpConnection>();

  constructor(private readonly options: AmqpServerOptions = {}) {
    this.netServer = net.createServer((socket) => {
      socket.setNoDelay(true);
      this.attachSocket(socket);
    });
  }

  /**
   * Serve AMQP over an arbitrary duplex stream — the transport behind
   * in-memory (no-TCP) connections; see `openInMemorySocket`/`connectInMemory`.
   */
  attachSocket(socket: Duplex): AmqpConnection {
    const connection = new AmqpConnection(socket, this.broker, this.options);
    this.connections.add(connection);
    socket.on('close', () => this.connections.delete(connection));
    return connection;
  }

  /** Listen and resolve with the bound port (pass 0 for an ephemeral one). */
  listen(port = 0, host = '127.0.0.1'): Promise<{ port: number; host: string }> {
    return new Promise((resolve, reject) => {
      this.netServer.once('error', reject);
      this.netServer.listen(port, host, () => {
        this.netServer.removeListener('error', reject);
        const address = this.netServer.address() as net.AddressInfo;
        resolve({ port: address.port, host });
      });
    });
  }

  get port(): number | null {
    const address = this.netServer.address();
    return address && typeof address === 'object' ? address.port : null;
  }

  /** Stop accepting connections and close existing ones. */
  async close(): Promise<void> {
    for (const connection of this.connections) connection.closeGracefully();
    if (!this.netServer.listening) return; // in-memory only, nothing to unbind
    await new Promise<void>((resolve, reject) => {
      this.netServer.close((err) => (err ? reject(err) : resolve()));
      // net.Server.close waits for sockets; nudge any stragglers shut
      setTimeout(() => {
        for (const connection of this.connections) connection.closeGracefully();
      }, 100).unref();
    });
  }
}
