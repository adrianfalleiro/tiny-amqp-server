import net from 'node:net';
import { Duplex } from 'node:stream';
import type { AmqpServer } from './server';

/**
 * In-memory transport: connect an amqplib client to an AmqpServer without a
 * TCP socket, a port, or the network stack at all.
 *
 * amqplib's client `Connection` runs over any duplex stream; only its
 * `connect.js` insists on creating a real socket (via `net.connect`). So we
 * build a cross-wired pair of Duplex streams — one end is handed to the
 * server, the other impersonates a net.Socket for the client.
 */

/**
 * Two duplex streams where writes to one side are reads on the other.
 *
 * Delivery is deferred with setImmediate — one event-loop turn of "latency",
 * like a real socket. Synchronous delivery breaks amqplib: its close path
 * sends Connection.Close and *then* swaps in the frame handler that expects
 * CloseOk, relying on the network round-trip taking at least a tick. All
 * transfers (data, EOF, destroy) go through the same queue, so ordering is
 * preserved.
 */
export function duplexPair(): [Duplex, Duplex] {
  let a: Duplex;
  let b: Duplex;
  const make = (peer: () => Duplex) =>
    new Duplex({
      read() {},
      write(chunk: Buffer, _encoding, callback) {
        setImmediate(() => {
          const other = peer();
          if (!other.destroyed) other.push(chunk);
        });
        callback();
      },
      final(callback) {
        setImmediate(() => {
          const other = peer();
          if (!other.destroyed) other.push(null);
        });
        callback();
      },
      destroy(error, callback) {
        setImmediate(() => {
          const other = peer();
          // destroyed is set before _destroy runs, so this cannot recurse
          if (!other.destroyed) other.destroy();
        });
        callback(error);
      },
    });
  a = make(() => b);
  b = make(() => a);
  return [a, b];
}

/** The net.Socket surface amqplib's connect.js touches on top of Duplex. */
export type FakeSocket = Duplex & {
  setNoDelay(noDelay?: boolean): FakeSocket;
  setKeepAlive(enable?: boolean, delay?: number): FakeSocket;
  setTimeout(timeout: number, callback?: () => void): FakeSocket;
  ref(): FakeSocket;
  unref(): FakeSocket;
};

function asFakeSocket(stream: Duplex): FakeSocket {
  const socket = stream as FakeSocket;
  const noop = () => socket;
  socket.setNoDelay = noop;
  socket.setKeepAlive = noop;
  socket.setTimeout = noop;
  socket.ref = noop;
  socket.unref = noop;
  return socket;
}

/**
 * Attach a new in-memory connection to `server` and return the client end,
 * ready to be driven by an amqplib client `Connection` (or anything else
 * that speaks AMQP over a duplex stream).
 */
export function openInMemorySocket(server: AmqpServer): FakeSocket {
  const [serverSide, clientSide] = duplexPair();
  server.attachSocket(serverSide);
  return asFakeSocket(clientSide);
}

interface AmqplibLike<TConnection> {
  connect(url: string, socketOptions?: unknown): PromiseLike<TConnection>;
}

/**
 * `amqp.connect(...)`, but over an in-memory stream to `server` — no port is
 * opened and `server.listen()` is not required.
 *
 * Works by substituting `net.connect` for the (fully synchronous) duration of
 * the `amqp.connect` call, so amqplib adopts our fake socket instead of
 * dialling TCP. Node is single-threaded and the patch window contains no
 * awaits, so concurrent connects cannot observe the substitution. Verified
 * against amqplib 0.10.x and 2.x, whose connect.js both resolve `net.connect`
 * at call time.
 */
export function connectInMemory<TConnection>(
  amqplib: AmqplibLike<TConnection>,
  server: AmqpServer,
  url = 'amqp://guest:guest@in-memory',
): Promise<TConnection> {
  const clientSocket = openInMemorySocket(server);
  const netModule = net as { connect: typeof net.connect };
  const realConnect = netModule.connect;
  netModule.connect = ((_options: unknown, onConnect?: () => void) => {
    if (onConnect) process.nextTick(onConnect);
    process.nextTick(() => clientSocket.emit('connect'));
    return clientSocket as unknown as net.Socket;
  }) as typeof net.connect;
  try {
    return Promise.resolve(amqplib.connect(url));
  } finally {
    netModule.connect = realConnect;
  }
}
