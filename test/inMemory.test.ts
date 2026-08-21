import net from 'node:net';
import amqp from 'amqplib';
import { afterEach, describe, expect, it } from 'vitest';
import { AmqpServer, connectInMemory } from '../src/index';

const servers: AmqpServer[] = [];

function makeServer(): AmqpServer {
  const server = new AmqpServer();
  servers.push(server);
  return server;
}

afterEach(async () => {
  for (const server of servers.splice(0)) await server.close();
});

describe('in-memory transport (no TCP)', () => {
  it('round-trips publish/consume without listening on any port', async () => {
    const server = makeServer(); // note: no listen()
    expect(server.port).toBeNull();

    const connection = await connectInMemory(amqp, server);
    const channel = await connection.createChannel();
    await channel.assertQueue('ram-only');
    channel.sendToQueue('ram-only', Buffer.from('no sockets were harmed'), {
      headers: { transport: 'duplexPair' },
    });

    const msg = await new Promise<amqp.ConsumeMessage>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out')), 2000);
      channel.consume('ram-only', (m) => {
        if (m) {
          clearTimeout(timer);
          resolve(m);
        }
      });
    });
    expect(msg.content.toString()).toBe('no sockets were harmed');
    expect(msg.properties.headers).toMatchObject({ transport: 'duplexPair' });
    await connection.close();
  });

  it('restores net.connect after the connect call', async () => {
    const original = net.connect;
    const server = makeServer();
    const connection = await connectInMemory(amqp, server);
    expect(net.connect).toBe(original);
    await connection.close();
  });

  it('supports several concurrent in-memory connections with shared broker state', async () => {
    const server = makeServer();
    const producer = await connectInMemory(amqp, server);
    const consumer = await connectInMemory(amqp, server);

    const producerChannel = await producer.createConfirmChannel();
    await producerChannel.assertQueue('shared');
    const consumerChannel = await consumer.createChannel();

    const received = new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out')), 2000);
      consumerChannel.consume('shared', (m) => {
        if (m) {
          clearTimeout(timer);
          consumerChannel.ack(m);
          resolve(m.content.toString());
        }
      });
    });
    await new Promise<void>((resolve, reject) =>
      producerChannel.sendToQueue('shared', Buffer.from('cross-connection'), {}, (err) =>
        err ? reject(err) : resolve(),
      ),
    );
    expect(await received).toBe('cross-connection');
    await producer.close();
    await consumer.close();
  });

  it('server.close() tears down in-memory connections', async () => {
    const server = makeServer();
    const connection = await connectInMemory(amqp, server);
    const closed = new Promise<void>((resolve) => connection.on('close', () => resolve()));
    connection.on('error', () => {}); // forced shutdown surfaces as an error too
    await server.close();
    await closed;
  });
});
