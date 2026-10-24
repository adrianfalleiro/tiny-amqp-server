import net from 'node:net';
import amqp from 'amqplib';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { AmqpServer, connectInMemory } from '../src/index';

const servers: AmqpServer[] = [];

function makeServer(): AmqpServer {
  const server = new AmqpServer();
  servers.push(server);
  return server;
}

afterEach(async () => {
  vi.useRealTimers();
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

describe('review regressions', () => {
  it.each(['cancel', 'close'] as const)(
    'keeps another channel consumer with the same tag on %s',
    async (action) => {
      const server = makeServer();
      const connection = await connectInMemory(amqp, server);
      const first = await connection.createChannel();
      const second = await connection.createChannel();
      await first.assertQueue('shared-tags');
      const received: string[] = [];
      await first.consume(
        'shared-tags',
        (msg) => {
          if (msg) received.push(msg.content.toString());
        },
        { consumerTag: 'same', noAck: true },
      );
      await second.consume('shared-tags', () => {}, { consumerTag: 'same', noAck: true });
      if (action === 'cancel') await second.cancel('same');
      else await second.close();
      first.sendToQueue('shared-tags', Buffer.from('survivor'));
      await first.checkQueue('shared-tags');
      expect(received).toEqual(['survivor']);
      await connection.close();
    },
  );

  it('does not dead-letter messages after deleting their queue', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    const connection = await connectInMemory(amqp, makeServer());
    const channel = await connection.createChannel();
    await channel.assertQueue('dead');
    await channel.assertQueue('doomed', {
      messageTtl: 1000,
      deadLetterExchange: '',
      deadLetterRoutingKey: 'dead',
    });
    channel.sendToQueue('doomed', Buffer.from('deleted'));
    expect((await channel.deleteQueue('doomed')).messageCount).toBe(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(await channel.get('dead', { noAck: true })).toBe(false);
    await connection.close();
  });

  it('rejects direct publishes to internal exchanges but permits dead-letter routing', async () => {
    const connection = await connectInMemory(amqp, makeServer());
    const channel = await connection.createChannel();
    await channel.assertExchange('internal', 'fanout', { internal: true });
    await channel.assertQueue('dead');
    await channel.bindQueue('dead', 'internal', '');
    await channel.assertQueue('source', { deadLetterExchange: 'internal' });
    channel.sendToQueue('source', Buffer.from('dead-letter'));
    const msg = await channel.get('source');
    channel.reject(msg as amqp.GetMessage, false);
    const dead = await channel.get('dead', { noAck: true });
    expect((dead as amqp.GetMessage).content.toString()).toBe('dead-letter');
    channel.on('error', () => {});
    channel.publish('internal', '', Buffer.from('forbidden'));
    await expect(channel.checkQueue('dead')).rejects.toThrow(/403/);
    const fresh = await connection.createChannel();
    expect(await fresh.get('dead', { noAck: true })).toBe(false);
    await connection.close();
  });

  it('rejects changing the internal flag on an existing exchange', async () => {
    const connection = await connectInMemory(amqp, makeServer());
    const channel = await connection.createChannel();
    await channel.assertExchange('internal', 'fanout', { internal: true });
    await channel.assertExchange('internal', 'fanout', { internal: true });
    channel.on('error', () => {});
    await expect(channel.assertExchange('internal', 'fanout', { internal: false })).rejects.toThrow(
      /406/,
    );
    await connection.close();
  });

  it.each(['ack', 'nack'] as const)(
    'rejects an unknown multiple %s without losing outstanding messages',
    async (action) => {
      const connection = await connectInMemory(amqp, makeServer());
      const channel = await connection.createChannel();
      await channel.assertQueue('unacked');
      channel.sendToQueue('unacked', Buffer.from('preserved'));
      const msg = (await channel.get('unacked')) as amqp.GetMessage;
      channel.on('error', () => {});
      channel[action](
        { ...msg, fields: { ...msg.fields, deliveryTag: msg.fields.deliveryTag + 1 } },
        true,
        false,
      );
      await expect(channel.checkQueue('unacked')).rejects.toThrow(/406/);
      const fresh = await connection.createChannel();
      const redelivered = (await fresh.get('unacked', { noAck: true })) as amqp.GetMessage;
      expect(redelivered.content.toString()).toBe('preserved');
      expect(redelivered.fields.redelivered).toBe(true);
      await connection.close();
    },
  );
});

describe('consumer prefetch', () => {
  it('applies non-global prefetch to each new consumer independently', async () => {
    const connection = await connectInMemory(amqp, makeServer());
    const channel = await connection.createChannel();
    await channel.assertQueue('qos');
    await channel.prefetch(1);
    const first: amqp.ConsumeMessage[] = [];
    const second: amqp.ConsumeMessage[] = [];
    await channel.consume('qos', (msg) => {
      if (msg) first.push(msg);
    });
    await channel.prefetch(2);
    await channel.consume('qos', (msg) => {
      if (msg) second.push(msg);
    });
    for (let i = 0; i < 4; i++) channel.sendToQueue('qos', Buffer.from(String(i)));
    expect((await channel.checkQueue('qos')).messageCount).toBe(1);
    expect(first.map((m) => m.content.toString())).toEqual(['0']);
    expect(second.map((m) => m.content.toString())).toEqual(['1', '2']);
    channel.ack(first[0]);
    await channel.checkQueue('qos');
    expect(first.map((m) => m.content.toString())).toEqual(['0', '3']);
    await connection.close();
  });

  it('enforces the global limit across consumers and resumes when removed', async () => {
    const connection = await connectInMemory(amqp, makeServer());
    const channel = await connection.createChannel();
    await channel.assertQueue('global-qos');
    await channel.prefetch(1);
    await channel.prefetch(1, true);
    const received: string[] = [];
    for (let i = 0; i < 2; i++) {
      await channel.consume('global-qos', (msg) => {
        if (msg) received.push(msg.content.toString());
      });
    }
    channel.sendToQueue('global-qos', Buffer.from('a'));
    channel.sendToQueue('global-qos', Buffer.from('b'));
    channel.sendToQueue('global-qos', Buffer.from('c'));
    expect((await channel.checkQueue('global-qos')).messageCount).toBe(2);
    expect(received).toEqual(['a']);
    await channel.prefetch(0, true);
    expect((await channel.checkQueue('global-qos')).messageCount).toBe(1);
    expect(received).toEqual(['a', 'b']);
    await connection.close();
  });
});

it('allows equivalent declarations of built-in exchanges while reserving new amq names', async () => {
  const connection = await connectInMemory(amqp, makeServer());
  const channel = await connection.createChannel();
  channel.on('error', () => {});
  expect(await channel.assertExchange('amq.direct', 'direct')).toEqual({ exchange: 'amq.direct' });
  channel.on('error', () => {});
  await expect(channel.assertExchange('amq.custom', 'direct')).rejects.toThrow(/403/);
  await connection.close();
});
