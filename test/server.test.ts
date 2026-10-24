import net from 'node:net';
import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { AmqpServer } from '../src/index';

let server: AmqpServer;
let url: string;
const openConnections: ChannelModel[] = [];

async function connect(): Promise<ChannelModel> {
  const connection = await amqp.connect(url);
  openConnections.push(connection);
  return connection;
}

/** Wait for the next message delivered to a fresh consumer on `queue`. */
function nextMessage(channel: Channel, queue: string, opts: { noAck?: boolean } = {}) {
  return new Promise<ConsumeMessage>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timed out waiting for message')), 2000);
    channel.consume(
      queue,
      (msg) => {
        if (msg) {
          clearTimeout(timer);
          resolve(msg);
        }
      },
      { noAck: opts.noAck ?? true },
    );
  });
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

beforeEach(async () => {
  server = new AmqpServer();
  const { port } = await server.listen();
  url = `amqp://127.0.0.1:${port}`;
});

afterEach(async () => {
  for (const connection of openConnections.splice(0)) {
    await connection.close().catch(() => {});
  }
  await server.close();
});

describe('connection lifecycle', () => {
  it('completes the handshake and closes cleanly', async () => {
    const connection = await connect();
    const channel = await connection.createChannel();
    await channel.close();
    await connection.close();
    openConnections.length = 0;
  });

  it('rejects a non-AMQP client by echoing the protocol header', async () => {
    const port = server.port!;
    const response = await new Promise<Buffer>((resolve, reject) => {
      const socket = net.connect(port, '127.0.0.1', () => {
        socket.write('GET / HTTP/1.1\r\n\r\n');
      });
      const chunks: Buffer[] = [];
      socket.on('data', (c) => chunks.push(c));
      socket.on('close', () => resolve(Buffer.concat(chunks)));
      socket.on('error', reject);
      setTimeout(() => socket.destroy(), 1500).unref();
    });
    expect(response.toString('latin1')).toBe(`AMQP${String.fromCharCode(0, 0, 9, 1)}`);
  });
});

describe('publish and consume', () => {
  it('round-trips a message through the default exchange with properties intact', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('inbox');
    channel.sendToQueue('inbox', Buffer.from('hello'), {
      contentType: 'application/json',
      correlationId: 'corr-1',
      deliveryMode: 2,
      headers: { tenant: 'classdojo', n: 42 },
      messageId: 'msg-1',
    });
    const msg = await nextMessage(channel, 'inbox');
    expect(msg.content.toString()).toBe('hello');
    expect(msg.fields.exchange).toBe('');
    expect(msg.fields.routingKey).toBe('inbox');
    expect(msg.properties.contentType).toBe('application/json');
    expect(msg.properties.correlationId).toBe('corr-1');
    expect(msg.properties.deliveryMode).toBe(2);
    expect(msg.properties.messageId).toBe('msg-1');
    expect(msg.properties.headers).toMatchObject({ tenant: 'classdojo', n: 42 });
  });

  it('fragments and reassembles bodies larger than frameMax', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('big');
    const body = Buffer.alloc(1024 * 1024);
    for (let i = 0; i < body.length; i++) body[i] = i % 251;
    channel.sendToQueue('big', body);
    const msg = await nextMessage(channel, 'big');
    expect(msg.content.length).toBe(body.length);
    expect(msg.content.equals(body)).toBe(true);
  });

  it('supports basic.get and get-empty', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('pull');
    channel.sendToQueue('pull', Buffer.from('one'));
    await wait(20);
    const first = await channel.get('pull', { noAck: true });
    expect(first).not.toBe(false);
    expect((first as ConsumeMessage).content.toString()).toBe('one');
    const second = await channel.get('pull', { noAck: true });
    expect(second).toBe(false);
  });
});

describe('exchanges and routing', () => {
  it('routes via a direct exchange by exact key', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('dx', 'direct');
    await channel.assertQueue('dx-a');
    await channel.assertQueue('dx-b');
    await channel.bindQueue('dx-a', 'dx', 'alpha');
    await channel.bindQueue('dx-b', 'dx', 'beta');
    channel.publish('dx', 'alpha', Buffer.from('to-a'));
    await wait(30);
    expect(server.broker.queues.get('dx-a')!.messageCount).toBe(1);
    expect(server.broker.queues.get('dx-b')!.messageCount).toBe(0);
  });

  it('routes via a fanout exchange to all bound queues', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('fx', 'fanout');
    await channel.assertQueue('fx-a');
    await channel.assertQueue('fx-b');
    await channel.bindQueue('fx-a', 'fx', '');
    await channel.bindQueue('fx-b', 'fx', 'ignored');
    channel.publish('fx', 'anything', Buffer.from('all'));
    await wait(30);
    expect(server.broker.queues.get('fx-a')!.messageCount).toBe(1);
    expect(server.broker.queues.get('fx-b')!.messageCount).toBe(1);
  });

  it('routes via topic patterns with * and #', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('tx', 'topic');
    const cases: Array<[queue: string, pattern: string]> = [
      ['t-star', 'user.*.created'],
      ['t-hash', 'user.#'],
      ['t-all', '#'],
      ['t-exact', 'user.42.created'],
      ['t-tail', '#.created'],
      ['t-none', 'order.*'],
    ];
    for (const [queue, pattern] of cases) {
      await channel.assertQueue(queue);
      await channel.bindQueue(queue, 'tx', pattern);
    }
    channel.publish('tx', 'user.42.created', Buffer.from('m'));
    await wait(30);
    const counts = Object.fromEntries(
      cases.map(([q]) => [q, server.broker.queues.get(q)!.messageCount]),
    );
    expect(counts).toEqual({
      't-star': 1,
      't-hash': 1,
      't-all': 1,
      't-exact': 1,
      't-tail': 1,
      't-none': 0,
    });
    // '#' should also match zero words: 'user.#' matches bare 'user'
    channel.publish('tx', 'user', Buffer.from('m2'));
    await wait(30);
    expect(server.broker.queues.get('t-hash')!.messageCount).toBe(2);
    expect(server.broker.queues.get('t-star')!.messageCount).toBe(1);
  });

  it('routes each message to exactly one queue via x-consistent-hash', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('hx', 'x-consistent-hash');
    await channel.assertQueue('hx-a');
    await channel.assertQueue('hx-b');
    await channel.bindQueue('hx-a', 'hx', '1');
    await channel.bindQueue('hx-b', 'hx', '1');
    for (let i = 0; i < 50; i++) channel.publish('hx', `key-${i}`, Buffer.from('m'));
    await wait(50);
    const a = server.broker.queues.get('hx-a')!.messageCount;
    const b = server.broker.queues.get('hx-b')!.messageCount;
    expect(a + b).toBe(50); // exactly one queue per message, never both
    expect(a).toBeGreaterThan(0);
    expect(b).toBeGreaterThan(0);
    // Routing is deterministic: the same key always lands on the same queue
    channel.publish('hx', 'key-0', Buffer.from('again'));
    await wait(30);
    expect(
      server.broker.queues.get('hx-a')!.messageCount +
        server.broker.queues.get('hx-b')!.messageCount,
    ).toBe(51);
  });
});

describe('acknowledgement and QoS', () => {
  it('redelivers a nacked message with the redelivered flag set', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('retry');
    channel.sendToQueue('retry', Buffer.from('flaky'));

    const deliveries: ConsumeMessage[] = [];
    const done = new Promise<void>((resolve) => {
      channel.consume(
        'retry',
        (msg) => {
          if (!msg) return;
          deliveries.push(msg);
          if (deliveries.length === 1) channel.nack(msg, false, true);
          else {
            channel.ack(msg);
            resolve();
          }
        },
        { noAck: false },
      );
    });
    await done;
    expect(deliveries[0].fields.redelivered).toBe(false);
    expect(deliveries[1].fields.redelivered).toBe(true);
    expect(deliveries[1].content.toString()).toBe('flaky');
  });

  it('drops a rejected message when requeue is false', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('dropzone');
    channel.sendToQueue('dropzone', Buffer.from('bad'));
    await wait(20);
    const msg = (await channel.get('dropzone', { noAck: false })) as ConsumeMessage;
    channel.reject(msg, false);
    await wait(20);
    expect(server.broker.queues.get('dropzone')!.messageCount).toBe(0);
    expect(await channel.get('dropzone', { noAck: true })).toBe(false);
  });

  it('honours prefetch: a second message waits for the ack of the first', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('paced');
    await channel.prefetch(1);
    channel.sendToQueue('paced', Buffer.from('1'));
    channel.sendToQueue('paced', Buffer.from('2'));

    const received: ConsumeMessage[] = [];
    await channel.consume('paced', (msg) => msg && received.push(msg), { noAck: false });
    await wait(50);
    expect(received.map((m) => m.content.toString())).toEqual(['1']);

    channel.ack(received[0]);
    await wait(50);
    expect(received.map((m) => m.content.toString())).toEqual(['1', '2']);
  });
});

describe('confirms and returns', () => {
  it('acks publishes on a confirm channel', async () => {
    const channel = await (await connect()).createConfirmChannel();
    await channel.assertQueue('confirmed');
    await new Promise<void>((resolve, reject) => {
      channel.sendToQueue('confirmed', Buffer.from('sure'), {}, (err) =>
        err ? reject(err) : resolve(),
      );
    });
    await channel.waitForConfirms();
  });

  it('returns mandatory messages that route nowhere', async () => {
    const channel = await (await connect()).createChannel();
    const returned = new Promise<amqp.Message>((resolve) => channel.on('return', resolve));
    channel.publish('', 'no-such-queue', Buffer.from('lost'), { mandatory: true });
    const msg = await returned;
    expect(msg.fields.replyCode).toBe(312); // NO_ROUTE
    expect(msg.fields.routingKey).toBe('no-such-queue');
    expect(msg.content.toString()).toBe('lost');
  });
});

describe('dead-lettering and TTL', () => {
  it('dead-letters a rejected message with x-death headers', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('dl-x', 'direct');
    await channel.assertQueue('dl-q');
    await channel.bindQueue('dl-q', 'dl-x', 'work');
    await channel.assertQueue('work', { deadLetterExchange: 'dl-x' });

    channel.sendToQueue('work', Buffer.from('poison'), { headers: { attempt: 1 } });
    await wait(20);
    const msg = (await channel.get('work', { noAck: false })) as ConsumeMessage;
    channel.reject(msg, false);
    await wait(30);

    expect(server.broker.queues.get('work')!.messageCount).toBe(0);
    const dead = (await channel.get('dl-q', { noAck: true })) as ConsumeMessage;
    expect(dead).not.toBe(false);
    expect(dead.content.toString()).toBe('poison');
    expect(dead.fields.exchange).toBe('dl-x');
    expect(dead.fields.routingKey).toBe('work'); // original key preserved
    const headers = dead.properties.headers!;
    expect(headers['x-first-death-reason']).toBe('rejected');
    expect(headers['x-first-death-queue']).toBe('work');
    expect(headers['x-death']).toMatchObject([
      { count: 1, reason: 'rejected', queue: 'work', exchange: '', 'routing-keys': ['work'] },
    ]);
    expect(headers.attempt).toBe(1); // original headers kept
  });

  it('expires messages into the DLX with no consumer attached', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('ttl-x', 'topic');
    await channel.assertQueue('expired-q');
    await channel.bindQueue('expired-q', 'ttl-x', 'jobs.#');
    await channel.assertQueue('ttl-q', {
      messageTtl: 80,
      deadLetterExchange: 'ttl-x',
      deadLetterRoutingKey: 'jobs.timed-out',
    });

    channel.sendToQueue('ttl-q', Buffer.from('slow'));
    await wait(30);
    expect(server.broker.queues.get('ttl-q')!.messageCount).toBe(1); // not yet
    await wait(150);
    expect(server.broker.queues.get('ttl-q')!.messageCount).toBe(0);
    const dead = (await channel.get('expired-q', { noAck: true })) as ConsumeMessage;
    expect(dead.fields.routingKey).toBe('jobs.timed-out'); // dl routing key override
    expect(dead.properties.headers!['x-death']).toMatchObject([
      { count: 1, reason: 'expired', queue: 'ttl-q' },
    ]);
  });

  it('drops expired messages when no DLX is configured', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('vanish', { messageTtl: 50 });
    channel.sendToQueue('vanish', Buffer.from('gone'));
    await wait(150);
    expect(server.broker.queues.get('vanish')!.messageCount).toBe(0);
    expect(await channel.get('vanish', { noAck: true })).toBe(false);
  });

  it('honours per-message expiration and strips it on dead-lettering', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('pm-x', 'fanout');
    await channel.assertQueue('pm-dlq');
    await channel.bindQueue('pm-dlq', 'pm-x', '');
    await channel.assertQueue('pm-q', { deadLetterExchange: 'pm-x' }); // no queue TTL

    channel.sendToQueue('pm-q', Buffer.from('brief'), { expiration: '60' });
    await wait(200);
    expect(server.broker.queues.get('pm-q')!.messageCount).toBe(0);
    const dead = (await channel.get('pm-dlq', { noAck: true })) as ConsumeMessage;
    expect(dead.properties.expiration).toBeUndefined(); // must not re-expire in the DLQ
    expect(dead.properties.headers!['x-death']).toMatchObject([
      { reason: 'expired', queue: 'pm-q', 'original-expiration': '60' },
    ]);
  });
});

describe('queue limits and lifetimes', () => {
  it('deletes an idle x-expires queue but keeps a consumed one', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('lease-idle', { expires: 100 });
    await channel.assertQueue('lease-used', { expires: 100 });
    await channel.consume('lease-used', () => {}, { noAck: true });
    await wait(300);
    expect(server.broker.queues.has('lease-idle')).toBe(false);
    expect(server.broker.queues.has('lease-used')).toBe(true);
  });

  it('drops the head to the DLX when x-max-length is exceeded', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('cap-x', 'fanout');
    await channel.assertQueue('cap-dlq');
    await channel.bindQueue('cap-dlq', 'cap-x', '');
    await channel.assertQueue('cap', { maxLength: 2, deadLetterExchange: 'cap-x' });
    for (const body of ['a', 'b', 'c']) channel.sendToQueue('cap', Buffer.from(body));
    expect((await channel.checkQueue('cap')).messageCount).toBe(2);
    const dropped = (await channel.get('cap-dlq', { noAck: true })) as ConsumeMessage;
    expect(dropped.content.toString()).toBe('a'); // oldest goes first
    expect(dropped.properties.headers!['x-death']).toMatchObject([
      { reason: 'maxlen', queue: 'cap' },
    ]);
    const next = (await channel.get('cap', { noAck: true })) as ConsumeMessage;
    expect(next.content.toString()).toBe('b');
  });

  it('nacks the publisher when x-overflow is reject-publish', async () => {
    const channel = await (await connect()).createConfirmChannel();
    await channel.assertQueue('full', { maxLength: 1, overflow: 'reject-publish' } as never);
    const confirm = (body: string) =>
      new Promise<Error | null>((resolve) =>
        channel.sendToQueue('full', Buffer.from(body), {}, (err) => resolve(err)),
      );
    expect(await confirm('fits')).toBeNull();
    expect(await confirm('overflows')).toBeTruthy(); // server basic.nack
    expect(server.broker.queues.get('full')!.messageCount).toBe(1);
  });

  it('drops messages caught in a fully-automatic dead-letter cycle', async () => {
    const channel = await (await connect()).createChannel();
    // TTL queue whose DLX routes straight back to itself: without cycle
    // detection this would spin forever
    await channel.assertQueue('spin', {
      messageTtl: 60,
      deadLetterExchange: '',
      deadLetterRoutingKey: 'spin',
    });
    channel.sendToQueue('spin', Buffer.from('dizzy'));
    await wait(250);
    expect(server.broker.queues.get('spin')!.messageCount).toBe(0);
  });

  it('allows retry cycles that include a rejection (retry-queue pattern)', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertExchange('retry-x', 'fanout');
    await channel.assertQueue('retry-q', {
      messageTtl: 60,
      deadLetterExchange: '',
      deadLetterRoutingKey: 'work2',
    });
    await channel.bindQueue('retry-q', 'retry-x', '');
    await channel.assertQueue('work2', { deadLetterExchange: 'retry-x' });

    channel.sendToQueue('work2', Buffer.from('flaky-job'));
    await wait(20);
    const attempt = (await channel.get('work2', { noAck: false })) as ConsumeMessage;
    channel.reject(attempt, false); // → retry-q, expires there → back to work2
    await wait(250);
    const retried = (await channel.get('work2', { noAck: true })) as ConsumeMessage;
    expect(retried).not.toBe(false);
    expect(retried.content.toString()).toBe('flaky-job');
    const deaths = retried.properties.headers!['x-death'] as Array<Record<string, unknown>>;
    expect(deaths).toHaveLength(2);
    expect(deaths[0]).toMatchObject({ reason: 'expired', queue: 'retry-q' });
    expect(deaths[1]).toMatchObject({ reason: 'rejected', queue: 'work2' });
  });
});

describe('error semantics', () => {
  it('fails a passive declare of a missing queue with 404 and keeps the connection alive', async () => {
    const connection = await connect();
    const doomed = await connection.createChannel();
    doomed.on('error', () => {}); // amqplib also emits the server close as an event
    await expect(doomed.checkQueue('does-not-exist')).rejects.toThrow(/404/);
    // The channel died but the connection must survive
    const fresh = await connection.createChannel();
    await fresh.assertQueue('still-works');
    expect(server.broker.queues.has('still-works')).toBe(true);
  });

  it('fails an inequivalent redeclare with 406', async () => {
    const connection = await connect();
    const first = await connection.createChannel();
    await first.assertQueue('props', { durable: false });
    const second = await connection.createChannel();
    second.on('error', () => {}); // amqplib also emits the server close as an event
    await expect(second.assertQueue('props', { durable: true })).rejects.toThrow(/406/);
  });

  it('fails a redeclare with different arguments with 406', async () => {
    const connection = await connect();
    const first = await connection.createChannel();
    await first.assertQueue('argq', { messageTtl: 1000 });
    await first.assertQueue('argq', { messageTtl: 1000 }); // identical is fine
    const second = await connection.createChannel();
    second.on('error', () => {});
    await expect(second.assertQueue('argq', { messageTtl: 2000 })).rejects.toThrow(/406/);
  });

  it('deletes exclusive queues when their connection closes', async () => {
    const owner = await connect();
    const channel = await owner.createChannel();
    await channel.assertQueue('mine', { exclusive: true });
    expect(server.broker.queues.has('mine')).toBe(true);
    await owner.close();
    openConnections.splice(openConnections.indexOf(owner), 1);
    await wait(50);
    expect(server.broker.queues.has('mine')).toBe(false);
  });

  it('sends basic.cancel to consumers when their queue is deleted', async () => {
    const connection = await connect();
    const consumerChannel = await connection.createChannel();
    await consumerChannel.assertQueue('doomed');
    // amqplib surfaces a server-sent basic.cancel as a null message
    const cancelled = new Promise<boolean>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no cancel notification')), 2000);
      consumerChannel.consume('doomed', (msg) => {
        if (msg === null) {
          clearTimeout(timer);
          resolve(true);
        }
      });
    });
    const admin = await connection.createChannel();
    await admin.deleteQueue('doomed');
    expect(await cancelled).toBe(true);
  });

  it('rejects exchange-to-exchange bindings with 540 but keeps the connection alive', async () => {
    const connection = await connect();
    const doomed = await connection.createChannel();
    doomed.on('error', () => {});
    await expect(doomed.bindExchange('dest-x', 'src-x', 'key')).rejects.toThrow(/540/);
    const fresh = await connection.createChannel();
    await fresh.assertQueue('survives-540');
    expect(server.broker.queues.has('survives-540')).toBe(true);
  });

  it('reports queue counts on purge', async () => {
    const channel = await (await connect()).createChannel();
    await channel.assertQueue('heap');
    for (let i = 0; i < 3; i++) channel.sendToQueue('heap', Buffer.from(`${i}`));
    await wait(30);
    const { messageCount } = await channel.purgeQueue('heap');
    expect(messageCount).toBe(3);
  });
});
