import { ChannelError, ConnectionError } from '../errors';
import { constants, type MethodFields } from '../wire';
import { Exchange, type ExchangeType } from './exchange';
import { type DeadLetterReason, type Message, Queue } from './queue';

const AMQP_RESERVED_PREFIX = 'amq.';

/**
 * In-memory broker state shared by all connections: the exchange and queue
 * tables plus declare/bind/publish semantics. Frame-level concerns live in
 * connection.ts/channel.ts.
 */
export class Broker {
  readonly exchanges = new Map<string, Exchange>();
  readonly queues = new Map<string, Queue>();
  private generatedQueueCounter = 0;

  constructor() {
    const defaults: Array<[string, ExchangeType]> = [
      ['', 'direct'],
      ['amq.direct', 'direct'],
      ['amq.fanout', 'fanout'],
      ['amq.topic', 'topic'],
    ];
    for (const [name, type] of defaults) {
      this.exchanges.set(
        name,
        new Exchange(name, {
          type,
          durable: true,
          autoDelete: false,
          internal: false,
          arguments: {},
        }),
      );
    }
  }

  assertExchange(
    name: string,
    type: string,
    opts: {
      passive: boolean;
      durable: boolean;
      autoDelete: boolean;
      internal: boolean;
      arguments: MethodFields;
    },
  ): Exchange {
    const existing = this.exchanges.get(name);
    if (opts.passive) {
      if (!existing) {
        throw new ChannelError(constants.NOT_FOUND, `no exchange '${name}' in vhost '/'`);
      }
      return existing;
    }
    if (name === '' || (!existing && name.startsWith(AMQP_RESERVED_PREFIX))) {
      throw new ChannelError(
        constants.ACCESS_REFUSED,
        `exchange name '${name}' contains reserved prefix '${AMQP_RESERVED_PREFIX}'`,
      );
    }
    if (
      type !== 'direct' &&
      type !== 'fanout' &&
      type !== 'topic' &&
      type !== 'x-consistent-hash'
    ) {
      // Mirrors RabbitMQ: an unknown exchange type is a connection error
      throw new ConnectionError(constants.COMMAND_INVALID, `unknown exchange type '${type}'`);
    }
    if (existing) {
      const p = existing.properties;
      if (
        p.type !== type ||
        p.internal !== opts.internal ||
        p.durable !== opts.durable ||
        p.autoDelete !== opts.autoDelete ||
        !deepEqual(p.arguments ?? {}, opts.arguments ?? {})
      ) {
        throw new ChannelError(
          constants.PRECONDITION_FAILED,
          `inequivalent arg for exchange '${name}': received declaration differs from existing`,
        );
      }
      return existing;
    }
    const exchange = new Exchange(name, {
      type,
      durable: opts.durable,
      autoDelete: opts.autoDelete,
      internal: opts.internal,
      arguments: opts.arguments,
    });
    this.exchanges.set(name, exchange);
    return exchange;
  }

  deleteExchange(name: string, ifUnused: boolean): void {
    const exchange = this.exchanges.get(name);
    // Deleting a missing exchange succeeds (RabbitMQ behaviour)
    if (!exchange) return;
    if (name === '' || name.startsWith(AMQP_RESERVED_PREFIX)) {
      throw new ChannelError(constants.ACCESS_REFUSED, `cannot delete exchange '${name}'`);
    }
    if (ifUnused && exchange.bindings.length > 0) {
      throw new ChannelError(constants.PRECONDITION_FAILED, `exchange '${name}' in use`);
    }
    this.exchanges.delete(name);
  }

  assertQueue(
    name: string,
    opts: {
      passive: boolean;
      durable: boolean;
      exclusive: boolean;
      autoDelete: boolean;
      arguments: MethodFields;
    },
    connectionId: string,
  ): Queue {
    if (name !== '') {
      const existing = this.queues.get(name);
      if (existing) {
        this.checkQueueAccess(existing, connectionId);
        if (!opts.passive) {
          const p = existing.properties;
          if (
            p.durable !== opts.durable ||
            p.exclusive !== opts.exclusive ||
            p.autoDelete !== opts.autoDelete ||
            !deepEqual(p.arguments ?? {}, opts.arguments ?? {})
          ) {
            throw new ChannelError(
              constants.PRECONDITION_FAILED,
              `inequivalent arg for queue '${name}': received declaration differs from existing`,
            );
          }
        }
        existing.touchUsage(); // a redeclare counts as use for x-expires
        return existing;
      }
      if (opts.passive) {
        throw new ChannelError(constants.NOT_FOUND, `no queue '${name}' in vhost '/'`);
      }
      if (name.startsWith(AMQP_RESERVED_PREFIX)) {
        throw new ChannelError(
          constants.ACCESS_REFUSED,
          `queue name '${name}' contains reserved prefix '${AMQP_RESERVED_PREFIX}'`,
        );
      }
    }
    if (name === '' && opts.passive) {
      throw new ChannelError(constants.NOT_FOUND, `no queue '' in vhost '/'`);
    }
    const queueName = name === '' ? `amq.gen-${++this.generatedQueueCounter}` : name;
    const queue = new Queue(
      queueName,
      {
        durable: opts.durable,
        exclusive: opts.exclusive,
        autoDelete: opts.autoDelete,
        arguments: opts.arguments,
      },
      opts.exclusive ? connectionId : null,
      {
        deadLetter: (source, message, reason) => this.deadLetter(source, message, reason),
        expireQueue: (idle) => {
          if (this.queues.get(idle.name) === idle) this.removeQueue(idle);
        },
      },
    );
    this.queues.set(queueName, queue);
    return queue;
  }

  /** Look up a queue for consume/get/bind/purge/delete; 404 if missing. */
  getQueue(name: string, connectionId: string): Queue {
    const queue = this.queues.get(name);
    if (!queue) {
      throw new ChannelError(constants.NOT_FOUND, `no queue '${name}' in vhost '/'`);
    }
    this.checkQueueAccess(queue, connectionId);
    return queue;
  }

  private checkQueueAccess(queue: Queue, connectionId: string): void {
    if (queue.exclusiveOwner !== null && queue.exclusiveOwner !== connectionId) {
      throw new ChannelError(
        constants.RESOURCE_LOCKED,
        `cannot obtain exclusive access to locked queue '${queue.name}'`,
      );
    }
  }

  bindQueue(
    queueName: string,
    exchangeName: string,
    bindingKey: string,
    args: MethodFields,
    connectionId: string,
  ): void {
    const queue = this.getQueue(queueName, connectionId);
    const exchange = this.exchanges.get(exchangeName);
    if (!exchange) {
      throw new ChannelError(constants.NOT_FOUND, `no exchange '${exchangeName}' in vhost '/'`);
    }
    if (exchangeName === '') {
      throw new ChannelError(constants.ACCESS_REFUSED, 'cannot bind to the default exchange');
    }
    exchange.bind(queue.name, bindingKey, args);
  }

  unbindQueue(
    queueName: string,
    exchangeName: string,
    bindingKey: string,
    connectionId: string,
  ): void {
    const queue = this.getQueue(queueName, connectionId);
    const exchange = this.exchanges.get(exchangeName);
    // Unbinding something that isn't bound is a no-op success
    exchange?.unbind(queue.name, bindingKey);
  }

  purgeQueue(name: string, connectionId: string): number {
    return this.getQueue(name, connectionId).purge();
  }

  deleteQueue(
    name: string,
    opts: { ifUnused: boolean; ifEmpty: boolean },
    connectionId: string,
  ): number {
    const queue = this.queues.get(name);
    // Deleting a missing queue succeeds with zero messages (RabbitMQ behaviour)
    if (!queue) return 0;
    this.checkQueueAccess(queue, connectionId);
    if (opts.ifUnused && queue.consumerCount > 0) {
      throw new ChannelError(constants.PRECONDITION_FAILED, `queue '${name}' in use`);
    }
    if (opts.ifEmpty && queue.messageCount > 0) {
      throw new ChannelError(constants.PRECONDITION_FAILED, `queue '${name}' not empty`);
    }
    return this.removeQueue(queue);
  }

  /** Remove a queue and all its bindings; returns the dropped message count. */
  removeQueue(queue: Queue): number {
    queue.dispose();
    queue.cancelAllConsumers();
    for (const exchange of this.exchanges.values()) {
      exchange.removeBindingsFor(queue.name);
    }
    this.queues.delete(queue.name);
    return queue.messageCount;
  }

  autoDeleteIfUnused(queue: Queue): void {
    if (queue.shouldAutoDelete && this.queues.get(queue.name) === queue) {
      this.removeQueue(queue);
    }
  }

  /**
   * Route a message. `routed` lists the matched queues (for mandatory
   * handling); `accepted` is false when any of them refused it
   * (x-overflow: reject-publish), which a confirm channel reports as a nack.
   * The default exchange routes directly to the queue named by the routing key.
   */
  publish(
    exchangeName: string,
    routingKey: string,
    message: Message,
  ): { routed: Queue[]; accepted: boolean } {
    const exchange = this.exchanges.get(exchangeName);
    if (!exchange) {
      throw new ChannelError(constants.NOT_FOUND, `no exchange '${exchangeName}' in vhost '/'`);
    }
    if (exchange.properties.internal) {
      throw new ChannelError(
        constants.ACCESS_REFUSED,
        `cannot publish to internal exchange '${exchangeName}'`,
      );
    }
    const routed = this.resolveTargets(exchange, routingKey);
    let accepted = true;
    for (const queue of routed) {
      // Each queue gets its own message instance so redelivered flags and
      // requeues on one queue don't leak into another
      if (!queue.publish({ ...message })) accepted = false;
    }
    return { routed, accepted };
  }

  private resolveTargets(exchange: Exchange, routingKey: string): Queue[] {
    const targets: Queue[] = [];
    if (exchange.name === '') {
      const queue = this.queues.get(routingKey);
      if (queue) targets.push(queue);
    } else {
      for (const queueName of exchange.route(routingKey)) {
        const queue = this.queues.get(queueName);
        if (queue) targets.push(queue);
      }
    }
    return targets;
  }

  /**
   * Dead-letter a message leaving `source` (rejected with requeue=false, or
   * expired). Without a usable `x-dead-letter-exchange` the message is
   * dropped. Mirrors RabbitMQ: republished to the DLX with `x-death` /
   * `x-first-death-*` headers, and the `expiration` property stripped so the
   * message cannot re-expire in the dead-letter queue.
   */
  deadLetter(source: Queue, message: Message, reason: DeadLetterReason): void {
    const exchangeName = source.deadLetterExchange;
    if (exchangeName === undefined) return;
    const exchange = this.exchanges.get(exchangeName);
    if (!exchange) return;
    const routingKey = source.deadLetterRoutingKey ?? message.routingKey;
    const dead = buildDeadLetteredMessage(message, source.name, reason, exchangeName, routingKey);
    // Cycle detection, as in RabbitMQ: a message returning to a queue it has
    // already died from is dropped if the whole chain was automatic (no
    // explicit rejection anywhere in its history)
    const deaths = (dead.properties.headers as MethodFields)['x-death'] as Array<{
      queue: string;
      reason: string;
    }>;
    const automaticOnly = deaths.every((d) => d.reason !== 'rejected');
    for (const queue of this.resolveTargets(exchange, routingKey)) {
      // The target queue appearing in the history means a cycle; a rejection
      // anywhere in the chain (client intervention) permits it to continue —
      // that's the retry-queue pattern — otherwise the message is dropped
      if (automaticOnly && deaths.some((d) => d.queue === queue.name)) continue;
      queue.publish({ ...dead });
    }
  }

  /** Drop state owned by a closing connection (exclusive queues). */
  cleanupConnection(connectionId: string): void {
    for (const queue of [...this.queues.values()]) {
      if (queue.exclusiveOwner === connectionId) {
        this.removeQueue(queue);
      }
    }
  }
}

interface DeathEntry extends Record<string, unknown> {
  count: number;
  reason: DeadLetterReason;
  queue: string;
  exchange: string;
}

/** Structural equality for AMQP field tables (declare-equivalence checks). */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (Buffer.isBuffer(a) && Buffer.isBuffer(b)) return a.equals(b);
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((value, i) => deepEqual(value, b[i]));
  }
  if (a !== null && b !== null && typeof a === 'object' && typeof b === 'object') {
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);
    if (keysA.length !== keysB.length) return false;
    return keysA.every((key) =>
      deepEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]),
    );
  }
  return false;
}

/** RabbitMQ encodes x-death times as AMQP timestamps (seconds). */
function amqpTimestampNow(): MethodFields {
  return { '!': 'timestamp', value: Math.floor(Date.now() / 1000) };
}

function buildDeadLetteredMessage(
  message: Message,
  queueName: string,
  reason: DeadLetterReason,
  exchangeName: string,
  routingKey: string,
): Message {
  const properties: MethodFields = { ...message.properties };
  const headers: MethodFields = { ...(properties.headers as MethodFields | undefined) };

  const entry: DeathEntry = {
    count: 1,
    reason,
    queue: queueName,
    time: amqpTimestampNow(),
    exchange: message.exchange,
    'routing-keys': [message.routingKey],
  };
  if (typeof properties.expiration === 'string') {
    entry['original-expiration'] = properties.expiration;
    delete properties.expiration;
  }
  const deaths = (Array.isArray(headers['x-death']) ? [...headers['x-death']] : []) as DeathEntry[];
  const seen = deaths.findIndex((d) => d.queue === queueName && d.reason === reason);
  if (seen !== -1) {
    entry.count = (deaths[seen].count ?? 0) + 1;
    deaths.splice(seen, 1);
  }
  deaths.unshift(entry);

  headers['x-death'] = deaths;
  headers['x-first-death-reason'] ??= reason;
  headers['x-first-death-queue'] ??= queueName;
  headers['x-first-death-exchange'] ??= message.exchange;
  properties.headers = headers;

  return {
    exchange: exchangeName,
    routingKey,
    properties,
    body: message.body,
    redelivered: false,
    // expiresAt intentionally not carried over: only the dead-letter queue's
    // own x-message-ttl may expire it from here
  };
}
