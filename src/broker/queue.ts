import type { MethodFields } from '../wire';

/** A message held by a queue or in flight to a consumer. */
export interface Message {
  exchange: string;
  routingKey: string;
  properties: MethodFields;
  body: Buffer;
  redelivered: boolean;
  /** Absolute epoch-ms deadline, set when enqueued into a TTL'd queue. */
  expiresAt?: number;
}

export type DeadLetterReason = 'rejected' | 'expired' | 'maxlen';

/** Provided by the Broker: dead-letter (or drop) a message leaving `queue`. */
export type DeadLetterFn = (queue: Queue, message: Message, reason: DeadLetterReason) => void;

export interface QueueHooks {
  deadLetter: DeadLetterFn;
  /** Queue idle past its x-expires — remove it from the broker. */
  expireQueue(queue: Queue): void;
}

/**
 * A consumer registered on a queue — implemented by Channel. `deliver`
 * returns false when the consumer cannot accept right now (prefetch window
 * full), in which case the queue keeps the message and retries later.
 */
export interface Consumer {
  readonly consumerTag: string;
  readonly noAck: boolean;
  readonly exclusive: boolean;
  deliver(message: Message, queue: Queue): boolean;
  /** The queue is going away — notify the client (server-sent basic.cancel). */
  cancel(): void;
}

export interface QueueProperties {
  durable: boolean;
  exclusive: boolean;
  autoDelete: boolean;
  arguments: MethodFields;
}

export class Queue {
  readonly messages: Message[] = [];
  private readonly consumers: Consumer[] = [];
  private rrIndex = 0;
  /** True once a consumer has ever been registered — drives auto-delete. */
  private hadConsumers = false;

  private idleTimer: NodeJS.Timeout | undefined;

  constructor(
    readonly name: string,
    readonly properties: QueueProperties,
    /** Owning connection id for exclusive queues, null otherwise. */
    readonly exclusiveOwner: string | null,
    private readonly hooks: QueueHooks,
  ) {
    this.scheduleIdleExpiry();
  }

  private deadLetter(queue: Queue, message: Message, reason: DeadLetterReason): void {
    this.hooks.deadLetter(queue, message, reason);
  }

  /** Queue-level TTL in ms (`x-message-ttl` argument), if configured. */
  get messageTtl(): number | undefined {
    const value = this.properties.arguments['x-message-ttl'];
    return typeof value === 'number' && value >= 0 ? value : undefined;
  }

  get deadLetterExchange(): string | undefined {
    const value = this.properties.arguments['x-dead-letter-exchange'];
    return typeof value === 'string' ? value : undefined;
  }

  get deadLetterRoutingKey(): string | undefined {
    const value = this.properties.arguments['x-dead-letter-routing-key'];
    return typeof value === 'string' ? value : undefined;
  }

  /** Queue idle-lifetime in ms (`x-expires`), if configured. */
  get expiresMs(): number | undefined {
    const value = this.properties.arguments['x-expires'];
    return typeof value === 'number' && value > 0 ? value : undefined;
  }

  get maxLength(): number | undefined {
    const value = this.properties.arguments['x-max-length'];
    return typeof value === 'number' && value >= 0 ? value : undefined;
  }

  get maxLengthBytes(): number | undefined {
    const value = this.properties.arguments['x-max-length-bytes'];
    return typeof value === 'number' && value >= 0 ? value : undefined;
  }

  get overflowBehaviour(): 'drop-head' | 'reject-publish' {
    return this.properties.arguments['x-overflow'] === 'reject-publish'
      ? 'reject-publish'
      : 'drop-head';
  }

  get messageCount(): number {
    return this.messages.length;
  }

  get consumerCount(): number {
    return this.consumers.length;
  }

  get hasExclusiveConsumer(): boolean {
    return this.consumers.some((c) => c.exclusive);
  }

  /** @returns false if the message was refused (`x-overflow: reject-publish`). */
  publish(message: Message): boolean {
    if (this.overflowBehaviour === 'reject-publish' && this.wouldOverflow(message)) {
      return false;
    }
    this.applyTtl(message);
    this.messages.push(message);
    // Let an available consumer take it before trimming (limits apply to
    // ready messages, so a deliverable message shouldn't count against them)
    this.drain();
    this.enforceMaxLength();
    return true;
  }

  /** Requeue messages at the head, preserving their original order. */
  requeue(messages: Message[]): void {
    const now = Date.now();
    const live: Message[] = [];
    for (const m of messages) {
      m.redelivered = true;
      // Expiry keeps counting from the original enqueue while unacked
      if (m.expiresAt !== undefined && m.expiresAt <= now) {
        this.deadLetter(this, m, 'expired');
      } else {
        live.push(m);
      }
    }
    this.messages.unshift(...live);
    for (const m of live) {
      if (m.expiresAt !== undefined) this.scheduleExpiry(m, m.expiresAt - now);
    }
    this.drain();
  }

  /** Pop the head message (basic.get). */
  take(): Message | undefined {
    this.touchUsage();
    this.expireHead();
    return this.messages.shift();
  }

  private get currentBytes(): number {
    return this.messages.reduce((total, m) => total + m.body.length, 0);
  }

  private wouldOverflow(incoming: Message): boolean {
    const { maxLength, maxLengthBytes } = this;
    if (maxLength !== undefined && this.messages.length + 1 > maxLength) return true;
    if (maxLengthBytes !== undefined && this.currentBytes + incoming.body.length > maxLengthBytes) {
      return true;
    }
    return false;
  }

  /** drop-head overflow: shed oldest ready messages to the DLX (reason maxlen). */
  private enforceMaxLength(): void {
    const { maxLength, maxLengthBytes } = this;
    if (maxLength === undefined && maxLengthBytes === undefined) return;
    while (
      (maxLength !== undefined && this.messages.length > maxLength) ||
      (maxLengthBytes !== undefined && this.currentBytes > maxLengthBytes)
    ) {
      const head = this.messages.shift();
      if (!head) return;
      this.deadLetter(this, head, 'maxlen');
    }
  }

  /**
   * x-expires: (re)start the idle countdown. "Used" means having consumers,
   * being redeclared, or serving a basic.get — publishes don't count, per
   * RabbitMQ. A no-op unless x-expires is set and the queue has no consumers.
   */
  touchUsage(): void {
    this.scheduleIdleExpiry();
  }

  private scheduleIdleExpiry(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = undefined;
    const idleMs = this.expiresMs;
    if (idleMs === undefined || this.consumers.length > 0) return;
    this.idleTimer = setTimeout(() => {
      if (this.consumers.length === 0) this.hooks.expireQueue(this);
    }, idleMs);
    this.idleTimer.unref?.();
  }

  /** Cancel timers when the broker removes this queue. */
  dispose(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = undefined;
  }

  /** Stamp the effective TTL: min of queue TTL and the `expiration` property. */
  private applyTtl(message: Message): void {
    const queueTtl = this.messageTtl;
    const raw = message.properties.expiration;
    const perMessage =
      typeof raw === 'string' && /^\d+$/.test(raw) ? parseInt(raw, 10) : undefined;
    const ttl =
      queueTtl !== undefined && perMessage !== undefined
        ? Math.min(queueTtl, perMessage)
        : (queueTtl ?? perMessage);
    if (ttl === undefined) return;
    message.expiresAt = Date.now() + ttl;
    this.scheduleExpiry(message, ttl);
  }

  /**
   * Proactive expiry so dead-lettering happens with no consumer attached.
   * The timer no-ops if the message has left the queue by then (delivered,
   * purged, acked...); duplicate timers after a requeue are harmless for the
   * same reason.
   */
  private scheduleExpiry(message: Message, delayMs: number): void {
    const timer = setTimeout(() => {
      const i = this.messages.indexOf(message);
      if (i !== -1) {
        this.messages.splice(i, 1);
        this.deadLetter(this, message, 'expired');
      }
    }, Math.max(0, delayMs));
    timer.unref?.();
  }

  /** Lazily expire due messages at the head (delivery/get must never see them). */
  private expireHead(): void {
    const now = Date.now();
    while (this.messages.length > 0 && (this.messages[0].expiresAt ?? Infinity) <= now) {
      this.deadLetter(this, this.messages.shift()!, 'expired');
    }
  }

  purge(): number {
    const n = this.messages.length;
    this.messages.length = 0;
    return n;
  }

  addConsumer(consumer: Consumer): void {
    this.consumers.push(consumer);
    this.hadConsumers = true;
    this.scheduleIdleExpiry(); // having consumers pauses x-expires
    this.drain();
  }

  /** @returns true if the consumer was registered here. */
  removeConsumer(consumerTag: string): boolean {
    const i = this.consumers.findIndex((c) => c.consumerTag === consumerTag);
    if (i === -1) return false;
    this.consumers.splice(i, 1);
    if (this.rrIndex > i) this.rrIndex--;
    this.scheduleIdleExpiry(); // idle countdown starts when the last one leaves
    return true;
  }

  /** Detach every consumer, letting each notify its client. */
  cancelAllConsumers(): void {
    for (const consumer of this.consumers.splice(0)) consumer.cancel();
    this.rrIndex = 0;
  }

  /** Auto-delete queues go away when their last consumer is cancelled. */
  get shouldAutoDelete(): boolean {
    return this.properties.autoDelete && this.hadConsumers && this.consumers.length === 0;
  }

  /**
   * Deliver buffered messages round-robin to consumers that can accept.
   * Called whenever a message arrives, a consumer joins, or prefetch
   * capacity frees up (ack/nack).
   */
  drain(): void {
    for (;;) {
      this.expireHead();
      if (this.messages.length === 0 || this.consumers.length === 0) return;
      const message = this.messages[0];
      let delivered = false;
      for (let n = 0; n < this.consumers.length; n++) {
        const consumer = this.consumers[(this.rrIndex + n) % this.consumers.length];
        if (consumer.deliver(message, this)) {
          this.rrIndex = (this.rrIndex + n + 1) % this.consumers.length;
          this.messages.shift();
          delivered = true;
          break;
        }
      }
      if (!delivered) return; // everyone's window is full
    }
  }
}
