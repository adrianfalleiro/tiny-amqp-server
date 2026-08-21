import type { Broker } from './broker/broker';
import type { Message, Queue } from './broker/queue';
import { ChannelError, ConnectionError } from './errors';
import {
  BasicProperties,
  type BodyFrame,
  classIdOf,
  constants,
  type DecodedFrame,
  encodeMethod,
  encodeProperties,
  FRAME_OVERHEAD,
  type HeaderFrame,
  makeBodyFrame,
  type MethodFields,
  methodIdOf,
  methodName,
  methods,
} from './wire';

/** What a Channel needs from its owning connection. */
export interface ChannelHost {
  readonly connectionId: string;
  readonly broker: Broker;
  readonly frameMax: number;
  /** True while the socket's write buffer is full — hold new deliveries. */
  readonly sendBlocked: boolean;
  send(buf: Buffer): void;
  /** Remove the channel from the connection's table once fully closed. */
  channelClosed(channelNumber: number): void;
}

interface PendingPublish {
  fields: MethodFields;
  confirmSeq: number;
  properties: MethodFields | null;
  totalSize: number;
  received: number;
  chunks: Buffer[];
}

interface UnackedEntry {
  message: Message;
  queue: Queue;
  /** Consumer deliveries count toward the prefetch window; basic.get does not. */
  fromConsumer: boolean;
}

type ChannelState = 'open' | 'closing-by-server' | 'closed';

export class Channel {
  private state: ChannelState = 'open';
  private prefetchCount = 0;
  private confirmMode = false;
  private publishSeq = 0;
  private deliveryTagCounter = 0;
  /** Insertion order == delivery-tag order; relied on for `multiple` acks and requeue order. */
  private readonly unacked = new Map<number, UnackedEntry>();
  private readonly consumerQueues = new Map<string, Queue>();
  private consumerTagCounter = 0;
  private pendingPublish: PendingPublish | null = null;
  private lastDeclaredQueue: string | null = null;

  constructor(
    readonly channelNumber: number,
    private readonly host: ChannelHost,
  ) {
    this.send(methods.ChannelOpenOk, { channelId: Buffer.alloc(0) });
  }

  private get broker(): Broker {
    return this.host.broker;
  }

  private send(methodId: number, fields: MethodFields): void {
    this.host.send(encodeMethod(methodId, this.channelNumber, fields));
  }

  private sendContent(properties: MethodFields, body: Buffer): void {
    this.host.send(encodeProperties(BasicProperties, this.channelNumber, body.length, properties));
    const maxBody = this.host.frameMax - FRAME_OVERHEAD;
    for (let offset = 0; offset < body.length; offset += maxBody) {
      this.host.send(
        makeBodyFrame(this.channelNumber, body.subarray(offset, Math.min(offset + maxBody, body.length))),
      );
    }
  }

  /** Entry point for every frame the connection routes to this channel. */
  accept(frame: DecodedFrame): void {
    if (this.state === 'closed') return;
    if (this.state === 'closing-by-server') {
      // Per spec the peer must discard frames until it confirms our close
      if ('id' in frame && 'fields' in frame && frame.id === methods.ChannelCloseOk) {
        this.state = 'closed';
        this.host.channelClosed(this.channelNumber);
      }
      return;
    }

    if ('content' in frame) {
      this.handleBody(frame);
      return;
    }
    if ('size' in frame) {
      this.handleHeader(frame);
      return;
    }
    if ('fields' in frame) {
      try {
        this.dispatch(frame.id, frame.fields);
      } catch (err) {
        if (err instanceof ChannelError) {
          this.failChannel(err, frame.id);
          return;
        }
        throw err;
      }
    }
  }

  /** Send Channel.Close for a channel-level error and await the client's CloseOk. */
  private failChannel(err: ChannelError, causeMethodId: number): void {
    this.releaseAll();
    this.pendingPublish = null;
    this.state = 'closing-by-server';
    this.send(methods.ChannelClose, {
      replyCode: err.replyCode,
      replyText: err.replyText,
      classId: err.classId || classIdOf(causeMethodId),
      methodId: err.methodId || methodIdOf(causeMethodId),
    });
  }

  private dispatch(id: number, fields: MethodFields): void {
    switch (id) {
      case methods.ChannelClose:
        this.releaseAll();
        this.state = 'closed';
        this.send(methods.ChannelCloseOk, {});
        this.host.channelClosed(this.channelNumber);
        return;
      case methods.ExchangeDeclare:
        return this.exchangeDeclare(fields);
      case methods.ExchangeDelete:
        return this.exchangeDelete(fields);
      case methods.QueueDeclare:
        return this.queueDeclare(fields);
      case methods.QueueBind:
        return this.queueBind(fields);
      case methods.QueueUnbind:
        return this.queueUnbind(fields);
      case methods.QueuePurge:
        return this.queuePurge(fields);
      case methods.QueueDelete:
        return this.queueDelete(fields);
      case methods.BasicQos:
        return this.basicQos(fields);
      case methods.BasicPublish:
        return this.basicPublish(fields);
      case methods.BasicConsume:
        return this.basicConsume(fields);
      case methods.BasicCancel:
        return this.basicCancel(fields);
      case methods.BasicGet:
        return this.basicGet(fields);
      case methods.BasicAck:
        return this.basicAck(fields);
      case methods.BasicNack:
        return this.basicNack(fields);
      case methods.BasicReject:
        return this.basicReject(fields);
      case methods.BasicRecover:
        return this.basicRecover(fields);
      case methods.ConfirmSelect:
        this.confirmMode = true;
        if (!fields.nowait) this.send(methods.ConfirmSelectOk, {});
        return;
      case methods.TxSelect:
      case methods.TxCommit:
      case methods.TxRollback:
      case methods.ChannelFlow:
        throw new ChannelError(constants.NOT_IMPLEMENTED, `${methodName(id)} is not supported`, id);
      default:
        // Connection-class traffic on a data channel is a protocol violation;
        // anything else unsupported (exchange.bind, ...) fails just this channel
        if (classIdOf(id) === 10) {
          throw new ConnectionError(
            constants.COMMAND_INVALID,
            `unexpected method ${methodName(id)} on channel ${this.channelNumber}`,
            id,
          );
        }
        throw new ChannelError(constants.NOT_IMPLEMENTED, `${methodName(id)} is not supported`, id);
    }
  }

  // ---------------------------------------------------------------- exchanges

  private exchangeDeclare(f: MethodFields): void {
    this.broker.assertExchange(f.exchange as string, f.type as string, {
      passive: !!f.passive,
      durable: !!f.durable,
      autoDelete: !!f.autoDelete,
      internal: !!f.internal,
      arguments: (f.arguments as MethodFields) ?? {},
    });
    if (!f.nowait) this.send(methods.ExchangeDeclareOk, {});
  }

  private exchangeDelete(f: MethodFields): void {
    this.broker.deleteExchange(f.exchange as string, !!f.ifUnused);
    if (!f.nowait) this.send(methods.ExchangeDeleteOk, {});
  }

  // ------------------------------------------------------------------- queues

  private queueDeclare(f: MethodFields): void {
    const queue = this.broker.assertQueue(
      f.queue as string,
      {
        passive: !!f.passive,
        durable: !!f.durable,
        exclusive: !!f.exclusive,
        autoDelete: !!f.autoDelete,
        arguments: (f.arguments as MethodFields) ?? {},
      },
      this.host.connectionId,
    );
    this.lastDeclaredQueue = queue.name;
    if (!f.nowait) {
      this.send(methods.QueueDeclareOk, {
        queue: queue.name,
        messageCount: queue.messageCount,
        consumerCount: queue.consumerCount,
      });
    }
  }

  /** Empty queue names refer to the last queue declared on this channel. */
  private resolveQueueName(name: string): string {
    if (name !== '') return name;
    if (this.lastDeclaredQueue === null) {
      throw new ChannelError(constants.NOT_FOUND, 'no previously declared queue on this channel');
    }
    return this.lastDeclaredQueue;
  }

  private queueBind(f: MethodFields): void {
    this.broker.bindQueue(
      this.resolveQueueName(f.queue as string),
      f.exchange as string,
      f.routingKey as string,
      (f.arguments as MethodFields) ?? {},
      this.host.connectionId,
    );
    if (!f.nowait) this.send(methods.QueueBindOk, {});
  }

  private queueUnbind(f: MethodFields): void {
    this.broker.unbindQueue(
      this.resolveQueueName(f.queue as string),
      f.exchange as string,
      f.routingKey as string,
      this.host.connectionId,
    );
    this.send(methods.QueueUnbindOk, {});
  }

  private queuePurge(f: MethodFields): void {
    const messageCount = this.broker.purgeQueue(
      this.resolveQueueName(f.queue as string),
      this.host.connectionId,
    );
    if (!f.nowait) this.send(methods.QueuePurgeOk, { messageCount });
  }

  private queueDelete(f: MethodFields): void {
    const messageCount = this.broker.deleteQueue(
      this.resolveQueueName(f.queue as string),
      { ifUnused: !!f.ifUnused, ifEmpty: !!f.ifEmpty },
      this.host.connectionId,
    );
    if (!f.nowait) this.send(methods.QueueDeleteOk, { messageCount });
  }

  // ---------------------------------------------------------------- publish

  private basicPublish(f: MethodFields): void {
    const confirmSeq = this.confirmMode ? ++this.publishSeq : 0;
    const exchangeName = (f.exchange as string) ?? '';
    if (!this.broker.exchanges.has(exchangeName)) {
      throw new ChannelError(constants.NOT_FOUND, `no exchange '${exchangeName}' in vhost '/'`);
    }
    this.pendingPublish = {
      fields: f,
      confirmSeq,
      properties: null,
      totalSize: 0,
      received: 0,
      chunks: [],
    };
  }

  private handleHeader(frame: HeaderFrame): void {
    const pending = this.pendingPublish;
    if (!pending || pending.properties !== null) {
      throw new ConnectionError(
        constants.UNEXPECTED_FRAME,
        `unexpected content header on channel ${this.channelNumber}`,
      );
    }
    pending.properties = frame.fields;
    pending.totalSize = frame.size;
    if (pending.totalSize === 0) this.completePublish();
  }

  private handleBody(frame: BodyFrame): void {
    const pending = this.pendingPublish;
    if (!pending || pending.properties === null) {
      throw new ConnectionError(
        constants.UNEXPECTED_FRAME,
        `unexpected content body on channel ${this.channelNumber}`,
      );
    }
    // frame.content aliases the read buffer — copy before retaining
    pending.chunks.push(Buffer.from(frame.content));
    pending.received += frame.content.length;
    if (pending.received > pending.totalSize) {
      throw new ConnectionError(constants.FRAME_ERROR, 'content body exceeds declared size');
    }
    if (pending.received === pending.totalSize) this.completePublish();
  }

  private completePublish(): void {
    const pending = this.pendingPublish!;
    this.pendingPublish = null;
    const f = pending.fields;
    const message: Message = {
      exchange: (f.exchange as string) ?? '',
      routingKey: (f.routingKey as string) ?? '',
      properties: pending.properties ?? {},
      body: Buffer.concat(pending.chunks),
      redelivered: false,
    };
    let accepted = true;
    try {
      const result = this.broker.publish(message.exchange, message.routingKey, message);
      accepted = result.accepted;
      if (result.routed.length === 0 && f.mandatory) {
        this.send(methods.BasicReturn, {
          replyCode: constants.NO_ROUTE,
          replyText: 'NO_ROUTE',
          exchange: message.exchange,
          routingKey: message.routingKey,
        });
        this.sendContent(message.properties, message.body);
      }
    } catch (err) {
      if (err instanceof ChannelError) {
        this.failChannel(err, methods.BasicPublish);
        return;
      }
      throw err;
    }
    if (this.confirmMode) {
      // A queue refusing the message (x-overflow: reject-publish) surfaces
      // to the publisher as a nack, as in RabbitMQ
      this.send(
        accepted ? methods.BasicAck : methods.BasicNack,
        accepted
          ? { deliveryTag: pending.confirmSeq, multiple: false }
          : { deliveryTag: pending.confirmSeq, multiple: false, requeue: false },
      );
    }
  }

  // ---------------------------------------------------------------- consume

  private basicQos(f: MethodFields): void {
    this.prefetchCount = (f.prefetchCount as number) ?? 0;
    this.send(methods.BasicQosOk, {});
    this.drainConsumerQueues();
  }

  private basicConsume(f: MethodFields): void {
    const queue = this.broker.getQueue(
      this.resolveQueueName(f.queue as string),
      this.host.connectionId,
    );
    const consumerTag =
      (f.consumerTag as string) || `amq.ctag-${this.channelNumber}-${++this.consumerTagCounter}`;
    if (this.consumerQueues.has(consumerTag)) {
      throw new ChannelError(constants.NOT_ALLOWED, `consumer tag '${consumerTag}' already in use`);
    }
    if (f.exclusive && queue.consumerCount > 0) {
      throw new ChannelError(
        constants.ACCESS_REFUSED,
        `queue '${queue.name}' in exclusive use`,
      );
    }
    if (queue.hasExclusiveConsumer) {
      throw new ChannelError(
        constants.ACCESS_REFUSED,
        `queue '${queue.name}' in exclusive use`,
      );
    }
    const noAck = !!f.noAck;
    if (!f.nowait) this.send(methods.BasicConsumeOk, { consumerTag });
    this.consumerQueues.set(consumerTag, queue);
    // Registering triggers delivery of buffered messages, so ConsumeOk went first
    queue.addConsumer({
      consumerTag,
      noAck,
      exclusive: !!f.exclusive,
      deliver: (message, fromQueue) => this.deliverToConsumer(consumerTag, noAck, message, fromQueue),
      cancel: () => this.consumerCancelledByServer(consumerTag),
    });
  }

  private deliverToConsumer(
    consumerTag: string,
    noAck: boolean,
    message: Message,
    queue: Queue,
  ): boolean {
    if (this.state !== 'open') return false;
    if (this.host.sendBlocked) return false; // socket backed up — queue keeps it
    if (!noAck && this.prefetchCount > 0 && this.unackedConsumeCount() >= this.prefetchCount) {
      return false;
    }
    const deliveryTag = ++this.deliveryTagCounter;
    if (!noAck) this.unacked.set(deliveryTag, { message, queue, fromConsumer: true });
    this.send(methods.BasicDeliver, {
      consumerTag,
      deliveryTag,
      redelivered: message.redelivered,
      exchange: message.exchange,
      routingKey: message.routingKey,
    });
    this.sendContent(message.properties, message.body);
    return true;
  }

  private unackedConsumeCount(): number {
    let n = 0;
    for (const entry of this.unacked.values()) if (entry.fromConsumer) n++;
    return n;
  }

  /** The queue vanished from under this consumer — send basic.cancel. */
  private consumerCancelledByServer(consumerTag: string): void {
    if (!this.consumerQueues.delete(consumerTag)) return;
    if (this.state === 'open') {
      this.send(methods.BasicCancel, { consumerTag, nowait: true });
    }
  }

  private basicCancel(f: MethodFields): void {
    const consumerTag = f.consumerTag as string;
    const queue = this.consumerQueues.get(consumerTag);
    if (queue) {
      this.consumerQueues.delete(consumerTag);
      queue.removeConsumer(consumerTag);
      this.broker.autoDeleteIfUnused(queue);
    }
    // Cancelling an unknown consumer succeeds (RabbitMQ behaviour)
    if (!f.nowait) this.send(methods.BasicCancelOk, { consumerTag });
  }

  private basicGet(f: MethodFields): void {
    const queue = this.broker.getQueue(
      this.resolveQueueName(f.queue as string),
      this.host.connectionId,
    );
    const message = queue.take();
    if (!message) {
      this.send(methods.BasicGetEmpty, {});
      return;
    }
    const deliveryTag = ++this.deliveryTagCounter;
    if (!f.noAck) this.unacked.set(deliveryTag, { message, queue, fromConsumer: false });
    this.send(methods.BasicGetOk, {
      deliveryTag,
      redelivered: message.redelivered,
      exchange: message.exchange,
      routingKey: message.routingKey,
      messageCount: queue.messageCount,
    });
    this.sendContent(message.properties, message.body);
  }

  // ------------------------------------------------------------ acknowledgement

  /** Resolve a delivery tag (+multiple) to unacked entries, removing them. */
  private takeUnacked(deliveryTag: number, multiple: boolean): UnackedEntry[] {
    const taken: UnackedEntry[] = [];
    if (multiple) {
      // tag 0 with multiple means "everything so far"
      for (const [tag, entry] of this.unacked) {
        if (deliveryTag === 0 || tag <= deliveryTag) {
          taken.push(entry);
          this.unacked.delete(tag);
        }
      }
    } else {
      const entry = this.unacked.get(deliveryTag);
      if (!entry) {
        throw new ChannelError(
          constants.PRECONDITION_FAILED,
          `unknown delivery tag ${deliveryTag}`,
        );
      }
      taken.push(entry);
      this.unacked.delete(deliveryTag);
    }
    return taken;
  }

  private basicAck(f: MethodFields): void {
    this.takeUnacked((f.deliveryTag as number) ?? 0, !!f.multiple);
    this.drainConsumerQueues();
  }

  private basicNack(f: MethodFields): void {
    const taken = this.takeUnacked((f.deliveryTag as number) ?? 0, !!f.multiple);
    if (f.requeue) this.requeueEntries(taken);
    else this.deadLetterEntries(taken);
    this.drainConsumerQueues();
  }

  private basicReject(f: MethodFields): void {
    const taken = this.takeUnacked((f.deliveryTag as number) ?? 0, false);
    if (f.requeue) this.requeueEntries(taken);
    else this.deadLetterEntries(taken);
    this.drainConsumerQueues();
  }

  /** Rejected without requeue → the queue's DLX (or dropped if none). */
  private deadLetterEntries(entries: UnackedEntry[]): void {
    for (const entry of entries) {
      this.broker.deadLetter(entry.queue, entry.message, 'rejected');
    }
  }

  private basicRecover(f: MethodFields): void {
    const taken = [...this.unacked.values()];
    this.unacked.clear();
    if (f.requeue) this.requeueEntries(taken);
    this.send(methods.BasicRecoverOk, {});
    this.drainConsumerQueues();
  }

  /** Return messages to their queues, head-first, preserving tag order per queue. */
  private requeueEntries(entries: UnackedEntry[]): void {
    const byQueue = new Map<Queue, Message[]>();
    for (const entry of entries) {
      // Skip queues deleted since delivery
      if (this.broker.queues.get(entry.queue.name) !== entry.queue) continue;
      let list = byQueue.get(entry.queue);
      if (!list) byQueue.set(entry.queue, (list = []));
      list.push(entry.message);
    }
    for (const [queue, list] of byQueue) queue.requeue(list);
  }

  /** Prefetch capacity may have freed up — let our queues try to deliver. */
  private drainConsumerQueues(): void {
    for (const queue of new Set(this.consumerQueues.values())) queue.drain();
  }

  /** The connection's write buffer drained — resume paused deliveries. */
  onWritable(): void {
    this.drainConsumerQueues();
  }

  // ---------------------------------------------------------------- teardown

  /** Cancel consumers and requeue unacked messages (channel/connection close). */
  releaseAll(): void {
    for (const [consumerTag, queue] of this.consumerQueues) {
      queue.removeConsumer(consumerTag);
      this.broker.autoDeleteIfUnused(queue);
    }
    this.consumerQueues.clear();
    const entries = [...this.unacked.values()];
    this.unacked.clear();
    this.requeueEntries(entries);
  }

  /** Called when the whole connection is going away (no frames may be sent). */
  shutdown(): void {
    this.state = 'closed';
    this.releaseAll();
  }
}
