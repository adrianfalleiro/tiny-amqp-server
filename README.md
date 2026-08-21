# tiny-amqp-server

A small, in-memory AMQP 0-9-1 server for tests and local development — an
`amqplib`-compatible stand-in for RabbitMQ with no external processes.

The binary wire protocol (framing, method encode/decode, field tables,
heartbeats) is amqplib's own implementation, vendored from `amqplib@2.0.1`
(`src/vendor/amqplib`, MIT — see `LICENSE-amqplib` there). The AMQP protocol
layer is symmetric, so the same machinery the client uses serves the broker;
this project adds only the server-side state machine and routing.

## Usage

```ts
import { AmqpServer } from 'tiny-amqp-server';
import amqp from 'amqplib';

const server = new AmqpServer();
const { port } = await server.listen();          // 0 = ephemeral port

const connection = await amqp.connect(`amqp://127.0.0.1:${port}`);
// ...use it like RabbitMQ...

await server.close();
```

Test-friendly introspection is available on `server.broker`
(`queues`, `exchanges`, per-queue `messageCount`/`consumerCount`).

### In-memory transport (no TCP)

Skip the network entirely — no port, no `listen()`:

```ts
import { AmqpServer, connectInMemory } from 'tiny-amqp-server';
import amqp from 'amqplib';

const server = new AmqpServer();                    // never listens
const connection = await connectInMemory(amqp, server);
// a normal amqplib connection, over an in-process stream pair
```

`connectInMemory` substitutes `net.connect` for the synchronous duration of
`amqp.connect` so amqplib adopts an in-process duplex pair instead of dialling
TCP (verified against amqplib 0.10.x and 2.x). Lower-level pieces are also
exported: `openInMemorySocket(server)` returns a client-side stream for
driving amqplib's `Connection` directly, and `server.attachSocket(duplex)`
serves AMQP over any duplex stream.

## Supported

- Connection/channel lifecycle, heartbeats, PLAIN auth (accepts anything)
- `exchange.declare/delete` — direct, fanout, topic (`*`/`#` patterns), and
  `x-consistent-hash` (simplified: deterministic weighted hash per routing key)
- `queue.declare/bind/unbind/purge/delete`, exclusive + auto-delete queues,
  server-named queues, the default exchange
- `basic.publish/consume/cancel/get/ack/nack/reject/recover`, prefetch
  (`basic.qos`), requeue with `redelivered` flags
- Publisher confirms (`confirm.select`) and `mandatory` → `basic.return`
- Message TTL (`x-message-ttl` and per-message `expiration`, min wins) and
  dead-lettering (`x-dead-letter-exchange`/`x-dead-letter-routing-key`) on
  reject/nack without requeue, expiry, and length-limit overflow, with
  RabbitMQ-style `x-death` / `x-first-death-*` headers and automatic-cycle
  detection (retry loops that include a rejection still work)
- Queue lifetimes and limits: `x-expires` (idle queues are deleted),
  `x-max-length`/`x-max-length-bytes` with `x-overflow` `drop-head`
  (default, dead-letters the oldest) or `reject-publish` (publisher nack)
- Declare equivalence: redeclaring a queue/exchange with different
  properties **or arguments** fails with 406, like RabbitMQ
- Outbound backpressure: deliveries pause while the client socket's write
  buffer is full and resume on drain
- Server-sent `basic.cancel` when a consumed queue is deleted
  (`consumer_cancel_notify`)
- RabbitMQ-style error semantics: channel 404/406/403/405/530/540,
  connection 501/503/504/505; unsupported methods (e.g. `exchange.bind`)
  fail their channel with 540 rather than the whole connection

## Not supported

Durability/persistence (everything is in memory), TLS, real authentication,
transactions (`tx.*`), `channel.flow`, exchange-to-exchange bindings, headers
exchanges, priority queues (`x-max-priority`).

## Development

```sh
npm test          # vitest integration suite (real amqplib client over TCP)
npm run demo      # end-to-end publish/consume round trip
npm run build     # compile to dist/
```
