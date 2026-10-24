# tiny-amqp-server

An in-memory AMQP 0-9-1 server for tests and local development. Use your
existing `amqplib` client without running RabbitMQ.

```sh
npm install --save-dev @adrianfalleiro/tiny-amqp-server amqplib
```

## Usage

```ts
import amqp from 'amqplib';
import { AmqpServer, connectInMemory } from '@adrianfalleiro/tiny-amqp-server';

const server = new AmqpServer();
const connection = await connectInMemory(amqp, server);
const channel = await connection.createChannel();

await channel.assertQueue('jobs');
channel.sendToQueue('jobs', Buffer.from('hello'));
const message = await channel.get('jobs', { noAck: true });
console.log(message && message.content.toString()); // hello

await connection.close();
await server.close();
```

For TCP clients, replace `connectInMemory` with:

```ts
const { port } = await server.listen();
const connection = await amqp.connect(`amqp://127.0.0.1:${port}`);
```

Inspect queues and exchanges through `server.broker`.

## Scope

Supports direct, fanout, topic, and simplified consistent-hash routing;
consumer acknowledgements, prefetch, publisher confirms, TTL, dead-lettering,
and queue limits.

Everything stays in memory. This is a test double, not a production broker:
no persistence, TLS, real authentication, transactions, or full RabbitMQ parity.
Wire-protocol code is vendored from amqplib under its MIT license.

## Development

```sh
npm ci
npm test
npm run build
```

To release, update the version with `npm version`, then push the commit and
matching `v<version>` tag. GitHub Actions tests, builds, and publishes to npm
using trusted publishing.
