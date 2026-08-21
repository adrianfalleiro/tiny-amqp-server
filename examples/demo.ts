/**
 * End-to-end demo: start the server, connect a real amqplib client,
 * publish through a topic exchange, and consume the message back.
 *
 * Run with: npm run demo
 */
import amqp from 'amqplib';
import { AmqpServer } from '../src/index';

async function main() {
  const server = new AmqpServer();
  const { port } = await server.listen();
  console.log(`tiny-amqp-server listening on amqp://127.0.0.1:${port}`);

  const connection = await amqp.connect(`amqp://127.0.0.1:${port}`);
  const channel = await connection.createChannel();

  await channel.assertExchange('events', 'topic', { durable: false });
  const { queue } = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(queue, 'events', 'user.*.created');

  const received = new Promise<void>((resolve) => {
    channel.consume(queue, (msg) => {
      if (!msg) return;
      console.log(
        `received on '${msg.fields.routingKey}':`,
        msg.content.toString(),
        msg.properties.headers,
      );
      channel.ack(msg);
      resolve();
    });
  });

  channel.publish('events', 'user.42.created', Buffer.from('hello from tiny-amqp-server'), {
    contentType: 'text/plain',
    headers: { source: 'demo' },
  });

  await received;
  await connection.close();
  await server.close();
  console.log('done');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
