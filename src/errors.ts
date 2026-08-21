import { classIdOf, constantStrs, methodIdOf } from './wire';

/**
 * An AMQP protocol error carrying the reply code and the class/method of the
 * offending method, ready to be turned into a Channel.Close or
 * Connection.Close frame.
 */
export abstract class AmqpProtocolError extends Error {
  readonly classId: number;
  readonly methodId: number;

  constructor(
    readonly replyCode: number,
    replyText: string,
    causeMethodId = 0,
  ) {
    super(`${constantStrs[replyCode] ?? replyCode} - ${replyText}`);
    this.classId = classIdOf(causeMethodId);
    this.methodId = methodIdOf(causeMethodId);
  }

  get replyText(): string {
    return this.message;
  }
}

/** Closes only the channel it occurred on; the connection survives. */
export class ChannelError extends AmqpProtocolError {}

/** Closes the whole connection. */
export class ConnectionError extends AmqpProtocolError {}
