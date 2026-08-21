/**
 * Typed access to the vendored amqplib wire-protocol modules
 * (src/vendor/amqplib, MIT — see LICENSE-amqplib there).
 *
 * The AMQP 0-9-1 framing layer is symmetric, so the same encoders/decoders
 * the amqplib client uses work verbatim for a server. This module is the
 * single import point for everything wire-level.
 */

/* eslint-disable @typescript-eslint/no-var-requires */
// The vendor files are CommonJS with no type declarations; we assign our own
// signatures here rather than letting TS infer over generated code.
const defsJs = require('./vendor/amqplib/defs.js');
const frameJs = require('./vendor/amqplib/frame.js');
const heartbeatJs = require('./vendor/amqplib/heartbeat.js');

export type MethodFields = Record<string, unknown>;

/** Raw frame as split off the byte stream by parseFrame. */
export interface FrameStructure {
  type: number;
  channel: number;
  size: number;
  /** NOTE: subarray aliasing the read buffer — copy before retaining. */
  payload: Buffer;
  rest: Buffer;
}

export interface MethodFrame {
  id: number;
  channel: number;
  fields: MethodFields;
}

export interface HeaderFrame {
  /** class id — 60 (BasicProperties) for all content we handle */
  id: number;
  channel: number;
  /** total content body size in bytes */
  size: number;
  fields: MethodFields;
}

export interface BodyFrame {
  channel: number;
  /** NOTE: subarray aliasing the read buffer — copy before retaining. */
  content: Buffer;
}

/** Singleton sentinel returned for heartbeat frames — compare with `===`. */
export type HeartbeatFrame = { channel: 0 };

export type DecodedFrame = MethodFrame | HeaderFrame | BodyFrame | HeartbeatFrame;

export interface MethodArgInfo {
  type: string;
  name: string;
  default?: unknown;
}

export interface MethodInfo {
  id: number;
  classId: number;
  methodId: number;
  name: string;
  args: MethodArgInfo[];
}

interface WireConstants {
  FRAME_METHOD: number;
  FRAME_HEADER: number;
  FRAME_BODY: number;
  FRAME_HEARTBEAT: number;
  FRAME_MIN_SIZE: number;
  FRAME_END: number;
  REPLY_SUCCESS: number;
  CONTENT_TOO_LARGE: number;
  NO_ROUTE: number;
  NO_CONSUMERS: number;
  CONNECTION_FORCED: number;
  INVALID_PATH: number;
  ACCESS_REFUSED: number;
  NOT_FOUND: number;
  RESOURCE_LOCKED: number;
  PRECONDITION_FAILED: number;
  FRAME_ERROR: number;
  SYNTAX_ERROR: number;
  COMMAND_INVALID: number;
  CHANNEL_ERROR: number;
  UNEXPECTED_FRAME: number;
  RESOURCE_ERROR: number;
  NOT_ALLOWED: number;
  NOT_IMPLEMENTED: number;
  INTERNAL_ERROR: number;
}

export const constants: WireConstants = defsJs.constants;
export const constantStrs: Record<number, string> = defsJs.constant_strs;
export const FRAME_OVERHEAD: number = defsJs.FRAME_OVERHEAD;

/** Decode a method's (or content header's) argument payload. */
export const decode: (id: number, buf: Buffer) => MethodFields = defsJs.decode;
/** Returns a COMPLETE wire frame (type, size, FRAME_END included) — write it directly. */
export const encodeMethod: (id: number, channel: number, fields: MethodFields) => Buffer =
  defsJs.encodeMethod;
/** Complete content-header frame; id must be 60 (BasicProperties), size = total body bytes. */
export const encodeProperties: (
  id: number,
  channel: number,
  size: number,
  fields: MethodFields,
) => Buffer = defsJs.encodeProperties;
export const methodInfo: (id: number) => MethodInfo = defsJs.info;

export const PROTOCOL_HEADER: string = frameJs.PROTOCOL_HEADER;
/** Returns false until a complete frame is buffered; throws on a bad frame-end byte. */
export const parseFrame: (bin: Buffer) => FrameStructure | false = frameJs.parseFrame;
export const decodeFrame: (frame: FrameStructure) => DecodedFrame = frameJs.decodeFrame;
export const makeBodyFrame: (channel: number, payload: Buffer) => Buffer = frameJs.makeBodyFrame;
export const HEARTBEAT: HeartbeatFrame = frameJs.HEARTBEAT;
export const HEARTBEAT_BUF: Buffer = frameJs.HEARTBEAT_BUF;

import type { EventEmitter } from 'node:events';
/** Role-agnostic heartbeat timer: 'beat' → send HEARTBEAT_BUF, 'timeout' → kill connection. */
export interface Heart extends EventEmitter {
  interval: number;
  clear(): void;
}
export const Heart: new (
  intervalSeconds: number,
  checkSend: () => boolean,
  checkRecv: () => boolean,
) => Heart = heartbeatJs.Heart;

/** Method id constants (classId << 16 | methodId), both directions. */
export const methods = {
  ConnectionStart: defsJs.ConnectionStart as number,
  ConnectionStartOk: defsJs.ConnectionStartOk as number,
  ConnectionSecure: defsJs.ConnectionSecure as number,
  ConnectionSecureOk: defsJs.ConnectionSecureOk as number,
  ConnectionTune: defsJs.ConnectionTune as number,
  ConnectionTuneOk: defsJs.ConnectionTuneOk as number,
  ConnectionOpen: defsJs.ConnectionOpen as number,
  ConnectionOpenOk: defsJs.ConnectionOpenOk as number,
  ConnectionClose: defsJs.ConnectionClose as number,
  ConnectionCloseOk: defsJs.ConnectionCloseOk as number,
  ConnectionBlocked: defsJs.ConnectionBlocked as number,
  ConnectionUnblocked: defsJs.ConnectionUnblocked as number,
  ChannelOpen: defsJs.ChannelOpen as number,
  ChannelOpenOk: defsJs.ChannelOpenOk as number,
  ChannelFlow: defsJs.ChannelFlow as number,
  ChannelFlowOk: defsJs.ChannelFlowOk as number,
  ChannelClose: defsJs.ChannelClose as number,
  ChannelCloseOk: defsJs.ChannelCloseOk as number,
  ExchangeDeclare: defsJs.ExchangeDeclare as number,
  ExchangeDeclareOk: defsJs.ExchangeDeclareOk as number,
  ExchangeDelete: defsJs.ExchangeDelete as number,
  ExchangeDeleteOk: defsJs.ExchangeDeleteOk as number,
  QueueDeclare: defsJs.QueueDeclare as number,
  QueueDeclareOk: defsJs.QueueDeclareOk as number,
  QueueBind: defsJs.QueueBind as number,
  QueueBindOk: defsJs.QueueBindOk as number,
  QueueUnbind: defsJs.QueueUnbind as number,
  QueueUnbindOk: defsJs.QueueUnbindOk as number,
  QueuePurge: defsJs.QueuePurge as number,
  QueuePurgeOk: defsJs.QueuePurgeOk as number,
  QueueDelete: defsJs.QueueDelete as number,
  QueueDeleteOk: defsJs.QueueDeleteOk as number,
  BasicQos: defsJs.BasicQos as number,
  BasicQosOk: defsJs.BasicQosOk as number,
  BasicConsume: defsJs.BasicConsume as number,
  BasicConsumeOk: defsJs.BasicConsumeOk as number,
  BasicCancel: defsJs.BasicCancel as number,
  BasicCancelOk: defsJs.BasicCancelOk as number,
  BasicPublish: defsJs.BasicPublish as number,
  BasicReturn: defsJs.BasicReturn as number,
  BasicDeliver: defsJs.BasicDeliver as number,
  BasicGet: defsJs.BasicGet as number,
  BasicGetOk: defsJs.BasicGetOk as number,
  BasicGetEmpty: defsJs.BasicGetEmpty as number,
  BasicAck: defsJs.BasicAck as number,
  BasicNack: defsJs.BasicNack as number,
  BasicReject: defsJs.BasicReject as number,
  BasicRecover: defsJs.BasicRecover as number,
  BasicRecoverOk: defsJs.BasicRecoverOk as number,
  ConfirmSelect: defsJs.ConfirmSelect as number,
  ConfirmSelectOk: defsJs.ConfirmSelectOk as number,
  TxSelect: defsJs.TxSelect as number,
  TxCommit: defsJs.TxCommit as number,
  TxRollback: defsJs.TxRollback as number,
} as const;

/** Content header class id. */
export const BasicProperties: number = defsJs.BasicProperties;

export function classIdOf(methodId: number): number {
  return methodId >>> 16;
}

export function methodIdOf(methodId: number): number {
  return methodId & 0xffff;
}

/** Human-readable method name for errors/logging; falls back to the raw id. */
export function methodName(id: number): string {
  try {
    return methodInfo(id).name;
  } catch {
    return `method:${id}`;
  }
}
