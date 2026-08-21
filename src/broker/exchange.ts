import type { MethodFields } from '../wire';

export type ExchangeType = 'direct' | 'fanout' | 'topic' | 'x-consistent-hash';

export interface ExchangeProperties {
  type: ExchangeType;
  durable: boolean;
  autoDelete: boolean;
  internal: boolean;
  arguments: MethodFields;
}

export interface Binding {
  queueName: string;
  bindingKey: string;
  arguments: MethodFields;
}

const topicRegexCache = new Map<string, RegExp>();

/**
 * Compile an AMQP topic binding pattern to a RegExp.
 * `*` matches exactly one word; `#` matches zero or more words.
 */
export function topicPatternToRegex(pattern: string): RegExp {
  let regex = topicRegexCache.get(pattern);
  if (regex) return regex;

  const escape = (w: string) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  let src = pattern
    .split('.')
    .map((w) => (w === '*' ? '[^.]+' : w === '#' ? '#' : escape(w)))
    .join('\\.');
  // Collapse runs of '#' separated by dots — '#.#' is equivalent to '#'
  src = src.replace(/#(?:\\\.#)+/g, '#');
  // '#' must match zero-or-more words, absorbing an adjacent dot
  src = src.replace(/\\\.#\\\./g, '\\.(?:[^.]+\\.)*'); //  a.#.b
  src = src.replace(/^#\\\./g, '(?:[^.]+\\.)*'); //         #.b
  src = src.replace(/\\\.#$/g, '(?:\\.[^.]+)*'); //         a.#
  src = src.replace(/^#$/, '.*'); //                        just #

  regex = new RegExp(`^${src}$`);
  topicRegexCache.set(pattern, regex);
  return regex;
}

export class Exchange {
  readonly bindings: Binding[] = [];

  constructor(
    readonly name: string,
    readonly properties: ExchangeProperties,
  ) {}

  bind(queueName: string, bindingKey: string, args: MethodFields): void {
    const exists = this.bindings.some(
      (b) => b.queueName === queueName && b.bindingKey === bindingKey,
    );
    if (!exists) this.bindings.push({ queueName, bindingKey, arguments: args });
  }

  /** @returns true if a binding was removed. */
  unbind(queueName: string, bindingKey: string): boolean {
    const i = this.bindings.findIndex(
      (b) => b.queueName === queueName && b.bindingKey === bindingKey,
    );
    if (i === -1) return false;
    this.bindings.splice(i, 1);
    return true;
  }

  removeBindingsFor(queueName: string): void {
    for (let i = this.bindings.length - 1; i >= 0; i--) {
      if (this.bindings[i].queueName === queueName) this.bindings.splice(i, 1);
    }
  }

  /** Names of queues this routing key routes to (deduplicated). */
  route(routingKey: string): Set<string> {
    const matches = new Set<string>();
    if (this.properties.type === 'x-consistent-hash') {
      const queueName = this.routeByHash(routingKey);
      if (queueName !== null) matches.add(queueName);
      return matches;
    }
    for (const binding of this.bindings) {
      switch (this.properties.type) {
        case 'fanout':
          matches.add(binding.queueName);
          break;
        case 'direct':
          if (binding.bindingKey === routingKey) matches.add(binding.queueName);
          break;
        case 'topic':
          if (topicPatternToRegex(binding.bindingKey).test(routingKey)) {
            matches.add(binding.queueName);
          }
          break;
      }
    }
    return matches;
  }

  /**
   * x-consistent-hash semantics (rabbitmq-consistent-hash-exchange plugin):
   * every message goes to exactly one bound queue, chosen by hashing the
   * routing key; the binding key is an integer weight. This is a simplified
   * weighted-modulo stand-in — stable for a fixed binding set, which is what
   * tests need, but without the plugin's minimal-reshuffling guarantee.
   */
  private routeByHash(routingKey: string): string | null {
    if (this.bindings.length === 0) return null;
    const weighted = [...this.bindings].sort((a, b) => a.queueName.localeCompare(b.queueName));
    let totalWeight = 0;
    const cumulative: Array<{ queueName: string; upTo: number }> = [];
    for (const binding of weighted) {
      const weight = Math.max(1, parseInt(binding.bindingKey, 10) || 1);
      totalWeight += weight;
      cumulative.push({ queueName: binding.queueName, upTo: totalWeight });
    }
    const point = fnv1a(routingKey) % totalWeight;
    for (const entry of cumulative) {
      if (point < entry.upTo) return entry.queueName;
    }
    return cumulative[cumulative.length - 1].queueName;
  }
}

/** FNV-1a 32-bit — a small, stable string hash. */
function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}
