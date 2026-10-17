import type { ImportPlan, SourceBundle } from '../types.ts';

/**
 * Largest accepted import plan, in UTF-8 bytes of the canonical JSON the
 * fingerprint and every storage port encode (the plan without its
 * `fingerprint` field). See README "Plan-size cap" for the measurements.
 */
export const PLAN_BYTES_LIMIT = 300_000_000;

/** Checked arithmetic for measured bytes and the counters that change their
 * numeric lexemes. Signed deltas must stay exact as well as the final total. */
export function checkedSizeSum(a: number, b: number): number {
  const total = a + b;
  if (!Number.isSafeInteger(a) || !Number.isSafeInteger(b) || !Number.isSafeInteger(total)) {
    throw new Error('INVALID_JSON: canonical byte size exceeds safe integer range');
  }
  return total;
}

/** A plan (or its source, which the plan contains) over PLAN_BYTES_LIMIT.
 * `bytes` is the measured canonical size; for `subject: 'source'` it is a
 * lower bound of the plan that would have been built. */
export class PlanTooLargeError extends Error {
  readonly code = 'IMPORT_PLAN_TOO_LARGE';
  readonly bytes: number;
  readonly limit: number;
  readonly subject: 'source' | 'plan';
  constructor(bytes: number, limit: number, subject: 'source' | 'plan') {
    super(`IMPORT_PLAN_TOO_LARGE: the import ${subject} is ${bytes} canonical JSON bytes; the limit is ${limit}`);
    this.bytes = bytes; this.limit = limit; this.subject = subject;
  }
}

function stringBytes(text: string): number {
  let n = 2;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c === 0x22 || c === 0x5c || c === 0x08 || c === 0x09 || c === 0x0a || c === 0x0c || c === 0x0d) n += 2;
    else if (c < 0x20) n += 6;
    else if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xd800 && c <= 0xdbff) {
      const next = i + 1 < text.length ? text.charCodeAt(i + 1) : 0;
      if (next >= 0xdc00 && next <= 0xdfff) { n += 4; i++; } else n += 6;
    } else if (c >= 0xdc00 && c <= 0xdfff) n += 6;
    else n += 3;
  }
  return checkedSizeSum(n, 0);
}

/** A planning-local cache of immutable string sizes. Both entry count and
 * retained UTF-16 units are bounded; containers and property reads are never
 * cached across size walks. Oversized strings are measured without retention. */
export class StringSizeCache {
  private readonly sizes = new Map<string, number>();
  private units = 0;
  measure(text: string): number {
    const previous = this.sizes.get(text);
    if (previous !== undefined) return previous;
    const bytes = stringBytes(text);
    if (text.length <= 64 * 1024 && this.sizes.size < 4096 && this.units + text.length <= 512 * 1024) {
      this.sizes.set(text, bytes); this.units += text.length;
    }
    return bytes;
  }
}

/** UTF-8 byte length of canonicalJson(value), without building that string.
 * Shared references count at every occurrence. Cycles and nesting beyond 256
 * containers are rejected before the recursive encoder/credential walk runs. */
export function canonicalJsonBytes(value: unknown, skipKey?: string, strings = new StringSizeCache(), sorted = false): number {
  const active = new Set<object>();
  const measured = new WeakMap<object, { bytes: number; depth: number }>();
  const visit = (item: unknown, depth: number, omit?: string): number => {
    if (item === null) return 4;
    if (typeof item === 'boolean') return item ? 4 : 5;
    if (typeof item === 'string') return strings.measure(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item).length;
    if (typeof item !== 'object' || !item || (!Array.isArray(item) && Object.getPrototypeOf(item) !== Object.prototype && Object.getPrototypeOf(item) !== null)) throw new Error('INVALID_JSON: durable values must be finite JSON');
    if (depth >= 256 || active.has(item)) throw new Error('INVALID_JSON: cyclic or excessively nested durable value');
    // Root omission changes its shape; descendants are always counted whole.
    const cached = omit === undefined ? measured.get(item) : undefined;
    if (cached !== undefined && depth <= cached.depth) return cached.bytes;
    active.add(item);
    let n = 2;
    if (Array.isArray(item)) {
      for (let i = 0; i < item.length; i++) n = checkedSizeSum(n, checkedSizeSum(visit(item[i], depth + 1), i ? 1 : 0));
    } else {
      let first = true;
      const keys = Object.keys(item);
      if (sorted) keys.sort();
      for (const key of keys) {
        if (key === omit) continue;
        const prefix = checkedSizeSum(strings.measure(key), first ? 1 : 2);
        n = checkedSizeSum(n, checkedSizeSum(prefix, visit((item as Record<string, unknown>)[key], depth + 1)));
        first = false;
      }
    }
    active.delete(item);
    if (omit === undefined) measured.set(item, { bytes: n, depth });
    return n;
  };
  return visit(value, 0, skipKey);
}

export function assertPlanBytes(bytes: number): void {
  checkedSizeSum(bytes, 0);
  if (bytes > PLAN_BYTES_LIMIT) throw new PlanTooLargeError(bytes, PLAN_BYTES_LIMIT, 'plan');
}

/** Canonical size of the plan content the fingerprint encodes. */
export const planContentBytes = (plan: ImportPlan, strings?: StringSizeCache): number => canonicalJsonBytes(plan, 'fingerprint', strings);

/** Checked before any whole-source or whole-plan encoding. */
export function assertSourceSize(source: SourceBundle): void {
  const bytes = canonicalJsonBytes(source);
  if (bytes > PLAN_BYTES_LIMIT) throw new PlanTooLargeError(bytes, PLAN_BYTES_LIMIT, 'source');
}
export function assertPlanSize(plan: ImportPlan): void {
  const bytes = planContentBytes(plan);
  if (bytes > PLAN_BYTES_LIMIT) throw new PlanTooLargeError(bytes, PLAN_BYTES_LIMIT, 'plan');
}
