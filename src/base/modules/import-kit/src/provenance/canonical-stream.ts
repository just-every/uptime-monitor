import { Buffer } from 'node:buffer';
import { jsonStringParts } from './identity.ts';
export { canonicalHash } from './canonical-hash.ts';

function* segments(value: unknown): Generator<string> {
  if (typeof value === 'string') { yield* jsonStringParts(value); return; }
  if (value === null || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) { yield JSON.stringify(value); return; }
  if (Array.isArray(value)) {
    yield '[';
    const length = value.length;
    for (let i = 0; i < length; i++) { if (i) yield ','; yield* segments(value[i]); }
    yield ']'; return;
  }
  if (typeof value === 'object' && value && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
    yield '{'; let first = true;
    for (const key of Object.keys(value).sort()) { if (!first) yield ','; first = false; yield* jsonStringParts(key); yield ':'; yield* segments((value as Record<string, unknown>)[key]); }
    yield '}'; return;
  }
  throw new Error('INVALID_JSON: durable values must be finite JSON');
}

/** Exact canonical JSON without constructing a second whole-plan string.
 * Storage chunks are bounded by UTF-8 bytes and never split a code point. */
export function* canonicalChunks(value: unknown, limit = 128 * 1024): Generator<string> {
  let parts: string[] = [], bytes = 0;
  for (const segment of segments(value)) {
    const size = Buffer.byteLength(segment);
    if (size <= limit) {
      if (bytes + size > limit) { yield parts.join(''); parts = []; bytes = 0; }
      parts.push(segment); bytes += size;
    } else {
      const encoded = Buffer.from(segment, 'utf8');
      for (let start = 0; start < encoded.length;) {
        if (bytes) { yield parts.join(''); parts = []; bytes = 0; }
        let end = Math.min(start + limit, encoded.length);
        while (end < encoded.length && (encoded[end] & 0xc0) === 0x80) end--;
        const part = encoded.toString('utf8', start, end); start = end;
        if (end < encoded.length) yield part; else { parts.push(part); bytes = Buffer.byteLength(part); }
      }
    }
  }
  if (bytes) yield parts.join('');
}

/** Copy validated JSON without materializing a second whole-document string.
 * Immutable string values stay shared; mutable containers remain independent. */
export function cloneJson<T>(value: T): T {
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) return (Object.is(value, -0) ? 0 : value) as T;
  if (Array.isArray(value)) return Array.from({ length: value.length }, (_, index) => cloneJson(value[index])) as T;
  if (typeof value === 'object' && value && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) Object.defineProperty(result, key, {value:cloneJson((value as any)[key]),writable:true,enumerable:true,configurable:true});
    return result as T;
  }
  throw new Error('INVALID_JSON: durable values must be finite JSON');
}

/** Exact encoded size without constructing the encoded document. */
export function canonicalSize(value: unknown): number {
  const strings = new Map<string, number>();
  const textSize = (text: string) => {
    const previous = strings.get(text); if (previous !== undefined) return previous;
    let size = 0;
    for (const part of jsonStringParts(text)) size += Buffer.byteLength(part);
    if (strings.size < 4096) strings.set(text, size);
    return size;
  };
  const sizeOf = (item: unknown): number => {
    if (item === null) return 4;
    if (typeof item === 'string') return textSize(item);
    if (typeof item === 'boolean') return item ? 4 : 5;
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item).length;
    if (Array.isArray(item)) { const length = item.length; let size = 2 + Math.max(0, length - 1); for (let index = 0; index < length; index++) size += sizeOf(item[index]); return size; }
    if (item && typeof item === 'object') {
      const prototype = Object.getPrototypeOf(item);
      if (prototype === Object.prototype || prototype === null) {
        const keys = Object.keys(item); let size = 2 + Math.max(0, keys.length - 1);
        for (const key of keys) size += textSize(key) + 1 + sizeOf((item as any)[key]);
        return size;
      }
    }
    throw new Error('INVALID_JSON: durable values must be finite JSON');
  };
  return sizeOf(value);
}
