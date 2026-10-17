import { createHash } from 'node:crypto';
import { Buffer } from 'node:buffer';
import { jsonStringParts } from './identity.ts';
import { ResumableSha256, type Sha256State } from './resumable-sha256.ts';

const encoder = new TextEncoder();
let availableBuffer: Uint8Array | undefined;
type StringCache = { strings: Map<string, Uint8Array>; retainedBytes: number };
const stringCache = (): StringCache => ({ strings: new Map(), retainedBytes: 0 });

/** Reuse only immutable string encodings during one caller-owned operation.
 * Clear in its finally block; no source containers or digests are cached. */
export function createCanonicalHasher() {
  const cache = stringCache();
  return {
    hash: (value: unknown) => hashCanonical(value, cache),
    clear() {
      for (const bytes of cache.strings.values()) bytes.fill(0);
      cache.strings.clear(); cache.retainedBytes = 0;
    },
  };
}

/** Hash exact canonical UTF-8 with a fixed output buffer. Repeated immutable
 * strings share encoded bytes only within this call; object contents are
 * always traversed afresh, including reads from normalized page views. */
export function canonicalHash(value: unknown): string {
  return hashCanonical(value, stringCache());
}
function hashCanonical(value: unknown, cache: StringCache): string {
  const writer = canonicalWriter(cache);
  try { writer.value(value); return writer.digest(); }
  finally { writer.release(); }
}

/** One canonical document may draw array elements from awaited custody reads.
 * Containers and values still use the same writer; no object digest is reused. */
export function createCanonicalStreamHash() {
  const cache = stringCache(), writer = canonicalWriter(cache);
  let closed = false;
  const active = () => { if (closed) throw new Error('CANONICAL_HASH_CLOSED'); };
  return {
    value(value: unknown) { active(); writer.value(value); },
    token(value: '[' | ']' | '{' | '}' | ':' | ',') { active(); writer.token(value); },
    digest() { active(); return writer.digest(); },
    clear() {
      if (closed) return;
      closed = true; writer.release();
      for (const bytes of cache.strings.values()) bytes.fill(0);
      cache.strings.clear(); cache.retainedBytes = 0;
    },
  };
}

/** The same canonical stream hash whose state can be saved between requests:
 * `checkpoint()` returns it, and `resume` continues from a saved one. Callers
 * keep the saved state server-side; it fences nothing by itself. */
export function createResumableCanonicalHash(resume?: Sha256State) {
  const cache = stringCache(), hash = resume ? ResumableSha256.from(resume) : new ResumableSha256(), writer = canonicalWriter(cache, hash);
  return {
    value(value: unknown) { writer.value(value); },
    token(value: '[' | ']' | '{' | '}' | ':' | ',') { writer.token(value); },
    checkpoint(): Sha256State { writer.flush(); return hash.state(); },
    digest() { return writer.digest(); },
    clear() { writer.release(); for (const bytes of cache.strings.values()) bytes.fill(0); cache.strings.clear(); cache.retainedBytes = 0; },
  };
}

function canonicalWriter(cache: StringCache, hash: { update(bytes: Uint8Array): unknown; digest(encoding: 'hex'): string } = createHash('sha256')) {
  // Hashing many small records must not allocate a new 64 KiB backing store
  // for each record. A nested getter acquires its own buffer; returned scratch
  // is wiped even when validation fails and retains no source bytes.
  const buffer = availableBuffer ?? new Uint8Array(64 * 1024), { strings } = cache;
  availableBuffer = undefined;
  const cacheEntries = 4096, cacheBytes = 1024 * 1024;
  let offset = 0;
  const flush = () => { if (offset) hash.update(buffer.subarray(0, offset)); offset = 0; };
  const byte = (value: number) => { if (offset === buffer.length) flush(); buffer[offset++] = value; };
  const ascii = (text: string) => { for (let index = 0; index < text.length; index++) byte(text.charCodeAt(index)); };
  const encoded = (bytes: Uint8Array) => {
    if (offset + bytes.length > buffer.length) flush();
    if (bytes.length >= buffer.length) hash.update(bytes);
    else { buffer.set(bytes, offset); offset += bytes.length; }
  };
  const text = (value: string) => {
    const previous = strings.get(value);
    if (previous !== undefined) { encoded(previous); return; }
    if (value.length > 1024 * 1024) { for (const part of jsonStringParts(value)) writeQuoted(part); return; }
    const quoted = JSON.stringify(value);
    if (strings.size < cacheEntries && Buffer.byteLength(quoted) <= cacheBytes - cache.retainedBytes) {
      const bytes = encoder.encode(quoted);
      strings.set(value, bytes); cache.retainedBytes += bytes.length; encoded(bytes); return;
    }
    // Large or distinct literals never grow the cache or create a second
    // whole-literal byte array. encodeInto preserves complete code points.
    writeQuoted(quoted);
  };
  const writeQuoted = (quoted: string) => {
    for (let consumed = 0; consumed < quoted.length;) {
      const { read, written } = encoder.encodeInto(quoted.slice(consumed), buffer.subarray(offset));
      offset += written; consumed += read;
      if (!read || offset === buffer.length) flush();
    }
  };
  const visit = (item: unknown): void => {
    if (item === null) { ascii('null'); return; }
    if (typeof item === 'string') { text(item); return; }
    if (typeof item === 'boolean') { ascii(item ? 'true' : 'false'); return; }
    if (typeof item === 'number' && Number.isFinite(item)) { ascii(JSON.stringify(item)); return; }
    if (Array.isArray(item)) {
      byte(91);
      const length = item.length;
      for (let index = 0; index < length; index++) { if (index) byte(44); visit(item[index]); }
      byte(93); return;
    }
    if (item && typeof item === 'object' && (Object.getPrototypeOf(item) === Object.prototype || Object.getPrototypeOf(item) === null)) {
      byte(123); let first = true;
      for (const key of Object.keys(item).sort()) { if (!first) byte(44); first = false; text(key); byte(58); visit((item as Record<string, unknown>)[key]); }
      byte(125); return;
    }
    throw new Error('INVALID_JSON: durable values must be finite JSON');
  };
  return {
    value: visit,
    token(value: string) { if (!/^[\[\]{}:,]$/.test(value)) throw new Error('INVALID_CANONICAL_TOKEN'); byte(value.charCodeAt(0)); },
    flush,
    digest() { flush(); return hash.digest('hex'); },
    release() { buffer.fill(0); availableBuffer ??= buffer; },
  };
}
