import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { Json } from '../types.ts';

const STRING_SLICE = 1 << 20;
/** JSON.stringify of one string, escaped in bounded slices. A single
 * JSON.stringify call buffers its whole output, and V8 aborts the process
 * (uncatchable) once that buffer passes 256 MiB. Slices never split a
 * surrogate pair, so the output is identical to JSON.stringify(text). */
export function jsonString(text: string): string {
  if (text.length <= STRING_SLICE) return JSON.stringify(text);
  return [...jsonStringParts(text)].join('');
}
/** The same exact escaping for consumers which must not join large literals. */
export function* jsonStringParts(text: string): Generator<string> {
  if (text.length <= STRING_SLICE) { yield JSON.stringify(text); return; }
  yield '"';
  for (let start = 0; start < text.length;) {
    let end = Math.min(start + STRING_SLICE, text.length);
    const last = text.charCodeAt(end - 1);
    if (end < text.length && last >= 0xd800 && last <= 0xdbff) end--;
    yield JSON.stringify(text.slice(start, end)).slice(1, -1);
    start = end;
  }
  yield '"';
}
/** Reject lossy JS values so a fingerprint always names exactly what will persist. */
export function canonicalJson(value: unknown): string {
  if (typeof value === 'string') return jsonString(value);
  if (value === null || typeof value === 'boolean') return JSON.stringify(value);
  if (typeof value === 'number' && Number.isFinite(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + Array.from({ length: value.length }, (_, index) => canonicalJson(value[index])).join(',') + ']';
  if (typeof value === 'object' && value && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)) {
    return '{' + Object.keys(value).sort().map(key => jsonString(key) + ':' + canonicalJson((value as Record<string, Json>)[key])).join(',') + '}';
  }
  throw new Error('INVALID_JSON: durable values must be finite JSON');
}
export const sha256 = (value: string | Uint8Array): string => createHash('sha256').update(value).digest('hex');
export const sourceIdentity = (destination: string, namespace: string, kind: string, sourceId: string): string => sha256(canonicalJson([destination, namespace, kind, sourceId]));
export const targetIdentity = (destination: string, kind: string, id: string): string => canonicalJson([destination, kind, id]);
export function requireText(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !value.trim() || value.length > 4096) throw new Error(`INVALID_ID: ${label}`);
}
export { assertNoCredentials } from './credentials.ts';
export function attachmentHash(attachment: { base64: string; sha256: string; mediaType: string } | undefined): string | null {
  if (!attachment) return null;
  const bytes = Buffer.from(attachment.base64, 'base64');
  if (bytes.toString('base64') !== attachment.base64 || sha256(bytes) !== attachment.sha256) throw new Error('ATTACHMENT_HASH_MISMATCH');
  requireText(attachment.mediaType, 'mediaType');
  return attachment.sha256;
}
