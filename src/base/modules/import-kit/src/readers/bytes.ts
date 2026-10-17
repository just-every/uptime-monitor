import { createHash } from 'node:crypto';
import { OriginalBytes } from './original.ts';
import type { SourceLocation } from '../types.ts';
import { abortable, checkAbort, fail } from './errors.ts';

export interface ByteBudget { used: number; max: number }
/** One pull at a time; closes the iterator on decoding, parsing, size or cancellation failure. */
export async function consumeBytes(
  bytes: Uint8Array | AsyncIterable<Uint8Array>, budget: ByteBudget, location: SourceLocation,
  consume: (chunk: Uint8Array) => void, signal?: AbortSignal,
): Promise<{ sha256: string; size: number; original: { status: 'available'; base64: string } }> {
  const hash = createHash('sha256');
  const original = new OriginalBytes();
  let size = 0;
  const accept = (chunk: Uint8Array): void => {
    checkAbort(signal);
    if (!(chunk instanceof Uint8Array)) fail('INVALID_SOURCE_CHUNK', location);
    if (chunk.byteLength > budget.max - budget.used) fail('SOURCE_BYTE_LIMIT', location);
    budget.used += chunk.byteLength; size += chunk.byteLength;
    hash.update(chunk); original.append(chunk); consume(chunk);
  };
  if (bytes instanceof Uint8Array) accept(bytes);
  else {
    let iterator: AsyncIterator<Uint8Array>;
    try { iterator = bytes[Symbol.asyncIterator](); } catch { fail('INVALID_SOURCE_STREAM', location); }
    let complete = false;
    try {
      while (true) {
        checkAbort(signal);
        let result: IteratorResult<Uint8Array>;
        try { result = await abortable(Promise.resolve(iterator.next()), signal); }
        catch { checkAbort(signal); fail('SOURCE_READ_FAILED', location); }
        if (result.done) { complete = true; break; }
        accept(result.value);
      }
    } finally {
      if (!complete && iterator.return) {
        // Do not let an uncooperative producer delay cancellation or mask the real error.
        try { void Promise.resolve(iterator.return()).catch(() => {}); } catch {}
      }
    }
  }
  return { sha256: hash.digest('hex'), size, original: original.finish() };
}

export function utf8Decoder(location: SourceLocation): { decode(chunk?: Uint8Array, final?: boolean): string } {
  const decoder = new TextDecoder('utf-8', { fatal: true });
  return { decode(chunk, final = false) {
    try { return decoder.decode(chunk, { stream: !final }); }
    catch { fail('MALFORMED_UTF8', location); }
  } };
}

/** Cancels a web body on early exit, including a read that is still pending. */
export async function* responseBytes(response: Response, signal?: AbortSignal): AsyncGenerator<Uint8Array> {
  if (!response.body) return;
  const reader = response.body.getReader();
  let complete = false;
  try {
    while (true) {
      checkAbort(signal);
      const result = await abortable(reader.read(), signal);
      if (result.done) { complete = true; break; }
      yield result.value;
    }
  } finally {
    if (!complete) { try { await reader.cancel(); } catch {} }
    reader.releaseLock();
  }
}
export function discardBody(response: Response): void {
  try { void response.body?.cancel().catch(() => {}); } catch {}
}
