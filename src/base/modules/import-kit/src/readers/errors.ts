import type { Diagnostic, SourceLocation } from '../types.ts';

/** Safe, structured diagnostics: never include input bytes, URLs or thrown causes. */
export class ReaderError extends Error {
  readonly code: string;
  readonly diagnostics: Diagnostic[];
  constructor(code: string, location?: SourceLocation) {
    super(code);
    this.name = 'ReaderError';
    this.code = code;
    this.diagnostics = [{ severity: 'error', code, message: code, ...(location ? { location } : {}) }];
  }
}
export function fail(code: string, location?: SourceLocation): never { throw new ReaderError(code, location); }
export function checkAbort(signal?: AbortSignal): void { if (signal?.aborted) fail('SOURCE_CANCELLED'); }

/** Observe cancellation even when a supplied transport/sleep does not cooperate. */
export async function abortable<T>(work: Promise<T>, signal?: AbortSignal): Promise<T> {
  // Creating work can synchronously abort the signal. Observe its rejection even
  // when cancellation has already won, so abandoned reads cannot reject unhandled.
  if (signal?.aborted) { void work.catch(() => {}); fail('SOURCE_CANCELLED'); }
  if (!signal) return work;
  let onAbort: () => void = () => {};
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(new ReaderError('SOURCE_CANCELLED'));
    signal.addEventListener('abort', onAbort, { once: true });
  });
  try { return await Promise.race([work, aborted]); }
  finally { signal.removeEventListener('abort', onAbort); }
}
