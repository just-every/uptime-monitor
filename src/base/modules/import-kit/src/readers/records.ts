import { Buffer } from 'node:buffer';
import type { Json, PaginationCursorPolicy, ParsedRecord, ReaderLimits, SourceBundle, SourceLocation } from '../types.ts';
import { assertNoCredentials, canonicalJson, sha256 } from '../provenance/identity.ts';
import { paginationPolicy, reflectedCredentialMatcher } from '../provenance/credentials.ts';
import { reflectedNumberCredentialMatcher } from '../provenance/numeric-credentials.ts';
import { fail } from './errors.ts';

export function readerPaginationPolicy(value: unknown): PaginationCursorPolicy | undefined {
  try { return paginationPolicy(value); } catch { fail('INVALID_PAGINATION_CURSOR_POLICY'); }
}

export const DEFAULT_LIMITS = { maxBytes: 64 * 1024 * 1024, maxRecords: 100_000, maxRecordBytes: 1024 * 1024 };
export function limitsFor(input: ReaderLimits = {}): Required<ReaderLimits> {
  const limits = { ...DEFAULT_LIMITS, ...input };
  for (const value of Object.values(limits)) if (!Number.isSafeInteger(value) || value < 1) fail('INVALID_READER_LIMIT');
  return limits;
}
export function textId(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !value.trim() || value.length > 4096) fail('INVALID_SOURCE_ID');
}
export function safeValue(value: unknown, secrets: readonly string[] = [], location?: SourceLocation, policy?: PaginationCursorPolicy): string {
  let json: string;
  try {
    json = canonicalJson(value);
    assertNoCredentials(value, policy);
  } catch { fail('INVALID_OR_CREDENTIAL_SOURCE_DATA', location); }
  // Inspect scalar values; JSON escaping and numeric coercion cannot hide authentication.
  const reflectsCredential = reflectedCredentialMatcher(secrets);
  const reflectsNumber = reflectedNumberCredentialMatcher(secrets, reflectsCredential);
  const check = (item: unknown): void => {
    if (typeof item === 'string' && reflectsCredential(item)) fail('CREDENTIAL_IN_SOURCE_DATA', location);
    if (typeof item === 'number' && reflectsNumber(String(item))) fail('CREDENTIAL_IN_SOURCE_DATA', location);
    if ((item === null || typeof item === 'boolean') && reflectsCredential(String(item))) fail('CREDENTIAL_IN_SOURCE_DATA', location);
    if (item && typeof item === 'object') for (const [key, child] of Object.entries(item)) { check(key); check(child); }
  };
  check(value);
  return json;
}

export class RecordCollector {
  readonly bundle: SourceBundle = { records: [], artifacts: [], diagnostics: [] };
  readonly seen = new Map<string, string>();
  readonly namespace: string;
  readonly limits: Required<ReaderLimits>;
  readonly secrets: readonly string[];
  readonly paginationCursorPolicy?: PaginationCursorPolicy;
  constructor(namespace: string, limits: Required<ReaderLimits>, secrets: readonly string[] = [], paginationCursorPolicy?: PaginationCursorPolicy) {
    paginationPolicy(paginationCursorPolicy);
    this.paginationCursorPolicy = paginationCursorPolicy;
    if (paginationCursorPolicy) this.bundle.paginationCursorPolicy = paginationCursorPolicy;
    textId(namespace); safeValue(namespace, secrets, undefined, paginationCursorPolicy);
    this.namespace = namespace; this.limits = limits; this.secrets = secrets;
  }
  add(parsed: ParsedRecord[], location: SourceLocation, artifactHash: string): void {
    if (!Array.isArray(parsed)) fail('INVALID_PARSED_RECORDS', location);
    if (this.bundle.records.length + parsed.length > this.limits.maxRecords) fail('SOURCE_RECORD_LIMIT', location);
    for (const record of parsed) {
      if (!record || typeof record !== 'object') fail('INVALID_PARSED_RECORD', location);
      try { textId(record.kind); textId(record.sourceId); } catch { fail('INVALID_PARSED_RECORD_ID', location); }
      const encoded = safeValue(record, this.secrets, location, this.paginationCursorPolicy);
      if (Buffer.byteLength(encoded) > this.limits.maxRecordBytes) fail('SOURCE_RECORD_BYTE_LIMIT', location);
      if (record.pointer !== undefined && (typeof record.pointer !== 'string' || (record.pointer !== '' && !record.pointer.startsWith('/')))) fail('INVALID_SOURCE_POINTER', location);
      // Detach callback-owned objects; later callback mutations cannot alter earlier records.
      const payload: Json = JSON.parse(safeValue(record.payload, this.secrets, location, this.paginationCursorPolicy));
      const payloadHash = sha256(canonicalJson(payload));
      const actualLocation = { ...location, ...(record.pointer !== undefined ? { pointer: record.pointer } : {}) };
      const key = canonicalJson([record.kind, record.sourceId]);
      if (this.seen.has(key)) {
        const conflict = this.seen.get(key) !== payloadHash;
        this.bundle.diagnostics.push({ severity: conflict ? 'error' : 'warning', code: conflict ? 'DUPLICATE_SOURCE_CONFLICT' : 'DUPLICATE_SOURCE', message: conflict ? 'Source identity has different content.' : 'Source identity repeats identical content.', kind: record.kind, sourceId: record.sourceId, location: actualLocation });
      } else this.seen.set(key, payloadHash);
      this.bundle.records.push({ namespace: this.namespace, kind: record.kind, sourceId: record.sourceId, payload, payloadHash, artifactHash, location: actualLocation });
    }
  }
}
