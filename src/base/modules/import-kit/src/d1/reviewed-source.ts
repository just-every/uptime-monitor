import type { SourceRecord } from '../types.ts';
import { canonicalHash, cloneJson } from '../provenance/canonical-stream.ts';

/** During initial review, source records already remain available to the
 * planner. Read a bounded fresh copy and fence it against the normalized page
 * observed during mapping. Durable continuation uses only stored plans. */
export function reviewedSourceRecords(records: SourceRecord[], hashes: string[], pageSize: number, total: number): SourceRecord[] {
  let current = -1, page: SourceRecord[] = [];
  const at = (index: number): SourceRecord => {
    const ordinal = Math.floor(index / pageSize);
    if (current !== ordinal) {
      const start = ordinal * pageSize;
      const next = cloneJson(records.slice(start, Math.min(start + pageSize, total)));
      if (records.length !== total || canonicalHash(next) !== hashes[ordinal]) throw new Error('IMPORT_STREAM_SOURCE_CHANGED');
      page = next; current = ordinal;
    }
    return page[index % pageSize];
  };
  return new Proxy(new Array<SourceRecord>(total), {
    get(target, key, receiver) { if (typeof key === 'string' && /^(0|[1-9][0-9]*)$/.test(key)) return Number(key) < total ? at(Number(key)) : undefined; return Reflect.get(target,key,receiver); },
    has(target, key) { return typeof key === 'string' && /^(0|[1-9][0-9]*)$/.test(key) && Number(key) < total || Reflect.has(target,key); },
  });
}
