import { TargetIdentities } from './target-identities.ts';
import { reviewedSourceRecords } from './reviewed-source.ts';
import { compressedPlan, expandCompressedPlan } from './compressed-plan.ts';
export { compressedPlan, expandCompressedPlan } from './compressed-plan.ts';
import { partitionPlan } from './stream-partition.ts';
import { measurePlanWorkingSet, mergePlanPages } from './stream-working-set.ts';
import { Buffer } from 'node:buffer';
import type { ImportPlan, SourceBundle, ImportMapping } from '../types.ts';
import { buildStreamPagePlan, planFingerprint } from '../plan/index.ts';
import { canonicalHash, cloneJson } from '../provenance/canonical-stream.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import type { D1ImportContext, D1PlanCodec } from './types.ts';
import { authorize } from './database.ts';
import { SNAPSHOT_BYTES } from './sets.ts';
import { requestContext } from './budget.ts';

export interface UnverifiedStreamPages {
  version: 1;
  /** Independently verified, compressed normalized pages; no mapper is retained. */
  pages: string[];
  lengths: number[];
  header: Omit<ImportPlan, 'entries' | 'source' | 'fingerprint'> & { source: Omit<SourceBundle, 'records'> };
}
export interface ImportStreamPlan extends UnverifiedStreamPages { fingerprint: string }
const codecs = new WeakMap<ImportStreamPlan, D1PlanCodec>();
const reviewed = new WeakMap<ImportStreamPlan, { fingerprint: string; header: string; pages: string[]; lengths: number[]; codecId?: string; encode?: D1PlanCodec['encode']; decode?: D1PlanCodec['decode'] }>();
/** Reuse only an exact in-memory snapshot whose complete canonical fingerprint
 * was already computed. A changed byte takes the ordinary verification path. */
export function streamFingerprintMatches(stream: ImportStreamPlan, codec?: D1PlanCodec, original = stream): boolean {
  const prior = reviewed.get(original);
  if (prior && prior.codecId === codec?.id && prior.encode === codec?.encode && prior.decode === codec?.decode
    && prior.fingerprint === stream.fingerprint && prior.header === canonicalHash(stream.header)
    && prior.pages.length === stream.pages.length && prior.pages.every((page, i) => page === stream.pages[i])
    && prior.lengths.length === stream.lengths.length && prior.lengths.every((length, i) => length === stream.lengths[i])) return true;
  return planFingerprint(streamView(stream, codec)) === stream.fingerprint;
}
/** Array-compatible read view retaining only one normalized page at a time.
 * The view is consumed internally for the original canonical fingerprint. */
export function streamView(stream: ImportStreamPlan, codec = codecs.get(stream)): ImportPlan {
  const starts: number[] = []; let total = 0;
  for (const size of stream.lengths) { starts.push(total); total += size; }
  let current = -1, page: ImportPlan | undefined;
  const at = (index: number) => {
    let low = 0, high = starts.length;
    while (low + 1 < high) { const mid = (low + high) >>> 1; if (starts[mid] <= index) low = mid; else high = mid; }
    if (current !== low) { page = undefined; page = expandCompressedPlan(stream.pages[low],codec); current = low; }
    return page!.entries[index - starts[low]];
  };
  const view = <T>(get: (index: number) => T): T[] => new Proxy(new Array<T>(total), {
    get(target, key, receiver) { if (typeof key === 'string' && /^(0|[1-9][0-9]*)$/.test(key)) return Number(key) < total ? get(Number(key)) : undefined; return Reflect.get(target, key, receiver); },
    has(target, key) { return typeof key === 'string' && /^(0|[1-9][0-9]*)$/.test(key) && Number(key) < total || Reflect.has(target, key); },
  });
  return { ...stream.header, source: { ...stream.header.source, records: view(index => at(index).source) }, entries: view(at), fingerprint: stream.fingerprint };
}

/** Bounded normalized imports. Each input identity must be distinct; original
 * bytes and unscoped source diagnostics use the existing general import API. */
export async function planD1ImportStream(source: SourceBundle, mapping: ImportMapping, input: D1ImportContext): Promise<ImportStreamPlan> {
  input = requestContext(input);
  const { stream: pages, sourceHashes, pageSize } = await collectStreamPages(source, mapping, input);
  const stream: ImportStreamPlan = { ...pages, fingerprint: '' };
  const codec = input.storage.targets.planCodec;
  if (codec) codecs.set(stream,codec);
  const initial = streamView(stream);
  initial.source.records = reviewedSourceRecords(source.records, sourceHashes, pageSize, stream.header.counts.unique);
  stream.fingerprint = planFingerprint(initial);
  reviewed.set(stream, { fingerprint: stream.fingerprint, header: canonicalHash(stream.header), pages: [...stream.pages], lengths: [...stream.lengths], codecId: codec?.id, encode: codec?.encode, decode: codec?.decode });
  await authorize(input, 'plan');
  return stream;
}

/** Private preparation has no verified stream fingerprint. Only cold
 * verification may turn these independently checked pages into a public job. */
export async function collectStreamPages(source: SourceBundle, mapping: ImportMapping, input: D1ImportContext): Promise<{ stream: UnverifiedStreamPages; sourceHashes: string[]; pageSize: number }> {
  const context = requestContext(input); await authorize(context, 'plan');
  if (source.diagnostics.length || source.artifacts.some(a => a.original?.status === 'available')) throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
  const cursorPolicy = source.paginationCursorPolicy === undefined ? {} : { paginationCursorPolicy: source.paginationCursorPolicy };
  const pages: string[] = [], lengths: number[] = [], sourceHashes: string[] = [], identities = new Set<string>(), targets = new TargetIdentities();
  let header: ImportStreamPlan['header'] | undefined;
  const pageSize = 100;
  let pending: ImportPlan[] = [], pendingBytes = 0, pendingExpandedBytes = 0;
  const flush = () => {
    if (!pending.length) return;
    const combined = mergePlanPages(pending);
    for (const page of partitionPlan(combined, !!context.storage.targets.planCodec, 8 * 1024 * 1024)) { pages.push(compressedPlan(page,context.storage.targets.planCodec)); lengths.push(page.entries.length); }
    pending = []; pendingBytes = 0; pendingExpandedBytes = 0;
  };
  for (let start = 0; start < source.records.length || !start; start += pageSize) {
    const plan = buildStreamPagePlan({ ...cursorPolicy, artifacts: source.artifacts, diagnostics: [], records: source.records.slice(start, start + pageSize) }, mapping, context);
    sourceHashes.push(canonicalHash(plan.source.records));
    if (plan.counts.duplicates) throw new Error('IMPORT_STREAM_DUPLICATE_SOURCE');
    const sharedDiagnostics = plan.source.artifacts.filter(a => a.original?.status === 'unavailable').length;
    for (const entry of plan.entries) {
      if (identities.has(entry.identity)) throw new Error('IMPORT_STREAM_DUPLICATE_SOURCE');
      identities.add(entry.identity);
      for (const effect of entry.effects) {
        if (effect.attachment) throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
        if (!targets.claim(plan.destination, effect.targetKind, effect.targetId)) throw new Error('DUPLICATE_TARGET');
      }
    }
    if (!header) {
      const { entries: _entries, fingerprint: _fingerprint, source: _source, ...metadata } = plan;
      header = { ...cloneJson(metadata), source: { ...cursorPolicy, artifacts: cloneJson(plan.source.artifacts), diagnostics: [] } };
      header.counts.byKind = Object.assign(Object.create(null),header.counts.byKind);
    } else {
      for (const key of ['input','unique','duplicates','mapped','unmapped','rejected','effects'] as const) header.counts[key] += plan.counts[key];
      for (const [kind, count] of Object.entries(plan.counts.byKind)) header.counts.byKind[kind] = (header.counts.byKind[kind] ?? 0) + count;
      header.diagnostics.push(...plan.diagnostics.slice(sharedDiagnostics));
    }
    const measurement=context.storage.targets.planCodec ? measurePlanWorkingSet(plan) : undefined;
    if (measurement?.amplified) {
      const {bytes,expandedBytes}=measurement;
      if (pending.length && (pending.length === 2 || pendingBytes + bytes > 8 * 1024 * 1024 || pendingExpandedBytes + expandedBytes > 8 * 1024 * 1024)) flush();
      pending.push(plan); pendingBytes += bytes; pendingExpandedBytes += expandedBytes;
      if (pendingExpandedBytes >= 8 * 1024 * 1024) flush();
    } else {
      flush();
      for (const page of partitionPlan(plan, !!context.storage.targets.planCodec)) { pages.push(compressedPlan(page,context.storage.targets.planCodec)); lengths.push(page.entries.length); }
    }
  }
  flush();
  identities.clear(); targets.clear();
  return { stream: { version: 1, pages, lengths, header: header! }, sourceHashes, pageSize };
}

/** The existing chunk manifest also fences compressed page bytes exactly. */
export function storedStreamPage(body: string, id: string, codec?: D1PlanCodec) {
  const envelope=JSON.parse(body);
  if ((envelope.expandedSize ?? envelope.size) <= SNAPSHOT_BYTES) return { body: envelope.d1Plan === 3 ? body : canonicalJson(expandCompressedPlan(body,codec)), chunks: [] as { job_id:string; ordinal:number; body:string }[] };
  const chunks = [];
  for (let offset=0; offset<body.length; offset+=SNAPSHOT_BYTES) chunks.push({job_id:id,ordinal:chunks.length,body:body.slice(offset,offset+SNAPSHOT_BYTES)});
  return {body:canonicalJson({d1Plan:1,chunks:chunks.length,size:Buffer.byteLength(body),sha256:sha256(body)}),chunks};
}
