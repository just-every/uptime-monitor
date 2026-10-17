import { randomUUID } from 'node:crypto';
import { Buffer } from 'node:buffer';
import type { D1ImportContext } from './types.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { canonicalJson, sha256, requireText } from '../provenance/identity.ts';
import { originalsFor } from '../storage/originals.ts';
import { planIndex } from './erasure-index.ts';
import { expandCompressedPlan, type UnverifiedStreamPages } from './stream-plan.ts';
import { assertStreamPage } from './stream-page-integrity.ts';
import { layoutHash } from './stream-state.ts';
import { stagingAtomic, stagingChunks, StageWriter, type StageBinding } from './staging-transport.ts';
import { reserveStaging } from './staging-admission.ts';
import { stageRevisionWrites } from './staging-control.ts';
import { beginStage } from './staging-lifecycle.ts';
import { policy, policyHash } from './undo-state.ts';
import { stagingPlan } from './staging-plan.ts';
import { admitStreamIdentities } from './stream-identity-admission.ts';

/** Shared private writes; callers own exact generation cleanup and publication. */
export async function stageStreamPages(stream: UnverifiedStreamPages, fingerprint: string, context: D1ImportContext, own: (stage: StageBinding) => void, beforeSeal?: (stage: StageBinding, revision: number) => Promise<number>, assertInput?: () => void) {
  const h = stream.header;
  const originals = originalsFor({ ...h, source: { ...h.source, records: [] }, entries: [], fingerprint: fingerprint }, '');
  const operation = context.operationKey ?? fingerprint;
  requireText(operation, 'operationKey');
  const id = randomUUID(), pages = stream.pages.map((_, ordinal) => ({ stream_id: id, ordinal, job_id: ordinal === 0 ? id : randomUUID(), total: stream.lengths[ordinal] }));
  const stage: StageBinding = { id, generation: randomUUID(), destination: h.destination, namespace: h.namespace, mapping_version: h.mappingVersion, principal_id: h.principalId, fingerprint: fingerprint, operation_key: operation, total: h.counts.unique, pages: pages.length, layout_hash: layoutHash(pages), reversal_policy: policy(context) };
  const originalRows = originals.map(o => [o.key, o.name, o.sha256, o.size]);
  const reservation = reserveStaging({ ...stream, fingerprint }, originalRows, context, stage, pages);
  await admitStreamIdentities(stream, context, reservation);
  assertInput?.();
  own(stage);
    await beginStage(stage, context, !!beforeSeal, assertInput);
    let revision = await registerPages(stream, pages, stage, context);
    await stagingAtomic(context, stageRevisionWrites(stage, revision, false));
    const chunks = new StageWriter(context, stage, 'chunks', ['page_ordinal','ordinal','body','body_hash','body_size']); chunks.revision = revision;
    for (const page of pages) {
      let ordinal = 0;
      for (const body of stagingChunks(stagingPlan(stream.pages[page.ordinal], context.storage.targets.planCodec).body)) await chunks.append([page.ordinal, ordinal++, body, sha256(body), Buffer.byteLength(body)]);
    }
    await chunks.flush(); revision = chunks.revision;
    const artifacts = new StageWriter(context, stage, 'originals', ['original_key','name','sha256','size']); artifacts.revision = revision;
    for (const row of originalRows) await artifacts.append(row);
    await artifacts.flush(); revision = artifacts.revision;
    if (beforeSeal) revision = await beforeSeal(stage, revision);
    await stagingAtomic(context, stageRevisionWrites(stage, revision, true));
    return { stage, pages, revision };
}

async function registerPages(stream: UnverifiedStreamPages, pages: { ordinal: number; job_id: string; total: number }[], stage: StageBinding, context: D1ImportContext): Promise<number> {
  const index = new StageWriter(context, stage, 'index', ['page_ordinal','ordinal','source_identity','target_identity']);
  const metadata: unknown[][] = [];
  const counts = { input: 0, unique: 0, duplicates: 0, mapped: 0, unmapped: 0, rejected: 0, effects: 0, byKind: Object.create(null) as Record<string, number> };
  for (const page of pages) {
    const originalBody = stream.pages[page.ordinal], plan = expandCompressedPlan(originalBody, context.storage.targets.planCodec);
    const { body, inline } = stagingPlan(originalBody, context.storage.targets.planCodec);
    assertStreamPage(stream, plan);
    for (const key of ['input','unique','duplicates','mapped','unmapped','rejected','effects'] as const) counts[key] += plan.counts[key];
    for (const [kind, count] of Object.entries(plan.counts.byKind)) counts.byKind[kind] = (counts.byKind[kind] ?? 0) + count;
    if (plan.entries.some(entry => entry.effects.some(effect => effect.attachment)) || plan.source.artifacts.some(a => a.original?.status === 'available')) throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
    if (plan.entries.length !== page.total) throw new Error('IMPORT_STREAM_PAGE_CHANGED');
    const identities = planIndex(plan, page.job_id).map(row => [page.ordinal, row.ordinal, row.source_identity, row.target_identity]);
    for (const row of identities) {
      if (!/^[a-f0-9]{64}$/.test(String(row[2])) || row[3] !== '' && !/^[a-f0-9]{64}$/.test(String(row[3])) || Buffer.byteLength(JSON.stringify(row)) + 1 > 200) throw new Error('IMPORT_STREAM_PAGE_CHANGED');
      await index.append(row);
    }
    let chunks = 0;
    for (const _ of stagingChunks(body)) chunks++;
    const hash = sha256(body), size = Buffer.byteLength(body);
    const row = [page.ordinal, page.job_id, page.total, plan.fingerprint, canonicalJson({ d1Plan: 1, chunks, size, sha256: hash }), hash, size, chunks, identities.length, canonicalHash(identities), policyHash(page.job_id, plan.fingerprint, stage.reversal_policy), inline];
    if (Buffer.byteLength(JSON.stringify(row)) + 1 > 1024) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
    metadata.push(row);
  }
  if (canonicalHash(counts) !== canonicalHash(stream.header.counts)) throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
  await index.flush();
  const records = new StageWriter(context, stage, 'pages', ['ordinal','job_id','total','fingerprint','plan_json','body_hash','body_size','chunks','identities','index_hash','policy_hash','inline_plan']); records.revision = index.revision;
  for (const row of metadata) await records.append(row);
  await records.flush();
  return records.revision;
}
