import type { ImportPlan } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import type { UnverifiedStreamPages } from './stream-plan.ts';
import type { Sha256State } from '../provenance/resumable-sha256.ts';
import { createResumableCanonicalHash } from '../provenance/canonical-hash.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { planIndex } from './erasure-index.ts';
import { assertStreamPage } from './stream-page-integrity.ts';
import { policyHash } from './undo-state.ts';
import { stagingAtomic } from './staging-transport.ts';
import { controlUpdate, progress, stageRevisions, stagesFence, type ImportPreparationStep, type StepControl, type StepStage } from './preparation-step-control.ts';
import { locate, mergedHeader, stageHeader, stageIndex, stagePages } from './preparation-step-pages.ts';

/** Pages one verifying step reads, checks and hashes (in each of two passes). */
export const STEP_PAGES = 125;

type Counts = ImportPlan['counts'];
type Writer = ReturnType<typeof createResumableCanonicalHash>;
/** The fingerprint's progress between requests. `frame` is the merged header
 * without diagnostics: every field the document needs after its prefix. */
interface HashState { revisions: { id: string; revision: number }[]; pass: 'entries' | 'records'; page: number; first: boolean; sha: Sha256State; counts: Counts; frame: Record<string, any> }

const zero = (): Counts => ({ input: 0, unique: 0, duplicates: 0, mapped: 0, unmapped: 0, rejected: 0, effects: 0, byKind: Object.create(null) });
const documentOf = (header: Record<string, any>) => ({ ...header, entries: [], source: { ...header.source, records: [] } });

/** One verifying step: the same sorted keys, values and page order as
 * planFingerprint and cold verification, written into a hash whose state is
 * saved with the generations' revisions it read. */
export async function hashStep(context: D1ImportContext, row: StepControl): Promise<ImportPreparationStep> {
  const stages: StepStage[] = JSON.parse(row.stages), pages = stages.reduce((n, stage) => n + stage.pages, 0);
  const revisions = await stageRevisions(context, stages);
  let state: HashState, writer: Writer;
  if (!row.hash_json) {
    const header = mergedHeader(await Promise.all(stages.map(stage => stageHeader(context, stage.id))));
    if (header.version !== 1 || header.destination !== row.destination || header.namespace !== row.namespace || header.mappingVersion !== row.mapping_version || header.principalId !== row.principal_id || header.counts.unique !== row.total) throw new Error('PLAN_BINDING_MISMATCH');
    writer = createResumableCanonicalHash();
    prefix(writer, documentOf(header));
    const { diagnostics: _, ...frame } = header;
    state = { revisions, pass: 'entries', page: 0, first: true, sha: writer.checkpoint(), counts: zero(), frame };
  } else {
    state = JSON.parse(row.hash_json);
    if (canonicalHash(state.revisions) !== canonicalHash(revisions)) throw new Error('IMPORT_PREPARATION_CHANGED');
    writer = createResumableCanonicalHash(state.sha);
  }
  const headers = new Map<string, UnverifiedStreamPages['header']>();
  try {
    const end = Math.min(pages, state.page + STEP_PAGES);
    while (state.page < end) {
      const { stage, ordinal, first } = locate(stages, state.page), to = Math.min(stage.pages, end - first);
      const read = await stagePages(context, stage.id, ordinal, to);
      const index = state.pass === 'entries' ? await stageIndex(context, stage.id, ordinal, to) : [];
      if (!headers.has(stage.id) && state.pass === 'entries') headers.set(stage.id, await stageHeader(context, stage.id));
      let cursor = 0;
      for (const { page, plan } of read) {
        if (state.pass === 'entries') cursor = checkPage(page, plan, headers.get(stage.id)!, index, cursor, row.reversal_policy, state.counts);
        for (const entry of plan.entries) { if (!state.first) writer.token(','); state.first = false; writer.value(state.pass === 'entries' ? entry : entry.source); }
      }
      if (state.pass === 'entries' && cursor !== index.length) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
      state.page += to - ordinal;
    }
    let fingerprint: string | undefined;
    if (state.page === pages && state.pass === 'entries') {
      if (canonicalHash(state.counts) !== canonicalHash(state.frame.counts)) throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
      middle(writer, documentOf(state.frame)); state.pass = 'records'; state.page = 0; state.first = true;
    } else if (state.page === pages) {
      suffix(writer, documentOf(state.frame)); fingerprint = writer.digest();
    }
    const changes = fingerprint ? { phase: 'combining', fingerprint, hash_json: JSON.stringify({ revisions: state.revisions, frame: state.frame }) } : { hash_json: JSON.stringify({ ...state, sha: writer.checkpoint() }) };
    await stagingAtomic(context, [...stagesFence(revisions), ...controlUpdate(row, changes)]);
    return progress(row, 'verifying');
  } finally { writer.clear(); }
}

/** The checks cold verification makes on each page: its scoped projection of
 * the header, normalized custody only, and its exact privacy index, index and
 * policy hashes. Returns the next unread index row. */
function checkPage(page: any, plan: ImportPlan, header: UnverifiedStreamPages['header'], index: any[], cursor: number, reversal: string, counts: Counts): number {
  assertStreamPage({ header }, plan);
  if (plan.entries.some(entry => entry.effects.some(effect => effect.attachment)) || plan.source.artifacts.some(artifact => artifact.original?.status === 'available')) throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
  for (const key of ['input', 'unique', 'duplicates', 'mapped', 'unmapped', 'rejected', 'effects'] as const) counts[key] += plan.counts[key];
  for (const [kind, count] of Object.entries(plan.counts.byKind)) counts.byKind[kind] = (counts.byKind[kind] ?? 0) + count;
  const identities = planIndex(plan, page.job_id).map(item => [page.ordinal, item.ordinal, item.source_identity, item.target_identity]);
  if (identities.length !== page.identities || canonicalHash(identities) !== page.index_hash || policyHash(page.job_id, plan.fingerprint, reversal as never) !== page.policy_hash) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  for (const expected of identities) {
    const actual = index[cursor++];
    if (!actual || canonicalHash(expected) !== canonicalHash([actual.page_ordinal, actual.ordinal, actual.source_identity, actual.target_identity])) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  }
  return cursor;
}

/** `{` and every key before `entries`, then `"entries":[`. */
function prefix(writer: Writer, document: Record<string, any>): void {
  writer.token('{'); let first = true;
  for (const key of Object.keys(document).sort()) {
    if (!first) writer.token(','); first = false;
    writer.value(key); writer.token(':');
    if (key === 'entries') { writer.token('['); return; }
    writer.value(document[key]);
  }
  throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
}

/** `]`, the keys between `entries` and `source`, then `"source":{` with its keys
 * before `records`, then `"records":[`. */
function middle(writer: Writer, document: Record<string, any>): void {
  writer.token(']');
  const keys = Object.keys(document).sort();
  for (const key of keys.slice(keys.indexOf('entries') + 1)) {
    writer.token(','); writer.value(key); writer.token(':');
    if (key !== 'source') { writer.value(document[key]); continue; }
    writer.token('{'); let first = true;
    for (const sourceKey of Object.keys(document.source).sort()) {
      if (!first) writer.token(','); first = false;
      writer.value(sourceKey); writer.token(':');
      if (sourceKey === 'records') { writer.token('['); return; }
      writer.value(document.source[sourceKey]);
    }
  }
  throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
}

/** `]`, the rest of `source`, and every key after it. */
function suffix(writer: Writer, document: Record<string, any>): void {
  writer.token(']');
  const sourceKeys = Object.keys(document.source).sort();
  for (const key of sourceKeys.slice(sourceKeys.indexOf('records') + 1)) { writer.token(','); writer.value(key); writer.token(':'); writer.value(document.source[key]); }
  writer.token('}');
  const keys = Object.keys(document).sort();
  for (const key of keys.slice(keys.indexOf('source') + 1)) { writer.token(','); writer.value(key); writer.token(':'); writer.value(document[key]); }
  writer.token('}');
}
