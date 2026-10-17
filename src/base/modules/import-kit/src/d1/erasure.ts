import { erasedCustody } from './erased-custody.ts';
import { targetGuards } from './target-guards.ts';
import type { D1FieldMatch, D1ImportContext, D1RelatedErasure, D1Sql } from './types.ts';
import { requestContext, packGuards, remaining, D1RequestLimitError } from './budget.ts';
import { readAtomic, authority, finalAuthority, readonlyChecks, guard, sql, prepare } from './database.ts';
import { decode } from './jobs.ts';
import { retainPlan } from './plans.ts';
import { inventoryGuards, custodyGuards } from './integrity.ts';
import { readOriginals } from './originals.ts';
import { exactRows, jsonChunks, selectedCounts, selectKeys } from './sets.ts';
import { canonicalJson, sha256, requireText } from '../provenance/identity.ts';
import { erasePlanRecords, type RelatedPlanErasure } from './erased-plan.ts';
import { fieldSelectionSnapshot, relatedSelection, type JobSelection } from './erasure-related.ts';
import { indexedDestinationGuard, planIndex, planIndexRows, targetIdentity, unindexedJobs } from './erasure-index.ts';
import { readInventorySnapshot } from './inventory-snapshot.ts';
import { erasureWrites, type ErasedDecision, type ErasedJob } from './erasure-writes.ts';
import { stagedSourceIdentities, stageErasure } from './staging-erasure.ts';
import { attachReversal, loadReversalState, reversalGuards, policyHash } from './undo-state.ts';
import { eraseReversalEvidence } from './erased-reversal.ts';
import type { ImportPlan } from '../types.ts';
import { changed, deleteRows } from './expected-state.ts';
import { eraseBounded } from './erasure-bounded.ts';
import { originalVersions } from './original-storage.ts';
import { fieldRedactions, inRange, redactPlanFields, unmatchedPath } from './field-redaction.ts';
import { redactedCustody, redactedNativeFields } from './redacted-custody.ts';

/** Erase one normalized native entity and every retained copy of its source.
 * nativeStatements execute in the same D1 batch as all kit custody changes.
 * Current erase authority is destination-scoped, independently of job ownership.
 * `related` also erases other retained records and originals the product
 * recognizes as holding the same person's data; their effects are severed, so
 * undo leaves those native records alone, and the product's nativeStatements
 * must redact them. `related.fields` instead sanitizes peer values while
 * preserving their effects, provenance and reversible before-images. */
export async function eraseD1ImportTarget(kind: string, id: string, nativeStatements: D1Sql[], context: D1ImportContext, related?: D1RelatedErasure): Promise<{ nativeResults: unknown[]; erasedRecords: number; fieldMatches: D1FieldMatch[] }> {
  context = requestContext(context);
  if (related) related = { ...related, fields: fieldRedactions(related.fields) };
  if (context.originalErasurePolicy !== undefined && !['withdraw-affected-artifacts', 'withdraw-all-job-artifacts'].includes(context.originalErasurePolicy)) throw new Error('INVALID_ORIGINAL_ERASURE_POLICY');
  requireText(kind, 'target kind'); requireText(id, 'target id');
  await readAtomic(context, [], undefined, 'erase');
  if(context.maxBatchBytes!==undefined){if(related)throw new Error('IMPORT_RELATED_ERASURE_UNBOUNDED_ONLY');return { ...await eraseBounded(kind,id,nativeStatements,context), fieldMatches: [] };}
  const { before, after, final, decisions, fieldMatches } = await targetErasure({ kind, id }, context, related);
  const db = context.storage.database;
  const prefix = packGuards([authority(context, 'erase'), ...before]);
  // Assertion cleanup can itself revoke authority through native SQL triggers.
  // The last statement must abort the same batch without creating another row.
  const suffix = [...packGuards([...after, authority(context, 'erase'), sql('DELETE FROM ik_d1_assertions')]), ...readonlyChecks(final), finalAuthority(context, 'erase')];
  if (prefix.length + nativeStatements.length + suffix.length > remaining(context)) throw new D1RequestLimitError();
  const results = await db.batch([...prefix, ...nativeStatements, ...suffix].map(statement => prepare(db, statement)));
  return { nativeResults: results.slice(prefix.length, prefix.length + nativeStatements.length), erasedRecords: decisions.length, fieldMatches };
}

/** Restricts one bounded step of a resumable redaction: only `selection` jobs are
 * read, and with `owned` only the native rows their active provenance owns are
 * rewritten. `matched` carries match counts from earlier steps. */
export interface ErasureStep { selection: JobSelection; owned?: boolean; matched?: D1FieldMatch[] }

/** The unbounded erasure of one target (or, with no target, only of the related
 * fields) as guards before the native statements and writes after them. Jobs in
 * `retiring` are left alone: the caller deletes their whole custody in the same
 * batch, so their plans are not rewritten. */
export async function targetErasure(target: { kind: string; id: string } | undefined, context: D1ImportContext, related?: D1RelatedErasure, retiring: ReadonlySet<string> = new Set(), step?: ErasureStep): Promise<{ before: D1Sql[]; after: D1Sql[]; final: D1Sql[]; decisions: ErasedDecision[]; jobs: string[]; fieldMatches: D1FieldMatch[] }> {
  const { database: db, targets } = context.storage;
  const kind = target?.kind ?? '', id = target?.id ?? '';
  if (await db.prepare(`${unindexedJobs} LIMIT 1`).bind(context.destination).first()) throw new Error('IMPORT_ERASURE_INDEX_UPGRADE_REQUIRED');
  const digest = target ? targetIdentity(context.destination, kind, id) : undefined;
  const targetRows = digest ? await selectKeys(db, 'ik_d1_plan_index', 'target_identity', [digest]) : [];
  const scan = related ? await relatedSelection(context, related, step?.selection) : undefined;
  const identities = new Set<string>([...targetRows.map(row => row.source_identity), ...digest ? await stagedSourceIdentities(digest, context) : [], ...scan?.identities ?? []]);
  const severed: RelatedPlanErasure = { identities: new Set([...scan?.identities ?? []].filter(identity => !targetRows.some(row => row.source_identity === identity))), target: digest ?? '' };
  const privateErasure = digest ? stageErasure(context.destination, digest, [...identities]) : { before: [], after: [] };
  const sourceRows = await selectKeys(db, 'ik_d1_plan_index', 'source_identity', [...identities]);
  const ids = [...new Set([...sourceRows.map(row => row.job_id), ...scan?.originals.keys() ?? [], ...scan?.redactedJobs ?? []])];
  const snapshot = await readInventorySnapshot(ids, db, targets.planCodec);
  const before: D1Sql[] = [indexedDestinationGuard(context.destination), ...privateErasure.before, ...scan?.guards ?? [],
    ...digest ? selectedCounts('ik_d1_plan_index', 'target_identity', [digest], targetRows) : [],
    ...selectedCounts('ik_d1_plan_index', 'source_identity', [...identities], sourceRows),
    ...exactRows('ik_d1_plan_index', ['job_id','ordinal','source_identity','target_identity'], sourceRows)];
  const after: D1Sql[] = [], final: D1Sql[] = [], changedJobs: ErasedJob[] = [], decisions: ErasedDecision[] = [], redactions: ErasedDecision[] = [];
  const fields = related?.fields ?? [];
  if (fields.length || related?.snapshot !== undefined) {
    const range = step?.selection.range;
    const owned = step?.owned ? new Set(snapshot.rows('ik_provenance', 'job_id', ids).filter(row => !row.reversed && inRange(range, row.ordinal)).map(row => `${row.target_kind}\u0000${row.target_id}`)) : undefined;
    const native = await redactedNativeFields(kind, id, context, fields, scan!.fieldMatches, owned && (row => owned.has(`${row.targetKind}\u0000${row.targetId}`)));
    if (related?.snapshot !== undefined && related.snapshot !== fieldSelectionSnapshot([...scan!.guards, ...native.before])) throw new Error('IMPORT_REDACTION_SNAPSHOT_CHANGED');
    for (const earlier of step?.matched ?? []) { const count = scan!.fieldMatches.find(item => canonicalJson(item.path) === canonicalJson(earlier.path)); if (count) count.matches += earlier.matches; }
    if (!step?.owned && unmatchedPath(fields, scan!.fieldMatches)) throw new Error('IMPORT_REDACTION_PATH_UNMATCHED');
    before.push(...native.before); after.push(...native.after); final.push(...native.final);
  }
  const changedPlans = new Map<string, ImportPlan>();
  const kept = ids.filter(job => !retiring.has(job));
  for (const raw of snapshot.rows('ik_jobs', 'id', kept)) {
    const plan = snapshot.plans.get(raw.id)!;
    const originals = scan?.originals.get(raw.id);
    if (plan.destination !== context.destination || (!originals && !scan?.redactedJobs.has(raw.id) && !plan.entries.some(entry => identities.has(entry.identity)))) throw new Error('IMPORT_ERASURE_INDEX_MISMATCH');
    if (plan.entries.some(entry => identities.has(entry.identity) && !severed.identities.has(entry.identity) && entry.effects.some(effect => effect.targetKind !== kind || effect.targetId !== id))) throw new Error('IMPORT_ERASURE_SHARED_SOURCE');
    const job = decode(raw);
    const reversal = await loadReversalState(raw, plan, context, false);
    attachReversal(job, reversal); before.push(...reversalGuards(job.id, reversal));
    await readOriginals(plan, job.id, context, snapshot);
    before.push(...custodyGuards(plan, job.id), ...await inventoryGuards(job, plan, context, snapshot));
    let next = erasePlanRecords(plan, identities, context.originalErasurePolicy, { ...severed, originals });
    if (fields.length) next = redactPlanFields(next, fields, context.originalErasurePolicy, step?.selection.range).plan;
    const retained = retainPlan(next, job.id, next.source.artifacts.some(a => a.original?.status === 'available') ? undefined : targets.planCodec);
    originalVersions.set(next,originalVersions.get(plan)!);
    changedPlans.set(job.id, next);
    after.push(...erasedCustody(plan, next, job.id, snapshot), ...retained.statements);
    if (fields.length) after.push(...redactedCustody(plan, next, job.id, snapshot, fields, step?.selection.range));
    // A severed record's index row moves from its native target (or none) to the erased target.
    if (canonicalJson(planIndex(next, job.id)) !== canonicalJson(planIndex(plan, job.id))) after.push(sql('DELETE FROM ik_d1_plan_index WHERE job_id=?', job.id), ...planIndexRows(next, job.id));
    after.push(guard(sql('(SELECT count(*) FROM ik_d1_creation_plans WHERE job_id=? AND released=0)=? AND (SELECT count(*) FROM ik_d1_plan_chunks WHERE job_id=?)=?', job.id, retained.encoded.chunks.length, job.id, retained.encoded.chunks.length)));
    changedJobs.push({ id: job.id, body: retained.body, fingerprint: next.fingerprint, erasedRecords: next.entries.filter(entry => entry.erased).length, policyHash: reversal.binding === null ? null : policyHash(job.id, next.fingerprint, reversal.policy) });
    // Changing the fingerprint fences stale apply/undo evidence while preserving
    // the apply journal's sequential fences for a partially imported file.
    for (const [ordinal, entry] of next.entries.entries()) {
      if (!entry.erased && (entry.source.payloadHash !== plan.entries[ordinal].source.payloadHash || canonicalJson(entry.effects) !== canonicalJson(plan.entries[ordinal].effects))) {
        redactions.push({ job_id: job.id, ordinal, identity: entry.identity, source_hash: entry.source.payloadHash, effects_hash: sha256(canonicalJson(entry.effects)) });
      }
      if (!identities.has(entry.identity)) continue;
      decisions.push({ job_id: job.id, ordinal, identity: entry.identity, source_hash: entry.source.payloadHash, effects_hash: sha256(canonicalJson(entry.effects)) });
    }
  }
  const releaseCopies = jsonChunks(kept).flatMap(chunk => [sql('UPDATE ik_d1_creation_plans SET released=1 WHERE job_id IN (SELECT value FROM json_each(?))', chunk), sql('DELETE FROM ik_d1_creation_plans WHERE job_id IN (SELECT value FROM json_each(?))', chunk)]);
  const removeChunks = [...releaseCopies, ...jsonChunks(kept).map(chunk => {const selected=new Set(JSON.parse(chunk));return changed(sql('DELETE FROM ik_d1_plan_chunks WHERE job_id IN (SELECT value FROM json_each(?))', chunk),deleteRows('ik_d1_plan_chunks',row=>selected.has(row.job_id)));})];
  after.unshift(...removeChunks);
  const reports = await eraseReversalEvidence(context, changedPlans);
  before.push(...reports.before);
  after.push(...releaseCopies, ...erasureWrites(changedJobs, decisions, redactions), ...reports.after, ...privateErasure.after);
  if (fields.length || related?.snapshot !== undefined) for (const table of ['ik_d1_staging', 'ik_d1_native_staging', 'ik_d1_preparation_steps']) {
    after.push(sql(`DELETE FROM ${table} WHERE destination=?`, context.destination), guard(sql(`NOT EXISTS(SELECT 1 FROM ${table} WHERE destination=?)`, context.destination)));
  }
  for (const [id, next] of changedPlans) after.push(...custodyGuards(next, id));
  for (const chunk of jsonChunks([...identities])) after.push(sql('INSERT OR IGNORE INTO ik_d1_erased_sources(destination,identity) SELECT ?,value FROM json_each(?)', context.destination, chunk));
  if (digest) {
    after.push(sql('INSERT OR IGNORE INTO ik_d1_erased_targets(destination,identity) VALUES(?,?)', context.destination, digest));
    const current = await targets.read(db, context.destination, kind, id);
    before.push(...targetGuards(targets,current ?? null, context.destination, kind, id));
    after.push(...targetGuards(targets,null, context.destination, kind, id));
  }
  return { before, after, final, decisions, jobs: ids, fieldMatches: scan?.fieldMatches ?? [] };
}
