import type { InventorySnapshot } from './inventory-snapshot.ts';
import type { ImportJob, ImportPlan, TargetRow } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import type { D1ReversalPolicy, D1UndoDisposition, D1UndoEvidence } from './undo-types.ts';
import { canonicalJson, sha256, attachmentHash, assertNoCredentials } from '../provenance/identity.ts';
import { effectIdentity } from '../storage/provenance.ts';
import { exactRows, jsonChunks } from './sets.ts';
import { guard, sql } from './database.ts';
import { erasedDisposition, redactedEvidenceMatches } from './erasure-disposition.ts';

export const EMPTY_REVERSAL_HASH = sha256('[]');
export const policyHash = (id: string, fingerprint: string, selected: D1ReversalPolicy) => sha256(canonicalJson([id, fingerprint, selected, 'reversal-policy-v1']));
export interface ReversalState { policy: D1ReversalPolicy; binding: string | null; cursor: number; hash: string; rows: any[]; dispositions: D1UndoDisposition[] }
const states = new WeakMap<ImportJob, ReversalState>();
export function policy(context: Pick<D1ImportContext, 'reversalPolicy'>): D1ReversalPolicy {
  const value = context.reversalPolicy ?? 'strict';
  if (!['strict', 'preserve-conflicts'].includes(value)) throw new Error('INVALID_REVERSAL_POLICY');
  return value;
}
export function undoEvidence(row: TargetRow | null, context: D1ImportContext): D1UndoEvidence {
  if (!row) return { revision: null, valueHash: null, attachmentHash: null, ownedHash: null, ownedValue: null };
  const ownedValue = context.storage.targets.ownedContent?.(structuredClone(row)) ?? { revision: row.revision, value: row.value, attachment: row.attachment ?? null };
  const encoded = canonicalJson(ownedValue);
  if (encoded === undefined || canonicalJson(JSON.parse(encoded)) !== encoded) throw new Error('INVALID_UNDO_OWNERSHIP');
  assertNoCredentials(ownedValue);
  return { revision: row.revision, valueHash: sha256(canonicalJson(row.value)), attachmentHash: attachmentHash(row.attachment), ownedHash: sha256(encoded), ownedValue };
}
export const reportHash = (rows: any[]) => sha256(canonicalJson(rows.map(r => ({ job_id: r.job_id, effect_id: r.effect_id, position: r.position, record_json: r.record_json }))));
export async function loadReversalState(raw: any, plan: ImportPlan, context: D1ImportContext, checkPolicy = true, snapshot?:InventorySnapshot): Promise<ReversalState> {
  const { reversal_policy: selected, reversal_policy_hash: binding, reversal_cursor: cursor, reversal_hash: hash } = raw;
  if (!['strict', 'preserve-conflicts'].includes(selected) || !Number.isSafeInteger(cursor) || cursor < 0 || typeof hash !== 'string') throw new Error('IMPORT_REVERSAL_INTEGRITY');
  if (binding === null ? selected !== 'strict' : binding !== policyHash(raw.id, raw.fingerprint, selected)) throw new Error('IMPORT_REVERSAL_INTEGRITY');
  if (checkPolicy && context.reversalPolicy !== undefined && policy(context) !== selected) throw new Error('REVERSAL_POLICY_CONFLICT');
  if (selected === 'strict') {
    if (cursor !== 0 || hash !== EMPTY_REVERSAL_HASH || snapshot?.tables.has('ik_d1_reversal_reports') && snapshot.rows('ik_d1_reversal_reports','job_id',[raw.id]).length !== 0) throw new Error('IMPORT_REVERSAL_INTEGRITY');
    return { policy: selected, binding, cursor, hash, rows: [], dispositions: [] };
  }
  const results=snapshot?.tables.has('ik_d1_reversal_reports')?snapshot.rows('ik_d1_reversal_reports','job_id',[raw.id]).sort((a,b)=>a.position-b.position):(await context.storage.database.prepare('SELECT r.*,p.reversed AS native_reversed FROM ik_d1_reversal_reports r LEFT JOIN ik_provenance p ON p.id=r.effect_id AND p.job_id=r.job_id WHERE r.job_id=? ORDER BY r.position').bind(raw.id).all()).results;
  if (results.length !== cursor || reportHash(results) !== hash) throw new Error('IMPORT_REVERSAL_INTEGRITY');
  const expected = new Map(plan.entries.flatMap(entry => entry.effects.map((effect, index) => [effectIdentity(raw.id, entry.identity, index), { destination: plan.destination, targetKind: effect.targetKind, targetId: effect.targetId, revision: sha256(canonicalJson([raw.id, entry.identity, index, 'applied'])), value: effect.value, ...(effect.attachment ? { attachment: effect.attachment } : {}) } as TargetRow] as const)));
  const dispositions = results.map((row, index) => {
    let d: D1UndoDisposition;
    try { d = JSON.parse(row.record_json); } catch { throw new Error('IMPORT_REVERSAL_INTEGRITY'); }
    if (d?.redacted === 'erased') {
      const entry = plan.entries.find(entry => entry.erased && entry.identity === d.sourceIdentity);
      if (!entry || row.position !== index || d.jobId !== raw.id || d.effectId !== row.effect_id || d.effectId !== effectIdentity(raw.id, entry.identity, 0) || row.native_reversed !== null || !['reversed','retained'].includes(d.disposition) || (d.disposition === 'reversed' ? d.reason !== 'unchanged' : !['edited','dependency'].includes(d.reason)) || canonicalJson(d) !== canonicalJson(erasedDisposition(d, entry.identity))) throw new Error('IMPORT_REVERSAL_INTEGRITY');
      return d;
    }
    const applied = expected.get(row.effect_id);
    if (!d || !applied || row.position !== index || d.jobId !== raw.id || d.effectId !== row.effect_id || d.redacted !== 'evidence' && (d.targetKind !== applied.targetKind || d.targetId !== applied.targetId) || !d.entity || typeof d.entity.table !== 'string' || typeof d.entity.recordId !== 'string' || !Array.isArray(d.protectedBy) || !['reversed','retained'].includes(d.disposition) || (d.disposition === 'reversed' ? d.reason !== 'unchanged' || row.native_reversed !== 1 : !['edited','dependency'].includes(d.reason) || row.native_reversed !== 0) || !d.observed || (d.redacted === 'evidence' ? !redactedEvidenceMatches(d) : d.redacted !== undefined || canonicalJson(d.expected) !== canonicalJson(undoEvidence(applied, context)) || d.observed.ownedHash !== null && d.observed.ownedHash !== sha256(canonicalJson(d.observed.ownedValue))) || (d.reason === 'dependency' && !d.protectedBy.length)) throw new Error('IMPORT_REVERSAL_INTEGRITY');
    return d;
  });
  if (!['undoing','undone','rolling_back','rolled_back'].includes(raw.state) && cursor !== 0) throw new Error('IMPORT_REVERSAL_INTEGRITY');
  return { policy: selected, binding, cursor, hash, rows: results, dispositions };
}
export function attachReversal(job: ImportJob, state: ReversalState): void { states.set(job, state); }
export function reversalState(job: ImportJob): ReversalState {
  const state = states.get(job);
  if (!state) throw new Error('IMPORT_REVERSAL_INTEGRITY');
  return state;
}
export function reversalGuards(id: string, state: ReversalState): D1Sql[] {
  return [guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND reversal_policy=? AND reversal_policy_hash IS ? AND reversal_cursor=? AND reversal_hash=?)', id, state.policy, state.binding, state.cursor, state.hash)),
    ...exactRows('ik_d1_reversal_reports', ['job_id','effect_id','position','record_json'], state.rows, sql('job_id=?', id), undefined, undefined, {key:'job_id',values:[id]}),
    // Every disposition still requires its exact native provenance state. One
    // JSON snapshot per byte-bounded chunk avoids a three-bind predicate for
    // every previously published effect in all later undo/report requests.
    // The report's exact rows and scoped count close the disposition set;
    // pending provenance belongs to the separately fenced full inventory.
    ...jsonChunks(state.dispositions.filter(d => d.redacted !== 'erased').map(d => [d.effectId, id, Number(d.disposition === 'reversed')])).map(chunk => guard(sql(
      "NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_provenance p WHERE p.id IS json_extract(e.value,'$[0]') AND typeof(p.id)=typeof(json_extract(e.value,'$[0]')) AND p.job_id IS json_extract(e.value,'$[1]') AND typeof(p.job_id)=typeof(json_extract(e.value,'$[1]')) AND p.reversed IS json_extract(e.value,'$[2]') AND typeof(p.reversed)=typeof(json_extract(e.value,'$[2]'))))", chunk)))];
}

/** A pre-read monotonic revision fences already validated report bytes without
 * copying their private cross-effect evidence into another transport store. */
export function snapshotReversalGuards(raw:any,state:ReversalState):D1Sql[]{
  if(!Number.isSafeInteger(raw.state_revision))throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');
  const fence=guard(sql('EXISTS(SELECT 1 FROM ik_d1_custody_states WHERE job_id=? AND revision=?)',raw.id,raw.state_revision));
  fence.expectedSnapshots=reversalGuards(raw.id,state).flatMap(row=>row.expectedSnapshots??[]);
  return [fence];
}
