import type { InventorySnapshot } from './inventory-snapshot.ts';
import { planFingerprint } from '../plan/index.ts';
import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import { decodeProvenance, effectIdentity } from '../storage/provenance.ts';
import type { D1ImportContext, D1Sql, D1ProvenanceRow } from './types.ts';
import { guard, sql } from './database.ts';
import { selectKeys, exactRows, jobColumns, claimColumns, provenanceColumns, jsonChunks } from './sets.ts';
import { loadPlans, planGuards } from './plans.ts';
import { loadReversalState, reversalGuards,snapshotReversalGuards } from './undo-state.ts';
import { readOriginals } from './originals.ts';
import { custodyGuards } from './original-storage.ts';

/** One preflight closure, no process-wide cache. Every row used here is guarded
 * with its complete stored fields before publication. */
export async function provenanceSet(raws: any[], context: D1ImportContext, guards: D1Sql[], extraOwners: string[] = [], extraClaims: string[] = [], snapshot?: InventorySnapshot) {
  const db = context.storage.database;
  const read = (table: string, key: string, values: string[]) => snapshot ? Promise.resolve(snapshot.rows(table, key, values)) : selectKeys(db, table, key, values);
  // Close preserved successors' history in both directions. Strict jobs retain
  // their existing export contract; strict undo still uses restorePredecessor.
  const initialOwners = await read('ik_jobs', 'id', [...extraOwners, ...raws.map(r => r.job_id)]);
  const preservedOwners = new Set(initialOwners.filter(r => r.reversal_policy === 'preserve-conflicts').map(r => r.id));
  const originalLength = raws.length, all = [...raws], known = new Map(raws.map(r => [r.id, r]));
  let level = raws.filter(r => preservedOwners.has(r.job_id));
  while (level.length) {
    const ids = level.filter(r => !r.reversed).map(r => (decodeProvenance(r) as D1ProvenanceRow).predecessorId).filter((id): id is string => id !== undefined && !known.has(id));
    const predecessors = await read('ik_d1_history', 'id', ids), next: any[] = [];
    for (const prior of predecessors) { if (!known.has(prior.id)) { known.set(prior.id, prior); all.push(prior); next.push(prior); } }
    for (const raw of level) {
      const p = decodeProvenance(raw) as D1ProvenanceRow;
      if (p.predecessorId === undefined || p.reversed) continue;
      const prior = known.get(p.predecessorId);
      if (p.predecessorId === p.id || !prior || prior.successor_id !== p.id || prior.reversed !== 0) throw new Error('IMPORT_HISTORY_MISMATCH');
    }
    level = next;
  }
  raws = all;
  const owners = [...initialOwners, ...await read('ik_jobs', 'id', raws.map(r => r.job_id).filter(id => !initialOwners.some(o => o.id === id)))];
  const ownerMap = new Map(owners.map(r => [r.id, r])), plans = snapshot ? new Map(owners.map(owner => [owner.id, snapshot.plans.get(owner.id)!])) : await loadPlans(owners, db, context.storage.targets.planCodec);
  for (const owner of owners) {
    const plan = plans.get(owner.id)!;
    if (planFingerprint(plan) !== owner.fingerprint || plan.fingerprint !== owner.fingerprint || plan.version !== 1 || plan.destination !== owner.destination || plan.namespace !== owner.namespace || plan.mappingVersion !== owner.mapping_version || plan.principalId !== owner.principal_id || owner.total !== plan.entries.length || !Number.isSafeInteger(owner.checkpoint) || owner.checkpoint < 0 || owner.checkpoint > owner.total) throw new Error('PROVENANCE_JOB_MISMATCH');
    const reversal=await loadReversalState(owner,plan,context,false,snapshot);
    guards.push(...planGuards(plan,owner.id),...(snapshot?.tables.has('ik_d1_reversal_reports')?snapshotReversalGuards(owner,reversal):reversalGuards(owner.id,reversal)));
    if(context.maxBatchBytes!==undefined){await readOriginals(plan,owner.id,context,snapshot);guards.push(...custodyGuards(plan,owner.id));}
  }
  const claims = await read('ik_sources', 'identity', [...extraClaims, ...raws.filter(r => !r.reversed).map(r => r.source_identity)]);
  const claimMap = new Map(claims.map(r => [r.identity, r]));
  const successorIds = raws.filter(r => r.successor_id !== undefined).map(r => r.successor_id);
  const activeSuccessors = await read('ik_provenance', 'id', successorIds);
  const historicalSuccessors = await read('ik_d1_history', 'id', successorIds);
  guards.push(...exactRows('ik_jobs', jobColumns, owners), ...exactRows('ik_sources', claimColumns, claims));
  // Each successor must exist in exactly one store, including at the boundary.
  for (const chunk of jsonChunks([...new Set(successorIds)])) guards.push(guard(sql('NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE (SELECT COUNT(*) FROM ik_provenance WHERE id=e.value)+(SELECT COUNT(*) FROM ik_d1_history WHERE id=e.value)<>1)', chunk)));
  guards.push(...exactRows('ik_provenance', provenanceColumns, activeSuccessors), ...exactRows('ik_d1_history', [...provenanceColumns,'successor_id'], historicalSuccessors));
  guards.push(...exactRows('ik_provenance', provenanceColumns, raws.filter(r => r.successor_id === undefined)), ...exactRows('ik_d1_history', [...provenanceColumns,'successor_id'], raws.filter(r => r.successor_id !== undefined)));
  const successors = new Map<string, any[]>();
  for (const row of [...activeSuccessors, ...historicalSuccessors]) successors.set(row.id, [...(successors.get(row.id) ?? []), row]);
  const decoded = raws.map(raw => {
    const p = decodeProvenance(raw), owner = ownerMap.get(p.jobId), plan = plans.get(p.jobId);
    if (!owner || !plan) throw new Error('PROVENANCE_JOB_MISSING');
    const entry = plan.entries[raw.ordinal], effect = entry?.effects[raw.effect_ordinal];
    if (!Number.isSafeInteger(raw.ordinal) || !Number.isSafeInteger(raw.effect_ordinal) || !entry || !effect || raw.ordinal >= owner.checkpoint || p.id !== effectIdentity(p.jobId, entry.identity, raw.effect_ordinal) || p.sourceIdentity !== entry.identity || p.sourceKind !== entry.source.kind || p.sourceId !== entry.source.sourceId || p.sourceHash !== entry.source.payloadHash || p.artifactHash !== entry.source.artifactHash || canonicalJson(p.location) !== canonicalJson(entry.source.location) || p.destination !== plan.destination || p.targetKind !== effect.targetKind || p.targetId !== effect.targetId || p.valueHash !== sha256(canonicalJson(effect.value)) || p.attachmentHash !== attachmentHash(effect.attachment) || p.appliedRevision !== sha256(canonicalJson([p.jobId, entry.identity, raw.effect_ordinal, 'applied']))) throw new Error('PROVENANCE_ATTRIBUTION_MISMATCH');
    if (p.before === undefined || (p.before ? p.before.destination !== p.destination || p.before.targetKind !== p.targetKind || p.before.targetId !== p.targetId || p.before.revision !== effect.expectedRevision : effect.expectedRevision !== undefined)) throw new Error('PROVENANCE_BEFORE_IMAGE_MISMATCH');
    if (!p.reversed) {
      const claim = claimMap.get(p.sourceIdentity);
      if (!claim || claim.job_id !== p.jobId || claim.source_hash !== entry.source.payloadHash || claim.mapping_version !== plan.mappingVersion || claim.effects_hash !== sha256(canonicalJson(entry.effects))) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
    }
    if (raw.successor_id !== undefined) {
      const found = successors.get(raw.successor_id) ?? [], next = found.length === 1 ? decodeProvenance(found[0]) as D1ProvenanceRow : undefined;
      if (p.reversed || typeof raw.successor_id !== 'string' || !next || found[0].reversed !== 0 || next.reversed || next.id === p.id || next.predecessorId !== p.id || !next.before || next.destination !== p.destination || next.targetKind !== p.targetKind || next.targetId !== p.targetId || next.before.revision !== p.appliedRevision || sha256(canonicalJson(next.before.value)) !== p.valueHash || attachmentHash(next.before.attachment) !== p.attachmentHash) throw new Error('IMPORT_HISTORY_MISMATCH');
    }
    return p;
  });
  return { decoded: decoded.slice(0, originalLength), claims: claimMap, owners: ownerMap, plans };
}
