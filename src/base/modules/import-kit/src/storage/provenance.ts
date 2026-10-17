import type { ImportPlan, ProvenanceRow, Reconciliation, SqlDatabase } from '../types.ts';
import { canonicalJson, sha256, attachmentHash, sourceIdentity, targetIdentity } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';

export type Integrity = Omit<Reconciliation, 'ok'>;
export const emptyIntegrity = (): Integrity => ({ missing: [], duplicate: [], extra: [], mismatched: [] });
export const effectIdentity = (job: string, identity: string, index: number): string => sha256(canonicalJson([job, identity, index]));
export interface DurableProvenance { sql: any; record?: ProvenanceRow }
export function decodeProvenance(row: any): ProvenanceRow {
  const p = JSON.parse(row.record_json) as ProvenanceRow;
  if (!p || typeof p !== 'object' || p.id !== row.id || p.jobId !== row.job_id || p.sourceIdentity !== row.source_identity || p.destination !== row.destination || p.targetKind !== row.target_kind || p.targetId !== row.target_id || typeof p.reversed !== 'boolean' || Number(p.reversed) !== row.reversed) throw new Error('PROVENANCE_STORAGE_MISMATCH');
  return p;
}
export interface OwnerPlan { job: any; plan: ImportPlan; effectsHashes: Map<number, string> }
/** Cache is confined to one immutable transaction snapshot. */
export function ownerPlan(db: SqlDatabase, id: string, cache: Map<string, OwnerPlan>): OwnerPlan {
  const prior = cache.get(id); if (prior) return prior;
  const job = db.prepare('SELECT * FROM ik_jobs WHERE id=?').get(id);
  if (!job) throw new Error('PROVENANCE_JOB_MISSING');
  const plan = JSON.parse(job.plan_json) as ImportPlan;
  if (planFingerprint(plan) !== job.fingerprint || plan.fingerprint !== job.fingerprint || plan.destination !== job.destination || plan.namespace !== job.namespace || plan.mappingVersion !== job.mapping_version || plan.principalId !== job.principal_id || plan.version !== 1 || job.total !== plan.entries.length || !Number.isSafeInteger(job.checkpoint) || job.checkpoint < 0 || job.checkpoint > job.total) throw new Error('PROVENANCE_JOB_MISMATCH');
  const result = { job, plan, effectsHashes: new Map<number, string>() }; cache.set(id, result); return result;
}
/** Source-only entries have the same publishing authority as entries with effects. */
export function validateSourceClaim(db: SqlDatabase, claim: any, cache = new Map<string, OwnerPlan>()): void {
  const { job, plan } = ownerPlan(db, claim.job_id, cache);
  const ordinal = plan.entries.findIndex(entry => entry.identity === claim.identity), entry = plan.entries[ordinal];
  if (!entry || ordinal >= job.checkpoint || ['undone', 'rolled_back'].includes(job.state) || entry.identity !== sourceIdentity(job.destination, job.namespace, entry.source.kind, entry.source.sourceId) || entry.source.namespace !== job.namespace || sha256(canonicalJson(entry.source.payload)) !== entry.source.payloadHash || claim.source_hash !== entry.source.payloadHash || claim.mapping_version !== plan.mappingVersion || claim.effects_hash !== sha256(canonicalJson(entry.effects))) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
}
/** Verify a physical provenance record against the plan that actually published it. */
export function validateProvenance(db: SqlDatabase, row: any, cache = new Map<string, OwnerPlan>()): ProvenanceRow {
  const p = decodeProvenance(row);
  const { job, plan, effectsHashes } = ownerPlan(db, row.job_id, cache);
  const entry = plan.entries[row.ordinal], effect = entry?.effects[row.effect_ordinal];
  if (!Number.isSafeInteger(row.ordinal) || !Number.isSafeInteger(row.effect_ordinal) || !entry || !effect || row.ordinal >= job.checkpoint) throw new Error('PROVENANCE_POSITION_MISMATCH');
  const source = entry.source;
  if (entry.identity !== sourceIdentity(job.destination, job.namespace, source.kind, source.sourceId) || source.namespace !== job.namespace || sha256(canonicalJson(source.payload)) !== source.payloadHash) throw new Error('PROVENANCE_SOURCE_MISMATCH');
  const claim = db.prepare('SELECT * FROM ik_sources WHERE identity=?').get(entry.identity);
  if (!effectsHashes.has(row.ordinal)) effectsHashes.set(row.ordinal, sha256(canonicalJson(entry.effects)));
  if (!p.reversed && (!claim || claim.job_id !== job.id || claim.source_hash !== source.payloadHash || claim.mapping_version !== job.mapping_version || claim.effects_hash !== effectsHashes.get(row.ordinal))) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
  if (p.id !== effectIdentity(job.id, entry.identity, row.effect_ordinal) || p.sourceIdentity !== entry.identity || p.sourceKind !== source.kind || p.sourceId !== source.sourceId || p.sourceHash !== source.payloadHash || p.artifactHash !== source.artifactHash || canonicalJson(p.location) !== canonicalJson(source.location) || p.destination !== job.destination || p.targetKind !== effect.targetKind || p.targetId !== effect.targetId || p.valueHash !== sha256(canonicalJson(effect.value)) || p.attachmentHash !== attachmentHash(effect.attachment) || p.appliedRevision !== sha256(canonicalJson([job.id, entry.identity, row.effect_ordinal, 'applied']))) throw new Error('PROVENANCE_ATTRIBUTION_MISMATCH');
  if (p.before === undefined || (p.before !== null && typeof p.before !== 'object')) throw new Error('PROVENANCE_BEFORE_IMAGE_MISMATCH');
  if (p.before ? p.before.destination !== p.destination || p.before.targetKind !== p.targetKind || p.before.targetId !== p.targetId || p.before.revision !== effect.expectedRevision : effect.expectedRevision !== undefined) throw new Error('PROVENANCE_BEFORE_IMAGE_MISMATCH');
  if (!plan.source.artifacts.some(a => a.name === source.location.artifact && a.sha256 === source.artifactHash)) throw new Error('PROVENANCE_ARTIFACT_MISMATCH');
  return p;
}
export function inspectDurableProvenance(db: SqlDatabase, destination: string): { entries: DurableProvenance[]; integrity: Integrity } {
  const integrity = emptyIntegrity(), plans = new Map<string, OwnerPlan>();
  // Include rows whose SQL destination was corrupted away from their owning job.
  const entries = db.prepare('SELECT * FROM ik_provenance WHERE reversed=0 AND (destination=? OR job_id IN (SELECT id FROM ik_jobs WHERE destination=?)) ORDER BY id').all(destination, destination).map(sql => {
    let record: ProvenanceRow | undefined;
    try { const parsed = JSON.parse(sql.record_json); if (parsed && typeof parsed === 'object' && ['id', 'jobId', 'destination', 'targetKind', 'targetId'].every(key => typeof parsed[key] === 'string') && typeof parsed.reversed === 'boolean') record = parsed; } catch { /* reported below */ }
    try { validateProvenance(db, sql, plans); } catch { integrity.mismatched.push('provenance-storage:' + sql.id); }
    return { sql, record };
  });
  const ids = new Map<string, string>();
  for (const { sql, record } of entries) {
    if (!record || typeof record.id !== 'string') continue;
    const key = targetIdentity(sql.destination, sql.target_kind, sql.target_id), prior = ids.get(record.id);
    if (prior !== undefined) integrity.duplicate.push('provenance:' + key);
    ids.set(record.id, key);
  }
  return { entries, integrity };
}
