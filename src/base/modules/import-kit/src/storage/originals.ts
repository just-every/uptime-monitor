import { Buffer } from 'node:buffer';
import type { ImportPlan, PaginationCursorPolicy, RetainedOriginal, SourceArtifact, SqlDatabase } from '../types.ts';
import { attachmentHash, canonicalJson, assertNoCredentials, sha256 } from '../provenance/identity.ts';

/** Inspect JSON envelopes before retaining bytes; reader format-specific checks still apply. */
function inspectOriginal(bytes: Uint8Array, policy?: PaginationCursorPolicy): void {
  let text: string;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); } catch { return; }
  let value: unknown;
  try { value = JSON.parse(text); } catch {
    // An NDJSON original can contain credential metadata outside mapped records.
    for (const line of text.split(/\r?\n/)) {
      try { value = JSON.parse(line); } catch { continue; }
      assertNoCredentials(value, policy);
    }
    return;
  }
  assertNoCredentials(value, policy);
}
export function validateArtifactOriginal(artifact: SourceArtifact, policy?: PaginationCursorPolicy): void {
  if (!artifact.original) return;
  if (artifact.original.status === 'unavailable') {
    if (artifact.original.reason === 'unavailable-after-erasure') {
      if (!['withdraw-affected-artifacts', 'withdraw-all-job-artifacts'].includes(artifact.original.policy) || Object.keys(artifact.original).some(k => !['status', 'reason', 'policy'].includes(k))) throw new Error('INVALID_ORIGINAL');
    } else if (artifact.original.reason !== 'not-supplied' || Object.keys(artifact.original).some(k => !['status', 'reason'].includes(k))) throw new Error('INVALID_ORIGINAL');
    return;
  }
  if (artifact.original.status !== 'available' || typeof artifact.original.base64 !== 'string' || Object.keys(artifact.original).some(k => !['status', 'base64'].includes(k))) throw new Error('INVALID_ORIGINAL');
  const bytes = Buffer.from(artifact.original.base64, 'base64');
  if (bytes.toString('base64') !== artifact.original.base64 || bytes.length !== artifact.size || sha256(bytes) !== artifact.sha256) throw new Error('ORIGINAL_HASH_SIZE_MISMATCH');
  inspectOriginal(bytes, policy);
}
export function originalsFor(plan: ImportPlan, jobId: string): RetainedOriginal[] {
  const result: RetainedOriginal[] = [], seen = new Set<string>();
  for (const artifact of plan.source.artifacts) {
    validateArtifactOriginal(artifact, plan.source.paginationCursorPolicy);
    if (artifact.original?.status === 'unavailable' && artifact.original.reason === 'unavailable-after-erasure' && plan.erasure?.originalPolicy !== artifact.original.policy) throw new Error('ORIGINAL_ERASURE_POLICY_MISMATCH');
    const key = canonicalJson([artifact.name, artifact.sha256]);
    if (seen.has(key)) throw new Error('DUPLICATE_ARTIFACT');
    seen.add(key);
    result.push({ jobId, kind: 'artifact', key, name: artifact.name, sha256: artifact.sha256, size: artifact.size,
      original: artifact.original?.status === 'available' ? { status: 'retained', base64: artifact.original.base64 } : artifact.original ?? { status: 'unavailable', reason: 'not-supplied' } });
  }
  for (const entry of plan.entries) for (const [index, effect] of entry.effects.entries()) if (effect.attachment) {
    attachmentHash(effect.attachment);
    const bytes = Buffer.from(effect.attachment.base64, 'base64');
    result.push({ jobId, kind: 'attachment', key: canonicalJson([entry.identity, index]), name: canonicalJson([effect.targetKind, effect.targetId]), sha256: effect.attachment.sha256, size: bytes.length, original: { status: 'retained', base64: effect.attachment.base64 } });
  }
  return result;
}
/** Caller owns the same native transaction that inserts the job. */
export function retainImportOriginals(db: SqlDatabase, plan: ImportPlan, jobId: string): void {
  const originals = originalsFor(plan, jobId);
  db.prepare('INSERT INTO ik_original_sets(job_id,version) VALUES(?,1)').run(jobId);
  for (const original of originals) db.prepare('INSERT INTO ik_originals(job_id,kind,original_key,name,sha256,size,bytes) VALUES(?,?,?,?,?,?,?)').run(jobId, original.kind, original.key, original.name, original.sha256, original.size, original.original.status === 'retained' ? Buffer.from(original.original.base64, 'base64') : null);
}
export function readStoredOriginals(db: SqlDatabase, plan: ImportPlan, jobId: string): RetainedOriginal[] {
  const expected = originalsFor(plan, jobId);
  const version = db.prepare('SELECT version FROM ik_original_sets WHERE job_id=?').get(jobId);
  const rows = db.prepare('SELECT * FROM ik_originals WHERE job_id=? ORDER BY kind,original_key').all(jobId);
  if (!version) {
    // Pre-custody jobs really retain mapped attachment bytes in their plan, but
    // have no file/page originals. Do not reconstruct those from parsed records.
    if (rows.length || plan.source.artifacts.some(a => a.original !== undefined)) throw new Error('ORIGINAL_CUSTODY_MISSING');
    return expected;
  }
  if (version.version !== 1 || rows.length !== expected.length) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
  return expected.map(original => {
    const row = rows.find(r => r.kind === original.kind && r.original_key === original.key);
    if (!row || row.name !== original.name || row.sha256 !== original.sha256 || row.size !== original.size) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
    if (original.original.status === 'unavailable') {
      if (row.bytes !== null) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
      return original;
    }
    if (row.bytes === null) throw new Error('ORIGINAL_CUSTODY_MISSING');
    const bytes = Buffer.from(row.bytes);
    if (bytes.length !== row.size || sha256(bytes) !== row.sha256 || bytes.toString('base64') !== original.original.base64) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
    return { ...original, original: { status: 'retained' as const, base64: bytes.toString('base64') } };
  });
}
