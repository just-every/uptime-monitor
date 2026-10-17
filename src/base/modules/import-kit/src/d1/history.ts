import type { TargetRow } from '../types.ts';
import type { D1ImportContext, D1Sql, D1ProvenanceRow } from './types.ts';
import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import { guard, sql } from './database.ts';
import { validatedProvenance } from './integrity.ts';
import { changed, putRow, deleteRows } from './expected-state.ts';

export const provenanceColumns = 'id,job_id,ordinal,effect_ordinal,source_identity,destination,target_kind,target_id,reversed,record_json';

/** Transfer only a currently attributed exact native snapshot, explicitly reviewed
 * through expectedRevision. Original attribution remains unchanged in history. */
export async function supersedeTarget(successor: D1ProvenanceRow, before: TargetRow | undefined, context: D1ImportContext, statements: D1Sql[]): Promise<void> {
  const db = context.storage.database;
  const prior = await db.prepare('SELECT * FROM ik_provenance WHERE destination=? AND target_kind=? AND target_id=? AND reversed=0').bind(successor.destination, successor.targetKind, successor.targetId).first();
  if (!prior) {
    statements.push(guard(sql('NOT EXISTS(SELECT 1 FROM ik_provenance WHERE destination=? AND target_kind=? AND target_id=? AND reversed=0)', successor.destination, successor.targetKind, successor.targetId)));
    return;
  }
  const p = await validatedProvenance(prior, context, statements);
  if (!before || before.revision !== p.appliedRevision || sha256(canonicalJson(before.value)) !== p.valueHash || attachmentHash(before.attachment) !== p.attachmentHash) throw new Error('NATIVE_EDIT_CONFLICT');
  const owner = await db.prepare('SELECT state FROM ik_jobs WHERE id=?').bind(p.jobId).first();
  if (owner?.state !== 'completed') throw new Error('TARGET_OWNER_BUSY');
  successor.predecessorId = p.id;
  statements.push(guard(sql("EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND state='completed')", p.jobId)), changed(sql(`INSERT INTO ik_d1_history(${provenanceColumns},successor_id) SELECT ${provenanceColumns},? FROM ik_provenance WHERE id=?`, successor.id, p.id),putRow('ik_d1_history',{...prior,successor_id:successor.id})), changed(sql('DELETE FROM ik_provenance WHERE id=?', p.id),deleteRows('ik_provenance',row=>row.id===p.id)));
}

/** Undo cannot remove a historical layer underneath any newer import. Guarded
 * again in each mutation batch so a racing successor cannot be overlooked. */
export async function noSuccessors(id: string, context: D1ImportContext): Promise<D1Sql> {
  if (await context.storage.database.prepare('SELECT id FROM ik_d1_history WHERE job_id=? LIMIT 1').bind(id).first()) throw new Error('IMPORT_SUPERSEDED');
  return guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE job_id=?)', id));
}

/** `prior` may come from one read for a whole batch; undefined reads it here. */
export async function restorePredecessor(p: D1ProvenanceRow, context: D1ImportContext, before: D1Sql[], after: D1Sql[], prior?: any): Promise<void> {
  if (prior === undefined) prior = await context.storage.database.prepare('SELECT * FROM ik_d1_history WHERE successor_id=?').bind(p.id).first();
  if (!prior) {
    if (p.predecessorId !== undefined) throw new Error('IMPORT_HISTORY_MISMATCH');
    before.push(guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE successor_id=?)', p.id)));
    return;
  }
  const predecessor = await validatedProvenance(prior, context, before);
  if (p.predecessorId !== predecessor.id) throw new Error('IMPORT_HISTORY_MISMATCH');
  if (!p.before || p.before.destination !== predecessor.destination || p.before.targetKind !== predecessor.targetKind || p.before.targetId !== predecessor.targetId || p.before.revision !== predecessor.appliedRevision || sha256(canonicalJson(p.before.value)) !== predecessor.valueHash || attachmentHash(p.before.attachment) !== predecessor.attachmentHash) throw new Error('IMPORT_HISTORY_MISMATCH');
  const {successor_id,...restored}=prior;
  after.push(changed(sql(`INSERT INTO ik_provenance(${provenanceColumns}) SELECT ${provenanceColumns} FROM ik_d1_history WHERE id=? AND successor_id=?`, predecessor.id, p.id),putRow('ik_provenance',restored)), changed(sql('DELETE FROM ik_d1_history WHERE id=? AND successor_id=?', predecessor.id, p.id),deleteRows('ik_d1_history',row=>row.id===predecessor.id&&row.successor_id===p.id)));
}
