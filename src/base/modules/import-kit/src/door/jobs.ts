import { createD1ImportJob, readD1ImportJob } from '../d1/jobs.ts';
import { applyD1ImportBatches } from '../d1/apply.ts';
import { rollbackD1Import, undoD1Import } from '../d1/reverse.ts';
import type { DoorPlanRow, DoorRequest } from './types.ts';
import { kitContext, reserveApply } from './authority.ts';
import { inspectRecords } from './inventory.ts';
import { DoorError } from './grants.ts';
import { assertion, authorized, lineage, loadSnapshot, planRow, sql, transact } from './store.ts';
import { text } from './uploads.ts';

/** Creation and indexing may be separate commits; the immutable operation key
 * makes a queued job recoverable even if its response or index update was lost. */
async function associated(request: DoorRequest, row: DoorPlanRow): Promise<DoorPlanRow> {
  if (row.job_id) return row;
  const job = await request.options.database.prepare('SELECT id,fingerprint FROM ik_jobs WHERE destination=? AND principal_id=? AND operation_key=?')
    .bind(request.options.destination,request.options.ownerPrincipalId,'door:'+row.id).first();
  if (!job) return row;
  if (job.fingerprint !== row.fingerprint) throw new DoorError('job_binding_mismatch',409);
  await transact(request.options.database,request.authority,[sql('UPDATE ik_door_plans SET job_id=? WHERE id=? AND job_id IS NULL',job.id,row.id)]);
  return {...row,job_id:job.id};
}
export async function apply(request: DoorRequest, input: Record<string,unknown>) {
  if (Object.keys(input).some(k=>!['planId','reviewedFingerprint'].includes(k))) throw new DoorError('invalid_apply_input',400);
  const row = await associated(request,await planRow(request,text(input.planId,'plan_id')));
  if (input.reviewedFingerprint !== row.fingerprint || request.grant.fingerprint !== row.fingerprint) throw new DoorError('review_fingerprint_mismatch');
  const snapshot = await loadSnapshot(request,row), context = kitContext(request,snapshot);
  const current = await lineage(request);
  if (current.head_id === row.id && row.job_id) {
    const approval = await request.options.database.prepare('SELECT approval_id,approved_at FROM ik_door_approvals WHERE plan_id=?').bind(row.id).first();
    if (approval?.approval_id !== request.grant.approvalRecordId || approval?.approved_at !== request.grant.approvedAt) throw new DoorError('approval_mismatch');
    return {planId:row.id,job:await readD1ImportJob(row.job_id,context)};
  }
  await reserveApply(request,snapshot);
  const job = await createD1ImportJob(snapshot.plan,context);
  await transact(request.options.database,request.authority,[
    sql('UPDATE ik_door_plans SET job_id=? WHERE id=? AND (job_id IS NULL OR job_id=?)',job.id,row.id,job.id),
    assertion(sql('EXISTS(SELECT 1 FROM ik_door_plans WHERE id=? AND job_id=?)',row.id,job.id)),
  ]);
  const result = await applyD1ImportBatches(job.id,context);
  await lineage(request);
  return {planId:row.id,job:result};
}
export async function status(request: DoorRequest, input: Record<string,unknown>) {
  const row = await associated(request,await planRow(request,text(input.planId,'plan_id'))), snapshot = await loadSnapshot(request,row);
  const job = row.job_id ? await readD1ImportJob(row.job_id,kitContext(request,snapshot)) : null;
  await lineage(request);
  const approval = await request.options.database.prepare('SELECT owner,approved_at,approval_id,reviewer FROM ik_door_approvals WHERE plan_id=?').bind(row.id).first();
  return {planId:row.id,fingerprint:row.fingerprint,job,approval,
    caption:approval?`approved via Business by ${approval.owner} at ${approval.approved_at}, fingerprint ${row.fingerprint}`:null};
}
export async function reconcile(request: DoorRequest, input: Record<string,unknown>) {
  const row = await associated(request,await planRow(request,text(input.planId,'plan_id'))), snapshot = await loadSnapshot(request,row);
  if (!row.job_id || (await readD1ImportJob(row.job_id,kitContext(request,snapshot))).state!=='completed') throw new DoorError('import_not_completed',409);
  const current = await lineage(request);
  if (current.head_id!==row.id) throw new DoorError('import_superseded',409);
  const report = await inspectRecords(request,snapshot.records);
  await authorized(request);
  return {planId:row.id,fingerprint:row.fingerprint,bundleSha256:snapshot.bundleSha256,...report};
}
export async function undo(request: DoorRequest, input: Record<string,unknown>) {
  const id = text(input.planId,'plan_id');
  const candidate = await request.options.database.prepare('SELECT * FROM ik_door_plans WHERE id=? AND installation_id=? AND namespace=?').bind(id,request.grant.installationId,request.grant.namespace).first<DoorPlanRow>();
  if (!candidate) throw new DoorError('plan_not_found',404);
  const row = await associated(request,candidate);
  if (!row.job_id) {
    // Cancel an interrupted private preparation, which has no kit effects.
    await transact(request.options.database,request.authority,[
      assertion(sql('NOT EXISTS(SELECT 1 FROM ik_jobs WHERE destination=? AND principal_id=? AND operation_key=?)',request.options.destination,request.options.ownerPrincipalId,'door:'+id)),
      sql('UPDATE ik_door_namespaces SET pending_id=NULL WHERE installation_id=? AND namespace=? AND pending_id=?',request.grant.installationId,request.grant.namespace,id),
      sql('DELETE FROM ik_door_plan_chunks WHERE plan_id=?',id),sql('DELETE FROM ik_door_approvals WHERE plan_id=?',id),sql('DELETE FROM ik_door_plans WHERE id=? AND job_id IS NULL',id),
    ]);
    return {planId:id,state:'cancelled'};
  }
  const snapshot = await loadSnapshot(request,row), context = kitContext(request,snapshot), job = await readD1ImportJob(row.job_id,context);
  const result = ['completed','undoing','undone'].includes(job.state)?await undoD1Import(job.id,context):await rollbackD1Import(job.id,context);
  await lineage(request);
  return {planId:id,job:result};
}
