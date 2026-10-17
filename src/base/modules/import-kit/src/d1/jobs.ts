import { captureJobLayout, jobLayoutGuards, jobLayoutReads } from './stream-layout.ts';
import { needsStreamOrigin } from './stream-origin.ts';
import { nativeStagingScope } from './native-staging-context.ts';
import { streamBoundary } from './stream-boundary.ts';
import { requestContext } from './budget.ts';
import { loadPlans, planBody, planGuards, planChunkReads } from './plans.ts';
import { randomUUID } from 'node:crypto';
import type { ImportJob, ImportPlan, ImportMapping, SourceBundle, ImportAction } from '../types.ts';
import { buildImportPlan, planFingerprint } from '../plan/index.ts';
import { creationStatements, validateCreation } from './creation.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { atomic, readAtomic, authorize, guard, sql } from './database.ts';
import { custodyGuards, inventoryGuards } from './integrity.ts';
import { guardErasedSources } from './erasure-claims.ts';
import { readOriginals } from './originals.ts';
import { retiredError } from './retirement.ts';
import { attachReversal, loadReversalState, reversalGuards, reversalState } from './undo-state.ts';

export function decode(row: any): ImportJob {
  return { id: row.id, fingerprint: row.fingerprint, destination: row.destination, namespace: row.namespace, mappingVersion: row.mapping_version, principalId: row.principal_id, state: row.state, checkpoint: row.checkpoint, total: row.total, fence: row.fence, error: row.error };
}
const planReads = new WeakMap<ImportJob, number>();
export const storedReads = (job: ImportJob) => 1 + (planReads.get(job) ?? 0) + Number(reversalState(job).policy === 'preserve-conflicts') + jobLayoutReads(job);
export async function stored(id: string, context: D1ImportContext, action: ImportAction = 'read'): Promise<{ job: ImportJob; plan: ImportPlan }> {
  const bounded = context.maxBatchBytes !== undefined;
  const query = `SELECT j.*${bounded?',r.revision AS custody_revision':''},${needsStreamOrigin('j.id')} AS stream_origin_required,(SELECT stream_id FROM ik_d1_stream_origins WHERE job_id=j.id) AS origin_stream_id,(SELECT stream_id FROM ik_d1_stream_pages WHERE job_id=j.id) AS page_stream_id FROM ik_jobs j${bounded?' LEFT JOIN ik_d1_custody_revisions r ON r.job_id=j.id':''} WHERE j.id=?`;
  const row = await context.storage.database.prepare(query).bind(id).first();
  if (!row) throw await retiredError(context.storage.database, id, 'job') ?? new Error('IMPORT_JOB_NOT_FOUND');
  const job = decode(row), plan = (await loadPlans([row], context.storage.database, context.storage.targets.planCodec)).get(id)!;
  planReads.set(job, planChunkReads(row.plan_json));
  if (job.destination !== context.destination || job.namespace !== context.namespace || job.mappingVersion !== context.mappingVersion || job.principalId !== context.authorization.principalId) throw new Error('JOB_BINDING_MISMATCH');
  if (context.reviewedFingerprint !== undefined && context.reviewedFingerprint !== job.fingerprint) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  if (planFingerprint(plan) !== job.fingerprint || plan.fingerprint !== job.fingerprint || plan.destination !== job.destination || plan.namespace !== job.namespace || plan.mappingVersion !== job.mappingVersion || plan.principalId !== job.principalId || plan.entries.length !== job.total) throw new Error('STORED_PLAN_CHANGED');
  job.erasedRecords = plan.entries.filter(entry => entry.erased).length;
  attachReversal(job, await loadReversalState(row, plan, context));
  await captureJobLayout(job, row, context, action);
  return { job, plan };
}
export function boundary(job: ImportJob, plan: ImportPlan, action: ImportAction = 'read'): D1Sql[] {
  return [...jobLayoutGuards(job), ...streamBoundary(job.id, action), ...planGuards(plan, job.id), ...reversalGuards(job.id, reversalState(job)), guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND fence=? AND checkpoint=? AND state=? AND fingerprint=? AND plan_json=? AND principal_id=? AND destination=? AND namespace=? AND mapping_version=? AND total=? AND error IS ?)', job.id, job.fence, job.checkpoint, job.state, job.fingerprint, planBody(plan), job.principalId, job.destination, job.namespace, job.mappingVersion, job.total, job.error))];
}
export async function planD1Import(source: SourceBundle, mapping: ImportMapping, context: D1ImportContext): Promise<ImportPlan> {
  context = requestContext(context);
  await authorize(context, 'plan');
  const plan = buildImportPlan(source, mapping, context);
  await authorize(context, 'plan');
  return plan;
}
export async function createD1ImportJob(plan: ImportPlan, context: D1ImportContext): Promise<ImportJob> {
  context = requestContext(context);
  await authorize(context, 'create');
  const operationKey = validateCreation(plan, context);
  const revalidate = () => {
    if (validateCreation(plan, context) !== operationKey) throw new Error('OPERATION_KEY_CONFLICT');
  };
  const db = context.storage.database;
  const prior = await db.prepare('SELECT * FROM ik_jobs WHERE destination=? AND principal_id=? AND operation_key=?').bind(context.destination, context.authorization.principalId, operationKey).first();
  // Lookup is an awaited boundary on both fresh creation and replay.
  revalidate();
  if (prior) {
    const current = await stored(prior.id, { ...context, reviewedFingerprint: undefined }, 'create');
    revalidate();
    if (prior.fingerprint !== plan.fingerprint && current.plan.erasure?.reviewedFingerprint !== plan.fingerprint) throw new Error('OPERATION_KEY_CONFLICT');
    await readOriginals(current.plan, current.job.id, context);
    revalidate();
    const inventory = await inventoryGuards(current.job, current.plan, context);
    revalidate();
    await atomic(context, 'create', [...boundary(current.job, current.plan), ...custodyGuards(current.plan, current.job.id), ...inventory]);
    return current.job;
  }
  await guardErasedSources(plan, db);
  revalidate();
  const id = randomUUID();
  const creation = creationStatements(plan, context, id, operationKey);
  const body = creation.find(s => s.sql.startsWith('INSERT INTO ik_jobs('))!.params![7];
  const returnReads = 1 + planChunkReads(body) + Number(context.reversalPolicy === 'preserve-conflicts');
  await atomic(context, 'create', creation, returnReads);
  return (await stored(id, context, 'create')).job;
}
export async function readD1ImportJob(id: string, context: D1ImportContext): Promise<ImportJob> {
  context = requestContext(context);
  await authorize(context, 'read');
  const { job, plan } = await stored(id, context);
  if(context.maxBatchBytes!==undefined)await readOriginals(plan,id,context);
  await readAtomic(context, [...boundary(job, plan)], nativeStagingScope(job,plan));
  return job;
}
