import type { D1ImportContext, D1Sql } from './types.ts';
import { authorize, guard, prepare, readonlyChecks, sql } from './database.ts';
import { requestContext, remaining, D1RequestLimitError } from './budget.ts';
import { bindingPredicate, stageClear, stagingBatchBytes, STAGING_BATCH_BYTES, type StageBinding } from './staging-transport.ts';
import { exactRows } from './sets.ts';

export interface ImportPreparation {
  id: string; state: 'preparing' | 'preparation-interrupted'; expectedFingerprint: string;
  checkpoint: 0; total: number; error: string | null;
}
export type PreparationState = Awaited<ReturnType<typeof preparationState>>;
export const preparationStatus = (stage: StageBinding): ImportPreparation => ({ id:stage.id,state:'preparing',expectedFingerprint:stage.fingerprint,checkpoint:0,total:stage.total,error:null });

export async function preparationState(id: string, input: D1ImportContext, action:'read'|'create'='read') {
  const context = requestContext(input); await authorize(context,action);
  const db=context.storage.database;
  const row=await db.prepare('SELECT * FROM ik_d1_staging WHERE id=?').bind(id).first();
  if(!row || row.destination!==context.destination || row.namespace!==context.namespace || row.mapping_version!==context.mappingVersion || row.principal_id!==context.authorization.principalId) throw new Error('IMPORT_PREPARATION_NOT_FOUND');
  const stage:StageBinding={id:row.id,generation:row.generation,destination:row.destination,namespace:row.namespace,mapping_version:row.mapping_version,principal_id:row.principal_id,fingerprint:row.fingerprint,operation_key:row.operation_key,total:row.total,pages:row.pages,layout_hash:row.layout_hash,reversal_policy:row.reversal_policy};
  const receipt=await db.prepare('SELECT * FROM ik_d1_preparation_receipts WHERE stage_id=?').bind(id).first();
  if(!receipt || receipt.generation!==stage.generation || row.registered_revision===null || row.sealed_revision!==row.revision) throw new Error('IMPORT_PREPARATION_INCOMPLETE');
  if(receipt.codec_id!==(context.storage.targets.planCodec?.id??null)) throw new Error('IMPORT_PLAN_CODEC_REQUIRED');
  if(context.reviewedFingerprint!==undefined && context.reviewedFingerprint!==stage.fingerprint) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  const state={context,stage,revision:row.revision as number,receipt,action};
  await preparationRead(state,sql('SELECT 1 AS present'));
  return state;
}

/** Every awaited read has the same generation/revision/receipt and current
 * authority on both sides. No stale read confers permission to publish. */
export async function preparationRead(state:{context:D1ImportContext;stage:StageBinding;revision:number;receipt:any;action:'read'|'create'}, query:D1Sql):Promise<any[]> {
  const {context,stage,revision,receipt}=state;
  const checks=readonlyChecks([guard(context.authorization.predicate(state.action,context.destination)),guard(bindingPredicate(stage)),guard(stageClear(stage)),
    guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=? AND sealed_revision=? AND registered_revision IS NOT NULL)',stage.id,revision,revision)),
    ...exactRows('ik_d1_preparation_receipts',Object.keys(receipt),[receipt],sql('stage_id=?',stage.id))]);
  const statements=[...checks,query,...checks];
  if(statements.length>remaining(context))throw new D1RequestLimitError();
  if(stagingBatchBytes(statements)>STAGING_BATCH_BYTES)throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
  const results=await context.storage.database.batch(statements.map(statement=>prepare(context.storage.database,statement)));
  const result=results[checks.length] as any,rows=Array.isArray(result)?result:result?.results;
  if(!Array.isArray(rows))throw new Error('IMPORT_D1_READ_RECEIPT_REQUIRED');
  return rows;
}

export async function readD1ImportPreparation(id:string,input:D1ImportContext):Promise<ImportPreparation>{
  const context=requestContext(input);await authorize(context,'read');
  const row=await context.storage.database.prepare('SELECT * FROM ik_d1_staging WHERE id=? AND destination=? AND namespace=? AND mapping_version=? AND principal_id=?').bind(id,context.destination,context.namespace,context.mappingVersion,context.authorization.principalId).first();
  if(!row)throw new Error('IMPORT_PREPARATION_NOT_FOUND');
  if(context.reviewedFingerprint!==undefined&&context.reviewedFingerprint!==row.fingerprint)throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  const receipt=await context.storage.database.prepare('SELECT generation FROM ik_d1_preparation_receipts WHERE stage_id=?').bind(id).first();
  if(receipt&&receipt.generation===row.generation&&row.sealed_revision===row.revision&&row.registered_revision!==null)return preparationStatus((await preparationState(id,context)).stage);
  const statements=readonlyChecks([guard(context.authorization.predicate('read',context.destination)),...exactRows('ik_d1_staging',Object.keys(row),[row],sql('id=?',id))]);
  if(statements.length>remaining(context))throw new D1RequestLimitError();
  await context.storage.database.batch(statements.map(statement=>prepare(context.storage.database,statement)));
  return{id,state:'preparation-interrupted',expectedFingerprint:row.fingerprint,checkpoint:0,total:row.total,error:'Preparation is incomplete or has changed. Retry the reviewed files or cancel this preparation.'};
}
