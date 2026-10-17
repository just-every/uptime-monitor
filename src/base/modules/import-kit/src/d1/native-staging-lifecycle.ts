import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql, authorize } from './database.ts';
import { requestContext } from './budget.ts';
import { requireText } from '../provenance/identity.ts';
import { nativeStageAtomic, nativeStageGuards, type NativeStageBinding } from './native-staging-state.ts';
export function beginNativeStage(stage:NativeStageBinding,baseline:D1Sql[]):D1Sql[]{
  return [...baseline,
    sql('DELETE FROM ik_d1_native_staging WHERE destination=? AND namespace=? AND principal_id=? AND job_id=?',stage.destination,stage.namespace,stage.principal_id,stage.job_id),
    sql(`INSERT INTO ik_d1_native_staging(${Object.keys(stage).join(',')}) VALUES(${Object.keys(stage).map(()=>'?').join(',')})`,...Object.values(stage)),
    ...baseline,...nativeStageGuards(stage,0)];
}
export const discardNativeStage = (stage:NativeStageBinding):D1Sql[] => [sql('DELETE FROM ik_d1_native_staging WHERE id=? AND generation=? AND destination=? AND namespace=? AND principal_id=? AND job_id=?',stage.id,stage.generation,stage.destination,stage.namespace,stage.principal_id,stage.job_id)];
/** Current owner cancellation removes private values only, never public progress. */
export async function cancelD1NativeStaging(jobId:string,input:D1ImportContext):Promise<void>{
  requireText(jobId,'jobId');const context=requestContext(input);await authorize(context,'create');
  await nativeStageAtomic(context,'create',[guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND destination=? AND namespace=? AND principal_id=?)',jobId,context.destination,context.namespace,context.authorization.principalId)),sql('DELETE FROM ik_d1_native_staging WHERE job_id=? AND destination=? AND namespace=? AND principal_id=?',jobId,context.destination,context.namespace,context.authorization.principalId)]);
}

/** A privacy operator owns only the private erasure generation, not the
 * importing operator's public job. Cancellation preserves all public custody. */
export async function cancelD1ErasureStaging(jobId:string,input:D1ImportContext):Promise<void>{
  requireText(jobId,'jobId');const context=requestContext(input);await authorize(context,'erase');
  const predicate=sql("NOT EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE job_id=? AND destination=? AND namespace=? AND principal_id=? AND action='erase')",jobId,context.destination,context.namespace,context.authorization.principalId);
  await nativeStageAtomic(context,'erase',[
    guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND destination=?)',jobId,context.destination)),
    sql("DELETE FROM ik_d1_native_staging WHERE job_id=? AND destination=? AND namespace=? AND principal_id=? AND action='erase'",jobId,context.destination,context.namespace,context.authorization.principalId),
  ],0,[],[predicate]);
}

/** Reversal intent also cancels abandoned forward/private native work. */
export async function cancelNativeJobStaging(context:D1ImportContext,action:'undo'|'rollback',jobId:string,fence:number):Promise<void>{
  await nativeStageAtomic(context,action,[guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND fence=? AND destination=? AND namespace=? AND principal_id=?)',jobId,fence,context.destination,context.namespace,context.authorization.principalId)),sql('DELETE FROM ik_d1_native_staging WHERE job_id=? AND destination=? AND namespace=? AND principal_id=?',jobId,context.destination,context.namespace,context.authorization.principalId)]);
}
