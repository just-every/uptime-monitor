import { stagingStatements } from './staging-statements.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, prepare, sql } from './database.ts';
import { batchBytes } from './transport.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import type { ImportAction } from '../types.ts';
export interface NativeStageBinding {
  id: string; generation: string; destination: string; namespace: string;
  mapping_version: string; principal_id: string; job_id: string; fingerprint: string;
  job_fence: number; action: ImportAction; values_count: number; chunks_count: number; identities: number;
}
export const nativeStageBinding = (stage: NativeStageBinding) => sql(`EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE ${Object.keys(stage).map(key=>`${key} IS ?`).join(' AND ')})`,...Object.values(stage));
export const nativeStageClear = (stage: NativeStageBinding) => sql('NOT EXISTS(SELECT 1 FROM ik_d1_native_staging_index i WHERE i.stage_id=? AND (EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=i.source_identity) OR EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=i.target_identity)))',stage.id,stage.destination,stage.destination);
export function nativeStageGuards(stage: NativeStageBinding, revision: number, clear=true): D1Sql[] {
  return [guard(nativeStageBinding(stage)),guard(sql('EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=? AND revision=?)',stage.id,revision)),...(clear?[guard(nativeStageClear(stage))]:[])];
}
export async function nativeStageAtomic(context: D1ImportContext, action: ImportAction, statements: D1Sql[], returnReads=0, release: D1Sql[]=[], final:D1Sql[]=[]): Promise<unknown[]> {
  const packed=stagingStatements(context,action,statements,release,final);
  if(packed.length+returnReads>remaining(context))throw new D1RequestLimitError();
  if(batchBytes(packed)>Math.min(context.maxBatchBytes??2*1024*1024,2*1024*1024))throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
  return context.storage.database.batch(packed.map(query=>prepare(context.storage.database,query)));
}
