import { guardPredicate } from './guards.ts';
import type { D1ImportContext,D1Sql } from './types.ts';
import { guard,sql } from './database.ts';
import { streamBoundary } from './stream-boundary.ts';
import { nativeStageGuards,type NativeStageBinding } from './native-staging-state.ts';
import { discardNativeStage } from './native-staging-lifecycle.ts';

/** The final batch contains only compact references to already verified values. */
export function nativePublication(context:D1ImportContext,stage:NativeStageBinding,input:{baseline:D1Sql[];writes:D1Sql[];post:D1Sql[];jobs:string[];targets:{kind:string;id:string;present:boolean}[];binding:D1Sql[];final:D1Sql[];erasure?:boolean}){
  if(!context.storage.targets.stagingPublication)throw new Error('IMPORT_NATIVE_STAGING_ADAPTER_REQUIRED');
  const id=stage.id,sealed=stage.identities+stage.chunks_count,jobIds=JSON.stringify(input.jobs),protection=context.storage.targets.stagingPublication(id,context.destination,input.targets);
  const publication=[...input.baseline,...nativeStageGuards(stage,sealed),guard(sql('EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=? AND sealed_revision=revision AND registered_revision IS NOT NULL)',id)),...input.writes,
    ...protection.protect,sql('INSERT INTO ik_d1_publication_fences(stream_id,job_id) SELECT ?,value FROM json_each(?)',id,jobIds),guard(sql('(SELECT count(*) FROM ik_d1_publication_fences WHERE stream_id=?)=json_array_length(?) AND NOT EXISTS(SELECT 1 FROM json_each(?) j WHERE NOT EXISTS(SELECT 1 FROM ik_d1_publication_fences f WHERE f.stream_id=? AND f.job_id=j.value))',id,jobIds,jobIds,id)),...streamBoundary(stage.job_id,stage.action),...input.binding,...input.post,...protection.seal,sql('UPDATE ik_d1_custody_revisions SET seal=? WHERE job_id IN (SELECT value FROM json_each(?))',id,jobIds),
    ...nativeStageGuards(stage,sealed,!input.erasure),...discardNativeStage(stage),guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=?)',id))];
  const release=[...protection.release,sql('DELETE FROM ik_d1_publication_fences WHERE stream_id=?',id)];
  const final=[...streamBoundary(stage.job_id,stage.action).map(query=>guardPredicate(query)!),...input.final,...protection.final,sql('NOT EXISTS(SELECT 1 FROM json_each(?) j WHERE NOT EXISTS(SELECT 1 FROM ik_d1_custody_revisions r WHERE r.job_id=j.value AND r.seal=?))',jobIds,id),sql('NOT EXISTS(SELECT 1 FROM ik_d1_creation_plans WHERE job_id IN (SELECT value FROM json_each(?)))',jobIds)];
  return {publication,release,final};
}
