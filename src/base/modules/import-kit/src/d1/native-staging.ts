import { guardPredicate } from './guards.ts';
import { nativePublication } from './native-staging-publication.ts';
import { randomUUID } from 'node:crypto';
import { Buffer } from 'node:buffer';
import type { ImportAction } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import { sha256 } from '../provenance/identity.ts';
import { planIndex,erasedIndexGuards } from './erasure-index.ts';
import { stagingChunks } from './staging-transport.ts';
import { replaceRetained } from './retained-values.ts';
import { nativeStagingBaseline, nativeStagingPostBinding, nativeStagingPostPredicate, type NativeStagingScope } from './native-staging-context.ts';
import { nativeStageAtomic, nativeStageGuards, type NativeStageBinding } from './native-staging-state.ts';
import { NativeStageRows } from './native-staging-rows.ts';
import { nativeRowsCost, nativeStageCost } from './native-staging-admission.ts';
import { beginNativeStage, discardNativeStage } from './native-staging-lifecycle.ts';
const indexColumns=['ordinal','source_identity','target_identity'];
const valueColumns=['value_ordinal','ordinal','body','body_hash','body_size','value_chunks'];
function* chunks(value:string):Generator<string>{if(!value.length)yield '';else yield* stagingChunks(value);}
/** Only explicitly described safe values enter custody. Authority bindings and
 * arbitrary SQL parameters always remain in the final guarded transaction. */
export async function stageNativeTransaction(context:D1ImportContext,action:ImportAction,statements:D1Sql[],scope:NativeStagingScope,returnReads:number):Promise<unknown[]>{
  if(!context.storage.targets.stagingPublication || scope.targets.length && !context.storage.targets.stagingRead)throw new Error('IMPORT_NATIVE_STAGING_ADAPTER_REQUIRED');
  const baseline=nativeStagingBaseline(scope,action), id=randomUUID(), values:string[]=[], indexes=new Map<string,number>();
  const reference=(value:string):D1Sql=>{
    let ordinal=indexes.get(value);if(ordinal===undefined){ordinal=values.length;values.push(value);indexes.set(value,ordinal);}
    return sql("(SELECT group_concat(body,'') FROM (SELECT body FROM ik_d1_native_staging_values WHERE stage_id=? AND value_ordinal=? ORDER BY ordinal))",id,ordinal);
  };
  const native=[...statements,...scope.postGuards].map(statement=>replaceRetained(statement,reference));
  if(!values.length)throw new Error('IMPORT_NATIVE_STAGING_VALUES_REQUIRED');
  // A related owner's exact plan guards can retain additional safe bytes. Its
  // entire privacy membership joins the index before any values are written.
  const plans=new Set([scope.plan,...[...statements,...scope.postGuards].flatMap(statement=>statement.retainedValues?.privacyPlan?[statement.retainedValues.privacyPlan]:[])]);
  for(const plan of plans)if(plan.source.artifacts.some(artifact=>artifact.original?.status==='available')||plan.entries.some(entry=>entry.effects.some(effect=>effect.attachment!==undefined)))throw new Error('IMPORT_NATIVE_STAGE_ORIGINAL_BYTES_REFUSED');
  const identities=new Map<string,{source_identity:string;target_identity:string}>();
  for(const plan of plans)for(const row of planIndex(plan,''))if(!plan.entries[row.ordinal].erased)identities.set(row.source_identity+':'+row.target_identity,row);
  const index=[...identities.values()].map((row,ordinal)=>[ordinal,row.source_identity,row.target_identity]);
  const jobIds=[...new Set([scope.job.id,...[...statements,...scope.postGuards].flatMap(statement=>statement.retainedValues?.privacyJobId?[statement.retainedValues.privacyJobId]:[])])];
  const sizes=values.map(value=>{let count=0;for(const _ of chunks(value))count++;return count;});
  const stage:NativeStageBinding={id,generation:randomUUID(),destination:context.destination,namespace:context.namespace,mapping_version:context.mappingVersion,principal_id:context.authorization.principalId,job_id:scope.job.id,fingerprint:scope.plan.fingerprint,job_fence:scope.job.fence,action,values_count:values.length,chunks_count:sizes.reduce((a,b)=>a+b,0),identities:index.length};
  const valueRows=function*(){for(const [valueOrdinal,value] of values.entries()){let ordinal=0;for(const body of chunks(value))yield [valueOrdinal,ordinal++,body,sha256(body),Buffer.byteLength(body),sizes[valueOrdinal]];}};
  const registered=index.length, sealed=registered+stage.chunks_count;
  const begin=beginNativeStage(stage,baseline), discard=discardNativeStage(stage);
  const registration=[...baseline,...nativeStageGuards(stage,registered),guard(sql('(SELECT count(*) FROM ik_d1_native_staging_index WHERE stage_id=?)=?',id,stage.identities)),sql('UPDATE ik_d1_native_staging SET registered_revision=revision WHERE id=?',id),...baseline,...nativeStageGuards(stage,registered)];
  const seal=[...baseline,...nativeStageGuards(stage,sealed),guard(sql('EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=? AND registered_revision IS NOT NULL) AND (SELECT count(*) FROM ik_d1_native_staging_values WHERE stage_id=?)=? AND (SELECT count(DISTINCT value_ordinal) FROM ik_d1_native_staging_values WHERE stage_id=?)=? AND NOT EXISTS(SELECT 1 FROM ik_d1_native_staging_values WHERE stage_id=? GROUP BY value_ordinal HAVING value_ordinal<0 OR value_ordinal>=? OR min(ordinal)<>0 OR max(ordinal)<>count(*)-1 OR min(value_chunks)<>count(*) OR max(value_chunks)<>count(*))',id,id,stage.chunks_count,id,stage.values_count,id,stage.values_count)),sql('UPDATE ik_d1_native_staging SET sealed_revision=revision WHERE id=?',id),...baseline,...nativeStageGuards(stage,sealed)];
  const after=native.splice(statements.length);
  const {publication,release,final}=nativePublication(context,stage,{baseline,writes:native,post:after,jobs:jobIds,targets:scope.targets,binding:nativeStagingPostBinding(scope),final:[...[...plans].flatMap(plan=>erasedIndexGuards(plan).map(query=>guardPredicate(query)!)),sql("NOT EXISTS(SELECT 1 FROM json_each(?) i WHERE EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=json_extract(i.value,'$[1]')) OR EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=json_extract(i.value,'$[2]')))",JSON.stringify(index),stage.destination,stage.destination),nativeStagingPostPredicate(scope)]});
  // Every exact SQL member, authority predicate, readback, cleanup and return
  // read is admitted before begin can retain any normalized private bytes.
  const indexCost=nativeRowsCost(context,stage,baseline,'index',indexColumns,()=>index,0);
  const valueCost=nativeRowsCost(context,stage,baseline,'values',valueColumns,valueRows,registered);
  const cost=indexCost.queries+valueCost.queries+[begin,registration,seal,discard].reduce((n,batch)=>n+nativeStageCost(context,stage,batch),0)+nativeStageCost(context,stage,publication,release,final)+returnReads;
  if(cost>remaining(context))throw new D1RequestLimitError();
  try{
    await nativeStageAtomic(context,action,begin);
    const identityRows=new NativeStageRows(context,stage,baseline,'index',indexColumns);for(const row of index)await identityRows.append(row);await identityRows.flush();
    await nativeStageAtomic(context,action,registration);
    const payload=new NativeStageRows(context,stage,baseline,'values',valueColumns);payload.revision=registered;for(const row of valueRows())await payload.append(row);await payload.flush();
    await nativeStageAtomic(context,action,seal);
    const results=await nativeStageAtomic(context,action,publication,returnReads,release,final);
    return results;
  }catch(error){await nativeStageAtomic(context,action,discard);throw error;}
}
