import type { D1FieldMatch,D1ImportContext,D1ImportPerson,D1Sql } from './types.ts';
import { guard,sql } from './database.ts';
import { guardPredicate } from './guards.ts';
import { exactRows,jsonChunks,selectedCounts,selectKeys } from './sets.ts';
import { indexedDestinationGuard,targetIdentity,unindexedJobs } from './erasure-index.ts';
import { stageErasure,stagedSourceIdentities } from './staging-erasure.ts';
import { fieldSelectionSnapshot,relatedSelection } from './erasure-related.ts';
import { fieldRedactions,unmatchedPath } from './field-redaction.ts';
import { redactedNativeFields } from './redacted-custody.ts';
import { targetGuards } from './target-guards.ts';
import { erasureFragments,type ErasureFragment } from './erasure-fragments.ts';
import { measureErasurePages,type ErasurePage } from './erasure-pages.ts';

/** Select once before private preparation. Later passes reread only measured
 * custody pages; native snapshots, selector counts and private-stage sets stay
 * fixed while our own native stage is registered and populated. */
export async function prepareErasureSelection(person:D1ImportPerson,nativeStatements:D1Sql[],context:D1ImportContext,stageId:string,retiring:ReadonlySet<string>=new Set()):Promise<{
  fragments:()=>AsyncGenerator<ErasureFragment>;snapshotGuards:D1Sql[];final:D1Sql[];
  targets:{kind:string;id:string;present:boolean}[];fieldMatches:D1FieldMatch[];
}>{
  const {database:db,targets}=context.storage,target=person.target,kind=target?.kind??'',targetId=target?.id??'';
  const related=person.related?{...person.related,fields:fieldRedactions(person.related.fields)}:undefined;
  if(await db.prepare(`${unindexedJobs} LIMIT 1`).bind(context.destination).first())throw new Error('IMPORT_ERASURE_INDEX_UPGRADE_REQUIRED');
  const digest=target?targetIdentity(context.destination,kind,targetId):undefined;
  const targetRows=digest?await selectKeys(db,'ik_d1_plan_index','target_identity',[digest]):[];
  const scan=related?await relatedSelection(context,related,undefined,stageId):undefined;
  const identities=new Set<string>([...targetRows.map(row=>row.source_identity),...digest?await stagedSourceIdentities(digest,context):[],...scan?.identities??[]]);
  const direct=new Set<string>(targetRows.map(row=>row.source_identity));
  const severed={identities:new Set([...scan?.identities??[]].filter(identity=>!direct.has(identity))),target:digest??''};
  const sourceRows=await selectKeys(db,'ik_d1_plan_index','source_identity',[...identities]);
  const ids=[...new Set<string>([...sourceRows.map(row=>row.job_id),...scan?.originals.keys()??[],...scan?.redactedJobs??[]])];
  const reports=(await db.prepare("SELECT id FROM ik_jobs WHERE destination=? AND reversal_policy='preserve-conflicts' AND EXISTS(SELECT 1 FROM ik_d1_reversal_reports r WHERE r.job_id=ik_jobs.id) ORDER BY id").bind(context.destination).all()).results.map(row=>row.id as string);
  const privateErasure=digest?stageErasure(context.destination,digest,[...identities],stageId):{before:[],after:[],final:[]};
  const snapshot=target&&targets.stagingRead?await targets.stagingRead(db,context.destination,kind,targetId):undefined;
  if(snapshot&&!snapshot.guards.length)throw new Error('IMPORT_NATIVE_STAGING_GUARD_REQUIRED');
  const native=target?(snapshot?snapshot.row:await targets.read(db,context.destination,kind,targetId)):undefined;
  const snapshotGuards=snapshot?snapshot.guards.map(guard):[];
  const before=[indexedDestinationGuard(context.destination),...privateErasure.before,...scan?.guards??[],
    ...digest?selectedCounts('ik_d1_plan_index','target_identity',[digest],targetRows):[],
    ...selectedCounts('ik_d1_plan_index','source_identity',[...identities],sourceRows),
    ...exactRows('ik_d1_plan_index',['job_id','ordinal','source_identity','target_identity'],sourceRows),
    guard(sql("(SELECT count(*) FROM ik_jobs WHERE destination=? AND reversal_policy='preserve-conflicts' AND EXISTS(SELECT 1 FROM ik_d1_reversal_reports r WHERE r.job_id=ik_jobs.id))=?",context.destination,reports.length)),
    ...target?targetGuards(targets,native??null,context.destination,kind,targetId):[]];
  const writes=[...nativeStatements],post=target?targetGuards(targets,null,context.destination,kind,targetId):[],final=[...privateErasure.final];
  const fields=related?.fields??[];
  if(fields.length||related?.snapshot!==undefined){
    const redaction=await redactedNativeFields(kind,targetId,context,fields,scan!.fieldMatches);
    if(related?.snapshot!==undefined&&related.snapshot!==fieldSelectionSnapshot([...(scan!.discoveryGuards??scan!.guards),...redaction.before]))throw new Error('IMPORT_REDACTION_SNAPSHOT_CHANGED');
    if(unmatchedPath(fields,scan!.fieldMatches))throw new Error('IMPORT_REDACTION_PATH_UNMATCHED');
    before.push(...redaction.before);snapshotGuards.push(...redaction.before);
    writes.push(...redaction.after);final.push(...redaction.final.map(query=>guardPredicate(query)??query));
  }
  const after=[...privateErasure.after];
  if(fields.length||related?.snapshot!==undefined){
    for(const table of ['ik_d1_staging','ik_d1_native_staging','ik_d1_preparation_steps']){
      const exclude=table==='ik_d1_native_staging'?' AND id<>?':'',params=exclude?[context.destination,stageId]:[context.destination];
      after.push(sql(`DELETE FROM ${table} WHERE destination=?${exclude}`,...params),guard(sql(`NOT EXISTS(SELECT 1 FROM ${table} WHERE destination=?${exclude})`,...params)));
      final.push(sql(`NOT EXISTS(SELECT 1 FROM ${table} WHERE destination=?)`,context.destination));
    }
  }
  for(const chunk of jsonChunks([...identities])){
    after.push(sql('INSERT OR IGNORE INTO ik_d1_erased_sources(destination,identity) SELECT ?,value FROM json_each(?)',context.destination,chunk));
    final.push(sql('NOT EXISTS(SELECT 1 FROM json_each(?) i WHERE NOT EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=i.value))',chunk,context.destination));
  }
  if(digest){
    after.push(sql('INSERT OR IGNORE INTO ik_d1_erased_targets(destination,identity) VALUES(?,?)',context.destination,digest));
    final.push(sql('EXISTS(SELECT 1 FROM ik_d1_erased_targets WHERE destination=? AND identity=?)',context.destination,digest));
  }
  let pages:ErasurePage[]|undefined;
  const fragments=async function*():AsyncGenerator<ErasureFragment>{
    yield {before,writes,post,plans:[],jobs:[],erased:0};
    pages??=await measureErasurePages([...new Set([...ids,...reports,...retiring])],db);
    yield* erasureFragments({ids,reports,identities,kind,targetId,severed,originals:scan?.originals,fields,redactedJobs:scan?.redactedJobs,retiring},context,pages);
    yield {before:[],writes:after,post:[],plans:[],jobs:[],erased:0};
  };
  return {fragments,snapshotGuards,final,targets:target?[{kind,id:targetId,present:false}]:[],fieldMatches:scan?.fieldMatches??[]};
}
