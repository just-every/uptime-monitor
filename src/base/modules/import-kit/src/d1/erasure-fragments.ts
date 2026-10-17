import { erasedCustody } from './erased-custody.ts';
import type { ImportPlan } from '../types.ts';
import type { D1FieldRedaction, D1ImportContext, D1Sql } from './types.ts';
import { canonicalJson,sha256 } from '../provenance/identity.ts';
import { sql,guard } from './database.ts';
import { exactRows,selectedCounts,jsonChunks,jobColumns,insertJsonRows } from './sets.ts';
import { retainPlan,planGuards } from './plans.ts';
import { decode } from './jobs.ts';
import { readOriginals } from './originals.ts';
import { originalVersions,custodyGuards } from './original-storage.ts';
import { inventoryGuards,releaseInventory } from './integrity.ts';
import { attachReversal,loadReversalState,snapshotReversalGuards,policyHash } from './undo-state.ts';
import { erasePlanRecords,type RelatedPlanErasure } from './erased-plan.ts';
import { erasureWrites,type ErasedDecision,type ErasedJob } from './erasure-writes.ts';
import { eraseReport } from './erasure-report.ts';
import { changed,deleteRows,ExpectedState } from './expected-state.ts';
import type { ErasurePage } from './erasure-pages.ts';
import { readErasureSnapshot } from './erasure-snapshot.ts';
import { planIndex,erasedIndexGuards } from './erasure-index.ts';
import { redactPlanFields } from './field-redaction.ts';
import { redactedCustody } from './redacted-custody.ts';

export interface ErasureFragment {
  before:D1Sql[]; writes:D1Sql[]; post:D1Sql[];
  plans:ImportPlan[]; jobs:any[]; erased:number;
}
export interface ErasureSelection {
  ids:string[]; reports:string[]; identities:Set<string>;kind:string;targetId:string;
  severed?:RelatedPlanErasure; originals?:Map<string,Set<string>>;
  fields?:readonly D1FieldRedaction[]; redactedJobs?:ReadonlySet<string>; retiring?:ReadonlySet<string>;
}
/** Bulk reads and set writes for small custody; large pages stay isolated.
 * No decoded plan or inventory body survives advancing this iterator. */
export async function* erasureFragments(selection:ErasureSelection,context:D1ImportContext,pages:ErasurePage[]):AsyncGenerator<ErasureFragment>{
  const {database:db,targets}=context.storage,affected=new Set(selection.ids),reports=new Set(selection.reports),retiring=selection.retiring??new Set<string>(),fields=selection.fields??[],changedJobs=new Set([...selection.ids,...selection.reports].filter(id=>!retiring.has(id)));
  for(const page of pages){
    const {ids}=page;
    const snapshot=await readErasureSnapshot(page,db,targets.planCodec),before:D1Sql[]=[],payload:D1Sql[]=[],reportsAfter:D1Sql[]=[],post:D1Sql[]=[],jobs:ErasedJob[]=[],decisions:ErasedDecision[]=[],redactions:ErasedDecision[]=[];
    const group=new Set(ids),changedIds=ids.filter(id=>affected.has(id)&&!retiring.has(id));
    const release=jsonChunks(changedIds).flatMap(chunk=>[sql('UPDATE ik_d1_creation_plans SET released=1 WHERE job_id IN (SELECT value FROM json_each(?))',chunk),sql('DELETE FROM ik_d1_creation_plans WHERE job_id IN (SELECT value FROM json_each(?))',chunk)]);
    const remove=jsonChunks(changedIds).map(chunk=>{const selected=new Set(JSON.parse(chunk));return changed(sql('DELETE FROM ik_d1_plan_chunks WHERE job_id IN (SELECT value FROM json_each(?))',chunk),deleteRows('ik_d1_plan_chunks',row=>selected.has(row.job_id)));});
    for(const id of ids){
      const raw=snapshot.rows('ik_jobs','id',[id])[0],plan=snapshot.plans.get(id)!;if(!raw)throw new Error('IMPORT_JOB_NOT_FOUND');
      if(plan.destination!==context.destination)throw new Error('PLAN_BINDING_MISMATCH');
      const job=decode(raw),reversal=await loadReversalState(raw,plan,context,false,snapshot);attachReversal(job,reversal);
      await readOriginals(plan,id,context,snapshot);
      const pages=snapshot.rows('ik_d1_stream_pages','job_id',[id]);
      before.push(...snapshotReversalGuards(raw,reversal),...custodyGuards(plan,id),...await inventoryGuards(job,plan,context,snapshot),...exactRows('ik_d1_stream_pages',['stream_id','ordinal','job_id','total','erased_records'],pages),...selectedCounts('ik_d1_stream_pages','job_id',[id],pages));
      if(retiring.has(id)){
        post.push(...planGuards(plan,id),...custodyGuards(plan,id));
        continue;
      }
      let next=plan;
      if(affected.has(id)&&!retiring.has(id)){
        const originals=selection.originals?.get(id);
        if(!originals&&!selection.redactedJobs?.has(id)&&!plan.entries.some(entry=>selection.identities.has(entry.identity)))throw new Error('IMPORT_ERASURE_INDEX_MISMATCH');
        if(plan.entries.some(entry=>selection.identities.has(entry.identity)&&!selection.severed?.identities.has(entry.identity)&&entry.effects.some(effect=>effect.targetKind!==selection.kind||effect.targetId!==selection.targetId)))throw new Error('IMPORT_ERASURE_SHARED_SOURCE');
        next=erasePlanRecords(plan,selection.identities,context.originalErasurePolicy,{identities:selection.severed?.identities??new Set(),target:selection.severed?.target??'',originals});
        if(fields.length)next=redactPlanFields(next,fields,context.originalErasurePolicy).plan;
        originalVersions.set(next,originalVersions.get(plan)!);
        const retained=retainPlan(next,id,next.source.artifacts.some(a=>a.original?.status==='available')?undefined:targets.planCodec);
        decisions.push(...next.entries.flatMap((entry,ordinal)=>selection.identities.has(entry.identity)?[{job_id:id,ordinal,identity:entry.identity,source_hash:entry.source.payloadHash,effects_hash:sha256(canonicalJson(entry.effects))}]:[]));
        redactions.push(...next.entries.flatMap((entry,ordinal)=>!entry.erased&&(entry.source.payloadHash!==plan.entries[ordinal].source.payloadHash||canonicalJson(entry.effects)!==canonicalJson(plan.entries[ordinal].effects))?[{job_id:id,ordinal,identity:entry.identity,source_hash:entry.source.payloadHash,effects_hash:sha256(canonicalJson(entry.effects))}]:[]));
        jobs.push({id,body:retained.body,fingerprint:next.fingerprint,erasedRecords:next.entries.filter(entry=>entry.erased).length,policyHash:reversal.binding===null?null:policyHash(id,next.fingerprint,reversal.policy)});
        payload.push(...erasedCustody(plan,next,id,snapshot),...retained.statements,guard(sql('(SELECT count(*) FROM ik_d1_creation_plans WHERE job_id=? AND released=0)=? AND (SELECT count(*) FROM ik_d1_plan_chunks WHERE job_id=?)=?',id,retained.encoded.chunks.length,id,retained.encoded.chunks.length)));
        if(fields.length)payload.push(...redactedCustody(plan,next,id,snapshot,fields));
        if(canonicalJson(planIndex(next,id))!==canonicalJson(planIndex(plan,id)))payload.push(changed(sql('DELETE FROM ik_d1_plan_index WHERE job_id=?',id),deleteRows('ik_d1_plan_index',row=>row.job_id===id)),...insertJsonRows('ik_d1_plan_index',['job_id','ordinal','source_identity','target_identity'],planIndex(next,id)));
        post.push(...exactRows('ik_d1_plan_chunks',['job_id','ordinal','body'],retained.encoded.chunks,sql('job_id=?',id)));
      }else post.push(...planGuards(plan,id));
      if(reports.has(id)&&!retiring.has(id)){const report=await eraseReport(raw,plan,next,context,snapshot);before.push(...report.before);reportsAfter.push(...report.after);}
      post.push(...erasedIndexGuards(next),...custodyGuards(next,id),...exactRows('ik_d1_plan_index',['job_id','ordinal','source_identity','target_identity'],planIndex(next,id),sql('job_id=?',id)),guard(sql('EXISTS(SELECT 1 FROM ik_d1_indexed_jobs WHERE job_id=? AND fingerprint=?)',id,next.fingerprint)),guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_creation_plans WHERE job_id=?)',id)));
    }
    const writes=[...release,...remove,...payload,...release,...erasureWrites(jobs,decisions,redactions),...reportsAfter];
    const excluded=new Set([...changedJobs].filter(id=>!group.has(id)));
    post.unshift(...new ExpectedState([...before,...writes]).guards(excluded));
    releaseInventory(context);
    yield {before,writes,post,plans:[...snapshot.plans.values()],jobs:snapshot.tables.get('ik_jobs')!,erased:decisions.length};
  }
}

export function erasureBaselines(jobs:any[]):D1Sql[]{
  for(const row of jobs)if(!Number.isSafeInteger(row.state_revision))throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');
  return [...exactRows('ik_jobs',jobColumns.filter(column=>column!=='plan_json'),jobs),...jsonChunks(jobs.map(row=>[row.id,row.state_revision])).map(body=>guard(sql("NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_custody_states s WHERE s.job_id=json_extract(e.value,'$[0]') AND s.revision=json_extract(e.value,'$[1]')))",body)))];
}
