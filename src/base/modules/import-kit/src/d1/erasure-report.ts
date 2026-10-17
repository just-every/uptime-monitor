import type { InventorySnapshot } from './inventory-snapshot.ts';
import type { ImportPlan } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { canonicalJson } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
import { effectIdentity } from '../storage/provenance.ts';
import { exactRows, insertJsonRows, jobColumns } from './sets.ts';
import { planGuards } from './plans.ts';
import { loadReversalState, reversalGuards, reportHash, policyHash } from './undo-state.ts';
import { erasedDisposition, redactDisposition } from './erasure-disposition.ts';
import { sql,guard } from './database.ts';
import { changed, deleteRows, updateRows } from './expected-state.ts';

/** One validated report page; callers release its values before reading another. */
export async function eraseReport(raw:any,plan:ImportPlan,next:ImportPlan,context:D1ImportContext,snapshot?:InventorySnapshot):Promise<{before:D1Sql[];after:D1Sql[]}>{
  if(planFingerprint(plan)!==raw.fingerprint||plan.fingerprint!==raw.fingerprint||plan.destination!==raw.destination||plan.principalId!==raw.principal_id||plan.namespace!==raw.namespace||plan.mappingVersion!==raw.mapping_version||plan.entries.length!==raw.total)throw new Error('STORED_PLAN_CHANGED');
  const state=await loadReversalState(raw,plan,context,false,snapshot);
  let before=[...exactRows('ik_jobs',jobColumns,[raw]),...planGuards(plan,raw.id),...reversalGuards(raw.id,state)];
  if(context.maxBatchBytes!==undefined){
    if(!Number.isSafeInteger(raw.state_revision))throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');
    // This revision was read before the report. It covers all report/plan/job
    // writes, so old cross-effect evidence never needs private transport.
    const fence=guard(sql('EXISTS(SELECT 1 FROM ik_d1_custody_states WHERE job_id=? AND revision=?)',raw.id,raw.state_revision));
    fence.expectedSnapshots=before.flatMap(row=>row.expectedSnapshots??[]);
    before=[fence];
  }
  const erased=new Map(next.entries.filter(entry=>entry.erased).map(entry=>[effectIdentity(raw.id,entry.identity,0),entry.identity]));
  const rows=state.rows.map((row,index)=>({job_id:raw.id,effect_id:row.effect_id,position:row.position,record_json:canonicalJson(erased.has(row.effect_id)?erasedDisposition(state.dispositions[index],erased.get(row.effect_id)!):redactDisposition(state.dispositions[index]))}));
  const hash=reportHash(rows),binding=policyHash(raw.id,next.fingerprint,state.policy);
  return {before,after:[changed(sql('DELETE FROM ik_d1_reversal_reports WHERE job_id=?',raw.id),deleteRows('ik_d1_reversal_reports',row=>row.job_id===raw.id)),...insertJsonRows('ik_d1_reversal_reports',['job_id','effect_id','position','record_json'],rows),changed(sql('UPDATE ik_jobs SET reversal_hash=?,reversal_policy_hash=? WHERE id=?',hash,binding,raw.id),updateRows('ik_jobs',row=>row.id===raw.id,row=>({...row,reversal_hash:hash,reversal_policy_hash:binding})))]};
}
