import { jobLayoutGuards, jobLayoutPredicate } from './stream-layout.ts';
import { reversalState } from './undo-state.ts';
import type { ImportAction, ImportJob, ImportPlan, TargetRow } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { storedPlanSnapshot, planBody } from './plans.ts';
import { custodyGuards } from './integrity.ts';
import { streamBoundary } from './stream-boundary.ts';
export interface NativeStagingScope {
  job: ImportJob; plan: ImportPlan; nativeGuards: D1Sql[]; postGuards: D1Sql[];
  targets: { kind: string; id: string; present: boolean }[];

}
export const nativeStagingScope = (job: ImportJob, plan: ImportPlan): NativeStagingScope => ({job,plan,nativeGuards:[],postGuards:[],targets:[]});
export async function readStagedTarget(context: D1ImportContext, scope: NativeStagingScope, kind: string, id: string): Promise<TargetRow | undefined> {
  const {database,targets}=context.storage;
  if(context.maxBatchBytes===undefined || !targets.stagingRead){const row=await targets.read(database,context.destination,kind,id);scope.targets.push({kind,id,present:!!row});return row;}
  const snapshot=await targets.stagingRead(database,context.destination,kind,id);
  if(!snapshot.guards.length)throw new Error('IMPORT_NATIVE_STAGING_GUARD_REQUIRED');
  scope.nativeGuards.push(...snapshot.guards.map(guard));
  scope.targets.push({kind,id,present:!!snapshot.row});
  return snapshot.row;
}
/** The revision was captured before loading any plan chunk. It fences the exact
 * decoded, fingerprint-verified bytes, never a later revision read and rebound. */
export function nativeStagingBaseline(scope: NativeStagingScope, action: ImportAction): D1Sql[] {
  const {job,plan}=scope, revision=storedPlanSnapshot(plan,job.id).revision;
  if(revision===undefined||!Number.isSafeInteger(revision)||revision<0)throw new Error('IMPORT_NATIVE_STAGING_REVISION_REQUIRED');
  return [...jobLayoutGuards(job), ...streamBoundary(job.id,action), ...custodyGuards(plan,job.id), ...nativeStagingPostBinding(scope),
    guard(sql('EXISTS(SELECT 1 FROM ik_jobs j JOIN ik_d1_custody_revisions r ON r.job_id=j.id WHERE j.id=? AND j.fence=? AND j.checkpoint=? AND j.state=? AND j.fingerprint=? AND j.plan_json IS ? AND j.principal_id=? AND j.destination=? AND j.namespace=? AND j.mapping_version=? AND j.total=? AND r.revision=?)',job.id,job.fence,job.checkpoint,job.state,job.fingerprint,planBody(plan),job.principalId,job.destination,job.namespace,job.mappingVersion,job.total,revision)),...scope.nativeGuards];
}

/** Native progress may advance, but reviewed custody and owner binding cannot. */
export const nativeStagingPostBinding=(scope:NativeStagingScope):D1Sql[]=>[guard(nativeStagingPostPredicate(scope))];
export function nativeStagingPostPredicate(scope:NativeStagingScope):D1Sql{
  const {job,plan}=scope, revision=storedPlanSnapshot(plan,job.id).revision, policy=reversalState(job);
  const binding = sql('EXISTS(SELECT 1 FROM ik_jobs j JOIN ik_d1_custody_revisions r ON r.job_id=j.id WHERE j.id=? AND j.destination=? AND j.namespace=? AND j.mapping_version=? AND j.principal_id=? AND j.fingerprint=? AND j.plan_json IS ? AND j.total=? AND r.revision=? AND j.reversal_policy=? AND j.reversal_policy_hash IS ?)',job.id,job.destination,job.namespace,job.mappingVersion,job.principalId,job.fingerprint,planBody(plan),job.total,revision,policy.policy,policy.binding);
  const layout = jobLayoutPredicate(job);
  return layout ? sql(`(${binding.sql}) AND (${layout.sql})`, ...binding.params!, ...layout.params??[]) : binding;
}

export function expectedStagedTarget(scope:NativeStagingScope,kind:string,id:string,present:boolean):void{
  scope.targets=scope.targets.map(target=>target.kind===kind&&target.id===id?{kind,id,present}:target);
}
