import type { ImportPlan } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { loadPlans } from './plans.ts';
import { guard, sql } from './database.ts';
import { eraseReport } from './erasure-report.ts';

/** Ordinary unbounded-policy calls retain their existing atomic behavior.
 * Bounded consumers visit reports one page at a time in erasure-fragments. */
export async function eraseReversalEvidence(context: D1ImportContext, changedPlans: Map<string, ImportPlan>): Promise<{ before: D1Sql[]; after: D1Sql[] }> {
  const db=context.storage.database;
  const {results:jobs}=await db.prepare("SELECT * FROM ik_jobs WHERE destination=? AND reversal_policy='preserve-conflicts' AND EXISTS(SELECT 1 FROM ik_d1_reversal_reports r WHERE r.job_id=ik_jobs.id)").bind(context.destination).all();
  const before=[guard(sql("(SELECT count(*) FROM ik_jobs j WHERE j.destination=? AND j.reversal_policy='preserve-conflicts' AND EXISTS(SELECT 1 FROM ik_d1_reversal_reports r WHERE r.job_id=j.id))=?",context.destination,jobs.length))],after:D1Sql[]=[];
  for(const raw of jobs){
    const plan=(await loadPlans([raw],db,context.storage.targets.planCodec)).get(raw.id)!;
    const report=await eraseReport(raw,plan,changedPlans.get(raw.id)??plan,context);
    before.push(...report.before);after.push(...report.after);
  }
  return {before,after};
}
