import { guardErasedSources } from './erasure-claims.ts';
import type { ImportPlan } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
import { requestContext } from './budget.ts';
import { atomic, authorize, guard, sql } from './database.ts';
import { exactRows, jobColumns, jsonChunks } from './sets.ts';
import { loadPlans, planGuards } from './plans.ts';

export const targetIdentity = (destination: string, kind: string, id: string): string => sha256(canonicalJson([destination, kind, id]));
export function planIndex(plan: ImportPlan, id: string) {
  return plan.entries.flatMap((entry,ordinal)=>{
    if(entry.erased && (entry.effects.length!==0 || typeof entry.erasedTargetIdentity!=='string' || !/^[a-f0-9]{64}$/.test(entry.erasedTargetIdentity)))throw new Error('IMPORT_ERASURE_BINDING_MISSING');
    const targets=entry.erased?[entry.erasedTargetIdentity!]:[...new Set(entry.effects.map(effect=>targetIdentity(plan.destination,effect.targetKind,effect.targetId)))].concat(entry.effects.length?[]:['']);
    return targets.map(target=>({job_id:id,ordinal,source_identity:entry.identity,target_identity:target}));
  });
}

/** The erased target digest is part of the fingerprinted plan. It remains
 * distinct from a source that never mapped an effect; both tombstones persist. */
export function erasedIndexGuards(plan:ImportPlan):D1Sql[]{
  if(!plan.entries.some(entry=>entry.erased))return [];
  return jsonChunks(planIndex(plan,'').filter(row=>plan.entries[row.ordinal].erased)).map(body=>guard(sql("NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=json_extract(e.value,'$.source_identity')) OR NOT EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=json_extract(e.value,'$.target_identity')))",body,plan.destination,plan.destination)));
}
export function planIndexRows(plan: ImportPlan, id: string): D1Sql[] {
  return jsonChunks(planIndex(plan, id), 1500000).map(chunk => sql("INSERT INTO ik_d1_plan_index(job_id,ordinal,source_identity,target_identity) SELECT json_extract(value,'$.job_id'),json_extract(value,'$.ordinal'),json_extract(value,'$.source_identity'),json_extract(value,'$.target_identity') FROM json_each(?)", chunk));
}
export function retainPlanIndex(plan: ImportPlan, id: string): D1Sql[] {
  return [...planIndexRows(plan, id), sql('INSERT INTO ik_d1_indexed_jobs(job_id,fingerprint) VALUES(?,?)', id, plan.fingerprint)];
}
export const unindexedJobs = 'SELECT j.id FROM ik_jobs j LEFT JOIN ik_d1_indexed_jobs i ON i.job_id=j.id WHERE j.destination=? AND (i.job_id IS NULL OR i.fingerprint<>j.fingerprint)';

/** One bounded upgrade page. Run until complete after applying d1ErasureSchema.
 * A fingerprint fence prevents an old page from indexing a concurrently changed
 * plan. Index values are hashes only; no answers, source IDs or native IDs. */
export async function upgradeD1ImportErasureIndex(context: D1ImportContext, pageSize = 4): Promise<{ indexed: number; complete: boolean }> {
  context = requestContext(context);
  if (!Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 100) throw new Error('INVALID_BATCH_LIMIT');
  await authorize(context, 'erase');
  const db = context.storage.database;
  const { results: rows } = await db.prepare(`SELECT * FROM ik_jobs WHERE id IN (${unindexedJobs}) ORDER BY id LIMIT ?`).bind(context.destination, pageSize).all();
  // Only one plan is materialized at a time, including old chunked plans.
  for (const row of rows) {
    const plan = (await loadPlans([row], db, context.storage.targets.planCodec)).get(row.id)!;
    if (plan.fingerprint !== row.fingerprint || planFingerprint(plan) !== row.fingerprint || plan.destination !== row.destination || plan.entries.length !== row.total) throw new Error('STORED_PLAN_CHANGED');
    await atomic(context, 'erase', [...await guardErasedSources(plan, db), ...exactRows('ik_jobs', jobColumns, [row]), ...planGuards(plan, row.id),
      sql('DELETE FROM ik_d1_plan_index WHERE job_id=?', row.id), sql('DELETE FROM ik_d1_indexed_jobs WHERE job_id=?', row.id), ...retainPlanIndex(plan, row.id)]);
  }
  const pending = await db.prepare(`${unindexedJobs} LIMIT 1`).bind(context.destination).first();
  return { indexed: rows.length, complete: !pending };
}
export const indexedDestinationGuard = (destination: string): D1Sql => guard(sql(`NOT EXISTS(${unindexedJobs})`, destination));
