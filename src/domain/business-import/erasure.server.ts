import type {Bindings} from '../control.server.ts'
import type {AuditActor} from '../audit.server.ts'
import type {D1Sql} from '../../base/modules/import-kit/src/d1/types.ts'
import {retirementScope,scopeGuards,retirementWrites} from '../../base/modules/import-kit/src/d1/retirement-scope.ts'
import {finalGuard,readonlyChecks} from '../../base/modules/import-kit/src/d1/database.ts'
import {sha256} from '../../base/modules/import-kit/src/provenance/identity.ts'
import {BUSINESS_DESTINATION} from './context.server.ts'

export async function businessErasureScope(env:Bindings,id:string,name:string){
 const plans=(await env.DB.prepare('SELECT id,namespace,job_id,snapshot_hash FROM ik_door_plans WHERE complete=1').all<any>()).results
 const namespaces=new Set<string>()
 for(const plan of plans){
  const chunks=(await env.DB.prepare('SELECT body FROM ik_door_plan_chunks WHERE plan_id=? ORDER BY ordinal').bind(plan.id).all<{body:string}>()).results
  const text=chunks.map(chunk=>chunk.body).join('')
  if(sha256(text)!==plan.snapshot_hash)throw Error('Business import custody changed; erasure was not applied.')
  const snapshot=JSON.parse(text)
  if(snapshot.plan.entries.some((entry:any)=>entry.effects.some((effect:any)=>effect.targetId===id)||entry.source.payload?.friendlyName===name))namespaces.add(plan.namespace)
 }
 return [...namespaces]
}

/** A source export is an indivisible original. Erasing one of its monitors
 * retires that move's custody and undo, while peer native monitors stay intact. */
export async function eraseBusinessCustody(env:Bindings,id:string,actor:AuditActor,version:number,namespaces:string[],native:D1PreparedStatement[],checkpoint:D1PreparedStatement[]){
 const selected=JSON.stringify(namespaces),sql=(sql:string,...params:unknown[]):D1Sql=>({sql,params})
 const jobs=(await env.DB.prepare(`SELECT j.namespace FROM ik_jobs j JOIN ik_door_plans p ON p.job_id=j.id
  WHERE p.namespace IN(SELECT value FROM json_each(?))`).bind(selected).all<{namespace:string}>()).results.map(row=>row.namespace)
 const scope=await retirementScope(env.DB,BUSINESS_DESTINATION,jobs)
 const cleanup=[
  sql(`UPDATE uptime_business_import_targets SET retired=1 WHERE monitor_id IN(SELECT target_id FROM ik_provenance
   WHERE job_id IN(SELECT job_id FROM ik_door_plans WHERE namespace IN(SELECT value FROM json_each(?))))`,selected),
  sql('DELETE FROM uptime_business_import_targets WHERE monitor_id=?',id),
  sql('DELETE FROM ik_door_chunks WHERE upload_id IN(SELECT id FROM ik_door_uploads WHERE namespace IN(SELECT value FROM json_each(?)))',selected),
  sql('DELETE FROM ik_door_uploads WHERE namespace IN(SELECT value FROM json_each(?))',selected),
  sql('DELETE FROM ik_door_plan_chunks WHERE plan_id IN(SELECT id FROM ik_door_plans WHERE namespace IN(SELECT value FROM json_each(?)))',selected),
  sql('DELETE FROM ik_door_approvals WHERE plan_id IN(SELECT id FROM ik_door_plans WHERE namespace IN(SELECT value FROM json_each(?)))',selected),
  sql('DELETE FROM ik_door_plans WHERE namespace IN(SELECT value FROM json_each(?))',selected),
  sql('DELETE FROM ik_door_namespaces WHERE namespace IN(SELECT value FROM json_each(?))',selected),
 ]
 const predicate=sql("EXISTS(SELECT 1 FROM oa_operators WHERE id=? AND role='owner' AND status='active' AND version=?)",actor.ref,version)
 const receipt=crypto.randomUUID(),at=new Date().toISOString(),retire=retirementWrites(scope,BUSINESS_DESTINATION,receipt,at)
 // The seed owns the door's additional stores. Compose Base's retirement SQL
 // with the native erasure, whose full-table count guards fence this discovery.
 // The generic Kit person-retirement helper intentionally refuses unscoped stores.
 const deleted=[...cleanup,...retire].filter(query=>/^DELETE FROM /i.test(query.sql))
 const stores=[]
 for(const query of deleted){const name=/^DELETE FROM (\w+)/i.exec(query.sql)![1];const result=await env.DB.prepare(query.sql.replace(/^DELETE FROM /i,'SELECT COUNT(*) AS n FROM ')).bind(...query.params??[]).first<{n:number}>();stores.push({store:name,rows:result?.n??0})}
 const prepare=(query:D1Sql)=>env.DB.prepare(query.sql).bind(...query.params??[])
 const journal=sql('INSERT INTO ik_d1_erasure_journal(id,destination,recorded_at,records_erased,jobs_retired,streams_retired,stores_json) VALUES(?,?,?,?,?,?,?)',receipt,BUSINESS_DESTINATION,at,0,scope.jobs.length,scope.anchors.length,JSON.stringify(stores))
 await env.DB.batch([prepare(finalGuard(predicate)),...readonlyChecks(scopeGuards(scope)).map(prepare),
  ...native,...cleanup.map(prepare),...retire.map(prepare),...checkpoint,prepare(journal),prepare(finalGuard(predicate))])
}
