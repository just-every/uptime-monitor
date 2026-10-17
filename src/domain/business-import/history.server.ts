import type {Bindings} from '../control.server.ts'
import {listBatches,undoBatch} from '../definitions/index.server.ts'
import type {AuditActor} from '../audit.server.ts'
import {readImportDoorHistory} from '../../base/modules/import-kit/src/door/index.ts'
import {undoD1Import,rollbackD1Import} from '../../base/modules/import-kit/src/d1/reverse.ts'
import {businessOwnerAuthority,ownerImportContext} from './context.server.ts'

export async function definitionImportHistory(env:Bindings,request:Request){
 const native=await listBatches(env),installation=env.RER_OWNER_CLAIM_INSTALLATION_ID??''
 const history=await readImportDoorHistory(env.DB,installation,await businessOwnerAuthority(env,request)) as Array<Record<string,any>>
 const business=await Promise.all(history.map(async item=>{
  const records=(await env.DB.prepare(`SELECT p.target_id,m.definition,m.revision,p.record_json FROM ik_provenance p
   LEFT JOIN monitors m ON m.id=p.target_id WHERE p.job_id=? ORDER BY p.id`).bind(item.jobId).all<any>()).results
  const undone=['undone','rolled_back'].includes(String(item.state))
  return {id:'business:'+item.planId,createdAt:Date.parse(String(item.approvedAt)),source:'uptimerobot',
   counts:{create:records.length,change:0},undoneAt:null,undone,restoredAt:null,
   businessApproval:{owner:item.owner,approvedAt:item.approvedAt,fingerprint:item.fingerprint,approvalId:item.approvalRecordId},
   entries:records.map(row=>{const provenance=JSON.parse(row.record_json);return {monitorId:row.target_id,
    name:row.definition?JSON.parse(row.definition).name:null,action:provenance.before?'change':'create',
    revision:row.revision,currentRevision:row.revision,undoOutcome:undone?'paused':null}})}
 }))
 return [...native,...business].sort((a,b)=>b.createdAt-a.createdAt)
}
export async function undoDefinitionImport(env:Bindings,request:Request,id:string,actor:AuditActor){
 if(!id.startsWith('business:'))return undoBatch(env,id,actor)
 const planId=id.slice('business:'.length),installation=env.RER_OWNER_CLAIM_INSTALLATION_ID??''
 const row=await env.DB.prepare(`SELECT p.id,p.job_id,j.namespace,j.state FROM ik_door_plans p JOIN ik_jobs j ON j.id=p.job_id
  WHERE p.id=? AND p.installation_id=?`).bind(planId,installation).first<any>()
 if(!row)throw Object.assign(Error('Import not found'),{status:404})
 const newer=await env.DB.prepare(`SELECT 1 FROM ik_door_plans p JOIN ik_jobs j ON j.id=p.job_id
  WHERE p.previous_id=? AND j.state NOT IN ('undone','rolled_back')`).bind(planId).first()
 if(newer)throw Object.assign(Error('Undo the newest Business snapshot first.'),{status:409})
 const context=await ownerImportContext(env,request,row.namespace)
 const job=await (['completed','undoing','undone'].includes(row.state)?undoD1Import:rollbackD1Import)(row.job_id,context)
 if(!['undone','rolled_back'].includes(job.state))throw Object.assign(Error('Undo is in progress. Choose Undo this batch again to continue.'),{status:409})
 return {batchId:id,job}
}
