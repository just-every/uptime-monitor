import type {D1Database,D1TargetAdapter} from '../../base/modules/import-kit/src/d1/types.ts'
import type {TargetRow} from '../../base/modules/import-kit/src/types.ts'
import {lastScheduledStart,type Bindings} from '../control.server.ts'
import {revisionWrites,retirementWrites} from './targets/native-writes.ts'
import {assignmentGuard,assignmentPolicy} from './targets/policy.ts'
import {markerTable,markerPredicate,nativePredicate,publishedPredicate,readSnapshot,sameTarget,sql,target,together,type Snapshot} from './targets/snapshots.ts'

/** Request-local adapter. Tombstones retain native monitors after undo and make
 * reusing their IDs an explicit refusal, never a hidden overwrite. */
export function businessTargets(env:Bindings,ownerId:string):D1TargetAdapter{
 const snapshots=new Map<string,Snapshot>()
 let runners:Awaited<ReturnType<typeof assignmentPolicy>>|undefined
 const supported=(kind:string)=>{if(kind!=='monitor')throw Error('BUSINESS_IMPORT_TARGET_KIND')}
 const remember=async(database:D1Database,id:string)=>{
  const [snapshot,currentRunners]=await Promise.all([readSnapshot(database,id),runners?Promise.resolve(runners):assignmentPolicy(database)])
  runners=currentRunners
  if(snapshot.native&&snapshot.marker&&!snapshot.marker.retired)snapshot.started=await lastScheduledStart(env,id)
  snapshots.set(id,snapshot);return snapshot
 }
 const need=(id:string)=>{const snapshot=snapshots.get(id);if(!snapshot)throw Error('BUSINESS_IMPORT_NATIVE_READ_REQUIRED');return snapshot}
 const adapter:D1TargetAdapter={
  async read(database,destination,kind,id){supported(kind);return target(await remember(database,id),destination)},
  matches(row,destination,kind,id){
   supported(kind)
   if(row&&(row.destination!==destination||row.targetKind!==kind||row.targetId!==id||row.attachment))return sql('0')
   const snapshot=snapshots.get(id)
   if(row){
    // Before-images guard the complete raw native row, including values which
    // ordinary scheduling changes without a definition revision.
    if(snapshot&&sameTarget(target(snapshot,destination),row))return together(nativePredicate(snapshot.native,id),markerPredicate(snapshot.marker,id))
    return publishedPredicate(row)
   }
   if(snapshot?.native){
    // A retired binding is an absent import target, not an absent native row.
    // On undo this is also the exact intended post-state of retirementWrites.
    if(!snapshot.marker)return sql('0')
    return together(nativePredicate(snapshot.native,id),markerPredicate({...snapshot.marker,retired:1},id))
   }
   return together(nativePredicate(null,id),markerPredicate(null,id))
  },
  write(row){
   supported(row.targetKind)
   if(row.attachment)throw Error('BUSINESS_IMPORT_ATTACHMENTS_UNSUPPORTED')
   if(!runners)throw Error('BUSINESS_IMPORT_NATIVE_READ_REQUIRED')
   const policy=assignmentGuard(env,row.value,runners)
   return [{sql:`SELECT CASE WHEN (${policy.sql}) THEN 1 ELSE json('BUSINESS_IMPORT_RUNNER_CHANGED') END`,params:policy.params},...revisionWrites(row,need(row.targetId),ownerId)]
  },
  remove(_destination,kind,id){supported(kind);return retirementWrites(id,need(id),ownerId)},
  async list(database,destination){
   const {results}=await database.prepare(`SELECT monitor_id FROM ${markerTable} WHERE retired=0 ORDER BY monitor_id`).all<{monitor_id:string}>()
   const rows:TargetRow[]=[]
   for(const {monitor_id:id} of results){const row=target(await remember(database,id),destination);if(!row)throw Error('BUSINESS_IMPORT_NATIVE_MONITOR_MISSING');rows.push(row)}
   return rows
  },
  matchesCount(count){return sql(`(SELECT COUNT(*) FROM ${markerTable} WHERE retired=0)=?`,count)},
 }
 return adapter
}
