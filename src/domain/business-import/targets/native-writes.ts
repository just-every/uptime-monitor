import type {D1Sql} from '../../../base/modules/import-kit/src/d1/types.ts'
import type {TargetRow} from '../../../base/modules/import-kit/src/types.ts'
import {canonicalJson} from '../../../base/modules/import-kit/src/provenance/identity.ts'
import {MONITOR_LIMIT} from '../../control.server.ts'
import {markerTable,markerPredicate,nativePredicate,sql,together,type Snapshot} from './snapshots.ts'

const assert=(predicate:D1Sql)=>sql(`SELECT CASE WHEN (${predicate.sql}) THEN 1 ELSE json('BUSINESS_IMPORT_NATIVE_CONFLICT') END`,...predicate.params??[])
function audit(ownerId:string,id:string,action:string,now:number):D1Sql{
 return sql(`INSERT INTO uptime_audit(id,actor_kind,actor_ref,occurred_at,action,subject_kind,subject_ref)
  VALUES(?,'feature',?,?,?,?,?)`,crypto.randomUUID(),`business-import:${ownerId}`,now,action,'monitor',id)
}
/** Pure SQL equivalent of revisionStatements: the async scheduled-start lookup
 * has already happened in read(). The kit executes these statements together
 * with its own current-authority, provenance and exact native guards. */
export function revisionWrites(row:TargetRow,snapshot:Snapshot,ownerId:string):D1Sql[]{
 const definition=row.value as any,old=snapshot.native,marker=snapshot.marker
 if(!definition||definition.enabled!==false)throw Error('BUSINESS_IMPORT_REQUIRES_PAUSED_MONITOR')
 if(marker?.retired)throw Error('BUSINESS_IMPORT_RETIRED_ID_USE_NEW_TARGET')
 if(old&&(!marker||marker.native_revision!==old.revision))throw Error('BUSINESS_IMPORT_NATIVE_EDIT_CONFLICT')
 if(old&&snapshot.started===undefined)throw Error('BUSINESS_IMPORT_NATIVE_READ_REQUIRED')
 const now=Date.now(),revision=(old?.revision??0)+1,data=canonicalJson(definition)
 const previous=old?JSON.parse(old.definition):null,previousState=old?JSON.parse(old.state):{}
 const started=snapshot.started??null,nextDue=Math.max(now,started===null?now:started+Math.min(definition.intervalMs,previous.intervalMs))
 const state=JSON.stringify({state:'Unknown',confirmedFailure:previousState.confirmedFailure??false,
  notificationOpen:previousState.notificationOpen??false,incidentKey:previousState.incidentKey??null,incidentRevision:previousState.incidentRevision??null})
 const statements:D1Sql[]=[assert(together(nativePredicate(old,row.targetId),markerPredicate(marker,row.targetId)))]
 if(!old)statements.push(assert(sql('(SELECT COUNT(*) FROM monitors)<?',MONITOR_LIMIT)))
 statements.push(old?
  sql('UPDATE monitors SET revision=?,definition=?,next_due=?,state=? WHERE id=? AND revision=?',revision,data,nextDue,state,row.targetId,old.revision):
  sql('INSERT INTO monitors(id,revision,definition,next_due,state) VALUES(?,?,?,?,?)',row.targetId,revision,data,nextDue,state),
  sql('INSERT INTO revisions(monitor_id,revision,definition,created_at) VALUES(?,?,?,?)',row.targetId,revision,data,now),
  sql('INSERT OR IGNORE INTO evidence_generation(monitor_id) VALUES(?)',row.targetId))
 if(previous?.type==='heartbeat'&&definition.type==='heartbeat'){
  statements.push(sql('UPDATE heartbeat_authority SET revision=? WHERE monitor_id=? AND revision=?',revision,row.targetId,old!.revision))
 }else if(previous?.type==='heartbeat'||definition.type==='heartbeat'){
  statements.push(sql(`INSERT INTO uptime_audit(id,actor_kind,actor_ref,occurred_at,action,subject_kind,subject_ref)
   SELECT ?,'feature',?,?,'credential_retire','credential',? WHERE EXISTS(SELECT 1 FROM heartbeat_authority WHERE monitor_id=?)`,
   crypto.randomUUID(),'business-import',now,crypto.randomUUID(),row.targetId),sql('DELETE FROM heartbeat_authority WHERE monitor_id=?',row.targetId))
 }
 statements.push(sql(`INSERT INTO ${markerTable}(monitor_id,kit_revision,native_revision,retired) VALUES(?,?,?,0)
  ON CONFLICT(monitor_id) DO UPDATE SET kit_revision=excluded.kit_revision,native_revision=excluded.native_revision,retired=0`,row.targetId,row.revision,revision),
  audit(ownerId,row.targetId,old?'business_import_revision':'business_import_create',now))
 return statements
}
/** Undo of a create releases import ownership, while the paused native monitor,
 * immutable revisions and any later evidence remain permanently intact. */
export function retirementWrites(id:string,snapshot:Snapshot,ownerId:string):D1Sql[]{
 if(!snapshot.native||!snapshot.marker||snapshot.marker.retired||snapshot.marker.native_revision!==snapshot.native.revision||JSON.parse(snapshot.native.definition).enabled!==false)throw Error('BUSINESS_IMPORT_NATIVE_EDIT_CONFLICT')
 return [assert(together(nativePredicate(snapshot.native,id),markerPredicate(snapshot.marker,id))),
  sql(`UPDATE ${markerTable} SET retired=1 WHERE monitor_id=? AND retired=0`,id),audit(ownerId,id,'business_import_undo',Date.now())]
}
