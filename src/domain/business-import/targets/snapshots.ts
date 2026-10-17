import type {D1Database,D1Sql} from '../../../base/modules/import-kit/src/d1/types.ts'
import type {TargetRow} from '../../../base/modules/import-kit/src/types.ts'
import {canonicalJson} from '../../../base/modules/import-kit/src/provenance/identity.ts'

export const markerTable='uptime_business_import_targets'
export const sql=(sql:string,...params:any[]):D1Sql=>({sql,params})
export type NativeMonitor={id:string;revision:number;definition:string;next_due:number;state:string}
export type Marker={monitor_id:string;kit_revision:string;native_revision:number;retired:number}
export type Snapshot={native:NativeMonitor|null;marker:Marker|null;started?:number|null}

export function target(snapshot:Snapshot,destination:string):TargetRow|undefined{
 if(snapshot.marker?.retired)return undefined
 const native=snapshot.native
 if(!native){if(snapshot.marker)throw Error('BUSINESS_IMPORT_NATIVE_MONITOR_MISSING');return undefined}
 return {destination,targetKind:'monitor',targetId:native.id,value:JSON.parse(native.definition),
  revision:snapshot.marker?.native_revision===native.revision?snapshot.marker.kit_revision:`native:${native.revision}`}
}
export async function readSnapshot(database:D1Database,id:string):Promise<Snapshot>{
 const [native,marker]=await Promise.all([
  database.prepare('SELECT id,revision,definition,next_due,state FROM monitors WHERE id=?').bind(id).first<NativeMonitor>(),
  database.prepare(`SELECT monitor_id,kit_revision,native_revision,retired FROM ${markerTable} WHERE monitor_id=?`).bind(id).first<Marker>(),
 ])
 return {native,marker}
}
export function nativePredicate(native:NativeMonitor|null,id:string):D1Sql{
 return native?sql('EXISTS(SELECT 1 FROM monitors WHERE id=? AND revision=? AND definition=? AND next_due=? AND state=?)',
  id,native.revision,native.definition,native.next_due,native.state):sql('NOT EXISTS(SELECT 1 FROM monitors WHERE id=?)',id)
}
export function markerPredicate(marker:Marker|null,id:string):D1Sql{
 return marker?sql(`EXISTS(SELECT 1 FROM ${markerTable} WHERE monitor_id=? AND kit_revision=? AND native_revision=? AND retired=?)`,
  id,marker.kit_revision,marker.native_revision,marker.retired):sql(`NOT EXISTS(SELECT 1 FROM ${markerTable} WHERE monitor_id=?)`,id)
}
export const together=(...parts:D1Sql[])=>sql(parts.map(p=>`(${p.sql})`).join(' AND '),...parts.flatMap(p=>p.params??[]))
export function sameTarget(left:TargetRow|undefined,right:TargetRow):boolean{
 return Boolean(left&&left.revision===right.revision&&canonicalJson(left.value)===canonicalJson(right.value))
}
/** Post-write values are derived from the real native definition and its numeric
 * revision. The marker holds kit identity only, never a second definition. */
export function publishedPredicate(row:TargetRow):D1Sql{
 return sql(`EXISTS(SELECT 1 FROM ${markerTable} b JOIN monitors m ON m.id=b.monitor_id
  WHERE b.monitor_id=? AND b.retired=0 AND b.kit_revision=? AND b.native_revision=m.revision AND m.definition=?)`,
  row.targetId,row.revision,canonicalJson(row.value))
}
