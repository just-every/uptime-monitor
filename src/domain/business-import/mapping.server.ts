import type {Bindings} from '../control.server.ts'
import {grants,MONITOR_LIMIT} from '../control.server.ts'
import {uptimeRobotImporter} from '../definitions/importers/uptimerobot.ts'
import {keptAssignments} from '../definitions/plan.server.ts'
import {assignRunners,type Runner} from '../runner-registration.server.ts'
import {externalCheckUrl} from '../external-check.server.ts'
// @ts-ignore Shared product policy used by the ordinary definitions importer.
import {validateDefinition} from '../policy.mjs'
import {sha256} from '../../base/modules/import-kit/src/provenance/identity.ts'
import type {ImportMapping,Json,ParsedRecord} from '../../base/modules/import-kit/src/types.ts'
import {BUSINESS_MAPPING} from './context.server.ts'

// The Business reader transports saved v3 pages as NDJSON. No source fetch runs here.
export function parseRobotPage(value:Json,index:number):ParsedRecord[]{
 const entries=uptimeRobotImporter.entries(JSON.stringify(value))
 return entries.map(entry=>{
  const id=entry.value?.id
  if(!(typeof id==='string'&&id.trim()&&id.length<=256)&&!(typeof id==='number'&&Number.isSafeInteger(id)&&id>=0))throw Error('Each saved monitor needs its source id')
  return {kind:'monitor',sourceId:String(id),payload:entry.value,pointer:`/${index}/data/${entry.index}`}
 })
}
export async function robotMapping(env:Bindings,namespace:string):Promise<ImportMapping>{
 const [stored,runners,external]=await Promise.all([
  env.DB.prepare('SELECT id,definition FROM monitors').all<{id:string;definition:string}>(),
  env.DB.prepare('SELECT * FROM runners ORDER BY registered_at,runner_id').all<Runner>(),externalCheckUrl(env),
 ])
 const monitors=stored.results.map(row=>({...row,definition:JSON.parse(row.definition)}))
 const first=(runners.results.find(row=>row.legacy_key)??runners.results[0])?.runner_id
 const names=new Set<string>();let creates=0
 return {version:BUSINESS_MAPPING,map(record){
  const entry={index:0,label:'Saved UptimeRobot monitor',value:record.payload},mapped=uptimeRobotImporter.map(entry)
  if(!('definition' in mapped))return {effects:[],rejection:'unsupported' in mapped?mapped.unsupported:mapped.refused}
  const unmapped=mapped.differences.map(reason=>({reason}))
  try{
   if(!first)throw Error('Open Runner / archive to establish this installation’s runner before importing.')
   const id='uptimerobot-'+sha256(JSON.stringify([env.RER_OWNER_CLAIM_INSTALLATION_ID,namespace,record.sourceId]))
   const old=monitors.find(row=>row.id===id),name=String(mapped.definition.name)
   if(names.has(name))throw Error('Duplicate monitor name in this export.')
   names.add(name)
   if(monitors.some(row=>row.id!==id&&row.definition.name===name))throw Error('A monitor with this name already exists. Review it in Definitions before importing a separate source.')
   if(!old&&monitors.length+(++creates)>MONITOR_LIMIT)throw Error('The installation monitor limit would be exceeded.')
   const definition=validateDefinition({...mapped.definition,enabled:old?.definition.enabled??false,
    maintenanceUntil:old?.definition.maintenanceUntil??0,...(old?.definition.maintenanceWindows?{maintenanceWindows:old.definition.maintenanceWindows}:{})},grants(env))
   const assigned=assignRunners(runners.results,first,definition,old?{assignments:keptAssignments(old.definition,definition)}:{},external,env.RER_DEMO!=='1',env.UPTIME_ORIGIN)
   return {effects:[{targetKind:'monitor',targetId:id,value:assigned}],unmapped}
  }catch(error){return {effects:[],rejection:error instanceof Error?error.message:'Invalid monitor definition',unmapped}}
 }}
}
