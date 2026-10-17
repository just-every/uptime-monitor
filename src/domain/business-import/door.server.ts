import type {Bindings} from '../control.server.ts'
import {createImportDoorHandler,verifyImportGrant} from '../../base/modules/import-kit/src/door/index.ts'
import {businessTargets} from './targets.server.ts'
import {businessClaim,businessOwner,BUSINESS_DESTINATION} from './context.server.ts'
import {parseRobotPage,robotMapping} from './mapping.server.ts'

// Finish bounded rejected requests too. Leaving their body unread can reset an
// HTTP keep-alive connection just as Business sends its next request.
async function finishRequest(request:Request,response:Response){
 if(request.body&&!request.bodyUsed){
  const reader=request.body.getReader();let bytes=0
  try{for(;;){const {done,value}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>100_000){await reader.cancel();break}}}
  finally{reader.releaseLock()}
 }
 return response
}
export async function businessImportDoor(env:Bindings,request:Request){
 const installation=env.RER_OWNER_CLAIM_INSTALLATION_ID??''
 const row=await env.DB.prepare('SELECT enabled FROM ik_door_settings WHERE installation_id=?').bind(installation).first<{enabled:number}>()
 if(row?.enabled!==1)return finishRequest(request,Response.json({error:'not_found'},{status:404,headers:{'cache-control':'no-store'}}))
 try{
  const claim=businessClaim(env),token=request.headers.get('authorization')
  if(!token?.startsWith('Bearer '))return finishRequest(request,Response.json({error:'grant_required'},{status:401,headers:{'cache-control':'no-store'}}))
  const {grant}=await verifyImportGrant(token.slice(7),claim),owner=await businessOwner(env)
  return finishRequest(request,(await createImportDoorHandler({database:env.DB,targets:businessTargets(env,owner),claim,ownerPrincipalId:owner,
   destination:BUSINESS_DESTINATION,mapping:await robotMapping(env,grant.namespace),parse:parseRobotPage,format:'ndjson',
   limits:{maxBytes:2*1024*1024,maxRecords:500}})(request))!)
 }catch(error){
  const status=error&&typeof error==='object'&&'status' in error?Number(error.status):503
  return finishRequest(request,Response.json({error:status===403?'invalid_grant':'import_door_unavailable'},{status,headers:{'cache-control':'no-store'}}))
 }
}
