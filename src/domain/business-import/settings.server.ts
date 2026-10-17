import type {Bindings} from '../control.server.ts'
import {writeImportDoorSettings} from '../../base/modules/import-kit/src/door/index.ts'
import {businessClaim,businessOwnerAuthority} from './context.server.ts'

export async function businessImportSettings(env:Bindings,request:Request,input?:any){
 const authority=await businessOwnerAuthority(env,request),installation=env.RER_OWNER_CLAIM_INSTALLATION_ID??''
 let keys:string[]=[]
 try{keys=Object.keys(businessClaim(env).publicKeys)}catch{/* An unconfigured installation can still display its disabled setting. */}
 if(input!==undefined){
  if(!input||Object.keys(input).some(key=>!['enabled','revokedKeyIds'].includes(key))||typeof input.enabled!=='boolean'||!Array.isArray(input.revokedKeyIds)||input.revokedKeyIds.some((key:unknown)=>typeof key!=='string'||!/^[A-Za-z0-9_-]{1,64}$/.test(key)))throw Error('Invalid Business import settings')
  if(input.enabled&&!keys.length)throw Error('Configure this installation’s owner-claim grant key ring before enabling Business imports.')
  await writeImportDoorSettings(env.DB,installation,{enabled:input.enabled,revokedKids:input.revokedKeyIds},authority)
 }
 const [settings,revoked]=await Promise.all([
  env.DB.prepare('SELECT enabled FROM ik_door_settings WHERE installation_id=?').bind(installation).first<{enabled:number}>(),
  env.DB.prepare('SELECT kid FROM ik_door_revoked_keys WHERE installation_id=? ORDER BY kid').bind(installation).all<{kid:string}>(),
 ])
 return {enabled:settings?.enabled===1,revokedKeyIds:revoked.results.map(row=>row.kid),keys}
}
