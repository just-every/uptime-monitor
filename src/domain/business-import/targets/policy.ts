import type {D1Database,D1Sql} from '../../../base/modules/import-kit/src/d1/types.ts'
import {canonicalJson} from '../../../base/modules/import-kit/src/provenance/identity.ts'
import {grants,type Bindings} from '../../control.server.ts'
import {assignRunners,type Runner} from '../../runner-registration.server.ts'
// @ts-ignore Shared edition policy is JavaScript.
import {validateDefinition} from '../../policy.mjs'
import {sql} from './snapshots.ts'

type AssignmentRunner=Pick<Runner,'runner_id'|'registered_at'|'network_label'|'legacy_key'|'revoked_at'|'capabilities'>
export async function assignmentPolicy(database:D1Database){
 const {results}=await database.prepare('SELECT runner_id,registered_at,network_label,legacy_key,revoked_at,capabilities FROM runners ORDER BY registered_at,runner_id').all<AssignmentRunner>()
 return results
}
/** Validate retained values against current real runners. No lifecycle writes or
 * implicit creation is permitted while an import is being reviewed/applied. */
export function assignmentGuard(env:Bindings,definition:any,rows:AssignmentRunner[]):D1Sql{
 if(!['http','keyword','heartbeat'].includes(definition?.type)||definition.enabled!==false)throw Error('BUSINESS_IMPORT_UNSUPPORTED_DEFINITION')
 if(!rows.length)throw Error('BUSINESS_IMPORT_RUNNER_NOT_CONFIGURED')
 const runners:Runner[]=rows.map(row=>({...row,key_ciphertext:null,last_contact_at:null}))
 const first=rows.find(row=>row.legacy_key)?.runner_id??rows[0].runner_id
 const normalized=validateDefinition(definition,grants(env))
 const assigned=assignRunners(runners,first,normalized,{assignments:definition.assignments},null,env.RER_DEMO!=='1',env.UPTIME_ORIGIN)
 if(canonicalJson(assigned)!==canonicalJson(definition))throw Error('BUSINESS_IMPORT_ASSIGNMENTS_CHANGED_REVIEW_AGAIN')
 // Assignment authority is installation-local. Fence the set without selecting
 // runner secrets or noisy contact clocks into the retained import definition.
 return sql(`(SELECT COUNT(*) FROM runners)=? AND NOT EXISTS(
  SELECT 1 FROM json_each(?) expected WHERE NOT EXISTS(SELECT 1 FROM runners r
   WHERE r.runner_id=json_extract(expected.value,'$.runner_id')
   AND r.registered_at=json_extract(expected.value,'$.registered_at')
   AND r.network_label=json_extract(expected.value,'$.network_label')
   AND r.legacy_key=json_extract(expected.value,'$.legacy_key')
   AND r.revoked_at IS json_extract(expected.value,'$.revoked_at')
   AND r.capabilities IS json_extract(expected.value,'$.capabilities')))`,rows.length,JSON.stringify(rows))
}
