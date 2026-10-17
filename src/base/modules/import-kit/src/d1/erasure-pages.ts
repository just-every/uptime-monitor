import type { D1Database,D1Sql } from './types.ts';
import { jsonChunks,jobColumns,provenanceColumns,claimColumns } from './sets.ts';

const PAGE_BYTES=2*1024*1024;
const UNKNOWN_BYTES=Number.MAX_SAFE_INTEGER;
// The unknown-size sentinel means an owner closure must occupy its own page.
// Saturate before addition so it remains an exact safe integer across SQL/JS.
const addBytes=(left:number,right:number):number=>left>=UNKNOWN_BYTES-right?UNKNOWN_BYTES:left+right;
const fields:[string,string[]][]=[
  ['ik_provenance',provenanceColumns],['ik_d1_history',[...provenanceColumns,'successor_id']],
  ['ik_sources',claimColumns],['ik_d1_entries',['job_id','ordinal','source_identity','owner_job_id','effects_hash']],
  ['ik_batches',['job_id','action','position','end_position','fence']],
  ['ik_originals',['job_id','kind','original_key','name','sha256','size','bytes']],
  ['ik_original_sets',['job_id','version']],['ik_d1_original_chunks',['job_id','kind','original_key','ordinal','bytes']],
  ['ik_d1_plan_chunks',['job_id','ordinal','body']],['ik_d1_creation_plans',['job_id','ordinal','body','released']],
  ['ik_d1_plan_index',['job_id','ordinal','source_identity','target_identity']],['ik_d1_indexed_jobs',['job_id','fingerprint']],
  ['ik_d1_reversal_reports',['job_id','effect_id','position','record_json']],['ik_d1_stream_pages',['stream_id','ordinal','job_id','total','erased_records']],
];
const rowBytes=(alias:string,columns:string[])=>'768+6*('+columns.map(column=>`coalesce(length(CAST(${alias}.${column} AS BLOB)),0)`).join('+')+')';
const storedBytes=rowBytes('j',jobColumns)+'+'+fields.map(([table,columns])=>`(SELECT coalesce(sum(${rowBytes('r',columns)}),0) FROM ${table} r WHERE r.job_id=j.id)`).join('+');
// Chunk manifests deliberately occupy a page alone: compressed byte length is
// not an expanded working-set measurement. Inline envelopes declare the exact
// expanded size, which the ordinary decoder independently verifies on loading.
const expandedBytes="CASE json_extract(j.plan_json,'$.d1Plan') WHEN 1 THEN 9007199254740991 WHEN 3 THEN CASE WHEN typeof(json_extract(j.plan_json,'$.expandedSize'))='integer' AND json_extract(j.plan_json,'$.expandedSize')>=0 THEN json_extract(j.plan_json,'$.expandedSize') ELSE 9007199254740991 END WHEN 2 THEN CASE WHEN typeof(json_extract(j.plan_json,'$.size'))='integer' AND json_extract(j.plan_json,'$.size')>=0 THEN json_extract(j.plan_json,'$.size') ELSE 9007199254740991 END ELSE length(CAST(j.plan_json AS BLOB)) END";
const measuredBytes=`CASE WHEN (${expandedBytes})>=${UNKNOWN_BYTES}-min(${UNKNOWN_BYTES},(${storedBytes})) THEN ${UNKNOWN_BYTES} ELSE (${storedBytes})+(${expandedBytes}) END`;
const dependencies=`SELECT owner_job_id AS id FROM ik_d1_entries WHERE job_id=j.id
  UNION SELECT h.job_id FROM (SELECT record_json FROM ik_provenance WHERE job_id=j.id UNION ALL SELECT record_json FROM ik_d1_history WHERE job_id=j.id) p JOIN ik_d1_history h ON h.id=json_extract(p.record_json,'$.predecessorId')
  UNION SELECT p.job_id FROM ik_d1_history h JOIN ik_provenance p ON p.id=h.successor_id WHERE h.job_id=j.id
  UNION SELECT p.job_id FROM ik_d1_history h JOIN ik_d1_history p ON p.id=h.successor_id WHERE h.job_id=j.id`;
interface Weight {bytes:number;dependencies:string[];revision:number}
export interface ErasurePage {ids:string[];fence:D1Sql;measuredBytes:number;readQueries?:number}

/** Group small independent or reused jobs with set reads. Only numeric sizes
 * and ownership IDs span groups; no plan, provenance or original body is read
 * here. Reused/history owners count once toward a group's complete closure. */
export async function measureErasurePages(ids:string[],db:D1Database):Promise<ErasurePage[]>{
  const weights=new Map<string,Weight>();let frontier=[...new Set(ids)].sort();
  while(frontier.length){
    const next=new Set<string>();
    for(const chunk of jsonChunks(frontier)){
      const rows=(await db.prepare(`SELECT j.id,s.revision,${measuredBytes} AS bytes,(SELECT json_group_array(id) FROM (${dependencies})) AS dependencies FROM ik_jobs j JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE j.id IN (SELECT value FROM json_each(?))`).bind(chunk).all()).results;
      if(rows.length!==JSON.parse(chunk).length)throw new Error('IMPORT_JOB_NOT_FOUND');
      for(const row of rows){if(!Number.isSafeInteger(Number(row.revision))||!Number.isSafeInteger(Number(row.bytes))||Number(row.bytes)<0)throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');const related=[...new Set(JSON.parse(row.dependencies as string) as string[])].sort();weights.set(row.id as string,{bytes:Number(row.bytes),dependencies:related,revision:Number(row.revision)});for(const id of related)if(!weights.has(id))next.add(id);}
    }
    frontier=[...next].filter(id=>!weights.has(id)).sort();
  }
  const closure=(id:string)=>{const result=new Set<string>(),pending=[id];while(pending.length){const value=pending.pop()!;if(result.has(value))continue;result.add(value);pending.push(...weights.get(value)!.dependencies);}return result;};
  const pages:ErasurePage[]=[];
  const page=(ids:string[],owners:Set<string>,measuredBytes:number):ErasurePage=>{
    const chunks=jsonChunks([...owners].sort().map(id=>[id,weights.get(id)!.revision]));
    return {ids,measuredBytes,fence:{sql:chunks.map(()=>"NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_custody_states s WHERE s.job_id=json_extract(e.value,'$[0]') AND s.revision=json_extract(e.value,'$[1]')))").join(' AND '),params:chunks}};
  };
  let group:string[]=[],owners=new Set<string>(),bytes=0;
  for(const id of ids){
    const related=closure(id),extra=()=>[...related].filter(owner=>!owners.has(owner)).reduce((n,owner)=>addBytes(n,weights.get(owner)!.bytes),0);
    if(group.length&&addBytes(bytes,extra())>PAGE_BYTES){pages.push(page(group,owners,bytes));group=[];owners=new Set();bytes=0;}
    group.push(id);bytes=addBytes(bytes,extra());for(const owner of related)owners.add(owner);
    if(bytes>=PAGE_BYTES){pages.push(page(group,owners,bytes));group=[];owners=new Set();bytes=0;}
  }
  if(group.length)pages.push(page(group,owners,bytes));
  return pages;
}
