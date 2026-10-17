import type { D1Sql } from './types.ts';
import { retainedTemplate as sql, retainedSql } from './retained-values.ts';
import { jsonChunks } from './sets.ts';
import { changed, updateRows, deleteRows } from './expected-state.ts';
/** Rewrite retained provenance or history bodies one packed chunk per statement,
 * so a job-wide change (a withdrawn artifact's name) does not cost one per row. */
export function recordWrites(table: string, rows: { id: string; body: string }[]): D1Sql[] {
  return jsonChunks(rows, 500000).map(chunk => {
    const selected = new Map<string, string>(JSON.parse(chunk).map((row: { id: string; body: string }) => [row.id, row.body]));
    // Joined by key, not a correlated lookup per row: cost stays linear in the chunk.
    return changed(retainedSql([`UPDATE ${table} SET record_json=d.body FROM (SELECT json_extract(value,'$.id') AS id,json_extract(value,'$.body') AS body FROM json_each(`, `)) AS d WHERE ${table}.id=d.id`], [chunk], [0]),
      updateRows(table, item => selected.has(item.id), item => ({ ...item, record_json: selected.get(item.id)! })));
  });
}
export interface ErasedDecision { job_id: string; ordinal: number; identity: string; source_hash: string; effects_hash: string }
export interface ErasedJob { id: string; body: string; fingerprint: string; erasedRecords: number; policyHash: string | null }
/** Set writes keep repeated imports from spending one D1 statement per copy.
 * Literal template boundaries make each normalized JSON binding retainable. */
export function erasureWrites(jobs: ErasedJob[], decisions: ErasedDecision[], redactions: ErasedDecision[] = []): D1Sql[] {
  const result: D1Sql[] = [];
  for (const chunk of jsonChunks(jobs, 500000)) {
    const selected=new Map<string,ErasedJob>(JSON.parse(chunk).map((row:ErasedJob)=>[row.id,row]));
    result.push(changed(sql`UPDATE ik_jobs SET plan_json=(SELECT json_extract(value,'$.body') FROM json_each(${chunk}) WHERE json_extract(value,'$.id')=ik_jobs.id),fingerprint=(SELECT json_extract(value,'$.fingerprint') FROM json_each(${chunk}) WHERE json_extract(value,'$.id')=ik_jobs.id) WHERE id IN (SELECT json_extract(value,'$.id') FROM json_each(${chunk}))`,updateRows('ik_jobs',row=>selected.has(row.id),row=>({...row,plan_json:selected.get(row.id)!.body,fingerprint:selected.get(row.id)!.fingerprint}))));
    result.push(changed(sql`UPDATE ik_jobs SET reversal_policy_hash=(SELECT json_extract(value,'$.policyHash') FROM json_each(${chunk}) WHERE json_extract(value,'$.id')=ik_jobs.id) WHERE id IN (SELECT json_extract(value,'$.id') FROM json_each(${chunk}))`,updateRows('ik_jobs',row=>selected.has(row.id),row=>({...row,reversal_policy_hash:selected.get(row.id)!.policyHash}))));
    result.push(changed(sql`UPDATE ik_d1_indexed_jobs SET fingerprint=(SELECT json_extract(value,'$.fingerprint') FROM json_each(${chunk}) WHERE json_extract(value,'$.id')=job_id) WHERE job_id IN (SELECT json_extract(value,'$.id') FROM json_each(${chunk}))`,updateRows('ik_d1_indexed_jobs',row=>selected.has(row.job_id),row=>({...row,fingerprint:selected.get(row.job_id)!.fingerprint}))));
    result.push(changed(sql`UPDATE ik_d1_stream_pages SET erased_records=(SELECT json_extract(value,'$.erasedRecords') FROM json_each(${chunk}) WHERE json_extract(value,'$.id')=job_id) WHERE job_id IN (SELECT json_extract(value,'$.id') FROM json_each(${chunk}))`,updateRows('ik_d1_stream_pages',row=>selected.has(row.job_id),row=>({...row,erased_records:selected.get(row.job_id)!.erasedRecords}))));
  }
  for (const chunk of jsonChunks(decisions, 500000)) {
    const selected:ErasedDecision[]=JSON.parse(chunk);
    const match=(row:any)=>selected.some(decision=>decision.job_id===row.job_id&&decision.ordinal===row.ordinal);
    for (const table of ['ik_provenance', 'ik_d1_history']) result.push(changed(retainedSql([`DELETE FROM ${table} WHERE EXISTS(SELECT 1 FROM json_each(`,") WHERE json_extract(value,'$.job_id')=job_id AND json_extract(value,'$.ordinal')=ordinal)"],[chunk],[0]),deleteRows(table,match)));
  }
  // Updates join each packed chunk by key instead of looking every row up in it:
  // a correlated lookup costs rows x chunk, which took seconds at a few thousand.
  for (const chunk of jsonChunks([...decisions, ...redactions], 500000)) {
    const selected:ErasedDecision[]=JSON.parse(chunk),byEntry=new Map(selected.map(row=>[`${row.job_id} ${row.ordinal}`,row]));
    // A source shared by several jobs keeps one claim, owned by one job; its
    // entries can differ per job (a redacted entry keeps each job's own effects),
    // so the claim takes the hashes of its own job's decision.
    const bySource=new Map(selected.map(row=>[`${row.identity} ${row.job_id}`,row])),sources=JSON.stringify([...bySource.values()]);
    result.push(changed(sql`UPDATE ik_sources SET source_hash=d.source_hash,effects_hash=d.effects_hash FROM (SELECT json_extract(value,'$.identity') AS identity,json_extract(value,'$.job_id') AS job_id,json_extract(value,'$.source_hash') AS source_hash,json_extract(value,'$.effects_hash') AS effects_hash FROM json_each(${sources})) AS d WHERE ik_sources.identity=d.identity AND ik_sources.job_id=d.job_id`,updateRows('ik_sources',row=>bySource.has(`${row.identity} ${row.job_id}`),row=>({...row,source_hash:bySource.get(`${row.identity} ${row.job_id}`)!.source_hash,effects_hash:bySource.get(`${row.identity} ${row.job_id}`)!.effects_hash}))));
    result.push(changed(sql`UPDATE ik_d1_entries SET effects_hash=d.effects_hash FROM (SELECT json_extract(value,'$.job_id') AS job_id,json_extract(value,'$.ordinal') AS ordinal,json_extract(value,'$.effects_hash') AS effects_hash FROM json_each(${chunk})) AS d WHERE ik_d1_entries.job_id=d.job_id AND ik_d1_entries.ordinal=d.ordinal`,updateRows('ik_d1_entries',row=>byEntry.has(`${row.job_id} ${row.ordinal}`),row=>({...row,effects_hash:byEntry.get(`${row.job_id} ${row.ordinal}`)!.effects_hash}))));
    result.push(changed(sql`DELETE FROM ik_sources WHERE identity IN (SELECT json_extract(value,'$.identity') FROM json_each(${chunk})) AND job_id IN (SELECT id FROM ik_jobs WHERE state IN ('undone','rolled_back')) AND NOT EXISTS(SELECT 1 FROM ik_provenance p WHERE p.source_identity=ik_sources.identity AND p.reversed=0) AND NOT EXISTS(SELECT 1 FROM ik_d1_history h WHERE h.source_identity=ik_sources.identity)`,deleteRows('ik_sources',(row,state)=>selected.some(decision=>decision.identity===row.identity)&&[...state.rows('ik_jobs').values()].some(job=>job.id===row.job_id&&['undone','rolled_back'].includes(job.state))&&![...state.rows('ik_provenance').values()].some(p=>p.source_identity===row.identity&&!p.reversed)&&![...state.rows('ik_d1_history').values()].some(p=>p.source_identity===row.identity))));
  }
  return result;
}
