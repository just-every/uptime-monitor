import { Buffer } from 'node:buffer';
import type { D1Database, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { retainedSql } from './retained-values.ts';
import { changedLazy, putRow } from './expected-state.ts';

// JSON snapshots are split by UTF-8 size, never by source count alone. Each
// statement compares the original column values, not their recorded hashes.
export const SNAPSHOT_BYTES = 128 * 1024;
export const PACKED_JSON_BYTES = 1900000;
export function jsonChunks<T>(rows: T[], maxBytes = SNAPSHOT_BYTES): string[] {
  const chunks: string[] = []; let parts: string[] = [], size = 2;
  for (const row of rows) {
    const value = JSON.stringify(row), bytes = Buffer.byteLength(value) + 1;
    if (bytes + 1 > PACKED_JSON_BYTES) throw new Error('IMPORT_D1_VALUE_LIMIT');
    if (parts.length && size + bytes > maxBytes) { chunks.push(`[${parts.join(',')}]`); parts = []; size = 2; }
    parts.push(value); size += bytes;
  }
  if (parts.length) chunks.push(`[${parts.join(',')}]`);
  return chunks;
}
export async function selectKeys(db: D1Database, table: string, key: string, values: string[], fence?:D1Sql): Promise<any[]> {
  const rows: any[] = [], keys = [...new Set(values)];
  for (const chunk of jsonChunks(keys)) {
    // A singleton needs no JSON table. Keep the same size admission, result
    // normalization and optional snapshot fence as the bounded set path.
    const singleton = keys.length === 1;
    const result = await db.prepare(`SELECT ${table === 'ik_originals' ? "*,CASE WHEN typeof(name)='text' THEN json_quote(name) ELSE NULL END AS name_json" : '*'} FROM ${table} WHERE ${key}${singleton ? '=?' : ' IN (SELECT value FROM json_each(?))'}${fence?` AND (${fence.sql})`:''}`).bind(singleton ? keys[0] : chunk,...fence?.params??[]).all();
    rows.push(...result.results.map(row => table === 'ik_originals' ? originalRow(row) : row));
  }
  return rows;
}
/** Close a selected-key snapshot against additions, including empty owner sets.
 * Pair these counts with exactRows for the existing rows' complete fields. */
export function selectedCounts(table: string, key: string, values: string[], rows: any[]): D1Sql[] {
  const counts = new Map<string, number>();
  for (const row of rows) counts.set(row[key], (counts.get(row[key]) ?? 0) + 1);
  return jsonChunks([...new Set(values)]).map(chunk => {
    const selected = JSON.parse(chunk);
    const result = guard(sql(`(SELECT COUNT(*) FROM ${table} WHERE ${key} IN (SELECT value FROM json_each(?)))=?`, chunk, selected.reduce((n: number, value: string) => n + (counts.get(value) ?? 0), 0)));
    result.expectedSnapshots = [{table,columns:[],rows:[],selection:{key,values:selected}}];
    return result;
  });
}
/** Full-field comparison plus a separate scoped cardinality fence. Callers using
 * a selected-key closure guard that closure's counts, not unrelated rows. */
export function exactRows(table: string, columns: string[], rows: any[], scope?: D1Sql, maxBytes = SNAPSHOT_BYTES, retain = !['ik_originals','ik_d1_original_chunks'].includes(table), selection?: {key:string;values:string[]}): D1Sql[] {
  const result: D1Sql[] = [];
  if (scope) result.push(guard(sql(`(SELECT COUNT(*) FROM ${table} WHERE ${scope.sql})=?`, ...(scope.params ?? []), rows.length)));
  // Very large old inline plans must not grow beyond D1's value ceiling merely
  // because JSON encoding escapes them a second time.
  const ordinary: any[] = [];
  for (const row of rows) {
    const picked = Object.fromEntries(columns.map(c => [c, row[c]]));
    if (Buffer.byteLength(JSON.stringify(picked)) + 2 > maxBytes) {
      const parts=columns.map((c,i)=>`${i?' AND ':`EXISTS(SELECT 1 FROM ${table} t WHERE `}t.${c} IS `).concat(')'), params=columns.map(c=>row[c]);
      result.push(guard(retain?retainedSql(parts,params,params.flatMap((value,i)=>typeof value==='string'?[i]:[])):sql(parts.join('?'),...params)));
    } else ordinary.push(picked);
  }
  for (const chunk of jsonChunks(ordinary, maxBytes)) {
    const parts=['NOT EXISTS(SELECT 1 FROM json_each(',`) e WHERE NOT EXISTS(SELECT 1 FROM ${table} t WHERE ${columns.map(c => `t.${c} IS json_extract(e.value,'$.${c}')`).join(' AND ')}))`];
    result.push(guard(retain?retainedSql(parts,[chunk],[0]):sql(parts.join('?'),chunk)));
  }
  if (result.length) result[0].expectedSnapshots = [{table,columns,rows,...(selection?{selection}:{})}];
  return result;
}
export const jobColumns = ['id','destination','namespace','mapping_version','principal_id','fingerprint','operation_key','plan_json','state','checkpoint','total','fence','lease_token','lease_expires','error','reversal_policy','reversal_policy_hash','reversal_cursor','reversal_hash'];
export const provenanceColumns = ['id','job_id','ordinal','effect_ordinal','source_identity','destination','target_kind','target_id','reversed','record_json'];
export const claimColumns = ['identity','source_hash','mapping_version','effects_hash','job_id'];

/** One bound JSON value per statement; SQLite preserves strings as TEXT even
 * in BLOB-affinity columns. Column names are internal constants only. */
export function insertJsonRows(table: string, columns: string[], rows: any[]): D1Sql[] {
  return jsonChunks(rows, PACKED_JSON_BYTES).map(chunk => changedLazy(retainedSql(
    [`INSERT INTO ${table}(${columns.join(',')}) SELECT ${columns.map(c => `json_extract(value,'$.${c}')`).join(',')} FROM json_each(`,')'],[chunk],['ik_originals','ik_d1_original_chunks'].includes(table)?[]:[0]),()=>JSON.parse(chunk).map((row:any)=>putRow(table,row))));
}

function originalRow(row:any):any{if(row.name_json===null)throw new Error('ORIGINAL_CUSTODY_MISMATCH');return {...row,name:JSON.parse(row.name_json)};}
