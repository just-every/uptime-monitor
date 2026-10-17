import { copyStatement } from './guards.ts';
import type { D1Sql } from './types.ts';
import { guard } from './database.ts';
import { retainedSql } from './retained-values.ts';
import { exactRows, selectedCounts, jsonChunks } from './sets.ts';

type Row = Record<string, any>;
const keys: Record<string, string[]> = {
  ik_jobs: ['id'], ik_provenance: ['id'], ik_sources: ['identity'],
  ik_d1_history: ['id'], ik_d1_entries: ['job_id','ordinal'],
  ik_batches: ['job_id','action','position'], ik_original_sets: ['job_id'],
  ik_originals: ['job_id','kind','original_key'],
  ik_d1_original_chunks: ['job_id','kind','original_key','ordinal'],
  ik_d1_plan_chunks: ['job_id','ordinal'], ik_d1_reversal_reports: ['job_id','effect_id'],
  ik_d1_indexed_jobs: ['job_id'], ik_d1_plan_index: ['job_id','ordinal','target_identity'],
  ik_d1_stream_pages: ['stream_id','ordinal'], ik_d1_streams: ['id'],
};
export interface ExpectedSnapshot { table: string; columns: string[]; rows: Row[]; selection?: { key: string; values: string[] } }
export interface ExpectedChange { table: string; apply(rows: Map<string, Row>, state: ExpectedState): void }
const identity = (table: string, row: Row) => JSON.stringify(keys[table].map(key => row[key]));

/** Transaction-local intended state. Snapshots come from the already validated
 * read, and edits come from explicit maintained mutations, never observed SQL
 * after a write. No SQL parsing or newly read revision can bless changed data. */
export class ExpectedState {
  private tables = new Map<string, Map<string, Row>>();
  private deleted = new Map<string, Map<string, Row>>();
  private selections: { table: string; key: string; values: string[] }[] = [];
  constructor(statements: D1Sql[]) {
    for (const statement of statements) for (const snapshot of statement.expectedSnapshots ?? []) {
      if (!keys[snapshot.table]) continue;
      const rows = this.rows(snapshot.table);
      for (const row of snapshot.rows) {
        const id = identity(snapshot.table,row), prior = rows.get(id) ?? {};
        // A later assertion may already describe the intended after-state.
        // Keep the first complete preflight fields; mutations below own changes.
        for (const column of snapshot.columns) if (!(column in prior)) prior[column] = row[column];
        rows.set(id, prior);
      }
      if (snapshot.selection) this.selections.push({table:snapshot.table,...snapshot.selection});
    }
    for (const statement of statements) for (const change of typeof statement.expectedChanges==='function'?statement.expectedChanges():statement.expectedChanges??[]) change.apply(this.rows(change.table), this);
  }
  rows(table: string): Map<string, Row> {
    let rows = this.tables.get(table); if (!rows) this.tables.set(table, rows = new Map()); return rows;
  }
  put(table: string, row: Row): void {
    const id=identity(table,row);this.deleted.get(table)?.delete(id);
    this.rows(table).set(id, {...row});
  }
  remove(table:string,id:string,row:Row):void{
    let deleted=this.deleted.get(table);if(!deleted)this.deleted.set(table,deleted=new Map());
    deleted.set(id,Object.fromEntries([...new Set([...keys[table],'job_id'])].filter(key=>key in row).map(key=>[key,row[key]])));
    this.rows(table).delete(id);
  }
  guards(excludeJobs:ReadonlySet<string>=new Set()): D1Sql[] {
    const result: D1Sql[] = [];
    for (const [table, rows] of this.tables) {
      // Related jobs can have different validated field projections. Compare
      // every known field without inventing NULL values for unread columns.
      const groups=new Map<string,{columns:string[];rows:Row[]}>();
      for(const row of rows.values()){
        if(excludeJobs.has(table==='ik_jobs'?row.id:row.job_id))continue;
        const columns=Object.keys(row).sort(),key=JSON.stringify(columns);
        let group=groups.get(key);if(!group)groups.set(key,group={columns,rows:[]});group.rows.push(row);
      }
      for(const {columns,rows:group} of groups.values())if(columns.length)result.push(...exactRows(table,columns,group,undefined,undefined,!['ik_originals','ik_d1_original_chunks'].includes(table)));
    }
    for(const [table,deleted] of this.deleted){
      const rows=[...deleted.values()].filter(row=>!excludeJobs.has(table==='ik_jobs'?row.id:row.job_id));
      for(const chunk of jsonChunks(rows))result.push(guard(retainedSql(['NOT EXISTS(SELECT 1 FROM json_each(',`) e JOIN ${table} t ON ${keys[table].map(key=>`t.${key} IS json_extract(e.value,'$.${key}')`).join(' AND ')})`],[chunk],[0])));
    }
    const seen=new Set<string>();
    for(let selection of this.selections){
      if(selection.key==='job_id'||selection.table==='ik_jobs'&&selection.key==='id')selection={...selection,values:selection.values.filter(id=>!excludeJobs.has(id))};
      const stamp=JSON.stringify(selection);if(seen.has(stamp))continue;seen.add(stamp);
      result.push(...selectedCounts(selection.table,selection.key,selection.values,[...this.rows(selection.table).values()]));
    }
    return result;
  }
}
export function changed(query: D1Sql, ...changes: ExpectedChange[]): D1Sql { const result=copyStatement(query); result.expectedChanges=changes; return result; }
export function changedLazy(query:D1Sql,changes:()=>ExpectedChange[]):D1Sql{const result=copyStatement(query);result.expectedChanges=changes;return result;}
export function putRow(table: string, row: Row): ExpectedChange { return {table,apply(_rows,state){state.put(table,row)}}; }
export function updateRows(table: string, matches: (row: Row) => boolean, update: (row: Row) => Row): ExpectedChange {
  return {table,apply(rows,state){for(const [id,row] of [...rows])if(matches(row)){state.remove(table,id,row);state.put(table,update({...row}));}}};
}
export function deleteRows(table: string, matches: (row: Row, state: ExpectedState) => boolean): ExpectedChange {
  return {table,apply(rows,state){for(const [id,row] of rows)if(matches(row,state))state.remove(table,id,row);}};
}
