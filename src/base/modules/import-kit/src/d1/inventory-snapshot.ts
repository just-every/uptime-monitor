import type { ImportPlan } from '../types.ts';
import type { D1Database, D1PlanCodec,D1Sql } from './types.ts';
import { selectKeys,jsonChunks } from './sets.ts';
import { loadPlans } from './plans.ts';
import { decodeProvenance } from '../storage/provenance.ts';
import type { D1ProvenanceRow } from './types.ts';

/** A single publication's real database snapshot, shared by all affected jobs.
 * Validation emits exact row and set guards; nothing survives the invocation. */
export class InventorySnapshot {
  readonly tables = new Map<string, any[]>();
  readonly plans = new Map<string, ImportPlan>();
  readonly checkedPlans = new Set<string>();
  private indexes = new Map<string, Map<string, any[]>>();
  rows(table: string, key: string, values: string[]): any[] {
    if (!this.tables.has(table)) throw new Error('IMPORT_INVENTORY_SNAPSHOT_MISSING');
    const name = `${table}.${key}`;
    let index = this.indexes.get(name);
    if (!index) {
      index = new Map();
      for (const row of this.tables.get(table)!) { const rows = index.get(row[key]) ?? []; rows.push(row); index.set(row[key], rows); }
      this.indexes.set(name, index);
    }
    return [...new Set(values)].flatMap(value => index!.get(value) ?? []);
  }
}
export async function readInventorySnapshot(ids: string[], db: D1Database, codec?: D1PlanCodec, revisions=false,fence?:D1Sql): Promise<InventorySnapshot> {
  const snapshot = new InventorySnapshot(), owners = new Set(ids);
  const read=async(table:string,key:string,values:string[])=>{const rows=await selectKeys(db,table,key,values,fence);return revisions?orderedRows(rows):rows;};
  let pending = ids, entries: any[] = [];
  while (pending.length) {
    const found = await read('ik_d1_entries', 'job_id', pending); entries.push(...found);
    pending = [...new Set(found.map(row => row.owner_job_id))].filter(id => !owners.has(id));
    for (const id of pending) owners.add(id);
  }
  const allIds = [...owners]; snapshot.tables.set('ik_d1_entries', entries);
  for (const table of ['ik_jobs', 'ik_provenance', 'ik_d1_history', 'ik_sources', 'ik_batches', 'ik_original_sets', 'ik_originals', 'ik_d1_original_chunks']) {
    snapshot.tables.set(table, table==='ik_jobs'&&revisions?await revisionJobs(allIds,db,fence):await read(table, table === 'ik_jobs' ? 'id' : 'job_id', allIds));
  }
  // Preserved undo validates every still-owned predecessor. Those owners need
  // not be entry-reuse owners, so close their durable history independently.
  const jobs = snapshot.tables.get('ik_jobs')!, history = snapshot.tables.get('ik_d1_history')!;
  const preserved = new Set(jobs.filter(job => job.reversal_policy === 'preserve-conflicts').map(job => job.id));
  let frontier = [...snapshot.tables.get('ik_provenance')!, ...history].filter(row => preserved.has(row.job_id));
  const knownHistory = new Set(history.map(row => row.id)), knownOwners = new Set(jobs.map(row => row.id));
  while (frontier.length) {
    const ids = frontier.filter(row => !row.reversed).map(row => (decodeProvenance(row) as D1ProvenanceRow).predecessorId).filter((id): id is string => id !== undefined && !knownHistory.has(id));
    frontier = await read('ik_d1_history', 'id', ids);
    for (const row of frontier) { history.push(row); knownHistory.add(row.id); }
    const missing = [...new Set(frontier.map(row => row.job_id))].filter(id => !knownOwners.has(id));
    for (const owner of await (revisions?revisionJobs(missing,db,fence):read('ik_jobs', 'id', missing))) { jobs.push(owner); knownOwners.add(owner.id); }
  }
  const provenance = [...snapshot.tables.get('ik_provenance')!, ...snapshot.tables.get('ik_d1_history')!];
  const successors = [...new Set(provenance.filter(row => row.successor_id !== undefined).map(row => row.successor_id))];
  for (const table of ['ik_provenance', 'ik_d1_history']) {
    const rows = snapshot.tables.get(table)!, known = new Set(rows.map(row => row.id));
    rows.push(...(await read(table, 'id', successors)).filter(row => !known.has(row.id)));
  }
  const claims = snapshot.tables.get('ik_sources')!, known = new Set(claims.map(row => row.identity));
  claims.push(...(await read('ik_sources', 'identity', [...entries.map(row => row.source_identity), ...provenance.map(row => row.source_identity)])).filter(row => !known.has(row.identity)));
  if(revisions){
    for(const job of jobs)if(!Number.isSafeInteger(job.custody_revision)||!Number.isSafeInteger(job.state_revision))throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');
    const reports:any[]=[];
    for(const chunk of jsonChunks(jobs.map(row=>row.id)))reports.push(...(await db.prepare(`SELECT r.*,p.reversed AS native_reversed FROM ik_d1_reversal_reports r LEFT JOIN ik_provenance p ON p.id=r.effect_id AND p.job_id=r.job_id WHERE r.job_id IN (SELECT value FROM json_each(?))${fence?` AND (${fence.sql})`:''} ORDER BY r.job_id,r.position`).bind(chunk,...fence?.params??[]).all()).results);
    snapshot.tables.set('ik_d1_reversal_reports',reports);
    snapshot.tables.set('ik_d1_stream_pages',await read('ik_d1_stream_pages','job_id',jobs.map(row=>row.id)));
  }
  if(revisions)for(const rows of snapshot.tables.values())orderedRows(rows);
  for (const [id, plan] of await loadPlans(snapshot.tables.get('ik_jobs')!, db, codec,fence)) snapshot.plans.set(id, plan);
  return snapshot;
}

async function revisionJobs(ids:string[],db:D1Database,fence?:D1Sql):Promise<any[]>{
  const rows:any[]=[];
  for(const chunk of jsonChunks(ids))rows.push(...(await db.prepare(`SELECT j.*,r.revision AS custody_revision,s.revision AS state_revision FROM ik_jobs j LEFT JOIN ik_d1_custody_revisions r ON r.job_id=j.id LEFT JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE j.id IN (SELECT value FROM json_each(?))${fence?` AND (${fence.sql})`:''}`).bind(chunk,...fence?.params??[]).all()).results);
  return orderedRows(rows);
}

// Only primary-key scalars participate; repeated bounded passes never depend
// on unspecified database row order or compare complete private JSON bodies.
function orderedRows(rows:any[]):any[]{
  const columns=['job_id','id','identity','stream_id','ordinal','effect_id','action','position','kind','original_key'];
  return rows.sort((a,b)=>{for(const column of columns){const x=a[column],y=b[column];if(x===y)continue;if(x===undefined)return -1;if(y===undefined)return 1;return x<y?-1:1;}return 0;});
}
