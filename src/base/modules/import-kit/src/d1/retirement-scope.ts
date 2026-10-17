import type { D1Database, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { stepGenerations } from './staging-lifecycle.ts';

const among = 'IN (SELECT value FROM json_each(?))';
// Every header cell a product can fill from what a person typed or uploaded.
export const headerCells: { table: string; columns: string[]; destination: string }[] = [
  { table: 'ik_jobs', columns: ['namespace', 'operation_key'], destination: 'destination' },
  { table: 'ik_d1_streams', columns: ['namespace', 'operation_key'], destination: 'destination' },
  { table: 'ik_d1_stream_revisions', columns: ['namespace', 'operation_key'], destination: 'destination' },
  { table: 'ik_d1_stream_origins', columns: ['parent_namespace', 'parent_operation_key', 'job_operation_key'], destination: 'parent_destination' },
  { table: 'ik_d1_staging', columns: ['namespace', 'operation_key'], destination: 'destination' },
  { table: 'ik_d1_native_staging', columns: ['namespace'], destination: 'destination' },
  { table: 'ik_d1_preparation_steps', columns: ['namespace', 'operation_key'], destination: 'destination' },
];
const matching = (columns: string[]) => '(' + columns.map(column => `${column} ${among}`).join(' OR ') + ')';
const params = (columns: string[], values: string) => columns.map(() => values);

export interface RetirementScope { jobs: string[]; streams: string[]; stages: string[]; anchors: string[]; preparations: string[] }

/** Jobs, streams and private stages whose headers hold one of the exact values,
 * closed over everything their deletion would break: whole streams, jobs whose
 * entries reuse a retired job's sources, and preserved history in both directions. */
export async function retirementScope(db: D1Database, destination: string, values: string[]): Promise<RetirementScope> {
  const jobs = new Set<string>(), streams = new Set<string>(), stages = new Set<string>(), anchors = new Set<string>(), preparations = new Set<string>();
  const exact = JSON.stringify(values);
  const read = async (query: string, ...bound: unknown[]) => (await db.prepare(query).bind(...bound).all<{ id: string }>()).results.map(row => row.id);
  if (values.length) {
    for (const id of await read(`SELECT id FROM ik_jobs WHERE destination=? AND ${matching(['namespace', 'operation_key'])}`, destination, exact, exact)) jobs.add(id);
    for (const id of await read(`SELECT DISTINCT o.job_id AS id FROM ik_originals o JOIN ik_jobs j ON j.id=o.job_id WHERE j.destination=? AND o.name ${among}`, destination, exact)) jobs.add(id);
    for (const id of await read(`SELECT id FROM ik_d1_streams WHERE destination=? AND ${matching(['namespace', 'operation_key'])}`, destination, exact, exact)) streams.add(id);
    for (const id of await read(`SELECT stream_id AS id FROM ik_d1_stream_origins WHERE parent_destination=? AND ${matching(['parent_namespace', 'parent_operation_key', 'job_operation_key'])}`, destination, exact, exact, exact)) streams.add(id);
    for (const id of await read(`SELECT stream_id AS id FROM ik_d1_stream_revisions WHERE destination=? AND ${matching(['namespace', 'operation_key'])}`, destination, exact, exact)) anchors.add(id);
    for (const id of await read(`SELECT id FROM ik_d1_staging WHERE destination=? AND ${matching(['namespace', 'operation_key'])} UNION SELECT stage_id FROM ik_d1_staging_originals WHERE name ${among} AND stage_id IN (SELECT id FROM ik_d1_staging WHERE destination=?)`, destination, exact, exact, exact, destination)) stages.add(id);
    // A preparation in steps goes whole: its control row and every generation under its internal keys.
    for (const row of (await db.prepare(`SELECT id,destination,principal_id FROM ik_d1_preparation_steps WHERE destination=? AND ${matching(['namespace', 'operation_key'])}`).bind(destination, exact, exact).all<{ id: string; destination: string; principal_id: string }>()).results) {
      preparations.add(row.id);
      const generations = stepGenerations('SELECT id FROM ik_d1_staging', row);
      for (const id of await read(generations.sql, ...generations.params!)) stages.add(id);
    }
  }
  for (let size = -1; size !== jobs.size + streams.size;) {
    size = jobs.size + streams.size;
    const j = JSON.stringify([...jobs]), s = JSON.stringify([...streams]);
    for (const id of await read(`SELECT job_id AS id FROM ik_d1_stream_pages WHERE stream_id ${among} UNION SELECT job_id FROM ik_d1_stream_origins WHERE stream_id ${among}`, s, s)) jobs.add(id);
    for (const id of await read(`SELECT stream_id AS id FROM ik_d1_stream_pages WHERE job_id ${among} UNION SELECT stream_id FROM ik_d1_stream_origins WHERE job_id ${among}`, j, j)) streams.add(id);
    for (const id of await read(`SELECT job_id AS id FROM ik_d1_entries WHERE owner_job_id ${among}`, j)) jobs.add(id);
    for (const id of await read(`SELECT h.job_id AS id FROM ik_d1_history h JOIN ik_provenance p ON p.id=h.successor_id WHERE p.job_id ${among} UNION SELECT p.job_id FROM ik_d1_history h JOIN ik_provenance p ON p.id=h.successor_id WHERE h.job_id ${among}`, j, j)) jobs.add(id);
  }
  for (const id of streams) anchors.add(id);
  return { jobs: [...jobs].sort(), streams: [...streams].sort(), stages: [...stages].sort(), anchors: [...anchors].sort(), preparations: [...preparations].sort() };
}

/** Commit-time proof that the closure read above is still the whole closure. */
export function scopeGuards(scope: RetirementScope): D1Sql[] {
  const j = JSON.stringify(scope.jobs), s = JSON.stringify(scope.streams);
  return [
    guard(sql(`(SELECT count(*) FROM ik_jobs WHERE id ${among})=?`, j, scope.jobs.length)),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages WHERE (job_id ${among}) <> (stream_id ${among}))`, j, s)),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_entries WHERE owner_job_id ${among} AND job_id NOT ${among})`, j, j)),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_history h JOIN ik_provenance p ON p.id=h.successor_id WHERE (h.job_id ${among}) <> (p.job_id ${among}))`, j, j)),
  ];
}

/** Deletes the whole custody of the retired jobs and streams, children first,
 * and rewrites each retired stream's revision anchor to drop its header. */
export function retirementWrites(scope: RetirementScope, destination: string, receipt: string, retiredAt: string): D1Sql[] {
  const j = JSON.stringify(scope.jobs), s = JSON.stringify(scope.streams);
  const byJob = ['ik_d1_stream_pages', 'ik_d1_plan_index', 'ik_d1_indexed_jobs', 'ik_d1_entries', 'ik_d1_history', 'ik_provenance', 'ik_sources', 'ik_stages', 'ik_batches',
    'ik_d1_original_chunks', 'ik_originals', 'ik_original_sets', 'ik_d1_plan_chunks', 'ik_d1_reversal_reports', 'ik_d1_native_staging'];
  return [
    sql(`UPDATE ik_d1_creation_plans SET released=1 WHERE job_id ${among} AND released=0`, j),
    sql(`DELETE FROM ik_d1_creation_plans WHERE job_id ${among}`, j),
    ...byJob.map(table => sql(`DELETE FROM ${table} WHERE job_id ${among}`, j)),
    sql(`DELETE FROM ik_jobs WHERE id ${among}`, j),
    sql(`DELETE FROM ik_d1_streams WHERE id ${among}`, s),
    sql(`DELETE FROM ik_d1_staging WHERE id ${among}`, JSON.stringify(scope.stages)),
    sql(`DELETE FROM ik_d1_preparation_steps WHERE id ${among}`, JSON.stringify(scope.preparations)),
    sql(`INSERT INTO ik_d1_retirements(id,kind,destination,receipt_id,retired_at) SELECT value,'job',?,?,? FROM json_each(?)`, destination, receipt, retiredAt, j),
    sql(`INSERT INTO ik_d1_retirements(id,kind,destination,receipt_id,retired_at) SELECT value,'stream',?,?,? FROM json_each(?) WHERE NOT EXISTS(SELECT 1 FROM ik_d1_retirements WHERE id=value AND kind='stream')`, destination, receipt, retiredAt, JSON.stringify(scope.anchors)),
    sql(`UPDATE ik_d1_stream_revisions SET namespace='',operation_key='',progress_revision=progress_revision+1 WHERE stream_id ${among} AND (namespace<>'' OR operation_key<>'')`, JSON.stringify(scope.anchors)),
  ];
}

/** No header cell in this destination equals a named value after the batch. */
export function headerAbsence(destination: string, values: string[]): D1Sql[] {
  if (!values.length) return [];
  const exact = JSON.stringify(values);
  return [
    ...headerCells.map(({ table, columns, destination: column }) => guard(sql(`NOT EXISTS(SELECT 1 FROM ${table} WHERE ${column}=? AND ${matching(columns)})`, destination, ...params(columns, exact)))),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_originals o JOIN ik_jobs j ON j.id=o.job_id WHERE j.destination=? AND o.name ${among})`, destination, exact)),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_staging_originals o JOIN ik_d1_staging g ON g.id=o.stage_id WHERE g.destination=? AND o.name ${among})`, destination, exact)),
  ];
}
