import type { D1Database, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';

// Bookkeeping that every custody change bumps, plus the erasure's own records.
const SKIPPED = new Set(['ik_d1_assertions', 'ik_d1_custody_states', 'ik_d1_custody_revisions', 'ik_d1_publication_fences',
  'ik_d1_erasure_tally', 'ik_d1_erasure_journal', 'ik_d1_erasure_progress', 'ik_d1_retirements', 'ik_d1_erased_sources', 'ik_d1_erased_targets', 'ik_targets']);
const COUNTERS = new Set(['revision', 'layout_revision', 'progress_revision', 'seal']);

export interface TallyScope { destination: string; jobs: string[]; streams: string[] }
interface Store { name: string; image: string; where: string; params: (scope: TallyScope) => unknown[] }

/** Each custody store with the row image the journal counts and the rows an
 * erasure of these jobs and streams can touch. */
export async function tallyStores(db: D1Database): Promise<Store[]> {
  const { results } = await db.prepare("SELECT m.name AS store, p.name AS column, p.type AS type FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table' AND m.name LIKE 'ik\\_%' ESCAPE '\\' ORDER BY m.name, p.cid").all<{ store: string; column: string; type: string }>();
  const columns = new Map<string, { column: string; type: string }[]>();
  for (const row of results) if (!SKIPPED.has(row.store)) columns.set(row.store, [...columns.get(row.store) ?? [], row]);
  const jobs = (scope: TallyScope) => [JSON.stringify(scope.jobs)], streams = (scope: TallyScope) => [JSON.stringify(scope.streams)];
  const destination = (scope: TallyScope) => [scope.destination];
  return [...columns].map(([name, list]) => {
    const names = list.map(item => item.column);
    const image = `json_array(${list.filter(item => !COUNTERS.has(item.column)).map(item => /BLOB/i.test(item.type) ? `hex("${item.column}")` : `"${item.column}"`).join(',')})`;
    if (name === 'ik_jobs') return { name, image, where: 'id IN (SELECT value FROM json_each(?))', params: jobs };
    if (name === 'ik_d1_streams') return { name, image, where: 'id IN (SELECT value FROM json_each(?))', params: streams };
    if (name === 'ik_d1_stream_revisions') return { name, image, where: 'stream_id IN (SELECT value FROM json_each(?))', params: streams };
    if (names.includes('stage_id')) {
      const parent = name.startsWith('ik_d1_native_staging') ? 'ik_d1_native_staging' : 'ik_d1_staging';
      return { name, image, where: `stage_id IN (SELECT id FROM ${parent} WHERE destination=?)`, params: destination };
    }
    if (names.includes('job_id')) return { name, image, where: 'job_id IN (SELECT value FROM json_each(?))', params: jobs };
    if (names.includes('destination')) return { name, image, where: 'destination=?', params: destination };
    throw new Error(`IMPORT_RETIREMENT_UNSCOPED_STORE:${name}`);
  });
}

/** Images of every row in scope, taken first in the erasure batch. */
export function tallyBefore(stores: Store[], receipt: string, scope: TallyScope): D1Sql[] {
  return stores.map(store => sql(`INSERT INTO ik_d1_erasure_tally(receipt_id,store,image) SELECT ?,?,${store.image} FROM ${store.name} WHERE ${store.where}`, receipt, store.name, ...store.params(scope)));
}

/** Drops the images still present unchanged; the rest were deleted or rewritten.
 * Unless the batch carries the restore checkpoint, none may remain. The journal
 * row then counts them per store, and the tally is emptied. */
export function tallyJournal(stores: Store[], receipt: string, scope: TallyScope, entry: { recordedAt: string; recordsErased: number; jobsRetired: number; streamsRetired: number }, fenced: boolean): D1Sql[] {
  return [
    ...stores.map(store => sql(`DELETE FROM ik_d1_erasure_tally WHERE receipt_id=? AND store=? AND image IN (SELECT ${store.image} FROM ${store.name} WHERE ${store.where})`, receipt, store.name, ...store.params(scope))),
    ...fenced ? [] : [guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_erasure_tally WHERE receipt_id=?)', receipt))],
    sql("INSERT INTO ik_d1_erasure_journal(id,destination,recorded_at,records_erased,jobs_retired,streams_retired,stores_json) SELECT ?,?,?,?,?,?,(SELECT json_group_array(json_object('store',store,'rows',rows)) FROM (SELECT store, count(*) AS rows FROM ik_d1_erasure_tally WHERE receipt_id=? GROUP BY store ORDER BY store))",
      receipt, scope.destination, entry.recordedAt, entry.recordsErased, entry.jobsRetired, entry.streamsRetired, receipt),
    sql('DELETE FROM ik_d1_erasure_tally WHERE receipt_id=?', receipt),
  ];
}
