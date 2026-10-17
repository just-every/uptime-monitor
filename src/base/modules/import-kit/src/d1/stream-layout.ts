import type { ImportAction, ImportJob } from '../types.ts';
import type { D1Database, D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { needsStreamOrigin, streamOriginsMatch } from './stream-origin.ts';
import { readStreamReceipt } from './stream-receipt.ts';

// Retain only scalar SQL bindings. No page/job arrays, plan or authority result
// is cached. Every use still executes the predicate in its current transaction.
const layouts = new WeakMap<ImportJob, { predicate: D1Sql; reads: number }>();
const contexts = new WeakMap<D1ImportContext['authorization'], { database: D1Database; streamId: string; predicate: D1Sql }>();
export function rememberStreamContext(context: D1ImportContext, streamId: string, predicate: D1Sql): void {
  contexts.set(context.authorization, { database: context.storage.database, streamId, predicate });
}
/** Current direction and immutable layout remain fixed across child progress. */
export function streamSnapshotPredicate(row: any, progress = false): D1Sql {
  return sql(`EXISTS(SELECT 1 FROM ik_d1_streams s JOIN ik_d1_stream_revisions r ON r.stream_id=s.id WHERE s.id=? AND s.destination=? AND s.namespace=? AND s.mapping_version=? AND s.principal_id=? AND s.fingerprint=? AND s.operation_key=? AND s.total=? AND s.layout_hash=? AND s.state=? AND r.layout_revision=?${progress?' AND r.progress_revision=?':''} AND ${streamOriginsMatch('s')})`, row.id,row.destination,row.namespace,row.mapping_version,row.principal_id,row.fingerprint,row.operation_key,row.total,row.layout_hash,row.state,row.layout_revision,...progress?[row.progress_revision]:[]);
}
/** Every direct child entry validates the real complete parent layout before
 * its boundary can authorize native work. A stream entry can reuse only its own
 * already validated receipt on the same request database and parent identity. */
export async function captureJobLayout(job: ImportJob, row: any, context: D1ImportContext, action: ImportAction): Promise<void> {
  if (row.stream_origin_required === 0) {
    // A concurrent conversion of an ordinary job into a child must not bypass
    // validation merely because this read found no structural stream origin.
    layouts.set(job, { predicate: sql(`NOT EXISTS(SELECT 1 FROM ik_jobs child WHERE child.id=? AND ${needsStreamOrigin('child.id')})`, job.id), reads: 0 });
    return;
  }
  if (row.stream_origin_required !== 1 || typeof row.origin_stream_id !== 'string' || row.origin_stream_id !== row.page_stream_id) throw new Error('IMPORT_ATOMIC_GUARD');
  const shared = contexts.get(context.authorization);
  if (shared?.database === context.storage.database && shared.streamId === row.origin_stream_id) {
    layouts.set(job, { predicate: shared.predicate, reads: 0 });
    return;
  }
  const receipt = await readStreamReceipt(context, row.origin_stream_id, action);
  if (!receipt.jobs.some(child => child.id === job.id)) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  layouts.set(job, { predicate: streamSnapshotPredicate(receipt.row), reads: receipt.queries });
}
export const jobLayoutReads = (job: ImportJob): number => layouts.get(job)?.reads ?? 0;
export const jobLayoutPredicate = (job: ImportJob): D1Sql | undefined => layouts.get(job)?.predicate;
export const jobLayoutGuards = (job: ImportJob): D1Sql[] => {
  const predicate = jobLayoutPredicate(job);
  return predicate ? [guard(predicate)] : [];
};
