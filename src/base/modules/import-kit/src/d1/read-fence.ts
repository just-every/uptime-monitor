import type { ImportAction } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import type { NativeStagingScope } from './native-staging-context.ts';
import { nativeStagingPostBinding } from './native-staging-context.ts';
import { authority, guard, prepare, sql } from './database.ts';
import { D1RequestLimitError, packGuards, remaining } from './budget.ts';
import { batchBytes } from './transport.ts';
import { batchRows, checkBatchResults } from './read-results.ts';
import { erasedIndexGuards } from './erasure-index.ts';
import { jsonChunks } from './sets.ts';

/** Check exact custody without writing private copies merely to read them.
 * Every batch sees the same monotonic revisions, captured before any exact
 * check. The final fence also retains the plan revision captured before decode.
 * Native target reads have no such custody fence and remain one atomic batch. */
export async function readBoundedGuards(context: D1ImportContext, guards: D1Sql[], scope: NativeStagingScope, limit: number, action: ImportAction = 'read'): Promise<void> {
  const ids = new Set([scope.job.id]);
  for (const query of guards) {
    if (query.retainedValues?.privacyJobId) ids.add(query.retainedValues.privacyJobId);
    for (const snapshot of query.expectedSnapshots ?? []) {
      for (const row of snapshot.rows) {
        for (const key of ['job_id', 'owner_job_id']) if (typeof row[key] === 'string') ids.add(row[key]);
        if (snapshot.table === 'ik_jobs' && typeof row.id === 'string') ids.add(row.id);
      }
      if (snapshot.selection?.key === 'job_id' || snapshot.table === 'ik_jobs' && snapshot.selection?.key === 'id') {
        for (const id of snapshot.selection.values) ids.add(id);
      }
    }
  }
  const revisions: [string, number, number][] = [];
  const grant = context.authorization.predicate(action, context.destination);
  for (const body of jsonChunks([...ids])) {
    const query = sql(`SELECT j.id,s.revision AS state_revision,r.revision AS custody_revision FROM ik_jobs j JOIN ik_d1_custody_states s ON s.job_id=j.id JOIN ik_d1_custody_revisions r ON r.job_id=j.id WHERE j.id IN (SELECT value FROM json_each(?)) AND (${grant.sql})`, body, ...grant.params ?? []);
    if (batchBytes([query]) > limit) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
    const rows = batchRows(await context.storage.database.prepare(query.sql).bind(...query.params!).all(), 'IMPORT_ATOMIC_GUARD');
    for (const row of rows) {
      if (!ids.has(row.id) || !Number.isSafeInteger(row.state_revision) || row.state_revision < 0 || !Number.isSafeInteger(row.custody_revision) || row.custody_revision < 0) throw new Error('IMPORT_ATOMIC_GUARD');
      revisions.push([row.id, row.state_revision, row.custody_revision]);
    }
  }
  if (revisions.length !== ids.size || new Set(revisions.map(row => row[0])).size !== ids.size) throw new Error('IMPORT_ATOMIC_GUARD');
  const fences = jsonChunks(revisions).map(body => guard(sql("NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_custody_states s JOIN ik_d1_custody_revisions r ON r.job_id=s.job_id WHERE s.job_id=json_extract(e.value,'$[0]') AND s.revision=json_extract(e.value,'$[1]') AND r.revision=json_extract(e.value,'$[2]')))", body)));
  const prefix = [authority(context, action), ...fences, ...nativeStagingPostBinding(scope)];
  const batches: D1Sql[][] = []; let pending: D1Sql[] = [];
  const packed = (members: D1Sql[]) => packGuards([...prefix, ...members], true);
  for (const member of [...guards, ...erasedIndexGuards(scope.plan)]) {
    if (batchBytes(packed([...pending, member])) > limit) {
      if (!pending.length) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
      batches.push(packed(pending)); pending = [];
      if (batchBytes(packed([member])) > limit) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
    }
    pending.push(member);
  }
  batches.push(packed(pending));
  if (batches.reduce((n, batch) => n + batch.length, 0) > remaining(context)) throw new D1RequestLimitError();
  for (const batch of batches) {
    const results = await context.storage.database.batch(batch.map(query => prepare(context.storage.database, query)));
    checkBatchResults(results, batch.length, 'IMPORT_ATOMIC_GUARD');
    for (const result of results) {
      const rows = batchRows(result, 'IMPORT_ATOMIC_GUARD');
      if (rows.length !== 1 || rows[0].ok !== 1) throw new Error('IMPORT_ATOMIC_GUARD');
    }
  }
}
