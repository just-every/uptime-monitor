import { retiredError } from './retirement.ts';
import type { ImportAction } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import { authority, guard, prepare, sql } from './database.ts';
import { packGuards, remaining, D1RequestLimitError } from './budget.ts';
import { streamOriginsMatch } from './stream-origin.ts';
import { batchBytes } from './transport.ts';
import { batchRows, checkBatchResults } from './read-results.ts';
import { streamJob } from './stream-snapshot.ts';

export type StreamCreationBinding = { operation: string; fingerprint: string };
/** The complete actual builder is also used to reserve the eventual return.
 * Array size affects SQL outputs only, never predicate inputs or query count. */
export function streamReceipt(context: D1ImportContext, id: string, action: ImportAction, creation?: StreamCreationBinding) {
  const authorization = () => packGuards([authority(context, action)], true);
  const structural = () => packGuards([guard(sql(`EXISTS(SELECT 1 FROM ik_d1_streams s WHERE s.id=? AND s.destination=? AND s.namespace=? AND s.mapping_version=? AND s.principal_id=? AND s.state IN ('applying','undoing')${creation?' AND s.operation_key=? AND s.fingerprint=?':''} AND ${streamOriginsMatch('s')})`, id, context.destination, context.namespace, context.mappingVersion, context.authorization.principalId, ...creation?[creation.operation, creation.fingerprint]:[]))], true);
  const beforeAuthority = authorization(), beforeLayout = structural();
  const reads = [sql('SELECT s.*,r.layout_revision,r.progress_revision FROM ik_d1_streams s JOIN ik_d1_stream_revisions r ON r.stream_id=s.id WHERE s.id=?', id),
    sql('SELECT * FROM ik_d1_stream_pages WHERE stream_id=? ORDER BY ordinal', id),
    sql('SELECT j.id,j.destination,j.namespace,j.mapping_version,j.principal_id,j.operation_key,j.state,j.checkpoint,j.total,j.fence,j.error,j.reversal_policy,j.reversal_policy_hash FROM ik_jobs j JOIN ik_d1_stream_pages p ON p.job_id=j.id WHERE p.stream_id=? ORDER BY p.ordinal', id)];
  const afterLayout = structural(), afterAuthority = authorization();
  const statements = [...beforeAuthority, ...beforeLayout, ...reads, ...afterLayout, ...afterAuthority];
  const limit = context.maxBatchBytes ?? 2 * 1024 * 1024;
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error('INVALID_TRANSPORT_LIMIT');
  if (batchBytes(statements) > Math.min(limit, 2 * 1024 * 1024)) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
  return { statements, offset: beforeAuthority.length + beforeLayout.length,
    authorityIndexes: [...beforeAuthority.keys(), ...afterAuthority.map((_, i) => statements.length - afterAuthority.length + i)],
    layoutIndexes: [...beforeLayout.map((_, i) => beforeAuthority.length + i), ...afterLayout.map((_, i) => beforeAuthority.length + beforeLayout.length + reads.length + i)] };
}

export async function readStreamReceipt(context: D1ImportContext, id: string, action: ImportAction, creation?: StreamCreationBinding) {
  const receipt = streamReceipt(context, id, action, creation);
  if (receipt.statements.length > remaining(context)) throw new D1RequestLimitError();
  const result = await context.storage.database.batch(receipt.statements.map(statement => prepare(context.storage.database, statement)));
  checkBatchResults(result, receipt.statements.length);
  const valid = (index: number) => { const rows = batchRows(result[index]); return rows.length === 1 && rows[0].ok === 1; };
  if (!receipt.authorityIndexes.every(valid)) throw new Error('IMPORT_ATOMIC_GUARD');
  const parents = batchRows(result[receipt.offset]), pages = batchRows(result[receipt.offset + 1]), jobs = batchRows(result[receipt.offset + 2]);
  if (parents.length > 1 || parents[0] && parents[0].id !== id) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  if (!parents.length) { const retired = await retiredError(context.storage.database, id, 'stream'); if (retired) throw retired; }
  const row = parents[0], job = streamJob(row, pages, jobs, context);
  if (creation && (row.operation_key !== creation.operation || row.fingerprint !== creation.fingerprint)) throw new Error('OPERATION_KEY_CONFLICT');
  if (!receipt.layoutIndexes.every(valid)) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  return { row, pages, jobs, job, queries: receipt.statements.length };
}
