import { stagingStatements } from './staging-statements.ts';
import { Buffer } from 'node:buffer';
import type { ImportStreamPlan } from './stream-plan.ts';
import type { D1ImportContext } from './types.ts';
import { guard, sql } from './database.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import { bindingPredicate, stageClear, stagingBatchBytes, stagingChunks, STAGING_BATCH_BYTES, STAGING_ROWS_BYTES, type StageBinding } from './staging-transport.ts';
import { stagePublication, publishedStagePredicates } from './staging-publication.ts';
import { stagingPlan } from './staging-plan.ts';
import { stagingControlQueries } from './staging-control.ts';

/** Reserve all transactions before private writes. Greedy row batches are full
 * to at least limit-largestRow, except their last batch. Counts include both
 * current-authority checks, mutation guards, cleanup and public return reads. */
export function reserveStaging(stream: ImportStreamPlan, originals: unknown[][], context: D1ImportContext, stage: StageBinding, pages:{ordinal:number;job_id:string;total:number}[]): { executions: number; indexBatches: number } {
  const batches = (bytes: number, largest: number) => {
    if (largest + 2 > STAGING_ROWS_BYTES) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
    return bytes ? Math.ceil(bytes / (STAGING_ROWS_BYTES - largest - 2)) : 0;
  };
  const indices = stream.header.counts.effects + stream.header.counts.unique;
  if (!Number.isSafeInteger(indices) || indices < 0) throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
  let chunkBytes = 0, largestChunk = 0;
  for (let page = 0; page < stream.pages.length; page++) {
    let ordinal = 0;
    for (const body of stagingChunks(stagingPlan(stream.pages[page], context.storage.targets.planCodec).body)) {
      const bytes = Buffer.byteLength(JSON.stringify([page, ordinal++, body, '0'.repeat(64), Buffer.byteLength(body)])) + 1;
      chunkBytes += bytes; largestChunk = Math.max(largestChunk, bytes);
    }
  }
  let originalBytes = 0, largestOriginal = 0;
  for (const row of originals) { const bytes = Buffer.byteLength(JSON.stringify(row)) + 1; originalBytes += bytes; largestOriginal = Math.max(largestOriginal, bytes); }
  const sample = stagingStatements(context, 'create', [guard(bindingPredicate(stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)', stage.id, 0)), guard(stageClear(stage)),
    sql('INSERT INTO ik_d1_staging_chunks(stage_id,page_ordinal,ordinal,body,body_hash,body_size) SELECT ?,json_extract(value,\'$[0]\'),json_extract(value,\'$[1]\'),json_extract(value,\'$[2]\'),json_extract(value,\'$[3]\'),json_extract(value,\'$[4]\') FROM json_each(?)', stage.id, ''),
    guard(bindingPredicate(stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)', stage.id, Number.MAX_SAFE_INTEGER)), guard(stageClear(stage)), sql('SELECT page_ordinal,ordinal,body,body_hash,body_size FROM ik_d1_staging_chunks WHERE stage_id=? ORDER BY rowid LIMIT ? OFFSET ?', stage.id, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER)]);
  if (stagingBatchBytes(sample) + 2 * STAGING_ROWS_BYTES + 100000 > STAGING_BATCH_BYTES) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
  const indexBatches = batches(indices * 200, 200);
  const batchCount = indexBatches + batches(stream.pages.length * 1024, 1024) + batches(chunkBytes, largestChunk) + batches(originalBytes, largestOriginal);
  const publication = stagingStatements(context, 'create', stagePublication(stage, Number.MAX_SAFE_INTEGER), [sql('DELETE FROM ik_d1_publication_fences WHERE stream_id=?', stage.id)], publishedStagePredicates(stage,pages));
  if (stagingBatchBytes(publication) > STAGING_BATCH_BYTES) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
  // Use the same packed control/receipt builders as execution, including
  // generation cleanup and authorized concurrent-winner recovery.
  const executions = batchCount * sample.length + indexBatches + publication.length + stagingControlQueries(context, stage);
  if (!Number.isSafeInteger(executions) || executions > remaining(context)) throw new D1RequestLimitError();
  return { executions, indexBatches };
}
