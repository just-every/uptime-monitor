import { randomUUID } from 'node:crypto';
import type { D1ImportContext } from './types.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { guard, sql } from './database.ts';
import { exactRows } from './sets.ts';
import { layoutHash } from './stream-state.ts';
import { policyHash } from './undo-state.ts';
import { preparationHeader } from './preparation-header.ts';
import { reserveReturn } from './budget.ts';
import { beginStageWrites, stageRevisionWrites } from './staging-control.ts';
import { stagingAtomic, type StageBinding } from './staging-transport.ts';
import { controlUpdate, progress, stageRevisions, stagesFence, type ImportPreparationStep, type StepControl, type StepStage } from './preparation-step-control.ts';
import { mergedHeader, stageHeader } from './preparation-step-pages.ts';

/** The last step: copy every step generation into one sealed preparation under
 * the verified fingerprint, in SQL. Pages take their global ordinals and new
 * job ids (the first page's is the stream id), so their index and policy
 * hashes are recomputed from the stored index; payload bytes are copied, never
 * re-sent. The step generations go in the sealing transaction, and the control
 * row then records the revision the fingerprint was verified for. */
export async function combineStep(context: D1ImportContext, row: StepControl): Promise<ImportPreparationStep> {
  const db = context.storage.database, stages: StepStage[] = JSON.parse(row.stages), state = JSON.parse(row.hash_json);
  const revisions = await stageRevisions(context, stages);
  if (canonicalHash(state.revisions) !== canonicalHash(revisions)) throw new Error('IMPORT_PREPARATION_CHANGED');
  const ids = JSON.stringify(stages.map(stage => stage.id));
  const stored = (await db.prepare('SELECT stage_id,ordinal,total,fingerprint,identities,chunks FROM ik_d1_staging_pages WHERE stage_id IN (SELECT value FROM json_each(?))').bind(ids).all<any>()).results;
  const mapping: [string, number, number, string, string, string][] = [], layout: { ordinal: number; job_id: string; total: number }[] = [];
  let chunks = 0, identities = 0;
  for (const stage of stages) {
    const pages = stored.filter(page => page.stage_id === stage.id).sort((a, b) => a.ordinal - b.ordinal);
    if (pages.length !== stage.pages) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
    const index = await stageRows(context, stage.id);
    for (const page of pages) {
      const ordinal = layout.length, job = ordinal === 0 ? row.id : randomUUID();
      const rows = index.filter(item => item.page_ordinal === page.ordinal).map(item => [ordinal, item.ordinal, item.source_identity, item.target_identity]);
      if (rows.length !== page.identities) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
      mapping.push([stage.id, page.ordinal, ordinal, job, canonicalHash(rows), policyHash(job, page.fingerprint, row.reversal_policy)]);
      layout.push({ ordinal, job_id: job, total: page.total });
      chunks += page.chunks; identities += rows.length;
    }
  }
  const header = mergedHeader(await Promise.all(stages.map(stage => stageHeader(context, stage.id))));
  const final: StageBinding = { id: row.id, generation: randomUUID(), destination: row.destination, namespace: row.namespace, mapping_version: row.mapping_version, principal_id: row.principal_id,
    fingerprint: row.fingerprint, operation_key: row.operation_key, total: layout.reduce((n, page) => n + page.total, 0), pages: layout.length, layout_hash: layoutHash(layout), reversal_policy: row.reversal_policy };
  if (final.total !== row.total) throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
  const originals = (await db.prepare('SELECT count(*) AS n FROM ik_d1_staging_originals WHERE stage_id=?').bind(stages[0].id).first<{ n: number }>())!.n;
  const prior = await db.prepare('SELECT * FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?').bind(row.destination, row.principal_id, row.operation_key).first<any>();
  if (prior && (prior.id !== row.id || prior.fingerprint !== row.fingerprint)) throw new Error('OPERATION_KEY_CONFLICT');
  const map = JSON.stringify(mapping), at = (n: number) => `json_extract(m.value,'$[${n}]')`;
  const head = preparationHeader(header, context);
  const reserved = reserveReturn(context, head.reserve + 16);
  // One transaction: the new generation, its pages and complete privacy index
  // (registered before any payload), then its chunks and originals.
  await stagingAtomic(reserved, [...stagesFence(revisions), ...exactRows('ik_d1_preparation_steps', Object.keys(row), [row]), ...beginStageWrites(final, prior),
    sql(`INSERT INTO ik_d1_staging_pages(stage_id,ordinal,job_id,total,fingerprint,plan_json,body_hash,body_size,chunks,identities,index_hash,policy_hash,inline_plan) SELECT ?,${at(2)},${at(3)},p.total,p.fingerprint,p.plan_json,p.body_hash,p.body_size,p.chunks,p.identities,${at(4)},${at(5)},p.inline_plan FROM json_each(?) AS m JOIN ik_d1_staging_pages p ON p.stage_id=${at(0)} AND p.ordinal=${at(1)} ORDER BY m.key`, final.id, map),
    sql(`INSERT INTO ik_d1_staging_index(stage_id,page_ordinal,ordinal,source_identity,target_identity) SELECT ?,${at(2)},i.ordinal,i.source_identity,i.target_identity FROM json_each(?) AS m JOIN ik_d1_staging_index i ON i.stage_id=${at(0)} AND i.page_ordinal=${at(1)} ORDER BY m.key,i.rowid`, final.id, map),
    // A normalized stream names each source once and each native target once.
    guard(sql("NOT EXISTS(SELECT 1 FROM ik_d1_staging_index WHERE stage_id=? GROUP BY source_identity HAVING count(DISTINCT page_ordinal||':'||ordinal)>1) AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_index WHERE stage_id=? AND target_identity<>'' GROUP BY target_identity HAVING count(*)>1)", final.id, final.id)),
    ...stageRevisionWrites(final, layout.length + identities, false),
    sql(`INSERT INTO ik_d1_staging_chunks(stage_id,page_ordinal,ordinal,body,body_hash,body_size) SELECT ?,${at(2)},c.ordinal,c.body,c.body_hash,c.body_size FROM json_each(?) AS m JOIN ik_d1_staging_chunks c ON c.stage_id=${at(0)} AND c.page_ordinal=${at(1)} ORDER BY m.key,c.ordinal`, final.id, map),
    sql('INSERT INTO ik_d1_staging_originals(stage_id,original_key,name,sha256,size) SELECT ?,original_key,name,sha256,size FROM ik_d1_staging_originals WHERE stage_id=? ORDER BY rowid', final.id, stages[0].id),
    // Every slice came from the same artifacts, so each holds the same originals.
    guard(sql("NOT EXISTS(SELECT 1 FROM json_each(?) AS s WHERE (SELECT count(*) FROM ik_d1_staging_originals o WHERE o.stage_id=s.value)<>? OR EXISTS(SELECT 1 FROM ik_d1_staging_originals o WHERE o.stage_id=s.value AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_originals f WHERE f.stage_id=? AND f.original_key=o.original_key AND f.name=o.name AND f.sha256=o.sha256 AND f.size=o.size)))", ids, originals, final.id)),
    guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)', final.id, layout.length + identities + chunks + originals))]);
  const revision = await head.store(final, layout.length + identities + chunks + originals);
  await stagingAtomic(context, [...stagesFence(revisions), ...stageRevisionWrites(final, revision, true),
    sql('DELETE FROM ik_d1_staging WHERE id IN (SELECT value FROM json_each(?))', ids), guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE id IN (SELECT value FROM json_each(?)))', ids)),
    ...controlUpdate(row, { phase: 'prepared', stages: '[]', hash_json: null, verified_revision: revision })]);
  return progress(row, 'verifying');
}

/** All privacy index rows of one step generation, in staged order. */
async function stageRows(context: D1ImportContext, stage: string): Promise<any[]> {
  const rows: any[] = [];
  for (let cursor = 0; ;) {
    const { results } = await context.storage.database.prepare('SELECT rowid AS cursor,page_ordinal,ordinal,source_identity,target_identity FROM ik_d1_staging_index WHERE stage_id=? AND rowid>? ORDER BY rowid LIMIT 4096').bind(stage, cursor).all<any>();
    if (!results.length) return rows;
    rows.push(...results); cursor = results[results.length - 1].cursor;
  }
}
