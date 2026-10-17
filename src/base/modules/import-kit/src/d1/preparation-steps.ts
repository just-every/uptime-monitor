import { randomUUID } from 'node:crypto';
import type { ImportMapping, SourceBundle } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import { requestContext, reserveReturn } from './budget.ts';
import { authorize, guard, sql } from './database.ts';
import { canonicalJson, requireText, sha256 } from '../provenance/identity.ts';
import { collectStreamPages } from './stream-plan.ts';
import { stageStreamPages } from './stream-stage.ts';
import { preparationHeader } from './preparation-header.ts';
import { stagingAtomic, type StageBinding } from './staging-transport.ts';
import { discardStage } from './staging-lifecycle.ts';
import { exactRows } from './sets.ts';
import { policy } from './undo-state.ts';
import { hashStep } from './preparation-step-hash.ts';
import { combineStep } from './preparation-step-combine.ts';
import { preparationState } from './preparation-state.ts';
import { preparationHeaderRead } from './preparation-reader.ts';
import { control, controlUpdate, progress, stageRevisions, stagesFence, type ImportPreparationStep, type StepControl, type StepStage } from './preparation-step-control.ts';
export type { ImportPreparationStep } from './preparation-step-control.ts';

/** Source records one collecting step maps, normalizes and stages privately. */
export const STEP_RECORDS = 5000;

/** Normalized imports too large for one request's CPU. Each call does one bounded
 * step and returns where the preparation stands; call again (in a new request)
 * with the same source, mapping and operationKey until it is `reviewable`, show
 * that review, then publish with `publishD1ImportPreparation(id)` and the
 * acknowledged fingerprint as `reviewedFingerprint`.
 *
 * Collecting steps map and stage STEP_RECORDS records each as private, sealed
 * generations under the usual privacy admission. Verifying steps then compute
 * the complete canonical fingerprint from that stored custody with a resumable
 * hash whose state stays in D1, fenced by every generation's revision, so a
 * changed, missing or reordered page changes the fingerprint or fails the step.
 * The last step combines the generations into one sealed preparation in SQL. */
export async function prepareD1ImportSteps(source: SourceBundle, mapping: ImportMapping, input: D1ImportContext): Promise<ImportPreparationStep> {
  const context = requestContext(input); await authorize(context, 'create');
  const operation = context.operationKey; requireText(operation, 'operationKey');
  const db = context.storage.database;
  const digest = sourceDigest(source);
  const published = await db.prepare('SELECT s.id,d.source_digest FROM ik_d1_streams s LEFT JOIN ik_d1_stream_sources d ON d.stream_id=s.id WHERE s.destination=? AND s.principal_id=? AND s.operation_key=?').bind(context.destination, context.authorization.principalId, operation).first<{ id: string; source_digest: string | null }>();
  // Only the upload these steps published replays; a different one (or an
  // import published in one request, which kept no digest) is refused.
  if (published && published.source_digest !== digest) throw new Error('OPERATION_KEY_CONFLICT');
  if (published) return { id: published.id, state: 'published', records: source.records.length, total: source.records.length };
  let row = await control(context, operation!);
  if (!row) {
    await stagingAtomic(context, [
      guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?)', context.destination, context.authorization.principalId, operation)),
      guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_streams WHERE destination=? AND principal_id=? AND operation_key=?)', context.destination, context.authorization.principalId, operation)),
      sql('INSERT INTO ik_d1_preparation_steps(id,destination,namespace,mapping_version,principal_id,operation_key,reversal_policy,codec_id,source_digest,total,phase) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
        randomUUID(), context.destination, context.namespace, context.mappingVersion, context.authorization.principalId, operation, policy(context), context.storage.targets.planCodec?.id ?? null, digest, source.records.length, 'collecting')]);
    row = (await control(context, operation!))!;
  }
  if (row.namespace !== context.namespace || row.mapping_version !== context.mappingVersion || row.reversal_policy !== policy(context)) throw new Error('OPERATION_KEY_CONFLICT');
  if (row.codec_id !== (context.storage.targets.planCodec?.id ?? null)) throw new Error('IMPORT_PLAN_CODEC_REQUIRED');
  if (row.phase === 'collecting') {
    if (row.source_digest !== digest) throw new Error('OPERATION_KEY_CONFLICT');
    return collectStep(source, mapping, context, row);
  }
  if (row.phase === 'hashing') return hashStep(context, row);
  if (row.phase === 'combining') return combineStep(context, row);
  const state = await preparationState(row.id, context, 'create');
  const header = await preparationHeaderRead(state);
  return { id: row.id, state: 'reviewable', records: row.total, total: row.total, fingerprint: state.stage.fingerprint, counts: header.counts, diagnostics: header.diagnostics };
}

/** The records count, artifacts and cursor policy the preparation began with:
 * a different upload under the same operation key is refused, not merged. */
function sourceDigest(source: SourceBundle): string {
  return sha256(canonicalJson({ artifacts: source.artifacts, diagnostics: source.diagnostics, cursor: source.paginationCursorPolicy ?? null, records: source.records.length }));
}

async function collectStep(source: SourceBundle, mapping: ImportMapping, context: D1ImportContext, row: StepControl): Promise<ImportPreparationStep> {
  const stages: StepStage[] = JSON.parse(row.stages);
  await stageRevisions(context, stages);
  const slice = source.records.slice(row.records, row.records + STEP_RECORDS);
  const { stream } = await collectStreamPages({ ...source, records: slice }, mapping, context);
  // Each step's generation has an internal operation key and a fingerprint no
  // review produced, so it can never be published on its own.
  const step = `${row.id}:${stages.length}`, stepContext = { ...context, operationKey: step };
  // A request that died after sealing this step's generation, before recording
  // it, left it unrecorded under the step's key: discard it (fenced by the
  // control row this step read) and stage the slice again.
  if (await context.storage.database.prepare('SELECT 1 FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?').bind(context.destination, context.authorization.principalId, step).first()) {
    await stagingAtomic(context, [...exactRows('ik_d1_preparation_steps', Object.keys(row), [row]),
      sql('DELETE FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?', context.destination, context.authorization.principalId, step)]);
  }
  const header = preparationHeader(stream.header, stepContext);
  let stage: StageBinding | undefined;
  try {
    const staged = await stageStreamPages(stream, sha256(step), reserveReturn(stepContext, header.reserve + 8), binding => { stage = binding; }, header.store);
    const records = row.records + slice.length, next = [...stages, { id: staged.stage.id, pages: stream.pages.length, records: slice.length }];
    await stagingAtomic(context, [...stagesFence([{ id: staged.stage.id, revision: staged.revision }]),
      ...controlUpdate(row, { records, stages: JSON.stringify(next), phase: records >= row.total ? 'hashing' : 'collecting' })]);
    return progress(row, records >= row.total ? 'verifying' : 'preparing', records);
  } catch (error) {
    if (stage) await discardStage(stage, context);
    throw error;
  }
}

