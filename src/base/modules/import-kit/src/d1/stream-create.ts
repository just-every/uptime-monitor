import { stageStreamPages } from './stream-stage.ts';
import { streamInputCheck } from './stream-input.ts';
import type { D1ImportContext } from './types.ts';
import { requestContext } from './budget.ts';
import { authorize, sql } from './database.ts';
import { cloneJson } from '../provenance/canonical-stream.ts';
import { requireText } from '../provenance/identity.ts';
import { streamFingerprintMatches, type ImportStreamPlan } from './stream-plan.ts';
import { publishedStream } from './published-stream.ts';
import { publicationReturnQueries } from './staging-control.ts';
import { stagingAtomic, type StageBinding } from './staging-transport.ts';
import { stagePublication, publishedStagePredicates } from './staging-publication.ts';
import { discardStage, publishedStage } from './staging-lifecycle.ts';

/** Publish all normalized pages and their exact layout atomically. The retained
 * values are compressed safe plans, never original file bytes or callbacks. */
export async function createD1ImportStream(stream: ImportStreamPlan, input: D1ImportContext) {
  const context = requestContext(input); await authorize(context, 'create');
  const original = stream;
  stream = { ...stream, header: cloneJson(stream.header), pages: [...stream.pages], lengths: [...stream.lengths] };
  const h = stream.header;
  if (!stream.pages.length || stream.pages.length !== stream.lengths.length || stream.lengths.some(length => !Number.isSafeInteger(length) || length < 0 || length === 0 && stream.pages.length !== 1)) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  if (!context.reviewedFingerprint || context.reviewedFingerprint !== stream.fingerprint || !streamFingerprintMatches(stream,context.storage.targets.planCodec,original)) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  if (stream.version !== 1 || h.destination !== context.destination || h.namespace !== context.namespace || h.mappingVersion !== context.mappingVersion || h.principalId !== context.authorization.principalId) throw new Error('PLAN_BINDING_MISMATCH');
  for (const artifact of h.source.artifacts) {
    requireText(artifact.name, 'artifact name');
    if (!/^[a-f0-9]{64}$/.test(artifact.sha256) || !Number.isSafeInteger(artifact.size) || artifact.size < 0) throw new Error('INVALID_ARTIFACT');
    if (artifact.original?.status === 'available') throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
  }
  const operation = context.operationKey ?? stream.fingerprint;
  requireText(operation, 'operationKey');
  const assertInput = streamInputCheck(original, stream, () => context.storage.targets.planCodec);
  const prior = await publishedStage(context, operation, stream.fingerprint);
  assertInput();
  if (prior) return prior;
  let stage: StageBinding | undefined;
  try {
    const staged = await stageStreamPages(stream, stream.fingerprint, context, binding => { stage = binding; }, undefined, assertInput);
    assertInput();
    const { pages, revision } = staged, binding = staged.stage;
    await stagingAtomic(context, stagePublication(binding, revision), publicationReturnQueries(context, binding), [sql('DELETE FROM ik_d1_publication_fences WHERE stream_id=?', binding.id)], publishedStagePredicates(binding,pages));
    const result = await publishedStream(context, binding.id, operation, stream.fingerprint);
    assertInput();
    return result;
  } catch (error) {
    // A successful concurrent publisher can be returned only after the ordinary
    // exact binding, layout and current-authority checks. Cleanup names this
    // attempt's generation, so it cannot remove the winner or a takeover.
    if (stage) await discardStage(stage, context);
    const winner = await publishedStage(context, operation, stream.fingerprint);
    assertInput();
    if (winner) return winner;
    throw error;
  }
}
