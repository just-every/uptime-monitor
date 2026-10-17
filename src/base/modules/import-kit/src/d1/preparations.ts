import type { SourceBundle, ImportMapping } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import { requestContext, reserveReturn } from './budget.ts';
import { authorize, guard, sql } from './database.ts';
import { requireText } from '../provenance/identity.ts';
import { collectStreamPages, streamView } from './stream-plan.ts';
import { reviewedSourceRecords } from './reviewed-source.ts';
import { planFingerprint } from '../plan/index.ts';
import { stageStreamPages } from './stream-stage.ts';
import { preparationHeader } from './preparation-header.ts';
import { preparationState, preparationRead, preparationStatus, readD1ImportPreparation } from './preparation-state.ts';
import { preparationHeaderRead, preparationPages } from './preparation-reader.ts';
import { verifyPreparationFingerprint } from './preparation-fingerprint.ts';
import { bindingPredicate, stageClear, stagingAtomic, type StageBinding } from './staging-transport.ts';
import { discardStage, publishedStage } from './staging-lifecycle.ts';
import { stagePublication, publishedStagePredicates } from './staging-publication.ts';
import { publishedStream } from './published-stream.ts';
import { publicationReturnQueries } from './staging-control.ts';
import { exactRows } from './sets.ts';

export { readD1ImportPreparation } from './preparation-state.ts';
export type { ImportPreparation } from './preparation-state.ts';

/** Explicitly unverified custody after acknowledgment. The expected fingerprint
 * supplies no evidence that these pages were reviewed; cold verification must
 * recompute it before the first public job or native effect can exist. */
export async function prepareD1ImportStream(source:SourceBundle,mapping:ImportMapping,input:D1ImportContext){
  const context=requestContext(input);await authorize(context,'create');
  const expected=context.reviewedFingerprint;
  if(!expected||!/^[a-f0-9]{64}$/.test(expected))throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  const operation=context.operationKey??expected;requireText(operation,'operationKey');
  const {stream,sourceHashes,pageSize}=await collectStreamPages(source,mapping,context);
  const prior=await publishedStage(context,operation,expected);
  const pending=await context.storage.database.prepare('SELECT id,fingerprint,revision,sealed_revision FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?').bind(context.destination,context.authorization.principalId,operation).first();
  if(pending&&pending.fingerprint!==expected)throw new Error('OPERATION_KEY_CONFLICT');
  const receipt=pending?await context.storage.database.prepare('SELECT stage_id FROM ik_d1_preparation_receipts WHERE stage_id=?').bind(pending.id).first():undefined;
  const ready=!!receipt&&pending!.revision===pending!.sealed_revision;
  if(prior||ready){
    // Replay still verifies the submitted values. It cannot use the supplied
    // digest to substitute a different mapping, timestamp or source.
    const review=streamView({...stream,fingerprint:expected},context.storage.targets.planCodec);
    review.source.records=reviewedSourceRecords(source.records,sourceHashes,pageSize,stream.header.counts.unique);
    if(planFingerprint(review)!==expected)throw new Error('REVIEW_FINGERPRINT_MISMATCH');
    if(prior)return prior;
    return readD1ImportPreparation(pending!.id,context);
  }
  const header=preparationHeader(stream.header,context);
  let stage:StageBinding|undefined;
  try{
    const prepared=await stageStreamPages(stream,expected,reserveReturn(context,header.reserve),binding=>{stage=binding;},header.store);
    await preparationState(prepared.stage.id,context);
    return preparationStatus(prepared.stage);
  }catch(error){
    if(stage)await discardStage(stage,context);
    // This attempt has not verified its submitted stream. A concurrent public
    // winner cannot make those different submitted values a successful replay.
    // A new request takes the complete fingerprint-checked replay path above.
    throw error;
  }
}

/** Cold continuation needs only stored safe custody and current native ports. */
export async function publishD1ImportPreparation(id:string,input:D1ImportContext){
  const context=requestContext(input);await authorize(context,'create');
  const state=await preparationState(id,context,'create');
  const {stage,revision}=state;
  try {
  const header=await preparationHeaderRead(state),pages=await preparationPages(state);
  // A preparation made in steps was verified step by step from this exact
  // custody; its control row records the revision that verification covers.
  const steps=await context.storage.database.prepare('SELECT * FROM ik_d1_preparation_steps WHERE id=?').bind(id).first<any>();
  const verified=steps?.phase==='prepared'&&steps.verified_revision===revision&&steps.fingerprint===stage.fingerprint&&steps.destination===stage.destination&&steps.principal_id===stage.principal_id&&steps.operation_key===stage.operation_key;
  if(!verified)await verifyPreparationFingerprint(state,header,pages);
  await preparationRead(state,sql('SELECT 1 AS verified'));
  // Receipt/schema triggers make deletion part of the same revision fences as
  // every other private value. Existing final publication owns exact cleanup.
  await stagingAtomic(context,[guard(bindingPredicate(stage)),guard(stageClear(stage)),
    ...steps?[...exactRows('ik_d1_preparation_steps',Object.keys(steps),[steps]),sql('DELETE FROM ik_d1_preparation_steps WHERE id=?',id)]:[],
    ...stagePublication(stage,revision),...steps?[sql('INSERT INTO ik_d1_stream_sources(stream_id,destination,source_digest) VALUES(?,?,?)',id,stage.destination,steps.source_digest)]:[]],publicationReturnQueries(context,stage),[sql('DELETE FROM ik_d1_publication_fences WHERE stream_id=?',id)],[...publishedStagePredicates(stage,pages),sql('NOT EXISTS(SELECT 1 FROM ik_d1_preparation_receipts WHERE stage_id=?) AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_headers WHERE stage_id=?)',id,id)]);
  return await publishedStream(context,id,stage.operation_key,stage.fingerprint);
  } catch(error) {
    await discardStage(stage,context);
    const winner=await publishedStage(context,stage.operation_key,stage.fingerprint);
    if(winner)return winner;
    throw error;
  }
}
