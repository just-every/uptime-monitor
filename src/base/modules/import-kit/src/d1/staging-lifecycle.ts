import type { D1ImportContext, D1Sql } from './types.ts';
import { authorize, sql } from './database.ts';
import { exactRows } from './sets.ts';
import { requestContext } from './budget.ts';
import { publishedStream } from './published-stream.ts';
import { beginStageWrites, discardStageWrites } from './staging-control.ts';
import { requireText } from '../provenance/identity.ts';
import { stagingAtomic, type StageBinding } from './staging-transport.ts';

export async function publishedStage(context: D1ImportContext, operation: string, fingerprint: string) {
  const prior = await context.storage.database.prepare('SELECT id,fingerprint FROM ik_d1_streams WHERE destination=? AND principal_id=? AND operation_key=?').bind(context.destination, context.authorization.principalId, operation).first();
  if (!prior) return undefined;
  if (prior.fingerprint !== fingerprint) throw new Error('OPERATION_KEY_CONFLICT');
  return publishedStream(context, prior.id, operation, fingerprint);
}

export async function beginStage(stage: StageBinding, context: D1ImportContext, preservePreparation = false, assertInput?: () => void): Promise<void> {
  const prior = await context.storage.database.prepare('SELECT * FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?').bind(stage.destination, stage.principal_id, stage.operation_key).first();
  if (prior && (prior.namespace !== stage.namespace || prior.mapping_version !== stage.mapping_version || prior.fingerprint !== stage.fingerprint)) throw new Error('OPERATION_KEY_CONFLICT');
  if (prior && prior.reversal_policy !== stage.reversal_policy) throw new Error('REVERSAL_POLICY_CONFLICT');
  if (prior && preservePreparation && prior.sealed_revision === prior.revision && await context.storage.database.prepare('SELECT stage_id FROM ik_d1_preparation_receipts WHERE stage_id=?').bind(prior.id).first()) throw new Error('IMPORT_PREPARATION_EXISTS');
  assertInput?.();
  await stagingAtomic(context, beginStageWrites(stage, prior));
}
export async function discardStage(stage: StageBinding, context: D1ImportContext): Promise<void> {
  await stagingAtomic(context, discardStageWrites(stage));
}
/** Owner cancellation of private-only custody needs no native entity or job.
 * It cannot erase another operator's generation or any published import. */
export async function cancelD1ImportStaging(operationKey: string, input: D1ImportContext): Promise<void> {
  const context = requestContext(input); requireText(operationKey, 'operationKey');
  await authorize(context, 'create');
  // A preparation made in steps: its control row and every generation under its
  // internal keys, recorded or left unrecorded by an interrupted step.
  const steps = await context.storage.database.prepare('SELECT * FROM ik_d1_preparation_steps WHERE destination=? AND namespace=? AND principal_id=? AND operation_key=?').bind(context.destination, context.namespace, context.authorization.principalId, operationKey).first<any>();
  if (steps) await stagingAtomic(context, [...exactRows('ik_d1_preparation_steps', Object.keys(steps), [steps]),
    stepGenerations('DELETE FROM ik_d1_staging', steps), sql('DELETE FROM ik_d1_preparation_steps WHERE id=?', steps.id)]);
  const stage = await context.storage.database.prepare('SELECT * FROM ik_d1_staging WHERE destination=? AND namespace=? AND principal_id=? AND operation_key=?').bind(context.destination, context.namespace, context.authorization.principalId, operationKey).first();
  if (!stage) return;
  if (context.reviewedFingerprint !== undefined && context.reviewedFingerprint !== stage.fingerprint) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  await stagingAtomic(context, [...exactRows('ik_d1_staging', Object.keys(stage), [stage]), sql('DELETE FROM ik_d1_staging WHERE id=? AND generation=?', stage.id, stage.generation)]);
}

/** Every generation a preparation's steps staged: each has the internal key
 * `<preparation id>:<step>` under the preparation's destination and principal. */
export function stepGenerations(statement: string, steps: { id: string; destination: string; principal_id: string }): D1Sql {
  const prefix = `${steps.id}:`;
  return sql(`${statement} WHERE destination=? AND principal_id=? AND substr(operation_key,1,?)=?`, steps.destination, steps.principal_id, prefix.length, prefix);
}
