import type { D1ImportContext } from './types.ts';
import { guard, sql } from './database.ts';
import { exactRows } from './sets.ts';
import { bindingPredicate, stageClear, type StageBinding } from './staging-transport.ts';
import { stageMembership } from './staging-publication.ts';
import { stagingStatements } from './staging-statements.ts';
import { publishedStreamReceipt } from './published-stream.ts';

export function beginStageWrites(stage: StageBinding, prior?: any) {
  const before = prior ? exactRows('ik_d1_staging', Object.keys(prior), [prior]) : [guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE destination=? AND principal_id=? AND operation_key=?)', stage.destination, stage.principal_id, stage.operation_key))];
  return [...before,
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_streams WHERE destination=? AND principal_id=? AND operation_key=?)', stage.destination, stage.principal_id, stage.operation_key)),
    ...(prior ? [sql('DELETE FROM ik_d1_staging WHERE id=? AND generation=?', prior.id, prior.generation)] : []),
    sql(`INSERT INTO ik_d1_staging(${Object.keys(stage).join(',')}) VALUES(${Object.keys(stage).map(() => '?').join(',')})`, ...Object.values(stage))];
}
export const discardStageWrites = (stage: StageBinding) => [sql('DELETE FROM ik_d1_staging WHERE id=? AND generation=? AND destination=? AND namespace=? AND mapping_version=? AND principal_id=? AND operation_key=? AND fingerprint=?', stage.id, stage.generation, stage.destination, stage.namespace, stage.mapping_version, stage.principal_id, stage.operation_key, stage.fingerprint)];

export function stageRevisionWrites(stage: StageBinding, revision: number, seal: boolean) {
  return [guard(bindingPredicate(stage)), guard(sql(`EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?${seal ? ' AND registered_revision IS NOT NULL' : ''})`, stage.id, revision)), guard(stageMembership(stage, seal)), guard(stageClear(stage)),
    sql(`UPDATE ik_d1_staging SET ${seal ? 'sealed_revision' : 'registered_revision'}=revision WHERE id=?`, stage.id)];
}

/** Reserve the complete successful receipt plus the reachable failure path:
 * generation-only discard, operation lookup, and another authorized receipt.
 * Packing the actual statements also admits their binding and transport sizes. */
export function publicationReturnQueries(context: D1ImportContext, stage: StageBinding): number {
  const receipt = publishedStreamReceipt(context, stage.id, stage.operation_key, stage.fingerprint).statements.length;
  const discard = stagingStatements(context, 'create', discardStageWrites(stage)).length;
  return receipt + discard + 1 + receipt;
}

/** Bounded fixed work outside the payload batches. Include the largest prior
 * generation shape and the optional preparation lookup before its replacement. */
export function stagingControlQueries(context: D1ImportContext, stage: StageBinding): number {
  const prior = { ...stage, revision: Number.MAX_SAFE_INTEGER, registered_revision: Number.MAX_SAFE_INTEGER, sealed_revision: Number.MAX_SAFE_INTEGER };
  const packed = (statements: ReturnType<typeof stageRevisionWrites>) => stagingStatements(context, 'create', statements).length;
  return 1 // Destination erasure-set admission read.
    + 2 // Existing generation and optional preparation receipt.
    + Math.max(packed(beginStageWrites(stage)), packed(beginStageWrites(stage, prior)))
    + packed(stageRevisionWrites(stage, Number.MAX_SAFE_INTEGER, false))
    + packed(stageRevisionWrites(stage, Number.MAX_SAFE_INTEGER, true))
    + publicationReturnQueries(context, stage);
}
