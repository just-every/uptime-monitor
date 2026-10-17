import type { ImportPlan } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { exactRows } from './sets.ts';

/** Where a preparation spread over requests stands. `reviewable` carries the
 * complete canonical fingerprint and the review counts and diagnostics. */
export interface ImportPreparationStep {
  id: string;
  state: 'preparing' | 'verifying' | 'reviewable' | 'published';
  records: number;
  total: number;
  fingerprint?: string;
  counts?: ImportPlan['counts'];
  diagnostics?: ImportPlan['diagnostics'];
}
export interface StepStage { id: string; pages: number; records: number }
export type StepControl = Record<string, any> & { id: string; stages: string; phase: 'collecting' | 'hashing' | 'combining' | 'prepared'; revision: number };

export async function control(context: D1ImportContext, operation: string): Promise<StepControl | undefined> {
  return (await context.storage.database.prepare('SELECT * FROM ik_d1_preparation_steps WHERE destination=? AND principal_id=? AND operation_key=?')
    .bind(context.destination, context.authorization.principalId, operation).first<StepControl>()) ?? undefined;
}

/** Advance the control row exactly once from the state this step read. */
export function controlUpdate(row: StepControl, changes: Record<string, unknown>): D1Sql[] {
  const keys = Object.keys(changes);
  return [...exactRows('ik_d1_preparation_steps', Object.keys(row), [row]),
    sql(`UPDATE ik_d1_preparation_steps SET ${keys.map(key => `${key}=?`).join(',')},revision=revision+1 WHERE id=? AND revision=?`, ...Object.values(changes), row.id, row.revision),
    guard(sql('EXISTS(SELECT 1 FROM ik_d1_preparation_steps WHERE id=? AND revision=?)', row.id, row.revision + 1))];
}

/** Fence every generation the steps staged at the revision it was read at. */
export function stagesFence(stages: { id: string; revision: number }[]): D1Sql[] {
  return exactRows('ik_d1_staging', ['id', 'revision', 'sealed_revision'], stages.map(stage => ({ id: stage.id, revision: stage.revision, sealed_revision: stage.revision })));
}

export async function stageRevisions(context: D1ImportContext, stages: StepStage[]): Promise<{ id: string; revision: number }[]> {
  const { results } = await context.storage.database.prepare('SELECT id,revision,sealed_revision,registered_revision FROM ik_d1_staging WHERE id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(stages.map(stage => stage.id))).all<any>();
  const rows = stages.map(stage => results.find(row => row.id === stage.id));
  // A generation erased, cancelled or replaced since it was staged ends the
  // preparation: cancel it and prepare the upload again.
  if (rows.some(row => !row || row.sealed_revision !== row.revision || row.registered_revision === null)) throw new Error('IMPORT_PREPARATION_INTERRUPTED');
  return rows.map(row => ({ id: row.id, revision: row.revision }));
}

export const progress = (row: StepControl, state: ImportPreparationStep['state'], records = row.records): ImportPreparationStep => ({ id: row.id, state, records, total: row.total });

