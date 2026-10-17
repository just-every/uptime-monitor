import { randomUUID } from 'node:crypto';
import type { D1ImportContext, D1Sql } from './types.ts';
import { copyStatement } from './guards.ts';
import { prepareErasureSelection } from './erasure-selection.ts';
import { executeErasureStage, stageD1ImportErasure } from './erasure-stage.ts';
import { canonicalJson } from '../provenance/identity.ts';
import type { D1ErasureStep, D1ErasureStepOptions } from './erasure-progress.ts';
import { erasureStatementBinding } from './erasure-progress.ts';

/** With an operation key, resumes only the journal's verified offset. Without
 * options the complete operation must still fit this single invocation. */
export function eraseBounded(kind: string, targetId: string, nativeStatements: D1Sql[], context: D1ImportContext): Promise<{ nativeResults: unknown[]; erasedRecords: number }>;
export function eraseBounded(kind: string, targetId: string, nativeStatements: D1Sql[], context: D1ImportContext, options: D1ErasureStepOptions): Promise<D1ErasureStep>;
export async function eraseBounded(kind: string, targetId: string, nativeStatements: D1Sql[], context: D1ImportContext, options?: D1ErasureStepOptions): Promise<D1ErasureStep | { nativeResults: unknown[]; erasedRecords: number }> {
  const keys = nativeStatements.map(() => Symbol('native erasure result'));
  const statements = nativeStatements.map((row, i) => Object.assign(copyStatement(row), { resultKey: keys[i] }));
  const build = async (id: string, ctx: D1ImportContext) => ({ ...await prepareErasureSelection({ target: { kind, id: targetId } }, statements, ctx, id), resultKeys: keys });
  if (options) return stageD1ImportErasure(context, options, canonicalJson([kind, targetId, erasureStatementBinding(nativeStatements)]), build);
  const id = randomUUID(), result = await executeErasureStage(context, id, await build(id, context));
  if (result.phase !== 'complete') throw new Error('IMPORT_ERASURE_INCOMPLETE');
  return { nativeResults: result.nativeResults, erasedRecords: result.erasedRecords };
}
