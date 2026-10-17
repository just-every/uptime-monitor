import type { D1ImportContext, D1Sql } from './types.ts';
import { eraseBounded } from './erasure-bounded.ts';
import type { D1ErasureStep, D1ErasureStepOptions } from './erasure-progress.ts';
import { requireText } from '../provenance/identity.ts';

/** The target-only equivalent of retireD1ImportCustodyStep. Its operation key
 * loads the journal's verified native-stage offset; arbitrary offsets are never
 * accepted from a request. Use retirement when headers or restore fences apply. */
export function eraseD1ImportTargetStep(kind: string, id: string, nativeStatements: D1Sql[], context: D1ImportContext, options: D1ErasureStepOptions): Promise<D1ErasureStep> {
  requireText(kind, 'target kind'); requireText(id, 'target id');
  return eraseBounded(kind, id, nativeStatements, context, options);
}
