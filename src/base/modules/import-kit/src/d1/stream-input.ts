import { canonicalHash } from '../provenance/canonical-stream.ts';
import type { ImportStreamPlan } from './stream-plan.ts';
import type { D1PlanCodec } from './types.ts';

/** Compare the current caller values with the verified private snapshot. No
 * mutable container, including a shallow-frozen one, grants reusable trust. */
export function streamInputCheck(original: ImportStreamPlan, snapshot: ImportStreamPlan, codec: () => D1PlanCodec | undefined): () => void {
  const header = canonicalHash(snapshot.header), initialCodec = codec();
  const codecId = initialCodec?.id, encode = initialCodec?.encode, decode = initialCodec?.decode;
  return () => {
    const current = codec();
    if (original.version !== snapshot.version || original.fingerprint !== snapshot.fingerprint
      || canonicalHash(original.header) !== header
      || original.pages.length !== snapshot.pages.length || snapshot.pages.some((page, i) => page !== original.pages[i])
      || original.lengths.length !== snapshot.lengths.length || snapshot.lengths.some((length, i) => length !== original.lengths[i])
      || current?.id !== codecId || current?.encode !== encode || current?.decode !== decode) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  };
}
