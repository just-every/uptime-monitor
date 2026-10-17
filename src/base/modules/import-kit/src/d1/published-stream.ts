import type { D1ImportContext } from './types.ts';
import { readStreamReceipt, streamReceipt } from './stream-receipt.ts';

/** Creation/replay and public status share one bounded, SELECT-only receipt.
 * Creation has its own current authority and exact operation/fingerprint bind. */
export function publishedStreamReceipt(context: D1ImportContext, id: string, operation: string, fingerprint: string) {
  return streamReceipt(context, id, 'create', { operation, fingerprint });
}
export async function publishedStream(context: D1ImportContext, id: string, operation: string, fingerprint: string) {
  return (await readStreamReceipt(context, id, 'create', { operation, fingerprint })).job;
}
