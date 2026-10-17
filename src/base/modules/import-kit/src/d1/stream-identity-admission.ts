import { Buffer } from 'node:buffer';
import type { D1ImportContext } from './types.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import { planIndex } from './erasure-index.ts';
import { expandCompressedPlan, type UnverifiedStreamPages } from './stream-plan.ts';
import { sql } from './database.ts';
import { assertStagingIdentitiesClear, stagingPrivacyClear } from './staging-privacy.ts';
import { STAGING_ROWS_BYTES } from './staging-transport.ts';

/** Establish complete identity admission before the first private write. Empty
 * destination erasure sets prove it without another page decode. Otherwise
 * retain only one decoded page and one bounded digest batch at a time. */
export async function admitStreamIdentities(stream: UnverifiedStreamPages, context: D1ImportContext, reservation: { executions: number; indexBatches: number }): Promise<void> {
  const clear = await stagingPrivacyClear(context, sql('NOT EXISTS(SELECT 1 FROM ik_d1_erased_sources WHERE destination=?) AND NOT EXISTS(SELECT 1 FROM ik_d1_erased_targets WHERE destination=?)', context.destination, context.destination));
  if (clear) return;
  // The empty-set read has already consumed one of the reserved executions.
  // Reserve all additional identity reads, all subsequent writes and cleanup.
  if (reservation.executions - 1 + reservation.indexBatches > remaining(context)) throw new D1RequestLimitError();
  let parts: string[] = [], bytes = 2, reads = 0;
  const flush = async () => {
    if (!parts.length) return;
    if (++reads > reservation.indexBatches) throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
    const body = `[${parts.join(',')}]`;
    parts = []; bytes = 2;
    await assertStagingIdentitiesClear(context, context.destination, body, 'admission');
  };
  for (let page = 0; page < stream.pages.length; page++) {
    const plan = expandCompressedPlan(stream.pages[page], context.storage.targets.planCodec);
    for (const identity of planIndex(plan, '')) {
      const text = JSON.stringify([page, identity.ordinal, identity.source_identity, identity.target_identity]), size = Buffer.byteLength(text) + 1;
      if (!/^[a-f0-9]{64}$/.test(identity.source_identity) || identity.target_identity !== '' && !/^[a-f0-9]{64}$/.test(identity.target_identity) || size > 200) throw new Error('IMPORT_STREAM_PAGE_CHANGED');
      if (parts.length && bytes + size > STAGING_ROWS_BYTES) await flush();
      parts.push(text); bytes += size;
    }
  }
  await flush();
}
