import type { D1ImportContext } from './types.ts';
import type { D1UndoReport } from './undo-types.ts';
import { requestContext } from './budget.ts';
import { authorize, readAtomic } from './database.ts';
import { stored, boundary } from './jobs.ts';
import { custodyGuards, inventoryGuards } from './integrity.ts';
import { readOriginals } from './originals.ts';
import { reversalState } from './undo-state.ts';
import { nativeStagingScope } from './native-staging-context.ts';

export async function readD1ImportUndoReport(id: string, context: D1ImportContext, page: { offset?: number; limit?: number } = {}): Promise<D1UndoReport> {
  const offset = page.offset ?? 0, limit = page.limit ?? 100;
  if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('INVALID_UNDO_REPORT_PAGE');
  context = requestContext(context);
  await authorize(context, 'read');
  const { job, plan } = await stored(id, context);
  await readOriginals(plan, id, context);
  await readAtomic(context, [...boundary(job, plan), ...custodyGuards(plan, id), ...await inventoryGuards(job, plan, context)], nativeStagingScope(job, plan));
  const state = reversalState(job);
  return { job, policy: state.policy, cursor: state.cursor, total: state.dispositions.length, nextOffset: offset + limit < state.dispositions.length ? offset + limit : null, dispositions: state.dispositions.slice(offset, offset + limit) };
}
