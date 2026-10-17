import type { D1ImportContext } from './types.ts';
import { requestContext, reserveReturn } from './budget.ts';
import { atomic, guard, sql } from './database.ts';
import { streamState, streamReturnQueries, streamSnapshotPredicate } from './stream-state.ts';
import { applyD1ImportBatches } from './apply.ts';
import { reverseD1Import } from './reverse.ts';
export { planD1ImportStream, type ImportStreamPlan } from './stream-plan.ts';
export { createD1ImportStream } from './stream-create.ts';

export async function readD1ImportStream(id: string, input: D1ImportContext) {
  return (await streamState(id, requestContext(input), 'read')).job;
}
/** One native bounded page invocation. Progress never needs the original input
 * or mapper; all child pages were committed with the reviewed parent layout. */
export async function resumeD1ImportStream(id: string, input: D1ImportContext) {
  const context = requestContext(input), current = await streamState(id, context, 'apply');
  if (current.row.state === 'undoing') return undoD1ImportStream(id, current.childContext);
  const pending = current.jobs.find(j => j.state !== 'completed');
  if (pending) await applyD1ImportBatches(pending.id, { ...reserveReturn(current.childContext, streamReturnQueries(current, context, 'apply')), maxBatches: 1 });
  return (await streamState(id, context, 'apply')).job;
}
export async function undoD1ImportStream(id: string, input: D1ImportContext) {
  const context = requestContext(input);
  let current = await streamState(id, context, 'undo');
  const returns = streamReturnQueries(current, context, 'undo');
  if (current.row.state !== 'undoing') {
    // The only intended change is this parent direction. Any trigger mutation
    // of layout, binding, child progress or direction changes these revisions
    // and aborts before commit, including during assertion cleanup.
    const after = { ...current.row, state: 'undoing', progress_revision: current.row.progress_revision + 1 };
    const transition = sql("UPDATE ik_d1_streams SET state='undoing' WHERE id=?", id);
    transition.finalChecks = [guard(streamSnapshotPredicate(after, true))];
    await atomic(reserveReturn({ ...context, maxBatchBytes: Math.min(context.maxBatchBytes ?? 2 * 1024 * 1024, 2 * 1024 * 1024) }, returns * 2), 'undo', [...current.guards, transition]);
    current = await streamState(id, context, 'undo');
  }
  const pending = current.jobs.findLast(j => !['undone','rolled_back'].includes(j.state));
  if (pending) await reverseD1Import(pending.id, ['completed','undoing'].includes(pending.state) ? 'undo' : 'rollback', { ...reserveReturn(current.childContext, returns), maxBatches: 1 });
  return (await streamState(id, context, 'undo')).job;
}
