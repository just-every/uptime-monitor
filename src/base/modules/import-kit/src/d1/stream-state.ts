import type { ImportAction } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import { sql, guard } from './database.ts';
import { streamSnapshotPredicate, rememberStreamContext } from './stream-layout.ts';
export { streamSnapshotPredicate } from './stream-layout.ts';
import { readStreamReceipt, streamReceipt } from './stream-receipt.ts';
export { layoutHash, streamJob } from './stream-snapshot.ts';

export async function streamState(id: string, context: D1ImportContext, action: ImportAction = 'read') {
  const current = await readStreamReceipt(context, id, action);
  const predicate = streamSnapshotPredicate(current.row);
  const childContext: D1ImportContext = { ...context, reviewedFingerprint: undefined,
    authorization: { ...context.authorization, predicate(childAction, destination) {
      const outer = context.authorization.predicate(action, destination);
      const child = childAction === action ? undefined : context.authorization.predicate(childAction, destination);
      return sql(`(${outer.sql})${child?` AND (${child.sql})`:''} AND (${predicate.sql})`, ...outer.params??[], ...child?.params??[], ...predicate.params!);
    } } };
  rememberStreamContext(childContext, current.row.id, predicate);
  return { ...current, guards: [guard(streamSnapshotPredicate(current.row, true))], childContext };
}
/** Count exactly the action-specific builder that will run after the child.
 * It performs transport admission now, before native or private publication. */
export function streamReturnQueries(current: Pick<Awaited<ReturnType<typeof streamState>>, 'row'>, context: D1ImportContext, action: ImportAction = 'read'): number {
  return streamReceipt(context, current.row.id, action).statements.length;
}
