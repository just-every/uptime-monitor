import type { ImportAction } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { atomicStatements, finalAuthority, finalGuard } from './database.ts';
/** Release and assertion cleanup are mutations. All final predicates and current
 * authority follow them, with no subsequent write in the transaction. */
export function stagingStatements(context: D1ImportContext, action: ImportAction, statements: D1Sql[], release: D1Sql[] = [], final: D1Sql[] = []): D1Sql[] {
  return [...atomicStatements({...context,maxBatchBytes:undefined},action,statements),...release,...final.map(finalGuard),finalAuthority(context,action)];
}
