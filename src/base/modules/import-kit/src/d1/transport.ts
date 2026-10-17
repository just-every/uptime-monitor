import { rpcBytes, wireBatch } from '../../../d1-transport/index.mjs';
import type { D1Sql } from './types.ts';
import { checkSql } from './budget.ts';

/** Includes JSON escaping and SQL, including both actual authority predicates.
 * Each member is encoded separately, never the complete import's transport. */
export function batchBytes(statements: D1Sql[]): number {
  for (const statement of statements) checkSql(statement);
  return rpcBytes(wireBatch(statements));
}
