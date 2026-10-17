import { Buffer } from 'node:buffer';
import { wireDatabase } from '../../../d1-transport/index.mjs';
import { guard, guardPredicate } from './guards.ts';
import type { D1Database, D1ImportContext, D1Sql } from './types.ts';
const budgets = new WeakMap<D1Database, { used: number; limit: number }>();
const reservedTail = Symbol('import request return queries');
type ReservedContext = D1ImportContext & { [reservedTail]?: number };
/** Distinguish this request's measured exhaustion from adapter/provider errors. */
export class D1RequestLimitError extends Error {
  constructor() { super('IMPORT_D1_REQUEST_LIMIT'); }
}
export const valueBytes = (v: any): number => typeof v === 'string' ? Buffer.byteLength(v) : v instanceof ArrayBuffer || ArrayBuffer.isView(v) ? v.byteLength : 8;
export function checkSql(s: D1Sql): void {
  if (Buffer.byteLength(s.sql) > 100000 || (s.params?.length ?? 0) > 100) throw new Error('IMPORT_D1_STATEMENT_LIMIT');
  if ((s.params ?? []).some(p => valueBytes(p) > 2000000) || (s.params ?? []).reduce((n, p) => n + valueBytes(p), 0) > 2000000) throw new Error('IMPORT_D1_VALUE_LIMIT');
}
export function remaining(context: D1ImportContext): number { const budget=budgets.get(context.storage.database);return (budget?.limit??1000)-(budget?.used??0)-((context as ReservedContext)[reservedTail]??0); }
/** A smaller public-operation budget also stops reads, before execution. Nested
 * calls can only tighten the shared request's existing execution ceiling. */
export function limitStatements(context:D1ImportContext,max:number):D1ImportContext {
  const budget=budgets.get(context.storage.database);
  if(!budget||!Number.isSafeInteger(max)||max<1||max>1000)throw new Error('INVALID_ERASURE_STATEMENT_LIMIT');
  budget.limit=Math.min(budget.limit,budget.used+max);
  return context;
}
/** A nested operation leaves the outer operation's exact bounded return path
 * available. Spreads preserve the symbol; the original outer context releases
 * the reservation simply by continuing its own already admitted reads. */
export function reserveReturn(context: D1ImportContext, queries: number): D1ImportContext {
  if(!Number.isSafeInteger(queries)||queries<0||queries>remaining(context))throw new D1RequestLimitError();
  return {...context,[reservedTail]:((context as ReservedContext)[reservedTail]??0)+queries} as ReservedContext;
}
/** A fresh wrapper belongs to the outer public invocation; nested resume calls
 * reuse it. It counts individual executed SQL statements, including batches. */
export function requestContext(context: D1ImportContext): D1ImportContext {
  if (budgets.has(context.storage.database)) return context;
  const db = wireDatabase(context.storage.database), budget = { used: 0, limit: 1000 };
  const consume = (n: number) => { if (budget.used + n > budget.limit) throw new D1RequestLimitError(); budget.used += n; };
  const raw = new WeakMap<object, any>();
  const wrap = (statement: any, query: D1Sql): any => {
    checkSql(query);
    const result = { bind(...params: any[]) { return wrap(statement.bind(...params), { sql: query.sql, params }); }, async first(...args: any[]) { consume(1); return statement.first(...args); }, async all(...args: any[]) { consume(1); return statement.all(...args); } };
    raw.set(result, statement); return result;
  };
  const database: D1Database = { prepare(query) { return wrap(db.prepare(query), { sql: query }); }, async batch(statements) { consume(statements.length); return db.batch(statements.map(s => raw.get(s))); } };
  budgets.set(database, budget); return { ...context, storage: { ...context.storage, database } };
}
function conjunction(predicates: string[], start = 0, end = predicates.length): string {
  if (end - start === 1) return predicates[start];
  const middle = start + Math.floor((end - start) / 2);
  return `(${conjunction(predicates, start, middle)} AND ${conjunction(predicates, middle, end)})`;
}
/** Adjacent assertions can share one CHECK without changing transactional order.
 * Native statements remain between their original pre/postconditions. */
export function packGuards(statements: D1Sql[], readOnly = false): D1Sql[] {
  const prefix = readOnly ? 'SELECT CASE WHEN (' : 'INSERT INTO ik_d1_assertions(ok) SELECT CASE WHEN (';
  const suffix = readOnly ? ') THEN 1 ELSE 0 END AS ok' : ') THEN 1 ELSE 0 END';
  const result: D1Sql[] = []; const seen = new Map<string, D1Sql[]>(); let predicates: string[] = [], params: any[] = [], bytes = 0;
  const flush = () => {
    if (predicates.length) {
      // A flat AND list becomes a left-deep SQLite expression tree. Keep the
      // added depth logarithmic without changing predicates or binding order.
      const text=conjunction(predicates),packed = readOnly ? { sql: `${prefix}${text}${suffix}`, params } : guard({sql:text,params});
      checkSql(packed); result.push(packed);
    }
    predicates = []; params = []; bytes = 0;
  };
  for (const statement of statements) {
    checkSql(statement);
    const predicate = guardPredicate(statement);
    if (!predicate) {
      if (readOnly) throw new Error('IMPORT_READ_GUARD_REQUIRED');
      flush(); seen.clear(); result.push(statement); continue;
    }
    const prior = seen.get(statement.sql) ?? [];
    if (prior.some(s => (s.params?.length ?? 0) === (statement.params?.length ?? 0) && (s.params ?? []).every((p, i) => p === statement.params![i]))) continue;
    prior.push(statement); seen.set(statement.sql, prior);
    const p = predicate.params ?? [], text = `(${predicate.sql})`;
    const nextBytes = Buffer.byteLength(text);
    if (predicates.length && (params.length + p.length > 100 || bytes + nextBytes > 90000 || params.reduce((n, p) => n + valueBytes(p), 0) + p.reduce((n, p) => n + valueBytes(p), 0) > 1500000 || predicates.length >= 50)) flush();
    predicates.push(text); params.push(...p); bytes += nextBytes;
  }
  flush(); return result;
}
