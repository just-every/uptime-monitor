import type { ImportPlan } from '../types.ts';
import type { D1Database, D1Sql } from './types.ts';
import { jsonChunks } from './sets.ts';
import { guard, sql } from './database.ts';
import { targetIdentity } from './erasure-index.ts';

/** Tombstones survive every job's undo. Both source and native target digests
 * fence a stale reviewed plan, including one with a new source namespace. */
export function erasedSourcePredicates(plan: ImportPlan): D1Sql[] {
  const predicates: D1Sql[] = [];
  const live = plan.entries.filter(entry => !entry.erased);
  const sources = live.map(entry => entry.identity);
  const targets = live.flatMap(entry => entry.effects.map(effect => targetIdentity(plan.destination, effect.targetKind, effect.targetId)));
  for (const [table, identities] of [['ik_d1_erased_sources', sources], ['ik_d1_erased_targets', targets]] as const) {
    for (const chunk of jsonChunks([...new Set(identities)])) {
      const predicate = sql(`NOT EXISTS(SELECT 1 FROM ${table} WHERE destination=? AND identity IN (SELECT value FROM json_each(?)))`, plan.destination, chunk);
      predicates.push(predicate);
    }
  }
  return predicates;
}

export async function guardErasedSources(plan: ImportPlan, db: D1Database): Promise<D1Sql[]> {
  const predicates = erasedSourcePredicates(plan);
  for (const predicate of predicates) if (!(await db.prepare(`SELECT ${predicate.sql} AS clear`).bind(...predicate.params!).first())?.clear) throw new Error('IMPORT_SOURCE_ERASED');
  return predicates.map(guard);
}
