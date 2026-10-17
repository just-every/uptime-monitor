import type { D1Sql } from './types.ts';

// Scalar SQL and bindings cannot change after admission. Expected-state and
// retained-value annotations remain extensible on the maintained statement.
// Only this WeakMap conveys predicate provenance; arbitrary DML has none.
const predicates = new WeakMap<D1Sql, D1Sql>();
export function guard(predicate: D1Sql): D1Sql {
  const retained = predicate.retainedValues;
  const scalar: D1Sql = { sql: predicate.sql, params: [...(predicate.params ?? [])],
    ...(retained ? { retainedValues: { ...retained, parts: [...retained.parts], indexes: [...retained.indexes] } } : {}) };
  Object.freeze(scalar.params);
  if (scalar.retainedValues) {
    Object.freeze(scalar.retainedValues.parts); Object.freeze(scalar.retainedValues.indexes);
    Object.freeze(scalar.retainedValues);
  }
  Object.freeze(scalar);
  const prefix = 'INSERT INTO ik_d1_assertions(ok) SELECT CASE WHEN (', suffix = ') THEN 1 ELSE 0 END';
  const statement: D1Sql = { sql: prefix + scalar.sql + suffix, params: scalar.params };
  Object.defineProperty(statement, 'sql', { writable: false });
  Object.defineProperty(statement, 'params', { writable: false });
  if (retained) {
    const parts = [...retained.parts]; parts[0] = prefix + parts[0]; parts[parts.length - 1] += suffix;
    statement.retainedValues = { ...retained, parts, indexes: [...retained.indexes] };
  }
  predicates.set(statement, scalar);
  return statement;
}
export const guardPredicate = (statement: D1Sql): D1Sql | undefined => predicates.get(statement);

/** Copy construction annotations without discarding trusted scalar provenance.
 * A copied arbitrary statement remains arbitrary; SQL text never grants trust. */
export function copyStatement(statement: D1Sql): D1Sql {
  const scalar = guardPredicate(statement);
  if (!scalar) return { ...statement };
  const { sql: _sql, params: _params, ...metadata } = statement;
  return Object.assign(guard(scalar), metadata);
}
