import type { ImportPlan, TargetRow } from '../types.ts';
import type { D1FieldMatch, D1FieldRedaction, D1ImportContext, D1Sql } from './types.ts';
import type { InventorySnapshot } from './inventory-snapshot.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { inRange, redactFields, type EntryRange } from './field-redaction.ts';
import { withdrawnLocation } from './erased-artifacts.ts';
import { recordWrites } from './erasure-writes.ts';
import { targetGuards } from './target-guards.ts';
import { guard } from './database.ts';

/** Keep attribution and revision identities; change only the erased fields and
 * their content bindings. A sanitized before-image is what undo will restore. */
export function redactedCustody(plan: ImportPlan, next: ImportPlan, id: string, snapshot: InventorySnapshot, fields: readonly D1FieldRedaction[], range?: EntryRange): D1Sql[] {
  const writes: D1Sql[] = [];
  for (const table of ['ik_provenance', 'ik_d1_history']) {
    const rows: { id: string; body: string }[] = [];
    for (const row of snapshot.rows(table, 'job_id', [id])) {
      if (!inRange(range, row.ordinal)) continue;
      const entry = next.entries[row.ordinal], effect = entry.effects[row.effect_ordinal];
      if (!effect) continue; // Whole-record tombstones delete their provenance.
      const record = JSON.parse(row.record_json);
      const before = record.before ? { ...record.before, value: redactFields(record.before.value, fields) } : null;
      const body = canonicalJson({ ...record, sourceHash: entry.source.payloadHash, valueHash: sha256(canonicalJson(effect.value)),
        location: withdrawnLocation(plan, next, record.location, record.artifactHash), before });
      if (body !== row.record_json) rows.push({ id: row.id, body });
    }
    writes.push(...recordWrites(table, rows));
  }
  return writes;
}

/** Sanitize the adapter's native set without claiming any unrelated edit as an
 * imported value. Provenance retains the sanitized planned hash; other edits
 * therefore continue to conflict on undo. */
export async function redactedNativeFields(kind: string, id: string, context: D1ImportContext, fields: readonly D1FieldRedaction[], counts: D1FieldMatch[], owned?: (row: TargetRow) => boolean): Promise<{ before: D1Sql[]; after: D1Sql[]; final: D1Sql[] }> {
  const { database: db, targets } = context.storage, destination = context.destination;
  if (owned) {
    // One step of a resumable redaction rewrites only the rows its jobs own, each
    // fenced by its exact snapshot; the final step proves no other row holds one.
    const scope = targets.redaction ? await targets.redaction(db, destination, fields) : undefined;
    const rows = (scope ? scope.rows : await targets.list(db, destination)).filter(owned);
    const changed = rows.map(row => ({ row, next: { ...row, value: redactFields(row.value, fields, counts) } })).filter(({ row, next }) => next.value !== row.value);
    const before = changed.map(({ row }) => row), after = changed.map(({ next }) => next);
    if (scope) return { before: scope.matches(before).map(guard), after: [...scope.matches(before).map(guard), ...scope.write(after)], final: scope.matches(after).map(guard) };
    const snapshot = (row: TargetRow) => targetGuards(targets, row, destination, row.targetKind, row.targetId);
    return { before: before.flatMap(snapshot), after: changed.flatMap(({ row, next }) => [...snapshot(row), ...targets.write(next)]), final: after.flatMap(snapshot) };
  }
  if (targets.redaction) {
    // Only rows holding a selected value are read, fenced and rewritten; one
    // holding-set predicate proves no other row holds one before and after.
    const scope = await targets.redaction(db, destination, fields);
    const redacted = scope.rows.map(row => ({ ...row, value: redactFields(row.value, fields, counts) }));
    if (redacted.some((row, index) => row.value === scope.rows[index].value)) throw new Error('IMPORT_REDACTION_SCOPE_MISMATCH');
    const kept = (row: TargetRow) => row.targetKind !== kind || row.targetId !== id;
    const rows = scope.rows.filter(kept), next = redacted.filter(kept);
    return { before: [...scope.holding(scope.rows), ...scope.matches(scope.rows)].map(guard),
      after: [...[...scope.holding(rows), ...scope.matches(rows)].map(guard), ...scope.write(next)],
      final: [...scope.holding([]), ...scope.matches(next)].map(guard) };
  }
  const rows = await targets.list(db, destination);
  const before = targets.matchesMany ? targets.matchesMany(rows, destination).map(guard)
    : targets.matchesCount ? [guard(targets.matchesCount(rows.length, destination)), ...rows.flatMap(row => targetGuards(targets, row, destination, row.targetKind, row.targetId))] : [];
  if (!before.length) throw new Error('IMPORT_NATIVE_SET_GUARD_REQUIRED');
  const after: D1Sql[] = [], survivors: TargetRow[] = [];
  for (const row of rows) {
    const value = redactFields(row.value, fields, counts);
    if (row.targetKind === kind && row.targetId === id) continue;
    const next = value === row.value ? row : { ...row, value };
    survivors.push(next);
    if (next !== row) after.push(...targetGuards(targets, row, destination, row.targetKind, row.targetId), ...targets.write(next));
    after.push(...targetGuards(targets, next, destination, row.targetKind, row.targetId));
  }
  const final = targets.matchesMany ? targets.matchesMany(survivors, destination).map(guard)
    : [guard(targets.matchesCount!(survivors.length, destination)), ...survivors.flatMap(row => targetGuards(targets, row, destination, row.targetKind, row.targetId))];
  return { before, after, final };
}
