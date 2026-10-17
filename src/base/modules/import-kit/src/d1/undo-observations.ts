import type { D1ImportContext, D1Sql } from './types.ts';
import type { D1UndoEffect, D1UndoSnapshot } from './undo-types.ts';
import { canonicalJson, targetIdentity } from '../provenance/identity.ts';
import { guard } from './database.ts';

/** A full native set can fence an imported subset, including absent targets.
 * The supplied observations must first agree byte-for-byte with that set; an
 * unrelated native row cannot be used as evidence for a selected effect. */
export function observationGuards(effects: D1UndoEffect[], snapshot: D1UndoSnapshot, context: D1ImportContext): D1Sql[] {
  const { targets } = context.storage, guards: D1Sql[] = [];
  const expected = new Map(effects.map(e => [e.provenance.id, e.expected]));
  if (snapshot.nativeRows !== undefined) {
    if (!Array.isArray(snapshot.nativeRows) || !targets.matchesMany) throw new Error('IMPORT_NATIVE_SET_GUARD_REQUIRED');
    const native = new Map<string, string>();
    for (const row of snapshot.nativeRows) {
      if (!row || row.destination !== context.destination || typeof row.targetKind !== 'string' || !row.targetKind || typeof row.targetId !== 'string' || !row.targetId || typeof row.revision !== 'string') throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
      const key = targetIdentity(row.destination, row.targetKind, row.targetId);
      if (native.has(key)) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
      native.set(key, canonicalJson(row));
    }
    for (const observation of snapshot.observations) {
      const effect = expected.get(observation.effectId);
      if (!effect) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
      const encoded = native.get(targetIdentity(context.destination, effect.targetKind, effect.targetId));
      if (encoded === undefined ? observation.current !== null : observation.current === null || canonicalJson(observation.current) !== encoded) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
    }
    const predicates = targets.matchesMany(snapshot.nativeRows, context.destination);
    if (!Array.isArray(predicates) || !predicates.length) throw new Error('IMPORT_NATIVE_SET_GUARD_REQUIRED');
    guards.push(...predicates.map(guard));
  }
  for (const observation of snapshot.observations) {
    const effect = expected.get(observation.effectId);
    if (!effect) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
    // Domain-specific absence/ownership markers remain additional constraints.
    if (observation.guard) guards.push(guard(observation.guard));
    else if (snapshot.nativeRows === undefined) guards.push(guard(targets.matches(observation.current, context.destination, effect.targetKind, effect.targetId)));
  }
  return guards;
}
