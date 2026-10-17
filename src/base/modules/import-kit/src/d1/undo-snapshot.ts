import type { D1ImportContext, D1Sql } from './types.ts';
import type { D1UndoEffect, D1UndoDisposition, D1UndoSnapshot } from './undo-types.ts';
import { canonicalJson, assertNoCredentials } from '../provenance/identity.ts';
import { undoEvidence } from './undo-state.ts';
import { guard } from './database.ts';
import { observationGuards } from './undo-observations.ts';

const validEntity = (e: any) => e && typeof e.table === 'string' && !!e.table && typeof e.recordId === 'string' && !!e.recordId && (e.field === undefined || typeof e.field === 'string') && (e.targetId === undefined || typeof e.targetId === 'string');
/** Ownership is evaluated in application code; every decision is fenced by the
 * adapter's exact, complete current snapshot inside the publication transaction. */
export function classifyUndo(effects: D1UndoEffect[], snapshot: D1UndoSnapshot, context: D1ImportContext): { ordered: D1UndoDisposition[]; groups: D1UndoDisposition[][]; guards: D1Sql[]; observations: Map<string, D1UndoSnapshot['observations'][number]> } {
  if (!snapshot || !Array.isArray(snapshot.observations) || !Array.isArray(snapshot.dependencies) || !Array.isArray(snapshot.guards) || !snapshot.guards.length) throw new Error('IMPORT_UNDO_SNAPSHOT_REQUIRED');
  const expected = new Map(effects.map(e => [e.provenance.id, e]));
  const observations = new Map(snapshot.observations.map(o => [o.effectId, o]));
  if (expected.size !== effects.length || observations.size !== snapshot.observations.length || observations.size !== expected.size) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
  const guards = [...snapshot.guards.map(guard), ...observationGuards(effects, snapshot, context)], dispositions = new Map<string, D1UndoDisposition>(), retained = new Set<string>();
  for (const o of snapshot.observations) {
    const effect = expected.get(o.effectId), row = o.current;
    if (!effect || !validEntity(o.entity) || (row !== null && (!row || row.destination !== context.destination || row.targetKind !== effect.expected.targetKind || row.targetId !== effect.expected.targetId || typeof row.revision !== 'string'))) throw new Error('IMPORT_UNDO_SNAPSHOT_MISMATCH');
    const applied = undoEvidence(effect.expected, context), observed = undoEvidence(row, context);
    // A replacement layer must restore the exact historical before image. An
    // ownership projection cannot relax the predecessor's equality contract.
    const strictReplacement = !!effect.provenance.before;
    const unchanged = row !== null && applied.ownedHash === observed.ownedHash && (!strictReplacement || (applied.revision === observed.revision && applied.valueHash === observed.valueHash && applied.attachmentHash === observed.attachmentHash));
    if (!unchanged || effect.retained) retained.add(o.effectId);
    const d: D1UndoDisposition = { jobId: effect.provenance.jobId, effectId: o.effectId, targetKind: effect.expected.targetKind, targetId: effect.expected.targetId, entity: o.entity, disposition: unchanged ? 'reversed' : 'retained', reason: unchanged ? 'unchanged' : 'edited', expected: applied, observed, protectedBy: [], evidence: o.evidence ?? null };
    assertNoCredentials(d); dispositions.set(o.effectId, d);
  }
  const dependencies = [...snapshot.dependencies];
  const groupRoots = new Map(effects.map(e => [e.provenance.id, e.provenance.id]));
  const root = (id: string): string => { while (groupRoots.get(id) !== id) id = groupRoots.get(id)!; return id; };
  if (snapshot.atomicGroups !== undefined && !Array.isArray(snapshot.atomicGroups)) throw new Error('IMPORT_UNDO_GROUP_MISMATCH');
  for (const group of snapshot.atomicGroups ?? []) {
    if (!Array.isArray(group) || !group.length || new Set(group).size !== group.length || group.some(id => !expected.has(id))) throw new Error('IMPORT_UNDO_GROUP_MISMATCH');
    for (const id of group) groupRoots.set(root(id), root(group[0]));
  }
  const units = new Map<string, string[]>();
  for (const id of expected.keys()) { const key = root(id); units.set(key, [...(units.get(key) ?? []), id]); }
  const keys = new Set<string>();
  for (const edge of dependencies) {
    if (!edge || !expected.has(edge.requiredEffectId) || (edge.dependentEffectId !== undefined && !expected.has(edge.dependentEffectId)) || !validEntity(edge.cause) || (edge.order !== undefined && edge.order !== false) || edge.evidence === undefined) throw new Error('IMPORT_UNDO_DEPENDENCY_MISMATCH');
    const key = canonicalJson(edge);
    if (keys.has(key)) throw new Error('IMPORT_UNDO_DEPENDENCY_MISMATCH');
    keys.add(key); assertNoCredentials(edge);
    if (edge.dependentEffectId === undefined) retained.add(edge.requiredEffectId);
  }
  let changed = true;
  while (changed) {
    changed = false;
    for (const edge of dependencies) if ((edge.dependentEffectId === undefined || retained.has(edge.dependentEffectId)) && !retained.has(edge.requiredEffectId)) { retained.add(edge.requiredEffectId); changed = true; }
    for (const unit of units.values()) {
      const cause = unit.find(id => retained.has(id));
      if (cause === undefined) continue;
      for (const id of unit) if (!retained.has(id)) {
        retained.add(id); changed = true;
        dependencies.push({ requiredEffectId: id, dependentEffectId: cause, order: false, cause: dispositions.get(cause)!.entity, evidence: { atomicGroup: root(id), reason: 'atomic-reversal-unit' } });
      }
    }
  }
  for (const id of retained) {
    const d = dispositions.get(id)!;
    if (d.reason !== 'edited') d.reason = 'dependency';
    d.disposition = 'retained';
    d.protectedBy = dependencies.filter(e => e.requiredEffectId === id && (e.dependentEffectId === undefined || retained.has(e.dependentEffectId)));
  }
  const pending = effects.filter(e => !e.retained).map(e => e.provenance.id), groups: D1UndoDisposition[][] = [];
  // Retentions are individually durable. Every removable atomic unit is
  // topologically ordered both against other units and within its own effects.
  for (const id of pending.filter(id => retained.has(id))) groups.push([dispositions.get(id)!]);
  const removing = new Set(pending.filter(id => !retained.has(id)));
  const orderMembers = (members: string[]) => {
    const left = new Set(members), ordered: D1UndoDisposition[] = [];
    while (left.size) {
      const ready = members.find(id => left.has(id) && !dependencies.some(e => e.order !== false && e.requiredEffectId === id && e.dependentEffectId !== undefined && left.has(e.dependentEffectId)));
      if (ready === undefined) throw new Error('IMPORT_UNDO_DEPENDENCY_CYCLE');
      ordered.push(dispositions.get(ready)!); left.delete(ready);
    }
    return ordered;
  };
  while (removing.size) {
    const ready = pending.find(id => removing.has(id) && !dependencies.some(e => e.order !== false && root(e.requiredEffectId) === root(id) && e.dependentEffectId !== undefined && removing.has(e.dependentEffectId) && root(e.dependentEffectId) !== root(id)));
    if (ready === undefined) throw new Error('IMPORT_UNDO_DEPENDENCY_CYCLE');
    const members = units.get(root(ready))!.filter(id => removing.has(id));
    groups.push(orderMembers(members)); members.forEach(id => removing.delete(id));
  }
  return { ordered: groups.flat(), groups, guards, observations };
}
