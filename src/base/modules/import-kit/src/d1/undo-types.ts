import type { Json, TargetRow, ImportJob } from '../types.ts';
import type { D1Database, D1Sql, D1ProvenanceRow } from './types.ts';

export type D1ReversalPolicy = 'strict' | 'preserve-conflicts';
export interface D1UndoEntity { table: string; recordId: string; field?: string; targetId?: string }
export interface D1UndoEffect {
  provenance: D1ProvenanceRow;
  /** Immutable applied snapshot reconstructed by Kit from the reviewed plan. */
  expected: TargetRow;
  /** A previous batch made its retention terminal. */
  retained: boolean;
}
export interface D1UndoObservation {
  effectId: string;
  current: TargetRow | null;
  entity: D1UndoEntity;
  /** Optional exact observation predicate for domain absence with retained
   * ownership markers. Otherwise Kit uses targets.matches(current,...). */
  guard?: D1Sql;
  /** Domain evidence, e.g. the named native field and required relation rule. */
  evidence?: Json;
}
export interface D1UndoDependency {
  /** This effect must survive while the dependent survives, and must reverse
   * AFTER that dependent. Omit dependentEffectId for a live outside dependency. */
  requiredEffectId: string;
  dependentEffectId?: string;
  /** false makes this a retention-only edge (e.g. required MANY validity). */
  order?: false;
  cause: D1UndoEntity;
  evidence: Json;
}
export interface D1UndoSnapshot {
  observations: D1UndoObservation[];
  /** Optional complete destination set from targets.list, including unrelated
   * rows. Kit checks every current observation against this set (null means
   * absent), then fences it through targets.matchesMany. This requires that
   * adapter method; it does not replace custom observation/dependency guards. */
  nativeRows?: TargetRow[];
  dependencies: D1UndoDependency[];
  /** Domain-required atomic reversal units, e.g. required MANY owner + joins.
   * Overlapping groups are united. Retaining any member retains the unit. */
  atomicGroups?: string[][];
  /** Nonempty scalar predicates fencing the WHOLE observed native/dependency
   * set, including absent/incoming edges and cardinalities. No mutations. */
  guards: D1Sql[];
}
export interface D1PreservedUndoAdapter {
  preparePreservedUndo(database: D1Database, destination: string, effects: D1UndoEffect[]): Promise<D1UndoSnapshot>;
  /** Pure ownership projection applied by Kit to BOTH expected and current
   * snapshots. May exclude unrelated shared revisions/fields. Never omit an
   * owned value, attachment or owned revision. Full snapshots remain guarded. */
  ownedContent?(row: TargetRow): Json;
}
export interface D1UndoEvidence {
  revision: string | null;
  valueHash: string | null;
  attachmentHash: string | null;
  ownedHash: string | null;
  ownedValue: Json;
}
export interface D1UndoDisposition {
  /** Privacy redaction preserves the disposition while removing personal
   * evidence. An erased entity is identified only by its source digest. */
  redacted?: 'erased' | 'evidence';
  sourceIdentity?: string;
  jobId: string;
  effectId: string;
  targetKind: string;
  targetId: string;
  entity: D1UndoEntity;
  disposition: 'reversed' | 'retained';
  reason: 'unchanged' | 'edited' | 'dependency';
  expected: D1UndoEvidence;
  observed: D1UndoEvidence;
  protectedBy: D1UndoDependency[];
  evidence: Json;
}
export interface D1UndoReport {
  job: ImportJob;
  policy: D1ReversalPolicy;
  cursor: number;
  total: number;
  nextOffset: number | null;
  dispositions: D1UndoDisposition[];
}
