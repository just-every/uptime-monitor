import type { ImportAction, ImportContext, TargetRow, ProvenanceRow, ImportPlan, Json } from '../types.ts';
import type { D1PreservedUndoAdapter, D1ReversalPolicy } from './undo-types.ts';
/** Structural subset of the Cloudflare D1 binding; no provider package required. */
export interface D1Statement {
  bind(...values: any[]): D1Statement;
  first<T = Record<string, any>>(): Promise<T | null>;
  all<T = Record<string, any>>(): Promise<{ results: T[] }>;
}
export interface D1Database {
  prepare(sql: string): D1Statement;
  batch(statements: D1Statement[]): Promise<unknown[]>;
}
export interface D1Sql {
  sql: string; params?: any[];
  /** Internal exact preflight and maintained intended-state descriptions. */
  finalChecks?: D1Sql[];
  expectedSnapshots?: import('./expected-state.ts').ExpectedSnapshot[];
  expectedChanges?: import('./expected-state.ts').ExpectedChange[] | (() => import('./expected-state.ts').ExpectedChange[]);
  resultKey?: symbol;
  /** Produced by retainedSql; never mark authorization or original-byte input. */
  retainedValues?: { parts: string[]; indexes: number[]; privacyPlan?: ImportPlan; privacyJobId?: string };
}
/** Native SQL must be inert and confined to this database. matches must cover the
 * entire row (including evidence, revision and attachments), not just an ID. */
export interface D1PlanCodec {
  id: string;
  /** Pure deterministic lossless representation. Both roundtrips preserve canonical
   * bytes; unused encoded fields are refused when loading retained custody. */
  encode(plan: ImportPlan): Json;
  decode(payload: Json): ImportPlan;
}
export interface D1TargetAdapter extends Partial<D1PreservedUndoAdapter> {
  planCodec?: D1PlanCodec;
  /** Optional transport staging support. Guards must fence every native field
   * from before the first read, including same-revision edits and absence. */
  stagingRead?(db: D1Database, destination: string, kind: string, id: string): Promise<{ row: TargetRow | undefined; guards: D1Sql[] }>;
  /** Protect exact post-state during cleanup, install a known invalidatable
   * receipt after exact checks, then release. Final scalar predicates run
   * read-only after every mutation and must bind the expected post-state. */
  stagingPublication?(stageId: string, destination: string, targets: { kind: string; id: string; present: boolean }[]): { protect: D1Sql[]; seal: D1Sql[]; release: D1Sql[]; final: D1Sql[] };
  read(db: D1Database, destination: string, kind: string, id: string): Promise<TargetRow | undefined>;
  matches(row: TargetRow | null, destination: string, kind: string, id: string): D1Sql;
  /** Optional exact row snapshot split into conjunctive predicates. All parts,
   * including absence/cardinality, commit in the same native transaction. */
  matchesParts?(row: TargetRow | null, destination: string, kind: string, id: string): D1Sql[];
  /** Nonempty exact full-set predicates, including native cardinality and all
   * evidence, even when rows is empty. */
  matchesMany?(rows: TargetRow[], destination: string): D1Sql[];
  /** Exact cardinality of the destination's native set. Export requires this
   * with individual matches, or matchesMany with equivalent full-set coverage. */
  matchesCount?(count: number, destination: string): D1Sql;
  write(row: TargetRow): D1Sql[];
  remove(destination: string, kind: string, id: string): D1Sql[];
  list(db: D1Database, destination: string): Promise<TargetRow[]>;
  /** Optional field-redaction scope, so its cost follows the affected rows, not
   * the destination: `rows` are exactly the destination's rows whose native
   * value holds one of `fields` (exact path and value). `holding` gives nonempty
   * predicates that exactly the given rows hold one; `matches`, set predicates
   * that each given row matches its snapshot; `write` rewrites existing rows. */
  redaction?(db: D1Database, destination: string, fields: readonly D1FieldRedaction[]): Promise<D1RedactionScope>;
}
export interface D1RedactionScope {
  rows: TargetRow[];
  holding(rows: TargetRow[]): D1Sql[];
  matches(rows: TargetRow[]): D1Sql[];
  write(rows: TargetRow[]): D1Sql[];
}
/** Product recognition of an erased person's data outside the erased target's own
 * sources: another record's mapped values, a rejected row, or an original's bytes.
 * Called with decoded retained content only; must be pure and must not log it. */
export interface D1RelatedErasure {
  /** Optional readD1ImportFieldMatches snapshot. Fences optional-selector
   * discovery and cancels incomplete private preparations even with no fields. */
  snapshot?: string;
  /** True when this retained source record holds the erased person's data. */
  source?(record: import('../types.ts').SourceRecord): boolean;
  /** True when this retained original's exact bytes hold the erased person's data. */
  original?(bytes: Uint8Array, original: { name: string; sha256: string }): boolean;
  /** Remove exact string values at named paths in retained source payloads,
   * effect/native values and reversal before-images. `*` visits array items.
   * Matching array items are removed; matching object properties are deleted.
   * Peer records and explicit aggregate counts remain. Each distinct path must
   * match at least one exact leaf across all supplied values or erasure refuses. */
  fields?: readonly D1FieldRedaction[];
}
export interface D1FieldRedaction {
  path: readonly (string | number)[];
  value: string;
  /** An optional reference (an alternate identifier the person may not have):
   * its path may match nothing. A path with a required selector must match. */
  optional?: boolean;
}
/** Exact leaf occurrences per distinct selector path, never selected values. */
export interface D1FieldMatch {
  path: readonly (string | number)[];
  matches: number;
}
export interface D1ImportContext extends Pick<ImportContext, 'destination' | 'namespace' | 'mappingVersion' | 'reviewedFingerprint' | 'operationKey' | 'batchSize' | 'maxBatches'> {
  /** Selected at creation. Omission on later calls uses the durable policy;
   * an explicit different policy is an error. Legacy jobs remain strict. */
  reversalPolicy?: D1ReversalPolicy;
  /** Explicit privacy choice for this erasure; recorded in each changed plan.
   * Whole affected artifacts become unavailable, including other records' bytes. */
  originalErasurePolicy?: import('../types.ts').OriginalErasurePolicy;
  /** Optional caller transport policy. SELECT fences always have a 2 MiB ceiling
   * and honor a lower selected limit. Native mutations gather fewer whole entries
   * to satisfy an explicitly selected bound. */
  maxBatchBytes?: number;
  storage: { database: D1Database; targets: D1TargetAdapter };
  authorization: {
    principalId: string;
    /** Scalar SQL boolean consulting current session, actor, grant and expiry.
     * Evaluated inside the same atomic batch, before AND after native effects. */
    predicate(action: ImportAction, destination: string): D1Sql;
  };
}

/** The predecessor marker stays in successor audit JSON even after reversal. */
export interface D1ProvenanceRow extends ProvenanceRow { predecessorId?: string }
/** A person named for erasure from import custody, by exact identifiers only.
 * `target` is the native entity their records were imported into; `values` are
 * exact strings (an email, a label the product built from the person) that may
 * sit whole in a job or stream header, an original's name or a private stage.
 * Nothing is ever matched as a substring. */
export interface D1ImportPerson {
  target?: { kind: string; id: string };
  values?: string[];
  related?: D1RelatedErasure;
}
/** One erasure journal row: what was erased and where, never the values. */
export interface D1ErasureJournalEntry {
  receiptId: string;
  destination: string;
  recordedAt: string;
  recordsErased: number;
  jobsRetired: number;
  streamsRetired: number;
  stores: { store: string; rows: number }[];
}
/** `resumable`: a redaction too large for one request commits bounded steps and
 * returns `D1RetirementProgress`; call again in a new request with its
 * `continuation` until the result's phase is `complete`. */
export interface D1RetirementOptions { resumable?: boolean; continuation?: string }
/** A committed step of a resumable redaction. `receiptId` names its journal row
 * when it rewrote custody. Keep `continuation` server-side; it is bound to this
 * person's selectors and destination and holds no selected value. */
export interface D1RetirementProgress {
  phase: 'redacting';
  continuation: string;
  jobsRedacted: number;
  jobsPending: number;
  fieldMatches: D1FieldMatch[];
  receiptId?: string;
}
export interface D1CustodyRetirement extends D1ErasureJournalEntry {
  phase: 'complete';
  fieldMatches: D1FieldMatch[];
  /** Jobs and streams whose whole custody was retired: no undo or resume. */
  retiredJobs: string[];
  retiredStreams: string[];
  nativeResults: unknown[];
}
