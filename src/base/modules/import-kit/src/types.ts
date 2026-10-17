/** All durable values are JSON; credentials belong only to reader call arguments. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface SourceLocation { artifact: string; row?: number; page?: number; pointer?: string }
export interface Diagnostic { severity: 'warning' | 'error'; code: string; message: string; kind?: string; sourceId?: string; location?: SourceLocation; field?: string }
export interface SourceRecord { namespace: string; kind: string; sourceId: string; payload: Json; payloadHash: string; artifactHash: string; location: SourceLocation }
/** Exact input bytes, never reserialized JSON. Omission is legacy metadata-only input. */
export type OriginalErasurePolicy = 'withdraw-affected-artifacts' | 'withdraw-all-job-artifacts';
export type UnavailableOriginal = { status: 'unavailable'; reason: 'not-supplied' } | { status: 'unavailable'; reason: 'unavailable-after-erasure'; policy: OriginalErasurePolicy };
export type ArtifactOriginal = { status: 'available'; base64: string } | UnavailableOriginal;
export interface SourceArtifact { name: string; sha256: string; size: number; original?: ArtifactOriginal }
/** Durable, job-bound custody readback. No reference/URL can claim byte retention. */
export interface RetainedOriginal { jobId: string; kind: 'artifact' | 'attachment'; key: string; name: string; sha256: string; size: number; original: { status: 'retained'; base64: string } | UnavailableOriginal }
/** Exact noncredential pagination query key; strict credential policy when omitted. */
export type PaginationCursorPolicy = 'page-token-v1';
export interface SourceBundle { paginationCursorPolicy?: PaginationCursorPolicy; records: SourceRecord[]; artifacts: SourceArtifact[]; diagnostics: Diagnostic[] }
export interface ReaderLimits { maxBytes?: number; maxRecords?: number; maxRecordBytes?: number }
export interface ParsedRecord { kind: string; sourceId: string; payload: Json; pointer?: string }
export interface FileSourceInput { paginationCursorPolicy?: PaginationCursorPolicy; namespace: string; name: string; bytes: Uint8Array | AsyncIterable<Uint8Array>; format: 'json' | 'ndjson'; parse: (value: Json, index: number) => ParsedRecord | ParsedRecord[] }
/** Object keys are decoded strings; array indices are numbers. */
export type JsonPathSegment = string | number;
export interface ApiPageContext { readonly numericLexemes: Readonly<Record<string, string>> }
export interface ApiSourceInput { paginationCursorPolicy?: PaginationCursorPolicy; namespace: string; url: string; token?: string; headers?: Record<string, string>; signal?: AbortSignal; maxRequests?: number; maxRetries?: number; maxRetryAfterMs?: number; limits?: ReaderLimits; preserveNumberLexeme?: (pathSegments: readonly JsonPathSegment[]) => boolean; parsePage: (value: Json, page: number, context: ApiPageContext) => { records: ParsedRecord[]; next?: string | null }; sleep?: (ms: number, signal?: AbortSignal) => Promise<void> }
export type ApiTransport = (request: Request) => Promise<Response>;
/** Effects are inert retained data, never executable jobs or grants. */
export interface TargetEffect { targetKind: string; targetId: string; value: Json; expectedRevision?: string; attachment?: { base64: string; sha256: string; mediaType: string } }
export interface MappingResult { effects: TargetEffect[]; unmapped?: { field?: string; reason: string }[]; rejection?: string }
export interface ImportMapping { version: string; map: (record: SourceRecord) => MappingResult }
export interface PlanEntry { source: SourceRecord; identity: string; effects: TargetEffect[]; unmapped: { field?: string; reason: string }[]; rejection?: string; erased?: true; erasedTargetIdentity?: string }
export interface ImportPlan { version: 1; destination: string; namespace: string; mappingVersion: string; principalId: string; source: SourceBundle; entries: PlanEntry[]; diagnostics: Diagnostic[]; counts: { input: number; unique: number; duplicates: number; mapped: number; unmapped: number; rejected: number; effects: number; byKind: Record<string, number> }; fingerprint: string; erasure?: { reviewedFingerprint: string; originalPolicy?: OriginalErasurePolicy } }
export type ImportAction = 'plan' | 'create' | 'apply' | 'undo' | 'rollback' | 'read' | 'erase';
export interface SqlStatement { get(...values: any[]): any; all(...values: any[]): any[]; run(...values: any[]): { changes: number | bigint } }
export interface SqlDatabase { exec(sql: string): void; prepare(sql: string): SqlStatement }
export interface TargetRow { destination: string; targetKind: string; targetId: string; value: Json; revision: string; attachment?: { base64: string; sha256: string; mediaType: string } }
/** Native adapter must use this database ONLY, synchronously, without external effects. */
export interface TargetAdapter { read(db: SqlDatabase, destination: string, kind: string, id: string): TargetRow | undefined; write(db: SqlDatabase, row: TargetRow): void; remove(db: SqlDatabase, destination: string, kind: string, id: string): void; list(db: SqlDatabase, destination: string): TargetRow[] }
export interface ImportStorage { database: SqlDatabase; targets: TargetAdapter; transaction<T>(body: () => T): T; close(): void }
export interface ImportContext { destination: string; namespace: string; mappingVersion: string; authorization: { principalId: string; /** Must synchronously consult current authority, preferably in this database; throw on denial. */ assertAuthorized: (action: ImportAction, destination: string, db: SqlDatabase) => void }; storage: ImportStorage; reviewedFingerprint?: string; operationKey?: string; batchSize?: number; maxBatches?: number; leaseMs?: number }
export type JobState = 'queued' | 'applying' | 'completed' | 'failed' | 'undoing' | 'undone' | 'rolling_back' | 'rolled_back';
export interface ImportJob { id: string; fingerprint: string; destination: string; namespace: string; mappingVersion: string; principalId: string; state: JobState; checkpoint: number; total: number; fence: number; error: string | null; erasedRecords?: number }
export interface ProvenanceRow { id: string; jobId: string; sourceIdentity: string; sourceKind: string; sourceId: string; sourceHash: string; artifactHash: string; location: SourceLocation; destination: string; targetKind: string; targetId: string; valueHash: string; attachmentHash: string | null; appliedRevision: string; before: TargetRow | null; reversed: boolean }
/** integrity is present only when durable SQL identities/JSON disagree. Independent readbacks may omit it. */
export interface PersistedImport { rows: TargetRow[]; provenance: ProvenanceRow[]; integrity?: Omit<Reconciliation, 'ok'> }
export interface Reconciliation { ok: boolean; missing: string[]; duplicate: string[]; extra: string[]; mismatched: string[] }
