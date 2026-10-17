import type { D1FieldMatch, D1ImportContext, D1Sql } from './types.ts';
import { canonicalJson, requireText, sha256 } from '../provenance/identity.ts';
import { guard, sql } from './database.ts';
import { requestContext, limitStatements } from './budget.ts';

export interface D1ErasureStepOptions {
  /** Stable product operation identity. Reuse it after a lost acknowledgement. */
  operationKey: string;
  /** Maximum executed SQL statements in this invocation, including reads. */
  maxStatements?: number;
  /** Compile and admit one future step without retaining or changing any data. */
  admissionOnly?: boolean;
}
export interface D1ErasurePending {
  phase: 'pending'; operationId: string; indexRows: number; valueRows: number;
}
export interface D1ErasureComplete {
  phase: 'complete'; operationId: string; erasedRecords: number;
  fieldMatches: D1FieldMatch[]; nativeResults: unknown[];
}
export type D1ErasureStep = D1ErasurePending | D1ErasureComplete;
export interface ErasureProgress {
  id: string; destination: string; request_hash: string; generation: string;
  phase: 'index' | 'values' | 'sealed' | 'complete'; index_rows: number;
  value_rows: number; revision: number; records_erased: number;
  field_matches_json: string; created_at: string;
}
export const erasureStatementBinding = (statements:D1Sql[]) => statements.map(row => [row.sql, (row.params??[]).map(value =>
  value instanceof ArrayBuffer || ArrayBuffer.isView(value) ? ['bytes', sha256(value instanceof ArrayBuffer ? new Uint8Array(value) : new Uint8Array(value.buffer,value.byteOffset,value.byteLength))] : value)]);
export function erasureRequestContext(input: D1ImportContext, options: D1ErasureStepOptions): D1ImportContext {
  if (!options || typeof options !== 'object' || Array.isArray(options) || Object.keys(options).some(key => !['operationKey', 'maxStatements', 'admissionOnly'].includes(key))) throw new Error('IMPORT_ERASURE_OPTIONS_INVALID');
  const max = options.maxStatements ?? 1000;
  if (!Number.isSafeInteger(max) || max < 1 || max > 1000) throw new Error('INVALID_ERASURE_STATEMENT_LIMIT');
  const bytes = input.maxBatchBytes ?? 2 * 1024 * 1024;
  if (!Number.isSafeInteger(bytes) || bytes < 1) throw new Error('INVALID_TRANSPORT_LIMIT');
  const context = requestContext({ ...input, maxBatchBytes: Math.min(bytes, 2 * 1024 * 1024) });
  return limitStatements(context, max);
}
export function operationIdentity(context: D1ImportContext, options: D1ErasureStepOptions, binding: string) {
  requireText(options.operationKey, 'erasure operationKey'); requireText(binding, 'erasure binding');
  return {
    id: sha256(canonicalJson([context.destination, context.authorization.principalId, options.operationKey])),
    requestHash: sha256(canonicalJson([context.destination, context.namespace, context.mappingVersion, context.authorization.principalId, context.originalErasurePolicy ?? null, binding])),
  };
}
export async function readErasureProgress(context: D1ImportContext, id: string, requestHash: string): Promise<ErasureProgress | undefined> {
  const row = await context.storage.database.prepare('SELECT * FROM ik_d1_erasure_progress WHERE id=?').bind(id).first<ErasureProgress>();
  if (!row) return undefined;
  if (row.destination !== context.destination || row.request_hash !== requestHash) throw new Error('IMPORT_ERASURE_OPERATION_CHANGED');
  return row;
}
export const progressGuard = (row: ErasureProgress) => guard(sql('EXISTS(SELECT 1 FROM ik_d1_erasure_progress WHERE id=? AND request_hash=? AND generation=? AND phase=? AND index_rows=? AND value_rows=? AND revision=?)', row.id, row.request_hash, row.generation, row.phase, row.index_rows, row.value_rows, row.revision));
export function progressInsert(row: ErasureProgress): D1Sql {
  return sql(`INSERT INTO ik_d1_erasure_progress(${Object.keys(row).join(',')}) VALUES(${Object.keys(row).map(() => '?').join(',')})`, ...Object.values(row));
}
export function progressChange(from: ErasureProgress, changes: Partial<ErasureProgress>): D1Sql[] {
  const next = { ...from, ...changes }, keys = Object.keys(changes) as (keyof ErasureProgress)[];
  return [progressGuard(from), sql(`UPDATE ik_d1_erasure_progress SET ${keys.map(key => `${key}=?`).join(',')} WHERE id=?`, ...keys.map(key => next[key]), from.id), progressGuard(next)];
}
export const pendingErasure = (row: ErasureProgress): D1ErasurePending => ({ phase: 'pending', operationId: row.id, indexRows: row.index_rows, valueRows: row.value_rows });
export const completedErasure = (row: ErasureProgress, nativeResults: unknown[] = []): D1ErasureComplete => ({ phase: 'complete', operationId: row.id, erasedRecords: row.records_erased, fieldMatches: JSON.parse(row.field_matches_json), nativeResults });
