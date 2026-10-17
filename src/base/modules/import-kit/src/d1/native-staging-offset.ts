import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, readAtomic, sql } from './database.ts';
import { guardPredicate } from './guards.ts';
import { nativeStageGuards, type NativeStageBinding } from './native-staging-state.ts';

export type NativeStageTable = 'index' | 'values';
export interface NativeStageProgress { written: number; revision: number }
export type NativeStageCheckpoint = (from: NativeStageProgress, to: NativeStageProgress) => D1Sql[];
declare const verifiedOffset: unique symbol;
/** Only a successful read fence can mint this capability; object casts are inert. */
export interface VerifiedNativeStageOffset { readonly [verifiedOffset]: true }
interface OffsetBinding extends NativeStageProgress {
  context: D1ImportContext; database: D1ImportContext['storage']['database'];
  stage: string; baseline: string; table: NativeStageTable;
}
const offsets = new WeakMap<VerifiedNativeStageOffset, OffsetBinding>();
const columns = {
  index: Object.freeze(['ordinal', 'source_identity', 'target_identity']),
  values: Object.freeze(['value_ordinal', 'ordinal', 'body', 'body_hash', 'body_size', 'value_chunks']),
};
const invalid = (): never => { throw new Error('IMPORT_NATIVE_STAGE_OFFSET_INVALID'); };
const integer = (n: number) => Number.isSafeInteger(n) && n >= 0;
function baselineIdentity(baseline: D1Sql[]): string {
  const hash = createHash('sha256');
  for (const statement of baseline) {
    const scalar = guardPredicate(statement);
    if (!scalar) invalid();
    hash.update(JSON.stringify([scalar!.sql, (scalar!.params ?? []).map(value =>
      value instanceof ArrayBuffer || ArrayBuffer.isView(value)
        ? ['bytes', Buffer.from(value instanceof ArrayBuffer ? new Uint8Array(value) : new Uint8Array(value.buffer, value.byteOffset, value.byteLength)).toString('base64')]
        : value)]) + '\n');
  }
  return hash.digest('hex');
}
export function validateNativeStageRows(context: D1ImportContext, stage: NativeStageBinding, table: NativeStageTable, selected?: string[]): void {
  if ((table !== 'index' && table !== 'values') ||
      context.destination !== stage.destination || context.namespace !== stage.namespace ||
      context.mappingVersion !== stage.mapping_version || context.authorization.principalId !== stage.principal_id ||
      ![stage.identities, stage.chunks_count, stage.values_count, stage.job_fence].every(integer)) invalid();
  if (selected && (selected.length !== columns[table].length || selected.some((column, i) => column !== columns[table][i]))) invalid();
}
/** Revisions count every row mutation, so count equality proves an append-only
 * prefix without fetching its bodies again. Registration separates the phases. */
export function nativeStageOffsetGuards(stage: NativeStageBinding, table: NativeStageTable, written: number, revision: number): D1Sql[] {
  if ((table !== 'index' && table !== 'values') || !integer(written) || !integer(revision) ||
      ![stage.identities, stage.chunks_count, stage.values_count].every(integer) ||
      written > (table === 'index' ? stage.identities : stage.chunks_count) ||
      revision !== written + (table === 'values' ? stage.identities : 0)) invalid();
  const indexes = table === 'index' ? written : stage.identities;
  const values = table === 'values' ? written : 0;
  return [...nativeStageGuards(stage, revision), guard(sql(
    `EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=? AND sealed_revision IS NULL AND registered_revision ${table === 'index' ? 'IS NULL' : 'IS ?'}) AND (SELECT count(*) FROM ik_d1_native_staging_index WHERE stage_id=?)=? AND (SELECT count(*) FROM ik_d1_native_staging_values WHERE stage_id=?)=?`,
    stage.id, ...(table === 'values' ? [stage.identities] : []), stage.id, indexes, stage.id, values))];
}
export async function verifyNativeStageOffset(context: D1ImportContext, stage: NativeStageBinding, baseline: D1Sql[], table: NativeStageTable, checkpoint: NativeStageProgress & { guards: D1Sql[] }): Promise<VerifiedNativeStageOffset> {
  validateNativeStageRows(context, stage, table);
  if (!checkpoint || !Array.isArray(checkpoint.guards) || !checkpoint.guards.length || checkpoint.guards.some(query => !guardPredicate(query))) invalid();
  const binding: OffsetBinding = { context, database: context.storage.database, stage: JSON.stringify(stage), baseline: baselineIdentity(baseline), table, written: checkpoint.written, revision: checkpoint.revision };
  const checks = [...baseline, ...nativeStageOffsetGuards(stage, table, binding.written, binding.revision), ...checkpoint.guards];
  await readAtomic(context, checks, undefined, stage.action);
  // Reject mutation of caller-owned binding objects across the awaited fence.
  if (JSON.stringify(stage) !== binding.stage || baselineIdentity(baseline) !== binding.baseline || context.storage.database !== binding.database) invalid();
  validateNativeStageRows(context, stage, table);
  const token = Object.freeze({}) as VerifiedNativeStageOffset;
  offsets.set(token, binding);
  return token;
}
export function nativeStageOffsetProgress(token: VerifiedNativeStageOffset, context: D1ImportContext, stage: NativeStageBinding, baseline: D1Sql[], table: NativeStageTable): NativeStageProgress {
  const binding = token && typeof token === 'object' ? offsets.get(token) : undefined;
  if (!binding || binding.context !== context || binding.database !== context.storage.database || binding.stage !== JSON.stringify(stage) || binding.baseline !== baselineIdentity(baseline) || binding.table !== table) invalid();
  return { written: binding!.written, revision: binding!.revision };
}
