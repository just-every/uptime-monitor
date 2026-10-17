import { Buffer } from 'node:buffer';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { nativeStageAtomic, type NativeStageBinding } from './native-staging-state.ts';
import { nativeStageOffsetGuards, nativeStageOffsetProgress, validateNativeStageRows, type NativeStageCheckpoint, type VerifiedNativeStageOffset } from './native-staging-offset.ts';
export { verifyNativeStageOffset, type VerifiedNativeStageOffset, type NativeStageCheckpoint, type NativeStageProgress } from './native-staging-offset.ts';
export const NATIVE_STAGE_ROWS_BYTES = 512 * 1024;
/** Both insertion and atomic equality bind the body. Leave the rest of a
 * smaller caller-selected transport limit for escaping, guards and journals. */
export const nativeStageRowsBytes = (context:D1ImportContext) => Math.min(NATIVE_STAGE_ROWS_BYTES, Math.floor(Math.min(context.maxBatchBytes??2*1024*1024,2*1024*1024)/4));
export interface NativeStageRowsOptions { offset?: VerifiedNativeStageOffset; checkpoint?: NativeStageCheckpoint }

/** Replay deterministic input from its beginning; only authenticated prior rows
 * are skipped. Each new batch and its journal advance commit together. */
export class NativeStageRows {
  revision = 0;
  private committed = 0;
  private replay = 0;
  private skipped = 0;
  private rows: string[] = [];
  private bytes = 2;
  private context: D1ImportContext;
  private stage: NativeStageBinding;
  private baseline: D1Sql[];
  private table: 'index' | 'values';
  private columns: string[];
  private checkpoint?: NativeStageCheckpoint;
  private rowBytes: number;
  constructor(context: D1ImportContext, stage: NativeStageBinding, baseline: D1Sql[], table: 'index' | 'values', columns: string[], options: NativeStageRowsOptions = {}) {
    validateNativeStageRows(context, stage, table, columns);
    if (!options || typeof options !== 'object' || Array.isArray(options) || Object.keys(options).some(key => key !== 'offset' && key !== 'checkpoint') || options.checkpoint !== undefined && typeof options.checkpoint !== 'function') throw new Error('IMPORT_NATIVE_STAGE_OFFSET_INVALID');
    this.context = context; this.stage = {...stage}; this.baseline = [...baseline]; this.table = table; this.columns = [...columns]; this.checkpoint = options.checkpoint;
    this.rowBytes = nativeStageRowsBytes(context);
    if (Object.hasOwn(options, 'offset')) {
      const progress = nativeStageOffsetProgress(options.offset!, context, stage, baseline, table);
      this.committed = this.replay = progress.written; this.revision = progress.revision;
    }
  }
  get written(): number { return this.committed; }
  async append(row: unknown[]): Promise<void> {
    if (this.skipped < this.replay) { this.skipped++; return; }
    const text = JSON.stringify(row), bytes = Buffer.byteLength(text) + 1;
    if (bytes + 2 > this.rowBytes) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
    if (this.rows.length && this.bytes + bytes > this.rowBytes) await this.flush();
    this.rows.push(text); this.bytes += bytes;
  }
  async flush(): Promise<void> {
    if (this.skipped < this.replay) throw new Error('IMPORT_NATIVE_STAGE_OFFSET_INPUT_INCOMPLETE');
    if (!this.rows.length) return;
    const body = '[' + this.rows.join(',') + ']', count = this.rows.length;
    const writes = nativeRowStatements(this.stage, this.baseline, this.table, this.columns, body, count, this.committed, this.revision, this.checkpoint);
    const result = await nativeStageAtomic(this.context, this.stage.action, writes), read = result[result.length - 4] as any;
    const rows = Array.isArray(read) ? read : read?.results, expected = JSON.parse(body) as unknown[][];
    if (!Array.isArray(rows) || rows.length !== count || rows.some((row, i) => this.columns.some((column, j) => row[column] !== expected[i][j]))) throw new Error('IMPORT_NATIVE_STAGE_CONTENT_CHANGED');
    this.revision += count; this.committed += count;
    this.rows = []; this.bytes = 2;
  }
}

export function nativeRowStatements(stage: NativeStageBinding, baseline: D1Sql[], table: 'index' | 'values', columns: string[], body: string, count: number, written: number, revision: number, checkpoint?: NativeStageCheckpoint): D1Sql[] {
  if (!Number.isSafeInteger(count) || count < 1) throw new Error('IMPORT_NATIVE_STAGE_OFFSET_INVALID');
  const before = nativeStageOffsetGuards(stage, table, written, revision);
  const after = nativeStageOffsetGuards(stage, table, written + count, revision + count);
  const progress = checkpoint?.({written, revision}, {written: written + count, revision: revision + count}) ?? [];
  return [...baseline, ...before,
    sql(`INSERT INTO ik_d1_native_staging_${table}(stage_id,${columns.join(',')}) SELECT ?,${columns.map((_, i) => `json_extract(value,'$[${i}]')`).join(',')} FROM json_each(?)`, stage.id, body),
    ...progress, ...baseline, ...after,
    // Check inside the transaction, including changes caused by journal triggers.
    // One extra body binding leaves room for guards under the 2 MiB ceiling.
    guard(sql(`CASE WHEN NOT EXISTS(SELECT 1 FROM json_each(?) expected WHERE NOT EXISTS(SELECT 1 FROM ik_d1_native_staging_${table} actual WHERE actual.stage_id=? AND ${columns.map((column, i) => `actual.${column} IS json_extract(expected.value,'$[${i}]')`).join(' AND ')})) THEN 1 ELSE json_extract('{}','$IMPORT_NATIVE_STAGE_CONTENT_CHANGED') END`, body, stage.id)),
    sql(`SELECT ${columns.join(',')} FROM ik_d1_native_staging_${table} WHERE stage_id=? ORDER BY rowid LIMIT ? OFFSET ?`, stage.id, count, written)];
}
