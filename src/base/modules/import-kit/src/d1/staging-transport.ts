import { assertStagingIdentitiesClear } from './staging-privacy.ts';
import { stagingStatements } from './staging-statements.ts';
import { Buffer } from 'node:buffer';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, prepare, sql } from './database.ts';
import { D1RequestLimitError, remaining } from './budget.ts';

export const STAGING_BATCH_BYTES = 2 * 1024 * 1024;
export const STAGING_ROWS_BYTES = 768 * 1024;
export const STAGING_CHUNK_CHARS = 64 * 1024;
import { batchBytes as stagingBatchBytes } from './transport.ts';
export { batchBytes as stagingBatchBytes } from './transport.ts';
export async function stagingAtomic(context: D1ImportContext, statements: D1Sql[], returnReads = 0, release: D1Sql[] = [], final: D1Sql[] = []): Promise<unknown[]> {
  const packed = stagingStatements(context, 'create', statements, release, final);
  if (packed.length + returnReads > remaining(context)) throw new D1RequestLimitError();
  if (stagingBatchBytes(packed) > STAGING_BATCH_BYTES) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
  return context.storage.database.batch(packed.map(s => prepare(context.storage.database, s)));
}
export function* stagingChunks(body: string): Generator<string> {
  for (let start = 0; start < body.length;) {
    let end = Math.min(start + STAGING_CHUNK_CHARS, body.length);
    if (end < body.length && body.charCodeAt(end - 1) >= 0xd800 && body.charCodeAt(end - 1) <= 0xdbff) end--;
    yield body.slice(start, end); start = end;
  }
}
export interface StageBinding {
  id: string; generation: string; destination: string; namespace: string; mapping_version: string;
  principal_id: string; fingerprint: string; operation_key: string; total: number; pages: number; layout_hash: string; reversal_policy: 'strict' | 'preserve-conflicts';
}
export const bindingPredicate = (stage: StageBinding) => sql(`EXISTS(SELECT 1 FROM ik_d1_staging WHERE ${Object.keys(stage).map(key => `${key} IS ?`).join(' AND ')})`, ...Object.values(stage));
export const stageClear = (stage: StageBinding) => sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging_index i WHERE i.stage_id=? AND (EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=i.source_identity) OR EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=i.target_identity)))', stage.id, stage.destination, stage.destination);

/** Buffers one table's bounded JSON rows, not all pages or SQL statements. */
export class StageWriter {
  revision = 0;
  private parts: string[] = [];
  private bytes = 2;
  private written = 0;
  readonly context: D1ImportContext;
  readonly stage: StageBinding;
  readonly table: string;
  readonly columns: string[];
  constructor(context: D1ImportContext, stage: StageBinding, table: string, columns: string[]) {
    this.context = context; this.stage = stage; this.table = table; this.columns = columns;
  }
  async append(row: unknown[]): Promise<void> {
    const text = JSON.stringify(row), bytes = Buffer.byteLength(text) + 1;
    if (bytes + 2 > STAGING_ROWS_BYTES) throw new Error('IMPORT_STAGE_TRANSPORT_LIMIT');
    if (this.parts.length && this.bytes + bytes > STAGING_ROWS_BYTES) await this.flush();
    this.parts.push(text); this.bytes += bytes;
  }
  async flush(): Promise<void> {
    if (!this.parts.length) return;
    const count = this.parts.length, body = `[${this.parts.join(',')}]`;
    this.parts = []; this.bytes = 2;
    if (this.table === 'index') await assertStagingIdentitiesClear(this.context, this.stage.destination, body, 'staging');
    const writes = [guard(bindingPredicate(this.stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)', this.stage.id, this.revision)), guard(stageClear(this.stage)),
      sql(`INSERT INTO ik_d1_staging_${this.table}(stage_id,${this.columns.join(',')}) SELECT ?,${this.columns.map((_, index) => `json_extract(value,'$[${index}]')`).join(',')} FROM json_each(?)`, this.stage.id, body),
      guard(bindingPredicate(this.stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)', this.stage.id, this.revision + count)), guard(stageClear(this.stage)),
      sql(`SELECT ${this.columns.join(',')} FROM ik_d1_staging_${this.table} WHERE stage_id=? ORDER BY rowid LIMIT ? OFFSET ?`, this.stage.id, count, this.written)];
    const results = await stagingAtomic(this.context, writes);
    // The read is part of the same guarded transaction, after all insertion
    // triggers. Compare its exact values, never a newly observed revision.
    const result = results[results.length - 4] as any;
    const rows = Array.isArray(result) ? result : result?.results;
    const expected = JSON.parse(body) as unknown[][];
    if (!Array.isArray(rows) || rows.length !== count || rows.some((row, i) => this.columns.some((column, j) => row[column] !== expected[i][j]))) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
    this.revision += count; this.written += count;
  }
}
