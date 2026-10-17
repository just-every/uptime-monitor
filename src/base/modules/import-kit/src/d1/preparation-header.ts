import { Buffer } from 'node:buffer';
import type { UnverifiedStreamPages } from './stream-plan.ts';
import type { D1ImportContext } from './types.ts';
import { canonicalChunks, canonicalHash, canonicalSize } from '../provenance/canonical-stream.ts';
import { sha256 } from '../provenance/identity.ts';
import { guard, sql } from './database.ts';
import { remaining, D1RequestLimitError } from './budget.ts';
import { StageWriter, bindingPredicate, stageClear, stagingAtomic, STAGING_ROWS_BYTES, type StageBinding } from './staging-transport.ts';

export function preparationHeader(header: UnverifiedStreamPages['header'], context: D1ImportContext) {
  const header_hash = canonicalHash(header), header_size = canonicalSize(header);
  let header_chunks = 0, bytes = 0, largest = 0;
  for (const body of canonicalChunks(header, 64 * 1024)) {
    const size = Buffer.byteLength(JSON.stringify([header_chunks++,body,sha256(body),Buffer.byteLength(body)])) + 1;
    bytes += size; largest = Math.max(largest, size);
  }
  // Reserve the additional header writes and their guarded receipt. The shared
  // stage admission still reserves all ordinary writes and publication.
  const reserve = Math.ceil(bytes / (STAGING_ROWS_BYTES - largest - 2)) * 16 + 16;
  if (!Number.isSafeInteger(reserve) || reserve > remaining(context)) throw new D1RequestLimitError();
  return { reserve, async store(stage: StageBinding, revision: number) {
    const writer = new StageWriter(context, stage, 'headers', ['ordinal','body','body_hash','body_size']); writer.revision = revision;
    let ordinal = 0;
    for (const body of canonicalChunks(header,64 * 1024)) await writer.append([ordinal++,body,sha256(body),Buffer.byteLength(body)]);
    await writer.flush(); revision = writer.revision;
    const values = [stage.id,stage.generation,context.storage.targets.planCodec?.id ?? null,header_hash,header_size,header_chunks];
    await stagingAtomic(context,[guard(bindingPredicate(stage)),guard(stageClear(stage)),guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)',stage.id,revision)),
      sql('INSERT INTO ik_d1_preparation_receipts(stage_id,generation,codec_id,header_hash,header_size,header_chunks) VALUES(?,?,?,?,?,?)',...values),
      guard(sql('EXISTS(SELECT 1 FROM ik_d1_preparation_receipts WHERE stage_id IS ? AND generation IS ? AND codec_id IS ? AND header_hash IS ? AND header_size IS ? AND header_chunks IS ?)',...values)),
      guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=?)',stage.id,revision+1))]);
    return revision + 1;
  } };
}
