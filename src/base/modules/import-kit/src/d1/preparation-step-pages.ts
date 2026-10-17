import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { ImportPlan } from '../types.ts';
import type { D1ImportContext } from './types.ts';
import type { UnverifiedStreamPages } from './stream-plan.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { sha256 } from '../provenance/identity.ts';
import { storedPagePlan } from './preparation-reader.ts';
import type { StepStage } from './preparation-step-control.ts';

type Header = UnverifiedStreamPages['header'];

/** A step generation's stored header, checked against its preparation receipt. */
export async function stageHeader(context: D1ImportContext, stage: string): Promise<Header> {
  const db = context.storage.database;
  const receipt = await db.prepare('SELECT header_hash,header_size,header_chunks FROM ik_d1_preparation_receipts WHERE stage_id=?').bind(stage).first<any>();
  if (!receipt) throw new Error('IMPORT_PREPARATION_INTERRUPTED');
  const { results } = await db.prepare('SELECT ordinal,body,body_hash,body_size FROM ik_d1_staging_headers WHERE stage_id=? ORDER BY ordinal').bind(stage).all<any>();
  const hash = createHash('sha256'); let bytes = 0;
  results.forEach((row, ordinal) => {
    if (row.ordinal !== ordinal || Buffer.byteLength(row.body) !== row.body_size || sha256(row.body) !== row.body_hash) throw new Error('IMPORT_PREPARATION_CHANGED');
    hash.update(row.body); bytes += row.body_size;
  });
  if (results.length !== receipt.header_chunks || bytes !== receipt.header_size || hash.digest('hex') !== receipt.header_hash) throw new Error('IMPORT_PREPARATION_CHANGED');
  const header = JSON.parse(results.map(row => row.body).join(''));
  if (canonicalHash(header) !== receipt.header_hash) throw new Error('IMPORT_PREPARATION_CHANGED');
  return header;
}

/** The header one collecting pass over the whole source would have built: the
 * first slice's, with every later slice's counts added and its own diagnostics
 * appended (each slice repeats the shared artifact diagnostics first). */
export function mergedHeader(headers: Header[]): Header {
  const [first, ...rest] = headers, merged: Header = structuredClone(first);
  merged.counts.byKind = Object.assign(Object.create(null), merged.counts.byKind);
  const shared = first.source.artifacts.filter(artifact => artifact.original?.status === 'unavailable').length;
  for (const header of rest) {
    for (const key of ['input', 'unique', 'duplicates', 'mapped', 'unmapped', 'rejected', 'effects'] as const) merged.counts[key] += header.counts[key];
    for (const [kind, count] of Object.entries(header.counts.byKind)) merged.counts.byKind[kind] = (merged.counts.byKind[kind] ?? 0) + count;
    merged.diagnostics.push(...header.diagnostics.slice(shared));
    const { counts: _a, diagnostics: _b, ...same } = header, { counts: _c, diagnostics: _d, ...expected } = first;
    if (canonicalHash(same) !== canonicalHash(expected)) throw new Error('PLAN_BINDING_MISMATCH');
  }
  return merged;
}

/** Which step generation holds global page `page`, and its ordinal there. */
export function locate(stages: StepStage[], page: number): { stage: StepStage; ordinal: number; first: number } {
  let first = 0;
  for (const stage of stages) { if (page < first + stage.pages) return { stage, ordinal: page - first, first }; first += stage.pages; }
  throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
}

/** Pages [from, to) of one step generation, each checked against its manifest. */
export async function stagePages(context: D1ImportContext, stage: string, from: number, to: number): Promise<{ page: any; plan: ImportPlan }[]> {
  const db = context.storage.database;
  const pages = (await db.prepare('SELECT * FROM ik_d1_staging_pages WHERE stage_id=? AND ordinal>=? AND ordinal<? ORDER BY ordinal').bind(stage, from, to).all<any>()).results;
  const chunks = (await db.prepare('SELECT page_ordinal,ordinal,body,body_hash,body_size FROM ik_d1_staging_chunks WHERE stage_id=? AND page_ordinal>=? AND page_ordinal<? ORDER BY page_ordinal,ordinal').bind(stage, from, to).all<any>()).results;
  if (pages.length !== to - from || pages.some((page, index) => page.ordinal !== from + index)) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  let next = 0;
  const result = pages.map(page => {
    const parts: string[] = []; let bytes = 0;
    for (let ordinal = 0; ordinal < page.chunks; ordinal++) {
      const chunk = chunks[next++];
      if (!chunk || chunk.page_ordinal !== page.ordinal || chunk.ordinal !== ordinal || typeof chunk.body !== 'string' || Buffer.byteLength(chunk.body) !== chunk.body_size || sha256(chunk.body) !== chunk.body_hash) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
      parts.push(chunk.body); bytes += chunk.body_size;
    }
    return { page, plan: storedPagePlan(page, parts, bytes, context.storage.targets.planCodec) };
  });
  if (next !== chunks.length) throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  return result;
}

/** The privacy index rows of pages [from, to), in the order they were staged. */
export async function stageIndex(context: D1ImportContext, stage: string, from: number, to: number): Promise<any[]> {
  return (await context.storage.database.prepare('SELECT page_ordinal,ordinal,source_identity,target_identity FROM ik_d1_staging_index WHERE stage_id=? AND page_ordinal>=? AND page_ordinal<? ORDER BY rowid').bind(stage, from, to).all<any>()).results;
}
