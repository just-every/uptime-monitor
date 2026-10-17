import type { D1Sql } from './types.ts';
import type { StoredPlan } from './plans.ts';
import { guard, sql } from './database.ts';
import { jsonChunks, PACKED_JSON_BYTES } from './sets.ts';

/** A copy descriptor contains positions, never a second copy of the bytes. */
interface Copy {
  job_id: string; kind: string; original_key: string; ordinal: number;
  first: number; start: number; length: number; second: number; tail: number;
}
export function originalCopies(encoded: StoredPlan, chunks: any[]): Copy[] {
  const body = encoded.chunks.map(c => c.body).join('');
  const starts: number[] = []; let size = 0;
  for (const chunk of encoded.chunks) { starts.push(size); size += chunk.body.length; }
  const grouped = new Map<string, any[]>();
  for (const chunk of chunks) {
    const key = JSON.stringify([chunk.kind, chunk.original_key]);
    const group = grouped.get(key) ?? []; group.push(chunk); grouped.set(key, group);
  }
  const copies: Copy[] = [];
  for (const group of grouped.values()) {
    // Valid canonical base64 needs no escaping in canonical JSON. Any exact
    // occurrence is equivalent; no source identity is inferred from position.
    const base64 = group.map(c => c.bytes).join('');
    const found = body.indexOf(`"${base64}"`);
    if (found < 0) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
    let position = found + 1, first = 0;
    for (const chunk of group) {
      while (first + 1 < starts.length && starts[first + 1] <= position) first++;
      const local = position - starts[first], text = encoded.chunks[first].body;
      const length = Math.min(chunk.bytes.length, text.length - local), tail = chunk.bytes.length - length;
      // Plan chunks use UTF-8 bounds. SQLite substr counts Unicode code points,
      // not JavaScript UTF-16 units. The copied base64 itself is ASCII.
      const start = Array.from(text.slice(0, local)).length + 1;
      if (tail && (!encoded.chunks[first + 1] || tail > encoded.chunks[first + 1].body.length)) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
      copies.push({ job_id: chunk.job_id, kind: chunk.kind, original_key: chunk.original_key, ordinal: chunk.ordinal, first, start, length, second: tail ? first + 1 : first, tail });
      position += chunk.bytes.length;
    }
  }
  return copies;
}
const value = (key: string) => `json_extract(e.value,'$.${key}')`;
const joins = `JOIN ik_d1_plan_chunks a ON a.job_id=${value('job_id')} AND a.ordinal=${value('first')} JOIN ik_d1_plan_chunks b ON b.job_id=${value('job_id')} AND b.ordinal=${value('second')}`;
const bytes = `substr(a.body,${value('start')},${value('length')}) || substr(b.body,1,${value('tail')})`;
export function retainCopies(copies: Copy[]): D1Sql[] {
  return jsonChunks(copies, PACKED_JSON_BYTES).map(chunk => sql(`INSERT INTO ik_d1_original_chunks(job_id,kind,original_key,ordinal,bytes) SELECT ${['job_id','kind','original_key','ordinal'].map(value).join(',')},${bytes} FROM json_each(?) e ${joins}`, chunk));
}
/** The caller also guards the exact plan chunks in this same transaction.
 * Equality with those immutable chunks proves original content without binding
 * another complete base64 copy into the D1 request. Cardinality is separate. */
export function copyGuards(copies: Copy[]): D1Sql[] {
  return jsonChunks(copies, PACKED_JSON_BYTES).map(chunk => guard(sql(`NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_original_chunks t ${joins} WHERE ${['job_id','kind','original_key','ordinal'].map(c => `t.${c} IS ${value(c)}`).join(' AND ')} AND t.bytes IS (${bytes})))`, chunk)));
}
