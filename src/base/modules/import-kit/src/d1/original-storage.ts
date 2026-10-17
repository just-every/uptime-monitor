import { storedPlanSnapshot } from './plans.ts';
import { originalCopies, copyGuards } from './original-copies.ts';
import { Buffer } from 'node:buffer';
import type { ImportPlan } from '../types.ts';
import { originalsFor } from '../storage/originals.ts';
import type { D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { exactRows, jsonChunks, PACKED_JSON_BYTES, SNAPSHOT_BYTES } from './sets.ts';
export const originalVersions = new WeakMap<ImportPlan, number>();
export function originalStorage(plan: ImportPlan, id: string, version: number) {
  const chunks: any[] = [];
  const rows = originalsFor(plan, id).map(o => {
    if (version === 3) {
      if (o.original.status === 'retained') {
        const base64 = o.original.base64;
        // Empty originals still have one exact chunk; every boundary is a
        // complete base64 quantum. No provider binary conversion is required.
        for (let start = 0, ordinal = 0; start < base64.length || ordinal === 0; start += SNAPSHOT_BYTES, ordinal++) chunks.push({ job_id: id, kind: o.kind, original_key: o.key, ordinal, bytes: base64.slice(start, start + SNAPSHOT_BYTES) });
      }
      return { job_id: id, kind: o.kind, original_key: o.key, name: o.name, sha256: o.sha256, size: o.size, bytes: null };
    }
    const bytes = o.original.status === 'retained' ? Buffer.from(o.original.base64, 'base64') : null;
    if (version === 2 && bytes && bytes.length > SNAPSHOT_BYTES) for (let start = 0, ordinal = 0; start < bytes.length; start += SNAPSHOT_BYTES, ordinal++) chunks.push({ job_id: id, kind: o.kind, original_key: o.key, ordinal, bytes: bytes.subarray(start, start + SNAPSHOT_BYTES).toString('hex').toUpperCase() });
    return { job_id: id, kind: o.kind, original_key: o.key, name: o.name, sha256: o.sha256, size: o.size, bytes: bytes && !(version === 2 && bytes.length > SNAPSHOT_BYTES) ? bytes.toString('hex').toUpperCase() : null };
  });
  return { rows, chunks };
}
export function custodyGuards(plan: ImportPlan, id: string): D1Sql[] {
  const version = originalVersions.get(plan) ?? 3, { rows, chunks } = originalStorage(plan, id, version);
  const guards = [guard(sql('EXISTS(SELECT 1 FROM ik_original_sets WHERE job_id=? AND version=?)', id, version))];
  if (version === 3) {
    const encoded = storedPlanSnapshot(plan, id);
    return [...guards,
      ...exactRows('ik_originals', ['job_id','kind','original_key','name','sha256','size','bytes'], rows, sql('job_id=?', id), PACKED_JSON_BYTES),
      ...(encoded.chunks.length ? [guard(sql('(SELECT COUNT(*) FROM ik_d1_original_chunks WHERE job_id=?)=?', id, chunks.length)), ...copyGuards(originalCopies(encoded, chunks))]
        : exactRows('ik_d1_original_chunks', ['job_id','kind','original_key','ordinal','bytes'], chunks, sql('job_id=?', id), PACKED_JSON_BYTES))];
  }
  for (const [table, expected, columns] of [
    ['ik_originals', rows, ['job_id','kind','original_key','name','sha256','size']],
    ['ik_d1_original_chunks', chunks, ['job_id','kind','original_key','ordinal']],
  ] as const) {
    guards.push(guard(sql(`(SELECT COUNT(*) FROM ${table} WHERE job_id=?)=?`, id, expected.length)));
    for (const row of expected) {
      // An old inline original can be near 2 MB; encode no giant hex snapshot.
      if (row.bytes !== null && row.bytes.length > 2 * SNAPSHOT_BYTES) {
        guards.push(...exactRows(table, [...columns], [row]));
        const key = columns.filter(c => c === 'job_id' || c === 'kind' || c === 'original_key' || c === 'ordinal');
        guards.push(guard(sql(`EXISTS(SELECT 1 FROM ${table} WHERE ${key.map(c => `${c}=?`).join(' AND ')} AND bytes IS ?)`, ...key.map(c => row[c]), Uint8Array.from(Buffer.from(row.bytes, 'hex')).buffer)));
      }
    }
    for (const chunk of jsonChunks(expected.filter(r => r.bytes === null || r.bytes.length <= 2 * SNAPSHOT_BYTES))) guards.push(guard(sql(`NOT EXISTS(SELECT 1 FROM json_each(?) e WHERE NOT EXISTS(SELECT 1 FROM ${table} t WHERE ${columns.map(c => `t.${c} IS json_extract(e.value,'$.${c}')`).join(' AND ')} AND CASE WHEN t.bytes IS NULL THEN NULL ELSE hex(t.bytes) END IS json_extract(e.value,'$.bytes')))`, chunk)));
  }
  return guards;
}
