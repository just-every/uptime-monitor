import type { InventorySnapshot } from './inventory-snapshot.ts';
import { requestContext } from './budget.ts';
import { originalStorage, originalVersions, custodyGuards } from './original-storage.ts';
import { Buffer } from 'node:buffer';
import type { ImportPlan, RetainedOriginal } from '../types.ts';
import { originalsFor } from '../storage/originals.ts';
import { sha256 } from '../provenance/identity.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { sql, guard, authorize, readAtomic } from './database.ts';
import { stored, boundary } from './jobs.ts';
import { originalCopies, retainCopies } from './original-copies.ts';
import type { StoredPlan } from './plans.ts';
import { insertJsonRows } from './sets.ts';
import { nativeStagingScope } from './native-staging-context.ts';

export function retainOriginals(plan: ImportPlan, id: string, encoded: StoredPlan): D1Sql[] {
  const { rows, chunks } = originalStorage(plan, id, 3);
  return [sql('INSERT INTO ik_original_sets(job_id,version) VALUES(?,3)', id),
    ...insertJsonRows('ik_originals', ['job_id','kind','original_key','name','sha256','size','bytes'], rows),
    ...(encoded.chunks.length ? retainCopies(originalCopies(encoded, chunks)) : insertJsonRows('ik_d1_original_chunks', ['job_id','kind','original_key','ordinal','bytes'], chunks)),
    // INSERT SELECT must not silently publish fewer rows if a source chunk
    // disappears. A failing CHECK rolls the whole creation transaction back.
    guard(sql('(SELECT COUNT(*) FROM ik_d1_original_chunks WHERE job_id=?)=?', id, chunks.length))];
}
export async function readOriginals(plan: ImportPlan, id: string, context: D1ImportContext, snapshot?: InventorySnapshot): Promise<RetainedOriginal[]> {
  const expected = originalsFor(plan, id), db = context.storage.database;
  // Validate metadata types with the existing version read, without spending an
  // additional D1 request or passing a BLOB to SQLite's JSON encoder.
  const version = snapshot ? snapshot.rows('ik_original_sets', 'job_id', [id])[0] : await db.prepare("SELECT version,EXISTS(SELECT 1 FROM ik_originals WHERE job_id=ik_original_sets.job_id AND typeof(name)<>'text') AS invalid_names FROM ik_original_sets WHERE job_id=?").bind(id).first();
  if (version?.invalid_names) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
  // JSON encoding preserves embedded NULs across SQLite text drivers, including Node 22.
  const results = snapshot ? snapshot.rows('ik_originals', 'job_id', [id]) : (await db.prepare('SELECT *,json_quote(name) AS name_json FROM ik_originals WHERE job_id=?').bind(id).all()).results;
  const chunks = snapshot ? snapshot.rows('ik_d1_original_chunks', 'job_id', [id]).sort((a, b) => a.ordinal - b.ordinal) : (await db.prepare('SELECT * FROM ik_d1_original_chunks WHERE job_id=? ORDER BY kind,original_key,ordinal').bind(id).all()).results;
  if (![1,2,3,4].includes(version?.version) || results.length !== expected.length) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
  if (version!.version === 4 && expected.some(o => o.original.status !== 'unavailable')) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
  const encoded = originalStorage(plan, id, version!.version);
  if (chunks.length !== encoded.chunks.length) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
  for (const o of expected) {
    const row = results.find(r => r.kind === o.kind && r.original_key === o.key);
    if (!row || (row.name_json === undefined ? row.name : JSON.parse(row.name_json)) !== o.name || row.sha256 !== o.sha256 || row.size !== o.size) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
    if (o.original.status === 'unavailable') { if (row.bytes !== null) throw new Error('ORIGINAL_CUSTODY_MISMATCH'); }
    else {
      const parts = chunks.filter(c => c.kind === o.kind && c.original_key === o.key);
      const expectedParts = encoded.chunks.filter(c => c.kind === o.kind && c.original_key === o.key);
      if (parts.length !== expectedParts.length || parts.some((c, i) => c.ordinal !== i || (version!.version === 3 ? c.bytes : Buffer.from(c.bytes).toString('hex').toUpperCase()) !== expectedParts[i].bytes)) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
      if (parts.length && row.bytes !== null) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
      if (!parts.length && row.bytes === null) throw new Error('ORIGINAL_CUSTODY_MISSING');
      const bytes = parts.length ? version!.version === 3 ? Buffer.from(parts.map(c => c.bytes).join(''), 'base64') : Buffer.concat(parts.map(c => Buffer.from(c.bytes))) : Buffer.from(row.bytes);
      if (bytes.length !== o.size || sha256(bytes) !== o.sha256 || bytes.toString('base64') !== o.original.base64) throw new Error('ORIGINAL_CUSTODY_MISMATCH');
    }
  }
  originalVersions.set(plan, version!.version);
  return expected;
}
export async function readD1ImportOriginals(id: string, context: D1ImportContext): Promise<RetainedOriginal[]> {
  context = requestContext(context);
  await authorize(context, 'read');
  const { job, plan } = await stored(id, context), originals = await readOriginals(plan, id, context);
  await readAtomic(context, [...boundary(job, plan), ...custodyGuards(plan, id)], nativeStagingScope(job, plan));
  return originals;
}
