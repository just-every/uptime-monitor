import { retainedSql } from './retained-values.ts';
import { changedLazy, putRow } from './expected-state.ts';
import { compressedPlan, storedStreamPage } from './stream-plan.ts';
import { expandCompressedEnvelope } from './compressed-plan.ts';
import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { ImportPlan } from '../types.ts';
import type { D1Database, D1Sql, D1PlanCodec } from './types.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { guard, sql } from './database.ts';
import { canonicalChunks, canonicalHash } from '../provenance/canonical-stream.ts';
import { jsonChunks, PACKED_JSON_BYTES, SNAPSHOT_BYTES } from './sets.ts';

export interface StoredPlan { revision?: number; body: string; chunks: { job_id: string; ordinal: number; body: string }[] }
const snapshots = new WeakMap<ImportPlan, StoredPlan>();
// Legacy chunks may approach D1's 2 MB value ceiling. Three rows remain below
// the RPC ceiling even with JSON escaping and whole-result two-byte strings.
const PLAN_READ_ROWS = 3;
export function planChunkReads(body: string): number {
  const envelope = JSON.parse(body);
  return envelope?.d1Plan === 1 ? Math.ceil((envelope.chunks + 1) / PLAN_READ_ROWS) : 0;
}
export function encodePlan(plan: ImportPlan, id: string): StoredPlan {
  const chunks: StoredPlan['chunks'] = [], hash = createHash('sha256'); let size = 0;
  for (const body of canonicalChunks(plan, SNAPSHOT_BYTES)) { chunks.push({ job_id: id, ordinal: chunks.length, body }); size += Buffer.byteLength(body); hash.update(body); }
  if (chunks.length === 1) return { body: chunks[0].body, chunks: [] };
  return { body: canonicalJson({ d1Plan: 1, chunks: chunks.length, size, sha256: hash.digest('hex') }), chunks };
}
export function retainPlan(plan: ImportPlan, id: string, codec?: D1PlanCodec): { body: string; statements: D1Sql[]; encoded: StoredPlan } {
  const encoded = codec ? storedStreamPage(compressedPlan(plan, codec), id, codec) : encodePlan(plan, id);
  const rows = encoded.chunks.map(({ ordinal, body }) => ({ ordinal, body }));
  return { encoded, body: encoded.body, statements: jsonChunks(rows, PACKED_JSON_BYTES).map(body => changedLazy(retainedSql(["INSERT INTO ik_d1_plan_chunks(job_id,ordinal,body) SELECT owner.job_id,json_extract(value,'$.ordinal'),json_extract(value,'$.body') FROM json_each(",") CROSS JOIN (SELECT "," AS job_id) owner"],[body,id],[0]),()=>JSON.parse(body).map((row:any)=>putRow('ik_d1_plan_chunks',{job_id:id,...row})))) };
}
export async function loadPlans(rows: any[], db: D1Database, codec?: D1PlanCodec, fence?:D1Sql): Promise<Map<string, ImportPlan>> {
  const result = new Map<string, ImportPlan>();
  const marked = rows.filter(r => JSON.parse(r.plan_json)?.d1Plan === 1);
  const chunks: any[] = [];
  // D1 serializes the entire result as one RPC string, not separate row values.
  // Read one job at a time; the final hash/cardinality check rejects torn reads.
  if (marked.length) {
    for (const row of marked) {
      const envelope = JSON.parse(row.plan_json);
      if (!Number.isSafeInteger(envelope.chunks) || envelope.chunks < 1) throw new Error('STORED_PLAN_CHANGED');
      for (let offset = 0; offset <= envelope.chunks; offset += PLAN_READ_ROWS) {
        const found = await db.prepare(`SELECT * FROM ik_d1_plan_chunks WHERE job_id IN (SELECT value FROM json_each(?))${fence?` AND (${fence.sql})`:''} ORDER BY job_id,ordinal LIMIT ${PLAN_READ_ROWS} OFFSET ${offset}`).bind(JSON.stringify([row.id]),...fence?.params??[]).all();
        chunks.push(...found.results);
        if (found.results.length < PLAN_READ_ROWS) break;
      }
    }
  }
  for (const row of rows) {
    const parsed = JSON.parse(row.plan_json), parts = chunks.filter(c => c.job_id === row.id);
    let plan: ImportPlan;
    if (parsed?.d1Plan === 1) {
      if (canonicalJson(parsed) !== row.plan_json || canonicalJson(Object.keys(parsed).sort()) !== '["chunks","d1Plan","sha256","size"]' || !Number.isSafeInteger(parsed.size) || parsed.size < 0 || typeof parsed.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(parsed.sha256) || !Number.isSafeInteger(parsed.chunks) || parsed.chunks < 1 || parts.length !== parsed.chunks || parts.some((c, i) => c.ordinal !== i || typeof c.body !== 'string')) throw new Error('STORED_PLAN_CHANGED');
      const body = parts.map(c => c.body).join('');
      if (Buffer.byteLength(body) !== parsed.size || sha256(body) !== parsed.sha256) throw new Error('STORED_PLAN_CHANGED');
      const payload = JSON.parse(body);
      if ([2,3].includes(payload?.d1Plan)) plan = expandCompressedEnvelope(body,payload,codec);
      else { if (canonicalHash(payload) !== parsed.sha256) throw new Error('STORED_PLAN_CHANGED'); plan = payload; }
    } else plan = [2,3].includes(parsed?.d1Plan) ? expandCompressedEnvelope(row.plan_json,parsed,codec) : parsed;
    snapshots.set(plan, { body: row.plan_json, chunks: parts, ...(row.custody_revision === undefined ? {} : {revision: row.custody_revision}) }); result.set(row.id, plan);
  }
  return result;
}
export function planBody(plan: ImportPlan): string { return snapshots.get(plan)?.body ?? canonicalJson(plan); }
export function planGuards(plan: ImportPlan, id: string): D1Sql[] {
  const snapshot = snapshots.get(plan);
  if (!snapshot?.chunks.length) {
    const predicate=retainedSql(['(SELECT COUNT(*) FROM ik_d1_plan_chunks WHERE job_id=',')=0'],[id],[]);
    predicate.retainedValues!.privacyPlan=plan;predicate.retainedValues!.privacyJobId=id;
    return [guard(predicate)];
  }
  const guards = [guard(sql('(SELECT count(*) FROM ik_d1_plan_chunks WHERE job_id=?)=?', id, snapshot.chunks.length))];
  for(const body of jsonChunks(snapshot.chunks, PACKED_JSON_BYTES)) {
    const predicate=retainedSql(["NOT EXISTS(SELECT 1 FROM json_each(", ") e WHERE NOT EXISTS(SELECT 1 FROM ik_d1_plan_chunks c WHERE c.job_id IS json_extract(e.value,'$.job_id') AND c.ordinal IS json_extract(e.value,'$.ordinal') AND c.body IS json_extract(e.value,'$.body')))"], [body], [0]);
    predicate.retainedValues!.privacyPlan=plan;
    predicate.retainedValues!.privacyJobId=id;
    guards.push(guard(predicate));
  }
  return guards;
}

export function storedPlanSnapshot(plan: ImportPlan, id: string): StoredPlan { return snapshots.get(plan) ?? encodePlan(plan, id); }
