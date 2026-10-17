import { retainedSql } from './retained-values.ts';
import { changed, putRow, updateRows, deleteRows } from './expected-state.ts';
import { targetGuards } from './target-guards.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import type { ImportJob } from '../types.ts';
import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import { decodeProvenance } from '../storage/provenance.ts';
import type { D1ImportContext, D1Sql, D1ProvenanceRow } from './types.ts';
import { atomic, atomicCount, guard, limits, sql } from './database.ts';
import { decode } from './jobs.ts';
import { restorePredecessor } from './history.ts';
import { attachReversal, EMPTY_REVERSAL_HASH, policy } from './undo-state.ts';
import { captureJobLayout, jobLayoutGuards } from './stream-layout.ts';
import { needsStreamOrigin } from './stream-origin.ts';
import { streamBoundary } from './stream-boundary.ts';

type Action = 'undo' | 'rollback';
const reversing = (action: Action) => action === 'undo' ? 'undoing' : 'rolling_back';
const terminal = (action: Action) => action === 'undo' ? 'undone' : 'rolled_back';

/** Appended after every other write of a verified strict reversal batch, so the
 * captured revision includes this batch's own custody changes. Only the complete
 * inventory path classifies entry reuse; a fenced batch carries it forward. */
export function recordReversalFence(id: string, action: Action, erasedRecords: number, verified: boolean): D1Sql[] {
  const write = verified
    ? sql('INSERT OR REPLACE INTO ik_d1_reversal_fences(job_id,action,fence,state_revision,erased_records,reused) SELECT j.id,?,j.fence,s.revision,?,EXISTS(SELECT 1 FROM ik_d1_entries e WHERE e.job_id=j.id AND e.owner_job_id<>j.id) FROM ik_jobs j JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE j.id=?', action, erasedRecords, id)
    : sql('UPDATE ik_d1_reversal_fences SET fence=(SELECT fence FROM ik_jobs WHERE id=?),state_revision=(SELECT revision FROM ik_d1_custody_states WHERE job_id=?) WHERE job_id=? AND action=?', id, id, id, action);
  return [write, guard(sql('EXISTS(SELECT 1 FROM ik_d1_reversal_fences f JOIN ik_jobs j ON j.id=f.job_id JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE f.job_id=? AND f.action=? AND f.fence=j.fence AND f.state_revision=s.revision AND f.erased_records=?)', id, action, erasedRecords))];
}

/** Releases claims no longer backing an unreversed effect: the whole job's on
 * a verified or final batch (sources with zero effects release too), and only
 * the batch's own sources on any other fenced batch. Reused sources belong to
 * their owner. */
export function reversalProgress(job: ImportJob, action: Action, done: boolean, sources?: string[]): D1Sql[] {
  const state = done ? terminal(action) : reversing(action), only = done ? undefined : sources;
  const released = only ? sql("DELETE FROM ik_sources WHERE job_id=? AND identity IN (SELECT value FROM json_each(?)) AND NOT EXISTS(SELECT 1 FROM ik_provenance WHERE ik_provenance.source_identity=ik_sources.identity AND reversed=0) AND NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE ik_d1_history.source_identity=ik_sources.identity)", job.id, JSON.stringify(only))
    : sql('DELETE FROM ik_sources WHERE job_id=? AND NOT EXISTS(SELECT 1 FROM ik_provenance WHERE ik_provenance.source_identity=ik_sources.identity AND reversed=0) AND NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE ik_d1_history.source_identity=ik_sources.identity)', job.id);
  return [changed(released,deleteRows('ik_sources',(row,expected)=>row.job_id===job.id&&(!only||only.includes(row.identity))&&![...expected.rows('ik_provenance').values()].some(p=>p.source_identity===row.identity&&!p.reversed)&&![...expected.rows('ik_d1_history').values()].some(p=>p.source_identity===row.identity))), changed(sql('INSERT INTO ik_batches(job_id,action,position,end_position,fence) VALUES(?,?,?,?,?)', job.id, action, job.fence, job.fence + 1, job.fence + 1),putRow('ik_batches',{job_id:job.id,action,position:job.fence,end_position:job.fence+1,fence:job.fence+1})), changed(sql('UPDATE ik_jobs SET state=?,fence=fence+1,error=NULL WHERE id=?', state, job.id),updateRows('ik_jobs',row=>row.id===job.id,row=>({...row,state,fence:job.fence+1,error:null})))];
}

async function readFence(id: string, context: D1ImportContext): Promise<any> {
  return context.storage.database.prepare(`SELECT j.*,s.revision AS state_revision,f.action AS fence_action,f.fence AS fence_fence,f.state_revision AS fence_revision,f.erased_records AS fence_erased,f.reused AS fence_reused,EXISTS(SELECT 1 FROM ik_d1_indexed_jobs i WHERE i.job_id=j.id AND i.fingerprint=j.fingerprint) AS indexed,${needsStreamOrigin('j.id')} AS stream_origin_required,(SELECT stream_id FROM ik_d1_stream_origins WHERE job_id=j.id) AS origin_stream_id,(SELECT stream_id FROM ik_d1_stream_pages WHERE job_id=j.id) AS page_stream_id FROM ik_jobs j JOIN ik_d1_custody_states s ON s.job_id=j.id LEFT JOIN ik_d1_reversal_fences f ON f.job_id=j.id WHERE j.id=?`).bind(id).first();
}
function current(row: any, action: Action, context: D1ImportContext): boolean {
  return !!row && row.fence_action === action && row.fence_fence === row.fence && row.fence_revision === row.state_revision && row.state === reversing(action) && row.error === null && row.indexed === 1 && row.fence_reused === 0
    && row.reversal_policy === 'strict' && (context.reversalPolicy === undefined || policy(context) === 'strict')
    && row.destination === context.destination && row.namespace === context.namespace && row.mapping_version === context.mappingVersion && row.principal_id === context.authorization.principalId
    && (context.reviewedFingerprint === undefined || context.reviewedFingerprint === row.fingerprint);
}
function fencedJob(row: any): ImportJob {
  const job = decode(row); job.erasedRecords = row.fence_erased;
  attachReversal(job, { policy: 'strict', binding: null, cursor: 0, hash: EMPTY_REVERSAL_HASH, rows: [], dispositions: [] });
  return job;
}

/** Continue a strict, unbounded reversal whose custody is unchanged since its
 * last verified batch. Each batch reads only its own provenance rows and their
 * native targets; the custody revision replaces re-reading the whole job.
 * Returns undefined when the fence is not current on entry: the caller then
 * verifies the complete inventory as before. A fence that stops being current
 * after this call's own batches ends the request with that published progress. */
export async function fencedReversal(id: string, action: Action, context: D1ImportContext, batches: number, published?: ImportJob): Promise<ImportJob | undefined> {
  const { batchSize } = limits(context), { database: db, targets } = context.storage;
  try {
    for (let count = 0; count < batches; count++) {
      const row = await readFence(id, context);
      if (!current(row, action, context)) return count ? published : undefined;
      const job = fencedJob(row);
      await captureJobLayout(job, row, context, action);
      const statements: D1Sql[] = [...jobLayoutGuards(job), ...streamBoundary(id, action),
        guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE job_id=?)', id)),
        guard(sql('EXISTS(SELECT 1 FROM ik_jobs j JOIN ik_d1_custody_states s ON s.job_id=j.id JOIN ik_d1_reversal_fences f ON f.job_id=j.id WHERE j.id=? AND j.state=? AND j.fence=? AND j.fingerprint=? AND j.error IS NULL AND s.revision=? AND f.action=? AND f.fence=j.fence AND f.state_revision=s.revision)', id, job.state, job.fence, job.fingerprint, row.state_revision, action))];
      const { results: rows } = await db.prepare('SELECT * FROM ik_provenance WHERE job_id=? AND reversed=0 ORDER BY ordinal DESC,effect_ordinal DESC LIMIT ?').bind(id, batchSize + 1).all();
      const batch = rows.slice(0, batchSize), predecessors = new Map<string, any>();
      for (const prior of (await db.prepare('SELECT * FROM ik_d1_history WHERE successor_id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(batch.map(raw => raw.id))).all()).results) predecessors.set(prior.successor_id, prior);
      // The request that publishes the first `count` rows, with its one return read.
      const publication = (statements: D1Sql[], count: number) => [...statements, ...reversalProgress(job, action, count === rows.length, [...new Set(batch.slice(0, count).map(raw => raw.source_identity))]), ...recordReversalFence(id, action, job.erasedRecords!, false)];
      let processed = 0, planned = atomicCount(context, action, publication(statements, 0), 1);
      for (const raw of batch) {
        // Leave room for this row's reads; the exact request is counted once built.
        if (remaining(context) - planned < 70) { if (!processed) { if (published) return published; throw new D1RequestLimitError(); } break; }
        // The verified inventory covered this exact row; its revision is unchanged.
        const p = decodeProvenance(raw) as D1ProvenanceRow;
        if (p.reversed || p.jobId !== id) throw new Error('PROVENANCE_STORAGE_MISMATCH');
        const target = await targets.read(db, context.destination, p.targetKind, p.targetId);
        if (!target || target.revision !== p.appliedRevision || sha256(canonicalJson(target.value)) !== p.valueHash || attachmentHash(target.attachment) !== p.attachmentHash) throw new Error('NATIVE_EDIT_CONFLICT');
        const candidate = [...statements], restoration: D1Sql[] = [];
        await restorePredecessor(p, context, candidate, restoration, predecessors.get(p.id) ?? null);
        candidate.push(guard(retainedSql(['EXISTS(SELECT 1 FROM ik_provenance WHERE id=', ' AND record_json=', ' AND reversed=0)'], [raw.id, raw.record_json], [1])), ...targetGuards(targets, target, context.destination, p.targetKind, p.targetId), ...(p.before ? targets.write(p.before) : targets.remove(context.destination, p.targetKind, p.targetId)), ...targetGuards(targets, p.before, context.destination, p.targetKind, p.targetId), changed(retainedSql(['UPDATE ik_provenance SET reversed=1,record_json=', ' WHERE id=', ''], [canonicalJson({ ...p, reversed: true }), raw.id], [0]),updateRows('ik_provenance',row=>row.id===raw.id,row=>({...row,reversed:1,record_json:canonicalJson({...p,reversed:true})}))), ...restoration);
        const spent = atomicCount(context, action, publication(candidate, processed + 1), 1);
        if (spent > remaining(context)) { if (processed) break; if (published) return published; throw new D1RequestLimitError(); }
        statements.splice(0, statements.length, ...candidate); processed++; planned = spent;
      }
      const done = processed === rows.length;
      await atomic(context, action, publication(statements, processed), 1);
      published = { ...job, state: done ? terminal(action) : reversing(action), fence: job.fence + 1, error: null };
      attachReversal(published, { policy: 'strict', binding: null, cursor: 0, hash: EMPTY_REVERSAL_HASH, rows: [], dispositions: [] });
      if (done || remaining(context) < 200) return published;
    }
  } catch (error) {
    if (error instanceof D1RequestLimitError && published) return published;
    throw error;
  }
  return published;
}
