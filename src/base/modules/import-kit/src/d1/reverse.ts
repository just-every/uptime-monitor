import { retainedSql } from './retained-values.ts';
import { changed, updateRows } from './expected-state.ts';
import { cancelNativeJobStaging } from './native-staging-lifecycle.ts';
import { nativeStagingScope, readStagedTarget, expectedStagedTarget, type NativeStagingScope } from './native-staging-context.ts';
import { targetGuards } from './target-guards.ts';
import { D1RequestLimitError, requestContext, remaining } from './budget.ts';
import type { ImportJob } from '../types.ts';
import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { atomic, atomicCount, authorize, guard, limits, sql, fitsBatch } from './database.ts';
import { stored, boundary, storedReads } from './jobs.ts';
import { custodyGuards, validatedProvenance, inventoryGuards } from './integrity.ts';
import { noSuccessors, restorePredecessor } from './history.ts';
import { readOriginals } from './originals.ts';
import { reversalState } from './undo-state.ts';
import { preserveReverse } from './preserved-reverse.ts';
import { fencedReversal, recordReversalFence, reversalProgress } from './reversal-fence.ts';

export async function reverseD1Import(id: string, action: 'undo' | 'rollback', context: D1ImportContext): Promise<ImportJob> {
  context = requestContext(context);
  const { batchSize, maxBatches } = limits(context), state = action === 'undo' ? 'undoing' : 'rolling_back', final = action === 'undo' ? 'undone' : 'rolled_back';
  await authorize(context, action);
  // Unbounded strict reversal fences each later batch on the custody revision
  // its last verified batch recorded, instead of re-reading the whole job.
  const unbounded = context.maxBatchBytes === undefined;
  const fenced = unbounded ? await fencedReversal(id, action, context, maxBatches) : undefined;
  if (fenced) return fenced;
  // Persist reversal intent before reading targets: a conflicting native edit may
  // block this invocation, but a later request must never resume forward apply.
  let current = await stored(id, context, action);
  if(context.maxBatchBytes!==undefined)await cancelNativeJobStaging(context,action,id,current.job.fence);
  if ((action === 'undo' && current.job.state === 'rolled_back') || (action === 'rollback' && current.job.state === 'undone')) throw new Error('REVERSAL_MODE_CONFLICT');
  if (['undone', 'rolled_back'].includes(current.job.state)) {
    await readOriginals(current.plan, id, context);
    await atomic(context, action, [...boundary(current.job, current.plan, action), ...custodyGuards(current.plan, id), ...await inventoryGuards(current.job, current.plan, context)], 0, nativeStagingScope(current.job,current.plan));
    return current.job;
  }
  if (current.job.state === (action === 'undo' ? 'rolling_back' : 'undoing')) throw new Error('REVERSAL_MODE_CONFLICT');
  if (action === 'undo' && !['completed', 'undoing'].includes(current.job.state)) throw new Error('UNDO_REQUIRES_COMPLETED_IMPORT');
  await readOriginals(current.plan, id, context);
  if (reversalState(current.job).policy === 'preserve-conflicts') return preserveReverse(current, context, action);
  if (current.job.state !== state) {
    const intentScope=nativeStagingScope(current.job,current.plan);
    intentScope.postGuards.push(guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND state=? AND fence=?)',id,state,current.job.fence+1)));
    await atomic(context, action, [...boundary(current.job, current.plan, action), await noSuccessors(id, context), ...custodyGuards(current.plan, id), ...await inventoryGuards(current.job, current.plan, context), changed(sql('UPDATE ik_jobs SET state=?,fence=fence+1 WHERE id=?', state, id),updateRows('ik_jobs',row=>row.id===id,row=>({...row,state,fence:current.job.fence+1}))), ...unbounded ? recordReversalFence(id, action, current.job.erasedRecords!, true) : []], storedReads(current.job), intentScope);
    const next = unbounded ? await fencedReversal(id, action, context, maxBatches) : undefined;
    if (next) return next;
    current = await stored(id, context, action);
  }
  let published: ImportJob | undefined;
  try {
    for (let count = 0; count < maxBatches; count++) {
      const { job, plan } = current, { database: db, targets } = context.storage;
      if (job.state === final) return job;
      if (job.state !== state) throw new Error('REVERSAL_MODE_CONFLICT');
      await readOriginals(plan, id, context);
      const { results: rows } = await db.prepare('SELECT * FROM ik_provenance WHERE job_id=? AND reversed=0 ORDER BY ordinal DESC,effect_ordinal DESC LIMIT ?').bind(id, batchSize).all();
      const statements: D1Sql[] = [...boundary(job, plan, action), await noSuccessors(id, context), ...custodyGuards(plan, id), ...await inventoryGuards(job, plan, context)];
      const remainingRow = await db.prepare('SELECT COUNT(*) AS n FROM ik_provenance WHERE job_id=? AND reversed=0').bind(id).first();
      const returnReads = storedReads(job), publication = (statements: D1Sql[], scope: NativeStagingScope, done: boolean) => ({
        statements: [...statements, ...reversalProgress(job, action, done), ...unbounded ? recordReversalFence(id, action, job.erasedRecords!, true) : []],
        scope: { ...scope, postGuards: [...scope.postGuards, guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND state=? AND fence=? AND error IS NULL)', id, done ? final : state, job.fence + 1))] },
      }), cost = (request: ReturnType<typeof publication>) => atomicCount(context, action, request.statements, returnReads, request.scope);
      let processed = 0, scope = nativeStagingScope(job, plan), planned = cost(publication(statements, scope, remainingRow!.n === 0));
      for (const raw of rows) {
        // Leave room for this row's reads; the exact request is counted once built.
        if (remaining(context) - planned < 70) { if (!processed) { if (count > 0) return job; throw new D1RequestLimitError(); } break; }
        const candidate = [...statements], nextScope = { ...scope, nativeGuards:[...scope.nativeGuards], postGuards:[...scope.postGuards], targets:[...scope.targets] };
        const p = await validatedProvenance(raw, context, candidate);
        const row = await readStagedTarget(context, nextScope, p.targetKind, p.targetId);
        if (!row || row.revision !== p.appliedRevision || sha256(canonicalJson(row.value)) !== p.valueHash || attachmentHash(row.attachment) !== p.attachmentHash) throw new Error('NATIVE_EDIT_CONFLICT');
        const restoration: D1Sql[] = [], post = targetGuards(targets,p.before,context.destination,p.targetKind,p.targetId);
        await restorePredecessor(p, context, candidate, restoration);
        candidate.push(guard(retainedSql(['EXISTS(SELECT 1 FROM ik_provenance WHERE id=', ' AND record_json=', ' AND reversed=0)'], [raw.id, raw.record_json], [1])), ...targetGuards(targets,row, context.destination, p.targetKind, p.targetId), ...(p.before ? targets.write(p.before) : targets.remove(context.destination, p.targetKind, p.targetId)), ...post, changed(retainedSql(['UPDATE ik_provenance SET reversed=1,record_json=', ' WHERE id=', ''], [canonicalJson({ ...p, reversed: true }), raw.id], [0]),updateRows('ik_provenance',row=>row.id===raw.id,row=>({...row,reversed:1,record_json:canonicalJson({...p,reversed:true})}))), ...restoration);
        nextScope.postGuards.push(...post);
        const next = publication(candidate, nextScope, remainingRow!.n === processed + 1), spent = cost(next);
        if (spent > remaining(context)) { if (processed) break; if (count > 0) return job; throw new D1RequestLimitError(); }
        const fits = fitsBatch(context, action, next.statements, next.scope);
        if (!fits && processed) break;
        statements.splice(0, statements.length, ...candidate);
        expectedStagedTarget(nextScope,p.targetKind,p.targetId,!!p.before);
        scope = nextScope; processed++; planned = spent;
        if (!fits) break;
      }
      const done = remainingRow!.n === processed, request = publication(statements, scope, done);
      await atomic(context, action, request.statements, returnReads, request.scope);
      if (unbounded && !done) {
        const next = await fencedReversal(id, action, context, maxBatches - count - 1, { ...job, state, fence: job.fence + 1, error: null });
        if (next) return next;
      }
      current = await stored(id, context, action); published = current.job;
      if (done || remaining(context) < 200) return published;
    }
  } catch (error) {
    // Only actual reversal progress qualifies: intent alone still fails when
    // no native batch fits, while preventing any later forward apply.
    if (error instanceof D1RequestLimitError && published) return published;
    throw error;
  }
  return current.job;
}
export const undoD1Import = (id: string, context: D1ImportContext) => reverseD1Import(id, 'undo', context);
export const rollbackD1Import = (id: string, context: D1ImportContext) => reverseD1Import(id, 'rollback', context);
