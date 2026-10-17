import { retainedSql } from './retained-values.ts';
import { nativeStagingScope, readStagedTarget, expectedStagedTarget, type NativeStagingScope } from './native-staging-context.ts';
import { targetGuards } from './target-guards.ts';
import { D1RequestLimitError, requestContext, remaining } from './budget.ts';
import type { ImportJob, PlanEntry, ProvenanceRow, TargetRow } from '../types.ts';
import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import { validateEffect } from '../plan/index.ts';
import { effectIdentity } from '../storage/provenance.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { atomic, atomicCount, authorize, guard, limits, sql, fitsBatch } from './database.ts';
import { stored, boundary, storedReads } from './jobs.ts';
import { custodyGuards, validatedProvenance, inventoryGuards, reusedSourceGuards } from './integrity.ts';
import { supersedeTarget } from './history.ts';
import { readOriginals } from './originals.ts';
import { changed, putRow, updateRows } from './expected-state.ts';

export async function applyD1ImportBatches(jobOrId: ImportJob | string, context: D1ImportContext): Promise<ImportJob> {
  context = requestContext(context);
  const id = typeof jobOrId === 'string' ? jobOrId : jobOrId.id, { batchSize, maxBatches } = limits(context);
  await authorize(context, 'apply');
  let current = await stored(id, context, 'apply'), published: ImportJob | undefined;
  try {
    for (let count = 0; count < maxBatches; count++) {
      const { job, plan } = current;
      if (typeof jobOrId !== 'string' && jobOrId.fingerprint !== job.fingerprint) throw new Error('JOB_FINGERPRINT_MISMATCH');
      await readOriginals(plan, id, context);
      if (job.state === 'completed') { await atomic(context, 'apply', [...boundary(job, plan, 'apply'), ...custodyGuards(plan, id), ...await inventoryGuards(job, plan, context)], 0, nativeStagingScope(job, plan)); return job; }
      if (!['queued', 'applying', 'failed'].includes(job.state)) throw new Error('IMPORT_ALREADY_REVERSING');
      const statements: D1Sql[] = [...boundary(job, plan, 'apply'), ...custodyGuards(plan, id), ...await inventoryGuards(job, plan, context)], entries: PlanEntry[] = [], returnReads = storedReads(job);
      let effects = 0, scope = nativeStagingScope(job, plan), planned = cost(context, publication(job, 0, statements, scope), returnReads);
      for (const entry of plan.entries.slice(job.checkpoint, job.checkpoint + batchSize)) {
        if (entries.length && effects + entry.effects.length > 1000) break;
        if (entry.effects.length > 1000) throw new Error('SOURCE_EFFECT_LIMIT');
        // Stop before an entry's own reads could exhaust the request. The exact
        // request, as atomic will send it, is counted again once the entry is built.
        const reserve = 40 + entry.effects.length * 16;
        if (remaining(context) - planned < reserve) { if (!entries.length) { if (count > 0) return job; throw new D1RequestLimitError(); } break; }
        const candidate = [...statements], nextScope = { ...scope, nativeGuards:[...scope.nativeGuards], postGuards:[...scope.postGuards], targets:[...scope.targets] };
        await publishEntry(id, entry, job.checkpoint + entries.length, context, candidate, nextScope);
        const next = publication(job, entries.length + 1, candidate, nextScope), spent = cost(context, next, returnReads);
        if (spent > remaining(context)) { if (entries.length) break; if (count > 0) return job; throw new D1RequestLimitError(); }
        const fits = fitsBatch(context, 'apply', next.statements, next.scope);
        if (!fits && entries.length) break;
        statements.splice(0, statements.length, ...candidate);
        scope = nextScope; entries.push(entry); effects += entry.effects.length; planned = spent;
        if (!fits) break;
      }
      const request = publication(job, entries.length, statements, scope), done = job.checkpoint + entries.length === job.total;
      await atomic(context, 'apply', request.statements, returnReads, request.scope);
      current = await stored(id, context, 'apply'); published = current.job;
      if (done || remaining(context) < 200) return published;
    }
  } catch (error) {
    // Later preflight has no native effects. Its measured budget exhaustion
    // returns the last checked publication; integrity/authority errors still fail.
    if (error instanceof D1RequestLimitError && published) return published;
    throw error;
  }
  return current.job;
}
async function publishEntry(id: string, entry: PlanEntry, ordinal: number, context: D1ImportContext, statements: D1Sql[], scope: NativeStagingScope): Promise<void> {
  const { database: db, targets } = context.storage, effectsHash = sha256(canonicalJson(entry.effects));
  const prior = await db.prepare('SELECT * FROM ik_sources WHERE identity=?').bind(entry.identity).first();
  if (prior) {
    if (prior.source_hash !== entry.source.payloadHash || prior.mapping_version !== context.mappingVersion || prior.effects_hash !== effectsHash) throw new Error('SOURCE_ID_CONTENT_CONFLICT');
    await reusedSourceGuards(entry, prior.job_id, context, statements);
    statements.push(guard(sql('EXISTS(SELECT 1 FROM ik_sources WHERE identity=? AND source_hash=? AND mapping_version=? AND effects_hash=? AND job_id=?)', entry.identity, prior.source_hash, prior.mapping_version, prior.effects_hash, prior.job_id)));
    for (const effect of entry.effects) {
      const raw = await db.prepare('SELECT * FROM ik_provenance WHERE source_identity=? AND destination=? AND target_kind=? AND target_id=? AND reversed=0').bind(entry.identity, context.destination, effect.targetKind, effect.targetId).first();
      if (!raw) throw new Error('PROVENANCE_MISSING');
      const p = await validatedProvenance(raw, context, statements);
      const current = await readStagedTarget(context, scope, effect.targetKind, effect.targetId);
      if (!current || current.revision !== p.appliedRevision || sha256(canonicalJson(current.value)) !== p.valueHash || attachmentHash(current.attachment) !== p.attachmentHash) throw new Error('NATIVE_EDIT_CONFLICT');
      const post = targetGuards(targets,current,context.destination,effect.targetKind,effect.targetId);
      scope.postGuards.push(...post);
      statements.push(guard(retainedSql(['EXISTS(SELECT 1 FROM ik_provenance WHERE id=', ' AND record_json=', ' AND reversed=0)'], [raw.id, raw.record_json], [1])), ...post);
    }
    statements.push(entryWrite(id,ordinal,entry.identity,prior.job_id,effectsHash));
    return;
  }
  statements.push(guard(sql('NOT EXISTS(SELECT 1 FROM ik_sources WHERE identity=?)', entry.identity)));
  for (const [index, effect] of entry.effects.entries()) {
    validateEffect(effect);
    const before = await readStagedTarget(context, scope, effect.targetKind, effect.targetId);
    if (before ? effect.expectedRevision !== before.revision : effect.expectedRevision !== undefined) throw new Error('TARGET_REVISION_CONFLICT');
    const appliedRevision = sha256(canonicalJson([id, entry.identity, index, 'applied']));
    const row: TargetRow = { destination: context.destination, targetKind: effect.targetKind, targetId: effect.targetId, value: effect.value, revision: appliedRevision, ...(effect.attachment ? { attachment: effect.attachment } : {}) };
    expectedStagedTarget(scope,effect.targetKind,effect.targetId,true);
    const post = targetGuards(targets, row, context.destination, effect.targetKind, effect.targetId);
    scope.postGuards.push(...post);
    const p: ProvenanceRow = { id: effectIdentity(id, entry.identity, index), jobId: id, sourceIdentity: entry.identity, sourceKind: entry.source.kind, sourceId: entry.source.sourceId, sourceHash: entry.source.payloadHash, artifactHash: entry.source.artifactHash, location: entry.source.location, destination: context.destination, targetKind: effect.targetKind, targetId: effect.targetId, valueHash: sha256(canonicalJson(effect.value)), attachmentHash: attachmentHash(effect.attachment), appliedRevision, before: before ?? null, reversed: false };
    await supersedeTarget(p, before, context, statements);
    const record_json=canonicalJson(p);
    statements.push(...targetGuards(targets,before ?? null, context.destination, effect.targetKind, effect.targetId), ...targets.write(row), ...post, changed(retainedSql(['INSERT INTO ik_provenance(id,job_id,ordinal,effect_ordinal,source_identity,destination,target_kind,target_id,record_json) VALUES(', ',', ',', ',', ',', ',', ',', ',', ',', ')'], [p.id, id, ordinal, index, entry.identity, context.destination, effect.targetKind, effect.targetId, record_json], [8]),putRow('ik_provenance',{id:p.id,job_id:id,ordinal,effect_ordinal:index,source_identity:entry.identity,destination:context.destination,target_kind:effect.targetKind,target_id:effect.targetId,reversed:0,record_json})));
  }
  statements.push(entryWrite(id,ordinal,entry.identity,id,effectsHash));
  statements.push(changed(sql('INSERT INTO ik_sources(identity,source_hash,mapping_version,effects_hash,job_id) VALUES(?,?,?,?,?)', entry.identity, entry.source.payloadHash, context.mappingVersion, effectsHash, id),putRow('ik_sources',{identity:entry.identity,source_hash:entry.source.payloadHash,mapping_version:context.mappingVersion,effects_hash:effectsHash,job_id:id})));
}

function entryWrite(job_id:string,ordinal:number,source_identity:string,owner_job_id:string,effects_hash:string):D1Sql{
  return changed(sql('INSERT INTO ik_d1_entries(job_id,ordinal,source_identity,owner_job_id,effects_hash) VALUES(?,?,?,?,?)',job_id,ordinal,source_identity,owner_job_id,effects_hash),putRow('ik_d1_entries',{job_id,ordinal,source_identity,owner_job_id,effects_hash}));
}

/** The request that publishes the first `count` gathered entries: their SQL,
 * the progress write and the guard that the progress committed. */
function publication(job: ImportJob, count: number, statements: D1Sql[], scope: NativeStagingScope): { statements: D1Sql[]; scope: NativeStagingScope } {
  const end = job.checkpoint + count;
  return { statements: [...statements, ...progress(job, count)], scope: { ...scope, postGuards: [...scope.postGuards, guard(sql('EXISTS(SELECT 1 FROM ik_jobs WHERE id=? AND checkpoint=? AND state=? AND fence=? AND error IS NULL)', job.id, end, end === job.total ? 'completed' : 'applying', job.fence + 1))] } };
}
const cost = (context: D1ImportContext, request: { statements: D1Sql[]; scope: NativeStagingScope }, returnReads: number): number => atomicCount(context, 'apply', request.statements, returnReads, request.scope);
function progress(job: ImportJob, count: number): D1Sql[] {
  const end = job.checkpoint + count;
  const state=end===job.total?'completed':'applying';
  return [changed(sql("INSERT INTO ik_batches(job_id,action,position,end_position,fence) VALUES(?,'apply',?,?,?)", job.id, job.checkpoint, end, job.fence + 1),putRow('ik_batches',{job_id:job.id,action:'apply',position:job.checkpoint,end_position:end,fence:job.fence+1})), changed(sql('UPDATE ik_jobs SET checkpoint=?,state=?,fence=fence+1,error=NULL WHERE id=?', end, state, job.id),updateRows('ik_jobs',row=>row.id===job.id,row=>({...row,checkpoint:end,state,fence:job.fence+1,error:null})))];
}
