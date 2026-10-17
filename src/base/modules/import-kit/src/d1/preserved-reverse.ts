import { retainedSql } from './retained-values.ts';
import { changed, putRow, updateRows, deleteRows } from './expected-state.ts';
import { nativeStagingScope, readStagedTarget, expectedStagedTarget } from './native-staging-context.ts';
import { targetGuards } from './target-guards.ts';
import type { ImportJob, ImportPlan } from '../types.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import type { D1UndoEffect } from './undo-types.ts';
import { canonicalJson } from '../provenance/identity.ts';
import { D1RequestLimitError, remaining } from './budget.ts';
import { atomic, sql, guard, limits } from './database.ts';
import { stored, storedReads, boundary } from './jobs.ts';
import { custodyGuards, inventoryGuards, validatedProvenance } from './integrity.ts';
import { noSuccessors, restorePredecessor } from './history.ts';
import { readOriginals } from './originals.ts';
import { reversalState, reportHash, reversalGuards } from './undo-state.ts';
import { classifyUndo } from './undo-snapshot.ts';
import { insertJsonRows } from './sets.ts';

/** Called after public authority, binding, reversal-mode and custody checks. */
export async function preserveReverse(current: { job: ImportJob; plan: ImportPlan }, context: D1ImportContext, action: 'undo' | 'rollback'): Promise<ImportJob> {
  const { maxBatches, batchSize } = limits(context), id = current.job.id;
  const reversing = action === 'undo' ? 'undoing' : 'rolling_back', terminal = action === 'undo' ? 'undone' : 'rolled_back';
  const { database: db, targets } = context.storage;
  if (!targets.preparePreservedUndo) throw new Error('IMPORT_PRESERVED_UNDO_ADAPTER_REQUIRED');
  if (current.job.state !== reversing) {
    await atomic(context, action, [...boundary(current.job, current.plan, action), await noSuccessors(id, context), ...custodyGuards(current.plan, id), ...await inventoryGuards(current.job, current.plan, context), changed(sql('UPDATE ik_jobs SET state=?,fence=fence+1 WHERE id=?', reversing, id),updateRows('ik_jobs',row=>row.id===id,row=>({...row,state:reversing,fence:current.job.fence+1})))], storedReads(current.job),nativeStagingScope(current.job,current.plan));
    current = await stored(id, context, action);
  }
  let published: ImportJob | undefined;
  try {
    for (let count = 0; count < maxBatches; count++) {
      const { job, plan } = current, state = reversalState(job);
      if (job.state !== reversing) throw new Error('REVERSAL_MODE_CONFLICT');
      await readOriginals(plan, id, context);
      const scope=nativeStagingScope(job,plan);
      const statements: D1Sql[] = [...boundary(job, plan, action), await noSuccessors(id, context), ...custodyGuards(plan, id), ...await inventoryGuards(job, plan, context)];
      const { results: rows } = await db.prepare('SELECT * FROM ik_provenance WHERE job_id=? AND reversed=0 ORDER BY ordinal DESC,effect_ordinal DESC').bind(id).all();
      const retained = new Set(state.dispositions.filter(d => d.disposition === 'retained').map(d => d.effectId));
      const effects: D1UndoEffect[] = [];
      for (const raw of rows) {
        const p = await validatedProvenance(raw, context, statements), effect = plan.entries[raw.ordinal].effects[raw.effect_ordinal];
        effects.push({ provenance: p, retained: retained.has(p.id), expected: { destination: p.destination, targetKind: p.targetKind, targetId: p.targetId, revision: p.appliedRevision, value: effect.value, ...(effect.attachment ? { attachment: effect.attachment } : {}) } });
      }
      const decision = classifyUndo(effects, await targets.preparePreservedUndo(db, context.destination, structuredClone(effects)), context);
      statements.push(...decision.guards);
      if(context.maxBatchBytes!==undefined)for(const effect of effects)await readStagedTarget(context,scope,effect.provenance.targetKind,effect.provenance.targetId);
      const selected: typeof decision.ordered = [], newRows: any[] = [];
      for (const group of decision.groups) {
        if (selected.length && selected.length + group.length > batchSize) break;
        selected.push(...group);
        if (selected.length >= batchSize) break;
      }
      for (const d of selected) {
        const effect = effects.find(e => e.provenance.id === d.effectId)!, p = effect.provenance;
        if (d.disposition === 'reversed') {
          const restoration: D1Sql[] = [];
          await restorePredecessor(p, context, statements, restoration);
          const o = decision.observations.get(p.id)!;
          statements.push(guard(o.guard ?? targets.matches(o.current, context.destination, p.targetKind, p.targetId)), ...(p.before ? targets.write(p.before) : targets.remove(context.destination, p.targetKind, p.targetId)), guard(targets.matches(p.before, context.destination, p.targetKind, p.targetId)), changed(retainedSql(['UPDATE ik_provenance SET reversed=1,record_json=', ' WHERE id=', ''], [canonicalJson({ ...p, reversed: true }), p.id], [0]),updateRows('ik_provenance',row=>row.id===p.id,row=>({...row,reversed:1,record_json:canonicalJson({...p,reversed:true})}))), ...restoration);
        }
        expectedStagedTarget(scope,p.targetKind,p.targetId,d.disposition==='reversed'?!!p.before:!!decision.observations.get(p.id)!.current);
        newRows.push({ job_id: id, effect_id: d.effectId, position: state.cursor + newRows.length, record_json: canonicalJson(d) });
      }
      if(context.maxBatchBytes!==undefined)for(const effect of effects){const p=effect.provenance,reverse=selected.some(d=>d.effectId===p.id&&d.disposition==='reversed'),row=reverse?p.before:decision.observations.get(p.id)!.current;scope.postGuards.push(...targetGuards(targets,row,context.destination,p.targetKind,p.targetId));}
      statements.push(...insertJsonRows('ik_d1_reversal_reports', ['job_id','effect_id','position','record_json'], newRows));
      const done = selected.length === decision.ordered.length, allRows = [...state.rows, ...newRows];
      const next = { ...state, cursor: allRows.length, hash: reportHash(allRows), rows: allRows, dispositions: [...state.dispositions, ...selected] };
      statements.push(changed(sql('DELETE FROM ik_sources WHERE job_id=? AND NOT EXISTS(SELECT 1 FROM ik_provenance WHERE ik_provenance.source_identity=ik_sources.identity AND reversed=0) AND NOT EXISTS(SELECT 1 FROM ik_d1_history WHERE ik_d1_history.source_identity=ik_sources.identity)', id),deleteRows('ik_sources',(row,expected)=>row.job_id===id&&![...expected.rows('ik_provenance').values()].some(p=>p.source_identity===row.identity&&!p.reversed)&&![...expected.rows('ik_d1_history').values()].some(p=>p.source_identity===row.identity))),
        changed(sql('INSERT INTO ik_batches(job_id,action,position,end_position,fence) VALUES(?,?,?,?,?)', id, action, job.fence, job.fence + 1, job.fence + 1),putRow('ik_batches',{job_id:id,action,position:job.fence,end_position:job.fence+1,fence:job.fence+1})),
        changed(sql('UPDATE ik_jobs SET state=?,fence=fence+1,error=NULL,reversal_cursor=?,reversal_hash=? WHERE id=?', done ? terminal : reversing, next.cursor, next.hash, id),updateRows('ik_jobs',row=>row.id===id,row=>({...row,state:done?terminal:reversing,fence:job.fence+1,error:null,reversal_cursor:next.cursor,reversal_hash:next.hash}))),
        ...reversalGuards(id, next));
      await atomic(context, action, statements, storedReads(job),scope);
      current = await stored(id, context, action); published = current.job;
      if (done || remaining(context) < 200) return published;
    }
  } catch (error) {
    if (error instanceof D1RequestLimitError && published) return published;
    throw error;
  }
  return current.job;
}
