import { guardErasedSources } from './erasure-claims.ts';
import type { InventorySnapshot } from './inventory-snapshot.ts';
import type { ImportJob, ImportPlan, PlanEntry, ProvenanceRow } from '../types.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { claimColumns, exactRows, selectKeys, selectedCounts } from './sets.ts';
import { provenanceSet } from './provenance-set.ts';
import { reversalState } from './undo-state.ts';
export { custodyGuards } from './original-storage.ts';
// Only the current preflight: inventory already emits exact guards for every
// memoized row. Replaced before each publication and never shared by requests.
const inventories = new WeakMap<D1ImportContext, Map<string, { raw: string; decoded: ProvenanceRow }>>();

export const releaseInventory=(context:D1ImportContext):void=>{inventories.delete(context);};

export async function validatedProvenance(raw: any, context: D1ImportContext, guards: D1Sql[]): Promise<ProvenanceRow> {
  const cached = inventories.get(context)?.get(raw.id);
  if (cached?.raw === canonicalJson(raw)) return cached.decoded;
  return (await provenanceSet([raw], context, guards)).decoded[0];
}
function validateReusedEntry(entry: PlanEntry, mappingVersion: string, owner: any, plan: ImportPlan | undefined, decision: any): void {
  if (!owner || !plan) throw new Error('PROVENANCE_JOB_MISSING');
  const index = plan.entries.findIndex(e => e.identity === entry.identity), original = plan.entries[index];
  const effectsHash = sha256(canonicalJson(entry.effects));
  if (plan.mappingVersion !== mappingVersion || !decision || decision.job_id !== owner.id || decision.ordinal !== index || decision.owner_job_id !== owner.id || decision.source_identity !== entry.identity || decision.effects_hash !== effectsHash || !original || index >= owner.checkpoint || original.source.payloadHash !== entry.source.payloadHash || sha256(canonicalJson(original.effects)) !== effectsHash) throw new Error('IMPORT_INVENTORY_MISMATCH');
}
/** A matching source claim is insufficient, especially when no native effects
 * exist: validate and fence the actual owner's committed decision before reuse. */
export async function reusedSourceGuards(entry: PlanEntry, ownerId: string, context: D1ImportContext, guards: D1Sql[]): Promise<void> {
  const state = await provenanceSet([], context, guards, [ownerId]);
  const owner = state.owners.get(ownerId), plan = state.plans.get(ownerId);
  const index = plan?.entries.findIndex(e => e.identity === entry.identity) ?? -1;
  const decision = await context.storage.database.prepare('SELECT * FROM ik_d1_entries WHERE job_id=? AND ordinal=?').bind(ownerId, index).first();
  validateReusedEntry(entry, context.mappingVersion, owner, plan, decision);
  guards.push(...exactRows('ik_d1_entries', ['job_id','ordinal','source_identity','owner_job_id','effects_hash'], [decision]));
}
/** Complete inventory closure, validated once with bounded set reads. No cache
 * survives this preflight; exact full-field snapshots fence the atomic batch. */
export async function inventoryGuards(job: ImportJob, plan: ImportPlan, context: D1ImportContext, snapshot?: InventorySnapshot): Promise<D1Sql[]> {
  const db = context.storage.database, guards: D1Sql[] = snapshot ? [] : await guardErasedSources(plan, context.storage.database);
  if (!Number.isSafeInteger(job.checkpoint) || job.checkpoint < 0 || job.checkpoint > job.total) throw new Error('CHECKPOINT_CONFLICT');
  const read = (table: string, key: string, values: string[]) => snapshot ? Promise.resolve(snapshot.rows(table, key, values)) : selectKeys(db, table, key, values);
  const entries = (await read('ik_d1_entries', 'job_id', [job.id])).sort((a, b) => a.ordinal - b.ordinal);
  const provenance = await read('ik_provenance', 'job_id', [job.id]);
  const history = await read('ik_d1_history', 'job_id', [job.id]);
  if (entries.length !== job.checkpoint) throw new Error('IMPORT_INVENTORY_MISMATCH');
  const reusedOwners = [...new Set(entries.filter(r => r.owner_job_id !== job.id).map(r => r.owner_job_id))];
  const reusedActive = await read('ik_provenance', 'job_id', reusedOwners);
  const reusedHistory = await read('ik_d1_history', 'job_id', reusedOwners);
  const reusedEntries = await read('ik_d1_entries', 'job_id', reusedOwners);
  const reusedClaims = await read('ik_sources', 'job_id', reusedOwners);
  guards.push(...selectedCounts('ik_provenance', 'job_id', reusedOwners, reusedActive),
    ...selectedCounts('ik_d1_history', 'job_id', reusedOwners, reusedHistory),
    ...selectedCounts('ik_d1_entries', 'job_id', reusedOwners, reusedEntries),
    ...selectedCounts('ik_sources', 'job_id', reusedOwners, reusedClaims));
  guards.push(...exactRows('ik_sources', claimColumns, reusedClaims));
  guards.push(...exactRows('ik_d1_entries', ['job_id','ordinal','source_identity','owner_job_id','effects_hash'], reusedEntries));
  const all = [...provenance, ...history, ...reusedActive, ...reusedHistory];
  const state = await provenanceSet(all, context, guards, [job.id, ...reusedOwners], entries.map(r => r.source_identity), snapshot);
  let expectedEffects = 0;
  const expectedClaims = new Set([...provenance, ...history].filter(r => !r.reversed).map(r => r.source_identity));
  for (const [ordinal, recorded] of entries.entries()) {
    const entry = plan.entries[ordinal], effectsHash = sha256(canonicalJson(entry.effects));
    if (recorded.ordinal !== ordinal || recorded.source_identity !== entry.identity || recorded.effects_hash !== effectsHash) throw new Error('IMPORT_INVENTORY_MISMATCH');
    if (recorded.owner_job_id === job.id) {
      expectedEffects += entry.effects.length;
      if (!entry.effects.length && !['undoing', 'rolling_back', 'undone', 'rolled_back'].includes(job.state)) expectedClaims.add(entry.identity);
    } else {
      const owner = state.owners.get(recorded.owner_job_id), ownerPlan = state.plans.get(recorded.owner_job_id);
      const index = ownerPlan?.entries.findIndex(e => e.identity === entry.identity);
      const decision = reusedEntries.find(e => e.job_id === recorded.owner_job_id && e.ordinal === index);
      validateReusedEntry(entry, plan.mappingVersion, owner, ownerPlan, decision);
      // Non-reversed effects validate their claims in provenanceSet. A source
      // with no effects needs the same check here on every later checkpoint.
      // Reversal may legitimately release its claim while retaining decisions.
      if (!entry.effects.length && !['undoing', 'rolling_back', 'undone', 'rolled_back'].includes(owner.state)) {
        const claim = state.claims.get(entry.identity);
        if (!claim || claim.job_id !== owner.id || claim.source_hash !== entry.source.payloadHash
          || claim.mapping_version !== ownerPlan!.mappingVersion || claim.effects_hash !== effectsHash) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
      }
    }
  }
  const reversal = reversalState(job);
  const reportEffects = expectedEffects + reversal.dispositions.filter(d => d.redacted === 'erased').length;
  if (reversal.cursor > reportEffects || (reversal.policy === 'preserve-conflicts' && ['undone','rolled_back'].includes(job.state) && reversal.cursor !== reportEffects)) throw new Error('IMPORT_REVERSAL_INTEGRITY');
  if (new Set([...provenance, ...history].map(r => r.id)).size !== expectedEffects || provenance.length + history.length !== expectedEffects) throw new Error('IMPORT_INVENTORY_MISMATCH');
  const ownedClaims = await read('ik_sources', 'job_id', [job.id]);
  // A source-only claim may still be present immediately after reversal intent;
  // the first reversing batch releases it atomically.
  const zeroClaims = new Set(entries.filter(r => r.owner_job_id === job.id && !plan.entries[r.ordinal].effects.length).map(r => r.source_identity));
  for (const claim of ownedClaims) {
    if (!expectedClaims.has(claim.identity) && !(zeroClaims.has(claim.identity) && ['undoing', 'rolling_back'].includes(job.state))) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
    const entry = plan.entries.find(e => e.identity === claim.identity);
    if (!entry || claim.source_hash !== entry.source.payloadHash || claim.mapping_version !== plan.mappingVersion || claim.effects_hash !== sha256(canonicalJson(entry.effects))) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
  }
  for (const identity of expectedClaims) if (!ownedClaims.some(c => c.identity === identity)) throw new Error('PROVENANCE_SOURCE_CLAIM_MISMATCH');
  guards.push(...exactRows('ik_sources', ['identity','source_hash','mapping_version','effects_hash','job_id'], ownedClaims, sql('job_id=?', job.id)));
  guards.push(...exactRows('ik_d1_entries', ['job_id','ordinal','source_identity','owner_job_id','effects_hash'], entries, sql('job_id=?', job.id)));
  // Row comparisons are supplied by provenanceSet; cardinalities close each set.
  if(context.maxBatchBytes!==undefined){
    for(const [table,rows] of [['ik_provenance',provenance],['ik_d1_history',history],['ik_sources',ownedClaims],['ik_d1_entries',entries]] as const) guards.push(...selectedCounts(table,'job_id',[job.id],rows));
  }else{
    guards.push(guard(sql('(SELECT COUNT(*) FROM ik_provenance WHERE job_id=?)=?', job.id, provenance.length)));
    guards.push(guard(sql('(SELECT COUNT(*) FROM ik_d1_history WHERE job_id=?)=?', job.id, history.length)));
  }
  const allBatches = await read('ik_batches', 'job_id', [job.id]);
  if(context.maxBatchBytes!==undefined)guards.push(...exactRows('ik_batches',['job_id','action','position','end_position','fence'],allBatches),...selectedCounts('ik_batches','job_id',[job.id],allBatches));
  const batches = allBatches.filter(row => row.action === 'apply').sort((a, b) => a.position - b.position);
  let checkpoint = 0, fence = 0;
  for (const batch of batches) {
    if (batch.position !== checkpoint || batch.end_position < batch.position || batch.end_position > job.total || batch.fence !== fence + 1 || batch.fence > job.fence) throw new Error('IMPORT_JOURNAL_MISMATCH');
    checkpoint = batch.end_position; fence = batch.fence;
  }
  if (checkpoint !== job.checkpoint) throw new Error('IMPORT_JOURNAL_MISMATCH');
  guards.push(...exactRows('ik_batches', ['job_id','action','position','end_position','fence'], batches, sql("job_id=? AND action='apply'", job.id)));
  if (reversal.policy === 'preserve-conflicts' && ['undoing','undone','rolling_back','rolled_back'].includes(job.state)) {
    const reversedBatches=snapshot?snapshot.rows('ik_batches','job_id',[job.id]).filter(row=>row.action!=='apply').sort((a,b)=>a.position-b.position):(await db.prepare("SELECT * FROM ik_batches WHERE job_id=? AND action<>'apply' ORDER BY position").bind(job.id).all()).results;
    const action = ['undoing','undone'].includes(job.state) ? 'undo' : 'rollback';
    let expectedFence = fence + 1; // The separately durable reversal intent.
    for (const batch of reversedBatches) {
      if (batch.action !== action || batch.position !== expectedFence || batch.end_position !== expectedFence + 1 || batch.fence !== expectedFence + 1) throw new Error('IMPORT_JOURNAL_MISMATCH');
      expectedFence++;
    }
    if (job.fence !== expectedFence || (reversal.cursor > 0 && !reversedBatches.length) || (['undone','rolled_back'].includes(job.state) && !reversedBatches.length)) throw new Error('IMPORT_JOURNAL_MISMATCH');
    guards.push(...exactRows('ik_batches', ['job_id','action','position','end_position','fence'], reversedBatches, sql("job_id=? AND action<>'apply'", job.id)));
  }

  inventories.set(context, new Map(all.map((raw, i) => [raw.id, { raw: canonicalJson(raw), decoded: state.decoded[i] }])));
  return guards;
}
