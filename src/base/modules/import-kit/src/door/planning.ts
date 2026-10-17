import { randomUUID } from 'node:crypto';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { readFileSource } from '../readers/file.ts';
import { planD1Import } from '../d1/jobs.ts';
import type { ImportPlan, MappingResult } from '../types.ts';
import type { DoorPlanRow, DoorRecord, DoorRequest, DoorSnapshot } from './types.ts';
import { kitContext } from './authority.ts';
import { importedTarget, recordKey } from './inventory.ts';
import { DoorError } from './grants.ts';
import { lineage, loadSnapshot, planRow, saveSnapshot } from './store.ts';
import { bundleBytes, text, uploadRow } from './uploads.ts';

export function preview(snapshot: DoorSnapshot) {
  const {plan,differences} = snapshot;
  return {planId:snapshot.id,fingerprint:plan.fingerprint,bundleSha256:snapshot.bundleSha256,
    summary:{sourceRecords:snapshot.sourceCount,created:differences.additions.length,updated:differences.replacements.length,unchanged:differences.unchanged.length,gone:differences.gone.length},
    counts:plan.counts,rejected:plan.entries.filter(e=>e.rejection).map(e=>({kind:e.source.kind,sourceId:e.source.sourceId,reason:e.rejection})),
    diagnostics:plan.diagnostics,differences};
}
export async function plan(request: DoorRequest, input: Record<string, unknown>) {
  if (Object.keys(input).some(k=>!['uploadId','catchup'].includes(k)) || (input.catchup !== undefined && typeof input.catchup !== 'boolean')) throw new DoorError('invalid_plan_input',400);
  const upload = await uploadRow(request,text(input.uploadId,'upload_id'));
  const current = await lineage(request);
  const existing = await request.options.database.prepare('SELECT * FROM ik_door_plans WHERE installation_id=? AND namespace=? AND bundle_sha256=?')
    .bind(request.grant.installationId,request.grant.namespace,upload.bundle_sha256).first<DoorPlanRow>();
  if (existing) {
    if (!existing.complete) throw new DoorError('plan_staging_interrupted_cancel_and_replan',409,{planId:existing.id});
    const snapshot = await loadSnapshot(request,existing);
    if (snapshot.sourceCount > request.grant.maxRecords) throw new DoorError('plan_exceeds_grant_quota');
    if (snapshot.plan.mappingVersion !== request.options.mapping.version) throw new DoorError('snapshot_mapping_changed_use_new_namespace',409);
    if (request.grant.catchup === 'bounded' && (!snapshot.standingApproval || snapshot.standingApproval.id !== request.grant.approvalRecordId || snapshot.standingApproval.at !== request.grant.approvedAt)) throw new DoorError('standing_approval_mismatch');
    return preview(snapshot);
  }
  if (current.pending_id) throw new DoorError('namespace_has_pending_import',409);
  const previous = current.head_id ? await loadSnapshot(request,await planRow(request,current.head_id)) : null;
  if (previous && input.catchup !== true) throw new DoorError('catchup_required',409);
  if (!previous && (input.catchup === true || request.grant.catchup === 'bounded')) throw new DoorError('catchup_requires_baseline',409);
  const bytes = await bundleBytes(request,upload), options = request.options;
  const kitNamespace = 'door:' + sha256(canonicalJson([request.grant.namespace,upload.bundle_sha256]));
  const source = await readFileSource({namespace:kitNamespace,name:upload.name,bytes,format:options.format,parse:options.parse},
    {...options.limits,maxBytes:Math.min(options.limits?.maxBytes ?? request.grant.maxBytes,request.grant.maxBytes),maxRecords:Math.min(options.limits?.maxRecords ?? request.grant.maxRecords,request.grant.maxRecords)});
  const prior = new Map((previous?.records ?? []).map(r=>[recordKey(r),r]));
  const revisions = new Map<string,{revision:string;matches:boolean}>();
  for (const record of prior.values()) for (const target of record.targets) {
    const found = await importedTarget(request,target);
    revisions.set(canonicalJson([target.kind,target.id]),{revision:found.provenance.appliedRevision,matches:found.matches});
  }
  const snapshot: DoorSnapshot = {id:randomUUID(),namespace:request.grant.namespace,bundleSha256:upload.bundle_sha256,kitNamespace,
    previous:previous?.id ?? null,records:[],sourceCount:0,plan:{} as ImportPlan,
    differences:{additions:[],replacements:[],unchanged:[],gone:[],conflicts:[]},
    standingApproval:request.grant.catchup==='bounded'?{id:request.grant.approvalRecordId!,at:request.grant.approvedAt!}:null};
  const mapping = {version:options.mapping.version,map(record:typeof source.records[number]):MappingResult {
    const key = recordKey(record), old = prior.get(key);
    let result: MappingResult;
    if (old && (old.targets.length || old.explanation) && old.sourceHash === record.payloadHash && previous?.plan.mappingVersion === options.mapping.version) {
      result = {effects:[],unmapped:[{reason:'Unchanged source; previous imported targets retained.'}]};
      snapshot.differences.unchanged.push(key);
    } else {
      result = options.mapping.map(record);
      if (old?.targets.length && !result.rejection) {
        const keys = result.effects.map(e=>canonicalJson([e.targetKind,e.targetId])).sort();
        const oldKeys = old.targets.map(t=>canonicalJson([t.kind,t.id])).sort();
        if (canonicalJson(keys)!==canonicalJson(oldKeys)) result = {effects:[],rejection:'Changed target set requires a separate owner proposal; previous targets retained.'};
        else if (keys.some(k=>!revisions.get(k)?.matches)) result = {effects:[],rejection:'Native edit conflict; previous imported targets preserved.'};
        else result = {...result,effects:result.effects.map(e=>({...e,expectedRevision:revisions.get(canonicalJson([e.targetKind,e.targetId]))!.revision}))};
      } else if (previous && result.effects.some(e=>e.expectedRevision!==undefined)) {
        result = {effects:[],rejection:'Catch-up additions cannot replace records outside this import.'};
      }
      if (result.rejection) snapshot.differences.conflicts.push(key);
      else if (result.effects.length) (old?.targets.length?snapshot.differences.replacements:snapshot.differences.additions).push(key);
    }
    return result;
  }};
  snapshot.plan = await planD1Import(source,mapping,kitContext(request,{...snapshot,plan:{mappingVersion:options.mapping.version} as ImportPlan}));
  const seen = new Set<string>();
  for (const entry of snapshot.plan.entries) {
    const key = recordKey(entry.source), old = prior.get(key); seen.add(key);
    const unchanged = snapshot.differences.unchanged.includes(key);
    const record: DoorRecord = {kind:entry.source.kind,sourceId:entry.source.sourceId,
      sourceHash:entry.rejection && old ? old.sourceHash : entry.source.payloadHash,
      targets:entry.effects.length?entry.effects.map(e=>({kind:e.targetKind,id:e.targetId,ownerPlanId:snapshot.id,sourceIdentity:entry.identity})):(old?.targets ?? [])};
    const reason = unchanged ? (old?.targets.length ? undefined : old?.explanation) : entry.rejection ?? (entry.unmapped.length ? entry.unmapped.map(u=>u.reason).join('; ') : undefined);
    if (reason) record.explanation = reason;
    snapshot.records.push(record);
  }
  snapshot.sourceCount = seen.size;
  for (const [key,old] of prior) if (!seen.has(key)) {
    snapshot.differences.gone.push(key);
    snapshot.records.push({...old,explanation:'Absent from source snapshot; retained in the seed. No deletion proposed.'});
  }
  if (request.grant.catchup === 'bounded') assertBounded(snapshot,previous!);
  await saveSnapshot(request,snapshot);
  return preview(snapshot);
}
export function assertBounded(snapshot: DoorSnapshot, previous: DoorSnapshot): void {
  const d = snapshot.differences, changed = d.additions.length + d.replacements.length;
  const refused = !previous.sourceCount ? 'empty_baseline' : d.gone.length ? 'source_records_gone' : d.conflicts.length ? 'native_or_mapping_conflict'
    : snapshot.plan.counts.rejected || snapshot.records.some(r=>!r.targets.length && !d.unchanged.includes(recordKey(r))) ? 'rejected_or_unmapped_records'
    : changed * 100 >= previous.sourceCount * 5 ? 'change_must_be_under_five_percent' : null;
  if (refused) throw new DoorError('catchup_requires_proposal:' + refused,409);
}
