import { canonicalJson, sha256, attachmentHash } from '../provenance/identity.ts';
import type { ProvenanceRow, TargetRow } from '../types.ts';
import type { DoorRecord, DoorRequest, DoorTarget } from './types.ts';
import { DoorError } from './grants.ts';
import { planRow } from './store.ts';
import { readD1PersistedImport } from '../d1/index.ts';

export const recordKey = (r: {kind:string;sourceId:string}) => canonicalJson([r.kind,r.sourceId]);
/** Product lineage only locates evidence. The kit's immutable plan/provenance
 * owns the revision and full native snapshot; a current native revision is never
 * promoted to permission to overwrite an edit. */
export async function importedTarget(request: DoorRequest, target: DoorTarget): Promise<{provenance:ProvenanceRow;current:TargetRow|undefined;matches:boolean}> {
  const owner = await planRow(request, target.ownerPlanId);
  if (!owner.job_id) throw new DoorError('import_lineage_incomplete', 409);
  request.persisted ??= readD1PersistedImport({destination:request.options.destination,namespace:request.grant.namespace,mappingVersion:request.options.mapping.version,
    storage:{database:request.options.database,targets:request.options.targets},
    authorization:{principalId:request.options.ownerPrincipalId,predicate:action=>action==='read'?request.authority:{sql:'0'}}});
  const persisted = await request.persisted;
  const provenance = persisted.provenance.find(p=>p.jobId===owner.job_id && p.sourceIdentity===target.sourceIdentity && p.destination===request.options.destination && p.targetKind===target.kind && p.targetId===target.id && !p.reversed);
  if (!provenance) throw new DoorError('import_attribution_missing', 409);
  if (provenance.jobId !== owner.job_id || provenance.sourceIdentity !== target.sourceIdentity || provenance.destination !== request.options.destination || provenance.targetKind !== target.kind || provenance.targetId !== target.id || provenance.reversed) throw new DoorError('import_attribution_mismatch', 409);
  const current = await request.options.targets.read(request.options.database, request.options.destination, target.kind, target.id);
  let matches = false;
  try { matches = !!current && current.revision === provenance.appliedRevision && sha256(canonicalJson(current.value)) === provenance.valueHash && attachmentHash(current.attachment) === provenance.attachmentHash; } catch { /* Corrupt native attachment is a conflict, never replacement authority. */ }
  return {provenance,current,matches};
}
export async function inspectRecords(request: DoorRequest, records: DoorRecord[]) {
  const result = [];
  let present = 0, explained = 0, targets = 0;
  const counts: Record<string, number> = Object.create(null);
  for (const record of records) {
    const conflicts: string[] = [];
    for (const target of record.targets) {
      const actual = await importedTarget(request,target);
      if (!actual.matches) conflicts.push(canonicalJson([target.kind,target.id]));
      else { targets++; counts[target.kind] = (counts[target.kind] ?? 0) + 1; }
    }
    if (!record.targets.length && !record.explanation) conflicts.push('no_native_target_or_explanation');
    if (conflicts.length) result.push({kind:record.kind,sourceId:record.sourceId,status:'mismatched',conflicts});
    else if (record.explanation) { explained++; result.push({kind:record.kind,sourceId:record.sourceId,status:'explained',reason:record.explanation}); }
    else { present++; result.push({kind:record.kind,sourceId:record.sourceId,status:'present'}); }
  }
  return {ok: result.every(r=>r.status!=='mismatched'), sourceIds:result, counts:{sourceRecords:records.length,present,explained,nativeTargets:targets,byKind:counts}};
}
