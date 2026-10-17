import type { ImportContext, ImportMapping, ImportPlan, SourceBundle, TargetEffect, PlanEntry } from '../types.ts';
import { canonicalJson, sourceIdentity, targetIdentity, requireText, assertNoCredentials, attachmentHash } from '../provenance/identity.ts';
import { authorize } from '../journal/authority.ts';
import { validateArtifactOriginal } from '../storage/originals.ts';
import { assertPlanSize, assertSourceSize } from './size.ts';
import { PlanSizeBudget } from './budget.ts';
import { canonicalHash, cloneJson } from '../provenance/canonical-stream.ts';
import { validateJson } from '../provenance/validate-json.ts';
import { snapshotJson } from '../provenance/json-snapshot.ts';
import { createCanonicalHasher } from '../provenance/canonical-hash.ts';

export const planFingerprint = (plan: ImportPlan): string => {
  const { fingerprint: _fingerprint, ...content } = plan;
  return canonicalHash(content);
};
export function validateEffect(effect: TargetEffect): void {
  requireText(effect.targetKind, 'targetKind'); requireText(effect.targetId, 'targetId');
  validateJson(effect.value); attachmentHash(effect.attachment);
  if (effect.expectedRevision !== undefined) requireText(effect.expectedRevision, 'expectedRevision');
  if (/(^|[_-])(grant|grants|job|jobs|dispatch|outbox|session|sessions|credential|credentials)([_-]|$)/i.test(effect.targetKind)) throw new Error('ACTIVE_EFFECT_FORBIDDEN');
  if (/(^|[_-])(operator|operators|principal|principals|user|users|member|members|account|accounts|contact|contacts)([_-]|$)/i.test(effect.targetKind) && effect.value && typeof effect.value === 'object' && !Array.isArray(effect.value)) {
    for (const [key, value] of Object.entries(effect.value)) {
      if (/^(active|enabled|verified|can_administrate|canAdministrate|can_send|canSend)$/i.test(key) && value !== false && value !== 0 && value !== null) throw new Error('ACTIVE_EFFECT_FORBIDDEN');
    }
  }
}
export function planImport(source: SourceBundle, mapping: ImportMapping, context: ImportContext): ImportPlan {
  authorize(context, 'plan');
  return buildImportPlan(source, mapping, context);
}
/** Pure validated planner shared by independently authorized storage ports. */
type PlanningContext = Pick<ImportContext, 'destination' | 'namespace' | 'mappingVersion'> & { authorization: { principalId: string } };
export function buildImportPlan(source: SourceBundle, mapping: ImportMapping, context: PlanningContext): ImportPlan {
  return buildPlan(source, mapping, context, value => JSON.parse(canonicalJson(value)) as SourceBundle);
}
/** Stream pages preserve independent containers while sharing immutable text.
 * Ordinary eager planning keeps its established canonical source-copy contract. */
export function buildStreamPagePlan(source: SourceBundle, mapping: ImportMapping, context: PlanningContext): ImportPlan {
  return buildPlan(source, mapping, context, cloneJson);
}
function buildPlan(source: SourceBundle, mapping: ImportMapping, context: PlanningContext, copySource: (source: SourceBundle) => SourceBundle): ImportPlan {
  const payloadHasher = createCanonicalHasher();
  try { return buildPlanWithHasher(source, mapping, context, copySource, payloadHasher.hash); }
  finally { payloadHasher.clear(); }
}
function buildPlanWithHasher(source: SourceBundle, mapping: ImportMapping, context: PlanningContext, copySource: (source: SourceBundle) => SourceBundle, hashPayload: typeof canonicalHash): ImportPlan {
  requireText(mapping.version, 'mapping version');
  if (mapping.version !== context.mappingVersion) throw new Error('MAPPING_VERSION_MISMATCH');
  assertSourceSize(source);
  const plan: ImportPlan = { version: 1, destination: context.destination, namespace: context.namespace, mappingVersion: mapping.version, principalId: context.authorization.principalId, source, entries: [], diagnostics: source.diagnostics, counts: { input: source.records.length, unique: 0, duplicates: 0, mapped: 0, unmapped: 0, rejected: 0, effects: 0, byKind: Object.create(null) }, fingerprint: '' };
  // Source diagnostics are persisted twice. Admit the initial plan before
  // encoding or copying any of the source, including caller metadata.
  const budget = new PlanSizeBudget(plan);
  assertNoCredentials(source, source.paginationCursorPolicy);
  const copy = copySource(source);
  plan.source = copy; plan.diagnostics = [...copy.diagnostics];
  const seen = new Map<string, string>(), targets = new Set<string>();
  const artifacts = new Set<string>();
  for (const artifact of copy.artifacts) {
    requireText(artifact.name, 'artifact name');
    if (!/^[a-f0-9]{64}$/.test(artifact.sha256) || !Number.isSafeInteger(artifact.size) || artifact.size < 0) throw new Error('INVALID_ARTIFACT');
    const key = canonicalJson([artifact.name, artifact.sha256]);
    if (artifacts.has(key)) throw new Error('DUPLICATE_ARTIFACT');
    artifacts.add(key);
    const previousBytes = budget.measure(artifact);
    if (!artifact.original) artifact.original = { status: 'unavailable', reason: 'not-supplied' };
    if (artifact.original.status === 'unavailable') budget.diagnostic({ severity: 'warning', code: 'ORIGINAL_UNAVAILABLE', message: 'Exact original bytes were not supplied for ' + artifact.name });
    budget.replace(previousBytes, artifact); budget.assert();
    validateArtifactOriginal(artifact, copy.paginationCursorPolicy);
  }
  for (const sourceRecord of copy.records) {
    requireText(sourceRecord.kind, 'kind'); requireText(sourceRecord.sourceId, 'sourceId'); requireText(sourceRecord.location?.artifact, 'source location');
    if (sourceRecord.namespace !== context.namespace) throw new Error('SOURCE_NAMESPACE_MISMATCH');
    const validatePayload = () => { if (hashPayload(sourceRecord.payload) !== sourceRecord.payloadHash) throw new Error('SOURCE_HASH_MISMATCH'); };
    if (!copy.artifacts.some(artifact => artifact.sha256 === sourceRecord.artifactHash && artifact.name === sourceRecord.location.artifact)) throw new Error('SOURCE_ARTIFACT_MISSING');
    const identity = sourceIdentity(context.destination, context.namespace, sourceRecord.kind, sourceRecord.sourceId);
    if (seen.has(identity)) {
      validatePayload();
      if (seen.get(identity) !== sourceRecord.payloadHash) throw new Error('DUPLICATE_SOURCE_CONFLICT');
      budget.increment('duplicates');
      budget.diagnostic({ severity: 'warning', code: 'DUPLICATE_SOURCE', message: 'Identical source identity collapsed.', kind: sourceRecord.kind, sourceId: sourceRecord.sourceId, location: sourceRecord.location });
      budget.assert();
      continue;
    }
    // Reserve the second persisted occurrence of source before payload
    // encoding or the mapper-input clone. Identity components are bounded IDs.
    const entry: PlanEntry = { source: sourceRecord, identity, effects: [], unmapped: [] };
    budget.entry(entry); budget.assert();
    validatePayload();
    seen.set(identity, sourceRecord.payloadHash);
    const mapped = snapshotJson(mapping.map(cloneJson(sourceRecord)));
    // Validate durable values without serializing the mapper's expansion.
    budget.measure(mapped, true);
    if (!Array.isArray(mapped.effects)) throw new Error('INVALID_MAPPING');
    if (mapped.effects.length > 1000) throw new Error('SOURCE_EFFECT_LIMIT');
    if (mapped.rejection && mapped.effects.length) throw new Error('REJECTED_RECORD_HAS_EFFECTS');
    const unmapped = !mapped.effects.length && !mapped.rejection && !mapped.unmapped?.length
      ? [{ reason: 'Source object has no native mapping; parsed record retained. Original availability is reported separately.' }]
      : mapped.unmapped ?? [];
    budget.mapping(entry, mapped.effects, unmapped, mapped.rejection);
    for (const item of unmapped) budget.diagnostic({ severity: 'warning', code: 'UNMAPPED', message: item.reason, kind: sourceRecord.kind, sourceId: sourceRecord.sourceId, location: sourceRecord.location, ...(item.field ? { field: item.field } : {}) });
    if (mapped.rejection) budget.diagnostic({ severity: 'error', code: 'REJECTED', message: mapped.rejection, kind: sourceRecord.kind, sourceId: sourceRecord.sourceId, location: sourceRecord.location });
    // Includes all repeated fields and cumulative mappings, before cloning,
    // effect validation (attachment decoding), or target identity encoding.
    budget.assert();
    for (const item of unmapped) requireText(item.reason, 'unmapped reason');
    assertNoCredentials(mapped);
    const effects = cloneJson(mapped.effects);
    entry.effects = effects;
    entry.unmapped = cloneJson(unmapped);
    for (const effect of effects) {
      validateEffect(effect);
      const key = targetIdentity(context.destination, effect.targetKind, effect.targetId);
      if (targets.has(key)) throw new Error('DUPLICATE_TARGET');
      targets.add(key);
    }

  }
  plan.counts.byKind = JSON.parse(canonicalJson(plan.counts.byKind));
  assertPlanSize(plan);
  plan.fingerprint = planFingerprint(plan);
  return plan;
}
