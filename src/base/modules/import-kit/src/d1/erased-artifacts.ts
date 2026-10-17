import type { ImportPlan, OriginalErasurePolicy, SourceLocation, SourceRecord } from '../types.ts';
import { canonicalJson, sha256, sourceIdentity } from '../provenance/identity.ts';

const artifactKey = (name: string, hash: string) => canonicalJson([name, hash]);
/** Lineage, including duplicate source occurrences and byte-identical artifacts,
 * selects complete originals. Names and pointers can themselves contain PII. */
export function withdrawArtifacts(plan: ImportPlan, next: ImportPlan, identities: Set<string>, policy?: OriginalErasurePolicy, originals?: Set<string>): void {
  if (policy === undefined) return;
  if (!['withdraw-affected-artifacts', 'withdraw-all-job-artifacts'].includes(policy)) throw new Error('INVALID_ORIGINAL_ERASURE_POLICY');
  if (plan.erasure?.originalPolicy && plan.erasure.originalPolicy !== policy) throw new Error('IMPORT_ORIGINAL_ERASURE_POLICY_CONFLICT');
  const hashes = new Set([...plan.source.records.filter(record => identities.has(sourceIdentity(plan.destination, plan.namespace, record.kind, record.sourceId))).map(record => record.artifactHash), ...originals ?? []]);
  const names = new Set<string>(), replacements = new Map<string, string>();
  next.source.artifacts = plan.source.artifacts.map(artifact => {
    if (policy !== 'withdraw-all-job-artifacts' && !hashes.has(artifact.sha256)) return artifact;
    names.add(artifact.name);
    const key = artifactKey(artifact.name, artifact.sha256);
    const name = artifact.original?.status === 'unavailable' && artifact.original.reason === 'unavailable-after-erasure' ? artifact.name : `erased-artifact:${sha256(key)}`;
    replacements.set(key, name);
    return { name, sha256: artifact.sha256, size: artifact.size, original: { status: 'unavailable' as const, reason: 'unavailable-after-erasure' as const, policy } };
  });
  if (!replacements.size) return;
  next.erasure = { ...next.erasure!, originalPolicy: policy };
  const source = (record: SourceRecord): SourceRecord => {
    const name = replacements.get(artifactKey(record.location.artifact, record.artifactHash));
    return name ? { ...record, location: { artifact: name } } : record;
  };
  next.source.records = next.source.records.map(source);
  next.entries = next.entries.map(entry => ({ ...entry, source: source(entry.source) }));
  // Artifact-scoped or unscoped text may contain filenames, URLs or wrapper PII.
  const keep = (d: ImportPlan['diagnostics'][number]) => d.location ? !names.has(d.location.artifact) : !!d.kind && !!d.sourceId && !identities.has(sourceIdentity(plan.destination, plan.namespace, d.kind, d.sourceId));
  next.diagnostics = next.diagnostics.filter(keep);
  next.source.diagnostics = next.source.diagnostics.filter(keep);
}

export function withdrawnLocation(plan: ImportPlan, next: ImportPlan, location: SourceLocation, hash: string): SourceLocation {
  const index = plan.source.artifacts.findIndex(a => a.name === location.artifact && a.sha256 === hash);
  return index >= 0 && plan.source.artifacts[index].name !== next.source.artifacts[index].name ? { artifact: next.source.artifacts[index].name } : location;
}
