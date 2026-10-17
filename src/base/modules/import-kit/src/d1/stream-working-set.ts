import type { ImportPlan } from '../types.ts';
import { canonicalSize } from '../provenance/canonical-stream.ts';
import { planFingerprint } from '../plan/index.ts';
import { dictionaryWeight } from './stream-partition.ts';

/** Long repeated values can make independent small dictionaries dominate total
 * custody. Coalesce only those normalized pages, within a fixed working set.
 * Ordinary imports retain their expanded-byte and 100-record boundaries. */
export function measurePlanWorkingSet(plan: ImportPlan): { bytes: number; expandedBytes: number; amplified: boolean } {
  const expandedBytes=canonicalSize(plan),bytes=dictionaryWeight(plan);
  return {bytes,expandedBytes,amplified:expandedBytes>4*bytes};
}
export function amplifiedPlan(plan: ImportPlan): boolean {
  return measurePlanWorkingSet(plan).amplified;
}
export function mergePlanPages(pages: ImportPlan[]): ImportPlan {
  if (pages.length === 1) return pages[0];
  const first = pages[0], entries = pages.flatMap(page => page.entries);
  const counts = { input: 0, unique: 0, duplicates: 0, mapped: 0, unmapped: 0, rejected: 0, effects: 0, byKind: Object.create(null) as Record<string, number> };
  const diagnostics = [...first.diagnostics];
  const artifactWarnings = first.source.artifacts.filter(artifact => artifact.original?.status === 'unavailable').length;
  for (const [index, page] of pages.entries()) {
    for (const key of ['input','unique','duplicates','mapped','unmapped','rejected','effects'] as const) counts[key] += page.counts[key];
    for (const [kind, count] of Object.entries(page.counts.byKind)) counts.byKind[kind] = (counts.byKind[kind] ?? 0) + count;
    if (index) diagnostics.push(...page.diagnostics.slice(artifactWarnings));
  }
  const plan = { ...first, counts, diagnostics, entries, source: { ...first.source, records: entries.map(entry => entry.source) } };
  plan.fingerprint = planFingerprint(plan);
  return plan;
}
