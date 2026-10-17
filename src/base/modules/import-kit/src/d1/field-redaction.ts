import type { Json, ImportPlan, OriginalErasurePolicy } from '../types.ts';
import type { D1FieldRedaction, D1FieldMatch } from './types.ts';
import { requireText, canonicalJson, sha256, sourceIdentity } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
import { withdrawArtifacts } from './erased-artifacts.ts';

/** Snapshot the caller's selectors before any awaited reads. Never retain the
 * selected private values in the plan or journal. */
export function fieldRedactions(fields: readonly D1FieldRedaction[] = []): D1FieldRedaction[] {
  return fields.map(({ path, value, optional }) => {
    requireText(value, 'redaction value');
    if (!Array.isArray(path) || !path.length || path.length > 256 || path.some(segment =>
      typeof segment !== 'string' && !(Number.isSafeInteger(segment) && segment >= 0))) throw new Error('IMPORT_INVALID_REDACTION_PATH');
    if (optional !== undefined && typeof optional !== 'boolean') throw new Error('IMPORT_INVALID_REDACTION_PATH');
    return { path: [...path], value, ...optional ? { optional } : {} };
  });
}

/** A path matched nowhere is a typo unless every selector on it is optional. */
export function unmatchedPath(fields: readonly D1FieldRedaction[], counts: readonly D1FieldMatch[]): boolean {
  return counts.some(count => count.matches === 0 && fields.some(field => !field.optional && canonicalJson(field.path) === canonicalJson(count.path)));
}

export function fieldMatches(fields: readonly D1FieldRedaction[]): D1FieldMatch[] {
  return [...new Map(fields.map(field => [canonicalJson(field.path), { path: [...field.path], matches: 0 }])).values()];
}

const removed = Symbol('removed JSON field');
/** Copy only changed branches. Wildcards select arrays, never arbitrary object
 * properties; exact leaves prevent substring replacement in unrelated text. */
export function redactFields(value: Json, fields: readonly D1FieldRedaction[], counts?: D1FieldMatch[]): Json {
  const visit = (input: Json, selected: readonly D1FieldRedaction[], at: number): Json | typeof removed => {
    const matched = selected.filter(field => field.path.length === at && input === field.value);
    if (matched.length) {
      for (const count of counts ?? []) if (matched.some(field => canonicalJson(field.path) === canonicalJson(count.path))) count.matches++;
      return removed;
    }
    if (input === null || typeof input !== 'object') return input;
    const result: Json[] | Record<string, Json> = Array.isArray(input) ? [] : {};
    let changed = false;
    for (const [key, item] of Object.entries(input)) {
      const matching = selected.filter(field => field.path.length > at && (Array.isArray(input)
        ? field.path[at] === '*' || field.path[at] === Number(key) : field.path[at] === key));
      const next = matching.length ? visit(item, matching, at + 1) : item;
      changed ||= next !== item;
      if (next === removed) continue;
      if (Array.isArray(result)) result.push(next);
      else Object.defineProperty(result, key, { value: next, enumerable: true, writable: true, configurable: true });
    }
    return changed ? result : input;
  };
  return visit(value, fields, 0) as Json;
}

/** Entries `from` (inclusive) to `to` (exclusive) of one job's plan: one page
 * of a job too large to redact in one request. */
export interface EntryRange { from: number; to: number }
export const inRange = (range: EntryRange | undefined, ordinal: number): boolean => !range || ordinal >= range.from && ordinal < range.to;

/** With `range`, only those entries and the source records they collapse are
 * redacted; the rest of the plan stays as it is until a later page. */
export function redactPlanFields(plan: ImportPlan, fields: readonly D1FieldRedaction[], policy?: OriginalErasurePolicy, range?: EntryRange): { plan: ImportPlan; identities: Set<string> } {
  const identities = new Set<string>();
  const paged = range && new Set(plan.entries.slice(range.from, range.to).map(entry => entry.identity));
  const record = (source: ImportPlan['source']['records'][number]) => {
    if (paged && !paged.has(sourceIdentity(plan.destination, plan.namespace, source.kind, source.sourceId))) return source;
    const payload = redactFields(source.payload, fields);
    return payload === source.payload ? source : { ...source, payload, payloadHash: sha256(canonicalJson(payload)) };
  };
  const entries = plan.entries.map((entry, ordinal) => {
    if (!inRange(range, ordinal)) return entry;
    const source = record(entry.source);
    const effects = entry.effects.map(effect => {
      const value = redactFields(effect.value, fields);
      return value === effect.value ? effect : { ...effect, value };
    });
    if (source !== entry.source || effects.some((effect, index) => effect !== entry.effects[index])) identities.add(entry.identity);
    return { ...entry, source, effects };
  });
  const next = { ...plan, entries, source: { ...plan.source, records: plan.source.records.map(record) },
    erasure: plan.erasure ?? { reviewedFingerprint: plan.fingerprint } };
  if (identities.size && policy === undefined && plan.source.artifacts.some(a => a.original?.status === 'available')) throw new Error('IMPORT_ERASURE_RETAINED_ORIGINAL');
  withdrawArtifacts(plan, next, identities, policy);
  next.fingerprint = planFingerprint(next);
  return { plan: next, identities };
}
