import type { D1FieldMatch, D1FieldRedaction, D1ImportContext, D1RelatedErasure, D1Sql } from './types.ts';
import { authorize, guard, sql } from './database.ts';
import { loadPlans } from './plans.ts';
import { readOriginals } from './originals.ts';
import { exactRows, jsonChunks } from './sets.ts';
import { fieldMatches, fieldRedactions, inRange, redactFields, type EntryRange } from './field-redaction.ts';
import { requestContext } from './budget.ts';
import { redactedNativeFields } from './redacted-custody.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';

export interface RelatedSelection { identities: Set<string>; originals: Map<string, Set<string>>; guards: D1Sql[]; discoveryGuards?: D1Sql[]; redactedJobs: Set<string>; fieldMatches: D1FieldMatch[] }

/** Visit every retained plan and original in the destination once. The exact job
 * set and fingerprints are fenced, so a job created or changed after this scan
 * aborts the erasure instead of keeping an unscanned copy. */
/** Which jobs a bounded step reads. `clean` jobs were already read in an earlier
 * step and are fenced by their unchanged custody revision instead; `complete`
 * also proves the destination holds no other job. `range` limits a step over one
 * job to a page of its entries: only they are counted and redacted. */
export interface JobSelection { jobs: string[]; clean: Map<string, number>; complete: boolean; range?: EntryRange }

export async function relatedSelection(context: D1ImportContext, related: D1RelatedErasure, selection?: JobSelection, ownStage?: string): Promise<RelatedSelection> {
  const db = context.storage.database;
  // Only compact headers span pages. Read the revision before any plan,
  // original or before-image: it fences all copies observed by this scan.
  const rows: { id: string; fingerprint: string; state_revision: number }[] = [];
  for (const selected of selection ? jsonChunks(selection.jobs) : [undefined]) {
    let cursor: string | undefined;
    while (true) {
      const scope = selected === undefined ? '' : ' AND j.id IN (SELECT value FROM json_each(?))';
      const after = cursor === undefined ? '' : ' AND j.id>?';
      const params = [context.destination, ...selected === undefined ? [] : [selected], ...cursor === undefined ? [] : [cursor]];
      const { results } = await db.prepare(`SELECT j.id,j.fingerprint,s.revision AS state_revision FROM ik_jobs j LEFT JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE j.destination=?${scope}${after} ORDER BY j.id LIMIT 32`).bind(...params).all<typeof rows[number]>();
      for (const row of results) {
        if (!Number.isSafeInteger(row.state_revision)) throw new Error('IMPORT_CUSTODY_STATE_REQUIRED');
        rows.push(row);
      }
      if (results.length < 32) break;
      cursor = results.at(-1)!.id;
    }
  }
  rows.sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  const identities = new Set<string>(), originals = new Map<string, Set<string>>(), redactedJobs = new Set<string>();
  const fields = related.fields ?? [], counts = fieldMatches(fields);
  for (const row of rows) {
    const raw = await db.prepare('SELECT * FROM ik_jobs WHERE id=? AND destination=?').bind(row.id, context.destination).first();
    if (!raw) throw new Error('IMPORT_JOB_NOT_FOUND');
    if (raw.fingerprint !== row.fingerprint) throw new Error('IMPORT_CUSTODY_STATE_CHANGED');
    const plan = (await loadPlans([raw], db, context.storage.targets.planCodec)).get(row.id)!;
    if (related.source) for (const entry of plan.entries) if (!entry.erased && related.source(entry.source)) identities.add(entry.identity);
    if (fields.length) {
      for (const [ordinal, entry] of plan.entries.entries()) {
        if (!inRange(selection?.range, ordinal)) continue;
        if (redactFields(entry.source.payload, fields, counts) !== entry.source.payload) redactedJobs.add(row.id);
        for (const effect of entry.effects) if (redactFields(effect.value, fields, counts) !== effect.value) redactedJobs.add(row.id);
      }
    }
    if (fields.length || related.snapshot !== undefined) {
      // Three near-limit records fit a bounded read. Release their decoded
      // values immediately; the monotonic job revision carries the fence.
      for (const table of ['ik_provenance', 'ik_d1_history']) {
        let cursor: string | undefined;
        while (true) {
          const after = cursor === undefined ? '' : ' AND id>?';
          const { results } = await db.prepare(`SELECT id,ordinal,record_json FROM ${table} WHERE job_id=?${after} ORDER BY id LIMIT 3`).bind(row.id, ...cursor === undefined ? [] : [cursor]).all();
          for (const beforeImage of results) {
            if (!inRange(selection?.range, beforeImage.ordinal)) continue;
            const before = JSON.parse(beforeImage.record_json).before;
            if (before && redactFields(before.value, fields, counts) !== before.value) redactedJobs.add(row.id);
          }
          if (results.length < 3) break;
          cursor = results.at(-1)!.id;
        }
      }
    }
    if (!related.original) continue;
    for (const original of await readOriginals(plan, row.id, context)) {
      if (original.kind !== 'artifact' || original.original.status !== 'retained') continue;
      if (!related.original(Buffer.from(original.original.base64, 'base64'), { name: original.name, sha256: original.sha256 })) continue;
      if (!originals.has(row.id)) originals.set(row.id, new Set());
      originals.get(row.id)!.add(original.sha256);
    }
  }
  const jobs = rows.map(row => ({ id: row.id, fingerprint: row.fingerprint }));
  const guards = selection ? exactRows('ik_jobs', ['id', 'fingerprint'], jobs) : exactRows('ik_jobs', ['id', 'fingerprint'], jobs, sql('destination=?', context.destination));
  guards.push(...exactRows('ik_d1_custody_states', ['job_id', 'revision'], rows.map(row => ({ job_id: row.id, revision: row.state_revision }))));
  if (selection?.complete) {
    guards.push(guard(sql('(SELECT count(*) FROM ik_jobs WHERE destination=?)=?', context.destination, rows.length + selection.clean.size)));
    guards.push(...exactRows('ik_d1_custody_states', ['job_id', 'revision'], [...selection.clean].map(([job_id, revision]) => ({ job_id, revision }))));
  }
  let discoveryGuards: D1Sql[] | undefined;
  if (fields.length || related.snapshot !== undefined) {
    // Incomplete private plans cannot be safely field-rewritten. Cancel every
    // private preparation in this destination atomically, including other actors.
    discoveryGuards = [...guards];
    for (const table of ['ik_d1_staging', 'ik_d1_native_staging', 'ik_d1_preparation_steps']) {
      const exclude = ownStage && table === 'ik_d1_native_staging' ? ' AND id<>?' : '';
      const params = exclude ? [context.destination, ownStage] : [context.destination];
      const { results } = await db.prepare(`SELECT id FROM ${table} WHERE destination=?${exclude} ORDER BY id`).bind(...params).all();
      guards.push(...exactRows(table, ['id'], results, sql(`destination=?${exclude}`, ...params)));
      // Discovery predates our preparation. Keep its public snapshot stable
      // while publication permits precisely the stage this erasure creates.
      discoveryGuards.push(...exactRows(table, ['id'], results, sql('destination=?', context.destination)));
    }
  }
  return { identities, originals, guards, discoveryGuards, redactedJobs, fieldMatches: counts };
}

/** Read-only selector discovery for products with optional references. Erasure
 * scans again and refuses an explicit path that no longer matches. */
export async function readD1ImportFieldMatches(fields: readonly D1FieldRedaction[], input: D1ImportContext): Promise<{ fieldMatches: D1FieldMatch[]; snapshot: string }> {
  fields = fieldRedactions(fields);
  const context = requestContext(input);
  await authorize(context, 'read');
  const selection = await relatedSelection(context, { fields, snapshot: '' });
  const native = await redactedNativeFields('', '', context, fields, selection.fieldMatches);
  await authorize(context, 'read');
  return { fieldMatches: selection.fieldMatches, snapshot: fieldSelectionSnapshot([...selection.guards, ...native.before]) };
}

/** Bind discovery to exact custody/native predicates without returning their values. */
export function fieldSelectionSnapshot(guards: readonly D1Sql[]): string {
  return sha256(canonicalJson(guards.map(({ sql, params }) => ({ sql, params: params ?? [] }))));
}
