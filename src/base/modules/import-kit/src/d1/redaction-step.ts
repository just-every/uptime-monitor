import { randomUUID } from 'node:crypto';
import type { D1FieldRedaction, D1ImportContext, D1RetirementProgress, D1Sql } from './types.ts';
import { packGuards, remaining, D1RequestLimitError } from './budget.ts';
import { authority, finalAuthority, readonlyChecks, prepare, sql } from './database.ts';
import { targetErasure } from './erasure.ts';
import { exactRows } from './sets.ts';
import type { EntryRange } from './field-redaction.ts';
import { addMatches, revisionsAfter, STEP_JOBS, writeProgress, type JobRevision, type RedactionPage, type RedactionProgress } from './retirement-steps.ts';
import { tallyJournal, tallyBefore, tallyStores } from './retirement-tally.ts';

/** Statements one request may run (D1's per-invocation limit, as budget.ts counts it). */
const BUDGET = 1000;

/** A step that did not fit its request: the statements it read plus those its
 * batch needed, or undefined when its reads alone ran out of the request. */
export interface Overflow { cost?: number }
/** The statements a batch needed that the request no longer had. Nothing was written. */
export const overflow = (needed: number): D1RequestLimitError => Object.assign(new D1RequestLimitError(), { needed });

/** Resumable redaction in bounded steps. A step takes as many pending jobs as fit
 * one request (at least one, at most STEP_JOBS); a job that does not fit alone is
 * redacted in pages of its entries. A step that does not fit writes nothing: the
 * next is sized from what it would have cost and tried in the same request while
 * that request still has room, otherwise in the next one, so every committed
 * step stays inside the limit and the erasure reaches its final step. */
export async function redactionSteps(fields: readonly D1FieldRedaction[], context: D1ImportContext, fence: D1Sql[], progress: RedactionProgress, pending: JobRevision[], failed?: Overflow): Promise<D1RetirementProgress> {
  let { span, page } = progress, unsized = failed;
  for (;;) {
    let job = page && pending.find(item => item.id === page!.job);
    // A job changed since its last page starts again from its first entry; one no longer pending is done.
    if (page && (!job || job.revision !== page.revision)) page = job ? { ...page, from: 0, revision: job.revision } : undefined;
    if (unsized) {
      const next = smaller(span, page, pending, unsized.cost);
      ({ span, page } = next); job = page && pending.find(item => item.id === page!.job);
      // Try it now only while this request has room for it; otherwise commit nothing and let the next request take it.
      if (unsized.cost === undefined || unsized.cost * next.fraction > remaining(context)) {
        return { phase: 'redacting', continuation: writeProgress({ ...progress, span, page }), jobsRedacted: 0, jobsPending: pending.length, fieldMatches: progress.matched };
      }
    }
    const jobs = job ? [job] : pending.slice(0, span), range = page && job ? { from: page.from, to: Math.min(page.from + page.count, job.total) } : undefined;
    const start = remaining(context);
    let done: Awaited<ReturnType<typeof redactionStep>>;
    try { done = await redactionStep(fields, context, fence, progress, jobs, range); }
    catch (error) {
      if (!(error instanceof D1RequestLimitError)) throw error;
      const needed = (error as { needed?: number }).needed;
      unsized = { cost: needed === undefined ? undefined : start - remaining(context) + needed };
      continue;
    }
    // A step that used under half a request lets the next one take twice as much.
    const grow = (start - remaining(context)) * 2 <= BUDGET;
    let next: RedactionPage | undefined, finished = jobs.length;
    if (page && job && range) {
      const revision = done.revisions.get(job.id) ?? null;
      if (range.to < job.total) { next = { job: job.id, from: range.to, count: grow ? page.count * 2 : page.count, revision }; finished = 0; }
      else { if (revision !== null) done.clean.set(job.id, revision); span = STEP_JOBS; }
    } else if (grow) span = Math.min(STEP_JOBS, span * 2);
    return { phase: 'redacting', continuation: writeProgress({ ...progress, clean: done.clean, matched: done.matched, span, page: next }),
      jobsRedacted: done.redacted, jobsPending: pending.length - finished, fieldMatches: done.matched, ...done.receiptId ? { receiptId: done.receiptId } : {} };
  }
}

/** The next, strictly smaller step after one cost `cost` statements: in proportion,
 * with headroom, or half when its reads alone ran out. Fewer jobs first, then
 * pages of one job's entries; `fraction` is its expected share of that cost. */
function smaller(span: number, page: RedactionPage | undefined, pending: JobRevision[], cost: number | undefined): { span: number; page?: RedactionPage; fraction: number } {
  const size = (current: number) => Math.min(current - 1, cost === undefined ? Math.floor(current / 2) : Math.floor(current * BUDGET * 0.7 / cost));
  if (page) {
    const count = size(page.count);
    if (count < 1) throw new D1RequestLimitError(); // One entry alone is more than a request.
    return { span, page: { ...page, count }, fraction: count / page.count };
  }
  const jobs = Math.min(span, pending.length);
  if (jobs > 1) { const fewer = Math.max(1, size(jobs)); return { span: fewer, fraction: fewer / jobs }; }
  const [job] = pending, count = size(job.total);
  if (count < 1) throw new D1RequestLimitError();
  return { span: 1, page: { job: job.id, from: 0, count, revision: job.revision }, fraction: count / job.total };
}

/** One bounded step of a resumable redaction: read `jobs`' custody (with `range`,
 * one job's page of entries), and in one batch with the restore checkpoint and a
 * journal row rewrite the jobs holding a selected value together with the native
 * rows they own. Jobs found free of the values are fenced at the revision read
 * before their plans. The caller's native statements wait for the final step,
 * which also proves no other row holds one. */
async function redactionStep(fields: readonly D1FieldRedaction[], context: D1ImportContext, fence: D1Sql[], progress: RedactionProgress, jobs: JobRevision[], range?: EntryRange) {
  const db = context.storage.database, destination = context.destination;
  const erasure = await targetErasure(undefined, context, { fields }, new Set(), { selection: { jobs: jobs.map(job => job.id), clean: new Map(), complete: false, ...range ? { range } : {} }, owned: true });
  const holding = new Set(erasure.jobs), free = jobs.filter(job => !holding.has(job.id) && job.revision !== null);
  const clean = new Map(progress.clean), matched = addMatches(progress.matched, erasure.fieldMatches), revisions = new Map(jobs.map(job => [job.id, job.revision]));
  // A page leaves its job pending until its last page.
  if (!range) for (const job of free) clean.set(job.id, job.revision!);
  let receiptId: string | undefined;
  if (holding.size) {
    receiptId = randomUUID();
    const recordedAt = new Date().toISOString(), touched = { destination, jobs: [...holding].sort(), streams: [] as string[] };
    touched.streams = (await db.prepare('SELECT DISTINCT stream_id AS id FROM ik_d1_stream_pages WHERE job_id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(touched.jobs)).all<{ id: string }>()).results.map(row => row.id).sort();
    const stores = await tallyStores(db);
    const prefix = packGuards([authority(context, 'erase'), ...tallyBefore(stores, receiptId, touched), ...erasure.before,
      ...exactRows('ik_d1_custody_states', ['job_id', 'revision'], free.map(job => ({ job_id: job.id, revision: job.revision })))]);
    const suffix = [...packGuards([...erasure.after, ...tallyJournal(stores, receiptId, touched, { recordedAt, recordsErased: 0, jobsRetired: 0, streamsRetired: 0 }, true),
      authority(context, 'erase'), sql('DELETE FROM ik_d1_assertions')]), ...readonlyChecks(erasure.final), revisionsAfter(touched.jobs), finalAuthority(context, 'erase')];
    const needed = prefix.length + fence.length + suffix.length;
    if (needed > remaining(context)) throw overflow(needed);
    const results = await db.batch([...prefix, ...fence, ...suffix].map(statement => prepare(db, statement)));
    for (const row of (results[results.length - 2] as { results: { job_id: string; revision: number }[] }).results) {
      revisions.set(row.job_id, row.revision);
      if (!range) clean.set(row.job_id, row.revision);
    }
  }
  return { clean, matched, revisions, redacted: holding.size, receiptId };
}
