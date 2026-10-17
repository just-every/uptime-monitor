import { randomUUID } from 'node:crypto';
import type { D1CustodyRetirement, D1ErasureJournalEntry, D1ImportContext, D1ImportPerson, D1RetirementOptions, D1RetirementProgress, D1Sql } from './types.ts';
import { requestContext, packGuards, remaining, D1RequestLimitError } from './budget.ts';
import { authority, authorize, finalAuthority, readonlyChecks, prepare, readAtomic, sql } from './database.ts';
import { requireText } from '../provenance/identity.ts';
import { targetErasure, type ErasureStep } from './erasure.ts';
import { destinationRevisions, readProgress, type JobRevision, type RedactionProgress } from './retirement-steps.ts';
import { overflow, redactionSteps } from './redaction-step.ts';
import { headerAbsence, retirementScope, retirementWrites, scopeGuards } from './retirement-scope.ts';
import { tallyBefore, tallyJournal, tallyStores } from './retirement-tally.ts';
import { fieldRedactions } from './field-redaction.ts';
import { retireD1ImportCustodyStep } from './retirement-bounded.ts';
import type { D1ErasurePending, D1ErasureStepOptions } from './erasure-progress.ts';

/** Erase a person from import custody and journal it, in one D1 batch with the
 * product's native statements.
 *
 * Their records (by exact native target) become zero-effect tombstones in every
 * job, so undo and resume of everyone else's records keep working. A job or
 * stream whose header holds one of the person's exact values cannot be renamed
 * without breaking its source identities, so its whole custody is retired: no
 * undo or resume, a plain retirement row naming the journal receipt, and the
 * native rows it already wrote stay. Retirement closes over the streams, reused
 * sources and preserved history that depend on a retired job. The journal row
 * holds the receipt id, row counts per store and the time, never a value.
 *
 * `checkpoint` is the product's restore fence for rewritten rows (portability's
 * redaction checkpoint). It commits in the batch whenever the batch rewrites or
 * retires custody, so an archive older than the erasure is refused whole once
 * the later journal is supplied; a batch without it proves no custody row changed. */
export function retireD1ImportCustody(person: D1ImportPerson, nativeStatements: D1Sql[], input: D1ImportContext, checkpoint: D1Sql[]): Promise<D1CustodyRetirement>;
export function retireD1ImportCustody(person: D1ImportPerson, nativeStatements: D1Sql[], input: D1ImportContext, checkpoint: D1Sql[], options: D1ErasureStepOptions): Promise<D1CustodyRetirement | D1ErasurePending>;
export function retireD1ImportCustody(person: D1ImportPerson, nativeStatements: D1Sql[], input: D1ImportContext, checkpoint: D1Sql[], options: D1RetirementOptions): Promise<D1CustodyRetirement | D1RetirementProgress>;
export async function retireD1ImportCustody(person: D1ImportPerson, nativeStatements: D1Sql[], input: D1ImportContext, checkpoint: D1Sql[], options: D1RetirementOptions | D1ErasureStepOptions = {}): Promise<D1CustodyRetirement | D1RetirementProgress | D1ErasurePending> {
  if ('operationKey' in options) return retireD1ImportCustodyStep(person, nativeStatements, input, checkpoint, options);
  person = { ...person, related: person.related ? { ...person.related, fields: fieldRedactions(person.related.fields) } : undefined };
  const context = requestContext({ ...input, originalErasurePolicy: input.originalErasurePolicy ?? 'withdraw-affected-artifacts' });
  if (context.maxBatchBytes !== undefined) throw new Error('IMPORT_RETIREMENT_OPERATION_KEY_REQUIRED');
  const values = [...new Set(person.values ?? [])].sort();
  for (const value of values) requireText(value, 'person value');
  if (person.target) { requireText(person.target.kind, 'target kind'); requireText(person.target.id, 'target id'); }
  const fields = person.related?.fields ?? [];
  if (!person.target && !values.length && !fields.length) throw new Error('IMPORT_RETIREMENT_PERSON_REQUIRED');
  // Without a native target only field redaction applies: whole-record erasure
  // (`source`, `original`) binds what it severs to the erased target.
  if (person.related && !person.target && (person.related.source || person.related.original || !fields.length)) throw new Error('IMPORT_RETIREMENT_TARGET_REQUIRED');
  await readAtomic(context, [], undefined, 'erase');
  const db = context.storage.database, destination = context.destination;
  const scope = await retirementScope(db, destination, values), retiring = new Set(scope.jobs);
  const fenced = Boolean(person.target) || fields.length > 0 || scope.jobs.length + scope.streams.length + scope.stages.length + scope.anchors.length + scope.preparations.length > 0;
  if (fenced && !checkpoint.length) throw new Error('IMPORT_RETIREMENT_CHECKPOINT_REQUIRED');
  const fence = fenced ? checkpoint : [];
  // Only the checkpoint fences rows the tally does not count (another import's jobs, a seed's tables): unfenced caller SQL is refused.
  if (nativeStatements.length && !fence.length) throw new Error('IMPORT_RETIREMENT_NATIVE_REQUIRES_CHECKPOINT');
  let step: ErasureStep | undefined, progress: RedactionProgress | undefined, pending: JobRevision[] = [];
  if (options.resumable && fields.length) {
    progress = readProgress(options.continuation, destination, fields);
    const jobs = await destinationRevisions(db, destination);
    const clean = new Map(jobs.filter(job => job.revision !== null && progress!.clean.get(job.id) === job.revision).map(job => [job.id, job.revision!]));
    pending = jobs.filter(job => !clean.has(job.id));
    progress = { ...progress, clean };
    if (progress.page || pending.length > progress.span) {
      if (person.related?.snapshot !== undefined) throw new Error('IMPORT_REDACTION_SNAPSHOT_UNBOUNDED_ONLY');
      return redactionSteps(fields, context, fence, progress, pending);
    }
    step = { selection: { jobs: pending.map(job => job.id), clean, complete: true }, matched: progress.matched };
  }
  const start = remaining(context);
  let erasure: Awaited<ReturnType<typeof targetErasure>>, receiptId: string, prefix: D1Sql[], suffix: D1Sql[];
  try {
    erasure = person.target || fields.length ? await targetErasure(person.target, context, person.related, retiring, step) : { before: [], after: [], final: [], decisions: [], jobs: [], fieldMatches: [] };
    receiptId = randomUUID(); const recordedAt = new Date().toISOString();
    const touched = { destination, jobs: [...new Set([...erasure.jobs, ...scope.jobs])].sort(), streams: [] as string[] };
    touched.streams = [...new Set([...scope.anchors, ...await pageStreams(db, touched.jobs)])].sort();
    const stores = await tallyStores(db);
    const entry = { recordedAt, recordsErased: erasure.decisions.length, jobsRetired: scope.jobs.length, streamsRetired: scope.anchors.length };
    prefix = packGuards([authority(context, 'erase'), ...tallyBefore(stores, receiptId, touched), ...erasure.before, ...scopeGuards(scope)]);
    suffix = [...packGuards([...erasure.after, ...retirementWrites(scope, destination, receiptId, recordedAt), ...headerAbsence(destination, values),
      ...tallyJournal(stores, receiptId, touched, entry, fenced), authority(context, 'erase'), sql('DELETE FROM ik_d1_assertions')]), ...readonlyChecks(erasure.final), finalAuthority(context, 'erase')];
    // One read after the batch returns its journal row.
    const needed = prefix.length + nativeStatements.length + fence.length + suffix.length + 1;
    if (needed > remaining(context)) throw overflow(needed);
  } catch (error) {
    // A redaction too large for one request goes on in steps; nothing was written.
    if (!step || !pending.length || person.related?.snapshot !== undefined || !(error instanceof D1RequestLimitError)) throw error;
    const needed = (error as { needed?: number }).needed;
    return redactionSteps(fields, context, fence, progress!, pending, { cost: needed === undefined ? undefined : start - remaining(context) + needed });
  }
  const results = await db.batch([...prefix, ...nativeStatements, ...fence, ...suffix].map(statement => prepare(db, statement)));
  const [journal] = await readJournal(db, destination, receiptId);
  return { ...journal, phase: 'complete', fieldMatches: erasure.fieldMatches, retiredJobs: scope.jobs, retiredStreams: scope.anchors, nativeResults: results.slice(prefix.length, prefix.length + nativeStatements.length) };
}

const pageStreams = async (db: D1ImportContext['storage']['database'], jobs: string[]) =>
  (await db.prepare('SELECT DISTINCT stream_id AS id FROM ik_d1_stream_pages WHERE job_id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(jobs)).all<{ id: string }>()).results.map(row => row.id);

/** The destination's erasure journal, oldest first. Seeds show it, export it in
 * archives and use it to explain a retired job or stream. */
export async function readD1ErasureJournal(input: D1ImportContext): Promise<D1ErasureJournalEntry[]> {
  const context = requestContext(input);
  await authorize(context, 'read');
  return readJournal(context.storage.database, context.destination);
}

async function readJournal(db: D1ImportContext['storage']['database'], destination: string, receiptId?: string): Promise<D1ErasureJournalEntry[]> {
  const { results } = await db.prepare(`SELECT * FROM ik_d1_erasure_journal WHERE destination=?${receiptId ? ' AND id=?' : ''} ORDER BY recorded_at, id`).bind(...[destination, receiptId].filter(value => value !== undefined)).all<any>();
  return results.map(row => ({ receiptId: row.id, destination: row.destination, recordedAt: row.recorded_at, recordsErased: row.records_erased, jobsRetired: row.jobs_retired, streamsRetired: row.streams_retired, stores: JSON.parse(row.stores_json) }));
}

/** A job or stream that is gone because a person's erasure retired it. */
export async function retiredError(db: D1ImportContext['storage']['database'], id: string, kind: 'job' | 'stream'): Promise<Error | undefined> {
  const row = await db.prepare('SELECT receipt_id FROM ik_d1_retirements WHERE id=? AND kind=?').bind(id, kind).first<{ receipt_id: string }>();
  return row ? Object.assign(new Error(kind === 'job' ? 'IMPORT_JOB_RETIRED' : 'IMPORT_STREAM_RETIRED'), { receiptId: row.receipt_id }) : undefined;
}
