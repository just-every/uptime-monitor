import type { D1CustodyRetirement, D1ImportContext, D1ImportPerson, D1Sql } from './types.ts';
import { canonicalJson, requireText } from '../provenance/identity.ts';
import { fieldRedactions } from './field-redaction.ts';
import { reserveReturn } from './budget.ts';
import { guardPredicate, copyStatement } from './guards.ts';
import { sql } from './database.ts';
import { prepareErasureSelection } from './erasure-selection.ts';
import { stageD1ImportErasure } from './erasure-stage.ts';
import { erasureRequestContext, erasureStatementBinding, type D1ErasurePending, type D1ErasureStepOptions } from './erasure-progress.ts';
import { headerAbsence, retirementScope, retirementWrites, scopeGuards } from './retirement-scope.ts';
import { tallyBefore, tallyJournal, tallyStores } from './retirement-tally.ts';

/** Retire headers and erase records over bounded requests. Only the final
 * publication changes public custody, native rows and the restore checkpoint.
 * operationKey identifies that publication even when its acknowledgement is lost. */
export async function retireD1ImportCustodyStep(person: D1ImportPerson, nativeStatements: D1Sql[], input: D1ImportContext, checkpoint: D1Sql[], options: D1ErasureStepOptions): Promise<D1CustodyRetirement | D1ErasurePending> {
  person = { ...person, values: [...new Set(person.values ?? [])].sort(), related: person.related ? { ...person.related, fields: fieldRedactions(person.related.fields) } : undefined };
  for (const value of person.values!) requireText(value, 'person value');
  if (person.target) { requireText(person.target.kind, 'target kind'); requireText(person.target.id, 'target id'); }
  const fields = person.related?.fields ?? [];
  if (!person.target && !person.values!.length && !fields.length) throw new Error('IMPORT_RETIREMENT_PERSON_REQUIRED');
  if (person.related && !person.target && (person.related.source || person.related.original || !fields.length)) throw new Error('IMPORT_RETIREMENT_TARGET_REQUIRED');
  const context = erasureRequestContext({ ...input, originalErasurePolicy: input.originalErasurePolicy ?? 'withdraw-affected-artifacts' }, options);
  // Returning the completed receipt and retirement ids costs exactly two reads.
  const nested = reserveReturn(context, 2);
  const binding = canonicalJson(['retirement-v1', person.target ?? null, person.values, person.related ? { fields, snapshot: person.related.snapshot ?? null, source: person.related.source?.toString() ?? null, original: person.related.original?.toString() ?? null } : null, erasureStatementBinding(nativeStatements), erasureStatementBinding(checkpoint)]);
  const keys = nativeStatements.map(() => Symbol('native retirement result'));
  const native = nativeStatements.map((row, index) => Object.assign(copyStatement(row), { resultKey: keys[index] }));
  const result = await stageD1ImportErasure(nested, options, binding, async (id, bounded) => {
    const db = bounded.storage.database, destination = bounded.destination;
    const scope = await retirementScope(db, destination, person.values!), retiring = new Set(scope.jobs);
    const fenced = !!person.target || !!fields.length || Object.values(scope).some(ids => ids.length > 0);
    if (fenced && !checkpoint.length) throw new Error('IMPORT_RETIREMENT_CHECKPOINT_REQUIRED');
    if (native.length && !fenced) throw new Error('IMPORT_RETIREMENT_NATIVE_REQUIRES_CHECKPOINT');
    const selection = await prepareErasureSelection(person, native, bounded, id, retiring);
    const closure = scopeGuards(scope);
    const absence = headerAbsence(destination, person.values!);
    const retired = [sql('NOT EXISTS(SELECT 1 FROM ik_jobs WHERE id IN (SELECT value FROM json_each(?)))', JSON.stringify(scope.jobs)),
      sql('NOT EXISTS(SELECT 1 FROM ik_d1_streams WHERE id IN (SELECT value FROM json_each(?)))', JSON.stringify(scope.streams))];
    // The stage is transport, not erased custody. Its private before-images
    // would inflate the tally and retain values after a completed publication.
    const stores = (await tallyStores(db)).filter(store => !/^ik_d1_native_staging/.test(store.name));
    return { ...selection, resultKeys: keys, direct: !scope.jobs.length,
      snapshotGuards: [...selection.snapshotGuards, ...closure],
      survivingJobs: compiler => [...compiler.jobs.keys()].filter(job => !retiring.has(job)),
      final: [...selection.final, ...absence.map(row => guardPredicate(row)!), ...retired],
      publication: async (compiler, receipt) => {
        const jobs = [...compiler.jobs.keys()].sort();
        const streams = [...new Set([...scope.anchors, ...(await db.prepare('SELECT DISTINCT stream_id AS id FROM ik_d1_stream_pages WHERE job_id IN (SELECT value FROM json_each(?)) ORDER BY id').bind(JSON.stringify(jobs)).all<{ id: string }>()).results.map(row => row.id)])].sort();
        const touched = { destination, jobs, streams };
        return {
          before: [...closure, ...tallyBefore(stores, receipt.id, touched)],
          after: fenced ? checkpoint : [],
          tail: [...retirementWrites(scope, destination, receipt.id, receipt.recordedAt), ...absence,
            ...tallyJournal(stores, receipt.id, touched, { recordedAt: receipt.recordedAt, recordsErased: compiler.erased, jobsRetired: scope.jobs.length, streamsRetired: scope.anchors.length }, fenced)],
        };
      },
    };
  });
  if (result.phase === 'pending') return result;
  const db = context.storage.database;
  const journal = await db.prepare('SELECT * FROM ik_d1_erasure_journal WHERE id=? AND destination=?').bind(result.operationId, context.destination).first<any>();
  if (!journal) throw new Error('IMPORT_ERASURE_JOURNAL_MISSING');
  const retired = (await db.prepare('SELECT id,kind FROM ik_d1_retirements WHERE receipt_id=? AND destination=? ORDER BY id').bind(result.operationId, context.destination).all<{ id: string; kind: string }>()).results;
  return { phase: 'complete', receiptId: journal.id, destination: journal.destination, recordedAt: journal.recorded_at, recordsErased: journal.records_erased, jobsRetired: journal.jobs_retired, streamsRetired: journal.streams_retired, stores: JSON.parse(journal.stores_json), fieldMatches: result.fieldMatches, retiredJobs: retired.filter(row => row.kind === 'job').map(row => row.id), retiredStreams: retired.filter(row => row.kind === 'stream').map(row => row.id), nativeResults: result.nativeResults };
}
