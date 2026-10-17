import { streamParentFields } from './stream-revision-schema.ts';

const anchored = streamParentFields.filter(field => field !== 'namespace' && field !== 'operation_key');

/** SQLite's REPLACE removes a conflicting row without firing DELETE triggers, so
 * an insert over an existing key is refused before conflict resolution. A store
 * that applied an earlier d1RetirementSchema applies these as its next migration. */
export const d1RetirementAppendOnly = [
  "CREATE TRIGGER IF NOT EXISTS ik_d1_erasure_journal_insert BEFORE INSERT ON ik_d1_erasure_journal WHEN EXISTS(SELECT 1 FROM ik_d1_erasure_journal WHERE id=NEW.id) BEGIN SELECT RAISE(ABORT,'IMPORT_ERASURE_JOURNAL_APPEND_ONLY'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_retirement_insert BEFORE INSERT ON ik_d1_retirements WHEN EXISTS(SELECT 1 FROM ik_d1_retirements WHERE id=NEW.id AND kind=NEW.kind) BEGIN SELECT RAISE(ABORT,'IMPORT_RETIREMENT_CHANGED'); END",
];

/** Additive upgrade for person erasure from import custody, applied after every
 * other D1 schema. Existing applied migrations stay byte for byte.
 *
 * - `ik_d1_retirements`: jobs and streams whose whole custody was retired, with
 *   the journal receipt that did it. No header, namespace or operation key.
 *   Append-only like the journal.
 * - `ik_d1_erasure_journal`: append-only (no update, delete or insert over an
 *   existing id), one row per erasure: receipt id, the stores touched with
 *   their row counts, and the time. It holds no erased value.
 * - `ik_d1_erasure_tally`: transaction-local row images used to count the
 *   journal's rows; every erasure batch empties it before it commits.
 * - The stream revision anchor keeps its monotonic counters and layout fields,
 *   and may drop its namespace and operation key (to '') once its parent stream
 *   is gone and the stream is recorded as retired. Nothing else may change. */
export const d1RetirementSchema = [
  "CREATE TABLE IF NOT EXISTS ik_d1_retirements (id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('job','stream')), destination TEXT NOT NULL, receipt_id TEXT NOT NULL, retired_at TEXT NOT NULL, PRIMARY KEY(id, kind))",
  "CREATE TABLE IF NOT EXISTS ik_d1_erasure_journal (id TEXT PRIMARY KEY, destination TEXT NOT NULL, recorded_at TEXT NOT NULL, records_erased INTEGER NOT NULL CHECK(records_erased>=0), jobs_retired INTEGER NOT NULL CHECK(jobs_retired>=0), streams_retired INTEGER NOT NULL CHECK(streams_retired>=0), stores_json TEXT NOT NULL CHECK(json_valid(stores_json)))",
  'CREATE INDEX IF NOT EXISTS ik_d1_erasure_journal_time ON ik_d1_erasure_journal(destination, recorded_at)',
  ...d1RetirementAppendOnly,
  "CREATE TRIGGER IF NOT EXISTS ik_d1_erasure_journal_update BEFORE UPDATE ON ik_d1_erasure_journal BEGIN SELECT RAISE(ABORT,'IMPORT_ERASURE_JOURNAL_APPEND_ONLY'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_erasure_journal_delete BEFORE DELETE ON ik_d1_erasure_journal BEGIN SELECT RAISE(ABORT,'IMPORT_ERASURE_JOURNAL_APPEND_ONLY'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_retirement_update BEFORE UPDATE ON ik_d1_retirements BEGIN SELECT RAISE(ABORT,'IMPORT_RETIREMENT_CHANGED'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_retirement_delete BEFORE DELETE ON ik_d1_retirements BEGIN SELECT RAISE(ABORT,'IMPORT_RETIREMENT_CHANGED'); END",
  'CREATE TABLE IF NOT EXISTS ik_d1_erasure_tally (receipt_id TEXT NOT NULL, store TEXT NOT NULL, image TEXT NOT NULL)',
  'DROP TRIGGER IF EXISTS ik_d1_stream_revision_update',
  `CREATE TRIGGER ik_d1_stream_revision_update BEFORE UPDATE ON ik_d1_stream_revisions WHEN NEW.stream_id IS NOT OLD.stream_id OR ${anchored.map(field => `NEW.${field} IS NOT OLD.${field}`).join(' OR ')} OR NEW.layout_revision<OLD.layout_revision OR NEW.progress_revision<=OLD.progress_revision OR ((NEW.namespace IS NOT OLD.namespace OR NEW.operation_key IS NOT OLD.operation_key) AND NOT (NEW.namespace='' AND NEW.operation_key='' AND NOT EXISTS(SELECT 1 FROM ik_d1_streams WHERE id=OLD.stream_id) AND EXISTS(SELECT 1 FROM ik_d1_retirements WHERE id=OLD.stream_id AND kind='stream'))) BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_REVISION_CHANGED'); END`,
];
