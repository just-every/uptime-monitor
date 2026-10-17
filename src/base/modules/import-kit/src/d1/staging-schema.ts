import { liveStreamOrigin, needsStreamOrigin } from './stream-origin.ts';
import { publicationFenceSchema } from './publication-fence.ts';

/** Additive private custody transport. Apply after the erasure and preserved
 * undo evolutions; no existing migration or public table is replaced. */
export const d1StagingSchema = [
  ...publicationFenceSchema,
  'CREATE TABLE IF NOT EXISTS ik_d1_staging (id TEXT PRIMARY KEY, generation TEXT NOT NULL, destination TEXT NOT NULL, namespace TEXT NOT NULL, mapping_version TEXT NOT NULL, principal_id TEXT NOT NULL, fingerprint TEXT NOT NULL, operation_key TEXT NOT NULL, total INTEGER NOT NULL, pages INTEGER NOT NULL, layout_hash TEXT NOT NULL, reversal_policy TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0, registered_revision INTEGER, sealed_revision INTEGER, UNIQUE(destination,principal_id,operation_key))',
  'CREATE TABLE IF NOT EXISTS ik_d1_staging_pages (stage_id TEXT NOT NULL REFERENCES ik_d1_staging(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, job_id TEXT NOT NULL UNIQUE, total INTEGER NOT NULL, fingerprint TEXT NOT NULL, plan_json TEXT NOT NULL, body_hash TEXT NOT NULL, body_size INTEGER NOT NULL, chunks INTEGER NOT NULL, identities INTEGER NOT NULL, index_hash TEXT NOT NULL, policy_hash TEXT NOT NULL, inline_plan INTEGER NOT NULL CHECK(inline_plan IN (0,1)), PRIMARY KEY(stage_id,ordinal))',
  'CREATE TABLE IF NOT EXISTS ik_d1_staging_chunks (stage_id TEXT NOT NULL REFERENCES ik_d1_staging(id) ON DELETE CASCADE, page_ordinal INTEGER NOT NULL, ordinal INTEGER NOT NULL, body TEXT NOT NULL, body_hash TEXT NOT NULL, body_size INTEGER NOT NULL, PRIMARY KEY(stage_id,page_ordinal,ordinal))',
  'CREATE TABLE IF NOT EXISTS ik_d1_staging_index (stage_id TEXT NOT NULL REFERENCES ik_d1_staging(id) ON DELETE CASCADE, page_ordinal INTEGER NOT NULL, ordinal INTEGER NOT NULL, source_identity TEXT NOT NULL, target_identity TEXT NOT NULL, PRIMARY KEY(stage_id,page_ordinal,ordinal,target_identity))',
  'CREATE INDEX IF NOT EXISTS ik_d1_staging_source ON ik_d1_staging_index(source_identity,stage_id)',
  'CREATE INDEX IF NOT EXISTS ik_d1_staging_target ON ik_d1_staging_index(target_identity,stage_id)',
  'CREATE TABLE IF NOT EXISTS ik_d1_staging_originals (stage_id TEXT NOT NULL REFERENCES ik_d1_staging(id) ON DELETE CASCADE, original_key TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY(stage_id,original_key))',
  "CREATE TRIGGER IF NOT EXISTS ik_d1_staging_binding BEFORE UPDATE OF id,generation,destination,namespace,mapping_version,principal_id,fingerprint,operation_key,total,pages,layout_hash,reversal_policy ON ik_d1_staging BEGIN SELECT RAISE(ABORT,'IMPORT_STAGE_BINDING_CHANGED'); END",
  ...['pages', 'index', 'chunks', 'originals'].flatMap(table => ['INSERT', 'UPDATE', 'DELETE'].map(event => {
    const refs = event === 'INSERT' ? 'id=NEW.stage_id' : event === 'DELETE' ? 'id=OLD.stage_id' : 'id=OLD.stage_id OR id=NEW.stage_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_staging_${table}_${event.toLowerCase()} AFTER ${event} ON ik_d1_staging_${table} BEGIN UPDATE ik_d1_staging SET revision=revision+1,sealed_revision=NULL${['pages','index'].includes(table) ? ',registered_revision=NULL' : ''} WHERE ${refs}; END`;
  })),
  // Invalidating the complete privacy index also discards private values.
  // An interrupted generation can never hide payload by losing its index.
  ...['INSERT', 'UPDATE', 'DELETE'].map(event => {
    const refs = event === 'INSERT' ? 'stage_id=NEW.stage_id' : event === 'DELETE' ? 'stage_id=OLD.stage_id' : 'stage_id=OLD.stage_id OR stage_id=NEW.stage_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_staging_index_custody_${event.toLowerCase()} AFTER ${event} ON ik_d1_staging_index WHEN EXISTS(SELECT 1 FROM ik_d1_staging_chunks WHERE ${refs}) OR EXISTS(SELECT 1 FROM ik_d1_staging_originals WHERE ${refs}) BEGIN DELETE FROM ik_d1_staging_chunks WHERE ${refs}; DELETE FROM ik_d1_staging_originals WHERE ${refs}; END`;
  }),
  ...['chunks', 'originals'].map(table => `CREATE TRIGGER IF NOT EXISTS ik_d1_staging_${table}_registered BEFORE INSERT ON ik_d1_staging_${table} WHEN NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=NEW.stage_id AND registered_revision IS NOT NULL) BEGIN SELECT RAISE(ABORT,'IMPORT_STAGE_INDEX_REQUIRED'); END`),
];

/** Earlier streams shared version-three custody with ordinary jobs. Convert
 * only actual page membership; durable origins reject known orphan custody.
 * A legacy job whose structural evidence was already lost cannot be classified
 * from its ID, operation key, payload or nullable reversal-policy binding. */
export const d1LegacyStreamReceiptUpgrade = [
  `INSERT INTO ik_d1_assertions(ok) SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ik_jobs legacy WHERE ${needsStreamOrigin('legacy.id')} AND NOT ${liveStreamOrigin('legacy.id')}) THEN 1 ELSE 0 END`,
  'UPDATE ik_original_sets SET version=4 WHERE version=3 AND job_id IN (SELECT job_id FROM ik_d1_stream_pages)',
  'DELETE FROM ik_d1_assertions',
];
