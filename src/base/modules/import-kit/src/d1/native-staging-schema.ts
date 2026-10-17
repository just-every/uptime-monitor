import { publicationReceiptSchema } from './publication-receipt-schema.ts';
/** Additive, opt-in native transaction custody. No original byte APIs change. */
export const d1NativeStagingSchema = [
  'CREATE TABLE IF NOT EXISTS ik_d1_custody_revisions (job_id TEXT PRIMARY KEY REFERENCES ik_jobs(id) ON DELETE CASCADE,revision INTEGER NOT NULL DEFAULT 0,seal TEXT)',
  'INSERT OR IGNORE INTO ik_d1_custody_revisions(job_id) SELECT id FROM ik_jobs',
  ...publicationReceiptSchema,
  'CREATE TRIGGER IF NOT EXISTS ik_d1_custody_job_insert AFTER INSERT ON ik_jobs BEGIN INSERT INTO ik_d1_custody_revisions(job_id) VALUES(NEW.id); END',
  'CREATE TRIGGER IF NOT EXISTS ik_d1_custody_job_update AFTER UPDATE OF plan_json ON ik_jobs BEGIN UPDATE ik_d1_custody_revisions SET revision=revision+1 WHERE job_id=OLD.id OR job_id=NEW.id; END',
  ...['INSERT','UPDATE','DELETE'].map(event => {
    const refs = event === 'INSERT' ? 'job_id=NEW.job_id' : event === 'DELETE' ? 'job_id=OLD.job_id' : 'job_id=OLD.job_id OR job_id=NEW.job_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_custody_chunks_${event.toLowerCase()} AFTER ${event} ON ik_d1_plan_chunks BEGIN UPDATE ik_d1_custody_revisions SET revision=revision+1 WHERE ${refs}; END`;
  }),
  'CREATE TABLE IF NOT EXISTS ik_d1_native_staging (id TEXT PRIMARY KEY,generation TEXT NOT NULL,destination TEXT NOT NULL,namespace TEXT NOT NULL,mapping_version TEXT NOT NULL,principal_id TEXT NOT NULL,job_id TEXT NOT NULL REFERENCES ik_jobs(id) ON DELETE CASCADE,fingerprint TEXT NOT NULL,job_fence INTEGER NOT NULL,action TEXT NOT NULL,values_count INTEGER NOT NULL,chunks_count INTEGER NOT NULL,identities INTEGER NOT NULL,revision INTEGER NOT NULL DEFAULT 0,registered_revision INTEGER,sealed_revision INTEGER,UNIQUE(destination,principal_id,job_id,action))',
  'CREATE TABLE IF NOT EXISTS ik_d1_native_staging_index (stage_id TEXT NOT NULL REFERENCES ik_d1_native_staging(id) ON DELETE CASCADE,ordinal INTEGER NOT NULL,source_identity TEXT NOT NULL,target_identity TEXT NOT NULL,PRIMARY KEY(stage_id,ordinal,target_identity))',
  'CREATE INDEX IF NOT EXISTS ik_d1_native_staging_source ON ik_d1_native_staging_index(source_identity,stage_id)',
  'CREATE INDEX IF NOT EXISTS ik_d1_native_staging_target ON ik_d1_native_staging_index(target_identity,stage_id)',
  'CREATE TABLE IF NOT EXISTS ik_d1_native_staging_values (stage_id TEXT NOT NULL REFERENCES ik_d1_native_staging(id) ON DELETE CASCADE,value_ordinal INTEGER NOT NULL,ordinal INTEGER NOT NULL,body TEXT NOT NULL,body_hash TEXT NOT NULL,body_size INTEGER NOT NULL,value_chunks INTEGER NOT NULL,PRIMARY KEY(stage_id,value_ordinal,ordinal))',
  "CREATE TRIGGER IF NOT EXISTS ik_d1_native_staging_binding BEFORE UPDATE OF id,generation,destination,namespace,mapping_version,principal_id,job_id,fingerprint,job_fence,action,values_count,chunks_count,identities ON ik_d1_native_staging BEGIN SELECT RAISE(ABORT,'IMPORT_NATIVE_STAGE_BINDING_CHANGED'); END",
  ...['index','values'].flatMap(table=>['INSERT','UPDATE','DELETE'].map(event=>{
    const refs = event === 'INSERT' ? 'id=NEW.stage_id' : event === 'DELETE' ? 'id=OLD.stage_id' : 'id=OLD.stage_id OR id=NEW.stage_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_native_staging_${table}_${event.toLowerCase()} AFTER ${event} ON ik_d1_native_staging_${table} BEGIN UPDATE ik_d1_native_staging SET revision=revision+1,sealed_revision=NULL${table==='index'?',registered_revision=NULL':''} WHERE ${refs}; END`;
  })),
  ...['INSERT','UPDATE','DELETE'].map(event=>{
    const refs = event === 'INSERT' ? 'stage_id=NEW.stage_id' : event === 'DELETE' ? 'stage_id=OLD.stage_id' : 'stage_id=OLD.stage_id OR stage_id=NEW.stage_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_native_staging_index_custody_${event.toLowerCase()} AFTER ${event} ON ik_d1_native_staging_index BEGIN DELETE FROM ik_d1_native_staging_values WHERE ${refs}; END`;
  }),
  "CREATE TRIGGER IF NOT EXISTS ik_d1_native_staging_values_registered BEFORE INSERT ON ik_d1_native_staging_values WHEN NOT EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=NEW.stage_id AND registered_revision IS NOT NULL) BEGIN SELECT RAISE(ABORT,'IMPORT_NATIVE_STAGE_INDEX_REQUIRED'); END",
];
