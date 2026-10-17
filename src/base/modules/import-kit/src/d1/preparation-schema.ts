/** Additive custody for an acknowledged but unverified preparation. Neither
 * table is a public job. Both disappear with their exact staging generation. */
export const d1PreparationSchema = [
  'CREATE TABLE IF NOT EXISTS ik_d1_preparation_receipts (stage_id TEXT PRIMARY KEY REFERENCES ik_d1_staging(id) ON DELETE CASCADE, generation TEXT NOT NULL, codec_id TEXT, header_hash TEXT NOT NULL, header_size INTEGER NOT NULL, header_chunks INTEGER NOT NULL)',
  'CREATE TABLE IF NOT EXISTS ik_d1_staging_headers (stage_id TEXT NOT NULL REFERENCES ik_d1_staging(id) ON DELETE CASCADE, ordinal INTEGER NOT NULL, body TEXT NOT NULL, body_hash TEXT NOT NULL, body_size INTEGER NOT NULL, PRIMARY KEY(stage_id,ordinal))',
  ...['INSERT','UPDATE','DELETE'].flatMap(event => {
    const refs = event === 'INSERT' ? 'id=NEW.stage_id' : event === 'DELETE' ? 'id=OLD.stage_id' : 'id=OLD.stage_id OR id=NEW.stage_id';
    return ['ik_d1_preparation_receipts','ik_d1_staging_headers'].map(table => `CREATE TRIGGER IF NOT EXISTS ${table}_${event.toLowerCase()} AFTER ${event} ON ${table} BEGIN UPDATE ik_d1_staging SET revision=revision+1,sealed_revision=NULL WHERE ${refs}; END`);
  }),
  "CREATE TRIGGER IF NOT EXISTS ik_d1_preparation_receipt_immutable BEFORE UPDATE ON ik_d1_preparation_receipts BEGIN SELECT RAISE(ABORT,'IMPORT_PREPARATION_CHANGED'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_staging_headers_registered BEFORE INSERT ON ik_d1_staging_headers WHEN NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=NEW.stage_id AND registered_revision IS NOT NULL) BEGIN SELECT RAISE(ABORT,'IMPORT_STAGE_INDEX_REQUIRED'); END",
  ...['INSERT','UPDATE','DELETE'].map(event => {
    const refs = event === 'INSERT' ? 'stage_id=NEW.stage_id' : event === 'DELETE' ? 'stage_id=OLD.stage_id' : 'stage_id=OLD.stage_id OR stage_id=NEW.stage_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_preparation_privacy_${event.toLowerCase()} AFTER ${event} ON ik_d1_staging_index BEGIN DELETE FROM ik_d1_staging_headers WHERE ${refs}; END`;
  }),
];
