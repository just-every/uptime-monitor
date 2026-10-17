/** Additive control rows for preparations spread over several requests. A row
 * names the private generations its steps staged and the resumable fingerprint
 * state; it holds no source value. A stream the steps published keeps the
 * source digest they began with, so only that upload replays it. Apply after
 * d1PreparationSchema. */
export const d1PreparationStepsSchema = [
  "CREATE TABLE IF NOT EXISTS ik_d1_preparation_steps (id TEXT PRIMARY KEY, destination TEXT NOT NULL, namespace TEXT NOT NULL, mapping_version TEXT NOT NULL, principal_id TEXT NOT NULL, operation_key TEXT NOT NULL, reversal_policy TEXT NOT NULL, codec_id TEXT, source_digest TEXT NOT NULL, total INTEGER NOT NULL, records INTEGER NOT NULL DEFAULT 0, stages TEXT NOT NULL DEFAULT '[]', phase TEXT NOT NULL CHECK(phase IN ('collecting','hashing','combining','prepared')), hash_json TEXT, fingerprint TEXT, verified_revision INTEGER, revision INTEGER NOT NULL DEFAULT 0, UNIQUE(destination,principal_id,operation_key))",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_preparation_steps_binding BEFORE UPDATE OF id,destination,namespace,mapping_version,principal_id,operation_key,reversal_policy,codec_id,source_digest,total ON ik_d1_preparation_steps BEGIN SELECT RAISE(ABORT,'IMPORT_PREPARATION_BINDING_CHANGED'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_preparation_steps_revision BEFORE UPDATE ON ik_d1_preparation_steps WHEN NEW.revision<>OLD.revision+1 BEGIN SELECT RAISE(ABORT,'IMPORT_PREPARATION_CHANGED'); END",
  'CREATE TABLE IF NOT EXISTS ik_d1_stream_sources (stream_id TEXT PRIMARY KEY REFERENCES ik_d1_streams(id) ON DELETE CASCADE, destination TEXT NOT NULL, source_digest TEXT NOT NULL)',
];
