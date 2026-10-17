/** Add these statements as a new seed migration, after the D1 import-kit schema.
 * Installing the schema never enables the door. No seed is bound automatically. */
export const importDoorSchema = [
  `CREATE TABLE IF NOT EXISTS ik_door_settings (
    installation_id TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)))`,
  `CREATE TABLE IF NOT EXISTS ik_door_revoked_keys (
    installation_id TEXT NOT NULL, kid TEXT NOT NULL, PRIMARY KEY(installation_id,kid))`,
  `CREATE TABLE IF NOT EXISTS ik_door_grants (
    installation_id TEXT NOT NULL, jti TEXT NOT NULL, action TEXT NOT NULL,
    namespace TEXT NOT NULL, kid TEXT NOT NULL, expires INTEGER NOT NULL,
    PRIMARY KEY(installation_id,jti))`,
  `CREATE TABLE IF NOT EXISTS ik_door_uploads (
    id TEXT PRIMARY KEY, installation_id TEXT NOT NULL, namespace TEXT NOT NULL,
    name TEXT NOT NULL, bundle_sha256 TEXT NOT NULL, total_bytes INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS ik_door_chunks (
    upload_id TEXT NOT NULL REFERENCES ik_door_uploads(id), ordinal INTEGER NOT NULL,
    sha256 TEXT NOT NULL, size INTEGER NOT NULL, base64 TEXT NOT NULL,
    PRIMARY KEY(upload_id,ordinal))`,
  `CREATE TABLE IF NOT EXISTS ik_door_namespaces (
    installation_id TEXT NOT NULL, namespace TEXT NOT NULL, head_id TEXT, pending_id TEXT,
    PRIMARY KEY(installation_id,namespace))`,
  `CREATE TABLE IF NOT EXISTS ik_door_plans (
    id TEXT PRIMARY KEY, installation_id TEXT NOT NULL, namespace TEXT NOT NULL,
    bundle_sha256 TEXT NOT NULL, fingerprint TEXT NOT NULL, previous_id TEXT,
    job_id TEXT UNIQUE, snapshot_hash TEXT NOT NULL,
    complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
    UNIQUE(installation_id,namespace,bundle_sha256))`,
  `CREATE TABLE IF NOT EXISTS ik_door_plan_chunks (
    plan_id TEXT NOT NULL REFERENCES ik_door_plans(id), ordinal INTEGER NOT NULL,
    body TEXT NOT NULL, PRIMARY KEY(plan_id,ordinal))`,
  `CREATE TABLE IF NOT EXISTS ik_door_approvals (
    plan_id TEXT PRIMARY KEY REFERENCES ik_door_plans(id), approval_id TEXT NOT NULL,
    owner TEXT NOT NULL, approved_at TEXT NOT NULL, fingerprint TEXT NOT NULL,
    reviewer TEXT NOT NULL)`,
];
