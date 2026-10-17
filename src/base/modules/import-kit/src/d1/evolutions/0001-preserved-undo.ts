/** Additive evolution. Existing products apply these statements ONCE in their
 * next numbered migration. Never edit an already applied product migration. */
export const preservedUndoEvolution = [
  "ALTER TABLE ik_jobs ADD COLUMN reversal_policy TEXT NOT NULL DEFAULT 'strict' CHECK(reversal_policy IN ('strict','preserve-conflicts'))",
  'ALTER TABLE ik_jobs ADD COLUMN reversal_policy_hash TEXT',
  'ALTER TABLE ik_jobs ADD COLUMN reversal_cursor INTEGER NOT NULL DEFAULT 0 CHECK(reversal_cursor>=0)',
  "ALTER TABLE ik_jobs ADD COLUMN reversal_hash TEXT NOT NULL DEFAULT '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945'",
  'CREATE TABLE ik_d1_creation_plans (job_id TEXT NOT NULL REFERENCES ik_jobs(id), ordinal INTEGER NOT NULL, body TEXT NOT NULL, released INTEGER NOT NULL DEFAULT 0 CHECK(released IN (0,1)), PRIMARY KEY(job_id,ordinal))',
  'CREATE TRIGGER ik_d1_creation_plan_capture AFTER INSERT ON ik_d1_plan_chunks BEGIN INSERT INTO ik_d1_creation_plans(job_id,ordinal,body) VALUES(NEW.job_id,NEW.ordinal,NEW.body); END',
  `CREATE TRIGGER ik_d1_creation_plan_immutable BEFORE UPDATE ON ik_d1_creation_plans WHEN NEW.job_id IS NOT OLD.job_id OR NEW.ordinal IS NOT OLD.ordinal OR NEW.body IS NOT OLD.body OR OLD.released<>0 OR NEW.released<>1 OR NOT EXISTS(SELECT 1 FROM ik_d1_plan_chunks p WHERE p.job_id=OLD.job_id AND p.ordinal=OLD.ordinal AND p.body IS OLD.body) BEGIN SELECT RAISE(ABORT,'IMPORT_CREATION_SNAPSHOT_CHANGED'); END`,
  `CREATE TRIGGER ik_d1_creation_plan_release BEFORE DELETE ON ik_d1_creation_plans WHEN OLD.released<>1 BEGIN SELECT RAISE(ABORT,'IMPORT_CREATION_SNAPSHOT_CHANGED'); END`,
  'CREATE TABLE ik_d1_reversal_reports (job_id TEXT NOT NULL REFERENCES ik_jobs(id), effect_id TEXT NOT NULL, position INTEGER NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY(job_id,effect_id), UNIQUE(job_id,position))',
];
