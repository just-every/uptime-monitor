/** One row per reversing job: the custody revision its last verified reversal
 * batch left behind. Every custody table of the job, its plan, originals and
 * erasure tombstones advance ik_d1_custody_states, so an equal revision proves
 * that nothing the complete inventory checked has changed since. Jobs that
 * reuse another job's entries keep the complete per-batch inventory. */
export const d1ReversalFenceSchema = [
  'CREATE TABLE IF NOT EXISTS ik_d1_reversal_fences (job_id TEXT PRIMARY KEY REFERENCES ik_jobs(id) ON DELETE CASCADE,action TEXT NOT NULL,fence INTEGER NOT NULL,state_revision INTEGER NOT NULL,erased_records INTEGER NOT NULL,reused INTEGER NOT NULL)',
  // A batch's next effects, and the claims it may release, are index lookups
  // rather than scans of every job's provenance and claims.
  'CREATE INDEX IF NOT EXISTS ik_provenance_job ON ik_provenance(job_id,reversed,ordinal,effect_ordinal)',
  'CREATE INDEX IF NOT EXISTS ik_provenance_source ON ik_provenance(source_identity,reversed)',
  'CREATE INDEX IF NOT EXISTS ik_d1_history_source ON ik_d1_history(source_identity)',
  'CREATE INDEX IF NOT EXISTS ik_sources_job ON ik_sources(job_id)',
];
