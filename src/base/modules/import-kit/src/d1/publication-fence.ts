/** Exact publication is immutable until private cleanup and the final current
 * authority check finish. These guards carry only identities, never payloads. */
const guarded = [
  ['ik_jobs', 'id', 'job_id'], ['ik_d1_streams', 'id', 'stream_id'],
  ...['ik_d1_plan_chunks','ik_d1_plan_index','ik_originals','ik_d1_original_chunks','ik_original_sets','ik_d1_indexed_jobs','ik_d1_reversal_reports','ik_provenance','ik_sources','ik_d1_entries','ik_d1_history','ik_batches'].map(table => [table, 'job_id', 'job_id']),
  ['ik_d1_stream_pages', 'stream_id', 'stream_id'],
];
export const publicationFenceSchema = [
  'CREATE TABLE IF NOT EXISTS ik_d1_publication_fences (stream_id TEXT NOT NULL,job_id TEXT PRIMARY KEY)',
  'CREATE INDEX IF NOT EXISTS ik_d1_publication_fence_stream ON ik_d1_publication_fences(stream_id)',
  ...guarded.flatMap(([table, column, fence]) => ['INSERT','UPDATE','DELETE'].map(event => {
    const refs = event === 'INSERT' ? `${fence}=NEW.${column}` : event === 'DELETE' ? `${fence}=OLD.${column}` : `${fence}=OLD.${column} OR ${fence}=NEW.${column}`;
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_publication_${table}_${event.toLowerCase()} BEFORE ${event} ON ${table} WHEN EXISTS(SELECT 1 FROM ik_d1_publication_fences WHERE ${refs}) BEGIN SELECT RAISE(ABORT,'IMPORT_PUBLICATION_CHANGED'); END`;
  })),
  // A native publication uses its private generation as stream_id. Fence the
  // real parent through the guarded child as well as through a stream receipt.
  ...['UPDATE','DELETE'].map(event=>`CREATE TRIGGER IF NOT EXISTS ik_d1_publication_parent_${event.toLowerCase()} BEFORE ${event} ON ik_d1_streams WHEN EXISTS(SELECT 1 FROM ik_d1_stream_pages p JOIN ik_d1_publication_fences f ON f.job_id=p.job_id WHERE p.stream_id=OLD.id) BEGIN SELECT RAISE(ABORT,'IMPORT_PUBLICATION_CHANGED'); END`),
  ...['INSERT','UPDATE','DELETE'].map(event=>{
    const refs=event==='INSERT'?'job_id=NEW.job_id':event==='DELETE'?'job_id=OLD.job_id':'job_id=OLD.job_id OR job_id=NEW.job_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_publication_child_page_${event.toLowerCase()} BEFORE ${event} ON ik_d1_stream_pages WHEN EXISTS(SELECT 1 FROM ik_d1_publication_fences WHERE ${refs}) BEGIN SELECT RAISE(ABORT,'IMPORT_PUBLICATION_CHANGED'); END`;
  }),
];
