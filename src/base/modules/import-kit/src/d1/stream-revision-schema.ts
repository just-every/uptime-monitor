/** Parent identity is retained even after parent/child deletion. Revisions never
 * reset, so deleting and reconstructing the same IDs cannot recreate an earlier
 * receipt. Only immutable layout changes advance layout_revision; child progress
 * advances progress_revision and remains legal under a layout-only child fence. */
export const streamParentFields = ['destination','namespace','mapping_version','principal_id','fingerprint','operation_key','total','layout_hash'];
export const streamAnchorMatch = (s: string, r: string) => streamParentFields.map(field => `${s}.${field} IS ${r}.${field}`).join(' AND ');
const fields = streamParentFields.join(',');
const remember = (ref: string) => `INSERT INTO ik_d1_stream_revisions(stream_id,${fields}) SELECT ${ref}.id,${streamParentFields.map(field=>`${ref}.${field}`).join(',')} WHERE NOT EXISTS(SELECT 1 FROM ik_d1_stream_revisions WHERE stream_id=${ref}.id); UPDATE ik_d1_stream_revisions SET layout_revision=layout_revision+1,progress_revision=progress_revision+1 WHERE stream_id=${ref}.id;`;
const bump = (where: string, layout = true) => `UPDATE ik_d1_stream_revisions SET ${layout?'layout_revision=layout_revision+1,':''}progress_revision=progress_revision+1 WHERE ${where};`;
const member = (refs: string[]) => `stream_id IN (SELECT stream_id FROM ik_d1_stream_pages WHERE ${refs.map(ref=>`job_id=${ref}.id`).join(' OR ')} UNION SELECT stream_id FROM ik_d1_stream_origins WHERE ${refs.map(ref=>`job_id=${ref}.id`).join(' OR ')})`;
export const d1StreamRevisionSchema = [
  `CREATE TABLE IF NOT EXISTS ik_d1_stream_revisions(stream_id TEXT PRIMARY KEY,${streamParentFields.map(field=>`${field} ${field==='total'?'INTEGER':'TEXT'} NOT NULL`).join(',')},layout_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(layout_revision)='integer' AND layout_revision>=0),progress_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(progress_revision)='integer' AND progress_revision>=0))`,
  // This backfill runs only at schema installation; it does not bless a changed
  // layout on later reads or reinitialization. Origins and canonical hashes must
  // still validate before any public result or child effect.
  `INSERT INTO ik_d1_stream_revisions(stream_id,${fields}) SELECT id,${fields} FROM ik_d1_streams WHERE NOT EXISTS(SELECT 1 FROM ik_d1_stream_revisions WHERE stream_id=ik_d1_streams.id)`,
  "CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_insert BEFORE INSERT ON ik_d1_stream_revisions WHEN EXISTS(SELECT 1 FROM ik_d1_stream_revisions WHERE stream_id=NEW.stream_id) BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_REVISION_CHANGED'); END",
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_update BEFORE UPDATE ON ik_d1_stream_revisions WHEN NEW.stream_id IS NOT OLD.stream_id OR ${streamParentFields.map(field=>`NEW.${field} IS NOT OLD.${field}`).join(' OR ')} OR NEW.layout_revision<OLD.layout_revision OR NEW.progress_revision<=OLD.progress_revision BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_REVISION_CHANGED'); END`,
  "CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_delete BEFORE DELETE ON ik_d1_stream_revisions BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_REVISION_CHANGED'); END",
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_parent_insert AFTER INSERT ON ik_d1_streams BEGIN ${remember('NEW')} END`,
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_parent_delete AFTER DELETE ON ik_d1_streams BEGIN ${bump('stream_id=OLD.id')} END`,
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_parent_update AFTER UPDATE ON ik_d1_streams BEGIN ${bump('stream_id=OLD.id OR stream_id=NEW.id',false)} END`,
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_parent_layout AFTER UPDATE OF id,${fields} ON ik_d1_streams BEGIN ${bump('stream_id=OLD.id OR stream_id=NEW.id')} END`,
  ...['ik_d1_stream_pages','ik_d1_stream_origins'].flatMap(table=>['INSERT','UPDATE','DELETE'].map(event=> {
    const refs = event==='INSERT'?'stream_id=NEW.stream_id':event==='DELETE'?'stream_id=OLD.stream_id':'stream_id=OLD.stream_id OR stream_id=NEW.stream_id';
    const columns = table==='ik_d1_stream_pages' && event==='UPDATE'?' OF stream_id,ordinal,job_id,total':'';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_${table}_${event.toLowerCase()} AFTER ${event}${columns} ON ${table} BEGIN ${bump(refs)} END`;
  })),
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_page_erasure AFTER UPDATE OF erased_records ON ik_d1_stream_pages BEGIN ${bump('stream_id=OLD.stream_id OR stream_id=NEW.stream_id',false)} END`,
  ...['INSERT','UPDATE','DELETE'].map(event=> {
    const refs = event==='INSERT'?['NEW']:event==='DELETE'?['OLD']:['OLD','NEW'];
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_job_${event.toLowerCase()} AFTER ${event} ON ik_jobs BEGIN ${bump(member(refs),event!=='UPDATE')} END`;
  }),
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_revision_job_layout AFTER UPDATE OF id,destination,namespace,mapping_version,principal_id,fingerprint,operation_key,total,reversal_policy,reversal_policy_hash ON ik_jobs BEGIN ${bump(member(['OLD','NEW']))} END`,
];
