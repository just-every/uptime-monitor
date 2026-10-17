/** The known publication receipt survives private cleanup. Every public write
 * invalidates it; a non-null receipt can be installed only under its fence. */
export const publicationReceiptSchema = [
  "CREATE TRIGGER IF NOT EXISTS ik_d1_custody_receipt_revision AFTER UPDATE OF revision ON ik_d1_custody_revisions BEGIN UPDATE ik_d1_custody_revisions SET seal=NULL WHERE job_id=NEW.job_id; END",
  ...['INSERT','UPDATE OF seal'].map(event=>`CREATE TRIGGER IF NOT EXISTS ik_d1_custody_receipt_${event.startsWith('INSERT')?'insert':'seal'} BEFORE ${event} ON ik_d1_custody_revisions WHEN NEW.seal IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ik_d1_publication_fences WHERE job_id=NEW.job_id AND stream_id=NEW.seal) BEGIN SELECT RAISE(ABORT,'IMPORT_PUBLICATION_RECEIPT_CHANGED'); END`),
  ...[['ik_jobs','id'],...['ik_d1_plan_chunks','ik_d1_plan_index','ik_originals','ik_d1_original_chunks','ik_original_sets','ik_d1_indexed_jobs','ik_d1_reversal_reports','ik_provenance','ik_sources','ik_d1_entries','ik_d1_history','ik_batches'].map(table=>[table,'job_id'])].flatMap(([table,column])=>['INSERT','UPDATE','DELETE'].map(event=>{
    const refs=event==='INSERT'?`job_id=NEW.${column}`:event==='DELETE'?`job_id=OLD.${column}`:`job_id=OLD.${column} OR job_id=NEW.${column}`;
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_receipt_${table}_${event.toLowerCase()} AFTER ${event} ON ${table} BEGIN UPDATE ik_d1_custody_revisions SET seal=NULL WHERE ${refs}; END`;
  })),
  ...['INSERT','UPDATE','DELETE'].map(event=>{
    const refs=event==='INSERT'?'job_id=NEW.job_id':event==='DELETE'?'job_id=OLD.job_id':'job_id=OLD.job_id OR job_id=NEW.job_id';
    return `CREATE TRIGGER IF NOT EXISTS ik_d1_receipt_pages_${event.toLowerCase()} AFTER ${event} ON ik_d1_stream_pages BEGIN UPDATE ik_d1_custody_revisions SET seal=NULL WHERE ${refs}; END`;
  }),
  ...['UPDATE','DELETE'].map(event=>`CREATE TRIGGER IF NOT EXISTS ik_d1_receipt_stream_${event.toLowerCase()} BEFORE ${event} ON ik_d1_streams BEGIN UPDATE ik_d1_custody_revisions SET seal=NULL WHERE job_id IN (SELECT job_id FROM ik_d1_stream_pages WHERE stream_id=OLD.id); END`),
];
