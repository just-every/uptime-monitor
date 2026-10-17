import { d1StreamRevisionSchema, streamAnchorMatch } from './stream-revision-schema.ts';
/** A stream origin comes only from real page membership. It survives loss of
 * that page or parent; ordinary legacy custody is never inferred from data. */
const parentFields = ['destination','namespace','mapping_version','principal_id','fingerprint','operation_key','total','layout_hash'];
const columns = ['job_id','stream_id','ordinal','page_total','job_operation_key',...parentFields.map(field=>'parent_'+field)];
const values = (p:string,s:string,j:string) => [`${p}.job_id`,`${s}.id`,`${p}.ordinal`,`${p}.total`,`${j}.operation_key`,...parentFields.map(field=>`${s}.${field}`)];
/** Nested membership checks share D1's expression-depth budget. Keep every
 * exact comparison, but give the conjunction logarithmic rather than linear depth. */
function conjunction(terms:string[]):string {
  if(terms.length===1)return terms[0];
  const middle=Math.floor(terms.length/2);
  return `(${conjunction(terms.slice(0,middle))} AND ${conjunction(terms.slice(middle))})`;
}
function exactOrigin(o:string,p:string,s:string,j:string):string {
  return conjunction([...columns.map((column,index)=>`${o}.${column} IS ${values(p,s,j)[index]}`),
    `${p}.stream_id IS ${s}.id`,`${j}.total IS ${p}.total`,...['destination','namespace','mapping_version','principal_id'].map(field=>`${j}.${field} IS ${s}.${field}`)]);
}
/** Both directions close the original membership set, including deleted peers. */
export function streamOriginsMatch(s:string):string {
  return `EXISTS(SELECT 1 FROM ik_d1_stream_revisions sr WHERE sr.stream_id=${s}.id AND ${streamAnchorMatch(s,'sr')}) AND NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages op WHERE op.stream_id=${s}.id AND NOT EXISTS(SELECT 1 FROM ik_d1_stream_origins oo JOIN ik_jobs oj ON oj.id=oo.job_id WHERE ${exactOrigin('oo','op',s,'oj')})) AND NOT EXISTS(SELECT 1 FROM ik_d1_stream_origins oo WHERE oo.stream_id=${s}.id AND NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages op JOIN ik_jobs oj ON oj.id=op.job_id WHERE ${exactOrigin('oo','op',s,'oj')}))`;
}
export function liveStreamOrigin(job:string):string {
  return `EXISTS(SELECT 1 FROM ik_d1_stream_origins o JOIN ik_d1_stream_pages p ON p.job_id=o.job_id JOIN ik_d1_streams s ON s.id=o.stream_id JOIN ik_jobs j ON j.id=o.job_id WHERE o.job_id=${job} AND ${exactOrigin('o','p','s','j')} AND s.state IN ('applying','undoing') AND ${streamOriginsMatch('s')})`;
}
export function needsStreamOrigin(job:string):string {
  return `(EXISTS(SELECT 1 FROM ik_original_sets os WHERE os.job_id=${job} AND os.version=4) OR EXISTS(SELECT 1 FROM ik_d1_stream_origins oo WHERE oo.job_id=${job}) OR EXISTS(SELECT 1 FROM ik_d1_stream_pages op WHERE op.job_id=${job}))`;
}

const capture = (where='') => `INSERT INTO ik_d1_stream_origins(${columns.join(',')}) SELECT ${values('p','s','j').join(',')} FROM ik_d1_stream_pages p JOIN ik_d1_streams s ON s.id=p.stream_id JOIN ik_jobs j ON j.id=p.job_id WHERE NOT EXISTS(SELECT 1 FROM ik_d1_stream_origins o WHERE o.job_id=p.job_id) AND j.total IS p.total AND ${['destination','namespace','mapping_version','principal_id'].map(field=>`j.${field} IS s.${field}`).join(' AND ')}${where}`;
/** Additive setup, after erasure tables and before legacy receipt conversion.
 * The job-delete trigger also works when a SQLite caller disables foreign keys.
 * Child fingerprints and progress can change through erasure; parent identity,
 * layout and the original child operation binding cannot. */
export const d1StreamOriginSchema = [
  `CREATE TABLE IF NOT EXISTS ik_d1_stream_origins (job_id TEXT PRIMARY KEY,stream_id TEXT NOT NULL,ordinal INTEGER NOT NULL,page_total INTEGER NOT NULL,job_operation_key TEXT NOT NULL,${parentFields.map(field=>`parent_${field} ${field==='total'?'INTEGER':'TEXT'} NOT NULL`).join(',')})`,
  'CREATE INDEX IF NOT EXISTS ik_d1_stream_origin_parent ON ik_d1_stream_origins(stream_id,ordinal)',
  `CREATE TRIGGER IF NOT EXISTS ik_d1_stream_origin_insert BEFORE INSERT ON ik_d1_stream_origins WHEN EXISTS(SELECT 1 FROM ik_d1_stream_origins WHERE job_id=NEW.job_id) OR NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages p JOIN ik_d1_streams s ON s.id=p.stream_id JOIN ik_jobs j ON j.id=p.job_id WHERE ${exactOrigin('NEW','p','s','j')}) BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_ORIGIN_CHANGED'); END`,
  "CREATE TRIGGER IF NOT EXISTS ik_d1_stream_origin_update BEFORE UPDATE ON ik_d1_stream_origins BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_ORIGIN_CHANGED'); END",
  "CREATE TRIGGER IF NOT EXISTS ik_d1_stream_origin_delete BEFORE DELETE ON ik_d1_stream_origins WHEN EXISTS(SELECT 1 FROM ik_jobs WHERE id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'IMPORT_STREAM_ORIGIN_CHANGED'); END",
  'CREATE TRIGGER IF NOT EXISTS ik_d1_stream_origin_job_delete AFTER DELETE ON ik_jobs BEGIN DELETE FROM ik_d1_stream_origins WHERE job_id=OLD.id; END',
  ...['INSERT','UPDATE OF stream_id,ordinal,job_id,total'].map(event=>`CREATE TRIGGER IF NOT EXISTS ik_d1_stream_origin_page_${event==='INSERT'?'insert':'update'} AFTER ${event} ON ik_d1_stream_pages BEGIN ${capture(' AND p.job_id=NEW.job_id')}; END`),
  capture(),
  ...d1StreamRevisionSchema,
];
