import { streamOriginsMatch } from './stream-origin.ts';
import type { D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { bindingPredicate, stageClear, type StageBinding } from './staging-transport.ts';
import { reportHash } from './undo-state.ts';
import { publishedPlan } from './staging-plan.ts';

/** Membership and order are checked again at seal. Hashes were computed from
 * reviewed bytes before insertion, then every stored column is read back and
 * compared exactly. The expected revision fences those verified values. */
export function stageMembership(stage: StageBinding, payload: boolean): D1Sql {
  return sql(`EXISTS(SELECT 1 FROM ik_d1_staging s WHERE s.id=? AND
    (SELECT count(*) FROM ik_d1_staging_pages p WHERE p.stage_id=s.id)=s.pages AND
    (SELECT coalesce(sum(total),0) FROM ik_d1_staging_pages p WHERE p.stage_id=s.id)=s.total AND
    NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p WHERE p.stage_id=s.id AND
      (p.ordinal<0 OR p.ordinal>=s.pages OR p.total<0 OR p.chunks<1 OR
       json_extract(p.plan_json,'$.sha256') IS NOT p.body_hash OR json_extract(p.plan_json,'$.size') IS NOT p.body_size OR json_extract(p.plan_json,'$.chunks') IS NOT p.chunks OR
       (SELECT count(*) FROM ik_d1_staging_index i WHERE i.stage_id=s.id AND i.page_ordinal=p.ordinal)<>p.identities OR
       (SELECT count(DISTINCT ordinal) FROM ik_d1_staging_index i WHERE i.stage_id=s.id AND i.page_ordinal=p.ordinal)<>p.total
       ${payload ? 'OR (SELECT count(*) FROM ik_d1_staging_chunks c WHERE c.stage_id=s.id AND c.page_ordinal=p.ordinal)<>p.chunks OR (SELECT coalesce(sum(body_size),0) FROM ik_d1_staging_chunks c WHERE c.stage_id=s.id AND c.page_ordinal=p.ordinal)<>p.body_size' : ''})) AND
    NOT EXISTS(SELECT 1 FROM ik_d1_staging_index i LEFT JOIN ik_d1_staging_pages p ON p.stage_id=i.stage_id AND p.ordinal=i.page_ordinal WHERE i.stage_id=s.id AND (p.ordinal IS NULL OR i.ordinal<0 OR i.ordinal>=p.total))
    ${payload ? 'AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_chunks c LEFT JOIN ik_d1_staging_pages p ON p.stage_id=c.stage_id AND p.ordinal=c.page_ordinal WHERE c.stage_id=s.id AND (p.ordinal IS NULL OR c.ordinal<0 OR c.ordinal>=p.chunks OR c.body_size<>length(CAST(c.body AS BLOB))))' : ''})`, stage.id);
}

/** Small SQL-only publication: no plan/index body crosses the transport twice.
 * Every public derived set is compared to its unchanged private exact source. */
export function stagePublication(stage: StageBinding, revision: number): D1Sql[] {
  const id = stage.id;
  return [guard(bindingPredicate(stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=? AND sealed_revision=? AND registered_revision IS NOT NULL)', id, revision, revision)), guard(stageMembership(stage, true)), guard(stageClear(stage)),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_streams WHERE destination=? AND principal_id=? AND operation_key=?)', stage.destination, stage.principal_id, stage.operation_key)),
    sql(`INSERT INTO ik_jobs(id,destination,namespace,mapping_version,principal_id,fingerprint,operation_key,plan_json,state,total,reversal_policy,reversal_policy_hash) SELECT p.job_id,s.destination,s.namespace,s.mapping_version,s.principal_id,p.fingerprint,s.id||':'||p.ordinal,${publishedPlan()},'queued',p.total,s.reversal_policy,p.policy_hash FROM ik_d1_staging_pages p JOIN ik_d1_staging s ON s.id=p.stage_id WHERE s.id=?`, id),
    sql('INSERT INTO ik_d1_plan_chunks(job_id,ordinal,body) SELECT p.job_id,c.ordinal,c.body FROM ik_d1_staging_chunks c JOIN ik_d1_staging_pages p ON p.stage_id=c.stage_id AND p.ordinal=c.page_ordinal WHERE c.stage_id=? AND p.inline_plan=0', id),
    sql('INSERT INTO ik_d1_plan_index(job_id,ordinal,source_identity,target_identity) SELECT p.job_id,i.ordinal,i.source_identity,i.target_identity FROM ik_d1_staging_index i JOIN ik_d1_staging_pages p ON p.stage_id=i.stage_id AND p.ordinal=i.page_ordinal WHERE i.stage_id=?', id),
    sql("INSERT INTO ik_d1_streams(id,destination,namespace,mapping_version,principal_id,fingerprint,operation_key,total,layout_hash,state) SELECT id,destination,namespace,mapping_version,principal_id,fingerprint,operation_key,total,layout_hash,'applying' FROM ik_d1_staging WHERE id=?", id),
    sql('INSERT INTO ik_d1_stream_pages(stream_id,ordinal,job_id,total) SELECT stage_id,ordinal,job_id,total FROM ik_d1_staging_pages WHERE stage_id=?', id),
    sql("INSERT INTO ik_originals(job_id,kind,original_key,name,sha256,size) SELECT p.job_id,'artifact',o.original_key,o.name,o.sha256,o.size FROM ik_d1_staging_originals o JOIN ik_d1_staging_pages p ON p.stage_id=o.stage_id WHERE o.stage_id=?", id),
    sql('INSERT INTO ik_original_sets(job_id,version) SELECT job_id,4 FROM ik_d1_staging_pages WHERE stage_id=?', id),
    sql('INSERT INTO ik_d1_indexed_jobs(job_id,fingerprint) SELECT job_id,fingerprint FROM ik_d1_staging_pages WHERE stage_id=?', id),
    sql("UPDATE ik_d1_streams SET state='applying' WHERE id=?", id),
    sql('INSERT INTO ik_d1_publication_fences(stream_id,job_id) SELECT stage_id,job_id FROM ik_d1_staging_pages WHERE stage_id=?', id),
    guard(sql('((SELECT count(*) FROM ik_d1_publication_fences WHERE stream_id=?)=(SELECT count(*) FROM ik_d1_staging_pages WHERE stage_id=?)) AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p WHERE p.stage_id=? AND NOT EXISTS(SELECT 1 FROM ik_d1_publication_fences f WHERE f.stream_id=p.stage_id AND f.job_id=p.job_id))', id, id, id)),
    guard(sql(`NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p JOIN ik_d1_staging s ON s.id=p.stage_id WHERE s.id=? AND NOT EXISTS(SELECT 1 FROM ik_jobs j WHERE j.id=p.job_id AND j.destination=s.destination AND j.namespace=s.namespace AND j.mapping_version=s.mapping_version AND j.principal_id=s.principal_id AND j.fingerprint=p.fingerprint AND j.operation_key=s.id||':'||p.ordinal AND j.plan_json IS (${publishedPlan()}) AND j.total=p.total AND j.state='queued' AND j.checkpoint=0 AND j.fence=0 AND j.error IS NULL AND j.lease_token IS NULL AND j.lease_expires IS NULL AND j.reversal_policy=s.reversal_policy AND j.reversal_policy_hash=p.policy_hash AND j.reversal_cursor=0))`, id)),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_jobs j JOIN ik_d1_staging_pages p ON p.job_id=j.id WHERE p.stage_id=? AND (j.reversal_hash<>? OR EXISTS(SELECT 1 FROM ik_d1_reversal_reports r WHERE r.job_id=j.id)))', id, reportHash([]))),
    ...exactPublishedSets(id),
    // The upstream capture trigger provides a second exact in-transaction copy.
    // Release it only while its corresponding verified live chunk still exists.
    sql('UPDATE ik_d1_creation_plans SET released=1 WHERE job_id IN (SELECT job_id FROM ik_d1_staging_pages WHERE stage_id=?)', id),
    sql('DELETE FROM ik_d1_creation_plans WHERE job_id IN (SELECT job_id FROM ik_d1_staging_pages WHERE stage_id=?)', id),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_creation_plans WHERE job_id IN (SELECT job_id FROM ik_d1_staging_pages WHERE stage_id=?))', id)),
    sql('UPDATE ik_d1_custody_revisions SET seal=? WHERE job_id IN (SELECT job_id FROM ik_d1_staging_pages WHERE stage_id=?)',id,id),
    guard(stageClear(stage)), guard(bindingPredicate(stage)), guard(sql('EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=? AND revision=? AND sealed_revision=?)', id, revision, revision)),
    sql('DELETE FROM ik_d1_staging WHERE id=? AND generation=?', id, stage.generation),
    // Public mutations (including cleanup-trigger effects) are prohibited by
    // the exact identity fence until the final transaction authority check.
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=?)', id))];
}

function exactPublishedSets(id: string): D1Sql[] {
  return [
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p WHERE p.stage_id=? AND ((SELECT count(*) FROM ik_d1_plan_chunks c WHERE c.job_id=p.job_id)<>CASE WHEN p.inline_plan=1 THEN 0 ELSE p.chunks END OR (SELECT count(*) FROM ik_d1_creation_plans c WHERE c.job_id=p.job_id AND c.released=0)<>CASE WHEN p.inline_plan=1 THEN 0 ELSE p.chunks END OR (SELECT count(*) FROM ik_d1_plan_index i WHERE i.job_id=p.job_id)<>p.identities))', id)),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging_chunks c JOIN ik_d1_staging_pages p ON p.stage_id=c.stage_id AND p.ordinal=c.page_ordinal WHERE c.stage_id=? AND p.inline_plan=0 AND (NOT EXISTS(SELECT 1 FROM ik_d1_plan_chunks live WHERE live.job_id=p.job_id AND live.ordinal=c.ordinal AND live.body IS c.body) OR NOT EXISTS(SELECT 1 FROM ik_d1_creation_plans live WHERE live.job_id=p.job_id AND live.ordinal=c.ordinal AND live.body IS c.body AND live.released=0)))', id)),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging_index i JOIN ik_d1_staging_pages p ON p.stage_id=i.stage_id AND p.ordinal=i.page_ordinal WHERE i.stage_id=? AND NOT EXISTS(SELECT 1 FROM ik_d1_plan_index live WHERE live.job_id=p.job_id AND live.ordinal=i.ordinal AND live.source_identity=i.source_identity AND live.target_identity=i.target_identity))', id)),
    guard(sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p WHERE p.stage_id=? AND ((SELECT count(*) FROM ik_originals o WHERE o.job_id=p.job_id)<>(SELECT count(*) FROM ik_d1_staging_originals o WHERE o.stage_id=p.stage_id) OR EXISTS(SELECT 1 FROM ik_d1_original_chunks c WHERE c.job_id=p.job_id) OR NOT EXISTS(SELECT 1 FROM ik_original_sets o WHERE o.job_id=p.job_id AND o.version=4) OR NOT EXISTS(SELECT 1 FROM ik_d1_indexed_jobs i WHERE i.job_id=p.job_id AND i.fingerprint=p.fingerprint)))', id)),
    guard(sql("NOT EXISTS(SELECT 1 FROM ik_d1_staging_originals o JOIN ik_d1_staging_pages p ON p.stage_id=o.stage_id WHERE o.stage_id=? AND NOT EXISTS(SELECT 1 FROM ik_originals live WHERE live.job_id=p.job_id AND live.kind='artifact' AND live.original_key=o.original_key AND live.name IS o.name AND live.sha256=o.sha256 AND live.size=o.size AND live.bytes IS NULL))", id)),
    guard(sql(`EXISTS(SELECT 1 FROM ik_d1_staging s JOIN ik_d1_streams live ON live.id=s.id WHERE s.id=? AND live.destination=s.destination AND live.namespace=s.namespace AND live.mapping_version=s.mapping_version AND live.principal_id=s.principal_id AND live.fingerprint=s.fingerprint AND live.operation_key=s.operation_key AND live.total=s.total AND live.layout_hash=s.layout_hash AND live.state='applying' AND ${streamOriginsMatch('live')} AND (SELECT count(*) FROM ik_d1_stream_pages WHERE stream_id=s.id)=s.pages AND NOT EXISTS(SELECT 1 FROM ik_d1_staging_pages p WHERE p.stage_id=s.id AND NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages l WHERE l.stream_id=s.id AND l.ordinal=p.ordinal AND l.job_id=p.job_id AND l.total=p.total AND l.erased_records=0)))`, id)),
  ];
}

/** Known receipts fence the already exact-checked public sets after all private
 * bytes and publication fences are deleted. Nothing writes after these checks. */
export function publishedStagePredicates(stage:StageBinding,pages:{ordinal:number;job_id:string;total:number}[]):D1Sql[]{
  const binding={id:stage.id,destination:stage.destination,namespace:stage.namespace,mapping_version:stage.mapping_version,principal_id:stage.principal_id,fingerprint:stage.fingerprint,operation_key:stage.operation_key,total:stage.total,layout_hash:stage.layout_hash,state:'applying'};
  const rows=JSON.stringify(pages.map(({ordinal,job_id,total})=>[ordinal,job_id,total]));
  return [sql(`EXISTS(SELECT 1 FROM ik_d1_streams live WHERE ${Object.keys(binding).map(key=>`live.${key} IS ?`).join(' AND ')} AND ${streamOriginsMatch('live')})`,...Object.values(binding)),
    sql(`(SELECT count(*) FROM ik_d1_stream_pages WHERE stream_id=?)=? AND NOT EXISTS(SELECT 1 FROM json_each(?) expected WHERE NOT EXISTS(SELECT 1 FROM ik_d1_stream_pages p JOIN ik_d1_custody_revisions r ON r.job_id=p.job_id JOIN ik_d1_custody_states c ON c.job_id=p.job_id WHERE p.stream_id=? AND p.ordinal=json_extract(expected.value,'$[0]') AND p.job_id=json_extract(expected.value,'$[1]') AND p.total=json_extract(expected.value,'$[2]') AND p.erased_records=0 AND typeof(c.revision)='integer' AND c.revision>=0 AND r.seal=?))`,stage.id,pages.length,rows,stage.id,stage.id),
    sql('NOT EXISTS(SELECT 1 FROM ik_d1_plan_index i JOIN ik_d1_stream_pages p ON p.job_id=i.job_id WHERE p.stream_id=? AND (EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=i.source_identity) OR EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=i.target_identity)))',stage.id,stage.destination,stage.destination),
    sql('NOT EXISTS(SELECT 1 FROM ik_d1_creation_plans c JOIN ik_d1_stream_pages p ON p.job_id=c.job_id WHERE p.stream_id=?)',stage.id),
    sql('NOT EXISTS(SELECT 1 FROM ik_d1_staging WHERE id=?) AND NOT EXISTS(SELECT 1 FROM ik_d1_publication_fences WHERE stream_id=?)',stage.id,stage.id)];
}
