import { guardPredicate } from './guards.ts';
import type { D1ImportContext, D1Sql } from './types.ts';
import { authority, sql } from './database.ts';

/** A privacy observation and its current authority share one SQL snapshot.
 * Denied reads never disclose whether a matching identity was erased. */
export async function stagingPrivacyClear(context: D1ImportContext, predicate: D1Sql): Promise<boolean> {
  const authorized = guardPredicate(authority(context, 'create'))!;
  const row = await context.storage.database.prepare(`WITH permission AS (SELECT CASE WHEN (${authorized.sql}) THEN 1 ELSE 0 END AS authorized) SELECT authorized,CASE WHEN authorized THEN (${predicate.sql}) ELSE NULL END AS clear FROM permission`)
    .bind(...authorized.params ?? [], ...predicate.params ?? []).first();
  if (!row || row.authorized !== 0 && row.authorized !== 1) throw new Error('IMPORT_D1_READ_RECEIPT_REQUIRED');
  if (!row.authorized) throw new Error('IMPORT_ATOMIC_GUARD');
  if (row.clear !== 0 && row.clear !== 1) throw new Error('IMPORT_D1_READ_RECEIPT_REQUIRED');
  return row.clear === 1;
}

/** Complete admission separates already erased input from a later race.
 * Every matching digest rejects; stageClear also fences the transaction. */
export async function assertStagingIdentitiesClear(context:D1ImportContext,destination:string,body:string,phase:'admission'|'staging'):Promise<void>{
  const clear = await stagingPrivacyClear(context, sql("NOT EXISTS(SELECT 1 FROM json_each(?) i WHERE EXISTS(SELECT 1 FROM ik_d1_erased_sources s WHERE s.destination=? AND s.identity=json_extract(i.value,'$[2]')) OR EXISTS(SELECT 1 FROM ik_d1_erased_targets t WHERE t.destination=? AND t.identity=json_extract(i.value,'$[3]')))", body,destination,destination));
  if(!clear)throw new Error(phase==='admission'?'IMPORT_SOURCE_ERASED':'IMPORT_ATOMIC_GUARD');
}
