import type { D1ImportContext, D1Sql } from './types.ts';
import { guard, sql } from './database.ts';
import { jsonChunks } from './sets.ts';

/** Privacy authority belongs to the destination, including other importing
 * principals. Only hash identities are read; no private payload is exposed. */
export async function stagedSourceIdentities(digest: string, context: D1ImportContext): Promise<string[]> {
  const { results } = await context.storage.database.prepare('SELECT DISTINCT i.source_identity FROM ik_d1_staging_index i JOIN ik_d1_staging s ON s.id=i.stage_id WHERE s.destination=? AND i.target_identity=? UNION SELECT DISTINCT i.source_identity FROM ik_d1_native_staging_index i JOIN ik_d1_native_staging s ON s.id=i.stage_id WHERE s.destination=? AND i.target_identity=?').bind(context.destination, digest, context.destination, digest).all<{ source_identity: string }>();
  return results.map(row => row.source_identity);
}
export function stageErasure(destination: string, digest: string, identities: string[], ownStage?:string): { before: D1Sql[]; after: D1Sql[]; final:D1Sql[] } {
  // This closure guard catches an index registration or publication race that
  // introduces a source not covered by the public-plan snapshot above.
  const chunks = jsonChunks(identities, 1500000);
  const listed = chunks.length ? chunks.map(() => 'i.source_identity IN (SELECT value FROM json_each(?))').join(' OR ') : '0';
  const before:D1Sql[]=[],after:D1Sql[]=[],final:D1Sql[]=[];
  for(const table of ['ik_d1_staging','ik_d1_native_staging']){
    before.push(guard(sql(`NOT EXISTS(SELECT 1 FROM ${table}_index i JOIN ${table} s ON s.id=i.stage_id WHERE s.destination=? AND i.target_identity=? AND NOT (${listed}))`,destination,digest,...chunks)));
    const exclude=ownStage&&table==='ik_d1_native_staging'?' AND id<>?':'', extra=exclude?[ownStage]:[];
    for(const chunk of chunks){
      after.push(sql(`DELETE FROM ${table} WHERE destination=? AND id IN (SELECT stage_id FROM ${table}_index WHERE source_identity IN (SELECT value FROM json_each(?)))${exclude}`,destination,chunk,...extra));
      final.push(sql(`NOT EXISTS(SELECT 1 FROM ${table} s JOIN ${table}_index i ON i.stage_id=s.id WHERE s.destination=? AND i.source_identity IN (SELECT value FROM json_each(?)))`,destination,chunk));
    }
    after.push(sql(`DELETE FROM ${table} WHERE destination=? AND id IN (SELECT stage_id FROM ${table}_index WHERE target_identity=?)${exclude}`,destination,digest,...extra));
    final.push(sql(`NOT EXISTS(SELECT 1 FROM ${table} s JOIN ${table}_index i ON i.stage_id=s.id WHERE s.destination=? AND i.target_identity=?)`,destination,digest));
  }
  return { before, after, final };
}
