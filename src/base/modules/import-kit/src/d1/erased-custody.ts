import type { ImportPlan } from '../types.ts';
import type { D1Sql } from './types.ts';
import type { InventorySnapshot } from './inventory-snapshot.ts';
import { canonicalJson } from '../provenance/identity.ts';
import { sql } from './database.ts';
import { originalStorage, originalVersions } from './original-storage.ts';
import { insertJsonRows } from './sets.ts';
import { changed, deleteRows } from './expected-state.ts';
import { recordWrites } from './erasure-writes.ts';
import { withdrawnLocation } from './erased-artifacts.ts';

/** Original bytes are only deleted, never rewritten. Unaffected custody remains
 * byte-for-byte in its existing storage version; postguards prove the new set. */
export function erasedCustody(plan: ImportPlan, next: ImportPlan, id: string, snapshot: InventorySnapshot): D1Sql[] {
  const version = originalVersions.get(plan)!;
  originalVersions.set(next, version);
  const after = originalStorage(next, id, version), writes: D1Sql[] = [];
  for (const [index, artifact] of plan.source.artifacts.entries()) {
    if (artifact.name === next.source.artifacts[index].name) continue;
    const key = canonicalJson([artifact.name, artifact.sha256]);
    for (const table of ['ik_d1_original_chunks', 'ik_originals']) writes.push(changed(sql(`DELETE FROM ${table} WHERE job_id=? AND kind='artifact' AND original_key=?`, id, key), deleteRows(table, row => row.job_id === id && row.kind === 'artifact' && row.original_key === key)));
    const replacement = after.rows.find(row => row.kind === 'artifact' && row.name === next.source.artifacts[index].name)!;
    writes.push(...insertJsonRows('ik_originals', ['job_id','kind','original_key','name','sha256','size','bytes'], [replacement]));
  }
  // Peers can share an artifact whose name contains the erased person's data.
  // Preserve attribution and before-images, changing only that location metadata.
  for (const table of ['ik_provenance', 'ik_d1_history']) {
    const rows: { id: string; body: string }[] = [];
    for (const row of snapshot.rows(table, 'job_id', [id])) {
      const record = JSON.parse(row.record_json), location = withdrawnLocation(plan, next, record.location, record.artifactHash);
      if (location !== record.location) rows.push({ id: row.id, body: canonicalJson({ ...record, location }) });
    }
    writes.push(...recordWrites(table, rows));
  }
  return writes;
}
