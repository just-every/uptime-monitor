import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
import type { D1Database, D1Sql } from '../d1/types.ts';
import type { DoorPlanRow, DoorRequest, DoorSnapshot } from './types.ts';
import { DoorError } from './grants.ts';

export const sql = (sql: string, ...params: unknown[]): D1Sql => ({ sql, params });
export const statement = (db: D1Database, query: D1Sql) => db.prepare(query.sql).bind(...query.params ?? []);
export const assertion = (query: D1Sql) => sql(`INSERT INTO ik_d1_assertions(ok) SELECT CASE WHEN (${query.sql}) THEN 1 ELSE 0 END`, ...query.params ?? []);
export async function transact(db: D1Database, authority: D1Sql, queries: D1Sql[]): Promise<void> {
  await db.batch([assertion(authority), ...queries, assertion(authority), sql('DELETE FROM ik_d1_assertions')].map(q => statement(db, q)));
}
export async function authorized(request: DoorRequest): Promise<void> {
  const row = await statement(request.options.database, sql(`SELECT CASE WHEN (${request.authority.sql}) THEN 1 ELSE 0 END AS ok`, ...request.authority.params ?? [])).first();
  if (row?.ok !== 1) throw new DoorError('grant_revoked_or_expired');
}
export async function planRow(request: DoorRequest, id: string): Promise<DoorPlanRow> {
  const row = await request.options.database.prepare('SELECT * FROM ik_door_plans WHERE id=? AND installation_id=? AND namespace=? AND complete=1')
    .bind(id, request.grant.installationId, request.grant.namespace).first<DoorPlanRow>();
  if (!row) throw new DoorError('plan_not_found', 404);
  return row;
}
export async function loadSnapshot(request: DoorRequest, row: DoorPlanRow): Promise<DoorSnapshot> {
  const chunks = await request.options.database.prepare('SELECT ordinal,body FROM ik_door_plan_chunks WHERE plan_id=? ORDER BY ordinal').bind(row.id).all();
  if (!chunks.results.length || chunks.results.some((c, i) => c.ordinal !== i)) throw new DoorError('plan_custody_mismatch', 409);
  const snapshot = JSON.parse(chunks.results.map(c => c.body).join('')) as DoorSnapshot;
  if (sha256(canonicalJson(snapshot)) !== row.snapshot_hash || snapshot.id !== row.id || snapshot.namespace !== row.namespace || snapshot.bundleSha256 !== row.bundle_sha256
    || snapshot.previous !== row.previous_id || snapshot.plan.fingerprint !== row.fingerprint
    || planFingerprint(snapshot.plan) !== row.fingerprint
    || snapshot.plan.namespace !== snapshot.kitNamespace || snapshot.plan.destination !== request.options.destination
    || snapshot.plan.principalId !== request.options.ownerPrincipalId) throw new DoorError('plan_custody_mismatch', 409);
  return snapshot;
}
/** Recover indexing after a committed kit batch or an owner undo through the seed.
 * Kit state remains authoritative. Never hide a partially applied pending import. */
export async function lineage(request: DoorRequest): Promise<{ head_id: string | null; pending_id: string | null }> {
  const db = request.options.database, installation = request.grant.installationId, namespace = request.grant.namespace;
  let current = await db.prepare('SELECT head_id,pending_id FROM ik_door_namespaces WHERE installation_id=? AND namespace=?').bind(installation, namespace).first<{head_id:string|null;pending_id:string|null}>();
  if (!current) return { head_id: null, pending_id: null };
  for (;;) {
    const candidate = current.pending_id ?? current.head_id;
    if (!candidate) return current;
    const row = await db.prepare('SELECT p.previous_id,j.state FROM ik_door_plans p LEFT JOIN ik_jobs j ON j.id=p.job_id WHERE p.id=?').bind(candidate).first();
    let next: {head_id:string|null;pending_id:string|null} = current;
    if (current.pending_id && row?.state === 'completed') next = { head_id: candidate, pending_id: null };
    else if (row && ['undone', 'rolled_back'].includes(row.state)) next = { head_id: current.pending_id ? current.head_id : row.previous_id, pending_id: null };
    if (next === current) return current;
    await transact(db, request.authority, [sql('UPDATE ik_door_namespaces SET head_id=?,pending_id=? WHERE installation_id=? AND namespace=? AND head_id IS ? AND pending_id IS ?', next.head_id, next.pending_id, installation, namespace, current.head_id, current.pending_id)]);
    current = await db.prepare('SELECT head_id,pending_id FROM ik_door_namespaces WHERE installation_id=? AND namespace=?').bind(installation, namespace).first<typeof next>();
    if (!current) throw new DoorError('lineage_conflict', 409);
  }
}
export async function saveSnapshot(request: DoorRequest, snapshot: DoorSnapshot): Promise<void> {
  const db = request.options.database, g = request.grant;
  await transact(db, request.authority, [
    sql('INSERT OR IGNORE INTO ik_door_namespaces(installation_id,namespace) VALUES(?,?)', g.installationId, g.namespace),
    assertion(sql('EXISTS(SELECT 1 FROM ik_door_namespaces WHERE installation_id=? AND namespace=? AND head_id IS ? AND pending_id IS NULL)', g.installationId, g.namespace, snapshot.previous)),
    sql('INSERT INTO ik_door_plans(id,installation_id,namespace,bundle_sha256,fingerprint,previous_id,snapshot_hash) VALUES(?,?,?,?,?,?,?)', snapshot.id, g.installationId, g.namespace, snapshot.bundleSha256, snapshot.plan.fingerprint, snapshot.previous, sha256(canonicalJson(snapshot))),
    sql('UPDATE ik_door_namespaces SET pending_id=? WHERE installation_id=? AND namespace=?', snapshot.id, g.installationId, g.namespace),
  ]);
  // Private staging remains visibly pending until every immutable chunk is stored.
  // Small batches keep D1 bindings and transport bounded; no domain effect occurs.
  const body = canonicalJson(snapshot), chunks: D1Sql[] = [];
  for (let offset = 0, ordinal = 0; offset < body.length; ordinal++) {
    let end = Math.min(offset + 32_000,body.length);
    if (end < body.length && /[\uD800-\uDBFF]/.test(body[end-1])) end--;
    chunks.push(sql('INSERT INTO ik_door_plan_chunks(plan_id,ordinal,body) VALUES(?,?,?)', snapshot.id, ordinal, body.slice(offset,end)));
    offset = end;
  }
  for (let i = 0; i < chunks.length; i += 8) await transact(db, request.authority, chunks.slice(i, i + 8));
  await transact(db, request.authority, [sql('UPDATE ik_door_plans SET complete=1 WHERE id=?', snapshot.id)]);
}
