import type { D1Database, D1Sql } from '../d1/types.ts';
import type { ImportAction } from '../types.ts';
import type { DoorRequest, DoorSnapshot, ImportDoorOptions } from './types.ts';
import { DoorError, verifyImportGrant, type DoorAction } from './grants.ts';
import { assertion, sql, statement, transact } from './store.ts';

export async function enabled(options: ImportDoorOptions): Promise<boolean> {
  const row = await options.database.prepare('SELECT enabled FROM ik_door_settings WHERE installation_id=?').bind(options.claim.installationId).first();
  return row?.enabled === 1;
}
export async function admit(options: ImportDoorOptions, request: Request, action: DoorAction): Promise<DoorRequest> {
  const authorization = request.headers.get('Authorization');
  if (!authorization?.startsWith('Bearer ')) throw new DoorError('grant_required', 401);
  const {grant, kid} = await verifyImportGrant(authorization.slice(7), options.claim);
  if (grant.action !== action || new URL(request.url).origin !== options.claim.appOrigin) throw new DoorError('grant_binding_mismatch');
  const basic = sql(`EXISTS(SELECT 1 FROM ik_door_settings WHERE installation_id=? AND enabled=1)
    AND NOT EXISTS(SELECT 1 FROM ik_door_revoked_keys WHERE installation_id=? AND kid=?)
    AND unixepoch() < ?`, grant.installationId, grant.installationId, kid, grant.exp);
  try {
    await transact(options.database, basic, [sql('INSERT INTO ik_door_grants(installation_id,jti,action,namespace,kid,expires) VALUES(?,?,?,?,?,?)', grant.installationId, grant.jti, action, grant.namespace, kid, grant.exp)]);
  } catch { throw new DoorError('grant_replayed_revoked_or_expired'); }
  const authority = sql(`(${basic.sql}) AND EXISTS(SELECT 1 FROM ik_door_grants WHERE installation_id=? AND jti=? AND action=? AND namespace=? AND kid=? AND expires=?)`, ...basic.params ?? [], grant.installationId, grant.jti, action, grant.namespace, kid, grant.exp);
  return {options, grant, kid, authority};
}
const actions: Record<DoorAction, ImportAction[]> = {
  uploads: [], plan: ['plan', 'read'], apply: ['create', 'apply', 'read'],
  status: ['read'], reconcile: ['read'], undo: ['read', 'undo', 'rollback'],
};
export function kitContext(request: DoorRequest, snapshot: Pick<DoorSnapshot, 'kitNamespace'|'plan'|'id'>) {
  const {options, grant} = request;
  return {
    destination: options.destination, namespace: snapshot.kitNamespace,
    mappingVersion: snapshot.plan.mappingVersion,
    operationKey: 'door:' + snapshot.id,
    ...(grant.action === 'apply' ? {reviewedFingerprint: grant.fingerprint} : {}),
    batchSize: 20, maxBatches: 1,
    storage: {database: options.database, targets: options.targets},
    authorization: {
      principalId: options.ownerPrincipalId,
      predicate(action: ImportAction, destination: string): D1Sql {
        if (destination !== options.destination || !actions[grant.action].includes(action)) return sql('0');
        const fingerprint = grant.action === 'apply' ? sql(`EXISTS(SELECT 1 FROM ik_door_plans p JOIN ik_door_approvals a ON a.plan_id=p.id
          WHERE p.id=? AND p.fingerprint=? AND p.complete=1 AND a.approval_id=? AND a.owner=? AND a.approved_at=? AND a.fingerprint=p.fingerprint)`, snapshot.id, grant.fingerprint, grant.approvalRecordId, grant.email, grant.approvedAt) : sql('1');
        return sql(`(${request.authority.sql}) AND (${fingerprint.sql})`, ...request.authority.params ?? [], ...fingerprint.params ?? []);
      },
    },
  };
}
/** Server-only integration hook. The predicate must consult the seed's CURRENT
 * owner session or the owner's provisioning approval in this same database. */
export async function writeImportDoorSettings(db: D1Database, installationId: string, changes: {enabled: boolean; revokedKids?: string[]}, ownerPredicate: D1Sql): Promise<void> {
  if (!installationId || typeof changes.enabled !== 'boolean' || changes.revokedKids?.some(k => !/^[A-Za-z0-9_-]{1,64}$/.test(k))) throw new DoorError('invalid_settings', 400);
  await transact(db, ownerPredicate, [
    sql('INSERT INTO ik_door_settings(installation_id,enabled) VALUES(?,?) ON CONFLICT(installation_id) DO UPDATE SET enabled=excluded.enabled', installationId, changes.enabled ? 1 : 0),
    ...(changes.revokedKids ?? []).map(kid => sql('INSERT OR IGNORE INTO ik_door_revoked_keys(installation_id,kid) VALUES(?,?)', installationId, kid)),
  ]);
}
/** Use alongside the seed's existing owner-bound import list and undo route. */
export async function readImportDoorHistory(db: D1Database, installationId: string, ownerPredicate: D1Sql) {
  const query = sql(`SELECT p.id AS planId,p.job_id AS jobId,p.namespace,p.fingerprint,j.state,
    a.owner,a.approved_at AS approvedAt,a.approval_id AS approvalRecordId,a.reviewer
    FROM ik_door_plans p JOIN ik_door_approvals a ON a.plan_id=p.id JOIN ik_jobs j ON j.id=p.job_id
    WHERE p.installation_id=? ORDER BY a.approved_at DESC,p.id`, installationId);
  const results = await db.batch([
    statement(db, sql(`SELECT CASE WHEN (${ownerPredicate.sql}) THEN 1 ELSE json('IMPORT_DOOR_OWNER_REQUIRED') END`, ...ownerPredicate.params ?? [])),
    statement(db, query),
    statement(db, sql(`SELECT CASE WHEN (${ownerPredicate.sql}) THEN 1 ELSE json('IMPORT_DOOR_OWNER_REQUIRED') END`, ...ownerPredicate.params ?? [])),
  ]);
  const rows = (results[1] as {results: Record<string, unknown>[]} | Record<string, unknown>[]);
  return (Array.isArray(rows) ? rows : rows.results).map(row => ({...row, caption: `approved via Business by ${row.owner} at ${row.approvedAt}, fingerprint ${row.fingerprint}`}));
}
export async function reserveApply(request: DoorRequest, snapshot: DoorSnapshot): Promise<void> {
  const g = request.grant;
  if (snapshot.standingApproval && (g.catchup !== 'bounded' || g.approvalRecordId !== snapshot.standingApproval.id || g.approvedAt !== snapshot.standingApproval.at)) throw new DoorError('standing_approval_mismatch');
  if (!snapshot.standingApproval && g.catchup === 'bounded') throw new DoorError('standing_approval_mismatch');
  await transact(request.options.database, request.authority, [
    assertion(sql('EXISTS(SELECT 1 FROM ik_door_namespaces WHERE installation_id=? AND namespace=? AND pending_id=? AND head_id IS ?)', g.installationId, g.namespace, snapshot.id, snapshot.previous)),
    sql('INSERT OR IGNORE INTO ik_door_approvals(plan_id,approval_id,owner,approved_at,fingerprint,reviewer) VALUES(?,?,?,?,?,?)', snapshot.id, g.approvalRecordId, g.email, g.approvedAt, g.fingerprint, snapshot.standingApproval?.id ?? g.approvalRecordId),
    assertion(sql('EXISTS(SELECT 1 FROM ik_door_approvals WHERE plan_id=? AND approval_id=? AND owner=? AND approved_at=? AND fingerprint=?)', snapshot.id, g.approvalRecordId, g.email, g.approvedAt, g.fingerprint)),
  ]);
}
