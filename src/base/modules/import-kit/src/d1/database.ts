import { d1StreamOriginSchema } from './stream-origin.ts';
import { d1PreparationSchema } from './preparation-schema.ts';
import { d1PreparationStepsSchema } from './preparation-steps-schema.ts';
import { stageNativeTransaction } from './native-staging.ts';
import type { NativeStagingScope } from './native-staging-context.ts';
import { d1NativeStagingSchema } from './native-staging-schema.ts';
import { d1CustodyStateSchema } from './custody-state-schema.ts';
import { d1ReversalFenceSchema } from './reversal-fence-schema.ts';
import { batchBytes } from './transport.ts';
import { D1RequestLimitError, packGuards, remaining } from './budget.ts';
import { d1ErasureSchema } from './erasure-schema.ts';
import { d1RetirementSchema } from './retirement-schema.ts';
import { d1ErasureProgressSchema } from './erasure-progress-schema.ts';
import { schema } from '../storage/schema.ts';
import { requireText } from '../provenance/identity.ts';
import type { ImportAction } from '../types.ts';
import type { D1Database, D1ImportContext, D1Sql, D1Statement } from './types.ts';
import { guard, guardPredicate } from './guards.ts';
import { batchRows, checkBatchResults } from './read-results.ts';
import { streamBoundary } from './stream-boundary.ts';
import { jobLayoutGuards } from './stream-layout.ts';
export { guard } from './guards.ts';
import { preservedUndoEvolution } from './evolutions/0001-preserved-undo.ts';
import { d1StagingSchema, d1LegacyStreamReceiptUpgrade } from './staging-schema.ts';
import { ExpectedState } from './expected-state.ts';
import { custodyGuards } from './original-storage.ts';
import { planGuards } from './plans.ts';
import { planIndex,erasedIndexGuards } from './erasure-index.ts';
import { exactRows } from './sets.ts';
import { readBoundedGuards } from './read-fence.ts';

const baseSchema = [...schema.split(';').map(s => s.trim()).filter(Boolean),
  'CREATE TABLE IF NOT EXISTS ik_d1_plan_chunks (job_id TEXT NOT NULL REFERENCES ik_jobs(id), ordinal INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(job_id,ordinal))',
  'CREATE TABLE IF NOT EXISTS ik_d1_original_chunks (job_id TEXT NOT NULL REFERENCES ik_jobs(id), kind TEXT NOT NULL, original_key TEXT NOT NULL, ordinal INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(job_id,kind,original_key,ordinal))',
  'CREATE TABLE IF NOT EXISTS ik_d1_history (id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES ik_jobs(id), ordinal INTEGER NOT NULL, effect_ordinal INTEGER NOT NULL, source_identity TEXT NOT NULL, destination TEXT NOT NULL, target_kind TEXT NOT NULL, target_id TEXT NOT NULL, reversed INTEGER NOT NULL CHECK(reversed=0), record_json TEXT NOT NULL, successor_id TEXT NOT NULL UNIQUE)',
  'CREATE INDEX IF NOT EXISTS ik_d1_history_job ON ik_d1_history(job_id)',
  'CREATE TABLE IF NOT EXISTS ik_d1_entries (job_id TEXT NOT NULL REFERENCES ik_jobs(id), ordinal INTEGER NOT NULL, source_identity TEXT NOT NULL, owner_job_id TEXT NOT NULL REFERENCES ik_jobs(id), effects_hash TEXT NOT NULL, PRIMARY KEY(job_id,ordinal))',
  'CREATE TABLE IF NOT EXISTS ik_d1_assertions (ok INTEGER NOT NULL CONSTRAINT IMPORT_ATOMIC_GUARD CHECK(ok=1))'];
export const d1Schema = [...baseSchema, ...preservedUndoEvolution];
export { preservedUndoEvolution };
export async function initializeD1ImportStorage(database: D1Database): Promise<void> {
  await database.batch(baseSchema.map(sql => database.prepare(sql)));
  const columns = await database.prepare('PRAGMA table_info(ik_jobs)').all();
  const installed = ['reversal_policy','reversal_policy_hash','reversal_cursor','reversal_hash'].filter(name => columns.results.some(c => c.name === name));
  const report = await database.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='ik_d1_reversal_reports'").first();
  const creation = await database.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='ik_d1_creation_plans'").first();
  const triggers = await database.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type='trigger' AND name IN ('ik_d1_creation_plan_capture','ik_d1_creation_plan_immutable','ik_d1_creation_plan_release')").first();
  if (!installed.length && !report && !creation && triggers?.n === 0) await database.batch(preservedUndoEvolution.map(sql => database.prepare(sql)));
  else if (installed.length !== 4 || !report || !creation || triggers?.n !== 3) throw new Error('IMPORT_SCHEMA_EVOLUTION_INCOMPLETE');
  await database.batch(d1ErasureSchema.map(sql => database.prepare(sql)));
  await database.batch([...d1StreamOriginSchema, ...d1LegacyStreamReceiptUpgrade].map(sql => database.prepare(sql)));
  await database.batch(d1StagingSchema.map(sql => database.prepare(sql)));
  await database.batch(d1NativeStagingSchema.map(sql => database.prepare(sql)));
  await database.batch(d1CustodyStateSchema.map(sql => database.prepare(sql)));
  await database.batch(d1ReversalFenceSchema.map(sql => database.prepare(sql)));
  await database.batch(d1PreparationSchema.map(sql => database.prepare(sql)));
  await database.batch(d1PreparationStepsSchema.map(sql => database.prepare(sql)));
  await database.batch(d1RetirementSchema.map(sql => database.prepare(sql)));
  await database.batch(d1ErasureProgressSchema.map(sql => database.prepare(sql)));
}
export const sql = (sql: string, ...params: any[]): D1Sql => ({ sql, params });
export const prepare = (db: D1Database, query: D1Sql): D1Statement => db.prepare(query.sql).bind(...(query.params ?? []));

export function authority(context: Pick<D1ImportContext, 'destination' | 'namespace' | 'mappingVersion' | 'authorization'>, action: ImportAction): D1Sql {
  for (const name of ['destination', 'namespace', 'mappingVersion'] as const) requireText(context[name], name);
  requireText(context.authorization.principalId, 'principalId');
  const predicate = context.authorization.predicate(action, context.destination);
  if (!predicate || typeof predicate.sql !== 'string' || !predicate.sql.trim()) throw new Error('AUTHORIZATION_PREDICATE_REQUIRED');
  return guard(predicate);
}
/** Final checks are read-only: no assertion cleanup can mutate state afterward.
 * SQLite evaluates only the selected CASE branch; malformed JSON aborts the
 * whole batch when the exact predicate is false or NULL. */
export function finalGuard(predicate: D1Sql): D1Sql {
  predicate=guardPredicate(predicate)??predicate;
  const prefix="SELECT CASE WHEN (",suffix=") THEN 1 ELSE json('IMPORT_FINAL_GUARD') END";
  const result=sql(prefix+predicate.sql+suffix,...predicate.params??[]);
  if(predicate.retainedValues){const parts=[...predicate.retainedValues.parts];parts[0]=prefix+parts[0];parts[parts.length-1]+=suffix;result.retainedValues={...predicate.retainedValues,parts,indexes:[...predicate.retainedValues.indexes]};}
  return result;
}
export function finalAuthority(context: Parameters<typeof authority>[0], action: ImportAction): D1Sql {
  authority(context,action); // Validate the authorization contract as usual.
  return finalGuard(context.authorization.predicate(action,context.destination));
}
export const readonlyChecks = (guards:D1Sql[]):D1Sql[] => packGuards(guards).map(finalGuard);
export function atomicStatements(context: Parameters<typeof authority>[0] & Pick<D1ImportContext,'maxBatchBytes'>, action: ImportAction, statements: D1Sql[]): D1Sql[] {
  return [...packGuards([authority(context, action), ...statements, authority(context, action), sql('DELETE FROM ik_d1_assertions')]),...(context.maxBatchBytes===undefined?[]:[finalAuthority(context,action)])];
}
export async function atomic(context: D1ImportContext, action: ImportAction, statements: D1Sql[], returnReads = 0, scope?: NativeStagingScope): Promise<void> {
  const {packed,post,count}=atomicPreparation(context,action,statements,scope,returnReads);
  if (count > remaining(context)) throw new D1RequestLimitError();
  if (!withinBatchBytes(context, packed)) {
    if (!scope) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
    await stageNativeTransaction(context, action, statements, {...scope,postGuards:post}, returnReads); return;
  }
  await context.storage.database.batch(packed.map(s => prepare(context.storage.database, s)));
}
function postConditions(context:Pick<D1ImportContext,'maxBatchBytes'>,statements:D1Sql[],scope?:NativeStagingScope,action:ImportAction='read'):D1Sql[]{
  const stream = scope ? [...jobLayoutGuards(scope.job), ...streamBoundary(scope.job.id, action)] : [];
  if(context.maxBatchBytes===undefined)return [...stream,...statements.flatMap(statement=>statement.finalChecks??[])];
  const readOnly=statements.every(s=>guardPredicate(s));
  const post=[...stream,...(readOnly?statements:new ExpectedState(statements).guards()),...statements.flatMap(statement=>statement.finalChecks??[])];
  if(scope){
    const plans=new Map([[scope.job.id,scope.plan]]);
    for(const statement of statements){const values=statement.retainedValues;if(values?.privacyPlan&&values.privacyJobId&&!plans.has(values.privacyJobId))plans.set(values.privacyJobId,values.privacyPlan);}
    for(const [id,plan] of plans)post.push(...erasedIndexGuards(plan),...custodyGuards(plan,id),...planGuards(plan,id),...exactRows('ik_d1_plan_index',['job_id','ordinal','source_identity','target_identity'],planIndex(plan,id),sql('job_id=?',id),undefined,true),guard(sql('EXISTS(SELECT 1 FROM ik_d1_indexed_jobs WHERE job_id=? AND fingerprint=?)',id,plan.fingerprint)));
    post.push(...scope.postGuards);
  }
  return post;
}
export function withinBatchBytes(context: D1ImportContext, packed: D1Sql[]): boolean {
  const limit = context.maxBatchBytes;
  if (limit === undefined) return true;
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error('INVALID_TRANSPORT_LIMIT');
  return batchBytes(packed) <= limit;
}
/** Includes both current-authority predicates and the eventual progress write. */
export const fitsBatch = (context:D1ImportContext,action:ImportAction,statements:D1Sql[],scope?:NativeStagingScope):boolean => context.maxBatchBytes===undefined||withinBatchBytes(context,preparedAtomic(context,action,statements,scope));
export function preparedAtomic(context:Parameters<typeof authority>[0]&Pick<D1ImportContext,'maxBatchBytes'>,action:ImportAction,statements:D1Sql[],scope?:NativeStagingScope):D1Sql[]{
  return atomicPreparation(context,action,statements,scope).packed;
}
/** The statements one atomic() request spends: the packed writes and guards,
 * authority, post-conditions with the scope's guards, final authority and the
 * return reads. Planners size a batch with this same count before sending it. */
export const atomicCount = (context:Parameters<typeof authority>[0]&Pick<D1ImportContext,'maxBatchBytes'>,action:ImportAction,statements:D1Sql[],returnReads:number,scope?:NativeStagingScope):number => atomicPreparation(context,action,statements,scope,returnReads).count;
function atomicPreparation(context:Parameters<typeof authority>[0]&Pick<D1ImportContext,'maxBatchBytes'>,action:ImportAction,statements:D1Sql[],scope?:NativeStagingScope,returnReads=0){
  const post=postConditions(context,statements,scope,action);
  const packed=[...atomicStatements(context,action,statements),...readonlyChecks(post),...(post.length?[finalAuthority(context,action)]:[])];
  return {post,packed,count:packed.length+returnReads};
}
/** SELECT-only fence: inspect every packed scalar before publishing snapshots.
 * Mutations continue to use atomic() and SQL CHECK rollback, never this path. */
export async function readAtomic(context: D1ImportContext, guards: D1Sql[], scope?: NativeStagingScope, action: ImportAction = 'read'): Promise<void> {
  const checks = [...guards, ...postConditions(context, guards, scope, action)];
  const packed = packGuards([authority(context, action), ...checks, authority(context, action)], true);
  const limit = context.maxBatchBytes;
  if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) throw new Error('INVALID_TRANSPORT_LIMIT');
  if (limit !== undefined && !withinBatchBytes({...context,maxBatchBytes:Math.min(limit,2*1024*1024)}, packed)) {
    if (!scope) throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
    await readBoundedGuards(context, checks, scope, Math.min(limit,2*1024*1024), action);
    return;
  }
  if (packed.length > remaining(context)) throw new D1RequestLimitError();
  const results = await context.storage.database.batch(packed.map(s => prepare(context.storage.database, s)));
  checkBatchResults(results, packed.length, 'IMPORT_ATOMIC_GUARD');
  for (const result of results) {
    const rows = batchRows(result, 'IMPORT_ATOMIC_GUARD');
    if (rows.length !== 1 || rows[0].ok !== 1) throw new Error('IMPORT_ATOMIC_GUARD');
  }
}
export async function authorize(context: D1ImportContext, action: ImportAction): Promise<void> {
  if (action === 'read' || action === 'plan') await readAtomic(context, [], undefined, action);
  else await atomic(context, action, []);
}
export function limits(context: D1ImportContext): { batchSize: number; maxBatches: number } {
  const batchSize = context.batchSize ?? 100, maxBatches = context.maxBatches ?? Number.MAX_SAFE_INTEGER;
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 1000 || !Number.isSafeInteger(maxBatches) || maxBatches < 1) throw new Error('INVALID_BATCH_LIMIT');
  return { batchSize, maxBatches };
}
