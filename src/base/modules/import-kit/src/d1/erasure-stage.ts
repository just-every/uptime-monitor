import type { D1FieldMatch, D1ImportContext, D1Sql } from './types.ts';
import type { ErasureFragment } from './erasure-fragments.ts';
import { ErasureCompiler } from './erasure-compiler.ts';
import { authority, finalAuthority, guard, prepare, readAtomic, readonlyChecks, sql, withinBatchBytes } from './database.ts';
import { D1RequestLimitError, packGuards, remaining } from './budget.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { nativeStageAtomic, nativeStageGuards, type NativeStageBinding } from './native-staging-state.ts';
import { beginNativeStage, discardNativeStage } from './native-staging-lifecycle.ts';
import { nativeStageCost } from './native-staging-admission.ts';
import { NativeStageRows, nativeRowStatements, verifyNativeStageOffset } from './native-staging-rows.ts';
import { nativePublication } from './native-staging-publication.ts';
import { stagingStatements } from './staging-statements.ts';
import { guardPredicate } from './guards.ts';
import { erasureIndexColumns, erasureValueColumns, privacyRows, valueRows } from './erasure-values.ts';
import { completedErasure, erasureRequestContext, operationIdentity, pendingErasure, progressChange, progressGuard, progressInsert, readErasureProgress, type D1ErasureStep, type D1ErasureStepOptions, type ErasureProgress } from './erasure-progress.ts';

/** Advanced product-owned custody rewrites use the same runner as retirement.
 * Fragments must describe authenticated normalized custody, in deterministic
 * order. The binding identifies the product's immutable erasure intent. */
export interface D1ErasureStageSpec {
  fragments: () => AsyncGenerator<ErasureFragment>;
  snapshotGuards: D1Sql[];
  targets: { kind: string; id: string; present: boolean }[];
  final: D1Sql[];
  fieldMatches?: D1FieldMatch[];
  resultKeys?: symbol[];
  direct?: boolean;
  survivingJobs?: (compiler: ErasureCompiler) => string[];
  publication?: (compiler: ErasureCompiler, receipt: { id: string; recordedAt: string }) => Promise<{ before: D1Sql[]; after: D1Sql[]; tail: D1Sql[] }>;
}
type Operation = { row?: ErasureProgress; requestHash: string; admissionOnly?: boolean };

export async function stageD1ImportErasure(input: D1ImportContext, options: D1ErasureStepOptions, binding: string,
  build: (id: string, context: D1ImportContext) => Promise<D1ErasureStageSpec>): Promise<D1ErasureStep> {
  const context = erasureRequestContext(input, options);
  await readAtomic(context, [], undefined, 'erase');
  const identity = operationIdentity(context, options, binding);
  const row = await readErasureProgress(context, identity.id, identity.requestHash);
  if (row?.phase === 'complete') return completedErasure(row);
  return executeErasureStage(context, identity.id, await build(identity.id, context), { row, requestHash: identity.requestHash, admissionOnly: options.admissionOnly });
}

/** Without an operation this retains the original one-invocation contract.
 * A journaled operation admits each step, not the sum of all future requests. */
export async function executeErasureStage(context: D1ImportContext, id: string, spec: D1ErasureStageSpec, operation?: Operation): Promise<D1ErasureStep> {
  const db = context.storage.database, compiler = new ErasureCompiler(context, id, spec.fragments);
  await compiler.inspect();
  const recordedAt = new Date().toISOString(), createdAt = operation?.row?.created_at ?? recordedAt;
  const own = await spec.publication?.(compiler, { id, recordedAt }) ?? { before: [], after: [], tail: [] };
  const fresh: ErasureProgress = { id, destination: context.destination, request_hash: operation?.requestHash ?? '', generation: '', phase: 'index', index_rows: 0, value_rows: 0, revision: 0, records_erased: compiler.erased, field_matches_json: canonicalJson(spec.fieldMatches ?? []), created_at: createdAt };
  const final = [...spec.final, ...compiler.erasedFinal];
  const resultKeys = spec.resultKeys ?? [];
  const resultsAt = (packed: D1Sql[]) => resultKeys.map(key => {
    const index = packed.findIndex(row => row.resultKey === key);
    if (index < 0) throw new Error('IMPORT_NATIVE_RESULT_MISSING');
    return index;
  });
  const complete = (row: ErasureProgress) => progressChange(row, { phase: 'complete', records_erased: compiler.erased, field_matches_json: fresh.field_matches_json });
  if (spec.direct !== false && compiler.direct && !operation?.row) {
    const { before, writes, post } = compiler.direct;
    const finished = { ...fresh, phase: 'complete' as const };
    const protection = context.storage.targets.stagingPublication?.(id, context.destination, spec.targets) ?? { protect: [], seal: [], release: [], final: [] };
    const snapshots = spec.snapshotGuards.map(row => guardPredicate(row) ? row : guard(row));
    const packed = [...packGuards([authority(context, 'erase'), ...snapshots, ...own.before, ...before, ...writes, ...own.after,
      ...protection.protect, ...post, ...protection.seal, ...protection.release, ...own.tail,
      ...operation ? [progressInsert(finished)] : [], authority(context, 'erase'), sql('DELETE FROM ik_d1_assertions')]), ...readonlyChecks([...post, ...final.map(guard), ...protection.final.map(guard)]), finalAuthority(context, 'erase')];
    if (packed.length <= remaining(context) && withinBatchBytes(context, packed)) {
      const indexes = resultsAt(packed);
      if (operation?.admissionOnly) return pendingErasure(fresh);
      const result = await db.batch(packed.map(row => prepare(db, row)));
      return completedErasure(finished, indexes.map(index => result[index]));
    }
  }
  if (compiler.unsafeOriginals) throw new Error('IMPORT_NATIVE_STAGE_ORIGINAL_BYTES_REFUSED');
  if (!context.storage.targets.stagingPublication || !spec.snapshotGuards.length) throw new Error('IMPORT_NATIVE_STAGING_ADAPTER_REQUIRED');
  const jobs = [...compiler.jobs.values()], anchor = jobs[0];
  if (!anchor) throw new Error('IMPORT_ERASURE_STAGE_ANCHOR_REQUIRED');
  const baseline = [...compiler.baseline(), ...spec.snapshotGuards.map(row => guardPredicate(row) ? row : guard(row))];
  const stage: NativeStageBinding = { id, generation: sha256(canonicalJson([compiler.fingerprints, baseline])), destination: context.destination, namespace: context.namespace, mapping_version: context.mappingVersion, principal_id: context.authorization.principalId, job_id: anchor.id, fingerprint: anchor.fingerprint, job_fence: anchor.fence, action: 'erase', values_count: compiler.values, chunks_count: compiler.payload.count, identities: compiler.indexes.count };
  fresh.generation = stage.generation;
  const current = operation?.row;
  const stored = current ? await db.prepare('SELECT * FROM ik_d1_native_staging WHERE id=?').bind(id).first() : null;
  const resumed = current && current.generation === stage.generation && stored && Object.keys(stage).every(key => stored[key] === stage[key as keyof NativeStageBinding]);
  let progress = resumed ? current! : fresh, writes = 0;
  const begin = [...operation ? current ? progressChange(current, { generation: fresh.generation, phase: 'index', index_rows: 0, value_rows: 0, revision: 0, records_erased: fresh.records_erased, field_matches_json: fresh.field_matches_json }) : [progressInsert(fresh)] : [], ...beginNativeStage(stage, baseline)];
  const registered = stage.identities, sealed = registered + stage.chunks_count;
  const registration = [...baseline, ...nativeStageGuards(stage, registered), guard(sql('(SELECT count(*) FROM ik_d1_native_staging_index WHERE stage_id=?)=?', id, registered)), sql('UPDATE ik_d1_native_staging SET registered_revision=revision WHERE id=?', id), ...baseline, ...nativeStageGuards(stage, registered)];
  const seal = [...baseline, ...nativeStageGuards(stage, sealed), guard(sql('EXISTS(SELECT 1 FROM ik_d1_native_staging WHERE id=? AND registered_revision IS NOT NULL) AND (SELECT count(*) FROM ik_d1_native_staging_values WHERE stage_id=?)=? AND (SELECT count(DISTINCT value_ordinal) FROM ik_d1_native_staging_values WHERE stage_id=?)=? AND NOT EXISTS(SELECT 1 FROM ik_d1_native_staging_values WHERE stage_id=? GROUP BY value_ordinal HAVING min(ordinal)<>0 OR max(ordinal)<>count(*)-1 OR min(value_chunks)<>count(*) OR max(value_chunks)<>count(*))', id, id, stage.chunks_count, id, stage.values_count, id)), sql('UPDATE ik_d1_native_staging SET sealed_revision=revision WHERE id=?', id), ...baseline, ...nativeStageGuards(stage, sealed)];
  const finished = { ...fresh, phase: 'sealed' as const, index_rows: registered, value_rows: stage.chunks_count, revision: sealed };
  const publication = nativePublication(context, stage, { baseline, writes: [...own.before, ...compiler.compiled.before, ...compiler.compiled.writes, ...own.after], post: compiler.compiled.post, jobs: spec.survivingJobs?.(compiler) ?? jobs.map(row => row.id), targets: spec.targets, binding: [], final, erasure: true });
  publication.release.push(...own.tail, ...operation ? complete(finished) : [], sql('DELETE FROM ik_d1_assertions'));
  const packed = stagingStatements(context, 'erase', publication.publication, publication.release, publication.final), indexes = resultsAt(packed);
  const publicationCost = nativeStageCost(context, stage, publication.publication, publication.release, publication.final);
  const checkpoint = (table: 'index' | 'values') => (from: { written: number; revision: number }, to: { written: number; revision: number }) => {
    const column = table === 'index' ? 'index_rows' : 'value_rows';
    return operation ? progressChange({ ...progress, [column]: from.written, revision: from.revision }, { [column]: to.written, revision: to.revision }) : [];
  };
  const indexCost = compiler.indexes.cost(context, stage, baseline, 'index', 0, checkpoint('index'));
  const valueCost = compiler.payload.cost(context, stage, baseline, 'values', registered, checkpoint('values'));
  const registrationCost = nativeStageCost(context, stage, registration), sealCost = nativeStageCost(context, stage, seal);
  if (!operation) {
    const cost = 2 * compiler.readQueries + indexCost + valueCost + [begin, discardNativeStage(stage)].reduce((n, rows) => n + nativeStageCost(context, stage, rows), 0) + registrationCost + sealCost + publicationCost;
    if (cost > remaining(context)) throw new D1RequestLimitError();
  } else {
    // At least one replay and useful flush must fit after inspection. Both
    // registration and sealing are independently journaled atomic steps.
    const flushCost = nativeStageCost(context, stage, nativeRowStatements(stage, baseline, 'values', erasureValueColumns, '[]', 1, 0, registered, checkpoint('values')));
    if (Math.max(compiler.readQueries + nativeStageCost(context, stage, begin) + flushCost + 2, publicationCost, registrationCost + 2, sealCost + 2) > remaining(context)) throw new D1RequestLimitError();
    if (operation.admissionOnly) return pendingErasure(progress);
  }
  const write = async (rows: D1Sql[], next: ErasureProgress, release: D1Sql[] = [], checks: D1Sql[] = []) => {
    await nativeStageAtomic(context, 'erase', rows, 0, release, checks); progress = next; writes++;
  };
  try {
    if (!resumed) await write(begin, fresh);
    for (const table of ['index', 'values'] as const) {
      if (table === 'index' ? progress.phase !== 'index' : progress.phase !== 'values') continue;
      const column = table === 'index' ? 'index_rows' : 'value_rows';
      const columns = table === 'index' ? erasureIndexColumns : erasureValueColumns;
      const offset = operation ? await verifyNativeStageOffset(context, stage, baseline, table, { written: progress[column], revision: progress.revision, guards: [progressGuard(progress)] }) : undefined;
      const writer = new NativeStageRows(context, stage, baseline, table, columns, { ...offset ? { offset } : {}, checkpoint: (from, to) => checkpoint(table)(from, to) });
      if (!operation) writer.revision = table === 'index' ? 0 : registered;
      try {
        await compiler.replay(async (fragment, retained, start, indexStart) => {
          for (const row of table === 'index' ? privacyRows(fragment, indexStart) : valueRows(retained, start)) await writer.append(row);
        });
        await writer.flush();
      } finally {
        if (writer.revision !== progress.revision) { progress = { ...progress, [column]: writer.written, revision: writer.revision }; writes++; }
      }
      const next = { ...progress, phase: table === 'index' ? 'values' as const : 'sealed' as const };
      await write([...(table === 'index' ? registration : seal), ...operation ? progressChange(progress, { phase: next.phase }) : []], next);
    }
    const result = await nativeStageAtomic(context, 'erase', publication.publication, 0, publication.release, publication.final);
    return completedErasure({ ...finished, phase: 'complete' }, indexes.map(index => result[index]));
  } catch (error) {
    if (operation) {
      if (error instanceof D1RequestLimitError && writes) return pendingErasure(progress);
      // A provider may have committed before losing its acknowledgement. Keep
      // durable progress so a new invocation verifies the actual checkpoint.
      throw error;
    }
    await nativeStageAtomic(context, 'erase', discardNativeStage(stage));
    throw error;
  }
}
