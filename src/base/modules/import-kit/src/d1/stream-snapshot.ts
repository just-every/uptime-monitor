import { createHash } from 'node:crypto';
import { canonicalJson } from '../provenance/identity.ts';
import type { ImportJob } from '../types.ts';
import type { D1ImportContext } from './types.ts';

/** Feed canonical tuples individually: no second whole-layout JSON string. */
export function layoutHash(rows: any[]): string {
  const hash = createHash('sha256'); hash.update('[');
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index]; if (index) hash.update(',');
    hash.update(canonicalJson({ ordinal: row.ordinal, job_id: row.job_id, total: row.total }));
  }
  hash.update(']'); return hash.digest('hex');
}
const natural = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
/** Interpret only the three rowsets from the same checked SQL transaction. */
export function streamJob(row: any, pages: any[], jobs: any[], context: D1ImportContext): ImportJob {
  if (!row) throw new Error('IMPORT_JOB_NOT_FOUND');
  if (row.destination !== context.destination || row.namespace !== context.namespace || row.mapping_version !== context.mappingVersion || row.principal_id !== context.authorization.principalId) throw new Error('JOB_BINDING_MISMATCH');
  if (!['applying','undoing'].includes(row.state) || !natural(row.total) || !natural(row.layout_revision) || !natural(row.progress_revision)) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  if (!pages.length || layoutHash(pages) !== row.layout_hash || pages.some((p, i) => p.stream_id !== row.id || p.ordinal !== i || !natural(p.total) || !natural(p.erased_records) || p.erased_records > p.total) || pages.reduce((n, p) => n + p.total, 0) !== row.total) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  if (jobs.length !== pages.length || jobs.some((j, i) => j.id !== pages[i].job_id || j.total !== pages[i].total || j.destination !== row.destination || j.namespace !== row.namespace || j.mapping_version !== row.mapping_version || j.principal_id !== row.principal_id || !natural(j.checkpoint) || j.checkpoint > j.total || !natural(j.fence) || !['queued','applying','completed','failed','undoing','undone','rolling_back','rolled_back'].includes(j.state))) throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  if (context.reversalPolicy !== undefined && jobs.some(j => j.reversal_policy !== context.reversalPolicy)) throw new Error('REVERSAL_POLICY_CONFLICT');
  const state = row.state === 'undoing' ? jobs.every(j => ['undone','rolled_back'].includes(j.state)) ? 'undone' : 'undoing' : jobs.every(j => j.state === 'completed') ? 'completed' : 'applying';
  return { id:row.id, destination:row.destination, namespace:row.namespace, mappingVersion:row.mapping_version, principalId:row.principal_id, fingerprint:row.fingerprint, total:row.total, checkpoint:jobs.reduce((n, j) => n+j.checkpoint,0), fence:jobs.reduce((n, j) => n+j.fence,0), state, error:jobs.find(j => j.error)?.error ?? null, erasedRecords:pages.reduce((n,p)=>n+p.erased_records,0) };
}
