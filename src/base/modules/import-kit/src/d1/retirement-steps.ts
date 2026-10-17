import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';
import type { D1Database, D1FieldMatch, D1FieldRedaction, D1Sql } from './types.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';
import { sql } from './database.ts';

/** The most jobs one resumable step reads, which bounds its CPU. A step takes
 * fewer when they do not fit one request's statements (see redaction-step.ts). */
export const STEP_JOBS = 25;

/** A job too large for one step is redacted in pages of `count` entries; the
 * next page starts at `from`. `revision` is the job's custody revision after
 * the last page, so a job changed in between starts again from its first entry. */
export interface RedactionPage { job: string; from: number; count: number; revision: number | null }
/** Where a resumable redaction stands: the jobs already read and found free of
 * the selected values (or rewritten), each at the custody revision it then had,
 * and the matches counted so far. A changed revision makes a job pending again.
 * `span` is how many jobs the next step reads, and `page` a job being redacted
 * in pages. It holds job ids, revisions, counts, sizes and a salted digest of
 * the selectors, never a selected value; it is bound to the destination and
 * selectors it began with. */
export interface RedactionProgress { nonce: string; digest: string; clean: Map<string, number>; matched: D1FieldMatch[]; span: number; page?: RedactionPage }

const digestOf = (nonce: string, destination: string, fields: readonly D1FieldRedaction[]) =>
  sha256(canonicalJson([nonce, destination, fields.map(({ path, value, optional }) => [path, value, optional === true])]));

export function readProgress(continuation: string | undefined, destination: string, fields: readonly D1FieldRedaction[]): RedactionProgress {
  if (continuation === undefined) {
    const nonce = randomUUID();
    return { nonce, digest: digestOf(nonce, destination, fields), clean: new Map(), matched: [], span: STEP_JOBS };
  }
  let token: any;
  try { token = JSON.parse(Buffer.from(continuation, 'base64url').toString('utf8')); } catch { throw new Error('IMPORT_REDACTION_CONTINUATION_INVALID'); }
  if (token?.v !== 1 || typeof token.nonce !== 'string' || !Array.isArray(token.clean) || !Array.isArray(token.matched)
    || !token.clean.every((item: unknown) => Array.isArray(item) && typeof item[0] === 'string' && Number.isSafeInteger(item[1]))
    || token.span !== undefined && !(Number.isSafeInteger(token.span) && token.span >= 1 && token.span <= STEP_JOBS)
    || token.page !== undefined && !(typeof token.page?.job === 'string' && [token.page.from, token.page.count].every(n => Number.isSafeInteger(n) && n >= 0) && token.page.count >= 1
      && (token.page.revision === null || Number.isSafeInteger(token.page.revision)))) throw new Error('IMPORT_REDACTION_CONTINUATION_INVALID');
  // A continuation from another erasure (other selectors or destination) must
  // never let this one skip a job.
  if (token.digest !== digestOf(token.nonce, destination, fields)) throw new Error('IMPORT_REDACTION_CONTINUATION_MISMATCH');
  const { job, from, count, revision } = token.page ?? {};
  return { nonce: token.nonce, digest: token.digest, clean: new Map(token.clean), matched: token.matched, span: token.span ?? STEP_JOBS, ...token.page ? { page: { job, from, count, revision } } : {} };
}

export function writeProgress(progress: RedactionProgress): string {
  return Buffer.from(JSON.stringify({ v: 1, nonce: progress.nonce, digest: progress.digest, clean: [...progress.clean], matched: progress.matched, span: progress.span, ...progress.page ? { page: progress.page } : {} }), 'utf8').toString('base64url');
}

export function addMatches(total: D1FieldMatch[], step: readonly D1FieldMatch[]): D1FieldMatch[] {
  const result = total.map(item => ({ path: [...item.path], matches: item.matches }));
  for (const item of step) {
    const prior = result.find(count => canonicalJson(count.path) === canonicalJson(item.path));
    if (prior) prior.matches += item.matches; else result.push({ path: [...item.path], matches: item.matches });
  }
  return result;
}

/** A job of the destination with its custody revision and entry count. */
export interface JobRevision { id: string; revision: number | null; total: number }
/** Every job of the destination with its custody revision, read before any plan,
 * so a change after the read always shows as a newer revision. */
export async function destinationRevisions(db: D1Database, destination: string): Promise<JobRevision[]> {
  return (await db.prepare('SELECT j.id AS id,s.revision AS revision,j.total AS total FROM ik_jobs j LEFT JOIN ik_d1_custody_states s ON s.job_id=j.id WHERE j.destination=? ORDER BY j.id').bind(destination).all<JobRevision>()).results;
}

/** Read inside a step's batch, after its writes: the revisions it leaves. */
export const revisionsAfter = (jobs: string[]): D1Sql => sql('SELECT job_id,revision FROM ik_d1_custody_states WHERE job_id IN (SELECT value FROM json_each(?))', JSON.stringify(jobs));
