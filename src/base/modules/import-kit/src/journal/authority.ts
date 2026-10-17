import type { ImportAction, ImportContext, ImportJob } from '../types.ts';
import { requireText } from '../provenance/identity.ts';
export function authorize(context: ImportContext, action: ImportAction): void {
  requireText(context.destination, 'destination'); requireText(context.namespace, 'namespace'); requireText(context.mappingVersion, 'mappingVersion'); requireText(context.authorization?.principalId, 'principalId');
  const result = context.authorization.assertAuthorized(action, context.destination, context.storage.database) as unknown;
  if (result && typeof (result as Promise<unknown>).then === 'function') throw new Error('ASYNC_AUTHORIZATION_FORBIDDEN');
  if (result === false) throw new Error('NOT_AUTHORIZED');
}
export function bindJob(job: ImportJob, context: ImportContext): void {
  if (job.destination !== context.destination || job.namespace !== context.namespace || job.mappingVersion !== context.mappingVersion || job.principalId !== context.authorization.principalId) throw new Error('JOB_BINDING_MISMATCH');
  if (context.reviewedFingerprint !== undefined && context.reviewedFingerprint !== job.fingerprint) throw new Error('REVIEW_FINGERPRINT_MISMATCH');
}
