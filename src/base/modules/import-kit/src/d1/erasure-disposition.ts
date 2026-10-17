import type { D1UndoDisposition, D1UndoEvidence } from './undo-types.ts';
import { canonicalJson } from '../provenance/identity.ts';

const emptyEvidence = (): D1UndoEvidence => ({ revision: null, valueHash: null, attachmentHash: null, ownedHash: null, ownedValue: null });
export function erasedDisposition(d: Pick<D1UndoDisposition, 'jobId'|'effectId'|'disposition'|'reason'>, identity: string): D1UndoDisposition {
  return { jobId: d.jobId, effectId: d.effectId, targetKind: 'erased', targetId: d.effectId, entity: { table: 'erased', recordId: d.effectId }, disposition: d.disposition, reason: d.reason, expected: emptyEvidence(), observed: emptyEvidence(), protectedBy: [], evidence: null, redacted: 'erased', sourceIdentity: identity };
}
export function redactDisposition(d: D1UndoDisposition): D1UndoDisposition {
  if (d.redacted === 'erased') return d;
  return { jobId: d.jobId, effectId: d.effectId, targetKind: 'redacted', targetId: d.effectId, entity: { table: 'redacted', recordId: d.effectId }, disposition: d.disposition, reason: d.reason,
    expected: emptyEvidence(), observed: emptyEvidence(),
    protectedBy: d.reason === 'dependency' ? [{ requiredEffectId: d.effectId, cause: { table: 'redacted', recordId: d.effectId }, evidence: null }] : [], evidence: null, redacted: 'evidence' };
}
export function redactedEvidenceMatches(d: D1UndoDisposition): boolean {
  return d.redacted === 'evidence' && canonicalJson(d) === canonicalJson(redactDisposition(d));
}
