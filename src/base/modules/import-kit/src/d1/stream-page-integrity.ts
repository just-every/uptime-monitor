import type { ImportPlan } from '../types.ts';
import type { UnverifiedStreamPages } from './stream-plan.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { canonicalJson } from '../provenance/identity.ts';
/** The complete reviewed view contains global metadata. Each retained page must
 * be exactly its scoped projection, with no additional unreviewed source copy. */
export function assertStreamPage(stream: Pick<UnverifiedStreamPages, 'header'>, plan: ImportPlan) {
  const h=stream.header;
  const counts={input:plan.entries.length,unique:plan.entries.length,duplicates:0,mapped:0,unmapped:0,rejected:0,effects:0,byKind:Object.create(null) as Record<string,number>};
  const identities=new Set(plan.entries.map(e=>canonicalJson([e.source.kind,e.source.sourceId])));
  for(const e of plan.entries){counts.byKind[e.source.kind]=(counts.byKind[e.source.kind]??0)+1;counts.mapped+=Number(e.effects.length>0);counts.unmapped+=Number(e.unmapped.length>0);counts.rejected+=Number(!!e.rejection);counts.effects+=e.effects.length;}
  const diagnostics=h.diagnostics.filter(d=>!d.kind||!d.sourceId||identities.has(canonicalJson([d.kind,d.sourceId])));
  const same=(a:unknown,b:unknown)=>canonicalHash(a)===canonicalHash(b);
  if (!same(Object.keys(plan).sort(),['counts','destination','diagnostics','entries','fingerprint','mappingVersion','namespace','principalId','source','version'])
    || !(['version','destination','namespace','mappingVersion','principalId'] as const).every(key=>plan[key]===h[key])
    || !same(counts,plan.counts) || !same(diagnostics,plan.diagnostics)
    || !same(plan.source,{...h.source,records:plan.entries.map(e=>e.source)})) throw new Error('IMPORT_STREAM_PAGE_CHANGED');
}
