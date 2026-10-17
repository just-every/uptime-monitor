import { Buffer } from 'node:buffer';
import type { ImportPlan } from '../types.ts';
import { canonicalJson } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
/** Retained literals and structural references, counted without allocating a
 * second dictionary tree. Keys and container slots remain part of the bound. */
export function dictionaryWeight(value: unknown, dictionary = true, shared = new Set<string>()): number {
  const weight = (value: unknown): number => {
    if (typeof value === 'string') {
      if (dictionary) { if (shared.has(value)) return 16; shared.add(value); }
      return Buffer.byteLength(JSON.stringify(value));
    }
    if (Array.isArray(value)) return 2 + value.reduce((n, item) => n + 1 + weight(item), 0);
    if (value && typeof value === 'object') return 2 + Object.entries(value).reduce((n, [key, item]) => n + Buffer.byteLength(JSON.stringify(key)) + 2 + weight(item), 0);
    return Buffer.byteLength(JSON.stringify(value));
  };
  return weight(value);
}
/** Native units per stream child. Reversal and preserved-undo classification
 * cost grows with a child's units, so many tiny effects split into more
 * children instead of one child whose undo request outgrows the budget. */
export const STREAM_CHILD_UNITS = 100;
/** One unit per effect; an entry without effects still holds a decision. */
export const entryUnits = (entry: ImportPlan['entries'][number]): number => Math.max(1, entry.effects.length);
/** Preserve already normalized effects; partitioning never reruns the mapper.
 * Large individual native entities stay atomic, while ordinary pages are byte
 * bounded as well as unit bounded. */
export function* partitionPlan(plan: ImportPlan, dictionary = true, byteLimit = 512 * 1024, unitLimit = STREAM_CHILD_UNITS): Generator<ImportPlan> {
  let start=0,size=0,units=0;
  let shared = new Set<string>();
  // Dictionary-backed custody retains each literal once regardless of its
  // length. Counting expanded repetitions splits the dictionary over many
  // pages and can make the complete compressed collection larger than memory.
  // Non-dictionary custody still partitions by its expanded representation.
  const part=(end:number):ImportPlan=>{
    if(start===0&&end===plan.entries.length)return plan;
    const entries=plan.entries.slice(start,end), identities=new Set(entries.map(e=>canonicalJson([e.source.kind,e.source.sourceId])));
    const counts={input:entries.length,unique:entries.length,duplicates:0,mapped:0,unmapped:0,rejected:0,effects:0,byKind:Object.create(null) as Record<string,number>};
    for(const entry of entries){counts.byKind[entry.source.kind]=(counts.byKind[entry.source.kind]??0)+1;counts.mapped+=Number(entry.effects.length>0);counts.unmapped+=Number(entry.unmapped.length>0);counts.rejected+=Number(!!entry.rejection);counts.effects+=entry.effects.length;}
    const next={...plan,entries,source:{...plan.source,records:entries.map(e=>e.source)},counts,diagnostics:plan.diagnostics.filter(d=>!d.kind||!d.sourceId||identities.has(canonicalJson([d.kind,d.sourceId])))};
    next.fingerprint=planFingerprint(next);return next;
  };
  for(let index=0;index<plan.entries.length;index++){
    const entry=plan.entries[index];
    let bytes=dictionaryWeight(entry,dictionary,shared)+dictionaryWeight(entry.source,dictionary,shared);
    if(index>start&&(size+bytes>byteLimit||units+entryUnits(entry)>unitLimit)){yield part(index);start=index;size=0;units=0;shared=new Set();bytes=dictionaryWeight(entry,dictionary,shared)+dictionaryWeight(entry.source,dictionary,shared);}
    size+=bytes;units+=entryUnits(entry);
  }
  yield part(plan.entries.length);
}
