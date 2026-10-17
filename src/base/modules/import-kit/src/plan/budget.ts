import type { ImportPlan } from '../types.ts';
import { assertPlanBytes, canonicalJsonBytes, checkedSizeSum, planContentBytes, StringSizeCache } from './size.ts';

type Entry = ImportPlan['entries'][number];
type Diagnostic = ImportPlan['diagnostics'][number];
/** Exact running size, including repeated source/diagnostic text, punctuation
 * and changing numeric lexemes. Each appended value is measured once; no
 * growing plan is encoded or rescanned to decide admission. */
export class PlanSizeBudget {
  private bytes: number;
  private readonly strings = new StringSizeCache();
  private kinds = 0;
  private plan: ImportPlan;
  constructor(plan: ImportPlan) {
    this.plan = plan;
    this.bytes = planContentBytes(plan, this.strings);
    this.assert();
  }
  measure(value: unknown, sorted = false): number { return canonicalJsonBytes(value, undefined, this.strings, sorted); }
  assert(): void { assertPlanBytes(this.bytes); }
  private add(delta: number): void { this.bytes = checkedSizeSum(this.bytes, delta); }
  replace(previousBytes: number, next: unknown): void {
    this.add(checkedSizeSum(this.measure(next), -previousBytes));
  }
  diagnostic(item: Diagnostic): void {
    this.add(checkedSizeSum(this.measure(item), Number(this.plan.diagnostics.length > 0)));
    this.plan.diagnostics.push(item);
  }
  increment(key: Exclude<keyof ImportPlan['counts'], 'byKind'>, amount = 1): void {
    const before = this.plan.counts[key];
    const after = checkedSizeSum(before, amount);
    this.add(checkedSizeSum(String(after).length, -String(before).length));
    this.plan.counts[key] = after;
  }
  entry(entry: Entry): void {
    this.add(checkedSizeSum(this.measure(entry), Number(this.plan.entries.length > 0)));
    this.plan.entries.push(entry);
    const kinds = this.plan.counts.byKind, kind = entry.source.kind, previous = kinds[kind];
    const next = checkedSizeSum(previous ?? 0, 1);
    if (previous === undefined) {
      this.add(checkedSizeSum(this.measure(kind), this.kinds > 0 ? 3 : 2));
      this.kinds = checkedSizeSum(this.kinds, 1);
    } else this.add(checkedSizeSum(String(next).length, -String(previous).length));
    kinds[kind] = next;
    this.increment('unique');
  }
  mapping(entry: Entry, effects: Entry['effects'], unmapped: Entry['unmapped'], rejection?: string): void {
    // A new entry starts with empty arrays; its source is already reserved.
    this.add(checkedSizeSum(this.measure(effects), -2));
    this.add(checkedSizeSum(this.measure(unmapped), -2));
    entry.effects = effects; entry.unmapped = unmapped;
    if (rejection) {
      this.add(checkedSizeSum(checkedSizeSum(this.measure('rejection'), 2), this.measure(rejection)));
      entry.rejection = rejection; this.increment('rejected');
    }
    if (effects.length) this.increment('mapped');
    if (unmapped.length) this.increment('unmapped');
    this.increment('effects', effects.length);
  }
}
