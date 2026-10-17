import type { TargetRow } from '../types.ts';
import type { D1TargetAdapter } from './types.ts';
import { guard } from './database.ts';
/** Every part is a required conjunct; an empty snapshot cannot authorize a write. */
export function targetGuards(targets: D1TargetAdapter, row: TargetRow | null, destination: string, kind: string, id: string) {
  const parts = targets.matchesParts ? targets.matchesParts(row,destination,kind,id) : [targets.matches(row,destination,kind,id)];
  if (!parts.length) throw new Error('IMPORT_NATIVE_ROW_GUARD_REQUIRED');
  return parts.map(guard);
}
