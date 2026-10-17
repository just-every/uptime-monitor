/** Exact target uniqueness without copying a shared destination and kind into
 * every retained key. Identity components remain immutable strings; no hash or
 * truncated surrogate stands in for the complete reviewed tuple. */
export class TargetIdentities {
  private destinations = new Map<string, Map<string, Set<string>>>();
  private count = 0;
  get size(): number { return this.count; }
  claim(destination: string, kind: string, id: string): boolean {
    let kinds = this.destinations.get(destination);
    if (!kinds) { kinds = new Map(); this.destinations.set(destination, kinds); }
    let ids = kinds.get(kind);
    if (!ids) { ids = new Set(); kinds.set(kind, ids); }
    if (ids.has(id)) return false;
    ids.add(id); this.count++; return true;
  }
  clear(): void { this.destinations.clear(); this.count = 0; }
}
