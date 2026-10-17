/** Capture each mapper property once in canonical order before admission.
 * Shared containers stay shared in this private snapshot, so a small DAG with
 * an enormous logical encoding is measured before any expanded clone. */
export function snapshotJson<T>(value: T): T {
  const copies = new WeakMap<object, object>(), active = new Set<object>();
  const visit = (item: unknown, depth: number): any => {
    if (item === null || typeof item === 'string' || typeof item === 'boolean' || typeof item === 'number' && Number.isFinite(item)) return Object.is(item, -0) ? 0 : item;
    if (!item || typeof item !== 'object' || !Array.isArray(item) && Object.getPrototypeOf(item) !== Object.prototype && Object.getPrototypeOf(item) !== null) throw new Error('INVALID_JSON: durable values must be finite JSON');
    if (depth >= 256 || active.has(item)) throw new Error('INVALID_JSON: cyclic or excessively nested durable value');
    const previous = copies.get(item); if (previous) return previous;
    active.add(item);
    const copy: any = Array.isArray(item) ? [] : {};
    copies.set(item, copy);
    if (Array.isArray(item)) {
      const length = item.length;
      for (let index = 0; index < length; index++) copy.push(visit(item[index], depth + 1));
    } else for (const key of Object.keys(item).sort()) Object.defineProperty(copy, key, { value: visit((item as Record<string, unknown>)[key], depth + 1), enumerable: true, writable: true, configurable: true });
    active.delete(item); return copy;
  };
  return visit(value, 0);
}
