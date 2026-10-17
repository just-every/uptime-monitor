/** Validate finite JSON in canonical read order without allocating text.
 * Cycles and excessive nesting fail before recursive encoding can overflow. */
export function validateJson(value: unknown): void {
  const active = new Set<object>();
  const visit = (item: unknown, depth: number): void => {
    if (item === null || typeof item === 'string' || typeof item === 'boolean' || typeof item === 'number' && Number.isFinite(item)) return;
    if (item && typeof item === 'object' && (Array.isArray(item) || Object.getPrototypeOf(item) === Object.prototype || Object.getPrototypeOf(item) === null)) {
      if (depth >= 256 || active.has(item)) throw new Error('INVALID_JSON: cyclic or excessively nested durable value');
      active.add(item);
      if (Array.isArray(item)) {
        const length = item.length;
        for (let index = 0; index < length; index++) visit(item[index], depth + 1);
      } else for (const key of Object.keys(item).sort()) visit((item as Record<string, unknown>)[key], depth + 1);
      active.delete(item); return;
    }
    throw new Error('INVALID_JSON: durable values must be finite JSON');
  };
  visit(value, 0);
}
