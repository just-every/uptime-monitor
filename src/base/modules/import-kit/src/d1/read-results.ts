/** Both supported port shapes must be successful, complete rowsets. An adapter
 * cannot turn a failed or missing D1 envelope into an empty successful receipt. */
export function batchRows(result: unknown, error = 'IMPORT_D1_READ_RECEIPT_REQUIRED'): Record<string, any>[] {
  let rows: unknown;
  if (Array.isArray(result)) rows = result;
  else if (result && typeof result === 'object' && 'results' in result) {
    if (('success' in result && result.success !== true) || ('error' in result && result.error != null)) throw new Error(error);
    rows = result.results;
  }
  if (!Array.isArray(rows) || rows.some(row => !row || typeof row !== 'object' || Array.isArray(row))) throw new Error(error);
  return rows;
}
export function checkBatchResults(results: unknown, count: number, error = 'IMPORT_D1_READ_RECEIPT_REQUIRED'): asserts results is unknown[] {
  if (!Array.isArray(results) || results.length !== count) throw new Error(error);
  for (const result of results) batchRows(result, error);
}
