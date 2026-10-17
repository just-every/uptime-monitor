import type { D1Database,D1PlanCodec,D1Statement } from './types.ts';
import type { ErasurePage } from './erasure-pages.ts';
import { readInventorySnapshot } from './inventory-snapshot.ts';

/** Replays reuse the first measurement's owners, revisions and read ceiling.
 * A changed closure cannot spend the reservation for publication or cleanup. */
export async function readErasureSnapshot(page:ErasurePage,database:D1Database,codec?:D1PlanCodec){
  let reads=0;
  const consume=()=>{if(page.readQueries!==undefined&&reads>=page.readQueries)throw new Error('IMPORT_CUSTODY_STATE_CHANGED');reads++;};
  const wrap=(statement:D1Statement):D1Statement=>({bind(...values:unknown[]){return wrap(statement.bind(...values));},async first<T=Record<string,any>>(){consume();return statement.first<T>();},async all<T=Record<string,any>>(){consume();return statement.all<T>();}});
  const measured:D1Database={prepare:query=>wrap(database.prepare(query)),async batch(){throw new Error('IMPORT_SNAPSHOT_WRITE_REFUSED');}};
  const result=await readInventorySnapshot(page.ids,measured,codec,true,page.fence);
  if(page.readQueries===undefined)page.readQueries=reads;
  return result;
}
