import type { ImportDoorOptions } from './types.ts';
import type { DoorAction } from './grants.ts';
import { DoorError } from './grants.ts';
import { admit, enabled } from './authority.ts';
import { authorized } from './store.ts';
import { uploads } from './uploads.ts';
import { plan } from './planning.ts';
import { apply, status, reconcile, undo } from './jobs.ts';
export type * from './types.ts';
export type { DoorAction, DoorGrant } from './grants.ts';
export { verifyImportGrant, DoorError } from './grants.ts';
export { importDoorSchema } from './schema.ts';
export { writeImportDoorSettings, readImportDoorHistory } from './authority.ts';

const routes = {uploads,plan,apply,status,reconcile,undo};
const headers = {'Cache-Control':'no-store','Content-Type':'application/json','X-Content-Type-Options':'nosniff'};
async function readInput(request: Request): Promise<Record<string,unknown>> {
  if (request.headers.get('Content-Type')?.split(';')[0] !== 'application/json' || !request.body) throw new DoorError('json_body_required',400);
  const reader = request.body.getReader(), chunks: Uint8Array[] = []; let size = 0;
  try {
    for (;;) {
      const {done,value} = await reader.read(); if (done) break;
      size += value.byteLength;
      if (size > 100_000) throw new DoorError('request_too_large',413);
      chunks.push(value);
    }
  } finally { await reader.cancel(); reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const part of chunks) { bytes.set(part,offset); offset += part.byteLength; }
  let value;
  try { value = JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)); } catch { throw new DoorError('invalid_json',400); }
  if (!value || typeof value!=='object' || Array.isArray(value)) throw new DoorError('invalid_input',400);
  return value;
}
/** Server-side mount only. A null response belongs to another dispatcher. */
export function createImportDoorHandler(options: ImportDoorOptions): (request: Request)=>Promise<Response|null> {
  if (!options.ownerPrincipalId?.trim() || !options.destination?.trim() || !options.mapping?.version || typeof options.parse!=='function') throw new DoorError('invalid_configuration',503);
  return async request => {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/api/rer/import/')) return null;
    try {
      // Check opt-in before revealing routes, authentication, or parser behavior.
      if (!await enabled(options)) return Response.json({error:'not_found'},{status:404,headers});
      const action = url.pathname.slice('/api/rer/import/'.length) as DoorAction;
      if (!Object.hasOwn(routes,action)) return Response.json({error:'not_found'},{status:404,headers});
      if (request.method!=='POST') return Response.json({error:'method_not_allowed'},{status:405,headers:{...headers,Allow:'POST'}});
      if (url.search || url.hash) throw new DoorError('query_transport_forbidden',400);
      const admitted = await admit(options,request,action), input = await readInput(request);
      if (['status','reconcile','undo'].includes(action) && Object.keys(input).some(k=>k!=='planId')) throw new DoorError('invalid_input',400);
      const result = await routes[action](admitted,input);
      await authorized(admitted);
      return Response.json(result,{headers});
    } catch (error) {
      const known = error instanceof DoorError;
      // Never reflect a parser/native adapter error containing customer records.
      return Response.json({error:known?error.code:'import_door_conflict',...(known && error.details?{details:error.details}:{})},{status:known?error.status:409,headers});
    }
  };
}
