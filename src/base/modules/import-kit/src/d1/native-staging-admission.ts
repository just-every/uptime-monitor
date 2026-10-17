import { stagingStatements } from './staging-statements.ts';
import { Buffer } from 'node:buffer';
import type { D1ImportContext, D1Sql } from './types.ts';
import { batchBytes } from './transport.ts';
import { nativeStageRowsBytes, nativeRowStatements } from './native-staging-rows.ts';
import type { NativeStageBinding } from './native-staging-state.ts';
export function nativeStageCost(context:D1ImportContext,stage:NativeStageBinding,statements:D1Sql[],release:D1Sql[]=[],final:D1Sql[]=[]):number{
  const packed=stagingStatements(context,stage.action,statements,release,final);
  if(batchBytes(packed)>Math.min(context.maxBatchBytes??2*1024*1024,2*1024*1024))throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
  return packed.length;
}
/** Simulate the same greedy bounded batches before retaining a single byte. */
export function nativeRowsCost(context:D1ImportContext,stage:NativeStageBinding,baseline:D1Sql[],table:'index'|'values',columns:string[],rows:()=>Iterable<unknown[]>,revision:number):{queries:number;revision:number}{
  const rowBytes=nativeStageRowsBytes(context);
  let parts:string[]=[],bytes=2,written=0,queries=0;
  const flush=()=>{if(!parts.length)return;queries+=nativeStageCost(context,stage,nativeRowStatements(stage,baseline,table,columns,'['+parts.join(',')+']',parts.length,written,revision));revision+=parts.length;written+=parts.length;parts=[];bytes=2;};
  for(const row of rows()){const part=JSON.stringify(row),size=Buffer.byteLength(part)+1;if(size+2>rowBytes)throw new Error('IMPORT_D1_TRANSPORT_LIMIT');if(parts.length&&bytes+size>rowBytes)flush();parts.push(part);bytes+=size;}
  flush();return {queries,revision};
}
