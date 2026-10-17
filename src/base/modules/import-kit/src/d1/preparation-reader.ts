import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { sha256 } from '../provenance/identity.ts';
import { planFingerprint } from '../plan/index.ts';
import { expandCompressedEnvelope } from './compressed-plan.ts';
import { sql } from './database.ts';
import { layoutHash } from './stream-state.ts';
import { preparationRead, type PreparationState } from './preparation-state.ts';
import type { ImportPlan } from '../types.ts';
import type { UnverifiedStreamPages } from './stream-plan.ts';
import type { D1PlanCodec } from './types.ts';

async function* chunks(state:PreparationState,headers=false):AsyncGenerator<any>{
  let page=-1,ordinal=-1;
  for(;;){
    const rows=await preparationRead(state,headers
      ?sql('SELECT ordinal,body,body_hash,body_size FROM ik_d1_staging_headers WHERE stage_id=? AND ordinal>? ORDER BY ordinal LIMIT 32',state.stage.id,ordinal)
      :sql('SELECT page_ordinal,ordinal,body,body_hash,body_size FROM ik_d1_staging_chunks WHERE stage_id=? AND (page_ordinal>? OR (page_ordinal=? AND ordinal>?)) ORDER BY page_ordinal,ordinal LIMIT 32',state.stage.id,page,page,ordinal));
    if(!rows.length)return;
    for(const row of rows){
      if(typeof row.body!=='string'||Buffer.byteLength(row.body)!==row.body_size||sha256(row.body)!==row.body_hash)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
      page=row.page_ordinal??-1;ordinal=row.ordinal;yield row;
    }
  }
}

export async function preparationHeaderRead(state:PreparationState):Promise<UnverifiedStreamPages['header']>{
  const bodies:string[]=[],hash=createHash('sha256');let bytes=0,ordinal=0;
  for await(const row of chunks(state,true)){
    if(row.ordinal!==ordinal++)throw new Error('IMPORT_PREPARATION_CHANGED');
    bodies.push(row.body);hash.update(row.body);bytes+=row.body_size;
  }
  if(ordinal!==state.receipt.header_chunks||bytes!==state.receipt.header_size||hash.digest('hex')!==state.receipt.header_hash)throw new Error('IMPORT_PREPARATION_CHANGED');
  let body=bodies.join('');bodies.length=0;
  const header=JSON.parse(body);body='';
  if(canonicalHash(header)!==state.receipt.header_hash)throw new Error('IMPORT_PREPARATION_CHANGED');
  return header;
}

export async function preparationPages(state:PreparationState){
  const pages=await preparationRead(state,sql('SELECT ordinal,job_id,total FROM ik_d1_staging_pages WHERE stage_id=? ORDER BY ordinal',state.stage.id));
  if(pages.length!==state.stage.pages||pages.some((p,i)=>p.ordinal!==i||!Number.isSafeInteger(p.total)||p.total<0)||pages.reduce((n,p)=>n+p.total,0)!==state.stage.total||layoutHash(pages)!==state.stage.layout_hash)throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
  return pages;
}

/** At most 32 stored chunks and one expanded plan are live. A second canonical
 * source traversal starts a fresh reader; no expanded page cache spans pages. */
export async function* readPreparationPlans(state:PreparationState,pages:any[]):AsyncGenerator<{page:any;plan:ImportPlan}>{
  const stored=chunks(state)[Symbol.asyncIterator]();
  try {
  let pageNumber=0;
  for await(const page of pageMetadata(state)){
    const expected=pages[pageNumber++];
    if(!expected||page.ordinal!==expected.ordinal||page.job_id!==expected.job_id||page.total!==expected.total)throw new Error('IMPORT_STREAM_LAYOUT_CHANGED');
    const parts:string[]=[];let bytes=0;
    for(let ordinal=0;ordinal<page.chunks;ordinal++){
      const next=await stored.next();
      if(next.done||next.value.page_ordinal!==page.ordinal||next.value.ordinal!==ordinal)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
      parts.push(next.value.body);bytes+=next.value.body_size;
    }
    yield {page,plan:storedPagePlan(page,parts,bytes,state.context.storage.targets.planCodec)};
  }
  if(pageNumber!==pages.length||!(await stored.next()).done)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  } finally { await stored.return?.(undefined); }
}

/** One staged page's plan from its chunk bodies, checked against the page's
 * manifest, size, digest, plan fingerprint and record count. */
export function storedPagePlan(page:any,parts:string[],bytes:number,codec?:D1PlanCodec):ImportPlan{
  const body=parts.join('');parts.length=0;
  if(bytes!==page.body_size||sha256(body)!==page.body_hash)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  const manifest=JSON.parse(page.plan_json);
  if(canonicalHash(manifest)!==sha256(page.plan_json)||canonicalHash(manifest)!==canonicalHash({d1Plan:1,chunks:page.chunks,size:bytes,sha256:page.body_hash}))throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
  const envelope=JSON.parse(body);
  const plan=([2,3].includes(envelope?.d1Plan)?expandCompressedEnvelope(body,envelope,codec):envelope) as ImportPlan;
  if(![2,3].includes(envelope?.d1Plan)&&(page.inline_plan!==1||canonicalHash(plan)!==page.body_hash||planFingerprint(plan)!==plan.fingerprint))throw new Error('STORED_PLAN_CHANGED');
  if(plan.fingerprint!==page.fingerprint||plan.entries.length!==page.total)throw new Error('IMPORT_STREAM_PAGE_CHANGED');
  return plan;
}

export async function* preparationIndex(state:PreparationState):AsyncGenerator<any>{
  let cursor=0;
  for(;;){
    const rows=await preparationRead(state,sql('SELECT rowid AS cursor,page_ordinal,ordinal,source_identity,target_identity FROM ik_d1_staging_index WHERE stage_id=? AND rowid>? ORDER BY rowid LIMIT 4096',state.stage.id,cursor));
    if(!rows.length)return;
    for(const row of rows){cursor=row.cursor;yield row;}
  }
}

async function* pageMetadata(state:PreparationState):AsyncGenerator<any>{
  let ordinal=-1;
  for(;;){
    const rows=await preparationRead(state,sql('SELECT * FROM ik_d1_staging_pages WHERE stage_id=? AND ordinal>? ORDER BY ordinal LIMIT 128',state.stage.id,ordinal));
    if(!rows.length)return;
    for(const row of rows){ordinal=row.ordinal;yield row;}
  }
}
