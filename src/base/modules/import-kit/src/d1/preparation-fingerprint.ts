import { createCanonicalStreamHash } from '../provenance/canonical-hash.ts';
import { canonicalHash } from '../provenance/canonical-stream.ts';
import { originalsFor } from '../storage/originals.ts';
import { planIndex } from './erasure-index.ts';
import { assertStreamPage } from './stream-page-integrity.ts';
import { readPreparationPlans, preparationIndex } from './preparation-reader.ts';
import { preparationRead, type PreparationState } from './preparation-state.ts';
import { policyHash } from './undo-state.ts';
import { sql } from './database.ts';
import type { UnverifiedStreamPages } from './stream-plan.ts';

/** Same sorted object keys, canonical values and array order as planFingerprint.
 * Only the two large arrays obtain each page across an awaited custody read. */
export async function verifyPreparationFingerprint(state:PreparationState,header:UnverifiedStreamPages['header'],pages:any[]):Promise<void>{
  if(header.version!==1||header.destination!==state.stage.destination||header.namespace!==state.stage.namespace||header.mappingVersion!==state.stage.mapping_version||header.principalId!==state.stage.principal_id||header.counts.unique!==state.stage.total)throw new Error('PLAN_BINDING_MISMATCH');
  const writer=createCanonicalStreamHash(),index=preparationIndex(state)[Symbol.asyncIterator]();
  const counts={input:0,unique:0,duplicates:0,mapped:0,unmapped:0,rejected:0,effects:0,byKind:Object.create(null) as Record<string,number>};
  const document={...header,entries:[],source:{...header.source,records:[]}};
  const records=async(source:boolean)=>{
    writer.token('[');let first=true;
    for await(const {page,plan} of readPreparationPlans(state,pages)){
      assertStreamPage({header},plan);
      if(!source){
        if(plan.entries.some(e=>e.effects.some(effect=>effect.attachment))||plan.source.artifacts.some(a=>a.original?.status==='available'))throw new Error('IMPORT_STREAM_NORMALIZED_SOURCE_REQUIRED');
        for(const key of ['input','unique','duplicates','mapped','unmapped','rejected','effects'] as const)counts[key]+=plan.counts[key];
        for(const [kind,count]of Object.entries(plan.counts.byKind))counts.byKind[kind]=(counts.byKind[kind]??0)+count;
        const identities=planIndex(plan,page.job_id).map(row=>[page.ordinal,row.ordinal,row.source_identity,row.target_identity]);
        if(identities.length!==page.identities||canonicalHash(identities)!==page.index_hash||policyHash(page.job_id,plan.fingerprint,state.stage.reversal_policy)!==page.policy_hash)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
        for(const expected of identities){const actual=await index.next();if(actual.done||canonicalHash(expected)!==canonicalHash([actual.value.page_ordinal,actual.value.ordinal,actual.value.source_identity,actual.value.target_identity]))throw new Error('IMPORT_STAGE_CONTENT_CHANGED');}
      }
      for(const entry of plan.entries){if(!first)writer.token(',');first=false;writer.value(source?entry.source:entry);}
    }
    writer.token(']');
  };
  try{
    writer.token('{');let first=true;
    for(const key of Object.keys(document).sort()){
      if(!first)writer.token(',');first=false;writer.value(key);writer.token(':');
      if(key==='entries')await records(false);
      else if(key==='source'){
        writer.token('{');let firstSource=true;
        for(const sourceKey of Object.keys(document.source).sort()){
          if(!firstSource)writer.token(',');firstSource=false;writer.value(sourceKey);writer.token(':');
          if(sourceKey==='records')await records(true);else writer.value((document.source as any)[sourceKey]);
        }
        writer.token('}');
      }else writer.value((document as any)[key]);
    }
    writer.token('}');
    if(!(await index.next()).done)throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
    if(writer.digest()!==state.stage.fingerprint)throw new Error('REVIEW_FINGERPRINT_MISMATCH');
  }finally{writer.clear();await index.return?.(undefined);}
  if(canonicalHash(counts)!==canonicalHash(header.counts))throw new Error('IMPORT_STREAM_COUNTS_CHANGED');
  const expected=originalsFor({...header,source:{...header.source,records:[]},entries:[],fingerprint:state.stage.fingerprint},'').map(o=>[o.key,o.name,o.sha256,o.size]);
  const stored=await preparationRead(state,sql('SELECT original_key,name,sha256,size FROM ik_d1_staging_originals WHERE stage_id=? ORDER BY rowid',state.stage.id));
  if(canonicalHash(stored.map(o=>[o.original_key,o.name,o.sha256,o.size]))!==canonicalHash(expected))throw new Error('IMPORT_STAGE_CONTENT_CHANGED');
}
