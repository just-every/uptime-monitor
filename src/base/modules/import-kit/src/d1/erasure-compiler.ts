import { guardPredicate } from './guards.ts';
import {createHash} from 'node:crypto';
import type {D1ImportContext,D1Sql} from './types.ts';
import type {ErasureFragment} from './erasure-fragments.ts';
import {erasureBaselines} from './erasure-fragments.ts';
import {bare,prepareErasureFragment,privacyRows,valueRows,ErasureRowsAdmission} from './erasure-values.ts';
import {erasedIndexGuards} from './erasure-index.ts';
import {jobColumns} from './sets.ts';
import {batchBytes} from './transport.ts';
import {canonicalJson} from '../provenance/identity.ts';
import {remaining} from './budget.ts';
import {nativeStageRowsBytes} from './native-staging-rows.ts';

type Parts={before:D1Sql[];writes:D1Sql[];post:D1Sql[]};
const parts=():Parts=>({before:[],writes:[],post:[]});
const headers=(fragment:ErasureFragment)=>fragment.jobs.map(row=>Object.fromEntries([...jobColumns.filter(c=>c!=='plan_json'),'state_revision'].map(column=>[column,row[column]])));
export class ErasureCompiler {
  readonly compiled=parts();direct:Parts|undefined=parts();
  readonly indexes:ErasureRowsAdmission;readonly payload:ErasureRowsAdmission;
  readonly jobs=new Map<string,any>();readonly fingerprints:string[]=[];
  readonly erasedFinal:D1Sql[]=[];
  values=0;erased=0;readQueries=0;unsafeOriginals=false;
  readonly context:D1ImportContext;readonly id:string;readonly fragments:()=>AsyncGenerator<ErasureFragment>;
  constructor(context:D1ImportContext,id:string,fragments:()=>AsyncGenerator<ErasureFragment>){this.context=context;this.id=id;this.fragments=fragments;this.indexes=new ErasureRowsAdmission(nativeStageRowsBytes(context));this.payload=new ErasureRowsAdmission(nativeStageRowsBytes(context));}
  async inspect(){
    const before=remaining(this.context),iterator=this.fragments();
    while(true){
      let item:IteratorResult<ErasureFragment>|undefined=await iterator.next();if(item.done)break;
      this.collect(item.value);item=undefined;
    }
    this.readQueries=before-remaining(this.context);
  }
  private collect(fragment:ErasureFragment){
    if(fragment.plans.some(plan=>plan.destination!==this.context.destination))throw new Error('PLAN_BINDING_MISMATCH');
    const prepared=prepareErasureFragment(fragment,this.id,this.values),hash=createHash('sha256');
    hash.update(prepared.hash);hash.update(canonicalJson(headers(fragment)));
    for(const row of privacyRows(fragment,this.indexes.count)){this.indexes.append(row);hash.update(JSON.stringify(row)+'\n');}
    for(const row of valueRows(prepared.values,this.values))this.payload.append(row);
    this.values+=prepared.values.length;this.erased+=fragment.erased;
    this.unsafeOriginals ||= fragment.plans.some(plan=>(this.context.originalErasurePolicy===undefined&&plan.source.artifacts.some(a=>a.original?.status==='available'))||plan.entries.some(e=>e.effects.some(effect=>effect.attachment!==undefined)));
    for(const row of headers(fragment)){const prior=this.jobs.get(row.id);if(prior&&canonicalJson(prior)!==canonicalJson(row))throw new Error('IMPORT_CUSTODY_STATE_CHANGED');this.jobs.set(row.id,row);}
    this.fingerprints.push(hash.digest('hex'));
    // Only digest predicates survive the page; tombstones have no seal trigger.
    for(const plan of fragment.plans)this.erasedFinal.push(...erasedIndexGuards(plan).map(query=>guardPredicate(query)!));
    for(const key of ['before','writes','post'] as const){this.compiled[key].push(...prepared[key]);this.direct?.[key].push(...fragment[key].map(bare));}
    // The direct alternative is retained only while its complete values fit.
    // Prepared references and numeric admission data remain bounded separately.
    if(this.direct&&batchBytes([...this.direct.before,...this.direct.writes,...this.direct.post])>this.context.maxBatchBytes!)this.direct=undefined;
    if(batchBytes([...this.compiled.before,...this.compiled.writes,...this.compiled.post])>this.context.maxBatchBytes!)throw new Error('IMPORT_D1_TRANSPORT_LIMIT');
  }
  baseline():D1Sql[]{return erasureBaselines([...this.jobs.values()]);}
  /** Both later passes must reproduce the validated instructions, exact value
   * hashes and complete privacy membership under the same monotonic fences. */
  async replay(accept:(fragment:ErasureFragment,values:string[],valueStart:number,indexStart:number)=>Promise<void>){
    let ordinal=0,valueStart=0,indexStart=0;const iterator=this.fragments();
    while(true){
      let item:IteratorResult<ErasureFragment>|undefined=await iterator.next();if(item.done)break;
      let prepared:ReturnType<typeof prepareErasureFragment>|undefined=prepareErasureFragment(item.value,this.id,valueStart);
      const hash=createHash('sha256');hash.update(prepared.hash);hash.update(canonicalJson(headers(item.value)));
      let count=0;for(const row of privacyRows(item.value,indexStart)){hash.update(JSON.stringify(row)+'\n');count++;}
      if(hash.digest('hex')!==this.fingerprints[ordinal++])throw new Error('IMPORT_CUSTODY_STATE_CHANGED');
      await accept(item.value,prepared.values,valueStart,indexStart);
      valueStart+=prepared.values.length;indexStart+=count;prepared=undefined;item=undefined;
    }
    if(ordinal!==this.fingerprints.length||valueStart!==this.values||indexStart!==this.indexes.count)throw new Error('IMPORT_CUSTODY_STATE_CHANGED');
  }
}
