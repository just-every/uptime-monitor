import type { Json } from '../types.ts';
import { canonicalSize } from '../provenance/canonical-stream.ts';
/** Page-local lossless JSON dictionary. Tags distinguish every JSON type; object
 * keys are data, including __proto__. No dictionary survives outside its page. */
export function dictionaryEncode(value: Json): Json {
  const strings: string[] = [], indexes = new Map<string, number>();
  const json: Json[] = [], jsonIndexes = new Map<string, number>();
  const intern = (text: string) => { const prior=indexes.get(text);if(prior!==undefined)return prior;const id=strings.length;strings.push(text);indexes.set(text,id);return id; };
  const encodedString = (text: string, newEntry: boolean): Json => {
    if (text[0] === '{' || text[0] === '[') {
      const prior = jsonIndexes.get(text);
      if (prior !== undefined) return [3, prior];
      let parsed: Json | undefined;
      if (newEntry) try { const candidate = JSON.parse(text); if (JSON.stringify(candidate) === text) parsed = candidate; } catch {}
      if (parsed !== undefined) {
        const tree = encode(parsed, false), index = json.length;
        json.push(tree); jsonIndexes.set(text, index); return [3, index];
      }
    }
    return [0, intern(text)];
  };
  const encode=(item: Json, newEntry = true):Json=>{
    if(typeof item==='string')return encodedString(item,newEntry);
    if(Array.isArray(item))return [1,...Array.from({length:item.length},(_,index)=>encode(item[index],newEntry))];
    if(item&&typeof item==='object'&&(Object.getPrototypeOf(item)===Object.prototype||Object.getPrototypeOf(item)===null))return [2,...Object.entries(item).flatMap(([key,entry])=>[intern(key),encode(entry,newEntry)])];
    if(item===null||typeof item==='boolean'||typeof item==='number'&&Number.isFinite(item))return Object.is(item,-0)?0:item;
    throw new Error('INVALID_JSON: durable values must be finite JSON');
  };
  // Canonically serialized JSON strings share their inner literals too. Their
  // exact original text is reconstructed once, independently of occurrences.
  // Encoded descendants are arrays and finite primitives. Keeping the only
  // object in sorted-key order makes native JSON.stringify byte-canonical.
  const tree=encode(value);return {json,strings,tree,version:2};
}
export function dictionaryDecode(value: any, maximumPayloadBytes = Number.MAX_SAFE_INTEGER): Json {
  if (!Number.isSafeInteger(maximumPayloadBytes) || maximumPayloadBytes < 0) throw new Error('STORED_PLAN_CHANGED');
  const current = value?.version === 2;
  if(!value||Object.keys(value).sort().join(',')!==(current?'json,strings,tree,version':'strings,tree')||!Array.isArray(value.strings)||current&&!Array.isArray(value.json))throw new Error('STORED_PLAN_CHANGED');
  const distinct = new Set<string>();
  for(let index=0;index<value.strings.length;index++){const text=value.strings[index];if(typeof text!=='string'||distinct.has(text))throw new Error('STORED_PLAN_CHANGED');distinct.add(text);}
  const used=new Set<number>();
  const jsonStrings: string[] = [], usedJson = new Set<number>(), distinctJson = new Set<string>();
  if(current){
    const direct=new Set<number>();
    const scan=(item:any):void=>{if(Array.isArray(item)){if(item[0]===3&&item.length===2){const index=item[1];if(!Number.isSafeInteger(index)||index<0||index>=value.json.length)throw new Error('STORED_PLAN_CHANGED');direct.add(index);}else for(let index=1;index<item.length;index++)scan(item[index]);}};
    scan(value.tree);
    // Every reconstructed string occurs in the payload itself. Intermediate
    // nested text stays literal, so the complete table is bounded by payload
    // bytes rather than the sum of arbitrary hidden reconstruction chains.
    if(direct.size!==value.json.length)throw new Error('STORED_PLAN_CHANGED');
  }
  const string=(index:unknown)=>{if(!Number.isSafeInteger(index)||Number(index)<0||Number(index)>=value.strings.length)throw new Error('STORED_PLAN_CHANGED');used.add(Number(index));return value.strings[Number(index)] as string;};
  const decode=(item:any):Json=>{
    if(!Array.isArray(item)){if(item===null||typeof item==='boolean'||typeof item==='number'&&Number.isFinite(item))return item;throw new Error('STORED_PLAN_CHANGED');}
    if(item[0]===0&&item.length===2)return string(item[1]);
    if(current&&item[0]===3&&item.length===2){const index=item[1];if(!Number.isSafeInteger(index)||index<0||index>=jsonStrings.length)throw new Error('STORED_PLAN_CHANGED');usedJson.add(index);return jsonStrings[index];}
    if(item[0]===1)return Array.from({length:item.length-1},(_,index)=>decode(item[index+1]));
    if(item[0]!==2||item.length%2!==1)throw new Error('STORED_PLAN_CHANGED');
    const result:Record<string,Json>={};
    for(let i=1;i<item.length;i+=2){const key=string(item[i]);if(Object.hasOwn(result,key))throw new Error('STORED_PLAN_CHANGED');Object.defineProperty(result,key,{value:decode(item[i+1]),enumerable:true,writable:true,configurable:true});}
    return result;
  };
  let reconstructedBytes=0;
  if(current)for(let index=0;index<value.json.length;index++){
    const parsed=decode(value.json[index]);
    if(!parsed||typeof parsed!=='object')throw new Error('STORED_PLAN_CHANGED');
    // A small reference graph can describe exponentially larger embedded
    // text. Check its exact serialized size before allocating that string.
    const size=canonicalSize(parsed);
    if(size>maximumPayloadBytes-reconstructedBytes)throw new Error('STORED_PLAN_CHANGED');
    reconstructedBytes+=size;
    const text=JSON.stringify(parsed);
    if(distinctJson.has(text))throw new Error('STORED_PLAN_CHANGED');
    distinctJson.add(text);jsonStrings.push(text);
  }
  const decoded=decode(value.tree);
  if(used.size!==value.strings.length||usedJson.size!==jsonStrings.length)throw new Error('STORED_PLAN_CHANGED');
  if(current&&canonicalSize(decoded)>maximumPayloadBytes)throw new Error('STORED_PLAN_CHANGED');
  return decoded;
}
