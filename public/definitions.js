import {escape,icon} from './console-format.js';
// The Definitions view (/app#definitions): upload or paste a definition file or an UptimeRobot export, read the
// preview, apply exactly that preview, and undo a batch. The server plans and validates; this module only shows the
// plan it returns. Its state outlives the console's five-second refresh, which never re-renders an open preview.
const LIMIT=2*1024*1024;
const ACTIONS={create:['Create','enabled'],unchanged:['Unchanged','queued'],change:['Change','stale'],refused:['Refused','rejected'],unsupported:['Unsupported','unknown']};
const OUTCOMES={paused:'Paused, history kept',restored:'Restored to its earlier settings','edited-since':'Edited since; left as is','enabled-since':'Enabled since; left running',missing:'No longer present'};
let state={payload:null,preview:null,batches:null};
const value=v=>escape(typeof v==='string'?v:JSON.stringify(v));

async function send(path,{body,type}={}){
 const response=await fetch(path,{method:body===undefined&&!type?'GET':'POST',headers:type?{'content-type':type}:{},body}),result=await response.json().catch(()=>({error:`Request ${response.status}`}));
 if([401,403].includes(response.status))window.dispatchEvent(new Event('uptime:auth-lost'));
 if(!response.ok)throw Object.assign(Error(response.status===413?'The file is larger than 2 MiB.':result.error??`Request ${response.status}`),{status:response.status});
 return result;
}
const query=(params)=>new URLSearchParams(Object.entries(params).filter(([,v])=>v)).toString();

function countsMarkup(preview){
 if(preview.kind==='history')return `<p class="ui-callout">${escape(preview.note)}</p>`;
 const c=preview.counts,o=preview.outcomes;
 return `<dl class="definition-counts" aria-label="Preview counts">${Object.entries(ACTIONS).map(([key,[label]])=>`<div><dt>${label}</dt><dd>${c[key]}</dd></div>`).join('')}</dl>
 <p class="definition-source">${escape(preview.sourceLabel)}: ${o.importable} importable, ${o.withDifferences} importable with listed differences, ${o.unsupported} unsupported, ${o.refused} refused. New monitors arrive paused.</p>`;
}
function criteriaMarkup(definition){
 if(!definition||!['http','keyword'].includes(definition.type))return '';
 const a=definition.assertions,items=[['HTTP status',Array.isArray(a.status)?a.status.join(', '):String(a.status)]];
 if(definition.tlsTrust)items.push(['HTTPS trust',definition.tlsTrust.mode==='fingerprint'?'Pinned leaf SHA-256 '+definition.tlsTrust.sha256:definition.tlsTrust.mode==='ca'?'Owner-supplied CA; private runner required':'Certificate errors ignored for initial origin; private runner required']);
 if(a.bodyIncludes!==undefined)items.push(['Body contains',a.bodyIncludes]);
 if(a.bodyExcludes!==undefined)items.push(['Body must not contain',a.bodyExcludes]);
 if(a.bodyRegex)items.push(['Body expression','/'+a.bodyRegex.pattern+'/'+(a.bodyRegex.flags??'')]);
 if(a.header)items.push(['Header',a.header.name+' = '+a.header.equals]);
 if(a.json)items.push(['JSON',a.json.path+' = '+JSON.stringify(a.json.equals)]);
 if(a.latencyMs!==undefined)items.push(['Response time','At most '+a.latencyMs+' ms']);
 return '<p><strong>Reviewed check criteria</strong></p><ul>'+items.map(([label,text])=>`<li>${escape(label)}: <code>${escape(text)}</code></li>`).join('')+'</ul>';
}
function rowMarkup(e){
 const [label,pill]=ACTIONS[e.action],details=[
  ...(e.reason?[`<p>${escape(e.reason)}</p>`]:[]),
  criteriaMarkup(e.target),
  ...(e.changes??[]).map(ch=>`<p><span class="field">${escape(ch.field)}</span>: ${value(ch.from)} → ${value(ch.to)}</p>`),
  ...(e.differences.length?[`<ul>${e.differences.map(d=>`<li>${escape(d)}</li>`).join('')}</ul>`]:[]),
 ];
 const target=e.target?(e.target.url?`<span class="url">${escape(e.target.url)}</span>`:'<span class="url url-prose">Incoming heartbeat</span>'):'';
 return `<tr><td data-label="Action"><span class="ui-pill ${pill}">${label}</span></td><td data-label="Monitor"><span class="monitor-name">${escape(e.name??e.label)}</span>${target}</td><td data-label="Details" class="definition-details">${details.join('')||'<p>No differences.</p>'}</td></tr>`;
}
function previewMarkup(preview){
 if(!preview)return '';
 if(preview.kind==='history')return countsMarkup(preview);
 const c=preview.counts,applying=c.create+c.change;
 return `${countsMarkup(preview)}<p><button id="definition-apply" class="primary"${applying?'':' disabled'}>${icon('check')}Apply: create ${c.create}, change ${c.change}</button></p>
 <p class="definition-digest">Plan digest <code>${escape(preview.digest)}</code></p>
 <div class="table-scroll" tabindex="0" role="region" aria-label="Preview entries"><table class="ui-table ui-table--stack-md" aria-label="Preview entries"><thead><tr><th scope="col">Action</th><th scope="col">Monitor</th><th scope="col">Details</th></tr></thead><tbody>${preview.entries.map(rowMarkup).join('')}</tbody></table></div>`;
}
function batchesMarkup(batches){
 if(!batches)return '<p>Loading batches…</p>';
 if(!batches.length)return '<p>No definition files have been applied.</p>';
 return `<ul class="definition-batches">${batches.map(b=>{const created=b.entries.filter(e=>e.action==='create').length,changed=b.entries.length-created,status=b.restoredAt?'From an archive (history only)':b.undone?'Undone':b.undoneAt?`Undone ${new Date(b.undoneAt).toLocaleString()}`:'Applied';
  return `<li><p><strong>${escape(new Date(b.createdAt).toLocaleString())}</strong> · ${escape(b.source)} · ${created} created, ${changed} changed · ${escape(status)}</p>
  ${b.businessApproval?`<p>Approved via Business by ${escape(b.businessApproval.owner)} at ${escape(new Date(b.businessApproval.approvedAt).toLocaleString())}, fingerprint <code>${escape(b.businessApproval.fingerprint)}</code></p>`:''}
  <ul>${b.entries.map(e=>`<li>${escape(e.name??e.monitorId)} — ${e.action==='create'?'created':'changed'}${e.undoOutcome?`; ${escape(OUTCOMES[e.undoOutcome]??e.undoOutcome)}`:''}</li>`).join('')}</ul>
  ${!b.undone&&!b.undoneAt&&!b.restoredAt?`<button class="definition-undo" data-batch="${escape(b.id)}">${icon('restore')}Undo this batch</button>`:''}</li>`}).join('')}</ul>`;
}
export function renderDefinitions({area,action}){
 if(area.querySelector('.definitions-panel'))return;
 area.innerHTML=`<section class="empty runner-panel definitions-panel"><h2>Monitor definitions</h2>
 <p>Preview a definition file or saved UptimeRobot v3 pages or Pingdom HTTP check details before anything changes. New monitors arrive paused. A file never enables a monitor, issues a heartbeat token, sends a notification or changes a destination.</p>
 <section class="archive-tools definition-input" aria-labelledby="definition-input-heading"><h3 id="definition-input-heading">Preview a file</h3>
 <label>Definition file, UptimeRobot list/log or Pingdom HTTP details (2 MiB max)<input type="file" id="definition-file" accept=".json,.csv,application/json,text/csv"></label>
 <label>Or paste the file<textarea id="definition-text" rows="5" spellcheck="false"></textarea></label>
 <label>Source<select id="definition-source"><option value="auto">Detect from the file</option><option value="uptime.monitors">Uptime definition file</option><option value="uptimerobot">UptimeRobot v3 monitor list</option><option value="pingdom">Pingdom HTTP check details</option><option value="uptimerobot-log">UptimeRobot log export (CSV)</option></select></label>
 <p><button id="definition-preview" class="primary">Preview</button> <button id="definition-export">${icon('export')}Export current definitions</button></p></section>
 <section aria-labelledby="definition-preview-heading"><h3 id="definition-preview-heading">Preview</h3><div id="definition-result">${state.preview?'':'<p>Nothing previewed yet.</p>'}</div></section>
 <section aria-labelledby="definition-batches-heading"><h3 id="definition-batches-heading">Applied batches</h3><p>Undo pauses monitors a batch created and restores monitors it changed, unless they were edited or enabled since. History is never removed.</p><div id="definition-batches"></div></section></section>`;
 const $=s=>area.querySelector(s);
 const showPreview=()=>{$('#definition-result').innerHTML=state.preview?previewMarkup(state.preview):'<p>Nothing previewed yet.</p>';const apply=$('#definition-apply');if(apply)apply.onclick=applyPreview};
 const showBatches=()=>{$('#definition-batches').innerHTML=batchesMarkup(state.batches);for(const button of area.querySelectorAll('.definition-undo'))button.onclick=()=>undo(button.dataset.batch)};
 const loadBatches=async()=>{state.batches=await send('/api/definitions/batches');showBatches()};
 async function preview(){
  const file=$('#definition-file').files[0],text=$('#definition-text').value,source=$('#definition-source').value;
  await action(async()=>{
   if(!file&&!text.trim())throw Error('Choose a file or paste one first.');
   if(file&&file.size>LIMIT)throw Error(`The file is ${file.size.toLocaleString()} bytes; files are limited to 2 MiB.`);
   const body=file??text,csv=file?/\.csv$/i.test(file.name)||file.type==='text/csv':!/^\s*[[{]/.test(text);
   state.payload={body,type:csv?'text/csv':'application/json',source};
   state.preview=await send(`/api/definitions/preview?${query({source})}`,state.payload);showPreview();
  },{pending:'Previewing…',success:'Preview ready. Nothing has changed yet.'});
 }
 async function applyPreview(){
  const {payload,preview}=state;if(!payload||!preview)return;
  const result=await action(()=>send(`/api/definitions/apply?${query({source:payload.source,digest:preview.digest})}`,payload),{pending:'Applying…',success:`Applied: ${preview.counts.create} created (paused), ${preview.counts.change} changed.`});
  if(result.ok){state.preview=null;state.payload=null;showPreview();await loadBatches()}
 }
 async function undo(id){
  const result=await action(()=>send(`/api/definitions/batches/${encodeURIComponent(id)}/undo`,{body:'{}',type:'application/json'}),{pending:'Undoing batch…',success:'Batch undone. Anything edited or enabled since was left as is.'});
  if(result.ok)await loadBatches();
 }
 $('#definition-preview').onclick=preview;
 $('#definition-export').onclick=()=>action(async()=>{const file=await send('/api/definitions/export'),url=URL.createObjectURL(new Blob([JSON.stringify(file,null,2)+'\n'],{type:'application/json'})),a=document.createElement('a');a.href=url;a.download='uptime.monitors.json';a.click();URL.revokeObjectURL(url)},{success:'Current definitions exported.'});
 showPreview();showBatches();loadBatches().catch(error=>{$('#definition-batches').innerHTML=`<p>${escape(error.message)}</p>`});
}
