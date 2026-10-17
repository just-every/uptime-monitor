// IMPORT-001: definition files and UptimeRobot imports through the Worker's owner door. Preview writes nothing;
// hostile, malformed, stale, cross-origin and unauthenticated requests leave every table byte-identical; apply is
// exactly the preview, creates paused monitors and touches no destination, heartbeat or notification; export
// round-trips; undo pauses or restores without removing history; apply and undo keep a heartbeat's live token.
// MIGRATE-001 shares this journey: accepted status sets/ranges and both keyword conditions survive every door.
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {readFile} from 'node:fs/promises';
import {migratedWorker,runtimeDirectory,cleanupDirectory,availablePorts} from './notify-acceptance-runtime.mjs';
import {testAuthBindings,authRequest,sessionCookie,finishSetup} from './auth-test-support.mjs';
import {populateArchiveFixture} from './archive-fixture.mjs';

const directory=await runtimeDirectory(),[port]=await availablePorts(1),origin=`http://127.0.0.1:${port}`;
const savedRobot=await readFile('tests/fixtures/uptimerobot-v3-monitors.synthetic.json');
const robotMonitor={type:'HTTP',url:'https://migration.example.test/',interval:300,timeout:5,httpMethodType:'GET'};
const migrationCases=[
 {...robotMonitor,friendlyName:'Status set',successHttpResponseCodes:['200',201,'204','206-208','3xx']},
 {...robotMonitor,friendlyName:'Default accepted statuses'},
 {...robotMonitor,friendlyName:'Case-insensitive absence',type:'KEYWORD',keywordType:'ALERT_EXISTS',keywordValue:'Internal error',keywordCaseType:'CaseInsensitive',successHttpResponseCodes:['200-299']},
 {...robotMonitor,friendlyName:'Unmapped response codes',successHttpResponseCodes:['200','unknown']},
 {...robotMonitor,friendlyName:'Reversed response range',successHttpResponseCodes:['299-200']},
];
const uptimeRobot=Buffer.concat([savedRobot,Buffer.from('\n'+JSON.stringify({data:migrationCases,nextLink:null}))]);
const uptimeRobotLog=await readFile('tests/fixtures/uptimerobot-logs.synthetic.csv');
const file=monitors=>JSON.stringify({format:'uptime.monitors',version:1,monitors});
let worker;
try{
 let DB;({worker,DB}=await migratedWorker({port,persist:join(directory,'local'),bindings:{UPTIME_ORIGIN:origin}}));
 const call=authRequest(origin);
 const cookie=sessionCookie(await call('/auth/bootstrap',{token:testAuthBindings.SETUP_TOKEN,displayName:'Import owner'}));
 await finishSetup(origin,cookie);
 await populateArchiveFixture({...testAuthBindings,UPTIME_ORIGIN:origin,DB});
 const send=(path,body,{type='application/json',headers={},method='POST'}={})=>fetch(origin+path,{method,headers:{origin,cookie,...(body===undefined?{}:{'content-type':type}),...headers},body});
 const json=async response=>({status:response.status,body:await response.json()});
 // Every table, every row, in storage order. Reads in this test never write.
 async function scan(){
  const names=(await DB.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'_cf_METADATA' ORDER BY name").all()).results.map(r=>r.name);
  const rows=[];
  for(let offset=0;offset<names.length;offset+=32){
   const chunk=names.slice(offset,offset+32),results=await DB.batch(chunk.map(name=>DB.prepare(`SELECT * FROM "${name}" ORDER BY rowid`)));
   rows.push(...chunk.map((name,index)=>[name,results[index].results]));
  }
  return Object.fromEntries(rows);
 }
 const unchanged=async(label,attempt)=>{const before=await scan(),result=await attempt();assert.deepEqual(await scan(),before,`${label} leaves the installation byte-identical`);return result};
 const preview=async(text,source='auto',type)=>json(await send(`/api/definitions/preview?source=${source}`,text,{type}));
 const apply=async(text,digest,source='auto')=>json(await send(`/api/definitions/apply?source=${source}&digest=${digest}`,text));

 // Refusals at the door.
 const valid=file([{name:'Door check',url:'https://door.example.test/'}]);
 await unchanged('an oversized file',async()=>assert.equal((await send('/api/definitions/preview',`{"format":"uptime.monitors","version":1,"monitors":[],"pad":"${'x'.repeat(2*1024*1024)}"}`)).status,413));
 await unchanged('malformed JSON',async()=>{const r=await preview('{"format":"uptime.monitors","version":1,"monitors":[');assert.equal(r.status,400);assert.match(r.body.error,/not valid JSON/)});
 await unchanged('non-UTF-8 bytes',async()=>assert.equal((await send('/api/definitions/preview',new Uint8Array([0x7b,0xff,0xfe,0x7d]))).status,400));
 await unchanged('the wrong file type',async()=>{for(const [text,source] of [[uptimeRobot,'uptime.monitors'],['[1,2,3]','auto'],['{"monitors":[]}','uptimerobot']]){const r=await preview(text,source);assert.equal(r.status,400,source);assert.ok(r.body.error)}});
 await unchanged('too many monitors',async()=>{const r=await preview(file(Array.from({length:501},(_,i)=>({name:`m${i}`,url:'https://example.test/'}))));assert.equal(r.status,400);assert.match(r.body.error,/501 monitors/)});
 const hostile=file([{name:'Private address',url:'http://10.0.0.8/'},{name:'Userinfo',url:'https://admin@intranet.example.test/'},{name:'Credential in URL',url:'https://user:secret@example.test/'},{name:'Not HTTP',url:'ftp://files.example.test/'},{name:'Loopback',url:'http://127.0.0.1:9/'},
  {name:'Header secret',url:'https://example.test/',headers:{authorization:'Bearer secret'}},{name:'Body secret',url:'https://example.test/',body:'password=secret'},{name:'Unknown field',url:'https://example.test/',assignments:{}},
  {name:'Header assertion secret',url:'https://example.test/',assertions:{header:{name:'x-a',equals:'b',authorization:'Bearer secret'}}},{name:'JSON assertion secret',url:'https://example.test/',assertions:{json:{path:'a',equals:1,token:'password=secret'}}}]);
 await unchanged('hostile and secret-bearing entries',async()=>{
  const r=await preview(hostile);assert.equal(r.status,200);assert.equal(r.body.counts.refused,10);assert.equal(r.body.counts.create,0);
  for(const entry of r.body.entries)assert.match(entry.reason,/^Entry \d+ \(/,'each refusal names its entry');
  assert.match(r.body.entries[8].reason,/“authorization”/);assert.match(r.body.entries[9].reason,/“token”/);
  assert.ok(!JSON.stringify(r.body).includes('Bearer secret')&&!JSON.stringify(r.body).includes('password=secret'),'request secrets are never echoed');
  const refused=await apply(hostile,r.body.digest);assert.equal(refused.status,400);assert.match(refused.body.error,/nothing to create or change/);
 });
 await unchanged('duplicate names in a preview',async()=>{const r=await preview(file([{name:'Twice',url:'https://a.example.test/'},{name:'Twice',url:'https://b.example.test/'}]));assert.deepEqual([r.body.entries[0].action,r.body.entries[1].action],['create','refused']);assert.match(r.body.entries[1].reason,/Entry 2 \(“Twice”\): duplicate name; Entry 1 \(“Twice”\)/)});
 await unchanged('a cross-origin apply',async()=>{const digest=(await preview(valid)).body.digest;for(const headers of [{origin:'https://attacker.example.test'},{'sec-fetch-site':'cross-site'}])assert.equal((await send(`/api/definitions/apply?digest=${digest}`,valid,{headers})).status,403)});
 await unchanged('requests without an owner session',async()=>{const digest=(await preview(valid)).body.digest;for(const path of ['/api/definitions/preview',`/api/definitions/apply?digest=${digest}`,'/api/definitions/batches/x/undo'])assert.equal((await fetch(origin+path,{method:'POST',headers:{origin,'content-type':'application/json'},body:valid})).status,401);assert.equal((await fetch(origin+'/api/definitions/export')).status,401)});
 await unchanged('a CSV log export',async()=>{const r=await preview(uptimeRobotLog,'auto','text/csv');assert.equal(r.status,200);assert.equal(r.body.kind,'history');assert.equal(r.body.rows,3);assert.equal(r.body.stored,false);assert.equal((await apply(uptimeRobotLog,'0'.repeat(64))).status,400)});
 const stalePreview=(await preview(valid)).body;
 await DB.prepare("INSERT INTO monitors VALUES('elsewhere',1,?,0,'{\"state\":\"Unknown\"}')").bind(JSON.stringify({name:'Added elsewhere',type:'http',network:'public',url:'https://elsewhere.example.test/',method:'GET',probes:['local-a'],assertions:{status:200},intervalMs:60000,timeoutMs:2000,retries:1,failureQuorum:1,confirmSlots:2,recoverSlots:2,freshnessMs:180000,enabled:false,maintenanceUntil:0})).run();
 await unchanged('a stale digest',async()=>{const r=await apply(valid,stalePreview.digest);assert.equal(r.status,409);assert.match(r.body.error,/changed since this preview/)});
 await DB.prepare("DELETE FROM monitors WHERE id='elsewhere'").run();

 // UptimeRobot import: counts first, then exactly the preview, paused, nothing else touched.
 const imported=await unchanged('an UptimeRobot preview',()=>preview(uptimeRobot));
 assert.deepEqual(imported.body.counts,{create:9,unchanged:0,change:0,refused:2,unsupported:6});
 assert.deepEqual(imported.body.outcomes,{importable:0,withDifferences:9,unsupported:6,refused:2});
 const before=await scan(),applied=await apply(uptimeRobot,imported.body.digest);assert.equal(applied.status,201);assert.equal(applied.body.created.length,9);
 assert.equal(applied.body.preview.digest,imported.body.digest,'apply uses the reviewed plan');
 const after=await scan();
 for(const table of ['notification_destinations','notification_credentials','notification_jobs','outbox','slots','observations','heartbeat_authority','integration_deliveries'])assert.deepEqual(after[table],before[table],`import does not touch ${table}`);
 const created=after.monitors.filter(m=>applied.body.created.includes(m.id)).map(m=>JSON.parse(m.definition));
 assert.equal(created.length,9);assert.ok(created.every(d=>d.enabled===false),'imported monitors arrive paused');
 assert.equal(created.find(d=>d.name==='Partner API').request.credentialRef,'uptimerobot-9000009');assert.equal(created.find(d=>d.name==='Nightly backup').type,'heartbeat');
 const expectedAssertions={
  'Shop home':{status:200},
  'Docs':{status:['2xx','3xx']},
  'Checkout keyword':{status:['2xx','3xx'],bodyIncludes:'Add to cart'},
  'Error banner absent':{status:['2xx','3xx'],bodyExcludes:'Internal error'},
  'Status set':{status:[200,201,204,'206-208','3xx']},
  'Default accepted statuses':{status:['2xx','3xx']},
  'Case-insensitive absence':{status:['200-299'],bodyExcludes:'Internal error'},
 };
 const readImported=async()=>Promise.all(applied.body.created.map(async id=>{
  const read=await json(await send(`/api/monitors/${id}/revisions`,undefined,{method:'GET'}));assert.equal(read.status,200);return read.body[0].definition;
 }));
 for(const definition of await readImported()){
  assert.equal(definition.enabled,false,'the owner reads every migrated monitor paused');
  if(expectedAssertions[definition.name])assert.deepEqual(definition.assertions,expectedAssertions[definition.name],`${definition.name} keeps the complete accepted responses and keyword condition`);
  const reviewed=imported.body.entries.find(entry=>entry.name===definition.name);assert.deepEqual(definition.assertions,reviewed.target.assertions,'apply preserves the reviewed check criteria');
 }
 for(const name of ['Checkout keyword','Case-insensitive absence'])assert.ok(imported.body.entries.find(entry=>entry.name===name).differences.some(d=>/ignoring case/.test(d)),'case-insensitive conditions name their semantic difference');
 const currentPreview=await unchanged('the same source after apply',()=>preview(uptimeRobot));
 assert.equal(currentPreview.body.counts.unchanged,9,'the same source previews as unchanged after apply');
 const changedSource=Buffer.from(uptimeRobot.toString().replace('206-208','206-209'));
 const changedCodes=await unchanged('a changed status-range preview',()=>preview(changedSource));
 assert.equal(changedCodes.body.counts.change,1,'changing the accepted set is a real monitor change');
 assert.notEqual(changedCodes.body.digest,currentPreview.body.digest,'accepted status ranges contribute to the review digest');
 await unchanged('a digest with changed status semantics',async()=>assert.equal((await apply(changedSource,currentPreview.body.digest)).status,409));
 assert.equal((await apply(uptimeRobot,imported.body.digest)).status,409,'a second apply of the same preview is stale');
 // A daily heartbeat with a grace period imports with the grace shortened to the one-day ceiling, and the preview names the numbers.
 const daily=await unchanged('a daily UptimeRobot heartbeat preview',()=>preview(JSON.stringify({data:[{id:1,friendlyName:'Daily export',type:'HEARTBEAT',interval:86400,gracePeriod:300}],nextLink:null})));
 assert.equal(daily.body.entries[0].action,'create');assert.ok(daily.body.entries[0].differences.some(d=>d.includes('86,700 s')),'the preview names interval plus grace');

 // Export round-trips: the exported file previews as all unchanged.
 const exported=await json(await send('/api/definitions/export',undefined,{method:'GET'}));
 assert.equal(exported.status,200);assert.equal(exported.body.monitors.length,10);
 for(const [name,assertions] of Object.entries(expectedAssertions))assert.deepEqual(exported.body.monitors.find(m=>m.name===name).assertions,assertions,'export preserves migrated assertions');
 const roundTrip=await unchanged('previewing the exported file',()=>preview(JSON.stringify(exported.body)));
 assert.deepEqual(roundTrip.body.counts,{create:0,unchanged:10,change:0,refused:0,unsupported:0});

 // A change keeps the monitor's paused state and history; undo restores the earlier settings, history intact.
 const fixtureMonitor=(await DB.prepare("SELECT definition FROM monitors WHERE id='monitor'").first()).definition;
 assert.equal((await json(await send('/api/monitors/monitor',JSON.stringify({...JSON.parse(fixtureMonitor),enabled:false}),{method:'PATCH'}))).status,200);
 const history=async()=>Object.fromEntries(await Promise.all(['slots','observations','outbox','archive_history'].map(async t=>[t,(await DB.prepare(`SELECT * FROM ${t} ORDER BY rowid`).all()).results])));
 const historyBefore=await history();
 const edited=structuredClone(exported.body);edited.monitors.find(m=>m.name==='Archive health').url='https://example.test/moved';edited.monitors.find(m=>m.name==='Archive health').enabled=true;
 const changePreview=await preview(JSON.stringify(edited));assert.deepEqual(changePreview.body.counts,{create:0,unchanged:9,change:1,refused:0,unsupported:0});
 const change=changePreview.body.entries.find(e=>e.action==='change');assert.deepEqual(change.changes.map(c=>c.field),['url']);assert.match(change.differences[0],/stays paused/);
 const changed=await apply(JSON.stringify(edited),changePreview.body.digest);assert.equal(changed.status,201);
 const moved=JSON.parse((await DB.prepare("SELECT definition FROM monitors WHERE id='monitor'").first()).definition);assert.equal(moved.url,'https://example.test/moved');assert.equal(moved.enabled,false);
 const undone=await json(await send(`/api/definitions/batches/${changed.body.batchId}/undo`,'{}'));
 assert.equal(undone.status,200);assert.deepEqual(undone.body.outcomes.map(o=>o.outcome),['restored']);
 const restored=JSON.parse((await DB.prepare("SELECT definition FROM monitors WHERE id='monitor'").first()).definition);assert.equal(restored.url,'https://example.test/health');assert.equal(restored.enabled,false);
 assert.deepEqual(await history(),historyBefore,'apply and undo never remove or rewrite history');
 assert.equal((await json(await send(`/api/definitions/batches/${changed.body.batchId}/undo`,'{}'))).status,409,'a batch is undone once');

 // Undo pauses what a batch created, and reports a monitor edited since instead of overriding it.
 const pair=file([{name:'Undo kept',url:'https://kept.example.test/'},{name:'Undo edited',url:'https://edited.example.test/'}]);
 const pairBatch=(await apply(pair,(await preview(pair)).body.digest)).body;
 const editedId=pairBatch.created[1],editedDefinition=JSON.parse((await DB.prepare('SELECT definition FROM monitors WHERE id=?').bind(editedId).first()).definition);
 assert.equal((await json(await send(`/api/monitors/${editedId}`,JSON.stringify({...editedDefinition,timeoutMs:3000}),{method:'PATCH'}))).status,200);
 const pairUndo=(await json(await send(`/api/definitions/batches/${pairBatch.batchId}/undo`,'{}'))).body;
 assert.deepEqual(pairUndo.outcomes.map(o=>[o.name,o.outcome]),[['Undo kept','paused'],['Undo edited','edited-since']]);
 const batches=(await json(await send('/api/definitions/batches',undefined,{method:'GET'}))).body;
 assert.deepEqual(batches.map(b=>b.undoneAt!==null),[true,true,false]);assert.deepEqual(batches[0].entries.map(e=>e.undoOutcome),['paused','edited-since']);
 const migrationUndo=await json(await send(`/api/definitions/batches/${applied.body.batchId}/undo`,'{}'));
 assert.equal(migrationUndo.status,200);assert.equal(migrationUndo.body.outcomes.length,9);assert.ok(migrationUndo.body.outcomes.every(o=>o.outcome==='paused'));
 for(const definition of await readImported()){
  assert.equal(definition.enabled,false,'undo keeps the migrated monitor paused');
  if(expectedAssertions[definition.name])assert.deepEqual(definition.assertions,expectedAssertions[definition.name],'undo preserves the imported assertions and history');
 }
 // A file that changes a live heartbeat, and its undo, keep the token the owner's jobs use.
 const heartbeat=await json(await send('/api/monitors',JSON.stringify({name:'Nightly job',type:'heartbeat',canaryUrl:'https://canary.example.test/health',heartbeat:{maxAgeMs:600000}})));assert.equal(heartbeat.status,201);
 const token=(await json(await send(`/api/monitors/${heartbeat.body.id}/heartbeat-key`,'{}'))).body.token;let sequence=0;
 const ping=async()=>(await fetch(`${origin}/api/heartbeats/${heartbeat.body.id}`,{method:'POST',headers:{'content-type':'application/json',authorization:`Bearer ${token}`,'x-heartbeat-sequence':String(++sequence)},body:'{}'})).status;
 assert.equal(await ping(),200);
 const longer=file([{name:'Nightly job',type:'heartbeat',heartbeat:{maxAgeMs:900000}}]),longerPreview=(await preview(longer)).body;
 assert.equal(longerPreview.entries[0].action,'change');assert.deepEqual(longerPreview.entries[0].differences,[]);
 const longerBatch=(await apply(longer,longerPreview.digest)).body;assert.equal(await ping(),200,'a pinged heartbeat keeps its token across apply');
 assert.equal((await json(await send(`/api/definitions/batches/${longerBatch.batchId}/undo`,'{}'))).status,200);assert.equal(await ping(),200,'and across undo');
 const toHttp=(await preview(file([{name:'Nightly job',url:'https://job.example.test/'}]))).body.entries[0];assert.equal(toHttp.action,'change');assert.equal(toHttp.differences.length,1,'the preview says when the token would be retired');
 console.log('IMPORT-001 / MIGRATE-001 passed: Worker-door refusals leave every table byte-identical; UptimeRobot preview, paused apply, export round-trip, change and undo with history intact.');
}finally{await worker?.dispose();await cleanupDirectory(directory)}
