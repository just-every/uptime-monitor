// One public-door journey: synthetic saved UptimeRobot pages, signed grants,
// native zero-write preview, reviewed apply, server readback, and both undo doors.
import assert from 'node:assert/strict';
import {createHash,randomUUID,webcrypto} from 'node:crypto';
import {join} from 'node:path';
import {migratedWorker,runtimeDirectory,cleanupDirectory,availablePorts} from './notify-acceptance-runtime.mjs';
import {testAuthBindings,authRequest,sessionCookie,finishSetup} from './auth-test-support.mjs';

const directory=await runtimeDirectory(),[port]=await availablePorts(1),origin=`http://127.0.0.1:${port}`;
const keys=await webcrypto.subtle.generateKey({name:'ECDSA',namedCurve:'P-256'},true,['sign','verify']);
const publicKey=await webcrypto.subtle.exportKey('jwk',keys.publicKey),kid='business-test';
const bindings={...testAuthBindings,UPTIME_ORIGIN:origin,RER_OWNER_CLAIM_ISSUER:'https://business.example.test',
 RER_OWNER_CLAIM_INSTALLATION_ID:'synthetic-uptime-installation',RER_OWNER_CLAIM_APP_ORIGIN:origin,
 RER_OWNER_CLAIM_PUBLIC_KEYS:JSON.stringify({[kid]:publicKey})};
const source=Buffer.from(JSON.stringify({data:[
 {id:1001,friendlyName:'Checkout API',type:'HTTP',url:'https://checkout.example.test/health',interval:300,timeout:5,httpMethodType:'GET',successHttpResponseCodes:['200','201']},
 {id:1002,friendlyName:'Storefront',type:'KEYWORD',url:'https://shop.example.test/',interval:300,timeout:5,httpMethodType:'GET',keywordType:'ALERT_NOT_EXISTS',keywordValue:'Add to cart',keywordCaseType:'CaseSensitive'},
 {id:1003,friendlyName:'Legacy ping',type:'PING',url:'https://legacy.example.test/',interval:300},
],nextLink:null})+'\n');
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const approval={approvalRecordId:'synthetic-owner-browser-approval',approvedAt:new Date().toISOString()};
let worker;
try{
 const runtime=await migratedWorker({port,persist:join(directory,'local'),bindings});worker=runtime.worker;const {DB}=runtime;
 const cookie=sessionCookie(await authRequest(origin)('/auth/bootstrap',{token:bindings.SETUP_TOKEN,displayName:'Migration owner'}));await finishSetup(origin,cookie);
 const owner=async(path,body)=>{const r=await fetch(origin+path,{method:body===undefined?'GET':'POST',headers:{origin,cookie,'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)});return {status:r.status,body:await r.json()}};
 const settings=body=>owner('/api/business-import-settings',body);
 async function grant(action,extra={},namespace='uptimerobot:saved-monitors'){
  const now=Math.floor(Date.now()/1000),header=Buffer.from(JSON.stringify({alg:'ES256',typ:'rer-import-grant+jwt',kid})).toString('base64url');
  const payload=Buffer.from(JSON.stringify({v:1,iss:bindings.RER_OWNER_CLAIM_ISSUER,aud:origin,installationId:bindings.RER_OWNER_CLAIM_INSTALLATION_ID,email:bindings.OWNER_EMAIL,
   jti:randomUUID(),iat:now,exp:now+300,action,namespace,maxBytes:2*1024*1024,maxChunks:32,maxRecords:500,...extra})).toString('base64url');
  return header+'.'+payload+'.'+Buffer.from(await webcrypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},keys.privateKey,Buffer.from(header+'.'+payload))).toString('base64url');
 }
 const door=async(action,body,extra={},namespace)=>{const token=await grant(action,extra,namespace);const r=await fetch(origin+'/api/rer/import/'+action,{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+token},body:JSON.stringify(body)});return {status:r.status,body:await r.json()}};
 const good=async(action,body,extra,namespace)=>{const r=await door(action,body,extra,namespace);assert.equal(r.status,200,`${action}: ${JSON.stringify(r.body)}`);return r.body};
 const snapshot=async()=>{const names=(await DB.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'ik_%' AND name<>'_cf_METADATA' ORDER BY name").all()).results.map(row=>row.name);return Object.fromEntries(await Promise.all(names.map(async name=>[name,(await DB.prepare(`SELECT * FROM "${name}" ORDER BY rowid`).all()).results])))};

 assert.equal((await door('uploads',{})).status,404,'off by default before grant/body admission');
 assert.equal((await settings()).body.enabled,false);
 assert.equal((await fetch(origin+'/api/business-import-settings')).status,401);
 assert.equal((await settings({enabled:true,revokedKeyIds:[]})).status,200);
 // Establish the actual runner using Uptime's ordinary console door.
 assert.equal((await owner('/api/overview')).status,200);
 const upload=await good('uploads',{name:'uptimerobot.synthetic.ndjson',bundleSha256:hash(source),totalBytes:source.length,totalChunks:1,index:0,chunkSha256:hash(source),base64:source.toString('base64')});
 const beforePlan=await snapshot(),plan=await good('plan',{uploadId:upload.uploadId});
 assert.deepEqual(await snapshot(),beforePlan,'planning writes no native or installation product state');
 assert.equal(plan.counts.input,3);assert.equal(plan.counts.effects,2);assert.equal(plan.rejected.length,1);
 assert.match(plan.rejected[0].reason,/not supported/);
 assert.equal((await door('apply',{planId:plan.planId,reviewedFingerprint:'0'.repeat(64)},{...approval,fingerprint:plan.fingerprint})).status,403);
 assert.deepEqual(await snapshot(),beforePlan,'wrong reviewed fingerprint cannot write');
 let applied=await good('apply',{planId:plan.planId,reviewedFingerprint:plan.fingerprint},{...approval,fingerprint:plan.fingerprint});
 for(let n=0;applied.job.state!=='completed'&&n<10;n++)applied=await good('apply',{planId:plan.planId,reviewedFingerprint:plan.fingerprint},{...approval,fingerprint:plan.fingerprint});
 assert.equal(applied.job.state,'completed');
 const rows=(await DB.prepare('SELECT * FROM monitors ORDER BY id').all()).results;
 assert.equal(rows.length,2);assert.ok(rows.every(row=>JSON.parse(row.definition).enabled===false));
 assert.deepEqual(JSON.parse(rows.find(row=>JSON.parse(row.definition).name==='Checkout API').definition).assertions.status,[200,201]);
 for(const name of ['observations','slots','outbox','notification_jobs','notification_destinations','heartbeat_authority'])assert.deepEqual((await snapshot())[name],beforePlan[name],`${name} stays inert`);
 const reconciliation=await good('reconcile',{planId:plan.planId});assert.equal(reconciliation.ok,true);assert.equal(reconciliation.counts.sourceRecords,3);assert.equal(reconciliation.counts.nativeTargets,2);
 const status=await good('status',{planId:plan.planId});assert.equal(status.approval.approval_id,approval.approvalRecordId);
 const history=(await owner('/api/definitions/batches')).body,batch=history.find(row=>row.id==='business:'+plan.planId);
 assert.equal(batch.businessApproval.fingerprint,plan.fingerprint);assert.equal(batch.businessApproval.owner,bindings.OWNER_EMAIL);assert.equal(batch.entries.length,2);
 const undone=await good('undo',{planId:plan.planId});assert.equal(undone.job.state,'undone');
 assert.deepEqual((await DB.prepare('SELECT * FROM monitors ORDER BY id').all()).results,rows,'undo keeps paused native monitors and their history');
 assert.equal((await owner('/api/definitions/batches')).body.find(row=>row.id===batch.id).undone,true);
 // The ordinary owner history remains an undo door after Business is disabled or its key revoked.
 assert.equal((await settings({enabled:false,revokedKeyIds:[kid]})).status,200);
 assert.equal((await door('status',{planId:plan.planId})).status,404);
 assert.equal((await owner('/api/definitions/batches/'+encodeURIComponent(batch.id)+'/undo',{})).status,200);
 assert.equal((await settings({enabled:true,revokedKeyIds:[]})).status,200);
 assert.deepEqual((await settings()).body.revokedKeyIds,[kid]);
 assert.equal((await door('status',{planId:plan.planId})).status,403,'revoked keys remain revoked after re-enabling');
 const erased=rows.find(row=>JSON.parse(row.definition).name==='Checkout API');
 const erasure=await owner('/api/monitors/'+erased.id+'/erase',{name:'Checkout API'});assert.equal(erasure.status,200,JSON.stringify(erasure.body));
 assert.equal(await DB.prepare('SELECT id FROM monitors WHERE id=?').bind(erased.id).first(),null);
 assert.equal((await DB.prepare('SELECT COUNT(*) n FROM monitors').first()).n,1,'peer native monitor survives source custody retirement');
 assert.equal((await DB.prepare('SELECT COUNT(*) n FROM ik_door_chunks').first()).n,0,'the whole source original is withdrawn');
 assert.equal((await DB.prepare('SELECT COUNT(*) n FROM ik_door_plan_chunks').first()).n,0,'retained door plan copies are withdrawn');
 assert.equal((await DB.prepare('SELECT COUNT(*) n FROM ik_jobs').first()).n,0,'affected import custody is retired');
 console.log('Business import door passed: opt-in off 404; synthetic upload, zero-write native plan, fingerprint apply, reconciliation, Business history, undo, durable key revocation.');
}finally{await worker?.dispose();await cleanupDirectory(directory)}
