import {saveNotificationPreferences} from './domain/notification-preferences.server.ts'
import {proposedWebhookPreview,savedWebhookPreview} from './domain/notification-webhook.ts'
// @ts-ignore Source definition parser shared with the owner preview.
import {previewUptimeRobotWebhook} from './domain/webhook-contact-import.mjs'
import {configureHeartbeat,heartbeatRuns} from './domain/heartbeat-jobs.server.ts'
import {catalogBulkPreview,catalogBulkApply,catalogAddPreview,catalogAddApply} from './domain/catalog.server.ts'
import {readAlertRoute,previewAlertRoute,applyAlertRoute} from './domain/alert-routes.server.ts'
import {previewMaintenance,applyMaintenance,cancelMaintenance} from './domain/planned-maintenance.server.ts'
// The package's demo module: operator-auth runs from the package and reads demo mode from this same instance.
import {withDemo} from '@runeditrun/rer/modules/demo'
import {kickHostedWork} from './domain/worker-kick.server.ts'
import {recordCronRun} from './domain/cron-run.server.ts'
import {scheduledPublicChecks} from './domain/worker-checks.server.ts'
import {seedDemo} from './demo.ts'
import {demoBindings} from './domain/demo.server.ts'
import {incidentWorkRequest} from './domain/incident-work/handler.server.ts'
import {incidentList} from './domain/incident-list.server.ts'
import {incidents} from './domain/incidents.server.ts'
import {readApiOpenApi} from './domain/read-api-openapi.ts'
import {monitorReport,monitorLatency,reportCsv,ReportInputError} from './domain/reports/index.server.ts'
import {monitoringWatch} from './domain/monitoring-watch.server.ts'
import {scheduledNotifications} from './domain/notification-worker.server.ts'
import {SubrequestBudget,RUN_SUBREQUESTS,CHECK_SUBREQUESTS,WATCH_SUBREQUESTS} from './domain/worker-budget.ts'
import {createDiscovery,readDiscovery,claimDiscovery,completeDiscovery,discoveryAnswer,DISCOVERY_RESULT_BYTES} from './domain/discovery.server.ts'
import {readExternalCheck,saveExternalCheck} from './domain/external-check.server.ts'
// @ts-ignore Shared bounded credential diagnostics.
import {runnerResultLimit} from './domain/credential-binding.mjs'
import {signedRunnerBody} from './domain/runner-signature.server.ts'
import {enrollRunner,authenticateRunner,createRunner,revokeRunner,listRunners,ensureFirstRunner,type Runner} from './domain/runner-registration.server.ts'
import {configurationResponse,validateWorkerConfiguration} from './domain/startup-config.mjs'
import {isPublicDocument,publicPath} from './domain/public-paths.ts'
import {limitPublicRequest} from './domain/public-rate-limits.server.ts'
import {erase} from './domain/erasure.server.ts'
import {previewDefinitions,applyDefinitions,exportDefinitions} from './domain/definitions/index.server.ts'
import {businessImportDoor} from './domain/business-import/door.server.ts'
import {businessImportSettings} from './domain/business-import/settings.server.ts'
import {definitionImportHistory,undoDefinitionImport} from './domain/business-import/history.server.ts'
import {exportJournal} from './domain/archive-fence.server.ts'
import {operatorSession,authenticatedSession} from './domain/operator-auth.server.ts'
import {accessDeniedDocument} from './domain/access-document.server.ts'
import {boundedHealth,health} from './domain/health.server.ts'
import {monitorEditor} from './domain/monitor-editor.server.ts'
import {destinations,createDestination,updateDestination,rotateDestination} from './domain/notification-destinations.server.ts'
import {notificationOverview,testDestination} from './domain/notifications.server.ts'
import {notificationRpc} from './domain/notification-rpc.server.ts'
import {startDrill} from './domain/alert-drills.server.ts'
import {alertDrillDocument} from './domain/alert-drill-document.server.ts'
import {isPublicAsset} from './domain/assets.server.ts'
import {classifyStorage,storageLogLine,storageOperation} from './domain/storage-error.ts'
import {maintenance} from './domain/maintenance.server.ts'
import {authRoute,authFailure,developmentMail,signOut,signInDocument} from './domain/operator-auth.server.ts'
import {authDocument,nativeClaimFailure} from './domain/auth-document.server.ts'
import {firstRun,firstRunComplete,setupRedirect} from './domain/first-run.server.ts'
import {ownerClaim} from './domain/owner-claim.server.ts'
import {editorConfigResponse,editorDocument} from './domain/editor.server.ts'
import {notFoundDocument} from './domain/document-shell.server.ts'
import {issueHeartbeatKey,receiveHeartbeat} from './domain/heartbeat.server'
import start from '@tanstack/react-start/server-entry'
import {Hono} from 'hono'
import {dispatch} from './base/dispatch'
import {lobbyResponse} from './base/lobby'
import {claimedWhen,setupStateResponse} from './base/setup-state'
import {identity} from './identity'
import {recordContact} from './domain/runner-contact.server.ts'
import {session,overview,save,get,claim,complete,history,revisions,archive,restore,type Bindings} from './domain/control.server'
import {checkInput,InputError} from './domain/request-boundary.ts'
const api=new Hono<{Bindings:Bindings;Variables:{requestText:string;input:any;runner:Runner}}> ()
api.get('/health',c=>health(c.env))
api.use('/api/*',async(c,next)=>{c.header('cache-control','no-store');const origin=c.req.header('origin');if(c.env.UPTIME_LOCAL_REVIEW!=='true'&&['POST','PATCH','DELETE'].includes(c.req.method)&&!c.req.path.startsWith('/api/runner/')&&!c.req.path.startsWith('/api/heartbeats/')&&origin!==c.env.UPTIME_ORIGIN)return c.json({error:'Same origin required'},403);if(c.req.header('sec-fetch-site')==='cross-site'||(origin&&origin!==new URL(c.req.url).origin))return c.json({error:'Same origin required'},403);if(Number(c.req.header('content-length')??0)>2*1024*1024)return c.json({error:'Payload too large'},413);if(['POST','PATCH'].includes(c.req.method)&&c.req.header('content-type')!=='application/json'&&!(c.req.path==='/api/definitions/preview'&&c.req.header('content-type')==='text/csv'))return c.json({error:'JSON required'},415);if(['POST','PATCH'].includes(c.req.method)){const reader=c.req.raw.body?.getReader(),decoder=new TextDecoder('utf-8',{fatal:true});let size=0,text='';if(reader){try{while(true){const {done,value}=await reader.read();if(done)break;size+=value.byteLength;if(size>(c.req.path.startsWith('/api/heartbeats/')?10240:2*1024*1024)){return c.json({error:'Payload too large'},413)}text+=decoder.decode(value,{stream:true})}text+=decoder.decode()}catch(error){if(error instanceof TypeError)return c.json({error:'Invalid UTF-8 request body'},400);throw error}finally{reader.releaseLock()}}c.set('requestText',text)}await next()})
// Every route's path, query and body schema, once the caller's authority is known and before any service (request-boundary.ts).
// Heartbeats carry their own token, so their input is checked before their handler.
const boundary=async(c:any,next:()=>Promise<void>)=>{const headers=c.req.raw.headers,hasBody=Number(headers.get('content-length')??0)>0||headers.has('transfer-encoding');let checked;try{checked=checkInput(c.req.method==='HEAD'?'GET':c.req.method,new URL(c.req.url),c.get('requestText')??'',hasBody)}catch(error){if(error instanceof InputError)return c.json({error:error.message},400);throw error}if(!checked)return c.json({error:'Not found'},404);c.set('input',checked.body);await next()}
api.use('/api/heartbeats/*',boundary)
api.post('/api/heartbeats/:id',async c=>{if(c.get('requestText')!=='{}')return c.json({error:'Empty JSON object required'},400);return c.json(await receiveHeartbeat(c.env,c.req.param('id'),c.req.header('authorization')??'',c.req.header('x-heartbeat-sequence')??''))})
for(const state of ['start','success','fail'] as const)for(const path of state==='fail'?['/api/heartbeats/:id/fail','/api/heartbeats/:id/fail/:exitCode']:[`/api/heartbeats/:id/${state}`])api.post(path,async c=>{
 if(new TextEncoder().encode(c.get('requestText')).byteLength>10240)return c.json({error:'Heartbeat body exceeds 10 KiB'},413);
 const body=c.get('requestText')?JSON.parse(c.get('requestText')):{};
 return c.json(await receiveHeartbeat(c.env,c.req.param('id')!,c.req.header('authorization')??'',c.req.header('x-heartbeat-sequence')??'',state,c.req.param('exitCode')===undefined?null:Number(c.req.param('exitCode')),body));
});
api.use('/api/*',async(c,next)=>{if(c.req.path.startsWith('/api/runner/')){const row=await storageOperation('runner.authenticate',()=>authenticateRunner(c.env,c.req.header('x-runner-id'),c.req.header('x-runner-key')??''));if(!row)return c.json({error:'Runner authorization required'},401);c.set('runner',row)}else if(!await storageOperation('owner.session',()=>session(c.env,c.req.raw)))return c.json({error:'Owner session required'},401);await next()})
api.use('/api/*',boundary)
// Only authenticated owner requests can kick hosted work; public traffic and private runners cannot.
api.use('/api/*',async(c,next)=>{await next();if(c.res.ok&&(c.req.path==='/api/overview'||/^\/api\/(monitors|notifications)(\/|$)/.test(c.req.path))){c.executionCtx.waitUntil(kickHostedWork(c.env).catch(()=>console.error('Hosted console work failed; queued work remains available to Cron.')))}})
api.post('/api/auth/sign-out',async c=>{try{const result=await signOut(c.env,c.req.raw);c.header('set-cookie',result.cookie);return c.json({signedOut:true})}catch(error){return authFailure(error)}})
api.get('/api/overview',async c=>{const monitorId=c.req.query('monitor');return c.json(await overview(c.env,monitorId===undefined?{}:{monitorId}))})
for(const [path,preview,apply] of [
 ['/api/catalog',catalogBulkPreview,catalogBulkApply],['/api/catalog/add',catalogAddPreview,catalogAddApply],['/api/maintenance',previewMaintenance,applyMaintenance],
] as const){
 api.post(`${path}/preview`,async c=>c.json(await preview(c.env,c.get('input'))))
 api.post(`${path}/apply`,async c=>{const {digest,...input}=c.get('input') as any;return c.json(await apply(c.env,input,digest,{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id}),201)})
}
api.post('/api/maintenance/cancel',async c=>c.json(await cancelMaintenance(c.env,c.get('input'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.get('/api/monitor-editor',async c=>{c.header('cache-control','no-store, private');return c.json(await monitorEditor(c.env,c.req.query('monitor')))})
api.get('/api/monitors/:id/alert-route',async c=>c.json(await storageOperation('alertRoutes.read',()=>readAlertRoute(c.env,c.req.param('id')))))
api.post('/api/monitors/:id/alert-route/preview',async c=>c.json(await storageOperation('alertRoutes.preview',()=>previewAlertRoute(c.env,c.req.param('id'),c.get('input')))))
api.post('/api/monitors/:id/alert-route/apply',async c=>{const actor={kind:'operator' as const,ref:(await operatorSession(c.env,c.req.raw))!.operator.id};return c.json(await storageOperation('alertRoutes.apply',()=>applyAlertRoute(c.env,c.req.param('id'),c.get('input'),actor)))})
api.post('/api/monitors',async c=>c.json(await save(c.env,c.get('input'),undefined,{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id}),201))
api.patch('/api/monitors/:id',async c=>c.json(await save(c.env,c.get('input'),c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.post('/api/monitors/:id/heartbeat-key',async c=>c.json(await issueHeartbeatKey(c.env,c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.post('/api/monitors/:id/heartbeat-settings',async c=>c.json(await configureHeartbeat(c.env,c.req.param('id'),c.get('input'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})));
api.get('/api/monitors/:id/heartbeat-runs',async c=>{const monitor=await get(c.env,c.req.param('id'));if(!monitor)return c.json({error:'Monitor not found'},404);return c.json(await heartbeatRuns(c.env,monitor.id,{before:c.req.query('before')}))});
api.get('/api/monitors/:id/history',async c=>c.json(await history(c.env,c.req.param('id'))))
api.get('/api/monitors/:id/incidents',async c=>c.json(await incidents(c.env,c.req.param('id'))))
api.get('/api/monitors/:id/revisions',async c=>c.json(await revisions(c.env,c.req.param('id'))))
api.get('/api/v1/incidents',async c=>c.json(await incidentList(c.env,new URL(c.req.url).searchParams)))
for(const path of ['/api/incidents/:id','/api/v1/incidents/:id','/api/incident-settings'])api.get(path,c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.patch('/api/incident-settings',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.post('/api/incidents/:id/comments',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.patch('/api/incidents/:id/comments/:commentId',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.delete('/api/incidents/:id/comments/:commentId',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.patch('/api/incidents/:id/root-cause',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.post('/api/incidents/:id/actions',c=>incidentWorkRequest(c.env,c.req.raw,c.get('input')))
api.get('/api/v1/openapi.json',c=>c.json(readApiOpenApi))
// Versioned reads use the same immutable period engine as console downloads.
for(const suffix of ['uptime','report'] as const)api.get(`/api/v1/monitors/:id/${suffix}`,async c=>{
 const report=await monitorReport(c.env,c.req.param('id'),new URL(c.req.url).searchParams)
 if(!report)return c.json({error:'Monitor not found'},404)
 if(c.req.query('format')==='csv'||c.req.header('accept')?.includes('text/csv')){
  c.header('content-type','text/csv; charset=utf-8');c.header('content-disposition','attachment; filename="uptime-report.csv"');return c.body(reportCsv(report))
 }
 return c.json(report)
})
api.get('/api/v1/monitors/:id/response-times',async c=>{
 const result=await monitorLatency(c.env,c.req.param('id'),new URL(c.req.url).searchParams)
 return result?c.json(result):c.json({error:'Monitor not found'},404)
})

api.post('/api/monitors/:id/run',async c=>{const m=await get(c.env,c.req.param('id'));if(!m||!m.definition.enabled)return c.json({error:'Monitor unavailable or paused'},409);const id=`${m.id}:${m.revision}:manual:${crypto.randomUUID()}`;const inserted=await c.env.DB.prepare("INSERT INTO slots(id,monitor_id,revision,due_at,status,definition,lease_owner,lease_until,missed_count,manual) SELECT ?,?,?,?,'queued',?,'unclaimed',0,0,1 WHERE EXISTS(SELECT 1 FROM monitors WHERE id=?)").bind(id,m.id,m.revision,Date.now(),JSON.stringify(m.definition),m.id).run();if(!inserted.meta.changes)return c.json({error:'Monitor unavailable or erased'},409);return c.json({queued:true,slotId:id,policyAdvanced:false})})
api.get('/api/runners',async c=>c.json(await listRunners(c.env)))
api.post('/api/runners',async c=>c.json(await createRunner(c.env,c.get('input')),201))
api.delete('/api/runners/:id',async c=>c.json(await revokeRunner(c.env,c.req.param('id'))))
// The installation-wide External check URL: owner session only, same origin, validated before it is stored.
api.patch('/api/notifications/preferences',async c=>c.json(await saveNotificationPreferences(c.env,c.get('input'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.get('/api/external-check',async c=>c.json(await readExternalCheck(c.env)))
api.post('/api/external-check',async c=>c.json(await saveExternalCheck(c.env,c.get('requestText'))))
api.post('/api/runner/enroll',async c=>{const registration=await enrollRunner(c.env,c.get('requestText'),c.req.header('x-sent-at')??'',c.req.header('x-runner-signature')??'',c.get('runner'));return registration?c.json(registration):c.json({error:'Invalid signed enrolment'},403)})
api.post('/api/runner/claim',async c=>{const row=c.get('runner').runner_id?c.get('runner'):await ensureFirstRunner(c.env);const task=await storageOperation('runner.claim',()=>claim(c.env,row.runner_id));await storageOperation('runner.claim.contact',async()=>{await recordContact(c.env.DB);await c.env.DB.prepare('UPDATE runners SET last_contact_at=? WHERE runner_id=? AND revoked_at IS NULL').bind(Date.now(),row.runner_id).run()});
 // `now` is the Worker's clock at the claim: the runner aligns the times it reports to it (see docs/runner.md).
 return c.json({task,now:Date.now()})})
api.post('/api/runner/results',async c=>{const text=c.get('requestText');if(!await signedRunnerBody(c.env,c.get('runner'),text,c.req.header('x-sent-at')??'',c.req.header('x-runner-signature')??'',runnerResultLimit(text)))return c.json({error:'Invalid signed result'},403);return c.json(await storageOperation('runner.results',()=>complete(c.env,c.get('input'),c.get('runner').runner_id)))})
// Discovery: the owner's console asks, the owner's runner claims and answers; the Worker never fetches the target.
api.post('/api/discoveries',async c=>c.json(await createDiscovery(c.env,c.get('requestText')),201))
api.get('/api/discoveries/:id',async c=>c.json(await readDiscovery(c.env,c.req.param('id'))))
api.post('/api/runner/discoveries/claim',async c=>c.json(await claimDiscovery(c.env,c.get('requestText'),c.get('runner'))))
api.post('/api/runner/discoveries/:id/result',async c=>{const text=c.get('requestText');if(!await signedRunnerBody(c.env,c.get('runner'),text,c.req.header('x-sent-at')??'',c.req.header('x-runner-signature')??'',DISCOVERY_RESULT_BYTES))return c.json({error:'Invalid signed result'},403);return c.json(await completeDiscovery(c.env,c.req.param('id'),discoveryAnswer(text),c.get('runner')))})
api.get('/api/archive',async c=>c.json(await archive(c.env)))
api.post('/api/restore',async c=>{const input=c.get('input');return c.json(await restore(c.env,input.archive??input,undefined,{laterJournal:input.laterJournal,actor:{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id}}))})
// Definition files: preview writes nothing; apply takes the previewed digest; export is read-only; undo pauses or restores.
const operator=async(c:any)=>({kind:'operator' as const,ref:(await operatorSession(c.env,c.req.raw))!.operator.id})
api.post('/api/definitions/preview',async c=>c.json(await previewDefinitions(c.env,c.get('requestText'),c.req.query('source'))))
api.post('/api/definitions/apply',async c=>c.json(await applyDefinitions(c.env,c.get('requestText'),c.req.query('source'),c.req.query('digest'),await operator(c)),201))
api.get('/api/definitions/export',async c=>{c.header('content-disposition','attachment; filename="uptime.monitors.json"');return c.json(await exportDefinitions(c.env))})
api.get('/api/definitions/batches',async c=>c.json(await definitionImportHistory(c.env,c.req.raw)))
api.post('/api/definitions/batches/:id/undo',async c=>c.json(await undoDefinitionImport(c.env,c.req.raw,c.req.param('id'),await operator(c))))
api.get('/api/business-import-settings',async c=>c.json(await businessImportSettings(c.env,c.req.raw)))
api.post('/api/business-import-settings',async c=>c.json(await businessImportSettings(c.env,c.req.raw,c.get('input'))))
api.get('/api/erasure-journal',async c=>c.json(await exportJournal(c.env)))
api.post('/api/monitors/:id/erase',async c=>c.json(await erase(c.env,'monitor',c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id},c.get('input').name)))
api.post('/api/operators/:id/erase',async c=>c.json(await erase(c.env,'operator',c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.post('/api/notifications/webhook-preview',async c=>c.json(await proposedWebhookPreview(c.get('input'))))
api.post('/api/notifications/webhook-import-preview',async c=>c.json(previewUptimeRobotWebhook(c.get('input'))))
api.post('/api/notifications/destinations/:id/preview',async c=>c.json(await savedWebhookPreview(c.env,c.req.param('id'),c.get('input'))))
api.get('/api/notifications',async c=>c.json(await notificationOverview(c.env)))
api.get('/api/notifications/destinations',async c=>c.json(await destinations(c.env)))
api.post('/api/notifications/destinations',async c=>c.json(await createDestination(c.env,c.get('input')),201))
api.patch('/api/notifications/destinations/:id',async c=>c.json(await updateDestination(c.env,c.req.param('id'),c.get('input'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.delete('/api/notifications/destinations/:id',async c=>c.json(await erase(c.env,'destination',c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.post('/api/notifications/destinations/:id/rotate',async c=>c.json(await rotateDestination(c.env,c.req.param('id'),{kind:'operator',ref:(await operatorSession(c.env,c.req.raw))!.operator.id})))
api.post('/api/notifications/destinations/:id/test',async c=>c.json(await testDestination(c.env,c.req.param('id')),202))
api.post('/api/notifications/destinations/:id/drill',async c=>c.json(await startDrill(c.env,c.req.param('id'),(await operator(c)).ref),202))
api.post('/api/runner/notifications',async c=>c.json(await notificationRpc(c.env,c.get('input'))))
// A failed mutation guard or a racing writer's constraint means the installation changed between reading and writing: a conflict.
// Any other D1 failure is a retryable storage fault. The log line names the operation, class, status and a sanitized cause, never values.
api.onError((error,c)=>{
 if(error instanceof ReportInputError)return c.json({error:error.message,code:error.code},400)
 const status=(error as Error&{status?:number}).status,limited=status===429,fault=classifyStorage(error)
 if(limited)c.header('retry-after','60')
 if(fault){
  console.error(storageLogLine(c.req.method,storageRoute(c.req.path),fault))
  const operation={class:fault.class,cause:fault.cause,...(fault.operation?{operation:fault.operation}:{})}
  if(fault.code==='conflict')return c.json({error:'The installation changed during this request. Nothing was saved; try again.',code:'conflict',...operation},409)
  c.header('retry-after','5');return c.json({error:'Storage operation failed',code:'storage_unavailable',retryable:true,...operation},503)
 }
 return c.json({error:error.message},status===409?409:limited?429:status===404?404:400)
})
const storageRoute=(path:string)=>path.split('/').map(part=>part.length>24||/[0-9a-f]{8}-[0-9a-f]{4}/i.test(part)?':id':part).join('/')
// Lobby readiness follows /setup completion, separately from permanent Base owner admission.
const claimed=claimedWhen('SELECT 1 FROM uptime_first_run WHERE id = 1')
// Base records owner admission (by setup token or rer's owner claim) in oa_bootstrap before the owner row.
const ownerAdmitted=claimedWhen('SELECT 1 FROM oa_bootstrap WHERE singleton = 1')
// The public docs carry no installation data and need no session.

// One monitor's page in the console (/app/monitors/<id>); it is gated like /app.
const isMonitorPage=(path:string)=>/^\/app\/monitors\/[^/]+\/?$/.test(path)
// A monitor page whose id is not valid percent-encoding names no monitor; after the session check it gets the 404 page.
const undecodableMonitorId=(path:string)=>{try{decodeURIComponent(path.slice('/app/monitors/'.length).replace(/\/$/,''));return false}catch{return true}}
// The private console documents: the monitor list, the monitor editor and each monitor's page.
// Documents React hydrates carry no-transform, so no proxy (Cloudflare's email address obfuscation) rewrites them first.
const noTransform=(response:Response)=>{
 const headers=new Headers(response.headers),policy=headers.get('cache-control');
 if(!headers.get('content-type')?.startsWith('text/html')||policy?.includes('no-transform'))return response;
 headers.set('cache-control',policy?policy+', no-transform':'no-transform');
 return new Response(response.body,{status:response.status,statusText:response.statusText,headers})
}
const isConsoleDocument=(path:string)=>['/app','/app/','/app/monitor','/app/monitor/'].includes(path)||isMonitorPage(path)
const handler={async fetch(request,env,execution){
 env=await demoBindings(env as Bindings);
 const invalid=await configurationResponse(env as Bindings);if(invalid)return invalid;
 const limited=await limitPublicRequest(env as Bindings,request);if(limited)return limited;
 const bindings=env as Bindings,url=new URL(request.url),path=url.pathname;
 if(url.origin!==bindings.UPTIME_ORIGIN||(request.headers.has('host')&&request.headers.get('host')?.toLowerCase()!==url.host.toLowerCase()))return new Response('Invalid host',{status:403});
 if(path.startsWith('/api/rer/import/'))return businessImportDoor(bindings,request);
 // The Lobby at / and GET /setup/state: Base's shared responses, before the public-path guard and the document renderer.
 const lobbyBindings={DB:bindings.DB,AUTH_ORIGIN:bindings.UPTIME_ORIGIN!};
 const state=await setupStateResponse(request,lobbyBindings,claimed);if(state)return state;
 const lobby=await lobbyResponse(request,lobbyBindings,identity,claimed);if(lobby)return lobby;
 if(!publicPath(path,bindings.UPTIME_LOCAL_REVIEW==='true')&&!isConsoleDocument(path)&&!path.startsWith('/_serverFn/')&&path!=='/api'&&!path.startsWith('/api/'))return notFoundDocument();
 if(path==='/auth/owner-claim')return (await ownerClaim(bindings,request))!;
 if(path==='/api/editor/config')return editorConfigResponse(bindings,request);
 if(path.startsWith('/auth/'))return nativeClaimFailure(request,(await authRoute(bindings,request))!,bindings.UPTIME_DEPLOY_ENVIRONMENT);
 if(path==='/signin')return signInDocument(bindings,request);
 if(path==='/setup')return firstRun(bindings,request);
 if(path==='/alert-drill')return alertDrillDocument(bindings,request);
 // Once an owner is admitted the first-owner claim is spent, so sign-in stops offering it.
 if(path==='/login'||path==='/claim')return authDocument(path==='/claim',path==='/login'&&url.searchParams.get('notice')==='sent',developmentMail(bindings),false,bindings.UPTIME_DEPLOY_ENVIRONMENT,!await ownerAdmitted(bindings.DB),path==='/login'&&url.searchParams.get('notice')==='mail_unavailable',url.searchParams.get('login_hint')??'');
 if(isPublicAsset(path))return bindings.ASSETS?bindings.ASSETS.fetch(request):new Response('Not found',{status:404});
 if(path==='/monitor'||path==='/monitor/')return new Response(null,{status:308,headers:{location:`/app/monitor${url.search}`,'cache-control':'no-store'}});
 if(path!=='/health'&&path!=='/api'&&!path.startsWith('/api/')&&!isPublicDocument(path)){
  // Only native document routes and Start transports reach the private renderer.
  if(!isConsoleDocument(path)&&!path.startsWith('/_serverFn/'))return notFoundDocument();
  try{const principal=await authenticatedSession(bindings,request);if(!principal)return new Response(null,{status:303,headers:{location:'/login','cache-control':'no-store'}});if(principal.operator.role!=='owner')return accessDeniedDocument(principal.operator.displayName,request.method==='HEAD');if(!await firstRunComplete(bindings))return setupRedirect()}catch(error){return authFailure(error)}
  if(isMonitorPage(path)&&undecodableMonitorId(path))return notFoundDocument();
 }
 const response=await dispatch(request,()=>api.fetch(request,bindings,execution),()=>start.fetch(request));
 return noTransform(isConsoleDocument(path)?editorDocument(response,bindings):response)
},async scheduled(_controller,env){env=await demoBindings(env as Bindings);await validateWorkerConfiguration(env as Bindings);const bindings=env as Bindings;await recordCronRun(bindings);await maintenance(bindings);const checks=new SubrequestBudget(CHECK_SUBREQUESTS);await scheduledPublicChecks(bindings,checks);await scheduledNotifications(bindings,new SubrequestBudget(RUN_SUBREQUESTS-WATCH_SUBREQUESTS-(CHECK_SUBREQUESTS-checks.remaining)));await monitoringWatch(bindings,()=>boundedHealth(()=>health(bindings)),async()=>{const data=await overview(bindings);return data.monitors.filter(m=>m.definition.enabled).every(m=>m.state.targetState==='Healthy')})}} satisfies ExportedHandler
export default withDemo({...handler,fetch(request,env,execution){
 return new URL(request.url).pathname==='/health'?boundedHealth(()=>handler.fetch(request,env,execution)):handler.fetch(request,env,execution)
}} satisfies ExportedHandler,{seed:seedDemo})
