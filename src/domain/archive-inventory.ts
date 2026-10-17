import {businessImportLocalTables} from './business-import/inventory.server.ts'
import {TABLES,columns} from './archive-schema.ts'
/** Every permanent schema table has a portability decision. Unknown tables and
 * foreign integration namespaces refuse export rather than silently lose data. */
export const installationLocalTables:Record<string,string>={
 ...businessImportLocalTables,
 notification_preferences:'Installation alert time zone; immutable event contexts preserve their own saved zone in archives.',
 alert_routes:'Installation alert recipient policy; a receiving owner configures local routes explicitly.',
 alert_route_changes:'Installation detailed route history; opaque routing audit entries travel separately.',
 alert_recovery_matches:'Installation dispatch pairing between a recovery and its original trigger job; never replay restored alerts.',
 public_rate_limits:'Installation public-path request counters; expire after their window.',
 _cf_METADATA:'D1 installation metadata; never product records.',
 _rer_deploy_migrations:'Historical deployment migration ledger; installation-local permanent history, never portable product records.',
 d1_migrations:'Receiving installation migration ledger; never replace applied migration history.',
 sessions:'Retired installation authentication sessions.',login_attempts:'Authentication rate-limit authority.',
 oa_operators:'Installation operator identities, roles and authorization.',oa_bootstrap:'Installation owner-claim authority.',
 oa_owner_signins:'Installation one-use hosted owner sign-in authority; never portable.',
 oa_tokens:'Magic-link and invitation authentication authority.',oa_sessions:'Installation session authority.',
 oa_dev_credentials:'Installation record of credentials minted in a local dev session; never portable.',oa_external_identities:'Installation external authentication identities.',oa_limits:'Authentication rate limits.',oa_invitation_cancellations:'Invitation authorization and revocation ledger.',
 oa_domain_rules:'Installation sign-in provider policy; never portable authorization.',
 oa_step_up_challenges:'Installation session-bound step-up token authority; never portable.',
 runner_contact:'Installation runner contact, not product target evidence.',
 attempt_checkpoints:'Rebuildable installation-local scheduled evidence checkpoints.',
 runners:'Installation runner identities and encrypted per-runner keys.',runner_assignments:'Installation execution assignments; revisions retain their selected configuration.',evidence_generation:'Installation atomic evidence projection generations.',monitoring_watch:'Installation watch dispatch state; never portable authority.',
 cron_runs:'Installation record of the latest Cron invocation; a receiving installation records its own.',
 heartbeat_authority:'Installation heartbeat token and receipt authority.',heartbeat_claims:'Installation heartbeat execution snapshots.',heartbeat_limits:'Installation ingress rate limits.',
 notification_credentials:'Installation destination ciphertext, webhook/Slack URLs and signing keys; email definition is separately exported.',
 notification_claim_custody:'Installation dispatch reservation hashes and claim authorization; product attempts and outcomes are retained separately.',
 integration_tokens:'Installation integration token authority.',integration_token_usage:'Installation token authorization audit.',integration_token_events:'Installation token administration audit.',
 rer_mail_development_outbox:'Retired pre-v2 development-capture table; excluded from product archives. Current local sign-in uses automatic owner entry, with no captured-mail inbox.',
 oc_owner_claims:'Installation signed owner admission ledger; retain permanently, never export credentials or authority.',
 uptime_first_run:'Installation first-run completion; a receiving owner completes setup independently.',
 alert_drills:'Installation alert drill history and confirmation capability hashes; a restored destination starts never proven.',
 external_check:'Installation External check URL: the receiving installation chooses its own network\'s default, and monitors keep the URL saved in their revisions, which do travel.',
 discovery_jobs:'Installation discovery questions and the runner\'s derived answers; transient, never evidence or definitions of record.',
 oa_identity_providers:'Installation linked-provider pin and endpoints; a restored installation must link again.',oa_identity_provider_health:'Installation provider reachability.',oa_oidc_transactions:'Installation in-flight provider sign-in state; expires.',oa_oidc_limits:'Installation provider sign-in rate limits.',
 oa_auth_settings:'Installation provider sign-in setting; a restored installation starts with it off.',oa_auth_audit:'Installation record of owner changes to provider sign-in and links.',
 oa_provider_sessions:'Installation provider session provenance; never portable authority.',oa_provider_session_pins:'Installation provider pin of each provider session; never portable authority.',oa_logout_events:'Installation one-use back-channel logout tokens.',oa_credentials:'Installation minted credential authority; Uptime mints none.',oa_setup_links:'Installation first-setup provider approvals; expire.',
 rer_dependency_codes:'Installation one-use link handshake digests.',rer_dependency_installation:'Installation sector identity for the optional Auth link.',rer_dependency_keys:'Installation private link key; never exported.',rer_dependency_link:'Installation provider link record; a restored installation must link again.',rer_dependency_pending:'Installation pending link approvals; expire.',
 oc_idp_grants:'Installation one-use provider owner-claim grant ledger.',
};
/** SQLite's own sqlite_% tables and Cloudflare's _cf_% tables (real D1 adds _cf_KV and others) belong to the platform: never exported, never restored into, never refused. GLOB keeps the underscore literal. */
export async function assertArchiveInventory(db:D1Database){
 const rows=await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT GLOB '_cf_*' ORDER BY name").all<{name:string}>();
 for(const {name} of rows.results)if(!name.startsWith('rer_portability_')&&!TABLES.includes(name)&&!Object.hasOwn(installationLocalTables,name))throw Error(`Archive unclassified table: ${name}`);
 const sourceColumns:Record<string,string[]>={...columns,notification_destinations:['id','name','kind','enabled','credential_id','created_at','deleted_at','webhook_template'],notification_jobs:[...columns.notification_jobs,'credential_id','claim','claim_until'],integration_deliveries:[...columns.integration_deliveries,'fingerprint','destination_hash','claim','claim_until'],integration_replays:[...columns.integration_replays,'fingerprint']};
 const schemas=await db.batch(TABLES.map(name=>db.prepare(`PRAGMA table_info(${name})`)));
 for(let i=0;i<TABLES.length;i++){const name=TABLES[i],actual=schemas[i].results.map((r:any)=>r.name).sort().join(',');if(actual!==sourceColumns[name].slice().sort().join(','))throw Error(`Archive unsupported table columns: ${name}`)}
 for(const name of TABLES.filter(n=>n.startsWith('integration_'))){const row=await db.prepare(`SELECT COUNT(*) n FROM ${name} WHERE namespace <> 'uptime-notifications'`).first<{n:number}>();if(row?.n)throw Error(`Archive unsupported product namespace: ${name}`)}
}
