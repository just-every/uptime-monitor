// A monitor's page (/app/monitors/<id>) starts the reads it renders while the document is still parsing, before React
// hydrates and public/app.js loads: its own overview (/api/overview?monitor=<id>), recent checks, revisions and incidents,
// all at once. app.js takes each answer once through takePrefetched (public/app.js); later polls read fresh.
// Each read is the owner API itself with the page's own session; nothing is cached or stored.
export const consolePrefetchScript=`(function(){
var m=location.pathname.match(/^\\/app\\/monitors\\/([^/]+)\\/?$/);if(!m||!window.fetch)return;
var id=m[1],paths=['/api/overview?monitor='+id,'/api/monitors/'+id+'/history','/api/monitors/'+id+'/revisions','/api/monitors/'+id+'/incidents'],early={};
paths.forEach(function(path){early[path]={at:Date.now(),response:fetch(path,{headers:{'content-type':'application/json'},credentials:'same-origin',cache:'no-store'})}});
window.uptimePrefetch=early;
})()`
// public/app.js and the modules it imports, so the browser fetches the whole graph while React hydrates instead of in
// three waves after it (they are revalidated on each load). Keep it to app.js's static imports, all levels; a module left
// out only loads later, a module that is no longer imported is fetched for nothing.
export const CONSOLE_MODULES=['app.js','runner-settings.js','business-import-settings.js','notification-dialogs.js','notifications.js','action-feedback.mjs','console-format.js',
 'monitor-coverage.js','monitor-page.js','definitions.js','alert-drills.js','monitor-incidents.js','heartbeat-key.js','monitor-timeline.js','flow-panel.js','protocol-panel.js']
