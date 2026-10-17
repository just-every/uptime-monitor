import {dialogFocusReturn} from './notification-dialogs.js';
import {businessImportSettings} from './business-import-settings.js';
export function runnerSettings(container,runners,api,refresh){
 businessImportSettings(container,api);
 const heading=document.createElement('h3');heading.textContent='Runner networks';container.append(heading);
 const list=document.createElement('ul');
 for(const runner of runners){const row=document.createElement('li');row.textContent=`${runner.networkLabel} · ${runner.runnerId} · ${runner.revokedAt?'Revoked':runner.lastContactAt?'Contact '+new Date(runner.lastContactAt).toLocaleString():'Never contacted'} · ${runner.capabilities?.browser?'Has a browser for flows':'No browser for flows'}`;
  if(!runner.revokedAt&&!runner.hosted){const button=document.createElement('button');button.textContent='Revoke runner';button.onclick=async()=>{if(!confirm('Revoke this runner key? Its future requests will be refused.'))return;button.disabled=true;try{await api('/api/runners/'+encodeURIComponent(runner.runnerId),'DELETE');await refresh()}catch(error){status.textContent=error.message;button.disabled=false}};row.append(button)}list.append(row)
 }
 container.append(list);
 const form=document.createElement('form'),label=document.createElement('label'),input=document.createElement('input'),button=document.createElement('button'),status=document.createElement('p');
 form.id='runner-network-form';input.id='runner-network-label';label.textContent='Independent network label';input.required=true;input.maxLength=80;label.append(input);button.id='add-runner';button.type='submit';button.textContent='Add runner';status.setAttribute('role','status');form.append(label,button,status);container.append(form);
 form.onsubmit=async event=>{event.preventDefault();const returnFocus=dialogFocusReturn();button.disabled=true;try{const result=await api('/api/runners','POST',{networkLabel:input.value});showRunnerCommand(result,refresh,returnFocus)}catch(error){status.textContent=error.message}finally{button.disabled=false}};
}
function showRunnerCommand(runner,refresh,returnFocus){
 const quote=value=>"'"+String(value).replaceAll("'", "'\"'\"'")+"'";
 const settings={UPTIME_CONTROL_ORIGIN:window.location.origin,UPTIME_RUNNER_ID:runner.runnerId,UPTIME_RUNNER_KEY:runner.key,UPTIME_NETWORK_LABEL:runner.networkLabel};
 if(window.location.protocol==='http:'&&window.location.hostname==='127.0.0.1')settings.UPTIME_LOCAL_REVIEW='true';
 const command=Object.entries(settings).map(([name,value])=>`export ${name}=${quote(value)}`).join('\n')+'\nnode runners/src/private-runner.mjs';
 const dialog=document.createElement('dialog'),title=document.createElement('h3'),help=document.createElement('p'),value=document.createElement('pre'),close=document.createElement('button');
 dialog.id='runner-key-dialog';title.id='runner-key-heading';title.textContent='Start this private runner';dialog.setAttribute('aria-labelledby',title.id);
 help.textContent='From an installed Uptime checkout on this network, paste the complete command into your shell. Keep it private: the key is shown once. Keep the runner running under a supervisor.';
 value.textContent=command;close.textContent='Saved — close';close.onclick=()=>dialog.close();dialog.append(title,help,value,close);document.body.append(dialog);
 dialog.addEventListener('close',async()=>{value.textContent='';dialog.remove();await refresh();returnFocus()},{once:true});dialog.showModal();
}
