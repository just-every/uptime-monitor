// renderRunners replaces its container on every poll. Reattach this row intact so a draft, an open disclosure or a
// pending request survives that redraw. A different console API belongs to a new session and gets a fresh row.
let mounted=null;
window.addEventListener('uptime:auth-lost',()=>{mounted=null});
window.addEventListener('uptime:console-unmount',()=>{mounted=null});

export function businessImportSettings(container,api){
 if(!mounted||mounted.api!==api)mounted={api,row:createSettings(api)};
 container.append(mounted.row);
}

function createSettings(api){
 const row=document.createElement('section'),heading=document.createElement('h3'),form=document.createElement('form'),label=document.createElement('label'),enabled=document.createElement('input'),details=document.createElement('details'),summary=document.createElement('summary'),help=document.createElement('p'),keys=document.createElement('div'),actions=document.createElement('p'),save=document.createElement('button'),reload=document.createElement('button'),status=document.createElement('p');
 row.id='business-import-settings';heading.id='business-import-heading';heading.textContent='Business imports';row.setAttribute('aria-labelledby',heading.id);
 form.id='business-import-form';enabled.type='checkbox';enabled.id='business-import-enabled';enabled.name='enabled';label.append(enabled,'Allow owner-approved imports from Business');
 summary.id='business-import-keys';summary.textContent='Grant signing keys';help.className='ui-helper';help.textContent='Revoke a signing key to refuse Business grants signed by that key. Revocation cannot be undone here.';
 details.append(summary,help,keys);save.type='submit';save.id='business-import-save';save.textContent='Save';reload.type='button';reload.id='business-import-reload';reload.textContent='Reload saved settings';actions.append(save,reload);
 status.id='business-import-status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');enabled.setAttribute('aria-describedby',status.id);
 form.append(label,details,actions,status);row.append(heading,form);
 let saved=null,busy=false,checks=[];
 const show=(text,error=false)=>{status.textContent=text;status.className=error?'ui-callout error':'ui-helper'};
 const controls=()=>{enabled.disabled=busy||!saved;save.disabled=busy||!saved;reload.disabled=busy;for(const {input,id}of checks)input.disabled=busy||!saved||saved.revokedKeyIds.includes(id);form.setAttribute('aria-busy',String(busy))};
 const describe=()=>`${saved.enabled?'On. Owner-approved Business imports are allowed.':'Off. Business imports are refused.'}${saved.keys.length?'':' No trusted signing keys are configured.'}${saved.revokedKeyIds.length?` ${saved.revokedKeyIds.length} signing key(s) revoked.`:''}`;
 const changed=()=>show('Unsaved changes. Choose Save to apply them.');
 enabled.onchange=changed;
 async function read(){
  const result=await api('/api/business-import-settings');
  saved=result;enabled.checked=result.enabled;checks=[];keys.replaceChildren();
  for(const [index,id]of [...new Set([...result.keys,...result.revokedKeyIds])].entries()){
   const keyLabel=document.createElement('label'),input=document.createElement('input'),text=document.createElement('span'),revoked=result.revokedKeyIds.includes(id);
   input.type='checkbox';input.id='business-import-key-'+index;input.name='revokedKeyIds';input.value=id;input.checked=revoked;input.onchange=changed;text.className='definition-digest';text.textContent=`${revoked?'Revoked':'Revoke'} key ${id}`;
   keyLabel.append(input,text);keys.append(keyLabel);checks.push({input,id});
  }
  if(!checks.length){const empty=document.createElement('p');empty.textContent='No signing key IDs are configured.';keys.append(empty)}
 }
 async function load(){
  if(busy)return;busy=true;controls();show('Loading saved Business import settings…');
  try{await read();show(describe())}
  catch(error){show(`Could not load Business import settings: ${error.message}`,true)}
  finally{busy=false;controls()}
 }
 form.onsubmit=async event=>{
  event.preventDefault();if(busy||!saved)return;
  const body={enabled:enabled.checked,revokedKeyIds:checks.filter(({input})=>input.checked).map(({id})=>id)};
  busy=true;controls();show('Saving Business import settings…');let written=false;
  try{
   await api('/api/business-import-settings','POST',body);written=true;saved=null;
   await read();show('Saved. '+describe());
  }catch(error){saved=null;show(written?`Save succeeded, but saved settings could not be read: ${error.message}. Reload saved settings to check.`:`Could not confirm the save: ${error.message}. Reload saved settings to check before saving again.`,true)}
  finally{busy=false;controls()}
 };
 reload.onclick=load;load();
 return row;
}
