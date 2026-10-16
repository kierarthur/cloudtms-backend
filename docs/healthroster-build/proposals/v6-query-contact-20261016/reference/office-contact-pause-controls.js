(function(root){
 'use strict';
 const uuid=value=>typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value);
 const copy=value=>JSON.parse(JSON.stringify(value));
 const escape=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
 const local=value=>String(value||'').replace(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}:\d{2}) Europe\/London$/,'$3/$2/$1 $4 (UK)');
 const messages={CONTACT_REVISION_CONFLICT:'Contact settings changed. Refresh before trying again.',
  CONTACT_IDEMPOTENCY_CONFLICT:'This request identity cannot be reused. Refresh before trying again.',
  CONTACT_REQUEST_INVALID:'Check the selected audience and note.',
  WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED:'Current Office administrator permission is required.'};

 function create({candidateId,clientId,candidate,client,operation='PAUSE',command,isCurrent}){
  if(!uuid(candidateId)||!uuid(clientId)||!['PAUSE','RESUME'].includes(operation)||typeof command!=='function'||typeof isCurrent!=='function')throw new TypeError('CONTACT_CONTROLS_CONTEXT_INVALID');
  const decoder=root.CloudTMSContactPauseContractV1;
  if(typeof decoder?.decode!=='function')throw new Error('Contact controls are unavailable. Refresh the page.');
  const pair=Object.freeze({candidate_id:candidateId,client_id:clientId});
  const state={scope:null,acknowledgement:null,audience:'BOTH',note:'',busy:false,pending:null,stale:true,error:'',message:''};
  let notify=()=>{};
  const current=()=>{if(!isCurrent())throw new Error('Return to the original worker and client before continuing.');};
  const changed=()=>notify();
  const snapshot=()=>copy(state);
  async function load(){
   current();if(state.busy)throw new Error('Contact controls are being checked.');
   if(state.pending)throw new Error('Retry the original save before refreshing.');
   state.busy=true;state.error='';changed();
   try{
    const reply=await command('CONTACT_PAUSE_GET',pair);current();
    state.scope=copy(decoder.decode(reply,'CONTACT_PAUSE_GET',pair).scope);state.stale=false;
   }catch(error){state.stale=true;state.error=messages[error?.code]||'Contact settings cannot be confirmed. Refresh settings.';throw error;}
   finally{state.busy=false;changed();}
   return snapshot();
  }
  function edit({audience=state.audience,note=state.note}={}){
   current();if(state.busy||state.pending)throw new Error('Retry the original save before changing it.');
   if(!['WORKER','MANAGER','BOTH'].includes(audience)||typeof note!=='string'||[...note].length>500)throw new Error('Check the selected audience and note.');
   state.audience=audience;state.note=note;changed();return snapshot();
  }
  async function submit(retry=false){
   current();if(state.busy)throw new Error('The original save is still being checked.');
   if(!state.pending){
    if(retry)throw new Error('There is no original save to retry.');
    if(!state.scope||state.stale||!(operation==='PAUSE'?state.scope.can_pause:state.scope.can_resume))throw new Error('Refresh contact settings before saving.');
    if(typeof root.crypto?.randomUUID!=='function')throw new Error('A request identity could not be created. Refresh the page.');
    state.pending={action:operation==='PAUSE'?'CONTACT_PAUSE':'CONTACT_RESUME',payload:{...pair,request_id:root.crypto.randomUUID(),
     expected_revision:state.scope.revision,audience:state.audience,note:state.note}};
   }else if(!retry)throw new Error('Retry the original save before changing it.');
   const pending=copy(state.pending);
   state.busy=true;state.error='';state.message='';changed();
   try{
    const reply=await command(pending.action,pending.payload);current();
    const accepted=copy(decoder.decode(reply,pending.action,pending.payload));
    state.acknowledgement=accepted;state.scope=accepted.scope;state.pending=null;
    // This is an immutable acknowledgement, possibly replayed after newer
    // commands. It is not a claim about the current scope until a fresh GET.
    state.stale=true;
    state.message=operation==='PAUSE'?'The pause was saved. Refresh to check the current contact state.':
     'Your resume request was saved. We will check which messages are still needed. Refresh to check the current contact state.';
    return snapshot();
   }catch(error){
    if([400,409].includes(error?.status)&&Object.hasOwn(messages,error?.code)){
     state.pending=null;state.stale=true;state.error=messages[error.code];
    }else state.error=messages[error?.code]||'The save result is being checked. Retry the original save.';
    throw error;
   }finally{state.busy=false;changed();}
  }
  function html(){
   const worker=escape(candidate||'This worker'),place=escape(client||'this client');
   const s=state.scope,disabled=state.busy||!!state.pending;
   const expiry=s?local(s.next_pause_until_local):'';
   const expiryParts=expiry.match(/^(\d{2}\/\d{2}\/\d{4}) (\d{2}:\d{2}) \(UK\)$/);
   const explanation=operation==='PAUSE'?`This pauses questions, reminders and related messages for ${worker} at ${place}. ${worker} can still open MyTMS and reply.${expiryParts?` Messages resume Monday ${escape(expiryParts[1])} at ${escape(expiryParts[2])} (UK), or you can resume them earlier.`:''}`:
    'We will check which messages are still needed. Old reminders will not all be sent at once.';
   const bits=s?`${state.stale&&state.acknowledgement?'<h4>Saved acknowledgement</h4>':''}${['worker','manager'].map(key=>`<p><strong>${key==='worker'?worker:`Managers about ${worker}`}</strong>: ${s[key].paused?`Contact paused until ${escape(local(s[key].paused_until_local))}`:state.stale?'No pause recorded in this acknowledgement':'No current contact pause'}${s[key].reason?`<small>${escape(s[key].reason)}</small>`:''}</p>`).join('')}`:'';
   const inflight=s?.in_flight.length?`<aside role="status"><strong>One message had already started; its result is being checked.</strong><ul>${s.in_flight.map(item=>`<li>${item.channel==='EMAIL'?'Email':'Worker push'} · ${item.status==='AMBIGUOUS'?'Checking send result':'Started'} · ${escape(new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/London',dateStyle:'short',timeStyle:'medium',hour12:false}).format(new Date(item.started_at_utc)))} (UK)</li>`).join('')}</ul></aside>`:'';
   return `<section class="ws-contact-controls" aria-label="${operation==='PAUSE'?'Pause':'Resume'} messages for ${worker} at ${place}"><h3>${operation==='PAUSE'?'Pause':'Resume'} messages for ${worker} at ${place}</h3><p>${explanation}</p>${state.stale?'<p role="status">Refresh to confirm the current contact state.</p>':''}${bits}${inflight}
    ${state.error?`<p role="alert">${escape(state.error)}</p>`:''}${state.message?`<p role="status">${escape(state.message)}</p>`:''}
    <fieldset ${disabled?'disabled':''}><legend>Messages</legend>${[['WORKER',worker],['MANAGER',`Managers about ${worker}`],['BOTH','Both']].map(([value,label])=>`<label><input type="radio" name="contact_audience" value="${value}"${state.audience===value?' checked':''}>${label}</label>`).join('')}<label>Note (optional)<textarea name="contact_note">${escape(state.note)}</textarea></label></fieldset>
    <div class="ws-toolbar"><button type="button" class="btn btn-outline" data-contact-refresh${state.busy||state.pending?' disabled':''}>Refresh contact settings</button>${state.pending?`<button type="button" class="btn" data-contact-retry${state.busy?' disabled':''}>${state.busy?'Checking save…':'Retry original save'}</button>`:`<button type="button" class="btn" data-contact-save${state.busy||state.stale||!s||!(operation==='PAUSE'?s.can_pause:s.can_resume)?' disabled':''}>${operation==='PAUSE'?'Pause messages':'Resume messages now'}</button>`}</div></section>`;
  }
  function mount(host){
   if(!host?.querySelector)throw new TypeError('CONTACT_CONTROLS_HOST_REQUIRED');
   const render=()=>{
    host.innerHTML=html();
    host.querySelectorAll('[name="contact_audience"]').forEach(input=>input.addEventListener('change',()=>{try{edit({audience:input.value});}catch(error){state.error=error.message;render();}}));
    host.querySelector('[name="contact_note"]')?.addEventListener('input',event=>{
     // Preserve focus/caret during note entry; do not rerender on each key.
     if(disabledNow())return;state.note=event.target.value;
    });
    host.querySelector('[data-contact-refresh]')?.addEventListener('click',()=>{void load().catch(()=>{});});
    host.querySelector('[data-contact-save]')?.addEventListener('click',()=>{
     try{edit({note:state.note});void submit().catch(()=>{});}catch(error){state.error=error.message;render();}
    });
    host.querySelector('[data-contact-retry]')?.addEventListener('click',()=>{void submit(true).catch(()=>{});});
   };
   const disabledNow=()=>state.busy||!!state.pending;
   notify=render;render();return()=>{notify=()=>{};host.replaceChildren();};
  }
  return Object.freeze({load,edit,save:()=>submit(false),retry:()=>submit(true),snapshot,html,mount});
 }
 const api=Object.freeze({create});
 if(typeof module==='object'&&module.exports)module.exports=api;
 root.CloudTMSContactPauseControlsV1=api;
})(typeof window==='object'?window:globalThis);
