(function(root){'use strict';
 const VERSION='DAILY_RATE_CORRECTION_V1',UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
 const copy=value=>JSON.parse(JSON.stringify(value));
 function node(tag,text,className){const value=root.document.createElement(tag);if(text!==undefined)value.textContent=text;if(className)value.className=className;return value;}
 function required(adapter){for(const name of ['preview','accept','get','runAction'])if(typeof adapter?.[name]!=='function')throw new TypeError('DAILY_RATE_NATIVE_ADAPTER_REQUIRED');if(!UUID.test(adapter.actorUserId||''))throw new TypeError('DAILY_RATE_ACTOR_REQUIRED');}
 function createController({timesheetId,adapter,onChange=()=>{}}){
  required(adapter);const dto=root.CloudTMSCorrectionContractV3;if(!dto||!UUID.test(timesheetId||''))throw new TypeError('DAILY_RATE_CONTEXT_REQUIRED');
  const storage=adapter.pendingStorage||root.sessionStorage,key='cloudtms.daily-rate-correction.v1:'+adapter.actorUserId+':'+timesheetId;
  const state={phase:'DETAILS',note:'',preview:null,result:null,pending:null,error:null,busy:false};let previewRequest=null;
  const emit=()=>{onChange(state);};
  function save(command){dto.validateAcceptRequest(command,{daily:true});storage.setItem(key,JSON.stringify(command));}
  function clear(){storage.removeItem(key);state.pending=null;}
  try{const saved=storage.getItem(key);if(saved){const command=JSON.parse(saved);dto.validateAcceptRequest(command,{daily:true});state.pending=command;state.phase='RECOVERY';}}
  catch{state.phase='RECOVERY';state.error='Saved request recovery is unavailable. Reopen this page in the original tab.';}
  function bind(value,operationId){dto.validateResult(value);if(value.version!==VERSION||value.purpose!=='DAILY_RATE_CORRECTION'||value.operation_id!==operationId
   ||value.placement.original_timesheet_id!==timesheetId||value.placement.current_timesheet_id!==timesheetId)throw new TypeError('DAILY_RATE_RESULT_BINDING_INVALID');return value;}
  function failure(error,accepting=false){
   const code=error?.error_code||error?.code;
   if(code==='REVIEW_STALE'){if(accepting)clear();state.preview=null;previewRequest=null;state.phase='DETAILS';state.error='The Timesheet or its rates changed. Preview the current values again.';}
   else if(accepting){state.phase='RECOVERY';state.error='The result is not confirmed. Check the saved result or retry the same request.';}
   else state.error='The current result is unavailable. Please try again.';
  }
  async function work(body){if(state.busy)return;state.busy=true;state.error=null;emit();try{await body();}catch(error){failure(error,state.pending!==null);}finally{state.busy=false;emit();}}
  return Object.freeze({state,
   setNote(note){if(state.busy||state.pending||state.phase==='RESULT')return;state.note=note;state.preview=null;previewRequest=null;state.phase='DETAILS';state.error=null;
    try{dto.normalizeReason(note,'DAILY_RATE_CORRECTION');}catch(error){state.error='Details must be 1969 characters or fewer.';emit();throw error;}emit();},
   preview(){return work(async()=>{
    if(state.pending||state.phase==='RESULT'||state.phase==='RECOVERY')return;
    if(!previewRequest||state.preview)previewRequest={version:VERSION,request_id:root.crypto.randomUUID(),timesheet_id:timesheetId,reason:state.note};
    state.preview=null;state.phase='DETAILS';emit();
    dto.validatePreviewRequest(previewRequest,{daily:true});const value=await adapter.preview(copy(previewRequest));dto.validatePreview(value,{daily:true});if(value.current_timesheet_id!==timesheetId)throw new TypeError('DAILY_RATE_PREVIEW_BINDING_INVALID');
    state.preview=value;state.phase='PREVIEW';
   });},
   accept(){return work(async()=>{
    if(state.phase==='RESULT'||state.phase==='RECOVERY'&&!state.pending)return;
    if(!state.pending){const review=state.preview;if(!review?.ready_to_confirm||!review.confirm)return;
     const command={version:VERSION,request_id:root.crypto.randomUUID(),operation_id:review.operation_id,review_id:review.confirm.review_id,
      review_digest:review.confirm.review_digest,expected_state_revision:review.confirm.expected_state_revision};
     save(command);state.pending=command;
    }
    state.phase='ACCEPTING';emit();const command=copy(state.pending),value=bind(await adapter.accept(command),command.operation_id);
    state.result=value;if(['COMPLETE','SUPERSEDED'].includes(value.state)){clear();state.preview=null;state.phase='RESULT';}else state.phase='RECOVERY';
   });},
   get(){return work(async()=>{
    if(!state.pending)return;const value=bind(await adapter.get(state.pending.operation_id),state.pending.operation_id);state.result=value;
    if(['COMPLETE','SUPERSEDED'].includes(value.state)){clear();state.preview=null;state.phase='RESULT';}else state.phase='RECOVERY';
   });},
   runAction(action){dto.validateAction(action);return adapter.runAction(copy(action));}
  });
 }
 function open({timesheetId,adapter}){
  const previous=root.document.activeElement,dialog=node('dialog',undefined,'daily-rate-correction'),heading=node('h2','Apply updated rates');
  heading.id='daily-rate-correction-'+root.crypto.randomUUID();dialog.setAttribute('aria-labelledby',heading.id);
  const header=node('header'),close=node('button','Close');close.type='button';header.append(heading,close);
  const body=node('div',undefined,'daily-rate-correction__body'),description=node('p','Review the current Client charge rates and applicable pay rates for this Daily Timesheet.'),details=node('label','Details (optional)'),note=node('textarea');
  note.rows=3;note.maxLength=3938;details.append(note);
  const content=node('div'),error=node('p',undefined,'daily-rate-correction__error');error.setAttribute('role','alert');
  body.append(description,details,content,error);
  const footer=node('footer'),previewButton=node('button','Preview'),acceptButton=node('button','Apply updated rates','daily-rate-correction__apply'),getButton=node('button','Check saved result');
  for(const button of [previewButton,acceptButton,getButton]){button.type='button';footer.append(button);}dialog.append(header,body,footer);
  const payNames={NOT_REQUIRED:'No pay publication required',REGISTERED:'Pay revision registered',PROCESSING:'Pay revision processing',APPLIED:'Pay revision applied',ACTION_REQUIRED:'Pay result needs attention'};
  const billNames={NOT_REQUIRED:'No invoice correction required',PREPARING_DRAFTS:'Invoice drafts being prepared',DRAFTS_READY:'Invoice drafts ready',PARTIALLY_ISSUED:'Invoice correction partly issued',ISSUED:'Invoice correction issued',RETRY_WAIT:'Invoice preparation waiting to retry',ACTION_REQUIRED:'Invoice result needs attention'};
  const appendEffect=(label,value)=>{content.append(node('h3',label));if(value?.visibility==='FULL')content.append(node('p',value.text));else content.append(node('p','Restricted for your current role.'));};
  let controller;
  const render=state=>{
   content.replaceChildren();error.textContent=state.error||'';dialog.setAttribute('aria-busy',String(state.busy));
   note.disabled=state.busy||state.pending!==null||['RESULT','RECOVERY'].includes(state.phase);
   previewButton.hidden=['ACCEPTING','RECOVERY','RESULT'].includes(state.phase);previewButton.disabled=state.busy||[...state.note].length>1969;previewButton.textContent=state.preview?'Refresh preview':'Preview';
   acceptButton.hidden=state.phase==='RESULT'||!(state.preview?.ready_to_confirm||state.pending);acceptButton.disabled=state.busy;acceptButton.textContent=state.pending?'Retry same request':'Apply updated rates';
   getButton.hidden=!state.pending;getButton.disabled=state.busy;
   const value=state.result||state.preview;
   if(state.result){content.append(node('h3',state.result.state==='COMPLETE'?'Updated rates applied':'Saved correction result'));
    content.append(node('p',payNames[state.result.financial.status]),node('p',billNames[state.result.billing.status]));
    if(state.result.financial.summary)appendEffect('Pay',state.result.financial.summary);if(state.result.billing.visible_summary)appendEffect('Invoices',state.result.billing.visible_summary);
   }else if(state.preview){content.append(node('p',state.preview.summary.worked_facts_preserved?'The Daily Timesheet, worked facts and signatures are retained.':'Worked facts require review before applying rates.'));appendEffect('Pay',state.preview.summary.pay_effect);appendEffect('Invoices',state.preview.summary.invoice_effect);
    for(const blocker of state.preview.known_blockers)content.append(node('p',blocker.code.replaceAll('_',' ')));
   }else if(state.phase==='RECOVERY')content.append(node('p','Use the original saved request to check or retry this correction.'));
   const actions=[...(state.result?.next_actions||[]),...(value?.links||[]),...(state.preview?.known_blockers||[]).flatMap(b=>b.permitted_action?[b.permitted_action]:[])];
   for(const action of actions){const button=node('button',action.caption);button.type='button';button.disabled=!action.enabled||state.busy;button.title=action.disabled_reason||'';button.addEventListener('click',()=>{Promise.resolve(controller.runAction(action)).catch(()=>{error.textContent='The action is currently unavailable.';});});content.append(button);}
  };
  controller=createController({timesheetId,adapter,onChange:render});render(controller.state);
  note.addEventListener('input',()=>{try{controller.setNote(note.value);}catch{error.textContent='Details must be 1969 characters or fewer.';}});
  previewButton.addEventListener('click',()=>{void controller.preview();});acceptButton.addEventListener('click',()=>{void controller.accept();});getButton.addEventListener('click',()=>{void controller.get();});
  close.addEventListener('click',()=>dialog.close());dialog.addEventListener('close',()=>{dialog.remove();previous?.focus();});
  root.document.body.append(dialog);dialog.showModal();note.focus();return Object.freeze({element:dialog,controller,close:()=>dialog.close()});
 }
 root.CloudTMSDailyRateCorrectionV1=Object.freeze({open,createController});
})(typeof window!=='undefined'?window:globalThis);
