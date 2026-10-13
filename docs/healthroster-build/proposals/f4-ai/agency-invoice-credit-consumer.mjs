const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA=/^[0-9a-f]{64}$/;
const CID=/^awc:([0-9a-f-]{36}):([0-9a-f-]{36})$/;
const CONTRACT='TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1';
const keys=['request_id','root_timesheet_id','correction_id','state_revision','source_revision','original_invoice_id','reversal_timesheet_id',
  'reversed_timesheet_id','replacement_timesheet_id','client_id','candidate_id','week_ending_date','candidate_name',
  'invoice_number','blocker_code','selection_key','selectable','automatic'];
function fail(code) {throw Object.assign(new Error(code),{code,status:502});}
function object(v) {return !!v&&typeof v==='object'&&!Array.isArray(v);}
function row(v) {
  const cid=typeof v?.correction_id==='string'?v.correction_id.match(CID):null;
  if (!object(v)||Object.keys(v).length!==keys.length||Object.keys(v).some(k=>!keys.includes(k))
    || ['request_id','root_timesheet_id','original_invoice_id','reversal_timesheet_id','reversed_timesheet_id','replacement_timesheet_id','client_id','candidate_id'].some(k=>typeof v[k]!=='string'||!UUID.test(v[k]))
    ||!cid||cid[1]!==v.root_timesheet_id||cid[2]!==v.request_id||typeof v.state_revision!=='string'||!(/^[1-9][0-9]{0,18}$/).test(v.state_revision)
    || (v.source_revision!==null&&(typeof v.source_revision!=='string'||!SHA.test(v.source_revision)))
    ||typeof v.candidate_name!=='string'||typeof v.invoice_number!=='string'||typeof v.week_ending_date!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v.week_ending_date)
    || (v.blocker_code!==null&&(typeof v.blocker_code!=='string'||!(/^[A-Z][A-Z0-9_]{0,159}$/).test(v.blocker_code)))
    ||(v.blocker_code===null&&v.source_revision===null)||v.selectable!==false||v.automatic!==true
    ||v.selection_key!==`healthroster-credit:${v.original_invoice_id}:${v.reversal_timesheet_id}`
    ||new Set([v.reversal_timesheet_id,v.reversed_timesheet_id,v.replacement_timesheet_id]).size!==3) fail('CORRECTION_CREDIT_PROJECTION_INVALID');
  return Object.freeze(structuredClone(v));
}
function unwrap(v,name) {if(Array.isArray(v)&&v.length===1)v=v[0];if(object(v)&&Object.hasOwn(v,name))v=v[name];return v;}
export function isCorrectionCreditKey(value) {return typeof value==='string'&&/^healthroster-credit:[0-9a-f-]{36}:[0-9a-f-]{36}$/.test(value);}
// The root supplies this qualified capability from its existing controlled
// feature/release admission. Absence returns the existing ordinary behavior;
// code presence, environment strings or browser fields cannot enable it.
export async function loadAutomaticCorrectionCredits(deps,actorId,query) {
  if(typeof deps.healthRosterCorrectionConsumersReady!=='function'||await deps.healthRosterCorrectionConsumersReady()!==true) return null;
  if(typeof actorId!=='string'||!UUID.test(actorId)||!object(query)||query.action!=='GENERATE'||query.mode==='EXPLICIT_KEYS') fail('CORRECTION_CREDIT_QUERY_INVALID');
  let after=null;const rows=[],pages=[],seen=new Set();
  for(let index=0;index<100;index++) {
    const name='timesheet_correction_credit_candidates_v1';
    const value=unwrap(await deps.rpc(name,{p_request:{actor_user_id:actorId,query:structuredClone(query),after_request_id:after}}),name);
    if(!object(value)||Object.keys(value).some(k=>!['contract_version','rows','snapshot_hash','page'].includes(k))||value.contract_version!==CONTRACT
      ||!Array.isArray(value.rows)||value.rows.length>250||typeof value.snapshot_hash!=='string'||!SHA.test(value.snapshot_hash)
      ||!object(value.page)||Object.keys(value.page).some(k=>!['returned_count','has_more','next_request_id'].includes(k))
      ||value.page.returned_count!==value.rows.length||typeof value.page.has_more!=='boolean'
      ||(value.page.has_more?(value.rows.length!==250||!UUID.test(value.page.next_request_id)||value.page.next_request_id!==value.rows.at(-1)?.request_id):value.page.next_request_id!==null)) fail('CORRECTION_CREDIT_PROJECTION_INVALID');
    for(const raw of value.rows) {const item=row(raw);if(seen.has(item.selection_key)||(after&&item.request_id<=after))fail('CORRECTION_CREDIT_PROJECTION_INVALID');seen.add(item.selection_key);rows.push(item);}
    pages.push(value.snapshot_hash);
    if(!value.page.has_more)return Object.freeze({contract_version:CONTRACT,rows:Object.freeze(rows),page_hashes:Object.freeze(pages)});
    if(after&&value.page.next_request_id<=after)fail('CORRECTION_CREDIT_PROJECTION_INVALID');after=value.page.next_request_id;
  }
  fail('CORRECTION_CREDIT_COMMAND_LIMIT_EXCEEDED');
}
async function token(item) {
  const bytes=new TextEncoder().encode(['HEALTHROSTER_TIMESHEET_CREDIT_V1',item.original_invoice_id,item.reversal_timesheet_id,item.correction_id].join('|'));
  return 'HRCN:'+Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
}
function boundedCode(value) {return typeof value==='string'&&/^[A-Z][A-Z0-9_]{0,159}$/.test(value)?value:'CORRECTION_CREDIT_START_FAILED';}
export async function startAutomaticCorrectionCredits(envelope,startNative) {
  if(envelope===null)return [];
  if(!object(envelope)||envelope.contract_version!==CONTRACT||!Array.isArray(envelope.rows)||envelope.rows.length>25000||typeof startNative!=='function')fail('CORRECTION_CREDIT_PROJECTION_INVALID');
  // Validate every producer row before the first native command registration.
  const items=envelope.rows.map(row), seen=new Set();
  for(const item of items){if(seen.has(item.selection_key))fail('CORRECTION_CREDIT_PROJECTION_INVALID');seen.add(item.selection_key);}
  const results=[];
  for(const item of items) {
    try {
      const response=await startNative({command_type:'GENERATE_CREDIT_NOTE',base_invoice_id:item.original_invoice_id,
        credit_scope:{reversal_timesheet_id:item.reversal_timesheet_id},credit_reason:'HealthRoster correction',command_token:await token(item)});
      const value=response instanceof Response?await response.json():response;
      const commands=value?.per_command_results;
      if(!object(value)||!Array.isArray(commands)||commands.length!==1)fail('CORRECTION_CREDIT_START_RECEIPT_INVALID');
      const receipt=commands[0];
      if(receipt.command_type!=='GENERATE_CREDIT_NOTE'||receipt.selection_expansion_pending===true||receipt.selection_contract_version
        ||(receipt.operation_id!=null&&(typeof receipt.operation_id!=='string'||!UUID.test(receipt.operation_id))))fail('CORRECTION_CREDIT_START_RECEIPT_INVALID');
      const code=receipt.terminal_error?.code||receipt.error?.code||receipt.code||receipt.error_code;
      const done=code==='CREDIT_SCOPE_ALREADY_CREDITED';
      const accepted=receipt.accepted===true&&UUID.test(receipt.operation_id||'');
      if(!done&&!accepted&&receipt.accepted!==false)fail('CORRECTION_CREDIT_START_RECEIPT_INVALID');
      if(receipt.operation_id && (typeof receipt.status!=='string'||typeof receipt.phase!=='string'
        ||typeof receipt.operation_type!=='string'||typeof receipt.accepted!=='boolean'))fail('CORRECTION_CREDIT_START_RECEIPT_INVALID');
      const operation_receipt=receipt.operation_id?Object.fromEntries(['command_no','command_type','operation_id','operation_type',
        'accepted','status','phase','source_revision','change_seq','created','reused_active','reused_ready','blocked'].filter(key=>Object.hasOwn(receipt,key)).map(key=>[key,receipt[key]])):null;
      results.push({selection_key:item.selection_key,original_invoice_id:item.original_invoice_id,reversal_timesheet_id:item.reversal_timesheet_id,
        operation_id:receipt.operation_id||null,operation_receipt,accepted,blocked:receipt.blocked===true,already_credited:done,
        code:done?code:receipt.blocked===true?boundedCode(code||'CREDIT_SCOPE_BLOCKED'):accepted?null:boundedCode(code)});
    } catch(error) {results.push({selection_key:item.selection_key,original_invoice_id:item.original_invoice_id,reversal_timesheet_id:item.reversal_timesheet_id,
      operation_id:null,operation_receipt:null,accepted:false,blocked:false,already_credited:false,code:boundedCode(error?.code)});}
  }
  return results;
}
