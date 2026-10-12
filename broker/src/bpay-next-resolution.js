/** WB29 typed native decisions. Validation only; prices and units belong to SQL. */
const encoder=new TextEncoder(),uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const modes=['SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT'];
const channels=['PAYE','UMBRELLA'];
const rowCodes=['BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS','BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE'];
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
const exact=(v,keys)=>object(v)&&Reflect.ownKeys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k)&&Object.getOwnPropertyDescriptor(v,k)?.enumerable===true&&Object.hasOwn(Object.getOwnPropertyDescriptor(v,k),'value'));
const same=(a,b)=>a===b||Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((v,n)=>same(v,b[n]))||object(a)&&object(b)&&exact(a,Object.keys(b))&&Object.keys(a).every(k=>same(a[k],b[k]));
const nullable=(v,check)=>v===null||check(v);
const id=v=>typeof v==='string'&&uuid.test(v);
const integer=(v,positive=false,max=9223372036854775807n)=>typeof v==='string'&&(positive?/^[1-9][0-9]{0,18}$/:/^(0|[1-9][0-9]{0,18})$/).test(v)&&BigInt(v)<=max;
const decimal=(v,scale=2,digits=16,signed=false)=>typeof v==='string'&&new RegExp(`^${signed?'-?':''}(0|[1-9][0-9]{0,${digits-1}})(\\.[0-9]{1,${scale}})?$`).test(v);
const rate=v=>decimal(v,6,12),money=v=>decimal(v),signedMoney=v=>decimal(v,2,16,true);
const scaled=(v,scale)=>{const negative=v.startsWith('-'),[a,b='']=v.replace(/^-/,'').split('.');return (negative?-1n:1n)*(BigInt(a)*10n**BigInt(scale)+BigInt(b.padEnd(scale,'0')));};
const cents=v=>scaled(v,2);
const zero=v=>typeof v==='string'&&/^0(?:\.0+)?$/.test(v);
const text=v=>typeof v==='string';
const reason=v=>nullable(v,x=>text(x)&&encoder.encode(x).length<=2000);
const nonblank=v=>text(v)&&v.replace(/^ +| +$/g,'')!=='';
const date=v=>{if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||v<'0001-01-01')return false;const d=new Date(v+'T00:00:00Z');return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v;};
const identity=r=>id(r.approved_line_id)&&['STANDARD','ADDITIONAL','ADJUSTMENT'].includes(r.rate_family)&&text(r.rate_code)&&[...r.rate_code].length>=1&&[...r.rate_code].length<=256&&(r.rate_family!=='STANDARD'||['DAY','NIGHT','SAT','SUN','BH'].includes(r.rate_code))&&(r.rate_family!=='ADDITIONAL'||[...r.rate_code].length<=245&&r.rate_code===r.rate_code.replace(/^ +| +$/g,'').toUpperCase());
const identityKey=r=>JSON.stringify([r.approved_line_id,r.rate_family,r.rate_code]);
const intent=(v,r)=>exact(v,['resolution_mode','manual_replacement_rate','manual_amount_ex_vat'])&&modes.includes(v.resolution_mode)&&nullable(v.manual_replacement_rate,rate)&&nullable(v.manual_amount_ex_vat,money)&&(v.resolution_mode==='SUGGESTED_EQUIVALENT_BASIS'?v.manual_replacement_rate===null&&v.manual_amount_ex_vat===null:nonblank(r)&&(v.resolution_mode==='MANUAL_REPLACEMENT_RATE'?v.manual_replacement_rate!==null&&v.manual_amount_ex_vat===null:v.manual_replacement_rate===null&&v.manual_amount_ex_vat!==null));
const fault=(code,status=503,retryable=false,reload=false)=>Object.assign(new Error(code),{code,status,retryable,reload});

export function normalizeResolutionInput(input){
  const bad=()=>{throw fault('BPAY_NEXT_RESOLUTION_REQUEST_INVALID',400);};
  if(!exact(input,['resolution']))bad();const s=input.resolution;
  if(!exact(s,['candidate_id','works','reason'])||!id(s.candidate_id)||!reason(s.reason)||!Array.isArray(s.works)||s.works.length<1||s.works.length>100||encoder.encode(JSON.stringify(input)).length>32768)bad();
  const works=new Set();let count=0;
  for(const w of s.works){
    if(!exact(w,['work_id','expected_revision_id','rows'])||!id(w.work_id)||!id(w.expected_revision_id)||!Array.isArray(w.rows)||!w.rows.length||w.rows.length>100)bad();
    if(works.has(w.work_id))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);works.add(w.work_id);const rows=new Set();
    for(const r of w.rows){
      if(++count>100)throw fault('BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE',400);
      if(!exact(r,['approved_line_id','rate_family','rate_code','intent'])||!identity(r)||!nullable(r.intent,x=>intent(x,s.reason)))bad();
      const key=identityKey(r);if(rows.has(key))throw fault('BPAY_NEXT_RESOLUTION_DUPLICATE_SCOPE',400);rows.add(key);
    }
  }
  return input;
}

const target=v=>channels.includes(v.target_pay_channel)&&(v.target_pay_channel==='PAYE'?v.target_umbrella_id===null&&v.target_umbrella_vat_chargeable===null:id(v.target_umbrella_id)&&typeof v.target_umbrella_vat_chargeable==='boolean');
function available(v){
  return exact(v,['provenance','paye_rate','umbrella_rate','charge_rate','unit_label'])&&['UNAVAILABLE','CURRENT_CONTRACT','APPROVAL_FROZEN_DAILY'].includes(v.provenance)&&['paye_rate','umbrella_rate','charge_rate'].every(k=>nullable(v[k],rate))&&nullable(v.unit_label,text)&&(v.provenance!=='UNAVAILABLE'||['paye_rate','umbrella_rate','charge_rate','unit_label'].every(k=>v[k]===null));
}
function basis(v){
  return exact(v,['work_id','physical_timesheet_id','physical_timesheet_version','financial_snapshot_id','source_kind','source_head_id','source_event_id','source_pay_channel','source_umbrella_id','valuation_policy_id','component_key','component_kind','source_component_id','work_date','unit_label','tax_treatment','rate_family','rate_code','source_units','source_rate','source_charge_rate','component_source_ex_vat','semantics'])
    &&['work_id','physical_timesheet_id','valuation_policy_id'].every(k=>id(v[k]))&&['financial_snapshot_id','source_head_id','source_event_id','source_umbrella_id','source_component_id'].every(k=>nullable(v[k],id))
    &&integer(v.physical_timesheet_version,true,2147483647n)&&['ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT'].includes(v.source_kind)&&channels.includes(v.source_pay_channel)
    &&text(v.component_key)&&[...v.component_key].length>=1&&[...v.component_key].length<=256&&['WORK','PROTECTED_WORK','ADDITIONAL','ADJUSTMENT'].includes(v.component_kind)
    &&nullable(v.work_date,date)&&nullable(v.unit_label,text)&&v.tax_treatment==='TAXABLE'&&identity({...v,approved_line_id:'00000000-0000-0000-0000-000000000000'})
    &&['source_units','source_rate','source_charge_rate'].every(k=>nullable(v[k],rate))&&money(v.component_source_ex_vat)&&['RATE_BEARING','AMOUNT_LED'].includes(v.semantics)
    &&(v.rate_family==='STANDARD'?['WORK','PROTECTED_WORK'].includes(v.component_kind)&&v.semantics==='RATE_BEARING':v.rate_family==='ADDITIONAL'?v.component_kind==='ADDITIONAL'&&v.component_key==='ADDITIONAL:'+v.rate_code:v.component_kind==='ADJUSTMENT'&&v.component_key===v.rate_code)
    &&v.semantics===(['STANDARD','ADDITIONAL'].includes(v.rate_family)&&v.source_units!==null&&!zero(v.source_units)&&v.source_rate!==null&&v.source_charge_rate!==null?'RATE_BEARING':'AMOUNT_LED');
}
function decision(v){
  return exact(v,['decision_id','decision_version','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','valuation_policy_id','valuation_date','resolution_mode','manual_replacement_rate','manual_amount_ex_vat','reason','clearable'])
    &&id(v.decision_id)&&integer(v.decision_version,true)&&target(v)&&id(v.valuation_policy_id)&&date(v.valuation_date)&&reason(v.reason)&&v.clearable===true
    &&intent({resolution_mode:v.resolution_mode,manual_replacement_rate:v.manual_replacement_rate,manual_amount_ex_vat:v.manual_amount_ex_vat},v.reason);
}
function quote(v,policy,position,saved=null){
  const keys=['source_ex_vat','source_component_residual_ex_vat','included_source_component_ex_vat','remaining_units','quote','valuation_policy_id','valuation_window_id'];
  if(saved!==null)keys.push('decision_id','decision_version');
  return exact(v,keys)&&nullable(v.source_ex_vat,money)&&money(v.source_component_residual_ex_vat)&&money(v.included_source_component_ex_vat)&&nullable(v.remaining_units,rate)
    &&v.valuation_policy_id===policy&&id(v.valuation_window_id)&&exact(v.quote,['ex','vat','inc','rate'])&&['ex','vat','inc'].every(k=>money(v.quote[k]))
    &&nullable(v.quote.rate,x=>decimal(x,6,120000))&&cents(v.quote.ex)+cents(v.quote.vat)===cents(v.quote.inc)
    &&cents(v.source_component_residual_ex_vat)===cents(position.approved_source_ex_vat)-cents(position.realised_source_ex_vat)-cents(position.held_source_ex_vat)
    &&cents(v.included_source_component_ex_vat)===cents(v.source_component_residual_ex_vat)
    &&(!zero(v.included_source_component_ex_vat)||zero(v.source_ex_vat)&&['ex','vat','inc'].every(k=>zero(v.quote[k]))&&v.quote.rate===null&&zero(v.remaining_units))
    &&(saved===null||v.decision_id===saved.decision_id&&v.decision_version===saved.decision_version);
}
const pair=v=>exact(v,['decision_id','decision_version'])&&id(v.decision_id)&&integer(v.decision_version,true);
const unavailable=v=>v===null||rowCodes.includes(v);
const quoteOrCode=(q,c)=>unavailable(c)&&((q===null)!==(c===null));
function manualEcho(q,choice){
  if(zero(q.included_source_component_ex_vat))return ['ex','vat','inc'].every(k=>zero(q.quote[k]))&&q.quote.rate===null&&zero(q.remaining_units);
  return choice.resolution_mode==='MANUAL_AMOUNT'?cents(q.quote.ex)===cents(choice.manual_amount_ex_vat)&&q.quote.rate===null
    :choice.resolution_mode!=='MANUAL_REPLACEMENT_RATE'||q.quote.rate!==null&&scaled(q.quote.rate,6)===scaled(choice.manual_replacement_rate,6);
}

export function validateResolutionResponse(payload,args,actorId){
  normalizeResolutionInput(args);const scope=args.resolution,bad=()=>{throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');};
  if(!exact(payload,['ok','kind','context','works'])||payload.ok!==true||payload.kind!=='RESOLUTION_CONTEXT'||encoder.encode(JSON.stringify(payload)).length>120000)bad();
  const c=payload.context;
  if(!exact(c,['agency_id','actor_user_id','candidate_id','owner_epoch','metadata_version','financial_view_revision','workbench_revision','adoption_revision','last_command_sequence','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','target_umbrella_enabled','official_pay_date','works'])
    ||!id(c.agency_id)||!id(c.actor_user_id)||c.actor_user_id!==actorId||c.candidate_id!==scope.candidate_id||!integer(c.owner_epoch,true)||!integer(c.metadata_version,true)
    ||!['financial_view_revision','workbench_revision','adoption_revision','last_command_sequence'].every(k=>integer(c[k]))||!target(c)||c.target_umbrella_enabled!==(c.target_pay_channel==='UMBRELLA'?true:null)||!date(c.official_pay_date)
    ||!Array.isArray(c.works)||!Array.isArray(payload.works)||c.works.length!==scope.works.length||payload.works.length!==scope.works.length)bad();
  const owners=new Set();let ownerCount=0;
  for(let n=0;n<scope.works.length;n++){
    const requested=scope.works[n],w=c.works[n],result=payload.works[n];
    if(!exact(w,['work_id','expected_revision_id','applied_revision_id','revision_no','physical_timesheet_id','physical_timesheet_version','original_timesheet_id','contract_id','week_ending_date','valuation_policy_id','source_pay_channel','source_umbrella_id','rows'])
      ||w.work_id!==requested.work_id||w.expected_revision_id!==requested.expected_revision_id||w.applied_revision_id!==requested.expected_revision_id||!integer(w.revision_no,true)
      ||!id(w.physical_timesheet_id)||!integer(w.physical_timesheet_version,true,2147483647n)||!id(w.original_timesheet_id)||!nullable(w.contract_id,id)||!date(w.week_ending_date)||!id(w.valuation_policy_id)||!channels.includes(w.source_pay_channel)||!nullable(w.source_umbrella_id,id)
      ||!exact(result,['work_id','expected_revision_id','rows'])||result.work_id!==w.work_id||result.expected_revision_id!==w.expected_revision_id||!Array.isArray(w.rows)||!Array.isArray(result.rows)||w.rows.length!==requested.rows.length||result.rows.length!==requested.rows.length)bad();
    for(let m=0;m<requested.rows.length;m++){
      const req=requested.rows[m],r=w.rows[m],out=result.rows[m];
      if(!exact(r,['approved_line_id','rate_family','rate_code','original_basis','position_basis','position_applied_revision_id','source_book_state','source_book_revision','source_binding_version_id','available_rates','decisions'])
        ||identityKey(r)!==identityKey(req)||!basis(r.original_basis)||!available(r.available_rates)||r.position_applied_revision_id!==w.applied_revision_id||!['UNBOUND','READY'].includes(r.source_book_state)
        ||!integer(r.source_book_revision,r.source_book_state==='READY')||r.source_book_state==='UNBOUND'&&r.source_book_revision!=='0'||!nullable(r.source_binding_version_id,id)||!Array.isArray(r.decisions)||r.decisions.length>100)bad();
      const b=r.original_basis,p=r.position_basis;
      if(!['work_id','physical_timesheet_id','physical_timesheet_version','valuation_policy_id','source_pay_channel','source_umbrella_id'].every(k=>b[k]===w[k])||b.rate_family!==req.rate_family||b.rate_code!==req.rate_code
        ||!exact(p,['approved_source_ex_vat','realised_source_ex_vat','held_source_ex_vat','source_basis_channel'])||!money(p.approved_source_ex_vat)||!signedMoney(p.realised_source_ex_vat)||!money(p.held_source_ex_vat)||!nullable(p.source_basis_channel,x=>channels.includes(x))||cents(p.approved_source_ex_vat)!==cents(b.component_source_ex_vat))bad();
      if(!exact(out,['approved_line_id','rate_family','rate_code','original_basis','available_rates','suggestion','suggestion_unavailable_code','expected_decision','saved_decision','saved_quote','saved_unavailable_code','incompatible_decisions','preview','preview_unavailable_code','requires_zero_confirmation'])
        ||identityKey(out)!==identityKey(req)||!same(out.original_basis,b)||!same(out.available_rates,r.available_rates)||!Array.isArray(out.incompatible_decisions)||typeof out.requires_zero_confirmation!=='boolean'
        ||!quoteOrCode(out.suggestion,out.suggestion_unavailable_code)||!nullable(out.suggestion,q=>quote(q,w.valuation_policy_id,p)))bad();
      let prior=null;
      for(const d of r.decisions){if(!decision(d)||prior!==null&&d.decision_id<=prior||owners.has(d.decision_id)||++ownerCount>100)bad();owners.add(d.decision_id);prior=d.decision_id;}
      const saved=out.saved_decision;
      if(saved===null){if(out.expected_decision!==null||out.saved_quote!==null||out.saved_unavailable_code!==null)bad();}
      else if(!decision(saved)||!pair(out.expected_decision)||out.expected_decision.decision_id!==saved.decision_id||out.expected_decision.decision_version!==saved.decision_version
        ||!['target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable'].every(k=>saved[k]===c[k])||saved.valuation_policy_id!==w.valuation_policy_id||saved.valuation_date!==c.official_pay_date
        ||!quoteOrCode(out.saved_quote,out.saved_unavailable_code)||!nullable(out.saved_quote,q=>quote(q,w.valuation_policy_id,p,saved)&&manualEcho(q,saved)))bad();
      const all=[...(saved===null?[]:[saved]),...out.incompatible_decisions];
      if(all.length!==r.decisions.length||new Set(all.map(d=>d?.decision_id)).size!==all.length||!all.every(d=>r.decisions.some(x=>same(x,d)))
        ||out.incompatible_decisions.some(d=>d.target_pay_channel===c.target_pay_channel&&d.target_umbrella_id===c.target_umbrella_id&&d.target_umbrella_vat_chargeable===c.target_umbrella_vat_chargeable&&d.valuation_policy_id===w.valuation_policy_id&&d.valuation_date===c.official_pay_date))bad();
      if(req.intent===null){if(out.preview!==null||out.preview_unavailable_code!==null||out.requires_zero_confirmation)bad();}
      else{
        if(!quoteOrCode(out.preview,out.preview_unavailable_code)||!nullable(out.preview,q=>quote(q,w.valuation_policy_id,p)))bad();
        if(out.preview!==null&&(!manualEcho(out.preview,req.intent)||req.intent.resolution_mode==='SUGGESTED_EQUIVALENT_BASIS'&&!same(out.preview,out.suggestion)))bad();
        const needs=out.preview!==null&&(zero(req.intent.manual_replacement_rate)||zero(req.intent.manual_amount_ex_vat)||zero(out.preview.quote.ex));if(out.requires_zero_confirmation!==needs)bad();
      }
    }
  }
  return payload;
}

// Exact native SQLSTATE/message pairs only. Unknown failures retain the normal read fault.
const nativePairs=new Map();
function errors(state,names,status,retryable=false,reload=false){for(const name of names.split(' '))nativePairs.set(state+':BPAY_NEXT_'+name,{status,retryable,reload});}
errors('22023','RESOLUTION_REQUEST_INVALID RESOLUTION_DUPLICATE_SCOPE RESOLUTION_IDENTITY_INVALID',400);
errors('22023','CONTRACT_RATE_SHAPE_INVALID ADDITIONAL_RATE_SHAPE_INVALID ADDITIONAL_RATE_CODE_INVALID RATE_INVALID',409);
errors('22003','RATE_PRECISION_UNSUPPORTED MILEAGE_RATE_INVALID',409);
errors('54000','RESOLUTION_SCOPE_TOO_LARGE WORKBENCH_RESPONSE_TOO_LARGE',413);
errors('42501','WORKBENCH_FORBIDDEN',403);
errors('55000','MODULE_NOT_ACTIVE',409);
errors('55000','WORKBENCH_DATE_UNAVAILABLE RESOLUTION_CONTEXT_UNAVAILABLE',503,true);
errors('23514','RESOLUTION_STALE RESOLUTION_POSITION_BASIS_CHANGED RESOLUTION_BASIS_CHANGED DAILY_REVISION_EVIDENCE_REQUIRED RESOLUTION_PUBLICATION_INCOMPATIBLE RESOLUTION_POSITION_INCOMPATIBLE',409,false,true);
errors('23514','RESOLUTION_IDENTITY_INVALID RESOLUTION_BUCKET_MISSING RESOLUTION_COMPATIBILITY_AMBIGUOUS RESOLUTION_SOURCE_BASIS_UNAVAILABLE RESOLUTION_MODE_INVALID RESOLUTION_POLICY_WINDOW_MISSING RESOLUTION_QUOTE_RANGE_INVALID TARGET_POLICY_WINDOW_AMBIGUOUS RESOLUTION_VERSION_MISSING',409);
export const BPAY_NEXT_RESOLUTION_REFUSALS=Object.freeze([...new Set([...nativePairs.keys()].map(k=>k.slice(6)))]);
export function resolutionNativeFault(payload){const rule=nativePairs.get(payload?.code+':'+payload?.message);return rule?fault(payload.message,rule.status,rule.retryable,rule.reload):fault('BPAY_NEXT_WORKBENCH_UNAVAILABLE',503,true);}
export function validateResolutionFault(payload){
  if(!exact(payload,['ok','error_code','retryable','reload_required','workbench_revision','complete','projection_state','dirty_candidate_count','dirty_group_count','pending_task_count'])
    ||payload.ok!==false||payload.complete!==false||typeof payload.retryable!=='boolean'||typeof payload.reload_required!=='boolean'
    ||!['workbench_revision','projection_state','dirty_candidate_count','dirty_group_count','pending_task_count'].every(k=>payload[k]===null)
    ||!BPAY_NEXT_RESOLUTION_REFUSALS.includes(payload.error_code)&&!['BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_RESPONSE_INVALID','BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE','BPAY_NEXT_WORKBENCH_REQUEST_INVALID','BPAY_NEXT_WORKBENCH_FORBIDDEN','BPAY_NEXT_WORKBENCH_UNAUTHORIZED','BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED'].includes(payload.error_code))throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');
  const native=[...nativePairs.entries()].filter(([key])=>key.slice(6)===payload.error_code).map(([,rule])=>rule);
  const generic={BPAY_NEXT_WORKBENCH_UNAVAILABLE:{retryable:true,reload:false},BPAY_NEXT_WORKBENCH_RESPONSE_INVALID:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_REQUEST_INVALID:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_FORBIDDEN:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_UNAUTHORIZED:{retryable:false,reload:false},BPAY_NEXT_WORKBENCH_METHOD_NOT_ALLOWED:{retryable:false,reload:false}};
  const rules=native.length?native:[generic[payload.error_code]];
  if(!rules.some(rule=>rule&&payload.retryable===rule.retryable&&payload.reload_required===rule.reload))throw fault('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID');return payload;
}
