// FC-03 exact transport shapes only; SQL alone derives schedules and money.
  const CASE_INPUT_SCHEMA=Object.freeze({candidate_id:'uuid',case_kind:'family',case_subtype:'subtype',tax_treatment:'tax',principal:'positiveMoney',due_date:'date?',start_monday:'date?',weekly_due:'positiveMoney?',week_count:'positive?',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',reason:'reason',note:'note?',original_liability_reference:'reference?'});
  const enc=new TextEncoder(),uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
  const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
  const equal=(a,b)=>object(a)&&object(b)?Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&equal(a[k],b[k])):Array.isArray(a)&&Array.isArray(b)?a.length===b.length&&a.every((x,i)=>equal(x,b[i])):a===b;
  const numeric=v=>typeof v==='string'&&/^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/.test(v)&&enc.encode(v).byteLength<=1024;
  const whole=v=>typeof v==='string'&&/^(?:0|[1-9]\d{0,18})$/.test(v)&&BigInt(v)<=9223372036854775807n;
  const date=v=>typeof v==='string'&&/^(?!0000)\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
  function value(v,t){if(t.endsWith('?'))return v===null||value(v,t.slice(0,-1));
    if(t==='uuid')return typeof v==='string'&&uuid.test(v);
    if(t==='money')return numeric(v);if(t==='positiveMoney')return numeric(v)&&/[1-9]/.test(v);
    if(t==='positive')return whole(v)&&v!=='0';if(t==='integer')return whole(v);
    if(t==='date')return date(v);
    if(t==='reason'||t==='note'||t==='reference')return typeof v==='string'&&!v.includes('\0')&&enc.encode(v).byteLength<=2048&&(t==='note'||v.trim()!=='');
    if(t==='family')return ['LOAN','ADVANCE','MANUAL_DEBT','CREDIT'].includes(v);
    if(t==='subtype')return ['LOAN','PAYMENT_ADVANCE','MANUAL_DEBT','MANUAL_CREDIT'].includes(v);
    if(t==='tax')return ['NOT_APPLICABLE','TAXABLE','NON_TAXABLE'].includes(v);
    if(t==='channel')return ['PAYE','UMBRELLA'].includes(v);return false;}
  const shape=(v,s)=>object(v)&&Object.keys(v).length===Object.keys(s).length&&Object.entries(s).every(([k,t])=>Object.hasOwn(v,k)&&value(v[k],t));
  function caseInput(v){return shape(v,CASE_INPUT_SCHEMA)&&enc.encode(JSON.stringify(v)).byteLength<=12288
    &&(v.original_liability_reference===null||v.case_kind==='MANUAL_DEBT')
    &&({LOAN:'LOAN',ADVANCE:'PAYMENT_ADVANCE',MANUAL_DEBT:'MANUAL_DEBT',CREDIT:'MANUAL_CREDIT'})[v.case_kind]===v.case_subtype
    &&(['LOAN','ADVANCE'].includes(v.case_kind)?v.tax_treatment==='NOT_APPLICABLE':['TAXABLE','NON_TAXABLE'].includes(v.tax_treatment))
    &&(v.case_kind==='CREDIT'?['start_monday','weekly_due','week_count','minimum_earnings_threshold','take_home_floor_override'].every(k=>v[k]===null)
      :v.start_monday!==null&&new Date(v.start_monday+'T00:00:00Z').getUTCDay()===1&&(v.weekly_due!==null||v.week_count!==null));}
  const CONTEXT_SCHEMA={actor_user_id:'uuid',owner_epoch:'positive',financial_view_revision:'integer',pay_channel:'channel',candidate_default_floor:'money?',valuation_policy_id:'uuid'};
  const EDIT_CONTEXT_SCHEMA={...CONTEXT_SCHEMA,case_id:'uuid',case_revision:'positive',rule_id:'uuid',rule_revision:'positive'};
  const EDIT_ISSUES=Object.freeze(['BPAY_NEXT_CASE_HAS_ACTIVITY','BPAY_NEXT_CASE_HELD_BY_DRAFT','BPAY_NEXT_CASE_EDIT_UNSUPPORTED','BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID']);
  function caseEditToken(v,edit){return object(v)&&Object.keys(v).sort().join()==='context,input'&&caseInput(v.input)
    &&shape(v.context,EDIT_CONTEXT_SCHEMA)&&enc.encode(JSON.stringify(v)).byteLength<=32768
    &&(!edit||(typeof edit.case_id==='string'&&v.context.case_id.toLowerCase()===edit.case_id.toLowerCase()
      &&v.context.case_revision===edit.expected_case_revision));}
  function caseEditState(v){
    const schema={case_id:'uuid',candidate_id:'uuid',case_revision:'positive',rule_id:'uuid',rule_revision:'positive',applied_edit_command_id:'uuid?'};
    if(!object(v)||Object.keys(v).sort().join()!==[...Object.keys(schema),'status','editable','issue_code','input','protection'].sort().join()
      ||!shape(Object.fromEntries(Object.keys(schema).map(k=>[k,v[k]])),schema)
      ||!['OPEN','PAUSED','CLOSED','WRITTEN_OFF'].includes(v.status)||typeof v.editable!=='boolean'
      ||!(v.issue_code===null||EDIT_ISSUES.includes(v.issue_code))||v.editable!==(v.issue_code===null)
      ||!(v.input===null||caseInput(v.input)&&v.input.candidate_id.toLowerCase()===v.candidate_id.toLowerCase()))return false;
    if(v.protection!==null){const p=v.protection,ps={run_id:'uuid',run_worker_id:'uuid?',channel:'channel?',protected_amount:'money?'};
      if(!object(p)||Object.keys(p).sort().join()!==[...Object.keys(ps),'run_status','worker_status','next_action'].sort().join()
        ||!shape(Object.fromEntries(Object.keys(ps).map(k=>[k,p[k]])),ps)
        ||!['PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE','EXPIRED','DISCARDED'].includes(p.run_status)
        ||!(p.worker_status===null||['PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE'].includes(p.worker_status))
        ||!['CANCEL_CANDIDATE_PAYMENT','REMOVE_CANDIDATE_OR_DISCARD','DISCARD_PREPARATION'].includes(p.next_action)
        ||v.issue_code!=='BPAY_NEXT_CASE_HELD_BY_DRAFT')return false;}
    return !v.editable||v.input!==null;
  }
  function caseEditReceipt(v){return object(v)&&Object.keys(v).sort().join()===['command_id','case_id','candidate_id','case_revision','rule_id','phase','issue_code'].sort().join()
    &&shape(Object.fromEntries(['command_id','case_id','candidate_id','case_revision','rule_id'].map(k=>[k,v[k]])),
      {command_id:'uuid',case_id:'uuid',candidate_id:'uuid',case_revision:'positive',rule_id:'uuid'})
    &&['PENDING','CASE_EDITED','BLOCKED'].includes(v.phase)
    &&(v.phase==='BLOCKED'?EDIT_ISSUES.includes(v.issue_code):v.issue_code===null)
    &&v.command_id.toLowerCase()===v.rule_id.toLowerCase();}

  const PAUSE_ISSUES=Object.freeze(['BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PAUSE_UNSUPPORTED',
    'BPAY_NEXT_CASE_PAUSE_STATE_CHANGED','BPAY_NEXT_CASE_PROTECTION_PENDING','BPAY_NEXT_CASE_PROTECTED_SLICE','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID']);
  const PAUSE_RECEIPT_SCHEMA=Object.freeze({command_id:'uuid',case_id:'uuid',candidate_id:'uuid',action:'pauseAction',
    sequence:'positive',expected_case_revision:'positive',phase:'pausePhase',applied_status:'pauseStatus?',
    applied_case_revision:'positive?',issue_code:'pauseIssue?',protection_summary:'pauseSummary?',first_protection:'pauseProtection?'});
  function pauseReason(v){return v===null||typeof v==='string'&&!v.includes('\0')&&enc.encode(v).byteLength<=2048;}
  function pauseSummary(v){return shape(Object.fromEntries(['unreserved_capacity','protected_count','protected_amount'].map(k=>[k,v?.[k]])),
    {unreserved_capacity:'money?',protected_count:'integer',protected_amount:'money?'})&&object(v)
    &&Object.keys(v).sort().join()==='next_cursor,protected_amount,protected_count,unreserved_capacity'
    &&(v.next_cursor===null||object(v.next_cursor)&&Object.keys(v.next_cursor).sort().join()==='after,case_revision'
      &&value(v.next_cursor.case_revision,'positive')&&v.next_cursor.after===null)
    &&(v.protected_count==='0'?v.next_cursor===null&&typeof v.protected_amount==='string'&&/^0(?:\.0+)?$/.test(v.protected_amount):v.next_cursor!==null);}
  function pauseProtection(v){const s={run_id:'uuid',run_worker_id:'uuid?',channel:'channel?',protected_amount:'money?'};
    return object(v)&&Object.keys(v).sort().join()===[...Object.keys(s),'run_status','worker_status','next_action'].sort().join()
      &&shape(Object.fromEntries(Object.keys(s).map(k=>[k,v[k]])),s)
      &&['PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE','EXPIRED','DISCARDED'].includes(v.run_status)
      &&(v.worker_status===null||['PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE'].includes(v.worker_status))
      &&['CANCEL_CANDIDATE_PAYMENT','REMOVE_CANDIDATE_OR_DISCARD','DISCARD_PREPARATION'].includes(v.next_action);}
  function pauseValue(v,t){if(t.endsWith('?'))return v===null||pauseValue(v,t.slice(0,-1));
    if(t==='pauseAction')return ['CASE_PAUSE','CASE_RESUME'].includes(v);
    if(t==='pausePhase')return ['PENDING','CASE_PAUSED','CASE_RESUMED','BLOCKED'].includes(v);
    if(t==='pauseStatus')return ['OPEN','PAUSED'].includes(v);
    if(t==='pauseIssue')return PAUSE_ISSUES.includes(v);
    if(t==='pauseSummary')return pauseSummary(v);if(t==='pauseProtection')return pauseProtection(v);return value(v,t);}
  function casePauseState(v){const s={case_id:'uuid',candidate_id:'uuid',case_revision:'positive'};
    return object(v)&&Object.keys(v).sort().join()===[...Object.keys(s),'status','supported','issue_code','protection_summary','first_protection'].sort().join()
      &&shape(Object.fromEntries(Object.keys(s).map(k=>[k,v[k]])),s)&&['OPEN','PAUSED','CLOSED','WRITTEN_OFF'].includes(v.status)
      &&typeof v.supported==='boolean'&&(v.issue_code===null||['BPAY_NEXT_CASE_PAUSE_UNSUPPORTED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID'].includes(v.issue_code))
      &&v.supported===(v.issue_code===null)&&(v.protection_summary===null||pauseSummary(v.protection_summary))
      &&(v.first_protection===null||pauseProtection(v.first_protection))
      &&(!v.supported||v.protection_summary!==null&&(v.protection_summary.protected_count==='0')===(v.first_protection===null));}
  function casePauseReceipt(v){if(!object(v)||Object.keys(v).length!==Object.keys(PAUSE_RECEIPT_SCHEMA).length
    ||!Object.entries(PAUSE_RECEIPT_SCHEMA).every(([k,t])=>Object.hasOwn(v,k)&&pauseValue(v[k],t)))return false;
    if(v.phase==='PENDING')return ['applied_status','applied_case_revision','issue_code','protection_summary','first_protection'].every(k=>v[k]===null);
    if(v.phase==='BLOCKED')return v.issue_code!==null&&v.applied_status===null&&v.applied_case_revision===null
      &&(!['BPAY_NEXT_CASE_PROTECTED_SLICE','BPAY_NEXT_CASE_PROTECTION_PENDING'].includes(v.issue_code)
        ||v.protection_summary!==null&&v.protection_summary.protected_count!=='0'&&v.first_protection!==null
          &&(v.issue_code==='BPAY_NEXT_CASE_PROTECTION_PENDING'?v.protection_summary.protected_amount===null&&v.protection_summary.unreserved_capacity===null:v.protection_summary.protected_amount!==null));
    return v.issue_code===null&&v.applied_case_revision!==null&&BigInt(v.applied_case_revision)===BigInt(v.expected_case_revision)+1n
      &&v.phase===(v.action==='CASE_PAUSE'?'CASE_PAUSED':'CASE_RESUMED')&&v.applied_status===(v.action==='CASE_PAUSE'?'PAUSED':'OPEN')
      &&v.protection_summary!==null&&v.protection_summary.protected_count==='0'&&v.first_protection===null;}

  function caseToken(v){return object(v)&&Object.keys(v).sort().join() === 'context,input'&&caseInput(v.input)&&shape(v.context,CONTEXT_SCHEMA)&&enc.encode(JSON.stringify(v)).byteLength<=24576;}
  function casePreview(v,input,edit=null){const s={candidate_id:'uuid',weekly_due:'positiveMoney?',week_count:'positive?',first_due_monday:'date?',last_due_monday:'date?',total:'positiveMoney',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',effective_take_home_floor:'money?'};
    if(!object(v)||Object.keys(v).length!==11||!shape(Object.fromEntries(Object.keys(s).map(k=>[k,v[k]])),s)||!(edit?caseEditToken(v.preview_token,edit):caseToken(v.preview_token))
      ||!equal(v.preview_token.input,input)||v.candidate_id.toLowerCase()!==input.candidate_id.toLowerCase()
      ||!Array.isArray(v.warnings)||v.warnings.length>1||v.warnings.some(x=>x!=='EXISTING_REPAYMENTS'))return false;
    const pennies=v=>v===null?null:BigInt(v.split('.')[0])*100n+BigInt((v.split('.')[1]??'').padEnd(2,'0'));
    if(pennies(v.total)!==pennies(input.principal)||pennies(v.minimum_earnings_threshold)!==pennies(input.minimum_earnings_threshold)
      ||pennies(v.take_home_floor_override)!==pennies(input.take_home_floor_override)
      ||pennies(v.effective_take_home_floor)!==pennies(input.take_home_floor_override??v.preview_token.context.candidate_default_floor)
      ||(input.weekly_due!==null&&pennies(v.weekly_due)!==pennies(input.weekly_due))
      ||(input.week_count!==null&&v.week_count!==input.week_count))return false;
    if(input.case_kind==='CREDIT')return ['weekly_due','week_count','first_due_monday','last_due_monday'].every(k=>v[k]===null);
    return [v.weekly_due,v.week_count,v.first_due_monday,v.last_due_monday].every(x=>x!==null)
      &&v.first_due_monday===input.start_monday&&new Date(v.first_due_monday+'T00:00:00Z').getUTCDay()===1
      &&new Date(v.last_due_monday+'T00:00:00Z').getUTCDay()===1
      &&BigInt((Date.parse(v.last_due_monday+'T00:00:00Z')-Date.parse(v.first_due_monday+'T00:00:00Z'))/604800000)===BigInt(v.week_count)-1n;}

// FC12 finite arrangement contract: validation only, SQL owns all derivation.
const RESTRUCTURE_INPUT={total_remaining:'positiveMoney',start_monday:'date',weekly_due:'positiveMoney?',week_count:'positive?',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',reason:'reason'};
const RESTRUCTURE_CONTEXT={actor_user_id:'uuid',candidate_id:'uuid',owner_epoch:'positive',financial_view_revision:'integer',case_id:'uuid',case_revision:'positive',rule_id:'uuid',rule_revision:'positive',approved_principal:'positiveMoney',original_command_id:'uuid',basis:'positiveMoney',outstanding:'money',status:'restructureStatus'};
const RESTRUCTURE_ISSUES=['BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_RESTRUCTURE_UNSUPPORTED','BPAY_NEXT_CASE_RESTRUCTURE_NO_BALANCE','BPAY_NEXT_CASE_RESTRUCTURE_TOTAL_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID'];
const restructureMoney=v=>v===null?null:BigInt(v.split('.')[0])*100n+BigInt((v.split('.')[1]??'').padEnd(2,'0'));
function restructureShape(v,s){return object(v)&&Object.keys(v).length===Object.keys(s).length&&Object.entries(s).every(([k,t])=>Object.hasOwn(v,k)&&(t==='restructureStatus'?['OPEN','PAUSED'].includes(v[k]):value(v[k],t)));}
function caseRestructureInput(v,defaults=false){return restructureShape(v,defaults?{...RESTRUCTURE_INPUT,reason:'note'}:RESTRUCTURE_INPUT)&&new Date(v.start_monday+'T00:00:00Z').getUTCDay()===1&&(v.weekly_due===null)!==(v.week_count===null)&&enc.encode(JSON.stringify(v)).byteLength<=8192;}
function restructureTerms(v){return shape(v,{weekly_due:'positiveMoney',week_count:'positive',first_due_monday:'date',last_due_monday:'date',total:'positiveMoney',minimum_earnings_threshold:'money?',take_home_floor_override:'money?'})
 &&new Date(v.first_due_monday+'T00:00:00Z').getUTCDay()===1&&new Date(v.last_due_monday+'T00:00:00Z').getUTCDay()===1
 &&BigInt((Date.parse(v.last_due_monday+'T00:00:00Z')-Date.parse(v.first_due_monday+'T00:00:00Z'))/604800000)===BigInt(v.week_count)-1n;}
function caseRestructureToken(v,args){return object(v)&&Object.keys(v).sort().join()==='context,input,terms'&&caseRestructureInput(v.input)&&restructureShape(v.context,RESTRUCTURE_CONTEXT)&&restructureTerms(v.terms)
 &&v.context.case_revision!=='9223372036854775807'&&enc.encode(JSON.stringify(v)).byteLength<=16384
 &&(!args||v.context.case_id.toLowerCase()===args.case_id.toLowerCase()&&v.context.case_revision===args.expected_case_revision&&equal(v.input,args.input))
 &&restructureMoney(v.terms.total)===restructureMoney(v.input.total_remaining)&&restructureMoney(v.terms.total)<=restructureMoney(v.context.basis)
 &&v.terms.first_due_monday===v.input.start_monday&&restructureMoney(v.terms.minimum_earnings_threshold)===restructureMoney(v.input.minimum_earnings_threshold)
 &&restructureMoney(v.terms.take_home_floor_override)===restructureMoney(v.input.take_home_floor_override)
 &&(v.input.week_count===null||v.terms.week_count===v.input.week_count)&&(v.input.weekly_due===null||restructureMoney(v.terms.weekly_due)===restructureMoney(v.input.weekly_due));}
function caseRestructurePreview(v,args,actor){return object(v)&&Object.keys(v).sort().join()==='after,before,context,defaults,preview_token,protection'&&restructureShape(v.context,RESTRUCTURE_CONTEXT)&&caseRestructureInput(v.defaults,true)
 &&restructureTerms(v.before)&&pauseSummary(v.protection)&&v.context.case_id.toLowerCase()===args.case_id.toLowerCase()&&v.context.case_revision===args.expected_case_revision
 &&(!actor||v.context.actor_user_id.toLowerCase()===actor.toLowerCase())&&restructureMoney(v.before.total)===restructureMoney(v.context.basis)
 &&restructureMoney(v.defaults.total_remaining)===restructureMoney(v.context.basis)
 &&(args.input===null?v.after===null&&v.preview_token===null:caseRestructureToken(v.preview_token,args)&&equal(v.preview_token.context,v.context)&&equal(v.preview_token.terms,v.after));}
function caseRestructureReceipt(v){const s={command_id:'uuid',case_id:'uuid',candidate_id:'uuid',sequence:'positive',expected_case_revision:'positive',rule_id:'uuid',applied_case_revision:'positive?'};
 if(!object(v)||Object.keys(v).sort().join()!==[...Object.keys(s),'phase','issue_code','completed_at_utc'].sort().join()||!shape(Object.fromEntries(Object.keys(s).map(k=>[k,v[k]])),s)
  ||v.rule_id.toLowerCase()!==v.command_id.toLowerCase()||!['PENDING','CASE_RESTRUCTURED','BLOCKED'].includes(v.phase))return false;
 if(v.phase==='PENDING')return v.applied_case_revision===null&&v.issue_code===null&&v.completed_at_utc===null;
 if(typeof v.completed_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T/.test(v.completed_at_utc)||!Number.isFinite(Date.parse(v.completed_at_utc)))return false;
 return v.phase==='BLOCKED'?RESTRUCTURE_ISSUES.includes(v.issue_code)&&v.applied_case_revision===null:v.issue_code===null&&BigInt(v.applied_case_revision??0)===BigInt(v.expected_case_revision)+1n;}

// FC13 finite native decision contract. Validation only; SQL owns valuation.
const CHANNEL_ISSUES=Object.freeze(['BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_CHANNEL_UNSUPPORTED',
 'BPAY_NEXT_CASE_CHANNEL_NO_BALANCE','BPAY_NEXT_CASE_PREVIEW_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID',
 'BPAY_NEXT_CASE_CHANNEL_CONTEXT_UNAVAILABLE']);
const CHANNEL_INPUT={resolution_mode:'channelMode',manual_total:'positiveMoney?',effective_date:'date',
 weekly_due:'positiveMoney?',week_count:'positive?',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',reason:'reason'};
const CHANNEL_CONTEXT={actor_user_id:'uuid',candidate_id:'uuid',owner_epoch:'positive',financial_view_revision:'integer',
 case_id:'uuid',case_revision:'positive',rule_id:'uuid',rule_revision:'positive',status:'channelStatus',case_kind:'channelFamily',
 source_basis:'money?',target_pay_channel:'channel',target_umbrella_id:'uuid?',target_umbrella_vat_chargeable:'boolean?',
 component_count:'integer',valuation_date:'date'};
const CHANNEL_RECEIPT_SCHEMA={command_id:'uuid',case_id:'uuid',candidate_id:'uuid',sequence:'positive',
 expected_case_revision:'positive',rule_id:'uuid?',phase:'channelPhase',applied_case_revision:'positive?',
 applied_component_count:'integer?',issue_code:'channelIssue?',completed_at_utc:'channelTimestamp?'};
function channelValue(v,t){if(t.endsWith('?'))return v===null||channelValue(v,t.slice(0,-1));
 if(t==='boolean')return typeof v==='boolean';
 if(t==='channelPercent')return typeof v==='string'&&/^(0|[1-9][0-9]*)([.][0-9]+)?$/.test(v)&&v.length<=1024;
 if(t==='channelMode')return ['SUGGESTED_EQUIVALENT_BASIS','MANUAL_AMOUNT'].includes(v);
 if(t==='channelStatus')return ['OPEN','PAUSED'].includes(v);
 if(t==='channelFamily')return ['OVERPAYMENT','MANUAL_DEBT','CREDIT'].includes(v);
 if(t==='channelSubtype')return ['OVERPAYMENT','MANUAL_DEBT','UNDERPAYMENT','MANUAL_CREDIT'].includes(v);
 if(t==='channelInstruction')return ['RECOVERY','CREDIT'].includes(v);
 if(t==='channelPhase')return ['PENDING','CASE_CHANNEL_RESOLVED','BLOCKED'].includes(v);
 if(t==='channelIssue')return CHANNEL_ISSUES.includes(v);
 if(t==='channelTimestamp')return typeof v==='string'&&/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?Z$/.test(v)&&Number.isFinite(Date.parse(v));
 return value(v,t);}
const channelShape=(v,s)=>object(v)&&Object.keys(v).length===Object.keys(s).length&&Object.entries(s).every(([k,t])=>Object.hasOwn(v,k)&&channelValue(v[k],t));
function caseChannelInput(v,defaults=false){return channelShape(v,defaults?{...CHANNEL_INPUT,reason:'note'}:CHANNEL_INPUT)
 &&enc.encode(JSON.stringify(v)).byteLength<=8192
 &&(v.resolution_mode==='MANUAL_AMOUNT')===(v.manual_total!==null)
 &&!(v.week_count!==null&&v.weekly_due!==null);}
function channelContext(v){return channelShape(v,CHANNEL_CONTEXT)&&v.case_revision!=='9223372036854775807'
 &&v.rule_revision!=='9223372036854775807'
 &&(v.target_pay_channel==='PAYE'?v.target_umbrella_id===null&&v.target_umbrella_vat_chargeable===null:
   v.target_umbrella_id!==null&&v.target_umbrella_vat_chargeable!==null);}
function channelTerms(v,input,context){if(context.case_kind==='CREDIT')return v===null&&['weekly_due','week_count',
 'minimum_earnings_threshold','take_home_floor_override'].every(k=>input[k]===null);
 if(!object(v)||Object.keys(v).sort().join()!=='effective_date,last_target_payment_week,last_target_week_amount,source_terms,target_payment_week_count,target_terms'
  ||v.effective_date!==input.effective_date||!restructureTerms(v.target_terms)||!restructureTerms(v.source_terms)
  ||!value(v.last_target_week_amount,'positiveMoney')||!value(v.target_payment_week_count,'positive')||!value(v.last_target_payment_week,'date')
  ||BigInt(v.target_payment_week_count)>BigInt(v.target_terms.week_count)||v.target_terms.week_count!==v.source_terms.week_count
  ||v.target_terms.first_due_monday!==v.source_terms.first_due_monday
  ||v.target_terms.last_due_monday!==v.source_terms.last_due_monday)return false;
 const date=new Date(input.effective_date+'T00:00:00Z'),monday=new Date(date.getTime()-((date.getUTCDay()+6)%7)*86400000).toISOString().slice(0,10);
 const total=restructureMoney(v.target_terms.total),weekly=restructureMoney(v.target_terms.weekly_due),payments=BigInt(v.target_payment_week_count);
 if(weekly<=0n||payments!==(total+weekly-1n)/weekly||restructureMoney(v.last_target_week_amount)!==total-weekly*(payments-1n))return false;
 const expectedLast=new Date(new Date(monday+'T00:00:00Z').getTime()+Number(payments-1n)*7*86400000).toISOString().slice(0,10);
 return v.last_target_payment_week===expectedLast&&v.target_terms.first_due_monday===monday&&restructureMoney(v.source_terms.total)===restructureMoney(context.source_basis)
  &&(input.week_count===null||v.target_terms.week_count===input.week_count)
  &&(input.weekly_due===null||restructureMoney(v.target_terms.weekly_due)===restructureMoney(input.weekly_due))
  &&['minimum_earnings_threshold','take_home_floor_override'].every(k=>restructureMoney(v.target_terms[k])===restructureMoney(input[k])
    &&restructureMoney(v.source_terms[k])===restructureMoney(input[k]));}
function caseChannelToken(v,args){if(!object(v)||Object.keys(v).sort().join()!=='context,input,target_total_ex_vat,target_total_inc_vat,target_total_vat,terms'
 ||!caseChannelInput(v.input)||!channelContext(v.context)||!channelTerms(v.terms,v.input,v.context)
 ||!value(v.target_total_ex_vat,'positiveMoney')||!value(v.target_total_vat,'money')||!value(v.target_total_inc_vat,'positiveMoney')
 ||!value(v.context.source_basis,'positiveMoney')||v.context.component_count==='0'
 ||v.context.valuation_date!==v.input.effective_date||enc.encode(JSON.stringify(v)).byteLength>32768)return false;
 return restructureMoney(v.target_total_inc_vat)===restructureMoney(v.target_total_ex_vat)+restructureMoney(v.target_total_vat)
  &&(v.terms===null||restructureMoney(v.terms.target_terms.total)===restructureMoney(v.target_total_ex_vat))
  &&(v.input.resolution_mode!=='MANUAL_AMOUNT'||restructureMoney(v.input.manual_total)===restructureMoney(v.target_total_ex_vat))
  &&(!args||v.context.case_id.toLowerCase()===args.case_id.toLowerCase()&&v.context.case_revision===args.expected_case_revision&&equal(v.input,args.input));}
function caseChannelCursor(v){return v===null||object(v)&&Object.keys(v).sort().join()==='after_component_ordinal,context'
 &&value(v.after_component_ordinal,'positive')&&channelContext(v.context)&&enc.encode(JSON.stringify(v)).byteLength<=8192;}
function caseChannelPreview(v,args,actor){const keys=['context','defaults','scheduled','before','source_principal','source_outstanding','source_reserved',
 'suggested_total_ex_vat','suggested_total_vat','suggested_total_inc_vat','after','chosen_total_ex_vat','chosen_total_vat','chosen_total_inc_vat',
 'preview_token','issue_code','protection','rows','next_cursor','candidate_balances','conversion_pct','source_pay_channels'];
 if(!object(v)||Object.keys(v).sort().join()!==keys.sort().join()||!channelContext(v.context)||!caseChannelInput(v.defaults,true)
  ||typeof v.scheduled!=='boolean'||v.scheduled!==(v.context.case_kind!=='CREDIT')||!(v.before===null||restructureTerms(v.before))
  ||!pauseSummary(v.protection)||!channelValue(v.issue_code,'channelIssue?')||!caseChannelCursor(v.next_cursor)
  ||v.context.case_id.toLowerCase()!==args.case_id.toLowerCase()||v.context.case_revision!==args.expected_case_revision
  ||actor&&v.context.actor_user_id.toLowerCase()!==actor.toLowerCase()
  ||!Array.isArray(v.rows)||v.rows.length>(args.limit??100)||enc.encode(JSON.stringify(v)).byteLength>120000)return false;
 if(!channelValue(v.conversion_pct,'channelPercent?')||!Array.isArray(v.candidate_balances)||v.candidate_balances.length>128
  ||!Array.isArray(v.source_pay_channels)||v.source_pay_channels.length>2||v.source_pay_channels.some(c=>!value(c,'channel'))
  ||new Set(v.source_pay_channels).size!==v.source_pay_channels.length)return false;
 const balances=new Set();for(const b of v.candidate_balances){if(!object(b)||Object.keys(b).sort().join()!=='case_kind,case_subtype,instruction_kind,issue_code,outstanding,source_pay_channel,tax_treatment'
  ||!['LOAN','ADVANCE','OVERPAYMENT','MANUAL_DEBT','CREDIT'].includes(b.case_kind)||!['LOAN','PAYMENT_ADVANCE','OVERPAYMENT','MANUAL_DEBT','UNDERPAYMENT','MANUAL_CREDIT'].includes(b.case_subtype)
  ||!['PAYOUT','RECOVERY','CREDIT'].includes(b.instruction_kind)||!['TAXABLE','NON_TAXABLE','NOT_APPLICABLE'].includes(b.tax_treatment)||!value(b.source_pay_channel,'channel')
  ||!value(b.outstanding,'money?')||!(b.issue_code===null||b.issue_code==='BPAY_NEXT_CASE_CHANNEL_CONTEXT_UNAVAILABLE')
  ||(b.outstanding===null)!==(b.issue_code!==null))return false;const key=[b.case_kind,b.case_subtype,b.instruction_kind,b.tax_treatment,b.source_pay_channel].join('|');if(balances.has(key))return false;balances.add(key);}
 const moneyKeys=['source_principal','source_outstanding','source_reserved','suggested_total_ex_vat','suggested_total_vat',
  'suggested_total_inc_vat','chosen_total_ex_vat','chosen_total_vat','chosen_total_inc_vat'];
 if(!moneyKeys.every(k=>value(v[k],'money?'))||v.issue_code!==null&&v.issue_code!=='BPAY_NEXT_CASE_CHANNEL_NO_BALANCE'&&['source_principal','source_outstanding','source_reserved',
  'suggested_total_ex_vat','suggested_total_vat','suggested_total_inc_vat'].some(k=>v[k]!==null))return false;
 const rowSchema={case_component_id:'uuid',component_ordinal:'positive',case_subtype:'channelSubtype',tax_treatment:'tax',
  instruction_kind:'channelInstruction',source_pay_channel:'channel',source_basis:'money?',target_ex_vat:'money?',target_vat:'money?',
  target_inc_vat:'money?',erni_pct:'channelPercent?',vat_rate_pct:'channelPercent?',converted:'boolean',issue_code:'channelIssue?'};let prior=BigInt(args.after?.after_component_ordinal??0);const ids=new Set();
 for(const r of v.rows){if(!channelShape(r,rowSchema)||r.tax_treatment==='NOT_APPLICABLE'||BigInt(r.component_ordinal)<=prior||ids.has(r.case_component_id.toLowerCase()))return false;
  prior=BigInt(r.component_ordinal);ids.add(r.case_component_id.toLowerCase());
  if((r.target_ex_vat===null)!==(r.target_vat===null)||(r.target_ex_vat===null)!==(r.target_inc_vat===null)
   ||r.target_ex_vat!==null&&restructureMoney(r.target_inc_vat)!==restructureMoney(r.target_ex_vat)+restructureMoney(r.target_vat))return false;}
 if(v.next_cursor!==null&&(!equal(v.next_cursor.context,v.context)||v.rows.length===0||BigInt(v.next_cursor.after_component_ordinal)!==prior))return false;
 if(args.after&&!equal(args.after.context,v.context))return false;
 if(args.input===null||v.issue_code!==null)return v.after===null&&v.preview_token===null&&['chosen_total_ex_vat','chosen_total_vat','chosen_total_inc_vat'].every(k=>v[k]===null);
 return caseChannelToken(v.preview_token,args)&&equal(v.preview_token.context,v.context)&&equal(v.preview_token.terms,v.after)
  &&['target_total_ex_vat','target_total_vat','target_total_inc_vat'].every(k=>v.preview_token[k]===v[k.replace('target_total','chosen_total')]);}
function caseChannelReceipt(v){if(!channelShape(v,CHANNEL_RECEIPT_SCHEMA)||v.rule_id!==null&&v.rule_id.toLowerCase()!==v.command_id.toLowerCase())return false;
 if(v.phase==='PENDING')return ['applied_case_revision','applied_component_count','issue_code','completed_at_utc'].every(k=>v[k]===null);
 if(v.completed_at_utc===null)return false;
 return v.phase==='BLOCKED'?v.applied_case_revision===null&&v.applied_component_count==='0'&&v.issue_code!==null:
  v.issue_code===null&&value(v.applied_case_revision,'positive')&&BigInt(v.applied_case_revision)===BigInt(v.expected_case_revision)+1n
   &&value(v.applied_component_count,'positive');}

export {CHANNEL_ISSUES,CHANNEL_RECEIPT_SCHEMA,channelValue,caseChannelInput,caseChannelToken,caseChannelCursor,caseChannelPreview,caseChannelReceipt,CASE_INPUT_SCHEMA,caseRestructureInput,caseRestructureToken,caseRestructurePreview,caseRestructureReceipt,RESTRUCTURE_ISSUES,caseInput,caseToken,caseEditToken,caseEditState,caseEditReceipt,PAUSE_ISSUES,PAUSE_RECEIPT_SCHEMA,pauseReason,pauseSummary,pauseProtection,pauseValue,casePauseState,casePauseReceipt,casePreview,equal};
