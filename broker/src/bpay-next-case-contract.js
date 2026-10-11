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

export {CASE_INPUT_SCHEMA,caseInput,caseToken,caseEditToken,caseEditState,caseEditReceipt,PAUSE_ISSUES,PAUSE_RECEIPT_SCHEMA,pauseReason,pauseSummary,pauseProtection,pauseValue,casePauseState,casePauseReceipt,casePreview,equal};
