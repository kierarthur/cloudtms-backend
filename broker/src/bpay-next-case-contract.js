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
  function caseToken(v){return object(v)&&Object.keys(v).sort().join() === 'context,input'&&caseInput(v.input)&&shape(v.context,CONTEXT_SCHEMA)&&enc.encode(JSON.stringify(v)).byteLength<=24576;}
  function casePreview(v,input){const s={candidate_id:'uuid',weekly_due:'positiveMoney?',week_count:'positive?',first_due_monday:'date?',last_due_monday:'date?',total:'positiveMoney',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',effective_take_home_floor:'money?'};
    if(!object(v)||Object.keys(v).length!==11||!shape(Object.fromEntries(Object.keys(s).map(k=>[k,v[k]])),s)||!caseToken(v.preview_token)
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

export {CASE_INPUT_SCHEMA,caseInput,caseToken,casePreview,equal};
