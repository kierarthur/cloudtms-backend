const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const localQuery=require('./fixtures/banking-pay-local-query.cjs');
const oracle=require('./fixtures/banking-pay-legacy-oracle.cjs')();
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const literal=value=>`'${JSON.stringify(value).replaceAll("'","''")}'::jsonb`;
const sqlPath=path.resolve(__dirname,'../supabase/repeatable/28082026_1354_banking_pay_modal_case_action_facts.sql');
const candidate='00000000-0000-4000-8000-000000000031';
const financeCase='00000000-0000-4000-8000-000000000032';
const timesheet='00000000-0000-4000-8000-000000000033';
const base={candidate_id:candidate,finance_case_id:financeCase,case_key:`finance:${financeCase}`,case_needs_resolution:true};
const fields=['candidate_id','finance_case_id','case_key','linked_timesheet_id','resolution_family','case_needs_resolution',
  'case_resolution_satisfied_now','excluded_from_run','has_actionable_suggested_resolution','resolution_action_requires_actionable_components'];
const project=entry=>entry ? Object.fromEntries(fields.map(key=>[key,entry[key]])) : null;
function legacy(row){
  const state=oracle.buildCaseResolutionDisplayState({previewRoot:{},candidateRows:[],cache:{},caseLines:[row]});
  return state.caseGroups.flatMap(group=>group.entries)[0] || null;
}

test('case projection only publishes current action facts; resolution and amount owners stay untouched',()=>{
  const source=fs.readFileSync(sqlPath,'utf8');
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|pay_batch_items|pay_advances)\b/i);
  for(const name of ['openBucketedResolution','openNonBucketResolution','openTaxableFinanceCaseRestructure','toggleExcludeTimesheet']) assert.ok(source.includes(name));
});

test('all existing case-resolution buttons and exclusion gates match the frozen legacy renderer', {skip:!enabled},()=>{
  const fixtures=[];
  const components=[
    [], [{}], [{requires_resolution:false,suggested_resolution_payload_json:{}}],
    [{requires_resolution:true,suggested_resolution_payload_json:{}}],
    [{requires_resolution:true,suggested_resolution_result_json:{}}],
    [{requires_resolution:true,has_suggested_resolution:true}],
    [{requires_resolution:true,is_actionable_resolution_row:false,suggested_resolution_payload_json:{}}],
    [{requires_resolution:true,component_key_type:'TS_DAY',source_units:8,source_rate:20,source_charge_rate:25,bucket_code:'DAY'}],
    [{requires_resolution:true,component_key_type:'TS_DAY',source_units:8,source_rate:20,source_charge_rate:25}],
    [{needs_resolution:true,component_key_type:'ADDITIONAL_CODE',source_units:1,source_rate:0,source_charge_rate:0}],
    [{resolution_required:true,component_key_type:'TS_TOTAL',source_units:0,source_rate:20,source_charge_rate:25,bucket_code:'TOTAL'}]
  ];
  for(const family of ['BUCKETED','NON_BUCKET','TAXABLE_CHANNEL_RESTRUCTURE','']) {
    for(const needs of [true,false]) for(const satisfied of [true,false]) for(const summary of [null,{}]) {
      for(const list of components) for(const linked of ['',timesheet]) fixtures.push({...base,resolution_family:family,
        case_needs_resolution:needs,case_resolution_satisfied_now:satisfied,case_resolution_summary:summary,
        case_components:list,linked_timesheet_id:linked});
    }
  }
  for(let start=0;start<fixtures.length;start+=200){
    const batch=fixtures.slice(start,start+200);
    const [actual]=localQuery(`SELECT jsonb_agg(jsonb_build_object('meta',private.pay_workbench_modal_case_meta_v2(f),
      'actions',private.pay_workbench_modal_case_actions_v2(private.pay_workbench_modal_case_meta_v2(f))) ORDER BY ord)
      FROM jsonb_array_elements(${literal(batch)}) WITH ORDINALITY AS data(f,ord);`);
    batch.forEach((row,index)=>{
      const entry=legacy(row);
      assert.deepEqual(project(actual[index].meta),project(entry),`case facts ${start+index}`);
      const expected=entry ? [...oracle.renderCaseActionButtons(entry).matchAll(/data-action="([^"]+)"/g)].map(match=>match[1]) : [];
      assert.deepEqual(actual[index].actions,expected,`case actions ${start+index}`);
    });
  }
  assert.equal(fixtures.length,704);
});

test('legacy nested case, raw envelope, summary and explicit-empty component precedence is retained', {skip:!enabled},()=>{
  const component={requires_resolution:true,suggested_resolution_payload_json:{},source_basis_fingerprint:'synthetic-basis'};
  const cases=[
    {...base,resolution_family:'BUCKETED',components:[component],case_resolution_summary:{}},
    {...base,resolution_family:'BUCKETED',components:[component],case_components:[],case_resolution_summary:{}},
    {...base,case:{resolution_family:'NON_BUCKET',case_needs_resolution:false},case_resolution_summary:{case_needs_resolution:true}},
    {candidate_id:candidate,case:{...base,resolution_family:'TAXABLE_CHANNEL_RESTRUCTURE'}},
    {candidate_id:candidate,raw_case:{case:{...base,resolution_family:'NON_BUCKET',timesheet_id:timesheet}}},
    {...base,case_resolution_summary:{resolution_family:'BUCKETED'},row_json:{case_components:[component]}},
    {...base,caseResolutionSummary:{resolution_family:'BUCKETED'},rowJson:{caseComponents:[component]}},
    {...base,case_needs_resolution:false,case_resolution_summary:{case_needs_resolution:true,resolution_family:'NON_BUCKET'}},
    {...base,case_resolution_satisfied_now:true,case_resolution_summary:{case_resolution_satisfied_now:false}},
    {...base,excluded_from_run:false,exclude_from_run:true,linked_timesheet_id:timesheet},
    {...base,case_components:[{...component,is_actionable:'false'}],resolution_family:'BUCKETED',case_resolution_summary:{}},
    {candidate_id:candidate,case_type:'TIMESHEET_PAYMENT',timesheet_id:timesheet,needs_resolution:'yes',resolution_family:'BUCKETED'},
    {candidate_id:candidate},{}
  ];
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_case_meta_v2(f) ORDER BY ord)
    FROM jsonb_array_elements(${literal(cases)}) WITH ORDINALITY AS data(f,ord);`);
  assert.deepEqual(actual.map(project),cases.map(row=>project(legacy(row))));
});

test('case flags and manual-rate basis preserve false, blank, null, numeric and nested identities', {skip:!enabled},()=>{
  const cases=[];
  for(const flag of [undefined,null,false,true,0,1,'',' true ',' false ',' YES ','0']) {
    for(const actionable of [undefined,null,false,true,'',' false ',' yes ']) cases.push({...base,
      resolution_family:'BUCKETED',case_resolution_summary:{},case_components:[{
        requires_resolution:flag,is_actionable_resolution_row:actionable,suggested_resolution_payload_json:{}
      }]});
  }
  for(const units of [undefined,null,0,1,-1,1e-9,1.001e-9,' 1 ','1e2','not a number','Infinity']) {
    for(const rate of [undefined,null,0,'0','',-1]) cases.push({...base,resolution_family:'BUCKETED',
      case_resolution_summary:{},case_components:[{requires_resolution:true,component_key_type:'TS_DAY',
        source_units:units,source_rate:rate,source_charge_rate:0,source_basis_json:{bucket_code:'DAY'}}]});
  }
  for(const envelope of ['case','case_state','state','raw_case']) cases.push({candidate_id:candidate,
    [envelope]:{...base,resolution_family:'NON_BUCKET',timesheet_id:timesheet}});
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_case_meta_v2(f) ORDER BY ord)
    FROM jsonb_array_elements(${literal(cases)}) WITH ORDINALITY AS data(f,ord);`);
  cases.forEach((row,index)=>assert.deepEqual(project(actual[index]),project(legacy(row)),`edge case ${index}`));
  assert.equal(cases.length,147);
});
