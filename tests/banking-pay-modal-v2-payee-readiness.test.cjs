const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const legacyOracle=require('./fixtures/banking-pay-legacy-oracle.cjs');
const localQuery=require('./fixtures/banking-pay-local-query.cjs');
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const literal=value=>`'${JSON.stringify(value).replaceAll("'","''")}'::jsonb`;
const candidate='00000000-0000-4000-8000-000000000041';
const umbrella='00000000-0000-4000-8000-000000000042';
const sqlPath=path.resolve(__dirname,'../supabase/repeatable/28082026_1657_banking_pay_modal_payee_readiness_projection.sql');
const canonical=value=>JSON.parse(JSON.stringify(value));
test('synthetic readiness projection cannot create a payment, provider operation, new economic key or browser grant',()=>{
  const source=fs.readFileSync(sqlPath,'utf8');
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(source,/IMMUTABLE SECURITY INVOKER SET search_path TO ''/);
  assert.match(source,/FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(source,/CANDIDATE_FALLBACK/); // Existing display identity only; never a Draft economic key.
});
test('all synthetic bank problems and running/failed job states retain the complete existing row payload',{skip:!enabled},()=>{
  const codes=['BLOCKED_BANK_DETAILS','BLOCKED_NAME_CHECK','BLOCKED_NO_PAYEE_MAP','BLOCKED_UMBRELLA_INACTIVE'];
  const fixtures=[];
  for(let bits=0;bits<16;bits++)for(const status of ['','QUEUED','RUNNING','PENDING','IN_PROGRESS','FAILED','ERROR','SUCCEEDED'])for(const job of ['PAYEE_READINESS_ENSURE','OTHER']){
    fixtures.push({candidate_id:candidate,payee_entity_kind:'UMBRELLA',payee_entity_id:umbrella,bank_details_hash:'synthetic-hash',
      blockers:codes.filter((_,i)=>bits&(1<<i)),payee_readiness_status:status,latest_job_type:job,
      name_check:{status:'NEAR_MATCH',has_override:true},payee_map:{present:false},section_amount_display:'12.34'});
  }
  for(let from=0;from<fixtures.length;from+=100){
    const rows=fixtures.slice(from,from+100);
    const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_payee_readiness_row_v2(p,NULL,NULL) ORDER BY ord)
      FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY f(p,ord);`);
    const expected=rows.map(p=>canonical(legacyOracle({},'ALL',[],{preview:{payees:[p]}}).collectPayeeReadinessBlockedLines()[0]||null));
    assert.deepEqual(actual,expected,`readiness variants ${from}`);
  }
  assert.equal(fixtures.length,256);
});
test('fallbacks, explicit false, amount precedence, route aliases and missing physical payment preserve old semantics',{skip:!enabled},()=>{
  const meta={candidate_id:candidate,display_name:'Synthetic Candidate',tms_ref:'SYN-41',current_pay_method:'PAYE',
    payee_entity_kind:'CANDIDATE',payee_entity_id:candidate,bank_details_hash:'fallback-hash',
    payee_context:{bank_details_hash:'context-fallback'}};
  const base={candidate_id:candidate,blockers:['BLOCKED_BANK_DETAILS']};
  const rows=[base,{...base,candidate_id:undefined,candidateId:candidate},
    {...base,blockers:'blocked_bank_details, BLOCKED_BANK_DETAILS'},
    {...base,blockers:'["BLOCKED_BANK_DETAILS","BLOCKED_NAME_CHECK"]'},
    {...base,blockers:['BLOCKED_NAME_CHECK'],name_check_status:'FAIL',name_check_has_override:false,name_check:{has_override:true},payee_map_present:false,payee_map:{present:true}},
    {...base,payeeEntityKind:'UMBRELLA',payeeEntityId:umbrella,bankDetailsHash:'camel-display-only'},
    {...base,payee_context:{bank_details_hash:'nested-current'},bank_details_hash:''},
    {...base,bank_details_hash:' ',payee_bank_hash:'must-not-fallback'},
    {...base,payee_entity_kind:' ',entity_kind:'UMBRELLA'},
    {...base,section_amount_display:'',section_amount_ex_vat:'13.37',amount_display:'99.99'},
    {...base,section_amount_display:0,section_amount_ex_vat:'13.37'},
    {...base,amount_display:null,safe_amount_ex:'22.50',safe_amount_inc_vat:'27.00'},
    {...base,clientId:'client-fixture',payee_name:'Synthetic Account',pay_channel:' umbrella '},
    {...base,name_check_has_override:null,name_check:{has_override:true}},
    {...base,blockers:[],job_type:'PAYEE_READINESS_ENSURE',latest_payee_readiness_job_status:'RUNNING'},
    {blockers:['BLOCKED_BANK_DETAILS']},null,{}
  ];
  const oracleInput=p=>({preview:{payees:[p]},ready:[{candidate_id:candidate,section_amount_display:7.50}]});
  const expected=rows.map(p=>canonical(legacyOracle({},'ALL',[meta],oracleInput(p)).collectPayeeReadinessBlockedLines()[0]||null));
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_payee_readiness_row_v2(p,
    CASE WHEN COALESCE(p->>'candidate_id',p->>'candidateId')=${literal(candidate)}#>>'{}' THEN ${literal(meta)} ELSE NULL END,7.5) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY f(p,ord);`);
  actual.forEach((row,i)=>assert.deepEqual(row,expected[i],`readiness alias fixture ${i}`));
});
test('the synthetic bank route remains the exact old display route, not a new owner fallback',{skip:!enabled},()=>{
  const variants=[{}, {candidate_id:candidate},{candidateId:candidate},
    {payee_entity_kind:'UMBRELLA',payee_entity_id:umbrella},
    {entityKind:'umbrella',entityId:umbrella,payeeBankHash:'alias-hash'},
    {bank_details_hash:' ',bankDetailsHash:'must-not-fallback'},
    {payee_entity_kind:'',payeeEntityKind:'CANDIDATE',payeeEntityId:candidate}];
  const fallback={candidate_id:candidate,payee_entity_kind:'CANDIDATE',payee_entity_id:candidate,bank_details_hash:'fallback'};
  const oracle=legacyOracle();
  for(const meta of [null,fallback]){
    const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_payee_route_v2(p,${literal(meta)})->'legacy_display_route' ORDER BY ord)
      FROM jsonb_array_elements(${literal(variants)}) WITH ORDINALITY f(p,ord);`);
    assert.deepEqual(actual,variants.map(p=>oracle.payeeRouteKeyFrom(p,meta)));
  }
});
test('complete stored candidate bank metadata matches the unchanged candidate-map builder',{skip:!enabled},()=>{
  const rows=[];
  for(const context of [{},{payee_entity_kind:'UMBRELLA',payee_entity_id:umbrella,bank_details_hash:'context-hash',
    name_check_status:'NEAR_MATCH',name_check_has_override:true,payee_map_present:true},
    {bank_details_hash_snapshot:'context-snapshot',snapshot_bank_details_hash:'other-snapshot',blockers:['BLOCKED_NAME_CHECK']}]){
    for(const overrides of [{},{name_check_has_override:false,payee_map_present:false},
      {bank_details_hash:' ',payee_bank_hash:'must-not-override-space'},
      {display_name:'',candidate_name:'Fallback name',pay_channel:'PAYE',current_pay_method:'UMBRELLA'},
      {blockers:[],payee_entity_kind:'CANDIDATE',payee_entity_id:candidate},
      {name_check_status:'',payee_readiness_status:'QUEUED',latest_job_type:'PAYEE_READINESS_ENSURE'},
      {readiness_status:'FAILED',latest_payee_readiness_job_status:'ERROR',latest_job_status:'FAILED'}]){
      rows.push({candidate_id:candidate,payee_context:context,...overrides});
    }
  }
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_candidate_bank_meta_v2(p) ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY f(p,ord);`);
  const expected=canonical(legacyOracle().buildCandidateBankMeta(rows));
  // The real map deduplicates candidate ID. Compare each input separately so
  // all 21 normalization cases are tested, rather than only its final entry.
  rows.forEach((row,i)=>assert.deepEqual(actual[i],canonical(legacyOracle().buildCandidateBankMeta([row])[0]),`candidate bank metadata ${i}`));
  assert.equal(expected.length,1);
});
