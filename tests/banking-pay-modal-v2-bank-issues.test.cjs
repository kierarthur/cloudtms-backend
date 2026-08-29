const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const query=require('./fixtures/banking-pay-local-query.cjs');
const root=path.resolve(__dirname,'..');
const file='supabase/repeatable/28082026_1952_banking_pay_modal_bank_issues.sql';
const base={candidate_id:'10000000-0000-4000-8000-000000000002',payee_entity_kind:'CANDIDATE',payee_entity_id:'10000000-0000-4000-8000-000000000002',bank_details_hash:'fixture',blockers:['BLOCKED_NAME_CHECK'],name_check_status:'NEAR_MATCH',name_check_has_override:false,payee_map_present:false};
const facts={rail_provider:'REVOLUT',rail_env:'SANDBOX',owner_exists:true,owner_link_valid:true,target_is_current:true,name_check_exists:true,name_check_status:'NEAR_MATCH',name_check_version:'current-result',override_current:false,mapping_present:false,umbrella_enabled:null};
const literal=value=>`'${JSON.stringify(value).replaceAll("'","''")}'::jsonb`;
test('bank problem projection is read-only, set-wise and reuses the existing action guard',()=>{
 const source=fs.readFileSync(path.join(root,file),'utf8');
 for(const owner of ['bank_sources_v2','bank_target_facts_v2','bank_job_facts_v2','bank_action_v2'])assert.ok(source.includes(owner));
 assert.match(source,/STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
 assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('current bank actions, editor availability, stale evidence and failed work remain distinct',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const cases=[];
 const add=(row,fact,job,editor,state,action,code)=>cases.push({row:{...base,...row},fact:{...facts,...fact},job,editor,state,action,code});
 add({}, {},null,false,'ACTION_REQUIRED','banking:pay:acceptBankDetails','NAME_CHECK_REVIEW');
 for(const status of ['FAIL','NEAR_MATCH','UNAVAILABLE'])add({name_check_status:status},{name_check_status:status},null,false,'ACTION_REQUIRED','banking:pay:acceptBankDetails','NAME_CHECK_REVIEW');
 add({name_check_status:''},{name_check_exists:false,name_check_status:null,name_check_version:null},null,false,'ACTION_REQUIRED','banking:pay:runBankNameCheck','NAME_CHECK_REQUIRED');
 add({blockers:['BLOCKED_NO_PAYEE_MAP'],name_check_status:'PASS'},{name_check_status:'PASS'},null,false,'ACTION_REQUIRED','banking:pay:ensurePayeeMap','PAYEE_MAP_REQUIRED');
 add({blockers:['BLOCKED_NO_PAYEE_MAP'],name_check_has_override:true},{override_current:true},null,false,'ACTION_REQUIRED','banking:pay:ensurePayeeMap','PAYEE_MAP_REQUIRED');
 for(const field of ['owner_exists','owner_link_valid','target_is_current'])add({}, {[field]:false},null,true,'BLOCKED',null,'BANK_TARGET_CHANGED');
 for(const changed of [{name_check_status:'PASS'},{override_current:true},{name_check_exists:false,name_check_version:null},{mapping_present:true}])add({},changed,null,true,'BLOCKED',null,'BANK_RESULT_CHANGED');
 add({}, {},{can_progress:true,is_failed:false,job_generation:'exact'},false,'UPDATING',null,'PAYEE_READINESS_PENDING');
 add({}, {target_is_current:false},{can_progress:true,is_failed:false},true,'BLOCKED',null,'BANK_TARGET_CHANGED');
 add({}, {},{can_progress:false,is_failed:true,job_generation:'failed'},false,'ACTION_REQUIRED','banking:pay:acceptBankDetails','PAYEE_READINESS_FAILED');
 add({blockers:[],name_check_status:'PASS',payee_map_present:true},{name_check_status:'PASS',mapping_present:true},{can_progress:false,is_failed:true},false,'BLOCKED',null,'PAYEE_READINESS_FAILED');
 add({}, {},{can_progress:false,is_failed:false,blocked_code:'READINESS_JOB_UNIT_TOO_LARGE'},true,'BLOCKED',null,'READINESS_JOB_UNIT_TOO_LARGE');
 for(const editor of [false,true])add({blockers:['BLOCKED_BANK_DETAILS'],bank_details_hash:''},{target_is_current:false},null,editor,editor?'ACTION_REQUIRED':'BLOCKED',editor?'openCandidate':null,'BLOCKED_BANK_DETAILS');
 for(const editor of [false,true])add({payee_entity_kind:'UMBRELLA',blockers:['BLOCKED_UMBRELLA_INACTIVE']},{umbrella_enabled:false},null,editor,editor?'ACTION_REQUIRED':'BLOCKED',editor?'openUmbrella':null,'BLOCKED_UMBRELLA_INACTIVE');
 add({blockers:[],payee_readiness_status:'RUNNING'}, {},null,false,'BLOCKED',null,'BANKING_PAY_REQUIRES_REFRESH');
 assert.equal(cases.length,24);
 const result=query(`SELECT jsonb_agg(private.pay_workbench_modal_bank_issue_v2(v.row_json,v.facts,v.job,v.editor) ORDER BY v.n) FROM (VALUES ${cases.map((c,n)=>`(${n},${literal(c.row)},${literal(c.fact)},${literal(c.job)},${c.editor})`).join(',')}) v(n,row_json,facts,job,editor);`)[0];
 assert.equal(result.length,cases.length);
 for(let i=0;i<cases.length;i++){const c=cases[i];assert.equal(result[i].state,c.state,`state ${i}`);assert.equal(result[i].action,c.action,`action ${i}`);assert.equal(result[i].code,c.code,`code ${i}`);}
 const hidden=query(`SELECT coalesce(private.pay_workbench_modal_bank_issue_v2(${literal({...base,presentation_role:'HIDDEN_INDEFINITE_SNOOZE'})},${literal(facts)},NULL,true),'null'::jsonb);`)[0];
 assert.equal(hidden,null);
});
test('one shared account task retains every scoped candidate and original source, including a source-only member',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1951_banking_pay_bank_issues_runtime.sql'],{cwd:root,encoding:'utf8',timeout:60000});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: shared bank problems retain/);
});
