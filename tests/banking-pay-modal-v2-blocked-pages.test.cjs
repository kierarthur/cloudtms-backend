const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const {pathToFileURL}=require('node:url');const root=path.resolve(__dirname,'..');
test('Blocked list is one bounded service-only read of complete existing issue owners',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2201_banking_pay_modal_blocked_pages.sql'),'utf8');
 for(const owner of ['issue_index','bank_issue_members','source_issue_members','row_payload','blocked_presentation','context','draft_gate'])
  assert.ok(sql.includes('private.pay_workbench_modal_'+owner+'_v2'),owner);
 assert.match(sql,/public\.pay_workbench_session_get_blocked_page_v1/);assert.match(sql,/p_limit NOT BETWEEN 1 AND 100/);
 assert.match(sql,/256\*1024/);assert.match(sql,/SECURITY DEFINER SET search_path TO ''/);assert.match(sql,/TO service_role/);
 assert.doesNotMatch(sql,/\b(?:OFFSET|INSERT INTO|UPDATE public\.|DELETE FROM|LOOP)\b/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('actual Blocked list retains complete paged/sorted reasons and exact source-only evidence',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},async()=>{
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2201_banking_pay_blocked_pages_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: complete Blocked list/);
 const results=r.stdout.trim().split(/\r?\n/).filter(line=>line.startsWith('{')).map(JSON.parse);
 const worker=await import(pathToFileURL(path.join(root,'broker/src/banking-pay-modal-v2.js')).href);
 const issues=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-issues.js'));
 assert.equal(results.length,22);
 for(const {label,payload,summary} of results){
  const limit=label.startsWith('forty_')?40:100,cursor=payload.page_number>1?'current_boundary':null;
  const args={p_session_id:payload.session_id,p_options_json:{expected_session_version:payload.session_version,
   expected_progress_counter_version:payload.progress_counter_version,scope_hash:payload.scope_hash,pay_channel_scope:'ALL'},
   p_sort_key:payload.sort_key,p_sort_direction:payload.sort_direction,p_search:payload.search,p_limit:limit,p_cursor:cursor};
  assert.doesNotThrow(()=>worker.validateBankingPayModalEnvelope(payload,'blocked',args),label);
  assert.doesNotThrow(()=>issues.validate(payload,summary,'blocked',cursor,limit),label);
 }
 for(const key of ['CANDIDATE','REASON','AMOUNT'])for(const direction of ['ASC','DESC']){
  const rows=results.find(r=>r.label===key+'_'+direction).payload.rows.concat(results.find(r=>r.label===key+'_'+direction+'_next').payload.rows);
  assert.equal(rows.length,109);assert.equal(new Set(rows.map(row=>row.identity)).size,109);
  for(let i=1;i<rows.length;i++){
   const field=row=>key==='AMOUNT'?Number(row.affected_display_amount):key==='REASON'?row.reason.toLowerCase():
    row.candidate_name.toLowerCase()+'\n'+row.candidate_reference.toLowerCase();
   const a=field(rows[i-1]),b=field(rows[i]);
   assert.ok(direction==='ASC'?a<=b:a>=b,key+' '+direction+' complete order');
   if(a===b)assert.ok(rows[i-1].identity<rows[i].identity,'stable issue tie-breaker');
  }
 }
});
