const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const test=require('node:test');
const {install,clone,ownerHash}=require('./fixtures/banking-pay-draft-worker-harness.cjs');
const root=path.resolve(__dirname,'..');
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const frontendRoot=process.env.BANKING_MODAL_FRONTEND_ROOT;
let snapshots;
function captures(){
  if(snapshots)return snapshots;
  snapshots={};
  for(const [mode,legacy] of [['legacy','true'],['v2','false']]){
    const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
      '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-v',`legacy=${legacy}`,
      '-f',path.join(__dirname,'28082026_1548_banking_pay_draft_contract_selection.sql')],
      {encoding:'utf8',timeout:45000,maxBuffer:12*1024*1024,cwd:root});
    assert.equal(result.status,0,result.error?.message||result.stderr);
    snapshots[mode]=result.stdout.trim().split(/\r?\n/).map(line=>JSON.parse(line));
    assert.deepEqual(snapshots[mode].map(value=>value.phase),['ALL','SOME','NONE']);
  }
  return snapshots;
}
function paymentContract(result){
  // Audit timestamps and revision increments legitimately differ between one
  // candidate action and several old individual actions. Compare the actual
  // unchanged Draft reader's membership/contracts, not invented amount maths.
  return clone({ok:result.ok,error_code:result.error_code,scope_counts:result.scope_counts,
    all_selected_preview_row_ids:result.all_selected_preview_row_ids,
    selected_preview_row_ids:result.selected_preview_row_ids,
    all_selected_preview_row_contracts:result.all_selected_preview_row_contracts,
    selected_preview_row_contracts:result.selected_preview_row_contracts,
    all_selected_economic_keys:result.all_selected_economic_keys,selected_economic_keys:result.selected_economic_keys});
}
test('the Draft contract runtime fixture is local-only, rollback-contained and cannot run a payment',()=>{
  const sql=fs.readFileSync(path.join(__dirname,'28082026_1548_banking_pay_draft_contract_selection.sql'),'utf8');
  const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
  assert.match(setup,/current_database\(\)<>'banking_modal_v2_test'/);assert.match(sql,/BEGIN;/);assert.match(sql,/ROLLBACK;\s*$/);
  assert.doesNotMatch(sql,/\b(?:COMMIT|pay_batch_execute|pay_batch_settle|remittance|banking_pay_operation_advance)\b/i);
  assert.match(sql,/before_batches<>\(SELECT count\(\*\) FROM public.pay_batches\)/);
  assert.match(sql,/before_items<>\(SELECT count\(\*\) FROM public.pay_batch_items\)/);
});
test('the existing complete Worker Create Draft owner is unchanged',()=>{
  assert.equal(ownerHash,'fa41380e83f7a6050dbadab046516cb3b8e42e1260896dd1c60e3c347b61ffd8');
});
for(const [index,phase] of ['ALL','SOME','NONE'].entries()){
  test(`real PG17 ${phase}: final locked Draft selection guard accepts the exact set and rejects four mismatches`,{skip:!enabled},()=>{
    for(const mode of ['legacy','v2']){
      const snapshot=captures()[mode][index];assert.equal(snapshot.guards.length,5);
      assert.equal(snapshot.guards[0].code,'WORKBENCH_REFRESH_IN_PROGRESS');
      assert.ok(snapshot.guards.slice(1).every(value=>value.code==='WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED'));
      assert.ok(snapshot.pages.every(page=>page.ok!==false));
    }
  });
  for(const scope of ['ALL','PAYE','UMBRELLA']){
    test(`real PG17 ${phase}/${scope}: new candidate changes supply the same unchanged Worker Draft contract`,{skip:!enabled},async()=>{
      const old=install(captures().legacy[index]),current=install(captures().v2[index]);
      const [oldResult,currentResult]=await Promise.all([old.read(scope),current.read(scope)]);
      assert.deepEqual(paymentContract(currentResult),paymentContract(oldResult));
      if(phase==='NONE'&&scope==='PAYE')assert.equal(currentResult.error_code,'BANKING_PAY_CREATE_DRAFT_NO_ROWS_FOR_SCOPE');
      else assert.equal(currentResult.ok,true,JSON.stringify({error:currentResult.error_code,invalid:currentResult.invalid_rows}));
      assert.equal(currentResult.all_selected_preview_row_count,phase==='ALL'?111:phase==='SOME'?110:2);
      assert.equal(current.requests.length,2); // one session read and one <=1000 selected-row page
    });
  }
  test(`real PG17 ${phase}: original preview-page payloads satisfy the unchanged frontend Draft recheck`,
    {skip:!enabled||!frontendRoot},async()=>{
    const {install:frontendInstall}=require(path.join(frontendRoot,'tests/fixtures/banking-pay-draft-owner-harness.cjs'));
    const outputs=[];
    for(const mode of ['legacy','v2']){
      const snapshot=captures()[mode][index],s=snapshot.session;
      const wizard={workbench:{session_id:s.id,session_version:s.version,progress_counter_version:s.progress_counter_version},
        decisions:{},preview:{data:{session_id:s.id,session_version:s.version},first_page_applied:true}};
      let cursor=0;
      const api=frontendInstall({wizard,readPage:async(session,section)=>{
        assert.equal(session,s.id);const next=snapshot.pages[cursor++];
        assert.equal(next.requested_section||next.section,section);return clone(next);
      }});
      const review={session_id:s.id,session_version:s.version,progress_counter_version:s.progress_counter_version,
        selected_preview_row_ids:mode==='legacy'?snapshot.rows.map(row=>row.id):[],selected_set_complete:mode==='legacy'};
      const result=await api.refresh(s.id,review.selected_preview_row_ids,'IMPLICIT_ALL','ALL',s.version,review);
      assert.equal(result.ok,true,JSON.stringify({error:result.error_code,invalid:result.invalid_preview_row_ids}));
      assert.equal(result.selected_preview_row_ids.length,snapshot.rows.length);assert.equal(cursor,snapshot.pages.length);
      outputs.push(clone(api.projectRequest(result,'ALL').request));
    }
    assert.deepEqual(outputs[0],outputs[1]);
  });
}
