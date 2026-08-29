const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const localQuery=require('./fixtures/banking-pay-local-query.cjs');
const oracle=require('./fixtures/banking-pay-legacy-oracle.cjs')();
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);
const sourcePath=path.resolve(__dirname,'../supabase/repeatable/28082026_1915_banking_pay_modal_finance_tasks.sql');
const literal=value=>`'${JSON.stringify(value).replaceAll("'","''")}'::jsonb`;
const candidate='10000000-0000-4000-8000-000000000002';
const finance='10000000-0000-4000-8000-000000003001';
const timesheet='10000000-0000-4000-8000-000000004001';
const base={candidate_id:candidate,finance_case_id:finance,case_key:`finance:${finance}`,linked_timesheet_id:timesheet,
  row_key:'finance-task-fixture',case_needs_resolution:true,case_resolution_satisfied_now:false};
const entry=row=>oracle.buildCaseResolutionDisplayState({previewRoot:{},candidateRows:[],cache:{},caseLines:[row]}).caseGroups.flatMap(g=>g.entries)[0];
const actions=html=>[...html.matchAll(/data-action="([^"]+)"/g)].map(m=>m[1]);
test('finance grouping uses existing action guards and never writes or creates financial authority',()=>{
  const source=fs.readFileSync(sourcePath,'utf8');
  for(const owner of ['case_meta','case_actions','component_actions','eligible_rows','source_progress_facts','hidden','row_matches_scope'])
    assert.ok(source.includes(`pay_workbench_modal_${owner}_v2`));
  assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE)\b/i);
  assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(source,/\b(?:min|max)\([^)]*::uuid/i);
});
test('finance tasks retain exact available actions; optional controls and fixed components do not create work',{skip:!enabled},()=>{
  const fixtures=[];
  for(const family of ['BUCKETED','NON_BUCKET','TAXABLE_CHANNEL_RESTRUCTURE'])
    for(const needs of [true,false])for(const satisfied of [true,false])
      for(const component of [null,{key:'C1',needs_action:true,show_manual_rate_control:true},
        {key:'C2',needs_action:true,show_manual_amount_control:true},
        {key:'C3',needs_action:true,show_suggested_rate:true,suggested_available:true},
        {key:'C4',needs_action:true,has_operator_choice:true},
        {key:'C5',needs_action:true,show_manual_amount_control:true,is_fixed_reimbursement:true}])
        fixtures.push({...base,resolution_family:family,case_needs_resolution:needs,case_resolution_satisfied_now:satisfied,
          case_components:component?[{...component,source_basis_fingerprint:'basis-1'}]:[]});
  const [actual]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_finance_tasks_v2(f,${literal(finance)} #>> '{}','generation-1') ORDER BY ord)
    FROM jsonb_array_elements(${literal(fixtures)}) WITH ORDINALITY data(f,ord);`);
  fixtures.forEach((row,i)=>{
    const legacy=entry(row),caseActions=actions(oracle.renderCaseActionButtons(legacy));
    const primary=caseActions.filter(a=>a!=='banking:pay:toggleExcludeTimesheet');
    const eligible=(legacy?.components||[]).filter(c=>c.needs_action===true
      &&!Boolean(c.is_fixed_reimbursement||c.is_fixed_no_action_taxable_row||['FIXED','NOT_REQUIRED'].includes(c.resolution_state))
      &&actions(oracle.renderComponentRows({...legacy,components:[c]})).some(a=>a!=='banking:pay:componentClearResolution'));
    const expected=primary.length?1:eligible.length;
    assert.equal(actual[i].length,expected,`task count ${i}`);
    if(primary.length){assert.equal(actual[i][0].family,'FINANCE_CASE');assert.deepEqual(actual[i][0].actions,caseActions);}
    else eligible.forEach((component,j)=>{
      assert.equal(actual[i][j].family,'FINANCE_COMPONENT');
      assert.deepEqual(actual[i][j].actions,actions(oracle.renderComponentRows({...legacy,components:[component]})));
      assert.deepEqual(actual[i][j].component,JSON.parse(JSON.stringify(component)));
    });
  });
  assert.equal(fixtures.length,72);
});
test('task identity is stable for a repeated case but changes for owner or current lineage',{skip:!enabled},()=>{
  const rows=[base,{...base,row_key:'another-presentation'},base,base];
  const [actual]=localQuery(`SELECT jsonb_build_array(
    private.pay_workbench_modal_finance_tasks_v2(${literal(rows[0])},'case-owner','generation-1'),
    private.pay_workbench_modal_finance_tasks_v2(${literal(rows[1])},'case-owner','generation-1'),
    private.pay_workbench_modal_finance_tasks_v2(${literal(rows[2])},'other-owner','generation-1'),
    private.pay_workbench_modal_finance_tasks_v2(${literal(rows[3])},'case-owner','generation-2'));`);
  assert.equal(actual[0][0].task_key,actual[1][0].task_key);
  assert.notEqual(actual[0][0].task_key,actual[2][0].task_key);
  assert.notEqual(actual[0][0].task_key,actual[3][0].task_key);
});
test('current preview empty components cannot revive stale nested component actions',{skip:!enabled},()=>{
  const row={...base,case_needs_resolution:false,case_components:[],components:[{key:'old',needs_action:true,show_manual_rate_control:true}]};
  const [result]=localQuery(`SELECT private.pay_workbench_modal_finance_tasks_v2(${literal(row)},'case-owner','generation-1');`);
  assert.deepEqual(result,[]);
});
test('component task keys bind the exact component and source basis; hidden rows never create tasks',{skip:!enabled},()=>{
  const component={key:'component-A',source_basis_fingerprint:'basis-A',needs_action:true,show_manual_amount_control:true};
  const row={...base,case_needs_resolution:false,case_components:[component]};
  const rows=[row,{...row,case_components:[{...component,key:'component-B'}]},
    {...row,case_components:[{...component,source_basis_fingerprint:'basis-B'}]},
    {...row,case_components:[{...component,presentation_role:'HIDDEN_INDEFINITE_SNOOZE'}]},
    {...base,presentation_role:'HIDDEN_INDEFINITE_SNOOZE'}];
  const [result]=localQuery(`SELECT jsonb_agg(private.pay_workbench_modal_finance_tasks_v2(f,'case-owner','generation-1') ORDER BY ord)
    FROM jsonb_array_elements(${literal(rows)}) WITH ORDINALITY data(f,ord);`);
  assert.equal(new Set(result.slice(0,3).map(items=>items[0].task_key)).size,3);
  assert.deepEqual(result[3],[]);assert.deepEqual(result[4],[]);
});
test('complete current finance task grouping retains all105 affected rows and rejects conflicting owners',{skip:!enabled},()=>{
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
    '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1919_banking_pay_finance_tasks_runtime.sql'],
    {cwd:path.resolve(__dirname,'..'),encoding:'utf8',timeout:60000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.match(result.stderr,/PASS:105 case presentations grouped into one task/);
});
