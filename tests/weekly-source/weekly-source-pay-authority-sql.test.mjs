import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const sql=fs.readFileSync(new URL('../../supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql',import.meta.url),'utf8').replace(/\r\n/g,'\n');
function body(source,name){
  const escaped=name.replace(/\./g,'\\.');
  const start=source.search(new RegExp(`create or replace function ${escaped}\\s*\\(`,'i'));
  assert.ok(start>=0,name);const end=source.indexOf('$function$;',start);assert.ok(end>start,name);
  return source.slice(start,end+11);
}
function mutations(source){
  const clean=source.replace(/--[^\n]*/g,'');
  return [...clean.matchAll(/\b(?:insert\s+into|update(?=\s+(?:public|private)\.)|delete\s+from)\s+[\s\S]*?;/gi)].map(m=>m[0]);
}

test('C1/local payment-state writers preserve every existing financial mutation and refusal code byte-for-byte',()=>{
  const predecessors={
    weekly_exceptional_pay_record_c1_unknown_v1:'15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
    weekly_exceptional_pay_record_c1_checkpoint_v1:'15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
    weekly_exceptional_pay_record_c1_recovery_v1:'15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
    weekly_exceptional_pay_read_c1_request_v1:'15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
    weekly_exceptional_pay_complete_c1_publication_v1:'15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
    weekly_exceptional_pay_action_publication_status_v1:'15092026_1534_weekly_source_protected_action_orchestration_v1.sql',
    weekly_exceptional_pay_complete_local_v1:'04102026_1257_weekly_source_local_protected_decision_v1.sql',
  };
  for(const [name,file] of Object.entries(predecessors)){
    const old=body(fs.readFileSync(new URL(`../../supabase/repeatable/${file}`,import.meta.url),'utf8').replace(/\r\n/g,'\n'),`public.${name}`);
    const current=body(sql,`public.${name}`);
    assert.deepEqual(mutations(current),mutations(old),name);
    assert.deepEqual([...current.matchAll(/'WEEKLY_[A-Z0-9_]+'/g)].map(m=>m[0]),
      [...old.matchAll(/'WEEKLY_[A-Z0-9_]+'/g)].map(m=>m[0]),`${name}: error/contract order`);
    assert.equal((current.match(/for update/gi)||[]).length,(old.match(/for update/gi)||[]).length,`${name}: lock coverage`);
    assert.match(current,/weekly_source_pay_actor_allowed_v1/);
    assert.doesNotMatch(current,/payment_authoriser|payment_golden_key/);
  }
});

test('WP-PP changes no Banking/Candidate/pinned owner and ends all sixteen definitions in service-only ACL',()=>{
  const names=[...sql.matchAll(/create or replace function ([\w.]+)\s*\(/gi)].map(m=>m[1]);
  assert.equal(names.length,16);assert.equal(new Set(names).size,16);
  assert.doesNotMatch(names.join(' '),/banking|bpay|candidate|timesheet_authorise_generic_atomic|timesheet_unauthorise_atomic/);
  assert.doesNotMatch(sql,/pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
  assert.equal((sql.match(/revoke all on function/g)||[]).length,16);
  assert.equal((sql.match(/grant execute on function/g)||[]).length,16);
  assert.match(sql,/notify pgrst,'reload schema';\s*commit;/);
  const helper=body(sql,'private.weekly_source_pay_actor_allowed_v1');
  assert.match(helper,/returns boolean language sql stable security definer/);
  assert.match(helper,/set search_path to 'pg_catalog','pg_temp'/);
  assert.match(helper,/actor\.is_active is true/);
  assert.doesNotMatch(helper,/\b(insert|update|delete|Draft|provider|settlement|banking)\b/i);
});

test('reduced views retain current released-file reader, refuse finalise/recheck readers and keep Checklist pending',()=>{
  const ordinary=body(sql,'public.weekly_source_office_workspace_v1');
  const combined=body(sql,'public.weekly_source_combined_review_workspace_v1');
  assert.match(ordinary,/WEEKLY_SOURCE_PAYMENT_CURSOR_V1/);
  assert.match(ordinary,/'actor',v_actor,'view','PAY','source',v_workspace_version/);
  assert.match(ordinary,/v_result-array\['imports','finalise','history','recheck_payload','lifecycle_policy'\]/);
  assert.match(combined,/NHSP_PREFINAL_RELEASED_V1/);
  assert.match(combined,/weekly_source_upload_client_eligible_v1/);
  assert.match(combined,/if v_tab='queries' and not v_pay_view then\s*for v_scope/);
  assert.match(combined,/if v_tab='queries' and not v_pay_view then\s*for v_pending/);
  for(const source of [ordinary,combined]){
    assert.match(source,/WEEKLY_SOURCE_PAYMENT_VIEW_V1/);
    assert.match(source,/'checklist',jsonb_build_object\('available',false,'read_only',true,'pay_only',true,'action_links',false\)/);
  }
});
