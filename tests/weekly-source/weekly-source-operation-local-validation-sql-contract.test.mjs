import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('../../supabase/repeatable/22092026_1052_weekly_source_operation_local_validation_v1.sql',import.meta.url),'utf8');
test('Source observers never take global before/after job, token or scope snapshots',()=>{
  for(const forbidden of ['v_jobs_before','v_scope_tx_before','v_scope_state_before','v_pre_jobs','job_row.id=any(v_pre)']) assert.equal(source.includes(forbidden),false,forbidden);
  assert.equal((source.match(/cross join lateral/g)||[]).length,5);
  assert.equal((source.match(/effect\.row_id offset 0/g)||[]).length,8);
});
test('Capture is protected, transaction local, update-aware and cleared',()=>{
  assert.equal((source.match(/on commit delete rows/g)||[]).length,3);
  assert.match(source,/c\.relowner=v_owner and c\.relpersistence='t'/);
  assert.match(source,/WEEKLY_SOURCE_OBSERVATION_WRITE_AFTER_PROOF/);
  assert.match(source,/WEEKLY_SOURCE_OBSERVATION_IDENTITY_CHANGE/);
  assert.match(source,/WEEKLY_SOURCE_OBSERVATION_UNEXPECTED_DELETE/);
  assert.equal((source.match(/ws_operation_effect_capture_v1 after insert or update or delete/g)||[]).length,4);
  for (const name of ['storage','begin','seal','end']) assert.match(source,new RegExp('private.weekly_source_observation_'+name+'_v1'));
});

test('Candidate-registry-only writes are captured and validated by both owners before sealing',()=>{
  assert.match(source,/kind in \('JOB','TOKEN','SCOPE','REGISTRY'\)/);
  assert.match(source,/on private\.banking_pay_workbench_candidate_scope_registry for each row execute function private\.weekly_source_observe_effect_v1/);
  assert.match(source,/v_kind:='REGISTRY';\s+if tg_op='DELETE' then v_id:=old\.candidate_id; else v_id:=new\.candidate_id; end if;\s+if tg_op='UPDATE' then v_old_id:=old\.candidate_id; end if;/);
  const publication=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.weekly_source_entitlement_publish_core_v1'),source.indexOf('CREATE OR REPLACE FUNCTION public.weekly_source_first_authorisation_withdraw_v1'));
  const withdrawal=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.weekly_source_invalidation_contract_assert_v1'));
  for(const body of [publication,withdrawal]) {
    for(const reason of ['REGISTRY_ROW_MISSING_OR_DELETED','REGISTRY_FOR_A_DIFFERENT_CANDIDATE','REGISTRY_CARRIES_A_DIFFERENT_TOKEN']) assert.ok(body.includes(reason),reason);
    assert.ok(body.indexOf("effect.kind='REGISTRY'")<body.indexOf('perform private.weekly_source_observation_seal_v1'));
    assert.match(body,/where native\.candidate_id=effect\.row_id offset 0/);
  }
});
test('Inventory cache is invalidated by every member/head write and uses compatible locks',()=>{
  assert.match(source,/delete from pg_temp\.ws_inventory_checked_v1 where head_id=v_head/);
  assert.equal((source.match(/ws_inventory_changed_v1 before insert or update or delete/g)||[]).length,2);
  const invalidator=source.slice(source.indexOf('create or replace function private.weekly_source_inventory_changed_v1'),source.indexOf('-- BEGIN COMPLETE OWNER REPLACEMENTS'));
  assert.match(invalidator,/if tg_op='DELETE' then return old; end if;\s+return new;/);
  assert.match(source,/where head_row\.id=v_head_id for no key update/);
  assert.match(source,/if exists\(select 1 from pg_temp\.ws_inventory_checked_v1 where head_id=v_head_id\)/);
});
test('Withdrawal includes protected-hours effects before sealing and preserves the genuine owner',()=>{
  const start=source.indexOf('CREATE OR REPLACE FUNCTION public.weekly_source_first_authorisation_withdraw_v1');
  const end=source.indexOf('CREATE OR REPLACE FUNCTION private.weekly_source_invalidation_contract_assert_v1',start);
  const body=source.slice(start,end);
  assert.ok(body.indexOf('v_protected:=')<body.indexOf('v_contract:='));
  assert.match(body,/public\.timesheet_unauthorise_atomic/);
  assert.match(body,/perform private\.weekly_source_observation_end_v1\(v_observation_frame\)/);
  assert.match(source,/scope_state_row\.candidate_id is distinct from v_candidate_id/);
});

test('Withdrawal independently validates every captured scope before success/seal',()=>{
  const body=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.weekly_source_invalidation_contract_assert_v1'),source.indexOf('CREATE OR REPLACE FUNCTION private.weekly_source_entitlement_head_inventory_assert_v1'));
  for(const reason of ['SCOPE_ROW_MISSING_OR_DELETED','SCOPE_FOR_A_DIFFERENT_CANDIDATE','SCOPE_CARRIES_A_DIFFERENT_TOKEN','SCOPE_OUTSIDE_THE_DECLARED_SCOPE']) assert.ok(body.includes(reason),reason);
  assert.match(body,/left join lateral/);
  assert.ok(body.indexOf('for v_scope_row in')<body.indexOf('if v_failure is not null'));
  assert.ok(body.indexOf('if v_failure is not null')<body.indexOf('perform private.weekly_source_observation_seal_v1'));
});
