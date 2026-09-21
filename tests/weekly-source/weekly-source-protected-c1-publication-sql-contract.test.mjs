import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL(
  '../../supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
  import.meta.url,
), 'utf8');

test('protected C1 durable owners are service-only and complete the exact staged generation', () => {
  for (const name of [
    'weekly_exceptional_pay_record_c1_unknown_v1',
    'weekly_exceptional_pay_record_c1_checkpoint_v1',
    'weekly_exceptional_pay_record_c1_recovery_v1',
    'weekly_exceptional_pay_read_c1_request_v1',
    'weekly_exceptional_pay_stage_c1_request_v1',
    'weekly_exceptional_pay_complete_c1_publication_v1',
  ]) {
    assert.match(sql, new RegExp(`create or replace function public\\.${name}\\(`, 'i'));
    assert.match(sql, new RegExp(`revoke all on function public\\.${name}\\(jsonb\\)[\\s\\S]*?from public,anon,authenticated,service_role`, 'i'));
    assert.match(sql, new RegExp(`grant execute on function public\\.${name}\\(jsonb\\)[\\s\\S]*?to service_role`, 'i'));
  }
  assert.match(sql, /set lifecycle_state='PUBLISHED'/i);
  assert.match(sql, /set ownership_state='TARGET_MANAGED',current_generation_id=v_generation\.id/i);
  assert.match(sql, /set state='COMPLETE',after_state_fingerprint=v_receipt_hash/i);
  assert.doesNotMatch(sql, /insert\s+into\s+public\.pay_batches/i);
  assert.doesNotMatch(sql, /insert\s+into\s+public\.pay_batch_items/i);
});

test('unknown START retains the request identity without mislabelling it as a C1 operation', () => {
  assert.match(sql, /if v_stream_kind='START' then[\s\S]*?v_stream_id:=\(v_recovery->>'stream_id'\)::uuid;[\s\S]*?v_stream_id is distinct from v_request_id[\s\S]*?v_stream_id:=null;/i);
  assert.match(sql, /c1_operation_id=coalesce\(c1_operation_id,v_stream_id\)/i);
});

test('durable checkpoints accept the exact control or status envelope but no union-shaped hybrid', () => {
  assert.match(sql, /v_control_result_keys constant text\[\]:=array\[/i);
  assert.match(sql, /v_status_result_keys constant text\[\]:=array\[/i);
  assert.match(sql, /weekly_exceptional_json_keys_exact_v1\(v_result,v_control_result_keys\)[\s\S]*?or private\.weekly_exceptional_json_keys_exact_v1\(v_result,v_status_result_keys\)/i);
  assert.doesNotMatch(sql, /v_result_keys constant text\[\]/i);
});

