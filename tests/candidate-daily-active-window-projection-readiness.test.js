import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const read=(path)=>fs.readFileSync(new URL('../'+path,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const replacement='supabase/repeatable/31082026_1025_candidate_daily_active_window_projection_readiness_v1.sql';
const verifier='supabase/verification/31082026_1026_candidate_daily_active_window_projection_readiness_verification.sql';

test('Daily sync readiness counts only projection work for the active published window',()=>{
  const sql=read(replacement);
  assert.match(sql,/select g\.\* into v_generation/);
  assert.match(sql,/v_generation\.generation_id is null\s+or o\.availability_date between v_generation\.window_start and v_generation\.window_end/);
  assert.match(sql,/count\(\*\) filter\(where o\.state='TERMINAL'\)/);
  assert.match(sql,/if v_terminal>0 then v_state:='ERROR'/);
  assert.match(sql,/elsif v_effective>=v_scope\.canonical_version then v_state:='READY'/);
  assert.doesNotMatch(sql,/\b(delete|truncate)\s+from\s+public\.candidate_daily_sheet_projection_outbox/i);
  assert.doesNotMatch(sql,/pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
});

test('Active-window projection readiness has mandatory rollback-contained first-use proof',()=>{
  const release=JSON.parse(read('supabase/release/current-release.json'));
  for(const key of ['verificationFiles','newVerificationFiles']) {
    assert.ok(release[key].includes(verifier),`${key} is missing active-window verifier`);
  }
  const proof=read(verifier);
  for(const marker of [
    'historical failure blocked or was erased',
    'current-window failure was bypassed',
    'internal helper exposed',
    "state='TERMINAL'",
    'LEGACY_TARGET_UNAVAILABLE'
  ]) assert.ok(proof.includes(marker),marker);
  assert.match(proof,/^begin;/m);
  assert.match(proof,/^rollback;\s*$/m);
});
