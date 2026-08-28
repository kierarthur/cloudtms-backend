import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read=p=>fs.readFileSync(new URL('../'+p,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const replacement='supabase/repeatable/28082026_1320_candidate_daily_calendar_freshness_v1.sql';
const verifier='supabase/verification/28082026_1321_candidate_daily_calendar_freshness_verification.sql';
const sql=read(replacement);

test('Operational Rota retains the last complete London window without a time-based read TTL',()=>{
  assert.match(sql,/v_today date:=\(p_now_utc at time zone 'Europe\/London'\)::date/);
  assert.match(sql,/window_start>v_today/);
  assert.match(sql,/window_end is distinct from v_generation.window_start\+13/);
  assert.match(sql,/v_days<>14/);
  assert.match(sql,/state<>'ACTIVE'/);
  assert.match(sql,/generation_age_seconds',v_age/);
  assert.match(sql,/projection_warning_seconds',120/);
  assert.match(sql,/PROJECTION_LAG/);
  assert.match(sql,/TERMINAL_OUTBOX/);
  assert.match(sql,/IDENTITY_NOT_READY/);
  assert.doesNotMatch(sql,/v_age\s*>\s*120|pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
  assert.equal((sql.match(/create or replace function/gi)||[]).length,3);
  const freshness=sql.split('create or replace function public.')[0];
  assert.doesNotMatch(freshness,/\b(update|insert|delete)\s+(into|from|public\.|private\.)/i);
  assert.match(sql,/v_policy='CANDIDATE_SURFACE' and v_from=v_today/);
  assert.match(sql,/d.rota_date>=v_today/);
  assert.match(sql,/v_row.rota_date<\(now\(\) at time zone 'Europe\/London'\)::date/);
});

test('Calendar freshness runtime guard is mandatory for UPGRADE and NEW and tests real first use',()=>{
  const release=JSON.parse(read('supabase/release/current-release.json'));
  for(const key of ['verificationFiles','newVerificationFiles']) assert.ok(release[key].includes(verifier));
  const proof=read(verifier);
  for(const marker of ['23-hour','25-hour','projection lag bypassed','failed projection bypassed',
    'partial window accepted','last complete window disappeared at midnight','internal helper exposed',
    'past availability write accepted','unpublished date write accepted',
    'private._candidate_daily_capability_v1']) assert.ok(proof.includes(marker),marker);
  assert.match(proof,/^begin;/m);
  assert.match(proof,/^rollback;\s*$/m);
});

test('Controlled authority transition retains its separate strict freshness proof',()=>{
  const transition=read('supabase/repeatable/26082026_1607_candidate_daily_membership_ready_activation_v1.sql');
  assert.match(transition,/interval '120 seconds'/);
  assert.match(transition,/FEDERATED_MEMBERSHIP_ACTIVE/);
  assert.doesNotMatch(sql,/create or replace function public\.candidate_daily_(system_policy_activate_ready|authority_transition)/);
});
