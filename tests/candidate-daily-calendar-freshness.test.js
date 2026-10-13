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

test('Queued Candidate availability copies are a narrow canonical-ready exception in the late owner',()=>{
  const late=read('supabase/repeatable/13102026_1033_candidate_daily_availability_copy_readiness_v1.sql');
  assert.match(late,/v_scope.authority_mode='SUPABASE_PRIMARY'/);
  assert.match(late,/not v_scope.transition_in_progress/);
  assert.match(late,/c.actor_class is distinct from 'CANDIDATE'/);
  assert.match(late,/c.command_class is distinct from 'AVAILABILITY_APPLY'/);
  assert.match(late,/c.state is distinct from 'COMPLETED'/);
  assert.match(late,/count\(distinct o.availability_version\)/);
  assert.match(late,/o.state not in \('PENDING','CLAIMED','RETRY','DELIVERED'\)/);
  assert.match(late,/c.canonical_version_after is distinct from o.availability_version/);
  assert.equal((late.match(/CREATE OR REPLACE FUNCTION|create or replace function/g)||[]).length,2);
  assert.match(late,/BOOKED_DAILY_SHIFT/); assert.match(late,/candidate_entry.eligible/);
  assert.match(late,/d.booked then 'BOOKED'/);
  assert.doesNotMatch(late,/update public.candidate_daily_rota_days|delete from public.candidate_daily_rota_days/);
  const proof=read(verifier);
  for(const marker of ['second save lost either exact projection','older Sheet acknowledgement replaced the newer choice',
    'availability overwrote a new booking','unknown copy gap bypassed','background copy bypassed transition barrier']) {
    assert.ok(proof.includes(marker),marker);
  }
});
