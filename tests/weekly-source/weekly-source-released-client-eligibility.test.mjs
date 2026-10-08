import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { buildWeeklySourceProjectionRows } from '../../broker/src/weekly-source/upload-publication-owner.mjs';

const sql = readFileSync(new URL('../../supabase/repeatable/08102026_1635_weekly_source_released_client_eligibility_v1.sql', import.meta.url), 'utf8');
const verifier = readFileSync(new URL('../../supabase/verification/15092026_1534_weekly_source_upload_context_v1.sql', import.meta.url), 'utf8');
const helper = sql.slice(sql.indexOf('create or replace function private.weekly_source_upload_client_eligible_v1'), sql.indexOf('alter function private.weekly_source_upload_client_eligible_v1'));

test('a matched released-file client keeps both links while a missing contract remains an Office check', () => {
  const candidateId = '92000000-0000-4000-8000-000000000001';
  const clientId = '92000000-0000-4000-8000-000000000002';
  const [row] = buildWeeklySourceProjectionRows({
    profile_id: 'NHSP_PREFINAL_RELEASED_V1',
    rows: [{
      upload_row_id: '92000000-0000-4000-8000-000000000003',
      candidate_match_count: 1, candidate_id: candidateId, client_id: clientId,
      source_row_ordinal: 2, work_date: '2026-09-21', contracts: [],
    }],
  });
  assert.equal(row.mapping_state, 'NO_ELIGIBLE_CONTRACT');
  assert.equal(row.blocker_code, 'NO_ELIGIBLE_CONTRACT');
  assert.equal(row.candidate_id, candidateId);
  assert.equal(row.client_id, clientId);
});

test('only the saved previously released checking profile permits cross-group clients', () => {
  assert.match(helper, /profile\.profile_code='NHSP_PREFINAL_RELEASED_V1'/);
  assert.match(helper, /source_group\.source_family='NHSP'/);
  assert.match(helper, /upload\.report_scope_id is null/);
  assert.match(helper, /profile\.row_finalisation_capability='CHECKING_ONLY'/);
  assert.match(helper, /not profile\.single_client_required/);
  assert.match(helper, /join public\.clients client on client\.id=p_client_id/);
  assert.match(helper, /p_work_date is not null/);
});

test('released eligibility requires applicable NHSP client settings, not an old or future flag', () => {
  assert.match(helper, /select settings\.is_nhsp\s+from public\.client_settings settings/);
  assert.match(helper, /settings\.client_id=client\.id/);
  assert.match(helper, /settings\.effective_from is null or settings\.effective_from<=p_work_date/);
  assert.match(helper, /order by settings\.effective_from desc nulls last,settings\.updated_at desc,settings\.id desc\s+limit 1/);
  assert.match(helper, /\),false\)/);
});

test('backing and other profiles retain membership dates and exact report identity', () => {
  assert.match(helper, /else[\s\S]*membership\.source_group_id=cycle\.source_group_id/);
  assert.match(helper, /p_work_date between membership\.valid_from and coalesce\(membership\.valid_to,'infinity'::date\)/);
  assert.match(helper, /scope\.client_id=client\.id and scope\.source_cycle_id=cycle\.id/);
  assert.match(helper, /scope\.source_group_id=cycle\.source_group_id/);
  assert.match(sql, /v_scope\.client_id is not null and v_client is distinct from v_scope\.client_id/);
  assert.match(sql, /WEEKLY_SOURCE_CONTEXT_REPORT_SCOPE_MISMATCH/);
});

test('automatic matching, saved choices and manual rechecks share one eligibility helper', () => {
  const context = sql.slice(sql.indexOf('create or replace function public.weekly_source_upload_context_v1'), sql.indexOf('create or replace function public.weekly_source_office_recheck_begin_v1'));
  const office = sql.slice(sql.indexOf('create or replace function public.weekly_source_office_recheck_begin_v1'));
  assert.equal((context.match(/private\.weekly_source_upload_client_eligible_v1\(/g) || []).length, 2);
  assert.match(context, /count\(\*\)=1[\s\S]*lower\(pg_catalog\.btrim\(client\.name\)\)/);
  assert.match(context, /v_row_client_id:=case when v_profile\.profile_code='NHSP_PREFINAL_RELEASED_V1'\s+then null else v_client_id end/);
  assert.match(office, /not private\.weekly_source_upload_client_eligible_v1\(\s*v_upload\.id,v_client,v_row\.work_date\)/);
  assert.match(office, /WEEKLY_SOURCE_CANDIDATE_INACTIVE_OR_MISSING/);
  assert.match(office, /WEEKLY_SOURCE_CONTRACT_NOT_ELIGIBLE/);
  assert.match(office, /WEEKLY_SOURCE_PREVIEW_STALE/);
});

test('helper is private and existing public RPC permissions and schema notification remain strict', () => {
  assert.match(sql, /revoke all on function private\.weekly_source_upload_client_eligible_v1\(uuid,uuid,date\)\s+from public,anon,authenticated,service_role/);
  for (const name of ['weekly_source_upload_context_v1', 'weekly_source_office_recheck_begin_v1']) {
    assert.match(sql, new RegExp('revoke all on function public\\.' + name + '\\(jsonb\\) from public,anon,authenticated'));
    assert.match(sql, new RegExp('grant execute on function public\\.' + name + '\\(jsonb\\) to service_role'));
  }
  assert.match(sql, /notify pgrst, 'reload schema'/);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('mandatory rollback verifier covers multi-client rows, manual replay and same-group backing rejection', () => {
  for (const assertion of [
    'released multi-client rows were not independently matched',
    'released upload client hint overrode row clients',
    'released file rejected a real client outside its group',
    'released file accepted a missing client',
    'released file accepted a non-NHSP client or missing settings',
    'released profile accepted a non-NHSP source',
    'released automatic match ignored applicable non-NHSP settings',
    'released automatic match silently selected an ambiguous NHSP client',
    'non-NHSP namesake blocked a unique eligible NHSP match',
    'released manual link accepted a non-NHSP client',
    'rejected non-NHSP link saved a choice',
    'backing report lost its exact client boundary',
    'rejected backing link saved a choice',
    'malformed released report scope bypassed client checks',
    'released manual client retry was not idempotent',
    'released saved manual client choice was lost',
    'released stale recheck accepted',
    'row client helper must be private to definer owners',
  ]) assert.ok(verifier.includes(assertion), assertion);
  assert.match(verifier, /rollback;\s*$/);
  assert.doesNotMatch(verifier, /disable trigger|session_replication_role/i);
});
