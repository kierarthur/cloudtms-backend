// Reproducible synthetic XLSX -> real parser/staging producer. This creates
// rollback SQL for root; it never opens a database or changes application data.
import fs from 'node:fs';
import crypto from 'node:crypto';
import * as XLSX from 'xlsx';
import { parseWeeklySourceFile } from '../../../broker/src/weekly-source/index.js';
import { adaptWeeklySourceParserOutput } from '../../../broker/src/weekly-source/upload-staging-adapter.mjs';

const prefix = 'f5510000-0000-4000-8000-';
const id = (n) => prefix + String(n).padStart(12, '0');
const scope = { environment: 'TEST', agency_id: id(99), source_group_id: id(50), source_cycle_id: id(80), client_id: id(10) };
const week = '2026-11-01', workDate = '2026-10-26';
const values = { checking_profile_kind: 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1', invoice_sender: 'CLIENT_SELF_BILL',
  invoice_amount_basis: 'SOURCE_AMOUNT', source_amount_basis: 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',
  invoice_membership: 'FINALISED_IN_CYCLE_WINDOW',
  line_vat_method: 'LINE_GROSS_MINUS_NET_FROM_SOURCE', rate_difference_policy: 'FLAG_ALLOW_IGNORE' };
const headers = ['Request Id','Staff','Date','From','To','Break','Start','End','Actual Break','Hours',
  'Finalised Date','Timesheet Finalised By','Agency Worker Pay','Actual Cost','Agency Commission Amount'];
const workbook = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet([headers,
  ['F5-000123','F5 Worker',workDate,'09:00','17:00',0,'09:00','17:00',0,'8:00',
    '28/10/2026 23:59','F5 Manager','100.00','120.02','DO NOT MAP FEES']]), 'Export');
const bytes = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
const parsed = await parseWeeklySourceFile(bytes, { profileId: values.checking_profile_kind,
  expectedClient: 'F5 fixture self bill', effectiveRuleSet: { values } });
if (!parsed.ok) throw new Error(JSON.stringify(parsed.fatalErrors));
const produced = adaptWeeklySourceParserOutput(parsed, { actor_user_id: id(1), original_filename: 'f5-verify-money.xlsx',
  import_use: 'CHECKING', coverage: { start_local_date: '2026-10-26', end_local_date: week,
    confirmed_at_utc: '2026-10-13T12:00:00Z' } }, scope);
const fixture = { contract: 'HEALTHROSTER_F5_PRODUCER_FIXTURE_V1', fixture_prefix: prefix, fixed_week_ending: week,
  generated_from: 'Actual synthetic XLSX bytes through parseWeeklySourceFile and adaptWeeklySourceParserOutput',
  source_sha256: crypto.createHash('sha256').update(bytes).digest('hex'), byte_count: bytes.length,
  scope, rule_values: values, begin_request: produced.beginRequest, stage_request: produced.stageRequest,
  parser_money: parsed.rows[0].invoiceMoney, parser_finalised_at_utc: parsed.rows[0].finalisedAtUtc,
  financial_proof: false, native_database_calls: ['rule version save','dated assignment','bounded feature gate','stage begin','stage rows','seal','invoice hold release'] };
// The adapter exposes rows separately, so keep the actual API request shape.
fixture.stage_request ??= { physical_rows: produced.physicalRows, normalised_rows: produced.normalisedRows,
  money_evidence: produced.moneyEvidence, expense_evidence: produced.expenseEvidence };
if (!Array.isArray(fixture.stage_request.normalised_rows)) throw new Error('Missing actual stage producer');
if (fixture.stage_request.normalised_rows[0].finalised_at_utc !== '2026-10-28T23:59:00.000Z') throw new Error('Missing actual finalised timestamp producer');
const literal = (value) => "'" + JSON.stringify(value).replaceAll("'", "''") + "'::jsonb";
fs.mkdirSync('docs/healthroster-build', { recursive: true });
fs.writeFileSync('docs/healthroster-build/F5_PRODUCER_FIXTURE.json', JSON.stringify(fixture, null, 2) + '\n');
const sql = `-- F5 first-use configuration/staging/hold proof, root-only hosted TEST rollback.
-- No pay/finalisation/invoice/provider/comms owner; no fabricated READY/signature/release.
-- Fixed future week ${week}: regenerate/review if no longer future before rehearsal.
\\set ON_ERROR_STOP on
begin;
set local lock_timeout='5s';
set local statement_timeout='60s';
select set_config('request.jwt.claim.role','service_role',true);
create function pg_temp.f5_assert(p_ok boolean,p_name text) returns void language plpgsql as $f$
begin if p_ok is not true then raise exception 'F5_VERIFY_FAILED: %',p_name; end if; end; $f$;
create function pg_temp.f5_refuses(p_sql text,p_code text) returns void language plpgsql as $f$
begin begin execute p_sql; exception when others then if sqlerrm=p_code then return; end if;
raise exception 'F5_WRONG_REFUSAL: expected %, received %',p_code,sqlerrm; end;
raise exception 'F5_REFUSAL_MISSING: %',p_code; end; $f$;
select pg_temp.f5_assert(date '${week}'>current_date,'future fixture date');
select pg_temp.f5_assert(not exists(select 1 from public.tms_users where id::text like 'f5510000-%' or email like 'f5-%@example.invalid')
 and not exists(select 1 from public.clients where id::text like 'f5510000-%' or name='F5 fixture self bill')
 and not exists(select 1 from public.client_settings where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_groups where id::text like 'f5510000-%' or code='F5_VERIFY_MONEY')
 and not exists(select 1 from public.weekly_source_group_clients where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_client_policies where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_cycles where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_uploads where original_filename='f5-verify-money.xlsx')
 and not exists(select 1 from public.weekly_source_rule_sets where code='F5_VERIFY_RULES' or created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_rule_set_versions where created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_group_rule_assignments where created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_invoice_hold_releases where actor_user_id::text like 'f5510000-%'),'exact fixture vacancy');
insert into public.tms_users(id,email,role,password_hash,display_name,is_active,payment_authoriser,payment_golden_key) values
 ('${id(1)}','f5-admin@example.invalid','admin','not-a-real-password','F5 admin',true,false,false),
 ('${id(2)}','f5-authoriser@example.invalid','user','not-a-real-password','F5 authoriser',true,true,false);
insert into public.clients(id,name,ts_queries_email,vat_chargeable) values ('${id(10)}','F5 fixture self bill','f5-manager@example.invalid',true);
insert into public.client_settings(id,client_id,effective_from,hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,no_timesheet_required,requires_hr,is_nhsp)
 values ('${id(20)}','${id(10)}',date '${week}'-60,true,true,true,true,true,false);
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
 values ('${id(50)}','TEST','${id(99)}','F5_VERIFY_MONEY','F5 money','ROSTER',3,'23:59');
insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,created_by_user_id)
 values ('${id(60)}','${id(50)}','${id(10)}',date '${week}'-60,'${id(1)}');
insert into public.weekly_source_client_policies(id,source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,created_by_user_id)
 values ('${id(70)}','${id(50)}','${id(10)}',date '${week}'-60,'SOURCE_AUTHORITY','CHECK_ONLY',true,false,false,'${id(1)}');
insert into public.weekly_source_cycles(id,source_group_id,scope_client_id,finalisation_week_ending,cutoff_at_utc)
 values ('${id(80)}','${id(50)}','${id(10)}',date '${week}',((date '${week}'+3)+time '23:59') at time zone 'Europe/London');
create temporary table f5_owner_proof(name text primary key,value jsonb);
do $verify$
declare base jsonb:=${literal({ actor_user_id: id(1), agency_id: id(99), environment: 'TEST', source_group_id: id(50) })};
 saved jsonb; assigned jsonb; enabled jsonb; begun jsonb; staged jsonb; sealed jsonb; released jsonb; request jsonb;
 row_id uuid; row_hash text; cycle_version bigint; latest private.cloudtms_database_releases%rowtype; sig text; role_name text;
begin
 saved:=public.weekly_source_rule_set_version_save_atomic_v1(base||jsonb_build_object('request_id','${id(100)}',
   'rule_set',jsonb_build_object('id',null,'code','F5_VERIFY_RULES','display_name','F5 source money','expected_version',0),'values',${literal(values)}));
 assigned:=public.weekly_source_group_rule_assign_atomic_v1(base||jsonb_build_object('request_id','${id(110)}',
   'rule_set_version_id',saved->>'rule_set_version_id','effective_from_week_ending','${week}','expected_group_version',1));
 perform pg_temp.f5_assert(private.weekly_source_effective_rule_set_v1('${id(10)}','${id(80)}') is null,'saved assignment remains OFF');
 select * into strict latest from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1;
 perform pg_temp.f5_assert(latest.status='VERIFIED','actual latest release VERIFIED');
 enabled:=public.weekly_source_group_rule_feature_set_atomic_v1(base||jsonb_build_object('request_id','${id(120)}',
   'assignment_id',assigned->>'assignment_id','rule_set_version_id',saved->>'rule_set_version_id','effective_from_week_ending','${week}',
   'expected_assignment_version',1,'feature_enabled',true,'feature_purpose','RULE_SET','release_id',latest.release_id,'reason','F5 rollback staging fixture only'));
 begun:=public.weekly_source_upload_stage_begin_atomic_v1(${literal(fixture.begin_request)});
 perform pg_temp.f5_assert(begun->>'status'='STAGING','actual stage begin');
 staged:=public.weekly_source_upload_stage_rows_atomic_v1(${literal(fixture.stage_request)}||jsonb_build_object('actor_user_id','${id(1)}','upload_id',begun->>'logical_upload_id'));
 perform pg_temp.f5_assert(staged->>'status'='STAGING','actual money evidence stage');
 sealed:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id','${id(1)}','upload_id',begun->>'logical_upload_id'));
 perform pg_temp.f5_assert(sealed->>'status'='CURRENT','actual immutable seal: '||sealed::text);
 select r.id,encode(r.normalised_row_hash,'hex') into strict row_id,row_hash from public.weekly_source_upload_rows r where r.upload_id=(begun->>'logical_upload_id')::uuid;
 select version into strict cycle_version from public.weekly_source_cycles where id='${id(80)}';
 perform pg_temp.f5_assert((select source_shift_charge_pence=10000 and source_vat_pence=2002 and source_gross_pence=12002 and finalised_at_utc='2026-10-28 23:59Z'::timestamptz from public.weekly_source_upload_rows where id=row_id),'native stored literal money and real timestamp');
 request:=jsonb_build_object('actor_user_id','${id(1)}','source_cycle_id','${id(80)}','upload_row_id',row_id,
   'expected_source_row_hash',row_hash,'expected_authority_scope_version',cycle_version,'hold_kind','VAT_FIT');
 perform pg_temp.f5_refuses(format('select public.weekly_source_invoice_hold_release_atomic_v1(%L::jsonb)',request||jsonb_build_object('actor_user_id','${id(2)}')),'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED');
 perform pg_temp.f5_refuses(format('select public.weekly_source_invoice_hold_release_atomic_v1(%L::jsonb)',request||jsonb_build_object('expected_authority_scope_version',cycle_version+1)),'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE');
 released:=public.weekly_source_invoice_hold_release_atomic_v1(request);
 perform pg_temp.f5_assert(released->>'action'='BILL_AT_NET_PLUS_20' and released->>'net_pence'='10000' and released->>'vat_pence'='2000' and released->>'gross_pence'='12000','native bounded default hold release');
 perform pg_temp.f5_assert((public.weekly_source_invoice_hold_release_atomic_v1(request)->>'idempotent')::boolean,'exact hold replay');
 perform pg_temp.f5_refuses(format('select public.weekly_source_invoice_hold_release_atomic_v1(%L::jsonb)',request||jsonb_build_object('action','BILL_AS_FILE')),'WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT');
 perform pg_temp.f5_refuses(format('update public.weekly_source_invoice_hold_releases set action=action where id=%L::uuid',released->>'release_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
 perform pg_temp.f5_refuses(format('delete from public.weekly_source_invoice_hold_releases where id=%L::uuid',released->>'release_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
 perform pg_temp.f5_assert((select count(*)=2 from public.weekly_source_money_cell_evidence where upload_id=(begun->>'logical_upload_id')::uuid),'two exact source evidence cells; no fee');
 perform pg_temp.f5_assert(not exists(select 1 from public.weekly_source_final_revisions where source_cycle_id='${id(80)}') and not exists(select 1 from public.weekly_source_billing_movements where actual_client_id='${id(10)}'),'no fabricated financial producer');
 insert into f5_owner_proof values ('hold_release',released),('native_seal',sealed),('feature_gate',enabled);
end;
$verify$;
select * from f5_owner_proof order by name;
rollback;
`;
fs.writeFileSync('supabase/verification/13102026_1440_weekly_source_invoice_money_staging_hold_verification.sql', sql);
process.stdout.write(JSON.stringify({ generated: true, source_sha256: fixture.source_sha256,
  rows: fixture.stage_request.normalised_rows.length, money_cells: fixture.stage_request.money_evidence.length }) + '\n');
