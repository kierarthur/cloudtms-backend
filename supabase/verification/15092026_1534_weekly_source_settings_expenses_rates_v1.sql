\set ON_ERROR_STOP on

begin;

insert into public.tms_users(id,email,password_hash,role,payment_authoriser,payment_golden_key)
values ('90000000-0000-4000-8000-000000000001','weekly-source-policy-proof@example.invalid','proof','admin',true,true);

insert into public.clients(id,name,vat_chargeable)
values
  ('90000000-0000-4000-8000-000000000011','Policy Proof NHSP',true),
  ('90000000-0000-4000-8000-000000000012','Policy Proof Roster',true),
  ('90000000-0000-4000-8000-000000000013','Policy Proof Timesheet Authority',true);

insert into public.contracts(
  id,client_id,start_date,end_date,pay_method_snapshot,self_bill,overrideclientsettings
) values
  ('90000000-0000-4000-8000-000000000021','90000000-0000-4000-8000-000000000012','2026-01-01','2026-12-31','PAYE',true,false),
  ('90000000-0000-4000-8000-000000000022','90000000-0000-4000-8000-000000000013','2026-01-01','2026-12-31','PAYE',false,false);

insert into public.weekly_source_groups(
  id,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time,nhsp_report_heading_name
) values
  ('90000000-0000-4000-8000-000000000031','90000000-0000-4000-8000-000000000099','PROOF_NHSP','Proof NHSP','NHSP',3,'15:00','Proof Agency'),
  ('90000000-0000-4000-8000-000000000032','90000000-0000-4000-8000-000000000099','PROOF_ROSTER','Proof Roster','ROSTER',3,'15:00',null);

insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('90000000-0000-4000-8000-000000000031','90000000-0000-4000-8000-000000000011','2026-01-01','90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000032','90000000-0000-4000-8000-000000000012','2026-01-01','90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000032','90000000-0000-4000-8000-000000000013','2026-01-01','90000000-0000-4000-8000-000000000001');

insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,source_fixed_expenses_enabled,source_expense_vat_enabled,
  weekly_rate_classification_method,duration_break_tie_rule,
  candidate_queries_enabled,manager_queries_enabled,
  created_by_user_id
) values
  ('90000000-0000-4000-8000-000000000031','90000000-0000-4000-8000-000000000011','2026-01-01',
   'SOURCE_AUTHORITY','IMPORT_ONLY',true,false,false,'SPLIT_RATE_WINDOWS','EARLIEST_LONGEST_PORTION',true,true,
   '90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000032','90000000-0000-4000-8000-000000000012','2026-01-01',
   'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'WHOLE_SHIFT_START_DAY',null,true,true,
   '90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000032','90000000-0000-4000-8000-000000000013','2026-01-01',
   'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,false,true,'SPLIT_RATE_WINDOWS','EARLIEST_LONGEST_PORTION',false,true,
   '90000000-0000-4000-8000-000000000001');

insert into public.weekly_source_contract_policies(
  contract_id,effective_from,source_fixed_expenses_enabled_override,
  source_expense_vat_enabled_override,created_by_user_id
) values (
  '90000000-0000-4000-8000-000000000021','2026-01-01',false,true,
  '90000000-0000-4000-8000-000000000001'
);

do $verify_weekly_source_policy$
declare
  v_nhsp jsonb;
  v_roster jsonb;
  v_timesheet jsonb;
begin
  if (select count(*) from public.weekly_source_format_profiles where active)<>5 then
    raise exception 'expected five active release-controlled source profiles';
  end if;
  if exists(
    select 1 from public.weekly_source_format_profiles
    where profile_code ilike '%MAGNIT%' or profile_json::text ilike '%MAGNIT%'
  ) then
    raise exception 'a client-specific Magnit identity leaked into the source profile registry';
  end if;
  if exists(
    select 1 from public.weekly_source_format_profiles
    where profile_sha256 is distinct from extensions.digest(
      pg_catalog.convert_to(profile_json::text,'UTF8'),'sha256'
    )
  ) then
    raise exception 'source profile hash mismatch';
  end if;

  v_nhsp:=private._weekly_source_effective_policy_v1(
    '90000000-0000-4000-8000-000000000011',null,'2026-09-01'
  );
  v_roster:=private._weekly_source_effective_policy_v1(
    '90000000-0000-4000-8000-000000000012',
    '90000000-0000-4000-8000-000000000021','2026-09-01'
  );
  v_timesheet:=private._weekly_source_effective_policy_v1(
    '90000000-0000-4000-8000-000000000013',
    '90000000-0000-4000-8000-000000000022','2026-09-01'
  );

  if v_nhsp->>'c1_source_mode' is distinct from 'NHSP_WEEKLY' then
    raise exception 'NHSP C1 normalisation failed: %',v_nhsp;
  end if;
  if v_roster->>'c1_source_mode' is distinct from 'HEALTHROSTER_WEEKLY' then
    raise exception 'Roster C1 normalisation failed: %',v_roster;
  end if;
  if (v_roster->>'source_fixed_expenses_enabled')::boolean is not false
     or (v_roster->>'source_expense_vat_enabled')::boolean is not false then
    raise exception 'disabled Contract source-expense override did not also disable VAT: %',v_roster;
  end if;
  if v_timesheet->>'c1_source_mode' is not null
     or v_timesheet->>'authority_mode' is distinct from 'TIMESHEET_AUTHORITY'
     or (v_timesheet->>'self_bill_enabled')::boolean is not false then
    raise exception 'Timesheet-authority policy leaked into C1: %',v_timesheet;
  end if;
  if nullif(v_nhsp->>'policy_sha256','') is null
     or nullif(v_roster->>'policy_sha256','') is null then
    raise exception 'effective policy fingerprint missing';
  end if;
end;
$verify_weekly_source_policy$;

rollback;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_settings_expenses_rates_v1',
  'profile_count',5,
  'roster_c1_source_mode','HEALTHROSTER_WEEKLY',
  'client_specific_banking_mode',false
) as result;
