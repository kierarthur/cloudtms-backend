-- H2 F-013 isolated post-scope proof for the six production-visible finance
-- categories. The active proof uses the unchanged current context collector,
-- canonical producer, certified Workbench publisher, VALIDATE_SESSION/prepare,
-- the provisional 02092026_1030 scope owner and every unchanged downstream
-- Draft owner. It runs one independent Candidate/variant per transaction and
-- stops at the first downstream error. It is not release evidence: F-010a must
-- still be combined into one final scope owner before any publication. The
-- immutable 01092026_2313 pre-change fixture remains the red authority.
-- Synthetic rows are rolled back; provider, settlement and remittance owners
-- are never called. No economic, PAYE, Umbrella, VAT or routing rule changes.
\set ON_ERROR_STOP on
begin;
set local jit=off;
set local statement_timeout='15s';
set local lock_timeout='1500ms';
set local idle_in_transaction_session_timeout='30s';
set local cloudtms.rollback_fixture_scope='BANKING_PAY_DRAFT_FINANCE_CATEGORY_END_TO_END_V1';

do $local_only$
begin
  if current_database()<>'banking_modal_v2_test' then
    raise exception 'LOCAL_FIXTURE_ONLY';
  end if;
  if exists (
       select 1 from public.pay_batches
       where id::text like '41000000-0000-4000-8000-%'
     )
     or exists (
       select 1 from public.banking_pay_operations
       where id::text like '41000000-0000-4000-8000-%'
          or idempotency_key like 'h2-f013-e2e-%'
     )
     or exists (
       select 1 from public.pay_advances
       where id::text like '41000000-0000-4000-8000-%'
     ) then
    raise exception 'ROLLBACK_FIXTURE_ID_COLLISION';
  end if;
end;
$local_only$;

\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

-- Match the exact current Miget TEST semantic-publication switches.  The
-- disposable baseline defaults all three off; the read-only TEST identity
-- check on 2026-09-02 proved all three are on.  This update is inside the
-- outer rollback and changes no shared or hosted state.
update public.settings_defaults
set banking_pay_workbench_semantic_ready_observe_v2_enabled=true,
    banking_pay_workbench_semantic_ready_publication_v3_enabled=true,
    banking_pay_selection_intent_identity_v1_enabled=true
where id=1;

update public.candidates
set pay_method='PAYE',
    account_holder='H2 finance PAYE candidate',
    sort_code='010203',
    account_number='12345678',
    bank_details_hash='h2-f013-e2e-paye-bank'
where id='10000000-0000-4000-8000-000000000002';

insert into public.umbrellas(
  id,name,remittance_email,bank_name,sort_code,account_number,
  vat_chargeable,enabled,bank_details_hash
) values (
  '41000000-0000-4000-8000-000000000010','H2 finance Umbrella',
  'h2-finance-umbrella@example.invalid','H2 Test Bank','112233','87654321',
  true,true,'h2-f013-e2e-umbrella-bank'
);

insert into public.candidates(
  id,tms_ref,display_name,pay_method,umbrella_id,active,
  account_holder,sort_code,account_number,bank_details_hash
) values (
  '41000000-0000-4000-8000-000000000002','H2-F013-UMB',
  'H2 finance Umbrella candidate','UMBRELLA',
  '41000000-0000-4000-8000-000000000010',true,
  'H2 finance Umbrella candidate','445566','11223344',
  'h2-f013-e2e-umbrella-candidate-bank'
);

insert into public.settings_finance_windows(
  id,date_from,date_to,vat_rate_pct,erni_pct,holiday_pay_pct,
  apply_holiday_to,apply_erni_to,margin_includes
) values (
  '41000000-0000-4000-8000-000000000020','2026-01-01','2026-12-31',
  20,0,0,'NONE','NONE','[]'::jsonb
);

create temporary table pg_temp.h2_f013_e2e_categories(
  category_order integer primary key,
  visible_alias text not null unique,
  case_type public.pay_finance_case_type_enum not null,
  advance_kind public.pay_advance_kind_enum not null,
  direction_sign integer not null check(direction_sign in (-1,1)),
  expected_final_item_type text not null,
  supports_taxable boolean not null
) on commit drop;

insert into pg_temp.h2_f013_e2e_categories values
  (1,'OVERPAYMENT_RECOVERY','OVERPAYMENT','OVERPAYMENT',-1,'OVERPAYMENT_RECOVERY',true),
  (2,'MANUAL_DEBT_RECOVERY','MANUAL_DEBT_ADJUSTMENT','LEGACY_ADVANCE',-1,'MANUAL_DEBT_RECOVERY',true),
  (3,'PAYMENT_ADVANCE_REPAYMENT','PAYMENT_ADVANCE','LOAN',-1,'LOAN_REPAYMENT',false),
  (4,'LOAN_PAYOUT','PAYMENT_ADVANCE','LOAN',1,'LOAN_PAYOUT',false),
  (5,'UNDERPAYMENT_PAYMENT','UNDERPAYMENT','UNDERPAYMENT',1,'UNDERPAYMENT_PAYMENT',true),
  (6,'MANUAL_CREDIT_ADJUSTMENT_PAYMENT','MANUAL_CREDIT_ADJUSTMENT','LEGACY_ADVANCE',1,'MANUAL_CREDIT_PAYOUT',true);

create temporary table pg_temp.h2_f013_e2e_variants on commit drop as
with variant_rows as (
  select
    category_row.*,
    channel_row.pay_channel,
    tax_row.taxability
  from pg_temp.h2_f013_e2e_categories as category_row
  cross join (values ('PAYE'::text),('UMBRELLA'::text)) as channel_row(pay_channel)
  cross join (values ('NON_TAXABLE'::text),('TAXABLE'::text)) as tax_row(taxability)
  where tax_row.taxability='NON_TAXABLE' or category_row.supports_taxable
)
select
  row_number() over(order by category_order,taxability,pay_channel)::integer as ordinal,
  variant_rows.*,
  case when pay_channel='PAYE'
       then '10000000-0000-4000-8000-000000000002'::uuid
       else '41000000-0000-4000-8000-000000000002'::uuid end as candidate_id,
  case when taxability='TAXABLE'
       then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
       when direction_sign>0
       then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
       else 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum end as classification,
  case when pay_channel='UMBRELLA' then 'NONE'
       when taxability='TAXABLE' and direction_sign>0 then 'GROSS_ADD'
       when taxability='TAXABLE' and direction_sign<0 then 'GROSS_DEDUCT'
       when direction_sign>0 then 'NET_ADD'
       else 'NET_DEDUCT' end as expected_paye_treatment,
  case when pay_channel='PAYE' then 'NORMAL_PAY_ROUTE'
       when visible_alias='LOAN_PAYOUT'
         or (visible_alias='MANUAL_CREDIT_ADJUSTMENT_PAYMENT' and taxability='NON_TAXABLE')
       then 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
       else 'UMBRELLA_COMPANY' end as routing_kind,
  (direction_sign*10.00)::numeric(12,2) as expected_amount_ex_vat,
  (case when pay_channel='UMBRELLA' and taxability='TAXABLE'
        then direction_sign*2.00 else 0 end)::numeric(12,2) as expected_amount_vat,
  (case when pay_channel='UMBRELLA' and taxability='TAXABLE'
        then direction_sign*12.00 else direction_sign*10.00 end)::numeric(12,2) as expected_amount_inc_vat
from variant_rows;

do $finite_variant_contract$
begin
  if (select count(*) from pg_temp.h2_f013_e2e_variants)<>20
     or (select count(distinct visible_alias) from pg_temp.h2_f013_e2e_variants)<>6
     or exists (
       select 1 from pg_temp.h2_f013_e2e_variants
       where visible_alias in ('LOAN_REPAYMENT','MANUAL_CREDIT_PAYOUT')
     ) then
    raise exception 'H2_F013_E2E_VARIANT_CONTRACT_CHANGED';
  end if;
end;
$finite_variant_contract$;

insert into public.pay_advances(
  id,candidate_id,reason,original_amount,outstanding_amount,status,
  schedule_json,weekly_due,weeks_total,start_week_start,
  advance_kind,payout_status,case_type,taxability,routing_kind,
  oneoff_bank_details_required,adjustment_comment,created_by
)
select
  ('41000000-0000-4000-8000-'||lpad((50000+ordinal)::text,12,'0'))::uuid,
  candidate_id,
  case case_type
    when 'PAYMENT_ADVANCE' then 'LOAN'::public.pay_advance_reason_enum
    when 'OVERPAYMENT' then 'OVERPAYMENT'::public.pay_advance_reason_enum
    when 'UNDERPAYMENT' then 'UNDERPAYMENT'::public.pay_advance_reason_enum
    else 'MANUAL_ADVANCE'::public.pay_advance_reason_enum end,
  10.00,10.00,'ACTIVE',
  case when direction_sign<0 then
    jsonb_build_array(jsonb_build_object(
      'week_start','2026-08-24','amount',10.00,'status','DUE'
    )) else '[]'::jsonb end,
  case when direction_sign<0 then 10.00 else null end,
  case when direction_sign<0 then 1 else null end,
  case when direction_sign<0 then '2026-08-24'::date else null end,
  advance_kind,
  case when visible_alias='PAYMENT_ADVANCE_REPAYMENT' then 'PAID'::public.pay_advance_payout_status_enum
       when visible_alias in ('LOAN_PAYOUT','MANUAL_CREDIT_ADJUSTMENT_PAYMENT')
         then 'PENDING'::public.pay_advance_payout_status_enum
       else null end,
  case_type,taxability::public.pay_finance_taxability_enum,
  routing_kind::public.pay_finance_routing_kind_enum,
  routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT',
  'H2 F013 E2E '||visible_alias,
  '10000000-0000-4000-8000-000000000001'::uuid
from pg_temp.h2_f013_e2e_variants;

insert into public.pay_finance_case_oneoff_payout_bank_details(
  finance_case_id,candidate_id,beneficiary_name,sort_code,account_number,
  bank_details_hash,note,created_by_user_id,updated_by_user_id
)
select
  ('41000000-0000-4000-8000-'||lpad((50000+ordinal)::text,12,'0'))::uuid,
  candidate_id,'H2 one-off beneficiary','778899','55667788',
  'h2-f013-e2e-oneoff-'||ordinal,'ROLLBACK FIXTURE',
  '10000000-0000-4000-8000-000000000001'::uuid,
  '10000000-0000-4000-8000-000000000001'::uuid
from pg_temp.h2_f013_e2e_variants
where routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT';

insert into public.pay_finance_case_components(
  id,finance_case_id,candidate_id,source_family_key,
  component_key_type,component_key_value,classification,source_pay_method,
  source_basis_json,source_amount,remaining_source_amount,
  saved_target_pay_method,saved_resolution_mode,
  saved_resolution_payload_json,saved_resolution_result_json,
  resolution_fingerprint,is_resolution_stale,
  allocation_priority_group,allocation_priority_order
)
select
  ('41000000-0000-4000-8000-'||lpad((80000+ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((50000+ordinal)::text,12,'0'))::uuid,
  candidate_id,'case:41000000-0000-4000-8000-'||lpad((50000+ordinal)::text,12,'0'),
  'CASE_TOTAL','TOTAL',classification,pay_channel,
  jsonb_build_object(
    'case_type',case_type::text,'visible_alias',visible_alias,
    'taxability',taxability,'routing_kind',routing_kind,
    'paye_treatment',expected_paye_treatment
  ),10.00,10.00,
  case when classification='TAXABLE_CHANNEL_SENSITIVE' then pay_channel else null end,
  case when classification='TAXABLE_CHANNEL_SENSITIVE'
       then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum else null end,
  case when classification='TAXABLE_CHANNEL_SENSITIVE'
       then jsonb_build_object('same_channel',true,'target_amount',10.00,'target_pay_method',pay_channel)
       else null end,
  case when classification='TAXABLE_CHANNEL_SENSITIVE'
       then jsonb_build_object('target_amount',10.00,'target_pay_method',pay_channel)
       else null end,
  case when classification='TAXABLE_CHANNEL_SENSITIVE'
       then 'h2-f013-e2e-resolution-'||ordinal else null end,
  false,0,ordinal
from pg_temp.h2_f013_e2e_variants;

-- Full current-owner chain. Each variant has its own Candidate and eligible
-- Timesheet.  Run one Candidate per transaction, matching the Worker's
-- Candidate-chunk boundary and preventing this rollback fixture itself from
-- accumulating relation locks across unrelated Candidates.  The bounded
-- 20-variant matrix is executed by invoking this file once per ordinal.
create temporary table pg_temp.h2_f013_canonical_variants on commit drop as
select *
from pg_temp.h2_f013_e2e_variants
where ordinal=coalesce(
  nullif(current_setting('cloudtms.h2_f013_variant_ordinal',true),'')::integer,
  1
);

do $canonical_variant_contract$
begin
  if (select count(*) from pg_temp.h2_f013_canonical_variants)<>1 then
    raise exception 'H2_F013_CANONICAL_VARIANT_REQUIRED';
  end if;
end;
$canonical_variant_contract$;

insert into public.clients(id,cli_ref,name,vat_chargeable,payment_terms_days)
values (
  '43000000-0000-4000-8000-000000000020','H2-F013-CANONICAL',
  'H2 F013 Canonical Client',true,30
);

insert into public.candidates(
  id,tms_ref,display_name,pay_method,umbrella_id,active,
  account_holder,sort_code,account_number,bank_details_hash
)
select
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  'H2-CANON-'||variant.ordinal,'H2 Canonical '||variant.visible_alias,
  variant.pay_channel,
  case when variant.pay_channel='UMBRELLA'
       then '41000000-0000-4000-8000-000000000010'::uuid else null end,
  true,'H2 Canonical '||variant.visible_alias,'010203',
  lpad((12000000+variant.ordinal)::text,8,'0'),
  'h2-f013-canonical-bank-'||variant.ordinal
from pg_temp.h2_f013_canonical_variants as variant;

select set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
select set_config('cloudtms.banking_pay_explicit_action','false',true);
select set_config('cloudtms.banking_pay_dirty_allowed','false',true);

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
  job_title_norm,week_ending_date,sheet_scope,
  authorised_at_server,is_current,version,is_adjustment
)
select
  ('43000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  'h2-f013-canonical-'||variant.ordinal,'h2-canonical-'||variant.ordinal,
  'h2 canonical hospital','h2 canonical ward','h2 canonical role',
  '2026-08-30','DAILY',now(),true,1,false
from pg_temp.h2_f013_canonical_variants as variant;

select set_config('cloudtms.lifecycle_mutation_context','',true);

insert into public.timesheets_financials(
  id,timesheet_id,timesheet_version,basis,is_current,is_stale,
  worked_start_iso,worked_end_iso,actual_schedule_json,
  candidate_id,client_id,role,pay_method,occupant_key_norm,
  candidate_assignment,processing_status,hours_day,pay_day,charge_day,
  total_hours,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
  pay_on_hold,has_rate_issue,has_pay_channel_issue
)
select
  ('43000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  1,'SELF_REPORTED',true,false,
  '2026-08-24 08:00:00+01','2026-08-24 18:00:00+01',
  '{"date":"2026-08-24","start":"08:00","end":"18:00","break_minutes":0}'::jsonb,
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  '43000000-0000-4000-8000-000000000020','h2 canonical role',
  variant.pay_channel,'h2-canonical-'||variant.ordinal,
  'ASSIGNED','READY_FOR_INVOICE',10,10,15,10,100,150,50,
  false,false,false
from pg_temp.h2_f013_canonical_variants as variant;

insert into public.pay_advances(
  id,candidate_id,reason,original_amount,outstanding_amount,status,
  schedule_json,weekly_due,weeks_total,start_week_start,
  advance_kind,payout_status,case_type,taxability,routing_kind,
  oneoff_bank_details_required,adjustment_comment,created_by
)
select
  ('43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  case variant.case_type
    when 'PAYMENT_ADVANCE' then 'LOAN'::public.pay_advance_reason_enum
    when 'OVERPAYMENT' then 'OVERPAYMENT'::public.pay_advance_reason_enum
    when 'UNDERPAYMENT' then 'UNDERPAYMENT'::public.pay_advance_reason_enum
    else 'MANUAL_ADVANCE'::public.pay_advance_reason_enum end,
  10.00,10.00,'ACTIVE',
  case when variant.direction_sign<0 then jsonb_build_array(jsonb_build_object(
    'week_start','2026-08-24','amount',10.00,'status','DUE'
  )) else '[]'::jsonb end,
  case when variant.direction_sign<0 then 10.00 else null end,
  case when variant.direction_sign<0 then 1 else null end,
  case when variant.direction_sign<0 then '2026-08-24'::date else null end,
  variant.advance_kind,
  case when variant.visible_alias='PAYMENT_ADVANCE_REPAYMENT'
       then 'PAID'::public.pay_advance_payout_status_enum
       when variant.visible_alias in ('LOAN_PAYOUT','MANUAL_CREDIT_ADJUSTMENT_PAYMENT')
       then 'PENDING'::public.pay_advance_payout_status_enum else null end,
  variant.case_type,variant.taxability::public.pay_finance_taxability_enum,
  variant.routing_kind::public.pay_finance_routing_kind_enum,
  variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT',
  'H2 F013 canonical '||variant.visible_alias,
  '10000000-0000-4000-8000-000000000001'::uuid
from pg_temp.h2_f013_canonical_variants as variant;

insert into public.pay_finance_case_oneoff_payout_bank_details(
  finance_case_id,candidate_id,beneficiary_name,sort_code,account_number,
  bank_details_hash,note,created_by_user_id,updated_by_user_id
)
select
  ('43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  'H2 canonical one-off beneficiary','778899','55667788',
  'h2-f013-canonical-oneoff-'||variant.ordinal,'ROLLBACK FIXTURE',
  '10000000-0000-4000-8000-000000000001'::uuid,
  '10000000-0000-4000-8000-000000000001'::uuid
from pg_temp.h2_f013_canonical_variants as variant
where variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT';

insert into public.pay_finance_case_components(
  id,finance_case_id,candidate_id,source_family_key,
  component_key_type,component_key_value,classification,source_pay_method,
  source_basis_json,source_amount,remaining_source_amount,
  saved_target_pay_method,saved_resolution_mode,
  saved_resolution_payload_json,saved_resolution_result_json,
  resolution_fingerprint,is_resolution_stale,
  allocation_priority_group,allocation_priority_order
)
select
  ('43000000-0000-4000-8000-'||lpad((80000+variant.ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  'case:43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
  'CASE_TOTAL','TOTAL',variant.classification,variant.pay_channel,
  jsonb_build_object(
    'case_type',variant.case_type::text,'visible_alias',variant.visible_alias,
    'taxability',variant.taxability,'routing_kind',variant.routing_kind,
    'paye_treatment',variant.expected_paye_treatment
  ),10.00,10.00,
  case when variant.classification='TAXABLE_CHANNEL_SENSITIVE'
       then variant.pay_channel else null end,
  case when variant.classification='TAXABLE_CHANNEL_SENSITIVE'
       then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum else null end,
  case when variant.classification='TAXABLE_CHANNEL_SENSITIVE'
       then jsonb_build_object('same_channel',true,'target_amount',10.00,
                               'target_pay_method',variant.pay_channel) else null end,
  case when variant.classification='TAXABLE_CHANNEL_SENSITIVE'
       then jsonb_build_object('target_amount',10.00,
                               'target_pay_method',variant.pay_channel) else null end,
  case when variant.classification='TAXABLE_CHANNEL_SENSITIVE'
       then 'h2-f013-canonical-resolution-'||variant.ordinal else null end,
  false,0,variant.ordinal
from pg_temp.h2_f013_canonical_variants as variant;

set constraints all immediate;

-- The fixed setup rows above are one logical request.  Production invokes the
-- subsequent Draft phases through separate PostgREST transactions, so the
-- rollback fixture must clear the transaction-local scope token after forcing
-- the deferred scope finaliser.  This is the repository's established
-- rollback-verifier boundary simulation; it neither suppresses the trigger nor
-- changes its generation semantics.
select set_config('cloudtms.scope_generation_finalising','false',true);
select set_config('cloudtms.banking_pay_scope_tx_token','',true);
set constraints trg_pay_workbench_scope_change_finalize_v1 deferred;

create temporary table pg_temp.h2_f013_canonical_lines(
  ordinal integer not null,
  candidate_id uuid not null,
  line_json jsonb not null
) on commit drop;

do $canonical_build$
declare
  variant record;
  context_json jsonb;
  collect_result jsonb;
  build_result jsonb;
begin
  for variant in select * from pg_temp.h2_f013_canonical_variants order by ordinal loop
    context_json:=public.pay_preview_build_context(
      p_pay_date=>'2026-08-28',
      p_week_ending_cutoff=>'2026-08-30',
      p_actor_user_id=>'10000000-0000-4000-8000-000000000001',
      p_candidate_id=>(
        '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
      )::uuid,
      p_client_id=>null,
      p_preview_decisions_json=>'{}'::jsonb
    )||jsonb_build_object(
      'pay_channel_scope',variant.pay_channel,
      'force_include_timesheet_ids','[]'::jsonb,
      'exclude_timesheet_ids','[]'::jsonb
    );
    collect_result:=public.pay_preview_candidate_collect_scope(
      context_json,
      ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
      null,100
    );
    if coalesce((collect_result#>>'{source_build_collect_diagnostics,has_more}')::boolean,true)
       is true then
      raise exception 'H2_F013_CANONICAL_SCOPE_COLLECTION_INCOMPLETE:%:%',
        variant.ordinal,collect_result;
    end if;
    perform public.pay_preview_candidate_build_case_component_rows(
      context_json,
      ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid
    );
    build_result:=public.pay_preview_candidate_build_canonical_lines(
      context_json,
      ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid
    );
    if build_result#>>'{source_build_canonical_diagnostics,source_rows_seen}'<>'1'
       or build_result->>'ready_preview_line_count'<>'2'
       or build_result->>'blocked_preview_line_count'<>'0' then
      raise exception 'H2_F013_CANONICAL_BUILD_NOT_READY:%:%',
        variant.ordinal,build_result-'hidden_recovery_template_lines';
    end if;
    insert into pg_temp.h2_f013_canonical_lines(ordinal,candidate_id,line_json)
    select variant.ordinal,candidate_id,line_json
    from pg_temp.canonical_preview_lines;
  end loop;
end;
$canonical_build$;

do $canonical_shape$
declare
  observed_shape jsonb;
begin
  select jsonb_agg(jsonb_build_object(
    'line_type',line.line_json->>'line_type',
    'draftable',line.line_json->>'draftable',
    'selection_allowed',line.line_json->>'selection_allowed',
    'timesheet_id',line.line_json->>'timesheet_id',
    'finance_case_id',line.line_json->>'finance_case_id',
    'key_type',coalesce(
      line.line_json#>>'{economic_key,key_type}',
      line.line_json->>'component_key_type'
    ),
    'key_value',coalesce(
      line.line_json#>>'{economic_key,key_value}',
      line.line_json->>'component_key_value'
    ),
    'amount_ex_vat',line.line_json->>'amount_ex_vat'
  ) order by line.ordinal,line.line_json->>'line_type',line.line_json::text)
  into observed_shape
  from pg_temp.h2_f013_canonical_lines as line;
  raise notice 'H2_F013_CANONICAL_PRODUCER_ROWS=%',observed_shape;

  if (select count(*) from pg_temp.h2_f013_canonical_lines)<>3
     or exists(
       select 1
       from pg_temp.h2_f013_canonical_variants as variant
       cross join lateral (
         select count(*) as line_count,
                count(*) filter(
                  where line.line_json->>'line_type'='TIMESHEET_PAYMENT'
                    and nullif(coalesce(
                      line.line_json#>>'{economic_key,key_type}',
                      line.line_json->>'component_key_type'
                    ),'') is null
                    and coalesce((line.line_json->>'draftable')::boolean,false)=false
                    and coalesce((line.line_json->>'selection_allowed')::boolean,false)=false
                ) as timesheet_context_count,
                count(*) filter(
                  where line.line_json->>'line_type'='TIMESHEET_PAYMENT'
                    and coalesce(
                      line.line_json#>>'{economic_key,key_type}',
                      line.line_json->>'component_key_type'
                    )='TS_DAY'
                    and coalesce((line.line_json->>'selection_allowed')::boolean,false)=true
                ) as timesheet_constituent_count,
                count(*) filter(
                  where line.line_json->>'line_type'=variant.visible_alias
                    and coalesce((line.line_json->>'selection_allowed')::boolean,false)=true
                ) as finance_count
         from pg_temp.h2_f013_canonical_lines as line
         where line.ordinal=variant.ordinal
       ) as counts
       where counts.line_count<>3
          or counts.timesheet_context_count<>1
          or counts.timesheet_constituent_count<>1
          or counts.finance_count<>1
     )
     or exists(
       select 1 from pg_temp.h2_f013_canonical_lines
       where line_json->>'line_type' in ('LOAN_REPAYMENT','MANUAL_CREDIT_PAYOUT')
     ) then
    raise exception 'H2_F013_CANONICAL_PRODUCER_SHAPE_CHANGED:%',observed_shape;
  end if;
end;
$canonical_shape$;

insert into public.banking_pay_workbench_sessions(
  id,actor_user_id,pay_date,week_ending_cutoff,filters_json,session_signature,
  source_snapshot_run_id,status,version,server_selected_preview_row_ids,
  server_selected_preview_row_ids_provided,scope_seed_complete,scope_total_count,
  scope_seeded_count,scope_ready_count,scope_pending_count,scope_failed_count,
  line_units_total,line_units_ready,line_units_pending,line_units_failed,
  preview_row_count,selected_row_count,progress_state,progress_counter_version,
  scope_candidate_ids,scope_change_generation_target,scope_change_generation_applied,
  scope_change_generation_shadow_checked
)
select
  '43000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-30','{}',
  'h2-f013-canonical','10000000-0000-4000-8000-000000000004',
  'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,3,2,'READY',1,
  array_agg(
    ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid
    order by variant.ordinal
  ),generation_value,generation_value,generation_value
from pg_temp.h2_f013_canonical_variants as variant
cross join lateral (
  select public.pay_workbench_scope_current_generation_v1() as generation_value
) as generation
group by generation_value;

insert into public.banking_pay_workbench_session_candidate_state(
  session_id,candidate_id,status,source_change_seq,session_version
)
select
  '43000000-0000-4000-8000-000000000005',
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  'READY',coalesce(change_counter.seq,0),1
from pg_temp.h2_f013_canonical_variants as variant
left join public.app_change_counters as change_counter
  on change_counter.entity_key=
    'pay_candidate:43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0');

insert into public.banking_pay_workbench_session_scope(
  session_id,candidate_id,scope_ordinal,status,seeded,dirty,
  certified_preview_publication_attestation_json
)
select
  '43000000-0000-4000-8000-000000000005',
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  variant.ordinal,'READY',true,false,
  '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
from pg_temp.h2_f013_canonical_variants as variant;

create temporary table pg_temp.h2_f013_canonical_preview_seed on commit drop as
select
  row_number() over(
    order by canonical.ordinal,
      case
        when canonical.line_json->>'line_type'='TIMESHEET_PAYMENT'
         and nullif(coalesce(
           canonical.line_json#>>'{economic_key,key_type}',
           canonical.line_json->>'component_key_type'
         ),'') is null then 1
        when canonical.line_json->>'line_type'='TIMESHEET_PAYMENT' then 2
        else 3
      end,
      coalesce(canonical.line_json->>'line_id',canonical.line_json->>'preview_row_id','')
  )::integer as stable_ordinal,
  canonical.*
from pg_temp.h2_f013_canonical_lines as canonical;

create temporary table pg_temp.h2_f013_canonical_resolved_seed on commit drop as
select seed.*,
       case
         when seed.line_json->>'line_type' in ('TIMESHEET_PAYMENT','MANUAL_ADJUSTMENT_CARRY_FORWARD')
           then resolved.economic_key_json
         else jsonb_strip_nulls(coalesce(seed.line_json->'economic_key','{}'::jsonb))
       end as economic_key_json
from pg_temp.h2_f013_canonical_preview_seed as seed
cross join lateral (
  select public.pay_workbench_preview_line_economic_key(
    p_line_json=>seed.line_json,
    p_timesheet_id=>case
      when coalesce(seed.line_json->>'timesheet_id','')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then (seed.line_json->>'timesheet_id')::uuid else null end,
    p_item_type=>seed.line_json->>'line_type',
    p_segment_json=>'{}'::jsonb,
    p_key_type_hint=>coalesce(
      seed.line_json#>>'{economic_key,key_type}',seed.line_json->>'component_key_type'
    ),
    p_key_value_hint=>coalesce(
      seed.line_json#>>'{economic_key,key_value}',seed.line_json->>'component_key_value'
    )
  ) as economic_key_json
) as resolved;

do $canonical_key_resolution$
declare
  unresolved jsonb;
begin
  select jsonb_agg(jsonb_build_object(
    'ordinal',seed.ordinal,
    'line_type',seed.line_json->>'line_type',
    'timesheet_id',seed.line_json->>'timesheet_id',
    'key_type_hint',coalesce(seed.line_json#>>'{economic_key,key_type}',seed.line_json->>'component_key_type'),
    'key_value_hint',coalesce(seed.line_json#>>'{economic_key,key_value}',seed.line_json->>'component_key_value'),
    'resolution',seed.economic_key_json
  ) order by seed.stable_ordinal)
  into unresolved
  from pg_temp.h2_f013_canonical_resolved_seed as seed
  where (
      seed.line_json->>'line_type'='TIMESHEET_PAYMENT'
      and coalesce((seed.economic_key_json->>'ok')::boolean,false) is not true
    ) or (
      seed.line_json->>'line_type'='MANUAL_ADJUSTMENT_CARRY_FORWARD'
      and (
        coalesce((seed.economic_key_json->>'ok')::boolean,false) is not true
        or seed.economic_key_json->>'key_type'<>'MANUAL_CARRY_FORWARD'
        or seed.economic_key_json->>'key_value'
             is distinct from seed.line_json->>'manual_adjustment_carry_forward_id'
      )
    ) or (
      seed.line_json->>'line_type' not in ('TIMESHEET_PAYMENT','MANUAL_ADJUSTMENT_CARRY_FORWARD')
      and (
        nullif(btrim(coalesce(seed.line_json->>'finance_case_id','')),'') is null
        or nullif(btrim(coalesce(seed.economic_key_json->>'key_type','')),'') is null
        or nullif(btrim(coalesce(seed.economic_key_json->>'key_value','')),'') is null
      )
    );
  if unresolved is not null then
    raise exception 'H2_F013_CANONICAL_ECONOMIC_KEY_RESOLUTION_INCOMPLETE:%',unresolved;
  end if;
end;
$canonical_key_resolution$;

-- Reproduce the current source-build -> certified-publication handoff instead
-- of inserting public preview rows directly.  The source row projection below
-- is deliberately limited to the identity/contract fields copied by the
-- current source-build owner; all finance economics still originate in the
-- canonical builder above and the existing economic-key/contract helpers.
create temporary table pg_temp.h2_f013_certified_source_seed on commit drop as
select
  seed.*,
  row_number() over(
    partition by seed.ordinal
    order by case
               when seed.line_json->>'line_type'='TIMESHEET_PAYMENT'
                and nullif(coalesce(
                  seed.line_json#>>'{economic_key,key_type}',
                  seed.line_json->>'component_key_type'
                ),'') is null then 1
               when seed.line_json->>'line_type'='TIMESHEET_PAYMENT' then 2
               else 3
             end,
             seed.stable_ordinal
  )::bigint as candidate_source_ordinal,
  coalesce(seed.line_json->>'preview_row_id',seed.line_json->>'line_id',
           'h2-f013-canonical-'||seed.stable_ordinal) as source_line_key,
  public.pay_workbench_preview_section_from_line_json(seed.line_json) as target_section
from pg_temp.h2_f013_canonical_resolved_seed as seed;

insert into public.banking_pay_workbench_candidate_source_lines(
  id,session_id,candidate_id,session_version,source_change_seq,
  source_build_run_id,source_publication_id,source_ordinal,line_key,
  parent_line_key,split_suffix,timesheet_id,section,source_row_json,
  economic_key_json,contract_json,pay_channel_scope,refresh_scope_kind,status
)
select
  ('44000000-0000-4000-8000-'||lpad((70000+seed.stable_ordinal)::text,12,'0'))::uuid,
  '43000000-0000-4000-8000-000000000005',seed.candidate_id,1,
  candidate_state.source_change_seq,
  ('44000000-0000-4000-8000-'||lpad((30000+seed.ordinal)::text,12,'0'))::uuid,
  private.pay_workbench_source_publication_identity_v1(
    '43000000-0000-4000-8000-000000000005',seed.candidate_id,1,
    candidate_state.source_change_seq,
    ('44000000-0000-4000-8000-'||lpad((30000+seed.ordinal)::text,12,'0'))::uuid
  ),
  seed.candidate_source_ordinal,seed.source_line_key,seed.source_line_key,
  case when seed.line_json->>'line_type'='TIMESHEET_PAYMENT'
       then 'timesheet_total' else 'component:'||coalesce(
         seed.line_json#>>'{case_components,0,finance_component_id}',seed.ordinal::text
       ) end,
  case when coalesce(seed.economic_key_json->>'timesheet_id','')
                  ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       then (seed.economic_key_json->>'timesheet_id')::uuid else null end,
  seed.target_section,
  jsonb_strip_nulls(
    seed.line_json||jsonb_build_object(
      'source_function','pay_workbench_candidate_source_build_chunk',
      'source_kind','VALID_PREVIEW_LINE',
      'session_id','43000000-0000-4000-8000-000000000005',
      'session_version',1,'candidate_id',seed.candidate_id::text,
      'source_build_run_id',
        '44000000-0000-4000-8000-'||lpad((30000+seed.ordinal)::text,12,'0'),
      'source_change_seq',candidate_state.source_change_seq,
      'source_ordinal',seed.candidate_source_ordinal,
      'line_key',seed.source_line_key,'line_ordinal',seed.candidate_source_ordinal,
      'target_section',seed.target_section,'section',seed.target_section,
      'economic_key',seed.economic_key_json,
      'preview_contract',public.pay_workbench_preview_line_contract_ok(
        seed.line_json,seed.economic_key_json,seed.target_section
      ),
      'dependency_family_kind',case when seed.line_json->>'finance_case_id' is not null
        then 'FINANCE_CASE' else 'TIMESHEET' end,
      'dependency_family_key',case when seed.line_json->>'finance_case_id' is not null
        then 'finance:'||(seed.line_json->>'finance_case_id')
        else 'timesheet:'||(seed.economic_key_json->>'timesheet_id') end,
      'refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'requested_refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'actual_refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'pay_channel_scope',variant.pay_channel,
      'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
    )
  ),
  seed.economic_key_json,
  public.pay_workbench_preview_line_contract_ok(
    seed.line_json,seed.economic_key_json,seed.target_section
  )||jsonb_build_object('policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'),
  variant.pay_channel,'CANDIDATE_FULL_LIVE','CURRENT'
from pg_temp.h2_f013_certified_source_seed as seed
join pg_temp.h2_f013_canonical_variants as variant using(ordinal)
join public.banking_pay_workbench_session_candidate_state as candidate_state
  on candidate_state.session_id='43000000-0000-4000-8000-000000000005'
 and candidate_state.candidate_id=seed.candidate_id;

insert into private.banking_pay_workbench_economic_builds(
  id,build_token,candidate_id,session_id,session_version,source_snapshot_run_id,
  source_build_run_id,captured_candidate_generation,source_change_seq,status,
  private_stage,scope_cursor_json,closure_cursor_json,seed_scope_count,
  seed_scope_digest,seed_scope_sealed_at_utc,scope_count,dependency_node_count,
  dependency_edge_count,dependency_edge_stream_digest,
  dependency_edge_stream_terminal_key_json,dependency_edge_stream_complete,
  tagged_edge_count,edge_tag_stream_terminal_key_json,edge_tag_digest,
  edge_tag_stream_complete,unit_count,unit_digest,row_seal_count,
  last_stable_ordinal,sealed_fingerprint_digest,fact_count,canonical_count,
  scope_digest,dependency_digest,canonical_digest,attestation_json,
  dependency_closure_sealed_at_utc,created_at_utc,updated_at_utc,
  ready_at_utc,reconciled_at_utc,completed_at_utc
)
select
  ('44000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  ('44000000-0000-4000-8000-'||lpad((40000+variant.ordinal)::text,12,'0'))::uuid,
  source_row.candidate_id,'43000000-0000-4000-8000-000000000005',1,
  '10000000-0000-4000-8000-000000000004',
  ('44000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
  0,source_row.source_change_seq,'COMPLETE','COMPLETE',
  '{"terminal":true}'::jsonb,'{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
  0,md5('h2-f013-seed-'||variant.ordinal),clock_timestamp(),0,0,0,
  md5('h2-f013-edge-stream-'||variant.ordinal),'{}'::jsonb,true,0,'{}'::jsonb,
  md5('h2-f013-edge-tag-'||variant.ordinal),true,0,
  md5('h2-f013-unit-'||variant.ordinal),0,0,
  md5('h2-f013-seal-'||variant.ordinal),0,count(*)::integer,
  md5('h2-f013-scope-'||variant.ordinal),md5('h2-f013-dependency-'||variant.ordinal),
  md5(string_agg(md5(source_row.source_row_json::text),'' order by source_row.source_ordinal)),
  jsonb_build_object(
    'effect_plan_sealed',true,
    'effect_plan_digest',md5('h2-f013-effect-plan-'||variant.ordinal),
    'observed_finance_effect_digest',md5('h2-f013-observed-effect-'||variant.ordinal)
  ),
  statement_timestamp(),statement_timestamp(),statement_timestamp(),
  statement_timestamp(),statement_timestamp(),statement_timestamp()
from pg_temp.h2_f013_canonical_variants as variant
join public.banking_pay_workbench_candidate_source_lines as source_row
  on source_row.session_id='43000000-0000-4000-8000-000000000005'
 and source_row.candidate_id=(
   '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
 )::uuid
group by variant.ordinal,source_row.candidate_id,source_row.source_change_seq;

insert into public.banking_pay_workbench_jobs(
  id,job_type,status,dedupe_key,snapshot_run_id,session_id,candidate_id,
  payload_json,completed_at_utc,economic_build_id,private_stage,
  private_cursor_kind,private_cursor_json,private_stage_version
)
select
  ('44000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  'WORKBENCH_CANDIDATE_SOURCE_BUILD','SUCCEEDED','h2-f013-publish-'||variant.ordinal,
  '10000000-0000-4000-8000-000000000004',
  '43000000-0000-4000-8000-000000000005',
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  jsonb_build_object('source_change_seq',candidate_state.source_change_seq),
  clock_timestamp(),
  ('44000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  'COMPLETE','COMPLETE','{"terminal":true}'::jsonb,1
from pg_temp.h2_f013_canonical_variants as variant
join public.banking_pay_workbench_session_candidate_state as candidate_state
  on candidate_state.session_id='43000000-0000-4000-8000-000000000005'
 and candidate_state.candidate_id=(
   '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
 )::uuid;

update private.banking_pay_workbench_economic_builds as build_row
set source_job_id=job_row.id
from public.banking_pay_workbench_jobs as job_row
where build_row.id=job_row.economic_build_id
  and build_row.session_id='43000000-0000-4000-8000-000000000005';

insert into private.banking_pay_workbench_candidate_scope_registry(
  candidate_id,initialisation_status,dirty_generation,evaluated_generation,
  current_source_change_seq,current_build_id,last_dirty_reason,last_evaluated_at_utc,
  initialised_at_utc,failure_json,last_dirtied_at_utc,created_at_utc,updated_at_utc
)
select
  candidate_state.candidate_id,'READY',0,0,candidate_state.source_change_seq,
  ('44000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  'H2_F013_CERTIFIED_SOURCE_EVIDENCE',statement_timestamp(),statement_timestamp(),'{}'::jsonb,
  statement_timestamp(),statement_timestamp(),statement_timestamp()
from pg_temp.h2_f013_canonical_variants as variant
join public.banking_pay_workbench_session_candidate_state as candidate_state
  on candidate_state.session_id='43000000-0000-4000-8000-000000000005'
 and candidate_state.candidate_id=(
   '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
 )::uuid
on conflict(candidate_id) do update set
  initialisation_status=excluded.initialisation_status,
  dirty_generation=excluded.dirty_generation,
  evaluated_generation=excluded.evaluated_generation,
  current_source_change_seq=excluded.current_source_change_seq,
  current_build_id=excluded.current_build_id,
  last_dirty_reason=excluded.last_dirty_reason,
  last_evaluated_at_utc=excluded.last_evaluated_at_utc,
  initialised_at_utc=excluded.initialised_at_utc,
  failure_json=excluded.failure_json,
  updated_at_utc=excluded.updated_at_utc;

do $certified_publication$
declare
  variant record;
  publication_result jsonb;
begin
  for variant in select * from pg_temp.h2_f013_canonical_variants order by ordinal loop
    publication_result:=private.pay_workbench_publish_certified_source_preview_v1(
      '43000000-0000-4000-8000-000000000005',
      ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
      ('44000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
      ('44000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
      coalesce((select seq from public.app_change_counters where entity_key=
        'pay_candidate:43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')),0),
      1,
      ('44000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
      'CANDIDATE_FULL_LIVE','[]'::jsonb,'[]'::jsonb,
      jsonb_build_object(
        'authority_kind','BOUNDED_FULL_SOURCE_BUILD',
        'invocation_kind','INITIAL_COMPLETION','contract_version',3,
        'final_state','READY','semantic_contract_version','READY_TO_PAY_SEMANTIC_V2'
      )
    );
    if coalesce((publication_result->>'parity_complete')::boolean,false) is not true
       or publication_result->>'preview_row_count'<>'3'
       or publication_result->>'selected_row_count'<>'2' then
      raise exception 'H2_F013_CERTIFIED_PUBLICATION_INCOMPLETE:%:%',
        variant.ordinal,publication_result;
    end if;
  end loop;
end;
$certified_publication$;

update public.banking_pay_workbench_sessions as session_update
set server_selected_preview_row_ids=(
  select jsonb_agg(preview_row.id::text order by preview_row.row_ordinal,preview_row.id)
  from public.banking_pay_workbench_preview_rows as preview_row
  where preview_row.session_id=session_update.id
    and preview_row.selected=true
)
where session_update.id='43000000-0000-4000-8000-000000000005';

-- Only work generated in this rollback transaction is settled. This models
-- an already-materialised READY session and leaves all pre-existing history
-- untouched.
update public.banking_pay_workbench_jobs
set status='SUCCEEDED',
    completed_at_utc=coalesce(completed_at_utc,clock_timestamp()),
    updated_at_utc=clock_timestamp()
where created_at_utc>=transaction_timestamp()
  and status in ('QUEUED','RUNNING','FAILED','DEAD');

insert into public.banking_pay_operations(
  id,operation_type,status,phase,actor_user_id,workbench_session_id,
  idempotency_key,input_json,config_json,progress_json
) values (
  '43000000-0000-4000-8000-000000000100','DRAFT_CREATE','RUNNING','VALIDATE_SESSION',
  '10000000-0000-4000-8000-000000000001',
  '43000000-0000-4000-8000-000000000005','h2-f013-canonical',
  '{"pay_date":"2026-08-28","week_start":"2026-08-24","pay_channel_scope":"ALL"}',
  '{}','{}'
);

update public.banking_pay_operations as operation_row
set input_json=operation_row.input_json||jsonb_build_object(
  'expected_workbench_progress_counter_version',session_row.progress_counter_version::text,
  'expected_workbench_selected_preview_row_ids',(
    select jsonb_agg(preview_row.id::text order by preview_row.row_ordinal,preview_row.id)
    from public.banking_pay_workbench_preview_rows as preview_row
    where preview_row.session_id=session_row.id
      and preview_row.session_version=session_row.version
      and preview_row.selected=true
      and preview_row.selection_state='SELECTED'
      and preview_row.status='READY'
  ),
  'selection_review_contract_version',1,
  'selection_reviewed_by_user_id','10000000-0000-4000-8000-000000000001'
)
from public.banking_pay_workbench_sessions as session_row
where operation_row.id='43000000-0000-4000-8000-000000000100'
  and session_row.id=operation_row.workbench_session_id;

do $canonical_validate_scope_allocate$
declare
  selected_ids jsonb;
  prepare_result jsonb;
  blocker_state jsonb;
  scope_result jsonb;
  scope_error text;
  scope_error_detail text;
  selected_finance_row jsonb;
begin
  select jsonb_agg(preview_row.id::text order by preview_row.row_ordinal,preview_row.id)
  into selected_ids
  from public.banking_pay_workbench_preview_rows as preview_row
  where preview_row.session_id='43000000-0000-4000-8000-000000000005'
    and preview_row.selected=true;

  blocker_state:=public.pay_workbench_scope_blocker_state_v1(
    '43000000-0000-4000-8000-000000000005',
    public.pay_workbench_scope_current_generation_v1(),null
  );
  if coalesce((blocker_state->>'all_clear')::boolean,false) is not true then
    raise exception 'H2_F013_CANONICAL_READY_BLOCKED:%',
      blocker_state-'active_sample'-'failure_sample';
  end if;

  prepare_result:=public.pay_workbench_prepare_draft(
    '43000000-0000-4000-8000-000000000005',
    '10000000-0000-4000-8000-000000000001',selected_ids,'ALL',
    null,false,false,null,null,
    '43000000-0000-4000-8000-000000000100',true,false
  );
  if coalesce((prepare_result->>'ok')::boolean,true) is not true then
    raise exception 'H2_F013_CANONICAL_VALIDATE_REJECTED:%',prepare_result;
  end if;

  update public.banking_pay_operations set phase='SEED_CANDIDATE_SCOPE'
  where id='43000000-0000-4000-8000-000000000100';

  -- The immutable 01092026_2313 fixture proves that the current owner rejects
  -- this exact producer-owned row. This post-change fixture requires the
  -- provisional scope owner to accept it without changing any producer field.
  begin
    select to_jsonb(scope_seed)
    into scope_result
    from public.pay_workbench_prepare_draft_scope_seed(
      '43000000-0000-4000-8000-000000000100',
      '43000000-0000-4000-8000-000000000005',
      '10000000-0000-4000-8000-000000000001',selected_ids,'ALL','{}'
    ) as scope_seed;
  exception when others then
    get stacked diagnostics
      scope_error=message_text,
      scope_error_detail=pg_exception_detail;
  end;

  if scope_error is not null then
    raise exception 'H2_F013_PROVISIONAL_SCOPE_REJECTED:%:%',
      scope_error,scope_error_detail;
  end if;

  if scope_result is null
     or (scope_result->>'candidate_scope_count')::integer<>1
     or (scope_result->>'selected_row_count')::integer<>jsonb_array_length(selected_ids)
     or (scope_result->>'timesheet_count')::integer<>1
     or (scope_result->>'finance_case_count')::integer<>1
     or (scope_result->>'pay_channel_count')::integer<>1 then
    raise exception 'H2_F013_PROVISIONAL_SCOPE_COUNTS_CHANGED:%',scope_result;
  end if;

  select to_jsonb(preview_row)||jsonb_build_object(
    'finance_case_id',preview_row.row_json->>'finance_case_id',
    'item_type',preview_row.row_json->>'line_type',
    'pay_channel',preview_row.row_json->>'pay_channel',
    'economic_key',jsonb_build_object(
      'key_type',preview_row.key_type,
      'key_value',preview_row.key_value
    )
  )
  into selected_finance_row
  from public.banking_pay_workbench_preview_rows as preview_row
  where preview_row.session_id='43000000-0000-4000-8000-000000000005'
    and preview_row.selected=true
    and nullif(preview_row.row_json->>'finance_case_id','') is not null;

  if selected_finance_row is null
     or selected_finance_row->>'status'<>'READY'
     or selected_finance_row->>'selection_state'<>'SELECTED'
     or selected_finance_row->>'timesheet_id' is not null
     or selected_finance_row#>>'{row_json,source_function}'
          <>'pay_workbench_candidate_source_build_chunk'
     or selected_finance_row#>>'{row_json,policy_x_authority_scope}'
          <>'PRE_DRAFT_LIVE_TRUTH'
     or coalesce(
          selected_finance_row#>>'{economic_key,key_type}',
          selected_finance_row->>'component_key_type'
        ) not in (
          'TS_DAY',
          'TS_TOTAL',
          'ADDITIONAL_CODE',
          'ADJUSTMENT_CODE',
          'EXPENSE_CODE',
          'MANUAL_CARRY_FORWARD',
          'CASE_TOTAL',
          'FINANCE_COMPONENT'
        ) then
    raise exception 'H2_F013_CANONICAL_FINANCE_CONSTITUENT_NOT_PROVED:%',
      selected_finance_row;
  end if;

  if not exists (
       select 1
       from public.banking_pay_operations as operation_row
       where operation_row.id='43000000-0000-4000-8000-000000000100'
         and operation_row.scope_freeze_status='FROZEN'
         and operation_row.source_scope_seed_complete=true
         and operation_row.frozen_candidate_scope_count=1
         and operation_row.frozen_selected_row_count=jsonb_array_length(selected_ids)
     )
     or (select count(*)
         from public.banking_pay_operation_candidate_scope
         where operation_id='43000000-0000-4000-8000-000000000100')<>1
     or not exists (
       select 1
       from public.banking_pay_operation_candidate_scope as scope_row
       cross join lateral jsonb_array_elements(
         scope_row.selected_canonical_preview_lines_json
       ) as selected(line_json)
       where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
         and selected.line_json->>'finance_case_id'=selected_finance_row->>'finance_case_id'
         and selected.line_json->>'line_type'=selected_finance_row->>'item_type'
         and selected.line_json->>'row_key'=selected_finance_row->>'row_key'
  ) then
    raise exception 'H2_F013_PROVISIONAL_SCOPE_FREEZE_OR_IDENTITY_CHANGED:%',
      jsonb_build_object(
        'scope_result',scope_result,
        'operation',(
          select jsonb_build_object(
            'scope_freeze_status',operation_row.scope_freeze_status,
            'source_scope_seed_complete',operation_row.source_scope_seed_complete,
            'frozen_candidate_scope_count',operation_row.frozen_candidate_scope_count,
            'frozen_selected_row_count',operation_row.frozen_selected_row_count
          )
          from public.banking_pay_operations as operation_row
          where operation_row.id='43000000-0000-4000-8000-000000000100'
        ),
        'scope_count',(
          select count(*)
          from public.banking_pay_operation_candidate_scope
          where operation_id='43000000-0000-4000-8000-000000000100'
        ),
        'selected_finance_lines',(
          select jsonb_agg(jsonb_build_object(
            'finance_case_id',selected.line_json->>'finance_case_id',
            'line_type',selected.line_json->>'line_type',
            'row_key',selected.line_json->>'row_key'
          ))
          from public.banking_pay_operation_candidate_scope as scope_row
          cross join lateral jsonb_array_elements(
            scope_row.selected_canonical_preview_lines_json
          ) as selected(line_json)
          where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
            and nullif(selected.line_json->>'finance_case_id','') is not null
        ),
        'expected_finance_case_id',selected_finance_row->>'finance_case_id',
        'expected_line_type',selected_finance_row->>'item_type',
        'expected_row_key',selected_finance_row->>'row_key'
      );
  end if;

  raise notice 'H2_F013_PROVISIONAL_SCOPE_PASS=%',jsonb_build_object(
    'variant_ordinal',current_setting('cloudtms.h2_f013_variant_ordinal',true),
    'visible_alias',selected_finance_row->>'item_type',
    'pay_channel',selected_finance_row->>'pay_channel',
    'economic_key',selected_finance_row->'economic_key',
    'scope_result',scope_result,
    'frozen',true
  );
end;
$canonical_validate_scope_allocate$;

do $canonical_allocation_seed$
declare
  scope_ids jsonb;
  first_result jsonb;
  replay_result jsonb;
begin
  update public.banking_pay_operations set phase='SEED_ALLOCATION_ROWS'
  where id='43000000-0000-4000-8000-000000000100';

  select jsonb_agg(scope_row.id::text order by scope_row.chunk_sequence,scope_row.id)
  into scope_ids
  from public.banking_pay_operation_candidate_scope as scope_row
  where scope_row.operation_id='43000000-0000-4000-8000-000000000100';

  select to_jsonb(seed_result) into first_result
  from public.pay_workbench_prepare_draft_allocation_rows_seed(
    '43000000-0000-4000-8000-000000000100',scope_ids
  ) as seed_result;
  select to_jsonb(seed_result) into replay_result
  from public.pay_workbench_prepare_draft_allocation_rows_seed(
    '43000000-0000-4000-8000-000000000100',scope_ids
  ) as seed_result;

  if first_result is null
     or (first_result->>'candidate_scopes_processed')::integer<>1
     or (first_result->>'failures')::integer<>0
     or (first_result->>'allocation_rows_inserted')::integer
        +(first_result->>'allocation_rows_reused')::integer<=0
     or replay_result is null
     or (replay_result->>'candidate_scopes_processed')::integer<>1
     or (replay_result->>'failures')::integer<>0
     or (replay_result->>'allocation_rows_inserted')::integer<>0
     or (replay_result->>'allocation_rows_reused')::integer<=0 then
    raise exception 'H2_F013_ALLOCATION_SEED_OR_REPLAY_CHANGED:%:%',
      first_result,replay_result;
  end if;
  raise notice 'H2_F013_ALLOCATION_SEED_PASS=%',jsonb_build_object(
    'first',first_result,'replay',replay_result
  );
end;
$canonical_allocation_seed$;

do $canonical_allocation_row_evidence$
declare
  allocation_rows jsonb;
begin
  select jsonb_agg(jsonb_build_object(
    'allocation_type',allocation_row.allocation_type,
    'allocated_amount',allocation_row.allocated_amount,
    'finance_case_id',allocation_row.finance_case_id,
    'finance_component_id',allocation_row.finance_component_id,
    'source_ref',allocation_row.source_ref,
    'operation_source_key',allocation_row.operation_source_key,
    'timesheet_id',allocation_row.allocation_basis_json->>'timesheet_id',
    'key_type',allocation_row.allocation_basis_json->>'key_type',
    'key_value',allocation_row.allocation_basis_json->>'key_value',
    'line_row_key',allocation_row.allocation_basis_json#>>'{line,row_key}',
    'line_source_ref',allocation_row.allocation_basis_json#>>'{line,source_ref}',
    'line_finance_case_id',allocation_row.allocation_basis_json#>>'{line,finance_case_id}',
    'line_finance_component_id',allocation_row.allocation_basis_json#>>'{line,finance_component_id}',
    'component_finance_component_id',allocation_row.allocation_basis_json#>>'{component,finance_component_id}',
    'finance_component_finance_component_id',allocation_row.allocation_basis_json#>>'{finance_component,finance_component_id}',
    'line_candidate_id',allocation_row.allocation_basis_json#>>'{line,candidate_id}',
    'line_pay_channel',allocation_row.allocation_basis_json#>>'{line,pay_channel}',
    'line_presentation_section',allocation_row.allocation_basis_json#>>'{line,presentation_section}',
    'line_draftable',allocation_row.allocation_basis_json#>>'{line,draftable}',
    'line_is_ready_for_draft',allocation_row.allocation_basis_json#>>'{line,is_ready_for_draft}',
    'line_selection_allowed',allocation_row.allocation_basis_json#>>'{line,selection_allowed}',
    'line_amount_ex_vat',allocation_row.allocation_basis_json#>>'{line,amount_ex_vat}',
    'preview_line_type',allocation_row.allocation_basis_json#>>'{preview_row,line_type}',
    'planned_item_type',allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_type}',
    'status',allocation_row.status
  ) order by allocation_row.sort_order,allocation_row.id)
  into allocation_rows
  from public.banking_pay_operation_candidate_allocation_rows as allocation_row
  where allocation_row.operation_id='43000000-0000-4000-8000-000000000100';

  if jsonb_array_length(coalesce(allocation_rows,'[]'::jsonb))<>2 then
    raise exception 'H2_F013_ALLOCATION_ROW_EVIDENCE_COUNT_CHANGED:%',allocation_rows;
  end if;
  raise notice 'H2_F013_CANONICAL_ALLOCATION_ROWS=%',allocation_rows;
end;
$canonical_allocation_row_evidence$;

\if true
update public.banking_pay_operations set phase='CREATE_BATCH_SHELLS'
where id='43000000-0000-4000-8000-000000000100';

do $canonical_batch_shell$
declare
  channel_row record;
begin
  for channel_row in
    select distinct scope_row.pay_channel
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
    order by scope_row.pay_channel
  loop
    perform * from public.pay_batch_shell_ensure_from_operation(
      '43000000-0000-4000-8000-000000000100',
      '43000000-0000-4000-8000-000000000005',
      '10000000-0000-4000-8000-000000000001',
      channel_row.pay_channel,channel_row.pay_channel,
      '{"pay_date":"2026-08-28","week_start":"2026-08-24","pay_channel_scope":"ALL"}'
    );
  end loop;
end;
$canonical_batch_shell$;

do $canonical_insert_candidates$
declare
  channel_row record;
  candidate_result jsonb;
begin
  update public.banking_pay_operations set phase='INSERT_CANDIDATES'
  where id='43000000-0000-4000-8000-000000000100';
  for channel_row in
    select scope_row.pay_batch_id,scope_row.pay_channel,
           jsonb_agg(scope_row.id::text order by scope_row.id) as scope_ids
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
    group by scope_row.pay_batch_id,scope_row.pay_channel
    order by scope_row.pay_channel
  loop
    candidate_result:=public.pay_batch_insert_candidates_from_preview(
      channel_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
      '43000000-0000-4000-8000-000000000100',channel_row.scope_ids
    );
    if coalesce((candidate_result->>'ok')::boolean,true) is not true then
      raise exception 'H2_F013_CANONICAL_INSERT_CANDIDATES_REJECTED:%',candidate_result;
    end if;
  end loop;
end;
$canonical_insert_candidates$;

insert into public.pay_batch_paye_net_inputs(
  pay_batch_candidate_id,source,net_amount,imported_at_utc
)
select batch_candidate.id,'MANUAL_ENTRY',100.00,now()
from public.pay_batch_candidates as batch_candidate
join public.pay_batches as batch_row on batch_row.id=batch_candidate.pay_batch_id
where batch_row.source_workbench_session_id='43000000-0000-4000-8000-000000000005'
  and batch_row.batch_kind_fixed='PAYE';

create temporary table pg_temp.h2_f013_canonical_observed(
  ordinal integer primary key,
  visible_alias text not null,
  pay_channel text not null,
  taxability text not null,
  terminal_stage text not null default 'INSERT_ITEMS',
  insert_result jsonb,
  insert_replay_result jsonb,
  insert_error text,
  insert_error_detail text,
  prechange_insert_error text,
  prechange_insert_error_detail text,
  certified_defer_simulated boolean not null default false,
  certified_defer_insert_result jsonb,
  finance_error text,
  finance_error_detail text,
  finalizer_error text,
  finalizer_error_detail text,
  active_item_count integer not null default 0,
  finance_item_count integer not null default 0,
  observed_finance_item_type text,
  observed_finance_amount_ex_vat numeric(12,2),
  observed_finance_amount_vat numeric(12,2),
  observed_finance_amount_inc_vat numeric(12,2),
  observed_finance_paye_treatment text,
  observed_finance_pay_channel text,
  observed_finance_umbrella_id uuid,
  observed_finance_component_id uuid,
  observed_payout_instruction_snapshot_json jsonb,
  allocation_status text,
  reservation_count integer not null default 0,
  reserved_amount numeric(12,2) not null default 0,
  case_status text,
  payout_status text
) on commit drop;

insert into pg_temp.h2_f013_canonical_observed(
  ordinal,visible_alias,pay_channel,taxability
)
select ordinal,visible_alias,pay_channel,taxability
from pg_temp.h2_f013_e2e_variants;

-- The production Worker uses banking_pay_draft_create_step_v1 for
-- INSERT_ITEMS.  Exercise that exact caller inside a rollback-only
-- subtransaction so the direct owner proof below still starts from a clean
-- INSERT_ITEMS boundary.  A response-loss replay after the step advanced the
-- operation must fail closed as phase-moved; it must not re-run or duplicate
-- the completed chunk.
do $canonical_worker_step_caller$
declare
  scope_row record;
  seeded record;
  step_result jsonb;
  response_loss_replay jsonb;
  chunk_result jsonb;
  active_item_count integer;
  finance_item_count integer;
  caught boolean:=false;
begin
  select candidate_scope.id as scope_id,candidate_scope.pay_batch_id
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  begin
    update public.settings_defaults
    set banking_pay_draft_step_rpc_v1_enabled=true
    where id=1;

    update public.banking_pay_operations
    set phase='INSERT_ITEMS',status='RUNNING',
        lease_owner=null,lease_expires_at_utc=null,
        locked_by=null,lock_expires_at_utc=null
    where id='43000000-0000-4000-8000-000000000100';

    delete from public.banking_pay_operation_chunks
    where operation_id='43000000-0000-4000-8000-000000000100'
      and phase='INSERT_ITEMS';

    select * into strict seeded
    from public.banking_pay_operation_seed_chunks(
      '43000000-0000-4000-8000-000000000100','INSERT_ITEMS',
      'CANDIDATE_SCOPE',100,jsonb_build_array(scope_row.scope_id::text)
    );
    if seeded.total_units<>1 or seeded.chunk_count<>1 then
      raise exception 'H2_F013_WORKER_STEP_CHUNK_SEED_CHANGED:%',to_jsonb(seeded);
    end if;

    step_result:=public.banking_pay_draft_create_step_v1(
      '43000000-0000-4000-8000-000000000100',
      'h2-f013-worker-step','INSERT_ITEMS',15000
    );

    select operation_chunk.result_json into strict chunk_result
    from public.banking_pay_operation_chunks as operation_chunk
    where operation_chunk.operation_id='43000000-0000-4000-8000-000000000100'
      and operation_chunk.phase='INSERT_ITEMS'
      and operation_chunk.status='COMPLETE';

    select count(*)::integer,
           count(*) filter(where item.finance_case_id is not null)::integer
    into active_item_count,finance_item_count
    from public.pay_batch_items as item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id=item.pay_batch_candidate_id
    where batch_candidate.pay_batch_id=scope_row.pay_batch_id
      and coalesce(item.is_voided,false)=false;

    if step_result->>'ok'<>'true'
       or step_result->>'handled'<>'true'
       or step_result->>'phase'<>'INSERT_ITEMS'
       or step_result->>'next_phase'<>'APPLY_FINANCE_ADJUSTMENTS'
       or step_result->>'phase_complete'<>'true'
       or chunk_result#>>'{results,0,result,ok}'<>'true'
       or (chunk_result#>>'{results,0,result,ordinary_page_allocation_row_count}')::integer<>1
       or (chunk_result#>>'{results,0,result,deferred_finance_adjustment_rows}')::integer<>1
       or (chunk_result#>>'{results,0,result,inserted_item_rows}')::integer<>1
       or (chunk_result#>>'{results,0,result,skipped_item_rows}')::integer<>1
       or (chunk_result#>>'{results,0,result,failed_item_rows}')::integer<>0
       or active_item_count<>1
       or finance_item_count<>0 then
      raise exception 'H2_F013_WORKER_STEP_CALLER_CHANGED:%:%:%:%',
        step_result,chunk_result,active_item_count,finance_item_count;
    end if;

    response_loss_replay:=public.banking_pay_draft_create_step_v1(
      '43000000-0000-4000-8000-000000000100',
      'h2-f013-worker-step','INSERT_ITEMS',15000
    );
    if response_loss_replay->>'handled'<>'false'
       or response_loss_replay->>'code'<>'DRAFT_CREATE_PHASE_MOVED'
       or (select count(*)
           from public.pay_batch_items as item
           join public.pay_batch_candidates as batch_candidate
             on batch_candidate.id=item.pay_batch_candidate_id
           where batch_candidate.pay_batch_id=scope_row.pay_batch_id
             and coalesce(item.is_voided,false)=false)<>1 then
      raise exception 'H2_F013_WORKER_STEP_RESPONSE_LOSS_REPLAY_CHANGED:%',
        response_loss_replay;
    end if;

    raise exception 'H2_F013_WORKER_STEP_ROLLBACK_SENTINEL';
  exception when sqlstate 'P0001' then
    if sqlerrm<>'H2_F013_WORKER_STEP_ROLLBACK_SENTINEL' then raise; end if;
    caught:=true;
  end;

  if caught is not true
     or exists (
       select 1
       from public.banking_pay_operation_chunks as operation_chunk
       where operation_chunk.operation_id='43000000-0000-4000-8000-000000000100'
         and operation_chunk.phase='INSERT_ITEMS'
     )
     or exists (
       select 1
       from public.pay_batch_items as item
       join public.pay_batch_candidates as batch_candidate
         on batch_candidate.id=item.pay_batch_candidate_id
       where batch_candidate.pay_batch_id=scope_row.pay_batch_id
         and coalesce(item.is_voided,false)=false
     ) then
    raise exception 'H2_F013_WORKER_STEP_ROLLBACK_LEFT_PARTIAL_STATE';
  end if;
end;
$canonical_worker_step_caller$;

-- The older operation-scoped all-in-one database wrapper is not used by the
-- current Worker, but remains installed and calls INSERT_ITEMS followed by the
-- unchanged finance owner.  Prove its exact compatibility and idempotent
-- response-loss replay without treating it as the production orchestration
-- path or enabling the prohibited legacy-unchunked mode.
do $canonical_operation_wrapper_caller$
declare
  scope_row record;
  first_result jsonb;
  replay_result jsonb;
  active_item_count integer;
  finance_item_count integer;
  caught boolean:=false;
begin
  select candidate_scope.id as scope_id,candidate_scope.pay_batch_id
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  begin
    update public.banking_pay_operations
    set phase='INSERT_ITEMS',status='RUNNING'
    where id='43000000-0000-4000-8000-000000000100';

    first_result:=public.pay_build_batch_artifacts_from_preview(
      p_pay_date=>'2026-08-28',p_week_ending_cutoff=>'2026-08-30',
      p_actor_user_id=>'10000000-0000-4000-8000-000000000001',
      p_preview_payload_json=>'{}'::jsonb,p_selected_preview_row_ids=>'[]'::jsonb,
      p_source_workbench_session_id=>'43000000-0000-4000-8000-000000000005',
      p_source_snapshot_run_id=>'10000000-0000-4000-8000-000000000004',
      p_source_session_version=>1,
      p_operation_id=>'43000000-0000-4000-8000-000000000100',
      p_candidate_scope_ids=>jsonb_build_array(scope_row.scope_id::text),
      p_existing_pay_batch_id=>scope_row.pay_batch_id,
      p_allow_legacy_unchunked=>false
    );
    replay_result:=public.pay_build_batch_artifacts_from_preview(
      p_pay_date=>'2026-08-28',p_week_ending_cutoff=>'2026-08-30',
      p_actor_user_id=>'10000000-0000-4000-8000-000000000001',
      p_preview_payload_json=>'{}'::jsonb,p_selected_preview_row_ids=>'[]'::jsonb,
      p_source_workbench_session_id=>'43000000-0000-4000-8000-000000000005',
      p_source_snapshot_run_id=>'10000000-0000-4000-8000-000000000004',
      p_source_session_version=>1,
      p_operation_id=>'43000000-0000-4000-8000-000000000100',
      p_candidate_scope_ids=>jsonb_build_array(scope_row.scope_id::text),
      p_existing_pay_batch_id=>scope_row.pay_batch_id,
      p_allow_legacy_unchunked=>false
    );

    select count(*)::integer,
           count(*) filter(where item.finance_case_id is not null)::integer
    into active_item_count,finance_item_count
    from public.pay_batch_items as item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id=item.pay_batch_candidate_id
    where batch_candidate.pay_batch_id=scope_row.pay_batch_id
      and coalesce(item.is_voided,false)=false;

    if first_result->>'ok'<>'true'
       or first_result->>'operation_mode'<>'true'
       or first_result#>>'{stages,insert_items,ok}'<>'true'
       or (first_result#>>'{stages,insert_items,ordinary_page_allocation_row_count}')::integer<>1
       or (first_result#>>'{stages,insert_items,deferred_finance_adjustment_rows}')::integer<>1
       or first_result#>>'{stages,finance_adjustments,ok}'<>'true'
       or replay_result->>'ok'<>'true'
       or (replay_result#>>'{stages,insert_items,inserted_item_rows}')::integer<>0
       or (replay_result#>>'{stages,insert_items,reused_item_rows}')::integer<>2
       or (replay_result#>>'{stages,insert_items,linked_allocation_rows}')::integer<>2
       or (replay_result#>>'{stages,insert_items,deferred_finance_adjustment_rows}')::integer<>0
       or replay_result#>>'{stages,finance_adjustments,ok}'<>'true'
       or (replay_result#>>'{stages,finance_adjustments,reused_count}')::integer<>1
       or (replay_result#>>'{stages,finance_adjustments,applied_count}')::integer<>0
       or active_item_count<>2
       or finance_item_count<>1 then
      raise exception 'H2_F013_OPERATION_WRAPPER_CALLER_CHANGED:%:%:%:%',
        first_result,replay_result,active_item_count,finance_item_count;
    end if;

    raise exception 'H2_F013_OPERATION_WRAPPER_ROLLBACK_SENTINEL';
  exception when sqlstate 'P0001' then
    if sqlerrm<>'H2_F013_OPERATION_WRAPPER_ROLLBACK_SENTINEL' then raise; end if;
    caught:=true;
  end;

  if caught is not true
     or exists (
       select 1
       from public.pay_batch_items as item
       join public.pay_batch_candidates as batch_candidate
         on batch_candidate.id=item.pay_batch_candidate_id
       where batch_candidate.pay_batch_id=scope_row.pay_batch_id
         and coalesce(item.is_voided,false)=false
     ) then
    raise exception 'H2_F013_OPERATION_WRAPPER_ROLLBACK_LEFT_PARTIAL_STATE';
  end if;
end;
$canonical_operation_wrapper_caller$;

do $canonical_mixed_page_finance_does_not_hide_ordinary$
declare
  scope_row record;
  source_finance_row record;
  mixed_result jsonb;
  caught boolean:=false;
begin
  select candidate_scope.id as scope_id,candidate_scope.pay_batch_id
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  select allocation_row.*
  into strict source_finance_row
  from public.banking_pay_operation_candidate_allocation_rows as allocation_row
  where allocation_row.candidate_scope_id=scope_row.scope_id
    and allocation_row.finance_case_id is not null;

  begin
    insert into public.banking_pay_operation_candidate_allocation_rows(
      id,operation_id,candidate_scope_id,pay_batch_id,candidate_id,pay_channel,
      finance_case_id,finance_component_id,allocation_type,source_ref,
      operation_source_key,allocated_amount,allocation_basis_json,sort_order,status
    )
    select
      gen_random_uuid(),source_finance_row.operation_id,
      source_finance_row.candidate_scope_id,source_finance_row.pay_batch_id,
      source_finance_row.candidate_id,source_finance_row.pay_channel,
      source_finance_row.finance_case_id,source_finance_row.finance_component_id,
      source_finance_row.allocation_type,source_finance_row.source_ref,
      source_finance_row.operation_source_key||':page-proof:'||page_row.ordinal,
      source_finance_row.allocated_amount,source_finance_row.allocation_basis_json,
      1000+page_row.ordinal,'PENDING'
    from generate_series(1,99) as page_row(ordinal);

    update public.banking_pay_operations
    set phase='INSERT_ITEMS'
    where id='43000000-0000-4000-8000-000000000100';

    mixed_result:=public.pay_batch_insert_items_from_preview(
      scope_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
      '43000000-0000-4000-8000-000000000100',jsonb_build_array(scope_row.scope_id::text)
    );

    if mixed_result->>'ok'<>'true'
       or (mixed_result->>'allocation_row_count')::integer<>101
       or (mixed_result->>'page_allocation_row_count')::integer<>100
       or (mixed_result->>'ordinary_page_allocation_row_count')::integer<>1
       or (mixed_result->>'deferred_finance_adjustment_rows')::integer<>99
       or (mixed_result->>'inserted_item_rows')::integer<>1
       or (mixed_result->>'skipped_item_rows')::integer<>99
       or (mixed_result->>'failed_item_rows')::integer<>0
       or (mixed_result->>'has_more')::boolean then
      raise exception 'H2_F013_MIXED_PAGE_CURSOR_OR_COUNT_CHANGED:%',mixed_result;
    end if;

    raise exception 'H2_F013_MIXED_PAGE_ROLLBACK_SENTINEL';
  exception when sqlstate 'P0001' then
    if sqlerrm<>'H2_F013_MIXED_PAGE_ROLLBACK_SENTINEL' then raise; end if;
    caught:=true;
  end;

  if caught is not true
     or (select count(*) from public.banking_pay_operation_candidate_allocation_rows
         where operation_id='43000000-0000-4000-8000-000000000100')<>2
     or exists (
       select 1
       from public.pay_batch_items as item
       join public.pay_batch_candidates as candidate on candidate.id=item.pay_batch_candidate_id
       where candidate.pay_batch_id=scope_row.pay_batch_id
     ) then
    raise exception 'H2_F013_MIXED_PAGE_ROLLBACK_LEFT_PARTIAL_STATE';
  end if;
end;
$canonical_mixed_page_finance_does_not_hide_ordinary$;

do $canonical_insert_items$
declare
  scope_variant record;
  first_result jsonb;
  replay_result jsonb;
  first_error text;
  first_detail text;
  replay_error text;
begin
  update public.banking_pay_operations set phase='INSERT_ITEMS'
  where id='43000000-0000-4000-8000-000000000100';
  for scope_variant in
    select scope_row.id as scope_id,scope_row.pay_batch_id,variant.*
    from public.banking_pay_operation_candidate_scope as scope_row
    join pg_temp.h2_f013_e2e_variants as variant
      on scope_row.candidate_id=(
        '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
      )::uuid
    where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
    order by variant.ordinal
  loop
    first_result:=null;replay_result:=null;
    first_error:=null;first_detail:=null;replay_error:=null;
    begin
      first_result:=public.pay_batch_insert_items_from_preview(
        scope_variant.pay_batch_id,'10000000-0000-4000-8000-000000000001',
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics first_error=message_text,first_detail=pg_exception_detail;
    end;
    begin
      replay_result:=public.pay_batch_insert_items_from_preview(
        scope_variant.pay_batch_id,'10000000-0000-4000-8000-000000000001',
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics replay_error=message_text;
    end;
    if replay_error is distinct from first_error then
      raise exception 'H2_F013_CANONICAL_INSERT_REPLAY_CHANGED:%:%:%',
        scope_variant.visible_alias,first_error,replay_error;
    end if;
    update pg_temp.h2_f013_canonical_observed
    set insert_result=first_result,
        insert_replay_result=replay_result,
        insert_error=first_error,
        insert_error_detail=first_detail
    where ordinal=scope_variant.ordinal;
  end loop;
end;
$canonical_insert_items$;

do $canonical_insert_items_green_assertions$
declare
  observed record;
begin
  for observed in
    select *
    from pg_temp.h2_f013_canonical_observed
    where insert_result is not null
    order by ordinal
  loop
    if observed.insert_error is not null
       or observed.insert_result->>'ok'<>'true'
       or (observed.insert_result->>'ordinary_page_allocation_row_count')::integer<>1
       or (observed.insert_result->>'deferred_finance_adjustment_rows')::integer<>1
       or (observed.insert_result->>'inserted_item_rows')::integer<>1
       or (observed.insert_result->>'skipped_item_rows')::integer<>1
       or (observed.insert_result->>'failed_item_rows')::integer<>0
       or (observed.insert_result->>'has_more')::boolean
       or observed.insert_replay_result->>'ok'<>'true'
       or (observed.insert_replay_result->>'ordinary_page_allocation_row_count')::integer<>0
       or (observed.insert_replay_result->>'deferred_finance_adjustment_rows')::integer<>1
       or (observed.insert_replay_result->>'inserted_item_rows')::integer<>0
       or (observed.insert_replay_result->>'skipped_item_rows')::integer<>1
       or (observed.insert_replay_result->>'failed_item_rows')::integer<>0
       or (observed.insert_replay_result->>'has_more')::boolean then
      raise exception 'H2_F013_INSERT_ITEMS_GREEN_RECEIPT_MISMATCH:%',to_jsonb(observed);
    end if;
  end loop;

  if (select count(*) from pg_temp.h2_f013_canonical_observed where insert_result is not null)<>1 then
    raise exception 'H2_F013_INSERT_ITEMS_GREEN_VARIANT_COUNT_CHANGED';
  end if;
end;
$canonical_insert_items_green_assertions$;

do $canonical_insert_items_negative_guards$
declare
  scope_row record;
  finance_row record;
  batch_candidate_id uuid;
  item_count_before integer;
  item_count_after integer;
  caught boolean;
begin
  select candidate_scope.id as scope_id,
         candidate_scope.pay_batch_id,
         variant.visible_alias
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  join pg_temp.h2_f013_e2e_variants as variant
    on candidate_scope.candidate_id=(
      '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
    )::uuid
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  select allocation_row.*
  into strict finance_row
  from public.banking_pay_operation_candidate_allocation_rows as allocation_row
  where allocation_row.candidate_scope_id=scope_row.scope_id
    and allocation_row.finance_case_id is not null;

  select batch_candidate.id into strict batch_candidate_id
  from public.pay_batch_candidates as batch_candidate
  where batch_candidate.pay_batch_id=scope_row.pay_batch_id
    and batch_candidate.candidate_id=finance_row.candidate_id;

  select count(*)::integer into item_count_before
  from public.pay_batch_items as item
  where item.pay_batch_candidate_id=batch_candidate_id
    and coalesce(item.is_voided,false)=false;

  caught:=false;
  begin
    update public.banking_pay_operation_candidate_allocation_rows
    set source_ref='advance:00000000-0000-4000-8000-000000000000'
    where id=finance_row.id;
    perform public.pay_batch_insert_items_from_preview(
      scope_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
      '43000000-0000-4000-8000-000000000100',jsonb_build_array(scope_row.scope_id::text)
    );
  exception when sqlstate 'P0001' then
    if sqlerrm<>'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID' then raise; end if;
    caught:=true;
  end;
  if caught is not true then
    raise exception 'H2_F013_TAMPERED_FINANCE_SOURCE_ACCEPTED';
  end if;

  if scope_row.visible_alias='PAYMENT_ADVANCE_REPAYMENT' then
    caught:=false;
    begin
      update public.banking_pay_operation_candidate_allocation_rows
      set allocation_type='LOAN_REPAYMENT'
      where id=finance_row.id;
      perform public.pay_batch_insert_items_from_preview(
        scope_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
        '43000000-0000-4000-8000-000000000100',jsonb_build_array(scope_row.scope_id::text)
      );
    exception when sqlstate 'P0001' then
      if sqlerrm<>'MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE' then raise; end if;
      caught:=true;
    end;
    if caught is not true then
      raise exception 'H2_F013_HIDDEN_LOAN_REPAYMENT_VISIBLE_INPUT_ACCEPTED';
    end if;
  end if;

  caught:=false;
  begin
    insert into public.pay_batch_items(
      pay_batch_candidate_id,item_type,pay_channel,operation_source_key,
      amount_ex_vat,finance_case_id,finance_component_id,source_ref
    ) values (
      batch_candidate_id,'ADJUSTMENT_DELTA',finance_row.pay_channel,
      finance_row.operation_source_key,finance_row.allocated_amount,
      finance_row.finance_case_id,finance_row.finance_component_id,finance_row.source_ref
    );
    perform public.pay_batch_insert_items_from_preview(
      scope_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
      '43000000-0000-4000-8000-000000000100',jsonb_build_array(scope_row.scope_id::text)
    );
  exception when sqlstate 'P0001' then
    if sqlerrm<>'DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH' then raise; end if;
    caught:=true;
  end;
  if caught is not true then
    raise exception 'H2_F013_WRONG_ORDINARY_FINANCE_LINK_ACCEPTED';
  end if;

  select count(*)::integer into item_count_after
  from public.pay_batch_items as item
  where item.pay_batch_candidate_id=batch_candidate_id
    and coalesce(item.is_voided,false)=false;
  if item_count_after<>item_count_before then
    raise exception 'H2_F013_NEGATIVE_GUARD_LEFT_ITEM_EFFECT:%:%',item_count_before,item_count_after;
  end if;
end;
$canonical_insert_items_negative_guards$;

select case when exists (
  select 1
  from pg_temp.h2_f013_canonical_observed
  where insert_error is not null
) then 'true' else 'false' end as h2_f013_insert_diverged
\gset

\if :h2_f013_insert_diverged
do $canonical_insert_first_divergence$
declare
  observed record;
begin
  select * into strict observed
  from pg_temp.h2_f013_canonical_observed
  where insert_error is not null;

  if exists (
       select 1
       from public.pay_batch_items as item
       join public.pay_batch_candidates as batch_candidate
         on batch_candidate.id=item.pay_batch_candidate_id
       join public.pay_batches as batch_row
         on batch_row.id=batch_candidate.pay_batch_id
       where batch_row.source_workbench_session_id=
             '43000000-0000-4000-8000-000000000005'
     )
     or exists (
       select 1
       from public.pay_advance_reservations as reservation
       join public.pay_batches as batch_row
         on batch_row.id=reservation.pay_batch_id
       where batch_row.source_workbench_session_id=
             '43000000-0000-4000-8000-000000000005'
     )
     or exists (
       select 1 from public.pay_bank_transfers
       where pay_batch_id in (
         select id from public.pay_batches
         where source_workbench_session_id=
               '43000000-0000-4000-8000-000000000005'
       )
     )
     or exists (
       select 1 from public.banking_pay_operation_provider_attempts
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_settlement_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_remittance_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     ) then
    raise exception 'H2_F013_INSERT_DIVERGENCE_LEFT_FINANCIAL_OR_EXTERNAL_EFFECT';
  end if;

  raise notice 'H2_F013_FIRST_DOWNSTREAM_DIVERGENCE=%',jsonb_build_object(
    'stage','INSERT_ITEMS',
    'variant_ordinal',observed.ordinal,
    'visible_alias',observed.visible_alias,
    'pay_channel',observed.pay_channel,
    'taxability',observed.taxability,
    'error',observed.insert_error,
    'detail',observed.insert_error_detail,
    'zero_financial_or_external_effect',true
  );
end;
$canonical_insert_first_divergence$;

select case when lower(coalesce(
  current_setting('cloudtms.h2_f013_simulate_certified_defer',true),'false'
)) in ('true','t','1','yes','y','on') then 'true' else 'false' end
as h2_f013_simulate_certified_defer
\gset

\if :h2_f013_simulate_certified_defer
do $canonical_certified_defer_interface$
declare
  scope_row record;
  allocation_digest text;
begin
  select candidate_scope.id as scope_id,candidate_scope.pay_batch_id,
         variant.ordinal
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  join pg_temp.h2_f013_e2e_variants as variant
    on candidate_scope.candidate_id=(
      '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
  )::uuid
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  select encode(extensions.digest(convert_to(
    coalesce(jsonb_agg(to_jsonb(allocation_row) order by allocation_row.id),'[]'::jsonb)::text,
    'UTF8'
  ),'sha256'),'hex')
  into allocation_digest
  from public.banking_pay_operation_candidate_allocation_rows as allocation_row
  where allocation_row.operation_id='43000000-0000-4000-8000-000000000100';

  if (select count(*)
      from public.banking_pay_operation_candidate_allocation_rows
      where operation_id='43000000-0000-4000-8000-000000000100')<>2
     or (select count(*)
         from public.banking_pay_operation_candidate_allocation_rows
         where operation_id='43000000-0000-4000-8000-000000000100'
           and finance_case_id is not null)<>1 then
    raise exception 'H2_F013_CERTIFIED_DEFER_INTERFACE_ALLOCATION_STATE_CHANGED';
  end if;

  update pg_temp.h2_f013_canonical_observed
  set prechange_insert_error=insert_error,
      prechange_insert_error_detail=insert_error_detail,
      insert_error=null,
      insert_error_detail=null,
      terminal_stage='CERTIFIED_DEFER_INTERFACE',
      certified_defer_simulated=true,
      certified_defer_insert_result=jsonb_build_object(
        'simulated_result','CERTIFIED_FINANCE_DEFER',
        'ordinary_item_materialisation_exercised',false,
        'allocation_rows_unchanged',true,
        'allocation_rows_digest_sha256',allocation_digest
      )
  where ordinal=scope_row.ordinal;

  raise notice 'H2_F013_CERTIFIED_DEFER_INTERFACE_PASS=%',jsonb_build_object(
    'variant_ordinal',scope_row.ordinal,
    'ordinary_item_materialisation_exercised',false,
    'allocation_rows_unchanged',true,
    'allocation_rows_digest_sha256',allocation_digest,
    'classification','INTERFACE_ONLY_NOT_END_TO_END_PASS'
  );
end;
$canonical_certified_defer_interface$;
\else
rollback;
\quit
\endif
\endif

do $canonical_apply_finance$
declare
  scope_variant record;
  first_result jsonb;
  replay_result jsonb;
  first_error text;
  first_detail text;
  replay_error text;
begin
  update public.banking_pay_operations set phase='APPLY_FINANCE_ADJUSTMENTS'
  where id='43000000-0000-4000-8000-000000000100';
  for scope_variant in
    select scope_row.id as scope_id,scope_row.pay_batch_id,variant.*
    from public.banking_pay_operation_candidate_scope as scope_row
    join pg_temp.h2_f013_e2e_variants as variant
      on scope_row.candidate_id=(
        '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
      )::uuid
    join pg_temp.h2_f013_canonical_observed as observed using(ordinal)
    where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
      and observed.insert_error is null
    order by variant.ordinal
  loop
    first_result:=null;replay_result:=null;
    first_error:=null;first_detail:=null;replay_error:=null;
    begin
      first_result:=public.pay_batch_apply_finance_adjustments(
        scope_variant.pay_batch_id,scope_variant.pay_channel,
        '10000000-0000-4000-8000-000000000001',null,null,
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics first_error=message_text,first_detail=pg_exception_detail;
    end;
    begin
      replay_result:=public.pay_batch_apply_finance_adjustments(
        scope_variant.pay_batch_id,scope_variant.pay_channel,
        '10000000-0000-4000-8000-000000000001',null,null,
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics replay_error=message_text;
    end;
    if replay_error is distinct from first_error then
      raise exception 'H2_F013_CANONICAL_FINANCE_REPLAY_CHANGED:%:%:%',
        scope_variant.visible_alias,first_error,replay_error;
    end if;
    update pg_temp.h2_f013_canonical_observed
    set terminal_stage='APPLY_FINANCE_ADJUSTMENTS',
        finance_error=first_error,finance_error_detail=first_detail
    where ordinal=scope_variant.ordinal;
  end loop;
end;
$canonical_apply_finance$;

select case when exists (
  select 1
  from pg_temp.h2_f013_canonical_observed
  where finance_error is not null
) then 'true' else 'false' end as h2_f013_finance_diverged
\gset

\if :h2_f013_finance_diverged
do $canonical_finance_first_divergence$
declare
  observed record;
begin
  select * into strict observed
  from pg_temp.h2_f013_canonical_observed
  where finance_error is not null;

  if exists (
       select 1 from public.pay_bank_transfers
       where pay_batch_id in (
         select id from public.pay_batches
         where source_workbench_session_id=
               '43000000-0000-4000-8000-000000000005'
       )
     )
     or exists (
       select 1 from public.banking_pay_operation_provider_attempts
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_settlement_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_remittance_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     ) then
    raise exception 'H2_F013_FINANCE_DIVERGENCE_CROSSED_EXTERNAL_BOUNDARY';
  end if;

  raise notice 'H2_F013_FIRST_DOWNSTREAM_DIVERGENCE=%',jsonb_build_object(
    'stage','APPLY_FINANCE_ADJUSTMENTS',
    'variant_ordinal',observed.ordinal,
    'visible_alias',observed.visible_alias,
    'pay_channel',observed.pay_channel,
    'taxability',observed.taxability,
    'error',observed.finance_error,
    'detail',observed.finance_error_detail,
    'zero_external_effect',true
  );
end;
$canonical_finance_first_divergence$;
rollback;
\quit
\endif

do $canonical_finalizer$
declare
  scope_variant record;
  first_result jsonb;
  replay_result jsonb;
  first_error text;
  first_detail text;
  replay_error text;
begin
  update public.banking_pay_operations set phase='FINALISE_RESERVATIONS'
  where id='43000000-0000-4000-8000-000000000100';
  for scope_variant in
    select scope_row.id as scope_id,scope_row.pay_batch_id,variant.*
    from public.banking_pay_operation_candidate_scope as scope_row
    join pg_temp.h2_f013_e2e_variants as variant
      on scope_row.candidate_id=(
        '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
      )::uuid
    join pg_temp.h2_f013_canonical_observed as observed using(ordinal)
    where scope_row.operation_id='43000000-0000-4000-8000-000000000100'
      and observed.insert_error is null and observed.finance_error is null
    order by variant.ordinal
  loop
    first_result:=null;replay_result:=null;
    first_error:=null;first_detail:=null;replay_error:=null;
    begin
      first_result:=public.pay_batch_finalize_reservations_and_markers(
        scope_variant.pay_batch_id,scope_variant.pay_channel,
        '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-24',
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics first_error=message_text,first_detail=pg_exception_detail;
    end;
    begin
      replay_result:=public.pay_batch_finalize_reservations_and_markers(
        scope_variant.pay_batch_id,scope_variant.pay_channel,
        '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-24',
        '43000000-0000-4000-8000-000000000100',
        jsonb_build_array(scope_variant.scope_id::text)
      );
    exception when others then
      get stacked diagnostics replay_error=message_text;
    end;
    if replay_error is distinct from first_error then
      raise exception 'H2_F013_CANONICAL_FINALIZER_REPLAY_CHANGED:%:%:%',
        scope_variant.visible_alias,first_error,replay_error;
    end if;
    update pg_temp.h2_f013_canonical_observed
    set terminal_stage=case when first_error is null then 'COMPLETE'
                            else 'FINALISE_RESERVATIONS' end,
        finalizer_error=first_error,finalizer_error_detail=first_detail
    where ordinal=scope_variant.ordinal;
  end loop;
end;
$canonical_finalizer$;

select case when exists (
  select 1
  from pg_temp.h2_f013_canonical_observed
  where finalizer_error is not null
) then 'true' else 'false' end as h2_f013_finalizer_diverged
\gset

\if :h2_f013_finalizer_diverged
do $canonical_finalizer_first_divergence$
declare
  observed record;
begin
  select * into strict observed
  from pg_temp.h2_f013_canonical_observed
  where finalizer_error is not null;

  if exists (
       select 1 from public.pay_bank_transfers
       where pay_batch_id in (
         select id from public.pay_batches
         where source_workbench_session_id=
               '43000000-0000-4000-8000-000000000005'
       )
     )
     or exists (
       select 1 from public.banking_pay_operation_provider_attempts
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_settlement_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     )
     or exists (
       select 1 from public.banking_pay_operation_remittance_scope
       where operation_id='43000000-0000-4000-8000-000000000100'
     ) then
    raise exception 'H2_F013_FINALIZER_DIVERGENCE_CROSSED_EXTERNAL_BOUNDARY';
  end if;

  raise notice 'H2_F013_FIRST_DOWNSTREAM_DIVERGENCE=%',jsonb_build_object(
    'stage','FINALISE_RESERVATIONS',
    'variant_ordinal',observed.ordinal,
    'visible_alias',observed.visible_alias,
    'pay_channel',observed.pay_channel,
    'taxability',observed.taxability,
    'error',observed.finalizer_error,
    'detail',observed.finalizer_error_detail,
    'zero_external_effect',true
  );
end;
$canonical_finalizer_first_divergence$;
rollback;
\quit
\endif

do $canonical_exact_finance_link_response_loss_replay$
declare
  scope_row record;
  replay_result jsonb;
  item_count_before integer;
  item_count_after integer;
begin
  select candidate_scope.id as scope_id,candidate_scope.pay_batch_id
  into strict scope_row
  from public.banking_pay_operation_candidate_scope as candidate_scope
  where candidate_scope.operation_id='43000000-0000-4000-8000-000000000100';

  select count(*)::integer into item_count_before
  from public.pay_batch_items as item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id=item.pay_batch_candidate_id
  where batch_candidate.pay_batch_id=scope_row.pay_batch_id
    and coalesce(item.is_voided,false)=false;

  update public.banking_pay_operations
  set phase='INSERT_ITEMS'
  where id='43000000-0000-4000-8000-000000000100';

  replay_result:=public.pay_batch_insert_items_from_preview(
    scope_row.pay_batch_id,'10000000-0000-4000-8000-000000000001',
    '43000000-0000-4000-8000-000000000100',jsonb_build_array(scope_row.scope_id::text)
  );

  update public.banking_pay_operations
  set phase='FINALISE_RESERVATIONS'
  where id='43000000-0000-4000-8000-000000000100';

  select count(*)::integer into item_count_after
  from public.pay_batch_items as item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id=item.pay_batch_candidate_id
  where batch_candidate.pay_batch_id=scope_row.pay_batch_id
    and coalesce(item.is_voided,false)=false;

  if replay_result->>'ok'<>'true'
     or (replay_result->>'inserted_item_rows')::integer<>0
     or (replay_result->>'reused_item_rows')::integer<>2
     or (replay_result->>'failed_item_rows')::integer<>0
     or (replay_result->>'has_more')::boolean
     or item_count_after<>item_count_before then
    raise exception 'H2_F013_EXACT_FINANCE_LINK_RESPONSE_LOSS_REPLAY_CHANGED:%:%:%',
      replay_result,item_count_before,item_count_after;
  end if;
end;
$canonical_exact_finance_link_response_loss_replay$;

update pg_temp.h2_f013_canonical_observed as observed
set active_item_count=outcome.active_item_count,
    finance_item_count=outcome.finance_item_count,
    observed_finance_item_type=outcome.finance_item_type,
    observed_finance_amount_ex_vat=outcome.finance_amount_ex_vat,
    observed_finance_amount_vat=outcome.finance_amount_vat,
    observed_finance_amount_inc_vat=outcome.finance_amount_inc_vat,
    observed_finance_paye_treatment=outcome.finance_paye_treatment,
    observed_finance_pay_channel=outcome.finance_pay_channel,
    observed_finance_umbrella_id=outcome.finance_umbrella_id,
    observed_finance_component_id=outcome.finance_component_id,
    observed_payout_instruction_snapshot_json=outcome.payout_instruction_snapshot_json,
    allocation_status=outcome.allocation_status,
    reservation_count=outcome.reservation_count,
    reserved_amount=outcome.reserved_amount,
    case_status=outcome.case_status,
    payout_status=outcome.payout_status
from (
  select variant.ordinal,
    count(item.id)::integer as active_item_count,
    count(item.id) filter(where item.finance_case_id is not null)::integer as finance_item_count,
    min(item.item_type) filter(where item.finance_case_id is not null) as finance_item_type,
    min(item.amount_ex_vat) filter(where item.finance_case_id is not null) as finance_amount_ex_vat,
    min(item.amount_vat) filter(where item.finance_case_id is not null) as finance_amount_vat,
    min(item.amount_inc_vat) filter(where item.finance_case_id is not null) as finance_amount_inc_vat,
    min(item.paye_treatment) filter(where item.finance_case_id is not null) as finance_paye_treatment,
    min(item.pay_channel::text) filter(where item.finance_case_id is not null) as finance_pay_channel,
    (min(item.umbrella_id::text) filter(where item.finance_case_id is not null))::uuid as finance_umbrella_id,
    (min(item.finance_component_id::text) filter(where item.finance_case_id is not null))::uuid as finance_component_id,
    case
      when count(item.id) filter(where item.finance_case_id is not null)=1
        then min(item.payout_instruction_snapshot_json::text)::jsonb
      else null::jsonb
    end as payout_instruction_snapshot_json,
    min(allocation.status) filter(where allocation.finance_case_id is not null) as allocation_status,
    count(distinct reservation.id)::integer as reservation_count,
    coalesce(sum(distinct reservation.reserved_amount),0)::numeric(12,2) as reserved_amount,
    min(advance.status::text) as case_status,
    min(advance.payout_status::text) as payout_status
  from pg_temp.h2_f013_e2e_variants as variant
  join public.banking_pay_operation_candidate_scope as scope_row
    on scope_row.operation_id='43000000-0000-4000-8000-000000000100'
   and scope_row.candidate_id=(
     '43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
   )::uuid
  join public.pay_advances as advance
    on advance.id=(
      '43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0')
    )::uuid
  left join public.banking_pay_operation_candidate_allocation_rows as allocation
    on allocation.candidate_scope_id=scope_row.id
   and allocation.finance_case_id=advance.id
  left join public.pay_batch_candidates as batch_candidate
    on batch_candidate.pay_batch_id=scope_row.pay_batch_id
   and batch_candidate.candidate_id=scope_row.candidate_id
  left join public.pay_batch_items as item
    on item.pay_batch_candidate_id=batch_candidate.id
   and coalesce(item.is_voided,false)=false
  left join public.pay_advance_reservations as reservation
    on reservation.pay_batch_id=scope_row.pay_batch_id
   and reservation.finance_case_id=advance.id
   and reservation.status in ('RESERVED','COMMITTED')
  group by variant.ordinal
) as outcome
where observed.ordinal=outcome.ordinal;

do $canonical_policy_interface_assertions$
declare
  interface_row record;
  expected_component_id uuid;
  expected_umbrella_id uuid;
  expected_payout_status text;
  expected_payee_kind text;
  expected_appears_on_umbrella_remittance boolean;
  expected_candidate_payment_advice boolean;
  expected_candidate_directed_oneoff boolean;
begin
  if (select count(*) from pg_temp.h2_f013_canonical_observed where terminal_stage='COMPLETE')<>1
     or exists (
       select 1
       from pg_temp.h2_f013_canonical_observed
       where insert_result is not null
         and (
           insert_error is not null
           or finance_error is not null
           or finalizer_error is not null
           or terminal_stage<>'COMPLETE'
         )
     ) then
    raise exception 'H2_F013_COMPLETE_GREEN_CHAIN_CARDINALITY_OR_STAGE_CHANGED:%',(
      select jsonb_agg(jsonb_build_object(
        'ordinal',ordinal,
        'terminal_stage',terminal_stage,
        'insert_error',insert_error,
        'finance_error',finance_error,
        'finalizer_error',finalizer_error
      ) order by ordinal)
      from pg_temp.h2_f013_canonical_observed
    );
  end if;

  for interface_row in
    select observed.*,variant.direction_sign,variant.expected_final_item_type,
           variant.expected_amount_ex_vat,variant.expected_amount_vat,
           variant.expected_amount_inc_vat,variant.expected_paye_treatment,
           variant.routing_kind
    from pg_temp.h2_f013_canonical_observed as observed
    join pg_temp.h2_f013_e2e_variants as variant using(ordinal)
    where observed.terminal_stage='COMPLETE'
    order by observed.ordinal
  loop
    expected_component_id:=(
      '43000000-0000-4000-8000-'||
      lpad((80000+interface_row.ordinal)::text,12,'0')
    )::uuid;
    expected_umbrella_id:=case when interface_row.pay_channel='UMBRELLA'
      then '41000000-0000-4000-8000-000000000010'::uuid
      else null::uuid end;
    expected_payout_status:=case
      when interface_row.visible_alias='PAYMENT_ADVANCE_REPAYMENT' then 'PAID'
      when interface_row.visible_alias in ('LOAN_PAYOUT','MANUAL_CREDIT_ADJUSTMENT_PAYMENT') then 'PENDING'
      else null::text end;
    expected_payee_kind:=case when interface_row.routing_kind='UMBRELLA_COMPANY'
      then 'UMBRELLA' else 'CANDIDATE' end;
    expected_appears_on_umbrella_remittance:=
      interface_row.routing_kind='UMBRELLA_COMPANY';
    expected_candidate_payment_advice:=
      interface_row.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT';
    expected_candidate_directed_oneoff:=
      interface_row.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT';

    if interface_row.certified_defer_simulated
       or interface_row.active_item_count<>2
       or interface_row.finance_item_count<>1
       or interface_row.observed_finance_item_type<>
          interface_row.expected_final_item_type
       or round(interface_row.observed_finance_amount_ex_vat,2)<>
          round(interface_row.expected_amount_ex_vat,2)
       or round(interface_row.observed_finance_amount_vat,2)<>
          round(interface_row.expected_amount_vat,2)
       or round(interface_row.observed_finance_amount_inc_vat,2)<>
          round(interface_row.expected_amount_inc_vat,2)
       or interface_row.observed_finance_paye_treatment<>
          interface_row.expected_paye_treatment
       or interface_row.observed_finance_pay_channel<>
          interface_row.pay_channel
       or interface_row.observed_finance_umbrella_id is distinct from
          expected_umbrella_id
       or interface_row.observed_finance_component_id is distinct from
          expected_component_id
       or interface_row.allocation_status<>'ITEM_CREATED'
       or interface_row.reservation_count<>1
       or round(interface_row.reserved_amount,2)<>10.00
       or interface_row.case_status<>'ACTIVE'
       or interface_row.payout_status is distinct from expected_payout_status
       or interface_row.observed_payout_instruction_snapshot_json->>'routing_kind'<>
          interface_row.routing_kind
       or interface_row.observed_payout_instruction_snapshot_json->>'pay_channel'<>
          interface_row.pay_channel
       or interface_row.observed_payout_instruction_snapshot_json->>'taxability'<>
          interface_row.taxability
       or interface_row.observed_payout_instruction_snapshot_json->>'payee_entity_kind'<>
          expected_payee_kind
       or (interface_row.observed_payout_instruction_snapshot_json->>'appears_on_umbrella_remittance')::boolean
          is distinct from expected_appears_on_umbrella_remittance
       or (interface_row.observed_payout_instruction_snapshot_json->>'generates_candidate_payment_advice')::boolean
          is distinct from expected_candidate_payment_advice
       or (interface_row.observed_payout_instruction_snapshot_json->>'is_candidate_directed_oneoff_payout')::boolean
          is distinct from expected_candidate_directed_oneoff then
      raise exception 'H2_F013_COMPLETE_GREEN_POLICY_PARITY_MISMATCH:%',
        to_jsonb(interface_row);
    end if;
  end loop;

  raise notice 'H2_F013_COMPLETE_GREEN_POLICY_PARITY_PASS=%',
    jsonb_build_object(
      'variants',20,
      'variants_exercised_in_this_rollback',1,
      'visible_aliases',6,
      'runtime_owner_changed','INSERT_ITEMS_HANDOFF_ONLY',
      'finance_owner_changed',false,
      'policy_or_economic_delta_count',0
    );
end;
$canonical_policy_interface_assertions$;

do $canonical_snapshot$
declare
  snapshot jsonb;
begin
  select jsonb_agg(to_jsonb(observed) order by observed.ordinal)
  into snapshot
  from pg_temp.h2_f013_canonical_observed as observed;
  raise notice 'H2_F013_CANONICAL_OBSERVED=%',snapshot;
  if (select count(*) from pg_temp.h2_f013_canonical_observed)<>20 then
    raise exception 'H2_F013_CANONICAL_EXECUTION_INCOMPLETE';
  end if;
  if exists(
    select 1 from public.pay_bank_transfers
    where pay_batch_id in (
      select id from public.pay_batches
      where source_workbench_session_id='43000000-0000-4000-8000-000000000005'
    )
  ) or exists(
    select 1 from public.banking_pay_operation_provider_attempts
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) or exists(
    select 1 from public.banking_pay_operation_settlement_scope
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) or exists(
    select 1 from public.banking_pay_operation_remittance_scope
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) then
    raise exception 'H2_F013_CANONICAL_CROSSED_EXTERNAL_PAYMENT_BOUNDARY';
  end if;
end;
$canonical_snapshot$;
\endif

-- Historical manually constructed INSERT_ITEMS/APPLY_FINANCE evidence. This
-- does not carry the current semantic scope transaction token and therefore
-- must not run or be represented as current-runtime proof.
\if false
insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id,
  source_workbench_session_id,source_snapshot_run_id,source_session_version,
  execution_commit_state
)
select
  ('41000000-0000-4000-8000-'||lpad((30000+ordinal)::text,12,'0'))::uuid,
  '2026-08-28','DRAFT','MONZO_CSV','CSV','CSV','SANDBOX',pay_channel,
  '10000000-0000-4000-8000-000000000001'::uuid,
  '10000000-0000-4000-8000-000000000005'::uuid,
  '10000000-0000-4000-8000-000000000004'::uuid,1,'NOT_SUBMITTED'
from pg_temp.h2_f013_e2e_variants;

insert into public.pay_batch_candidates(
  id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
  paye_state,settlement_status,gross_preview,net_bank_amount
)
select
  ('41000000-0000-4000-8000-'||lpad((40000+ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((30000+ordinal)::text,12,'0'))::uuid,
  candidate_id,'H2-F013-'||ordinal,'H2 F013 E2E '||pay_channel,
  'READY','PENDING',10.00,case when pay_channel='PAYE' then 100.00 else 0.00 end
from pg_temp.h2_f013_e2e_variants;

insert into public.pay_batch_paye_net_inputs(
  pay_batch_candidate_id,source,net_amount,imported_at_utc
)
select
  ('41000000-0000-4000-8000-'||lpad((40000+ordinal)::text,12,'0'))::uuid,
  'MANUAL_ENTRY',100.00,now()
from pg_temp.h2_f013_e2e_variants
where pay_channel='PAYE';

insert into public.banking_pay_operations(
  id,operation_type,status,phase,actor_user_id,workbench_session_id,
  pay_batch_id,idempotency_key,input_json,config_json,progress_json
)
select
  ('41000000-0000-4000-8000-'||lpad((10000+ordinal)::text,12,'0'))::uuid,
  'DRAFT_CREATE','RUNNING','INSERT_ITEMS',
  '10000000-0000-4000-8000-000000000001'::uuid,
  '10000000-0000-4000-8000-000000000005'::uuid,
  ('41000000-0000-4000-8000-'||lpad((30000+ordinal)::text,12,'0'))::uuid,
  'h2-f013-e2e-'||ordinal,'{}','{}','{}'
from pg_temp.h2_f013_e2e_variants;

insert into public.banking_pay_operation_candidate_scope(
  id,operation_id,workbench_session_id,source_snapshot_run_id,
  source_session_version,candidate_id,pay_channel,pay_batch_id,
  selected_finance_case_ids_json,effective_canonical_preview_lines_json,
  selected_canonical_preview_lines_json,candidate_totals_json,
  allocation_basis_json,scope_hash,chunk_sequence,status
)
select
  ('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  '10000000-0000-4000-8000-000000000005'::uuid,
  '10000000-0000-4000-8000-000000000004'::uuid,1,
  variant.candidate_id,variant.pay_channel,
  ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
  jsonb_build_array('41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0')),
  jsonb_build_array(line_row.line_json),jsonb_build_array(line_row.line_json),
  jsonb_build_object('selected_row_count',1,'selected_preview_row_count',1),
  '{}'::jsonb,'h2-f013-e2e-'||variant.ordinal,1,'ALLOCATED'
from pg_temp.h2_f013_e2e_variants as variant
cross join lateral (
  select jsonb_build_object(
    'preview_row_id','41000000-0000-4000-8000-'||lpad((70000+variant.ordinal)::text,12,'0'),
    'line_id','finance:41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0')||':'||lower(variant.visible_alias),
    'row_key','finance:41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
    'candidate_id',variant.candidate_id::text,
    'line_type',variant.visible_alias,'item_direction',case when variant.direction_sign<0 then 'DEDUCTION' else 'PAYMENT' end,
    'finance_case_id','41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
    'case_type',variant.case_type::text,
    'source_ref','advance:41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
    'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
    'key_type','CASE_TOTAL','key_value','TOTAL',
    'economic_key',jsonb_build_object('key_type','CASE_TOTAL','key_value','TOTAL'),
    'pay_channel',variant.pay_channel,'current_pay_method',variant.pay_channel,
    'paye_treatment',variant.expected_paye_treatment,
    'taxability',variant.taxability,'routing_kind',variant.routing_kind,
    'beneficiary_name',case when variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT' then 'H2 one-off beneficiary'
                            when variant.routing_kind='UMBRELLA_COMPANY' then 'H2 finance Umbrella'
                            else 'H2 finance PAYE candidate' end,
    'masked_bank_account',case when variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT' then '******88'
                               when variant.routing_kind='UMBRELLA_COMPANY' then '******21'
                               else '******78' end,
    'bank_details_hash',case when variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT' then 'h2-f013-e2e-oneoff-'||variant.ordinal
                             when variant.routing_kind='UMBRELLA_COMPANY' then 'h2-f013-e2e-umbrella-bank'
                             else 'h2-f013-e2e-paye-bank' end,
    'is_candidate_directed_oneoff_payout',variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT',
    'appears_on_umbrella_remittance',variant.routing_kind='UMBRELLA_COMPANY',
    'generates_candidate_payment_advice',variant.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT',
    'amount_ex_vat',to_char(variant.expected_amount_ex_vat,'FM999999990.00'),
    'amount_display',to_char(variant.expected_amount_inc_vat,'FM999999990.00'),
    'case_components',jsonb_build_array(jsonb_build_object(
      'finance_component_id','41000000-0000-4000-8000-'||lpad((80000+variant.ordinal)::text,12,'0'),
      'source_family_key','case:41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
      'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
      'classification',variant.classification::text,'source_pay_method',variant.pay_channel,
      'source_basis_json',jsonb_build_object('taxability',variant.taxability,'routing_kind',variant.routing_kind),
      'source_amount','10.00','remaining_source_amount','10.00',
      'allocated_source_due_amount_ex_vat','10.00','preview_due_amount_ex_vat','10.00',
      'target_pay_ex_vat','10.00'
    )),
    'presentation_section','READY_TO_PAY','readiness_state','READY_TO_PAY',
    'draftable',true,'is_ready_for_draft',true,'selection_allowed',true,
    'selected',true,'selection_state','SELECTED','status','READY',
    'is_excluded_from_allocation',false,
    'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
  ) as line_json
) as line_row;

insert into public.banking_pay_operation_candidate_allocation_rows(
  id,operation_id,candidate_scope_id,pay_batch_id,candidate_id,pay_channel,
  finance_case_id,finance_component_id,allocation_type,source_ref,
  operation_source_key,allocated_amount,allocation_basis_json,sort_order,status
)
select
  ('41000000-0000-4000-8000-'||lpad((60000+variant.ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
  variant.candidate_id,variant.pay_channel,
  ('41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid,
  ('41000000-0000-4000-8000-'||lpad((80000+variant.ordinal)::text,12,'0'))::uuid,
  variant.visible_alias,
  'advance:41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'),
  '41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0')
    ||':allocation:41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0')
    ||':41000000-0000-4000-8000-'||lpad((70000+variant.ordinal)::text,12,'0'),
  variant.expected_amount_ex_vat,
  jsonb_build_object(
    'line',scope_row.selected_canonical_preview_lines_json->0,
    'component',scope_row.selected_canonical_preview_lines_json->0->'case_components'->0,
    'finance_component',scope_row.selected_canonical_preview_lines_json->0->'case_components'->0
  ),variant.ordinal,'PENDING'
from pg_temp.h2_f013_e2e_variants as variant
join public.banking_pay_operation_candidate_scope as scope_row
  on scope_row.id=('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid;

with operation_scopes as (
  select operation_id,jsonb_build_array(id) as candidate_scope_ids
  from public.banking_pay_operation_candidate_scope
  where operation_id::text like '41000000-0000-4000-8000-%'
), current_plan as (
  select operation_scopes.operation_id as scope_operation_id,plan_row.*
  from operation_scopes
  cross join lateral private.pay_workbench_draft_finance_item_plan_v1(
    operation_scopes.operation_id,operation_scopes.candidate_scope_ids
  ) as plan_row
)
update public.banking_pay_operation_candidate_allocation_rows as allocation_update
set allocation_basis_json=allocation_update.allocation_basis_json||jsonb_build_object(
  'draft_finance_item_plan',jsonb_build_object(
    'contract_version',1,
    'planned_item_key',current_plan.planned_item_key,
    'planned_item_type',current_plan.planned_item_type,
    'contribution_amount',current_plan.contribution_amount,
    'planned_item_amount',current_plan.planned_item_amount,
    'contribution_count',current_plan.contribution_count,
    'plan_digest',current_plan.plan_digest
  )
)
from current_plan
where allocation_update.operation_id=current_plan.scope_operation_id
  and allocation_update.operation_source_key=current_plan.allocation_source_key;

create temporary table pg_temp.h2_f013_e2e_observed(
  ordinal integer primary key,
  visible_alias text not null,
  pay_channel text not null,
  taxability text not null,
  terminal_stage text not null,
  insert_error text,
  insert_error_detail text,
  finance_error text,
  finance_error_detail text,
  finalizer_error text,
  finalizer_error_detail text,
  insert_result jsonb,
  finance_result jsonb,
  finalizer_result jsonb,
  active_item_count integer not null,
  observed_item_type text,
  observed_amount_ex_vat numeric(12,2),
  observed_amount_vat numeric(12,2),
  observed_amount_inc_vat numeric(12,2),
  observed_paye_treatment text,
  observed_pay_channel text,
  observed_finance_component_id uuid,
  observed_payout_snapshot jsonb,
  allocation_status text not null,
  reservation_count integer not null,
  reserved_amount numeric(12,2),
  case_status text not null,
  payout_status text
) on commit drop;

do $execute_complete_path$
declare
  variant record;
  insert_result jsonb;
  finance_result jsonb;
  finalizer_result jsonb;
  insert_error text;
  insert_error_detail text;
  insert_replay_error text;
  finance_error text;
  finance_error_detail text;
  finance_replay_error text;
  finalizer_error text;
  finalizer_error_detail text;
  finalizer_replay_error text;
  terminal_stage text;
  item_row record;
  allocation_status text;
  reservation_count integer;
  reserved_amount numeric(12,2);
  case_status text;
  payout_status text;
begin
  for variant in select * from pg_temp.h2_f013_e2e_variants order by ordinal loop
    insert_result:=null; finance_result:=null; finalizer_result:=null;
    insert_error:=null; insert_error_detail:=null;
    insert_replay_error:=null;
    finance_error:=null; finance_error_detail:=null;
    finance_replay_error:=null;
    finalizer_error:=null; finalizer_error_detail:=null;
    finalizer_replay_error:=null;
    terminal_stage:='INSERT_ITEMS';
    begin
      insert_result:=public.pay_batch_insert_items_from_preview(
        ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
        '10000000-0000-4000-8000-000000000001'::uuid,
        ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
        jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
      );
    exception when others then
      get stacked diagnostics insert_error=message_text,insert_error_detail=pg_exception_detail;
    end;

    -- A response can be lost after the owner commits but before the Worker
    -- finishes the current chunk.  Re-enter only while the operation still
    -- has the exact INSERT_ITEMS phase; replay after phase advance is invalid.
    begin
      perform public.pay_batch_insert_items_from_preview(
        ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
        '10000000-0000-4000-8000-000000000001'::uuid,
        ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
        jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
      );
    exception when others then
      get stacked diagnostics insert_replay_error=message_text;
    end;
    if insert_replay_error is distinct from insert_error then
      raise exception 'H2_F013_INSERT_RESPONSE_LOSS_REPLAY_CHANGED:%:%:%',
        variant.visible_alias,insert_error,insert_replay_error;
    end if;

    if insert_error is null then
      update public.banking_pay_operations
      set phase='APPLY_FINANCE_ADJUSTMENTS'
      where id=('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid;
      terminal_stage:='APPLY_FINANCE_ADJUSTMENTS';
      begin
        finance_result:=public.pay_batch_apply_finance_adjustments(
          ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
          variant.pay_channel,
          '10000000-0000-4000-8000-000000000001'::uuid,
          null,null,
          ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
          jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
        );
      exception when others then
        get stacked diagnostics finance_error=message_text,finance_error_detail=pg_exception_detail;
      end;
      begin
        perform public.pay_batch_apply_finance_adjustments(
          ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
          variant.pay_channel,
          '10000000-0000-4000-8000-000000000001'::uuid,
          null,null,
          ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
          jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
        );
      exception when others then
        get stacked diagnostics finance_replay_error=message_text;
      end;
      if finance_replay_error is distinct from finance_error then
        raise exception 'H2_F013_FINANCE_RESPONSE_LOSS_REPLAY_CHANGED:%:%:%',
          variant.visible_alias,finance_error,finance_replay_error;
      end if;
    end if;

    if insert_error is null and finance_error is null then
      update public.banking_pay_operations
      set phase='FINALISE_RESERVATIONS'
      where id=('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid;
      terminal_stage:='FINALISE_RESERVATIONS';
      begin
        finalizer_result:=public.pay_batch_finalize_reservations_and_markers(
          ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
          variant.pay_channel,
          '10000000-0000-4000-8000-000000000001'::uuid,
          '2026-08-28','2026-08-24',
          ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
          jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
        );
      exception when others then
        get stacked diagnostics finalizer_error=message_text,finalizer_error_detail=pg_exception_detail;
      end;
      begin
        perform public.pay_batch_finalize_reservations_and_markers(
          ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid,
          variant.pay_channel,
          '10000000-0000-4000-8000-000000000001'::uuid,
          '2026-08-28','2026-08-24',
          ('41000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
          jsonb_build_array('41000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))
        );
      exception when others then
        get stacked diagnostics finalizer_replay_error=message_text;
      end;
      if finalizer_replay_error is distinct from finalizer_error then
        raise exception 'H2_F013_FINALIZER_RESPONSE_LOSS_REPLAY_CHANGED:%:%:%',
          variant.visible_alias,finalizer_error,finalizer_replay_error;
      end if;
      if finalizer_error is null then terminal_stage:='COMPLETE'; end if;
    end if;

    select
      count(*)::integer as active_item_count,
      min(item_type) as observed_item_type,
      min(amount_ex_vat) as observed_amount_ex_vat,
      min(amount_vat) as observed_amount_vat,
      min(amount_inc_vat) as observed_amount_inc_vat,
      min(paye_treatment) as observed_paye_treatment,
      min(pay_channel) as observed_pay_channel,
      min(finance_component_id::text)::uuid as observed_finance_component_id,
      case when count(*)=1 then min(payout_instruction_snapshot_json::text)::jsonb else null end as observed_payout_snapshot
    into item_row
    from public.pay_batch_items as item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id=item.pay_batch_candidate_id
    where batch_candidate.pay_batch_id=
      ('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid
      and coalesce(item.is_voided,false)=false;

    select status into strict allocation_status
    from public.banking_pay_operation_candidate_allocation_rows
    where id=('41000000-0000-4000-8000-'||lpad((60000+variant.ordinal)::text,12,'0'))::uuid;

    select count(*)::integer,coalesce(sum(reservation.reserved_amount),0)::numeric(12,2)
    into reservation_count,reserved_amount
    from public.pay_advance_reservations as reservation
    where reservation.pay_batch_id=('41000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid
      and reservation.status in ('RESERVED','COMMITTED');

    select advance.status::text,advance.payout_status::text
    into strict case_status,payout_status
    from public.pay_advances as advance
    where advance.id=('41000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid;

    insert into pg_temp.h2_f013_e2e_observed values (
      variant.ordinal,variant.visible_alias,variant.pay_channel,variant.taxability,
      terminal_stage,insert_error,insert_error_detail,
      finance_error,finance_error_detail,finalizer_error,finalizer_error_detail,
      insert_result,finance_result,finalizer_result,
      item_row.active_item_count,item_row.observed_item_type,
      item_row.observed_amount_ex_vat,item_row.observed_amount_vat,
      item_row.observed_amount_inc_vat,item_row.observed_paye_treatment,
      item_row.observed_pay_channel,item_row.observed_finance_component_id,
      item_row.observed_payout_snapshot,
      allocation_status,reservation_count,reserved_amount,case_status,payout_status
    );
  end loop;
end;
$execute_complete_path$;

do $current_owner_outcome_contract$
declare
  current_owner_snapshot jsonb;
begin
  select jsonb_agg(to_jsonb(observed) order by ordinal)
  into current_owner_snapshot
  from pg_temp.h2_f013_e2e_observed as observed;
  raise notice 'H2_F013_CURRENT_OWNER_ROWS=%',current_owner_snapshot;

  if (select count(*) from pg_temp.h2_f013_e2e_observed where visible_alias='OVERPAYMENT_RECOVERY')<>4
     or exists (
       select 1
       from pg_temp.h2_f013_e2e_observed as observed
       join pg_temp.h2_f013_e2e_variants as expected using (ordinal)
       where observed.visible_alias='OVERPAYMENT_RECOVERY'
         and (
           observed.terminal_stage<>'COMPLETE'
           or observed.insert_error is not null
           or observed.finance_error is not null
           or observed.finalizer_error is not null
           or observed.active_item_count<>1
           or observed.observed_item_type<>expected.expected_final_item_type
           or observed.observed_amount_ex_vat<>expected.expected_amount_ex_vat
           or observed.observed_amount_vat<>expected.expected_amount_vat
           or observed.observed_amount_inc_vat<>expected.expected_amount_inc_vat
           or observed.observed_paye_treatment<>expected.expected_paye_treatment
           or observed.observed_pay_channel<>expected.pay_channel
           or observed.observed_finance_component_id<>
             ('41000000-0000-4000-8000-'||lpad((80000+expected.ordinal)::text,12,'0'))::uuid
           or observed.allocation_status<>'ITEM_CREATED'
           or observed.reservation_count<>1
           or observed.reserved_amount<>10.00
           or observed.case_status<>'ACTIVE'
         )
     ) then
    raise exception 'H2_F013_OVERPAYMENT_CURRENT_OWNER_OUTCOME_CHANGED:%',current_owner_snapshot;
  end if;

  if (select count(*) from pg_temp.h2_f013_e2e_observed where visible_alias<>'OVERPAYMENT_RECOVERY')<>16
     or exists (
       select 1
       from pg_temp.h2_f013_e2e_observed as observed
       where observed.visible_alias<>'OVERPAYMENT_RECOVERY'
         and (
           observed.terminal_stage<>'INSERT_ITEMS'
           or observed.insert_error<>'MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE'
           or observed.finance_error is not null
           or observed.finalizer_error is not null
           or observed.active_item_count<>0
           or observed.allocation_status<>'PENDING'
           or observed.reservation_count<>0
           or observed.reserved_amount<>0.00
           or observed.case_status<>'ACTIVE'
         )
     ) then
    raise exception 'H2_F013_NON_OVERPAYMENT_CURRENT_OWNER_OUTCOME_CHANGED';
  end if;

  if exists (
    select 1
    from public.banking_pay_operation_candidate_scope as scope_row
    cross join lateral jsonb_array_elements(scope_row.selected_canonical_preview_lines_json) as selected(line_json)
    where scope_row.operation_id::text like '41000000-0000-4000-8000-%'
      and (
        upper(coalesce(selected.line_json->>'presentation_section',''))<>'READY_TO_PAY'
        or lower(coalesce(selected.line_json->>'draftable','false'))<>'true'
        or lower(coalesce(selected.line_json->>'is_ready_for_draft','false'))<>'true'
        or lower(coalesce(selected.line_json->>'selection_allowed','false'))<>'true'
        or upper(coalesce(selected.line_json#>>'{economic_key,key_type}',''))<>'CASE_TOTAL'
        or coalesce(selected.line_json#>>'{economic_key,key_value}','')<>'TOTAL'
      )
  ) then
    raise exception 'H2_F013_CANONICAL_SELECTED_INPUT_CONTRACT_CHANGED';
  end if;
end;
$current_owner_outcome_contract$;

do $diagnostic_snapshot$
declare
  snapshot jsonb;
begin
  select jsonb_agg(to_jsonb(observed) order by ordinal)
  into snapshot
  from pg_temp.h2_f013_e2e_observed as observed;
  raise notice 'H2_F013_E2E_OBSERVED=%',snapshot;

  if (select count(*) from pg_temp.h2_f013_e2e_observed)<>20 then
    raise exception 'H2_F013_E2E_EXECUTION_INCOMPLETE';
  end if;
  raise notice 'H2_F013_E2E_SUMMARY=20_VARIANTS;4_COMPLETE_OVERPAYMENT;16_DETERMINISTIC_INSERT_REJECTIONS;REPLAY_STABLE;ROLLBACK_ONLY';
  if exists (
    select 1 from public.pay_bank_transfers
    where pay_batch_id::text like '41000000-0000-4000-8000-%'
  ) or exists (
    select 1 from public.banking_pay_operation_provider_attempts
    where operation_id::text like '41000000-0000-4000-8000-%'
       or pay_batch_id::text like '41000000-0000-4000-8000-%'
  ) or exists (
    select 1 from public.banking_pay_operation_settlement_scope
    where operation_id::text like '41000000-0000-4000-8000-%'
       or pay_batch_id::text like '41000000-0000-4000-8000-%'
  ) or exists (
    select 1 from public.banking_pay_operation_remittance_scope
    where operation_id::text like '41000000-0000-4000-8000-%'
       or pay_batch_id::text like '41000000-0000-4000-8000-%'
  ) then
    raise exception 'H2_F013_E2E_CROSSED_PROVIDER_SETTLEMENT_OR_REMITTANCE_BOUNDARY';
  end if;
end;
$diagnostic_snapshot$;
\endif

do $canonical_external_boundary$
begin
  if exists (
    select 1 from public.pay_bank_transfers
    where pay_batch_id in (
      select id from public.pay_batches
      where source_workbench_session_id='43000000-0000-4000-8000-000000000005'
    )
  ) or exists (
    select 1 from public.banking_pay_operation_provider_attempts
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) or exists (
    select 1 from public.banking_pay_operation_settlement_scope
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) or exists (
    select 1 from public.banking_pay_operation_remittance_scope
    where operation_id='43000000-0000-4000-8000-000000000100'
  ) then
    raise exception 'H2_F013_CANONICAL_CROSSED_EXTERNAL_PAYMENT_BOUNDARY';
  end if;
  raise notice 'H2_F013_CANONICAL_PRECHANGE_PASS=EXPECTED_SCOPE_DIVERGENCE;ZERO_PARTIAL_SCOPE;ZERO_EXTERNAL_EFFECT';
end;
$canonical_external_boundary$;

rollback;
