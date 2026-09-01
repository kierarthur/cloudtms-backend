-- Rollback-contained real first-use of the signed recovery Draft finaliser.
-- Local PostgreSQL 17 only: no provider, execution, settlement or remittance.
\set ON_ERROR_STOP on
begin;
set local statement_timeout='45s';
set local lock_timeout='5s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
  job_title_norm,week_ending_date
) values (
  '10000000-0000-4000-8000-000000009001','signed-return-fixture',
  'signed return candidate','signed return hospital','signed return ward',
  'signed return role','2026-03-15'
);

insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id
) values (
  '10000000-0000-4000-8000-000000009101','2026-08-28','DRAFT',
  'MONZO_CSV','CSV','CSV','SANDBOX','PAYE',
  '10000000-0000-4000-8000-000000000001'
);
insert into public.pay_batch_candidates(
  id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
  paye_state,settlement_status,gross_preview,net_bank_amount
) values (
  '10000000-0000-4000-8000-000000009102',
  '10000000-0000-4000-8000-000000009101',
  '10000000-0000-4000-8000-000000000002',
  'SELECT-A','Signed return candidate','READY','PENDING',37.39,37.39
);
insert into public.banking_pay_operations(
  id,operation_type,status,phase,actor_user_id,workbench_session_id,
  pay_batch_id,idempotency_key
) values (
  '10000000-0000-4000-8000-000000009103','DRAFT_CREATE','RUNNING',
  'FINALISE_RESERVATIONS','10000000-0000-4000-8000-000000000001',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000009101','signed-return-finalizer-fixture'
);
insert into public.banking_pay_operation_candidate_scope(
  id,operation_id,workbench_session_id,source_snapshot_run_id,
  source_session_version,candidate_id,pay_channel,pay_batch_id,scope_hash,
  chunk_sequence,status
) values (
  '10000000-0000-4000-8000-000000009104',
  '10000000-0000-4000-8000-000000009103',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000000004',1,
  '10000000-0000-4000-8000-000000000002','PAYE',
  '10000000-0000-4000-8000-000000009101','signed-return-scope',1,'DRAFTED'
);

do $fixture$
declare
  component jsonb;
  expected_seal text;
begin
  expected_seal:=md5(jsonb_build_object(
    'sealed_evidence_version',2,
    'financial_revision_digest','financial-digest-fixture',
    'target_authority_digest','target-digest-fixture',
    'conversion_context_digest','conversion-digest-fixture',
    'physical_bucket_digest','physical-digest-fixture',
    'economic_key_type','TS_DAY','economic_key_value','2026-03-13',
    'truth_ex_vat',round(0::numeric,2),
    'baseline_ex_vat',round(-37.39::numeric,2),
    'reserved_ex_vat',round(0::numeric,2)
  )::text);
  component:=jsonb_build_object(
    'component_key_type','TS_DAY','component_key_value','2026-03-13',
    'component_amount_ex_vat',37.39::numeric,
    'source_pay_ex_vat',37.39::numeric,'source_charge_ex_vat',0::numeric,
    'authoritative_truth_ex_vat',0::numeric,
    'authoritative_baseline_ex_vat',-37.39::numeric,
    'authoritative_reserved_ex_vat',0::numeric,
    'authoritative_outstanding_ex_vat',37.39::numeric,
    'physical_bucket_key','fixture-physical-bucket',
    'physical_bucket_digest','physical-digest-fixture',
    'financial_revision_digest','financial-digest-fixture',
    'target_authority_digest','target-digest-fixture',
    'conversion_context_digest','conversion-digest-fixture',
    'sealed_evidence_digest',expected_seal,
    'source_family_key','timesheet:10000000-0000-4000-8000-000000009001',
    'source_pay_method','PAYE','current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object(
      'component_fallback','WORKED_TIME_AMOUNT','work_date','2026-03-13'
    )
  );
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,source_ref,description,
    amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,
    frozen_component_key_type,frozen_component_key_value,
    frozen_component_snapshot_json,frozen_source_basis_json,
    frozen_source_pay_method,frozen_target_pay_method,
    frozen_resolution_payload_json,frozen_source_amount,
    frozen_target_amount_ex_vat,frozen_target_amount_vat,
    frozen_target_amount_inc_vat,operation_source_key
  ) values (
    '10000000-0000-4000-8000-000000009105',
    '10000000-0000-4000-8000-000000009102','SEGMENT_DELTA',
    '10000000-0000-4000-8000-000000009001','signed-return-fixture',
    'Return earlier overpayment deduction',37.39,0,37.39,'PAYE',
    'TS_DAY','2026-03-13',
    jsonb_build_object(
      'component_key_type','TS_DAY','component_key_value','2026-03-13',
      'source_family_key','timesheet:10000000-0000-4000-8000-000000009001',
      'source_amount_ex_vat',17.39::numeric,'target_amount_ex_vat',37.39::numeric
    ),
    jsonb_build_object(
      'source_family_key','timesheet:10000000-0000-4000-8000-000000009001',
      'source_amount_ex_vat',17.39::numeric,'work_date','2026-03-13'
    ),
    'PAYE','PAYE',jsonb_build_object('case_components',jsonb_build_array(component)),
    17.39,37.39,0,37.39,'signed-return-source'
  );
end;
$fixture$;

insert into public.banking_pay_operation_candidate_allocation_rows(
  id,operation_id,candidate_scope_id,pay_batch_id,candidate_id,pay_channel,
  allocation_type,source_ref,operation_source_key,allocated_amount,status,
  pay_batch_item_id
) values (
  '10000000-0000-4000-8000-000000009106',
  '10000000-0000-4000-8000-000000009103',
  '10000000-0000-4000-8000-000000009104',
  '10000000-0000-4000-8000-000000009101',
  '10000000-0000-4000-8000-000000000002','PAYE','READY_ITEM',
  'signed-return-fixture','signed-return-source',37.39,'ITEM_CREATED',
  '10000000-0000-4000-8000-000000009105'
);

do $proof$
declare
  ordinary_source numeric;
  result jsonb;
begin
  select source_amount_ex_vat into strict ordinary_source
  from public._pay_batch_item_economic_components(
    null,array['10000000-0000-4000-8000-000000009105'::uuid]
  );
  if ordinary_source<>17.39 then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_FIXTURE_DID_NOT_REPRODUCE_LEGACY_SOURCE: %',ordinary_source;
  end if;

  result:=public.pay_batch_finalize_reservations_and_markers(
    '10000000-0000-4000-8000-000000009101','PAYE',
    '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-24',
    '10000000-0000-4000-8000-000000009103',
    '["10000000-0000-4000-8000-000000009104"]'::jsonb
  );
  if coalesce((result->>'ok')::boolean,false) is not true
     or coalesce((result->>'reservation_requested_amount_ex_vat')::numeric,0)<>37.39
     or coalesce((result->>'reservation_outstanding_before_batch_ex_vat')::numeric,0)<>37.39 then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_FINALIZER_DID_NOT_SETTLE: %',result;
  end if;
  if exists(
       select 1 from public.pay_bank_transfers
       where pay_batch_id='10000000-0000-4000-8000-000000009101'
     )
     or exists(
       select 1 from public.banking_pay_operation_provider_attempts
       where operation_id='10000000-0000-4000-8000-000000009103'
          or pay_batch_id='10000000-0000-4000-8000-000000009101'
     )
     or exists(
       select 1 from public.banking_pay_operation_settlement_scope
       where operation_id='10000000-0000-4000-8000-000000009103'
          or pay_batch_id='10000000-0000-4000-8000-000000009101'
     )
     or exists(
       select 1 from public.banking_pay_operation_remittance_scope
       where operation_id='10000000-0000-4000-8000-000000009103'
          or pay_batch_id='10000000-0000-4000-8000-000000009101'
     ) then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_FINALIZER_CROSSED_PROVIDER_BOUNDARY';
  end if;
end;
$proof$;

insert into public.pay_batches(
  id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id
) values (
  '10000000-0000-4000-8000-000000009201','2026-08-28','DRAFT',
  'MONZO_CSV','CSV','CSV','SANDBOX','PAYE',
  '10000000-0000-4000-8000-000000000001'
);
insert into public.pay_batch_candidates(
  id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
  paye_state,settlement_status,gross_preview,net_bank_amount
) values (
  '10000000-0000-4000-8000-000000009202',
  '10000000-0000-4000-8000-000000009201',
  '10000000-0000-4000-8000-000000000002',
  'SELECT-A','Signed return candidate','READY','PENDING',37.39,37.39
);
insert into public.banking_pay_operations(
  id,operation_type,status,phase,actor_user_id,workbench_session_id,
  pay_batch_id,idempotency_key
) values (
  '10000000-0000-4000-8000-000000009203','DRAFT_CREATE','RUNNING',
  'FINALISE_RESERVATIONS','10000000-0000-4000-8000-000000000001',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000009201','signed-return-duplicate-fixture'
);
insert into public.banking_pay_operation_candidate_scope(
  id,operation_id,workbench_session_id,source_snapshot_run_id,
  source_session_version,candidate_id,pay_channel,pay_batch_id,scope_hash,
  chunk_sequence,status
) values (
  '10000000-0000-4000-8000-000000009204',
  '10000000-0000-4000-8000-000000009203',
  '10000000-0000-4000-8000-000000000005',
  '10000000-0000-4000-8000-000000000004',1,
  '10000000-0000-4000-8000-000000000002','PAYE',
  '10000000-0000-4000-8000-000000009201','signed-return-duplicate-scope',1,'DRAFTED'
);
insert into public.pay_batch_items(
  id,pay_batch_candidate_id,item_type,timesheet_id,source_ref,description,
  amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,umbrella_id,bank_reference,
  repayment_week_start,is_voided,is_mismatch,finance_case_id,paye_treatment,
  finance_component_id,frozen_component_snapshot_json,
  frozen_component_key_type,frozen_component_key_value,
  frozen_component_classification,frozen_source_basis_json,
  frozen_source_pay_method,frozen_target_pay_method,frozen_resolution_mode,
  frozen_resolution_payload_json,frozen_resolution_result_json,
  frozen_source_amount,frozen_target_amount_ex_vat,frozen_target_amount_vat,
  frozen_target_amount_inc_vat,payout_instruction_snapshot_json,
  operation_source_key
)
select
  '10000000-0000-4000-8000-000000009205',
  '10000000-0000-4000-8000-000000009202',item_type,timesheet_id,
  source_ref,description,amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,
  umbrella_id,bank_reference,repayment_week_start,is_voided,is_mismatch,
  finance_case_id,paye_treatment,finance_component_id,
  frozen_component_snapshot_json,frozen_component_key_type,
  frozen_component_key_value,frozen_component_classification,
  frozen_source_basis_json,frozen_source_pay_method,frozen_target_pay_method,
  frozen_resolution_mode,frozen_resolution_payload_json,
  frozen_resolution_result_json,frozen_source_amount,
  frozen_target_amount_ex_vat,frozen_target_amount_vat,
  frozen_target_amount_inc_vat,payout_instruction_snapshot_json,
  'signed-return-duplicate-source'
from public.pay_batch_items
where id='10000000-0000-4000-8000-000000009105';
insert into public.banking_pay_operation_candidate_allocation_rows(
  id,operation_id,candidate_scope_id,pay_batch_id,candidate_id,pay_channel,
  allocation_type,source_ref,operation_source_key,allocated_amount,status,
  pay_batch_item_id
) values (
  '10000000-0000-4000-8000-000000009206',
  '10000000-0000-4000-8000-000000009203',
  '10000000-0000-4000-8000-000000009204',
  '10000000-0000-4000-8000-000000009201',
  '10000000-0000-4000-8000-000000000002','PAYE','READY_ITEM',
  'signed-return-duplicate-fixture','signed-return-duplicate-source',37.39,
  'ITEM_CREATED','10000000-0000-4000-8000-000000009205'
);

do $duplicate_and_cancel$
declare
  result jsonb;
begin
  begin
    perform public.pay_batch_finalize_reservations_and_markers(
      '10000000-0000-4000-8000-000000009201','PAYE',
      '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-24',
      '10000000-0000-4000-8000-000000009203',
      '["10000000-0000-4000-8000-000000009204"]'::jsonb
    );
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_ACTIVE_DUPLICATE_ACCEPTED';
  exception when others then
    if position('PAY_BATCH_RESERVATION_OVERRUN' in sqlerrm)=0 then raise; end if;
  end;

  update public.pay_batches set status='CANCELLED',cancelled_at_utc=clock_timestamp(),
    cancel_reason='ROLLBACK_FIXTURE_ONLY'
  where id='10000000-0000-4000-8000-000000009101';

  result:=public.pay_batch_finalize_reservations_and_markers(
    '10000000-0000-4000-8000-000000009201','PAYE',
    '10000000-0000-4000-8000-000000000001','2026-08-28','2026-08-24',
    '10000000-0000-4000-8000-000000009203',
    '["10000000-0000-4000-8000-000000009204"]'::jsonb
  );
  if coalesce((result->>'ok')::boolean,false) is not true
     or coalesce((result->>'reservation_requested_amount_ex_vat')::numeric,0)<>37.39
     or coalesce((result->>'reservation_outstanding_before_batch_ex_vat')::numeric,0)<>37.39 then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_CANCELLED_RELEASE_DID_NOT_SETTLE: %',result;
  end if;
  if exists(
       select 1 from public.pay_bank_transfers
       where pay_batch_id in (
         '10000000-0000-4000-8000-000000009101',
         '10000000-0000-4000-8000-000000009201'
       )
     )
     or exists(
       select 1 from public.banking_pay_operation_provider_attempts
       where operation_id in (
         '10000000-0000-4000-8000-000000009103',
         '10000000-0000-4000-8000-000000009203'
       )
          or pay_batch_id in (
            '10000000-0000-4000-8000-000000009101',
            '10000000-0000-4000-8000-000000009201'
          )
     )
     or exists(
       select 1 from public.banking_pay_operation_settlement_scope
       where operation_id in (
         '10000000-0000-4000-8000-000000009103',
         '10000000-0000-4000-8000-000000009203'
       )
          or pay_batch_id in (
            '10000000-0000-4000-8000-000000009101',
            '10000000-0000-4000-8000-000000009201'
          )
     )
     or exists(
       select 1 from public.banking_pay_operation_remittance_scope
       where operation_id in (
         '10000000-0000-4000-8000-000000009103',
         '10000000-0000-4000-8000-000000009203'
       )
          or pay_batch_id in (
            '10000000-0000-4000-8000-000000009101',
            '10000000-0000-4000-8000-000000009201'
          )
     ) then
    raise exception 'BANKING_PAY_SIGNED_RECOVERY_DUPLICATE_PROOF_CROSSED_PROVIDER_BOUNDARY';
  end if;
end;
$duplicate_and_cancel$;

rollback;
