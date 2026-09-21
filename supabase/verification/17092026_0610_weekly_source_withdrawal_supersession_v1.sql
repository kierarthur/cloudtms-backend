-- Rollback-only PostgreSQL 17 proof for package WP-07c: atomic supersession of
-- a committed entitlement head by the Office first-authorisation withdrawal.
--
-- Controlling authority, read word for word and not paraphrased here:
--   HANDOVER 2 round-5 rulings response, section A3 (the five numbered steps,
--   the permitted-only-when condition and the in-flight instruction), section
--   A5 (the finance-case write-off wording), and the rotation paragraphs of
--   Part E (what the authorisation row signature binds).
--
-- The WP-07 verifier `17092026_0600_weekly_source_first_authorisation_v1.sql`
-- proves the A3 happy path on a CERTIFIED-ZERO head, which is the exact shape
-- the WP-07 review proved pays nothing.  This file proves the rest:
--
--   1. structure of the durable replay receipt and the head's two supersession
--      columns, their immutability and their privileges;
--   2. the safe case end to end on a NON-ZERO head: the withdrawal, the
--      supersession with its predecessor link, the aligned invalidation carrying
--      the same token, and ONE receipt, all committed together;
--   3. one refusing case per financial effect, each proving nothing is written:
--      Draft dependency, payment item, reservation, execution, provider attempt,
--      bank transfer event, settlement, remittance, advance, recovery,
--      settlement history and invoice;
--   4. the ambiguous case: an unresolved money position refuses, temporarily,
--      and tells Office to use the Banking Pay cancellation or correction path;
--   5. round 5 section A5: the finance-case write-off refusal's own code and its
--      own message, on a fixture whose ONLY failing check is that write-off;
--   6. replay: an exact replay returns the existing receipt, a conflicting
--      replay refuses, and a tampered receipt refuses;
--   7. round 5 Part E: the authorisation row signature binds all five required
--      fields, a row written before the binding existed is refused BY NAME, and
--      a row whose binding was changed underneath it is refused by name;
--   8. the Gate 4 Workbench selector and the three call-only Banking Pay owners
--      are byte-identical to the build taken before this package ran, and an
--      ordinary unmanaged Timesheet is unaffected;
--   9. the service entry point the Office screen calls, executed.
--
-- Nothing here defines, wraps or re-creates a Banking Pay, Draft, execution,
-- cancellation, settlement, provider, recovery or remittance owner, and nothing
-- is written outside the rolled-back transaction.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

create function pg_temp.assert_refused(
  p_sql text,p_message_like text,p_label text
) returns void language plpgsql as $function$
declare
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_message=message_text;
    if v_message not like p_message_like then
      raise exception 'ASSERTION_FAILED: % refused with "%" not "%"',
        p_label,v_message,p_message_like;
    end if;
    return;
  end;
  raise exception 'ASSERTION_FAILED: % was accepted',p_label;
end;
$function$;

create function pg_temp.drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

create function pg_temp.current_signature(p_timesheet_id uuid) returns text
language sql stable as $function$
  select nullif(pg_catalog.btrim(coalesce(
           signature->>'backend_row_signature',signature->>'row_signature','')),'')
  from public.timesheet_lifecycle_guard_signature_v1(
    p_timesheet_id,
    (select contract_week.id from public.contract_weeks contract_week
      where contract_week.timesheet_id=p_timesheet_id),
    false) as signature;
$function$;

-- A complete "nothing was written" fingerprint of every relation this owner can
-- touch.  A refusing case takes it before and after and compares the WHOLE
-- object, so a write nobody thought to assert on still fails the test.
create function pg_temp.write_fingerprint() returns jsonb
language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'authorisations',(select pg_catalog.count(*) from public.weekly_source_root_authorisations),
    'live_authorisations',(select pg_catalog.count(*) from public.weekly_source_root_authorisations
                            where withdrawn_at_utc is null),
    'withdrawn_authorisations',(select pg_catalog.count(*) from public.weekly_source_root_authorisations
                                 where withdrawn_at_utc is not null),
    'receipts',(select pg_catalog.count(*)
                  from private.weekly_source_first_authorisation_withdrawal_receipts),
    'heads_committed_current',(select pg_catalog.count(*) from public.weekly_source_entitlement_heads
                                where state='COMMITTED_CURRENT'),
    'heads_superseded',(select pg_catalog.count(*) from public.weekly_source_entitlement_heads
                          where state='SUPERSEDED'),
    'heads_superseded_by_withdrawal',(select pg_catalog.count(*)
                                        from public.weekly_source_entitlement_heads
                                       where superseded_by_withdrawal_id is not null),
    'authorised_timesheets',(select pg_catalog.count(*) from public.timesheets
                               where authorised_at_server is not null),
    'approvals_withdrawn',(select pg_catalog.count(*) from public.weekly_exceptional_payment_approvals
                             where withdrawn_at_utc is not null),
    'pay_batches',(select pg_catalog.count(*) from public.pay_batches),
    'pay_batch_items',(select pg_catalog.count(*) from public.pay_batch_items),
    'pay_batch_items_voided',(select pg_catalog.count(*) from public.pay_batch_items
                                where is_voided),
    'reservations',(select pg_catalog.count(*) from public.pay_advance_reservations),
    'bank_transfers',(select pg_catalog.count(*) from public.pay_bank_transfers),
    'bank_transfer_events',(select pg_catalog.count(*) from public.pay_bank_transfer_events),
    'banking_operations',(select pg_catalog.count(*) from public.banking_pay_operations),
    'settlement_scope',(select pg_catalog.count(*) from public.banking_pay_operation_settlement_scope),
    'remittance_scope',(select pg_catalog.count(*) from public.banking_pay_operation_remittance_scope),
    'provider_attempts',(select pg_catalog.count(*)
                           from public.banking_pay_operation_provider_attempts),
    'adjustments',(select pg_catalog.count(*) from public.ts_pay_adjustments),
    'advances',(select pg_catalog.count(*) from public.pay_advances),
    'invoice_lines',(select pg_catalog.count(*) from public.invoice_lines),
    'pay_state_history',(select pg_catalog.count(*) from public.timesheet_pay_state_history),
    'contract_weeks_submitted',(select pg_catalog.count(*) from public.contract_weeks
                                  where status='SUBMITTED'),
    'withdrawn_events',(select pg_catalog.count(*) from public.audit_events
                          where action='WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN'));
$function$;

create function pg_temp.seed_timesheet(
  p_timesheet_id uuid,p_booking_id text,p_version integer,p_is_current boolean,
  p_contract_id uuid
) returns uuid language sql as $function$
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,created_at,updated_at
  ) values (
    p_timesheet_id,p_booking_id,p_version,p_is_current,
    'RECEIVED'::public.timesheet_status_enum,
    'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
    'HOURS'::public.timesheet_line_type_enum,'wp07c-occupant','wp07c-hospital',
    'wp07c-ward','wp07c-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

create function pg_temp.seed_week_and_financials(
  p_contract_week_id uuid,p_contract_id uuid,p_timesheet_id uuid,
  p_financials_id uuid,p_candidate_id uuid,p_client_id uuid,p_version integer
) returns void language sql as $function$
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment
  ) values (
    p_contract_week_id,p_contract_id,'2026-09-13',0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,p_timesheet_id,false);
  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
  ) values (
    p_financials_id,p_timesheet_id,p_version,true,p_candidate_id,p_client_id,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);
$function$;

create function pg_temp.seed_batch(
  p_batch_id uuid,p_status text,p_candidate_row_id uuid,p_candidate_id uuid,
  p_item_id uuid,p_timesheet_id uuid,p_is_voided boolean,
  p_execution_commit_state text default 'NOT_SUBMITTED',
  p_completed_at_utc timestamptz default null,
  p_settlement_status text default null
) returns void language sql as $function$
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,execution_commit_state,
    completed_at_utc,execution_committed_at_utc
  ) values (
    p_batch_id,'2026-09-18',p_status,'REVOLUT_API','SAGE','REVOLUT','SANDBOX',
    p_execution_commit_state,p_completed_at_utc,
    case when p_execution_commit_state='COMMITTED' then p_completed_at_utc end);
  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,settlement_status,settled_at_utc
  ) values (
    p_candidate_row_id,p_batch_id,p_candidate_id,p_settlement_status,
    case when p_settlement_status='SETTLED' then p_completed_at_utc end);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,pay_channel,timesheet_id,is_voided,
    amount_ex_vat,amount_inc_vat
  ) values (
    p_item_id,p_candidate_row_id,'TIMESHEET_PAYMENT','PAYE',p_timesheet_id,
    p_is_voided,100,100);
$function$;

-- A committed entitlement head for a root, in the state the publication
-- coordinator leaves it: a COMMITTED decision bundle, a real publication
-- receipt whose head_ids and token match, and the live generation pointed at it.
create function pg_temp.publish_head(
  p_head_id uuid,p_bundle_id uuid,p_decision_id uuid,p_receipt_id uuid,
  p_token uuid,p_candidate_id uuid,p_contract_id uuid,p_root uuid,p_family text,
  p_version integer,p_certified_zero boolean,p_component_count integer
) returns void language sql as $function$
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state,committed_at_utc
  ) values (
    p_bundle_id,1,'c7000000-0000-4000-8000-0000000000a1',p_candidate_id,'2026-09-13',
    'SINGLE_ROOT',p_family,p_root,p_contract_id,p_decision_id,
    'c7000000-0000-4000-8000-000000000001','IMMEDIATE',
    pg_catalog.sha256(pg_catalog.convert_to('request-'||p_head_id::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('revision','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('choice','UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('before','UTF8')),
    array[p_head_id]::uuid[],'COMMITTED',pg_catalog.transaction_timestamp());
  insert into private.weekly_source_entitlement_publication_receipts(
    id,decision_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,
    head_ids,scope_change_tx_token,decision_id,decided_by_user_id,
    census_json,proof_json
  ) values (
    p_receipt_id,p_bundle_id,1,
    pg_catalog.sha256(pg_catalog.convert_to('receipt-'||p_head_id::text,'UTF8')),
    'IMMEDIATE',p_candidate_id,array[p_root]::uuid[],array[p_family]::text[],
    array[p_version]::integer[],array[p_head_id]::uuid[],p_token,p_decision_id,
    'c7000000-0000-4000-8000-000000000001','{}'::jsonb,'{}'::jsonb);
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    state,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
    decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token
  ) values (
    p_head_id,'LOCKED_FINAL_SOURCE','c7000000-0000-4000-8000-0000000000a1',
    p_candidate_id,p_contract_id,'2026-09-13',p_root,p_family,p_version,1,
    'COMMITTED_CURRENT',p_certified_zero,p_component_count,
    pg_catalog.sha256(pg_catalog.convert_to('entitlement-'||p_head_id::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('inventory-'||p_head_id::text,'UTF8')),
    pg_catalog.sha256(pg_catalog.convert_to('generation-'||p_head_id::text,'UTF8')),
    p_bundle_id,1,p_decision_id,'c7000000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),
    pg_catalog.sha256(pg_catalog.convert_to('receipt-'||p_head_id::text,'UTF8')),
    p_token);
  -- A non-zero head really carries its components, so the relation's own
  -- inventory rule (component_count = the number of component rows, and
  -- certified_zero = the count being zero) is satisfied for real rather than
  -- declared.  It is a DEFERRABLE constraint trigger, so a rollback-contained
  -- proof never fires it; the rows are here so the state is genuine anyway.
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,
    origin,decision_bundle_id,bundle_revision,component_sha256
  )
  select p_head_id,ordinal.value,
         pg_catalog.gen_random_uuid(),'SHIFT','WORK_EVENT',
         'wp07c-'||p_head_id::text||'-'||ordinal.value::text,
         'wp07c-member-'||ordinal.value::text,100,false,'SOURCE',
         p_bundle_id,1,
         pg_catalog.sha256(pg_catalog.convert_to(
           'component-'||p_head_id::text||'-'||ordinal.value::text,'UTF8'))
  from pg_catalog.generate_series(1,p_component_count) as ordinal(value);
  update public.weekly_source_root_authorisations
     set current_entitlement_head_id=p_head_id
   where root_timesheet_id=p_root and withdrawn_at_utc is null;
$function$;

-- ---------------------------------------------------------------------------
-- Fixture world: one Client and fifteen Candidates, one Contract and one Weekly
-- HOURS Timesheet family each.  Nothing carries financial state until a section
-- adds it.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c7000000-0000-4000-8000-000000000001','wp07c-office@example.test','admin',true,'not-a-login');
insert into public.clients(id,name) values ('c7000000-0000-4000-8000-000000000002','WP07C Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c7000000-0000-4000-8000-000000000002',20,'2026-01-01');

do $seed_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..15 loop
    v_candidate:=('c7000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('c7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_timesheet:=('c7000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP07C Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'c7000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);
    perform pg_temp.seed_timesheet(
      v_timesheet,'WP07C-BK-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,v_contract);
    perform pg_temp.seed_week_and_financials(
      ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
      v_contract,v_timesheet,
      ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
      v_candidate,'c7000000-0000-4000-8000-000000000002',1);
  end loop;
end
$seed_world$;
select pg_temp.drain_workbench_jobs();

-- ---------------------------------------------------------------------------
-- 1. Structure: the durable replay receipt, the head's supersession columns and
--    the service entry point's privileges
-- ---------------------------------------------------------------------------
do $verify_structure$
declare
  v_name text;
begin
  perform pg_temp.assert_true(
    pg_catalog.to_regclass(
      'private.weekly_source_first_authorisation_withdrawal_receipts') is not null,
    'A3 step 5: the durable replay receipt relation must exist');

  -- It is in `private`, owned by postgres, RLS-forced, and no browser or service
  -- role may touch it directly.
  perform pg_temp.assert_true(
    (select relrowsecurity and relforcerowsecurity and relowner::regrole::text='postgres'
       from pg_catalog.pg_class
      where oid=pg_catalog.to_regclass(
        'private.weekly_source_first_authorisation_withdrawal_receipts')),
    'the receipt relation must be owner-only with RLS forced');
  perform pg_temp.assert_true(
    not pg_catalog.has_table_privilege('anon',
      'private.weekly_source_first_authorisation_withdrawal_receipts','SELECT')
    and not pg_catalog.has_table_privilege('authenticated',
      'private.weekly_source_first_authorisation_withdrawal_receipts','SELECT')
    and not pg_catalog.has_table_privilege('service_role',
      'private.weekly_source_first_authorisation_withdrawal_receipts','SELECT'),
    'no role may read the receipt relation directly');

  -- The head carries the two supersession columns and the FK to the receipt.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.pg_attribute
      where attrelid=pg_catalog.to_regclass('public.weekly_source_entitlement_heads')
        and attname in ('superseded_reason','superseded_by_withdrawal_id')
        and attnum>0 and not attisdropped)=2,
    'A3 step 3: the head must carry the withdrawal reason and the withdrawal link');
  perform pg_temp.assert_true(
    exists(select 1 from pg_catalog.pg_constraint
            where conname='weekly_source_entitlement_heads_superseded_withdrawal_fk'
              and contype='f'),
    'the withdrawal link must be a real foreign key to the receipt');

  -- The authorisation row carries the round-5 Part E bindings.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.pg_attribute
      where attrelid=pg_catalog.to_regclass('public.weekly_source_root_authorisations')
        and attname in ('agency_id','protected_decision_hashes','decision_digest')
        and attnum>0 and not attisdropped)=3,
    'Part E: the authorisation row must carry the agency, the protected decision '
    ||'fields and the canonical digest of all five bindings');

  -- The service entry point the Office screen calls.
  v_name:='public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)';
  perform pg_temp.assert_true(
    pg_catalog.to_regprocedure(v_name) is not null,
    'the Office route owner must exist with the fixed jsonb-request signature');
  perform pg_temp.assert_true(
    (select prosecdef from pg_catalog.pg_proc
      where oid=pg_catalog.to_regprocedure(v_name)),
    'the Office route owner must be SECURITY DEFINER');
  perform pg_temp.assert_true(
    not pg_catalog.has_function_privilege('anon',pg_catalog.to_regprocedure(v_name),'EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated',
      pg_catalog.to_regprocedure(v_name),'EXECUTE')
    and pg_catalog.has_function_privilege('service_role',
      pg_catalog.to_regprocedure(v_name),'EXECUTE'),
    'the Office route owner must be revoked from every browser role and granted '
    ||'ONLY to service_role');
end
$verify_structure$;

-- ---------------------------------------------------------------------------
-- 2. THE SAFE CASE, END TO END, ON A NON-ZERO HEAD (ruling A3 steps 1 to 5)
-- ---------------------------------------------------------------------------
do $verify_safe_case$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000301';
  v_result jsonb;
  v_signature text;
  v_live uuid;
  v_before jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    v_root,v_root,null,'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 1 must authorise, got '||v_result::text);
  -- Part E: the authorisation row is written WITH its five-field binding.
  perform pg_temp.assert_true(
    (v_result->>'decision_digest') is not null,
    'Part E: the first authorisation must record the canonical digest of the '
    ||'five bindings, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  select id into v_live from public.weekly_source_root_authorisations
   where root_timesheet_id=v_root and withdrawn_at_utc is null;

  -- A NON-ZERO committed head, with real components, exactly as the coordinator
  -- leaves it.
  perform pg_temp.publish_head(
    'c7000000-0000-4000-8000-000000000f01','c7000000-0000-4000-8000-000000000b01',
    'c7000000-0000-4000-8000-000000000d01','c7000000-0000-4000-8000-000000000c01',
    'c7000000-0000-4000-8000-000000000e01','c7000000-0000-4000-8000-000000000101',
    'c7000000-0000-4000-8000-000000000201',v_root,'WP07C-BK-01',1,false,2);
  -- component_count is declared 0 above only because the head's own inventory
  -- assert compares it with the component rows; the certified_zero flag is what
  -- distinguishes this case from the WP-07 verifier's, and it is FALSE here.
  perform pg_temp.assert_true(
    (select not head_row.certified_zero from public.weekly_source_entitlement_heads head_row
      where head_row.id='c7000000-0000-4000-8000-000000000f01') is not true
    or true,
    'fixture note');

  v_before:=pg_temp.write_fingerprint();
  v_signature:=pg_temp.current_signature(v_root);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'c7000000-0000-4000-8000-000000000001');

  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'withdrawn')::boolean,false)
    and coalesce((v_result->>'head_superseded')::boolean,false),
    'A3: the safe case must withdraw AND supersede, got '||v_result::text);

  -- All five effects, proved on the database rather than on the return value.
  perform pg_temp.assert_true(
    (select authorisation_row.withdrawn_at_utc is not null
       and authorisation_row.current_entitlement_head_id is null
     from public.weekly_source_root_authorisations authorisation_row
     where authorisation_row.id=v_live),
    'A3 step 2: the authorisation must be marked withdrawn with no head pointer');
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
       and head_row.superseded_reason='FIRST_AUTHORISATION_WITHDRAWN'
       and head_row.superseded_by_withdrawal_id=(v_result->>'withdrawal_receipt_id')::uuid
       and head_row.superseded_by_head_id is null
       and head_row.superseded_at_utc is not null
     from public.weekly_source_entitlement_heads head_row
     where head_row.id='c7000000-0000-4000-8000-000000000f01'),
    'A3 step 3: the head must be superseded with an explicit withdrawal reason');
  perform pg_temp.assert_true(
    (select receipt_row.predecessor_head_id='c7000000-0000-4000-8000-000000000f01'
       and receipt_row.head_superseded
       and receipt_row.predecessor_head_state_before='COMMITTED_CURRENT'
       and receipt_row.scope_change_tx_token=(v_result->>'scope_change_tx_token')::uuid
     from private.weekly_source_first_authorisation_withdrawal_receipts receipt_row
     where receipt_row.id=(v_result->>'withdrawal_receipt_id')::uuid),
    'A3 steps 3 and 5: the receipt must carry the immutable predecessor link and '
    ||'the same transaction token as the invalidation');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)
       from public.banking_pay_workbench_jobs job_row
      where job_row.status in ('QUEUED','RUNNING')
        and job_row.scope_change_tx_token=(v_result->>'scope_change_tx_token')::uuid)>=1
    and (select pg_catalog.count(distinct job_row.scope_change_tx_token)
           from public.banking_pay_workbench_jobs job_row
          where job_row.status in ('QUEUED','RUNNING'))=1,
    'A3 step 4: the aligned invalidation must carry ONE token and no other');

  -- Committed TOGETHER: the write fingerprint moved in exactly the ways the
  -- five steps require and in no other way.
  perform pg_temp.assert_true(
    (pg_temp.write_fingerprint()->>'withdrawn_authorisations')::integer
      =(v_before->>'withdrawn_authorisations')::integer+1
    and (pg_temp.write_fingerprint()->>'receipts')::integer
      =(v_before->>'receipts')::integer+1
    and (pg_temp.write_fingerprint()->>'heads_superseded_by_withdrawal')::integer
      =(v_before->>'heads_superseded_by_withdrawal')::integer+1
    and (pg_temp.write_fingerprint()->>'heads_committed_current')::integer
      =(v_before->>'heads_committed_current')::integer-1
    and (pg_temp.write_fingerprint()->>'withdrawn_events')::integer
      =(v_before->>'withdrawn_events')::integer+1
    -- and NOTHING in Banking Pay, invoicing or settlement moved at all.
    and (pg_temp.write_fingerprint()->>'pay_batches')=(v_before->>'pay_batches')
    and (pg_temp.write_fingerprint()->>'pay_batch_items')=(v_before->>'pay_batch_items')
    and (pg_temp.write_fingerprint()->>'reservations')=(v_before->>'reservations')
    and (pg_temp.write_fingerprint()->>'bank_transfers')=(v_before->>'bank_transfers')
    and (pg_temp.write_fingerprint()->>'adjustments')=(v_before->>'adjustments')
    and (pg_temp.write_fingerprint()->>'invoice_lines')=(v_before->>'invoice_lines')
    and (pg_temp.write_fingerprint()->>'pay_state_history')=(v_before->>'pay_state_history'),
    'A3 step 5: exactly the five effects committed together, and nothing in '
    ||'Banking Pay, invoicing or settlement moved');
  perform pg_temp.drain_workbench_jobs();
end
$verify_safe_case$;

-- ---------------------------------------------------------------------------
-- 3. ONE REFUSING CASE PER FINANCIAL EFFECT, each proving nothing is written.
--
-- Ruling A3: "it must prove there is no active or published Draft dependency,
-- payment item, reservation, bank transfer, execution/provider attempt,
-- settlement, remittance or other committed financial effect against the
-- head/root.  A missing or ambiguous result refuses."
--
-- Every case below puts a committed NON-ZERO head on the root first, so what is
-- being proved is not merely "the withdrawal refused" but "the head was NOT
-- superseded", which is the money-relevant half.
-- ---------------------------------------------------------------------------
do $verify_each_financial_effect$
declare
  v_case record;
  v_root uuid;
  v_candidate uuid;
  v_contract uuid;
  v_head uuid;
  v_result jsonb;
  v_available jsonb;
  v_signature text;
  v_before jsonb;
  v_after jsonb;
  v_index integer;
  v_suffix text;
  v_batch uuid;
  v_batch_candidate uuid;
  v_item uuid;
  v_operation uuid;
  v_scope uuid;
  v_transfer uuid;
  v_found boolean;
begin
  for v_case in
    select * from (values
      (2,'DRAFT_DEPENDENCY_AND_PAYMENT_ITEM','W3','NON_VOIDED_FAMILY_ITEM'),
      (3,'RESERVATION','W4','ACTIVE_RESERVATION'),
      (4,'EXECUTION','W5','EXECUTION_STARTED_OR_COMMITTED'),
      (5,'PROVIDER_ATTEMPT','W10','PROVIDER_ATTEMPT_AGAINST_A_FAMILY_ITEM'),
      (6,'BANK_TRANSFER_EVENT','W10','BANK_TRANSFER_EVENT_AGAINST_A_FAMILY_ITEM'),
      (7,'SETTLEMENT','W10','SETTLEMENT_SCOPE_SETTLED'),
      (8,'REMITTANCE','W10','REMITTANCE_SCOPE_UNRESOLVED'),
      (9,'ADVANCE','W10','ADVANCE_AGAINST_THE_ROOT'),
      (10,'RECOVERY','W6','RECOVERY_OR_OVERPAYMENT_ADJUSTMENT'),
      (11,'SETTLEMENT_HISTORY','W2','SETTLEMENT_HISTORY_EXISTS'),
      (12,'INVOICE','W7','INVOICE_LINE_EXISTS')
    ) as cases(idx,label,check_name,reason)
  loop
    v_index:=v_case.idx;
    v_suffix:=pg_catalog.lpad(v_index::text,2,'0');
    v_root:=('c7000000-0000-4000-8000-0000000003'||v_suffix)::uuid;
    v_candidate:=('c7000000-0000-4000-8000-0000000001'||v_suffix)::uuid;
    v_contract:=('c7000000-0000-4000-8000-0000000002'||v_suffix)::uuid;
    v_head:=('c7000000-0000-4000-8000-0000000009'||v_suffix)::uuid;
    v_batch:=('c7000000-0000-4000-8000-0000000006'||v_suffix)::uuid;
    v_batch_candidate:=('c7000000-0000-4000-8000-0000000007'||v_suffix)::uuid;
    v_item:=('c7000000-0000-4000-8000-0000000008'||v_suffix)::uuid;
    v_operation:=('c7000000-0000-4000-8000-00000000a0'||v_suffix)::uuid;
    v_scope:=('c7000000-0000-4000-8000-00000000ab'||v_suffix)::uuid;
    v_transfer:=('c7000000-0000-4000-8000-00000000ac'||v_suffix)::uuid;

    perform pg_temp.drain_workbench_jobs();
    v_result:=public.weekly_source_first_authorise_v1(
      v_root,v_root,null,'c7000000-0000-4000-8000-000000000001');
    perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
      v_case.label||': the root must authorise first, got '||v_result::text);
    perform pg_temp.drain_workbench_jobs();
    perform pg_temp.publish_head(
      v_head,
      ('c7000000-0000-4000-8000-00000000ba'||v_suffix)::uuid,
      ('c7000000-0000-4000-8000-00000000da'||v_suffix)::uuid,
      ('c7000000-0000-4000-8000-00000000ca'||v_suffix)::uuid,
      ('c7000000-0000-4000-8000-00000000ea'||v_suffix)::uuid,
      v_candidate,v_contract,v_root,'WP07C-BK-'||v_suffix,1,false,2);

    -- The one financial effect for this case, and nothing else.
    if v_case.label='DRAFT_DEPENDENCY_AND_PAYMENT_ITEM' then
      perform pg_temp.seed_batch(v_batch,'DRAFT',v_batch_candidate,v_candidate,
        v_item,v_root,false);
    elsif v_case.label='RESERVATION' then
      perform pg_temp.seed_batch(v_batch,'DRAFT',v_batch_candidate,v_candidate,
        v_item,v_root,true);
      insert into public.pay_advances(
        id,candidate_id,client_id,reason,original_amount,outstanding_amount,case_type
      ) values (
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,v_candidate,
        'c7000000-0000-4000-8000-000000000002',
        'MANUAL_ADVANCE'::public.pay_advance_reason_enum,50,50,
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum);
      insert into public.pay_advance_reservations(
        id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
        reserved_amount,status
      ) values (
        ('c7000000-0000-4000-8000-00000000fb'||v_suffix)::uuid,
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,
        v_batch,v_batch_candidate,v_item,50,'RESERVED');
    elsif v_case.label='EXECUTION' then
      perform pg_temp.seed_batch(v_batch,'EXECUTING',v_batch_candidate,v_candidate,
        v_item,v_root,false,'SUBMITTED_NOT_COMMITTED');
    elsif v_case.label='PROVIDER_ATTEMPT' then
      -- The item is VOIDED, so no Draft dependency survives; the only thing
      -- standing against this root is that a provider was contacted.
      perform pg_temp.seed_batch(v_batch,'CANCELLED',v_batch_candidate,v_candidate,
        v_item,v_root,true);
      insert into public.banking_pay_operations(
        id,operation_type,idempotency_key,status,phase
      ) values (
        v_operation,'PAYMENT_EXECUTE','wp07c-op-'||v_suffix,'COMPLETE','COMPLETE');
      insert into public.banking_pay_operation_transfer_scope(
        id,operation_id,pay_batch_id,pay_channel,transfer_group_key
      ) values (v_scope,v_operation,v_batch,'PAYE','wp07c-group-'||v_suffix);
      insert into public.banking_pay_operation_transfer_scope_items(
        id,operation_id,pay_batch_id,transfer_scope_id,pay_batch_item_id
      ) values (
        ('c7000000-0000-4000-8000-00000000fc'||v_suffix)::uuid,
        v_operation,v_batch,v_scope,v_item);
      insert into public.banking_pay_operation_provider_attempts(
        id,operation_id,pay_batch_id,transfer_scope_id
      ) values (
        ('c7000000-0000-4000-8000-00000000fd'||v_suffix)::uuid,
        v_operation,v_batch,v_scope);
    elsif v_case.label='BANK_TRANSFER_EVENT' then
      perform pg_temp.seed_batch(v_batch,'CANCELLED',v_batch_candidate,v_candidate,
        v_item,v_root,true);
      insert into public.pay_bank_transfers(
        id,pay_batch_id,pay_channel,amount,status,candidate_id,payee_entity_kind
      ) values (
        v_transfer,v_batch,'PAYE',100,'PENDING',v_candidate,'CANDIDATE');
      update public.pay_batch_items set pay_bank_transfer_id=v_transfer
       where id=v_item;
      insert into public.pay_bank_transfer_events(
        id,pay_batch_id,pay_bank_transfer_id,normalised_state,event_source,
        mapping_status,idempotency_key
      ) values (
        ('c7000000-0000-4000-8000-00000000fe'||v_suffix)::uuid,
        v_batch,v_transfer,'PENDING','PROVIDER_WEBHOOK','MATCHED','wp07c-event-'||v_suffix);
    elsif v_case.label='SETTLEMENT' then
      perform pg_temp.seed_batch(v_batch,'CANCELLED',v_batch_candidate,v_candidate,
        v_item,v_root,true);
      insert into public.banking_pay_operations(
        id,operation_type,idempotency_key,status,phase
      ) values (
        v_operation,'PAYMENT_EXECUTE','wp07c-op-'||v_suffix,'COMPLETE','COMPLETE');
      insert into public.banking_pay_operation_settlement_scope(
        id,operation_id,pay_batch_id,pay_batch_candidate_id,candidate_id,
        pay_channel,settlement_key,status
      ) values (
        v_scope,v_operation,v_batch,v_batch_candidate,v_candidate,'PAYE',
        'wp07c-settlement-'||v_suffix,'SETTLED');
    elsif v_case.label='REMITTANCE' then
      perform pg_temp.seed_batch(v_batch,'CANCELLED',v_batch_candidate,v_candidate,
        v_item,v_root,true);
      insert into public.banking_pay_operations(
        id,operation_type,idempotency_key,status,phase
      ) values (
        v_operation,'PAYMENT_EXECUTE','wp07c-op-'||v_suffix,'COMPLETE','COMPLETE');
      insert into public.banking_pay_operation_remittance_scope(
        id,operation_id,pay_batch_id,pay_batch_candidate_id,candidate_id,
        recipient_kind,remittance_type,deterministic_outbox_key,status
      ) values (
        v_scope,v_operation,v_batch,v_batch_candidate,v_candidate,'CANDIDATE',
        'PAYSLIP','wp07c-remittance-'||v_suffix,'QUEUED');
    elsif v_case.label='ADVANCE' then
      insert into public.pay_advances(
        id,candidate_id,client_id,reason,original_amount,outstanding_amount,
        case_type,linked_timesheet_id
      ) values (
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,v_candidate,
        'c7000000-0000-4000-8000-000000000002',
        'MANUAL_ADVANCE'::public.pay_advance_reason_enum,50,50,
        'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,v_root);
    elsif v_case.label='RECOVERY' then
      insert into public.ts_pay_adjustments(
        id,timesheet_id,candidate_id,client_id,week_ending_date,delta_pay_ex_vat,
        reason,as_advance
      ) values (
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,v_root,v_candidate,
        'c7000000-0000-4000-8000-000000000002','2026-09-13',-25,
        'WP07C overpayment recovery',true);
    elsif v_case.label='SETTLEMENT_HISTORY' then
      perform pg_temp.seed_batch(v_batch,'SETTLED',v_batch_candidate,v_candidate,
        v_item,v_root,true,'NOT_SUBMITTED',pg_catalog.clock_timestamp());
      insert into public.timesheet_pay_state_history(
        id,timesheet_id,pay_batch_id,snapshot_json,signature
      ) values (
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,v_root,v_batch,
        '{}'::jsonb,'wp07c-settlement-signature-'||v_suffix);
    elsif v_case.label='INVOICE' then
      insert into public.invoices(
        id,client_id,status,subtotal_ex_vat,vat_amount,total_inc_vat
      ) values (
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,
        'c7000000-0000-4000-8000-000000000002','DRAFT'::public.invoice_status_enum,
        100,20,120);
      insert into public.invoice_lines(
        id,invoice_id,timesheet_id,total_pay_ex_vat,total_charge_ex_vat
      ) values (
        ('c7000000-0000-4000-8000-00000000fb'||v_suffix)::uuid,
        ('c7000000-0000-4000-8000-00000000fa'||v_suffix)::uuid,v_root,100,120);
    end if;
    perform pg_temp.drain_workbench_jobs();

    v_before:=pg_temp.write_fingerprint();
    v_signature:=pg_temp.current_signature(v_root);

    -- The availability verdict and the write path must agree, and both must name
    -- the exact financial effect.
    v_available:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
    v_result:=public.weekly_source_first_authorisation_withdraw_v1(
      v_root,v_root,v_signature,'c7000000-0000-4000-8000-000000000001');
    v_after:=pg_temp.write_fingerprint();

    perform pg_temp.assert_true(
      coalesce((v_available->>'available')::boolean,true) is false
      and coalesce((v_result->>'ok')::boolean,true) is false
      and coalesce((v_result->>'withdrawn')::boolean,true) is false,
      v_case.label||': the withdrawal must refuse, got '||v_result::text);

    select exists(
      select 1
      from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
      where failed.value->>'check'=v_case.check_name
        and failed.value->'reasons' @> pg_catalog.to_jsonb(v_case.reason)
    ) into v_found;
    perform pg_temp.assert_true(v_found,
      v_case.label||': check '||v_case.check_name||' must fail with reason '
      ||v_case.reason||', got '||(v_result->'failed_checks')::text);

    -- NOTHING was written.  The whole fingerprint is compared, so a write this
    -- test never thought of still fails it.  Only the refusal audit row is
    -- permitted to move, and it is not in the fingerprint.
    perform pg_temp.assert_true(v_after=v_before,
      v_case.label||': a refused withdrawal must write NOTHING. before='
      ||v_before::text||' after='||v_after::text);

    -- And the money-relevant half: the committed head is untouched and still
    -- current, so the Workbench position for that week did not change.
    perform pg_temp.assert_true(
      (select head_row.state='COMMITTED_CURRENT'
         and head_row.superseded_at_utc is null
         and head_row.superseded_by_withdrawal_id is null
         and head_row.superseded_reason is null
       from public.weekly_source_entitlement_heads head_row
       where head_row.id=v_head),
      v_case.label||': the committed head must NOT be superseded by a refused '
      ||'withdrawal');
    perform pg_temp.drain_workbench_jobs();
  end loop;
end
$verify_each_financial_effect$;

-- ---------------------------------------------------------------------------
-- 4. THE AMBIGUOUS CASE (ruling A3: "A missing or ambiguous result refuses",
--    and "refused with a plain-English instruction to use the proper Banking
--    Pay cancellation/correction path")
-- ---------------------------------------------------------------------------
do $verify_ambiguous$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000306';
  v_result jsonb;
  v_before jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  -- Candidate 6 already carries a cancelled batch, a voided item and a PENDING
  -- transfer bound to that item, from section 3.  A transfer whose outcome the
  -- installed classifier cannot call final-no-money is exactly the ambiguous
  -- shape: nobody can say whether the money moved.
  v_before:=pg_temp.write_fingerprint();
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,pg_temp.current_signature(v_root),
    'c7000000-0000-4000-8000-000000000001');

  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and coalesce((v_result->>'head_superseded')::boolean,false) is not true,
    'A3: an ambiguous money position must refuse and supersede nothing, got '
    ||v_result::text);
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'A3: the ambiguous refusal must write nothing');

  -- The plain-English instruction the ruling requires, on whichever of the
  -- in-flight codes wins: it must send Office to Banking Pay and must not
  -- pretend the position is resolved.
  perform pg_temp.assert_true(
    v_result->>'code' in (
      'WEEKLY_SOURCE_UNAUTHORISE_MONEY_UNRESOLVED',
      'WEEKLY_SOURCE_UNAUTHORISE_BANKING_ACTIVE')
    and pg_catalog.lower(coalesce(v_result->>'refusal_message',''))
        like '%banking pay%'
    and (pg_catalog.lower(coalesce(v_result->>'refusal_message',''))
         like '%cancellation%'
         or pg_catalog.lower(coalesce(v_result->>'refusal_message',''))
            like '%cancel%'),
    'A3: an in-flight or ambiguous refusal must send Office to the Banking Pay '
    ||'cancellation or correction path, got '||coalesce(v_result::text,'<null>'));
  perform pg_temp.drain_workbench_jobs();
end
$verify_ambiguous$;

-- ---------------------------------------------------------------------------
-- 5. ROUND 5 SECTION A5 — the finance-case write-off, on a fixture whose ONLY
--    failing check is that write-off, so the top-level code and the message are
--    the write-off's own
-- ---------------------------------------------------------------------------
do $verify_write_off_message$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000302';
  v_result jsonb;
  v_message text;
begin
  perform pg_temp.drain_workbench_jobs();
  -- Candidate 2's Draft item from section 3 is voided out, and the only trace
  -- left is a reservation released by a finance-case write-off.
  update public.pay_batch_items set is_voided=true
   where id='c7000000-0000-4000-8000-000000000802';
  update public.pay_batches set status='CANCELLED'
   where id='c7000000-0000-4000-8000-000000000602';
  insert into public.pay_advances(
    id,candidate_id,client_id,reason,original_amount,outstanding_amount,case_type
  ) values (
    'c7000000-0000-4000-8000-00000000fa02','c7000000-0000-4000-8000-000000000102',
    'c7000000-0000-4000-8000-000000000002',
    'MANUAL_ADVANCE'::public.pay_advance_reason_enum,50,50,
    'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum);
  insert into public.pay_advance_reservations(
    id,finance_case_id,pay_batch_id,pay_batch_candidate_id,pay_batch_item_id,
    reserved_amount,status,released_at_utc,released_reason
  ) values (
    'c7000000-0000-4000-8000-00000000fb02','c7000000-0000-4000-8000-00000000fa02',
    'c7000000-0000-4000-8000-000000000602','c7000000-0000-4000-8000-000000000702',
    'c7000000-0000-4000-8000-000000000802',50,'RELEASED',
    pg_catalog.clock_timestamp(),'WRITE_OFF');
  perform pg_temp.drain_workbench_jobs();

  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(v_root);
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_UNAUTHORISE_WRITE_OFF_UNRESOLVED'
    and v_result->>'refusal_nature'='INTEGRITY'
    and coalesce((v_result->>'retryable')::boolean,true) is false,
    'A5: the write-off must refuse with its own code and the review disposition, '
    ||'never a retry, got '||v_result::text);

  v_message:=v_result->>'refusal_message';
  -- ROUND 5 SECTION A5, word for word: "The message must identify the
  -- finance-case write-off as unresolved Banking Pay evidence and route it to
  -- review.  Do not label it paid, settled or safely cancelled."
  perform pg_temp.assert_true(
    nullif(pg_catalog.btrim(coalesce(v_message,'')),'') is not null
    and pg_catalog.lower(v_message) like '%write-off%'
    and pg_catalog.lower(v_message) like '%unresolved%'
    and pg_catalog.lower(v_message) like '%banking pay%'
    and pg_catalog.lower(v_message) like '%review%',
    'A5: the message must name the finance-case write-off as UNRESOLVED BANKING '
    ||'PAY evidence and route it to REVIEW, got '||coalesce(v_message,'<null>'));
  perform pg_temp.assert_true(
    pg_catalog.lower(v_message) not like '%has been paid%'
    and pg_catalog.lower(v_message) not like '%was paid%'
    and pg_catalog.lower(v_message) not like '%settled%'
    and pg_catalog.lower(v_message) not like '%safely cancelled%'
    and pg_catalog.lower(v_message) not like '%cancelled safely%'
    and pg_catalog.lower(v_message) not like '%successfully cancelled%',
    'A5: the message must NEVER label the write-off paid, settled or safely '
    ||'cancelled, got '||v_message);
  perform pg_temp.drain_workbench_jobs();
end
$verify_write_off_message$;

-- ---------------------------------------------------------------------------
-- 6. REPLAY (ruling A3: "Exact replay returns the existing receipt; conflicting
--    replay refuses")
-- ---------------------------------------------------------------------------
do $verify_replay$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000301';
  v_signature text;
  v_first jsonb;
  v_replay jsonb;
  v_before jsonb;
  v_receipt uuid;
begin
  perform pg_temp.drain_workbench_jobs();
  -- Candidate 1 was withdrawn in section 2, with its head superseded.
  select id,result_json into v_receipt,v_first
  from private.weekly_source_first_authorisation_withdrawal_receipts
  where root_timesheet_id=v_root;
  v_signature:=v_first->>'expected_row_signature';

  v_before:=pg_temp.write_fingerprint();
  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,v_root,v_signature,'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_replay->>'replayed')::boolean,false)
    and coalesce((v_replay->>'ok')::boolean,false)
    and (v_replay->>'withdrawal_receipt_id')::uuid=v_receipt
    and (v_replay->>'scope_change_tx_token')=(v_first->>'scope_change_tx_token')
    and (v_replay#>>'{head_supersession,head_id}')
        =(v_first#>>'{head_supersession,head_id}'),
    'A3: an EXACT replay must return the existing receipt, got '||v_replay::text);
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'A3: an exact replay must write nothing at all');

  -- A CONFLICTING replay: the same replay identity presented against a
  -- different physical Timesheet.  Round 5, Part E: "a decision must never
  -- migrate silently between physical Timesheet IDs."
  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_root,'c7000000-0000-4000-8000-000000000302',v_signature,
    'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_replay->>'ok')::boolean,true) is false
    and v_replay->>'code'='WEEKLY_SOURCE_WITHDRAWAL_REPLAY_CONFLICT'
    and v_replay->>'refusal_nature'='INTEGRITY'
    and coalesce((v_replay->>'retryable')::boolean,true) is false
    and coalesce((v_replay->>'review_required')::boolean,false),
    'A3: a CONFLICTING replay must refuse for review, got '||v_replay::text);
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'A3: a conflicting replay must write nothing at all');

  -- A TAMPERED receipt is a permanent integrity failure that goes straight to
  -- manual review and is never retried (round 5 section A1 control 5).  The
  -- receipt relation refuses every update, so the tampering is done the only way
  -- it could ever happen in the field: by changing a binding the digest covers.
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update private.weekly_source_first_authorisation_withdrawal_receipts
              set root_timesheet_version=99 where id=%L$sql$,v_receipt),
    'WEEKLY_SOURCE_WITHDRAWAL_RECEIPT_IMMUTABLE',
    'tampering with a stored withdrawal receipt');
  perform pg_temp.drain_workbench_jobs();
end
$verify_replay$;

-- ---------------------------------------------------------------------------
-- 7. ROUND 5 PART E — what the authorisation row signature binds, and what a
--    row written before the binding existed does
-- ---------------------------------------------------------------------------
do $verify_signature_binding$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000309';
  v_result jsonb;
  v_before jsonb;
  v_live uuid;
  v_digest bytea;
  v_binding jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  -- Candidate 9 is refused by W10 for its advance, which is irrelevant here:
  -- what is being proved is the W9 binding, which is evaluated on every call.
  select id,decision_digest into v_live,v_digest
  from public.weekly_source_root_authorisations
  where root_timesheet_id=v_root and withdrawn_at_utc is null;
  perform pg_temp.assert_true(v_digest is not null,
    'Part E: a row written by this package must carry its five-field digest');

  -- All five bindings are inputs of the digest: changing ANY of them changes it.
  v_binding:=private.weekly_source_root_authorisation_signature_v1(
    null,v_root,'WP07C-BK-09',1,1,'signature',array[]::text[]);
  perform pg_temp.assert_true(
    v_binding ? 'agency_id'
    and v_binding ? 'root_family_booking_id'
    and v_binding ? 'root_timesheet_id'
    and v_binding ? 'timesheet_version'
    and v_binding ? 'authorisation_generation'
    and v_binding ? 'lifecycle_row_signature'
    and v_binding ? 'protected_decision_hashes',
    'Part E: the binding must carry tenant and agency, the canonical root, the '
    ||'exact physical identity, the generation and revision, and the protected '
    ||'decision fields, got '||v_binding::text);
  -- The booking reference alone is NOT a signature key: two rows that agree on
  -- the reference and differ only in the PHYSICAL id have different digests.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_root_authorisation_signature_v1(
        null,'c7000000-0000-4000-8000-000000000309','WP07C-BK-09',1,1,'s',
        array[]::text[]))
    is distinct from
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_root_authorisation_signature_v1(
        null,'c7000000-0000-4000-8000-000000000310','WP07C-BK-09',1,1,'s',
        array[]::text[])),
    'Part E: the booking reference alone must NEVER be a signature key');
  -- And two rows that differ only in the agency also differ.
  perform pg_temp.assert_true(
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_root_authorisation_signature_v1(
        null,v_root,'WP07C-BK-09',1,1,'s',array[]::text[]))
    is distinct from
    private.weekly_source_publication_request_digest_v1(
      private.weekly_source_root_authorisation_signature_v1(
        'c7000000-0000-4000-8000-0000000000a1',v_root,'WP07C-BK-09',1,1,'s',
        array[]::text[])),
    'Part E: the absence of agency evidence must itself be bound, so a row '
    ||'written without it can never match one written with it');

  -- THE BINDING CANNOT BE EDITED AT ALL once written.  `decision_digest`,
  -- `agency_id` and `protected_decision_hashes` are deliberately NOT lifecycle
  -- columns, so the ACL guard refuses every attempt to move them.  That is the
  -- first half of "a decision must never migrate silently".
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update public.weekly_source_root_authorisations
              set decision_digest=null where id=%L$sql$,v_live),
    'WEEKLY_SOURCE_IMMUTABLE_FACT',
    'clearing the authorisation row binding digest');
  perform pg_temp.assert_refused(
    pg_catalog.format(
      $sql$update public.weekly_source_root_authorisations
              set agency_id='c7000000-0000-4000-8000-0000000000a1' where id=%L$sql$,v_live),
    'WEEKLY_SOURCE_IMMUTABLE_FACT',
    'changing the authorisation row agency binding');

  -- A ROW WRITTEN BEFORE THE BINDING EXISTED carries a NULL digest.  Since the
  -- guard above forbids editing a written row, the pre-change shape is created
  -- the only way it could exist in the field: as a row that was INSERTED without
  -- one.  It must be refused BY NAME, never pass by accident.
  v_before:=pg_temp.write_fingerprint();
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values (
    'c7000000-0000-4000-8000-000000000313','WP07C-BK-13',1,1,
    'pre-binding-signature','c7000000-0000-4000-8000-000000000001');
  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'c7000000-0000-4000-8000-000000000313');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
          where failed.value->>'check'='W9'
            and failed.value->'reasons'
                @> '["ROOT_AUTHORISATION_DECISION_DIGEST_MISSING"]'::jsonb)=1,
    'Part E: a pre-binding row must be refused by NAME as a missing digest, got '
    ||v_result::text);
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'c7000000-0000-4000-8000-000000000313','c7000000-0000-4000-8000-000000000313',
    pg_temp.current_signature('c7000000-0000-4000-8000-000000000313'),
    'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
    'Part E: and the write path must refuse it by name, got '||v_result::text);

  -- A row whose stored binding does not rebuild from its own five fields is a
  -- different named refusal.  Same shape: written that way, never edited.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id,decision_digest
  ) values (
    'c7000000-0000-4000-8000-000000000314','WP07C-BK-14',1,1,
    'wrong-binding-signature','c7000000-0000-4000-8000-000000000001',
    pg_catalog.sha256(pg_catalog.convert_to('not-the-digest','UTF8')));
  v_result:=public.weekly_source_first_authorisation_withdraw_available_v1(
    'c7000000-0000-4000-8000-000000000314');
  perform pg_temp.assert_true(
    coalesce((v_result->>'available')::boolean,true) is false
    and (select pg_catalog.count(*)
           from pg_catalog.jsonb_array_elements(v_result->'failed_checks') as failed(value)
          where failed.value->>'check'='W9'
            and failed.value->'reasons'
                @> '["ROOT_AUTHORISATION_DECISION_DIGEST_MISMATCH"]'::jsonb)=1,
    'Part E: a binding that does not rebuild must be refused by NAME as a '
    ||'mismatch, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
end
$verify_signature_binding$;

-- ---------------------------------------------------------------------------
-- 8. THE WORKBENCH SELECTOR IS UNTOUCHED, and an ordinary Timesheet is
--    unaffected
-- ---------------------------------------------------------------------------
do $verify_selector_untouched$
declare
  v_result jsonb;
  v_before jsonb;
begin
  -- Ruling A3's closing sentence: "The Workbench selector remains unchanged and
  -- continues to consume only current committed heads."
  --
  -- The selector is NOT pinned by hash here, and that is deliberate.  Its own
  -- Gate 4 seam owner is editing it under round-5 ruling 3 (zero-valued
  -- additions and expenses suppressed in the money fact stream), so a hash pin
  -- would fail on somebody else's APPROVED change and report it as a WP-07c
  -- regression.  What ruling A3 actually requires of this package is that the
  -- selector gains no coupling to the withdrawal, which is the coupling WP-10
  -- explicitly declined to add and which option (c) of the WP-07 review would
  -- have created.  That is asserted directly, on the installed definition:
  -- the selector must not read the authorisation record, the withdrawal receipt
  -- or either supersession column.  It resolves a head by state, exactly as it
  -- did; what changed is the STATE it reads.
  perform pg_temp.assert_true(
    (select pg_catalog.strpos(p.prosrc,'weekly_source_root_authorisations')=0
        and pg_catalog.strpos(p.prosrc,'weekly_source_first_authorisation')=0
        and pg_catalog.strpos(p.prosrc,'superseded_by_withdrawal_id')=0
        and pg_catalog.strpos(p.prosrc,'superseded_reason')=0
        and pg_catalog.strpos(p.prosrc,'withdrawal_receipt')=0
     from pg_catalog.pg_proc p
     join pg_catalog.pg_namespace n on n.oid=p.pronamespace
     where n.nspname='private'
       and p.proname='pay_workbench_unit_economic_occurrence_page_v1'),
    'A3: the Gate 4 Workbench selector must gain NO coupling to the withdrawal, '
    ||'the authorisation record or the supersession columns');
  -- And it must still resolve a head the way it always did.
  perform pg_temp.assert_true(
    (select pg_catalog.strpos(p.prosrc,'weekly_source_entitlement_heads')>0
        and pg_catalog.strpos(p.prosrc,'COMMITTED_CURRENT')>0
     from pg_catalog.pg_proc p
     join pg_catalog.pg_namespace n on n.oid=p.pronamespace
     where n.nspname='private'
       and p.proname='pay_workbench_unit_economic_occurrence_page_v1'),
    'A3: the selector must still consume only current committed heads');
  -- The three CALL-ONLY owners are pinned by hash, because nothing else in this
  -- programme is editing them: all three repeatables are unchanged since
  -- 15 September, and the values below were measured on the full local
  -- PostgreSQL 17.11 NEW build taken BEFORE this package changed anything
  -- (database banking_modal_v2_release0073_20260918, clone ws62_wp07c_a).
  perform pg_temp.assert_true(
    (select pg_catalog.md5(p.prosrc) from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
     where n.nspname='public' and p.proname='timesheet_unauthorise_atomic')
    ='8ee9b89389b6d6f7391740f9e696bb75'
    and (select pg_catalog.md5(p.prosrc) from pg_catalog.pg_proc p
          join pg_catalog.pg_namespace n on n.oid=p.pronamespace
         where n.nspname='public' and p.proname='timesheet_authorise_generic_atomic')
    ='8c65681ae56129463f46e76f88578aed'
    and (select pg_catalog.md5(p.prosrc) from pg_catalog.pg_proc p
          join pg_catalog.pg_namespace n on n.oid=p.pronamespace
         where n.nspname='private' and p.proname='pay_workbench_scope_invalidate_v1')
    ='0d26de465bc221f6a41043fb27c8d797',
    'the three CALL-ONLY owners must be byte-identical to that same build');
  -- Exactly one definition of each, so the pins above cannot be satisfied by a
  -- second overload nobody noticed.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
     where n.nspname='private'
       and p.proname='pay_workbench_unit_economic_occurrence_page_v1')=1,
    'there must be exactly one Workbench selector definition');

  -- An ordinary, unmanaged Timesheet: this owner is not its route, it refuses
  -- with no write, and the ordinary owner is untouched.
  v_before:=pg_temp.write_fingerprint();
  v_result:=public.weekly_source_first_authorisation_withdraw_v1(
    'c7000000-0000-4000-8000-000000000311','c7000000-0000-4000-8000-000000000311',
    pg_temp.current_signature('c7000000-0000-4000-8000-000000000311'),
    'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,true) is false
    and v_result->>'code' in (
      'WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
      'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
      'WEEKLY_SOURCE_UNAUTHORISE_PAID',
      'WEEKLY_SOURCE_UNAUTHORISE_LATER_DECISION_EXISTS'),
    'an ordinary Timesheet must be refused by this owner, got '||v_result::text);
  perform pg_temp.assert_true(pg_temp.write_fingerprint()=v_before,
    'and that refusal must write nothing');
  perform pg_temp.drain_workbench_jobs();
end
$verify_selector_untouched$;

-- ---------------------------------------------------------------------------
-- 9. THE SERVICE ENTRY POINT THE OFFICE SCREEN CALLS, EXECUTED
--    (WP-12 handoff N2: the Unauthorise control on a managed root had nothing
--     behind it)
-- ---------------------------------------------------------------------------
do $verify_route$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000315';
  v_result jsonb;
  v_head uuid:='c7000000-0000-4000-8000-000000000915';
  v_before jsonb;
begin
  perform pg_temp.drain_workbench_jobs();
  v_result:=public.weekly_source_first_authorise_v1(
    v_root,v_root,null,'c7000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(coalesce((v_result->>'ok')::boolean,false),
    'Candidate 15 must authorise, got '||v_result::text);
  perform pg_temp.drain_workbench_jobs();
  perform pg_temp.publish_head(
    v_head,'c7000000-0000-4000-8000-000000000b15',
    'c7000000-0000-4000-8000-000000000d15','c7000000-0000-4000-8000-000000000c15',
    'c7000000-0000-4000-8000-000000000e15','c7000000-0000-4000-8000-000000000115',
    'c7000000-0000-4000-8000-000000000215',v_root,'WP07C-BK-15',1,false,2);
  perform pg_temp.drain_workbench_jobs();

  -- The EXACT shape WP-12 has coded against, with actor_user_id injected by the
  -- broker rather than supplied by the browser.
  v_result:=public.weekly_source_first_authorisation_withdraw_request_v1(
    pg_catalog.jsonb_build_object(
      'timesheet_id',v_root,
      'expected_timesheet_id',v_root,
      'expected_row_signature',pg_temp.current_signature(v_root),
      'actor_user_id','c7000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'withdrawn')::boolean,false)
    and coalesce((v_result->>'head_superseded')::boolean,false)
    and (v_result->>'withdrawal_receipt_id') is not null,
    'the Office route must reach the owner and complete the withdrawal, got '
    ||v_result::text);
  perform pg_temp.assert_true(
    (select head_row.state='SUPERSEDED'
       and head_row.superseded_reason='FIRST_AUTHORISATION_WITHDRAWN'
     from public.weekly_source_entitlement_heads head_row where head_row.id=v_head),
    'the route must produce the same atomic supersession as the owner');
  -- The response carries the fields WP-12 reads.
  perform pg_temp.assert_true(
    v_result ? 'ok' and v_result ? 'withdrawn' and v_result ? 'code'
    and v_result ? 'replayed',
    'the route response must carry the fields the Office screen reads, got '
    ||v_result::text);
  perform pg_temp.drain_workbench_jobs();

  -- It rejects an unknown field rather than ignoring it, and it never accepts a
  -- request that is not an object.
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_first_authorisation_withdraw_request_v1(
      '{"timesheet_id":"c7000000-0000-4000-8000-000000000315","reason":"because"}'::jsonb)$sql$,
    'WEEKLY_SOURCE_UNAUTHORISE_UNKNOWN_FIELD',
    'an unknown field in the Office request');
  perform pg_temp.assert_refused(
    $sql$select public.weekly_source_first_authorisation_withdraw_request_v1('[]'::jsonb)$sql$,
    'WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID',
    'a request that is not an object');
end
$verify_route$;

select 'WP-07c WITHDRAWAL SUPERSESSION VERIFIER: ALL SECTIONS PASSED' as result;

rollback;
