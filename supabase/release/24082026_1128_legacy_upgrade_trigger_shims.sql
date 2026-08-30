-- One-time LEGACY_UPGRADE bridge for historical trigger/RPC functions that
-- were installed outside public.schema_migrations on the original platform.
-- These fail-closed/no-op definitions exist only while the protected migration
-- history is replayed. Current repeatables must replace every shim before
-- contract verification and atomic adoption are permitted.

\set ON_ERROR_STOP on

begin;

do $legacy_upgrade$
declare
  v_name text;
begin
  foreach v_name in array array[
    'pay_workbench_mark_candidate_dirty',
    'pay_workbench_mark_finance_case_dirty',
    'pay_workbench_mark_contract_client_dirty'
  ]
  loop
    if to_regprocedure(format('public.%I()', v_name)) is null then
      execute format($ddl$
        create function public.%I()
        returns trigger
        language plpgsql
        set search_path to 'public'
        as $function$
        begin
          perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
          if TG_OP = 'DELETE' then
            return OLD;
          end if;
          return NEW;
        end
        $function$
      $ddl$, v_name);
    end if;
  end loop;
end
$legacy_upgrade$;

create table if not exists public.timesheet_archive_transition_capability (
  capability_token uuid not null,
  backend_pid integer not null,
  transaction_id bigint not null,
  timesheet_id uuid not null,
  action text not null,
  created_at_utc timestamptz not null default clock_timestamp(),
  constraint timesheet_archive_transition_capability_pk
    primary key (capability_token, timesheet_id),
  constraint timesheet_archive_transition_capability_action_ck
    check (action in ('ARCHIVE', 'UNARCHIVE'))
);

alter table public.timesheet_archive_transition_capability enable row level security;
revoke all on table public.timesheet_archive_transition_capability from public;

do $legacy_upgrade$
declare
  v_name text;
begin
  foreach v_name in array array[
    'timesheet_archive_row_guard_v1',
    'timesheet_archived_evidence_guard_v1',
    'invoice_line_archived_timesheet_guard_v1',
    'timesheet_financial_retention_capture_trigger_v1'
  ]
  loop
    if to_regprocedure(format('public.%I()', v_name)) is null then
      execute format($ddl$
        create function public.%I()
        returns trigger
        language plpgsql
        security definer
        set search_path to 'pg_catalog', 'public', 'pg_temp'
        as $function$
        begin
          perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
          if TG_OP = 'DELETE' then
            return OLD;
          end if;
          if TG_LEVEL = 'STATEMENT' then
            return null;
          end if;
          return NEW;
        end
        $function$
      $ddl$, v_name);
    end if;
  end loop;
end
$legacy_upgrade$;

-- Historical LIVE does not carry two May/August Banking dependency routines
-- that the ordered repeatable authority expects while compiling its oldest
-- complete source file. These empty-shape definitions are never callable
-- business implementations; current repeatables replace both before the release
-- can pass the transition-shim assertion.
do $legacy_upgrade$
begin
  if to_regprocedure('public._pay_timesheet_rotation_scope(uuid[])') is null then
    execute $ddl$
      create function public._pay_timesheet_rotation_scope(p_timesheet_ids uuid[])
      returns table(
        requested_timesheet_id uuid,
        booking_id text,
        canonical_timesheet_id uuid,
        family_timesheet_id uuid,
        family_is_current boolean,
        family_version integer,
        requested_is_canonical boolean
      )
      language sql
      stable
      security definer
      set search_path to 'public'
      as $function$
        select
          null::uuid,null::text,null::uuid,null::uuid,
          null::boolean,null::integer,null::boolean
        where false /* CLOUDTMS_LEGACY_TRANSITION_SHIM */;
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public._pay_active_settled_components(uuid[])') is null then
    execute $ddl$
      create function public._pay_active_settled_components(p_timesheet_ids uuid[])
      returns table(
        timesheet_id uuid,
        key_type text,
        key_value text,
        amount_ex_vat numeric,
        amount_inc_vat numeric
      )
      language sql
      stable
      security definer
      set search_path to 'public'
      as $function$
        select null::uuid,null::text,null::text,null::numeric,null::numeric
        where false /* CLOUDTMS_LEGACY_TRANSITION_SHIM */;
      $function$
    $ddl$;
  end if;
end
$legacy_upgrade$;

do $legacy_upgrade$
begin
  if to_regprocedure('public.pay_workbench_case_resolution_origin_backfill_v1(uuid,integer,boolean)') is null then
    execute $ddl$
      create function public.pay_workbench_case_resolution_origin_backfill_v1(
        p_after_id uuid default null::uuid,
        p_limit integer default 500,
        p_apply boolean default true
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        if exists (
          select 1
          from public.banking_pay_workbench_session_case_resolutions
        ) then
          raise exception using message =
            'LEGACY_UPGRADE_REQUIRES_CANONICAL_ORIGIN_BACKFILL';
        end if;
        return pg_catalog.jsonb_build_object(
          'ok', true,
          'apply', p_apply,
          'limit', p_limit,
          'after_id', case when p_after_id is null then null else p_after_id::text end,
          'scanned_count', 0,
          'valid_count', 0,
          'updated_count', 0,
          'invalid_count', 0,
          'invalid_samples', '[]'::jsonb,
          'unresolved_total_count', 0,
          'next_cursor', null,
          'has_more', false,
          'complete', p_apply is true
        );
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.pay_workbench_case_resolution_origin_guard_v1()') is null then
    execute $ddl$
      create function public.pay_workbench_case_resolution_origin_guard_v1()
      returns trigger
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb)') is null then
    execute $ddl$
      create function public.pay_payment_cancel_not_sent_and_recalculate(
        p_pay_batch_id uuid,
        p_selection_json jsonb default '{}'::jsonb,
        p_actor_user_id uuid default null::uuid,
        p_reason text default null::text,
        p_idempotency_key text default null::text,
        p_confirmation_json jsonb default '{}'::jsonb
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      declare
        v_scope_type text := 'BATCH';
        v_resolved_scope_json jsonb := coalesce(p_selection_json, '{}'::jsonb);
        v_transition_payload jsonb;
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        v_transition_payload := jsonb_build_object(
      'scope_type', v_scope_type,
      'pay_batch_id', p_pay_batch_id::text,
          'actor_user_id', case when p_actor_user_id is null then null else p_actor_user_id::text end,
          'reason', p_reason,
          'idempotency_key', p_idempotency_key,
          'confirmation_json', p_confirmation_json,
          'resolved_scope_json', v_resolved_scope_json
        );
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
        return v_transition_payload;
      end
      $function$
    $ddl$;
  end if;

  if to_regclass('public.banking_pay_workbench_session_case_resolutions') is not null
     and not exists (
       select 1
       from pg_catalog.pg_trigger
       where tgrelid = 'public.banking_pay_workbench_session_case_resolutions'::regclass
         and tgname = 'trg_bpay_wb_case_resolution_origin_guard'
         and tgisinternal is false
     ) then
    execute $ddl$
      create trigger trg_bpay_wb_case_resolution_origin_guard
      before insert or update
      on public.banking_pay_workbench_session_case_resolutions
      for each row
      execute function public.pay_workbench_case_resolution_origin_guard_v1()
    $ddl$;
  end if;

  if to_regprocedure('public.timesheet_financial_retention_mark_v1(uuid[])') is null then
    execute $ddl$
      create function public.timesheet_financial_retention_mark_v1(p_timesheet_ids uuid[])
      returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamptz)') is null then
    execute $ddl$
      create function public.timesheet_archive_transition_v1(
        p_timesheet_id uuid,
        p_action text,
        p_removal_kind text default 'STANDARD_DELETE'::text,
        p_actor_user_id uuid default null::uuid,
        p_expected_timesheet_id uuid default null::uuid,
        p_expected_row_signature text default null::text,
        p_now_utc timestamptz default now()
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.timesheet_r2_cleanup_claim_v1(integer,integer)') is null then
    execute $ddl$
      create function public.timesheet_r2_cleanup_claim_v1(
        p_limit integer default 50,
        p_lease_seconds integer default 300
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.timesheet_r2_cleanup_record_v1(text,uuid,uuid[],jsonb,uuid)') is null then
    execute $ddl$
      create function public.timesheet_r2_cleanup_record_v1(
        p_delete_operation_id text,
        p_requested_timesheet_id uuid,
        p_deleted_timesheet_ids uuid[],
        p_failures jsonb,
        p_claim_token uuid default null::uuid
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;

  if to_regprocedure('public.timesheet_r2_cleanup_complete_v1(text,uuid,text[])') is null then
    execute $ddl$
      create function public.timesheet_r2_cleanup_complete_v1(
        p_delete_operation_id text,
        p_claim_token uuid,
        p_r2_keys text[]
      ) returns jsonb
      language plpgsql
      security definer
      set search_path to 'pg_catalog', 'public', 'pg_temp'
      as $function$
      begin
        perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
        raise exception using message = 'LEGACY_UPGRADE_IN_PROGRESS';
      end
      $function$
    $ddl$;
  end if;
end
$legacy_upgrade$;

do $legacy_upgrade$
declare
  v_role text;
  v_signature text;
begin
  foreach v_role in array array['anon', 'authenticated', 'service_role']::text[]
  loop
    if exists (select 1 from pg_catalog.pg_roles where rolname = v_role) then
      execute format(
        'revoke all on table public.timesheet_archive_transition_capability from %I',
        v_role
      );
    end if;
  end loop;

  foreach v_signature in array array[
    'public.timesheet_archive_row_guard_v1()',
    'public.timesheet_archived_evidence_guard_v1()',
    'public.invoice_line_archived_timesheet_guard_v1()',
    'public.timesheet_financial_retention_capture_trigger_v1()',
    'public.pay_workbench_case_resolution_origin_backfill_v1(uuid,integer,boolean)',
    'public.pay_workbench_case_resolution_origin_guard_v1()',
    'public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb)',
    'public.timesheet_financial_retention_mark_v1(uuid[])',
    'public.timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamptz)',
    'public.timesheet_r2_cleanup_claim_v1(integer,integer)',
    'public.timesheet_r2_cleanup_record_v1(text,uuid,uuid[],jsonb,uuid)',
    'public.timesheet_r2_cleanup_complete_v1(text,uuid,text[])',
    'public._pay_timesheet_rotation_scope(uuid[])',
    'public._pay_active_settled_components(uuid[])'
  ]
  loop
    if to_regprocedure(v_signature) is not null then
      execute format('revoke all on function %s from public', v_signature);
      foreach v_role in array array['anon', 'authenticated', 'service_role']::text[]
      loop
        if exists (select 1 from pg_catalog.pg_roles where rolname = v_role) then
          execute format('revoke all on function %s from %I', v_signature, v_role);
        end if;
      end loop;
    end if;
  end loop;
end
$legacy_upgrade$;

commit;
