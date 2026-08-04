-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: b3f1be45233a48197b730d9b00863b7e

CREATE OR REPLACE FUNCTION public.pay_batch_schedule(p_pay_batch_id uuid, p_schedule_kind text, p_scheduled_at_utc timestamp with time zone, p_funding_account_ref text, p_warning_hours_json jsonb, p_actor_user_id uuid, p_operation_id uuid DEFAULT NULL::uuid, p_freshness_result_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_kind text := upper(btrim(coalesce(p_schedule_kind,'')));
  v_batch record;
  v_cfg record;

  v_provider text := null;

  v_sched_at timestamptz;
  v_warn jsonb;
  v_authoritative_payment_date date;
  v_commit_ts timestamptz := now();

  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;
  v_funding text := nullif(btrim(coalesce(p_funding_account_ref,'')), '');

  v_missing_bank int := 0;
  v_blocked_name int := 0;
  v_missing_map int := 0;
  v_pending_transfers int := 0;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_batch_kind_fixed text := null;
  v_bad_loans_payee_ct int := 0;

  v_reservations_committed_count int := 0;
  v_reservations_committed_amount numeric := 0;

  v_worker_communications jsonb := '{}'::jsonb;
  v_comm_result jsonb := '{}'::jsonb;
  v_comm_error text := null;
  v_comm_trigger_status text := null;
  v_comm_message_kind text := null;

  v_payout_notice_result jsonb := '{}'::jsonb;
  v_payout_notice_trigger_status text := null;
  v_payout_notice_error text := null;
  v_payout_notice_dispatch_required boolean := false;
  v_payout_notice_targeted_count int := 0;

  v_remittance_result jsonb := '{}'::jsonb;
  v_remittance_trigger_status text := null;
  v_remittance_error text := null;
  v_remittance_dispatch_required boolean := false;
  v_remittance_targeted_count int := 0;
  v_execution_commit_state text := 'NOT_SUBMITTED';
  v_execution_commit_ref text := null;
  v_execution_committed_at_utc timestamptz := null;

  v_execution_intent_json jsonb := '{}'::jsonb;
  v_execution_mode text := 'STANDARD_BANK';
  v_suppress_remittances boolean := false;
  v_has_external_submission_evidence boolean := false;
  v_submission_evidence_json jsonb := '{}'::jsonb;
  v_intent_payment_date date := null;
  v_batch_status_upper text := null;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_operation_mode boolean := false;
  v_expected_freshness_hash text := null;
  v_expected_freshness_scope_hash text := null;
  v_stored_freshness_status text := null;
  v_stored_freshness_hash text := null;
  v_stored_freshness_scope_hash text := null;
  v_timing_setting_if_known text := null;
begin
  PERFORM public.banking_pay_hot_path_budget_apply('OPERATION_ADVANCE');

  if p_pay_batch_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_batch_schedule: pay_batch_id is required'
    )::text;
  end if;
  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_batch_schedule: actor_user_id is required',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;
  if v_kind not in ('IMMEDIATE','SCHEDULED') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'INVALID_SCHEDULE_KIND',
      'message', 'pay_batch_schedule: invalid schedule_kind (IMMEDIATE|SCHEDULED)',
      'schedule_kind', v_kind,
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  select
    pb.id,
    pb.status,
    pb.batch_kind_fixed,
    pb.pay_date,
    pb.authoritative_payment_date,
    pb.authoritative_payment_date_source,
    pb.schedule_kind,
    pb.scheduled_at_utc,
    pb.scheduled_by_user_id,
    pb.funding_account_ref,
    pb.funds_warning_hours_json,
    pb.rail_provider_snapshot,
    pb.rail_env_snapshot,
    pb.execution_commit_state,
    pb.execution_commit_ref,
    pb.execution_committed_at_utc,
    pb.execution_intent_json,
    pb.freshness_validation_status,
    pb.freshness_result_hash,
    pb.freshness_scope_hash
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_batch_schedule: pay_batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_execution_commit_state := upper(btrim(coalesce(v_batch.execution_commit_state, 'NOT_SUBMITTED')));
  if v_execution_commit_state not in ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') then
    v_execution_commit_state := 'NOT_SUBMITTED';
  end if;
  v_execution_commit_ref := v_batch.execution_commit_ref;
  v_execution_committed_at_utc := v_batch.execution_committed_at_utc;

  if v_execution_commit_state <> 'NOT_SUBMITTED' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'EXECUTION_STATE_CONFLICT',
      'message', 'pay_batch_schedule: batch has already crossed the execution submission boundary',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_commit_state', v_execution_commit_state,
      'execution_commit_ref', v_execution_commit_ref,
      'execution_committed_at_utc', case when v_execution_committed_at_utc is null then null else v_execution_committed_at_utc::text end
    )::text;
  end if;

  v_batch_kind_fixed := upper(btrim(coalesce(v_batch.batch_kind_fixed,'')));
  v_batch_status_upper := upper(btrim(coalesce(v_batch.status, '')));

  if v_batch_status_upper in ('COMMITTED','PAID','SETTLED','CANCELLED') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'BATCH_NOT_SCHEDULABLE',
      'message', 'pay_batch_schedule: batch status cannot be scheduled or authorised',
      'pay_batch_id', p_pay_batch_id::text,
      'status', v_batch.status
    )::text;
  end if;

  if v_batch.execution_intent_json is not null and jsonb_typeof(v_batch.execution_intent_json) = 'object' then
    v_execution_intent_json := v_batch.execution_intent_json;
  else
    select pbar_intent.execution_intent_json
    into v_execution_intent_json
    from public.pay_batch_auth_requests pbar_intent
    where pbar_intent.pay_batch_id = p_pay_batch_id
      and pbar_intent.state in ('AWAITING','AUTHORISED')
      and pbar_intent.execution_intent_json is not null
    order by pbar_intent.created_at_utc desc, pbar_intent.id desc
    limit 1;

    if v_execution_intent_json is null or jsonb_typeof(v_execution_intent_json) <> 'object' then
      v_execution_intent_json := '{}'::jsonb;
    end if;
  end if;

  v_execution_mode := upper(btrim(coalesce(v_execution_intent_json->>'execution_mode', 'STANDARD_BANK')));
  if v_execution_mode = '' then
    v_execution_mode := 'STANDARD_BANK';
  end if;
  if v_execution_mode not in ('STANDARD_BANK','CSV_SETTLEMENT','EXTERNAL_SETTLEMENT') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'INVALID_EXECUTION_MODE',
      'message', 'pay_batch_schedule: invalid frozen execution_mode',
      'pay_batch_id', p_pay_batch_id::text,
      'execution_mode', v_execution_mode
    )::text;
  end if;

  v_suppress_remittances := lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances', 'false'))) in ('true','1','yes','y','on');

  if nullif(btrim(coalesce(v_execution_intent_json->>'payment_date', '')), '') ~ '^\d{4}-\d{2}-\d{2}$' then
    v_intent_payment_date := (v_execution_intent_json->>'payment_date')::date;
  else
    v_intent_payment_date := null;
  end if;

  v_operation_mode := p_operation_id IS NOT NULL OR nullif(btrim(coalesce(p_freshness_result_hash, '')), '') IS NOT NULL;

  SELECT COALESCE(NULLIF(btrim(public.settings_defaults.payment_remittance_send_timing), ''), 'ON_EXECUTION')
  INTO v_timing_setting_if_known
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_timing_setting_if_known := upper(coalesce(nullif(btrim(v_timing_setting_if_known), ''), 'ON_EXECUTION'));

  IF v_timing_setting_if_known NOT IN ('ON_EXECUTION','ON_PAYMENT_CONFIRMED') THEN
    v_timing_setting_if_known := 'ON_EXECUTION';
  END IF;

  v_stored_freshness_status := upper(btrim(coalesce(v_batch.freshness_validation_status, '')));
  v_stored_freshness_hash := nullif(btrim(coalesce(v_batch.freshness_result_hash, '')), '');
  v_stored_freshness_scope_hash := nullif(btrim(coalesce(v_batch.freshness_scope_hash, '')), '');

  if p_operation_id is not null then
    select operation_row.*
    into v_operation_row
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'OPERATION_NOT_FOUND',
        'message', 'pay_batch_schedule: operation was not found',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text;
    end if;

    if v_operation_row.pay_batch_id is not null and v_operation_row.pay_batch_id <> p_pay_batch_id then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'OPERATION_BATCH_MISMATCH',
        'message', 'pay_batch_schedule: operation belongs to another batch',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'operation_pay_batch_id', v_operation_row.pay_batch_id::text
      )::text;
    end if;

    if v_operation_row.actor_user_id is not null and v_operation_row.actor_user_id <> p_actor_user_id then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'OPERATION_ACTOR_MISMATCH',
        'message', 'pay_batch_schedule: operation belongs to another actor',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text
      )::text;
    end if;

    v_expected_freshness_hash := coalesce(
      nullif(btrim(coalesce(p_freshness_result_hash, '')), ''),
      nullif(btrim(coalesce(v_operation_row.progress_json->>'freshness_result_hash', '')), ''),
      nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,freshness_result_hash}', '')), '')
    );
    v_expected_freshness_scope_hash := coalesce(
      nullif(btrim(coalesce(v_operation_row.progress_json->>'freshness_scope_hash', '')), ''),
      nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,freshness_scope_hash}', '')), '')
    );
  else
    v_expected_freshness_hash := nullif(btrim(coalesce(p_freshness_result_hash, '')), '');
  end if;

  if v_operation_mode then
    if v_stored_freshness_status <> 'PASSED' then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'FRESHNESS_NOT_PASSED',
        'message', 'pay_batch_schedule: completed passing chunked freshness is required before scheduling',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', case when p_operation_id is null then null else p_operation_id::text end,
        'freshness_validation_status', v_stored_freshness_status,
        'freshness_result_hash', v_stored_freshness_hash,
        'freshness_scope_hash', v_stored_freshness_scope_hash
      )::text;
    end if;

    if p_operation_id is not null and v_expected_freshness_hash is null then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'OPERATION_FRESHNESS_RESULT_HASH_MISSING',
        'message', 'pay_batch_schedule: operation mode requires the operation to carry the completed freshness result hash',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'freshness_validation_status', v_stored_freshness_status,
        'freshness_result_hash', v_stored_freshness_hash,
        'freshness_scope_hash', v_stored_freshness_scope_hash
      )::text;
    end if;

    if v_expected_freshness_hash is not null and v_stored_freshness_hash is distinct from v_expected_freshness_hash then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'FRESHNESS_RESULT_HASH_MISMATCH',
        'message', 'pay_batch_schedule: operation freshness hash does not match stored batch freshness hash',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', case when p_operation_id is null then null else p_operation_id::text end,
        'expected_freshness_result_hash', v_expected_freshness_hash,
        'actual_freshness_result_hash', v_stored_freshness_hash
      )::text;
    end if;

    if v_expected_freshness_scope_hash is not null and v_stored_freshness_scope_hash is distinct from v_expected_freshness_scope_hash then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'FRESHNESS_SCOPE_HASH_MISMATCH',
        'message', 'pay_batch_schedule: operation freshness scope hash does not match stored batch freshness scope hash',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', case when p_operation_id is null then null else p_operation_id::text end,
        'expected_freshness_scope_hash', v_expected_freshness_scope_hash,
        'actual_freshness_scope_hash', v_stored_freshness_scope_hash
      )::text;
    end if;
  else
    v_fresh := public.pay_batch_validate_freshness(
      p_pay_batch_id => p_pay_batch_id,
      p_actor_user_id => p_actor_user_id,
      p_allow_large_full_scan => false
    );
    v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
    v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

    if v_is_stale = true then
      select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
        into v_diff_sample
      from (
        select elem
        from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
        limit 50
      ) x;

      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_BATCH_SCHEDULE:STALE',
          jsonb_build_object(
            'pay_batch_id', p_pay_batch_id::text,
            'stale_reasons', v_stale_reasons,
            'diff_sample', v_diff_sample
          ),
          'pay_batches',
          p_pay_batch_id::text
        );
      exception when others then
        null;
      end;

      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION' else 'BATCH_STALE' end,
        'message', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'pay_batch_schedule: chunked freshness validation is required before proceeding' else 'pay_batch_schedule: batch is stale; regenerate draft before proceeding' end,
        'pay_batch_id', p_pay_batch_id::text,
        'stale_reasons', v_stale_reasons,
        'diff', v_diff_sample
      )::text;
    end if;
  end if;

  v_provider := upper(btrim(coalesce(v_batch.rail_provider_snapshot,'')));
  if v_provider = 'REV' then
    v_provider := 'REVOLUT';
  end if;
  if v_provider = '' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'RAIL_PROVIDER_MISSING_ON_BATCH',
      'message', 'pay_batch_schedule: rail_provider_snapshot missing on batch; blank provider is not treated as CSV',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;
  if v_provider not in ('REVOLUT','CSV') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'UNKNOWN_RAIL_PROVIDER',
      'message', 'pay_batch_schedule: unsupported rail_provider_snapshot',
      'pay_batch_id', p_pay_batch_id::text,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot
    )::text;
  end if;

  select
    sd.funds_warning_hours_json,
    sd.rail_supports_name_check,
    sd.rail_supports_scheduling,
    sd.rail_default_funding_account_ref
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg.funds_warning_hours_json is null
     and v_cfg.rail_supports_name_check is null
     and v_cfg.rail_supports_scheduling is null
     and v_cfg.rail_default_funding_account_ref is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SETTINGS_DEFAULTS_MISSING',
      'message', 'pay_batch_schedule: settings_defaults missing (id=1)',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  if v_execution_mode <> 'STANDARD_BANK' then
    v_warn := coalesce(p_warning_hours_json, coalesce(v_cfg.funds_warning_hours_json, '[]'::jsonb));
    if v_warn is not null and jsonb_typeof(v_warn) <> 'array' then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'WARNING_HOURS_JSON_INVALID',
        'message', 'pay_batch_schedule: warning_hours_json must be a JSON array',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_mode', v_execution_mode
      )::text;
    end if;

    if v_kind = 'IMMEDIATE' then
      v_sched_at := now();
    else
      if p_scheduled_at_utc is null then
        raise exception '%', jsonb_build_object(
          'error', 'PAY_BATCH_SCHEDULE',
          'code', 'SCHEDULED_AT_REQUIRED',
          'message', 'pay_batch_schedule: scheduled_at_utc is required when schedule_kind=SCHEDULED',
          'pay_batch_id', p_pay_batch_id::text,
          'schedule_kind', v_kind,
          'execution_mode', v_execution_mode
        )::text;
      end if;
      v_sched_at := p_scheduled_at_utc;
    end if;

    v_authoritative_payment_date := coalesce(
      v_intent_payment_date,
      (v_sched_at at time zone 'Europe/London')::date,
      v_batch.authoritative_payment_date,
      v_batch.pay_date
    );

    select count(*)::int
    into v_pending_transfers
    from public.pay_bank_transfers pbt_non_native_pending
    where pbt_non_native_pending.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt_non_native_pending.status,'')) = 'PENDING';

    if v_pending_transfers = 0 then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'NO_PENDING_TRANSFERS',
        'message', 'pay_batch_schedule: no PENDING transfers exist for this batch (execute-bank required first)',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_mode', v_execution_mode,
        'ui_hint', 'RERUN_PREVIEW_OR_EXECUTE_BANK'
      )::text;
    end if;

    if v_batch_kind_fixed = 'LOANS' then
      select count(*)::int
      into v_bad_loans_payee_ct
      from public.pay_bank_transfers pbt_non_native_loans
      where pbt_non_native_loans.pay_batch_id = p_pay_batch_id
        and upper(coalesce(pbt_non_native_loans.status,'')) = 'PENDING'
        and upper(
          coalesce(
            pbt_non_native_loans.payee_entity_kind,
            case
              when upper(coalesce(pbt_non_native_loans.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA'
              else 'CANDIDATE'
            end
          )
        ) <> 'CANDIDATE';

      if v_bad_loans_payee_ct > 0 then
        raise exception '%', jsonb_build_object(
          'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
          'code', 'LOANS_PAYEE_MUST_BE_CANDIDATE',
          'message', 'pay_batch_schedule: LOANS batches must pay candidates (not umbrellas)',
          'pay_batch_id', p_pay_batch_id::text,
          'bad_transfer_count', v_bad_loans_payee_ct,
          'execution_mode', v_execution_mode,
          'ui_hint', 'REGENERATE_TRANSFERS_AS_CANDIDATE_PAYEES'
        )::text;
      end if;
    end if;

    v_submission_evidence_json := public.pay_batch_submission_evidence(p_pay_batch_id);
    v_has_external_submission_evidence := coalesce((v_submission_evidence_json->>'has_external_submission_evidence')::boolean, false);

    if v_has_external_submission_evidence = true then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'EXTERNAL_SUBMISSION_EXISTS',
        'message', 'pay_batch_schedule: CSV/external settlement cannot be scheduled after native rail submission evidence exists',
        'pay_batch_id', p_pay_batch_id::text,
        'execution_mode', v_execution_mode
      )::text;
    end if;

    update public.pay_batches pb_non_native
    set
      schedule_kind = v_kind,
      scheduled_at_utc = v_sched_at,
      scheduled_by_user_id = p_actor_user_id,
      funding_account_ref = v_funding,
      funds_warning_hours_json = v_warn,
      authoritative_payment_date = v_authoritative_payment_date,
      authoritative_payment_date_source = 'EXECUTION_INTENT_PAYMENT_DATE',
      execution_intent_json = coalesce(v_execution_intent_json, '{}'::jsonb) || jsonb_build_object(
        'execution_mode', v_execution_mode,
        'payment_date', v_authoritative_payment_date::text,
        'schedule_kind', v_kind,
        'scheduled_at_utc', v_sched_at::text,
        'suppress_remittances', v_suppress_remittances,
        'operation_id', case when p_operation_id is null then null else p_operation_id::text end,
        'freshness_result_hash', v_stored_freshness_hash,
        'freshness_scope_hash', v_stored_freshness_scope_hash
      ),
      execution_commit_state = coalesce(nullif(btrim(coalesce(pb_non_native.execution_commit_state, '')), ''), 'NOT_SUBMITTED')
    where pb_non_native.id = p_pay_batch_id;

    begin
      v_comm_result := public.pay_remittance_maybe_queue_for_trigger(
        p_pay_batch_id => p_pay_batch_id,
        p_trigger => 'ON_EXECUTION',
        p_scope => 'ALL',
        p_actor_user_id => p_actor_user_id,
        p_only_confirmed => false,
        p_root_operation_id => CASE WHEN v_operation_mode THEN p_operation_id ELSE NULL::uuid END,
        p_operation_mode => v_operation_mode
      );

      if coalesce((v_comm_result->>'ok')::boolean, true) = false then
        raise exception '%', coalesce(
          nullif(btrim(coalesce(v_comm_result->>'error','')), ''),
          nullif(btrim(coalesce(v_comm_result #>> '{queue_result,error}','')), ''),
          'pay_batch_schedule: remittance/payout notice queue timing gate failed'
        );
      end if;

      v_comm_message_kind := coalesce(
        nullif(btrim(coalesce(v_comm_result #>> '{queue_result,message_kind}', '')), ''),
        case when v_batch_kind_fixed in ('LOANS','LOAN','FINANCE_PAYOUTS','PAYOUTS') then 'PAYOUT_NOTICE' else 'REMITTANCE' end
      );

      v_comm_trigger_status := coalesce(
        nullif(btrim(coalesce(v_comm_result->>'trigger_status', '')), ''),
        nullif(btrim(coalesce(v_comm_result #>> '{queue_result,trigger_status}', '')), ''),
        case
          when lower(btrim(coalesce(v_comm_result->>'deferred', 'false'))) in ('true','1','yes','y','on') then 'REMITTANCE_QUEUE_DEFERRED_BY_TIMING'
          when lower(btrim(coalesce(v_comm_result->>'suppressed', 'false'))) in ('true','1','yes','y','on') then 'SUPPRESSED_BY_EXECUTION_INTENT'
          when lower(btrim(coalesce(v_comm_result->>'dispatch_required', 'false'))) in ('true','1','yes','y','on') then 'QUEUE_STAGE_BUILD_READY'
          else 'NO_QUEUEABLE_REMITTANCE_OR_PAYOUT_NOTICE_JOB'
        end
      );

      v_comm_error := coalesce(
        nullif(btrim(coalesce(v_comm_result->>'error','')), ''),
        nullif(btrim(coalesce(v_comm_result #>> '{queue_result,error}','')), '')
      );

      if upper(coalesce(v_comm_message_kind, '')) = 'PAYOUT_NOTICE' then
        v_payout_notice_result := coalesce(v_comm_result->'queue_result', v_comm_result);
        v_remittance_result := jsonb_build_object(
          'ok', true,
          'trigger_status', 'NOT_APPLICABLE_FOR_FINANCE_PAYOUT_BATCH',
          'message_kind', 'REMITTANCE',
          'automatic_commit_stage', true,
          'dispatch_required', false,
          'pay_batch_id', p_pay_batch_id::text,
          'scope', 'ALL'
        );
        v_payout_notice_trigger_status := v_comm_trigger_status;
        v_payout_notice_error := v_comm_error;
        v_payout_notice_dispatch_required := lower(btrim(coalesce(v_comm_result->>'dispatch_required', v_payout_notice_result->>'dispatch_required', 'false'))) in ('true','1','yes','y','on');
        v_remittance_trigger_status := 'NOT_APPLICABLE_FOR_FINANCE_PAYOUT_BATCH';
        v_remittance_error := null;
        v_remittance_dispatch_required := false;
        if btrim(coalesce(v_payout_notice_result->>'candidate_count_targeted', '')) ~ '^[0-9]+$' then
          v_payout_notice_targeted_count := (v_payout_notice_result->>'candidate_count_targeted')::int;
        else
          v_payout_notice_targeted_count := 0;
        end if;
        v_remittance_targeted_count := 0;
      else
        v_remittance_result := coalesce(v_comm_result->'queue_result', v_comm_result);
        v_payout_notice_result := jsonb_build_object(
          'ok', true,
          'trigger_status', 'NOT_APPLICABLE_FOR_STANDARD_PAY_BATCH',
          'message_kind', 'PAYOUT_NOTICE',
          'automatic_commit_stage', true,
          'dispatch_required', false,
          'pay_batch_id', p_pay_batch_id::text
        );
        v_remittance_trigger_status := v_comm_trigger_status;
        v_remittance_error := v_comm_error;
        v_remittance_dispatch_required := lower(btrim(coalesce(v_comm_result->>'dispatch_required', v_remittance_result->>'dispatch_required', 'false'))) in ('true','1','yes','y','on');
        v_payout_notice_trigger_status := 'NOT_APPLICABLE_FOR_STANDARD_PAY_BATCH';
        v_payout_notice_error := null;
        v_payout_notice_dispatch_required := false;
        if btrim(coalesce(v_remittance_result->>'candidate_count_targeted', '')) ~ '^[0-9]+$' then
          v_remittance_targeted_count := (v_remittance_result->>'candidate_count_targeted')::int;
        else
          v_remittance_targeted_count := 0;
        end if;
        v_payout_notice_targeted_count := 0;
      end if;
    exception when others then
      v_comm_trigger_status := 'COMMIT_STAGE_QUEUE_TIMING_GATE_ERROR';
      v_comm_error := left(coalesce(SQLERRM, 'UNKNOWN_COMMS_ERROR'), 1000);
      v_comm_message_kind := coalesce(v_comm_message_kind, 'PAYOUT_NOTICE_AND_REMITTANCE');
      v_payout_notice_result := jsonb_build_object(
        'ok', false,
        'trigger_status', v_comm_trigger_status,
        'error', v_comm_error,
        'message_kind', 'PAYOUT_NOTICE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id::text
      );
      v_remittance_result := jsonb_build_object(
        'ok', false,
        'trigger_status', v_comm_trigger_status,
        'error', v_comm_error,
        'message_kind', 'REMITTANCE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id::text,
        'scope', 'ALL'
      );
      v_payout_notice_dispatch_required := false;
      v_remittance_dispatch_required := false;
      v_payout_notice_targeted_count := 0;
      v_remittance_targeted_count := 0;
      v_comm_result := jsonb_build_object(
        'ok', false,
        'trigger', 'ON_EXECUTION',
        'configured_timing', v_timing_setting_if_known,
        'trigger_status', v_comm_trigger_status,
        'error', v_comm_error,
        'message_kind', v_comm_message_kind,
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'operation_id', NULL::text,
        'deferred', false,
        'suppressed', false,
        'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
        'remittance', coalesce(v_remittance_result, '{}'::jsonb),
        'queue_result', jsonb_build_object(
          'ok', false,
          'trigger', 'ON_EXECUTION',
          'configured_timing', v_timing_setting_if_known,
          'trigger_status', v_comm_trigger_status,
          'error', v_comm_error,
          'operation_id', NULL::text,
          'deferred', false,
          'suppressed', false
        ),
        'execution_mode', v_execution_mode
      );
    end;

    v_comm_result := coalesce(v_comm_result, '{}'::jsonb) || jsonb_build_object(
      'trigger', coalesce(nullif(btrim(coalesce(v_comm_result->>'trigger', '')), ''), 'ON_EXECUTION'),
      'configured_timing', coalesce(nullif(btrim(coalesce(v_comm_result->>'configured_timing', '')), ''), nullif(btrim(coalesce(v_comm_result#>>'{queue_result,configured_timing}', '')), ''), v_timing_setting_if_known),
      'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
      'remittance', coalesce(v_remittance_result, '{}'::jsonb)
    );

    v_worker_communications := jsonb_build_object(
      'automatic_commit_stage', true,
      'message_kind', coalesce(v_comm_message_kind, 'PAYOUT_NOTICE_AND_REMITTANCE'),
      'trigger_status', v_comm_trigger_status,
      'error', v_comm_error,
      'dispatch_required', (v_payout_notice_dispatch_required = true or v_remittance_dispatch_required = true),
      'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
      'remittance', coalesce(v_remittance_result, '{}'::jsonb),
      'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb),
      'result', coalesce(v_comm_result, '{}'::jsonb),
      'execution_mode', v_execution_mode,
      'suppress_remittances', v_suppress_remittances
    );

    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:NON_NATIVE_MODE_OK',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'execution_mode', v_execution_mode,
          'schedule_kind', v_kind,
          'scheduled_at_utc', v_sched_at::text,
          'authoritative_payment_date', v_authoritative_payment_date::text,
          'pending_transfers', v_pending_transfers,
          'worker_communications', v_worker_communications
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'status', (select pb_non_native_ret.status from public.pay_batches pb_non_native_ret where pb_non_native_ret.id = p_pay_batch_id),
      'schedule_kind', v_kind,
      'scheduled_at_utc', v_sched_at::text,
      'authoritative_payment_date', v_authoritative_payment_date::text,
      'authoritative_payment_date_source', 'EXECUTION_INTENT_PAYMENT_DATE',
      'funding_account_ref', v_funding,
      'funds_warning_hours_json', v_warn,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot,
      'rail_env_snapshot', v_batch.rail_env_snapshot,
      'execution_mode', v_execution_mode,
      'execution_intent_json', (select pb_non_native_ret.execution_intent_json from public.pay_batches pb_non_native_ret where pb_non_native_ret.id = p_pay_batch_id),
      'execution_commit_state', (select pb_non_native_ret.execution_commit_state from public.pay_batches pb_non_native_ret where pb_non_native_ret.id = p_pay_batch_id),
      'execution_commit_ref', (select pb_non_native_ret.execution_commit_ref from public.pay_batches pb_non_native_ret where pb_non_native_ret.id = p_pay_batch_id),
      'execution_committed_at_utc', (select case when pb_non_native_ret.execution_committed_at_utc is null then null else pb_non_native_ret.execution_committed_at_utc::text end from public.pay_batches pb_non_native_ret where pb_non_native_ret.id = p_pay_batch_id),
      'finance_reservations', jsonb_build_object(
        'committed_count', 0,
        'committed_amount', 0
      ),
      'remittance_trigger', 'ON_EXECUTION',
      'remittance_configured_timing', coalesce(v_comm_result->>'configured_timing', v_comm_result#>>'{queue_result,configured_timing}'),
      'remittance_deferred', lower(btrim(coalesce(v_comm_result->>'deferred', 'false'))) in ('true','1','yes','y','on'),
      'remittance_suppressed', lower(btrim(coalesce(v_comm_result->>'suppressed', 'false'))) in ('true','1','yes','y','on'),
      'remittance_operation_queued', lower(btrim(coalesce(v_comm_result->>'operation_queued', 'false'))) in ('true','1','yes','y','on'),
      'remittance_child_operation_id', coalesce(
        nullif(btrim(coalesce(v_comm_result->>'operation_id', '')), ''),
        nullif(btrim(coalesce(v_comm_result#>>'{queue_result,operation_id}', '')), '')
      ),
      'remittance_trigger_status', coalesce(
        nullif(btrim(coalesce(v_comm_result->>'trigger_status', '')), ''),
        nullif(btrim(coalesce(v_comm_result#>>'{queue_result,trigger_status}', '')), ''),
        v_comm_trigger_status
      ),
      'remittance_message_kind', v_comm_message_kind,
      'worker_communications', v_worker_communications,
      'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb)
    );
  end if;

  if v_provider = 'CSV' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SCHEDULING_NOT_SUPPORTED_FOR_MANUAL_RAIL',
      'message', format('pay_batch_schedule: scheduling is not supported for manual rail_provider_snapshot=%s', v_batch.rail_provider_snapshot),
      'pay_batch_id', p_pay_batch_id::text,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot
    )::text;
  end if;

  if coalesce(v_cfg.rail_supports_scheduling,false) = false then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'SCHEDULING_DISABLED_IN_SETTINGS',
      'message', 'pay_batch_schedule: scheduling is not enabled in settings_defaults (rail_supports_scheduling=false)',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  v_need_name_check := (coalesce(v_cfg.rail_supports_name_check,false) = true);
  v_requires_payee_map := true;

  v_warn := coalesce(p_warning_hours_json, v_cfg.funds_warning_hours_json);
  if v_warn is not null and jsonb_typeof(v_warn) <> 'array' then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'WARNING_HOURS_JSON_INVALID',
      'message', 'pay_batch_schedule: warning_hours_json must be a JSON array',
      'pay_batch_id', p_pay_batch_id::text
    )::text;
  end if;

  if v_kind = 'IMMEDIATE' then
    v_sched_at := now();
  else
    if p_scheduled_at_utc is null then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE',
        'code', 'SCHEDULED_AT_REQUIRED',
        'message', 'pay_batch_schedule: scheduled_at_utc is required when schedule_kind=SCHEDULED',
        'pay_batch_id', p_pay_batch_id::text,
        'schedule_kind', v_kind
      )::text;
    end if;
    v_sched_at := p_scheduled_at_utc;
  end if;

  v_authoritative_payment_date := (v_sched_at at time zone 'Europe/London')::date;

  if v_funding is null then
    v_funding := nullif(btrim(coalesce(v_cfg.rail_default_funding_account_ref,'')), '');
  end if;
  if v_funding is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'FUNDING_ACCOUNT_REQUIRED',
      'message', format('pay_batch_schedule: funding_account_ref is required for this rail (provider=%s)', v_batch.rail_provider_snapshot),
      'pay_batch_id', p_pay_batch_id::text,
      'rail_provider_snapshot', v_batch.rail_provider_snapshot
    )::text;
  end if;

  select count(*)::int
  into v_pending_transfers
  from public.pay_bank_transfers pbt
  where pbt.pay_batch_id = p_pay_batch_id
    and upper(coalesce(pbt.status,'')) = 'PENDING';

  if v_pending_transfers = 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE',
      'code', 'NO_PENDING_TRANSFERS',
      'message', 'pay_batch_schedule: no PENDING transfers exist for this batch (execute-bank required first)',
      'pay_batch_id', p_pay_batch_id::text,
      'ui_hint', 'RERUN_PREVIEW_OR_EXECUTE_BANK'
    )::text;
  end if;

  if v_batch_kind_fixed = 'LOANS' then
    select count(*)::int
    into v_bad_loans_payee_ct
    from public.pay_bank_transfers pbt2
    where pbt2.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt2.status,'')) = 'PENDING'
      and upper(
        coalesce(
          pbt2.payee_entity_kind,
          case
            when upper(coalesce(pbt2.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA'
            else 'CANDIDATE'
          end
        )
      ) <> 'CANDIDATE';

    if v_bad_loans_payee_ct > 0 then
      begin
        perform public._imp_debug_audit(
          p_actor_user_id,
          'PAY_BATCH_SCHEDULE:BLOCKED_LOANS_PAYEE_KIND',
          jsonb_build_object(
            'pay_batch_id', p_pay_batch_id::text,
            'bad_transfer_count', v_bad_loans_payee_ct
          ),
          'pay_batches',
          p_pay_batch_id::text
        );
      exception when others then
        null;
      end;

      raise exception '%', jsonb_build_object(
        'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
        'code', 'LOANS_PAYEE_MUST_BE_CANDIDATE',
        'message', 'pay_batch_schedule: LOANS batches must pay candidates (not umbrellas)',
        'pay_batch_id', p_pay_batch_id::text,
        'bad_transfer_count', v_bad_loans_payee_ct,
        'ui_hint', 'REGENERATE_TRANSFERS_AS_CANDIDATE_PAYEES'
      )::text;
    end if;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')), '') as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(case when t.bank_hash is null or btrim(t.bank_hash) = '' then 1 else 0 end)::int
  into v_missing_bank
  from t;

  if v_missing_bank > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_BANK_DETAILS',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_missing_bank
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_BANK_DETAILS',
      'message', format('pay_batch_schedule: BLOCKED_BANK_DETAILS for %s payee(s)', v_missing_bank::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_missing_bank,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')), '') as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_need_name_check = true then
          case
            when coalesce(bnc.status,'UNVERIFIED') = 'PASS' then 0
            when (bnc.override_reason is not null and bnc.override_hash = t.bank_hash) then 0
            else 1
          end
        else 0
      end
    )::int
  into v_blocked_name
  from t
  left join public.bank_name_checks bnc
    on bnc.rail_provider = v_batch.rail_provider_snapshot
   and bnc.rail_env = v_batch.rail_env_snapshot
   and bnc.entity_kind = t.payee_kind
   and bnc.entity_id = t.payee_id
   and bnc.bank_details_hash = t.bank_hash;

  if v_blocked_name > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_NAME_CHECK',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_blocked_name
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_NAME_CHECK',
      'message', format('pay_batch_schedule: BLOCKED_NAME_CHECK for %s payee(s)', v_blocked_name::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_blocked_name,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  with t as (
    select
      upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) as payee_kind,
      coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else pbt.candidate_id end
      ) as payee_id,
      nullif(btrim(coalesce(pbt.bank_details_hash_snapshot,'')), '') as bank_hash
    from public.pay_bank_transfers pbt
    left join public.candidates c
      on c.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then null else pbt.candidate_id end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'CANDIDATE'
    left join public.umbrellas u
      on u.id = coalesce(
        pbt.payee_entity_id,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then pbt.umbrella_id else null end
      )
     and upper(coalesce(pbt.payee_entity_kind,
        case when upper(coalesce(pbt.pay_channel,'')) = 'UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end
      )) = 'UMBRELLA'
    where pbt.pay_batch_id = p_pay_batch_id
      and upper(coalesce(pbt.status,'')) = 'PENDING'
    group by 1,2,3
  )
  select
    sum(
      case
        when v_requires_payee_map = true then
          case when bpm.payee_id is null then 1 else 0 end
        else 0
      end
    )::int
  into v_missing_map
  from t
  left join public.bank_payee_map bpm
    on bpm.rail_provider = v_batch.rail_provider_snapshot
   and bpm.rail_env = v_batch.rail_env_snapshot
   and bpm.entity_kind = t.payee_kind
   and bpm.entity_id = t.payee_id
   and bpm.bank_details_hash = t.bank_hash;

  if v_missing_map > 0 then
    begin
      perform public._imp_debug_audit(
        p_actor_user_id,
        'PAY_BATCH_SCHEDULE:BLOCKED_NO_PAYEE_MAP',
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'count', v_missing_map
        ),
        'pay_batches',
        p_pay_batch_id::text
      );
    exception when others then
      null;
    end;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_BATCH_SCHEDULE_BLOCKED',
      'code', 'BLOCKED_NO_PAYEE_MAP',
      'message', format('pay_batch_schedule: BLOCKED_NO_PAYEE_MAP for %s payee(s)', v_missing_map::text),
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_missing_map,
      'ui_hint', 'RERUN_PREVIEW'
    )::text;
  end if;

  update public.pay_batches pb
  set
    schedule_kind = v_kind,
    scheduled_at_utc = v_sched_at,
    scheduled_by_user_id = p_actor_user_id,
    funding_account_ref = v_funding,
    funds_warning_hours_json = v_warn,
    authoritative_payment_date = v_authoritative_payment_date,
    authoritative_payment_date_source = 'SCHEDULED_AT_UTC',
    pay_date = v_authoritative_payment_date,
    status = 'SCHEDULED',
    execution_commit_state = v_execution_commit_state,
    execution_commit_ref = v_execution_commit_ref,
    execution_committed_at_utc = v_execution_committed_at_utc,
    execution_intent_json = coalesce(v_execution_intent_json, '{}'::jsonb) || jsonb_build_object(
      'execution_mode', v_execution_mode,
      'payment_date', v_authoritative_payment_date::text,
      'schedule_kind', v_kind,
      'scheduled_at_utc', case when v_sched_at is null then null else v_sched_at::text end,
      'operation_id', case when p_operation_id is null then null else p_operation_id::text end,
      'freshness_result_hash', v_stored_freshness_hash,
      'freshness_scope_hash', v_stored_freshness_scope_hash
    )
  where pb.id = p_pay_batch_id;

  update public.pay_advance_reservations par
  set
    status = 'COMMITTED',
    committed_at_utc = coalesce(par.committed_at_utc, v_commit_ts),
    updated_by_user_id = p_actor_user_id
  where par.pay_batch_id = p_pay_batch_id
    and upper(coalesce(par.status,'')) = 'RESERVED';

  get diagnostics v_reservations_committed_count = row_count;

  select round(coalesce(sum(par.reserved_amount),0),2)
  into v_reservations_committed_amount
  from public.pay_advance_reservations par
  where par.pay_batch_id = p_pay_batch_id
    and upper(coalesce(par.status,'')) = 'COMMITTED';

  insert into public.pay_finance_case_events(
    finance_case_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    reservation_id,
    before_json,
    after_json,
    reason,
    note
  )
  select
    par.finance_case_id,
    'RESERVATION_COMMITTED',
    v_commit_ts,
    p_actor_user_id,
    p_pay_batch_id,
    par.id,
    jsonb_build_object('reservation_status', 'RESERVED'),
    jsonb_build_object(
      'reservation_status', 'COMMITTED',
      'authoritative_payment_date', v_authoritative_payment_date::text,
      'schedule_kind', v_kind,
      'scheduled_at_utc', v_sched_at::text
    ),
    'schedule_commit',
    null
  from public.pay_advance_reservations par
  where par.pay_batch_id = p_pay_batch_id
    and upper(coalesce(par.status,'')) = 'COMMITTED'
    and par.committed_at_utc = v_commit_ts;

  begin
    v_comm_result := public.pay_remittance_maybe_queue_for_trigger(
      p_pay_batch_id => p_pay_batch_id,
      p_trigger => 'ON_EXECUTION',
      p_scope => 'ALL',
      p_actor_user_id => p_actor_user_id,
      p_only_confirmed => false,
      p_root_operation_id => CASE WHEN v_operation_mode THEN p_operation_id ELSE NULL::uuid END,
      p_operation_mode => v_operation_mode
    );

    if coalesce((v_comm_result->>'ok')::boolean, true) = false then
      raise exception '%', coalesce(
        nullif(btrim(coalesce(v_comm_result->>'error','')), ''),
        'pay_batch_schedule: remittance timing queue stage failed'
      );
    end if;

    v_comm_trigger_status := coalesce(
      nullif(btrim(coalesce(v_comm_result->>'trigger_status','')), ''),
      nullif(btrim(coalesce(v_comm_result#>>'{queue_result,trigger_status}','')), ''),
      case
        when lower(btrim(coalesce(v_comm_result->>'deferred', 'false'))) in ('true','1','yes','y','on') then 'REMITTANCE_QUEUE_DEFERRED_BY_TIMING'
        when lower(btrim(coalesce(v_comm_result->>'suppressed', 'false'))) in ('true','1','yes','y','on') then 'SUPPRESSED_BY_EXECUTION_INTENT'
        when lower(btrim(coalesce(v_comm_result->>'operation_queued', 'false'))) in ('true','1','yes','y','on') then 'REMITTANCE_QUEUE_OPERATION_READY'
        when lower(btrim(coalesce(v_comm_result->>'dispatch_required', v_comm_result#>>'{queue_result,dispatch_required}', 'false'))) in ('true','1','yes','y','on') then 'QUEUE_STAGE_BUILD_READY'
        else 'ON_EXECUTION_QUEUE_GATE_PROCESSED'
      end
    );
    v_comm_message_kind := coalesce(
      nullif(btrim(coalesce(v_comm_result->>'message_kind','')), ''),
      case when v_batch_kind_fixed = 'LOANS' then 'PAYOUT_NOTICE' else 'REMITTANCE' end
    );
    v_comm_error := nullif(btrim(coalesce(v_comm_result->>'error','')), '');

    v_payout_notice_result := case
      when v_batch_kind_fixed = 'LOANS' then coalesce(v_comm_result->'queue_result', v_comm_result)
      else '{}'::jsonb
    end;
    v_remittance_result := case
      when v_batch_kind_fixed <> 'LOANS' then coalesce(v_comm_result->'queue_result', v_comm_result)
      else '{}'::jsonb
    end;

    v_payout_notice_dispatch_required := (
      v_batch_kind_fixed = 'LOANS'
      and lower(btrim(coalesce(v_comm_result->>'dispatch_required', v_comm_result#>>'{queue_result,dispatch_required}', 'false'))) in ('true','1','yes','y','on')
    );
    v_remittance_dispatch_required := (
      v_batch_kind_fixed <> 'LOANS'
      and lower(btrim(coalesce(v_comm_result->>'dispatch_required', v_comm_result#>>'{queue_result,dispatch_required}', 'false'))) in ('true','1','yes','y','on')
    );

    if btrim(coalesce(coalesce(v_comm_result->>'candidate_count_targeted', v_comm_result#>>'{queue_result,candidate_count_targeted}'), '')) ~ '^[0-9]+$' then
      if v_batch_kind_fixed = 'LOANS' then
        v_payout_notice_targeted_count := coalesce(v_comm_result->>'candidate_count_targeted', v_comm_result#>>'{queue_result,candidate_count_targeted}')::int;
        v_remittance_targeted_count := 0;
      else
        v_remittance_targeted_count := coalesce(v_comm_result->>'candidate_count_targeted', v_comm_result#>>'{queue_result,candidate_count_targeted}')::int;
        v_payout_notice_targeted_count := 0;
      end if;
    else
      v_payout_notice_targeted_count := 0;
      v_remittance_targeted_count := 0;
    end if;

    insert into public.audit_events(
      actor_user_id,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    values (
      p_actor_user_id,
      'pay_batch',
      p_pay_batch_id::text,
      'PAY_BATCH_COMMIT_STAGE_COMMUNICATION_TIMING_GATE',
      null,
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'batch_kind_fixed', v_batch_kind_fixed,
        'schedule_kind', v_kind,
        'scheduled_at_utc', v_sched_at::text,
        'authoritative_payment_date', v_authoritative_payment_date::text,
        'trigger_status', v_comm_trigger_status,
        'message_kind', v_comm_message_kind,
        'result', coalesce(v_comm_result, '{}'::jsonb),
        'execution_mode', v_execution_mode,
        'suppress_remittances', v_suppress_remittances
      ),
      'commit_stage_communication_timing_gate'
    );
  exception when others then
    v_comm_trigger_status := 'COMMIT_STAGE_QUEUE_TIMING_GATE_ERROR';
    v_comm_error := left(coalesce(SQLERRM,'UNKNOWN_COMMS_ERROR'), 1000);
    v_comm_message_kind := coalesce(v_comm_message_kind, 'PAYOUT_NOTICE_AND_REMITTANCE');
    v_comm_result := jsonb_build_object(
      'ok', false,
      'trigger', 'ON_EXECUTION',
      'configured_timing', v_timing_setting_if_known,
      'trigger_status', v_comm_trigger_status,
      'error', v_comm_error,
      'message_kind', v_comm_message_kind,
      'automatic_commit_stage', (v_provider <> 'CSV'),
      'dispatch_required', false,
      'operation_id', NULL::text,
      'deferred', false,
      'suppressed', false,
      'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
      'remittance', coalesce(v_remittance_result, '{}'::jsonb),
      'queue_result', jsonb_build_object(
        'ok', false,
        'trigger', 'ON_EXECUTION',
        'configured_timing', v_timing_setting_if_known,
        'trigger_status', v_comm_trigger_status,
        'error', v_comm_error,
        'operation_id', NULL::text,
        'deferred', false,
        'suppressed', false
      )
    );

    begin
      insert into public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      values (
        p_actor_user_id,
        'pay_batch',
        p_pay_batch_id::text,
        'PAY_BATCH_COMMIT_STAGE_COMMUNICATION_ERROR',
        null,
        jsonb_build_object(
          'pay_batch_id', p_pay_batch_id::text,
          'batch_kind_fixed', v_batch_kind_fixed,
          'schedule_kind', v_kind,
          'scheduled_at_utc', v_sched_at::text,
          'authoritative_payment_date', v_authoritative_payment_date::text,
          'trigger_status', v_comm_trigger_status,
          'message_kind', v_comm_message_kind,
          'error', v_comm_error,
          'queue_result', coalesce(v_comm_result, '{}'::jsonb)
        ),
        'commit_stage_communication'
      );
    exception when others then
      null;
    end;

    if v_execution_commit_state = 'NOT_SUBMITTED' then
      begin
        update public.pay_batches as pb_schedule_rollback
        set
          status = v_batch.status,
          schedule_kind = v_batch.schedule_kind,
          scheduled_at_utc = v_batch.scheduled_at_utc,
          scheduled_by_user_id = v_batch.scheduled_by_user_id,
          funding_account_ref = v_batch.funding_account_ref,
          funds_warning_hours_json = v_batch.funds_warning_hours_json,
          pay_date = v_batch.pay_date,
          authoritative_payment_date = v_batch.authoritative_payment_date,
          authoritative_payment_date_source = v_batch.authoritative_payment_date_source,
          execution_commit_state = v_execution_commit_state,
          execution_commit_ref = v_execution_commit_ref,
          execution_committed_at_utc = v_execution_committed_at_utc,
          execution_intent_json = v_batch.execution_intent_json
        where pb_schedule_rollback.id = p_pay_batch_id
          and upper(coalesce(pb_schedule_rollback.execution_commit_state, 'NOT_SUBMITTED')) = 'NOT_SUBMITTED'
          and not exists (
            select 1
            from public.pay_bank_transfer_events as rollback_transfer_event
            where rollback_transfer_event.pay_batch_id = p_pay_batch_id
          )
          and not exists (
            select 1
            from public.pay_bank_transfers as rollback_transfer
            where rollback_transfer.pay_batch_id = p_pay_batch_id
              and (
                nullif(btrim(coalesce(rollback_transfer.rail_tx_id, '')), '') is not null
                or upper(coalesce(rollback_transfer.status, '')) in ('SUBMITTED', 'PROCESSING', 'SENT', 'COMPLETED', 'PAID', 'SETTLED')
              )
          );
      exception when others then
        null;
      end;
    end if;

    v_worker_communications := jsonb_build_object(
      'automatic_commit_stage', (v_provider <> 'CSV'),
      'message_kind', coalesce(nullif(btrim(coalesce(v_comm_result->>'message_kind','')), ''), v_comm_message_kind, 'PAYOUT_NOTICE_AND_REMITTANCE'),
      'trigger_status', v_comm_trigger_status,
      'error', v_comm_error,
      'dispatch_required', false,
      'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
      'remittance', coalesce(v_remittance_result, '{}'::jsonb),
      'result', coalesce(v_comm_result, '{}'::jsonb),
      'execution_mode', v_execution_mode,
      'suppress_remittances', v_suppress_remittances
    );

    return jsonb_build_object(
      'ok', false,
      'error', 'PAY_BATCH_COMMIT_STAGE_COMMUNICATION_ERROR',
      'code', 'COMMIT_STAGE_COMMUNICATION_ERROR',
      'message', 'pay_batch_schedule: commit-stage communication failed before provider submission boundary',
      'pay_batch_id', p_pay_batch_id::text,
      'status', (select pb2.status from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'schedule_kind', (select pb2.schedule_kind from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'scheduled_at_utc', (select case when pb2.scheduled_at_utc is null then null else pb2.scheduled_at_utc::text end from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'authoritative_payment_date', (select case when pb2.authoritative_payment_date is null then null else pb2.authoritative_payment_date::text end from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'authoritative_payment_date_source', (select pb2.authoritative_payment_date_source from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'funding_account_ref', (select pb2.funding_account_ref from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'funds_warning_hours_json', (select pb2.funds_warning_hours_json from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'execution_commit_state', (select pb2.execution_commit_state from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'execution_commit_ref', (select pb2.execution_commit_ref from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'execution_committed_at_utc', (select case when pb2.execution_committed_at_utc is null then null else pb2.execution_committed_at_utc::text end from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'execution_intent_json', (select pb2.execution_intent_json from public.pay_batches as pb2 where pb2.id = p_pay_batch_id),
      'execution_mode', v_execution_mode,
      'suppress_remittances', v_suppress_remittances,
      'remittance_trigger', 'ON_EXECUTION',
      'remittance_configured_timing', coalesce(v_comm_result->>'configured_timing', v_comm_result#>>'{queue_result,configured_timing}'),
      'remittance_deferred', false,
      'remittance_suppressed', false,
      'remittance_operation_queued', false,
      'remittance_child_operation_id', null,
      'remittance_trigger_status', v_comm_trigger_status,
      'remittance_message_kind', v_comm_message_kind,
      'worker_communications', v_worker_communications,
      'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb)
    );
  end;
  v_worker_communications := jsonb_build_object(
    'automatic_commit_stage', (v_provider <> 'CSV'),
    'message_kind', coalesce(nullif(btrim(coalesce(v_comm_result->>'message_kind','')), ''), v_comm_message_kind, 'PAYOUT_NOTICE_AND_REMITTANCE'),
    'trigger_status', v_comm_trigger_status,
    'error', v_comm_error,
    'dispatch_required', (v_payout_notice_dispatch_required = true or v_remittance_dispatch_required = true),
    'payout_notice', coalesce(v_payout_notice_result, '{}'::jsonb),
    'remittance', coalesce(v_remittance_result, '{}'::jsonb),
    'result', coalesce(v_comm_result, '{}'::jsonb),
    'execution_mode', v_execution_mode,
    'suppress_remittances', v_suppress_remittances
  );

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_SCHEDULE:OK',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'schedule_kind', v_kind,
        'scheduled_at_utc', v_sched_at::text,
        'authoritative_payment_date', v_authoritative_payment_date::text,
        'funding_account_ref', v_funding,
        'rail_provider_snapshot', v_batch.rail_provider_snapshot,
        'rail_env_snapshot', v_batch.rail_env_snapshot,
        'execution_mode', v_execution_mode,
        'suppress_remittances', v_suppress_remittances,
        'pending_transfers', v_pending_transfers,
        'reservations_committed_count', v_reservations_committed_count,
        'reservations_committed_amount', v_reservations_committed_amount,
        'worker_communications', v_worker_communications
      ),
      'pay_batches',
      p_pay_batch_id::text
    );
  exception when others then
    null;
  end;

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'status', (select pb2.status from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'schedule_kind', (select pb2.schedule_kind from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'scheduled_at_utc', (select case when pb2.scheduled_at_utc is null then null else pb2.scheduled_at_utc::text end from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'authoritative_payment_date', (select case when pb2.authoritative_payment_date is null then null else pb2.authoritative_payment_date::text end from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'authoritative_payment_date_source', (select pb2.authoritative_payment_date_source from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'funding_account_ref', (select pb2.funding_account_ref from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'funds_warning_hours_json', (select pb2.funds_warning_hours_json from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'rail_provider_snapshot', v_batch.rail_provider_snapshot,
    'rail_env_snapshot', v_batch.rail_env_snapshot,
    'execution_commit_state', (select pb2.execution_commit_state from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'execution_commit_ref', (select pb2.execution_commit_ref from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'execution_committed_at_utc', (select case when pb2.execution_committed_at_utc is null then null else pb2.execution_committed_at_utc::text end from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'execution_mode', v_execution_mode,
    'execution_intent_json', (select pb2.execution_intent_json from public.pay_batches pb2 where pb2.id = p_pay_batch_id),
    'suppress_remittances', v_suppress_remittances,
    'finance_reservations', jsonb_build_object(
      'committed_count', v_reservations_committed_count,
      'committed_amount', v_reservations_committed_amount
    ),
    'remittance_trigger', 'ON_EXECUTION',
    'remittance_configured_timing', coalesce(v_comm_result->>'configured_timing', v_comm_result#>>'{queue_result,configured_timing}'),
    'remittance_deferred', lower(btrim(coalesce(v_comm_result->>'deferred', 'false'))) in ('true','1','yes','y','on'),
    'remittance_suppressed', lower(btrim(coalesce(v_comm_result->>'suppressed', 'false'))) in ('true','1','yes','y','on'),
    'remittance_operation_queued', lower(btrim(coalesce(v_comm_result->>'operation_queued', 'false'))) in ('true','1','yes','y','on'),
    'remittance_child_operation_id', coalesce(
      nullif(btrim(coalesce(v_comm_result->>'operation_id', '')), ''),
      nullif(btrim(coalesce(v_comm_result#>>'{queue_result,operation_id}', '')), '')
    ),
    'remittance_trigger_status', coalesce(
      nullif(btrim(coalesce(v_comm_result->>'trigger_status', '')), ''),
      nullif(btrim(coalesce(v_comm_result#>>'{queue_result,trigger_status}', '')), ''),
      v_comm_trigger_status
    ),
    'remittance_message_kind', v_comm_message_kind,
    'worker_communications', v_worker_communications,
    'remittance_queue_stage_result', coalesce(v_comm_result, '{}'::jsonb)
  );
end;
$function$;

ALTER FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) TO postgres;
GRANT EXECUTE ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) TO anon;
GRANT EXECUTE ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_batch_schedule(uuid,text,timestamp with time zone,text,jsonb,uuid,uuid,text) TO service_role;
