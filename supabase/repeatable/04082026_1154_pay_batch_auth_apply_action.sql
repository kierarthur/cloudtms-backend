-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.

create or replace function public.pay_batch_auth_apply_action(
  p_auth_request_id uuid,
  p_actor_user_id uuid,
  p_action text,
  p_note text default null
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, private, extensions, pg_temp
set statement_timeout = '6000ms'
set lock_timeout = '1000ms'
as $$
declare
  v_now timestamptz := now();
  v_action text := upper(btrim(coalesce(p_action,'')));

  v_req record;
  v_user record;

  v_inserted_id uuid := null;
  v_approved_count int := 0;

  v_became_authorised boolean := false;
  v_new_auth_state text := null;
  v_new_batch_status text := null;
  v_execution_intent_json jsonb := '{}'::jsonb;

  v_schedule_kind text := null;
  v_scheduled_at_utc timestamptz := null;
  v_funding_account_ref text := null;
  v_funds_warning_hours_json jsonb := null;
  v_schedule_result jsonb := '{}'::jsonb;
  v_worker_communications jsonb := '{}'::jsonb;
  v_remittance_queue_stage_result jsonb := '{}'::jsonb;
  v_remittance_send_timing text := 'ON_EXECUTION';
  v_pay_batch_id uuid := null::uuid;
  v_mutation_guard jsonb := '{}'::jsonb;
  v_active_scope_hash text := null::text;
  v_payment_operation_id uuid := null::uuid;
begin
  if p_auth_request_id is null then
    raise exception 'pay_batch_auth_apply_action: auth_request_id is required';
  end if;
  if p_actor_user_id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user_id is required';
  end if;

  if v_action not in ('AUTHORISE','USE_GOLDEN_KEY','REJECT') then
    raise exception 'pay_batch_auth_apply_action: invalid action (AUTHORISE|USE_GOLDEN_KEY|REJECT)';
  end if;

  select auth_lookup.pay_batch_id
  into v_pay_batch_id
  from public.pay_batch_auth_requests as auth_lookup
  where auth_lookup.id = p_auth_request_id;

  if v_pay_batch_id is null then
    raise exception 'pay_batch_auth_apply_action: auth_request not found';
  end if;

  v_mutation_guard := private.pay_payment_mutation_guard_v1(
    v_pay_batch_id,
    null::uuid,
    'NEW_PAYMENT_ACTION'
  );

  if coalesce((v_mutation_guard->>'blocked')::boolean, true) then
    raise exception 'PAYMENT_CHANGE_IN_PROGRESS'
      using errcode = 'P0001', detail = jsonb_build_object(
        'code', coalesce(v_mutation_guard->>'code', 'PAYMENT_CHANGE_IN_PROGRESS'),
        'message', coalesce(v_mutation_guard->>'message', 'A payment change is in progress.'),
        'pay_batch_id', v_pay_batch_id
      )::text;
  end if;

  select
    pbar.id,
    pbar.pay_batch_id,
    pbar.state,
    pbar.required_quantity,
    pbar.schedule_kind,
    pbar.scheduled_at_utc,
    pbar.funding_account_ref,
    pbar.funds_warning_hours_json,
    pbar.execution_intent_json
  into v_req
  from public.pay_batch_auth_requests pbar
  where pbar.id = p_auth_request_id
  for update;

  if v_req.id is null then
    raise exception 'pay_batch_auth_apply_action: auth_request not found';
  end if;

  if v_req.state <> 'AWAITING' then
    raise exception 'pay_batch_auth_apply_action: auth_request must be AWAITING (current=%)', v_req.state;
  end if;

  if v_req.execution_intent_json is not null and jsonb_typeof(v_req.execution_intent_json) = 'object' then
    v_execution_intent_json := v_req.execution_intent_json;
  else
    v_execution_intent_json := '{}'::jsonb;
  end if;

  v_active_scope_hash := private.pay_payment_correction_sha256_v1(
    jsonb_build_object(
      'version', 1,
      'pay_batch_id', v_req.pay_batch_id,
      'active_items', (
        select coalesce(
          jsonb_agg(
            jsonb_build_array(
              candidate_row.id,
              item_row.id,
              round(coalesce(item_row.amount_inc_vat, 0) * 100)::bigint,
              item_row.item_type,
              item_row.pay_channel,
              item_row.reservation_id,
              item_row.finance_component_id,
              item_row.pay_bank_transfer_id
            )
            order by candidate_row.id, item_row.id
          ),
          '[]'::jsonb
        )
        from public.pay_batch_candidates as candidate_row
        join public.pay_batch_items as item_row
          on item_row.pay_batch_candidate_id = candidate_row.id
        where candidate_row.pay_batch_id = v_req.pay_batch_id
          and coalesce(item_row.is_voided, false) is not true
      )
    )
  );

  if nullif(btrim(coalesce(v_execution_intent_json->>'active_scope_hash', '')), '') is not null
     and v_execution_intent_json->>'active_scope_hash' is distinct from v_active_scope_hash then
    raise exception 'PAY_BATCH_AUTH_SCOPE_STALE'
      using errcode = 'P0001', detail = jsonb_build_object(
        'code', 'AUTH_SCOPE_STALE',
        'pay_batch_id', v_req.pay_batch_id,
        'expected_active_scope_hash', v_execution_intent_json->>'active_scope_hash',
        'actual_active_scope_hash', v_active_scope_hash
      )::text;
  end if;

  v_execution_intent_json := v_execution_intent_json || jsonb_build_object(
    'active_scope_hash', v_active_scope_hash,
    'active_scope_revalidated_at_utc', v_now
  );

  select coalesce(nullif(btrim(coalesce(sd.payment_remittance_send_timing, '')), ''), 'ON_EXECUTION')
  into v_remittance_send_timing
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  v_remittance_send_timing := coalesce(nullif(btrim(coalesce(v_remittance_send_timing, '')), ''), 'ON_EXECUTION');
  if v_remittance_send_timing not in ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED') then
    v_remittance_send_timing := 'ON_EXECUTION';
  end if;

  select
    tu.id,
    tu.is_active,
    tu.payment_authoriser,
    tu.payment_golden_key
  into v_user
  from public.tms_users tu
  where tu.id = p_actor_user_id
  limit 1;

  if v_user.id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user not found';
  end if;

  if coalesce(v_user.is_active,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user is not active';
  end if;

  if coalesce(v_user.payment_authoriser,false) = false and coalesce(v_user.payment_golden_key,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user is not an authoriser';
  end if;

  if v_action = 'USE_GOLDEN_KEY' and coalesce(v_user.payment_golden_key,false) = false then
    raise exception 'pay_batch_auth_apply_action: actor_user does not have payment golden key';
  end if;

  perform public._imp_debug_audit(
    p_actor_user_id,
    'PAY_BATCH_AUTH_APPLY_ACTION_START',
    jsonb_build_object(
      'auth_request_id', p_auth_request_id::text,
      'pay_batch_id', v_req.pay_batch_id::text,
      'action', v_action,
      'required_quantity', v_req.required_quantity,
      'payment_remittance_send_timing', v_remittance_send_timing
    ),
    'pay_batch_auth_requests',
    p_auth_request_id::text,
    null::jsonb,
    null::text,
    null::text,
    null::text
  );

  insert into public.pay_batch_auth_actions(
    auth_request_id,
    pay_batch_id,
    actor_user_id,
    action,
    action_at_utc,
    note
  )
  values (
    p_auth_request_id,
    v_req.pay_batch_id,
    p_actor_user_id,
    v_action,
    v_now,
    p_note
  )
  on conflict on constraint ux_pay_batch_auth_actions_one_per_user
  do nothing
  returning id into v_inserted_id;

  if v_inserted_id is null then
    raise exception 'pay_batch_auth_apply_action: actor_user has already acted on this request';
  end if;

  if v_action = 'REJECT' then
    update public.pay_batch_auth_requests pbar2
    set
      state = 'REJECTED',
      finalised_at_utc = v_now,
      finalised_by_user_id = p_actor_user_id,
      golden_key_used = false,
      golden_key_user_id = null
    where pbar2.id = p_auth_request_id;

    update public.pay_batch_auth_tokens pbat2
    set used_at_utc = v_now
    where pbat2.auth_request_id = p_auth_request_id
      and pbat2.used_at_utc is null;

    update public.pay_batches pb2
    set
      status = 'READY',
      schedule_kind = null,
      scheduled_at_utc = null,
      scheduled_by_user_id = null,
      funding_account_ref = null,
      funds_warning_hours_json = null,
      execution_intent_json = null
    where pb2.id = v_req.pay_batch_id;
    v_worker_communications := jsonb_build_object(
      'automatic_commit_stage', false,
      'message_kind', 'NONE',
      'trigger_status', 'AUTH_REJECTED_NO_QUEUE',
      'dispatch_required', false,
      'error', null,
      'result', jsonb_build_object(
        'ok', true,
        'queued', false,
        'deferred', false,
        'dispatch_required', false,
        'trigger_status', 'AUTH_REJECTED_NO_QUEUE',
        'configured_timing', v_remittance_send_timing
      )
    );
    v_remittance_queue_stage_result := v_worker_communications->'result';

    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_AUTH_APPLY_ACTION_REJECTED',
      jsonb_build_object(
        'auth_request_id', p_auth_request_id::text,
        'pay_batch_id', v_req.pay_batch_id::text,
        'worker_communications', v_worker_communications
      ),
      'pay_batch_auth_requests',
      p_auth_request_id::text,
      null::jsonb,
      null::text,
      null::text,
      null::text
    );

    return jsonb_build_object(
      'ok', true,
      'pay_batch_id', v_req.pay_batch_id::text,
      'status', (select pb3.status from public.pay_batches pb3 where pb3.id = v_req.pay_batch_id),
      'auth_request_id', p_auth_request_id::text,
      'auth_state', 'REJECTED',
      'state', 'REJECTED',
      'operation_id', NULL,
      'schedule_revision', (
        select coalesce(pb_revision.source_scope_change_generation, 0)
        from public.pay_batches as pb_revision
        where pb_revision.id = v_req.pay_batch_id
      ),
      'active_scope_hash', v_active_scope_hash,
      'display_message', 'Payment authorisation rejected.',
      'required_quantity', v_req.required_quantity,
      'approved_count', 0,
      'became_authorised', false,
      'payment_remittance_send_timing', v_remittance_send_timing,
      'execution_intent_json', v_execution_intent_json,
      'execution_mode', v_execution_intent_json->>'execution_mode',
      'suppress_remittances', lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on'),
      'worker_communications', v_worker_communications,
      'remittance_queue_stage_result', v_remittance_queue_stage_result
    );
  end if;

  select count(*)::int
  into v_approved_count
  from public.pay_batch_auth_actions pbaa0
  where pbaa0.auth_request_id = p_auth_request_id
    and pbaa0.action in ('AUTHORISE','USE_GOLDEN_KEY');

  if v_action = 'USE_GOLDEN_KEY' or v_approved_count >= greatest(1, coalesce(v_req.required_quantity,1)) then
    v_became_authorised := true;

    v_schedule_kind := upper(btrim(coalesce(
      nullif(v_req.schedule_kind, ''),
      nullif(v_execution_intent_json->>'schedule_kind', ''),
      'IMMEDIATE'
    )));
    if v_schedule_kind not in ('IMMEDIATE','SCHEDULED') then
      raise exception 'pay_batch_auth_apply_action: invalid stored schedule_kind %', v_schedule_kind;
    end if;

    v_scheduled_at_utc := v_req.scheduled_at_utc;
    if v_scheduled_at_utc is null and nullif(btrim(coalesce(v_execution_intent_json->>'scheduled_at_utc', '')), '') is not null then
      v_scheduled_at_utc := (v_execution_intent_json->>'scheduled_at_utc')::timestamptz;
    end if;
    if v_schedule_kind = 'SCHEDULED' and v_scheduled_at_utc is null then
      raise exception 'pay_batch_auth_apply_action: stored scheduled_at_utc is required for SCHEDULED authorisation';
    end if;

    v_funding_account_ref := v_req.funding_account_ref;
    v_funds_warning_hours_json := v_req.funds_warning_hours_json;

    update public.pay_batch_auth_requests pbar3
    set
      state = 'AUTHORISED',
      finalised_at_utc = v_now,
      finalised_by_user_id = p_actor_user_id,
      golden_key_used = case when v_action = 'USE_GOLDEN_KEY' then true else coalesce(pbar3.golden_key_used,false) end,
      golden_key_user_id = case when v_action = 'USE_GOLDEN_KEY' then p_actor_user_id else pbar3.golden_key_user_id end,
      execution_intent_json = v_execution_intent_json
    where pbar3.id = p_auth_request_id;

    update public.pay_batch_auth_tokens pbat3
    set used_at_utc = v_now
    where pbat3.auth_request_id = p_auth_request_id
      and pbat3.used_at_utc is null;

    update public.pay_batches pb_before_schedule
    set
      execution_intent_json = v_execution_intent_json,
      execution_commit_state = coalesce(nullif(btrim(coalesce(pb_before_schedule.execution_commit_state, '')), ''), 'NOT_SUBMITTED')
    where pb_before_schedule.id = v_req.pay_batch_id;

    select started_operation.operation_id
    into v_payment_operation_id
    from public.banking_pay_operation_start(
      'PAYMENT_EXECUTE',
      p_actor_user_id,
      'payment-execute-auth:' || p_auth_request_id::text || ':' || v_active_scope_hash,
      null::uuid,
      v_req.pay_batch_id,
      null::uuid,
      jsonb_build_object(
        'auth_request_id', p_auth_request_id,
        'active_scope_hash', v_active_scope_hash,
        'freshness_result_hash', v_execution_intent_json->>'freshness_result_hash',
        'initial_phase', 'INITIALISE',
        'backend_runner_owned', true
      ),
      jsonb_strip_nulls(jsonb_build_object(
        'run_after_utc', case
          when v_schedule_kind = 'SCHEDULED' then v_scheduled_at_utc
          else v_now
        end
      ))
    ) as started_operation
    limit 1;

    v_execution_intent_json := v_execution_intent_json || jsonb_build_object(
      'operation_id', v_payment_operation_id,
      'active_scope_hash', v_active_scope_hash
    );

    update public.pay_batch_auth_requests as auth_with_operation
    set execution_intent_json = v_execution_intent_json
    where auth_with_operation.id = p_auth_request_id;

    v_schedule_result := public.pay_batch_schedule(
      v_req.pay_batch_id,
      v_schedule_kind,
      v_scheduled_at_utc,
      v_funding_account_ref,
      v_funds_warning_hours_json,
      p_actor_user_id,
      v_payment_operation_id,
      v_execution_intent_json->>'freshness_result_hash'
    );

    update public.pay_batches pb4
    set
      status = 'AUTHORISED_FOR_PAYMENT',
      execution_intent_json = v_execution_intent_json,
      authoritative_payment_date = coalesce(
        pb4.authoritative_payment_date,
        (pb4.scheduled_at_utc at time zone 'Europe/London')::date,
        pb4.pay_date
      ),
      authoritative_payment_date_source = coalesce(pb4.authoritative_payment_date_source, 'AUTH_APPLY_SCHEDULED_AT_UTC')
    where pb4.id = v_req.pay_batch_id;

    v_worker_communications := coalesce(v_schedule_result->'worker_communications', '{}'::jsonb);
    v_remittance_queue_stage_result := coalesce(
      v_schedule_result->'remittance_queue_stage_result',
      v_worker_communications->'remittance_queue_stage_result',
      v_worker_communications->'result',
      '{}'::jsonb
    );

    v_new_auth_state := 'AUTHORISED';
    v_new_batch_status := 'AUTHORISED_FOR_PAYMENT';
  else
    v_became_authorised := false;
    v_new_auth_state := 'AWAITING';

    update public.pay_batches pb5
    set
      status = 'AWAITING_AUTHORISATION',
      execution_intent_json = v_execution_intent_json
    where pb5.id = v_req.pay_batch_id;

    v_new_batch_status := 'AWAITING_AUTHORISATION';
    v_worker_communications := jsonb_build_object(
      'automatic_commit_stage', false,
      'message_kind', case when upper(btrim(coalesce(v_execution_intent_json->>'execution_mode', 'STANDARD_BANK'))) in ('CSV_SETTLEMENT','EXTERNAL_SETTLEMENT') then 'PAYOUT_NOTICE_AND_REMITTANCE' else 'REMITTANCE' end,
      'trigger_status', 'AWAITING_AUTHORISATION',
      'dispatch_required', false,
      'error', null,
      'result', jsonb_build_object(
        'ok', true,
        'queued', false,
        'deferred', true,
        'dispatch_required', false,
        'trigger_status', 'AWAITING_AUTHORISATION',
        'configured_timing', v_remittance_send_timing
      )
    );
    v_remittance_queue_stage_result := v_worker_communications->'result';
  end if;

  perform public._imp_debug_audit(
    p_actor_user_id,
    'PAY_BATCH_AUTH_APPLY_ACTION_RESULT',
    jsonb_build_object(
      'auth_request_id', p_auth_request_id::text,
      'pay_batch_id', v_req.pay_batch_id::text,
      'action', v_action,
      'approved_count', v_approved_count,
      'became_authorised', v_became_authorised,
      'auth_state', v_new_auth_state,
      'batch_status', v_new_batch_status,
      'schedule_result', v_schedule_result,
      'worker_communications', v_worker_communications
    ),
    'pay_batch_auth_requests',
    p_auth_request_id::text,
    null::jsonb,
    null::text,
    null::text,
    null::text
  );

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_req.pay_batch_id::text,
    'status', (select pb6.status from public.pay_batches pb6 where pb6.id = v_req.pay_batch_id),
    'auth_request_id', p_auth_request_id::text,
    'auth_state', v_new_auth_state,
    'state', v_new_auth_state,
    'required_quantity', v_req.required_quantity,
    'approved_count', v_approved_count,
    'became_authorised', v_became_authorised,
    'operation_id', v_payment_operation_id,
    'schedule_revision', coalesce(
      nullif(v_schedule_result->>'schedule_revision', '')::bigint,
      (
        select coalesce(pb_revision.source_scope_change_generation, 0)
        from public.pay_batches as pb_revision
        where pb_revision.id = v_req.pay_batch_id
      )
    ),
    'active_scope_hash', v_active_scope_hash,
    'display_message', case
      when v_became_authorised then 'Payment authorised for the current active scope.'
      else 'Authorisation recorded. Further approval is required.'
    end,
    'payment_remittance_send_timing', v_remittance_send_timing,
    'execution_intent_json', v_execution_intent_json,
    'execution_mode', v_execution_intent_json->>'execution_mode',
    'suppress_remittances', lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on'),
    'schedule_result', v_schedule_result,
    'worker_communications', v_worker_communications,
    'remittance_queue_stage_result', v_remittance_queue_stage_result
  );
exception
  when others then
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_BATCH_AUTH_APPLY_ACTION_ERROR',
      jsonb_build_object(
        'auth_request_id', p_auth_request_id::text,
        'action', v_action,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_batch_auth_requests',
      coalesce(p_auth_request_id::text, 'NO_AUTH_REQUEST_ID'),
      null::jsonb,
      null::text,
      null::text,
      null::text
    );
    raise;
end;
$$;
ALTER FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_auth_apply_action(uuid,uuid,text,text) TO service_role;
