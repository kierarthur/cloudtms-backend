-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: cde6442c97df8d2c3c05759435a941f3

CREATE OR REPLACE FUNCTION public.pay_batch_auth_apply_action(p_auth_request_id uuid, p_actor_user_id uuid, p_action text, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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

    v_schedule_result := public.pay_batch_schedule(
      v_req.pay_batch_id,
      v_schedule_kind,
      v_scheduled_at_utc,
      v_funding_account_ref,
      v_funds_warning_hours_json,
      p_actor_user_id
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
    'required_quantity', v_req.required_quantity,
    'approved_count', v_approved_count,
    'became_authorised', v_became_authorised,
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
$function$;

ALTER FUNCTION pay_batch_auth_apply_action(uuid,uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_batch_auth_apply_action(uuid,uuid,text,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_batch_auth_apply_action(uuid,uuid,text,text) TO postgres;
GRANT EXECUTE ON FUNCTION pay_batch_auth_apply_action(uuid,uuid,text,text) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_batch_auth_apply_action(uuid,uuid,text,text) TO service_role;
