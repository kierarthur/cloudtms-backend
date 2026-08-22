-- Immutable CloudTMS TEST function snapshot, page 00.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- _audit_insert(text,text,text,jsonb,jsonb,text,uuid)
CREATE OR REPLACE FUNCTION public._audit_insert(p_object_type text, p_object_id_text text, p_action text, p_before_json jsonb, p_after_json jsonb, p_reason text, p_actor_user_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_actor_display text := null;
  v_actor_role    text := null;
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name, u.email, '')), ''),
      nullif(btrim(coalesce(u.role, '')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;
  end if;

  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time
  )
  values (
    coalesce(nullif(btrim(p_object_type),''), 'generic'),
    nullif(btrim(p_object_id_text), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    nullif(btrim(p_reason), ''),
    p_actor_user_id,
    nullif(btrim(v_actor_display), ''),
    nullif(btrim(v_actor_role), '')
  );
end;
$function$;

-- _bank_hash(text,text,text)
CREATE OR REPLACE FUNCTION public._bank_hash(p_sort_code text, p_account_number text, p_account_holder text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
  v_sort text;
  v_acct text;
  v_name text;
  v_raw  text;
begin
  v_sort := regexp_replace(coalesce(p_sort_code,''), '[^0-9]+', '', 'g');
  v_acct := regexp_replace(coalesce(p_account_number,''), '[^0-9]+', '', 'g');
  v_name := upper(regexp_replace(btrim(coalesce(p_account_holder,'')), '\s+', ' ', 'g'));

  if v_sort = '' or v_acct = '' then
    return null;
  end if;

  v_raw := v_sort || '|' || v_acct || '|' || v_name;

  -- md5() is built-in (no pgcrypto dependency) and returns a stable hex string.
  return md5(v_raw);
end $function$;

-- _banking_alert_user_filter_allows(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._banking_alert_user_filter_allows(p_user_id uuid, p_alert_payload jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_payload jsonb := '{}'::jsonb;
  v_preferences jsonb := '{}'::jsonb;
  v_enabled boolean := true;
  v_alert_kind text := NULL::text;
  v_failure_reason_group text := NULL::text;
  v_provider_key text := NULL::text;
  v_pay_batch_id_text text := NULL::text;
  v_severity text := NULL::text;
  v_severity_min text := 'ACTION_REQUIRED';
  v_snoozed_until_utc timestamptz := NULL::timestamptz;
  v_success_only boolean := false;
  v_is_progress boolean := false;
  v_is_informational boolean := false;
  v_is_action_required boolean := false;
  v_allowlist_contains boolean := false;
  v_blocklist_contains boolean := false;
  v_reason_allowlist_contains boolean := false;
  v_reason_blocklist_contains boolean := false;
  v_provider_muted boolean := false;
  v_batch_muted boolean := false;
  v_payload_lifecycle text := NULL::text;
  v_required_user_action text := NULL::text;
BEGIN
  IF p_user_id IS NULL THEN
    RETURN false;
  END IF;

  IF p_alert_payload IS NULL OR COALESCE(jsonb_typeof(p_alert_payload), 'null') <> 'object' THEN
    RETURN false;
  END IF;

  v_payload := p_alert_payload;
  v_preferences := public.banking_alert_preferences_get(p_user_id);

  v_enabled := COALESCE((v_preferences ->> 'enabled')::boolean, true);
  IF v_enabled IS NOT TRUE THEN
    RETURN false;
  END IF;

  v_alert_kind := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'alert_kind',
    v_payload ->> 'kind',
    v_payload ->> 'alert_type',
    ''
  )), ''));

  v_failure_reason_group := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'provider_failure_reason_group',
    v_payload ->> 'failure_reason_group',
    v_payload ->> 'reason_group',
    ''
  )), ''));

  v_provider_key := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'provider_key',
    v_payload ->> 'provider',
    v_payload ->> 'rail_provider',
    v_payload ->> 'rail',
    ''
  )), ''));

  v_pay_batch_id_text := LOWER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'pay_batch_id',
    v_payload ->> 'entity_id',
    ''
  )), ''));

  v_required_user_action := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'required_user_action',
    v_payload ->> 'recommended_action',
    ''
  )), ''));

  v_payload_lifecycle := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'payment_lifecycle_state',
    v_payload ->> 'current_status',
    v_payload ->> 'status',
    ''
  )), ''));

  v_severity := UPPER(NULLIF(BTRIM(COALESCE(
    v_payload ->> 'alert_severity',
    v_payload ->> 'severity',
    ''
  )), ''));

  IF v_alert_kind IS NULL AND v_required_user_action IS NULL AND v_failure_reason_group IS NULL THEN
    RETURN false;
  END IF;

  IF v_alert_kind IN (
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS',
    'PAYMENT_COMPLETED',
    'PAYMENT_SUCCESS',
    'PAYMENT_SUCCEEDED',
    'PROVIDER_PAYMENT_COMPLETED',
    'PROVIDER_PAYMENT_SUCCESS',
    'SETTLEMENT_COMPLETED',
    'BANK_PAYMENT_SUCCESS',
    'SUCCESS',
    'COMPLETED'
  ) THEN
    v_success_only := true;
  END IF;

  IF v_severity IS NULL THEN
    IF v_alert_kind IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS') THEN
      v_severity := 'PROGRESS';
    ELSIF v_alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN
      v_severity := 'INFO';
    ELSE
      v_severity := 'ACTION_REQUIRED';
    END IF;
  END IF;

  v_severity_min := UPPER(NULLIF(BTRIM(COALESCE(v_preferences ->> 'severity_min', 'ACTION_REQUIRED')), ''));
  IF v_severity_min IS NULL OR v_severity_min NOT IN ('INFO','PROGRESS','ACTION_REQUIRED','CRITICAL') THEN
    v_severity_min := 'ACTION_REQUIRED';
  END IF;

  v_success_only := v_success_only OR lower(BTRIM(COALESCE(
    v_payload ->> 'alert_candidate_is_success_only',
    v_payload ->> 'is_success_only',
    v_payload ->> 'success_only',
    'false'
  ))) IN ('true','t','1','yes','y','on');

  IF v_success_only IS NOT TRUE
     AND v_alert_kind IS NULL
     AND v_required_user_action IS NULL
     AND v_payload_lifecycle IN ('PAID', 'PAID_OR_SETTLED', 'SETTLED', 'COMPLETED', 'SUCCESS', 'SUCCEEDED') THEN
    v_success_only := true;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_preferences ->> 'snoozed_until_utc', '')), '') IS NOT NULL THEN
    BEGIN
      v_snoozed_until_utc := (v_preferences ->> 'snoozed_until_utc')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
      v_snoozed_until_utc := NULL::timestamptz;
    END;
  END IF;

  IF v_snoozed_until_utc IS NOT NULL AND v_snoozed_until_utc > now() THEN
    RETURN false;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'alert_kind_blocklist'), 'null') = 'array' AND v_alert_kind IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'alert_kind_blocklist') AS blocked_alert_kind(value)
      WHERE UPPER(BTRIM(blocked_alert_kind.value)) = v_alert_kind
    )
    INTO v_blocklist_contains;

    IF v_blocklist_contains THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'alert_kind_allowlist'), 'null') = 'array' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'alert_kind_allowlist') AS allowed_alert_kind(value)
      WHERE UPPER(BTRIM(allowed_alert_kind.value)) = v_alert_kind
    )
    INTO v_allowlist_contains;

    IF v_allowlist_contains IS NOT TRUE THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only IS NOT TRUE
     AND COALESCE(jsonb_typeof(v_preferences -> 'failure_reason_blocklist'), 'null') = 'array'
     AND v_failure_reason_group IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'failure_reason_blocklist') AS blocked_reason(value)
      WHERE UPPER(BTRIM(blocked_reason.value)) = v_failure_reason_group
    )
    INTO v_reason_blocklist_contains;

    IF v_reason_blocklist_contains THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only IS NOT TRUE
     AND COALESCE(jsonb_typeof(v_preferences -> 'failure_reason_allowlist'), 'null') = 'array' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'failure_reason_allowlist') AS allowed_reason(value)
      WHERE UPPER(BTRIM(allowed_reason.value)) = v_failure_reason_group
    )
    INTO v_reason_allowlist_contains;

    IF v_reason_allowlist_contains IS NOT TRUE THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'muted_provider_keys'), 'null') = 'array' AND v_provider_key IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'muted_provider_keys') AS muted_provider(value)
      WHERE UPPER(BTRIM(muted_provider.value)) = v_provider_key
    )
    INTO v_provider_muted;

    IF v_provider_muted THEN
      RETURN false;
    END IF;
  END IF;

  IF COALESCE(jsonb_typeof(v_preferences -> 'muted_pay_batch_ids'), 'null') = 'array' AND v_pay_batch_id_text IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_preferences -> 'muted_pay_batch_ids') AS muted_batch(value)
      WHERE LOWER(BTRIM(muted_batch.value)) = v_pay_batch_id_text
    )
    INTO v_batch_muted;

  IF v_batch_muted THEN
      RETURN false;
    END IF;
  END IF;

  IF v_success_only THEN
    RETURN COALESCE((v_preferences ->> 'include_success_alerts')::boolean, true);
  END IF;

  v_is_progress := v_alert_kind IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS') OR v_severity = 'PROGRESS';
  v_is_informational := v_alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' OR v_severity = 'INFO';
  v_is_action_required := v_severity IN ('ACTION_REQUIRED', 'CRITICAL') OR v_required_user_action IS NOT NULL;

  IF v_is_progress AND COALESCE((v_preferences ->> 'include_progress_alerts')::boolean, true) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_informational AND COALESCE((v_preferences ->> 'include_informational_alerts')::boolean, false) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_action_required AND COALESCE((v_preferences ->> 'include_action_required')::boolean, true) IS NOT TRUE THEN
    RETURN false;
  END IF;

  IF v_is_progress OR v_is_informational THEN
    RETURN true;
  END IF;

  IF v_severity_min = 'CRITICAL' AND v_severity <> 'CRITICAL' THEN
    RETURN false;
  ELSIF v_severity_min = 'ACTION_REQUIRED' AND v_severity NOT IN ('ACTION_REQUIRED', 'CRITICAL') THEN
    RETURN false;
  ELSIF v_severity_min = 'PROGRESS' AND v_severity NOT IN ('PROGRESS', 'ACTION_REQUIRED', 'CRITICAL') THEN
    RETURN false;
  END IF;

  RETURN true;
END;
$function$;

-- _banking_pay_operation_touch_updated_at()
CREATE OR REPLACE FUNCTION public._banking_pay_operation_touch_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
    new.updated_at_utc := now();
    return new;
end;
$function$;

-- _banking_provider_failure_reason_normalise(text,text,text,text,jsonb)
CREATE OR REPLACE FUNCTION public._banking_provider_failure_reason_normalise(p_provider_key text, p_provider_state text, p_provider_reason_code text DEFAULT NULL::text, p_provider_reason_text text DEFAULT NULL::text, p_event_payload jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_provider_key text := UPPER(NULLIF(BTRIM(COALESCE(p_provider_key, '')), ''));
  v_provider_state text := LOWER(NULLIF(BTRIM(COALESCE(p_provider_state, '')), ''));
  v_event_payload jsonb := '{}'::jsonb;
  v_reason_code text := NULL::text;
  v_reason_text text := NULL::text;
  v_reason_probe text := NULL::text;
  v_failure_reason_group text := NULL::text;
  v_failure_reason_label text := NULL::text;
  v_is_user_filterable boolean := false;
  v_is_success_state boolean := false;
  v_is_pending_state boolean := false;
  v_is_terminal_failure_state boolean := false;
  v_support_details jsonb := '{}'::jsonb;
BEGIN
  IF p_event_payload IS NOT NULL AND COALESCE(jsonb_typeof(p_event_payload), 'null') = 'object' THEN
    v_event_payload := p_event_payload;
  END IF;

  v_reason_code := COALESCE(
    NULLIF(BTRIM(p_provider_reason_code), ''),
    NULLIF(BTRIM(v_event_payload ->> 'provider_failure_reason_code'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'failure_reason_code'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'reason_code'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'reasonCode'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'error_code'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'errorCode'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,provider_failure_reason_code}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,failure_reason_code}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,reason_code}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,reasonCode}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,error_code}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,errorCode}'), '')
  );

  v_reason_text := COALESCE(
    NULLIF(BTRIM(p_provider_reason_text), ''),
    NULLIF(BTRIM(v_event_payload ->> 'provider_failure_reason_text'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'failure_reason'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'failureReason'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'reason'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'message'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'error_message'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'errorMessage'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'decline_reason'), ''),
    NULLIF(BTRIM(v_event_payload ->> 'declineReason'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,provider_failure_reason_text}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,failure_reason}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,failureReason}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,reason}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,message}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,error_message}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,errorMessage}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,decline_reason}'), ''),
    NULLIF(BTRIM(v_event_payload #>> '{data,declineReason}'), '')
  );

  v_reason_probe := LOWER(CONCAT_WS(' ', v_provider_key, v_provider_state, v_reason_code, v_reason_text));

  v_is_success_state := v_provider_state IN (
    'paid',
    'settled',
    'completed',
    'complete',
    'success',
    'succeeded'
  );

  v_is_pending_state := v_provider_state IN (
    'created',
    'pending',
    'accepted',
    'scheduled',
    'submitted',
    'sent',
    'processing',
    'in_flight',
    'queued',
    'pending_settlement',
    'pending_confirmation'
  );

  v_is_terminal_failure_state := v_provider_state IN (
    'failed',
    'failure',
    'declined',
    'rejected',
    'cancelled',
    'canceled',
    'submission_failed',
    'failed_before_commit'
  );

  IF v_is_success_state THEN
    v_failure_reason_group := NULL::text;
    v_failure_reason_label := NULL::text;
    v_is_user_filterable := false;
  ELSIF v_reason_probe ~ '(insufficient|insufficient_funds|not enough|available balance|balance too low|low balance|lack of funds|funds unavailable|insufficient balance)' THEN
    v_failure_reason_group := 'INSUFFICIENT_FUNDS';
    v_failure_reason_label := 'Insufficient funds';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(unknown recipient|recipient unknown|unknown beneficiary|beneficiary unknown|payee unknown|recipient not found|beneficiary not found|payee not found|name check|confirmation of payee)' THEN
    v_failure_reason_group := 'UNKNOWN_RECIPIENT';
    v_failure_reason_label := 'Unknown recipient';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(invalid account|invalid iban|invalid sort|invalid routing|invalid beneficiary|invalid recipient|account number invalid|iban invalid|sort code invalid|routing number invalid|bad account)' THEN
    v_failure_reason_group := 'INVALID_ACCOUNT';
    v_failure_reason_label := 'Invalid account';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(account closed|closed account|beneficiary account closed|recipient account closed)' THEN
    v_failure_reason_group := 'ACCOUNT_CLOSED';
    v_failure_reason_label := 'Account closed';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(outage|unavailable|service unavailable|temporarily unavailable|timeout|timed out|gateway timeout|connection refused|connection reset|network error|provider down|bank unavailable|503|504)' THEN
    v_failure_reason_group := 'PROVIDER_OUTAGE';
    v_failure_reason_label := 'Provider outage';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(webhook unmatched|webhook_unmatched|unmatched webhook|event unmatched|unmatched event)' THEN
    v_failure_reason_group := 'WEBHOOK_UNMATCHED';
    v_failure_reason_label := 'Webhook unmatched';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(unknown|ambiguous|unmatched|cannot match|not matched|unable to match|outcome unknown|state unknown)' THEN
    v_failure_reason_group := 'PROVIDER_UNKNOWN';
    v_failure_reason_label := 'Provider outcome unknown';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(compliance|sanction|aml|kyc|screening|review|fraud|risk review|manual review|restricted)' THEN
    v_failure_reason_group := 'COMPLIANCE_REVIEW';
    v_failure_reason_label := 'Compliance review';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(duplicate|already submitted|already exists|idempotency|duplicate transaction|duplicate payment)' THEN
    v_failure_reason_group := 'DUPLICATE_RISK';
    v_failure_reason_label := 'Duplicate risk';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(paid recovery|recovery required|overpayment|paid_settled|paid or settled|settled recovery)' THEN
    v_failure_reason_group := 'PAID_RECOVERY_REQUIRED';
    v_failure_reason_label := 'Paid recovery required';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(manual adjustment|carry forward|carry-forward|manual_adjustment|manual adjustment blocker)' THEN
    v_failure_reason_group := 'MANUAL_ADJUSTMENT_BLOCKER';
    v_failure_reason_label := 'Manual adjustment blocker';
    v_is_user_filterable := true;
  ELSIF v_reason_probe ~ '(bank rejected|bank_rejected|rejected by bank|declined by bank|refused by bank|beneficiary bank rejected|receiving bank rejected)' THEN
    v_failure_reason_group := 'BANK_REJECTED';
    v_failure_reason_label := 'Bank rejected';
    v_is_user_filterable := true;
  ELSIF v_is_pending_state THEN
    v_failure_reason_group := NULL::text;
    v_failure_reason_label := NULL::text;
    v_is_user_filterable := false;
  ELSIF v_is_terminal_failure_state THEN
    v_failure_reason_group := 'PROVIDER_FAILED_UNSPECIFIED';
    v_failure_reason_label := 'Provider failed';
    v_is_user_filterable := true;
  ELSE
    v_failure_reason_group := NULL::text;
    v_failure_reason_label := NULL::text;
    v_is_user_filterable := false;
  END IF;

  v_support_details := jsonb_build_object(
    'provider_key', v_provider_key,
    'provider_state', v_provider_state,
    'provider_reason_code', v_reason_code,
    'provider_reason_text', v_reason_text,
    'is_success_state', v_is_success_state,
    'is_pending_state', v_is_pending_state,
    'is_terminal_failure_state', v_is_terminal_failure_state
  );

  RETURN jsonb_build_object(
    'failure_reason_group', v_failure_reason_group,
    'failure_reason_code', v_reason_code,
    'failure_reason_label', v_failure_reason_label,
    'is_user_filterable', v_is_user_filterable,
    'support_details_json', v_support_details
  );
END;
$function$;

-- _change_bump(text)
CREATE OR REPLACE FUNCTION public._change_bump(p_entity_key text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  if p_entity_key is null or btrim(p_entity_key) = '' then
    return;
  end if;

  update public.app_change_counters c
     set seq = c.seq + 1,
         updated_at = now()
   where c.entity_key = p_entity_key;

  if not found then
    insert into public.app_change_counters(entity_key, seq, updated_at)
    values (p_entity_key, 1, now())
    on conflict (entity_key) do update
      set seq = public.app_change_counters.seq + 1,
          updated_at = now();
  end if;
end;
$function$;

-- _cloudtms_touch_updated_at_utc()
CREATE OR REPLACE FUNCTION public._cloudtms_touch_updated_at_utc()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  NEW.updated_at_utc := now();
  RETURN NEW;
END;
$function$;

-- _ctms_assert_correction_invoice_scope_v1(uuid[],uuid,uuid,boolean,boolean,boolean,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_correction_invoice_scope_v1(p_timesheet_ids uuid[], p_target_invoice_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_require_complete_selection boolean DEFAULT false, p_require_appendable boolean DEFAULT false, p_lock_rows boolean DEFAULT false, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_input uuid[];
  v_id uuid;
  v_scope jsonb;
  v_members uuid[];
  v_scopes jsonb := '[]'::jsonb;
  v_already boolean;
begin
  select coalesce(array_agg(distinct id order by id), array[]::uuid[]) into v_input
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) x(id) where id is not null;
  foreach v_id in array v_input loop
    if coalesce((public._ctms_import_correction_classify_v1(v_id)
      ->> 'is_import_authoritative_correction')::boolean, false) is not true then continue; end if;
    v_scope := public.invoice_correction_pair_scope_v1(
      v_id, p_target_invoice_id, p_actor_user_id, p_lock_rows, 100
    );
    if coalesce((v_scope->>'valid')::boolean,false) is not true then
      raise exception 'INVOICE_CORRECTION_SCOPE_INVALID' using errcode='P0001',detail=v_scope::text;
    end if;
    select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_members
    from jsonb_array_elements_text(v_scope->'pair_timesheet_ids') value;
    v_already := coalesce((v_scope->>'existing_line_member_count')::integer,0)
      = coalesce((v_scope->>'expected_member_count')::integer,0)
      and coalesce((v_scope->>'existing_line_invoice_count')::integer,0)=1;
    if p_require_complete_selection and not v_already and not (v_members <@ v_input) then
      raise exception 'INVOICE_CORRECTION_UNIT_MUST_BE_SELECTED_TOGETHER'
        using errcode='P0001',detail=jsonb_build_object('context',p_context,'selected',to_jsonb(v_input),'required',to_jsonb(v_members))::text;
    end if;
    if p_require_appendable and not v_already
       and coalesce((v_scope->>'target_appendable')::boolean,false) is not true then
      raise exception 'INVOICE_CORRECTION_UNIT_NOT_APPENDABLE' using errcode='P0001',detail=v_scope::text;
    end if;
    v_scopes := v_scopes || jsonb_build_array(v_scope);
  end loop;
  return jsonb_build_object('ok',true,'context',p_context,'scopes',v_scopes);
end;
$function$;

-- _ctms_assert_import_correction_settings_write_v1(boolean,boolean,boolean,correction_financials_date_basis_enum,correction_financials_date_basis_enum)
CREATE OR REPLACE FUNCTION public._ctms_assert_import_correction_settings_write_v1(p_is_nhsp boolean, p_autoprocess_hr boolean, p_no_timesheet_required boolean, p_reversal_complete_financials_date correction_financials_date_basis_enum, p_reversal_replacement_financials_date correction_financials_date_basis_enum)
 RETURNS void
 LANGUAGE plpgsql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  if p_reversal_complete_financials_date is null
     and p_reversal_replacement_financials_date is null then
    return;
  end if;

  if not (
    coalesce(p_is_nhsp, false)
    or (
      -- Preserve the installed parameter name for named-call compatibility.
      -- Review callers pass the current requires_hr eligibility value here.
      coalesce(p_autoprocess_hr, false)
      and coalesce(p_no_timesheet_required, false)
    )
  ) then
    raise exception 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT'
      using errcode = '22023',
            detail = jsonb_build_object(
              'code', 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT',
              'eligibility', 'is_nhsp OR (requires_hr AND no_timesheet_required)',
              'retained_values_rule', 'stored values may remain but are ignored while ineligible'
            )::text;
  end if;
end;
$function$;

-- _ctms_assert_invoice_can_unissue_v1(uuid,boolean,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_invoice_can_unissue_v1(p_invoice_id uuid, p_lock_row boolean DEFAULT true, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_invoice public.invoices%rowtype; v_credit_count integer;
begin
  if not exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;
  if p_lock_row then select * into v_invoice from public.invoices where id=p_invoice_id for update;
  else select * into v_invoice from public.invoices where id=p_invoice_id; end if;
  if not found then raise exception 'INVOICE_NOT_FOUND' using errcode='P0001'; end if;
  select count(*)::integer into v_credit_count from public.invoices i
  where i.original_invoice_id=p_invoice_id and upper(coalesce(i.type::text,''))='CREDIT_NOTE';
  if v_invoice.paid_at_utc is not null or upper(coalesce(v_invoice.status::text,''))='PAID'
     or v_invoice.credit_note_created_at_utc is not null or v_credit_count>0 then
    raise exception 'INVOICE_UNISSUE_BLOCKED_BY_DOWNSTREAM_AUTHORITY'
      using errcode='P0001',detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id)::text;
  end if;
end;
$function$;

-- _ctms_assert_invoice_correction_lines_v1(uuid,uuid,boolean,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_invoice_correction_lines_v1(p_invoice_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_lock_rows boolean DEFAULT false, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_id uuid; v_scope jsonb; v_scopes jsonb:='[]'::jsonb;
begin
  for v_id in
    select distinct il.timesheet_id from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  loop
    v_scope := public.invoice_correction_pair_scope_v1(v_id,p_invoice_id,p_actor_user_id,p_lock_rows,100);
    if coalesce((v_scope->>'valid')::boolean,false) is not true
       or (case when upper(coalesce(p_context,''))='INVOICE_APPLY_EDITS_RESULT'
          then coalesce(v_scope->>'placement_state','MALFORMED_PAIR') not in (
            'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE','UNPLACED')
          else coalesce(v_scope->>'placement_state','MALFORMED_PAIR') not in (
            'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES') end) then
      raise exception 'INVOICE_CORRECTION_LINES_NOT_UNIT_SAFE' using errcode='P0001',detail=v_scope::text;
    end if;
    v_scopes:=v_scopes||jsonb_build_array(v_scope);
  end loop;
  return jsonb_build_object('ok',true,'invoice_id',p_invoice_id,'scopes',v_scopes);
end;
$function$;

-- _ctms_assert_invoice_mutable_draft_v1(uuid,text,boolean)
CREATE OR REPLACE FUNCTION public._ctms_assert_invoice_mutable_draft_v1(p_invoice_id uuid, p_context text DEFAULT 'IMPORT_CORRECTION'::text, p_lock_row boolean DEFAULT true)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_invoice public.invoices%rowtype;
begin
  if not exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;
  if p_lock_row then
    select * into v_invoice from public.invoices where id=p_invoice_id for update;
  else
    select * into v_invoice from public.invoices where id=p_invoice_id;
  end if;
  if not found then raise exception 'INVOICE_NOT_FOUND' using errcode='P0001'; end if;
  if upper(coalesce(v_invoice.status::text,'')) <> 'DRAFT' or v_invoice.issued_at_utc is not null then
    raise exception 'POLICY_X_FROZEN_INVOICE_NOT_EDITABLE'
      using errcode='P0001', detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id,'status',v_invoice.status)::text;
  end if;
end;
$function$;

-- _ctms_assert_pay_batch_mutable_v1(uuid,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_pay_batch_mutable_v1(p_pay_batch_id uuid, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_batch public.pay_batches%rowtype;
  v_transfer_count integer;
  v_export_item_count integer;
  v_reasons text[] := array[]::text[];
  v_status text;
begin
  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_ID_REQUIRED' using errcode = '22023';
  end if;

  -- Preserve the installed behaviour for every ordinary batch. This stricter
  -- guard exists only when the batch actually contains an import-authoritative
  -- correction timesheet.
  if not exists (
    select 1
    from public.pay_batch_candidates pbc
    join public.timesheets_financials tf
      on tf.candidate_id=pbc.candidate_id
    where pbc.pay_batch_id=p_pay_batch_id
      and coalesce((public._ctms_import_correction_classify_v1(tf.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;

  select * into v_batch
  from public.pay_batches
  where id = p_pay_batch_id
  for update;
  if not found then
    raise exception 'PAY_BATCH_NOT_FOUND' using errcode = 'P0001';
  end if;

  v_status := upper(btrim(coalesce(v_batch.status::text, '')));
  if v_status in (
    'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'AUTHORISED',
    'AUTHORIZED', 'APPROVED', 'SUBMITTED', 'EXPORTED', 'EXECUTING',
    'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
  ) then
    v_reasons := array_append(v_reasons, 'status:' || v_status);
  end if;
  if v_batch.monzo_confirmed_at_utc is not null then v_reasons := array_append(v_reasons, 'monzo_confirmed'); end if;
  if v_batch.executing_started_at_utc is not null then v_reasons := array_append(v_reasons, 'execution_started'); end if;
  if v_batch.completed_at_utc is not null then v_reasons := array_append(v_reasons, 'completed'); end if;
  if v_batch.cancelled_at_utc is not null then v_reasons := array_append(v_reasons, 'cancelled'); end if;
  if coalesce(upper(btrim(v_batch.execution_commit_state::text)), 'NOT_SUBMITTED') <> 'NOT_SUBMITTED'
     or v_batch.execution_commit_ref is not null
     or v_batch.execution_committed_at_utc is not null
     or v_batch.bank_csv_export_json is not null
     or v_batch.execution_intent_json is not null
     or v_batch.settlement_confirmation_json is not null then
    v_reasons := array_append(v_reasons, 'frozen_or_external_evidence');
  end if;

  select count(*)::integer into v_transfer_count
  from public.pay_bank_transfers t where t.pay_batch_id = p_pay_batch_id;
  if v_transfer_count > 0 then v_reasons := array_append(v_reasons, 'bank_transfers'); end if;

  select count(*)::integer into v_export_item_count
  from public.pay_batch_items i
  where i.pay_bank_transfer_id is not null
    and i.pay_batch_candidate_id in (
      select c.id from public.pay_batch_candidates c where c.pay_batch_id = p_pay_batch_id
    );
  if v_export_item_count > 0 then v_reasons := array_append(v_reasons, 'exported_items'); end if;

  if cardinality(v_reasons) > 0 then
    raise exception 'PAY_BATCH_MUTATION_LOCKED_BY_POLICY_X'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'context', p_context, 'pay_batch_id', p_pay_batch_id,
              'status', v_batch.status, 'reasons', to_jsonb(v_reasons)
            )::text;
  end if;
end;
$function$;

-- _ctms_assert_payload_corrections_fresh_v1(jsonb,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_payload_corrections_fresh_v1(p_payload jsonb, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_id uuid;
  v_chain jsonb;
  v_checked jsonb := '[]'::jsonb;
begin
  foreach v_id in array public._ctms_payload_timesheet_ids_v1(p_payload, 100) loop
    if coalesce((public._ctms_import_correction_classify_v1(v_id)
      ->> 'is_import_authoritative_correction')::boolean, false) then
      v_chain := public.timesheet_correction_chain_scope_v1(v_id, false, 32, 100);
      if coalesce((v_chain ->> 'valid')::boolean, false) is not true then
        raise exception 'CORRECTION_CHAIN_STALE_OR_INVALID'
          using errcode = '40001', detail = jsonb_build_object(
            'context', p_context, 'timesheet_id', v_id, 'chain', v_chain
          )::text;
      end if;
      v_checked := v_checked || jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_id, 'chain_fingerprint', v_chain ->> 'chain_fingerprint'
      ));
    end if;
  end loop;
  return jsonb_build_object('ok', true, 'context', p_context, 'checked', v_checked);
end;
$function$;

-- _ctms_assert_session_correction_residuals_draftable_v1(uuid,jsonb,text)
CREATE OR REPLACE FUNCTION public._ctms_assert_session_correction_residuals_draftable_v1(p_session_id uuid, p_selected_preview_row_ids jsonb DEFAULT NULL::jsonb, p_context text DEFAULT 'BANKING_PAY_DRAFT'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_candidate uuid;
  v_residuals jsonb;
  v_bad jsonb;
  v_carry_bad jsonb;
  v_all jsonb := '[]'::jsonb;
begin
  if p_session_id is null then raise exception 'WORKBENCH_SESSION_ID_REQUIRED' using errcode='22023'; end if;
  for v_candidate in
    with selected_ids as (
      select value::uuid as id
      from jsonb_array_elements_text(coalesce(p_selected_preview_row_ids, '[]'::jsonb)) x(value)
      where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
    select distinct pr.candidate_id
    from public.banking_pay_workbench_preview_rows pr
    where pr.session_id = p_session_id
      and (p_selected_preview_row_ids is null
        or jsonb_array_length(coalesce(p_selected_preview_row_ids, '[]'::jsonb)) = 0
        or pr.id in (select id from selected_ids))
      and coalesce((public._ctms_import_correction_classify_v1(pr.timesheet_id)
        ->> 'is_import_authoritative_correction')::boolean, false)
  loop
    select jsonb_agg(jsonb_build_object(
      'registration_id',carry_row.id,
      'status',carry_row.status,
      'state_reason_code',carry_row.state_reason_code,
      'canonical_resolution_key',carry_row.canonical_resolution_key
    ) order by carry_row.source_priority,carry_row.created_at_utc)
    into v_carry_bad
    from public.banking_pay_workbench_case_resolution_carry_registrations carry_row
    where carry_row.target_session_id=p_session_id
      and carry_row.candidate_id=v_candidate
      and carry_row.status in ('PENDING','STALE','INCOMPATIBLE');

    if jsonb_array_length(coalesce(v_carry_bad,'[]'::jsonb))>0 then
      raise exception 'CORRECTION_RESOLUTION_CARRY_NOT_DRAFTABLE'
        using errcode='P0001',
              detail=jsonb_build_object(
                'candidate_id',v_candidate,
                'carry_registrations',v_carry_bad
              )::text;
    end if;

    v_residuals := public._ctms_candidate_correction_residuals_v1(
      p_session_id, v_candidate, null, p_context
    );
    select jsonb_agg(x) into v_bad
    from jsonb_array_elements(v_residuals) x
    where coalesce((x ->> 'draftable')::boolean, false) is not true;
    if jsonb_array_length(coalesce(v_bad, '[]'::jsonb)) > 0 then
      raise exception 'CORRECTION_RESIDUAL_NOT_DRAFTABLE'
        using errcode = 'P0001', detail = v_bad::text;
    end if;
    v_all := v_all || v_residuals;
  end loop;
  return jsonb_build_object('ok', true, 'correction_residuals', v_all);
end;
$function$;

-- _ctms_assert_tsfin_batch_units_v1(jsonb)
CREATE OR REPLACE FUNCTION public._ctms_assert_tsfin_batch_units_v1(p_rows jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_ids uuid[];
  v_id uuid;
  v_chain jsonb;
  v_unit jsonb;
  v_members uuid[];
  v_row jsonb;
begin
  if jsonb_typeof(coalesce(p_rows, '[]'::jsonb)) <> 'array'
     or jsonb_array_length(coalesce(p_rows, '[]'::jsonb)) > 100 then
    raise exception 'TSFIN_BATCH_ROWS_INVALID' using errcode='22023';
  end if;
  v_ids := public._ctms_payload_timesheet_ids_v1(p_rows, 100);
  for v_row in select value from jsonb_array_elements(coalesce(p_rows,'[]'::jsonb)) loop
    v_id := nullif(btrim(coalesce(
      v_row->>'timesheet_id', v_row->>'p_timesheet_id',
      v_row#>>'{snapshot_json,timesheet_id}', v_row#>>'{snapshot,timesheet_id}',
      v_row#>>'{row,timesheet_id}', ''
    )), '')::uuid;
    if v_id is null then continue; end if;
    perform public._ctms_assert_tsfin_snapshot_policy_v1(v_id, v_row);
    if coalesce((public._ctms_import_correction_classify_v1(v_id)
      ->>'is_import_authoritative_correction')::boolean,false) then
      v_chain:=public.timesheet_correction_chain_scope_v1(v_id,false,32,100);
      v_unit:=v_chain->'requested_correction_unit';
      select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_members
      from jsonb_array_elements_text(v_unit->'member_ids') value;
      if not (v_members <@ v_ids) then
        raise exception 'TSFIN_CORRECTION_UNIT_INCOMPLETE'
          using errcode='P0001',detail=jsonb_build_object('timesheet_id',v_id,'required',to_jsonb(v_members),'supplied',to_jsonb(v_ids))::text;
      end if;
    end if;
  end loop;
end;
$function$;

-- _ctms_assert_tsfin_snapshot_policy_v1(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._ctms_assert_tsfin_snapshot_policy_v1(p_timesheet_id uuid, p_payload jsonb)
 RETURNS void
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_class jsonb;
  v_leg jsonb;
  v_tsfin_policy jsonb;
  v_invoice_policy jsonb;
  v_envelope_fingerprint text;
  v_leg_fingerprint text;
  v_tsfin_policy_fingerprint text;
  v_invoice_policy_fingerprint text;
  v_supplied_tsfin_policy jsonb;
  v_supplied_invoice_policy jsonb;
  v_erni text;
  v_apply_erni text;
  v_pay_vat text;
begin
  v_class := public._ctms_import_correction_classify_v1(p_timesheet_id);
  if coalesce((v_class ->> 'is_import_authoritative_correction')::boolean, false) is not true then
    return;
  end if;
  v_leg := public._ctms_correction_policy_leg_read_v1(p_timesheet_id);
  v_tsfin_policy := v_leg -> 'tsfin_policy';
  v_invoice_policy := v_leg -> 'invoice_policy';

  v_envelope_fingerprint := nullif(btrim(coalesce(
    p_payload #>> '{policy_snapshot_json,correction_financials_policy_envelope_fingerprint}',
    p_payload #>> '{policy_snapshot_json,correction_financials_policy_envelope,envelope_fingerprint}',
    p_payload #>> '{rate_source_refs_json,correction_financials_policy_envelope_fingerprint}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,correction_financials_policy_envelope_fingerprint}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,correction_financials_policy_envelope,envelope_fingerprint}',
    p_payload #>> '{snapshot,policy_snapshot_json,correction_financials_policy_envelope_fingerprint}',
    p_payload ->> 'correction_financials_policy_envelope_fingerprint', ''
  )), '');
  v_leg_fingerprint := nullif(btrim(coalesce(
    p_payload #>> '{policy_snapshot_json,correction_leg_fingerprint}',
    p_payload #>> '{rate_source_refs_json,correction_leg_fingerprint}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,correction_leg_fingerprint}',
    p_payload #>> '{snapshot_json,rate_source_refs_json,correction_leg_fingerprint}',
    p_payload #>> '{snapshot,policy_snapshot_json,correction_leg_fingerprint}',
    p_payload ->> 'correction_leg_fingerprint', ''
  )), '');
  v_tsfin_policy_fingerprint := nullif(btrim(coalesce(
    p_payload #>> '{policy_snapshot_json,correction_tsfin_policy_fingerprint}',
    p_payload #>> '{rate_source_refs_json,correction_tsfin_policy_fingerprint}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,correction_tsfin_policy_fingerprint}',
    p_payload #>> '{snapshot,policy_snapshot_json,correction_tsfin_policy_fingerprint}',
    p_payload ->> 'correction_tsfin_policy_fingerprint',''
  )), '');
  v_invoice_policy_fingerprint := nullif(btrim(coalesce(
    p_payload #>> '{policy_snapshot_json,correction_invoice_policy_fingerprint}',
    p_payload #>> '{rate_source_refs_json,correction_invoice_policy_fingerprint}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,correction_invoice_policy_fingerprint}',
    p_payload #>> '{snapshot,policy_snapshot_json,correction_invoice_policy_fingerprint}',
    p_payload ->> 'correction_invoice_policy_fingerprint',''
  )), '');
  v_supplied_tsfin_policy := coalesce(
    p_payload #> '{policy_snapshot_json,correction_tsfin_policy}',
    p_payload #> '{snapshot_json,policy_snapshot_json,correction_tsfin_policy}',
    p_payload #> '{snapshot,policy_snapshot_json,correction_tsfin_policy}'
  );
  v_supplied_invoice_policy := coalesce(
    p_payload #> '{policy_snapshot_json,correction_invoice_policy}',
    p_payload #> '{snapshot_json,policy_snapshot_json,correction_invoice_policy}',
    p_payload #> '{snapshot,policy_snapshot_json,correction_invoice_policy}'
  );
  v_erni := coalesce(
    p_payload #>> '{policy_snapshot_json,erni_pct}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,erni_pct}',
    p_payload #>> '{snapshot,policy_snapshot_json,erni_pct}'
  );
  v_apply_erni := coalesce(
    p_payload #>> '{policy_snapshot_json,apply_erni_to}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,apply_erni_to}',
    p_payload #>> '{snapshot,policy_snapshot_json,apply_erni_to}'
  );
  v_pay_vat := coalesce(
    p_payload ->> 'pay_vat_rate_pct_snapshot',
    p_payload #>> '{policy_snapshot_json,pay_vat_rate_pct}',
    p_payload #>> '{snapshot_json,pay_vat_rate_pct_snapshot}',
    p_payload #>> '{snapshot_json,policy_snapshot_json,pay_vat_rate_pct}',
    p_payload #>> '{snapshot,policy_snapshot_json,pay_vat_rate_pct}'
  );
  if v_envelope_fingerprint is distinct from v_leg ->> 'envelope_fingerprint'
     or v_leg_fingerprint is distinct from v_leg ->> 'leg_fingerprint'
     or v_tsfin_policy_fingerprint is distinct from v_tsfin_policy ->> 'tsfin_policy_fingerprint'
     or v_invoice_policy_fingerprint is distinct from v_invoice_policy ->> 'invoice_policy_fingerprint'
     or v_supplied_tsfin_policy is distinct from v_tsfin_policy
     or v_supplied_invoice_policy is distinct from v_invoice_policy
     or v_erni is null or v_erni !~ '^-?[0-9]+([.][0-9]+)?$'
     or v_erni::numeric is distinct from (v_tsfin_policy ->> 'erni_pct')::numeric
     or upper(btrim(coalesce(v_apply_erni, ''))) is distinct from upper(btrim(coalesce(v_tsfin_policy ->> 'apply_erni_to', '')))
     or v_pay_vat is null or v_pay_vat !~ '^-?[0-9]+([.][0-9]+)?$'
     or v_pay_vat::numeric is distinct from (v_tsfin_policy ->> 'applied_pay_vat_rate_pct')::numeric
     or p_payload #>> '{policy_snapshot_json,invoice_vat_rate_pct}' is not null
     or p_payload #>> '{snapshot_json,policy_snapshot_json,invoice_vat_rate_pct}' is not null
     or p_payload #>> '{snapshot,policy_snapshot_json,invoice_vat_rate_pct}' is not null then
    raise exception 'TSFIN_CORRECTION_POLICY_MISMATCH'
      using errcode = 'P0001', detail = jsonb_build_object(
        'timesheet_id', p_timesheet_id,
        'expected_envelope_fingerprint', v_leg ->> 'envelope_fingerprint',
        'supplied_envelope_fingerprint', v_envelope_fingerprint,
        'expected_leg_fingerprint', v_leg ->> 'leg_fingerprint',
        'supplied_leg_fingerprint', v_leg_fingerprint,
        'expected_tsfin_policy_fingerprint', v_tsfin_policy ->> 'tsfin_policy_fingerprint',
        'supplied_tsfin_policy_fingerprint', v_tsfin_policy_fingerprint,
        'expected_invoice_policy_fingerprint', v_invoice_policy ->> 'invoice_policy_fingerprint',
        'supplied_invoice_policy_fingerprint', v_invoice_policy_fingerprint
      )::text;
  end if;
end;
$function$;

-- _ctms_candidate_correction_residuals_v1(uuid,uuid,uuid,text)
CREATE OR REPLACE FUNCTION public._ctms_candidate_correction_residuals_v1(p_session_id uuid, p_candidate_id uuid, p_exclude_pay_batch_id uuid DEFAULT NULL::uuid, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
DECLARE
  v_result jsonb := '[]'::jsonb;
  v_seen_roots uuid[] := array[]::uuid[];
  v_chain jsonb;
  v_root uuid;
  v_residual jsonb;
  v_target_pay_method text;
  v_cache_enabled boolean := false;
  v_session_key text := COALESCE(p_session_id::text, '');
  v_exclude_key text := COALESCE(p_exclude_pay_batch_id::text, '');
  r record;
BEGIN
  IF p_candidate_id IS NULL THEN RETURN v_result; END IF;

  v_cache_enabled :=
    COALESCE(current_setting('cloudtms.pay_workbench_execution_profile_version', true), '') = '2'
    AND COALESCE(current_setting('cloudtms.pay_workbench_reconciliation_optimization_version', true), '') = '1'
    AND NULLIF(BTRIM(COALESCE(
      current_setting('cloudtms.pay_workbench_overpayment_sync_token', true),
      ''
    )), '') IS NOT NULL;

  IF v_cache_enabled THEN
    CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp._bpay_wb_correction_residual_cache_v2 (
      candidate_id uuid NOT NULL,
      session_key text NOT NULL,
      exclude_key text NOT NULL,
      result_json jsonb NOT NULL,
      PRIMARY KEY(candidate_id, session_key, exclude_key)
    ) ON COMMIT DROP;

    SELECT cache_row.result_json
    INTO v_result
    FROM pg_temp._bpay_wb_correction_residual_cache_v2 AS cache_row
    WHERE cache_row.candidate_id = p_candidate_id
      AND cache_row.session_key = v_session_key
      AND cache_row.exclude_key = v_exclude_key;

    IF FOUND THEN
      RETURN COALESCE(v_result, '[]'::jsonb);
    END IF;
  END IF;

  -- The correction chain is one economic unit.  Its target channel is the
  -- candidate's current pay method, not whichever historical correction leg
  -- happens to sort first by timesheet UUID.
  SELECT CASE
    WHEN UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))) = 'PAYE' THEN 'PAYE'
    WHEN UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))) = 'UMBRELLA' THEN 'UMBRELLA'
    ELSE NULL::text
  END
  INTO v_target_pay_method
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF v_target_pay_method IS NULL THEN
    RAISE EXCEPTION 'CORRECTION_CHAIN_TARGET_PAY_METHOD_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'candidate_id', p_candidate_id::text,
              'context', p_context
            )::text;
  END IF;

  FOR r IN
    SELECT DISTINCT financial.timesheet_id
    FROM public.timesheets_financials AS financial
    WHERE financial.is_current = true
      AND financial.candidate_id = p_candidate_id
      AND COALESCE((public._ctms_import_correction_classify_v1(financial.timesheet_id)
        ->> 'is_import_authoritative_correction')::boolean, false)
    ORDER BY financial.timesheet_id
    LIMIT 100
  LOOP
    v_chain := public.timesheet_correction_chain_scope_v1(r.timesheet_id, false, 32, 100);
    IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'CORRECTION_CHAIN_INVALID_FOR_BANKING_PAY'
        USING ERRCODE = 'P0001', DETAIL = v_chain::text;
    END IF;
    v_root := NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;
    IF v_root = ANY(v_seen_roots) THEN CONTINUE; END IF;
    v_seen_roots := array_append(v_seen_roots, v_root);
    v_residual := public.pay_correction_chain_residual_v1(
      r.timesheet_id,
      p_candidate_id,
      v_target_pay_method,
      p_session_id,
      p_exclude_pay_batch_id,
      100
    );
    v_result := v_result || jsonb_build_array(v_residual);
  END LOOP;

  IF v_cache_enabled THEN
    INSERT INTO pg_temp._bpay_wb_correction_residual_cache_v2(
      candidate_id, session_key, exclude_key, result_json
    )
    VALUES (p_candidate_id, v_session_key, v_exclude_key, COALESCE(v_result, '[]'::jsonb))
    ON CONFLICT (candidate_id, session_key, exclude_key) DO UPDATE
    SET result_json = EXCLUDED.result_json;
  END IF;

  RETURN COALESCE(v_result, '[]'::jsonb);
END;
$function$;

-- _ctms_clear_correction_chain_snoozes_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION public._ctms_clear_correction_chain_snoozes_v1(p_snooze_id uuid, p_actor_user_id uuid)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_row public.pay_item_snoozes%rowtype;
  v_chain jsonb;
  v_source_ref text;
  v_count integer:=0;
begin
  select * into v_row from public.pay_item_snoozes where id=p_snooze_id for update;
  if not found or v_row.timesheet_id is null
     or coalesce((public._ctms_import_correction_classify_v1(v_row.timesheet_id)
       ->>'is_import_authoritative_correction')::boolean,false) is not true then return 0; end if;
  v_chain:=public.timesheet_correction_chain_scope_v1(v_row.timesheet_id,false,32,100);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_CHAIN_INVALID_FOR_SNOOZE_CLEAR' using errcode='P0001',detail=v_chain::text;
  end if;
  v_source_ref:='correction-chain:'||(v_chain->>'root_timesheet_id');
  update public.pay_item_snoozes s set
    cleared_at_utc=coalesce(s.cleared_at_utc,now()),
    cleared_by_user_id=coalesce(s.cleared_by_user_id,p_actor_user_id),
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where s.candidate_id=v_row.candidate_id and s.cleared_at_utc is null and s.cancelled_at_utc is null
    and (s.id=p_snooze_id or s.segment_stable_key=v_source_ref or s.source_ref=v_source_ref);
  get diagnostics v_count=row_count;
  return v_count;
end;
$function$;

-- _ctms_correction_carrier_identity_v1(uuid,uuid,text,text)
CREATE OR REPLACE FUNCTION public._ctms_correction_carrier_identity_v1(p_candidate_id uuid, p_correction_root_id uuid, p_component_key_type text, p_component_key_value text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE STRICT
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE
  v_key_type text := upper(btrim(p_component_key_type));
  v_key_value text := btrim(p_component_key_value);
BEGIN
  IF v_key_type <> 'TS_DAY' THEN
    RAISE EXCEPTION 'CORRECTION_CARRIER_COMPONENT_KEY_TYPE_UNSUPPORTED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CORRECTION_CARRIER_COMPONENT_KEY_TYPE_UNSUPPORTED',
              'component_key_type', v_key_type
            )::text;
  END IF;

  IF v_key_value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
     OR (v_key_value::date)::text <> v_key_value THEN
    RAISE EXCEPTION 'CORRECTION_CARRIER_TS_DAY_KEY_INVALID'
      USING ERRCODE = '22007',
            DETAIL = jsonb_build_object(
              'code', 'CORRECTION_CARRIER_TS_DAY_KEY_INVALID',
              'component_key_value', v_key_value
            )::text;
  END IF;

  RETURN concat_ws(
    '|',
    'CORRECTION_CHAIN_V1',
    p_candidate_id::text,
    p_correction_root_id::text,
    v_key_type,
    v_key_value
  );
END;
$function$;

-- _ctms_correction_financials_policy_build_v2(uuid,uuid,text,text,text,uuid,text,timestamp with time zone,boolean,integer)
CREATE OR REPLACE FUNCTION public._ctms_correction_financials_policy_build_v2(p_timesheet_id uuid, p_import_id uuid, p_source_row_key text, p_correction_action text, p_correction_shape text, p_operation_id uuid, p_request_hash text, p_operation_at_utc timestamp with time zone, p_lock_rows boolean DEFAULT false, p_max_depth integer DEFAULT 32)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text := upper(btrim(coalesce(p_correction_action, '')));
  v_shape text := upper(btrim(coalesce(p_correction_shape, '')));
  v_source_row_key text := nullif(btrim(coalesce(p_source_row_key, '')), '');
  v_operation_date date;
  v_import public.hr_imports%rowtype;
  v_shift public.nhsp_shifts%rowtype;
  v_root_timesheet_id uuid;
  v_shift_root_timesheet_id uuid;
  v_root_contract_id uuid;
  v_root_fin public.timesheets_financials%rowtype;
  v_client public.clients%rowtype;
  v_settings public.client_settings%rowtype;
  v_defaults public.settings_defaults%rowtype;
  v_contract public.contracts%rowtype;

  v_reversal_setting public.correction_financials_date_basis_enum;
  v_replacement_setting public.correction_financials_date_basis_enum;
  v_reversal_source text;
  v_replacement_source text;

  v_root_paid_date date;
  v_pay_batch_date_count integer := 0;
  v_pay_batch_date date;
  v_pay_batch_refs jsonb := '[]'::jsonb;
  v_pay_batch_match_scope text := 'NONE';
  v_root_pay_policy_date date;
  v_root_erni numeric;
  v_root_apply_erni text;
  v_root_pay_vat numeric;

  v_invoice_line_count integer := 0;
  v_invoice_rate_count integer := 0;
  v_invoice_chargeability_count integer := 0;
  v_invoice_stream_count integer := 0;
  v_root_invoice_applied_rate numeric;
  v_root_invoice_source_rate numeric;
  v_root_invoice_chargeable boolean;
  v_root_invoice_policy_date date;
  v_invoice_stream text;
  v_invoice_refs jsonb := '[]'::jsonb;
  v_has_invoice_artifact_without_line boolean := false;
  v_cancellation_identity text;
  v_cancellation_identity_present boolean := false;
  v_cancellation_client_in_scope boolean := false;

  v_leg_name text;
  v_setting public.correction_financials_date_basis_enum;
  v_setting_source text;
  v_window public.settings_finance_windows%rowtype;
  v_window_count integer;
  v_operation_window public.settings_finance_windows%rowtype;
  v_operation_window_count integer;
  v_policy_date date;
  v_erni numeric;
  v_apply_erni text;
  v_pay_source_vat numeric;
  v_pay_applied_vat numeric;
  v_pay_vat_applicable boolean;
  v_pay_vat_chargeable boolean;
  v_pay_method text;
  v_invoice_source_vat numeric;
  v_invoice_applied_vat numeric;
  v_invoice_chargeable boolean;
  v_invoice_policy_date date;
  v_pay_evidence_class text;
  v_invoice_evidence_class text;
  v_pay_evidence_refs jsonb;
  v_invoice_evidence_refs jsonb;
  v_pay_component_evidence jsonb;
  v_erni_from_window boolean;
  v_apply_erni_from_window boolean;
  v_pay_vat_from_window boolean;
  v_tsfin_payload jsonb;
  v_tsfin_policy jsonb;
  v_invoice_payload jsonb;
  v_invoice_policy jsonb;
  v_leg_payload jsonb;
  v_leg jsonb;
  v_reversal jsonb;
  v_replacement jsonb;
  v_expected_roles jsonb;
  v_envelope_payload jsonb;
  v_envelope jsonb;
begin
  if p_timesheet_id is null or p_import_id is null or p_operation_id is null then
    raise exception 'CORRECTION_POLICY_OPERATION_SCOPE_REQUIRED'
      using errcode = '22023';
  end if;
  if p_operation_at_utc is null then
    raise exception 'CORRECTION_POLICY_OPERATION_TIMESTAMP_REQUIRED'
      using errcode = '22023';
  end if;
  if nullif(btrim(coalesce(p_request_hash, '')), '') is null then
    raise exception 'CORRECTION_POLICY_REQUEST_HASH_REQUIRED'
      using errcode = '22023';
  end if;
  if v_source_row_key is null then
    raise exception 'CORRECTION_POLICY_SOURCE_ROW_KEY_REQUIRED'
      using errcode = '22023';
  end if;
  if v_action not in ('CHANGED_HOURS','CANCELLATION') then
    raise exception 'CORRECTION_POLICY_ACTION_INVALID'
      using errcode = '22023',
            detail = jsonb_build_object('correction_action', v_action)::text;
  end if;
  if v_shape not in ('REVERSAL_REPLACEMENT','REVERSAL_ONLY') then
    raise exception 'CORRECTION_POLICY_SHAPE_INVALID'
      using errcode = '22023',
            detail = jsonb_build_object('correction_shape', v_shape)::text;
  end if;
  if v_action = 'CANCELLATION' and v_shape <> 'REVERSAL_ONLY' then
    raise exception 'CORRECTION_POLICY_CANCELLATION_MUST_BE_REVERSAL_ONLY'
      using errcode = '22023';
  end if;
  if v_action = 'CHANGED_HOURS' and v_shape <> 'REVERSAL_REPLACEMENT' then
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_MUST_REVERSE_AND_REPLACE'
      using errcode = '22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_POLICY_MAX_DEPTH_INVALID'
      using errcode = '22023';
  end if;

  v_operation_date := (p_operation_at_utc at time zone 'Europe/London')::date;

  select hi.* into v_import
  from public.hr_imports hi
  where hi.id = p_import_id;
  if not found then
    raise exception 'CORRECTION_POLICY_IMPORT_NOT_FOUND' using errcode = 'P0002';
  end if;

  if p_lock_rows then
    select ns.* into v_shift
    from public.nhsp_shifts ns
    where ns.external_row_key = v_source_row_key
    for update;
  else
    select ns.* into v_shift
    from public.nhsp_shifts ns
    where ns.external_row_key = v_source_row_key;
  end if;
  if not found then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_NOT_FOUND'
      using errcode = 'P0002',
            detail = jsonb_build_object('source_row_key', v_source_row_key)::text;
  end if;

  if v_shift.source_system is distinct from v_import.source_system then
    raise exception 'CORRECTION_POLICY_SOURCE_SYSTEM_MISMATCH'
      using errcode = 'P0001';
  end if;
  if v_import.client_id is not null
     and v_shift.client_id is distinct from v_import.client_id then
    raise exception 'CORRECTION_POLICY_IMPORT_CLIENT_MISMATCH'
      using errcode = 'P0001';
  end if;
  if v_shift.timesheet_id is null then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_NOT_LINKED'
      using errcode = 'P0001';
  end if;
  if v_action = 'CHANGED_HOURS'
     and not exists (
       select 1
       from public.hr_rows source_row
       where source_row.import_id = p_import_id
         and source_row.external_row_key = v_source_row_key
     ) then
    -- The source shift legitimately belongs to the previously applied import
    -- until this new immutable revision commits.  Requiring latest_import_id to
    -- equal the new import before that commit rejects every protected amendment.
    -- Prove the new evidence instead: the exact canonical source identity must
    -- be present in this import.  The review guard separately binds the selected
    -- action, evidence fingerprint and request hash to this same immutable row.
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_IMPORT_EVIDENCE_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'source_shift_id', v_shift.id,
              'expected_import_id', p_import_id,
              'source_row_key', v_source_row_key,
              'actual_latest_import_id', v_shift.latest_import_id
            )::text;
  end if;
  if v_action = 'CANCELLATION' then
    -- Before source commit, prove that the canonical source identity is absent
    -- from this exact import. On replay, require the durable cancellation mark
    -- to belong to this same import. This gives preview/claim a fail-closed
    -- cancellation proof without requiring a mutation before the claim exists.
    if v_shift.cancelled_at_utc is not null then
      if v_shift.cancelled_by_import_id is distinct from p_import_id then
        raise exception 'CORRECTION_POLICY_CANCELLATION_IMPORT_EVIDENCE_MISMATCH'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'expected_import_id', p_import_id,
                  'actual_cancelled_by_import_id', v_shift.cancelled_by_import_id
                )::text;
      end if;
    elsif upper(v_shift.source_system::text) = 'HEALTHROSTER' then
      v_cancellation_identity := nullif(btrim(v_shift.hr_request_id), '');
      if v_cancellation_identity is null then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_IDENTITY_MISSING'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'HR_REQUEST_ID'
                )::text;
      end if;
      select exists (
        select 1
        from public.hr_rows source_row
        where source_row.import_id = p_import_id
          and lower(regexp_replace(btrim(coalesce(
                nullif(source_row.hr_request_id, ''),
                nullif(source_row.payload_json ->> 'request_id', '')
              )), '\s+', ' ', 'g'))
              = lower(regexp_replace(v_cancellation_identity, '\s+', ' ', 'g'))
      ) into v_cancellation_identity_present;
      if v_cancellation_identity_present then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_STILL_PRESENT'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'HR_REQUEST_ID',
                  'import_id', p_import_id
                )::text;
      end if;
    elsif upper(v_shift.source_system::text) = 'NHSP' then
      v_cancellation_identity := nullif(btrim(v_shift.ref_num), '');
      if v_cancellation_identity is null then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_IDENTITY_MISSING'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'REF_NUM'
                )::text;
      end if;
      select exists (
        select 1
        from public.hr_rows source_row
        where source_row.import_id = p_import_id
          and lower(regexp_replace(btrim(coalesce(
                nullif(source_row.payload_json ->> 'ref_num', ''),
                nullif(source_row.payload_json ->> 'Reference', ''),
                nullif(source_row.hr_request_id, '')
              )), '\s+', ' ', 'g'))
              = lower(regexp_replace(v_cancellation_identity, '\s+', ' ', 'g'))
      ) into v_cancellation_identity_present;
      if v_cancellation_identity_present then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_STILL_PRESENT'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'REF_NUM',
                  'import_id', p_import_id
                )::text;
      end if;
      select exists (
        select 1
        from public.weekly_import_phase2(
          p_import_id := p_import_id,
          p_system_type := 'NHSP'
        ) import_scope
        where import_scope.client_id = v_shift.client_id
          and import_scope.candidate_id is not null
          and import_scope.work_date is not null
      ) into v_cancellation_client_in_scope;
      if not v_cancellation_client_in_scope then
        raise exception 'CORRECTION_POLICY_CANCELLATION_CLIENT_OUT_OF_SCOPE'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'client_id', v_shift.client_id,
                  'import_id', p_import_id
                )::text;
      end if;
    else
      raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_SYSTEM_INVALID'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'source_shift_id', v_shift.id,
                'source_system', v_shift.source_system
              )::text;
    end if;
  end if;
  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts
    where ts.timesheet_id = p_timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null
      and not a.cycle
      and a.depth < p_max_depth
  )
  select a.timesheet_id into v_root_timesheet_id
  from ancestors a
  order by a.depth desc
  limit 1;

  if v_root_timesheet_id is null then
    raise exception 'CORRECTION_POLICY_TIMESHEET_NOT_FOUND' using errcode = 'P0002';
  end if;
  if exists (
    with recursive ancestors as (
      select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
             array[ts.timesheet_id]::uuid[] path, false cycle
      from public.timesheets ts where ts.timesheet_id = p_timesheet_id
      union all
      select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
             a.path || parent.timesheet_id,
             parent.timesheet_id = any(a.path)
      from ancestors a
      join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
      where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
    ) select 1 from ancestors where cycle
  ) then
    raise exception 'CORRECTION_POLICY_PARENT_CYCLE' using errcode = 'P0001';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id = v_shift.timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
  )
  select a.timesheet_id into v_shift_root_timesheet_id
  from ancestors a
  order by a.depth desc
  limit 1;

  if v_shift_root_timesheet_id is distinct from v_root_timesheet_id then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_ROOT_MISMATCH'
      using errcode = 'P0001';
  end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts
    where ts.timesheet_id in (p_timesheet_id, v_shift.timesheet_id, v_root_timesheet_id)
    order by ts.timesheet_id
    for update;
  end if;

  select ts.contract_id into v_root_contract_id
  from public.timesheets ts where ts.timesheet_id = v_root_timesheet_id;
  select c.* into v_contract from public.contracts c where c.id = v_root_contract_id;

  select tf.* into v_root_fin
  from public.timesheets_financials tf
  where tf.timesheet_id = v_root_timesheet_id
  order by (tf.paid_at_utc is not null) desc,
           (tf.authorised_at_utc is not null) desc,
           tf.is_current desc,
           tf.computed_at_utc desc,
           tf.id
  limit 1;

  select c.* into v_client
  from public.clients c
  where c.id = coalesce(v_shift.client_id, v_root_fin.client_id, v_contract.client_id);
  if not found then
    raise exception 'CORRECTION_POLICY_CLIENT_UNRESOLVED' using errcode = 'P0001';
  end if;

  select d.* into v_defaults
  from public.settings_defaults d
  where d.id = 1
  for share;
  if not found then
    raise exception 'CORRECTION_POLICY_GLOBAL_DEFAULTS_MISSING' using errcode = 'P0001';
  end if;

  select cs.* into v_settings
  from public.client_settings cs
  where cs.client_id = v_client.id
    and (cs.effective_from is null or cs.effective_from <= v_operation_date)
  order by cs.effective_from desc nulls last, cs.updated_at desc, cs.id desc
  limit 1;
  if not found then
    raise exception 'CORRECTION_POLICY_CLIENT_SETTINGS_MISSING' using errcode = 'P0001';
  end if;

  -- Use the same contract-aware authority resolver as staging, review and
  -- final apply.  A contract override must not preview as authoritative and
  -- then be rejected here by a client-settings-only predicate.
  if not coalesce((
    select authority.import_authoritative
    from public._import_review_effective_authority_core_v1(
      case
        when upper(btrim(coalesce(v_shift.source_system::text,'')))='NHSP'
          then 'NHSP'
        else 'HEALTHROSTER'
      end,
      v_root_contract_id,
      v_client.id,
      v_shift.work_date
    ) authority
    limit 1
  ),false) then
    raise exception 'CORRECTION_POLICY_CLIENT_NOT_IMPORT_AUTHORITATIVE'
      using errcode = '22023',
            detail = jsonb_build_object('client_id', v_client.id)::text;
  end if;

  v_reversal_setting := coalesce(
    v_settings.reversal_complete_financials_date,
    v_defaults.reversal_complete_financials_date
  );
  v_replacement_setting := coalesce(
    v_settings.reversal_replacement_financials_date,
    v_defaults.reversal_replacement_financials_date
  );
  v_reversal_source := case
    when v_settings.reversal_complete_financials_date is null then 'GLOBAL'
    else 'CLIENT' end;
  v_replacement_source := case
    when v_settings.reversal_replacement_financials_date is null then 'GLOBAL'
    else 'CLIENT' end;

  if v_root_fin.id is not null then
    v_root_erni := case
      when coalesce(v_root_fin.policy_snapshot_json ->> 'erni_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'erni_pct')::numeric
      else null end;
    v_root_apply_erni := nullif(btrim(v_root_fin.policy_snapshot_json ->> 'apply_erni_to'), '');
    v_root_pay_vat := coalesce(
      v_root_fin.pay_vat_rate_pct_snapshot,
      case when coalesce(v_root_fin.policy_snapshot_json ->> 'pay_vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'pay_vat_rate_pct')::numeric else null end,
      case when coalesce(v_root_fin.policy_snapshot_json ->> 'vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'vat_rate_pct')::numeric else null end
    );
    v_root_paid_date := case when v_root_fin.paid_at_utc is null then null
      else (v_root_fin.paid_at_utc at time zone 'Europe/London')::date end;
    v_root_pay_policy_date := coalesce(
      case when v_root_fin.authorised_at_utc is null then null
        else (v_root_fin.authorised_at_utc at time zone 'Europe/London')::date end,
      (select (ts.authorised_at_server at time zone 'Europe/London')::date
       from public.timesheets ts where ts.timesheet_id = v_root_timesheet_id),
      v_root_paid_date
    );
  end if;

  /*
   * PAID_DATE belongs to the current dated correction component, not to every
   * historical payment ever made against the root timesheet.  A repeated
   * correction can legitimately have an original root settlement and a later
   * canonical carrier settlement on different pay dates.
   *
   * Prefer frozen canonical evidence which proves that the exact current
   * positive timesheet belongs to this TS_DAY component.  Older frozen member
   * lists cannot contain a replacement which did not yet exist, so this is a
   * deterministic Policy X boundary rather than a "latest batch" heuristic.
   */
  select count(distinct pb.authoritative_payment_date)
           filter (where pb.authoritative_payment_date is not null),
         min(pb.authoritative_payment_date),
         coalesce(jsonb_agg(distinct jsonb_build_object(
           'pay_batch_id', pb.id,
           'pay_batch_item_id', pbi.id,
           'authoritative_payment_date', pb.authoritative_payment_date,
           'status', pb.status,
           'source_snapshot_run_id', pb.source_snapshot_run_id,
           'payment_date_match_scope', 'CURRENT_COMPONENT_LINEAGE',
           'frozen_component_snapshot_fingerprint', case
             when pbi.frozen_component_snapshot_json is null then null
             else encode(digest(convert_to(pbi.frozen_component_snapshot_json::text, 'UTF8'), 'sha256'), 'hex')
           end,
           'frozen_source_basis_fingerprint', case
             when pbi.frozen_source_basis_json is null then null
             else encode(digest(convert_to(pbi.frozen_source_basis_json::text, 'UTF8'), 'sha256'), 'hex')
           end,
           'frozen_resolution_result_fingerprint', case
             when pbi.frozen_resolution_result_json is null then null
             else encode(digest(convert_to(pbi.frozen_resolution_result_json::text, 'UTF8'), 'sha256'), 'hex')
           end
         )) filter (where pbi.id is not null), '[]'::jsonb)
  into v_pay_batch_date_count, v_pay_batch_date, v_pay_batch_refs
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
  join public.pay_batches pb on pb.id = pbc.pay_batch_id
  where pbi.timesheet_id = v_root_timesheet_id
    and coalesce(pbi.is_voided, false) = false
    and coalesce(
          nullif(pbi.frozen_component_key_type, ''),
          nullif(pbi.frozen_component_snapshot_json ->> 'component_key_type', '')
        ) = 'TS_DAY'
    and coalesce(
          nullif(pbi.frozen_component_key_value, ''),
          nullif(pbi.frozen_component_snapshot_json ->> 'component_key_value', '')
        ) = v_shift.work_date::text
    and pbi.frozen_component_snapshot_json ->> 'correction_root_id'
          = v_root_timesheet_id::text
    and jsonb_typeof(
          pbi.frozen_component_snapshot_json -> 'ordered_member_timesheet_ids'
        ) = 'array'
    and (pbi.frozen_component_snapshot_json -> 'ordered_member_timesheet_ids')
          ? p_timesheet_id::text;

  if v_pay_batch_date_count > 0 then
    v_pay_batch_match_scope := 'CURRENT_COMPONENT_LINEAGE';
  else
    /*
     * Legacy/first-correction artefacts may predate canonical member
     * provenance.  Fall back only to this exact TS_DAY component.  If that
     * narrower legacy scope still contains multiple payment dates, fail closed
     * because the current authority cannot be proved safely.
     */
    select count(distinct pb.authoritative_payment_date)
             filter (where pb.authoritative_payment_date is not null),
           min(pb.authoritative_payment_date),
           coalesce(jsonb_agg(distinct jsonb_build_object(
             'pay_batch_id', pb.id,
             'pay_batch_item_id', pbi.id,
             'authoritative_payment_date', pb.authoritative_payment_date,
             'status', pb.status,
             'source_snapshot_run_id', pb.source_snapshot_run_id,
             'payment_date_match_scope', 'LEGACY_COMPONENT',
             'frozen_component_snapshot_fingerprint', case
               when pbi.frozen_component_snapshot_json is null then null
               else encode(digest(convert_to(pbi.frozen_component_snapshot_json::text, 'UTF8'), 'sha256'), 'hex')
             end,
             'frozen_source_basis_fingerprint', case
               when pbi.frozen_source_basis_json is null then null
               else encode(digest(convert_to(pbi.frozen_source_basis_json::text, 'UTF8'), 'sha256'), 'hex')
             end,
             'frozen_resolution_result_fingerprint', case
               when pbi.frozen_resolution_result_json is null then null
               else encode(digest(convert_to(pbi.frozen_resolution_result_json::text, 'UTF8'), 'sha256'), 'hex')
             end
           )) filter (where pbi.id is not null), '[]'::jsonb)
    into v_pay_batch_date_count, v_pay_batch_date, v_pay_batch_refs
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb on pb.id = pbc.pay_batch_id
    where pbi.timesheet_id = v_root_timesheet_id
      and coalesce(pbi.is_voided, false) = false
      and coalesce(
            nullif(pbi.frozen_component_key_type, ''),
            nullif(pbi.frozen_component_snapshot_json ->> 'component_key_type', '')
          ) = 'TS_DAY'
      and coalesce(
            nullif(pbi.frozen_component_key_value, ''),
            nullif(pbi.frozen_component_snapshot_json ->> 'component_key_value', '')
          ) = v_shift.work_date::text;
    if v_pay_batch_date_count > 0 then
      v_pay_batch_match_scope := 'LEGACY_COMPONENT';
    end if;
  end if;

  if v_pay_batch_date_count > 1 then
    raise exception 'CORRECTION_POLICY_COMPONENT_PAYMENT_DATE_AMBIGUOUS'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'correction_root_id', v_root_timesheet_id,
              'component_key_type', 'TS_DAY',
              'component_key_value', v_shift.work_date,
              'payment_date_match_scope', v_pay_batch_match_scope
            )::text;
  end if;
  -- Frozen component evidence is more specific than the root TSFIN paid date.
  v_root_paid_date := coalesce(v_pay_batch_date, v_root_paid_date);

  select count(*)::integer,
         count(distinct il.vat_rate_pct)::integer,
         min(il.vat_rate_pct),
         count(distinct case
           when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) in ('true','false')
             then lower(i.header_snapshot_json ->> 'vat_chargeable')
           else null end)::integer,
         min(case when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) = 'true' then 1
                  when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) = 'false' then 0 end)::integer = 1,
         min(case when coalesce(i.header_snapshot_json ->> 'applied_vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
           then (i.header_snapshot_json ->> 'applied_vat_rate_pct')::numeric end),
         min((coalesce(i.issued_at_utc, i.created_at) at time zone 'Europe/London')::date),
         count(distinct case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
           then 'SELF_BILL' else 'NORMAL' end)::integer,
         min(case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
           then 'SELF_BILL' else 'NORMAL' end),
         coalesce(jsonb_agg(distinct jsonb_build_object(
           'invoice_id', i.id,
           'invoice_line_id', il.id,
           'invoice_status', i.status,
           'invoice_stream', case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
             then 'SELF_BILL' else 'NORMAL' end,
           'line_vat_rate_pct', il.vat_rate_pct,
           'line_vat_amount', il.vat_amount
         )), '[]'::jsonb)
  into v_invoice_line_count, v_invoice_rate_count, v_root_invoice_applied_rate,
       v_invoice_chargeability_count, v_root_invoice_chargeable,
       v_root_invoice_source_rate, v_root_invoice_policy_date,
       v_invoice_stream_count, v_invoice_stream, v_invoice_refs
  from public.invoice_lines il
  join public.invoices i on i.id = il.invoice_id
  where il.timesheet_id = v_root_timesheet_id;

  if v_invoice_rate_count > 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_VAT_AMBIGUOUS'
      using errcode = 'P0001';
  end if;
  if v_invoice_stream_count > 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_STREAM_AMBIGUOUS'
      using errcode = 'P0001';
  end if;
  if v_invoice_line_count > 0 and v_invoice_chargeability_count <> 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_CHARGEABILITY_UNPROVEN'
      using errcode = 'P0001';
  end if;
  if exists (
    select 1
    from public.invoice_lines il
    join public.invoices i on i.id=il.invoice_id
    where il.timesheet_id=v_root_timesheet_id
      and (
        case
          when coalesce(i.header_snapshot_json->>'applied_vat_rate_pct','') ~ '^-?[0-9]+([.][0-9]+)?$'
            then (i.header_snapshot_json->>'applied_vat_rate_pct')::numeric is distinct from il.vat_rate_pct
          when nullif(btrim(coalesce(i.header_snapshot_json->>'applied_vat_rate_pct','')),'') is null
            then false
          else true
        end
        or (
          lower(i.header_snapshot_json->>'vat_chargeable')='false'
          and il.vat_rate_pct<>0
        )
      )
  ) then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_HEADER_LINE_CONFLICT'
      using errcode='P0001';
  end if;
  v_has_invoice_artifact_without_line := v_invoice_line_count = 0 and (
    v_shift.invoice_id is not null
    or v_root_fin.locked_by_invoice_id is not null
    or exists (
      select 1
      from jsonb_array_elements(
        case
          when jsonb_typeof(v_root_fin.invoice_breakdown_json) = 'array'
            then v_root_fin.invoice_breakdown_json
          when jsonb_typeof(v_root_fin.invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_root_fin.invoice_breakdown_json -> 'segments') = 'array'
            then v_root_fin.invoice_breakdown_json -> 'segments'
          else '[]'::jsonb
        end
      ) segment_row
      where nullif(btrim(coalesce(
              segment_row ->> 'invoice_locked_invoice_id',
              segment_row ->> 'invoice_id',
              ''
            )), '') is not null
    )
  );
  v_invoice_stream := coalesce(
    v_invoice_stream,
    case when coalesce(v_contract.self_bill, false) then 'SELF_BILL' else 'NORMAL' end
  );

  v_pay_method := upper(btrim(coalesce(v_root_fin.pay_method, (
    select candidate.pay_method from public.candidates candidate
    where candidate.id = coalesce(v_root_fin.candidate_id, v_shift.candidate_id)
  ), '')));

  for v_leg_name in select unnest(array['reversal','replacement']) loop
    if v_leg_name = 'replacement' and v_shape = 'REVERSAL_ONLY' then
      v_tsfin_payload := jsonb_build_object(
        'applicable', false,
        'evidence_class', 'NOT_APPLICABLE',
        'materialisation_stage', 'TSFIN'
      );
      v_tsfin_policy := v_tsfin_payload || jsonb_build_object(
        'tsfin_policy_fingerprint', encode(extensions.digest(convert_to(v_tsfin_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      v_invoice_payload := jsonb_build_object(
        'applicable', false,
        'evidence_class', 'NOT_APPLICABLE',
        'invoice_stream', v_invoice_stream,
        'materialisation_stage', 'INVOICE_GENERATION',
        'final_invoice_vat_materialised', false
      );
      v_invoice_policy := v_invoice_payload || jsonb_build_object(
        'invoice_policy_fingerprint', encode(extensions.digest(convert_to(v_invoice_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      v_leg_payload := jsonb_build_object(
        'leg', 'REPLACEMENT',
        'applicable', false,
        'setting', 'NOT_APPLICABLE',
        'setting_source', 'NOT_APPLICABLE',
        'tsfin_policy', v_tsfin_policy,
        'invoice_policy', v_invoice_policy
      );
      v_replacement := v_leg_payload || jsonb_build_object(
        'leg_fingerprint', encode(extensions.digest(convert_to(v_leg_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      continue;
    end if;

    v_setting := case when v_leg_name = 'reversal' then v_reversal_setting else v_replacement_setting end;
    v_setting_source := case when v_leg_name = 'reversal' then v_reversal_source else v_replacement_source end;
    v_window := null;
    v_window_count := 0;
    v_operation_window := null;
    v_operation_window_count := 0;
    v_policy_date := null;
    v_erni := null;
    v_apply_erni := null;
    v_pay_source_vat := null;
    v_pay_applied_vat := null;
    v_pay_vat_applicable := null;
    v_pay_vat_chargeable := null;
    v_invoice_source_vat := null;
    v_invoice_applied_vat := null;
    v_invoice_chargeable := null;
    v_invoice_policy_date := null;
    v_pay_evidence_class := null;
    v_invoice_evidence_class := null;
    v_pay_evidence_refs := '{}'::jsonb;
    v_invoice_evidence_refs := '{}'::jsonb;
    v_pay_component_evidence := '{}'::jsonb;
    v_erni_from_window := false;
    v_apply_erni_from_window := false;
    v_pay_vat_from_window := false;

    if v_setting = 'PAID_DATE'::public.correction_financials_date_basis_enum then
      v_policy_date := coalesce(v_root_pay_policy_date, v_root_paid_date);
      v_erni := v_root_erni;
      v_apply_erni := v_root_apply_erni;
      v_pay_source_vat := v_root_pay_vat;
      v_pay_applied_vat := v_root_pay_vat;
      v_pay_evidence_class := 'TSFIN_SNAPSHOT';
      v_pay_evidence_refs := jsonb_build_object(
        'root_tsfin_id', v_root_fin.id,
        'root_timesheet_id', v_root_timesheet_id,
        'pay_batch_snapshots', v_pay_batch_refs
      );

      if v_erni is null or v_apply_erni is null or v_pay_source_vat is null then
        if v_root_paid_date is null then
          raise exception 'CORRECTION_POLICY_PAID_DATE_TSFIN_FALLBACK_UNAVAILABLE'
            using errcode = 'P0001';
        end if;
        select count(*) into v_window_count
        from public.settings_finance_windows fw
        where fw.date_from <= v_root_paid_date
          and (fw.date_to is null or fw.date_to >= v_root_paid_date);
        if v_window_count <> 1 then
          raise exception 'CORRECTION_POLICY_PAID_DATE_WINDOW_NOT_EXACT'
            using errcode = 'P0001',
                  detail = jsonb_build_object('paid_date', v_root_paid_date, 'match_count', v_window_count)::text;
        end if;
        select fw.* into v_window
        from public.settings_finance_windows fw
        where fw.date_from <= v_root_paid_date
          and (fw.date_to is null or fw.date_to >= v_root_paid_date)
        order by fw.date_from desc, fw.id
        limit 1;
        v_policy_date := coalesce(v_policy_date, v_root_paid_date);
        v_erni_from_window := v_erni is null;
        v_apply_erni_from_window := v_apply_erni is null;
        v_pay_vat_from_window := v_pay_source_vat is null;
        v_erni := coalesce(v_erni, v_window.erni_pct);
        v_apply_erni := coalesce(v_apply_erni, v_window.apply_erni_to, 'PAYE_ONLY');
        v_pay_source_vat := coalesce(v_pay_source_vat, v_window.vat_rate_pct);
        v_pay_applied_vat := coalesce(v_pay_applied_vat, v_window.vat_rate_pct);
        v_pay_evidence_class := 'FINANCE_WINDOW_EXACT_MATCH';
        v_pay_evidence_refs := v_pay_evidence_refs || jsonb_build_object(
          'legacy_gap_component_only', true,
          'finance_window_id', v_window.id,
          'finance_window_updated_at', v_window.updated_at,
          'finance_window_date_from', v_window.date_from,
          'finance_window_date_to', v_window.date_to
        );
      end if;
      v_apply_erni := upper(btrim(v_apply_erni));
      v_pay_component_evidence := jsonb_build_object(
        'erni_pct', jsonb_build_object(
          'evidence_class', case when v_erni_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_erni,
          'root_tsfin_id', case when v_erni_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_erni_from_window then v_window.id else null end
        ),
        'apply_erni_to', jsonb_build_object(
          'evidence_class', case when v_apply_erni_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_apply_erni,
          'root_tsfin_id', case when v_apply_erni_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_apply_erni_from_window then v_window.id else null end
        ),
        'pay_vat_rate_pct', jsonb_build_object(
          'evidence_class', case when v_pay_vat_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_pay_source_vat,
          'root_tsfin_id', case when v_pay_vat_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_pay_vat_from_window then v_window.id else null end
        ),
        'pay_vat_applicability', jsonb_build_object(
          'evidence_class', 'TSFIN_SNAPSHOT',
          'pay_method_snapshot', nullif(v_pay_method, ''),
          'rule', 'UMBRELLA_PAY_METHOD_ONLY'
        )
      );
      v_pay_evidence_refs := v_pay_evidence_refs || jsonb_build_object(
        'component_evidence', v_pay_component_evidence
      );

      if v_invoice_line_count > 0 then
        v_invoice_policy_date := v_root_invoice_policy_date;
        v_invoice_chargeable := v_root_invoice_chargeable;
        v_invoice_source_vat := case when v_root_invoice_chargeable then
          coalesce(v_root_invoice_source_rate,v_root_invoice_applied_rate)
          else null end;
        v_invoice_applied_vat := v_root_invoice_applied_rate;
        v_invoice_evidence_class := 'INVOICE_LINE';
        v_invoice_evidence_refs := jsonb_build_object(
          'root_timesheet_id', v_root_timesheet_id,
          'invoice_rows', v_invoice_refs
        );

        if v_root_invoice_chargeable is false then
          if v_root_invoice_policy_date is null then
            raise exception 'CORRECTION_POLICY_ROOT_INVOICE_SOURCE_RATE_UNPROVEN'
              using errcode = 'P0001';
          end if;
          select count(*) into v_window_count
          from public.settings_finance_windows fw
          where fw.date_from <= v_root_invoice_policy_date
            and (fw.date_to is null or fw.date_to >= v_root_invoice_policy_date);
          if v_window_count <> 1 then
            raise exception 'CORRECTION_POLICY_ROOT_INVOICE_WINDOW_NOT_EXACT'
              using errcode = 'P0001';
          end if;
          select fw.* into v_window
          from public.settings_finance_windows fw
          where fw.date_from <= v_root_invoice_policy_date
            and (fw.date_to is null or fw.date_to >= v_root_invoice_policy_date)
          order by fw.date_from desc, fw.id limit 1;
          v_invoice_source_vat := v_window.vat_rate_pct;
          v_invoice_evidence_refs := v_invoice_evidence_refs || jsonb_build_object(
            'non_chargeable_source_rate_evidence_class', 'FINANCE_WINDOW_EXACT_MATCH',
            'source_rate_finance_window_id', v_window.id,
            'source_rate_finance_window_updated_at', v_window.updated_at
          );
        end if;
      elsif v_has_invoice_artifact_without_line then
        -- An invoice identifier/lock without its authoritative line/header is
        -- inconsistent evidence. Never guess historical invoice treatment.
        raise exception 'CORRECTION_POLICY_INVOICE_EVIDENCE_INCOMPLETE'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'root_timesheet_id', v_root_timesheet_id,
                  'root_paid_date', v_root_paid_date,
                  'invoice_line_count', v_invoice_line_count,
                  'invoice_artifact_without_line', true,
                  'pay_vat_used_as_invoice_evidence', false,
                  'current_client_state_used_as_historical_invoice_evidence', false
                )::text;
      else
        -- A genuinely never-invoiced root has no historical invoice VAT to
        -- reconstruct. Keep the pay/TSFIN leg on PAID_DATE, but freeze only
        -- the invoice sub-policy from the correction operation window.
        select count(*) into v_operation_window_count
        from public.settings_finance_windows fw
        where fw.date_from <= v_operation_date
          and (fw.date_to is null or fw.date_to >= v_operation_date);
        if v_operation_window_count <> 1 then
          raise exception 'CORRECTION_POLICY_OPERATION_DATE_WINDOW_NOT_EXACT'
            using errcode = 'P0001',
                  detail = jsonb_build_object(
                    'operation_date', v_operation_date,
                    'match_count', v_operation_window_count,
                    'invoice_fallback_reason', 'NO_HISTORICAL_INVOICE'
                  )::text;
        end if;
        select fw.* into v_operation_window
        from public.settings_finance_windows fw
        where fw.date_from <= v_operation_date
          and (fw.date_to is null or fw.date_to >= v_operation_date)
        order by fw.date_from desc, fw.id
        limit 1;
        v_invoice_policy_date := v_operation_date;
        v_invoice_chargeable := v_client.vat_chargeable;
        v_invoice_source_vat := coalesce(
          v_settings.vat_rate_pct,
          v_operation_window.vat_rate_pct
        );
        v_invoice_applied_vat := case when v_invoice_chargeable
          then v_invoice_source_vat else 0 end;
        v_invoice_evidence_class := 'CURRENT_OPERATION_WINDOW';
        v_invoice_evidence_refs := jsonb_build_object(
          'fallback_reason', 'NO_HISTORICAL_INVOICE',
          'requested_leg_setting', 'PAID_DATE',
          'effective_invoice_basis', 'NOW',
          'finance_window_id', v_operation_window.id,
          'finance_window_updated_at', v_operation_window.updated_at,
          'client_id', v_client.id,
          'client_updated_at', v_client.updated_at,
          'client_settings_id', v_settings.id,
          'client_settings_updated_at', v_settings.updated_at,
          'root_timesheet_id', v_root_timesheet_id,
          'root_invoice_line_count', 0,
          'root_invoice_artifact_detected', false,
          'pay_vat_used_as_invoice_evidence', false
        );
      end if;
    else
      v_policy_date := v_operation_date;
      select count(*) into v_window_count
      from public.settings_finance_windows fw
      where fw.date_from <= v_operation_date
        and (fw.date_to is null or fw.date_to >= v_operation_date);
      if v_window_count <> 1 then
        raise exception 'CORRECTION_POLICY_OPERATION_DATE_WINDOW_NOT_EXACT'
          using errcode = 'P0001',
                detail = jsonb_build_object('operation_date', v_operation_date, 'match_count', v_window_count)::text;
      end if;
      select fw.* into v_window
      from public.settings_finance_windows fw
      where fw.date_from <= v_operation_date
        and (fw.date_to is null or fw.date_to >= v_operation_date)
      order by fw.date_from desc, fw.id limit 1;

      v_erni := coalesce(v_settings.erni_pct, v_window.erni_pct);
      v_apply_erni := upper(btrim(coalesce(
        nullif(v_settings.apply_erni_to, ''),
        nullif(v_window.apply_erni_to, ''),
        'PAYE_ONLY'
      )));
      v_pay_source_vat := coalesce(v_settings.vat_rate_pct, v_window.vat_rate_pct);
      v_pay_vat_applicable := v_pay_method like 'UMBRELLA%';
      v_pay_vat_chargeable := coalesce((
        select umbrella.vat_chargeable
        from public.candidates candidate
        join public.umbrellas umbrella on umbrella.id = candidate.umbrella_id
        where candidate.id = coalesce(v_root_fin.candidate_id, v_shift.candidate_id)
      ), false);
      v_pay_applied_vat := case when v_pay_vat_applicable and v_pay_vat_chargeable
        then v_pay_source_vat else 0 end;
      v_pay_evidence_class := 'CURRENT_OPERATION_WINDOW';
      v_pay_evidence_refs := jsonb_build_object(
        'finance_window_id', v_window.id,
        'finance_window_updated_at', v_window.updated_at,
        'client_settings_id', v_settings.id,
        'client_settings_updated_at', v_settings.updated_at,
        'component_evidence', jsonb_build_object(
          'erni_pct', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_erni,
            'source', case when v_settings.erni_pct is null
              then 'FINANCE_WINDOW' else 'CLIENT_SETTINGS' end
          ),
          'apply_erni_to', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_apply_erni,
            'source', case when nullif(v_settings.apply_erni_to, '') is null
              then case when nullif(v_window.apply_erni_to, '') is null
                then 'SYSTEM_FALLBACK' else 'FINANCE_WINDOW' end
              else 'CLIENT_SETTINGS' end
          ),
          'pay_vat_rate_pct', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_pay_source_vat,
            'source', case when v_settings.vat_rate_pct is null
              then 'FINANCE_WINDOW' else 'CLIENT_SETTINGS' end
          ),
          'pay_vat_applicability', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'pay_method_snapshot', nullif(v_pay_method, ''),
            'rule', 'UMBRELLA_PAY_METHOD_ONLY'
          )
        ),
        'erni_economic_eligibility', 'PAYE_ONLY'
      );

      v_invoice_policy_date := v_operation_date;
      v_invoice_chargeable := v_client.vat_chargeable;
      v_invoice_source_vat := coalesce(v_settings.vat_rate_pct, v_window.vat_rate_pct);
      v_invoice_applied_vat := case when v_invoice_chargeable then v_invoice_source_vat else 0 end;
      v_invoice_evidence_class := 'CURRENT_OPERATION_WINDOW';
      v_invoice_evidence_refs := jsonb_build_object(
        'finance_window_id', v_window.id,
        'finance_window_updated_at', v_window.updated_at,
        'client_id', v_client.id,
        'client_updated_at', v_client.updated_at,
        'client_settings_id', v_settings.id,
        'client_settings_updated_at', v_settings.updated_at,
        'pay_vat_used_as_invoice_evidence', false
      );
    end if;

    if v_apply_erni not in ('ALL', 'PAYE_ONLY') then
      raise exception 'CORRECTION_POLICY_APPLY_ERNI_TO_INVALID'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'leg', upper(v_leg_name),
                'apply_erni_to', v_apply_erni,
                'allowed_values', jsonb_build_array('ALL', 'PAYE_ONLY')
              )::text;
    end if;

    v_pay_vat_applicable := coalesce(v_pay_vat_applicable, v_pay_method like 'UMBRELLA%');
    v_pay_vat_chargeable := coalesce(
      v_pay_vat_chargeable,
      v_pay_vat_applicable and coalesce(v_pay_applied_vat, 0) > 0
    );

    if v_erni is null or v_apply_erni is null or v_pay_source_vat is null
       or v_pay_applied_vat is null or v_invoice_source_vat is null
       or v_invoice_applied_vat is null or v_invoice_chargeable is null then
      raise exception 'CORRECTION_POLICY_COMPONENT_UNRESOLVED'
        using errcode = 'P0001',
              detail = jsonb_build_object('leg', upper(v_leg_name), 'setting', v_setting)::text;
    end if;
    if v_pay_evidence_class not in (
      'TSFIN_SNAPSHOT','PAY_BATCH_SNAPSHOT','FINANCE_WINDOW_EXACT_MATCH','CURRENT_OPERATION_WINDOW'
    ) or v_invoice_evidence_class not in (
      'INVOICE_LINE','FINANCE_WINDOW_EXACT_MATCH','CURRENT_OPERATION_WINDOW'
    ) then
      raise exception 'CORRECTION_POLICY_EVIDENCE_CLASS_INVALID' using errcode = 'P0001';
    end if;

    v_tsfin_payload := jsonb_build_object(
      'applicable', true,
      'requested_basis', v_setting,
      'effective_basis', v_setting,
      'pay_policy_date', v_policy_date,
      'erni_pct', v_erni,
      'apply_erni_to', v_apply_erni,
      'pay_method_snapshot', nullif(v_pay_method, ''),
      'pay_vat_applicable', v_pay_vat_applicable,
      'pay_vat_chargeable', v_pay_vat_chargeable,
      'source_pay_vat_rate_pct', v_pay_source_vat,
      'applied_pay_vat_rate_pct', v_pay_applied_vat,
      'evidence_class', v_pay_evidence_class,
      'evidence_refs', v_pay_evidence_refs,
      'materialisation_stage', 'TSFIN'
    );
    v_tsfin_policy := v_tsfin_payload || jsonb_build_object(
      'tsfin_policy_fingerprint', encode(extensions.digest(convert_to(v_tsfin_payload::text,'UTF8'),'sha256'::text),'hex')
    );

    v_invoice_payload := jsonb_build_object(
      'applicable', true,
      'requested_basis', v_setting,
      'effective_basis', case
        when v_invoice_evidence_refs ->> 'fallback_reason' = 'NO_HISTORICAL_INVOICE'
          then 'NOW'
        else v_setting::text
      end,
      'invoice_policy_date', coalesce(v_invoice_policy_date, v_policy_date),
      'invoice_stream', v_invoice_stream,
      'invoice_vat_chargeable', v_invoice_chargeable,
      'source_vat_rate_pct', v_invoice_source_vat,
      'applied_vat_rate_pct', v_invoice_applied_vat,
      'evidence_class', v_invoice_evidence_class,
      'evidence_refs', v_invoice_evidence_refs,
      'materialisation_stage', 'INVOICE_GENERATION',
      'final_invoice_vat_materialised', false
    );
    v_invoice_policy := v_invoice_payload || jsonb_build_object(
      'invoice_policy_fingerprint', encode(extensions.digest(convert_to(v_invoice_payload::text,'UTF8'),'sha256'::text),'hex')
    );

    v_leg_payload := jsonb_build_object(
      'leg', upper(v_leg_name),
      'applicable', true,
      'setting', v_setting,
      'setting_source', v_setting_source,
      'tsfin_policy', v_tsfin_policy,
      'invoice_policy', v_invoice_policy
    );
    v_leg := v_leg_payload || jsonb_build_object(
      'leg_fingerprint', encode(extensions.digest(convert_to(v_leg_payload::text,'UTF8'),'sha256'::text),'hex')
    );
    if v_leg_name = 'reversal' then v_reversal := v_leg; else v_replacement := v_leg; end if;

    v_pay_vat_applicable := null;
    v_pay_vat_chargeable := null;
  end loop;

  v_expected_roles := case when v_shape = 'REVERSAL_ONLY'
    then jsonb_build_array('REVERSAL')
    else jsonb_build_array('REVERSAL','REPLACEMENT') end;

  v_envelope_payload := jsonb_build_object(
    'policy_schema_version', 'IMPORT_CORRECTION_FINANCIALS_POLICY_V2',
    'route_family', 'IMPORT_AUTHORITATIVE',
    'classification', jsonb_build_object(
      'canonical', true,
      'classification_source', 'IMPORT_SOURCE_SHIFT',
      'import_id', p_import_id,
      'source_system', v_import.source_system,
      'source_shift_id', v_shift.id,
      'source_row_key', v_shift.external_row_key,
      'source_shift_timesheet_id', v_shift.timesheet_id,
      'client_eligible_at_operation', true
    ),
    'operation', jsonb_build_object(
      'operation_id', p_operation_id,
      'request_hash', p_request_hash,
      'operation_at_utc', p_operation_at_utc,
      'operation_date_london', v_operation_date,
      'correction_action', v_action
    ),
    'root_timesheet_id', v_root_timesheet_id,
    'correction_chain_id', v_import.source_system::text || ':' || v_shift.id::text,
    'correction_shape', v_shape,
    'expected_member_roles', v_expected_roles,
    'expected_member_count', jsonb_array_length(v_expected_roles),
    'invoice_stream', v_invoice_stream,
    'settings_snapshot', jsonb_build_object(
      'global_settings_id', v_defaults.id,
      'global_settings_updated_at', v_defaults.updated_at,
      'client_settings_id', v_settings.id,
      'client_settings_effective_from', v_settings.effective_from,
      'client_settings_updated_at', v_settings.updated_at,
      'client_id', v_client.id,
      'client_updated_at', v_client.updated_at,
      'reversal_setting', v_reversal_setting,
      'reversal_setting_source', v_reversal_source,
      'replacement_setting', v_replacement_setting,
      'replacement_setting_source', v_replacement_source
    ),
    'root_financial_evidence', jsonb_build_object(
      'root_tsfin_id', v_root_fin.id,
      'root_tsfin_timesheet_id', v_root_fin.timesheet_id,
      'root_tsfin_authorised_at_utc', v_root_fin.authorised_at_utc,
      'root_tsfin_paid_at_utc', v_root_fin.paid_at_utc,
      'root_tsfin_policy_snapshot_fingerprint', case when v_root_fin.id is null then null else
        encode(extensions.digest(convert_to(v_root_fin.policy_snapshot_json::text,'UTF8'),'sha256'::text),'hex') end,
      'pay_batch_snapshots', v_pay_batch_refs,
      'invoice_rows', v_invoice_refs
    ),
    'reversal', v_reversal,
    'replacement', v_replacement
  );
  v_envelope := v_envelope_payload || jsonb_build_object(
    'envelope_fingerprint', encode(extensions.digest(convert_to(v_envelope_payload::text,'UTF8'),'sha256'::text),'hex')
  );
  return v_envelope;
end;
$function$;

-- _ctms_correction_policy_envelope_read_v1(uuid)
CREATE OR REPLACE FUNCTION public._ctms_correction_policy_envelope_read_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_class jsonb;
  v_envelope jsonb;
  v_stored_fingerprint text;
  v_recomputed_fingerprint text;
begin
  v_class := public._ctms_import_correction_classify_v1(p_timesheet_id);

  if coalesce((v_class ->> 'is_import_authoritative_correction')::boolean, false) is not true then
    return null;
  end if;

  select coalesce(
    ts.candidate_hint_text -> 'correction_financials_policy_envelope',
    tf.policy_snapshot_json -> 'correction_financials_policy_envelope',
    tf.rate_source_refs_json -> 'correction_financials_policy_envelope'
  )
  into v_envelope
  from public.timesheets ts
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.timesheet_id = p_timesheet_id;

  if jsonb_typeof(v_envelope) <> 'object' then
    raise exception 'IMPORT_CORRECTION_POLICY_ENVELOPE_MISSING'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'IMPORT_CORRECTION_POLICY_ENVELOPE_MISSING',
              'timesheet_id', p_timesheet_id
            )::text;
  end if;

  v_stored_fingerprint := nullif(btrim(v_envelope ->> 'envelope_fingerprint'), '');
  v_recomputed_fingerprint := encode(
    extensions.digest(
      convert_to((v_envelope - 'envelope_fingerprint')::text, 'UTF8'),
      'sha256'::text
    ),
    'hex'
  );

  if v_stored_fingerprint is null
     or v_stored_fingerprint is distinct from v_recomputed_fingerprint then
    raise exception 'IMPORT_CORRECTION_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'IMPORT_CORRECTION_POLICY_ENVELOPE_FINGERPRINT_INVALID',
              'timesheet_id', p_timesheet_id,
              'stored_fingerprint', v_stored_fingerprint,
              'recomputed_fingerprint', v_recomputed_fingerprint
            )::text;
  end if;

  return v_envelope;
end;
$function$;

-- _ctms_correction_policy_leg_read_v1(uuid)
CREATE OR REPLACE FUNCTION public._ctms_correction_policy_leg_read_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
declare
  v_kind text;
  v_envelope jsonb;
  v_leg jsonb;
  v_tsfin_policy jsonb;
  v_invoice_policy jsonb;
begin
  select upper(btrim(coalesce(ts.correction_kind, '')))
  into v_kind
  from public.timesheets ts
  where ts.timesheet_id = p_timesheet_id;

  v_envelope := public._ctms_correction_policy_envelope_read_v1(p_timesheet_id);
  if v_envelope is null then
    return null;
  end if;

  v_leg := case
    when v_kind in ('CHANGED_HOURS_REVERSAL', 'CANCELLATION_REVERSAL')
      then v_envelope -> 'reversal'
    when v_kind in ('CHANGED_HOURS_REPLACEMENT', 'CANCELLATION_REPLACEMENT')
      then v_envelope -> 'replacement'
    else null
  end;

  if jsonb_typeof(v_leg) <> 'object' then
    raise exception 'IMPORT_CORRECTION_POLICY_LEG_MISSING'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'IMPORT_CORRECTION_POLICY_LEG_MISSING',
              'timesheet_id', p_timesheet_id,
              'correction_kind', v_kind
            )::text;
  end if;

  if nullif(v_leg ->> 'leg_fingerprint', '') is null
     or nullif(v_leg ->> 'leg_fingerprint', '') is distinct from encode(
       extensions.digest(
         convert_to((v_leg - 'leg_fingerprint')::text, 'UTF8'),
         'sha256'::text
       ),
       'hex'
     ) then
    raise exception 'IMPORT_CORRECTION_POLICY_LEG_FINGERPRINT_INVALID'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'IMPORT_CORRECTION_POLICY_LEG_FINGERPRINT_INVALID',
              'timesheet_id', p_timesheet_id,
              'correction_kind', v_kind
            )::text;
  end if;

  v_tsfin_policy := v_leg -> 'tsfin_policy';
  v_invoice_policy := v_leg -> 'invoice_policy';
  if jsonb_typeof(v_tsfin_policy) <> 'object'
     or nullif(v_tsfin_policy ->> 'tsfin_policy_fingerprint','') is distinct from encode(
       extensions.digest(
         convert_to((v_tsfin_policy - 'tsfin_policy_fingerprint')::text,'UTF8'),
         'sha256'::text
       ),'hex'
     ) then
    raise exception 'IMPORT_CORRECTION_TSFIN_SUBPOLICY_FINGERPRINT_INVALID'
      using errcode = 'P0001',
            detail = jsonb_build_object('timesheet_id',p_timesheet_id,'correction_kind',v_kind)::text;
  end if;
  if jsonb_typeof(v_invoice_policy) <> 'object'
     or nullif(v_invoice_policy ->> 'invoice_policy_fingerprint','') is distinct from encode(
       extensions.digest(
         convert_to((v_invoice_policy - 'invoice_policy_fingerprint')::text,'UTF8'),
         'sha256'::text
       ),'hex'
     )
     or v_invoice_policy ->> 'invoice_stream' is distinct from v_envelope ->> 'invoice_stream' then
    raise exception 'IMPORT_CORRECTION_INVOICE_SUBPOLICY_FINGERPRINT_INVALID'
      using errcode = 'P0001',
            detail = jsonb_build_object('timesheet_id',p_timesheet_id,'correction_kind',v_kind)::text;
  end if;

  return v_leg || jsonb_build_object(
    'envelope_fingerprint', v_envelope ->> 'envelope_fingerprint',
    'correction_shape', v_envelope ->> 'correction_shape',
    'correction_chain_id', v_envelope ->> 'correction_chain_id',
    'operation_id', v_envelope #>> '{operation,operation_id}',
    'invoice_stream', v_envelope ->> 'invoice_stream'
  );
end;
$function$;

-- _ctms_correction_resolution_authority_fingerprint_v1(jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._ctms_correction_resolution_authority_fingerprint_v1(p_residual jsonb, p_component jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE STRICT
 SET search_path TO 'pg_catalog', 'extensions'
AS $function$
  SELECT encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'canonical_correction_key',
            p_component ->> 'canonical_correction_key',
          'ordered_member_timesheet_ids',
            coalesce(
              p_residual -> 'ordered_member_timesheet_ids',
              p_residual -> 'member_timesheet_ids',
              '[]'::jsonb
            ),
          'component_lineage_fingerprint',
            p_component ->> 'component_lineage_fingerprint',
          'chain_fingerprint', p_residual ->> 'chain_fingerprint',
          'source_basis_fingerprint',
            p_component ->> 'source_basis_fingerprint',
          'correction_financials_policy_envelope_fingerprint',
            p_residual
              ->> 'correction_financials_policy_envelope_fingerprint',
          'classification',
            coalesce(
              p_component ->> 'classification',
              'TAXABLE_CHANNEL_SENSITIVE'
            ),
          'source_pay_methods',
            coalesce(p_residual -> 'source_pay_methods', '[]'::jsonb),
          'target_pay_method', p_residual ->> 'target_pay_method',
          'truth_ex_vat', p_component -> 'truth_ex_vat',
          'baseline_ex_vat', p_component -> 'baseline_ex_vat',
          'reserved_ex_vat', p_component -> 'reserved_ex_vat',
          'raw_outstanding_ex_vat',
            p_component -> 'raw_outstanding_ex_vat',
          'effective_source_outstanding_ex_vat',
            p_component -> 'effective_source_outstanding_ex_vat',
          'raw_outstanding_inc_vat',
            p_component -> 'raw_outstanding_inc_vat',
          'effective_source_outstanding_inc_vat',
            p_component -> 'effective_source_outstanding_inc_vat',
          'reservation_overrun_detected',
            p_component -> 'reservation_overrun_detected'
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );
$function$;

-- _ctms_enrich_correction_resolution_payload_v1(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._ctms_enrich_correction_resolution_payload_v1(p_session_id uuid, p_payload jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_candidate uuid;
  v_timesheet uuid;
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
begin
  perform public._ctms_assert_payload_corrections_fresh_v1(v_payload,'PAY_CASE_RESOLUTION');
  v_candidate:=nullif(v_payload->>'candidate_id','')::uuid;
  select id into v_timesheet from unnest(public._ctms_payload_timesheet_ids_v1(v_payload,100)) x(id) limit 1;
  if v_candidate is null or v_timesheet is null then
    return v_payload;
  end if;
  v_residuals:=public._ctms_candidate_correction_residuals_v1(
    p_session_id,v_candidate,null::uuid,'PAY_CASE_RESOLUTION'
  );
  select residual.value
  into v_residual
  from jsonb_array_elements(v_residuals) as residual(value)
  where exists (
    select 1
    from jsonb_array_elements_text(
      coalesce(residual.value->'member_timesheet_ids','[]'::jsonb)
    ) as member(member_id)
    where member.member_id=v_timesheet::text
  )
  limit 1;
  if v_residual is null or jsonb_typeof(v_residual)<>'object' then
    -- A correction root is not itself labelled as a replacement/reversal, but
    -- it is still an authoritative member of its residual chain.  Membership
    -- above is therefore the primary gate.  Preserve ordinary timesheets
    -- unchanged; only a row explicitly classified as an import correction
    -- must fail closed when its residual cannot be found.
    if coalesce((
         public._ctms_import_correction_classify_v1(v_timesheet)
           ->>'is_import_authoritative_correction'
       )::boolean,false) then
      raise exception 'CORRECTION_RESIDUAL_REQUIRED_FOR_CASE_RESOLUTION'
        using errcode='P0001';
    end if;
    return v_payload;
  end if;

  select component.value
  into v_component
  from jsonb_array_elements(
    coalesce(v_residual->'components','[]'::jsonb)
  ) component(value)
  where upper(component.value->>'component_key_type')=upper(coalesce(
      v_payload->>'component_key_type',
      v_payload#>>'{bucket_resolutions,0,component_key_type}',
      v_payload#>>'{bucket_resolutions,0,key_type}',
      ''
    ))
    and component.value->>'component_key_value'=coalesce(
      v_payload->>'component_key_value',
      v_payload#>>'{bucket_resolutions,0,component_key_value}',
      v_payload#>>'{bucket_resolutions,0,key_value}',
      ''
    )
  limit 1;

  if v_component is null or jsonb_typeof(v_component)<>'object' then
    raise exception 'CORRECTION_COMPONENT_REQUIRED_FOR_CASE_RESOLUTION'
      using errcode='P0001',
            detail=jsonb_build_object(
              'candidate_id',v_candidate,
              'timesheet_id',v_timesheet,
              'component_key_type',coalesce(
                v_payload->>'component_key_type',
                v_payload#>>'{bucket_resolutions,0,component_key_type}',
                v_payload#>>'{bucket_resolutions,0,key_type}'
              ),
              'component_key_value',coalesce(
                v_payload->>'component_key_value',
                v_payload#>>'{bucket_resolutions,0,component_key_value}',
                v_payload#>>'{bucket_resolutions,0,key_value}'
              )
            )::text;
  end if;

  return v_payload||jsonb_build_object(
    'resolution_identity_key',v_component->>'canonical_correction_key',
    'resolution_identity_version','CORRECTION_CHAIN_V1',
    'canonical_correction_key',v_component->>'canonical_correction_key',
    'resolution_economic_fingerprint',
      v_component->>'resolution_economic_fingerprint',
    'correction_root_id',v_residual->>'root_timesheet_id',
    'ordered_member_timesheet_ids',
      v_residual->'ordered_member_timesheet_ids',
    'component_lineage_fingerprint',
      v_component->>'component_lineage_fingerprint',
    'source_family_key',v_residual->>'source_family_key',
    'correction_financials_policy_envelope',v_residual->'correction_financials_policy_envelope',
    'correction_financials_policy_envelope_fingerprint',v_residual->>'correction_financials_policy_envelope_fingerprint',
    'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
    'correction_chain_fingerprint',v_residual->>'chain_fingerprint'
  );
end;
$function$;

-- _ctms_expand_correction_member_ids_v1(uuid[],integer)
CREATE OR REPLACE FUNCTION public._ctms_expand_correction_member_ids_v1(p_timesheet_ids uuid[], p_max_members integer DEFAULT 100)
 RETURNS uuid[]
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_result uuid[];
begin
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'CORRECTION_MEMBER_LIMIT_INVALID' using errcode = '22023';
  end if;
  if coalesce(cardinality(p_timesheet_ids), 0) > p_max_members then
    raise exception 'CORRECTION_MEMBER_INPUT_TOO_LARGE' using errcode = '22023';
  end if;

  with requested as (
    select distinct x.timesheet_id
    from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) x(timesheet_id)
    where x.timesheet_id is not null
  ), classified as (
    select r.timesheet_id,
           public._ctms_import_correction_classify_v1(r.timesheet_id) as class_json
    from requested r
  ), expanded as (
    select c.timesheet_id
    from classified c
    where coalesce((c.class_json ->> 'is_import_authoritative_correction')::boolean, false) is not true
    union
    select partner.timesheet_id
    from classified c
    join public.timesheets partner
      on partner.correction_id = c.class_json ->> 'correction_id'
     and partner.is_current = true
    where coalesce((c.class_json ->> 'is_import_authoritative_correction')::boolean, false) = true
      and upper(btrim(coalesce(partner.adjustment_origin, ''))) in (
        'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
      )
  )
  select coalesce(array_agg(e.timesheet_id order by e.timesheet_id), array[]::uuid[])
  into v_result
  from (select distinct timesheet_id from expanded) e;

  if cardinality(v_result) > p_max_members then
    raise exception 'CORRECTION_MEMBER_EXPANSION_TOO_LARGE' using errcode = '22023';
  end if;

  return v_result;
end;
$function$;

-- _ctms_expand_lifecycle_items_v1(jsonb,text,uuid,integer)
CREATE OR REPLACE FUNCTION public._ctms_expand_lifecycle_items_v1(p_items jsonb, p_action text, p_actor_user_id uuid, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_result jsonb := '[]'::jsonb;
  v_seen_correction_ids text[] := array[]::text[];
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
  v_preview jsonb;
  v_group jsonb;
  v_transition_item jsonb;
  v_expected_pair_fingerprint text;
  v_correction_id text;
begin
  if v_action not in ('AUTHORISE', 'UNAUTHORISE') then
    raise exception 'CORRECTION_LIFECYCLE_ACTION_INVALID' using errcode = '22023';
  end if;
  if p_actor_user_id is null then
    raise exception 'CORRECTION_LIFECYCLE_ACTOR_REQUIRED' using errcode = '22023';
  end if;
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array'
     or jsonb_array_length(coalesce(p_items, '[]'::jsonb)) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_ITEMS_INVALID' using errcode = '22023';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_id_text := nullif(btrim(coalesce(
      v_item ->> 'timesheet_id', v_item ->> 'timesheetId',
      v_item ->> 'current_timesheet_id', v_item ->> 'currentTimesheetId',
      v_item ->> 'requested_timesheet_id', v_item ->> 'requestedTimesheetId',
      case when coalesce(v_item ->> 'row_key', '') like 'timesheet:%'
        then substring(v_item ->> 'row_key' from 11) end,
      ''
    )), '');

    if v_id_text is null
       or v_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_id := v_id_text::uuid;
    v_class := public._ctms_import_correction_classify_v1(v_id);
    if coalesce((v_class ->> 'is_import_authoritative_correction')::boolean, false) is not true then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_correction_id := nullif(v_class ->> 'correction_id', '');
    if v_correction_id = any(v_seen_correction_ids) then
      continue;
    end if;

    v_preview:=public.timesheet_correction_pair_lifecycle_preview_v1(
      jsonb_build_array(v_item),v_action,p_actor_user_id,p_max_members);
    v_group:=v_preview#>'{groups,0}';
    v_transition:=public.timesheet_correction_pair_transition_v1(
      v_id,v_action,p_actor_user_id,null::uuid,v_group->>'chain_fingerprint',true,p_max_members);
    v_expected_pair_fingerprint:=nullif(btrim(coalesce(
      v_item->>'expected_pair_fingerprint',v_item->>'pair_fingerprint','')),'');
    if v_expected_pair_fingerprint is not null
       and v_expected_pair_fingerprint is distinct from v_group->>'pair_fingerprint' then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_PREVIEW_STALE'
        using errcode='40001',detail=v_group::text;
    end if;
    if coalesce((v_group->>'valid')::boolean,false) is not true
       or coalesce((v_group->>'action_ready')::boolean,false) is not true then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
        using errcode = 'P0001', detail = v_group::text;
    end if;

    v_seen_correction_ids := array_append(v_seen_correction_ids, v_correction_id);
    for v_transition_item in
      select value from jsonb_array_elements(coalesce(v_transition->'transition_items','[]'::jsonb))
    loop
      v_result:=v_result||jsonb_build_array(v_transition_item||jsonb_build_object(
        'lifecycle_group_id','correction:'||v_correction_id,
        'lifecycle_group_size',coalesce((v_transition->>'expected_member_count')::integer,2),
        'pair_fingerprint',v_group->>'pair_fingerprint',
        'selected_timesheet_id',v_id,
        'repairing_legacy_mixed_state',coalesce((v_group->>'repairing_legacy_mixed_state')::boolean,false)
      ));
    end loop;
  end loop;

  if jsonb_array_length(v_result) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_EXPANSION_TOO_LARGE' using errcode = '22023';
  end if;
  return v_result;
end;
$function$;

-- _ctms_import_correction_classify_v1(uuid)
CREATE OR REPLACE FUNCTION public._ctms_import_correction_classify_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
  with target as (
    select
      ts.timesheet_id,
      ts.correction_id,
      upper(btrim(coalesce(ts.correction_kind, ''))) as correction_kind,
      upper(btrim(coalesce(ts.adjustment_origin, ''))) as adjustment_origin,
      coalesce(tf.client_id, c.client_id) as client_id,
      coalesce(
        ts.candidate_hint_text -> 'correction_financials_policy_envelope',
        tf.policy_snapshot_json -> 'correction_financials_policy_envelope',
        tf.rate_source_refs_json -> 'correction_financials_policy_envelope'
      ) as envelope
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    left join public.contracts c on c.id = ts.contract_id
    where ts.timesheet_id = p_timesheet_id
  ), evidence as (
    select t.*,
      case when jsonb_typeof(t.envelope) = 'object' then encode(
        extensions.digest(
          convert_to((t.envelope - 'envelope_fingerprint')::text,'UTF8'),
          'sha256'::text
        ),'hex'
      ) end as recomputed_envelope_fingerprint
    from target t
  ), latest_settings as (
    select cs.*
    from evidence t
    join lateral (
      select cs1.*
      from public.client_settings cs1
      where cs1.client_id = t.client_id
        and (cs1.effective_from is null
             or cs1.effective_from <= (statement_timestamp() at time zone 'Europe/London')::date)
      order by cs1.effective_from desc nulls last,
               cs1.updated_at desc,
               cs1.id desc
      limit 1
    ) cs on true
  )
  select jsonb_build_object(
    'timesheet_id', t.timesheet_id,
    'client_id', t.client_id,
    'correction_id', t.correction_id,
    'correction_kind', nullif(t.correction_kind, ''),
    'adjustment_origin', nullif(t.adjustment_origin, ''),
    'client_eligible', coalesce(
      ls.is_nhsp = true
      or (ls.autoprocess_hr = true and ls.no_timesheet_required = true),
      false
    ),
    'is_import_authoritative_correction',
      coalesce(
        t.adjustment_origin in (
          'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
          'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
          'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
        )
        and t.correction_kind in (
          'CHANGED_HOURS_REVERSAL', 'CHANGED_HOURS_REPLACEMENT',
          'CANCELLATION_REVERSAL', 'CANCELLATION_REPLACEMENT'
        )
        and jsonb_typeof(t.envelope) = 'object'
        and t.envelope ->> 'policy_schema_version' = 'IMPORT_CORRECTION_FINANCIALS_POLICY_V2'
        and t.envelope ->> 'route_family' = 'IMPORT_AUTHORITATIVE'
        and coalesce((t.envelope #>> '{classification,canonical}')::boolean,false)
        and nullif(t.envelope #>> '{operation,operation_id}','') is not null
        and nullif(t.envelope ->> 'correction_chain_id','') is not null
        and t.envelope ->> 'envelope_fingerprint' is not distinct from t.recomputed_envelope_fingerprint
        and (
          (t.correction_kind like 'CHANGED_HOURS_%'
            and t.envelope #>> '{operation,correction_action}' = 'CHANGED_HOURS')
          or
          (t.correction_kind like 'CANCELLATION_%'
            and t.envelope #>> '{operation,correction_action}' = 'CANCELLATION')
        ),
        false
      )
  )
  from evidence t
  left join latest_settings ls on true;
$function$;

-- _ctms_import_correction_operation_find_v1(uuid,uuid,text,text,text)
CREATE OR REPLACE FUNCTION public._ctms_import_correction_operation_find_v1(p_import_id uuid, p_root_timesheet_id uuid, p_source_row_key text, p_correction_action text, p_correction_shape text)
 RETURNS uuid
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_ids uuid[];
begin
  select coalesce(array_agg(distinct o.id order by o.id), array[]::uuid[])
  into v_ids
  from public.import_apply_operations o
  cross join lateral jsonb_array_elements(
    case when jsonb_typeof(o.response_json #> '{correction_operation_contract,correction_units}') = 'array'
      then o.response_json #> '{correction_operation_contract,correction_units}' else '[]'::jsonb end
  ) unit
  where o.import_id = p_import_id
    and o.state not in ('BLOCKED','FAILED_BEFORE_COMMIT')
    and unit ->> 'root_timesheet_id' = p_root_timesheet_id::text
    and unit ->> 'source_row_key' = p_source_row_key
    and upper(unit ->> 'correction_action') = upper(p_correction_action)
    and upper(unit ->> 'correction_shape') = upper(p_correction_shape);

  if cardinality(v_ids) <> 1 then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_UNIQUE'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'import_id', p_import_id,
              'root_timesheet_id', p_root_timesheet_id,
              'source_row_key', p_source_row_key,
              'correction_action', upper(p_correction_action),
              'correction_shape', upper(p_correction_shape),
              'matching_operation_count', cardinality(v_ids)
            )::text;
  end if;
  return v_ids[1];
end;
$function$;

-- _ctms_invoice_payload_has_financial_edit_v1(jsonb)
CREATE OR REPLACE FUNCTION public._ctms_invoice_payload_has_financial_edit_v1(p_payload jsonb)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select coalesce(p_payload::text, '') ~* '"(lines|line_edits|invoice_lines|timesheet_id|shift_id|source_key|vat_rate_pct|vat_amount|subtotal_ex_vat|total_inc_vat|total_charge_ex_vat|total_pay_ex_vat|margin_ex_vat|hours_day|hours_night|hours_sat|hours_sun|hours_bh|pay_day|pay_night|pay_sat|pay_sun|pay_bh|charge_day|charge_night|charge_sat|charge_sun|charge_bh|amount|rate|remove|delete)"';
$function$;

-- _ctms_invoice_vat_rate_for_timesheet_v1(uuid,numeric)
CREATE OR REPLACE FUNCTION public._ctms_invoice_vat_rate_for_timesheet_v1(p_timesheet_id uuid, p_ordinary_rate numeric)
 RETURNS numeric
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_leg jsonb;
  v_invoice_policy jsonb;
  v_rate numeric;
begin
  if coalesce(
    (public._ctms_import_correction_classify_v1(p_timesheet_id)
      ->> 'is_import_authoritative_correction')::boolean,
    false
  ) is not true then
    return p_ordinary_rate;
  end if;

  v_leg := public._ctms_correction_policy_leg_read_v1(p_timesheet_id);
  v_invoice_policy := v_leg -> 'invoice_policy';
  if jsonb_typeof(v_invoice_policy) <> 'object'
     or coalesce((v_invoice_policy ->> 'applicable')::boolean,false) is not true
     or v_invoice_policy ->> 'materialisation_stage' <> 'INVOICE_GENERATION'
     or coalesce((v_invoice_policy ->> 'final_invoice_vat_materialised')::boolean,true) is not false
     or nullif(v_invoice_policy ->> 'applied_vat_rate_pct', '') is null
     or nullif(v_invoice_policy ->> 'source_vat_rate_pct', '') is null
     or nullif(v_invoice_policy ->> 'invoice_vat_chargeable', '') is null then
    raise exception 'IMPORT_CORRECTION_INVOICE_VAT_UNRESOLVED'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'IMPORT_CORRECTION_INVOICE_VAT_UNRESOLVED',
              'timesheet_id', p_timesheet_id
            )::text;
  end if;

  v_rate := (v_invoice_policy ->> 'applied_vat_rate_pct')::numeric;
  if v_rate < 0 or v_rate > 100 then
    raise exception 'IMPORT_CORRECTION_INVOICE_VAT_INVALID'
      using errcode = '22023';
  end if;
  if coalesce((v_invoice_policy ->> 'invoice_vat_chargeable')::boolean,false) is false
     and v_rate <> 0 then
    raise exception 'IMPORT_CORRECTION_INVOICE_VAT_CHARGEABILITY_MISMATCH'
      using errcode = 'P0001';
  end if;

  return v_rate;
end;
$function$;

-- _ctms_invoice_week_candidate_ids_v1(uuid,date,integer)
CREATE OR REPLACE FUNCTION public._ctms_invoice_week_candidate_ids_v1(p_client_id uuid, p_invoice_week_start date, p_max_members integer DEFAULT 100)
 RETURNS uuid[]
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_ids uuid[];
begin
  if p_client_id is null or p_invoice_week_start is null then
    return array[]::uuid[];
  end if;
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'INVOICE_CORRECTION_SCOPE_LIMIT_INVALID' using errcode='22023';
  end if;
  select coalesce(array_agg(x.timesheet_id order by x.timesheet_id),array[]::uuid[])
  into v_ids
  from (
    select distinct tf.timesheet_id
    from public.timesheets_financials tf
    join public.timesheets ts on ts.timesheet_id=tf.timesheet_id and ts.is_current=true
    where tf.is_current=true
      and tf.client_id=p_client_id
      and tf.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and coalesce((public._ctms_import_correction_classify_v1(tf.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
      and (
        (ts.week_ending_date::date - 6)=p_invoice_week_start
        or exists (
          select 1
          from jsonb_array_elements(
            case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
              then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end
          ) seg
          where nullif(btrim(coalesce(seg->>'invoice_target_week_start','')),'')::date
            = p_invoice_week_start
        )
      )
    limit p_max_members + 1
  ) x;
  if cardinality(v_ids)>p_max_members then
    raise exception 'INVOICE_CORRECTION_SCOPE_LIMIT_EXCEEDED' using errcode='22023';
  end if;
  return v_ids;
end;
$function$;

-- _ctms_materialise_candidate_correction_residuals_v1(uuid,uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public._ctms_materialise_candidate_correction_residuals_v1(p_session_id uuid, p_candidate_id uuid, p_source_build_run_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_suggested_component jsonb;
  v_suggestion_payload jsonb;
  v_suggestion_result jsonb;
  v_suggestion_original_basis numeric;
  v_suggestion_current_basis numeric;
  v_suggestion_current_target_amount numeric;
  v_suggestion_target_per_source numeric;
  v_suggestion_scale numeric;
  v_suggestion_original_units numeric;
  v_suggestion_rebased_units numeric;
  v_suggestion_source_rate numeric;
  v_suggestion_charge_rate numeric;
  v_suggestion_source_charge_ex numeric;
  v_suggestion_target_rate numeric;
  v_suggestion_target_ex numeric;
  v_suggestion_target_vat numeric;
  v_suggestion_target_inc numeric;
  v_suggestion_matching_bucket_count integer := 0;
  v_has_suggested_resolution boolean := false;
  v_member_ids uuid[];
  v_carrier_row_ids uuid[];
  v_root_id uuid;
  v_carrier_row_id uuid;
  v_carrier_has_finance_case boolean;
  v_finance_parent_row_id uuid;
  v_finance_component_json jsonb;
  v_finance_component_due numeric := 0;
  v_chain_in_source_build boolean;
  v_source_pay_method text;
  v_line_key text;
  v_resolution_pending boolean := false;
  v_component_needs_resolution boolean := false;
  v_component_outstanding numeric := 0;
  v_component_source_outstanding numeric := 0;
  v_updated integer := 0;
  v_superseded integer := 0;
  v_row_count integer := 0;
begin
  -- Session replacement registers saved decisions before retiring the source
  -- session. Candidate source build is the first point at which target
  -- correction evidence is authoritative, so consume the candidate's locked
  -- registrations here. Missing evidence remains durably PENDING.
  perform public._pay_workbench_case_resolution_carry_process_candidate_v1(
    p_session_id,
    p_candidate_id,
    p_source_build_run_id,
    coalesce(p_now_utc,now())
  );

  v_residuals := public._ctms_candidate_correction_residuals_v1(
    p_session_id,p_candidate_id,null::uuid,'PAY_WORKBENCH_SOURCE_BUILD'
  );
  for v_residual in select value from jsonb_array_elements(v_residuals) loop
    v_root_id:=nullif(v_residual->>'root_timesheet_id','')::uuid;
    v_resolution_pending:=false;

    if coalesce(v_residual->>'block_code','')
         = 'CORRECTION_CHAIN_RESERVATION_OVERRUN'
       and coalesce((v_residual->>'reservation_overrun_count')::integer,0) > 0
       and v_root_id is not null
       and not exists (
         select 1
         from jsonb_array_elements(
           coalesce(v_residual->'components','[]'::jsonb)
         ) live_component(value)
         where round(coalesce(
           nullif(live_component.value->>'target_outstanding_ex_vat','')::numeric,
           0
         ),2) <> 0
       )
       and exists (
         select 1
         from public.pay_batch_items active_batch_item
         join public.pay_batch_candidates active_batch_candidate
           on active_batch_candidate.id = active_batch_item.pay_batch_candidate_id
         join public.pay_batches active_batch
           on active_batch.id = active_batch_candidate.pay_batch_id
         where active_batch_candidate.candidate_id = p_candidate_id
           and coalesce(active_batch_item.is_voided,false) is not true
           and active_batch.cancelled_at_utc is null
           and upper(btrim(coalesce(active_batch.status,''))) in (
             'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
             'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
             'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
           )
           and coalesce(
             active_batch_item.frozen_component_snapshot_json->>'correction_root_id',
             active_batch_item.frozen_resolution_payload_json->>'correction_root_id',
             ''
           ) = v_root_id::text
       ) then
      -- The correction root is already represented by frozen batch authority.
      -- Do not rebuild a second live carrier while that batch remains active.
      -- This branch is valid only when no live component has a remaining
      -- delta. Raw worked-time aliases from the bounded source build must stop
      -- being current; otherwise the workbench misleadingly asks the user to
      -- resolve amounts that are already frozen in the active draft. Keep
      -- independent expense/additional-code domains current. A later changed
      -- amount cannot enter this branch and remains subject to ordinary
      -- freshness/draft-conflict handling.
      select coalesce(
               array_agg(value::uuid order by value),
               array[]::uuid[]
             )
      into v_member_ids
      from jsonb_array_elements_text(
        coalesce(v_residual->'member_timesheet_ids','[]'::jsonb)
      ) value;

      update public.banking_pay_workbench_candidate_source_lines l
      set status='SUPERSEDED',
          updated_at_utc=coalesce(p_now_utc,now())
      where l.session_id=p_session_id
        and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id
        and l.status='CURRENT'
        and l.timesheet_id=any(v_member_ids)
        and l.section='cases_resolutions'
        and upper(coalesce(l.economic_key_json->>'key_type','')) in (
          'TS_TOTAL','TS_DAY'
        );
      get diagnostics v_row_count = row_count;
      v_superseded:=v_superseded+v_row_count;
      continue;
    end if;

    if coalesce((v_residual->>'draftable')::boolean,false) is not true then
      if coalesce(v_residual->>'block_code','')
           = 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
         and coalesce((v_residual->>'unresolved_count')::integer,0) > 0
         and coalesce((v_residual->>'reservation_overrun_count')::integer,0) = 0
         and coalesce((v_residual->>'component_count')::integer,0) > 0 then
        -- A previously resolved canonical carrier is valid only for the source
        -- basis against which it was saved.  When current evidence now needs a
        -- fresh PAYE/umbrella decision, retire those generated carrier rows
        -- before retaining the ordinary source rows for the resolver.  Without
        -- this cleanup a settled historical target (for example £43) can remain
        -- selectable beside the new live delta even though the residual has
        -- correctly rejected the old source-basis fingerprint.
        update public.banking_pay_workbench_candidate_source_lines l
        set status='SUPERSEDED',
            updated_at_utc=coalesce(p_now_utc,now())
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.line_key like
                'correction-chain:'||v_root_id::text||':%'
          and nullif(
                btrim(coalesce(
                  l.source_row_json->>'canonical_correction_key',
                  ''
                )),
                ''
              ) is not null;
        get diagnostics v_row_count = row_count;
        v_superseded:=v_superseded+v_row_count;

        -- Keep the chain fail-closed for drafting, but do not leave its raw
        -- member rows as four independent browser decisions.  The component
        -- loop below rewrites exactly one server-owned carrier per canonical
        -- date/total key into Cases / Resolutions and supersedes every alias.
        -- Components whose saved decision is still fresh remain visible as
        -- waiting siblings; only components with resolution_required=true
        -- ask the user for a new PAYE/umbrella decision.
        v_resolution_pending:=true;
      else
        raise exception 'CORRECTION_RESIDUAL_NOT_DRAFTABLE'
          using errcode='P0001',detail=v_residual::text;
      end if;
    end if;
    select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;
    v_carrier_row_ids:=array[]::uuid[];

    -- Targeted source builds deliberately contain only the timesheet family
    -- that dirtied the workbench.  Do not require an unrelated historical
    -- correction chain to be present in that bounded build.  If any member of
    -- the chain is present, the component-level carrier checks below continue
    -- to fail closed exactly as before.
    select exists (
      select 1
      from public.banking_pay_workbench_candidate_source_lines source_line
      where source_line.session_id=p_session_id
        and source_line.candidate_id=p_candidate_id
        and source_line.source_build_run_id=p_source_build_run_id
        and source_line.status='CURRENT'
        and source_line.timesheet_id=any(v_member_ids)
    )
    into v_chain_in_source_build;
    if coalesce(v_chain_in_source_build,false) is not true then
      continue;
    end if;

    for v_component in select value from jsonb_array_elements(v_residual->'components') loop
      v_suggested_component:=null;
      v_suggestion_payload:=null;
      v_suggestion_result:=null;
      v_suggestion_current_basis:=null;
      v_has_suggested_resolution:=false;
      v_component_needs_resolution:=v_resolution_pending
        and coalesce((v_component->>'resolution_required')::boolean,false)
        and coalesce((v_component->>'resolution_complete')::boolean,false) is not true;
      v_component_source_outstanding:=round(coalesce(
        nullif(v_component->>'effective_source_outstanding_ex_vat','')::numeric,
        nullif(v_component->>'truth_ex_vat','')::numeric,
        0
      ),2);
      v_component_outstanding:=case
        when v_component_needs_resolution
          then v_component_source_outstanding
        else round(coalesce(
          nullif(v_component->>'target_outstanding_ex_vat','')::numeric,
          0
        ),2)
      end;

      if v_component_outstanding=0 then
        update public.banking_pay_workbench_candidate_source_lines l
        set status='SUPERSEDED',
            updated_at_utc=coalesce(p_now_utc,now())
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.timesheet_id=any(v_member_ids)
          and upper(coalesce(l.economic_key_json->>'key_type',''))
              =upper(coalesce(v_component->>'component_key_type',''))
          and coalesce(l.economic_key_json->>'key_value','')
              =coalesce(v_component->>'component_key_value','');
        get diagnostics v_row_count = row_count;
        v_superseded:=v_superseded+v_row_count;
        continue;
      end if;

      v_line_key:='correction-chain:'||v_root_id::text||':'||lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value');

      select l.id into v_carrier_row_id
      from public.banking_pay_workbench_candidate_source_lines l
      where l.session_id=p_session_id and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
        and l.timesheet_id=any(v_member_ids)
        and upper(coalesce(l.economic_key_json->>'key_type',''))=upper(coalesce(v_component->>'component_key_type',''))
        and coalesce(l.economic_key_json->>'key_value','')=coalesce(v_component->>'component_key_value','')
      order by
        -- Replays of the same source-build run must retain the carrier that
        -- already owns the canonical correction-chain identity.
        case when l.line_key=v_line_key then 0 else 1 end,
        case
          when v_component_outstanding < 0
           and nullif(btrim(coalesce(l.source_row_json->>'finance_case_id','')),'') is not null then 0
          when v_component_outstanding >= 0
           and nullif(btrim(coalesce(l.source_row_json->>'finance_case_id','')),'') is null then 0
          else 1
        end,
        case when l.timesheet_id=v_root_id then 0 else 1 end,
        l.source_ordinal,
        l.id
      limit 1 for update;

      -- The ordinary finance-case projector exposes a multi-component case as
      -- one TS_TOTAL parent with server-owned component allocations nested in
      -- case_components.  A resolved negative correction requires one exact
      -- finance-backed carrier per dated component.  Clone only the matching
      -- nested component into a provisional child row; the canonical rewrite
      -- below then owns its final identity.  The component's nominal recovery
      -- comes from the already-authoritative correction residual; this only
      -- splits the aggregate presentation and introduces no new calculation.
      if v_carrier_row_id is null
         and v_component_outstanding < 0
         and coalesce(v_resolution_pending,false) is not true then
        v_finance_parent_row_id:=null::uuid;
        v_finance_component_json:=null::jsonb;

        select
          finance_parent.id,
          nested_component.value
        into
          v_finance_parent_row_id,
          v_finance_component_json
        from public.banking_pay_workbench_candidate_source_lines
               finance_parent
        cross join lateral jsonb_array_elements(
          case
            when jsonb_typeof(
                   finance_parent.source_row_json->'case_components'
                 )='array'
              then finance_parent.source_row_json->'case_components'
            else '[]'::jsonb
          end
        ) nested_component(value)
        where finance_parent.session_id=p_session_id
          and finance_parent.candidate_id=p_candidate_id
          and finance_parent.source_build_run_id=p_source_build_run_id
          and finance_parent.status='CURRENT'
          and finance_parent.timesheet_id=any(v_member_ids)
          and upper(coalesce(
                finance_parent.economic_key_json->>'key_type',
                ''
              ))='TS_TOTAL'
          and nullif(
                btrim(coalesce(
                  finance_parent.source_row_json->>'finance_case_id',
                  ''
                )),
                ''
              ) is not null
          and nullif(
                btrim(coalesce(
                  nested_component.value->>'finance_component_id',
                  ''
                )),
                ''
              ) is not null
          and coalesce(
                nested_component.value->>'source_family_key',
                ''
              )=coalesce(v_residual->>'source_family_key','')
          and upper(coalesce(
                nested_component.value->>'component_key_type',
                ''
              ))=upper(coalesce(
                v_component->>'component_key_type',
                ''
              ))
          and coalesce(
                nested_component.value->>'component_key_value',
                ''
              )=coalesce(v_component->>'component_key_value','')
        order by finance_parent.source_ordinal,finance_parent.id
        limit 1
        for update of finance_parent;

        if v_finance_parent_row_id is not null
           and v_finance_component_json is not null then
          v_finance_component_due:=round(abs(v_component_outstanding),2);

          insert into public.banking_pay_workbench_candidate_source_lines (
            id,
            session_id,
            candidate_id,
            session_version,
            source_change_seq,
            source_build_run_id,
            source_publication_id,
            source_ordinal,
            line_key,
            parent_line_key,
            split_suffix,
            timesheet_id,
            section,
            source_row_json,
            economic_key_json,
            contract_json,
            pay_channel_scope,
            refresh_scope_kind,
            status,
            created_at_utc,
            updated_at_utc
          )
          select
            gen_random_uuid(),
            finance_parent.session_id,
            finance_parent.candidate_id,
            finance_parent.session_version,
            finance_parent.source_change_seq,
            finance_parent.source_build_run_id,
            finance_parent.source_publication_id,
            (
              select coalesce(max(existing_line.source_ordinal),0)+1
              from public.banking_pay_workbench_candidate_source_lines
                     existing_line
              where existing_line.session_id=p_session_id
                and existing_line.candidate_id=p_candidate_id
                and existing_line.source_build_run_id=p_source_build_run_id
            ),
            finance_parent.line_key
              ||':component:'
              ||lower(coalesce(
                   v_component->>'component_key_type',
                   ''
                 ))
              ||':'
              ||lower(coalesce(
                   v_component->>'component_key_value',
                   ''
                 )),
            finance_parent.line_key,
            lower(coalesce(
              v_component->>'component_key_type',
              ''
            ))
              ||':'
              ||lower(coalesce(
                   v_component->>'component_key_value',
                   ''
                 )),
            finance_parent.timesheet_id,
            finance_parent.section,
            coalesce(finance_parent.source_row_json,'{}'::jsonb)
              || jsonb_build_object(
                'finance_component_id',
                  v_finance_component_json->>'finance_component_id',
                'component_key_type',
                  v_component->>'component_key_type',
                'component_key_value',
                  v_component->>'component_key_value',
                'case_components',
                  jsonb_build_array(v_finance_component_json),
                'economic_key',
                  coalesce(
                    finance_parent.source_row_json->'economic_key',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'timesheet_id',finance_parent.timesheet_id::text,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  ),
                -- Recovery is initially blocked with zero allocatable value.
                -- The existing headroom revalidator promotes only the amount
                -- supported by retained positive pay.  Keep the full dated
                -- authority separately as the component's nominal due.
                'amount_ex_vat',0,
                'amount_display',0,
                'preview_amount_ex_vat',0,
                'section_amount_ex_vat',0,
                'component_amount_ex_vat',0,
                'preview_component_amount_ex_vat',0,
                'nominal_due_amount_ex_vat',v_finance_component_due,
                'recoverable_this_pay_run_ex_vat',0,
                'preview_contract',
                  coalesce(
                    finance_parent.source_row_json->'preview_contract',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'amount_ex_vat',0,
                    'selection_amount_ex_vat',0,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  )
              ),
            jsonb_build_object(
              'timesheet_id',finance_parent.timesheet_id::text,
              'key_type',v_component->>'component_key_type',
              'key_value',v_component->>'component_key_value'
            ),
            coalesce(finance_parent.contract_json,'{}'::jsonb),
            finance_parent.pay_channel_scope,
            finance_parent.refresh_scope_kind,
            'CURRENT',
            coalesce(p_now_utc,now()),
            coalesce(p_now_utc,now())
          from public.banking_pay_workbench_candidate_source_lines
                 finance_parent
          where finance_parent.id=v_finance_parent_row_id
          on conflict do nothing
          returning id into v_carrier_row_id;

          -- Safe replay can encounter the child after an earlier retry inserted
          -- it.  Re-read the exact current carrier instead of creating another.
          if v_carrier_row_id is null then
            select exact_line.id
            into v_carrier_row_id
            from public.banking_pay_workbench_candidate_source_lines exact_line
            where exact_line.session_id=p_session_id
              and exact_line.candidate_id=p_candidate_id
              and exact_line.source_build_run_id=p_source_build_run_id
              and exact_line.status='CURRENT'
              and exact_line.timesheet_id=any(v_member_ids)
              and upper(coalesce(
                    exact_line.economic_key_json->>'key_type',
                    ''
                  ))=upper(coalesce(
                    v_component->>'component_key_type',
                    ''
                  ))
              and coalesce(
                    exact_line.economic_key_json->>'key_value',
                    ''
                  )=coalesce(v_component->>'component_key_value','')
              and nullif(
                    btrim(coalesce(
                      exact_line.source_row_json->>'finance_case_id',
                      ''
                    )),
                    ''
                  ) is not null
            order by exact_line.source_ordinal,exact_line.id
            limit 1
            for update;
          end if;
        end if;
      end if;

      -- The bounded source builder may expose an unresolved correction member
      -- as one TS_TOTAL row before its saved dated decisions are normalised.
      -- Use one unretained raw member as the dated carrier, then rewrite its
      -- key below from the server-owned residual. A negative component may
      -- use this fallback only while the coupled chain is still pending
      -- resolution: that row is a locked, non-selectable decision surface and
      -- cannot enter draft scope. Once the chain is resolved, a negative
      -- component must use its exact finance-case row.
      if v_carrier_row_id is null
         and (
           v_component_outstanding > 0
           or v_resolution_pending
         ) then
        select l.id into v_carrier_row_id
        from public.banking_pay_workbench_candidate_source_lines l
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.timesheet_id=any(v_member_ids)
          and l.section in ('cases_resolutions','canonical_preview_lines')
          and upper(coalesce(l.economic_key_json->>'key_type',''))='TS_TOTAL'
          and nullif(
            btrim(coalesce(l.source_row_json->>'finance_case_id','')),
            ''
          ) is null
          and not exists (
            select 1
            from unnest(
              coalesce(v_carrier_row_ids,array[]::uuid[])
            ) as retained_carrier(carrier_row_id)
            where retained_carrier.carrier_row_id=l.id
          )
        order by
          case when l.timesheet_id=v_root_id then 0 else 1 end,
          l.source_ordinal,
          l.id
        limit 1
        for update;
      end if;

      if v_carrier_row_id is null then
        -- A chain-wide residual includes settled/unchanged dated components so
        -- the live calculation remains complete. Those zero-outstanding
        -- components legitimately have no current Banking Pay source row.
        -- Only money-bearing components require a carrier and fail closed.
        if v_component_outstanding=0 then
          continue;
        end if;
        raise exception 'CORRECTION_RESIDUAL_SOURCE_COMPONENT_MISSING'
          using errcode='P0001',detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;

      select exists (
        select 1
        from public.banking_pay_workbench_candidate_source_lines carrier_source
        where carrier_source.id=v_carrier_row_id
          and nullif(
            btrim(coalesce(carrier_source.source_row_json->>'finance_case_id','')),
            ''
          ) is not null
      )
      into v_carrier_has_finance_case;

      if v_component_outstanding < 0
         and coalesce(v_resolution_pending,false) is not true
         and coalesce(v_carrier_has_finance_case,false) is not true then
        raise exception 'CORRECTION_CHAIN_OVERPAYMENT_FINANCE_CASE_CARRIER_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;
      if v_component_outstanding > 0
         and nullif(v_component->>'effective_source_outstanding_ex_vat','') is null then
        raise exception 'CORRECTION_CHAIN_SOURCE_OUTSTANDING_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;
      v_source_pay_method:=null;
      if v_component_outstanding > 0 or v_component_needs_resolution then
        select case
                 when count(distinct upper(btrim(source_method.value)))=1
                   then max(upper(btrim(source_method.value)))
                 else null
               end
        into v_source_pay_method
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(v_component->'source_pay_methods')='array'
              then v_component->'source_pay_methods'
            else '[]'::jsonb
          end
        ) as source_method(value)
        where upper(btrim(source_method.value)) in ('PAYE','UMBRELLA');

        if v_source_pay_method is null then
          raise exception 'CORRECTION_CHAIN_SOURCE_PAY_METHOD_REQUIRED'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'source_pay_methods',
                      coalesce(v_component->'source_pay_methods','[]'::jsonb)
                  )::text;
        end if;
      end if;

      v_suggested_component:=null;
      if v_component_needs_resolution then
        -- The canonical residual owns identity and outstanding economics, but
        -- the bounded source row owns the server-generated PAYE/umbrella rate
        -- evidence used by Suggested Rates Review. Preserve only those
        -- suggestion fields while retaining the canonical residual
        -- fingerprint and component identity. Never ask the browser to invent
        -- a rate or reuse a suggestion for another date/component.
        with signed_source_buckets as (
          select
            upper(coalesce(
              delta_component.value->>'bucket_code',
              delta_component.value#>>'{source_basis_json,bucket_code}',
              ''
            )) as bucket_code,
            round(coalesce(sum(
              case
                when coalesce(
                       delta_component.value->>'source_pay_ex_vat',
                       ''
                     ) ~ '^-?[0-9]+([.][0-9]+)?$'
                  then (
                    delta_component.value->>'source_pay_ex_vat'
                  )::numeric
                else 0
              end
            ),0),2) as signed_bucket_source_pay
          from public.banking_pay_workbench_candidate_source_lines
            delta_line
          cross join lateral jsonb_array_elements(
            coalesce(
              delta_line.source_row_json->'case_components',
              '[]'::jsonb
            )
          ) delta_component(value)
          where delta_line.session_id=p_session_id
            and delta_line.candidate_id=p_candidate_id
            and delta_line.source_build_run_id=p_source_build_run_id
            and delta_line.status in ('CURRENT','SUPERSEDED')
            and delta_line.timesheet_id=any(v_member_ids)
            and upper(coalesce(
                  delta_component.value->>'component_key_type',
                  ''
                ))=upper(coalesce(
                  v_component->>'component_key_type',
                  ''
                ))
            and coalesce(
                  delta_component.value->>'component_key_value',
                  ''
                )=coalesce(
                  v_component->>'component_key_value',
                  ''
                )
          group by 1
        ), suggestion_candidates as (
          select
            source_component.value as component_json,
            case when source_line.id=v_carrier_row_id then 0 else 1 end
              as carrier_ordinal,
            case when source_line.status='CURRENT' then 0 else 1 end
              as status_ordinal,
            source_line.source_ordinal,
            source_line.id as source_line_id,
            source_bucket.bucket_code,
            source_bucket.signed_bucket_source_pay
          from public.banking_pay_workbench_candidate_source_lines
            source_line
          cross join lateral jsonb_array_elements(
            coalesce(
              source_line.source_row_json->'case_components',
              '[]'::jsonb
            )
          ) source_component(value)
          join signed_source_buckets source_bucket
            on source_bucket.bucket_code=upper(coalesce(
              source_component.value->>'bucket_code',
              source_component.value#>>'{source_basis_json,bucket_code}',
              ''
            ))
          where source_line.session_id=p_session_id
            and source_line.candidate_id=p_candidate_id
            and source_line.source_build_run_id=p_source_build_run_id
            and source_line.status in ('CURRENT','SUPERSEDED')
            and source_line.timesheet_id=any(v_member_ids)
            and upper(coalesce(
                  source_component.value->>'component_key_type',
                  ''
                ))=upper(coalesce(v_component->>'component_key_type',''))
            and coalesce(
                  source_component.value->>'component_key_value',
                  ''
                )=coalesce(v_component->>'component_key_value','')
            and jsonb_typeof(
                  source_component.value
                    ->'suggested_resolution_payload_json'
                )='object'
            and jsonb_typeof(
                  source_component.value
                    ->'suggested_resolution_result_json'
                )='object'
            and upper(coalesce(
                  source_component.value->>'source_pay_method',
                  source_component.value
                    #>>'{suggested_resolution_result_json,source_pay_method}',
                  ''
                ))=upper(v_source_pay_method)
            and upper(coalesce(
                  source_component.value
                    #>>'{suggested_resolution_payload_json,target_pay_method}',
                  source_component.value
                    #>>'{suggested_resolution_result_json,target_pay_method}',
                  ''
                ))=upper(coalesce(v_component->>'target_pay_method',''))
        ), eligible_suggestion_candidates as (
          select suggestion_candidate.*
          from suggestion_candidates suggestion_candidate
          where abs(suggestion_candidate.signed_bucket_source_pay)>0.005
            and sign(suggestion_candidate.signed_bucket_source_pay)
                =sign(v_component_source_outstanding)
        )
        select
          count(distinct eligible.bucket_code)::integer,
          (
            jsonb_agg(
              eligible.component_json
              order by
                eligible.carrier_ordinal,
                eligible.status_ordinal,
                eligible.source_ordinal,
                eligible.source_line_id
            )->0
          )
        into
          v_suggestion_matching_bucket_count,
          v_suggested_component
        from eligible_suggestion_candidates eligible;

        if v_suggested_component is null then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_REQUIRED'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'component_key_type',
                      v_component->>'component_key_type',
                    'component_key_value',
                      v_component->>'component_key_value',
                    'source_pay_method',v_source_pay_method,
                    'target_pay_method',
                      upper(coalesce(v_component->>'target_pay_method',''))
                  )::text;
        end if;
        if v_suggestion_matching_bucket_count<>1 then
          -- A dated correction component can span more than one pay bucket
          -- (for example DAY plus NIGHT). No single historical rate is then
          -- an honest Suggested Rate. Keep the canonical resolution case
          -- available for the established custom-resolution pathway. Preserve
          -- one deterministic row-backed source basis solely so the operator
          -- can enter a replacement rate; do not expose its target suggestion,
          -- guess, average, or let an optional suggestion abort the whole
          -- Workbench source build.
          v_suggestion_current_basis:=round(
            abs(coalesce(v_component_source_outstanding,0)),
            2
          );
          v_suggestion_source_rate:=nullif(
            v_suggested_component->>'source_rate',
            ''
          )::numeric;
          v_suggestion_charge_rate:=nullif(
            v_suggested_component->>'source_charge_rate',
            ''
          )::numeric;
          v_suggestion_rebased_units:=case
            when coalesce(v_suggestion_source_rate,0)>0
              then round(
                v_suggestion_current_basis/v_suggestion_source_rate,
                6
              )
            else null
          end;
          v_suggestion_source_charge_ex:=case
            when v_suggestion_rebased_units is not null
             and v_suggestion_charge_rate is not null
              then round(
                v_suggestion_rebased_units*v_suggestion_charge_rate,
                2
              )
            else null
          end;
          v_suggested_component:=v_suggested_component
            ||jsonb_build_object(
              'source_units',v_suggestion_rebased_units,
              'source_rate',v_suggestion_source_rate,
              'source_charge_rate',v_suggestion_charge_rate,
              'source_pay_ex_vat',v_suggestion_current_basis,
              'source_charge_ex_vat',v_suggestion_source_charge_ex,
              'source_margin_ex_vat',case
                when v_suggestion_source_charge_ex is null then null
                else round(
                  v_suggestion_source_charge_ex
                    -v_suggestion_current_basis,
                  2
                )
              end
            );
        else

        -- A source component can describe the full historical shift while the
        -- canonical correction residual now represents only a smaller unpaid
        -- remainder. Keep the historical source basis/fingerprint as
        -- provenance, but proportionally rebase the actionable suggestion to
        -- the current residual. Copying the old payload unchanged would turn a
        -- £17.39 residual back into the historical £130 target.
        v_suggestion_payload:=coalesce(
          v_suggested_component->'suggested_resolution_payload_json',
          '{}'::jsonb
        );
        v_suggestion_result:=coalesce(
          v_suggested_component->'suggested_resolution_result_json',
          '{}'::jsonb
        );
        v_suggestion_original_basis:=case
          when coalesce(
                 v_suggestion_result
                   ->>'applied_basis_source_amount_ex_vat',
                 v_suggestion_result->>'basis_source_amount_ex_vat',
                 v_suggestion_payload
                   ->>'applied_basis_source_amount_ex_vat',
                 v_suggested_component->>'source_pay_ex_vat',
                 v_suggested_component->>'component_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then abs(coalesce(
              v_suggestion_result
                ->>'applied_basis_source_amount_ex_vat',
              v_suggestion_result->>'basis_source_amount_ex_vat',
              v_suggestion_payload
                ->>'applied_basis_source_amount_ex_vat',
              v_suggested_component->>'source_pay_ex_vat',
              v_suggested_component->>'component_amount_ex_vat'
            )::numeric)
          else null
        end;
        v_suggestion_target_per_source:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_ex_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
           and abs(
                 (
                   v_suggestion_result
                     ->>'target_amount_ex_vat_per_source_ex_vat'
                 )::numeric
               ) > 0
            then abs(
              (
                v_suggestion_result
                  ->>'target_amount_ex_vat_per_source_ex_vat'
              )::numeric
            )
          when coalesce(v_suggestion_original_basis,0)>0
           and coalesce(
                 v_suggestion_result->>'target_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
           and abs(
                 (v_suggestion_result->>'target_amount_ex_vat')::numeric
               ) > 0
            then abs(
              (v_suggestion_result->>'target_amount_ex_vat')::numeric
                / v_suggestion_original_basis
            )
          else null
        end;
        -- The signed correction ledger is source-channel authority. Settled
        -- and reserved correction movements are reconciled through their
        -- frozen source reservation amounts, so only the remaining source
        -- residual may be converted to the target channel. Using the full
        -- historical shift units here would turn a £5.22 residual back into
        -- the historical £130 target.
        v_suggestion_current_basis:=round(
          abs(coalesce(v_component_source_outstanding,0)),
          2
        );
        v_suggestion_current_target_amount:=case
          when coalesce(v_suggestion_target_per_source,0)>0
            then round(
              v_suggestion_current_basis
                * v_suggestion_target_per_source,
              2
            )
          else null
        end;
        v_suggestion_original_units:=case
          when coalesce(
                 v_suggestion_result->>'target_units',
                 v_suggestion_payload->>'target_units',
                 v_suggested_component->>'source_units',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then abs(coalesce(
              v_suggestion_result->>'target_units',
              v_suggestion_payload->>'target_units',
              v_suggested_component->>'source_units'
            )::numeric)
          else null
        end;
        v_suggestion_source_rate:=case
          when coalesce(
                 v_suggested_component->>'source_rate',
                 v_suggested_component#>>'{source_basis_json,source_rate}',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then coalesce(
              v_suggested_component->>'source_rate',
              v_suggested_component#>>'{source_basis_json,source_rate}'
            )::numeric
          else null
        end;
        v_suggestion_charge_rate:=case
          when coalesce(
                 v_suggested_component->>'source_charge_rate',
                 v_suggested_component
                   #>>'{source_basis_json,source_charge_rate}',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then coalesce(
              v_suggested_component->>'source_charge_rate',
              v_suggested_component
                #>>'{source_basis_json,source_charge_rate}'
            )::numeric
          else null
        end;

        if coalesce(v_suggestion_original_basis,0)<=0
           or coalesce(v_suggestion_original_units,0)<=0
           or coalesce(v_suggestion_current_target_amount,0)<=0
           or coalesce(v_suggestion_target_per_source,0)<=0
           or coalesce(v_suggestion_current_basis,0)<=0 then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_BASIS_INVALID'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'historical_basis_source_amount_ex_vat',
                      v_suggestion_original_basis,
                    'current_basis_source_amount_ex_vat',
                      v_suggestion_current_basis,
                    'current_target_amount_ex_vat',
                      v_suggestion_current_target_amount,
                    'target_amount_ex_vat_per_source_ex_vat',
                      v_suggestion_target_per_source,
                    'historical_source_units',
                      v_suggestion_original_units
                  )::text;
        end if;

        v_suggestion_scale:=
          v_suggestion_current_basis/v_suggestion_original_basis;
        v_suggestion_rebased_units:=round(
          v_suggestion_original_units*v_suggestion_scale,
          6
        );
        v_suggestion_target_rate:=case
          when coalesce(
                 v_suggestion_payload->>'suggested_target_rate',
                 v_suggestion_result->>'replacement_rate',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(coalesce(
              v_suggestion_payload->>'suggested_target_rate',
              v_suggestion_result->>'replacement_rate'
            )::numeric,2)
          else null
        end;
        v_suggestion_target_ex:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_ex_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              v_suggestion_current_basis
                * (
                    v_suggestion_result
                      ->>'target_amount_ex_vat_per_source_ex_vat'
                  )::numeric,
              2
            )
          when coalesce(
                 v_suggestion_result->>'target_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'target_amount_ex_vat')::numeric
                * v_suggestion_scale,
              2
            )
          when v_suggestion_target_rate is not null
            then round(
              v_suggestion_rebased_units*v_suggestion_target_rate,
              2
            )
          else null
        end;
        v_suggestion_target_vat:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              v_suggestion_current_basis
                * (
                    v_suggestion_result
                      ->>'target_amount_vat_per_source_ex_vat'
                  )::numeric,
              2
            )
          when coalesce(
                 v_suggestion_result->>'target_amount_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'target_amount_vat')::numeric
                * v_suggestion_scale,
              2
            )
          else 0
        end;
        -- Inclusive VAT is a derived total.  Re-scaling a historical inclusive
        -- ratio independently can differ by a penny from the separately rounded
        -- ex-VAT and VAT authorities, so always add those two final amounts.
        v_suggestion_target_inc:=round(
          coalesce(v_suggestion_target_ex,0)
            + coalesce(v_suggestion_target_vat,0),
          2
        );
        if v_suggestion_target_rate is null
           and coalesce(v_suggestion_rebased_units,0)<>0
           and v_suggestion_target_ex is not null then
          v_suggestion_target_rate:=round(
            v_suggestion_target_ex/v_suggestion_rebased_units,
            2
          );
        end if;
        if v_suggestion_target_ex is null
           or v_suggestion_target_rate is null then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_RESULT_INVALID'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'current_basis_source_amount_ex_vat',
                      v_suggestion_current_basis,
                    'rebased_source_units',
                      v_suggestion_rebased_units
                  )::text;
        end if;

        v_suggestion_source_charge_ex:=case
          when v_suggestion_charge_rate is not null
            then round(
              v_suggestion_rebased_units*v_suggestion_charge_rate,
              2
            )
          when coalesce(
                 v_suggestion_result->>'source_charge_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'source_charge_ex_vat')::numeric
                * v_suggestion_scale,
              2
            )
          else null
        end;

        v_suggestion_payload:=jsonb_strip_nulls(
          v_suggestion_payload
          || jsonb_build_object(
            'applied_basis_source_amount_ex_vat',
              v_suggestion_current_basis,
            'source_units',v_suggestion_rebased_units,
            'target_units',v_suggestion_rebased_units,
            'suggested_target_rate',v_suggestion_target_rate,
            'reuse_mode','PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'correction_residual_basis_rebased',true,
            'correction_residual_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          )
        );
        v_suggestion_result:=jsonb_strip_nulls(
          v_suggestion_result
          || jsonb_build_object(
            'basis_source_amount_ex_vat',v_suggestion_current_basis,
            'applied_basis_source_amount_ex_vat',
              v_suggestion_current_basis,
            'source_units',v_suggestion_rebased_units,
            'target_units',v_suggestion_rebased_units,
            'replacement_rate',v_suggestion_target_rate,
            'target_amount_ex_vat',v_suggestion_target_ex,
            'target_amount_vat',v_suggestion_target_vat,
            'target_amount_inc_vat',v_suggestion_target_inc,
            'source_pay_ex_vat',v_suggestion_current_basis,
            'source_charge_ex_vat',v_suggestion_source_charge_ex,
            'source_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex
                  - v_suggestion_current_basis,
                2
              )
            end,
            'target_pay_ex_vat',v_suggestion_target_ex,
            'target_charge_ex_vat',v_suggestion_source_charge_ex,
            'target_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex-v_suggestion_target_ex,
                2
              )
            end,
            'margin_delta_ex_vat',round(
              v_suggestion_current_basis-v_suggestion_target_ex,
              2
            ),
            'reuse_mode','PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'correction_residual_basis_rebased',true,
            'correction_residual_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          )
        );
        v_suggested_component:=v_suggested_component
          || jsonb_build_object(
            'source_units',v_suggestion_rebased_units,
            'source_rate',v_suggestion_source_rate,
            'source_charge_rate',v_suggestion_charge_rate,
            'source_pay_ex_vat',v_suggestion_current_basis,
            'source_charge_ex_vat',v_suggestion_source_charge_ex,
            'source_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex
                  - v_suggestion_current_basis,
                2
              )
            end,
            'suggested_resolution_payload_json',v_suggestion_payload,
            'suggested_resolution_result_json',v_suggestion_result,
            'suggestion_explanation_text',
              'This suggestion applies the existing PAYE/umbrella conversion to the current correction residual only. Historical full-shift evidence remains unchanged.'
          );
        v_has_suggested_resolution:=true;
        end if;
      end if;

      update public.banking_pay_workbench_candidate_source_lines l
      set section=case
            when v_resolution_pending then 'cases_resolutions'
            when v_component_outstanding>0 then 'canonical_preview_lines'
            else 'blocked_for_pay'
          end,
          timesheet_id=v_root_id,
          line_key=v_line_key,
          parent_line_key='correction-chain:'||v_root_id::text,
          split_suffix=lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value'),
          source_row_json=coalesce(l.source_row_json,'{}'::jsonb)||jsonb_build_object(
            'timesheet_id',v_root_id::text,'real_business_timesheet_id',v_root_id::text,
            'economic_key',coalesce(l.source_row_json->'economic_key','{}'::jsonb)||jsonb_build_object(
              'timesheet_id',v_root_id::text,
              'key_type',v_component->>'component_key_type',
              'key_value',v_component->>'component_key_value'
            ),
            'source_family_key',v_residual->>'source_family_key',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'resolution_identity',
              v_component->>'canonical_correction_key',
            'case_key',v_line_key,
            'linked_timesheet_id',v_root_id::text,
            'correction_identity_version','CORRECTION_CHAIN_V1',
            'correction_root_id',v_residual->>'root_timesheet_id',
            'ordered_member_timesheet_ids',
              v_residual->'ordered_member_timesheet_ids',
            'component_lineage_fingerprint',
              v_component->>'component_lineage_fingerprint',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint',
            'correction_chain_residual',v_residual,
            'correction_chain_component',v_component,
            'case_components',jsonb_build_array(
              v_component||jsonb_build_object(
                'candidate_id',p_candidate_id::text,
                'linked_timesheet_id',v_root_id::text,
                'component_resolution_key',
                  v_component->>'canonical_correction_key',
                'resolution_identity',
                  v_component->>'canonical_correction_key',
                'source_family_key',v_residual->>'source_family_key',
                'component_key_type',v_component->>'component_key_type',
                'component_key_value',v_component->>'component_key_value',
                'source_pay_method',v_source_pay_method,
                'current_target_pay_method',
                  upper(v_component->>'target_pay_method'),
                'requires_resolution',v_component_needs_resolution,
                'needs_resolution',v_component_needs_resolution,
                'is_actionable_resolution_row',v_component_needs_resolution,
                'has_suggested_resolution',v_has_suggested_resolution,
                'bucket_code',case
                  when v_suggested_component is not null
                    then upper(coalesce(
                      v_suggested_component->>'bucket_code',
                      v_suggested_component
                        #>>'{source_basis_json,bucket_code}',
                      ''
                    ))
                  else null
                end,
                'source_units',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_units','')::numeric
                  else null
                end,
                'source_rate',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_rate','')::numeric
                  else null
                end,
                'source_charge_rate',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_charge_rate','')::numeric
                  else null
                end,
                'source_charge_ex_vat',case
                  when v_suggested_component is not null
                    then nullif(
                      v_suggested_component->>'source_charge_ex_vat',
                      ''
                    )::numeric
                  else null
                end,
                'source_margin_ex_vat',case
                  when v_suggested_component is not null
                    then nullif(
                      v_suggested_component->>'source_margin_ex_vat',
                      ''
                    )::numeric
                  else null
                end,
                'source_basis_json',case
                  when v_suggested_component is not null
                    and jsonb_typeof(
                      v_suggested_component->'source_basis_json'
                    )='object'
                    then v_suggested_component->'source_basis_json'
                  else null
                end,
                'suggested_resolution_payload_json',case
                  when v_has_suggested_resolution
                    then v_suggested_component
                      ->'suggested_resolution_payload_json'
                  else null
                end,
                'suggested_resolution_result_json',case
                  when v_has_suggested_resolution
                    then v_suggested_component
                      ->'suggested_resolution_result_json'
                  else null
                end,
                'suggestion_explanation_text',case
                  when v_has_suggested_resolution
                    then nullif(
                      v_suggested_component->>'suggestion_explanation_text',
                      ''
                    )
                  else null
                end,
                'target_pay_ex_vat',case
                  when v_component_needs_resolution then null
                  else v_component_outstanding
                end,
                'component_amount_ex_vat',v_component_outstanding,
                'preview_due_amount_ex_vat',v_component_outstanding,
                'source_pay_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'source_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'source_entitlement_amount_ex_vat',abs((v_component->>'truth_ex_vat')::numeric),
                'source_reservation_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'remaining_source_amount',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end
              )
            ),
            'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
            'raw_correction_member_rows_suppressed',true,
            -- The raw source row was classified before carried decisions were
            -- replayed.  Project an incomplete component into one canonical
            -- Cases / Resolutions row; project the chain into Ready or Blocked
            -- only after every required component is complete. Draft seeding
            -- still revalidates the canonical key and fingerprints
            -- transactionally.
            'target_section',case
              when v_resolution_pending then 'cases_resolutions'
              when v_component_outstanding>0 then 'canonical_preview_lines'
              else 'blocked_for_pay'
            end,
            'section',case
              when v_resolution_pending then 'cases_resolutions'
              when v_component_outstanding>0 then 'canonical_preview_lines'
              else 'blocked_for_pay'
            end,
            'presentation_section',case
              when v_resolution_pending then 'CASES_RESOLUTIONS'
              when v_component_outstanding>0 then 'READY_TO_PAY'
              else 'BLOCKED_FOR_PAY'
            end,
            'draftable',not v_resolution_pending and v_component_outstanding>0,
            'is_ready_for_draft',not v_resolution_pending and v_component_outstanding>0,
            'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0,
            'case_is_blocked',false,
            'case_needs_resolution_now',v_component_needs_resolution,
            'case_needs_resolution',v_component_needs_resolution,
            'is_case_resolution_satisfied',not v_component_needs_resolution,
            'case_resolution_satisfied_now',not v_component_needs_resolution,
            'has_resolved_rate',not v_component_needs_resolution,
            'resolved_rate_family','BUCKETED',
            'resolution_family','BUCKETED',
            'resolution_action_label',case
              when v_has_suggested_resolution then 'Suggested Rate'
              else 'Custom Rate'
            end,
            'selection_allowed',not v_resolution_pending and v_component_outstanding>0,
            'blocked_reason_codes',case
              when v_component_needs_resolution
                then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
              when v_resolution_pending
                then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
              when v_component_outstanding>0 then '[]'::jsonb
              else jsonb_build_array('NO_PAY_HEADROOM')
            end,
            'case_resolution_summary',
              coalesce(l.source_row_json->'case_resolution_summary','{}'::jsonb)
              || jsonb_build_object(
                'is_blocked',false,
                'has_resolved_rate',not v_component_needs_resolution,
                'case_needs_resolution',v_component_needs_resolution,
                'case_resolution_satisfied_now',not v_component_needs_resolution,
                'resolved_rate_family','BUCKETED',
                'resolution_family','BUCKETED',
                'resolution_action_label',case
                  when v_has_suggested_resolution then 'Suggested Rate'
                  else 'Custom Rate'
                end,
                'resolved_rate_component_count',case
                  when v_component_needs_resolution then 0
                  else 1
                end,
                'unresolved_taxable_count',case
                  when v_component_needs_resolution then 1
                  else 0
                end,
                'unresolved_taxable_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else 0
                end,
                'blocked_case_amount_ex_vat',0,
                'safe_amount_ex_vat',case
                  when v_resolution_pending then 0
                  else greatest(v_component_outstanding,0)
                end,
                'blocked_reason_codes',case
                  when v_component_needs_resolution
                    then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
                  when v_resolution_pending
                    then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
                  when v_component_outstanding>0 then '[]'::jsonb
                  else jsonb_build_array('NO_PAY_HEADROOM')
                end
              ),
            'resolution_badge',case
              when v_component_needs_resolution then 'REQUIRES_RESOLUTION'
              when v_resolution_pending then 'WAITING'
              else 'RESOLVED'
            end,
            'resolution_state',case
              when v_component_needs_resolution then 'RESOLUTION_REQUIRED'
              when v_resolution_pending then 'WAITING_LINKED_RESOLUTION'
              else 'RESOLVED'
            end,
            'policy_x_pre_draft_key_resolved',not v_component_needs_resolution,
            'presentation_reason',case
              when v_component_needs_resolution then 'PAY_METHOD_RESOLUTION_REQUIRED'
              when v_resolution_pending then 'LINKED_CORRECTION_RESOLUTION_PENDING'
              when v_component_outstanding>0 then 'READY_TO_PAY'
              else 'NO_PAY_HEADROOM'
            end
          )
          || case
            when v_component_outstanding < 0
              then jsonb_build_object(
                'amount_ex_vat',0,
                -- Keep the unresolved recovery non-allocatable, but do not
                -- erase the amount whose pay-channel treatment still needs
                -- an operator decision.  Presentation is not draft authority.
                'amount_display',case
                  when v_component_needs_resolution
                    then round(
                      coalesce(
                        v_suggestion_current_basis,
                        abs(v_component_outstanding)
                      ),
                      2
                    )
                  else 0
                end,
                'preview_amount_ex_vat',0,
                'ready_preview_amount_ex_vat',0,
                'section_amount_ex_vat',0,
                'section_amount_display',case
                  when v_component_needs_resolution
                    then round(
                      coalesce(
                        v_suggestion_current_basis,
                        abs(v_component_outstanding)
                      ),
                      2
                    )
                  else 0
                end,
                'component_amount_ex_vat',0,
                'preview_component_amount_ex_vat',0,
                'target_pay_ex_vat',0,
                'nominal_due_amount_ex_vat',
                  round(abs(v_component_outstanding),2),
                'recoverable_this_pay_run_ex_vat',0,
                'preview_contract',
                  coalesce(
                    l.source_row_json->'preview_contract',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'amount_ex_vat',0,
                    'selection_amount_ex_vat',0,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  )
              )
            else jsonb_build_object(
              'amount_ex_vat',v_component_outstanding,
              'amount_display',v_component_outstanding,
              'preview_amount_ex_vat',v_component_outstanding,
              'ready_preview_amount_ex_vat',case
                when v_resolution_pending then 0
                else v_component_outstanding
              end,
              'section_amount_ex_vat',v_component_outstanding,
              'section_amount_display',v_component_outstanding,
              'component_amount_ex_vat',v_component_outstanding,
              'preview_component_amount_ex_vat',v_component_outstanding,
              'source_pay_method',v_source_pay_method,
              'target_pay_method',upper(v_component->>'target_pay_method'),
              'source_pay_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'source_amount_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'source_reservation_amount_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'remaining_source_amount',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'target_pay_ex_vat',case
                when v_component_needs_resolution then null
                else v_component_outstanding
              end,
              'preview_contract',coalesce(l.source_row_json->'preview_contract','{}'::jsonb)||jsonb_build_object(
                'amount_ex_vat',v_component_outstanding,
                'selection_amount_ex_vat',case
                  when v_resolution_pending then 0
                  else v_component_outstanding
                end,
                'source_entitlement_amount_ex_vat',abs((v_component->>'truth_ex_vat')::numeric),
                'source_reservation_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'key_type',v_component->>'component_key_type',
                'key_value',v_component->>'component_key_value',
                'line_key',v_line_key,
                'target_section',case
                  when v_resolution_pending then 'cases_resolutions'
                  when v_component_outstanding>0 then 'canonical_preview_lines'
                  else 'blocked_for_pay'
                end,
                'presentation_section',case
                  when v_resolution_pending then 'CASES_RESOLUTIONS'
                  when v_component_outstanding>0 then 'READY_TO_PAY'
                  else 'BLOCKED_FOR_PAY'
                end,
                'draftable',not v_resolution_pending and v_component_outstanding>0,
                'is_ready_for_draft',not v_resolution_pending and v_component_outstanding>0,
                'selection_allowed',not v_resolution_pending and v_component_outstanding>0,
                'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0,
                'reasons',case
                  when v_component_needs_resolution
                    then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
                  when v_resolution_pending
                    then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
                  when v_component_outstanding>0 then '[]'::jsonb
                  else jsonb_build_array('NO_PAY_HEADROOM')
                end,
                'reason_count',case
                  when v_resolution_pending then 1
                  when v_component_outstanding>0 then 0
                  else 1
                end
              )
            )
          end,
          economic_key_json=jsonb_build_object(
            'timesheet_id',v_root_id::text,
            'key_type',v_component->>'component_key_type',
            'key_value',v_component->>'component_key_value'
          ),
          contract_json=coalesce(l.contract_json,'{}'::jsonb)||jsonb_build_object(
            'policy_x_authority_scope','PRE_DRAFT_CORRECTION_RESIDUAL',
            'residual_fingerprint',v_residual->>'residual_fingerprint',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'correction_identity_version','CORRECTION_CHAIN_V1',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          ),
          updated_at_utc=coalesce(p_now_utc,now())
      where l.id=v_carrier_row_id;
      v_updated:=v_updated+1;
      v_carrier_row_ids:=array_append(v_carrier_row_ids,v_carrier_row_id);

      update public.banking_pay_workbench_candidate_source_lines l
      set status='SUPERSEDED',updated_at_utc=coalesce(p_now_utc,now())
      where l.session_id=p_session_id and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
        and l.id<>v_carrier_row_id and l.timesheet_id=any(v_member_ids)
        and upper(coalesce(l.economic_key_json->>'key_type',''))=upper(coalesce(v_component->>'component_key_type',''))
        and coalesce(l.economic_key_json->>'key_value','')=coalesce(v_component->>'component_key_value','');
      get diagnostics v_row_count = row_count;
      v_superseded := v_superseded + v_row_count;
    end loop;

    -- Remove the case-level TS_TOTAL recovery parent only when every nested
    -- component belongs to this correction chain and now has its own exact
    -- finance-backed carrier.  Mixed or unrelated finance cases remain intact.
    update public.banking_pay_workbench_candidate_source_lines aggregate_line
    set status='SUPERSEDED',
        updated_at_utc=coalesce(p_now_utc,now())
    where aggregate_line.session_id=p_session_id
      and aggregate_line.candidate_id=p_candidate_id
      and aggregate_line.source_build_run_id=p_source_build_run_id
      and aggregate_line.status='CURRENT'
      and aggregate_line.timesheet_id=any(v_member_ids)
      and upper(coalesce(
            aggregate_line.economic_key_json->>'key_type',
            ''
          ))='TS_TOTAL'
      and nullif(
            btrim(coalesce(
              aggregate_line.source_row_json->>'finance_case_id',
              ''
            )),
            ''
          ) is not null
      and jsonb_typeof(
            aggregate_line.source_row_json->'case_components'
          )='array'
      and jsonb_array_length(
            aggregate_line.source_row_json->'case_components'
          )>0
      and not exists (
        select 1
        from jsonb_array_elements(
          aggregate_line.source_row_json->'case_components'
        ) nested_component(value)
        where coalesce(
                nested_component.value->>'source_family_key',
                ''
              )<>coalesce(v_residual->>'source_family_key','')
           or not exists (
             select 1
             from public.banking_pay_workbench_candidate_source_lines
                    exact_component_line
             where exact_component_line.session_id=p_session_id
               and exact_component_line.candidate_id=p_candidate_id
               and exact_component_line.source_build_run_id
                     =p_source_build_run_id
               and exact_component_line.status='CURRENT'
               and exact_component_line.timesheet_id=any(v_member_ids)
               and upper(coalesce(
                     exact_component_line.economic_key_json->>'key_type',
                     ''
                   ))=upper(coalesce(
                     nested_component.value->>'component_key_type',
                     ''
                   ))
               and coalesce(
                     exact_component_line.economic_key_json->>'key_value',
                     ''
                   )=coalesce(
                     nested_component.value->>'component_key_value',
                     ''
                   )
               and nullif(
                     btrim(coalesce(
                       exact_component_line.source_row_json
                         ->>'finance_case_id',
                       ''
                     )),
                     ''
                   ) is not null
           )
      );
    get diagnostics v_row_count = row_count;
    v_superseded:=v_superseded+v_row_count;

    -- A correction chain is one coupled economic unit. Once its dated
    -- component carriers have been materialised, no raw member row may remain
    -- current merely because it used a broader TS_TOTAL key. Independent
    -- expenses and additional codes retain their own non-TS_TOTAL component
    -- keys and are not affected by this lineage suppression.
    update public.banking_pay_workbench_candidate_source_lines l
    set status='SUPERSEDED',updated_at_utc=coalesce(p_now_utc,now())
    where l.session_id=p_session_id and l.candidate_id=p_candidate_id
      and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
      and l.timesheet_id=any(v_member_ids)
      and l.section in ('cases_resolutions','canonical_preview_lines')
      and upper(coalesce(l.economic_key_json->>'key_type',''))='TS_TOTAL'
      and not exists (
        select 1
        from unnest(coalesce(v_carrier_row_ids,array[]::uuid[])) as retained_carrier(carrier_row_id)
        where retained_carrier.carrier_row_id=l.id
      );
    get diagnostics v_row_count = row_count;
    v_superseded:=v_superseded+v_row_count;
  end loop;
  return jsonb_build_object('ok',true,'residual_count',jsonb_array_length(v_residuals),
    'materialised_component_count',v_updated,'superseded_raw_member_row_count',v_superseded);
end;
$function$;

-- _ctms_normalise_correction_case_resolutions_v1(uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public._ctms_normalise_correction_case_resolutions_v1(p_session_id uuid, p_candidate_id uuid, p_anchor_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_resolution public.banking_pay_workbench_session_case_resolutions%rowtype;
  v_source_amount numeric;
  v_target_amount numeric;
  v_bucket_resolutions jsonb:='[]'::jsonb;
  v_bucket_set_digest text;
  v_physical_decision_count integer:=0;
  v_invalid_decision_count integer:=0;
  v_projected_target_count integer:=0;
  v_physical_source_total numeric:=0;
  v_physical_target_total numeric:=0;
  v_base_payload jsonb:='{}'::jsonb;
  v_component_timesheet_id uuid;
  v_canonical_resolution_id uuid;
  v_normalised_resolution_id uuid;
  v_alias_deleted_count integer:=0;
  v_alias_deleted_this_component integer:=0;
  v_updated integer:=0;
  v_normalised_resolution_ids jsonb:='[]'::jsonb;
  v_normalised_resolution_identity_keys jsonb:='[]'::jsonb;
begin
  if p_session_id is null
     or p_candidate_id is null
     or p_anchor_timesheet_id is null
     or coalesce((public._ctms_import_correction_classify_v1(
       p_anchor_timesheet_id
     )->>'is_import_authoritative_correction')::boolean,false) is not true then
    return jsonb_build_object('ok',true,'normalised_count',0);
  end if;

  v_residuals:=public._ctms_candidate_correction_residuals_v1(
    p_session_id,p_candidate_id,null::uuid,'PAY_CASE_RESOLUTION_NORMALISE'
  );

  select residual.value
  into v_residual
  from jsonb_array_elements(v_residuals) as residual(value)
  where exists (
    select 1
    from jsonb_array_elements_text(
      coalesce(residual.value->'member_timesheet_ids','[]'::jsonb)
    ) as member(member_id)
    where member.member_id=p_anchor_timesheet_id::text
  )
  limit 1;

  if v_residual is null or jsonb_typeof(v_residual)<>'object' then
    raise exception 'CORRECTION_RESIDUAL_REQUIRED_FOR_CASE_RESOLUTION'
      using errcode='P0001';
  end if;

  select coalesce(array_agg(member_id::uuid order by member_id),array[]::uuid[])
  into v_member_ids
  from jsonb_array_elements_text(
    coalesce(v_residual->'member_timesheet_ids','[]'::jsonb)
  ) as member(member_id);

  for v_component in
    select component.value
    from jsonb_array_elements(
      coalesce(v_residual->'components','[]'::jsonb)
    ) as component(value)
    where coalesce((component.value->>'resolution_required')::boolean,false)
      and round(coalesce(
        nullif(component.value->>'effective_source_outstanding_ex_vat','')::numeric,
        0
      ),2)<>0
    order by component.value->>'component_key_type',
             component.value->>'component_key_value'
  loop
    v_resolution:=null;
    v_canonical_resolution_id:=null;
    v_normalised_resolution_id:=null;
    select resolution_row.*
    into v_resolution
    from public.banking_pay_workbench_session_case_resolutions as resolution_row
    where resolution_row.session_id=p_session_id
      and resolution_row.candidate_id=p_candidate_id
      and resolution_row.resolution_family='BUCKETED'
      and resolution_row.timesheet_id=any(v_member_ids)
      and upper(btrim(coalesce(resolution_row.component_key_type,'')))
          =upper(btrim(coalesce(v_component->>'component_key_type','')))
      and btrim(coalesce(resolution_row.component_key_value,''))
          =btrim(coalesce(v_component->>'component_key_value',''))
    order by resolution_row.updated_at_utc desc,
             resolution_row.created_at_utc desc,
             resolution_row.id desc
    limit 1
    for update;

    if v_resolution.id is null then
      -- A durable carry registered for this exact canonical component is the
      -- authoritative pending decision source.  Do not manufacture a linked
      -- decision from another dated component while that carry is still
      -- waiting to be replayed.  Otherwise the synthetic row wins the unique
      -- session/key constraint and the genuine carried decision is
      -- incorrectly superseded.
      if exists (
        select 1
        from public.banking_pay_workbench_case_resolution_carry_registrations
          as pending_carry
        where pending_carry.target_session_id=p_session_id
          and pending_carry.candidate_id=p_candidate_id
          and pending_carry.status='PENDING'
          and pending_carry.resolution_scope_kind='CORRECTION_COMPONENT'
          and pending_carry.canonical_resolution_key=
            v_component->>'canonical_correction_key'
      ) then
        continue;
      end if;

      -- A correction chain can contain a financially material component that
      -- has no standalone workbench preview row (for example, the historical
      -- carrier day of a paired reversal).  "Resolve linked work" must cover
      -- that component too; otherwise the preview can look complete while
      -- draft seeding correctly rejects the incomplete chain.  Clone only a
      -- decision from the same fingerprinted source family, then bind the new
      -- row to this component's own current source basis below.
      select resolution_row.*
      into v_resolution
      from public.banking_pay_workbench_session_case_resolutions as resolution_row
      where resolution_row.session_id=p_session_id
        and resolution_row.candidate_id=p_candidate_id
        and resolution_row.resolution_family='BUCKETED'
        and resolution_row.timesheet_id=any(v_member_ids)
        and resolution_row.source_family_key=v_residual->>'source_family_key'
      order by resolution_row.updated_at_utc desc,
               resolution_row.created_at_utc desc,
               resolution_row.id desc
      limit 1
      for update;

      if v_resolution.id is null then
        raise exception 'CORRECTION_CHAIN_RESOLUTION_ROW_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object(
                  'session_id',p_session_id,
                  'candidate_id',p_candidate_id,
                  'source_family_key',v_residual->>'source_family_key',
                  'component_key_type',v_component->>'component_key_type',
                  'component_key_value',v_component->>'component_key_value'
                )::text;
      end if;

      v_component_timesheet_id:=nullif(
        btrim(coalesce(v_component->>'carrier_timesheet_id','')),
        ''
      )::uuid;
      if v_component_timesheet_id is null
         or v_component_timesheet_id<>all(v_member_ids) then
        raise exception 'CORRECTION_CHAIN_RESOLUTION_CARRIER_ID_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object(
                  'session_id',p_session_id,
                  'candidate_id',p_candidate_id,
                  'source_family_key',v_residual->>'source_family_key',
                  'component_key_type',v_component->>'component_key_type',
                  'component_key_value',v_component->>'component_key_value'
                )::text;
      end if;

      insert into public.banking_pay_workbench_session_case_resolutions (
        session_id,
        candidate_id,
        case_key,
        resolution_family,
        resolution_identity_key,
        timesheet_id,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        payload_json,
        created_at_utc,
        updated_at_utc
      )
      values (
        p_session_id,
        p_candidate_id,
        'timesheet:'||v_component_timesheet_id::text,
        'BUCKETED',
        v_component->>'canonical_correction_key',
        v_component_timesheet_id,
        v_component->>'source_basis_fingerprint',
        v_residual->>'source_family_key',
        v_resolution.bucket_code,
        upper(v_component->>'component_key_type'),
        v_component->>'component_key_value',
        coalesce(v_resolution.payload_json,'{}'::jsonb)
          ||jsonb_build_object(
            'linked_timesheet_id',v_component_timesheet_id::text,
            'timesheet_id',v_component_timesheet_id::text,
            'case_key','timesheet:'||v_component_timesheet_id::text,
            'applied_via_linked_scope',true,
            'source_anchor_case_key',v_resolution.case_key
          ),
        now(),
        now()
      )
      on conflict (session_id,resolution_identity_key)
      do update
      set source_basis_fingerprint=excluded.source_basis_fingerprint,
          source_family_key=excluded.source_family_key,
          component_key_type=excluded.component_key_type,
          component_key_value=excluded.component_key_value,
          payload_json=excluded.payload_json,
          updated_at_utc=now()
      returning public.banking_pay_workbench_session_case_resolutions.*
      into v_resolution;
    end if;

    select canonical_resolution.id
    into v_canonical_resolution_id
    from public.banking_pay_workbench_session_case_resolutions
      as canonical_resolution
    where canonical_resolution.session_id=p_session_id
      and canonical_resolution.candidate_id=p_candidate_id
      and canonical_resolution.resolution_identity_key=
        v_component->>'canonical_correction_key'
    order by canonical_resolution.updated_at_utc desc,
             canonical_resolution.created_at_utc desc,
             canonical_resolution.id desc
    limit 1
    for update;
    v_component_timesheet_id:=coalesce(
      nullif(btrim(coalesce(v_component->>'carrier_timesheet_id','')),'')::uuid,
      case
        when btrim(coalesce(v_resolution.case_key,'')) ~*
             '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then substring(btrim(v_resolution.case_key) from 11)::uuid
        else v_resolution.timesheet_id
      end
    );
    if v_component_timesheet_id is null
       or v_component_timesheet_id<>all(v_member_ids) then
      raise exception 'CORRECTION_CHAIN_RESOLUTION_MEMBER_ID_REQUIRED'
        using errcode='P0001',
              detail=jsonb_build_object(
                'session_id',p_session_id,
                'candidate_id',p_candidate_id,
                'case_key',v_resolution.case_key
              )::text;
    end if;
    v_source_amount:=abs(round(
      (v_component->>'effective_source_outstanding_ex_vat')::numeric,2
    ));
    -- Preserve the exact saved physical decisions.  The correction residual
    -- is a dated aggregate, so it may consume those decisions only when every
    -- valid bucket independently projects the same two-decimal residual
    -- amount.  This proves that DAY/NIGHT/SAT/SUN/BH/additional-rate choice
    -- cannot change the aggregate result; no representative bucket is chosen.
    with physical_resolution as (
      select resolution_row.*,
             bucket_element.value as bucket_json,
             bucket_element.ordinality
      from public.banking_pay_workbench_session_case_resolutions resolution_row
      left join lateral jsonb_array_elements(
        case
          when jsonb_typeof(resolution_row.payload_json->'bucket_resolutions')='array'
            then resolution_row.payload_json->'bucket_resolutions'
          else '[]'::jsonb
        end
      ) with ordinality bucket_element(value,ordinality) on true
      where resolution_row.session_id=p_session_id
        and resolution_row.candidate_id=p_candidate_id
        and resolution_row.resolution_family='BUCKETED'
        and resolution_row.timesheet_id=any(v_member_ids)
        and upper(btrim(coalesce(resolution_row.component_key_type,'')))=
            upper(btrim(coalesce(v_component->>'component_key_type','')))
        and btrim(coalesce(resolution_row.component_key_value,''))=
            btrim(coalesce(v_component->>'component_key_value',''))
        and resolution_row.source_family_key is distinct from
            v_residual->>'source_family_key'
        and coalesce(resolution_row.payload_json->>'source_anchor_timesheet_id','')=
            p_anchor_timesheet_id::text
    ), validated as (
      select physical_resolution.*,
        case
          when jsonb_typeof(bucket_json)='object'
           and jsonb_array_length(payload_json->'bucket_resolutions')=1
           and coalesce(bucket_json->>'source_pay_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
           and abs((bucket_json->>'source_pay_ex_vat')::numeric)>0
           and coalesce(
                 bucket_json->>'target_pay_ex_vat',
                 bucket_json->>'target_amount_ex_vat',''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
           and upper(btrim(coalesce(bucket_json->>'target_pay_method','')))=
               upper(btrim(coalesce(v_residual->>'target_pay_method','')))
           and coalesce(bucket_json->>'timesheet_id','')=timesheet_id::text
           and coalesce(bucket_json->>'source_basis_fingerprint','')=
               coalesce(source_basis_fingerprint,'')
           and upper(btrim(coalesce(bucket_json->>'component_key_type','')))=
               upper(btrim(coalesce(v_component->>'component_key_type','')))
           and btrim(coalesce(bucket_json->>'component_key_value',''))=
               btrim(coalesce(v_component->>'component_key_value',''))
            then true else false
        end as valid_decision
      from physical_resolution
    )
    select count(*)::integer,
      count(*) filter(where not valid_decision)::integer,
      count(distinct case when valid_decision then round(
        v_source_amount*abs(coalesce(
          nullif(bucket_json->>'target_pay_ex_vat','')::numeric,
          nullif(bucket_json->>'target_amount_ex_vat','')::numeric
        ))/abs((bucket_json->>'source_pay_ex_vat')::numeric),2
      ) end)::integer,
      min(case when valid_decision then round(
        v_source_amount*abs(coalesce(
          nullif(bucket_json->>'target_pay_ex_vat','')::numeric,
          nullif(bucket_json->>'target_amount_ex_vat','')::numeric
        ))/abs((bucket_json->>'source_pay_ex_vat')::numeric),2
      ) end),
      coalesce(jsonb_agg(
        bucket_json||jsonb_build_object(
          'physical_resolution_identity_key',resolution_identity_key,
          'physical_source_basis_fingerprint',source_basis_fingerprint
        ) order by timesheet_id,coalesce(bucket_code,''),resolution_identity_key
      ) filter(where valid_decision),'[]'::jsonb),
      round(coalesce(sum(abs((bucket_json->>'source_pay_ex_vat')::numeric))
        filter(where valid_decision),0),2),
      round(coalesce(sum(abs(coalesce(
        nullif(bucket_json->>'target_pay_ex_vat','')::numeric,
        nullif(bucket_json->>'target_amount_ex_vat','')::numeric
      ))) filter(where valid_decision),0),2),
      coalesce((array_agg(payload_json order by timesheet_id,
        coalesce(bucket_code,''),resolution_identity_key)
        filter(where valid_decision))[1],'{}'::jsonb)
    into v_physical_decision_count,v_invalid_decision_count,
         v_projected_target_count,v_target_amount,v_bucket_resolutions,
         v_physical_source_total,v_physical_target_total,v_base_payload
    from validated;

    if v_physical_decision_count=0 then
      -- Carry replay and a correction component without a standalone preview
      -- row use the already-proven canonical family decision.  Preserve the
      -- existing coupled-chain rule, but validate every carried bucket.
      select coalesce(v_resolution.payload_json->'bucket_resolutions','[]'::jsonb),
             coalesce(v_resolution.payload_json,'{}'::jsonb)
      into v_bucket_resolutions,v_base_payload;

      select count(*)::integer,
        count(*) filter(where not (
          jsonb_typeof(bucket.value)='object'
          and coalesce(bucket.value->>'source_pay_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
          and abs((bucket.value->>'source_pay_ex_vat')::numeric)>0
          and coalesce(bucket.value->>'target_pay_ex_vat',
                bucket.value->>'target_amount_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
          and upper(btrim(coalesce(bucket.value->>'target_pay_method','')))=
              upper(btrim(coalesce(v_residual->>'target_pay_method','')))
        ))::integer,
        count(distinct case when
          coalesce(bucket.value->>'source_pay_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
          and abs((bucket.value->>'source_pay_ex_vat')::numeric)>0
          and coalesce(bucket.value->>'target_pay_ex_vat',
                bucket.value->>'target_amount_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
        then round(v_source_amount*abs(coalesce(
          nullif(bucket.value->>'target_pay_ex_vat','')::numeric,
          nullif(bucket.value->>'target_amount_ex_vat','')::numeric
        ))/abs((bucket.value->>'source_pay_ex_vat')::numeric),2) end)::integer,
        min(case when
          coalesce(bucket.value->>'source_pay_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
          and abs((bucket.value->>'source_pay_ex_vat')::numeric)>0
          and coalesce(bucket.value->>'target_pay_ex_vat',
                bucket.value->>'target_amount_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'
        then round(v_source_amount*abs(coalesce(
          nullif(bucket.value->>'target_pay_ex_vat','')::numeric,
          nullif(bucket.value->>'target_amount_ex_vat','')::numeric
        ))/abs((bucket.value->>'source_pay_ex_vat')::numeric),2) end),
        round(coalesce(sum(abs((bucket.value->>'source_pay_ex_vat')::numeric))
          filter(where coalesce(bucket.value->>'source_pay_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'),0),2),
        round(coalesce(sum(abs(coalesce(
          nullif(bucket.value->>'target_pay_ex_vat','')::numeric,
          nullif(bucket.value->>'target_amount_ex_vat','')::numeric
        ))) filter(where coalesce(bucket.value->>'target_pay_ex_vat',
          bucket.value->>'target_amount_ex_vat','') ~ '^-?[0-9]+([.][0-9]+)?$'),0),2)
      into v_physical_decision_count,v_invalid_decision_count,
           v_projected_target_count,v_target_amount,
           v_physical_source_total,v_physical_target_total
      from jsonb_array_elements(v_bucket_resolutions) bucket(value);
    end if;

    if v_physical_decision_count=0 or v_invalid_decision_count>0 then
      raise exception 'CORRECTION_CHAIN_RESOLUTION_BUCKET_SET_INVALID'
        using errcode='P0001',detail=jsonb_build_object(
          'session_id',p_session_id,'candidate_id',p_candidate_id,
          'component_key_type',v_component->>'component_key_type',
          'component_key_value',v_component->>'component_key_value',
          'decision_count',v_physical_decision_count,
          'invalid_decision_count',v_invalid_decision_count
        )::text;
    end if;
    if v_projected_target_count<>1 or v_target_amount is null then
      raise exception 'CORRECTION_CHAIN_RESOLUTION_PROJECTION_CONFLICT'
        using errcode='P0001',detail=jsonb_build_object(
          'session_id',p_session_id,'candidate_id',p_candidate_id,
          'component_key_type',v_component->>'component_key_type',
          'component_key_value',v_component->>'component_key_value',
          'decision_count',v_physical_decision_count,
          'distinct_projected_target_count',v_projected_target_count
        )::text;
    end if;

    v_bucket_set_digest:=encode(extensions.digest(
      convert_to(v_bucket_resolutions::text,'UTF8'),'sha256'),'hex');

    -- Never repurpose one physical decision as the correction-chain row.  A
    -- separate canonical aggregate lets the preview continue matching every
    -- original timesheet/rate fingerprint exactly.
    if v_canonical_resolution_id is null then
      insert into public.banking_pay_workbench_session_case_resolutions(
        session_id,candidate_id,case_key,resolution_family,
        resolution_identity_key,timesheet_id,source_basis_fingerprint,
        source_family_key,bucket_code,component_key_type,
        component_key_value,payload_json,resolution_origin_session_id,
        resolution_origin_pay_date,
        resolution_origin_source_basis_fingerprint,
        created_at_utc,updated_at_utc
      ) values (
        p_session_id,p_candidate_id,
        'timesheet:'||v_component_timesheet_id::text,'BUCKETED',
        v_component->>'canonical_correction_key',v_component_timesheet_id,
        v_component->>'source_basis_fingerprint',
        v_residual->>'source_family_key',null,
        upper(v_component->>'component_key_type'),
        v_component->>'component_key_value',v_base_payload,
        v_resolution.resolution_origin_session_id,
        v_resolution.resolution_origin_pay_date,
        v_resolution.resolution_origin_source_basis_fingerprint,
        now(),now()
      )
      on conflict(session_id,resolution_identity_key) do update
      set updated_at_utc=excluded.updated_at_utc
      returning id into v_normalised_resolution_id;
    else
      v_normalised_resolution_id:=v_canonical_resolution_id;
    end if;

    -- A previously frozen batch may have been created from an older decision
    -- carrying this same stable canonical identity.  The frozen batch retains
    -- its own immutable payload, while the open workbench session must reuse
    -- the one canonical pre-draft row for the component.  Updating that row
    -- avoids a unique-key collision and prevents a second active decision for
    -- the same economic component.
    update public.banking_pay_workbench_session_case_resolutions
    set timesheet_id=v_component_timesheet_id,
        resolution_identity_key=
          v_component->>'canonical_correction_key',
        source_basis_fingerprint=v_component->>'source_basis_fingerprint',
        source_family_key=v_residual->>'source_family_key',
        component_key_type=upper(v_component->>'component_key_type'),
        component_key_value=v_component->>'component_key_value',
        payload_json=coalesce(v_base_payload,'{}'::jsonb)
          ||jsonb_build_object(
            'linked_timesheet_id',v_component_timesheet_id::text,
            'timesheet_id',v_component_timesheet_id::text,
            'source_family_key',v_residual->>'source_family_key',
            'resolution_identity_key',
              v_component->>'canonical_correction_key',
            'resolution_identity_version','CORRECTION_CHAIN_V1',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint',
            'correction_root_id',v_residual->>'root_timesheet_id',
            'ordered_member_timesheet_ids',
              v_residual->'ordered_member_timesheet_ids',
            'component_lineage_fingerprint',
              v_component->>'component_lineage_fingerprint',
            -- Keep the canonical component result available both at the
            -- resolution row boundary and in its detailed bucket.  The
            -- correction residual reader accepts both shapes so decisions
            -- saved before this repeatable was installed remain valid.
            'target_pay_method',v_residual->>'target_pay_method',
            'target_amount_ex_vat',v_target_amount,
            'target_pay_ex_vat',v_target_amount,
            'saved_resolution_result_json',
              jsonb_build_object(
                'source_pay_ex_vat',v_source_amount,
                'target_amount_ex_vat',v_target_amount,
                'target_pay_ex_vat',v_target_amount
              ),
            'correction_resolution_aggregate_version',
              'CORRECTION_CHAIN_BUCKET_SET_V1',
            'physical_decision_count',v_physical_decision_count,
            'physical_decision_set_digest',v_bucket_set_digest,
            'physical_source_total_ex_vat',v_physical_source_total,
            'physical_target_total_ex_vat',v_physical_target_total,
            'distinct_projected_target_count',v_projected_target_count,
            'correction_chain_fingerprint',v_residual->>'chain_fingerprint',
            'correction_chain_residual_fingerprint',
              v_residual->>'residual_fingerprint',
            'correction_financials_policy_envelope',
              v_residual->'correction_financials_policy_envelope',
            'correction_financials_policy_envelope_fingerprint',
              v_residual->>'correction_financials_policy_envelope_fingerprint',
            'bucket_resolutions',v_bucket_resolutions
          ),
        updated_at_utc=now()
    where id=v_normalised_resolution_id;

    -- Physical decisions are the Workbench overlay authority and must remain
    -- available at their exact timesheet/rate grain.
    v_alias_deleted_this_component:=0;
    v_alias_deleted_count:=
      v_alias_deleted_count+v_alias_deleted_this_component;
    v_normalised_resolution_ids:=
      v_normalised_resolution_ids
      ||jsonb_build_array(v_normalised_resolution_id::text);
    v_normalised_resolution_identity_keys:=
      v_normalised_resolution_identity_keys
      ||jsonb_build_array(v_component->>'canonical_correction_key');
    v_updated:=v_updated+1;
  end loop;

  return jsonb_build_object(
    'ok',true,
    'normalised_count',v_updated,
    'alias_deleted_count',v_alias_deleted_count,
    'case_resolution_id',
      case
        when jsonb_array_length(v_normalised_resolution_ids)>0
          then v_normalised_resolution_ids->>0
        else null
      end,
    'case_resolution_ids',v_normalised_resolution_ids,
    'resolution_identity_keys',v_normalised_resolution_identity_keys,
    'source_family_key',v_residual->>'source_family_key',
    'root_timesheet_id',v_residual->>'root_timesheet_id'
  );
end;
$function$;

-- _ctms_payload_timesheet_ids_v1(jsonb,integer)
CREATE OR REPLACE FUNCTION public._ctms_payload_timesheet_ids_v1(p_payload jsonb, p_max_members integer DEFAULT 100)
 RETURNS uuid[]
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_ids uuid[];
begin
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'CORRECTION_PAYLOAD_TIMESHEET_LIMIT_INVALID' using errcode = '22023';
  end if;
  select coalesce(array_agg(id order by id), array[]::uuid[])
  into v_ids
  from (
    select distinct candidate.id
    from (
      select value::uuid as id
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(coalesce(p_payload, '{}'::jsonb) -> 'timesheet_ids') = 'array'
            then coalesce(p_payload, '{}'::jsonb) -> 'timesheet_ids'
          else '[]'::jsonb
        end
      ) value
      where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

      union

      select value::uuid as id
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(coalesce(p_payload, '{}'::jsonb) -> 'timesheetIds') = 'array'
            then coalesce(p_payload, '{}'::jsonb) -> 'timesheetIds'
          else '[]'::jsonb
        end
      ) value
      where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

      union

      select match[1]::uuid as id
      from regexp_matches(
        coalesce(p_payload, '{}'::jsonb)::text,
        '"(?:timesheet_id|timesheetId|current_timesheet_id|currentTimesheetId|requested_timesheet_id|requestedTimesheetId)"[[:space:]]*:[[:space:]]*"([0-9a-fA-F-]{36})"',
        'g'
      ) match
      where match[1] ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) candidate
    limit p_max_members + 1
  ) bounded;
  if cardinality(v_ids) > p_max_members then
    raise exception 'CORRECTION_PAYLOAD_TIMESHEET_LIMIT_EXCEEDED' using errcode = '22023';
  end if;
  return v_ids;
end;
$function$;

-- _ctms_rewrite_source_build_correction_negative_components_v1(uuid,uuid,uuid[])
CREATE OR REPLACE FUNCTION public._ctms_rewrite_source_build_correction_negative_components_v1(p_session_id uuid, p_candidate_id uuid, p_scope_timesheet_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_rewritten_chain_count integer := 0;
  v_inserted_component_count integer := 0;
begin
  if p_session_id is null
     or p_candidate_id is null
     or to_regclass('pg_temp._tmp_pay_wb_sync_negative_components') is null
     or to_regclass('pg_temp._tmp_pay_wb_sync_rotation_scope') is null then
    return jsonb_build_object(
      'ok', true,
      'rewritten_chain_count', 0,
      'inserted_component_count', 0
    );
  end if;

  for v_residual in
    select value
    from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      null::uuid,
      'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD'
    ))
  loop
    select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
    into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

    if coalesce(array_length(v_member_ids, 1), 0) = 0
       or (
         coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids)
    ) then
      continue;
    end if;

    -- A pay-method mismatch remains visible through the Workbench resolution
    -- surface, but its raw member rows must not remain in the authoritative
    -- negative-component set.  They would otherwise manufacture recovery
    -- authority from an unresolved target amount and fail the metadata gate.
    if coalesce((v_residual->>'draftable')::boolean, false) is not true then
      delete from pg_temp._tmp_pay_wb_sync_negative_components negative_component
      where negative_component.timesheet_id = any(v_member_ids);
      continue;
    end if;

    v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
    if v_root_id is null then
      raise exception 'CORRECTION_CHAIN_SOURCE_BUILD_ROOT_REQUIRED'
        using errcode='P0001', detail=v_residual::text;
    end if;
    if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
       and not (v_root_id = any(p_scope_timesheet_ids)) then
      raise exception 'CORRECTION_CHAIN_SOURCE_BUILD_SCOPE_MUST_INCLUDE_ROOT'
        using errcode='P0001',
              detail=jsonb_build_object(
                'root_timesheet_id', v_root_id,
                'member_timesheet_ids', to_jsonb(v_member_ids)
              )::text;
    end if;

    delete from pg_temp._tmp_pay_wb_sync_negative_components negative_component
    where negative_component.timesheet_id = any(v_member_ids);

    if not exists (
      select 1
      from pg_temp._tmp_pay_wb_sync_rotation_scope rotation_scope
      where rotation_scope.requested_timesheet_id = v_root_id
    ) then
      insert into pg_temp._tmp_pay_wb_sync_rotation_scope (
        requested_timesheet_id,
        canonical_timesheet_id,
        family_timesheet_id
      )
      values (v_root_id, v_root_id, v_root_id);
    end if;

    for v_component in
      select value
      from jsonb_array_elements(v_residual->'components')
      where round(
        coalesce(nullif(value->>'target_outstanding_ex_vat', '')::numeric, 0),
        2
      ) < 0
    loop
      insert into pg_temp._tmp_pay_wb_sync_negative_components (
        timesheet_id,
        key_type,
        key_value,
        truth_ex_vat,
        baseline_ex_vat,
        reserved_ex_vat,
        outstanding_ex_vat,
        baseline_signature
      )
      values (
        v_root_id,
        upper(btrim(v_component->>'component_key_type')),
        btrim(v_component->>'component_key_value'),
        round(coalesce(nullif(v_component->>'truth_ex_vat', '')::numeric, 0), 2),
        round(coalesce(nullif(v_component->>'baseline_ex_vat', '')::numeric, 0), 2),
        0,
        round(
          coalesce(nullif(v_component->>'target_outstanding_ex_vat', '')::numeric, 0),
          2
        ),
        v_residual->>'residual_fingerprint'
      );
      v_inserted_component_count := v_inserted_component_count + 1;
    end loop;

    v_rewritten_chain_count := v_rewritten_chain_count + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_chain_count,
    'inserted_component_count', v_inserted_component_count
  );
end;
$function$;

-- _ctms_rewrite_sync_authoritative_correction_negative_components(uuid,uuid,uuid[])
CREATE OR REPLACE FUNCTION public._ctms_rewrite_sync_authoritative_correction_negative_components(p_session_id uuid, p_candidate_id uuid, p_scope_timesheet_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_rewritten_chain_count integer := 0;
  v_inserted_component_count integer := 0;
begin
  if p_session_id is null
     or p_candidate_id is null
     or to_regclass('pg_temp.tmp_sync_authoritative_negative_components') is null then
    return jsonb_build_object(
      'ok', true,
      'rewritten_chain_count', 0,
      'inserted_component_count', 0
    );
  end if;

  for v_residual in
    select value
    from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      null::uuid,
      'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW'
    ))
  loop
    select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
    into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

    if coalesce(array_length(v_member_ids, 1), 0) = 0
       or (
         coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids)
    ) then
      continue;
    end if;

    -- Mirror the source-build boundary.  A non-draftable pay-method mismatch
    -- must remain a resolution case, not a raw recovery component.
    if coalesce((v_residual->>'draftable')::boolean, false) is not true then
      delete from pg_temp.tmp_sync_authoritative_negative_components negative_component
      where negative_component.timesheet_id = any(v_member_ids);
      continue;
    end if;

    v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
    if v_root_id is null then
      raise exception 'CORRECTION_CHAIN_SYNC_ROOT_REQUIRED'
        using errcode='P0001', detail=v_residual::text;
    end if;
    if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
       and not (v_root_id = any(p_scope_timesheet_ids)) then
      raise exception 'CORRECTION_CHAIN_SYNC_SCOPE_MUST_INCLUDE_ROOT'
        using errcode='P0001',
              detail=jsonb_build_object(
                'root_timesheet_id', v_root_id,
                'member_timesheet_ids', to_jsonb(v_member_ids)
              )::text;
    end if;

    delete from pg_temp.tmp_sync_authoritative_negative_components negative_component
    where negative_component.timesheet_id = any(v_member_ids);

    for v_component in
      select value
      from jsonb_array_elements(v_residual->'components')
      where round(
        coalesce(nullif(value->>'target_outstanding_ex_vat', '')::numeric, 0),
        2
      ) < 0
    loop
      insert into pg_temp.tmp_sync_authoritative_negative_components (
        timesheet_id,
        key_type,
        key_value,
        truth_ex_vat,
        baseline_ex_vat,
        reserved_ex_vat,
        outstanding_ex_vat,
        baseline_signature
      )
      values (
        v_root_id,
        upper(btrim(v_component->>'component_key_type')),
        btrim(v_component->>'component_key_value'),
        round(coalesce(nullif(v_component->>'truth_ex_vat', '')::numeric, 0), 2),
        round(coalesce(nullif(v_component->>'baseline_ex_vat', '')::numeric, 0), 2),
        0,
        round(
          coalesce(nullif(v_component->>'target_outstanding_ex_vat', '')::numeric, 0),
          2
        ),
        v_residual->>'residual_fingerprint'
      );
      v_inserted_component_count := v_inserted_component_count + 1;
    end loop;

    v_rewritten_chain_count := v_rewritten_chain_count + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_chain_count,
    'inserted_component_count', v_inserted_component_count
  );
end;
$function$;

-- _ctms_rewrite_sync_correction_cases_v1(uuid,uuid[],uuid[])
CREATE OR REPLACE FUNCTION public._ctms_rewrite_sync_correction_cases_v1(p_session_id uuid, p_candidate_ids uuid[], p_scope_timesheet_ids uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_candidate_id uuid;
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_template record;
  v_negative_amount numeric(12,2);
  v_source_original_paid numeric(12,2);
  v_source_corrected_paid numeric(12,2);
  v_linked_shift_date date;
  v_components_json jsonb;
  v_rewritten_count integer := 0;
  v_resolution_pending_count integer := 0;
  v_resolution_pending_member_ids uuid[] := array[]::uuid[];
begin
  if p_session_id is null
     or coalesce(array_length(p_candidate_ids, 1), 0) = 0
     or to_regclass('pg_temp.tmp_sync_timesheet_case_candidates') is null then
    return jsonb_build_object('ok', true, 'rewritten_chain_count', 0);
  end if;

  foreach v_candidate_id in array p_candidate_ids loop
    for v_residual in
      select value
      from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
        p_session_id,
        v_candidate_id,
        null::uuid,
        'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW'
      ))
    loop
      select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
      into v_member_ids
      from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

      if coalesce(array_length(v_member_ids, 1), 0) = 0 then
        continue;
      end if;
      if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids) then
        continue;
      end if;

      v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
      if v_root_id is null then
        raise exception 'CORRECTION_CHAIN_SYNC_ROOT_REQUIRED'
          using errcode='P0001', detail=v_residual::text;
      end if;

      if exists (
        select 1
        from public.pay_advances finance_case
        join public.pay_advance_reservations reservation
          on reservation.finance_case_id = finance_case.id
        left join public.pay_batch_items batch_item
          on batch_item.id = reservation.pay_batch_item_id
        where finance_case.candidate_id = v_candidate_id
          and finance_case.linked_timesheet_id = any(v_member_ids)
          and upper(btrim(coalesce(reservation.status, ''))) in ('RESERVED', 'COMMITTED')
          and reservation.released_at_utc is null
          and (batch_item.id is null or coalesce(batch_item.is_voided, false) is not true)
      ) then
        if exists (
          select 1
          from public.pay_advances finance_case
          join public.pay_advance_reservations reservation
            on reservation.finance_case_id = finance_case.id
          left join public.pay_batch_items batch_item
            on batch_item.id = reservation.pay_batch_item_id
          where finance_case.candidate_id = v_candidate_id
            and finance_case.linked_timesheet_id = any(v_member_ids)
            and upper(btrim(coalesce(reservation.status, ''))) in ('RESERVED', 'COMMITTED')
            and reservation.released_at_utc is null
            and (batch_item.id is null or coalesce(batch_item.is_voided, false) is not true)
            and not exists (
              select 1
              from public.pay_batch_candidates active_batch_candidate
              join public.pay_batches active_batch
                on active_batch.id = active_batch_candidate.pay_batch_id
              where active_batch_candidate.id = batch_item.pay_batch_candidate_id
                and active_batch_candidate.candidate_id = v_candidate_id
                and active_batch.cancelled_at_utc is null
                and upper(btrim(coalesce(active_batch.status, ''))) in (
                  'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
                  'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
                  'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
                )
                and coalesce(
                  batch_item.frozen_component_snapshot_json->>'correction_root_id',
                  batch_item.frozen_resolution_payload_json->>'correction_root_id',
                  ''
                ) = v_root_id::text
            )
        ) then
          raise exception 'CORRECTION_CHAIN_ACTIVE_FINANCE_RESERVATION'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'candidate_id', v_candidate_id::text,
                    'root_timesheet_id', v_root_id::text,
                    'message', 'A correction-chain reservation is not safely covered by its active frozen Banking Pay batch.'
                  )::text;
        end if;

        -- This correction root is already frozen in an active batch.  Remove
        -- its live members from the pre-draft finance-sync workspace so the
        -- refresh cannot recreate, amend or clear the frozen authority.
        delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
        where candidate_row.candidate_id = v_candidate_id
          and candidate_row.timesheet_id = any(v_member_ids);

        continue;
      end if;

      if coalesce((v_residual->>'draftable')::boolean,false) is not true then
        if coalesce(v_residual->>'block_code','')
             = 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
           and coalesce((v_residual->>'unresolved_count')::integer,0) > 0
           and coalesce((v_residual->>'reservation_overrun_count')::integer,0) = 0
           and coalesce((v_residual->>'component_count')::integer,0) > 0 then
          -- The unresolved chain must remain visible to the Workbench's
          -- existing case-resolution projection, but it must not create,
          -- amend or clear finance authority until the saved resolution is
          -- fresh.  Suppress every member as one coupled economic unit.
          delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
          where candidate_row.candidate_id = v_candidate_id
            and candidate_row.timesheet_id = any(v_member_ids);

          select coalesce(array_agg(distinct member_id order by member_id),array[]::uuid[])
          into v_resolution_pending_member_ids
          from unnest(v_resolution_pending_member_ids || v_member_ids) member_id;
          v_resolution_pending_count := v_resolution_pending_count + 1;
          continue;
        end if;

        raise exception 'CORRECTION_RESIDUAL_NOT_READY_FOR_OVERPAYMENT_SYNC'
          using errcode='P0001',detail=v_residual::text;
      end if;

      select candidate_row.*
      into v_template
      from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
      where candidate_row.candidate_id = v_candidate_id
        and candidate_row.timesheet_id = any(v_member_ids)
      order by
        case
          when candidate_row.desired_case_type =
               'OVERPAYMENT'::public.pay_finance_case_type_enum then 0
          else 1
        end,
        case when candidate_row.timesheet_id = v_root_id then 0 else 1 end,
        candidate_row.timesheet_id
      limit 1;

      select
        round(coalesce(sum(abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric)), 0), 2),
        round(coalesce(sum(coalesce(nullif(component->>'baseline_ex_vat', '')::numeric, 0)), 0), 2),
        round(coalesce(sum(coalesce(nullif(component->>'truth_ex_vat', '')::numeric, 0)), 0), 2),
        min(case
          when upper(coalesce(component->>'component_key_type', '')) = 'TS_DAY'
           and coalesce(component->>'component_key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
          then (component->>'component_key_value')::date
          else null::date
        end),
        coalesce(jsonb_agg(
          jsonb_strip_nulls(jsonb_build_object(
            'candidate_id', v_candidate_id::text,
            'client_id', v_residual->>'client_id',
            'linked_timesheet_id', v_root_id::text,
            'source_family_key', v_residual->>'source_family_key',
            'component_key_type', component->>'component_key_type',
            'component_key_value', component->>'component_key_value',
            'classification', coalesce(component->>'classification', 'TAXABLE_CHANNEL_SENSITIVE'),
            -- The correction-chain resolution has already converted this
            -- amount onto the current target pay channel.  Finance sync must
            -- not offer a second PAYE/umbrella conversion for the same money.
            'source_pay_method', v_residual->>'target_pay_method',
            'current_target_pay_method', v_residual->>'target_pay_method',
            'source_amount', abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric),
            'remaining_source_amount', abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric),
            'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH',
            'source_basis_json', jsonb_strip_nulls(jsonb_build_object(
              'linked_timesheet_id', v_root_id::text,
              'source_family_key', v_residual->>'source_family_key',
              'component_key_type', component->>'component_key_type',
              'component_key_value', component->>'component_key_value',
              'classification', coalesce(component->>'classification', 'TAXABLE_CHANNEL_SENSITIVE'),
              'baseline_ex_vat', component->>'baseline_ex_vat',
              'truth_ex_vat', component->>'truth_ex_vat',
              'correction_chain_fingerprint', v_residual->>'chain_fingerprint',
              'correction_chain_residual_fingerprint', v_residual->>'residual_fingerprint',
              'upstream_correction_pay_method_resolution_applied', true,
              'correction_financials_policy_envelope', component->'correction_financials_policy_envelope',
              'correction_financials_policy_envelope_fingerprint', component->>'correction_financials_policy_envelope_fingerprint'
            )),
            'target_basis_json', jsonb_strip_nulls(jsonb_build_object(
              'current_target_pay_method', v_residual->>'target_pay_method',
              'correction_chain_fingerprint', v_residual->>'chain_fingerprint',
              'correction_chain_residual_fingerprint', v_residual->>'residual_fingerprint'
            ))
          ))
          order by component->>'component_key_type', component->>'component_key_value'
        ), '[]'::jsonb)
      into
        v_negative_amount,
        v_source_original_paid,
        v_source_corrected_paid,
        v_linked_shift_date,
        v_components_json
      from jsonb_array_elements(v_residual->'components') component
      where round(coalesce(nullif(component->>'target_outstanding_ex_vat', '')::numeric, 0), 2) < 0;

      delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
      where candidate_row.candidate_id = v_candidate_id
        and candidate_row.timesheet_id = any(v_member_ids);

      if coalesce(v_negative_amount, 0) > 0 then
        -- Correction members are deliberately suppressed once their coupled
        -- residual becomes the sole finance authority.  Therefore a raw
        -- member template may be absent here.  The residual already carries
        -- the authoritative client, target channel, amounts and component
        -- evidence needed to create the single coupled recovery candidate.
        insert into pg_temp.tmp_sync_timesheet_case_candidates (
          candidate_id,
          timesheet_id,
          client_id,
          linked_shift_date,
          corrected_amount_ex,
          baseline_signature,
          candidate_pay_method,
          case_is_blocked,
          needs_lifecycle_tracking,
          overpayment_amount_ex,
          underpayment_amount_ex,
          desired_case_type,
          desired_advance_kind,
          desired_reason,
          source_original_paid_amount,
          source_corrected_paid_amount,
          components_sync_json
        )
        values (
          v_candidate_id,
          v_root_id,
          coalesce(nullif(v_residual->>'client_id', '')::uuid, v_template.client_id),
          v_linked_shift_date,
          v_source_corrected_paid,
          v_residual->>'residual_fingerprint',
          coalesce(nullif(v_residual->>'target_pay_method', ''), v_template.candidate_pay_method),
          false,
          true,
          v_negative_amount,
          0,
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'OVERPAYMENT'::public.pay_advance_kind_enum,
          'OVERPAYMENT'::public.pay_advance_reason_enum,
          v_source_original_paid,
          v_source_corrected_paid,
          v_components_json
        );
      end if;

      v_rewritten_count := v_rewritten_count + 1;
    end loop;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_count,
    'resolution_pending_chain_count', v_resolution_pending_count,
    'resolution_pending_member_timesheet_ids',
      to_jsonb(v_resolution_pending_member_ids)
  );
end;
$function$;

-- _imp_debug_audit(uuid,text,jsonb,text,text,jsonb,text,text,text,text,text)
CREATE OR REPLACE FUNCTION public._imp_debug_audit(p_actor_user_id uuid, p_action text, p_after_json jsonb, p_entity text, p_subject_id text, p_before_json jsonb, p_ip text, p_user_agent text, p_correlation_id text, p_unused_1 text, p_unused_2 text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  -- Forward to the canonical 9-arg implementation (which applies invoice_debug gating)
  perform public._imp_debug_audit(
    p_actor_user_id,
    p_action,
    p_after_json,
    p_entity,
    p_subject_id,
    p_before_json,
    p_ip,
    p_user_agent,
    p_correlation_id
  );
end;
$function$;

-- _imp_debug_audit(uuid,text,jsonb,text,text,jsonb,text,text,text,text)
CREATE OR REPLACE FUNCTION public._imp_debug_audit(p_actor_user_id uuid, p_action text, p_after_json jsonb, p_entity text, p_subject_id text, p_before_json jsonb, p_ip text, p_user_agent text, p_correlation_id text, p_unused_1 text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  -- Forward to the canonical 9-arg implementation (which applies invoice_debug gating)
  perform public._imp_debug_audit(
    p_actor_user_id,
    p_action,
    p_after_json,
    p_entity,
    p_subject_id,
    p_before_json,
    p_ip,
    p_user_agent,
    p_correlation_id
  );
end;
$function$;

-- _imp_debug_audit(uuid,text,jsonb,text,text,jsonb,text,text,text)
CREATE OR REPLACE FUNCTION public._imp_debug_audit(p_actor_user_id uuid, p_action text, p_after_json jsonb, p_entity text, p_subject_id text, p_before_json jsonb DEFAULT NULL::jsonb, p_ip text DEFAULT NULL::text, p_user_agent text DEFAULT NULL::text, p_correlation_id text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_invoice_debug boolean := false;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  when others then
    v_invoice_debug := false;
  end;

  if not v_invoice_debug then
    return;
  end if;

  -- Never allow audit failures to break the caller
  begin
    perform public._inv_write_audit(
      p_actor_user_id,
      p_action,
      p_after_json,
      p_entity,
      p_subject_id,
      p_before_json,
      'INVOICE_DEBUG',
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  exception when others then
    null;
  end;
end;
$function$;

-- _import_apply_operation_claim_core_v2(uuid,uuid,hr_source_enum,text,text,uuid,jsonb)
CREATE OR REPLACE FUNCTION public._import_apply_operation_claim_core_v2(p_operation_id uuid, p_import_id uuid, p_source_system hr_source_enum, p_import_revision text, p_request_hash text, p_actor_user_id uuid, p_request_envelope jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_op public.import_apply_operations%rowtype;
  v_same public.import_apply_operations%rowtype;
  v_hash text;
  v_envelope jsonb:=coalesce(p_request_envelope,'{}');
  v_units jsonb;
  v_unit jsonb;
  v_policy jsonb;
  v_canonical_unit jsonb;
  v_canonical_units jsonb:='[]'::jsonb;
  v_preview_consequences jsonb:='[]'::jsonb;
  v_contract_payload jsonb;
  v_contract jsonb;
  v_contract_fingerprint text;
  v_response jsonb;
  v_now timestamptz:=statement_timestamp();
  v_action text;
  v_shape text;
  v_expected_roles jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_operation_id is null or p_import_id is null or p_source_system is null
     or length(btrim(coalesce(p_import_revision,''))) not between 1 and 512
     or jsonb_typeof(v_envelope)<>'object' or pg_column_size(v_envelope)>1048576 then
    raise exception 'IMPORT_OPERATION_V2_INPUT_INVALID' using errcode='22023';
  end if;
  if v_envelope->>'schema_version' is distinct from 'IMPORT_REVIEW_APPLY_V1'
     or nullif(v_envelope->>'import_id','')::uuid is distinct from p_import_id
     or jsonb_typeof(v_envelope->'selected_action_ids')<>'array'
     or jsonb_typeof(v_envelope->'reference_invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_OPERATION_V2_REVIEW_ENVELOPE_INVALID' using errcode='22023';
  end if;
  v_units:=coalesce(v_envelope->'correction_units','[]'::jsonb);
  if jsonb_typeof(v_units)<>'array' or jsonb_array_length(v_units)>500 then
    raise exception 'IMPORT_OPERATION_V2_CORRECTION_SCOPE_LIMIT' using errcode='22023';
  end if;
  if exists(
    select 1 from jsonb_array_elements(v_units) u
    where jsonb_typeof(u)<>'object'
       or nullif(u->>'root_timesheet_id','') is null
       or (u->>'root_timesheet_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or nullif(btrim(u->>'source_row_key'),'') is null
       or upper(btrim(coalesce(u->>'correction_action',''))) not in ('CHANGED_HOURS','CANCELLATION')
       or upper(btrim(coalesce(u->>'correction_shape',''))) not in ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
       or (upper(btrim(u->>'correction_action'))='CANCELLATION' and upper(btrim(u->>'correction_shape'))<>'REVERSAL_ONLY')
       or (upper(btrim(u->>'correction_action'))='CHANGED_HOURS' and upper(btrim(u->>'correction_shape'))<>'REVERSAL_REPLACEMENT')
  ) then raise exception 'IMPORT_OPERATION_V2_CORRECTION_UNIT_INVALID' using errcode='22023'; end if;
  if (select count(*) from jsonb_array_elements(v_units))<>(
    select count(distinct concat_ws('|',u->>'root_timesheet_id',u->>'source_row_key',upper(u->>'correction_action'),upper(u->>'correction_shape')))
    from jsonb_array_elements(v_units) u
  ) then raise exception 'IMPORT_OPERATION_V2_CORRECTION_UNIT_DUPLICATE' using errcode='22023'; end if;

  v_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(coalesce(p_request_hash,'')))<>v_hash then
    raise exception 'IMPORT_OPERATION_V2_REQUEST_HASH_MISMATCH' using errcode='22023',
      detail=jsonb_build_object('server_request_hash',v_hash)::text;
  end if;
  if not exists(select 1 from public.hr_imports i where i.id=p_import_id and i.source_system=p_source_system) then
    raise exception 'IMPORT_OPERATION_V2_IMPORT_OR_SOURCE_MISMATCH' using errcode='P0002';
  end if;

  perform pg_advisory_xact_lock(hashtextextended('IMPORT_OPERATION_V2|'||p_operation_id::text,21072026));
  select * into v_op from public.import_apply_operations where id=p_operation_id for update;
  if found then
    if v_op.import_id<>p_import_id or v_op.source_system<>p_source_system
       or v_op.import_revision<>btrim(p_import_revision) or v_op.request_hash<>v_hash
       or v_op.actor_user_id<>p_actor_user_id
       or v_op.response_json->'request_envelope' is distinct from v_envelope then
      raise exception 'IMPORT_OPERATION_V2_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'replay',true,'operation_id',v_op.id,'state',v_op.state,
      'request_hash',v_op.request_hash,'source_committed',v_op.committed_at_utc is not null,'response_json',v_op.response_json);
  end if;
  select * into v_same from public.import_apply_operations
  where import_id=p_import_id and import_revision=btrim(p_import_revision) and request_hash=v_hash for update;
  if found then
    raise exception 'IMPORT_OPERATION_V2_OPERATION_ID_MISMATCH_FOR_EXISTING_REQUEST' using errcode='23505',
      detail=jsonb_build_object('existing_operation_id',v_same.id)::text;
  end if;

  for v_unit in select value from jsonb_array_elements(v_units) loop
    v_action:=upper(btrim(v_unit->>'correction_action'));
    v_shape:=upper(btrim(v_unit->>'correction_shape'));
    v_expected_roles:=case when v_shape='REVERSAL_ONLY' then jsonb_build_array('REVERSAL')
      else jsonb_build_array('REVERSAL','REPLACEMENT') end;
    v_policy:=public._ctms_correction_financials_policy_build_v2(
      (v_unit->>'root_timesheet_id')::uuid,p_import_id,btrim(v_unit->>'source_row_key'),v_action,v_shape,
      p_operation_id,v_hash,v_now,true,32);
    v_canonical_unit:=jsonb_build_object(
      'action_id',v_unit->>'action_id','root_timesheet_id',v_policy->>'root_timesheet_id',
      'correction_chain_id',v_policy->>'correction_chain_id','source_shift_id',v_policy#>>'{classification,source_shift_id}',
      'source_row_key',btrim(v_unit->>'source_row_key'),'correction_action',v_action,'correction_shape',v_shape,
      'expected_member_roles',v_expected_roles,'expected_member_count',jsonb_array_length(v_expected_roles),
      'settings_snapshot',v_policy->'settings_snapshot','policy_envelope_fingerprint',v_policy->>'envelope_fingerprint',
      'policy_envelope',v_policy);
    v_canonical_units:=v_canonical_units||jsonb_build_array(v_canonical_unit);
  end loop;

  if jsonb_array_length(v_canonical_units)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'action_id',u->>'action_id','root_timesheet_id',u->>'root_timesheet_id','source_row_key',u->>'source_row_key',
      'correction_action',u->>'correction_action','correction_shape',u->>'correction_shape',
      'expected_member_roles',u->'expected_member_roles','expected_member_count',u->'expected_member_count',
      'reversal',jsonb_build_object('applicable',coalesce((u#>>'{policy_envelope,reversal,applicable}')::boolean,false),
        'setting',u#>>'{policy_envelope,reversal,setting}','setting_source',u#>>'{policy_envelope,reversal,setting_source}',
        'tsfin_policy',u#>'{policy_envelope,reversal,tsfin_policy}','invoice_policy',u#>'{policy_envelope,reversal,invoice_policy}',
        'leg_fingerprint',u#>>'{policy_envelope,reversal,leg_fingerprint}'),
      'replacement',jsonb_build_object('applicable',coalesce((u#>>'{policy_envelope,replacement,applicable}')::boolean,false),
        'setting',u#>>'{policy_envelope,replacement,setting}','setting_source',u#>>'{policy_envelope,replacement,setting_source}',
        'tsfin_policy',u#>'{policy_envelope,replacement,tsfin_policy}','invoice_policy',u#>'{policy_envelope,replacement,invoice_policy}',
        'leg_fingerprint',u#>>'{policy_envelope,replacement,leg_fingerprint}'),
      'policy_envelope_fingerprint',u->>'policy_envelope_fingerprint') order by u->>'action_id'),'[]'::jsonb)
    into v_preview_consequences from jsonb_array_elements(v_canonical_units) u;
    v_contract_payload:=jsonb_build_object(
      'schema_version','IMPORT_CORRECTION_OPERATION_V2','route_family','IMPORT_AUTHORITATIVE',
      'operation_id',p_operation_id,'import_id',p_import_id,'source_system',p_source_system,
      'import_revision',btrim(p_import_revision),'request_hash',v_hash,'request_seed_fingerprint',v_hash,
      'operation_at_utc',v_now,'operation_date_london',(v_now at time zone 'Europe/London')::date,
      'requested_timesheet_ids',(select coalesce(jsonb_agg(distinct u->>'root_timesheet_id'),'[]'::jsonb) from jsonb_array_elements(v_canonical_units) u),
      'expanded_timesheet_ids',(select coalesce(jsonb_agg(distinct u->>'root_timesheet_id'),'[]'::jsonb) from jsonb_array_elements(v_canonical_units) u),
      'correction_units',v_canonical_units);
    v_contract_fingerprint:=encode(extensions.digest(convert_to(v_contract_payload::text,'UTF8'),'sha256'),'hex');
    v_contract:=v_contract_payload||jsonb_build_object('operation_contract_fingerprint',v_contract_fingerprint);
  end if;

  v_response:=jsonb_build_object('schema_version','IMPORT_APPLY_OPERATION_V2','request_envelope',v_envelope,'server_request_hash',v_hash);
  if v_contract is not null then
    v_response:=v_response||jsonb_build_object('correction_operation_at_utc',v_now,
      'correction_operation_date_london',(v_now at time zone 'Europe/London')::date,
      'preview_consequences',v_preview_consequences,'correction_operation_contract',v_contract);
  end if;
  if pg_column_size(v_response)>4194304 then raise exception 'IMPORT_OPERATION_V2_RESPONSE_TOO_LARGE' using errcode='54000'; end if;
  insert into public.import_apply_operations(id,import_id,source_system,import_revision,request_hash,actor_user_id,state,response_json)
  values(p_operation_id,p_import_id,p_source_system,btrim(p_import_revision),v_hash,p_actor_user_id,'PREPARED',v_response)
  returning * into v_op;
  return jsonb_build_object('ok',true,'replay',false,'operation_id',v_op.id,'state',v_op.state,'request_hash',v_op.request_hash,
    'source_committed',false,'response_json',v_op.response_json);
end $function$;

-- _import_review_action_catalog_core_v1(uuid,integer,integer)
CREATE OR REPLACE FUNCTION public._import_review_action_catalog_core_v1(p_import_id uuid, p_preview_generation integer, p_max_actions integer DEFAULT 5000)
 RETURNS TABLE(action_id text, action_kind text, action_category text, target_key text, source_identity text, hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid, contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean, default_selected boolean, blocking boolean, summary_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>5000 then
    raise exception 'IMPORT_REVIEW_ACTION_CATALOG_INPUT_INVALID' using errcode='22023';
  end if;

  create temporary table if not exists pg_temp.import_review_catalog_v1 (
    action_id text, action_kind text, action_category text, target_key text, source_identity text,
    hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid,
    contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean,
    default_selected boolean, blocking boolean, summary_json jsonb
  ) on commit drop;
  truncate pg_temp.import_review_catalog_v1;

  insert into pg_temp.import_review_catalog_v1
  with import_row as (
    select hi.* from public.hr_imports hi where hi.id=p_import_id
  ), raw as (
    select r.*, i.source_system::text as source_system, upper(coalesce(i.import_scope,'')) as import_scope,
      i.client_id as import_client_id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,'')) as staff_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),'') as staff_key,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),nullif(r.unit_raw,''),nullif(r.unit_hint,'')) as client_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),'') as client_key,
      lower(btrim(coalesce(nullif(r.assignment_grade_norm,''),r.payload_json->>'grade_raw',r.payload_json->>'Request_Grade',''))) as grade_key,
      coalesce(nullif(r.external_row_key,''),'hr-row:'||r.id::text) as source_row_key
    from public.hr_rows r join import_row i on true where r.import_id=p_import_id
    order by r.id limit 501
  ), mapped as (
    select raw.*,
      coalesce(c_alias.id,c_map.candidate_id,c_exact.candidate_id) as resolved_candidate_id,
      coalesce(raw.import_client_id,ch.client_id,c_client.client_id) as resolved_client_id
    from raw
    left join lateral (
      select c.id from public.candidates c
      where c.nhsp_hr_name_aliases is not null and raw.staff_key is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[raw.staff_key]::text[])
      order by c.id limit 1
    ) c_alias on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(coalesce(raw.staff_label,''))),raw.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) c_map on c_alias.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as candidate_id
      from public.candidates c where c.active and raw.staff_key is not null
        and (regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=raw.staff_key
          or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=raw.staff_key)
    ) c_exact on c_alias.id is null and c_map.candidate_id is null
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where raw.client_key is not null and ch.hospital_name_norm @> to_jsonb(array[raw.client_key]::text[])
      order by ch.id limit 1
    ) ch on raw.import_client_id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as client_id
      from public.clients c where raw.client_key is not null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=raw.client_key
    ) c_client on raw.import_client_id is null and ch.client_id is null
  ), weekly_phase as materialized (
    -- weekly_import_phase2 remains the single authority for assignment-code
    -- mapping precedence and contract choice.  The review catalogue consumes
    -- its answer rather than maintaining a second resolver.
    select w.*
    from import_row i
    cross join lateral public.weekly_import_phase2(
      p_import_id,
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end
    ) w
    where not (upper(i.source_system::text)='HEALTHROSTER_DAILY'
      or upper(coalesce(i.import_scope,'')) like '%DAILY%')
  ), classified as (
    select m.*,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
        then case when rtsx.contract_id is not null then 1 else 0 end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wp.week_ending_date as resolved_week_ending_date,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
      dtsx.submitted_timesheet_count as daily_submitted_timesheet_count,
      dtsx.submitted_timesheet_evidence_hash as daily_submitted_timesheet_evidence_hash,
      tsx.timesheet_contract_ids,dcon.contract_ids as eligible_contract_ids,dcon.contract_evidence_hash,
      cr.route_eligible as contract_route_eligible,cr.rate_complete as contract_rate_complete,
      cr.import_authoritative,cr.authority_mode,cr.authority_fingerprint,
      cr.rate_evidence as contract_rate_evidence,
      wopts.options as weekly_contract_options,dopts.options as daily_role_options,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      nss.start_utc as existing_shift_start_utc,nss.end_utc as existing_shift_end_utc,
      nss.break_mins as existing_shift_break_minutes,nss.pay_minutes as existing_shift_paid_minutes,
      nss.assignment_code as existing_shift_role,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join weekly_phase wp on wp.hr_row_id=m.id
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
    ) con on true
    left join lateral (
      select count(*)::integer mapping_count,
        (array_agg(gm.id order by gm.updated_at desc,gm.id))[1] mapping_id,
        (array_agg(gm.role_code order by gm.updated_at desc,gm.id))[1] role_code,
        (array_agg(gm.band_norm order by gm.updated_at desc,gm.id))[1] band_norm,
        (array_agg(gm.updated_at order by gm.updated_at desc,gm.id))[1] updated_at
      from public.hr_daily_grade_role_mappings gm
      where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active
    ) dgm on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer contract_count,
        case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id,
        array_agg(c.id order by c.id) contract_ids,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',c.id,c.updated_at,c.role,c.band,a.authority_fingerprint),',' order by c.id),'')) contract_evidence_hash
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        and coalesce(dgm.mapping_count,0)=1 and a.route_eligible
        and lower(btrim(coalesce(c.role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(c.band,'')))=lower(btrim(dgm.band_norm)))
    ) dcon on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer submitted_timesheet_count,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,
          t.worked_end_iso,t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,
          t.tsfin_role,t.tsfin_band,ts.contract_id,ts.updated_at),',' order by t.timesheet_id),''))
          submitted_timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) dtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      with candidates as (
        select abm.*,
          case when abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id then 3
            when abm.candidate_id=m.resolved_candidate_id and abm.client_id is null then 2
            when abm.candidate_id is null and abm.client_id=m.resolved_client_id then 1 else 0 end specificity
        from public.assignment_band_mappings abm
        where abm.active and upper(btrim(abm.system_type))=
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end
          and lower(btrim(abm.incoming_code))=m.grade_key
          and ((abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id=m.resolved_candidate_id and abm.client_id is null)
            or (abm.candidate_id is null and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id is null and abm.client_id is null))
      ), chosen as (select * from candidates where specificity=(select max(specificity) from candidates))
      select exists(select 1 from chosen) has_weekly_mapping,
        public._import_review_hash_v1(coalesce((select string_agg(concat_ws('|',id,updated_at,target_contract_id,band_match_pattern),',' order by id)
          from chosen),'')) mapping_evidence
    ) wm on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             array_agg(ts.contract_id order by t.worked_start_iso,t.timesheet_id) timesheet_contract_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id,
             public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,t.worked_end_iso,
               t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,t.tsfin_role,t.tsfin_band,
               ts.contract_id,ts.updated_at),',' order by t.timesheet_id),'')) timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
        and coalesce(dgm.mapping_count,0)=1
        and lower(btrim(coalesce(t.tsfin_role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(t.tsfin_band,'')))=lower(btrim(dgm.band_norm)))
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join lateral (
      select ts.contract_id
      from public.timesheets ts
      where ts.timesheet_id=coalesce(
        case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id)
        and ts.is_current and ts.revoked_at is null
      order by ts.updated_at desc limit 1
    ) rtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select a.route_eligible,a.import_authoritative,a.authority_mode,a.authority_fingerprint,
        (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else
            (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete,
        public._import_review_hash_v1(concat_ws('|',c.id,c.updated_at,c.start_date,c.end_date,c.role,c.band,
          c.pay_method_snapshot,c.rates_json,c.overrideclientsettings,c.is_nhsp,c.autoprocess_hr,c.requires_hr,
          c.no_timesheet_required,a.client_settings_id,a.client_settings_updated_at,
          a.effective_is_nhsp,a.effective_autoprocess_hr,a.effective_requires_hr,
          a.effective_no_timesheet_required,a.authority_fingerprint)) rate_evidence
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1(
        case when upper(m.source_system)='NHSP' then 'NHSP'
          when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then 'HR_DAILY'
          else 'HR_WEEKLY' end,c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
      where c.id=case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        -- Choosing a contract records the server-approved assignment mapping;
        -- it does not apply the import or grant financial authority.  An
        -- authoritative contract with incomplete rates must therefore remain
        -- selectable here and will still be blocked by the refreshed action
        -- catalogue before final application.
        'selectable',coalesce(o.route_eligible,false),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE' end,
        'display_label',concat_ws(' · ',nullif(o.role,''),nullif(o.band,''),nullif(o.display_site,''),
          to_char(o.start_date,'DD Mon YYYY')||' to '||coalesce(to_char(o.end_date,'DD Mon YYYY'),'open ended'))
      ) order by lower(coalesce(o.role,'')),lower(coalesce(o.band,'')),o.start_date desc,o.id),'[]'::jsonb) options
      from (
        select c.*,a.route_eligible,a.import_authoritative,a.authority_mode,
          (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete
        from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1(
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end,
          c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        order by c.start_date desc,c.id limit 25
      ) o
    ) wopts on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','daily-role:'||public._import_review_hash_v1(lower(concat_ws('|',o.role,o.band))),
        'role_code',o.role,'band_norm',o.band,'selectable',true,
        'display_label',concat_ws(' · ',nullif(o.role,''),coalesce(nullif(o.band,''),'No band'))
      ) order by lower(o.role),lower(coalesce(o.band,''))),'[]'::jsonb) options
      from (
        select distinct t.tsfin_role role,t.tsfin_band band
        from public.v_timesheets_daily_match t
        where t.candidate_id=m.resolved_candidate_id
          and t.client_id=m.resolved_client_id
          and t.sheet_scope::text='DAILY'
          and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
          and nullif(btrim(t.tsfin_role),'') is not null
        order by t.tsfin_role,t.tsfin_band
        limit 25
      ) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id) as authoritative_target_timesheet_id,
      public._import_review_timesheet_has_calculated_expenses_core_v1(
        coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
      ) as authoritative_timesheet_has_calculated_expenses,
      mutable_replacement.timesheet_id as mutable_replacement_timesheet_id,
      mutable_replacement.protection as mutable_replacement_protection,
      source_timesheet.authorised_at_server as source_authorised_at_server,
      source_tf.authorised_at_utc as source_tsfin_authorised_at_utc,
      source_tf.policy_snapshot_json as source_policy_snapshot_json,
      source_tf.basis::text as source_tsfin_basis,
      authoritative_hours.hours_day as authoritative_hours_day,
      authoritative_hours.hours_night as authoritative_hours_night,
      authoritative_hours.hours_sat as authoritative_hours_sat,
      authoritative_hours.hours_sun as authoritative_hours_sun,
      authoritative_hours.hours_bh as authoritative_hours_bh,
      authoritative_hours.total_hours as authoritative_total_hours,
      coalesce((auto_authorise.value->>'effective_value')::boolean,false) as effective_auto_authorise,
      public._import_review_timesheet_protection_core_v1(coalesce(
        c.resolved_timesheet_id,c.existing_shift_timesheet_id,base_week.timesheet_id
      )) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
    left join lateral (
      select cw.timesheet_id
      from public.contract_weeks cw
      where not c.is_daily
        and coalesce(c.import_authoritative,false)
        and cw.contract_id=c.resolved_contract_id
        and cw.week_ending_date=coalesce(
          c.resolved_week_ending_date,
          c.date_local + ((7-extract(dow from c.date_local)::integer)%7)
        )
        and cw.is_adjustment=false
        and coalesce(cw.additional_seq,0)=0
      order by cw.id
      limit 1
    ) base_week on true
    left join public.timesheets source_timesheet
      on source_timesheet.timesheet_id=coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
    left join public.timesheets_financials source_tf
      on source_tf.timesheet_id=source_timesheet.timesheet_id and source_tf.is_current=true
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(source_tf.policy_snapshot_json,'{}'::jsonb),
      (c.payload_json->>'start_utc')::timestamptz,
      (c.payload_json->>'end_utc')::timestamptz,
      coalesce((c.payload_json->>'actual_break_mins')::integer,
        (c.payload_json->>'actual_break_minutes')::integer,
        (c.payload_json->>'break_mins')::integer,
        (c.payload_json->>'break_minutes')::integer,0)
    ) authoritative_hours on not c.is_daily and coalesce(c.import_authoritative,false)
      and c.existing_shift_id is not null
    left join lateral (
      select case
        when not c.is_daily
          and coalesce(c.import_authoritative,false)
          and c.resolved_client_id is not null
          and c.resolved_contract_id is not null
        then public.import_auto_authorise_policy_resolve_v1(
          case when upper(c.source_system)='NHSP' then 'NHSP'::public.hr_source_enum else 'HEALTHROSTER'::public.hr_source_enum end,
          c.resolved_client_id,c.resolved_contract_id,false
        )
        else null::jsonb
      end as value
    ) auto_authorise on true
    left join lateral (
      select replacement_candidate.timesheet_id,replacement_candidate.protection
      from (
        select
          replacement_timesheet.timesheet_id,
          replacement_timesheet.updated_at,
          replacement_timesheet.created_at,
          public._import_review_timesheet_protection_core_v1(
            replacement_timesheet.timesheet_id
          ) as protection
        from public.timesheets replacement_timesheet
        where not c.is_daily
          and c.existing_shift_id is not null
          and replacement_timesheet.is_adjustment is true
          and replacement_timesheet.is_current is true
          and replacement_timesheet.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and jsonb_typeof(replacement_timesheet.actual_schedule_json)='array'
          and replacement_timesheet.actual_schedule_json @> jsonb_build_array(
            jsonb_build_object(
              'shift_id',c.existing_shift_id::text,
              'external_row_key',c.source_row_key
            )
          )
      ) replacement_candidate
      where coalesce(
          (replacement_candidate.protection->>'paid')::boolean,
          false
        ) is false
        and coalesce(
          (replacement_candidate.protection->>'invoice_locked')::boolean,
          false
        ) is false
      order by
        replacement_candidate.updated_at desc nulls last,
        replacement_candidate.created_at desc nulls last
      limit 1
    ) mutable_replacement on true
  ), reconciliation_source_rows as (
    select
      f.*,
      ((row_number() over (order by f.source_row_key) - 1) / 100)::integer as reconciliation_batch
    from facts f
    where not f.is_daily and coalesce(f.import_authoritative,false) and f.existing_shift_id is not null
  ), reconciliation_inputs as (
    select coalesce(jsonb_agg(jsonb_build_object(
      'source_identity',f.source_row_key,
      'source_system',case when upper(f.source_system)='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      'source_shift_id',f.existing_shift_id,
      'external_row_key',f.source_row_key,
      'hr_row_id',f.id,
      'source_timesheet_id',coalesce(f.existing_shift_timesheet_id,f.authoritative_target_timesheet_id),
      'candidate_id',f.resolved_candidate_id,'client_id',f.resolved_client_id,'contract_id',f.resolved_contract_id,
      'week_ending_date',coalesce(f.resolved_week_ending_date,f.date_local+((7-extract(dow from f.date_local)::integer)%7)),
      'invoice_stream',case when upper(coalesce(f.source_tsfin_basis,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end,
      'authoritative_import_id',p_import_id,
      'authoritative_schedule_json',jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'date',f.date_local,'start_utc',f.payload_json->>'start_utc','end_utc',f.payload_json->>'end_utc',
        'break_mins',coalesce((f.payload_json->>'actual_break_mins')::integer,(f.payload_json->>'actual_break_minutes')::integer,
          (f.payload_json->>'break_mins')::integer,(f.payload_json->>'break_minutes')::integer,0),
        'shift_id',f.existing_shift_id,'external_row_key',f.source_row_key,'import_id',p_import_id,
        'ref_num',coalesce(f.hr_request_id,f.payload_json->>'ref_num',f.payload_json->>'reference_number')
      ))),
      'authoritative_hours',jsonb_build_object(
        'hours_day',coalesce(f.authoritative_hours_day,0),'hours_night',coalesce(f.authoritative_hours_night,0),
        'hours_sat',coalesce(f.authoritative_hours_sat,0),'hours_sun',coalesce(f.authoritative_hours_sun,0),
        'hours_bh',coalesce(f.authoritative_hours_bh,0),'total_hours',coalesce(f.authoritative_total_hours,f.hours_worked,0)
      )
    ) order by f.source_row_key),'[]'::jsonb) items
    from reconciliation_source_rows f
    group by f.reconciliation_batch
  ), reconciliation_balances as materialized (
    select b.source_identity,b.balance_json
    from reconciliation_inputs i
    cross join lateral public._import_review_effective_invoice_balance_core_v1(
      p_import_id,i.items,100,512,256,128
    ) b
  ), evidenced as (
    select c.*,
      rb.balance_json as reconciliation_balance,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,coalesce(rb.balance_json::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
    left join reconciliation_balances rb on rb.source_identity=c.source_row_key
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when not f.is_daily and coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when coalesce(f.authoritative_timesheet_has_calculated_expenses,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when coalesce((f.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
          then 'APPLY_AMENDMENT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or ((f.payload_json->>'break_mins') is not null
            and (f.payload_json->>'break_mins')::integer is distinct from
              coalesce((select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id),0))
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY'
             or nullif(m.reconciliation_balance->>'blocking_code','') is not null
             or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or nullif(m.reconciliation_balance->>'blocking_code','') is not null
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT' then 'CONTRACT_MISSING'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and coalesce(m.weekly_resolution_action,'')<>'OK' then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'DAILY_TIMESHEET_NOT_SUBMITTED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when nullif(m.reconciliation_balance->>'blocking_code','') is not null
            then m.reconciliation_balance->>'blocking_code'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'existing_shift_id',m.existing_shift_id,
        'invoice_stream',m.reconciliation_balance->>'invoice_stream',
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'effective_hours_net_is_positive')::boolean,false)
            and coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,false)
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.protection->>'paid')::boolean,false)
            then 'AMEND_PAID_UNINVOICED_SOURCE'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'reconciliation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'FROZEN_INVOICE_BALANCE'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_SOURCE'
          else null end,
        'mutable_replacement_timesheet_id',coalesce(
          (select x.value::uuid from jsonb_array_elements_text(coalesce(m.reconciliation_balance->'active_mutable_member_ids','[]'::jsonb)) x(value)
            join public.timesheets mutable_ts on mutable_ts.timesheet_id=x.value::uuid and mutable_ts.correction_kind='CHANGED_HOURS_REPLACEMENT' limit 1),
          m.mutable_replacement_timesheet_id),
        'correction_id',m.reconciliation_balance->>'active_mutable_correction_id',
        'reviewed_existing_correction_id',m.reconciliation_balance->>'reviewed_existing_correction_id',
        'repair_identity_mode',m.reconciliation_balance->>'repair_identity_mode',
        'physically_missing_mutable_roles',coalesce(m.reconciliation_balance->'physically_missing_mutable_roles','[]'::jsonb),
        'archived_ignored_roles',coalesce(m.reconciliation_balance->'archived_history_roles','[]'::jsonb),
        'reversal_repair_required',coalesce((m.reconciliation_balance->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((m.reconciliation_balance->>'replacement_repair_required')::boolean,false),
        'correction_generation_required',coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
          and not coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false),
        'standard_representable',coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,true),
        'B_hours',m.reconciliation_balance->'B_hours','B_financials',m.reconciliation_balance->'B_financials',
        'B_standard_schedule_json',m.reconciliation_balance->'B_standard_schedule_json',
        'B_policy_fingerprint',m.reconciliation_balance->>'B_policy_fingerprint',
        'review_policy_basis_kind','IMPORT_AUTHORITATIVE_WEEKLY_V1',
        'review_policy_basis_fingerprint',public._import_review_hash_v1(concat_ws('|','review-policy-basis-v1',
          m.reconciliation_balance->>'source_scope_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
          m.reconciliation_balance->>'role_evidence_fingerprint',m.authority_fingerprint,
          m.reconciliation_balance->>'B_policy_fingerprint',m.reconciliation_balance->>'invoice_stream')),
        'effective_invoice_ids',m.reconciliation_balance->'effective_invoice_ids',
        'effective_invoice_line_ids',m.reconciliation_balance->'effective_invoice_line_ids',
        'M_hours',m.reconciliation_balance->'M_hours','M_existing_financials',m.reconciliation_balance->'M_existing_financials',
        'A_hours',m.reconciliation_balance->'A_hours','A_schedule_json',m.reconciliation_balance->'A_schedule_json',
        'effective_invoice_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
        'mutable_generation_fingerprint',m.reconciliation_balance->>'active_mutable_fingerprint',
        'authoritative_evidence_fingerprint',m.reconciliation_balance->>'A_evidence_fingerprint',
        'reconciliation_fingerprint',m.reconciliation_balance->>'reconciliation_fingerprint',
        'source_scope_fingerprint',m.reconciliation_balance->>'source_scope_fingerprint'
      ) || jsonb_build_object(
        'archived_timesheet_ids',m.reconciliation_balance->'archived_timesheet_ids',
        'archived_history_timesheet_ids',m.reconciliation_balance->'archived_history_timesheet_ids',
        'archived_history_roles',m.reconciliation_balance->'archived_history_roles',
        'historical_missing_timesheet_ids',m.reconciliation_balance->'historical_missing_timesheet_ids',
        'active_mutable_member_ids',m.reconciliation_balance->'active_mutable_member_ids',
        'missing_mutable_roles',m.reconciliation_balance->'active_mutable_missing_roles',
        'active_mutable_parent_timesheet_id',m.reconciliation_balance->>'active_mutable_parent_timesheet_id',
        'pre_apply_authorised',m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null,
        'effective_auto_authorise',m.effective_auto_authorise,
        'intended_authorisation_action',case
          when m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null then 'REAUTHORISE'
          when m.effective_auto_authorise then 'AUTHORISE' else 'LEAVE_UNAUTHORISED' end,
        'financial_validation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_TSFIN_DEFINES_A' end,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
          (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.start_time_local,'end',m.end_time_local,
          'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
            (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
          'worked_hours',m.hours_worked,'worked_minutes',case when m.hours_worked is null then null else round(m.hours_worked*60) end,
          'reference',m.hr_request_id,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),'grade',m.grade_key)),
        'current_evidence',case when m.is_daily and m.resolved_timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'start',m.worked_start_iso,'end',m.worked_end_iso,
          'break_minutes',m.ts_break_minutes,'elapsed_minutes',m.worked_minutes,
          'worked_minutes',greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0),
          'worked_hours',round(greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0)/60.0,2),
          'reference',m.reference_number,'role',m.tsfin_role,'band',m.tsfin_band,'timesheet_id',m.resolved_timesheet_id))
          when not m.is_daily and m.existing_shift_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.existing_shift_start_utc,'end',m.existing_shift_end_utc,
          'break_minutes',m.existing_shift_break_minutes,'worked_minutes',m.existing_shift_paid_minutes,
          'role',m.existing_shift_role,'timesheet_id',m.existing_shift_timesheet_id,'shift_id',m.existing_shift_id)) end,
        'difference_codes',to_jsonb(array_remove(array[
          case when m.existing_shift_id is null and not m.is_daily then 'NEW_SHIFT'::text end,
          case when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_SELECTION_REQUIRED'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.start_time_local is distinct from
            (m.worked_start_iso at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.end_time_local is distinct from
            (m.worked_end_iso at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.ts_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0))>1
            then 'WORKED_HOURS'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.start_time_local is distinct from
            (m.existing_shift_start_utc at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.end_time_local is distinct from
            (m.existing_shift_end_utc at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.existing_shift_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end,
          case when not m.is_daily and coalesce((m.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
            then 'FINANCIAL_POSITION'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate candidate timesheet'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'Request timesheet from candidate'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'Candidate timesheet states they did not work this shift'
          when not m.is_daily and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'Timesheet occupied by expenses'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'TMS will repair current correction generation'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'TMS will create correction generation'
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.protection->>'paid')::boolean,false)
            then 'TMS will amend paid uninvoiced shift'
          when m.action_kind='APPLY_AMENDMENT' then 'TMS will amend shift'
          when m.action_kind='APPLY_CANCELLATION' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift' else 'TMS will cancel shift' end
          when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' when m.action_kind='NO_ACTION' then 'No action required'
          else 'Resolve before continuing' end,
        'resolution_kind',case
          when m.resolved_candidate_id is null then 'CANDIDATE_LINK'
          when m.resolved_client_id is null then 'CLIENT_LINK'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'DAILY_GRADE_ROLE'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'WEEKLY_ASSIGNMENT_CONTRACT'
          when m.is_daily and m.resolved_timesheet_id is null and coalesce(m.timesheet_count,0)>0 then 'DAILY_EXISTING_TIMESHEET' end,
        'resolution_options',case
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then m.daily_role_options
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH' then m.weekly_contract_options
          else '[]'::jsonb end,
        'mapping_evidence',case when m.is_daily then jsonb_strip_nulls(jsonb_build_object(
          'mapping_id',m.daily_mapping_id,'updated_at',m.daily_mapping_updated_at,'role',m.daily_mapped_role,'band',m.daily_mapped_band))
          else jsonb_strip_nulls(jsonb_build_object('mapping_fingerprint',m.weekly_mapping_evidence,
            'resolution_action',m.weekly_resolution_action,'resolution_reason',m.weekly_resolution_reason)) end,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
        'occupied_timesheet_id',case when coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
          then m.authoritative_target_timesheet_id end,
        'protection',m.protection
      )) summary_json
    from main_actions m
  )
  select * from rendered;

  -- Daily mismatch/query actions are independent of the evidence association.
  insert into pg_temp.import_review_catalog_v1
  with r as (
    select h.*,d.resolved_timesheet_id as timesheet_id,t.candidate_id,t.client_id,t.worked_start_iso,t.worked_end_iso,
      t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status::text,
      c.id contract_id,public._import_review_timesheet_protection_core_v1(d.resolved_timesheet_id) protection
    from public.hr_rows h
    join public.hr_imports i on i.id=h.import_id
    join public.import_review_daily_timesheet_resolutions d on d.import_id=h.import_id and d.hr_row_id=h.id and d.status in ('CURRENT','APPLIED')
    join public.v_timesheets_daily_match t on t.timesheet_id=d.resolved_timesheet_id
    left join public.contracts c on c.id=(select ts.contract_id from public.timesheets ts where ts.timesheet_id=t.timesheet_id)
    where h.import_id=p_import_id and (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
    order by h.id limit 501
  ), mismatch as (
    select r.*,
      case
        when r.hours_worked is not null and r.worked_minutes is not null
          and abs((r.hours_worked*60)-greatest(r.worked_minutes-coalesce(r.break_minutes,0),0))>1
          then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_evidence_supplied')::boolean,false)
          and (r.payload_json->>'break_mins')::integer is distinct from coalesce(r.break_minutes,0)
          then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
      jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'comparison_key','hr-row:'||m.id::text,
        'work_date',m.date_local,
        'match_status',m.reason_code,
        'timesheet_start',to_char(m.worked_start_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_end',to_char(m.worked_end_iso at time zone 'Europe/London','HH24:MI'),
        'timesheet_break_mins',m.break_minutes,
        'healthroster_start',to_char(m.start_time_local,'HH24:MI'),
        'healthroster_end',to_char(m.end_time_local,'HH24:MI'),
        'healthroster_break_mins',case
          when coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            then nullif(m.payload_json->>'break_mins','')::integer end,
        'healthroster_unit',coalesce(nullif(m.payload_json->>'Unit',''),nullif(m.payload_json->>'unit',''),
          nullif(m.unit_raw,''),nullif(m.unit_hint,'')),
        'healthroster_hospital',coalesce(nullif(m.payload_json->>'hospital_or_trust',''),
          nullif(m.payload_json->>'trust','')),
        'healthroster_request_grade',coalesce(nullif(m.payload_json->>'Request Grade',''),
          nullif(m.payload_json->>'Request_Grade',''),nullif(m.payload_json->>'grade_raw',''),
          nullif(m.assignment_grade_norm,'')),
        'ref_before',m.reference_number,
        'ref_after',m.hr_request_id
      ))) email_comparisons,
      lower(btrim(case when coalesce(m.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=m.contract_id),false)
        then (select c.ts_queries_alt_email_address from public.contracts c where c.id=m.contract_id)
        else (select c.ts_queries_email from public.clients c where c.id=m.client_id) end)) route_email
    from mismatch m where m.reason_code is not null
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,i.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||i.issue_fingerprint,i.issue_fingerprint,i.id,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v2',i.issue_fingerprint,i.email_comparisons::text,i.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select concat_ws('|',c.updated_at,c.ts_queries_alt_email_address) from public.contracts c where c.id=i.contract_id)
        else (select concat_ws('|',c.rev,c.updated_at,c.ts_queries_email) from public.clients c where c.id=i.client_id) end)),
    not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    e.id is null and not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    false,
    jsonb_build_object('reason_code',i.reason_code,'issue_fingerprint',i.issue_fingerprint,'work_date',i.date_local,
      'candidate_name',i.staff_raw,'timesheet_id',i.timesheet_id,'recipient_scope_key',
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||i.contract_id::text else 'CLIENT_DEFAULT:'||i.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(i.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_alt_email_address,''))),c.updated_at)) from public.contracts c where c.id=i.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_email,''))),c.rev,c.updated_at)) from public.clients c where c.id=i.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'comparisons',i.email_comparisons,
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(i.route_email,'')) not between 3 and 320 or position('@' in coalesce(i.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((i.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',i.protection)
  from issues i left join public.hr_issue_emails e on e.issue_fingerprint=i.issue_fingerprint;

  -- Weekly validation-only issues use the installed comparison engine, but
  -- normalise every user choice into the same server-owned decision catalogue.
  if exists(select 1 from public.hr_imports i where i.id=p_import_id
      and i.source_system='HEALTHROSTER'::public.hr_source_enum
      and upper(coalesce(i.import_scope,'HR_WEEKLY')) not like '%DAILY%') then
    v_weekly_preview:=public.hr_weekly_validation_preview(p_import_id);

    -- Validation-only Weekly evidence has two distinct, server-proven states.
    -- Neither state is an instruction to mutate CloudTMS financial records.
    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), eligible_validation_groups as (
      select d.candidate_id,d.summary_json->>'week_ending_date' week_ending_date
      from pg_temp.import_review_catalog_v1 d
      where d.candidate_id is not null
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      group by d.candidate_id,d.summary_json->>'week_ending_date'
      having bool_and(d.action_kind='NO_ACTION' and not d.blocking)
    ), missing_timesheets as (
      select p.row_json,p.timesheet_id,p.candidate_id,
        d.hr_row_id shift_hr_row_id,d.client_id shift_client_id,
        d.contract_id shift_contract_id,d.source_identity shift_source_identity,
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), omitted_shifts as (
      select p.*,cx.value comparison_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null and cx.value->>'match_status'='HR_ONLY'
    ), confirmed_exceptions as (
      select p.*,cx.value exception_json
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_TIMESHEET_NOT_SUBMITTED',m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-not-submitted',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-not-submitted-v2',
        m.shift_evidence_fingerprint,m.row_json::text)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code','WEEKLY_TIMESHEET_NOT_SUBMITTED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'difference_codes',jsonb_build_array('TIMESHEET_NOT_SUBMITTED'),
        'outcome_label','Request timesheet from candidate'))
    from missing_timesheets m
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',o.comparison_json->>'hr_row_id')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-candidate-did-not-work',o.comparison_json->>'hr_row_id'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      nullif(o.comparison_json->>'hr_row_id','')::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      o.comparison_json->>'exception_evidence_fingerprint',
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Confirm candidate did not work this shift')
    from omitted_shifts o
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',c.exception_json->>'hr_row_id')),
      'NO_ACTION','NO_ACTION',
      concat_ws(':','weekly-candidate-did-not-work',c.exception_json->>'hr_row_id'),
      concat_ws('|',c.timesheet_id,c.exception_json->>'work_date',
        c.exception_json->>'healthroster_start',c.exception_json->>'healthroster_end'),
      nullif(c.exception_json->>'hr_row_id','')::uuid,c.timesheet_id,null::uuid,c.client_id,c.candidate_id,c.contract_id,null::uuid,
      c.exception_json->>'evidence_fingerprint',
      false,false,false,
      jsonb_build_object(
        'reason_code','CANDIDATE_DID_NOT_WORK_CONFIRMED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',c.row_json->>'candidate_name','week_ending_date',c.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(c.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'confirmed_exception_count',coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'work_date',c.exception_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',c.exception_json->>'work_date','start',c.exception_json->>'healthroster_start',
          'end',c.exception_json->>'healthroster_end',
          'break_minutes',nullif(c.exception_json->>'healthroster_break_mins','')::integer,
          'reference',c.exception_json->>'reference')),
        'current_evidence',jsonb_build_object('timesheet_id',c.timesheet_id),
        'difference_codes',jsonb_build_array('CONFIRMED_EXCEPTION'),
        'outcome_label','Passed with confirmed exception')
    from confirmed_exceptions c;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id,
        nullif(r.value->>'issue_fingerprint','') issue_fingerprint
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), email_filtered as (
      select p.*,
        coalesce((select jsonb_agg(cx.value||jsonb_strip_nulls(jsonb_build_object(
            'healthroster_unit',coalesce(nullif(hr.payload_json->>'Unit',''),nullif(hr.payload_json->>'unit',''),
              nullif(hr.unit_raw,''),nullif(hr.unit_hint,''),nullif(cx.value->>'location_after','')),
            'healthroster_hospital',coalesce(nullif(hr.payload_json->>'hospital_or_trust',''),
              nullif(hr.payload_json->>'trust','')),
            'healthroster_request_grade',coalesce(nullif(hr.payload_json->>'Request Grade',''),
              nullif(hr.payload_json->>'Request_Grade',''),nullif(hr.payload_json->>'grade_raw',''),
              nullif(hr.assignment_grade_norm,''))
          )) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          left join public.hr_rows hr on hr.id=nullif(cx.value->>'hr_row_id','')::uuid
          where (
            coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)
          where fr.value<>'HealthRoster has a shift not present on the timesheet.'),'[]'::jsonb) email_failure_reasons
      from preview_rows p
    ), routed as (
      select p.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_WEEKLY','validation-email-v2',
          p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text)) email_issue_fingerprint,
        c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from email_filtered p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=public._import_review_hash_v1(concat_ws('|',
        'HEALTHROSTER_WEEKLY','validation-email-v2',p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text))
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
        and exists (
          select 1 from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
        )
    ), email_actions as (
      select r.*,
        public._import_review_hash_v1(concat_ws('|','query-route-v1',r.recipient_scope_key,r.recipient_email,
          case when r.recipient_scope='CONTRACT_OVERRIDE' then r.contract_updated_at::text
            else concat_ws('|',r.client_rev,r.client_updated_at) end)) route_fingerprint,
        length(coalesce(r.recipient_email,'')) between 3 and 320
          and r.recipient_email~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' valid_email
      from routed r
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.email_issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.email_issue_fingerprint,a.email_issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v2',a.timesheet_id,
        a.row_json->>'candidate_name',a.row_json->>'week_ending_date',
        a.email_comparisons::text,a.email_days::text,a.email_failure_reasons::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.email_issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(a.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(a.row_json->>'confirmed_exception_count','')::integer,0),
        'validation_difference_count',jsonb_array_length(a.email_comparisons),
      'failure_reasons',a.email_failure_reasons,
        'days',a.email_days,'comparisons',a.email_comparisons,
        'evidence_rows',coalesce((
          select jsonb_agg(jsonb_build_object(
            'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'healthroster_start',
              'end',cx.value->>'healthroster_end','break_minutes',nullif(cx.value->>'healthroster_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'hr_minutes','')::integer,'reference',cx.value->>'ref_after')),
            'current_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'timesheet_start',
              'end',cx.value->>'timesheet_end','break_minutes',nullif(cx.value->>'timesheet_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'ts_minutes','')::integer,'reference',cx.value->>'ref_before')),
            'difference_codes',to_jsonb(array_remove(array[
              case when coalesce(cx.value->>'match_status','MATCH')<>'MATCH' then cx.value->>'match_status' end,
              case when coalesce((cx.value->>'ref_changed')::boolean,false) then 'REFERENCE' end,
              case when coalesce(day_json.value->>'day_status','OK')<>'OK' then 'WORKED_HOURS' end
            ],null))
          ) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(a.email_comparisons) cx(value)
          left join lateral (select d.value from jsonb_array_elements(coalesce(a.row_json->'days','[]'::jsonb)) d(value)
            where d.value->>'date'=cx.value->>'work_date' limit 1) day_json on true
        ),'[]'::jsonb),
        'outcome_label',case when a.issue_id is null then 'Request amend shift' else 'Request amend shift reminder' end,
        'recipient_scope_key',a.recipient_scope_key,'recipient_route_fingerprint',a.route_fingerprint,
        'delivery_history_status',coalesce(a.delivery_history_status,'NEW'),'sent_count',coalesce(a.sent_count,0),
        'default_excluded_reason',case when a.issue_id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
          when not a.valid_email then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
          when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
        'protection',a.protection)
    from email_actions a;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
      where nullif(r.value->>'timesheet_id','') is not null
    ), invalidations as (
      select p.*,cx.value comparison_json,nullif(btrim(cx.value->>'comparison_key'),'') comparison_key,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where coalesce((cx.value->>'is_destructive_invalidation')::boolean,false)
        and exists(select 1 from public.hr_imports hi where hi.id=p_import_id
          and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'))
        and nullif(btrim(cx.value->>'comparison_key'),'') is not null
        and nullif(btrim(cx.value->>'ref_before'),'') is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',i.timesheet_id,i.comparison_key)),
      'INVALIDATE_REFERENCE','PENDING','timesheet:'||i.timesheet_id::text||':'||i.comparison_key,
      i.comparison_key,null::uuid,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-reference-invalidation-v1',i.timesheet_id,i.comparison_json::text,i.protection::text)),
      not coalesce((i.protection->>'protected')::boolean,false),false,false,
      jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT',
        'candidate_name',i.row_json->>'candidate_name','week_ending_date',i.row_json->>'week_ending_date',
        'timesheet_id',i.timesheet_id,'comparison_key',i.comparison_key,'comparison',i.comparison_json,
        'protection',i.protection,'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
    from invalidations i;
  end if;

  -- Complete Daily coverage also exposes existing timesheets that are absent
  -- from the file.  Missing rows are query-email candidates; reference
  -- invalidation is a separate, explicit, default-off decision.
  insert into pg_temp.import_review_catalog_v1
  with i as (
    select * from public.hr_imports where id=p_import_id
  ), missing as (
    select t.*,ts.contract_id,c.first_name,c.last_name,cl.name as client_name,
      public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection,
      lower(btrim(case when coalesce(ts.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=ts.contract_id),false)
        then (select ct.ts_queries_alt_email_address from public.contracts ct where ct.id=ts.contract_id)
        else cl.ts_queries_email end)) route_email,
      public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY','MISSING_FROM_IMPORT',
        t.timesheet_id,t.candidate_id,t.client_id,(t.worked_start_iso at time zone 'Europe/London')::date,
        coalesce(t.reference_number,''))) issue_fingerprint
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join public.candidates c on c.id=t.candidate_id
    join public.clients cl on cl.id=t.client_id
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (
        select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED')
      )
    order by t.timesheet_id limit 501
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,m.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||m.issue_fingerprint,m.issue_fingerprint,null::uuid,m.timesheet_id,null::uuid,
    m.client_id,m.candidate_id,m.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','missing-daily-email-v1',m.issue_fingerprint,m.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(m.contract_id is not null and (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select concat_ws('|',ct.updated_at,ct.ts_queries_alt_email_address) from public.contracts ct where ct.id=m.contract_id)
        else (select concat_ws('|',cl.rev,cl.updated_at,cl.ts_queries_email) from public.clients cl where cl.id=m.client_id) end)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,
    e.id is null and not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,false,
    jsonb_build_object('reason_code','MISSING_FROM_IMPORT','issue_fingerprint',m.issue_fingerprint,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,
      'week_ending_date',(m.worked_start_iso at time zone 'Europe/London')::date
        + ((7-extract(dow from (m.worked_start_iso at time zone 'Europe/London')::date)::integer)%7),
      'candidate_name',btrim(concat_ws(' ',m.first_name,m.last_name)),'client_name',m.client_name,
      'timesheet_id',m.timesheet_id,'reference_number',m.reference_number,
      'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,
      'break_minutes',m.break_minutes,'role',m.tsfin_role,
      'recipient_scope_key',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||m.contract_id::text else 'CLIENT_DEFAULT:'||m.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||ct.id::text,
          lower(btrim(coalesce(ct.ts_queries_alt_email_address,''))),ct.updated_at)) from public.contracts ct where ct.id=m.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||cl.id::text,
          lower(btrim(coalesce(cl.ts_queries_email,''))),cl.rev,cl.updated_at)) from public.clients cl where cl.id=m.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(m.route_email,'')) not between 3 and 320 or position('@' in coalesce(m.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',m.protection)
  from missing m left join public.hr_issue_emails e on e.issue_fingerprint=m.issue_fingerprint;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists(select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'MARK_VALIDATION_ERROR',m.timesheet_id)),
    'MARK_VALIDATION_ERROR','READY','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-validation-v1',m.timesheet_id,m.worked_start_iso,
      m.worked_end_iso,m.break_minutes,m.worked_minutes,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false)
      then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_IMPORT' end,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,'break_minutes',m.break_minutes,
      'hours_worked',m.worked_minutes/60.0,'role',m.tsfin_role,'protection',m.protection)
  from missing m;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and nullif(btrim(t.reference_number),'') is not null
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',m.timesheet_id)),
    'INVALIDATE_REFERENCE','PENDING','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-reference-v1',m.timesheet_id,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'protected')::boolean,false),false,false,
    jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT',
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'protection',m.protection,
      'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
  from missing m;

  -- Omitted existing shifts are proposed only inside immutable complete coverage.
  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select s.*,public._import_review_timesheet_protection_core_v1(s.timesheet_id) protection
    from public.nhsp_shifts s
    join i on true
    cross join lateral public._import_review_effective_authority_core_v1(
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
      s.contract_id,s.client_id,coalesce(s.week_ending_date,s.work_date)) authority
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and authority.import_authoritative
      and s.cancelled_at_utc is null
      and s.work_date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or s.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=s.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=s.candidate_id))
      and not exists (select 1 from public.hr_rows h where h.import_id=i.id and h.external_row_key=s.external_row_key)
    order by s.id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'APPLY_CANCELLATION',m.id)),
    'APPLY_CANCELLATION','READY','shift:'||m.id::text,m.external_row_key,null::uuid,m.timesheet_id,m.id,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-shift-v1',m.id,m.updated_at,m.timesheet_id,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_COMPLETE_IMPORT' end,
      'work_date',m.work_date,'week_ending_date',m.week_ending_date,'candidate_id',m.candidate_id,'client_id',m.client_id,
      'start_time',m.start_utc,'end_time',m.end_utc,'break_minutes',m.break_mins,'role',m.assignment_code,'protection',m.protection)
  from missing m;

  -- Query emails can be committed only with one exact, complete, current
  -- timesheet PDF.  Invoice-linked validation records are never eligible for
  -- validation.  The same evidence fingerprint is frozen into the decision so
  -- document or invoice lifecycle movement makes a reviewed action stale.
  with evidence as materialized (
    select c.action_id,public._import_review_query_evidence_core_v1(c.timesheet_id) evidence_json
    from pg_temp.import_review_catalog_v1 c
    where c.timesheet_id is not null
      and (
        c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
        or c.summary_json->>'authority_mode'='VALIDATION_ONLY'
        or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
      )
  )
  update pg_temp.import_review_catalog_v1 c
  set evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','query-evidence-decision-v1',
        c.evidence_fingerprint,e.evidence_json->>'evidence_fingerprint')),
      action_category=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then 'PENDING'
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then 'PENDING'
        else c.action_category end,
      selectable=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.selectable end,
      default_selected=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.default_selected end,
      blocking=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then true
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then true
        else c.blocking end,
      summary_json=c.summary_json||jsonb_build_object(
        'attachment_evidence',e.evidence_json,
        'attachment_fingerprint',e.evidence_json->>'evidence_fingerprint'
      )||case
        when (
          c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null
        ) or (
          (
            c.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
          ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
        ) then jsonb_build_object(
          'reason_code',e.evidence_json->>'reason_code',
          'default_excluded_reason',e.evidence_json->>'reason_code',
          'outcome_label',case e.evidence_json->>'reason_code'
            when 'TIMESHEET_PRESENT_BUT_INVOICED' then 'Timesheet present but invoiced'
            when 'TIMESHEET_EVIDENCE_PREPARING' then 'Preparing timesheet evidence'
            else 'Timesheet evidence incomplete' end
        ) else '{}'::jsonb end
  from evidence e
  where e.action_id=c.action_id;

  -- An invoice-linked validation shift owns this blocker.  Do not also expose
  -- the aggregate query-email action as a second visual copy of the same
  -- shift.  Preserve any delivery history on the owning shift before removing
  -- only that redundant email row; genuine query emails remain untouched.
  update pg_temp.import_review_catalog_v1 owner_row
  set summary_json=owner_row.summary_json||jsonb_strip_nulls(jsonb_build_object(
        'delivery_history_status',email_row.summary_json->>'delivery_history_status',
        'sent_count',nullif(email_row.summary_json->>'sent_count','')::integer,
        'previously_queried',coalesce(nullif(email_row.summary_json->>'sent_count','')::integer,0)>0
      ))
  from pg_temp.import_review_catalog_v1 email_row
  where owner_row.timesheet_id=email_row.timesheet_id
    and owner_row.action_id<>email_row.action_id
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  delete from pg_temp.import_review_catalog_v1 email_row
  using pg_temp.import_review_catalog_v1 owner_row
  where email_row.timesheet_id=owner_row.timesheet_id
    and email_row.action_id<>owner_row.action_id
    and email_row.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
    and email_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
    and owner_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and owner_row.summary_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED';

  -- Daily validation is atomic per Daily timesheet.  An email, document hold
  -- or invoice blocker for that record prevents only that record from entering
  -- validation and TSFIN work.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','daily-validation-held-v1',
        current_row.evidence_fingerprint,current_row.timesheet_id)),
      summary_json=current_row.summary_json||jsonb_build_object(
        'daily_validation_held',true,
        'validation_hold_label','Validation held: resolve this Daily timesheet first'
      )
  where current_row.action_kind='NO_ACTION'
    and coalesce(nullif(current_row.summary_json->>'is_daily','')::boolean,false)
    and current_row.timesheet_id is not null
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.timesheet_id=current_row.timesheet_id
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  -- Weekly validation is all-or-nothing per candidate/client/contract/week.
  -- One mismatch, unresolved exception, missing attachment or invoice blocker
  -- holds the whole Weekly timesheet while leaving the actual issue visible.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','weekly-validation-held-v2',
        current_row.evidence_fingerprint,current_row.candidate_id,current_row.client_id,
        current_row.contract_id,current_row.summary_json->>'week_ending_date')),
      summary_json=current_row.summary_json||jsonb_build_object(
        'weekly_validation_held',true,
        'weekly_validation_badge_code','WEEKLY_VALIDATION_HELD',
        'weekly_validation_badge_label','Validation held: resolve outstanding shift',
        'validation_hold_label','Validation held: one or more shifts still require action'
      )
  where current_row.action_kind='NO_ACTION'
    and current_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and current_row.summary_json->>'source_route' not like '%DAILY%'
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.candidate_id=current_row.candidate_id
        and hold.client_id=current_row.client_id
        and hold.contract_id is not distinct from current_row.contract_id
        and hold.summary_json->>'week_ending_date'=current_row.summary_json->>'week_ending_date'
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  select count(*) into v_count from pg_temp.import_review_catalog_v1;
  if v_count>p_max_actions then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count',v_count,'max',p_max_actions)::text;
  end if;

  return query select c.action_id,c.action_kind,c.action_category,c.target_key,c.source_identity,
    c.hr_row_id,c.timesheet_id,c.shift_id,c.client_id,c.candidate_id,c.contract_id,c.issue_id,
    c.evidence_fingerprint,c.selectable,c.default_selected,c.blocking,c.summary_json
  from pg_temp.import_review_catalog_v1 c order by c.action_id;
end
$function$;

-- _import_review_action_outcomes_immutable_guard_v1()
CREATE OR REPLACE FUNCTION public._import_review_action_outcomes_immutable_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  raise exception 'IMPORT_REVIEW_ACTION_OUTCOMES_ARE_APPEND_ONLY' using errcode='55000';
end
$function$;

-- _import_review_apply_complete_core_v1(uuid,uuid,uuid,jsonb,boolean)
CREATE OR REPLACE FUNCTION public._import_review_apply_complete_core_v1(p_import_id uuid, p_operation_id uuid, p_actor_user_id uuid, p_response_json jsonb, p_follow_up_required boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
  v_tsfin_required boolean; v_email_required boolean; v_response jsonb; v_refresh jsonb;
  v_selected_ids text[]; v_remaining_blockers integer; v_remaining_selectable integer;
  v_terminal boolean; v_result_status text;
begin
  select * into v from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if v.status<>'APPLYING' or v.last_operation_id<>p_operation_id or o.id is null then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_MISMATCH' using errcode='40001';
  end if;
  select coalesce(array_agg(value order by value),array[]::text[]) into v_selected_ids
  from jsonb_array_elements_text(coalesce(o.response_json#>'{request_envelope,selected_action_ids}','[]'::jsonb)) value;
  if cardinality(v_selected_ids)=0 then
    raise exception 'IMPORT_REVIEW_APPLY_COMPLETION_ACTION_SET_MISSING' using errcode='55000';
  end if;
  v_tsfin_required:=jsonb_typeof(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'affected_timesheet_ids','[]'::jsonb))>0;
  v_email_required:=jsonb_typeof(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))='array'
    and jsonb_array_length(coalesce(p_response_json->'post_commit_email_action_ids','[]'::jsonb))>0;
  v_response:=coalesce(p_response_json,'{}'::jsonb)||jsonb_build_object(
    'review_tsfin_follow_up_status',case when v_tsfin_required then 'PENDING' else 'NOT_REQUIRED' end,
    'review_email_follow_up_status',case when v_email_required then 'PENDING' else 'NOT_REQUIRED' end,
    'applied_action_ids',to_jsonb(v_selected_ids),'applied_action_count',cardinality(v_selected_ids));

  insert into public.import_review_action_outcomes(
    action_id,import_id,operation_id,action_kind,source_identity,candidate_id,client_id,contract_id,
    hr_row_id,timesheet_id,shift_id,evidence_fingerprint,completed_label,summary_json,applied_by_user_id
  )
  select d.action_id,p_import_id,p_operation_id,d.action_kind,d.source_identity,d.candidate_id,d.client_id,d.contract_id,
    d.hr_row_id,d.timesheet_id,d.shift_id,d.evidence_fingerprint,
    case d.action_kind
      when 'INCLUDE_SHIFT' then 'TMS added shift'
      when 'APPLY_AMENDMENT' then case
        when d.summary_json->>'amendment_route'='AMEND_PAID_UNINVOICED_SOURCE' then 'TMS amended the paid uninvoiced shift'
        when d.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT' then 'TMS repaired the existing correction generation'
        when d.summary_json->>'amendment_route'='CREATE_REVERSAL_REPLACEMENT' then 'TMS created the required reversal and corrected-hours generation'
        else 'TMS amended the existing shift' end
      when 'APPLY_CANCELLATION' then case
        when coalesce((d.summary_json#>>'{protection,paid}')::boolean,false)
          or coalesce((d.summary_json#>>'{protection,invoice_locked}')::boolean,false)
        then 'TMS reversed shift' else 'TMS cancelled shift' end
      when 'MARK_VALIDATION_ERROR' then 'Timesheet validated'
      when 'INVALIDATE_REFERENCE' then 'Stored reference cleared'
      when 'DAILY_TIMESHEET_RESOLUTION' then 'Timesheet linked'
      when 'EMAIL_ISSUE' then 'Client query queued'
      when 'EMAIL_REMINDER' then 'Client query reminder queued'
      when 'NO_ACTION' then 'No action confirmed'
      else 'Review action completed' end,
    d.summary_json,p_actor_user_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.candidate_id is not null and d.client_id is not null
  on conflict(action_id) do nothing;
  if (select count(*) from public.import_review_action_outcomes x
      where x.import_id=p_import_id and x.operation_id=p_operation_id)<>cardinality(v_selected_ids) then
    raise exception 'IMPORT_REVIEW_APPLY_OUTCOME_SET_MISMATCH' using errcode='55000';
  end if;

  update public.import_review_daily_timesheet_resolutions r set status='APPLIED',applied_operation_id=p_operation_id,applied_at_utc=now(),updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and exists(
    select 1 from public.import_review_decisions d where d.import_id=r.import_id and d.hr_row_id=r.hr_row_id
      and d.is_current and d.action_id=any(v_selected_ids));

  update public.import_review_states set status='IN_REVIEW',state_version=state_version+1,
    follow_up_status=case when v_tsfin_required or v_email_required or p_follow_up_required then 'PENDING' else 'NOT_REQUIRED' end,
    follow_up_error_code=null,follow_up_error_message=null,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v;

  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v.state_version,p_actor_user_id,5000);
  select count(*) filter(where d.blocking),count(*) filter(where d.selectable and not (
      d.action_kind='NO_ACTION' and exists(select 1 from public.import_review_action_outcomes x
        where x.import_id=d.import_id and x.source_identity=d.source_identity)))
    into v_remaining_blockers,v_remaining_selectable
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;
  v_terminal:=v_remaining_blockers=0 and v_remaining_selectable=0;
  if v_terminal then
    update public.import_review_states set status='APPLIED',state_version=state_version+1,
      applied_at_utc=now(),applied_by_user_id=p_actor_user_id,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id
    where import_id=p_import_id returning * into v;
    update public.hr_imports set applied_at=coalesce(applied_at,now()) where id=p_import_id;
  else
    select * into v from public.import_review_states where import_id=p_import_id;
  end if;
  v_result_status:=v.status;
  v_response:=v_response||jsonb_build_object(
    'partial_application',not v_terminal,
    'review_status_after_commit',v_result_status,
    'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
  update public.import_apply_operations
  set state=case when v_tsfin_required or v_email_required or p_follow_up_required then 'SOURCE_COMMITTED_TSFIN_PENDING' else 'COMPLETE' end,
    committed_at_utc=coalesce(committed_at_utc,now()),
    finalised_at_utc=case when v_tsfin_required or v_email_required or p_follow_up_required then finalised_at_utc else coalesce(finalised_at_utc,now()) end,
    response_json=response_json||v_response,updated_at_utc=now()
  where id=p_operation_id;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v.state_version,p_operation_id,'APPLY_COMMITTED',p_actor_user_id,jsonb_build_object(
    'follow_up_status',v.follow_up_status,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable));
  return jsonb_build_object('ok',true,'status',v.status,'follow_up_status',v.follow_up_status,
    'state_version',v.state_version,'partial_application',not v_terminal,
    'applied_action_count',cardinality(v_selected_ids),'remaining_blocker_count',v_remaining_blockers,
    'remaining_selectable_count',v_remaining_selectable);
end $function$;

-- _import_review_apply_envelope_core_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_apply_envelope_core_v1(p_import_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype;
  v_import public.hr_imports%rowtype;
  v_selected_ids text[];
  v_invalidation_ids text[];
  v_correction_units jsonb;
  v_reconciliation_units jsonb;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(r.action_id order by r.action_id),array[]::text[])
  into v_selected_ids from public._import_review_ready_action_ids_core_v1(p_import_id) r;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(jsonb_build_object(
    'action_id',d.action_id,'root_timesheet_id',d.timesheet_id,'source_row_key',d.source_identity,
    'correction_action',case when d.action_kind='APPLY_AMENDMENT' then 'CHANGED_HOURS' else 'CANCELLATION' end,
    'correction_shape',case when d.action_kind='APPLY_AMENDMENT' then 'REVERSAL_REPLACEMENT' else 'REVERSAL_ONLY' end
  ) order by d.action_id),'[]'::jsonb)
  into v_correction_units
  from public.import_review_decisions d
  cross join lateral (
    select public._import_review_timesheet_protection_core_v1(d.timesheet_id) as protection
  ) pr
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select a.import_authoritative
      from public._import_review_effective_authority_core_v1(
        case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
        d.contract_id,d.client_id,coalesce(d.summary_json->>'work_date',d.summary_json->>'week_ending_date')::date) a),false);
  select coalesce(jsonb_agg(unit_json order by action_id),'[]'::jsonb)
  into v_reconciliation_units
  from (
    select d.action_id,
      jsonb_build_object(
        'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_V1',
        'action_id',d.action_id,'source_identity',d.source_identity,
        'source_system',case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HEALTHROSTER' end,
        'source_shift_id',d.summary_json->>'existing_shift_id',
        'hr_row_id',d.hr_row_id,
        'authoritative_import_id',p_import_id,
        'source_timesheet_id',d.timesheet_id,
        'candidate_id',d.candidate_id,'client_id',d.client_id,'contract_id',d.contract_id,
        'week_ending_date',d.summary_json->>'week_ending_date',
        'invoice_stream',d.summary_json->>'invoice_stream',
        'source_scope_fingerprint',d.summary_json->>'source_scope_fingerprint',
        'route',d.summary_json->>'amendment_route',
        'reconciliation_mode',d.summary_json->>'reconciliation_mode',
        'B_effective_invoice_ids',coalesce(d.summary_json->'effective_invoice_ids','[]'::jsonb),
        'B_effective_invoice_line_ids',coalesce(d.summary_json->'effective_invoice_line_ids','[]'::jsonb),
        'B_hours',d.summary_json->'B_hours','B_financials',d.summary_json->'B_financials',
        'B_standard_schedule_json',coalesce(d.summary_json->'B_standard_schedule_json','[]'::jsonb),
        'B_invoice_fingerprint',d.summary_json->>'effective_invoice_fingerprint',
        'M_active_member_ids',coalesce(d.summary_json->'active_mutable_member_ids','[]'::jsonb),
        'M_missing_roles',coalesce(d.summary_json->'physically_missing_mutable_roles',d.summary_json->'missing_mutable_roles','[]'::jsonb),
        'M_hours',d.summary_json->'M_hours','M_fingerprint',d.summary_json->>'mutable_generation_fingerprint',
        'A_schedule_json',d.summary_json->'A_schedule_json','A_hours',d.summary_json->'A_hours',
        'A_evidence_fingerprint',d.summary_json->>'authoritative_evidence_fingerprint',
        'archived_timesheet_ids',coalesce(d.summary_json->'archived_history_timesheet_ids',d.summary_json->'archived_timesheet_ids','[]'::jsonb),
        'archived_history_roles',coalesce(d.summary_json->'archived_history_roles','[]'::jsonb),
        'historical_missing_timesheet_ids',coalesce(d.summary_json->'historical_missing_timesheet_ids','[]'::jsonb),
        'reviewed_existing_correction_id',coalesce(d.summary_json->>'reviewed_existing_correction_id',d.summary_json->>'correction_id'),
        'reviewed_existing_member_ids',coalesce(d.summary_json->'active_mutable_member_ids','[]'::jsonb),
        'repair_identity_mode',d.summary_json->>'repair_identity_mode',
        'reversal_repair_required',coalesce((d.summary_json->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((d.summary_json->>'replacement_repair_required')::boolean,false),
        'expected_roles',case when d.summary_json->>'amendment_route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          then jsonb_build_array('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') else '[]'::jsonb end,
        'parent_timesheet_id',coalesce(
          nullif(d.summary_json->>'active_mutable_parent_timesheet_id','')::uuid,
          d.timesheet_id
        ),
        'review_policy_basis_kind',d.summary_json->>'review_policy_basis_kind',
        'review_policy_basis_fingerprint',d.summary_json->>'review_policy_basis_fingerprint',
        'intended_authorisation_action',d.summary_json->>'intended_authorisation_action',
        'financial_validation_mode',d.summary_json->>'financial_validation_mode',
        'reconciliation_fingerprint',d.summary_json->>'reconciliation_fingerprint',
        'unit_fingerprint',public._import_review_hash_v1(concat_ws('|','unit-v2',d.action_id,d.source_identity,
          d.summary_json->>'existing_shift_id',d.summary_json->>'amendment_route',d.summary_json->>'reconciliation_mode',
          d.summary_json->>'reconciliation_fingerprint',d.summary_json->>'review_policy_basis_kind',
          d.summary_json->>'review_policy_basis_fingerprint',d.evidence_fingerprint))
      ) unit_json
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.selectable
      and d.action_id=any(v_selected_ids) and d.action_kind='APPLY_AMENDMENT'
      and upper(coalesce(d.summary_json->>'authority_mode',''))='AUTHORITATIVE'
      and coalesce((d.summary_json->>'is_daily')::boolean,false)=false
  ) frozen;
  if exists (
    select 1 from jsonb_array_elements(v_reconciliation_units) u
    where nullif(u->>'action_id','') is null or nullif(u->>'source_identity','') is null
      or nullif(u->>'route','') is null or nullif(u->>'reconciliation_fingerprint','') is null
      or jsonb_typeof(u->'A_schedule_json')<>'array' or jsonb_typeof(u->'A_hours')<>'object'
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
  end if;
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units,
    'reconciliation_units',v_reconciliation_units,
    'batch_scope_units',coalesce((select jsonb_agg(jsonb_build_object(
      'candidate_id',u.candidate_id,'client_id',u.client_id) order by u.candidate_id,u.client_id)
      from (select distinct d.candidate_id,d.client_id from public.import_review_decisions d
        where d.import_id=p_import_id and d.action_id=any(v_selected_ids)) u),'[]'::jsonb),
    'deferred_action_count',(select count(*) from public.import_review_decisions d
      where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected));
end
$function$;

-- _import_review_assert_actor_v1(uuid)
CREATE OR REPLACE FUNCTION public._import_review_assert_actor_v1(p_actor_user_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  if p_actor_user_id is null or not exists (
    select 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false)
  ) then
    raise exception 'IMPORT_REVIEW_ACTOR_INVALID' using errcode='42501';
  end if;
end
$function$;

-- _import_review_auto_authorise_targets_core_v1(uuid[],hr_source_enum,boolean)
CREATE OR REPLACE FUNCTION public._import_review_auto_authorise_targets_core_v1(p_timesheet_ids uuid[], p_source_system hr_source_enum, p_validation_context boolean DEFAULT false)
 RETURNS uuid[]
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_source text:=upper(btrim(coalesce(p_source_system::text,'')));
  v_result uuid[]:=array[]::uuid[];
begin
  if coalesce(cardinality(p_timesheet_ids),0)>100 then
    raise exception 'IMPORT_REVIEW_AUTO_AUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
  end if;
  if v_source not in ('NHSP','HEALTHROSTER','HEALTHROSTER_DAILY') then
    raise exception 'IMPORT_REVIEW_AUTO_AUTHORISE_SOURCE_INVALID' using errcode='22023';
  end if;

  select coalesce(array_agg(target.timesheet_id order by target.timesheet_id),array[]::uuid[])
  into v_result
  from (
    select distinct t.timesheet_id
    from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    join public.timesheets t
      on t.timesheet_id=requested.timesheet_id
     and t.is_current=true
     and t.revoked_at is null
    join public.contracts c on c.id=t.contract_id
    cross join lateral (
      select public._import_review_timesheet_protection_core_v1(t.timesheet_id) as value
    ) protection
    cross join lateral (
      select public.import_auto_authorise_policy_resolve_v1(
        p_source_system,c.client_id,c.id,coalesce(p_validation_context,false)
      ) as value
    ) policy
    where t.authorised_at_server is null
      -- Import correction reversals/replacements are mandatory-authorisation
      -- targets owned by the transactional route, not policy-controlled new
      -- timesheets.  Keep the configuration helper strictly ordinary-only.
      and not (coalesce(t.is_adjustment,false) and t.correction_id is not null)
      and not exists (
        select 1 from public.timesheets_financials tf
        where tf.timesheet_id=t.timesheet_id and tf.is_current=true
          and tf.authorised_at_utc is not null
      )
      and not exists (
        select 1 from public.contract_weeks cw
        where cw.timesheet_id=t.timesheet_id and upper(coalesce(cw.status::text,''))='AUTHORISED'
      )
      and coalesce((protection.value->>'paid')::boolean,false)=false
      and coalesce((protection.value->>'invoice_locked')::boolean,false)=false
      and coalesce((protection.value->>'active_pay_draft')::boolean,false)=false
      and public._import_review_timesheet_has_calculated_expenses_core_v1(t.timesheet_id)=false
      and coalesce((policy.value->>'effective_value')::boolean,false)=true
  ) target;
  return v_result;
end
$function$;

-- _import_review_create_core_v2(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text,uuid,bigint)
CREATE OR REPLACE FUNCTION public._import_review_create_core_v2(p_import_id uuid, p_coverage_mode text, p_coverage_start_date date, p_coverage_end_date date, p_scope_clients jsonb DEFAULT '[]'::jsonb, p_scope_candidates jsonb DEFAULT '[]'::jsonb, p_expected_source_file_sha256 text DEFAULT NULL::text, p_expected_parser_version text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_key text DEFAULT NULL::text, p_supersede_import_id uuid DEFAULT NULL::uuid, p_expected_supersede_state_version bigint DEFAULT NULL::bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_import public.hr_imports%rowtype;
  v_mode text:=upper(btrim(coalesce(p_coverage_mode,'')));
  v_clients jsonb:=coalesce(p_scope_clients,'[]'::jsonb);
  v_candidates jsonb:=coalesce(p_scope_candidates,'[]'::jsonb);
  v_operation_key text:=btrim(coalesce(p_operation_key,''));
  v_request_hash text; v_fingerprint text; v_revision_group uuid; v_revision_no integer;
  v_existing public.hr_imports%rowtype; v_state public.import_review_states%rowtype;
  v_supersede_import public.hr_imports%rowtype; v_supersede_state public.import_review_states%rowtype;
  v_refresh jsonb; v_overlap jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_mode not in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES','PARTIAL')
     or p_coverage_start_date is null or p_coverage_end_date is null
     or p_coverage_start_date>p_coverage_end_date
     or p_coverage_end_date-p_coverage_start_date>366 then
    raise exception 'IMPORT_REVIEW_COVERAGE_INVALID' using errcode='22023';
  end if;
  if length(v_operation_key)<16 or length(v_operation_key)>256 then
    raise exception 'IMPORT_REVIEW_OPERATION_KEY_INVALID' using errcode='22023';
  end if;
  if jsonb_typeof(v_clients)<>'array' or jsonb_typeof(v_candidates)<>'array'
     or jsonb_array_length(v_clients)>100 or jsonb_array_length(v_candidates)>500
     or pg_column_size(v_clients)+pg_column_size(v_candidates)>262144 then
    raise exception 'IMPORT_REVIEW_SCOPE_LIMIT_EXCEEDED' using errcode='22023';
  end if;
  if v_mode='COMPLETE_SELECTED_CANDIDATES' and jsonb_array_length(v_candidates)=0 then
    raise exception 'IMPORT_REVIEW_SELECTED_CANDIDATES_REQUIRED' using errcode='22023';
  end if;
  if v_mode<>'COMPLETE_SELECTED_CANDIDATES' and jsonb_array_length(v_candidates)>0 then
    raise exception 'IMPORT_REVIEW_CANDIDATES_NOT_ALLOWED_FOR_MODE' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_array_elements(v_clients) x where jsonb_typeof(x)<>'object'
    or nullif(btrim(x->>'source_client_key'),'') is null or length(btrim(x->>'source_client_key'))>512
    or length(coalesce(x->>'source_display_label',''))>512)
    or exists(select 1 from jsonb_array_elements(v_candidates) x where jsonb_typeof(x)<>'object'
    or nullif(btrim(x->>'source_candidate_key'),'') is null or length(btrim(x->>'source_candidate_key'))>512
    or length(coalesce(x->>'source_display_label',''))>512) then
    raise exception 'IMPORT_REVIEW_SCOPE_ITEM_INVALID' using errcode='22023';
  end if;
  if (select count(*) from jsonb_array_elements(v_clients))<>(select count(distinct btrim(x->>'source_client_key')) from jsonb_array_elements(v_clients)x)
    or (select count(*) from jsonb_array_elements(v_candidates))<>(select count(distinct btrim(x->>'source_candidate_key')) from jsonb_array_elements(v_candidates)x) then
    raise exception 'IMPORT_REVIEW_SCOPE_ITEM_DUPLICATE' using errcode='22023';
  end if;

  v_request_hash:=public._import_review_hash_v1((jsonb_build_object(
    'import_id',p_import_id,'coverage_mode',v_mode,'coverage_start_date',p_coverage_start_date,
    'coverage_end_date',p_coverage_end_date,'clients',(select coalesce(jsonb_agg(x order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(x order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x),
    'source_file_sha256',lower(btrim(coalesce(p_expected_source_file_sha256,''))),
    'parser_version',btrim(coalesce(p_expected_parser_version,'')))
    || case when p_supersede_import_id is null then '{}'::jsonb else jsonb_build_object(
      'supersede_import_id',p_supersede_import_id,
      'expected_supersede_state_version',p_expected_supersede_state_version) end)::text);

  select * into v_existing from public.hr_imports where coverage_operation_key=v_operation_key for update;
  if found then
    if v_existing.id<>p_import_id or v_existing.coverage_request_hash is distinct from v_request_hash then
      raise exception 'IMPORT_REVIEW_OPERATION_KEY_CONFLICT' using errcode='23505';
    end if;
    select * into v_state from public.import_review_states where import_id=p_import_id;
    return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
      'replay',true,'import_id',p_import_id,'status',v_state.status,
      'state_version',v_state.state_version,'preview_generation',v_state.preview_generation,
      'coverage_fingerprint',v_existing.coverage_fingerprint,'preview_fingerprint',v_state.preview_fingerprint);
  end if;

  select * into v_import from public.hr_imports where id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_IMPORT_NOT_FOUND' using errcode='P0002'; end if;
  if v_import.applied_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_ALREADY_APPLIED' using errcode='55000'; end if;
  if v_import.pruned_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_PRUNED' using errcode='55000'; end if;
  if v_import.coverage_locked_at is not null then raise exception 'IMPORT_REVIEW_ALREADY_CREATED' using errcode='55000'; end if;
  if nullif(btrim(coalesce(v_import.source_file_sha256,'')),'') is null
     or nullif(btrim(coalesce(v_import.parser_version,'')),'') is null then
    raise exception 'IMPORT_REVIEW_STAGING_EVIDENCE_REQUIRED' using errcode='55000';
  end if;
  if lower(v_import.source_file_sha256) is distinct from lower(btrim(coalesce(p_expected_source_file_sha256,''))) then
    raise exception 'IMPORT_REVIEW_SOURCE_HASH_MISMATCH' using errcode='40001';
  end if;
  if v_import.parser_version is distinct from btrim(coalesce(p_expected_parser_version,'')) then
    raise exception 'IMPORT_REVIEW_PARSER_VERSION_MISMATCH' using errcode='40001';
  end if;

  if v_import.client_id is not null and jsonb_array_length(v_clients)=0 then
    v_clients:=jsonb_build_array(jsonb_build_object('source_client_key','client:'||v_import.client_id::text,
      'source_display_label',null,'client_id',v_import.client_id));
  end if;
  if exists(select 1 from jsonb_array_elements(v_clients)x where nullif(x->>'client_id','') is not null
    and not exists(select 1 from public.clients c where c.id=(x->>'client_id')::uuid))
    or exists(select 1 from jsonb_array_elements(v_candidates)x where nullif(x->>'candidate_id','') is not null
    and not exists(select 1 from public.candidates c where c.id=(x->>'candidate_id')::uuid and c.active)) then
    raise exception 'IMPORT_REVIEW_SCOPE_RESOLUTION_INVALID' using errcode='22023';
  end if;

  -- Serialize overlap decisions per route/scope. NHSP is a cross-client feed;
  -- HealthRoster is scoped by its immutable client list and Weekly/Daily route.
  perform pg_advisory_xact_lock(hashtextextended(concat_ws('|','IMPORT_REVIEW_OVERLAP',
    v_import.source_system::text,upper(coalesce(v_import.import_scope,v_import.source_system::text)),
    case when v_import.source_system='NHSP'::public.hr_source_enum then 'ALL_CLIENTS'
      else coalesce((select string_agg(coalesce(nullif(x->>'client_id',''),x->>'source_client_key'),','
        order by coalesce(nullif(x->>'client_id',''),x->>'source_client_key')) from jsonb_array_elements(v_clients)x),'NO_CLIENT') end),
    22072026));

  v_overlap:=public._import_review_overlap_preflight_core_v2(
    p_import_id,v_import.source_system,coalesce(v_import.import_scope,v_import.source_system::text),
    p_coverage_start_date,p_coverage_end_date,v_clients);

  if p_supersede_import_id is null then
    if jsonb_array_length(v_overlap)>0 then
      raise exception 'IMPORT_REVIEW_OVERLAP_CONFLICT' using errcode='55000',
        detail=jsonb_build_object('overlapping_unfinished_reviews',v_overlap)::text;
    end if;
  else
    if p_expected_supersede_state_version is null then
      raise exception 'IMPORT_REVIEW_REPLACE_VERSION_REQUIRED' using errcode='22023';
    end if;
    if jsonb_array_length(v_overlap)<>1
       or (v_overlap->0->>'import_id')::uuid is distinct from p_supersede_import_id then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_CONFLICT' using errcode='40001',
        detail=jsonb_build_object('overlapping_unfinished_reviews',v_overlap)::text;
    end if;
    select * into v_supersede_import from public.hr_imports where id=p_supersede_import_id for update;
    select * into v_supersede_state from public.import_review_states where import_id=p_supersede_import_id for update;
    if v_supersede_import.id is null or v_supersede_state.import_id is null then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_NOT_FOUND' using errcode='P0002';
    end if;
    if v_supersede_state.state_version<>p_expected_supersede_state_version then
      raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_supersede_state.state_version::text;
    end if;
    if v_supersede_state.status='APPLYING' then
      raise exception 'IMPORT_REVIEW_REPLACE_TARGET_APPLYING' using errcode='55000';
    end if;
    if v_supersede_state.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then
      raise exception 'IMPORT_REVIEW_REPLACE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;

  v_revision_group:=case when p_supersede_import_id is not null
    then coalesce(v_supersede_import.revision_group_id,v_supersede_import.id)
    else coalesce(v_import.revision_group_id,p_import_id) end;
  if p_supersede_import_id is null and v_import.revision_no is not null then v_revision_no:=v_import.revision_no;
  else select coalesce(max(hi.revision_no),0)+1 into v_revision_no from public.hr_imports hi where hi.revision_group_id=v_revision_group; end if;
  v_fingerprint:=public._import_review_hash_v1(jsonb_build_object('schema','IMPORT_REVIEW_COVERAGE_V1',
    'route',coalesce(v_import.import_scope,v_import.source_system::text),'mode',v_mode,
    'from',p_coverage_start_date,'to',p_coverage_end_date,
    'clients',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_client_key'),'client_id',x->>'client_id') order by x->>'source_client_key'),'[]') from jsonb_array_elements(v_clients)x),
    'candidates',(select coalesce(jsonb_agg(jsonb_build_object('key',btrim(x->>'source_candidate_key'),'candidate_id',x->>'candidate_id') order by x->>'source_candidate_key'),'[]') from jsonb_array_elements(v_candidates)x))::text);

  update public.hr_imports set revision_group_id=v_revision_group,revision_no=v_revision_no,
    supersedes_import_id=p_supersede_import_id,
    coverage_mode=v_mode,coverage_start_date=p_coverage_start_date,coverage_end_date=p_coverage_end_date,
    coverage_fingerprint=v_fingerprint,coverage_confirmed_by=p_actor_user_id,
    coverage_operation_key=v_operation_key,coverage_request_hash=v_request_hash
  where id=p_import_id;
  insert into public.import_review_scope_clients(import_id,source_client_key,source_display_label,client_id,created_by_user_id,resolved_at_utc,resolved_by_user_id)
  select p_import_id,btrim(x->>'source_client_key'),nullif(btrim(x->>'source_display_label'),''),nullif(x->>'client_id','')::uuid,p_actor_user_id,
    case when nullif(x->>'client_id','') is not null then now() end,case when nullif(x->>'client_id','') is not null then p_actor_user_id end
  from jsonb_array_elements(v_clients)x;
  insert into public.import_review_scope_candidates(import_id,source_candidate_key,source_display_label,candidate_id,created_by_user_id,resolved_at_utc,resolved_by_user_id)
  select p_import_id,btrim(x->>'source_candidate_key'),nullif(btrim(x->>'source_display_label'),''),nullif(x->>'candidate_id','')::uuid,p_actor_user_id,
    case when nullif(x->>'candidate_id','') is not null then now() end,case when nullif(x->>'candidate_id','') is not null then p_actor_user_id end
  from jsonb_array_elements(v_candidates)x;
  update public.hr_imports set coverage_locked_at=now() where id=p_import_id;
  insert into public.import_review_states(import_id,created_by_user_id,updated_by_user_id) values(p_import_id,p_actor_user_id,p_actor_user_id);
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(p_import_id,1,'REVIEW_CREATED',p_actor_user_id,jsonb_build_object('coverage_mode',v_mode,'coverage_fingerprint',v_fingerprint,
    'coverage_start_date',p_coverage_start_date,'coverage_end_date',p_coverage_end_date,'operation_key_hash',public._import_review_hash_v1(v_operation_key)));

  if p_supersede_import_id is not null then
    update public.import_review_states set status='SUPERSEDED',state_version=state_version+1,
      superseded_at_utc=now(),superseded_by_user_id=p_actor_user_id,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id
    where import_id=p_supersede_import_id returning * into v_supersede_state;
    insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
    values(p_supersede_import_id,v_supersede_state.state_version,'REVIEW_SUPERSEDED',p_actor_user_id,
      jsonb_build_object('new_import_id',p_import_id,'atomic_replace',true));
    insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
    values(p_import_id,1,'REVIEW_SUPERSEDES_PRIOR',p_actor_user_id,
      jsonb_build_object('old_import_id',p_supersede_import_id,'revision_group_id',v_revision_group,
        'revision_no',v_revision_no,'atomic_replace',true));
  end if;
  v_refresh:=public._import_review_refresh_core_v1(p_import_id,1,p_actor_user_id,5000);
  return v_refresh||jsonb_build_object('schema_contract_version','IMPORT_REVIEW_DB_V1',
    'replay',false,'coverage_fingerprint',v_fingerprint,'overlapping_unfinished_reviews','[]'::jsonb,
    'superseded_import_id',p_supersede_import_id,
    'superseded_state_version',case when p_supersede_import_id is not null then v_supersede_state.state_version end);
end
$function$;

-- _import_review_daily_resolution_guard_v1()
CREATE OR REPLACE FUNCTION public._import_review_daily_resolution_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  if tg_op='DELETE' then
    if old.status='APPLIED' then raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000'; end if;
    return old;
  end if;
  if new.import_id is distinct from old.import_id or new.hr_row_id is distinct from old.hr_row_id then
    raise exception 'IMPORT_REVIEW_RESOLUTION_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' and to_jsonb(new) is distinct from to_jsonb(old) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' then return new; end if;
  if not (case old.status when 'CURRENT' then new.status in ('CURRENT','STALE','CLEARED','APPLIED')
    when 'STALE' then new.status in ('STALE','CURRENT','CLEARED')
    when 'CLEARED' then new.status in ('CLEARED','CURRENT','STALE') else false end) then
    raise exception 'IMPORT_REVIEW_RESOLUTION_TRANSITION_INVALID' using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status='APPLIED' and (new.applied_operation_id is null or new.applied_at_utc is null) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_METADATA_REQUIRED' using errcode='23514';
  end if;
  return new;
end
$function$;

