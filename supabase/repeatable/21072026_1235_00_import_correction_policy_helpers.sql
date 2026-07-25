-- Internal helpers for import-authoritative correction policy.
-- These helpers are deliberately owner-only. Public RPCs call them from an
-- explicit, guarded correction branch; ordinary/manual/QR/electronic rows are
-- returned unchanged.

create or replace function public._ctms_import_correction_classify_v1(
  p_timesheet_id uuid
)
returns jsonb
language sql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
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

create or replace function public._ctms_correction_policy_envelope_read_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

create or replace function public._ctms_correction_policy_leg_read_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

create or replace function public._ctms_invoice_vat_rate_for_timesheet_v1(
  p_timesheet_id uuid,
  p_ordinary_rate numeric
)
returns numeric
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

create or replace function public._ctms_expand_correction_member_ids_v1(
  p_timesheet_ids uuid[],
  p_max_members integer default 100
)
returns uuid[]
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

create or replace function public._ctms_expand_lifecycle_items_v1(
  p_items jsonb,
  p_action text,
  p_actor_user_id uuid,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_result jsonb := '[]'::jsonb;
  v_seen_correction_ids text[] := array[]::text[];
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
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

    v_transition := public.timesheet_correction_pair_transition_v1(
      v_id, v_action, p_actor_user_id, null::uuid, null::text, true, p_max_members
    );
    if coalesce((v_transition ->> 'valid')::boolean, false) is not true
       or coalesce((v_transition ->> 'action_ready')::boolean, false) is not true then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
        using errcode = 'P0001', detail = v_transition::text;
    end if;

    v_seen_correction_ids := array_append(v_seen_correction_ids, v_correction_id);
    v_result := v_result || coalesce(v_transition -> 'transition_items', '[]'::jsonb);
  end loop;

  if jsonb_array_length(v_result) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_EXPANSION_TOO_LARGE' using errcode = '22023';
  end if;
  return v_result;
end;
$function$;

create or replace function public._ctms_payload_timesheet_ids_v1(
  p_payload jsonb,
  p_max_members integer default 100
)
returns uuid[]
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
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

create or replace function public._ctms_assert_tsfin_snapshot_policy_v1(
  p_timesheet_id uuid,
  p_payload jsonb
)
returns void
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

create or replace function public._ctms_assert_tsfin_batch_units_v1(p_rows jsonb)
returns void
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
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

revoke all on function public._ctms_import_correction_classify_v1(uuid) from public, anon, authenticated, service_role;
revoke all on function public._ctms_correction_policy_envelope_read_v1(uuid) from public, anon, authenticated, service_role;
revoke all on function public._ctms_correction_policy_leg_read_v1(uuid) from public, anon, authenticated, service_role;
revoke all on function public._ctms_invoice_vat_rate_for_timesheet_v1(uuid, numeric) from public, anon, authenticated, service_role;
revoke all on function public._ctms_expand_correction_member_ids_v1(uuid[], integer) from public, anon, authenticated, service_role;
revoke all on function public._ctms_expand_lifecycle_items_v1(jsonb, text, uuid, integer) from public, anon, authenticated, service_role;
revoke all on function public._ctms_payload_timesheet_ids_v1(jsonb, integer) from public, anon, authenticated, service_role;
revoke all on function public._ctms_assert_tsfin_snapshot_policy_v1(uuid, jsonb) from public, anon, authenticated, service_role;
revoke all on function public._ctms_assert_tsfin_batch_units_v1(jsonb) from public, anon, authenticated, service_role;
