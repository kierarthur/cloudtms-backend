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