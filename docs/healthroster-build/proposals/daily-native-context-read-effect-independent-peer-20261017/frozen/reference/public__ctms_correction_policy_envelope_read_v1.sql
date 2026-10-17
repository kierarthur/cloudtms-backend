create or replace function public._ctms_correction_policy_envelope_read_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
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