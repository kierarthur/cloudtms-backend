create or replace function public._ctms_correction_policy_leg_read_v1(
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