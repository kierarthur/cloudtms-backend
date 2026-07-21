-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: d695f16c6bb9.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_outbox_enqueue_hours(
  p_timesheet_ids uuid[],
  p_actor_user_id uuid,
  p_meta jsonb default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ids uuid[];
  v_sig text;
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;

  -- ✅ HR validation gating
  v_blocked_ids uuid[];

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_details jsonb := '{}'::jsonb;

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
  end;

  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids, 1), 0) = 0 then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REJECTED',
          jsonb_build_object(
            'reason', 'timesheet_ids_empty',
            'input_ids', to_jsonb(p_timesheet_ids),
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('hours:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'timesheet_ids[] required';
  end if;

  select array_agg(q.x order by q.x::text)
  into v_ids
  from (
    select distinct unnest(p_timesheet_ids) as x
  ) q
  where q.x is not null;

  if v_ids is null or coalesce(array_length(v_ids, 1), 0) = 0 then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REJECTED',
          jsonb_build_object(
            'reason', 'timesheet_ids_empty',
            'input_ids', to_jsonb(p_timesheet_ids),
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('hours:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'timesheet_ids[] required';
  end if;

  v_ids:=public._ctms_expand_correction_member_ids_v1(v_ids,100);
  perform public._ctms_assert_correction_invoice_scope_v1(
    v_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_ENQUEUE_HOURS'
  );

  -- ------------------------------------------------------------
  -- ✅ HR validation gating (reject entire request if any selected timesheet is blocked)
  -- Rule:
  --   If hr_validation_required_for_invoice = true, validation_status must be VALIDATION_OK or OVERRIDDEN.
  --   NULL validation_status is treated as blocked when required.
  -- ------------------------------------------------------------
  select array_agg(b.timesheet_id order by b.timesheet_id::text)
  into v_blocked_ids
  from (
    select vts.timesheet_id
    from public.v_timesheets_summary_base vts
    where vts.timesheet_id = any(v_ids)
      and coalesce(vts.hr_validation_required_for_invoice, false)
      and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
      and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
  ) b;

  if v_blocked_ids is not null and coalesce(array_length(v_blocked_ids, 1), 0) > 0 then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REJECTED',
          jsonb_build_object(
            'reason', 'hr_validation_not_passed',
            'blocked_timesheet_ids', to_jsonb(v_blocked_ids),
            'input_timesheet_ids', to_jsonb(v_ids),
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('hours:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    raise exception 'HR validation not passed for timesheet_ids: %', array_to_string(v_blocked_ids::text[], ',');
  end if;

  v_sig := md5(array_to_string(v_ids::text[], '|'));

  v_payload := jsonb_build_object(
    'timesheet_ids', to_jsonb(v_ids),
    'timesheet_ids_sig', v_sig
  );

  if p_actor_user_id is not null then
    v_payload := v_payload || jsonb_build_object('actor_user_id', p_actor_user_id::text);
  end if;

  if p_meta is not null then
    if jsonb_typeof(p_meta) = 'object' then
      v_payload := v_payload || p_meta;
    else
      v_payload := v_payload || jsonb_build_object('meta', p_meta);
    end if;
  end if;

  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'HOURS'
    and (o.payload->>'timesheet_ids_sig') = v_sig
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_HOURS_REUSED',
          jsonb_build_object(
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now()),
            'timesheet_ids_count', coalesce(array_length(v_ids,1),0),
            'timesheet_ids_sig', v_sig,
            'existing_outbox_id', v_existing::text,
            'payload', v_payload
          ),
          'invoice_jobs_outbox',
          v_existing::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('HOURS'::text, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_HOURS_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'timesheet_ids_count', coalesce(array_length(v_ids,1),0),
          'timesheet_ids_sig', v_sig,
          'new_outbox_id', v_new::text,
          'payload', v_payload
        ),
        'invoice_jobs_outbox',
        v_new::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_new;
end;
$$;
