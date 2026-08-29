-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 3202b2e553ab.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_outbox_enqueue(
  p_kind text,
  p_payload jsonb,
  p_actor_user_id uuid default null,
  p_meta jsonb default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_kind text := upper(btrim(coalesce(p_kind,'')));
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_existing uuid;
  v_new uuid;
  v_client_id text;
  v_week_start text;
  v_sig text;
  v_scope_ids uuid[] := array[]::uuid[];

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();

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

  if v_kind = '' then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_REJECTED',
          jsonb_build_object(
            'reason', 'kind_required',
            'kind_input', p_kind,
            'payload_input', p_payload,
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('enqueue:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'kind is required';
  end if;

  if jsonb_typeof(v_payload) <> 'object' then
    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_REJECTED',
          jsonb_build_object(
            'reason', 'payload_not_object',
            'kind', v_kind,
            'payload_type', jsonb_typeof(v_payload),
            'payload_input', p_payload,
            'meta', p_meta,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('enqueue:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;
    raise exception 'payload must be a jsonb object';
  end if;

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

  v_scope_ids := public._ctms_payload_timesheet_ids_v1(v_payload,100);
  if cardinality(v_scope_ids)>0 and exists (
    select 1 from unnest(v_scope_ids) scoped(timesheet_id)
    where coalesce((public._ctms_import_correction_classify_v1(scoped.timesheet_id)
      ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    v_scope_ids := public._ctms_expand_correction_member_ids_v1(v_scope_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_scope_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_ENQUEUE'
    );
    if jsonb_typeof(v_payload->'timesheet_ids')='array' then
      v_payload:=jsonb_set(v_payload,'{timesheet_ids}',to_jsonb(v_scope_ids),true);
      v_payload:=jsonb_set(v_payload,'{timesheet_ids_sig}',to_jsonb(md5(array_to_string(v_scope_ids::text[],'|'))),true);
    end if;
    if jsonb_typeof(v_payload->'timesheetIds')='array' then
      v_payload:=jsonb_set(v_payload,'{timesheetIds}',to_jsonb(v_scope_ids),true);
    end if;
  end if;

  -- Best-effort idempotency
  if v_kind = 'BY_WEEK' then
    v_client_id := nullif(btrim(coalesce(v_payload->>'client_id','')), '');
    v_week_start := nullif(btrim(coalesce(v_payload->>'invoice_week_start','')), '');

    if v_client_id is not null and v_week_start is not null then
      -- ✅ Concurrency guard: serialize check+insert for (client_id, invoice_week_start)
      -- Uses hashtext on both text keys to avoid new casting failures / behaviour changes.
      perform pg_advisory_xact_lock(hashtext(v_client_id), hashtext(v_week_start));

      select o.id
      into v_existing
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = v_client_id
        and (o.payload->>'invoice_week_start') = v_week_start
      order by o.created_at desc
      limit 1;

      if v_existing is not null then
        if v_invoice_debug then
          begin
            perform public._inv_write_audit(
              p_actor_user_id,
              'INVOICE_OUTBOX_ENQUEUE_REUSED',
              jsonb_build_object(
                'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
                'run_finished_at_utc', public._inv_iso_utc(now()),
                'kind', v_kind,
                'client_id', v_client_id,
                'invoice_week_start', v_week_start,
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
    end if;

  elsif v_kind = 'HOURS' then
    v_sig := nullif(btrim(coalesce(v_payload->>'timesheet_ids_sig','')), '');

    if v_sig is not null then
      -- ✅ Concurrency guard: serialize check+insert for (kind=HOURS, timesheet_ids_sig)
      perform pg_advisory_xact_lock(hashtext(v_kind), hashtext(v_sig));

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
              'INVOICE_OUTBOX_ENQUEUE_REUSED',
              jsonb_build_object(
                'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
                'run_finished_at_utc', public._inv_iso_utc(now()),
                'kind', v_kind,
                'client_id', v_client_id,
                'invoice_week_start', v_week_start,
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
    end if;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values (v_kind, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'kind', v_kind,
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
