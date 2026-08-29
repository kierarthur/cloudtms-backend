-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- This compatibility delegate remains part of the final invoice RPC suite. It
-- bounds raw JSON cardinality before expansion and forwards to the amended
-- canonical selected-timesheet overload.
create or replace function public.invoice_outbox_enqueue_by_week_selected(
  p_rows jsonb,
  p_actor_user_id uuid,
  p_allow_early boolean default false,
  p_meta jsonb default null
)
returns table (
  client_id uuid,
  invoice_week_start date,
  outbox_id uuid,
  action text
)
language plpgsql
security definer
set search_path = 'public', 'pg_temp'
as $$
declare
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_row jsonb;

  v_client_id uuid;
  v_week_start date;

  v_in_ids uuid[];

  v_existing_id uuid;
  v_outbox_id uuid;

  v_invoice_consolidation_mode text;

  v_meta_outer jsonb;
  v_meta_inner jsonb;
  v_total_timesheet_id_count integer := 0;
begin
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    raise exception 'p_rows must be a JSON array';
  end if;

  if coalesce(jsonb_array_length(p_rows), 0) = 0 then
    raise exception 'p_rows must not be empty';
  end if;

  if jsonb_array_length(p_rows) > 100 then
    raise exception 'INVOICE_SELECTED_ROWS_LIMIT_EXCEEDED'
      using errcode = '22023', detail = jsonb_build_object('max_rows', 100)::text;
  end if;

  for v_row in
    select t.value
    from jsonb_array_elements(p_rows) as t(value)
  loop
    if jsonb_typeof(v_row) <> 'object' then
      raise exception 'each element of p_rows must be a JSON object';
    end if;

    v_client_id := nullif(btrim(coalesce(v_row->>'client_id', '')), '')::uuid;
    v_week_start := (v_row->>'invoice_week_start')::date;

    if v_client_id is null then
      raise exception 'row missing client_id';
    end if;

    if v_week_start is null then
      raise exception 'row missing invoice_week_start';
    end if;

    if not (v_row ? 'timesheet_ids') then
      raise exception 'row missing timesheet_ids';
    end if;

    if jsonb_typeof(v_row->'timesheet_ids') <> 'array' then
      raise exception 'row timesheet_ids must be a JSON array';
    end if;

    if jsonb_array_length(v_row->'timesheet_ids') > 100 then
      raise exception 'INVOICE_SELECTED_TIMESHEET_IDS_LIMIT_EXCEEDED'
        using errcode = '22023',
              detail = jsonb_build_object('max_timesheet_ids_per_row', 100)::text;
    end if;

    v_total_timesheet_id_count := v_total_timesheet_id_count
      + jsonb_array_length(v_row->'timesheet_ids');

    if v_total_timesheet_id_count > 100 then
      raise exception 'INVOICE_SELECTED_TOTAL_TIMESHEET_IDS_LIMIT_EXCEEDED'
        using errcode = '22023',
              detail = jsonb_build_object('max_total_timesheet_ids', 100)::text;
    end if;

    if exists (
      select 1
      from jsonb_array_elements_text(v_row->'timesheet_ids') as raw_id
      where raw_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) then
      raise exception 'INVOICE_SELECTED_TIMESHEET_ID_INVALID'
        using errcode = '22023';
    end if;

    select array_agg(q.x order by q.x::text)
      into v_in_ids
    from (
      select distinct (t.val)::uuid as x
      from jsonb_array_elements_text(v_row->'timesheet_ids') as t(val)
    ) q;

    if v_in_ids is null or coalesce(array_length(v_in_ids, 1), 0) = 0 then
      raise exception 'row timesheet_ids empty/invalid';
    end if;

    select coalesce(cs0.invoice_consolidation_mode::text, 'NONE')
      into v_invoice_consolidation_mode
    from public.client_settings cs0
    where cs0.client_id = v_client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;

    if v_invoice_consolidation_mode is null then
      v_invoice_consolidation_mode := 'NONE';
    end if;

    v_meta_outer := coalesce(p_meta, '{}'::jsonb);

    if jsonb_typeof(v_meta_outer) <> 'object' then
      v_meta_outer := jsonb_build_object('meta', v_meta_outer);
    end if;

    v_meta_inner :=
      case
        when jsonb_typeof(v_meta_outer->'meta') = 'object'
          then coalesce(v_meta_outer->'meta', '{}'::jsonb)
        else '{}'::jsonb
      end;

    v_meta_inner := v_meta_inner
      || jsonb_build_object('invoice_consolidation_mode', v_invoice_consolidation_mode);

    v_meta_outer := v_meta_outer || jsonb_build_object('meta', v_meta_inner);

    perform pg_advisory_xact_lock(
      hashtext(v_client_id::text),
      (v_week_start - date '2000-01-01')::int
    );

    select o.id
      into v_existing_id
    from public.invoice_jobs_outbox o
    where o.kind = 'BY_WEEK'
      and (o.payload->>'client_id') = v_client_id::text
      and (o.payload->>'invoice_week_start') = v_week_start::text
    order by o.created_at desc
    limit 1;

    v_outbox_id := public.invoice_outbox_enqueue_by_week(
      p_client_id          => v_client_id,
      p_invoice_week_start => v_week_start,
      p_actor_user_id      => p_actor_user_id,
      p_allow_early        => p_allow_early,
      p_meta               => v_meta_outer,
      p_timesheet_ids      => v_in_ids,
      p_auto_invoice_only  => false
    );

    client_id := v_client_id;
    invoice_week_start := v_week_start;
    outbox_id := v_outbox_id;
    action := case when v_existing_id is null then 'INSERTED' else 'UPDATED' end;
    return next;
  end loop;
end;
$$;

comment on function public.invoice_outbox_enqueue_by_week_selected(jsonb, uuid, boolean, jsonb)
  is 'Compatibility batch delegate for selected weekly invoice enqueue; bounds raw input then calls the canonical seven-argument gate.';
