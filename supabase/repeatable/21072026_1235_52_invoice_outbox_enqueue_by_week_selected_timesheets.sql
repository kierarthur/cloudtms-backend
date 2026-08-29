-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 25714d2fa2f8.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_outbox_enqueue_by_week(
  p_client_id uuid,
  p_invoice_week_start date,
  p_actor_user_id uuid,
  p_allow_early boolean default false,
  p_meta jsonb default '{}'::jsonb,
  p_timesheet_ids uuid[] default null,
  p_auto_invoice_only boolean default false
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;

  v_london_today date := (now() at time zone 'Europe/London')::date;
  v_week_end date := (p_invoice_week_start + interval '6 days')::date;

  v_has_due boolean := false;

  -- selection handling
  v_in_ids uuid[] := null;
  v_ok_ids uuid[] := null;
  v_sig text := null;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_nonseg_due_count int := null;
  v_dbg_seg_due_count int := null;
  v_dbg_nonseg_due_sample jsonb := '[]'::jsonb;
  v_dbg_seg_due_sample jsonb := '[]'::jsonb;
  v_dbg_input_ids_count int := null;
  v_dbg_ok_ids_count int := null;
  v_dbg_ok_ids_sig text := null;

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

  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  -- ------------------------------------------------------------
  -- Selection-mode input normalisation (UI-selected subset)
  -- ------------------------------------------------------------
  if p_timesheet_ids is not null then
    select array_agg(q.x order by q.x::text)
    into v_in_ids
    from (
      select distinct unnest(p_timesheet_ids) as x
    ) q
    where q.x is not null;

    if v_in_ids is null or coalesce(array_length(v_in_ids, 1), 0) = 0 then
      raise exception 'timesheet_ids[] required';
    end if;

    v_in_ids:=public._ctms_expand_correction_member_ids_v1(v_in_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_in_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK_SELECTION'
    );
    v_dbg_input_ids_count := coalesce(array_length(v_in_ids, 1), 0);
  end if;

  -- ------------------------------------------------------------
  -- ✅ Canonical eligibility resolution (returns deterministic timesheet_ids)
  -- Rules enforced (matches brief):
  --   - HR validation gate (required => VALIDATION_OK/OVERRIDDEN only; NULL blocked)
  --   - allow_early applies only to non-delayed items
  --   - delayed segments never early (target week start <= today only)
  --   - segments-empty treated as invoiceable if any charge evidence exists (defensive)
  --   - precheck must be OK (includes "expense-only no ref needed" behaviour)
  --   - optional UI selection filter (p_timesheet_ids)
  --   - optional auto-invoice-only filter (coalesce(contract.auto_invoice, client_settings.auto_invoice_default, false)=true)
  -- ------------------------------------------------------------
  select array_agg(q.timesheet_id order by q.timesheet_id::text)
  into v_ok_ids
  from (
    select distinct x.timesheet_id
    from (
      -- NON-SEGMENTS (or SEGMENTS-empty treated as NON-SEGMENTS): natural invoice week
      select tf.timesheet_id
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = tf.timesheet_id
      left join public.v_timesheets_summary_base vts
        on vts.timesheet_id = tf.timesheet_id

      -- auto-invoice context (only applied if p_auto_invoice_only=true)
      left join public.contract_weeks cw
        on cw.timesheet_id = tf.timesheet_id
      left join public.contracts c
        on c.id = coalesce(ts.contract_id, cw.contract_id)
      left join lateral (
        select cs0.auto_invoice_default
        from public.client_settings cs0
        where cs0.client_id = tf.client_id
          and (cs0.effective_from <= v_london_today or cs0.effective_from is null)
        order by cs0.effective_from desc nulls last
        limit 1
      ) cs on true

      where tf.is_current = true
        and tf.client_id = p_client_id
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and tf.locked_by_invoice_id is null
        and ts.revoked_at is null
        and upper(coalesce(pc.precheck_status,'')) = 'OK'

        -- ✅ optional selection filter
        and (v_in_ids is null or tf.timesheet_id = any(v_in_ids))

        -- ✅ auto-invoice filter (when enabled)
        and (
          p_auto_invoice_only is not true
          or coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
        )

        -- ✅ HR validation gate
        and not (
          coalesce(vts.hr_validation_required_for_invoice, false)
          and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
          and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
        )

        and (
          coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
          or (
            coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
            and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
            and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
            and (
                 coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.expenses_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.travel_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.accommodation_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.other_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.mileage_charge_ex_vat,0)::numeric <> 0
              or (
                case
                  when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
                  when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                    then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
                  else 0::numeric
                end
              ) <> 0
            )
          )
        )
        and (ts.week_ending_date::date - 6) = p_invoice_week_start
        and (p_allow_early = true or ts.week_ending_date::date < v_london_today)

      union all

      -- SEGMENTS mode: timesheet eligible if it has ≥1 eligible segment for this invoice week
      select tf.timesheet_id
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = tf.timesheet_id
      left join public.v_timesheets_summary_base vts
        on vts.timesheet_id = tf.timesheet_id

      -- auto-invoice context (only applied if p_auto_invoice_only=true)
      left join public.contract_weeks cw
        on cw.timesheet_id = tf.timesheet_id
      left join public.contracts c
        on c.id = coalesce(ts.contract_id, cw.contract_id)
      left join lateral (
        select cs0.auto_invoice_default
        from public.client_settings cs0
        where cs0.client_id = tf.client_id
          and (cs0.effective_from <= v_london_today or cs0.effective_from is null)
        order by cs0.effective_from desc nulls last
        limit 1
      ) cs on true

      cross join lateral jsonb_array_elements(
        case
          when jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' then tf.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) seg

      where tf.is_current = true
        and tf.client_id = p_client_id
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and tf.locked_by_invoice_id is null
        and ts.revoked_at is null
        and upper(coalesce(pc.precheck_status,'')) = 'OK'

        -- ✅ optional selection filter
        and (v_in_ids is null or tf.timesheet_id = any(v_in_ids))

        -- ✅ auto-invoice filter (when enabled)
        and (
          p_auto_invoice_only is not true
          or coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
        )

        -- ✅ HR validation gate
        and not (
          coalesce(vts.hr_validation_required_for_invoice, false)
          and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
          and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
        )

        and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
        and jsonb_typeof(seg) = 'object'
        and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
        and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null

        -- segment belongs to this invoice_week_start (target week, else natural week start)
        and coalesce(
              nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
              (ts.week_ending_date::date - 6)
            ) = p_invoice_week_start

        and (
          -- DELAYED segment: eligible only when delay has arrived (<= today), never by allow_early
          (
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is not null
            and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
            and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <= v_london_today
          )
          or
          -- NON-DELAYED segment: week gate applies; allow_early can override
          (
            (
              nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is null
              or nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
            )
            and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
          )
        )
    ) x
    where x.timesheet_id is not null
  ) q;

  v_has_due := (v_ok_ids is not null and coalesce(array_length(v_ok_ids, 1), 0) > 0);

  if v_invoice_debug then
    v_dbg_ok_ids_count := coalesce(array_length(v_ok_ids, 1), 0);
  end if;

  if not v_has_due then
    -- Mirror existing UX: if week hasn't passed and allow_early is false, show that message.
    if (p_allow_early is not true) and (v_week_end >= v_london_today) then
      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
            jsonb_build_object(
              'reason', 'week_not_passed_allow_early_false',
              'client_id', p_client_id::text,
              'invoice_week_start', p_invoice_week_start::text,
              'week_ending', v_week_end::text,
              'london_today', v_london_today::text,
              'allow_early', coalesce(p_allow_early,false),
              'auto_invoice_only', coalesce(p_auto_invoice_only,false),
              'input_ids_count', v_dbg_input_ids_count,
              'ok_ids_count', v_dbg_ok_ids_count,
              'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
              'run_finished_at_utc', public._inv_iso_utc(now())
            ),
            'invoice_jobs_outbox',
            ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
            null,
            'INVOICE_DEBUG',
            null, null, null
          );
        exception when others then
          null;
        end;
      end if;

      raise exception 'Week ending % has not passed (London today=%). Use allow_early to override.', v_week_end, v_london_today;
    end if;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REJECTED',
          jsonb_build_object(
            'reason', 'no_invoiceable_items',
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'auto_invoice_only', coalesce(p_auto_invoice_only,false),
            'input_ids_count', v_dbg_input_ids_count,
            'ok_ids_count', v_dbg_ok_ids_count,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoice_jobs_outbox',
          ('by_week:' || public._inv_iso_utc(v_dbg_run_started)),
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    raise exception 'No eligible timesheets/segments for client=% and invoice_week_start=%', p_client_id, p_invoice_week_start;
  end if;

  -- Strict selection parity: if user supplied ids, all must be eligible for this client/week/run.
  if v_in_ids is not null then
    if coalesce(array_length(v_ok_ids, 1), 0) <> coalesce(array_length(v_in_ids, 1), 0) then
      raise exception 'Some selected timesheets are not eligible or do not match client/week (client=% invoice_week_start=%)', p_client_id, p_invoice_week_start;
    end if;
  end if;

  perform public._ctms_assert_correction_invoice_scope_v1(
    v_ok_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK_ELIGIBLE'
  );

  -- Deterministic signature for idempotency / traceability
  v_sig := md5(array_to_string(v_ok_ids::text[], '|'));
  if v_invoice_debug then
    v_dbg_ok_ids_sig := v_sig;
  end if;

  -- Optional DEBUG: lightweight due breakdown samples (only when invoice_debug)
  if v_invoice_debug then
    begin
      -- NON-SEGMENTS-like eligible sample (subset)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_due_count,
        v_dbg_nonseg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        where tf.is_current = true
          and tf.timesheet_id = any(v_ok_ids)
          and (
            coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
            or (
              coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
              and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
              and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
            )
          )
      ) s;

      -- SEGMENTS eligible sample (segment-level sample; subset)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_seg_due_count,
        v_dbg_seg_due_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          coalesce(seg_el.value->>'label','') as label,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        cross join lateral jsonb_array_elements(
          case
            when jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' then tf.invoice_breakdown_json->'segments'
            else '[]'::jsonb
          end
        ) seg_el(value)
        where tf.is_current = true
          and tf.timesheet_id = any(v_ok_ids)
          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
      ) s;
    exception when others then
      null;
    end;
  end if;

  -- Build payload (canonical + deterministic)
  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text,
    'allow_early', coalesce(p_allow_early, false),
    'timesheet_ids', to_jsonb(v_ok_ids),
    'timesheet_ids_sig', v_sig,
    'auto_invoice_only', coalesce(p_auto_invoice_only, false)
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

  -- ✅ Concurrency guard: serialize enqueue per (client_id, invoice_week_start)
  perform pg_advisory_xact_lock(
    hashtext(p_client_id::text),
    (p_invoice_week_start - date '2000-01-01')::int
  );

  -- Idempotent: reuse existing outbox row if present
  select o.id
  into v_existing
  from public.invoice_jobs_outbox o
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    update public.invoice_jobs_outbox o
    set payload = coalesce(o.payload, '{}'::jsonb) || v_payload
    where o.id = v_existing;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_REUSED',
          jsonb_build_object(
            'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
            'run_finished_at_utc', public._inv_iso_utc(now()),
            'client_id', p_client_id::text,
            'invoice_week_start', p_invoice_week_start::text,
            'week_ending', v_week_end::text,
            'london_today', v_london_today::text,
            'allow_early', coalesce(p_allow_early,false),
            'auto_invoice_only', coalesce(p_auto_invoice_only,false),
            'input_ids_count', v_dbg_input_ids_count,
            'ok_ids_count', v_dbg_ok_ids_count,
            'ok_ids_sig', v_dbg_ok_ids_sig,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'nonseg_due_sample', v_dbg_nonseg_due_sample,
            'seg_due_sample', v_dbg_seg_due_sample,
            'existing_outbox_id', v_existing::text
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
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_OUTBOX_ENQUEUE_BY_WEEK_INSERTED',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'client_id', p_client_id::text,
          'invoice_week_start', p_invoice_week_start::text,
          'week_ending', v_week_end::text,
          'london_today', v_london_today::text,
          'allow_early', coalesce(p_allow_early,false),
          'auto_invoice_only', coalesce(p_auto_invoice_only,false),
          'input_ids_count', v_dbg_input_ids_count,
          'ok_ids_count', v_dbg_ok_ids_count,
          'ok_ids_sig', v_dbg_ok_ids_sig,
          'nonseg_due_count', v_dbg_nonseg_due_count,
          'seg_due_count', v_dbg_seg_due_count,
          'nonseg_due_sample', v_dbg_nonseg_due_sample,
          'seg_due_sample', v_dbg_seg_due_sample,
          'new_outbox_id', v_new::text
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
