-- Immutable CloudTMS TEST function snapshot, page 07.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- invoice_enqueue_auto_invoice_ready(integer)
CREATE OR REPLACE FUNCTION public.invoice_enqueue_auto_invoice_ready(p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));

  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
begin
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    left join public.contract_weeks cw
      on cw.timesheet_id = tf.timesheet_id
    left join public.contracts c
      on c.id = coalesce(t.contract_id, cw.contract_id)
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true
    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and pc.week_ending_date < (select a.anchor_ymd from anchor a)
      and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
      and not (
        coalesce(vts.hr_validation_required_for_invoice, false)
        and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
        and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  if v_invoice_debug then
    begin
      with anchor as (
        select (now() at time zone 'Europe/London')::date as anchor_ymd
      ),
      eligible_ts as (
        select
          tf.client_id,
          (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
        from public.timesheets_financials tf
        join public.timesheets t
          on t.timesheet_id = tf.timesheet_id
         and t.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        left join public.contract_weeks cw
          on cw.timesheet_id = tf.timesheet_id
        left join public.contracts c
          on c.id = coalesce(t.contract_id, cw.contract_id)
        left join lateral (
          select cs0.auto_invoice_default
          from public.client_settings cs0
          cross join anchor a
          where cs0.client_id = tf.client_id
            and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
          order by cs0.effective_from desc nulls last
          limit 1
        ) cs on true
        where tf.is_current = true
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and t.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and pc.week_ending_date < (select a.anchor_ymd from anchor a)
          and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select a.anchor_ymd from anchor a),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select a2.n from already a2)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_AUTO_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_ins;
end;
$function$;

-- invoice_enqueue_ready_for_invoice(integer)
CREATE OR REPLACE FUNCTION public.invoice_enqueue_ready_for_invoice(p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));

  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
begin
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true
    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and pc.week_ending_date < (select a.anchor_ymd from anchor a)
      and coalesce(cs.auto_invoice_default, false) = true
      and (
        coalesce(vts.client_hr_validation_required, false) = false
        or vts.validation_status = any(array[
          'VALIDATION_OK'::public.validation_status_enum,
          'OVERRIDDEN'::public.validation_status_enum
        ])
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  if v_invoice_debug then
    begin
      with anchor as (
        select (now() at time zone 'Europe/London')::date as anchor_ymd
      ),
      eligible_ts as (
        select
          tf.client_id,
          (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
        from public.timesheets_financials tf
        join public.timesheets t
          on t.timesheet_id = tf.timesheet_id
         and t.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        left join lateral (
          select cs0.auto_invoice_default
          from public.client_settings cs0
          cross join anchor a
          where cs0.client_id = tf.client_id
            and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
          order by cs0.effective_from desc nulls last
          limit 1
        ) cs on true
        where tf.is_current = true
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and t.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and pc.week_ending_date < (select a.anchor_ymd from anchor a)
          and coalesce(cs.auto_invoice_default, false) = true
          and (
            coalesce(vts.client_hr_validation_required, false) = false
            or vts.validation_status = any(array[
              'VALIDATION_OK'::public.validation_status_enum,
              'OVERRIDDEN'::public.validation_status_enum
            ])
          )
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select a.anchor_ymd from anchor a),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select a2.n from already a2)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_ins;
end;
$function$;

-- invoice_generate_from_outbox_batch__stub(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_generate_from_outbox_batch__stub(p_outbox_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(outbox_id uuid, ok boolean, invoice_ids uuid[], warnings jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  raise exception
    'invoice_generate_from_outbox_batch not implemented: your uploaded BACKEND FOR CLOUDTMS.js contains redacted "..." inside the invoice builders. Paste unredacted handleCreateInvoiceTsfin / handleCreateInvoiceTsfinByWeek / extractBillableSegmentsForWeek so SQL can be mirrored exactly.';
end;
$function$;

-- invoice_generate_from_outbox_batch(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_generate_from_outbox_batch(p_outbox_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(outbox_id uuid, ok boolean, invoice_ids uuid[], warnings jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_consol_mode text := 'NONE';
  v_payload_consol_mode text := null;
  v_outbox_invoice_ids uuid[] := array[]::uuid[];
  v_outbox_warnings jsonb := '[]'::jsonb;
  v_entries_all jsonb := '[]'::jsonb;
  grp record;

  v_outbox_id uuid;
  v_job record;

  v_kind text;
  v_payload jsonb;

  v_ip text;
  v_ua text;
  v_corr text;

  v_now timestamptz;
  v_anchor_ymd date;

  v_invoice_id uuid;

  -- shared defaults
  v_default_labels jsonb := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');

  -- contract week enum sanity
  v_has_invoiced boolean;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_results jsonb := '[]'::jsonb;

  -- last loaded outbox row meta (avoid touching unassigned v_job record in exception paths)
  v_dbg_job_attempt_count int := null;
  v_dbg_job_next_attempt_at timestamptz := null;
  v_dbg_job_last_error text := null;
  v_dbg_job_created_at timestamptz := null;
  v_dbg_first_ip text := null;
  v_dbg_first_ua text := null;
  v_dbg_first_corr text := null;

begin
  if p_outbox_ids is null or coalesce(array_length(p_outbox_ids,1),0) = 0 then
    return;
  end if;

  select ('INVOICED' = any(enum_range(null::public.contract_week_status_enum)::text[]))
  into v_has_invoiced;

  if not v_has_invoiced then
    raise exception 'contract_week_status_enum does not contain INVOICED; cannot mirror setWeeksInvoicedForTimesheets.';
  end if;

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

  foreach v_outbox_id in array p_outbox_ids loop
    begin
      v_invoice_id := null;
      v_now := now();
      v_anchor_ymd := (v_now at time zone 'Europe/London')::date;

      -- reset per-outbox debug meta
      v_dbg_job_attempt_count := null;
      v_dbg_job_next_attempt_at := null;
      v_dbg_job_last_error := null;
      v_dbg_job_created_at := null;


      select *
      into v_job
      from public.invoice_jobs_outbox
      where id = v_outbox_id
      for update;

      if not found then
        outbox_id := v_outbox_id;
        ok := false;
        invoice_ids := null;
        warnings := jsonb_build_object('error','outbox row not found');
        if v_invoice_debug then
          v_dbg_results := v_dbg_results || jsonb_build_array(
            jsonb_build_object(
              'outbox_id', v_outbox_id::text,
              'kind', null,
              'payload', null,
              'result', jsonb_build_object(
                'ok', ok,
                'invoice_ids', null,
                'warnings', warnings
              ),
              'job_row', null,
              'timing', jsonb_build_object(
                'now_utc', public._inv_iso_utc(v_now),
                'anchor_ymd', v_anchor_ymd::text
              )
            )
          );
        end if;
        return next;
        continue;
      end if;

      v_kind := upper(coalesce(v_job.kind,''));
      v_payload := coalesce(v_job.payload, '{}'::jsonb);

      -- stash outbox row meta for debug (safe scalars)
      v_dbg_job_attempt_count := v_job.attempt_count;
      v_dbg_job_next_attempt_at := v_job.next_attempt_at;
      v_dbg_job_last_error := v_job.last_error;
      v_dbg_job_created_at := v_job.created_at;

      -- Optional audit meta from payload (SQL has no req headers)
      v_ip   := nullif(btrim(coalesce(v_payload->>'ip','')), '');
      v_ua   := nullif(btrim(coalesce(v_payload->>'user_agent','')), '');
      v_corr := nullif(btrim(coalesce(v_payload->>'correlation_id','')), '');

      -- ======================================================
      -- KIND: HOURS  (mirror handleCreateInvoiceTsfin)
      -- ======================================================
      if v_kind = 'HOURS' then
        -- Parse timesheet_ids from payload
       declare
  v_ts_ids uuid[];
  v_ts_ids_to_use uuid[];
  v_client_id uuid;
          v_client record;
          v_def record;
          v_cs record;
          v_global_vat numeric := 20;
          v_vat_rate numeric := 20;
          v_ordinary_vat_rate numeric := 20;

          v_terms_days int;
          v_due_at timestamptz;

          v_requires_hr_any boolean := false;
          v_hr_attach_any boolean := false;
          v_ts_attach_any boolean := false;
          v_hr_attach_default boolean := true;
          v_ts_attach_default boolean := true;

          v_stationery_key text;
          v_margins jsonb := jsonb_build_object('top',32,'right',12,'bottom',20,'left',12);
          v_hide_bank_footer boolean := true;

          v_header jsonb;

          v_used_ts_ids uuid[];
          v_lock_iso text := public._inv_iso_utc(v_now);

              v_sum_ex numeric := 0;
      v_sum_vat numeric := 0;
      v_sum_inc numeric := 0;

      -- run-level totals audit
      v_prev_ex numeric := 0;
      v_prev_vat numeric := 0;
      v_prev_inc numeric := 0;
      v_prev_status text := null;
      v_prev_invoice_no text := null;

      v_new_ex numeric := 0;
      v_new_vat numeric := 0;
      v_new_inc numeric := 0;

      v_delta_ex numeric := 0;
      v_delta_vat numeric := 0;
      v_delta_inc numeric := 0;

      v_run_ts_ids uuid[] := null;
      v_run_source_keys text[] := null;
      v_run_line_count int := 0;

      s record;
      t record;
      c record;
                cand_display text;

          contract_id uuid;
          labels jsonb;

          c_daily_calc boolean := false;
          c_bucket_labels jsonb := null;
          c_role text := null;
          c_display_site text := null;
          c_ward_hint text := null;


          schedule_refs jsonb;
          schedule_ref_count int;

          wants_daily boolean;
          can_daily boolean;

          segments jsonb;
          has_date boolean;

          -- daily byDate loop vars
          d_rec record;

          -- additional iterators
          kv record;
          code text;
          ex jsonb;
          bucket_name text;
          unit_name text;
          pay_rate numeric;
          charge_rate numeric;
          days_obj jsonb;

          any_daily_add boolean;

                unit_count numeric;
          charge_ex numeric;
          pay_ex numeric;

          line_desc text;
          meta jsonb;

          base_pay_ex numeric;
          base_chg_ex numeric;
          margin_ex numeric;
          vat_amt numeric;
          inc_amt numeric;

          v_line_source_key text;

          disallowed boolean;

        begin
          select array_agg(distinct (x)::uuid)
          into v_ts_ids
          from (
            select jsonb_array_elements_text(v_payload->'timesheet_ids') as x
          ) q
          where (q.x ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

          if v_ts_ids is null or coalesce(array_length(v_ts_ids,1),0) = 0 then
            raise exception 'HOURS job requires payload.timesheet_ids[]';
          end if;

          -- eligible snaps (locked null, is_current, READY_FOR_INVOICE) and precheck OK + schedule-aware refs if required
          -- disallow NHSP/HR self-bill bases
            declare
  v_snap_cnt int := 0;
  v_client_cnt int := 0;
  v_has_disallowed boolean := false;
  v_has_segments_mode boolean := false;
begin
  with snaps_all as (
    select tf.*
    from public.timesheets_financials tf
    where tf.timesheet_id = any(v_ts_ids)
      and tf.is_current = true
      and tf.locked_by_invoice_id is null
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  )
  select
    count(*)::int,
    count(distinct client_id)::int,

    -- ✅ FIX: Postgres has no min(uuid). Pick a deterministic client_id instead.
    (
      select sa.client_id
      from snaps_all sa
      where sa.client_id is not null
      order by sa.client_id asc
      limit 1
    ) as picked_client_id,

    exists(
      select 1
      from snaps_all sa
      where upper(coalesce(sa.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT')
    ),

    exists(
      select 1
      from snaps_all sa
      where coalesce(sa.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
    )
  into v_snap_cnt, v_client_cnt, v_client_id, v_has_disallowed, v_has_segments_mode
  from snaps_all;

  if v_snap_cnt = 0 then
    raise exception 'No eligible timesheets (need READY_FOR_INVOICE & unlocked).';
  end if;

  if v_has_disallowed then
    raise exception 'This endpoint cannot invoice NHSP or HR self-bill timesheets (use BY_WEEK).';
  end if;

  if v_has_segments_mode then
    raise exception 'This endpoint cannot invoice SEGMENTS-mode timesheets (use BY_WEEK).';
  end if;

  if v_client_cnt <> 1 then
    raise exception 'Expected exactly one client across snapshots.';
  end if;

  if v_client_id is null then
    raise exception 'No eligible timesheets (need READY_FOR_INVOICE & unlocked).';
  end if;
end;


          select array_agg(distinct tf.timesheet_id)
          into v_ts_ids_to_use
          from public.timesheets_financials tf
          join public.timesheets ts on ts.timesheet_id = tf.timesheet_id
          join public.v_ts_invoice_precheck pc on pc.timesheet_id = tf.timesheet_id
          where tf.timesheet_id = any(v_ts_ids)
            and tf.is_current = true
            and tf.locked_by_invoice_id is null
            and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
            and upper(coalesce(pc.precheck_status,'')) = 'OK'
            and (
              pc.require_reference_to_invoice is not true
              or public._inv_timesheet_has_invoice_reference(
                    ts.sheet_scope::text,
                    coalesce(ts.submission_mode::text,''),
                    ts.reference_number,
                    ts.day_references_json,
                    ts.actual_schedule_json
                 )
            );

          if v_ts_ids_to_use is null or coalesce(array_length(v_ts_ids_to_use,1),0) = 0 then
            raise exception 'No eligible timesheets after contract-resolved invoice precheck/reference gating.';
          end if;

          -- Load client + defaults
          select id, name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
          into v_client
          from public.clients
          where id = v_client_id;


          if not found then
            raise exception 'Client not found for snapshots.';
          end if;

                   select bank_name, bank_sort_code, bank_account_number, vat_registration_number,
                 hr_attach_to_invoice, ts_attach_to_invoice,
                 agency_name, agency_logo, registered_address, company_reg_number
          into v_def
          from public.settings_defaults
          where id = 1;


          -- Finance VAT (anchor is invoice creation time, Europe/London ymd)
        select coalesce(sf.vat_rate_pct, 20)
into v_global_vat
from public.settings_finance_pick(v_anchor_ymd) sf
where (sf.date_from is null or sf.date_from <= v_anchor_ymd)
  and (sf.date_to   is null or sf.date_to   >= v_anchor_ymd)
order by sf.date_from desc nulls last
limit 1;


          -- client_settings: HOURS uses effective_from<=anchor OR effective_from IS NULL (fallback row)
          select cs.client_id, cs.vat_rate_pct, cs.requires_hr, cs.hr_attach_to_invoice, cs.ts_attach_to_invoice, cs.invoice_consolidation_mode, cs.effective_from
          into v_cs
          from public.client_settings cs
          where cs.client_id = v_client_id
            and (cs.effective_from <= v_anchor_ymd or cs.effective_from is null)
          order by cs.effective_from desc nulls last
          limit 1;

          v_vat_rate :=
            case when v_client.vat_chargeable = false then 0
                 else coalesce(v_cs.vat_rate_pct, v_global_vat, 20)
            end;
          v_ordinary_vat_rate := v_vat_rate;

          v_hr_attach_default :=
            case when found and v_cs.hr_attach_to_invoice is false then false else true end;

          v_ts_attach_default :=
            case when found and v_cs.ts_attach_to_invoice is false then false else true end;

          v_terms_days := coalesce(v_client.payment_terms_days, 30);
          v_due_at := v_now + make_interval(days => v_terms_days);

                   -- Contract mapping via contract_weeks only (matches JS HOURS endpoint)
          -- Aggregate requires_hr per contract
           with ts_map as (
            select distinct tf.timesheet_id, cw.contract_id
            from public.timesheets_financials tf
            left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
            join public.v_ts_invoice_precheck pc on pc.timesheet_id = tf.timesheet_id
            join public.timesheets ts on ts.timesheet_id = tf.timesheet_id
                      where tf.timesheet_id = any(v_ts_ids_to_use)
              and tf.is_current=true
              and tf.locked_by_invoice_id is null
              and tf.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
              and upper(coalesce(pc.precheck_status,''))='OK'
              and (
                pc.require_reference_to_invoice is not true
                or public._inv_timesheet_has_invoice_reference(
                      ts.sheet_scope::text,
                      coalesce(ts.submission_mode::text,''),
                      ts.reference_number,
                      ts.day_references_json,
                      ts.actual_schedule_json
                   )
              )

          ),
               cons as (
            select ctr.*
            from public.contracts ctr
            where ctr.id in (
              select distinct tm.contract_id
              from ts_map tm
              where tm.contract_id is not null
            )
          )

          select
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.requires_hr, false)
                  else coalesce(v_cs.requires_hr, false)
                end
              ),
              coalesce(v_cs.requires_hr, false),
              false
            ) as req_hr,
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
                  else coalesce(v_cs.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
                end
              ),
              coalesce(v_cs.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
            ) as hr_attach_any,
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
                  else coalesce(v_cs.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
                end
              ),
              coalesce(v_cs.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
            ) as ts_attach_any
          into v_requires_hr_any, v_hr_attach_any, v_ts_attach_any
          from cons;




          -- Stationery for HOURS: default key from your JS (with PDF→PNG swap), unless payload provides override
          v_stationery_key :=
            nullif(btrim(coalesce(v_payload->>'stationery_key','')), '');

          if v_stationery_key is null then
            v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
          end if;

          if right(lower(v_stationery_key),4) = '.pdf' then
            v_stationery_key := left(v_stationery_key, length(v_stationery_key)-4) || '@300dpi.png';
          end if;
                 v_header := jsonb_build_object(
            'client_id', v_client_id::text,
            'client_name', v_client.name,
            'client_invoice_address', v_client.invoice_address,
            'client_primary_invoice_email', v_client.primary_invoice_email,
            'agency_name', v_def.agency_name,
            'agency_logo', v_def.agency_logo,
            'agency_logo_url', v_def.agency_logo,
            'registered_address', v_def.registered_address,
            'company_reg_number', v_def.company_reg_number,
            'company_registration_number', v_def.company_reg_number,
            'vat_chargeable', coalesce(v_client.vat_chargeable,true),
            'applied_vat_rate_pct', v_vat_rate,
            'payment_terms_days', v_terms_days,
            'issued_at_utc', null,
            'due_at_utc', null,
            'stationery_key', v_stationery_key,
            'stationery_margins_mm', v_margins,
            'hide_bank_footer', v_hide_bank_footer,
            'bank', jsonb_build_object(
              'name', v_def.bank_name,
              'sort_code', v_def.bank_sort_code,
              'account_number', v_def.bank_account_number
            ),
            'vat_registration_number', v_def.vat_registration_number,
            'meta', jsonb_build_object('source','TSFIN','timesheet_count', coalesce(array_length(v_ts_ids_to_use,1),0)),

            'attach_policy', jsonb_build_object(
              'requires_hr', coalesce(v_requires_hr_any,false),
              'hr_attach_to_invoice', coalesce(v_hr_attach_any, true),
              'ts_attach_to_invoice', coalesce(v_ts_attach_any, true)
            )
          );



            insert into public.invoices(
        client_id, status, issued_at_utc, due_at_utc,
        subtotal_ex_vat, vat_amount, total_inc_vat,
        header_snapshot_json
      )
      values (
        v_client_id,
        'DRAFT'::public.invoice_status_enum,
        null,
        null,
        0,0,0,
        v_header
      )
      returning id into v_invoice_id;

      -- record invoice for this outbox job
      v_outbox_invoice_ids := array_append(v_outbox_invoice_ids, v_invoice_id);
-- Track what THIS run actually inserted (so we can audit per-run delta)
      create temporary table if not exists pg_temp._inv_run_lines (
        timesheet_id uuid,
        source_key text,
        charge_ex numeric,
        vat_amount numeric,
        inc_amount numeric
      ) on commit drop;

      truncate pg_temp._inv_run_lines;

      -- Capture invoice totals/status BEFORE this run applies any changes
      select
        i.invoice_no,
        i.status::text,
        coalesce(i.subtotal_ex_vat,0)::numeric,
        coalesce(i.vat_amount,0)::numeric,
        coalesce(i.total_inc_vat,0)::numeric
      into
        v_prev_invoice_no,
        v_prev_status,
        v_prev_ex,
        v_prev_vat,
        v_prev_inc
      from public.invoices i
      where i.id = v_invoice_id
      limit 1;

      -- AUDIT: INVOICE_CREATED (entity 'invoices')
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_CREATED',
        jsonb_build_object(
          'invoice_id', v_invoice_id::text,
          'client_id', v_client_id::text,
          'timesheet_ids', to_jsonb(v_ts_ids_to_use),
          'status', 'DRAFT'
        ),
        'invoices',
        v_invoice_id::text,
        null,
        null,
        v_ip, v_ua, v_corr
      );


          -- Build lines per eligible snap (mirror JS)
          for s in
            select tf.*, ts.booking_id, ts.week_ending_date, ts.reference_number,
                   ts.sheet_scope::text as sheet_scope,
                   coalesce(ts.submission_mode::text,'') as submission_mode,
                   ts.day_references_json,
                   ts.actual_schedule_json,
                   cw.contract_id
            from public.timesheets_financials tf
            join public.timesheets ts on ts.timesheet_id = tf.timesheet_id
            left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
            join public.v_ts_invoice_precheck pc on pc.timesheet_id = tf.timesheet_id
                        where tf.timesheet_id = any(v_ts_ids_to_use)
              and tf.is_current=true
              and tf.locked_by_invoice_id is null
              and tf.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
              and upper(coalesce(pc.precheck_status,''))='OK'
              and (
                pc.require_reference_to_invoice is not true
                or public._inv_timesheet_has_invoice_reference(
                      ts.sheet_scope::text,
                      coalesce(ts.submission_mode::text,''),
                      ts.reference_number,
                      ts.day_references_json,
                      ts.actual_schedule_json
                   )
              )

          loop
            v_vat_rate:=public._ctms_invoice_vat_rate_for_timesheet_v1(
              s.timesheet_id,v_ordinary_vat_rate
            );
                          contract_id := s.contract_id;

            c_daily_calc := false;
            c_bucket_labels := null;
            c_role := null;
            c_display_site := null;
            c_ward_hint := null;

            if contract_id is not null then
              select
                coalesce(daily_calc_of_invoices,false),
                bucket_labels_json,
                role,
                display_site,
                ward_hint
              into c_daily_calc, c_bucket_labels, c_role, c_display_site, c_ward_hint
              from public.contracts
              where id = contract_id;
            end if;

            labels := case
              when c_bucket_labels is not null and jsonb_typeof(c_bucket_labels)='object'
                then c_bucket_labels
              else v_default_labels
            end;

                                              cand_display := null;

            select nullif(btrim(coalesce(cd.display_name,'')),'')
            into cand_display
            from public.candidates cd
            where cd.id = s.candidate_id;

            if cand_display is null and s.candidate_id is not null then
              cand_display := 'Candidate ' || substr(s.candidate_id::text,1,8) || '…';
            end if;


            schedule_refs := public._inv_collect_weekly_manual_schedule_refs(
              s.sheet_scope, s.submission_mode, s.actual_schedule_json
            );
            schedule_ref_count := case when jsonb_typeof(schedule_refs)='array' then jsonb_array_length(schedule_refs) else 0 end;

            wants_daily := c_daily_calc;



            -- segments eligibility for daily
            segments := null;
            has_date := false;

            if wants_daily
               and s.invoice_breakdown_json is not null
               and jsonb_typeof(s.invoice_breakdown_json)='object'
               and upper(coalesce(s.invoice_breakdown_json->>'mode',''))='SEGMENTS'
               and jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
               and jsonb_array_length(s.invoice_breakdown_json->'segments') > 0
            then
              select exists(
                select 1
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') seg
                where (seg->>'date') ~ '^\d{4}-\d{2}-\d{2}'
              ) into has_date;

              if has_date then
                segments := s.invoice_breakdown_json->'segments';
              end if;
            end if;

            can_daily := wants_daily and segments is not null;

            -- -------------------------
            -- HOURS lines
            -- -------------------------
            if can_daily then
              for d_rec in
                with segs as (
                  select
                    left(seg->>'date',10) as ymd,
                    coalesce((seg->>'hours_day')::numeric,0)   as h_day,
                    coalesce((seg->>'hours_night')::numeric,0) as h_night,
                    coalesce((seg->>'hours_sat')::numeric,0)   as h_sat,
                    coalesce((seg->>'hours_sun')::numeric,0)   as h_sun,
                    coalesce((seg->>'hours_bh')::numeric,0)    as h_bh,
                    coalesce((seg->>'pay_amount')::numeric,0)  as pay_ex,
                    coalesce((seg->>'charge_amount')::numeric,0) as chg_ex
                  from jsonb_array_elements(segments) seg
                  where left(coalesce(seg->>'date',''),10) ~ '^\d{4}-\d{2}-\d{2}$'
                ),
                agg as (
                  select
                    ymd,
                    sum(h_day)::numeric as hours_day,
                    sum(h_night)::numeric as hours_night,
                    sum(h_sat)::numeric as hours_sat,
                    sum(h_sun)::numeric as hours_sun,
                    sum(h_bh)::numeric as hours_bh,
                    sum(pay_ex)::numeric as pay_ex,
                    sum(chg_ex)::numeric as chg_ex
                  from segs
                  group by ymd
                )
                select *
                from agg
                order by ymd
              loop
                -- Allow zero/negative charges and hours: adjustment/reversal lines must be invoiceable
                charge_ex := public._inv_round2(d_rec.chg_ex);

                pay_ex := public._inv_round2(d_rec.pay_ex);
                margin_ex := public._inv_round2(charge_ex - pay_ex);
                vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(charge_ex + vat_amt);


                              v_sum_ex := v_sum_ex + charge_ex;
                v_sum_vat := v_sum_vat + vat_amt;
                v_sum_inc := v_sum_inc + inc_amt;

                line_desc := 'Timesheet ' || s.timesheet_id::text || ' – ' || d_rec.ymd;

                meta := jsonb_build_object(
                  'line_type','HOURS_DAILY',
                  'timesheet_id', s.timesheet_id::text,
                  'timesheet_version', s.timesheet_version,
                  'booking_id', s.booking_id,
                  'candidate_display', cand_display,
                  'role', c_role,
                  'hospital', c_display_site,
                  'ward', c_ward_hint,

                  'week_ending_date', s.week_ending_date::text,
                  'date', d_rec.ymd,
                  'ts_reference_number', s.reference_number,
                  'schedule_ref_nums', schedule_refs,
                  'schedule_ref_count', schedule_ref_count,
                  'policy_snapshot_json', s.policy_snapshot_json,
                  'rate_source_refs_json', s.rate_source_refs_json,
                  'bucket_labels', labels,
                  'hours_day', public._inv_round2(d_rec.hours_day),
                  'hours_night', public._inv_round2(d_rec.hours_night),
                  'hours_sat', public._inv_round2(d_rec.hours_sat),
                  'hours_sun', public._inv_round2(d_rec.hours_sun),
                  'hours_bh', public._inv_round2(d_rec.hours_bh),
                  'breakdown', jsonb_build_object('hours', jsonb_build_object(
                    'day', public._inv_round2(d_rec.hours_day),
                    'night', public._inv_round2(d_rec.hours_night),
                    'sat', public._inv_round2(d_rec.hours_sat),
                    'sun', public._inv_round2(d_rec.hours_sun),
                    'bh', public._inv_round2(d_rec.hours_bh)
                  )),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', charge_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || s.timesheet_id::text || ':HOURS:' || d_rec.ymd;

                              insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                  public._inv_round2(d_rec.hours_day),
                  public._inv_round2(d_rec.hours_night),
                  public._inv_round2(d_rec.hours_sat),
                  public._inv_round2(d_rec.hours_sun),
                  public._inv_round2(d_rec.hours_bh),
                  null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                  pay_ex, charge_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
                end if;


                    end loop;
            else
                         -- weekly fallback (CORE HOURS ONLY: exclude additional + expenses + mileage)
              base_pay_ex := public._inv_round2(
                coalesce(s.total_pay_ex_vat,0)
                - coalesce(s.additional_pay_ex_vat,0)
                - coalesce(s.expenses_pay_ex_vat,0)
                - coalesce(s.mileage_pay_ex_vat,0)
              );


              base_chg_ex := public._inv_round2(
                coalesce(s.total_charge_ex_vat,0)
                - coalesce(s.additional_charge_ex_vat,0)
                - coalesce(s.expenses_charge_ex_vat,0)
                - coalesce(s.mileage_charge_ex_vat,0)
              );              -- Core HOURS line must be invoiceable even when charge/hours are zero or negative (adjustments/reversals)
                  margin_ex := public._inv_round2(base_chg_ex - base_pay_ex);
                  vat_amt := public._inv_round2(base_chg_ex * v_vat_rate / 100);
                  inc_amt := public._inv_round2(base_chg_ex + vat_amt);

                  v_sum_ex := v_sum_ex + base_chg_ex;
                  v_sum_vat := v_sum_vat + vat_amt;
                  v_sum_inc := v_sum_inc + inc_amt;

                  line_desc := 'Timesheet ' || s.timesheet_id::text ||
                    case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

                  meta := jsonb_build_object(
                    'line_type','HOURS',
                    'timesheet_id', s.timesheet_id::text,
                    'timesheet_version', s.timesheet_version,
                    'booking_id', s.booking_id,
                    'candidate_display', cand_display,
                    'role', c_role,
                    'hospital', c_display_site,
                    'ward', c_ward_hint,
                    'week_ending_date', s.week_ending_date::text,
                    'ts_reference_number', s.reference_number,
                    'schedule_ref_nums', schedule_refs,
                    'schedule_ref_count', schedule_ref_count,
                    'policy_snapshot_json', s.policy_snapshot_json,
                    'rate_source_refs_json', s.rate_source_refs_json,
                    'bucket_labels', labels,
                    'hours_day', coalesce(s.hours_day,0),
                    'hours_night', coalesce(s.hours_night,0),
                    'hours_sat', coalesce(s.hours_sat,0),
                    'hours_sun', coalesce(s.hours_sun,0),
                    'hours_bh', coalesce(s.hours_bh,0),
                    'breakdown', jsonb_build_object('hours', jsonb_build_object(
                      'day', coalesce(s.hours_day,0),
                      'night', coalesce(s.hours_night,0),
                      'sat', coalesce(s.hours_sat,0),
                      'sun', coalesce(s.hours_sun,0),
                      'bh', coalesce(s.hours_bh,0)
                    )),
                    'totals', jsonb_build_object(
                      'line_pay_ex_vat', base_pay_ex,
                      'line_charge_ex_vat', base_chg_ex,
                      'margin_ex_vat', margin_ex,
                      'vat_rate_pct', v_vat_rate,
                      'vat_amount', vat_amt,
                      'total_inc_vat', inc_amt
                    )
                  );

                  v_line_source_key := 'TS:' || s.timesheet_id::text || ':HOURS:WEEK';

                  insert into public.invoice_lines(
                    invoice_id, timesheet_id, booking_id, description,
                    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                    vat_rate_pct, vat_amount, total_inc_vat,
                    paper_ts_r2_key, meta_json, source_key
                  )
                  values (
                    v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                    coalesce(s.hours_day,0), coalesce(s.hours_night,0), coalesce(s.hours_sat,0), coalesce(s.hours_sun,0), coalesce(s.hours_bh,0),
                    null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                    base_pay_ex, base_chg_ex, margin_ex,
                    v_vat_rate, vat_amt, inc_amt,
                    ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                    meta,
                    v_line_source_key
                  )
                  on conflict (invoice_id, source_key) do nothing;

                  if found then
                    insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                    values (s.timesheet_id, v_line_source_key, base_chg_ex, vat_amt, inc_amt);
            end if;
            end if; -- can_daily


            -- -------------------------
            -- ADDITIONAL lines
            -- -------------------------
            for kv in
              select key as k, value as v
              from jsonb_each(coalesce(s.additional_units_json, '{}'::jsonb))
            loop
              ex := kv.v;
              if ex is null or jsonb_typeof(ex) <> 'object' then
                continue;
              end if;

              code := upper(btrim(coalesce(kv.k,'')));
              if code = '' then
                continue;
              end if;

              bucket_name := nullif(btrim(coalesce(ex->>'bucket_name','')), '');
              if bucket_name is null then bucket_name := code; end if;

              unit_name := nullif(btrim(coalesce(ex->>'unit_name','')), '');
              if unit_name is null then unit_name := 'units'; end if;

              pay_rate := coalesce((ex->>'pay_rate')::numeric, 0);
              charge_rate := coalesce((ex->>'charge_rate')::numeric, 0);

              days_obj := ex->'days';

              if can_daily and days_obj is not null and jsonb_typeof(days_obj)='object' then
                any_daily_add := false;

                for d_rec in
                  select key as ymd, (value)::numeric as units
                  from jsonb_each_text(days_obj)
                  where left(key,10) ~ '^\d{4}-\d{2}-\d{2}$'
                  order by key
                loop
                  any_daily_add := true;

                  pay_ex := public._inv_round2(d_rec.units * pay_rate);
                  charge_ex := public._inv_round2(d_rec.units * charge_rate);
                  margin_ex := public._inv_round2(charge_ex - pay_ex);
                  vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
                  inc_amt := public._inv_round2(charge_ex + vat_amt);

                  v_sum_ex := v_sum_ex + charge_ex;
                  v_sum_vat := v_sum_vat + vat_amt;
                  v_sum_inc := v_sum_inc + inc_amt;

                                   line_desc := bucket_name || ' – ' || d_rec.units::text || ' ' || unit_name ||
                          case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end
                          || ' – ' || left(d_rec.ymd,10);

                                      meta := jsonb_build_object(
                    'line_type','ADDITIONAL_RATE_DAILY',
                    'timesheet_id', s.timesheet_id::text,
                    'timesheet_version', s.timesheet_version,
                    'booking_id', s.booking_id,
                    'candidate_display', cand_display,
                    'role', c_role,
                    'hospital', c_display_site,
                    'ward', c_ward_hint,
                    'week_ending_date', s.week_ending_date::text,
                    'date', left(d_rec.ymd,10),
                    'ts_reference_number', s.reference_number,
                    'schedule_ref_nums', schedule_refs,
                    'schedule_ref_count', schedule_ref_count,
                    'policy_snapshot_json', s.policy_snapshot_json,
                    'rate_source_refs_json', s.rate_source_refs_json,
                    'bucket_labels', labels,
                    'bucket', jsonb_build_object(
                      'code', code,
                      'bucket_name', coalesce(ex->>'bucket_name', bucket_name),
                      'unit_name', coalesce(ex->>'unit_name', unit_name),
                      'frequency', ex->'frequency'
                    ),
                    'units', jsonb_build_object(
                      'unit_count', d_rec.units,
                      'pay_rate', pay_rate,
                      'charge_rate', charge_rate
                    ),
                    'totals', jsonb_build_object(
                      'line_pay_ex_vat', pay_ex,
                      'line_charge_ex_vat', charge_ex,
                      'margin_ex_vat', margin_ex,
                      'vat_rate_pct', v_vat_rate,
                      'vat_amount', vat_amt,
                      'total_inc_vat', inc_amt
                    )
                  );



                  v_line_source_key := 'TS:' || s.timesheet_id::text || ':ADD:' || code || ':' || left(d_rec.ymd,10);

                  insert into public.invoice_lines(
                    invoice_id, timesheet_id, booking_id, description,
                    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                    vat_rate_pct, vat_amount, total_inc_vat,
                    paper_ts_r2_key, meta_json, source_key
                  )
                  values (
                    v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                    0,0,0,0,0,
                    null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                    pay_ex, charge_ex, margin_ex,
                    v_vat_rate, vat_amt, inc_amt,
                    ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                    meta,
                    v_line_source_key
                  )
                  on conflict (invoice_id, source_key) do nothing;

                  if found then
                    insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                    values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
                  end if;


                end loop;

                if any_daily_add then
                  continue;
                end if;
              end if;
              unit_count := coalesce((ex->>'unit_count')::numeric, 0);

              charge_ex := public._inv_round2(coalesce((ex->>'charge_ex_vat')::numeric, 0));

              pay_ex := public._inv_round2(coalesce((ex->>'pay_ex_vat')::numeric, 0));

              margin_ex := public._inv_round2(charge_ex - pay_ex);
              vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
              inc_amt := public._inv_round2(charge_ex + vat_amt);

              v_sum_ex := v_sum_ex + charge_ex;
              v_sum_vat := v_sum_vat + vat_amt;
              v_sum_inc := v_sum_inc + inc_amt;

                           line_desc := bucket_name || ' – ' || unit_count::text || ' ' || unit_name ||
                      ' @ £' || to_char(coalesce((ex->>'charge_rate')::numeric,0), 'FM999999990.00') ||
                      case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

                            meta := jsonb_build_object(
                'line_type','ADDITIONAL_RATE',
                'timesheet_id', s.timesheet_id::text,
                'timesheet_version', s.timesheet_version,
                'booking_id', s.booking_id,
                'candidate_display', cand_display,
                'role', c_role,
                'hospital', c_display_site,
                'ward', c_ward_hint,
                'week_ending_date', s.week_ending_date::text,
                'ts_reference_number', s.reference_number,
                'schedule_ref_nums', schedule_refs,
                'schedule_ref_count', schedule_ref_count,
                'policy_snapshot_json', s.policy_snapshot_json,
                'rate_source_refs_json', s.rate_source_refs_json,
                'bucket_labels', labels,
                'bucket', jsonb_build_object(
                  'code', code,
                  'bucket_name', coalesce(ex->>'bucket_name', bucket_name),
                  'unit_name', coalesce(ex->>'unit_name', unit_name),
                  'frequency', ex->'frequency'
                ),
                'units', jsonb_build_object(
                  'unit_count', unit_count,
                  'pay_rate', ex->'pay_rate',
                  'charge_rate', ex->'charge_rate'
                ),
                'totals', jsonb_build_object(
                  'line_pay_ex_vat', pay_ex,
                  'line_charge_ex_vat', charge_ex,
                  'margin_ex_vat', margin_ex,
                  'vat_rate_pct', v_vat_rate,
                  'vat_amount', vat_amt,
                  'total_inc_vat', inc_amt
                )
              );


              v_line_source_key := 'TS:' || s.timesheet_id::text || ':ADD:' || code || ':WEEK';

              insert into public.invoice_lines(
                invoice_id, timesheet_id, booking_id, description,
                hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                vat_rate_pct, vat_amount, total_inc_vat,
                paper_ts_r2_key, meta_json, source_key
              )
              values (
                v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                pay_ex, charge_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
              end if;



                  end loop; -- additional

                   -- -------------------------
            -- EXPENSES lines (per category; only if category charge>0)
            -- NOTE:
            --   - Amounts come from TSFIN category columns:
            --       travel_*, accommodation_*, other_*
            --   - expenses_description is treated as NOTES only (optionally JSON).
            --   - Evidence is enforced by v_ts_invoice_precheck (timesheet_evidence.kind).
            -- -------------------------
            declare
              v_desc_txt text := null;
              v_desc_json jsonb := null;

              v_note_global text := null;
              v_note_travel text := null;
              v_note_accom  text := null;
              v_note_other  text := null;
            begin
              v_desc_txt := nullif(btrim(coalesce(s.expenses_description,'')), '');

              -- If looks like JSON object, try parse; otherwise treat as plain note
              if v_desc_txt is not null and left(v_desc_txt, 1) = '{' then
                begin
                  v_desc_json := v_desc_txt::jsonb;
                exception when others then
                  v_desc_json := null;
                end;

                if v_desc_json is not null and jsonb_typeof(v_desc_json) = 'object' then
                  v_note_global := nullif(btrim(coalesce(v_desc_json->>'note','')), '');

                  v_note_travel := nullif(btrim(coalesce(
                    v_desc_json #>> '{travel,note}',
                    v_desc_json->>'travel_note',
                    v_desc_json->>'travel',
                    ''
                  )), '');

                  v_note_accom := nullif(btrim(coalesce(
                    v_desc_json #>> '{accommodation,note}',
                    v_desc_json->>'accommodation_note',
                    v_desc_json->>'accommodation',
                    ''
                  )), '');

                  v_note_other := nullif(btrim(coalesce(
                    v_desc_json #>> '{other,note}',
                    v_desc_json->>'other_note',
                    v_desc_json->>'other',
                    ''
                  )), '');

                  -- fallback to global note where per-category note is missing
                  if v_note_global is not null then
                    if v_note_travel is null then v_note_travel := v_note_global; end if;
                    if v_note_accom  is null then v_note_accom  := v_note_global; end if;
                    if v_note_other  is null then v_note_other  := v_note_global; end if;
                  end if;
                else
                  -- JSON parse failed or not object: treat as plain text note
                  v_note_travel := v_desc_txt;
                  v_note_accom  := v_desc_txt;
                  v_note_other  := v_desc_txt;
                end if;
              else
                -- Plain text note
                v_note_travel := v_desc_txt;
                v_note_accom  := v_desc_txt;
                v_note_other  := v_desc_txt;
              end if;

              -- -------------------------
              -- TRAVEL
              -- -------------------------
              if coalesce(s.travel_charge_ex_vat,0) <> 0 or coalesce(s.travel_pay_ex_vat,0) <> 0 then
                pay_ex := public._inv_round2(coalesce(s.travel_pay_ex_vat,0));
                charge_ex := public._inv_round2(coalesce(s.travel_charge_ex_vat,0));
                margin_ex := public._inv_round2(charge_ex - pay_ex);
                vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(charge_ex + vat_amt);

                v_sum_ex := v_sum_ex + charge_ex;
                v_sum_vat := v_sum_vat + vat_amt;
                v_sum_inc := v_sum_inc + inc_amt;

                line_desc :=
                  'Travel expenses'
                  || case when v_note_travel is not null then ' – ' || v_note_travel else '' end
                  || case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_TRAVEL',
                  'timesheet_id', s.timesheet_id::text,
                  'timesheet_version', s.timesheet_version,
                  'booking_id', s.booking_id,
                  'candidate_display', cand_display,
                  'role', c_role,
                  'hospital', c_display_site,
                  'ward', c_ward_hint,
                  'week_ending_date', s.week_ending_date::text,
                  'ts_reference_number', s.reference_number,
                  'policy_snapshot_json', s.policy_snapshot_json,
                  'rate_source_refs_json', s.rate_source_refs_json,
                  'expense', jsonb_build_object(
                    'category', 'TRAVEL',
                    'note', v_note_travel,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', charge_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', charge_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || s.timesheet_id::text || ':EXP:TRAVEL';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                  pay_ex, charge_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- -------------------------
              -- ACCOMMODATION
              -- -------------------------
              if coalesce(s.accommodation_charge_ex_vat,0) <> 0 or coalesce(s.accommodation_pay_ex_vat,0) <> 0 then
                pay_ex := public._inv_round2(coalesce(s.accommodation_pay_ex_vat,0));
                charge_ex := public._inv_round2(coalesce(s.accommodation_charge_ex_vat,0));
                margin_ex := public._inv_round2(charge_ex - pay_ex);
                vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(charge_ex + vat_amt);

                v_sum_ex := v_sum_ex + charge_ex;
                v_sum_vat := v_sum_vat + vat_amt;
                v_sum_inc := v_sum_inc + inc_amt;

                line_desc :=
                  'Accommodation expenses'
                  || case when v_note_accom is not null then ' – ' || v_note_accom else '' end
                  || case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_ACCOMMODATION',
                  'timesheet_id', s.timesheet_id::text,
                  'timesheet_version', s.timesheet_version,
                  'booking_id', s.booking_id,
                  'candidate_display', cand_display,
                  'role', c_role,
                  'hospital', c_display_site,
                  'ward', c_ward_hint,
                  'week_ending_date', s.week_ending_date::text,
                  'ts_reference_number', s.reference_number,
                  'policy_snapshot_json', s.policy_snapshot_json,
                  'rate_source_refs_json', s.rate_source_refs_json,
                  'expense', jsonb_build_object(
                    'category', 'ACCOMMODATION',
                    'note', v_note_accom,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', charge_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', charge_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || s.timesheet_id::text || ':EXP:ACCOMMODATION';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                  pay_ex, charge_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- -------------------------
              -- OTHER
              -- -------------------------
              if coalesce(s.other_charge_ex_vat,0) <> 0 or coalesce(s.other_pay_ex_vat,0) <> 0 then
                pay_ex := public._inv_round2(coalesce(s.other_pay_ex_vat,0));
                charge_ex := public._inv_round2(coalesce(s.other_charge_ex_vat,0));
                margin_ex := public._inv_round2(charge_ex - pay_ex);
                vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(charge_ex + vat_amt);

                v_sum_ex := v_sum_ex + charge_ex;
                v_sum_vat := v_sum_vat + vat_amt;
                v_sum_inc := v_sum_inc + inc_amt;

                line_desc :=
                  'Other expenses'
                  || case when v_note_other is not null then ' – ' || v_note_other else '' end
                  || case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_OTHER',
                  'timesheet_id', s.timesheet_id::text,
                  'timesheet_version', s.timesheet_version,
                  'booking_id', s.booking_id,
                  'candidate_display', cand_display,
                  'role', c_role,
                  'hospital', c_display_site,
                  'ward', c_ward_hint,
                  'week_ending_date', s.week_ending_date::text,
                  'ts_reference_number', s.reference_number,
                  'policy_snapshot_json', s.policy_snapshot_json,
                  'rate_source_refs_json', s.rate_source_refs_json,
                  'expense', jsonb_build_object(
                    'category', 'OTHER',
                    'note', v_note_other,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', charge_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', charge_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || s.timesheet_id::text || ':EXP:OTHER';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                  pay_ex, charge_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
                end if;
              end if;
            end;


            -- -------------------------
            -- MILEAGE line (one per timesheet, if charge>0)
            -- -------------------------
            if coalesce(s.mileage_units,0) <> 0 or coalesce(s.mileage_charge_ex_vat,0) <> 0 or coalesce(s.mileage_pay_ex_vat,0) <> 0 then
              unit_count := public._inv_round2(coalesce(s.mileage_units,0));
              pay_rate := coalesce(s.mileage_pay_rate,0);
              charge_rate := coalesce(s.mileage_charge_rate,0);

              pay_ex := public._inv_round2(coalesce(s.mileage_pay_ex_vat,0));
              charge_ex := public._inv_round2(coalesce(s.mileage_charge_ex_vat,0));
              margin_ex := public._inv_round2(charge_ex - pay_ex);
              vat_amt := public._inv_round2(charge_ex * v_vat_rate / 100);
              inc_amt := public._inv_round2(charge_ex + vat_amt);

              v_sum_ex := v_sum_ex + charge_ex;
              v_sum_vat := v_sum_vat + vat_amt;
              v_sum_inc := v_sum_inc + inc_amt;

              line_desc :=
                'Mileage – ' || unit_count::text || ' miles' ||
                case when charge_rate is not null then ' @ £' || to_char(charge_rate::numeric, 'FM999999990.00') else '' end ||
                case when s.week_ending_date is not null then ' (W/E ' || s.week_ending_date::text || ')' else '' end;

              meta := jsonb_build_object(
                'line_type','MILEAGE',
                'timesheet_id', s.timesheet_id::text,
                'timesheet_version', s.timesheet_version,
                'booking_id', s.booking_id,
                'candidate_display', cand_display,
                'role', c_role,
                'hospital', c_display_site,
                'ward', c_ward_hint,
                'week_ending_date', s.week_ending_date::text,
                'ts_reference_number', s.reference_number,
                'policy_snapshot_json', s.policy_snapshot_json,
                'rate_source_refs_json', s.rate_source_refs_json,
                'mileage', jsonb_build_object(
                  'mileage_units', unit_count,
                  'pay_rate', pay_rate,
                  'charge_rate', charge_rate,
                  'evidence_r2_key', s.mileage_evidence_r2_key,
                  'evidence_manifest', s.mileage_evidence_manifest
                ),
                'totals', jsonb_build_object(
                  'line_pay_ex_vat', pay_ex,
                  'line_charge_ex_vat', charge_ex,
                  'margin_ex_vat', margin_ex,
                  'vat_rate_pct', v_vat_rate,
                  'vat_amount', vat_amt,
                  'total_inc_vat', inc_amt
                )
              );

              v_line_source_key := 'TS:' || s.timesheet_id::text || ':MILEAGE';

              insert into public.invoice_lines(
                invoice_id, timesheet_id, booking_id, description,
                hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                vat_rate_pct, vat_amount, total_inc_vat,
                paper_ts_r2_key, meta_json, source_key
              )
              values (
                v_invoice_id, s.timesheet_id, s.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
      coalesce(s.charge_day,null),
      coalesce(s.charge_night,null),
      coalesce(s.charge_sat,null),
      coalesce(s.charge_sun,null),
      coalesce(s.charge_bh,null),

                pay_ex, charge_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || s.timesheet_id::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (s.timesheet_id, v_line_source_key, charge_ex, vat_amt, inc_amt);
              end if;
            end if;

          end loop; -- snaps

          -- If no lines, fail job (matches JS)
          if not exists (select 1 from public.invoice_lines where invoice_id = v_invoice_id) then
            raise exception 'Nothing to invoice (all billable amounts are zero after daily/weekly rules).';
          end if;


          -- Update invoice totals from lines (matches final outcome)

      update public.invoices i
      set
        subtotal_ex_vat = x.ex,
        vat_amount      = x.vat,
        total_inc_vat   = x.inc,
        updated_at      = now()
      from (
        select
          coalesce(sum(l.total_charge_ex_vat),0)::numeric as ex,
          coalesce(sum(l.vat_amount),0)::numeric as vat,
          coalesce(sum(l.total_inc_vat),0)::numeric as inc
        from public.invoice_lines l
        where l.invoice_id = v_invoice_id
      ) x
      where i.id = v_invoice_id;

      -- Compute NEW totals and delta (NEW - PREV)
      select
        coalesce(i.subtotal_ex_vat,0)::numeric,
        coalesce(i.vat_amount,0)::numeric,
        coalesce(i.total_inc_vat,0)::numeric
      into v_new_ex, v_new_vat, v_new_inc
      from public.invoices i
      where i.id = v_invoice_id
      limit 1;

      v_delta_ex  := public._inv_round2(v_new_ex  - v_prev_ex);
      v_delta_vat := public._inv_round2(v_new_vat - v_prev_vat);
      v_delta_inc := public._inv_round2(v_new_inc - v_prev_inc);

      select
        array_agg(distinct rl.timesheet_id),
        array_agg(distinct rl.source_key),
        count(*)::int
      into
        v_run_ts_ids,
        v_run_source_keys,
        v_run_line_count
      from pg_temp._inv_run_lines rl;

      -- Write ONE audit row that proves this run + what changed
      if (coalesce(v_delta_ex,0) <> 0 or coalesce(v_delta_vat,0) <> 0 or coalesce(v_delta_inc,0) <> 0) then
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_TOTALS_DELTA_APPLIED',
          jsonb_build_object(
            'outbox_id', v_outbox_id::text,
            'job_kind', v_kind,
            'run_at_utc', public._inv_iso_utc(v_now),

            'invoice_id', v_invoice_id::text,
            'invoice_no', v_prev_invoice_no,
            'client_id', v_client_id::text,

            'invoice_status_before', v_prev_status,
            'invoice_status_after', (select i.status::text from public.invoices i where i.id = v_invoice_id limit 1),

            'prev_subtotal_ex_vat', public._inv_round2(v_prev_ex),
            'prev_vat_amount', public._inv_round2(v_prev_vat),
            'prev_total_inc_vat', public._inv_round2(v_prev_inc),

            'delta_subtotal_ex_vat', v_delta_ex,
            'delta_vat_amount', v_delta_vat,
            'delta_total_inc_vat', v_delta_inc,

            'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
            'new_vat_amount', public._inv_round2(v_new_vat),
            'new_total_inc_vat', public._inv_round2(v_new_inc),

            'timesheet_ids_this_run', to_jsonb(coalesce(v_run_ts_ids, array[]::uuid[])),
            'source_keys_this_run', to_jsonb(coalesce(v_run_source_keys, array[]::text[])),
            'line_count_this_run', coalesce(v_run_line_count,0)
          ),
          'invoices',
          v_invoice_id::text,
          jsonb_build_object(
            'subtotal_ex_vat', public._inv_round2(v_prev_ex),
            'vat_amount', public._inv_round2(v_prev_vat),
            'total_inc_vat', public._inv_round2(v_prev_inc)
          ),
          'RUN_TOTALS_DELTA',
          v_ip, v_ua, v_corr
        );
      end if;


          -- Lock snapshots + lock all segments in breakdown_json (mirror JS step 7)
 if v_ts_ids_to_use is null or coalesce(array_length(v_ts_ids_to_use,1),0) = 0 then
  raise exception 'No eligible timesheets after contract-resolved invoice precheck/reference gating.';
end if;


-- Whole-timesheet lock (AGGREGATE only). For SEGMENTS, locking is handled by _inv_lock_segments_for_invoice.
update public.timesheets_financials tf
set
  locked_by_invoice_id = v_invoice_id,
  locked_at_utc = v_now,
  updated_at = v_now
where tf.timesheet_id = any(v_ts_ids_to_use)
  and tf.is_current = true
  and tf.locked_by_invoice_id is null
  and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  and (
    tf.invoice_breakdown_json is null
    or jsonb_typeof(tf.invoice_breakdown_json) <> 'object'
    or upper(coalesce(tf.invoice_breakdown_json->>'mode','')) <> 'SEGMENTS'
  );




          -- Mark weeks invoiced (mirror setWeeksInvoicedForTimesheets on usedTsIds)
      select array_agg(distinct tf.timesheet_id)
into v_used_ts_ids
from public.timesheets_financials tf
where tf.timesheet_id = any(v_ts_ids_to_use)
  and tf.is_current = true
  and tf.locked_by_invoice_id = v_invoice_id;


          if v_used_ts_ids is not null and coalesce(array_length(v_used_ts_ids,1),0) > 0 then
            update public.contract_weeks
            set status = 'INVOICED'::public.contract_week_status_enum,
                updated_at = v_now
            where timesheet_id = any(v_used_ts_ids);

            -- AUDIT: TIMESHEET_INVOICED per timesheet
            for contract_id in
              select unnest(v_used_ts_ids)
            loop
              perform public._inv_write_audit(
                p_actor_user_id,
                'TIMESHEET_INVOICED',
                jsonb_build_object(
                  'timesheet_id', contract_id::text,
                  'invoice_id', v_invoice_id::text,
                  'invoice_status','DRAFT',
                  'locked_at_utc', public._inv_iso_utc(v_now)
                ),
                'timesheets',
                contract_id::text,
                jsonb_build_object('locked_by_invoice_id', null),
                'LOCKED_BY_INVOICE',
                v_ip, v_ua, v_corr
              );
            end loop;
          end if;

                       -- Cache NHSP/HealthRoster source rows for this invoice (NHSP always; HealthRoster only when requires_hr AND hr_attach_to_invoice)
          -- Policy:
          --  - Always include NHSP rows when NHSP segments exist in invoice_breakdown_json.
          --  - Include HEALTHROSTER rows only when (v_requires_hr_any = true AND v_hr_attach_any = true).

            -- Build shift_ids from segments where segment_id startswith 'nhsp:' and source_system in (NHSP, HEALTHROSTER)
            delete from public.invoice_hr_source_rows where invoice_id = v_invoice_id;

                   with segs as (
              select
                left(seg->>'segment_id', 5) as pfx,
                upper(coalesce(seg->>'source_system','')) as src,
                substr(seg->>'segment_id', 6) as id_part
              from public.timesheets_financials tf
              join public.v_ts_invoice_precheck pc
                on pc.timesheet_id = tf.timesheet_id
              cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
              where tf.timesheet_id = any(v_ts_ids_to_use)
                and tf.is_current = true
                and jsonb_typeof(seg) = 'object'
                and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
),

            shift_ids as (
              select distinct (id_part)::uuid as shift_id
              from segs
              where pfx = 'nhsp:'
                and (src = 'NHSP' or (src = 'HEALTHROSTER' and coalesce(v_requires_hr_any,false) = true and coalesce(v_hr_attach_any,false) = true))
                and id_part ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ),
            useful as (
              select
                upper(coalesce(s.source_system::text,'UNKNOWN')) as source_system,
                s.latest_import_id as import_id,
                s.external_row_key
              from public.nhsp_shifts s
              where s.id in (select shift_id from shift_ids)
                and s.latest_import_id is not null
                and s.external_row_key is not null
            ),
            grouped as (
              select
                u.source_system,
                u.import_id,
                jsonb_agg(distinct u.external_row_key) as keys_json
              from useful u
              group by u.source_system, u.import_id
            ),
            hdr as (
              select
                g.source_system,
                g.import_id,
                case
                  when jsonb_typeof(hi.parse_summary_json->'header_columns')='array'
                    then (hi.parse_summary_json->'header_columns')
                  else '[]'::jsonb
                end as header_columns,
                g.keys_json
              from grouped g
              join public.hr_imports hi on hi.id = g.import_id
            ),
            rows_agg as (
              select
                h.source_system,
                h.import_id,
                h.header_columns,
                (
                  select coalesce(jsonb_agg(r.payload_json order by r.id), '[]'::jsonb)
                  from public.hr_rows r
                  where r.import_id = h.import_id
                    and r.external_row_key in (
                      select jsonb_array_elements_text(h.keys_json)
                    )
                ) as rows_json
              from hdr h
            )
            insert into public.invoice_hr_source_rows(invoice_id, source_system, import_id, header_columns, rows_json)
            select v_invoice_id, r.source_system, r.import_id, r.header_columns, r.rows_json
            from rows_agg r;





foreach v_invoice_id in array coalesce(v_outbox_invoice_ids,array[]::uuid[]) loop
            perform public._ctms_assert_invoice_correction_lines_v1(
              v_invoice_id,p_actor_user_id,true,'INVOICE_GENERATE_FROM_OUTBOX'
            );
          end loop;

          -- SUCCESS: delete outbox row
          delete from public.invoice_jobs_outbox where id = v_outbox_id;

          outbox_id := v_outbox_id;
          ok := true;
          invoice_ids := v_outbox_invoice_ids;
          warnings := jsonb_build_object(
            'kind','HOURS',
            'invoice_id', v_invoice_id::text,
            'client_id', v_client_id::text
          );
                if v_invoice_debug then
                  v_dbg_results := v_dbg_results || jsonb_build_array(
                    jsonb_build_object(
                      'outbox_id', v_outbox_id::text,
                      'kind', coalesce(v_kind,''),
                      'payload', coalesce(v_payload,'{}'::jsonb),
                      'decision_trace', jsonb_build_object(
                        'branch','HOURS',
                        'ts_ids_in_payload', to_jsonb(v_ts_ids),
                        'ts_ids_to_use', to_jsonb(v_ts_ids_to_use),
                        'client_id', case when v_client_id is null then null else v_client_id::text end,
                        'invoice_id', case when v_invoice_id is null then null else v_invoice_id::text end,
                        'vat_rate_pct', v_vat_rate,
                        'terms_days', v_terms_days,
                        'due_at_utc', public._inv_iso_utc(v_due_at),
                        'requires_hr_any', coalesce(v_requires_hr_any,false),
                        'hr_attach_default', coalesce(v_hr_attach_default,true),
                        'ts_attach_default', coalesce(v_ts_attach_default,true),
                        'sum_subtotal_ex_vat_accum', public._inv_round2(v_sum_ex),
                        'sum_vat_amount_accum', public._inv_round2(v_sum_vat),
                        'sum_total_inc_vat_accum', public._inv_round2(v_sum_inc),
                        'invoice_line_count', (select count(*) from public.invoice_lines l where l.invoice_id = v_invoice_id),
                        'invoice_totals', (select jsonb_build_object('status', i.status::text, 'invoice_no', i.invoice_no, 'subtotal_ex_vat', coalesce(i.subtotal_ex_vat,0)::numeric, 'vat_amount', coalesce(i.vat_amount,0)::numeric, 'total_inc_vat', coalesce(i.total_inc_vat,0)::numeric) from public.invoices i where i.id = v_invoice_id limit 1),
                        'tsfin_lock_count', (select count(*) from public.timesheets_financials tf where tf.timesheet_id = any(v_ts_ids_to_use) and tf.is_current=true and tf.locked_by_invoice_id = v_invoice_id)
                      ),
                      'result', jsonb_build_object(
                        'ok', ok,
                        'invoice_ids', case when invoice_ids is null then null else to_jsonb(invoice_ids) end,
                        'warnings', warnings
                      ),
                      'job_row', jsonb_build_object(
                        'attempt_count', coalesce(v_dbg_job_attempt_count,0),
                        'next_attempt_at', to_jsonb(v_dbg_job_next_attempt_at),
                        'last_error', v_dbg_job_last_error,
                        'created_at', to_jsonb(v_dbg_job_created_at)
                      ),
                      'timing', jsonb_build_object(
                        'now_utc', public._inv_iso_utc(v_now),
                        'anchor_ymd', v_anchor_ymd::text
                      ),
                      'audit_meta', jsonb_build_object(
                        'ip', v_ip,
                        'user_agent', v_ua,
                        'correlation_id', v_corr
                      )
                    )
                  );

                  if v_dbg_first_ip is null and v_ip is not null then v_dbg_first_ip := v_ip; end if;
                  if v_dbg_first_ua is null and v_ua is not null then v_dbg_first_ua := v_ua; end if;
                  if v_dbg_first_corr is null and v_corr is not null then v_dbg_first_corr := v_corr; end if;
                end if;
          return next;
          continue;
        end;

      -- ======================================================
      -- KIND: BY_WEEK  (mirror handleCreateInvoiceTsfinByWeek)
      -- ======================================================
         elsif v_kind = 'BY_WEEK' then
        declare
          v_client_id uuid;
          v_week_start date;

          v_allow_early boolean := false;
          v_has_due_delayed boolean := false;
          v_week_end_ymd date;
          v_next_attempt_at timestamptz;

        -- Optional: selection restriction for Batch Generate (payload.timesheet_ids)
        v_limit_ts_ids uuid[];

        -- Eligible timesheets after precheck/reference gating
        v_ts_ids_to_use uuid[];


          v_client record;
          v_def record;
          v_cs record;

          v_global_vat numeric := 20;
          v_vat_rate numeric := 20;
          v_ordinary_vat_rate numeric := 20;

          v_terms_days int;
          v_due_at timestamptz;

          v_hr_attach_default boolean := true;
          v_ts_attach_default boolean := true;

          v_requires_hr_any boolean := false;
          v_hr_attach_any boolean := null;
          v_ts_attach_any boolean := null;

          v_entries jsonb := '[]'::jsonb; -- array of entry objects
          v_entry_count int := 0;

              v_all_selfbill boolean := false;
          v_has_nhsp boolean := false;
          v_created boolean := false;

          v_mode text;


          v_invoice record;
          v_header jsonb;

          v_timesheet_ids uuid[];
          v_snap_ids uuid[];

            v_sum_ex numeric := 0;
      v_sum_vat numeric := 0;
      v_sum_inc numeric := 0;

      -- run-level totals audit
      v_prev_ex numeric := 0;
      v_prev_vat numeric := 0;
      v_prev_inc numeric := 0;
      v_prev_status text := null;
      v_prev_invoice_no text := null;

      v_new_ex numeric := 0;
      v_new_vat numeric := 0;
      v_new_inc numeric := 0;

      v_delta_ex numeric := 0;
      v_delta_vat numeric := 0;
      v_delta_inc numeric := 0;

      v_run_ts_ids uuid[] := null;
      v_run_source_keys text[] := null;
      v_run_line_count int := 0;

      -- already billed additional keys
      billed record;


   -- line build loop
v_ts_id uuid;
snap record;
ts_rec record;
con record;
cand_display text;
labels jsonb;
contract_id uuid;

con_daily_calc boolean := false;
con_bucket_labels jsonb := null;
con_role text := null;
con_display_site text := null;
con_ward_hint text := null;


h_day numeric;
h_night numeric;
h_sat numeric;
h_sun numeric;
h_bh numeric;



          -- per-ts entries temp
          e_rec record;
          wants_daily boolean;
          has_any_date boolean;
          can_daily boolean;

          bydate record;
          bydate_any boolean;

          -- additional
          kv record;
          code text;
          ex jsonb;
          bucket_name text;
          unit_name text;
          days_obj jsonb;
          units numeric;
          unit_count numeric;

          pay_rate numeric;
          charge_rate numeric;
          pay_ex numeric;
          chg_ex numeric;
          margin_ex numeric;
          vat_amt numeric;
          inc_amt numeric;

          -- track whether any EXP category line was written (for expenses_total fallback)
          v_any_expense_line boolean := false;

          line_desc text;
          meta jsonb;
          v_line_source_key text;

          -- ======================================================
          -- DEBUG (invoice_debug): extensive BY_WEEK trace
          -- ======================================================
          v_dbg_meta_count int := 0;
          v_dbg_meta_sample jsonb := '[]'::jsonb;
          v_dbg_entries_sample jsonb := '[]'::jsonb;
          v_dbg_timesheet_ids_pre uuid[] := null;
          v_dbg_ts_ids_to_use_pre uuid[] := null;
          v_dbg_groups_count int := 0;
          v_dbg_groups_rows jsonb := '[]'::jsonb;
          v_dbg_groups_reason text := null;
          v_dbg_groups_detail jsonb := '[]'::jsonb;
          v_dbg_grp_i int := 0;
          v_dbg_reused_invoice_id uuid := null;

          -- Option A partial-failure tracking (BY_WEEK + consolidation NONE)
          v_failed_ts_ids uuid[] := array[]::uuid[];
          v_failed_groups jsonb := '[]'::jsonb;
          v_succeeded_any boolean := false;

          -- already billed set: temp table

        begin

          v_client_id := nullif(btrim(coalesce(v_payload->>'client_id','')), '')::uuid;
          v_week_start := (v_payload->>'invoice_week_start')::date;

          if v_client_id is null then
            raise exception 'BY_WEEK job requires payload.client_id';
          end if;
          if v_week_start is null then
            raise exception 'BY_WEEK job requires payload.invoice_week_start (YYYY-MM-DD)';
          end if;

          -- ------------------------------------------------------------
          -- Safety net: do not run BY_WEEK before the week ending date has
          -- passed (Europe/London), unless payload.allow_early = true.
          -- Week ending date = invoice_week_start + 6 days.
          -- ------------------------------------------------------------
          if (v_payload ? 'allow_early') and jsonb_typeof(v_payload->'allow_early') = 'boolean' then
            v_allow_early := (v_payload->>'allow_early')::boolean;
          elsif (v_payload ? 'allow_early') and jsonb_typeof(v_payload->'allow_early') = 'string' then
            v_allow_early := (lower(btrim(coalesce(v_payload->>'allow_early','')) ) in ('true','1','yes','y'));
          end if;

          v_week_end_ymd := (v_week_start + 6);

          if coalesce(v_allow_early, false) = false
             and v_week_end_ymd >= v_anchor_ymd then

            -- If the invoice week has not ended yet, we normally defer.
            -- Exception: allow delayed segments (from other weeks) to be invoiced once their
            -- invoice_target_week_start (week start) has arrived. allow_early does NOT control this.
            select exists (
              select 1
              from public.timesheets_financials tf
              join public.timesheets ts
                on ts.timesheet_id = tf.timesheet_id
               and ts.is_current = true
              join public.v_ts_invoice_precheck pc
                on pc.timesheet_id = tf.timesheet_id
              where tf.client_id = v_client_id
                and tf.is_current = true
                and tf.locked_by_invoice_id is null
                and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
                and ts.revoked_at is null
                and upper(coalesce(pc.precheck_status,'')) = 'OK'
                -- if a selection list was provided, restrict to it
                and (
                  not (v_payload ? 'timesheet_ids' and jsonb_typeof(v_payload->'timesheet_ids') = 'array')
                  or tf.timesheet_id in (
                    select (v)::uuid
                    from jsonb_array_elements_text(v_payload->'timesheet_ids') as t(v)
                    where t.v ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  )
                )
                and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
                and v_week_start <= v_anchor_ymd
                        and exists (
                  select 1
                  from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
                  where jsonb_typeof(seg) = 'object'
                    and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
                    and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
                    and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = v_week_start
                    and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
                    and (
                      pc.require_reference_to_invoice is not true
                      or btrim(coalesce(seg->>'ref_num','')) <> ''
                    )
                )

            ) into v_has_due_delayed;

            if not coalesce(v_has_due_delayed,false) then
              -- schedule the next attempt for 00:05 Europe/London on the day after week end
              v_next_attempt_at := ((v_week_end_ymd + 1)::text || ' 00:05:00')::timestamp at time zone 'Europe/London';

              update public.invoice_jobs_outbox
              set next_attempt_at = v_next_attempt_at,
                  last_error = 'NOT_DUE_YET'
              where id = v_outbox_id;

              outbox_id := v_outbox_id;
              ok := false;
              invoice_ids := null;
              warnings := jsonb_build_object(
                'status', 'NOT_DUE_YET',
                'invoice_week_start', v_week_start::text,
                'week_ending_date', v_week_end_ymd::text,
                'allow_early', coalesce(v_allow_early,false),
                'next_attempt_at_utc', to_jsonb(v_next_attempt_at)
              );
                    if v_invoice_debug then
                      v_dbg_results := v_dbg_results || jsonb_build_array(
                        jsonb_build_object(
                          'outbox_id', v_outbox_id::text,
                          'kind', coalesce(v_kind,''),
                          'payload', coalesce(v_payload,'{}'::jsonb),
                          'decision_trace', jsonb_build_object(
                            'branch','BY_WEEK_NOT_DUE_YET',
                            'client_id', case when v_client_id is null then null else v_client_id::text end,
                            'invoice_week_start', case when v_week_start is null then null else v_week_start::text end,
                            'invoice_week_end', case when v_week_end_ymd is null then null else v_week_end_ymd::text end,
                            'allow_early', coalesce(v_allow_early,false),
                            'anchor_ymd', v_anchor_ymd::text,
                            'has_due_delayed', coalesce(v_has_due_delayed,false),
                            'scheduled_next_attempt_at_utc', case when v_next_attempt_at is null then null else public._inv_iso_utc(v_next_attempt_at) end
                          ),
                          'result', jsonb_build_object(
                            'ok', ok,
                            'invoice_ids', case when invoice_ids is null then null else to_jsonb(invoice_ids) end,
                            'warnings', warnings
                          ),
                          'job_row', jsonb_build_object(
                            'attempt_count', coalesce(v_dbg_job_attempt_count,0),
                            'next_attempt_at', to_jsonb(v_dbg_job_next_attempt_at),
                            'last_error', v_dbg_job_last_error,
                            'created_at', to_jsonb(v_dbg_job_created_at)
                          ),
                          'timing', jsonb_build_object(
                            'now_utc', public._inv_iso_utc(v_now),
                            'anchor_ymd', v_anchor_ymd::text
                          ),
                          'audit_meta', jsonb_build_object(
                            'ip', v_ip,
                            'user_agent', v_ua,
                            'correlation_id', v_corr
                          )
                        )
                      );

                      if v_dbg_first_ip is null and v_ip is not null then v_dbg_first_ip := v_ip; end if;
                      if v_dbg_first_ua is null and v_ua is not null then v_dbg_first_ua := v_ua; end if;
                      if v_dbg_first_corr is null and v_corr is not null then v_dbg_first_corr := v_corr; end if;
                    end if;
              return next;
              continue;
            end if;
          end if;


          -- Optional: restrict BY_WEEK job to an explicit set of timesheet_ids (Batch Generate selection)
          -- payload.timesheet_ids: JSON array of UUID strings
          v_limit_ts_ids := null;

          if (v_payload ? 'timesheet_ids')
             and jsonb_typeof(v_payload->'timesheet_ids') = 'array' then

            if jsonb_array_length(v_payload->'timesheet_ids') > 0 then
              -- NOTE: avoid DISTINCT+ORDER BY mismatch (42P10) by de-duping in a subquery then ordering in array_agg
              select array_agg(x order by x::text)
              into v_limit_ts_ids
              from (
                select distinct (v)::uuid as x
                from jsonb_array_elements_text(v_payload->'timesheet_ids') as t(v)
                where t.v ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) q;
            end if;

            -- If a timesheet_ids filter was explicitly provided, it must resolve to at least one valid UUID,
            -- otherwise we refuse to run to avoid accidentally invoicing the whole cohort.
            if v_limit_ts_ids is null or coalesce(array_length(v_limit_ts_ids, 1), 0) = 0 then
              raise exception 'BY_WEEK job payload.timesheet_ids provided but empty/invalid';
            end if;
          end if;


          -- Load client + defaults
          select id, name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
          into v_client
          from public.clients
          where id = v_client_id;

          if not found then
            raise exception 'Client not found.';
          end if;

                 select bank_name, bank_sort_code, bank_account_number, vat_registration_number,
                 hr_attach_to_invoice, ts_attach_to_invoice,
                 agency_name, agency_logo, registered_address, company_reg_number
          into v_def
          from public.settings_defaults
          where id = 1;


          -- VAT anchor: invoice create/amend time (now), Europe/London ymd
        select coalesce(sf.vat_rate_pct, 20)
into v_global_vat
from public.settings_finance_pick(v_anchor_ymd) sf
where (sf.date_from is null or sf.date_from <= v_anchor_ymd)
  and (sf.date_to   is null or sf.date_to   >= v_anchor_ymd)
order by sf.date_from desc nulls last
limit 1;


          -- client_settings: BY_WEEK uses effective_from <= anchorYmd (no NULL fallback)
          select cs.client_id, cs.vat_rate_pct, cs.requires_hr, cs.hr_attach_to_invoice, cs.ts_attach_to_invoice, cs.invoice_consolidation_mode, cs.effective_from
          into v_cs
          from public.client_settings cs
          where cs.client_id = v_client_id
            and cs.effective_from <= v_anchor_ymd
          order by cs.effective_from desc
          limit 1;

          v_vat_rate :=
            case when v_client.vat_chargeable = false then 0
                 else coalesce(v_cs.vat_rate_pct, v_global_vat, 20)
            end;
          v_ordinary_vat_rate := v_vat_rate;

          v_hr_attach_default :=
            case when found and v_cs.hr_attach_to_invoice is false then false else true end;

          v_ts_attach_default :=
            case when found and v_cs.ts_attach_to_invoice is false then false else true end;

          v_terms_days := coalesce(v_client.payment_terms_days, 30);
          v_due_at := v_now + make_interval(days => v_terms_days);

          -- Load eligible TSFIN snaps for this client (matches JS)
          -- Apply v_ts_invoice_precheck OK + schedule-aware refs gating.
    with snaps as (
  select
    tf.*,
    ts.week_ending_date,
    ts.booking_id,
    ts.reference_number,
    ts.contract_id as ts_contract_id,
    ts.sheet_scope::text as sheet_scope,
    coalesce(ts.submission_mode::text,'') as submission_mode,
    ts.day_references_json,
    ts.actual_schedule_json
  from public.timesheets_financials tf
  join public.timesheets ts on ts.timesheet_id = tf.timesheet_id
  join public.v_ts_invoice_precheck pc on pc.timesheet_id = tf.timesheet_id
  where tf.client_id = v_client_id
    and (
      -- ─────────────────────────────────────────────────────────────
      -- NON-SEGMENTS: natural week only; allow_early controls week-ending gate
      -- ─────────────────────────────────────────────────────────────
      (
        (
          coalesce(tf.invoice_breakdown_json->>'mode','') <> 'SEGMENTS'
          or (
            coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
            and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
            and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0
            and (
              coalesce(tf.total_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.total_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.additional_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.additional_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.expenses_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.expenses_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.travel_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.travel_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.accommodation_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.accommodation_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.other_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.other_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.mileage_charge_ex_vat,0)::numeric <> 0
              or coalesce(tf.mileage_pay_ex_vat,0)::numeric <> 0
              or coalesce(tf.mileage_units,0)::numeric <> 0
              or coalesce(nullif(btrim(coalesce(tf.invoice_breakdown_json->'additional'->>'charge_ex_vat','')), '')::numeric, 0) <> 0
              or coalesce(nullif(btrim(coalesce(tf.invoice_breakdown_json->'additional'->>'pay_ex_vat','')), '')::numeric, 0) <> 0
            )
          )
        )
        and ts.week_ending_date::date = (v_week_start + 6)
        and (
          v_allow_early = true
          or (v_week_start + 6) < v_anchor_ymd
        )
      )

      or

      -- ─────────────────────────────────────────────────────────────
      -- SEGMENTS: segment-week driven; allow_early NEVER overrides delayed segments
      -- ─────────────────────────────────────────────────────────────
          (
        coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
        and exists (
          select 1
          from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
          where jsonb_typeof(seg) = 'object'
            and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
            and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
            and coalesce(
                  nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
                  (ts.week_ending_date::date - 6)
                ) = v_week_start
            and (
              -- DELAYED segment: target differs from natural; eligible only once delay has arrived (<= today)
              (
                nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is not null
                and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
                and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <= v_anchor_ymd
              )
              or
              -- NON-DELAYED segment: target null OR equals natural; week-ending gate uses allow_early
              (
                (
                  nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is null
                  or nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
                )
                and (
                  v_allow_early = true
                  or (v_week_start + 6) < v_anchor_ymd
                )
              )
            )
            and (
              pc.require_reference_to_invoice is not true
              or btrim(coalesce(seg->>'ref_num','')) <> ''
            )
        )
      )

    )
    and (v_limit_ts_ids is null or tf.timesheet_id = any(v_limit_ts_ids))
    and tf.is_current = true
    and tf.locked_by_invoice_id is null
    and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    and upper(coalesce(pc.precheck_status,'')) = 'OK'
    and (
      pc.require_reference_to_invoice is not true
      or public._inv_timesheet_has_invoice_reference(
            ts.sheet_scope::text,
            coalesce(ts.submission_mode::text,''),
            ts.reference_number,
            ts.day_references_json,
            ts.actual_schedule_json
         )
    )
)

          select
            jsonb_agg(to_jsonb(snaps)) as snaps_json
          into meta
          from snaps;

          if meta is null or jsonb_typeof(meta) <> 'array' or jsonb_array_length(meta)=0 then
            raise exception 'No eligible snapshots for this client.';
          end if;

          if v_invoice_debug then
            v_dbg_meta_count := jsonb_array_length(meta);

            select coalesce(jsonb_agg(
              jsonb_build_object(
                'timesheet_id', s->>'timesheet_id',
                'tsfin_id', s->>'id',
                'basis', s->>'basis',
                'processing_status', s->>'processing_status',
                'total_charge_ex_vat', s->>'total_charge_ex_vat',
                'total_pay_ex_vat', s->>'total_pay_ex_vat',
                'locked_by_invoice_id', s->>'locked_by_invoice_id',
                'invoice_mode', coalesce(s->'invoice_breakdown_json'->>'mode',''),
                'segments_len', case
                  when jsonb_typeof(s->'invoice_breakdown_json'->'segments')='array'
                    then jsonb_array_length(s->'invoice_breakdown_json'->'segments')
                  else null
                end
              )
            ), '[]'::jsonb)
            into v_dbg_meta_sample
            from (
              select value as s
              from jsonb_array_elements(meta) as t(value)
              limit 25
            ) q;
          end if;
          -- Contract mapping: prefer timesheets.contract_id, fallback to contract_weeks (matches JS BY_WEEK)
               with ts_ids as (
            select distinct (s->>'timesheet_id')::uuid as timesheet_id,
                            nullif(s->>'week_ending_date','')::date as week_ending_date,
                            nullif(s->>'ts_contract_id','')::uuid as ts_contract_id,
                            nullif(s->>'candidate_id','')::uuid as candidate_id
            from jsonb_array_elements(meta) s
          ),
          cw_map as (
            select cw.timesheet_id, cw.contract_id
            from public.contract_weeks cw
            where cw.timesheet_id in (select timesheet_id from ts_ids)
          ),
          eff as (
            select
              t.timesheet_id,
              coalesce(t.ts_contract_id, cw.contract_id) as contract_id,
              t.candidate_id
            from ts_ids t
            left join cw_map cw on cw.timesheet_id = t.timesheet_id
          ),
          cons as (
            select ctr.*
            from public.contracts ctr
            where ctr.id in (
              select distinct ef.contract_id
              from eff ef
              where ef.contract_id is not null
            )
          )

                select
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.requires_hr, false)
                  else coalesce(v_cs.requires_hr, false)
                end
              ),
              coalesce(v_cs.requires_hr, false),
              false
            ) as req_hr,
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
                  else coalesce(v_cs.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
                end
              ),
              coalesce(v_cs.hr_attach_to_invoice, v_def.hr_attach_to_invoice, true)
            ) as hr_attach_any,
            coalesce(
              bool_or(
                case
                  when cons.overrideclientsettings is true then coalesce(cons.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
                  else coalesce(v_cs.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
                end
              ),
              coalesce(v_cs.ts_attach_to_invoice, v_def.ts_attach_to_invoice, true)
            ) as ts_attach_any
          into v_requires_hr_any, v_hr_attach_any, v_ts_attach_any
          from cons;




          -- Build entries (extractBillableSegmentsForWeek) into a jsonb array, preserving order with entry_ord
          with snaps as (
            select
              (s->>'id')::uuid as tsfin_id,
              (s->>'timesheet_id')::uuid as timesheet_id,
              nullif(s->>'candidate_id','')::uuid as candidate_id,
              nullif(s->>'client_id','')::uuid as client_id,
              upper(coalesce(s->>'basis','')) as basis,
              (s->'invoice_breakdown_json') as ib,
              (s->>'locked_by_invoice_id')::uuid as locked_by_invoice_id,
              nullif(s->>'week_ending_date','')::date as week_ending_date
            from jsonb_array_elements(meta) s
          ),
                  seg_entries as (
            select
              row_number() over () as entry_ord,
              sn.tsfin_id,
              sn.timesheet_id,
              sn.candidate_id,
              sn.client_id,
              sn.basis,
              (seg.value) as segment,
              (seg.ordinality - 1) as segment_index,
              false as pseudo
            from snaps sn
            cross join lateral (
              select value, ordinality
              from jsonb_array_elements(coalesce(sn.ib->'segments','[]'::jsonb)) with ordinality
            ) seg
            where sn.ib is not null
              and jsonb_typeof(sn.ib)='object'
              and (sn.ib->>'mode')='SEGMENTS'
              and jsonb_typeof(seg.value) = 'object'
              and nullif(btrim(coalesce(seg.value->>'segment_id','')), '') is not null
              and nullif(coalesce(seg.value->>'invoice_locked_invoice_id',''), '') is null
              and (
                coalesce(nullif(seg.value->>'invoice_target_week_start','')::date, (sn.week_ending_date - 6)) = v_week_start
              )
          ),

             weekly_atomic as (
            select
              row_number() over () + 1000000 as entry_ord,
              sn.tsfin_id,
              sn.timesheet_id,
              sn.candidate_id,
              sn.client_id,
              sn.basis,
              jsonb_build_object(
                -- IMPORTANT: for non-segment snapshots, treat as "lock whole" later by leaving segment_id NULL
                'segment_id', null,
                'date', coalesce(sn.week_ending_date::text, v_week_start::text),

                -- ✅ include hours buckets for AGGREGATE snapshots (prevents zero-hours false negatives)
                'hours_day',
                  public._inv_round2(
                    coalesce((
                      select (s2->>'hours_day')::numeric
                      from jsonb_array_elements(meta) s2
                      where (s2->>'id')::uuid = sn.tsfin_id
                      limit 1
                    ), 0)
                  ),
                'hours_night',
                  public._inv_round2(
                    coalesce((
                      select (s2->>'hours_night')::numeric
                      from jsonb_array_elements(meta) s2
                      where (s2->>'id')::uuid = sn.tsfin_id
                      limit 1
                    ), 0)
                  ),
                'hours_sat',
                  public._inv_round2(
                    coalesce((
                      select (s2->>'hours_sat')::numeric
                      from jsonb_array_elements(meta) s2
                      where (s2->>'id')::uuid = sn.tsfin_id
                      limit 1
                    ), 0)
                  ),
                'hours_sun',
                  public._inv_round2(
                    coalesce((
                      select (s2->>'hours_sun')::numeric
                      from jsonb_array_elements(meta) s2
                      where (s2->>'id')::uuid = sn.tsfin_id
                      limit 1
                    ), 0)
                  ),
                'hours_bh',
                  public._inv_round2(
                    coalesce((
                      select (s2->>'hours_bh')::numeric
                      from jsonb_array_elements(meta) s2
                      where (s2->>'id')::uuid = sn.tsfin_id
                      limit 1
                    ), 0)
                  ),

                -- CORE ONLY: exclude additional + expenses + mileage (these become separate invoice lines)
                'pay_amount',
                  public._inv_round2(
                    case
                      -- If totals are zero but non-core components exist (expenses/mileage/additional),
                      -- treat core as zero to avoid creating a negative/positive balancing core line.
                      when coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) = 0
                        and (
                          coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'travel_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'accommodation_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'other_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_units')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                        )
                        then 0
                      else
                        coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                    end
                  ),

                'charge_amount',
                  public._inv_round2(
                    case
                      -- If totals are zero but non-core components exist (expenses/mileage/additional),
                      -- treat core as zero to avoid creating a negative/positive balancing core line.
                      when coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) = 0
                        and (
                          coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'travel_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'accommodation_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'other_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_units')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                        )
                        then 0
                      else
                        coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        - coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                    end
                  ),

                'margin_amount',
                  public._inv_round2(
                    case
                      -- If totals are zero but non-core components exist, core margin should be zero.
                      when (
                        coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) = 0
                        and (
                          coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'travel_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'accommodation_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'other_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_units')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                        )
                      ) or (
                        coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) = 0
                        and (
                          coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'travel_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'accommodation_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'other_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                          or coalesce((select (s2->>'mileage_units')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0) <> 0
                        )
                      )
                        then 0
                      else
                        (
                          coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        )
                        -
                        (
                          coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                          - coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid = sn.tsfin_id limit 1), 0)
                        )
                    end
                  ),

                'invoice_target_week_start', v_week_start::text,
                'invoice_locked_invoice_id', null
              ) as segment,
              -1 as segment_index,
              true as pseudo
            from snaps sn
            where (sn.week_ending_date - 6) = v_week_start
              and sn.locked_by_invoice_id is null
              and not (sn.ib is not null and jsonb_typeof(sn.ib)='object' and (sn.ib->>'mode')='SEGMENTS' and jsonb_typeof(sn.ib->'segments')='array' and jsonb_array_length(sn.ib->'segments') > 0)
          ),

          all_e as (
            select * from seg_entries
            union all
            select * from weekly_atomic
          ),
          -- segment-level ref gating (matches JS: keep only if segment.ref_num non-empty when require_reference_to_invoice)
          gated as (
            select e.*
            from all_e e
            join public.v_ts_invoice_precheck pc on pc.timesheet_id = e.timesheet_id
            where pc.require_reference_to_invoice is not true
               or btrim(coalesce(e.segment->>'ref_num','')) <> ''
          )
          select
            coalesce(jsonb_agg(
              jsonb_build_object(
                'entry_ord', entry_ord,
                'tsfin_id', tsfin_id::text,
                'timesheet_id', timesheet_id::text,
                'candidate_id', case when candidate_id is null then null else candidate_id::text end,
                'client_id', case when client_id is null then null else client_id::text end,
                'basis', basis,
                'segment', segment,
                'segment_index', segment_index,
                'pseudo', pseudo
              )
              order by entry_ord
            ), '[]'::jsonb) as entries_json
          into v_entries
          from gated;

          v_entry_count := jsonb_array_length(v_entries);
          if v_entry_count = 0 then
            raise exception 'Nothing to invoice for this week (after reference gating).';
          end if;

          if v_invoice_debug then
            select coalesce(jsonb_agg(
              jsonb_build_object(
                'entry_ord', coalesce(nullif(e->>'entry_ord','')::int, 0),
                'timesheet_id', e->>'timesheet_id',
                'tsfin_id', e->>'tsfin_id',
                'basis', e->>'basis',
                'pseudo', coalesce(nullif(e->>'pseudo','')::boolean, false),
                'segment_index', nullif(e->>'segment_index','')::int,
                'segment_id', e->'segment'->>'segment_id',
                'invoice_target_week_start', e->'segment'->>'invoice_target_week_start',
                'invoice_locked_invoice_id', e->'segment'->>'invoice_locked_invoice_id',
                'ref_num', e->'segment'->>'ref_num',
                'hours_day', e->'segment'->>'hours_day',
                'hours_night', e->'segment'->>'hours_night',
                'hours_sat', e->'segment'->>'hours_sat',
                'hours_sun', e->'segment'->>'hours_sun',
                'hours_bh', e->'segment'->>'hours_bh',
                'pay_amount', e->'segment'->>'pay_amount',
                'charge_amount', e->'segment'->>'charge_amount'
              )
              order by coalesce(nullif(e->>'entry_ord','')::int, 0)
            ), '[]'::jsonb)
            into v_dbg_entries_sample
            from (
              select value as e
              from jsonb_array_elements(v_entries) as t(value)
              limit 50
            ) q;
          end if;

          -- timesheet_ids used
          select array_agg(distinct (e->>'timesheet_id')::uuid)
          into v_timesheet_ids
          from jsonb_array_elements(v_entries) e;

                 -- allSelfBill check (matches JS)
          select bool_and(upper(coalesce(e->>'basis','')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'))
          into v_all_selfbill
          from jsonb_array_elements(v_entries) e;

          -- NHSP detection (NHSP invoices must NOT require timesheet PDFs)
          select exists(
            select 1
            from jsonb_array_elements(v_entries) e
            where upper(coalesce(e->>'basis','')) in ('NHSP','NHSP_ADJUSTMENT')
          )
          into v_has_nhsp;

          v_mode := case when v_all_selfbill then 'SELF_BILL' else 'NORMAL' end;


          --
          -- Determine consolidation mode:
          --   Prefer payload.meta.invoice_consolidation_mode (stable between enqueue and generate)
          --   Fallback to client_settings.invoice_consolidation_mode
          v_payload_consol_mode := null;

          begin
            if jsonb_typeof(v_payload->'meta') = 'object' then
              v_payload_consol_mode := nullif(btrim(coalesce(v_payload->'meta'->>'invoice_consolidation_mode','')), '');
            end if;
          exception when others then
            v_payload_consol_mode := null;
          end;

          if v_payload_consol_mode is null then
            v_payload_consol_mode := nullif(btrim(coalesce(v_payload->>'invoice_consolidation_mode','')), '');
          end if;

          v_payload_consol_mode := upper(coalesce(v_payload_consol_mode, ''));

          if v_payload_consol_mode = 'ALL' then
            v_payload_consol_mode := 'ANY_WEEK';
          end if;

          if v_payload_consol_mode in ('NONE','BY_WEEK','ANY_WEEK') then
            v_consol_mode := v_payload_consol_mode;
          else
            v_consol_mode := upper(coalesce(v_cs.invoice_consolidation_mode::text, 'NONE'));
          end if;

          -- Preserve original entries for splitting (NONE mode)
          v_entries_all := v_entries;

          -- Build per-invoice groups.
          -- NONE  => one invoice per timesheet_id in this outbox job
          -- BY_WEEK/ANY_WEEK => one invoice for the whole outbox job
          create temporary table if not exists pg_temp._inv_groups(
            stream_mode text not null,
            week_ending_date date,
            ts_ids uuid[],
            entries jsonb
          ) on commit drop;

          truncate pg_temp._inv_groups;

          -- ✅ Ensure v_ts_ids_to_use is populated for grouping
          -- Prefer payload-limited timesheets when provided; otherwise use all timesheets derived from entries.
          if v_ts_ids_to_use is null or coalesce(array_length(v_ts_ids_to_use,1),0) = 0 then
            v_ts_ids_to_use := case
              when v_limit_ts_ids is not null and coalesce(array_length(v_limit_ts_ids,1),0) > 0 then v_limit_ts_ids
              else v_timesheet_ids
            end;
          end if;

          if v_invoice_debug then
            v_dbg_timesheet_ids_pre := v_timesheet_ids;
            v_dbg_ts_ids_to_use_pre := v_ts_ids_to_use;
          end if;

          if v_consol_mode = 'NONE' then
            -- Mode NONE keeps ordinary timesheets separate, but an import-authoritative
            -- correction unit is an atomic invoice group and must never be split by leg.
            truncate table pg_temp._inv_groups;

            with classified as (
              select
                u.tid,
                ts.correction_id,
                case
                  when coalesce(ts.is_adjustment, false) = true
                    and not (
                      upper(coalesce(ts.adjustment_origin, '')) like 'IMPORT_%'
                      or ts.correction_kind is not null
                      or ts.correction_id is not null
                    )
                  then 'NORMAL'
                  when coalesce(ts.is_adjustment, false) = true
                    and (
                      upper(coalesce(ts.adjustment_origin, '')) like 'IMPORT_%'
                      or ts.correction_kind is not null
                      or ts.correction_id is not null
                    )
                    and ts.parent_timesheet_id is not null
                    and ptf.timesheet_id is not null
                  then case
                    when upper(coalesce(ptf.basis::text, '')) in (
                      'NHSP','NHSP_ADJUSTMENT',
                      'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
                    ) then 'SELF_BILL'
                    else 'NORMAL'
                  end
                  else case
                    when upper(coalesce(tf.basis::text, '')) in (
                      'NHSP','NHSP_ADJUSTMENT',
                      'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
                    ) then 'SELF_BILL'
                    else 'NORMAL'
                  end
                end as stream_mode,
                ts.week_ending_date::date as week_ending_date,
                case
                  when coalesce((public._ctms_import_correction_classify_v1(u.tid)
                    ->>'is_import_authoritative_correction')::boolean,false)
                  then coalesce(ts.correction_id::text,u.tid::text)
                  else u.tid::text
                end as invoice_group_key
              from unnest(v_ts_ids_to_use) as u(tid)
              left join public.timesheets_financials tf
                on tf.timesheet_id = u.tid and tf.is_current
              left join public.timesheets ts
                on ts.timesheet_id = u.tid
              left join public.timesheets_financials ptf
                on ptf.timesheet_id = ts.parent_timesheet_id and ptf.is_current
            ),
            grouped as (
              select
                stream_mode,
                week_ending_date,
                invoice_group_key,
                array_agg(tid order by tid)::uuid[] as ts_ids
              from classified
              group by stream_mode,week_ending_date,invoice_group_key
            )
            insert into pg_temp._inv_groups(stream_mode, week_ending_date, ts_ids, entries)
            select
              g.stream_mode,
              g.week_ending_date,
              g.ts_ids,
              coalesce((
                select jsonb_agg(e.value order by (e.value->>'entry_ord')::int)
                from jsonb_array_elements(v_entries_all) as e(value)
                where nullif(coalesce(e.value->>'timesheet_id',''), '') is not null
                  and (e.value->>'timesheet_id')::uuid = any(g.ts_ids)
              ), '[]'::jsonb) as entries
            from grouped g;
          elsif v_consol_mode = 'BY_WEEK' then
            -- Mode BY_WEEK: one invoice per week-ending date per stream.
            truncate table pg_temp._inv_groups;

            with e as (
              select
                (x.value->>'timesheet_id')::uuid as timesheet_id,
                nullif(x.value->>'basis','')::public.timesheet_fin_basis_enum as basis,
                x.value as entry
              from jsonb_array_elements(v_entries_all) as x(value)
              where nullif(coalesce(x.value->>'timesheet_id',''), '') is not null
            ),
            ts as (
              select
                t.timesheet_id,
                t.week_ending_date::date as week_ending_date
              from public.timesheets t
              where t.timesheet_id in (
                select distinct e.timesheet_id
                from e
              )
            ),
            adj as (
              -- ✅ Adjustment timesheets inherit SELF_BILL vs NORMAL stream classification from the parent timesheet
              select distinct on (t.timesheet_id)
                t.timesheet_id,
                case
                  when ptf.timesheet_id is null then null
                  when upper(coalesce(ptf.basis::text, '')) in (
                    'NHSP',
                    'NHSP_ADJUSTMENT',
                    'HEALTHROSTER_SELF_BILL',
                    'HEALTHROSTER_ADJUSTMENT'
                  ) then 'SELF_BILL'
                  else 'NORMAL'
                end as parent_stream_mode
              from public.timesheets t
              left join public.timesheets_financials ptf
                on ptf.timesheet_id = t.parent_timesheet_id
               and ptf.is_current
              where t.timesheet_id in (
                select distinct e.timesheet_id
                from e
              )
                and t.is_adjustment is true
                and t.parent_timesheet_id is not null
                and (
                  upper(coalesce(t.adjustment_origin, '')) like 'IMPORT_%'
                  or t.correction_kind is not null
                  or t.correction_id is not null
                )
                and (
                  upper(coalesce(t.adjustment_origin, '')) like 'IMPORT_%'
                  or t.correction_kind is not null
                  or t.correction_id is not null
                )
                and (
                  upper(coalesce(t.adjustment_origin, '')) like 'IMPORT_%'
                  or t.correction_kind is not null
                  or t.correction_id is not null
                )
              order by t.timesheet_id, t.is_current desc, t.updated_at desc nulls last
            ),
            e2 as (
              select
                e.entry,
                e.timesheet_id,
                ts.week_ending_date,
                case
                  when coalesce(tcur.is_adjustment, false) = true
                    and not (
                      upper(coalesce(tcur.adjustment_origin, '')) like 'IMPORT_%'
                      or tcur.correction_kind is not null
                      or tcur.correction_id is not null
                    )
                    then 'NORMAL'
                  else coalesce(
                    adj.parent_stream_mode,
                    case
                      when e.basis in (
                        'NHSP'::public.timesheet_fin_basis_enum,
                        'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum,
                        'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum,
                        'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum
                      ) then 'SELF_BILL'
                      else 'NORMAL'
                    end
                  )
                end as stream_mode
              from e
              left join ts
                on ts.timesheet_id = e.timesheet_id
              left join public.timesheets tcur
                on tcur.timesheet_id = e.timesheet_id
              left join adj
                on adj.timesheet_id = e.timesheet_id
            )

            insert into pg_temp._inv_groups(stream_mode, week_ending_date, ts_ids, entries)
            select
              e2.stream_mode,
              e2.week_ending_date,
              array_agg(distinct e2.timesheet_id) as ts_ids,
              jsonb_agg(e2.entry order by (e2.entry->>'entry_ord')::int) as entries
            from e2
            group by e2.stream_mode, e2.week_ending_date;

          else
            -- Mode ANY_WEEK: one invoice across all week-ending dates per stream.
            truncate table pg_temp._inv_groups;

            with e as (
              select
                (x.value->>'timesheet_id')::uuid as timesheet_id,
                nullif(x.value->>'basis','')::public.timesheet_fin_basis_enum as basis,
                x.value as entry
              from jsonb_array_elements(v_entries_all) as x(value)
              where nullif(coalesce(x.value->>'timesheet_id',''), '') is not null
            ),
            adj as (
              -- ✅ Adjustment timesheets inherit SELF_BILL vs NORMAL stream classification from the parent timesheet
              select distinct on (t.timesheet_id)
                t.timesheet_id,
                case
                  when ptf.timesheet_id is null then null
                  when upper(coalesce(ptf.basis::text, '')) in (
                    'NHSP',
                    'NHSP_ADJUSTMENT',
                    'HEALTHROSTER_SELF_BILL',
                    'HEALTHROSTER_ADJUSTMENT'
                  ) then 'SELF_BILL'
                  else 'NORMAL'
                end as parent_stream_mode
              from public.timesheets t
              left join public.timesheets_financials ptf
                on ptf.timesheet_id = t.parent_timesheet_id
               and ptf.is_current
              where t.timesheet_id in (
                select distinct e.timesheet_id
                from e
              )
                and t.is_adjustment is true
                and t.parent_timesheet_id is not null
                and (
                  upper(coalesce(t.adjustment_origin, '')) like 'IMPORT_%'
                  or t.correction_kind is not null
                  or t.correction_id is not null
                )
              order by t.timesheet_id, t.is_current desc, t.updated_at desc nulls last
            ),
            e2 as (
              select
                e.entry,
                e.timesheet_id,
                case
                  when coalesce(tcur.is_adjustment, false) = true
                    and not (
                      upper(coalesce(tcur.adjustment_origin, '')) like 'IMPORT_%'
                      or tcur.correction_kind is not null
                      or tcur.correction_id is not null
                    )
                    then 'NORMAL'
                  else coalesce(
                    adj.parent_stream_mode,
                    case
                      when e.basis in (
                        'NHSP'::public.timesheet_fin_basis_enum,
                        'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum,
                        'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum,
                        'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum
                      ) then 'SELF_BILL'
                      else 'NORMAL'
                    end
                  )
                end as stream_mode
              from e
              left join public.timesheets tcur
                on tcur.timesheet_id = e.timesheet_id
              left join adj
                on adj.timesheet_id = e.timesheet_id
            )

            insert into pg_temp._inv_groups(stream_mode, week_ending_date, ts_ids, entries)
            select
              e2.stream_mode,
              null::date as week_ending_date,
              array_agg(distinct e2.timesheet_id) as ts_ids,
              jsonb_agg(e2.entry order by (e2.entry->>'entry_ord')::int) as entries
            from e2
            group by e2.stream_mode;

          end if;

          if v_invoice_debug then
            select count(*)::int
            into v_dbg_groups_count
            from pg_temp._inv_groups;

            select coalesce(jsonb_agg(
              jsonb_build_object(
                'ts_ids', to_jsonb(g.ts_ids),
                'ts_id_count', coalesce(array_length(g.ts_ids,1),0),
                'entry_count', case when g.entries is null then 0 else jsonb_array_length(g.entries) end
              )
            ), '[]'::jsonb)
            into v_dbg_groups_rows
            from (
              select ts_ids, entries
              from pg_temp._inv_groups
              limit 50
            ) g;

            if coalesce(v_dbg_groups_count,0) = 0 then
              if v_ts_ids_to_use is null then
                v_dbg_groups_reason := 'GROUPS_EMPTY: v_ts_ids_to_use IS NULL (no rows from unnest)';
              elsif coalesce(array_length(v_ts_ids_to_use,1),0) = 0 then
                v_dbg_groups_reason := 'GROUPS_EMPTY: v_ts_ids_to_use IS EMPTY';
              else
                v_dbg_groups_reason := 'GROUPS_EMPTY: inserted 0 rows for unknown reason';
              end if;
            end if;
          end if;

          -- Reset per-outbox accumulators
          v_outbox_invoice_ids := array[]::uuid[];
          v_outbox_warnings := '[]'::jsonb;

          for grp in
            select * from pg_temp._inv_groups
          loop
            -- Option A: isolate group failures when consolidation mode is NONE (one invoice per timesheet)
            declare
              v_grp_invoice_ids_before uuid[] := v_outbox_invoice_ids;
              v_grp_warnings_before jsonb := v_outbox_warnings;
              v_grp_invoice_id_before uuid := v_invoice_id;
              -- Track whether we selected a reusable invoice row into v_invoice in THIS group iteration.
              -- Avoid relying on FOUND across unrelated SQL statements (prevents "record v_invoice is not assigned yet").
              v_invoice_selected boolean := false;
            begin
              -- Reset per-group state to avoid cross-group leakage
              v_invoice_id := null;
              v_invoice_selected := false;
              v_dbg_reused_invoice_id := null;

              -- apply group scope
            v_ts_ids_to_use := grp.ts_ids;
            v_timesheet_ids := grp.ts_ids;
            v_entries := coalesce(grp.entries, '[]'::jsonb);
            v_entry_count := coalesce(jsonb_array_length(v_entries), 0);

                     -- Stream invariant: invoice_mode (SELF_BILL vs NORMAL)
            v_all_selfbill := (grp.stream_mode = 'SELF_BILL');

            -- NHSP detection (NHSP invoices must NOT require timesheet PDFs)
            select exists(
              select 1
              from jsonb_array_elements(v_entries) e
              where upper(coalesce(e->>'basis','')) in ('NHSP','NHSP_ADJUSTMENT')
            )
            into v_has_nhsp;

            v_mode := case when v_all_selfbill then 'SELF_BILL' else 'NORMAL' end;


            -- ensure header meta records consolidation mode

                -- Obtain invoice (reuse or create)
                v_created := false;

                -- If ANY_WEEK, warn if there is already an ISSUED invoice in this stream (non-reusable).
                if v_consol_mode = 'ANY_WEEK' and (not v_all_selfbill) then
                  if exists (
                    select 1
                    from public.invoices i
                    where i.client_id = v_client_id
                      and i.status = 'ISSUED'::public.invoice_status_enum
                      and (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_BY_WEEK'
                      and coalesce(i.header_snapshot_json->'meta'->>'self_bill','false') <> 'true'
                  ) then
                    v_outbox_warnings := v_outbox_warnings || jsonb_build_array(
                      jsonb_build_object(
                        'code','ISSUED_INVOICE_EXISTS_NEW_DRAFT',
                        'message','Client has issued invoice(s) while invoice_consolidation_mode=ANY_WEEK; issued invoices are not reusable and a DRAFT invoice will be reused/created instead.'
                      )
                    );
                  end if;
                end if;

                -- Reuse policy:
                --   - Status must be DRAFT (ON_HOLD is NOT eligible for reuse).
                --   - Streams never mix: SELF_BILL vs NORMAL are hard-split.
                --   - BY_WEEK requires the candidate invoice contents to contain only this same week_ending_date.
                if v_consol_mode <> 'NONE' and (not v_all_selfbill) then
                  -- NORMAL stream reuse (TSFIN_BY_WEEK only)
                  if v_consol_mode = 'BY_WEEK' then
                    select *
                    into v_invoice
                    from public.invoices i
                    where i.client_id = v_client_id
                      and i.status = 'DRAFT'::public.invoice_status_enum
                      and (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_BY_WEEK'
                      and coalesce(i.header_snapshot_json->'meta'->>'self_bill','false') <> 'true'
                      and grp.week_ending_date is not null
                      and not exists (
                        select 1
                        from public.invoice_lines il
                        left join public.timesheets ts
                          on ts.timesheet_id = il.timesheet_id
                        where il.invoice_id = i.id
                          and il.timesheet_id is not null
                          and (
                            ts.timesheet_id is null
                            or ts.week_ending_date is null
                            or ts.week_ending_date::date <> grp.week_ending_date
                          )
                      )
                    order by i.created_at desc nulls last
                    limit 1;
                  else
                    -- ANY_WEEK: allow mixed week-ending dates, reuse any DRAFT invoice in this stream
                    select *
                    into v_invoice
                    from public.invoices i
                    where i.client_id = v_client_id
                      and i.status = 'DRAFT'::public.invoice_status_enum
                      and (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_BY_WEEK'
                      and coalesce(i.header_snapshot_json->'meta'->>'self_bill','false') <> 'true'
                    order by i.created_at desc nulls last
                    limit 1;
                  end if;

                  if found then
                    v_created := false;
                    v_invoice_id := v_invoice.id;
                    if not (v_invoice_id = any(v_outbox_invoice_ids)) then
                      v_outbox_invoice_ids := array_append(v_outbox_invoice_ids, v_invoice_id);
                    end if;
                    if v_invoice_debug then
                      v_dbg_reused_invoice_id := v_invoice_id;
                    end if;
                  end if;
                end if;

                if v_all_selfbill then
                  -- SELF_BILL stream reuse (DRAFT only)
                  v_invoice_selected := false;
                  if v_consol_mode <> 'NONE' then
                    if v_consol_mode = 'BY_WEEK' then
                      select *
                      into v_invoice
                      from public.invoices i
                      where i.client_id = v_client_id
                        and i.status = 'DRAFT'::public.invoice_status_enum
                        and (
                          (i.header_snapshot_json->'meta'->>'self_bill') = 'true'
                          or (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_SEGMENTS'
                        )
                        and grp.week_ending_date is not null
                        and not exists (
                          select 1
                          from public.invoice_lines il
                          left join public.timesheets ts
                            on ts.timesheet_id = il.timesheet_id
                          where il.invoice_id = i.id
                            and il.timesheet_id is not null
                            and (
                              ts.timesheet_id is null
                              or ts.week_ending_date is null
                              or ts.week_ending_date::date <> grp.week_ending_date
                            )
                        )
                      order by i.created_at desc nulls last
                      limit 1;
                    else
                      -- ANY_WEEK: allow mixed week-ending dates, reuse any DRAFT invoice in this stream
                      select *
                      into v_invoice
                      from public.invoices i
                      where i.client_id = v_client_id
                        and i.status = 'DRAFT'::public.invoice_status_enum
                        and (
                          (i.header_snapshot_json->'meta'->>'self_bill') = 'true'
                          or (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_SEGMENTS'
                        )
                      order by i.created_at desc nulls last
                      limit 1;
                    end if;
                    v_invoice_selected := found;
                  end if;

                  if v_invoice_selected then
                    v_created := false;
                    v_invoice_id := v_invoice.id;
                    v_outbox_invoice_ids := array_append(v_outbox_invoice_ids, v_invoice_id);
                    if v_invoice_debug then
                      v_dbg_reused_invoice_id := v_invoice_id;
                    end if;
                  else
                  -- Create self-bill invoice header exactly like findOrCreateSelfBillInvoice
                declare
                sb_requires_hr boolean := false;
                sb_hr_attach boolean := true;
                sb_ts_attach boolean := true;

                -- derive requires_hr from referenced contracts (contracts has requires_hr; attach flags are client_settings-only)
                tmp record;
              begin
                with ts_ids as (
                  select unnest(v_timesheet_ids) as timesheet_id
                ),
                con_ids as (
                  select distinct coalesce(ts.contract_id, cw.contract_id) as contract_id
                  from ts_ids x
                  left join public.timesheets ts on ts.timesheet_id = x.timesheet_id
                  left join public.contract_weeks cw on cw.timesheet_id = x.timesheet_id
                  where coalesce(ts.contract_id, cw.contract_id) is not null
                ),
                           cons as (
                  select * from public.contracts c
                  where c.id in (
                    select ci.contract_id
                    from con_ids ci
                  )
                )

                select
                  coalesce(
                    bool_or(
                      case
                        when cons.overrideclientsettings is true then coalesce(cons.requires_hr, false)
                        else coalesce(v_cs.requires_hr, false)
                      end
                    ),
                    coalesce(v_cs.requires_hr, false),
                    false
                  ) as req_hr
                into sb_requires_hr
                from cons;
-- Attach flags use precedence: contract → client_settings → settings_defaults → true
                sb_hr_attach := coalesce(v_hr_attach_any, true);

                -- NHSP invoices must NOT require/attach timesheet PDFs
                if v_has_nhsp then
                  sb_ts_attach := false;
                else
                  sb_ts_attach := coalesce(v_ts_attach_any, true);
                end if;


                          v_header := jsonb_build_object(
                  'client_id', v_client_id::text,
                  'client_name', v_client.name,
                  'client_invoice_address', v_client.invoice_address,
                  'client_primary_invoice_email', v_client.primary_invoice_email,
            'agency_name', v_def.agency_name,
            'agency_logo', v_def.agency_logo,
            'agency_logo_url', v_def.agency_logo,
            'registered_address', v_def.registered_address,
            'company_reg_number', v_def.company_reg_number,
            'company_registration_number', v_def.company_reg_number,
                  'vat_chargeable', coalesce(v_client.vat_chargeable,true),
                  'applied_vat_rate_pct', v_vat_rate,
                  'payment_terms_days', v_terms_days,
                  'issued_at_utc', null,
                  'due_at_utc', null,
                  'stationery_key', nullif(btrim(coalesce(v_payload->>'stationery_key','')), ''),
                  'stationery_margins_mm', null,
                  'hide_bank_footer', null,
                  'bank', jsonb_build_object(
                    'name', v_def.bank_name,
                    'sort_code', v_def.bank_sort_code,
                    'account_number', v_def.bank_account_number
                  ),
                  'vat_registration_number', v_def.vat_registration_number,
                  'meta', jsonb_build_object(
                    'source','TSFIN_SEGMENTS',
                    'self_bill', true,
                    'invoice_week_start', v_week_start::text,
                    'timesheet_count', coalesce(array_length(v_timesheet_ids,1),0),
                    'segment_count', v_entry_count
                  ),
                  'attach_policy', jsonb_build_object(
                    'requires_hr', coalesce(sb_requires_hr,false),
                    'hr_attach_to_invoice', coalesce(sb_hr_attach,true),
                    'ts_attach_to_invoice', coalesce(sb_ts_attach,true)
                  )
                );




                             insert into public.invoices(
                  client_id,
                  status,
                  status_date_utc,
                  issued_at_utc,
                  due_at_utc,
                  subtotal_ex_vat,
                  vat_amount,
                  total_inc_vat,
                  header_snapshot_json
                )
                values (
                  v_client_id,
                  'DRAFT'::public.invoice_status_enum,
                  v_now,
                  null,
                  null,
                  0,0,0,
                  v_header
                )
                returning id into v_invoice_id;

      -- record invoice for this outbox job
      v_outbox_invoice_ids := array_append(v_outbox_invoice_ids, v_invoice_id);
v_created := true;
              end;
            end if;
                  -- Ensure self-bill invoices carry attach_policy (patch best-effort like JS)
                     update public.invoices i
            set header_snapshot_json =
              jsonb_set(
                coalesce(i.header_snapshot_json,'{}'::jsonb),
                '{attach_policy}',
                jsonb_build_object(
                  'requires_hr', coalesce(v_requires_hr_any,false),
                  'hr_attach_to_invoice', coalesce(v_hr_attach_any,true),
                  'ts_attach_to_invoice', case when v_has_nhsp then false else coalesce(v_ts_attach_any,true) end
                ),
                true
              ),
              updated_at = v_now
            where i.id = v_invoice_id;


            -- Self-bill auto-issue removed (invoices remain DRAFT until explicitly issued)


          else
            -- Normal BY_WEEK invoice
            if v_invoice_id is null then
                     v_header := jsonb_build_object(
              'client_id', v_client_id::text,
              'client_name', v_client.name,
              'client_invoice_address', v_client.invoice_address,
              'client_primary_invoice_email', v_client.primary_invoice_email,
            'agency_name', v_def.agency_name,
            'agency_logo', v_def.agency_logo,
            'agency_logo_url', v_def.agency_logo,
            'registered_address', v_def.registered_address,
            'company_reg_number', v_def.company_reg_number,
            'company_registration_number', v_def.company_reg_number,
              'vat_chargeable', coalesce(v_client.vat_chargeable,true),
              'applied_vat_rate_pct', v_vat_rate,
              'payment_terms_days', v_terms_days,
              'issued_at_utc', null,
              'due_at_utc', null,
              'stationery_key', nullif(btrim(coalesce(v_payload->>'stationery_key','')), ''),
              'stationery_margins_mm', null,
              'hide_bank_footer', null,
              'bank', jsonb_build_object(
                'name', v_def.bank_name,
                'sort_code', v_def.bank_sort_code,
                'account_number', v_def.bank_account_number
              ),
              'vat_registration_number', v_def.vat_registration_number,
              'meta', jsonb_build_object(
                'source','TSFIN_BY_WEEK',
                'consolidation_mode', v_consol_mode,

                'invoice_week_start', v_week_start::text,
                'timesheet_count', coalesce(array_length(v_timesheet_ids,1),0),
                'segment_count', v_entry_count
              ),
                                 'attach_policy', jsonb_build_object(
                'requires_hr', coalesce(v_requires_hr_any,false),
                'hr_attach_to_invoice', coalesce(v_hr_attach_any,true),
                'ts_attach_to_invoice', case when v_has_nhsp then false else coalesce(v_ts_attach_any,true) end
              )

            );



            insert into public.invoices(
              client_id, status, issued_at_utc, due_at_utc,
              subtotal_ex_vat, vat_amount, total_inc_vat,
              header_snapshot_json
            )
            values (
        v_client_id,
        'DRAFT'::public.invoice_status_enum,
        null,
        null,
              0,0,0,
              v_header
            )
            returning id into v_invoice_id;

      -- record invoice for this outbox job
      if not (v_invoice_id = any(v_outbox_invoice_ids)) then
        v_outbox_invoice_ids := array_append(v_outbox_invoice_ids, v_invoice_id);
      end if;
      v_created := true;
            end if;
          end if;

      -- Track what THIS run actually inserted (so we can audit per-run delta)
      create temporary table if not exists pg_temp._inv_run_lines (
        timesheet_id uuid,
        source_key text,
        charge_ex numeric,
        vat_amount numeric,
        inc_amount numeric
      ) on commit drop;


      truncate pg_temp._inv_run_lines;

      -- Capture invoice totals/status BEFORE this run applies any changes
      select
        i.invoice_no,
        i.status::text,
        coalesce(i.subtotal_ex_vat,0)::numeric,
        coalesce(i.vat_amount,0)::numeric,
        coalesce(i.total_inc_vat,0)::numeric
      into
        v_prev_invoice_no,
        v_prev_status,
        v_prev_ex,
        v_prev_vat,
        v_prev_inc
      from public.invoices i
      where i.id = v_invoice_id
      limit 1;

      -- AUDIT: invoice chosen/created
  perform public._inv_write_audit(
p_actor_user_id,
case
when v_all_selfbill then 'INVOICE_USED_FOR_WEEK_RUN'
when v_created then 'INVOICE_CREATED'
else 'INVOICE_USED_FOR_WEEK_RUN'
end,
jsonb_build_object(
'invoice_id', v_invoice_id::text,
'client_id', v_client_id::text,
'invoice_week_start', v_week_start::text,
'mode', v_mode,
'timesheet_count', coalesce(array_length(v_timesheet_ids,1),0),
'segment_count', v_entry_count
),
'invoices',
v_invoice_id::text,
null,
null,
v_ip, v_ua, v_corr
);



          -- Temp table of already-billed additional keys (self-bill only)
          create temporary table if not exists pg_temp._inv_billed_add_keys(
            timesheet_id uuid,
            code text,
            suffix text,
            primary key(timesheet_id, code, suffix)
          ) on commit delete rows;

          delete from pg_temp._inv_billed_add_keys where true;

          if v_all_selfbill then
            insert into pg_temp._inv_billed_add_keys(timesheet_id, code, suffix)
            select distinct
              l.timesheet_id,
              upper(coalesce(l.meta_json#>>'{bucket,code}', l.meta_json->>'bucket_code', '')) as code,
              case
                when upper(coalesce(l.meta_json->>'line_type','')) = 'ADDITIONAL_RATE_DAILY'
                 and (l.meta_json->>'date') ~ '^\d{4}-\d{2}-\d{2}$'
                  then (l.meta_json->>'date')
                else 'WEEK'
              end as suffix
            from public.invoice_lines l
            where l.invoice_id = v_invoice_id
              and upper(coalesce(l.meta_json->>'line_type','')) in ('ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY')
              and l.timesheet_id is not null
              and upper(coalesce(l.meta_json#>>'{bucket,code}', l.meta_json->>'bucket_code', '')) <> '';
          end if;

          -- Build per-timesheet sets from entries (preserve entry order with entry_ord)
          for v_ts_id in
            select distinct (e->>'timesheet_id')::uuid
            from jsonb_array_elements(v_entries) e
          loop
            -- Load snapshot + timesheet
            select
              tf.*,
              ts.week_ending_date,
              ts.booking_id,
              ts.reference_number,
              ts.contract_id as ts_contract_id
            into snap
            from public.timesheets_financials tf
            join public.timesheets ts on ts.timesheet_id = tf.timesheet_id
            where tf.timesheet_id = v_ts_id
              and tf.is_current = true;

            if not found then
              continue;
            end if;

            v_vat_rate:=public._ctms_invoice_vat_rate_for_timesheet_v1(
              v_ts_id,v_ordinary_vat_rate
            );

            -- reset per-timesheet expense-line tracker
            v_any_expense_line := false;

                     -- contract resolution
            contract_id := snap.ts_contract_id;

            if contract_id is null then
              select cw.contract_id
              into contract_id
              from public.contract_weeks cw
              where cw.timesheet_id = v_ts_id
              limit 1;
            end if;


                  con_daily_calc := false;
            con_bucket_labels := null;
            con_role := null;
            con_display_site := null;
            con_ward_hint := null;

            if contract_id is not null then
              select
                coalesce(daily_calc_of_invoices,false),
                bucket_labels_json,
                role,
                display_site,
                ward_hint
              into con_daily_calc, con_bucket_labels, con_role, con_display_site, con_ward_hint
              from public.contracts
              where id = contract_id;
            end if;

            labels := case
              when con_bucket_labels is not null and jsonb_typeof(con_bucket_labels)='object'
                then con_bucket_labels
              else v_default_labels
            end;

                                              cand_display := null;

            select nullif(btrim(coalesce(cd.display_name,'')),'')
            into cand_display
            from public.candidates cd
            where cd.id = snap.candidate_id;

            if cand_display is null and snap.candidate_id is not null then
              cand_display := 'Candidate ' || substr(snap.candidate_id::text,1,8) || '…';
            end if;



            wants_daily := con_daily_calc;



            -- hasAnyDate from entries for this v_ts_id
            select exists(
              select 1
              from jsonb_array_elements(v_entries) e
              where (e->>'timesheet_id')::uuid = v_ts_id
                and left(coalesce((e->'segment'->>'date'),''),10) ~ '^\d{4}-\d{2}-\d{2}$'
            ) into has_any_date;

            can_daily := wants_daily and has_any_date;

            -- DAILY hours lines
            bydate_any := false;

            if can_daily then
              for bydate in
                with rows as (
                  select
                    left((e->'segment'->>'date'),10) as ymd,
                    coalesce((e->'segment'->>'hours_day')::numeric,0) as h_day,
                    coalesce((e->'segment'->>'hours_night')::numeric,0) as h_night,
                    coalesce((e->'segment'->>'hours_sat')::numeric,0) as h_sat,
                    coalesce((e->'segment'->>'hours_sun')::numeric,0) as h_sun,
                    coalesce((e->'segment'->>'hours_bh')::numeric,0) as h_bh,
                    coalesce((e->'segment'->>'pay_amount')::numeric,0) as pay_ex,
                    coalesce((e->'segment'->>'charge_amount')::numeric,0) as chg_ex
                  from jsonb_array_elements(v_entries) e
                  where (e->>'timesheet_id')::uuid = v_ts_id
                    and left(coalesce((e->'segment'->>'date'),''),10) ~ '^\d{4}-\d{2}-\d{2}$'
                ),
                agg as (
                  select
                    ymd,
                    sum(h_day)::numeric as hours_day,
                    sum(h_night)::numeric as hours_night,
                    sum(h_sat)::numeric as hours_sat,
                    sum(h_sun)::numeric as hours_sun,
                    sum(h_bh)::numeric as hours_bh,
                    sum(pay_ex)::numeric as pay_ex,
                    sum(chg_ex)::numeric as chg_ex
                  from rows
                  group by ymd
                )
                select * from agg order by ymd
              loop
                -- Allow zero/negative charges and hours: adjustment/reversal lines must be invoiceable
                chg_ex := public._inv_round2(bydate.chg_ex);

                bydate_any := true;

                pay_ex := public._inv_round2(bydate.pay_ex);
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                              line_desc := coalesce(cand_display, ('TS '||v_ts_id::text)) ||
                        ' – '|| bydate.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

                meta := jsonb_build_object(
                  'line_type','HOURS_DAILY',
                  'timesheet_id', v_ts_id::text,
                  'tsfin_id', snap.id::text,
                  'candidate_display', cand_display,
                  'role', con_role,
                  'hospital', con_display_site,
                  'ward', con_ward_hint,

                  'week_ending_date', snap.week_ending_date::text,
                  'date', bydate.ymd,
                  'bucket_labels', labels,
                  'hours_day', public._inv_round2(bydate.hours_day),
                  'hours_night', public._inv_round2(bydate.hours_night),
                  'hours_sat', public._inv_round2(bydate.hours_sat),
                  'hours_sun', public._inv_round2(bydate.hours_sun),
                  'hours_bh', public._inv_round2(bydate.hours_bh),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', chg_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || v_ts_id::text || ':HOURS:' || bydate.ymd;
                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                  public._inv_round2(bydate.hours_day),
                  public._inv_round2(bydate.hours_night),
                  public._inv_round2(bydate.hours_sat),
                  public._inv_round2(bydate.hours_sun),
                  public._inv_round2(bydate.hours_bh),
                  null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;



              end loop;

              -- DAILY additional rates (only if bydate_any true in JS: wantsDailyAdd is canDaily, not bydate_any,
              -- but they later skip weekly if HOURS_DAILY exists; we mirror exactly by still doing daily-add if can_daily)
              for kv in
                select key as k, value as v
                from jsonb_each(coalesce(snap.additional_units_json, '{}'::jsonb))
              loop
                ex := kv.v;
                if ex is null or jsonb_typeof(ex) <> 'object' then
                  continue;
                end if;

                code := upper(btrim(coalesce(kv.k,'')));
                if code = '' then continue; end if;

                bucket_name := nullif(btrim(coalesce(ex->>'bucket_name','')), '');
                if bucket_name is null then bucket_name := code; end if;

                unit_name := nullif(btrim(coalesce(ex->>'unit_name','')), '');
                if unit_name is null then unit_name := 'units'; end if;

                days_obj := ex->'days';
                if days_obj is null or jsonb_typeof(days_obj) <> 'object' then
                  continue;
                end if;

                pay_rate := coalesce((ex->>'pay_rate')::numeric, 0);
                charge_rate := coalesce((ex->>'charge_rate')::numeric, 0);

                for bydate in
                  select key as ymd, (value)::numeric as units
                  from jsonb_each_text(days_obj)
                  where left(key,10) ~ '^\d{4}-\d{2}-\d{2}$'
                  order by key
                loop
                  units := coalesce(bydate.units,0);

                  -- self-bill dedupe
                  if v_all_selfbill and exists(
                    select 1 from pg_temp._inv_billed_add_keys b
                    where b.timesheet_id = v_ts_id and b.code = code and b.suffix = left(bydate.ymd,10)
                  ) then
                    continue;
                  end if;

                  pay_ex := public._inv_round2(units * pay_rate);
                  chg_ex := public._inv_round2(units * charge_rate);

                  margin_ex := public._inv_round2(chg_ex - pay_ex);
                  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                  inc_amt := public._inv_round2(chg_ex + vat_amt);

                                line_desc := coalesce(cand_display, ('TS '||v_ts_id::text)) ||
                          ' – '|| bucket_name || ' – '|| left(bydate.ymd,10) ||
                          ' – '|| units::text || ' '|| unit_name;

                                                 meta := jsonb_build_object(
                    'line_type','ADDITIONAL_RATE_DAILY',
                    'timesheet_id', v_ts_id::text,
                    'tsfin_id', snap.id::text,
                    'candidate_display', cand_display,
                    'role', con_role,
                    'hospital', con_display_site,
                    'ward', con_ward_hint,
                    'week_ending_date', snap.week_ending_date::text,
                    'date', left(bydate.ymd,10),
                    'bucket_labels', labels,
                    'bucket', jsonb_build_object(
                      'code', code,
                      'bucket_name', coalesce(ex->>'bucket_name', bucket_name),
                      'unit_name', coalesce(ex->>'unit_name', unit_name),
                      'frequency', ex->'frequency'
                    ),
                    'units', jsonb_build_object(
                      'unit_count', units,
                      'pay_rate', pay_rate,
                      'charge_rate', charge_rate
                    ),
                    'totals', jsonb_build_object(
                      'line_pay_ex_vat', pay_ex,
                      'line_charge_ex_vat', chg_ex,
                      'margin_ex_vat', margin_ex,
                      'vat_rate_pct', v_vat_rate,
                      'vat_amount', vat_amt,
                      'total_inc_vat', inc_amt
                    )
                  );



                  v_line_source_key := 'TS:' || v_ts_id::text || ':ADD:' || code || ':' || left(bydate.ymd,10);
                  insert into public.invoice_lines(
                    invoice_id, timesheet_id, booking_id, description,
                    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                    vat_rate_pct, vat_amount, total_inc_vat,
                    paper_ts_r2_key, meta_json, source_key
                  )
                  values (
                    v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                    0,0,0,0,0,
                    null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                    pay_ex, chg_ex, margin_ex,
                    v_vat_rate, vat_amt, inc_amt,
                    ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                    meta,
                    v_line_source_key
                  )
                  on conflict (invoice_id, source_key) do nothing;

                  if found then
                    insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                    values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                  end if;


                end loop;
              end loop;

              -- If any HOURS_DAILY lines exist for this ts, skip weekly fallback (mirror JS)
              if exists(
                select 1 from public.invoice_lines l
                where l.invoice_id = v_invoice_id
                  and l.timesheet_id = v_ts_id
                  and upper(coalesce(l.meta_json->>'line_type','')) = 'HOURS_DAILY'
              ) then
                continue;
              end if;
            end if;

            -- WEEKLY fallback hours line (aggregate ALL entries for this ts)
            with agg as (
              select
                sum(coalesce((e->'segment'->>'hours_day')::numeric,0)) as h_day,
                sum(coalesce((e->'segment'->>'hours_night')::numeric,0)) as h_night,
                sum(coalesce((e->'segment'->>'hours_sat')::numeric,0)) as h_sat,
                sum(coalesce((e->'segment'->>'hours_sun')::numeric,0)) as h_sun,
                sum(coalesce((e->'segment'->>'hours_bh')::numeric,0)) as h_bh,
                sum(coalesce((e->'segment'->>'pay_amount')::numeric,0)) as pay_ex,
                sum(coalesce((e->'segment'->>'charge_amount')::numeric,0)) as chg_ex
              from jsonb_array_elements(v_entries) e
              where (e->>'timesheet_id')::uuid = v_ts_id
            )
         select
  public._inv_round2(agg.pay_ex),
  public._inv_round2(agg.chg_ex),
  public._inv_round2(agg.h_day),
  public._inv_round2(agg.h_night),
  public._inv_round2(agg.h_sat),
  public._inv_round2(agg.h_sun),
  public._inv_round2(agg.h_bh)
into pay_ex, chg_ex, h_day, h_night, h_sat, h_sun, h_bh
from agg;
-- Core HOURS line must be invoiceable even when charge/hours are zero or negative (adjustments/reversals)
    margin_ex := public._inv_round2(chg_ex - pay_ex);
    vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
    inc_amt := public._inv_round2(chg_ex + vat_amt);

     line_desc := coalesce(cand_display, ('TS '||v_ts_id::text)) || ' – W/E ' || coalesce(snap.week_ending_date::text,'');

     meta := jsonb_build_object(
      'line_type','HOURS_WEEKLY',
      'timesheet_id', v_ts_id::text,
      'tsfin_id', snap.id::text,
      'candidate_display', cand_display,
      'role', con_role,
      'hospital', con_display_site,
      'ward', con_ward_hint,
      'week_ending_date', snap.week_ending_date::text,
      'bucket_labels', labels,
      'hours_day', h_day,
      'hours_night', h_night,
      'hours_sat', h_sat,
      'hours_sun', h_sun,
      'hours_bh', h_bh,
      'totals', jsonb_build_object(
        'line_pay_ex_vat', pay_ex,
        'line_charge_ex_vat', chg_ex,
        'margin_ex_vat', margin_ex,
        'vat_rate_pct', v_vat_rate,
        'vat_amount', vat_amt,
        'total_inc_vat', inc_amt
      )
    );


    -- Skip writing a zero-value weekly HOURS line (common for expenses-only timesheets).
    if not (
      coalesce(h_day,0) = 0
      and coalesce(h_night,0) = 0
      and coalesce(h_sat,0) = 0
      and coalesce(h_sun,0) = 0
      and coalesce(h_bh,0) = 0
      and coalesce(pay_ex,0) = 0
      and coalesce(chg_ex,0) = 0
    ) then
      v_line_source_key := 'TS:' || v_ts_id::text || ':HOURS:WEEK';

      insert into public.invoice_lines(
        invoice_id, timesheet_id, booking_id, description,
        hours_day, hours_night, hours_sat, hours_sun, hours_bh,
        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,
        total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
        vat_rate_pct, vat_amount, total_inc_vat,
        paper_ts_r2_key, meta_json, source_key
      )
      values (
        v_invoice_id, v_ts_id, snap.booking_id, line_desc,
        h_day, h_night, h_sat, h_sun, h_bh,
        null,null,null,null,null,
        coalesce(snap.charge_day,null),
        coalesce(snap.charge_night,null),
        coalesce(snap.charge_sat,null),
        coalesce(snap.charge_sun,null),
        coalesce(snap.charge_bh,null),

        pay_ex, chg_ex, margin_ex,
        v_vat_rate, vat_amt, inc_amt,
        ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
        meta,
        v_line_source_key
      )
      on conflict (invoice_id, source_key) do nothing;

      if found then
        insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
        values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
      end if;
    end if;
            -- WEEKLY additional rates (mirror JS)
            for kv in
              select key as k, value as v
              from jsonb_each(coalesce(snap.additional_units_json, '{}'::jsonb))
            loop
              ex := kv.v;
              if ex is null or jsonb_typeof(ex) <> 'object' then
                continue;
              end if;

              code := upper(btrim(coalesce(kv.k,'')));
              if code = '' then continue; end if;

              unit_count := coalesce((ex->>'unit_count')::numeric, 0);

              -- self-bill dedupe
              if v_all_selfbill and exists(
                select 1 from pg_temp._inv_billed_add_keys b
                where b.timesheet_id = v_ts_id and b.code = code and b.suffix = 'WEEK'
              ) then
                continue;
              end if;

              pay_ex := coalesce((ex->>'pay_ex_vat')::numeric, 0);
              chg_ex := coalesce((ex->>'charge_ex_vat')::numeric, 0);

              margin_ex := public._inv_round2(chg_ex - pay_ex);
              vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
              inc_amt := public._inv_round2(chg_ex + vat_amt);

              bucket_name := nullif(btrim(coalesce(ex->>'bucket_name','')), '');
              if bucket_name is null then bucket_name := code; end if;

              unit_name := nullif(btrim(coalesce(ex->>'unit_name','')), '');
              if unit_name is null then unit_name := 'units'; end if;

                        line_desc := coalesce(cand_display, ('TS '||v_ts_id::text)) ||
                      ' – ' || bucket_name || ' – ' || unit_count::text || ' ' || unit_name ||
                      ' (W/E ' || coalesce(snap.week_ending_date::text,'') || ')';

              meta := jsonb_build_object(
                'line_type','ADDITIONAL_RATE',
                'timesheet_id', v_ts_id::text,
                'tsfin_id', snap.id::text,
                'candidate_display', cand_display,
                               'role', con_role,
                'hospital', con_display_site,
                'ward', con_ward_hint,

                'week_ending_date', snap.week_ending_date::text,
                'bucket_labels', labels,
                'bucket', jsonb_build_object(
                  'code', code,
                  'bucket_name', coalesce(ex->>'bucket_name', bucket_name),
                  'unit_name', coalesce(ex->>'unit_name', unit_name),
                  'frequency', ex->'frequency',
                  'days', case when jsonb_typeof(ex->'days')='object' then ex->'days' else null end
                ),
                'units', jsonb_build_object(
                  'unit_count', unit_count,
                  'pay_rate', ex->'pay_rate',
                  'charge_rate', ex->'charge_rate'
                ),
                'totals', jsonb_build_object(
                  'line_pay_ex_vat', pay_ex,
                  'line_charge_ex_vat', chg_ex,
                  'margin_ex_vat', margin_ex,
                  'vat_rate_pct', v_vat_rate,
                  'vat_amount', vat_amt,
                  'total_inc_vat', inc_amt
                )
              );

              v_line_source_key := 'TS:' || v_ts_id::text || ':ADD:' || code || ':WEEK';
              insert into public.invoice_lines(
                invoice_id, timesheet_id, booking_id, description,
                hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                vat_rate_pct, vat_amount, total_inc_vat,
                paper_ts_r2_key, meta_json, source_key
              )
              values (
                v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                pay_ex, chg_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
              end if;



                     end loop;
            -- -------------------------
            -- EXPENSES lines (per category; only if category charge>0)
            -- NOTE:
            --   - Amounts come from TSFIN category columns:
            --       travel_*, accommodation_*, other_*
            --   - expenses_description is treated as NOTES only (optionally JSON).
            --   - Evidence is enforced by v_ts_invoice_precheck (timesheet_evidence.kind).
            -- -------------------------
            declare
              v_desc_txt text := null;
              v_desc_json jsonb := null;

              v_note_global text := null;
              v_note_travel text := null;
              v_note_accom  text := null;
              v_note_other  text := null;
            begin
              v_desc_txt := nullif(btrim(coalesce(snap.expenses_description,'')), '');

              if v_desc_txt is not null and left(v_desc_txt, 1) = '{' then
                begin
                  v_desc_json := v_desc_txt::jsonb;
                exception when others then
                  v_desc_json := null;
                end;

                if v_desc_json is not null and jsonb_typeof(v_desc_json) = 'object' then
                  v_note_global := nullif(btrim(coalesce(v_desc_json->>'note','')), '');

                  v_note_travel := nullif(btrim(coalesce(
                    v_desc_json #>> '{travel,note}',
                    v_desc_json->>'travel_note',
                    v_desc_json->>'travel',
                    ''
                  )), '');

                  v_note_accom := nullif(btrim(coalesce(
                    v_desc_json #>> '{accommodation,note}',
                    v_desc_json->>'accommodation_note',
                    v_desc_json->>'accommodation',
                    ''
                  )), '');

                  v_note_other := nullif(btrim(coalesce(
                    v_desc_json #>> '{other,note}',
                    v_desc_json->>'other_note',
                    v_desc_json->>'other',
                    ''
                  )), '');

                  if v_note_global is not null then
                    if v_note_travel is null then v_note_travel := v_note_global; end if;
                    if v_note_accom  is null then v_note_accom  := v_note_global; end if;
                    if v_note_other  is null then v_note_other  := v_note_global; end if;
                  end if;
                else
                  v_note_travel := v_desc_txt;
                  v_note_accom  := v_desc_txt;
                  v_note_other  := v_desc_txt;
                end if;
              else
                v_note_travel := v_desc_txt;
                v_note_accom  := v_desc_txt;
                v_note_other  := v_desc_txt;
              end if;

              -- TRAVEL
              if coalesce(snap.travel_charge_ex_vat,0) <> 0 or coalesce(snap.travel_pay_ex_vat,0) <> 0 then
                v_any_expense_line := true;
                pay_ex := public._inv_round2(coalesce(snap.travel_pay_ex_vat,0));
                chg_ex := public._inv_round2(coalesce(snap.travel_charge_ex_vat,0));
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                line_desc :=
                  'Travel expenses'
                  || case when v_note_travel is not null then ' – ' || v_note_travel else '' end
                  || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_TRAVEL',
                  'timesheet_id', v_ts_id::text,
                  'tsfin_id', snap.id::text,
                  'candidate_display', cand_display,
                  'role', con_role,
                  'hospital', con_display_site,
                  'ward', con_ward_hint,
                  'week_ending_date', snap.week_ending_date::text,
                  'expense', jsonb_build_object(
                    'category', 'TRAVEL',
                    'note', v_note_travel,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', chg_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', chg_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || v_ts_id::text || ':EXP:TRAVEL';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- ACCOMMODATION
              if coalesce(snap.accommodation_charge_ex_vat,0) <> 0 or coalesce(snap.accommodation_pay_ex_vat,0) <> 0 then
                v_any_expense_line := true;
                pay_ex := public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0));
                chg_ex := public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0));
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                line_desc :=
                  'Accommodation expenses'
                  || case when v_note_accom is not null then ' – ' || v_note_accom else '' end
                  || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_ACCOMMODATION',
                  'timesheet_id', v_ts_id::text,
                  'tsfin_id', snap.id::text,
                  'candidate_display', cand_display,
                  'role', con_role,
                  'hospital', con_display_site,
                  'ward', con_ward_hint,
                  'week_ending_date', snap.week_ending_date::text,
                  'expense', jsonb_build_object(
                    'category', 'ACCOMMODATION',
                    'note', v_note_accom,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', chg_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', chg_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || v_ts_id::text || ':EXP:ACCOMMODATION';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- OTHER
              if coalesce(snap.other_charge_ex_vat,0) <> 0 or coalesce(snap.other_pay_ex_vat,0) <> 0 then
                v_any_expense_line := true;
                pay_ex := public._inv_round2(coalesce(snap.other_pay_ex_vat,0));
                chg_ex := public._inv_round2(coalesce(snap.other_charge_ex_vat,0));
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                line_desc :=
                  'Other expenses'
                  || case when v_note_other is not null then ' – ' || v_note_other else '' end
                  || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSE_OTHER',
                  'timesheet_id', v_ts_id::text,
                  'tsfin_id', snap.id::text,
                  'candidate_display', cand_display,
                  'role', con_role,
                  'hospital', con_display_site,
                  'ward', con_ward_hint,
                  'week_ending_date', snap.week_ending_date::text,
                  'expense', jsonb_build_object(
                    'category', 'OTHER',
                    'note', v_note_other,
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', chg_ex
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', chg_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || v_ts_id::text || ':EXP:OTHER';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- EXPENSES TOTAL (fallback): if we have expenses totals but no category lines,
              -- write a single aggregate expense line so expenses-only timesheets can invoice.
              if v_any_expense_line is false and (coalesce(snap.expenses_charge_ex_vat,0) <> 0 or coalesce(snap.expenses_pay_ex_vat,0) <> 0) then
                pay_ex := public._inv_round2(coalesce(snap.expenses_pay_ex_vat,0));
                chg_ex := public._inv_round2(coalesce(snap.expenses_charge_ex_vat,0));
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                line_desc :=
                  'Expenses'
                  || case
                       when nullif(btrim(coalesce(snap.expenses_description,'')), '') is not null then ' – ' || nullif(btrim(coalesce(snap.expenses_description,'')), '')
                       when v_note_global is not null then ' – ' || v_note_global
                       else ''
                     end
                  || case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

                meta := jsonb_build_object(
                  'line_type','EXPENSES_TOTAL',
                  'timesheet_id', v_ts_id::text,
                  'tsfin_id', snap.id::text,
                  'candidate_display', cand_display,
                  'role', con_role,
                  'hospital', con_display_site,
                  'ward', con_ward_hint,
                  'week_ending_date', snap.week_ending_date::text,
                  'expense', jsonb_build_object(
                    'category', 'EXPENSES',
                    'note', nullif(btrim(coalesce(snap.expenses_description,'')), ''),
                    'pay_ex_vat', pay_ex,
                    'charge_ex_vat', chg_ex,
                    'evidence_r2_key', snap.expenses_evidence_r2_key,
                    'evidence_manifest', snap.expenses_evidence_manifest
                  ),
                  'totals', jsonb_build_object(
                    'line_pay_ex_vat', pay_ex,
                    'line_charge_ex_vat', chg_ex,
                    'margin_ex_vat', margin_ex,
                    'vat_rate_pct', v_vat_rate,
                    'vat_amount', vat_amt,
                    'total_inc_vat', inc_amt
                  )
                );

                v_line_source_key := 'TS:' || v_ts_id::text || ':EXP:TOTAL';

                insert into public.invoice_lines(
                  invoice_id, timesheet_id, booking_id, description,
                  hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                  pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                  charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                  total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                  vat_rate_pct, vat_amount, total_inc_vat,
                  paper_ts_r2_key, meta_json, source_key
                )
                values (
                  v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
                  coalesce(snap.charge_day,null),
                  coalesce(snap.charge_night,null),
                  coalesce(snap.charge_sat,null),
                  coalesce(snap.charge_sun,null),
                  coalesce(snap.charge_bh,null),
                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;
            end;


            -- -------------------------
            -- MILEAGE line (one per timesheet, if charge>0)
            -- -------------------------
            if coalesce(snap.mileage_units,0) <> 0 or coalesce(snap.mileage_charge_ex_vat,0) <> 0 or coalesce(snap.mileage_pay_ex_vat,0) <> 0 then
              unit_count := public._inv_round2(coalesce(snap.mileage_units,0));
              pay_rate := coalesce(snap.mileage_pay_rate,0);
              charge_rate := coalesce(snap.mileage_charge_rate,0);

              pay_ex := public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0));
              chg_ex := public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0));
              margin_ex := public._inv_round2(chg_ex - pay_ex);
              vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
              inc_amt := public._inv_round2(chg_ex + vat_amt);

              line_desc :=
                'Mileage – ' || unit_count::text || ' miles' ||
                case when charge_rate is not null then ' @ £' || to_char(charge_rate::numeric, 'FM999999990.00') else '' end ||
                case when snap.week_ending_date is not null then ' (W/E ' || snap.week_ending_date::text || ')' else '' end;

              meta := jsonb_build_object(
                'line_type','MILEAGE',
                'timesheet_id', v_ts_id::text,
                'tsfin_id', snap.id::text,
                'candidate_display', cand_display,
                'role', con_role,
                'hospital', con_display_site,
                'ward', con_ward_hint,
                'week_ending_date', snap.week_ending_date::text,
                'mileage', jsonb_build_object(
                  'mileage_units', unit_count,
                  'pay_rate', pay_rate,
                  'charge_rate', charge_rate,
                  'evidence_r2_key', snap.mileage_evidence_r2_key,
                  'evidence_manifest', snap.mileage_evidence_manifest
                ),
                'totals', jsonb_build_object(
                  'line_pay_ex_vat', pay_ex,
                  'line_charge_ex_vat', chg_ex,
                  'margin_ex_vat', margin_ex,
                  'vat_rate_pct', v_vat_rate,
                  'vat_amount', vat_amt,
                  'total_inc_vat', inc_amt
                )
              );

              v_line_source_key := 'TS:' || v_ts_id::text || ':MILEAGE';

              insert into public.invoice_lines(
                invoice_id, timesheet_id, booking_id, description,
                hours_day, hours_night, hours_sat, hours_sun, hours_bh,
                pay_day, pay_night, pay_sat, pay_sun, pay_bh,
                charge_day, charge_night, charge_sat, charge_sun, charge_bh,
                total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
                vat_rate_pct, vat_amount, total_inc_vat,
                paper_ts_r2_key, meta_json, source_key
              )
              values (
                v_invoice_id, v_ts_id, snap.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
      coalesce(snap.charge_day,null),
      coalesce(snap.charge_night,null),
      coalesce(snap.charge_sat,null),
      coalesce(snap.charge_sun,null),
      coalesce(snap.charge_bh,null),

                pay_ex, chg_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || v_ts_id::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (v_ts_id, v_line_source_key, chg_ex, vat_amt, inc_amt);
              end if;
            end if;

          end loop; -- per timesheet


          if not exists (select 1 from public.invoice_lines where invoice_id = v_invoice_id) then
            raise exception 'Nothing to invoice (all billable amounts are zero after daily/weekly rules).';
          end if;

             -- Totals: recompute from all lines (matches end outcome; avoids additive drift)

      update public.invoices i
      set
        subtotal_ex_vat = x.ex,
        vat_amount      = x.vat,
        total_inc_vat   = x.inc,
        updated_at      = v_now
      from (
        select
          coalesce(sum(l.total_charge_ex_vat),0)::numeric as ex,
          coalesce(sum(l.vat_amount),0)::numeric as vat,
          coalesce(sum(l.total_inc_vat),0)::numeric as inc
        from public.invoice_lines l
        where l.invoice_id = v_invoice_id
      ) x
      where i.id = v_invoice_id;

      -- Compute NEW totals and delta (NEW - PREV)
      select
        coalesce(i.subtotal_ex_vat,0)::numeric,
        coalesce(i.vat_amount,0)::numeric,
        coalesce(i.total_inc_vat,0)::numeric
      into v_new_ex, v_new_vat, v_new_inc
      from public.invoices i
      where i.id = v_invoice_id
      limit 1;

      v_delta_ex  := public._inv_round2(v_new_ex  - v_prev_ex);
      v_delta_vat := public._inv_round2(v_new_vat - v_prev_vat);
      v_delta_inc := public._inv_round2(v_new_inc - v_prev_inc);

      select
        array_agg(distinct rl.timesheet_id),
        array_agg(distinct rl.source_key),
        count(*)::int
      into
        v_run_ts_ids,
        v_run_source_keys,
        v_run_line_count
      from pg_temp._inv_run_lines rl;

      if v_invoice_debug then
        v_dbg_grp_i := coalesce(v_dbg_grp_i,0) + 1;
        v_dbg_groups_detail := v_dbg_groups_detail || jsonb_build_array(
          jsonb_build_object(
            'group_index', v_dbg_grp_i,
            'consolidation_mode', v_consol_mode,
            'mode', v_mode,
            'all_selfbill', coalesce(v_all_selfbill,false),

            'ts_ids', to_jsonb(coalesce(v_ts_ids_to_use, array[]::uuid[])),
            'ts_id_count', coalesce(array_length(v_ts_ids_to_use,1),0),
            'entry_count', coalesce(v_entry_count,0),

            'invoice_id', case when v_invoice_id is null then null else v_invoice_id::text end,
            'invoice_created', coalesce(v_created,false),
            'invoice_status_before', v_prev_status,
            'invoice_status_after', (select i.status::text from public.invoices i where i.id = v_invoice_id limit 1),

            'prev_totals', jsonb_build_object(
              'subtotal_ex_vat', public._inv_round2(v_prev_ex),
              'vat_amount', public._inv_round2(v_prev_vat),
              'total_inc_vat', public._inv_round2(v_prev_inc)
            ),
            'new_totals', jsonb_build_object(
              'subtotal_ex_vat', public._inv_round2(v_new_ex),
              'vat_amount', public._inv_round2(v_new_vat),
              'total_inc_vat', public._inv_round2(v_new_inc)
            ),
            'delta_totals', jsonb_build_object(
              'subtotal_ex_vat', v_delta_ex,
              'vat_amount', v_delta_vat,
              'total_inc_vat', v_delta_inc
            ),

            'this_run', jsonb_build_object(
              'timesheet_ids', to_jsonb(coalesce(v_run_ts_ids, array[]::uuid[])),
              'source_keys', to_jsonb(coalesce(v_run_source_keys, array[]::text[])),
              'line_count', coalesce(v_run_line_count,0)
            )
          )
        );
      end if;

      -- Write ONE audit row that proves this run + what changed (including later append runs)
      if (coalesce(v_delta_ex,0) <> 0 or coalesce(v_delta_vat,0) <> 0 or coalesce(v_delta_inc,0) <> 0) then
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_TOTALS_DELTA_APPLIED',
          jsonb_build_object(
            'outbox_id', v_outbox_id::text,
            'job_kind', v_kind,
            'run_at_utc', public._inv_iso_utc(v_now),

            'invoice_id', v_invoice_id::text,
            'invoice_no', v_prev_invoice_no,
            'client_id', v_client_id::text,
            'invoice_week_start', v_week_start::text,
            'mode', v_mode,

            'invoice_status_before', v_prev_status,
            'invoice_status_after', (select i.status::text from public.invoices i where i.id = v_invoice_id limit 1),

            'prev_subtotal_ex_vat', public._inv_round2(v_prev_ex),
            'prev_vat_amount', public._inv_round2(v_prev_vat),
            'prev_total_inc_vat', public._inv_round2(v_prev_inc),

            'delta_subtotal_ex_vat', v_delta_ex,
            'delta_vat_amount', v_delta_vat,
            'delta_total_inc_vat', v_delta_inc,

            'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
            'new_vat_amount', public._inv_round2(v_new_vat),
            'new_total_inc_vat', public._inv_round2(v_new_inc),

            'timesheet_ids_this_run', to_jsonb(coalesce(v_run_ts_ids, array[]::uuid[])),
            'source_keys_this_run', to_jsonb(coalesce(v_run_source_keys, array[]::text[])),
            'line_count_this_run', coalesce(v_run_line_count,0)
          ),
          'invoices',
          v_invoice_id::text,
          jsonb_build_object(
            'subtotal_ex_vat', public._inv_round2(v_prev_ex),
            'vat_amount', public._inv_round2(v_prev_vat),
            'total_inc_vat', public._inv_round2(v_prev_inc)
          ),
          'RUN_TOTALS_DELTA',
          v_ip, v_ua, v_corr
        );
      end if;

          -- Lock segments (mirror JS lockSegmentsForInvoice) using segmentRefs built from entries
          select jsonb_agg(
            jsonb_build_object(
              'tsfin_id', e->>'tsfin_id',
              'timesheet_id', e->>'timesheet_id',
              'segment_id', (e->'segment'->>'segment_id'),
              'basis', e->>'basis'
            )
          )
          into meta
          from jsonb_array_elements(v_entries) e;

          perform public._inv_lock_segments_for_invoice(v_invoice_id, meta);

          -- AUDIT: TIMESHEET_SEGMENTS_INVOICED (per timesheet)
          for v_ts_id in
            select distinct (e->>'timesheet_id')::uuid
            from jsonb_array_elements(v_entries) e
          loop
            -- segment_count and sums based on HOURS_* lines inserted
            select
              count(*)::int,
              coalesce(sum(l.total_charge_ex_vat),0)::numeric,
              coalesce(sum(l.total_pay_ex_vat),0)::numeric
            into v_terms_days, chg_ex, pay_ex
            from public.invoice_lines l
            where l.invoice_id = v_invoice_id
              and l.timesheet_id = v_ts_id
              and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY');

            -- bases: DAILY if any HOURS_DAILY exists; else first basis in entries order for this ts
            select exists(
              select 1 from public.invoice_lines l
              where l.invoice_id = v_invoice_id
                and l.timesheet_id = v_ts_id
                and upper(coalesce(l.meta_json->>'line_type','')) = 'HOURS_DAILY'
            ) into bydate_any;

            if bydate_any then
              meta := to_jsonb(array['DAILY']::text[]);
            else
              select to_jsonb(array[ upper(coalesce(e->>'basis','WEEKLY')) ]::text[])
              into meta
              from jsonb_array_elements(v_entries) e
              where (e->>'timesheet_id')::uuid = v_ts_id
              order by (e->>'entry_ord')::int asc
              limit 1;
            end if;

            perform public._inv_write_audit(
              p_actor_user_id,
              'TIMESHEET_SEGMENTS_INVOICED',
              jsonb_build_object(
                'timesheet_id', v_ts_id::text,
                'invoice_id', v_invoice_id::text,
                'invoice_week_start', v_week_start::text,
                'segment_count', v_terms_days,
                'total_charge_ex_vat', public._inv_round2(chg_ex),
                'total_pay_ex_vat', public._inv_round2(pay_ex),
                'bases', coalesce(meta, '[]'::jsonb)
              ),
              'timesheets',
              v_ts_id::text,
              null,
              null,
              v_ip, v_ua, v_corr
            );
          end loop;

          -- Mark weeks invoiced where fully locked (matches JS: locked_by_invoice_id=invoice.id)
          select array_agg(distinct tf.timesheet_id)
          into v_timesheet_ids
          from public.timesheets_financials tf
          where tf.is_current = true
            and tf.locked_by_invoice_id = v_invoice_id
            and tf.timesheet_id is not null;

          if v_timesheet_ids is not null and coalesce(array_length(v_timesheet_ids,1),0) > 0 then
            update public.contract_weeks
            set status = 'INVOICED'::public.contract_week_status_enum,
                updated_at = v_now
            where timesheet_id = any(v_timesheet_ids);

            for v_ts_id in
              select unnest(v_timesheet_ids)
            loop
              perform public._inv_write_audit(
                p_actor_user_id,
                'TIMESHEET_INVOICED',
                jsonb_build_object(
                  'timesheet_id', v_ts_id::text,
                  'invoice_id', v_invoice_id::text,
                  'invoice_week_start', v_week_start::text
                ),
                'timesheets',
                v_ts_id::text,
                null,
                'LOCKED_BY_INVOICE',
                v_ip, v_ua, v_corr
              );
            end loop;
          end if;

              -- group completed successfully
              v_succeeded_any := true;
            exception when others then
              if v_consol_mode <> 'NONE' then
                raise;
              end if;

              -- restore outer-scope accumulators (DB changes are rolled back automatically for this group)
              v_outbox_invoice_ids := v_grp_invoice_ids_before;
              v_outbox_warnings := v_grp_warnings_before;
              v_invoice_id := v_grp_invoice_id_before;

              v_failed_ts_ids := array_cat(v_failed_ts_ids, grp.ts_ids);

              v_failed_groups := v_failed_groups || jsonb_build_array(
                jsonb_build_object(
                  'ts_ids', to_jsonb(grp.ts_ids),
                  'error', sqlerrm
                )
              );

              v_outbox_warnings := v_outbox_warnings || jsonb_build_array(
                jsonb_build_object(
                  'kind','GROUP_FAILED',
                  'ts_ids', to_jsonb(grp.ts_ids),
                  'error', sqlerrm
                )
              );

              continue;
            end;
          end loop;
          -- Option A: partial failures allowed only when consolidation mode is NONE (one invoice per timesheet)
          if v_consol_mode = 'NONE' and coalesce(array_length(v_failed_ts_ids,1),0) > 0 then

            -- de-duplicate failed ids (defensive)
            select coalesce(array_agg(distinct x), array[]::uuid[])
            into v_failed_ts_ids
            from unnest(v_failed_ts_ids) x;

            update public.invoice_jobs_outbox o
            set
              payload = jsonb_set(coalesce(o.payload,'{}'::jsonb), '{timesheet_ids}', to_jsonb(v_failed_ts_ids), true),
              next_attempt_at = now() + interval '5 minutes',
              last_error = 'PARTIAL_FAILURE'
            where o.id = v_outbox_id;

            outbox_id := v_outbox_id;
            ok := false;
            invoice_ids := v_outbox_invoice_ids;
            warnings := jsonb_build_object(
              'kind','BY_WEEK',
              'partial_failure', true,
              'succeeded_any', v_succeeded_any,
              'failed', v_failed_groups,
              'created_invoice_ids', v_outbox_invoice_ids,
              'warnings', v_outbox_warnings,
              'client_id', v_client_id::text,
              'invoice_week_start', v_week_start::text,
              'mode', v_mode
            );

            if v_invoice_debug then
              v_dbg_results := v_dbg_results || jsonb_build_array(
                jsonb_build_object(
                  'outbox_id', v_outbox_id::text,
                  'kind', coalesce(v_kind,''),
                  'client_id', coalesce(v_client_id::text,''),
                  'mode', coalesce(v_mode,''),
                  'debug', jsonb_build_object(
                    'partial_failure', true,
                    'failed_ts_ids', to_jsonb(v_failed_ts_ids),
                    'failed_groups', v_failed_groups,
                    'created_invoice_ids', v_outbox_invoice_ids
                  )
                )
              );
            end if;

            return next;
            continue;
          end if;

          foreach v_invoice_id in array coalesce(v_outbox_invoice_ids,array[]::uuid[]) loop
            perform public._ctms_assert_invoice_correction_lines_v1(
              v_invoice_id,p_actor_user_id,true,'INVOICE_GENERATE_FROM_OUTBOX'
            );
          end loop;

          -- SUCCESS: delete outbox row
          delete from public.invoice_jobs_outbox where id = v_outbox_id;

          outbox_id := v_outbox_id;
          ok := true;
          invoice_ids := v_outbox_invoice_ids;
          warnings := jsonb_build_object(
            'kind','BY_WEEK',
            'invoice_ids', v_outbox_invoice_ids,
            'invoice_id', v_invoice_id::text,
            'warnings', v_outbox_warnings,

            'client_id', v_client_id::text,
            'invoice_week_start', v_week_start::text,
            'mode', v_mode
          );
                if v_invoice_debug then
                  v_dbg_results := v_dbg_results || jsonb_build_array(
                    jsonb_build_object(
                      'outbox_id', v_outbox_id::text,
                      'kind', coalesce(v_kind,''),
                      'payload', coalesce(v_payload,'{}'::jsonb),
                      'decision_trace', jsonb_build_object(
                        'branch','BY_WEEK',
                        'client_id', case when v_client_id is null then null else v_client_id::text end,
                        'invoice_week_start', case when v_week_start is null then null else v_week_start::text end,
                        'allow_early', coalesce(v_allow_early,false),
                        'consolidation_mode', v_consol_mode,
                        'all_selfbill', coalesce(v_all_selfbill,false),
                        'mode', v_mode,
                        'limit_ts_ids', case when v_limit_ts_ids is null then null else to_jsonb(v_limit_ts_ids) end,
                        'entries_count', v_entry_count,
                        'entries_timesheet_ids', to_jsonb(v_timesheet_ids),
                        'meta_count', coalesce(v_dbg_meta_count,0),
                        'meta_sample', coalesce(v_dbg_meta_sample,'[]'::jsonb),
                        'entries_sample', coalesce(v_dbg_entries_sample,'[]'::jsonb),
                        'timesheet_ids_pre_grouping', to_jsonb(coalesce(v_dbg_timesheet_ids_pre, array[]::uuid[])),
                        'ts_ids_to_use_pre_grouping', case when v_dbg_ts_ids_to_use_pre is null then null else to_jsonb(v_dbg_ts_ids_to_use_pre) end,
                        'groups_count', coalesce(v_dbg_groups_count,0),
                        'groups_rows', coalesce(v_dbg_groups_rows,'[]'::jsonb),
                        'groups_reason', v_dbg_groups_reason,
                        'groups_detail', coalesce(v_dbg_groups_detail,'[]'::jsonb),
                        'created_invoice_ids', to_jsonb(v_outbox_invoice_ids),
                        'invoice_summaries', (
                          select coalesce(jsonb_agg(
                            jsonb_build_object(
                              'invoice_id', i.id::text,
                              'invoice_no', i.invoice_no,
                              'status', i.status::text,
                              'subtotal_ex_vat', coalesce(i.subtotal_ex_vat,0)::numeric,
                              'vat_amount', coalesce(i.vat_amount,0)::numeric,
                              'total_inc_vat', coalesce(i.total_inc_vat,0)::numeric,
                              'line_count', (select count(*) from public.invoice_lines l where l.invoice_id=i.id)
                            )
                            order by i.created_at desc nulls last
                          ), '[]'::jsonb)
                          from public.invoices i
                          where i.id = any(v_outbox_invoice_ids)
                        )
                      ),
                      'result', jsonb_build_object(
                        'ok', ok,
                        'invoice_ids', case when invoice_ids is null then null else to_jsonb(invoice_ids) end,
                        'warnings', warnings
                      ),
                      'job_row', jsonb_build_object(
                        'attempt_count', coalesce(v_dbg_job_attempt_count,0),
                        'next_attempt_at', to_jsonb(v_dbg_job_next_attempt_at),
                        'last_error', v_dbg_job_last_error,
                        'created_at', to_jsonb(v_dbg_job_created_at)
                      ),
                      'timing', jsonb_build_object(
                        'now_utc', public._inv_iso_utc(v_now),
                        'anchor_ymd', v_anchor_ymd::text
                      ),
                      'audit_meta', jsonb_build_object(
                        'ip', v_ip,
                        'user_agent', v_ua,
                        'correlation_id', v_corr
                      )
                    )
                  );

                  if v_dbg_first_ip is null and v_ip is not null then v_dbg_first_ip := v_ip; end if;
                  if v_dbg_first_ua is null and v_ua is not null then v_dbg_first_ua := v_ua; end if;
                  if v_dbg_first_corr is null and v_corr is not null then v_dbg_first_corr := v_corr; end if;
                end if;
          return next;
          continue;
        end;

      else
        raise exception 'Unsupported invoice outbox kind: %', v_kind;
      end if;

    exception when others then
      -- failure: backoff + store error
  update public.invoice_jobs_outbox
set
  next_attempt_at = now() + interval '5 minutes',
  last_error = sqlerrm
where id = v_outbox_id;


      outbox_id := v_outbox_id;
      ok := false;
      invoice_ids := null;
      warnings := jsonb_build_object('error', sqlerrm);
            if v_invoice_debug then
              v_dbg_results := v_dbg_results || jsonb_build_array(
                jsonb_build_object(
                  'outbox_id', v_outbox_id::text,
                  'kind', coalesce(v_kind,''),
                  'payload', coalesce(v_payload,'{}'::jsonb),
                  'decision_trace', jsonb_build_object(
                    'branch','EXCEPTION',
                    'sqlerrm', sqlerrm
                  ),
                  'result', jsonb_build_object(
                    'ok', ok,
                    'invoice_ids', case when invoice_ids is null then null else to_jsonb(invoice_ids) end,
                    'warnings', warnings
                  ),
                  'job_row', jsonb_build_object(
                    'attempt_count', coalesce(v_dbg_job_attempt_count,0),
                    'next_attempt_at', to_jsonb(v_dbg_job_next_attempt_at),
                    'last_error', v_dbg_job_last_error,
                    'created_at', to_jsonb(v_dbg_job_created_at)
                  ),
                  'timing', jsonb_build_object(
                    'now_utc', public._inv_iso_utc(v_now),
                    'anchor_ymd', v_anchor_ymd::text
                  ),
                  'audit_meta', jsonb_build_object(
                    'ip', v_ip,
                    'user_agent', v_ua,
                    'correlation_id', v_corr
                  )
                )
              );

              if v_dbg_first_ip is null and v_ip is not null then v_dbg_first_ip := v_ip; end if;
              if v_dbg_first_ua is null and v_ua is not null then v_dbg_first_ua := v_ua; end if;
              if v_dbg_first_corr is null and v_corr is not null then v_dbg_first_corr := v_corr; end if;
            end if;
      return next;
      continue;
    end;
  end loop;
  -- Write one debug audit row for the whole RPC call (if enabled)
  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      'INVOICE_GENERATOR_DEBUG',
      jsonb_build_object(
        'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
        'run_finished_at_utc', public._inv_iso_utc(now()),
        'outbox_ids', to_jsonb(p_outbox_ids),
        'outbox_count', coalesce(array_length(p_outbox_ids,1),0),
        'results', v_dbg_results
      ),
      'invoice_jobs_outbox',
      ('batch:' || public._inv_iso_utc(v_dbg_run_started)),
      null,
      'INVOICE_DEBUG',
      v_dbg_first_ip,
      v_dbg_first_ua,
      v_dbg_first_corr
    );
  end if;
end;
$function$;

-- invoice_hold_batch(uuid[],uuid,text)
CREATE OR REPLACE FUNCTION public.invoice_hold_batch(p_invoice_ids uuid[], p_actor_user_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, on_hold_reason text, error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.on_hold_reason,
        null::text
      into invoice_id, ok, status, on_hold_reason, error
      from public.invoice_hold_one(v_id, p_actor_user_id, p_reason) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      on_hold_reason := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_hold_one(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.invoice_hold_one(p_invoice_id uuid, p_actor_user_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS TABLE(status text, on_hold_reason text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  if v_reason is null then
    v_reason := 'Placed on hold';
  end if;

    declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot hold a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot hold a PAID invoice';
    end if;
  end;

  update public.invoices
  set status = 'ON_HOLD'::public.invoice_status_enum,
      status_date_utc = v_now,
      on_hold_reason = v_reason
  where id = p_invoice_id;


  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_HELD',
    null,
    jsonb_build_object('reason', v_reason),
    null,
    p_actor_user_id
  );

  status := 'ON_HOLD';
  on_hold_reason := v_reason;
  return next;
end;
$function$;

-- invoice_issue_and_queue_emails_batch(uuid[],uuid,boolean)
CREATE OR REPLACE FUNCTION public.invoice_issue_and_queue_emails_batch(p_invoice_ids uuid[], p_actor_user_id uuid, p_allow_early boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_ids uuid[];
  v_allowed uuid[];
  v_not_due uuid[];

  v_max_attach int := 30;

  v_issue_json jsonb := '[]'::jsonb;
  v_not_due_json jsonb := '[]'::jsonb;
  v_email_json jsonb := '[]'::jsonb;
  v_email_warn_json jsonb := '[]'::jsonb;

  v_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_sqlstate text;
  v_err text;

  -- ✅ NEW: email dedupe lock helpers (prevents duplicate mail_outbox rows on concurrent runs)
  v_ref text;
  m_lock record;

  -- ✅ NEW: invoice-pdf outbox enqueue summary (Option A)
  v_pdf_invoice_ids uuid[] := null;
  v_pdf_invoice_count int := 0;
  v_pdf_rows_affected int := 0;

begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  -- normalize ids
  select array_agg(x order by x::text)
  into v_ids
  from (
    select distinct unnest(p_invoice_ids) as x
  ) q
  where q.x is not null;

  if v_ids is null or coalesce(array_length(v_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  -- global chunk size
  select coalesce(sd.max_attachments_per_email, 30)
  into v_max_attach
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  select coalesce(sd.invoice_debug,false)
  into v_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','start','now_utc',v_now::text,'anchor_ymd',v_anchor_ymd::text,'allow_early',coalesce(p_allow_early,false)));
  end if;

  if v_max_attach is null or v_max_attach < 1 then
    v_max_attach := 30;
  end if;

  -- gate by week end unless allow_early=true
  create temporary table tmp_gate on commit drop as
  with inv as (
    select
      i.id as invoice_id,
      i.client_id,
      i.invoice_no,
      i.header_snapshot_json,
      i.status::text as status,

      -- self-bill detect
      (
        lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', i.header_snapshot_json->>'self_bill', '')) in ('true','t','1','yes')
      ) as is_self_bill,

      -- week_start fallback (header)
      nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}',
                            i.header_snapshot_json->>'invoice_week_start', '')), '') as hdr_week_start_txt
    from public.invoices i
    where i.id = any(v_ids)
      and i.type::text = 'INVOICE'
      and i.status::text in ('DRAFT','ON_HOLD','ISSUED')  -- allow idempotency if already issued
  ),
  wk as (
    select
      inv.invoice_id,
      inv.client_id,
      inv.invoice_no,
      inv.status,
      inv.is_self_bill,

      case
        when inv.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$' then inv.hdr_week_start_txt::date
        else null::date
      end as invoice_week_start,

      coalesce(
        max(ts.week_ending_date)::date,
        case
          when inv.hdr_week_start_txt ~ '^\d{4}-\d{2}-\d{2}$'
            then (inv.hdr_week_start_txt::date + interval '6 days')::date
          else null::date
        end
      ) as week_ending_date

    from inv
    left join public.invoice_lines il
      on il.invoice_id = inv.invoice_id
     and il.timesheet_id is not null
    left join public.timesheets ts
      on ts.timesheet_id = il.timesheet_id
    group by inv.invoice_id, inv.client_id, inv.invoice_no, inv.status, inv.is_self_bill, inv.hdr_week_start_txt
  )
  select
    wk.invoice_id,
    wk.client_id,
    wk.invoice_no,
    wk.status,
    wk.is_self_bill,
    wk.invoice_week_start,
    wk.week_ending_date,
    (
      p_allow_early = true
      or wk.week_ending_date is null
      or wk.week_ending_date < v_anchor_ymd
    ) as due_ok
  from wk;

  -- split allowed vs not_due
  select array_agg(g.invoice_id order by g.invoice_id::text)
  into v_allowed
  from tmp_gate g
  where g.due_ok = true;

  select array_agg(g.invoice_id order by g.invoice_id::text)
  into v_not_due
  from tmp_gate g
  where g.due_ok = false;

  -- build NOT_DUE_YET results (for UI)
  if v_not_due is not null and array_length(v_not_due,1) > 0 then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', g.invoice_id::text,
          'ok', false,
          'status', null,
          'issued_at_utc', null,
          'on_hold_reason', null,
          'reasons', null,
          'error', 'NOT_DUE_YET'
        )
        order by g.invoice_id::text
      ),
      '[]'::jsonb
    )
    into v_not_due_json
    from tmp_gate g
    where g.invoice_id = any(v_not_due);
  end if;

  -- issue allowed invoices (if any)
  create temporary table tmp_issue on commit drop as
  select *
  from public.invoice_issue_batch(coalesce(v_allowed, array[]::uuid[]), p_actor_user_id);

  select coalesce(jsonb_agg(to_jsonb(t) order by t.invoice_id::text), '[]'::jsonb)
  into v_issue_json
  from tmp_issue t;

  -- ✅ NEW (Option A): enqueue invoice PDF render jobs for successfully ISSUED invoices (idempotent)
  select array_agg(x.invoice_id order by x.invoice_id::text)
  into v_pdf_invoice_ids
  from (
    select distinct t.invoice_id
    from tmp_issue t
    where t.ok = true
      and upper(coalesce(t.status,'')) = 'ISSUED'
  ) x;

  v_pdf_invoice_count := coalesce(array_length(v_pdf_invoice_ids, 1), 0);

  if v_pdf_invoice_count > 0 then
    -- p_limit is a safety cap; here we cap at the number of unique invoice_ids in this batch.
    v_pdf_rows_affected := public.invpdf_enqueue_many(v_pdf_invoice_ids, false, v_pdf_invoice_count);
  else
    v_pdf_rows_affected := 0;
  end if;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','invpdf_enqueued_after_issue',
      'pdf_invoice_count', v_pdf_invoice_count,
      'pdf_rows_affected', v_pdf_rows_affected
    ));
  end if;

  -- queue emails for successfully ISSUED invoices, excluding self-bill/do_not_send.
  -- Recipient routing is derived before tmp_to_email so batch queueing matches handleInvoiceEmail(...).
  create temporary table tmp_email_route_base on commit drop as
  select
    i.id as invoice_id,
    i.client_id,
    i.invoice_no,
    g.week_ending_date,
    coalesce(
      nullif(btrim(coalesce(i.header_snapshot_json->>'client_primary_invoice_email','')), ''),
      nullif(btrim(coalesce(c.primary_invoice_email,'')), '')
    ) as primary_to_email,
    g.is_self_bill,
    coalesce(i.do_not_send,false) as do_not_send
  from tmp_issue r
  join public.invoices i
    on i.id = r.invoice_id
  join tmp_gate g
    on g.invoice_id = i.id
  join public.clients c
    on c.id = i.client_id
  where r.ok = true
    and upper(coalesce(r.status,'')) = 'ISSUED';

  create temporary table tmp_latest_client_settings on commit drop as
  select distinct on (cs.client_id)
    cs.client_id,
    cs.send_manual_invoices_to_different_email,
    cs.manual_invoices_alt_email_address,
    cs.effective_from,
    cs.created_at
  from public.client_settings cs
  join (
    select distinct rb.client_id
    from tmp_email_route_base rb
    where rb.client_id is not null
  ) bc
    on bc.client_id = cs.client_id
  order by cs.client_id, cs.effective_from desc nulls last, cs.created_at desc nulls last;

  create temporary table tmp_email_route_lines on commit drop as
  select
    rb.invoice_id,
    il.timesheet_id,
    (il.timesheet_id is not null and ts.timesheet_id is null) as missing_current_timesheet,
    ts.contract_id as timesheet_contract_id,
    cw.contract_id as contract_week_contract_id,
    coalesce(ts.contract_id, cw.contract_id) as contract_id,
    coalesce(ts.is_adjustment,false) as timesheet_is_adjustment,
    coalesce(cw.is_adjustment,false) as contract_week_is_adjustment,
    (
      coalesce(ts.is_adjustment,false) = true
      and (
        left(upper(coalesce(ts.adjustment_origin::text,'')), 7) = 'IMPORT_'
        or ts.correction_id is not null
        or nullif(btrim(coalesce(ts.correction_kind::text,'')), '') is not null
      )
    ) as is_import_derived_adjustment,
    (
      upper(coalesce(ts.submission_mode::text,'')) in ('MANUAL','QR')
      or nullif(btrim(coalesce(ts.qr_status::text,'')), '') is not null
      or nullif(btrim(coalesce(ts.qr_token::text,'')), '') is not null
    ) as is_manual_or_qr,
    (
      (coalesce(ts.is_adjustment,false) = true or coalesce(cw.is_adjustment,false) = true)
      and (
        upper(coalesce(ts.submission_mode::text,'')) in ('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')), '') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')), '') is not null
      )
      and not (
        coalesce(ts.is_adjustment,false) = true
        and (
          left(upper(coalesce(ts.adjustment_origin::text,'')), 7) = 'IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')), '') is not null
        )
      )
    ) as is_user_created_manual_qr_adjustment
  from tmp_email_route_base rb
  left join public.invoice_lines il
    on il.invoice_id = rb.invoice_id
   and il.timesheet_id is not null
  left join public.timesheets ts
    on ts.timesheet_id = il.timesheet_id
   and ts.is_current = true
  left join lateral (
    select
      coalesce(bool_or(coalesce(cw0.is_adjustment,false)), false) as is_adjustment,
      (array_agg(cw0.contract_id order by cw0.contract_id::text) filter (where cw0.contract_id is not null))[1] as contract_id
    from public.contract_weeks cw0
    where cw0.timesheet_id = il.timesheet_id
  ) cw
    on true;

  create temporary table tmp_email_route_flags on commit drop as
  select
    rb.invoice_id,
    rb.client_id,
    rb.invoice_no,
    rb.week_ending_date,
    rb.primary_to_email,
    rb.is_self_bill,
    rb.do_not_send,
    coalesce(bool_or(coalesce(erl.missing_current_timesheet,false)), false) as has_missing_current_timesheet,
    coalesce(bool_or(coalesce(erl.is_import_derived_adjustment,false)), false) as has_import_derived_adjustment,
    coalesce(bool_or(coalesce(erl.is_user_created_manual_qr_adjustment,false)), false) as has_user_created_manual_qr_adjustment,
    coalesce(bool_or(coalesce(erl.is_user_created_manual_qr_adjustment,false) = true and erl.contract_id is null), false) as has_missing_user_created_manual_qr_contract
  from tmp_email_route_base rb
  left join tmp_email_route_lines erl
    on erl.invoice_id = rb.invoice_id
  group by
    rb.invoice_id,
    rb.client_id,
    rb.invoice_no,
    rb.week_ending_date,
    rb.primary_to_email,
    rb.is_self_bill,
    rb.do_not_send;

  create temporary table tmp_email_route_contracts on commit drop as
  select distinct
    erl.invoice_id,
    erl.contract_id,
    (ct.id is null) as missing_contract,
    coalesce(ct.overrideclientsettings,false) as overrideclientsettings,
    coalesce(ct.send_manual_invoices_to_different_email,false) as send_manual_invoices_to_different_email,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')), '') as manual_invoices_alt_email_address,
    (
      coalesce(ct.overrideclientsettings,false) = true
      and coalesce(ct.send_manual_invoices_to_different_email,false) = true
    ) as override_enabled
  from tmp_email_route_lines erl
  left join public.contracts ct
    on ct.id = erl.contract_id
  where erl.is_user_created_manual_qr_adjustment = true
    and erl.contract_id is not null;

  create temporary table tmp_email_route_contract_agg on commit drop as
  select
    erc.invoice_id,
    coalesce(bool_or(coalesce(erc.missing_contract,false)), false) as missing_contract,
    coalesce(bool_or(coalesce(erc.override_enabled,false)), false) as has_enabled_contract_override,
    coalesce(bool_or(coalesce(erc.override_enabled,false) and erc.manual_invoices_alt_email_address is null), false) as has_missing_contract_alt_email,
    count(distinct erc.manual_invoices_alt_email_address) filter (
      where erc.override_enabled = true
        and erc.manual_invoices_alt_email_address is not null
    ) as contract_alt_email_count,
    (array_agg(distinct erc.manual_invoices_alt_email_address) filter (
      where erc.override_enabled = true
        and erc.manual_invoices_alt_email_address is not null
    ))[1] as contract_alt_email,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', erc.contract_id::text,
          'email', erc.manual_invoices_alt_email_address
        )
      ) filter (
        where erc.override_enabled = true
          and erc.manual_invoices_alt_email_address is not null
      ),
      '[]'::jsonb
    ) as contract_alt_details,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', erc.contract_id::text
        )
      ) filter (
        where erc.override_enabled = true
          and erc.manual_invoices_alt_email_address is null
      ),
      '[]'::jsonb
    ) as missing_contract_alt_details
  from tmp_email_route_contracts erc
  group by erc.invoice_id;

  create temporary table tmp_email_queue_warnings on commit drop as
  select
    erf.invoice_id,
    'EMAIL_ROUTING_CHECK_FAILED'::text as warning_code,
    'Current timesheet data could not be loaded for one or more invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'EMAIL_ROUTING_CHECK_FAILED',
      'message', 'Current timesheet data could not be loaded for one or more invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_missing_current_timesheet = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_ROUTING_CHECK_FAILED'::text as warning_code,
    'Contract data could not be resolved for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_ROUTING_CHECK_FAILED',
      'message', 'Contract data could not be resolved for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erf.has_missing_user_created_manual_qr_contract = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_ROUTING_CHECK_FAILED'::text as warning_code,
    'Contract data could not be loaded for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_ROUTING_CHECK_FAILED',
      'message', 'Contract data could not be loaded for one or more user-created manual/QR adjustment invoice lines, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erca.missing_contract = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_MANUAL_EMAIL_MISSING'::text as warning_code,
    'Contract manual invoice email is enabled but no alternate email address is configured, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_MANUAL_EMAIL_MISSING',
      'message', 'Contract manual invoice email is enabled but no alternate email address is configured, so invoice email routing was not queued.',
      'contracts', coalesce(erca.missing_contract_alt_details, '[]'::jsonb)
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and erca.has_missing_contract_alt_email = true

  union all

  select
    erf.invoice_id,
    'CONTRACT_MANUAL_EMAIL_CONFLICT'::text as warning_code,
    'Multiple contract manual invoice email overrides apply to this invoice and they disagree, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CONTRACT_MANUAL_EMAIL_CONFLICT',
      'message', 'Multiple contract manual invoice email overrides apply to this invoice and they disagree, so invoice email routing was not queued.',
      'contracts', coalesce(erca.contract_alt_details, '[]'::jsonb)
    ) as warning_json
  from tmp_email_route_flags erf
  join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and coalesce(erca.contract_alt_email_count,0) > 1

  union all

  select
    erf.invoice_id,
    'CLIENT_MANUAL_EMAIL_MISSING'::text as warning_code,
    'Client manual adjustment email is enabled but no alternate email address is configured, so invoice email routing was not queued.'::text as warning_message,
    jsonb_build_object(
      'invoice_id', erf.invoice_id::text,
      'warning_code', 'CLIENT_MANUAL_EMAIL_MISSING',
      'message', 'Client manual adjustment email is enabled but no alternate email address is configured, so invoice email routing was not queued.'
    ) as warning_json
  from tmp_email_route_flags erf
  left join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  left join tmp_latest_client_settings lcs
    on lcs.client_id = erf.client_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and erf.has_user_created_manual_qr_adjustment = true
    and coalesce(erca.has_enabled_contract_override,false) = false
    and coalesce(lcs.send_manual_invoices_to_different_email,false) = true
    and nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '') is null;

  select coalesce(
    jsonb_agg(t.warning_json order by t.invoice_id::text, t.warning_code),
    '[]'::jsonb
  )
  into v_email_warn_json
  from tmp_email_queue_warnings t;

  create temporary table tmp_to_email on commit drop as
  select
    erf.invoice_id,
    erf.client_id,
    erf.invoice_no,
    erf.week_ending_date,
    case
      when erf.has_user_created_manual_qr_adjustment = true
        and coalesce(erca.contract_alt_email_count,0) = 1
        then erca.contract_alt_email
      when erf.has_user_created_manual_qr_adjustment = true
        and coalesce(erca.has_enabled_contract_override,false) = false
        and coalesce(lcs.send_manual_invoices_to_different_email,false) = true
        and nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '') is not null
        then nullif(btrim(coalesce(lcs.manual_invoices_alt_email_address,'')), '')
      else erf.primary_to_email
    end as to_email,
    erf.is_self_bill
  from tmp_email_route_flags erf
  left join tmp_email_route_contract_agg erca
    on erca.invoice_id = erf.invoice_id
  left join tmp_latest_client_settings lcs
    on lcs.client_id = erf.client_id
  where coalesce(erf.is_self_bill,false) = false
    and coalesce(erf.do_not_send,false) = false
    and not exists (
      select 1
      from tmp_email_queue_warnings w
      where w.invoice_id = erf.invoice_id
    );

  -- build queued mail_outbox rows in chunks
  create temporary table tmp_mail_rows on commit drop as
  with base as (
    select
      t.client_id,
      t.week_ending_date,
      t.to_email,
      t.invoice_id,
      t.invoice_no
    from tmp_to_email t
    where t.to_email is not null and length(btrim(t.to_email)) > 0
  ),
  numbered as (
    select
      b.*,
      row_number() over (
        partition by b.client_id, b.week_ending_date, b.to_email
        order by b.invoice_no nulls last, b.invoice_id::text
      ) as rn
    from base b
  ),
  chunked as (
    select
      n.client_id,
      n.week_ending_date,
      n.to_email,
      floor((n.rn - 1)::numeric / v_max_attach)::int as chunk_idx,
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', n.invoice_id::text,
          'filename', case
            when n.invoice_no is not null and length(btrim(n.invoice_no)) > 0
              then ('Invoice_' || btrim(n.invoice_no) || '.pdf')
            else ('Invoice_' || n.invoice_id::text || '.pdf')
          end
        )
        order by n.rn
      ) as attachments,
      array_agg(n.invoice_id order by n.rn) as invoice_ids
    from numbered n
    group by n.client_id, n.week_ending_date, n.to_email, floor((n.rn - 1)::numeric / v_max_attach)::int
  )
  select * from chunked;

  -- ✅ NEW: prevent duplicate emails when two users run batch issue concurrently.
  -- We lock per (reference,to_email) and then insert only if that exact (type,reference,to) doesn't already exist.
  for m_lock in
    select m.client_id, m.week_ending_date, m.to_email, m.chunk_idx
    from tmp_mail_rows m
  loop
    v_ref :=
      'invoice_batch:' || m_lock.client_id::text || ':' || coalesce(m_lock.week_ending_date::text,'') || ':part:' || (m_lock.chunk_idx + 1)::text;

    perform pg_advisory_xact_lock(
      hashtext(v_ref),
      hashtext(coalesce(m_lock.to_email,''))
    );
  end loop;

  -- insert into mail_outbox
  -- NOTE: attachments are invoice_id placeholders; worker will resolve PDFs at send time.
  insert into public.mail_outbox(
    type,
    "to",
    cc,
    bcc,
    reply_to,
    importance,
    email_type,
    subject,
    body_text,
    attachments,
    status,
    reference,
    created_at_utc,
    created_by,

    -- ✅ NEW: comms log metadata
    recipient_kind,
    recipient_id,
    context_kind,
    context_id,
    mailshot_run_id,
    document_template_id
  )
  select
    'INVOICE'::text,
    m.to_email,
    null::text,
    null::text,
    null::text,
    'Normal'::text,
    'plain'::text,
    'Invoices – Week ending ' || coalesce(m.week_ending_date::text, ''),
    'Please find the attached invoices.',
    m.attachments,
    'QUEUED'::public.mail_status_enum,
    'invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text,
    v_now,
    p_actor_user_id,

    -- ✅ NEW: recipient/context for unified outbox + comms tabs
    'client'::text,
    m.client_id,
    'invoices'::text,
    null::uuid,
    null::uuid,
    null::uuid
  from tmp_mail_rows m
  where not exists (
    select 1
    from public.mail_outbox o2
    where o2.type = 'INVOICE'
      and o2.reference = ('invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text)
      and o2."to" = m.to_email
  );

  -- ✅ NEW: ensure existing (already-present) queued invoice emails get comms metadata too (idempotent backfill-on-touch)
  update public.mail_outbox o3
  set
    recipient_kind = 'client'::text,
    recipient_id = m3.client_id,
    context_kind = 'invoices'::text,
    context_id = null::uuid,
    mailshot_run_id = null::uuid,
    document_template_id = null::uuid,
    email_type = coalesce(o3.email_type, 'plain'::text),
    importance = coalesce(o3.importance, 'Normal'::text)
  from tmp_mail_rows m3
  where o3.type = 'INVOICE'
    and o3."to" = m3.to_email
    and o3.reference = ('invoice_batch:' || m3.client_id::text || ':' || coalesce(m3.week_ending_date::text,'') || ':part:' || (m3.chunk_idx + 1)::text)
    and (
      o3.recipient_kind is null
      or o3.recipient_id is null
      or o3.context_kind is null
      or o3.email_type is null
      or o3.importance is null
    );

  -- collect email outbox rows as json
  -- ✅ NEW: return rows matching the intended (reference,to) for this run, whether inserted now or already present.
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'mail_outbox_id', o.id::text,
        'to', o."to",
        'subject', o.subject,
        'reference', o.reference
      )
      order by o.id::text
    ),
    '[]'::jsonb
  )
  into v_email_json
  from (
    select o.id, o.reference, o."to", o.subject
    from public.mail_outbox o
    join tmp_mail_rows m
      on o.type = 'INVOICE'
     and o."to" = m.to_email
     and o.reference = ('invoice_batch:' || m.client_id::text || ':' || coalesce(m.week_ending_date::text,'') || ':part:' || (m.chunk_idx + 1)::text)
  ) o;

  if coalesce(v_debug,false) = true then
    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','before_return',
      'allowed_count',coalesce(array_length(v_allowed,1),0),
      'not_due_count',coalesce(array_length(v_not_due,1),0),
      'max_attachments_per_email',v_max_attach,
      'pdf_invoice_count', v_pdf_invoice_count,
      'pdf_rows_affected', v_pdf_rows_affected
    ));

    perform public._inv_write_audit(
      p_actor_user_id,
      'INVOICE_ISSUE_AND_QUEUE_EMAILS_BATCH_DEBUG',
      jsonb_build_object(
        'allow_early', coalesce(p_allow_early,false),
        'anchor_ymd', v_anchor_ymd::text,
        'input_invoice_ids', to_jsonb(v_ids),
        'allowed_invoice_ids', to_jsonb(coalesce(v_allowed, array[]::uuid[])),
        'not_due_invoice_ids', to_jsonb(coalesce(v_not_due, array[]::uuid[])),
        'issue_results', v_issue_json,
        'not_due_results', v_not_due_json,
        'email_outbox', v_email_json,
        'email_queue_warnings', v_email_warn_json,
        'max_attachments_per_email', v_max_attach,
        'pdf_invoice_ids', to_jsonb(coalesce(v_pdf_invoice_ids, array[]::uuid[])),
        'pdf_invoice_count', v_pdf_invoice_count,
        'pdf_rows_affected', v_pdf_rows_affected,
        'steps', v_steps
      ),
      'invoices',
      null,
      null,
      null,
      null, null, null
    );
  end if;

  return jsonb_build_object(
    'invoice_results', (v_issue_json || v_not_due_json),
    'email_outbox', v_email_json,
    'email_queue_warnings', v_email_warn_json,
    'max_attachments_per_email', v_max_attach,
    'allow_early', coalesce(p_allow_early,false),

    -- ✅ NEW: PDF enqueue summary (safe additive fields)
    'pdf_invoice_count', v_pdf_invoice_count,
    'pdf_rows_affected', v_pdf_rows_affected
  );
exception
  when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

    if coalesce(v_debug,false) = true then
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_AND_QUEUE_EMAILS_BATCH_ERROR',
        jsonb_build_object(
          'sqlstate', v_sqlstate,
          'error', v_err,
          'allow_early', coalesce(p_allow_early,false),
          'anchor_ymd', v_anchor_ymd::text,
          'input_invoice_ids', to_jsonb(coalesce(v_ids, array[]::uuid[])),
          'allowed_invoice_ids', to_jsonb(coalesce(v_allowed, array[]::uuid[])),
          'not_due_invoice_ids', to_jsonb(coalesce(v_not_due, array[]::uuid[])),
          'max_attachments_per_email', v_max_attach,
          'steps', v_steps
        ),
        'invoices',
        null,
        null,
        null,
        null, null, null
      );
    end if;

    raise;
end;
$function$;

-- invoice_issue_batch(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_issue_batch(p_invoice_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, issued_at_utc timestamp with time zone, on_hold_reason text, reasons text[], error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.issued_at_utc,
        x.on_hold_reason,
        x.reasons,
        null::text
      into invoice_id, ok, status, issued_at_utc, on_hold_reason, reasons, error
      from public.invoice_issue_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      issued_at_utc := null;
      on_hold_reason := null;
      reasons := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_issue_one(uuid,uuid)
CREATE OR REPLACE FUNCTION public.invoice_issue_one(p_invoice_id uuid, p_actor_user_id uuid)
 RETURNS TABLE(status text, issued_at_utc timestamp with time zone, on_hold_reason text, reasons text[])
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;

  v_terms_days int := null;
  v_due_at timestamptz := null;
  v_hdr jsonb := null;
  v_client_id uuid := null;

  v_group_nightsat_sunbh boolean := null;

  v_ts_ids uuid[];
  v_worked_ts_ids uuid[];

  v_reasons text[] := array[]::text[];
  v_precheck_reasons text[] := array[]::text[];
  v_hr_reasons text[] := array[]::text[];
  v_issue_ref_reasons text[] := array[]::text[];

  v_on_hold_reason text := null;

  -- ref-to-issue flag (effective: contract override aware via v_ts_invoice_precheck)
  v_ref_required_to_issue boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_started timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_ts jsonb := '[]'::jsonb;

  -- ======================================================
  -- PDF invalidation helpers (optional columns; dynamic update)
  -- ======================================================
  v_has_updated_at boolean := false;
  v_has_pdf_fresh boolean := false;

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

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','start',
    'now_utc', public._inv_iso_utc(v_now),
    'anchor_ymd', v_anchor_ymd::text,
    'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end
  ));

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,true,'INVOICE_ISSUE'
  );

  -- Detect optional invoice columns used to invalidate invoice PDF freshness on ISSUE
  begin
    select exists(
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'invoices'
        and c.column_name = 'updated_at'
    )
    into v_has_updated_at;

    select exists(
      select 1
      from information_schema.columns c
      where c.table_schema = 'public'
        and c.table_name = 'invoices'
        and c.column_name = 'pdf_fresh'
    )
    into v_has_pdf_fresh;
  exception when others then
    v_has_updated_at := false;
    v_has_pdf_fresh := false;
  end;

  -- Load invoice + basic guards
  declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot issue a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot issue a PAID invoice';
    end if;

    if v_inv.status::text = 'ISSUED' then
      status := 'ISSUED';
      issued_at_utc := v_inv.issued_at_utc;
      on_hold_reason := v_inv.on_hold_reason;
      reasons := array[]::text[];

      if v_invoice_debug then
        begin
          perform public._inv_write_audit(
            p_actor_user_id,
            'INVOICE_ISSUE_DEBUG',
            jsonb_build_object(
              'result','ALREADY_ISSUED',
              'invoice_id', p_invoice_id::text,
              'status', v_inv.status::text,
              'issued_at_utc', to_jsonb(v_inv.issued_at_utc),
              'steps', v_dbg_steps
            ),
            'invoices',
            p_invoice_id::text,
            null,
            'INVOICE_DEBUG',
            null, null, null
          );
        exception when others then
          null;
        end;
      end if;

      return next;
      return;
    end if;

    -- ✅ HARD BLOCK:
    -- If invoice is currently ON_HOLD, do not re-evaluate blockers; require UNHOLD first.
    if v_inv.status::text = 'ON_HOLD' then
      status := 'ON_HOLD';
      issued_at_utc := null;
      on_hold_reason := 'Unhold first';
      reasons := array['Unhold first']::text[];
      return next;
      return;
    end if;

    if v_inv.status::text not in ('DRAFT','ON_HOLD') then
      raise exception 'Only DRAFT/ON_HOLD invoices can be issued (current status=%)', v_inv.status::text;
    end if;
  end;

  -- Invoice client_id (used for settings lookup and header snapshot)
  select i.client_id
  into v_client_id
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Timesheets on invoice
  select array_agg(distinct l.timesheet_id)
  into v_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    raise exception 'Invoice has no timesheets to validate';
  end if;

  -- Timesheets that have WORKED content on this invoice (hours/additional only)
  select array_agg(distinct l.timesheet_id)
  into v_worked_ts_ids
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null
    and upper(coalesce(l.meta_json->>'line_type','')) in (
      'HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY'
    );

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','collected_timesheets',
    'timesheet_count', coalesce(array_length(v_ts_ids,1),0),
    'worked_timesheet_count', coalesce(array_length(v_worked_ts_ids,1),0)
  ));

  -- ✅ Determine whether ref-to-issue is enabled for THIS invoice (contract override aware)
  -- We treat this as: any worked timesheet on the invoice has reference_number_required_to_issue_invoice = true
  begin
    select coalesce(bool_or(coalesce(pc.reference_number_required_to_issue_invoice,false)), false)
    into v_ref_required_to_issue
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id;
  exception when others then
    v_ref_required_to_issue := false;
  end;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_issue_ref_policy',
    'reference_number_required_to_issue_invoice', v_ref_required_to_issue
  ));

  -- ------------------------------------------------------------
  -- 1) Precheck blockers (authoritative: PDF/reference/evidence)
  -- ------------------------------------------------------------
  select array_agg(
    case
      when pc.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': precheck missing'

      when upper(coalesce(pc.precheck_status,'')) = 'OK'
        then null

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_REFERENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing reference/PO'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_PDF'
        then 'TS ' || pc.timesheet_id::text || ': missing timesheet PDF'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_MILEAGE_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing mileage evidence'

      when upper(coalesce(pc.precheck_status,'')) = 'BLOCK_NO_EXPENSES_EVIDENCE'
        then 'TS ' || pc.timesheet_id::text || ': missing expenses evidence'

      else
        'TS ' || pc.timesheet_id::text || ': precheck blocker ' || upper(coalesce(pc.precheck_status,''))
    end
  )
  into v_precheck_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_ts_invoice_precheck pc
    on pc.timesheet_id = x.timesheet_id;

  v_precheck_reasons := array_remove(v_precheck_reasons, null);

  -- ------------------------------------------------------------
  -- 2) OPTIONAL HARDENING: HR validation blockers
  -- ------------------------------------------------------------
  select array_agg(
    case
      when s.timesheet_id is null
        then 'TS ' || x.timesheet_id::text || ': summary missing'

      when coalesce(s.hr_validation_required_for_invoice, false) = true
        and (
          s.validation_status is null
          or s.validation_status <> all (array[
            'VALIDATION_OK'::public.validation_status_enum,
            'OVERRIDDEN'::public.validation_status_enum
          ])
        )
        then 'TS ' || x.timesheet_id::text || ': HR validation not passed'

      else null
    end
  )
  into v_hr_reasons
  from unnest(v_ts_ids) as x(timesheet_id)
  left join public.v_timesheets_summary_base s
    on s.timesheet_id = x.timesheet_id;

  v_hr_reasons := array_remove(v_hr_reasons, null);

  -- ------------------------------------------------------------
  -- 3) ISSUE-TIME reference gating (contract override aware, per-timesheet)
  -- ------------------------------------------------------------

  -- 3) ISSUE-TIME reference gating (scoped to this invoice)
  -- Only segments locked to THIS invoice can block issuing (so other-invoice segments never block).
  select array_agg(
    case
      when t.timesheet_id is null then null
      when t.issue_missing_count > 0
        then 'TS '||t.timesheet_id::text||': missing reference/PO for '||t.issue_missing_count::text||' shift(s) (required to issue)'
      else null
    end
  )
  into v_issue_ref_reasons
  from (
    select
      x.timesheet_id,
      case
        when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

        when tf.invoice_breakdown_json is not null
          and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
          and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
        then (
          select count(*)::int
          from jsonb_array_elements(tf.invoice_breakdown_json->'segments') as s(seg)
          where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
            and (
              (
                (case when coalesce(s.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_day')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_night')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sat')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_sun')::numeric else 0 end)
              + (case when coalesce(s.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'hours_bh')::numeric else 0 end)
              ) > 0
              or (case when coalesce(s.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (s.seg->>'charge_amount')::numeric else 0 end) > 0
            )
            and nullif(btrim(coalesce(s.seg->>'ref_num','')), '') is null
        )

        else (
          case
            when public._inv_timesheet_has_invoice_reference(
              ts.sheet_scope::text,
              coalesce(ts.submission_mode::text,''),
              ts.reference_number,
              ts.day_references_json,
              ts.actual_schedule_json
            )
            then 0 else 1 end
        )
      end as issue_missing_count
    from unnest(coalesce(v_worked_ts_ids, array[]::uuid[])) as x(timesheet_id)
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = x.timesheet_id
    left join public.timesheets ts
      on ts.timesheet_id = x.timesheet_id
     and ts.is_current = true
    left join public.timesheets_financials tf
      on tf.timesheet_id = x.timesheet_id
     and tf.is_current = true
  ) t;

  v_issue_ref_reasons := array_remove(v_issue_ref_reasons, null);

  -- Merge blockers
  v_reasons := array_cat(v_precheck_reasons, v_hr_reasons);
  v_reasons := array_cat(v_reasons, v_issue_ref_reasons);
  v_reasons := array_remove(v_reasons, null);

  -- Debug per-timesheet snapshot (only if enabled)
  if v_invoice_debug then
    begin
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'timesheet_id', x.timesheet_id::text,
          'has_worked_lines', (v_worked_ts_ids is not null and x.timesheet_id = any(v_worked_ts_ids)),
          'precheck_status', coalesce(pc.precheck_status,'')::text,
          'ref_required_to_issue_effective', coalesce(pc.reference_number_required_to_issue_invoice,false),
          'issue_missing_reference_on_invoice', ((
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_invoice_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ) > 0),
          'issue_missing_reference_on_invoice_count', (
            case
              when x.has_worked_lines is not true then 0
              when coalesce(pc.reference_number_required_to_issue_invoice,false) is not true then 0

              when s.invoice_breakdown_json is not null
                and jsonb_typeof(s.invoice_breakdown_json) = 'object'
                and upper(coalesce(s.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                and jsonb_typeof(s.invoice_breakdown_json->'segments') = 'array'
              then coalesce((
                select count(*)::int
                from jsonb_array_elements(s.invoice_breakdown_json->'segments') as sg(seg)
                where nullif(btrim(coalesce(sg.seg->>'invoice_locked_invoice_id','')), '') = p_invoice_id::text
                  and (
                    (
                      (case when coalesce(sg.seg->>'hours_day','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_day')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_night','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_night')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sat','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sat')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_sun','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_sun')::numeric else 0 end)
                    + (case when coalesce(sg.seg->>'hours_bh','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'hours_bh')::numeric else 0 end)
                    ) > 0
                    or (case when coalesce(sg.seg->>'charge_amount','') ~ '^[-]?[0-9]+(\.[0-9]+)?$' then (sg.seg->>'charge_amount')::numeric else 0 end) > 0
                  )
                  and nullif(btrim(coalesce(sg.seg->>'ref_num','')), '') is null
              ),0)

              else (
                case
                  when public._inv_timesheet_has_invoice_reference(
                    t0.sheet_scope::text,
                    coalesce(t0.submission_mode::text,''),
                    t0.reference_number,
                    t0.day_references_json,
                    t0.actual_schedule_json
                  ) then 0 else 1 end
              )
            end
          ),
          'hr_required', coalesce(s.hr_validation_required_for_invoice,false),
          'validation_status', case when s.validation_status is null then null else s.validation_status::text end
        )
      ), '[]'::jsonb)
      into v_dbg_ts
      from unnest(v_ts_ids) as x(timesheet_id)
      left join public.v_ts_invoice_precheck pc on pc.timesheet_id = x.timesheet_id
      left join public.v_timesheets_summary_base s on s.timesheet_id = x.timesheet_id;
    exception when others then
      v_dbg_ts := '[]'::jsonb;
    end;
  end if;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step','computed_blockers',
    'precheck_reasons_count', coalesce(array_length(v_precheck_reasons,1),0),
    'hr_reasons_count', coalesce(array_length(v_hr_reasons,1),0),
    'issue_ref_reasons_count', coalesce(array_length(v_issue_ref_reasons,1),0),
    'total_reasons_count', coalesce(array_length(v_reasons,1),0)
  ));

  -- Any blockers => ON_HOLD
  if coalesce(array_length(v_reasons, 1), 0) > 0 then
    v_on_hold_reason := array_to_string(v_reasons, '; ');

    update public.invoices
    set status = 'ON_HOLD'::public.invoice_status_enum,
        status_date_utc = v_now,
        issued_at_utc = null,
        on_hold_reason = v_on_hold_reason
    where id = p_invoice_id;

    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_ON_HOLD',
      null,
      jsonb_build_object('reasons', v_reasons),
      null,
      p_actor_user_id
    );

    status := 'ON_HOLD';
    issued_at_utc := null;
    on_hold_reason := v_on_hold_reason;
    reasons := v_reasons;

    if v_invoice_debug then
      begin
        perform public._inv_write_audit(
          p_actor_user_id,
          'INVOICE_ISSUE_DEBUG',
          jsonb_build_object(
            'result','ON_HOLD',
            'invoice_id', p_invoice_id::text,
            'client_id', case when v_client_id is null then null else v_client_id::text end,
            'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
            'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
            'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
            'reasons', to_jsonb(coalesce(v_reasons, array[]::text[])),
            'timesheets_debug', v_dbg_ts,
            'steps', v_dbg_steps,
            'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
            'run_finished_at_utc', public._inv_iso_utc(now())
          ),
          'invoices',
          p_invoice_id::text,
          null,
          'INVOICE_DEBUG',
          null, null, null
        );
      exception when others then
        null;
      end;
    end if;

    return next;
    return;
  end if;

  -- ------------------------------------------------------------
  -- No blockers => issue
  -- ------------------------------------------------------------
  select i.client_id, i.header_snapshot_json
  into v_client_id, v_hdr
  from public.invoices i
  where i.id = p_invoice_id;

  if v_hdr is null or jsonb_typeof(v_hdr) <> 'object' then
    v_hdr := '{}'::jsonb;
  end if;

  if not (v_hdr ? 'group_nightsat_sunbh') then
    select cs0.group_nightsat_sunbh
    into v_group_nightsat_sunbh
    from public.client_settings cs0
    where cs0.client_id = v_client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;

    v_hdr := v_hdr || jsonb_build_object('group_nightsat_sunbh', coalesce(v_group_nightsat_sunbh, false));
  end if;

  begin
    if (v_hdr ? 'payment_terms_days') then
      v_terms_days := (v_hdr->>'payment_terms_days')::int;
    end if;
  exception when others then
    v_terms_days := null;
  end;

  if v_terms_days is null then
    begin
      select c.payment_terms_days
      into v_terms_days
      from public.clients c
      where c.id = v_client_id;
    exception when others then
      v_terms_days := null;
    end;
  end if;

  v_terms_days := coalesce(v_terms_days, 30);
  v_due_at := v_now + make_interval(days => v_terms_days);

  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = v_now,
      due_at_utc = v_due_at,
      on_hold_reason = null,
      header_snapshot_json = v_hdr
  where id = p_invoice_id;

  -- ✅ FIX: invalidate invoice PDF freshness on ISSUE so invpdf regeneration will occur
  -- (uses optional columns if present; never fails function creation)
  begin
    if v_has_updated_at then
      execute 'update public.invoices set updated_at = $1 where id = $2'
      using v_now, p_invoice_id;
    end if;

    if v_has_pdf_fresh then
      execute 'update public.invoices set pdf_fresh = false where id = $1'
      using p_invoice_id;
    end if;
  exception when others then
    null;
  end;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_ISSUED',
    null,
    '{}'::jsonb,
    null,
    p_actor_user_id
  );

  status := 'ISSUED';
  issued_at_utc := v_now;
  on_hold_reason := null;
  reasons := array[]::text[];

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ISSUED',
          'invoice_id', p_invoice_id::text,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return next;

exception when others then
  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_ISSUE_DEBUG',
        jsonb_build_object(
          'result','ERROR',
          'invoice_id', case when p_invoice_id is null then null else p_invoice_id::text end,
          'client_id', case when v_client_id is null then null else v_client_id::text end,
          'reference_number_required_to_issue_invoice', v_ref_required_to_issue,
          'sqlstate', sqlstate,
          'error', sqlerrm,
          'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
          'worked_timesheet_ids', to_jsonb(coalesce(v_worked_ts_ids, array[]::uuid[])),
          'timesheets_debug', v_dbg_ts,
          'steps', v_dbg_steps,
          'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
          'run_finished_at_utc', public._inv_iso_utc(now())
        ),
        'invoices',
        case when p_invoice_id is null then null else p_invoice_id::text end,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;
  raise;
end;
$function$;

-- invoice_line_archived_timesheet_guard_v1()
CREATE OR REPLACE FUNCTION public.invoice_line_archived_timesheet_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_current_timesheet_id uuid;
  v_archived_at_utc timestamptz;
BEGIN
  -- Exact identity is authoritative whenever the invoice-line mutation supplies
  -- a timesheet_id.  Do not replace it with the current row for the booking:
  -- a historical exact identity can itself be Archived.
  IF NEW.timesheet_id IS NOT NULL THEN
    SELECT
      t.timesheet_id,
      t.archived_at_utc
    INTO
      v_current_timesheet_id,
      v_archived_at_utc
    FROM public.timesheets AS t
    WHERE t.timesheet_id = NEW.timesheet_id
    FOR KEY SHARE OF t;
  ELSIF NULLIF(BTRIM(COALESCE(NEW.booking_id, '')), '') IS NOT NULL THEN
    -- Booking lookup is fallback-only when the caller supplied no exact ID.
    SELECT
      t.timesheet_id,
      t.archived_at_utc
    INTO
      v_current_timesheet_id,
      v_archived_at_utc
    FROM public.timesheets AS t
    WHERE t.is_current = true
      AND t.booking_id = NEW.booking_id
    ORDER BY t.timesheet_id
    LIMIT 1
    FOR KEY SHARE OF t;
  END IF;

  IF v_current_timesheet_id IS NOT NULL
     AND v_archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_ARCHIVED',
      DETAIL = jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'reason', 'archived_timesheet_cannot_be_added_to_invoice'
      )::text;
  END IF;

  RETURN NEW;
END;
$function$;

-- invoice_list_ids(jsonb)
CREATE OR REPLACE FUNCTION public.invoice_list_ids(p_filters jsonb)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_client_id uuid := null;
  v_q text := null;
  v_issued_from timestamptz := null;
  v_issued_to timestamptz := null;
  v_due_from timestamptz := null;
  v_due_to timestamptz := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_week_ending_from date := null;
  v_week_ending_to date := null;
  v_status_raw jsonb := null;
  v_status_list text[] := null;
  v_legacy_paid_filter text := null;
  v_ids_arr uuid[] := null;
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if (p_filters ? 'client_id') and nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'issued_from','')), '') is not null then v_issued_from := (p_filters->>'issued_from')::timestamptz; end if; exception when others then v_issued_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'issued_to','')), '') is not null then v_issued_to := (p_filters->>'issued_to')::timestamptz; end if; exception when others then v_issued_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_from','')), '') is not null then v_due_from := (p_filters->>'due_from')::timestamptz; end if; exception when others then v_due_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_to','')), '') is not null then v_due_to := (p_filters->>'due_to')::timestamptz; end if; exception when others then v_due_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'week_ending_from','')), '') is not null then v_week_ending_from := (p_filters->>'week_ending_from')::date; end if; exception when others then v_week_ending_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'week_ending_to','')), '') is not null then v_week_ending_to := (p_filters->>'week_ending_to')::date; end if; exception when others then v_week_ending_to := null; end;

  if p_filters ? 'status' then
    v_status_raw := p_filters->'status';
  end if;

  if v_status_raw is not null then
    if jsonb_typeof(v_status_raw) = 'array' then
      select array_agg(upper(btrim(x))) into v_status_list
      from (
        select jsonb_array_elements_text(v_status_raw) as x
      ) s
      where nullif(btrim(coalesce(x,'')), '') is not null;
    else
      v_status_list := array_remove(string_to_array(upper(btrim(trim(both '"' from coalesce(v_status_raw::text,'')))), ','), '');
    end if;
  end if;

  if v_status_list is not null and array_length(v_status_list,1) = 1 then
    if lower(v_status_list[1]) = 'paid' or v_status_list[1] = 'PAID' then
      v_legacy_paid_filter := 'paid';
      v_status_list := null;
    elsif lower(v_status_list[1]) = 'unpaid' or v_status_list[1] = 'UNPAID' then
      v_legacy_paid_filter := 'unpaid';
      v_status_list := null;
    end if;
  end if;

  if v_status_list is not null then
    select array_agg(s)
    into v_status_list
    from (
      select distinct replace(replace(replace(replace(upper(btrim(x)), '(', ''), ')', ''), ',', ''), '"', '') as s
      from unnest(v_status_list) x
    ) t
    where s in ('DRAFT','ISSUED','ON_HOLD','PAID');

    if v_status_list is null or coalesce(array_length(v_status_list,1),0) = 0 then
      v_status_list := null;
    end if;
  end if;

  if p_filters ? 'ids' then
    if jsonb_typeof(p_filters->'ids') = 'array' then
      select array_agg((x)::uuid)
      into v_ids_arr
      from jsonb_array_elements_text(p_filters->'ids') x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    elsif nullif(btrim(coalesce(p_filters->>'ids','')), '') is not null then
      select array_agg(val::uuid)
      into v_ids_arr
      from (
        select distinct nullif(btrim(x), '') as val
        from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
      ) s
      where s.val is not null;
    end if;
  end if;

  return query
  with quick_ids as (
    select qq.invoice_id as id
    from public.invoice_quicksearch_ids(v_q, 20000) qq
    where v_q is not null
  ),
  filtered as (
    select i.id
    from public.invoices i
    where (v_client_id is null or i.client_id = v_client_id)
      and (v_ids_arr is null or i.id = any(v_ids_arr))
      and (
        v_q is null
        or exists (
          select 1
          from quick_ids qi
          where qi.id = i.id
        )
      )
      and (v_issued_from is null or i.issued_at_utc >= v_issued_from)
      and (v_issued_to is null or i.issued_at_utc <= v_issued_to)
      and (v_due_from is null or i.due_at_utc >= v_due_from)
      and (v_due_to is null or i.due_at_utc <= v_due_to)
      and (v_created_from is null or i.created_at >= v_created_from)
      and (v_created_to is null or i.created_at <= v_created_to)
      and (
        v_legacy_paid_filter is null
        or (v_legacy_paid_filter = 'paid' and i.paid_at_utc is not null)
        or (v_legacy_paid_filter = 'unpaid' and i.paid_at_utc is null)
      )
      and (
        v_status_list is null
        or i.status::text = any(v_status_list)
      )
      and (
        (v_week_ending_from is null and v_week_ending_to is null)
        or exists (
          select 1
          from public.invoice_lines il
          join public.timesheets t on t.timesheet_id = il.timesheet_id
          where il.invoice_id = i.id
            and (v_week_ending_from is null or t.week_ending_date >= v_week_ending_from)
            and (v_week_ending_to is null or t.week_ending_date <= v_week_ending_to)
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$function$;

-- invoice_list_totals(jsonb)
CREATE OR REPLACE FUNCTION public.invoice_list_totals(p_filters jsonb)
 RETURNS TABLE(count_all bigint, subtotal_ex_vat_sum numeric, total_inc_vat_sum numeric, margin_ex_vat_sum numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_client_id uuid := null;
  v_q text := null;

  v_issued_from timestamptz := null;
  v_issued_to   timestamptz := null;
  v_due_from    timestamptz := null;
  v_due_to      timestamptz := null;
  v_created_from timestamptz := null;
  v_created_to   timestamptz := null;

  -- NEW: week-ending filters (date)
  v_week_ending_from date := null;
  v_week_ending_to   date := null;

  v_status_raw jsonb := null;
  v_status_list text[] := null;
  v_legacy_paid_filter text := null; -- 'paid' | 'unpaid' | null
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  -- client_id
  begin
    if (p_filters ? 'client_id') and nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then
      v_client_id := (p_filters->>'client_id')::uuid;
    end if;
  exception when others then
    v_client_id := null;
  end;

  -- q (partial invoice_no)
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  -- dates (accept ISO date or timestamptz strings)
  begin if nullif(btrim(coalesce(p_filters->>'issued_from','')), '') is not null then v_issued_from := (p_filters->>'issued_from')::timestamptz; end if; exception when others then v_issued_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'issued_to','')), '') is not null then v_issued_to := (p_filters->>'issued_to')::timestamptz; end if; exception when others then v_issued_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_from','')), '') is not null then v_due_from := (p_filters->>'due_from')::timestamptz; end if; exception when others then v_due_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_to','')), '') is not null then v_due_to := (p_filters->>'due_to')::timestamptz; end if; exception when others then v_due_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;

  -- NEW: week ending (accept ISO date strings; ignore invalid)
  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_from','')), '') is not null then
      v_week_ending_from := (p_filters->>'week_ending_from')::date;
    end if;
  exception when others then
    v_week_ending_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'week_ending_to','')), '') is not null then
      v_week_ending_to := (p_filters->>'week_ending_to')::date;
    end if;
  exception when others then
    v_week_ending_to := null;
  end;

  -- status logic:
  -- - p_filters.status may be array OR comma string OR single enum string OR 'paid'/'unpaid'
  if p_filters ? 'status' then
    v_status_raw := p_filters->'status';
  end if;

  if v_status_raw is not null then
    if jsonb_typeof(v_status_raw) = 'array' then
      select array_agg(upper(btrim(x))) into v_status_list
      from (
        select jsonb_array_elements_text(v_status_raw) as x
      ) s
      where nullif(btrim(coalesce(x,'')), '') is not null;
    else
      -- string / scalar (trim JSON quotes safely)
      v_status_list := array_remove(
        string_to_array(
          upper(btrim(trim(both '"' from coalesce(v_status_raw::text,'')))),
          ','
        ),
        ''
      );
    end if;
  end if;

  -- legacy paid/unpaid filter support
  if v_status_list is not null and array_length(v_status_list,1) = 1 then
    if lower(v_status_list[1]) = 'paid' or v_status_list[1] = 'PAID' then
      v_legacy_paid_filter := 'paid';
      v_status_list := null;
    elsif lower(v_status_list[1]) = 'unpaid' or v_status_list[1] = 'UNPAID' then
      v_legacy_paid_filter := 'unpaid';
      v_status_list := null;
    end if;
  end if;

  -- Sanitize enum status list (DRAFT/ISSUED/ON_HOLD/PAID)
  if v_status_list is not null then
    select array_agg(s)
    into v_status_list
    from (
      select distinct
        replace(
          replace(
            replace(
              replace(upper(btrim(x)),'(',''),')',''
            ),',',''
          ),'"',''
        ) as s
      from unnest(v_status_list) x
    ) t
    where s in ('DRAFT','ISSUED','ON_HOLD','PAID');

    if v_status_list is null or coalesce(array_length(v_status_list,1),0) = 0 then
      v_status_list := null;
    end if;
  end if;

  return query
  with inv as (
    select
      i.id,
      i.subtotal_ex_vat,
      i.total_inc_vat
    from public.invoices i
    where (v_client_id is null or i.client_id = v_client_id)
      and (v_q is null or i.invoice_no ilike ('%'||v_q||'%'))
      and (v_issued_from is null or i.issued_at_utc >= v_issued_from)
      and (v_issued_to   is null or i.issued_at_utc <= v_issued_to)
      and (v_due_from    is null or i.due_at_utc >= v_due_from)
      and (v_due_to      is null or i.due_at_utc <= v_due_to)
      and (v_created_from is null or i.created_at >= v_created_from)
      and (v_created_to   is null or i.created_at <= v_created_to)
      and (
        v_legacy_paid_filter is null
        or (v_legacy_paid_filter = 'paid' and i.paid_at_utc is not null)
        or (v_legacy_paid_filter = 'unpaid' and i.paid_at_utc is null)
      )
      and (
        v_status_list is null
        or i.status::text = any(v_status_list)
      )
      -- NEW: Week ending range filter (matches list endpoint semantics: via invoice_lines → timesheets)
      and (
        (v_week_ending_from is null and v_week_ending_to is null)
        or exists (
          select 1
          from public.invoice_lines l
          join public.timesheets t
            on t.timesheet_id = l.timesheet_id
          where l.invoice_id = i.id
            and (v_week_ending_from is null or t.week_ending_date >= v_week_ending_from)
            and (v_week_ending_to   is null or t.week_ending_date <= v_week_ending_to)
        )
      )
  ),
  m as (
    select
      l.invoice_id,
      sum(coalesce(l.margin_ex_vat,0))::numeric as margin_sum
    from public.invoice_lines l
    join inv on inv.id = l.invoice_id
    group by l.invoice_id
  )
  select
    count(*)::bigint as count_all,
    coalesce(sum(inv.subtotal_ex_vat),0)::numeric as subtotal_ex_vat_sum,
    coalesce(sum(inv.total_inc_vat),0)::numeric as total_inc_vat_sum,
    coalesce(sum(coalesce(m.margin_sum,0)),0)::numeric as margin_ex_vat_sum
  from inv
  left join m on m.invoice_id = inv.id;

end;
$function$;

-- invoice_mark_paid_batch(uuid[],uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_mark_paid_batch(p_invoice_ids uuid[], p_actor_user_id uuid, p_paid_at timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, paid_at_utc timestamp with time zone, error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select x.invoice_id, true, x.status, x.paid_at_utc, null::text
      into invoice_id, ok, status, paid_at_utc, error
      from public.invoice_mark_paid_one(v_id, p_actor_user_id, p_paid_at) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      paid_at_utc := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_mark_paid_one(uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_mark_paid_one(p_invoice_id uuid, p_actor_user_id uuid, p_paid_at timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS TABLE(invoice_id uuid, status text, paid_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_paid_at timestamptz := coalesce(p_paid_at, v_now);
  v_inv record;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot mark a CREDIT_NOTE as PAID';
  end if;

  if v_inv.status::text = 'PAID' then
    -- Idempotent: ensure paid_at_utc is set
    update public.invoices
    set paid_at_utc = coalesce(paid_at_utc, v_paid_at),
        status_date_utc = coalesce(status_date_utc, v_now)
    where id = p_invoice_id;

    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_PAID',
      jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
      jsonb_build_object('status', 'PAID', 'paid_at_utc', coalesce(v_inv.paid_at_utc, v_paid_at)),
      null,
      p_actor_user_id
    );

    invoice_id := p_invoice_id;
    status := 'PAID';
    paid_at_utc := coalesce(v_inv.paid_at_utc, v_paid_at);
    return next;
    return;
  end if;

  if v_inv.status::text <> 'ISSUED' then
    raise exception 'Only ISSUED invoices can be marked as PAID (current status=%)', v_inv.status::text;
  end if;

  update public.invoices
  set status = 'PAID'::public.invoice_status_enum,
      status_date_utc = v_now,
      paid_at_utc = v_paid_at,
      on_hold_reason = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_PAID',
    jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
    jsonb_build_object('status', 'PAID', 'paid_at_utc', v_paid_at),
    null,
    p_actor_user_id
  );

  invoice_id := p_invoice_id;
  status := 'PAID';
  paid_at_utc := v_paid_at;
  return next;
end;
$function$;

-- invoice_mark_unpaid_batch(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_mark_unpaid_batch(p_invoice_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, paid_at_utc timestamp with time zone, error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select x.invoice_id, true, x.status, x.paid_at_utc, null::text
      into invoice_id, ok, status, paid_at_utc, error
      from public.invoice_mark_unpaid_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      paid_at_utc := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_mark_unpaid_one(uuid,uuid)
CREATE OR REPLACE FUNCTION public.invoice_mark_unpaid_one(p_invoice_id uuid, p_actor_user_id uuid)
 RETURNS TABLE(invoice_id uuid, status text, paid_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_inv record;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot mark a CREDIT_NOTE as UNPAID';
  end if;

  if v_inv.status::text <> 'PAID' then
    raise exception 'Only PAID invoices can be marked as UNPAID (current status=%)', v_inv.status::text;
  end if;

  update public.invoices
  set status = 'ISSUED'::public.invoice_status_enum,
      status_date_utc = v_now,
      paid_at_utc = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNPAID',
    jsonb_build_object('status', v_inv.status::text, 'paid_at_utc', v_inv.paid_at_utc),
    jsonb_build_object('status', 'ISSUED', 'paid_at_utc', null),
    null,
    p_actor_user_id
  );

  invoice_id := p_invoice_id;
  status := 'ISSUED';
  paid_at_utc := null;
  return next;
end;
$function$;

-- invoice_no_next()
CREATE OR REPLACE FUNCTION public.invoice_no_next()
 RETURNS text
 LANGUAGE sql
AS $function$
  select nextval('public.invoice_no_seq')::text;
$function$;

-- invoice_operation_advance_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_operation_advance_batch(p_claims jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_valid jsonb := '[]'::jsonb;
  v_rejected jsonb := '[]'::jsonb;
  v_result jsonb := '[]'::jsonb;
  v_part jsonb := '[]'::jsonb;
  v_ignored integer;
begin
  if jsonb_typeof(p_claims)<>'array'
     or jsonb_array_length(p_claims)<1
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing between 1 and 100 claims';
  end if;

  /*
   * Ownership is classified per supplied item.  Bad input, a missing row, an
   * expired lease, or a stale fence is returned as a typed rejection and does
   * not roll back unrelated current claims.
   */
  with supplied as materialized (
    select
      x.ordinality::integer request_no,
      x.value raw_claim,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version
    from jsonb_array_elements(p_claims) with ordinality x(value,ordinality)
  ),
  inspected as materialized (
    select s.*,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.payload_json,
      c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,
      c.operation_control_version current_control_version,
      c.is_manifest_member,
      c.manifest_committed carrier_manifest_committed,
      c.lease_expires_at_utc,o.status operation_status,
      o.entity_type operation_entity_type,
      o.control_version operation_current_control_version,
      o.manifest_committed root_manifest_committed,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_CLAIM'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.entity_type='INVOICE_BATCH'
          and not (
            (
              not c.is_manifest_member
              and coalesce(c.payload_json->>'is_selection_expander','false')
                in ('true','t','1','yes','on')
              and c.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
              and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
            )
            or (
              c.is_manifest_member
              and c.manifest_committed
              and o.manifest_committed
              and c.phase not in (
                'AWAITING_MANIFEST_COMMIT',
                'AWAITING_RELEASE'
              )
              and coalesce(c.payload_json->>'is_selection_expander','false')
                not in ('true','t','1','yes','on')
            )
          ) then 'MANIFEST_CLAIM_NOT_RELEASED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED') then 'OPERATION_TERMINAL'
      end rejection_code
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
  ),
  locked as materialized (
    select i.*
    from inspected i
    join public.invoice_operation_chunks c on c.id=i.chunk_id
    where i.rejection_code is null
    order by c.id
    for update of c
  )
  select
    coalesce((select jsonb_agg(jsonb_build_object(
      'chunk_id',l.chunk_id,'operation_id',l.operation_id,'chunk_type',l.chunk_type,
      'phase',l.phase,'entity_type',l.entity_type,'entity_id',l.entity_id,
      'document_version_id',l.document_version_id,'document_asset_id',l.document_asset_id,
      'payload_json',l.payload_json,'lease_token',l.lease_token,
      'fence_token',l.fence_token,
      'operation_control_version',l.operation_control_version
    ) order by l.request_no) from locked l),'[]'::jsonb),
    coalesce((select jsonb_agg(jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','phase','OWNERSHIP_REJECTED',
      'accepted',false,'code',i.rejection_code,'request_no',i.request_no
    ) order by i.request_no) from inspected i where i.rejection_code is not null),'[]'::jsonb)
  into v_valid,v_rejected;

  if jsonb_array_length(v_valid)>0 then
    v_part:=private._invoice_dispatch_advance_batch(v_valid,v_now);
    v_result:=v_result||coalesce(v_part,'[]'::jsonb);
  end if;

  /*
   * A private dispatcher must move every accepted claim out of RUNNING.  If
   * it did not, preserve the diagnostic and schedule a bounded retry instead
   * of clearing the lease and pretending the phase succeeded.
   */
  with valid_ids as (
    select (x->>'chunk_id')::uuid id
    from jsonb_array_elements(v_valid) x
  ),
  unhandled as (
    update public.invoice_operation_chunks c
    set status=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else 'RETRY_WAIT' end,
        phase=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else c.phase end,
        run_after_utc=case when c.attempt_count>=c.max_attempts then c.run_after_utc
          else v_now+make_interval(secs=>least(900,15*(2^least(c.attempt_count,6)))::integer)
             +(random()*10||' seconds')::interval end,
        error_json=jsonb_build_object(
          'code','UNHANDLED_PHASE_RESULT',
          'chunk_type',c.chunk_type,
          'phase',c.phase,
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),
            'at_utc',v_now))),
        failed_at_utc=case when c.attempt_count>=c.max_attempts then v_now else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    where c.id in(select id from valid_ids) and c.status='RUNNING'
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  )
  select v_result||coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,'error',error_json
  )),'[]'::jsonb) into v_result from unhandled;

  /* Non-running rows are never allowed to retain a current lease. */
  with valid_ids as (
    select (x->>'chunk_id')::uuid id from jsonb_array_elements(v_valid) x
  ),
  released as (
    update public.invoice_operation_chunks c
    set lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now
    where c.id in(select id from valid_ids) and c.status<>'RUNNING'
    returning c.operation_id
  ),
  affected as (
    select distinct operation_id from released
  )
  select count(*) into v_ignored
  from private._invoice_operation_rollup_batch(
    coalesce((select array_agg(operation_id) from affected),array[]::uuid[]),
    v_now,true);

  return coalesce(v_result,'[]'::jsonb)||v_rejected;
end;
$function$;

-- invoice_operation_control_batch(jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_operation_control_batch(p_actions jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=statement_timestamp();
  v_jwt_role text:=coalesce(
    nullif(current_setting('request.jwt.claim.role',true),''),
    auth.jwt()->>'role','');
  v_auth_user uuid:=auth.uid();
  v_actions jsonb;
  v_request_token text;
  v_supplied_request_hash text;
  v_request_hash text;
  v_receipt_identity text;
  v_existing_result jsonb;
  v_existing_request_hash text;
  v_existing_expires_at_utc timestamptz;
  v_result jsonb;
begin
  if v_jwt_role='service_role' then
    v_now:=coalesce(p_now_utc,statement_timestamp());
  end if;

  if jsonb_typeof(coalesce(p_actions,'null'::jsonb))<>'object'
     or exists(
       select 1
       from jsonb_object_keys(p_actions) key_name
       where key_name not in(
         'contract_version','request_token','request_hash','actions'
       )
     )
     or coalesce(p_actions->>'contract_version','')
       <>'INVOICE_OPERATION_CONTROL_V2'
     or jsonb_typeof(p_actions->'request_token') is distinct from 'string'
     or jsonb_typeof(p_actions->'request_hash') is distinct from 'string'
     or jsonb_typeof(p_actions->'actions') is distinct from 'array' then
    raise exception using errcode='22023',
      message='OPERATION_CONTROL_ACTION_SCHEMA_INVALID';
  end if;

  v_request_token:=btrim(coalesce(p_actions->>'request_token',''));
  v_supplied_request_hash:=lower(coalesce(p_actions->>'request_hash',''));
  v_actions:=p_actions->'actions';

  if v_request_token=''
     or octet_length(v_request_token)>256
     or v_request_token~'[[:cntrl:]]' then
    raise exception using errcode='22023',
      message=case when v_request_token=''
        then 'OPERATION_CONTROL_REQUEST_TOKEN_REQUIRED'
        else 'OPERATION_CONTROL_REQUEST_TOKEN_INVALID' end;
  end if;

  if v_supplied_request_hash!~'^[0-9a-f]{64}$' then
    raise exception using errcode='22023',
      message='OPERATION_CONTROL_REQUEST_HASH_MISMATCH';
  end if;

  if jsonb_array_length(v_actions)<1
     or jsonb_array_length(v_actions)>100
     or exists(
       select 1
       from jsonb_array_elements(v_actions) supplied(raw_action)
       where jsonb_typeof(supplied.raw_action) is distinct from 'object'
          or not(supplied.raw_action?&array['action','operation_id'])
          or jsonb_typeof(supplied.raw_action->'action') is distinct from
             'string'
          or jsonb_typeof(supplied.raw_action->'operation_id') is distinct
             from 'string'
          or upper(coalesce(supplied.raw_action->>'action','')) not in(
             'RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY'
          )
          or coalesce(supplied.raw_action->>'operation_id','')!~*
             '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or (
            upper(supplied.raw_action->>'action')='CANCEL'
            and exists(
              select 1 from jsonb_object_keys(supplied.raw_action) key_name
              where key_name not in('action','operation_id')
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RETRY'
            and exists(
              select 1 from jsonb_object_keys(supplied.raw_action) key_name
              where key_name not in(
                'action','operation_id','retry_chunk_id','replacement'
              )
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RETRY'
            and supplied.raw_action?'retry_chunk_id'
            and (
              jsonb_typeof(supplied.raw_action->'retry_chunk_id')
                is distinct from 'string'
              or coalesce(supplied.raw_action->>'retry_chunk_id','')!~*
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RETRY'
            and supplied.raw_action?'replacement'
            and (
              v_jwt_role<>'service_role'
              or jsonb_typeof(supplied.raw_action->'replacement')
                is distinct from 'object'
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RESCHEDULE'
            and (
              not(supplied.raw_action?'run_after_utc')
              or jsonb_typeof(supplied.raw_action->'run_after_utc')
                is distinct from 'string'
              or exists(
                select 1 from jsonb_object_keys(supplied.raw_action) key_name
                where key_name not in('action','operation_id','run_after_utc')
              )
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RAISE_PRIORITY'
            and exists(
              select 1 from jsonb_object_keys(supplied.raw_action) key_name
              where key_name not in('action','operation_id','priority')
            )
          )
          or (
            upper(supplied.raw_action->>'action')='RAISE_PRIORITY'
            and supplied.raw_action?'priority'
            and (
              v_jwt_role<>'service_role'
              or jsonb_typeof(supplied.raw_action->'priority')
                is distinct from 'number'
              or coalesce(supplied.raw_action->>'priority','')!~'^[0-9]{1,4}$'
              or (supplied.raw_action->>'priority')::integer>2000
            )
          )
     ) then
    raise exception using errcode='22023',
      message='OPERATION_CONTROL_ACTION_SCHEMA_INVALID';
  end if;

  if not exists(
       select 1 from public.tms_users u
       where u.id=p_actor_user_id and u.is_active and lower(u.role)='admin')
     or(v_jwt_role<>'service_role' and v_auth_user is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Active administrator required';
  end if;

  v_request_hash:=private._invoice_batch_hash_v2(jsonb_build_object(
    'contract_version','INVOICE_OPERATION_CONTROL_V2',
    'request_token',v_request_token,
    'actor_user_id',p_actor_user_id,
    'actions',v_actions
  ));

  if v_supplied_request_hash is distinct from v_request_hash then
    raise exception using errcode='22023',
      message='OPERATION_CONTROL_REQUEST_HASH_MISMATCH';
  end if;

  v_receipt_identity:=private._invoice_batch_hash_v2(jsonb_build_object(
    'contract_version','INVOICE_OPERATION_CONTROL_V2',
    'actor_user_id',p_actor_user_id,
    'request_token',v_request_token
  ));

  perform pg_advisory_xact_lock(hashtextextended(
    'INVOICE_OPERATION_CONTROL_V2|'||p_actor_user_id::text||'|'||
      v_request_token,
    0
  ));

  select
    o.result_json,
    o.input_json->>'canonical_request_hash',
    (o.input_json->>'expires_at_utc')::timestamptz
  into
    v_existing_result,
    v_existing_request_hash,
    v_existing_expires_at_utc
  from public.invoice_operations o
  where o.operation_type='OPERATION_CONTROL_REQUEST'
    and o.actor_user_id=p_actor_user_id
    and o.idempotency_key=v_receipt_identity
  order by o.created_at_utc desc,o.id desc
  limit 1;

  if v_existing_result is not null then
    if v_existing_request_hash is distinct from v_request_hash then
      raise exception using errcode='40001',
        message='OPERATION_CONTROL_IDEMPOTENCY_CONFLICT';
    end if;
    if v_existing_expires_at_utc is null
       or v_existing_expires_at_utc<=v_now then
      raise exception using errcode='40001',
        message='OPERATION_CONTROL_IDEMPOTENCY_EXPIRED';
    end if;
    return coalesce(v_existing_result->'logical_result','[]'::jsonb);
  end if;

  with recursive raw_supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_action,
      coalesce(x.value->>'operation_id','') operation_id_text,
      coalesce(x.value->>'run_after_utc','') run_after_text,
      coalesce(x.value->>'retry_chunk_id','') retry_chunk_id_text,
      x.value->'replacement' replacement_json,
      upper(coalesce(x.value->>'action','')) action,
      coalesce(x.value->>'priority','') priority_text,
      count(*) over(partition by x.value->>'operation_id') duplicate_count
    from jsonb_array_elements(v_actions) with ordinality x(value,ordinality)
  ),
  supplied as materialized (
    select r.request_no,r.raw_action,
      case when r.operation_id_text ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then r.operation_id_text::uuid end operation_id,
      r.action,
      greatest(0,least(case when r.priority_text ~ '^[0-9]{1,4}$'
        then r.priority_text::integer else 1000 end,2000)) requested_priority,
      case when r.run_after_text ~
          '^20[0-9]{2}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$'
        and substring(r.run_after_text from 9 for 2)::integer<=case
          when substring(r.run_after_text from 6 for 2)::integer in(4,6,9,11) then 30
          when substring(r.run_after_text from 6 for 2)::integer=2
            then case when substring(r.run_after_text from 1 for 4)::integer%4=0
              then 29 else 28 end
          else 31 end
        then least(r.run_after_text::timestamptz,v_now+interval '30 days') end
        requested_run_after_utc,
      r.run_after_text<>'' run_after_supplied,
      case when r.retry_chunk_id_text ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then r.retry_chunk_id_text::uuid end retry_chunk_id,
      r.retry_chunk_id_text<>'' retry_chunk_supplied,
      jsonb_typeof(r.replacement_json)='object' replacement_requested,
      r.replacement_json->>'work_key' replacement_work_key,
      r.replacement_json->'payload_json' replacement_payload_json,
      r.duplicate_count
    from raw_supplied r
  ),
  operation_tree(request_no,root_operation_id,operation_id,depth,path)
  as materialized (
    select s.request_no,s.operation_id,o.id,0,array[o.id]::uuid[]
    from supplied s
    join public.invoice_operations o on o.id=s.operation_id
    union all
    select t.request_no,t.root_operation_id,child.id,t.depth+1,t.path||child.id
    from operation_tree t
    join public.invoice_operations child on child.parent_operation_id=t.operation_id
    join supplied requested
      on requested.request_no=t.request_no
     and requested.action='CANCEL'
    where not child.id=any(t.path)
      and t.depth<64
  ),
  chunk_chain(request_no,operation_id,requested_chunk_id,current_chunk_id,
    replaced_by_chunk_id,current_status,path,depth,cycle) as (
    select
      requested.request_no,
      c.operation_id,
      c.id,
      c.id,
      c.replaced_by_chunk_id,
      c.status,
      array[c.id]::uuid[],
      1,
      false
    from supplied requested
    join public.invoice_operation_chunks c
      on c.id=requested.retry_chunk_id
     and c.operation_id=requested.operation_id
    where requested.action='RETRY'
      and requested.retry_chunk_id is not null

    union all

    select
      chain.request_no,
      chain.operation_id,
      chain.requested_chunk_id,
      replacement.id,
      replacement.replaced_by_chunk_id,
      replacement.status,
      chain.path||replacement.id,
      chain.depth+1,
      replacement.id=any(chain.path)
    from chunk_chain chain
    join public.invoice_operation_chunks replacement
      on replacement.id=chain.replaced_by_chunk_id
    where chain.replaced_by_chunk_id is not null
      and not chain.cycle
      and chain.depth<64
  ),
  targeted_current_graph as materialized (
    select distinct on (chain.request_no,chain.requested_chunk_id)
      chain.request_no,
      chain.operation_id,
      chain.requested_chunk_id,
      case
        when chain.replaced_by_chunk_id is null
         and not chain.cycle
         and chain.depth<64
          then chain.current_chunk_id
      end current_chunk_id,
      case
        when chain.cycle then 'INVALID'
        when chain.depth>=64 and chain.replaced_by_chunk_id is not null
          then 'INVALID'
        when chain.replaced_by_chunk_id is not null then 'INVALID'
        else 'VALID'
      end replacement_chain_status,
      chain.current_status
    from chunk_chain chain
    order by chain.request_no,chain.requested_chunk_id,chain.depth desc
  ),
  current_graph as materialized (
    select targeted.*
    from targeted_current_graph targeted
  ),
  inspected as materialized (
    select s.*,o.status current_status,o.operation_type,o.control_version,
      o.priority current_priority,o.input_json,o.source_revision,
      o.manifest_committed,o.release_complete,
      case
        when s.operation_id is null then 'INVALID_OPERATION_ID'
        when s.action not in('RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY')
          then 'UNSUPPORTED_ACTION'
        when s.duplicate_count>1 then 'DUPLICATE_OPERATION_ACTION'
        when o.id is null then 'OPERATION_NOT_FOUND'
        when o.operation_type='OPERATION_CONTROL_REQUEST'
          then 'OPERATION_CONTROL_RECEIPT_IMMUTABLE'
        when s.action='RESCHEDULE' and s.run_after_supplied
          and s.requested_run_after_utc is null then 'INVALID_RUN_AFTER_UTC'
        when s.action='CANCEL' and exists(
          select 1 from public.invoice_operation_chunks c
          join operation_tree tree
            on tree.request_no=s.request_no and tree.operation_id=c.operation_id
          join public.invoices i on i.id=c.entity_id
          where c.chunk_type='ISSUE_INVOICE'
            and c.entity_type='INVOICE' and i.status in('ISSUED','PAID'))
          then 'COMPLETED_LEGAL_ISSUE_CANNOT_BE_CANCELLED'
        when s.action='CANCEL' and exists(
          select 1
          from operation_tree boundary
          join public.invoice_operations child
            on child.parent_operation_id=boundary.operation_id
          where boundary.request_no=s.request_no
            and boundary.depth=64
            and not child.id=any(boundary.path))
          then 'OPERATION_TREE_DEPTH_EXCEEDED'
        when s.action='RETRY'
          and o.status not in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
          and not(
            o.status='WAITING'
            and o.requires_user_action
            and exists(
              select 1
              from public.invoice_operation_chunks retryable_chunk
              where retryable_chunk.operation_id=o.id
                and retryable_chunk.replaced_by_chunk_id is null
                and retryable_chunk.status in(
                  'FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'
                )
                and(
                  s.retry_chunk_id is null
                  or retryable_chunk.id=s.retry_chunk_id
                )
            )
          )
          then 'OPERATION_NOT_RETRYABLE'
        when s.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and o.error_json->>'code'
            ='BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
          then 'BATCH_FRESH_SELECTION_REQUIRED'
        when s.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and not o.manifest_committed
          and o.source_revision is distinct from (
            private._invoice_candidate_snapshot_get_v2(
              o.input_json->>'action',
              v_now
            )->>'revision'
          )
          then 'BATCH_FRESH_SELECTION_REQUIRED'
        when s.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and o.manifest_committed
          and s.retry_chunk_id is null
          then 'BATCH_TARGETED_RETRY_REQUIRED'
        when s.action='RETRY'
          and s.retry_chunk_id is not null
          and exists (
            select 1
            from public.invoice_operation_chunks member
            where member.id=s.retry_chunk_id
              and member.is_manifest_member
              and not member.manifest_committed
          )
          then 'UNCOMMITTED_MANIFEST_CARRIER_NOT_RETRYABLE'
        when s.action='RETRY' and s.retry_chunk_supplied
          and s.retry_chunk_id is null then 'INVALID_RETRY_CHUNK_ID'
        when s.replacement_requested and v_jwt_role<>'service_role'
          then 'REPLACEMENT_SERVICE_ONLY'
        when s.replacement_requested and(
          s.action<>'RETRY' or s.retry_chunk_id is null)
          then 'REPLACEMENT_REQUIRES_TARGETED_RETRY'
        when s.replacement_requested
          and jsonb_typeof(s.replacement_payload_json)<>'object'
          then 'INVALID_REPLACEMENT_PAYLOAD'
        when s.replacement_requested
          and nullif(btrim(s.replacement_payload_json->>'source_revision'),'')
            is null
          then 'REPLACEMENT_SOURCE_REVISION_REQUIRED'
        when s.replacement_requested and exists(
          select 1
          from current_graph current
          join public.invoice_operation_chunks old
            on old.id=current.current_chunk_id
          left join public.invoice_document_versions dv
            on dv.id=old.document_version_id
          left join public.invoice_document_assets da
            on da.id=old.document_asset_id
          where current.current_chunk_id=s.retry_chunk_id
            and s.replacement_payload_json->>'source_revision'
              is distinct from coalesce(
                dv.source_revision,da.source_revision,
                old.payload_json->>'source_revision'))
          then 'REPLACEMENT_SOURCE_REVISION_CHANGED'
        when s.action='RETRY' and s.retry_chunk_id is not null and not exists(
          select 1 from current_graph retryable
          where retryable.current_chunk_id=s.retry_chunk_id
            and retryable.operation_id=o.id
            and retryable.replacement_chain_status='VALID'
            and retryable.current_status in(
              'FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'))
          then 'RETRY_CHUNK_NOT_RETRYABLE'
        when s.action='RETRY'
          and s.retry_chunk_id is not null
          and exists(
          select 1 from current_graph invalid
          where invalid.operation_id=o.id
            and invalid.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when s.action='RESCHEDULE'
          and o.status not in('QUEUED','RETRY_WAIT','WAITING')
          then 'OPERATION_NOT_RESCHEDULABLE'
        when s.action='RAISE_PRIORITY'
          and o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'TERMINAL_OPERATION_PRIORITY_IMMUTABLE'
      end rejection_code
    from supplied s left join public.invoice_operations o on o.id=s.operation_id
  ),
  locked as materialized (
    select i.*
    from inspected i join public.invoice_operations o on o.id=i.operation_id
    where i.rejection_code is null
    order by o.id for update of o
  ),
  changed_operations as materialized (
    update public.invoice_operations o
    set
      control_version=case when l.action in('RETRY','CANCEL') then o.control_version+1
        else o.control_version end,
      status=case
        when l.action='CANCEL' then 'CANCELLED'
        when l.action='RETRY' then 'QUEUED'
        when l.action='RESCHEDULE' and o.status in('WAITING','RETRY_WAIT') then 'QUEUED'
        else o.status end,
      phase=case
        when l.action='CANCEL' then 'CANCELLED'
        when l.action='RETRY'
          and o.input_json->>'contract_version'
            ='INVOICE_BATCH_SELECTION_ROOT_V2'
          and not o.manifest_committed then 'BUILD_MANIFEST'
        when l.action='RETRY' then 'RETRY'
        else o.phase end,
      priority=case when l.action='RAISE_PRIORITY'
        then greatest(o.priority,l.requested_priority) else o.priority end,
      error_json=case when l.action='RETRY' then jsonb_build_object(
        'history',coalesce((
          select jsonb_agg(h.value order by h.ordinality)
          from jsonb_array_elements(coalesce(o.error_json->'history','[]'::jsonb))
               with ordinality h(value,ordinality)
          where h.ordinality>greatest(
            jsonb_array_length(coalesce(o.error_json->'history','[]'::jsonb))-6,0)
        ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
          'code',coalesce(o.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
        else o.error_json end,
      requires_user_action=case when l.action='RETRY' then false else o.requires_user_action end,
      failed_at_utc=case when l.action='RETRY' then null else o.failed_at_utc end,
      completed_at_utc=case when l.action='RETRY' then null else o.completed_at_utc end,
      updated_at_utc=v_now,change_seq=nextval('public.invoice_operation_change_seq')
    from locked l where o.id=l.operation_id
    returning o.*,l.action,l.requested_priority,l.requested_run_after_utc,
      l.retry_chunk_id,l.replacement_requested,l.replacement_work_key,
      l.replacement_payload_json
  ),
  replacement_chunks as materialized (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,
      sequence_no,level_no,entity_type,entity_id,document_version_id,
      document_asset_id,input_document_version_id,status,priority,
      run_after_utc,payload_json,progress_json,expected_page_count,
      expected_byte_count,attempt_count,max_attempts,fence_token,
      operation_control_version,created_at_utc,updated_at_utc)
    select old.operation_id,old.chunk_type,
      case old.chunk_type
        when 'GENERATION_GROUP' then 'VALIDATE_SOURCES'
        when 'DOCUMENT_PLAN' then 'BUILD_MANIFEST'
        when 'ISSUE_INVOICE' then 'VALIDATE'
        when 'DELIVERY_PREPARE' then 'PREPARE'
        when 'RECONCILE' then 'RECONCILE'
        else old.phase end,
      encode(extensions.digest(concat_ws('|','REPLACEMENT',old.operation_id::text,
        old.chunk_type,old.level_no::text,old.sequence_no::text,
        coalesce(old.entity_type,'~'),coalesce(old.entity_id::text,'~'),
        coalesce(old.document_version_id::text,'~'),
        coalesce(old.document_asset_id::text,'~'),
        coalesce(old.input_document_version_id::text,'~'),
        (old.plan_generation+1)::text,changed.replacement_payload_json::text),
        'sha256'),'hex'),
      old.plan_generation+1,
      old.sequence_no,old.level_no,old.entity_type,old.entity_id,
      old.document_version_id,old.document_asset_id,
      old.input_document_version_id,
      case when old.chunk_type='DOCUMENT_INPUT' then 'WAITING'
        else 'QUEUED' end,
      old.priority,v_now,changed.replacement_payload_json,'{}'::jsonb,
      old.expected_page_count,old.expected_byte_count,0,old.max_attempts,0,
      changed.control_version,v_now,v_now
    from changed_operations changed
    join public.invoice_operation_chunks old
      on old.id=changed.retry_chunk_id
    join current_graph current
      on current.current_chunk_id=old.id
      and current.replacement_chain_status='VALID'
    where changed.action='RETRY' and changed.replacement_requested
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning *
  ),
  linked_replacements as materialized (
    update public.invoice_operation_chunks old
    set status='SUPERSEDED',phase='SUPERSEDED',
      replaced_by_chunk_id=fresh.id,replacement_required=true,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','CHANGED_INPUT_REPLACED',
        'replacement_chunk_id',fresh.id,
        'replacement_plan_generation',fresh.plan_generation)
    from replacement_chunks fresh
    join changed_operations changed
      on changed.id=fresh.operation_id
      and changed.retry_chunk_id is not null
    where old.id=changed.retry_chunk_id
      and fresh.operation_id=old.operation_id
      and fresh.chunk_type=old.chunk_type
      and fresh.level_no=old.level_no
      and fresh.sequence_no=old.sequence_no
      and fresh.entity_type is not distinct from old.entity_type
      and fresh.entity_id is not distinct from old.entity_id
      and fresh.document_version_id is not distinct from old.document_version_id
      and fresh.document_asset_id is not distinct from old.document_asset_id
      and fresh.input_document_version_id is not distinct from
        old.input_document_version_id
      and fresh.plan_generation>old.plan_generation
    returning old.id,old.operation_id
  ),
  cancelled_descendant_operations as materialized (
    update public.invoice_operations child
    set control_version=child.control_version+1,status='CANCELLED',phase='CANCELLED',
        completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    from changed_operations root
    join operation_tree tree
      on tree.root_operation_id=root.id and tree.operation_id<>root.id
    where root.action='CANCEL' and child.id=tree.operation_id
      and child.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning child.*
  ),
  changed_chunks as materialized (
    update public.invoice_operation_chunks c
    set status=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
            then 'CANCELLED'
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then 'WAITING'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.chunk_type='DOCUMENT_INPUT' then 'WAITING'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then 'QUEUED'
          when o.action='RESCHEDULE' and c.status='RETRY_WAIT' then 'QUEUED'
          else c.status end,
        phase=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
            then 'CANCELLED'
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then 'WAITING_MANIFEST_COMMIT'
          when o.action='RETRY'
            and coalesce(c.payload_json->>'is_selection_expander','false')
              in ('true','t','1','yes','on')
            and not c.manifest_committed
            then 'BUILD_MANIFEST'
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then case c.chunk_type
              when 'GENERATION_GROUP' then 'VALIDATE_SOURCES'
              when 'DOCUMENT_PLAN' then 'BUILD_MANIFEST'
              when 'DOCUMENT_INPUT' then c.phase
              when 'ISSUE_INVOICE' then 'VALIDATE'
              when 'DELIVERY_PREPARE' then 'PREPARE'
              when 'RECONCILE' then 'RECONCILE'
              else c.phase end
          else c.phase end,
        priority=case
          when o.action='RAISE_PRIORITY'
            and c.chunk_type<>'DOCUMENT_INPUT'
            and c.status in('QUEUED','RETRY_WAIT','WAITING')
            then greatest(c.priority,o.requested_priority)
          else c.priority end,
        run_after_utc=case
          when o.action='RESCHEDULE' and c.status in('QUEUED','RETRY_WAIT')
            then coalesce(o.requested_run_after_utc,v_now)
          when o.action='RETRY'
            and c.is_manifest_member
            and not c.manifest_committed
            then c.run_after_utc
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then v_now
          when o.action='RAISE_PRIORITY'
            and c.chunk_type<>'DOCUMENT_INPUT'
            and c.status in('QUEUED','RETRY_WAIT','WAITING')
            then least(c.run_after_utc,v_now)
          else c.run_after_utc end,
        attempt_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then 0 else c.attempt_count end,
        result_json=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.result_json end,
        actual_page_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.actual_page_count end,
        actual_byte_count=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then null else c.actual_byte_count end,
        error_json=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id)
            and c.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
            then jsonb_build_object(
              'history',coalesce((
                select jsonb_agg(h.value order by h.ordinality)
                from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                     with ordinality h(value,ordinality)
                where h.ordinality>greatest(
                  jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
              ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
                'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
          else c.error_json end,
        failed_at_utc=case
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id) then null
          else c.failed_at_utc end,
        completed_at_utc=case
          when o.action='CANCEL'
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED') then v_now
          when o.action='RETRY'
            and(o.retry_chunk_id is null or c.id=o.retry_chunk_id) then null
          else c.completed_at_utc end,
        lease_owner=case when o.action in('RETRY','CANCEL') then null else c.lease_owner end,
        lease_token=case when o.action in('RETRY','CANCEL') then null else c.lease_token end,
        lease_expires_at_utc=case when o.action in('RETRY','CANCEL') then null
          else c.lease_expires_at_utc end,
        operation_control_version=o.control_version,
        updated_at_utc=v_now
    from changed_operations o
    where c.operation_id=o.id
      and (
        (
          o.retry_chunk_id is null
          and c.replaced_by_chunk_id is null
        )
        or c.id in(
          select current_chunk_id
          from current_graph
          where replacement_chain_status='VALID'
        )
      )
      and c.id not in(select id from linked_replacements)
      and not (
        o.action='RETRY'
        and o.retry_chunk_id is null
        and o.input_json->>'contract_version'
          ='INVOICE_BATCH_SELECTION_ROOT_V2'
        and coalesce(
          c.payload_json->>'is_selection_expander',
          'false'
        ) not in ('true','t','1','yes','on')
      )
      and(
        o.action in('RETRY','CANCEL')
        or(o.action='RESCHEDULE' and c.status in('QUEUED','RETRY_WAIT'))
        or(o.action='RAISE_PRIORITY' and c.chunk_type<>'DOCUMENT_INPUT'
          and c.status in('QUEUED','RETRY_WAIT','WAITING')))
    returning c.operation_id,c.id
  ),
  cancelled_descendant_chunks as materialized (
    update public.invoice_operation_chunks c
    set status='CANCELLED',phase='CANCELLED',completed_at_utc=v_now,
        failed_at_utc=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,
        operation_control_version=o.control_version,updated_at_utc=v_now
    from cancelled_descendant_operations o
    where c.operation_id=o.id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id,c.id,c.document_version_id,c.document_asset_id
  ),
  retry_roots as materialized (
    select c.*
    from changed_operations o
    join changed_chunks changed
      on changed.operation_id=o.id
    join public.invoice_operation_chunks c on c.id=changed.id
    where o.action='RETRY'
    union all
    select r.* from replacement_chunks r
  ),
  retried_assets as materialized (
    update public.invoice_document_assets a
    set status=case
          when r.chunk_type='ASSET_INSPECT' then 'DISCOVERED'
          else 'NORMALISING'
        end,
        normalised_manifest_json=case when r.chunk_type='ASSET_INSPECT'
          then '[]'::jsonb else a.normalised_manifest_json end,
        normalised_r2_key=null,normalised_sha256=null,
        normalised_manifest_hash=null,normalised_size_bytes=null,
        normalised_page_count=null,ready_at_utc=null,error_json=null,
        updated_at_utc=v_now
    from retry_roots r
    where r.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
      and a.id=r.document_asset_id and a.status<>'READY'
    returning a.id
  ),
  retry_document_scope as materialized (
    select distinct d.document_version_id,
      min(case
        when r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          or r.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') then 0
        when r.chunk_type='PDF_MERGE' then r.level_no
        when r.chunk_type='DOCUMENT_VERIFY' then 2147483647
        else 0 end) retry_level,
      bool_or(r.chunk_type in(
        'SOURCE_RENDER','INVOICE_CORE_RENDER','ASSET_INSPECT','ASSET_NORMALISE')) reset_inputs
    from retry_roots r
    join public.invoice_operation_chunks d
      on d.document_version_id=r.document_version_id
      or(r.document_asset_id is not null
        and d.chunk_type='DOCUMENT_INPUT'
        and d.document_asset_id=r.document_asset_id)
    where r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER','ASSET_INSPECT',
      'ASSET_NORMALISE','PDF_MERGE','DOCUMENT_VERIFY')
      and d.document_version_id is not null
    group by d.document_version_id
  ),
  fenced_retry_document_operations as materialized (
    update public.invoice_operations o
    set control_version=o.control_version+1,updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(
        select distinct v.operation_id
        from retry_document_scope scope
        join public.invoice_document_versions v on v.id=scope.document_version_id)
      and o.id not in(select id from changed_operations)
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning o.id,o.control_version
  ),
  fenced_retry_document_chunks as materialized (
    update public.invoice_operation_chunks c
    set operation_control_version=o.control_version,
        status=case when c.status='RUNNING' then 'RETRY_WAIT' else c.status end,
        run_after_utc=case when c.status='RUNNING' then v_now else c.run_after_utc end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=case when c.status='RUNNING' then jsonb_build_object(
          'code','UPSTREAM_RETRY_FENCED','at_utc',v_now) else c.error_json end,
        updated_at_utc=v_now
    from fenced_retry_document_operations o
    where c.operation_id=o.id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.id,c.operation_id
  ),
  retried_render_dependencies as (
    update public.invoice_operation_chunks d
    set status='WAITING',completed_at_utc=null,failed_at_utc=null,
        result_json=null,actual_page_count=null,actual_byte_count=null,
        error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    from retry_roots r
    where d.chunk_type='DOCUMENT_INPUT'
      and d.document_version_id=r.document_version_id
      and r.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
      and(
        d.payload_json->>'source_chunk_key'=r.payload_json->>'source_chunk_key'
        or(d.entity_type=r.entity_type and d.entity_id=r.entity_id
          and d.payload_json->>'input_type'=case
            when r.chunk_type='INVOICE_CORE_RENDER' then 'INVOICE_CORE'
            else r.payload_json->>'render_kind' end))
      and d.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
    returning d.operation_id,d.document_version_id
  ),
  retried_asset_dependencies as (
    update public.invoice_operation_chunks d
    set status='WAITING',completed_at_utc=null,failed_at_utc=null,
        result_json=null,actual_page_count=null,actual_byte_count=null,
        error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    where d.chunk_type='DOCUMENT_INPUT'
      and d.document_asset_id in(select id from retried_assets)
      and d.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
    returning d.operation_id,d.document_version_id
  ),
  invalidated_stale_document_chunks as (
    update public.invoice_operation_chunks stale
    set status='BLOCKED',phase='BLOCKED',
        result_json=null,actual_page_count=null,actual_byte_count=null,
        completed_at_utc=null,failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code','UPSTREAM_RETRY_INVALIDATED',
          'requires_replacement',true,
          'invalidated_at_utc',v_now),
        updated_at_utc=v_now
    from retry_document_scope scope
    where stale.document_version_id=scope.document_version_id
      and(
        stale.chunk_type='DOCUMENT_VERIFY'
          and not exists(
            select 1 from retry_roots selected
            where selected.id=stale.id and selected.chunk_type='DOCUMENT_VERIFY')
        or stale.chunk_type='PDF_MERGE'
          and(
            (scope.reset_inputs and stale.level_no>=scope.retry_level)
            or(not scope.reset_inputs and stale.level_no>scope.retry_level)))
      and stale.status<>'SUPERSEDED'
    returning stale.operation_id,stale.document_version_id,stale.id
  ),
  retried_document_versions as (
    update public.invoice_document_versions v
    set status=case when scope.reset_inputs then 'WAITING_FOR_INPUTS'
          when scope.retry_level<2147483647 then 'ASSEMBLING'
          else 'VERIFYING' end,
        r2_key=null,sha256=null,size_bytes=null,page_count=null,
        ready_at_utc=null,verified_at_utc=null,error_json=null
    from retry_document_scope scope
    where v.id=scope.document_version_id
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  ),
  retried_document_plans as (
    update public.invoice_operation_chunks p
    set status='QUEUED',
        phase=case when scope.reset_inputs then 'WAIT_FOR_INPUTS' else 'WAIT_FOR_MERGE' end,
        attempt_count=0,
        run_after_utc=v_now,
        failed_at_utc=null,error_json=null,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null,updated_at_utc=v_now
    from retry_document_scope scope
    where p.chunk_type='DOCUMENT_PLAN' and p.document_version_id=scope.document_version_id
      and p.status in('WAITING','RETRY_WAIT','BLOCKED','FAILED','DEAD_LETTER')
    returning p.id
  ),
  cancelled_document_versions as (
    update public.invoice_document_versions v
    set status='CANCELLED',error_json=jsonb_build_object(
          'code','OPERATION_CANCELLED','at_utc',v_now),
        superseded_at_utc=v_now
    where v.operation_id in(
        select id from changed_operations where action='CANCEL'
        union all select id from cancelled_descendant_operations)
      and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','FAILED')
    returning v.id,v.operation_id
  ),
  cancelled_operation_set as materialized (
    select id from changed_operations where action='CANCEL'
    union
    select id from cancelled_descendant_operations
  ),
  cancelled_invoice_pointers as (
    update public.invoices i
    set active_document_operation_id=case when i.active_document_operation_id=o.id
          then null else i.active_document_operation_id end,
        active_issue_operation_id=case when i.active_issue_operation_id=o.id
          then null else i.active_issue_operation_id end,
        document_state=case when i.active_document_operation_id=o.id
          and i.document_state in('QUEUED','PREPARING') then 'STALE' else i.document_state end,
        issue_state=case when i.active_issue_operation_id=o.id
          and i.issue_state not in('ISSUED') then 'CANCELLED' else i.issue_state end,
        updated_at=v_now
    from cancelled_operation_set cancelled
    join public.invoice_operations o on o.id=cancelled.id
    where i.active_document_operation_id=o.id or i.active_issue_operation_id=o.id
    returning i.id
  ),
  cancelled_timesheet_pointers as (
    update public.timesheets t
    set active_document_operation_id=null,
        document_state=case when t.document_state in('QUEUED','PREPARING')
          then 'STALE' else t.document_state end,
        updated_at=v_now
    from cancelled_operation_set cancelled
    join public.invoice_operations o on o.id=cancelled.id
    where t.active_document_operation_id=o.id
    returning t.timesheet_id
  ),
  affected_operation_ids as materialized (
    select id operation_id from changed_operations
    union select id from cancelled_descendant_operations
    union select id from fenced_retry_document_operations
    union select operation_id from retried_render_dependencies
    union select operation_id from retried_asset_dependencies
    union select operation_id from invalidated_stale_document_chunks
  ),
  recalculated_operations as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(operation_id) from affected_operation_ids),
        array[]::uuid[]),v_now,true) r
  ),
  results as (
    select i.request_no,jsonb_build_object(
      'operation_id',i.operation_id,'action',i.action,'accepted',false,
      'status',i.current_status,'control_version',i.control_version,
      'error',jsonb_build_object('code',i.rejection_code)) result
    from inspected i where i.rejection_code is not null
    union all
    select i.request_no,jsonb_build_object(
      'operation_id',changed.id,'action',changed.action,'accepted',true,
      'status',recalculated.status,'phase',recalculated.phase,
      'control_version',changed.control_version,
      'change_seq',recalculated.change_seq,'priority',changed.priority,
      'total_units',recalculated.total_units,
      'completed_units',recalculated.completed_units,
      'failed_units',recalculated.failed_units) result
    from inspected i
    join changed_operations changed on changed.id=i.operation_id
    join recalculated_operations recalculated
      on recalculated.operation_id=i.operation_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  insert into public.invoice_operations(
    operation_type,
    entity_type,
    actor_user_id,
    idempotency_key,
    status,
    phase,
    priority,
    source_revision,
    input_json,
    config_json,
    progress_json,
    result_json,
    total_units,
    completed_units,
    failed_units,
    chunk_count,
    completed_at_utc,
    created_at_utc,
    updated_at_utc
  ) values (
    'OPERATION_CONTROL_REQUEST',
    'OPERATION_CONTROL',
    p_actor_user_id,
    v_receipt_identity,
    'COMPLETE',
    'TERMINAL',
    0,
    v_request_hash,
    jsonb_build_object(
      'contract_version','INVOICE_OPERATION_CONTROL_V2',
      'request_token',v_request_token,
      'canonical_request_hash',v_request_hash,
      'canonical_request_payload',jsonb_build_object(
        'contract_version','INVOICE_OPERATION_CONTROL_V2',
        'request_token',v_request_token,
        'actor_user_id',p_actor_user_id,
        'actions',v_actions
      ),
      'actor_user_id',p_actor_user_id,
      'request_scope','OPERATION_CONTROL',
      'created_at_utc',v_now,
      'expires_at_utc',v_now+interval '30 days'
    ),
    jsonb_build_object(
      'command_type','OPERATION_CONTROL_REQUEST',
      'processor_policy',private._invoice_processor_limits()
    ),
    jsonb_build_object(
      'contract_version','INVOICE_OPERATION_CONTROL_V2',
      'status_message','Operation control request completed'
    ),
    jsonb_build_object(
      'contract_version','INVOICE_OPERATION_CONTROL_V2',
      'logical_result',v_result
    ),
    1,
    1,
    0,
    0,
    v_now,
    v_now,
    v_now
  );

  return v_result;
end;
$function$;

-- invoice_operation_get(uuid[],uuid,text,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_operation_get(p_operation_ids uuid[], p_actor_user_id uuid, p_mode text DEFAULT 'PROGRESS'::text, p_page_request jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_service boolean := coalesce(auth.role(),'')='service_role';
  v_role text;
  v_operations jsonb;
  v_batch_operations jsonb;
  v_batch_root_count integer;
  v_category text;
  v_after_selection_key text;
  v_after_chunk_id uuid;
  v_limit integer;
  v_root_id uuid;
  v_root_manifest_generation integer;
  v_root_result_page_revision bigint;
  v_expected_result_page_revision bigint;
  v_result_page jsonb;
begin
  if cardinality(coalesce(p_operation_ids,array[]::uuid[]))<1
     or cardinality(p_operation_ids)>100 then
    raise exception using
      errcode='22023',
      message='p_operation_ids must contain 1..100 IDs';
  end if;

  if not v_service
     and (
       auth.uid() is null
       or auth.uid() is distinct from p_actor_user_id
     ) then
    raise exception using
      errcode='42501',
      message='Authenticated actor mismatch';
  end if;

  select lower(btrim(coalesce(u.role,'')))
  into v_role
  from public.tms_users u
  where u.id=p_actor_user_id
    and u.is_active;

  if not found and not v_service then
    raise exception using errcode='42501', message='Active actor required';
  end if;

  select
    count(*)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'operation_id',root.id,
      'parent_operation_id',root.parent_operation_id,
      'operation_type',root.operation_type,
      'entity_type',root.entity_type,
      'entity_id',root.entity_id,
      'status',root.status,
      'phase',root.phase,
      'priority',root.priority,
      'source_revision',root.source_revision,
      'template_version',root.template_version,
      'total_units',root.total_units,
      'completed_units',root.completed_units,
      'failed_units',root.failed_units,
      'progress',coalesce(root.progress_json,'{}'::jsonb),
      'result',case
        when upper(coalesce(p_mode,'PROGRESS'))='DETAIL'
          or root.status='COMPLETE'
        then coalesce(root.result_json,'{}'::jsonb)
      end,
      'error_code',root.error_json->>'code',
      'error_summary',coalesce(
        root.error_json->>'summary',
        root.error_json->>'message'
      ),
      'requires_user_action',root.requires_user_action,
      'can_retry',root.status in (
        'FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'
      ),
      'can_cancel',root.status in (
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
      ) and not exists (
        select 1
        from public.invoice_operation_chunks issue_chunk
        join public.invoices issued_invoice
          on issued_invoice.id=issue_chunk.entity_id
        where issue_chunk.operation_id=root.id
          and issue_chunk.chunk_type='ISSUE_INVOICE'
          and issue_chunk.entity_type='INVOICE'
          and issued_invoice.status in ('ISSUED','PAID')
      ),
      'change_seq',root.change_seq,
      'effective_change_seq',root.change_seq,
      'result_page_revision',root.result_page_revision,
      'manifest_generation',root.manifest_generation,
      'manifest_committed',root.manifest_committed,
      'release_complete',root.release_complete,
      'total_chunks',root.chunk_count,
      'returned_chunks',0,
      'chunks_truncated',root.chunk_count>0,
      'children','[]'::jsonb,
      'chunks','[]'::jsonb,
      'created_at_utc',root.created_at_utc,
      'updated_at_utc',root.updated_at_utc,
      'completed_at_utc',root.completed_at_utc
    ) order by requested.ordinality),'[]'::jsonb)
  into v_batch_root_count,v_batch_operations
  from unnest(p_operation_ids) with ordinality requested(id,ordinality)
  join public.invoice_operations root on root.id=requested.id
  where (
      v_service
      or v_role='admin'
      or root.actor_user_id=p_actor_user_id
    )
    and root.entity_type='INVOICE_BATCH'
    and root.input_json->>'contract_version'
      ='INVOICE_BATCH_SELECTION_ROOT_V2';

  if p_page_request is null then
    if v_batch_root_count=cardinality(p_operation_ids) then
      return v_batch_operations;
    end if;
    return private._invoice_operation_get_core_v8(
      p_operation_ids,
      p_actor_user_id,
      p_mode
    );
  end if;

  if jsonb_typeof(p_page_request) is distinct from 'object' then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_PAGE_REQUEST_INVALID';
  end if;

  select
    root.id,
    root.manifest_generation,
    root.result_page_revision
  into
    v_root_id,
    v_root_manifest_generation,
    v_root_result_page_revision
  from unnest(p_operation_ids) requested(id)
  join public.invoice_operations root on root.id=requested.id
  where (
      v_service
      or v_role='admin'
      or root.actor_user_id=p_actor_user_id
    )
    and root.entity_type='INVOICE_BATCH'
    and root.input_json->>'contract_version'
      ='INVOICE_BATCH_SELECTION_ROOT_V2'
  order by root.id
  limit 1;

  if v_root_id is null
     or (
       select count(*)
       from unnest(p_operation_ids) requested(id)
       join public.invoice_operations root on root.id=requested.id
       where (
           v_service
           or v_role='admin'
           or root.actor_user_id=p_actor_user_id
         )
         and root.entity_type='INVOICE_BATCH'
         and root.input_json->>'contract_version'
           ='INVOICE_BATCH_SELECTION_ROOT_V2'
     )<>1 then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_ROOT_INVALID';
  end if;

  if coalesce(p_page_request->>'result_page_revision','')
    !~ '^[0-9]{1,18}$' then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CURSOR_INVALID';
  end if;
  v_expected_result_page_revision :=
    (p_page_request->>'result_page_revision')::bigint;

  if v_expected_result_page_revision
     is distinct from v_root_result_page_revision then
    raise exception using
      errcode='40001',
      message='OPERATION_RESULT_CURSOR_STALE';
  end if;

  v_category := upper(coalesce(
    nullif(p_page_request->>'category',''),
    'ALL'
  ));
  if v_category not in (
    'ALL','READY','COMPLETED','IN_PROGRESS',
    'GENERATED','REGENERATED','ISSUED','ISSUED_SEND_BLOCKED',
    'ALREADY_ACTIVE','BLOCKED','FAILED','CHANGED'
  ) then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CATEGORY_INVALID';
  end if;

  v_after_selection_key := nullif(btrim(coalesce(
    p_page_request->>'after_selection_key',
    p_page_request#>>'{cursor,after_selection_key}',
    ''
  )),'');

  if nullif(coalesce(
       p_page_request->>'after_chunk_id',
       p_page_request#>>'{cursor,after_chunk_id}',
       ''
     ),'') is not null then
    if not pg_input_is_valid(coalesce(
      p_page_request->>'after_chunk_id',
      p_page_request#>>'{cursor,after_chunk_id}'
    ),'uuid') then
      raise exception using
        errcode='22023',
        message='OPERATION_RESULT_CURSOR_INVALID';
    end if;
    v_after_chunk_id := coalesce(
      p_page_request->>'after_chunk_id',
      p_page_request#>>'{cursor,after_chunk_id}'
    )::uuid;
  end if;

  if (v_after_selection_key is null)
     is distinct from (v_after_chunk_id is null) then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CURSOR_INVALID';
  end if;

  if v_after_chunk_id is not null
     and not exists (
       select 1
       from public.invoice_operation_chunks anchor
       where anchor.id=v_after_chunk_id
         and anchor.operation_id=v_root_id
         and anchor.manifest_generation=v_root_manifest_generation
         and anchor.result_visible
         and anchor.selection_key=v_after_selection_key
         and anchor.replaced_by_chunk_id is null
     ) then
    raise exception using
      errcode='40001',
      message='OPERATION_RESULT_CURSOR_STALE';
  end if;

  v_limit := case
    when coalesce(p_page_request->>'limit','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((p_page_request->>'limit')::integer,100))
    else 100
  end;

  v_operations := v_batch_operations;

  with
  current_carriers as materialized (
    select carrier.*
    from public.invoice_operation_chunks carrier
    where carrier.operation_id=v_root_id
      and carrier.manifest_generation=v_root_manifest_generation
      and carrier.result_visible
      and carrier.selection_key is not null
      and carrier.replaced_by_chunk_id is null
      and carrier.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
  ),
  matching as materialized (
    select carrier.*
    from current_carriers carrier
    where v_category='ALL'
       or carrier.result_category=v_category
       or (
         v_category='READY'
         and carrier.status='COMPLETE'
       )
       or (
         v_category='COMPLETED'
         and carrier.result_category in (
           'GENERATED','REGENERATED','ISSUED',
           'ISSUED_SEND_BLOCKED','ALREADY_ACTIVE'
         )
       )
       or (
         v_category='IN_PROGRESS'
         and carrier.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
       )
  ),
  cursor_filtered as materialized (
    select carrier.*
    from matching carrier
    where v_after_selection_key is null
       or (
         carrier.selection_key,
         carrier.id
       )>(
         v_after_selection_key,
         v_after_chunk_id
       )
  ),
  page as materialized (
    select carrier.*
    from cursor_filtered carrier
    order by carrier.selection_key,carrier.id
    limit v_limit+1
  ),
  visible as materialized (
    select carrier.*
    from page carrier
    order by carrier.selection_key,carrier.id
    limit v_limit
  ),
  enriched as materialized (
    select
      carrier.*,
      case
        when carrier.entity_type='INVOICE' then carrier.entity_id
        when pg_input_is_valid(
          coalesce(carrier.payload_json->>'invoice_id',''),
          'uuid'
        ) then (carrier.payload_json->>'invoice_id')::uuid
      end invoice_id
    from visible carrier
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_RESULT_PAGE_V2',
    'root_operation_id',v_root_id,
    'result_page_revision',v_root_result_page_revision,
    'category',v_category,
    'rows',coalesce((
      select jsonb_agg(jsonb_build_object(
        'selection_key',row.selection_key,
        'chunk_id',row.id,
        'result_category',row.result_category,
        'entity_type',row.entity_type,
        'entity_id',row.entity_id,
        'invoice_id',row.invoice_id,
        'invoice_number',coalesce(
          row.payload_json->>'invoice_number',
          invoice.invoice_no
        ),
        'client_name',coalesce(
          row.payload_json->>'client_name',
          client.name
        ),
        'candidate_display',row.payload_json->>'candidate_display',
        'week_ending_display',row.payload_json->>'week_ending_display',
        'currency',coalesce(row.payload_json->>'currency','GBP'),
        'total_ex_vat',row.payload_json->'total_ex_vat',
        'total_inc_vat',row.payload_json->'total_inc_vat',
        'row_kind',row.payload_json->>'row_kind',
        'status',row.status,
        'phase',row.phase,
        'badge_codes',coalesce(
          row.result_json->'badge_codes',
          row.payload_json->'action_blocker_codes',
          row.payload_json->'issue_blocker_codes',
          case
            when row.error_json ? 'code'
              then jsonb_build_array(row.error_json->>'code')
          end,
          '[]'::jsonb
        ),
        'error_code',row.error_json->>'code',
        'document_version_id',coalesce(
          row.result_json->>'issued_document_version_id',
          row.result_json->>'document_version_id',
          row.document_version_id::text
        ),
        'can_view',coalesce(
          row.result_json->>'issued_document_version_id',
          row.result_json->>'document_version_id',
          row.document_version_id::text
        ) is not null
          and row.status='COMPLETE',
        'blocked_for_sending',coalesce(
          (row.payload_json->>'blocked_for_sending')::boolean,
          false
        )
      ) order by row.selection_key,row.id)
      from enriched row
      left join public.invoices invoice on invoice.id=row.invoice_id
      left join public.clients client on client.id=invoice.client_id
    ),'[]'::jsonb),
    'has_more',(select count(*) from page)>v_limit,
    'next_cursor_values',case
      when (select count(*) from page)>v_limit then (
        select jsonb_build_object(
          'after_selection_key',row.selection_key,
          'after_chunk_id',row.id
        )
        from visible row
        order by row.selection_key desc,row.id desc
        limit 1
      )
      else null
    end,
    'total_count',(select count(*) from matching),
    'limit',v_limit
  )
  into v_result_page;

  return jsonb_build_object(
    'operations',coalesce(v_operations,'[]'::jsonb),
    'result_page',v_result_page
  );
end;
$function$;

-- invoice_operation_start_batch(jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_operation_start_batch(p_commands jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := statement_timestamp();
  v_jwt_role text := coalesce(
    nullif(current_setting('request.jwt.claim.role', true), ''),
    auth.jwt()->>'role',
    ''
  );
  v_auth_user uuid := auth.uid();
  v_role text;
  v_has_selection boolean := false;
  v_results jsonb := '[]'::jsonb;
  v_command jsonb;
  v_command_no integer;
  v_command_type text;
  v_action text;
  v_error_code text;
  v_error_detail jsonb;
  v_selection_contract jsonb;
  v_selection jsonb;
  v_query jsonb;
  v_normalised_query jsonb;
  v_summary_query jsonb;
  v_allow_early boolean;
  v_deliver boolean;
  v_delivery_intent jsonb;
  v_command_token text;
  v_delivery_request_token text;
  v_filter_hash text;
  v_query_hash text;
  v_selection_hash text;
  v_delivery_hash text;
  v_idempotency_key text;
  v_operation_type text;
  v_chunk_type text;
  v_priority integer;
  v_operation_id uuid;
  v_operation_status text;
  v_operation_phase text;
  v_change_seq bigint;
  v_created boolean;
  v_reused boolean;
  v_core_result jsonb;
  v_result_item jsonb;
  v_summary jsonb;
  v_filtered_total integer;
  v_eligible_total integer;
  v_selected_total integer;
  v_previous_statement_timeout text := current_setting('statement_timeout');
BEGIN
  IF v_jwt_role = 'service_role' THEN
    v_now := coalesce(p_now_utc, statement_timestamp());
  END IF;

  IF jsonb_typeof(p_commands) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_commands must be a JSON array containing 1..1000 commands';
  END IF;

  IF jsonb_array_length(p_commands) < 1
     OR jsonb_array_length(p_commands) > 1000 THEN
    RAISE EXCEPTION USING errcode = '22023',
      message = 'p_commands must be a JSON array containing 1..1000 commands';
  END IF;

  IF p_actor_user_id IS NULL
     OR NOT EXISTS (
       SELECT 1
       FROM public.tms_users u
       WHERE u.id = p_actor_user_id
         AND u.is_active
         AND lower(u.role) = 'admin'
     )
     OR (v_jwt_role <> 'service_role' AND v_auth_user IS DISTINCT FROM p_actor_user_id) THEN
    RAISE EXCEPTION USING errcode = '42501',
      message = 'Active administrator actor and matching authenticated/service caller required';
  END IF;

  SELECT lower(btrim(coalesce(u.role, '')))
    INTO v_role
  FROM public.tms_users u
  WHERE u.id = p_actor_user_id
    AND u.is_active;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(p_commands) e(value)
    WHERE jsonb_typeof(e.value->'selection_contract') = 'object'
  ) INTO v_has_selection;

  IF NOT v_has_selection THEN
    RETURN private._invoice_operation_start_core_v8(p_commands, p_actor_user_id, v_now);
  END IF;

  FOR v_command, v_command_no IN
    SELECT e.value, e.ordinality::integer
    FROM jsonb_array_elements(p_commands) WITH ORDINALITY e(value, ordinality)
    ORDER BY e.ordinality
  LOOP
    v_command_type := upper(btrim(coalesce(v_command->>'command_type', v_command->>'type', '')));
    v_error_code := NULL;
    v_error_detail := NULL;
    v_result_item := NULL;

    IF jsonb_typeof(v_command->'selection_contract') IS DISTINCT FROM 'object' THEN
      v_core_result := private._invoice_operation_start_core_v8(
        jsonb_build_array(v_command),
        p_actor_user_id,
        v_now
      );

      IF jsonb_typeof(v_core_result) = 'array' AND jsonb_array_length(v_core_result) > 0 THEN
        v_results := v_results || jsonb_build_array((v_core_result->0) || jsonb_build_object('command_no', v_command_no));
      ELSE
        v_results := v_results || jsonb_build_array(jsonb_build_object(
          'command_no', v_command_no,
          'command_type', v_command_type,
          'accepted', false,
          'terminal_error', jsonb_build_object('code', 'CORE_START_RETURNED_NO_RESULT'),
          'error', jsonb_build_object('code', 'CORE_START_RETURNED_NO_RESULT')
        ));
      END IF;

      CONTINUE;
    END IF;

    IF v_command_type = 'GENERATE_SELECTED' THEN
      v_action := 'GENERATE';
      v_operation_type := 'GENERATE_INVOICES';
      v_chunk_type := 'GENERATION_GROUP';
      v_priority := 600;
    ELSIF v_command_type = 'ISSUE_INVOICES' THEN
      v_action := 'ISSUE';
      v_operation_type := 'ISSUE_INVOICES';
      v_chunk_type := 'ISSUE_INVOICE';
      v_priority := 850;
    ELSE
      v_error_code := 'BATCH_SELECTION_COMMAND_UNSUPPORTED';
    END IF;

    v_selection_contract := v_command->'selection_contract';

    IF v_error_code IS NULL
       AND coalesce(v_selection_contract->>'contract_version', '') <> 'INVOICE_BATCH_SELECTION_ROOT_V2' THEN
      v_error_code := 'BATCH_SELECTION_CONTRACT_INVALID';
    END IF;

    IF v_error_code IS NULL
       AND jsonb_typeof(v_selection_contract->'query') IS DISTINCT FROM 'object' THEN
      v_error_code := 'BATCH_QUERY_INVALID';
      v_error_detail := jsonb_build_object('field', 'selection_contract.query', 'reason', 'required_object');
    END IF;

    IF v_error_code IS NULL
       AND jsonb_typeof(v_selection_contract->'selection') IS DISTINCT FROM 'object' THEN
      v_error_code := 'BATCH_SELECTION_INVALID';
      v_error_detail := jsonb_build_object('field', 'selection_contract.selection', 'reason', 'required_object');
    END IF;

    IF v_error_code IS NULL THEN
      v_query := v_selection_contract->'query';
      v_selection := v_selection_contract->'selection';

      BEGIN
        v_query := private._invoice_batch_query_validate_v2(
          v_query,
          v_action
        );
      EXCEPTION
        WHEN OTHERS THEN
          v_error_code := sqlerrm;
          v_error_detail := jsonb_build_object(
            'sqlstate', sqlstate,
            'message', sqlerrm
          );
      END;
    END IF;

    IF v_error_code IS NULL THEN
      BEGIN
        PERFORM 1
        FROM private._invoice_batch_selection_rules_v2(v_selection)
        LIMIT 1;
      EXCEPTION WHEN OTHERS THEN
        v_error_code := CASE
          WHEN SQLSTATE = '22023' THEN 'BATCH_SELECTION_INVALID'
          ELSE 'BATCH_SELECTION_INVALID'
        END;
        v_error_detail := jsonb_build_object('sqlstate', SQLSTATE, 'message', SQLERRM);
      END;
    END IF;

    IF v_error_code IS NULL THEN
      v_command_token := nullif(btrim(coalesce(
        v_command->>'command_token',
        v_selection_contract->>'command_token',
        ''
      )), '');

      IF v_command_token IS NULL THEN
        v_error_code := CASE WHEN v_action = 'ISSUE'
          THEN 'ISSUE_COMMAND_TOKEN_REQUIRED'
          ELSE 'GENERATE_COMMAND_TOKEN_REQUIRED'
        END;
      ELSIF length(v_command_token) > 256 THEN
        v_error_code := 'BATCH_COMMAND_TOKEN_INVALID';
        v_error_detail := jsonb_build_object('field', 'command_token', 'reason', 'too_long', 'max_length', 256);
      END IF;
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'deliver'
       AND jsonb_typeof(v_command->'deliver') <> 'boolean' THEN
      v_error_code := 'ISSUE_FLAGS_MUST_BE_BOOLEAN';
    END IF;

    IF v_error_code IS NULL
       AND v_action = 'ISSUE'
       AND NOT (v_command ? 'deliver') THEN
      v_error_code := 'ISSUE_DELIVERY_MODE_REQUIRED';
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'allow_early'
       AND jsonb_typeof(v_command->'allow_early') <> 'boolean' THEN
      v_error_code := CASE WHEN v_action = 'ISSUE'
        THEN 'ISSUE_FLAGS_MUST_BE_BOOLEAN'
        ELSE 'ALLOW_EARLY_MUST_BE_BOOLEAN'
      END;
    END IF;

    IF v_error_code IS NULL
       AND v_command ? 'delivery_intent'
       AND jsonb_typeof(v_command->'delivery_intent') <> 'object' THEN
      v_error_code := 'ISSUE_DELIVERY_INTENT_MUST_BE_OBJECT';
    END IF;

    IF v_error_code IS NULL THEN
      v_allow_early := coalesce(
        (v_query#>>'{filters,allow_early}')::boolean,
        false
      );

      v_deliver := CASE
        WHEN jsonb_typeof(v_command->'deliver') = 'boolean'
          THEN (v_command->>'deliver')::boolean
        ELSE false
      END;

      v_delivery_intent := CASE
        WHEN jsonb_typeof(v_command->'delivery_intent') = 'object'
          THEN v_command->'delivery_intent'
        ELSE '{}'::jsonb
      END;

      v_delivery_request_token := nullif(btrim(coalesce(
        v_command->>'delivery_request_token',
        ''
      )), '');

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND (
           v_delivery_intent ? 'recipient_set'
           OR v_delivery_intent ? 'cc'
           OR v_delivery_intent ? 'bcc'
         ) THEN
        v_error_code := 'ISSUE_BATCH_DELIVERY_RECIPIENT_OVERRIDE_UNSUPPORTED';
      END IF;

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND upper(coalesce(v_delivery_intent->>'route_mode','')) <> 'SERVER_RESOLVED' THEN
        v_error_code := 'ISSUE_DELIVERY_INTENT_INVALID';
      END IF;

      IF v_error_code IS NULL
         AND v_action = 'ISSUE'
         AND v_deliver
         AND v_delivery_request_token IS NULL THEN
        v_error_code := 'DELIVERY_REQUEST_TOKEN_REQUIRED';
      ELSIF v_error_code IS NULL
         AND v_delivery_request_token IS NOT NULL
         AND length(v_delivery_request_token) > 256 THEN
        v_error_code := 'DELIVERY_REQUEST_TOKEN_INVALID';
        v_error_detail := jsonb_build_object('field', 'delivery_request_token', 'reason', 'too_long', 'max_length', 256);
      END IF;

      IF v_error_code IS NULL THEN
        v_normalised_query := private._invoice_batch_query_validate_v2(
          jsonb_build_object(
            'contract_version', 'INVOICE_BATCH_QUERY_V2',
            'action', v_action,
            'mode', 'EXPAND_SELECTION',
            'snapshot', v_query->'snapshot',
            'page_size', 250,
            'cursor', null,
            'filters', v_query->'filters',
            'sort', v_query->'sort',
            'selection', v_selection
          ),
          v_action
        );

        v_summary_query := private._invoice_batch_query_validate_v2(
          jsonb_build_object(
            'contract_version', 'INVOICE_BATCH_QUERY_V2',
            'action', v_action,
            'mode', 'SUMMARY',
            'snapshot', v_query->'snapshot',
            'filters', v_query->'filters',
            'sort', v_query->'sort',
            'selection', v_selection,
            'group_selectors', '[]'::jsonb
          ),
          v_action
        );
      END IF;

      IF v_error_code IS NULL THEN
        BEGIN
          perform set_config('statement_timeout','7000',true);
          v_summary := case when v_action='GENERATE'
            then private._invoice_batch_generate_candidate_rows_v2(
              v_summary_query,
              v_now
            )
            else private._invoice_batch_issue_candidate_rows_v2(
              v_summary_query,
              v_now
            )
          end;
          perform set_config('statement_timeout',v_previous_statement_timeout,true);

          v_filtered_total := coalesce(
            (v_summary#>>'{totals,filtered_total}')::integer,
            0
          );
          v_selected_total := coalesce(
            (v_summary#>>'{selection_summary,selected_total}')::integer,
            0
          );
          v_eligible_total := coalesce(
            (v_summary#>>'{selection_summary,eligible_total}')::integer,
            0
          );

          if v_filtered_total > 25000 then
            v_error_code := 'BATCH_SUMMARY_SCOPE_TOO_LARGE';
          elsif v_selected_total = 0 then
            v_error_code := 'BATCH_SELECTION_EMPTY';
          end if;
        exception
          when query_canceled then
            perform set_config('statement_timeout',v_previous_statement_timeout,true);
            v_error_code := 'BATCH_SUMMARY_TIMEOUT';
          when others then
            perform set_config('statement_timeout',v_previous_statement_timeout,true);
            v_error_code := case
              when sqlerrm in (
                'BATCH_SNAPSHOT_REQUIRED',
                'BATCH_SNAPSHOT_INVALID',
                'BATCH_SNAPSHOT_EXPIRED',
                'BATCH_SNAPSHOT_CHANGED',
                'BATCH_SELECTION_INVALID',
                'BATCH_SELECTION_CONTRACT_INVALID',
                'BATCH_SELECTION_SELECTOR_INVALID'
              ) then sqlerrm
              else 'BATCH_QUERY_INVALID'
            end;
            v_error_detail := jsonb_build_object(
              'sqlstate',sqlstate,
              'message',sqlerrm
            );
        end;
      END IF;

      IF v_error_code IS NULL THEN
      v_filter_hash := private._invoice_batch_hash_v2(
        jsonb_build_object(
          'action',v_action,
          'filters',coalesce(v_normalised_query->'filters','{}'::jsonb),
          'sort',coalesce(v_normalised_query->'sort','{}'::jsonb)
        )
      );
      v_query_hash := private._invoice_batch_hash_v2(
        jsonb_build_object(
          'contract_version','INVOICE_BATCH_QUERY_V2',
          'action',v_action,
          'filters',coalesce(v_normalised_query->'filters','{}'::jsonb),
          'sort',coalesce(v_normalised_query->'sort','{}'::jsonb),
          'snapshot',jsonb_build_object(
            'contract_version',
              v_normalised_query#>>'{snapshot,contract_version}',
            'action',v_normalised_query#>>'{snapshot,action}',
            'at_utc',v_normalised_query#>>'{snapshot,at_utc}',
            'revision',v_normalised_query#>>'{snapshot,revision}',
            'expires_at_utc',
              v_normalised_query#>>'{snapshot,expires_at_utc}',
            'key_id',v_normalised_query#>>'{snapshot,key_id}'
          )
        )
      );
      v_selection_hash := private._invoice_batch_hash_v2(v_selection);
      v_delivery_hash := private._invoice_batch_hash_v2(v_delivery_intent);

      v_idempotency_key := CASE WHEN v_action = 'GENERATE' THEN
        private._invoice_batch_hash_v2(jsonb_build_object(
          'command_type',
          'GENERATE_SELECTED',
          'query_hash',v_query_hash,
          'selection_hash',v_selection_hash,
          'allow_early',v_allow_early,
          'command_token',v_command_token
        ))
      ELSE
        private._invoice_batch_hash_v2(jsonb_build_object(
          'command_type',
          'ISSUE_INVOICES',
          'query_hash',v_query_hash,
          'selection_hash',v_selection_hash,
          'allow_early',v_allow_early,
          'deliver',v_deliver,
          'command_token',v_command_token,
          'delivery_request_token',v_delivery_request_token,
          'delivery_hash',v_delivery_hash
        ))
      END;

      PERFORM pg_advisory_xact_lock(hashtextextended('INVOICE_BATCH_SELECTION_ROOT|' || v_idempotency_key, 0));

      SELECT o.id, o.status, o.phase, o.change_seq
        INTO v_operation_id, v_operation_status, v_operation_phase, v_change_seq
      FROM public.invoice_operations o
      WHERE o.idempotency_key = v_idempotency_key
        AND o.status IN ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      ORDER BY (o.status = 'COMPLETE') DESC, o.created_at_utc DESC
      LIMIT 1;

      v_created := false;
      v_reused := v_operation_id IS NOT NULL;

      IF v_operation_id IS NULL THEN
        INSERT INTO public.invoice_operations(
          operation_type,
          entity_type,
          entity_id,
          actor_user_id,
          idempotency_key,
          status,
          phase,
          priority,
          source_revision,
          template_version,
          input_json,
          config_json,
          progress_json,
          total_units,
          completed_units,
          failed_units,
          chunk_count,
          control_version,
          change_seq,
          manifest_generation,
          manifest_committed,
          release_complete,
          result_page_revision,
          created_at_utc,
          updated_at_utc
        ) VALUES (
          v_operation_type,
          'INVOICE_BATCH',
          NULL,
          p_actor_user_id,
          v_idempotency_key,
          'QUEUED',
          'BUILD_MANIFEST',
          v_priority,
          v_normalised_query#>>'{snapshot,revision}',
          NULL,
          jsonb_build_object(
            'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
            'command_type', v_command_type,
            'action', v_action,
            'selection_expansion_pending', true,
            'filter_hash', v_filter_hash,
            'query_hash', v_query_hash,
            'selection_hash', v_selection_hash,
            'snapshot',v_normalised_query->'snapshot',
            'command_token', v_command_token,
            'deliver', v_deliver,
            'delivery_request_token', v_delivery_request_token,
            'delivery_intent', v_delivery_intent,
            'selection_contract', jsonb_build_object(
              'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
              'action', v_action,
              'query', v_normalised_query,
              'selection', v_selection
            )
          ),
          jsonb_build_object(
            'command_type', v_command_type,
            'processor_policy', private._invoice_processor_limits()
          ),
          jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message', 'Building selection manifest',
            'manifest_generation',1,
            'manifest_status','BUILDING',
            'selection_expansion_pending', true,
            'manifest_committed',false,
            'expected_scan_total',v_filtered_total,
            'scanned_total',0,
            'release_pending_total',0,
            'released_total',0,
            'release_conflict_total',0,
            'release_blocked_total',0,
            'release_complete',false,
            'committed_at_utc',null,
            'superseded_manifest_generation',null,
            'candidate_total', case when v_action='GENERATE' then v_filtered_total else 0 end,
            'invoice_total', case when v_action='ISSUE' then v_filtered_total else 0 end,
            'selected_total', v_selected_total,
            'excluded_total', greatest(v_eligible_total-v_selected_total,0),
            'expanded_total', 0,
            'queued_total', 0,
            'generated_total',0,
            'regenerated_total',0,
            'issued_total',0,
            'issued_send_blocked_total',0,
            'already_active_total',0,
            'blocked_total', 0,
            'changed_total', 0,
            'missing_total',0,
            'failed_total', 0,
            'in_progress_total',0,
            'delivery_pending_total',0,
            'delivery_complete_total',0,
            'delivery_blocked_total',0,
            'total_units', 1,
            'completed_units', 0,
            'failed_units', 0
          ),
          1,
          0,
          0,
          1,
          1,
          nextval('public.invoice_operation_change_seq'),
          1,
          false,
          false,
          0,
          v_now,
          v_now
        )
        RETURNING id, status, phase, change_seq
          INTO v_operation_id, v_operation_status, v_operation_phase, v_change_seq;

        v_created := true;
        v_reused := false;
      END IF;

      IF v_operation_status IS DISTINCT FROM 'COMPLETE' THEN
        INSERT INTO public.invoice_operation_chunks(
          operation_id,
          chunk_type,
          phase,
          sequence_no,
          level_no,
          work_key,
          entity_type,
          entity_id,
          status,
          priority,
          run_after_utc,
          payload_json,
          progress_json,
          operation_control_version,
          manifest_generation,
          is_manifest_member,
          manifest_committed,
          result_visible,
          created_at_utc,
          updated_at_utc
        ) VALUES (
          v_operation_id,
          v_chunk_type,
          'BUILD_MANIFEST',
          0,
          0,
          private._invoice_batch_hash_v2(jsonb_build_object(
            'work','BUILD_MANIFEST',
            'root',v_operation_id,
            'manifest_generation',1,
            'action',v_action
          )),
          'OPERATION',
          v_operation_id,
          'QUEUED',
          v_priority,
          v_now,
          jsonb_build_object(
            'is_selection_expander', true,
            'selection_key', NULL,
            'action', v_action,
            'filter_hash', v_filter_hash,
            'query_hash',v_query_hash,
            'selection_hash', v_selection_hash,
            'manifest_generation',1,
            'manifest_committed',false,
            'selection_contract', jsonb_build_object(
              'contract_version', 'INVOICE_BATCH_SELECTION_ROOT_V2',
              'action', v_action,
              'query', v_normalised_query,
              'selection', v_selection
            ),
            'query', v_normalised_query,
            'cursor', jsonb_build_object(),
            'deliver', v_deliver,
            'delivery_request_token', v_delivery_request_token,
            'delivery_intent', v_delivery_intent,
            'scanned', 0,
            'selected', 0,
            'queued', 0,
            'blocked', 0,
            'changed', 0,
            'already_active', 0,
            'completed', false,
            'release_cursor',null
          ),
          jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message', 'Building selection manifest'
          ),
          1,
          1,
          false,
          false,
          false,
          v_now,
          v_now
        )
        ON CONFLICT DO NOTHING;

        UPDATE public.invoice_operations o
        SET total_units = greatest(o.total_units, 1),
            chunk_count = greatest(o.chunk_count, 1),
            progress_json = coalesce(o.progress_json, '{}'::jsonb)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_PROGRESS_V2',
                'status_message', 'Building selection manifest',
                'selection_expansion_pending', true,
                'manifest_committed',false,
                'total_units', greatest(o.total_units, 1)
              ),
            updated_at_utc = v_now,
            change_seq = CASE
              WHEN v_created THEN o.change_seq
              ELSE nextval('public.invoice_operation_change_seq')
            END
        WHERE o.id = v_operation_id
          AND o.status <> 'COMPLETE'
        RETURNING o.status, o.phase, o.change_seq
          INTO v_operation_status, v_operation_phase, v_change_seq;

        IF NOT FOUND THEN
          SELECT o.status, o.phase, o.change_seq
            INTO v_operation_status, v_operation_phase, v_change_seq
          FROM public.invoice_operations o
          WHERE o.id = v_operation_id;
        END IF;
      END IF;

      v_result_item := jsonb_build_object(
        'command_no', v_command_no,
        'command_type', v_command_type,
        'accepted', true,
        'operation_id', v_operation_id,
        'operation_type', v_operation_type,
        'status', v_operation_status,
        'phase', v_operation_phase,
        'source_revision', v_filter_hash,
        'change_seq', v_change_seq,
        'created', v_created,
        'reused_active', v_reused AND v_operation_status <> 'COMPLETE',
        'reused_ready', v_reused AND v_operation_status = 'COMPLETE',
        'priority_raised', false,
        'blocked', v_operation_status = 'BLOCKED',
        'terminal_error', NULL,
        'chunk_count', 1,
        'selection_expansion_pending', v_operation_status IS DISTINCT FROM 'COMPLETE',
        'selection_contract_version', 'INVOICE_BATCH_SELECTION_V2',
        'estimated_filtered_total', v_filtered_total,
        'estimated_selected_total',v_selected_total,
        'nudge_state', CASE WHEN v_operation_status = 'COMPLETE' THEN 'REUSED_COMPLETE' ELSE 'DB_QUEUE' END
      );
    END IF;
    END IF;

    IF v_error_code IS NOT NULL THEN
      v_result_item := jsonb_build_object(
        'command_no', v_command_no,
        'command_type', v_command_type,
        'accepted', false,
        'created', false,
        'reused_active', false,
        'reused_ready', false,
        'priority_raised', false,
        'blocked', false,
        'selection_expansion_pending', false,
        'terminal_error', jsonb_build_object('code', v_error_code, 'detail', v_error_detail),
        'error', jsonb_build_object('code', v_error_code, 'detail', v_error_detail)
      );
    END IF;

    v_results := v_results || jsonb_build_array(v_result_item);
  END LOOP;

  RETURN v_results;
END;
$function$;

-- invoice_outbox_enqueue_by_week_selected(jsonb,uuid,boolean,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week_selected(p_rows jsonb, p_actor_user_id uuid, p_allow_early boolean DEFAULT false, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(client_id uuid, invoice_week_start date, outbox_id uuid, action text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
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
$function$;

-- invoice_outbox_enqueue_by_week(uuid,date,uuid,boolean,jsonb,uuid[],boolean)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week(p_client_id uuid, p_invoice_week_start date, p_actor_user_id uuid, p_allow_early boolean DEFAULT false, p_meta jsonb DEFAULT '{}'::jsonb, p_timesheet_ids uuid[] DEFAULT NULL::uuid[], p_auto_invoice_only boolean DEFAULT false)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
$function$;

-- invoice_outbox_enqueue_by_week(uuid,date,uuid,boolean,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week(p_client_id uuid, p_invoice_week_start date, p_actor_user_id uuid, p_allow_early boolean DEFAULT false, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
  v_correction_scope_ids uuid[] := array[]::uuid[];

  v_london_today date := (now() at time zone 'Europe/London')::date;
  v_week_end date := (p_invoice_week_start + interval '6 days')::date;

  v_has_due boolean := false;

  -- ======================================================
  -- DEBUG (optional): single audit row per RPC call
  -- ======================================================
  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_nonseg_due_count int := null;
  v_dbg_seg_due_count int := null;
  v_dbg_nonseg_due_sample jsonb := '[]'::jsonb;
  v_dbg_seg_due_sample jsonb := '[]'::jsonb;
  v_dbg_existing_outbox_id uuid := null;
  v_dbg_new_outbox_id uuid := null;

  -- extra breakdown (why NOT due)
  v_dbg_nonseg_any_count int := null;
  v_dbg_nonseg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_any_count int := null;
  v_dbg_seg_fail_sample jsonb := '[]'::jsonb;
  v_dbg_seg_delayed_not_due_count int := null;

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

  v_correction_scope_ids:=public._ctms_invoice_week_candidate_ids_v1(
    p_client_id,p_invoice_week_start,100
  );
  if cardinality(v_correction_scope_ids)>0 then
    v_correction_scope_ids:=public._ctms_expand_correction_member_ids_v1(v_correction_scope_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_correction_scope_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK'
    );
  end if;

  -- ------------------------------------------------------------
  -- ✅ Due/invoiceable existence check (prevents preview/enqueue mismatch)
  -- Implements the confirmed rules:
  --   - allow_early applies to SEGMENTS + NON-SEGMENTS week-ending gate
  --   - allow_early does NOT override delayed segments
  --   - delayed segments eligible only once delay date reached (target week start <= today)
  --   - SEGMENTS-empty (expense-only) is invoiceable when ANY expense/mileage/additional-charge evidence exists,
  --     even if total_charge_ex_vat is accidentally 0 (defensive fallback).
  --
  -- ✅ HR validation gating:
  --   If hr_validation_required_for_invoice is true, validation_status must be VALIDATION_OK or OVERRIDDEN.
  --   NULL validation_status is treated as blocked when required.
  -- ------------------------------------------------------------
  select exists (
    -- NON-SEGMENTS (or SEGMENTS-empty treated as NON-SEGMENTS): invoice week is natural week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    left join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

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

    -- SEGMENTS mode: segment-level eligibility for this invoice week
    select 1
    from public.timesheets_financials tf
    join public.timesheets ts
      on ts.timesheet_id = tf.timesheet_id
     and ts.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    left join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
    where tf.is_current = true
      and tf.client_id = p_client_id
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'

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

      -- segment belongs to this invoice_week_start (target week, else natural week)
      and coalesce(
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
            (ts.week_ending_date::date - 6)
          ) = p_invoice_week_start

      and (
        -- DELAYED segment:
        -- invoice_target_week_start differs from natural week start
        -- eligibility depends ONLY on delay reaching (<= today), NOT allow_early
        (
          nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is not null
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
          and nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date <= v_london_today
        )
        or
        -- NON-DELAYED segment:
        -- (target is null OR equals natural week start)
        -- eligibility uses timesheet week-ending gate with allow_early
        (
          (
            nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '') is null
            or nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
          )
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
        )
      )
    limit 1
  ) into v_has_due;

  -- DEBUG: capture due breakdown (no effect unless enabled)
  if v_invoice_debug then
    begin
      -- NON-SEGMENTS (or segments mode with empty segments array but invoiceable via totals/expenses/mileage/additional)
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
          tf.processing_status::text as processing_status,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          coalesce(tf.total_charge_ex_vat,0)::numeric as total_charge_ex_vat,
          coalesce(tf.expenses_charge_ex_vat,0)::numeric as expenses_charge_ex_vat,
          coalesce(tf.travel_charge_ex_vat,0)::numeric as travel_charge_ex_vat,
          coalesce(tf.accommodation_charge_ex_vat,0)::numeric as accommodation_charge_ex_vat,
          coalesce(tf.other_charge_ex_vat,0)::numeric as other_charge_ex_vat,
          coalesce(tf.mileage_charge_ex_vat,0)::numeric as mileage_charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
            else 0::numeric
          end as additional_charge_ex_vat,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'

          -- ✅ HR validation gate (mirror v_has_due)
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )

          and (ts.week_ending_date::date - 6) = p_invoice_week_start
          and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
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
      ) s;

      -- SEGMENTS mode (segment-level eligibility)
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
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'

          -- ✅ HR validation gate (mirror v_has_due)
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )

          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
          and (
            (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <= v_london_today
            )
            or
            (
              (
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
                or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6)
              )
              and (p_allow_early = true or ts.week_ending_date::date < v_london_today)
            )
          )
      ) s;

      -- Breakdown: candidates for this client/week that are NOT due (helps explain why v_has_due=false)
      -- NON-SEGMENTS candidates (natural week start = invoice_week_start)
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb)
      into
        v_dbg_nonseg_any_count,
        v_dbg_nonseg_fail_sample
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          tf.processing_status::text as processing_status,
          (tf.locked_by_invoice_id is not null) as locked_by_invoice,
          (ts.revoked_at is not null) as revoked,
          pc.precheck_status as precheck_status,
          coalesce(tf.invoice_breakdown_json->>'mode','') as invoice_mode,
          jsonb_typeof(tf.invoice_breakdown_json->'segments') as segments_type,
          coalesce(tf.total_charge_ex_vat,0)::numeric as total_charge_ex_vat,
          coalesce(tf.expenses_charge_ex_vat,0)::numeric as expenses_charge_ex_vat,
          coalesce(tf.travel_charge_ex_vat,0)::numeric as travel_charge_ex_vat,
          coalesce(tf.accommodation_charge_ex_vat,0)::numeric as accommodation_charge_ex_vat,
          coalesce(tf.other_charge_ex_vat,0)::numeric as other_charge_ex_vat,
          coalesce(tf.mileage_charge_ex_vat,0)::numeric as mileage_charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
            when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
            else 0::numeric
          end as additional_charge_ex_vat,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when (
              coalesce(vts.hr_validation_required_for_invoice, false)
              and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
              and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
            ) then 'HR_VALIDATION_BLOCKED'
            when (p_allow_early is not true) and (ts.week_ending_date::date >= v_london_today) then 'WEEK_NOT_PASSED'
            when (
              (coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
               and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
               and jsonb_array_length(tf.invoice_breakdown_json->'segments') = 0)
              and (
                   coalesce(tf.total_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.expenses_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.travel_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.accommodation_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.other_charge_ex_vat,0)::numeric = 0
               and coalesce(tf.mileage_charge_ex_vat,0)::numeric = 0
               and (
                 case
                   when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') is null then 0::numeric
                   when nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                     then (nullif(btrim(coalesce(tf.invoice_breakdown_json#>>'{additional,charge_ex_vat}','')), '')::numeric)
                   else 0::numeric
                 end
               ) = 0
              )
            ) then 'SEGMENTS_EMPTY_AND_ZERO_TOTAL'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = p_client_id
          and (ts.week_ending_date::date - 6) = p_invoice_week_start
      ) s;

      -- SEGMENTS candidates for this invoice_week_start (segment-level), including delayed-not-due reasons
      select
        count(*)::int,
        coalesce(jsonb_agg(s) filter (where s.rn <= 25), '[]'::jsonb),
        count(*) filter (where s.fail_reason = 'DELAYED_NOT_DUE')::int
      into
        v_dbg_seg_any_count,
        v_dbg_seg_fail_sample,
        v_dbg_seg_delayed_not_due_count
      from (
        select
          tf.timesheet_id::text as timesheet_id,
          ts.week_ending_date::date as week_ending_date,
          nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') as invoice_target_week_start,
          nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
          coalesce(seg_el.value->>'segment_type','') as segment_type,
          coalesce(seg_el.value->>'label','') as label,
          coalesce(seg_el.value->>'charge_ex_vat','') as charge_ex_vat,
          coalesce(vts.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice,
          vts.validation_status::text as validation_status,
          case
            when tf.processing_status <> 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then 'NOT_READY_FOR_INVOICE'
            when tf.locked_by_invoice_id is not null then 'LOCKED_BY_INVOICE'
            when ts.revoked_at is not null then 'REVOKED'
            when upper(coalesce(pc.precheck_status,'')) <> 'OK' then 'PRECHECK_NOT_OK'
            when (
              coalesce(vts.hr_validation_required_for_invoice, false)
              and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
              and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
            ) then 'HR_VALIDATION_BLOCKED'
            when nullif(btrim(coalesce(seg_el.value->>'invoice_locked_invoice_id','')), '') is not null then 'SEGMENT_LOCKED'
            when (
              nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is not null
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date <> (ts.week_ending_date::date - 6)
              and nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date > v_london_today
            ) then 'DELAYED_NOT_DUE'
            when (
              (nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '') is null
               or nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date = (ts.week_ending_date::date - 6))
              and (p_allow_early is not true)
              and (ts.week_ending_date::date >= v_london_today)
            ) then 'WEEK_NOT_PASSED'
            else 'OTHER'
          end as fail_reason,
          row_number() over (order by ts.updated_at desc nulls last) as rn
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg_el(value)
        where tf.is_current = true
          and tf.client_id = p_client_id
          and coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
          and jsonb_typeof(seg_el.value) = 'object'
          and nullif(btrim(coalesce(seg_el.value->>'segment_id','')), '') is not null
          and coalesce(
                nullif(btrim(coalesce(seg_el.value->>'invoice_target_week_start','')), '')::date,
                (ts.week_ending_date::date - 6)
              ) = p_invoice_week_start
      ) s;
    exception when others then
      null;
    end;
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
              'has_due', v_has_due,
              'nonseg_due_count', v_dbg_nonseg_due_count,
              'seg_due_count', v_dbg_seg_due_count,
              'nonseg_due_sample', v_dbg_nonseg_due_sample,
              'seg_due_sample', v_dbg_seg_due_sample,
              'nonseg_any_count', v_dbg_nonseg_any_count,
              'seg_any_count', v_dbg_seg_any_count,
              'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
              'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
              'seg_fail_sample', v_dbg_seg_fail_sample,
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
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'nonseg_due_sample', v_dbg_nonseg_due_sample,
            'seg_due_sample', v_dbg_seg_due_sample,
            'nonseg_any_count', v_dbg_nonseg_any_count,
            'seg_any_count', v_dbg_seg_any_count,
            'seg_delayed_not_due_count', v_dbg_seg_delayed_not_due_count,
            'nonseg_fail_sample', v_dbg_nonseg_fail_sample,
            'seg_fail_sample', v_dbg_seg_fail_sample,
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
    raise exception 'No invoiceable timesheets/segments for client=% and invoice_week_start=%', p_client_id, p_invoice_week_start;
  end if;

  -- Build payload
  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text,
    'allow_early', coalesce(p_allow_early, false)
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
  -- Prevents duplicate BY_WEEK outbox rows from concurrent check+insert races.
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
    -- Merge allow_early/actor/meta into existing payload so subsequent calls are consistent
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
            'has_due', v_has_due,
            'nonseg_due_count', v_dbg_nonseg_due_count,
            'seg_due_count', v_dbg_seg_due_count,
            'existing_outbox_id', v_existing::text,
            'payload_merge', v_payload
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
          'has_due', v_has_due,
          'nonseg_due_count', v_dbg_nonseg_due_count,
          'seg_due_count', v_dbg_seg_due_count,
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
$function$;

-- invoice_outbox_enqueue_by_week(uuid,date,uuid,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_by_week(p_client_id uuid, p_invoice_week_start date, p_actor_user_id uuid, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_payload jsonb;
  v_existing uuid;
  v_new uuid;
  v_correction_scope_ids uuid[] := array[]::uuid[];
begin
  if p_client_id is null then
    raise exception 'client_id is required';
  end if;

  if p_invoice_week_start is null then
    raise exception 'invoice_week_start is required';
  end if;

  v_correction_scope_ids:=public._ctms_invoice_week_candidate_ids_v1(
    p_client_id,p_invoice_week_start,100
  );
  if cardinality(v_correction_scope_ids)>0 then
    v_correction_scope_ids:=public._ctms_expand_correction_member_ids_v1(v_correction_scope_ids,100);
    perform public._ctms_assert_correction_invoice_scope_v1(
      v_correction_scope_ids,null::uuid,p_actor_user_id,true,false,false,'INVOICE_OUTBOX_BY_WEEK'
    );
  end if;

  v_payload := jsonb_build_object(
    'client_id', p_client_id::text,
    'invoice_week_start', p_invoice_week_start::text
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
  where o.kind = 'BY_WEEK'
    and (o.payload->>'client_id') = p_client_id::text
    and (o.payload->>'invoice_week_start') = p_invoice_week_start::text
  order by o.created_at desc
  limit 1;

  if v_existing is not null then
    return v_existing;
  end if;

  insert into public.invoice_jobs_outbox(kind, payload)
  values ('BY_WEEK'::text, v_payload)
  returning id into v_new;

  return v_new;
end;
$function$;

-- invoice_outbox_enqueue_hours(uuid[],uuid,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue_hours(p_timesheet_ids uuid[], p_actor_user_id uuid, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
$function$;

-- invoice_outbox_enqueue(text,jsonb,uuid,jsonb)
CREATE OR REPLACE FUNCTION public.invoice_outbox_enqueue(p_kind text, p_payload jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_meta jsonb DEFAULT NULL::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
$function$;

-- invoice_quicksearch_ids(text,integer)
CREATE OR REPLACE FUNCTION public.invoice_quicksearch_ids(p_q text, p_limit integer DEFAULT 20000)
 RETURNS TABLE(invoice_id uuid)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select s.invoice_id
  from (
    select distinct on (i.id)
      i.id as invoice_id,
      coalesce(i.issued_at_utc, i.created_at) as sort_ts,
      i.invoice_no as sort_no
    from public.invoices as i
    left join public.clients as c
      on c.id = i.client_id
    left join public.invoice_lines as il
      on il.invoice_id = i.id
    left join public.v_timesheets_summary as vts
      on vts.timesheet_id = il.timesheet_id
    where
      p_q is not null
      and btrim(p_q) <> ''
      and (
        i.invoice_no ilike ('%' || p_q || '%')
        or c.name ilike ('%' || p_q || '%')
        or vts.candidate_name ilike ('%' || p_q || '%')
      )
    -- DISTINCT ON requires i.id first in ORDER BY; after that we pick the “best” row per invoice
    order by
      i.id,
      coalesce(i.issued_at_utc, i.created_at) desc nulls last,
      i.invoice_no desc
  ) as s
  order by
    s.sort_ts desc nulls last,
    s.sort_no desc
  limit greatest(1, least(coalesce(p_limit, 20000), 20000));
$function$;

-- invoice_recompute_totals(uuid)
CREATE OR REPLACE FUNCTION public.invoice_recompute_totals(p_invoice_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_subtotal_ex_vat numeric;
  v_vat_amount numeric;
  v_total_inc_vat numeric;

  v_type_text text;
  v_type_norm text;
begin
  -- Lock the invoice row to avoid concurrent totals races.
  perform 1
  from public.invoices i
  where i.id = p_invoice_id
  for update;

  if not found then
    raise exception 'invoice_recompute_totals: invoice % not found', p_invoice_id;
  end if;

  -- Read invoice type (row already locked above).
  select i.type::text
  into v_type_text
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  v_type_norm := upper(coalesce(v_type_text, ''));
  v_type_norm := replace(v_type_norm, ' ', '_');
  v_type_norm := replace(v_type_norm, '-', '_');

  -- Sum line totals (as stored on invoice_lines).
  select
    coalesce(sum(l.total_charge_ex_vat), 0)::numeric,
    coalesce(sum(l.vat_amount), 0)::numeric,
    coalesce(sum(l.total_inc_vat), 0)::numeric
  into
    v_subtotal_ex_vat,
    v_vat_amount,
    v_total_inc_vat
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  v_subtotal_ex_vat := round(v_subtotal_ex_vat, 2);
  v_vat_amount := round(v_vat_amount, 2);
  v_total_inc_vat := round(v_total_inc_vat, 2);

  -- OPTION A: enforce signed totals for CREDIT_NOTE at the canonical source-of-truth.
  -- This is intentionally conservative:
  -- - For CREDIT_NOTE: force negative (or zero) values, regardless of line sign.
  -- - For INVOICE: leave computed totals as-is (do NOT abs), so discounts/negative lines still work normally.
  if v_type_norm in ('CREDIT_NOTE', 'CREDITNOTE') then
    v_subtotal_ex_vat := -abs(coalesce(v_subtotal_ex_vat, 0));
    v_vat_amount := -abs(coalesce(v_vat_amount, 0));
    v_total_inc_vat := -abs(coalesce(v_total_inc_vat, 0));

    -- Re-round after abs/sign enforcement (keeps consistent 2dp storage)
    v_subtotal_ex_vat := round(v_subtotal_ex_vat, 2);
    v_vat_amount := round(v_vat_amount, 2);
    v_total_inc_vat := round(v_total_inc_vat, 2);
  end if;

  update public.invoices
  set
    subtotal_ex_vat = v_subtotal_ex_vat,
    vat_amount = v_vat_amount,
    total_inc_vat = v_total_inc_vat,
    invoice_pdf_r2_key = null,
    updated_at = now()
  where id = p_invoice_id;

  return jsonb_build_object(
    'ok', true,
    'invoice_id', p_invoice_id,
    'subtotal_ex_vat', v_subtotal_ex_vat,
    'vat_amount', v_vat_amount,
    'total_inc_vat', v_total_inc_vat,
    'invoice_pdf_r2_key_cleared', true
  );
end;
$function$;

-- invoice_reference_rows(uuid)
CREATE OR REPLACE FUNCTION public.invoice_reference_rows(p_invoice_id uuid)
 RETURNS TABLE(timesheet_id uuid, sheet_scope text, submission_mode text, ref_target text, segment_id text, day_ymd text, start_utc text, end_utc text, current_reference text, is_required boolean, row_key text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (v_now at time zone 'Europe/London')::date;
  v_invoice_debug boolean := false;

  v_ts_ids uuid[] := array[]::uuid[];

  v_rows_out int := 0;
  v_ts_out int := 0;

  v_dbg_steps jsonb := '[]'::jsonb;

  r_ts record;
  r_seg record;
  r_day record;

  v_tf_mode text;
  v_segments_json jsonb;
  v_sched_json jsonb;
  v_dayrefs_json jsonb;

  v_seg_locked text;

  v_idx int;
  v_start_local text;
  v_end_local text;

  -- invoice-level worked content (HOURS + ADDITIONAL only) for this timesheet on THIS invoice
  v_inv_worked_charge_ex numeric := 0;
  v_inv_worked_hours_sum numeric := 0;

  -- required flag for UI display: true if refs required for either invoice or issue
  v_is_required boolean := false;

  -- per-segment positivity test
  v_seg_hours_sum numeric := 0;
  v_seg_charge_ex numeric := 0;

  -- SEGMENTS-mode: SEGMENT refs are sourced from TSFIN segments (invoice_breakdown_json)
  v_seg_id_local text;
  v_sched_ref text;
  v_sched_found boolean;

begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load invoice_debug flag safely
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  select array_agg(distinct il.timesheet_id)
    into v_ts_ids
  from public.invoice_lines il
  where il.invoice_id = p_invoice_id
    and il.timesheet_id is not null;

  if v_ts_ids is null or coalesce(array_length(v_ts_ids, 1), 0) = 0 then
    return;
  end if;

  v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object(
    'step', 'load_timesheets',
    'timesheet_count', coalesce(array_length(v_ts_ids,1),0)
  ));

  for r_ts in
    select
      ts.timesheet_id as ts_id,
      ts.sheet_scope as ts_sheet_scope,
      ts.submission_mode as ts_submission_mode,
      ts.reference_number as ts_reference_number,
      ts.week_ending_date as ts_week_ending_date,
      ts.worked_start_iso as ts_worked_start_iso,
      ts.worked_end_iso as ts_worked_end_iso,
      ts.scheduled_start_iso as ts_scheduled_start_iso,
      ts.scheduled_end_iso as ts_scheduled_end_iso,
      ts.actual_schedule_json as ts_actual_schedule_json,
      ts.day_references_json as ts_day_references_json,
      tf.invoice_breakdown_json as tf_invoice_breakdown_json,
      pc.require_reference_to_invoice as pc_require_reference_to_invoice,
      pc.reference_number_required_to_issue_invoice as pc_ref_to_issue
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = ts.timesheet_id
    where ts.timesheet_id = any(v_ts_ids)
    order by ts.week_ending_date asc nulls last, ts.timesheet_id
  loop
    v_ts_out := v_ts_out + 1;

    -- Two independent policies: required-to-invoice and required-to-issue.
    -- For UI display we mark required if either is true.
    v_is_required := coalesce(r_ts.pc_require_reference_to_invoice,false)
                     or coalesce(r_ts.pc_ref_to_issue,false);

    -- Invoice-level worked content on THIS invoice for this timesheet:
    -- Only HOURS_* and ADDITIONAL_RATE* lines are considered "worked content".
    -- If net worked content is <= 0 (hours AND charge), no refs are required and no rows should be returned.
    select
      coalesce(sum(
        case
          when upper(coalesce(il.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY','ADDITIONAL_RATE','ADDITIONAL_RATE_DAILY')
            then coalesce(il.total_charge_ex_vat,0)
          else 0
        end
      ),0),
      coalesce(sum(
        case
          when upper(coalesce(il.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY')
            then coalesce(il.hours_day,0)+coalesce(il.hours_night,0)+coalesce(il.hours_sat,0)+coalesce(il.hours_sun,0)+coalesce(il.hours_bh,0)
          else 0
        end
      ),0)
    into v_inv_worked_charge_ex, v_inv_worked_hours_sum
    from public.invoice_lines il
    where il.invoice_id = p_invoice_id
      and il.timesheet_id = r_ts.ts_id;

    if coalesce(v_inv_worked_charge_ex,0) <= 0
       and coalesce(v_inv_worked_hours_sum,0) <= 0 then
      continue;
    end if;

    v_tf_mode := upper(coalesce(r_ts.tf_invoice_breakdown_json->>'mode',''));
    v_segments_json := r_ts.tf_invoice_breakdown_json->'segments';
    v_sched_json := r_ts.ts_actual_schedule_json;
    v_dayrefs_json := r_ts.ts_day_references_json;

    -- A) SEGMENTS mode: per-shift rows, only locked to this invoice and net-positive.
    if v_tf_mode = 'SEGMENTS' and jsonb_typeof(v_segments_json) = 'array' then
      for r_seg in
        select value as seg
        from jsonb_array_elements(v_segments_json) value
        order by
          coalesce(value->>'start_utc',''),
          coalesce(value->>'segment_id','')
      loop
        v_seg_locked := nullif(btrim(coalesce(r_seg.seg->>'invoice_locked_invoice_id','')), '');
        if v_seg_locked is null or v_seg_locked <> p_invoice_id::text then
          continue;
        end if;

        begin
          v_seg_hours_sum :=
            coalesce(nullif(r_seg.seg->>'hours_day','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_night','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_sat','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_sun','')::numeric,0)
          + coalesce(nullif(r_seg.seg->>'hours_bh','')::numeric,0);
        exception when others then
          v_seg_hours_sum := 0;
        end;

        begin
          v_seg_charge_ex := coalesce(nullif(r_seg.seg->>'charge_amount','')::numeric,0);
        exception when others then
          v_seg_charge_ex := 0;
        end;

        if coalesce(v_seg_hours_sum,0) <= 0 and coalesce(v_seg_charge_ex,0) <= 0 then
          continue;
        end if;

        v_seg_id_local := nullif(btrim(coalesce(r_seg.seg->>'segment_id','')), '');

        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'SEGMENT';

        day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
        start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
        end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');

        -- Fallback: derive day_ymd from start_utc (Europe/London) if missing
        if day_ymd is null and start_utc is not null then
          begin
            day_ymd := (((start_utc::timestamptz) at time zone 'Europe/London')::date)::text;
          exception when others then
            day_ymd := null;
          end;
        end if;

        -- Fallback: if segment_id missing, produce stable identifier from start/end (used for UI row identity).
        segment_id := v_seg_id_local;
        if segment_id is null and start_utc is not null and end_utc is not null then
          segment_id := 'SE:' || start_utc || '|' || end_utc;
        end if;

        -- SEGMENTS mode: current_reference is sourced from TSFIN segments (invoice_breakdown_json),
        -- which invoice_apply_edits keeps in sync when refs are edited.
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- B) WEEKLY MANUAL: schedule entries (start/end present)
    elsif r_ts.ts_sheet_scope::text = 'WEEKLY'
      and r_ts.ts_submission_mode::text = 'MANUAL'
      and jsonb_typeof(v_sched_json) = 'array'
    then
      for r_seg in
        select value as seg, ordinality as idx
        from jsonb_array_elements(v_sched_json) with ordinality
      loop
        v_start_local := nullif(btrim(coalesce(r_seg.seg->>'start','')), '');
        v_end_local   := nullif(btrim(coalesce(r_seg.seg->>'end','')), '');
        if v_start_local is null or v_end_local is null then
          continue;
        end if;

        v_idx := (r_seg.idx - 1);

        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'SEGMENT';
        segment_id := ('ts:' || r_ts.ts_id::text || ':' || v_idx::text);
        day_ymd := nullif(btrim(coalesce(r_seg.seg->>'date','')), '');
        start_utc := nullif(btrim(coalesce(r_seg.seg->>'start_utc','')), '');
        end_utc := nullif(btrim(coalesce(r_seg.seg->>'end_utc','')), '');
        current_reference := nullif(btrim(coalesce(r_seg.seg->>'ref_num','')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- C) WEEKLY non-MANUAL: per-key FREEFORM rows from day_references_json (excluding internal __ keys)
    elsif r_ts.ts_sheet_scope::text = 'WEEKLY'
      and r_ts.ts_submission_mode::text <> 'MANUAL'
      and jsonb_typeof(v_dayrefs_json) = 'object'
    then
      for r_day in
        select j.key as k, j.value as v
        from jsonb_each_text(v_dayrefs_json) as j(key, value)
        where left(coalesce(j.key,''), 2) <> '__'
        order by j.key
      loop
        timesheet_id := r_ts.ts_id;
        sheet_scope := r_ts.ts_sheet_scope::text;
        submission_mode := r_ts.ts_submission_mode::text;
        ref_target := 'FREEFORM';
        segment_id := null;
        day_ymd := nullif(btrim(coalesce(r_day.k,'')), '');
        start_utc := null;
        end_utc := null;
        current_reference := nullif(btrim(coalesce(r_day.v,'')), '');
        is_required := v_is_required;

        row_key := r_ts.ts_id::text
          || '|' || coalesce(ref_target,'')
          || '|' || coalesce(segment_id,'')
          || '|' || coalesce(day_ymd,'')
          || '|' || coalesce(start_utc,'')
          || '|' || coalesce(end_utc,'');

        v_rows_out := v_rows_out + 1;
        return next;
      end loop;

    -- D) DAILY or any other fallback: single timesheet-level ref
    else
      timesheet_id := r_ts.ts_id;
      sheet_scope := r_ts.ts_sheet_scope::text;
      submission_mode := r_ts.ts_submission_mode::text;
      ref_target := 'TIMESHEET';
      segment_id := null;
      day_ymd := null;

      if r_ts.ts_worked_start_iso is not null then
        day_ymd := ((r_ts.ts_worked_start_iso at time zone 'Europe/London')::date)::text;
      elsif r_ts.ts_scheduled_start_iso is not null then
        day_ymd := ((r_ts.ts_scheduled_start_iso at time zone 'Europe/London')::date)::text;
      elsif r_ts.ts_week_ending_date is not null then
        day_ymd := r_ts.ts_week_ending_date::text;
      end if;

      -- ✅ FIX: start_utc/end_utc use JSON timestamptz rendering (stable ISO) instead of timestamptz::text
      start_utc := coalesce(
        (to_jsonb(r_ts.ts_worked_start_iso)#>>'{}'),
        (to_jsonb(r_ts.ts_scheduled_start_iso)#>>'{}')
      );
      end_utc := coalesce(
        (to_jsonb(r_ts.ts_worked_end_iso)#>>'{}'),
        (to_jsonb(r_ts.ts_scheduled_end_iso)#>>'{}')
      );
      current_reference := nullif(btrim(coalesce(r_ts.ts_reference_number,'')), '');
      is_required := v_is_required;

      row_key := r_ts.ts_id::text
        || '|' || coalesce(ref_target,'')
        || '|' || coalesce(segment_id,'')
        || '|' || coalesce(day_ymd,'')
        || '|' || coalesce(start_utc,'')
        || '|' || coalesce(end_utc,'');

      v_rows_out := v_rows_out + 1;
      return next;
    end if;
  end loop;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_REFERENCE_ROWS_DEBUG',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'anchor_ymd', v_anchor_ymd::text,
        'timesheet_count', coalesce(array_length(v_ts_ids,1),0),
        'timesheets_scanned', v_ts_out,
        'rows_returned', v_rows_out,
        'steps', v_dbg_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

exception when others then
  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_REFERENCE_ROWS_ERROR',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'anchor_ymd', v_anchor_ymd::text,
        'sqlstate', sqlstate,
        'error', sqlerrm,
        'steps', v_dbg_steps
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;
  raise;
end;
$function$;

-- invoice_remove_nhsp_shifts(uuid,uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_remove_nhsp_shifts(p_invoice_id uuid, p_shift_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(invoice_id uuid, subtotal_ex_vat numeric, vat_amount numeric, total_inc_vat numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_shift_ids uuid[];

  -- prev/new totals
  v_prev_ex numeric := 0;
  v_prev_vat numeric := 0;
  v_prev_inc numeric := 0;
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  v_delta_ex numeric := 0;
  v_delta_vat numeric := 0;
  v_delta_inc numeric := 0;

  v_invoice_no text := null;
  v_prev_status text := null;
  v_new_status text := null;

  -- removed-lines detail
  v_removed_ts_ids uuid[] := null;
  v_removed_source_keys text[] := null;
  v_removed_line_count int := 0;
  v_removed_ex numeric := 0;
  v_removed_vat numeric := 0;
  v_removed_inc numeric := 0;

  v_pdf_jobs_enqueued int := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  if p_shift_ids is null or coalesce(array_length(p_shift_ids,1),0) = 0 then
    raise exception 'shift_ids[] required';
  end if;

  v_shift_ids := (select array_agg(distinct x) from unnest(p_shift_ids) x where x is not null);

  perform public._ctms_assert_invoice_mutable_draft_v1(
    p_invoice_id,'INVOICE_REMOVE_NHSP_SHIFTS',true
  );
  if exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and (il.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (il.meta_json->>'nhsp_shift_id')::uuid=any(v_shift_ids)
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    raise exception 'IMPORT_CORRECTION_INVOICE_SHIFT_REMOVAL_FORBIDDEN'
      using errcode='P0001',detail=jsonb_build_object('invoice_id',p_invoice_id)::text;
  end if;

  -- Capture invoice BEFORE
  select
    i.invoice_no,
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_invoice_no,
    v_prev_status,
    v_prev_ex,
    v_prev_vat,
    v_prev_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Identify lines that will be removed (for audit detail)
  with to_remove as (
    select
      l.timesheet_id,
      l.source_key,
      coalesce(l.total_charge_ex_vat,0)::numeric as ex,
      coalesce(l.vat_amount,0)::numeric as vat,
      coalesce(l.total_inc_vat,0)::numeric as inc
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and (l.meta_json ? 'nhsp_shift_id')
      and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids)
  )
  select
    array_agg(distinct timesheet_id),
    array_agg(distinct source_key),
    count(*)::int,
    coalesce(sum(ex),0)::numeric,
    coalesce(sum(vat),0)::numeric,
    coalesce(sum(inc),0)::numeric
  into
    v_removed_ts_ids,
    v_removed_source_keys,
    v_removed_line_count,
    v_removed_ex,
    v_removed_vat,
    v_removed_inc
  from to_remove;

  -- 1) Unlink shifts (only those currently linked to this invoice)
  update public.nhsp_shifts s
  set invoice_status = 'PENDING',
      invoice_id = null,
      updated_at = v_now
  where s.id = any(v_shift_ids)
    and s.invoice_id = p_invoice_id;

  -- 2) Delete invoice lines referencing these shifts
  delete from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and (l.meta_json ? 'nhsp_shift_id')
    and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids);

  -- 3) Recompute totals from remaining lines (also clears invoice_pdf_r2_key)
  perform public.invoice_recompute_totals(p_invoice_id);

  -- 3.1) Invalidate cached render artifacts explicitly (policy: draft/on-hold must regen bundle/attachments)
  update public.invoices i0
  set
    invoice_pdf_generated_at_utc = null,
    invoice_render_manifest = null,
    paper_ts_r2_manifest = null,
    updated_at = v_now
  where i0.id = p_invoice_id;

  -- 3.2) Enqueue FORCE_REGEN invoice PDF bundle job (idempotent)
  v_pdf_jobs_enqueued := public.invpdf_enqueue_one(p_invoice_id, true);

  -- 4) Unlock TSFIN if a timesheet now has no remaining lines on this invoice
  update public.timesheets_financials tf
  set locked_by_invoice_id = null,
      updated_at = v_now
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null
    and not exists (
      select 1
      from public.invoice_lines l2
      where l2.invoice_id = p_invoice_id
        and l2.timesheet_id = tf.timesheet_id
    );

  -- Capture invoice AFTER + compute delta
  select
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_new_status,
    v_new_ex,
    v_new_vat,
    v_new_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  v_delta_ex  := public._inv_round2(v_new_ex  - v_prev_ex);
  v_delta_vat := public._inv_round2(v_new_vat - v_prev_vat);
  v_delta_inc := public._inv_round2(v_new_inc - v_prev_inc);

  -- Existing audit (kept), now with totals + timesheets + delta
  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'NHSP_INVOICE_SHIFT_REMOVED',
    jsonb_build_object(
      'invoice_no', v_invoice_no,
      'status', v_prev_status,
      'subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'vat_amount', public._inv_round2(v_prev_vat),
      'total_inc_vat', public._inv_round2(v_prev_inc)
    ),
    jsonb_build_object(
      'invoice_id', p_invoice_id::text,
      'invoice_no', v_invoice_no,
      'shift_ids', to_jsonb(v_shift_ids),

      'removed_line_count', coalesce(v_removed_line_count,0),
      'removed_timesheet_ids', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
      'removed_source_keys', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),

      'removed_subtotal_ex_vat', public._inv_round2(v_removed_ex),
      'removed_vat_amount', public._inv_round2(v_removed_vat),
      'removed_total_inc_vat', public._inv_round2(v_removed_inc),

      'invoice_status_before', v_prev_status,
      'invoice_status_after', v_new_status,

      'prev_subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'prev_vat_amount', public._inv_round2(v_prev_vat),
      'prev_total_inc_vat', public._inv_round2(v_prev_inc),

      'delta_subtotal_ex_vat', v_delta_ex,
      'delta_vat_amount', v_delta_vat,
      'delta_total_inc_vat', v_delta_inc,

      'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
      'new_vat_amount', public._inv_round2(v_new_vat),
      'new_total_inc_vat', public._inv_round2(v_new_inc),

      'invoice_pdf_force_regen_enqueued', v_pdf_jobs_enqueued,
      'run_at_utc', public._inv_iso_utc(v_now),
      'run_kind', 'REMOVE_NHSP_SHIFTS'
    ),
    null,
    p_actor_user_id
  );

  -- Generic “totals delta applied” audit (optional but recommended for unified reporting)
  if (coalesce(v_delta_ex,0) <> 0 or coalesce(v_delta_vat,0) <> 0 or coalesce(v_delta_inc,0) <> 0) then
    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_TOTALS_DELTA_APPLIED',
      jsonb_build_object(
        'invoice_no', v_invoice_no,
        'status', v_prev_status,
        'subtotal_ex_vat', public._inv_round2(v_prev_ex),
        'vat_amount', public._inv_round2(v_prev_vat),
        'total_inc_vat', public._inv_round2(v_prev_inc)
      ),
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'invoice_no', v_invoice_no,
        'run_at_utc', public._inv_iso_utc(v_now),
        'run_kind', 'REMOVE_NHSP_SHIFTS',

        'invoice_status_before', v_prev_status,
        'invoice_status_after', v_new_status,

        'delta_subtotal_ex_vat', v_delta_ex,
        'delta_vat_amount', v_delta_vat,
        'delta_total_inc_vat', v_delta_inc,

        'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
        'new_vat_amount', public._inv_round2(v_new_vat),
        'new_total_inc_vat', public._inv_round2(v_new_inc),

        'timesheet_ids_this_run', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
        'source_keys_this_run', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),
        'line_count_this_run', coalesce(v_removed_line_count,0),

        'invoice_pdf_force_regen_enqueued', v_pdf_jobs_enqueued
      ),
      null,
      p_actor_user_id
    );
  end if;

  -- Return updated invoice totals
  select u.id, u.subtotal_ex_vat, u.vat_amount, u.total_inc_vat
  into invoice_id, subtotal_ex_vat, vat_amount, total_inc_vat
  from public.invoices u
  where u.id = p_invoice_id;

  return next;
end;
$function$;

-- invoice_render_manifest(uuid)
CREATE OR REPLACE FUNCTION public.invoice_render_manifest(p_invoice_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_debug boolean := false;
  v_dbg_started timestamptz := now();
  v_manifest jsonb := null;

  v_lines_count int := 0;
  v_timesheet_ids_count int := 0;
  v_evidence_count int := 0;
  v_ts_evidence_count int := 0;
  v_ev_other_count int := 0;
  v_history_count int := 0;
  v_seg_keys_count int := 0;

  v_sqlstate text := null;
  v_err text := null;
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

  with inv as (
    select
      i.*,
      (i.header_snapshot_json->'attach_policy') as attach_policy
    from public.invoices i
    where i.id = p_invoice_id
    limit 1
  ),
  lines as (
    select
      l.*,
      ts.manual_pdf_r2_key,
      coalesce(
        l.paper_ts_r2_key,
        ts.manual_pdf_r2_key,
        case
          when l.timesheet_id is not null
            then ('docs-pdf/timesheets/ts_' || l.timesheet_id::text || '.pdf')
          else null
        end
      ) as effective_paper_ts_r2_key
    from public.invoice_lines l
    left join public.timesheets ts
      on ts.timesheet_id = l.timesheet_id
    where l.invoice_id = p_invoice_id
    order by l.created_at asc
  ),
  ts_ids as (
    select distinct
      case
        when l.timesheet_id is not null then l.timesheet_id
        when l.meta_json is not null
          and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') is not null
          and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '')::uuid
        else null
      end as timesheet_id
    from lines l
    where
      l.timesheet_id is not null
      or (
        l.meta_json is not null
        and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') is not null
        and nullif(btrim(coalesce(l.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      )
  ),

  -- ✅ UPDATED: reference rows joined to candidate display name (for UI display)
  -- Hardened candidate resolution via COALESCE(contract candidate vs TSFIN candidate).
  ref_rows_joined as (
    select
      r.*,
      con0.id as contract_id,
      coalesce(con0.candidate_id, tf0.candidate_id) as candidate_id,
      coalesce(
        nullif(btrim(coalesce(cand_contract.display_name,'')), ''),
        nullif(btrim(coalesce(cand_tf.display_name,'')), '')
      ) as candidate_display
    from public.invoice_reference_rows(p_invoice_id) r
    left join public.timesheets ts0
      on ts0.timesheet_id = r.timesheet_id
    left join public.contracts con0
      on con0.id = ts0.contract_id
    left join public.timesheets_financials tf0
      on tf0.timesheet_id = r.timesheet_id
     and tf0.is_current = true
    left join public.candidates cand_contract
      on cand_contract.id = con0.candidate_id
    left join public.candidates cand_tf
      on cand_tf.id = tf0.candidate_id
  ),

  -- ✅ additional timesheet ids referenced by reference rows (may include ids not present in lines)
  ref_ts_ids as (
    select distinct r.timesheet_id
    from ref_rows_joined r
    where r.timesheet_id is not null
  ),

  -- ✅ union set for reference-source hydration
  ts_ids_for_ref_sources as (
    select timesheet_id from ts_ids
    union
    select timesheet_id from ref_ts_ids
  ),

  -- ✅ TSFIN sources for timesheets that may have SEGMENTS refs (NHSP/HR/etc)
  tsfin_ref_sources as (
    select
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids_for_ref_sources)
  ),

  -- ✅ sources needed by FE to rebuild reference update payloads without extra calls
  --   - If timesheets.actual_schedule_json is present and non-empty, use it.
  --   - Else if TSFIN is SEGMENTS mode, derive an editable schedule-like array from TSFIN segments
  --     so multi-shift/day (NHSP/HR/manual) works via segment_id/start/end matching.
  ts_reference_sources as (
    select
      t.timesheet_id,
      t.reference_number,
      t.day_references_json,
      case
        when tf.invoice_breakdown_json is not null
          and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
          and upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
        then coalesce((
          select jsonb_agg(
            jsonb_build_object(
              'segment_id', seg->>'segment_id',
              'date', seg->>'date',
              'start_utc', seg->>'start_utc',
              'end_utc', seg->>'end_utc',
              -- legacy-friendly aliases (FE/DB matching may use either)
              'start', seg->>'start_utc',
              'end', seg->>'end_utc',
              'ref_num', seg->>'ref_num',
              'source_system', seg->>'source_system'
            )
            order by
              coalesce(seg->>'date',''),
              coalesce(seg->>'start_utc',''),
              coalesce(seg->>'segment_id','')
          )
          from jsonb_array_elements(tf.invoice_breakdown_json->'segments') seg
        ), '[]'::jsonb)

        when t.actual_schedule_json is not null
          and jsonb_typeof(t.actual_schedule_json) = 'array'
          and jsonb_array_length(t.actual_schedule_json) > 0
        then t.actual_schedule_json

        else '[]'::jsonb
      end as actual_schedule_json
    from public.timesheets t
    left join tsfin_ref_sources tf
      on tf.timesheet_id = t.timesheet_id
    where t.timesheet_id in (select timesheet_id from ts_ids_for_ref_sources)
  ),

  -- ✅ NEW: per-timesheet deterministic current refs signature (invoice-scoped)
  ref_sig_by_timesheet as (
    select
      r.timesheet_id,
      encode(
        extensions.digest(
          coalesce(
            string_agg(
              (
                concat_ws(
                  '|',
                  r.timesheet_id::text,
                  r.ref_target,
                  coalesce(r.segment_id,''),
                  coalesce(r.day_ymd::text,''),
                  coalesce(r.start_utc::text,''),
                  coalesce(r.end_utc::text,'')
                )
                || '=' || coalesce(r.current_reference,'')
              ),
              '||'
              ORDER BY
                concat_ws(
                  '|',
                  r.timesheet_id::text,
                  r.ref_target,
                  coalesce(r.segment_id,''),
                  coalesce(r.day_ymd::text,''),
                  coalesce(r.start_utc::text,''),
                  coalesce(r.end_utc::text,'')
                )
            ),
            ''
          ),
          'sha256'
        ),
        'hex'
      ) as current_refs_sig
    from ref_rows_joined r
    where r.timesheet_id is not null
    group by r.timesheet_id
  ),

  -- ✅ NEW: summary-derived exclusion flags (NHSP / no_timesheet_required) per timesheet
  ts_summary_flags as (
    select
      v.timesheet_id,
      coalesce(v.client_is_nhsp, false) as client_is_nhsp,
      coalesce(v.client_no_timesheet_required, false) as client_no_timesheet_required
    from public.v_timesheets_summary_base v
    where v.timesheet_id in (select timesheet_id from ts_ids)
  ),

  -- ✅ NEW: per-timesheet document flags (electronic regen + QR refs changed)
  -- ✅ FIX: exclude NHSP / no_timesheet_required from BOTH electronic and QR flags
  timesheet_doc_flags as (
    select
      ts.timesheet_id,
      jsonb_build_object(
        'electronic_refs_changed', flags.electronic_changed,
        'electronic_pdf_regen_required', flags.electronic_changed,
        'qr_refs_changed', flags.qr_changed,
        'reasons', reasons.reasons_json
      ) as flags_json
    from ts_ids tid
    join public.timesheets ts
      on ts.timesheet_id = tid.timesheet_id
    left join ref_sig_by_timesheet rs
      on rs.timesheet_id = ts.timesheet_id
    left join ts_summary_flags sf
      on sf.timesheet_id = ts.timesheet_id
    cross join lateral (
      select
        (
          (not x.is_excluded)
          and upper(coalesce(ts.submission_mode::text,'')) = 'ELECTRONIC'
          and ts.manual_pdf_r2_key is null
          and rs.current_refs_sig is not null
          and (ts.generated_pdf_refs_sig is null or ts.generated_pdf_refs_sig <> rs.current_refs_sig)
        ) as electronic_changed,

        (
          (not x.is_excluded)
          and (
            (
              ts.qr_status is not null
              or ts.qr_token is not null
              or ts.qr_last_sent_hash is not null
              or ts.qr_last_sent_at_utc is not null
            )
          )
          and rs.current_refs_sig is not null
          and (ts.qr_sent_refs_sig is null or ts.qr_sent_refs_sig <> rs.current_refs_sig)
        ) as qr_changed
      from (
        select
          (
            coalesce(sf.client_is_nhsp, false) = true
            or coalesce(sf.client_no_timesheet_required, false) = true
          ) as is_excluded
      ) x
    ) flags
    cross join lateral (
      select coalesce(jsonb_agg(z.reason) filter (where z.reason is not null), '[]'::jsonb) as reasons_json
      from (
        select
          case
            when flags.electronic_changed and ts.generated_pdf_refs_sig is null then 'ELECTRONIC_REFS_SIG_MISSING'
            when flags.electronic_changed then 'ELECTRONIC_REFS_SIG_DIFFERENT'
            else null
          end as reason
        union all
        select
          case
            when flags.qr_changed and ts.qr_sent_refs_sig is null then 'QR_REFS_SIG_MISSING'
            when flags.qr_changed then 'QR_REFS_SIG_DIFFERENT'
            else null
          end as reason
      ) z
    ) reasons
  ),

  ev as (
    select
      e.timesheet_id,
      e.kind,
      e.display_name,
      e.storage_key,
      e.created_at
    from public.timesheet_evidence e
    where e.timesheet_id in (select timesheet_id from ts_ids)
    order by e.created_at asc
  ),
  hr_cache as (
    select
      r.invoice_id,
      r.source_system,
      r.import_id,
      r.header_rows,
      r.header_columns,
      r.rows_json
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
  ),
  tsfin as (
    select
      tf.id as tsfin_id,
      tf.timesheet_id,
      tf.external_source_rows_json,

      -- ✅ Mileage fields for invoice itemisation
      tf.mileage_units,
      tf.mileage_pay_rate,
      tf.mileage_charge_rate
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids)
  ),

  tsfin_segments as (
    select
      tf.id as tsfin_id,
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids)
  ),
  seg_stats as (
    select
      t.timesheet_id,
      t.tsfin_id,

      -- ONLY segments on THIS invoice
      coalesce(
        jsonb_agg(
          s.seg
          order by coalesce(s.seg->>'date',''), coalesce(s.seg->>'segment_id','')
        ) filter (
          where s.seg is not null
            and s.locked_text = p_invoice_id::text
        ),
        '[]'::jsonb
      ) as invoiced_segments,

      -- counts (do not return the segments)
      (count(*) filter (where s.seg is not null and s.locked_text is null))::int
        as uninvoiced_segment_count,

      (count(*) filter (where s.seg is not null and s.locked_text is not null and s.locked_text <> p_invoice_id::text))::int
        as locked_elsewhere_segment_count

    from tsfin_segments t

    left join lateral (
      select
        value as seg,
        nullif(btrim(coalesce(value->>'invoice_locked_invoice_id','')), '') as locked_text
      from jsonb_array_elements(
        case
          when t.invoice_breakdown_json is not null
            and jsonb_typeof(t.invoice_breakdown_json) = 'object'
            and jsonb_typeof(t.invoice_breakdown_json->'segments') = 'array'
          then t.invoice_breakdown_json->'segments'
          else '[]'::jsonb
        end
      ) value
    ) s on true

    where upper(coalesce(t.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'

    group by t.timesheet_id, t.tsfin_id
  ),
  hist_audit as (
    select
      ae.id,
      ae.ts_utc,
      ae.actor_user_id,
      coalesce(ae.actor_display, tu.display_name, tu.email, 'CloudTMS server') as actor_display,
      coalesce(ae.actor_role_at_time, tu.role, 'system') as actor_role_at_time,
      ae.object_type,
      ae.object_id_text,
      ae.action,
      ae.before_json,
      ae.after_json,
      ae.reason,
      ae.ip,
      ae.user_agent,
      ae.correlation_id
    from public.audit_events ae
    left join public.tms_users tu
      on tu.id = ae.actor_user_id
    where ae.object_type in ('invoice','invoices')
      and ae.object_id_text = p_invoice_id::text
    order by ae.ts_utc desc, ae.id desc
    limit 500
  ),
  hist_mail as (
    select
      m.id as mail_outbox_id,
      m.created_at_utc as ts_utc,
      m.status,
      m."to" as to_email,
      m.subject,
      m.reference
    from public.mail_outbox m
    where upper(coalesce(m.type,'')) = 'INVOICE'
      and m.attachments is not null
      and jsonb_typeof(m.attachments) = 'array'
      and exists (
        select 1
        from jsonb_array_elements(m.attachments) a
        where btrim(coalesce(a->>'invoice_id','')) = p_invoice_id::text
      )
    order by m.created_at_utc desc, m.id desc
    limit 200
  ),
  history as (
    select
      jsonb_build_object(
        'kind','AUDIT',
        'id', ae.id::text,
        'ts_utc', to_jsonb(ae.ts_utc),
        'actor_user_id', case when ae.actor_user_id is null then null else ae.actor_user_id::text end,
        'actor_display', ae.actor_display,
        'actor_role_at_time', ae.actor_role_at_time,
        'action', ae.action,
        'reason', ae.reason,
        'object_type', ae.object_type,
        'object_id_text', ae.object_id_text,
        'before_json', ae.before_json,
        'after_json', ae.after_json,
        'ip', ae.ip,
        'user_agent', ae.user_agent,
        'correlation_id', ae.correlation_id
      ) as row_json,
      ae.ts_utc as ts_sort
    from hist_audit ae

    union all

    select
      jsonb_build_object(
        'kind','EMAIL',
        'mail_outbox_id', m.mail_outbox_id::text,
        'ts_utc', to_jsonb(m.ts_utc),
        'status', m.status::text,
        'to', m.to_email,
        'subject', m.subject,
        'reference', m.reference
      ) as row_json,
      m.ts_utc as ts_sort
    from hist_mail m
  ),
  email_summary as (
    select
      count(*)::int as email_count,
      max(m.created_at_utc) as last_email_at_utc
    from public.mail_outbox m
    where upper(coalesce(m.type,'')) = 'INVOICE'
      and m.attachments is not null
      and jsonb_typeof(m.attachments) = 'array'
      and exists (
        select 1
        from jsonb_array_elements(m.attachments) a
        where btrim(coalesce(a->>'invoice_id','')) = p_invoice_id::text
      )
  )
  select jsonb_build_object(
    'invoice', to_jsonb(inv.*),
    'header_snapshot_json', coalesce((select inv.header_snapshot_json from inv), '{}'::jsonb),
    'attach_policy', coalesce((select inv.attach_policy from inv), null),

    'lines', coalesce((
      select jsonb_agg(
        to_jsonb(l.*)
        || jsonb_build_object('paper_ts_r2_key', l.effective_paper_ts_r2_key)
        || jsonb_build_object('is_adjustment', (l.timesheet_id is null or upper(coalesce(l.meta_json->>'line_type','')) = 'ADJUSTMENT'))
        || jsonb_build_object('line_type_norm', upper(coalesce(l.meta_json->>'line_type','')))
        order by l.created_at
      )
      from lines l
    ), '[]'::jsonb),

    'timesheet_ids', coalesce((select jsonb_agg(t.timesheet_id::text) from ts_ids t), '[]'::jsonb),

    -- ✅ NEW: per-timesheet document flags for renderer (electronic regen + QR refs changed)
    'timesheet_doc_flags_by_id', coalesce((
      select jsonb_object_agg(
        f.timesheet_id::text,
        f.flags_json
      )
      from timesheet_doc_flags f
    ), '{}'::jsonb),

    -- ✅ mapping needed for segment edits (tsfin_id is invoice_apply_edits input)
    'tsfin_id_by_timesheet_id', coalesce((
      select jsonb_object_agg(
        t.timesheet_id::text,
        t.tsfin_id::text
      )
      from tsfin t
    ), '{}'::jsonb),

    -- ✅ mileage units/rates per timesheet (for PDF builder / UI without scanning lines)
    'mileage_by_timesheet_id', coalesce((
      select jsonb_object_agg(
        t.timesheet_id::text,
        jsonb_build_object(
          'mileage_units', t.mileage_units,
          'mileage_pay_rate', t.mileage_pay_rate,
          'mileage_charge_rate', t.mileage_charge_rate
        )
      )
      from tsfin t
    ), '{}'::jsonb),

    -- ✅ reference sources needed by FE to build reference update payloads with no extra calls
    'timesheet_reference_sources_by_id', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'reference_number', s.reference_number,
          'day_references_json', s.day_references_json,
          'actual_schedule_json', s.actual_schedule_json
        )
      )
      from ts_reference_sources s
    ), '{}'::jsonb),

    -- ✅ UPDATED: embed reference edit rows for zero-subrequest ref modal
    -- Adds candidate_display for UI rendering and deterministic row_key to stabilise FE staging identity.
    'reference_rows', coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'row_key', concat_ws(
            '::',
            r.timesheet_id::text,
            r.ref_target,
            coalesce(r.segment_id,''),
            coalesce(r.day_ymd::text,''),
            coalesce(r.start_utc::text,''),
            coalesce(r.end_utc::text,'')
          ),
          'timesheet_id', r.timesheet_id::text,
          'candidate_id', case when r.candidate_id is null then null else r.candidate_id::text end,
          'candidate_display', r.candidate_display,
          'sheet_scope', r.sheet_scope,
          'submission_mode', r.submission_mode,
          'ref_target', r.ref_target,
          'segment_id', r.segment_id,
          'day_ymd', r.day_ymd,
          'start_utc', r.start_utc,
          'end_utc', r.end_utc,
          'current_reference', r.current_reference,
          'is_required', r.is_required
        )
        order by
          r.timesheet_id::text,
          r.ref_target,
          r.day_ymd nulls last,
          r.start_utc nulls last,
          r.end_utc nulls last,
          r.segment_id nulls last
      )
      from ref_rows_joined r
    ), '[]'::jsonb),

    -- SEGMENTS: segment info for invoice modal expansion
    'segments_by_timesheet', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'tsfin_id', case when s.tsfin_id is null then null else s.tsfin_id::text end,
          'invoiced_segments', coalesce(s.invoiced_segments, '[]'::jsonb),
          'uninvoiced_segment_count', coalesce(s.uninvoiced_segment_count, 0),
          'locked_elsewhere_segment_count', coalesce(s.locked_elsewhere_segment_count, 0)
        )
      )
      from seg_stats s
    ), '{}'::jsonb),

    -- Alias required by brief
    'segments_on_invoice_by_timesheet', coalesce((
      select jsonb_object_agg(
        s.timesheet_id::text,
        jsonb_build_object(
          'tsfin_id', case when s.tsfin_id is null then null else s.tsfin_id::text end,
          'invoiced_segments', coalesce(s.invoiced_segments, '[]'::jsonb),
          'uninvoiced_segment_count', coalesce(s.uninvoiced_segment_count, 0),
          'locked_elsewhere_segment_count', coalesce(s.locked_elsewhere_segment_count, 0)
        )
      )
      from seg_stats s
    ), '{}'::jsonb),

    -- Backward compatible aggregate (all evidence)
    'evidence', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
    ), '[]'::jsonb),

    -- New explicit splits
    'timesheet_evidence', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
      where upper(coalesce(ev.kind,'')) = 'TIMESHEET'
    ), '[]'::jsonb),

    'evidence_other', coalesce((
      select jsonb_agg(to_jsonb(ev.*) order by ev.created_at)
      from ev
      where upper(coalesce(ev.kind,'')) <> 'TIMESHEET'
    ), '[]'::jsonb),

    'hr_source_rows_cache', coalesce((select jsonb_agg(to_jsonb(h.*)) from hr_cache h), '[]'::jsonb),
    'tsfin_external_source_rows', coalesce((select jsonb_agg(to_jsonb(t.*)) from tsfin t), '[]'::jsonb),

    -- Invoice history
    'history', coalesce((
      select jsonb_agg(h.row_json order by h.ts_sort desc)
      from history h
    ), '[]'::jsonb),

    -- email summary for UI label (Email vs Re-email)
    'email_summary', jsonb_build_object(
      'emailed_once', (coalesce((select email_count from email_summary),0) > 0),
      'email_count', coalesce((select email_count from email_summary),0),
      'last_email_at_utc', (select last_email_at_utc from email_summary)
    )
  )
  into v_manifest
  from inv;

  if v_manifest is null then
    v_manifest := '{}'::jsonb;
  end if;

  -- Extract simple counts for debug
  begin
    v_lines_count := coalesce(jsonb_array_length(coalesce(v_manifest->'lines','[]'::jsonb)), 0);
  exception when others then
    v_lines_count := 0;
  end;

  begin
    v_timesheet_ids_count := coalesce(jsonb_array_length(coalesce(v_manifest->'timesheet_ids','[]'::jsonb)), 0);
  exception when others then
    v_timesheet_ids_count := 0;
  end;

  begin
    v_evidence_count := coalesce(jsonb_array_length(coalesce(v_manifest->'evidence','[]'::jsonb)), 0);
  exception when others then
    v_evidence_count := 0;
  end;

  begin
    v_ts_evidence_count := coalesce(jsonb_array_length(coalesce(v_manifest->'timesheet_evidence','[]'::jsonb)), 0);
  exception when others then
    v_ts_evidence_count := 0;
  end;

  begin
    v_ev_other_count := coalesce(jsonb_array_length(coalesce(v_manifest->'evidence_other','[]'::jsonb)), 0);
  exception when others then
    v_ev_other_count := 0;
  end;

  begin
    v_history_count := coalesce(jsonb_array_length(coalesce(v_manifest->'history','[]'::jsonb)), 0);
  exception when others then
    v_history_count := 0;
  end;

  begin
    select coalesce(count(*),0)
    into v_seg_keys_count
    from jsonb_object_keys(coalesce(v_manifest->'segments_by_timesheet','{}'::jsonb)) k;
  exception when others then
    v_seg_keys_count := 0;
  end;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_RENDER_MANIFEST_DEBUG',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
        'run_finished_at_utc', public._inv_iso_utc(now()),
        'counts', jsonb_build_object(
          'lines', v_lines_count,
          'timesheet_ids', v_timesheet_ids_count,
          'evidence', v_evidence_count,
          'timesheet_evidence', v_ts_evidence_count,
          'evidence_other', v_ev_other_count,
          'history', v_history_count,
          'segments_by_timesheet_keys', v_seg_keys_count
        )
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

  return v_manifest;

exception when others then
  v_sqlstate := sqlstate;
  v_err := sqlerrm;

  if v_invoice_debug then
    perform public._inv_write_audit(
      null,
      'INVOICE_RENDER_MANIFEST_ERROR',
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'run_started_at_utc', public._inv_iso_utc(v_dbg_started),
        'run_failed_at_utc', public._inv_iso_utc(now()),
        'error', jsonb_build_object(
          'sqlstate', v_sqlstate,
          'message', v_err
        )
      ),
      'invoices',
      p_invoice_id::text,
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

  raise;
end;
$function$;

-- invoice_source_rows_collect(uuid,boolean)
CREATE OR REPLACE FUNCTION public.invoice_source_rows_collect(p_invoice_id uuid, p_force_refresh boolean DEFAULT true)
 RETURNS TABLE(source_system text, import_id uuid, header_rows jsonb, header_columns jsonb, rows_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_has_cache boolean := false;
  v_requires_hr boolean := false;
  v_hr_attach_to_invoice boolean := true;
  v_hr_allowed boolean := false;
  v_invoice_status public.invoice_status_enum;
  v_invoice_issued_at_utc timestamptz;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Attachment policy gating (must match generator policy):
  -- - NHSP rows are always eligible.
  -- - HEALTHROSTER rows are eligible only when (requires_hr = true AND hr_attach_to_invoice = true).
  select
    coalesce((i.header_snapshot_json #>> '{attach_policy,requires_hr}')::boolean, false) as requires_hr,
    coalesce((i.header_snapshot_json #>> '{attach_policy,hr_attach_to_invoice}')::boolean, true) as hr_attach_to_invoice,
    i.status,i.issued_at_utc
  into v_requires_hr, v_hr_attach_to_invoice,v_invoice_status,v_invoice_issued_at_utc
  from public.invoices i
  where i.id = p_invoice_id;

  if not found then
    raise exception 'invoice not found';
  end if;

  v_hr_allowed := coalesce(v_requires_hr,false) = true
                  and coalesce(v_hr_attach_to_invoice,false) = true;

  -- Issued/paid/on-hold invoice evidence is frozen.  A force flag may refresh
  -- a draft only; it never rewrites or back-fills an already-issued artefact.
  select exists(
    select 1
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
  ) into v_has_cache;

  if v_has_cache and (coalesce(p_force_refresh,false) = false
      or v_invoice_status<>'DRAFT'::public.invoice_status_enum or v_invoice_issued_at_utc is not null) then
    return query
    select
      r.source_system,
      r.import_id,
      r.header_rows,
      r.header_columns,
      r.rows_json
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
    order by r.source_system, r.import_id;
    return;
  end if;
  if not v_has_cache and (v_invoice_status<>'DRAFT'::public.invoice_status_enum or v_invoice_issued_at_utc is not null) then
    raise exception 'INVOICE_EVIDENCE_FROZEN_CACHE_MISSING' using errcode='55000';
  end if;

  -- Recompute + refresh cache (safe even if cache is empty)
  delete from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id;

  with lines as (
    select
      l.timesheet_id,
      l.meta_json
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
  ),
  -- ✅ FIX: derive timesheet_ids from either invoice_lines.timesheet_id OR meta_json.timesheet_id (UUID validated)
  ts_ids as (
    select distinct
      case
        when ln.timesheet_id is not null then ln.timesheet_id
        when ln.meta_json is not null
          and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') is not null
          and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '')::uuid
        else null
      end as timesheet_id
    from lines ln
    where
      ln.timesheet_id is not null
      or (
        ln.meta_json is not null
        and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') is not null
        and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      )
  ),
  fin as (
    select
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select t.timesheet_id from ts_ids t)
  ),
  segs as (
    select
      upper(coalesce(seg->>'source_system','')) as source_system,
      nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') as locked_invoice_id_text,
      nullif(btrim(coalesce(seg->>'nhsp_shift_id','')), '') as nhsp_shift_id_text
    from fin f
    cross join lateral jsonb_array_elements(coalesce(f.invoice_breakdown_json->'segments','[]'::jsonb)) seg
    where jsonb_typeof(seg) = 'object'
  ),
  shift_ids as (
    select distinct (sg.nhsp_shift_id_text)::uuid as shift_id
    from segs sg
    where sg.nhsp_shift_id_text is not null
      -- ✅ Critical: ONLY rows/segments locked to THIS invoice
      and sg.locked_invoice_id_text = p_invoice_id::text
      -- Source-system gating: NHSP always; HealthRoster only when allowed by policy
      and (
        sg.source_system = 'NHSP'
        or (sg.source_system = 'HEALTHROSTER' and v_hr_allowed = true)
      )
      and sg.nhsp_shift_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),

  -- Shift-based evidence (NHSP + weekly HealthRoster via nhsp_shifts)
  useful_shift as (
    select
      upper(coalesce(s.source_system::text,'UNKNOWN')) as source_system,
      s.latest_import_id as import_id,
      s.external_row_key as external_row_key
    from public.nhsp_shifts s
    where s.id in (select sh.shift_id from shift_ids sh)
      and s.latest_import_id is not null
      and s.external_row_key is not null
  ),

  -- Applied review resolutions are the Daily evidence authority. Historical
  -- imports without a review state retain the legacy payload link as read-only
  -- compatibility evidence.
  useful_daily as (
    select
      'HEALTHROSTER_DAILY'::text as source_system,
      hr.import_id as import_id,
      hr.external_row_key as external_row_key
    from public.hr_rows hr
    join public.hr_imports hi
      on hi.id = hr.import_id
    left join public.import_review_daily_timesheet_resolutions rr
      on rr.import_id=hr.import_id and rr.hr_row_id=hr.id and rr.status='APPLIED'
    where v_hr_allowed = true
      and hi.source_system = 'HEALTHROSTER_DAILY'::public.hr_source_enum
      and hr.import_id is not null
      and hr.external_row_key is not null
      and (
        rr.resolved_timesheet_id in (select t.timesheet_id from ts_ids t where t.timesheet_id is not null)
        or (rr.id is null and not exists(select 1 from public.import_review_states s where s.import_id=hr.import_id)
          and (hr.payload_json->>'resolved_timesheet_id') in
            (select t.timesheet_id::text from ts_ids t where t.timesheet_id is not null))
      )
  ),

  -- Unified evidence set
  useful as (
    select
      us.source_system,
      us.import_id,
      us.external_row_key
    from useful_shift us

    union all

    select
      ud.source_system,
      ud.import_id,
      ud.external_row_key
    from useful_daily ud
  ),

  grouped as (
    select
      u.source_system,
      u.import_id,
      jsonb_agg(distinct u.external_row_key) as keys_json
    from useful u
    group by u.source_system, u.import_id
  ),
  hdr as (
    select
      g.source_system,
      g.import_id,
      -- Prefer multi-row header if stored; else wrap single-row header_columns; else []
      case
        when jsonb_typeof(hi.parse_summary_json->'header_rows') = 'array'
          then (hi.parse_summary_json->'header_rows')
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then jsonb_build_array(hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_rows,
      case
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then (hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_columns,
      g.keys_json
    from grouped g
    join public.hr_imports hi
      on hi.id = g.import_id
  ),
  rows_agg as (
    select
      h.source_system,
      h.import_id,
      h.header_rows,
      h.header_columns,
      (
        select coalesce(jsonb_agg(r.payload_json order by r.id), '[]'::jsonb)
        from public.hr_rows r
        where r.import_id = h.import_id
          and r.external_row_key in (
            select jsonb_array_elements_text(h.keys_json)
          )
      ) as rows_json
    from hdr h
  )
  insert into public.invoice_hr_source_rows(
    invoice_id,
    source_system,
    import_id,
    header_rows,
    header_columns,
    rows_json
  )
  select
    p_invoice_id,
    ra.source_system,
    ra.import_id,
    ra.header_rows,
    ra.header_columns,
    ra.rows_json
  from rows_agg ra;

  -- Return refreshed cache
  return query
  select
    r.source_system,
    r.import_id,
    r.header_rows,
    r.header_columns,
    r.rows_json
  from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id
  order by r.source_system, r.import_id;

end;
$function$;

-- invoice_unhold_batch(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_unhold_batch(p_invoice_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        null::text
      into invoice_id, ok, status, error
      from public.invoice_unhold_one(v_id, p_actor_user_id) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_unhold_one(uuid,uuid)
CREATE OR REPLACE FUNCTION public.invoice_unhold_one(p_invoice_id uuid, p_actor_user_id uuid)
 RETURNS TABLE(status text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

   declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot unhold a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot unhold a PAID invoice';
    end if;

    if v_inv.status::text = 'DRAFT' then
      status := 'DRAFT';
      return next;
      return;
    end if;

    if v_inv.status::text <> 'ON_HOLD' then
      raise exception 'Only ON_HOLD invoices can be unheld (current status=%)', v_inv.status::text;
    end if;
  end;

  update public.invoices
  set status = case
        when paid_at_utc is not null then 'PAID'::public.invoice_status_enum
        when issued_at_utc is not null then 'ISSUED'::public.invoice_status_enum
        else 'DRAFT'::public.invoice_status_enum
      end,
      status_date_utc = v_now,
      on_hold_reason = null
  where id = p_invoice_id;


  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNHELD',
    null,
    '{}'::jsonb,
    null,
    p_actor_user_id
  );

  status := 'DRAFT';
  return next;
end;
$function$;

-- invoice_unissue_batch(uuid[],uuid,boolean)
CREATE OR REPLACE FUNCTION public.invoice_unissue_batch(p_invoice_ids uuid[], p_actor_user_id uuid, p_clear_pdf boolean DEFAULT false)
 RETURNS TABLE(invoice_id uuid, ok boolean, status text, cleared_pdf boolean, error text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids,1),0) = 0 then
    raise exception 'invoice_ids[] required';
  end if;

  foreach v_id in array p_invoice_ids loop
    begin
      select
        v_id,
        true,
        x.status,
        x.cleared_pdf,
        null::text
      into invoice_id, ok, status, cleared_pdf, error
      from public.invoice_unissue_one(v_id, p_actor_user_id, p_clear_pdf) x;

      return next;
    exception when others then
      invoice_id := v_id;
      ok := false;
      status := null;
      cleared_pdf := null;
      error := sqlerrm;
      return next;
    end;
  end loop;
end;
$function$;

-- invoice_unissue_one(uuid,uuid,boolean)
CREATE OR REPLACE FUNCTION public.invoice_unissue_one(p_invoice_id uuid, p_actor_user_id uuid, p_clear_pdf boolean DEFAULT false)
 RETURNS TABLE(status text, cleared_pdf boolean)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=now();
  v_inv public.invoices%rowtype;
  v_historical_version uuid;
  v_document_version_ids uuid[]:=array[]::uuid[];
  v_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
begin
  if p_invoice_id is null then raise exception 'invoice_id is required'; end if;
  if not v_service and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if(not found or v_role<>'admin') and not v_service then
    raise exception using errcode='42501',message='Invoice administrator permission required';
  end if;

  perform public._ctms_assert_invoice_can_unissue_v1(p_invoice_id,true,'INVOICE_UNISSUE');
  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,true,'INVOICE_UNISSUE');

  select * into v_inv from public.invoices where id=p_invoice_id for update;
  if not found then raise exception 'Invoice not found'; end if;
  if v_inv.type::text='CREDIT_NOTE' then raise exception 'Cannot unissue a CREDIT_NOTE'; end if;
  if v_inv.status::text='PAID' then raise exception 'Cannot unissue a PAID invoice'; end if;
  if v_inv.status::text='DRAFT' then
    status:='DRAFT';cleared_pdf:=false;return next;return;
  end if;
  if v_inv.status::text<>'ISSUED' then
    raise exception 'Only ISSUED invoices can be unissued (current status=%)',v_inv.status::text;
  end if;
  v_historical_version:=v_inv.issued_document_version_id;

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
      and v.id is distinct from v_historical_version
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_document_version_ids,v_document_operation_ids
  from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=v_now,updated_at_utc=v_now,
    error_json=jsonb_build_object(
      'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
  where c.document_version_id=any(v_document_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,updated_at_utc=v_now,
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_document_operation_ids)
    and o.operation_type='BUILD_DOCUMENT'
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_issue_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='CANCELLED',phase='CANCELLED',
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_delivery_operation_ids from changed;

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  perform 1 from public.invoice_operations o
  where o.id=any(v_operation_ids) for update;

  perform 1
  from private._invoice_operation_rollup_batch(v_operation_ids,v_now,true);

  update public.mail_outbox m
  set status='FAILED',failed_at=v_now,last_error='CANCELLED_BY_INVOICE_UNISSUE',
    attempt_lease_token=null,attempt_leased_at_utc=null,attempt_lease_expires_at_utc=null
  where m.status='QUEUED'
    and v_historical_version is not null
    and jsonb_typeof(m.attachments)='array'
    and exists(
      select 1 from jsonb_array_elements(m.attachments) descriptor(value)
      where descriptor.value->>'invoice_id'=p_invoice_id::text
        and descriptor.value->>'document_version_id'=v_historical_version::text);

  update public.invoices
  set status='DRAFT',status_date_utc=v_now,issued_at_utc=null,due_at_utc=null,
    on_hold_reason=null,invoice_pdf_r2_key=null,invoice_pdf_generated_at_utc=null,
    issued_document_version_id=null,preview_document_version_id=null,
    active_document_operation_id=null,active_issue_operation_id=null,
    document_revision=document_revision+1,
    document_state='STALE',issue_state='NOT_STARTED',
    header_snapshot_json=coalesce(header_snapshot_json,'{}')||jsonb_build_object(
      'last_unissued_document_version_id',v_historical_version,
      'last_unissued_at_utc',v_now),
    updated_at=v_now
  where id=p_invoice_id;

  perform public._audit_insert('invoice',p_invoice_id::text,'INVOICE_UNISSUED',null,
    jsonb_build_object(
      'clear_pdf_requested_ignored',p_clear_pdf,
      'compatibility_pointer_cleared',true,
      'historical_document_version_id',v_historical_version,
      'historical_object_preserved',true,
      'fenced_operation_ids',to_jsonb(v_operation_ids)),null,p_actor_user_id);

  status:='DRAFT';cleared_pdf:=true;return next;
end;
$function$;

-- invoice_work_claim_batch(text[],text,integer,integer,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_work_claim_batch(p_chunk_types text[], p_worker_id text, p_limit integer DEFAULT 25, p_lease_seconds integer DEFAULT 60, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS TABLE(chunk_id uuid, operation_id uuid, chunk_type text, phase text, entity_type text, entity_id uuid, document_version_id uuid, document_asset_id uuid, input_document_version_id uuid, payload_json jsonb, lease_token uuid, fence_token bigint, operation_control_version bigint, lease_expires_at_utc timestamp with time zone, attempt_count integer, max_attempts integer, priority integer, expected_page_count integer, expected_byte_count bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_limit integer := greatest(1,least(coalesce(p_limit,25),100));
  v_lease integer := greatest(15,least(coalesce(p_lease_seconds,60),600));
  v_types text[];
begin
  if coalesce(btrim(p_worker_id),'')='' then
    raise exception using errcode='22023',message='p_worker_id is required';
  end if;
  select coalesce(array_agg(distinct upper(btrim(x)) order by upper(btrim(x))),array[]::text[])
    into v_types from unnest(coalesce(p_chunk_types,array[]::text[])) x
    where upper(btrim(x)) in (
      'GENERATION_GROUP','DOCUMENT_PLAN','ASSET_INSPECT','ASSET_NORMALISE',
      'SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY',
      'ISSUE_INVOICE','DELIVERY_PREPARE','RECONCILE'
    );
  if cardinality(v_types)=0 then
    raise exception using errcode='22023',message='At least one runnable chunk type is required';
  end if;

  return query
  with invalid_running as (
    update public.invoice_operation_chunks c
    set status=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else 'RETRY_WAIT' end,
        phase=case when c.attempt_count>=c.max_attempts then 'DEAD_LETTER' else c.phase end,
        run_after_utc=case when c.attempt_count>=c.max_attempts then c.run_after_utc else v_now end,
        failed_at_utc=case when c.attempt_count>=c.max_attempts then v_now else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code',case
            when c.lease_token is null then 'RUNNING_WITHOUT_LEASE_TOKEN'
            when c.lease_owner is null then 'RUNNING_WITHOUT_LEASE_OWNER'
            when c.lease_expires_at_utc is null then 'RUNNING_WITHOUT_LEASE_EXPIRY'
            else 'LEASE_EXPIRED' end,
          'retryable',c.attempt_count<c.max_attempts,
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now))),
        updated_at_utc=v_now
    from public.invoice_operations o
    where o.id=c.operation_id and c.chunk_type=any(v_types) and c.status='RUNNING'
      and(c.lease_token is null or c.lease_owner is null
        or c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now)
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
    returning c.id,c.operation_id,c.document_asset_id,c.document_version_id,c.status
  ),
  exhausted as (
    update public.invoice_operation_chunks c
    set status='DEAD_LETTER',failed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object('code','MAX_ATTEMPTS_EXHAUSTED','retryable',false,
          'attempt_count',c.attempt_count,'max_attempts',c.max_attempts)
    from public.invoice_operations o
    where o.id=c.operation_id
      and c.chunk_type=any(v_types)
      and c.status in ('QUEUED','RETRY_WAIT')
      and c.attempt_count>=c.max_attempts
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
    returning c.id,c.operation_id,c.document_asset_id,c.document_version_id
  ),
  all_exhausted as materialized (
    select e.id,e.operation_id,e.document_asset_id,e.document_version_id
    from exhausted e
    union all
    select r.id,r.operation_id,r.document_asset_id,r.document_version_id
    from invalid_running r where r.status='DEAD_LETTER'
  ),
  failed_assets as (
    update public.invoice_document_assets a
    set status='FAILED',updated_at_utc=v_now,
        error_json=jsonb_build_object('code','ASSET_WORK_DEAD_LETTER',
          'chunk_id',e.id,'retryable',false)
    from all_exhausted e
    where a.id=e.document_asset_id and a.status not in('READY','SUPERSEDED')
    returning a.id
  ),
  failed_versions as (
    update public.invoice_document_versions v
    set status='FAILED',
        error_json=jsonb_build_object('code','DOCUMENT_WORK_DEAD_LETTER',
          'retryable',false,'failed_at_utc',v_now)
    where v.id in(
        select e.document_version_id from all_exhausted e
        where e.document_version_id is not null)
      and v.status not in('READY','SUPERSEDED','CANCELLED','FAILED')
    returning v.id
  ),
  blocked_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object('code','DEPENDENCY_DEAD_LETTER',
          'document_asset_id',d.document_asset_id,
          'input_document_version_id',d.input_document_version_id)
    where d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and(
        d.document_asset_id in(select id from failed_assets)
        or d.input_document_version_id in(select id from failed_versions))
    returning d.operation_id
  ),
  dead_ops as (
    update public.invoice_operations o
    set status=case when exists(
          select 1 from public.invoice_operation_chunks b
          where b.operation_id=o.id and b.status='BLOCKED') then 'BLOCKED' else 'DEAD_LETTER' end,
        phase=case when exists(
          select 1 from public.invoice_operation_chunks b
          where b.operation_id=o.id and b.status='BLOCKED') then 'BLOCKED' else 'DEAD_LETTER' end,
        requires_user_action=true,
        failed_at_utc=coalesce(o.failed_at_utc,v_now),updated_at_utc=v_now,
        error_json=jsonb_build_object('code','MAX_ATTEMPTS_EXHAUSTED','summary','One or more chunks exhausted retry attempts'),
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(
        select e.operation_id from all_exhausted e
        union select b.operation_id from blocked_dependencies b)
    returning o.id
  ),
  dead_rollup as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(id) from dead_ops),array[]::uuid[]),
      v_now,true) r
  ),
  candidate_rows as materialized (
    select c.id,c.operation_id,
      (c.priority+least(100,floor(extract(epoch from
        (v_now-c.created_at_utc))/3600)::integer)) effective_priority,
      c.run_after_utc,c.created_at_utc
    from public.invoice_operation_chunks c
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type=any(v_types)
      and c.chunk_type<>'DOCUMENT_INPUT'
      and c.status in ('QUEUED','RETRY_WAIT')
      and c.run_after_utc<=v_now
      and c.attempt_count<c.max_attempts
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      and (
        o.entity_type is distinct from 'INVOICE_BATCH'
        or (
          not c.is_manifest_member
          and coalesce(c.payload_json->>'is_selection_expander','false')
            in ('true','t','1','yes','on')
          and c.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
          and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
        )
        or (
          c.is_manifest_member
          and o.manifest_committed
          and c.manifest_committed
          and c.phase not in (
            'AWAITING_MANIFEST_COMMIT',
            'AWAITING_RELEASE'
          )
          and coalesce(c.payload_json->>'is_selection_expander','false')
            not in ('true','t','1','yes','on')
        )
      )
      and(select count(*) from dead_rollup)>=0
    order by (c.priority + least(100,floor(extract(epoch from (v_now-c.created_at_utc))/3600)::integer)) desc,
      c.run_after_utc,c.created_at_utc,c.id
    limit least(500,v_limit*5)
  ),
  candidate_graph as materialized (
    select g.*
    from private._invoice_current_chunk_ids_v2(
      coalesce((select array_agg(distinct candidate.id)
        from candidate_rows candidate),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      500) g
  ),
  picked as materialized (
    select c.id
    from candidate_rows candidate
    join candidate_graph g on g.requested_chunk_id=candidate.id
      and g.current_chunk_id=candidate.id
      and g.replacement_chain_status='CURRENT'
    join public.invoice_operation_chunks c on c.id=candidate.id
    order by candidate.effective_priority desc,
      candidate.run_after_utc,candidate.created_at_utc,candidate.id
    for update of c skip locked
    limit v_limit
  ),
  claimed as (
    update public.invoice_operation_chunks c
    set status='RUNNING',lease_owner=p_worker_id,lease_token=gen_random_uuid(),
        lease_expires_at_utc=v_now+make_interval(secs=>v_lease),
        fence_token=c.fence_token+1,operation_control_version=o.control_version,
        attempt_count=c.attempt_count+1,started_at_utc=coalesce(c.started_at_utc,v_now),
        updated_at_utc=v_now,
        error_json=case
          when c.error_json is null then null
          else jsonb_build_object('history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(
              c.error_json->'history','[]'::jsonb))
              with ordinality h(value,ordinality)
            where h.ordinality>greatest(jsonb_array_length(coalesce(
              c.error_json->'history','[]'::jsonb))-7,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now)))
          end
    from picked p,public.invoice_operations o
    where c.id=p.id and o.id=c.operation_id
    returning c.*
  ),
  touched_ops as (
    update public.invoice_operations o
    set status='RUNNING',
        phase=coalesce((
          select c.phase from claimed c where c.operation_id=o.id
          order by c.priority desc,c.sequence_no,c.id limit 1),o.phase),
        started_at_utc=coalesce(o.started_at_utc,v_now),
        updated_at_utc=v_now,change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in (select distinct cl.operation_id from claimed cl)
      and o.status in ('QUEUED','WAITING','RETRY_WAIT','RUNNING')
    returning o.id
  )
  select c.id,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
    c.document_version_id,c.document_asset_id,c.input_document_version_id,
    c.payload_json,c.lease_token,c.fence_token,c.operation_control_version,
    c.lease_expires_at_utc,c.attempt_count,c.max_attempts,c.priority,
    c.expected_page_count,c.expected_byte_count
  from claimed c
  order by c.priority desc,c.created_at_utc,c.id;
end;
$function$;

-- invoice_work_complete_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_work_complete_batch(p_results jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_valid_results jsonb:='[]'::jsonb;
  v_rejections jsonb:='[]'::jsonb;
  v_result jsonb:='[]'::jsonb;
  v_ignored integer;
begin
  if p_results is null or jsonb_typeof(p_results)<>'array' then
    raise exception using errcode='22023',
      message='p_results must be an array containing 1..100 items';
  end if;

  if jsonb_array_length(p_results)<1
     or jsonb_array_length(p_results)>100 then
    raise exception using errcode='22023',
      message='p_results must be an array containing 1..100 items';
  end if;

  /*
   * Ownership and payload validity are classified per item.  Ownership
   * failures are returned without mutation.  A current claim with a malformed
   * processor result is transitioned to a permanent failure or bounded retry;
   * it is never left RUNNING.
   */
  with supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_result,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version,
      upper(coalesce(x.value->>'outcome','')) outcome,
      coalesce(x.value->'result','{}'::jsonb) processor_result,
      coalesce(x.value->'error','{}'::jsonb) processor_error
    from jsonb_array_elements(p_results) with ordinality x(value,ordinality)
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      coalesce((
        select array_agg(distinct c.operation_id)
        from supplied s
        join public.invoice_operation_chunks c on c.id=s.chunk_id),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      null,null,10000) g
  ),
  inspected as materialized (
    select s.*,c.operation_id,c.chunk_type,c.phase,c.plan_generation,
      c.level_no,c.sequence_no,
      c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.input_document_version_id,
      c.payload_json,c.expected_page_count,c.expected_byte_count,c.attempt_count,c.max_attempts,
      c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,c.operation_control_version current_control_version,
      c.lease_expires_at_utc,o.status operation_status,
      o.control_version operation_current_control_version,
      o.config_json->'processor_policy' processor_policy,
      a.source_revision asset_source_revision,
      a.original_r2_key registered_original_r2_key,
      a.original_sha256 registered_original_sha256,
      a.original_size_bytes registered_original_size_bytes,
      dv.source_revision document_source_revision,dv.manifest_hash,dv.manifest_json,
      dv.snapshot_json,dv.snapshot_hash,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_COMPLETION'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when exists(select 1 from current_graph g
          where g.operation_id=c.operation_id
            and g.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when not exists(select 1 from current_graph g
          where g.current_chunk_id=c.id
            and g.replacement_chain_status='VALID')
          then 'CHUNK_NOT_CURRENT'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'OPERATION_TERMINAL'
      end ownership_error
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
    left join public.invoice_document_assets a on a.id=c.document_asset_id
    left join public.invoice_document_versions dv on dv.id=c.document_version_id
  ),
  identified as materialized (
    select i.*,
      case
        when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
          then 'invoice-assets/'||i.document_asset_id||'/'||
            coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
            i.fence_token||'/'
        when i.chunk_type='SOURCE_RENDER'
          then 'invoice-documents/'||i.document_version_id||'/source/'||
            i.chunk_id||'/'||i.fence_token||'/'
        when i.chunk_type='INVOICE_CORE_RENDER'
          then 'invoice-documents/'||i.document_version_id||'/core/'||
            i.chunk_id||'/'||i.fence_token||'/'
        when i.chunk_type='PDF_MERGE'
          then 'invoice-documents/'||i.document_version_id||'/merge/'||
            i.level_no||'/'||i.sequence_no||'/'||i.chunk_id||'/'||
            i.fence_token||'/'
        when i.chunk_type='DOCUMENT_VERIFY' then null
      end expected_output_prefix,
      case when coalesce(
          i.processor_policy->'result'->>i.chunk_type,'')~'^[1-9][0-9]{0,9}$'
        then(i.processor_policy->'result'->>i.chunk_type)::integer
      end result_limit_bytes
    from inspected i
  ),
  classified as materialized (
    select i.*,
      case
        when i.outcome not in('SUCCESS','RETRY','BLOCKED','FAILED','SUPERSEDED','CANCELLED')
          then 'INVALID_OUTCOME'
        when coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version')<>'INVOICE_PROCESSOR_LIMITS_V4'
          or i.result_limit_bytes is null
          then 'PROCESSOR_POLICY_INVALID'
        when jsonb_typeof(i.processor_result)<>'object'
          or octet_length(i.processor_result::text)>i.result_limit_bytes
          or lower(i.processor_result::text) ~
            '"(base64|file_bytes|raw_bytes|processor_dump)"[[:space:]]*:'
          then 'INVALID_RESULT_PAYLOAD'
        when i.outcome='SUCCESS' and(
          coalesce(i.processor_result->>'chunk_id','')<>i.chunk_id::text
          or coalesce(i.processor_result->>'fence_token','')<>i.fence_token::text
          or upper(coalesce(i.processor_result->>'action',''))<>
            case i.chunk_type
              when 'ASSET_INSPECT' then 'ASSET_INSPECT'
              when 'ASSET_NORMALISE' then 'ASSET_NORMALISE'
              when 'SOURCE_RENDER' then 'SOURCE_RENDER'
              when 'INVOICE_CORE_RENDER' then 'INVOICE_CORE_RENDER'
              when 'PDF_MERGE' then 'PDF_MERGE'
              when 'DOCUMENT_VERIFY' then 'DOCUMENT_VERIFY'
              else upper(i.chunk_type) end
          or(i.document_asset_id is not null
            and coalesce(i.processor_result->>'document_asset_id','')
              <>i.document_asset_id::text)
          or(i.document_version_id is not null
            and coalesce(i.processor_result->>'document_version_id','')
              <>i.document_version_id::text)
          or coalesce(i.processor_result->>'plan_generation','')<>
            i.plan_generation::text
          or coalesce(i.processor_result->>'processor_policy_version','')<>
            coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
          or(i.chunk_type<>'DOCUMENT_VERIFY' and
            coalesce(i.processor_result->>'output_prefix','')<>
              coalesce(i.expected_output_prefix,''))
          or(i.document_version_id is not null
            and nullif(i.document_source_revision,'') is not null
            and coalesce(i.processor_result->>'template_version','')<>
              case when i.chunk_type='SOURCE_RENDER'
                then coalesce(i.payload_json->>'template_version','')
                else coalesce((select v.template_version
                  from public.invoice_document_versions v
                  where v.id=i.document_version_id),'')
              end)
          or(i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
            and coalesce(i.processor_result->>'render_kind','')<>
              coalesce(i.payload_json->>'render_kind',
                case when i.chunk_type='INVOICE_CORE_RENDER'
                  then 'INVOICE_CORE' end,''))
          or(i.chunk_type='PDF_MERGE'
            and coalesce(i.processor_result->>'ordered_input_hash','')<>
              coalesce(i.payload_json->>'ordered_input_hash',''))
          or(i.chunk_type='PDF_MERGE'
            and coalesce(i.processor_result->>'apply_final_page_numbers','false')
              is distinct from coalesce(
                i.payload_json->>'apply_final_page_numbers','false'))
          or(i.chunk_type='PDF_MERGE'
            and coalesce(i.processor_result->>'page_numbering_contract','')
              is distinct from coalesce(
                i.payload_json->>'page_numbering_contract',''))
          or(i.chunk_type='PDF_MERGE'
            and coalesce(i.processor_result->'page_numbering_excluded_pages',
                  '[]'::jsonb)
              is distinct from coalesce(
                i.payload_json->'page_numbering_excluded_pages','[]'::jsonb))
        ) then 'PROCESSOR_RESULT_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and (
            /*
             * Render completion must prove the same frozen presentation model
             * identity that was planned and handed out by context.  For source
             * renders the source-model identity must come from the chunk
             * payload / expected_result_identity; we deliberately do not fall
             * back to the invoice-root model hash for source chunks.
             */
            nullif(coalesce(
              i.processor_result->>'presentation_model_schema_version',
              i.processor_result#>>'{render_identity,presentation_model_schema_version}',
              i.processor_result#>>'{identity,presentation_model_schema_version}',
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'presentation_model_hash',
              i.processor_result#>>'{render_identity,presentation_model_hash}',
              i.processor_result#>>'{identity,presentation_model_hash}',
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'snapshot_hash',
              i.processor_result#>>'{render_identity,snapshot_hash}',
              i.processor_result#>>'{identity,snapshot_hash}',
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'presentation_model_schema_version',
              i.payload_json#>>'{expected_result_identity,presentation_model_schema_version}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json#>>'{presentation_model,schema_version}' end,
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'presentation_model_hash',
              i.payload_json#>>'{expected_result_identity,presentation_model_hash}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json->>'presentation_model_hash' end,
              ''),'') is null
            or nullif(coalesce(
              i.payload_json->>'snapshot_hash',
              i.payload_json#>>'{expected_result_identity,snapshot_hash}',
              i.snapshot_hash,
              ''),'') is null
            or nullif(coalesce(
              i.processor_result->>'presentation_model_schema_version',
              i.processor_result#>>'{render_identity,presentation_model_schema_version}',
              i.processor_result#>>'{identity,presentation_model_schema_version}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'presentation_model_schema_version',
              i.payload_json#>>'{expected_result_identity,presentation_model_schema_version}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json#>>'{presentation_model,schema_version}' end,
              ''),'')
            or nullif(coalesce(
              i.processor_result->>'presentation_model_hash',
              i.processor_result#>>'{render_identity,presentation_model_hash}',
              i.processor_result#>>'{identity,presentation_model_hash}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'presentation_model_hash',
              i.payload_json#>>'{expected_result_identity,presentation_model_hash}',
              case when i.chunk_type='INVOICE_CORE_RENDER'
                then i.snapshot_json->>'presentation_model_hash' end,
              ''),'')
            or nullif(coalesce(
              i.processor_result->>'snapshot_hash',
              i.processor_result#>>'{render_identity,snapshot_hash}',
              i.processor_result#>>'{identity,snapshot_hash}',
              ''),'') is distinct from nullif(coalesce(
              i.payload_json->>'snapshot_hash',
              i.payload_json#>>'{expected_result_identity,snapshot_hash}',
              i.snapshot_hash,
              ''),'')
          ) then 'RENDER_MODEL_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in('','unknown','application/octet-stream')
          then 'ASSET_MEDIA_TYPE_UNSUPPORTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='empty'
          then 'ASSET_EMPTY'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='truncated'
          then 'ASSET_TRUNCATED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_kind',''))='missing'
          then 'MISSING_SOURCE'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and coalesce(i.processor_result->>'is_encrypted','false')='true'
          then 'ASSET_PDF_ENCRYPTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) not in(
              'application/pdf','pdf','image/jpeg','jpeg','jpg','image/png','png')
          then 'ASSET_MEDIA_TYPE_UNSUPPORTED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and(coalesce(i.processor_result->>'original_size_bytes','') !~ '^[0-9]{1,18}$'
            or case when coalesce(i.processor_result->>'original_size_bytes','')
                ~ '^[0-9]{1,18}$'
              then(i.processor_result->>'original_size_bytes')::bigint
              else 0 end<=0
            or coalesce(i.processor_result->>'original_sha256','')
              !~ '^[0-9a-f]{64}$')
          then 'INVALID_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and(
            (i.processor_result->>'original_size_bytes')::bigint>
              (i.processor_policy#>>'{asset,max_source_bytes}')::bigint
            or not exists(
              select 1
              from jsonb_array_elements_text(
                i.processor_policy#>'{asset,allowed_media_types}') allowed(media_type)
              where allowed.media_type=lower(
                i.processor_result->>'detected_media_type')))
          then 'ASSET_SOURCE_SIZE_EXCEEDED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in('application/pdf','pdf')
          and(coalesce(i.processor_result->>'page_count','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'parse_verified','false')<>'true'
            or coalesce(i.processor_result->>'is_encrypted','false')<>'false')
          then 'INVALID_PDF_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in(
              'image/jpeg','jpeg','jpg','image/png','png')
          and(coalesce(i.processor_result->>'width_pixels','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'height_pixels','') !~ '^[1-9][0-9]{0,8}$'
            or coalesce(i.processor_result->>'estimated_decoded_bytes','')
              !~ '^[1-9][0-9]{0,17}$'
            or coalesce(i.processor_result->>'decode_verified','false')<>'true'
            or coalesce(i.processor_result->>'orientation_degrees','')
              !~ '^(0|90|180|270)$')
          then 'INVALID_IMAGE_INSPECTION_RESULT'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and lower(coalesce(i.processor_result->>'detected_media_type',
            i.processor_result->>'detected_kind','')) in(
              'image/jpeg','jpeg','jpg','image/png','png')
          and(
            (i.processor_result->>'width_pixels')::bigint*
              (i.processor_result->>'height_pixels')::bigint>
                (i.processor_policy#>>'{asset,max_pixels}')::bigint
            or(i.processor_result->>'estimated_decoded_bytes')::bigint>
                (i.processor_policy#>>'{asset,max_decoded_bytes}')::bigint)
          then 'ASSET_DECODE_POLICY_EXCEEDED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_INSPECT'
          and i.registered_original_sha256 is not null
          and i.processor_result->>'original_sha256'
            is distinct from i.registered_original_sha256
          then 'ORIGINAL_HASH_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE')
          and(coalesce(i.processor_result->>'r2_key','')=''
            or coalesce(i.processor_result->>'sha256','')
              !~ '^[0-9a-f]{64}$'
            or coalesce(i.processor_result->>'size_bytes','') !~ '^[0-9]{1,18}$'
            or case when coalesce(i.processor_result->>'size_bytes','')
                ~ '^[0-9]{1,18}$'
              then(i.processor_result->>'size_bytes')::bigint else 0 end<=0
            or coalesce(i.processor_result->>'page_count','') !~ '^[0-9]{1,9}$'
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else 0 end<=0
            or coalesce(i.processor_result->>'parse_verified','false')<>'true')
          then 'INVALID_ARTIFACT_RESULT'
        when i.outcome='SUCCESS'
          and i.chunk_type in('ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE')
          and left(i.processor_result->>'r2_key',length(
            case
              when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
                then 'invoice-assets/'||i.document_asset_id||'/'||
                  coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
                  i.fence_token||'/'
              when i.chunk_type='SOURCE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/source/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='INVOICE_CORE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/core/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='PDF_MERGE'
                then 'invoice-documents/'||i.document_version_id||'/merge/'||
                  i.level_no||'/'||i.sequence_no||'/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              else 'invoice-documents/'||i.document_version_id||'/verify/'||
                i.chunk_id||'/'||i.fence_token||'/'
            end)) is distinct from
            case
              when i.chunk_type in('ASSET_NORMALISE','ASSET_INSPECT')
                then 'invoice-assets/'||i.document_asset_id||'/'||
                  coalesce(i.asset_source_revision,'')||'/'||i.chunk_id||'/'||
                  i.fence_token||'/'
              when i.chunk_type='SOURCE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/source/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='INVOICE_CORE_RENDER'
                then 'invoice-documents/'||i.document_version_id||'/core/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              when i.chunk_type='PDF_MERGE'
                then 'invoice-documents/'||i.document_version_id||'/merge/'||
                  i.level_no||'/'||i.sequence_no||'/'||
                  i.chunk_id||'/'||i.fence_token||'/'
              else 'invoice-documents/'||i.document_version_id||'/verify/'||
                i.chunk_id||'/'||i.fence_token||'/'
            end
          then 'OUTPUT_IDENTITY_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(coalesce(i.processor_result->>'input_count','') !~ '^[0-9]{1,9}$'
            or case when coalesce(i.processor_result->>'input_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'input_count')::integer else -1 end<>
                jsonb_array_length(coalesce(i.payload_json->'inputs','[]'::jsonb))
            or coalesce(i.processor_result->>'ordered_input_hash','')<>
              coalesce(i.payload_json->>'ordered_input_hash',''))
          then 'MERGE_INPUT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(
            jsonb_typeof(i.processor_result->'input_receipts')<>'array'
            or jsonb_array_length(i.processor_result->'input_receipts')<>
              jsonb_array_length(coalesce(i.payload_json->'inputs','[]'::jsonb))
            or exists(
              select 1
              from jsonb_array_elements(coalesce(
                i.payload_json->'inputs','[]'::jsonb))
                with ordinality expected(value,ordinality)
              full join jsonb_array_elements(coalesce(
                i.processor_result->'input_receipts','[]'::jsonb))
                with ordinality actual(value,ordinality)
                using(ordinality)
              where expected.value is null or actual.value is null
                or actual.value->>'input_chunk_id'
                  is distinct from expected.value->>'input_chunk_id'
                or actual.value->>'actual_input_order'
                  is distinct from expected.value->>'input_order'
                or actual.value->>'actual_input_order'
                  is distinct from actual.ordinality::text
                or actual.value->>'actual_r2_key'
                  is distinct from expected.value->>'r2_key'
                or actual.value->>'actual_sha256'
                  is distinct from expected.value->>'sha256'
                or actual.value->>'actual_page_count'
                  is distinct from expected.value->>'page_count'
                or actual.value->>'actual_size_bytes'
                  is distinct from expected.value->>'size_bytes'
                or(
                  expected.value?'expected_physical_receipt'
                  and(
                    actual.value->>'logical_source_key'
                      is distinct from expected.value->>'logical_source_key'
                    or actual.value->>'logical_manifest_ordinal'
                      is distinct from
                        expected.value->>'logical_manifest_ordinal'
                    or actual.value->>'physical_part_no'
                      is distinct from expected.value->>'physical_part_no'
                    or actual.value->>'actual_physical_receipt'
                      is distinct from
                        expected.value->>'expected_physical_receipt'
                    or actual.value->>'actual_physical_receipt'
                      is distinct from encode(digest(jsonb_build_object(
                        'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
                        'logical_source_key',
                          actual.value->>'logical_source_key',
                        'logical_manifest_ordinal',
                          case when coalesce(actual.value->>
                              'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                            then(actual.value->>
                              'logical_manifest_ordinal')::integer end,
                        'physical_part_no',
                          case when coalesce(actual.value->>
                              'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                            then(actual.value->>
                              'physical_part_no')::integer end,
                        'object_key',actual.value->>'actual_r2_key',
                        'stored_sha256',actual.value->>'actual_sha256',
                        'expected_page_count',
                          case when coalesce(actual.value->>
                              'actual_page_count','')~'^[1-9][0-9]{0,8}$'
                            then(actual.value->>
                              'actual_page_count')::integer end,
                        'expected_byte_count',
                          case when coalesce(actual.value->>
                              'actual_size_bytes','')~'^[1-9][0-9]{0,17}$'
                            then(actual.value->>
                              'actual_size_bytes')::bigint end
                      )::text,'sha256'),'hex')))
                or(
                  expected.value?'child_merge_receipt_hash'
                  and(
                    actual.value->'actual_child_merge_receipt'
                      is distinct from expected.value->'child_merge_receipt'
                    or encode(digest(coalesce(actual.value->
                        'actual_child_merge_receipt','{}'::jsonb)::text,
                      'sha256'),'hex')
                        is distinct from
                          expected.value->>'child_merge_receipt_hash'
                    or actual.value->>'actual_child_merge_receipt_hash'
                      is distinct from
                        expected.value->>'child_merge_receipt_hash'))
            ))
          then 'MERGE_RECEIPT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(
            jsonb_typeof(i.processor_result->'merge_receipt')<>'object'
            or coalesce(i.processor_result#>>
              '{merge_receipt,receipt_contract}','')<>
                'ACTUAL_BYTES_MERGE_RECEIPT_V3'
            or jsonb_typeof(i.processor_result#>
              '{merge_receipt,input_receipts}')<>'array'
            or i.processor_result#>'{merge_receipt,input_receipts}'
              is distinct from i.processor_result->'input_receipts'
            or nullif(i.processor_result#>>
              '{merge_receipt,processor_version}','') is null
            or coalesce(i.processor_result#>>
              '{merge_receipt,processor_policy_version}','')<>
                coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_ordered_input_hash}','')<>
                coalesce(i.payload_json->>'ordered_input_hash','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,output_sha256}','')<>
                coalesce(i.processor_result->>'sha256','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,output_page_count}','')<>
                coalesce(i.processor_result->>'page_count','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_child_receipt_hash}','')<>
                coalesce(i.payload_json->>'expected_child_receipt_hash','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,actual_child_receipt_hash}','')<>
              coalesce((
                select encode(digest(string_agg(coalesce(
                  nullif(r.value->>'actual_physical_receipt',''),
                  encode(digest(coalesce(r.value->
                    'actual_child_merge_receipt','{}'::jsonb)::text,
                    'sha256'),'hex')),'||'
                  order by r.ordinality),'sha256'),'hex')
                from jsonb_array_elements(coalesce(
                  i.processor_result->'input_receipts','[]'::jsonb))
                  with ordinality r(value,ordinality)
              ),'')
            or i.processor_result#>'{merge_receipt,physical_receipts}'
                is distinct from
                  coalesce(i.payload_json->'expected_physical_receipts',
                    '[]'::jsonb)
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_logical_receipt_root}','')<>
                coalesce(i.payload_json->>'expected_logical_receipt_root','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_physical_receipt_root}','')<>
                coalesce(i.payload_json->>'expected_physical_receipt_root','')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_physical_receipt_root}','')<>
              coalesce((
                select encode(digest(string_agg(
                  r.value->>'physical_receipt','||'
                  order by
                    case when coalesce(r.value->>
                        'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                      then(r.value->>
                        'logical_manifest_ordinal')::integer end,
                    case when coalesce(r.value->>
                        'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                      then(r.value->>'physical_part_no')::integer end,
                    r.ordinality),'sha256'),'hex')
                from jsonb_array_elements(coalesce(i.processor_result#>
                  '{merge_receipt,physical_receipts}','[]'::jsonb))
                  with ordinality r(value,ordinality)
                where coalesce(r.value->>'physical_receipt','')
                  ~'^[0-9a-f]{64}$'
              ),'')
            or coalesce(i.processor_result#>>
              '{merge_receipt,combined_logical_receipt_root}','')<>
              coalesce((
                select encode(digest(string_agg(
                  logical.logical_receipt,'||'
                  order by logical.logical_manifest_ordinal,
                    logical.logical_source_key),'sha256'),'hex')
                from(
                  select r.value->>'logical_source_key'
                      logical_source_key,
                    min(case when coalesce(r.value->>
                        'logical_manifest_ordinal','')~'^[0-9]{1,9}$'
                      then(r.value->>
                        'logical_manifest_ordinal')::integer end)
                      logical_manifest_ordinal,
                    encode(digest(jsonb_build_object(
                      'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
                      'logical_source_key',
                        r.value->>'logical_source_key',
                      'logical_manifest_ordinal',
                        min(case when coalesce(r.value->>
                            'logical_manifest_ordinal','')
                              ~'^[0-9]{1,9}$'
                          then(r.value->>
                            'logical_manifest_ordinal')::integer end),
                      'ordered_physical_receipts',string_agg(
                        r.value->>'physical_receipt','||'
                        order by case when coalesce(r.value->>
                            'logical_manifest_ordinal','')
                              ~'^[0-9]{1,9}$'
                          then(r.value->>
                            'logical_manifest_ordinal')::integer end,
                          case when coalesce(r.value->>
                            'physical_part_no','')
                              ~'^[1-9][0-9]{0,8}$'
                          then(r.value->>
                            'physical_part_no')::integer end,
                          r.ordinality)
                    )::text,'sha256'),'hex') logical_receipt
                  from jsonb_array_elements(coalesce(
                    i.processor_result#>
                      '{merge_receipt,physical_receipts}',
                    '[]'::jsonb)) with ordinality r(value,ordinality)
                  where nullif(r.value->>
                      'logical_source_key','') is not null
                    and coalesce(r.value->>'physical_receipt','')
                      ~'^[0-9a-f]{64}$'
                  group by r.value->>'logical_source_key'
                ) logical
              ),'')
            or coalesce(i.processor_result#>>
              '{merge_receipt,plan_generation}','')<>
                i.plan_generation::text
            or coalesce(i.processor_result#>>
              '{merge_receipt,page_numbering_applied}','false')<>
                coalesce(i.payload_json->>'apply_final_page_numbers','false')
            or coalesce(i.processor_result#>>
              '{merge_receipt,page_numbering_contract}','')<>
                coalesce(i.payload_json->>'page_numbering_contract','')
            or coalesce(i.processor_result#>
              '{merge_receipt,page_numbering_excluded_pages}','[]'::jsonb)<>
                coalesce(i.payload_json->'page_numbering_excluded_pages',
                  '[]'::jsonb)
          ) then 'MERGE_RECEIPT_CHAIN_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='PDF_MERGE'
          and(i.expected_page_count is null
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else -1 end<>
                i.expected_page_count)
          then 'MERGE_PAGE_COUNT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_NORMALISE'
          and(coalesce(i.processor_result->>'consumed_original_r2_key','')<>coalesce(i.registered_original_r2_key,'')
            or coalesce(i.processor_result->>'consumed_original_sha256','')<>coalesce(i.registered_original_sha256,'')
            or coalesce(i.processor_result->>'consumed_original_size_bytes','')<>coalesce(i.registered_original_size_bytes::text,''))
          then 'ASSET_SOURCE_IDENTITY_CHANGED'
        when i.outcome='SUCCESS' and i.chunk_type='ASSET_NORMALISE'
          and(i.expected_page_count is null
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer else -1 end<>
                i.expected_page_count)
          then 'ASSET_PART_PAGE_COUNT_MISMATCH'
        when i.outcome='SUCCESS'
          and i.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and(
            lower(coalesce(i.processor_result->>'output_type','')) not in(
              'pdf','application/pdf')
            or coalesce(i.processor_result->>'template_version','')<>
              coalesce(i.payload_json->>'template_version','')
            or coalesce(i.processor_result->>'render_kind','')<>
              coalesce(i.payload_json->>'render_kind','')
            or coalesce(i.processor_result->>'document_version_id','')<>
              i.document_version_id::text)
          then 'RENDER_CONTRACT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='SOURCE_RENDER'
          and i.payload_json->>'render_kind'='ATTACHMENT_INDEX'
          and(
            coalesce(i.processor_result->>'layout_phase','')<>
              coalesce(i.payload_json->>'layout_phase','')
            or coalesce(i.processor_result->>'layout_pass','')<>
              coalesce(i.payload_json->>'layout_pass','')
            or coalesce(i.processor_result->>'layout_page_count','')<>
              coalesce(i.processor_result->>'page_count','')
            or nullif(i.processor_result->>'processor_version','') is null
            or coalesce(i.processor_result->>'processor_policy_version','')<>
              coalesce(i.processor_policy->>'policy_version',i.processor_policy->>'version','')
            or(i.payload_json->>'layout_phase'='FINAL'
              and(
                coalesce(i.processor_result->>'displayed_start_pages_hash','')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or jsonb_typeof(i.processor_result->'displayed_page_map')<>'array'
                or i.processor_result->'displayed_page_map'
                  is distinct from i.payload_json->'attachments'
                or encode(digest(coalesce(
                    i.processor_result->'displayed_page_map','[]'::jsonb)::text,
                    'sha256'),'hex')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or coalesce(i.processor_result->>'displayed_page_map_hash','')<>
                  coalesce(i.payload_json->>'expected_start_pages_hash','')
                or coalesce(i.processor_result->>'displayed_row_count','')<>
                  coalesce(i.payload_json->>'displayed_row_count','')
                or coalesce(i.processor_result->>'final_index_page_count','')<>
                  coalesce(i.processor_result->>'page_count','')
                or coalesce(i.processor_result->>'pagination_stream_hash','')<>
                  coalesce(i.payload_json->>'pagination_stream_hash','')
                or coalesce(i.processor_result->>'layout_identity_hash','')<>
                  encode(digest(coalesce(i.payload_json->'determinism',
                    '{}'::jsonb)::text,'sha256'),'hex')
                or coalesce(i.processor_result->>'displayed_rows_verified','false')
                  <>'true'))
          ) then 'ATTACHMENT_INDEX_CONTRACT_MISMATCH'
        when i.outcome='SUCCESS' and i.chunk_type='DOCUMENT_VERIFY'
          and(coalesce(i.processor_result->>'manifest_coverage_verified','false')<>'true'
            or coalesce(i.processor_result->>'ordering_verified','false')<>'true'
            or coalesce(i.processor_result->>'manifest_hash','')<>i.manifest_hash
            or coalesce(i.processor_result->>'root_merge_receipt_hash','')<>
              coalesce(i.payload_json->>'final_merge_receipt_hash','')
            or encode(digest(coalesce(
              i.payload_json->'final_merge_receipt','{}'::jsonb)::text,
              'sha256'),'hex')<>
              coalesce(i.payload_json->>'final_merge_receipt_hash','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,receipt_contract}','')<>
                'ACTUAL_BYTES_MERGE_RECEIPT_V3'
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,combined_logical_receipt_root}','')<>
                coalesce(i.payload_json->>
                  'expected_logical_root_receipt','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,combined_physical_receipt_root}','')<>
                coalesce(i.payload_json->>
                  'expected_physical_root_receipt','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,actual_child_receipt_hash}','')<>
                coalesce(i.payload_json->>'expected_ordered_input_root','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,output_page_count}','')<>
                coalesce(i.payload_json->>'independent_expected_page_count','')
            or coalesce(i.payload_json#>>
              '{final_merge_receipt,output_sha256}','')<>
                coalesce(i.payload_json->>'candidate_sha256','')
            or coalesce(i.processor_result->>
              'root_merge_receipt_identity','')<>
                coalesce(i.payload_json->>'root_merge_receipt_identity','')
            or coalesce(i.processor_result->>
              'actual_logical_root_receipt','')<>
                coalesce(i.payload_json->>
                  'expected_logical_root_receipt','')
            or coalesce(i.processor_result->>
              'actual_physical_root_receipt','')<>
                coalesce(i.payload_json->>
                  'expected_physical_root_receipt','')
            or coalesce(i.processor_result->>
              'actual_ordered_input_root','')<>
                coalesce(i.payload_json->>'expected_ordered_input_root','')
            or encode(digest(jsonb_build_object(
                'receipt_contract','DOCUMENT_ROOT_RECEIPT_V3',
                'logical_root',i.processor_result->>
                  'actual_logical_root_receipt',
                'physical_root',i.processor_result->>
                  'actual_physical_root_receipt',
                'ordered_input_root',i.processor_result->
                  'actual_ordered_input_root',
                'page_count',case when coalesce(
                    i.processor_result->>'actual_page_count','')~'^[1-9][0-9]{0,8}$'
                  then(i.processor_result->>'actual_page_count')::integer end,
                'output_sha256',i.processor_result->>'verified_candidate_sha256')::text,
              'sha256'),'hex')<>
                coalesce(i.payload_json->>'root_merge_receipt_identity','')
            or coalesce(i.processor_result->>
              'verified_candidate_sha256','')<>
                coalesce(i.payload_json->>'candidate_sha256','')
            or coalesce(i.processor_result->>'verified_candidate_size_bytes','')<>
                coalesce(i.payload_json->>'candidate_size_bytes','')
            or coalesce(i.processor_result->>'actual_page_count','')<>
                coalesce(i.expected_page_count::text,'')
            or coalesce(i.processor_result->>
              'verified_candidate_r2_key','')<>
                coalesce(i.payload_json->>'candidate_r2_key','')
            or coalesce(i.payload_json->>'resolved_input_coverage_hash','')<>
              coalesce(i.payload_json->>'expected_coverage_hash','')
            or coalesce(i.processor_result->>'assembled_input_coverage_hash','')<>
              coalesce(i.payload_json->>'expected_coverage_hash','')
            or coalesce(i.processor_result->>'assembled_input_count','')!~
              '^[0-9]{1,9}$'
            or case when coalesce(
                i.processor_result->>'assembled_input_count','')~'^[0-9]{1,9}$'
              then(i.processor_result->>'assembled_input_count')::integer end
                is distinct from case when coalesce(
                  i.payload_json->>'expected_input_count','')~'^[0-9]{1,9}$'
                  then(i.payload_json->>'expected_input_count')::integer end
            or coalesce(
                i.processor_result->>'assembled_physical_input_count','')
                !~ '^[0-9]{1,9}$'
            or jsonb_typeof(i.processor_result->'actual_input_receipts')<>'array'
            or case when coalesce(
                i.processor_result->>'assembled_physical_input_count','')
                  ~'^[0-9]{1,9}$'
              then(i.processor_result->>
                'assembled_physical_input_count')::integer end
                is distinct from case when coalesce(
                  i.payload_json->>'expected_physical_input_count','')
                    ~'^[0-9]{1,9}$'
                  then(i.payload_json->>
                    'expected_physical_input_count')::integer end
            or coalesce(
                i.processor_result->>'assembled_physical_input_hash','')<>
              coalesce(i.payload_json->>'expected_physical_input_hash','')
            or coalesce((
              select encode(digest(string_agg(concat_ws('|',
                receipt.value->>'logical_ordinal',
                receipt.value->>'physical_part_no',
                receipt.value->>'r2_key',
                receipt.value->>'sha256'),'||'
                 order by case when coalesce(
                     receipt.value->>'logical_ordinal','')~'^[0-9]{1,9}$'
                     then(receipt.value->>'logical_ordinal')::integer end,
                   case when coalesce(
                     receipt.value->>'physical_part_no','')
                       ~'^[1-9][0-9]{0,8}$'
                     then(receipt.value->>'physical_part_no')::integer end),
                'sha256'),'hex')
              from jsonb_array_elements(
                case when jsonb_typeof(
                    i.processor_result->'actual_input_receipts')='array'
                  then i.processor_result->'actual_input_receipts'
                  else '[]'::jsonb end) receipt(value)
              where coalesce(receipt.value->>'logical_ordinal','')~'^[0-9]{1,9}$'
                and coalesce(receipt.value->>'physical_part_no','')~'^[1-9][0-9]{0,8}$'
                and nullif(receipt.value->>'r2_key','') is not null
                and coalesce(receipt.value->>'sha256','')~'^[0-9a-f]{64}$'
            ),'')<>coalesce(i.payload_json->>'expected_physical_input_hash','')
            or case when coalesce(i.processor_result->>'page_count','')
                ~ '^[0-9]{1,9}$'
              then(i.processor_result->>'page_count')::integer end
                is distinct from i.expected_page_count)
          then 'FINAL_VERIFICATION_MISMATCH'
        when i.outcome='SUCCESS'
          and coalesce(
            case when i.chunk_type='SOURCE_RENDER'
              then i.payload_json->>'source_revision' end,
            i.asset_source_revision,i.document_source_revision,
            i.payload_json->>'source_revision','')<>''
          and coalesce(i.processor_result->>'source_revision','')<>
            coalesce(
              case when i.chunk_type='SOURCE_RENDER'
                then i.payload_json->>'source_revision' end,
              i.asset_source_revision,i.document_source_revision,
              i.payload_json->>'source_revision','')
          then 'SOURCE_REVISION_MISMATCH'
      end result_error
    from identified i
  ),
  valid as materialized (
    select c.*,
      case
        when c.result_error is null then c.outcome
        when c.result_error in(
          'INVALID_OUTCOME','INVALID_RESULT_PAYLOAD','INVALID_INSPECTION_RESULT',
          'INVALID_ARTIFACT_RESULT') then 'RETRY'
        else 'FAILED'
      end effective_outcome,
      case when c.result_error is null then c.processor_error
        else jsonb_build_object('code',c.result_error,'retryable',
          c.result_error in(
            'INVALID_OUTCOME','INVALID_RESULT_PAYLOAD','INVALID_INSPECTION_RESULT',
            'INVALID_ARTIFACT_RESULT')) end effective_error
    from classified c where c.ownership_error is null
  )
  select
    coalesce((select jsonb_agg(jsonb_build_object(
      'request_no',v.request_no,'chunk_id',v.chunk_id,'operation_id',v.operation_id,
      'chunk_type',v.chunk_type,'phase',v.phase,'entity_type',v.entity_type,
      'entity_id',v.entity_id,'document_version_id',v.document_version_id,
      'document_asset_id',v.document_asset_id,
      'input_document_version_id',v.input_document_version_id,
      'payload_json',v.payload_json,'expected_page_count',v.expected_page_count,
      'expected_byte_count',v.expected_byte_count,'attempt_count',v.attempt_count,
      'max_attempts',v.max_attempts,'fence_token',v.fence_token,
      'processor_policy',v.processor_policy,
      'outcome',v.effective_outcome,'result',v.processor_result,'error',v.effective_error
    ) order by v.request_no) from valid v),'[]'::jsonb),
    coalesce((select jsonb_agg(jsonb_build_object(
      'request_no',c.request_no,'chunk_id',c.chunk_id,'status','REJECTED',
      'accepted',false,'code',c.ownership_error
    ) order by c.request_no) from classified c where c.ownership_error is not null),'[]'::jsonb)
  into v_valid_results,v_rejections;

  /* Successful inspection is explicit and always creates bounded next work. */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'='SUCCESS' and x.value->>'chunk_type'='ASSET_INSPECT'
  ),
  inspected_assets as materialized (
    update public.invoice_document_assets a
    set detected_media_type=case lower(coalesce(s.item#>>'{result,detected_media_type}',
          s.item#>>'{result,detected_kind}'))
          when 'pdf' then 'application/pdf'
          when 'jpeg' then 'image/jpeg' when 'jpg' then 'image/jpeg'
          when 'png' then 'image/png'
          else lower(coalesce(s.item#>>'{result,detected_media_type}',
            s.item#>>'{result,detected_kind}')) end,
        original_sha256=s.item#>>'{result,original_sha256}',
        original_size_bytes=(s.item#>>'{result,original_size_bytes}')::bigint,
        width_pixels=case when coalesce(s.item#>>'{result,width_pixels}','') ~ '^[0-9]{1,9}$'
          then(s.item#>>'{result,width_pixels}')::integer end,
        height_pixels=case when coalesce(s.item#>>'{result,height_pixels}','') ~ '^[0-9]{1,9}$'
          then(s.item#>>'{result,height_pixels}')::integer end,
        orientation_degrees=case
          when coalesce(s.item#>>'{result,orientation_degrees}','') ~ '^(0|90|180|270)$'
            then(s.item#>>'{result,orientation_degrees}')::integer
          else a.orientation_degrees end,
        source_page_count=case
          when coalesce(s.item#>>'{result,page_count}','') ~ '^[0-9]{1,9}$'
            then(s.item#>>'{result,page_count}')::integer
          when lower(coalesce(s.item#>>'{result,detected_media_type}',
            s.item#>>'{result,detected_kind}')) in(
              'image/jpeg','jpeg','jpg','image/png','png') then 1
          else null end,
        is_encrypted=false,status='NORMALISING',updated_at_utc=v_now,error_json=null
    from supplied s
    join public.invoice_operation_chunks c on c.id=s.chunk_id
    where a.id=c.document_asset_id
    returning a.*,c.operation_id work_operation_id,c.priority,
      s.item->'processor_policy' processor_policy,
      case when coalesce(s.item#>>'{result,estimated_decoded_bytes}','')
          ~ '^[0-9]{1,18}$'
        then(s.item#>>'{result,estimated_decoded_bytes}')::bigint else 0 end
        estimated_decoded_bytes,
      case when jsonb_typeof(s.item#>'{result,recommended_ranges}')='array'
        then s.item#>'{result,recommended_ranges}' else '[]'::jsonb end
        recommended_ranges
  ),
  adaptive_limits as materialized (
    select i.*,
      greatest(1,least(
        (i.processor_policy
          #>>'{asset,max_pdf_part_pages}')::integer,least(
        case when i.source_page_count>0
          then greatest(1,floor(
            (i.processor_policy
              #>>'{asset,max_part_input_bytes}')::numeric/
            greatest(i.original_size_bytes::numeric/i.source_page_count,1))::integer)
          else 1 end,
        case when i.estimated_decoded_bytes>0 and i.source_page_count>0
          then greatest(1,floor(
            (i.processor_policy
              #>>'{asset,max_part_estimated_decoded_bytes}')::numeric/
            greatest(i.estimated_decoded_bytes::numeric/i.source_page_count,1))::integer)
          else(i.processor_policy
            #>>'{asset,max_pdf_part_pages}')::integer end))) target_pages
    from inspected_assets i
  ),
  recommended_range_rows as materialized (
    select i.id,i.source_page_count,x.ordinality,
      case when coalesce(x.value->>'start','')~'^[0-9]{1,9}$'
        then(x.value->>'start')::integer end range_start,
      case when coalesce(x.value->>'end','')~'^[0-9]{1,9}$'
        then(x.value->>'end')::integer end range_end
    from adaptive_limits i
    cross join lateral jsonb_array_elements(i.recommended_ranges)
      with ordinality x(value,ordinality)
  ),
  recommended_range_checks as materialized (
    select i.id,
      jsonb_array_length(i.recommended_ranges)>0
      and count(r.ordinality)=jsonb_array_length(i.recommended_ranges)
      and bool_and(r.range_start is not null and r.range_end is not null
        and r.range_start between 1 and i.source_page_count
        and r.range_end between r.range_start and i.source_page_count)
      and min(r.range_start)=1 and max(r.range_end)=i.source_page_count
      and sum(r.range_end-r.range_start+1)=i.source_page_count
      and bool_and(coalesce(r.range_start=lagged.previous_end+1,
        r.range_start=1)) recommended_valid
    from adaptive_limits i
    left join lateral (
      select p.*,
        lag(p.range_end) over(order by p.range_start,p.range_end,p.ordinality)
          previous_end
      from recommended_range_rows p where p.id=i.id
    ) lagged on true
    left join recommended_range_rows r
      on r.id=lagged.id and r.ordinality=lagged.ordinality
    group by i.id,i.recommended_ranges,i.source_page_count
  ),
  ranges as materialized (
    select i.*,
      r.range_start,r.range_end,
      row_number() over(partition by i.id order by r.range_start)::integer-1 sequence_no
    from adaptive_limits i
    cross join lateral (
      select x.range_start,x.range_end
      from recommended_range_rows x
      join recommended_range_checks ck on ck.id=x.id
      where x.id=i.id and ck.recommended_valid
      union all
      select g,least(g+i.target_pages-1,i.source_page_count)
      from generate_series(1,i.source_page_count,i.target_pages) g
      where not coalesce((select ck.recommended_valid
        from recommended_range_checks ck where ck.id=i.id),false)
    ) r
  ),
  seeded as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_asset_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,
      created_at_utc,updated_at_utc)
    select r.work_operation_id,'ASSET_NORMALISE','NORMALISE',
      encode(digest(concat_ws('|','ASSET_NORMALISE',r.id::text,
        r.source_revision,r.range_start::text,r.range_end::text,
        r.processor_policy->>'policy_version','1'),'sha256'),'hex'),
      r.sequence_no,
      'DOCUMENT_ASSET',r.id,r.id,'QUEUED',r.priority,v_now,
      jsonb_build_object(
        'page_range',jsonb_build_object('start',r.range_start,'end',r.range_end),
        'detected_media_type',r.detected_media_type,
        'source_revision',r.source_revision,
        'adaptive_limits',jsonb_build_object(
          'policy_version',r.processor_policy->>'policy_version',
          'max_part_pages',r.target_pages,
          'max_input_bytes',(r.processor_policy
            #>>'{asset,max_part_input_bytes}')::bigint,
          'max_estimated_decoded_bytes',(r.processor_policy
            #>>'{asset,max_part_estimated_decoded_bytes}')::bigint)),
      r.range_end-r.range_start+1,
      greatest(1,ceil(r.original_size_bytes::numeric*
        (r.range_end-r.range_start+1)/greatest(r.source_page_count,1)))::bigint,
      o.control_version,v_now,v_now
    from ranges r join public.invoice_operations o on o.id=r.work_operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning id
  ),
  completed_inspection as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',
        result_json=s.item->'result',error_json=null,
        completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from supplied s where c.id=s.chunk_id
    returning c.id
  )
  select count(*) into v_ignored from completed_inspection;

  /* All other validated successful artifacts become immutable chunk results. */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'='SUCCESS'
      and x.value->>'chunk_type'<>'ASSET_INSPECT'
  ),
  completed as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',result_json=s.item->'result',error_json=null,
        actual_page_count=(s.item#>>'{result,page_count}')::integer,
        actual_byte_count=(s.item#>>'{result,size_bytes}')::bigint,
        completed_at_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from supplied s where c.id=s.chunk_id
    returning c.id
  )
  select count(*) into v_ignored from completed;

  /*
   * The attachment index is stabilised within the frozen policy's bounded
   * deterministic pass budget.
   * A new pass is inserted before the prior pass is typed as replaced, so the
   * old pass remains current if creation of its replacement fails.
   */
  with index_results as materialized (
    select c.*,o.control_version,o.config_json->'processor_policy'
      processor_policy
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    join public.invoice_operations o on o.id=c.operation_id
    where x->>'outcome'='SUCCESS' and c.chunk_type='SOURCE_RENDER'
      and c.status='COMPLETE'
      and c.payload_json->>'render_kind'='ATTACHMENT_INDEX'
  ),
  next_pass_source as materialized (
    select r.*,
      (r.payload_json->>'layout_pass')::integer+1 next_pass,
      r.actual_page_count expected_index_page_count
    from index_results r
    where r.payload_json->>'layout_phase'='MEASURE'
       or(r.payload_json->>'layout_phase'='FINAL'
          and r.actual_page_count is distinct from
            (r.payload_json->>'expected_index_page_count')::integer
          and(r.payload_json->>'layout_pass')::integer<
            (r.processor_policy#>>
              '{attachment_index,max_render_passes}')::integer)
  ),
  final_attachments as materialized (
    select n.id prior_chunk_id,n.operation_id,n.document_version_id,
      n.sequence_no,n.priority,n.plan_generation,n.control_version,
      n.next_pass,n.expected_index_page_count,n.processor_policy,
      n.payload_json,calculated.attachments,
      calculated.source_display_count,calculated.matched_display_count,
      encode(digest(coalesce(n.payload_json->'pagination_stream',
        '[]'::jsonb)::text,'sha256'),'hex') pagination_stream_hash
    from next_pass_source n
    cross join lateral (
      select coalesce(jsonb_agg(
        jsonb_build_object(
          'row_id',coalesce(
            nullif(display_row.value->>'logical_source_key',''),
            nullif(display_row.value->>'row_id','')),
          'attachment_number',coalesce(
            (display_row.value->>'attachment_number')::integer,
            display_row.display_no::integer),
          'worker',coalesce(
            nullif(display_row.value->>'worker',''),
            nullif(display_row.value->>'source','')),
          'week_or_date',nullif(display_row.value->>'week_or_date',''),
          'document_type',coalesce(
            nullif(display_row.value->>'document_type',''),
            nullif(display_row.value->>'label',''),
            nullif(display_row.value->>'input_type','')),
          'evidence_description',coalesce(
            nullif(display_row.value->>'evidence_description',''),
            nullif(display_row.value->>'label','')),
          'reference',nullif(display_row.value->>'reference',''),
          'page_count',(display_row.value->>'page_count')::integer,
          'start_page',1+coalesce((
            select sum(case
              when preceding.value->>'input_type'='ATTACHMENT_INDEX'
                then n.expected_index_page_count
              else (preceding.value->>'page_count')::integer end)
            from jsonb_array_elements(
              case when jsonb_typeof(n.payload_json->'pagination_stream')='array'
                then n.payload_json->'pagination_stream'
                else '[]'::jsonb end) with ordinality
              preceding(value,stream_no)
            where preceding.stream_no<target.stream_no),0))
        order by target.stream_no,
          display_row.value->>'input_chunk_id'),'[]'::jsonb) attachments,
        jsonb_array_length(case
          when jsonb_typeof(n.payload_json->'display_rows')='array'
            then n.payload_json->'display_rows'
          when jsonb_typeof(n.payload_json->'attachments')='array'
            then n.payload_json->'attachments'
          else '[]'::jsonb end)::integer source_display_count,
        count(*)::integer matched_display_count
      from jsonb_array_elements(
        case when jsonb_typeof(n.payload_json->'display_rows')='array'
          then n.payload_json->'display_rows'
          when jsonb_typeof(n.payload_json->'attachments')='array'
          then n.payload_json->'attachments'
          else '[]'::jsonb end) with ordinality
        display_row(value,display_no)
      join lateral (
        select stream.stream_no
        from jsonb_array_elements(
          case when jsonb_typeof(n.payload_json->'pagination_stream')='array'
            then n.payload_json->'pagination_stream'
            else '[]'::jsonb end) with ordinality stream(value,stream_no)
        where stream.value->>'logical_source_key'=coalesce(
            display_row.value->>'logical_source_key',
            display_row.value->>'row_id')
          and coalesce(stream.value->>'is_displayed_attachment','false')='true'
        order by stream.stream_no
        limit 1
      ) target on true
    ) calculated
  ),
  replacement_index_passes as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select f.operation_id,'SOURCE_RENDER','ATTACHMENT_INDEX_FINAL',
      encode(digest(concat_ws('|','ATTACHMENT_INDEX',
        f.document_version_id::text,f.sequence_no::text,
        f.payload_json->>'source_chunk_key','FINAL',f.next_pass::text,
        f.expected_index_page_count::text,
        encode(digest(f.attachments::text,'sha256'),'hex'),
        f.payload_json->>'template_version'),'sha256'),'hex'),
      f.plan_generation+1,f.sequence_no,'DOCUMENT',f.document_version_id,
      f.document_version_id,'QUEUED',f.priority,v_now,
      f.payload_json||jsonb_build_object(
        'layout_phase','FINAL','layout_pass',f.next_pass,
        'expected_index_page_count',f.expected_index_page_count,
        'attachments',f.attachments,
        'display_rows',f.attachments,
        'displayed_row_count',jsonb_array_length(f.attachments),
        'pagination_stream_hash',f.pagination_stream_hash,
        'expected_start_pages_hash',
          encode(digest(f.attachments::text,'sha256'),'hex'),
        'previous_layout_measurements',
          coalesce(f.payload_json->'previous_layout_measurements','[]'::jsonb)
          ||jsonb_build_array(jsonb_build_object(
            'pass',(f.payload_json->>'layout_pass')::integer,
            'phase',f.payload_json->>'layout_phase',
            'page_count',f.expected_index_page_count))),
      f.control_version,v_now,v_now
    from final_attachments f
    where f.source_display_count=f.matched_display_count
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do update set priority=greatest(
        public.invoice_operation_chunks.priority,excluded.priority),
        updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,sequence_no,plan_generation
  ),
  linked_index_passes as (
    update public.invoice_operation_chunks old
    set status='SUPERSEDED',phase='SUPERSEDED',
      replaced_by_chunk_id=fresh.id,replacement_required=true,
      completed_at_utc=v_now,failed_at_utc=null,updated_at_utc=v_now,
      result_json=coalesce(old.result_json,'{}'::jsonb)||
        jsonb_build_object('replacement_chunk_id',fresh.id)
    from replacement_index_passes fresh
    where old.id in(
      select prior_chunk_id from final_attachments f
      where f.operation_id=fresh.operation_id
        and f.document_version_id=fresh.document_version_id
        and f.sequence_no=fresh.sequence_no
        and f.plan_generation+1=fresh.plan_generation)
    returning old.id
  ),
  unstable_passes as materialized (
    update public.invoice_operation_chunks c
    set status='FAILED',phase='FAILED',failed_at_utc=v_now,
      completed_at_utc=null,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code',case
          when calc.prior_chunk_id is not null
            and calc.source_display_count<>calc.matched_display_count
            then 'ATTACHMENT_INDEX_PAGINATION_MAPPING_INVALID'
          else 'ATTACHMENT_INDEX_LAYOUT_UNSTABLE' end,
        'retryable',false,'max_layout_passes',
          (r.processor_policy#>>
            '{attachment_index,max_render_passes}')::integer,
        'expected_page_count',
          (c.payload_json->>'expected_index_page_count')::integer,
        'actual_page_count',c.actual_page_count,
        'measurements',
          coalesce(c.payload_json->'previous_layout_measurements','[]'::jsonb)
          ||jsonb_build_array(jsonb_build_object(
            'pass',(r.payload_json->>'layout_pass')::integer,
            'phase','FINAL','page_count',c.actual_page_count)))
    from index_results r
    left join final_attachments calc on calc.prior_chunk_id=r.id
    where c.id=r.id
      and(
        (calc.prior_chunk_id is not null
          and calc.source_display_count<>calc.matched_display_count)
        or(
          r.payload_json->>'layout_phase'='FINAL'
          and(r.payload_json->>'layout_pass')::integer=
            (r.processor_policy#>>
              '{attachment_index,max_render_passes}')::integer
          and r.actual_page_count is distinct from
            (r.payload_json->>'expected_index_page_count')::integer))
    returning c.id,c.operation_id,c.document_version_id,
      c.payload_json->>'source_chunk_key' source_chunk_key,c.error_json
  ),
  blocked_index_dependencies as (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
      completed_at_utc=null,updated_at_utc=v_now,
      error_json=u.error_json
    from unstable_passes u
    where d.operation_id=u.operation_id
      and d.document_version_id=u.document_version_id
      and d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and d.payload_json->>'source_chunk_key'=u.source_chunk_key
    returning d.document_version_id
  ),
  failed_index_versions as (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
      'code','ATTACHMENT_INDEX_LAYOUT_UNSTABLE','retryable',false)
    where v.id in(select document_version_id
      from blocked_index_dependencies)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  )
  select count(*) into v_ignored from linked_index_passes;

  /* Finalise an asset only after every adaptive output part is complete. */
  with affected as materialized (
    select distinct c.document_asset_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS' and c.chunk_type='ASSET_NORMALISE'
  ),
  current_normalise as materialized (
    select c.*
    from private._invoice_current_chunks_batch(
      null,null,(select array_agg(document_asset_id) from affected),10000) g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where g.replacement_chain_status='VALID'
      and c.chunk_type='ASSET_NORMALISE'
  ),
  ready as materialized (
    select a.id,
      jsonb_agg(jsonb_build_object(
        'sequence_no',c.sequence_no,'r2_key',c.result_json->>'r2_key',
        'sha256',c.result_json->>'sha256','size_bytes',c.actual_byte_count,
        'page_count',c.actual_page_count,'page_range',c.payload_json->'page_range')
        order by c.sequence_no) manifest,
      sum(c.actual_byte_count)::bigint bytes,
      sum(c.actual_page_count)::integer pages,
      min(c.result_json->>'sha256') filter(where c.sequence_no=0) single_hash,
      encode(digest(jsonb_agg(jsonb_build_object(
        'sequence_no',c.sequence_no,'r2_key',c.result_json->>'r2_key',
        'sha256',c.result_json->>'sha256','size_bytes',c.actual_byte_count,
        'page_count',c.actual_page_count,'page_range',c.payload_json->'page_range')
        order by c.sequence_no)::text,'sha256'),'hex') manifest_hash
    from affected x
    join public.invoice_document_assets a on a.id=x.document_asset_id
    join current_normalise c on c.document_asset_id=a.id
    group by a.id
    having count(*)>0 and bool_and(c.status='COMPLETE')
      and bool_and(c.actual_byte_count>0 and c.actual_page_count>0
        and coalesce(c.result_json->>'r2_key','')<>''
        and coalesce(c.result_json->>'sha256','')<>'')
      and min((c.payload_json#>>'{page_range,start}')::integer)=1
      and max((c.payload_json#>>'{page_range,end}')::integer)=a.source_page_count
      and sum((c.payload_json#>>'{page_range,end}')::integer
        -(c.payload_json#>>'{page_range,start}')::integer+1)=a.source_page_count
      and sum(c.actual_page_count)=a.source_page_count
      and not exists(
        select 1
        from (
          select
            (c2.payload_json#>>'{page_range,start}')::integer range_start,
            lag((c2.payload_json#>>'{page_range,end}')::integer)
              over(order by
                (c2.payload_json#>>'{page_range,start}')::integer,
                (c2.payload_json#>>'{page_range,end}')::integer,
                c2.sequence_no,c2.id) previous_end
          from current_normalise c2
          where c2.document_asset_id=a.id
            and coalesce(c2.payload_json#>>'{page_range,start}','')
              ~'^[0-9]{1,9}$'
            and coalesce(c2.payload_json#>>'{page_range,end}','')
              ~'^[0-9]{1,9}$'
        ) ordered_ranges
        where range_start<>coalesce(previous_end+1,1))
  ),
  updated_assets as materialized (
    update public.invoice_document_assets a
    set normalised_manifest_json=case when jsonb_array_length(r.manifest)=1
          then '[]'::jsonb else r.manifest end,
        normalised_r2_key=case when jsonb_array_length(r.manifest)=1
          then r.manifest->0->>'r2_key' else null end,
        normalised_sha256=case when jsonb_array_length(r.manifest)=1
          then r.single_hash else null end,
        normalised_manifest_hash=case when jsonb_array_length(r.manifest)>1
          then r.manifest_hash else null end,
        normalised_size_bytes=r.bytes,
        normalised_page_count=r.pages,status='READY',ready_at_utc=v_now,
        updated_at_utc=v_now,error_json=null
    from ready r where a.id=r.id
    returning a.*
  ),
  evidence_ready as (
    update public.timesheet_evidence e
    set document_asset_id=a.id,
        source_revision=a.source_revision,
        processing_state='READY',
        processing_error_json=null
    from updated_assets a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind,e.document_asset_id
  ),
  manual_evidence_ready as (
    update public.timesheets t
    set manual_document_asset_id=e.document_asset_id,
        manual_pdf_r2_key=null,
        document_state='QUEUED',last_document_error_json=null,updated_at=v_now
    from evidence_ready e
    join updated_assets a on a.id=e.document_asset_id
    where upper(e.kind)='TIMESHEET' and t.timesheet_id=e.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  manual_asset_ready as (
    update public.timesheets t
    set manual_document_asset_id=a.id,
        manual_pdf_r2_key=null,
        document_state='QUEUED',last_document_error_json=null,updated_at=v_now
    from updated_assets a
    where a.source_kind='MANUAL_TIMESHEET'
      and t.timesheet_id=a.source_id and t.is_current
    returning t.timesheet_id
  ),
  manual_document_targets as materialized (
    select distinct t.timesheet_id,t.document_revision,a.operation_id
      asset_operation_id,asset_op.actor_user_id,
      asset_op.config_json->'processor_policy' processor_policy
    from updated_assets a
    join public.invoice_operations asset_op on asset_op.id=a.operation_id
    join public.timesheets t on t.is_current and(
      (a.source_kind='MANUAL_TIMESHEET' and t.timesheet_id=a.source_id)
      or(a.source_kind='TIMESHEET_EVIDENCE' and exists(
        select 1 from public.timesheet_evidence e
        where e.id=a.source_id and e.timesheet_id=t.timesheet_id
          and upper(coalesce(e.kind,''))='TIMESHEET')))
    where not exists(
      select 1 from public.invoice_document_versions v
      where v.entity_type='TIMESHEET' and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.template_version='timesheet-professional-v1'
        and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY'))
  ),
  inserted_manual_document_operations as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,chunk_count,
      control_version,change_seq,created_at_utc,updated_at_utc)
    select t.asset_operation_id,'BUILD_DOCUMENT','TIMESHEET',t.timesheet_id,
      t.actor_user_id,
      encode(digest(concat_ws('|','BUILD_DOCUMENT','TIMESHEET',
        t.timesheet_id::text,t.document_revision::text,
        'timesheet-professional-v1'),'sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',800,t.document_revision::text,
      'timesheet-professional-v1',
      jsonb_build_object('entity_type','TIMESHEET',
        'entity_id',t.timesheet_id,'purpose','TIMESHEET',
        'source_revision',t.document_revision::text,
        'template_version','timesheet-professional-v1',
        'reason','MANUAL_ASSET_READY'),
      jsonb_build_object('processor_policy',t.processor_policy),
      jsonb_build_object(
        'status_message','Manual timesheet document queued'),
      1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    from manual_document_targets t
    on conflict do nothing
    returning *
  ),
  selected_manual_document_operations as materialized (
    select t.timesheet_id,t.document_revision,
      coalesce(created.id,existing.id) operation_id,
      coalesce(created.control_version,existing.control_version)
        control_version
    from manual_document_targets t
    left join inserted_manual_document_operations created
      on created.entity_id=t.timesheet_id
        and created.source_revision=t.document_revision::text
    left join lateral(
      select o.id,o.control_version
      from public.invoice_operations o
      where o.operation_type='BUILD_DOCUMENT'
        and o.entity_type='TIMESHEET' and o.entity_id=t.timesheet_id
        and o.source_revision=t.document_revision::text
        and o.template_version='timesheet-professional-v1'
        and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      order by o.created_at_utc desc,o.id desc limit 1
    ) existing on created.id is null
    where coalesce(created.id,existing.id) is not null
  ),
  inserted_manual_document_versions as materialized (
    insert into public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,
      manifest_hash,created_at_utc)
    select 'TIMESHEET',s.timesheet_id,'TIMESHEET',s.operation_id,
      s.document_revision::text,'timesheet-professional-v1','PLANNING',
      '{}'::jsonb,encode(digest('{}','sha256'),'hex'),'[]'::jsonb,
      encode(digest('[]','sha256'),'hex'),v_now
    from selected_manual_document_operations s
    on conflict do nothing
    returning *
  ),
  selected_manual_document_versions as materialized (
    select s.*,coalesce(created.id,existing.id) document_version_id
    from selected_manual_document_operations s
    left join inserted_manual_document_versions created
      on created.operation_id=s.operation_id
    left join public.invoice_document_versions existing
      on created.id is null
      and existing.entity_type='TIMESHEET'
      and existing.entity_id=s.timesheet_id
      and existing.purpose='TIMESHEET'
      and existing.source_revision=s.document_revision::text
      and existing.template_version='timesheet-professional-v1'
      and existing.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY')
  ),
  inserted_manual_document_plans as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,
      run_after_utc,payload_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select s.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',
        s.document_version_id::text,s.document_revision::text,
        'timesheet-professional-v1','1'),'sha256'),'hex'),
      0,0,'TIMESHEET',s.timesheet_id,s.document_version_id,
      'QUEUED',800,v_now,
      jsonb_build_object('purpose','TIMESHEET',
        'source_revision',s.document_revision::text,
        'template_version','timesheet-professional-v1',
        'reason','MANUAL_ASSET_READY'),
      s.control_version,v_now,v_now
    from selected_manual_document_versions s
    where s.document_version_id is not null
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do nothing
    returning operation_id,document_version_id
  ),
  manual_document_pointer_updates as (
    update public.timesheets t
    set active_document_operation_id=s.operation_id,
      document_state=case when v.status='READY' then 'READY' else 'QUEUED' end,
      current_document_version_id=case when v.status='READY'
        then v.id else t.current_document_version_id end,
      updated_at=v_now
    from selected_manual_document_versions s
    join public.invoice_document_versions v
      on v.id=s.document_version_id
    where t.timesheet_id=s.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  released_dependencies as materialized (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        result_json=jsonb_build_object(
          'asset_id',a.id,'source_revision',a.source_revision,
          'normalised_r2_key',a.normalised_r2_key,
          'normalised_manifest',a.normalised_manifest_json,
          'parts',a.normalised_manifest_json,
          'sha256',a.normalised_sha256,
          'normalised_manifest_hash',a.normalised_manifest_hash,
          'size_bytes',a.normalised_size_bytes,
          'page_count',a.normalised_page_count),
        expected_page_count=a.normalised_page_count,
        actual_page_count=a.normalised_page_count,
        expected_byte_count=a.normalised_size_bytes,
        actual_byte_count=a.normalised_size_bytes,error_json=null
    from updated_assets a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and(
        d.status='WAITING'
        or(d.status='BLOCKED'
          and d.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'))
    returning d.id,d.operation_id,d.document_version_id
  ),
  reset_document_versions as (
    update public.invoice_document_versions v
    set status='WAITING_FOR_INPUTS',error_json=null
    where v.id in(select document_version_id from released_dependencies)
      and v.status='FAILED'
      and v.error_json->>'code'='DOCUMENT_DEPENDENCY_PERMANENT_FAILURE'
    returning v.id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.status in('QUEUED','WAITING','RETRY_WAIT','BLOCKED')
      and p.document_version_id in(
        select document_version_id from released_dependencies)
    returning p.id
  ),
  released_operation_updates as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(distinct d.operation_id)
        from released_dependencies d),array[]::uuid[]),
      v_now,true) r
  )
  select count(*) into v_ignored from released_dependencies;

  /*
   * A terminal set of normalisation parts that does not cover the registered
   * source exactly is a permanent asset failure.  It must never leave the
   * asset in NORMALISING with every worker chunk complete.
   */
  with affected as materialized (
    select distinct c.document_asset_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS' and c.chunk_type='ASSET_NORMALISE'
  ),
  current_normalise as materialized (
    select c.*
    from private._invoice_current_chunks_batch(
      null,null,(select array_agg(document_asset_id) from affected),10000) g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where g.replacement_chain_status='VALID'
      and c.chunk_type='ASSET_NORMALISE'
  ),
  coverage_failures as materialized (
    update public.invoice_document_assets a
    set status='FAILED',updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','ASSET_RANGE_COVERAGE_MISMATCH',
          'class','PERMANENT','retryable',false,
          'source_page_count',a.source_page_count)
    where a.id in(select document_asset_id from affected)
      and a.status='NORMALISING'
      and exists(
        select 1 from current_normalise n
        where n.document_asset_id=a.id)
      and not exists(
        select 1 from current_normalise n
        where n.document_asset_id=a.id and n.status<>'COMPLETE')
    returning a.*
  ),
  evidence_failed as (
    update public.timesheet_evidence e
    set processing_state='FAILED',processing_error_json=a.error_json
    from coverage_failures a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind
  ),
  manual_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,
        updated_at=v_now
    from coverage_failures a
    where t.is_current and(
       (a.source_kind='MANUAL_TIMESHEET' and t.timesheet_id=a.source_id)
       or(a.source_kind='TIMESHEET_EVIDENCE'
          and exists(select 1 from evidence_failed e
            where e.id=a.source_id and upper(e.kind)='TIMESHEET'
              and e.timesheet_id=t.timesheet_id)))
    returning t.timesheet_id
  ),
  blocked_inputs as materialized (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        updated_at_utc=v_now,error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'asset_error',a.error_json,'document_asset_id',a.id)
    from coverage_failures a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and d.status in('WAITING','BLOCKED')
    returning d.operation_id,d.document_version_id
  ),
  failed_versions as materialized (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
      'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
      'asset_code','ASSET_RANGE_COVERAGE_MISMATCH')
    where v.id in(select document_version_id from blocked_inputs)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,
        updated_at_utc=v_now,lease_owner=null,lease_token=null,
        lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id in(select id from failed_versions)
      and p.status in('WAITING','RETRY_WAIT','BLOCKED')
    returning p.id
  ),
  issue_block as (
    update public.invoice_operation_chunks i
    set status='BLOCKED',phase='WAIT_DOCUMENT',failed_at_utc=v_now,
        updated_at_utc=v_now,error_json=jsonb_build_object(
          'code','FINAL_DOCUMENT_PERMANENT_FAILURE',
          'document_version_id',v.id,
          'document_error','ASSET_RANGE_COVERAGE_MISMATCH')
    from failed_versions v
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end)=v.id
    returning i.id
  )
  select count(*) into v_ignored from coverage_failures;

  /* Source/core render dependencies may also be shared across child operations. */
  with rendered as materialized (
    select c.*
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where x->>'outcome'='SUCCESS'
      and c.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
      and c.status='COMPLETE'
  ),
  released as materialized (
    update public.invoice_operation_chunks d
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
        result_json=r.result_json,actual_page_count=r.actual_page_count,
        actual_byte_count=r.actual_byte_count,error_json=null
    from rendered r
    where d.chunk_type='DOCUMENT_INPUT' and d.status='WAITING'
      and d.document_version_id=r.document_version_id
      and(
        d.payload_json->>'source_chunk_key'=r.payload_json->>'source_chunk_key'
        or(d.entity_type=r.entity_type and d.entity_id=r.entity_id
          and d.payload_json->>'input_type'=case
            when r.chunk_type='INVOICE_CORE_RENDER' then 'INVOICE_CORE'
            else r.payload_json->>'render_kind' end))
    returning d.operation_id,d.document_version_id
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN' and p.status in('WAITING','RETRY_WAIT')
      and p.document_version_id in(select document_version_id from released)
    returning p.id
  )
  select count(*) into v_ignored from released;

  /*
   * Verification is authoritative only when it agrees with the independently
   * frozen manifest hash, coverage hash, order and expected page count.
   */
  with verified as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,x->'result' result
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    join public.invoice_document_versions v on v.id=c.document_version_id
    where x->>'outcome'='SUCCESS' and c.chunk_type='DOCUMENT_VERIFY'
      and c.status='COMPLETE'
      and x#>>'{result,parse_verified}'='true'
      and x#>>'{result,manifest_coverage_verified}'='true'
      and x#>>'{result,ordering_verified}'='true'
      and x#>>'{result,manifest_hash}'=v.manifest_hash
      and c.payload_json->>'resolved_input_coverage_hash'=
        c.payload_json->>'expected_coverage_hash'
      and x#>>'{result,assembled_input_coverage_hash}'=
        c.payload_json->>'expected_coverage_hash'
      and coalesce(x#>>'{result,assembled_input_count}','')~'^[0-9]{1,9}$'
      and coalesce(c.payload_json->>'expected_input_count','')~'^[0-9]{1,9}$'
      and(x#>>'{result,assembled_input_count}')::integer=
        (c.payload_json->>'expected_input_count')::integer
      and coalesce(x#>>'{result,assembled_physical_input_count}','')
        ~'^[0-9]{1,9}$'
      and coalesce(c.payload_json->>'expected_physical_input_count','')
        ~'^[0-9]{1,9}$'
      and(x#>>'{result,assembled_physical_input_count}')::integer=
        (c.payload_json->>'expected_physical_input_count')::integer
      and x#>>'{result,assembled_physical_input_hash}'=
        c.payload_json->>'expected_physical_input_hash'
      and x#>>'{result,actual_logical_root_receipt}'=
        c.payload_json->>'expected_logical_root_receipt'
      and x#>>'{result,actual_physical_root_receipt}'=
        c.payload_json->>'expected_physical_root_receipt'
      and x#>>'{result,actual_ordered_input_root}'=
        c.payload_json->>'expected_ordered_input_root'
      and x#>>'{result,root_merge_receipt_identity}'=
        c.payload_json->>'root_merge_receipt_identity'
      and x#>>'{result,verified_candidate_sha256}'=
        c.payload_json->>'candidate_sha256'
      and x#>>'{result,verified_candidate_r2_key}'=
        c.payload_json->>'candidate_r2_key'
      and x#>>'{result,verified_candidate_size_bytes}'=c.payload_json->>'candidate_size_bytes'
      and (
        v.entity_type<>'INVOICE'
        or (
          c.payload_json#>>'{final_merge_receipt,page_numbering_applied}'='true'
          and c.payload_json#>>'{final_merge_receipt,page_numbering_contract}'
            in('FINAL_MERGE_GLOBAL_V1','FINAL_MERGE_SELECTIVE_V2')
          and (
            c.payload_json#>>'{final_merge_receipt,page_numbering_contract}'
              <>'FINAL_MERGE_SELECTIVE_V2'
            or jsonb_typeof(c.payload_json#>
              '{final_merge_receipt,page_numbering_excluded_pages}')='array'
          )
        )
      )
      and coalesce(x#>>'{result,actual_page_count}','')~'^[0-9]{1,9}$'
      and(x#>>'{result,actual_page_count}')::integer=c.expected_page_count
  ),
  ready_versions as materialized (
    update public.invoice_document_versions v
    set status='READY',r2_key=q.result->>'verified_candidate_r2_key',
        sha256=q.result->>'verified_candidate_sha256',
        size_bytes=(q.result->>'verified_candidate_size_bytes')::bigint,
        page_count=(q.result->>'actual_page_count')::integer,
        core_page_count=case when coalesce(q.result->>'core_page_count','') ~ '^[0-9]{1,9}$'
          then(q.result->>'core_page_count')::integer end,
        supporting_page_count=case
          when coalesce(q.result->>'supporting_page_count','') ~ '^[0-9]{1,9}$'
          then(q.result->>'supporting_page_count')::integer end,
        ready_at_utc=v_now,verified_at_utc=v_now,error_json=null
    from verified q where v.id=q.document_version_id
    returning v.*
  ),
  invoice_preview_ready as (
    update public.invoices i
    set preview_document_version_id=v.id,document_state='READY',
        invoice_pdf_r2_key=v.r2_key,invoice_pdf_generated_at_utc=v.verified_at_utc,
        active_document_operation_id=case when i.active_document_operation_id=v.operation_id
          then null else i.active_document_operation_id end,
        last_document_error_json=null,updated_at=v_now
    from ready_versions v
    where v.entity_type='INVOICE' and v.purpose='DRAFT_PREVIEW'
      and i.id=v.entity_id and i.document_revision::text=v.source_revision
    returning i.id
  ),
  timesheet_document_ready as (
    update public.timesheets t
    set current_document_version_id=v.id,document_state='READY',
        manual_pdf_r2_key=case
          when v.snapshot_json#>>'{presentation_model,schema_version}'
              ='TIMESHEET_RENDER_MODEL_V1'
            and t.manual_document_asset_id is not null
          then v.r2_key else t.manual_pdf_r2_key end,
        generated_pdf_refs_snapshot_json=case
          when v.snapshot_json#>>'{presentation_model,schema_version}'
              ='TIMESHEET_RENDER_MODEL_V2'
          then coalesce((
            select jsonb_agg(jsonb_build_object(
              'row_key',shift.value->>'reference_row_key',
              'current_reference',shift.value->>'booking_reference')
              order by (day.value->>'display_order')::integer,
                (shift.value->>'display_order')::integer)
            from jsonb_array_elements(coalesce(
              v.snapshot_json#>'{presentation_model,week_period,days}',
              '[]'::jsonb)) day(value)
            cross join lateral jsonb_array_elements(coalesce(
              day.value->'shift_lines','[]'::jsonb)) shift(value)
            where nullif(shift.value->>'booking_reference','') is not null
          ),'[]'::jsonb)
          else t.generated_pdf_refs_snapshot_json end,
        generated_pdf_refs_sig=case
          when v.snapshot_json#>>'{presentation_model,schema_version}'
              ='TIMESHEET_RENDER_MODEL_V2'
          then v.snapshot_json#>>'{presentation_model,reference_signature}'
          else t.generated_pdf_refs_sig end,
        generated_pdf_refs_captured_at_utc=case
          when v.snapshot_json#>>'{presentation_model,schema_version}'
              ='TIMESHEET_RENDER_MODEL_V2'
          then v_now else t.generated_pdf_refs_captured_at_utc end,
        generated_pdf_at_utc=case
          when v.snapshot_json#>>'{presentation_model,schema_version}'
              ='TIMESHEET_RENDER_MODEL_V2'
          then v_now else t.generated_pdf_at_utc end,
        active_document_operation_id=case when t.active_document_operation_id=v.operation_id
          then null else t.active_document_operation_id end,
        last_document_error_json=null,updated_at=v_now
    from ready_versions v
    where v.entity_type='TIMESHEET' and v.purpose='TIMESHEET'
      and t.timesheet_id=v.entity_id and t.document_revision::text=v.source_revision
    returning t.timesheet_id
  ),
  qr_mail_release as (
    update public.mail_outbox m
    set attachments=jsonb_build_array(jsonb_build_object(
          'r2_key',v.r2_key,
          'filename','Timesheet_'||
            coalesce(t.week_ending_date::text,t.timesheet_id::text)||'.pdf',
          'sha256',v.sha256,
          'size_bytes',v.size_bytes,
          'document_version_id',v.id)),
        status='QUEUED'::public.mail_status_enum,
        scheduled_for_utc=v_now,
        next_attempt_at_utc=v_now,
        last_error=null,
        failed_at=null,
        payment_scope_json=coalesce(m.payment_scope_json,'{}'::jsonb)
          ||jsonb_build_object(
            'mail_held_until_pdf_rendered',false,
            'mail_hold_reason',null,
            'mail_delayed_for_pdf_render',false,
            'requires_pdf_render',false,
            'release_mail_after_pdf_render',false,
            'pdf_storage_key',v.r2_key,
            'pdf_sha256',v.sha256,
            'pdf_size_bytes',v.size_bytes,
            'document_version_id',v.id,
            'pdf_ready_at_utc',v_now)
    from ready_versions v
    join public.timesheets t on t.timesheet_id=v.entity_id and t.is_current
    where v.entity_type='TIMESHEET' and v.purpose='TIMESHEET'
      and v.snapshot_json#>>'{presentation_model,schema_version}'
        ='TIMESHEET_RENDER_MODEL_V2'
      and v.snapshot_json#>>'{presentation_model,form_variant}'='QR_UNSIGNED'
      and t.document_revision::text=v.source_revision
      and upper(coalesce(t.qr_status::text,''))='PENDING'
      and nullif(t.qr_scanned_at::text,'') is null
      and nullif(t.qr_signed_hash,'') is null
      and nullif(t.qr_signed_at_utc::text,'') is null
      -- Candidate PAPER email is released only by the complete-pack scheduler.
      -- The ordinary QR base-document completion must never replace its frozen
      -- workflow binding, hold state or complete-pack attachment.
      and nullif(btrim(coalesce(
        m.payment_scope_json->>'candidate_workflow_id','')),'') is null
      and not exists(
        select 1
        from public.candidate_submission_workflows candidate_workflow
        where candidate_workflow.route='PAPER'
          and candidate_workflow.state='AWAITING_PAPER_RETURN'
          and (candidate_workflow.target_timesheet_id=t.timesheet_id
            or candidate_workflow.anchor_timesheet_id=t.timesheet_id)
      )
      and m.type='TIMESHEET_QR'
      and m.context_kind='timesheets'
      and m.context_id=t.timesheet_id
      and m.status<>'SENT'::public.mail_status_enum
      and m."to"=m.payment_scope_json->>'recipient_email'
      and m.payment_scope_json->>'current_timesheet_id'=t.timesheet_id::text
      and m.payment_scope_json->>'document_version_id'=v.id::text
      and m.payment_scope_json->>'document_revision'=v.source_revision
      and m.payment_scope_json->>'template_version'=v.template_version
      and m.payment_scope_json->>'qr_token_hash'=encode(
        digest(coalesce(t.qr_token,''),'sha256'),'hex')
      and m.payment_scope_json->>'qr_payload_hash'=encode(
        digest(coalesce(t.qr_payload_json,'{}'::jsonb)::text,'sha256'),'hex')
      and m.payment_scope_json->>'week_period_hash'=
        v.snapshot_json#>>'{presentation_model,week_period_hash}'
      and m.payment_scope_json->>'reference_signature'=
        v.snapshot_json#>>'{presentation_model,reference_signature}'
      and m.payment_scope_json->>'additional_units_hash'=
        v.snapshot_json#>>'{presentation_model,additional_units_hash}'
      and m.payment_scope_json->>'presentation_settings_hash'=
        v.snapshot_json#>>'{presentation_model,presentation_settings_hash}'
    returning m.id
  ),
  issue_wake as (
    update public.invoice_operation_chunks i
    set status='QUEUED',phase='FINALISE',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,error_json=null
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end
      ) in(
        select id from ready_versions where purpose='FINAL_ISSUE')
    returning i.operation_id
  )
  select count(*) into v_ignored from ready_versions;

  /*
   * Non-success outcomes, and malformed current success results reclassified
   * above, receive one explicit transition with bounded error history.
   */
  with supplied as materialized (
    select x.value item,(x.value->>'chunk_id')::uuid chunk_id,
      upper(x.value->>'outcome') outcome,
      upper(coalesce(x.value#>>'{error,code}','PROCESSOR_FAILURE')) error_code,
      (lower(coalesce(x.value#>>'{error,transient}','false'))='true'
        or upper(coalesce(x.value#>>'{error,class}',''))='TRANSIENT'
        or upper(x.value->>'outcome')='RETRY') transient
    from jsonb_array_elements(v_valid_results) x(value)
    where x.value->>'outcome'<>'SUCCESS'
  ),
  transitioned as materialized (
    update public.invoice_operation_chunks c
    set status=case
          when s.outcome='CANCELLED' then 'CANCELLED'
          when s.outcome='SUPERSEDED' then 'SUPERSEDED'
          when s.outcome='BLOCKED' then 'BLOCKED'
          when s.transient and c.attempt_count<c.max_attempts then 'RETRY_WAIT'
          when s.transient then 'DEAD_LETTER'
          else 'FAILED' end,
        phase=case
          when s.outcome='CANCELLED' then 'CANCELLED'
          when s.outcome='SUPERSEDED' then 'SUPERSEDED'
          when s.outcome='BLOCKED' then 'BLOCKED'
          when s.transient and c.attempt_count<c.max_attempts then c.phase
          when s.transient then 'DEAD_LETTER'
          else 'FAILED' end,
        run_after_utc=case when s.transient and c.attempt_count<c.max_attempts
          then v_now+make_interval(secs=>
            least(1800,30*(2^least(greatest(c.attempt_count-1,0),6)))::integer
            +floor(random()*least(60,5*greatest(c.attempt_count,1)))::integer)
          else c.run_after_utc end,
        error_json=jsonb_build_object(
          'code',s.error_code,
          'class',case when s.transient then 'TRANSIENT' else 'PERMANENT' end,
          'retryable',s.transient and c.attempt_count<c.max_attempts,
          'details',s.item->'error',
          'history',coalesce((
            select jsonb_agg(h.value order by h.ordinality)
            from jsonb_array_elements(coalesce(c.error_json->'history','[]'::jsonb))
                 with ordinality h(value,ordinality)
            where h.ordinality>greatest(
              jsonb_array_length(coalesce(c.error_json->'history','[]'::jsonb))-6,0)
          ),'[]'::jsonb)||jsonb_build_array(jsonb_build_object(
            'code',coalesce(c.error_json->>'code','UNKNOWN'),'at_utc',v_now))),
        failed_at_utc=case
          when not(s.transient and c.attempt_count<c.max_attempts) then v_now else null end,
        completed_at_utc=case when s.outcome in('CANCELLED','SUPERSEDED') then v_now
          else c.completed_at_utc end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now
    from supplied s where c.id=s.chunk_id
    returning c.*
  ),
  asset_failures as materialized (
    update public.invoice_document_assets a
    set status=case upper(coalesce(t.error_json->>'code',''))
          when 'ASSET_MEDIA_TYPE_UNSUPPORTED' then 'UNSUPPORTED'
          when 'ASSET_PDF_ENCRYPTED' then 'UNSUPPORTED'
          when 'CORRUPT_PDF' then 'CORRUPT'
          when 'TRUNCATED_FILE' then 'CORRUPT'
          when 'EMPTY_FILE' then 'CORRUPT'
          when 'MISSING_SOURCE' then 'MISSING'
          else 'FAILED' end,
        error_json=t.error_json,updated_at_utc=v_now
    from transitioned t
    where a.id=t.document_asset_id
      and t.status in('FAILED','DEAD_LETTER','BLOCKED')
      and a.status not in('READY','SUPERSEDED')
    returning a.*
  ),
  evidence_failed as (
    update public.timesheet_evidence e
    set processing_state=a.status,processing_error_json=a.error_json
    from asset_failures a
    where a.source_kind='TIMESHEET_EVIDENCE' and e.id=a.source_id
    returning e.id,e.timesheet_id,e.kind,e.document_asset_id
  ),
  manual_evidence_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,updated_at=v_now
    from evidence_failed e
    join asset_failures a on a.id=e.document_asset_id
    where upper(e.kind)='TIMESHEET' and t.timesheet_id=e.timesheet_id and t.is_current
    returning t.timesheet_id
  ),
  manual_asset_failed as (
    update public.timesheets t
    set document_state='FAILED',last_document_error_json=a.error_json,
        updated_at=v_now
    from asset_failures a
    where a.source_kind='MANUAL_TIMESHEET'
      and t.timesheet_id=a.source_id and t.is_current
    returning t.timesheet_id
  ),
  blocked_dependencies as materialized (
    update public.invoice_operation_chunks d
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'document_asset_id',a.id,'source_kind',a.source_kind,
          'source_id',a.source_id,'asset_error',a.error_json)
    from asset_failures a
    where d.chunk_type='DOCUMENT_INPUT' and d.document_asset_id=a.id
      and d.status='WAITING'
    returning d.id,d.operation_id,d.document_version_id,d.error_json
  ),
  failed_document_versions as (
    update public.invoice_document_versions v
    set status='FAILED',error_json=jsonb_build_object(
          'code','DOCUMENT_DEPENDENCY_PERMANENT_FAILURE',
          'source_errors',coalesce((
            select jsonb_agg(d.error_json order by d.id)
            from blocked_dependencies d
            where d.document_version_id=v.id),'[]'::jsonb)),
        superseded_at_utc=null
    where v.id in(select document_version_id from blocked_dependencies)
      and v.status not in('READY','SUPERSEDED','CANCELLED')
    returning v.id,v.operation_id,v.entity_type,v.entity_id,v.purpose,v.error_json
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',phase='WAIT_FOR_INPUTS',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id in(select document_version_id from blocked_dependencies)
      and p.status in('WAITING','RETRY_WAIT','BLOCKED')
    returning p.id
  ),
  issue_block as (
    update public.invoice_operation_chunks i
    set status='BLOCKED',phase='WAIT_DOCUMENT',failed_at_utc=v_now,updated_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','FINAL_DOCUMENT_PERMANENT_FAILURE',
          'document_version_id',v.id,'document_error',v.error_json)
    from failed_document_versions v
    where i.chunk_type='ISSUE_INVOICE' and i.phase='WAIT_DOCUMENT'
      and i.status in('WAITING','RETRY_WAIT')
      and coalesce(
        case when coalesce(i.payload_json->>'document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'document_version_id')::uuid end,
        case when coalesce(i.payload_json->>'final_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(i.payload_json->>'final_document_version_id')::uuid end)=v.id
    returning i.operation_id
  ),
  blocked_operation_updates as materialized (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(distinct affected.operation_id)
        from (
          select d.operation_id from blocked_dependencies d
          union
          select i.operation_id from issue_block i
        ) affected),array[]::uuid[]),
      v_now,true) r
  )
  select count(*) into v_ignored from transitioned;

  /* Merge completion or dependency change wakes the owning plan exactly once. */
  with affected_versions as materialized (
    select distinct c.document_version_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    where c.document_version_id is not null
      and c.chunk_type in(
        'ASSET_NORMALISE','SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY')
      and c.status in('COMPLETE','FAILED','DEAD_LETTER','BLOCKED')
  ),
  plan_wake as (
    update public.invoice_operation_chunks p
    set status='QUEUED',run_after_utc=v_now,updated_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where p.chunk_type='DOCUMENT_PLAN' and p.status in('WAITING','RETRY_WAIT')
      and p.document_version_id in(select document_version_id from affected_versions)
    returning p.id
  )
  select count(*) into v_ignored from plan_wake;

  with affected as materialized (
    select distinct c.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks c on c.id=(x->>'chunk_id')::uuid
    union
    select distinct d.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks source
      on source.id=(x->>'chunk_id')::uuid
    join public.invoice_operation_chunks d
      on d.chunk_type='DOCUMENT_INPUT'
      and d.document_asset_id=source.document_asset_id
    where source.document_asset_id is not null
    union
    select distinct p.operation_id
    from jsonb_array_elements(v_valid_results) x
    join public.invoice_operation_chunks source
      on source.id=(x->>'chunk_id')::uuid
    join public.invoice_operation_chunks p
      on p.chunk_type='DOCUMENT_PLAN'
      and p.document_version_id=source.document_version_id
    where source.document_version_id is not null
  ),
  operation_updates as (
    select r.*
    from private._invoice_operation_rollup_batch(
      coalesce((select array_agg(operation_id) from affected),
        array[]::uuid[]),v_now,true) r
  )
  select count(*) into v_ignored from operation_updates;

  with valid_items as (
    select x.value item from jsonb_array_elements(v_valid_results) x(value)
  ),
  results as (
    select (item->>'request_no')::integer request_no,jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,'accepted',true,
      'run_after_utc',c.run_after_utc,'attempt_count',c.attempt_count,
      'error',c.error_json,'result',case when c.status='COMPLETE' then c.result_json end) result
    from valid_items v join public.invoice_operation_chunks c
      on c.id=(v.item->>'chunk_id')::uuid
    union all
    select (x.value->>'request_no')::integer request_no,x.value result
    from jsonb_array_elements(v_rejections) x(value)
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  return v_result;
end;
$function$;

-- invoice_work_context_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_work_context_batch(p_claims jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  if p_claims is null or jsonb_typeof(p_claims) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_claims must be an array containing 1..100 claims';
  end if;
  if jsonb_array_length(p_claims)<1
     or jsonb_array_length(p_claims)>100 then
    raise exception using errcode='22023',
      message='p_claims must be an array containing 1..100 claims';
  end if;

  with supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_claim,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version
    from jsonb_array_elements(p_claims) with ordinality x(value,ordinality)
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      coalesce((
        select array_agg(distinct c.operation_id)
        from supplied s
        join public.invoice_operation_chunks c on c.id=s.chunk_id),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      null,null,10000) g
  ),
  inspected as materialized (
    select s.*,c.operation_id,c.chunk_type,c.phase,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,c.input_document_version_id,
      c.payload_json,c.plan_generation,c.level_no,c.sequence_no,
      c.expected_page_count,c.expected_byte_count,
      c.lease_token current_lease_token,c.fence_token current_fence_token,
      c.operation_control_version current_control_version,
      c.lease_expires_at_utc,c.status current_status,
      o.control_version operation_current_control_version,o.status operation_status,
      o.config_json operation_config,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_CLAIM'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when exists(select 1 from current_graph g
          where g.operation_id=c.operation_id
            and g.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when not exists(select 1 from current_graph g
          where g.current_chunk_id=c.id
            and g.replacement_chain_status='VALID')
          then 'CHUNK_NOT_CURRENT'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'OPERATION_TERMINAL'
      end ownership_error
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
  ),
  valid as materialized (
    select i.*,a.id asset_exists,a.source_kind,a.source_id,a.source_revision asset_source_revision,
      a.original_r2_key,a.original_filename,a.declared_media_type,a.detected_media_type,
      a.original_sha256,a.original_size_bytes,
      a.orientation_degrees,a.source_page_count,a.normalised_manifest_json,
      dv.id version_exists,dv.entity_type document_entity_type,dv.entity_id document_entity_id,
      dv.purpose,dv.source_revision document_source_revision,dv.template_version,
      dv.snapshot_json,dv.snapshot_hash,
      case when dv.snapshot_json is not null then encode(digest(dv.snapshot_json::text,'sha256'),'hex') end calculated_snapshot_hash,
      case when dv.snapshot_json ? 'presentation_model' then dv.snapshot_json->'presentation_model'
           when dv.snapshot_json ? 'timesheet' then dv.snapshot_json
           else '{}'::jsonb end root_presentation_model,
      case when dv.snapshot_json ? 'presentation_model' then dv.snapshot_json#>>'{presentation_model,schema_version}'
           when dv.snapshot_json ? 'timesheet' then coalesce(dv.snapshot_json#>>'{timesheet,schema_version}',dv.snapshot_json->>'snapshot_schema_version')
           else null end root_presentation_schema_version,
      case when dv.snapshot_json ? 'presentation_model' then coalesce(dv.snapshot_json->>'presentation_model_hash', dv.snapshot_json#>>'{presentation_model,presentation_model_hash}', encode(digest((dv.snapshot_json->'presentation_model')::text,'sha256'),'hex'))
           when dv.snapshot_json ? 'timesheet' then encode(digest(dv.snapshot_json::text,'sha256'),'hex')
           else null end root_presentation_model_hash,
      dv.manifest_json,dv.manifest_hash,
      input_v.status input_document_status,input_v.r2_key input_document_r2_key,
      input_v.sha256 input_document_sha256,input_v.size_bytes input_document_size_bytes,
      input_v.page_count input_document_page_count,
      i.operation_config->'processor_policy' processor_limits
    from inspected i
    left join public.invoice_document_assets a on a.id=i.document_asset_id
    left join public.invoice_document_versions dv on dv.id=i.document_version_id
    left join public.invoice_document_versions input_v on input_v.id=i.input_document_version_id
    where i.ownership_error is null
  ),
  source_models as materialized (
    select v.chunk_id,m.value manifest_item,
      case
        when m.value->>'input_type'='ELECTRONIC_TIMESHEET' then coalesce(
          (select x.value->'render_model'
           from jsonb_array_elements(
             case when jsonb_typeof(v.snapshot_json->'timesheet_sources')='array'
               then v.snapshot_json->'timesheet_sources' else '[]'::jsonb end)
             x(value)
           where x.value->>'timesheet_id'=m.value->>'source_entity_id'
           limit 1),
          case when v.document_entity_type='TIMESHEET'
            then coalesce(v.snapshot_json->'presentation_model',v.snapshot_json) end)
        when m.value->>'input_type' in('HEALTHROSTER_SUPPORT','NHSP_SUPPORT')
          then coalesce(
            (select coalesce(x.value->'render_model',x.value)
             from jsonb_array_elements(
               case when jsonb_typeof(v.snapshot_json->'source_support')='array'
                 then v.snapshot_json->'source_support'
                 when jsonb_typeof(v.snapshot_json->'supporting_sources')='array'
                 then v.snapshot_json->'supporting_sources'
                 else '[]'::jsonb end) x(value)
             where x.value->>'import_id'=m.value->>'source_entity_id'
             limit 1),'{}'::jsonb)
        when m.value->>'input_type'='HIGHER_RATE_SUPPORT' then
          coalesce(
            case when jsonb_typeof(v.snapshot_json->'higher_rate_support')='object'
              then v.snapshot_json->'higher_rate_support' end,
            jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1',
              'rows',coalesce((
                select jsonb_agg(x.value order by x.ordinality)
                from jsonb_array_elements(
                  case when jsonb_typeof(v.snapshot_json->'lines')='array'
                    then v.snapshot_json->'lines' else '[]'::jsonb end)
                  with ordinality x(value,ordinality)
                where upper(coalesce(x.value#>>'{business_meta,line_type}',''))
                    like '%HIGHER%'
                  or coalesce(x.value->'business_meta','{}'::jsonb)?'higher_rate'
              ),'[]'::jsonb))
          )
        when m.value->>'input_type' in('ATTACHMENT_INDEX','SECTION_SEPARATOR')
          then jsonb_build_object(
            'display_label',m.value->>'display_label',
            'input_type',m.value->>'input_type',
            'manifest_ordinal',m.value->'ordinal')
        else coalesce(m.value->'frozen_model','{}'::jsonb)
      end frozen_model,
      case
        when m.value->>'input_type'='ELECTRONIC_TIMESHEET' then coalesce(
          (select x.value#>>'{render_model,schema_version}'
           from jsonb_array_elements(
             case when jsonb_typeof(v.snapshot_json->'timesheet_sources')='array'
               then v.snapshot_json->'timesheet_sources' else '[]'::jsonb end)
             x(value)
           where x.value->>'timesheet_id'=m.value->>'source_entity_id'
           limit 1),
          case when v.document_entity_type='TIMESHEET'
            then coalesce(v.snapshot_json#>>'{presentation_model,schema_version}',
              v.snapshot_json->>'schema_version') end)
        when m.value->>'input_type'='HEALTHROSTER_SUPPORT' then coalesce(
          (select coalesce(x.value#>>'{render_model,schema_version}',
            x.value->>'schema_version')
           from jsonb_array_elements(
             case when jsonb_typeof(v.snapshot_json->'source_support')='array'
               then v.snapshot_json->'source_support'
               when jsonb_typeof(v.snapshot_json->'supporting_sources')='array'
               then v.snapshot_json->'supporting_sources'
               else '[]'::jsonb end) x(value)
           where x.value->>'import_id'=m.value->>'source_entity_id'
           limit 1),
          'HEALTHROSTER_PRESENTATION_V1')
        when m.value->>'input_type'='NHSP_SUPPORT' then 'NHSP_PRESENTATION_V1'
        when m.value->>'input_type'='HIGHER_RATE_SUPPORT' then 'HIGHER_RATE_PRESENTATION_V1'
        when m.value->>'input_type'='ATTACHMENT_INDEX' then 'ATTACHMENT_INDEX_PRESENTATION_V1'
        else coalesce(m.value#>>'{frozen_model,schema_version}',m.value->>'schema_version')
      end frozen_model_schema_version,
      encode(digest((case
        when m.value->>'input_type'='ELECTRONIC_TIMESHEET' then coalesce(
          (select x.value->'render_model'
           from jsonb_array_elements(
             case when jsonb_typeof(v.snapshot_json->'timesheet_sources')='array'
               then v.snapshot_json->'timesheet_sources' else '[]'::jsonb end)
             x(value)
           where x.value->>'timesheet_id'=m.value->>'source_entity_id'
           limit 1),
          case when v.document_entity_type='TIMESHEET' then coalesce(v.snapshot_json->'presentation_model',v.snapshot_json) end)
        when m.value->>'input_type' in('HEALTHROSTER_SUPPORT','NHSP_SUPPORT') then coalesce(
          (select coalesce(x.value->'render_model',x.value)
           from jsonb_array_elements(case when jsonb_typeof(v.snapshot_json->'source_support')='array'
              then v.snapshot_json->'source_support' when jsonb_typeof(v.snapshot_json->'supporting_sources')='array'
              then v.snapshot_json->'supporting_sources' else '[]'::jsonb end) x(value)
           where x.value->>'import_id'=m.value->>'source_entity_id' limit 1),'{}'::jsonb)
        when m.value->>'input_type'='HIGHER_RATE_SUPPORT' then coalesce(
          case when jsonb_typeof(v.snapshot_json->'higher_rate_support')='object'
            then v.snapshot_json->'higher_rate_support' end,
          jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1',
            'rows',coalesce((
              select jsonb_agg(x.value order by x.ordinality)
              from jsonb_array_elements(
                case when jsonb_typeof(v.snapshot_json->'lines')='array'
                  then v.snapshot_json->'lines' else '[]'::jsonb end)
                with ordinality x(value,ordinality)
              where upper(coalesce(x.value#>>'{business_meta,line_type}',''))
                  like '%HIGHER%'
                or coalesce(x.value->'business_meta','{}'::jsonb)?'higher_rate'
            ),'[]'::jsonb))
        )
        when m.value->>'input_type' in('ATTACHMENT_INDEX','SECTION_SEPARATOR') then jsonb_build_object('display_label',m.value->>'display_label','input_type',m.value->>'input_type','manifest_ordinal',m.value->'ordinal')
        else coalesce(m.value->'frozen_model','{}'::jsonb)
      end)::text,'sha256'),'hex') frozen_model_hash
    from valid v
    left join lateral jsonb_array_elements(
      case when jsonb_typeof(v.manifest_json)='array' then v.manifest_json else '[]'::jsonb end
    ) m(value) on
      case when coalesce(v.payload_json->>'manifest_ordinal','') ~ '^[0-9]{1,9}$'
             and coalesce(m.value->>'ordinal','') ~ '^[0-9]{1,9}$'
        then (m.value->>'ordinal')::integer=(v.payload_json->>'manifest_ordinal')::integer
        else nullif(m.value->>'source_chunk_key','') is not null
             and m.value->>'source_chunk_key'=v.payload_json->>'source_chunk_key' end
    where v.chunk_type='SOURCE_RENDER'
  ),
  projected as materialized (
    select v.request_no,v.chunk_id,v.operation_id,v.chunk_type,v.phase,
      v.entity_type,v.entity_id,v.document_version_id,v.document_asset_id,
      case
        when v.processor_limits is null
          or jsonb_typeof(v.processor_limits)<>'object'
          or nullif(coalesce(
              v.processor_limits->>'policy_version',
              v.processor_limits->>'version'
            ),'') is null
          then 'PROCESSOR_POLICY_MISSING'
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') and v.asset_exists is null
          then 'ASSET_NOT_FOUND'
        when v.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER','PDF_MERGE','DOCUMENT_VERIFY')
          and v.version_exists is null then 'DOCUMENT_VERSION_NOT_FOUND'
        when v.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and(jsonb_typeof(v.snapshot_json)<>'object' or v.snapshot_json='{}'::jsonb)
          then 'FROZEN_SNAPSHOT_MISSING'
        when v.chunk_type in('SOURCE_RENDER','INVOICE_CORE_RENDER')
          and (v.snapshot_hash is null
            or v.calculated_snapshot_hash is distinct from v.snapshot_hash)
          then 'FROZEN_SNAPSHOT_HASH_MISMATCH'
        when v.chunk_type='SOURCE_RENDER' and sm.manifest_item is null
          then 'SOURCE_MANIFEST_ITEM_MISSING'
        when v.chunk_type='SOURCE_RENDER'
          and coalesce(sm.frozen_model,'{}'::jsonb)='{}'::jsonb
          then 'SOURCE_FROZEN_MODEL_MISSING'
        when v.chunk_type='INVOICE_CORE_RENDER'
          and coalesce(v.root_presentation_schema_version,'') <> 'INVOICE_RENDER_MODEL_V1'
          then 'RENDER_MODEL_SCHEMA_UNSUPPORTED'
        when v.chunk_type='SOURCE_RENDER' and (
          case coalesce(sm.manifest_item->>'render_kind',
              sm.manifest_item->>'input_type',v.payload_json->>'render_kind')
            when 'ELECTRONIC_TIMESHEET' then coalesce(sm.frozen_model->>'schema_version','')
              not in('TIMESHEET_RENDER_MODEL_V1','TIMESHEET_RENDER_MODEL_V2')
            when 'HEALTHROSTER_SUPPORT' then coalesce(sm.frozen_model->>'schema_version','')
              not in('HEALTHROSTER_PRESENTATION_V1','HEALTHROSTER_PRESENTATION_V2')
            when 'NHSP_SUPPORT' then coalesce(sm.frozen_model->>'schema_version','')<>'NHSP_PRESENTATION_V1'
            when 'HIGHER_RATE_SUPPORT' then coalesce(sm.frozen_model->>'schema_version','')<>'HIGHER_RATE_PRESENTATION_V1'
            when 'ATTACHMENT_INDEX' then false
            when 'SECTION_SEPARATOR' then false
            else false end)
          then 'RENDER_MODEL_KIND_MISMATCH'
        when v.chunk_type='SOURCE_RENDER'
          and nullif(v.payload_json->>'presentation_model_hash','') is not null
          and v.payload_json->>'presentation_model_hash' is distinct from sm.frozen_model_hash
          then 'RENDER_MODEL_HASH_MISMATCH'
        when v.chunk_type='SOURCE_RENDER'
          and nullif(v.payload_json->>'presentation_model_schema_version','') is not null
          and v.payload_json->>'presentation_model_schema_version' is distinct from sm.frozen_model_schema_version
          then 'RENDER_MODEL_SCHEMA_MISMATCH'
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
          and nullif(v.payload_json->>'source_revision','') is not null
          and v.payload_json->>'source_revision'<>v.asset_source_revision
          then 'SOURCE_REVISION_CHANGED'
        when v.chunk_type='SOURCE_RENDER'
          and nullif(v.payload_json->>'source_revision','') is not null
          and v.payload_json->>'source_revision'
            is distinct from sm.manifest_item->>'source_revision'
          then 'SOURCE_REVISION_CHANGED'
        when v.input_document_version_id is not null and v.input_document_status<>'READY'
          then 'INPUT_DOCUMENT_NOT_READY'
        when v.chunk_type='INVOICE_CORE_RENDER'
          and nullif(v.payload_json->>'presentation_model_hash','') is not null
          and v.payload_json->>'presentation_model_hash' is distinct from v.root_presentation_model_hash
          then 'RENDER_MODEL_HASH_MISMATCH'
        when v.chunk_type='INVOICE_CORE_RENDER'
          and nullif(v.payload_json->>'presentation_model_schema_version','') is not null
          and v.payload_json->>'presentation_model_schema_version' is distinct from v.root_presentation_schema_version
          then 'RENDER_MODEL_SCHEMA_MISMATCH'
        when v.chunk_type='DOCUMENT_VERIFY'
          and(coalesce(v.payload_json->>'candidate_r2_key','')=''
            or coalesce(v.payload_json->>'candidate_sha256','')='')
          then 'FINAL_CANDIDATE_MISSING'
      end context_error,
      case
        when v.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE') then jsonb_build_object(
          'processor_policy_version',coalesce(v.processor_limits->>'policy_version',v.processor_limits->>'version'),
          'original_r2_key',v.original_r2_key,
          'expected_original_r2_key',case when v.chunk_type='ASSET_NORMALISE' then v.original_r2_key end,
          'expected_original_sha256',case when v.chunk_type='ASSET_NORMALISE' then v.original_sha256 end,
          'expected_original_size_bytes',case when v.chunk_type='ASSET_NORMALISE' then v.original_size_bytes end,
          'expected_original_media_type',case when v.chunk_type='ASSET_NORMALISE' then coalesce(v.detected_media_type,v.declared_media_type) end,
          'expected_source_revision',v.asset_source_revision,
          'expected_source_page_count',case when v.chunk_type='ASSET_NORMALISE' then v.source_page_count end,
          'allowed_media_types',v.processor_limits#>'{asset,allowed_media_types}',
          'max_source_bytes',v.processor_limits#>'{asset,max_source_bytes}',
          'original_filename',v.original_filename,
          'declared_media_type',v.declared_media_type,
          'detected_media_type',v.detected_media_type,
          'source_kind',v.source_kind,'source_id',v.source_id,
          'source_revision',v.asset_source_revision,
          'orientation_degrees',v.orientation_degrees,
          'source_page_count',v.source_page_count,
          'normalised_manifest',v.normalised_manifest_json,
          'page_range',v.payload_json->'page_range',
          'output_profile',coalesce(v.processor_limits->'asset','{}'::jsonb)
            ||jsonb_build_object(
              'mime_type','application/pdf','preserve_readability',true,
              'allow_upscale',false,'reject_encrypted',true),
          'immutable_destination_prefix','invoice-assets/'||v.document_asset_id||'/'||
            v.asset_source_revision||'/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='SOURCE_RENDER' then jsonb_build_object(
          'render_kind',coalesce(sm.manifest_item->>'render_kind',
            sm.manifest_item->>'input_type',v.payload_json->>'render_kind'),
          'source_entity_type',sm.manifest_item->>'source_entity_type',
          'source_entity_id',sm.manifest_item->>'source_entity_id',
          'source_revision',sm.manifest_item->>'source_revision',
          'manifest_ordinal',sm.manifest_item->'ordinal',
          'frozen_presentation_model',coalesce(
            sm.frozen_model,'{}'::jsonb),
          'presentation_model_schema_version',sm.frozen_model_schema_version,
          'presentation_model_hash',sm.frozen_model_hash,
          'snapshot_hash',v.snapshot_hash,
          'render_variant',sm.frozen_model->>'form_variant',
          'source_template_version',sm.frozen_model->>'template_version',
          'layout_contract_version',sm.frozen_model->>'layout_contract_version',
          'one_page_required',sm.frozen_model#>'{layout,one_page_required}',
          'additional_units_hash',sm.frozen_model->>'additional_units_hash',
          'reference_signature',sm.frozen_model->>'reference_signature',
          'component_page_numbering',coalesce(
            sm.manifest_item->>'component_page_numbering','NONE'),
          'asset_dependencies',coalesce(sm.manifest_item->'asset_dependencies','[]'::jsonb),
          'attachment_index_layout',case
            when v.payload_json->>'render_kind'='ATTACHMENT_INDEX'
            then jsonb_build_object(
              'layout_phase',v.payload_json->>'layout_phase',
              'layout_pass',v.payload_json->'layout_pass',
              'max_layout_passes',v.payload_json->'max_layout_passes',
              'expected_index_page_count',
                v.payload_json->'expected_index_page_count',
               'core_page_count',v.payload_json->'core_page_count',
               'attachments',v.payload_json->'attachments',
               'display_rows',v.payload_json->'display_rows',
               'displayed_row_count',v.payload_json->'displayed_row_count',
               'pagination_stream',v.payload_json->'pagination_stream',
               'pagination_stream_hash',
                 v.payload_json->>'pagination_stream_hash',
               'expected_start_pages_hash',
                 v.payload_json->>'expected_start_pages_hash',
              'determinism',v.payload_json->'determinism',
              'prior_measurements',
                v.payload_json->'previous_layout_measurements')
            else null end,
          'template_version',coalesce(
            v.payload_json->>'template_version',
            sm.frozen_model->>'template_version',
            v.template_version),
          'processor_policy_version',coalesce(v.processor_limits->>'policy_version',v.processor_limits->>'version'),
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/source/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='INVOICE_CORE_RENDER' then jsonb_build_object(
          'render_kind','INVOICE_CORE',
          'document_entity_type',v.document_entity_type,
          'document_entity_id',v.document_entity_id,
          'purpose',v.purpose,
          'source_revision',v.document_source_revision,
          'frozen_presentation_model',coalesce(v.root_presentation_model,'{}'::jsonb),
          'presentation_model_schema_version',v.root_presentation_schema_version,
          'presentation_model_hash',v.root_presentation_model_hash,
          'snapshot_hash',v.snapshot_hash,
          'attachment_index',coalesce(v.snapshot_json->'attachment_index','[]'::jsonb),
          'component_page_numbering',case
            when v.document_entity_type='INVOICE' then 'NONE'
            else 'LOCAL' end,
          'template_version',v.template_version,
          'processor_policy_version',coalesce(v.processor_limits->>'policy_version',v.processor_limits->>'version'),
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/core/'||v.chunk_id||'/'||v.fence_token||'/')
        when v.chunk_type='PDF_MERGE' then jsonb_build_object(
          'ordered_inputs',v.payload_json->'inputs',
          'merge_level',v.level_no,'sequence_no',v.sequence_no,
          'plan_generation',v.plan_generation,
          'apply_final_page_numbers',coalesce(
            (v.payload_json->>'apply_final_page_numbers')::boolean,false),
          'page_numbering_contract',
            v.payload_json->>'page_numbering_contract',
          'page_numbering_excluded_pages',coalesce(
            v.payload_json->'page_numbering_excluded_pages','[]'::jsonb),
          'document_entity_type',v.document_entity_type,
          'document_version_id',v.document_version_id,
          'expected_page_count',v.expected_page_count,
          'expected_byte_count',v.expected_byte_count,
          'expected_ordered_input_hash',
            v.payload_json->>'ordered_input_hash',
          'expected_child_receipt_hash',
            v.payload_json->>'expected_child_receipt_hash',
          'expected_logical_receipt_root',
            v.payload_json->>'expected_logical_receipt_root',
          'expected_physical_receipt_root',
            v.payload_json->>'expected_physical_receipt_root',
          'expected_parent_receipt_hash',encode(digest(
            jsonb_build_object(
              'child_receipt_hash',
                v.payload_json->>'expected_child_receipt_hash',
              'logical_receipt_root',
                v.payload_json->>'expected_logical_receipt_root',
              'physical_receipt_root',
                v.payload_json->>'expected_physical_receipt_root',
              'expected_page_count',v.expected_page_count,
              'plan_generation',v.plan_generation)::text,
            'sha256'),'hex'),
          'required_receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
          'required_object_receipt_contract',
            'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
          'required_logical_receipt_contract',
            'LOGICAL_SOURCE_RECEIPT_V3',
          'source_revision',v.document_source_revision,
          'template_version',v.template_version,
          'receipt_evidence_requirements',jsonb_build_object(
            'hash_actual_fetched_bytes',true,
            'hash_algorithm','SHA-256',
            'parse_each_pdf',true,
            'report_actual_page_count',true,
            'report_actual_byte_count',true,
            'preserve_actual_input_order',true,
            'include_processor_and_parser_versions',true),
          'processor_policy_version',coalesce(v.processor_limits->>'policy_version',v.processor_limits->>'version'),
          'limits',coalesce(v.payload_json->'limits',
            v.processor_limits->'merge','{}'::jsonb),
          'immutable_destination_prefix','invoice-documents/'||v.document_version_id||
            '/merge/'||v.level_no||'/'||v.sequence_no||'/'||v.chunk_id||'/'||
            v.fence_token||'/')
        when v.chunk_type='DOCUMENT_VERIFY' then jsonb_build_object(
          'processor_policy_version',coalesce(v.processor_limits->>'policy_version',v.processor_limits->>'version'),
          'verification_mode','VERIFY_EXISTING_CANDIDATE',
          'final_candidate_key',v.payload_json->>'candidate_r2_key',
          'final_candidate_sha256',v.payload_json->>'candidate_sha256',
          'final_candidate_size_bytes',v.payload_json->'candidate_size_bytes',
          'expected_manifest_hash',v.manifest_hash,
          'expected_coverage_hash',v.payload_json->>'expected_coverage_hash',
          'expected_physical_input_count',
            v.payload_json->'expected_physical_input_count',
          'expected_physical_input_hash',
            v.payload_json->>'expected_physical_input_hash',
          'expected_page_count',v.expected_page_count,
          'expected_input_count',v.payload_json->'expected_input_count',
          'expected_logical_input_hash',
            v.payload_json->>'expected_coverage_hash',
          'resolved_logical_input_hash',
            v.payload_json->>'resolved_input_coverage_hash',
          'expected_physical_input_count',
            v.payload_json->'expected_physical_input_count',
          'expected_physical_input_hash',
            v.payload_json->>'expected_physical_input_hash',
          'expected_logical_source_count',
            v.payload_json->'expected_logical_source_count',
          'expected_logical_root_receipt',
            v.payload_json->>'expected_logical_root_receipt',
          'expected_physical_root_receipt',
            v.payload_json->>'expected_physical_root_receipt',
          'expected_ordered_input_root',
            v.payload_json->>'expected_ordered_input_root',
          'root_merge_receipt_identity',
            v.payload_json->>'root_merge_receipt_identity',
          'receipt_contract',v.payload_json->>'receipt_contract',
          'source_revision',v.document_source_revision,
          'template_version',v.template_version,
          'receipt_evidence_requirements',jsonb_build_object(
            'reopen_final_candidate',true,
            'verify_actual_final_hash',true,
            'verify_actual_final_page_count',true,
            'verify_root_receipt_chain',true),
          'plan_generation',v.payload_json->'plan_generation',
          'final_merge_receipt',v.payload_json->'final_merge_receipt',
          'final_merge_receipt_hash',
            v.payload_json->>'final_merge_receipt_hash',
          'verification_policy',v.processor_limits->'verify',
          'document_version_id',v.document_version_id)
        else '{}'::jsonb
      end context
    from valid v left join source_models sm on sm.chunk_id=v.chunk_id
  ),
  sized as materialized (
    select p.*,
      octet_length(p.context::text) context_size_bytes,
      case when coalesce(p.context_error,'')='' and octet_length(p.context::text)>
          case when coalesce(v.processor_limits->'context'->>v.chunk_type,'')~
              '^[0-9]{1,9}$'
            then(v.processor_limits->'context'->>v.chunk_type)::integer
            else 0 end
        then 'CONTEXT_TOO_LARGE'
        else p.context_error end sized_context_error
    from projected p join valid v on v.chunk_id=p.chunk_id
  ),
  all_results as (
    select i.request_no,jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','accepted',false,
      'code',i.ownership_error) result
    from inspected i where i.ownership_error is not null
    union all
    select p.request_no,
      case when p.sized_context_error is null then jsonb_build_object(
        'chunk_id',p.chunk_id,'operation_id',p.operation_id,
        'chunk_type',p.chunk_type,'phase',p.phase,
        'entity_type',p.entity_type,'entity_id',p.entity_id,
        'document_version_id',p.document_version_id,
        'document_asset_id',p.document_asset_id,
        'status','OK','accepted',true,
        'expected_result_identity',jsonb_build_object(
          'chunk_id',p.chunk_id,'fence_token',v.fence_token,
          'plan_generation',v.plan_generation,
          'action',p.chunk_type,'document_version_id',p.document_version_id,
          'document_asset_id',p.document_asset_id,
          'source_revision',p.context->>'source_revision',
          'template_version',p.context->>'template_version',
          'processor_policy_version',
            p.context->>'processor_policy_version',
          'immutable_destination_prefix',
            p.context->>'immutable_destination_prefix',
          'render_kind',p.context->>'render_kind',
          'presentation_model_schema_version',p.context->>'presentation_model_schema_version',
          'presentation_model_hash',p.context->>'presentation_model_hash',
          'snapshot_hash',p.context->>'snapshot_hash',
          'ordered_input_hash',coalesce(p.context->>'ordered_input_hash',p.context->>'expected_ordered_input_hash'),
          'apply_final_page_numbers',p.context->'apply_final_page_numbers',
          'page_numbering_contract',p.context->>'page_numbering_contract',
          'page_numbering_excluded_pages',
            p.context->'page_numbering_excluded_pages'),
        'context',p.context)
      else jsonb_build_object(
        'chunk_id',p.chunk_id,'operation_id',p.operation_id,
        'chunk_type',p.chunk_type,'phase',p.phase,
        'status','CONTEXT_ERROR','accepted',true,
        'permanent',p.sized_context_error<>'CONTEXT_TOO_LARGE',
        'retryable',p.sized_context_error='CONTEXT_TOO_LARGE',
        'code',p.sized_context_error,
        'context_size_bytes',p.context_size_bytes)
      end result
    from sized p join valid v on v.chunk_id=p.chunk_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from all_results;

  return v_result;
end;
$function$;

-- invoice_work_touch_batch(jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.invoice_work_touch_batch(p_touches jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  if jsonb_typeof(p_touches)<>'array'
     or jsonb_array_length(p_touches)<1
     or jsonb_array_length(p_touches)>100 then
    raise exception using errcode='22023',
      message='p_touches must be an array containing 1..100 items';
  end if;

  with recursive supplied as materialized (
    select x.ordinality::integer request_no,x.value raw_touch,
      case when coalesce(x.value->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'chunk_id')::uuid end chunk_id,
      case when coalesce(x.value->>'lease_token','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'lease_token')::uuid end lease_token,
      case when coalesce(x.value->>'fence_token','') ~ '^[0-9]{1,18}$'
        then (x.value->>'fence_token')::bigint end fence_token,
      case when coalesce(x.value->>'operation_control_version','') ~ '^[0-9]{1,18}$'
        then (x.value->>'operation_control_version')::bigint end operation_control_version,
      coalesce(x.value->'progress_patch','{}'::jsonb) patch,
      greatest(15,least(case
        when coalesce(x.value->>'lease_extension_seconds','') ~ '^[0-9]{1,4}$'
          then (x.value->>'lease_extension_seconds')::integer else 60 end,300)) extension_seconds
    from jsonb_array_elements(p_touches) with ordinality x(value,ordinality)
  ),
  ownership_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      coalesce((
        select array_agg(distinct c.operation_id)
        from supplied s
        join public.invoice_operation_chunks c on c.id=s.chunk_id),
        array['00000000-0000-0000-0000-000000000000'::uuid]),
      null,null,10000) g
  ),
  inspected as materialized (
    select s.*,c.operation_id,c.status current_status,c.lease_token current_lease_token,
      c.fence_token current_fence_token,c.operation_control_version current_control_version,
      c.lease_expires_at_utc,o.control_version operation_current_control_version,
      o.status operation_status,
      case
        when s.chunk_id is null or s.lease_token is null
          or s.fence_token is null or s.operation_control_version is null then 'INVALID_TOUCH'
        when jsonb_typeof(s.patch)<>'object' or octet_length(s.patch::text)>16384
          or lower(s.patch::text) ~
            '"(base64|file_bytes|raw_bytes|snapshot_json|manifest_json|ordered_inputs|processor_dump)"[[:space:]]*:'
          then 'INVALID_PROGRESS_PATCH'
        when c.id is null then 'CHUNK_NOT_FOUND'
        when exists(select 1 from ownership_graph g
          where g.operation_id=c.operation_id
            and g.replacement_chain_status='INVALID')
          then 'INVALID_REPLACEMENT_GRAPH'
        when not exists(select 1 from ownership_graph g
          where g.current_chunk_id=c.id
            and g.replacement_chain_status='VALID')
          then 'CHUNK_NOT_CURRENT'
        when c.status<>'RUNNING' then 'CHUNK_NOT_RUNNING'
        when c.lease_token is distinct from s.lease_token then 'LEASE_TOKEN_MISMATCH'
        when c.fence_token is distinct from s.fence_token then 'FENCE_TOKEN_MISMATCH'
        when c.operation_control_version is distinct from s.operation_control_version
          or o.control_version is distinct from s.operation_control_version then 'CONTROL_VERSION_MISMATCH'
        when c.lease_expires_at_utc is null or c.lease_expires_at_utc<=v_now then 'LEASE_EXPIRED'
        when o.status in('COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED')
          then 'OPERATION_TERMINAL'
      end rejection_code
    from supplied s
    left join public.invoice_operation_chunks c on c.id=s.chunk_id
    left join public.invoice_operations o on o.id=c.operation_id
  ),
  eligible as materialized (
    select i.*,c.progress_json old_progress,
      jsonb_strip_nulls(i.patch) effective_patch,
      exists(
        select 1
        from unnest(array[
          'status_message','pages_complete','pages_total',
          'bytes_complete','bytes_total','merge_level',
          'parts_complete','parts_total','verification_phase','phase']) k
        where jsonb_strip_nulls(i.patch)?k
          and c.progress_json->k
            is distinct from jsonb_strip_nulls(i.patch)->k
      ) material_change
    from inspected i
    join public.invoice_operation_chunks c on c.id=i.chunk_id
    where i.rejection_code is null
  ),
  updated as materialized (
    update public.invoice_operation_chunks c
    set progress_json=coalesce(c.progress_json,'{}'::jsonb)||i.effective_patch,
        lease_expires_at_utc=greatest(
          c.lease_expires_at_utc,v_now+make_interval(secs=>i.extension_seconds)),
        updated_at_utc=v_now
    from eligible i
    where c.id=i.chunk_id
    returning c.id,c.operation_id,c.lease_expires_at_utc,c.progress_json,
      i.material_change
  ),
  affected as materialized (
    select distinct operation_id from updated
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from affected),null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  phase_scope as materialized (
    select distinct on(c.operation_id)
      c.operation_id,c.chunk_type,
      case when c.chunk_type='PDF_MERGE' then c.level_no end level_no
    from current_graph g
    join public.invoice_operation_chunks c on c.id=g.current_chunk_id
    where c.status in('RUNNING','QUEUED','RETRY_WAIT','WAITING')
    order by c.operation_id,
      case c.status when 'RUNNING' then 0 when 'QUEUED' then 1
        when 'RETRY_WAIT' then 2 else 3 end,
      c.level_no desc,c.priority desc,c.updated_at_utc desc,c.id
  ),
  grouped as materialized (
    select c.operation_id,count(*)::integer total,
      count(*) filter(where c.status='COMPLETE')::integer completed,
      count(*) filter(where c.status in('FAILED','DEAD_LETTER','BLOCKED'))::integer failed,
      max(c.updated_at_utc) latest_update,
      (array_agg(c.progress_json->>'status_message'
        order by(c.progress_json?'status_message') desc,c.updated_at_utc desc,c.id))[1] status_message,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'pages_complete','')~'^[0-9]+$'
        then(c.progress_json->>'pages_complete')::bigint else 0 end)
          pages_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'pages_total','')~'^[0-9]+$'
        then(c.progress_json->>'pages_total')::bigint else 0 end)
          pages_total,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'bytes_complete','')~'^[0-9]+$'
        then(c.progress_json->>'bytes_complete')::bigint else 0 end)
          bytes_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'bytes_total','')~'^[0-9]+$'
        then(c.progress_json->>'bytes_total')::bigint else 0 end)
          bytes_total,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'parts_complete','')~'^[0-9]+$'
        then(c.progress_json->>'parts_complete')::bigint else 0 end)
          parts_complete,
      sum(case when c.chunk_type=s.chunk_type
          and(s.level_no is null or c.level_no=s.level_no)
          and coalesce(c.progress_json->>'parts_total','')~'^[0-9]+$'
        then(c.progress_json->>'parts_total')::bigint else 0 end)
          parts_total,
      max(case when coalesce(c.progress_json->>'merge_level','')~'^[0-9]+$'
        then(c.progress_json->>'merge_level')::integer end) merge_level,
      (array_agg(c.progress_json->>'verification_phase'
        order by(c.progress_json?'verification_phase') desc,
          c.updated_at_utc desc,c.id))[1] verification_phase,
      bool_or(u.material_change) material_change
    from current_graph current
    join public.invoice_operation_chunks c on c.id=current.current_chunk_id
    join phase_scope s on s.operation_id=c.operation_id
    left join updated u on u.id=c.id
    group by c.operation_id
  ),
  operation_progress as (
    update public.invoice_operations o
    set total_units=g.total,chunk_count=g.total,completed_units=g.completed,failed_units=g.failed,
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||jsonb_build_object(
          'status_message',coalesce(g.status_message,o.progress_json->>'status_message'),
          'completed_units',g.completed,'failed_units',g.failed,
          'pages_complete',g.pages_complete,'pages_total',g.pages_total,
          'bytes_complete',g.bytes_complete,'bytes_total',g.bytes_total,
          'parts_complete',g.parts_complete,'parts_total',g.parts_total,
          'merge_level',g.merge_level,'verification_phase',g.verification_phase),
        updated_at_utc=v_now,
        change_seq=case when g.material_change
          then nextval('public.invoice_operation_change_seq') else o.change_seq end
    from grouped g where o.id=g.operation_id
    returning o.id
  ),
  ancestor_scope(id) as (
    select distinct o.parent_operation_id
    from public.invoice_operations o
    join grouped g on g.operation_id=o.id and g.material_change
    where o.parent_operation_id is not null
    union
    select distinct o.parent_operation_id
    from public.invoice_operations o
    join ancestor_scope s on s.id=o.id
    where o.parent_operation_id is not null
  ),
  ancestor_updates as (
    update public.invoice_operations o
    set updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    where o.id in(select id from ancestor_scope)
      and o.status not in('CANCELLED','SUPERSEDED')
    returning o.id
  ),
  results as (
    select i.request_no,jsonb_build_object(
      'chunk_id',i.chunk_id,'status','REJECTED','accepted',false,'code',i.rejection_code) result
    from inspected i where i.rejection_code is not null
    union all
    select i.request_no,jsonb_build_object(
      'chunk_id',u.id,'status','TOUCHED','accepted',true,
      'lease_expires_at_utc',u.lease_expires_at_utc,'progress',u.progress_json) result
    from inspected i join updated u on u.id=i.chunk_id
  )
  select coalesce(jsonb_agg(result order by request_no),'[]'::jsonb)
  into v_result from results;

  return v_result;
end;
$function$;

-- invpdf_dequeue_batch_ids(integer)
CREATE OR REPLACE FUNCTION public.invpdf_dequeue_batch_ids(p_limit integer DEFAULT 10)
 RETURNS TABLE(outbox_id uuid, invoice_id uuid, reason invoice_pdf_reason_enum, attempt_count integer, next_attempt_at timestamp with time zone, created_at timestamp with time zone, force_regen boolean)
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_lim int := greatest(1, least(coalesce(p_limit, 10), 200));
begin
  return query
  with picked as (
    select o.id
    from public.invoice_pdf_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  )
  update public.invoice_pdf_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning
    o.id as outbox_id,
    o.invoice_id,
    o.reason,
    o.attempt_count,
    o.next_attempt_at,
    o.created_at,
    o.force_regen;
end;
$function$;

-- invpdf_enqueue_many(uuid[],boolean,integer)
CREATE OR REPLACE FUNCTION public.invpdf_enqueue_many(p_invoice_ids uuid[], p_force_regen boolean DEFAULT false, p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));
  v_count int := 0;
  v_i int := 0;
  v_id uuid;
begin
  if p_invoice_ids is null or coalesce(array_length(p_invoice_ids, 1), 0) = 0 then
    return 0;
  end if;

  foreach v_id in array p_invoice_ids loop
    exit when v_i >= v_lim;
    v_i := v_i + 1;

    if v_id is null then
      continue;
    end if;

    v_count := v_count + public.invpdf_enqueue_one(v_id, p_force_regen);
  end loop;

  return v_count;
end;
$function$;

-- invpdf_enqueue_one(uuid,boolean)
CREATE OR REPLACE FUNCTION public.invpdf_enqueue_one(p_invoice_id uuid, p_force_regen boolean DEFAULT false)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_reason public.invoice_pdf_reason_enum := case when p_force_regen then 'FORCE_REGEN' else 'READY_FOR_RENDER' end;
  v_updated int := 0;
  v_inserted int := 0;
begin
  if p_invoice_id is null then
    return 0;
  end if;

  -- If caller requests force regen, ensure we don't leave redundant rows behind.
  if p_force_regen then
    delete from public.invoice_pdf_outbox o
    where o.invoice_id = p_invoice_id;

    insert into public.invoice_pdf_outbox(
      invoice_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      force_regen,
      created_at
    )
    values (
      p_invoice_id,
      v_reason,
      0,
      v_now,
      null,
      true,
      v_now
    )
    on conflict (invoice_id, reason) do update
      set next_attempt_at = excluded.next_attempt_at,
          last_error      = null,
          force_regen     = true;

    return 1;
  end if;

  -- Non-force: if ANY job exists for this invoice (READY and/or FORCE),
  -- bump them to run asap (idempotent), without RETURNING-into-scalar issues.
  update public.invoice_pdf_outbox o
     set next_attempt_at = v_now,
         last_error      = null
   where o.invoice_id = p_invoice_id;

  get diagnostics v_updated = row_count;

  if coalesce(v_updated, 0) > 0 then
    return v_updated;
  end if;

  -- No job exists yet: create READY job.
  insert into public.invoice_pdf_outbox(
    invoice_id,
    reason,
    attempt_count,
    next_attempt_at,
    last_error,
    force_regen,
    created_at
  )
  values (
    p_invoice_id,
    v_reason,
    0,
    v_now,
    null,
    false,
    v_now
  )
  on conflict (invoice_id, reason) do update
    set next_attempt_at = excluded.next_attempt_at,
        last_error      = null,
        force_regen     = public.invoice_pdf_outbox.force_regen or excluded.force_regen;

  get diagnostics v_inserted = row_count;
  return coalesce(v_inserted, 0);
end;
$function$;

-- invpdf_enqueue_ready_for_render(integer)
CREATE OR REPLACE FUNCTION public.invpdf_enqueue_ready_for_render(p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 2000));
begin
  with eligible as (
    select i.id as invoice_id
    from public.invoices i
    where i.status = 'ISSUED'::public.invoice_status_enum
      and (
        i.invoice_pdf_r2_key is null
        or btrim(i.invoice_pdf_r2_key) = ''
        or i.invoice_pdf_generated_at_utc is null
        or (i.updated_at is not null and i.invoice_pdf_generated_at_utc is not null and i.updated_at > i.invoice_pdf_generated_at_utc)
      )
      -- Don't enqueue if ANY outbox row already exists for this invoice (READY or FORCE)
      and not exists (
        select 1
        from public.invoice_pdf_outbox o
        where o.invoice_id = i.id
      )
    order by i.updated_at desc nulls last
    limit v_lim
  ),
  ins as (
    insert into public.invoice_pdf_outbox(
      invoice_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      force_regen,
      created_at
    )
    select
      e.invoice_id,
      'READY_FOR_RENDER'::public.invoice_pdf_reason_enum,
      0,
      v_now,
      null,
      false,
      v_now
    from eligible e
    on conflict (invoice_id, reason) do nothing
    returning 1
  )
  select count(*) into v_ins from ins;

  return v_ins;
end;
$function$;

-- invpdf_work_fail_bulk(jsonb)
CREATE OR REPLACE FUNCTION public.invpdf_work_fail_bulk(p_rows jsonb)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_count int := 0;
  r record;
begin
  if p_rows is null then return 0; end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid as outbox_id,
      left(coalesce(elem->>'error',''), 4000) as err
    from jsonb_array_elements(p_rows) as elem
  loop
    update public.invoice_pdf_outbox o
    set last_error = r.err,
        next_attempt_at = v_now + interval '30 minutes'
    where o.id = r.outbox_id;

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$function$;

-- invpdf_work_success_bulk(uuid[])
CREATE OR REPLACE FUNCTION public.invpdf_work_success_bulk(p_ids uuid[])
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_count int := 0;
begin
  if p_ids is null or coalesce(array_length(p_ids, 1), 0) = 0 then
    return 0;
  end if;

  with gone as (
    delete from public.invoice_pdf_outbox o
    where o.id = any(p_ids)
    returning 1
  )
  select count(*) into v_count
  from gone;

  return v_count;
end;
$function$;

-- job_titles_delete_apply(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.job_titles_delete_apply(p_job_title_id uuid, p_actor_user_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_is_role boolean;
  v_in_use int;
  v_before jsonb;
BEGIN
  IF p_job_title_id IS NULL THEN
    RAISE EXCEPTION 'job_title_id is required';
  END IF;

  SELECT jsonb_build_object('id', djt.id::text, 'label', djt.label, 'parent_id', CASE WHEN djt.parent_id IS NULL THEN null ELSE djt.parent_id::text END, 'is_role', djt.is_role),
         djt.is_role
    INTO v_before, v_is_role
  FROM public.default_job_titles djt
  WHERE djt.id = p_job_title_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Job title not found';
  END IF;

  IF v_is_role THEN
    SELECT COUNT(*) INTO v_in_use
    FROM public.candidate_job_titles cjt
    WHERE cjt.job_title_id = p_job_title_id;

    IF v_in_use > 0 THEN
      RAISE EXCEPTION 'Candidates have been assigned to this role. You need to reassign the candidates before you can delete this role';
    END IF;

    DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

    PERFORM public._audit_insert(
      'job_titles',
      p_job_title_id::text,
      'DELETE_ROLE',
      v_before,
      jsonb_build_object('deleted', true),
      COALESCE(p_reason, 'DELETE_JOB_TITLE'),
      p_actor_user_id
    );

    RETURN jsonb_build_object('deleted', true, 'kind', 'role', 'id', p_job_title_id::text);
  END IF;

  -- Category: gather all descendants
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id, d1.is_role
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id, d2.is_role
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  ),
  descendants AS (
    SELECT id, is_role
    FROM tree
    WHERE id <> p_job_title_id
  ),
  role_desc AS (
    SELECT id
    FROM descendants
    WHERE is_role = true
  )
  SELECT COUNT(*)
    INTO v_in_use
  FROM public.candidate_job_titles cjt
  WHERE cjt.job_title_id IN (SELECT id FROM role_desc);

  IF v_in_use > 0 THEN
    RAISE EXCEPTION 'Candidates have been assigned to roles in this category. You need to reassign the candidates before you can delete this category';
  END IF;

  -- Delete descendants first, then category
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  )
  DELETE FROM public.default_job_titles djt
  WHERE djt.id IN (SELECT id FROM tree)
    AND djt.id <> p_job_title_id;

  DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

  PERFORM public._audit_insert(
    'job_titles',
    p_job_title_id::text,
    'DELETE_CATEGORY',
    v_before,
    jsonb_build_object('deleted', true),
    COALESCE(p_reason, 'DELETE_JOB_TITLE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'kind', 'category', 'id', p_job_title_id::text);
END;
$function$;

-- mailshot_enqueue(jsonb,jsonb,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.mailshot_enqueue(p_prepare_json jsonb, p_final_edits_json jsonb, p_delivery_timing_json jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_context_kind text;
  v_entity_type text;
  v_output_type text;

  v_template_id uuid;
  v_to_field_key text;

  v_rows jsonb;
  v_row jsonb;

  v_run_id uuid;

  v_queued int := 0;
  v_skipped int := 0;
  v_failed int := 0;

  v_skip_list jsonb := '[]'::jsonb;

  v_subject_tpl_override text;
  v_body_text_tpl_override text;
  v_body_html_tpl_override text;
  v_message_tpl_override text;

  v_cc_override text;
  v_bcc_override text;
  v_reply_to_override text;
  v_importance_override text;
  v_email_type_override text;

  v_global_attachments jsonb := '[]'::jsonb;
  v_row_attachments jsonb := '[]'::jsonb;
  v_effective_attachments jsonb := '[]'::jsonb;

  v_to text;
  v_recipient_kind text;
  v_recipient_id uuid;
  v_context_id uuid;

  v_field_values jsonb;
  v_kv record;

  v_rendered_subject text;
  v_rendered_body_text text;
  v_rendered_body_html text;
  v_rendered_message text;

  v_ref text;

  v_sms_max int := 1000;
  v_voice_max int := 1200;
  v_whatsapp_max int := 600;

  v_cfg jsonb;
  v_cfg_wati jsonb;
  v_cfg_clicksend jsonb;
  v_cfg_scheduling jsonb;

  v_provider_key text;

  v_sanitised boolean := false;
  v_truncated boolean := false;
  v_original_len int := 0;

  v_row_subject_tpl text;
  v_row_body_text_tpl text;
  v_row_body_html_tpl text;
  v_row_message_tpl text;
  v_row_cc text;
  v_row_bcc text;
  v_row_reply_to text;
  v_row_importance text;
  v_row_email_type text;
  v_row_template_content jsonb := '{}'::jsonb;
  v_wati_template_name text;
  v_wati_param_name text;

  v_delivery_timing_json jsonb := '{}'::jsonb;
  v_delivery_mode text := 'NOW';
  v_requested_timezone text := null;
  v_requested_local_text text := null;
  v_relative_minutes int := null;

  v_scheduled_for_utc timestamptz := null;
  v_next_attempt_at_utc timestamptz := null;

  v_max_future_days int := 365;
  v_allow_past_grace_minutes int := 15;
  v_scheduled_text text;

  v_prepare_counts jsonb := '{}'::jsonb;
  v_selected_count_context jsonb := '{}'::jsonb;
  v_requested_selected_count int := null;
  v_resolved_context_count int := null;
  v_prepare_row_count int := null;
  v_prepare_eligible_count int := null;
  v_prepare_skipped_count int := null;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_prepare_json is null or jsonb_typeof(p_prepare_json) <> 'object' then
    raise exception 'prepare_json object required';
  end if;

  if p_final_edits_json is not null and jsonb_typeof(p_final_edits_json) <> 'object' then
    raise exception 'final_edits_json object required';
  end if;

  if p_delivery_timing_json is not null and jsonb_typeof(p_delivery_timing_json) <> 'object' then
    raise exception 'delivery_timing_json object required';
  end if;

  v_context_kind := lower(coalesce(p_prepare_json->>'context_kind',''));
  v_entity_type := lower(coalesce(p_prepare_json->>'entity_type',''));
  v_output_type := upper(coalesce(p_prepare_json->>'output_type',''));

  if v_context_kind = '' or v_entity_type = '' or v_output_type = '' then
    raise exception 'prepare_json missing context_kind/entity_type/output_type';
  end if;

  v_rows := p_prepare_json->'rows';
  if v_rows is null or jsonb_typeof(v_rows) <> 'array' then
    raise exception 'prepare_json.rows must be array';
  end if;

  if p_prepare_json ? 'prepare_counts' and jsonb_typeof(p_prepare_json->'prepare_counts') = 'object' then
    v_prepare_counts := coalesce(p_prepare_json->'prepare_counts', '{}'::jsonb);
  else
    v_prepare_counts := '{}'::jsonb;
  end if;

  if p_prepare_json ? 'selected_count_context' and jsonb_typeof(p_prepare_json->'selected_count_context') = 'object' then
    v_selected_count_context := coalesce(p_prepare_json->'selected_count_context', '{}'::jsonb);
  else
    v_selected_count_context := '{}'::jsonb;
  end if;

  if nullif(coalesce(v_selected_count_context->>'summary_selected_count',''), '') is not null then
    v_requested_selected_count := (v_selected_count_context->>'summary_selected_count')::int;
  elsif nullif(coalesce(v_selected_count_context->>'selected_count',''), '') is not null then
    v_requested_selected_count := (v_selected_count_context->>'selected_count')::int;
  elsif nullif(coalesce(v_prepare_counts->>'requested_context_count',''), '') is not null then
    v_requested_selected_count := (v_prepare_counts->>'requested_context_count')::int;
  elsif nullif(coalesce(p_prepare_json->>'requested_context_count',''), '') is not null then
    v_requested_selected_count := (p_prepare_json->>'requested_context_count')::int;
  else
    v_requested_selected_count := jsonb_array_length(v_rows);
  end if;

  if nullif(coalesce(v_selected_count_context->>'actionable_context_count',''), '') is not null then
    v_resolved_context_count := (v_selected_count_context->>'actionable_context_count')::int;
  elsif nullif(coalesce(v_prepare_counts->>'resolved_context_count',''), '') is not null then
    v_resolved_context_count := (v_prepare_counts->>'resolved_context_count')::int;
  elsif nullif(coalesce(p_prepare_json->>'resolved_context_count',''), '') is not null then
    v_resolved_context_count := (p_prepare_json->>'resolved_context_count')::int;
  elsif nullif(coalesce(v_selected_count_context->>'selected_count',''), '') is not null then
    v_resolved_context_count := (v_selected_count_context->>'selected_count')::int;
  else
    v_resolved_context_count := jsonb_array_length(v_rows);
  end if;

  if nullif(coalesce(v_prepare_counts->>'returned_row_count',''), '') is not null then
    v_prepare_row_count := (v_prepare_counts->>'returned_row_count')::int;
  elsif nullif(coalesce(p_prepare_json->>'returned_row_count',''), '') is not null then
    v_prepare_row_count := (p_prepare_json->>'returned_row_count')::int;
  else
    v_prepare_row_count := jsonb_array_length(v_rows);
  end if;

  if nullif(coalesce(v_prepare_counts->>'eligible_count',''), '') is not null then
    v_prepare_eligible_count := (v_prepare_counts->>'eligible_count')::int;
  elsif nullif(coalesce(p_prepare_json->>'eligible_count',''), '') is not null then
    v_prepare_eligible_count := (p_prepare_json->>'eligible_count')::int;
  else
    select count(*)
    into v_prepare_eligible_count
    from jsonb_array_elements(v_rows) as jbe(value)
    where coalesce((jbe.value->>'eligible')::boolean, false) = true;
  end if;

  if nullif(coalesce(v_prepare_counts->>'skipped_count',''), '') is not null then
    v_prepare_skipped_count := (v_prepare_counts->>'skipped_count')::int;
  elsif nullif(coalesce(p_prepare_json->>'skipped_count',''), '') is not null then
    v_prepare_skipped_count := (p_prepare_json->>'skipped_count')::int;
  else
    v_prepare_skipped_count := greatest(coalesce(v_prepare_row_count, 0) - coalesce(v_prepare_eligible_count, 0), 0);
  end if;

  v_to_field_key := nullif(btrim(coalesce(p_prepare_json->>'to_field_key','')), '');

  v_template_id := null;
  if nullif(coalesce(p_prepare_json->>'document_template_id',''),'') is not null then
    v_template_id := (p_prepare_json->>'document_template_id')::uuid;
  end if;

  select sd.comms_adaptors_json
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg is not null and jsonb_typeof(v_cfg) = 'object' then
    v_cfg_wati := v_cfg->'wati';
    v_cfg_clicksend := v_cfg->'clicksend';
    v_cfg_scheduling := v_cfg->'scheduling';

    if v_cfg_wati is not null and jsonb_typeof(v_cfg_wati) = 'object' then
      v_whatsapp_max := coalesce(nullif((v_cfg_wati->>'whatsapp_max_chars')::int, 0), v_whatsapp_max);
    end if;

    if v_cfg_clicksend is not null and jsonb_typeof(v_cfg_clicksend) = 'object' then
      v_sms_max := coalesce(nullif((v_cfg_clicksend->>'sms_max_chars')::int, 0), v_sms_max);
      v_voice_max := coalesce(nullif((v_cfg_clicksend->>'voice_max_chars')::int, 0), v_voice_max);
    end if;

    if v_cfg_scheduling is not null and jsonb_typeof(v_cfg_scheduling) = 'object' then
      v_max_future_days := coalesce(nullif((v_cfg_scheduling->>'max_future_days')::int, 0), v_max_future_days);
      v_allow_past_grace_minutes := coalesce(nullif((v_cfg_scheduling->>'allow_past_grace_minutes')::int, 0), v_allow_past_grace_minutes);
    end if;
  end if;

  if p_delivery_timing_json is null or p_delivery_timing_json = '{}'::jsonb then
    v_delivery_mode := 'NOW';
  else
    v_delivery_mode := upper(btrim(coalesce(p_delivery_timing_json->>'mode','NOW')));
    if v_delivery_mode = '' then
      v_delivery_mode := 'NOW';
    end if;
  end if;

  if v_delivery_mode = 'RELATIVE_DELAY' then
    v_delivery_mode := 'AFTER_DELAY';
  end if;

  if v_delivery_mode not in ('NOW','AT_TIME','AFTER_DELAY') then
    raise exception 'delivery_timing_json.mode must be NOW, AT_TIME or AFTER_DELAY';
  end if;

  v_requested_timezone := nullif(btrim(coalesce(p_delivery_timing_json->>'requested_timezone','')), '');
  v_requested_local_text := nullif(btrim(coalesce(p_delivery_timing_json->>'requested_local_text','')), '');

  if nullif(coalesce(p_delivery_timing_json->>'relative_minutes',''),'') is not null then
    v_relative_minutes := (p_delivery_timing_json->>'relative_minutes')::int;
  else
    v_relative_minutes := null;
  end if;

  if v_delivery_mode = 'AT_TIME' then
    v_scheduled_text := nullif(btrim(coalesce(p_delivery_timing_json->>'scheduled_for_utc','')), '');
    if v_scheduled_text is null then
      raise exception 'delivery_timing_json.scheduled_for_utc required when mode=AT_TIME';
    end if;

    begin
      v_scheduled_for_utc := v_scheduled_text::timestamptz;
    exception
      when others then
        raise exception 'delivery_timing_json.scheduled_for_utc invalid';
    end;

    if v_scheduled_for_utc > (v_now + make_interval(days => v_max_future_days)) then
      raise exception 'delivery_timing_json.scheduled_for_utc exceeds max_future_days';
    end if;

    if v_scheduled_for_utc < (v_now - make_interval(mins => v_allow_past_grace_minutes)) then
      raise exception 'delivery_timing_json.scheduled_for_utc too far in past';
    end if;

    if v_scheduled_for_utc <= v_now then
      v_next_attempt_at_utc := v_now;
    else
      v_next_attempt_at_utc := v_scheduled_for_utc;
    end if;
  elsif v_delivery_mode = 'AFTER_DELAY' then
    v_scheduled_text := nullif(btrim(coalesce(p_delivery_timing_json->>'scheduled_for_utc','')), '');

    if v_scheduled_text is not null then
      begin
        v_scheduled_for_utc := v_scheduled_text::timestamptz;
      exception
        when others then
          raise exception 'delivery_timing_json.scheduled_for_utc invalid';
      end;
    else
      if v_relative_minutes is null then
        raise exception 'delivery_timing_json.relative_minutes required when mode=AFTER_DELAY';
      end if;

      v_scheduled_for_utc := v_now + make_interval(mins => v_relative_minutes);
    end if;

    if v_scheduled_for_utc > (v_now + make_interval(days => v_max_future_days)) then
      raise exception 'delivery_timing_json.scheduled_for_utc exceeds max_future_days';
    end if;

    if v_scheduled_for_utc < (v_now - make_interval(mins => v_allow_past_grace_minutes)) then
      raise exception 'delivery_timing_json.scheduled_for_utc too far in past';
    end if;

    if v_scheduled_for_utc <= v_now then
      v_next_attempt_at_utc := v_now;
    else
      v_next_attempt_at_utc := v_scheduled_for_utc;
    end if;
  else
    v_scheduled_for_utc := null;
    v_next_attempt_at_utc := null;
  end if;

  v_delivery_timing_json := jsonb_build_object(
    'mode', v_delivery_mode,
    'scheduled_for_utc', case when v_scheduled_for_utc is null then null else to_jsonb(v_scheduled_for_utc) end,
    'requested_timezone', to_jsonb(v_requested_timezone),
    'requested_local_text', to_jsonb(v_requested_local_text),
    'relative_minutes', to_jsonb(v_relative_minutes)
  );

  v_subject_tpl_override := nullif(coalesce(p_final_edits_json->>'subject',''), '');
  v_body_text_tpl_override := nullif(coalesce(p_final_edits_json->>'body_text',''), '');
  v_body_html_tpl_override := nullif(coalesce(p_final_edits_json->>'body_html',''), '');
  v_message_tpl_override := nullif(coalesce(p_final_edits_json->>'message_text',''), '');

  v_cc_override := nullif(coalesce(p_final_edits_json->>'cc',''), '');
  v_bcc_override := nullif(coalesce(p_final_edits_json->>'bcc',''), '');
  v_reply_to_override := nullif(coalesce(p_final_edits_json->>'reply_to',''), '');
  v_importance_override := nullif(coalesce(p_final_edits_json->>'importance',''), '');
  v_email_type_override := nullif(coalesce(p_final_edits_json->>'email_type',''), '');

  if p_final_edits_json is not null and p_final_edits_json ? 'attachments' then
    if jsonb_typeof(p_final_edits_json->'attachments') <> 'array' then
      raise exception 'final_edits_json.attachments must be array';
    end if;

    v_global_attachments := coalesce(p_final_edits_json->'attachments', '[]'::jsonb);
  else
    v_global_attachments := '[]'::jsonb;
  end if;

  insert into public.mailshot_runs(
    context_kind,
    output_type,
    document_template_id,
    created_by,
    created_at_utc,
    selection_json,
    result_json,
    delivery_timing_json
  )
  values (
    v_context_kind,
    v_output_type,
    v_template_id,
    p_actor_user_id,
    v_now,
    p_prepare_json,
    '{}'::jsonb,
    v_delivery_timing_json
  )
  returning id into v_run_id;

  for v_row in
    select jbe.value
    from jsonb_array_elements(v_rows) as jbe(value)
  loop
    if coalesce((v_row->>'eligible')::boolean, false) = false then
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', coalesce(v_row->>'skip_reason', 'NOT_ELIGIBLE')
        )
      );
      continue;
    end if;

    v_to := nullif(btrim(coalesce(v_row->>'to','')), '');

    if v_to is null then
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', 'MISSING_TO'
        )
      );
      continue;
    end if;

    v_recipient_kind := nullif(btrim(coalesce(v_row->>'recipient_kind','')), '');

    v_recipient_id := null;
    if nullif(coalesce(v_row->>'recipient_id',''),'') is not null then
      v_recipient_id := (v_row->>'recipient_id')::uuid;
    end if;

    v_context_id := null;
    if nullif(coalesce(v_row->>'context_id',''),'') is not null then
      v_context_id := (v_row->>'context_id')::uuid;
    end if;

    v_field_values := coalesce(v_row->'field_values', '{}'::jsonb);
    v_row_template_content := coalesce(v_row->'template_content_json', '{}'::jsonb);

    if v_row ? 'attachment_instructions' and jsonb_typeof(v_row->'attachment_instructions') = 'array' then
      v_row_attachments := coalesce(v_row->'attachment_instructions', '[]'::jsonb);
    elsif v_row ? 'attachments' and jsonb_typeof(v_row->'attachments') = 'array' then
      v_row_attachments := coalesce(v_row->'attachments', '[]'::jsonb);
    else
      v_row_attachments := '[]'::jsonb;
    end if;

    v_effective_attachments := coalesce(v_global_attachments, '[]'::jsonb) || coalesce(v_row_attachments, '[]'::jsonb);

    v_row_subject_tpl := coalesce(
      v_subject_tpl_override,
      nullif(coalesce(v_row_template_content->>'subject',''), '')
    );

    v_row_body_text_tpl := coalesce(
      v_body_text_tpl_override,
      nullif(coalesce(v_row_template_content->>'body_text',''), '')
    );

    v_row_body_html_tpl := coalesce(
      v_body_html_tpl_override,
      nullif(coalesce(v_row_template_content->>'body_html',''), '')
    );

    v_row_message_tpl := coalesce(
      v_message_tpl_override,
      nullif(coalesce(v_row_template_content->>'message_text',''), '')
    );

    v_row_cc := coalesce(
      v_cc_override,
      nullif(coalesce(v_row_template_content->>'cc',''), '')
    );

    v_row_bcc := coalesce(
      v_bcc_override,
      nullif(coalesce(v_row_template_content->>'bcc',''), '')
    );

    v_row_reply_to := coalesce(
      v_reply_to_override,
      nullif(coalesce(v_row_template_content->>'reply_to',''), '')
    );

    v_row_importance := coalesce(
      v_importance_override,
      nullif(coalesce(v_row_template_content->>'importance',''), ''),
      'Normal'
    );

    v_row_email_type := coalesce(
      v_email_type_override,
      nullif(coalesce(v_row_template_content->>'email_type',''), ''),
      nullif(btrim(coalesce(v_row->>'email_type','')), ''),
      'html'
    );

    v_rendered_subject := coalesce(v_row_subject_tpl, '');
    v_rendered_body_text := coalesce(v_row_body_text_tpl, '');
    v_rendered_body_html := coalesce(v_row_body_html_tpl, '');
    v_rendered_message := coalesce(v_row_message_tpl, '');

    for v_kv in
      select
        jet.key as k,
        coalesce(jet.value, '') as v
      from jsonb_each_text(v_field_values) as jet(key, value)
    loop
      v_rendered_subject := replace(v_rendered_subject, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_text := replace(v_rendered_body_text, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_html := replace(v_rendered_body_html, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_message := replace(v_rendered_message, '{{' || v_kv.k || '}}', v_kv.v);
    end loop;

    if v_output_type = 'EMAIL' then
      v_ref := 'mailshot:' || v_run_id::text || ':' || coalesce(v_context_id::text, '') || ':' || md5(coalesce(v_to, ''));

      insert into public.mail_outbox(
        type,
        "to",
        cc,
        bcc,
        reply_to,
        importance,
        email_type,
        subject,
        body_text,
        body_html,
        attachments,
        status,
        reference,
        created_at_utc,
        created_by,
        recipient_kind,
        recipient_id,
        context_kind,
        context_id,
        mailshot_run_id,
        document_template_id,
        scheduled_for_utc,
        next_attempt_at_utc
      )
      values (
        'MAILSHOT_EMAIL'::text,
        v_to,
        v_row_cc,
        v_row_bcc,
        v_row_reply_to,
        v_row_importance,
        v_row_email_type,
        v_rendered_subject,
        nullif(v_rendered_body_text, ''),
        nullif(v_rendered_body_html, ''),
        v_effective_attachments,
        'QUEUED'::public.mail_status_enum,
        v_ref,
        v_now,
        p_actor_user_id,
        v_recipient_kind,
        v_recipient_id,
        v_context_kind,
        v_context_id,
        v_run_id,
        v_template_id,
        v_scheduled_for_utc,
        v_next_attempt_at_utc
      );

      v_queued := v_queued + 1;

    elsif v_output_type in ('WHATSAPP','SMS','VOICE') then
      v_original_len := char_length(v_rendered_message);
      v_sanitised := false;
      v_truncated := false;
      v_wati_template_name := null;
      v_wati_param_name := null;

      if v_output_type = 'WHATSAPP' then
        v_wati_template_name := coalesce(
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'wati_template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'watiTemplateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'templateName','')), '')
        );

        v_wati_param_name := coalesce(
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'wati_param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'watiParamName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'paramName','')), '')
        );

        if v_wati_template_name is null then
          v_skipped := v_skipped + 1;
          v_skip_list := v_skip_list || jsonb_build_array(
            jsonb_build_object(
              'context_id', v_row->>'context_id',
              'recipient_kind', v_row->>'recipient_kind',
              'recipient_id', v_row->>'recipient_id',
              'reason', 'WATI_TEMPLATE_NAME_MISSING'
            )
          );
          continue;
        end if;

        if v_wati_param_name is null then
          v_skipped := v_skipped + 1;
          v_skip_list := v_skip_list || jsonb_build_array(
            jsonb_build_object(
              'context_id', v_row->>'context_id',
              'recipient_kind', v_row->>'recipient_kind',
              'recipient_id', v_row->>'recipient_id',
              'reason', 'WATI_PARAM_NAME_MISSING'
            )
          );
          continue;
        end if;

        v_rendered_message := regexp_replace(v_rendered_message, E'[\\r\\n\\t]+', ' ', 'g');
        v_rendered_message := regexp_replace(v_rendered_message, '[^A-Za-z ,]+', '', 'g');
        v_rendered_message := regexp_replace(v_rendered_message, E'\\s+', ' ', 'g');
        v_rendered_message := btrim(v_rendered_message);

        if char_length(v_rendered_message) <> v_original_len then
          v_sanitised := true;
        end if;

        if char_length(v_rendered_message) > v_whatsapp_max then
          v_rendered_message := left(v_rendered_message, v_whatsapp_max);
          v_truncated := true;
        end if;
      elsif v_output_type = 'SMS' then
        if char_length(v_rendered_message) > v_sms_max then
          v_rendered_message := left(v_rendered_message, v_sms_max);
          v_truncated := true;
        end if;
      else
        if char_length(v_rendered_message) > v_voice_max then
          v_rendered_message := left(v_rendered_message, v_voice_max);
          v_truncated := true;
        end if;
      end if;

      if nullif(v_rendered_message, '') is null then
        v_skipped := v_skipped + 1;
        v_skip_list := v_skip_list || jsonb_build_array(
          jsonb_build_object(
            'context_id', v_row->>'context_id',
            'recipient_kind', v_row->>'recipient_kind',
            'recipient_id', v_row->>'recipient_id',
            'reason', 'EMPTY_MESSAGE_AFTER_SANITIZE'
          )
        );
        continue;
      end if;

      v_provider_key := 'AUTO';

      insert into public.comms_outbox(
        channel,
        status,
        to_address,
        message_text,
        provider_key,
        provider_message_id,
        provider_payload_json,
        provider_response_json,
        last_error,
        created_at_utc,
        sent_at,
        delivered_at,
        read_at,
        failed_at,
        created_by,
        recipient_kind,
        recipient_id,
        context_kind,
        context_id,
        mailshot_run_id,
        document_template_id,
        scheduled_for_utc,
        next_attempt_at_utc
      )
      values (
        v_output_type,
        'QUEUED',
        v_to,
        v_rendered_message,
        v_provider_key,
        null,
        jsonb_build_object(
          'original_len', v_original_len,
          'was_sanitised', v_sanitised,
          'was_truncated', v_truncated
        ) ||
        case
          when v_output_type = 'WHATSAPP' then
            jsonb_build_object(
              'provider_contract',
              jsonb_build_object(
                'provider', 'WATI',
                'template_name', v_wati_template_name,
                'param_name', v_wati_param_name
              )
            )
          else
            '{}'::jsonb
        end,
        '{}'::jsonb,
        null,
        v_now,
        null,
        null,
        null,
        null,
        p_actor_user_id,
        v_recipient_kind,
        v_recipient_id,
        v_context_kind,
        v_context_id,
        v_run_id,
        v_template_id,
        v_scheduled_for_utc,
        v_next_attempt_at_utc
      );

      v_queued := v_queued + 1;
    else
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', 'UNSUPPORTED_OUTPUT_TYPE'
        )
      );
    end if;
  end loop;

  update public.mailshot_runs as mr
  set result_json = jsonb_build_object(
    'queued', v_queued,
    'skipped', v_skipped,
    'failed', v_failed,
    'requested_selected_count', v_requested_selected_count,
    'resolved_context_count', v_resolved_context_count,
    'prepare_row_count', v_prepare_row_count,
    'eligible_count', v_prepare_eligible_count,
    'prepare_skipped_count', v_prepare_skipped_count,
    'prepare_counts', jsonb_build_object(
      'requested_selected_count', v_requested_selected_count,
      'resolved_context_count', v_resolved_context_count,
      'prepare_row_count', v_prepare_row_count,
      'eligible_count', v_prepare_eligible_count,
      'prepare_skipped_count', v_prepare_skipped_count
    ),
    'parity_snapshot', jsonb_build_object(
      'requested_selected_count', v_requested_selected_count,
      'resolved_context_count', v_resolved_context_count,
      'prepare_row_count', v_prepare_row_count,
      'eligible_count', v_prepare_eligible_count,
      'queued_count', v_queued,
      'skipped_count', v_skipped,
      'failed_count', v_failed
    ),
    'skips', v_skip_list,
    'delivery_timing', v_delivery_timing_json
  )
  where mr.id = v_run_id;

  return jsonb_build_object(
    'ok', true,
    'mailshot_run_id', v_run_id::text,
    'queued', v_queued,
    'skipped', v_skipped,
    'failed', v_failed,
    'requested_selected_count', v_requested_selected_count,
    'resolved_context_count', v_resolved_context_count,
    'prepare_row_count', v_prepare_row_count,
    'eligible_count', v_prepare_eligible_count,
    'prepare_skipped_count', v_prepare_skipped_count,
    'prepare_counts', jsonb_build_object(
      'requested_selected_count', v_requested_selected_count,
      'resolved_context_count', v_resolved_context_count,
      'prepare_row_count', v_prepare_row_count,
      'eligible_count', v_prepare_eligible_count,
      'prepare_skipped_count', v_prepare_skipped_count
    ),
    'parity_snapshot', jsonb_build_object(
      'requested_selected_count', v_requested_selected_count,
      'resolved_context_count', v_resolved_context_count,
      'prepare_row_count', v_prepare_row_count,
      'eligible_count', v_prepare_eligible_count,
      'queued_count', v_queued,
      'skipped_count', v_skipped,
      'failed_count', v_failed
    ),
    'skips', v_skip_list,
    'delivery_timing', v_delivery_timing_json
  );
end;
$function$;

-- mailshot_export(jsonb,jsonb,text,uuid)
CREATE OR REPLACE FUNCTION public.mailshot_export(p_prepare_json jsonb, p_final_edits_json jsonb, p_format text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_output_type text;
  v_format text := upper(btrim(coalesce(p_format,'')));

  v_rows jsonb;
  v_row jsonb;

  v_selected_keys jsonb;

  v_message_tpl text;
  v_body_html_tpl text;
  v_body_text_tpl text;
  v_subject_tpl text;

  v_field_values jsonb;
  v_kv record;

  v_rendered_message text;
  v_rendered_body_html text;
  v_rendered_body_text text;
  v_rendered_subject text;

  v_export_rows jsonb := '[]'::jsonb;

  v_cols jsonb := '[]'::jsonb;

  v_key text;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_prepare_json is null or jsonb_typeof(p_prepare_json) <> 'object' then
    raise exception 'prepare_json object required';
  end if;

  if v_format not in ('XLSX','CSV','WORD') then
    raise exception 'format must be XLSX, CSV, or WORD';
  end if;

  v_output_type := upper(coalesce(p_prepare_json->>'output_type',''));
  v_rows := p_prepare_json->'rows';
  v_selected_keys := p_prepare_json->'selected_field_keys';

  if v_output_type not in ('WORD','EXCEL') then
    raise exception 'mailshot_export is only for WORD/EXCEL output types (got: %)', v_output_type;
  end if;

  if v_rows is null or jsonb_typeof(v_rows) <> 'array' then
    raise exception 'prepare_json.rows must be array';
  end if;

  if v_output_type = 'EXCEL' and v_format not in ('XLSX','CSV') then
    raise exception 'EXCEL export requires XLSX or CSV format';
  end if;

  if v_output_type = 'WORD' and v_format <> 'WORD' then
    raise exception 'WORD export requires WORD format';
  end if;

  -- template sources (edits override template_content_json)
  v_subject_tpl := nullif(coalesce(p_final_edits_json->>'subject',''), '');
  v_body_text_tpl := nullif(coalesce(p_final_edits_json->>'body_text',''), '');
  v_body_html_tpl := nullif(coalesce(p_final_edits_json->>'body_html',''), '');
  v_message_tpl := nullif(coalesce(p_final_edits_json->>'message_text',''), '');

  if v_output_type = 'EXCEL' then
    if v_selected_keys is not null and jsonb_typeof(v_selected_keys) = 'array' then
      v_cols := v_selected_keys;
    else
      v_cols := '[]'::jsonb;
    end if;

    for v_row in
      select value
      from jsonb_array_elements(v_rows)
    loop
      if coalesce((v_row->>'eligible')::boolean,false) = false then
        continue;
      end if;

      v_field_values := coalesce(v_row->'field_values','{}'::jsonb);

      v_export_rows := v_export_rows || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'to', v_row->>'to',
          'fields', v_field_values
        )
      );
    end loop;

    return jsonb_build_object(
      'ok', true,
      'kind', 'EXCEL',
      'format', v_format,
      'columns', v_cols,
      'rows', v_export_rows,
      'generated_at_utc', v_now::text
    );
  end if;

  -- WORD export: render HTML per eligible row (backend will wrap into Word-openable document with page breaks)
  for v_row in
    select value
    from jsonb_array_elements(v_rows)
  loop
    if coalesce((v_row->>'eligible')::boolean,false) = false then
      continue;
    end if;

    v_field_values := coalesce(v_row->'field_values','{}'::jsonb);

    if v_body_html_tpl is null then
      v_body_html_tpl := nullif(coalesce((v_row->'template_content_json'->>'body_html'),''), '');
    end if;

    if v_body_text_tpl is null then
      v_body_text_tpl := nullif(coalesce((v_row->'template_content_json'->>'body_text'),''), '');
    end if;

    v_rendered_body_html := coalesce(v_body_html_tpl, '');
    v_rendered_body_text := coalesce(v_body_text_tpl, '');

    for v_kv in
      select e.key as k, coalesce(e.value,'') as v
      from jsonb_each_text(v_field_values) e
    loop
      v_rendered_body_html := replace(v_rendered_body_html, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_text := replace(v_rendered_body_text, '{{' || v_kv.k || '}}', v_kv.v);
    end loop;

    v_export_rows := v_export_rows || jsonb_build_array(
      jsonb_build_object(
        'context_id', v_row->>'context_id',
        'to', v_row->>'to',
        'body_html', nullif(v_rendered_body_html,''),
        'body_text', nullif(v_rendered_body_text,'')
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'kind', 'WORD',
    'format', 'WORD',
    'pages', v_export_rows,
    'generated_at_utc', v_now::text
  );
end;
$function$;

