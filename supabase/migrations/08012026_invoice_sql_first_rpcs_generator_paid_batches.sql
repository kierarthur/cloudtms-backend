-- ============================================================
-- CloudTMS: SQL-first invoice generator (supports HOURS + BY_WEEK)
-- RPC: public.invoice_generate_from_outbox_batch(p_outbox_ids uuid[], p_actor_user_id uuid)
--
-- 100% aligned to your pasted JS logic for:
-- - handleCreateInvoiceTsfin (HOURS)
-- - handleCreateInvoiceTsfinByWeek (BY_WEEK)
-- - extractBillableSegmentsForWeek
-- - lockSegmentsForInvoice
-- - findOrCreateSelfBillInvoice
-- - writeAudit semantics (but SQL has no req headers → ip/user_agent/correlation_id only from payload if provided)
--
-- Safe to re-run: all functions are CREATE OR REPLACE.
-- ============================================================

-- --------------------------
-- Helpers: rounding + ISO UTC
-- --------------------------
create or replace function public._inv_round2(n numeric)
returns numeric
language sql
immutable
as $$
  select round(coalesce(n,0)::numeric, 2);
$$;

create or replace function public._inv_iso_utc(ts timestamptz)
returns text
language sql
immutable
as $$
  select to_char((ts at time zone 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
$$;

-- ------------------------------------------
-- Helpers: schedule-aware reference gating
-- Mirrors handleCreateInvoiceTsfin local helpers
-- ------------------------------------------
create or replace function public._inv_day_refs_has_any(day_refs jsonb)
returns boolean
language sql
immutable
as $$
  select exists (
    select 1
    from jsonb_each_text(coalesce(day_refs, '{}'::jsonb)) e
    where btrim(coalesce(e.value,'')) <> ''
  );
$$;

create or replace function public._inv_weekly_manual_schedule_has_complete_refs(schedule jsonb)
returns boolean
language plpgsql
immutable
as $$
declare
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  n int;
begin
  if schedule is null or jsonb_typeof(schedule) <> 'array' then
    return false;
  end if;

  n := jsonb_array_length(schedule);
  if n <= 0 then
    return false;
  end if;

  for seg in
    select value from jsonb_array_elements(schedule) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));

    if v_start <> '' and v_end <> '' then
      v_ref := btrim(coalesce(seg->>'ref_num',''));
      if v_ref = '' then
        return false;
      end if;
    end if;
  end loop;

  return true;
end;
$$;

create or replace function public._inv_timesheet_has_invoice_reference(
  p_sheet_scope text,
  p_submission_mode text,
  p_reference_number text,
  p_day_references_json jsonb,
  p_actual_schedule_json jsonb
)
returns boolean
language plpgsql
immutable
as $$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
begin
  if scope = 'WEEKLY' and mode = 'MANUAL' then
    return public._inv_weekly_manual_schedule_has_complete_refs(p_actual_schedule_json);
  end if;

  if scope = 'WEEKLY' then
    return (btrim(coalesce(p_reference_number,'')) <> '') or public._inv_day_refs_has_any(p_day_references_json);
  end if;

  return (btrim(coalesce(p_reference_number,'')) <> '');
end;
$$;

create or replace function public._inv_collect_weekly_manual_schedule_refs(
  p_sheet_scope text,
  p_submission_mode text,
  p_actual_schedule_json jsonb
)
returns jsonb
language plpgsql
immutable
as $$
declare
  scope text := upper(btrim(coalesce(p_sheet_scope,'')));
  mode  text := upper(btrim(coalesce(p_submission_mode,'')));
  seg jsonb;
  v_start text;
  v_end   text;
  v_ref   text;
  seen text[] := array[]::text[];
  out jsonb := '[]'::jsonb;
begin
  if not (scope = 'WEEKLY' and mode = 'MANUAL') then
    return '[]'::jsonb;
  end if;

  if p_actual_schedule_json is null or jsonb_typeof(p_actual_schedule_json) <> 'array' then
    return '[]'::jsonb;
  end if;

  for seg in
    select value from jsonb_array_elements(p_actual_schedule_json) value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      continue;
    end if;

    v_start := btrim(coalesce(seg->>'start',''));
    v_end   := btrim(coalesce(seg->>'end',''));
    if not (v_start <> '' and v_end <> '') then
      continue;
    end if;

    v_ref := btrim(coalesce(seg->>'ref_num',''));
    if v_ref = '' then
      continue;
    end if;

    if v_ref = any(seen) then
      continue;
    end if;

    seen := array_append(seen, v_ref);
    out := out || jsonb_build_array(v_ref);
  end loop;

  return out;
end;
$$;

-- ------------------------------------------
-- Helper: write audit row (mirrors writeAudit, but no req headers)
-- ip/user_agent/correlation_id can be optionally supplied by payload.
-- ------------------------------------------
create or replace function public._inv_write_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb default null,
  p_reason text default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor_display text := null;
  v_actor_role text := 'system';
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name,'')), ''),
      nullif(btrim(coalesce(u.role,'')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;

    if v_actor_display is null then
      select nullif(btrim(coalesce(u.email,'')), '')
      into v_actor_display
      from public.tms_users u
      where u.id = p_actor_user_id
      limit 1;
    end if;
  end if;

  if v_actor_display is null then
    v_actor_display := 'CloudTMS server';
  end if;

  if v_actor_role is null then
    v_actor_role := 'system';
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
    actor_role_at_time,
    ip,
    user_agent,
    correlation_id
  )
  values (
    coalesce(nullif(btrim(p_entity),''), 'generic'),
    nullif(btrim(p_subject_id), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    p_reason,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    nullif(btrim(p_ip), ''),
    nullif(btrim(p_user_agent), ''),
    nullif(btrim(p_correlation_id), '')
  );
end;
$$;

-- ------------------------------------------
-- Helper: lock ALL segments in invoice_breakdown_json (HOURS endpoint)
-- Matches handleCreateInvoiceTsfin step 7 (locks every seg without lock).
-- ------------------------------------------
create or replace function public._inv_lock_all_segments_json(p_ib jsonb, p_invoice_id uuid)
returns jsonb
language plpgsql
immutable
as $$
declare
  seg jsonb;
  out_segs jsonb := '[]'::jsonb;
begin
  if p_ib is null or jsonb_typeof(p_ib) <> 'object' then
    return p_ib;
  end if;

  if coalesce(p_ib->>'mode','') <> 'SEGMENTS' then
    return p_ib;
  end if;

  if jsonb_typeof(p_ib->'segments') <> 'array' then
    return p_ib;
  end if;

  for seg in
    select value from jsonb_array_elements(p_ib->'segments') value
  loop
    if seg is null or jsonb_typeof(seg) <> 'object' then
      out_segs := out_segs || jsonb_build_array(seg);
    else
      if nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '') is null then
        seg := jsonb_set(seg, '{invoice_locked_invoice_id}', to_jsonb(p_invoice_id::text), true);
      end if;
      out_segs := out_segs || jsonb_build_array(seg);
    end if;
  end loop;

  return jsonb_set(p_ib, '{segments}', out_segs, true);
end;
$$;

-- ------------------------------------------
-- Helper: lock segments for BY_WEEK (exact mirror of lockSegmentsForInvoice JS)
-- segmentRefs_json must be jsonb array of objects: {tsfin_id, segment_id}
-- ------------------------------------------
create or replace function public._inv_lock_segments_for_invoice(p_invoice_id uuid, p_segment_refs jsonb)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now_iso text := public._inv_iso_utc(now());
  ref jsonb;
  v_tsfin_id uuid;

  -- per-tsfin
  v_lock_whole boolean;
  v_seg_ids text[];

  snap record;
  ib jsonb;
  basis text;
  is_selfbill_or_nhsp boolean;
  all_locked boolean;

  seg jsonb;
  segs_out jsonb;
  sid text;
  locked text;

  i int;
begin
  if p_segment_refs is null or jsonb_typeof(p_segment_refs) <> 'array' then
    return;
  end if;

  -- Build distinct tsfin_ids
  for ref in
    select value from jsonb_array_elements(p_segment_refs) value
  loop
    if jsonb_typeof(ref) <> 'object' then
      continue;
    end if;

    if nullif(coalesce(ref->>'tsfin_id',''), '') is null then
      continue;
    end if;
  end loop;

  -- Process each tsfin_id separately (mirror JS loop)
  for v_tsfin_id in
    select distinct (x->>'tsfin_id')::uuid
    from jsonb_array_elements(p_segment_refs) x
    where nullif(coalesce(x->>'tsfin_id',''), '') is not null
  loop
    -- Gather ref set for this tsfin_id
    select
      bool_or(nullif(coalesce(x->>'segment_id',''), '') is null) as lock_whole,
      array_agg(distinct (x->>'segment_id')) filter (where nullif(coalesce(x->>'segment_id',''), '') is not null) as seg_ids
    into v_lock_whole, v_seg_ids
    from jsonb_array_elements(p_segment_refs) x
    where (x->>'tsfin_id')::uuid = v_tsfin_id;

    v_lock_whole := coalesce(v_lock_whole,false);
    v_seg_ids := coalesce(v_seg_ids, array[]::text[]);

    -- Load snapshot
    select id, basis, locked_by_invoice_id, invoice_breakdown_json
    into snap
    from public.timesheets_financials
    where id = v_tsfin_id;

    if not found then
      continue;
    end if;

    ib := snap.invoice_breakdown_json;
    basis := upper(coalesce(snap.basis::text,''));

    is_selfbill_or_nhsp :=
      basis = 'NHSP' or
      basis = 'NHSP_ADJUSTMENT' or
      basis = 'HEALTHROSTER_SELF_BILL' or
      basis = 'HEALTHROSTER_ADJUSTMENT';

    all_locked := true;
    segs_out := null;

    if ib is not null
       and jsonb_typeof(ib) = 'object'
       and coalesce(ib->>'mode','') = 'SEGMENTS'
       and jsonb_typeof(ib->'segments') = 'array'
    then
      segs_out := '[]'::jsonb;

      for seg in
        select value from jsonb_array_elements(ib->'segments') value
      loop
        if seg is null or jsonb_typeof(seg) <> 'object' then
          segs_out := segs_out || jsonb_build_array(seg);
          continue;
        end if;

        sid := coalesce(seg->>'segment_id','');
        locked := nullif(coalesce(seg->>'invoice_locked_invoice_id',''), '');

        if v_lock_whole or (sid <> '' and sid = any(v_seg_ids)) then
          if locked is null then
            locked := p_invoice_id::text;
          end if;
        end if;

        if locked is null then
          all_locked := false;
        end if;

        seg := jsonb_set(seg, '{invoice_locked_invoice_id}', to_jsonb(locked), true);
        segs_out := segs_out || jsonb_build_array(seg);
      end loop;

      ib := jsonb_set(ib, '{segments}', segs_out, true);
    else
      -- No segments
      all_locked := (snap.locked_by_invoice_id is not null) or v_lock_whole;
    end if;

    -- Patch
    if ib is not null then
      update public.timesheets_financials
      set
        updated_at = now(),
        invoice_breakdown_json = ib,
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    else
      update public.timesheets_financials
      set
        updated_at = now(),
        locked_by_invoice_id = case
          when (not is_selfbill_or_nhsp) and all_locked then p_invoice_id
          else locked_by_invoice_id
        end,
        locked_at_utc = case
          when (not is_selfbill_or_nhsp) and all_locked then now()
          else locked_at_utc
        end
      where id = v_tsfin_id;
    end if;

  end loop;
end;
$$;

-- ============================================================
-- FINAL RPC: invoice_generate_from_outbox_batch
-- Supports outbox kinds:
--   - HOURS  (payload.timesheet_ids: uuid[])
--   - BY_WEEK (payload.client_id: uuid, payload.invoice_week_start: YYYY-MM-DD)
-- ============================================================
create or replace function public.invoice_generate_from_outbox_batch(
  p_outbox_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  outbox_id uuid,
  ok boolean,
  invoice_ids uuid[],
  warnings jsonb
)
language plpgsql
security definer
set search_path = public
as $$
declare
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

begin
  if p_outbox_ids is null or coalesce(array_length(p_outbox_ids,1),0) = 0 then
    return;
  end if;

  select ('INVOICED' = any(enum_range(null::public.contract_week_status_enum)::text[]))
  into v_has_invoiced;

  if not v_has_invoiced then
    raise exception 'contract_week_status_enum does not contain INVOICED; cannot mirror setWeeksInvoicedForTimesheets.';
  end if;

  foreach v_outbox_id in array p_outbox_ids loop
    begin
      v_invoice_id := null;
      v_now := now();
      v_anchor_ymd := (v_now at time zone 'Europe/London')::date;

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
        return next;
        continue;
      end if;

      v_kind := upper(coalesce(v_job.kind,''));
      v_payload := coalesce(v_job.payload, '{}'::jsonb);

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
      from snaps_all
      where upper(coalesce(basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT')
    )
  into v_snap_cnt, v_client_cnt, v_client_id, v_has_disallowed
  from snaps_all;

  if v_snap_cnt = 0 then
    raise exception 'No eligible timesheets (need READY_FOR_INVOICE & unlocked).';
  end if;

  if v_has_disallowed then
    raise exception 'This endpoint cannot invoice NHSP or HR self-bill timesheets (use BY_WEEK).';
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

          select bank_name, bank_sort_code, bank_account_number, vat_registration_number
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
          select cs.client_id, cs.vat_rate_pct, cs.hr_attach_to_invoice, cs.ts_attach_to_invoice, cs.effective_from
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

          v_hr_attach_default :=
            case when found and v_cs.hr_attach_to_invoice is false then false else true end;

          v_ts_attach_default :=
            case when found and v_cs.ts_attach_to_invoice is false then false else true end;

          v_terms_days := coalesce(v_client.payment_terms_days, 30);
          v_due_at := v_now + make_interval(days => v_terms_days);

          -- Contract mapping via contract_weeks only (matches JS HOURS endpoint)
          -- Aggregate requires_hr per contract (attach flags are client_settings-only)
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
            bool_or(coalesce(cons.requires_hr,false)) as req_hr
          into v_requires_hr_any
          from cons;

          -- ✅ Attach flags live in client_settings, not contracts.
          v_hr_attach_any := coalesce(v_hr_attach_default, true);
          v_ts_attach_any := coalesce(v_ts_attach_default, true);



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
            'vat_chargeable', coalesce(v_client.vat_chargeable,true),
            'applied_vat_rate_pct', v_vat_rate,
            'payment_terms_days', v_terms_days,
            'issued_at_utc', public._inv_iso_utc(v_now),
            'due_at_utc', public._inv_iso_utc(v_due_at),
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
              'hr_attach_to_invoice', coalesce(v_hr_attach_default,true),
              'ts_attach_to_invoice', coalesce(v_ts_attach_default,true)
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
        v_now,
        v_due_at,
        0,0,0,
        v_header
      )
      returning id into v_invoice_id;

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
                           -- Invoice rule: never emit an hours line with zero client charge
                charge_ex := public._inv_round2(d_rec.chg_ex);
                if charge_ex <= 0 then
                  continue;
                end if;

                -- Defensive: also skip if total hours are zero
                if (coalesce(d_rec.hours_day,0) + coalesce(d_rec.hours_night,0) + coalesce(d_rec.hours_sat,0) + coalesce(d_rec.hours_sun,0) + coalesce(d_rec.hours_bh,0)) <= 0 then
                  continue;
                end if;

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
                  null,null,null,null,null,
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
              );

              -- Invoice rule: never emit an hours line with zero client charge
              if base_chg_ex <= 0 then
                -- allow additional-only below
              else
                -- Defensive: also skip if total hours are zero
                if (coalesce(s.hours_day,0) + coalesce(s.hours_night,0) + coalesce(s.hours_sat,0) + coalesce(s.hours_sun,0) + coalesce(s.hours_bh,0)) <= 0 then
                  -- allow additional-only below
                else
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
                    null,null,null,null,null,
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
                end if;
              end if;
            end if;


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
                  if coalesce(d_rec.units,0) <= 0 then
                    continue;
                  end if;

                  any_daily_add := true;

                  pay_ex := public._inv_round2(d_rec.units * pay_rate);
                  charge_ex := public._inv_round2(d_rec.units * charge_rate);
                  if charge_ex <= 0 then
                    continue;
                  end if;

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
                    null,null,null,null,null,
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
              if unit_count <= 0 then
                continue;
              end if;

              charge_ex := public._inv_round2(coalesce((ex->>'charge_ex_vat')::numeric, 0));
              if charge_ex <= 0 then
                continue;
              end if;

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
                null,null,null,null,null,
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
              if public._inv_round2(coalesce(s.travel_charge_ex_vat,0)) > 0 then
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
                  null,null,null,null,null,
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
              if public._inv_round2(coalesce(s.accommodation_charge_ex_vat,0)) > 0 then
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
                  null,null,null,null,null,
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
              if public._inv_round2(coalesce(s.other_charge_ex_vat,0)) > 0 then
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
                  null,null,null,null,null,
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
            if public._inv_round2(coalesce(s.mileage_charge_ex_vat,0)) > 0 then
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
                null,null,null,null,null,
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

update public.timesheets_financials tf
set
  locked_by_invoice_id = v_invoice_id,
  locked_at_utc = v_now,
  updated_at = v_now,
  invoice_breakdown_json = public._inv_lock_all_segments_json(tf.invoice_breakdown_json, v_invoice_id)
where tf.timesheet_id = any(v_ts_ids_to_use)
  and tf.is_current = true
  and tf.locked_by_invoice_id is null
  and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum;



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

              -- Cache HR/NHSP source rows for this invoice (mirror JS step 9)
          -- Only when client-led effective_hr_attach_to_invoice is TRUE
          if coalesce(v_hr_attach_default, true) = true then
            -- Build shift_ids from segments where segment_id startswith 'nhsp:' and source_system in (NHSP, HEALTHROSTER)
            delete from public.invoice_hr_source_rows where invoice_id = v_invoice_id;

            with segs as (
              select
                left(seg->>'segment_id', 5) as pfx,
                upper(coalesce(seg->>'source_system','')) as src,
                substr(seg->>'segment_id', 6) as id_part
              from public.timesheets_financials tf
              cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg
              where tf.timesheet_id = any(v_ts_ids_to_use)
                and tf.is_current = true
            ),
            shift_ids as (
              select distinct (id_part)::uuid as shift_id
              from segs
              where pfx = 'nhsp:'
                and src in ('NHSP','HEALTHROSTER')
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
          end if;


          -- SUCCESS: delete outbox row
          delete from public.invoice_jobs_outbox where id = v_outbox_id;

          outbox_id := v_outbox_id;
          ok := true;
          invoice_ids := array[v_invoice_id];
          warnings := jsonb_build_object(
            'kind','HOURS',
            'invoice_id', v_invoice_id::text,
            'client_id', v_client_id::text
          );
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

          v_client record;
          v_def record;
          v_cs record;

          v_global_vat numeric := 20;
          v_vat_rate numeric := 20;

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
tsid uuid;
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

          line_desc text;
          meta jsonb;
          v_line_source_key text;

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

          -- Load client + defaults
          select id, name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
          into v_client
          from public.clients
          where id = v_client_id;

          if not found then
            raise exception 'Client not found.';
          end if;

          select bank_name, bank_sort_code, bank_account_number, vat_registration_number
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
          select cs.client_id, cs.vat_rate_pct, cs.hr_attach_to_invoice, cs.ts_attach_to_invoice, cs.effective_from
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
            bool_or(coalesce(cons.requires_hr,false)) as req_hr
          into v_requires_hr_any
          from cons;

          -- ✅ Attach flags live in client_settings, not contracts.
          v_hr_attach_any := coalesce(v_hr_attach_default, true);
          v_ts_attach_any := coalesce(v_ts_attach_default, true);



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
                    coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                  ),

                'charge_amount',
                  public._inv_round2(
                    coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    - coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                  ),

                'margin_amount',
                  public._inv_round2(
                    (
                      coalesce((select (s2->>'total_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'additional_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'expenses_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'mileage_charge_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    )
                    -
                    (
                      coalesce((select (s2->>'total_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'additional_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'expenses_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                      - coalesce((select (s2->>'mileage_pay_ex_vat')::numeric from jsonb_array_elements(meta) s2 where (s2->>'id')::uuid=sn.tsfin_id limit 1),0)
                    )
                  ),

                'invoice_target_week_start', v_week_start::text,
                'invoice_locked_invoice_id', null
              ) as segment,
              -1 as segment_index,
              true as pseudo
            from snaps sn
            where (sn.week_ending_date - 6) = v_week_start
              and sn.locked_by_invoice_id is null
              and not (sn.ib is not null and jsonb_typeof(sn.ib)='object' and (sn.ib->>'mode')='SEGMENTS' and jsonb_typeof(sn.ib->'segments')='array')
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

          -- timesheet_ids used
          select array_agg(distinct (e->>'timesheet_id')::uuid)
          into v_timesheet_ids
          from jsonb_array_elements(v_entries) e;

          -- allSelfBill check (matches JS)
          select bool_and(upper(coalesce(e->>'basis','')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'))
          into v_all_selfbill
          from jsonb_array_elements(v_entries) e;

          v_mode := case when v_all_selfbill then 'SELF_BILL' else 'NORMAL' end;

          -- Obtain invoice (reuse or create)
                if v_all_selfbill then
            select *
            into v_invoice
            from public.invoices i
            where i.client_id = v_client_id
              and i.status in (
                'DRAFT'::public.invoice_status_enum,
                'ON_HOLD'::public.invoice_status_enum,
                'ISSUED'::public.invoice_status_enum
              )
              and (i.header_snapshot_json->'meta'->>'source') = 'TSFIN_SEGMENTS'
              and (i.header_snapshot_json->'meta'->>'invoice_week_start') = v_week_start::text
            limit 1;


            if found then
              v_created := false;
              v_invoice_id := v_invoice.id;
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
                  bool_or(coalesce(cons.requires_hr,false)) as req_hr
                into sb_requires_hr
                from cons;

                -- ✅ Attach flags live in client_settings, not contracts.
                sb_hr_attach := coalesce(v_hr_attach_default, true);
                sb_ts_attach := coalesce(v_ts_attach_default, true);

                          v_header := jsonb_build_object(
                  'client_id', v_client_id::text,
                  'client_name', v_client.name,
                  'client_invoice_address', v_client.invoice_address,
                  'client_primary_invoice_email', v_client.primary_invoice_email,
                  'vat_chargeable', coalesce(v_client.vat_chargeable,true),
                  'applied_vat_rate_pct', v_vat_rate,
                  'payment_terms_days', v_terms_days,
                  'issued_at_utc', public._inv_iso_utc(v_now),
                  'due_at_utc', public._inv_iso_utc(v_due_at),
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
                    'hr_attach_to_invoice', coalesce(v_hr_attach_default,true),
                    'ts_attach_to_invoice', coalesce(v_ts_attach_default,true)
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
                  'ISSUED'::public.invoice_status_enum,
                  v_now,
                  v_now,
                  v_due_at,
                  0,0,0,
                  v_header
                )
                returning id into v_invoice_id;


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
                  'hr_attach_to_invoice', coalesce(v_hr_attach_default,true),
                  'ts_attach_to_invoice', coalesce(v_ts_attach_default,true)
                ),
                true
              ),
              updated_at = v_now
            where i.id = v_invoice_id;

            -- Self-bill rule: if this invoice is still DRAFT, promote it to ISSUED immediately
            -- (Do NOT override ON_HOLD)
            update public.invoices i
            set
              status = 'ISSUED'::public.invoice_status_enum,
              status_date_utc = v_now,
              issued_at_utc = coalesce(i.issued_at_utc, v_now),
              on_hold_reason = null,
              updated_at = v_now
            where i.id = v_invoice_id
              and i.status = 'DRAFT'::public.invoice_status_enum;

            if found then
              perform public._inv_write_audit(
                p_actor_user_id,
                'INVOICE_SELF_BILL_ISSUED',
                jsonb_build_object(
                  'invoice_id', v_invoice_id::text,
                  'client_id', v_client_id::text,
                  'invoice_week_start', v_week_start::text,
                  'mode', 'SELF_BILL',
                  'issued_at_utc', public._inv_iso_utc(v_now)
                ),
                'invoices',
                v_invoice_id::text,
                null,
                null,
                v_ip, v_ua, v_corr
              );
            end if;


          else
            -- Normal BY_WEEK invoice
                     v_header := jsonb_build_object(
              'client_id', v_client_id::text,
              'client_name', v_client.name,
              'client_invoice_address', v_client.invoice_address,
              'client_primary_invoice_email', v_client.primary_invoice_email,
              'vat_chargeable', coalesce(v_client.vat_chargeable,true),
              'applied_vat_rate_pct', v_vat_rate,
              'payment_terms_days', v_terms_days,
              'issued_at_utc', public._inv_iso_utc(v_now),
              'due_at_utc', public._inv_iso_utc(v_due_at),
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
                'invoice_week_start', v_week_start::text,
                'timesheet_count', coalesce(array_length(v_timesheet_ids,1),0),
                'segment_count', v_entry_count
              ),
              'attach_policy', jsonb_build_object(
                'requires_hr', coalesce(v_requires_hr_any,false),
                'hr_attach_to_invoice', coalesce(v_hr_attach_default,true),
                'ts_attach_to_invoice', coalesce(v_ts_attach_default,true)
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
              v_now,
              v_due_at,
              0,0,0,
              v_header
            )
            returning id into v_invoice_id;
            v_created := true;
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

          delete from pg_temp._inv_billed_add_keys;

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
          for tsid in
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
            where tf.timesheet_id = tsid
              and tf.is_current = true;

            if not found then
              continue;
            end if;

                     -- contract resolution
            contract_id := snap.ts_contract_id;

            if contract_id is null then
              select cw.contract_id
              into contract_id
              from public.contract_weeks cw
              where cw.timesheet_id = tsid
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



            -- hasAnyDate from entries for this tsid
            select exists(
              select 1
              from jsonb_array_elements(v_entries) e
              where (e->>'timesheet_id')::uuid = tsid
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
                  where (e->>'timesheet_id')::uuid = tsid
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
                chg_ex := public._inv_round2(bydate.chg_ex);
                if chg_ex <= 0 then
                  continue;
                end if;

                if (coalesce(bydate.hours_day,0)+coalesce(bydate.hours_night,0)+coalesce(bydate.hours_sat,0)+coalesce(bydate.hours_sun,0)+coalesce(bydate.hours_bh,0)) <= 0 then
                  continue;
                end if;

                bydate_any := true;

                pay_ex := public._inv_round2(bydate.pay_ex);
                margin_ex := public._inv_round2(chg_ex - pay_ex);
                vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                inc_amt := public._inv_round2(chg_ex + vat_amt);

                              line_desc := coalesce(cand_display, ('TS '||tsid::text)) ||
                        ' – '|| bydate.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

                meta := jsonb_build_object(
                  'line_type','HOURS_DAILY',
                  'timesheet_id', tsid::text,
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

                v_line_source_key := 'TS:' || tsid::text || ':HOURS:' || bydate.ymd;
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
                  v_invoice_id, tsid, snap.booking_id, line_desc,
                  public._inv_round2(bydate.hours_day),
                  public._inv_round2(bydate.hours_night),
                  public._inv_round2(bydate.hours_sat),
                  public._inv_round2(bydate.hours_sun),
                  public._inv_round2(bydate.hours_bh),
                  null,null,null,null,null,
                  null,null,null,null,null,
                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
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
                  if units <= 0 then continue; end if;

                  -- self-bill dedupe
                  if v_all_selfbill and exists(
                    select 1 from pg_temp._inv_billed_add_keys b
                    where b.timesheet_id = tsid and b.code = code and b.suffix = left(bydate.ymd,10)
                  ) then
                    continue;
                  end if;

                  pay_ex := public._inv_round2(units * pay_rate);
                  chg_ex := public._inv_round2(units * charge_rate);
                  if chg_ex <= 0 then continue; end if;

                  margin_ex := public._inv_round2(chg_ex - pay_ex);
                  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
                  inc_amt := public._inv_round2(chg_ex + vat_amt);

                                line_desc := coalesce(cand_display, ('TS '||tsid::text)) ||
                          ' – '|| bucket_name || ' – '|| left(bydate.ymd,10) ||
                          ' – '|| units::text || ' '|| unit_name;

                                                 meta := jsonb_build_object(
                    'line_type','ADDITIONAL_RATE_DAILY',
                    'timesheet_id', tsid::text,
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



                  v_line_source_key := 'TS:' || tsid::text || ':ADD:' || code || ':' || left(bydate.ymd,10);
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
                    v_invoice_id, tsid, snap.booking_id, line_desc,
                    0,0,0,0,0,
                    null,null,null,null,null,
                    null,null,null,null,null,
                    pay_ex, chg_ex, margin_ex,
                    v_vat_rate, vat_amt, inc_amt,
                    ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                    meta,
                    v_line_source_key
                  )
                  on conflict (invoice_id, source_key) do nothing;

                  if found then
                    insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                    values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
                  end if;


                end loop;
              end loop;

              -- If any HOURS_DAILY lines exist for this ts, skip weekly fallback (mirror JS)
              if exists(
                select 1 from public.invoice_lines l
                where l.invoice_id = v_invoice_id
                  and l.timesheet_id = tsid
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
              where (e->>'timesheet_id')::uuid = tsid
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
if chg_ex <= 0 then
  -- Invoice rule: never emit an hours line with zero client charge
else
  if (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) <= 0 then
    -- Defensive: also skip if hours are zero
  else
    margin_ex := public._inv_round2(chg_ex - pay_ex);
    vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
    inc_amt := public._inv_round2(chg_ex + vat_amt);

     line_desc := coalesce(cand_display, ('TS '||tsid::text)) || ' – W/E ' || coalesce(snap.week_ending_date::text,'');

     meta := jsonb_build_object(
      'line_type','HOURS_WEEKLY',
      'timesheet_id', tsid::text,
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


    v_line_source_key := 'TS:' || tsid::text || ':HOURS:WEEK';
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
      v_invoice_id, tsid, snap.booking_id, line_desc,
      h_day, h_night, h_sat, h_sun, h_bh,
      null,null,null,null,null,
      null,null,null,null,null,
      pay_ex, chg_ex, margin_ex,
      v_vat_rate, vat_amt, inc_amt,
      ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
      meta,
      v_line_source_key
    )
    on conflict (invoice_id, source_key) do nothing;

    if found then
      insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
      values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
    end if;
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
              if unit_count <= 0 then continue; end if;

              -- self-bill dedupe
              if v_all_selfbill and exists(
                select 1 from pg_temp._inv_billed_add_keys b
                where b.timesheet_id = tsid and b.code = code and b.suffix = 'WEEK'
              ) then
                continue;
              end if;

              pay_ex := coalesce((ex->>'pay_ex_vat')::numeric, 0);
              chg_ex := coalesce((ex->>'charge_ex_vat')::numeric, 0);
              if chg_ex <= 0 then continue; end if;

              margin_ex := public._inv_round2(chg_ex - pay_ex);
              vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
              inc_amt := public._inv_round2(chg_ex + vat_amt);

              bucket_name := nullif(btrim(coalesce(ex->>'bucket_name','')), '');
              if bucket_name is null then bucket_name := code; end if;

              unit_name := nullif(btrim(coalesce(ex->>'unit_name','')), '');
              if unit_name is null then unit_name := 'units'; end if;

                        line_desc := coalesce(cand_display, ('TS '||tsid::text)) ||
                      ' – ' || bucket_name || ' – ' || unit_count::text || ' ' || unit_name ||
                      ' (W/E ' || coalesce(snap.week_ending_date::text,'') || ')';

              meta := jsonb_build_object(
                'line_type','ADDITIONAL_RATE',
                'timesheet_id', tsid::text,
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

              v_line_source_key := 'TS:' || tsid::text || ':ADD:' || code || ':WEEK';
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
                v_invoice_id, tsid, snap.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
                null,null,null,null,null,
                pay_ex, chg_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
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
              if public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) > 0 then
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
                  'timesheet_id', tsid::text,
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

                v_line_source_key := 'TS:' || tsid::text || ':EXP:TRAVEL';

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
                  v_invoice_id, tsid, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
                  null,null,null,null,null,
                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- ACCOMMODATION
              if public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) > 0 then
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
                  'timesheet_id', tsid::text,
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

                v_line_source_key := 'TS:' || tsid::text || ':EXP:ACCOMMODATION';

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
                  v_invoice_id, tsid, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
                  null,null,null,null,null,
                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;

              -- OTHER
              if public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) > 0 then
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
                  'timesheet_id', tsid::text,
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

                v_line_source_key := 'TS:' || tsid::text || ':EXP:OTHER';

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
                  v_invoice_id, tsid, snap.booking_id, line_desc,
                  0,0,0,0,0,
                  null,null,null,null,null,
                  null,null,null,null,null,
                  pay_ex, chg_ex, margin_ex,
                  v_vat_rate, vat_amt, inc_amt,
                  ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                  meta,
                  v_line_source_key
                )
                on conflict (invoice_id, source_key) do nothing;

                if found then
                  insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                  values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
                end if;
              end if;
            end;


            -- -------------------------
            -- MILEAGE line (one per timesheet, if charge>0)
            -- -------------------------
            if public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) > 0 then
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
                'timesheet_id', tsid::text,
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

              v_line_source_key := 'TS:' || tsid::text || ':MILEAGE';

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
                v_invoice_id, tsid, snap.booking_id, line_desc,
                0,0,0,0,0,
                null,null,null,null,null,
                null,null,null,null,null,
                pay_ex, chg_ex, margin_ex,
                v_vat_rate, vat_amt, inc_amt,
                ('docs-pdf/timesheets/ts_' || tsid::text || '.pdf'),
                meta,
                v_line_source_key
              )
              on conflict (invoice_id, source_key) do nothing;

              if found then
                insert into pg_temp._inv_run_lines(timesheet_id, source_key, charge_ex, vat_amount, inc_amount)
                values (tsid, v_line_source_key, chg_ex, vat_amt, inc_amt);
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
          for tsid in
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
              and l.timesheet_id = tsid
              and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_DAILY','HOURS_WEEKLY');

            -- bases: DAILY if any HOURS_DAILY exists; else first basis in entries order for this ts
            select exists(
              select 1 from public.invoice_lines l
              where l.invoice_id = v_invoice_id
                and l.timesheet_id = tsid
                and upper(coalesce(l.meta_json->>'line_type','')) = 'HOURS_DAILY'
            ) into bydate_any;

            if bydate_any then
              meta := to_jsonb(array['DAILY']::text[]);
            else
              select to_jsonb(array[ upper(coalesce(e->>'basis','WEEKLY')) ]::text[])
              into meta
              from jsonb_array_elements(v_entries) e
              where (e->>'timesheet_id')::uuid = tsid
              order by (e->>'entry_ord')::int asc
              limit 1;
            end if;

            perform public._inv_write_audit(
              p_actor_user_id,
              'TIMESHEET_SEGMENTS_INVOICED',
              jsonb_build_object(
                'timesheet_id', tsid::text,
                'invoice_id', v_invoice_id::text,
                'invoice_week_start', v_week_start::text,
                'segment_count', v_terms_days,
                'total_charge_ex_vat', public._inv_round2(chg_ex),
                'total_pay_ex_vat', public._inv_round2(pay_ex),
                'bases', coalesce(meta, '[]'::jsonb)
              ),
              'timesheets',
              tsid::text,
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

            for tsid in
              select unnest(v_timesheet_ids)
            loop
              perform public._inv_write_audit(
                p_actor_user_id,
                'TIMESHEET_INVOICED',
                jsonb_build_object(
                  'timesheet_id', tsid::text,
                  'invoice_id', v_invoice_id::text,
                  'invoice_week_start', v_week_start::text
                ),
                'timesheets',
                tsid::text,
                null,
                'LOCKED_BY_INVOICE',
                v_ip, v_ua, v_corr
              );
            end loop;
          end if;

          -- SUCCESS: delete outbox row
          delete from public.invoice_jobs_outbox where id = v_outbox_id;

          outbox_id := v_outbox_id;
          ok := true;
          invoice_ids := array[v_invoice_id];
          warnings := jsonb_build_object(
            'kind','BY_WEEK',
            'invoice_id', v_invoice_id::text,
            'client_id', v_client_id::text,
            'invoice_week_start', v_week_start::text,
            'mode', v_mode
          );
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
      return next;
      continue;
    end;
  end loop;
end;
$$;


create or replace function public.invoice_source_rows_collect(
  p_invoice_id uuid,
  p_force_refresh boolean default true
)
returns table (
  source_system text,
  import_id uuid,
  header_columns jsonb,
  rows_json jsonb
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_has_cache boolean := false;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- If cache exists and caller does NOT force refresh, return cache
  select exists(
    select 1 from public.invoice_hr_source_rows r where r.invoice_id = p_invoice_id
  ) into v_has_cache;

  if v_has_cache and coalesce(p_force_refresh,false) = false then
    return query
    select
      r.source_system,
      r.import_id,
      r.header_columns,
      r.rows_json
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
    order by r.source_system, r.import_id;
    return;
  end if;

  -- Recompute + refresh cache (safe even if cache is empty)
  delete from public.invoice_hr_source_rows
  where invoice_id = p_invoice_id;

  with ts_ids as (
    select distinct l.timesheet_id
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.timesheet_id is not null
  ),
  fin as (
    select tf.timesheet_id, tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select timesheet_id from ts_ids)
  ),
  segs as (
    select
      upper(coalesce(seg->>'source_system','')) as source_system,
      left(coalesce(seg->>'segment_id',''), 5) as pfx,
      substr(coalesce(seg->>'segment_id',''), 6) as id_part
    from fin
    cross join lateral jsonb_array_elements(coalesce(fin.invoice_breakdown_json->'segments','[]'::jsonb)) seg
  ),
  shift_ids as (
    select distinct (id_part)::uuid as shift_id
    from segs
    where pfx = 'nhsp:'
      and source_system in ('NHSP','HEALTHROSTER')
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
          and r.external_row_key in (select jsonb_array_elements_text(h.keys_json))
      ) as rows_json
    from hdr h
  )
  insert into public.invoice_hr_source_rows(invoice_id, source_system, import_id, header_columns, rows_json)
  select
    p_invoice_id,
    r.source_system,
    r.import_id,
    r.header_columns,
    r.rows_json
  from rows_agg r;

  -- Return refreshed cache
  return query
  select
    r.source_system,
    r.import_id,
    r.header_columns,
    r.rows_json
  from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id
  order by r.source_system, r.import_id;
end;
$$;

create or replace function public.invoice_create_credit_note_and_unlock(
  p_invoice_id uuid,
  p_actor_user_id uuid
)
returns table (
  credit_note_id uuid,
  unlocked_snapshots int
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_inv record;
  v_base_hdr jsonb := '{}'::jsonb;

  v_original_issued_at timestamptz;
  v_anchor_ymd date;

  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_bank jsonb;
  v_vat_reg text;

  v_client_name text;
  v_client_addr text;
  v_client_email text;
  v_vat_chargeable boolean;
  v_terms_days int;

  v_applied_vat numeric;
  v_global_vat numeric := 20;
  v_client_vat_override numeric;

  v_due_at timestamptz;

  v_credit_id uuid;

  v_ts_ids uuid[];

  v_cn_ex numeric := 0;
  v_cn_vat numeric := 0;
  v_cn_inc numeric := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load original invoice
  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot credit a CREDIT_NOTE';
  end if;

  if jsonb_typeof(v_inv.header_snapshot_json) = 'object' then
    v_base_hdr := v_inv.header_snapshot_json;
  end if;

  -- Original issued time (prefer invoice.issued_at_utc, else snapshot.issued_at_utc, else now)
  v_original_issued_at := v_inv.issued_at_utc;
  if v_original_issued_at is null and (v_base_hdr ? 'issued_at_utc') then
    begin
      v_original_issued_at := (v_base_hdr->>'issued_at_utc')::timestamptz;
    exception when others then
      v_original_issued_at := null;
    end;
  end if;
  if v_original_issued_at is null then
    v_original_issued_at := v_now;
  end if;

  v_anchor_ymd := (v_original_issued_at at time zone 'Europe/London')::date;

  -- Stationery key
  v_stationery_key := nullif(btrim(coalesce(v_base_hdr->>'stationery_key','')), '');
  if v_stationery_key is null then
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  end if;

  if right(lower(v_stationery_key), 4) = '.pdf' then
    v_stationery_key := left(v_stationery_key, length(v_stationery_key) - 4) || '@300dpi.png';
  end if;

  while left(v_stationery_key, 1) = '/' loop
    v_stationery_key := substr(v_stationery_key, 2);
  end loop;

  -- Margins
  v_margins := v_base_hdr->'stationery_margins_mm';
  if jsonb_typeof(v_margins) = 'array' and jsonb_array_length(v_margins) = 4 then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>0)::numeric, 32),
      'right',  coalesce((v_margins->>1)::numeric, 12),
      'bottom', coalesce((v_margins->>2)::numeric, 20),
      'left',   coalesce((v_margins->>3)::numeric, 12)
    );
  elsif jsonb_typeof(v_margins) = 'object' then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>'top')::numeric, 32),
      'right',  coalesce((v_margins->>'right')::numeric, 12),
      'bottom', coalesce((v_margins->>'bottom')::numeric, 20),
      'left',   coalesce((v_margins->>'left')::numeric, 12)
    );
  else
    v_margins := jsonb_build_object('top',32,'right',12,'bottom',20,'left',12);
  end if;

  -- hide_bank_footer default TRUE
  if jsonb_typeof(v_base_hdr->'hide_bank_footer') = 'boolean' then
    v_hide_bank_footer := (v_base_hdr->>'hide_bank_footer')::boolean;
  else
    v_hide_bank_footer := true;
  end if;

  -- Bank + VAT registration
  if jsonb_typeof(v_base_hdr->'bank') = 'object' then
    v_bank := v_base_hdr->'bank';
  else
    v_bank := null;
  end if;

  v_vat_reg := nullif(btrim(coalesce(v_base_hdr->>'vat_registration_number','')), '');

  if v_bank is null or v_vat_reg is null then
    declare
      v_def record;
    begin
      select bank_name, bank_sort_code, bank_account_number, vat_registration_number
      into v_def
      from public.settings_defaults
      where id = 1
      limit 1;

      if v_bank is null then
        v_bank := jsonb_build_object(
          'name', v_def.bank_name,
          'sort_code', v_def.bank_sort_code,
          'account_number', v_def.bank_account_number
        );
      end if;

      if v_vat_reg is null then
        v_vat_reg := v_def.vat_registration_number;
      end if;
    end;
  end if;

  -- Client info
  v_client_name  := nullif(btrim(coalesce(v_base_hdr->>'client_name','')), '');
  v_client_addr  := nullif(btrim(coalesce(v_base_hdr->>'client_invoice_address','')), '');
  v_client_email := nullif(btrim(coalesce(v_base_hdr->>'client_primary_invoice_email','')), '');

  if jsonb_typeof(v_base_hdr->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_base_hdr->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := null;
  end if;

  if (v_base_hdr ? 'payment_terms_days') then
    begin
      v_terms_days := (v_base_hdr->>'payment_terms_days')::int;
    exception when others then
      v_terms_days := null;
    end;
  else
    v_terms_days := null;
  end if;

  if v_client_name is null or v_client_addr is null or v_vat_chargeable is null or v_terms_days is null then
    declare
      v_cli record;
    begin
      select name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
      into v_cli
      from public.clients
      where id = v_inv.client_id
      limit 1;

      if v_client_name is null then v_client_name := v_cli.name; end if;
      if v_client_addr is null then v_client_addr := v_cli.invoice_address; end if;
      if v_client_email is null then v_client_email := v_cli.primary_invoice_email; end if;

      if v_vat_chargeable is null then
        v_vat_chargeable := coalesce(v_cli.vat_chargeable, true);
      end if;

      if v_terms_days is null then
        v_terms_days := coalesce(v_cli.payment_terms_days, 30);
      end if;
    end;
  end if;

  -- VAT % (prefer original snapshot applied_vat_rate_pct; else compute anchored)
  v_applied_vat := null;
  if (v_base_hdr ? 'applied_vat_rate_pct') then
    begin
      v_applied_vat := (v_base_hdr->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_applied_vat := null;
    end;
  end if;

  if v_applied_vat is null then
    select coalesce(sf.vat_rate_pct, 20)
    into v_global_vat
    from public.settings_finance_pick(v_anchor_ymd) sf
    limit 1;

    select cs.vat_rate_pct
    into v_client_vat_override
    from public.client_settings cs
    where cs.client_id = v_inv.client_id
      and cs.effective_from <= v_anchor_ymd
    order by cs.effective_from desc
    limit 1;

    v_applied_vat := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_global_vat, 20)
    end;
  else
    if v_vat_chargeable = false then
      v_applied_vat := 0;
    end if;
  end if;

  v_due_at := v_now + make_interval(days => coalesce(v_terms_days, 30));

  -- Create the credit note invoice row
  insert into public.invoices (
    client_id,
    type,
    status,
    status_date_utc,
    issued_at_utc,
    due_at_utc,
    subtotal_ex_vat,
    vat_amount,
    total_inc_vat,
    original_invoice_id,
    header_snapshot_json
  )
  values (
    v_inv.client_id,
    'CREDIT_NOTE'::public.invoice_type_enum,
    'ISSUED'::public.invoice_status_enum,
    v_now,
    v_now,
    v_due_at,
    0,
    0,
    0,
    v_inv.id,
    jsonb_build_object(
      'client_id', v_inv.client_id::text,
      'client_name', v_client_name,
      'client_invoice_address', v_client_addr,
      'client_primary_invoice_email', v_client_email,
      'vat_chargeable', coalesce(v_vat_chargeable, true),
      'applied_vat_rate_pct', coalesce(v_applied_vat, 0),
      'payment_terms_days', coalesce(v_terms_days, 30),
      'issued_at_utc', public._inv_iso_utc(v_now),
      'due_at_utc', public._inv_iso_utc(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', v_bank,
      'vat_registration_number', v_vat_reg,
      'meta', jsonb_build_object(
        'source', 'CREDIT_NOTE',
        'original_invoice_id', v_inv.id::text,
        'vat_anchor_ymd', v_anchor_ymd::text,
        'original_invoice_issued_at_utc', public._inv_iso_utc(v_original_issued_at)
      )
    )
  )
  returning id into v_credit_id;

  -- ✅ Insert negative mirror lines (one-for-one from original invoice_lines)
  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  select
    v_credit_id,
    l.timesheet_id,
    l.booking_id,
    ('CREDIT NOTE – ' || coalesce(l.description,'')),

    l.hours_day, l.hours_night, l.hours_sat, l.hours_sun, l.hours_bh,

    l.pay_day, l.pay_night, l.pay_sat, l.pay_sun, l.pay_bh,
    l.charge_day, l.charge_night, l.charge_sat, l.charge_sun, l.charge_bh,

    public._inv_round2(-1 * coalesce(l.total_pay_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.total_charge_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.margin_ex_vat,0)),

    l.vat_rate_pct,
    public._inv_round2(-1 * coalesce(l.vat_amount,0)),
    public._inv_round2(-1 * coalesce(l.total_inc_vat,0)),

    l.paper_ts_r2_key,

    (coalesce(l.meta_json,'{}'::jsonb) ||
      jsonb_build_object(
        'credit_note', true,
        'original_invoice_id', v_inv.id::text,
        'original_invoice_line_id', l.id::text
      )
    ),

    ('CN:' || v_credit_id::text || ':LINE:' || l.id::text)
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  -- Update credit note totals from its lines
  select
    coalesce(sum(l2.total_charge_ex_vat),0)::numeric,
    coalesce(sum(l2.vat_amount),0)::numeric,
    coalesce(sum(l2.total_inc_vat),0)::numeric
  into v_cn_ex, v_cn_vat, v_cn_inc
  from public.invoice_lines l2
  where l2.invoice_id = v_credit_id;

  update public.invoices
  set
    subtotal_ex_vat = public._inv_round2(v_cn_ex),
    vat_amount      = public._inv_round2(v_cn_vat),
    total_inc_vat   = public._inv_round2(v_cn_inc),
    updated_at      = v_now
  where id = v_credit_id;

  -- Audit credit note creation (includes totals)
  perform public._audit_insert(
    'invoice',
    v_credit_id::text,
    'CREDIT_NOTE_CREATED',
    null,
    jsonb_build_object(
      'credit_note_id', v_credit_id::text,
      'original_invoice_id', v_inv.id::text,
      'subtotal_ex_vat', public._inv_round2(v_cn_ex),
      'vat_amount', public._inv_round2(v_cn_vat),
      'total_inc_vat', public._inv_round2(v_cn_inc)
    ),
    null,
    p_actor_user_id
  );

  -- Unlock snapshots locked by the original invoice
  select array_agg(distinct tf.timesheet_id)
  into v_ts_ids
  from public.timesheets_financials tf
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null;

  unlocked_snapshots := coalesce(array_length(v_ts_ids, 1), 0);

  if unlocked_snapshots > 0 then
    update public.timesheets_financials tf
    set locked_by_invoice_id = null,
        locked_at_utc = null,
        unlocked_by_credit_note_id = v_credit_id,
        is_stale = true,
        stale_reason = 'UNLOCKED_BY_CREDIT',
        updated_at = v_now
    where tf.is_current = true
      and tf.locked_by_invoice_id = p_invoice_id;

    -- Enqueue recompute (batch, idempotent)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    select
      x.timesheet_id,
      'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,
      v_now,
      null,
      v_now
    from (select unnest(v_ts_ids) as timesheet_id) x
    on conflict on constraint uq_tsfin_outbox do nothing;

    perform public._audit_insert(
      'invoice',
      v_credit_id::text,
      'CREDIT_NOTE_UNLOCKED_SNAPSHOTS',
      null,
      jsonb_build_object(
        'credit_note_id', v_credit_id::text,
        'original_invoice_id', v_inv.id::text,
        'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
        'unlocked_count', unlocked_snapshots
      ),
      null,
      p_actor_user_id
    );
  end if;

  credit_note_id := v_credit_id;
  return next;
end;
$$;
