create or replace function public.contracts_clone_and_extend_atomic(
  p_contract_id uuid,
  p_new_start_date date,
  p_new_end_date date,
  p_end_existing_on date,
  p_assign_existing_candidate boolean,
  p_new_candidate_id uuid default null,
  p_split_worker_note text default null,
  p_successor_overrides jsonb default null,
  p_force_schedule_clashes boolean default false,
  p_force_already_split_week boolean default false,
  p_confirmed_split_week boolean default false,
  p_actor_user_id uuid default null
) returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now                 timestamptz := now();

  v_cur                 public.contracts%rowtype;
  v_succ                public.contracts%rowtype;

  v_ov                  jsonb := coalesce(p_successor_overrides, '{}'::jsonb);

  v_close_to            date := p_end_existing_on;
  v_new_start           date := p_new_start_date;
  v_new_end             date := p_new_end_date;

  v_wew_pred            int;
  v_wew_succ            int;

  v_end_we_old          date;
  v_boundary_week_end   date;
  v_boundary_week_start date;

  v_split_week          boolean := false;
  v_already_split       boolean := false;
  v_split_note          text := null;
  v_split_group_key     text := null;
  v_old_mask            text := null;
  v_new_mask            text := null;

  v_schedule_clashes    jsonb := null;
  v_overlap_warnings    jsonb := '[]'::jsonb;
  v_clash_count         int := 0;

  v_err                 jsonb;

  -- Successor computed fields (merged overrides)
  v_succ_candidate_id   uuid;
  v_succ_client_id      uuid;
  v_succ_role           text;
  v_succ_band           text;
  v_succ_display_site   text;
  v_succ_ward_hint      text;

  v_succ_pay_method_snapshot text;
  v_succ_rates_json          jsonb;
  v_succ_std_schedule_json   jsonb;
  v_succ_std_hours_json      jsonb;
  v_succ_bucket_labels_json  jsonb;
  v_succ_additional_rates_json jsonb;

  v_succ_weekly_timesheet_source public.weekly_timesheet_source_enum;
  v_succ_overrideclientsettings boolean;
  v_succ_no_timesheet_required boolean;
  v_succ_daily_calc_of_invoices boolean;
  v_succ_group_nightsat_sunbh boolean;
  v_succ_is_nhsp boolean;
  v_succ_autoprocess_hr boolean;
  v_succ_requires_hr boolean;
  v_succ_hr_attach_to_invoice boolean;
  v_succ_ts_attach_to_invoice boolean;
  v_succ_reference_number_required_to_issue_invoice boolean;
  v_succ_send_manual_invoices_to_different_email boolean;
  v_succ_manual_invoices_alt_email_address text;
  v_succ_is_ad_hoc boolean;
  v_succ_default_submission_mode public.submission_mode_enum;

  -- std_hours derivation scratch
  v_day_key text;
  v_day_cfg jsonb;
  v_start_str text;
  v_end_str text;
  v_break_minutes numeric;
  v_start_h int;
  v_start_m int;
  v_end_h int;
  v_end_m int;
  v_start_mins int;
  v_end_mins int;
  v_minutes int;
  v_expected_minutes int;
  v_hours numeric;

  -- Split boundary hard-block helper
  v_bad_contract_id uuid;
  v_bad_timesheet_id uuid;

  -- Audit helpers
  v_before_state jsonb;
  v_after_state jsonb;
  v_audit_reason text;

  -- sink for CTE
  v_dummy int;

  -- =====================================================
  -- DEBUGGING (gated by settings_defaults.invoice_debug)
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := clock_timestamp();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_detail text := null;
  v_dbg_hint text := null;
  v_dbg_context text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_rc int := 0;

begin
  -- ─────────────────────────────────────────────────────────────
  -- Load invoice_debug flag (safe if column/table not present)
  -- ─────────────────────────────────────────────────────────────
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
      from public.settings_defaults sd
     where sd.id = 1
     limit 1;
  exception
    when undefined_column then
      v_invoice_debug := false;
    when undefined_table then
      v_invoice_debug := false;
    when others then
      -- never allow debug flag read to break functional flow
      v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'inputs', jsonb_build_object(
          'p_contract_id', coalesce(p_contract_id::text,''),
          'p_new_start_date', coalesce(p_new_start_date::text,''),
          'p_new_end_date', coalesce(p_new_end_date::text,''),
          'p_end_existing_on', coalesce(p_end_existing_on::text,''),
          'p_assign_existing_candidate', coalesce(p_assign_existing_candidate, true),
          'p_new_candidate_id', coalesce(p_new_candidate_id::text,''),
          'p_split_worker_note', coalesce(p_split_worker_note,''),
          'p_successor_overrides_type', case when p_successor_overrides is null then 'null' else jsonb_typeof(p_successor_overrides) end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false),
          'p_force_already_split_week', coalesce(p_force_already_split_week,false),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'p_actor_user_id', coalesce(p_actor_user_id::text,'')
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Basic input validation
  -- ─────────────────────────────────────────────────────────────
  if p_contract_id is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_contract_id is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','p_contract_id is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start is null or v_new_end is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date and p_new_end_date are required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','new_start or new_end is null',
          'error', v_err,
          'computed', jsonb_build_object('v_new_start', coalesce(v_new_start::text,''), 'v_new_end', coalesce(v_new_end::text,''))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start > v_new_end then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date must be <= p_new_end_date');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_new_start > v_new_end',
          'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_end_existing_on is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_close_to is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','validation_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end, 'v_close_to', v_close_to)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Lock predecessor contract
  -- ─────────────────────────────────────────────────────────────
  select c.*
    into v_cur
    from public.contracts as c
   where c.id = p_contract_id
   for update;

  if not found then
    v_err := jsonb_build_object('error','CONTRACT_NOT_FOUND','contract_id',p_contract_id);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','contract_not_found',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_predecessor',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'predecessor', jsonb_build_object(
          'id', v_cur.id,
          'candidate_id', coalesce(v_cur.candidate_id::text,''),
          'client_id', coalesce(v_cur.client_id::text,''),
          'start_date', v_cur.start_date,
          'end_date', v_cur.end_date,
          'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- Close window rules (end-existing enforced)
  if v_close_to < v_cur.start_date then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be >= predecessor.start_date',
      'predecessor_start_date', v_cur.start_date,
      'end_existing_on', v_close_to
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to < predecessor.start_date',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to > (v_new_start - 1) then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be <= p_new_start_date - 1 day',
      'end_existing_on', v_close_to,
      'new_start_date', v_new_start
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to > new_start - 1',
          'error', v_err,
          'computed', jsonb_build_object('new_start_minus_1', (v_new_start - 1))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','close_window_rules_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'predecessor_start_date', v_cur.start_date,
          'close_to', v_close_to,
          'new_start', v_new_start,
          'close_to_max', (v_new_start - 1)
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Determine successor candidate assignment
  -- ─────────────────────────────────────────────────────────────
  if coalesce(p_assign_existing_candidate, true) then
    v_succ_candidate_id := v_cur.candidate_id;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','succ_candidate_assignment',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'path','assign_existing_candidate',
          'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
        )
      );
    end if;

  else
    if p_new_candidate_id is not null then
      v_succ_candidate_id := p_new_candidate_id;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','explicit_param_new_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    elsif (v_ov ? 'candidate_id') and nullif(btrim(v_ov->>'candidate_id'), '') is not null then
      v_succ_candidate_id := (v_ov->>'candidate_id')::uuid;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','override_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    else
      v_succ_candidate_id := null;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','no_candidate_assigned',
            'successor_candidate_id',''
          )
        );
      end if;

    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Predecessor week-ending snapshot validation + derived dates
  -- ─────────────────────────────────────────────────────────────
  v_wew_pred := coalesce(v_cur.week_ending_weekday_snapshot, 0);

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','predecessor_wew_loaded',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'week_ending_weekday_snapshot', v_wew_pred
      )
    );
  end if;

  if v_wew_pred < 0 or v_wew_pred > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_CONTRACT_STATE',
      'message','predecessor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_predecessor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- endWEOld = week ending date of the week containing close_to (using predecessor snapshot)
  v_end_we_old :=
    (v_close_to + (((v_wew_pred - extract(dow from v_close_to)::int + 7) % 7)) * interval '1 day')::date;

  -- boundary week for newStart (using predecessor snapshot)
  v_boundary_week_end :=
    (v_new_start + (((v_wew_pred - extract(dow from v_new_start)::int + 7) % 7)) * interval '1 day')::date;
  v_boundary_week_start := (v_boundary_week_end - interval '6 days')::date;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','derived_dates',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'v_wew_pred', v_wew_pred,
          'v_end_we_old', v_end_we_old,
          'v_boundary_week_start', v_boundary_week_start,
          'v_boundary_week_end', v_boundary_week_end,
          'v_new_start', v_new_start,
          'v_new_end', v_new_end,
          'v_close_to', v_close_to
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Hard block: submitted beyond close window
  -- ─────────────────────────────────────────────────────────────
  if exists (
    select 1
      from public.contract_weeks as cw
     where cw.contract_id = v_cur.id
       and cw.timesheet_id is not null
       and cw.week_ending_date > v_end_we_old
  ) then
    v_err := jsonb_build_object(
      'error','SUBMITTED_BEYOND_CLOSE',
      'contract_id', v_cur.id,
      'end_week_ending_date', v_end_we_old
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_submitted_beyond_close',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','submitted_beyond_close_check_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'contract_id', v_cur.id::text,
        'end_we_old', v_end_we_old
      )
    );
  end if;

  -- NEW HARD RULE: ending week submitted + midweek truncation
  if v_close_to < v_end_we_old then
    if exists (
      select 1
        from public.contract_weeks as cw
       where cw.contract_id = v_cur.id
         and cw.week_ending_date = v_end_we_old
         and cw.timesheet_id is not null
    ) then
      v_err := jsonb_build_object(
        'error','ENDING_WEEK_SUBMITTED_CANNOT_TRUNCATE',
        'contract_id', v_cur.id,
        'close_to', v_close_to,
        'week_ending_date', v_end_we_old
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_ending_week_submitted_midweek_truncation',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ending_week_truncation_rule_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'close_to', v_close_to,
        'end_we_old', v_end_we_old,
        'is_midweek_truncation', (v_close_to < v_end_we_old)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week eligibility (same candidate on both halves + newStart midweek + predecessor ends in that same week)
  -- ─────────────────────────────────────────────────────────────
  v_split_week := (
    v_cur.candidate_id is not null
    and v_succ_candidate_id is not null
    and v_cur.candidate_id = v_succ_candidate_id
    and v_new_start <> v_boundary_week_start
    and v_end_we_old = v_boundary_week_end
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','split_week_eligibility',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'components', jsonb_build_object(
          'pred_candidate_not_null', (v_cur.candidate_id is not null),
          'succ_candidate_not_null', (v_succ_candidate_id is not null),
          'same_candidate', (v_cur.candidate_id is not null and v_succ_candidate_id is not null and v_cur.candidate_id = v_succ_candidate_id),
          'new_start_is_midweek', (v_new_start <> v_boundary_week_start),
          'pred_end_week_equals_boundary_week', (v_end_we_old = v_boundary_week_end)
        ),
        'result', v_split_week
      )
    );
  end if;

  if v_split_week then
    -- Hard block: boundary week already submitted (contract-based definition)
    select cw2.contract_id, cw2.timesheet_id
      into v_bad_contract_id, v_bad_timesheet_id
      from public.contracts as c2
      join public.contract_weeks as cw2
        on cw2.contract_id = c2.id
     where c2.candidate_id = v_cur.candidate_id
       and c2.client_id = v_cur.client_id
       and c2.start_date <= v_boundary_week_end
       and c2.end_date >= v_boundary_week_start
       and cw2.week_ending_date = v_boundary_week_end
       and cw2.timesheet_id is not null
     limit 1;

    if found then
      v_err := jsonb_build_object(
        'error','BOUNDARY_WEEK_TIMESHEET_ALREADY_SUBMITTED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_week_end', v_boundary_week_end,
        'contract_id', v_bad_contract_id,
        'timesheet_id', v_bad_timesheet_id
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_boundary_week_already_submitted',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'found_contract_id', coalesce(v_bad_contract_id::text,''),
            'found_timesheet_id', coalesce(v_bad_timesheet_id::text,''),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','boundary_week_submitted_check_ok',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'boundary_week_end', v_boundary_week_end
        )
      );
    end if;

    -- Already split detection (another overlapping contract for same candidate+client in boundary week)
    select exists (
      select 1
        from public.contracts as c2
       where c2.id <> v_cur.id
         and c2.candidate_id = v_cur.candidate_id
         and c2.client_id = v_cur.client_id
         and c2.start_date <= v_boundary_week_end
         and c2.end_date >= v_boundary_week_start
    ) into v_already_split;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','already_split_detection',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'already_split', v_already_split,
          'p_force_already_split_week', coalesce(p_force_already_split_week,false)
        )
      );
    end if;

    if v_already_split and not coalesce(p_force_already_split_week, false) then
      v_err := jsonb_build_object(
        'error','ALREADY_SPLIT_WEEK',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'overlapping_contract_ids', (
          select coalesce(jsonb_agg(c2.id order by c2.start_date), '[]'::jsonb)
            from public.contracts as c2
           where c2.candidate_id = v_cur.candidate_id
             and c2.client_id = v_cur.client_id
             and c2.start_date <= v_boundary_week_end
             and c2.end_date >= v_boundary_week_start
        )
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_already_split_week_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    -- Allowed day masks (Mon..Sun)
    -- Old half: max(predecessor.start_date, week_start) .. closeTo
    if greatest(v_cur.start_date, v_boundary_week_start) > v_close_to then
      v_old_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_old_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   greatest(v_cur.start_date, v_boundary_week_start)::timestamp,
                   v_close_to::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    end if;

    -- New half: newStart .. min(newEnd, week_end)
    if v_new_start > least(v_new_end, v_boundary_week_end) then
      v_new_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_new_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   v_new_start::timestamp,
                   least(v_new_end, v_boundary_week_end)::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    end if;

    -- Confirm gating (note required)
    v_split_note := btrim(coalesce(p_split_worker_note, ''));

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirm_gating',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'split_note_empty', (v_split_note = ''),
          'split_note_len', length(v_split_note),
          'old_mask', v_old_mask,
          'new_mask', v_new_mask
        )
      );
    end if;

    if (not coalesce(p_confirmed_split_week, false)) or v_split_note = '' then
      v_err := jsonb_build_object(
        'error','SPLIT_WEEK_CONFIRM_REQUIRED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
        'old_allowed_to', v_close_to,
        'new_allowed_from', v_new_start,
        'new_allowed_to', least(v_new_end, v_boundary_week_end),
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'suggested_worker_note',
          ('Contract rates have changed this week and therefore you need to submit two timesheets. One timesheet for work completed for ' ||
           to_char(greatest(v_cur.start_date, v_boundary_week_start),'YYYY-MM-DD') || ' to ' ||
           to_char(v_close_to,'YYYY-MM-DD') || ' and another timesheet for ' ||
           to_char(v_new_start,'YYYY-MM-DD') || ' to ' ||
           to_char(least(v_new_end, v_boundary_week_end),'YYYY-MM-DD') || '.')
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_split_week_confirm_required',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    v_split_group_key := gen_random_uuid()::text;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirmed',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'split_group_key', v_split_group_key,
          'worker_note', v_split_note
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Build successor merged fields from overrides
  -- ─────────────────────────────────────────────────────────────
  v_succ_client_id := coalesce(nullif(btrim(v_ov->>'client_id'), '')::uuid, v_cur.client_id);
  v_succ_role := coalesce(v_ov->>'role', v_cur.role);
  v_succ_band := coalesce(v_ov->>'band', v_cur.band);
  v_succ_display_site := coalesce(v_ov->>'display_site', v_cur.display_site);
  v_succ_ward_hint := coalesce(v_ov->>'ward_hint', v_cur.ward_hint);

  v_succ_pay_method_snapshot := coalesce(nullif(btrim(v_ov->>'pay_method_snapshot'), ''), v_cur.pay_method_snapshot);

  if (v_ov ? 'rates_json') and jsonb_typeof(v_ov->'rates_json') <> 'null' then
    v_succ_rates_json := v_ov->'rates_json';
  else
    v_succ_rates_json := v_cur.rates_json;
  end if;
  v_succ_rates_json := coalesce(v_succ_rates_json, '{}'::jsonb);

  if (v_ov ? 'std_schedule_json') then
    if jsonb_typeof(v_ov->'std_schedule_json') = 'null' then
      v_succ_std_schedule_json := null;
    else
      v_succ_std_schedule_json := v_ov->'std_schedule_json';
    end if;
  else
    v_succ_std_schedule_json := v_cur.std_schedule_json;
  end if;

  if v_succ_std_schedule_json is not null and jsonb_typeof(v_succ_std_schedule_json) = 'object' then
    -- Derive std_hours_json from std_schedule_json
    v_succ_std_hours_json := '{}'::jsonb;

    foreach v_day_key in array array['mon','tue','wed','thu','fri','sat','sun'] loop
      v_day_cfg := v_succ_std_schedule_json->v_day_key;

      if v_day_cfg is not null and jsonb_typeof(v_day_cfg) = 'object' then
        v_start_str := v_day_cfg->>'start';
        v_end_str := v_day_cfg->>'end';

        if v_start_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_start_h := split_part(v_start_str,':',1)::int;
          v_start_m := split_part(v_start_str,':',2)::int;
          if v_start_h between 0 and 23 and v_start_m between 0 and 59 then
            v_start_mins := v_start_h * 60 + v_start_m;
          else
            v_start_mins := null;
          end if;
        else
          v_start_mins := null;
        end if;

        if v_end_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_end_h := split_part(v_end_str,':',1)::int;
          v_end_m := split_part(v_end_str,':',2)::int;
          if v_end_h between 0 and 23 and v_end_m between 0 and 59 then
            v_end_mins := v_end_h * 60 + v_end_m;
          else
            v_end_mins := null;
          end if;
        else
          v_end_mins := null;
        end if;

        v_break_minutes := 0;
        if (v_day_cfg ? 'break_minutes')
          and (v_day_cfg->>'break_minutes') is not null
          and (v_day_cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
        then
          v_break_minutes := (v_day_cfg->>'break_minutes')::numeric;
        end if;

        if v_start_mins is not null and v_end_mins is not null then
          if v_end_mins <= v_start_mins then
            v_minutes := 1440 - v_start_mins + v_end_mins;
          else
            v_minutes := v_end_mins - v_start_mins;
          end if;

          v_expected_minutes := greatest(0, (v_minutes - v_break_minutes)::int);
          v_hours := round((v_expected_minutes::numeric / 60.0), 2);
        else
          v_hours := 0;
        end if;
      else
        v_hours := 0;
      end if;

      v_succ_std_hours_json := v_succ_std_hours_json || jsonb_build_object(v_day_key, v_hours);
    end loop;
  else
    if (v_ov ? 'std_hours_json') then
      if jsonb_typeof(v_ov->'std_hours_json') = 'null' then
        v_succ_std_hours_json := null;
      else
        v_succ_std_hours_json := v_ov->'std_hours_json';
      end if;
    else
      v_succ_std_hours_json := v_cur.std_hours_json;
    end if;
  end if;

  if (v_ov ? 'bucket_labels_json') then
    if jsonb_typeof(v_ov->'bucket_labels_json') = 'null' then
      v_succ_bucket_labels_json := null;
    else
      v_succ_bucket_labels_json := v_ov->'bucket_labels_json';
    end if;
  else
    v_succ_bucket_labels_json := v_cur.bucket_labels_json;
  end if;

  if (v_ov ? 'additional_rates_json') then
    if jsonb_typeof(v_ov->'additional_rates_json') = 'null' then
      v_succ_additional_rates_json := null;
    else
      v_succ_additional_rates_json := v_ov->'additional_rates_json';
    end if;
  else
    v_succ_additional_rates_json := v_cur.additional_rates_json;
  end if;

  if (v_ov ? 'weekly_timesheet_source') then
    v_succ_weekly_timesheet_source := nullif(btrim(v_ov->>'weekly_timesheet_source'), '')::public.weekly_timesheet_source_enum;
  else
    v_succ_weekly_timesheet_source := v_cur.weekly_timesheet_source;
  end if;

  if (v_ov ? 'overrideclientsettings') and jsonb_typeof(v_ov->'overrideclientsettings') <> 'null' then
    v_succ_overrideclientsettings := (v_ov->>'overrideclientsettings')::boolean;
  else
    v_succ_overrideclientsettings := v_cur.overrideclientsettings;
  end if;
  v_succ_overrideclientsettings := coalesce(v_succ_overrideclientsettings, false);

  if (v_ov ? 'no_timesheet_required') then
    if jsonb_typeof(v_ov->'no_timesheet_required') = 'null' then
      v_succ_no_timesheet_required := null;
    else
      v_succ_no_timesheet_required := (v_ov->>'no_timesheet_required')::boolean;
    end if;
  else
    v_succ_no_timesheet_required := v_cur.no_timesheet_required;
  end if;

  if (v_ov ? 'daily_calc_of_invoices') then
    if jsonb_typeof(v_ov->'daily_calc_of_invoices') = 'null' then
      v_succ_daily_calc_of_invoices := null;
    else
      v_succ_daily_calc_of_invoices := (v_ov->>'daily_calc_of_invoices')::boolean;
    end if;
  else
    v_succ_daily_calc_of_invoices := v_cur.daily_calc_of_invoices;
  end if;

  if (v_ov ? 'group_nightsat_sunbh') then
    if jsonb_typeof(v_ov->'group_nightsat_sunbh') = 'null' then
      v_succ_group_nightsat_sunbh := null;
    else
      v_succ_group_nightsat_sunbh := (v_ov->>'group_nightsat_sunbh')::boolean;
    end if;
  else
    v_succ_group_nightsat_sunbh := v_cur.group_nightsat_sunbh;
  end if;

  if (v_ov ? 'is_nhsp') then
    if jsonb_typeof(v_ov->'is_nhsp') = 'null' then
      v_succ_is_nhsp := null;
    else
      v_succ_is_nhsp := (v_ov->>'is_nhsp')::boolean;
    end if;
  else
    v_succ_is_nhsp := v_cur.is_nhsp;
  end if;

  if (v_ov ? 'autoprocess_hr') then
    if jsonb_typeof(v_ov->'autoprocess_hr') = 'null' then
      v_succ_autoprocess_hr := null;
    else
      v_succ_autoprocess_hr := (v_ov->>'autoprocess_hr')::boolean;
    end if;
  else
    v_succ_autoprocess_hr := v_cur.autoprocess_hr;
  end if;

  if (v_ov ? 'requires_hr') then
    if jsonb_typeof(v_ov->'requires_hr') = 'null' then
      v_succ_requires_hr := null;
    else
      v_succ_requires_hr := (v_ov->>'requires_hr')::boolean;
    end if;
  else
    v_succ_requires_hr := v_cur.requires_hr;
  end if;

  if (v_ov ? 'hr_attach_to_invoice') then
    if jsonb_typeof(v_ov->'hr_attach_to_invoice') = 'null' then
      v_succ_hr_attach_to_invoice := null;
    else
      v_succ_hr_attach_to_invoice := (v_ov->>'hr_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_hr_attach_to_invoice := v_cur.hr_attach_to_invoice;
  end if;

  if (v_ov ? 'ts_attach_to_invoice') then
    if jsonb_typeof(v_ov->'ts_attach_to_invoice') = 'null' then
      v_succ_ts_attach_to_invoice := null;
    else
      v_succ_ts_attach_to_invoice := (v_ov->>'ts_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_ts_attach_to_invoice := v_cur.ts_attach_to_invoice;
  end if;

  if (v_ov ? 'reference_number_required_to_issue_invoice') then
    if jsonb_typeof(v_ov->'reference_number_required_to_issue_invoice') = 'null' then
      v_succ_reference_number_required_to_issue_invoice := null;
    else
      v_succ_reference_number_required_to_issue_invoice := (v_ov->>'reference_number_required_to_issue_invoice')::boolean;
    end if;
  else
    v_succ_reference_number_required_to_issue_invoice := v_cur.reference_number_required_to_issue_invoice;
  end if;

  if (v_ov ? 'send_manual_invoices_to_different_email') then
    if jsonb_typeof(v_ov->'send_manual_invoices_to_different_email') = 'null' then
      v_succ_send_manual_invoices_to_different_email := null;
    else
      v_succ_send_manual_invoices_to_different_email := (v_ov->>'send_manual_invoices_to_different_email')::boolean;
    end if;
  else
    v_succ_send_manual_invoices_to_different_email := v_cur.send_manual_invoices_to_different_email;
  end if;

  if (v_ov ? 'manual_invoices_alt_email_address') then
    v_succ_manual_invoices_alt_email_address := nullif(btrim(v_ov->>'manual_invoices_alt_email_address'), '');
  else
    v_succ_manual_invoices_alt_email_address := v_cur.manual_invoices_alt_email_address;
  end if;

  if (v_ov ? 'is_ad_hoc') and jsonb_typeof(v_ov->'is_ad_hoc') <> 'null' then
    v_succ_is_ad_hoc := (v_ov->>'is_ad_hoc')::boolean;
  else
    v_succ_is_ad_hoc := v_cur.is_ad_hoc;
  end if;
  v_succ_is_ad_hoc := coalesce(v_succ_is_ad_hoc, false);

  if (v_ov ? 'default_submission_mode') then
    v_succ_default_submission_mode := nullif(btrim(v_ov->>'default_submission_mode'), '')::public.submission_mode_enum;
  else
    v_succ_default_submission_mode := v_cur.default_submission_mode;
  end if;

  if (v_ov ? 'week_ending_weekday_snapshot') and nullif(btrim(v_ov->>'week_ending_weekday_snapshot'), '') is not null then
    v_wew_succ := (v_ov->>'week_ending_weekday_snapshot')::int;
  else
    v_wew_succ := coalesce(v_cur.week_ending_weekday_snapshot, 0);
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','successor_fields_merged',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'merged', jsonb_build_object(
          'succ_candidate_id', coalesce(v_succ_candidate_id::text,''),
          'succ_client_id', coalesce(v_succ_client_id::text,''),
          'succ_role', coalesce(v_succ_role,''),
          'succ_band', coalesce(v_succ_band,''),
          'succ_display_site', coalesce(v_succ_display_site,''),
          'succ_ward_hint', coalesce(v_succ_ward_hint,''),
          'succ_pay_method_snapshot', coalesce(v_succ_pay_method_snapshot,''),
          'succ_week_ending_weekday_snapshot', v_wew_succ,
          'succ_overrideclientsettings', coalesce(v_succ_overrideclientsettings,false),
          'succ_weekly_timesheet_source', coalesce(v_succ_weekly_timesheet_source::text,''),
          'succ_no_timesheet_required', case when v_succ_no_timesheet_required is null then null else v_succ_no_timesheet_required end,
          'succ_daily_calc_of_invoices', case when v_succ_daily_calc_of_invoices is null then null else v_succ_daily_calc_of_invoices end,
          'succ_group_nightsat_sunbh', case when v_succ_group_nightsat_sunbh is null then null else v_succ_group_nightsat_sunbh end,
          'succ_is_nhsp', case when v_succ_is_nhsp is null then null else v_succ_is_nhsp end,
          'succ_autoprocess_hr', case when v_succ_autoprocess_hr is null then null else v_succ_autoprocess_hr end,
          'succ_requires_hr', case when v_succ_requires_hr is null then null else v_succ_requires_hr end,
          'succ_hr_attach_to_invoice', case when v_succ_hr_attach_to_invoice is null then null else v_succ_hr_attach_to_invoice end,
          'succ_ts_attach_to_invoice', case when v_succ_ts_attach_to_invoice is null then null else v_succ_ts_attach_to_invoice end,
          'succ_reference_number_required_to_issue_invoice', case when v_succ_reference_number_required_to_issue_invoice is null then null else v_succ_reference_number_required_to_issue_invoice end,
          'succ_send_manual_invoices_to_different_email', case when v_succ_send_manual_invoices_to_different_email is null then null else v_succ_send_manual_invoices_to_different_email end,
          'succ_manual_invoices_alt_email_address', coalesce(v_succ_manual_invoices_alt_email_address,''),
          'succ_is_ad_hoc', coalesce(v_succ_is_ad_hoc,false),
          'succ_default_submission_mode', coalesce(v_succ_default_submission_mode::text,'')
        ),
        'overrides_keys', (
          select coalesce(jsonb_agg(k), '[]'::jsonb)
          from jsonb_object_keys(v_ov) as k
        )
      )
    );
  end if;

  if v_wew_succ < 0 or v_wew_succ > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','successor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_wew_succ
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_successor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- Route flag validations
  if v_succ_is_nhsp is true and v_succ_autoprocess_hr is true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','is_nhsp and autoprocess_hr cannot both be true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','is_nhsp && autoprocess_hr',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_succ_no_timesheet_required is true and v_succ_autoprocess_hr is distinct from true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','no_timesheet_required=true requires autoprocess_hr=true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','no_timesheet_required && autoprocess_hr != true',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','route_flags_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
      )
    );
  end if;

  -- Snapshot predecessor state for audit before mutations
  v_before_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_start_date', v_cur.start_date,
    'predecessor_end_date', v_cur.end_date,
    'predecessor_candidate_id', v_cur.candidate_id,
    'predecessor_client_id', v_cur.client_id,
    'new_start_date', v_new_start,
    'new_end_date', v_new_end,
    'end_existing_on', v_close_to
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','before_state_snapshot',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'before_state', v_before_state
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Insert successor contract
  -- ─────────────────────────────────────────────────────────────
  insert into public.contracts as c (
    candidate_id,
    client_id,
    role,
    band,
    display_site,
    ward_hint,
    start_date,
    end_date,
    pay_method_snapshot,
    rates_json,
    std_hours_json,
    default_submission_mode,
    week_ending_weekday_snapshot,
    auto_invoice,
    require_reference_to_pay,
    require_reference_to_invoice,
    bucket_labels_json,
    std_schedule_json,
    mileage_pay_rate,
    mileage_charge_rate,
    additional_rates_json,
    created_at,
    updated_at,
    self_bill,
    weekly_timesheet_source,
    no_timesheet_required,
    daily_calc_of_invoices,
    group_nightsat_sunbh,
    is_nhsp,
    autoprocess_hr,
    requires_hr,
    hr_attach_to_invoice,
    ts_attach_to_invoice,
    overrideclientsettings,
    reference_number_required_to_issue_invoice,
    send_manual_invoices_to_different_email,
    manual_invoices_alt_email_address,
    is_ad_hoc
  ) values (
    v_succ_candidate_id,
    v_succ_client_id,
    v_succ_role,
    v_succ_band,
    v_succ_display_site,
    v_succ_ward_hint,
    v_new_start,
    v_new_end,
    v_succ_pay_method_snapshot,
    v_succ_rates_json,
    v_succ_std_hours_json,
    v_succ_default_submission_mode,
    v_wew_succ::smallint,
    v_cur.auto_invoice,
    v_cur.require_reference_to_pay,
    v_cur.require_reference_to_invoice,
    v_succ_bucket_labels_json,
    v_succ_std_schedule_json,
    v_cur.mileage_pay_rate,
    v_cur.mileage_charge_rate,
    v_succ_additional_rates_json,
    v_now,
    v_now,
    v_cur.self_bill,
    v_succ_weekly_timesheet_source,
    v_succ_no_timesheet_required,
    v_succ_daily_calc_of_invoices,
    v_succ_group_nightsat_sunbh,
    v_succ_is_nhsp,
    v_succ_autoprocess_hr,
    v_succ_requires_hr,
    v_succ_hr_attach_to_invoice,
    v_succ_ts_attach_to_invoice,
    v_succ_overrideclientsettings,
    v_succ_reference_number_required_to_issue_invoice,
    v_succ_send_manual_invoices_to_different_email,
    v_succ_manual_invoices_alt_email_address,
    v_succ_is_ad_hoc
  )
  returning c.* into v_succ;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','inserted_successor_contract',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'successor', jsonb_build_object(
          'id', v_succ.id,
          'candidate_id', coalesce(v_succ.candidate_id::text,''),
          'client_id', coalesce(v_succ.client_id::text,''),
          'start_date', v_succ.start_date,
          'end_date', v_succ.end_date,
          'week_ending_weekday_snapshot', v_succ.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Update predecessor end_date (end-existing enforced)
  -- ─────────────────────────────────────────────────────────────
  update public.contracts as c
     set end_date = v_close_to,
         updated_at = v_now
   where c.id = v_cur.id;

  get diagnostics v_rc = row_count;

  v_cur.end_date := v_close_to;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','updated_predecessor_end_date',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'predecessor_id', v_cur.id::text,
        'new_end_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Generate/ensure base contract_weeks rows for predecessor + successor in their final windows
  -- (including planned_schedule_json derived from std_schedule_json when present)
  -- ─────────────────────────────────────────────────────────────
  with contract_targets as (
    select
      v_cur.id as contract_id,
      v_cur.client_id as client_id,
      v_cur.start_date as start_date,
      v_cur.end_date as end_date,
      v_wew_pred as wew,
      v_cur.std_schedule_json as std_schedule_json,
      v_cur.overrideclientsettings as overrideclientsettings,
      v_cur.default_submission_mode as default_submission_mode
    union all
    select
      v_succ.id as contract_id,
      v_succ.client_id as client_id,
      v_succ.start_date as start_date,
      v_succ.end_date as end_date,
      v_wew_succ as wew,
      v_succ.std_schedule_json as std_schedule_json,
      v_succ.overrideclientsettings as overrideclientsettings,
      v_succ.default_submission_mode as default_submission_mode
  ),
  targets as (
    select
      ct.contract_id,
      ct.client_id,
      ct.start_date,
      ct.end_date,
      ct.wew,
      ct.std_schedule_json,
      ct.overrideclientsettings,
      ct.default_submission_mode,
      (
        select cs.default_submission_mode
          from public.client_settings as cs
         where cs.client_id = ct.client_id
         order by cs.effective_from desc nulls last, cs.updated_at desc
         limit 1
      ) as client_default_submission_mode
    from contract_targets as ct
  ),
  targets2 as (
    select
      t.contract_id,
      t.start_date,
      t.end_date,
      t.wew,
      t.std_schedule_json,
      case
        when t.overrideclientsettings is true
          then coalesce(t.default_submission_mode, t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
        else coalesce(t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
      end as submission_mode_snapshot,
      (t.start_date + (((t.wew - extract(dow from t.start_date)::int + 7) % 7)) * interval '1 day')::date as start_we,
      (t.end_date   + (((t.wew - extract(dow from t.end_date)::int   + 7) % 7)) * interval '1 day')::date as end_we
    from targets as t
  ),
  weeks as (
    select
      t2.contract_id,
      t2.start_date,
      t2.end_date,
      t2.wew,
      t2.std_schedule_json,
      t2.submission_mode_snapshot,
      gs::date as week_end
    from targets2 as t2
    cross join lateral generate_series(t2.start_we::timestamp, t2.end_we::timestamp, interval '7 days') as gs
  ),
  ins as (
    insert into public.contract_weeks as cw (
      contract_id,
      week_ending_date,
      additional_seq,
      status,
      timesheet_id,
      planned_schedule_json,
      created_at,
      updated_at,
      submission_mode_snapshot,
      is_adjustment,
      enforce_day_partition,
      allowed_days_mask,
      split_boundary_date,
      worker_note,
      split_group_key
    )
    select
      w.contract_id,
      w.week_end,
      0 as additional_seq,
      case
        when w.week_end <= current_date then 'OPEN'::public.contract_week_status_enum
        else 'PLANNED'::public.contract_week_status_enum
      end as status,
      null::uuid as timesheet_id,
      case
        when pj.plan_json is null then null
        when jsonb_typeof(pj.plan_json) <> 'array' then pj.plan_json
        when jsonb_array_length(pj.plan_json) = 0 then null
        else pj.plan_json
      end as planned_schedule_json,
      v_now as created_at,
      v_now as updated_at,
      w.submission_mode_snapshot,
      false as is_adjustment,
      false as enforce_day_partition,
      null::text as allowed_days_mask,
      null::date as split_boundary_date,
      null::text as worker_note,
      null::text as split_group_key
    from weeks as w
    left join lateral (
      select jsonb_agg(ent.entry_json order by ent.entry_date) as plan_json
      from (
        select
          (dt)::date as entry_date,
          jsonb_build_object(
            'date', to_char((dt)::date, 'YYYY-MM-DD'),
            'start', sc.cfg->>'start',
            'end',   sc.cfg->>'end',
            'breaks', case when jsonb_typeof(sc.cfg->'breaks') = 'array' then sc.cfg->'breaks' else '[]'::jsonb end,
            'break_minutes', br.break_minutes,
            'overnight', ov.overnight_flag,
            'expected_minutes', ov.expected_minutes
          ) as entry_json
        from generate_series(
               greatest(((w.week_end - interval '6 days')::date), w.start_date)::timestamp,
               least(w.week_end, w.end_date)::timestamp,
               interval '1 day'
             ) as dt
        cross join lateral (
          select case extract(dow from dt)::int
            when 1 then 'mon'
            when 2 then 'tue'
            when 3 then 'wed'
            when 4 then 'thu'
            when 5 then 'fri'
            when 6 then 'sat'
            else 'sun'
          end as day_key
        ) as dk
        cross join lateral (
          select (w.std_schedule_json -> dk.day_key) as cfg
        ) as sc
        cross join lateral (
          select
            case
              when (sc.cfg ? 'break_minutes')
               and (sc.cfg->>'break_minutes') is not null
               and (sc.cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
              then (sc.cfg->>'break_minutes')::numeric
              else 0::numeric
            end as break_minutes
        ) as br
        cross join lateral (
          select
            (sc.cfg->>'start') as start_str,
            (sc.cfg->>'end')   as end_str
        ) as se
        cross join lateral (
          select
            case
              when se.start_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.start_str,':',1)::int between 0 and 23
               and split_part(se.start_str,':',2)::int between 0 and 59
              then (split_part(se.start_str,':',1)::int * 60 + split_part(se.start_str,':',2)::int)
              else null
            end as start_mins,
            case
              when se.end_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.end_str,':',1)::int between 0 and 23
               and split_part(se.end_str,':',2)::int between 0 and 59
              then (split_part(se.end_str,':',1)::int * 60 + split_part(se.end_str,':',2)::int)
              else null
            end as end_mins
        ) as tm
        cross join lateral (
          select
            case
              when tm.start_mins is null or tm.end_mins is null then false
              when tm.end_mins <= tm.start_mins then true
              else false
            end as overnight_flag,
            case
              when tm.start_mins is null or tm.end_mins is null then 0
              when tm.end_mins <= tm.start_mins then (1440 - tm.start_mins + tm.end_mins)
              else (tm.end_mins - tm.start_mins)
            end as minutes_diff
        ) as md
        cross join lateral (
          select
            md.overnight_flag,
            greatest(0, (md.minutes_diff - br.break_minutes)::int) as expected_minutes
        ) as ov
        where w.std_schedule_json is not null
          and jsonb_typeof(w.std_schedule_json) = 'object'
          and jsonb_typeof(sc.cfg) = 'object'
          and tm.start_mins is not null
          and tm.end_mins is not null
      ) as ent
    ) as pj on true
    on conflict on constraint uq_contract_week do nothing
    returning 1
  )
  select 1 into v_dummy from ins limit 1;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ensure_contract_weeks_done',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'note','weeks ensure CTE executed (v_dummy indicates at least one insert when non-null)',
        'v_dummy', v_dummy,
        'predecessor_id', v_cur.id::text,
        'successor_id', v_succ.id::text
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Delete predecessor draft weeks beyond truncated window (never touches submitted weeks)
  -- ─────────────────────────────────────────────────────────────
  delete from public.contract_weeks as cw
   where cw.contract_id = v_cur.id
     and cw.timesheet_id is null
     and cw.week_ending_date > v_end_we_old;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','deleted_predecessor_draft_weeks_beyond_trunc',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'v_end_we_old', v_end_we_old
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Clamp predecessor ending week planned schedule (draft rows only; never delete the week row)
  -- If none remain, set planned_schedule_json = []
  -- ─────────────────────────────────────────────────────────────
  with tgt as (
    select
      cw.id as contract_week_id,
      case
        when cw.planned_schedule_json is null then '[]'::jsonb
        when jsonb_typeof(cw.planned_schedule_json) <> 'array' then cw.planned_schedule_json
        else (
          select coalesce(jsonb_agg(e.elem order by e.ord), '[]'::jsonb)
          from jsonb_array_elements(cw.planned_schedule_json) with ordinality as e(elem, ord)
          where (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            and (e.elem->>'date')::date <= v_close_to
        )
      end as new_plan
    from public.contract_weeks as cw
    where cw.contract_id = v_cur.id
      and cw.week_ending_date = v_end_we_old
      and cw.timesheet_id is null
  )
  update public.contract_weeks as cw
     set planned_schedule_json = tgt.new_plan,
         updated_at = v_now
    from tgt
   where cw.id = tgt.contract_week_id;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','clamped_predecessor_ending_week_plan',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'week_ending_date', v_end_we_old,
        'clamp_to_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week enforcement patch (draft rows only; includes additional_seq variants)
  -- ─────────────────────────────────────────────────────────────
  if v_split_week then
    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_old_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_cur.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_predecessor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_cur.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_old_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_new_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_succ.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_successor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_succ.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_new_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Compute schedule clashes inside DB (block unless forced)
  -- NOTE: avoid using CTE name "overlaps" (keyword/operator in SQL); use ovl_rows instead.
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_start',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'candidate_id', v_succ_candidate_id::text,
          'new_start', v_new_start,
          'new_end', v_new_end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    with params as (
      select
        (v_new_start - 1) as scan_from,
        (v_new_end + 1) as scan_to,
        (v_new_start - 7) as check_we_from,
        (v_new_end + 7) as check_we_to
    ),
    a_entries as (
      select
        cw.id as a_contract_week_id,
        cw.contract_id as a_contract_id,
        cw.week_ending_date as a_week_end,
        cw.additional_seq as a_additional_seq,
        (e.elem->>'date')::date as a_day_date,
        (e.elem->>'start') as a_start,
        (e.elem->>'end') as a_end,
        coalesce((e.elem->>'overnight')::boolean, false) as a_overnight
      from public.contract_weeks as cw
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      where cw.contract_id = v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= v_new_start
        and (e.elem->>'date')::date <= v_new_end
    ),
    a_ts as (
      select
        a.*,
        (a.a_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (a.a_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((a.a_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (a.a_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from a_entries as a
      cross join lateral (
        select
          case
            when a.a_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_start,':',1)::int between 0 and 23
             and split_part(a.a_start,':',2)::int between 0 and 59
            then (split_part(a.a_start,':',1)::int * 60 + split_part(a.a_start,':',2)::int)
            else null
          end as start_mins,
          case
            when a.a_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_end,':',1)::int between 0 and 23
             and split_part(a.a_end,':',2)::int between 0 and 59
            then (split_part(a.a_end,':',1)::int * 60 + split_part(a.a_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    b_entries as (
      select
        cw.id as b_contract_week_id,
        cw.contract_id as b_contract_id,
        c.client_id as b_client_id,
        cw.week_ending_date as b_week_end,
        cw.additional_seq as b_additional_seq,
        (e.elem->>'date')::date as b_day_date,
        (e.elem->>'start') as b_start,
        (e.elem->>'end') as b_end,
        coalesce((e.elem->>'overnight')::boolean, false) as b_overnight
      from public.contract_weeks as cw
      join public.contracts as c
        on c.id = cw.contract_id
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      cross join params as p
      where c.candidate_id = v_succ_candidate_id
        and c.id <> v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and cw.week_ending_date >= p.check_we_from
        and cw.week_ending_date <= p.check_we_to
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= p.scan_from
        and (e.elem->>'date')::date <= p.scan_to
    ),
    b_ts as (
      select
        b.*,
        (b.b_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (b.b_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((b.b_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (b.b_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from b_entries as b
      cross join lateral (
        select
          case
            when b.b_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_start,':',1)::int between 0 and 23
             and split_part(b.b_start,':',2)::int between 0 and 59
            then (split_part(b.b_start,':',1)::int * 60 + split_part(b.b_start,':',2)::int)
            else null
          end as start_mins,
          case
            when b.b_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_end,':',1)::int between 0 and 23
             and split_part(b.b_end,':',2)::int between 0 and 59
            then (split_part(b.b_end,':',1)::int * 60 + split_part(b.b_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    ovl_rows as (
      select
        greatest(a.start_ts, b.start_ts) as overlap_start,
        least(a.end_ts, b.end_ts) as overlap_end,

        a.a_contract_week_id,
        a.a_contract_id,
        a.a_week_end,
        a.a_additional_seq,
        to_char(a.a_day_date, 'YYYY-MM-DD') as a_date,
        a.a_start as a_start,
        a.a_end as a_end,

        b.b_contract_week_id,
        b.b_contract_id,
        b.b_client_id,
        b.b_week_end,
        b.b_additional_seq,
        to_char(b.b_day_date, 'YYYY-MM-DD') as b_date,
        b.b_start as b_start,
        b.b_end as b_end,

        row_number() over (order by greatest(a.start_ts, b.start_ts)) as rn,
        count(*) over () as total_count
      from a_ts as a
      join b_ts as b
        on a.start_ts < b.end_ts
       and a.end_ts > b.start_ts
      where least(a.end_ts, b.end_ts) > greatest(a.start_ts, b.start_ts)
    )
    select
      coalesce(max(o.total_count), 0),
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'overlap_start_utc', to_char(o.overlap_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'overlap_end_utc', to_char(o.overlap_end, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),

            'a_source', 'proposed',
            'a_contract_week_id', o.a_contract_week_id,
            'a_contract_id', o.a_contract_id,
            'a_week_end', o.a_week_end,
            'a_additional_seq', o.a_additional_seq,
            'a_date', o.a_date,
            'a_start', o.a_start,
            'a_end', o.a_end,

            'b_source', 'existing',
            'b_contract_week_id', o.b_contract_week_id,
            'b_contract_id', o.b_contract_id,
            'b_client_id', o.b_client_id,
            'b_week_end', o.b_week_end,
            'b_additional_seq', o.b_additional_seq,
            'b_date', o.b_date,
            'b_start', o.b_start,
            'b_end', o.b_end
          )
          order by o.overlap_start
        ) filter (where o.rn <= 500),
        '[]'::jsonb
      )
      into v_clash_count, v_schedule_clashes
      from ovl_rows as o;

    v_schedule_clashes := jsonb_build_object(
      'candidate_id', v_succ_candidate_id,
      'scan_from', to_char(v_new_start - 1, 'YYYY-MM-DD'),
      'scan_to', to_char(v_new_end + 1, 'YYYY-MM-DD'),
      'check_we_from', to_char(v_new_start - 7, 'YYYY-MM-DD'),
      'check_we_to', to_char(v_new_end + 7, 'YYYY-MM-DD'),
      'clash_count', coalesce(v_clash_count, 0),
      'clashes', coalesce(v_schedule_clashes, '[]'::jsonb)
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_done',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'clash_count', coalesce(v_clash_count,0),
          'forced', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    if coalesce(v_clash_count, 0) > 0 and not coalesce(p_force_schedule_clashes, false) then
      v_err := v_schedule_clashes || jsonb_build_object('error','SCHEDULE_CLASH');

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_schedule_clash_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Overlap warnings (date-range overlap with other contracts for successor candidate)
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', c2.id,
          'client_id', c2.client_id,
          'overlap_from', greatest(c2.start_date, v_new_start),
          'overlap_to', least(c2.end_date, v_new_end)
        )
        order by greatest(c2.start_date, v_new_start)
      ),
      '[]'::jsonb
    )
    into v_overlap_warnings
    from (
      select c2.*
      from public.contracts as c2
      where c2.candidate_id = v_succ_candidate_id
        and c2.id <> v_cur.id
        and c2.id <> v_succ.id
        and c2.start_date <= v_new_end
        and c2.end_date >= v_new_start
      order by c2.start_date
      limit 50
    ) as c2;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','overlap_warnings_computed',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'overlap_warning_count', case when v_overlap_warnings is null or jsonb_typeof(v_overlap_warnings) <> 'array' then null else jsonb_array_length(v_overlap_warnings) end
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Audit event (non-debug canonical writer)
  -- ─────────────────────────────────────────────────────────────
  v_after_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_closed_at', v_close_to,
    'successor_id', v_succ.id,
    'successor_start_date', v_succ.start_date,
    'successor_end_date', v_succ.end_date,
    'successor_candidate_id', v_succ.candidate_id,
    'successor_client_id', v_succ.client_id,
    'split_week', v_split_week,
    'split_group_key', v_split_group_key,
    'forced_schedule_clashes', coalesce(p_force_schedule_clashes, false),
    'forced_already_split_week', coalesce(p_force_already_split_week, false)
  );

  v_audit_reason :=
    'Clone & Extend: predecessor ' || v_cur.id::text ||
    ' closed to ' || to_char(v_close_to,'YYYY-MM-DD') ||
    '; successor ' || v_succ.id::text ||
    ' ' || to_char(v_succ.start_date,'YYYY-MM-DD') || '→' || to_char(v_succ.end_date,'YYYY-MM-DD') ||
    case when v_split_week then '; split week boundary ' || to_char(v_new_start,'YYYY-MM-DD') else '' end ||
    case when coalesce(p_force_schedule_clashes,false) then '; schedule clashes forced' else '' end ||
    case when coalesce(p_force_already_split_week,false) then '; already-split-week forced' else '' end;

  perform public._audit_insert(
    'contracts',
    v_cur.id::text,
    'CLONE_EXTEND',
    v_before_state,
    v_after_state,
    v_audit_reason,
    p_actor_user_id
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','canonical_audit_written',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'event','CLONE_EXTEND',
        'actor_user_id', coalesce(p_actor_user_id::text,'')
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- DEBUG AUDIT (single row per call; best-effort, never breaks flow)
  -- ─────────────────────────────────────────────────────────────
  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'predecessor_id', v_cur.id,
        'successor_id', v_succ.id,
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', v_boundary_week_start,
        'boundary_week_end', v_boundary_week_end,
        'end_we_old', v_end_we_old,
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_DEBUG',
        jsonb_build_object(
          'predecessor_id', v_cur.id::text,
          'successor_id', v_succ.id::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'warnings', jsonb_build_object(
            'schedule_clashes', v_schedule_clashes,
            'overlap_warnings', v_overlap_warnings
          )
        ),
        'contracts',
        ('contract:' || v_cur.id::text),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Return payload
  -- ─────────────────────────────────────────────────────────────
  return jsonb_build_object(
    'successor', jsonb_build_object(
      'id', v_succ.id,
      'candidate_id', v_succ.candidate_id,
      'client_id', v_succ.client_id,
      'role', v_succ.role,
      'band', v_succ.band,
      'display_site', v_succ.display_site,
      'ward_hint', v_succ.ward_hint,
      'start_date', v_succ.start_date,
      'end_date', v_succ.end_date
    ),
    'closed_at', v_close_to,
    'split', case
      when v_split_week then jsonb_build_object(
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'split_boundary_date', v_new_start,
        'worker_note', v_split_note,
        'split_group_key', v_split_group_key,
        'already_split', v_already_split
      )
      else null
    end,
    'overlap_warnings', v_overlap_warnings,
    'warnings', jsonb_build_object(
      'schedule_clashes', v_schedule_clashes,
      'overlap_warnings', v_overlap_warnings
    )
  );

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  begin
    get stacked diagnostics
      v_dbg_detail = PG_EXCEPTION_DETAIL,
      v_dbg_hint = PG_EXCEPTION_HINT,
      v_dbg_context = PG_EXCEPTION_CONTEXT;
  exception when others then
    v_dbg_detail := null;
    v_dbg_hint := null;
    v_dbg_context := null;
  end;

  if v_invoice_debug then
    begin
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','exception',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint
        )
      );

      v_dbg_stats := jsonb_build_object(
        'predecessor_id', coalesce(v_cur.id::text,''),
        'successor_id', coalesce(v_succ.id::text,''),
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', coalesce(v_boundary_week_start::text,''),
        'boundary_week_end', coalesce(v_boundary_week_end::text,''),
        'end_we_old', coalesce(v_end_we_old::text,''),
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_ERROR',
        jsonb_build_object(
          'predecessor_id', coalesce(v_cur.id::text,''),
          'successor_id', coalesce(v_succ.id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint,
          'context', v_dbg_context,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'contracts',
        ('contract:' || coalesce(p_contract_id::text,'')),
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
$$;
