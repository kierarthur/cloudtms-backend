-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 2975b2676cfd.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.tsfin_load_context_batch(p_timesheet_ids uuid[])
returns table (
  effective_timesheet_id uuid,
  out_timesheet jsonb,
  out_cur_fin jsonb,
  out_candidate jsonb,
  out_umbrella jsonb,
  out_client_id uuid,
  out_effective_flags jsonb,
  out_policy jsonb
)
language sql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $$
with input_ids as (
  select distinct unnest(p_timesheet_ids) as input_timesheet_id
  where p_timesheet_ids is not null
),
t_in as (
  select
    i.input_timesheet_id,
    t0.*
  from input_ids i
  join public.timesheets t0
    on t0.timesheet_id = i.input_timesheet_id
),
t_eff_id as (
  select
    t_in.input_timesheet_id,
    coalesce(tc.timesheet_id, t_in.timesheet_id) as effective_timesheet_id
  from t_in
  left join public.timesheets tc
    on tc.booking_id = t_in.booking_id
   and tc.is_current = true
),
t_eff as (
  select
    e.input_timesheet_id,
    e.effective_timesheet_id,
    te.*
  from t_eff_id e
  join public.timesheets te
    on te.timesheet_id = e.effective_timesheet_id
),
base as (
  select
    te.effective_timesheet_id,
    correction_policy.envelope_json as correction_policy_envelope,
    correction_policy.leg_json as correction_policy_leg,

    -- TIME anchor date for client_settings.effective_from selection:
    -- DAILY: worked_start_iso local date
    -- WEEKLY: week_ending_date (fallback)
    coalesce(
      case
        when te.worked_start_iso is not null
          then (te.worked_start_iso at time zone 'Europe/London')::date
        else null
      end,
      te.week_ending_date::date
    ) as time_anchor_date,

    -- FINANCE anchor date for finance windows:
    -- authorised_at_server local date if present, else "today" local date
    coalesce(
      case
        when nullif(correction_policy.leg_json #>> '{tsfin_policy,pay_policy_date}', '') is not null
          then (correction_policy.leg_json #>> '{tsfin_policy,pay_policy_date}')::date
        else null
      end,
      case
        when te.authorised_at_server is not null
          then (te.authorised_at_server at time zone 'Europe/London')::date
        else null
      end,
      (now() at time zone 'Europe/London')::date
    ) as finance_anchor_date,

    -- ✅ Ensure new correction fields are always present in out_timesheet (even if null)
    (to_jsonb(te)
      || jsonb_build_object(
        'correction_id', te.correction_id,
        'correction_kind', te.correction_kind,
        'adjustment_origin', te.adjustment_origin
      )
    ) as out_timesheet,

    -- ✅ This will now include travel_/accommodation_/other_ columns automatically
    to_jsonb(tf) as out_cur_fin,

    to_jsonb(c)  as out_candidate,
    to_jsonb(u)  as out_umbrella,

    cid.client_id as out_client_id,
    authority.settings_authority_json as settings_authority,

    -- ✅ Expand “effective flags” for weekly consumers (still source-of-truth = v_timesheets_summary)
    jsonb_build_object(
      'route_type',                    v.route_type,

      'contract_id',                   v.contract_id,
      'contract_week_id',              v.contract_week_id,
      'contract_week_ending_date',     v.contract_week_ending_date,
      'basis',                         v.basis,

      'client_requires_hr',            (authority.settings_authority_json#>>'{values,requires_hr}')::boolean,
      'client_autoprocess_hr',         (authority.settings_authority_json#>>'{values,autoprocess_hr}')::boolean,
      'client_no_timesheet_required',  (authority.settings_authority_json#>>'{values,no_timesheet_required}')::boolean,
      'client_is_nhsp',                (authority.settings_authority_json#>>'{values,is_nhsp}')::boolean,

      'require_reference_to_pay',      (authority.settings_authority_json#>>'{values,require_reference_to_pay}')::boolean,
      'require_reference_to_invoice',  (authority.settings_authority_json#>>'{values,require_reference_to_invoice}')::boolean,

      'client_hr_validation_required', (authority.settings_authority_json#>>'{values,hr_validation_required}')::boolean,
      'client_ts_reference_required',  (authority.settings_authority_json#>>'{values,ts_reference_required}')::boolean,
      'client_pay_reference_required', (authority.settings_authority_json#>>'{values,require_reference_to_pay}')::boolean,
      'client_invoice_reference_required', (authority.settings_authority_json#>>'{values,require_reference_to_invoice}')::boolean,

      'pay_method',                    v.pay_method,
      'processing_status',             v.processing_status,
      'authorised_at_server',          v.authorised_at_server,

      -- ✅ NEW: computed here (because v_timesheets_summary does not expose hr_validation_required_for_invoice)
      'hr_validation_required_for_invoice',
        (
          v.timesheet_id is not null
          and coalesce((authority.settings_authority_json#>>'{values,hr_validation_required}')::boolean, false) = true
          and coalesce((authority.settings_authority_json#>>'{values,no_timesheet_required}')::boolean, false) = false
          and coalesce(v.total_hours, tf.total_hours, 0::numeric) > 0::numeric
        ),

      -- ✅ NEW: pass through validation status for TSFIN recompute gating
      'validation_status', v.validation_status
    ) as out_effective_flags
  from t_eff te

  left join lateral (
    select
      public._ctms_correction_policy_envelope_read_v1(te.effective_timesheet_id) as envelope_json,
      public._ctms_correction_policy_leg_read_v1(te.effective_timesheet_id) as leg_json
  ) correction_policy on true

  left join public.timesheets_financials tf
    on tf.timesheet_id = te.effective_timesheet_id
   and tf.is_current = true

  left join lateral (
    with candidate_resolution as (
      select
        case
          when upper(coalesce(te.sheet_scope::text, '')) = 'DAILY'
           and upper(coalesce(te.submission_mode::text, '')) = 'MANUAL'
           and coalesce(te.candidate_hint_text->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (te.candidate_hint_text->>'candidate_id')::uuid
          else null::uuid
        end as manual_candidate_hint_id,
        case
          when upper(coalesce(te.sheet_scope::text, '')) = 'DAILY'
           and upper(coalesce(te.submission_mode::text, '')) = 'MANUAL'
            then tf.candidate_id
          else null::uuid
        end as manual_current_candidate_id
    ),
    candidate_matches as (
      select
        c1.id as candidate_id,
        0 as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      cross join candidate_resolution cr
      where cr.manual_candidate_hint_id is not null
        and c1.id = cr.manual_candidate_hint_id

      union all

      select
        c1.id as candidate_id,
        1 as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      cross join candidate_resolution cr
      where cr.manual_current_candidate_id is not null
        and c1.id = cr.manual_current_candidate_id
        and (
          cr.manual_candidate_hint_id is null
          or c1.id <> cr.manual_candidate_hint_id
        )

      union all

      select
        c1.id as candidate_id,
        case
          when c1.key_norm = te.occupant_key_norm then 2
          else 3
        end as match_priority,
        c1.updated_at as match_updated_at,
        c1.created_at as match_created_at
      from public.candidates c1
      where
        c1.key_norm = te.occupant_key_norm
        or (
          te.occupant_key_norm is not null
          and c1.nhsp_hr_name_aliases @> to_jsonb(array[te.occupant_key_norm]::text[])
        )
    )
    select candidate_matches.candidate_id
    from candidate_matches
    order by
      candidate_matches.match_priority,
      candidate_matches.match_updated_at desc nulls last,
      candidate_matches.match_created_at desc nulls last
    limit 1
  ) candidate_pick on true

  left join public.candidates c
    on c.id = candidate_pick.candidate_id

  left join public.umbrellas u
    on (c.umbrella_id is not null and u.id = c.umbrella_id)

  left join public.v_timesheets_summary v
    on v.timesheet_id = te.effective_timesheet_id

  left join lateral (
    select ch.client_id
    from public.client_hospitals ch
    where te.hospital_norm is not null
      and ch.hospital_name_norm @> jsonb_build_array(te.hospital_norm)
    limit 1
  ) ch on true

  -- ✅ Resolve a single client_id for this timesheet (works for WEEKLY and DAILY)
  left join lateral (
    select coalesce(v.client_id, tf.client_id, ch.client_id) as client_id
  ) cid on true

  left join lateral (
    select private._contract_settings_effective_core_v1(
      cid.client_id,
      te.contract_id,
      coalesce(
        (te.worked_start_iso at time zone 'Europe/London')::date,
        (te.scheduled_start_iso at time zone 'Europe/London')::date,
        te.week_ending_date::date
      ),
      case when te.sheet_scope='DAILY'::public.timesheet_scope_enum then 'DAILY' else 'FINANCE' end,
      te.effective_timesheet_id
    ) as settings_authority_json
  ) authority on true
),
ctx as (
  select
    b.effective_timesheet_id,

    b.out_timesheet,
    b.out_cur_fin,
    b.out_candidate,
    b.out_umbrella,
    b.out_client_id,
    b.out_effective_flags,

    -- Every policy input comes from the Timesheet authority snapshot.  The
    -- correction envelope keeps its existing explicit overrides.
    jsonb_build_object(
      'timezone_id', coalesce(b.settings_authority#>>'{values,timezone_id}','Europe/London'),
      'day_start',   coalesce(b.settings_authority#>>'{values,day_start}','06:00:00'),
      'day_end',     coalesce(b.settings_authority#>>'{values,day_end}','20:00:00'),
      'night_start', coalesce(b.settings_authority#>>'{values,night_start}','20:00:00'),
      'night_end',   coalesce(b.settings_authority#>>'{values,night_end}','06:00:00'),
      'sat_start',   coalesce(b.settings_authority#>>'{values,sat_start}','00:00:00'),
      'sat_end',     coalesce(b.settings_authority#>>'{values,sat_end}','00:00:00'),
      'sun_start',   coalesce(b.settings_authority#>>'{values,sun_start}','00:00:00'),
      'sun_end',     coalesce(b.settings_authority#>>'{values,sun_end}','00:00:00'),
      'bh_start',    coalesce(b.settings_authority#>>'{values,bh_start}','00:00:00'),
      'bh_end',      coalesce(b.settings_authority#>>'{values,bh_end}','00:00:00'),

      'vat_rate_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,applied_pay_vat_rate_pct}', '')::numeric,
        nullif(b.settings_authority#>>'{values,vat_rate_pct}','')::numeric,20::numeric),

      'pay_vat_rate_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,applied_pay_vat_rate_pct}', '')::numeric,
        nullif(b.settings_authority#>>'{values,vat_rate_pct}','')::numeric,20::numeric),

      'correction_financials_policy_envelope', b.correction_policy_envelope,
      'correction_financials_policy_envelope_fingerprint', b.correction_policy_leg ->> 'envelope_fingerprint',
      'correction_leg_fingerprint', b.correction_policy_leg ->> 'leg_fingerprint',
      'correction_tsfin_policy', b.correction_policy_leg -> 'tsfin_policy',
      'correction_tsfin_policy_fingerprint', b.correction_policy_leg #>> '{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy', b.correction_policy_leg -> 'invoice_policy',
      'correction_invoice_policy_fingerprint', b.correction_policy_leg #>> '{invoice_policy,invoice_policy_fingerprint}',
      'correction_invoice_stream', b.correction_policy_leg #>> '{invoice_policy,invoice_stream}',
      'correction_pay_policy_date', b.correction_policy_leg #>> '{tsfin_policy,pay_policy_date}',
      'correction_invoice_policy_date', b.correction_policy_leg #>> '{invoice_policy,invoice_policy_date}',

      'holiday_pay_pct',
      coalesce(nullif(b.settings_authority#>>'{values,holiday_pay_pct}','')::numeric,12.07::numeric),

      'erni_pct',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,erni_pct}', '')::numeric,
        nullif(b.settings_authority#>>'{values,erni_pct}','')::numeric,13.8::numeric),

      'mileage_pay_defaults',
      b.settings_authority#>'{values,mileage_pay_defaults}',

      'mileage_charge_defaults',
      b.settings_authority#>'{values,mileage_charge_defaults}',

      'apply_holiday_to',
      coalesce(b.settings_authority#>>'{values,apply_holiday_to}','PAYE_ONLY'),

      'apply_erni_to',
      coalesce(nullif(b.correction_policy_leg #>> '{tsfin_policy,apply_erni_to}', ''),
        b.settings_authority#>>'{values,apply_erni_to}','PAYE_ONLY'),

      'margin_includes',
      coalesce(b.settings_authority#>'{values,margin_includes}',
        jsonb_build_object('expenses',false,'mileage',false)),

      'bh_list',
      coalesce(b.settings_authority#>'{values,bh_list}','[]'::jsonb),

      'hr_attach_to_invoice', coalesce((b.settings_authority#>>'{values,hr_attach_to_invoice}')::boolean,true),
      'ts_attach_to_invoice', coalesce((b.settings_authority#>>'{values,ts_attach_to_invoice}')::boolean,true),

      'week_ending_weekday', coalesce((b.settings_authority#>>'{values,week_ending_weekday}')::integer,0),
      'default_submission_mode', coalesce(b.settings_authority#>>'{values,default_submission_mode}','ELECTRONIC'),
      'settings_authority_version',b.settings_authority->>'authority_version',
      'settings_authority_fingerprint',b.settings_authority->>'authority_fingerprint'
    ) as out_policy
  from base b
)
select
  effective_timesheet_id,
  out_timesheet,
  out_cur_fin,
  out_candidate,
  out_umbrella,
  out_client_id,
  out_effective_flags,
  out_policy
from ctx;
$$;

revoke all on function public.tsfin_load_context_batch(uuid[])
from public, anon, authenticated;

grant execute on function public.tsfin_load_context_batch(uuid[])
to service_role;
