-- Manual regression checks for pay_preview case_resolutions scoping/performance.
-- Usage:
--   Edit the `params` CTE at the start of each check block below.
--   `linked_case_resolutions` should be a JSON object keyed by case_key.
--   `known_bad_payload` should be the full jsonb payload for timeout repro.

-- 1) Empty case_resolutions path unchanged.
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client,
    '{}'::jsonb as linked_case_resolutions,
    null::jsonb as known_bad_payload
),
baseline as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, null::jsonb) as j
  from params
), explicit_empty as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, '{"case_resolutions":{}}'::jsonb) as j
  from params
)
select
  ((baseline.j->'canonical_preview_lines') = (explicit_empty.j->'canonical_preview_lines')) as canonical_preview_lines_equal,
  ((baseline.j->'summary') = (explicit_empty.j->'summary')) as summary_equal,
  ((baseline.j->'case_resolution_states') = (explicit_empty.j->'case_resolution_states')) as case_resolution_states_equal
from baseline, explicit_empty;

-- 2) Resolve one/two/all buckets from a real unresolved BUCKETED timesheet case.
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client
),
preview0 as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, null::jsonb) as j
  from params
),
unresolved_case as (
  select cs.case_json
  from preview0 p
  cross join lateral jsonb_array_elements(coalesce(p.j->'case_resolution_states', '[]'::jsonb)) as cs(case_json)
  where coalesce(cs.case_json->>'resolution_family','') = 'BUCKETED'
    and coalesce((cs.case_json->>'case_needs_resolution')::boolean, false) = true
    and jsonb_array_length(coalesce(cs.case_json->'case_components', '[]'::jsonb)) >= 3
  order by cs.case_json->>'case_key'
  limit 1
),
components as (
  select
    uc.case_json->>'case_key' as case_key,
    comp.comp_json,
    row_number() over (order by comp.comp_json->>'component_key_type', comp.comp_json->>'component_key_value') as rn,
    count(*) over () as component_count
  from unresolved_case uc
  cross join lateral jsonb_array_elements(uc.case_json->'case_components') as comp(comp_json)
  where coalesce((comp.comp_json->>'is_rate_bearing')::boolean, false) = true
),
mk_payload as (
  select
    c.case_key,
    jsonb_build_object(
      'case_resolutions',
      jsonb_build_object(
        c.case_key,
        jsonb_build_object(
          'case_key', c.case_key,
          'resolution_family', 'BUCKETED',
          'resolve_all_linked_timesheets', false,
          'bucket_resolutions', jsonb_agg(
            jsonb_strip_nulls(jsonb_build_object(
              'component_key_type', comp_json->>'component_key_type',
              'component_key_value', comp_json->>'component_key_value',
              'source_basis_fingerprint', comp_json->>'source_basis_fingerprint',
              'source_rate', (comp_json->>'source_rate')::numeric,
              'source_charge_rate', (comp_json->>'source_charge_rate')::numeric,
              'target_rate', round(((comp_json->>'source_rate')::numeric + 0.01), 2),
              'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS'
            ))
            order by rn
          ) filter (where rn <= 1)
        )
      )
    ) as one_bucket_payload,
    jsonb_build_object(
      'case_resolutions',
      jsonb_build_object(
        c.case_key,
        jsonb_build_object(
          'case_key', c.case_key,
          'resolution_family', 'BUCKETED',
          'resolve_all_linked_timesheets', false,
          'bucket_resolutions', jsonb_agg(
            jsonb_strip_nulls(jsonb_build_object(
              'component_key_type', comp_json->>'component_key_type',
              'component_key_value', comp_json->>'component_key_value',
              'source_basis_fingerprint', comp_json->>'source_basis_fingerprint',
              'source_rate', (comp_json->>'source_rate')::numeric,
              'source_charge_rate', (comp_json->>'source_charge_rate')::numeric,
              'target_rate', round(((comp_json->>'source_rate')::numeric + 0.01), 2),
              'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS'
            ))
            order by rn
          ) filter (where rn <= 2)
        )
      )
    ) as two_bucket_payload,
    jsonb_build_object(
      'case_resolutions',
      jsonb_build_object(
        c.case_key,
        jsonb_build_object(
          'case_key', c.case_key,
          'resolution_family', 'BUCKETED',
          'resolve_all_linked_timesheets', false,
          'bucket_resolutions', jsonb_agg(
            jsonb_strip_nulls(jsonb_build_object(
              'component_key_type', comp_json->>'component_key_type',
              'component_key_value', comp_json->>'component_key_value',
              'source_basis_fingerprint', comp_json->>'source_basis_fingerprint',
              'source_rate', (comp_json->>'source_rate')::numeric,
              'source_charge_rate', (comp_json->>'source_charge_rate')::numeric,
              'target_rate', round(((comp_json->>'source_rate')::numeric + 0.01), 2),
              'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS'
            ))
            order by rn
          )
        )
      )
    ) as all_bucket_payload
  from components c
  group by c.case_key
),
one_case as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, mp.one_bucket_payload) as j
  from mk_payload mp
  cross join params
),
two_case as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, mp.two_bucket_payload) as j
  from mk_payload mp
  cross join params
),
all_case as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, mp.all_bucket_payload) as j
  from mk_payload mp
  cross join params
),
counts as (
  select
    (select count(*) from components where rn <= 1) as expected_one,
    (select count(*) from components where rn <= 2) as expected_two,
    (select count(*) from components) as expected_all
),
resolved_counts as (
  select
    (select count(*)
     from one_case oc
     cross join lateral jsonb_array_elements(oc.j->'case_resolution_states') cs(case_json)
     cross join lateral jsonb_array_elements(coalesce(cs.case_json->'case_components','[]'::jsonb)) comp(comp_json)
     where comp.comp_json->>'resolution_state' = 'RESOLVED') as actual_one,
    (select count(*)
     from two_case tc
     cross join lateral jsonb_array_elements(tc.j->'case_resolution_states') cs(case_json)
     cross join lateral jsonb_array_elements(coalesce(cs.case_json->'case_components','[]'::jsonb)) comp(comp_json)
     where comp.comp_json->>'resolution_state' = 'RESOLVED') as actual_two,
    (select count(*)
     from all_case ac
     cross join lateral jsonb_array_elements(ac.j->'case_resolution_states') cs(case_json)
     cross join lateral jsonb_array_elements(coalesce(cs.case_json->'case_components','[]'::jsonb)) comp(comp_json)
     where comp.comp_json->>'resolution_state' = 'RESOLVED') as actual_all,
    (select bool_or(coalesce((cs.case_json->>'case_resolution_satisfied_now')::boolean, false))
     from all_case ac
     cross join lateral jsonb_array_elements(ac.j->'case_resolution_states') cs(case_json)) as all_case_marked_satisfied
)
select * from counts, resolved_counts;

-- 3) resolve_all_linked_timesheets=false remains local.
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client
),
preview0 as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, null::jsonb) as j
  from params
),
seed as (
  select cs.case_json
  from preview0 p
  cross join lateral jsonb_array_elements(coalesce(p.j->'case_resolution_states','[]'::jsonb)) cs(case_json)
  where coalesce((cs.case_json #>> '{linked_resolution_scope_json,linked_timesheet_count}')::int, 1) > 1
  limit 1
),
local_payload as (
  select jsonb_build_object(
    'case_resolutions',
    jsonb_build_object(
      seed.case_json->>'case_key',
      jsonb_build_object(
        'case_key', seed.case_json->>'case_key',
        'resolution_family', 'BUCKETED',
        'resolve_all_linked_timesheets', false,
        'bucket_resolutions', (
          select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
            'component_key_type', c->>'component_key_type',
            'component_key_value', c->>'component_key_value',
            'source_basis_fingerprint', c->>'source_basis_fingerprint',
            'source_rate', (c->>'source_rate')::numeric,
            'source_charge_rate', (c->>'source_charge_rate')::numeric,
            'target_rate', round(((c->>'source_rate')::numeric + 0.01), 2),
            'resolution_mode', 'SUGGESTED_EQUIVALENT_BASIS'
          )))
          from jsonb_array_elements(seed.case_json->'case_components') c
          where coalesce((c->>'is_rate_bearing')::boolean, false) = true
          limit 1
        )
      )
    )
  ) as payload
  from seed
),
preview_local as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, lp.payload) as j
  from local_payload lp
  cross join params
)
select
  cs.case_json->>'case_key' as case_key,
  cs.case_json #>> '{linked_resolution_scope_json,linked_timesheet_count}' as linked_timesheet_count,
  sum(case when comp.comp_json->>'resolution_state' = 'RESOLVED' then 1 else 0 end) as resolved_components
from preview_local pl
cross join lateral jsonb_array_elements(coalesce(pl.j->'case_resolution_states','[]'::jsonb)) cs(case_json)
cross join lateral jsonb_array_elements(coalesce(cs.case_json->'case_components','[]'::jsonb)) comp(comp_json)
group by 1,2;

-- 4) Linked-scope true applies to linked timesheets and does not duplicate effective rows.
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client,
    '{}'::jsonb as linked_case_resolutions
),
preview_linked as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client,
    jsonb_build_object('case_resolutions', params.linked_case_resolutions)
  ) as j
  from params
),
line_dupes as (
  select
    line_json->>'line_id' as line_id,
    count(*) as line_count
  from preview_linked pl
  cross join lateral jsonb_array_elements(coalesce(pl.j->'canonical_preview_lines','[]'::jsonb)) line(line_json)
  group by 1
  having count(*) > 1
)
select coalesce(count(*),0) as duplicate_line_id_count from line_dupes;

-- 5) Amount drift guard (summary totals should equal sum of canonical lines).
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client,
    '{}'::jsonb as linked_case_resolutions
),
p as (
  select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client,
    jsonb_build_object('case_resolutions', params.linked_case_resolutions)
  ) as j
  from params
),
line_totals as (
  select
    round(sum(coalesce((line_json->>'amount_ex_vat')::numeric,0)),2) as line_amount_ex
  from p
  cross join lateral jsonb_array_elements(coalesce(p.j->'canonical_preview_lines','[]'::jsonb)) line(line_json)
)
select
  line_totals.line_amount_ex,
  round(coalesce((p.j->'summary'->>'total_payment_amount_ex_vat')::numeric,0),2) as summary_amount_ex,
  (line_totals.line_amount_ex = round(coalesce((p.j->'summary'->>'total_payment_amount_ex_vat')::numeric,0),2)) as no_amount_drift
from p, line_totals;

-- 6) Timeout regression check for known problematic payload.
-- Replace known_bad_payload in params CTE with the exact JSON payload previously timing out.
set local statement_timeout = '15s';
with params as (
  select
    date '2026-03-27' as pay_date,
    date '2026-03-29' as week_ending,
    '00000000-0000-0000-0000-000000000000'::uuid as actor,
    null::uuid as candidate,
    null::uuid as client,
    null::jsonb as known_bad_payload
)
select public.pay_preview(params.pay_date, params.week_ending, params.actor, params.candidate, params.client, params.known_bad_payload)
from params;
