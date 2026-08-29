-- Repeatable CloudTMS function/view authority: candidate_calendar_authorised_statuses
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Candidate Bookings calendar status refresh. The return contract is
-- deliberately unchanged; only the state projection is modernised.
--
-- User-facing states:
--   PLANNED, NEEDS_ATTENTION, AWAITING_AUTHORISATION, AUTHORISED,
--   INVOICED, PAID, ON_HOLD, EMPTY.
--
-- Policy X boundary: this is a read-only, pre-draft calendar projection.
-- It reads canonical summary payment display state but does not create or
-- alter drafts, frozen artefacts, payment identity, settlement or economics.
create or replace function public.calendar_candidate_day_feed(
  candidate_id uuid,
  from_date date,
  to_date date
)
returns table (
  "date" date,
  contract_id uuid,
  state text,
  planned boolean,
  has_tsfin boolean,
  any_ready boolean,
  pay_on_hold boolean,
  invoice_on_hold boolean,
  invoiced boolean,
  paid boolean,
  pay_line_on_hold boolean,
  invoice_line_on_hold boolean,
  invoice_nos text
)
language sql
stable
as $function$
with candidate_contracts as (
  select contract_row.id as contract_id
  from public.contracts as contract_row
  where contract_row.candidate_id is not distinct from $1
    and contract_row.start_date <= $3
    and coalesce(contract_row.end_date, $3) >= $2
),
planned_days as (
  select
    contract_week.contract_id,
    (planned_row.value->>'date')::date as date
  from public.contract_weeks as contract_week
  join candidate_contracts
    on candidate_contracts.contract_id = contract_week.contract_id
  cross join lateral jsonb_array_elements(
    coalesce(contract_week.planned_schedule_json, '[]'::jsonb)
  ) as planned_row(value)
  where (planned_row.value->>'date')::date between $2 and $3
),
timesheet_rows as (
  select
    timesheet.timesheet_id,
    timesheet.contract_id,
    contract_week.id as contract_week_id,
    contract_week.planned_schedule_json as contract_week_planned_schedule_json,
    timesheet.week_ending_date,
    timesheet.sheet_scope,
    timesheet.worked_start_iso,
    timesheet.actual_schedule_json,
    financial.id as financial_id,
    financial.processing_status::text as processing_status,
    coalesce(financial.pay_on_hold, false) as pay_on_hold,
    financial.paid_at_utc,
    financial.locked_by_invoice_id,
    financial.invoice_breakdown_json,
    coalesce(
      case
        when coalesce(summary_pay_cache.summary_state_applies, false)
          then summary_pay_cache.summary_pay_status_code
        else null
      end,
      pay_state.summary_pay_status_code,
      case
        when pay_state.last_settled_at_utc is not null
          or financial.paid_at_utc is not null
          then 'PAID'
        else 'UNPAID'
      end
    )::text as pay_status_code
  from public.timesheets as timesheet
  join candidate_contracts
    on candidate_contracts.contract_id = timesheet.contract_id
  left join public.contract_weeks as contract_week
    on contract_week.timesheet_id = timesheet.timesheet_id
  left join public.timesheets_financials as financial
    on financial.timesheet_id = timesheet.timesheet_id
   and financial.is_current = true
  left join public.timesheet_summary_pay_state_cache as summary_pay_cache
    on summary_pay_cache.timesheet_id = timesheet.timesheet_id
  left join public.timesheet_pay_state as pay_state
    on pay_state.timesheet_id = timesheet.timesheet_id
  where timesheet.is_current = true
    and timesheet.archived_at_utc is null
    and timesheet.revoked_at is null
    and (
      (
        timesheet.sheet_scope = 'WEEKLY'
        and timesheet.week_ending_date between ($2 - 14) and ($3 + 14)
      )
      or
      (
        timesheet.sheet_scope = 'DAILY'
        and timesheet.worked_start_iso is not null
        and (
          timesheet.worked_start_iso at time zone 'Europe/London'
        )::date between $2 and $3
      )
    )
),
segment_events as (
  select
    source.contract_id,
    (segment_row.value->>'date')::date as date,
    source.timesheet_id,
    source.financial_id,
    source.processing_status,
    source.pay_on_hold,
    source.pay_status_code,
    coalesce(
      nullif(segment_row.value->>'invoice_locked_invoice_id', '')::uuid,
      source.locked_by_invoice_id
    ) as invoice_id,
    coalesce(
      (segment_row.value->>'exclude_from_pay')::boolean,
      false
    ) as pay_line_on_hold,
    (
      nullif(segment_row.value->>'invoice_locked_invoice_id', '') is null
      and nullif(segment_row.value->>'invoice_target_week_start', '') is not null
      and (
        nullif(segment_row.value->>'invoice_target_week_start', '')::date
        <>
        case
          when source.week_ending_date is not null
            then source.week_ending_date - 6
          else date_trunc(
            'week',
            ((segment_row.value->>'date')::date)::timestamp
          )::date
        end
      )
    ) as invoice_line_on_hold
  from timesheet_rows as source
  cross join lateral jsonb_array_elements(
    coalesce(source.invoice_breakdown_json->'segments', '[]'::jsonb)
  ) as segment_row(value)
  where source.financial_id is not null
    and coalesce(source.invoice_breakdown_json->>'mode', '') = 'SEGMENTS'
    and (segment_row.value->>'date')::date between $2 and $3
),
weekly_schedule_events as (
  select
    source.contract_id,
    (schedule_row.value->>'date')::date as date,
    source.timesheet_id,
    source.financial_id,
    source.processing_status,
    source.pay_on_hold,
    source.pay_status_code,
    source.locked_by_invoice_id as invoice_id,
    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from timesheet_rows as source
  cross join lateral jsonb_array_elements(
    coalesce(source.actual_schedule_json, '[]'::jsonb)
  ) as schedule_row(value)
  where source.sheet_scope = 'WEEKLY'
    and coalesce(source.invoice_breakdown_json->>'mode', '') <> 'SEGMENTS'
    and (schedule_row.value->>'date')::date between $2 and $3
),
weekly_plan_fallback_events as (
  select
    source.contract_id,
    (planned_row.value->>'date')::date as date,
    source.timesheet_id,
    source.financial_id,
    source.processing_status,
    source.pay_on_hold,
    source.pay_status_code,
    source.locked_by_invoice_id as invoice_id,
    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from timesheet_rows as source
  cross join lateral jsonb_array_elements(
    coalesce(source.contract_week_planned_schedule_json, '[]'::jsonb)
  ) as planned_row(value)
  where source.sheet_scope = 'WEEKLY'
    and source.financial_id is not null
    and coalesce(source.invoice_breakdown_json->>'mode', '') <> 'SEGMENTS'
    and coalesce(
      jsonb_array_length(coalesce(source.actual_schedule_json, '[]'::jsonb)),
      0
    ) = 0
    and (planned_row.value->>'date')::date between $2 and $3
),
daily_events as (
  select
    source.contract_id,
    (source.worked_start_iso at time zone 'Europe/London')::date as date,
    source.timesheet_id,
    source.financial_id,
    source.processing_status,
    source.pay_on_hold,
    source.pay_status_code,
    source.locked_by_invoice_id as invoice_id,
    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from timesheet_rows as source
  where source.sheet_scope = 'DAILY'
    and source.worked_start_iso is not null
    and (
      source.worked_start_iso at time zone 'Europe/London'
    )::date between $2 and $3
),
all_events as (
  select
    planned_day.contract_id,
    planned_day.date,
    true as planned,
    null::uuid as timesheet_id,
    null::uuid as financial_id,
    null::text as processing_status,
    false as pay_on_hold,
    'UNPAID'::text as pay_status_code,
    null::uuid as invoice_id,
    false as pay_line_on_hold,
    false as invoice_line_on_hold
  from planned_days as planned_day
  union all
  select
    event.contract_id, event.date, false, event.timesheet_id,
    event.financial_id, event.processing_status, event.pay_on_hold,
    event.pay_status_code, event.invoice_id, event.pay_line_on_hold,
    event.invoice_line_on_hold
  from segment_events as event
  union all
  select
    event.contract_id, event.date, false, event.timesheet_id,
    event.financial_id, event.processing_status, event.pay_on_hold,
    event.pay_status_code, event.invoice_id, event.pay_line_on_hold,
    event.invoice_line_on_hold
  from weekly_schedule_events as event
  union all
  select
    event.contract_id, event.date, false, event.timesheet_id,
    event.financial_id, event.processing_status, event.pay_on_hold,
    event.pay_status_code, event.invoice_id, event.pay_line_on_hold,
    event.invoice_line_on_hold
  from weekly_plan_fallback_events as event
  union all
  select
    event.contract_id, event.date, false, event.timesheet_id,
    event.financial_id, event.processing_status, event.pay_on_hold,
    event.pay_status_code, event.invoice_id, event.pay_line_on_hold,
    event.invoice_line_on_hold
  from daily_events as event
),
events_with_invoice as (
  select
    event.*,
    invoice.status::text as invoice_status,
    invoice.invoice_no
  from all_events as event
  left join public.invoices as invoice
    on invoice.id = event.invoice_id
),
day_states as (
  select
    event.date,
    event.contract_id,
    bool_or(event.planned) as planned,
    bool_or(event.financial_id is not null) as has_tsfin,
    bool_or(event.processing_status = 'READY_FOR_INVOICE') as any_ready,
    bool_or(event.pay_on_hold) as pay_on_hold,
    bool_or(event.invoice_status = 'ON_HOLD') as invoice_on_hold,
    bool_or(
      event.invoice_id is not null
      and coalesce(event.invoice_status, '') <> 'ON_HOLD'
    ) as invoiced,
    bool_or(event.pay_status_code in ('PAID', 'OVERPAID')) as paid,
    bool_or(event.pay_line_on_hold) as pay_line_on_hold,
    bool_or(event.invoice_line_on_hold) as invoice_line_on_hold,
    bool_or(
      event.timesheet_id is not null
      and event.pay_on_hold is not true
      and event.invoice_status is distinct from 'ON_HOLD'
      and event.pay_line_on_hold is not true
      and event.invoice_line_on_hold is not true
      and event.pay_status_code not in ('PAID', 'OVERPAID')
      and event.invoice_id is null
      and event.processing_status = 'PENDING_AUTH'
    ) as awaiting_authorisation,
    bool_or(
      event.timesheet_id is not null
      and event.pay_on_hold is not true
      and event.invoice_status is distinct from 'ON_HOLD'
      and event.pay_line_on_hold is not true
      and event.invoice_line_on_hold is not true
      and event.pay_status_code not in ('PAID', 'OVERPAID')
      and event.invoice_id is null
      and (
        event.financial_id is null
        or event.processing_status is null
        or event.processing_status not in (
          'PENDING_AUTH',
          'READY_FOR_INVOICE'
        )
      )
    ) as needs_attention,
    string_agg(distinct event.invoice_no, ',')
      filter (where event.invoice_no is not null) as invoice_nos
  from events_with_invoice as event
  group by event.date, event.contract_id
)
select
  day_state.date,
  day_state.contract_id,
  case
    when (
      day_state.pay_on_hold
      or day_state.invoice_on_hold
      or day_state.pay_line_on_hold
      or day_state.invoice_line_on_hold
    ) then 'ON_HOLD'
    when day_state.needs_attention then 'NEEDS_ATTENTION'
    when day_state.awaiting_authorisation then 'AWAITING_AUTHORISATION'
    when day_state.paid then 'PAID'
    when day_state.invoiced then 'INVOICED'
    when day_state.any_ready then 'AUTHORISED'
    when day_state.planned then 'PLANNED'
    else 'EMPTY'
  end as state,
  day_state.planned,
  day_state.has_tsfin,
  day_state.any_ready,
  day_state.pay_on_hold,
  day_state.invoice_on_hold,
  day_state.invoiced,
  day_state.paid,
  day_state.pay_line_on_hold,
  day_state.invoice_line_on_hold,
  day_state.invoice_nos
from day_states as day_state
where day_state.date between $2 and $3
order by day_state.date, day_state.contract_id;
$function$;

commit;
