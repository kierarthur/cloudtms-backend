-- TEST Banking Loans / Snoozes sortable, independently paged read contract.
--
-- This function deliberately wraps the existing authoritative register function.
-- It changes only the read/display boundary: filters and whole-dataset ordering are
-- applied before each independent page is returned. Finance economics, eligibility,
-- reservations, frozen batch artefacts and settlement behaviour remain untouched.

create or replace function public.pay_loans_snoozes_page_v2(
  p_candidate_id uuid default null::uuid,
  p_client_id uuid default null::uuid,
  p_hide_completed_non_current_items boolean default true,
  p_case_type text default null::text,
  p_view_mode text default null::text,
  p_snooze_mode text default null::text,
  p_finance_page integer default 1,
  p_finance_page_size integer default 10,
  p_finance_sort_key text default 'created_at'::text,
  p_finance_sort_dir text default 'desc'::text,
  p_timesheet_page integer default 1,
  p_timesheet_page_size integer default 10,
  p_timesheet_sort_key text default 'created_at'::text,
  p_timesheet_sort_dir text default 'desc'::text
)
returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_base jsonb := '{}'::jsonb;
  v_finance_filtered jsonb := '[]'::jsonb;
  v_timesheet_filtered jsonb := '[]'::jsonb;
  v_finance_page_rows jsonb := '[]'::jsonb;
  v_timesheet_page_rows jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
  v_case_type text := upper(nullif(btrim(coalesce(p_case_type, '')), ''));
  v_view_mode text := upper(nullif(btrim(coalesce(p_view_mode, '')), ''));
  v_snooze_mode text := upper(nullif(btrim(coalesce(p_snooze_mode, '')), ''));
  v_finance_sort_key text := lower(nullif(btrim(coalesce(p_finance_sort_key, '')), ''));
  v_finance_sort_dir text := lower(nullif(btrim(coalesce(p_finance_sort_dir, '')), ''));
  v_finance_page integer := greatest(coalesce(p_finance_page, 1), 1);
  v_finance_page_size integer := least(greatest(coalesce(p_finance_page_size, 10), 1), 50);
  v_timesheet_sort_key text := lower(nullif(btrim(coalesce(p_timesheet_sort_key, '')), ''));
  v_timesheet_sort_dir text := lower(nullif(btrim(coalesce(p_timesheet_sort_dir, '')), ''));
  v_timesheet_page integer := greatest(coalesce(p_timesheet_page, 1), 1);
  v_timesheet_page_size integer := least(greatest(coalesce(p_timesheet_page_size, 10), 1), 50);
  v_finance_total integer := 0;
  v_timesheet_total integer := 0;
  v_finance_total_pages integer := 1;
  v_timesheet_total_pages integer := 1;
begin
  if v_case_type not in (
    'PAYMENT_ADVANCE',
    'OVERPAYMENT',
    'UNDERPAYMENT',
    'MANUAL_DEBT_ADJUSTMENT',
    'MANUAL_CREDIT_ADJUSTMENT'
  ) then
    v_case_type := null;
  end if;

  if v_view_mode not in ('ACTIVE', 'SNOOZED', 'HISTORY') then
    v_view_mode := null;
  end if;

  if v_snooze_mode not in ('DATED', 'INDEFINITE') then
    v_snooze_mode := null;
  end if;

  if v_finance_sort_key not in ('created_at', 'candidate', 'case_type') then
    v_finance_sort_key := 'created_at';
  end if;

  if v_finance_sort_dir not in ('asc', 'desc') then
    v_finance_sort_dir := case when v_finance_sort_key = 'created_at' then 'desc' else 'asc' end;
  end if;

  if v_timesheet_sort_key not in ('created_at', 'candidate', 'timesheet') then
    v_timesheet_sort_key := 'created_at';
  end if;

  if v_timesheet_sort_dir not in ('asc', 'desc') then
    v_timesheet_sort_dir := case when v_timesheet_sort_key = 'created_at' then 'desc' else 'asc' end;
  end if;

  v_base := public.pay_loans_snoozes_list(
    p_candidate_id => p_candidate_id,
    p_client_id => p_client_id,
    p_hide_completed_non_current_items => coalesce(p_hide_completed_non_current_items, true)
  );

  with source_rows as (
    select
      case
        when vfcr.written_off_at_utc is not null then
          src.value || jsonb_build_object(
            'created_at', vfcr.created_at::text,
            'created_at_utc', vfcr.created_at::text,
            'status', 'WRITTEN_OFF',
            'finance_status', 'WRITTEN_OFF',
            'lifecycle_status_display', 'Written off',
            'finance_lifecycle_status_display', 'Written off'
          )
        else
          src.value || jsonb_build_object(
            'created_at', vfcr.created_at::text,
            'created_at_utc', vfcr.created_at::text
          )
      end as row_json
    from jsonb_array_elements(coalesce(v_base->'finance_cases', '[]'::jsonb)) src(value)
    join public.v_finance_cases_register vfcr
      on vfcr.finance_case_id = nullif(src.value->>'finance_case_id', '')::uuid
  ),
  classified_rows as (
    select
      sr.row_json,
      (
        nullif(sr.row_json#>>'{snooze,snooze_id}', '') is not null
        and upper(coalesce(sr.row_json#>>'{snooze,snooze_state}', '')) not in ('NOT_SNOOZED', 'CLEARED', 'CANCELLED')
      ) as has_active_snooze,
      (
        nullif(sr.row_json->>'written_off_at_utc', '') is not null
        or nullif(sr.row_json->>'cleared_at_utc', '') is not null
        or upper(coalesce(sr.row_json->>'status', '')) in (
          'PAID_OFF', 'PAID', 'CLEARED', 'WRITTEN_OFF', 'CANCELLED',
          'CLOSED', 'COMPLETED', 'COMPLETE', 'SETTLED', 'SETTLED_IN_FULL',
          'FINALISED', 'FINALIZED', 'FINISHED', 'ARCHIVED'
        )
      ) as is_history
    from source_rows sr
  )
  select coalesce(jsonb_agg(cr.row_json), '[]'::jsonb)
  into v_finance_filtered
  from classified_rows cr
  where (v_case_type is null or upper(coalesce(cr.row_json->>'case_type', '')) = v_case_type)
    and (
      v_view_mode is null
      or (v_view_mode = 'ACTIVE' and not cr.has_active_snooze and not cr.is_history)
      or (v_view_mode = 'SNOOZED' and cr.has_active_snooze)
      or (v_view_mode = 'HISTORY' and cr.is_history)
    )
    and (
      v_snooze_mode is null
      or (
        cr.has_active_snooze
        and v_snooze_mode = 'DATED'
        and nullif(cr.row_json#>>'{snooze,snooze_until_date}', '') is not null
      )
      or (
        cr.has_active_snooze
        and v_snooze_mode = 'INDEFINITE'
        and nullif(cr.row_json#>>'{snooze,snooze_until_date}', '') is null
      )
    );

  with source_rows as (
    select
      src.value as row_json,
      upper(coalesce(src.value->>'lifecycle_state', '')) in (
        'ACTIVE', 'SOURCE_REPLACED', 'SOURCE_CHANGED', 'SOURCE_UNAVAILABLE'
      ) as is_active
    from jsonb_array_elements(coalesce(v_base->'timesheet_snoozes', '[]'::jsonb)) src(value)
  )
  select coalesce(jsonb_agg(sr.row_json), '[]'::jsonb)
  into v_timesheet_filtered
  from source_rows sr
  where (
      v_view_mode is null
      or (v_view_mode = 'ACTIVE' and false)
      or (v_view_mode = 'SNOOZED' and sr.is_active)
      or (v_view_mode = 'HISTORY' and not sr.is_active)
    )
    and (
      v_snooze_mode is null
      or (v_snooze_mode = 'DATED' and nullif(sr.row_json->>'snooze_until_date', '') is not null)
      or (v_snooze_mode = 'INDEFINITE' and nullif(sr.row_json->>'snooze_until_date', '') is null)
    );

  v_finance_total := jsonb_array_length(v_finance_filtered);
  v_timesheet_total := jsonb_array_length(v_timesheet_filtered);
  v_finance_total_pages := greatest(1, ceil(v_finance_total::numeric / v_finance_page_size)::integer);
  v_timesheet_total_pages := greatest(1, ceil(v_timesheet_total::numeric / v_timesheet_page_size)::integer);
  v_finance_page := least(v_finance_page, v_finance_total_pages);
  v_timesheet_page := least(v_timesheet_page, v_timesheet_total_pages);

  with ordered_rows as (
    select
      src.value as row_json,
      row_number() over (
        order by
          case when v_finance_sort_key = 'created_at' and v_finance_sort_dir = 'asc'
            then nullif(src.value->>'created_at_utc', '')::timestamptz end asc nulls last,
          case when v_finance_sort_key = 'created_at' and v_finance_sort_dir = 'desc'
            then nullif(src.value->>'created_at_utc', '')::timestamptz end desc nulls last,
          case when v_finance_sort_key = 'candidate' and v_finance_sort_dir = 'asc'
            then lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) end asc nulls last,
          case when v_finance_sort_key = 'candidate' and v_finance_sort_dir = 'desc'
            then lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) end desc nulls last,
          case when v_finance_sort_key = 'case_type' and v_finance_sort_dir = 'asc'
            then lower(coalesce(src.value->>'admin_label', src.value->>'case_type', '')) end asc nulls last,
          case when v_finance_sort_key = 'case_type' and v_finance_sort_dir = 'desc'
            then lower(coalesce(src.value->>'admin_label', src.value->>'case_type', '')) end desc nulls last,
          lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) asc,
          coalesce(src.value->>'finance_case_id', '') asc
      ) as sort_position
    from jsonb_array_elements(v_finance_filtered) src(value)
  )
  select coalesce(jsonb_agg(orr.row_json order by orr.sort_position), '[]'::jsonb)
  into v_finance_page_rows
  from ordered_rows orr
  where orr.sort_position > ((v_finance_page - 1) * v_finance_page_size)
    and orr.sort_position <= (v_finance_page * v_finance_page_size);

  with ordered_rows as (
    select
      src.value as row_json,
      row_number() over (
        order by
          case when v_timesheet_sort_key = 'created_at' and v_timesheet_sort_dir = 'asc'
            then nullif(src.value->>'created_at_utc', '')::timestamptz end asc nulls last,
          case when v_timesheet_sort_key = 'created_at' and v_timesheet_sort_dir = 'desc'
            then nullif(src.value->>'created_at_utc', '')::timestamptz end desc nulls last,
          case when v_timesheet_sort_key = 'candidate' and v_timesheet_sort_dir = 'asc'
            then lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) end asc nulls last,
          case when v_timesheet_sort_key = 'candidate' and v_timesheet_sort_dir = 'desc'
            then lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) end desc nulls last,
          case when v_timesheet_sort_key = 'timesheet' and v_timesheet_sort_dir = 'asc'
            then lower(coalesce(nullif(src.value->>'reference_number', ''), nullif(src.value->>'current_timesheet_id', ''), nullif(src.value->>'timesheet_id', ''), src.value->>'original_timesheet_id', '')) end asc nulls last,
          case when v_timesheet_sort_key = 'timesheet' and v_timesheet_sort_dir = 'desc'
            then lower(coalesce(nullif(src.value->>'reference_number', ''), nullif(src.value->>'current_timesheet_id', ''), nullif(src.value->>'timesheet_id', ''), src.value->>'original_timesheet_id', '')) end desc nulls last,
          lower(coalesce(src.value->>'candidate_display_name', src.value->>'candidate_tms_ref', '')) asc,
          coalesce(src.value->>'snooze_id', '') asc
      ) as sort_position
    from jsonb_array_elements(v_timesheet_filtered) src(value)
  )
  select coalesce(jsonb_agg(orr.row_json order by orr.sort_position), '[]'::jsonb)
  into v_timesheet_page_rows
  from ordered_rows orr
  where orr.sort_position > ((v_timesheet_page - 1) * v_timesheet_page_size)
    and orr.sort_position <= (v_timesheet_page * v_timesheet_page_size);

  select jsonb_build_object(
    'payment_advances_active_count', count(*) filter (where upper(coalesce(src.value->>'case_type', '')) = 'PAYMENT_ADVANCE'),
    'overpayments_active_count', count(*) filter (where upper(coalesce(src.value->>'case_type', '')) = 'OVERPAYMENT'),
    'underpayments_active_count', count(*) filter (where upper(coalesce(src.value->>'case_type', '')) = 'UNDERPAYMENT'),
    'manual_debt_adjustments_active_count', count(*) filter (where upper(coalesce(src.value->>'case_type', '')) = 'MANUAL_DEBT_ADJUSTMENT'),
    'manual_credit_adjustments_count', count(*) filter (where upper(coalesce(src.value->>'case_type', '')) = 'MANUAL_CREDIT_ADJUSTMENT'),
    'mixed_finance_cases_count', count(*) filter (where coalesce((src.value->>'is_mixed_case')::boolean, false)),
    'unresolved_finance_cases_count', count(*) filter (where coalesce(nullif(src.value->>'unresolved_taxable_count', '')::integer, 0) > 0),
    'stale_finance_cases_count', count(*) filter (where coalesce(nullif(src.value->>'stale_count', '')::integer, 0) > 0),
    'finance_cases_with_active_snooze_count', count(*) filter (where nullif(src.value#>>'{snooze,snooze_id}', '') is not null),
    'timesheet_snoozes_count', v_timesheet_total,
    'timesheet_expense_snoozes_count', (
      select count(*)
      from jsonb_array_elements(v_timesheet_filtered) ts(value)
      where upper(coalesce(ts.value->>'row_kind', '')) = 'TIMESHEET_EXPENSE'
    )
  )
  into v_summary
  from jsonb_array_elements(v_finance_filtered) src(value);

  return jsonb_build_object(
    'ok', true,
    'filters', jsonb_build_object(
      'candidate_id', case when p_candidate_id is null then null else p_candidate_id::text end,
      'client_id', case when p_client_id is null then null else p_client_id::text end,
      'case_type', v_case_type,
      'view_mode', v_view_mode,
      'snooze_mode', v_snooze_mode,
      'hide_completed_non_current_items', coalesce(p_hide_completed_non_current_items, true)
    ),
    'summary', v_summary,
    'finance_cases', v_finance_page_rows,
    'timesheet_snoozes', v_timesheet_page_rows,
    'pagination', jsonb_build_object(
      'finance_cases', jsonb_build_object(
        'page', v_finance_page,
        'page_size', v_finance_page_size,
        'total_count', v_finance_total,
        'total_pages', v_finance_total_pages,
        'sort_key', v_finance_sort_key,
        'sort_dir', v_finance_sort_dir
      ),
      'timesheet_snoozes', jsonb_build_object(
        'page', v_timesheet_page,
        'page_size', v_timesheet_page_size,
        'total_count', v_timesheet_total,
        'total_pages', v_timesheet_total_pages,
        'sort_key', v_timesheet_sort_key,
        'sort_dir', v_timesheet_sort_dir
      )
    )
  );
end;
$function$;

revoke execute on function public.pay_loans_snoozes_page_v2(
  uuid, uuid, boolean, text, text, text, integer, integer, text, text, integer, integer, text, text
) from public, anon, authenticated;

grant execute on function public.pay_loans_snoozes_page_v2(
  uuid, uuid, boolean, text, text, text, integer, integer, text, text, integer, integer, text, text
) to service_role;

