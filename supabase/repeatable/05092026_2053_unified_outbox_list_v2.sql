-- Repeatable CloudTMS function/view authority: unified_outbox_list_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function public.outbox_unified_list_v2(
  p_status text default null,
  p_channel text default null,
  p_search text default null,
  p_queue_state text default null,
  p_operation_type text default null,
  p_entity_id uuid default null,
  p_requires_user_action boolean default null,
  p_limit integer default 50,
  p_offset integer default 0,
  p_sort_by text default 'created_at_utc',
  p_sort_dir text default 'desc',
  p_snapshot_at_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, pg_temp
as $function$
declare
  v_status text := nullif(upper(btrim(coalesce(p_status, ''))), '');
  v_channel text := nullif(upper(btrim(coalesce(p_channel, ''))), '');
  v_search text := nullif(btrim(coalesce(p_search, '')), '');
  v_queue_state text := nullif(upper(btrim(coalesce(p_queue_state, ''))), '');
  v_operation_type text := nullif(upper(btrim(coalesce(p_operation_type, ''))), '');
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 500);
  v_offset integer := greatest(coalesce(p_offset, 0), 0);
  v_sort_by text := lower(btrim(coalesce(p_sort_by, 'created_at_utc')));
  v_sort_dir text := lower(btrim(coalesce(p_sort_dir, 'desc')));
  v_snapshot_at_utc timestamptz := coalesce(p_snapshot_at_utc, now());
  v_total_count bigint := 0;
  v_legacy_total bigint := 0;
  v_invoice_total bigint := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if v_sort_by not in (
    'created_at_utc',
    'scheduled_for_utc',
    'effective_ready_at_utc',
    'status',
    'channel'
  ) then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_SORT';
  end if;

  if v_sort_dir not in ('asc', 'desc') then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_SORT';
  end if;

  if v_queue_state is not null and v_queue_state not in (
    'SCHEDULED',
    'QUEUED',
    'READY',
    'RUNNING',
    'ACTION_REQUIRED',
    'SENT',
    'DELIVERED',
    'READ',
    'FAILED'
  ) then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_QUEUE_STATE';
  end if;

  if v_status is not null and v_status !~ '^[A-Z_]+$' then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_STATUS';
  end if;

  if v_channel is not null and v_channel !~ '^[A-Z_]+$' then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_CHANNEL';
  end if;

  if v_operation_type is not null and v_operation_type !~ '^[A-Z_]+$' then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_OPERATION_TYPE';
  end if;

  if v_search is not null and (
    char_length(v_search) > 80
    or v_search !~ '^[a-zA-Z0-9 _-]+$'
  ) then
    raise exception using errcode = '22023', message = 'INVALID_OUTBOX_SEARCH';
  end if;

  with legacy_source as (
    select
      'LEGACY'::text as source_kind,
      upper(coalesce(u.channel, ''))::text as channel,
      u.outbox_id,
      u.outbox_type,
      u.status::text,
      u.delivery_status::text,
      u.created_at_utc,
      u.sent_at,
      u.delivered_at,
      u.read_at,
      u.failed_at,
      u.to_address::text,
      u.subject::text,
      u.body_text::text,
      u.reference::text,
      u.provider_message_id::text,
      u.last_error::text,
      u.created_by,
      u.recipient_kind::text,
      u.recipient_id,
      u.recipient_display_name::text,
      u.context_kind::text,
      u.context_id,
      u.mailshot_run_id,
      u.document_template_id,
      u.scheduled_for_utc,
      u.next_attempt_at_utc,
      coalesce(
        u.next_attempt_at_utc,
        u.scheduled_for_utc,
        u.created_at_utc
      ) as effective_ready_at_utc,
      case
        when u.read_at is not null then 'READ'
        when u.delivered_at is not null then 'DELIVERED'
        when u.sent_at is not null then 'SENT'
        when upper(coalesce(u.status, '')) = 'FAILED' or u.failed_at is not null then 'FAILED'
        when upper(coalesce(u.status, '')) = 'QUEUED'
          and coalesce(
            u.next_attempt_at_utc,
            u.scheduled_for_utc,
            u.created_at_utc
          ) > v_snapshot_at_utc then 'SCHEDULED'
        when upper(coalesce(u.status, '')) = 'QUEUED' then 'QUEUED'
        else upper(coalesce(u.status, ''))
      end::text as queue_state,
      (u.scheduled_for_utc is not null)::boolean as is_scheduled,
      false::boolean as requires_user_action,
      null::text as phase,
      null::text as entity_type,
      null::uuid as entity_id,
      null::bigint as change_seq,
      null::integer as completed_units,
      null::integer as total_units,
      null::integer as failed_units,
      null::jsonb as progress_json,
      null::jsonb as result_json,
      null::jsonb as error_json
    from public.v_outbox_unified as u
    where u.created_at_utc <= v_snapshot_at_utc
  ),
  invoice_values as (
    select
      o.*,
      nullif(
        btrim(coalesce(
          o.result_json #>> '{attempt_summary,run_after_utc}',
          o.progress_json ->> 'run_after_utc',
          ''
        )),
        ''
      ) as run_after_text
    from public.invoice_operations as o
    where o.created_at_utc <= v_snapshot_at_utc
      and o.operation_type <> 'OPERATION_CONTROL_REQUEST'
  ),
  invoice_source as (
    select
      'INVOICE'::text as source_kind,
      'INVOICE'::text as channel,
      i.id as outbox_id,
      i.operation_type::text as outbox_type,
      i.status::text,
      null::text as delivery_status,
      i.created_at_utc,
      null::timestamptz as sent_at,
      null::timestamptz as delivered_at,
      null::timestamptz as read_at,
      i.failed_at_utc as failed_at,
      null::text as to_address,
      null::text as subject,
      null::text as body_text,
      null::text as reference,
      null::text as provider_message_id,
      nullif(
        left(coalesce(i.error_json ->> 'code', i.error_json ->> 'error_code', ''), 120),
        ''
      )::text as last_error,
      i.actor_user_id as created_by,
      null::text as recipient_kind,
      null::uuid as recipient_id,
      null::text as recipient_display_name,
      i.entity_type::text as context_kind,
      i.entity_id as context_id,
      null::uuid as mailshot_run_id,
      null::uuid as document_template_id,
      case
        when i.run_after_text is not null
          and pg_input_is_valid(i.run_after_text, 'timestamptz')
          then i.run_after_text::timestamptz
        else null::timestamptz
      end as scheduled_for_utc,
      case
        when i.run_after_text is not null
          and pg_input_is_valid(i.run_after_text, 'timestamptz')
          then i.run_after_text::timestamptz
        else null::timestamptz
      end as next_attempt_at_utc,
      coalesce(
        case
          when i.run_after_text is not null
            and pg_input_is_valid(i.run_after_text, 'timestamptz')
            then i.run_after_text::timestamptz
          else null::timestamptz
        end,
        i.created_at_utc
      ) as effective_ready_at_utc,
      upper(coalesce(i.status, ''))::text as queue_state,
      (i.run_after_text is not null)::boolean as is_scheduled,
      i.requires_user_action,
      i.phase::text,
      i.entity_type::text,
      i.entity_id,
      i.change_seq,
      i.completed_units,
      i.total_units,
      i.failed_units,
      i.progress_json,
      i.result_json,
      i.error_json
    from invoice_values as i
  ),
  base_rows as (
    select * from legacy_source
    union all
    select * from invoice_source
  ),
  filtered as (
    select b.*
    from base_rows as b
    where
      (v_status is null or upper(coalesce(b.status, '')) = v_status)
      and (v_channel is null or b.channel = v_channel)
      and (
        v_operation_type is null
        or (b.source_kind = 'INVOICE' and upper(coalesce(b.outbox_type, '')) = v_operation_type)
      )
      and (
        p_entity_id is null
        or (b.source_kind = 'INVOICE' and b.entity_id = p_entity_id)
      )
      and (
        p_requires_user_action is null
        or b.requires_user_action = p_requires_user_action
      )
      and (
        v_queue_state is null
        or (
          b.source_kind = 'LEGACY'
          and b.queue_state = v_queue_state
        )
        or (
          b.source_kind = 'INVOICE'
          and (
            (v_queue_state = 'QUEUED' and b.queue_state in ('QUEUED', 'WAITING'))
            or (v_queue_state = 'READY' and b.queue_state = 'READY')
            or (v_queue_state = 'RUNNING' and b.queue_state = 'RUNNING')
            or (v_queue_state = 'SCHEDULED' and b.queue_state = 'RETRY_WAIT')
            or (
              v_queue_state = 'FAILED'
              and b.queue_state in ('FAILED', 'DEAD_LETTER', 'BLOCKED')
            )
            or (v_queue_state = 'ACTION_REQUIRED' and b.requires_user_action)
          )
        )
      )
      and (
        v_search is null
        or coalesce(b.to_address, '') ilike ('%' || v_search || '%')
        or coalesce(b.recipient_display_name, '') ilike ('%' || v_search || '%')
        or coalesce(b.subject, '') ilike ('%' || v_search || '%')
        or coalesce(b.body_text, '') ilike ('%' || v_search || '%')
        or coalesce(b.reference, '') ilike ('%' || v_search || '%')
        or coalesce(b.last_error, '') ilike ('%' || v_search || '%')
        or coalesce(b.outbox_type, '') ilike ('%' || v_search || '%')
        or coalesce(b.phase, '') ilike ('%' || v_search || '%')
        or b.outbox_id::text = lower(v_search)
        or coalesce(b.entity_id::text, '') = lower(v_search)
      )
  ),
  counted as (
    select
      count(*)::bigint as total_count,
      count(*) filter (where f.source_kind = 'LEGACY')::bigint as legacy_total,
      count(*) filter (where f.source_kind = 'INVOICE')::bigint as invoice_total
    from filtered as f
  ),
  ranked as (
    select
      f.*,
      row_number() over (
        order by
          case when v_sort_by = 'created_at_utc' and v_sort_dir = 'asc' then f.created_at_utc end asc nulls last,
          case when v_sort_by = 'created_at_utc' and v_sort_dir = 'desc' then f.created_at_utc end desc nulls last,
          case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'asc' then f.scheduled_for_utc end asc nulls last,
          case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'desc' then f.scheduled_for_utc end desc nulls last,
          case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'asc' then f.effective_ready_at_utc end asc nulls last,
          case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'desc' then f.effective_ready_at_utc end desc nulls last,
          case when v_sort_by = 'status' and v_sort_dir = 'asc' then upper(coalesce(f.status, '')) end asc nulls last,
          case when v_sort_by = 'status' and v_sort_dir = 'desc' then upper(coalesce(f.status, '')) end desc nulls last,
          case when v_sort_by = 'channel' and v_sort_dir = 'asc' then f.channel end asc nulls last,
          case when v_sort_by = 'channel' and v_sort_dir = 'desc' then f.channel end desc nulls last,
          f.created_at_utc desc,
          f.channel asc,
          f.outbox_id desc
      ) as sort_index
    from filtered as f
  ),
  page_rows as (
    select r.*
    from ranked as r
    where r.sort_index > v_offset
      and r.sort_index <= (v_offset + v_limit)
  )
  select
    c.total_count,
    c.legacy_total,
    c.invoice_total,
    coalesce(
      jsonb_agg(
        jsonb_strip_nulls(
          jsonb_build_object(
            'channel', p.channel,
            'outbox_id', p.outbox_id::text,
            'id', p.outbox_id::text,
            'outbox_type', p.outbox_type,
            'type', p.outbox_type,
            'status', p.status,
            'queue_state', p.queue_state,
            'delivery_status', p.delivery_status,
            'created_at_utc', p.created_at_utc::text,
            'sent_at', case when p.sent_at is null then null else p.sent_at::text end,
            'delivered_at', case when p.delivered_at is null then null else p.delivered_at::text end,
            'read_at', case when p.read_at is null then null else p.read_at::text end,
            'failed_at', case when p.failed_at is null then null else p.failed_at::text end,
            'to_address', p.to_address,
            'subject', p.subject,
            'body_preview', case
              when p.body_text is null then null
              when char_length(p.body_text) <= 200 then p.body_text
              else left(p.body_text, 200) || '…'
            end,
            'reference', p.reference,
            'provider_message_id', p.provider_message_id,
            'last_error', p.last_error,
            'error_code', p.last_error,
            'created_by', case when p.created_by is null then null else p.created_by::text end,
            'recipient_kind', p.recipient_kind,
            'recipient_id', case when p.recipient_id is null then null else p.recipient_id::text end,
            'recipient_display_name', p.recipient_display_name,
            'context_kind', p.context_kind,
            'context_id', case when p.context_id is null then null else p.context_id::text end,
            'entity_type', p.entity_type,
            'entity_id', case when p.entity_id is null then null else p.entity_id::text end,
            'mailshot_run_id', case when p.mailshot_run_id is null then null else p.mailshot_run_id::text end,
            'document_template_id', case when p.document_template_id is null then null else p.document_template_id::text end,
            'scheduled_for_utc', case when p.scheduled_for_utc is null then null else p.scheduled_for_utc::text end,
            'next_attempt_at_utc', case when p.next_attempt_at_utc is null then null else p.next_attempt_at_utc::text end,
            'effective_ready_at_utc', p.effective_ready_at_utc::text,
            'is_scheduled', p.is_scheduled,
            'requires_user_action', p.requires_user_action,
            'phase', p.phase,
            'change_seq', p.change_seq,
            'progress_summary', case
              when p.source_kind = 'INVOICE' then jsonb_build_object(
                'completed_units', coalesce(p.completed_units, 0),
                'total_units', coalesce(p.total_units, 0),
                'failed_units', coalesce(p.failed_units, 0),
                'pages_complete', case
                  when coalesce(p.progress_json ->> 'pages_complete', '') ~ '^[0-9]+$'
                    then (p.progress_json ->> 'pages_complete')::integer
                  else 0
                end,
                'pages_total', case
                  when coalesce(p.progress_json ->> 'pages_total', '') ~ '^[0-9]+$'
                    then (p.progress_json ->> 'pages_total')::integer
                  else 0
                end,
                'status_message', nullif(left(coalesce(p.progress_json ->> 'status_message', ''), 200), '')
              )
              else null
            end,
            'retry_summary', case
              when p.source_kind = 'INVOICE' then jsonb_build_object(
                'run_after_utc', case when p.scheduled_for_utc is null then null else p.scheduled_for_utc::text end,
                'attempt_count', case
                  when coalesce(p.result_json #>> '{attempt_summary,attempt_count}', '') ~ '^[0-9]+$'
                    then (p.result_json #>> '{attempt_summary,attempt_count}')::integer
                  else null
                end,
                'attempt_detail_available', true
              )
              else null
            end,
            'legal_issue_state', case
              when p.source_kind = 'INVOICE' then coalesce(
                nullif(p.result_json ->> 'legal_issue_state', ''),
                nullif(p.progress_json ->> 'legal_issue_state', ''),
                'NOT_REQUESTED'
              )
              else null
            end,
            'delivery_state', case
              when p.source_kind = 'INVOICE' then coalesce(
                nullif(p.result_json ->> 'delivery_state', ''),
                nullif(p.progress_json ->> 'delivery_state', ''),
                'NOT_REQUESTED'
              )
              else null
            end
          )
        )
        order by p.sort_index
      ) filter (where p.outbox_id is not null),
      '[]'::jsonb
    )
  into
    v_total_count,
    v_legacy_total,
    v_invoice_total,
    v_items
  from counted as c
  left join page_rows as p on true
  group by c.total_count, c.legacy_total, c.invoice_total;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total_count,
    'source_totals', jsonb_build_object(
      'legacy', v_legacy_total,
      'invoice', v_invoice_total
    ),
    'limit', v_limit,
    'offset', v_offset,
    'returned_count', jsonb_array_length(v_items),
    'has_more', (v_offset + jsonb_array_length(v_items)) < v_total_count,
    'next_offset', case
      when (v_offset + jsonb_array_length(v_items)) < v_total_count
        then v_offset + jsonb_array_length(v_items)
      else null
    end,
    'sort', jsonb_build_object('sort_by', v_sort_by, 'sort_dir', v_sort_dir),
    'snapshot_at_utc', v_snapshot_at_utc::text,
    'items', v_items
  );
end;
$function$;

alter function public.outbox_unified_list_v2(
  text,
  text,
  text,
  text,
  text,
  uuid,
  boolean,
  integer,
  integer,
  text,
  text,
  timestamptz
) owner to postgres;

revoke all on function public.outbox_unified_list_v2(
  text,
  text,
  text,
  text,
  text,
  uuid,
  boolean,
  integer,
  integer,
  text,
  text,
  timestamptz
) from public, anon, authenticated;

grant execute on function public.outbox_unified_list_v2(
  text,
  text,
  text,
  text,
  text,
  uuid,
  boolean,
  integer,
  integer,
  text,
  text,
  timestamptz
) to postgres, service_role;

notify pgrst, 'reload schema';

commit;
