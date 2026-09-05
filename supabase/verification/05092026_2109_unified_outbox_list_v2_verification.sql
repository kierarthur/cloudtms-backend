\set ON_ERROR_STOP on

-- Read-only installed-definition and bounded runtime proof for the unified
-- Office Outbox projection. This verifier does not alter application data.
begin;
set local statement_timeout = '120s';

do $verification$
declare
  v_signature regprocedure := pg_catalog.to_regprocedure(
    'public.outbox_unified_list_v2(text,text,text,text,text,uuid,boolean,integer,integer,text,text,timestamp with time zone)'
  );
  v_owner text;
  v_security_definer boolean;
  v_volatility "char";
  v_config text[];
  v_public_execute boolean;
  v_definition text;
  v_snapshot_at_utc timestamptz := pg_catalog.clock_timestamp();
  v_payload jsonb;
  v_items jsonb;
  v_legacy_total bigint;
  v_invoice_total bigint;
  v_expected_total bigint;
  v_actual_total bigint;
  v_duplicate_count bigint;
  v_violation_count bigint;
  v_sort_by text;
  v_sort_dir text;
begin
  if v_signature is null then
    raise exception 'UNIFIED_OUTBOX_LIST_V2_FUNCTION_MISSING';
  end if;

  select
    pg_catalog.pg_get_userbyid(proc.proowner),
    proc.prosecdef,
    proc.provolatile,
    proc.proconfig,
    exists (
      select 1
      from pg_catalog.aclexplode(
        coalesce(proc.proacl, pg_catalog.acldefault('f', proc.proowner))
      ) as acl
      where acl.grantee = 0
        and acl.privilege_type = 'EXECUTE'
    ),
    pg_catalog.pg_get_functiondef(proc.oid)
  into strict
    v_owner,
    v_security_definer,
    v_volatility,
    v_config,
    v_public_execute,
    v_definition
  from pg_catalog.pg_proc as proc
  where proc.oid = v_signature;

  if v_owner not in (current_user, 'postgres')
     or not v_security_definer
     or v_volatility <> 's'
     or not (
       'search_path=pg_catalog, public, pg_temp'
       = any(coalesce(v_config, array[]::text[]))
     )
     or v_public_execute
     or (
       pg_catalog.to_regrole('anon') is not null
       and pg_catalog.has_function_privilege('anon', v_signature, 'EXECUTE')
     )
     or (
       pg_catalog.to_regrole('authenticated') is not null
       and pg_catalog.has_function_privilege('authenticated', v_signature, 'EXECUTE')
     )
     or pg_catalog.to_regrole('service_role') is null
     or not pg_catalog.has_function_privilege('service_role', v_signature, 'EXECUTE') then
    raise exception 'UNIFIED_OUTBOX_LIST_V2_SECURITY_INVALID';
  end if;

  if v_definition !~ 'public\.v_outbox_unified'
     or v_definition !~ 'public\.invoice_operations'
     or v_definition !~ 'OPERATION_CONTROL_REQUEST'
     or v_definition !~ 'pg_input_is_valid'
     or v_definition ~* 'SET[[:space:]]+(LOCAL[[:space:]]+)?(statement_timeout|lock_timeout|idle_in_transaction_session_timeout)' then
    raise exception 'UNIFIED_OUTBOX_LIST_V2_DEFINITION_INVALID';
  end if;

  select pg_catalog.count(*)::bigint
  into v_legacy_total
  from public.v_outbox_unified as unified_row
  where unified_row.created_at_utc <= v_snapshot_at_utc;

  select pg_catalog.count(*)::bigint
  into v_invoice_total
  from public.invoice_operations as operation_row
  where operation_row.created_at_utc <= v_snapshot_at_utc
    and operation_row.operation_type <> 'OPERATION_CONTROL_REQUEST';

  v_expected_total := v_legacy_total + v_invoice_total;
  v_payload := public.outbox_unified_list_v2(
    p_limit => 500,
    p_offset => 0,
    p_sort_by => 'created_at_utc',
    p_sort_dir => 'desc',
    p_snapshot_at_utc => v_snapshot_at_utc
  );
  v_items := coalesce(v_payload -> 'items', '[]'::jsonb);
  v_actual_total := coalesce((v_payload ->> 'total_count')::bigint, -1);

  if pg_catalog.jsonb_typeof(v_payload) <> 'object'
     or pg_catalog.jsonb_typeof(v_items) <> 'array'
     or coalesce((v_payload ->> 'ok')::boolean, false) is not true
     or v_actual_total <> v_expected_total
     or coalesce((v_payload #>> '{source_totals,legacy}')::bigint, -1) <> v_legacy_total
     or coalesce((v_payload #>> '{source_totals,invoice}')::bigint, -1) <> v_invoice_total
     or pg_catalog.jsonb_array_length(v_items) <> least(v_expected_total, 500)::integer
     or coalesce((v_payload ->> 'returned_count')::integer, -1)
        <> pg_catalog.jsonb_array_length(v_items)
     or coalesce((v_payload ->> 'has_more')::boolean, false)
        <> (v_expected_total > 500) then
    raise exception 'UNIFIED_OUTBOX_LIST_V2_COUNT_CONTRACT_INVALID';
  end if;

  select pg_catalog.count(*) - pg_catalog.count(distinct (
    upper(coalesce(item ->> 'channel', '')) || '::' || coalesce(item ->> 'outbox_id', '')
  ))
  into v_duplicate_count
  from pg_catalog.jsonb_array_elements(v_items) as item;

  if v_duplicate_count <> 0
     or exists (
       select 1
       from pg_catalog.jsonb_array_elements(v_items) as item
       where coalesce(item ->> 'channel', '') = ''
          or coalesce(item ->> 'outbox_id', '') = ''
          or coalesce(item ->> 'created_at_utc', '') = ''
     ) then
    raise exception 'UNIFIED_OUTBOX_LIST_V2_ROW_IDENTITY_INVALID';
  end if;

  foreach v_sort_by in array array[
    'created_at_utc',
    'scheduled_for_utc',
    'effective_ready_at_utc',
    'status',
    'channel'
  ] loop
    foreach v_sort_dir in array array['asc', 'desc'] loop
      v_payload := public.outbox_unified_list_v2(
        p_limit => 500,
        p_offset => 0,
        p_sort_by => v_sort_by,
        p_sort_dir => v_sort_dir,
        p_snapshot_at_utc => v_snapshot_at_utc
      );

      if coalesce((v_payload ->> 'total_count')::bigint, -1) <> v_expected_total
         or coalesce(v_payload #>> '{sort,sort_by}', '') <> v_sort_by
         or coalesce(v_payload #>> '{sort,sort_dir}', '') <> v_sort_dir then
        raise exception 'UNIFIED_OUTBOX_LIST_V2_SORT_METADATA_INVALID: % %',
          v_sort_by,
          v_sort_dir;
      end if;

      if v_sort_by in ('created_at_utc', 'scheduled_for_utc', 'effective_ready_at_utc') then
        with ordered as (
          select
            ordinality,
            case
              when coalesce(item ->> v_sort_by, '') = '' then null::timestamptz
              else (item ->> v_sort_by)::timestamptz
            end as sort_value
          from pg_catalog.jsonb_array_elements(v_payload -> 'items')
            with ordinality as page_item(item, ordinality)
        ),
        compared as (
          select
            ordinality,
            sort_value,
            pg_catalog.lag(sort_value) over (order by ordinality) as previous_value
          from ordered
        )
        select pg_catalog.count(*)::bigint
        into v_violation_count
        from compared
        where ordinality > 1
          and (
            (previous_value is null and sort_value is not null)
            or (
              previous_value is not null
              and sort_value is not null
              and (
                (v_sort_dir = 'asc' and previous_value > sort_value)
                or (v_sort_dir = 'desc' and previous_value < sort_value)
              )
            )
          );
      else
        with ordered as (
          select
            ordinality,
            nullif(upper(coalesce(item ->> v_sort_by, '')), '') as sort_value
          from pg_catalog.jsonb_array_elements(v_payload -> 'items')
            with ordinality as page_item(item, ordinality)
        ),
        compared as (
          select
            ordinality,
            sort_value,
            pg_catalog.lag(sort_value) over (order by ordinality) as previous_value
          from ordered
        )
        select pg_catalog.count(*)::bigint
        into v_violation_count
        from compared
        where ordinality > 1
          and (
            (previous_value is null and sort_value is not null)
            or (
              previous_value is not null
              and sort_value is not null
              and (
                (v_sort_dir = 'asc' and previous_value > sort_value)
                or (v_sort_dir = 'desc' and previous_value < sort_value)
              )
            )
          );
      end if;

      if v_violation_count <> 0 then
        raise exception 'UNIFIED_OUTBOX_LIST_V2_SORT_ORDER_INVALID: % %',
          v_sort_by,
          v_sort_dir;
      end if;
    end loop;
  end loop;

  begin
    perform public.outbox_unified_list_v2(
      p_sort_by => 'not_a_visible_column',
      p_snapshot_at_utc => v_snapshot_at_utc
    );
    raise exception 'UNIFIED_OUTBOX_LIST_V2_INVALID_SORT_ACCEPTED';
  exception
    when sqlstate '22023' then
      null;
  end;
end;
$verification$;

rollback;
