create or replace function private._invoice_batch_query_validate_v2(
  p_query jsonb,
  p_action text
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_query jsonb := coalesce(p_query, '{}'::jsonb);
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_filters jsonb;
  v_sort jsonb;
  v_cursor jsonb;
  v_facet_request jsonb;
  v_selection jsonb;
begin
  if v_action not in ('GENERATE', 'ISSUE')
     or jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_INVALID';
  end if;

  if octet_length(v_query::text) > 4194304 then
    raise exception using
      errcode = '22023',
      message = 'BATCH_REQUEST_TOO_LARGE';
  end if;

  if exists (
    select 1
    from jsonb_object_keys(v_query) key_name
    where key_name not in (
      'contract_version',
      'action',
      'mode',
      'snapshot',
      'page_size',
      'cursor',
      'filters',
      'sort',
      'selection',
      'selection_keys',
      'expected_source_revisions',
      'facet_request',
      'allow_early',
      'display_mode'
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_UNKNOWN_FIELD';
  end if;

  if coalesce(v_query->>'contract_version', '') <>
      'INVOICE_BATCH_QUERY_V2'
     or upper(coalesce(v_query->>'action', v_action)) <> v_action then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  if upper(coalesce(v_query->>'mode', 'PAGE')) not in (
    'PAGE',
    'FACETS',
    'SUMMARY',
    'EXPAND_SELECTION',
    'EXPLICIT_KEYS'
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  if v_query ? 'page_size'
     and (
       jsonb_typeof(v_query->'page_size') <> 'number'
       or coalesce(v_query->>'page_size', '') !~ '^[1-9][0-9]{0,8}$'
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_PAGE_SIZE_INVALID';
  end if;

  if v_query ? 'allow_early'
     and jsonb_typeof(v_query->'allow_early') <> 'boolean' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_ALLOW_EARLY_INVALID';
  end if;

  if v_query ? 'display_mode'
     and (
       jsonb_typeof(v_query->'display_mode') <> 'string'
       or upper(v_query->>'display_mode') not in ('ALL', 'READY', 'BLOCKED')
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  v_filters := coalesce(v_query->'filters', '{}'::jsonb);
  if jsonb_typeof(v_filters) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_filters) key_name
       where key_name not in (
         'client_ids',
         'candidate_ids',
         'week_endings',
         'week_ending_from',
         'week_ending_to',
         'status_codes',
         'blocker_codes',
         'search',
         'allow_early',
         'display_mode',
         'invoice_streams'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from (
      values
        ('client_ids', 'uuid', 500),
        ('candidate_ids', 'uuid', 500),
        ('week_endings', 'date', 500),
        ('status_codes', 'text', 100),
        ('blocker_codes', 'text', 250),
        ('invoice_streams', 'text', 20)
    ) definition(field_name, value_type, maximum_count)
    where v_filters ? definition.field_name
      and (
        jsonb_typeof(v_filters->definition.field_name) <> 'array'
        or jsonb_array_length(v_filters->definition.field_name) >
          definition.maximum_count
        or exists (
          select 1
          from jsonb_array_elements(v_filters->definition.field_name) item(value)
          where jsonb_typeof(item.value) <> 'string'
             or nullif(btrim(item.value #>> '{}'), '') is null
             or length(btrim(item.value #>> '{}')) > 120
             or (
               definition.value_type = 'uuid'
               and not pg_input_is_valid(item.value #>> '{}', 'uuid')
             )
             or (
               definition.value_type = 'date'
               and not pg_input_is_valid(item.value #>> '{}', 'date')
             )
        )
      )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_INVALID';
  end if;

  if (v_filters ? 'week_ending_from'
      and jsonb_typeof(v_filters->'week_ending_from') <> 'null'
      and (
        jsonb_typeof(v_filters->'week_ending_from') <> 'string'
        or not pg_input_is_valid(v_filters->>'week_ending_from', 'date')
      ))
     or (v_filters ? 'week_ending_to'
      and jsonb_typeof(v_filters->'week_ending_to') <> 'null'
      and (
        jsonb_typeof(v_filters->'week_ending_to') <> 'string'
        or not pg_input_is_valid(v_filters->>'week_ending_to', 'date')
      ))
     or (
        v_filters ? 'week_ending_from'
        and v_filters ? 'week_ending_to'
        and jsonb_typeof(v_filters->'week_ending_from') <> 'null'
        and jsonb_typeof(v_filters->'week_ending_to') <> 'null'
        and (v_filters->>'week_ending_from')::date >
            (v_filters->>'week_ending_to')::date
      )
     or (v_filters ? 'search'
      and jsonb_typeof(v_filters->'search') <> 'null'
      and (
        jsonb_typeof(v_filters->'search') <> 'string'
        or length(btrim(v_filters->>'search')) > 120
      ))
     or (v_filters ? 'allow_early' and
       jsonb_typeof(v_filters->'allow_early') <> 'boolean')
     or (v_filters ? 'display_mode' and (
       jsonb_typeof(v_filters->'display_mode') <> 'string'
       or upper(v_filters->>'display_mode') not in ('ALL', 'READY', 'BLOCKED')
     )) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_FILTER_INVALID';
  end if;

  v_sort := coalesce(v_query->'sort', '{}'::jsonb);
  if jsonb_typeof(v_sort) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_sort) key_name
       where key_name not in ('group_preset', 'sort_key', 'sort_direction')
     )
     or (
       v_sort ? 'group_preset'
       and (
         jsonb_typeof(v_sort->'group_preset') <> 'string'
         or upper(v_sort->>'group_preset') not in (
           'WEEK_CLIENT_CANDIDATE',
           'CLIENT_WEEK_CANDIDATE',
           'CANDIDATE_WEEK_CLIENT',
           'STATUS_WEEK_CLIENT'
         )
       )
     )
     or (
       v_sort ? 'sort_key'
       and (
         jsonb_typeof(v_sort->'sort_key') <> 'string'
         or upper(v_sort->>'sort_key') not in (
           'WEEK_ENDING_DATE',
           'CLIENT_NAME',
           'CANDIDATE_NAME',
           'TOTAL_EX_VAT',
           'TOTAL_INC_VAT',
           'STATUS',
           'INVOICE_NUMBER'
         )
         or (
           upper(v_sort->>'sort_key') = 'INVOICE_NUMBER'
           and v_action <> 'ISSUE'
         )
       )
     )
     or (
       v_sort ? 'sort_direction'
       and (
         jsonb_typeof(v_sort->'sort_direction') <> 'string'
         or upper(v_sort->>'sort_direction') not in ('ASC', 'DESC')
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_SORT_INVALID';
  end if;

  v_cursor := case
    when jsonb_typeof(v_query->'cursor') = 'null' then '{}'::jsonb
    else coalesce(v_query->'cursor', '{}'::jsonb)
  end;
  if jsonb_typeof(v_cursor) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_cursor) key_name
       where key_name not in (
         'after_selection_key',
         'last_selection_key',
         'after_sort_date',
         'last_sort_date',
         'after_sort_text',
         'last_sort_text',
         'after_sort_numeric',
         'last_sort_numeric'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_CURSOR_INVALID';
  end if;

  v_facet_request := coalesce(v_query->'facet_request', '{}'::jsonb);
  if jsonb_typeof(v_facet_request) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(v_facet_request) key_name
       where key_name not in ('kinds', 'search', 'limit_per_kind', 'cursors')
     )
     or (
       v_facet_request ? 'kinds'
       and (
         jsonb_typeof(v_facet_request->'kinds') <> 'array'
         or jsonb_array_length(v_facet_request->'kinds') < 1
         or jsonb_array_length(v_facet_request->'kinds') > 5
         or exists (
           select 1
           from jsonb_array_elements_text(v_facet_request->'kinds') kind(value)
           where kind.value not in (
             'CLIENTS',
             'CANDIDATES',
             'WEEK_ENDINGS',
             'STATUSES',
             'BLOCKERS'
           )
         )
       )
     )
     or (
        v_facet_request ? 'search'
        and jsonb_typeof(v_facet_request->'search') <> 'null'
        and (
          jsonb_typeof(v_facet_request->'search') <> 'string'
          or length(btrim(v_facet_request->>'search')) > 120
       )
     )
     or (
       v_facet_request ? 'limit_per_kind'
       and (
         jsonb_typeof(v_facet_request->'limit_per_kind') <> 'number'
         or coalesce(v_facet_request->>'limit_per_kind', '') !~
            '^[1-9][0-9]{0,2}$'
         or (v_facet_request->>'limit_per_kind')::integer > 100
       )
     )
     or (
       v_facet_request ? 'cursors'
       and (
         jsonb_typeof(v_facet_request->'cursors') <> 'object'
         or exists (
           select 1
           from jsonb_object_keys(v_facet_request->'cursors') key_name
           where key_name not in (
             'clients',
             'candidates',
             'week_endings',
             'statuses',
             'blockers'
           )
         )
         or exists (
           select 1
           from jsonb_each(v_facet_request->'cursors') cursor_item(kind,value)
           where jsonb_typeof(cursor_item.value) <> 'object'
              or exists (
                select 1
                from jsonb_object_keys(cursor_item.value) cursor_key
                where (
                  cursor_item.kind in ('clients','candidates')
                  and cursor_key not in ('after_label','after_id')
                )
                   or (
                     cursor_item.kind='week_endings'
                     and cursor_key<>'after_value'
                   )
                   or (
                     cursor_item.kind in ('statuses','blockers')
                     and cursor_key<>'after_code'
                   )
              )
              or exists (
                select 1
                from jsonb_each(cursor_item.value) cursor_value(key,value)
                where jsonb_typeof(cursor_value.value)<>'string'
                   or nullif(btrim(cursor_value.value #>> '{}'),'') is null
                   or length(btrim(cursor_value.value #>> '{}'))>512
              )
         )
         or (
           v_facet_request#>>'{cursors,week_endings,after_value}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,week_endings,after_value}',
             'date'
           )
         )
         or (
           v_facet_request#>>'{cursors,clients,after_id}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,clients,after_id}',
             'uuid'
           )
         )
         or (
           v_facet_request#>>'{cursors,candidates,after_id}' is not null
           and not pg_input_is_valid(
             v_facet_request#>>'{cursors,candidates,after_id}',
             'uuid'
           )
         )
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_FACET_REQUEST_INVALID';
  end if;

  v_selection := coalesce(v_query->'selection', jsonb_build_object(
    'contract_version', 'INVOICE_BATCH_SELECTION_V2',
    'mode', 'IMPLICIT_ALL',
    'default_selected', true,
    'rules', '[]'::jsonb
  ));
  if octet_length(v_selection::text) > 3145728 then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SELECTION_PAYLOAD_TOO_LARGE';
  end if;
  perform 1
  from private._invoice_batch_selection_rules_v2(v_selection)
  limit 1;

  return v_query;
end;
$function$;

alter function private._invoice_batch_query_validate_v2(jsonb,text)
  owner to postgres;
revoke all on function private._invoice_batch_query_validate_v2(jsonb,text)
  from public, anon, authenticated;
grant execute on function private._invoice_batch_query_validate_v2(jsonb,text)
  to service_role;
