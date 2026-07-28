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
  v_query_action text;
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_cursor jsonb;
  v_facet_request jsonb;
  v_selection jsonb;
  v_snapshot jsonb;
  v_page_size integer;
  v_normalized_filters jsonb;
  v_normalized_sort jsonb;
  v_group_selector_rules jsonb;
  v_normalized_group_selectors jsonb := '[]'::jsonb;
  v_normalized_query jsonb;
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
      'group_selectors'
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_UNKNOWN_FIELD';
  end if;

  if coalesce(v_query->>'contract_version', '') <>
      'INVOICE_BATCH_QUERY_V2' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  if not (v_query ? 'action')
     or jsonb_typeof(v_query->'action') is distinct from 'string'
     or nullif(btrim(v_query->>'action'), '') is null then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_ACTION_REQUIRED';
  end if;

  v_query_action := upper(btrim(v_query->>'action'));
  if v_query_action not in ('GENERATE', 'ISSUE')
     or v_query_action <> v_action then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_ACTION_MISMATCH';
  end if;

  if not (v_query ? 'mode')
     or jsonb_typeof(v_query->'mode') is distinct from 'string' then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_mode := upper(btrim(v_query->>'mode'));
  if v_mode not in (
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

  if not (v_query ?& array[
    'contract_version',
    'action',
    'mode',
    'snapshot',
    'filters',
    'sort',
    'selection'
  ]) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_INVALID';
  end if;

  if exists (
    select 1
    from jsonb_object_keys(v_query) key_name
    where (
      v_mode = 'PAGE'
      and key_name not in (
        'contract_version','action','mode','snapshot','page_size','cursor',
        'filters','sort','selection'
      )
    ) or (
      v_mode = 'FACETS'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','facet_request'
      )
    ) or (
      v_mode = 'SUMMARY'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','group_selectors'
      )
    ) or (
      v_mode = 'EXPAND_SELECTION'
      and key_name not in (
        'contract_version','action','mode','snapshot','page_size','cursor',
        'filters','sort','selection'
      )
    ) or (
      v_mode = 'EXPLICIT_KEYS'
      and key_name not in (
        'contract_version','action','mode','snapshot','filters','sort',
        'selection','selection_keys','expected_source_revisions'
      )
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_QUERY_MODE_FIELD_INVALID';
  end if;

  if v_mode = 'FACETS' and not (v_query ? 'facet_request')
     or v_mode = 'EXPLICIT_KEYS' and (
       not (v_query ? 'selection_keys')
       or not (v_query ? 'expected_source_revisions')
     ) then
    raise exception using
      errcode = '22023',
      message = case
        when v_mode = 'FACETS' then 'BATCH_FACET_REQUEST_INVALID'
        else 'BATCH_EXPLICIT_KEYS_INVALID'
      end;
  end if;

  if v_query ? 'page_size'
     and (
       jsonb_typeof(v_query->'page_size') <> 'number'
       or coalesce(v_query->>'page_size', '') !~ '^[1-9][0-9]{0,8}$'
       or (
         v_mode = 'PAGE'
         and (v_query->>'page_size')::integer > 100
       )
       or (
         v_mode = 'EXPAND_SELECTION'
         and (v_query->>'page_size')::integer > 250
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_PAGE_SIZE_INVALID';
  end if;

  v_page_size := case
    when v_mode = 'EXPAND_SELECTION'
      then coalesce((v_query->>'page_size')::integer, 250)
    when v_mode = 'PAGE'
      then coalesce((v_query->>'page_size')::integer, 100)
    else null
  end;

  v_filters := v_query->'filters';
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
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                v_filters->definition.field_name
              ) = 'array'
                then v_filters->definition.field_name
              else '[]'::jsonb
            end
          ) item(value)
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
        or length(btrim(v_filters->>'search')) > 200
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

  if exists (
    select 1
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(v_filters->'status_codes') = 'array'
          then v_filters->'status_codes'
        else '[]'::jsonb
      end
    ) status_code(value)
    where (
      v_action = 'GENERATE'
      and upper(status_code.value) not in (
        'READY','STALE','FAILED','IN_PROGRESS','BLOCKED'
      )
    ) or (
      v_action = 'ISSUE'
      and upper(status_code.value) not in (
        'READY','READY_SEND_BLOCKED','STALE','FAILED',
        'IN_PROGRESS','BLOCKED'
      )
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_BATCH_STATUS_INVALID';
  end if;

  v_sort := v_query->'sort';
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
         'after_sort_date',
         'after_sort_text',
         'after_sort_numeric'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_CURSOR_INVALID';
  end if;

  if v_mode in ('PAGE', 'EXPAND_SELECTION')
     and jsonb_typeof(v_query->'cursor') not in ('object', 'null') then
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
           from jsonb_array_elements_text(
             case
               when jsonb_typeof(v_facet_request->'kinds') = 'array'
                 then v_facet_request->'kinds'
               else '[]'::jsonb
             end
           ) kind(value)
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
          or length(btrim(v_facet_request->>'search')) > 200
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

  v_snapshot := v_query->'snapshot';
  if jsonb_typeof(v_snapshot) not in ('object', 'null') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_mode <> 'PAGE' and jsonb_typeof(v_snapshot) = 'null' then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_REQUIRED';
  end if;

  if jsonb_typeof(v_snapshot) = 'object'
     and (
       not (v_snapshot ?& array[
         'contract_version',
         'action',
         'at_utc',
         'revision',
         'expires_at_utc',
         'key_id',
         'token'
       ])
       or exists (
         select 1
         from jsonb_object_keys(v_snapshot) key_name
         where key_name not in (
           'contract_version',
           'action',
           'at_utc',
           'revision',
           'expires_at_utc',
           'key_id',
           'token'
         )
       )
       or coalesce(v_snapshot->>'contract_version', '') <>
          'INVOICE_BATCH_SNAPSHOT_V2'
       or upper(coalesce(v_snapshot->>'action', '')) <> v_action
       or coalesce(v_snapshot->>'revision', '') !~ '^[0-9]+$'
       or coalesce(v_snapshot->>'key_id', '') !~
          '^[a-z0-9][a-z0-9._-]{0,63}$'
       or nullif(v_snapshot->>'token', '') is null
       or not pg_input_is_valid(
         coalesce(v_snapshot->>'at_utc', ''),
         'timestamp with time zone'
       )
       or not pg_input_is_valid(
         coalesce(v_snapshot->>'expires_at_utc', ''),
         'timestamp with time zone'
       )
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_query ? 'group_selectors' then
    if jsonb_typeof(v_query->'group_selectors') is distinct from 'array'
       or jsonb_array_length(v_query->'group_selectors') > 400 then
      raise exception using
        errcode = '22023',
        message = 'INVOICE_BATCH_QUERY_MODE_FIELD_INVALID';
    end if;

    if exists (
      select 1
      from jsonb_array_elements(v_query->'group_selectors') selector(value)
      where jsonb_typeof(selector.value) is distinct from 'object'
    ) then
      raise exception using
        errcode = '22023',
        message = 'BATCH_SELECTION_SELECTOR_INVALID';
    end if;

    select coalesce(jsonb_agg(jsonb_build_object(
      'sequence', selector.ordinality,
      'action', 'INCLUDE',
      'selector', selector.value
    ) order by selector.ordinality), '[]'::jsonb)
    into v_group_selector_rules
    from jsonb_array_elements(v_query->'group_selectors')
      with ordinality selector(value, ordinality);

    select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'type', normalized.selector_type,
      'selection_key', normalized.selection_key,
      'week_ending_date', normalized.week_ending_date,
      'client_id', normalized.client_id,
      'candidate_id', normalized.candidate_id,
      'status_code', normalized.status_code
    )) order by normalized.rule_sequence), '[]'::jsonb)
    into v_normalized_group_selectors
    from private._invoice_batch_selection_rules_v2(jsonb_build_object(
      'contract_version', 'INVOICE_BATCH_SELECTION_V2',
      'mode', 'IMPLICIT_ALL',
      'default_selected', true,
      'rules', v_group_selector_rules
    )) normalized;

    if (
      select count(*)
      from jsonb_array_elements(v_normalized_group_selectors)
    ) <> (
      select count(distinct selector.value)
      from jsonb_array_elements(v_normalized_group_selectors) selector(value)
    ) then
      raise exception using
        errcode = '22023',
        message = 'BATCH_SELECTION_SELECTOR_INVALID';
    end if;
  end if;

  if v_mode = 'EXPLICIT_KEYS' and (
    jsonb_typeof(v_query->'selection_keys') is distinct from 'array'
    or jsonb_array_length(v_query->'selection_keys') < 1
    or jsonb_array_length(v_query->'selection_keys') > 100
    or jsonb_typeof(v_query->'expected_source_revisions')
       is distinct from 'object'
    or exists (
      select 1
      from jsonb_array_elements(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      ) key_item(value)
      where jsonb_typeof(key_item.value) is distinct from 'string'
         or nullif(btrim(key_item.value #>> '{}'), '') is null
         or length(btrim(key_item.value #>> '{}')) > 512
    )
    or (
      select count(*)
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
    ) <> (
      select count(distinct key_value)
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
        explicit_key(key_value)
    )
    or exists (
      select 1
      from jsonb_array_elements_text(
        case
          when jsonb_typeof(v_query->'selection_keys') = 'array'
            then v_query->'selection_keys'
          else '[]'::jsonb
        end
      )
        explicit_key(key_value)
      where nullif(
        v_query->'expected_source_revisions'->>explicit_key.key_value,
        ''
      ) is null
    )
    or exists (
      select 1
      from jsonb_each(
        case
          when jsonb_typeof(
            v_query->'expected_source_revisions'
          ) = 'object'
            then v_query->'expected_source_revisions'
          else '{}'::jsonb
        end
      ) expected_revision(selection_key,value)
      where not (
        v_query->'selection_keys' ? expected_revision.selection_key
      )
         or jsonb_typeof(expected_revision.value) <> 'string'
         or nullif(
              btrim(expected_revision.value #>> '{}'),
              ''
            ) is null
         or length(
              btrim(expected_revision.value #>> '{}')
            ) > 512
    )
  ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_EXPLICIT_KEYS_INVALID';
  end if;

  select jsonb_build_object(
    'client_ids', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct lower(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'client_ids', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'candidate_ids', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct lower(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'candidate_ids', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'week_endings', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct (item.value::date)::text value
        from jsonb_array_elements_text(
          coalesce(v_filters->'week_endings', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'week_ending_from', case
      when nullif(v_filters->>'week_ending_from', '') is null then null
      else ((v_filters->>'week_ending_from')::date)::text
    end,
    'week_ending_to', case
      when nullif(v_filters->>'week_ending_to', '') is null then null
      else ((v_filters->>'week_ending_to')::date)::text
    end,
    'status_codes', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'status_codes', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'blocker_codes', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'blocker_codes', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb),
    'search', nullif(btrim(v_filters->>'search'), ''),
    'allow_early', coalesce((v_filters->>'allow_early')::boolean, false),
    'display_mode', upper(coalesce(
      nullif(v_filters->>'display_mode', ''),
      'ALL'
    )),
    'invoice_streams', coalesce((
      select jsonb_agg(to_jsonb(value) order by value)
      from (
        select distinct upper(btrim(item.value)) value
        from jsonb_array_elements_text(
          coalesce(v_filters->'invoice_streams', '[]'::jsonb)
        ) item(value)
      ) normalized
    ), '[]'::jsonb)
  ) into v_normalized_filters;

  v_normalized_sort := jsonb_build_object(
    'group_preset', upper(coalesce(
      nullif(v_sort->>'group_preset', ''),
      'WEEK_CLIENT_CANDIDATE'
    )),
    'sort_key', upper(coalesce(
      nullif(v_sort->>'sort_key', ''),
      'WEEK_ENDING_DATE'
    )),
    'sort_direction', upper(coalesce(
      nullif(v_sort->>'sort_direction', ''),
      'ASC'
    ))
  );

  v_normalized_query := jsonb_build_object(
    'contract_version', 'INVOICE_BATCH_QUERY_V2',
    'action', v_action,
    'mode', v_mode,
    'snapshot', v_snapshot,
    'filters', v_normalized_filters,
    'sort', v_normalized_sort,
    'selection', v_selection
  );

  if v_mode in ('PAGE', 'EXPAND_SELECTION') then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'page_size', v_page_size,
      'cursor', case
        when jsonb_typeof(v_query->'cursor') = 'object' then v_cursor
        else null
      end
    );
  elsif v_mode = 'FACETS' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'facet_request', v_facet_request
    );
  elsif v_mode = 'SUMMARY' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'group_selectors', v_normalized_group_selectors
    );
  elsif v_mode = 'EXPLICIT_KEYS' then
    v_normalized_query := v_normalized_query || jsonb_build_object(
      'selection_keys', v_query->'selection_keys',
      'expected_source_revisions', v_query->'expected_source_revisions'
    );
  end if;

  return v_normalized_query;
end;
$function$;

alter function private._invoice_batch_query_validate_v2(jsonb,text)
  owner to postgres;
revoke all on function private._invoice_batch_query_validate_v2(jsonb,text)
  from public, anon, authenticated;
grant execute on function private._invoice_batch_query_validate_v2(jsonb,text)
  to service_role;
