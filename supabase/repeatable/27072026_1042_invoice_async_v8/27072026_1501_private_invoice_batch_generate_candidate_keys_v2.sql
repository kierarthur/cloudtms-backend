create or replace function private._invoice_batch_generate_candidate_keys_v2(
  p_query jsonb,
  p_now_utc timestamptz default now()
) returns table(
  selection_key text,
  scope_key text,
  row_kind text,
  invoice_id uuid,
  source_revision text,
  client_id uuid,
  client_name text,
  candidate_ids jsonb,
  candidate_display text,
  week_ending_date date,
  currency text,
  invoice_stream text,
  total_ex_vat numeric,
  total_inc_vat numeric,
  row_status_seed text,
  blocker_codes_seed jsonb,
  is_early boolean,
  sort_date_key date,
  sort_text_key text,
  sort_numeric_key numeric,
  page_ordinal bigint,
  full_scope_count bigint
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with
query_input as materialized (
  select
    coalesce(p_query,'{}'::jsonb) query_json
),
params as materialized (
  select
    upper(coalesce(query_json->>'mode','PAGE')) mode,
    greatest(1,case
      when coalesce(query_json->>'page_size','')~'^[1-9][0-9]*$'
        then (query_json->>'page_size')::integer
      else 100
    end) page_size,
    coalesce(
      (query_json#>>'{filters,allow_early}')::boolean,
      false
    ) allow_early,
    upper(coalesce(
      nullif(query_json#>>'{filters,display_mode}',''),
      'ALL'
    )) display_mode,
    lower(nullif(btrim(coalesce(
      query_json#>>'{filters,search}',
      ''
    )),'')) search_text,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_from}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_from}')::date
    end week_ending_from,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{filters,week_ending_to}',''),
        'date'
      )
        then (query_json#>>'{filters,week_ending_to}')::date
    end week_ending_to,
    coalesce(query_json#>'{filters,client_ids}','[]'::jsonb)
      client_ids,
    coalesce(query_json#>'{filters,candidate_ids}','[]'::jsonb)
      candidate_ids,
    coalesce(query_json#>'{filters,week_endings}','[]'::jsonb)
      week_endings,
    coalesce(query_json#>'{filters,status_codes}','[]'::jsonb)
      status_codes,
    coalesce(query_json#>'{filters,blocker_codes}','[]'::jsonb)
      blocker_codes,
    coalesce(query_json#>'{filters,invoice_streams}','[]'::jsonb)
      invoice_streams,
    coalesce(query_json->'selection_keys','[]'::jsonb)
      selection_keys,
    upper(coalesce(
      nullif(query_json#>>'{sort,sort_key}',''),
      'WEEK_ENDING_DATE'
    )) sort_key,
    case
      when upper(coalesce(
        query_json#>>'{sort,sort_direction}',
        'ASC'
      ))='DESC'
        then 'DESC'
      else 'ASC'
    end sort_direction,
    nullif(query_json#>>'{cursor,after_selection_key}','')
      after_selection_key,
    case
      when pg_input_is_valid(
        nullif(query_json#>>'{cursor,after_sort_date}',''),
        'date'
      )
        then (query_json#>>'{cursor,after_sort_date}')::date
    end after_sort_date,
    nullif(query_json#>>'{cursor,after_sort_text}','')
      after_sort_text,
    case
      when coalesce(
        query_json#>>'{cursor,after_sort_numeric}',
        ''
      )~'^[+-]?[0-9]+([.][0-9]+)?$'
        then (
          query_json#>>'{cursor,after_sort_numeric}'
        )::numeric
    end after_sort_numeric
  from query_input
),
classified as materialized (
  select
    candidate.candidate_json->>'selection_key' selection_key,
    candidate.candidate_json->>'scope_key' scope_key,
    candidate.candidate_json->>'row_kind' row_kind,
    case
      when pg_input_is_valid(
        coalesce(candidate.candidate_json->>'invoice_id',''),
        'uuid'
      )
        then (candidate.candidate_json->>'invoice_id')::uuid
    end invoice_id,
    candidate.candidate_json->>'source_revision' source_revision,
    (candidate.candidate_json->>'client_id')::uuid client_id,
    candidate.candidate_json->>'client_name' client_name,
    coalesce(
      candidate.candidate_json->'candidate_ids',
      '[]'::jsonb
    ) candidate_ids,
    candidate.candidate_json->>'candidate_display' candidate_display,
    case
      when pg_input_is_valid(
        coalesce(candidate.candidate_json->>'week_ending_date',''),
        'date'
      )
        then (candidate.candidate_json->>'week_ending_date')::date
    end week_ending_date,
    coalesce(
      nullif(candidate.candidate_json->>'currency',''),
      'GBP'
    ) currency,
    upper(coalesce(
      nullif(candidate.candidate_json->>'invoice_stream',''),
      'NORMAL'
    )) invoice_stream,
    coalesce(
      (candidate.candidate_json->>'total_ex_vat')::numeric,
      0
    ) total_ex_vat,
    coalesce(
      (candidate.candidate_json->>'total_inc_vat')::numeric,
      0
    ) total_inc_vat,
    candidate.candidate_json->>'row_status' row_status,
    coalesce(
      candidate.candidate_json->'action_blocker_codes',
      '[]'::jsonb
    ) action_blocker_codes,
    coalesce(
      candidate.candidate_json->'informational_codes',
      '[]'::jsonb
    ) informational_codes,
    coalesce(
      (candidate.candidate_json->>'selectable')::boolean,
      false
    ) selectable,
    coalesce(
      (candidate.candidate_json->>'is_early')::boolean,
      false
    ) is_early
  from private._invoice_batch_generate_classification_v2(
    true,
    null,
    coalesce(p_now_utc,statement_timestamp())
  ) candidate
),
selection_rules as materialized (
  select rule.*
  from query_input
  cross join lateral private._invoice_batch_selection_rules_v2(
    coalesce(
      query_input.query_json->'selection',
      jsonb_build_object(
        'contract_version','INVOICE_BATCH_SELECTION_V2',
        'mode','IMPLICIT_ALL',
        'default_selected',true,
        'rules','[]'::jsonb
      )
    )
  ) rule
),
classified_with_selection as materialized (
  select
    classified.*,
    coalesce((
      select rule.action
      from selection_rules rule
      where (rule.selector_type='ROW'
          and rule.selection_key=classified.selection_key)
         or (rule.selector_type='WEEK'
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='CLIENT'
          and rule.client_id=classified.client_id)
         or (rule.selector_type='CANDIDATE' and exists(
           select 1
           from jsonb_array_elements_text(
             classified.candidate_ids
           ) candidate(value)
           where pg_input_is_valid(candidate.value,'uuid')
             and candidate.value::uuid=rule.candidate_id
         ))
         or (rule.selector_type='STATUS'
          and rule.status_code=classified.row_status)
         or (rule.selector_type='WEEK_CLIENT'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
         or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id
          and exists(
            select 1
            from jsonb_array_elements_text(
              classified.candidate_ids
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=rule.candidate_id
          ))
         or (rule.selector_type='STATUS_WEEK'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date)
         or (rule.selector_type='STATUS_WEEK_CLIENT'
          and rule.status_code=classified.row_status
          and rule.week_ending_date=classified.week_ending_date
          and rule.client_id=classified.client_id)
      order by rule.rule_sequence desc
      limit 1
    ),'INCLUDE') last_selection_action
  from classified
),
filtered as materialized (
  select classified.*
  from classified_with_selection classified
  cross join params
  where (params.allow_early or not classified.is_early)
    and (
      jsonb_array_length(params.client_ids)=0
      or classified.client_id::text in (
        select value
        from jsonb_array_elements_text(params.client_ids) value
      )
    )
    and (
      jsonb_array_length(params.candidate_ids)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.candidate_ids
        ) candidate(value)
        where candidate.value in (
          select value
          from jsonb_array_elements_text(params.candidate_ids) value
        )
      )
    )
    and (
      jsonb_array_length(params.week_endings)=0
      or classified.week_ending_date::text in (
        select value
        from jsonb_array_elements_text(params.week_endings) value
      )
    )
    and (
      jsonb_array_length(params.status_codes)=0
      or classified.row_status in (
        select upper(value)
        from jsonb_array_elements_text(params.status_codes) value
      )
    )
    and (
      jsonb_array_length(params.blocker_codes)=0
      or exists(
        select 1
        from jsonb_array_elements_text(
          classified.action_blocker_codes
          ||classified.informational_codes
        ) code(value)
        where code.value in (
          select upper(value)
          from jsonb_array_elements_text(params.blocker_codes) value
        )
      )
    )
    and (
      jsonb_array_length(params.invoice_streams)=0
      or classified.invoice_stream in (
        select upper(value)
        from jsonb_array_elements_text(params.invoice_streams) value
      )
    )
    and (
      params.mode<>'EXPLICIT_KEYS'
      or classified.selection_key in (
        select value
        from jsonb_array_elements_text(params.selection_keys) value
      )
    )
    and (
      params.search_text is null
      or lower(
        coalesce(classified.client_name,'')||' '||
        coalesce(classified.candidate_display,'')||' '||
        coalesce(classified.scope_key,'')
      ) like '%'||params.search_text||'%'
    )
    and (
      params.week_ending_from is null
      or classified.week_ending_date>=params.week_ending_from
    )
    and (
      params.week_ending_to is null
      or classified.week_ending_date<=params.week_ending_to
    )
    and (
      params.mode='EXPAND_SELECTION'
      or params.display_mode='ALL'
      or (
        params.display_mode='READY'
        and classified.selectable
      )
      or (
        params.display_mode='BLOCKED'
        and classified.row_status='BLOCKED'
      )
    )
    and (
      params.mode<>'EXPAND_SELECTION'
      or (
        classified.selectable
        and classified.last_selection_action<>'EXCLUDE'
      )
    )
),
sortable as materialized (
  select
    filtered.*,
    case
      when params.sort_key='WEEK_ENDING_DATE'
        then coalesce(
          filtered.week_ending_date,
          case
            when params.sort_direction='DESC'
              then date '0001-01-01'
            else date '9999-12-31'
          end
        )
    end sort_date_key,
    case
      when params.sort_key='CLIENT_NAME'
        then lower(coalesce(filtered.client_name,''))
      when params.sort_key='CANDIDATE_NAME'
        then lower(coalesce(filtered.candidate_display,''))
      when params.sort_key='STATUS'
        then lpad((case filtered.row_status
          when 'READY' then 10
          when 'STALE' then 20
          when 'FAILED' then 30
          when 'IN_PROGRESS' then 40
          else 50
        end)::text,3,'0')||'|'||lower(filtered.row_status)
    end sort_text_key,
    case
      when params.sort_key='TOTAL_EX_VAT'
        then filtered.total_ex_vat
      when params.sort_key='TOTAL_INC_VAT'
        then filtered.total_inc_vat
    end sort_numeric_key
  from filtered
  cross join params
),
scope_count as materialized (
  select count(*)::bigint full_scope_count
  from sortable
),
cursor_filtered as materialized (
  select sortable.*
  from sortable
  cross join params
  where params.after_selection_key is null
     or (
       params.mode='EXPAND_SELECTION'
       and sortable.selection_key>params.after_selection_key
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key='WEEK_ENDING_DATE'
       and params.after_sort_date is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_date_key>params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_date_key<params.after_sort_date
             or (
               sortable.sort_date_key=params.after_sort_date
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in(
         'CLIENT_NAME','CANDIDATE_NAME','STATUS'
       )
       and params.after_sort_text is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_text_key>params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_text_key<params.after_sort_text
             or (
               sortable.sort_text_key=params.after_sort_text
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
     or (
       params.mode<>'EXPAND_SELECTION'
       and params.sort_key in('TOTAL_EX_VAT','TOTAL_INC_VAT')
       and params.after_sort_numeric is not null
       and (
         (
           params.sort_direction='ASC'
           and (
             sortable.sort_numeric_key>params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
         or (
           params.sort_direction='DESC'
           and (
             sortable.sort_numeric_key<params.after_sort_numeric
             or (
               sortable.sort_numeric_key=params.after_sort_numeric
               and sortable.selection_key>
                 params.after_selection_key
             )
           )
         )
       )
     )
),
ordered as materialized (
  select
    cursor_filtered.*,
    row_number() over(order by
      case
        when params.mode='EXPAND_SELECTION'
          then cursor_filtered.selection_key
      end asc,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='ASC'
          then cursor_filtered.sort_date_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key='WEEK_ENDING_DATE'
         and params.sort_direction='DESC'
          then cursor_filtered.sort_date_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_text_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'CLIENT_NAME','CANDIDATE_NAME','STATUS'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_text_key
      end desc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='ASC'
          then cursor_filtered.sort_numeric_key
      end asc nulls last,
      case
        when params.mode<>'EXPAND_SELECTION'
         and params.sort_key in(
           'TOTAL_EX_VAT','TOTAL_INC_VAT'
         )
         and params.sort_direction='DESC'
          then cursor_filtered.sort_numeric_key
      end desc nulls last,
      cursor_filtered.selection_key
    ) page_ordinal
  from cursor_filtered
  cross join params
)
select
  ordered.selection_key,
  ordered.scope_key,
  ordered.row_kind,
  ordered.invoice_id,
  ordered.source_revision,
  ordered.client_id,
  ordered.client_name,
  ordered.candidate_ids,
  ordered.candidate_display,
  ordered.week_ending_date,
  ordered.currency,
  ordered.invoice_stream,
  ordered.total_ex_vat,
  ordered.total_inc_vat,
  ordered.row_status row_status_seed,
  ordered.action_blocker_codes blocker_codes_seed,
  ordered.is_early,
  ordered.sort_date_key,
  ordered.sort_text_key,
  ordered.sort_numeric_key,
  ordered.page_ordinal,
  scope_count.full_scope_count
from ordered
cross join params
cross join scope_count
where ordered.page_ordinal<=case
  when params.mode='EXPLICIT_KEYS'
    then jsonb_array_length(params.selection_keys)
  else params.page_size+1
end
order by ordered.page_ordinal;
$function$;

alter function private._invoice_batch_generate_candidate_keys_v2(
  jsonb,timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_generate_candidate_keys_v2(
  jsonb,timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_generate_candidate_keys_v2(
  jsonb,timestamptz
) to service_role;
