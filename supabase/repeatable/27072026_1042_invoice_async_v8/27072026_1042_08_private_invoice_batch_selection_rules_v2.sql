create or replace function private._invoice_batch_selection_rules_v2(
  p_selection jsonb default '{}'::jsonb
)
returns table(
  rule_sequence integer,
  action text,
  selector_type text,
  selection_key text,
  group_key text,
  week_ending_date date,
  client_id uuid,
  candidate_id uuid,
  status_code text
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_selection jsonb := coalesce(p_selection, '{}'::jsonb);
  v_rules jsonb;
begin
  if jsonb_typeof(v_selection) is distinct from 'object' then
    raise exception using errcode='22023', message='BATCH_SELECTION_INVALID';
  end if;

  if exists (
    select 1 from jsonb_object_keys(v_selection) key_name
    where key_name not in ('contract_version','mode','default_selected','rules')
  ) then
    raise exception using errcode='22023', message='BATCH_SELECTION_UNKNOWN_FIELD';
  end if;

  if coalesce(v_selection->>'contract_version','') <>
      'INVOICE_BATCH_SELECTION_V2' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_CONTRACT_INVALID';
  end if;

  if upper(coalesce(v_selection->>'mode','')) <> 'IMPLICIT_ALL' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_MODE_UNSUPPORTED';
  end if;

  if jsonb_typeof(v_selection->'default_selected') is distinct from 'boolean'
     or coalesce((v_selection->>'default_selected')::boolean, false) is not true then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_DEFAULT_INVALID';
  end if;

  v_rules := coalesce(v_selection->'rules', '[]'::jsonb);
  if jsonb_typeof(v_rules) is distinct from 'array' then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULES_INVALID';
  end if;

  if jsonb_array_length(v_rules) > 10000 then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    where jsonb_typeof(raw.rule) is distinct from 'object'
       or jsonb_typeof(raw.rule->'selector') is distinct from 'object'
       or exists (
         select 1 from jsonb_object_keys(raw.rule) key_name
         where key_name not in ('sequence','action','selector')
       )
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    where coalesce(raw.rule->>'sequence','') !~ '^[1-9][0-9]{0,8}$'
       or upper(coalesce(raw.rule->>'action','')) not in ('INCLUDE','EXCLUDE')
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select
        (rule->>'sequence')::integer sequence_no,
        lag((rule->>'sequence')::integer) over (order by ordinal) previous_sequence
      from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    ) ordered_rules
    where previous_sequence is not null
      and sequence_no <= previous_sequence
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select (rule->>'sequence')::integer sequence_no, count(*) row_count
      from jsonb_array_elements(v_rules) raw(rule)
      group by (rule->>'sequence')::integer
    ) duplicate_rules
    where row_count > 1
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral (
      select
        upper(coalesce(raw.rule#>>'{selector,type}','')) selector_type,
        array(
          select key_name
          from jsonb_object_keys(raw.rule->'selector') key_name
          where key_name <> 'type'
            and raw.rule->'selector'->key_name <> 'null'::jsonb
            and nullif(btrim(raw.rule->'selector'->>key_name),'') is not null
          order by key_name
        ) supplied_fields
    ) selector
    cross join lateral (
      select case selector.selector_type
        when 'ROW' then array['selection_key']::text[]
        when 'WEEK' then array['week_ending_date']::text[]
        when 'CLIENT' then array['client_id']::text[]
        when 'CANDIDATE' then array['candidate_id']::text[]
        when 'STATUS' then array['status_code']::text[]
        when 'WEEK_CLIENT' then array['client_id','week_ending_date']::text[]
        when 'WEEK_CLIENT_CANDIDATE'
          then array['candidate_id','client_id','week_ending_date']::text[]
        when 'STATUS_WEEK' then array['status_code','week_ending_date']::text[]
        when 'STATUS_WEEK_CLIENT'
          then array['client_id','status_code','week_ending_date']::text[]
      end expected_fields
    ) expected
    where selector.selector_type not in (
      'ROW','WEEK','CLIENT','CANDIDATE','STATUS','WEEK_CLIENT',
      'WEEK_CLIENT_CANDIDATE','STATUS_WEEK','STATUS_WEEK_CLIENT'
    )
       or expected.expected_fields is null
       or selector.supplied_fields is distinct from expected.expected_fields
       or exists (
         select 1
         from jsonb_object_keys(raw.rule->'selector') key_name
         where key_name not in (
           'type','selection_key','week_ending_date',
           'client_id','candidate_id','status_code'
         )
       )
       or (
         selector.selector_type = 'ROW'
         and (
           length(btrim(raw.rule#>>'{selector,selection_key}')) > 512
           or btrim(raw.rule#>>'{selector,selection_key}') = ''
         )
       )
       or (
         selector.selector_type in (
           'WEEK','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE',
           'STATUS_WEEK','STATUS_WEEK_CLIENT'
         )
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),
           'date'
         )
       )
       or (
         selector.selector_type in (
           'CLIENT','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE',
           'STATUS_WEEK_CLIENT'
         )
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,client_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type in ('CANDIDATE','WEEK_CLIENT_CANDIDATE')
         and not pg_input_is_valid(
           btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),
           'uuid'
         )
       )
       or (
         selector.selector_type in ('STATUS','STATUS_WEEK','STATUS_WEEK_CLIENT')
         and (
           btrim(coalesce(raw.rule#>>'{selector,status_code}','')) = ''
           or length(btrim(raw.rule#>>'{selector,status_code}')) > 120
         )
       )
  ) then
    raise exception using
      errcode='22023',
      message='BATCH_SELECTION_SELECTOR_INVALID';
  end if;

  return query
  select
    (raw.rule->>'sequence')::integer,
    upper(raw.rule->>'action'),
    upper(raw.rule#>>'{selector,type}'),
    nullif(btrim(raw.rule#>>'{selector,selection_key}'),''),
    null::text,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),
      'date'
    ) then btrim(raw.rule#>>'{selector,week_ending_date}')::date end,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,client_id}','')),
      'uuid'
    ) then btrim(raw.rule#>>'{selector,client_id}')::uuid end,
    case when pg_input_is_valid(
      btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),
      'uuid'
    ) then btrim(raw.rule#>>'{selector,candidate_id}')::uuid end,
    nullif(upper(btrim(raw.rule#>>'{selector,status_code}')),'')
  from jsonb_array_elements(v_rules) raw(rule)
  order by (raw.rule->>'sequence')::integer;
end;
$function$;

alter function private._invoice_batch_selection_rules_v2(jsonb)
  owner to postgres;
revoke all on function private._invoice_batch_selection_rules_v2(jsonb)
  from public, anon, authenticated;
grant execute on function private._invoice_batch_selection_rules_v2(jsonb)
  to service_role;
