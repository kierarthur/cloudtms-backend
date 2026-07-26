create or replace function private._invoice_batch_selection_rules_v1(
  p_selection jsonb default '{}'::jsonb
)
returns table(
  rule_sequence integer,
  action text,
  selector_type text,
  selection_key text,
  week_ending_date date,
  client_id uuid,
  candidate_id uuid
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_selection jsonb := coalesce(p_selection,'{}'::jsonb);
  v_rules jsonb;
  v_rule_count integer;
begin
  if jsonb_typeof(v_selection) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_INVALID';
  end if;

  if coalesce(v_selection->>'contract_version','') <> 'INVOICE_BATCH_SELECTION_V1' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_CONTRACT_INVALID';
  end if;

  if upper(coalesce(v_selection->>'mode','')) <> 'IMPLICIT_ALL' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_MODE_UNSUPPORTED';
  end if;

  if coalesce(v_selection->>'default_selected','false') not in ('true','t','1','yes','on') then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_DEFAULT_INVALID';
  end if;

  v_rules := coalesce(v_selection->'rules','[]'::jsonb);
  if jsonb_typeof(v_rules) is distinct from 'array' then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULES_INVALID';
  end if;

  v_rule_count := jsonb_array_length(v_rules);
  if v_rule_count > 10000 then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) with ordinality raw(rule, ordinal)
    where jsonb_typeof(raw.rule) is distinct from 'object'
       or jsonb_typeof(raw.rule->'selector') is distinct from 'object'
       or coalesce(raw.rule->>'sequence','') !~ '^[1-9][0-9]{0,8}$'
       or upper(coalesce(raw.rule->>'action','')) not in ('INCLUDE','EXCLUDE')
       or upper(coalesce(raw.rule#>>'{selector,type}','')) not in (
          'ROW','WEEK','CLIENT','CANDIDATE','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE'
       )
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_INVALID';
  end if;

  if exists (
    select 1
    from (
      select (rule->>'sequence')::integer seq, count(*) count_rows
      from jsonb_array_elements(v_rules) raw(rule)
      group by (rule->>'sequence')::integer
    ) duplicate_rules
    where duplicate_rules.count_rows > 1
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_SEQUENCE_DUPLICATE';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral jsonb_object_keys(raw.rule) key_name
    where key_name not in ('sequence','action','selector')
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_RULE_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral jsonb_object_keys(raw.rule->'selector') key_name
    where key_name not in ('type','selection_key','week_ending_date','client_id','candidate_id')
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_SELECTOR_UNKNOWN_FIELD';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(v_rules) raw(rule)
    cross join lateral (
      select upper(raw.rule#>>'{selector,type}') selector_type,
             btrim(coalesce(raw.rule#>>'{selector,selection_key}','')) selection_key,
             btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')) week_ending_text,
             btrim(coalesce(raw.rule#>>'{selector,client_id}','')) client_id_text,
             btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')) candidate_id_text
    ) selector_values
    where (selector_values.selector_type = 'ROW'
             and (selector_values.selection_key = '' or length(selector_values.selection_key) > 512))
       or (selector_values.selector_type in ('WEEK','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.week_ending_text,'date'))
       or (selector_values.selector_type in ('CLIENT','WEEK_CLIENT','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.client_id_text,'uuid'))
       or (selector_values.selector_type in ('CANDIDATE','WEEK_CLIENT_CANDIDATE')
             and not pg_input_is_valid(selector_values.candidate_id_text,'uuid'))
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SELECTION_SELECTOR_INVALID';
  end if;

  return query
  select
    (raw.rule->>'sequence')::integer as rule_sequence,
    upper(raw.rule->>'action') as action,
    upper(raw.rule#>>'{selector,type}') as selector_type,
    nullif(btrim(raw.rule#>>'{selector,selection_key}'),'') as selection_key,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,week_ending_date}','')),'date')
      then btrim(raw.rule#>>'{selector,week_ending_date}')::date end as week_ending_date,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,client_id}','')),'uuid')
      then btrim(raw.rule#>>'{selector,client_id}')::uuid end as client_id,
    case when pg_input_is_valid(btrim(coalesce(raw.rule#>>'{selector,candidate_id}','')),'uuid')
      then btrim(raw.rule#>>'{selector,candidate_id}')::uuid end as candidate_id
  from jsonb_array_elements(v_rules) raw(rule)
  order by (raw.rule->>'sequence')::integer;
end;
$function$;

revoke all on function private._invoice_batch_selection_rules_v1(jsonb) from public, anon, authenticated;
grant execute on function private._invoice_batch_selection_rules_v1(jsonb) to service_role;
