create or replace function private._invoice_batch_issue_source_rows_for_ids_v2(
  p_invoice_ids uuid[],
  p_allow_early boolean default false,
  p_now_utc timestamptz default now()
) returns table(
  client_id uuid,
  client_name text,
  invoice_week_start date,
  week_ending_date date,
  invoice_json jsonb
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_invoice_ids uuid[];
begin
  if p_invoice_ids is null
     or cardinality(p_invoice_ids)<1
     or cardinality(p_invoice_ids)>250 then
    raise exception using
      errcode='22023',
      message='ISSUE_CANDIDATE_ID_LIMIT_EXCEEDED';
  end if;

  if exists (
    select 1
    from unnest(p_invoice_ids) supplied(invoice_id)
    where supplied.invoice_id is null
  ) then
    raise exception using
      errcode='22023',
      message='ISSUE_CANDIDATE_ID_INVALID';
  end if;

  select array_agg(deduplicated.invoice_id order by deduplicated.invoice_id)
  into v_invoice_ids
  from (
    select distinct supplied.invoice_id
    from unnest(p_invoice_ids) supplied(invoice_id)
  ) deduplicated;

  return query
  select source.client_id,
    source.client_name,
    source.invoice_week_start,
    source.week_ending_date,
    source.invoice_json
  from private._invoice_batch_issue_source_rows_core_v2(
    coalesce(p_allow_early,false),
    cardinality(v_invoice_ids),
    coalesce(p_now_utc,now()),
    v_invoice_ids
  ) source;
end;
$function$;

alter function private._invoice_batch_issue_source_rows_for_ids_v2(
  uuid[],boolean,timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_issue_source_rows_for_ids_v2(
  uuid[],boolean,timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_issue_source_rows_for_ids_v2(
  uuid[],boolean,timestamptz
) to service_role;
