create or replace function public.invoice_batch_issue_candidates(
  p_query jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_selection jsonb;
begin
  if coalesce(auth.role(),'') <> 'service_role'
     and not exists (
       select 1
       from public.tms_users u
       where u.id = auth.uid()
         and u.is_active
         and lower(btrim(coalesce(u.role,''))) = 'admin'
     ) then
    raise exception using
      errcode='42501',
      message='Active administrator or service role required';
  end if;

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if octet_length(convert_to(v_query::text,'UTF8')) > 4194304 then
    raise exception using errcode='54000', message='BATCH_REQUEST_TOO_LARGE';
  end if;

  v_selection := coalesce(v_query->'selection','{}'::jsonb);
  if octet_length(convert_to(
    private._invoice_batch_canonical_text_v2(v_selection),
    'UTF8'
  )) > 3145728 then
    raise exception using
      errcode='54000',
      message='BATCH_SELECTION_PAYLOAD_TOO_LARGE';
  end if;

  return private._invoice_batch_issue_candidate_rows_v2(v_query,now());
end;
$function$;

alter function public.invoice_batch_issue_candidates(jsonb) owner to postgres;
revoke all on function public.invoice_batch_issue_candidates(jsonb)
  from public,anon;
grant execute on function public.invoice_batch_issue_candidates(jsonb)
  to authenticated,service_role;
