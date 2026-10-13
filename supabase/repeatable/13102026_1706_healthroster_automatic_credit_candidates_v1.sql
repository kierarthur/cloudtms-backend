-- Exact APPLIED scoped-credit read; activation/API/document qualification required.
\set ON_ERROR_STOP on
begin;
-- PROPOSAL. Existing request and scoped credit owners remain the authority.
-- This bounded, purpose-only projection contains no financial snapshot or new
-- correction, query, comparison, document, payment or idempotency store.
create or replace function public.timesheet_correction_credit_candidates_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_actor uuid; v_after uuid; v_query jsonb; f jsonb; r record; v_scope jsonb;
  v_rows jsonb:='[]'::jsonb; v_more boolean:=false; v_next uuid; v_count integer:=0;
  v_code text; v_revision text; v_fact jsonb; v_facts jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_request) k
    where k not in('actor_user_id','query','after_request_id'))
    or pg_input_is_valid(p_request->>'actor_user_id','uuid') is distinct from true
    or (p_request->>'after_request_id' is not null and pg_input_is_valid(p_request->>'after_request_id','uuid') is distinct from true) then
    raise exception 'CORRECTION_CREDIT_QUERY_INVALID' using errcode='22023'; end if;
  v_actor:=(p_request->>'actor_user_id')::uuid; v_after:=(p_request->>'after_request_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active and lower(btrim(u.role))='admin') then
    raise exception 'INVOICE_ADMINISTRATOR_PERMISSION_REQUIRED' using errcode='42501'; end if;
  v_query:=private._invoice_batch_query_validate_v2(p_request->'query','GENERATE'); f:=v_query->'filters';
  if v_query->>'mode'='EXPLICIT_KEYS' then
    raise exception 'CORRECTION_CREDIT_EXPLICIT_SELECTION_FORBIDDEN' using errcode='22023'; end if;
  for r in
    select request.*,invoice.invoice_no,coalesce(nullif(candidate.display_name,''),
      nullif(concat_ws(' ',candidate.first_name,candidate.last_name),''),candidate.tms_ref,candidate.id::text) candidate_name,
      client.name client_name
    from private.timesheet_correction_requests request
    join public.invoices invoice on invoice.id=request.original_invoice_id and invoice.type='INVOICE' and invoice.status in('ISSUED','PAID')
      and invoice.client_id=request.client_id
    join public.candidates candidate on candidate.id=request.candidate_id
    join public.clients client on client.id=request.client_id
    where request.state='APPLIED' and request.origin in('SIGNED_TIMESHEET_CORRECTION','AGENCY_INVOICE_CORRECTION')
      and request.reversal_timesheet_id is not null and (v_after is null or request.id>v_after)
      and not exists(select 1 from public.invoices cn join public.invoice_lines l on l.invoice_id=cn.id
        where cn.original_invoice_id=invoice.id and cn.type='CREDIT_NOTE' and l.meta_json->>'reversal_timesheet_id'=request.reversal_timesheet_id::text)
      and (jsonb_array_length(coalesce(f->'client_ids','[]'::jsonb))=0 or request.client_id::text in(select jsonb_array_elements_text(f->'client_ids')))
      and (jsonb_array_length(coalesce(f->'candidate_ids','[]'::jsonb))=0 or request.candidate_id::text in(select jsonb_array_elements_text(f->'candidate_ids')))
      and (jsonb_array_length(coalesce(f->'week_endings','[]'::jsonb))=0 or request.week_ending_date::text in(select jsonb_array_elements_text(f->'week_endings')))
      and (nullif(f->>'week_ending_from','') is null or request.week_ending_date>=(f->>'week_ending_from')::date)
      and (nullif(f->>'week_ending_to','') is null or request.week_ending_date<=(f->>'week_ending_to')::date)
      and (jsonb_array_length(coalesce(f->'invoice_streams','[]'::jsonb))=0 or exists(select 1 from jsonb_array_elements_text(f->'invoice_streams') stream
        where stream=upper(coalesce(nullif(invoice.header_snapshot_json#>>'{meta,invoice_stream}',''),
          nullif(invoice.header_snapshot_json->>'invoice_stream',''),case when lower(coalesce(invoice.header_snapshot_json#>>'{meta,self_bill}','false'))
          in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end))))
      and (nullif(btrim(f->>'search'),'') is null or concat_ws(' ',invoice.invoice_no,candidate.display_name,candidate.first_name,
        candidate.last_name,candidate.tms_ref,client.name,request.week_ending_date::text) ilike '%'||btrim(f->>'search')||'%')
    order by request.id
  loop
    -- A read error blocks this row truthfully. No missing-document or signed
    -- evidence becomes a fabricated READY row or an invoice-wide exception.
    begin
      v_scope:=private.invoice_credit_scope_context_v1(r.original_invoice_id,r.reversal_timesheet_id,null,null,false);
      v_code:=case when v_scope->>'valid'='true' then null else coalesce(v_scope->>'blocker','CREDIT_SCOPE_AUTHORITY_UNPROVED') end;
      v_revision:=v_scope->>'revision';
      if v_code is null and (v_scope->>'request_id' is distinct from r.id::text
        or v_scope->>'correction_id' is distinct from r.correction_id
        or v_scope->>'root_timesheet_id' is distinct from r.root_timesheet_id::text
        or v_scope->>'reversal_timesheet_id' is distinct from r.reversal_timesheet_id::text
        or v_scope->>'reversed_timesheet_id' is distinct from r.reversed_timesheet_id::text
        or v_scope->>'replacement_timesheet_id' is distinct from r.replacement_timesheet_id::text
        or v_scope->>'origin' is distinct from r.origin) then
        v_code:='CREDIT_SCOPE_AUTHORITY_UNPROVED';v_revision:=null;
      end if;
    exception when others then v_code:='CREDIT_SCOPE_AUTHORITY_UNAVAILABLE';v_revision:=null; end;
    -- No READY without the exact immutable native scoped revision.
    if coalesce(v_revision,'') !~ '^[0-9a-f]{64}$' then
      v_code:=coalesce(v_code,'CREDIT_SCOPE_AUTHORITY_UNPROVED');v_revision:=null;
    end if;
    if jsonb_array_length(coalesce(f->'status_codes','[]'::jsonb))>0 and not exists(select 1 from jsonb_array_elements_text(f->'status_codes') s
      where s=case when v_code is null then 'READY' else 'BLOCKED' end) then continue; end if;
    if coalesce(f->>'display_mode','ALL')<>'ALL' and f->>'display_mode'<>case when v_code is null then 'READY' else 'BLOCKED' end then continue; end if;
    if jsonb_array_length(coalesce(f->'blocker_codes','[]'::jsonb))>0 and not exists(select 1 from jsonb_array_elements_text(f->'blocker_codes') s where s=v_code) then continue; end if;
    if v_count=250 then v_more:=true;exit;end if;
    if v_revision is not null and v_revision !~ '^[0-9a-f]{64}$' then
      raise exception 'CORRECTION_CREDIT_PROJECTION_INVALID' using errcode='55000'; end if;
    v_fact:=jsonb_build_object('request_id',r.id,'root_timesheet_id',r.root_timesheet_id,'correction_id',r.correction_id,'state_revision',r.state_revision::text,
      'source_revision',v_revision,'original_invoice_id',r.original_invoice_id,'reversal_timesheet_id',r.reversal_timesheet_id,
      'reversed_timesheet_id',r.reversed_timesheet_id,'replacement_timesheet_id',r.replacement_timesheet_id,
      'client_id',r.client_id,'candidate_id',r.candidate_id,'week_ending_date',r.week_ending_date,
      'candidate_name',r.candidate_name,'invoice_number',r.invoice_no,'blocker_code',v_code);
    v_rows:=v_rows||jsonb_build_array(v_fact||jsonb_build_object('selection_key','healthroster-credit:'||r.original_invoice_id||':'||r.reversal_timesheet_id,
      'selectable',false,'automatic',true));
    v_facts:=v_facts||jsonb_build_array(v_fact);v_count:=v_count+1;v_next:=r.id;
  end loop;
  return jsonb_build_object('contract_version','TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1','rows',v_rows,
    'snapshot_hash',encode(private.weekly_source_sha256_jsonb_v1('TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1',
      jsonb_build_object('query',v_query,'after_request_id',v_after,'facts',v_facts)),'hex'),
    'page',jsonb_build_object('returned_count',v_count,'has_more',v_more,'next_request_id',case when v_more then v_next else null end));
end;
$function$;
alter function public.timesheet_correction_credit_candidates_v1(jsonb) owner to postgres;
revoke all on function public.timesheet_correction_credit_candidates_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_credit_candidates_v1(jsonb) to service_role;

commit;
