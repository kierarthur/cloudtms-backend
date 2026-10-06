-- Small factual readers only. No history reconstruction, financial calculator,
-- live post-Draft enrichment or agency-wide enumeration. Public authority is
-- the closed Office preparation wrapper; these owners have no service grant.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_current_work_page_v1(
  p_candidate_id uuid,p_after_week date,p_after_work_id uuid,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_row record; v_rows jsonb:='[]'::jsonb;
  v_bytes integer:=512; v_count integer:=0; v_more boolean:=false;
  v_query text;
begin
  if p_candidate_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_week is null)<>(p_after_work_id is null)
     or (p_after_week is not null and not pg_catalog.isfinite(p_after_week)) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- candidate/week/id is an existing index. The revision join is one exact
  -- current identity, never a search for a usable older approval. This is a
  -- factual list, NOT an assertion that an item is payable or has no blocker.
  -- Two fixed query shapes, bound values only. A nullable OR in a cached
  -- generic plan otherwise filters every earlier row again on later pages.
  -- EXECUTE replans this small query; it never interpolates caller values.
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
      select w.id as work_id,w.candidate_id,w.original_timesheet_id,w.work_kind,
        w.week_ending_date,w.approval_state,w.current_revision_id,w.applied_revision_id,
        coalesce(w.approval_state='APPROVED' and w.current_revision_id=w.applied_revision_id
          and r.sealed_at_utc is not null,false) as position_ready,
        r.source_pay_channel,r.timesheet_reference,r.client_display_name,
        r.approved_source_ex_vat::text
      from private.bpay_next_work w
      left join private.bpay_next_work_revision r
        on r.work_id=w.id and r.id=w.current_revision_id
      where w.candidate_id=$1$query$;
  if p_after_work_id is not null then
    v_query:=v_query||' and (w.week_ending_date,w.id)>($2,$3)';
  end if;
  v_query:=v_query||' order by w.week_ending_date,w.id limit $4) page';
  for v_row in execute v_query using p_candidate_id,p_after_week,p_after_work_id,p_limit
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;

create or replace function private.bpay_next_worker_status_page_v1(
  p_run_id uuid,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_header jsonb;v_row record;v_rows jsonb:='[]'::jsonb;
  v_bytes integer;v_count integer:=0;v_more boolean:=false;
  v_query text;
begin
  if p_run_id is null or p_worker_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_transfer_no is not null and p_after_transfer_no<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- One run, one worker, one accepted-projection unique lookup. During a net
  -- replacement show the accepted projection AND an explicit pending flag;
  -- never portray the new request as already accepted. Original CSV advice
  -- uses the transfer's own projection, not this current progress pointer.
  select pg_catalog.jsonb_build_object(
    'run_id',r.id,'run_status',r.status,'selection_state',r.selection_state,
    'review_revision',r.review_revision::text,'run_worker_id',w.id,
    'candidate_id',w.candidate_id,'worker_status',w.status,
    'target_pay_channel',w.target_pay_channel,'umbrella_id',w.target_umbrella_id,
    'preparation_revision',w.preparation_revision::text,
    'case_selection_revision',w.case_selection_revision::text,
    'net_request_revision',w.net_request_revision::text,
    'net_projection_revision',w.net_projection_revision::text,
    'net_pending',w.net_request_revision<>w.net_projection_revision,
    'review_issue_code',w.review_issue_code,'review_issue_work_id',w.review_issue_work_id,
    'frozen_gross_ex_vat',w.gross_ex_vat::text,'frozen_gross_vat',w.gross_vat::text,
    'frozen_gross_inc_vat',w.gross_inc_vat::text,
    'projection_id',p.id,'projection_input_kind',p.input_kind,
    'entered_paye_net',p.entered_paye_net::text,
    'accepted_recoveries',p.accepted_recoveries::text,
    'accepted_net_additions',p.accepted_net_additions::text,
    'cash_amount',p.cash_amount::text
  ) into v_header
  from private.bpay_next_pay_run r
  join private.bpay_next_run_worker w on w.run_id=r.id and w.id=p_worker_id
  left join private.bpay_next_net_projection p
    on p.run_worker_id=w.id and p.projection_no=w.net_projection_revision and p.retired_at_utc is null
  where r.id=p_run_id;
  if not found then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
  v_bytes:=512+pg_catalog.octet_length(v_header::text);
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
      select t.id as transfer_id,t.transfer_no::text,t.projection_id,t.execution_kind,
        t.status,t.cash_amount::text,t.original_transfer_id
      from private.bpay_next_transfer t where t.run_worker_id=$1$query$;
  if p_after_transfer_no is not null then
    v_query:=v_query||' and t.transfer_no>$2';
  end if;
  v_query:=v_query||' order by t.transfer_no,t.id limit $3) page';
  for v_row in execute v_query using p_worker_id,p_after_transfer_no,p_limit
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;
alter function private.bpay_next_current_work_page_v1(uuid,date,uuid,integer) owner to postgres;
alter function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_current_work_page_v1(uuid,date,uuid,integer),
  private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)
from public,anon,authenticated,service_role;
commit;
