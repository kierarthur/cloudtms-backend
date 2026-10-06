-- Repeatable CloudTMS function/view authority: weekly_source_office_next_paid_context_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Fixed owner discriminator only. Never substitute legacy payment evidence
-- when NEXT owns the module; this helper grants no financial admission.
create or replace function private.weekly_source_office_next_owner_v1()
returns boolean language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_next boolean;
begin
  if to_regclass('private.bpay_next_module_control') is null then return false; end if;
  execute 'select active_owner=''NEXT'' and owner_epoch>0 from private.bpay_next_module_control where id=1'
    into strict v_next;
  return v_next is true;
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_NEXT_OWNER_UNAVAILABLE' using errcode='23514';
end;
$function$;

-- C1: the Source server derives the exact current physical root/contract/
-- Candidate/week/raw booking and owning revision in the same STABLE snapshot
-- as the fixed Banking reader. It does not accept financial identities from
-- the Office browser, trim a booking, choose history or assemble page totals.
create or replace function private.weekly_source_office_next_paid_evidence_page_v1(
  p_actor_user_id uuid,p_root_timesheet_id uuid,p_kind text,
  p_context_id uuid default null,p_after jsonb default null,p_limit integer default 100
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb; v_work record; v_request jsonb; v_page jsonb; v_header jsonb;
  v_revision uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_actor_user_id is null or p_root_timesheet_id is null or p_kind is null
    or p_kind not in ('COMPONENTS','ACTIVE_HOLDS','WORKER_TRANSFERS')
    or p_limit is null or p_limit not between 1 and 100
    or (p_kind='WORKER_TRANSFERS' and p_context_id is null)
    or (p_kind<>'WORKER_TRANSFERS' and p_context_id is not null) then
    raise exception 'WEEKLY_SOURCE_NEXT_PAID_REQUEST_INVALID' using errcode='22023';
  end if;
  v_scope:=public.weekly_source_office_authorise_scope_v1(jsonb_build_object(
    'actor_user_id',p_actor_user_id,'timesheet_id',p_root_timesheet_id));
  if v_scope->'applicable' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_NEXT_PAID_SCOPE_FORBIDDEN' using errcode='42501';
  end if;
  if not private.weekly_source_office_next_owner_v1() then
    return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',
      'available',false,'reason','NEXT_NOT_ACTIVE','request',null,'page',null);
  end if;
  -- Fixed query, not arbitrary caller SQL. Dynamic planning allows this
  -- Source definition to install before the joined NEXT schema is activated.
  begin
  execute $scope$
    select w.id,w.current_revision_id,w.applied_revision_id
    from public.timesheets t join public.contracts c on c.id=t.contract_id
    join private.bpay_next_work w on w.work_kind='SOURCE' and w.booking_id=t.booking_id
      and w.contract_id=c.id and w.candidate_id=c.candidate_id and w.week_ending_date=t.week_ending_date
    where t.timesheet_id=$1 and t.is_current and t.revoked_at is null and t.archived_at_utc is null
      and t.sheet_scope='WEEKLY' and t.line_type='HOURS'
  $scope$ into strict v_work using p_root_timesheet_id;
  exception when no_data_found then
    return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',
      'available',false,'reason','NO_QUALIFIED_ORIGINAL','request',null,'page',null);
  when too_many_rows then
    raise exception 'WEEKLY_SOURCE_NEXT_PAID_SCOPE_AMBIGUOUS' using errcode='23514';
  end;
  v_revision:=coalesce(v_work.current_revision_id,v_work.applied_revision_id);
  if v_revision is null then
    return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',
      'available',false,'reason','CURRENT_ORIGIN_UNAVAILABLE','request',null,'page',null);
  end if;
  v_request:=jsonb_build_object('version','SOURCE_PAID_EVIDENCE_V1',
    'actor_user_id',p_actor_user_id,'root_timesheet_id',p_root_timesheet_id,
    'work_id',v_work.id,'expected_revision_id',v_revision,'kind',p_kind,
    'context_id',p_context_id,'after',p_after,'limit',p_limit);
  begin
    execute 'select public.bpay_next_source_paid_evidence_page_v1($1)' into v_page using v_request;
  exception when undefined_function or undefined_table then
    -- Missing informational dependency only; permission, request, foreign-scope
    -- and damaged financial tuples are deliberately NOT swallowed.
    return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',
      'available',false,'reason','NEXT_READER_UNAVAILABLE','request',null,'page',null);
  end;
  v_header:=v_page->'header';
  if jsonb_typeof(v_page) is distinct from 'object'
    or not v_page ?& array['version','ok','kind','header','rows','complete','next_cursor','quantity_certificate','activity_coverage']
    or v_page-array['version','ok','kind','header','rows','complete','next_cursor','quantity_certificate','activity_coverage']<>'{}'::jsonb
    or v_page->>'version' is distinct from 'SOURCE_PAID_EVIDENCE_V1'
    or v_page->'ok' is distinct from 'true'::jsonb or v_page->>'kind' is distinct from p_kind
    or jsonb_typeof(v_page->'rows') is distinct from 'array'
    or jsonb_array_length(v_page->'rows')>p_limit or octet_length(v_page::text)>120000
    or jsonb_typeof(v_page->'complete') is distinct from 'boolean'
    or jsonb_typeof(v_header) is distinct from 'object'
    or not v_header ?& array['root_timesheet_id','work_id','expected_revision_id','module_epoch',
      'current_origin_state','financial_view_revision','scope','quantity_authority_scope','context']
    or v_header-array['root_timesheet_id','work_id','expected_revision_id','module_epoch',
      'current_origin_state','financial_view_revision','scope','quantity_authority_scope','context']<>'{}'::jsonb
    or v_header->>'root_timesheet_id' is distinct from p_root_timesheet_id::text
    or v_header->>'work_id' is distinct from v_work.id::text
    or v_header->>'expected_revision_id' is distinct from v_revision::text
    or v_header->>'scope' is distinct from 'EXACT_SOURCE_WORK'
    or (p_kind<>'WORKER_TRANSFERS' and v_header->'context' is distinct from 'null'::jsonb)
    or (p_kind='WORKER_TRANSFERS' and v_header#>>'{context,run_worker_id}' is distinct from p_context_id::text) then
    raise exception 'WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID' using errcode='23514';
  end if;
  -- The Worker consumer independently validates every closed row, nullable
  -- type, certificate and cursor against this independently derived request.
  return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',
    'available',true,'reason',null,'request',v_request,'page',v_page);
end;
$function$;
alter function private.weekly_source_office_next_owner_v1() owner to current_user;
alter function private.weekly_source_office_next_paid_evidence_page_v1(uuid,uuid,text,uuid,jsonb,integer) owner to current_user;
revoke all on function private.weekly_source_office_next_owner_v1() from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_next_paid_evidence_page_v1(uuid,uuid,text,uuid,jsonb,integer) from public,anon,authenticated,service_role;

commit;
