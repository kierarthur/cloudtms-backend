-- SP15 / BD125: same-transaction affected-Work refresh only.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_source_query_refresh_v1(p_contract_id uuid,p_work_event_id uuid)
returns void language plpgsql volatile security definer set search_path=pg_catalog,private
as $function$
declare v_ids uuid[];v_id uuid;
begin
 -- Identity only; eligibility is decided by the current Workbench classifier.
 if (select m.active_owner from private.bpay_next_module_control m where m.id=1) is distinct from 'NEXT' then return;end if;
 select array_agg(x.id order by x.id) into v_ids from (
  select distinct w.id from public.weekly_work_events e
  join public.contracts c on c.id=p_contract_id and c.candidate_id=e.candidate_id and c.client_id=e.client_id
  join public.contract_weeks cw on cw.contract_id=c.id and cw.additional_seq=0 and cw.is_adjustment is false
   and cw.week_ending_date between e.work_date and e.work_date+6
  join public.timesheets t on t.timesheet_id=cw.timesheet_id and t.contract_id=c.id
   and t.week_ending_date=cw.week_ending_date and t.is_adjustment is false
   and t.sheet_scope='WEEKLY' and t.line_type='HOURS'
  join private.bpay_next_work w on w.booking_id=t.booking_id and w.contract_id=c.id and w.work_kind='SOURCE'
   and w.candidate_id=e.candidate_id and w.week_ending_date=cw.week_ending_date
  where e.id=p_work_event_id order by w.id limit 2
 ) x;
 if coalesce(cardinality(v_ids),0)=0 then return;end if;
 if cardinality(v_ids)<>1 then
  raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_QUERY_WORK_IDENTITY_AMBIGUOUS';end if;
 v_id:=v_ids[1];
 perform private.bpay_next_workbench_refresh_work_v1(v_id);
end
$function$;

create or replace function public.weekly_source_manual_review_open_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_row_id uuid; v_command_id uuid; v_reason text; v_hash bytea;
  v_prior private.weekly_source_manual_review_commands%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_source jsonb; v_route jsonb; v_result jsonb; v_review uuid; v_existing boolean;
  v_family_id uuid; v_sequence bigint; v_count integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_request) k
    where k not in ('actor_user_id','source_row_id','reason','command_id')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_row_id:=(p_request->>'source_row_id')::uuid;
    v_command_id:=(p_request->>'command_id')::uuid;
  exception when others then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end;
  v_reason:=btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_row_id is null or v_command_id is null or char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MANUAL_REVIEW_OPEN_V2',p_request);
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND:'||v_command_id::text,0));
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  -- The global try-key serialises participating source writers. Re-read the
  -- actual source and current authority after admission; never use a stale UI tuple.
  v_source:=private.weekly_source_manual_review_source_v2(v_row_id);
  if v_source is null then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001'; end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=(v_source->>'source_cycle_id')::uuid;
  perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
    (v_source->>'client_id')::uuid,v_cycle.finalisation_week_ending);
  v_route:=private.weekly_source_office_route_key_v1(v_cycle.id,(v_source->>'candidate_id')::uuid,
    (v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,(v_source->>'work_date')::date);
  if v_route->>'authority_mode' is distinct from 'SOURCE_AUTHORITY' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='22023'; end if;
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_QUERY_FAMILY:'||
    (v_source->>'contract_id')||':'||(v_source->>'week_ending_date'),0));
  select r.id into v_review from private.weekly_source_manual_reviews r
    where r.source_group_id=v_cycle.source_group_id and r.work_event_id=(v_source->>'work_event_id')::uuid
      and r.state='OPEN' for update;
  v_existing:=v_review is not null;
  if v_existing and not exists(select 1 from private.weekly_source_manual_reviews r where r.id=v_review
    and r.candidate_id=(v_source->>'candidate_id')::uuid and r.client_id=(v_source->>'client_id')::uuid
    and r.contract_id=(v_source->>'contract_id')::uuid and r.work_date=(v_source->>'work_date')::date) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='55000'; end if;
  if not v_existing then
    select count(*),(array_agg(f.id))[1] into v_count,v_family_id
      from public.weekly_exceptional_pay_target_families f
      where f.contract_id=(v_source->>'contract_id')::uuid and f.candidate_id=(v_source->>'candidate_id')::uuid
        and f.week_ending_date=(v_source->>'week_ending_date')::date;
    if v_count>1 then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_AMBIGUOUS' using errcode='55000'; end if;
    select coalesce(max(e.event_sequence),0) into v_sequence from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_family_id and e.durable_work_event_id=(v_source->>'work_event_id')::uuid;
    insert into private.weekly_source_manual_reviews(source_group_id,source_cycle_id,upload_row_id,
      row_resolution_id,source_row_hash,work_event_id,candidate_id,client_id,contract_id,work_date,
      state,office_reason,opened_by_user_id)
    values(v_cycle.source_group_id,v_cycle.id,v_row_id,(v_source->>'row_resolution_id')::uuid,
      decode(v_source->>'source_row_hash','hex'),(v_source->>'work_event_id')::uuid,
      (v_source->>'candidate_id')::uuid,(v_source->>'client_id')::uuid,(v_source->>'contract_id')::uuid,
      (v_source->>'work_date')::date,'OPEN',v_reason,v_actor) returning id into v_review;
    perform public._audit_insert('weekly_source_manual_review',v_review::text,'WEEKLY_SOURCE_MANUAL_REVIEW_OPENED',null,
      jsonb_build_object('source_row_id',v_row_id,'work_event_id',v_source->'work_event_id','reason',v_reason,
        'opening_final_revision_id',v_source->'final_revision_id',
        'opening_projection_publication_id',v_source->'projection_publication_id'),
      'Office sent imported shift to Pay Queries',v_actor);
  end if;
  v_result:=jsonb_build_object('ok',true,'review_id',v_review,'already_open',v_existing,
    'problem','Manually queried','idempotent_replay',false);
  insert into private.weekly_source_manual_review_commands(command_id,command_kind,actor_user_id,request_sha256,
    review_id,source_cycle_id,client_id,week_ending_date,opening_final_revision_id,opening_projection_publication_id,
    opening_target_family_id,opening_event_sequence,result_json)
  values(v_command_id,'OPEN',v_actor,v_hash,v_review,v_cycle.id,(v_source->>'client_id')::uuid,
    (v_source->>'week_ending_date')::date,case when not v_existing then (v_source->>'final_revision_id')::uuid end,
    case when not v_existing then (v_source->>'projection_publication_id')::uuid end,
    case when not v_existing then v_family_id end,case when not v_existing then v_sequence end,v_result);
  if not v_existing then
    perform private.bpay_next_source_query_refresh_v1((v_source->>'contract_id')::uuid,(v_source->>'work_event_id')::uuid);
  end if;
  return v_result;
end;
$function$;

create or replace function public.weekly_source_manual_review_resolve_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid; v_review_id uuid; v_command_id uuid; v_kind text; v_hash bytea;
  v_prior private.weekly_source_manual_review_commands%rowtype;
  v_review private.weekly_source_manual_reviews%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_anchor private.weekly_source_manual_review_commands%rowtype;
  v_scope jsonb; v_source jsonb; v_result jsonb; v_receipt jsonb;
  v_local jsonb; v_c1 jsonb; v_next jsonb;
  v_approval_id uuid; v_generation_id uuid; v_source_row_id uuid;
  v_expected_hash text; v_count integer; v_local_count integer; v_c1_count integer;
  v_next_count integer; v_audit_count integer; v_week date;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_request) k
    where k not in ('actor_user_id','review_id','resolution_kind','expected_current_row_hash',
      'command_id','accepted_approval_id','accepted_generation_id')) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_review_id:=(p_request->>'review_id')::uuid;
    v_command_id:=(p_request->>'command_id')::uuid;
    v_approval_id:=(p_request->>'accepted_approval_id')::uuid;
    v_generation_id:=(p_request->>'accepted_generation_id')::uuid;
  exception when others then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end;
  v_kind:=p_request->>'resolution_kind';
  if v_actor is null or v_review_id is null or v_command_id is null
     or v_kind is null or v_kind not in ('OFFICE_ACCEPTED_SOURCE','PROTECTED_PAY') then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MANUAL_REVIEW_RESOLVE_V2',p_request);
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'RESOLVE' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash or v_prior.review_id is distinct from v_review_id then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,
      case when v_kind='PROTECTED_PAY' then 'APPROVE_PROTECTED_PAY' else 'ACCEPT_SYSTEM_HOURS' end,
      v_cycle.source_group_id,v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND:'||v_command_id::text,0));
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'RESOLVE' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash or v_prior.review_id is distinct from v_review_id then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,
      case when v_kind='PROTECTED_PAY' then 'APPROVE_PROTECTED_PAY' else 'ACCEPT_SYSTEM_HOURS' end,
      v_cycle.source_group_id,v_prior.client_id,v_cycle.finalisation_week_ending);
    return v_prior.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  select r.* into v_review from private.weekly_source_manual_reviews r where r.id=v_review_id;
  if not found or v_review.state is distinct from 'OPEN' then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_NOT_OPEN' using errcode='40001'; end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_review.source_cycle_id;
  perform private.weekly_source_office_authority_v1(v_actor,
    case when v_kind='PROTECTED_PAY' then 'APPROVE_PROTECTED_PAY' else 'ACCEPT_SYSTEM_HOURS' end,
    v_review.source_group_id,v_review.client_id,v_cycle.finalisation_week_ending);
  select c.week_ending_weekday_snapshot into v_count from public.contracts c where c.id=v_review.contract_id;
  if v_count is null or v_count not between 0 and 6 then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_AUTHORITY_INVALID' using errcode='55000'; end if;
  v_week:=v_review.work_date+((v_count-extract(dow from v_review.work_date)::integer+7)%7);
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_QUERY_FAMILY:'||v_review.contract_id::text||':'||v_week::text,0));
  select r.* into strict v_review from private.weekly_source_manual_reviews r where r.id=v_review_id and r.state='OPEN' for update;
  if v_kind='OFFICE_ACCEPTED_SOURCE' then
    if v_approval_id is not null or v_generation_id is not null then
      if v_approval_id is null or v_generation_id is null or p_request->>'expected_current_row_hash' is not null then
        raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_CURRENT_SOURCE_REQUIRED' using errcode='22023'; end if;
      -- This union arm is an actual accepted WITHDRAW/RECONCILE decision, not
      -- a client-supplied absence flag or a relaxed latest-WAIT receipt.
      v_receipt:=private.weekly_source_accepted_removal_query_evidence_v1(
        v_review.id,v_approval_id,v_generation_id,v_actor,v_command_id);
      if v_receipt is null then
        raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_CURRENT_SOURCE_REQUIRED' using errcode='55000'; end if;
      v_source_row_id:=(v_receipt#>>'{selected_source_witness,basis,upload_row_id}')::uuid;
    else
    v_expected_hash:=p_request->>'expected_current_row_hash';
    if v_expected_hash is null or v_expected_hash!~'^[0-9a-f]{64}$'
       or v_approval_id is not null or v_generation_id is not null then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_CURRENT_SOURCE_REQUIRED' using errcode='22023'; end if;
    select u.* into strict v_upload from public.weekly_source_uploads u where u.id=(select r.upload_id
      from public.weekly_source_upload_rows r where r.id=v_review.upload_row_id);
    if not exists(select 1 from public.weekly_source_final_revisions f
      where f.source_cycle_id=v_cycle.id and f.report_scope_id is not distinct from v_upload.report_scope_id and f.state='CURRENT')
      and exists(select 1 from public.weekly_source_projection_publications p
        where p.source_cycle_id=v_cycle.id and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT'
          and (p.authority_scope_version>2147483647 or coalesce(p.projection_generation,0)>2147483647)) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003'; end if;
    -- The unique CURRENT authority in the original declared report scope,
    -- not an arbitrary newest row or a different client/report's upload.
    with final_authority as (
      select f.id,f.upload_id from public.weekly_source_final_revisions f
      where f.source_cycle_id=v_cycle.id and f.report_scope_id is not distinct from v_upload.report_scope_id and f.state='CURRENT'
    ), candidates as (
      select s.upload_row_id,s.row_resolution_id from final_authority f
      cross join lateral private.weekly_source_manual_review_final_rows_v2(f.id) s
      where s.work_event_id=v_review.work_event_id and s.candidate_id=v_review.candidate_id
        and s.client_id=v_review.client_id and s.contract_id=v_review.contract_id and s.work_date=v_review.work_date
      union all
      select r.upload_row_id,r.id from public.weekly_source_projection_publications p
      join public.weekly_source_upload_rows u on u.upload_id=p.upload_id
      join public.weekly_source_row_resolutions r on r.upload_row_id=u.id
        and r.generation=coalesce(p.projection_generation,p.authority_scope_version::integer)
      where not exists(select 1 from final_authority) and p.source_cycle_id=v_cycle.id
        and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT'
        and r.mapping_state='RESOLVED' and r.work_event_id=v_review.work_event_id
        and r.candidate_id=v_review.candidate_id and r.client_id=v_review.client_id
        and r.contract_id=v_review.contract_id and u.work_date=v_review.work_date
    ) select count(*),(array_agg(c.upload_row_id))[1] into v_count,v_source_row_id
      from candidates c join public.weekly_source_upload_rows u on u.id=c.upload_row_id
      where encode(u.normalised_row_hash,'hex')=v_expected_hash;
    if v_count<>1 then raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001'; end if;
    v_source:=private.weekly_source_manual_review_source_v2(v_source_row_id);
    if v_source is null or v_source->>'work_event_id' is distinct from v_review.work_event_id::text
       or v_source->>'candidate_id' is distinct from v_review.candidate_id::text
       or v_source->>'client_id' is distinct from v_review.client_id::text
       or v_source->>'contract_id' is distinct from v_review.contract_id::text then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_SOURCE_STALE' using errcode='40001'; end if;
    end if;
  else
    if v_approval_id is null or v_generation_id is null or p_request->>'expected_current_row_hash' is not null then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='22023'; end if;
    select a.* into v_approval from public.weekly_exceptional_payment_approvals a where a.id=v_approval_id;
    if not found or v_approval.work_event_id is distinct from v_review.work_event_id
       or v_approval.candidate_id is distinct from v_review.candidate_id or v_approval.client_id is distinct from v_review.client_id
       or v_approval.contract_id is distinct from v_review.contract_id or v_approval.week_ending is distinct from v_week then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000'; end if;
    select f.* into strict v_family from public.weekly_exceptional_pay_target_families f where f.id=v_approval.pay_target_family_id;
    select c.* into v_anchor from private.weekly_source_manual_review_commands c where c.review_id=v_review.id
      and (c.opening_final_revision_id is not null or c.opening_projection_publication_id is not null);
    if found then
      if (v_anchor.opening_target_family_id is not null and v_anchor.opening_target_family_id is distinct from v_family.id)
        or not exists(select 1 from public.weekly_exceptional_pay_family_events e
          where e.family_id=v_family.id and e.durable_work_event_id=v_review.work_event_id
            and e.evidence_approval_id=v_approval.id and e.event_sequence>v_anchor.opening_event_sequence) then
        raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000'; end if;
    elsif v_approval.approved_at_utc<v_review.opened_at_utc then
      -- Legacy reviews still allow an explicit new decision, not adoption of
      -- an earlier approval. No opening ancestry is fabricated for them.
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000';
    end if;
    v_scope:=private.weekly_source_pay_query_scope_v1(v_family.root_timesheet_id);
    if v_scope is null or v_scope->>'target_family_id' is distinct from v_family.id::text then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000'; end if;
    v_local:=private.weekly_source_pay_query_local_receipt_v1(v_family.root_timesheet_id,v_approval_id,v_generation_id);
    v_c1:=private.weekly_source_pay_query_c1_receipt_v1(v_family.root_timesheet_id,v_approval_id,v_generation_id);
    v_next:=private.weekly_source_pay_query_next_receipt_v1(v_family.root_timesheet_id,v_approval_id,v_generation_id);
    v_count:=(v_local is not null)::integer+(v_c1 is not null)::integer+(v_next is not null)::integer;
    select count(*) into v_local_count from private.weekly_source_local_protected_decision_receipts l
      join public.weekly_exceptional_c1_publication_requests r on r.id=l.publication_request_id
      where l.generation_id=v_generation_id or r.orchestration_run_id=v_approval.creation_orchestration_run_id;
    select count(*),count(*) filter (where
      (v_local is not null and r.id::text=v_local->>'publication_request_id' and r.state='RETIRED')
      or (v_c1 is not null and r.id::text=v_c1->>'publication_request_id' and r.state='PUBLISHED'))
      into v_c1_count,v_audit_count from public.weekly_exceptional_c1_publication_requests r
      where r.generation_id=v_generation_id or r.orchestration_run_id=v_approval.creation_orchestration_run_id;
    select count(*) into v_next_count from private.bpay_next_protected_source_receipt r
      where r.approval_id=v_approval_id or r.generation_id=v_generation_id
        or r.orchestration_run_id=v_approval.creation_orchestration_run_id;
    if v_count<>1
       or (v_local is not null and (v_local_count<>1 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
       or (v_c1 is not null and (v_local_count<>0 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
       or (v_next is not null and (v_local_count<>0 or v_c1_count<>0 or v_next_count<>1)) then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_PROTECTED_PAY_REQUIRED' using errcode='55000'; end if;
    v_receipt:=coalesce(v_local,v_c1,v_next);
  end if;
  update private.weekly_source_manual_reviews set state='RESOLVED',resolved_by_user_id=v_actor,
    resolved_at_utc=statement_timestamp(),resolution_kind=v_kind where id=v_review.id;
  perform public._audit_insert('weekly_source_manual_review',v_review.id::text,'WEEKLY_SOURCE_MANUAL_REVIEW_RESOLVED',null,
    jsonb_build_object('resolution_kind',v_kind,'work_event_id',v_review.work_event_id,
      'accepted_source_row_id',v_source_row_id,'accepted_receipt',v_receipt),
    'Office resolved imported-shift review',v_actor);
  v_result:=jsonb_build_object('ok',true,'review_id',v_review.id,'resolution_kind',v_kind,'idempotent_replay',false);
  insert into private.weekly_source_manual_review_commands(command_id,command_kind,actor_user_id,request_sha256,
    review_id,source_cycle_id,client_id,week_ending_date,result_json)
  values(v_command_id,'RESOLVE',v_actor,v_hash,v_review.id,v_cycle.id,v_review.client_id,v_week,v_result);
  perform private.bpay_next_source_query_refresh_v1(v_review.contract_id,v_review.work_event_id);
  return v_result;
end;
$function$;

create or replace function private.weekly_source_manual_reviews_finalised_v1(p_final_revision_id uuid,p_actor_user_id uuid)
returns integer language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_final public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_review private.weekly_source_manual_reviews%rowtype;
  v_anchor private.weekly_source_manual_review_commands%rowtype;
  v_opening_upload_id uuid;
  v_count integer:=0;
begin
  perform private.weekly_source_query_require_service_v1();
  select f.* into strict v_final from public.weekly_source_final_revisions f where f.id=p_final_revision_id;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_final.source_cycle_id;
  if p_actor_user_id is null or v_final.state is distinct from 'CURRENT' or v_final.finalised_by_user_id is distinct from p_actor_user_id
     or v_final.finalised_at_utc is null or not isfinite(v_final.finalised_at_utc) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_FINAL_AUTHORITY_INVALID' using errcode='55000'; end if;
  perform private.weekly_source_pay_query_admit_v2();
  -- The sole reviewed Final/Correct mutation callers invoke this private hook
  -- after their successful CURRENT CAS, never on preparation or replay. Do not
  -- compare preparation timestamps, nor xmin to the top xid: the real owner
  -- may write in a PL/pgSQL exception subtransaction. Validate its actual CAS
  -- result here; caller closure and genuine-owner native proof are mandatory.
  if (v_final.authority_scope_kind='CYCLE' and v_cycle.current_final_revision_id is distinct from v_final.id)
    or (v_final.authority_scope_kind='NHSP_REPORT_SCOPE' and not exists(
      select 1 from public.weekly_source_report_scopes s where s.id=v_final.report_scope_id
        and s.source_cycle_id=v_cycle.id and s.current_final_revision_id=v_final.id)) then
    raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_FINAL_AUTHORITY_INVALID' using errcode='55000'; end if;
  for v_review in
    with contained as materialized (
      select distinct s.work_event_id,s.candidate_id,s.client_id,s.contract_id,s.work_date
      from private.weekly_source_manual_review_final_rows_v2(v_final.id) s
    )
    select r.* from contained s join private.weekly_source_manual_reviews r
      on r.source_group_id=v_cycle.source_group_id and r.work_event_id=s.work_event_id and r.state='OPEN'
      and r.candidate_id=s.candidate_id and r.client_id=s.client_id
      and r.contract_id=s.contract_id and r.work_date=s.work_date
    order by r.id for update of r
  loop
    select c.* into v_anchor from private.weekly_source_manual_review_commands c where c.review_id=v_review.id
      and (c.opening_final_revision_id is not null or c.opening_projection_publication_id is not null);
    -- A legacy review without captured opening provenance needs an explicit
    -- Office remedy. Never manufacture an anchor using today's report.
    if not found or v_anchor.opening_final_revision_id=p_final_revision_id
       or not exists(select 1 from public.weekly_source_client_manifests m where m.final_revision_id=v_final.id
         and m.source_group_id=v_cycle.source_group_id and m.source_cycle_id=v_cycle.id and m.client_id=v_review.client_id) then continue; end if;
    -- A new Final for the report already queried is not a new imported report.
    -- Resolve the immutable opening authority, not preparation timestamps or
    -- the currently displayed row; rebuilt projections of that upload also
    -- remain the same report. Missing opening evidence is never a remedy.
    if v_anchor.opening_final_revision_id is not null then
      select f.upload_id into v_opening_upload_id from public.weekly_source_final_revisions f
        where f.id=v_anchor.opening_final_revision_id;
    else
      select p.upload_id into v_opening_upload_id from public.weekly_source_projection_publications p
        where p.id=v_anchor.opening_projection_publication_id;
    end if;
    if v_opening_upload_id is null or v_opening_upload_id=v_final.upload_id then continue; end if;
    perform private.weekly_source_office_authority_v1(p_actor_user_id,'ACCEPT_SYSTEM_HOURS',
      v_cycle.source_group_id,v_review.client_id,v_cycle.finalisation_week_ending);
    update private.weekly_source_manual_reviews set state='RESOLVED',resolved_by_user_id=p_actor_user_id,
      resolved_at_utc=statement_timestamp(),resolution_kind='OFFICE_ACCEPTED_SOURCE' where id=v_review.id;
    perform public._audit_insert('weekly_source_manual_review',v_review.id::text,'WEEKLY_SOURCE_MANUAL_REVIEW_RESOLVED',null,
      jsonb_build_object('resolution_kind','OFFICE_ACCEPTED_SOURCE','work_event_id',v_review.work_event_id,
        'final_revision_id',v_final.id,'reason','A later finalised report contains this shift'),
      'Later finalised source report resolved imported-shift review',p_actor_user_id);
    perform private.bpay_next_source_query_refresh_v1(v_review.contract_id,v_review.work_event_id);
    v_count:=v_count+1;
  end loop;
  return v_count;
end;
$function$;

create or replace function public.weekly_source_query_accept_system_hours_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_requested uuid[];
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_count integer:=0;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','source_cycle_id','projection_publication_id','incident_ids'))
     or pg_catalog.jsonb_typeof(p_request->'incident_ids')<>'array' then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    select pg_catalog.array_agg(value::uuid order by value::uuid) into v_requested
    from pg_catalog.jsonb_array_elements_text(p_request->'incident_ids');
  exception when others then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end;
  if coalesce(pg_catalog.array_length(v_requested,1),0)=0 then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_INVALID' using errcode='22023';
  end if;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date
  );
  perform private.weekly_source_pay_query_admit_v2();
  for v_incident in
    select incident.* from public.weekly_discrepancy_incidents incident
    where incident.id=any(v_requested) order by incident.id for update
  loop
    v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
    perform private.weekly_source_office_authority_v1(
      v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
      (v_guard->>'finalisation_week_ending')::date);
    if v_incident.source_cycle_id<>v_cycle_id or v_incident.state<>'OPEN' then
      raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_comparison from public.weekly_issue_comparison_revisions
    where id=v_incident.current_comparison_revision_id;
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
        candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
        manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
        resolved_at_utc=pg_catalog.transaction_timestamp(),
        resolution_kind='OFFICE_ACCEPTED_SYSTEM_HOURS'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications set operational_state='RESOLVED',
      resolved_at_utc=pg_catalog.transaction_timestamp()
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'OFFICE_ACCEPTED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'RESOLVED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED_RESOLVED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    perform private.bpay_next_source_query_refresh_v1(v_comparison.contract_id,v_incident.work_event_id);
    v_count:=v_count+1;
  end loop;
  if v_count<>pg_catalog.array_length(v_requested,1) then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
  end if;
  perform public._audit_insert(
    'weekly_source_query_resolution',v_publication_id::text,
    'WEEKLY_SOURCE_SYSTEM_HOURS_ACCEPTED',null,
    pg_catalog.jsonb_build_object('incident_count',v_count),
    'Office accepted system hours',v_actor
  );
  return pg_catalog.jsonb_build_object('ok',true,'resolved_count',v_count);
end;
$function$;

create or replace function public.weekly_source_manager_review_respond_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_credential_hash bytea;
  v_control_ticket uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_review_batch_version integer;
  v_response_fingerprint bytea;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_receipt public.weekly_manager_route_receipts%rowtype;
  v_batch public.weekly_manager_review_batches%rowtype;
  v_response jsonb;
  v_item public.weekly_manager_review_items%rowtype;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_kind text;
  v_expected bytea;
  v_response_hash bytea;
  v_event_id uuid;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_summary text;
  v_answered integer:=0;
  v_notices integer:=0;
  v_intended_start timestamp without time zone;
  v_intended_end timestamp without time zone;
  v_intended_break integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'credential_hash','control_plane_ticket_id',
                 'request_idempotency_key','review_batch_version','response_fingerprint','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_credential_hash:=private.weekly_source_query_hex32_v1(
      p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
    );
    v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_review_batch_version:=(p_request->>'review_batch_version')::integer;
    v_response_fingerprint:=private.weekly_source_query_hex32_v1(
      p_request->>'response_fingerprint','WEEKLY_SOURCE_MANAGER_RESPONSE_FINGERPRINT_INVALID'
    );
    v_responses:=(select pg_catalog.jsonb_agg(value order by (value->>'review_item_id')::uuid)
                 from pg_catalog.jsonb_array_elements(p_request->'responses'));
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RESPONSE_REQUEST_V1',pg_catalog.jsonb_build_object(
      'control_plane_ticket_id',v_control_ticket,
      'review_batch_version',v_review_batch_version,
      'response_fingerprint',pg_catalog.encode(v_response_fingerprint,'hex'),
      'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');
  -- Exact response replay wins before credential expiry or mutable fact checks.
  select pg_catalog.count(*),min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='MANAGER_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select receipt.* into strict v_receipt
  from public.weekly_manager_route_receipts receipt
  join public.weekly_manager_review_batches batch on batch.id=receipt.review_batch_id
  where receipt.credential_hash=v_credential_hash
    and receipt.control_plane_ticket_id=v_control_ticket
  for update of receipt;
  select * into strict v_batch from public.weekly_manager_review_batches
  where id=v_receipt.review_batch_id for update;
  if v_receipt.state<>'ACTIVE' or v_batch.state<>'ACTIVE'
     or pg_catalog.transaction_timestamp()>=v_receipt.expires_at_utc then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
  end if;
  if v_review_batch_version<>v_batch.credential_generation
     or v_response_fingerprint<>v_batch.sent_content_hash then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_BATCH_STALE' using errcode='40001';
  end if;
  for v_response in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'review_item_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_response)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                 where key not in (
                   'review_item_id','incident_episode','current_fact_version','response_kind',
                   'intended_start','intended_end','intended_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_item from public.weekly_manager_review_items
      where id=(v_response->>'review_item_id')::uuid and review_batch_id=v_batch.id
        and response_state='UNANSWERED' for update;
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=v_item.incident_id for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_response->>'current_fact_version','WEEKLY_SOURCE_MANAGER_FACT_VERSION_INVALID'
    );
    if v_incident.state<>'OPEN' or v_incident.episode_number<>(v_response->>'incident_episode')::integer
       or v_item.incident_episode<>v_incident.episode_number
       or private.weekly_source_covered_hours_incident_v1(v_incident.id)
       or v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_response->>'response_kind','')));
    if v_kind not in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK','MANAGER_CORRECTED_SOURCE')
       or (v_kind='SYSTEM_CORRECT' and v_comparison.issue_family<>'SOURCE_HOURS_DIFFER')
       or (v_kind='CANDIDATE_DID_NOT_WORK' and v_comparison.issue_family='SOURCE_HOURS_DIFFER') then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_KIND_INVALID' using errcode='22023';
    end if;
    v_intended_start:=null;
    v_intended_end:=null;
    v_intended_break:=null;
    if v_kind='MANAGER_CORRECTED_SOURCE' then
      begin
        if coalesce(v_response->>'intended_start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_response->>'intended_end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception 'invalid time';
        end if;
        v_intended_break:=(v_response->>'intended_break_minutes')::integer;
        v_intended_start:=v_work.work_date+(v_response->>'intended_start')::time;
        v_intended_end:=v_work.work_date+(v_response->>'intended_end')::time;
        -- 14 section 5.3.4 for the source side: "If start equals end, the row
        -- blocks because neither accepted profile supplies an explicit 24-hour
        -- marker."  The overnight roll below is for a genuine end-before-start
        -- shift; start equal to end is not a 24-hour shift and must not be
        -- recorded as one, now that Office reads this value.
        if v_intended_end=v_intended_start then raise exception 'equal start and end'; end if;
        if v_intended_end<v_intended_start then v_intended_end:=v_intended_end+interval '1 day'; end if;
        if v_intended_break<0 or v_intended_break>720
           or v_intended_end<=v_intended_start then raise exception 'invalid hours'; end if;
      exception when others then
        raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
      end;
    elsif v_response ? 'intended_start' or v_response ? 'intended_end'
       or v_response ? 'intended_break_minutes' then
      raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_MANAGER_RESPONSE_ITEM_V1',pg_catalog.jsonb_build_object(
        'review_item_id',v_item.id,'incident_id',v_incident.id,
        'incident_episode',v_incident.episode_number,
        'current_fact_version',pg_catalog.encode(v_expected,'hex'),
        'response_kind',v_kind,
        'intended_start_at_local',v_intended_start,
        'intended_end_at_local',v_intended_end,
        'intended_break_minutes',v_intended_break
      )
    );
    update public.weekly_manager_review_items
    set response_state='ANSWERED',response_kind=v_kind,
        intended_start_at_local=v_intended_start,
        intended_end_at_local=v_intended_end,
        intended_break_minutes=v_intended_break,
        response_fingerprint=v_response_hash,answered_at_utc=pg_catalog.transaction_timestamp()
    where id=v_item.id;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
      v_expected,'MANAGER_RESPONDED','MANAGER',pg_catalog.jsonb_build_object(
        'response_kind',v_kind,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses),
        'review_item_id',v_item.id
      ),'MANAGER_RESPONSE:'||v_request_key::text||':'||v_item.id::text
    ) returning id into v_event_id;
    -- A manager saying the source is right does not settle a candidate's
    -- explicit assertion that their own hours are right. Keep the ordinary
    -- open-incident authorisation hold until Office accepts current source
    -- hours or protects pay for this existing work identity.
    if v_kind in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK')
       and not (v_incident.candidate_action_state='RESPONDED'
         and exists (
           select 1 from public.weekly_discrepancy_events candidate_answer
           where candidate_answer.incident_id=v_incident.id
             and candidate_answer.issue_episode=v_incident.episode_number
             and candidate_answer.event_kind='CANDIDATE_RESPONDED'
             and candidate_answer.bounded_payload_json->>'choice'='CANDIDATE_CORRECT'
         )) then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state=case when candidate_action_state='NOT_ASKED' then 'NOT_REQUIRED' else candidate_action_state end,
          manager_potential_state='NOT_REQUIRED',manager_action_state='RESPONDED',
          waiting_source_state='NOT_WAITING',resolved_at_utc=pg_catalog.transaction_timestamp(),
          resolution_kind='MANAGER_CONFIRMED_SYSTEM_HOURS'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
        v_expected,'RESOLVED','MANAGER',
        pg_catalog.jsonb_build_object('resolution','MANAGER_CONFIRMED_SYSTEM_HOURS'),
        'MANAGER_CONFIRMED:'||v_event_id::text
      );
    elsif v_kind='MANAGER_CORRECTED_SOURCE' then
      update public.weekly_discrepancy_incidents
      set reconciliation_state='WAITING_FOR_SOURCE',manager_action_state='RESPONDED',
          waiting_source_state='WAITING_REIMPORT'
      where id=v_incident.id;
    else
      update public.weekly_discrepancy_incidents
      set reconciliation_state='UNRESOLVED',manager_action_state='RESPONDED',
          waiting_source_state='NOT_WAITING'
      where id=v_incident.id;
    end if;
    select * into strict v_client from public.clients where id=v_incident.client_id;
    select * into strict v_candidate from public.candidates where id=v_incident.candidate_id;
    select * into strict v_group from public.weekly_source_groups where id=v_incident.source_group_id;
    v_summary:=case
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
        then 'Shift missing or not yet authorised'
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
        then 'Shift not shown in system'
      else 'System hours reviewed'
    end;
    -- 04A section 4: "A corrected/added/finalised response records the manager's
    -- intended start, end and break and waits for a later accepted source
    -- upload; it never creates source hours."  The record lives on
    -- weekly_manager_review_items and is deliberately left out of every
    -- calculation.  It is repeated here, as plain non-financial text, so the
    -- Office notice this response raises actually carries the figures the
    -- manager gave; without them Office cannot compare the manager's stated
    -- correction with the later source, which is what section 6 expects it to
    -- do.  These keys are hours only: the fan-out owner rejects any pay,
    -- charge, rate, margin, VAT, invoice or banking key.
    v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
      v_incident.id,v_event_id,
      case when v_kind='MANAGER_CORRECTED_SOURCE'
        then 'WEEKLY_MANAGER_SOURCE_CORRECTED' else 'WEEKLY_MANAGER_SYSTEM_CONFIRMED' end,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'candidate_name',coalesce(nullif(v_candidate.display_name,''),
          pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
        'client_name',v_client.name,'work_date',v_work.work_date,
        'issue_summary',v_summary,'manager_response',v_kind,
        'manager_intended_start',to_char(v_intended_start,'YYYY-MM-DD HH24:MI'),
        'manager_intended_end',to_char(v_intended_end,'YYYY-MM-DD HH24:MI'),
        'manager_intended_break_minutes',v_intended_break
      ))
    );
    perform private.bpay_next_source_query_refresh_v1(v_comparison.contract_id,v_incident.work_event_id);
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.weekly_manager_review_items
                where review_batch_id=v_batch.id and response_state='UNANSWERED') then
    update public.weekly_manager_review_batches set state='COMPLETE',
      completed_at_utc=pg_catalog.transaction_timestamp() where id=v_batch.id;
    update public.weekly_manager_route_receipts set state='COMPLETE' where id=v_receipt.id;
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'office_notification_count',v_notices,
    'remaining_count',(select pg_catalog.count(*) from public.weekly_manager_review_items
                       where review_batch_id=v_batch.id and response_state='UNANSWERED')
  );
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
end;
$function$;

create or replace function public.weekly_source_candidate_response_submit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_candidate_id uuid;
  v_generation_id uuid;
  v_publication_id uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_item jsonb;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_membership public.weekly_candidate_outreach_memberships%rowtype;
  v_choice text;
  v_expected bytea;
  v_current_timesheet_hash bytea;
  v_response_hash bytea;
  v_draft public.weekly_candidate_response_drafts%rowtype;
  v_draft_version integer;
  v_event_id uuid;
  v_answered integer:=0;
  v_resolved integer:=0;
  v_notices integer:=0;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_summary text;
  v_recipient_generation_id uuid;
  v_recipient_route_id uuid;
  v_due_ids uuid[];
  v_intent_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'candidate_id','candidate_generation_id','projection_publication_id',
                 'request_idempotency_key','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end if;
  begin
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_responses:=(
      select pg_catalog.jsonb_agg(value order by (value->>'incident_id')::uuid)
      from pg_catalog.jsonb_array_elements(p_request->'responses')
    );
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_CANDIDATE_RESPONSE_REQUEST_V1',
    pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'candidate_generation_id',v_generation_id,
      'projection_publication_id',v_publication_id,'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');

  -- Exact durable replay is deliberately checked before any mutable generation,
  -- publication, Timesheet or incident test.
  select pg_catalog.count(*),
         min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='CANDIDATE_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;

  perform private.weekly_source_pay_query_admit_v2();
  select * into strict v_generation
  from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;
  if v_generation.state<>'ACTIVE' or v_generation.request_kind<>'CHECK_HOURS'
     or v_generation.candidate_id<>v_candidate_id
     or pg_catalog.transaction_timestamp()>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cohort from public.weekly_candidate_cohorts
  where id=v_generation.candidate_cohort_id for update;
  if v_cohort.current_generation_id<>v_generation.id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_candidate from public.candidates where id=v_candidate_id;
  select coalesce(pg_catalog.max(draft_version),0)+1 into v_draft_version
  from public.weekly_candidate_response_drafts where candidate_generation_id=v_generation.id;
  insert into public.weekly_candidate_response_drafts(
    candidate_generation_id,candidate_id,draft_version,current_projection_publication_id,
    state,draft_hash,submitted_at_utc
  ) values (
    v_generation.id,v_candidate_id,v_draft_version,v_publication_id,
    'SUBMITTED',v_semantic_hash,pg_catalog.transaction_timestamp()
  ) returning * into v_draft;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'incident_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_item) key
                 where key not in (
                   'incident_id','expected_comparison_fingerprint','expected_timesheet_hash',
                   'choice','corrected_start_at_local','corrected_end_at_local','corrected_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=(v_item->>'incident_id')::uuid for update;
      select * into strict v_membership from public.weekly_candidate_outreach_memberships
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id
        and state='ACTIONABLE' for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    if v_incident.state<>'OPEN' or v_incident.candidate_id<>v_candidate_id
       or v_incident.source_cycle_id<>v_cycle.id
       or private.weekly_source_covered_hours_incident_v1(v_incident.id) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_item->>'expected_comparison_fingerprint',
      'WEEKLY_SOURCE_CANDIDATE_RESPONSE_FINGERPRINT_INVALID'
    );
    if v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_current_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(
      v_comparison.candidate_timesheet_id
    );
    if v_current_timesheet_hash is null
       or v_current_timesheet_hash<>private.weekly_source_query_hex32_v1(
         v_item->>'expected_timesheet_hash','WEEKLY_SOURCE_CANDIDATE_TIMESHEET_HASH_INVALID'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
    end if;
    v_choice:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_item->>'choice','')));
    if v_choice not in ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT') then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_CHOICE_INVALID' using errcode='22023';
    end if;
    if (
      nullif(v_item->>'corrected_start_at_local','') is null
      or nullif(v_item->>'corrected_end_at_local','') is null
      or nullif(v_item->>'corrected_break_minutes','') is null
    ) and not (
      nullif(v_item->>'corrected_start_at_local','') is null
      and nullif(v_item->>'corrected_end_at_local','') is null
      and nullif(v_item->>'corrected_break_minutes','') is null
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_NOT_ALLOWED' using errcode='22023';
    end if;
    if v_choice='NEITHER_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_WRONG'
       and nullif(v_item->>'corrected_start_at_local','') is null
       and v_comparison.source_presence<>'ABSENT' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if nullif(v_item->>'corrected_start_at_local','') is not null and (
      nullif(v_item->>'corrected_break_minutes','')::integer<0
      or nullif(v_item->>'corrected_end_at_local','')::timestamp
         <=nullif(v_item->>'corrected_start_at_local','')::timestamp
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_query_response_fingerprint_v1(
      v_incident.id,v_comparison.id,v_candidate_id,v_draft_version,v_request_key,
      private.weekly_source_sha256_jsonb_v1('WEEKLY_CANDIDATE_RESPONSE_ITEM_V1',v_item)
    );
    insert into public.weekly_candidate_response_draft_items(
      response_draft_id,incident_id,comparison_revision_id,choice,
      corrected_start_at_local,corrected_end_at_local,corrected_break_minutes,
      response_fingerprint
    ) values (
      v_draft.id,v_incident.id,v_comparison.id,v_choice,
      nullif(v_item->>'corrected_start_at_local','')::timestamp,
      nullif(v_item->>'corrected_end_at_local','')::timestamp,
      nullif(v_item->>'corrected_break_minutes','')::integer,v_response_hash
    );
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'CANDIDATE_RESPONDED','CANDIDATE',
      pg_catalog.jsonb_build_object(
        'choice',v_choice,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,
        'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses)
      ),'CANDIDATE_RESPONSE:'||v_request_key::text||':'||v_incident.id::text
    ) returning id into v_event_id;
    if v_choice='CANDIDATE_WRONG' then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state='RESPONDED',manager_potential_state='NOT_REQUIRED',
          manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
          resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind='CANDIDATE_CORRECTED'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
      where incident_id=v_incident.id and response_state='UNANSWERED';
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication_id,
        v_comparison.material_comparison_fingerprint,'RESOLVED','CANDIDATE',
        pg_catalog.jsonb_build_object('resolution','CANDIDATE_CORRECTED'),
        'CANDIDATE_CORRECTED:'||v_event_id::text
      );
      v_resolved:=v_resolved+1;
    else
      v_policy:=private.weekly_source_query_incident_policy_v1(v_incident.id);
      update public.weekly_discrepancy_incidents
      set candidate_action_state='RESPONDED',
          manager_potential_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'AVAILABLE'
            else 'NOT_AVAILABLE'
          end,
          manager_action_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'DUE'
            else 'NOT_REQUIRED'
          end,
          reconciliation_state='WAITING_FOR_SOURCE'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='ANSWERED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
      select * into strict v_client from public.clients where id=v_incident.client_id;
      v_summary:=case
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
          then 'Shift missing or not yet authorised'
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
          then 'Shift missing from system hours'
        else 'System hours need review'
      end;
      v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
        v_incident.id,v_event_id,'WEEKLY_CANDIDATE_SOURCE_DISPUTED',
        pg_catalog.jsonb_build_object(
          'candidate_name',coalesce(nullif(v_candidate.display_name,''),
            pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
          'client_name',v_client.name,'work_date',v_work.work_date,
          'issue_summary',v_summary,'candidate_choice',v_choice
        )
      );
    end if;
    perform private.bpay_next_source_query_refresh_v1(v_comparison.contract_id,v_incident.work_event_id);
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(
    select 1 from public.weekly_candidate_outreach_memberships
    where candidate_generation_id=v_generation.id and state='ACTIONABLE'
  ) then
    update public.weekly_candidate_outreach_generations set state='COMPLETE'
    where id=v_generation.id;
    update public.weekly_message_intents set state='RETIRED'
    where candidate_generation_id=v_generation.id and state in ('DUE','RENDERED');

    select generation.id,generation.recipient_route_id
      into v_recipient_generation_id,v_recipient_route_id
    from public.weekly_manager_recipient_memberships membership
    join public.weekly_manager_recipient_generations generation
      on generation.id=membership.recipient_generation_id and generation.state='ACTIVE'
    join public.weekly_manager_recipient_routes route
      on route.id=generation.recipient_route_id
     and route.current_generation_id=generation.id
    where membership.candidate_cohort_id=v_cohort.id
    order by generation.generation_number desc
    limit 1
    for update of generation;
    if v_recipient_generation_id is not null
       and not exists(
         select 1
         from public.weekly_manager_recipient_memberships route_membership
         join public.weekly_route_activations activation
           on activation.id=route_membership.route_activation_id
          and activation.route_mode='CANDIDATE_FIRST'
         join public.weekly_candidate_cohorts candidate_cohort
           on candidate_cohort.id=route_membership.candidate_cohort_id
         join public.weekly_candidate_outreach_memberships candidate_membership
           on candidate_membership.candidate_generation_id=candidate_cohort.current_generation_id
          and candidate_membership.state='ACTIONABLE'
         where route_membership.recipient_generation_id=v_recipient_generation_id
       ) and exists(
      -- 04A section 6.1: only a row that is unanswered AND unsent becomes due.
      -- A row an accepted batch on this route already owns is not work, so it
      -- must not raise an EARLY_ALL event whose render would then be empty.
      select 1 from public.weekly_manager_recipient_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
        and not private.weekly_source_query_manager_row_owned_v1(
          v_recipient_route_id,incident.id,incident.episode_number
        )
    ) then
      insert into public.weekly_manager_cohort_due_events(
        recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
        due_at_utc,state,trigger_hash
      ) select distinct
        v_recipient_generation_id,membership.candidate_cohort_id,'EARLY_ALL',
        candidate_generation.started_at_utc,pg_catalog.transaction_timestamp(),'PENDING',
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_MANAGER_COHORT_DUE_V1',pg_catalog.jsonb_build_object(
            'recipient_generation_id',v_recipient_generation_id,
            'candidate_cohort_id',membership.candidate_cohort_id,
            'event_kind','EARLY_ALL',
            'candidate_generation_id',candidate_generation.id
          )
        )
      from public.weekly_manager_recipient_memberships membership
      join public.weekly_route_activations activation
        on activation.id=membership.route_activation_id
       and activation.route_mode='CANDIDATE_FIRST'
      join public.weekly_candidate_cohorts candidate_cohort
        on candidate_cohort.id=membership.candidate_cohort_id
      join public.weekly_candidate_outreach_generations candidate_generation
        on candidate_generation.id=candidate_cohort.current_generation_id
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
      on conflict (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash) do nothing;
      select pg_catalog.array_agg(id order by id) into v_due_ids
      from public.weekly_manager_cohort_due_events
      where recipient_generation_id=v_recipient_generation_id
        and event_kind='EARLY_ALL' and state='PENDING';
      if coalesce(pg_catalog.array_length(v_due_ids,1),0)>0
         and not exists(
           select 1 from public.weekly_message_intents intent
           where intent.recipient_generation_id=v_recipient_generation_id
             and intent.state in ('DUE','RENDERED')
         ) then
        v_intent_id:=private.weekly_source_query_manager_intent_v1(
          (select recipient_route_id from public.weekly_manager_recipient_generations where id=v_recipient_generation_id),
          v_recipient_generation_id,v_due_ids,'MANAGER_EARLY_ALL',1,
          pg_catalog.transaction_timestamp()
        );
        update public.weekly_manager_cohort_due_events set state='CONSUMED'
        where id=any(v_due_ids);
      end if;
    end if;
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'resolved_count',v_resolved,
    'office_notification_count',v_notices,'manager_message_intent_id',v_intent_id
  );
end;
$function$;

create or replace function public.weekly_source_query_sync_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_group_id uuid;
  v_upload_id uuid;
  v_scope_kind text;
  v_report_scope_id uuid;
  v_current_report_client_id uuid;
  v_final_authority_kind text;
  v_omission_meaning text;
  v_issue jsonb;
  v_event public.weekly_work_events%rowtype;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_policy jsonb;
  v_issue_family text;
  v_presence text;
  v_episode integer;
  v_revision integer;
  v_fingerprint bytea;
  v_expected bytea;
  v_contract_id uuid;
  v_timesheet_id uuid;
  v_timesheet_revision integer;
  v_candidate_shift_hash bytea;
  v_source_row_id uuid;
  v_source_link_id uuid;
  v_new_count integer:=0;
  v_changed_count integer:=0;
  v_unchanged_count integer:=0;
  v_resolved_count integer:=0;
  v_seen_work_events uuid[]:='{}'::uuid[];
  v_new_cohort_keys text[]:='{}'::text[];
  v_cohort_key text;
  v_is_new boolean;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','source_cycle_id','projection_publication_id','issues')
     ) or pg_catalog.jsonb_typeof(p_request->'issues')<>'array' then
    raise exception 'WEEKLY_SOURCE_QUERY_SYNC_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_QUERY_SYNC_REQUEST_INVALID' using errcode='22023';
  end;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  v_group_id:=(v_guard->>'source_group_id')::uuid;
  v_upload_id:=(v_guard->>'upload_id')::uuid;
  v_scope_kind:=v_guard->>'authority_scope_kind';
  v_report_scope_id:=nullif(v_guard->>'report_scope_id','')::uuid;
  select profile.final_authority_kind,profile.omission_meaning
  into strict v_final_authority_kind,v_omission_meaning
  from public.weekly_source_uploads upload
  join public.weekly_source_format_profiles profile
    on profile.id=upload.source_format_profile_id
  where upload.id=v_upload_id;
  if v_scope_kind='NHSP_REPORT_SCOPE' then
    select client_id into strict v_current_report_client_id
    from public.weekly_source_report_scopes
    where id=v_report_scope_id and source_cycle_id=v_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group_id,null,(v_guard->>'finalisation_week_ending')::date
  );
  perform private.weekly_source_pay_query_admit_v2();
  perform 1 from public.weekly_source_cycles where id=v_cycle_id for update;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'RECHECK_SOURCE',v_group_id,null,(v_guard->>'finalisation_week_ending')::date);

  for v_issue in
    select value
    from pg_catalog.jsonb_array_elements(p_request->'issues')
    order by value->>'work_event_id'
  loop
    if pg_catalog.jsonb_typeof(v_issue)<>'object'
       or exists(
         select 1 from pg_catalog.jsonb_object_keys(v_issue) key
         where key not in (
           'work_event_id','candidate_timesheet_id','candidate_timesheet_revision',
           'candidate_shift_fingerprint','source_row_id','source_work_event_link_id',
           'contract_id','issue_family','source_presence','candidate_start_at_local',
           'candidate_end_at_local','candidate_break_minutes','system_start_at_local',
           'system_end_at_local','system_break_minutes','expected_material_comparison_fingerprint'
         )
       ) then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_event from public.weekly_work_events
      where id=(v_issue->>'work_event_id')::uuid
        and first_source_group_id=v_group_id
        and (nullif(v_guard->>'scope_client_id','') is null
          or client_id=(v_guard->>'scope_client_id')::uuid);
      v_contract_id:=nullif(v_issue->>'contract_id','')::uuid;
      v_timesheet_id:=nullif(v_issue->>'candidate_timesheet_id','')::uuid;
      v_timesheet_revision:=nullif(v_issue->>'candidate_timesheet_revision','')::integer;
      v_candidate_shift_hash:=case when nullif(v_issue->>'candidate_shift_fingerprint','') is null
        then null else private.weekly_source_query_hex32_v1(
          v_issue->>'candidate_shift_fingerprint','WEEKLY_SOURCE_CANDIDATE_SHIFT_HASH_INVALID'
        ) end;
      v_source_row_id:=nullif(v_issue->>'source_row_id','')::uuid;
      v_source_link_id:=nullif(v_issue->>'source_work_event_link_id','')::uuid;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_IDENTITY_INVALID' using errcode='22023';
    end;
    if v_event.id=any(v_seen_work_events) then
      raise exception 'WEEKLY_SOURCE_QUERY_DUPLICATE_WORK_EVENT' using errcode='22023';
    end if;
    v_seen_work_events:=pg_catalog.array_append(v_seen_work_events,v_event.id);
    v_issue_family:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_issue->>'issue_family','')));
    v_presence:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_issue->>'source_presence','')));
    if v_issue_family='CANDIDATE_TIMESHEET_MISSING' then
      raise exception 'WEEKLY_SOURCE_USE_TIMESHEET_SUBMISSION_REQUEST' using errcode='22023';
    end if;
    if v_issue_family not in (
      'SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER',
      'REFERENCE_MISSING','HEALTHROSTER_NOT_FINALISED'
    ) or v_presence not in ('PRESENT','ABSENT','UNFINALISED') then
      raise exception 'WEEKLY_SOURCE_QUERY_ISSUE_KIND_INVALID' using errcode='22023';
    end if;
    if v_issue_family='SOURCE_HOURS_DIFFER' and v_presence<>'PRESENT' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_presence<>'ABSENT' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_issue_family='HEALTHROSTER_NOT_FINALISED' and v_presence<>'UNFINALISED' then
      raise exception 'WEEKLY_SOURCE_QUERY_PRESENCE_INVALID' using errcode='22023';
    end if;
    if v_timesheet_id is null or v_timesheet_revision is null or v_candidate_shift_hash is null
       or not exists(
         select 1 from public.timesheets t
         where t.timesheet_id=v_timesheet_id
           and t.version=v_timesheet_revision
           and t.contract_id=v_contract_id
           and t.week_ending_date>=v_event.work_date
           and t.week_ending_date<v_event.work_date+7
           and t.is_current and t.revoked_at is null and t.archived_at_utc is null
           and t.sheet_scope='WEEKLY' and t.line_type='HOURS'
           and t.r2_nurse_key is not null and t.img_sha256_nurse is not null
       ) then
      raise exception 'WEEKLY_SOURCE_SIGNED_TIMESHEET_REQUIRED' using errcode='55000';
    end if;
    if v_source_row_id is not null and not exists(
      select 1 from public.weekly_source_upload_rows r
      where r.id=v_source_row_id and r.upload_id=v_upload_id
    ) then
      raise exception 'WEEKLY_SOURCE_QUERY_SOURCE_ROW_STALE' using errcode='55000';
    end if;
    if v_source_link_id is not null and not exists(
      select 1
      from public.weekly_work_event_source_links link
      join public.weekly_source_row_resolutions resolution
        on resolution.id=link.row_resolution_id
      where link.id=v_source_link_id and link.work_event_id=v_event.id
        and (v_source_row_id is null or link.upload_row_id=v_source_row_id)
        and resolution.mapping_state='RESOLVED'
        and resolution.work_event_id=v_event.id
        and resolution.candidate_id=v_event.candidate_id
        and resolution.client_id=v_event.client_id
        and resolution.contract_id=v_contract_id
    ) then
      raise exception 'WEEKLY_SOURCE_QUERY_SOURCE_LINK_INVALID' using errcode='55000';
    end if;
    v_policy:=private._weekly_source_effective_policy_v1(
      v_event.client_id,v_contract_id,v_event.work_date
    );
    if v_policy->>'source_group_id'<>v_group_id::text
       or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
       or v_policy->>'document_mode'<>'CHECK_ONLY' then
      raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
    end if;

    perform 1 from public.weekly_work_events where id=v_event.id for update;
    if private.weekly_source_query_resolved_decision_matches_v1(v_group_id,v_event.id,v_issue) then
      v_unchanged_count:=v_unchanged_count+1;
      continue;
    end if;
    select * into v_incident
    from public.weekly_discrepancy_incidents
    where source_group_id=v_group_id and work_event_id=v_event.id and state='OPEN'
    for update;
    v_is_new:=not found;
    if v_is_new then
      select coalesce(pg_catalog.max(episode_number),0)+1 into v_episode
      from public.weekly_discrepancy_incidents
      where source_group_id=v_group_id and work_event_id=v_event.id;
      insert into public.weekly_discrepancy_incidents(
        source_group_id,work_event_id,episode_number,candidate_id,client_id,
        source_cycle_id,state,reconciliation_state,candidate_action_state,
        manager_potential_state,manager_action_state,waiting_source_state
      ) values (
        v_group_id,v_event.id,v_episode,v_event.candidate_id,v_event.client_id,
        v_cycle_id,'OPEN','UNRESOLVED',
        case when coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
          then 'NOT_ASKED' else 'NOT_REQUIRED' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'AVAILABLE' else 'NOT_AVAILABLE' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'NOT_SENT' else 'NOT_REQUIRED' end,
        'NOT_WAITING'
      ) returning * into v_incident;
      v_new_count:=v_new_count+1;
      v_new_cohort_keys:=pg_catalog.array_append(
        v_new_cohort_keys,
        v_cycle_id::text||':'||v_event.candidate_id::text||':'||v_event.client_id::text
      );
    else
      if v_incident.candidate_id is distinct from v_event.candidate_id
         or v_incident.client_id is distinct from v_event.client_id then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      if v_incident.current_comparison_revision_id is null then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      select * into strict v_comparison
      from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      if v_comparison.contract_id is distinct from v_contract_id
         or exists(
           select 1
           from public.weekly_source_cycles origin_cycle
           where origin_cycle.id=v_incident.source_cycle_id
             and origin_cycle.finalisation_week_ending>(v_guard->>'finalisation_week_ending')::date
         ) then
        raise exception 'WEEKLY_SOURCE_QUERY_INCIDENT_SCOPE_INVALID' using errcode='55000';
      end if;
      v_episode:=v_incident.episode_number;
    end if;
    if v_is_new then
      v_comparison.id:=null;
    end if;
    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_event.id,v_issue
    );
    if not v_is_new and v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      -- A changed source tuple is a new comparison revision of the same open
      -- episode. Only a resolved work event that reappears starts a new episode.
      -- Preserve route activation, due times and accepted manager memberships.
      v_changed_count:=v_changed_count+1;
    end if;
    if nullif(v_issue->>'expected_material_comparison_fingerprint','') is not null then
      v_expected:=private.weekly_source_query_hex32_v1(
        v_issue->>'expected_material_comparison_fingerprint',
        'WEEKLY_SOURCE_COMPARISON_HASH_INVALID'
      );
      if v_expected is distinct from v_fingerprint then
        raise exception 'WEEKLY_SOURCE_COMPARISON_HASH_MISMATCH' using errcode='40001';
      end if;
    end if;
    if v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint=v_fingerprint then
      v_unchanged_count:=v_unchanged_count+1;
    else
      select coalesce(pg_catalog.max(revision_number),0)+1 into v_revision
      from public.weekly_issue_comparison_revisions
      where incident_id=v_incident.id;
      insert into public.weekly_issue_comparison_revisions(
        incident_id,revision_number,projection_publication_id,comparison_upload_id,
        final_revision_id,candidate_timesheet_id,candidate_timesheet_revision,
        candidate_shift_fingerprint,source_row_id,source_work_event_link_id,
        contract_id,issue_family,source_presence,candidate_start_at_local,
        candidate_end_at_local,candidate_break_minutes,system_start_at_local,
        system_end_at_local,system_break_minutes,material_comparison_fingerprint
      ) values (
        v_incident.id,v_revision,v_publication_id,v_upload_id,null,
        v_timesheet_id,v_timesheet_revision,v_candidate_shift_hash,v_source_row_id,
        v_source_link_id,v_contract_id,v_issue_family,v_presence,
        nullif(v_issue->>'candidate_start_at_local','')::timestamp,
        nullif(v_issue->>'candidate_end_at_local','')::timestamp,
        nullif(v_issue->>'candidate_break_minutes','')::integer,
        nullif(v_issue->>'system_start_at_local','')::timestamp,
        nullif(v_issue->>'system_end_at_local','')::timestamp,
        nullif(v_issue->>'system_break_minutes','')::integer,
        v_fingerprint
      ) returning * into v_comparison;
      update public.weekly_discrepancy_incidents
      set current_comparison_revision_id=v_comparison.id
      where id=v_incident.id;
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication_id,v_fingerprint,
        'SOURCE_RECHECKED','SYSTEM',
        pg_catalog.jsonb_build_object(
          'issue_family',v_issue_family,'source_presence',v_presence,
          'comparison_revision_id',v_comparison.id
        ),
        'SOURCE_RECHECKED:'||v_incident.id::text||':'||pg_catalog.encode(v_fingerprint,'hex')
      ) on conflict (event_kind,idempotency_key) do nothing;
      perform private.bpay_next_source_query_refresh_v1(v_contract_id,v_incident.work_event_id);
    end if;
  end loop;

  for v_incident in
    select incident.*
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_source_cycles origin_cycle
      on origin_cycle.id=incident.source_cycle_id
    where incident.source_group_id=v_group_id
      and incident.state='OPEN'
      and (nullif(v_guard->>'scope_client_id','') is null
        or incident.client_id=(v_guard->>'scope_client_id')::uuid)
      and not (incident.work_event_id=any(v_seen_work_events))
      and origin_cycle.finalisation_week_ending<=(v_guard->>'finalisation_week_ending')::date
      and (
        v_scope_kind='CYCLE'
        or (v_scope_kind='NHSP_REPORT_SCOPE' and incident.client_id=v_current_report_client_id)
      )
      and (
        v_omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE'
        or (
          v_final_authority_kind='NHSP_TRUST_BACKING_REPORT'
          and exists(
            select 1
            from public.weekly_work_event_source_links current_link
            join public.weekly_source_upload_rows current_row
              on current_row.id=current_link.upload_row_id
            join public.weekly_source_row_resolutions current_resolution
              on current_resolution.id=current_link.row_resolution_id
            where current_row.upload_id=v_upload_id
              and current_link.work_event_id=incident.work_event_id
              and current_link.link_kind in ('PROVISIONAL_SOURCE','POSITIVE_SOURCE')
              and current_row.row_finalisation_state='SOURCE_WORKED'
              and current_resolution.mapping_state='RESOLVED'
              and current_resolution.work_event_id=incident.work_event_id
              and current_resolution.candidate_id=incident.candidate_id
              and current_resolution.client_id=incident.client_id
              and current_resolution.contract_id=comparison.contract_id
              and current_row.start_at_local=comparison.candidate_start_at_local
              and current_row.end_at_local=comparison.candidate_end_at_local
              and current_row.break_minutes=comparison.candidate_break_minutes
          )
        )
      )
      and not exists(
        select 1
        from public.weekly_source_upload_rows current_row
        join public.weekly_source_row_resolutions current_resolution
          on current_resolution.upload_row_id=current_row.id
        where current_row.upload_id=v_upload_id
          and current_resolution.mapping_state='RESOLVED'
          and current_resolution.work_event_id=incident.work_event_id
          and (
            current_resolution.candidate_id is distinct from incident.candidate_id
            or current_resolution.client_id is distinct from incident.client_id
            or current_resolution.contract_id is distinct from comparison.contract_id
          )
      )
    order by incident.id
    for update of incident
  loop
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
        candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
        manager_action_state='NOT_REQUIRED',waiting_source_state='SOURCE_MATCHED',
        resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind='SOURCE_MATCHED'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships
    set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items
    set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications
    set operational_state='RESOLVED',resolved_at_utc=pg_catalog.transaction_timestamp()
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,null,
      'RESOLVED','SYSTEM',pg_catalog.jsonb_build_object('resolution','SOURCE_MATCHED'),
      'SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication_id::text
    ) on conflict (event_kind,idempotency_key) do nothing;
    perform private.bpay_next_source_query_refresh_v1((select q.contract_id from public.weekly_issue_comparison_revisions q where q.id=v_incident.current_comparison_revision_id),v_incident.work_event_id);
    v_resolved_count:=v_resolved_count+1;
  end loop;

  for v_cohort_key in
    select distinct key_value collate "C" as key_value
    from pg_catalog.unnest(v_new_cohort_keys) as key_value
    order by key_value
  loop
    perform private.weekly_source_query_restart_activated_cohort_v1(
      pg_catalog.split_part(v_cohort_key,':',1)::uuid,
      pg_catalog.split_part(v_cohort_key,':',2)::uuid,
      pg_catalog.split_part(v_cohort_key,':',3)::uuid,
      v_publication_id
    );
  end loop;

  perform private.weekly_source_import_outreach_v1(v_publication_id);
  perform public._audit_insert(
    'weekly_source_query_sync',v_publication_id::text,'WEEKLY_SOURCE_QUERY_SYNCED',
    null,
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle_id,'new_incidents',v_new_count,
      'changed_comparisons',v_changed_count,'unchanged_incidents',v_unchanged_count,
      'resolved_incidents',v_resolved_count
    ),
    'Current Weekly source comparison issues synchronised',v_actor
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'source_cycle_id',v_cycle_id,'projection_publication_id',v_publication_id,
    'new_incidents',v_new_count,'changed_comparisons',v_changed_count,
    'unchanged_incidents',v_unchanged_count,'resolved_incidents',v_resolved_count
  );
end;
$function$;

create or replace function private.weekly_source_candidate_submission_compare_sync_v1(
  p_candidate_generation_id uuid,
  p_projection_publication_id uuid,
  p_membership_id uuid,
  p_timesheet_id uuid,
  p_timesheet_hash bytea,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile_code text;
  v_submission public.weekly_timesheet_submission_requests%rowtype;
  v_membership public.weekly_timesheet_submission_request_memberships%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate_row jsonb;
  v_source_json jsonb;
  v_source record;
  v_candidate_start timestamp without time zone;
  v_candidate_end timestamp without time zone;
  v_candidate_break integer;
  v_candidate_shift_hash bytea;
  v_durable_identity_hash bytea;
  v_prior_work_event_id uuid;
  v_work_event public.weekly_work_events%rowtype;
  v_issue jsonb;
  v_issues jsonb:='[]'::jsonb;
  v_used_source_rows uuid[]:='{}'::uuid[];
  v_seen_work_events uuid[]:='{}'::uuid[];
  v_tie_count integer;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_policy jsonb;
  v_episode integer;
  v_revision integer;
  v_fingerprint bytea;
  v_is_new boolean;
  v_issue_family text;
  v_presence text;
  v_new_count integer:=0;
  v_changed_count integer:=0;
  v_unchanged_count integer:=0;
  v_resolved_count integer:=0;
  v_open_count integer:=0;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_projection_publication_id is null or p_timesheet_id is null
     or p_timesheet_hash is null or pg_catalog.octet_length(p_timesheet_hash)<>32 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
  end if;

  if p_candidate_generation_id is null then
    -- A newly published checking source must recompare already-signed weekly
    -- evidence even when no submission request was needed or remains active.
    if p_membership_id is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
    end if;
    select * into strict v_publication
    from public.weekly_source_projection_publications
    where id=p_projection_publication_id and state='CURRENT';
    select * into strict v_cycle from public.weekly_source_cycles
    where id=v_publication.source_cycle_id for update;
    select * into strict v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current
      and revoked_at is null and archived_at_utc is null
      and sheet_scope='WEEKLY' and line_type='HOURS'
      and authorised_at_server is null
      and r2_nurse_key is not null and img_sha256_nurse is not null
    for update;
    select * into strict v_contract from public.contracts
    where id=v_timesheet.contract_id;
    v_generation.candidate_id:=v_contract.candidate_id;
    v_membership.contract_id:=v_contract.id;
    v_membership.client_id:=v_contract.client_id;
    v_membership.week_ending:=v_timesheet.week_ending_date;
  else
    if p_membership_id is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_REQUEST_INVALID' using errcode='22023';
    end if;
    select * into strict v_generation
    from public.weekly_candidate_outreach_generations
    where id=p_candidate_generation_id and request_kind='SUBMIT_TIMESHEET'
      and state='ACTIVE'
    for update;
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_generation.source_cycle_id
    for update;
  end if;
  perform private.weekly_source_query_current_publication_v1(
    v_cycle.id,p_projection_publication_id
  );
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=p_projection_publication_id
    and source_cycle_id=v_cycle.id and state='CURRENT';
  select * into strict v_upload from public.weekly_source_uploads
  where id=v_publication.upload_id and source_cycle_id=v_cycle.id;
  select profile.profile_code into strict v_profile_code
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id;
  if v_profile_code='NHSP_PREFINAL_RELEASED_V1'
     and (v_upload.confirmed_coverage_start_local_date is null
       or v_upload.confirmed_coverage_end_local_date is null) then
    raise exception 'WEEKLY_SOURCE_PREFINAL_COVERAGE_INVALID' using errcode='55000';
  end if;
  if p_candidate_generation_id is not null then
    select * into strict v_submission
    from public.weekly_timesheet_submission_requests
    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=v_generation.candidate_id
      and source_cycle_id=v_cycle.id
      and current_projection_publication_id=v_publication.id
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')
    for update;
    select * into strict v_membership
    from public.weekly_timesheet_submission_request_memberships
    where id=p_membership_id and submission_request_id=v_submission.id
      and state='WAITING'
    for update;
    select * into strict v_contract from public.contracts
    where id=v_membership.contract_id
      and candidate_id=v_generation.candidate_id
      and client_id=v_membership.client_id;
    select * into strict v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and contract_id=v_membership.contract_id
      and week_ending_date=v_membership.week_ending
      and is_current and revoked_at is null and archived_at_utc is null
      and sheet_scope='WEEKLY' and line_type='HOURS'
      and authorised_at_server is null
      and r2_nurse_key is not null and img_sha256_nurse is not null
    for update;
  end if;
  if v_timesheet.version<1
     or private.weekly_source_query_candidate_timesheet_hash_v1(v_timesheet.timesheet_id)
          is distinct from p_timesheet_hash then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_membership.week_ending
  );
  if v_policy->>'source_group_id' is distinct from v_group.id::text
     or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_policy->>'document_mode'<>'CHECK_ONLY' then
    raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
  end if;
  if exists(
    select 1
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where source_row.row_finalisation_state in (
      'BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'
    )
  ) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SOURCE_SCOPE_BLOCKED' using errcode='55000';
  end if;

  for v_candidate_row in
    select value
    from pg_catalog.jsonb_array_elements(
      private.weekly_source_candidate_app_schedule_v1(
        v_timesheet.actual_schedule_json,v_timesheet.additional_units_per_day
      )
    ) candidate_row
    where v_profile_code<>'NHSP_PREFINAL_RELEASED_V1'
      or (candidate_row.value->>'date')::date between
        v_upload.confirmed_coverage_start_local_date
        and v_upload.confirmed_coverage_end_local_date
    order by value->>'date',value->>'start',value->>'end',
      coalesce(value->>'row_key','') collate "C"
  loop
    v_candidate_start:=(v_candidate_row->>'date')::date+
      (v_candidate_row->>'start')::time;
    v_candidate_end:=(v_candidate_row->>'date')::date+
      (v_candidate_row->>'end')::time;
    if v_candidate_end<=v_candidate_start then
      v_candidate_end:=v_candidate_end+interval '1 day';
    end if;
    v_candidate_break:=coalesce(
      nullif(v_candidate_row#>>'{break_entry,break_minutes}','')::integer,
      nullif(v_candidate_row#>>'{break_entry,calculated_break_minutes}','')::integer,0
    );
    v_candidate_shift_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CANDIDATE_SHIFT_V1',
      pg_catalog.jsonb_build_object(
        'timesheet_id',v_timesheet.timesheet_id,'timesheet_revision',v_timesheet.version,
        'row_key',v_candidate_row->>'row_key','work_date',v_candidate_row->>'date',
        'start_at_local',v_candidate_start,'end_at_local',v_candidate_end,
        'break_minutes',v_candidate_break,
        'additional_units',coalesce(v_candidate_row->'additional_units','[]'::jsonb)
      )
    );

    select pg_catalog.jsonb_build_object(
      'source_row_id',source_row.source_row_id,'source_link_id',source_row.source_link_id,
      'source_link_kind',(select link.link_kind
        from public.weekly_work_event_source_links link
        where link.id=source_row.source_link_id),
      'work_event_id',source_row.work_event_id,'work_date',source_row.work_date,
      'start_at_local',source_row.start_at_local,'end_at_local',source_row.end_at_local,
      'break_minutes',source_row.break_minutes,
      'row_finalisation_state',source_row.row_finalisation_state
    ) into v_source_json
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where not (source_row.source_row_id=any(v_used_source_rows))
      and source_row.row_finalisation_state in (
        'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_UNFINALISED'
      )
      and (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id)
            not in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
      and source_row.work_date=(v_candidate_row->>'date')::date
      and source_row.start_at_local=v_candidate_start
      and source_row.end_at_local=v_candidate_end
      and coalesce(source_row.break_minutes,0)=v_candidate_break
    order by source_row.source_row_id limit 1;

    if v_source_json is null then
      with possible as (
        select source_row.*,
          (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id) as source_link_kind,
          case
            when source_row.start_at_local is null or source_row.end_at_local is null
              then 1000000000::numeric
            else
              pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.start_at_local-v_candidate_start)/60)
              +pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.end_at_local-v_candidate_end)/60)
              +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
          end as distance
        from private.weekly_source_candidate_current_rows_v1(
          v_publication.id,v_generation.candidate_id,v_membership.client_id,
          v_membership.contract_id,v_membership.week_ending
        ) source_row
        where not (source_row.source_row_id=any(v_used_source_rows))
          and source_row.work_date=(v_candidate_row->>'date')::date
          and source_row.row_finalisation_state in (
            'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED'
          )
      ),minimum as (select pg_catalog.min(distance) distance from possible)
      select pg_catalog.count(*)::integer into v_tie_count
      from possible,minimum where possible.distance=minimum.distance;
      if v_tie_count>1 then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_AMBIGUOUS' using errcode='55000';
      elsif v_tie_count=1 then
        with possible as (
          select source_row.*,
            (select link.link_kind from public.weekly_work_event_source_links link
             where link.id=source_row.source_link_id) as source_link_kind,
            case
              when source_row.start_at_local is null or source_row.end_at_local is null
                then 1000000000::numeric
              else
                pg_catalog.abs(pg_catalog.date_part('epoch',
                  source_row.start_at_local-v_candidate_start)/60)
                +pg_catalog.abs(pg_catalog.date_part('epoch',
                  source_row.end_at_local-v_candidate_end)/60)
                +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
            end as distance
          from private.weekly_source_candidate_current_rows_v1(
            v_publication.id,v_generation.candidate_id,v_membership.client_id,
            v_membership.contract_id,v_membership.week_ending
          ) source_row
          where not (source_row.source_row_id=any(v_used_source_rows))
            and source_row.work_date=(v_candidate_row->>'date')::date
            and source_row.row_finalisation_state in (
              'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED'
            )
        )
        select pg_catalog.jsonb_build_object(
          'source_row_id',source_row_id,'source_link_id',source_link_id,
          'source_link_kind',source_link_kind,
          'work_event_id',work_event_id,'work_date',work_date,
          'start_at_local',start_at_local,'end_at_local',end_at_local,
          'break_minutes',break_minutes,'row_finalisation_state',row_finalisation_state
        ) into v_source_json
        from possible order by distance,source_row_id limit 1;
      end if;
    end if;

    if v_source_json is null then
      -- A replacement NHSP file may omit a previously linked shift. Reuse
      -- that durable event instead of inventing a new issue/notification clock.
      v_prior_work_event_id:=null;
      if p_candidate_generation_id is null then
        with prior_events as (
          select link.work_event_id,
            pg_catalog.min(
              pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.start_at_local-v_candidate_start)/60)
              +pg_catalog.abs(pg_catalog.date_part('epoch',
                source_row.end_at_local-v_candidate_end)/60)
              +pg_catalog.abs(coalesce(source_row.break_minutes,0)-v_candidate_break)
            ) as distance
          from public.weekly_source_uploads historical_upload
          join public.weekly_source_upload_rows source_row
            on source_row.upload_id=historical_upload.id
          join public.weekly_source_row_resolutions resolution
            on resolution.upload_row_id=source_row.id
           and resolution.mapping_state='RESOLVED'
          join public.weekly_work_event_source_links link
            on link.upload_row_id=source_row.id
           and link.row_resolution_id=resolution.id
           and link.work_event_id=resolution.work_event_id
          join public.weekly_work_events work_event
            on work_event.id=link.work_event_id
          where historical_upload.source_cycle_id=v_cycle.id
            and historical_upload.id<>v_upload.id
            -- A provisional check must not choose a final backing-report
            -- event as the identity of a missing provisional shift.
            and historical_upload.source_format_profile_id=v_upload.source_format_profile_id
            and source_row.work_date=(v_candidate_row->>'date')::date
            and source_row.start_at_local is not null
            and source_row.end_at_local is not null
            and resolution.candidate_id=v_generation.candidate_id
            and resolution.client_id=v_membership.client_id
            and resolution.contract_id=v_membership.contract_id
            and work_event.first_source_group_id=v_group.id
          group by link.work_event_id
        ), nearest as (
          select work_event_id,distance,
            pg_catalog.count(*) over (partition by distance) as tie_count
          from prior_events
        )
        select work_event_id,tie_count
        into v_prior_work_event_id,v_tie_count
        from nearest order by distance,work_event_id limit 1;
        if v_tie_count>1 then
          raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_AMBIGUOUS' using errcode='55000';
        end if;
      end if;
      if v_prior_work_event_id is null then
        v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_WORK_EVENT_SCHEDULE_TUPLE_V1',
        pg_catalog.jsonb_build_object(
          'source_group_id',v_group.id,
          'source_format_profile_id',v_upload.source_format_profile_id,
          'candidate_id',v_generation.candidate_id,
          'client_id',v_membership.client_id,
          'work_date',(v_candidate_row->>'date')::date,
          'start_at_local',v_candidate_start,'end_at_local',v_candidate_end
        )
      );
        insert into public.weekly_work_events(
        candidate_id,client_id,work_date,identity_kind,profile_external_key,
        durable_identity_hash,first_source_group_id,source_format_profile_id
      ) values (
        v_generation.candidate_id,v_membership.client_id,
        (v_candidate_row->>'date')::date,'SCHEDULE_TUPLE',null,
        v_durable_identity_hash,v_group.id,v_upload.source_format_profile_id
      ) on conflict (durable_identity_hash) do nothing;
        select * into strict v_work_event from public.weekly_work_events
        where durable_identity_hash=v_durable_identity_hash;
        if v_work_event.candidate_id is distinct from v_generation.candidate_id
           or v_work_event.client_id is distinct from v_membership.client_id
           or v_work_event.work_date is distinct from (v_candidate_row->>'date')::date
           or v_work_event.first_source_group_id is distinct from v_group.id then
          raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_COLLISION' using errcode='55000';
        end if;
      else
        select * into strict v_work_event from public.weekly_work_events
        where id=v_prior_work_event_id;
      end if;
      v_issue:=pg_catalog.jsonb_build_object(
        'work_event_id',v_work_event.id,
        'candidate_timesheet_id',v_timesheet.timesheet_id,
        'candidate_timesheet_revision',v_timesheet.version,
        'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
        'source_row_id',null,'source_work_event_link_id',null,
        'contract_id',v_membership.contract_id,
        'issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
        'candidate_start_at_local',v_candidate_start,
        'candidate_end_at_local',v_candidate_end,
        'candidate_break_minutes',v_candidate_break,
        'system_start_at_local',null,'system_end_at_local',null,
        'system_break_minutes',null
      );
      v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
    else
      v_used_source_rows:=pg_catalog.array_append(
        v_used_source_rows,(v_source_json->>'source_row_id')::uuid
      );
      if v_source_json->>'source_link_kind' in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
         or v_source_json->>'row_finalisation_state'='SOURCE_ABSENT_ZERO' then
        v_issue_family:='SOURCE_MISSING_OR_NOT_AUTHORISED';
        v_presence:='ABSENT';
      elsif v_source_json->>'row_finalisation_state'='SOURCE_UNFINALISED' then
        v_issue_family:='HEALTHROSTER_NOT_FINALISED';
        v_presence:='UNFINALISED';
      elsif nullif(v_source_json->>'start_at_local','')::timestamp=v_candidate_start
         and nullif(v_source_json->>'end_at_local','')::timestamp=v_candidate_end
         and coalesce(nullif(v_source_json->>'break_minutes','')::integer,0)=v_candidate_break then
        v_issue_family:=null;
        v_presence:='PRESENT';
      else
        v_issue_family:='SOURCE_HOURS_DIFFER';
        v_presence:='PRESENT';
      end if;
      if v_issue_family is not null then
        v_issue:=pg_catalog.jsonb_build_object(
          'work_event_id',(v_source_json->>'work_event_id')::uuid,
          'candidate_timesheet_id',v_timesheet.timesheet_id,
          'candidate_timesheet_revision',v_timesheet.version,
          'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
          'source_row_id',(v_source_json->>'source_row_id')::uuid,
          'source_work_event_link_id',(v_source_json->>'source_link_id')::uuid,
          'contract_id',v_membership.contract_id,
          'issue_family',v_issue_family,'source_presence',v_presence,
          'candidate_start_at_local',v_candidate_start,
          'candidate_end_at_local',v_candidate_end,
          'candidate_break_minutes',v_candidate_break,
          'system_start_at_local',case when v_presence='PRESENT'
            then nullif(v_source_json->>'start_at_local','')::timestamp end,
          'system_end_at_local',case when v_presence='PRESENT'
            then nullif(v_source_json->>'end_at_local','')::timestamp end,
          'system_break_minutes',case when v_presence='PRESENT'
            then nullif(v_source_json->>'break_minutes','')::integer end
        );
        v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
      end if;
    end if;
  end loop;

  for v_source in
    select *
    from private.weekly_source_candidate_current_rows_v1(
      v_publication.id,v_generation.candidate_id,v_membership.client_id,
      v_membership.contract_id,v_membership.week_ending
    ) source_row
    where not (source_row.source_row_id=any(v_used_source_rows))
      and source_row.row_finalisation_state<>'SOURCE_ABSENT_ZERO'
      and (select link.link_kind from public.weekly_work_event_source_links link
           where link.id=source_row.source_link_id)
            not in ('FULL_NEGATIVE_SOURCE','ZERO_SOURCE')
    order by source_row.work_date,source_row.start_at_local,
      source_row.end_at_local,source_row.source_row_id
  loop
    v_candidate_shift_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CANDIDATE_SHIFT_V1',
      pg_catalog.jsonb_build_object(
        'timesheet_id',v_timesheet.timesheet_id,
        'timesheet_revision',v_timesheet.version,
        'work_event_id',v_source.work_event_id,'worked',false
      )
    );
    if v_source.row_finalisation_state='SOURCE_UNFINALISED' then
      v_issue_family:='HEALTHROSTER_NOT_FINALISED';
      v_presence:='UNFINALISED';
    elsif v_source.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED') then
      v_issue_family:='SOURCE_HOURS_DIFFER';
      v_presence:='PRESENT';
    else
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SOURCE_SCOPE_BLOCKED' using errcode='55000';
    end if;
    v_issue:=pg_catalog.jsonb_build_object(
      'work_event_id',v_source.work_event_id,
      'candidate_timesheet_id',v_timesheet.timesheet_id,
      'candidate_timesheet_revision',v_timesheet.version,
      'candidate_shift_fingerprint',pg_catalog.encode(v_candidate_shift_hash,'hex'),
      'source_row_id',v_source.source_row_id,
      'source_work_event_link_id',v_source.source_link_id,
      'contract_id',v_membership.contract_id,
      'issue_family',v_issue_family,'source_presence',v_presence,
      'candidate_start_at_local',null,'candidate_end_at_local',null,
      'candidate_break_minutes',null,
      'system_start_at_local',case when v_presence='PRESENT'
        then v_source.start_at_local end,
      'system_end_at_local',case when v_presence='PRESENT'
        then v_source.end_at_local end,
      'system_break_minutes',case when v_presence='PRESENT'
        then v_source.break_minutes end
    );
    v_issues:=v_issues||pg_catalog.jsonb_build_array(v_issue);
  end loop;

  for v_issue in
    select value from pg_catalog.jsonb_array_elements(v_issues)
    order by value->>'work_event_id'
  loop
    select * into strict v_work_event from public.weekly_work_events
    where id=(v_issue->>'work_event_id')::uuid
      and candidate_id=v_generation.candidate_id
      and client_id=v_membership.client_id
      and first_source_group_id=v_group.id;
    if v_work_event.id=any(v_seen_work_events) then
      raise exception 'WEEKLY_SOURCE_QUERY_DUPLICATE_WORK_EVENT' using errcode='22023';
    end if;
    v_seen_work_events:=pg_catalog.array_append(v_seen_work_events,v_work_event.id);
    v_issue_family:=v_issue->>'issue_family';
    v_presence:=v_issue->>'source_presence';
    v_policy:=private._weekly_source_effective_policy_v1(
      v_membership.client_id,v_membership.contract_id,v_work_event.work_date
    );
    if v_policy->>'source_group_id' is distinct from v_group.id::text
       or v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
       or v_policy->>'document_mode'<>'CHECK_ONLY' then
      raise exception 'WEEKLY_SOURCE_SECURE_QUERY_NOT_APPLICABLE' using errcode='55000';
    end if;

    perform 1 from public.weekly_work_events where id=v_work_event.id for update;
    if private.weekly_source_query_resolved_decision_matches_v1(v_group.id,v_work_event.id,v_issue) then
      v_unchanged_count:=v_unchanged_count+1;
      continue;
    end if;
    select * into v_incident
    from public.weekly_discrepancy_incidents
    where source_group_id=v_group.id and work_event_id=v_work_event.id
      and state='OPEN'
    for update;
    v_is_new:=not found;
    if v_is_new then
      select coalesce(pg_catalog.max(episode_number),0)+1 into v_episode
      from public.weekly_discrepancy_incidents
      where source_group_id=v_group.id and work_event_id=v_work_event.id;
      insert into public.weekly_discrepancy_incidents(
        source_group_id,work_event_id,episode_number,candidate_id,client_id,
        source_cycle_id,state,reconciliation_state,candidate_action_state,
        manager_potential_state,manager_action_state,waiting_source_state
      ) values (
        v_group.id,v_work_event.id,v_episode,v_generation.candidate_id,
        v_membership.client_id,v_cycle.id,'OPEN','UNRESOLVED',
        case when p_candidate_generation_id is null
          and coalesce((v_policy->>'candidate_queries_enabled')::boolean,false)
          then 'NOT_ASKED' else 'NOT_REQUIRED' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'AVAILABLE' else 'NOT_AVAILABLE' end,
        case when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                   and nullif(v_policy->>'manager_query_recipient','') is not null
          then 'NOT_SENT' else 'NOT_REQUIRED' end,
        'NOT_WAITING'
      ) returning * into v_incident;
      v_comparison.id:=null;
      v_new_count:=v_new_count+1;
    else
      if v_incident.candidate_id is distinct from v_generation.candidate_id
         or v_incident.client_id is distinct from v_membership.client_id
         or v_incident.source_cycle_id is distinct from v_cycle.id
         or v_incident.current_comparison_revision_id is null then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_INCIDENT_SCOPE_CONFLICT' using errcode='55000';
      end if;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      if v_comparison.contract_id is distinct from v_membership.contract_id then
        raise exception 'WEEKLY_SOURCE_CANDIDATE_INCIDENT_SCOPE_CONFLICT' using errcode='55000';
      end if;
      v_episode:=v_incident.episode_number;
    end if;

    v_fingerprint:=private.weekly_source_query_comparison_fingerprint_v1(
      v_incident.id,v_episode,v_work_event.id,v_issue
    );
    if not v_is_new and v_comparison.material_comparison_fingerprint<>v_fingerprint then
      -- A re-signed candidate week changes the current comparison but not the
      -- identity or outreach clock of an already-open work-event incident.
      v_changed_count:=v_changed_count+1;
    end if;
    if v_comparison.id is not null
       and v_comparison.material_comparison_fingerprint=v_fingerprint then
      v_unchanged_count:=v_unchanged_count+1;
    else
      select coalesce(pg_catalog.max(revision_number),0)+1 into v_revision
      from public.weekly_issue_comparison_revisions
      where incident_id=v_incident.id;
      insert into public.weekly_issue_comparison_revisions(
        incident_id,revision_number,projection_publication_id,comparison_upload_id,
        final_revision_id,candidate_timesheet_id,candidate_timesheet_revision,
        candidate_shift_fingerprint,source_row_id,source_work_event_link_id,
        contract_id,issue_family,source_presence,candidate_start_at_local,
        candidate_end_at_local,candidate_break_minutes,system_start_at_local,
        system_end_at_local,system_break_minutes,material_comparison_fingerprint
      ) values (
        v_incident.id,v_revision,v_publication.id,v_upload.id,null,
        v_timesheet.timesheet_id,v_timesheet.version,
        private.weekly_source_query_hex32_v1(
          v_issue->>'candidate_shift_fingerprint',
          'WEEKLY_SOURCE_CANDIDATE_SHIFT_HASH_INVALID'
        ),nullif(v_issue->>'source_row_id','')::uuid,
        nullif(v_issue->>'source_work_event_link_id','')::uuid,
        v_membership.contract_id,v_issue_family,v_presence,
        nullif(v_issue->>'candidate_start_at_local','')::timestamp,
        nullif(v_issue->>'candidate_end_at_local','')::timestamp,
        nullif(v_issue->>'candidate_break_minutes','')::integer,
        nullif(v_issue->>'system_start_at_local','')::timestamp,
        nullif(v_issue->>'system_end_at_local','')::timestamp,
        nullif(v_issue->>'system_break_minutes','')::integer,v_fingerprint
      ) returning * into v_comparison;
      update public.weekly_discrepancy_incidents
      set current_comparison_revision_id=v_comparison.id
      where id=v_incident.id;
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
        idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication.id,v_fingerprint,
        'SOURCE_RECHECKED','SYSTEM',
        pg_catalog.jsonb_build_object(
          'issue_family',v_issue_family,'source_presence',v_presence,
          'comparison_revision_id',v_comparison.id,
          'origin','CANDIDATE_TIMESHEET_SUBMISSION'
        ),
        'SOURCE_RECHECKED:'||v_incident.id::text||':'||pg_catalog.encode(v_fingerprint,'hex')
      ) on conflict (event_kind,idempotency_key) do nothing;
      perform private.bpay_next_source_query_refresh_v1(v_membership.contract_id,v_incident.work_event_id);
    end if;
  end loop;

  for v_incident in
    select incident.*
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    where incident.source_group_id=v_group.id
      and incident.source_cycle_id=v_cycle.id
      and incident.candidate_id=v_generation.candidate_id
      and incident.client_id=v_membership.client_id
      and comparison.contract_id=v_membership.contract_id
      and work_event.work_date between v_membership.week_ending-6 and v_membership.week_ending
      and (v_profile_code<>'NHSP_PREFINAL_RELEASED_V1'
        or work_event.work_date between v_upload.confirmed_coverage_start_local_date
          and v_upload.confirmed_coverage_end_local_date)
      and incident.state='OPEN'
      and not (incident.work_event_id=any(v_seen_work_events))
    order by incident.id for update of incident
  loop
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
      candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
      manager_action_state='NOT_REQUIRED',waiting_source_state='SOURCE_MATCHED',
      resolved_at_utc=p_now_utc,resolution_kind='SOURCE_MATCHED'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications
    set operational_state='RESOLVED',resolved_at_utc=p_now_utc
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication.id,null,
      'RESOLVED','SYSTEM',
      pg_catalog.jsonb_build_object(
        'resolution','SOURCE_MATCHED','origin','CANDIDATE_TIMESHEET_SUBMISSION'
      ),
      'SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication.id::text
    ) on conflict (event_kind,idempotency_key) do nothing;
    perform private.bpay_next_source_query_refresh_v1(v_membership.contract_id,v_incident.work_event_id);
    v_resolved_count:=v_resolved_count+1;
  end loop;

  select pg_catalog.count(*)::integer into v_open_count
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison
    on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work_event on work_event.id=incident.work_event_id
  where incident.source_cycle_id=v_cycle.id
    and incident.candidate_id=v_generation.candidate_id
    and incident.client_id=v_membership.client_id
    and comparison.contract_id=v_membership.contract_id
    and work_event.work_date between v_membership.week_ending-6 and v_membership.week_ending
    and incident.state='OPEN';

  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome',case when v_open_count=0 then 'MATCHED' else 'ISSUES' end,
    'open_issue_count',v_open_count,'new_incidents',v_new_count,
    'changed_comparisons',v_changed_count,'unchanged_incidents',v_unchanged_count,
    'resolved_incidents',v_resolved_count
  );
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_STALE' using errcode='40001';
end;
$function$;

create or replace function public.weekly_exceptional_pay_publish_next_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_keys constant text[]:=array['schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','client_id','work_event_id','evidence_timesheet_id','expected_family_bound_version',
    'protected_schedule','rate_classification','source_proposal','target_snapshot',
    'current_comparison_revision_id','current_final_revision_id','reason','idempotency_key'];
  v_request_hash bytea;
  v_source_hash bytea;
  v_target_hash bytea;
  v_schedule_hash bytea;
  v_policy_hash bytea;
  v_approval_hash bytea;
  v_prior_vector jsonb;
  v_prior_vector_hash bytea;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_receipt private.bpay_next_protected_source_receipt%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
  v_root public.timesheets%rowtype;
  v_evidence_row public.timesheets%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_context jsonb;
  v_policy jsonb;
  v_signed jsonb;
  v_components jsonb;
  v_component jsonb;
  v_source jsonb;
  v_break jsonb;
  v_actor uuid;
  v_event uuid;
  v_evidence uuid;
  v_work_date date;
  v_key text;
  v_total numeric:=0;
  v_match integer;
  v_n integer;
  v_issue_ids uuid[];
  v_issue_hash bytea;
  v_reason text;
  v_generation_reason text;
  v_state text;
  v_lifecycle text;
  v_event_sequence bigint;
  v_family_event_id uuid;
  v_prior_event_hash bytea;
  v_event_hash bytea;
  v_target_sequence bigint;
  v_prior_target_hash bytea;
  v_audit_hash bytea;
  v_step integer;
  v_stage record;
  v_publication record;
  v_bucket text;
  v_rate numeric;
  v_charge_rate numeric;
  v_hours numeric;
  v_local jsonb;
begin
  if coalesce(nullif(pg_catalog.current_setting('request.jwt.claim.role',true),''),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_SERVICE_REQUIRED';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys) or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_NEXT_PUBLISH_V1'
     or pg_catalog.octet_length(p_request::text)>128000
     or pg_catalog.jsonb_typeof(p_request->'target_snapshot')<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request->'target_snapshot',array['schema_version','components'])
     or p_request#>>'{target_snapshot,schema_version}'<>'WEEKLY_PROTECTED_NEXT_TARGET_V1'
     or pg_catalog.jsonb_typeof(p_request#>'{target_snapshot,components}')<>'array'
     or pg_catalog.jsonb_array_length(p_request#>'{target_snapshot,components}')>100 then
    raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_REQUEST_INVALID';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid; v_event:=(p_request->>'work_event_id')::uuid;
  v_evidence:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
  v_work_date:=(p_request#>>'{protected_schedule,work_date}')::date;
  v_key:=p_request->>'idempotency_key'; v_reason:=p_request->>'reason';
  if pg_catalog.char_length(v_key) not between 16 and 240 or pg_catalog.char_length(btrim(v_reason)) not between 1 and 1000
     or not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active
       and (u.payment_authoriser or u.payment_golden_key)) then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_PUBLISH_V1',p_request);
  -- Immutable exact replay precedes mutable current-root checks. A later
  -- legitimate physical rotation cannot invalidate this accepted receipt.
  select f.* into strict v_family from public.weekly_exceptional_pay_target_families f
    where f.id=(p_request->>'family_id')::uuid;
  select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=v_family.id;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
  if found then
    if v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.family_id<>v_family.id or v_run.state<>'COMPLETE'
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end if;
  -- BEGIN QUERY V2 DIRECT NEXT ADMISSION
  -- Exact immutable receipt replay above is lock-free. Fresh acceptance must
  -- admit before the protected booking/root lock; propagate exact 55P03.
  perform private.weekly_source_pay_query_admit_v2();
  -- END QUERY V2 DIRECT NEXT ADMISSION

  -- Accepted Local cannot be republished by the direct financial owner.
  -- Its status route exposes the actual original common-HEAD mapping instead.
  v_local:=private.bpay_next_protected_local_receipt_v1(v_family.id,v_run.id,v_actor);
  if v_local is not null then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;

  -- Actual first acceptance retains the Source advisory ordering, then only
  -- the exact current physical root, family and run mutexes. Recheck receipt
  -- after those locks for simultaneous identical first requests.
  begin
    perform private.bpay_next_protected_lock_current_root_v1(v_family.root_timesheet_id,v_family.candidate_id,
      v_family.contract_id,v_family.week_ending_date,v_family.root_family_booking_id);
  exception when sqlstate '55000' then
    if sqlerrm<>'BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT' then raise;end if;
    -- A concurrent identical first call may have committed while this call
    -- waited on the booking key, followed by a legitimate root rotation.
    -- Read only the immutable receipt; never retry an owner or adopt a root.
    select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
    if not found then raise;end if;
    select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
      where r.id=v_run.id and r.family_id=v_family.id;
    if v_run.requested_by_user_id is distinct from v_actor
       or v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.family_id<>v_family.id or v_run.state<>'COMPLETE'
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end;
  select f.* into strict v_family from public.weekly_exceptional_pay_target_families f where f.id=v_family.id for update;
  select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=v_family.id for update;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
  if found then
    if v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or v_receipt.family_id is distinct from v_family.id or v_run.state<>'COMPLETE'
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a
      where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  -- Recheck after the same family/run locks: no lane switch can occur between
  -- the pure status choice and fresh financial staging. First Authorise is an
  -- actual independent owner, never an approval metadata or saved-hour label.
  v_local:=private.bpay_next_protected_local_receipt_v1(v_family.id,v_run.id,v_actor);
  if v_local is not null or private.bpay_next_protected_owner_route_v1(v_family.id)<>'NEXT' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;
  perform private.bpay_next_protected_require_authorised_root_v1(v_family.root_timesheet_id);
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=(p_request->>'source_cycle_id')::uuid for share;
  select g.* into strict v_group from public.weekly_source_groups g where g.id=v_cycle.source_group_id and g.active for share;
  select c.* into strict v_contract from public.contracts c where c.id=v_family.contract_id for share;
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=v_family.root_timesheet_id for key share;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    v_contract.client_id,v_work_date);
  if v_run.state<>'RUNNING' or v_run.request_kind not in ('APPROVE','AMEND','WITHDRAW','RECONCILE','RECORD_NOT_WORKED')
     or v_family.bound_version<>(p_request->>'expected_family_bound_version')::bigint
     or v_run.request_fingerprint is null or v_family.agency_id<>v_group.agency_id
     or (v_family.current_generation_id is not null and v_family.target_domain_version<>'NEXT_V1')
     or v_family.c1_publication_state<>'NONE'
     or (p_request->>'client_id')::uuid<>v_contract.client_id
     or exists(select 1 from public.weekly_exceptional_c1_publication_requests r where r.family_id=v_family.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SCOPE_INVALID';
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_work_date);
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_policy->>'c1_source_mode' not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_policy->>'c1_source_mode'='NHSP_WEEKLY') then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_POLICY_INVALID';
  end if;
  v_context:=public.weekly_exceptional_pay_action_context_v1(pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1','actor_user_id',v_actor,
    'family_id',v_family.id,'orchestration_run_id',v_run.id,'source_cycle_id',v_cycle.id,
    'work_event_id',v_event,'protected_schedule',p_request->'protected_schedule','evidence_timesheet_id',v_evidence));
  if v_context->'source_proposal' is distinct from p_request->'source_proposal'
     or v_context->>'current_comparison_revision_id' is distinct from p_request->>'current_comparison_revision_id'
     or v_context->>'current_final_revision_id' is distinct from p_request->>'current_final_revision_id'
     or v_context->'protected_schedule' is distinct from p_request->'protected_schedule' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SOURCE_CHANGED';
  end if;
  if p_request->'rate_classification' is distinct from pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_SERVER_CALCULATION_V1',
      'calculator_owner','buildWeeklyScheduleSegmentsSnapshot',
      'policy_sha256',v_context#>>'{zero_rate_source_refs,effective_policy_sha256}')
     or v_context#>>'{candidate_submission,submission_id}' is distinct from v_evidence::text
     or pg_catalog.jsonb_typeof(p_request->'expected_family_bound_version') is distinct from 'string'
     or p_request->>'expected_family_bound_version' !~ '^[1-9][0-9]{0,18}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_PREVIEW_SCOPE_NOT_EXACT';
  end if;
  v_components:=p_request#>'{target_snapshot,components}';
  if pg_catalog.jsonb_array_length(v_components)<>(
      select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
      where d.value->>'state'='WAIT')+(
      select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(v_context->'next_source_segments') s(value)
      where not exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
        where d.value->>'work_event_id'=s.value#>>'{weekly_source,work_event_id}'
          and d.value->>'state' in ('WAIT','NOT_WORKED')))
      +pg_catalog.jsonb_array_length(v_context->'source_expenses') then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_MANIFEST_NOT_EXACT';
  end if;
  if exists(select 1 from pg_catalog.jsonb_array_elements(v_components) x(value)
      group by x.value->>'kind',x.value->>'work_event_id' having pg_catalog.count(*)<>1) then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_COMPONENT_DUPLICATE';
  end if;
  for v_component in select value from pg_catalog.jsonb_array_elements(v_components) loop
    if pg_catalog.jsonb_typeof(v_component)<>'object' or v_component->>'kind' not in ('WORK','EXPENSE')
       or pg_catalog.jsonb_typeof(v_component->'pay_ex_vat')<>'string'
       or pg_catalog.jsonb_typeof(v_component->'charge_ex_vat')<>'string'
       or (v_component->>'pay_ex_vat') !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'
       or (v_component->>'charge_ex_vat') !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_COMPONENT_INVALID';
    end if;
    if v_component->>'kind'='EXPENSE' then
      if not private.weekly_exceptional_json_keys_exact_v1(v_component,array['kind','work_event_id','source_expense_id',
          'final_revision_id','source_observation_kind','authority_sha256','pay_ex_vat','charge_ex_vat'])
         or not (v_component ?& array['kind','work_event_id','source_expense_id','final_revision_id',
          'source_observation_kind','authority_sha256','pay_ex_vat','charge_ex_vat']) then
        raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_EXPENSE_INVALID';
      end if;
      select pg_catalog.count(*)::integer into v_match from pg_catalog.jsonb_array_elements(v_context->'source_expenses') e(value)
        where e.value->>'work_event_id'=v_component->>'work_event_id'
          and e.value->>'source_expense_id'=v_component->>'source_expense_id'
          and e.value->>'final_revision_id'=v_component->>'final_revision_id'
          and e.value->>'source_observation_kind'=v_component->>'source_observation_kind'
          and e.value->>'document_sha256'=v_component->>'authority_sha256'
          and (e.value->>'pay_ex_vat')::numeric=(v_component->>'pay_ex_vat')::numeric
          and (e.value->>'charge_ex_vat')::numeric=(v_component->>'charge_ex_vat')::numeric;
      if v_match<>1 then raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_EXPENSE_NOT_EXACT'; end if;
    else
      if not private.weekly_exceptional_json_keys_exact_v1(v_component,array['kind','work_event_id','protected',
        'date','start','end','overnight','start_at_utc','end_at_utc','break_minutes','breaks','hours','rates',
        'charge_rates','pay_ex_vat','charge_ex_vat','source_provenance'])
        or not (v_component ?& array['kind','work_event_id','protected','date','start','end','overnight',
          'start_at_utc','end_at_utc','break_minutes','breaks','hours','rates','charge_rates','pay_ex_vat',
          'charge_ex_vat','source_provenance'])
        or pg_catalog.jsonb_typeof(v_component->'protected')<>'boolean'
        or pg_catalog.jsonb_typeof(v_component->'overnight')<>'boolean'
        or pg_catalog.jsonb_typeof(v_component->'breaks')<>'array'
        or pg_catalog.jsonb_array_length(v_component->'breaks')>100
        or v_component->>'start' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or v_component->>'end' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or (v_component->>'break_minutes')::integer<0 then
        raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_SHIFT_INVALID';
      end if;
      if (v_component->>'protected')::boolean then
        select pg_catalog.count(*)::integer into v_match from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
          where d.value->>'work_event_id'=v_component->>'work_event_id' and d.value->>'state'='WAIT'
            and d.value#>>'{fixed_schedule,date}'=v_component->>'date'
            and d.value#>>'{fixed_schedule,start}'=v_component->>'start'
            and d.value#>>'{fixed_schedule,end}'=v_component->>'end'
            and (d.value#>>'{fixed_schedule,break_mins}')::integer=(v_component->>'break_minutes')::integer;
        if v_match<>1 or v_component->'source_provenance'<>'null'::jsonb then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_SCHEDULE_NOT_APPROVED';
        end if;
      else
        select pg_catalog.count(*)::integer,pg_catalog.jsonb_agg(s.value)->0 into v_match,v_source
          from pg_catalog.jsonb_array_elements(v_context->'next_source_segments') s(value)
          where s.value#>>'{weekly_source,work_event_id}'=v_component->>'work_event_id'
            and s.value->>'date'=v_component->>'date' and s.value->>'start'=v_component->>'start'
            and s.value->>'end'=v_component->>'end' and s.value->>'overnight'=v_component->>'overnight'
            and (s.value->>'break_mins')::integer=(v_component->>'break_minutes')::integer
            and (s.value#>>'{weekly_source,pay_vector,total_pence}')::numeric/100=(v_component->>'pay_ex_vat')::numeric
            and (s.value#>>'{weekly_source,charge_vector,total_pence}')::numeric/100=(v_component->>'charge_ex_vat')::numeric
            and not exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
              where d.value->>'work_event_id'=v_component->>'work_event_id' and d.value->>'state' in ('WAIT','NOT_WORKED'));
        if v_match<>1 or v_component->'source_provenance' is distinct from pg_catalog.jsonb_build_object(
          'movement_id',v_source#>>'{weekly_source,movement_id}','final_revision_id',v_source#>>'{weekly_source,final_revision_id}',
          'row_resolution_id',v_source#>>'{weekly_source,row_resolution_id}','economic_snapshot_id',v_source#>>'{weekly_source,economic_snapshot_id}',
          'calculation_fingerprint',v_source#>>'{weekly_source,calculation_fingerprint}') then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_IMPORTED_SOURCE_NOT_EXACT';
        end if;
      end if;
      foreach v_bucket in array array['hours','rates','charge_rates'] loop
        if pg_catalog.jsonb_typeof(v_component->v_bucket)<>'object'
           or not private.weekly_exceptional_json_keys_exact_v1(v_component->v_bucket,array['day','night','sat','sun','bh'])
           or not ((v_component->v_bucket) ?& array['day','night','sat','sun','bh']) then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_BUCKET_INVALID';
        end if;
      end loop;
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        if pg_catalog.jsonb_typeof(v_component#>array['hours',v_bucket]) is distinct from 'string'
           or v_component#>>array['hours',v_bucket] !~ '^(0|[1-9][0-9]*)\.[0-9]{6}$'
           or pg_catalog.jsonb_typeof(v_component#>array['rates',v_bucket]) not in ('string','null')
           or pg_catalog.jsonb_typeof(v_component#>array['charge_rates',v_bucket]) not in ('string','null')
           or (v_component#>array['rates',v_bucket]<>'null'::jsonb
             and v_component#>>array['rates',v_bucket] !~ '^-?(0|[1-9][0-9]*)\.[0-9]{6}$')
           or (v_component#>array['charge_rates',v_bucket]<>'null'::jsonb
             and v_component#>>array['charge_rates',v_bucket] !~ '^-?(0|[1-9][0-9]*)\.[0-9]{6}$') then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_DECIMAL_TEXT_REQUIRED';
        end if;
        v_hours:=(v_component#>>array['hours',v_bucket])::numeric;
        v_rate:=(v_component#>>array['rates',v_bucket])::numeric;
        v_charge_rate:=(v_component#>>array['charge_rates',v_bucket])::numeric;
        if pg_catalog.jsonb_typeof(v_component#>array['hours',v_bucket])<>'string'
           or v_hours is null or v_hours<0 or v_hours<>pg_catalog.round(v_hours,6)
           or ((v_component->>'protected')::boolean and v_hours<>0 and v_rate is null)
           or (v_rate is not null and v_rate<>pg_catalog.round(v_rate,6))
           or (v_charge_rate is not null and v_charge_rate<>pg_catalog.round(v_charge_rate,6)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_USED_RATE_INVALID';
        end if;
        if not (v_component->>'protected')::boolean then
          if v_hours is distinct from (v_source#>>array['weekly_source','pay_vector','hours',v_bucket])::numeric
             or v_rate is distinct from (v_source#>>array['weekly_source','pay_vector','rates',v_bucket])::numeric
             or v_charge_rate is distinct from (v_source#>>array['weekly_source','charge_vector','rates',v_bucket])::numeric then
            raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_IMPORTED_RATE_NOT_EXACT';
          end if;
        else
          if v_hours<>0 and not exists(select 1 from private.bpay_next_contract_rate_rows_v1(
            v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
            v_contract.mileage_pay_rate,v_contract.mileage_charge_rate) r where r.rate_family='STANDARD'
              and r.rate_code=upper(v_bucket) and v_rate=case upper(v_contract.pay_method_snapshot)
                when 'PAYE' then r.paye_rate else r.umbrella_rate end and v_charge_rate=r.charge_rate) then
            raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_CALCULATED_RATE_NOT_EXACT';
          end if;
        end if;
      end loop;
      v_n:=0;
      for v_break in select value from pg_catalog.jsonb_array_elements(v_component->'breaks') loop
        if not private.weekly_exceptional_json_keys_exact_v1(v_break,array['start','end','minutes'])
           or not (v_break ?& array['start','end','minutes'])
           or v_break->>'start' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or v_break->>'end' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or (v_break->>'minutes')::integer<=0 then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_BREAK_INVALID';
        end if;
        if (v_break->>'minutes')::integer is distinct from
          ((extract(epoch from ((v_break->>'end')::time-(v_break->>'start')::time))/60)::integer
            +case when (v_break->>'end')::time<(v_break->>'start')::time then 1440 else 0 end) then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_BREAK_DURATION_NOT_EXACT';
        end if;
        v_n:=v_n+(v_break->>'minutes')::integer;
      end loop;
      if v_n<>0 and v_n<>(v_component->>'break_minutes')::integer then
        raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_BREAK_NOT_EXACT';
      end if;
    end if;
    v_total:=v_total+(v_component->>'pay_ex_vat')::numeric;
  end loop;
  if v_evidence is not null then
    select t.* into strict v_evidence_row from public.timesheets t where t.timesheet_id=v_evidence
      and t.contract_id=v_family.contract_id and t.week_ending_date=v_family.week_ending_date
      and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum and t.is_current
      and t.revoked_at is null and t.archived_at_utc is null for key share;
    v_signed:=private.weekly_exceptional_candidate_signed_evidence_v1(v_evidence);
  end if;
  select coalesce(pg_catalog.array_agg(i.id order by i.id),'{}'::uuid[]) into v_issue_ids
    from public.weekly_discrepancy_incidents i where i.source_group_id=v_group.id
      and i.work_event_id=v_event and i.candidate_id=v_family.candidate_id and i.client_id=v_contract.client_id;
  v_issue_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_ISSUE_SET_V1',pg_catalog.to_jsonb(v_issue_ids));
  v_schedule_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SCHEDULE_V1',p_request->'protected_schedule');
  v_source_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',p_request->'source_proposal');
  v_policy_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',pg_catalog.jsonb_build_object(
    'contract_id',v_contract.id,'rates_json',v_contract.rates_json,'policy',v_policy,'classification',p_request->'rate_classification'));
  v_target_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',p_request->'target_snapshot');
  v_approval_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_APPROVAL_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'work_event_id',v_event,'schedule_hash',pg_catalog.encode(v_schedule_hash,'hex'),
    'source_hash',pg_catalog.encode(v_source_hash,'hex'),'policy_hash',pg_catalog.encode(v_policy_hash,'hex'),
    'target_hash',pg_catalog.encode(v_target_hash,'hex'),'actor_user_id',v_actor,'reason',v_reason));
  if v_family.current_generation_id is null then
    if v_run.request_kind<>'APPROVE' then raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_INITIAL_ACTION_INVALID'; end if;
    v_prior_vector:=pg_catalog.jsonb_build_object('schema_version','WEEKLY_PROTECTED_NEXT_TARGET_V1','components','[]'::jsonb);
    v_generation_reason:='INITIAL_APPROVAL';
  else
    select g.complete_next_vector_json into strict v_prior_vector from public.weekly_exceptional_pay_generations g
      where g.id=v_family.current_generation_id and g.family_id=v_family.id and g.lifecycle_state='PUBLISHED' for share;
    v_generation_reason:=case v_run.request_kind when 'AMEND' then 'OFFICE_AMENDMENT' else v_run.request_kind end;
  end if;
  v_prior_vector_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_prior_vector);
  v_state:=case when v_run.request_kind in ('APPROVE','AMEND') then 'WAIT'
    when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE' else 'NOT_WORKED' end;
  v_lifecycle:=case when exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
      where d.value->>'state'='WAIT') then 'WAITING_SOURCE' when v_state='NOT_WORKED' then 'NOT_WORKED' else 'RECONCILED' end;
  insert into public.weekly_exceptional_payment_approvals(pay_target_family_id,work_event_id,evidence_timesheet_id,
    candidate_id,client_id,contract_id,week_ending,protected_work_date,protected_start_at_local,protected_end_at_local,
    protected_break_minutes,signed_submission_timesheet_id,signed_submission_revision,signed_submission_hash,signed_at_utc,
    contributing_issue_episode_ids,contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
    contract_rate_policy_source_fingerprint,approved_by_user_id,approval_reason,source_cycle_id,comparison_revision_id,
    final_revision_id,approved_target_pay_components_json,approved_target_gross,creation_orchestration_run_id,
    approval_hash,creation_idempotency_key)
  values(v_family.id,v_event,v_evidence,v_family.candidate_id,v_contract.client_id,v_contract.id,v_family.week_ending_date,
    v_work_date,(p_request#>>'{protected_schedule,start_at_local}')::timestamp,(p_request#>>'{protected_schedule,end_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,break_minutes}')::integer,v_evidence,(v_signed->>'timesheet_version')::integer,
    case when v_evidence is null then null else private.weekly_exceptional_hex_sha256_v1(v_signed->>'signature_sha256') end,
    (v_signed->>'signed_at_utc')::timestamptz,
    v_issue_ids,v_issue_hash,v_schedule_hash,v_policy_hash,v_actor,v_reason,v_cycle.id,
    nullif(p_request->>'current_comparison_revision_id','')::uuid,nullif(p_request->>'current_final_revision_id','')::uuid,
    p_request->'target_snapshot',v_total,v_run.id,v_approval_hash,v_key||':approval') returning * into v_approval;
  insert into public.weekly_exceptional_pay_generations(family_id,generation_number,prior_generation_id,prior_generation_hash,
    request_idempotency_key,reason,complete_prior_vector_json,complete_prior_vector_hash,complete_next_vector_json,
    complete_next_vector_hash,fixed_target_source_state_fingerprint,lifecycle_state)
  values(v_family.id,v_family.current_generation_number+1,v_family.current_generation_id,
    case when v_family.current_generation_id is null then null else v_prior_vector_hash end,v_key||':generation',
    v_generation_reason,v_prior_vector,v_prior_vector_hash,p_request->'target_snapshot',v_target_hash,v_source_hash,'READY_TO_STAGE')
  returning * into v_generation;
  -- Existing UNIQUE(family_id,event_sequence) indexes provide a one-row
  -- backward tail probe while the exact family mutex is held; no history aggregate.
  select e.event_sequence,e.event_hash into v_event_sequence,v_prior_event_hash
    from public.weekly_exceptional_pay_family_events e where e.family_id=v_family.id
    order by e.event_sequence desc limit 1;
  v_event_sequence:=coalesce(v_event_sequence,0)+1;
  v_event_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_FAMILY_EVENT_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'event_sequence',v_event_sequence,'work_event_id',v_event,'approval_id',v_approval.id,
    'target_vector_hash',pg_catalog.encode(v_target_hash,'hex'),'prior_event_hash',pg_catalog.encode(v_prior_event_hash,'hex')));
  insert into public.weekly_exceptional_pay_family_events(family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,source_proposal_snapshot_json,source_proposal_hash,
    fixed_office_target_snapshot_json,fixed_office_target_hash,state,current_comparison_revision_id,current_final_revision_id,
    office_actor_user_id,office_reason,prior_event_hash,event_hash)
  values(v_family.id,v_event_sequence,v_event,v_approval.id,v_work_date,v_approval.protected_start_at_local,
    v_approval.protected_end_at_local,v_approval.protected_break_minutes,p_request->'rate_classification',p_request->'source_proposal',
    v_source_hash,p_request->'protected_schedule',v_schedule_hash,v_state,v_approval.comparison_revision_id,v_approval.final_revision_id,
    v_actor,v_reason,v_prior_event_hash,v_event_hash) returning id into v_family_event_id;
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_protected_current_decision_capture_v1(v_family_event_id);
  end if;
  select e.event_sequence,e.event_hash into v_target_sequence,v_prior_target_hash
    from public.weekly_exceptional_pay_target_events e where e.family_id=v_family.id
    order by e.event_sequence desc limit 1;
  v_target_sequence:=coalesce(v_target_sequence,0)+1;
  v_audit_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_TARGET_EVENT_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'approval_id',v_approval.id,'generation_id',v_generation.id,'prior_hash',pg_catalog.encode(v_prior_target_hash,'hex'),
    'target_hash',pg_catalog.encode(v_target_hash,'hex')));
  insert into public.weekly_exceptional_pay_target_events(family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,fixed_target_component_snapshot,current_source_proposal_snapshot,
    complete_prior_family_vector_fingerprint,complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key)
  values(v_family.id,v_approval.id,v_target_sequence,v_approval.comparison_revision_id,v_approval.final_revision_id,v_prior_target_hash,
    p_request->'target_snapshot',p_request->'source_proposal',v_prior_vector_hash,v_target_hash,v_generation_reason,v_lifecycle,
    v_generation.id,v_actor,v_audit_hash,v_key||':target-event');
  update public.weekly_exceptional_pending_reconciliation_targets t set state=case when v_run.request_kind='WITHDRAW'
    then 'WITHDRAWN' else 'CONSUMED' end,completed_at_utc=pg_catalog.transaction_timestamp()
    where t.family_id=v_family.id and t.durable_work_event_id=v_event and t.state='ACTIVE';
  if v_state='WAIT' then
    insert into public.weekly_exceptional_pending_reconciliation_targets(family_id,approval_id,durable_work_event_id,incident_id,
      current_final_revision_id,intended_outcome,source_action_policy_target_fingerprint,state)
    values(v_family.id,v_approval.id,v_event,(select i.id from public.weekly_discrepancy_incidents i
      where i.source_group_id=v_group.id and i.work_event_id=v_event order by i.episode_number desc limit 1),
      v_approval.final_revision_id,'WAIT',v_source_hash,'ACTIVE');
  end if;
  select s.* into strict v_stage from private.bpay_next_stage_protected_source_v1(
    v_approval.id,v_generation.id,v_root.timesheet_id,v_components) s;
  -- All Source/family/work/revision locks precede the agency clock receipt.
  select p.* into strict v_publication from private.bpay_next_publish_source_staged_pair_v1(
    array[v_stage.work_id],array[v_stage.revision_id],array[v_approval.id]) p;
  if v_family.current_generation_id is not null then
    update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
      superseded_by_generation_id=v_generation.id,superseded_at_utc=pg_catalog.transaction_timestamp()
      where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
  end if;
  update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
    published_at_utc=pg_catalog.transaction_timestamp(),result_hash=v_request_hash where id=v_generation.id;
  update public.weekly_exceptional_pay_target_families set ownership_state='TARGET_MANAGED',target_domain_version='NEXT_V1',
    current_generation_id=v_generation.id,current_generation_number=v_generation.generation_number,
    current_complete_target_vector_hash=v_target_hash,current_source_proposal_hash=v_source_hash,
    current_lifecycle_state=v_lifecycle,current_component_count=pg_catalog.jsonb_array_length(v_components),
    bound_version=bound_version+1 where id=v_family.id;
  update public.weekly_exceptional_orchestration_runs set state='COMPLETE',after_state_fingerprint=v_request_hash,
    completed_at_utc=pg_catalog.transaction_timestamp() where id=v_run.id;
  insert into private.bpay_next_protected_source_receipt(orchestration_run_id,family_id,actor_user_id,approval_id,generation_id,
    work_id,revision_id,command_id,agency_sequence,accepted_family_bound_version,prepared_request_sha256,
    publication_request_sha256,source_manifest_sha256)
  values(v_run.id,v_family.id,v_actor,v_approval.id,v_generation.id,v_stage.work_id,v_stage.revision_id,v_publication.command_id,
    v_publication.agency_sequence,v_family.bound_version+1,v_run.request_fingerprint,v_request_hash,v_source_hash);
  insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key)
  values(v_family.id,v_approval.id,case when v_state='WAIT' then 'APPROVED' when v_state='NOT_WORKED' then 'RECORDED_NOT_WORKED'
    else 'MATCH_ACCEPTED' end,'NEXT_PUBLISHED',pg_catalog.jsonb_build_object('work_event_id',v_event,'generation_id',v_generation.id,
      'approval_id',v_approval.id,'revision_id',v_stage.revision_id,'command_id',v_publication.command_id,
      'approved_pay_ex_vat',v_total::text),v_key||':payment-event');
  select coalesce(max(s.sequence),0)+1 into v_step from public.weekly_exceptional_orchestration_steps s where s.orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,before_state_fingerprint,bounded_owner_response_json,
    owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
  values(v_run.id,v_step,'PUBLISH_NEXT',v_key,'public.weekly_exceptional_pay_publish_next_v1','jsonb->jsonb',v_request_hash,
    v_run.before_state_fingerprint,private.bpay_next_protected_receipt_json_v1(v_run.id,false),v_request_hash,v_request_hash,
    'COMPLETE',pg_catalog.transaction_timestamp());
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_workbench_refresh_work_v1(v_stage.work_id);
  end if;
  return private.bpay_next_protected_receipt_json_v1(v_run.id,false);
exception when no_data_found or too_many_rows then
  raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SCOPE_INVALID';
end
$function$;

create or replace function public.weekly_exceptional_pay_complete_local_v1(p_request jsonb)
returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
set lock_timeout to '2500ms'
set statement_timeout to '30s'
as $function$
declare
  v_keys constant text[]:=array['schema_version','actor_user_id','publication_request_id',
    'expected_request_sha256','idempotency_key'];
  v_actor uuid;
  v_request_id uuid;
  v_hash bytea;
  v_key text;
  v_request public.weekly_exceptional_c1_publication_requests%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_root public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
  v_locks jsonb;
  v_snapshot jsonb;
  v_result jsonb;
  v_write jsonb;
  v_proposal jsonb;
  v_components jsonb;
  v_expenses jsonb;
  v_head_request jsonb;
  v_target_state text;
  v_source_hash bytea;
  v_result_hash bytea;
  v_live_authorisations integer;
  v_bundle_id uuid;
  v_local_context jsonb;
  v_local_origin jsonb;
  v_pending boolean:=false;
  v_manual_review_id uuid;
  v_rate_policy_hash bytea;
  v_scope_date date; v_scope_group uuid; v_scope_client uuid;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_request_id:=(p_request->>'publication_request_id')::uuid;
  v_hash:=private.weekly_exceptional_hex_sha256_v1(p_request->>'expected_request_sha256');
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_actor is null or v_request_id is null or pg_catalog.char_length(v_key) not between 16 and 240
     or not exists(select 1 from public.tms_users actor where actor.id=v_actor and actor.is_active
                    and (actor.payment_authoriser or actor.payment_golden_key)) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='42501';
  end if;

  -- Exact committed receipt precedes freshness checks, locks and publication.
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  -- The retained action's own report cutoff remains the Office authority,
  -- including exact replay. Work date and a later unrelated upload are not it.
  select c.finalisation_week_ending,c.source_group_id,a.client_id
    into strict v_scope_date,v_scope_group,v_scope_client
    from public.weekly_exceptional_c1_publication_requests r
    join public.weekly_exceptional_payment_approvals a
      on a.creation_orchestration_run_id=r.orchestration_run_id and a.pay_target_family_id=r.family_id
    join public.weekly_source_cycles c on c.id=a.source_cycle_id
    where r.id=v_request_id;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
    v_scope_group,v_scope_client,v_scope_date);
  -- SELECT INTO above changes FOUND; test the retained receipt identity itself.
  if v_receipt.publication_request_id is not null then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests where id=v_request_id;
  -- I-1: Candidate serial gate, then all canonical family members, then rows.
  v_locks:=private.weekly_source_lock_and_resolve_families_v1(v_request.candidate_id,
    array[v_request.root_timesheet_id],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    v_request.id,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if coalesce((v_locks->>'ok')::boolean,false) is not true then
    return v_locks||jsonb_build_object('ok',false);
  end if;
  select * into v_receipt from private.weekly_source_local_protected_decision_receipts
    where publication_request_id=v_request_id;
  if found then
    if v_receipt.actor_user_id is distinct from v_actor or v_receipt.request_sha256 is distinct from v_hash
       or v_receipt.idempotency_key is distinct from v_key or v_receipt.state='PREPARING' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_REPLAY_CONFLICT' using errcode='55000';
    end if;
    return v_receipt.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  select * into strict v_family from public.weekly_exceptional_pay_target_families
    where id=v_request.family_id for update;
  select * into strict v_request from public.weekly_exceptional_c1_publication_requests
    where id=v_request_id for update;
  select * into strict v_generation from public.weekly_exceptional_pay_generations
    where id=v_request.generation_id and family_id=v_family.id for update;
  select * into strict v_run from public.weekly_exceptional_orchestration_runs
    where id=v_request.orchestration_run_id and family_id=v_family.id for update;
  select * into strict v_approval from public.weekly_exceptional_payment_approvals
    where creation_orchestration_run_id=v_run.id and pay_target_family_id=v_family.id for share;
  select * into strict v_root from public.timesheets where timesheet_id=v_family.root_timesheet_id for update;
  select * into strict v_fin from public.timesheets_financials
    where timesheet_id=v_root.timesheet_id and is_current for update;
  if v_hash is distinct from v_request.request_sha256 or v_run.requested_by_user_id is distinct from v_actor
     or v_request.state<>'READY' or v_run.state<>'RUNNING' or v_generation.lifecycle_state<>'PENDING_C1'
     or v_request.c1_operation_id is not null or v_request.c1_publication_id is not null
     or v_request.c1_head_revision is not null or v_request.c1_receipt_sha256 is not null
     or exists(select 1 from public.weekly_exceptional_c1_publication_checkpoints c where c.publication_request_id=v_request.id)
     or exists(select 1 from public.weekly_exceptional_c1_unknown_outcomes u
                where u.publication_request_id=v_request.id and u.state='RECOVERY_REQUIRED')
     or v_family.current_generation_id is distinct from v_generation.prior_generation_id
     or v_family.current_generation_number+1 is distinct from v_generation.generation_number
     or v_fin.id is distinct from v_request.financial_row_id
     or not v_root.is_current or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_root.contract_id is distinct from v_family.contract_id
     or v_root.week_ending_date is distinct from v_family.week_ending_date
     or v_root.version is distinct from (v_generation.complete_next_vector_json#>>'{target_snapshot,tsfin_snapshot_json,timesheet_version}')::integer
     or v_approval.withdrawn_at_utc is not null
     or exists(select 1 from private.weekly_source_local_protected_decision_receipts r
               where r.family_id=v_family.id and r.state='PENDING_FREEZE') then
    raise exception 'WEEKLY_PROTECTED_LOCAL_STATE_CHANGED' using errcode='55000';
  end if;
  select target.resulting_lifecycle_state into strict v_target_state
    from public.weekly_exceptional_pay_target_events target
    where target.financial_generation_id=v_generation.id and target.family_id=v_family.id;
  select event.source_proposal_hash into strict v_source_hash
    from public.weekly_exceptional_pay_family_events event
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  select private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',
    jsonb_build_object('contract_id',v_family.contract_id,'rates_json',contract.rates_json,
      'policy',private._weekly_source_effective_policy_v1(v_approval.client_id,v_family.contract_id,
        v_approval.protected_work_date),'classification',event.rate_classification_json))
    into strict v_rate_policy_hash
    from public.weekly_exceptional_pay_family_events event
    join public.contracts contract on contract.id=v_family.contract_id
    where event.evidence_approval_id=v_approval.id and event.family_id=v_family.id;
  if v_rate_policy_hash is distinct from v_approval.contract_rate_policy_source_fingerprint then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RATE_POLICY_CHANGED' using errcode='40001';
  end if;
  v_snapshot:=v_generation.complete_next_vector_json#>'{target_snapshot,tsfin_snapshot_json}';
  if jsonb_typeof(v_snapshot) is distinct from 'object'
     or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
        v_generation.complete_next_vector_json) is distinct from v_generation.complete_next_vector_hash
     or v_snapshot is distinct from v_approval.approved_target_pay_components_json->'tsfin_snapshot_json' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_INVALID' using errcode='55000';
  end if;
  v_snapshot:=v_snapshot||jsonb_build_object('timesheet_id',v_root.timesheet_id::text,
    'timesheet_version',v_root.version,'actual_schedule_json',
    v_generation.complete_next_vector_json#>'{target_snapshot,actual_schedule_json}');
  -- Reconfirm the staged identity proof before either mutable first-approval
  -- TSFIN or the common HEAD writer. Immutable old requests without this new
  -- metadata retain their existing route; they are not silently remapped.
  if v_generation.complete_next_vector_json ? 'protected_component_identity_basis'
     or exists(select 1 from jsonb_array_elements(v_snapshot#>'{invoice_breakdown_json,segments}') r
       where r ? 'weekly_protected_component_identity') then
    if v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,schema_version}'
          is distinct from 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_V1'
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,family_id}'
          is distinct from v_family.id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,root_timesheet_id}'
          is distinct from v_root.timesheet_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,contract_id}'
          is distinct from v_family.contract_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,candidate_id}'
          is distinct from v_family.candidate_id::text
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,accepted_schedule_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_ACCEPTED_SCHEDULE_V1',v_snapshot->'actual_schedule_json'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,calculated_segments_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_CALCULATED_SEGMENTS_V1',v_snapshot#>'{invoice_breakdown_json,segments}'),'hex')
       or v_generation.complete_next_vector_json#>>'{protected_component_identity_basis,approved_components_sha256}'
          is distinct from encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_COMPONENT_APPROVED_COMPONENTS_V1',
            case when v_generation.complete_next_vector_json ? 'prior_effective_inventory'
              then v_generation.complete_next_vector_json#>'{prior_effective_inventory,components}'
              else '[]'::jsonb end),'hex') then
      raise exception 'WEEKLY_PROTECTED_COMPONENT_MANIFEST_UNAVAILABLE' using errcode='55000'; end if;
  end if;
  select count(*) into v_live_authorisations from public.weekly_source_root_authorisations authority
    where authority.root_timesheet_id=v_root.timesheet_id and authority.withdrawn_at_utc is null;
  -- Exact accepted replay has already returned. Before admitting a NEW
  -- authorised decision, hold the existing owner share lock and require the
  -- same actual NEXT Local writer capabilities as the winning common core.
  -- Otherwise an old freeze could park a decision that cannot later publish.
  -- First Save remains the unauthorised TSFIN path and needs no Bank owner.
  if v_live_authorisations=1 and v_root.authorised_at_server is not null then
    if pg_catalog.to_regclass('private.bpay_next_module_control') is null
       or pg_catalog.to_regprocedure('private.bpay_next_legacy_callback_enabled_v1()') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
    perform private.bpay_next_legacy_callback_enabled_v1();
    if (select active_owner from private.bpay_next_module_control where id=1) is distinct from 'NEXT'
       or pg_catalog.to_regprocedure(
         'private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)') is null
       or pg_catalog.to_regprocedure(
         'private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)') is null
       or pg_catalog.to_regprocedure(
         'private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)') is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PUBLICATION_OWNER_NOT_READY' using errcode='55000';
    end if;
  end if;
  insert into private.weekly_source_local_protected_decision_receipts(
    publication_request_id,generation_id,family_id,root_timesheet_id,actor_user_id,
    request_sha256,idempotency_key,preparing_transaction_id,prior_financial_id,approved_snapshot_json,state
  ) values(v_request.id,v_generation.id,v_family.id,v_root.timesheet_id,v_actor,
    v_hash,v_key,pg_current_xact_id(),v_fin.id,v_snapshot,'PREPARING');
  if v_live_authorisations=0 and v_root.authorised_at_server is null then
    -- 24 section 4.1: mutable unauthorised TSFIN, not a fabricated Authorise.
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_snapshot->>'processing_status' is distinct from 'PENDING_AUTH'
       or nullif(v_snapshot->>'authorised_at_utc','') is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
    end if;
    v_write:=public.tsfin_write_current_snapshot_single_bounded(v_root.timesheet_id,
      v_root.version,v_snapshot,v_actor,statement_timestamp());
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SNAPSHOT_WRITE_REFUSED' using errcode='55000';
    end if;
    -- V4 permits display of this confirmed decision before first Authorise,
    -- but never admission to Banking. Seal the actual written row, not the
    -- proposed input or an hours-only total. Old receipts are not backfilled.
    select * into strict v_fin from public.timesheets_financials financial
      where financial.id=(v_write->>'timesheet_financials_id')::uuid
        and financial.timesheet_id=v_root.timesheet_id and financial.is_current;
    if v_fin.authorised_at_utc is not null or v_fin.processing_status<>'PENDING_AUTH'
       or v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    update private.weekly_source_local_protected_decision_receipts
      set approved_snapshot_json=v_snapshot||jsonb_build_object('saved_unauthorised_basis',
        jsonb_build_object('schema_version','SAVED_UNAUTHORISED_LOCAL_V1',
          'financial_snapshot_id',v_fin.id,
          'financial_snapshot_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_FINANCIAL_V1',to_jsonb(v_fin)),'hex'),
          'detail_sha256',encode(private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_SAVED_UNAUTHORISED_DETAIL_V1',jsonb_build_object(
              'segments',v_fin.invoice_breakdown_json->'segments',
              'actual_schedule',v_fin.actual_schedule_json,
              'additional_units',v_fin.additional_units_json)),'hex'),
          'inventory_sha256',encode(v_generation.complete_next_vector_hash,'hex'),
          'root_version',v_root.version,'family_booking_id',v_root.booking_id))
      where publication_request_id=v_request.id and state='PREPARING'
        and preparing_transaction_id=pg_current_xact_id();
    if not found then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SAVED_BASIS_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','UNAUTHORISED_TSFIN',
      'timesheet_financials_id',v_write->'timesheet_financials_id','requires_first_authorisation',true);
  elsif v_live_authorisations=1 and v_root.authorised_at_server is not null then
    -- Already authorised: the only later entitlement writer is the existing
    -- common-head publisher. Never rotate TSFIN, change Drafts or invoke C1.
    -- The real stage advances its own family bound version after sealing the
    -- old entitlement. The factual context proves that exact stage and the
    -- unchanged whole inventory, rather than treating the own increment as
    -- an unrelated intervening financial change.
    v_local_context:=private.weekly_source_local_publication_context_v2(v_request.id,v_generation.id,v_root.timesheet_id);
    if v_local_context is null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_SOURCE_QUALIFICATION_UNAVAILABLE' using errcode='55000'; end if;
    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,
      'source_observation_kind',expense.source_observation_kind,
      'candidate_reimbursement_ex_vat',expense.candidate_reimbursement_ex_vat,
      'client_charge_ex_vat',expense.client_charge_ex_vat) order by expense.work_event_id),'[]'::jsonb)
      into v_expenses from (
        select authority.*,row_number() over(partition by authority.work_event_id
          order by cycle.finalisation_week_ending desc,revision.finalised_at_utc desc,
            authority.generation desc,authority.id desc) event_rank
        from public.weekly_expense_authority_generations authority
        join public.weekly_source_final_revisions revision on revision.id=authority.final_revision_id and revision.state='CURRENT'
        join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
        join public.weekly_source_cycles action_cycle on action_cycle.id=v_approval.source_cycle_id
        where authority.contract_id=v_family.contract_id and authority.state='CURRENT'
          and cycle.source_group_id=action_cycle.source_group_id
          and exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
            where lineage.work_event_id=authority.work_event_id and lineage.contract_id=authority.contract_id
              and lineage.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(v_root.timesheet_id)))
      ) expense where expense.event_rank=1 and expense.source_expense_pence>0;
    v_components:=private.weekly_source_entitlement_components_v1(
      v_snapshot#>'{invoice_breakdown_json,segments}',v_expenses);
    -- Reuse the existing pure approved-additional mapper on the sealed
    -- calculator snapshot. It validates exact saved units/rates/totals and
    -- booking-family identity; it does not read or reprice live financial data.
    v_components:=private.weekly_source_initial_additional_components_v2(v_components,
      v_snapshot->'additional_units_json',
      (v_snapshot->>'additional_pay_ex_vat')::numeric,
      (v_snapshot->>'additional_charge_ex_vat')::numeric,v_root.booking_id);
    -- Source expenses must be the same approved observations and money, not
    -- a newer expense silently composed into an older protected decision.
    if (select coalesce(sum((item.value->>'pay_ex_vat')::numeric)
        filter(where not (item.value->>'exclude_from_pay')::boolean),0)
        from jsonb_array_elements(v_components) item(value)) is distinct from v_approval.approved_target_gross
       or (select coalesce(jsonb_agg(jsonb_build_object('code',upper('SOURCE_SUPPLIED:'||(item.value->>'work_event_id')),
          'pay',(item.value->>'candidate_reimbursement_ex_vat')::numeric,
          'charge',(item.value->>'client_charge_ex_vat')::numeric) order by item.value->>'work_event_id'),'[]'::jsonb)
          from jsonb_array_elements(v_expenses) item(value)) is distinct from
          (select coalesce(jsonb_agg(jsonb_build_object('code',upper(component.expense_code),
            'pay',component.pay_ex_vat,'charge',component.charge_ex_vat)
            order by component.expense_code),'[]'::jsonb)
            from public.weekly_exceptional_c1_component_records component
            where component.publication_request_id=v_request.id and component.component_kind='EXPENSE') then
      raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_ENTITLEMENT_CHANGED' using errcode='40001';
    end if;
    v_local_origin:=private.weekly_source_local_origin_canonical_v2(jsonb_build_object(
      'origin_kind','PROTECTED_LOCAL_DECISION_V1','publication_request_id',v_request.id,
      'generation_id',v_generation.id,'request_sha256',encode(v_request.request_sha256,'hex'),
      'source_qualification_digest',v_local_context->'source_qualification_digest',
      'policy_fingerprint',v_local_context->'policy_fingerprint','before_origin',v_local_context->'before_origin',
      'before_inventory_digest',v_local_context->'before_inventory_digest'));
    v_bundle_id:=pg_catalog.gen_random_uuid();
    update private.weekly_source_local_protected_decision_receipts set approved_snapshot_json=v_snapshot||jsonb_build_object(
      'common_decision_bundle_id',v_bundle_id,
      'local_source_qualification',v_local_context->'qualification','local_publication_origin',v_local_origin,
      'common_components_digest',encode(private.weekly_source_publication_request_digest_v1(
        (select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(item.value,'protected.component')
          order by item.ordinality),'[]'::jsonb) from jsonb_array_elements(v_components) with ordinality item(value,ordinality))),'hex')),
      common_decision_bundle_id=v_bundle_id,common_bundle_revision=1,
      publication_origin_kind='PROTECTED_LOCAL_DECISION_V1',
      publication_origin_digest=private.weekly_source_publication_request_digest_v1(v_local_origin),
      source_qualification_digest=decode(v_local_context->>'source_qualification_digest','hex')
      where publication_request_id=v_request.id and state='PREPARING' and preparing_transaction_id=pg_current_xact_id();
    if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_ORIGIN_SEAL_REFUSED' using errcode='55000'; end if;
    v_head_request:=private.weekly_source_entitlement_proposal_request_v2(v_root.timesheet_id,
      v_local_origin,'PROTECTED',v_bundle_id,1,pg_catalog.gen_random_uuid(),
      pg_catalog.gen_random_uuid(),v_components);
    v_proposal:=private.weekly_source_entitlement_proposal_record_v1(v_head_request,v_family.agency_id,
      v_family.contract_id,v_family.week_ending_date,v_actor);
    if coalesce((v_proposal->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_REFUSED' using errcode='55000';
    end if;
    v_write:=private.weekly_source_entitlement_publish_immediate_v1(v_head_request);
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception '%',coalesce(v_write->>'code','WEEKLY_PROTECTED_LOCAL_HEAD_REFUSED') using errcode='55000';
    end if;
    v_pending:=coalesce((v_write->>'published')::boolean,false) is not true;
    if v_pending and v_write->>'code'<>'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE' then
      raise exception 'WEEKLY_PROTECTED_LOCAL_HEAD_RESULT_INVALID' using errcode='55000';
    end if;
    v_result:=jsonb_build_object('authority','COMMON_ENTITLEMENT_HEAD',
      'decision_bundle_id',v_bundle_id,'publication',v_write,'requires_first_authorisation',false);
  else
    raise exception 'WEEKLY_PROTECTED_LOCAL_AUTHORISATION_CONTRADICTION' using errcode='55000';
  end if;

  v_result:=v_result||jsonb_build_object('ok',true,'outcome',case when v_pending
    then 'SAVED_PENDING_FREEZE' else 'PUBLISHED' end,'idempotent_replay',false,
    'family_id',v_family.id,'generation_id',v_generation.id,'publication_request_id',v_request.id,
    'state',case when v_pending then 'PENDING' else 'LIVE' end,
    'family_bound_version',v_family.bound_version+1);
  v_result_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_LOCAL_RESULT_V1',v_result);
  if not v_pending then
    if v_family.current_generation_id is not null then
      update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
        superseded_by_generation_id=v_generation.id,superseded_at_utc=statement_timestamp()
        where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
      if not found then raise exception 'WEEKLY_PROTECTED_LOCAL_PRIOR_GENERATION_INVALID' using errcode='55000'; end if;
    end if;
    update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
      published_at_utc=statement_timestamp(),result_hash=v_result_hash where id=v_generation.id;
    update public.weekly_exceptional_pay_target_families set current_generation_id=v_generation.id,
      current_generation_number=v_generation.generation_number,
      current_complete_target_vector_hash=v_generation.complete_next_vector_hash,
      current_source_proposal_hash=v_source_hash,current_lifecycle_state=v_target_state,
      current_component_count=v_request.expected_component_count,c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  else
    -- Previous effective family/head remains current while the existing frozen
    -- bundle owner holds the newly approved decision. No pretend live receipt.
    update public.weekly_exceptional_pay_target_families set c1_publication_state='NONE',
      bound_version=bound_version+1 where id=v_family.id;
  end if;
  update public.weekly_exceptional_c1_publication_requests set state='RETIRED',
    typed_result_json=v_result,completed_at_utc=statement_timestamp() where id=v_request.id;
  update public.weekly_exceptional_orchestration_runs set state='COMPLETE',
    after_state_fingerprint=v_result_hash,completed_at_utc=statement_timestamp() where id=v_run.id;
  update private.weekly_source_local_protected_decision_receipts set state=case when v_pending
    then 'PENDING_FREEZE' else 'COMPLETE' end,result_json=v_result,completed_at_utc=statement_timestamp()
    where publication_request_id=v_request.id;
  -- Finish the existing manual pay query atomically with its approved decision.
  -- The UI needs no second query command: only this accepted publication can
  -- clear the hold, using its exact retained approval and generation evidence.
  select review.id into v_manual_review_id from private.weekly_source_manual_reviews review
    join public.weekly_source_cycles cycle on cycle.id=v_approval.source_cycle_id
    where review.source_group_id=cycle.source_group_id
      and review.work_event_id=v_approval.work_event_id and review.state='OPEN'
      and review.contract_id=v_family.contract_id and review.candidate_id=v_family.candidate_id;
  insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,
    lifecycle_view,bounded_payload_json,idempotency_key) values(v_family.id,v_approval.id,
    'PROTECTED_STATE_OBSERVED',case when v_pending then 'PENDING' else 'LIVE' end,v_result,v_key||':payment-event');
  insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,
    step_kind,idempotency_key,allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,
    before_state_fingerprint,bounded_owner_response_json,owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
    select v_run.id,coalesce(max(step.sequence),0)+1,'COMPLETE_LOCAL_PROTECTED_DECISION',v_key,
      'CloudTMS.weekly_exceptional_pay_complete_local_v1','jsonb',v_hash,v_run.before_state_fingerprint,
      v_result,v_result_hash,v_result_hash,case when v_pending then 'PENDING' else 'COMPLETE' end,statement_timestamp()
    from public.weekly_exceptional_orchestration_steps step where step.orchestration_run_id=v_run.id;
  -- Finish the actual completion evidence before any query resolution or
  -- accepted-status read, in this same outer transaction; a refusal rolls back
  -- the decision, publication, completion step and query together.
  if v_manual_review_id is not null and v_run.request_kind in ('APPROVE','AMEND') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','PROTECTED_PAY',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  elsif v_manual_review_id is not null and v_run.request_kind in ('WITHDRAW','RECONCILE') then
    perform public.weekly_source_manual_review_resolve_v1(jsonb_build_object('actor_user_id',v_actor,
      'review_id',v_manual_review_id,'resolution_kind','OFFICE_ACCEPTED_SOURCE',
      'command_id',v_request.id,'accepted_approval_id',v_approval.id,
      'accepted_generation_id',v_generation.id));
  end if;
  -- Accepted pending approval is not publication. The existing release
  -- trigger retires its contacts only after the deferred receipt is proved.
  if not v_pending then
    perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);
  end if;
  if v_manual_review_id is null or v_run.request_kind not in ('APPROVE','AMEND','WITHDRAW','RECONCILE') then
    perform private.bpay_next_source_query_refresh_v1(v_family.contract_id,v_approval.work_event_id);
  end if;
  return v_result;
exception when no_data_found or too_many_rows then
  raise exception 'WEEKLY_PROTECTED_LOCAL_COMPLETE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_exceptional_pay_wait_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_keys constant text[]:=array[
    'schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','work_event_id','expected_family_bound_version',
    'expected_target_vector_sha256','source_proposal','protected_schedule',
    'reason','idempotency_key'
  ];
  v_actor uuid;
  v_family_id uuid;
  v_run_id uuid;
  v_cycle_id uuid;
  v_event_id uuid;
  v_expected_version bigint;
  v_expected_hash bytea;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_prior_event public.weekly_exceptional_pay_family_events%rowtype;
  v_family_event_id uuid;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_sequence bigint;
  v_prior_hash bytea;
  v_event_hash bytea;
  v_target_sequence bigint;
  v_prior_target_hash bytea;
  v_target_hash bytea;
  v_source_hash bytea;
  v_after bytea;
  v_key text;
  v_completed_witness public.weekly_exceptional_orchestration_runs%rowtype;
  v_completed_replay boolean:=false;
  v_pending public.weekly_exceptional_pending_reconciliation_targets%rowtype;
  v_incident uuid;
  v_next_wait boolean;
begin
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys)
     or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_WAIT_V1'
     or pg_catalog.jsonb_typeof(p_request->'source_proposal')<>'object'
     or pg_catalog.jsonb_typeof(p_request->'protected_schedule')<>'object' then
    raise exception 'WEEKLY_PROTECTED_WAIT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_family_id:=(p_request->>'family_id')::uuid;
    v_run_id:=(p_request->>'orchestration_run_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_event_id:=(p_request->>'work_event_id')::uuid;
    v_expected_version:=(p_request->>'expected_family_bound_version')::bigint;
    v_expected_hash:=private.weekly_exceptional_hex_sha256_v1(
      p_request->>'expected_target_vector_sha256'
    );
  exception when others then
    raise exception 'WEEKLY_PROTECTED_WAIT_REQUEST_INVALID' using errcode='22023';
  end;
  v_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  -- SP-06: actual payment authority is rechecked in this final transaction, including replay.
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY');
  -- Prepare is a separate RPC. A fresh Wait must independently enter the
  -- shared admission boundary before acquiring any business-row locks.
  -- Only an already completed, no-write return may bypass that boundary.
  select run.* into v_completed_witness
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family_id
    and run.requested_by_user_id=v_actor and run.request_kind='WAIT'
    and run.state='COMPLETE' and run.completed_at_utc is not null
    and pg_catalog.isfinite(run.completed_at_utc)
    and pg_catalog.octet_length(run.after_state_fingerprint)=32
    and run.request_fingerprint is not null
    and run.before_state_fingerprint is not null;
  v_completed_replay:=found;
  if not v_completed_replay then
    perform private.weekly_source_pay_query_admit_v2();
  end if;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=v_family_id for update;
  select run.* into strict v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.id=v_run_id and run.family_id=v_family.id for update;
  if v_completed_replay and v_run is distinct from v_completed_witness then
    -- Never fall through to a write after skipping fresh admission.
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
  end if;
  if v_run.state='COMPLETE' then
    if v_run.request_kind<>'WAIT' or v_run.requested_by_user_id is distinct from v_actor
       or v_run.completed_at_utc is null or not pg_catalog.isfinite(v_run.completed_at_utc)
       or pg_catalog.octet_length(v_run.after_state_fingerprint) is distinct from 32
       or v_run.request_fingerprint is null or v_run.before_state_fingerprint is null then
      raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'outcome','WAITING_FOR_SOURCE','idempotent_replay',true,
      'family_id',v_family.id,'family_bound_version',v_family.bound_version
    );
  end if;
  if v_run.request_kind<>'WAIT' or v_run.state<>'RUNNING'
     or v_run.requested_by_user_id is distinct from v_actor
     or v_family.bound_version<>v_expected_version
     or v_family.current_complete_target_vector_hash is distinct from v_expected_hash
     or v_family.current_generation_id is null
     or pg_catalog.char_length(v_key) not between 16 and 240 then
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
  end if;
  select approval.* into strict v_approval
  from public.weekly_exceptional_payment_approvals approval
  where approval.pay_target_family_id=v_family.id and approval.work_event_id=v_event_id
  order by approval.approved_at_utc desc,approval.id desc limit 1;
  select family_event.* into strict v_prior_event
  from public.weekly_exceptional_pay_family_events family_event
  where family_event.family_id=v_family.id
    and family_event.durable_work_event_id=v_event_id
  order by family_event.event_sequence desc limit 1;
  select generation.* into strict v_generation
  from public.weekly_exceptional_pay_generations generation
  where generation.id=v_family.current_generation_id
    and generation.family_id=v_family.id
    and generation.lifecycle_state='PUBLISHED' for share;
  v_source_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',p_request->'source_proposal'
  );
  select event_sequence,event_hash into v_sequence,v_prior_hash
  from public.weekly_exceptional_pay_family_events where family_id=v_family.id
  order by event_sequence desc limit 1;
  v_sequence:=coalesce(v_sequence,0)+1;
  v_event_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FAMILY_EVENT_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_sequence,
      'work_event_id',v_event_id,'approval_id',v_approval.id,
      'schedule',p_request->'protected_schedule',
      'source_proposal_hash',pg_catalog.encode(v_source_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'prior_event_hash',case when v_prior_hash is null then null else pg_catalog.encode(v_prior_hash,'hex') end
    )
  );
  insert into public.weekly_exceptional_pay_family_events(
    family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
    source_proposal_snapshot_json,source_proposal_hash,fixed_office_target_snapshot_json,
    fixed_office_target_hash,state,current_comparison_revision_id,
    current_final_revision_id,office_actor_user_id,office_reason,
    prior_event_hash,event_hash
  ) values (
    v_family.id,v_sequence,v_event_id,v_approval.id,
    (p_request#>>'{protected_schedule,work_date}')::date,
    (p_request#>>'{protected_schedule,start_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,end_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,break_minutes}')::integer,
    v_prior_event.rate_classification_json,p_request->'source_proposal',v_source_hash,
    p_request->'protected_schedule',v_prior_event.fixed_office_target_hash,'WAIT',
    v_prior_event.current_comparison_revision_id,v_prior_event.current_final_revision_id,
    v_actor,p_request->>'reason',v_prior_hash,v_event_hash
  ) returning id into v_family_event_id;
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_protected_current_decision_capture_v1(v_family_event_id);
  end if;
  select event_sequence,event_hash into v_target_sequence,v_prior_target_hash
  from public.weekly_exceptional_pay_target_events where family_id=v_family.id
  order by event_sequence desc limit 1;
  v_target_sequence:=coalesce(v_target_sequence,0)+1;
  v_target_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_TARGET_EVENT_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_sequence',v_target_sequence,
      'approval_id',v_approval.id,'generation_id',null,
      'prior_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'next_vector_hash',pg_catalog.encode(v_expected_hash,'hex'),
      'reason','WAIT','actor_user_id',v_actor
    )
  );
  insert into public.weekly_exceptional_pay_target_events(
    family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,fixed_target_component_snapshot,
    current_source_proposal_snapshot,complete_prior_family_vector_fingerprint,
    complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key
  ) values (
    v_family.id,v_approval.id,v_target_sequence,v_prior_event.current_comparison_revision_id,
    v_prior_event.current_final_revision_id,v_prior_target_hash,
    v_generation.complete_next_vector_json,p_request->'source_proposal',
    v_expected_hash,v_expected_hash,'WAIT','WAITING_SOURCE',null,
    v_actor,v_target_hash,v_key||':target-event'
  );
  v_next_wait:=v_family.target_domain_version='NEXT_V1';
  if v_next_wait then
    select incident.id into v_incident from public.weekly_discrepancy_incidents incident
      where incident.work_event_id=v_event_id and incident.candidate_id=v_family.candidate_id
        and incident.source_group_id=(select cycle.source_group_id from public.weekly_source_cycles cycle where cycle.id=v_cycle_id)
      order by incident.episode_number desc limit 1;
    select target.* into v_pending from public.weekly_exceptional_pending_reconciliation_targets target
      where target.family_id=v_family.id and target.approval_id=v_approval.id
        and target.source_action_policy_target_fingerprint=v_source_hash for update;
    if found and (v_pending.state<>'ACTIVE' or v_pending.completed_at_utc is not null
        or v_pending.durable_work_event_id<>v_event_id or v_pending.incident_id is distinct from v_incident
        or v_pending.current_final_revision_id is distinct from v_prior_event.current_final_revision_id
        or v_pending.intended_outcome<>'WAIT'
        or v_generation.complete_next_vector_hash<>v_expected_hash
        or v_prior_event.fixed_office_target_snapshot_json is distinct from p_request->'protected_schedule'
        or v_prior_event.source_proposal_snapshot_json is distinct from p_request->'source_proposal') then
      raise exception 'WEEKLY_PROTECTED_WAIT_PENDING_CONFLICT' using errcode='23514';
    end if;
    -- The existing partial UNIQUE(family_id,approval_id) WHERE ACTIVE bounds
    -- this transition to one row. Keep an exact existing ACTIVE key itself;
    -- do not erase audit history or disturb another protected event's wait.
    update public.weekly_exceptional_pending_reconciliation_targets target
      set state='SUPERSEDED',completed_at_utc=pg_catalog.statement_timestamp()
      where target.family_id=v_family.id and target.approval_id=v_approval.id and target.state='ACTIVE'
        and (v_pending.id is null or target.id<>v_pending.id);
    if v_pending.id is null then
      insert into public.weekly_exceptional_pending_reconciliation_targets(
        family_id,approval_id,durable_work_event_id,incident_id,current_final_revision_id,
        intended_outcome,source_action_policy_target_fingerprint,state)
      values(v_family.id,v_approval.id,v_event_id,v_incident,v_prior_event.current_final_revision_id,'WAIT',v_source_hash,'ACTIVE');
    end if;
  else
    update public.weekly_exceptional_pending_reconciliation_targets target
    set state='SUPERSEDED',completed_at_utc=pg_catalog.statement_timestamp()
    -- Wait is per existing work identity, not a decision for every sibling.
    where target.family_id=v_family.id and target.durable_work_event_id=v_event_id
      and target.state='ACTIVE'
      and not (target.approval_id=v_approval.id
        and target.source_action_policy_target_fingerprint=v_source_hash);
    insert into public.weekly_exceptional_pending_reconciliation_targets(
      family_id,approval_id,durable_work_event_id,incident_id,
      current_final_revision_id,intended_outcome,
      source_action_policy_target_fingerprint,state
    ) values (
      v_family.id,v_approval.id,v_event_id,
      (select incident.id from public.weekly_discrepancy_incidents incident
        where incident.work_event_id=v_event_id
          and incident.candidate_id=v_family.candidate_id
        order by incident.episode_number desc limit 1),
      v_prior_event.current_final_revision_id,'WAIT',v_source_hash,'ACTIVE'
    )
    -- A newly requested Wait for the same proposal retains the same pending
    -- identity. Replays still return above; a new command gets its own audit.
    -- Only retain an exact already-active row. Never reopen any retired decision
    -- (including superseded), or reuse another work/incident/revision's row.
    on conflict (family_id,approval_id,source_action_policy_target_fingerprint)
    do update set state='ACTIVE',completed_at_utc=null
    where weekly_exceptional_pending_reconciliation_targets.durable_work_event_id=v_event_id
      and weekly_exceptional_pending_reconciliation_targets.incident_id
        is not distinct from excluded.incident_id
      and weekly_exceptional_pending_reconciliation_targets.intended_outcome='WAIT'
      and weekly_exceptional_pending_reconciliation_targets.current_final_revision_id
        is not distinct from v_prior_event.current_final_revision_id
      and weekly_exceptional_pending_reconciliation_targets.state='ACTIVE'
      and weekly_exceptional_pending_reconciliation_targets.completed_at_utc is null;
    if not found then
      raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
    end if;
  end if;
  insert into public.weekly_exceptional_payment_events(
    family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key
  ) values (
    v_family.id,v_approval.id,'WAIT','WAITING_SOURCE',
    pg_catalog.jsonb_build_object('work_event_id',v_event_id,'source_cycle_id',v_cycle_id),
    v_key||':payment-event'
  );
  update public.weekly_exceptional_pay_target_families
  set current_source_proposal_hash=v_source_hash,
      current_lifecycle_state='WAITING_SOURCE',bound_version=bound_version+1
  where id=v_family.id;
  v_after:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_WAIT_AFTER_V1',pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'event_hash',pg_catalog.encode(v_event_hash,'hex'),
      'target_event_hash',pg_catalog.encode(v_target_hash,'hex'),
      'target_vector_hash',pg_catalog.encode(v_expected_hash,'hex')
    )
  );
  update public.weekly_exceptional_orchestration_runs
  set state='COMPLETE',after_state_fingerprint=v_after,
      completed_at_utc=pg_catalog.statement_timestamp()
  where id=v_run.id;
  perform private.bpay_next_source_query_refresh_v1(v_family.contract_id,v_event_id);
  return pg_catalog.jsonb_build_object(
    'ok',true,'outcome','WAITING_FOR_SOURCE','idempotent_replay',false,
    'family_id',v_family.id,'family_bound_version',v_expected_version+1,
    'target_vector_sha256',pg_catalog.encode(v_expected_hash,'hex')
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' using errcode='55000';
end;
$function$;
alter function private.bpay_next_source_query_refresh_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_query_refresh_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_manual_review_open_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_open_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_manual_review_open_v1(jsonb) to service_role;
alter function public.weekly_source_manual_review_resolve_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_resolve_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_manual_review_resolve_v1(jsonb) to service_role;
alter function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_manager_review_respond_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manager_review_respond_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_manager_review_respond_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_query_sync_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_query_sync_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_query_sync_atomic_v1(jsonb) to service_role;
alter function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) owner to postgres;
revoke all on function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamp with time zone) from public,anon,authenticated,service_role;
alter function public.weekly_exceptional_pay_publish_next_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_publish_next_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_publish_next_v1(jsonb) to service_role;
alter function public.weekly_exceptional_pay_complete_local_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;
alter function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_wait_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
