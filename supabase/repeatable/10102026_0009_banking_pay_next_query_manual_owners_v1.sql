-- F-25 true manual Query owners; original request/reply/row/audit rules retained.
-- Final-resolution is inherited stage-only; no inner command or clock.
\set ON_ERROR_STOP on
begin;

create or replace function public.weekly_source_manual_review_open_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_query_producer uuid; v_query_outer boolean;
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
  -- Pure active Office/admin permission; retain original scoped source reads
  -- after the advisory and exact race replay so an accepted reply cannot be
  -- preempted by a concurrently changed source or resolved review.
  perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',null,null,null);
  select entry.producer_id,entry.is_outer into v_query_producer,v_query_outer
    from private.bpay_next_query_producer_enter_v1('MANUAL_OPEN','MANUAL_OPEN',v_command_id::text,v_hash,'STANDALONE_QUERY') entry;
  perform pg_advisory_xact_lock(hashtextextended('WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND:'||v_command_id::text,0));
  select c.* into v_prior from private.weekly_source_manual_review_commands c where c.command_id=v_command_id;
  if found then
    if v_prior.command_kind is distinct from 'OPEN' or v_prior.actor_user_id is distinct from v_actor
       or v_prior.request_sha256 is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION' using errcode='23505'; end if;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_prior.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
      v_prior.client_id,v_cycle.finalisation_week_ending);
    if v_query_outer then
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'MANUAL_OPEN',v_command_id,
        pg_catalog.sha256(pg_catalog.convert_to(v_prior.result_json::text,'UTF8')));
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'MANUAL_OPEN');end if;
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
    if v_query_outer then
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'MANUAL_OPEN',v_command_id,
        pg_catalog.sha256(pg_catalog.convert_to(v_result::text,'UTF8')));
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'MANUAL_OPEN');end if;
  return v_result;
end;
$function$;

create or replace function public.weekly_source_manual_review_resolve_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_query_producer uuid; v_query_outer boolean;
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
  -- Pure actor permission before locks; original exact scope/OPEN validation
  -- follows the locked immutable reply probe, preserving concurrent replay.
  perform private.weekly_source_office_authority_v1(v_actor,
    case when v_kind='PROTECTED_PAY' then 'APPROVE_PROTECTED_PAY' else 'ACCEPT_SYSTEM_HOURS' end,null,null,null);
  select entry.producer_id,entry.is_outer into v_query_producer,v_query_outer
    from private.bpay_next_query_producer_enter_v1('MANUAL_RESOLVE','MANUAL_RESOLVE',v_command_id::text,v_hash,'STANDALONE_QUERY') entry;
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
    if v_query_outer then
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'MANUAL_RESOLVE',v_command_id,
        pg_catalog.sha256(pg_catalog.convert_to(v_prior.result_json::text,'UTF8')));
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'MANUAL_RESOLVE');end if;
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
    if v_query_outer then
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'MANUAL_RESOLVE',v_command_id,
        pg_catalog.sha256(pg_catalog.convert_to(v_result::text,'UTF8')));
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'MANUAL_RESOLVE');end if;
  return v_result;
end;
$function$;

create or replace function private.weekly_source_manual_reviews_finalised_v1(p_final_revision_id uuid,p_actor_user_id uuid)
returns integer language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_query_owner private.bpay_next_query_producer%rowtype;
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
  select p.* into v_query_owner from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id();
  if found then
    if v_query_owner.phase<>'BUSINESS_OPEN' or v_query_owner.owner_stack[1] not in ('SOURCE_FINAL','SOURCE_CORRECT_FINAL')
      or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=v_query_owner.preparing_transaction_id
        and t.module_epoch=v_query_owner.module_epoch and t.phase='OPEN') then
      raise exception using errcode='55000',message='BPAY_NEXT_QUERY_FINAL_RESOLUTION_OWNER_INVALID';end if;
  else
    perform private.weekly_source_pay_query_admit_v2();
    perform 1 from private.bpay_next_module_control m where m.id=1 for share;
    if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
      raise exception using errcode='55000',message='BPAY_NEXT_QUERY_FINAL_OWNER_NOT_INSTALLED';end if;
  end if;
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
    v_count:=v_count+1;
  end loop;
  return v_count;
end;
$function$;

alter function public.weekly_source_manual_review_open_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_open_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_manual_review_open_v1(jsonb) to service_role;

alter function public.weekly_source_manual_review_resolve_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_resolve_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_manual_review_resolve_v1(jsonb) to service_role;

alter function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_query_manual_review_row_stage_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare old_tuple jsonb:='{}'::jsonb;new_tuple jsonb:='{}'::jsonb;row_id uuid;candidate uuid;
begin
 if tg_op<>'INSERT' then
   old_tuple:=jsonb_build_object('source_group_id',old.source_group_id,'source_cycle_id',old.source_cycle_id,
     'work_event_id',old.work_event_id,'candidate_id',old.candidate_id,'client_id',old.client_id,
     'contract_id',old.contract_id,'work_date',old.work_date,'state',old.state);row_id:=old.id;candidate:=old.candidate_id;
 end if;
 if tg_op<>'DELETE' then
   new_tuple:=jsonb_build_object('source_group_id',new.source_group_id,'source_cycle_id',new.source_cycle_id,
     'work_event_id',new.work_event_id,'candidate_id',new.candidate_id,'client_id',new.client_id,
     'contract_id',new.contract_id,'work_date',new.work_date,'state',new.state);row_id:=new.id;candidate:=new.candidate_id;
 end if;
 if tg_op='UPDATE' and (new.id,new.source_group_id,new.source_cycle_id,new.work_event_id,new.candidate_id,new.client_id,new.contract_id,new.work_date)
   is distinct from (old.id,old.source_group_id,old.source_cycle_id,old.work_event_id,old.candidate_id,old.client_id,old.contract_id,old.work_date) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MANUAL_REVIEW_IDENTITY_IMMUTABLE';end if;
 if old_tuple=new_tuple then return null;end if;
 if not exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id()) then
   if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MATERIAL_WRITER_UNOWNED';end if;
   return null;
 end if;
 if not exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id()
   and p.owner_stack[pg_catalog.cardinality(p.owner_stack)] in ('MANUAL_OPEN','MANUAL_RESOLVE','SOURCE_FINAL','SOURCE_CORRECT_FINAL')) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MANUAL_REVIEW_WRITER_INVALID';end if;
 -- This relation has no canonical Timesheet/root column. Retain honest NULL.
 perform private.bpay_next_query_version_stage_v1('private.weekly_source_manual_reviews',row_id,candidate,null,
   tg_op<>'INSERT',tg_op<>'DELETE',old_tuple,new_tuple);
 return null;
end
$function$;

alter function private.bpay_next_query_manual_review_row_stage_v1() owner to postgres;
revoke all on function private.bpay_next_query_manual_review_row_stage_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_query_manual_review_row_stage_v1 on private.weekly_source_manual_reviews;
create trigger bpay_next_query_manual_review_row_stage_v1 after insert or update or delete on private.weekly_source_manual_reviews
 for each row execute function private.bpay_next_query_manual_review_row_stage_v1();

notify pgrst, 'reload schema';
commit;
