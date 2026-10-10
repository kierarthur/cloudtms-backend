-- F-25 exact standalone QuerySync staged-operation bijection.
-- Design: F-25-query-sync-completion-build.md. No owner/clock is added here.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_sync_expected_stages_v1(p_completion_id uuid)
returns table(operation text,owner_relation text,owner_row_id uuid,work_event_id uuid,comparison_id uuid,prior_comparison_id uuid,stage_rank integer)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select 'INSERT_INCIDENT','public.weekly_discrepancy_incidents',r.incident_id,r.work_event_id,r.comparison_revision_id,null::uuid,1
  from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome='NEW'
 union all
 select 'INSERT_COMPARISON','public.weekly_issue_comparison_revisions',r.comparison_revision_id,r.work_event_id,r.comparison_revision_id,r.prior_comparison_revision_id,2
  from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome in ('NEW','CHANGED')
 union all
 select 'SET_COMPARISON','public.weekly_discrepancy_incidents',r.incident_id,r.work_event_id,r.comparison_revision_id,r.prior_comparison_revision_id,3
  from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome in ('NEW','CHANGED')
 union all
 select 'RESOLVE_OMISSION','public.weekly_discrepancy_incidents',r.incident_id,r.work_event_id,r.comparison_revision_id,r.comparison_revision_id,1
  from private.bpay_next_query_sync_omission_completion r where r.completion_id=p_completion_id
$function$;

create or replace function private.bpay_next_query_sync_stage_matches_v1(p_event_id uuid,p_operation text,p_work_event_id uuid,
 p_comparison_id uuid,p_prior_comparison_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private,public
as $function$
 select coalesce((select case p_operation
  when 'INSERT_INCIDENT' then not e.old_present and e.new_present and q.old_gate_tuple='{}'::jsonb
   and q.new_gate_tuple->>'work_event_id'=p_work_event_id::text and q.new_gate_tuple->>'state'='OPEN'
   and q.new_gate_tuple->'current_comparison_revision_id'='null'::jsonb
   and q.new_gate_tuple->>'candidate_id'=q.candidate_id::text
  when 'INSERT_COMPARISON' then not e.old_present and e.new_present and q.old_gate_tuple='{}'::jsonb
   and q.new_gate_tuple=(select jsonb_build_object('incident_id',c.incident_id,'revision_number',c.revision_number,
     'contract_id',c.contract_id,'issue_family',c.issue_family,'material_comparison_fingerprint',encode(c.material_comparison_fingerprint,'hex'))
     from public.weekly_issue_comparison_revisions c where c.id=p_comparison_id)
  when 'SET_COMPARISON' then e.old_present and e.new_present
   and q.old_gate_tuple->>'work_event_id'=p_work_event_id::text
   and q.old_gate_tuple->>'state'='OPEN' and q.new_gate_tuple->>'state'='OPEN'
   and q.old_gate_tuple->>'current_comparison_revision_id' is not distinct from p_prior_comparison_id::text
   and q.new_gate_tuple->>'current_comparison_revision_id'=p_comparison_id::text
   and q.old_gate_tuple-'current_comparison_revision_id'=q.new_gate_tuple-'current_comparison_revision_id'
   and q.old_gate_tuple->>'candidate_id'=q.candidate_id::text
  when 'RESOLVE_OMISSION' then e.old_present and e.new_present
   and q.old_gate_tuple->>'work_event_id'=p_work_event_id::text
   and q.old_gate_tuple->>'state'='OPEN' and q.new_gate_tuple->>'state'='RESOLVED'
   and q.old_gate_tuple->>'current_comparison_revision_id'=p_comparison_id::text
   and q.old_gate_tuple-'state'=q.new_gate_tuple-'state'
   and q.old_gate_tuple->>'candidate_id'=q.candidate_id::text
  else false end
 from private.bpay_next_input_version_event e join private.bpay_next_query_input_version q on q.event_id=e.id
 where e.id=p_event_id and q.candidate_id is not null and q.root_timesheet_id is null),false)
$function$;

create or replace function private.bpay_next_query_sync_stages_assert_v1(p_completion_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare header private.bpay_next_query_root_completion%rowtype;owned private.bpay_next_query_producer%rowtype;raw record;expected_count bigint;
begin
 select h.* into strict header from private.bpay_next_query_root_completion h where h.id=p_completion_id and h.owner_kind='QUERY_SYNC';
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=header.id;
 select * into strict raw from private.bpay_next_query_raw_stages_v1(owned.id);
 select count(*) into expected_count from private.bpay_next_query_sync_expected_stages_v1(header.id);
 if raw.actual_stage_count<>expected_count or expected_count<>3*header.new_incident_count+2*header.changed_comparison_count+header.resolved_incident_count
   or expected_count>1000
   or (header.actual_stage_count,header.first_stage_ordinal,header.last_stage_ordinal,header.raw_stages_sha256) is distinct from
      (raw.actual_stage_count,raw.first_stage_ordinal,raw.last_stage_ordinal,raw.raw_stages_sha256)
   or (select count(*) from private.bpay_next_query_event_owner eo where eo.producer_id=owned.id)<>raw.actual_stage_count
   or exists(select 1 from private.bpay_next_query_event_owner eo
     left join private.bpay_next_input_version_event e on e.id=eo.event_id left join private.bpay_next_query_input_version q on q.event_id=eo.event_id
     where eo.producer_id=owned.id and
      (e.id is null or q.event_id is null or e.input_kind<>'PAY_QUERY' or e.module_epoch<>owned.module_epoch
       or e.preparing_transaction_id<>owned.preparing_transaction_id or e.original_command_id is not null
       or e.owner_relation<>q.owner_relation or e.owner_row_id<>q.owner_row_id
       or e.owner_relation not in ('public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions')
       or q.candidate_id is null or q.root_timesheet_id is not null
       or e.old_sha256<>sha256(convert_to(q.old_gate_tuple::text,'UTF8')) or e.new_sha256<>sha256(convert_to(q.new_gate_tuple::text,'UTF8'))
       or (not e.old_present and q.old_gate_tuple<>'{}'::jsonb) or (not e.new_present and q.new_gate_tuple<>'{}'::jsonb)))
   or exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
      and (e.input_kind<>'PAY_QUERY' or e.module_epoch<>owned.module_epoch or not exists(
         select 1 from private.bpay_next_query_event_owner eo where eo.event_id=e.id and eo.producer_id=owned.id))) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_STAGE_CENSUS_INVALID';end if;
 -- Each certified operation has exactly one real stage, and every real stage
 -- has exactly one certified operation. Neither plausible totals nor subset
 -- matches can conceal an unqualified canonical mutation.
 if exists(select 1 from private.bpay_next_query_sync_expected_stages_v1(header.id) x where
    (select count(*) from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id
     where eo.producer_id=owned.id and (e.owner_relation,e.owner_row_id)=(x.owner_relation,x.owner_row_id)
       and private.bpay_next_query_sync_stage_matches_v1(e.id,x.operation,x.work_event_id,x.comparison_id,x.prior_comparison_id))<>1)
   or exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id where eo.producer_id=owned.id and
    (select count(*) from private.bpay_next_query_sync_expected_stages_v1(header.id) x where
      (x.owner_relation,x.owner_row_id)=(e.owner_relation,e.owner_row_id)
      and private.bpay_next_query_sync_stage_matches_v1(e.id,x.operation,x.work_event_id,x.comparison_id,x.prior_comparison_id))<>1) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_STAGE_BIJECTION_INVALID';end if;
 -- A new incident's exact inserted tuple becomes the pointer update's OLD
 -- tuple; comparison and incident stages retain one actual Candidate scope.
 if exists(select 1 from private.bpay_next_query_sync_input_completion r
   join private.bpay_next_query_event_owner a on a.producer_id=owned.id
   join private.bpay_next_input_version_event ae on ae.id=a.event_id and ae.owner_relation='public.weekly_discrepancy_incidents' and ae.owner_row_id=r.incident_id
   join private.bpay_next_query_input_version aq on aq.event_id=ae.id
   join private.bpay_next_query_event_owner b on b.producer_id=owned.id
   join private.bpay_next_input_version_event be on be.id=b.event_id and be.owner_relation='public.weekly_discrepancy_incidents' and be.owner_row_id=r.incident_id
   join private.bpay_next_query_input_version bq on bq.event_id=be.id
   where r.completion_id=header.id and r.outcome='NEW' and not ae.old_present and be.old_present
     and aq.new_gate_tuple is distinct from bq.old_gate_tuple)
   or exists(select 1 from private.bpay_next_query_sync_input_completion r
    join private.bpay_next_query_event_owner a on a.producer_id=owned.id
    join private.bpay_next_input_version_event ae on ae.id=a.event_id and ae.owner_relation='public.weekly_issue_comparison_revisions' and ae.owner_row_id=r.comparison_revision_id
    join private.bpay_next_query_input_version aq on aq.event_id=ae.id
    join private.bpay_next_query_event_owner b on b.producer_id=owned.id
    join private.bpay_next_input_version_event be on be.id=b.event_id and be.owner_relation='public.weekly_discrepancy_incidents' and be.owner_row_id=r.incident_id
    join private.bpay_next_query_input_version bq on bq.event_id=be.id
    where r.completion_id=header.id and r.outcome in ('NEW','CHANGED') and aq.candidate_id is distinct from bq.candidate_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_STAGE_SCOPE_CONTINUITY_INVALID';end if;
 if exists(select 1 from private.bpay_next_query_sync_expected_stages_v1(header.id) earlier
    join private.bpay_next_query_sync_expected_stages_v1(header.id) later on later.work_event_id=earlier.work_event_id and later.stage_rank>earlier.stage_rank
    join private.bpay_next_input_version_event e on (e.owner_relation,e.owner_row_id)=(earlier.owner_relation,earlier.owner_row_id)
    join private.bpay_next_query_event_owner eo on eo.event_id=e.id and eo.producer_id=owned.id
    join private.bpay_next_input_version_event l on (l.owner_relation,l.owner_row_id)=(later.owner_relation,later.owner_row_id)
    join private.bpay_next_query_event_owner lo on lo.event_id=l.id and lo.producer_id=owned.id
    where private.bpay_next_query_sync_stage_matches_v1(e.id,earlier.operation,earlier.work_event_id,earlier.comparison_id,earlier.prior_comparison_id)
     and private.bpay_next_query_sync_stage_matches_v1(l.id,later.operation,later.work_event_id,later.comparison_id,later.prior_comparison_id)
     and e.transaction_ordinal>=l.transaction_ordinal) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_STAGE_ORDER_INVALID';end if;
end
$function$;
alter function private.bpay_next_query_sync_expected_stages_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_expected_stages_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_stage_matches_v1(uuid,text,uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_stage_matches_v1(uuid,text,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_stages_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_stages_assert_v1(uuid) from public,anon,authenticated,service_role;
commit;
