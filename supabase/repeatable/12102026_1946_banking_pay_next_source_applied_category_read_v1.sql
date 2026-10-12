-- SP17 bounded actual I3 read; same closed17-key reply and classifier.
-- Original2106 +0559 successors; only nativeSNOOZE and100-root bound.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_source_origin_state_v1(
  p_work_id uuid, p_expected_revision_id uuid
) returns jsonb
language plpgsql stable
set search_path = pg_catalog, private, public
as $function$
begin
  if p_work_id is null or p_expected_revision_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_ORIGIN_INVALID';
  end if;
  return (with facts as (
  select m.active_owner,m.owner_epoch,
    w.id as work_id,w.work_kind,w.approval_state,w.current_revision_id,w.applied_revision_id,
    w.current_revision_no,w.candidate_id,w.contract_id,w.booking_id,w.week_ending_date,
    r.id as revision_id,r.revision_no,r.source_kind,r.source_event_id,r.source_head_id,
    r.financial_snapshot_id,r.physical_timesheet_id,r.physical_timesheet_version,
    r.week_ending_date as revision_week,r.approved_at_utc,r.sealed_at_utc,
    r.source_inventory_digest as captured_inventory_digest,
    h.id as head_id,h.authority_kind,h.candidate_id as head_candidate_id,
    h.contract_id as head_contract_id,h.week_ending_date as head_week,
    h.root_timesheet_id as head_root,h.root_family_booking_id as head_booking,
    h.root_timesheet_version as head_version,h.state as head_state,
    h.committed_at_utc as head_committed_at,h.inventory_digest as head_inventory_digest,
    a.id as authorisation_id,a.root_timesheet_id as authorised_root,
    a.family_booking_id as authorised_booking,a.timesheet_version as authorised_version,
    a.authorised_at_utc as source_authorised_at,
    tf.id as snapshot_id,tf.timesheet_id as snapshot_root,tf.timesheet_version as snapshot_version,
    tf.candidate_id as snapshot_candidate,tf.authorised_at_utc as snapshot_authorised_at,
    p.command_id,p.candidate_id as publication_candidate,p.revision_no as publication_revision_no,
    p.predecessor_revision_id,p.status as publication_status,p.phase as publication_phase,
    p.cursor_key as publication_cursor,
    p.applied_at_utc as publication_applied_at,
    c.agency_sequence,c.module_epoch as command_module_epoch,c.command_kind,
    c.status as command_status,c.expected_member_count,c.enrolled_member_count,
    c.sealed_at_utc as command_sealed_at,
    cm.member_no,
    j.id as job_id,j.command_sequence,j.module_epoch as job_module_epoch,
    j.status as job_status,j.phase as job_phase,j.cursor_key as job_cursor,
    j.lease_nonce,j.lease_until_utc,
    wc.financial_view_revision
  from (select 1) singleton
  left join private.bpay_next_module_control m on m.id=1
  left join private.bpay_next_work w on w.id=p_work_id
  left join private.bpay_next_work_revision r
    on r.id=p_expected_revision_id and r.work_id=w.id
  left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id
  left join public.weekly_source_root_authorisations a
    on a.id=r.source_event_id and r.source_head_id is null
  left join public.timesheets_financials tf
    on tf.id=r.financial_snapshot_id and r.source_head_id is null
  left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
  left join private.bpay_next_command c on c.id=p.command_id
  left join private.bpay_next_command_member cm
    on cm.command_id=c.id and cm.candidate_id=w.candidate_id
  left join private.bpay_next_job j
    on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='POSITION_APPLY'
  left join private.bpay_next_worker_control wc on wc.candidate_id=w.candidate_id
), qualified as (
  select f.*,
    case when source_head_id is not null then head_inventory_digest
      else captured_inventory_digest end as inventory_digest,
    (work_kind='SOURCE' and approval_state='APPROVED'
      and current_revision_id=revision_id and current_revision_no=revision_no
      and revision_week=week_ending_date and source_event_id is not null
      and approved_at_utc is not null and sealed_at_utc is not null
      and pg_catalog.isfinite(approved_at_utc) and pg_catalog.isfinite(sealed_at_utc)
      and source_kind in ('SOURCE','PROTECTED')
      and (source_kind<>'PROTECTED' or source_head_id is not null)) as revision_compatible,
    case when source_head_id is not null then
      head_id=source_event_id and head_candidate_id=candidate_id
      and head_contract_id=contract_id and head_week=week_ending_date
      and head_root=physical_timesheet_id and head_booking=booking_id
      and head_version=physical_timesheet_version
      and head_state in ('COMMITTED_CURRENT','SUPERSEDED')
      and head_committed_at is not null and pg_catalog.isfinite(head_committed_at)
      and authority_kind=(case when source_kind='PROTECTED' then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end)
      and pg_catalog.octet_length(head_inventory_digest)=32
      and (captured_inventory_digest is null or captured_inventory_digest=head_inventory_digest)
    else source_kind='SOURCE' and authorisation_id=source_event_id
      and authorised_root=physical_timesheet_id and authorised_booking=booking_id
      and authorised_version=physical_timesheet_version
      and source_authorised_at is not null and pg_catalog.isfinite(source_authorised_at)
      and snapshot_id=financial_snapshot_id and snapshot_root=physical_timesheet_id
      and snapshot_version=physical_timesheet_version and snapshot_candidate=candidate_id
      and snapshot_authorised_at is not null and pg_catalog.isfinite(snapshot_authorised_at)
    end as origin_compatible,
    (publication_candidate=candidate_id and publication_revision_no=revision_no
      and command_kind='POSITION_APPLY' and command_module_epoch=owner_epoch
      and expected_member_count=1 and enrolled_member_count between 0 and 1
      and member_no=1 and command_sealed_at is not null and pg_catalog.isfinite(command_sealed_at)
      and financial_view_revision is not null
      and (job_id is null or (job_module_epoch=owner_epoch and command_sequence=agency_sequence))) as publication_compatible
  from facts f
), classified as (
  select q.*,
    case
      when owner_epoch is null then 'MODULE_UNAVAILABLE'
      when active_owner<>'NEXT' then 'MODULE_NOT_ACTIVE'
      when work_id is null then 'WORK_UNAVAILABLE'
      when revision_id is null then 'REVISION_UNAVAILABLE'
      when revision_compatible is not true then 'REVISION_INCOMPATIBLE'
      when (source_head_id is not null and head_id is null)
        or (source_head_id is null and (authorisation_id is null or snapshot_id is null)) then 'SOURCE_ORIGIN_UNAVAILABLE'
      when origin_compatible is not true then 'SOURCE_ORIGIN_INCOMPATIBLE'
      when inventory_digest is null then 'SOURCE_INVENTORY_UNAVAILABLE'
      when pg_catalog.octet_length(inventory_digest)<>32 then 'SOURCE_INVENTORY_INCOMPATIBLE'
      when command_id is null then 'PUBLICATION_UNAVAILABLE'
      when publication_compatible is not true then 'PUBLICATION_INCOMPATIBLE'
      when applied_revision_id=revision_id and publication_status='APPLIED'
        and publication_phase='DONE' and publication_cursor is null and publication_applied_at is not null
        and pg_catalog.isfinite(publication_applied_at)
        and command_status='COMPLETE' and enrolled_member_count=1
        and job_id is not null and job_status='DONE' and job_phase='DONE' and job_cursor is null
        and lease_nonce is null and lease_until_utc is null then 'APPLIED'
      when applied_revision_id is distinct from revision_id
        -- QUEUED/NEW may follow another accepted but not yet applied revision.
        -- The existing ordered owner certifies that backlog; no history walk.
        -- Once application has started, its exact predecessor must be applied.
        and ((publication_status='QUEUED' and publication_phase='NEW')
          or (publication_status='APPLYING' and publication_phase in ('NEW','REMOVED','SNOOZE')
            and applied_revision_id is not distinct from predecessor_revision_id))
        and publication_applied_at is null and command_status='SEALED'
        -- SNOOZE is an existing atomic publication/job checkpoint, not a
        -- permissive phase label on an unrelated or unenrolled job.
        and ((publication_phase<>'SNOOZE' and coalesce(job_phase,'')<>'SNOOZE')
          or (publication_phase='SNOOZE' and job_phase='SNOOZE'
            and publication_cursor is not distinct from job_cursor))
        and ((enrolled_member_count=0 and job_id is null)
          or (enrolled_member_count=1 and job_id is not null
            and job_status in ('READY','LEASED','BLOCKED') and job_phase in ('NEW','REMOVED','SNOOZE'))) then 'PENDING'
      else 'APPLICATION_INCOMPATIBLE'
    end as result_code
  from qualified q
)
select pg_catalog.jsonb_build_object(
  'ok',result_code in ('APPLIED','PENDING'), 'code',case
    when result_code in ('APPLIED','PENDING') then 'OK'
    when result_code in ('MODULE_UNAVAILABLE','WORK_UNAVAILABLE','REVISION_UNAVAILABLE',
      'SOURCE_ORIGIN_UNAVAILABLE','SOURCE_INVENTORY_UNAVAILABLE','PUBLICATION_UNAVAILABLE') then 'UNAVAILABLE'
    else 'INCOMPATIBLE' end,
  'work_id',work_id,'owner_epoch',owner_epoch::text,
  'current_revision_id',current_revision_id,'applied_revision_id',applied_revision_id,
  'source_event_id',source_event_id,'source_head_id',source_head_id,
  'financial_snapshot_id',financial_snapshot_id,'physical_root_id',physical_timesheet_id,
  'physical_root_version',physical_timesheet_version,
  'revision_seal',case when sealed_at_utc is not null and pg_catalog.isfinite(sealed_at_utc)
    then pg_catalog.to_char(sealed_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
  'source_inventory_digest',case when inventory_digest is not null
    then pg_catalog.encode(inventory_digest,'hex') end,
  'application_state',case when result_code in ('APPLIED','PENDING') then result_code
    when result_code in ('MODULE_UNAVAILABLE','WORK_UNAVAILABLE','REVISION_UNAVAILABLE',
      'SOURCE_ORIGIN_UNAVAILABLE','SOURCE_INVENTORY_UNAVAILABLE','PUBLICATION_UNAVAILABLE') then 'UNAVAILABLE'
    else 'INCOMPATIBLE' end,
  'command_id',command_id,'agency_sequence',agency_sequence::text,
  'financial_view_revision',financial_view_revision::text
)
from classified);
end
$function$;

-- The containing reviewed Source owner executes this read under its own
-- database authority. Neither service/browser callers nor PUBLIC can invoke
-- private I3 directly. Portable installer owner, no SET ROLE postgres.
alter function private.bpay_next_source_origin_state_v1(uuid,uuid) owner to current_user;
revoke all on function private.bpay_next_source_origin_state_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

create or replace function public.weekly_source_office_summary_rows_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_filters jsonb;
  v_row jsonb;
  v_root public.timesheets%rowtype;
  v_category jsonb;
  v_rows jsonb:='[]'::jsonb;
  v_authority_checked boolean:=false;
begin
  perform private.weekly_source_query_require_service_v1();
  if jsonb_typeof(p_request) is distinct from 'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,array['actor_user_id','p_filters'])
     or not (p_request ?& array['actor_user_id','p_filters'])
     or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
     or jsonb_typeof(p_request->'p_filters') is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_SUMMARY_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_filters:=p_request->'p_filters';
  -- A page reader, not a second membership/count/financial owner. Normal
  -- Office pages and targeted patches already supply this explicit bound.
  if v_actor is null or coalesce(v_filters->>'limit','') !~ '^[0-9]{1,3}$'
     or (v_filters->>'limit')::integer not between 1 and 100
     or lower(coalesce(v_filters->>'disable_paging',v_filters->>'disablePaging',
         v_filters->>'no_paging',v_filters->>'noPaging','')) in ('true','t','yes','y','1')
     or lower(coalesce(v_filters->>'apply_paging',v_filters->>'applyPaging','')) in ('false','f','no','n','0')
     or lower(coalesce(v_filters->>'purpose','')) in
         ('membership','memberships','ids','totals','total','count','counts') then
    raise exception 'WEEKLY_SOURCE_SUMMARY_PAGE_REQUIRED' using errcode='22023';
  end if;
  -- Exactly one call with the ORIGINAL filters. Do not re-sort, re-page,
  -- introduce membership predicates or drop contract-week-only rows.
  for v_row in select to_jsonb(r) from public.timesheet_summary_lightweight_rows_v1(v_filters) r loop
    if v_row->>'timesheet_id' is not null and v_row->>'sheet_scope'='WEEKLY'
       and (v_row->>'route_type'='WEEKLY_NHSP'
         or (v_row->>'route_type'='WEEKLY_HEALTHROSTER'
             and v_row->'client_no_timesheet_required'='true'::jsonb)
         -- A cold protected-only root may retain the ordinary route display
         -- until Source arrives. Its actual TARGET ownership, not that label,
         -- qualifies Source metadata. Never match just Candidate/week.
         or exists(select 1 from public.timesheets t
           join public.contracts c on c.id=t.contract_id
           join public.weekly_exceptional_pay_target_families f
             on f.root_family_booking_id=t.booking_id
               and f.root_timesheet_id=t.timesheet_id
               and f.candidate_id=c.candidate_id and f.contract_id=t.contract_id
               and f.week_ending_date=t.week_ending_date
           where t.timesheet_id=(v_row->>'timesheet_id')::uuid
             and t.is_adjustment=false
             and f.ownership_state='TARGET_MANAGED')) then
      if not v_authority_checked then
        perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS');
        v_authority_checked:=true;
      end if;
      select t.* into strict v_root from public.timesheets t
        where t.timesheet_id=(v_row->>'timesheet_id')::uuid;
      v_category:=private.weekly_source_timesheet_category_v2(v_root.timesheet_id);
      if v_category is not null and
         (v_category#>>'{category_basis,facts,root_timesheet_id}' is distinct from v_root.timesheet_id::text
          or v_category#>'{category_basis,facts,root_version}' is distinct from to_jsonb(v_root.version)
          or (v_category->>'presentation_category' is not null
            and v_category->>'presentation_category' is distinct from v_row->>'tools_stage')) then
        raise exception 'WEEKLY_SOURCE_SUMMARY_CATEGORY_CHANGED' using errcode='40001';
      end if;
      v_row:=v_row||jsonb_build_object('weekly_source_root_version',v_root.version,
        'weekly_source_operational_category',v_category,
        'weekly_source_processing_reason',v_category->'processing_reason');
    end if;
    v_rows:=v_rows||jsonb_build_array(v_row);
  end loop;
  return jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1','rows',v_rows);
exception when invalid_text_representation then
  raise exception 'WEEKLY_SOURCE_SUMMARY_REQUEST_INVALID' using errcode='22023';
end;
$function$;
alter function public.weekly_source_office_summary_rows_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_summary_rows_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_office_summary_rows_v1(jsonb) to service_role;
notify pgrst, 'reload schema';

commit;
