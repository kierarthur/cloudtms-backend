-- Repeatable CloudTMS function/view authority: weekly_source_pay_query_open_display_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- SP-13: bounded Source display facts, independent of the five-key pay gate.
-- All relevant OPEN rows remain informational; no receipt/blocking computation.
create or replace function private.weekly_source_pay_query_open_display_v1(
  p_root_timesheet_id uuid,
  p_limit integer default 100,
  p_after_query_kind text default null,
  p_after_query_id uuid default null
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb;
  v_queries jsonb;
  v_evidence_available boolean;
  v_more boolean;
  v_last_kind text;
  v_last_id uuid;
begin
  if p_root_timesheet_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_query_kind is null)<>(p_after_query_id is null)
     or (p_after_query_kind is not null
         and p_after_query_kind not in ('HOURS_INCIDENT','MANUAL_REVIEW')) then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_DISPLAY_INPUT_INVALID' using errcode='22023';
  end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null then
    -- Non-page error: no scope or resumable cursor, never an empty CLEAR page.
    return jsonb_build_object('ok',false,'code','WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE',
      'scope',null,'queries','[]'::jsonb,'complete',false,'next_cursor',null);
  end if;

  with selected as materialized (
    select relevant.query_kind,relevant.query_id
    from (
      select 'MANUAL_REVIEW'::text as query_kind,r.id as query_id
      from private.weekly_source_manual_reviews r
      where r.state='OPEN' and r.candidate_id=(v_scope->>'candidate_id')::uuid
        and r.client_id=(v_scope->>'client_id')::uuid
        and r.contract_id=(v_scope->>'contract_id')::uuid
        and r.work_date between (v_scope->>'week_ending_date')::date-6
                           and (v_scope->>'week_ending_date')::date
      union all
      select 'HOURS_INCIDENT'::text,i.id
      from public.weekly_discrepancy_incidents i
      left join public.weekly_work_events e on e.id=i.work_event_id
      left join public.weekly_issue_comparison_revisions owning
        on owning.incident_id=i.id and owning.revision_number=1
      where i.state='OPEN' and i.candidate_id=(v_scope->>'candidate_id')::uuid
        and i.client_id=(v_scope->>'client_id')::uuid
        and (e.work_date is null or e.work_date between
          (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date)
        and (owning.contract_id is null
          or owning.contract_id=(v_scope->>'contract_id')::uuid)
    ) relevant
    where p_after_query_kind is null
       or (relevant.query_kind collate "C",relevant.query_id)>
          (p_after_query_kind collate "C",p_after_query_id)
    order by relevant.query_kind collate "C",relevant.query_id
    limit p_limit+1
  ), page as materialized (
    select selected.query_kind,selected.query_id from selected
    order by selected.query_kind collate "C",selected.query_id
    limit p_limit
  ), details as (
    select page.query_kind,page.query_id,r.office_reason as reason,
      r.opened_by_user_id,nullif(actor.display_name,'') as opened_by_name,
      r.opened_at_utc,r.work_date,
      (e.id is not null and e.candidate_id is not distinct from r.candidate_id
        and e.client_id is not distinct from r.client_id
        and e.work_date is not distinct from r.work_date
        and cycle.source_group_id is not distinct from r.source_group_id
        and actor.id is not null and isfinite(r.opened_at_utc)) as evidence_available
    from page
    join private.weekly_source_manual_reviews r
      on page.query_kind='MANUAL_REVIEW' and r.id=page.query_id
    left join public.weekly_work_events e on e.id=r.work_event_id
    left join public.weekly_source_cycles cycle on cycle.id=r.source_cycle_id
    left join public.tms_users actor on actor.id=r.opened_by_user_id
    union all
    select page.query_kind,page.query_id,
      case when x.incident_id=i.id then x.issue_family else null end,
      null::uuid,null::text,i.created_at_utc,e.work_date,
      (e.id is not null and e.work_date is not null
        and e.candidate_id is not distinct from i.candidate_id
        and e.client_id is not distinct from i.client_id
        and cycle.source_group_id is not distinct from i.source_group_id
        and owning.id is not null
        and owning.contract_id is not distinct from (v_scope->>'contract_id')::uuid
        and x.id is not null and x.incident_id=i.id
        and x.id is not distinct from tail.id
        and x.contract_id is not distinct from (v_scope->>'contract_id')::uuid
        and x.issue_family is not null and x.material_comparison_fingerprint is not null
        and isfinite(i.created_at_utc))
    from page
    join public.weekly_discrepancy_incidents i
      on page.query_kind='HOURS_INCIDENT' and i.id=page.query_id
    left join public.weekly_work_events e on e.id=i.work_event_id
    left join public.weekly_source_cycles cycle on cycle.id=i.source_cycle_id
    left join public.weekly_issue_comparison_revisions owning
      on owning.incident_id=i.id and owning.revision_number=1
    left join public.weekly_issue_comparison_revisions x on x.id=i.current_comparison_revision_id
    left join lateral (
      select comparison.id from public.weekly_issue_comparison_revisions comparison
      where comparison.incident_id=i.id order by comparison.revision_number desc limit 1
    ) tail on true
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'query_kind',details.query_kind,'query_id',details.query_id,'reason',details.reason,
      'opened_by_user_id',details.opened_by_user_id,'opened_by_name',details.opened_by_name,
      'opened_at_utc',case when isfinite(details.opened_at_utc) then
        to_char(details.opened_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') else null end,
      'work_date',to_char(details.work_date,'YYYY-MM-DD'),
      'evidence_available',coalesce(details.evidence_available,false))
      order by details.query_kind collate "C",details.query_id),'[]'::jsonb),
    coalesce(bool_and(coalesce(details.evidence_available,false)),true),
    (select count(*)>p_limit from selected),
    (select page.query_kind from page
      order by page.query_kind collate "C" desc,page.query_id desc limit 1),
    (select page.query_id from page
      order by page.query_kind collate "C" desc,page.query_id desc limit 1)
  into v_queries,v_evidence_available,v_more,v_last_kind,v_last_id
  from details;

  return jsonb_build_object('ok',v_evidence_available,'code',case when v_evidence_available
      then 'OK' else 'WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE' end,
    'scope',v_scope,'queries',v_queries,'complete',not v_more,
    'next_cursor',case when v_more then jsonb_build_object(
      'query_kind',v_last_kind,'query_id',v_last_id) else null end);
end;
$function$;
alter function private.weekly_source_pay_query_open_display_v1(uuid,integer,text,uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_open_display_v1(uuid,integer,text,uuid)
  from public,anon,authenticated,service_role;
notify pgrst,'reload schema';

commit;
