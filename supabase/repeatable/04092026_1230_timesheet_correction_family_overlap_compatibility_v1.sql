-- Treat one already-certified import correction family as one logical Timesheet
-- for cross-record overlap admission. Ordinary and unrelated overlaps remain
-- prohibited. This is an exact replacement of the existing overlap assertion;
-- no payment, rate, tax, VAT, selection or Draft economics are calculated here.
\set ON_ERROR_STOP on
begin;

create or replace function private._timesheet_cross_record_overlap_assert_v1(
  p_candidate_id uuid,
  p_submission_json jsonb,
  p_actual_schedule_json jsonb,
  p_worked_start_iso timestamptz,
  p_worked_end_iso timestamptz,
  p_exclude_workflow_id uuid,
  p_exclude_timesheet_id uuid,
  p_exclude_booking_id text
) returns void language plpgsql volatile security definer
set search_path='public','private','pg_temp' as $function$
declare
  v_new_count integer:=0;
  v_conflict_kind text;
  v_conflict_id uuid;
  v_conflict_start timestamptz;
  v_conflict_end timestamptz;
  v_exact_chain jsonb;
  v_exact_correction_member_ids uuid[]:=array[]::uuid[];
  v_exact_correction_member_count integer:=0;
begin
  if p_candidate_id is null then
    raise exception using errcode='PT409',message='TIMESHEET_CANDIDATE_IDENTITY_REQUIRED_FOR_OVERLAP_CHECK';
  end if;

  select count(*)::integer into v_new_count
  from private._timesheet_work_intervals_v1(
    COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
    p_worked_start_iso,p_worked_end_iso
  );
  if v_new_count=0 then return; end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'timesheet-cross-record-overlap:'||p_candidate_id::text,31082026
  ));

  -- The correction-chain authority is consulted only for a current row that it
  -- classifies as an authoritative import correction. The exclusion activates
  -- only when every member of the resolved chain is accounted for by the root
  -- plus valid correction units and the requested unit is itself exact. This
  -- prevents an ordinary child, incomplete/tampered pair, duplicate leg or
  -- unrelated overlapping Timesheet from entering the exclusion set.
  if p_exclude_timesheet_id is not null
     and COALESCE((
       public._ctms_import_correction_classify_v1(p_exclude_timesheet_id)
         ->>'is_import_authoritative_correction'
     )::boolean,false) then
    v_exact_chain:=public.timesheet_correction_chain_scope_v1(
      p_exclude_timesheet_id,false,32,100
    );

    if COALESCE((v_exact_chain->>'valid')::boolean,false)
       and jsonb_typeof(v_exact_chain->'requested_correction_unit')='object'
       and COALESCE((
         v_exact_chain->'requested_correction_unit'->>'valid'
       )::boolean,false)
       and not exists (
         select 1
         from jsonb_array_elements(
           COALESCE(v_exact_chain->'correction_units','[]'::jsonb)
         ) correction_unit(unit_json)
         where COALESCE((correction_unit.unit_json->>'valid')::boolean,false) is not true
       ) then
      select COALESCE(
               array_agg(distinct exact_member.member_id order by exact_member.member_id),
               array[]::uuid[]
             )
      into v_exact_correction_member_ids
      from (
        select nullif(v_exact_chain->>'root_timesheet_id','')::uuid as member_id
        union all
        select correction_member.member_id_text::uuid
        from jsonb_array_elements(
          COALESCE(v_exact_chain->'correction_units','[]'::jsonb)
        ) correction_unit(unit_json)
        cross join lateral jsonb_array_elements_text(
          COALESCE(correction_unit.unit_json->'member_ids','[]'::jsonb)
        ) correction_member(member_id_text)
        where COALESCE((correction_unit.unit_json->>'valid')::boolean,false)
      ) exact_member
      where exact_member.member_id is not null;

      v_exact_correction_member_count:=COALESCE(
        (v_exact_chain->>'member_count')::integer,0
      );
      if v_exact_correction_member_count<>cardinality(v_exact_correction_member_ids)
         or p_exclude_timesheet_id<>all(v_exact_correction_member_ids) then
        v_exact_correction_member_ids:=array[]::uuid[];
      end if;
    end if;
  end if;

  with new_intervals as materialized (
    select * from private._timesheet_work_intervals_v1(
      COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
      p_worked_start_iso,p_worked_end_iso
    )
  )
  select 'WORKFLOW',w.id,existing.work_start_utc,existing.work_end_utc
  into v_conflict_kind,v_conflict_id,v_conflict_start,v_conflict_end
  from public.candidate_submission_workflows w
  cross join lateral private._timesheet_work_intervals_v1(
    COALESCE(w.immutable_submission_json,'{}'::jsonb),null,null,null
  ) existing
  join new_intervals proposed
    on proposed.work_start_utc<existing.work_end_utc
   and existing.work_start_utc<proposed.work_end_utc
  where w.candidate_id=p_candidate_id
    and w.id is distinct from p_exclude_workflow_id
    and w.worker_submitted_at_utc is not null
    and w.state in (
      'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
      'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
      'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
      'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED'
    )
    and not (
      p_exclude_booking_id is not null and exists(
        select 1 from public.timesheets linked
        where linked.timesheet_id in (w.target_timesheet_id,w.anchor_timesheet_id)
          and linked.booking_id=p_exclude_booking_id
      )
    )
  order by existing.work_start_utc,w.id
  limit 1;

  if v_conflict_id is null then
    with new_intervals as materialized (
      select * from private._timesheet_work_intervals_v1(
        COALESCE(p_submission_json,'{}'::jsonb),p_actual_schedule_json,
        p_worked_start_iso,p_worked_end_iso
      )
    ), accepted_timesheets as materialized (
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.timesheets_financials f
      join public.timesheets t on t.timesheet_id=f.timesheet_id
      where f.candidate_id=p_candidate_id and f.is_current=true
        and t.is_current=true and t.revoked_at is null
        and (t.authorised_at_server is not null or f.authorised_at_utc is not null)
      union
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.contracts c
      join public.timesheets t on t.contract_id=c.id
      where c.candidate_id=p_candidate_id and t.is_current=true
        and t.revoked_at is null and t.authorised_at_server is not null
      union
      select t.timesheet_id,t.booking_id,t.actual_schedule_json,
        t.worked_start_iso,t.worked_end_iso
      from public.candidates c
      join public.timesheets t on t.occupant_key_norm=c.key_norm
      where c.id=p_candidate_id and c.key_norm is not null
        and t.is_current=true and t.revoked_at is null
        and t.authorised_at_server is not null
    )
    select 'TIMESHEET',t.timesheet_id,existing.work_start_utc,existing.work_end_utc
    into v_conflict_kind,v_conflict_id,v_conflict_start,v_conflict_end
    from accepted_timesheets t
    cross join lateral private._timesheet_work_intervals_v1(
      '{}'::jsonb,t.actual_schedule_json,t.worked_start_iso,t.worked_end_iso
    ) existing
    join new_intervals proposed
      on proposed.work_start_utc<existing.work_end_utc
     and existing.work_start_utc<proposed.work_end_utc
    where t.timesheet_id is distinct from p_exclude_timesheet_id
      and not (t.timesheet_id=any(v_exact_correction_member_ids))
      and (p_exclude_booking_id is null or t.booking_id<>p_exclude_booking_id)
    order by existing.work_start_utc,t.timesheet_id
    limit 1;
  end if;

  if v_conflict_id is not null then
    raise exception using
      errcode='PT409',
      message='TIMESHEET_WORK_INTERVAL_OVERLAP',
      detail=jsonb_build_object(
        'error_code','TIMESHEET_WORK_INTERVAL_OVERLAP',
        'conflicting_record_kind',v_conflict_kind,
        'conflicting_record_id',v_conflict_id,
        'conflicting_start_utc',v_conflict_start,
        'conflicting_end_utc',v_conflict_end
      )::text;
  end if;
end;$function$;

alter function private._timesheet_cross_record_overlap_assert_v1(
  uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text
) owner to postgres;
revoke all on function private._timesheet_cross_record_overlap_assert_v1(
  uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text
) from public;

do $acl$ begin
  if exists(select 1 from pg_catalog.pg_roles where rolname='anon') then
    revoke all on function private._timesheet_cross_record_overlap_assert_v1(
      uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text
    ) from anon;
  end if;
  if exists(select 1 from pg_catalog.pg_roles where rolname='authenticated') then
    revoke all on function private._timesheet_cross_record_overlap_assert_v1(
      uuid,jsonb,jsonb,timestamptz,timestamptz,uuid,uuid,text
    ) from authenticated;
  end if;
end;$acl$;

notify pgrst,'reload schema';
commit;
