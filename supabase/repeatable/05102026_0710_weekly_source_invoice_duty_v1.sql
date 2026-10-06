-- Complete factual invoice-duty census. Never prepares, issues or changes money.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_invoice_duty_v1(p_root_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_scope jsonb; v_ids uuid[]; v_groups uuid[]; v_invoices uuid[];
  v_movement public.weekly_source_billing_movements%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_operation public.invoice_operations%rowtype;
  v_chunk public.invoice_operation_chunks%rowtype;
  v_member jsonb; v_members jsonb; v_context jsonb; v_guard jsonb; v_policy jsonb;
  v_basis jsonb:='[]'::jsonb; v_observation boolean;
  v_present boolean:=false; v_unknown boolean:=false;
  v_generation integer; v_count integer; v_invoice_id uuid; v_member_id uuid;
  v_linked_root uuid; v_text text; v_week date; v_relevant boolean;
  v_prior record;
begin
  select * into v_root from public.timesheets where timesheet_id=p_root_timesheet_id;
  if not found then return null; end if;
  select * into strict v_contract from public.contracts where id=v_root.contract_id;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  v_ids:=private.weekly_source_invoice_family_timesheet_ids_v1(p_root_timesheet_id);
  if v_scope is null or cardinality(v_ids) is null or cardinality(v_ids)=0 then return null; end if;
  select array_agg(distinct source_group_id) filter(where source_group_id is not null) into v_groups from (
    select c.source_group_id from public.weekly_source_row_timesheet_lineages l
      join public.weekly_source_cycles c on c.id=l.source_cycle_id where l.timesheet_id=any(v_ids)
    union select e.first_source_group_id from public.weekly_exceptional_pay_family_events fe
      join public.weekly_work_events e on e.id=fe.durable_work_event_id
      where fe.family_id=(v_scope->>'target_family_id')::uuid
    union select membership.source_group_id from public.weekly_source_group_clients membership
      where membership.client_id=v_contract.client_id
        and membership.valid_from<=v_root.week_ending_date
        and coalesce(membership.valid_to,'infinity'::date)>=v_root.week_ending_date-6
  ) groups;
  if cardinality(v_groups) is null or cardinality(v_groups)=0 then return null; end if;
  v_basis:=jsonb_build_array(jsonb_build_object('lane','DISCOVERY','groups',to_jsonb(v_groups),
    'scope',v_scope,'family_timesheet_ids',to_jsonb(v_ids)));
  select coalesce(array_agg(distinct l.invoice_id),'{}'::uuid[]) into v_invoices
    from public.invoice_lines l where l.timesheet_id=any(v_ids);

  -- Incomplete publication is not an empty invoice census. Discover across
  -- all cutoffs using client/group and actual work-date coverage, not the
  -- report's week. Nullable/unparsed date coverage remains relevant unknown.
  for v_upload in select u.* from public.weekly_source_uploads u
    join public.weekly_source_cycles c on c.id=u.source_cycle_id
    left join public.weekly_source_report_scopes s on s.id=u.report_scope_id
    where c.source_group_id=any(v_groups) and u.state not in ('CURRENT','SUPERSEDED','REJECTED')
      and (c.scope_client_id is null or c.scope_client_id=v_contract.client_id)
      and (s.client_id is null or s.client_id=v_contract.client_id)
      and (u.confirmed_coverage_start_local_date is null or u.confirmed_coverage_end_local_date is null
        or daterange(u.confirmed_coverage_start_local_date,u.confirmed_coverage_end_local_date+1,'[)')
          && daterange(v_root.week_ending_date-6,v_root.week_ending_date+1,'[)')
        or exists(select 1 from public.weekly_source_upload_rows r where r.upload_id=u.id
          and (r.work_date is null or r.work_date between v_root.week_ending_date-6 and v_root.week_ending_date)))
    order by u.id loop
    -- Prepared corrections have their complete old/new census below; they
    -- must not be permanently made unknown by their own CORRECTION_READY row.
    if v_upload.state='CORRECTION_READY' and exists(select 1 from public.weekly_final_source_correction_sessions s
      where s.id=v_upload.correction_session_id and s.replacement_correction_upload_id=v_upload.id
        and s.state in ('PREPARING','PREPARED','COMMITTING')) then continue; end if;
    v_unknown:=true;
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','INCOMPLETE_IMPORT',
      'upload',to_jsonb(v_upload),'present',null));
  end loop;

  -- Placement remains a duty across report cutoffs. A historical issued
  -- document alone is not work waiting to be invoiced again.
  for v_movement in select * from public.weekly_source_billing_movements where invoice_timesheet_id=any(v_ids)
    order by id loop
    v_observation:=null;
    if v_movement.candidate_id=v_contract.candidate_id and v_movement.contract_id=v_contract.id
       and v_movement.actual_client_id=v_contract.client_id then
      if v_movement.placement_state in ('UNPLACED','PLACED') then
        -- Prepared replacements are not yet admitted invoice movements. Their
        -- complete union and current seals are qualified in CORRECTION below.
        if exists(select 1 from public.weekly_source_final_revisions f
          where f.id=v_movement.final_revision_id and f.state='PREPARED') then
          if exists(select 1 from public.weekly_final_source_correction_sessions s
            where s.prepared_final_revision_id=v_movement.final_revision_id
              and s.state in ('PREPARING','PREPARED','COMMITTING')) then continue; end if;
        end if;
        if exists(select 1 from public.weekly_source_final_revisions f
          where f.id=v_movement.final_revision_id and f.state='CURRENT'
            and f.finalised_at_utc is not null and isfinite(f.finalised_at_utc)) then v_observation:=true; end if;
      elsif v_movement.placement_state='VOIDED_BY_CORRECT_FINAL' then
        if not exists(select 1 from public.weekly_source_invoice_line_bindings b
          where b.billing_movement_id=v_movement.id and b.state='CURRENT') then v_observation:=false; end if;
      elsif v_movement.placement_state='ISSUED' and exists(
        select 1 from public.weekly_source_invoice_line_bindings b
        join public.invoice_lines l on l.id=b.invoice_line_id and l.invoice_id=b.invoice_id
        join public.invoices i on i.id=b.invoice_id
        where b.billing_movement_id=v_movement.id and b.state='CURRENT'
          and b.client_id=v_movement.actual_client_id and i.client_id=v_movement.actual_client_id
          and l.timesheet_id=v_movement.invoice_timesheet_id
          and (i.status::text='ISSUED' or i.issue_state='ISSUED')) then v_observation:=false; end if;
    end if;
    v_present:=v_present or v_observation is true; v_unknown:=v_unknown or v_observation is null;
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','MOVEMENT','id',v_movement.id,
      'hash',encode(v_movement.movement_economic_hash,'hex'),'placement',v_movement.placement_state,'present',v_observation));
  end loop;

  -- Actual current imports, independent of their report/cutoff week. A
  -- checking-only NHSP import is positively excluded, not invoice authority.
  for v_upload in select u.* from public.weekly_source_uploads u
    join public.weekly_source_cycles c on c.id=u.source_cycle_id
    left join public.weekly_source_report_scopes s on s.id=u.report_scope_id
    where c.source_group_id=any(v_groups) and u.state='CURRENT'
      and (c.scope_client_id is null or c.scope_client_id=v_contract.client_id)
      and (s.client_id is null or s.client_id=v_contract.client_id) order by u.id
  loop
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','CURRENT_IMPORT',
      'upload',to_jsonb(v_upload),'cycle',(select to_jsonb(c) from public.weekly_source_cycles c where c.id=v_upload.source_cycle_id),
      'report_scope',(select to_jsonb(s) from public.weekly_source_report_scopes s where s.id=v_upload.report_scope_id)));
    select * into strict v_profile from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
    if v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' or v_profile.profile_json->>'purpose'='PREFINAL_CHECKING'
       or v_profile.row_finalisation_capability='CHECKING_ONLY' then continue; end if;
    -- A genuinely consumed Final already has its placement census above.
    if exists(select 1 from public.weekly_source_final_revisions f
      join public.weekly_source_cycles c on c.id=f.source_cycle_id
      left join public.weekly_source_report_scopes s on s.id=f.report_scope_id
      where f.upload_id=v_upload.id and f.source_cycle_id=v_upload.source_cycle_id
        and f.report_scope_id is not distinct from v_upload.report_scope_id
        and f.state='CURRENT' and f.finalised_at_utc is not null and isfinite(f.finalised_at_utc)
        and ((f.authority_scope_kind='CYCLE' and s.id is null and c.current_final_revision_id=f.id)
          or (f.authority_scope_kind='NHSP_REPORT_SCOPE' and s.current_final_revision_id=f.id))) then continue; end if;
    select count(*) into v_count from public.weekly_source_projection_publications p
      where p.upload_id=v_upload.id and p.source_cycle_id=v_upload.source_cycle_id
        and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT';
    if v_count<>1 then v_unknown:=true; continue; end if;
    select * into strict v_publication from public.weekly_source_projection_publications p
      where p.upload_id=v_upload.id and p.source_cycle_id=v_upload.source_cycle_id
        and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT';
    begin
      v_guard:=private.weekly_source_current_publication_guard_v1(v_upload.source_cycle_id,
        v_publication.authority_scope_kind,v_upload.report_scope_id,v_upload.id,v_publication.id,v_publication.authority_scope_version);
    exception when sqlstate '55000' or sqlstate '22023' then v_unknown:=true; continue; end;
    if v_publication.authority_scope_version>2147483647 or coalesce(v_publication.projection_generation,0)>2147483647
       or v_publication.published_at_utc is null or not isfinite(v_publication.published_at_utc) then
      v_unknown:=true; continue; end if;
    v_generation:=coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer);
    -- Match the genuine roster Final owner's complete prior/current union.
    -- A prior root may own a reversal when its event is omitted inside coverage
    -- OR retained by a current row that moves it to another root/work week.
    -- NHSP NO_INFERENCE does not use roster omission authority.
    if v_profile.final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
       and v_profile.omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE' then
      for v_prior in
        with prior_states as (
          select distinct on(t.work_event_id) t.new_snapshot_line_id,t.id as transition_id
          from public.weekly_source_state_transitions t
          join public.weekly_source_final_revisions f on f.id=t.final_revision_id and f.state='CURRENT'
          join public.weekly_source_cycles c on c.id=t.finalisation_cycle_id
          join public.weekly_source_client_manifests m on m.final_revision_id=f.id and m.client_id=v_contract.client_id
          where t.source_profile_kind=v_profile.final_authority_kind
            and c.source_group_id=(select source_group_id from public.weekly_source_cycles where id=v_upload.source_cycle_id)
            and c.finalisation_week_ending<(select finalisation_week_ending from public.weekly_source_cycles where id=v_upload.source_cycle_id)
          order by t.work_event_id,c.finalisation_week_ending desc,f.finalised_at_utc desc,
            f.revision_number desc,t.created_at_utc desc,t.id desc
        ) select line.*,s.transition_id,
            exists(select 1 from public.weekly_source_row_resolutions r
              join public.weekly_source_upload_rows current_row on current_row.id=r.upload_row_id
              where current_row.upload_id=v_upload.id and r.generation=v_generation
                and r.work_event_id=line.work_event_id) as has_current_replacement
          from prior_states s
          join public.weekly_source_final_snapshot_lines line on line.id=s.new_snapshot_line_id
          where line.candidate_id=v_contract.candidate_id and line.contract_id=v_contract.id
            and line.client_id=v_contract.client_id
            and line.work_date between v_root.week_ending_date-6 and v_root.week_ending_date
          order by line.id
      loop
        v_observation:=null;
        if v_upload.coverage_state='COMPLETE' and v_upload.coverage_confirmed_by_user_id is not null
           and v_upload.coverage_confirmed_at_utc is not null and isfinite(v_upload.coverage_confirmed_at_utc)
           and v_upload.coverage_timezone='Europe/London'
           and v_upload.confirmed_coverage_start_local_date is not null
           and v_upload.confirmed_coverage_end_local_date is not null
           and (v_prior.work_date between v_upload.confirmed_coverage_start_local_date and v_upload.confirmed_coverage_end_local_date
             or v_prior.has_current_replacement) then
          begin
            v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_prior.work_date);
            v_linked_root:=private.weekly_source_finalisation_lineage_assert_v1(v_prior.row_resolution_id,
              (select source_cycle_id from public.weekly_source_final_revisions where id=v_prior.final_revision_id));
            if v_linked_root=p_root_timesheet_id and v_policy->>'authority_mode'='SOURCE_AUTHORITY'
               and v_policy->>'self_bill_enabled'='true'
               and v_policy->>'source_group_id'=(select source_group_id::text from public.weekly_source_cycles where id=v_upload.source_cycle_id)
              then v_observation:=true; end if;
          exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023' then null; end;
        elsif v_upload.coverage_state='COMPLETE'
           and v_upload.confirmed_coverage_start_local_date is not null
           and v_upload.confirmed_coverage_end_local_date is not null
           and not v_prior.has_current_replacement
           and v_prior.work_date not between v_upload.confirmed_coverage_start_local_date and v_upload.confirmed_coverage_end_local_date
          then v_observation:=false;
        end if;
        v_present:=v_present or v_observation is true; v_unknown:=v_unknown or v_observation is null;
        v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','PREFINAL_ROSTER_PRIOR_CURRENT_UNION',
          'prior_snapshot',to_jsonb(v_prior),'publication',to_jsonb(v_publication),'present',v_observation));
      end loop;
    end if;
    for v_row in select * from public.weekly_source_upload_rows where upload_id=v_upload.id
      and (work_date is null or work_date between v_root.week_ending_date-6 and v_root.week_ending_date) order by id loop
      select * into v_resolution from public.weekly_source_row_resolutions
        where upload_row_id=v_row.id and generation=v_generation;
      v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','CURRENT_ROW_DISCOVERY',
        'row_id',v_row.id,'row_hash',encode(v_row.normalised_row_hash,'hex'),
        'resolution',to_jsonb(v_resolution),'publication',to_jsonb(v_publication)));
      if not found then v_unknown:=true; continue; end if;
      if v_resolution.candidate_id is not null and v_resolution.candidate_id<>v_contract.candidate_id
         or v_resolution.client_id is not null and v_resolution.client_id<>v_contract.client_id
         or v_resolution.contract_id is not null and v_resolution.contract_id<>v_contract.id then continue; end if;
      if v_resolution.mapping_state<>'RESOLVED' or v_row.work_date is null
         or v_contract.week_ending_weekday_snapshot is null
         or v_contract.week_ending_weekday_snapshot not between 0 and 6
         or v_row.work_date<v_contract.start_date
         or (v_contract.end_date is not null and v_row.work_date>v_contract.end_date)
         or v_root.submission_mode is distinct from 'MANUAL' then v_unknown:=true; continue; end if;
      begin
        v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_row.work_date);
      exception when sqlstate '55000' or sqlstate '22023' then v_unknown:=true; continue; end;
      if v_policy->>'authority_mode' is distinct from 'SOURCE_AUTHORITY'
         or v_policy->>'self_bill_enabled' is distinct from 'true'
         or v_policy->>'source_group_id' is distinct from (
           select c.source_group_id::text from public.weekly_source_cycles c where c.id=v_upload.source_cycle_id)
         or encode(v_resolution.effective_policy_fingerprint,'hex') is distinct from v_policy->>'policy_sha256'
         then v_unknown:=true; continue; end if;
      v_week:=v_row.work_date+((v_contract.week_ending_weekday_snapshot-extract(dow from v_row.work_date)::integer+7)%7);
      if v_week<>v_root.week_ending_date then continue; end if;
      v_linked_root:=null;
      if exists(select 1 from public.weekly_source_row_timesheet_lineages where row_resolution_id=v_resolution.id) then
        begin
          v_linked_root:=private.weekly_source_finalisation_lineage_assert_v1(v_resolution.id,v_upload.source_cycle_id);
        exception when sqlstate '55000' or sqlstate '40001' then v_unknown:=true; continue; end;
      else
        select cw.timesheet_id into v_linked_root from public.contract_weeks cw
          where cw.contract_id=v_contract.id and cw.week_ending_date=v_week and cw.additional_seq=0
            and not cw.is_adjustment and cw.status<>'CANCELLED'
            and cw.submission_mode_snapshot='MANUAL';
      end if;
      if v_linked_root is distinct from p_root_timesheet_id
         or not v_root.is_current or v_root.is_adjustment or v_root.revoked_at is not null
         or v_root.archived_at_utc is not null or v_root.sheet_scope<>'WEEKLY' or v_root.line_type<>'HOURS' then
        v_unknown:=true; continue; end if;
      v_present:=true;
      v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','PREFINAL','row_id',v_row.id,
        'resolution_id',v_resolution.id,'generation',v_generation,'publication_guard',v_guard));
    end loop;
  end loop;

  -- Correct Final's full old/new union includes removal of its last movement.
  -- Unprepared or stale relevant replacements stay unknown, not empty.
  for v_session in select s.* from public.weekly_final_source_correction_sessions s
    join public.weekly_source_cycles c on c.id=s.source_cycle_id
    where c.source_group_id=any(v_groups) and s.state in ('DRAFT','STAGING','READY','REVIEWED','PREPARING','PREPARED','COMMITTING')
      and exists(select 1 from public.weekly_source_client_manifests m
        where m.final_revision_id=s.expected_current_final_revision_id and m.client_id=v_contract.client_id) order by s.id
  loop
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','CORRECTION_DISCOVERY','session',to_jsonb(v_session)));
    if exists(select 1 from public.weekly_source_ordinary_pay_projection_receipts r
      where r.final_revision_id=v_session.expected_current_final_revision_id and r.root_timesheet_id=any(v_ids)
        and r.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED')) then
      if v_session.prepared_final_revision_id is null then v_unknown:=true; continue; end if;
    elsif v_session.prepared_final_revision_id is null then v_unknown:=true; continue; end if;
    begin v_context:=private.weekly_source_correct_final_prepared_context_v1(v_session.id);
    exception when sqlstate '55000' then v_unknown:=true; continue; end;
    if v_session.prepare_result_hash is distinct from private.weekly_source_sha256_jsonb_v1(
         'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_RESULT_V1',v_session.prepare_result_json)
       or v_context->'root_contexts' is distinct from v_session.prepare_result_json->'root_contexts'
       or not exists(select 1 from public.weekly_source_final_revisions f
         join public.weekly_source_cycles c on c.id=f.source_cycle_id
         left join public.weekly_source_report_scopes s on s.id=f.report_scope_id
         where f.id=v_session.expected_current_final_revision_id and f.state='CURRENT'
           and f.manifest_hash=v_session.expected_final_manifest_hash
           and f.finalised_at_utc is not null and isfinite(f.finalised_at_utc)
           and f.source_cycle_id=v_session.source_cycle_id and f.report_scope_id is not distinct from v_session.report_scope_id
           and ((f.authority_scope_kind='CYCLE' and c.current_final_revision_id=f.id)
             or (f.authority_scope_kind='NHSP_REPORT_SCOPE' and s.current_final_revision_id=f.id)))
       or not exists(select 1 from public.weekly_source_projection_publications p
         join public.weekly_source_uploads u on u.id=p.upload_id
         join public.weekly_source_final_revisions f on f.id=v_session.prepared_final_revision_id
         join public.weekly_source_format_profiles profile on profile.id=u.source_format_profile_id
         join public.weekly_source_final_revisions prior on prior.id=v_session.expected_current_final_revision_id
         join public.weekly_source_uploads prior_upload on prior_upload.id=prior.upload_id
         join public.weekly_source_format_profiles prior_profile on prior_profile.id=prior_upload.source_format_profile_id
         join public.weekly_source_cycles cycle on cycle.id=v_session.source_cycle_id
         join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
         left join public.weekly_source_report_scopes report_scope on report_scope.id=v_session.report_scope_id
         where p.id=v_session.replacement_projection_publication_id and u.id=v_session.replacement_correction_upload_id
           and p.state='CORRECTION_READY' and u.state='CORRECTION_READY'
           and p.correction_session_id=v_session.id and u.correction_session_id=v_session.id
           and p.source_cycle_id=v_session.source_cycle_id and u.source_cycle_id=v_session.source_cycle_id
           and p.report_scope_id is not distinct from v_session.report_scope_id
           and u.report_scope_id is not distinct from v_session.report_scope_id
           and u.purpose='FINAL_SOURCE_CORRECTION'
           and p.authority_scope_kind=v_session.authority_scope_kind
           and u.row_manifest_hash is not null and p.comparison_manifest_hash is not null and p.issue_set_hash is not null
           and p.authority_scope_version=case when v_session.authority_scope_kind='NHSP_REPORT_SCOPE'
             then report_scope.version else cycle.version end
           and prior_upload.state='CURRENT'
           and exists(select 1 from public.weekly_source_projection_publications current_publication
             where current_publication.id=case when v_session.authority_scope_kind='NHSP_REPORT_SCOPE'
               then report_scope.current_projection_publication_id else cycle.current_projection_publication_id end
               and current_publication.state='CURRENT' and current_publication.upload_id=prior_upload.id)
           and profile.final_authority_kind=prior_profile.final_authority_kind
           and (profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT')=(source_group.source_family='NHSP')
           and f.state='PREPARED' and f.reason='CORRECT_FINAL_SOURCE'
           and f.upload_id=u.id and f.source_cycle_id=cycle.id
           and f.report_scope_id is not distinct from v_session.report_scope_id
           and f.predecessor_revision_id=prior.id
           and v_session.prepare_result_json->>'final_revision_id'=f.id::text
           and f.manifest_hash=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_FINAL_REVISION_MANIFEST_V1',
             jsonb_build_object('source_cycle_id',cycle.id,'authority_scope_kind',v_session.authority_scope_kind,
               'report_scope_id',v_session.report_scope_id,'revision_number',f.revision_number,'upload_id',u.id,
               'authority_scope_version',p.authority_scope_version,'row_manifest_hash',encode(u.row_manifest_hash,'hex'),
               'comparison_manifest_hash',encode(p.comparison_manifest_hash,'hex'),'issue_set_hash',encode(p.issue_set_hash,'hex'),
               'profile_code',profile.profile_code,'profile_version',profile.version,
               'coverage_start_local_date',f.coverage_start_local_date,'coverage_end_local_date',f.coverage_end_local_date,
               'reason',f.reason,'predecessor_revision_id',f.predecessor_revision_id,
               'prior_state_cutoff_revision_id',f.prior_state_cutoff_revision_id,
               'policy_fingerprint',encode(f.policy_fingerprint,'hex')))) then
      v_unknown:=true; continue; end if;
    v_guard:=private.weekly_source_correct_final_office_preview_v1(v_session.id);
    if v_session.prepare_result_json->'office_preview' is distinct from v_guard
       or v_session.review_result_json->'office_preview' is distinct from v_guard
       or jsonb_array_length(v_guard->'blockers')<>0 then v_unknown:=true; continue; end if;
    if not exists(select 1 from jsonb_array_elements(v_context->'root_contexts') r
      where (r->>'root_timesheet_id')::uuid=any(v_ids)) then continue; end if;
    v_present:=true;
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','CORRECTION','session_id',v_session.id,
      'prepared_context',v_context,'prepare_result_hash',encode(v_session.prepare_result_hash,'hex')));
  end loop;

  -- Use actual committed member chunks, not a client-wide fence. Ordinary
  -- selection expanders already exclude Source-owned roots in their owner.
  for v_chunk in select c.* from public.invoice_operation_chunks c
    join public.invoice_operations o on o.id=c.operation_id
    where o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and c.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and (not c.is_manifest_member or c.manifest_committed)
      and coalesce(c.payload_json->>'is_selection_expander','false')<>'true'
      and (not c.is_manifest_member or coalesce(c.entity_type,'')<>'OPERATION') order by c.id
  loop
    select * into strict v_operation from public.invoice_operations where id=v_chunk.operation_id;
    v_observation:=false;
    v_relevant:=(v_chunk.entity_type='TIMESHEET' and v_chunk.entity_id=any(v_ids))
      or (v_chunk.entity_type='INVOICE' and v_chunk.entity_id=any(v_invoices))
      or (v_operation.entity_type='TIMESHEET' and v_operation.entity_id=any(v_ids))
      or (v_operation.entity_type='INVOICE' and v_operation.entity_id=any(v_invoices));
    if v_chunk.entity_type='TIMESHEET' and v_chunk.entity_id=any(v_ids) then
      if v_operation.operation_type in ('GENERATE_INVOICES','ISSUE_INVOICES','RECONCILE_INVOICE_WORK') then v_observation:=true;
      elsif v_operation.operation_type='BUILD_DOCUMENT' then v_observation:=null; end if;
    end if;
    if v_chunk.payload_json->>'command_type'='GENERATE_CREDIT_NOTE' then
      v_text:=coalesce(v_chunk.payload_json->>'source_invoice_id',v_chunk.entity_id::text);
      if coalesce(v_text,'')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
        if v_relevant then v_observation:=null; end if;
      else
        v_invoice_id:=v_text::uuid;
        if (v_chunk.entity_type='INVOICE' and v_chunk.entity_id=any(v_invoices)
          and v_chunk.entity_id is distinct from v_invoice_id) then
          v_observation:=null;
        elsif v_invoice_id=any(v_invoices) then
          v_relevant:=true;
          if v_chunk.entity_id is not null and v_chunk.entity_id<>v_invoice_id then v_observation:=null;
          else v_observation:=true; end if;
        end if;
      end if;
    elsif v_operation.operation_type='GENERATE_INVOICES' then
      v_members:=v_chunk.payload_json->'canonical_source_members';
      if v_chunk.payload_json ? 'canonical_source_members' and jsonb_typeof(v_members) is distinct from 'array' then
        -- A malformed primary manifest must not be replaced by fallback IDs.
        -- Fallback membership is discovery only, to qualify the relevant NULL.
        v_members:=coalesce(v_chunk.payload_json->'canonical_source_ids',v_chunk.payload_json->'source_ids','[]'::jsonb);
        if jsonb_typeof(v_members)='array' and exists(select 1 from jsonb_array_elements_text(v_members) s(value)
          where lower(s.value)=any(select id::text from unnest(v_ids) id)) then v_relevant:=true; end if;
        if v_relevant then v_observation:=null; end if;
        v_members:='[]'::jsonb;
      elsif v_members is null or jsonb_array_length(v_members)=0 then
        v_members:=coalesce(v_chunk.payload_json->'canonical_source_ids',v_chunk.payload_json->'source_ids','[]'::jsonb);
        if jsonb_typeof(v_members) is distinct from 'array' then
          if v_relevant then v_observation:=null; end if;
          v_members:='[]'::jsonb;
        else
          select coalesce(jsonb_agg(jsonb_build_object('source_type','TIMESHEET','source_id',s.value,
            'related_timesheet_id',s.value) order by s.ordinality),'[]'::jsonb) into v_members
            from jsonb_array_elements_text(v_members) with ordinality s(value,ordinality);
        end if;
      end if;
      -- Discover actual membership before validating ANY member, so malformed
      -- earlier evidence cannot disappear merely because the owned root is last.
      if exists(select 1 from jsonb_array_elements(v_members) m(value)
        where lower(m.value->>'related_timesheet_id')=any(select id::text from unnest(v_ids) id)
          or lower(m.value->>'source_id')=any(select id::text from unnest(v_ids) id)) then v_relevant:=true; end if;
      for v_member in select value from jsonb_array_elements(v_members) loop
        -- Canonical TIMESHEET members carry both independently valid identities.
        -- COALESCE cannot let a valid related ID conceal a malformed source ID.
        if upper(coalesce(v_member->>'source_type','TIMESHEET'))='TIMESHEET' then
          if coalesce(v_member->>'source_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             or coalesce(v_member->>'related_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            if v_relevant then v_observation:=null; end if;
            continue;
          end if;
          if (v_member->>'source_id')::uuid is distinct from (v_member->>'related_timesheet_id')::uuid then
            if v_relevant then v_observation:=null; end if;
            continue;
          end if;
        end if;
        v_text:=coalesce(v_member->>'related_timesheet_id',v_member->>'source_id');
        if coalesce(v_text,'')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          if v_relevant then v_observation:=null; end if;
          continue; end if;
        v_member_id:=v_text::uuid;
        if v_member_id=any(v_ids) then
          v_relevant:=true;
          if upper(coalesce(v_member->>'source_type','TIMESHEET'))='TIMESHEET' then
            if v_observation is not null then v_observation:=true; end if;
          else v_observation:=null; end if;
        end if;
      end loop;
    else
      v_text:=coalesce(v_chunk.payload_json->>'invoice_id',case when v_chunk.entity_type='INVOICE' then v_chunk.entity_id::text end);
      if v_chunk.entity_type='INVOICE' and v_chunk.entity_id=any(v_invoices)
         and v_text is distinct from v_chunk.entity_id::text then v_observation:=null;
      elsif v_text ~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' and v_text::uuid=any(v_invoices) then
        v_relevant:=true;
        if v_operation.operation_type='ISSUE_INVOICES' then v_observation:=true;
        elsif v_operation.operation_type='BUILD_DOCUMENT' then v_observation:=null; end if;
      end if;
    end if;
    if v_relevant then
      v_present:=v_present or v_observation is true; v_unknown:=v_unknown or v_observation is null;
      v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','OPERATION_CHUNK',
        'operation',to_jsonb(v_operation),'chunk',to_jsonb(v_chunk),'present',v_observation));
    end if;
  end loop;
  -- ISSUE can be queued before its exact invoice chunks have been expanded.
  for v_operation in select * from public.invoice_operations where operation_type='ISSUE_INVOICES'
    and status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED') order by id loop
    v_relevant:=v_operation.entity_type='INVOICE' and v_operation.entity_id=any(v_invoices);
    v_observation:=case when v_relevant then true else false end;
    if v_operation.input_json ? 'invoice_ids' then
      if jsonb_typeof(v_operation.input_json->'invoice_ids') is distinct from 'array' then
        if v_relevant then v_observation:=null; end if;
      else
        v_members:=v_operation.input_json->'invoice_ids';
        if exists(select 1 from jsonb_array_elements_text(v_members) s(value)
          where lower(s.value)=any(select id::text from unnest(v_invoices) id)) then v_relevant:=true; end if;
        if v_relevant then
          if exists(select 1 from jsonb_array_elements_text(v_members) s(value)
            where coalesce(s.value,'')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
            then v_observation:=null; else v_observation:=true; end if;
        end if;
      end if;
    end if;
    if v_relevant then
      v_present:=v_present or v_observation is true; v_unknown:=v_unknown or v_observation is null;
      v_basis:=v_basis||jsonb_build_array(jsonb_build_object('lane','QUEUED_ISSUE',
        'operation',to_jsonb(v_operation),'present',v_observation));
    end if;
  end loop;
  return jsonb_build_object('scope',v_scope,'present',case when v_present then true when v_unknown then null else false end,
    'discovery_complete',not v_unknown,'basis',v_basis);
end;
$function$;
alter function private.weekly_source_invoice_duty_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_invoice_duty_v1(uuid) from public,anon,authenticated,service_role;
commit;
