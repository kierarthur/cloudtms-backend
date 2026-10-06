-- Selected-duty factual evidence, never a calculator or payment/invoice owner.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_selected_source_witness_v1(
  p_family_id uuid,p_action_cycle_id uuid,p_work_event_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_contract public.contracts%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_protected public.weekly_exceptional_pay_family_events%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_mapping public.weekly_source_row_resolutions%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_final public.weekly_source_final_revisions%rowtype;
  v_transition public.weekly_source_state_transitions%rowtype;
  v_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_choice private.weekly_source_office_row_choices%rowtype;
  v_mode text; v_kind text; v_basis_kind text;
  v_source jsonb; v_selected jsonb; v_current_row jsonb;
  v_basis jsonb; v_scope jsonb; v_result jsonb; v_history jsonb;
  v_census jsonb:='[]'::jsonb;
  v_rows_checked jsonb;
  v_matches jsonb; v_root_ids uuid[];
  v_count integer; v_generation integer; v_has_positive boolean; v_has_negative boolean;
begin
  if p_family_id is null or p_action_cycle_id is null or p_work_event_id is null then
    raise exception 'WEEKLY_SOURCE_SELECTED_WITNESS_IDENTITY_REQUIRED' using errcode='22023'; end if;
  select * into strict v_family from public.weekly_exceptional_pay_target_families where id=p_family_id;
  select * into strict v_contract from public.contracts where id=v_family.contract_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=p_action_cycle_id;
  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;
  if v_contract.candidate_id is distinct from v_family.candidate_id
     or v_event.candidate_id is distinct from v_family.candidate_id
     or v_event.client_id is distinct from v_contract.client_id
     or v_event.first_source_group_id is distinct from v_cycle.source_group_id
     or v_event.work_date not between v_family.week_start_date and v_family.week_ending_date
     or (v_cycle.scope_client_id is not null and v_cycle.scope_client_id<>v_contract.client_id)
     or v_family.agency_id is distinct from (select agency_id from public.weekly_source_groups where id=v_cycle.source_group_id)
     or v_contract.week_ending_weekday_snapshot not between 0 and 6
     or v_contract.week_ending_weekday_snapshot is null
     or v_event.work_date+((v_contract.week_ending_weekday_snapshot-extract(dow from v_event.work_date)::integer+7)%7)
          is distinct from v_family.week_ending_date then
    raise exception 'WEEKLY_SOURCE_SELECTED_WITNESS_SCOPE_INVALID' using errcode='55000'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(v_family.root_timesheet_id);
  if v_scope is null or v_scope->>'target_family_id' is distinct from v_family.id::text then return null; end if;
  v_root_ids:=private.weekly_source_invoice_family_timesheet_ids_v1(v_family.root_timesheet_id);
  if cardinality(v_root_ids) is null or cardinality(v_root_ids)=0 then return null; end if;
  v_mode:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_event.work_date)->>'c1_source_mode';
  v_source:=private.weekly_source_protected_final_source_context_v1(v_family.id,v_cycle.id,v_event.id);
  select count(*),jsonb_agg(s.value)->0 into v_count,v_selected
    from jsonb_array_elements(v_source->'client_sources') s(value)
    where s.value->>'external_identity'=v_event.id::text;
  if v_count<>1 then return null; end if;

  -- Reuse the exact current-row reader, including Final precedence, original
  -- report scope, configured workweek and raw normalised-row hash domain.
  select count(*),jsonb_agg(s.source)->0 into v_count,v_current_row
    from public.weekly_source_row_resolutions r
    cross join lateral (select private.weekly_source_manual_review_source_v2(r.upload_row_id) source) s
    where r.work_event_id=v_event.id and r.candidate_id=v_family.candidate_id
      and r.client_id=v_contract.client_id and r.contract_id=v_contract.id
      and s.source is not null and s.source->>'row_resolution_id'=r.id::text
      and (v_source->'source_observed'='false'::jsonb
        or (v_mode='NHSP_WEEKLY' and exists(select 1 from public.weekly_source_billing_movements m
          where m.id::text=v_selected->>'client_source_id' and m.nhsp_upload_row_id=r.upload_row_id))
        or (v_mode='HEALTHROSTER_WEEKLY' and exists(select 1 from public.weekly_source_state_transitions t
          join public.weekly_source_final_snapshot_lines sn on sn.id=t.new_snapshot_line_id
          where t.id::text=v_selected->>'client_source_id' and sn.upload_row_id=r.upload_row_id)));
  if v_count>1 then return null; end if;
  if v_count=1 then
    if v_current_row->>'source_group_id' is distinct from v_cycle.source_group_id::text then return null; end if;
    v_kind:='CURRENT_ROW';
    v_basis_kind:=case when v_current_row->>'final_revision_id' is null
      then 'CURRENT_PROVISIONAL_ROW' else 'CURRENT_FINAL_ROW' end;
    v_basis:=v_current_row;
  else
    -- Latest selected clocks belong to the actual family event. Other duties
    -- in the same Timesheet are not a whole-family absence veto.
    select * into v_protected from public.weekly_exceptional_pay_family_events e
      where e.family_id=v_family.id and e.durable_work_event_id=v_event.id
      order by e.event_sequence desc limit 1;
    if not found then return null; end if;
    -- Failed/rebuilding authority can exist before a logical upload/row does.
    -- No work-date or nullable unparsed identity proves that it is unrelated.
    if exists(select 1 from public.weekly_source_cycles c
      where c.source_group_id=v_cycle.source_group_id
        and (c.scope_client_id is null or c.scope_client_id=v_contract.client_id)
        and c.projection_state in ('REBUILDING','FAILED'))
      or exists(select 1 from public.weekly_source_report_scopes s
        where s.source_group_id=v_cycle.source_group_id and s.client_id=v_contract.client_id
          and s.projection_state in ('REBUILDING','FAILED'))
      or exists(select 1 from public.weekly_source_upload_attempts a
        left join public.weekly_source_cycles c on c.id=a.source_cycle_id
        left join public.weekly_source_report_scopes rs on rs.id=a.report_scope_id
        where a.agency_id=v_family.agency_id
          and (a.source_group_id is null or a.source_group_id=v_cycle.source_group_id)
          and (c.scope_client_id is null or c.scope_client_id=v_contract.client_id)
          and (rs.client_id is null or rs.client_id=v_contract.client_id)
          and a.logical_upload_id is null and a.result in ('PARTIAL','CORRUPT','FAILED','CONFLICT','REJECTED')
          and not exists(select 1 from public.weekly_source_uploads repaired
            where a.content_sha256 is not null and repaired.content_sha256=a.content_sha256
              and repaired.source_cycle_id=a.source_cycle_id
              and repaired.report_scope_id is not distinct from a.report_scope_id
              and repaired.purpose=a.purpose and repaired.declared_scope_fingerprint=a.declared_scope_fingerprint
              and repaired.uploaded_at_utc>=a.attempted_at_utc and repaired.state='CURRENT'
              and (repaired.coverage_state='COMPLETE' or (
                repaired.coverage_proof_kind='NHSP_TRUST_REPORT_SCOPE' and repaired.report_scope_id is not null
                and exists(select 1 from public.weekly_source_format_profiles p
                  where p.id=repaired.source_format_profile_id and p.final_authority_kind='NHSP_TRUST_BACKING_REPORT')
                and repaired.row_manifest_hash=private.weekly_source_upload_manifest_hash_v1(repaired.id)
              )))) then return null; end if;
    -- Census every potentially relevant non-superseded import across cutoffs.
    -- Nullable resolution identity is UNKNOWN, not evidence of another worker.
    for v_upload in select u.* from public.weekly_source_uploads u
      join public.weekly_source_cycles c on c.id=u.source_cycle_id
      left join public.weekly_source_report_scopes rs on rs.id=u.report_scope_id
      where c.source_group_id=v_cycle.source_group_id and u.state<>'SUPERSEDED'
        and (c.scope_client_id is null or c.scope_client_id=v_contract.client_id)
        and (rs.client_id is null or rs.client_id=v_contract.client_id)
        and (u.confirmed_coverage_start_local_date is null or u.confirmed_coverage_end_local_date is null
          or daterange(v_event.work_date-1,v_event.work_date+2,'[)') &&
            daterange(u.confirmed_coverage_start_local_date,u.confirmed_coverage_end_local_date+1,'[)')
          or exists(select 1 from public.weekly_source_upload_rows rr where rr.upload_id=u.id
            and (rr.work_date is null or rr.work_date between v_event.work_date-1 and v_event.work_date+1)))
      order by u.source_cycle_id,u.report_scope_id,u.id
    loop
      if v_upload.state in ('STAGING','SEALED','CORRECTION_READY','REJECTED') then return null; end if;
      if v_upload.state<>'CURRENT' or v_upload.row_manifest_hash is null then return null; end if;
      select count(*) into v_count from public.weekly_source_projection_publications p
        where p.upload_id=v_upload.id and p.source_cycle_id=v_upload.source_cycle_id
          and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT';
      if v_count<>1 then return null; end if;
      select * into strict v_publication from public.weekly_source_projection_publications p
        where p.upload_id=v_upload.id and p.source_cycle_id=v_upload.source_cycle_id
          and p.report_scope_id is not distinct from v_upload.report_scope_id and p.state='CURRENT';
      if v_publication.published_at_utc is null or not isfinite(v_publication.published_at_utc)
         or (v_upload.report_scope_id is null and v_publication.authority_scope_kind is distinct from 'CYCLE')
         or (v_upload.report_scope_id is not null and (
           v_publication.authority_scope_kind is distinct from 'NHSP_REPORT_SCOPE'
           or not exists(select 1 from public.weekly_source_report_scopes rs
             join public.weekly_source_cycles c on c.id=rs.source_cycle_id
             where rs.id=v_upload.report_scope_id and rs.source_cycle_id=v_upload.source_cycle_id
               and rs.source_group_id=v_cycle.source_group_id and c.source_group_id=rs.source_group_id
               and rs.client_id=v_contract.client_id and rs.cutoff_at_utc=c.cutoff_at_utc)
         )) then return null; end if;
      if v_publication.authority_scope_version>2147483647
         or coalesce(v_publication.projection_generation,0)>2147483647 then
        raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003'; end if;
      v_generation:=coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer);
      if v_generation is null then return null; end if;
      -- Finalised authority deliberately uses its Final pointer, not equality
      -- with the provisional version that Finalisation legitimately advanced.
      if not exists(select 1 from public.weekly_source_final_revisions f
        join public.weekly_source_cycles c on c.id=f.source_cycle_id
        left join public.weekly_source_report_scopes rs on rs.id=f.report_scope_id
        where f.upload_id=v_upload.id and f.state='CURRENT'
          and f.source_cycle_id=v_upload.source_cycle_id
          and f.report_scope_id is not distinct from v_upload.report_scope_id
          and f.finalised_at_utc is not null and isfinite(f.finalised_at_utc)
          and ((f.authority_scope_kind='CYCLE' and f.report_scope_id is null and c.current_final_revision_id=f.id)
            or (f.authority_scope_kind='NHSP_REPORT_SCOPE' and rs.current_final_revision_id=f.id
              and rs.source_cycle_id=c.id and rs.source_group_id=c.source_group_id
              and rs.client_id=v_contract.client_id and rs.cutoff_at_utc=c.cutoff_at_utc))) then
        if not exists(select 1 from public.weekly_source_cycles c
          left join public.weekly_source_report_scopes rs on rs.id=v_upload.report_scope_id
          where c.id=v_upload.source_cycle_id and (
            (v_upload.report_scope_id is null and c.current_complete_upload_id=v_upload.id
              and c.current_projection_publication_id=v_publication.id and c.projection_state='CURRENT'
              and c.version=v_publication.authority_scope_version)
            or (rs.id is not null and rs.current_complete_upload_id=v_upload.id
              and rs.current_projection_publication_id=v_publication.id and rs.projection_state='CURRENT'
              and rs.version=v_publication.authority_scope_version))) then return null; end if;
      end if;
      v_rows_checked:='[]'::jsonb;
      for v_row in select * from public.weekly_source_upload_rows r where r.upload_id=v_upload.id
        and (r.work_date is null or r.work_date between v_event.work_date-1 and v_event.work_date+1)
        order by r.id
      loop
        select * into v_mapping from public.weekly_source_row_resolutions r
          where r.upload_row_id=v_row.id and r.generation=v_generation;
        if not found or v_mapping.mapping_state<>'RESOLVED' then return null; end if;
        v_rows_checked:=v_rows_checked||jsonb_build_array(jsonb_build_object(
          'upload_row_id',v_row.id,'source_row_hash',encode(v_row.normalised_row_hash,'hex'),
          'row_resolution_id',v_mapping.id,'generation',v_mapping.generation,
          'candidate_id',v_mapping.candidate_id,'client_id',v_mapping.client_id,
          'contract_id',v_mapping.contract_id,'work_event_id',v_mapping.work_event_id,
          'source_row_fingerprint',encode(v_mapping.source_row_fingerprint,'hex'),
          'work_event_match_fingerprint',encode(v_mapping.work_event_match_fingerprint,'hex'),
          'qualification_profile_fingerprint',encode(v_mapping.qualification_profile_fingerprint,'hex'),
          'qualifying_contract_set_hash',encode(v_mapping.qualifying_contract_set_hash,'hex'),
          'contract_and_rate_fingerprint',encode(v_mapping.contract_and_rate_fingerprint,'hex'),
          'effective_policy_fingerprint',encode(v_mapping.effective_policy_fingerprint,'hex')));
        if v_mapping.candidate_id<>v_family.candidate_id or v_mapping.client_id<>v_contract.client_id
           or v_mapping.contract_id<>v_contract.id then continue; end if;
        if v_mapping.candidate_id is null or v_mapping.client_id is null or v_mapping.contract_id is null
           or v_row.work_date is null then return null; end if;
        if v_mapping.work_event_id=v_event.id then
          -- Retained physical positive history is required for a genuine NHSP
          -- full reversal. It is not a surviving payable row: the active owner
          -- must already prove extinction, and the complete sealed physical
          -- history is qualified below. A provisional positive cannot pass.
          if not exists(select 1 from public.weekly_source_billing_movements m
            join public.weekly_source_final_revisions f on f.id=m.final_revision_id
              and f.upload_id=v_upload.id and f.source_cycle_id=v_upload.source_cycle_id
              and f.report_scope_id=v_upload.report_scope_id and f.state='CURRENT'
              and f.finalised_at_utc is not null and isfinite(f.finalised_at_utc)
            join public.weekly_source_report_scopes rs on rs.id=f.report_scope_id
              and rs.current_final_revision_id=f.id and rs.client_id=v_contract.client_id
            join public.weekly_source_client_manifests cm on cm.final_revision_id=f.id
              and cm.client_id=v_contract.client_id and cm.source_group_id=v_cycle.source_group_id
              and cm.source_cycle_id=f.source_cycle_id
            join public.weekly_source_manifest_movements mm on mm.client_manifest_id=cm.id
              and mm.billing_movement_id=m.id and mm.movement_hash=m.movement_economic_hash
            where v_mode='NHSP_WEEKLY' and v_source->'source_observed'='true'::jsonb
              and v_source#>'{source_proposal,source_present}'='false'::jsonb
              and m.nhsp_upload_row_id=v_row.id and m.work_event_id=v_event.id
              and m.candidate_id=v_family.candidate_id and m.contract_id=v_contract.id
              and m.actual_client_id=v_contract.client_id and m.invoice_timesheet_id=any(v_root_ids)
              and m.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
              and m.source_line_kind in ('NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE'))
            and not exists(select 1 from public.weekly_source_final_snapshot_lines sn
              join public.weekly_source_final_revisions old_final on old_final.id=sn.final_revision_id
                and old_final.upload_id=v_upload.id and old_final.source_cycle_id=v_upload.source_cycle_id
                and old_final.report_scope_id is null and old_final.state='CURRENT'
                and old_final.finalised_at_utc is not null and isfinite(old_final.finalised_at_utc)
              join public.weekly_source_cycles old_cycle on old_cycle.id=old_final.source_cycle_id
                and old_cycle.current_final_revision_id=old_final.id and old_cycle.source_group_id=v_cycle.source_group_id
              join public.weekly_source_client_manifests cm on cm.final_revision_id=old_final.id
                and cm.source_cycle_id=old_cycle.id and cm.source_group_id=old_cycle.source_group_id
                and cm.client_id=v_contract.client_id
              where v_mode='HEALTHROSTER_WEEKLY' and v_source->'source_observed'='true'::jsonb
                and v_source#>'{source_proposal,source_present}'='false'::jsonb
                and sn.upload_row_id=v_row.id and sn.row_resolution_id=v_mapping.id
                and sn.work_event_id=v_event.id and sn.candidate_id=v_family.candidate_id
                and sn.client_id=v_contract.client_id and sn.contract_id=v_contract.id
                and sn.work_date=v_event.work_date
                and exists(select 1 from public.weekly_source_state_transitions cancelled
                  join public.weekly_source_final_revisions cancel_final on cancel_final.id=cancelled.final_revision_id
                    and cancel_final.source_cycle_id=cancelled.finalisation_cycle_id
                    and cancel_final.state='CURRENT' and cancel_final.authority_scope_kind='CYCLE'
                  join public.weekly_source_cycles cancel_cycle on cancel_cycle.id=cancel_final.source_cycle_id
                    and cancel_cycle.current_final_revision_id=cancel_final.id
                    and cancel_cycle.source_group_id=v_cycle.source_group_id
                  where cancelled.id::text=v_selected->>'client_source_id' and cancelled.work_event_id=v_event.id
                    and cancelled.outcome='CANCEL' and cancelled.previous_present and not cancelled.new_present
                    and cancelled.previous_snapshot_line_id is not null and cancelled.new_snapshot_line_id is null))
            and not exists(select 1 from public.weekly_source_final_revisions cancel_final
              join public.weekly_source_format_profiles zero_profile on zero_profile.id=v_upload.source_format_profile_id
                and zero_profile.final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
                and zero_profile.omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE'
              join public.weekly_source_uploads actual_upload on actual_upload.id=cancel_final.upload_id
                and cancel_final.upload_id=v_upload.id and cancel_final.state='CURRENT'
                and cancel_final.authority_scope_kind='CYCLE' and cancel_final.report_scope_id is null
                and cancel_final.finalised_at_utc is not null and isfinite(cancel_final.finalised_at_utc)
              join public.weekly_source_cycles cancel_cycle on cancel_cycle.id=cancel_final.source_cycle_id
                and cancel_cycle.current_final_revision_id=cancel_final.id
                and cancel_cycle.source_group_id=v_cycle.source_group_id
              join public.weekly_source_client_manifests cm on cm.final_revision_id=cancel_final.id
                and cm.source_cycle_id=cancel_cycle.id and cm.source_group_id=cancel_cycle.source_group_id
                and cm.client_id=v_contract.client_id
              where v_mode='HEALTHROSTER_WEEKLY'
                and v_source#>'{source_proposal,source_present}'='false'::jsonb
                and v_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
                and v_upload.coverage_state='COMPLETE' and v_upload.coverage_timezone='Europe/London'
                and v_upload.coverage_confirmed_at_utc is not null and isfinite(v_upload.coverage_confirmed_at_utc)
                and v_upload.coverage_confirmed_by_user_id is not null
                and v_upload.confirmed_coverage_start_local_date=cancel_final.coverage_start_local_date
                and v_upload.confirmed_coverage_end_local_date=cancel_final.coverage_end_local_date
                and v_event.work_date between cancel_final.coverage_start_local_date and cancel_final.coverage_end_local_date)
          then return null; end if;
          continue;
        end if;
        select * into v_choice from private.weekly_source_office_row_choices c
          where c.upload_row_id=v_row.id and c.candidate_id=v_family.candidate_id
            and c.client_id=v_contract.client_id and c.contract_id=v_contract.id order by c.id desc limit 1;
        if found and v_choice.separate_shift and v_choice.work_event_id is null then continue; end if;
        v_matches:=private.weekly_source_protected_match_candidates_v1(v_row.id,v_family.candidate_id,v_contract.client_id);
        if exists(select 1 from jsonb_array_elements(v_matches) m
          where m->>'work_event_id'=v_event.id::text and m->>'contract_id'=v_contract.id::text
            and (m->'schedule_compatible'='true'::jsonb or m->'retained_source_identity'='true'::jsonb)) then return null; end if;
        if v_row.start_at_local is null or v_row.end_at_local is null
           or v_protected.start_at_local is null or v_protected.end_at_local is null
           or (v_row.start_at_local<v_protected.end_at_local and v_protected.start_at_local<v_row.end_at_local)
        then return null; end if;
      end loop;
      if exists(select 1 from public.weekly_source_format_profiles p
        where p.id=v_upload.source_format_profile_id and p.final_authority_kind='NHSP_TRUST_BACKING_REPORT') then
        -- NHSP completeness is the report's actual row/count/scope seal. It
        -- deliberately has no roster date-coverage attestation or inference.
        if v_upload.coverage_proof_kind is distinct from 'NHSP_TRUST_REPORT_SCOPE'
           or v_upload.report_scope_id is null
           or v_upload.row_manifest_hash is distinct from private.weekly_source_upload_manifest_hash_v1(v_upload.id)
        then return null; end if;
      elsif v_upload.coverage_state is distinct from 'COMPLETE' then return null; end if;
      v_census:=v_census||jsonb_build_array(jsonb_build_object(
        'upload_id',v_upload.id,'source_cycle_id',v_upload.source_cycle_id,'report_scope_id',v_upload.report_scope_id,
        'source_cycle_cutoff_at_utc',(select c.cutoff_at_utc from public.weekly_source_cycles c where c.id=v_upload.source_cycle_id),
        'publication_at_utc',v_publication.published_at_utc,'authority_scope_kind',v_publication.authority_scope_kind,
        'row_manifest_hash',encode(v_upload.row_manifest_hash,'hex'),
        'declared_scope_fingerprint',encode(v_upload.declared_scope_fingerprint,'hex'),
        'publication_id',v_publication.id,'authority_scope_version',v_publication.authority_scope_version,
        'projection_generation',v_generation,'comparison_manifest_hash',encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',encode(v_publication.issue_set_hash,'hex'),'rows_checked',v_rows_checked,
        'current_final_revisions',(select coalesce(jsonb_agg(jsonb_build_object('id',f.id,
          'manifest_hash',encode(f.manifest_hash,'hex'),'policy_fingerprint',encode(f.policy_fingerprint,'hex'))
          order by f.id),'[]'::jsonb) from public.weekly_source_final_revisions f where f.upload_id=v_upload.id and f.state='CURRENT')));
    end loop;

    if v_source#>'{source_proposal,source_present}' is distinct from 'false'::jsonb
       or v_source#>>'{source_proposal,source_minutes}' is distinct from '0' then return null; end if;
    if v_mode='NHSP_WEEKLY' and v_source->'source_observed'='true'::jsonb then
      -- The active-position owner has already proved this event extinguished.
      -- Keep the REAL physical positive/negative history, not its synthetic
      -- selected-event absence proposal encoding, as the certificate basis.
      select coalesce(jsonb_agg(jsonb_build_object('movement_id',m.id,'final_revision_id',f.id,
          'movement_hash',encode(m.movement_economic_hash,'hex'),'source_row_id',u.id,
          'source_row_hash',encode(u.normalised_row_hash,'hex'),'row_resolution_id',r.id,
          'backing_report_id',br.id,'row_manifest_hash',encode(br.row_manifest_hash,'hex'),
          'final_manifest_hash',encode(f.manifest_hash,'hex'),'client_manifest_id',cm.id,
          'client_manifest_hash',encode(cm.manifest_hash,'hex'),'manifest_movement_hash',encode(mm.movement_hash,'hex'))
          order by f.finalised_at_utc,m.id),'[]'::jsonb),
        bool_or(m.source_line_kind='NHSP_PHYSICAL_POSITIVE'),bool_or(m.source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE')
        into v_history,v_has_positive,v_has_negative
        from public.weekly_source_billing_movements m
        join public.weekly_source_final_revisions f on f.id=m.final_revision_id and f.state='CURRENT'
        join public.weekly_source_cycles c on c.id=f.source_cycle_id and c.source_group_id=v_cycle.source_group_id
        join public.weekly_source_report_scopes rs on rs.id=f.report_scope_id and rs.current_final_revision_id=f.id
          and rs.client_id=v_contract.client_id and rs.cutoff_at_utc=c.cutoff_at_utc
        join public.weekly_source_upload_rows u on u.id=m.nhsp_upload_row_id and u.upload_id=f.upload_id and u.work_date=v_event.work_date
        join public.weekly_source_row_resolutions r on r.id=(m.source_facts_json->>'row_resolution_id')::uuid
          and r.upload_row_id=u.id and r.mapping_state='RESOLVED' and r.work_event_id=v_event.id
          and r.candidate_id=v_family.candidate_id and r.client_id=v_contract.client_id and r.contract_id=v_contract.id
        join public.weekly_source_uploads up on up.id=f.upload_id and up.report_scope_id=rs.id and up.source_cycle_id=c.id
        join public.weekly_source_nhsp_backing_reports br on br.final_revision_id=f.id and br.upload_id=up.id
          and br.client_id=v_contract.client_id and br.report_scope_id=rs.id and br.cutoff_at_utc=rs.cutoff_at_utc
          and br.row_manifest_hash=up.row_manifest_hash
        join public.weekly_source_client_manifests cm on cm.final_revision_id=f.id and cm.client_id=v_contract.client_id
          and cm.source_cycle_id=c.id and cm.source_group_id=c.source_group_id
        join public.weekly_source_manifest_movements mm on mm.client_manifest_id=cm.id
          and mm.billing_movement_id=m.id and mm.movement_hash=m.movement_economic_hash
        where m.invoice_timesheet_id=any(v_root_ids) and m.work_event_id=v_event.id
          and m.candidate_id=v_family.candidate_id and m.contract_id=v_contract.id and m.actual_client_id=v_contract.client_id
          and m.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
          and m.source_line_kind in ('NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE');
      if v_has_positive is distinct from true or v_has_negative is distinct from true then return null; end if;
      if exists(select 1 from public.weekly_source_billing_movements m
        where m.invoice_timesheet_id=any(v_root_ids) and m.work_event_id=v_event.id
          and m.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
          and exists(select 1 from public.weekly_source_final_revisions f where f.id=m.final_revision_id and f.state='CURRENT')
          and not exists(select 1 from jsonb_array_elements(v_history) h where h->>'movement_id'=m.id::text)) then return null; end if;
      v_kind:='CERTIFIED_ABSENCE'; v_basis_kind:='NHSP_FULL_REVERSAL'; v_basis:=jsonb_build_object('physical_history',v_history);
    elsif v_mode='HEALTHROSTER_WEEKLY' then
      if v_source->'source_observed'='true'::jsonb then
        select * into v_transition from public.weekly_source_state_transitions t where t.id=(v_selected->>'client_source_id')::uuid
          and t.work_event_id=v_event.id and t.outcome='CANCEL' and t.previous_present and not t.new_present
          and t.previous_snapshot_line_id is not null and t.new_snapshot_line_id is null;
        if not found then return null; end if;
        select * into strict v_snapshot from public.weekly_source_final_snapshot_lines where id=v_transition.previous_snapshot_line_id;
        if v_snapshot.work_event_id is distinct from v_event.id or v_snapshot.candidate_id is distinct from v_family.candidate_id
           or v_snapshot.client_id is distinct from v_contract.client_id or v_snapshot.contract_id is distinct from v_contract.id
           or v_snapshot.work_date is distinct from v_event.work_date then return null; end if;
        if not exists(select 1 from public.weekly_source_upload_rows raw
          join public.weekly_source_row_resolutions resolution on resolution.id=v_snapshot.row_resolution_id
            and resolution.upload_row_id=raw.id and resolution.mapping_state='RESOLVED'
          join public.weekly_source_final_revisions previous_final on previous_final.id=v_snapshot.final_revision_id
            and previous_final.upload_id=raw.upload_id
          join public.weekly_source_cycles previous_cycle on previous_cycle.id=previous_final.source_cycle_id
            and previous_cycle.source_group_id=v_cycle.source_group_id
          where raw.id=v_snapshot.upload_row_id and raw.work_date=v_event.work_date
            and resolution.work_event_id=v_event.id and resolution.candidate_id=v_family.candidate_id
            and resolution.client_id=v_contract.client_id and resolution.contract_id=v_contract.id)
        then return null; end if;
        select * into v_final from public.weekly_source_final_revisions where id=v_transition.final_revision_id;
        if not found or v_transition.finalisation_cycle_id is distinct from v_final.source_cycle_id then return null; end if;
        v_basis_kind:='ROSTER_CANCEL';
      else
        select count(*),jsonb_agg(to_jsonb(f))->0 into v_count,v_basis
          from public.weekly_source_final_revisions f join public.weekly_source_cycles c on c.id=f.source_cycle_id
          join public.weekly_source_client_manifests m on m.final_revision_id=f.id and m.client_id=v_contract.client_id
          where c.source_group_id=v_cycle.source_group_id and f.state='CURRENT' and c.current_final_revision_id=f.id
            and f.authority_scope_kind='CYCLE' and v_event.work_date between f.coverage_start_local_date and f.coverage_end_local_date;
        if v_count>1 then return null; end if;
        if v_count=1 then
          select * into strict v_final from public.weekly_source_final_revisions where id=(v_basis->>'id')::uuid;
          v_basis_kind:='ROSTER_COVERAGE_OMISSION';
        end if;
      end if;
      if v_final.id is not null then
      select * into strict v_upload from public.weekly_source_uploads where id=v_final.upload_id;
      if v_final.state<>'CURRENT' or v_final.authority_scope_kind<>'CYCLE' or v_final.finalised_at_utc is null
         or not isfinite(v_final.finalised_at_utc) or v_final.coverage_timezone is distinct from 'Europe/London'
         or v_upload.coverage_timezone is distinct from 'Europe/London' or v_upload.coverage_state is distinct from 'COMPLETE'
         or v_upload.coverage_confirmed_at_utc is null or v_upload.coverage_confirmed_by_user_id is null
         or v_upload.confirmed_coverage_start_local_date is distinct from v_final.coverage_start_local_date
         or v_upload.confirmed_coverage_end_local_date is distinct from v_final.coverage_end_local_date
         or v_event.work_date not between v_final.coverage_start_local_date and v_final.coverage_end_local_date
         or not exists(select 1 from public.weekly_source_cycles c where c.id=v_final.source_cycle_id
           and c.source_group_id=v_cycle.source_group_id and c.current_final_revision_id=v_final.id)
         or not exists(select 1 from public.weekly_source_format_profiles p where p.id=v_upload.source_format_profile_id
           and p.final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
           and p.omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE')
         or not exists(select 1 from public.weekly_source_client_manifests m where m.final_revision_id=v_final.id
           and m.source_cycle_id=v_final.source_cycle_id and m.source_group_id=v_cycle.source_group_id and m.client_id=v_contract.client_id)
      then return null; end if;
      v_kind:='CERTIFIED_ABSENCE'; v_basis:=jsonb_build_object('final_revision_id',v_final.id,
        'manifest_hash',encode(v_final.manifest_hash,'hex'),'policy_fingerprint',encode(v_final.policy_fingerprint,'hex'),
        'upload_id',v_upload.id,'row_manifest_hash',encode(v_upload.row_manifest_hash,'hex'),
        'source_cycle_id',v_final.source_cycle_id,
        'source_cycle_cutoff_at_utc',(select c.cutoff_at_utc from public.weekly_source_cycles c where c.id=v_final.source_cycle_id),
        'declared_scope_fingerprint',encode(v_upload.declared_scope_fingerprint,'hex'),
        'client_manifests',(select jsonb_agg(jsonb_build_object('id',m.id,'manifest_hash',encode(m.manifest_hash,'hex'))
          order by m.id) from public.weekly_source_client_manifests m where m.final_revision_id=v_final.id
            and m.source_cycle_id=v_final.source_cycle_id and m.source_group_id=v_cycle.source_group_id and m.client_id=v_contract.client_id),
        'coverage_start',v_final.coverage_start_local_date,'coverage_end',v_final.coverage_end_local_date,
        'transition_id',v_transition.id,'transition_fingerprint',encode(v_transition.transition_fingerprint,'hex'),
        'previous_snapshot_id',v_snapshot.id,'previous_snapshot_hash',encode(v_snapshot.snapshot_line_hash,'hex'),
        'previous_upload_row_id',v_snapshot.upload_row_id,'previous_row_resolution_id',v_snapshot.row_resolution_id);
      end if;
    end if;
    if v_kind is null then
      if v_event.identity_kind<>'OFFICE_PROTECTED_SHIFT'
         or v_source->'source_observed' is distinct from 'false'::jsonb
         or exists(select 1 from public.weekly_source_row_resolutions r where r.work_event_id=v_event.id)
         or exists(select 1 from public.weekly_work_event_source_links l where l.work_event_id=v_event.id)
         or exists(select 1 from public.weekly_source_final_snapshot_lines s where s.work_event_id=v_event.id)
         or exists(select 1 from public.weekly_source_billing_movements m where m.work_event_id=v_event.id)
      then return null; end if;
      v_kind:='CERTIFIED_ABSENCE'; v_basis_kind:='NO_IMPORT_YET';
      v_basis:=jsonb_build_object('work_event_id',v_event.id,'durable_identity_hash',encode(v_event.durable_identity_hash,'hex'),
        'first_source_group_id',v_event.first_source_group_id,'family_event_id',v_protected.id,
        'family_event_sequence',v_protected.event_sequence,'action_cycle_id',v_cycle.id,
        'action_cycle_version',v_cycle.version,'action_cycle_projection_state',v_cycle.projection_state);
    end if;
    v_basis:=v_basis||jsonb_build_object('checked_current_imports',v_census);
  end if;
  v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_SELECTED_SOURCE_WITNESS_V1',
    'scope',v_scope||jsonb_build_object('work_event_id',v_event.id,'work_date',v_event.work_date,
      'source_group_id',v_cycle.source_group_id,'action_cycle_id',v_cycle.id),
    'kind',v_kind,'basis_kind',v_basis_kind,'source_proposal',v_source->'source_proposal','basis',v_basis);
  return v_result||jsonb_build_object('basis_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_SELECTED_SOURCE_WITNESS_V1',v_result),'hex'));
end;
$function$;
alter function private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_selected_source_witness_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
commit;
