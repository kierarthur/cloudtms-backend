-- Candidate approved expense-carrier projection v1.
-- Retains the complete paper evidence label authority, and ensures a separate
-- electronic expense carrier receives its already-proved manager approval
-- identity without pretending that it has a Candidate signature.

\set ON_ERROR_STOP on

-- A standalone expense claim starts against the current worked Timesheet but
-- does not become a Timesheet itself until finalisation.  If Office rotates the
-- worked row to a new Timesheet ID, carry only the still-live expense anchor to
-- that one current version.  Terminal history remains bound to the version it
-- actually used, and contradictory identities are never guessed.
create or replace function private._timesheet_expense_anchor_follow_current_v1()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
begin
  if new.is_current is distinct from true
     or new.archived_at_utc is not null
     or nullif(btrim(coalesce(new.booking_id,'')),'') is null
     or new.contract_id is null
     or new.week_ending_date is null then
    return new;
  end if;

  update public.candidate_submission_workflows workflow
  set anchor_timesheet_id=new.timesheet_id
  from public.timesheets prior_anchor
  where workflow.workflow_kind='CONTRACT_EXPENSE'
    and workflow.state not in ('FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
    and workflow.anchor_timesheet_id=prior_anchor.timesheet_id
    and workflow.anchor_timesheet_id is distinct from new.timesheet_id
    and prior_anchor.booking_id=new.booking_id
    and workflow.contract_id=new.contract_id
    and workflow.week_ending_date=new.week_ending_date;

  return new;
end;
$function$;

revoke all on function private._timesheet_expense_anchor_follow_current_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists timesheets_expense_anchor_follow_current_trg
  on public.timesheets;
create trigger timesheets_expense_anchor_follow_current_trg
after insert or update of is_current,archived_at_utc on public.timesheets
for each row
when (new.is_current=true and new.archived_at_utc is null)
execute function private._timesheet_expense_anchor_follow_current_v1();

create or replace function public.timesheet_expense_apply_atomic_v1(
  p_candidate_id uuid,
  p_environment text,
  p_target_timesheet_id uuid,
  p_workflow_id uuid,
  p_expected_workflow_generation integer,
  p_expected_row_signature text,
  p_claim_json jsonb,
  p_evidence_component_ids uuid[],
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_target_timesheet_id uuid:=p_target_timesheet_id;
  v_claim jsonb:=coalesce(p_claim_json,'{}'::jsonb);
  v_snapshot jsonb;
  v_result jsonb;
  v_capabilities jsonb;
  v_response jsonb;
  v_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_is_separate_carrier boolean:=false;
  v_is_paper boolean:=false;
  v_component_count integer:=0;
  v_required_categories text[]:='{}'::text[];
  v_category text;
  v_kind text;
  v_document_role text;
  v_paper_page jsonb;
  v_materialised_storage_key text;
  v_expense_line_type text;
  v_anchor_current_count integer:=0;
  v_anchor_current_timesheet_id uuid;
  v_system_actor uuid;
  v_constraint_name text;
  v_apply_expected_row_signature text:=p_expected_row_signature;
  v_signature_payload jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_workflow_id is null or p_expected_workflow_generation is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null or jsonb_typeof(v_claim)<>'object' then
    raise exception 'CANDIDATE_EXPENSE_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment or v_workflow.candidate_id<>p_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.last_mutation_idempotency_key=p_idempotency_key and v_workflow.last_mutation_response_json is not null then
    return v_workflow.last_mutation_response_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_workflow.generation<>p_expected_workflow_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  v_is_paper:=v_workflow.route='PAPER';
  if ((not v_is_paper and (
          v_workflow.state<>'READY_TO_FINALISE'
          or v_workflow.manager_approved_at_utc is null
        ))
      or (v_is_paper and v_workflow.state<>'RECEIVED')
      or current_setting('cloudtms.candidate_finalize_workflow',true)
        is distinct from v_workflow.id::text||':'||v_workflow.generation::text) then
    raise exception 'CANDIDATE_EXPENSE_APPLY_FINALISE_ONLY' using errcode='55000';
  end if;
  if v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
    raise exception 'CANDIDATE_EXPENSE_WORKFLOW_REQUIRED' using errcode='22023';
  end if;
  v_is_separate_carrier:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
    or v_workflow.target_timesheet_id is distinct from v_workflow.anchor_timesheet_id;
  if not v_is_paper then
    select * into v_manager_signature from public.candidate_submission_components
    where id=v_workflow.manager_signature_component_id and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state='IMMUTABLE';
    if v_manager_signature.id is null
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256 then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    if v_workflow.workflow_kind='CONTRACT_COMBINED' and not v_is_separate_carrier then
      select * into v_candidate_signature from public.candidate_submission_components
      where id=v_workflow.candidate_signature_component_id and workflow_id=v_workflow.id
        and workflow_generation=v_workflow.generation and state='IMMUTABLE';
      if v_candidate_signature.id is null
         or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256 then
        raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
      end if;
    end if;
    v_electronic_patch:=jsonb_build_object(
      -- A standalone expense workflow deliberately has no Candidate
      -- signature. Keep its carrier MANUAL so the invariant that every
      -- ELECTRONIC Timesheet has the full Candidate/manager pair remains
      -- intact, while retaining the manager approval projection below.
      'submission_mode',case when v_is_separate_carrier then 'MANUAL' else 'ELECTRONIC' end,
      'auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,
      'r2_nurse_key',case when v_candidate_signature.id is null then null else v_candidate_signature.storage_key end,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',case when v_candidate_signature.source_content_sha256 is null then null
        else encode(v_candidate_signature.source_content_sha256,'hex') end,
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc);
  elsif v_is_paper then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;
  if v_workflow.immutable_submission_json is null
     or v_workflow.immutable_submission_sha256 is null
     or (
       (v_workflow.workflow_kind='CONTRACT_EXPENSE'
         and (private._candidate_sha256_jsonb_v1(v_claim) is distinct from v_workflow.immutable_submission_sha256
           or v_claim is distinct from v_workflow.immutable_submission_json))
       or (v_workflow.workflow_kind='CONTRACT_COMBINED'
         and v_claim is distinct from v_workflow.immutable_submission_json
         and v_claim is distinct from v_workflow.immutable_submission_json->'expense_submission')
     ) then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks
  where id=v_workflow.contract_week_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.anchor_timesheet_id;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_FOUND' using errcode='P0002';
    end if;

    if v_anchor_timesheet.is_current is distinct from true
       or v_anchor_timesheet.archived_at_utc is not null then
      if v_workflow.workflow_kind<>'CONTRACT_EXPENSE'
         or nullif(btrim(coalesce(v_anchor_timesheet.booking_id,'')),'') is null then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_CURRENT' using errcode='40001';
      end if;

      select count(*)::integer,
        case when count(*)=1 then min(current_anchor.timesheet_id::text)::uuid end
      into v_anchor_current_count,v_anchor_current_timesheet_id
      from public.timesheets current_anchor
      where current_anchor.booking_id=v_anchor_timesheet.booking_id
        and current_anchor.is_current=true
        and current_anchor.archived_at_utc is null
        and current_anchor.contract_id=v_workflow.contract_id
        and current_anchor.week_ending_date=v_workflow.week_ending_date;

      if v_anchor_current_count=0 then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_CURRENT_VERSION_NOT_FOUND'
          using errcode='40001';
      elsif v_anchor_current_count<>1 then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_CURRENT_VERSION_AMBIGUOUS'
          using errcode='40001';
      end if;

      select * into v_anchor_timesheet
      from public.timesheets
      where timesheet_id=v_anchor_current_timesheet_id
        and is_current=true
        and archived_at_utc is null
      for share;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_CURRENT_VERSION_CHANGED'
          using errcode='40001';
      end if;

      update public.candidate_submission_workflows
      set anchor_timesheet_id=v_anchor_timesheet.timesheet_id
      where id=v_workflow.id
        and generation=v_workflow.generation
        and anchor_timesheet_id=v_workflow.anchor_timesheet_id;
      if not found then
        raise exception 'WORKFLOW_VERSION_MISMATCH' using errcode='40001';
      end if;
      v_workflow.anchor_timesheet_id:=v_anchor_timesheet.timesheet_id;
    end if;
  end if;
  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000';
  end if;
  if v_target_timesheet_id is not null and v_week.timesheet_id is distinct from v_target_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;

  v_snapshot:=v_claim->'canonical_tsfin_snapshot';
  if v_snapshot is null or jsonb_typeof(v_snapshot)<>'object' then
    raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
  end if;
  if nullif(v_snapshot->>'candidate_id','')::uuid is distinct from p_candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_contract.client_id then
    raise exception 'TSFIN_SNAPSHOT_MISMATCH' using errcode='22023';
  end if;
  if coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)<0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)<0 then
    raise exception 'CANDIDATE_EXPENSE_VALUE_INVALID' using errcode='22023';
  end if;

  if coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'TRAVEL');
  end if;
  if coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'ACCOMMODATION');
  end if;
  if coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)>0
     or (
       (
         coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0)>0
         or coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0)>0
       )
       and (
         coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0)
         +coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0)
         +coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0)
         +coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0)
         +coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0)
         +coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0)
       )=0
     ) then
    v_required_categories:=array_append(v_required_categories,'OTHER');
  end if;
  if coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0)>0
     or coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0)>0 then
    v_required_categories:=array_append(v_required_categories,'MILEAGE');
  end if;

  -- An empty reusable carrier may still have the generic EXPENSES line type.
  -- Finalisation must project the submitted claim's exact final type so a
  -- mileage-only claim cannot be rejected by the final-state guard.
  v_expense_line_type:=case
    when (
      abs(coalesce(nullif(v_snapshot->>'mileage_units','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'mileage_charge_ex_vat','')::numeric,0))
    )<>0
    and (
      abs(coalesce(nullif(v_snapshot->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'travel_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'travel_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'accommodation_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'accommodation_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'other_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(v_snapshot->>'other_charge_ex_vat','')::numeric,0))
    )=0 then 'MILEAGE'
    else 'EXPENSES'
  end;

  select count(*)::integer into v_component_count
  from public.candidate_submission_components component
  where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
    and component.workflow_id=v_workflow.id
    and component.workflow_generation=v_workflow.generation
    and component.state='IMMUTABLE'
    and component.immutable_at_utc is not null
    and (
      (not v_is_paper
        and component.required=true
        and component.component_kind<>'HOURS_TIMESHEET'
        and component.review_render_state='READY'
        and component.final_signed_render_state='READY'
        and component.final_signed_content_sha256 is not null)
      or (v_is_paper
        and component.component_kind='SIGNED_RETURN'
        and component.source_content_sha256 is not null
        and component.paper_return_page_key<>'HOURS_TIMESHEET')
    );
  if v_component_count<>coalesce(cardinality(p_evidence_component_ids),0) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_INVALID' using errcode='22023';
  end if;
  if not v_is_paper and exists(
    select 1 from public.candidate_submission_components component
    where component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.required=true and component.state<>'SUPERSEDED'
      and component.component_kind<>'HOURS_TIMESHEET'
      and not (component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])))
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  if v_is_paper and exists(
    select 1
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
    where expected_page->>'page_key'<>'HOURS_TIMESHEET'
      and not exists(
        select 1
        from public.candidate_submission_components component
        where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
          and component.workflow_id=v_workflow.id
          and component.workflow_generation=v_workflow.generation
          and component.component_kind='SIGNED_RETURN'
          and component.paper_return_page_key=expected_page->>'page_key'
          and component.state='IMMUTABLE'
      )
  ) then
    raise exception 'CANDIDATE_EVIDENCE_COMPONENT_SET_INCOMPLETE' using errcode='22023';
  end if;
  foreach v_category in array v_required_categories loop
    if not exists(
      select 1
      from public.candidate_submission_components component
      where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
        and component.workflow_id=v_workflow.id and component.workflow_generation=v_workflow.generation
        and component.state='IMMUTABLE'
        and (
          (not v_is_paper
            and component.expense_category=v_category
            and component.final_signed_render_state='READY'
            and component.document_role in ('SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM'))
          or (v_is_paper and exists(
            select 1
            from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
            where expected_page->>'page_key'=component.paper_return_page_key
              and expected_page->>'expense_category'=v_category
          ))
        )
    ) then
      raise exception 'EXPENSE_EVIDENCE_REQUIRED'
        using errcode='22023',detail=jsonb_build_object('code','EXPENSE_EVIDENCE_REQUIRED','category',v_category)::text;
    end if;
  end loop;
  if 'MILEAGE'=any(v_required_categories) and not exists(
    select 1 from public.candidate_submission_components component
    where component.id=any(coalesce(p_evidence_component_ids,'{}'::uuid[]))
      and component.workflow_id=v_workflow.id
      and component.workflow_generation=v_workflow.generation
      and component.state='IMMUTABLE'
      and (
        (not v_is_paper
          and component.component_kind='MILEAGE_FORM'
          and component.expense_category='MILEAGE'
          and component.final_signed_render_state='READY')
        or (v_is_paper and exists(
          select 1
          from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
          where expected_page->>'page_key'=component.paper_return_page_key
            and expected_page->>'component_kind'='MILEAGE_FORM'
            and expected_page->>'expense_category'='MILEAGE'
        ))
      )
  ) then
    raise exception 'EXPENSE_EVIDENCE_REQUIRED'
      using errcode='22023',detail=jsonb_build_object(
        'code','EXPENSE_EVIDENCE_REQUIRED','category','MILEAGE','document_role','MILEAGE_CLAIM_FORM')::text;
  end if;

  -- A CONTRACT_COMBINED finalisation has already completed its guarded hours
  -- save in this same transaction. That save can advance the lifecycle
  -- signature after producing its response receipt. The contract week is
  -- locked above, so re-read the canonical signature here and retain the
  -- mandatory comparison in the canonical weekly upsert. Standalone
  -- expense workflows continue to use the caller's original signature.
  if v_workflow.workflow_kind='CONTRACT_COMBINED' then
    v_signature_payload:=public.timesheet_lifecycle_guard_signature_v1(
      v_target_timesheet_id,v_week.id,false
    );
    v_apply_expected_row_signature:=nullif(btrim(coalesce(
      v_signature_payload->>'backend_row_signature',
      v_signature_payload->>'row_signature',
      v_signature_payload->>'signature',''
    )), '');
    if v_apply_expected_row_signature is null then
      raise exception 'CANDIDATE_COMBINED_POST_HOURS_SIGNATURE_REQUIRED'
        using errcode='55000';
    end if;
  end if;

  -- The canonical upsert accepts manager/workflow fields only while bound to
  -- this exact trusted finalisation. Expense-only workflows have no hours
  -- branch to establish that context, so establish it here for both new and
  -- reusable carriers.
  perform set_config(
    'cloudtms.candidate_electronic_finalise',
    v_workflow.id::text||':'||v_workflow.generation::text,
    true
  );
  v_result:=public.contract_week_manual_upsert_atomic(
    p_week_id=>v_week.id,
    p_expected_timesheet_id=>v_target_timesheet_id,
    p_timesheet_create_json=>case when v_target_timesheet_id is null then
      coalesce(v_claim->'timesheet_create_json','{}'::jsonb)
        ||case when v_is_separate_carrier then jsonb_build_object(
          'booking_id','CANDIDATE-EXPENSE-'||replace(v_week.id::text,'-',''),
          'contract_id',v_week.contract_id,
          'week_ending_date',v_week.week_ending_date,
          'status','RECEIVED',
          'submission_mode','MANUAL',
          'sheet_scope','WEEKLY',
          'line_type',v_expense_line_type,
          'is_adjustment',true,
          'occupant_key_norm',v_anchor_timesheet.occupant_key_norm,
          'hospital_norm',v_anchor_timesheet.hospital_norm,
          'ward_norm',v_anchor_timesheet.ward_norm,
          'job_title_norm',v_anchor_timesheet.job_title_norm,
          'shift_label_norm','weekly-expenses',
          'band',v_anchor_timesheet.band,
          'candidate_hint_text',coalesce(v_anchor_timesheet.candidate_hint_text,'{}'::jsonb),
          'manual_pdf_r2_key',null
        )||v_electronic_patch else v_electronic_patch end
      else null end,
    p_timesheet_patch_json=>coalesce(v_claim->'timesheet_patch_json','{}'::jsonb)
      ||case when v_is_separate_carrier then jsonb_build_object(
          'line_type',v_expense_line_type
        )||v_electronic_patch else v_electronic_patch end,
    p_contract_week_patch_json=>coalesce(v_claim->'contract_week_patch_json','{}'::jsonb),
    p_tsfin_snapshot_json=>v_snapshot,
    p_rotation_json=>null,
    p_actor_user_id=>v_system_actor,
    p_materialise_staged_evidence=>false,
    p_now_utc=>p_now_utc,
    p_expected_row_signature=>v_apply_expected_row_signature,
    p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
  );
  v_target_timesheet_id:=coalesce(nullif(v_result->>'timesheet_id','')::uuid,nullif(v_result#>>'{timesheet,timesheet_id}','')::uuid);
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_EXPENSE_TARGET_NOT_CREATED' using errcode='55000'; end if;

  for v_component in
    select * from public.candidate_submission_components
    where id=any(coalesce(p_evidence_component_ids,'{}'::uuid[])) order by component_no,id for update
  loop
    update public.candidate_submission_components set timesheet_id=v_target_timesheet_id
    where id=v_component.id;
    if v_is_paper then
      select expected_page into v_paper_page
      from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
      where expected_page->>'page_key'=v_component.paper_return_page_key;
      v_category:=nullif(v_paper_page->>'expense_category','');
      v_document_role:=case v_paper_page->>'component_kind'
        when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
        when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
        else 'SOURCE_EVIDENCE' end;
      v_materialised_storage_key:=v_component.storage_key;
    else
      v_category:=v_component.expense_category;
      v_document_role:=v_component.document_role;
      v_materialised_storage_key:=v_component.final_signed_storage_key;
    end if;
    v_kind:=case
      when v_document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY' then 'OTHER'
      when v_document_role='MILEAGE_CLAIM_FORM' then 'MILEAGE'
      when v_document_role='SIGNED_TIMESHEET' then 'TIMESHEET'
      when v_category='MILEAGE' then 'MILEAGE'
      else v_category end;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,document_role,candidate_component_id,processing_state
    ) values (
      v_target_timesheet_id,v_kind,coalesce(
        case when v_is_paper then nullif(v_paper_page->>'display_name','') end,
        nullif(v_claim->>'evidence_display_name',''),
        'Candidate submission evidence'
      ),
       v_materialised_storage_key,p_now_utc,v_document_role,v_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end loop;

  v_capabilities:=private._candidate_record_capabilities_v1(v_target_timesheet_id,v_week.id,'{}'::jsonb);
  if v_capabilities->>'record_role'='CONFLICT' then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS'
      using errcode='55000',detail=v_capabilities::text;
  end if;
  v_response:=jsonb_build_object(
    'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'target_timesheet_id',v_target_timesheet_id,'target_contract_week_id',v_week.id,
    'canonical_result',v_result,'capabilities',v_capabilities,'idempotent_replay',false
  );
  update public.candidate_submission_workflows set
    target_timesheet_id=v_target_timesheet_id,contract_week_id=v_week.id,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,'CANDIDATE_EXPENSE_APPLIED',null,
    jsonb_build_object('timesheet_id',v_target_timesheet_id,'contract_week_id',v_week.id,'component_count',v_component_count),
    null,v_system_actor,p_idempotency_key,p_now_utc);
  return v_response;
exception when unique_violation then
  get stacked diagnostics v_constraint_name=constraint_name;
  if v_constraint_name='candidate_submission_components_source_sha256_uq' then
    raise exception 'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' using errcode='23505';
  elsif v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
    raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
  end if;
  raise;
end;
$function$;

revoke all on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) from public,anon,authenticated;
revoke all on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) from public,anon,authenticated;
revoke all on function public.timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamptz) from public,anon,authenticated;
grant execute on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) to service_role;
grant execute on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) to service_role;
grant execute on function public.timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamptz) to service_role;
