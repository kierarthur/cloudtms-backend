-- Final authority for later expense-only pending-update totals.
-- The original advanced-expense policy remains historical; this closure only
-- replaces the exact submit validator and reasserts its service-only ACL.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_expense_update_submit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_update_id uuid,
  p_payload jsonb,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_context jsonb;
  v_office_context jsonb;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_response jsonb;
  v_new_submission jsonb;
  v_new_claim jsonb;
  v_prior_claim jsonb;
  v_category text;
  v_kind text;
  v_new_amount numeric;
  v_prior_amount numeric;
  v_new_charge numeric;
  v_prior_charge numeric;
  v_new_units numeric;
  v_prior_units numeric;
  v_submit_request_sha bytea;
  v_submit_context jsonb;
  v_change jsonb;
begin
  if p_workflow_id is null or p_expected_generation is null or p_update_id is null
     or jsonb_typeof(coalesce(p_payload,'{}'::jsonb))<>'object'
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  if p_session_id is null then
    begin
      v_office_context:=nullif(current_setting(
        'cloudtms.office_candidate_context',true
      ),'')::jsonb;
    exception when others then
      v_office_context:=null;
    end;
    if coalesce(v_office_context->>'environment','')<>v_environment
       or coalesce(v_office_context->>'permission','')<>'reject_submission'
       or coalesce(v_office_context->>'action','')<>'REJECT_EXPENSE_CATEGORY' then
      raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
    end if;
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
    for update;
  else
    v_context:=private._candidate_session_context_v1(
      p_session_id,v_environment,null,p_now_utc,true
    );
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=p_workflow_id and workflow.environment=v_environment
      and workflow.account_id=(v_context->>'account_id')::uuid
      and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
    for update;
  end if;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id and update_row.workflow_id=v_workflow.id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null and (
    v_update.actor_kind<>'OFFICE'
    or v_update.actor_id is distinct from (v_office_context->>'actor_user_id')::uuid
  ) then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
  end if;
  v_submit_request_sha:=private._candidate_sha256_jsonb_v1(jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_SUBMIT_REQUEST_V1',
    'workflow_id',v_workflow.id,'expected_generation',p_expected_generation,
    'update_id',v_update.update_id,'payload',coalesce(p_payload,'{}'::jsonb)
  ));
  if v_update.submit_idempotency_key is not null then
    if v_update.submit_idempotency_key<>btrim(p_idempotency_key)
       or v_update.submit_request_sha256<>v_submit_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_update.submit_result_json is not null
       and v_update.state in ('RENDERING','COMMITTED') then
      -- SUBMIT also owns an immutable receipt.  Rebind/commit state is read
      -- through the canonical detail projection, never by rewriting the
      -- result of a lost-response retry.
      return v_update.submit_result_json
        ||jsonb_build_object('idempotent_replay',true);
    end if;
  end if;
  if v_update.state<>'EDITING' or v_workflow.state<>'WORKER_DRAFT'
     or v_workflow.generation<>p_expected_generation
     or v_update.current_workflow_generation<>p_expected_generation then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  if v_update.update_mode='PENDING_MANAGER' then
    select request.* into v_approval
    from public.candidate_approval_requests request
    where request.id=v_update.approval_request_id
      and request.workflow_id=v_workflow.id
      and request.workflow_generation=v_update.from_workflow_generation
    for update;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  elsif v_update.update_mode='PAPER_REPLACEMENT' then
    if v_update.approval_request_id is not null or v_workflow.route<>'PAPER' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  else
    raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='22023';
  end if;
  v_new_submission:=coalesce(p_payload->'immutable_submission','{}'::jsonb);
  if jsonb_typeof(v_new_submission)<>'object' then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='22023';
  end if;
  -- REMOVE and Office-reject plans are already locked to an exact component
  -- at BEGIN. Reapply that server-owned removal here so a retry can safely
  -- recover an update opened by an older definition which left a root-level
  -- canonical snapshot unchanged.
  for v_change in
    select value from jsonb_array_elements(v_update.update_plan_json) item(value)
    where value->>'update_kind' in ('REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY')
  loop
    v_new_submission:=private._candidate_expense_submission_without_category_v1(
      v_new_submission,v_change->>'expense_category'
    );
  end loop;
  -- A later expense-only workflow stores its canonical figures at the root.
  -- Read that snapshot before falling back to the surrounding authority object.
  v_new_claim:=coalesce(
    v_new_submission#>'{expense_submission,canonical_tsfin_snapshot}',
    v_new_submission->'expense_submission',
    v_new_submission#>'{expense_claim,canonical_tsfin_snapshot}',
    v_new_submission->'expense_claim',
    v_new_submission->'canonical_tsfin_snapshot',
    v_new_submission,'{}'::jsonb
  );
  v_prior_claim:=coalesce(
    v_update.prior_immutable_submission_json#>'{expense_submission,canonical_tsfin_snapshot}',
    v_update.prior_immutable_submission_json->'expense_submission',
    v_update.prior_immutable_submission_json#>'{expense_claim,canonical_tsfin_snapshot}',
    v_update.prior_immutable_submission_json->'expense_claim',
    v_update.prior_immutable_submission_json->'canonical_tsfin_snapshot',
    v_update.prior_immutable_submission_json,'{}'::jsonb
  );
  foreach v_category in array array['MILEAGE','TRAVEL','ACCOMMODATION','OTHER'] loop
    select change->>'update_kind' into v_kind
    from jsonb_array_elements(v_update.update_plan_json) change
    where change->>'expense_category'=v_category limit 1;
    v_new_amount:=private._candidate_expense_number_v1(v_new_claim,case v_category
      when 'MILEAGE' then 'mileage_pay_ex_vat' when 'TRAVEL' then 'travel_pay_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_pay_ex_vat' else 'other_pay_ex_vat' end);
    v_prior_amount:=private._candidate_expense_number_v1(v_prior_claim,case v_category
      when 'MILEAGE' then 'mileage_pay_ex_vat' when 'TRAVEL' then 'travel_pay_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_pay_ex_vat' else 'other_pay_ex_vat' end);
    v_new_charge:=private._candidate_expense_number_v1(v_new_claim,case v_category
      when 'MILEAGE' then 'mileage_charge_ex_vat' when 'TRAVEL' then 'travel_charge_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_charge_ex_vat' else 'other_charge_ex_vat' end);
    v_prior_charge:=private._candidate_expense_number_v1(v_prior_claim,case v_category
      when 'MILEAGE' then 'mileage_charge_ex_vat' when 'TRAVEL' then 'travel_charge_ex_vat'
      when 'ACCOMMODATION' then 'accommodation_charge_ex_vat' else 'other_charge_ex_vat' end);
    v_new_units:=case when v_category='MILEAGE'
      then private._candidate_expense_number_v1(v_new_claim,'mileage_units') else 0 end;
    v_prior_units:=case when v_category='MILEAGE'
      then private._candidate_expense_number_v1(v_prior_claim,'mileage_units') else 0 end;
    if v_kind in ('ADD_CATEGORY','REPLACE_CATEGORY') then
      if v_new_amount<=0 and v_new_charge<=0 and v_new_units<=0 then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_EMPTY' using errcode='55000';
      end if;
    elsif v_kind in ('REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY') then
      if v_new_amount<>0 or v_new_charge<>0 or v_new_units<>0 then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
      end if;
    elsif v_new_amount is distinct from v_prior_amount
       or v_new_charge is distinct from v_prior_charge
       or v_new_units is distinct from v_prior_units then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
    end if;
  end loop;
  -- The update plan is the complete amendment boundary. New evidence may
  -- exist only for an ADD/REPLACE category, and every untouched immutable
  -- source component must remain byte-for-byte present. This closes both a
  -- crash/resume mismatch and an attempt to alter an unplanned category by
  -- superseding or uploading a component after BEGIN.
  if exists(
    select 1
    from public.candidate_submission_components current_component
    where current_component.workflow_id=v_workflow.id
      and current_component.workflow_generation=v_workflow.generation
      and current_component.state='IMMUTABLE'
      and current_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and current_component.source_component_id is null
      and not exists(
        select 1 from jsonb_array_elements(v_update.update_plan_json) change
        where change->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
          and change->>'expense_category'=current_component.expense_category
      )
  ) or exists(
    select 1 from jsonb_array_elements(v_update.update_plan_json) change
    where change->>'update_kind' in ('ADD_CATEGORY','REPLACE_CATEGORY')
      and not exists(
        select 1 from public.candidate_submission_components current_component
        where current_component.workflow_id=v_workflow.id
          and current_component.workflow_generation=v_workflow.generation
          and current_component.state='IMMUTABLE'
          and current_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
          and current_component.expense_category=change->>'expense_category'
          and current_component.source_content_sha256 is not null
      )
  ) or exists(
    select 1
    from public.candidate_submission_components prior_component
    where prior_component.workflow_id=v_workflow.id
      and prior_component.workflow_generation=v_update.from_workflow_generation
      and prior_component.state='IMMUTABLE'
      and prior_component.component_kind in (
        'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
      )
      and not exists(
        select 1 from jsonb_array_elements(v_update.update_plan_json) change
        where change->>'expense_category'=prior_component.expense_category
      )
      and not exists(
        select 1
        from public.candidate_submission_components current_component
        where current_component.workflow_id=prior_component.workflow_id
          and current_component.workflow_generation=v_workflow.generation
          and current_component.state='IMMUTABLE'
          and current_component.source_component_id=coalesce(
            prior_component.source_component_id,prior_component.id
          )
          and current_component.component_kind=prior_component.component_kind
          and current_component.expense_category is not distinct from
            prior_component.expense_category
          and current_component.source_content_sha256=prior_component.source_content_sha256
          and current_component.byte_size=prior_component.byte_size
          and current_component.media_type=prior_component.media_type
      )
  ) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_PLAN_MISMATCH' using errcode='40001';
  end if;
  if v_workflow.candidate_signature_component_id is not null and exists(
    select 1 from public.candidate_submission_components prior_hours
    where prior_hours.workflow_id=v_workflow.id
      and prior_hours.workflow_generation=v_update.from_workflow_generation
      and prior_hours.component_kind='HOURS_TIMESHEET'
      and prior_hours.state='IMMUTABLE'
  ) and not exists(
    select 1
    from public.candidate_submission_components current_hours
    join public.candidate_submission_components prior_hours
      on prior_hours.workflow_id=current_hours.workflow_id
      and prior_hours.workflow_generation=v_update.from_workflow_generation
      and prior_hours.component_kind='HOURS_TIMESHEET'
      and prior_hours.state='IMMUTABLE'
      and prior_hours.source_content_sha256=current_hours.source_content_sha256
    where current_hours.workflow_id=v_workflow.id
      and current_hours.workflow_generation=v_workflow.generation
      and current_hours.component_kind='HOURS_TIMESHEET'
      and current_hours.state='IMMUTABLE'
  ) then
    raise exception 'CANDIDATE_SIGNATURE_SCOPE_CHANGED' using errcode='40001';
  end if;
  v_submit_context:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_UPDATE_SUBMIT_CONTEXT_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'update_id',v_update.update_id,
    'update_mode',v_update.update_mode,
    'actor_kind',v_update.actor_kind,
    'actor_id',v_update.actor_id,
    'idempotency_key',btrim(p_idempotency_key)
  );
  perform set_config(
    'cloudtms.candidate_expense_update_submit_context',v_submit_context::text,true
  );
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,v_environment,v_workflow.id,'WORKER_SUBMIT',
    p_expected_generation,
    pg_catalog.jsonb_set(
      coalesce(p_payload,'{}'::jsonb),'{immutable_submission}',v_new_submission,true
    )||jsonb_build_object(
      'update_id',v_update.update_id,
      'candidate_signature_component_id',v_workflow.candidate_signature_component_id,
      'service_office_action',p_session_id is null,
      'actor_user_id',case when p_session_id is null then v_update.actor_id else null end
    ),
    p_idempotency_key,p_now_utc
  );
  perform set_config('cloudtms.candidate_expense_update_submit_context','{}',true);
  if v_update.update_mode='PENDING_MANAGER' then
    if coalesce(v_response->>'state','')<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'
       or jsonb_typeof(v_response->'render_contract') is distinct from 'object' then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RENDER_CONTRACT_MISSING'
        using errcode='55000';
    end if;
  elsif coalesce(v_response->>'state','')<>'WORKER_SUBMITTED' then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY'
      using errcode='55000';
  end if;
  v_response:=v_response||jsonb_build_object(
    'update_id',v_update.update_id,'update_state','UPDATING',
    'approval_request_id',v_approval.id,
    'approval_request_generation',v_approval.request_generation,
    'manager_link_preserved',v_update.update_mode='PENDING_MANAGER',
    'paper_pack_replacement',v_update.update_mode='PAPER_REPLACEMENT',
    'paper_prepare_required',v_update.update_mode='PAPER_REPLACEMENT',
    'old_pack_recoverable',v_update.update_mode='PAPER_REPLACEMENT'
  );
  update public.candidate_pending_expense_updates set
    state='RENDERING',current_workflow_generation=(v_response->>'generation')::integer,
    submit_idempotency_key=btrim(p_idempotency_key),
    submit_request_sha256=v_submit_request_sha,
    submit_result_json=v_response,
    expires_at_utc=p_now_utc+case when v_update.update_mode='PAPER_REPLACEMENT'
      then interval '15 minutes' else interval '5 minutes' end,
    updated_at_utc=p_now_utc
  where update_id=v_update.update_id and state='EDITING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  return v_response;
end;
$function$;

alter function public.candidate_expense_update_submit_atomic_v1(
  uuid,text,uuid,integer,uuid,jsonb,text,timestamptz
) owner to postgres;
revoke all on function public.candidate_expense_update_submit_atomic_v1(
  uuid,text,uuid,integer,uuid,jsonb,text,timestamptz
) from public,anon,authenticated,service_role;
grant execute on function public.candidate_expense_update_submit_atomic_v1(
  uuid,text,uuid,integer,uuid,jsonb,text,timestamptz
) to service_role;

notify pgrst, 'reload schema';

commit;
