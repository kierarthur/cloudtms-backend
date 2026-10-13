-- DY06 scoped native Daily rotation; complete2035 context and current0922 public owners.
-- BD104: ruleB original choice, twelve native financial roots, exact Work NOWAIT.
-- Original Source admission/core/legacy/ACL/history remain authoritative.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_daily_pay_retention_v1(
  p_booking_id text,
  p_history jsonb default null
)
returns boolean
language plpgsql stable security invoker
set search_path=pg_catalog,public,private
as $function$
declare
  v_timesheet_ids uuid[];
  v_contract_week_ids uuid[];
  v_work_id uuid;
  v_history jsonb:=p_history;
begin
  -- Original physical family only; same maximum as the native removal owner.
  select array(select t.timesheet_id from public.timesheets t
    where t.booking_id=p_booking_id limit 33)
    into v_timesheet_ids;
  if cardinality(v_timesheet_ids)=0 then
    raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  if cardinality(v_timesheet_ids)>32 then
    raise exception using message='REMOVAL_UNIT_TOO_LARGE',
      detail=jsonb_build_object('maximum',32,'actual',cardinality(v_timesheet_ids))::text;
  end if;
  if v_history is null then
    select array(select cw.id from public.contract_weeks cw
      where cw.timesheet_id=any(v_timesheet_ids) limit 33)
      into v_contract_week_ids;
    if cardinality(v_contract_week_ids)>32 then
      raise exception using message='REMOVAL_UNIT_TOO_LARGE',
        detail=jsonb_build_object('maximum',32,'actual',cardinality(v_contract_week_ids))::text;
    end if;
    v_history:=public.timesheet_removal_financial_history_v1(
      v_timesheet_ids,array[p_booking_id],v_contract_week_ids);
  end if;
  -- Archive-only retention is deliberately not a financial fact under NEXT.
  if exists(select 1 from jsonb_array_elements(coalesce(v_history->'blockers','[]'::jsonb)) blocker
    where blocker->>'code' not in ('TIMESHEET_STILL_AUTHORISED')) then
    return true;
  end if;
  select w.id into v_work_id from private.bpay_next_work w where w.booking_id=p_booking_id;
  if v_work_id is not null and (
    -- Choice has original selection/revision FKs. OPEN/PINNING is already capture;
    -- merely skipped run_choice_member records are not choices and are not read.
    exists(select 1 from private.bpay_next_work_choice c where c.work_id=v_work_id)
    or exists(select 1 from private.bpay_next_run_work rw where rw.work_id=v_work_id)
    or exists(select 1 from private.bpay_next_financial_effect e where e.work_id=v_work_id)
  ) then return true; end if;
  -- Exact twelve native retention roots, key-bounded and status-independent.
  return exists(select 1 from public.timesheets_financials f
      where f.timesheet_id=any(v_timesheet_ids) and (
        f.paid_at_utc is not null or f.paid_by_user_id is not null
        or nullif(btrim(coalesce(f.payment_reference,'')),'') is not null
        or f.remittance_last_sent_at_utc is not null or coalesce(f.remittance_send_count,0)>0))
    or exists(select 1 from public.pay_batch_items i where i.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.pay_batches b where b.force_include_timesheet_ids && v_timesheet_ids)
    or exists(select 1 from public.pay_batch_timesheet_snapshots s where s.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.timesheet_pay_state s where s.timesheet_id=any(v_timesheet_ids) and (
      s.last_settled_pay_batch_id is not null or s.last_settled_at_utc is not null
      or s.last_settled_signature is not null or s.last_settled_snapshot_json is not null
      or s.summary_pay_paid_at_utc is not null
      or upper(coalesce(s.summary_pay_status_code,'UNPAID')) in
        ('PAID','PARTIALLY_PAID','PART_PAID','OVERPAID','UNDERPAID','PROCESSING')))
    or exists(select 1 from public.timesheet_pay_state_history h where h.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.ts_pay_adjustments a where a.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.pay_finance_case_components c where c.linked_timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.pay_advances a where a.linked_timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.pay_payment_correction_items c where c.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.pay_manual_adjustment_carry_forwards c where c.timesheet_id=any(v_timesheet_ids))
    or exists(select 1 from public.timesheet_payment_overrides o where o.timesheet_id=any(v_timesheet_ids));
end;
$function$;

create or replace function private._timesheet_route_change_context_v1(
  p_current_timesheet_id uuid,
  p_target_action text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_contract public.contracts%rowtype;
  v_linked_workflow public.candidate_submission_workflows%rowtype;
  v_paper_workflow public.candidate_submission_workflows%rowtype;
  v_active_workflow public.candidate_submission_workflows%rowtype;
  v_request public.candidate_approval_requests%rowtype;
  v_signature jsonb:='{}'::jsonb;
  v_policy jsonb:='{}'::jsonb;
  v_import jsonb:='{}'::jsonb;
  v_restore_proof jsonb:='{}'::jsonb;
  v_history jsonb:='{}'::jsonb;
  v_paper_source_context jsonb:='{}'::jsonb;
  v_payload jsonb;
  v_context_hash text;
  v_route_family text;
  v_warning_code text;
  v_block_reason text;
  v_row_signature text;
  v_effective_mode text;
  v_booking_id text;
  v_contract_id uuid;
  v_client_id uuid;
  v_candidate_signed boolean:=false;
  v_manager_pending boolean:=false;
  v_manager_approved boolean:=false;
  v_final_signed_ready boolean:=false;
  v_qr_route_active boolean:=false;
  v_qr_code_generated boolean:=false;
  v_qr_pack_ready boolean:=false;
  v_qr_pack_issued_or_sent boolean:=false;
  v_qr_issued boolean:=false;
  v_qr_signed boolean:=false;
  v_prior_qr boolean:=false;
  v_import_authoritative boolean:=false;
  v_office_authorised boolean:=false;
  v_invoiced boolean:=false;
  v_paid boolean:=false;
  v_paper_allowed boolean:=false;
  v_permitted boolean:=false;
  v_reason_required boolean:=false;
  v_confirmation_required boolean:=false;
  v_active_workflow_count integer:=0;
  v_incomplete_expense_removal_required boolean:=false;
  v_resolvable_paper_context_conflict boolean:=false;
  v_contract_week_authorised boolean:=false;
  v_protected_history boolean:=false;
  v_scope text;
  v_basis text;
  v_actual_qr_backing boolean:=false;
begin
  if p_current_timesheet_id is null then
    raise exception 'ROUTE_CHANGE_TIMESHEET_REQUIRED' using errcode='22023';
  end if;
  if v_action not in (
    'CONVERT_QR_TO_MANUAL','ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN',
    'INVALIDATE_QR','REISSUE_QR','DISABLE_QR','SWITCH_TO_MANUAL',
    'SWITCH_DAILY_TO_MANUAL','REVERT_TO_ELECTRONIC'
  ) then
    raise exception 'ROUTE_CHANGE_ACTION_INVALID' using errcode='22023';
  end if;

  select * into v_requested from public.timesheets
  where timesheet_id=p_current_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  v_booking_id:=nullif(btrim(coalesce(v_requested.booking_id,'')),'');
  if v_booking_id is null then
    raise exception 'TIMESHEET_BOOKING_ID_REQUIRED' using errcode='55000';
  end if;
  select * into v_current from public.timesheets
  where booking_id=v_booking_id and is_current=true
  order by version desc,updated_at desc,created_at desc limit 1;
  if not found then raise exception 'TIMESHEET_CURRENT_VERSION_NOT_FOUND' using errcode='P0002'; end if;

  select * into v_week from public.contract_weeks
  where timesheet_id=v_current.timesheet_id
  order by updated_at desc,created_at desc,id desc limit 1;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_contract_id:=coalesce(v_week.contract_id,v_current.contract_id);
  if v_contract_id is not null then
    select * into v_contract from public.contracts where id=v_contract_id;
    v_client_id:=v_contract.client_id;
  end if;
  v_client_id:=coalesce(v_client_id,v_fin.client_id);

  if v_client_id is not null then
    v_import:=private._candidate_import_authoritative_v1(
      v_client_id,v_contract_id,v_current.timesheet_id,
      case when v_fin.id is null then null else to_jsonb(v_fin) end,
      v_current.week_ending_date
    );
    v_import_authoritative:=coalesce((v_import->>'is_import_authoritative')::boolean,false);
  end if;
  if v_client_id is not null and v_contract_id is not null then
    v_policy:=private._candidate_policy_resolve_v1(
      v_client_id,v_contract_id,
      coalesce(v_current.week_ending_date,(current_timestamp at time zone 'Europe/London')::date)
    );
    v_effective_mode:=private._candidate_submission_mode_v1(
      v_client_id,v_contract_id,
      coalesce(v_current.week_ending_date,(current_timestamp at time zone 'Europe/London')::date)
    )::text;
    v_paper_allowed:=coalesce((v_policy->>'paper_submission_enabled')::boolean,false);
  else
    v_effective_mode:=coalesce(v_current.submission_mode::text,'MANUAL');
  end if;

  select exists(
    select 1 from public.timesheets history
    where history.booking_id=v_booking_id and (
      history.qr_status is not null
      or nullif(btrim(coalesce(history.qr_token,'')),'') is not null
      or history.qr_generated_at is not null
      or history.qr_scanned_at is not null
      or nullif(btrim(coalesce(history.qr_r2_key,'')),'') is not null
      or history.qr_signed_at_utc is not null
    )
  ) into v_prior_qr;
  v_qr_route_active:=coalesce(v_current.qr_status is not null
    or nullif(btrim(coalesce(v_current.qr_token,'')),'') is not null
    or v_current.qr_generated_at is not null
    or nullif(btrim(coalesce(v_current.qr_r2_key,'')),'') is not null
    or v_current.qr_scanned_at is not null
    or v_current.qr_signed_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_signed_hash,'')),'') is not null
    or v_current.candidate_submission_route_intent='PAPER',false);
  -- The QR code is created before the printable PDF.  Keep code generation,
  -- durable pack readiness, actual issue/send and signed return as separate
  -- facts so the approved W08/W09 wording is never asserted prematurely.
  v_qr_code_generated:=v_qr_route_active
    and nullif(btrim(coalesce(v_current.qr_token,'')),'') is not null
    and v_current.qr_generated_at is not null;
  v_qr_pack_issued_or_sent:=v_qr_route_active and (
    v_current.qr_last_sent_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_last_sent_hash,'')),'') is not null
  );
  v_qr_pack_ready:=v_qr_route_active and (
      -- The unchanged QR backend writes this key only after successful PDF
      -- rendering.  It is proof of an unsigned generated pack, never proof of
      -- a returned signature.
      nullif(btrim(coalesce(v_current.manual_pdf_r2_key,'')),'') is not null
      or (
      upper(coalesce(v_current.document_state::text,''))='READY'
      and v_current.current_document_version_id is not null
      and exists(
        select 1 from public.invoice_document_versions document_version
        where document_version.id=v_current.current_document_version_id
          and document_version.entity_type='TIMESHEET'
          and document_version.entity_id=v_current.timesheet_id
          and upper(coalesce(document_version.status,''))='READY'
      )
    )
    or v_qr_pack_issued_or_sent
  );
  v_qr_signed:=v_qr_route_active and (
    v_current.qr_signed_at_utc is not null
    or nullif(btrim(coalesce(v_current.qr_signed_hash,'')),'') is not null
    or v_current.qr_scanned_at is not null
    or exists(
      select 1 from public.timesheet_evidence evidence
      where evidence.timesheet_id=v_current.timesheet_id
        and upper(btrim(evidence.kind))='TIMESHEET'
        and evidence.document_role='SIGNED_TIMESHEET'
        and evidence.processing_state<>'SUPERSEDED'
        and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
    )
  );
  v_scope:=upper(coalesce(v_current.sheet_scope::text,''));
  v_basis:=upper(coalesce(v_fin.basis::text,''));
  v_actual_qr_backing:=v_qr_route_active;
  v_route_family:=case
    when v_import_authoritative then 'IMPORT_AUTHORITATIVE'
    when v_actual_qr_backing then 'QR'
    when upper(coalesce(v_current.submission_mode::text,''))='ELECTRONIC'
      or upper(coalesce(v_week.submission_mode_snapshot::text,''))='ELECTRONIC'
      or v_current.candidate_submission_route_intent='ELECTRONIC'
      then 'ELECTRONIC'
    else 'MANUAL_NON_QR' end;

  -- Existing signed ELECTRONIC rows pre-date Candidate workflows.  Canonical
  -- CloudTMS signature/document evidence remains authoritative for warnings.
  v_candidate_signed:=nullif(btrim(coalesce(v_current.r2_nurse_key,'')),'') is not null
    or nullif(btrim(coalesce(v_current.img_sha256_nurse,'')),'') is not null;
  v_manager_approved:=nullif(btrim(coalesce(v_current.r2_auth_key,'')),'') is not null
    or nullif(btrim(coalesce(v_current.img_sha256_auth,'')),'') is not null
    or v_current.candidate_manager_approved_at_utc is not null;
  v_final_signed_ready:=v_manager_approved and exists(
    select 1 from public.timesheet_evidence evidence
    where evidence.timesheet_id=v_current.timesheet_id
      and upper(btrim(evidence.kind))='TIMESHEET'
      and evidence.processing_state<>'SUPERSEDED'
      and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
  );

  select workflow.* into v_linked_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=v_current.candidate_workflow_id
     or workflow.target_timesheet_id=v_current.timesheet_id
     or workflow.anchor_timesheet_id=v_current.timesheet_id
  order by (workflow.id=v_current.candidate_workflow_id) desc,
    workflow.updated_at_utc desc,workflow.created_at_utc desc
  limit 1;
  -- A QR/PAPER route is owned by the immutable delivery receipt matching the
  -- current token. The historical candidate_workflow_id remains useful audit
  -- lineage, but it must never outrank a later live expense/PAPER generation.
  -- With no current token, a sole nonterminal workflow is selected; finalised
  -- history is used only when no nonterminal workflow exists. Ambiguity is a
  -- server-owned preview conflict and is recomputed again under confirmation
  -- locks before any source-wide retirement or route rotation.
  if v_route_family='QR' then
    v_paper_source_context:=private._candidate_paper_source_workflow_context_v1(
      v_current.timesheet_id
    );
    if not coalesce(
      (v_paper_source_context->>'identity_conflict')::boolean,false
    ) and nullif(v_paper_source_context->>'selected_workflow_id','') is not null then
      select workflow.* into v_paper_workflow
      from public.candidate_submission_workflows workflow
      where workflow.id=(v_paper_source_context->>'selected_workflow_id')::uuid
        and workflow.generation=
          (v_paper_source_context->>'selected_workflow_generation')::integer
        and workflow.state=v_paper_source_context->>'selected_workflow_state';
      if not found then
        raise exception 'CANDIDATE_PAPER_SOURCE_CONTEXT_CHANGED' using errcode='40001';
      end if;
    end if;
  end if;
  select workflow.* into v_active_workflow
  from public.candidate_submission_workflows workflow
  where (workflow.id=v_current.candidate_workflow_id
      or workflow.target_timesheet_id=v_current.timesheet_id
      or workflow.anchor_timesheet_id=v_current.timesheet_id)
    -- REFUSED remains recoverable because candidate workflow AMEND expressly
    -- accepts it.  A route version must not make that recovery anchor historic.
    and workflow.state not in ('FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
  order by (workflow.id=v_current.candidate_workflow_id) desc,
    workflow.updated_at_utc desc,workflow.created_at_utc desc
  limit 1;
  select count(*) into v_active_workflow_count
  from public.candidate_submission_workflows workflow
  where (workflow.id=v_current.candidate_workflow_id
      or workflow.target_timesheet_id=v_current.timesheet_id
      or workflow.anchor_timesheet_id=v_current.timesheet_id)
    and workflow.state not in ('FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED');

  -- A source-to-MANUAL intervention may explicitly remove one standalone
  -- incomplete expense claim.  This includes REFUSED because AMEND still owns
  -- a valid recovery path until the office confirms that the claim is removed.
  v_incomplete_expense_removal_required:=v_action in (
      'SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL',
      'CONVERT_QR_TO_MANUAL','DISABLE_QR'
    )
    and v_active_workflow.id is not null
    and v_active_workflow.workflow_kind='CONTRACT_EXPENSE'
    and v_active_workflow.state not in (
      'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
    );
  v_resolvable_paper_context_conflict:=v_incomplete_expense_removal_required
    and v_route_family='QR'
    and v_active_workflow_count=1
    and coalesce((v_paper_source_context->>'identity_conflict')::boolean,false)
    and v_paper_source_context->>'conflict_reason' in (
      'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
      'CURRENT_QR_TOKEN_OWNER_TERMINAL_WITH_LIVE_WORKFLOW'
    )
    and coalesce(
      (v_paper_source_context->>'affected_nonterminal_workflow_count')::integer,0
    )=1
    and exists(
      select 1
      from jsonb_array_elements(
        coalesce(v_paper_source_context->'affected_nonterminal_workflows','[]'::jsonb)
      ) affected
      where affected->>'workflow_id'=v_active_workflow.id::text
    );

  if v_linked_workflow.id is not null then
    v_candidate_signed:=v_candidate_signed
      or v_linked_workflow.candidate_signature_component_id is not null
      or v_linked_workflow.candidate_signed_at_utc is not null;
    v_manager_approved:=v_manager_approved
      or v_linked_workflow.state in ('MANAGER_APPROVED','RECEIVED','FINALISED')
      or v_linked_workflow.manager_signature_component_id is not null
      or v_linked_workflow.manager_approved_at_utc is not null;
    v_final_signed_ready:=v_final_signed_ready or exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_linked_workflow.id
        and component.workflow_generation=v_linked_workflow.generation
        and component.required=true
        and component.state<>'SUPERSEDED'
        and component.final_signed_render_state='READY'
    );
    v_qr_signed:=v_qr_signed or (v_qr_route_active and exists(
      select 1 from public.candidate_submission_components component
      where component.workflow_id=v_linked_workflow.id
        and component.workflow_generation=v_linked_workflow.generation
        and component.component_kind='SIGNED_RETURN'
        and component.document_role='SIGNED_RETURN'
        and component.state='IMMUTABLE'
        and component.immutable_at_utc is not null
        and nullif(btrim(coalesce(component.storage_key,'')),'') is not null
    ));
  end if;
  -- A signed return necessarily followed issue even where a legacy send stamp
  -- is incomplete.  A merely generated or READY-but-unsent pack does not.
  v_qr_issued:=v_qr_pack_issued_or_sent or v_qr_signed;
  v_qr_pack_ready:=v_qr_pack_ready or v_qr_issued;
  if v_active_workflow.id is not null then
    select request_row.* into v_request
    from public.candidate_approval_requests request_row
    where request_row.workflow_id=v_active_workflow.id
      and request_row.workflow_generation=v_active_workflow.generation
      and (request_row.state='APPROVED' or (
        request_row.state='PENDING'
        and (request_row.expires_at_utc is null or request_row.expires_at_utc>now())
      ))
    order by (request_row.state='PENDING'
      and (request_row.expires_at_utc is null or request_row.expires_at_utc>now())) desc,
      request_row.request_generation desc,request_row.created_at_utc desc
    limit 1;
    v_manager_pending:=coalesce(v_request.state='PENDING',false);
    v_manager_approved:=v_manager_approved or coalesce(v_request.state='APPROVED',false);
  end if;

  v_contract_week_authorised:=upper(coalesce(v_week.status::text,''))='AUTHORISED';
  v_office_authorised:=v_current.authorised_at_server is not null
    or v_fin.authorised_at_utc is not null or v_contract_week_authorised;
  v_paid:=v_fin.paid_at_utc is not null;
  v_invoiced:=v_fin.locked_by_invoice_id is not null or exists(
    select 1 from jsonb_array_elements(
      case when jsonb_typeof(v_fin.invoice_breakdown_json->'segments')='array'
        then v_fin.invoice_breakdown_json->'segments' else '[]'::jsonb end
    ) segment
    where nullif(btrim(coalesce(segment->>'invoice_locked_invoice_id','')),'') is not null
  );
  select public.timesheet_removal_financial_history_v1(
    array_agg(history.timesheet_id order by history.timesheet_id),
    array[v_booking_id],
    case when v_week.id is null then array[]::uuid[] else array[v_week.id] end
  ) into v_history
  from public.timesheets history where history.booking_id=v_booking_id;
  v_protected_history:=coalesce((v_history->>'archive_required')::boolean,false)
    or exists(select 1 from jsonb_array_elements(coalesce(v_history->'blockers','[]'::jsonb)) blocker
      where blocker->>'code' not in ('TIMESHEET_STILL_AUTHORISED'));
  -- DY06: only genuine Daily NEXT reclassifies the approval Archive mark.
  if v_scope='DAILY'
     and (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    v_protected_history:=private.bpay_next_daily_pay_retention_v1(v_booking_id,v_history);
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    case when v_week.id is null then null else v_week.id end,
    false
  );
  v_row_signature:=nullif(btrim(coalesce(
    v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature',''
  )), '');
  if v_row_signature is null then
    raise exception 'ROUTE_CHANGE_ROW_SIGNATURE_UNAVAILABLE' using errcode='55000';
  end if;
  if v_action='REVERT_TO_ELECTRONIC' then
    v_restore_proof:=private._timesheet_exact_electronic_restore_proof_v1(v_current.timesheet_id);
  end if;

  if v_import_authoritative then
    v_warning_code:='ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK';
    v_block_reason:='IMPORT_AUTHORITATIVE';
  elsif v_paid or v_invoiced or v_protected_history then
    v_warning_code:='ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK';
    v_block_reason:='FINANCIAL_HISTORY';
  elsif v_office_authorised then
    v_warning_code:='ROUTE_CHANGE_REQUIRES_UNAUTHORISE';
    v_block_reason:='AUTHORISED';
  elsif coalesce((v_paper_source_context->>'identity_conflict')::boolean,false)
        and not v_resolvable_paper_context_conflict then
    v_warning_code:='ROUTE_CHANGE_WORKFLOW_CONFLICT';
    v_block_reason:=coalesce(
      v_paper_source_context->>'conflict_reason',
      'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
    );
  -- A distinct active workflow must ordinarily fail closed.  The one approved
  -- exception is an explicitly confirmed standalone incomplete expense claim,
  -- which confirmation supersedes before the route source is rotated.
  elsif v_action in ('SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL')
        and v_active_workflow.id is not null
        and not v_incomplete_expense_removal_required
        and (
          v_active_workflow.id is distinct from v_linked_workflow.id
          or v_active_workflow.state='REFUSED'
        ) then
    v_warning_code:='ROUTE_CHANGE_WORKFLOW_CONFLICT';
    v_block_reason:='CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT';
  elsif v_active_workflow_count>1 then
    v_warning_code:='ROUTE_CHANGE_WORKFLOW_CONFLICT';
    v_block_reason:='MULTIPLE_ACTIVE_WORKFLOWS';
  elsif v_scope='DAILY'
        and v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR') then
    v_warning_code:='DAILY_PAPER_ROUTE_NOT_ALLOWED';
    v_block_reason:='DAILY_PAPER_ROUTE_NOT_ALLOWED';
  elsif v_action='SWITCH_TO_MANUAL' and v_route_family='ELECTRONIC'
        and v_scope='WEEKLY' and v_week.id is not null and v_fin.id is not null
        and v_basis='CONTRACT_WEEKLY' and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_reason_required:=true;
    v_warning_code:=case
      when v_incomplete_expense_removal_required
        then 'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM'
      when v_manager_approved or v_final_signed_ready then 'MANAGER_APPROVED_TO_MANUAL'
      when v_candidate_signed or v_manager_pending then 'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL'
      else 'ELECTRONIC_UNSIGNED_TO_MANUAL' end;
  elsif v_action='SWITCH_DAILY_TO_MANUAL' and v_route_family='ELECTRONIC'
        and v_scope='DAILY' and v_fin.id is not null
        and v_basis not in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT')
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_reason_required:=true;
    v_warning_code:=case
      when v_incomplete_expense_removal_required
        then 'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM'
      when v_manager_approved or v_final_signed_ready then 'MANAGER_APPROVED_TO_MANUAL'
      when v_candidate_signed or v_manager_pending then 'CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL'
      else 'ELECTRONIC_UNSIGNED_TO_MANUAL' end;
  elsif v_action in ('CONVERT_QR_TO_MANUAL','DISABLE_QR') and v_route_family='QR'
        and v_actual_qr_backing and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    if v_incomplete_expense_removal_required then
      v_permitted:=true;
      v_reason_required:=true;
      v_warning_code:='CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVE_CONFIRM';
    elsif v_qr_signed then
      v_permitted:=true;
      v_reason_required:=true;
      v_warning_code:='QR_SIGNED_TO_MANUAL';
    elsif v_qr_pack_issued_or_sent then
      v_permitted:=true;
      v_reason_required:=true;
      v_warning_code:='QR_ISSUED_TO_MANUAL';
    elsif v_qr_pack_ready then
      v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
      v_block_reason:='QR_PACK_READY_NOT_ISSUED';
    else
      v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
      v_block_reason:='QR_PACK_PREPARATION_IN_PROGRESS';
    end if;
  elsif v_action='ALLOW_ELECTRONIC_AGAIN' and v_route_family='MANUAL_NON_QR'
        and upper(coalesce(v_effective_mode,''))='ELECTRONIC'
        and v_fin.id is not null and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='FRESH_ELECTRONIC_RESUBMISSION_REQUIRED';
  elsif v_action='ALLOW_QR_AGAIN' and v_route_family='MANUAL_NON_QR'
        and v_scope='WEEKLY'
        and (v_prior_qr or v_paper_allowed) and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='FRESH_PAPER_RESUBMISSION_REQUIRED';
  elsif v_action in ('INVALIDATE_QR','REISSUE_QR') and v_route_family='QR'
        and v_scope='WEEKLY'
        and v_actual_qr_backing and v_qr_issued and v_fin.id is not null
        and not coalesce(v_current.is_adjustment,false) then
    v_permitted:=true;
    v_warning_code:='QR_REPLACEMENT_PACK_REQUIRED';
  elsif v_action='REVERT_TO_ELECTRONIC' and v_route_family='MANUAL_NON_QR'
        and coalesce((v_restore_proof->>'proven')::boolean,false) then
    v_permitted:=true;
    v_warning_code:='EXACT_ELECTRONIC_RESTORE_PROVEN';
  else
    v_warning_code:='ROUTE_CHANGE_NOT_PERMITTED';
    v_block_reason:='ROUTE_OR_POLICY_MISMATCH';
  end if;
  v_confirmation_required:=v_permitted;

  v_payload:=jsonb_build_object(
    'contract_version','TIMESHEET_ROUTE_CHANGE_CONTEXT_V1',
    'target_action',v_action,
    'booking_id',v_booking_id,
    'requested_timesheet_id',v_requested.timesheet_id,
    'current_timesheet_id',v_current.timesheet_id,
    'current_version',v_current.version,
    'current_updated_at_utc',v_current.updated_at,
    'current_status',v_current.status,
    'current_row_signature',v_row_signature,
    'row_signature',v_row_signature,
    'sheet_scope',v_scope,
    'tsfin_basis',v_basis,
    'pending_route_intent',v_current.candidate_submission_route_intent,
    'route_family',v_route_family,
    'effective_submission_mode',v_effective_mode,
    'effective_paper_submission_allowed',v_paper_allowed,
    'prior_qr_lineage',v_prior_qr,
    'import_authoritative',v_import_authoritative,
    'office_authorised',v_office_authorised,
    'contract_week_authorised',v_contract_week_authorised,
    'invoiced',v_invoiced,
    'paid',v_paid,
    'protected_financial_history',v_protected_history,
    'candidate_signed',v_candidate_signed,
    'manager_approval_pending',v_manager_pending,
    'manager_approved',v_manager_approved,
    'final_signed_document_ready',v_final_signed_ready,
    'qr_route_active',v_qr_route_active,
    'qr_code_generated',v_qr_code_generated,
    'qr_pack_ready',v_qr_pack_ready,
    'qr_pack_issued_or_sent',v_qr_pack_issued_or_sent,
    'qr_pack_issued',v_qr_issued,
    'qr_signed_returned',v_qr_signed
  )||jsonb_build_object(
    'linked_workflow_id',v_linked_workflow.id,
    'linked_workflow_generation',v_linked_workflow.generation,
    'linked_workflow_state',v_linked_workflow.state,
    'linked_workflow_updated_at_utc',v_linked_workflow.updated_at_utc,
    'paper_workflow_id',v_paper_workflow.id,
    'paper_workflow_generation',v_paper_workflow.generation,
    'paper_workflow_state',v_paper_workflow.state,
    'paper_source_workflow_context',v_paper_source_context,
    'paper_source_nonterminal_workflow_count',
      coalesce((v_paper_source_context->>'nonterminal_workflow_count')::integer,0),
    'paper_source_affected_nonterminal_workflow_count',
      coalesce((v_paper_source_context->>'affected_nonterminal_workflow_count')::integer,0),
    'paper_source_current_token_owner_workflow_id',
      nullif(v_paper_source_context->>'current_token_owner_workflow_id','')::uuid,
    'active_workflow_id',v_active_workflow.id,
    'active_workflow_count',v_active_workflow_count,
    'active_workflow_generation',v_active_workflow.generation,
    'incomplete_expense_claim_removal_required',
      v_incomplete_expense_removal_required,
    'incomplete_expense_workflow_id',case
      when v_incomplete_expense_removal_required then v_active_workflow.id else null end,
    'incomplete_expense_workflow_state',case
      when v_incomplete_expense_removal_required then v_active_workflow.state else null end,
    'incomplete_expense_confirmation_message',case
      when v_incomplete_expense_removal_required then
        'The candidate has started an expense claim but has not completed it. Do you want to remove the incomplete claim and continue?'
      else null end,
    'active_approval_request_id',v_request.id,
    'active_approval_request_state',v_request.state,
    'active_approval_request_updated_at_utc',v_request.updated_at_utc,
    'exact_electronic_restore_proof',v_restore_proof,
    'tsfin_id',v_fin.id,
    'tsfin_updated_at_utc',v_fin.updated_at,
    'permitted_action',v_permitted,
    'warning_code',v_warning_code,
    'confirmation_required',v_confirmation_required,
    'reason_required',v_reason_required,
    'block_reason',v_block_reason,
    'fresh_submission_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'notification_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR')
  );
  v_context_hash:=encode(digest(convert_to(v_payload::text,'UTF8'),'sha256'),'hex');
  return v_payload||jsonb_build_object('context_sha256',v_context_hash);
end;
$function$;

create or replace function public.timesheet_route_version_rotate(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_target_action text,
  p_actor_user_id uuid,
  p_allow_manual_only boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_environment text;
  v_office_service boolean:=false;
  v_weekly_source_booking_id text;
  v_weekly_source_guard jsonb;
  v_import_current public.timesheets%rowtype;
  v_import_week public.contract_weeks%rowtype;
  v_import_fin public.timesheets_financials%rowtype;
  v_import_contract_id uuid;
  v_import_client_id uuid;
  v_import_authority jsonb;
  v_next_daily_current public.timesheets%rowtype;
begin
  -- Plan 6.2 G6-11 (proof/34 section 3).  An authorised Weekly-Source-managed
  -- root may never be rotated.  The refusal joins the existing block list ahead
  -- of FINANCIAL_HISTORY and sits in this dispatcher before the
  -- candidate_route_confirmation flag branch, so it covers both the
  -- _core_v1 and the _legacy_v1 body.  The guard writes nothing and this
  -- dispatcher has not written anything at this point.
  -- WP-09b review finding F1: the verdict must be taken while this transaction
  -- already holds the compatible locks of proof/34 section 5, not before them.
  -- Read before any lock, a first authorisation that holds the family and
  -- commits afterwards leaves this dispatcher free to rotate the now-authorised
  -- managed root.  Trimmed key first, then the raw key only when it differs,
  -- then the exact raw-booking family rows: the same deadlock-free order as
  -- public.timesheet_route_version_confirmed_v1 below and the I-1 helper, and
  -- the bodies this dispatcher calls re-take the raw key re-entrantly.  A null
  -- or blank booking identity is left to the guard, which already refuses it.
  select rotate_target.booking_id into v_weekly_source_booking_id
  from public.timesheets rotate_target
  where rotate_target.timesheet_id=p_current_timesheet_id;
  -- SP-12: existing admission before identity/rotation business locks.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  if v_weekly_source_booking_id is not null
     and btrim(v_weekly_source_booking_id)<>'' then
    perform pg_advisory_xact_lock(hashtext(btrim(v_weekly_source_booking_id)));
    if v_weekly_source_booking_id<>btrim(v_weekly_source_booking_id) then
      perform pg_advisory_xact_lock(hashtext(v_weekly_source_booking_id));
    end if;
    perform 1 from public.timesheets locked_member
    where locked_member.booking_id=v_weekly_source_booking_id for update;
  end if;
  v_weekly_source_guard:=private.weekly_source_managed_root_guard_v1(
    p_current_timesheet_id
  );
  -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
  -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
  -- protected family whose identity cannot be resolved, to a family carrying
  -- protected pay evidence but no authorisation row
  -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
  -- unauthorised-Timesheet contradiction, which must never be allowed to
  -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
  -- family -- including a malformed one -- keeps exactly the behaviour it had
  -- before this feature was installed.  Absent, null and non-boolean take the
  -- unsafe value at every read (Part 1 addendum rule 4).
  if (coalesce((v_weekly_source_guard->>'managed')::boolean, true)
       and (coalesce((v_weekly_source_guard->>'ok')::boolean, true)
            or coalesce((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
     or coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
     or (v_weekly_source_guard->>'protected_target_ownership_state') is not null then
    raise exception 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      using errcode='55000',detail=jsonb_build_object(
        'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
        'entry_point','E1:public.timesheet_route_version_rotate',
        'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
        'refusal_basis', case
          -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
          -- canonical booking-reference collision and must be named as one.
          -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
          -- release, so the order of this edit and WP-03's rename cannot open a gap in
          -- which Office stops seeing the ruled name.
          when v_weekly_source_guard->>'reason' in (
                 'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
            then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
          when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
            then 'WEEKLY_SOURCE_MANAGED_ROOT'
          when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
            then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
          when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
            then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
          else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
        'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
        'timesheet_id',p_current_timesheet_id,
        'reason',v_weekly_source_guard->>'reason'
      )::text;
  end if;
  -- Banking Pay RESET: the flag-off legacy ALLOW_QR_AGAIN branch does not
  -- perform its own import-authority check. The current confirmation preview
  -- and coordinated core both refuse imported roots. Enforce that same
  -- retained classifier here, before either mutation owner can be dispatched.
  -- The existing guard resolved this exact canonical current root while the
  -- compatible booking locks above are held; do not resolve a second family
  -- or classify a stale caller-named physical member. Unknown/unbound malformed
  -- identifiers keep the existing downstream typed outcomes.
  if coalesce((v_weekly_source_guard->>'ok')::boolean,false)
     and nullif(v_weekly_source_guard->>'canonical_timesheet_id','') is not null then
    select import_current.* into v_import_current
    from public.timesheets import_current
    where import_current.timesheet_id=(v_weekly_source_guard->>'canonical_timesheet_id')::uuid
      and import_current.is_current=true;
    if found then
      select import_week.* into v_import_week
      from public.contract_weeks import_week
      where import_week.timesheet_id=v_import_current.timesheet_id
      order by import_week.updated_at desc,import_week.created_at desc limit 1;
      select import_fin.* into v_import_fin
      from public.timesheets_financials import_fin
      where import_fin.timesheet_id=v_import_current.timesheet_id
        and import_fin.is_current=true
      order by import_fin.updated_at desc,import_fin.created_at desc limit 1;
      v_import_contract_id:=coalesce(v_import_week.contract_id,v_import_current.contract_id);
      v_import_client_id:=v_import_fin.client_id;
      if v_import_contract_id is not null then
        select coalesce(import_contract.client_id,v_import_client_id)
        into v_import_client_id from public.contracts import_contract
        where import_contract.id=v_import_contract_id;
      end if;
      if v_import_client_id is not null then
        v_import_authority:=private._candidate_import_authoritative_v1(
          v_import_client_id,v_import_contract_id,v_import_current.timesheet_id,
          case when v_import_fin.id is null then null else to_jsonb(v_import_fin) end,
          v_import_current.week_ending_date
        );
        if coalesce((v_import_authority->>'is_import_authoritative')::boolean,false) then
          raise exception 'ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK'
            using errcode='55000',detail=jsonb_build_object(
              'code','ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK',
              'entry_point','E1:public.timesheet_route_version_rotate',
              'block_reason','IMPORT_AUTHORITATIVE',
              'source_family',v_import_authority->>'source_family',
              'current_timesheet_id',v_import_current.timesheet_id,
              'action',upper(btrim(coalesce(p_target_action,'')))
            )::text;
        end if;
      end if;
    end if;
  end if;
  select candidate_app_environment into v_environment
  from public.settings_defaults where id=1;
  v_environment:=private._candidate_assert_environment(v_environment);
  v_office_service:=private._candidate_office_service_context_valid_v1(
    v_environment,p_actor_user_id,'ROUTE_CONFIRM'
  );
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
     and not v_office_service then
    -- DY06 accepted capture fence; preserve the original branch/input refusals.
    if p_actor_user_id is not null and p_current_timesheet_id is not null
       and p_expected_timesheet_id is not null
       and upper(btrim(coalesce(p_target_action,''))) in (
         'ALLOW_ELECTRONIC_AGAIN','SWITCH_DAILY_TO_MANUAL','REVERT_TO_ELECTRONIC')
       and (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
      select t.* into v_next_daily_current from public.timesheets t
        where t.booking_id=v_weekly_source_booking_id and t.timesheet_id=p_expected_timesheet_id
          and t.is_current is true and t.sheet_scope='DAILY';
      if found then
        -- NOWAIT avoids a new blocking Work-after-Timesheet edge.
        perform 1 from private.bpay_next_work w
          where w.booking_id=v_next_daily_current.booking_id for update nowait;
        if private.bpay_next_daily_pay_retention_v1(v_next_daily_current.booking_id) then
          raise exception 'ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK' using errcode='55000';
        end if;
      end if;
    end if;
    return private._timesheet_route_version_legacy_v1(
      p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
      p_actor_user_id,p_allow_manual_only
    );
  end if;
  if coalesce(current_setting('cloudtms.route_transition_confirmed',true),'') <> 'on' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_REQUIRED'
      using errcode='55000',detail=jsonb_build_object(
        'code','ROUTE_CHANGE_CONFIRMATION_REQUIRED',
        'preview_rpc','timesheet_route_version_preview_v1',
        'confirm_rpc','timesheet_route_version_confirmed_v1'
      )::text;
  end if;
  -- DY06 accepted capture fence; preserve the original branch/input refusals.
  if p_actor_user_id is not null and p_current_timesheet_id is not null
     and p_expected_timesheet_id is not null
     and upper(btrim(coalesce(p_target_action,''))) in (
       'ALLOW_ELECTRONIC_AGAIN','SWITCH_DAILY_TO_MANUAL','REVERT_TO_ELECTRONIC')
     and (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    select t.* into v_next_daily_current from public.timesheets t
      where t.booking_id=v_weekly_source_booking_id and t.timesheet_id=p_expected_timesheet_id
        and t.is_current is true and t.sheet_scope='DAILY';
    if found then
      -- NOWAIT avoids a new blocking Work-after-Timesheet edge.
      perform 1 from private.bpay_next_work w
        where w.booking_id=v_next_daily_current.booking_id for update nowait;
      if private.bpay_next_daily_pay_retention_v1(v_next_daily_current.booking_id) then
        raise exception 'ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK' using errcode='55000';
      end if;
    end if;
  end if;
  return private._timesheet_route_version_core_v1(
    p_current_timesheet_id,p_expected_timesheet_id,p_target_action,
    p_actor_user_id,p_allow_manual_only
  );
end;
$function$;

create or replace function public.timesheet_route_version_confirmed_v1(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_expected_context_sha256 text,
  p_target_action text,
  p_actor_user_id uuid,
  p_reason_code text default null,
  p_reason_note text default null,
  p_idempotency_key text default null,
  p_allow_manual_only boolean default false,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_target_action,'')));
  v_reason_code text:=upper(btrim(coalesce(p_reason_code,'')));
  v_reason_note text:=nullif(btrim(coalesce(p_reason_note,'')),'');
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_environment text;
  v_route_family_key text;
  v_weekly_source_guard jsonb;
  v_context jsonb;
  v_result jsonb;
  v_workflow_result jsonb:='{}'::jsonb;
  v_primary_workflow_result jsonb:='{}'::jsonb;
  v_incomplete_workflow_result jsonb:='{}'::jsonb;
  v_primary_workflow_id uuid;
  v_incomplete_expense_workflow_id uuid;
  v_notification_result jsonb:=jsonb_build_object(
    'notification_required',false,'notification_created',false,
    'notification_recipient_unavailable',false,'notification_dedupe_keys','[]'::jsonb
  );
  v_current_signature text;
  v_new_timesheet_id uuid;
  v_new_version integer;
  v_reason_required boolean;
  v_retire_workflow boolean:=false;
  v_retirement_reason text;
  v_intervention_reasons constant text[]:=array[
    'CANDIDATE_SUPPLIED_MANUAL_TIMESHEET',
    'CANDIDATE_REPORTED_HOURS_INCORRECT',
    'HIRING_MANAGER_REPORTED_HOURS_INCORRECT',
    'ELECTRONIC_SUBMISSION_TECHNICAL_FAILURE',
    'OTHER_EXCEPTIONAL_OFFICE_INTERVENTION'
  ];
begin
  if p_actor_user_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or coalesce(p_expected_context_sha256,'') !~ '^[0-9a-fA-F]{64}$' then
    raise exception 'ROUTE_CHANGE_CONFIRMATION_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_requested from public.timesheets
  where timesheet_id=p_current_timesheet_id;
  if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  select candidate_app_environment into v_environment
  from public.settings_defaults where id=1;
  v_environment:=private._candidate_assert_environment(v_environment);
  if not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
     and not private._candidate_office_service_context_valid_v1(
       v_environment,p_actor_user_id,'ROUTE_CONFIRM'
     ) then
    raise exception 'CANDIDATE_ROUTE_CONFIRMATION_DISABLED' using errcode='42501';
  end if;
  -- SP-12: existing admission before identity/rotation business locks.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  -- The same stable Candidate contract/week family lock is taken by office
  -- rejection, workflow cancellation/supersession and route intervention.
  -- It must precede target, booking, workflow and QR-source row locks.
  v_route_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
    ||coalesce(v_requested.contract_id::text,'-')||':'
    ||coalesce(v_requested.week_ending_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_route_family_key,0));
  perform pg_advisory_xact_lock(hashtext(btrim(v_requested.booking_id)));
  perform 1 from public.timesheets
  where booking_id=v_requested.booking_id for update;
  select * into v_current from public.timesheets
  where booking_id=v_requested.booking_id and is_current=true
  order by version desc,updated_at desc,created_at desc limit 1 for update;
  if v_current.timesheet_id is distinct from p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001',detail=jsonb_build_object(
      'current_timesheet_id',v_current.timesheet_id)::text;
  end if;
  -- Plan 6.2 G6-11 (proof/34 section 3).  The E1 dispatcher guard covers the
  -- rotation itself, but this route supersedes candidate workflow rows through
  -- private._timesheet_route_supersede_candidate_v1 before it reaches the
  -- dispatcher, so the managed-root refusal is also taken here: after the
  -- trimmed-key advisory lock and the family rows FOR UPDATE, and before this
  -- owner's first write (HANDOVER 2 round 2 Q9; ROT-004, ROT-012).
  v_weekly_source_guard:=private.weekly_source_managed_root_guard_v1(
    v_current.timesheet_id
  );
  -- HANDOVER 2 round-5 ruling B3 and A4 (18 September 2026).  The refusal is
  -- NARROWED: it applies to a Weekly-Source managed root, to a bound or
  -- protected family whose identity cannot be resolved, to a family carrying
  -- protected pay evidence but no authorisation row
  -- (PROTECTED_ROOT_AUTHORITY_MISSING), and to the live-record-on-an-
  -- unauthorised-Timesheet contradiction, which must never be allowed to
  -- continue merely because managed is false.  An unrelated, UNBOUND ordinary
  -- family -- including a malformed one -- keeps exactly the behaviour it had
  -- before this feature was installed.  Absent, null and non-boolean take the
  -- unsafe value at every read (Part 1 addendum rule 4).
  if (coalesce((v_weekly_source_guard->>'managed')::boolean, true)
       and (coalesce((v_weekly_source_guard->>'ok')::boolean, true)
            or coalesce((v_weekly_source_guard->>'weekly_source_bound')::boolean, true)))
     or coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
     or (v_weekly_source_guard->>'protected_target_ownership_state') is not null then
    raise exception 'WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'
      using errcode='55000',detail=jsonb_build_object(
        'code','WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED',
        'entry_point','E1a:public.timesheet_route_version_confirmed_v1',
        'block_reason','WEEKLY_SOURCE_MANAGED_ROOT',
        'refusal_basis', case
          -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a
          -- canonical booking-reference collision and must be named as one.
          -- WP-03 handoff N20: accept BOTH the installed token and the ruled name for one
          -- release, so the order of this edit and WP-03's rename cannot open a gap in
          -- which Office stops seeing the ruled name.
          when v_weekly_source_guard->>'reason' in (
                 'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION')
            then 'BOOKING_REFERENCE_CANONICAL_COLLISION'
          when coalesce((v_weekly_source_guard->>'managed')::boolean, true) and coalesce((v_weekly_source_guard->>'ok')::boolean, true)
            then 'WEEKLY_SOURCE_MANAGED_ROOT'
          when coalesce((v_weekly_source_guard->>'managed')::boolean, true)
            then 'WEEKLY_SOURCE_BOUND_OR_PROTECTED_ROOT_UNRESOLVABLE'
          when coalesce((v_weekly_source_guard->>'authorisation_record_without_authorised_timesheet')::boolean, false)
            then 'AUTHORISATION_RECORD_WITHOUT_AUTHORISED_TIMESHEET'
          else 'PROTECTED_ROOT_AUTHORITY_MISSING' end,
        'integrity_failure', not coalesce((v_weekly_source_guard->>'ok')::boolean,false),
        'timesheet_id',v_current.timesheet_id,
        'reason',v_weekly_source_guard->>'reason'
      )::text;
  end if;
  perform 1 from public.contract_weeks
  where timesheet_id=v_current.timesheet_id for update;
  perform 1 from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true for update;
  -- Lock the complete booking/version-family workflow catalogue, including a
  -- draft or submitted PAPER claim that has no mail receipt yet.  The family
  -- advisory lock above keeps this order consistent with rejection and claim
  -- retirement; UUID ordering keeps row acquisition deterministic.
  perform 1 from public.candidate_submission_workflows workflow
  where workflow.id=v_current.candidate_workflow_id
     or exists(
       select 1
       from public.timesheets binding_source
       where binding_source.timesheet_id in (
           workflow.target_timesheet_id,workflow.anchor_timesheet_id
         )
         and binding_source.booking_id=v_current.booking_id
     )
  order by workflow.id for update;
  perform 1 from public.candidate_approval_requests request_row
  where request_row.workflow_id in (
    select workflow.id
    from public.candidate_submission_workflows workflow
    where workflow.id=v_current.candidate_workflow_id
       or exists(
         select 1
         from public.timesheets binding_source
         where binding_source.timesheet_id in (
             workflow.target_timesheet_id,workflow.anchor_timesheet_id
           )
           and binding_source.booking_id=v_current.booking_id
       )
  ) order by request_row.id for update;

  -- DY06: serialize accepted Create before the original fresh context reproof.
  if v_current.sheet_scope='DAILY'
     and (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform 1 from private.bpay_next_work w where w.booking_id=v_current.booking_id for update nowait;
  end if;

  v_context:=private._timesheet_route_change_context_v1(v_current.timesheet_id,v_action);
  v_current_signature:=nullif(btrim(coalesce(v_context->>'current_row_signature','')),'');
  if v_current_signature is distinct from btrim(p_expected_row_signature) then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001',detail=jsonb_build_object(
      'expected_row_signature',p_expected_row_signature,
      'current_row_signature',v_current_signature)::text;
  end if;
  if lower(v_context->>'context_sha256') is distinct from lower(p_expected_context_sha256) then
    raise exception 'ROUTE_CHANGE_CONTEXT_CHANGED' using errcode='40001',detail=jsonb_build_object(
      'warning_code',v_context->>'warning_code',
      'current_context_sha256',v_context->>'context_sha256')::text;
  end if;
  if not coalesce((v_context->>'permitted_action')::boolean,false) then
    raise exception 'ROUTE_CHANGE_NOT_PERMITTED' using errcode='55000',detail=v_context::text;
  end if;
  v_reason_required:=coalesce((v_context->>'reason_required')::boolean,false);
  if v_reason_required and not (v_reason_code=any(v_intervention_reasons)) then
    raise exception 'ROUTE_INTERVENTION_REASON_REQUIRED' using errcode='22023';
  end if;
  if v_reason_code='OTHER_EXCEPTIONAL_OFFICE_INTERVENTION' and v_reason_note is null then
    raise exception 'ROUTE_INTERVENTION_REASON_NOTE_REQUIRED' using errcode='22023';
  end if;

  v_retire_workflow:=v_action in (
    'CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR',
    'SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL'
  );
  v_retirement_reason:=case when v_action in ('INVALIDATE_QR','REISSUE_QR')
    then 'QR_REPLACED_BY_OFFICE' else v_reason_code end;
  if v_retire_workflow then
    v_primary_workflow_id:=nullif(case
        when v_action in ('CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR')
          then coalesce(v_context->>'paper_workflow_id',v_context->>'linked_workflow_id')
        else v_context->>'linked_workflow_id'
      end,'')::uuid;
    v_primary_workflow_result:=private._timesheet_route_supersede_candidate_v1(
      v_primary_workflow_id,v_action,
      v_retirement_reason,v_reason_note,p_actor_user_id,p_now_utc
    );
    v_workflow_result:=v_primary_workflow_result;
  end if;
  if coalesce(
      (v_context->>'incomplete_expense_claim_removal_required')::boolean,false
    ) then
    v_incomplete_expense_workflow_id:=nullif(
      v_context->>'incomplete_expense_workflow_id',''
    )::uuid;
    if v_incomplete_expense_workflow_id is null then
      raise exception 'CANDIDATE_INCOMPLETE_EXPENSE_WORKFLOW_CONTEXT_CHANGED'
        using errcode='40001';
    end if;
    if v_incomplete_expense_workflow_id is not distinct from v_primary_workflow_id then
      v_incomplete_workflow_result:=v_primary_workflow_result;
    else
      v_incomplete_workflow_result:=private._timesheet_route_supersede_candidate_v1(
        v_incomplete_expense_workflow_id,v_action,
        v_retirement_reason,v_reason_note,p_actor_user_id,p_now_utc
      );
    end if;
    if not coalesce(
      (v_incomplete_workflow_result->>'workflow_changed')::boolean,false
    ) then
      raise exception 'CANDIDATE_INCOMPLETE_EXPENSE_CLAIM_REMOVAL_NOT_PROVEN'
        using errcode='40001',detail=v_incomplete_workflow_result::text;
    end if;
    v_workflow_result:=v_primary_workflow_result||jsonb_build_object(
      'workflow_changed',coalesce(
          (v_primary_workflow_result->>'workflow_changed')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'workflow_changed')::boolean,false
        ),
      'manager_request_cancelled',coalesce(
          (v_primary_workflow_result->>'manager_request_cancelled')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'manager_request_cancelled')::boolean,false
        ),
      'manager_cancellation_email_queued',coalesce(
          (v_primary_workflow_result->>'manager_cancellation_email_queued')::boolean,false
        ) or coalesce(
          (v_incomplete_workflow_result->>'manager_cancellation_email_queued')::boolean,false
        ),
      'manager_cancellation_mail_ids',
        coalesce(v_primary_workflow_result->'manager_cancellation_mail_ids','[]'::jsonb)
        ||case
          when v_incomplete_expense_workflow_id is distinct from v_primary_workflow_id
            then coalesce(
              v_incomplete_workflow_result->'manager_cancellation_mail_ids','[]'::jsonb
            )
          else '[]'::jsonb
        end,
      'incomplete_expense_claim_removed',true,
      'incomplete_expense_workflow_id',v_incomplete_expense_workflow_id,
      'primary_workflow_retirement',v_primary_workflow_result,
      'incomplete_expense_workflow_retirement',v_incomplete_workflow_result
    );
  end if;
  -- Last transactional invariant before any route/version rotation: no live
  -- PAPER workflow may remain tied exclusively to the worked-row source that
  -- is about to become historical.  This is intentionally mail-independent;
  -- WORKER_DRAFT and approval-stage workflows have no receipt to discover.
  if v_action in (
      'CONVERT_QR_TO_MANUAL','DISABLE_QR','INVALIDATE_QR','REISSUE_QR'
    ) and exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.route='PAPER'
        and workflow.state not in (
          'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
        )
        and workflow.contract_id is not distinct from v_current.contract_id
        and workflow.week_ending_date is not distinct from v_current.week_ending_date
        and exists(
          select 1
          from public.timesheets binding_source
          where binding_source.timesheet_id in (
              workflow.target_timesheet_id,workflow.anchor_timesheet_id
            )
            and binding_source.booking_id=v_current.booking_id
        )
    ) then
    raise exception 'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
        'current_timesheet_id',v_current.timesheet_id,
        'target_action',v_action
      )::text;
  end if;
  -- Equivalent mail-independent invariant for ELECTRONIC source rotation.
  -- The selected mutable workflow is superseded above.  Historical FINALISED
  -- truth may remain, but no active or amendable Candidate workflow may still
  -- resolve only through the booking/version family about to become historic.
  if v_action in ('SWITCH_TO_MANUAL','SWITCH_DAILY_TO_MANUAL') and exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.state not in (
          'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
        )
        and workflow.contract_id is not distinct from v_current.contract_id
        and exists(
          select 1
          from public.timesheets binding_source
          where binding_source.timesheet_id in (
              workflow.target_timesheet_id,workflow.anchor_timesheet_id
            )
            and binding_source.booking_id=v_current.booking_id
        )
    ) then
    raise exception 'CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_ROUTE_ACTIVE_WORKFLOW_CONFLICT',
        'current_timesheet_id',v_current.timesheet_id,
        'target_action',v_action
      )::text;
  end if;
  perform set_config('cloudtms.route_transition_confirmed','on',true);
  v_result:=public.timesheet_route_version_rotate(
    v_current.timesheet_id,p_expected_timesheet_id,v_action,p_actor_user_id,
    case when v_action='REVERT_TO_ELECTRONIC' then true else p_allow_manual_only end
  );
  v_new_timesheet_id:=nullif(v_result->>'new_timesheet_id','')::uuid;
  v_new_version:=nullif(v_result->>'new_version','')::integer;

  if v_reason_required then
    update public.timesheets set
      revoked_reason=left(coalesce(revoked_reason,v_action)||':'||v_reason_code,500),
      updated_at=p_now_utc
    where timesheet_id=v_current.timesheet_id and is_current=false;
  end if;
  perform private._candidate_audit_v1(
    'timesheet_route',v_current.timesheet_id::text,'ROUTE_VERSION_CONFIRMED',
    v_context-'context_sha256',
    jsonb_build_object('result',v_result,'reason_code',nullif(v_reason_code,''),
      'reason_note',v_reason_note,'idempotency_key',nullif(btrim(coalesce(p_idempotency_key,'')),'')),
    nullif(v_reason_code||case when v_reason_note is not null then ': '||v_reason_note else '' end,''),
    p_actor_user_id,nullif(btrim(coalesce(p_idempotency_key,'')),''),p_now_utc
  );

  if v_action='ALLOW_ELECTRONIC_AGAIN'
     and v_new_timesheet_id is not null then
    v_notification_result:=private._timesheet_route_resubmission_notifications_v1(
      v_current.timesheet_id,v_new_timesheet_id,v_new_version,v_action,p_now_utc
    );
  end if;
  return v_result||jsonb_build_object(
    'warning_code',v_context->>'warning_code',
    'confirmed_context_sha256',v_context->>'context_sha256',
    'intervention_reason_code',nullif(v_reason_code,''),
    'intervention_reason_note',v_reason_note,
    'workflow_retirement',v_workflow_result,
    'fresh_submission_required',v_action in ('ALLOW_ELECTRONIC_AGAIN','ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'notification_deferred_until_pack_ready',v_action in ('ALLOW_QR_AGAIN','INVALIDATE_QR','REISSUE_QR'),
    'retain_historical_evidence',true
  )||v_notification_result;
end;
$function$;

alter function private.bpay_next_daily_pay_retention_v1(text,jsonb) owner to postgres;
revoke all on function private.bpay_next_daily_pay_retention_v1(text,jsonb) from public,anon,authenticated,service_role;
alter function private._timesheet_route_change_context_v1(uuid,text) owner to postgres;
revoke all on function private._timesheet_route_change_context_v1(uuid,text) from public,anon,authenticated,service_role;
alter function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) owner to postgres;
revoke all on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) to service_role;
alter function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone) owner to postgres;
revoke all on function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamp with time zone) to service_role;
notify pgrst,'reload schema';
commit;
