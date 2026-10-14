-- Informational Banking -> Source V3 (8EEE3248) read. No financial admission,
-- mutation, history SUM, cash-to-hours conversion or legacy settlement writer.
-- V3.1 adds the two actual INTERNAL_ZERO statuses from registered 0480 only.

\set ON_ERROR_STOP on

begin;

create or replace function public.bpay_next_source_paid_evidence_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_actor uuid;v_root uuid;v_work uuid;v_revision uuid;v_context uuid;v_kind text;v_limit integer;
  v_after_key text;v_after_id uuid;v_after_no integer;v_after jsonb;v_scope record;v_worker record;
  v_origin jsonb;v_authority text;v_state text;v_reason text;v_header jsonb;v_context_value jsonb;
  v_query text;v_window jsonb;v_item jsonb;v_value jsonb;v_rows jsonb:='[]'::jsonb;
  v_cursor jsonb;v_count integer:=0;v_more boolean:=false;v_all_zero boolean:=true;
  v_positive_witness boolean:=false;v_membership_complete boolean:=false;v_certificate jsonb;
  v_coverage jsonb;v_result jsonb;v_bytes integer;v_scope_name text;
begin
  -- Public EXECUTE is service-only AND the actual PostgREST JWT is checked.
  perform private.weekly_source_query_require_service_v1();
  if pg_catalog.jsonb_typeof(p_request) is distinct from 'object'
    or not p_request ?& array['version','actor_user_id','root_timesheet_id','work_id','expected_revision_id',
      'kind','context_id','after','limit']
    or (p_request-array['version','actor_user_id','root_timesheet_id','work_id','expected_revision_id',
      'kind','context_id','after','limit'])<>'{}'::jsonb
    or p_request->>'version' is distinct from 'SOURCE_PAID_EVIDENCE_V1'
    or pg_catalog.jsonb_typeof(p_request->'version') is distinct from 'string'
    or pg_catalog.jsonb_typeof(p_request->'kind') is distinct from 'string'
    or p_request->>'kind' not in ('COMPONENTS','ACTIVE_HOLDS','WORKER_TRANSFERS')
    or pg_catalog.jsonb_typeof(p_request->'limit') is distinct from 'number'
    or p_request->>'limit' !~ '^([1-9][0-9]?|100)$' then
    raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_REQUEST_INVALID';
  end if;
  foreach v_kind in array array['actor_user_id','root_timesheet_id','work_id','expected_revision_id'] loop
    if pg_catalog.jsonb_typeof(p_request->v_kind) is distinct from 'string'
      or p_request->>v_kind !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_REQUEST_INVALID';
    end if;
  end loop;
  v_actor:=(p_request->>'actor_user_id')::uuid;v_root:=(p_request->>'root_timesheet_id')::uuid;
  v_work:=(p_request->>'work_id')::uuid;v_revision:=(p_request->>'expected_revision_id')::uuid;
  v_kind:=p_request->>'kind';v_limit:=(p_request->>'limit')::integer;v_after:=p_request->'after';
  if v_kind='WORKER_TRANSFERS' then
    if pg_catalog.jsonb_typeof(p_request->'context_id') is distinct from 'string'
      or p_request->>'context_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_REQUEST_INVALID';
    end if;
    v_context:=(p_request->>'context_id')::uuid;
  elsif p_request->'context_id'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_REQUEST_INVALID';
  end if;
  if v_after<>'null'::jsonb then
    if pg_catalog.jsonb_typeof(v_after) is distinct from 'object' then
      raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_CURSOR_INVALID';
    end if;
    if v_kind in ('COMPONENTS','ACTIVE_HOLDS') then
      if not v_after ? 'after_component_key'
        or pg_catalog.jsonb_typeof(v_after->'after_component_key') is distinct from 'string'
        or pg_catalog.octet_length(v_after->>'after_component_key') not between 1 and 1024
        or pg_catalog.char_length(v_after->>'after_component_key') not between 1 and 256 then
        raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_CURSOR_INVALID';
      end if;
      v_after_key:=v_after->>'after_component_key';
      if v_kind='COMPONENTS' and v_after-array['after_component_key']<>'{}'::jsonb then
        raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_CURSOR_INVALID';
      elsif v_kind='ACTIVE_HOLDS' then
        if not v_after ? 'after_hold_id' or v_after-array['after_component_key','after_hold_id']<>'{}'::jsonb
          or pg_catalog.jsonb_typeof(v_after->'after_hold_id') is distinct from 'string'
          or v_after->>'after_hold_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_CURSOR_INVALID';
        end if;
        v_after_id:=(v_after->>'after_hold_id')::uuid;
      end if;
    else
      if not v_after ? 'after_transfer_no' or v_after-array['after_transfer_no']<>'{}'::jsonb
        or pg_catalog.jsonb_typeof(v_after->'after_transfer_no') is distinct from 'string'
        or v_after->>'after_transfer_no' !~ '^[1-9][0-9]{0,9}$'
        or (v_after->>'after_transfer_no')::numeric>2147483647 then
        raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_PAID_CURSOR_INVALID';
      end if;
      v_after_no:=(v_after->>'after_transfer_no')::integer;
    end if;
  end if;
  if not exists(select 1 from private.bpay_next_module_control m where m.id=1
    and m.active_owner='NEXT' and m.owner_epoch>0) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',null,null,current_date);
  -- Current physical permission is independent of historical captured money.
  -- Exact raw family identity, not a trimmed remap or historical chooser.
  begin
  select t.timesheet_id,t.version,t.booking_id,t.week_ending_date,c.id as contract_id,c.client_id,
    c.candidate_id,w.original_timesheet_id as work_original_root,
    w.current_revision_id,w.applied_revision_id,w.current_revision_no,
    r.id as revision_id,r.source_kind,r.source_head_id,r.source_event_id,r.source_inventory_digest,
    r.physical_timesheet_id,r.physical_timesheet_version,r.sealed_at_utc,r.expected_line_count,
    a.id as live_authorisation_id,a.current_entitlement_head_id,
    a.family_booking_id as authorised_booking,a.timesheet_version as authorised_version,
    h.state as head_state,h.inventory_digest as head_inventory_digest,
    h.root_timesheet_id as head_root,h.root_timesheet_version as head_version,
    h.root_family_booking_id as head_booking,h.candidate_id as head_candidate,h.contract_id as head_contract,
    h.week_ending_date as head_week,h.authority_kind,
    tf.id as current_snapshot_id,tf.is_stale,tf.authorised_at_utc as snapshot_authorised_at,
    g.id as group_id,m.owner_epoch,wc.financial_view_revision
    into strict v_scope
  from public.timesheets t join public.contracts c on c.id=t.contract_id
  join private.bpay_next_work w on w.id=v_work and w.work_kind='SOURCE'
    and w.booking_id=t.booking_id and w.contract_id=c.id and w.candidate_id=c.candidate_id
    and w.week_ending_date=t.week_ending_date
  join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
  join private.bpay_next_module_control m on m.id=1
  left join private.bpay_next_worker_control wc on wc.candidate_id=w.candidate_id
  join public.weekly_source_group_clients gc on gc.client_id=c.client_id
    and t.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
  join public.weekly_source_groups g on g.id=gc.source_group_id and g.active
  left join public.weekly_source_root_authorisations a on a.root_timesheet_id=t.timesheet_id
    and a.withdrawn_at_utc is null
  left join public.weekly_source_entitlement_heads h on h.id=r.source_head_id
  left join public.timesheets_financials tf on tf.id=r.financial_snapshot_id and tf.is_current
  where t.timesheet_id=v_root and t.is_current and t.revoked_at is null and t.archived_at_utc is null
    and t.sheet_scope='WEEKLY' and t.line_type='HOURS';
  exception when no_data_found then
    raise exception using errcode='42501',message='BPAY_NEXT_SOURCE_PAID_SCOPE_FORBIDDEN';
  when too_many_rows then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_PAID_SCOPE_AMBIGUOUS';
  end;
  if coalesce(v_scope.current_revision_id,v_scope.applied_revision_id) is distinct from v_revision then
    raise exception using errcode='42501',message='BPAY_NEXT_SOURCE_PAID_SCOPE_FORBIDDEN';
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',
    v_scope.group_id,v_scope.client_id,v_scope.week_ending_date);
  v_origin:=private.bpay_next_source_origin_state_v1(v_work,v_revision); -- STABLE, not financial1519
  v_state:=v_origin->>'application_state';
  if v_state is null or v_state not in ('APPLIED','PENDING','UNAVAILABLE','INCOMPATIBLE')
    or v_origin->>'work_id' is distinct from v_work::text
    or v_origin->>'owner_epoch' is distinct from v_scope.owner_epoch::text then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_PAID_ORIGIN_INVALID';
  end if;
  v_authority:='NONE';
  if v_scope.current_revision_id is null then v_authority:='HISTORICAL_ONLY';
  elsif v_state='APPLIED' and v_scope.applied_revision_id=v_revision
    and v_scope.physical_timesheet_id=v_root and v_scope.physical_timesheet_version=v_scope.version
    and pg_catalog.octet_length(v_scope.source_inventory_digest)=32
    and v_scope.live_authorisation_id is not null and v_scope.authorised_booking=v_scope.booking_id
    and v_scope.authorised_version=v_scope.version
    and ((v_scope.source_head_id is null and v_scope.source_kind='SOURCE'
      and v_scope.current_entitlement_head_id is null and v_scope.source_event_id=v_scope.live_authorisation_id
      and v_scope.current_snapshot_id is not null and v_scope.is_stale is false
      and v_scope.snapshot_authorised_at is not null)
      or (v_scope.source_head_id is not null and v_scope.current_entitlement_head_id=v_scope.source_head_id
        and v_scope.source_event_id=v_scope.source_head_id and v_scope.head_state='COMMITTED_CURRENT'
        and v_scope.head_inventory_digest=v_scope.source_inventory_digest
        and (v_scope.head_root,v_scope.head_version,v_scope.head_booking,v_scope.head_candidate,
          v_scope.head_contract,v_scope.head_week)=(v_root,v_scope.version,v_scope.booking_id,
          v_scope.candidate_id,v_scope.contract_id,v_scope.week_ending_date)
        and v_scope.authority_kind=(case when v_scope.source_kind='PROTECTED' then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end))) then
    v_authority:='CURRENT';
  end if;
  v_reason:=case when v_scope.source_kind='PROTECTED' and v_scope.source_head_id is null
    then 'APPROVAL_UUID_ORIGIN_NOT_YET_QUALIFIED' when v_state='PENDING' then 'CURRENT_ORIGIN_PENDING'
    when v_authority<>'CURRENT' then 'CURRENT_ORIGIN_UNAVAILABLE' else null end;
  v_membership_complete:=v_authority='CURRENT' and v_state='APPLIED';
  if v_kind='WORKER_TRANSFERS' then
    select w.*,r.status as run_status,r.created_at_utc,r.preparation_expires_at_utc,r.confirmed_at_utc
      into v_worker from private.bpay_next_run_worker w join private.bpay_next_pay_run r on r.id=w.run_id
      where w.id=v_context and w.candidate_id=v_scope.candidate_id
        and (exists(select 1 from private.bpay_next_run_work rw where rw.run_worker_id=w.id and rw.work_id=v_work
          and rw.candidate_id=w.candidate_id)
          or (r.selection_state='SEALED' and exists(select 1 from private.bpay_next_run_selection s
            where s.run_id=r.id and s.work_id=v_work and s.candidate_id=w.candidate_id)));
    if not found then raise exception using errcode='42501',message='BPAY_NEXT_SOURCE_PAID_SCOPE_FORBIDDEN';end if;
    if not pg_catalog.isfinite(v_worker.created_at_utc)
      or (v_worker.preparation_expires_at_utc is not null and not pg_catalog.isfinite(v_worker.preparation_expires_at_utc))
      or (v_worker.confirmed_at_utc is not null and not pg_catalog.isfinite(v_worker.confirmed_at_utc))
      or v_worker.run_status not in ('PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE')
      or v_worker.status not in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE') then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_PAID_FINANCIAL_TUPLE_INVALID';
    end if;
    v_context_value:=pg_catalog.jsonb_build_object('run_id',v_worker.run_id,'run_worker_id',v_context,
      'run_status',v_worker.run_status,'worker_status',v_worker.status,
      'preparation_created_at_utc',pg_catalog.to_char(v_worker.created_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'preparation_deadline_utc',pg_catalog.to_char(v_worker.preparation_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'confirmed_at_utc',pg_catalog.to_char(v_worker.confirmed_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'captured_line_count',v_worker.captured_line_count::text,'captured_position_count',v_worker.captured_position_count::text,
      'realised_effect_count',v_worker.realised_effect_count::text,'active_case_hold_count',v_worker.active_case_hold_count::text);
  end if;
  v_header:=pg_catalog.jsonb_build_object('root_timesheet_id',v_root,'work_id',v_work,'expected_revision_id',v_revision,
    'module_epoch',v_scope.owner_epoch::text,'current_origin_state',v_state,
    'financial_view_revision',v_scope.financial_view_revision::text,'scope','EXACT_SOURCE_WORK',
    'quantity_authority_scope',v_authority,'context',v_context_value);

  if v_kind='COMPONENTS' then
    -- THREE independently seekable raw ranges. UNION/sort touches <=3*(limit+1)
    -- keys only; collection keys include fully-recovered retired zero positions.
    v_query:=$query$with current_keys as materialized (
      select p.component_key from private.bpay_next_position p
      where p.work_id=$1 and p.applied_revision_id=$2$query$;
    if v_after_key is not null then v_query:=v_query||' and p.component_key>$3';end if;
    v_query:=v_query||$query$ order by p.component_key limit $4
    ), relevant_keys as materialized (
      select p.component_key from private.bpay_next_position p where p.work_id=$1
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0)$query$;
    if v_after_key is not null then v_query:=v_query||' and p.component_key>$3';end if;
    v_query:=v_query||$query$ order by p.component_key limit $4
    ), collection_keys as materialized (
      select b.component_key from private.bpay_next_work_collection b where b.work_id=$1$query$;
    if v_after_key is not null then v_query:=v_query||' and b.component_key>$3';end if;
    v_query:=v_query||$query$ order by b.component_key limit $4
    ), raw as materialized (
      select component_key from (select * from current_keys union select * from relevant_keys
        union select * from collection_keys) bounded_keys order by component_key limit $4
    ), facts as materialized (
      select p.*,b.id as collection_id,b.case_id,b.case_component_id,b.paid_basis_qualified,
        b.qualification_state as collection_state,b.observed_revision_id,b.observed_approved_source_ex_vat,
        b.observed_realised_source_ex_vat,b.original_source_pay_channel,b.original_tax_treatment,
        b.original_effect_id,b.original_transfer_id,b.original_revision_id,b.original_approved_line_id,
        b.original_run_line_id,b.original_valuation_policy_id,
        r.source_kind as applied_source_kind,r.source_head_id as applied_head,r.source_event_id as applied_event,
        r.sealed_at_utc as applied_seal,r.source_pay_channel as applied_channel,r.detail_kind,
        r.certified_zero,r.expected_line_count,r.approved_source_ex_vat as revision_total,
        al.id as applied_line_id,al.component_kind as applied_kind,al.tax_treatment as applied_tax,
        al.source_component_id as applied_component,al.source_pay_ex_vat as line_approved,
        o.source_kind as original_source_kind,o.source_head_id as original_head,o.source_event_id as original_event,
        o.physical_timesheet_id as original_root,o.physical_timesheet_version as original_version,
        o.week_ending_date as original_week,o.source_inventory_digest as original_inventory,
        ot.timesheet_id as original_physical_row,ot.version as original_physical_version,
        ot.booking_id as original_physical_booking,ot.contract_id as original_physical_contract,
        ot.week_ending_date as original_physical_week,oc.candidate_id as original_physical_candidate,
        o.source_pay_channel as original_channel,o.sealed_at_utc as original_seal,o.detail_kind as original_detail,
        oa.id as original_line_id,oa.component_kind as original_kind,oa.tax_treatment as original_tax,
        oa.source_component_id as original_component,oa.approved_quantity as original_quantity,
        e.id as effect_id,e.effect_kind,e.operation_id,e.operation_item_id,e.candidate_id as effect_candidate,
        e.work_id as effect_work,e.component_key as effect_key,e.original_timesheet_id as effect_root,
        e.original_transfer_id as effect_transfer,e.source_disposition_ex_vat as positive_source,
        e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat,
        l.id as line_id,l.work_id as line_work,l.component_key as line_key,l.run_worker_id,l.captured_revision_id,l.approved_line_id,l.source_consumed_ex_vat,
        l.source_pay_channel,l.target_pay_channel,l.valuation_policy_id,l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat,
        lh.id as original_hold_id,lh.status as original_hold_status,lh.source_reserved_ex_vat,
        lh.target_amount_ex_vat as held_target_ex,lh.target_amount_vat as original_hold_target_vat,
        lh.target_amount_inc_vat as held_target_inc,
        t.id as transfer_id,t.transfer_no,t.run_worker_id as transfer_worker,t.candidate_id as transfer_candidate,
        t.execution_kind,t.status as transfer_status,t.original_transfer_id as parent_transfer,t.return_cash_id as parent_cash,
        t.beneficiary_kind,t.beneficiary_id,t.cash_amount,t.member_count,
        tm.subject_kind as member_kind,tm.signed_cash_contribution,tm.run_worker_id as member_worker,
        req.command_id as settlement_command,req.run_worker_id as settlement_worker,req.candidate_id as settlement_candidate,
        req.posted_member_count,req.posting_complete as settlement_complete,req.outcome_id as settlement_outcome_id,
        outc.id as actual_settlement_outcome,outc.outcome_kind as settlement_kind,outc.whole_transfer_amount as settlement_amount,
        ip.status as original_publication_status,ip.phase as original_publication_phase,
        ip.applied_at_utc as original_applied_at,ip.cursor_key as original_publication_cursor,
        ip.candidate_id as original_publication_candidate,ip.revision_no as publication_revision_no,
        o.revision_no as original_revision_no,ic.status as original_command_status,
        ic.command_kind as original_command_kind,ic.expected_member_count as original_expected_members,
        ic.enrolled_member_count as original_enrolled_members,ic.module_epoch as original_command_epoch,
        ic.agency_sequence as original_command_sequence,cm.member_no as original_member_no,
        j.id as original_position_job,j.status as original_job_status,j.phase as original_job_phase,
        j.cursor_key as original_job_cursor,j.module_epoch as original_job_epoch,j.command_sequence as original_job_sequence,
        j.lease_nonce as original_job_nonce,j.lease_until_utc as original_job_until,
        ih.id as original_head_id,ih.inventory_digest as original_head_inventory,
        ih.root_timesheet_id as original_head_root,ih.root_timesheet_version as original_head_version,
        ih.root_family_booking_id as original_head_booking,ih.candidate_id as original_head_candidate,
        ih.contract_id as original_head_contract,ih.week_ending_date as original_head_week,
        ih.authority_kind as original_authority,ih.state as original_head_state,
        ia.id as original_auth_id,ia.root_timesheet_id as original_auth_root,
        ia.timesheet_version as original_auth_version,ia.family_booking_id as original_auth_booking,
        tf.id as original_snapshot,tf.timesheet_id as snapshot_root,tf.timesheet_version as snapshot_version,
        tf.candidate_id as snapshot_candidate,tf.authorised_at_utc as snapshot_authorised,
        c.id as owned_case,c.candidate_id as case_candidate,c.case_kind,c.tax_treatment as case_tax,
        c.principal_funded,c.principal_approved,c.principal_recovered,c.principal_written_off,
        c.active_recovery_hold_amount,c.current_rule_id,c.current_rule_revision,
        x.id as owned_component,x.candidate_id as component_candidate,x.case_id as component_case,
        x.case_kind as component_case_kind,x.case_subtype,x.tax_treatment as component_tax,
        x.source_pay_channel as component_channel,x.instruction_kind,x.direction,x.payroll_stage,
        x.component_key as recovery_key,x.component_ordinal,x.approved_source_ex_vat as debt_approved,
        x.recovered_source_ex_vat as actual_recovered,x.written_off_source_ex_vat as actual_written_off,
        x.active_recovery_source_ex_vat as recovery_held,rule.id as rule_id,rule.rule_revision,rule.weekly_due_source_ex_vat,
        rr.command_id as return_command,rr.run_worker_id as return_worker,rr.candidate_id as return_candidate,
        rr.posting_complete as return_complete,ro.outcome_kind as return_kind,ro.whole_transfer_amount as return_amount,
        cash.id as cash_id,cash.candidate_id as cash_candidate,cash.amount_owed,cash.amount_held,cash.amount_reissued_paid,
        re.command_id as pending_reissue,re.run_worker_id as reissue_worker,re.candidate_id as reissue_candidate
      from raw k join private.bpay_next_position p on p.work_id=$1 and p.component_key=k.component_key
      join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=p.work_id
      left join private.bpay_next_approved_line al on al.revision_id=r.id and al.component_key=p.component_key
      left join private.bpay_next_work_collection b on b.work_id=p.work_id and b.component_key=p.component_key
      left join private.bpay_next_work_revision o on o.id=b.original_revision_id and o.work_id=p.work_id
      -- Original money is bound to its captured physical revision, not today's
      -- permission root. Neither join requires that original row to be current.
      left join public.timesheets ot on ot.timesheet_id=o.physical_timesheet_id
      left join public.contracts oc on oc.id=ot.contract_id
      left join private.bpay_next_approved_line oa on oa.id=b.original_approved_line_id and oa.revision_id=o.id
      left join private.bpay_next_financial_effect e on e.id=b.original_effect_id
      left join private.bpay_next_run_line l on l.id=b.original_run_line_id
      left join private.bpay_next_hold lh on lh.id=e.operation_item_id and lh.run_line_id=l.id
      left join private.bpay_next_transfer t on t.id=b.original_transfer_id
      left join private.bpay_next_transfer_member tm on tm.transfer_id=t.id and tm.run_line_id=l.id and tm.subject_kind='WORK'
      left join private.bpay_next_outcome_request req on req.transfer_id=t.id
      left join private.bpay_next_transfer_outcome outc on outc.id=req.outcome_id and outc.transfer_id=t.id
      left join private.bpay_next_publication ip on ip.work_id=p.work_id and ip.revision_id=o.id
      left join private.bpay_next_command ic on ic.id=ip.command_id
      left join private.bpay_next_command_member cm on cm.command_id=ic.id and cm.candidate_id=$5
      left join private.bpay_next_job j on j.command_id=ic.id and j.candidate_id=$5 and j.job_kind='POSITION_APPLY'
      left join public.weekly_source_entitlement_heads ih on ih.id=o.source_head_id
      left join public.weekly_source_root_authorisations ia on ia.id=o.source_event_id and o.source_head_id is null
      left join public.timesheets_financials tf on tf.id=o.financial_snapshot_id and o.source_head_id is null
      left join private.bpay_next_finance_case c on c.id=b.case_id
      left join private.bpay_next_case_component x on x.id=b.case_component_id
      left join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id
      left join private.bpay_next_return_request rr on rr.transfer_id=t.id
      left join private.bpay_next_transfer_outcome ro on ro.id=rr.outcome_id and ro.transfer_id=t.id
      left join private.bpay_next_return_cash cash on cash.original_transfer_id=t.id
      left join private.bpay_next_reissue_request re on re.return_cash_id=cash.id and re.transfer_id is null
    ), qualified as (
      select f.*,
        -- Known foreign/missing original tuples are damage, not numeric zero.
        (collection_id is not null and (effect_id is null or line_id is null or original_line_id is null
          or transfer_id is null or original_hold_id is null or member_kind is null or settlement_command is null
          or (effect_work,effect_key,effect_candidate,effect_transfer)
              is distinct from ($1,component_key,$5,original_transfer_id)
          or effect_root is distinct from $6
          or (line_work,line_key) is distinct from ($1,component_key)
          or (captured_revision_id,approved_line_id,source_pay_channel,valuation_policy_id)
              is distinct from (original_revision_id,original_approved_line_id,original_source_pay_channel,original_valuation_policy_id)
          or (source_consumed_ex_vat,frozen_ex_vat,frozen_vat,frozen_inc_vat)
              is distinct from (positive_source,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          or (transfer_worker,transfer_candidate,member_worker,settlement_worker,settlement_candidate)
              is distinct from (run_worker_id,$5,run_worker_id,run_worker_id,$5)
          or effect_kind<>'PAYROLL_SETTLED' or positive_source<=0 or parent_transfer is not null or parent_cash is not null
          or original_hold_status<>'REALISED' or source_reserved_ex_vat<>positive_source
          or (held_target_ex,original_hold_target_vat,held_target_inc) is distinct from (frozen_ex_vat,frozen_vat,frozen_inc_vat)
          or signed_cash_contribution<>frozen_inc_vat or operation_id<>settlement_command
          or posted_member_count>member_count or (settlement_complete and posted_member_count<>member_count)
          or (execution_kind='BANK' and (settlement_outcome_id is null or actual_settlement_outcome is null
            or settlement_kind is distinct from 'SETTLED' or settlement_amount is distinct from cash_amount))
          or (case_id is null)<>(case_component_id is null)
          or (case_id is not null and (owned_case is null or owned_component is null or rule_id is null
            or (case_candidate,component_candidate,component_case) is distinct from ($5,$5,case_id)
            or (principal_approved,principal_recovered,principal_written_off,active_recovery_hold_amount)
              is distinct from (debt_approved,actual_recovered,actual_written_off,recovery_held)
            or case_id<>collection_id or case_component_id<>collection_id)))) as damaged,
        case when collection_id is null then null when case_id is null and case_component_id is null then 0
          else actual_recovered end as recovered,
        case when collection_id is null then null when case_id is null and case_component_id is null then 0
          else actual_written_off end as written_off,
        case when collection_id is null then null when case_id is null and case_component_id is null then 0
          else recovery_held end as case_hold,
        case when coalesce(original_kind,applied_kind)='WORK'
          and coalesce(original_component,applied_component) is not null
          and component_key='SOURCE:'||coalesce(original_component,applied_component)::text then true
          when coalesce(original_kind,applied_kind) in ('EXPENSE','ADDITIONAL') then false else null end as quantity_subject,
        (collection_id is not null and original_kind='WORK' and original_tax='TAXABLE'
          and original_component is not null and component_key='SOURCE:'||original_component::text
          and original_quantity is not null and original_quantity>0
          and original_channel='PAYE' and source_pay_channel='PAYE' and target_pay_channel='PAYE'
          and original_seal is not null and pg_catalog.isfinite(original_seal)
          and pg_catalog.octet_length(original_inventory)=32
          and original_week=$7
          and (original_physical_row,original_physical_version,original_physical_booking,
            original_physical_contract,original_physical_candidate,original_physical_week)
            is not distinct from (original_root,original_version,$8,$9,$5,$7)
          and ((original_source_kind='SOURCE' and original_head is null
            and original_auth_id=original_event and original_auth_root=original_root
            and original_auth_version=original_version and original_auth_booking=$8
            and original_snapshot is not null and snapshot_root=original_root and snapshot_version=original_version
            and snapshot_candidate=$5 and snapshot_authorised is not null)
            or (original_source_kind in ('SOURCE','PROTECTED') and original_head=original_event
              and original_head_id=original_event and original_head_root=original_root
              and original_head_version=original_version and original_head_booking=$8
              and original_head_candidate=$5 and original_head_contract=$9 and original_head_week=$7
              and original_head_state in ('COMMITTED_CURRENT','SUPERSEDED')
              and original_head_inventory=original_inventory
              and original_authority=(case when original_source_kind='PROTECTED' then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end)))
          and original_publication_status='APPLIED' and original_publication_phase='DONE'
          and original_publication_cursor is null and original_publication_candidate=$5
          and publication_revision_no=original_revision_no
          and original_applied_at is not null and original_command_status='COMPLETE'
          and original_command_kind='POSITION_APPLY' and original_expected_members=1 and original_enrolled_members=1
          and original_member_no=1 and original_position_job is not null
          and original_job_status='DONE' and original_job_phase='DONE' and original_job_cursor is null
          and original_job_epoch=original_command_epoch and original_job_sequence=original_command_sequence
          and original_job_nonce is null and original_job_until is null) as original_qualified,
        (paid_basis_qualified and collection_state='READY' and original_source_pay_channel='PAYE'
          and original_tax_treatment='TAXABLE' and source_basis_channel='PAYE'
          and (observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat)
            is not distinct from (applied_revision_id,approved_source_ex_vat,realised_source_ex_vat)
          and approved_source_ex_vat>=0 and realised_source_ex_vat>=0
          and (realised_target_ex_vat,realised_target_vat,realised_target_inc_vat)
            is not distinct from (realised_source_ex_vat,0::numeric,realised_source_ex_vat)
          and applied_channel='PAYE' and applied_seal is not null
          and ((applied_source_kind='SOURCE' and applied_event is not null
              and (applied_head is null or applied_head=applied_event))
            or (applied_source_kind='PROTECTED' and applied_head is not null and applied_head=applied_event))
          and detail_kind=original_detail
          and ((applied_kind='WORK' and applied_tax='TAXABLE' and applied_component=original_component
              and line_approved=approved_source_ex_vat)
            or (certified_zero and revision_total=0 and expected_line_count=0 and approved_source_ex_vat=0))
          and ((case_id is null and case_component_id is null)
            or (case_kind='OVERPAYMENT' and case_tax='TAXABLE' and principal_funded=0
              and component_case_kind='OVERPAYMENT' and case_subtype='OVERPAYMENT' and component_tax='TAXABLE'
              and component_channel='PAYE' and instruction_kind='RECOVERY' and direction='DEDUCTION'
              and payroll_stage='GROSS_DEDUCT' and recovery_key='RECOVERY' and component_ordinal=1
              and current_rule_id=rule_id and current_rule_revision=rule_revision
              and rule_id=collection_id and weekly_due_source_ex_vat is null))) as basis_qualified,
        (transfer_status='RETURNED' and return_command is null)
          or (return_command is not null and (transfer_status<>'RETURNED'
          or (return_worker,return_candidate) is distinct from (transfer_worker,$5)
          or return_kind is distinct from 'RETURNED' or return_amount is distinct from cash_amount
          or return_complete is distinct from (cash_id is not null)))
          or (cash_id is not null and (cash_candidate<>$5 or amount_owed<>cash_amount))
          or (pending_reissue is not null and (reissue_worker,reissue_candidate) is distinct from (transfer_worker,$5))
          or (cash_id is not null and return_command is null) as cash_damaged
      from facts f
    ), classified as (
      select q.*,
        (original_qualified and basis_qualified and realised_source_ex_vat+recovered=positive_source) as only_original,
        case when collection_id is null then 'NO_QUALIFIED_ORIGINAL'
          when original_qualified is not true or basis_qualified is not true then 'QUANTITY_MAPPING_UNSUPPORTED'
          when realised_source_ex_vat+recovered<>positive_source then 'ADDITIONAL_POSITIVE_PAYROLL'
          when execution_kind<>'BANK' or beneficiary_kind<>'CANDIDATE' or beneficiary_id<>$5 or cash_amount<=0
            then 'QUANTITY_MAPPING_UNSUPPORTED'
          when settlement_complete is not true or posted_member_count<>member_count then 'ORIGINAL_POSTING_PENDING'
          when settlement_kind is distinct from 'SETTLED' or settlement_amount is distinct from cash_amount then 'ORIGINAL_POSTING_PENDING'
          when return_complete is not true or cash_id is null then 'RETURN_POSTING_PENDING'
          when amount_reissued_paid>0 then 'CASH_ALREADY_REPAID'
          when amount_held>0 or pending_reissue is not null then 'REISSUE_PENDING_OR_HELD'
          else null end as reason,
        case when return_command is null then null when not return_complete then 'RETURN_POSTING_PENDING'
          when beneficiary_kind<>'CANDIDATE' or beneficiary_id<>$5 then 'UNSUPPORTED_BENEFICIARY'
          when pending_reissue is not null then 'REISSUE_REQUESTED'
          when amount_owed-amount_held-amount_reissued_paid>0 then 'CASH_AVAILABLE'
          when amount_held>0 then 'CASH_RESERVED' else 'CASH_REPAID' end as action_state
      from qualified q
    ) select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'cursor',pg_catalog.jsonb_build_object('after_component_key',component_key),
      'damaged',coalesce(damaged,false) or coalesce(cash_damaged,false),
      'row',pg_catalog.jsonb_build_object('component_key',component_key,
        'source_component_id',coalesce(original_component,applied_component),'detail_kind',detail_kind,
        'quantity_subject',quantity_subject,'applied_revision_id',applied_revision_id,'collection_id',collection_id,
        'qualification_state',case when collection_id is null then 'NO_QUALIFIED_ORIGINAL'
          when original_qualified is true and basis_qualified is true then 'READY' else 'UNBOUND' end,
        'approved_source_ex_vat',approved_source_ex_vat::text,'realised_source_disposition_ex_vat',realised_source_ex_vat::text,
        'active_source_hold_ex_vat',held_source_ex_vat::text,'actual_recovered_source_ex_vat',recovered::text,
        'written_off_source_ex_vat',written_off::text,'active_recovery_hold_source_ex_vat',case_hold::text,
        'original_effect_id',original_effect_id,'original_transfer_id',original_transfer_id,
        'original_run_worker_id',run_worker_id,'original_positive_source_ex_vat',positive_source::text,
        'only_original_positive_payroll',case when original_qualified is true and basis_qualified is true then only_original end,
        'paid_quantity_certificate',case when quantity_subject is true and reason is null then 'ORIGINAL_RETURN_ZERO' else 'POSITION_WITHHELD' end,
        'paid_quantity',case when quantity_subject is true and reason is null then '0' end,'reason',reason,
        'original_cash',case when original_qualified is true then pg_catalog.jsonb_build_object(
          'transfer_id',transfer_id,'transfer_no',transfer_no::text,'scope','WHOLE_ORIGINAL_TRANSFER',
          'original_cash_amount',cash_amount::text,'original_settlement_posting_complete',settlement_complete,
          'original_return_command_id',return_command,'return_posting_complete',return_complete,'return_cash_id',cash_id,
          'cash_owed',amount_owed::text,'cash_held',amount_held::text,'cash_repaid',amount_reissued_paid::text,
          'cash_available',(amount_owed-amount_held-amount_reissued_paid)::text,
          'pending_reissue_command_id',pending_reissue,'action_state',action_state) end)
      ) order by component_key),'[]'::jsonb) from classified$query$;
    execute v_query into v_window using v_work,v_revision,v_after_key,v_limit+1,
      v_scope.candidate_id,v_scope.work_original_root,v_scope.week_ending_date,v_scope.booking_id,v_scope.contract_id;
    v_scope_name:='COMPONENT_ORIGINALS';
  elsif v_kind='ACTIVE_HOLDS' then
    v_query:=$query$with raw as materialized (
      select h.* from private.bpay_next_hold h where h.work_id=$1 and h.status='ACTIVE'$query$;
    if v_after_key is not null then v_query:=v_query||' and (h.component_key,h.id)>($2,$3)';end if;
    v_query:=v_query||$query$ order by h.component_key,h.id limit $4
    ) select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'cursor',pg_catalog.jsonb_build_object('after_component_key',h.component_key,'after_hold_id',h.id),
      'damaged',l.id is null or w.id is null or r.id is null or wr.id is null or a.id is null
        or w.candidate_id<>$5 or l.work_id<>$1 or wr.work_id<>$1
        or h.component_key is distinct from l.component_key or a.component_key is distinct from l.component_key
        or a.revision_id is distinct from l.captured_revision_id
        or r.status not in ('PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE')
        or w.status not in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE')
        or not pg_catalog.isfinite(r.created_at_utc)
        or (r.preparation_expires_at_utc is not null and not pg_catalog.isfinite(r.preparation_expires_at_utc))
        or (r.confirmed_at_utc is not null and not pg_catalog.isfinite(r.confirmed_at_utc)),
      'row',pg_catalog.jsonb_build_object('hold_id',h.id,'component_key',h.component_key,'run_id',r.id,
        'run_worker_id',w.id,'captured_revision_id',wr.id,
        'quantity_subject',case when a.component_kind='WORK' and a.source_component_id is not null then true
          when a.component_kind in ('EXPENSE','ADDITIONAL') then false end,
        'source_held_ex_vat',h.source_reserved_ex_vat::text,'target_held_ex_vat',h.target_amount_ex_vat::text,
        'target_held_vat',h.target_amount_vat::text,'target_held_inc_vat',h.target_amount_inc_vat::text,
        'run_status',r.status,'worker_status',w.status,
        'preparation_created_at_utc',pg_catalog.to_char(r.created_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'preparation_deadline_utc',pg_catalog.to_char(r.preparation_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'confirmed_at_utc',pg_catalog.to_char(r.confirmed_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'captured_line_count',w.captured_line_count::text,'active_case_hold_count',w.active_case_hold_count::text)
      ) order by h.component_key,h.id),'[]'::jsonb)
    from raw h left join private.bpay_next_run_line l on l.id=h.run_line_id
    left join private.bpay_next_work_revision wr on wr.id=l.captured_revision_id
    left join private.bpay_next_approved_line a on a.id=l.approved_line_id and a.revision_id=wr.id
    left join private.bpay_next_run_worker w on w.id=l.run_worker_id
    left join private.bpay_next_pay_run r on r.id=w.run_id$query$;
    execute v_query into v_window using v_work,v_after_key,v_after_id,v_limit+1,v_scope.candidate_id;
    v_scope_name:='ACTIVE_HOLDS_ONLY';
  else
    v_query:=$query$with raw as materialized (
      select t.id,t.transfer_no,t.original_transfer_id,t.return_cash_id,t.execution_kind,t.status,t.cash_amount,
        t.candidate_id,t.run_worker_id,t.projection_id,
        (t.account_approval_ref is null and t.destination_rail is null and t.beneficiary_name_snapshot is null
          and t.sort_code_snapshot is null and t.account_number_snapshot is null and t.bank_details_hash_snapshot is null)
          as internal_bank_fields_null
        from private.bpay_next_transfer t where t.run_worker_id=$1$query$;
    if v_after_no is not null then v_query:=v_query||' and t.transfer_no>$2';end if;
    v_query:=v_query||$query$ order by t.transfer_no limit $3
    ) select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'cursor',pg_catalog.jsonb_build_object('after_transfer_no',t.transfer_no::text),
      'damaged',t.candidate_id<>$4
        or t.status not in ('BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN','SETTLED',
          'RETURNED','REFUSED','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED')
        or not ((t.execution_kind='BANK' and t.cash_amount>0 and t.status not in ('INTERNAL_PROCESSING','INTERNAL_SETTLED')
            and (t.original_transfer_id is null)=(t.projection_id is not null)
            and (t.original_transfer_id is not null)=(t.return_cash_id is not null))
          or (t.execution_kind='INTERNAL_ZERO' and t.cash_amount=0 and t.projection_id is not null
            and t.original_transfer_id is null and t.return_cash_id is null and t.internal_bank_fields_null
            and t.status in ('BUILDING','MEMBERS_READY','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED')))
        or (cash.id is not null and cash.candidate_id<>$4)
        or (re.command_id is not null and (re.run_worker_id,re.candidate_id) is distinct from (t.run_worker_id,$4))
        or (t.return_cash_id is not null and (cash.id,cash.original_transfer_id) is distinct from (t.return_cash_id,t.original_transfer_id)),
      'row',pg_catalog.jsonb_build_object('transfer_id',t.id,'transfer_no',t.transfer_no::text,
        'original_transfer_id',t.original_transfer_id,'return_cash_id',t.return_cash_id,'execution_kind',t.execution_kind,
        'status',t.status,'cash_scope','WHOLE_WORKER_LEG','cash_amount',t.cash_amount::text,
        'outcome_posting_complete',o.posting_complete,'return_posting_complete',rr.posting_complete,
        'internal_posting_complete',case when ir.id is not null then o.posting_complete end,
        'reissue_action_state',case when cash.id is null then
            case when rr.command_id is not null and not rr.posting_complete then 'RETURN_POSTING_PENDING' end
          when original.beneficiary_kind<>'CANDIDATE' or original.beneficiary_id<>$4 then 'UNSUPPORTED_BENEFICIARY'
          when re.command_id is not null then 'REISSUE_REQUESTED'
          when cash.amount_owed-cash.amount_held-cash.amount_reissued_paid>0 then 'CASH_AVAILABLE'
          when cash.amount_held>0 then 'CASH_RESERVED' else 'CASH_REPAID' end)
      ) order by t.transfer_no),'[]'::jsonb)
    from raw t left join private.bpay_next_outcome_request o on o.transfer_id=t.id
    left join private.bpay_next_return_request rr on rr.transfer_id=t.id
    left join private.bpay_next_internal_receipt ir on ir.id=o.internal_receipt_id and ir.transfer_id=t.id
    left join private.bpay_next_transfer original on original.id=coalesce(t.original_transfer_id,t.id)
    left join private.bpay_next_return_cash cash on cash.original_transfer_id=original.id
    left join private.bpay_next_reissue_request re on re.return_cash_id=cash.id and re.transfer_id is null$query$;
    execute v_query into v_window using v_context,v_after_no,v_limit+1,v_scope.candidate_id;
    v_scope_name:='EXACT_RUN_WORKER';
  end if;
  -- Bounded materialisation only. The single query above sees one statement
  -- snapshot, including case C/W and original/return/reissue facts. STABLE
  -- subordinate scope/I3 reads share the caller's snapshot; no VOLATILE read.
  v_bytes:=2048+pg_catalog.octet_length(v_header::text);
  for v_item in select value from pg_catalog.jsonb_array_elements(v_window) loop
    if v_count=v_limit then v_more:=true;exit;end if;
    if v_item->>'damaged'='true' then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_PAID_FINANCIAL_TUPLE_INVALID';
    end if;
    v_value:=v_item->'row';
    if v_bytes+pg_catalog.octet_length(v_value::text)+2>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_SOURCE_PAID_ROW_TOO_LARGE';end if;
      v_more:=true;exit; -- leave this raw key UNCONSUMED
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_value::text)+2;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_value);v_cursor:=v_item->'cursor';v_count:=v_count+1;
    if v_kind='COMPONENTS' then
      if v_value->'quantity_subject'='null'::jsonb then v_all_zero:=false;end if;
      if v_value->>'quantity_subject'='true' then
        if v_value->>'paid_quantity_certificate'<>'ORIGINAL_RETURN_ZERO' then v_all_zero:=false;end if;
        if v_value->>'paid_quantity_certificate'='ORIGINAL_RETURN_ZERO'
          and (v_value->>'original_positive_source_ex_vat')::numeric>0 then v_positive_witness:=true;end if;
      end if;
    end if;
  end loop;
  if not v_more then v_cursor:=null;end if;
  if v_kind='COMPONENTS' and v_membership_complete and v_after='null'::jsonb
    and not v_more and v_all_zero and v_positive_witness then
    v_certificate:=pg_catalog.jsonb_build_object('scope','EXACT_SOURCE_WORK','state','ROOT_RETURN_ZERO',
      'quantity','0','unit','HOURS','reason',null,'component_set_complete',true);
  else
    v_certificate:=pg_catalog.jsonb_build_object('scope','NONE','state','POSITION_WITHHELD','quantity',null,'unit','HOURS',
      'reason',coalesce(v_reason,case when v_kind<>'COMPONENTS' or v_more or v_after<>'null'::jsonb
        then 'COMPONENT_SET_NOT_ONE_SNAPSHOT' when not v_positive_witness then 'NO_QUALIFIED_ORIGINAL'
        else 'QUANTITY_MAPPING_UNSUPPORTED' end),
      'component_set_complete',v_kind='COMPONENTS' and v_membership_complete and v_after='null'::jsonb and not v_more);
  end if;
  v_coverage:=pg_catalog.jsonb_build_object('scope',v_scope_name,'complete_for_scope',not v_more,
    'page_row_count',v_count::text,'root_preparing_count',null,'root_frozen_draft_count',null,
    'root_inflight_count',null,'reason','ROOT_ACTIVITY_NOT_INDEXED');
  v_result:=pg_catalog.jsonb_build_object('version','SOURCE_PAID_EVIDENCE_V1','ok',true,'kind',v_kind,
    'header',v_header,'rows',v_rows,'complete',not v_more,'next_cursor',v_cursor,
    'quantity_certificate',v_certificate,'activity_coverage',v_coverage);
  if pg_catalog.octet_length(v_result::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_SOURCE_PAID_PAGE_TOO_LARGE';
  end if;
  return v_result;
end
$function$;
alter function public.bpay_next_source_paid_evidence_page_v1(jsonb) owner to current_user;
revoke all on function public.bpay_next_source_paid_evidence_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_source_paid_evidence_page_v1(jsonb) to service_role;

notify pgrst,'reload schema';
commit;
