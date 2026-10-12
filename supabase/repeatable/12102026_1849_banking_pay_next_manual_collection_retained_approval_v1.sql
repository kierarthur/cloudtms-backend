-- FC15 S06 retained WITHDRAWN native liability; current Source admission unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_work_collection_observation_v1(
  p_work_id uuid,p_component_key text,p_revision_id uuid
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_position private.bpay_next_position%rowtype;v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_original private.bpay_next_work_revision%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_current private.bpay_next_approved_line%rowtype;v_segments record;v_segmented boolean;
  v_retained boolean:=false;v_complete boolean:=false;v_outstanding numeric;v_free numeric;v_negative numeric;v_positive numeric;v_overhang numeric;
begin
  select pos.* into v_position from private.bpay_next_position pos where pos.work_id=p_work_id and pos.component_key=p_component_key;
  select wrk.* into v_work from private.bpay_next_work wrk where wrk.id=p_work_id;
  select rev.* into v_revision from private.bpay_next_work_revision rev where rev.id=p_revision_id and rev.work_id=p_work_id;
  select coll.* into v_basis from private.bpay_next_work_collection coll where coll.work_id=p_work_id and coll.component_key=p_component_key;
  -- Withdrawal retains a previously known sealed observation, never approval zero.
  v_retained:=v_work.approval_state='WITHDRAWN' and v_work.current_revision_id is null
    and v_work.applied_revision_id=p_revision_id and v_basis.observed_revision_id=p_revision_id
    and v_basis.observed_approved_source_ex_vat=v_position.approved_source_ex_vat
    and v_basis.collection_mode='MANUAL' and v_basis.manual_reason_code='AUTOMATIC_COLLECTION_UNAVAILABLE'
    and v_basis.manual_outstanding_source_ex_vat is not null;
  if v_basis.id is null or v_position.applied_revision_id is distinct from p_revision_id
    or not (coalesce(v_retained,false) or (v_work.current_revision_id=p_revision_id and v_work.approval_state='APPROVED'))
    or v_revision.sealed_at_utc is null or v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  select rev.* into strict v_original from private.bpay_next_work_revision rev where rev.id=v_basis.original_revision_id;
  select line.* into strict v_line from private.bpay_next_approved_line line where line.id=v_basis.original_approved_line_id;
  select line.* into v_current from private.bpay_next_approved_line line where line.revision_id=p_revision_id and line.component_key=p_component_key;
  if v_basis.case_id is not null then
    select fc.* into strict v_case from private.bpay_next_finance_case fc where fc.id=v_basis.case_id;
    select cc.* into strict v_component from private.bpay_next_case_component cc where cc.id=v_basis.case_component_id;
    if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
      is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
        v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
      return jsonb_build_object('complete',false,'segmented',false);end if;
  end if;
  if v_revision.source_pay_channel is distinct from v_basis.original_source_pay_channel
    or v_position.source_basis_channel is distinct from v_basis.original_source_pay_channel then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  -- The same typed authorised key, not an arbitrary absent DAY/aggregate.
  v_complete:=v_revision.detail_kind=v_original.detail_kind and v_current.id is not null
    and v_current.component_kind=v_line.component_kind
    and v_current.tax_treatment is not distinct from v_line.tax_treatment
    and v_current.source_component_id is not distinct from v_line.source_component_id
    and v_current.source_pay_ex_vat=v_position.approved_source_ex_vat;
  v_complete:=v_complete or (v_revision.certified_zero and v_revision.expected_line_count=0
    and v_revision.approved_source_ex_vat=0 and v_position.approved_source_ex_vat=0);
  -- A Source head must be the genuinely authorised committed book. An
  -- unrelated/unbound binding is retained as AMOUNT_UNBOUND, never zero.
  if not coalesce(v_retained,false) and v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists(
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=bv.decision_bundle_id
        and bundle.bundle_revision=bv.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=bv.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=bv.publication_command_id_1
        and publication.work_id=p_work_id and publication.revision_id=p_revision_id and publication.candidate_id=v_work.candidate_id
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and bv.approval_work_id=p_work_id and bv.approval_component_key=p_component_key
        and bv.approval_revision_id=p_revision_id and bv.candidate_id=v_work.candidate_id
        and bv.publication_command_id_2 is null and bv.head_id=v_revision.source_head_id
        and v_revision.source_event_id=bv.head_id and bv.approval_source_channel=v_revision.source_pay_channel
        and receipt.decision_bundle_id=bv.decision_bundle_id and receipt.bundle_revision=bv.bundle_revision
        and receipt.request_digest=bv.request_digest and cardinality(receipt.head_ids)=1 and receipt.head_ids[1]=bv.head_id
        and publication.status in ('QUEUED','APPLYING','APPLIED')) then v_complete:=false;end if;
  v_segmented:=exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=p_revision_id and a.component_key=p_component_key)
    or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=p_component_key);
  if v_segmented then
    -- Reuse native maintained segment balances. Original actual target totals
    -- must also reconcile; no gross-to-net ratio or history re-sum.
    select count(d.segment_key) n,bool_and(d.approved_source_ex_vat>=0 and d.realised_source_ex_vat>=0) nonnegative,
      sum(d.approved_source_ex_vat) approved,sum(d.realised_source_ex_vat) realised,sum(d.held_source_ex_vat) held,
      sum(d.written_off_source_ex_vat) written_off,sum(d.active_recovery_source_ex_vat) recovery_held,
      sum(credit.ex) realised_ex,sum(credit.vat) realised_vat,sum(credit.inc) realised_inc,
      sum(held.ex) held_ex,sum(held.vat) held_vat,sum(held.inc) held_inc,
      sum(d.outstanding_source_ex_vat) outstanding,sum(d.free_recovery_source_ex_vat) free,
      sum(d.negative_source_ex_vat) negative,
      sum(greatest(d.approved_source_ex_vat-d.realised_source_ex_vat-d.held_source_ex_vat,0)) positive,
      sum(greatest(d.held_source_ex_vat-greatest(d.approved_source_ex_vat-d.realised_source_ex_vat,0),0)) overhang
      into v_segments from private.bpay_next_work_segment_debt_basis_v1(p_work_id,p_component_key,p_revision_id) d
      cross join lateral (select coalesce(sum(e.target_ex_vat),0) ex,coalesce(sum(e.target_vat),0) vat,coalesce(sum(e.target_inc_vat),0) inc
        from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id and e.component_key=p_component_key and e.segment_key=d.segment_key) credit
      cross join lateral (select coalesce(sum(s.target_reserved_ex_vat),0) ex,coalesce(sum(s.target_reserved_vat),0) vat,coalesce(sum(s.target_reserved_inc_vat),0) inc
        from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
        where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=d.segment_key) held;
    v_complete:=v_complete and v_line.component_kind='WORK' and (v_revision.detail_kind='SHIFT' or v_revision.certified_zero)
      and v_segments.n>0 and v_segments.nonnegative
      and (v_segments.approved,v_segments.realised,v_segments.held,v_segments.realised_ex,v_segments.realised_vat,v_segments.realised_inc,
        v_segments.held_ex,v_segments.held_vat,v_segments.held_inc,v_segments.written_off,v_segments.recovery_held)
        is not distinct from (v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,v_position.held_source_ex_vat,
          v_position.realised_target_ex_vat,v_position.realised_target_vat,v_position.realised_target_inc_vat,
          v_position.held_target_ex_vat,v_position.held_target_vat,v_position.held_target_inc_vat,
          coalesce(v_case.principal_written_off,0),coalesce(v_case.active_recovery_hold_amount,0));
    v_outstanding:=v_segments.outstanding;v_free:=v_segments.free;v_negative:=v_segments.negative;v_positive:=v_segments.positive;v_overhang:=v_segments.overhang;
  else
    v_outstanding:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-coalesce(v_case.principal_written_off,0),
      coalesce(v_case.active_recovery_hold_amount,0),0);
    v_free:=v_outstanding-coalesce(v_case.active_recovery_hold_amount,0);
    v_negative:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0);
    v_positive:=greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat-v_position.held_source_ex_vat,0);
    v_overhang:=greatest(v_position.held_source_ex_vat-greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat,0),0);
  end if;
  return jsonb_build_object('complete',coalesce(v_complete,false),'segmented',v_segmented,
    'outstanding_source_ex_vat',case when v_complete then v_outstanding end,
    'free_recovery_source_ex_vat',case when v_complete then v_free end,
    'negative_source_ex_vat',case when v_complete then v_negative end,
    'positive_source_ex_vat',case when v_complete then v_positive end,
    'held_overhang_source_ex_vat',case when v_complete then v_overhang end);
end
$function$;

create or replace function private.bpay_next_workbench_refresh_work_core_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_member record;v_accepted record;v_totals record;v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;
  v_group text:='WORK:'||p_work_id::text;v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_sunday date;v_sequence bigint;v_reason text;v_priority smallint;v_section text;v_complete boolean;
  v_pay_hold record;v_hold_canonical boolean;
  v_query_gate jsonb;v_query_scope jsonb;v_query_loaded boolean:=false;v_query_state text:='UNAVAILABLE';v_query_exclusions jsonb;v_segment_query_state text;
  v_wait_before boolean;v_wait_after boolean;v_case_id uuid;v_withdrawn_complete boolean;v_was_dirty boolean;
  v_manual private.bpay_next_work_collection%rowtype;v_manual_observation jsonb;v_manual_position private.bpay_next_position%rowtype;v_original_line private.bpay_next_approved_line%rowtype;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  -- Exact current booking authority; no historical/original-row fallback.
  select t.timesheet_id,tf.id financial_id,coalesce(tf.pay_on_hold,false) pay_on_hold,
    case when tf.pay_on_hold then tf.pay_on_hold_reason end pay_on_hold_reason into v_pay_hold
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_hold_canonical:=v_pay_hold.timesheet_id is not null and v_pay_hold.financial_id is not null;
  -- Read stored WAIT before replacing it; the withdrawal owner already changed work.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait_before;
  v_wait_after:=private.bpay_next_work_wait_state_v1(v_work.id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Reauthorisation becomes effective only after final position application.
  -- Keep its stored WAIT marker during Processing for the final boundary hook.
  if v_wait_before and v_work.approval_state='APPROVED'
    and v_work.current_revision_id is distinct from v_work.applied_revision_id then
    update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
      dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
      updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
    update private.bpay_next_workbench_group set complete=false,dirty=true,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    update private.bpay_next_workbench_item set selectable=false,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    return;
  end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end
      and r.work_id=v_work.id;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  v_withdrawn_complete:=v_work.approval_state='WITHDRAWN' and v_revision.id is not null
    and v_revision.sealed_at_utc is not null and not exists(select 1 from private.bpay_next_position p
      where p.work_id=v_work.id and p.applied_revision_id is distinct from v_work.applied_revision_id
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0));
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count-case when v_was_dirty then 1 else 0 end,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_work.current_revision_id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,dirty=false,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  if v_work.approval_state='WITHDRAWN' then
    -- Native retained positions supply A/C/H directly. No zero revision, job,
    -- debt, current residual offer, segment repricing or Case mutation here.
    for v_member in select p.component_key,p.approved_source_ex_vat approved_amount,
      p.realised_source_ex_vat realised_amount,p.held_source_ex_vat held_amount,p.source_basis_channel,
      a.id approved_line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label
      from private.bpay_next_position p left join private.bpay_next_approved_line a
        on a.revision_id=v_revision.id and a.component_key=p.component_key
      where p.work_id=v_work.id and v_withdrawn_complete and (p.applied_revision_id=v_work.applied_revision_id
        or p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
        or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
        or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0) order by p.component_key loop
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
      v_item.component_key:=v_member.component_key;v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_member.component_key;
      v_item.portion_kind:='WITHDRAWN';v_item.direction:='INFORMATIONAL';v_item.payroll_stage:='GROSS_ADD';
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_member.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_member.component_kind;v_item.tax_treatment:=v_member.tax_treatment;
      v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
      v_item.source_pay_channel:=v_member.source_basis_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_member.approved_amount;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=v_member.held_amount;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.reason_priority:=case when v_wait_after then 70 else 20 end;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      insert into private.bpay_next_workbench_item select (v_item).*;
      select b.* into v_manual from private.bpay_next_work_collection b where b.work_id=v_work.id
        and b.component_key=v_member.component_key and b.collection_mode='MANUAL' and b.case_id is null
        and b.observed_revision_id=v_revision.id and b.manual_outstanding_source_ex_vat>0;
      if v_manual.id is not null and coalesce((private.bpay_next_work_collection_observation_v1(
          v_work.id,v_manual.component_key,v_revision.id)->>'complete')::boolean,false) then
        -- The native WITHDRAWN row above alone owns A/C/H and the WAIT marker.
        v_count:=v_count+1;v_item.id:=gen_random_uuid();v_item.portion_kind:='AVAILABLE';
        v_item.nominal_source_ex_vat:=v_manual.manual_outstanding_source_ex_vat;
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.active_reserved_source_ex_vat:=0;
        v_item.residual_source_ex_vat:=0;v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;
        v_item.excluded_source_ex_vat:=0;v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.written_off_source_ex_vat:=0;v_item.deduction_balance_source_ex_vat:=0;
        v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;
        v_item.payroll_stage:=case when v_member.tax_treatment='NON_TAXABLE' then 'NET_DEDUCT' else 'GROSS_DEDUCT' end;
        insert into private.bpay_next_workbench_item select (v_item).*;
      end if;
    end loop;
    update private.bpay_next_workbench_group set complete=v_count>0,certified_zero=false,
      progress_state=case when v_withdrawn_complete then 'CURRENT' when v_work.applied_revision_id is not null then 'PROCESSING' else 'UNAVAILABLE' end
      where group_key=v_group;
  else
  for v_member in select m.component_key,m.approved_line_id,m.approved_segment_id,m.segment_key,m.component_kind,m.tax_treatment,m.work_date,m.approved_quantity,m.unit_label,m.approved_amount,m.realised_amount,m.held_amount,m.source_pay_channel,m.parent_complete,m.segment_attribution_complete from private.bpay_next_workbench_work_members_v1(v_work.id,v_revision.id) m order by m.component_key,m.segment_key nulls first loop
    v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
    v_item.approved_segment_id:=v_member.approved_segment_id;v_item.segment_key:=v_member.segment_key;
    v_item.component_key:=v_member.component_key;v_item.member_kind:=case when v_member.segment_key is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end;
    v_item.member_key:=coalesce(v_member.segment_key,v_member.component_key);v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;
    v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;v_item.work_date:=v_member.work_date;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
    v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
    v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.component_kind:=v_member.component_kind;
    v_item.tax_treatment:=v_member.tax_treatment;v_item.source_pay_channel:=v_member.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:='PAYMENT';v_item.payroll_stage:='GROSS_ADD';v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
    v_item.amount_known:=coalesce(v_complete,false) and v_member.parent_complete and v_member.segment_attribution_complete;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_complete then 'CURRENT' else 'PROCESSING' end;v_item.updated_at_utc:=transaction_timestamp();
    v_reason:='READY';v_priority:=200;v_section:='READY';
    if v_revision.id is null or v_target is null or v_work.week_ending_date is null
      or extract(isodow from v_work.week_ending_date)<>7 then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif v_work.approval_state<>'APPROVED' then v_reason:='MISSING_APPROVAL';v_priority:=21;v_section:='BLOCKED';
    elsif not v_complete then v_reason:='APPLY_PENDING';v_priority:=30;v_section:='BLOCKED';
    elsif not v_member.parent_complete then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif not v_member.segment_attribution_complete then v_reason:='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE';v_priority:=12;v_section:='BLOCKED';end if;
    if v_item.amount_known then
      v_positive:=greatest(v_member.approved_amount-v_member.realised_amount-v_member.held_amount,0);
      v_recovery:=greatest(v_member.realised_amount-v_member.approved_amount,0);
      v_overhang:=greatest(v_member.held_amount-greatest(v_member.approved_amount-v_member.realised_amount,0),0);
      v_item.nominal_source_ex_vat:=v_positive;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=0;
      v_item.written_off_source_ex_vat:=0;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=v_recovery;v_item.held_overhang_source_ex_vat:=v_overhang;
    else v_positive:=null;v_recovery:=null;v_overhang:=null;end if;
    select a.run_id,a.command_id,a.module_epoch,a.selected_source_ex_vat into v_accepted from private.bpay_next_workbench_accepted_work_v1(v_work.id,v_member.component_key,v_member.segment_key) a;
    if found then
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.selected_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.selected_source_ex_vat is not null;
      if v_priority>50 then v_reason:='IN_PREPARATION';v_priority:=50;v_section:='WAITING';end if;
    end if;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(v_work.id,v_revision.id,v_member.segment_key,v_member.component_key,v_sequence,v_today);
    if v_snooze is not null then
      v_item.snooze_id:=(v_snooze->>'id')::uuid;v_item.snooze_version:=(v_snooze->>'version')::bigint;
      v_item.snooze_scope:=v_snooze->>'scope_kind';v_item.snooze_until_date:=(v_snooze->>'until_date')::date;
      if v_positive>0 then v_item.excluded_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work.id and s.active
      and (s.until_date is null or s.until_date>=v_today) and ((s.scope_kind='SEGMENT' and s.segment_key=v_member.segment_key)
        or s.scope_kind='WHOLE_TIMESHEET' or (s.scope_kind='EXPENSE_LINE' and s.component_key=v_member.component_key)
        or (s.scope_kind='EXPENSES_ALL' and v_member.component_kind in ('EXPENSE','MILEAGE')))) then
      v_item.stale_snooze_code:=case when v_member.segment_key is not null and v_member.approved_segment_id is null
        then 'SNOOZE_SEGMENT_STALE' else 'SNOOZE_BASIS_STALE' end;
    end if;
    if v_priority>100 and v_work.week_ending_date>v_sunday then
      v_reason:='FUTURE_WEEK';v_priority:=100;v_section:='WAITING';v_item.eligible_from_date:=v_work.week_ending_date-6;
    elsif v_priority>110 and v_snooze is not null and v_positive>0 then v_reason:='SNOOZED';v_priority:=110;v_section:='WAITING';end if;
    v_item.query_qualified:=coalesce(v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED')
      and v_member.component_kind in ('WORK','PROTECTED_WORK') and v_revision.source_event_id is not null
      and v_revision.physical_timesheet_id is not null and v_item.amount_known and v_item.included_source_ex_vat>0,false);
    if v_item.query_qualified then v_item.query_state:='UNAVAILABLE';end if;
    v_item.section:=v_section;v_item.reason_code:=v_reason;v_item.reason_priority:=v_priority;
    v_item.pay_hold:=coalesce(v_pay_hold.pay_on_hold,false);v_item.pay_hold_reason:=v_pay_hold.pay_on_hold_reason;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      -- WB40: only a new positive offer is blocked. The saved v_base keeps
      -- native H/R and accepted Draft selection untouched (owner E1/E3).
      if not v_item.is_selected and not v_hold_canonical and v_item.reason_priority>15 then
        v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=15;v_item.section:='BLOCKED';
      elsif not v_item.is_selected and v_item.pay_hold and v_item.reason_priority>40 then
        v_item.reason_code:='PAY_ON_HOLD';v_item.reason_priority:=40;v_item.section:='BLOCKED';
      end if;
      v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      -- WB-07: query classification belongs only to this new positive portion.
      -- The original pre-query v_base/v_priority remain unchanged for H/R.
      if v_item.query_qualified and v_item.portion_kind='AVAILABLE' then
        if not v_query_loaded then
          v_query_gate:=null;v_query_state:='UNAVAILABLE';
          begin
            v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
          exception when others then
            v_query_gate:=null;
          end;
          v_query_loaded:=true;
          -- Guard object types separately: malformed scalar replies cannot
          -- reach the object-minus-keys operators or escape the unavailable path.
          if jsonb_typeof(v_query_gate)='object' then
            if (v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
                and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
                and v_query_gate->'ok'='true'::jsonb and v_query_gate->>'code'='OK'
                and jsonb_typeof(v_query_gate->'code')='string'
                and jsonb_typeof(v_query_gate->'blocked')='boolean'
                and jsonb_typeof(v_query_gate->'query_state_sha256')='string'
                and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is true then
              v_query_scope:=v_query_gate->'scope';
              if jsonb_typeof(v_query_scope)='object' then
                if (v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
                    'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
                    and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
                      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
                    and v_query_scope->'root_timesheet_id'=to_jsonb(v_revision.physical_timesheet_id::text)
                    and v_query_scope->'family_booking_id'=to_jsonb(v_work.booking_id)
                    and v_query_scope->'candidate_id'=to_jsonb(v_work.candidate_id::text)
                    and v_query_scope->'client_id'=to_jsonb(v_client::text)
                    and v_query_scope->'contract_id'=to_jsonb(v_work.contract_id::text)
                    and v_query_scope->'week_ending_date'=to_jsonb(v_work.week_ending_date::text)
                    and v_query_scope->'root_version'=to_jsonb(v_revision.physical_timesheet_version::text)
                    and ((v_query_scope->'target_family_id'='null'::jsonb
                        and v_query_scope->'family_bound_version'='null'::jsonb)
                      or (jsonb_typeof(v_query_scope->'target_family_id')='string'
                        and v_query_scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and jsonb_typeof(v_query_scope->'family_bound_version')='string'
                        and v_query_scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
                        and (char_length(v_query_scope->>'family_bound_version')<19
                          or (v_query_scope->>'family_bound_version') collate "C"<='9223372036854775807' collate "C")))) is true then
                  v_query_state:=case when v_query_gate->'blocked'='true'::jsonb then 'OPEN' else 'CLEAR' end;
                  v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_revision.id,v_query_gate);
                end if;
              end if;
            end if;
          end if;
        end if;
        v_segment_query_state:=v_query_state;
        if v_query_state='OPEN' and v_query_exclusions is not null and v_member.segment_key is not null then
          v_segment_query_state:=case when exists(select 1 from jsonb_array_elements(v_query_exclusions) reason(value)
            where reason.value->>'segment_key'=v_member.segment_key) then 'OPEN' else 'CLEAR' end;
        end if;
        v_item.query_state:=v_segment_query_state;
        if v_segment_query_state='UNAVAILABLE' and v_item.reason_priority>60 then
          v_item.reason_code:='PAY_QUERY_UNAVAILABLE';v_item.reason_priority:=60;v_item.section:='BLOCKED';
        elsif v_segment_query_state='OPEN' and v_item.reason_priority>61 then
          v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION';
        end if;
        v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      select b.* into v_manual from private.bpay_next_work_collection b where b.work_id=v_work.id
        and b.component_key=v_member.component_key and b.collection_mode='MANUAL'
        and b.observed_revision_id=v_revision.id;
      if v_manual.id is not null then
        v_item.section:='ACTION';v_item.reason_priority:=81;
        -- This informational liability never borrows a frozen accepted offer.
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
        v_item.accepted_run_id:=null;v_item.accepted_command_id:=null;v_item.accepted_module_epoch:=null;
        v_item.reason_code:=case when v_manual.case_id is not null then 'MANUAL_COLLECTION_REFERENCE'
          when v_manual.manual_outstanding_source_ex_vat is null then 'MANUAL_COLLECTION_UNAVAILABLE' else 'MANUAL_COLLECTION' end;
        v_item.nominal_source_ex_vat:=case when v_manual.case_id is not null then 0 else v_recovery end;
        if v_item.component_kind is null then
          select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
          v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
        end if;
      elsif v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_item.amount_known and v_member.held_amount>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.active_reserved_source_ex_vat:=v_member.held_amount;v_item.nominal_source_ex_vat:=v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      if v_positive>0 or v_recovery>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_priority>111 then v_item.reason_code:='RESERVED';v_item.reason_priority:=111;v_item.section:='WAITING';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_base.amount_known and (v_positive>0 or v_recovery>0 or v_member.held_amount>0
      or (v_member.approved_amount=0 and v_member.realised_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null)) then
      select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
        into v_totals from private.bpay_next_workbench_item i where i.group_key=v_group
          and i.component_key=v_member.component_key and i.member_kind=v_base.member_kind and i.member_key=v_base.member_key;
      if (v_totals.a,v_totals.c,v_totals.h,v_totals.residual,v_totals.split_residual)
        is distinct from (v_member.approved_amount,v_member.realised_amount,v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_NATIVE_MEMBER_NOT_CONSERVED';end if;
    end if;
  end loop;
  end if;
  -- A known retained manual liability is independent of unavailable positive
  -- Source classification. Keep that Evidence; never certify a new offer.
  update private.bpay_next_workbench_item i set reason_code='MANUAL_COLLECTION_UNAVAILABLE'
    from private.bpay_next_work_collection b where i.group_key=v_group and i.work_id=b.work_id
      and i.component_key=b.component_key and i.portion_kind='EVIDENCE' and not i.amount_known
      and not i.is_selected and b.collection_mode='MANUAL' and b.case_id is null
      and b.observed_revision_id=v_revision.id and b.manual_outstanding_source_ex_vat is null;
  if v_work.approval_state='APPROVED' and v_work.current_revision_id=v_work.applied_revision_id then
    for v_manual in select b.* from private.bpay_next_work_collection b where b.work_id=v_work.id
      and b.collection_mode='MANUAL' and b.case_id is null and b.observed_revision_id=v_revision.id
      and b.manual_outstanding_source_ex_vat>0 and not exists(select 1 from private.bpay_next_workbench_item i
        where i.group_key=v_group and i.component_key=b.component_key and i.amount_known) order by b.component_key loop
      v_manual_observation:=private.bpay_next_work_collection_observation_v1(v_work.id,v_manual.component_key,v_revision.id);
      if not coalesce((v_manual_observation->>'complete')::boolean,false) then continue;end if;
      select pos.* into strict v_manual_position from private.bpay_next_position pos where pos.work_id=v_work.id and pos.component_key=v_manual.component_key;
      select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.component_key:=v_manual.component_key;
      v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_manual.component_key;v_item.portion_kind:='RECOVERY';
      v_item.direction:='INFORMATIONAL';v_item.payroll_stage:=case when v_original_line.tax_treatment='NON_TAXABLE' then 'NET_DEDUCT' else 'GROSS_DEDUCT' end;
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_original_line.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
      v_item.source_pay_channel:=v_manual.original_source_pay_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_manual.manual_outstanding_source_ex_vat;
      v_item.approved_source_ex_vat:=v_manual_position.approved_source_ex_vat;v_item.realised_source_ex_vat:=v_manual_position.realised_source_ex_vat;
      v_item.active_reserved_source_ex_vat:=v_manual_position.held_source_ex_vat;
      v_item.residual_source_ex_vat:=v_manual_position.approved_source_ex_vat-v_manual_position.realised_source_ex_vat-v_manual_position.held_source_ex_vat;
      v_item.gross_due_source_ex_vat:=(v_manual_observation->>'positive_source_ex_vat')::numeric;
      v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=v_item.gross_due_source_ex_vat;
      v_item.recovery_source_ex_vat:=(v_manual_observation->>'negative_source_ex_vat')::numeric;
      v_item.held_overhang_source_ex_vat:=(v_manual_observation->>'held_overhang_source_ex_vat')::numeric;v_item.written_off_source_ex_vat:=0;
      v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      if v_item.included_source_ex_vat+v_item.excluded_source_ex_vat-v_item.recovery_source_ex_vat-v_item.held_overhang_source_ex_vat
        is distinct from v_item.residual_source_ex_vat then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_MANUAL_DISPLAY_NOT_CONSERVED';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
  end if;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    if v_work.approval_state='WITHDRAWN' then
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
    end if;
    v_item.reason_priority:=case when v_work.approval_state='WITHDRAWN' then case when v_wait_after then 70 else 20 end
      when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_work.approval_state='WITHDRAWN' and v_work.applied_revision_id is not null
      and not v_withdrawn_complete then 'PROCESSING' else 'UNAVAILABLE' end;
    v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
    v_zero:=coalesce(v_complete,false) and v_revision.certified_zero and v_revision.approved_source_ex_vat=0
      and v_revision.expected_line_count=0 and v_target is not null and v_work.week_ending_date is not null
      and extract(isodow from v_work.week_ending_date)=7
      and not exists(select 1 from private.bpay_next_position p where p.work_id=v_work.id);
    if v_zero then
      v_item.section:='READY';v_item.reason_code:='CERTIFIED_ZERO';v_item.reason_priority:=200;v_item.progress_state:='CURRENT';
      if v_work.week_ending_date>v_sunday then
        v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_work.week_ending_date-6;
      end if;
    end if;
    insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=g.complete and x.unknown=0,certified_zero=coalesce(v_complete,false) and coalesce(v_revision.certified_zero,false)
    from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,
      count(*) filter(where not amount_known and not (member_kind='GROUP_EVIDENCE' and coalesce(v_zero,false))) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  update private.bpay_next_workbench_group g set stale_snooze_count=x.n,
    stale_snooze_code=case when x.n>0 then 'SNOOZE_SEGMENT_STALE' end
    from(select count(*) n from private.bpay_next_snooze s where s.work_id=v_work.id and s.scope_kind='SEGMENT'
      and s.active and (s.until_date is null or s.until_date>=v_today) and not exists(select 1
        from private.bpay_next_approved_segment a where a.revision_id=v_revision.id and a.segment_key=s.segment_key)) x
    where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;

create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_segment_basis jsonb;v_segment_complete boolean;
  v_principal numeric;v_delta numeric;v_event text;
  v_observation jsonb;v_mode text;v_manual_amount numeric;v_manual_reason text;v_anchor uuid;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate for update;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    v_capturable:=v_capturable or (v_work.work_kind='EXPENSE' and v_approved.component_kind in ('EXPENSE','MILEAGE')
      and v_approved.tax_treatment='NON_TAXABLE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
        and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
      or(v_work.work_kind in ('ORDINARY','EXPENSE') and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARE' then
    if p_effect_id is not null or not exists(select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_worker_released_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_WORKER_RELEASE_SCOPE_INVALID';end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_DISCARD' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_discarded_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null then
      -- Current authorised change retains the original actual paid anchor.
      -- No business/history sweep, full-amount debt or invented payment.
      if v_position.realised_source_ex_vat<=0 then return;end if;
      select e.id into v_anchor from private.bpay_next_financial_effect e
        where e.work_id=p_work_id and e.component_key=p_component_key
          and e.effect_kind='PAYROLL_SETTLED' and e.source_disposition_ex_vat>0
        order by e.occurred_at_utc,e.id limit 1;
      if v_anchor is null then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PAID_ANCHOR_UNBOUND';end if;
      select eff.* into strict v_effect from private.bpay_next_financial_effect eff where eff.id=v_anchor;
      select l.* into strict v_line from private.bpay_next_hold h
        join private.bpay_next_run_line l on l.id=h.run_line_id
        where h.id=v_effect.operation_item_id and h.status='REALISED';
      select line.* into strict v_approved from private.bpay_next_approved_line line where line.id=v_line.approved_line_id;
      -- A lone exact original payment may qualify; mixed history never does.
      v_capturable:=v_work.work_kind='ORDINARY' and v_line.source_pay_channel='PAYE'
        and v_line.target_pay_channel='PAYE' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
      v_capturable:=v_capturable or(v_work.work_kind='EXPENSE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
        and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
          and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
      v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
        or (v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
    else v_anchor:=p_effect_id;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id,collection_mode,manual_reason_code)
      values(p_work_id,p_component_key,v_candidate,v_anchor,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        coalesce(v_supported_paid,false) and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id,
        case when v_capturable then 'AUTOMATIC' else 'MANUAL' end,
        case when not coalesce(v_capturable,false) then 'AMOUNT_UNBOUND' end) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not coalesce(v_supported_paid,false) then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or(v_work.work_kind='EXPENSE' and v_original_revision.source_kind='EXPENSE' and v_revision.source_kind='EXPENSE'
      and v_original_revision.financial_snapshot_id is not null and v_revision.financial_snapshot_id is not null
      and v_original_approved.component_kind in ('EXPENSE','MILEAGE') and v_original_approved.tax_treatment='NON_TAXABLE')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_segment_basis:=private.bpay_next_segment_collection_basis_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_segment_complete:=coalesce((v_segment_basis->>'complete')::boolean,false);
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is null or v_basis.original_tax_treatment not in ('TAXABLE','NON_TAXABLE') then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and not v_segment_complete and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind=v_original_approved.component_kind and a.tax_treatment=v_original_approved.tax_treatment
            and (v_work.work_kind in ('ORDINARY','EXPENSE')
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_position.applied_revision_id
        and v.head_id=v_revision.source_head_id and v_revision.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v.approval_revision_id
        and ((v_job.job_kind='POSITION_APPLY' and publication.command_id=v_job.command_id
              and publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and publication.status='APPLIED' and publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  v_observation:=private.bpay_next_work_collection_observation_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_mode:=case when v_state='READY' and v_basis.original_source_pay_channel='PAYE'
    and not (v_work.approval_state='WITHDRAWN' and v_basis.collection_mode='MANUAL') then 'AUTOMATIC' else 'MANUAL' end;
  v_manual_amount:=null;v_manual_reason:=null;
  if v_mode='MANUAL' then
    v_manual_reason:=case when coalesce((v_observation->>'complete')::boolean,false)
      then 'AUTOMATIC_COLLECTION_UNAVAILABLE' else 'AMOUNT_UNBOUND' end;
    if v_manual_reason='AUTOMATIC_COLLECTION_UNAVAILABLE' then
      v_manual_amount:=(v_observation->>'free_recovery_source_ex_vat')::numeric;end if;
  end if;
  if v_state='READY' or (v_basis.case_id is not null and v_manual_amount is not null) then
    if v_basis.case_id is null then
      v_principal:=case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric
        else greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0) end;
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT',v_basis.original_tax_treatment,'OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,'RECOVERY','DEDUCTION',
            case when v_basis.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end,'PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        case when v_mode='MANUAL' and coalesce((v_observation->>'segmented')::boolean,false) then
          (v_observation->>'outstanding_source_ex_vat')::numeric
        when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric else
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0) end;
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    collection_mode=v_mode,manual_outstanding_source_ex_vat=v_manual_amount,manual_reason_code=v_manual_reason,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    segment_attribution_complete=v_segment_complete,
    observed_segment_negative_ex_vat=case when v_segment_complete then (v_segment_basis->>'negative_source_ex_vat')::numeric else 0 end,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
  perform private.bpay_next_workbench_refresh_many_v1(array[p_work_id],
    case when v_basis.case_id is null then array[]::uuid[] else array[v_basis.case_id] end);
end
$function$;

revoke all on function private.bpay_next_work_collection_observation_v1(uuid,text,uuid) from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_workbench_refresh_work_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;
commit;
