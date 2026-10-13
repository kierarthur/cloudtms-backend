-- SP-10 / FC-15 accepted explicit Source removal; exact native net liability.
-- Successor of 12102026_1849 observation only. No producer or money formula change.
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
  v_retained boolean:=false;v_removed boolean:=false;v_complete boolean:=false;v_outstanding numeric;v_free numeric;v_negative numeric;v_positive numeric;v_overhang numeric;
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
  -- Explicit accepted REMOVED membership certifies this typed component's zero.
  -- A surviving fixed sibling may make the current revision FIXED; the paid
  -- component and native segment counters retain their original identity.
  v_removed:=v_work.work_kind='SOURCE' and v_original.work_id=p_work_id
    and v_line.revision_id=v_original.id and v_line.component_key=p_component_key
    and v_line.source_component_id is not null
    and p_component_key='SOURCE:'||v_line.source_component_id::text
    and v_current.id is null and v_position.approved_source_ex_vat=0
    and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id
    and ((v_original.source_kind='SOURCE' and v_original.source_event_id is not null
        and (v_original.source_head_id is null or v_original.source_head_id=v_original.source_event_id))
      or (v_original.source_kind='PROTECTED' and v_original.source_head_id is not null
        and v_original.source_head_id=v_original.source_event_id))
    and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null or v_revision.source_head_id=v_revision.source_event_id))
      or (v_revision.source_kind='PROTECTED' and v_revision.source_head_id is not null
        and v_revision.source_head_id=v_revision.source_event_id))
    and exists(
      select 1 from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          and bv.canonical_work_id=b.canonical_work_id and bv.canonical_component_key=b.canonical_component_key
          and bv.candidate_id=b.candidate_id and bv.source_component_id=b.source_component_id
        join private.bpay_next_approved_line classification on classification.id=bv.classification_line_id
        join private.bpay_next_work_revision classified on classified.id=classification.revision_id
          and classified.work_id=p_work_id and classified.sealed_at_utc is not null
        join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
        join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=bv.decision_bundle_id
          and bundle.bundle_revision=bv.bundle_revision and bundle.state='COMMITTED'
        join public.weekly_source_entitlement_heads head on head.id=bv.head_id and head.state='COMMITTED_CURRENT'
        join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
          and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
        join private.bpay_next_publication publication on publication.command_id=bv.publication_command_id_1
          and publication.work_id=bv.publication_work_id_1 and publication.revision_id=bv.publication_revision_id_1
          and publication.candidate_id=v_work.candidate_id
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and b.candidate_id=v_work.candidate_id and b.source_component_id=v_line.source_component_id
        and bv.component_state='REMOVED' and bv.approved_line_id is null
        and bv.approval_work_id=p_work_id and bv.approval_revision_id=p_revision_id
        and bv.approval_component_key=p_component_key and bv.approval_source_channel=v_basis.original_source_pay_channel
        and bv.publication_work_id_1=p_work_id and bv.publication_revision_id_1=p_revision_id
        and bv.publication_command_id_2 is null and bv.publication_work_id_2 is null and bv.publication_revision_id_2 is null
        and bv.head_id=v_revision.source_head_id
        and classification.component_key=p_component_key and classification.source_component_id=v_line.source_component_id
        and classification.component_kind=v_line.component_kind
        and classification.tax_treatment is not distinct from v_line.tax_treatment
        and bv.tax_treatment is not distinct from v_line.tax_treatment
        and classified.source_pay_channel=v_basis.original_source_pay_channel
        and ((classified.source_kind='SOURCE' and classified.source_event_id is not null
            and (classified.source_head_id is null or classified.source_head_id=classified.source_event_id))
          or (classified.source_kind='PROTECTED' and classified.source_head_id is not null
            and classified.source_head_id=classified.source_event_id))
        and receipt.candidate_id=v_work.candidate_id and receipt.decision_bundle_id=bv.decision_bundle_id
        and receipt.bundle_revision=bv.bundle_revision and receipt.request_digest=bv.request_digest
        and cardinality(receipt.head_ids)=1 and receipt.head_ids[1]=bv.head_id
        and publication.status in ('QUEUED','APPLYING','APPLIED'));
  v_complete:=v_complete or coalesce(v_removed,false);
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
    v_complete:=v_complete and v_line.component_kind='WORK' and (v_revision.detail_kind='SHIFT' or v_revision.certified_zero
        or (coalesce(v_removed,false) and v_original.detail_kind='SHIFT'))
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

commit;
