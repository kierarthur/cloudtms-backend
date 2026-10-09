-- F-25 accepted Source terms; actual monetary book remains current and once seeded.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_initial_tsfin_accepted_assert_v1(p_effective_work_cut_id uuid,p_binding_version_id uuid,p_component_key text)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v private.bpay_next_source_component_binding_version%rowtype;l private.bpay_next_approved_line%rowtype;
  cert jsonb;origin jsonb;basis jsonb;component jsonb;
  cut private.bpay_next_effective_work_cut%rowtype;member private.bpay_next_accepted_work_member%rowtype;
begin
  select x.* into strict v from private.bpay_next_source_component_binding_version x where x.id=p_binding_version_id;
  if v.origin_kind is distinct from 'INITIAL_AUTHORISED_TSFIN_V1' then
    raise exception 'BPAY_NEXT_INITIAL_ORIGIN_REQUIRED' using errcode='23514';end if;
  select c.* into strict cut from private.bpay_next_effective_work_cut c where c.id=p_effective_work_cut_id;
  select m.* into strict member from private.bpay_next_accepted_work_member m
    where m.accepted_work_input_id=cut.accepted_work_input_id and m.component_key=p_component_key
      and m.source_binding_version_id=v.id and m.source_binding_sha256=v.binding_digest;
  cert:=member.source_origin_certificate;origin:=cert->'origin';basis:=cert->'basis';component:=cert->'component';
  if (cut.work_id,cut.candidate_id,cut.effective_revision_id,member.effective_approved_line_id)
    is distinct from (v.canonical_work_id,v.candidate_id,v.approval_revision_id,v.approved_line_id)
    or decode(cert->>'certificate_digest','hex') is distinct from
      private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis)
    or (component->>'component_id')::uuid is distinct from v.source_component_id
    or decode(component->>'component_sha256','hex') is distinct from v.component_sha256 then
    raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
  select x.* into strict l from private.bpay_next_approved_line x
    where x.revision_id=(cert->>'revision_id')::uuid and x.source_component_id=v.source_component_id;
  if l.id is distinct from member.effective_approved_line_id
    or l.evidence_ref is distinct from 'source-component:'||v.source_component_id::text||'#'||encode(v.component_sha256,'hex') then
    raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
  if (v.candidate_id,v.canonical_work_id,v.approval_work_id,v.approval_revision_id,v.publication_command_id_1,
      v.initial_financial_snapshot_id,v.initial_root_timesheet_id,v.initial_root_version,v.initial_authorisation_generation,
      v.initial_authorised_row_signature,v.initial_financial_snapshot_digest,v.initial_inventory_digest,v.initial_entitlement_digest,
      v.initial_detail_digest,v.initial_policy_digest,v.initial_certificate_digest,v.component_sha256)
    is distinct from ((cert->>'candidate_id')::uuid,(cert->>'work_id')::uuid,(cert->>'work_id')::uuid,(cert->>'revision_id')::uuid,
      (cert->>'command_id')::uuid,(origin->>'financial_snapshot_id')::uuid,(origin->>'root_timesheet_id')::uuid,
      (origin->>'root_version')::integer,(origin->>'authorisation_generation')::integer,origin->>'authorised_row_signature',
      decode(origin->>'financial_snapshot_digest','hex'),decode(basis->>'inventory_digest','hex'),decode(basis->>'entitlement_digest','hex'),
      decode(basis->>'detail_digest','hex'),decode(basis->>'policy_fingerprint','hex'),decode(cert->>'certificate_digest','hex'),
      decode(component->>'component_sha256','hex'))
     or (v.approved_line_id,v.classification_line_id,v.canonical_component_key,v.approval_component_key,
         v.approval_source_channel,v.tax_treatment,v.publication_work_id_1,v.publication_revision_id_1)
       is distinct from (l.id,l.id,l.component_key,l.component_key,v.approval_source_channel,l.tax_treatment,
         (cert->>'work_id')::uuid,(cert->>'revision_id')::uuid)
     or v.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v.canonical_work_id,
       v.canonical_component_key,v.approval_revision_id,v.approved_line_id,v.initial_root_authorisation_id,
       cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'))
  then raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
end
$function$;

create or replace function private.bpay_next_accepted_source_origin_assert_v1(
 p_effective_work_cut_id uuid,p_component_key text,p_binding_version_id uuid
) returns void language plpgsql security definer
set search_path=pg_catalog,private,public as $function$
declare
 v_cut private.bpay_next_effective_work_cut%rowtype;
 v_parent private.bpay_next_accepted_input%rowtype;
 v_input private.bpay_next_accepted_work_input%rowtype;
 v_member private.bpay_next_accepted_work_member%rowtype;
 v_version private.bpay_next_source_component_binding_version%rowtype;
 v_revision private.bpay_next_work_revision%rowtype;v_line private.bpay_next_approved_line%rowtype;
 v_receipt private.weekly_source_entitlement_publication_receipts%rowtype;
 v_witness jsonb;v_publication record;v_count bigint;v_n integer;
begin
 perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
 if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select c.* into strict v_cut from private.bpay_next_effective_work_cut c where c.id=p_effective_work_cut_id;
 select a.* into strict v_parent from private.bpay_next_accepted_input a where a.id=v_cut.accepted_input_id;
 select a.* into strict v_input from private.bpay_next_accepted_work_input a where a.id=v_cut.accepted_work_input_id;
 select m.* into strict v_member from private.bpay_next_accepted_work_member m
   where m.accepted_work_input_id=v_input.id and m.component_key=p_component_key;
 select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=p_binding_version_id;
 select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_cut.effective_revision_id;
 select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_member.effective_approved_line_id;
 if (v_cut.accepted_input_id,v_cut.work_id,v_cut.candidate_id,v_cut.effective_revision_id,v_cut.publication_command_id,
      v_cut.predecessor_sequence,v_cut.selected_line_count)
     is distinct from (v_input.accepted_input_id,v_input.work_id,v_input.candidate_id,v_input.effective_revision_id,
       v_input.publication_command_id,v_input.predecessor_sequence,v_input.selected_line_count)
   or v_cut.evidence_sha256 is distinct from v_input.evidence_sha256
   or v_cut.predecessor_sequence>=v_parent.accepted_sequence
   or not exists(select 1 from private.bpay_next_job j where j.id=v_cut.prepare_job_id and j.job_kind='PREPARE'
     and (j.command_id,j.candidate_id,j.module_epoch)=(v_parent.prepare_command_id,v_parent.candidate_id,v_parent.module_epoch))
   or (v_member.source_binding_version_id,v_member.source_binding_sha256,v_member.work_id,v_member.effective_revision_id,
       v_member.effective_approved_line_id,v_member.source_component_id)
     is distinct from (v_version.id,v_version.binding_digest,v_version.canonical_work_id,v_version.approval_revision_id,
       v_version.approved_line_id,v_version.source_component_id)
   or (v_version.candidate_id,v_version.approval_work_id,v_version.canonical_component_key,v_version.approval_component_key,
       v_version.classification_line_id,v_version.approval_source_channel,v_version.tax_treatment,v_version.component_state)
     is distinct from (v_cut.candidate_id,v_cut.work_id,p_component_key,p_component_key,v_line.id,
       v_revision.source_pay_channel,v_line.tax_treatment,'PRESENT'::text)
   or (v_line.revision_id,v_line.component_key) is distinct from (v_cut.effective_revision_id,p_component_key)
   or v_revision.work_id<>v_cut.work_id or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
   or v_input.evidence_sha256 is distinct from sha256(convert_to((to_jsonb(v_input)-array['id','evidence_sha256'])::text,'UTF8'))
   or (select count(*) from private.bpay_next_accepted_work_member m where m.accepted_work_input_id=v_input.id)<>v_cut.selected_line_count then
   raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_ORIGIN_MISMATCH';end if;
 select b.value into strict v_witness from jsonb_array_elements(v_input.origin_proof->'bindings') b(value)
   where b.value->>'binding_version_id'=v_version.id::text;
 if (v_witness->>'approved_line_id',v_witness->>'binding_sha256',v_witness->>'source_receipt_id')
   is distinct from (v_line.id::text,encode(v_version.binding_digest,'hex'),v_version.source_receipt_id::text) then
   raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_BINDING_MISMATCH';end if;
 if not exists(select 1 from private.bpay_next_publication p
   join private.bpay_next_command c on c.id=p.command_id
   join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=p.candidate_id
   join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=m.candidate_id and j.job_kind='POSITION_APPLY'
   where p.command_id=v_cut.publication_command_id and p.work_id=v_cut.work_id and p.revision_id=v_cut.effective_revision_id
     and p.candidate_id=v_cut.candidate_id and (p.status,p.phase)=('APPLIED','DONE') and p.applied_at_utc is not null
     and c.command_kind='POSITION_APPLY' and c.agency_sequence=v_cut.predecessor_sequence and c.module_epoch=v_parent.module_epoch
     and c.status='COMPLETE' and c.expected_member_count=c.enrolled_member_count and c.sealed_at_utc is not null
     and (j.status,j.phase)=('DONE','DONE') and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
     and j.lease_nonce is null and j.lease_until_utc is null) then
   raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_SOURCE_PUBLICATION_PENDING';end if;
 if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
   perform private.bpay_next_initial_tsfin_accepted_assert_v1(v_cut.id,v_version.id,p_component_key);
 elsif v_version.origin_kind='COMMON_HEAD_RECEIPT_V1' then
   select r.* into strict v_receipt from private.weekly_source_entitlement_publication_receipts r where r.id=v_version.source_receipt_id;
   v_n:=cardinality(v_receipt.head_ids);
   if (v_receipt.candidate_id,v_receipt.decision_bundle_id,v_receipt.bundle_revision,v_receipt.request_digest)
      is distinct from (v_version.candidate_id,v_version.decision_bundle_id,v_version.bundle_revision,v_version.request_digest)
     or v_n not between 1 and 2 or jsonb_array_length(v_witness->'publication_members')<>v_n
     or (select count(distinct x.member_no) from jsonb_to_recordset(v_witness->'publication_members') x(member_no integer))<>v_n
     or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
       where b.decision_bundle_id=v_receipt.decision_bundle_id and b.bundle_revision=v_receipt.bundle_revision
         and b.candidate_id=v_receipt.candidate_id and b.request_digest=v_receipt.request_digest) then
     raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_RECEIPT_MISMATCH';end if;
   for v_publication in select x.* from jsonb_to_recordset(v_witness->'publication_members') x(
     candidate_id uuid,member_no integer,head_id uuid,prior_head_id uuid,work_id uuid,revision_id uuid,
     publication_command_id uuid,predecessor_revision_id uuid) loop
     if v_publication.member_no not between 1 and v_n or v_publication.candidate_id is distinct from v_cut.candidate_id then
       raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_MEMBER_MISMATCH';end if;
     select count(*) into v_count from public.weekly_source_entitlement_heads h
       join private.bpay_next_work_revision r on r.source_head_id=h.id and r.source_event_id=h.id
       join private.bpay_next_work w on w.id=r.work_id
       join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
       join private.bpay_next_command c on c.id=p.command_id
       join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
       join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='POSITION_APPLY'
       where h.id=v_receipt.head_ids[v_publication.member_no] and h.id=v_publication.head_id
         and h.prior_head_id is not distinct from v_publication.prior_head_id
         and (h.root_timesheet_id,h.root_family_booking_id,h.root_timesheet_version)=
           (v_receipt.member_root_ids[v_publication.member_no],v_receipt.member_family_booking_ids[v_publication.member_no],v_receipt.member_root_versions[v_publication.member_no])
         and h.candidate_id=v_cut.candidate_id and h.committed_at_utc is not null
         and h.publication_receipt_digest=v_receipt.request_digest
         and (h.decision_bundle_id,h.bundle_revision)=(v_receipt.decision_bundle_id,v_receipt.bundle_revision)
         and h.scope_change_tx_token=v_receipt.scope_change_tx_token
         and r.id=v_publication.revision_id and w.id=v_publication.work_id and p.command_id=v_publication.publication_command_id
         and p.predecessor_revision_id is not distinct from v_publication.predecessor_revision_id
         and ((p.command_id,p.work_id,p.revision_id)=(v_version.publication_command_id_1,v_version.publication_work_id_1,v_version.publication_revision_id_1)
           or (p.command_id,p.work_id,p.revision_id)=(v_version.publication_command_id_2,v_version.publication_work_id_2,v_version.publication_revision_id_2))
         and (p.status,p.phase)=('APPLIED','DONE') and p.applied_at_utc is not null
         and c.command_kind='POSITION_APPLY' and c.status='COMPLETE' and c.agency_sequence<v_parent.accepted_sequence
         and c.module_epoch=v_parent.module_epoch and c.sealed_at_utc is not null and c.expected_member_count=c.enrolled_member_count
         and (j.status,j.phase)=('DONE','DONE') and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
         and j.lease_nonce is null and j.lease_until_utc is null;
     if v_count<>1 then raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_MEMBER_MISMATCH';end if;
   end loop;
 else raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_SOURCE_ORIGIN_UNSUPPORTED';end if;
end
$function$;

create or replace function private.bpay_next_source_book_seed_proof_accepted_v1(
  p_effective_work_cut_id uuid,p_component_key text,p_seed_effect_id uuid,p_seed_hold_id uuid)
returns table(seed_kind text,basis_revision_id uuid,basis_line_id uuid,
  source_channel text,realised_source private.bpay_next_penny_amount,
  held_source private.bpay_next_penny_amount,seed_digest bytea)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_cut private.bpay_next_effective_work_cut%rowtype;v_member private.bpay_next_accepted_work_member%rowtype;
  p_work_id uuid;p_expected_applied_revision uuid;
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_position private.bpay_next_position%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_hold private.bpay_next_hold%rowtype;
  v_paid_hold private.bpay_next_hold%rowtype;v_frozen private.bpay_next_run_line%rowtype;
begin
  select c.* into strict v_cut from private.bpay_next_effective_work_cut c where c.id=p_effective_work_cut_id;
  p_work_id:=v_cut.work_id;p_expected_applied_revision:=v_cut.effective_revision_id;
  select m.* into strict v_member from private.bpay_next_accepted_work_member m
    where m.accepted_work_input_id=v_cut.accepted_work_input_id and m.component_key=p_component_key;
  perform private.bpay_next_accepted_source_origin_assert_v1(v_cut.id,p_component_key,v_member.source_binding_version_id);
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id;
  select p.* into strict v_position from private.bpay_next_position p where p.work_id=p_work_id and p.component_key=p_component_key;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=p_expected_applied_revision and r.work_id=p_work_id;
  if v_work.work_kind is distinct from 'SOURCE'
     or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
     or v_position.source_basis_channel is null
     or v_position.source_basis_channel not in ('PAYE','UMBRELLA')
     or not exists(select 1 from private.bpay_next_publication p where p.work_id=p_work_id
         and p.revision_id=p_expected_applied_revision and p.status='APPLIED' and p.phase='DONE')
     or p_component_key not like 'SOURCE:%' then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';
  end if;
  -- Negative/mixed old scalar states need a separately accepted signed basis.
  -- That CASE/manual owner is not implemented by this initialization seam.
  if v_position.realised_source_ex_vat<0 or v_position.held_source_ex_vat<0 then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SIGNED_SEED_UNAVAILABLE';
  end if;
  source_channel:=v_position.source_basis_channel;
  realised_source:=v_position.realised_source_ex_vat;held_source:=v_position.held_source_ex_vat;
  if realised_source>0 and p_seed_effect_id is null or held_source>0 and p_seed_hold_id is null then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_TYPED_WITNESS_REQUIRED';
  end if;
  if p_seed_effect_id is not null then
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_seed_effect_id;
    select h.* into strict v_paid_hold from private.bpay_next_hold h where h.id=v_effect.operation_item_id;
    select l.* into strict v_frozen from private.bpay_next_run_line l where l.id=v_paid_hold.run_line_id;
    if v_effect.work_id is distinct from p_work_id or v_effect.component_key is distinct from p_component_key
       or v_effect.candidate_id is distinct from v_work.candidate_id or v_effect.effect_kind is distinct from 'PAYROLL_SETTLED'
       or v_effect.original_timesheet_id is distinct from v_work.original_timesheet_id
       or v_effect.source_disposition_ex_vat<=0 or v_paid_hold.status is distinct from 'REALISED'
       or v_paid_hold.work_id is distinct from p_work_id or v_paid_hold.component_key is distinct from p_component_key
       or v_effect.source_disposition_ex_vat is distinct from v_paid_hold.source_reserved_ex_vat
       or v_frozen.work_id is distinct from p_work_id or v_frozen.component_key is distinct from p_component_key
       or v_frozen.source_pay_channel is distinct from source_channel
       or v_frozen.source_consumed_ex_vat is distinct from v_paid_hold.source_reserved_ex_vat
       or v_effect.target_amount_ex_vat is distinct from v_frozen.frozen_ex_vat
       or v_effect.target_amount_vat is distinct from v_frozen.frozen_vat
       or v_effect.target_amount_inc_vat is distinct from v_frozen.frozen_inc_vat
       or not exists(select 1 from private.bpay_next_job j where j.command_id=v_effect.operation_id
           and j.candidate_id=v_work.candidate_id and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT'))
       or not exists(select 1 from private.bpay_next_transfer t
           join private.bpay_next_transfer_member m on m.transfer_id=t.id and m.run_worker_id=t.run_worker_id
           where t.id=v_effect.original_transfer_id and t.candidate_id=v_work.candidate_id
             and t.status in ('SETTLED','RETURNED') and m.subject_kind='WORK' and m.run_line_id=v_frozen.id)
       or v_frozen.source_binding_version_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_PAID_WITNESS_MISMATCH';
    end if;
    select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_frozen.approved_line_id;
    basis_revision_id:=v_frozen.captured_revision_id;basis_line_id:=v_line.id;
  end if;
  if p_seed_hold_id is not null then
    select h.* into strict v_hold from private.bpay_next_hold h where h.id=p_seed_hold_id;
    select l.* into strict v_frozen from private.bpay_next_run_line l where l.id=v_hold.run_line_id;
    if held_source<=0 or v_hold.status is distinct from 'ACTIVE' or v_hold.work_id is distinct from p_work_id
       or v_hold.component_key is distinct from p_component_key or v_hold.source_reserved_ex_vat<=0
       or v_frozen.work_id is distinct from p_work_id or v_frozen.component_key is distinct from p_component_key
       or v_frozen.source_pay_channel is distinct from source_channel
       or v_frozen.source_consumed_ex_vat is distinct from v_hold.source_reserved_ex_vat
       or v_hold.target_amount_ex_vat is distinct from v_frozen.frozen_ex_vat
       or v_hold.target_amount_vat is distinct from v_frozen.frozen_vat
       or v_hold.target_amount_inc_vat is distinct from v_frozen.frozen_inc_vat
       or v_frozen.source_binding_version_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_HELD_WITNESS_MISMATCH';
    end if;
    if basis_line_id is null then
      select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_frozen.approved_line_id;
      basis_revision_id:=v_frozen.captured_revision_id;basis_line_id:=v_line.id;
    end if;
  end if;
  if basis_line_id is null then
    if realised_source<>0 or held_source<>0
       or v_position.realised_target_ex_vat<>0 or v_position.realised_target_vat<>0 or v_position.realised_target_inc_vat<>0
       or v_position.held_target_ex_vat<>0 or v_position.held_target_vat<>0 or v_position.held_target_inc_vat<>0 then
      raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ZERO_BASIS_UNAVAILABLE';
    end if;
    select l.* into strict v_line from private.bpay_next_approved_line l
      where l.id=v_member.effective_approved_line_id and l.revision_id=v_cut.effective_revision_id;
    -- ZERO describes the maintained book, not the absence of older children.
    basis_revision_id:=v_line.revision_id;basis_line_id:=v_line.id;seed_kind:='ZERO';
  else seed_kind:='ORIGINAL_POSITION';end if;
  if v_line.component_key is distinct from p_component_key or v_line.source_component_id is null
     or p_component_key is distinct from 'SOURCE:'||v_line.source_component_id::text then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_COMPONENT_MISMATCH';
  end if;
  seed_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(
    p_effective_work_cut_id,p_work_id,p_component_key,p_expected_applied_revision,basis_revision_id,basis_line_id,source_channel,
    realised_source,held_source,p_seed_effect_id,p_seed_hold_id,seed_kind)::text,'UTF8'));
  return next;
end
$function$;

create or replace function private.bpay_next_source_disposition_seed_accepted_v1(
  p_effective_work_cut_id uuid,p_component_key text,p_seed_effect_id uuid default null,p_seed_hold_id uuid default null)
returns table(qualification_state text,source_book_revision bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_cut private.bpay_next_effective_work_cut%rowtype;
  p_work_id uuid;p_expected_applied_revision uuid;v_candidate uuid;v_position private.bpay_next_position%rowtype;v_proof record;
begin
  select c.* into strict v_cut from private.bpay_next_effective_work_cut c where c.id=p_effective_work_cut_id;
  p_work_id:=v_cut.work_id;p_expected_applied_revision:=v_cut.effective_revision_id;
  if p_work_id is null or p_component_key is null or p_expected_applied_revision is null then
    raise exception using errcode='22023',message='BPAY_NEXT_SOURCE_BOOK_SEED_ID_REQUIRED';end if;
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED';end if;
  perform 1 from private.bpay_next_work w where w.id=p_work_id for update;
  select p.* into v_position from private.bpay_next_position p where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if not found then qualification_state:='POSITION_PENDING';source_book_revision:=0;return next;return;end if;
  if v_position.source_book_state='READY' then
    -- Existing real book is not reseeded by a later Draft.
    if v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
      or (v_position.source_book_seed_channel='PAYE' and
        (v_position.native_realised_paye,v_position.native_held_paye,v_position.native_realised_umbrella,v_position.native_held_umbrella)
          is distinct from (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,0::numeric,0::numeric))
      or (v_position.source_book_seed_channel='UMBRELLA' and
        (v_position.native_realised_umbrella,v_position.native_held_umbrella,v_position.native_realised_paye,v_position.native_held_paye)
          is distinct from (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,0::numeric,0::numeric))
      or v_position.source_book_seed_channel not in ('PAYE','UMBRELLA') then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_POSTING_BASIS_UNQUALIFIED';end if;
    qualification_state:='READY';source_book_revision:=v_position.source_book_revision;return next;return;
  end if;
  select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_accepted_v1(
    p_effective_work_cut_id,p_component_key,p_seed_effect_id,p_seed_hold_id) p;
  update private.bpay_next_position p set source_book_state='READY',source_book_revision=1,
    source_book_seed_effective_cut_id=p_effective_work_cut_id,
    native_realised_paye=case when v_proof.source_channel='PAYE' then v_proof.realised_source else 0 end,
    native_realised_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.realised_source else 0 end,
    native_held_paye=case when v_proof.source_channel='PAYE' then v_proof.held_source else 0 end,
    native_held_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.held_source else 0 end,
    source_book_seed_kind=v_proof.seed_kind,source_book_seed_applied_revision_id=p_expected_applied_revision,
    source_book_seed_revision_id=v_proof.basis_revision_id,source_book_seed_line_id=v_proof.basis_line_id,
    source_book_seed_effect_id=p_seed_effect_id,source_book_seed_hold_id=p_seed_hold_id,
    source_book_seed_channel=v_proof.source_channel,source_book_seed_digest=v_proof.seed_digest
    where p.work_id=p_work_id and p.component_key=p_component_key;
  qualification_state:='READY';source_book_revision:=1;return next;
end
$function$;

create or replace function private.bpay_next_accepted_source_snapshot_v1(p_effective_work_cut_id uuid,p_binding_version_id uuid,p_expected_book_revision bigint)
returns table(qualification_state text,selection_state text,candidate_id uuid,
  canonical_work_id uuid,canonical_component_key text,binding_version_id uuid,source_book_revision bigint,
  approval_work_id uuid,approval_revision_id uuid,approved_line_id uuid,head_id uuid,source_receipt_id uuid,
  source_channel text,tax_treatment text,classification_line_id uuid,valuation_policy_id uuid,
  approved_source_ex_vat private.bpay_next_penny_amount,
  native_realised_paye private.bpay_next_penny_amount,native_realised_umbrella private.bpay_next_penny_amount,
  native_held_paye private.bpay_next_penny_amount,native_held_umbrella private.bpay_next_penny_amount)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_version private.bpay_next_source_component_binding_version%rowtype;
  v_cut private.bpay_next_effective_work_cut%rowtype;
  v_position private.bpay_next_position%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_count integer;
begin
  if p_binding_version_id is null or p_expected_book_revision is null or p_expected_book_revision<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_BOUND_SNAPSHOT_ID_REQUIRED';end if;
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=p_binding_version_id;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_version.candidate_id for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED';end if;
  select c.* into strict v_cut from private.bpay_next_effective_work_cut c where c.id=p_effective_work_cut_id;
  perform private.bpay_next_accepted_source_origin_assert_v1(v_cut.id,v_version.canonical_component_key,v_version.id);
  select p.* into v_position from private.bpay_next_position p
    where p.work_id=v_cut.work_id and p.component_key=v_version.canonical_component_key for share;
  if not found then qualification_state:='POSITION_PENDING';return next;return;end if;
  if v_position.source_book_state is distinct from 'READY' then qualification_state:='BOOK_UNBOUND';return next;return;end if;
  if v_position.source_book_revision is distinct from p_expected_book_revision then
    qualification_state:='BOOK_REVISION_STALE';return next;return;end if;
  if v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
     or (v_position.source_book_seed_channel='PAYE' and
       (v_position.native_realised_paye is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_paye is distinct from v_position.held_source_ex_vat))
     or (v_position.source_book_seed_channel='UMBRELLA' and
       (v_position.native_realised_umbrella is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_umbrella is distinct from v_position.held_source_ex_vat)) then
    qualification_state:='BOOK_POSTING_REQUIRED';return next;return;end if;
  if (v_position.source_book_seed_channel='PAYE' and
      (v_position.native_realised_umbrella,v_position.native_held_umbrella) is distinct from (0::numeric,0::numeric))
    or (v_position.source_book_seed_channel='UMBRELLA' and
      (v_position.native_realised_paye,v_position.native_held_paye) is distinct from (0::numeric,0::numeric)) then
    qualification_state:='BOOK_POSTING_REQUIRED';return next;return;end if;
  if v_version.tax_treatment is null or v_version.classification_line_id is null then
    qualification_state:='CLASSIFICATION_UNBOUND';return next;return;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_version.approval_revision_id;
  if v_version.component_state='PRESENT' then
    select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_version.approved_line_id;
    approved_source_ex_vat:=v_line.source_pay_ex_vat;
  else
    if exists(select 1 from private.bpay_next_approved_line l
      where l.revision_id=v_version.approval_revision_id and l.component_key=v_version.approval_component_key) then
      raise exception using errcode='23514',message='BPAY_NEXT_BINDING_REMOVAL_LINE_CONFLICT';end if;
    approved_source_ex_vat:=0;
  end if;
  candidate_id:=v_version.candidate_id;canonical_work_id:=v_version.canonical_work_id;
  canonical_component_key:=v_version.canonical_component_key;binding_version_id:=v_version.id;
  source_book_revision:=v_position.source_book_revision;approval_work_id:=v_version.approval_work_id;
  approval_revision_id:=v_version.approval_revision_id;approved_line_id:=v_version.approved_line_id;
  head_id:=v_version.head_id;source_receipt_id:=v_version.source_receipt_id;source_channel:=v_version.approval_source_channel;
  tax_treatment:=v_version.tax_treatment;classification_line_id:=v_version.classification_line_id;
  valuation_policy_id:=v_revision.valuation_policy_id;
  native_realised_paye:=v_position.native_realised_paye;native_realised_umbrella:=v_position.native_realised_umbrella;
  native_held_paye:=v_position.native_held_paye;native_held_umbrella:=v_position.native_held_umbrella;
  selection_state:='UNBOUND';qualification_state:='READY';return next;
end
$function$;

create or replace function private.bpay_next_source_book_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_proof record;
begin
  if tg_op='INSERT' then
    if new.source_book_state is distinct from 'UNBOUND' or new.source_book_seed_effective_cut_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_DIRECT_READY_INSERT_FORBIDDEN';end if;
    return new;
  end if;
  if old.source_book_state='READY' then
    if (pg_catalog.to_jsonb(new)-array['applied_revision_id','source_basis_channel','approved_source_ex_vat',
      'realised_source_ex_vat','held_source_ex_vat','realised_target_ex_vat','realised_target_vat',
      'realised_target_inc_vat','held_target_ex_vat','held_target_vat','held_target_inc_vat','updated_at_utc',
      'source_book_revision','native_realised_paye','native_realised_umbrella','native_held_paye','native_held_umbrella'])
      is distinct from
      (pg_catalog.to_jsonb(old)-array['applied_revision_id','source_basis_channel','approved_source_ex_vat',
      'realised_source_ex_vat','held_source_ex_vat','realised_target_ex_vat','realised_target_vat',
      'realised_target_inc_vat','held_target_ex_vat','held_target_vat','held_target_inc_vat','updated_at_utc',
      'source_book_revision','native_realised_paye','native_realised_umbrella','native_held_paye','native_held_umbrella']) then
      raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONNECTED_POSTING_REQUIRED';end if;
    -- The actual owner changes the scalar once. Derive both native buckets
    -- in this BEFORE trigger; never issue a recursive UPDATE or count H->R twice.
    if new.source_book_revision is distinct from old.source_book_revision
       or (new.native_realised_paye,new.native_realised_umbrella,new.native_held_paye,new.native_held_umbrella)
         is distinct from (old.native_realised_paye,old.native_realised_umbrella,old.native_held_paye,old.native_held_umbrella) then
      raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CALLER_CACHE_WRITE_FORBIDDEN';
    end if;
    if (new.realised_source_ex_vat,new.held_source_ex_vat) is distinct from
       (old.realised_source_ex_vat,old.held_source_ex_vat) then
      if old.source_book_seed_channel not in ('PAYE','UMBRELLA')
         or old.source_basis_channel is distinct from old.source_book_seed_channel
         or new.source_basis_channel is distinct from old.source_book_seed_channel
         or (old.source_book_seed_channel='PAYE' and
           (old.native_realised_paye,old.native_held_paye,old.native_realised_umbrella,old.native_held_umbrella)
             is distinct from (old.realised_source_ex_vat,old.held_source_ex_vat,0::numeric,0::numeric))
         or (old.source_book_seed_channel='UMBRELLA' and
           (old.native_realised_umbrella,old.native_held_umbrella,old.native_realised_paye,old.native_held_paye)
             is distinct from (old.realised_source_ex_vat,old.held_source_ex_vat,0::numeric,0::numeric))
         or new.realised_source_ex_vat<0 or new.held_source_ex_vat<0 then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_POSTING_BASIS_UNQUALIFIED';
      end if;
      if old.source_book_seed_channel='PAYE' then
        new.native_realised_paye:=old.native_realised_paye+new.realised_source_ex_vat-old.realised_source_ex_vat;
        new.native_held_paye:=old.native_held_paye+new.held_source_ex_vat-old.held_source_ex_vat;
      else
        new.native_realised_umbrella:=old.native_realised_umbrella+new.realised_source_ex_vat-old.realised_source_ex_vat;
        new.native_held_umbrella:=old.native_held_umbrella+new.held_source_ex_vat-old.held_source_ex_vat;
      end if;
      new.source_book_revision:=old.source_book_revision+1;
    end if;
    return new;
  end if;
  if new.source_book_state='READY' then
    if new.source_book_seed_effective_cut_id is not null then
      if (new.work_id,new.source_book_seed_applied_revision_id) is distinct from (
        select c.work_id,c.effective_revision_id from private.bpay_next_effective_work_cut c
        where c.id=new.source_book_seed_effective_cut_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_SEED_CONFLICT';end if;
      select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_accepted_v1(
        new.source_book_seed_effective_cut_id,new.component_key,new.source_book_seed_effect_id,new.source_book_seed_hold_id) p;
    else
    select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_v1(
      new.work_id,new.component_key,new.source_book_seed_applied_revision_id,
      new.source_book_seed_effect_id,new.source_book_seed_hold_id) p;
    end if;
    if new.source_book_revision is distinct from 1
       or new.source_book_seed_kind is distinct from v_proof.seed_kind
       or new.source_book_seed_revision_id is distinct from v_proof.basis_revision_id
       or new.source_book_seed_line_id is distinct from v_proof.basis_line_id
       or new.source_book_seed_channel is distinct from v_proof.source_channel
       or new.source_book_seed_digest is distinct from v_proof.seed_digest
       or new.native_realised_paye is distinct from (case when v_proof.source_channel='PAYE' then v_proof.realised_source else 0 end)
       or new.native_realised_umbrella is distinct from (case when v_proof.source_channel='UMBRELLA' then v_proof.realised_source else 0 end)
       or new.native_held_paye is distinct from (case when v_proof.source_channel='PAYE' then v_proof.held_source else 0 end)
       or new.native_held_umbrella is distinct from (case when v_proof.source_channel='UMBRELLA' then v_proof.held_source else 0 end)
       or (pg_catalog.to_jsonb(new)-array['source_book_state','source_book_revision','native_realised_paye',
        'native_realised_umbrella','native_held_paye','native_held_umbrella','source_book_seed_kind',
        'source_book_seed_applied_revision_id','source_book_seed_revision_id','source_book_seed_line_id',
        'source_book_seed_effect_id','source_book_seed_hold_id','source_book_seed_channel','source_book_seed_digest',
        'source_book_seed_effective_cut_id'])
         is distinct from (pg_catalog.to_jsonb(old)-array['source_book_state','source_book_revision','native_realised_paye',
        'native_realised_umbrella','native_held_paye','native_held_umbrella','source_book_seed_kind',
        'source_book_seed_applied_revision_id','source_book_seed_revision_id','source_book_seed_line_id',
        'source_book_seed_effect_id','source_book_seed_hold_id','source_book_seed_channel','source_book_seed_digest',
        'source_book_seed_effective_cut_id']) then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_SEED_CONFLICT';end if;
  end if;
  if new.source_book_state='UNBOUND' and new.source_book_seed_effective_cut_id is distinct from old.source_book_seed_effective_cut_id then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_UNBOUND_SEED_REFERENCE_FORBIDDEN';end if;
  return new;
end
$function$;

alter function private.bpay_next_initial_tsfin_accepted_assert_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_accepted_assert_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_accepted_source_origin_assert_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_accepted_source_origin_assert_v1(uuid,text,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_source_book_seed_proof_accepted_v1(uuid,text,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_book_seed_proof_accepted_v1(uuid,text,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_source_disposition_seed_accepted_v1(uuid,text,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_disposition_seed_accepted_v1(uuid,text,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_accepted_source_snapshot_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_accepted_source_snapshot_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_source_book_guard_v1() owner to postgres;
revoke all on function private.bpay_next_source_book_guard_v1() from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
