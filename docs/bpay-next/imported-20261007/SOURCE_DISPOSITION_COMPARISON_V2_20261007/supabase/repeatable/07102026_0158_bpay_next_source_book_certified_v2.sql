-- E-only exact replacement of two actual 1246 functions; legacy body otherwise unchanged.
-- Read-only siblings are mechanically projected from the same pinned qualification body: no independent gates.
-- Comparison expiry sibling preserves the real 0510 owner proof outside its exact typed channel/policy lane.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_source_book_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_proof record;
begin

  if tg_op='UPDATE' and old.source_book_state='READY'
    and new.source_comparison_receipt_id is distinct from old.source_comparison_receipt_id then
    perform private.bpay_next_source_comparison_assert_book_update_v2(old,new);
    return new; -- Exact certificate already accounts for native and legitimate scalar changes once.
  end if;
  if tg_op='UPDATE' and old.source_comparison_receipt_id is not null
    and (private.bpay_next_source_comparison_scalar_v2(new) is distinct from private.bpay_next_source_comparison_scalar_v2(old)
      or new.source_book_revision is distinct from old.source_book_revision
      or (new.native_realised_paye,new.native_realised_umbrella,new.native_held_paye,new.native_held_umbrella)
        is distinct from (old.native_realised_paye,old.native_realised_umbrella,old.native_held_paye,old.native_held_umbrella)) then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_COMPARISON_EXACT_CARRY_REQUIRED';
  end if;
  if tg_op='INSERT' then
    if new.source_book_state is distinct from 'UNBOUND' then
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
    select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_v1(
      new.work_id,new.component_key,new.source_book_seed_applied_revision_id,
      new.source_book_seed_effect_id,new.source_book_seed_hold_id) p;
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
        'source_book_seed_effect_id','source_book_seed_hold_id','source_book_seed_channel','source_book_seed_digest'])
         is distinct from (pg_catalog.to_jsonb(old)-array['source_book_state','source_book_revision','native_realised_paye',
        'native_realised_umbrella','native_held_paye','native_held_umbrella','source_book_seed_kind',
        'source_book_seed_applied_revision_id','source_book_seed_revision_id','source_book_seed_line_id',
        'source_book_seed_effect_id','source_book_seed_hold_id','source_book_seed_channel','source_book_seed_digest']) then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_SEED_CONFLICT';end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_bound_source_snapshot_v1(p_binding_version_id uuid,p_expected_book_revision bigint)
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
  v_binding private.bpay_next_source_component_binding%rowtype;
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
  select b.* into strict v_binding from private.bpay_next_source_component_binding b
    where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key for share;
  if v_binding.current_binding_version_id is distinct from p_binding_version_id then
    qualification_state:='SUPERSEDED';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    perform private.bpay_next_initial_tsfin_bound_assert_v1(v_version.id);
  else
  if not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_version.decision_bundle_id and b.bundle_revision=v_version.bundle_revision
      and b.state='COMMITTED') then
    qualification_state:='BUNDLE_PENDING';return next;return;end if;
  end if;
  select p.* into v_position from private.bpay_next_position p
    where p.work_id=v_version.canonical_work_id and p.component_key=v_version.canonical_component_key for share;
  if not found then qualification_state:='POSITION_PENDING';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    select pg_catalog.count(*) into v_count from private.bpay_next_publication p
      join private.bpay_next_work w on w.id=p.work_id
      join private.bpay_next_command c on c.id=p.command_id
      join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
      join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='POSITION_APPLY'
      join private.bpay_next_worker_control ctl on ctl.candidate_id=w.candidate_id
      join private.bpay_next_module_control module on module.id=1 and module.active_owner='NEXT'
      where p.command_id=v_version.publication_command_id_1 and p.work_id=v_version.approval_work_id
        and p.revision_id=v_version.approval_revision_id and p.candidate_id=v_version.candidate_id
        and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
        and w.current_revision_id=p.revision_id and w.applied_revision_id=p.revision_id
        and c.id=private.bpay_next_source_command_id_v1(v_version.initial_root_authorisation_id)
        and c.command_kind='POSITION_APPLY' and c.status='COMPLETE' and c.module_epoch=module.owner_epoch
        and c.expected_member_count=1 and c.enrolled_member_count=1 and c.sealed_at_utc is not null
        and j.status='DONE' and j.phase='DONE' and j.module_epoch=c.module_epoch
        and j.command_sequence=c.agency_sequence and j.owner_epoch>0 and j.owner_epoch<=ctl.active_owner_epoch
        and j.lease_nonce is null and j.lease_until_utc is null
        and v_position.applied_revision_id=p.revision_id;
  else
  select pg_catalog.count(*) into v_count from private.bpay_next_source_binding_members_v1(v_version.source_receipt_id) m
    join private.bpay_next_publication p on p.command_id=m.publication_command_id
    join private.bpay_next_work w on w.id=m.work_id
    where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
      and w.current_revision_id=m.revision_id and w.applied_revision_id=m.revision_id
      and ((m.publication_command_id=v_version.publication_command_id_1
            and m.work_id=v_version.publication_work_id_1 and m.revision_id=v_version.publication_revision_id_1)
        or (m.publication_command_id=v_version.publication_command_id_2
            and m.work_id=v_version.publication_work_id_2 and m.revision_id=v_version.publication_revision_id_2));
  end if;
  if v_count is distinct from (case when v_version.publication_command_id_2 is null then 1 else 2 end) then
    qualification_state:='PUBLICATION_PENDING';return next;return;end if;
  if v_position.source_book_state is distinct from 'READY' then qualification_state:='BOOK_UNBOUND';return next;return;end if;
  if v_position.source_book_revision is distinct from p_expected_book_revision then
    qualification_state:='BOOK_REVISION_STALE';return next;return;end if;
  if not private.bpay_next_source_comparison_book_certified_v2(v_position) and (v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
     or (v_position.source_book_seed_channel='PAYE' and
       (v_position.native_realised_paye is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_paye is distinct from v_position.held_source_ex_vat))
     or (v_position.source_book_seed_channel='UMBRELLA' and
       (v_position.native_realised_umbrella is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_umbrella is distinct from v_position.held_source_ex_vat)) ) then
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

create or replace function private.bpay_next_source_binding_members_snapshot_v2(p_receipt_id uuid)
returns table(candidate_id uuid,member_no integer,head_id uuid,prior_head_id uuid,
  work_id uuid,revision_id uuid,publication_command_id uuid,predecessor_revision_id uuid)
language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_receipt private.weekly_source_entitlement_publication_receipts%rowtype;
  v_n integer; v_i integer; v_row record;
begin
  select r.* into strict v_receipt from private.weekly_source_entitlement_publication_receipts r
    where r.id=p_receipt_id;
  v_n:=pg_catalog.cardinality(v_receipt.head_ids);
  if v_n not between 1 and 2 or not exists (
    select 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_receipt.decision_bundle_id and b.bundle_revision=v_receipt.bundle_revision
      and b.candidate_id=v_receipt.candidate_id and b.state in ('PROPOSED','COMMITTED')
      and b.request_digest=v_receipt.request_digest and b.decision_id=v_receipt.decision_id
      and b.decided_by_user_id=v_receipt.decided_by_user_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_BINDING_RECEIPT_NOT_QUALIFIED';
  end if;
  for v_i in 1..v_n loop
    select h.id,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id into v_row
      from public.weekly_source_entitlement_heads h
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=v_receipt.head_ids[v_i] and h.state='COMMITTED_CURRENT'
        and h.candidate_id=v_receipt.candidate_id and h.root_timesheet_id=v_receipt.member_root_ids[v_i]
        and h.root_family_booking_id=v_receipt.member_family_booking_ids[v_i]
        and h.root_timesheet_version=v_receipt.member_root_versions[v_i]
        and h.decision_bundle_id=v_receipt.decision_bundle_id and h.bundle_revision=v_receipt.bundle_revision
        and h.publication_receipt_digest=v_receipt.request_digest
        and h.scope_change_tx_token=v_receipt.scope_change_tx_token;
    if not found then
      raise exception using errcode='55000',message='BPAY_NEXT_BINDING_MEMBER_NOT_CURRENT';
    end if;
    candidate_id:=v_receipt.candidate_id; member_no:=v_i; head_id:=v_row.id;
    prior_head_id:=v_row.prior_head_id; work_id:=v_row.work_id; revision_id:=v_row.revision_id;
    publication_command_id:=v_row.command_id; predecessor_revision_id:=v_row.predecessor_revision_id;
    return next;
  end loop;
end
$function$;

create or replace function private.bpay_next_initial_tsfin_certificate_snapshot_v2(p_root_authorisation_id uuid)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  a public.weekly_source_root_authorisations%rowtype;
  t public.timesheets%rowtype;f public.timesheets_financials%rowtype;
  w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  p private.bpay_next_publication%rowtype;c public.contracts%rowtype;
  inventory jsonb;basis jsonb;origin jsonb;details jsonb;n integer;epoch bigint;
begin
  if p_root_authorisation_id is null then
    raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  select m.owner_epoch into epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT';
  if epoch is null then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select x.* into strict a from public.weekly_source_root_authorisations x where x.id=p_root_authorisation_id;
  select x.* into strict t from public.timesheets x where x.timesheet_id=a.root_timesheet_id;
  select x.* into strict c from public.contracts x where x.id=t.contract_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.source_event_id=a.id;
  select x.* into strict w from private.bpay_next_work x where x.id=r.work_id;
  select x.* into strict f from public.timesheets_financials x where x.id=r.financial_snapshot_id;
  select x.* into strict p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(a.id);
  if a.withdrawn_at_utc is not null or a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(t.booking_id))
     or t.is_current is distinct from true or t.authorised_at_server is null
     or t.revoked_at is not null or t.archived_at_utc is not null
     or (a.family_booking_id,a.timesheet_version) is distinct from (t.booking_id,t.version)
     or (w.work_kind,w.candidate_id,w.contract_id,w.original_timesheet_id,w.booking_id,w.week_ending_date,w.current_revision_id)
       is distinct from ('SOURCE'::text,c.candidate_id,c.id,t.timesheet_id,t.booking_id,t.week_ending_date,r.id)
     or w.approval_state is distinct from 'APPROVED'
     or (r.source_kind,r.source_head_id,r.source_event_id,r.physical_timesheet_id,r.physical_timesheet_version,r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,a.id,t.timesheet_id,t.version,t.week_ending_date)
     or r.approved_at_utc is null or r.sealed_at_utc is null
     or f.is_current is distinct from true or f.is_stale is distinct from false or f.authorised_at_utc is null
     or (f.timesheet_id,f.timesheet_version,f.candidate_id,f.client_id)
       is distinct from (t.timesheet_id,t.version,w.candidate_id,c.client_id)
     or (p.work_id,p.revision_id,p.candidate_id,p.revision_no)
       is distinct from (w.id,r.id,w.candidate_id,r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=w.candidate_id
       where command.id=p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and p.status='QUEUED' and p.phase='NEW' and p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception 'BPAY_NEXT_INITIAL_AUTH_NOT_CURRENT' using errcode='55000';end if;
  inventory:=private.weekly_source_effective_inventory_v1(t.timesheet_id);
  basis:=inventory->'approval_basis';origin:=basis->'origin';
  details:=jsonb_build_object('segments',f.invoice_breakdown_json->'segments','additional_units',f.additional_units_json,
    'actual_schedule',f.actual_schedule_json,'rate_source_refs',f.rate_source_refs_json);
  if inventory->>'ok' is distinct from 'true' or inventory->>'authority' is distinct from 'TSFIN'
     or inventory->>'head_id' is not null or jsonb_typeof(inventory->'components') is distinct from 'array'
     or jsonb_typeof(basis) is distinct from 'object' or basis->'coverage_complete' is distinct from 'true'::jsonb
     or basis is distinct from private.weekly_source_inventory_approval_basis_v2(t.timesheet_id,inventory)
     or origin is distinct from jsonb_build_object('kind','INITIAL_AUTHORISED_TSFIN_V1',
       'root_authorisation_id',a.id,'financial_snapshot_id',f.id,'authorisation_generation',a.authorisation_generation,
       'root_timesheet_id',t.timesheet_id,'root_version',t.version::text,'authorised_row_signature',a.authorised_row_signature,
       'financial_snapshot_digest',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',
         jsonb_build_object('policy',f.policy_snapshot_json,'details',details,'components',inventory->'components')),'hex'),
       'inventory_digest',basis->>'inventory_digest','entitlement_digest',basis->>'entitlement_digest')
     or basis->>'detail_digest' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',details),'hex')
     or basis->>'policy_fingerprint' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',f.policy_snapshot_json),'hex')
     or r.source_inventory_digest is distinct from decode(basis->>'inventory_digest','hex')
     or r.approved_source_ex_vat is distinct from (basis->>'approved_pay_ex_vat')::numeric
     or r.expected_line_count is distinct from jsonb_array_length(inventory->'components')
  then raise exception 'BPAY_NEXT_INITIAL_I1_CERTIFICATE_REQUIRED' using errcode='55000';end if;
  select count(*) into n from private.bpay_next_approved_line l where l.revision_id=r.id;
  if n<>r.expected_line_count or exists(select 1 from jsonb_array_elements(inventory->'components') component
      left join private.bpay_next_approved_line l on l.revision_id=r.id
        and l.source_component_id=(component.value->>'component_id')::uuid
        and l.component_key='SOURCE:'||(component.value->>'component_id')
      where l.id is null or l.line_no is distinct from (component.value->>'component_ordinal')::integer
        or l.evidence_ref is distinct from 'source-component:'||(component.value->>'component_id')||'#'||(component.value->>'component_sha256')
        or l.source_pay_ex_vat is distinct from case when (component.value->>'exclude_from_pay')::boolean then 0 else (component.value->>'pay_ex_vat')::numeric end
        or l.component_kind is distinct from case component.value->>'component_kind' when 'WORKED_TIME' then 'WORK' when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end
        or l.tax_treatment is distinct from case when component.value->>'component_kind'='WORKED_TIME' then 'TAXABLE'::text else null::text end)
  then raise exception 'BPAY_NEXT_INITIAL_SEALED_COMPONENTS_MISMATCH' using errcode='23514';end if;
  return jsonb_build_object('candidate_id',w.candidate_id,'work_id',w.id,'revision_id',r.id,'command_id',p.command_id,
    'source_channel',r.source_pay_channel,'origin',origin,'basis',basis,'components',inventory->'components',
    'certificate_digest',encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis),'hex'));
end
$function$;

create or replace function private.bpay_next_initial_tsfin_bound_assert_snapshot_v2(p_binding_version_id uuid)
returns void language plpgsql stable security definer set search_path=pg_catalog,private,public
as $function$
declare v private.bpay_next_source_component_binding_version%rowtype;l private.bpay_next_approved_line%rowtype;
  cert jsonb;origin jsonb;basis jsonb;component jsonb;
begin
  select x.* into strict v from private.bpay_next_source_component_binding_version x where x.id=p_binding_version_id;
  if v.origin_kind is distinct from 'INITIAL_AUTHORISED_TSFIN_V1' then
    raise exception 'BPAY_NEXT_INITIAL_ORIGIN_REQUIRED' using errcode='23514';end if;
  cert:=private.bpay_next_initial_tsfin_certificate_snapshot_v2(v.initial_root_authorisation_id);origin:=cert->'origin';basis:=cert->'basis';
  select value into strict component from jsonb_array_elements(cert->'components') x(value)
    where (value->>'component_id')::uuid=v.source_component_id;
  select x.* into strict l from private.bpay_next_approved_line x
    where x.revision_id=(cert->>'revision_id')::uuid and x.source_component_id=v.source_component_id;
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
       is distinct from (l.id,l.id,l.component_key,l.component_key,cert->>'source_channel',l.tax_treatment,
         (cert->>'work_id')::uuid,(cert->>'revision_id')::uuid)
     or v.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v.canonical_work_id,
       v.canonical_component_key,v.approval_revision_id,v.approved_line_id,v.initial_root_authorisation_id,
       cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'))
  then raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
end
$function$;

create or replace function private.bpay_next_bound_source_snapshot_read_v2(p_binding_version_id uuid,p_expected_book_revision bigint)
returns table(qualification_state text,selection_state text,candidate_id uuid,
  canonical_work_id uuid,canonical_component_key text,binding_version_id uuid,source_book_revision bigint,
  approval_work_id uuid,approval_revision_id uuid,approved_line_id uuid,head_id uuid,source_receipt_id uuid,
  source_channel text,tax_treatment text,classification_line_id uuid,valuation_policy_id uuid,
  approved_source_ex_vat private.bpay_next_penny_amount,
  native_realised_paye private.bpay_next_penny_amount,native_realised_umbrella private.bpay_next_penny_amount,
  native_held_paye private.bpay_next_penny_amount,native_held_umbrella private.bpay_next_penny_amount)
language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare v_version private.bpay_next_source_component_binding_version%rowtype;
  v_binding private.bpay_next_source_component_binding%rowtype;
  v_position private.bpay_next_position%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_count integer;
begin
  if p_binding_version_id is null or p_expected_book_revision is null or p_expected_book_revision<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_BOUND_SNAPSHOT_ID_REQUIRED';end if;
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT';
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=p_binding_version_id;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_version.candidate_id;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED';end if;
  select b.* into strict v_binding from private.bpay_next_source_component_binding b
    where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key;
  if v_binding.current_binding_version_id is distinct from p_binding_version_id then
    qualification_state:='SUPERSEDED';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    perform private.bpay_next_initial_tsfin_bound_assert_snapshot_v2(v_version.id);
  else
  if not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_version.decision_bundle_id and b.bundle_revision=v_version.bundle_revision
      and b.state='COMMITTED') then
    qualification_state:='BUNDLE_PENDING';return next;return;end if;
  end if;
  select p.* into v_position from private.bpay_next_position p
    where p.work_id=v_version.canonical_work_id and p.component_key=v_version.canonical_component_key;
  if not found then qualification_state:='POSITION_PENDING';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    select pg_catalog.count(*) into v_count from private.bpay_next_publication p
      join private.bpay_next_work w on w.id=p.work_id
      join private.bpay_next_command c on c.id=p.command_id
      join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
      join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='POSITION_APPLY'
      join private.bpay_next_worker_control ctl on ctl.candidate_id=w.candidate_id
      join private.bpay_next_module_control module on module.id=1 and module.active_owner='NEXT'
      where p.command_id=v_version.publication_command_id_1 and p.work_id=v_version.approval_work_id
        and p.revision_id=v_version.approval_revision_id and p.candidate_id=v_version.candidate_id
        and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
        and w.current_revision_id=p.revision_id and w.applied_revision_id=p.revision_id
        and c.id=private.bpay_next_source_command_id_v1(v_version.initial_root_authorisation_id)
        and c.command_kind='POSITION_APPLY' and c.status='COMPLETE' and c.module_epoch=module.owner_epoch
        and c.expected_member_count=1 and c.enrolled_member_count=1 and c.sealed_at_utc is not null
        and j.status='DONE' and j.phase='DONE' and j.module_epoch=c.module_epoch
        and j.command_sequence=c.agency_sequence and j.owner_epoch>0 and j.owner_epoch<=ctl.active_owner_epoch
        and j.lease_nonce is null and j.lease_until_utc is null
        and v_position.applied_revision_id=p.revision_id;
  else
  select pg_catalog.count(*) into v_count from private.bpay_next_source_binding_members_snapshot_v2(v_version.source_receipt_id) m
    join private.bpay_next_publication p on p.command_id=m.publication_command_id
    join private.bpay_next_work w on w.id=m.work_id
    where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
      and w.current_revision_id=m.revision_id and w.applied_revision_id=m.revision_id
      and ((m.publication_command_id=v_version.publication_command_id_1
            and m.work_id=v_version.publication_work_id_1 and m.revision_id=v_version.publication_revision_id_1)
        or (m.publication_command_id=v_version.publication_command_id_2
            and m.work_id=v_version.publication_work_id_2 and m.revision_id=v_version.publication_revision_id_2));
  end if;
  if v_count is distinct from (case when v_version.publication_command_id_2 is null then 1 else 2 end) then
    qualification_state:='PUBLICATION_PENDING';return next;return;end if;
  if v_position.source_book_state is distinct from 'READY' then qualification_state:='BOOK_UNBOUND';return next;return;end if;
  if v_position.source_book_revision is distinct from p_expected_book_revision then
    qualification_state:='BOOK_REVISION_STALE';return next;return;end if;
  if not private.bpay_next_source_comparison_book_certified_v2(v_position) and (v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
     or (v_position.source_book_seed_channel='PAYE' and
       (v_position.native_realised_paye is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_paye is distinct from v_position.held_source_ex_vat))
     or (v_position.source_book_seed_channel='UMBRELLA' and
       (v_position.native_realised_umbrella is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_umbrella is distinct from v_position.held_source_ex_vat)) ) then
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

create or replace function private.bpay_next_source_comparison_expired_case_scope_v2(
  p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null
) returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_preparation_expiry_worker x on x.job_id=j.id and x.command_id=j.command_id and x.candidate_id=j.candidate_id
    join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
    join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_EXPIRY'
      and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
    join private.bpay_next_job original on original.id=x.original_job_id and original.command_id=q.original_prepare_command_id
      and original.candidate_id=x.candidate_id and original.module_epoch=j.module_epoch and original.job_kind='PREPARE' and original.status='DONE'
    join private.bpay_next_pay_run r on r.id=x.run_id
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_position canonical on canonical.work_id=b.work_id and canonical.component_key=b.component_key
    join private.bpay_next_source_comparison_receipt comparison on comparison.id=canonical.source_comparison_receipt_id
      and comparison.candidate_id=b.candidate_id and comparison.canonical_work_id=b.work_id and comparison.canonical_component_key=b.component_key
      and comparison.work_collection_id=b.id and comparison.case_id=b.case_id and comparison.case_component_id=b.case_component_id
      and private.bpay_next_source_comparison_book_certified_v2(canonical)
    join private.bpay_next_case_component component on component.id=b.case_component_id and component.case_id=b.case_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.preparation_revision=x.preparation_revision
      and i.case_component_id=b.case_component_id and i.case_id=b.case_id and i.work_collection_id=b.id
      and i.selection_revision=x.selection_revision
      and ((i.source_comparison_receipt_id is null and i.valuation_policy_id=b.original_valuation_policy_id)
        or exists(select 1 from private.bpay_next_source_comparison_receipt input where input.id=i.source_comparison_receipt_id
          and input.candidate_id=b.candidate_id and input.canonical_work_id=b.work_id and input.canonical_component_key=b.component_key
          and input.comparison_policy_id=i.valuation_policy_id))
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.candidate_id=x.candidate_id and s.preparation_revision=x.preparation_revision and s.selection_revision=x.selection_revision
      and s.job_id=x.original_job_id and s.pass_kind='DRAFT' and s.projection_no=0
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id
      and h.run_worker_id=w.id and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and j.job_kind='PREPARATION_EXPIRY' and j.status='LEASED' and j.phase='RELEASE'
      and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      and x.status='RELEASING' and x.stage='CASE' and x.expected_case_hold_count>0
      and x.released_case_hold_count<x.expected_case_hold_count
      and q.status='CANCELLING' and q.accepted_at_utc>=q.captured_deadline
      and q.captured_deadline=r.preparation_expires_at_utc and r.preparation_expires_at_utc=r.created_at_utc+interval '72 hours'
      and r.status='CANCELLING' and r.confirmed_at_utc is null and r.review_revision=q.selection_review_revision+1
      and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
        is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
      and (command.expected_member_count,command.enrolled_member_count)
        is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
      and q.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=r.id)
      and w.status='CANCELLING' and w.preparation_revision=x.preparation_revision and w.case_selection_revision=x.selection_revision
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and i.case_kind='OVERPAYMENT' and i.tax_treatment='TAXABLE' and i.source_pay_channel=component.source_pay_channel and i.target_pay_channel in ('PAYE','UMBRELLA')
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and i.payroll_stage='GROSS_DEDUCT' and i.hold_purpose='GROSS_RECOVERY'
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=q.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
        and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE'))
$function$;

alter function private.bpay_next_source_book_guard_v1() owner to postgres;
alter function private.bpay_next_bound_source_snapshot_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_source_book_guard_v1(),private.bpay_next_bound_source_snapshot_v1(uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_source_binding_members_snapshot_v2(uuid) owner to postgres;
revoke all on function private.bpay_next_source_binding_members_snapshot_v2(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_certificate_snapshot_v2(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_snapshot_v2(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_bound_assert_snapshot_v2(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_bound_assert_snapshot_v2(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_bound_source_snapshot_read_v2(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_bound_source_snapshot_read_v2(uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_source_comparison_expired_case_scope_v2(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_comparison_expired_case_scope_v2(uuid,uuid,uuid) from public,anon,authenticated,service_role;
commit;
