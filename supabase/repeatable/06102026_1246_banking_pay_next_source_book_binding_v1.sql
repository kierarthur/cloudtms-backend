-- Private receipt-derived SOURCE identity and once-only cache seed authority.
-- Not a publisher, repricer, selector, financial posting RPC or activation seam.

\set ON_ERROR_STOP on

begin;

-- Exact staged receipt membership; no arbitrary WORK/head IDs supplied by caller.
-- The lockset can run before publication/clock allocation, while bind requires
-- the subsequently sealed revision and real queued publication tuple.
create or replace function private.bpay_next_source_binding_members_v1(p_receipt_id uuid)
returns table(candidate_id uuid,member_no integer,head_id uuid,prior_head_id uuid,
  work_id uuid,revision_id uuid,publication_command_id uuid,predecessor_revision_id uuid)
language plpgsql security definer
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

-- Exact current/prior component census, including a first common HEAD that
-- removes a genuine initial-authorisation component before any binding exists.
-- This is one immediate accepted predecessor per member, not a history walk.
create or replace function private.bpay_next_source_binding_components_v1(p_receipt_id uuid)
returns table(source_component_id uuid)
language sql security definer
set search_path=pg_catalog,private,public
as $function$
  select c.component_id from private.bpay_next_source_binding_members_v1(p_receipt_id) m
    join public.weekly_source_entitlement_head_components c on c.head_id in (m.head_id,m.prior_head_id)
  union
  select l.source_component_id from private.bpay_next_source_binding_members_v1(p_receipt_id) m
    join private.bpay_next_approved_line l on l.revision_id=m.predecessor_revision_id
    where l.source_component_id is not null and l.component_key='SOURCE:'||l.source_component_id::text
  order by 1
$function$;

-- Acquires the complete direct canonical union BEFORE the existing publisher
-- takes its agency clock. Root must call this after staging all 1/2 members.
create or replace function private.bpay_next_source_binding_lockset_v1(p_receipt_id uuid)
returns table(candidate_id uuid,lock_kind text,work_id uuid,revision_id uuid,component_key text)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_candidate uuid; v_works uuid[]; v_ids uuid[]; v_keys text[]; v_row record;
begin
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED'; end if;
  select r.candidate_id into strict v_candidate from private.weekly_source_entitlement_publication_receipts r where r.id=p_receipt_id;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict do nothing;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select pg_catalog.array_agg('SOURCE:'||c.source_component_id::text order by c.source_component_id)
    into v_keys from private.bpay_next_source_binding_components_v1(p_receipt_id) c;
  select pg_catalog.array_agg(x.work_id order by x.work_id) into v_works from (
    select m.work_id from private.bpay_next_source_binding_members_v1(p_receipt_id) m
    union
    select b.canonical_work_id from private.bpay_next_source_component_binding b
    where b.candidate_id=v_candidate and b.source_component_id in (
      select c.source_component_id from private.bpay_next_source_binding_components_v1(p_receipt_id) c)
  ) x;
  for v_row in select w.id,w.current_revision_id,w.applied_revision_id from private.bpay_next_work w
    where w.id=any(v_works) and w.candidate_id=v_candidate order by w.id for update
  loop
    candidate_id:=v_candidate;lock_kind:='WORK';work_id:=v_row.id;revision_id:=null;component_key:=null;return next;
  end loop;
  select pg_catalog.array_agg(x.id order by x.id) into v_ids from (
    select m.revision_id as id from private.bpay_next_source_binding_members_v1(p_receipt_id) m
    union select m.predecessor_revision_id from private.bpay_next_source_binding_members_v1(p_receipt_id) m
    union select w.current_revision_id from private.bpay_next_work w where w.id=any(v_works)
    union select w.applied_revision_id from private.bpay_next_work w where w.id=any(v_works)
    union select b.canonical_revision_id from private.bpay_next_source_component_binding b
      where b.canonical_work_id=any(v_works) and b.canonical_component_key=any(v_keys)
  ) x where x.id is not null;
  for v_row in select r.work_id,r.id from private.bpay_next_work_revision r where r.id=any(v_ids) order by r.work_id,r.id for share loop
    candidate_id:=v_candidate;lock_kind:='REVISION';work_id:=v_row.work_id;revision_id:=v_row.id;component_key:=null;return next;
  end loop;
  for v_row in select p.work_id,p.component_key from private.bpay_next_position p
    where p.work_id=any(v_works) and p.component_key=any(v_keys)
    order by p.work_id,p.component_key collate "C" for update loop
    candidate_id:=v_candidate;lock_kind:='POSITION';work_id:=v_row.work_id;revision_id:=null;component_key:=v_row.component_key;return next;
  end loop;
end
$function$;

create or replace function private.bpay_next_source_component_bind_v1(p_receipt_id uuid,p_component_id uuid)
returns table(binding_version_id uuid,canonical_work_id uuid,canonical_component_key text,binding_revision bigint,qualification_state text)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_receipt private.weekly_source_entitlement_publication_receipts%rowtype;
  v_binding private.bpay_next_source_component_binding%rowtype;
  v_version private.bpay_next_source_component_binding_version%rowtype;
  v_members record; v_first record; v_second record; v_approval record; v_anchor record;
  v_line private.bpay_next_approved_line%rowtype;
  v_component public.weekly_source_entitlement_head_components%rowtype;
  v_count integer; v_no bigint; v_id uuid; v_digest bytea; v_detail bytea;
  v_key text:='SOURCE:'||p_component_id::text; v_class_line uuid; v_tax text; v_state text;
begin
  if p_receipt_id is null or p_component_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_BINDING_ID_REQUIRED';
  end if;
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select r.* into strict v_receipt from private.weekly_source_entitlement_publication_receipts r where r.id=p_receipt_id;
  -- Immutable accepted replay is independent of later HEAD currentness. It
  -- returns SUPERSEDED, never silently restores an earlier current pointer.
  select v.* into v_version from private.bpay_next_source_component_binding_version v
    where v.source_receipt_id=p_receipt_id and v.source_component_id=p_component_id;
  if found then
    select b.* into strict v_binding from private.bpay_next_source_component_binding b
      where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key;
    binding_version_id:=v_version.id;canonical_work_id:=v_version.canonical_work_id;
    canonical_component_key:=v_version.canonical_component_key;binding_revision:=v_version.binding_revision;
    qualification_state:=case when v_binding.current_binding_version_id=v_version.id then 'REGISTERED' else 'SUPERSEDED' end;
    return next;return;
  end if;
  perform * from private.bpay_next_source_binding_lockset_v1(p_receipt_id);
  select b.* into v_binding from private.bpay_next_source_component_binding b
    where b.candidate_id=v_receipt.candidate_id and b.source_component_id=p_component_id for update;
  for v_members in select m.* from private.bpay_next_source_binding_members_v1(p_receipt_id) m order by m.work_id loop
    if v_members.publication_command_id is null or not exists (
      select 1 from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id
      where r.id=v_members.revision_id and r.approved_at_utc is not null and r.sealed_at_utc is not null
        and w.current_revision_id=r.id) then
      qualification_state:='PUBLICATION_PENDING';return next;return;
    end if;
  end loop;
  select m.* into strict v_first from private.bpay_next_source_binding_members_v1(p_receipt_id) m order by m.work_id limit 1;
  select m.* into v_second from private.bpay_next_source_binding_members_v1(p_receipt_id) m order by m.work_id offset 1 limit 1;
  select pg_catalog.count(*) into v_count from public.weekly_source_entitlement_head_components c
    where c.head_id=any(v_receipt.head_ids) and c.component_id=p_component_id;
  if v_count=1 then
    select c.* into strict v_component from public.weekly_source_entitlement_head_components c
      where c.head_id=any(v_receipt.head_ids) and c.component_id=p_component_id;
    select m.* into strict v_approval from private.bpay_next_source_binding_members_v1(p_receipt_id) m where m.head_id=v_component.head_id;
    select l.* into strict v_line from private.bpay_next_approved_line l
      where l.revision_id=v_approval.revision_id and l.component_key=v_key and l.source_component_id=p_component_id;
    if v_line.evidence_ref is distinct from 'source-component:'||p_component_id::text||'#'||pg_catalog.encode(v_component.component_sha256,'hex')
       or v_line.source_pay_ex_vat is distinct from (case when v_component.exclude_from_pay then 0 else v_component.pay_ex_vat end) then
      raise exception using errcode='23514',message='BPAY_NEXT_BINDING_LINE_EVIDENCE_MISMATCH';
    end if;
    if v_component.component_kind='WORKED_TIME' and v_component.work_date is not null then
      select d.detail_sha256 into v_detail from private.bpay_next_source_chosen_detail d
        where d.head_id=v_component.head_id and d.component_id=p_component_id
          and d.decision_bundle_id=v_receipt.decision_bundle_id and d.bundle_revision=v_receipt.bundle_revision
          and d.component_sha256=v_component.component_sha256
          and d.detail_sha256=pg_catalog.sha256(pg_catalog.convert_to(d.detail_json::text,'UTF8'));
      if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_CHOSEN_DETAIL_REQUIRED'; end if;
    end if;
    v_state:='PRESENT';v_class_line:=v_line.id;v_tax:=v_line.tax_treatment;
  elsif v_count=0 then
    if v_binding.canonical_work_id is not null then
      select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=v_binding.current_binding_version_id;
      select m.* into v_approval from private.bpay_next_source_binding_members_v1(p_receipt_id) m
        where m.work_id=v_version.approval_work_id;
      if not found then
        select m.* into v_approval from private.bpay_next_source_binding_members_v1(p_receipt_id) m where m.work_id=v_binding.canonical_work_id;
      end if;
      if not found then qualification_state:='REMOVAL_ROOT_UNAVAILABLE';return next;return;end if;
      v_class_line:=v_version.classification_line_id;v_tax:=v_version.tax_treatment;
    else
      select pg_catalog.count(*) into v_count from private.bpay_next_source_binding_members_v1(p_receipt_id) m
        join private.bpay_next_approved_line l on l.revision_id=m.predecessor_revision_id
          and l.source_component_id=p_component_id and l.component_key=v_key;
      if v_count<>1 then qualification_state:='ORIGINAL_ANCHOR_UNAVAILABLE';return next;return;end if;
      select m.* into strict v_approval from private.bpay_next_source_binding_members_v1(p_receipt_id) m
        join private.bpay_next_approved_line l on l.revision_id=m.predecessor_revision_id
          and l.source_component_id=p_component_id and l.component_key=v_key;
      select l.id,l.tax_treatment into v_class_line,v_tax from private.bpay_next_approved_line l
        where l.revision_id=v_approval.predecessor_revision_id and l.source_component_id=p_component_id and l.component_key=v_key;
    end if;
    if exists(select 1 from private.bpay_next_approved_line l where l.revision_id=v_approval.revision_id and l.component_key=v_key) then
      raise exception using errcode='23514',message='BPAY_NEXT_BINDING_REMOVAL_LINE_CONFLICT';
    end if;
    v_state:='REMOVED';
  else
    qualification_state:='COMPONENT_AUTHORITY_UNAVAILABLE';return next;return;
  end if;
  if v_binding.canonical_work_id is null then
    -- At first adoption, only the receipt's exact immediate predecessors may
    -- establish an old original anchor. No revision/history traversal.
    select pg_catalog.count(*) into v_count from private.bpay_next_source_binding_members_v1(p_receipt_id) m
      join private.bpay_next_approved_line l on l.revision_id=m.predecessor_revision_id and l.source_component_id=p_component_id and l.component_key=v_key;
    if v_count=1 then
      if exists(select 1 from private.bpay_next_source_binding_members_v1(p_receipt_id) m
        join public.weekly_source_entitlement_head_components c on c.head_id=m.prior_head_id
        where c.component_id=p_component_id and c.movement_id is not null) then
        qualification_state:='ORIGINAL_ANCHOR_UNAVAILABLE';return next;return;
      end if;
      select m.work_id,l.revision_id,l.id into v_anchor from private.bpay_next_source_binding_members_v1(p_receipt_id) m
        join private.bpay_next_approved_line l on l.revision_id=m.predecessor_revision_id and l.source_component_id=p_component_id and l.component_key=v_key;
    elsif v_count=0 and v_component.movement_id is null then
      select v_approval.work_id as work_id,v_line.revision_id as revision_id,v_line.id as id into v_anchor;
    else qualification_state:='ORIGINAL_ANCHOR_UNAVAILABLE';return next;return;
    end if;
    v_no:=1;
  else
    select v_binding.canonical_work_id as work_id,v_binding.canonical_revision_id as revision_id,v_binding.canonical_line_id as id into v_anchor;
    v_no:=v_binding.binding_revision+1;
  end if;
  v_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(
    v_anchor.work_id,v_key,v_anchor.revision_id,v_anchor.id,v_receipt.id,v_receipt.candidate_id,p_component_id,
    v_receipt.decision_bundle_id,v_receipt.bundle_revision,pg_catalog.encode(v_receipt.request_digest,'hex'),
    v_approval.work_id,v_approval.revision_id,v_line.id,v_approval.head_id,v_state,v_class_line,v_tax,
    v_component.component_sha256,v_detail,v_first.publication_command_id,v_first.work_id,v_first.revision_id,
    case when v_second is null then null else v_second.publication_command_id end,
    case when v_second is null then null else v_second.work_id end,
    case when v_second is null then null else v_second.revision_id end)::text,'UTF8'));
  select v.* into v_version from private.bpay_next_source_component_binding_version v
    where v.source_receipt_id=p_receipt_id and v.source_component_id=p_component_id;
  if found then
    if v_version.binding_digest is distinct from v_digest then
      raise exception using errcode='23514',message='BPAY_NEXT_BINDING_REPLAY_CONFLICT';
    end if;
    binding_version_id:=v_version.id;canonical_work_id:=v_version.canonical_work_id;
    canonical_component_key:=v_version.canonical_component_key;binding_revision:=v_version.binding_revision;
    qualification_state:=case when v_binding.current_binding_version_id=v_version.id then 'REGISTERED' else 'SUPERSEDED' end;
    return next;return;
  end if;
  v_id:=pg_catalog.gen_random_uuid();
  insert into private.bpay_next_source_component_binding_version
    (id,canonical_work_id,canonical_component_key,candidate_id,source_component_id,binding_revision,
     source_receipt_id,decision_bundle_id,bundle_revision,request_digest,approval_work_id,approval_revision_id,
     approval_component_key,approved_line_id,head_id,component_state,approval_source_channel,tax_treatment,
     classification_line_id,component_sha256,chosen_detail_sha256,
     publication_command_id_1,publication_work_id_1,publication_revision_id_1,
     publication_command_id_2,publication_work_id_2,publication_revision_id_2,binding_digest)
    select v_id,v_anchor.work_id,v_key,v_receipt.candidate_id,p_component_id,v_no,
     v_receipt.id,v_receipt.decision_bundle_id,v_receipt.bundle_revision,v_receipt.request_digest,
     v_approval.work_id,v_approval.revision_id,v_key,v_line.id,v_approval.head_id,v_state,r.source_pay_channel,v_tax,
     v_class_line,v_component.component_sha256,v_detail,
     v_first.publication_command_id,v_first.work_id,v_first.revision_id,
     case when v_second is null then null else v_second.publication_command_id end,
     case when v_second is null then null else v_second.work_id end,
     case when v_second is null then null else v_second.revision_id end,v_digest
    from private.bpay_next_work_revision r where r.id=v_approval.revision_id;
  insert into private.bpay_next_source_component_binding
    (canonical_work_id,canonical_component_key,candidate_id,source_component_id,canonical_revision_id,canonical_line_id,current_binding_version_id,binding_revision)
    values(v_anchor.work_id,v_key,v_receipt.candidate_id,p_component_id,v_anchor.revision_id,v_anchor.id,v_id,v_no)
    on conflict on constraint bpay_next_source_component_binding_pkey do update
      set current_binding_version_id=excluded.current_binding_version_id,binding_revision=excluded.binding_revision;
  binding_version_id:=v_id;canonical_work_id:=v_anchor.work_id;canonical_component_key:=v_key;
  binding_revision:=v_no;qualification_state:='REGISTERED';return next;
end
$function$;

-- Point witnesses certify the original basis. They do NOT assert that one
-- effect/hold equals the aggregate scalar, and never SUM operational history.
create or replace function private.bpay_next_source_book_seed_proof_v1(
  p_work_id uuid,p_component_key text,p_expected_applied_revision uuid,
  p_seed_effect_id uuid,p_seed_hold_id uuid)
returns table(seed_kind text,basis_revision_id uuid,basis_line_id uuid,
  source_channel text,realised_source private.bpay_next_penny_amount,
  held_source private.bpay_next_penny_amount,seed_digest bytea)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_position private.bpay_next_position%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_hold private.bpay_next_hold%rowtype;
  v_paid_hold private.bpay_next_hold%rowtype;v_frozen private.bpay_next_run_line%rowtype;
begin
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id;
  select p.* into strict v_position from private.bpay_next_position p where p.work_id=p_work_id and p.component_key=p_component_key;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=p_expected_applied_revision and r.work_id=p_work_id;
  if v_work.work_kind is distinct from 'SOURCE'
     or v_work.current_revision_id is distinct from p_expected_applied_revision
     or v_work.applied_revision_id is distinct from p_expected_applied_revision
     or v_position.applied_revision_id is distinct from p_expected_applied_revision
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
    select l.* into v_line from private.bpay_next_approved_line l
      where l.revision_id=p_expected_applied_revision and l.component_key=p_component_key and l.source_component_id is not null;
    if not found then
      select l.* into v_line from private.bpay_next_source_component_binding b
        join private.bpay_next_approved_line l on l.id=b.canonical_line_id and l.revision_id=b.canonical_revision_id
        where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key;
      if not found or v_revision.source_head_id is null or not exists (
        select 1 from public.weekly_source_entitlement_heads h
          join private.weekly_source_entitlement_publication_receipts r on h.id=any(r.head_ids)
            and r.request_digest=h.publication_receipt_digest and r.decision_bundle_id=h.decision_bundle_id
            and r.bundle_revision=h.bundle_revision
        where h.id=v_revision.source_head_id and h.state='COMMITTED_CURRENT'
          and not exists(select 1 from public.weekly_source_entitlement_head_components c
            where c.head_id=h.id and c.component_id=v_line.source_component_id)) then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ZERO_COMPONENT_UNAVAILABLE';end if;
    end if;
    -- ZERO describes the maintained book, not the absence of older children.
    basis_revision_id:=v_line.revision_id;basis_line_id:=v_line.id;seed_kind:='ZERO';
  else seed_kind:='ORIGINAL_POSITION';end if;
  if v_line.component_key is distinct from p_component_key or v_line.source_component_id is null
     or p_component_key is distinct from 'SOURCE:'||v_line.source_component_id::text then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_COMPONENT_MISMATCH';
  end if;
  seed_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(
    p_work_id,p_component_key,p_expected_applied_revision,basis_revision_id,basis_line_id,source_channel,
    realised_source,held_source,p_seed_effect_id,p_seed_hold_id,seed_kind)::text,'UTF8'));
  return next;
end
$function$;

create or replace function private.bpay_next_source_disposition_seed_v1(
  p_work_id uuid,p_component_key text,p_expected_applied_revision uuid,
  p_seed_effect_id uuid default null,p_seed_hold_id uuid default null)
returns table(qualification_state text,source_book_revision bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_candidate uuid;v_position private.bpay_next_position%rowtype;v_proof record;
begin
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
    if v_position.source_book_seed_applied_revision_id is distinct from p_expected_applied_revision
       or v_position.source_book_seed_effect_id is distinct from p_seed_effect_id
       or v_position.source_book_seed_hold_id is distinct from p_seed_hold_id then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_SEED_REPLAY_CONFLICT';end if;
    qualification_state:='READY';source_book_revision:=v_position.source_book_revision;return next;return;
  end if;
  select p.* into strict v_proof from private.bpay_next_source_book_seed_proof_v1(
    p_work_id,p_component_key,p_expected_applied_revision,p_seed_effect_id,p_seed_hold_id) p;
  update private.bpay_next_position p set source_book_state='READY',source_book_revision=1,
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

-- Until the connected real hold/effect owner is installed, this once-seeded
-- cache is immutable. Old source/target columns remain freely handled by their
-- existing owners; a later change causes snapshot BOOK_POSTING_REQUIRED rather
-- than trapping an old child's release/settlement or presenting a stale quote.
create or replace function private.bpay_next_source_book_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_proof record;
begin
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
drop trigger if exists bpay_next_source_book_guard_v1 on private.bpay_next_position;
create trigger bpay_next_source_book_guard_v1 before insert or update on private.bpay_next_position
  for each row execute function private.bpay_next_source_book_guard_v1();

create or replace function private.bpay_next_source_binding_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  if tg_op in ('DELETE','TRUNCATE') or tg_table_name='bpay_next_source_component_binding_version' then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BINDING_IMMUTABLE';end if;
  if (pg_catalog.to_jsonb(new)-array['current_binding_version_id','binding_revision']) is distinct from
     (pg_catalog.to_jsonb(old)-array['current_binding_version_id','binding_revision'])
     or new.binding_revision is distinct from old.binding_revision+1
     or new.current_binding_version_id=old.current_binding_version_id
     or not exists(select 1 from private.bpay_next_source_component_binding_version v
       where v.id=new.current_binding_version_id and v.canonical_work_id=new.canonical_work_id
         and v.canonical_component_key=new.canonical_component_key and v.candidate_id=new.candidate_id
         and v.binding_revision=new.binding_revision) then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BINDING_ADVANCE_CONFLICT';end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_source_binding_guard_v1 on private.bpay_next_source_component_binding;
create trigger bpay_next_source_binding_guard_v1 before update or delete on private.bpay_next_source_component_binding
  for each row execute function private.bpay_next_source_binding_guard_v1();
drop trigger if exists bpay_next_source_binding_truncate_v1 on private.bpay_next_source_component_binding;
create trigger bpay_next_source_binding_truncate_v1 before truncate on private.bpay_next_source_component_binding
  for each statement execute function private.bpay_next_source_binding_guard_v1();
drop trigger if exists bpay_next_source_binding_version_guard_v1 on private.bpay_next_source_component_binding_version;
create trigger bpay_next_source_binding_version_guard_v1 before update or delete on private.bpay_next_source_component_binding_version
  for each row execute function private.bpay_next_source_binding_guard_v1();
drop trigger if exists bpay_next_source_binding_version_truncate_v1 on private.bpay_next_source_component_binding_version;
create trigger bpay_next_source_binding_version_truncate_v1 before truncate on private.bpay_next_source_component_binding_version
  for each statement execute function private.bpay_next_source_binding_guard_v1();

-- This typed snapshot admits identity and native book data only. It is NOT
-- permission to use AUTO or a manual selection; selection_state stays UNBOUND.
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
  if v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
     or (v_position.source_book_seed_channel='PAYE' and
       (v_position.native_realised_paye is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_paye is distinct from v_position.held_source_ex_vat))
     or (v_position.source_book_seed_channel='UMBRELLA' and
       (v_position.native_realised_umbrella is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_umbrella is distinct from v_position.held_source_ex_vat)) then
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

alter function private.bpay_next_source_binding_members_v1(uuid) owner to postgres;
alter function private.bpay_next_source_binding_components_v1(uuid) owner to postgres;
alter function private.bpay_next_source_binding_lockset_v1(uuid) owner to postgres;
alter function private.bpay_next_source_component_bind_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_source_book_guard_v1() owner to postgres;
alter function private.bpay_next_source_binding_guard_v1() owner to postgres;
alter function private.bpay_next_bound_source_snapshot_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_source_binding_members_v1(uuid),
  private.bpay_next_source_binding_components_v1(uuid),
  private.bpay_next_source_binding_lockset_v1(uuid),private.bpay_next_source_component_bind_v1(uuid,uuid),
  private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid),
  private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid),
  private.bpay_next_source_book_guard_v1(),private.bpay_next_source_binding_guard_v1(),
  private.bpay_next_bound_source_snapshot_v1(uuid,bigint) from public,anon,authenticated,service_role;

commit;
