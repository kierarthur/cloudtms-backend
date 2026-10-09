-- F-25 exact accepted-input authority. No current-pointer adoption, fixtures,
-- second command clock, execution epoch amendment or external action.

\set ON_ERROR_STOP on

begin;

-- This current-origin reader is called ONLY during pre-clock capture. Delayed
-- preparation reads the retained result, never this reader or a new live gate.
create or replace function private.bpay_next_acceptance_source_proof_v1(p_work_id uuid,p_revision_id uuid)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  p private.bpay_next_publication%rowtype;c private.bpay_next_command%rowtype;
  binding private.bpay_next_source_component_binding_version%rowtype;
  receipt private.bpay_next_protected_source_receipt%rowtype;
  epoch bigint;cert jsonb;v_lane jsonb;members jsonb:='[]'::jsonb;bindings jsonb:='[]'::jsonb;origin jsonb;
  line private.bpay_next_approved_line%rowtype;member record;
begin
  select m.owner_epoch into epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  select x.* into strict w from private.bpay_next_work x where x.id=p_work_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=w.id;
  select x.* into strict p from private.bpay_next_publication x where x.work_id=w.id and x.revision_id=r.id;
  select x.* into strict c from private.bpay_next_command x where x.id=p.command_id;
  if epoch is null or w.work_kind<>'SOURCE' or w.approval_state<>'APPROVED' or w.current_revision_id<>r.id
    or r.source_kind not in ('SOURCE','PROTECTED') or r.source_event_id is null
    or r.approved_at_utc is null or r.sealed_at_utc is null or r.week_ending_date<>w.week_ending_date
    or (p.candidate_id,p.revision_no) is distinct from (w.candidate_id,r.revision_no)
    or c.id<>private.bpay_next_source_command_id_v1(r.source_event_id)
    or c.command_kind<>'POSITION_APPLY' or c.module_epoch<>epoch or c.status not in ('SEALED','COMPLETE')
    or c.sealed_at_utc is null or not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=c.id and cm.candidate_id=w.candidate_id)
    or not exists(select 1 from public.timesheets t join public.contracts contract on contract.id=t.contract_id
      where t.timesheet_id=r.physical_timesheet_id and t.version=r.physical_timesheet_version
        and t.booking_id=w.booking_id and t.contract_id=w.contract_id and contract.candidate_id=w.candidate_id
        and t.week_ending_date=w.week_ending_date and t.is_current is true
        and t.revoked_at is null and t.archived_at_utc is null) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_ORIGIN_UNAVAILABLE';
  end if;
  if r.source_head_id is null and r.source_kind='SOURCE' then
    cert:=private.bpay_next_initial_tsfin_certificate_v1(r.source_event_id);
    origin:=pg_catalog.jsonb_build_object('origin_kind','INITIAL_AUTHORISED_TSFIN_V1',
      'certificate_origin',cert->'origin','certificate_basis',cert->'basis',
      'certificate_digest',cert->>'certificate_digest');
  elsif r.source_head_id is not null then
    if not exists(select 1 from public.weekly_source_entitlement_heads h
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      where h.id=r.source_head_id and h.id=r.source_event_id and h.state='COMMITTED_CURRENT'
        and h.authority_kind=case when r.source_kind='PROTECTED' then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end
        and h.root_timesheet_id=r.physical_timesheet_id and h.root_timesheet_version=r.physical_timesheet_version
        and h.root_family_booking_id=w.booking_id and h.candidate_id=w.candidate_id and h.contract_id=w.contract_id
        and h.week_ending_date=w.week_ending_date and h.committed_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_HEAD_UNAVAILABLE';
    end if;
    origin:=pg_catalog.jsonb_build_object('origin_kind','SOURCE_RECEIPT_V1','head_id',r.source_head_id);
  else
    select x.* into strict receipt from private.bpay_next_protected_source_receipt x where x.work_id=w.id and x.revision_id=r.id;
    if receipt.command_id<>p.command_id or receipt.approval_id<>r.source_event_id or receipt.agency_sequence<>c.agency_sequence then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_PROTECTED_RECEIPT_UNAVAILABLE';
    end if;
    v_lane:=private.weekly_source_pay_query_next_receipt_v1(r.physical_timesheet_id,receipt.approval_id,receipt.generation_id);
    if v_lane is null or (v_lane->>'lane',v_lane->>'receipt_state',v_lane->>'work_id',
        v_lane->>'revision_id',v_lane->>'command_id',v_lane->>'agency_sequence')
      is distinct from ('DIRECT_NEXT','COMPLETE',w.id::text,r.id::text,c.id::text,c.agency_sequence::text) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_PROTECTED_CANONICAL_RECEIPT_UNAVAILABLE';
    end if;
    origin:=pg_catalog.jsonb_build_object('origin_kind','DIRECT_NEXT_PROTECTED_V1',
      'approval_id',receipt.approval_id,'family_id',receipt.family_id,'canonical_receipt',v_lane);
  end if;
  -- Preserve each exact binding/member witness while it is still qualified.
  -- No SOURCE realised/held book or mutable accounting vector enters this proof.
  for line in select l.* from private.bpay_next_approved_line l where l.revision_id=r.id order by l.line_no loop
    select v.* into binding from private.bpay_next_source_component_binding_version v
      join private.bpay_next_source_component_binding b on b.current_binding_version_id=v.id
      where v.approval_work_id=w.id and v.approval_revision_id=r.id and v.approved_line_id=line.id
        and v.candidate_id=w.candidate_id and b.candidate_id=w.candidate_id;
    if found then
      if binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
        perform private.bpay_next_initial_tsfin_bound_assert_v1(binding.id);
      else
        members:='[]'::jsonb;
        for member in select m.* from private.bpay_next_source_binding_members_v1(binding.source_receipt_id) m order by m.member_no loop
          members:=members||pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(member));
        end loop;
        if not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
          where b.decision_bundle_id=binding.decision_bundle_id and b.bundle_revision=binding.bundle_revision
            and b.candidate_id=binding.candidate_id and b.state='COMMITTED' and b.request_digest=binding.request_digest) then
          raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_BUNDLE_PENDING';
        end if;
      end if;
      bindings:=bindings||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('binding_version_id',binding.id,
        'approved_line_id',line.id,'binding_sha256',pg_catalog.encode(binding.binding_digest,'hex'),
        'source_receipt_id',binding.source_receipt_id,'publication_members',
          case when binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then '[]'::jsonb else members end));
    elsif line.component_kind='WORK' and line.source_pay_ex_vat>0 then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_BINDING_UNAVAILABLE';
    end if;
  end loop;
  return origin||pg_catalog.jsonb_build_object('work_id',w.id,'revision_id',r.id,
    'command_id',c.id,'source_event_id',r.source_event_id,'source_head_id',r.source_head_id,'bindings',bindings);
end
$function$;

-- Shared trigger successor: canonical gate and genuine current origin are
-- certified here BEFORE PREPARE allocation. Their retained rows are consumed
-- afterward without a fresh Source read or current-head substitution.
create or replace function private.bpay_next_acceptance_stage_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_work private.bpay_next_accepted_work_input%rowtype;v_facts jsonb;v_origin jsonb;
begin
  if tg_when='AFTER' then
    if exists(select 1 from private.bpay_next_acceptance_stage s where s.preparing_transaction_id=new.preparing_transaction_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_NOT_CONSUMED';
    end if;
    return null;
  end if;
  if tg_op='INSERT' then
    if new.preparing_transaction_id is distinct from pg_catalog.pg_current_xact_id()
      or exists(select 1 from private.bpay_next_run_command rc where rc.run_id=new.run_id)
      or not exists(select 1 from private.bpay_next_pay_run r
        where r.id=new.run_id and r.status='PREPARING' and r.selection_state='SEALED') then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_OWNER_MISMATCH';
    end if;
    if new.record_kind='WORK' then
      v_work:=pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_work_input,new.record_json);
      if (new.entity_id,new.accepted_input_id,new.accepted_work_input_id,new.run_id)
        is distinct from (v_work.work_id,v_work.accepted_input_id,v_work.id,v_work.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTANCE_STAGE_WORK_MISMATCH';
      end if;
      if v_work.source_kind in ('SOURCE','PROTECTED') then
        v_facts:=private.weekly_source_pay_query_facts_v1(v_work.physical_timesheet_id);
        v_origin:=private.bpay_next_acceptance_source_proof_v1(v_work.work_id,v_work.effective_revision_id);
        if (v_facts->'ok'='true'::jsonb and v_facts->>'code'='OK' and v_facts->'blocked'='false'::jsonb) is not true
          or v_work.query_facts is distinct from v_facts or v_work.query_scope is distinct from v_facts->'scope'
          or v_work.origin_proof is distinct from v_origin then
          raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_QUERY_OR_ORIGIN_UNAVAILABLE';
        end if;
      end if;
    end if;
    return new;
  elsif tg_op='DELETE' then
    if old.preparing_transaction_id is distinct from pg_catalog.pg_current_xact_id()
      or not exists(select 1 from private.bpay_next_accepted_input i
        where i.id=old.accepted_input_id and i.run_id=old.run_id and i.prepare_command_id=old.prepare_command_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_OWNER_MISMATCH';
    end if;
    return old;
  end if;
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_STAGE_IMMUTABLE';
end
$function$;

create or replace function private.bpay_next_accepted_input_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private,public
as $function$
declare v_epoch bigint;v_channel text;v_company uuid;v_vat boolean;v_enabled boolean;
  v_floor numeric;v_pay_date date;v_policy uuid;v_selected_revision bigint;
  v_candidate_version uuid;v_umbrella_version uuid;v_policy_version uuid;v_raw_method text;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_EVIDENCE_IMMUTABLE';
  end if;
  -- Admission/row/FK lock closure belongs to SEAL_ENQUEUE before its clock.
  -- This INSERT guard checks exact authority; it acquires no business lock.
  select module.owner_epoch,upper(candidate.pay_method),candidate.umbrella_id,
      umbrella.vat_chargeable,umbrella.enabled,candidate.min_take_home_wtd,
      run.pay_date,policy.current_policy_id
    into v_epoch,v_channel,v_company,v_vat,v_enabled,v_floor,v_pay_date,v_policy
    from private.bpay_next_module_control module
    join private.bpay_next_command command on command.id=new.prepare_command_id
    join private.bpay_next_run_command rc on rc.command_id=command.id and rc.run_id=new.run_id
    join private.bpay_next_pay_run run on run.id=rc.run_id
    join private.bpay_next_selection_candidate selected on selected.run_id=run.id and selected.candidate_id=new.candidate_id
    join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=selected.candidate_id
      and member.member_no=selected.member_no
    join public.candidates candidate on candidate.id=selected.candidate_id
    left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
    join private.bpay_next_valuation_policy_control policy on policy.id=1
    where module.id=1 and module.active_owner='NEXT' and module.owner_epoch=new.module_epoch
      and command.module_epoch=new.module_epoch and command.agency_sequence=new.accepted_sequence
      and command.command_kind='PREPARE' and command.status='ENROLLING'
      and command.enrolled_member_count=0 and command.expected_member_count=run.selected_candidate_count
      and run.status='PREPARING' and run.selection_state='SEALED';
  if not found or (new.target_pay_channel,new.target_umbrella_id,new.umbrella_vat_chargeable,new.umbrella_enabled,
      new.candidate_floor_raw,new.pay_date,new.valuation_policy_id)
    is distinct from (v_channel,case when v_channel='UMBRELLA' then v_company else null end,
      case when v_channel='UMBRELLA' then v_vat else null end,
      case when v_channel='UMBRELLA' then v_enabled else null end,v_floor,v_pay_date,v_policy) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_INPUT_OWNER_MISMATCH';
  end if;
  if (new.floor_state,new.candidate_floor) is distinct from (
    case when v_floor is not null and v_floor::text not in ('NaN','Infinity','-Infinity')
      and v_floor>=0 and v_floor=pg_catalog.trunc(v_floor,2) then 'BOUND' else 'UNBOUND' end,
    case when v_floor is not null and v_floor::text not in ('NaN','Infinity','-Infinity')
      and v_floor>=0 and v_floor=pg_catalog.trunc(v_floor,2) then v_floor else null end) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_FLOOR_STATE_MISMATCH';
  end if;
  select c.pay_method into strict v_raw_method from public.candidates c where c.id=new.candidate_id;
  select v.event_id into v_candidate_version from private.bpay_next_candidate_input_version v
    join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
    where v.candidate_id=new.candidate_id and receipt.agency_sequence<new.accepted_sequence and receipt.module_epoch=new.module_epoch
    order by receipt.agency_sequence desc,v.event_id desc limit 1;
  if v_channel='UMBRELLA' then
    select v.event_id into v_umbrella_version from private.bpay_next_umbrella_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where v.umbrella_id=new.target_umbrella_id and receipt.agency_sequence<new.accepted_sequence and receipt.module_epoch=new.module_epoch
      order by receipt.agency_sequence desc,v.event_id desc limit 1;
  end if;
  select v.event_id into v_policy_version from private.bpay_next_policy_input_version v
    join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
    where receipt.agency_sequence<new.accepted_sequence and receipt.module_epoch=new.module_epoch order by receipt.agency_sequence desc,v.event_id desc limit 1;
  if (new.candidate_version_event_id,new.umbrella_version_event_id,new.policy_version_event_id)
      is distinct from (v_candidate_version,v_umbrella_version,v_policy_version)
    or (new.settings_witness_kind='BASELINE_AT_ACCEPTANCE') is distinct from (v_candidate_version is null)
    or (v_candidate_version is not null and not exists(select 1 from private.bpay_next_candidate_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where v.event_id=v_candidate_version and receipt.module_epoch=new.module_epoch
        and (v.new_pay_method,v.new_umbrella_id,v.new_floor) is not distinct from (v_raw_method,v_company,v_floor)))
    or (v_umbrella_version is not null and not exists(select 1 from private.bpay_next_umbrella_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where v.event_id=v_umbrella_version and receipt.module_epoch=new.module_epoch
        and (v.new_vat_chargeable,v.new_enabled) is not distinct from (v_vat,v_enabled)))
    or (v_policy_version is not null and not exists(select 1 from private.bpay_next_policy_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where v.event_id=v_policy_version and receipt.module_epoch=new.module_epoch and v.new_policy_id=v_policy)) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_VERSION_APPLICABILITY_UNAVAILABLE';
  end if;
  select s.selection_revision into v_selected_revision from private.bpay_next_case_selection s
    where s.run_id=new.run_id and s.candidate_id=new.candidate_id order by s.selection_revision desc limit 1;
  if (new.case_scope='NO_CASE' and (v_selected_revision is not null or
        exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=new.candidate_id)))
    or (new.case_scope='SEALED_SELECTION' and
      (v_selected_revision is distinct from new.case_selection_revision or not exists(
        select 1 from private.bpay_next_case_selection s where s.run_id=new.run_id and s.candidate_id=new.candidate_id
          and s.selection_revision=new.case_selection_revision and s.status='SEALED' and s.sealed_at_utc is not null))) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_CASE_SELECTION_UNAVAILABLE';
  end if;
  if new.evidence_sha256 is distinct from pg_catalog.sha256(pg_catalog.convert_to(
      pg_catalog.jsonb_build_array('BPAY_NEXT_ACCEPTED_INPUT_V1',new.run_id,new.prepare_command_id,new.candidate_id,
        new.module_epoch,new.accepted_sequence,new.target_pay_channel,new.target_umbrella_id,new.umbrella_vat_chargeable,
        new.umbrella_enabled,new.candidate_floor_raw,new.floor_state,new.candidate_floor,new.pay_date,new.pay_week_start,new.valuation_policy_id,
        new.case_scope,new.case_selection_revision,new.candidate_version_event_id,new.umbrella_version_event_id,
        new.policy_version_event_id,new.settings_witness_kind)::text,'UTF8')) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_INPUT_DIGEST_MISMATCH';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_accepted_work_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private,public
as $function$
declare v_input private.bpay_next_accepted_input%rowtype;
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_choice private.bpay_next_work_choice%rowtype;v_publication private.bpay_next_publication%rowtype;
  v_command private.bpay_next_command%rowtype;v_state jsonb;v_count bigint;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_EVIDENCE_IMMUTABLE';
  end if;
  select i.* into strict v_input from private.bpay_next_accepted_input i where i.id=new.accepted_input_id;
  select w.* into strict v_work from private.bpay_next_work w where w.id=new.work_id;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=new.effective_revision_id;
  select c.* into strict v_choice from private.bpay_next_work_choice c where c.run_id=new.run_id and c.work_id=new.work_id;
  select p.* into strict v_publication from private.bpay_next_publication p where p.command_id=new.publication_command_id;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_publication.command_id;
  if not exists(select 1 from private.bpay_next_acceptance_stage s
    where s.preparing_transaction_id=pg_catalog.pg_current_xact_id() and s.record_kind='WORK'
      and s.prepare_command_id=v_input.prepare_command_id and s.accepted_work_input_id=new.id
      and s.accepted_input_id=new.accepted_input_id and s.entity_id=new.work_id
      and s.record_json-'evidence_sha256'=pg_catalog.to_jsonb(new)-'evidence_sha256') then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_CAPTURE_MISSING';
  end if;
  if (new.run_id,new.candidate_id,new.work_id,new.original_revision_id,new.effective_revision_id,new.publication_command_id,
      new.predecessor_sequence,new.selection_mode,new.physical_timesheet_id,new.physical_timesheet_version,new.raw_booking_id,
      new.contract_id,new.week_ending_date,new.source_kind,new.source_event_id,new.source_head_id,new.financial_snapshot_id,
      new.valuation_policy_id)
    is distinct from (v_input.run_id,v_input.candidate_id,v_revision.work_id,v_choice.expected_revision_id,v_publication.revision_id,
      v_command.id,v_command.agency_sequence,v_choice.selection_mode,v_revision.physical_timesheet_id,
      v_revision.physical_timesheet_version,v_work.booking_id,v_work.contract_id,v_work.week_ending_date,
      v_revision.source_kind,v_revision.source_event_id,v_revision.source_head_id,v_revision.financial_snapshot_id,v_revision.valuation_policy_id)
    or v_work.candidate_id<>v_input.candidate_id or v_work.approval_state<>'APPROVED'
    or v_work.current_revision_id is distinct from v_revision.id or v_choice.selection_state<>'SEALED'
    or v_publication.candidate_id<>v_input.candidate_id or v_publication.work_id<>v_work.id
    or v_publication.revision_no<>v_revision.revision_no
    or v_command.command_kind<>'POSITION_APPLY' or v_command.module_epoch<>v_input.module_epoch
    or v_command.agency_sequence>=v_input.accepted_sequence
    or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
    or not exists(select 1 from public.contracts c where c.id=new.contract_id and c.candidate_id=new.candidate_id
      and c.client_id=new.client_id)
    or exists(select 1 from private.bpay_next_publication p join private.bpay_next_command c on c.id=p.command_id
      where p.work_id=new.work_id and c.agency_sequence>new.predecessor_sequence
        and c.agency_sequence<v_input.accepted_sequence) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_OWNER_MISMATCH';
  end if;
  select count(*) into v_count from private.bpay_next_approved_line l where l.revision_id=v_revision.id;
  if v_count<>v_revision.expected_line_count or new.selected_line_count is distinct from
      (case when v_choice.selection_mode='ALL' then v_count else v_choice.component_count end) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_MEMBER_COUNT_MISMATCH';
  end if;
  if v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED') then
    if (pg_catalog.jsonb_typeof(new.query_facts)='object'
      and new.query_facts ?& array['ok','code','scope','blocked','query_state_sha256','hours_incidents','manual_reviews']
      and new.query_facts-array['ok','code','scope','blocked','query_state_sha256','hours_incidents','manual_reviews']='{}'::jsonb
      and new.query_facts->'ok'='true'::jsonb and new.query_facts->>'code'='OK'
      and new.query_facts->'blocked'='false'::jsonb and new.query_disposition='CLEAR'
      and new.query_scope=new.query_facts->'scope'
      and new.query_facts->>'query_state_sha256' ~ '^[0-9a-f]{64}$'
      and pg_catalog.jsonb_typeof(new.query_facts->'hours_incidents')='array'
      and pg_catalog.jsonb_typeof(new.query_facts->'manual_reviews')='array') is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_QUERY_EVIDENCE_INVALID';
    end if;
    if (pg_catalog.jsonb_typeof(new.query_scope)='object' and new.query_scope ?&
      array['root_timesheet_id','family_booking_id','target_family_id','candidate_id','client_id',
        'contract_id','week_ending_date','root_version','family_bound_version'] and new.query_scope-
      array['root_timesheet_id','family_booking_id','target_family_id','candidate_id','client_id',
        'contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
      and new.query_scope->>'root_timesheet_id'=new.physical_timesheet_id::text
      and new.query_scope->>'root_version'=new.physical_timesheet_version::text
      and new.query_scope->>'family_booking_id'=new.raw_booking_id
      and new.query_scope->>'candidate_id'=new.candidate_id::text
      and new.query_scope->>'client_id'=new.client_id::text
      and new.query_scope->>'contract_id'=new.contract_id::text
      and new.query_scope->>'week_ending_date'=new.week_ending_date::text) is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_QUERY_SCOPE_MISMATCH';
    end if;
    v_state:=pg_catalog.jsonb_build_object('schema_version','PAY_QUERY_STATE_V3','scope',new.query_scope,
      'manual_reviews',new.query_facts->'manual_reviews','hours_incidents',new.query_facts->'hours_incidents');
    if new.query_state_sha256 is distinct from pg_catalog.decode(new.query_facts->>'query_state_sha256','hex')
      or new.query_state_sha256 is distinct from extensions.digest(pg_catalog.convert_to(v_state::text,'UTF8'),'sha256') then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_QUERY_DIGEST_MISMATCH';
    end if;
  elsif new.query_disposition is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_QUERY_UNEXPECTED';
  end if;
  if new.evidence_sha256 is distinct from pg_catalog.sha256(pg_catalog.convert_to(
      (pg_catalog.to_jsonb(new)-array['id','evidence_sha256'])::text,'UTF8')) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_DIGEST_MISMATCH';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_accepted_work_member_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_input private.bpay_next_accepted_work_input%rowtype;
  v_line private.bpay_next_approved_line%rowtype;v_original private.bpay_next_approved_line%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_expected_certificate jsonb;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_EVIDENCE_IMMUTABLE';
  end if;
  select i.* into strict v_input from private.bpay_next_accepted_work_input i where i.id=new.accepted_work_input_id;
  select l.* into strict v_line from private.bpay_next_approved_line l where l.id=new.effective_approved_line_id;
  if (new.run_id,new.work_id,new.original_revision_id,new.effective_revision_id,new.component_key,
      new.source_component_id,new.component_kind,new.work_date,new.tax_treatment)
    is distinct from (v_input.run_id,v_input.work_id,v_input.original_revision_id,v_line.revision_id,v_line.component_key,
      v_line.source_component_id,v_line.component_kind,v_line.work_date,v_line.tax_treatment)
    or v_line.revision_id<>v_input.effective_revision_id or new.member_no>v_input.selected_line_count then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_MEMBER_OWNER_MISMATCH';
  end if;
  if v_input.selection_mode='SUBSET' then
    if new.original_approved_line_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_MEMBER_LINEAGE_MISSING';
    end if;
    select l.* into strict v_original from private.bpay_next_approved_line l where l.id=new.original_approved_line_id;
    if not exists(select 1 from private.bpay_next_selected_component s where s.run_id=new.run_id
        and s.work_id=new.work_id and s.approved_line_id=new.original_approved_line_id
        and s.expected_revision_id=new.original_revision_id and s.component_key=new.component_key)
      or (v_original.revision_id,v_original.component_key,v_original.source_component_id,v_original.component_kind,
          v_original.tax_treatment,v_original.work_date)
        is distinct from (new.original_revision_id,v_line.component_key,v_line.source_component_id,v_line.component_kind,
          v_line.tax_treatment,v_line.work_date) then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_MEMBER_LINEAGE_CONFLICT';
    end if;
  elsif new.original_approved_line_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_ALL_MEMBER_SELECTION_CONFLICT';
  end if;
  if new.source_binding_version_id is not null then
    select v.* into strict v_binding from private.bpay_next_source_component_binding_version v
      where v.id=new.source_binding_version_id;
    if (v_binding.candidate_id,v_binding.approval_work_id,v_binding.approval_revision_id,v_binding.approved_line_id,
        v_binding.approval_component_key,v_binding.source_component_id,v_binding.binding_digest)
      is distinct from (v_input.candidate_id,v_input.work_id,new.effective_revision_id,new.effective_approved_line_id,
        new.component_key,new.source_component_id,new.source_binding_sha256)
      or not exists(select 1 from pg_catalog.jsonb_array_elements(v_input.origin_proof->'bindings') b(value)
        where b.value->>'binding_version_id'=v_binding.id::text
          and b.value->>'approved_line_id'=v_line.id::text
          and b.value->>'binding_sha256'=pg_catalog.encode(v_binding.binding_digest,'hex')) then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_BINDING_MISMATCH';
    end if;
    if v_binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
      v_expected_certificate:=pg_catalog.jsonb_build_object('candidate_id',v_input.candidate_id,'work_id',v_input.work_id,
        'revision_id',new.effective_revision_id,'command_id',v_input.publication_command_id,
        'origin',v_input.origin_proof->'certificate_origin','basis',v_input.origin_proof->'certificate_basis',
        'certificate_digest',v_input.origin_proof->>'certificate_digest',
        'component',pg_catalog.jsonb_build_object('component_id',v_binding.source_component_id,
          'component_sha256',pg_catalog.encode(v_binding.component_sha256,'hex')));
      if new.source_origin_certificate is distinct from v_expected_certificate
        or pg_catalog.decode(v_input.origin_proof->>'certificate_digest','hex') is distinct from v_binding.initial_certificate_digest
        or v_line.evidence_ref is distinct from 'source-component:'||v_binding.source_component_id::text||'#'||pg_catalog.encode(v_binding.component_sha256,'hex') then
        raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_INITIAL_CERTIFICATE_MISMATCH';
      end if;
    elsif new.source_origin_certificate is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_INITIAL_CERTIFICATE_UNEXPECTED';
    end if;
  elsif v_input.source_kind in ('SOURCE','PROTECTED') and new.component_kind='WORK' and v_line.source_pay_ex_vat>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_SOURCE_BINDING_UNAVAILABLE';
  elsif new.source_origin_certificate is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_SOURCE_CERTIFICATE_UNEXPECTED';
  end if;
  return new;
end
$function$;

-- One-time leased resolution after the known earlier Candidate publications.
-- It selects no current revision/head/query and changes no frozen evidence.
create or replace function private.bpay_next_seal_effective_work_cut_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_work_id uuid
) returns uuid language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_input private.bpay_next_accepted_input%rowtype;
  v_work_input private.bpay_next_accepted_work_input%rowtype;
  v_cut private.bpay_next_effective_work_cut%rowtype;v_module_epoch bigint;v_count bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 or p_work_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_EFFECTIVE_CUT_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_job.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED' or v_job.phase not in ('NEW','PINNING')
    or v_job.module_epoch<>v_module_epoch or v_job.owner_epoch<>p_owner_epoch
    or v_job.lease_nonce is distinct from p_lease_nonce or v_job.lease_until_utc is null
    or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
    or not exists(select 1 from private.bpay_next_worker_control c where c.candidate_id=v_job.candidate_id
      and c.active_owner_epoch=p_owner_epoch) then
    raise exception using errcode='55000',message='BPAY_NEXT_EFFECTIVE_CUT_LEASE_STALE';
  end if;
  select i.* into strict v_input from private.bpay_next_accepted_input i
    join private.bpay_next_pay_run run on run.id=i.run_id
    join private.bpay_next_run_command rc on rc.run_id=run.id and rc.command_id=i.prepare_command_id
    join private.bpay_next_command command on command.id=rc.command_id
    join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=i.candidate_id
    where i.prepare_command_id=v_job.command_id and i.candidate_id=v_job.candidate_id
      and i.accepted_sequence=v_job.command_sequence and i.module_epoch=v_module_epoch
      and command.agency_sequence=i.accepted_sequence and command.module_epoch=i.module_epoch
      and command.command_kind='PREPARE' and command.status='SEALED'
      and command.enrolled_member_count=command.expected_member_count and command.sealed_at_utc is not null
      and command.expected_member_count=run.selected_candidate_count
      and run.selection_state='SEALED' and run.status='PREPARING';
  if exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=v_job.candidate_id
      and earlier.command_sequence<v_job.command_sequence and earlier.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  select i.* into strict v_work_input from private.bpay_next_accepted_work_input i
    where i.accepted_input_id=v_input.id and i.work_id=p_work_id;
  if not exists(select 1 from private.bpay_next_publication p
      join private.bpay_next_command c on c.id=p.command_id
      join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=v_input.candidate_id
        and j.job_kind='POSITION_APPLY'
      where p.command_id=v_work_input.publication_command_id and p.work_id=p_work_id
        and p.candidate_id=v_input.candidate_id and p.revision_id=v_work_input.effective_revision_id
        and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
        and c.agency_sequence=v_work_input.predecessor_sequence and c.agency_sequence<v_input.accepted_sequence
        and c.command_kind='POSITION_APPLY' and c.module_epoch=v_input.module_epoch
        and c.status='COMPLETE' and c.expected_member_count=c.enrolled_member_count and c.sealed_at_utc is not null
        and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
        and j.status='DONE' and j.phase='DONE' and j.lease_nonce is null and j.lease_until_utc is null) then
    raise exception using errcode='55000',message='BPAY_NEXT_EFFECTIVE_CUT_PREDECESSOR_PENDING';
  end if;
  select count(*) into v_count from private.bpay_next_accepted_work_member m
    where m.accepted_work_input_id=v_work_input.id;
  if v_count<>v_work_input.selected_line_count or (v_work_input.selection_mode='ALL' and exists(
      select 1 from private.bpay_next_approved_line l where l.revision_id=v_work_input.effective_revision_id
        and not exists(select 1 from private.bpay_next_accepted_work_member m
          where m.accepted_work_input_id=v_work_input.id and m.effective_approved_line_id=l.id))) then
    raise exception using errcode='23514',message='BPAY_NEXT_EFFECTIVE_CUT_MEMBER_SEAL_MISMATCH';
  end if;
  select c.* into v_cut from private.bpay_next_effective_work_cut c
    where c.accepted_input_id=v_input.id and c.work_id=p_work_id;
  if found then
    if (v_cut.accepted_work_input_id,v_cut.candidate_id,v_cut.effective_revision_id,v_cut.prepare_job_id,
        v_cut.publication_command_id,v_cut.predecessor_sequence,v_cut.selected_line_count,v_cut.evidence_sha256)
      is distinct from (v_work_input.id,v_input.candidate_id,v_work_input.effective_revision_id,p_job_id,
        v_work_input.publication_command_id,v_work_input.predecessor_sequence,v_work_input.selected_line_count,v_work_input.evidence_sha256) then
      raise exception using errcode='23514',message='BPAY_NEXT_EFFECTIVE_CUT_REPLAY_CONFLICT';
    end if;
    return v_cut.id;
  end if;
  insert into private.bpay_next_effective_work_cut(accepted_input_id,accepted_work_input_id,candidate_id,work_id,
    effective_revision_id,prepare_job_id,publication_command_id,predecessor_sequence,selected_line_count,evidence_sha256)
    values(v_input.id,v_work_input.id,v_input.candidate_id,p_work_id,v_work_input.effective_revision_id,p_job_id,
      v_work_input.publication_command_id,v_work_input.predecessor_sequence,v_work_input.selected_line_count,v_work_input.evidence_sha256)
    returning id into v_cut.id;
  return v_cut.id;
end
$function$;

drop trigger if exists bpay_next_accepted_input_insert_guard_v1 on private.bpay_next_accepted_input;
create trigger bpay_next_accepted_input_insert_guard_v1 before insert on private.bpay_next_accepted_input
  for each row execute function private.bpay_next_accepted_input_guard_v1();
drop trigger if exists bpay_next_accepted_work_insert_guard_v1 on private.bpay_next_accepted_work_input;
create trigger bpay_next_accepted_work_insert_guard_v1 before insert on private.bpay_next_accepted_work_input
  for each row execute function private.bpay_next_accepted_work_guard_v1();
drop trigger if exists bpay_next_accepted_member_insert_guard_v1 on private.bpay_next_accepted_work_member;
create trigger bpay_next_accepted_member_insert_guard_v1 before insert on private.bpay_next_accepted_work_member
  for each row execute function private.bpay_next_accepted_work_member_guard_v1();

alter function private.bpay_next_accepted_input_guard_v1() owner to postgres;
alter function private.bpay_next_acceptance_source_proof_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_acceptance_stage_guard_v1() owner to postgres;
alter function private.bpay_next_accepted_work_guard_v1() owner to postgres;
alter function private.bpay_next_accepted_work_member_guard_v1() owner to postgres;
alter function private.bpay_next_seal_effective_work_cut_v1(uuid,uuid,bigint,uuid) owner to postgres;
revoke all on function private.bpay_next_acceptance_source_proof_v1(uuid,uuid),private.bpay_next_acceptance_stage_guard_v1(),
  private.bpay_next_accepted_input_guard_v1(),private.bpay_next_accepted_work_guard_v1(),
  private.bpay_next_accepted_work_member_guard_v1(),private.bpay_next_seal_effective_work_cut_v1(uuid,uuid,bigint,uuid)
  from public,anon,authenticated,service_role;

commit;
