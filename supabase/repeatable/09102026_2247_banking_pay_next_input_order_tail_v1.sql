-- F-25 operational terminal clock ownership and selected-only collection
-- qualification successors. Retained migration/function predecessors kept.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_receive_command_v1(
  p_command_id uuid,
  p_command_kind text
)
returns bigint
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_existing_sequence bigint;
  v_existing_kind text;
  v_existing_epoch bigint;
  v_active_owner text;
  v_module_epoch bigint;
  v_input_phase text;
begin
  if p_command_id is null or p_command_kind is null
     or pg_catalog.char_length(p_command_kind) not between 1 and 64 then
    raise exception using errcode='22023', message='BPAY_NEXT_COMMAND_INPUT_INVALID';
  end if;

  select active_owner,owner_epoch into strict v_active_owner,v_module_epoch
    from private.bpay_next_module_control where id=1 for share;
  if v_active_owner <> 'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;

  -- Terminal operational ownership is private, not a caller-resettable GUC.
  insert into private.bpay_next_input_transaction(preparing_transaction_id,module_epoch,phase)
    values(pg_catalog.pg_current_xact_id(),v_module_epoch,'OPEN')
    on conflict(preparing_transaction_id) do nothing;
  select t.phase into strict v_input_phase from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.module_epoch=v_module_epoch;
  if exists(select 1 from private.bpay_next_input_version_event e
      where e.preparing_transaction_id=pg_catalog.pg_current_xact_id()
        and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY')
        and not exists(select 1 from private.bpay_next_input_version_receipt r where r.event_id=e.id))
      and not (v_input_phase='FINALISING' and p_command_kind='INPUT_VERSION') then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_VERSION_PENDING';
  end if;
  if v_input_phase='FINALISING' and p_command_kind<>'INPUT_VERSION' then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_FINALISER_COMMAND_INVALID';
  end if;
  if p_command_kind='INPUT_VERSION' and not exists(select 1 from private.bpay_next_input_version_event e
    where e.id=p_command_id and e.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and e.module_epoch=v_module_epoch and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY')
      and v_input_phase='FINALISING') then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_VERSION_COMMAND_OWNER_INVALID';
  end if;
  if v_input_phase='OPEN' then
    update private.bpay_next_input_transaction t set phase='CLOCK_SEALED',clock_sealed_at_utc=pg_catalog.clock_timestamp()
      where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.phase='OPEN';
  end if;
  select last_sequence into strict v_last
    from private.bpay_next_command_clock where id=1 for update;
  select agency_sequence,command_kind,module_epoch
    into v_existing_sequence,v_existing_kind,v_existing_epoch
    from private.bpay_next_command where id=p_command_id;
  if found then
    if v_existing_kind<>p_command_kind or v_existing_epoch<>v_module_epoch then
      raise exception using errcode='23514', message='BPAY_NEXT_COMMAND_REPLAY_KIND_CONFLICT';
    end if;
    return v_existing_sequence;
  end if;

  if v_last=9223372036854775807 then
    raise exception using errcode='22003', message='BPAY_NEXT_COMMAND_SEQUENCE_EXHAUSTED';
  end if;
  update private.bpay_next_command_clock
    set last_sequence=v_last+1 where id=1;
  insert into private.bpay_next_command
    (id,agency_sequence,module_epoch,command_kind,status)
    values (p_command_id,v_last+1,v_module_epoch,p_command_kind,'RECEIVED');
  return v_last+1;
end
$function$;

create or replace function private.bpay_next_stage_acceptance_v1(p_run_id uuid,p_command_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_input private.bpay_next_accepted_input%rowtype;
  v_work_input private.bpay_next_accepted_work_input%rowtype;
  v_member private.bpay_next_accepted_work_member%rowtype;
  v_case_input private.bpay_next_accepted_case_input%rowtype;
  v_selected private.bpay_next_selection_candidate%rowtype;
  v_selection private.bpay_next_run_selection%rowtype;
  v_candidate public.candidates%rowtype;v_umbrella public.umbrellas%rowtype;
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_choice private.bpay_next_work_choice%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_case_selection private.bpay_next_case_selection%rowtype;
  v_case_item private.bpay_next_case_selection_item%rowtype;
  v_command private.bpay_next_command%rowtype;v_publication private.bpay_next_publication%rowtype;
  v_epoch bigint;v_policy uuid;v_count bigint;v_member_no bigint;v_original_line uuid;
begin
  if p_run_id is null or p_command_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_ACCEPTANCE_INPUT_REQUIRED';
  end if;
  if exists(select 1 from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.phase<>'OPEN') then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_TRANSACTION_NOT_FRESH';
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select m.owner_epoch into strict v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
    or exists(select 1 from private.bpay_next_run_command rc where rc.run_id=p_run_id)
    or exists(select 1 from private.bpay_next_input_version_event e
      where e.preparing_transaction_id=pg_catalog.pg_current_xact_id())
    or exists(select 1 from private.bpay_next_acceptance_stage s
      where s.preparing_transaction_id=pg_catalog.pg_current_xact_id()) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_TRANSACTION_NOT_FRESH';
  end if;
  -- Exact member/control locks protect queued case/week owners. These are
  -- acquired before WORK/revision and before the single PREPARE clock tail.
  insert into private.bpay_next_worker_control(candidate_id)
    select s.candidate_id from private.bpay_next_selection_candidate s where s.run_id=p_run_id
    order by s.candidate_id on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control o
    join private.bpay_next_selection_candidate s on s.candidate_id=o.candidate_id
    where s.run_id=p_run_id order by o.candidate_id for update of o;
  select p.current_policy_id into strict v_policy from private.bpay_next_valuation_policy_control p
    where p.id=1 for share;
  perform 1 from private.bpay_next_valuation_policy p where p.id=v_policy for key share;
  for v_selected in select s.* from private.bpay_next_selection_candidate s
    where s.run_id=p_run_id order by s.candidate_id for key share
  loop
    select c.* into strict v_candidate from public.candidates c where c.id=v_selected.candidate_id for share;
    v_umbrella:=null;
    if upper(v_candidate.pay_method)='UMBRELLA' then
      select u.* into strict v_umbrella from public.umbrellas u where u.id=v_candidate.umbrella_id for share;
      if v_umbrella.enabled is not true or v_umbrella.vat_chargeable is null then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_UMBRELLA_UNAVAILABLE';
      end if;
    elsif upper(v_candidate.pay_method) is distinct from 'PAYE' then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_CHANNEL_UNSUPPORTED';
    end if;
    -- Earlier CASE/rule/week commands cannot be mistaken for empty facts.
    -- Known POSITION_APPLY predecessors remain accepted and resolve at PIN.
    if exists(select 1 from private.bpay_next_command_member m
        join private.bpay_next_command c on c.id=m.command_id
        left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=m.candidate_id
          and j.command_sequence=c.agency_sequence and j.module_epoch=c.module_epoch
        where m.candidate_id=v_selected.candidate_id and c.command_kind<>'POSITION_APPLY'
          and (j.id is null or j.status<>'DONE')) then
      raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_EARLIER_ECONOMIC_COMMAND_PENDING';
    end if;
    v_input:=null;
    v_input.id:=pg_catalog.gen_random_uuid();v_input.run_id:=p_run_id;
    v_input.prepare_command_id:=p_command_id;v_input.candidate_id:=v_selected.candidate_id;
    v_input.module_epoch:=v_epoch;v_input.accepted_sequence:=0;
    v_input.target_pay_channel:=upper(v_candidate.pay_method);
    v_input.target_umbrella_id:=case when v_input.target_pay_channel='UMBRELLA' then v_candidate.umbrella_id else null end;
    v_input.umbrella_vat_chargeable:=case when v_input.target_pay_channel='UMBRELLA' then v_umbrella.vat_chargeable else null end;
    v_input.umbrella_enabled:=case when v_input.target_pay_channel='UMBRELLA' then v_umbrella.enabled else null end;
    v_input.candidate_floor_raw:=v_candidate.min_take_home_wtd;
    v_input.floor_state:=case when v_input.candidate_floor_raw is not null
      and v_input.candidate_floor_raw::text not in ('NaN','Infinity','-Infinity')
      and v_input.candidate_floor_raw>=0 then 'BOUND' else 'UNBOUND' end;
    v_input.candidate_floor:=case when v_input.floor_state='BOUND' then v_input.candidate_floor_raw else null end;
    v_input.pay_date:=v_run.pay_date;
    v_input.pay_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
    v_input.valuation_policy_id:=v_policy;v_input.captured_at_utc:=pg_catalog.clock_timestamp();
    select v.event_id into v_input.candidate_version_event_id from private.bpay_next_candidate_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where v.candidate_id=v_input.candidate_id and receipt.module_epoch=v_epoch
      order by receipt.agency_sequence desc,v.event_id desc limit 1;
    if v_input.target_pay_channel='UMBRELLA' then
      select v.event_id into v_input.umbrella_version_event_id from private.bpay_next_umbrella_input_version v
        join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
        where v.umbrella_id=v_input.target_umbrella_id and receipt.module_epoch=v_epoch
        order by receipt.agency_sequence desc,v.event_id desc limit 1;
    end if;
    select v.event_id into v_input.policy_version_event_id from private.bpay_next_policy_input_version v
      join private.bpay_next_input_version_receipt receipt on receipt.event_id=v.event_id
      where receipt.module_epoch=v_epoch order by receipt.agency_sequence desc,v.event_id desc limit 1;
    v_input.settings_witness_kind:=case when v_input.candidate_version_event_id is null
      then 'BASELINE_AT_ACCEPTANCE' else 'ORDERED_VERSION' end;
    perform 1 from private.bpay_next_input_version_event e
      where e.id in (v_input.candidate_version_event_id,v_input.umbrella_version_event_id,v_input.policy_version_event_id)
      order by e.id for key share;
    -- Accepted-child FKs reference these typed rows, not just event parents.
    -- Complete their RI lock closure before PREPARE's agency clock.
    perform 1 from private.bpay_next_candidate_input_version v
      where v.event_id=v_input.candidate_version_event_id and v.candidate_id=v_input.candidate_id for key share;
    perform 1 from private.bpay_next_umbrella_input_version v
      where v.event_id=v_input.umbrella_version_event_id and v.umbrella_id=v_input.target_umbrella_id for key share;
    perform 1 from private.bpay_next_policy_input_version v
      where v.event_id=v_input.policy_version_event_id and v.new_policy_id=v_input.valuation_policy_id for key share;
    select s.* into v_case_selection from private.bpay_next_case_selection s
      where s.run_id=p_run_id and s.candidate_id=v_input.candidate_id
      order by s.selection_revision desc limit 1 for share;
    if found then
      if v_case_selection.status<>'SEALED' or v_case_selection.sealed_at_utc is null then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_CASE_SELECTION_UNSEALED';
      end if;
      v_input.case_scope:='SEALED_SELECTION';v_input.case_selection_revision:=v_case_selection.selection_revision;
    else
      if exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_input.candidate_id) then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_CASE_SELECTION_REQUIRED';
      end if;
      v_input.case_scope:='NO_CASE';v_input.case_selection_revision:=null;
    end if;
    insert into private.bpay_next_acceptance_stage(preparing_transaction_id,run_id,prepare_command_id,
      record_kind,entity_id,accepted_input_id,record_json)
      values(pg_catalog.pg_current_xact_id(),p_run_id,p_command_id,'CANDIDATE',v_input.candidate_id,
        v_input.id,pg_catalog.to_jsonb(v_input));
    for v_selection in select s.* from private.bpay_next_run_selection s
      where s.run_id=p_run_id and s.candidate_id=v_input.candidate_id order by s.work_id for key share
    loop
      select w.* into strict v_work from private.bpay_next_work w where w.id=v_selection.work_id for share;
      select c.* into strict v_choice from private.bpay_next_work_choice c
        where c.run_id=p_run_id and c.work_id=v_work.id for key share;
      select r.* into strict v_revision from private.bpay_next_work_revision r
        where r.id=v_work.current_revision_id and r.work_id=v_work.id for key share;
      select p.* into strict v_publication from private.bpay_next_publication p
        where p.work_id=v_work.id and p.revision_id=v_revision.id for key share;
      select c.* into strict v_command from private.bpay_next_command c where c.id=v_publication.command_id for key share;
      if v_work.candidate_id<>v_input.candidate_id or v_work.approval_state<>'APPROVED'
        or v_choice.selection_state<>'SEALED' or v_choice.expected_revision_id is null
        or v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null
        or v_command.command_kind<>'POSITION_APPLY' or v_command.module_epoch<>v_epoch
        or v_command.status not in ('SEALED','COMPLETE') then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_WORK_UNAVAILABLE';
      end if;
      perform 1 from private.bpay_next_work_revision r where r.id=v_choice.expected_revision_id for key share;
      perform 1 from public.contracts c where c.id=v_work.contract_id and c.candidate_id=v_input.candidate_id for key share;
      select count(*) into v_count from private.bpay_next_approved_line l where l.revision_id=v_revision.id;
      if v_count<>v_revision.expected_line_count then
        raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTANCE_APPROVED_LINE_SEAL_MISMATCH';
      end if;
      v_work_input:=null;v_work_input.id:=pg_catalog.gen_random_uuid();
      v_work_input.accepted_input_id:=v_input.id;v_work_input.run_id:=p_run_id;
      v_work_input.candidate_id:=v_input.candidate_id;v_work_input.work_id:=v_work.id;
      v_work_input.original_revision_id:=v_choice.expected_revision_id;v_work_input.effective_revision_id:=v_revision.id;
      v_work_input.publication_command_id:=v_command.id;v_work_input.predecessor_sequence:=v_command.agency_sequence;
      v_work_input.selection_mode:=v_choice.selection_mode;
      v_work_input.selected_line_count:=case when v_choice.selection_mode='ALL' then v_count else v_choice.component_count end;
      v_work_input.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_work_input.physical_timesheet_version:=v_revision.physical_timesheet_version;
      v_work_input.raw_booking_id:=v_work.booking_id;v_work_input.contract_id:=v_work.contract_id;
      select c.client_id into strict v_work_input.client_id from public.contracts c where c.id=v_work.contract_id;
      v_work_input.week_ending_date:=v_work.week_ending_date;v_work_input.source_kind:=v_revision.source_kind;
      v_work_input.source_event_id:=v_revision.source_event_id;v_work_input.source_head_id:=v_revision.source_head_id;
      v_work_input.financial_snapshot_id:=v_revision.financial_snapshot_id;
      v_work_input.valuation_policy_id:=v_revision.valuation_policy_id;
      perform 1 from public.clients c where c.id=v_work_input.client_id for key share;
      perform 1 from public.timesheets t where t.timesheet_id=v_work_input.physical_timesheet_id for key share;
      perform 1 from public.timesheets_financials f where f.id=v_work_input.financial_snapshot_id for key share;
      perform 1 from private.bpay_next_valuation_policy p where p.id=v_work_input.valuation_policy_id for key share;
      if v_work_input.source_kind in ('SOURCE','PROTECTED') then
        v_work_input.query_facts:=private.weekly_source_pay_query_facts_v1(v_work_input.physical_timesheet_id);
        if (v_work_input.query_facts->'ok'='true'::jsonb and v_work_input.query_facts->>'code'='OK'
          and v_work_input.query_facts->'blocked'='false'::jsonb) is not true then
          raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_QUERY_NOT_CLEAR';
        end if;
        v_work_input.query_disposition:='CLEAR';v_work_input.query_scope:=v_work_input.query_facts->'scope';
        v_work_input.query_state_sha256:=pg_catalog.decode(v_work_input.query_facts->>'query_state_sha256','hex');
        v_work_input.origin_proof:=private.bpay_next_acceptance_source_proof_v1(v_work.id,v_revision.id);
      else
        v_work_input.origin_proof:=pg_catalog.jsonb_build_object('origin_kind','IMMUTABLE_APPROVED_REVISION_V1',
          'work_id',v_work.id,'revision_id',v_revision.id,'command_id',v_command.id);
      end if;
      v_work_input.evidence_sha256:=pg_catalog.sha256(pg_catalog.convert_to(
        (pg_catalog.to_jsonb(v_work_input)-array['id','evidence_sha256'])::text,'UTF8'));
      insert into private.bpay_next_acceptance_stage(preparing_transaction_id,run_id,prepare_command_id,
        record_kind,entity_id,accepted_input_id,accepted_work_input_id,record_json)
        values(pg_catalog.pg_current_xact_id(),p_run_id,p_command_id,'WORK',v_work.id,v_input.id,
          v_work_input.id,pg_catalog.to_jsonb(v_work_input));
      v_member_no:=0;
      for v_line in select l.* from private.bpay_next_approved_line l where l.revision_id=v_revision.id
        and (v_choice.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component s
          where s.run_id=p_run_id and s.work_id=v_work.id and s.component_key=l.component_key))
        order by l.line_no for key share
      loop
        v_original_line:=null;
        if v_choice.selection_mode='SUBSET' then
          select s.approved_line_id into strict v_original_line from private.bpay_next_selected_component s
            join private.bpay_next_approved_line old_line on old_line.id=s.approved_line_id
            where s.run_id=p_run_id and s.work_id=v_work.id and s.expected_revision_id=v_choice.expected_revision_id
              and (old_line.component_key,old_line.source_component_id,old_line.component_kind,old_line.work_date,old_line.tax_treatment)
                is not distinct from (v_line.component_key,v_line.source_component_id,v_line.component_kind,v_line.work_date,v_line.tax_treatment)
            for key share of s,old_line;
        end if;
        v_member:=null;v_member_no:=v_member_no+1;
        v_member.accepted_work_input_id:=v_work_input.id;v_member.run_id:=p_run_id;v_member.work_id:=v_work.id;
        v_member.original_revision_id:=v_choice.expected_revision_id;v_member.original_approved_line_id:=v_original_line;
        v_member.effective_revision_id:=v_revision.id;v_member.effective_approved_line_id:=v_line.id;
        v_member.component_key:=v_line.component_key;v_member.source_component_id:=v_line.source_component_id;
        v_member.component_kind:=v_line.component_kind;v_member.work_date:=v_line.work_date;
        v_member.tax_treatment:=v_line.tax_treatment;v_member.member_no:=v_member_no;
        if v_work_input.source_kind in ('SOURCE','PROTECTED') then
          select b.* into v_binding from private.bpay_next_source_component_binding_version b
            where b.approved_line_id=v_line.id and exists(select 1 from
              pg_catalog.jsonb_array_elements(v_work_input.origin_proof->'bindings') x(value)
              where x.value->>'binding_version_id'=b.id::text) for key share;
          if found then
            v_member.source_binding_version_id:=v_binding.id;v_member.source_binding_sha256:=v_binding.binding_digest;
            if v_binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
              v_member.source_origin_certificate:=pg_catalog.jsonb_build_object('candidate_id',v_input.candidate_id,
                'work_id',v_work.id,'revision_id',v_revision.id,'command_id',v_command.id,
                'origin',v_work_input.origin_proof->'certificate_origin','basis',v_work_input.origin_proof->'certificate_basis',
                'certificate_digest',v_work_input.origin_proof->>'certificate_digest',
                'component',pg_catalog.jsonb_build_object('component_id',v_binding.source_component_id,
                  'component_sha256',pg_catalog.encode(v_binding.component_sha256,'hex')));
            end if;
          elsif v_line.component_kind='WORK' and v_line.source_pay_ex_vat>0 then
            raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SOURCE_MEMBER_UNBOUND';
          end if;
        end if;
        insert into private.bpay_next_acceptance_stage(preparing_transaction_id,run_id,prepare_command_id,
          record_kind,entity_id,accepted_input_id,accepted_work_input_id,record_json)
          values(pg_catalog.pg_current_xact_id(),p_run_id,p_command_id,'MEMBER',v_line.id,v_input.id,
            v_work_input.id,pg_catalog.to_jsonb(v_member));
      end loop;
      if v_member_no<>v_work_input.selected_line_count then
        raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_SELECTED_LINEAGE_UNAVAILABLE';
      end if;
    end loop;
    if v_input.case_scope='SEALED_SELECTION' then
      for v_case_item in select s.* from private.bpay_next_case_selection_item s
        where s.run_id=p_run_id and s.candidate_id=v_input.candidate_id
          and s.selection_revision=v_input.case_selection_revision order by s.selection_no for key share
      loop
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_case_item.case_id for share;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_case_item.case_component_id for share;
        select r.* into strict v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id for key share;
        if v_case.candidate_id<>v_input.candidate_id or v_component.candidate_id<>v_input.candidate_id
          or v_component.case_id<>v_case.id or v_component.rule_id<>v_rule.id
          or v_rule.rule_revision<>v_case.current_rule_revision then
          raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_CASE_RULE_UNAVAILABLE';
        end if;
        if v_rule.original_funding_event_id is not null and not exists(select 1 from private.bpay_next_case_event e
          where e.id=v_rule.original_funding_event_id and e.case_id=v_case.id and e.event_kind='FUNDED'
            and e.occurred_at_utc=v_rule.original_funded_at_utc) then
          raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_FUNDING_ORIGIN_UNAVAILABLE';
        end if;
        perform 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id for key share;
        v_case_input:=null;v_case_input.accepted_input_id:=v_input.id;v_case_input.run_id:=p_run_id;
        v_case_input.candidate_id:=v_input.candidate_id;v_case_input.selection_revision:=v_input.case_selection_revision;
        v_case_input.case_component_id:=v_component.id;v_case_input.case_id:=v_case.id;
        v_case_input.is_selected:=v_case_item.is_selected;v_case_input.selection_no:=v_case_item.selection_no;
        v_case_input.rule_id:=v_rule.id;v_case_input.rule_revision:=v_rule.rule_revision;
        v_case_input.case_status:=v_case.status;v_case_input.case_revision:=v_case.case_revision;
        v_case_input.next_due_week_start:=v_rule.next_due_monday;v_case_input.component_revision:=v_component.component_revision;
        v_case_input.funded_eligible:=v_rule.original_funding_event_id is not null;
        v_case_input.original_funding_event_id:=v_rule.original_funding_event_id;
        v_case_input.original_funded_at_utc:=v_rule.original_funded_at_utc;
        select r.command_id,r.valuation_policy_id into v_case_input.case_create_command_id,v_case_input.valuation_policy_id
          from private.bpay_next_case_create_request r where r.case_id=v_case.id for key share;
        if found then v_case_input.economic_origin_kind:='CASE_CREATE';
        else
          select c.* into strict v_collection from private.bpay_next_work_collection c
            where c.case_id=v_case.id and c.case_component_id=v_component.id for share;
          if v_case_item.is_selected then
            -- Keep SOURCE comparison eligibility at its owning CM02a boundary.
            -- A retained collection FK alone is not a qualified economic origin.
            if exists(select 1 from private.bpay_next_work w where w.id=v_collection.work_id and w.work_kind='SOURCE') then
              raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
            end if;
            perform 1 from private.bpay_next_work w where w.id=v_collection.work_id for share;
            perform 1 from private.bpay_next_position p where p.work_id=v_collection.work_id
              and p.component_key=v_collection.component_key for share;
            v_collection:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_input.candidate_id);
            if v_collection.id is null then
              raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTANCE_WORK_COLLECTION_ORIGIN_UNAVAILABLE';
            end if;
            end if;
          v_case_input.work_collection_id:=v_collection.id;
          v_case_input.valuation_policy_id:=v_collection.original_valuation_policy_id;
          v_case_input.economic_origin_kind:='WORK_COLLECTION';
        end if;
        perform 1 from private.bpay_next_valuation_policy p where p.id=v_case_input.valuation_policy_id for key share;
        insert into private.bpay_next_acceptance_stage(preparing_transaction_id,run_id,prepare_command_id,
          record_kind,entity_id,accepted_input_id,record_json)
          values(pg_catalog.pg_current_xact_id(),p_run_id,p_command_id,'CASE',v_component.id,v_input.id,
            pg_catalog.to_jsonb(v_case_input));
      end loop;
      -- The true weekly origin selector owns exact output/projection/outcome
      -- proof. No contribution counter is copied as historical authority.
      perform private.bpay_next_stage_accepted_week_v1(p_run_id,p_command_id,v_input.id,v_input.candidate_id);
    end if;
  end loop;
end
$function$;

alter function private.bpay_next_receive_command_v1(uuid,text) owner to postgres;
alter function private.bpay_next_stage_acceptance_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_receive_command_v1(uuid,text),private.bpay_next_stage_acceptance_v1(uuid,uuid) from public,anon,authenticated,service_role;
commit;
