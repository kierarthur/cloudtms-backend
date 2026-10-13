-- F-25 retained CASE/WEEK receipts consume the exact pre-clock typed capture.
-- These checks take no new mutable business lock after PREPARE allocation.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_accepted_case_week_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare
  v_parent private.bpay_next_accepted_input%rowtype;
  v_record jsonb;v_entity uuid;v_kind text;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_EVIDENCE_IMMUTABLE';
  end if;
  select i.* into strict v_parent from private.bpay_next_accepted_input i
    join private.bpay_next_command c on c.id=i.prepare_command_id
    join private.bpay_next_run_command rc on rc.command_id=c.id and rc.run_id=i.run_id
    join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=i.candidate_id
    join private.bpay_next_module_control o on o.id=1 and o.active_owner='NEXT' and o.owner_epoch=i.module_epoch
    where i.id=new.accepted_input_id and i.candidate_id=new.candidate_id
      and c.command_kind='PREPARE' and c.module_epoch=i.module_epoch and c.agency_sequence=i.accepted_sequence
      and c.status='ENROLLING' and c.enrolled_member_count=0;
  if v_parent.case_scope<>'SEALED_SELECTION' then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_CASE_SCOPE_UNBOUND';
  end if;
  if tg_table_name='bpay_next_accepted_case_input' then
    v_kind:='CASE';v_entity:=new.case_component_id;
    if (new.run_id,new.selection_revision) is distinct from (v_parent.run_id,v_parent.case_selection_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_CASE_SELECTION_MISMATCH';
    end if;
  elsif tg_table_name='bpay_next_accepted_week_input' then
    v_kind:='WEEK';v_entity:=new.contribution_id;
    if new.pay_week_start<>v_parent.pay_week_start or new.economic_basis_sequence>=v_parent.accepted_sequence
      or new.eligibility_sequence>=v_parent.accepted_sequence
      or new.economic_basis_sha256 is distinct from pg_catalog.sha256(pg_catalog.convert_to(new.economic_origin_proof::text,'UTF8')) then
      raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WEEK_ORIGIN_MISMATCH';
    end if;
  else
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_CHILD_OWNER_INVALID';
  end if;
  select s.record_json into v_record from private.bpay_next_acceptance_stage s
    where s.preparing_transaction_id=pg_catalog.pg_current_xact_id() and s.run_id=v_parent.run_id
      and s.prepare_command_id=v_parent.prepare_command_id and s.accepted_input_id=v_parent.id
      and s.record_kind=v_kind and s.entity_id=v_entity and s.accepted_work_input_id is null;
  if not found or v_record is distinct from pg_catalog.to_jsonb(new) then
    raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_CHILD_CAPTURE_MISMATCH';
  end if;
  return new;
end
$function$;

drop trigger if exists bpay_next_accepted_case_insert_guard_v1 on private.bpay_next_accepted_case_input;
create trigger bpay_next_accepted_case_insert_guard_v1 before insert on private.bpay_next_accepted_case_input
  for each row execute function private.bpay_next_accepted_case_week_guard_v1();
drop trigger if exists bpay_next_accepted_week_insert_guard_v1 on private.bpay_next_accepted_week_input;
create trigger bpay_next_accepted_week_insert_guard_v1 before insert on private.bpay_next_accepted_week_input
  for each row execute function private.bpay_next_accepted_case_week_guard_v1();
alter function private.bpay_next_accepted_case_week_guard_v1() owner to postgres;
revoke all on function private.bpay_next_accepted_case_week_guard_v1() from public,anon,authenticated,service_role;

commit;
