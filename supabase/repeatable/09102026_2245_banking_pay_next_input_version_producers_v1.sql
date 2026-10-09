-- Repeatable CloudTMS function/view authority: banking_pay_next_input_version_producers_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Statement entry precedes target-row mutation and all existing callbacks.
-- No contact/bank/password data enters financial input evidence.
create or replace function private.bpay_next_settings_statement_enter_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_phase text;
begin
  select t.phase into v_phase from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id();
  if v_phase in ('FINALISING','CLOCK_SEALED') then
    raise exception using errcode='55000',message='BPAY_NEXT_PARTICIPATING_MUTATION_AFTER_CLOCK';
  end if;
  perform private.weekly_source_pay_query_admit_v2();
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then return null;end if;
  insert into private.bpay_next_input_transaction(preparing_transaction_id,module_epoch,phase)
    values(pg_catalog.pg_current_xact_id(),v_epoch,'OPEN') on conflict(preparing_transaction_id) do nothing;
  if not exists(select 1 from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.module_epoch=v_epoch and t.phase='OPEN') then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_TRANSACTION_OWNER_MISMATCH';
  end if;
  if tg_table_schema='public' and tg_table_name='settings_finance_windows' then
    perform 1 from private.bpay_next_valuation_policy_control p where p.id=1 for update;
  end if;
  return null;
end
$function$;

create or replace function private.bpay_next_settings_input_stage_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_old jsonb;v_new jsonb;v_id uuid;v_kind text;v_epoch bigint;v_ordinal bigint;v_event uuid;
begin
  -- Only these exact three relations/typed economics are admitted.
  if tg_table_schema='public' and tg_table_name='candidates' then
    v_kind:='CANDIDATE';v_id:=new.id;
    v_new:=pg_catalog.jsonb_build_array(new.pay_method,new.umbrella_id,new.min_take_home_wtd);
    v_old:=case when tg_op='INSERT' then 'null'::jsonb else
      pg_catalog.jsonb_build_array(old.pay_method,old.umbrella_id,old.min_take_home_wtd) end;
  elsif tg_table_schema='public' and tg_table_name='umbrellas' then
    v_kind:='UMBRELLA';v_id:=new.id;
    v_new:=pg_catalog.jsonb_build_array(new.vat_chargeable,new.enabled);
    v_old:=case when tg_op='INSERT' then 'null'::jsonb else
      pg_catalog.jsonb_build_array(old.vat_chargeable,old.enabled) end;
  elsif tg_table_schema='private' and tg_table_name='bpay_next_valuation_policy_control' then
    v_kind:='POLICY';v_id:=new.current_policy_id;
    v_new:=pg_catalog.jsonb_build_array(new.current_policy_id,new.current_policy_no);
    v_old:=pg_catalog.jsonb_build_array(old.current_policy_id,old.current_policy_no);
  else raise exception using errcode='55000',message='BPAY_NEXT_INPUT_STAGE_RELATION_NOT_ALLOWED';end if;
  if v_old is not distinct from v_new then return new;end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then return new;end if;
  update private.bpay_next_input_transaction t set next_event_ordinal=t.next_event_ordinal+1
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.module_epoch=v_epoch
      and t.phase='OPEN' and t.next_event_ordinal<1000 returning t.next_event_ordinal into v_ordinal;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_INPUT_STAGE_OWNER_OR_BOUND_INVALID';end if;
  v_event:=pg_catalog.gen_random_uuid();
  insert into private.bpay_next_input_version_event(id,preparing_transaction_id,module_epoch,input_kind,
    owner_relation,owner_row_id,old_present,new_present,old_sha256,new_sha256,transaction_ordinal)
    values(v_event,pg_catalog.pg_current_xact_id(),v_epoch,v_kind,tg_table_schema||'.'||tg_table_name,v_id,
      tg_op<>'INSERT',true,pg_catalog.sha256(pg_catalog.convert_to(v_old::text,'UTF8')),
      pg_catalog.sha256(pg_catalog.convert_to(v_new::text,'UTF8')),v_ordinal);
  if v_kind='CANDIDATE' then
    insert into private.bpay_next_candidate_input_version(event_id,candidate_id,old_pay_method,new_pay_method,
      old_umbrella_id,new_umbrella_id,old_floor,new_floor)
      values(v_event,new.id,case when tg_op='INSERT' then null else old.pay_method end,new.pay_method,
        case when tg_op='INSERT' then null else old.umbrella_id end,new.umbrella_id,
        case when tg_op='INSERT' then null else old.min_take_home_wtd end,new.min_take_home_wtd);
  elsif v_kind='UMBRELLA' then
    insert into private.bpay_next_umbrella_input_version(event_id,umbrella_id,old_vat_chargeable,new_vat_chargeable,
      old_enabled,new_enabled) values(v_event,new.id,case when tg_op='INSERT' then null else old.vat_chargeable end,
        new.vat_chargeable,case when tg_op='INSERT' then null else old.enabled end,new.enabled);
  else
    insert into private.bpay_next_policy_input_version(event_id,old_policy_id,new_policy_id,old_policy_no,new_policy_no)
      values(v_event,old.current_policy_id,new.current_policy_id,old.current_policy_no,new.current_policy_no);
  end if;
  return new;
end
$function$;

-- Called only after real immediate statement callbacks. Every material row
-- event receives its own finite INPUT_VERSION receipt in actual stage order.
-- These commands have no Candidate fanout, financial job or second clock.
create or replace function private.bpay_next_settings_flush_v1()
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_tx private.bpay_next_input_transaction%rowtype;v_event private.bpay_next_input_version_event%rowtype;
  v_sequence bigint;v_evidence bytea;v_typed jsonb;v_count bigint;
begin
  select t.* into v_tx from private.bpay_next_input_transaction t
    where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() for update;
  if not found then return;end if;
  select count(*) into v_count from private.bpay_next_input_version_event e
    where e.preparing_transaction_id=v_tx.preparing_transaction_id and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY')
      and not exists(select 1 from private.bpay_next_input_version_receipt r where r.event_id=e.id);
  if v_count=0 then return;end if;
  if v_tx.phase<>'OPEN' or v_count>1000 or not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_tx.module_epoch for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_FINALISE_OWNER_INVALID';
  end if;
  -- A mixed legacy deferred producer is unsupported. This is a real current
  -- transaction row identity, not a caller-resettable GUC proof or a wait.
  if exists(select 1 from public.banking_pay_scope_change_transactions x where x.state='PENDING'
    and x.xmin::text::numeric=mod(v_tx.preparing_transaction_id::text::numeric,4294967296)) then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_LEGACY_DEFERRED_OWNER_PENDING';
  end if;
  -- All receipt FK targets are staged/held before the first command clock.
  perform 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=v_tx.preparing_transaction_id
    and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY') order by e.transaction_ordinal for key share;
  perform 1 from private.bpay_next_candidate_input_version c join private.bpay_next_input_version_event e on e.id=c.event_id
    where e.preparing_transaction_id=v_tx.preparing_transaction_id for key share of c;
  perform 1 from private.bpay_next_umbrella_input_version u join private.bpay_next_input_version_event e on e.id=u.event_id
    where e.preparing_transaction_id=v_tx.preparing_transaction_id for key share of u;
  perform 1 from private.bpay_next_policy_input_version p join private.bpay_next_input_version_event e on e.id=p.event_id
    where e.preparing_transaction_id=v_tx.preparing_transaction_id for key share of p;
  update private.bpay_next_input_transaction t set phase='FINALISING' where t.preparing_transaction_id=v_tx.preparing_transaction_id;
  for v_event in select e.* from private.bpay_next_input_version_event e
    where e.preparing_transaction_id=v_tx.preparing_transaction_id and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY')
      and not exists(select 1 from private.bpay_next_input_version_receipt r where r.event_id=e.id)
    order by e.transaction_ordinal
  loop
    v_typed:=case v_event.input_kind
      when 'CANDIDATE' then (select pg_catalog.to_jsonb(c) from private.bpay_next_candidate_input_version c where c.event_id=v_event.id)
      when 'UMBRELLA' then (select pg_catalog.to_jsonb(u) from private.bpay_next_umbrella_input_version u where u.event_id=v_event.id)
      when 'POLICY' then (select pg_catalog.to_jsonb(p) from private.bpay_next_policy_input_version p where p.event_id=v_event.id) end;
    if v_typed is null then raise exception using errcode='55000',message='BPAY_NEXT_INPUT_TYPED_RECEIPT_MISSING';end if;
    v_evidence:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array('BPAY_NEXT_INPUT_VERSION_V1',
      v_event.id,v_event.module_epoch,v_event.input_kind,v_event.owner_relation,v_event.owner_row_id,
      v_event.old_present,v_event.new_present,pg_catalog.encode(v_event.old_sha256,'hex'),
      pg_catalog.encode(v_event.new_sha256,'hex'),v_event.transaction_ordinal,v_typed)::text,'UTF8'));
    v_sequence:=private.bpay_next_receive_command_v1(v_event.id,'INPUT_VERSION');
    insert into private.bpay_next_input_version_receipt(event_id,command_id,agency_sequence,module_epoch,evidence_sha256)
      values(v_event.id,v_event.id,v_sequence,v_event.module_epoch,v_evidence);
    update private.bpay_next_command c set expected_member_count=0,enrolled_member_count=0,status='SEALED',
      sealed_at_utc=pg_catalog.transaction_timestamp() where c.id=v_event.id and c.status='RECEIVED';
    if not found then raise exception using errcode='55000',message='BPAY_NEXT_INPUT_COMMAND_STATE_INVALID';end if;
  end loop;
  update private.bpay_next_input_transaction t set phase='CLOCK_SEALED',clock_sealed_at_utc=pg_catalog.clock_timestamp()
    where t.preparing_transaction_id=v_tx.preparing_transaction_id and t.phase='FINALISING';
end
$function$;

create or replace function private.bpay_next_settings_finalize_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
begin
  perform private.bpay_next_settings_flush_v1();
  return null;
end
$function$;

create or replace function private.bpay_next_input_transaction_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
begin
  if tg_op in ('DELETE','TRUNCATE') or new.preparing_transaction_id is distinct from pg_catalog.pg_current_xact_id()
    or (tg_op='UPDATE' and (new.preparing_transaction_id,new.module_epoch,new.created_at_utc)
      is distinct from (old.preparing_transaction_id,old.module_epoch,old.created_at_utc))
    or (tg_op='UPDATE' and old.phase='CLOCK_SEALED')
    or (tg_op='UPDATE' and new.next_event_ordinal<old.next_event_ordinal) then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_TRANSACTION_IMMUTABLE';
  end if;
  return new;
end
$function$;

drop trigger if exists bpay_next_input_transaction_guard_v1 on private.bpay_next_input_transaction;
create trigger bpay_next_input_transaction_guard_v1 before insert or update or delete on private.bpay_next_input_transaction
  for each row execute function private.bpay_next_input_transaction_guard_v1();
drop trigger if exists bpay_next_input_transaction_no_truncate_v1 on private.bpay_next_input_transaction;
create trigger bpay_next_input_transaction_no_truncate_v1 before truncate on private.bpay_next_input_transaction
  for each statement execute function private.bpay_next_input_transaction_guard_v1();
-- Queue finalisation when the owned latch is first inserted, BEFORE any event
-- queues its deferred committed-receipt FK. The callback runs at commit after
-- all immediate callbacks, creates actual receipts, then that FK can check.
drop trigger if exists bpay_next_settings_finalize_v1 on private.bpay_next_input_transaction;
create constraint trigger bpay_next_settings_finalize_v1 after insert on private.bpay_next_input_transaction
  deferrable initially deferred for each row execute function private.bpay_next_settings_finalize_v1();

drop trigger if exists bpay_next_candidate_input_entry_v1 on public.candidates;
create trigger bpay_next_candidate_input_entry_v1 before insert or update of pay_method,umbrella_id,min_take_home_wtd
  on public.candidates for each statement execute function private.bpay_next_settings_statement_enter_v1();
drop trigger if exists bpay_next_candidate_input_stage_v1 on public.candidates;
create trigger bpay_next_candidate_input_stage_v1 after insert or update of pay_method,umbrella_id,min_take_home_wtd
  on public.candidates for each row execute function private.bpay_next_settings_input_stage_v1();
drop trigger if exists bpay_next_umbrella_input_entry_v1 on public.umbrellas;
create trigger bpay_next_umbrella_input_entry_v1 before insert or update of vat_chargeable,enabled
  on public.umbrellas for each statement execute function private.bpay_next_settings_statement_enter_v1();
drop trigger if exists bpay_next_umbrella_input_stage_v1 on public.umbrellas;
create trigger bpay_next_umbrella_input_stage_v1 after insert or update of vat_chargeable,enabled
  on public.umbrellas for each row execute function private.bpay_next_settings_input_stage_v1();
drop trigger if exists bpay_next_policy_input_entry_v1 on public.settings_finance_windows;
create trigger bpay_next_policy_input_entry_v1 before insert or update or delete or truncate
  on public.settings_finance_windows for each statement execute function private.bpay_next_settings_statement_enter_v1();
drop trigger if exists bpay_next_policy_input_stage_v1 on private.bpay_next_valuation_policy_control;
create trigger bpay_next_policy_input_stage_v1 after update of current_policy_id,current_policy_no
  on private.bpay_next_valuation_policy_control for each row execute function private.bpay_next_settings_input_stage_v1();

alter function private.bpay_next_settings_statement_enter_v1() owner to postgres;
alter function private.bpay_next_settings_input_stage_v1() owner to postgres;
alter function private.bpay_next_settings_flush_v1() owner to postgres;
alter function private.bpay_next_settings_finalize_v1() owner to postgres;
alter function private.bpay_next_input_transaction_guard_v1() owner to postgres;
revoke all on function private.bpay_next_settings_statement_enter_v1(),private.bpay_next_settings_input_stage_v1(),
  private.bpay_next_settings_flush_v1(),private.bpay_next_settings_finalize_v1(),private.bpay_next_input_transaction_guard_v1()
  from public,anon,authenticated,service_role;

commit;
