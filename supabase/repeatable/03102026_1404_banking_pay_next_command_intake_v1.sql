-- Repeatable CloudTMS function/view authority: banking_pay_next_command_intake_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- One short row lock establishes accepted financial-command order. It is not
-- held for preparation, browser review, network I/O or a batch calculation.
-- A rolled-back receipt consumes no number; an exact retry returns the same.
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

alter function private.bpay_next_receive_command_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_receive_command_v1(uuid,text)
  from public, anon, authenticated, service_role;

commit;
