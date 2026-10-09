-- F-25 standalone actual Query metadata, one true-tail command, zero jobs.
-- First closed completion witnesses: genuine immutable manual commands.
-- Other true-root witnesses and Source groups follow their ordered successors.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_event_digest_v1(p_producer_id uuid,p_event_id uuid)
returns bytea language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_EVENT_V1',p.id,p.preparing_transaction_id::text,
   p.module_epoch,p.owner_kind,p.cause_kind,p.cause_key,pg_catalog.encode(p.request_sha256,'hex'),
   p.completion_witness_kind,p.completion_witness_id,pg_catalog.encode(p.completion_witness_sha256,'hex'),
   e.id,e.transaction_ordinal,e.owner_relation,e.owner_row_id,e.old_present,e.new_present,
   pg_catalog.encode(e.old_sha256,'hex'),pg_catalog.encode(e.new_sha256,'hex'),
   q.candidate_id,q.root_timesheet_id,q.old_gate_tuple,q.new_gate_tuple)::text,'UTF8'))
 from private.bpay_next_query_producer p join private.bpay_next_query_event_owner owner on owner.producer_id=p.id
 join private.bpay_next_input_version_event e on e.id=owner.event_id and e.preparing_transaction_id=p.preparing_transaction_id
   and e.module_epoch=p.module_epoch and e.input_kind='PAY_QUERY'
 join private.bpay_next_query_input_version q on q.event_id=e.id and q.owner_relation=e.owner_relation and q.owner_row_id=e.owner_row_id
 where p.id=p_producer_id and e.id=p_event_id
$function$;

create or replace function private.bpay_next_query_completion_witness_assert_v1(p_producer_id uuid,p_witness_id uuid,p_witness_sha256 bytea)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;original private.weekly_source_manual_review_commands%rowtype;
begin
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id;
 if p_witness_id is null or p_witness_sha256 is null or pg_catalog.octet_length(p_witness_sha256)<>32 then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_COMPLETION_WITNESS_INVALID';end if;
 if owned.owner_kind in ('MANUAL_OPEN','MANUAL_RESOLVE') then
   select c.* into original from private.weekly_source_manual_review_commands c where c.command_id=p_witness_id;
   if not found or owned.cause_key<>p_witness_id::text or original.request_sha256<>owned.request_sha256
     or original.command_kind is distinct from (case owned.owner_kind when 'MANUAL_OPEN' then 'OPEN' else 'RESOLVE' end)
     or pg_catalog.sha256(pg_catalog.convert_to(original.result_json::text,'UTF8'))<>p_witness_sha256 then
     raise exception using errcode='23514',message='BPAY_NEXT_QUERY_MANUAL_COMPLETION_UNBOUND';end if;
   if (select count(*) from private.bpay_next_query_event_owner eo where eo.producer_id=owned.id)>1
     or exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id
       join private.bpay_next_query_input_version q on q.event_id=e.id where eo.producer_id=owned.id and
       (e.owner_relation<>'private.weekly_source_manual_reviews' or e.owner_row_id<>original.review_id
         or (owned.owner_kind='MANUAL_OPEN' and (e.old_present or not e.new_present or q.new_gate_tuple->>'state' is distinct from 'OPEN'))
         or (owned.owner_kind='MANUAL_RESOLVE' and (not e.old_present or not e.new_present
           or q.old_gate_tuple->>'state' is distinct from 'OPEN' or q.new_gate_tuple->>'state' is distinct from 'RESOLVED')))) then
     raise exception using errcode='23514',message='BPAY_NEXT_QUERY_MANUAL_EVENT_SCOPE_INVALID';end if;
 else
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPLETION_OWNER_NOT_INSTALLED';
 end if;
end
$function$;

create or replace function private.bpay_next_query_version_command_assert_v1(p_command_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare header private.bpay_next_query_version_command%rowtype;owned private.bpay_next_query_producer%rowtype;
 command private.bpay_next_command%rowtype;actual_count bigint;first_ordinal bigint;last_ordinal bigint;ordered_digest bytea;
begin
 select h.* into strict header from private.bpay_next_query_version_command h where h.command_id=p_command_id;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=header.producer_id;
 select c.* into strict command from private.bpay_next_command c where c.id=header.command_id;
 if owned.phase<>'FINISHED' or owned.mode<>'STANDALONE_QUERY' or pg_catalog.cardinality(owned.owner_stack)<>1
   or (owned.preparing_transaction_id,owned.module_epoch,owned.owner_kind,owned.cause_kind,owned.cause_key,owned.request_sha256,
       owned.metadata_command_id,owned.completion_witness_kind,owned.completion_witness_id,owned.completion_witness_sha256)
     is distinct from (header.preparing_transaction_id,header.module_epoch,header.owner_kind,header.cause_kind,header.cause_key,
       header.request_sha256,header.command_id,header.completion_witness_kind,header.completion_witness_id,header.completion_witness_sha256)
   or (command.agency_sequence,command.module_epoch,command.command_kind,command.status,command.expected_member_count,command.enrolled_member_count)
     is distinct from (header.agency_sequence,header.module_epoch,'QUERY_INPUT_VERSION'::text,'SEALED'::text,0::bigint,0::bigint)
   or command.sealed_at_utc is null
   or exists(select 1 from private.bpay_next_query_source_group g where g.producer_id=owned.id)
   or exists(select 1 from private.bpay_next_query_event_source_member sm join private.bpay_next_query_event_owner eo on eo.event_id=sm.event_id where eo.producer_id=owned.id)
   or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
     and t.module_epoch=owned.module_epoch and t.phase='CLOCK_SEALED')
   or exists(select 1 from private.bpay_next_command_member m where m.command_id=command.id)
   or exists(select 1 from private.bpay_next_job j where j.command_id=command.id)
   or exists(select 1 from private.bpay_next_publication pub where pub.command_id=command.id)
   or exists(select 1 from private.bpay_next_run_command rc where rc.command_id=command.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_COMMAND_INVALID';end if;
 perform private.bpay_next_query_completion_witness_assert_v1(owned.id,header.completion_witness_id,header.completion_witness_sha256);
 select count(*),min(e.transaction_ordinal),max(e.transaction_ordinal),pg_catalog.sha256(pg_catalog.convert_to(
   jsonb_agg(jsonb_build_array(e.id,e.transaction_ordinal,pg_catalog.encode(private.bpay_next_query_event_digest_v1(owned.id,e.id),'hex'))
     order by e.transaction_ordinal)::text,'UTF8')) into actual_count,first_ordinal,last_ordinal,ordered_digest
 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id where eo.producer_id=owned.id;
 if (actual_count,first_ordinal,last_ordinal,ordered_digest) is distinct from
     (header.actual_event_count,header.first_event_ordinal,header.last_event_ordinal,header.ordered_events_sha256)
   or exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
     and (e.input_kind<>'PAY_QUERY' or e.module_epoch<>owned.module_epoch or not exists(
       select 1 from private.bpay_next_query_event_owner eo where eo.event_id=e.id and eo.producer_id=owned.id)))
   or (select count(*) from private.bpay_next_input_version_receipt r where r.command_id=command.id)<>actual_count
   or exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id
     left join private.bpay_next_query_input_version q on q.event_id=e.id
     left join private.bpay_next_input_version_receipt r on r.event_id=e.id where eo.producer_id=owned.id and
     (q.event_id is null or r.event_id is null or e.original_command_id is not null
       or (r.command_id,r.agency_sequence,r.module_epoch) is distinct from (command.id,command.agency_sequence,command.module_epoch)
       or e.old_sha256<>pg_catalog.sha256(pg_catalog.convert_to(q.old_gate_tuple::text,'UTF8'))
       or e.new_sha256<>pg_catalog.sha256(pg_catalog.convert_to(q.new_gate_tuple::text,'UTF8'))
       or r.evidence_sha256<>pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_VERSION_RECEIPT_V1',
         pg_catalog.encode(private.bpay_next_query_event_digest_v1(owned.id,e.id),'hex'),command.id,command.agency_sequence,
         command.module_epoch)::text,'UTF8'))))
   or header.header_sha256<>pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_VERSION_COMMAND_V1',
     header.command_id,header.agency_sequence,header.module_epoch,header.producer_id,header.preparing_transaction_id::text,
     header.owner_kind,header.cause_kind,header.cause_key,pg_catalog.encode(header.request_sha256,'hex'),header.actual_event_count,
     header.first_event_ordinal,header.last_event_ordinal,header.completion_witness_kind,header.completion_witness_id,
     pg_catalog.encode(header.completion_witness_sha256,'hex'),pg_catalog.encode(header.ordered_events_sha256,'hex'))::text,'UTF8')) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_RECEIPT_SET_INVALID';end if;
end
$function$;

create or replace function private.bpay_next_query_producer_finish_standalone_v1(
 p_producer_id uuid,p_owner_kind text,p_witness_id uuid,p_witness_sha256 bytea
) returns void language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;actual_count bigint;first_ordinal bigint;last_ordinal bigint;
 ordered_digest bytea;sequence bigint;event private.bpay_next_input_version_event%rowtype;header private.bpay_next_query_version_command%rowtype;
begin
 if p_producer_id is null then return;end if;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id
   and p.preparing_transaction_id=pg_catalog.pg_current_xact_id() for update;
 if owned.phase<>'BUSINESS_OPEN' or owned.mode<>'STANDALONE_QUERY' or owned.owner_kind is distinct from p_owner_kind
   or pg_catalog.cardinality(owned.owner_stack)<>1 or not exists(select 1 from private.bpay_next_input_transaction t
     where t.preparing_transaction_id=owned.preparing_transaction_id and t.module_epoch=owned.module_epoch and t.phase='OPEN')
   or not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=owned.module_epoch)
   or exists(select 1 from public.banking_pay_scope_change_transactions x where x.state='PENDING'
     and x.xmin::text::numeric=mod(owned.preparing_transaction_id::text::numeric,4294967296)) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_ROOT_FINISH_OWNER_INVALID';end if;
 perform private.bpay_next_query_completion_witness_assert_v1(owned.id,p_witness_id,p_witness_sha256);
 if exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
   and (e.input_kind<>'PAY_QUERY' or e.module_epoch<>owned.module_epoch or not exists(select 1 from private.bpay_next_query_event_owner eo
     where eo.event_id=e.id and eo.producer_id=owned.id))) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_ROOT_EVENT_OWNER_INVALID';end if;
 update private.bpay_next_query_producer p set completion_witness_kind=p.owner_kind,completion_witness_id=p_witness_id,
   completion_witness_sha256=p_witness_sha256 where p.id=owned.id;
 select count(*),min(e.transaction_ordinal),max(e.transaction_ordinal),pg_catalog.sha256(pg_catalog.convert_to(
   jsonb_agg(jsonb_build_array(e.id,e.transaction_ordinal,pg_catalog.encode(private.bpay_next_query_event_digest_v1(owned.id,e.id),'hex'))
     order by e.transaction_ordinal)::text,'UTF8')) into actual_count,first_ordinal,last_ordinal,ordered_digest
 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id where eo.producer_id=owned.id;
 if actual_count=0 then
   update private.bpay_next_query_producer p set phase='FINISHED',finished_at_utc=pg_catalog.clock_timestamp() where p.id=owned.id;
   update private.bpay_next_input_transaction t set phase='CLOCK_SEALED',clock_sealed_at_utc=pg_catalog.clock_timestamp()
     where t.preparing_transaction_id=owned.preparing_transaction_id and t.phase='OPEN';
   return;
 end if;
 if actual_count>1000 or ordered_digest is null or exists(select 1 from private.bpay_next_query_event_owner eo
   join private.bpay_next_input_version_event e on e.id=eo.event_id left join private.bpay_next_query_input_version q on q.event_id=e.id
   where eo.producer_id=owned.id and (q.event_id is null or e.original_command_id is not null
     or e.old_sha256<>pg_catalog.sha256(pg_catalog.convert_to(q.old_gate_tuple::text,'UTF8'))
     or e.new_sha256<>pg_catalog.sha256(pg_catalog.convert_to(q.new_gate_tuple::text,'UTF8'))
     or exists(select 1 from private.bpay_next_input_version_receipt r where r.event_id=e.id))) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_ROOT_STAGED_SET_INVALID';end if;
 -- All targets are own newly staged metadata, already held before the clock.
 perform 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
   order by e.transaction_ordinal for key share;
 perform 1 from private.bpay_next_query_input_version q join private.bpay_next_query_event_owner eo on eo.event_id=q.event_id
   where eo.producer_id=owned.id for key share of q;
 update private.bpay_next_query_producer p set phase='FINISHING',metadata_command_id=p.id where p.id=owned.id;
 update private.bpay_next_input_transaction t set phase='FINALISING' where t.preparing_transaction_id=owned.preparing_transaction_id and t.phase='OPEN';
 sequence:=private.bpay_next_receive_command_v1(owned.id,'QUERY_INPUT_VERSION');
 header.command_id:=owned.id;header.agency_sequence:=sequence;header.module_epoch:=owned.module_epoch;
 header.producer_id:=owned.id;header.preparing_transaction_id:=owned.preparing_transaction_id;
 header.owner_kind:=owned.owner_kind;header.cause_kind:=owned.cause_kind;header.cause_key:=owned.cause_key;header.request_sha256:=owned.request_sha256;
 header.actual_event_count:=actual_count;header.first_event_ordinal:=first_ordinal;header.last_event_ordinal:=last_ordinal;
 header.completion_witness_kind:=owned.owner_kind;header.completion_witness_id:=p_witness_id;header.completion_witness_sha256:=p_witness_sha256;
 header.ordered_events_sha256:=ordered_digest;header.header_sha256:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(
   'BPAY_NEXT_QUERY_VERSION_COMMAND_V1',header.command_id,header.agency_sequence,header.module_epoch,header.producer_id,
   header.preparing_transaction_id::text,header.owner_kind,header.cause_kind,header.cause_key,pg_catalog.encode(header.request_sha256,'hex'),
   header.actual_event_count,header.first_event_ordinal,header.last_event_ordinal,header.completion_witness_kind,header.completion_witness_id,
   pg_catalog.encode(header.completion_witness_sha256,'hex'),pg_catalog.encode(header.ordered_events_sha256,'hex'))::text,'UTF8'));
 insert into private.bpay_next_query_version_command select header.*;
 for event in select e.* from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id
   where eo.producer_id=owned.id order by e.transaction_ordinal loop
   insert into private.bpay_next_input_version_receipt(event_id,command_id,agency_sequence,module_epoch,evidence_sha256)
     values(event.id,owned.id,sequence,owned.module_epoch,pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(
       'BPAY_NEXT_QUERY_VERSION_RECEIPT_V1',pg_catalog.encode(private.bpay_next_query_event_digest_v1(owned.id,event.id),'hex'),
       owned.id,sequence,owned.module_epoch)::text,'UTF8')));
 end loop;
 update private.bpay_next_command c set expected_member_count=0,enrolled_member_count=0,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
   where c.id=owned.id and c.status='RECEIVED';
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_SEAL_CONFLICT';end if;
 update private.bpay_next_query_producer p set phase='FINISHED',finished_at_utc=pg_catalog.clock_timestamp() where p.id=owned.id;
 update private.bpay_next_input_transaction t set phase='CLOCK_SEALED',clock_sealed_at_utc=pg_catalog.clock_timestamp()
   where t.preparing_transaction_id=owned.preparing_transaction_id and t.phase='FINALISING';
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_TRANSACTION_CHANGED';end if;
 perform private.bpay_next_query_version_command_assert_v1(owned.id);
end
$function$;

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
  if v_input_phase='FINALISING' and p_command_kind not in ('INPUT_VERSION','QUERY_INPUT_VERSION') then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_FINALISER_COMMAND_INVALID';
  end if;
  if p_command_kind='INPUT_VERSION' and not exists(select 1 from private.bpay_next_input_version_event e
    where e.id=p_command_id and e.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and e.module_epoch=v_module_epoch and e.input_kind in ('CANDIDATE','UMBRELLA','POLICY')
      and v_input_phase='FINALISING') then
    raise exception using errcode='55000',message='BPAY_NEXT_INPUT_VERSION_COMMAND_OWNER_INVALID';
  end if;
  if p_command_kind='QUERY_INPUT_VERSION' and not exists(select 1 from private.bpay_next_query_producer p
    where p.id=p_command_id and p.metadata_command_id=p_command_id and p.preparing_transaction_id=pg_catalog.pg_current_xact_id()
      and p.module_epoch=v_module_epoch and p.mode='STANDALONE_QUERY' and p.phase='FINISHING'
      and pg_catalog.cardinality(p.owner_stack)=1 and p.completion_witness_id is not null and p.completion_witness_sha256 is not null
      and v_input_phase='FINALISING' and exists(select 1 from private.bpay_next_query_event_owner eo where eo.producer_id=p.id)) then
    raise exception using errcode='55000',message='BPAY_NEXT_QUERY_METADATA_RECEIVE_OWNER_INVALID';end if;
  if exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id())
    and p_command_kind<>'QUERY_INPUT_VERSION' then
    raise exception using errcode='55000',message='BPAY_NEXT_QUERY_ROOT_UNRELATED_COMMAND_UNSUPPORTED';end if;
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

create or replace function private.bpay_next_enroll_one_v1()
returns uuid
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_command private.bpay_next_command%rowtype;
  v_member private.bpay_next_command_member%rowtype;
  v_job_id uuid;
  v_epoch bigint;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_selected_candidate uuid;
  v_expired_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select * into v_command from private.bpay_next_command
    where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  if v_command.command_kind='QUERY_INPUT_VERSION' then
    if v_command.module_epoch<>v_epoch then raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_ENROLLMENT_EPOCH_INVALID';end if;
    perform private.bpay_next_query_version_command_assert_v1(v_command.id);
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence
      where id=1 and last_enrolled_sequence=v_command.agency_sequence-1;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_QUERY_METADATA_ENROLLMENT_ORDER_CHANGED';end if;
    return null;
  end if;
  if v_command.command_kind='INPUT_VERSION' then
    if v_command.module_epoch<>v_epoch or v_command.status<>'SEALED'
      or v_command.expected_member_count<>0 or v_command.enrolled_member_count<>0
      or v_command.sealed_at_utc is null
      or (select count(*) from private.bpay_next_input_version_receipt r
        join private.bpay_next_input_version_event e on e.id=r.event_id
        where r.command_id=v_command.id and r.agency_sequence=v_command.agency_sequence
          and r.module_epoch=v_command.module_epoch and e.module_epoch=r.module_epoch)<>1
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_INPUT_VERSION_ENROLLMENT_INVALID';
    end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence
      where id=1 and last_enrolled_sequence=v_command.agency_sequence-1;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_INPUT_VERSION_ORDER_CHANGED';end if;
    return null;
  end if;
  if v_command.command_kind='PREPARE' then
    select rc.run_id into strict v_run_id
      from private.bpay_next_run_command rc
      where rc.command_id=v_command.id;
    select * into strict v_run from private.bpay_next_pay_run
      where id=v_run_id for share;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and v_expired_preparation))
       or v_run.selection_state<>'SEALED'
       or v_command.expected_member_count<>v_run.selected_candidate_count
       or v_command.enrolled_member_count>=v_command.expected_member_count then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    select sc.candidate_id into strict v_selected_candidate
      from private.bpay_next_selection_candidate sc
      where sc.run_id=v_run_id
        and sc.member_no=v_command.enrolled_member_count+1;
    -- Acceptance already retained immutable member identities, not jobs.
    select m.* into strict v_member from private.bpay_next_command_member m
      where m.command_id=v_command.id and m.candidate_id=v_selected_candidate;
    if v_member.member_no<>v_command.enrolled_member_count+1 or not exists(
      select 1 from private.bpay_next_accepted_input i where i.prepare_command_id=v_command.id
        and i.candidate_id=v_selected_candidate and i.run_id=v_run_id
        and i.accepted_sequence=v_command.agency_sequence and i.module_epoch=v_command.module_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_ACCEPTED_MEMBER_MISMATCH';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_selected_candidate,'PREPARE',
             case when v_expired_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command
      set enrolled_member_count=enrolled_member_count+1,
          status=case when enrolled_member_count+1=expected_member_count
            then 'SEALED' else 'ENROLLING' end,
          sealed_at_utc=case when enrolled_member_count+1=expected_member_count
            then pg_catalog.transaction_timestamp() else null end
      where id=v_command.id;
    if v_command.enrolled_member_count+1=v_command.expected_member_count then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_EXPIRY' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_expiry_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.status <> 'SEALED' then
    return null;
  end if;
  if v_command.command_kind='CASE_WRITE_OFF' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_write_off_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_WRITE_OFF','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_create_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_CREATE','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='REISSUE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_reissue_cancel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'REISSUE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT') then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if v_command.command_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is null and r.internal_receipt_id is not null)
       or v_command.command_kind='CSV_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is not null and r.internal_receipt_id is null)
       or v_command.command_kind='CSV_RETURN' and not exists(select 1 from private.bpay_next_return_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id)
       or v_command.command_kind='CASH_REISSUE' and not exists(select 1 from private.bpay_next_reissue_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,v_command.command_kind,'READY','MEMBERS',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='SIMPLE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_cancel_request r
      where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
        and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'SIMPLE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PAYE_NET_ENTRY' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_paye_net_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'PAYE_NET_ENTRY','READY',
             case when exists(select 1 from private.bpay_next_paye_net_request r
               where r.command_id=v_command.id and r.case_draft_state_id is not null)
               then 'FINANCE_ALLOCATE' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='TRANSFER_BUILD' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_ENROLLMENT_INVALID';
    end if;
    select * into strict v_member from private.bpay_next_command_member
      where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_transfer t
                  where t.build_command_id=v_command.id
                    and t.candidate_id=v_member.candidate_id
                    and t.status='BUILDING') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'TRANSFER_BUILD','READY','WORK',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.module_epoch<>v_epoch
     or v_command.command_kind<>'POSITION_APPLY'
     or v_command.expected_member_count<>1
     or v_command.enrolled_member_count<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_COMMAND_INVALID';
  end if;
  select * into strict v_member from private.bpay_next_command_member
    where command_id=v_command.id and member_no=1;
  if not exists (select 1 from private.bpay_next_publication p
                 where p.command_id=v_command.id and p.candidate_id=v_member.candidate_id) then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_PUBLICATION_MISSING';
  end if;
  insert into private.bpay_next_job
    (command_id,command_sequence,module_epoch,candidate_id,job_kind,
     status,phase,owner_epoch)
    values (v_command.id,v_command.agency_sequence,v_epoch,
            v_member.candidate_id,'POSITION_APPLY','READY','NEW',1)
    returning id into v_job_id;
  update private.bpay_next_command set enrolled_member_count=1
    where id=v_command.id;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence where id=1;
  return v_job_id;
end
$function$;

alter function private.bpay_next_query_event_digest_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_query_event_digest_v1(uuid,uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) owner to postgres;
revoke all on function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) from public,anon,authenticated,service_role;

alter function private.bpay_next_query_version_command_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_version_command_assert_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_query_producer_finish_standalone_v1(uuid,text,uuid,bytea) owner to postgres;
revoke all on function private.bpay_next_query_producer_finish_standalone_v1(uuid,text,uuid,bytea) from public,anon,authenticated,service_role;

alter function private.bpay_next_receive_command_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_receive_command_v1(uuid,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;

create or replace function private.bpay_next_query_event_owner_insert_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if not exists(select 1 from private.bpay_next_query_producer p join private.bpay_next_input_version_event e
     on e.preparing_transaction_id=p.preparing_transaction_id and e.module_epoch=p.module_epoch and e.input_kind='PAY_QUERY'
   join private.bpay_next_query_input_version q on q.event_id=e.id and q.owner_relation=e.owner_relation and q.owner_row_id=e.owner_row_id
   where p.id=new.producer_id and e.id=new.event_id and p.preparing_transaction_id=pg_catalog.pg_current_xact_id()
     and p.phase in ('BUSINESS_OPEN','TAIL_PREPARED')) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_EVENT_OWNER_UNBOUND';end if;
 return new;
end
$function$;
drop trigger if exists bpay_next_query_event_owner_insert_guard_v1 on private.bpay_next_query_event_owner;
create trigger bpay_next_query_event_owner_insert_guard_v1 before insert on private.bpay_next_query_event_owner
 for each row execute function private.bpay_next_query_event_owner_insert_guard_v1();

create or replace function private.bpay_next_query_producer_finished_assert_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;
begin
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=new.id;
 if owned.phase<>'FINISHED' or owned.preparing_transaction_id<>pg_catalog.pg_current_xact_id()
   or pg_catalog.cardinality(owned.owner_stack)<>1 or not exists(select 1 from private.bpay_next_input_transaction t
     where t.preparing_transaction_id=owned.preparing_transaction_id and t.module_epoch=owned.module_epoch and t.phase='CLOCK_SEALED') then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_TRUE_ROOT_NOT_FINISHED';end if;
 if owned.mode<>'STANDALONE_QUERY' then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_SOURCE_COMPLETION_NOT_INSTALLED';end if;
 if owned.completion_witness_kind is distinct from owned.owner_kind then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_COMPLETION_KIND_INVALID';end if;
 perform private.bpay_next_query_completion_witness_assert_v1(owned.id,owned.completion_witness_id,owned.completion_witness_sha256);
 if exists(select 1 from private.bpay_next_query_source_group g where g.producer_id=owned.id)
   or exists(select 1 from private.bpay_next_query_event_source_member sm join private.bpay_next_query_event_owner eo on eo.event_id=sm.event_id
     where eo.producer_id=owned.id) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_STANDALONE_SOURCE_GROUP_INVALID';end if;
 if owned.metadata_command_id is null then
   if exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id)
     or exists(select 1 from private.bpay_next_query_version_command h where h.producer_id=owned.id) then
     raise exception using errcode='23514',message='BPAY_NEXT_QUERY_EMPTY_ROOT_HAS_EVENTS';end if;
 else perform private.bpay_next_query_version_command_assert_v1(owned.metadata_command_id);end if;
 return null;
end
$function$;
drop trigger if exists bpay_next_query_producer_finished_assert_v1 on private.bpay_next_query_producer;
create constraint trigger bpay_next_query_producer_finished_assert_v1 after insert on private.bpay_next_query_producer
 deferrable initially deferred for each row execute function private.bpay_next_query_producer_finished_assert_v1();

alter function private.bpay_next_query_event_owner_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_query_event_owner_insert_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_query_producer_finished_assert_v1() owner to postgres;
revoke all on function private.bpay_next_query_producer_finished_assert_v1() from public,anon,authenticated,service_role;

create or replace function private.bpay_next_settings_statement_enter_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_phase text;
begin
  if exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id()) then
    raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MIXED_TRANSACTION_UNSUPPORTED';end if;
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
alter function private.bpay_next_settings_statement_enter_v1() owner to postgres;
revoke all on function private.bpay_next_settings_statement_enter_v1() from public,anon,authenticated,service_role;

commit;
