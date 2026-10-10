-- F-25 actual direct and bulk System-hours Query roots.
-- Design: F-25-system-hours-completion-build.md. Original business preserved.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_system_action_digest_v1(p_actor uuid,p_cycle uuid,p_publication uuid,p_incidents uuid[])
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SYSTEM_HOURS_ACTION_V1',jsonb_build_object(
  'actor_user_id',p_actor,'source_cycle_id',p_cycle,'projection_publication_id',p_publication,'incident_ids',to_jsonb(p_incidents)))
$function$;

create or replace function private.bpay_next_query_system_witnesses_v1(p_producer_id uuid,p_cycle uuid,p_publication uuid,p_actor uuid,p_incidents uuid[])
returns table(witness_ordinal bigint,witness_kind text,discrepancy_event_id uuid,evidence_sha256 bytea)
language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;raw record;selected uuid;stage record;
 comparison public.weekly_issue_comparison_revisions%rowtype;accepted public.weekly_discrepancy_events%rowtype;
 resolved public.weekly_discrepancy_events%rowtype;ordinal bigint:=0;
begin
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id;
 if owned.owner_kind<>'SYSTEM_HOURS_ACCEPT' or p_actor is null or p_cycle is null or p_publication is null
   or p_incidents is null or cardinality(p_incidents) not between 1 and 1000
   or array_position(p_incidents,null) is not null
   or p_incidents is distinct from (select array_agg(id order by id) from(select distinct id from unnest(p_incidents) id) ids) then
  raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_SELECTION_UNBOUND';end if;
 select * into strict raw from private.bpay_next_query_raw_stages_v1(owned.id);
 if raw.actual_stage_count<>cardinality(p_incidents)
   or (select count(*) from private.bpay_next_query_event_owner eo where eo.producer_id=owned.id)<>raw.actual_stage_count
   or exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=owned.preparing_transaction_id
     and (e.module_epoch<>owned.module_epoch or e.input_kind<>'PAY_QUERY' or not exists(
       select 1 from private.bpay_next_query_event_owner eo where eo.event_id=e.id and eo.producer_id=owned.id)))
   or p_incidents is distinct from (select array_agg(e.owner_row_id order by e.owner_row_id)
     from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id where eo.producer_id=owned.id)
   or exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event e on e.id=eo.event_id
     join private.bpay_next_query_input_version q on q.event_id=e.id where eo.producer_id=owned.id and
     (e.owner_relation<>'public.weekly_discrepancy_incidents' or not e.old_present or not e.new_present
       or q.owner_relation<>e.owner_relation or q.owner_row_id<>e.owner_row_id
       or q.root_timesheet_id is not null or q.candidate_id::text is distinct from q.old_gate_tuple->>'candidate_id'
       or q.old_gate_tuple->>'state' is distinct from 'OPEN' or q.new_gate_tuple->>'state' is distinct from 'RESOLVED'
       or q.old_gate_tuple->>'source_cycle_id' is distinct from p_cycle::text
       or (q.old_gate_tuple-'state') is distinct from (q.new_gate_tuple-'state')
       or e.old_sha256<>sha256(convert_to(q.old_gate_tuple::text,'UTF8'))
       or e.new_sha256<>sha256(convert_to(q.new_gate_tuple::text,'UTF8')))) then
  raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_STAGE_SET_INVALID';end if;
 foreach selected in array p_incidents loop
  select q.* into strict stage from private.bpay_next_query_event_owner eo
    join private.bpay_next_input_version_event e on e.id=eo.event_id join private.bpay_next_query_input_version q on q.event_id=e.id
    where eo.producer_id=owned.id and e.owner_row_id=selected;
  select c.* into strict comparison from public.weekly_issue_comparison_revisions c
    where c.id=(stage.old_gate_tuple->>'current_comparison_revision_id')::uuid and c.incident_id=selected;
  select e.* into strict accepted from public.weekly_discrepancy_events e where e.event_kind='OFFICE_ACCEPTED'
    and e.idempotency_key='OFFICE_ACCEPTED:'||selected::text||':'||encode(comparison.material_comparison_fingerprint,'hex');
  select e.* into strict resolved from public.weekly_discrepancy_events e where e.event_kind='RESOLVED'
    and e.idempotency_key='OFFICE_ACCEPTED_RESOLVED:'||selected::text||':'||encode(comparison.material_comparison_fingerprint,'hex');
  if (accepted.incident_id,accepted.projection_publication_id,accepted.expected_comparison_fingerprint,accepted.actor_kind,accepted.actor_user_id,
      accepted.bounded_payload_json->>'resolution') is distinct from
     (selected,p_publication,comparison.material_comparison_fingerprint,'OFFICE'::text,p_actor,'OFFICE_ACCEPTED_SYSTEM_HOURS'::text)
    or (resolved.incident_id,resolved.issue_episode,resolved.projection_publication_id,resolved.expected_comparison_fingerprint,
      resolved.actor_kind,resolved.actor_user_id,resolved.bounded_payload_json->>'resolution') is distinct from
     (accepted.incident_id,accepted.issue_episode,accepted.projection_publication_id,accepted.expected_comparison_fingerprint,
      accepted.actor_kind,accepted.actor_user_id,accepted.bounded_payload_json->>'resolution') then
    raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_ORIGINAL_EVENT_UNBOUND';end if;
  ordinal:=ordinal+1;witness_ordinal:=ordinal;witness_kind:='OFFICE_ACCEPTED';discrepancy_event_id:=accepted.id;
  evidence_sha256:=private.bpay_next_query_original_evidence_digest_v1(witness_kind,null,discrepancy_event_id);return next;
  ordinal:=ordinal+1;witness_ordinal:=ordinal;witness_kind:='OFFICE_RESOLVED';discrepancy_event_id:=resolved.id;
  evidence_sha256:=private.bpay_next_query_original_evidence_digest_v1(witness_kind,null,discrepancy_event_id);return next;
 end loop;
end
$function$;

create or replace function private.bpay_next_query_root_completion_assert_v1(p_completion_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare header private.bpay_next_query_root_completion%rowtype;owned private.bpay_next_query_producer%rowtype;
 raw record;actual_count bigint;witness_digest bytea;expected_count bigint;expected_digest bytea;
begin
 select h.* into strict header from private.bpay_next_query_root_completion h where h.id=p_completion_id;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=header.id;
 select * into strict raw from private.bpay_next_query_raw_stages_v1(owned.id);
 if (header.preparing_transaction_id,header.module_epoch,header.owner_kind,header.request_sha256) is distinct from
    (owned.preparing_transaction_id,owned.module_epoch,owned.owner_kind,owned.request_sha256)
   or header.owner_kind<>'SYSTEM_HOURS_ACCEPT' or owned.mode<>'STANDALONE_QUERY'
   or cardinality(owned.owner_stack)<>1
   or header.root_variant not in ('DIRECT_SYSTEM_HOURS','BULK_SYSTEM_HOURS')
   or owned.cause_key is distinct from (case header.root_variant when 'DIRECT_SYSTEM_HOURS' then 'SYSTEM_HOURS_DIRECT:' else 'SYSTEM_HOURS_BULK:' end)||encode(owned.request_sha256,'hex')
   or header.child_action_sha256 is distinct from private.bpay_next_query_system_action_digest_v1(header.actor_user_id,header.source_cycle_id,
     header.projection_publication_id,header.input_identities)
   or (header.root_variant='DIRECT_SYSTEM_HOURS' and header.request_sha256<>header.child_action_sha256)
   or (header.actual_stage_count,header.first_stage_ordinal,header.last_stage_ordinal,header.raw_stages_sha256) is distinct from
    (raw.actual_stage_count,raw.first_stage_ordinal,raw.last_stage_ordinal,raw.raw_stages_sha256)
   or header.input_identities_sha256<>sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_INPUT_IDENTITIES_V1',header.input_identities)::text,'UTF8'))
   or header.omitted_incidents_sha256<>sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_OMITTED_INCIDENTS_V1',header.omitted_incident_ids)::text,'UTF8'))
   or header.omitted_incident_ids<>'{}'::uuid[]
   or header.completion_sha256<>private.bpay_next_query_root_completion_digest_v1(header) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_ROOT_COMPLETION_UNBOUND';end if;
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,w.comparison_revision_id,
    w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into actual_count,witness_digest from private.bpay_next_query_root_completion_witness w where w.completion_id=header.id;
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,null::uuid,
    w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into expected_count,expected_digest from private.bpay_next_query_system_witnesses_v1(owned.id,header.source_cycle_id,
   header.projection_publication_id,header.actor_user_id,header.input_identities) w;
 if (actual_count,witness_digest) is distinct from (expected_count,expected_digest)
    or (actual_count,witness_digest) is distinct from (header.original_witness_count,header.original_witnesses_sha256)
    or exists(select 1 from private.bpay_next_query_root_completion_witness w where w.completion_id=header.id
     and w.evidence_sha256 is distinct from private.bpay_next_query_original_evidence_digest_v1(w.witness_kind,w.comparison_revision_id,w.discrepancy_event_id)) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_COMPLETION_WITNESS_SET_INVALID';end if;
end
$function$;

create or replace function private.bpay_next_query_completion_insert_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;completion uuid;
begin
 if tg_table_name='bpay_next_query_root_completion' then completion:=new.id;else completion:=new.completion_id;end if;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=completion;
 if owned.preparing_transaction_id<>pg_current_xact_id() or owned.phase<>'BUSINESS_OPEN' or owned.mode<>'STANDALONE_QUERY'
   or cardinality(owned.owner_stack)<>1 or owned.owner_kind not in ('SYSTEM_HOURS_ACCEPT','QUERY_SYNC')
   or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
      and t.module_epoch=owned.module_epoch and t.phase='OPEN')
   or not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=owned.module_epoch)
   or owned.completion_witness_id is not null then
  raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPLETION_INSERT_NOT_OPEN';end if;
 if tg_table_name='bpay_next_query_root_completion' then
  if (new.preparing_transaction_id,new.module_epoch,new.owner_kind,new.request_sha256) is distinct from
     (owned.preparing_transaction_id,owned.module_epoch,owned.owner_kind,owned.request_sha256)
    or new.completed_at_utc is distinct from transaction_timestamp() then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_COMPLETION_INSERT_SCOPE_INVALID';end if;
 else
  if new.evidence_sha256 is distinct from private.bpay_next_query_original_evidence_digest_v1(
      new.witness_kind,new.comparison_revision_id,new.discrepancy_event_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_COMPLETION_INSERT_EVIDENCE_INVALID';end if;
 end if;
 return new;
end
$function$;

create or replace function private.bpay_next_query_system_complete_v1(p_producer_id uuid,p_variant text,p_cycle uuid,p_publication uuid,
 p_actor uuid,p_incidents uuid[],p_child_sha256 bytea)
returns bytea language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;header private.bpay_next_query_root_completion%rowtype;raw record;
 actual_count bigint;witness_digest bytea;
begin
 if p_producer_id is null then return null;end if;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id and p.preparing_transaction_id=pg_current_xact_id() for update;
 if p_variant is null or p_variant not in ('DIRECT_SYSTEM_HOURS','BULK_SYSTEM_HOURS')
   or owned.owner_kind<>'SYSTEM_HOURS_ACCEPT' or owned.phase<>'BUSINESS_OPEN' or owned.mode<>'STANDALONE_QUERY'
   or cardinality(owned.owner_stack)<>1 then
  raise exception using errcode='55000',message='BPAY_NEXT_SYSTEM_COMPLETION_OWNER_INVALID';end if;
 select * into strict raw from private.bpay_next_query_raw_stages_v1(owned.id);
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,null::uuid,
    w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into actual_count,witness_digest from private.bpay_next_query_system_witnesses_v1(owned.id,p_cycle,p_publication,p_actor,p_incidents) w;
 header.id:=owned.id;header.preparing_transaction_id:=owned.preparing_transaction_id;header.module_epoch:=owned.module_epoch;
 header.owner_kind:=owned.owner_kind;header.root_variant:=p_variant;header.request_sha256:=owned.request_sha256;header.child_action_sha256:=p_child_sha256;
 header.source_cycle_id:=p_cycle;header.projection_publication_id:=p_publication;header.actor_user_id:=p_actor;
 header.input_identity_count:=cardinality(p_incidents);header.input_identities:=p_incidents;
 header.input_identities_sha256:=sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_INPUT_IDENTITIES_V1',p_incidents)::text,'UTF8'));
 header.omitted_incident_count:=0;header.omitted_incident_ids:='{}'::uuid[];
 header.omitted_incidents_sha256:=sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_OMITTED_INCIDENTS_V1',header.omitted_incident_ids)::text,'UTF8'));
 header.new_incident_count:=0;header.changed_comparison_count:=0;header.unchanged_incident_count:=0;header.resolved_incident_count:=cardinality(p_incidents);
 header.actual_stage_count:=raw.actual_stage_count;header.first_stage_ordinal:=raw.first_stage_ordinal;header.last_stage_ordinal:=raw.last_stage_ordinal;
 header.raw_stages_sha256:=raw.raw_stages_sha256;header.original_witness_count:=actual_count;header.original_witnesses_sha256:=witness_digest;
 header.completion_sha256:=private.bpay_next_query_root_completion_digest_v1(header);header.completed_at_utc:=transaction_timestamp();
 insert into private.bpay_next_query_root_completion select (header).*;
 insert into private.bpay_next_query_root_completion_witness(completion_id,witness_ordinal,witness_kind,discrepancy_event_id,evidence_sha256)
  select header.id,w.witness_ordinal,w.witness_kind,w.discrepancy_event_id,w.evidence_sha256
  from private.bpay_next_query_system_witnesses_v1(owned.id,p_cycle,p_publication,p_actor,p_incidents) w;
 perform private.bpay_next_query_root_completion_assert_v1(header.id);
 return header.completion_sha256;
end
$function$;

create or replace function private.bpay_next_query_completion_witness_assert_v1(p_producer_id uuid,p_witness_id uuid,p_witness_sha256 bytea)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare owned private.bpay_next_query_producer%rowtype;original private.weekly_source_manual_review_commands%rowtype;
 app_receipt public.weekly_candidate_app_mutation_receipts%rowtype;
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
 elsif owned.owner_kind in ('MANAGER_RESPONSE','CANDIDATE_RESPONSE') then
   if p_witness_id::text is distinct from owned.cause_key
     or p_witness_sha256 is distinct from private.bpay_next_query_response_completion_digest_v1(owned.id) then
     raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESPONSE_COMPLETION_UNBOUND';end if;
 elsif owned.owner_kind='CANDIDATE_APP_SUBMIT' then
   select receipt.* into app_receipt from public.weekly_candidate_app_mutation_receipts receipt where receipt.id=p_witness_id;
   if not found or app_receipt.mutation_kind<>'FINAL_SUBMIT' or app_receipt.idempotency_key::text is distinct from owned.cause_key
     or app_receipt.request_hash is distinct from owned.request_sha256
     or pg_catalog.sha256(pg_catalog.convert_to(app_receipt.response_json::text,'UTF8')) is distinct from p_witness_sha256
     or exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_query_input_version q on q.event_id=eo.event_id
       where eo.producer_id=owned.id and q.candidate_id is distinct from app_receipt.candidate_id) then
     raise exception using errcode='23514',message='BPAY_NEXT_QUERY_APP_COMPLETION_UNBOUND';end if;
 elsif owned.owner_kind='SYSTEM_HOURS_ACCEPT' then
   if p_witness_id is distinct from owned.id or not exists(select 1 from private.bpay_next_query_root_completion h
     where h.id=owned.id and h.completion_sha256=p_witness_sha256) then
     raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_COMPLETION_WITNESS_UNBOUND';end if;
   perform private.bpay_next_query_root_completion_assert_v1(owned.id);
 else
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPLETION_OWNER_NOT_INSTALLED';
 end if;
end
$function$;


create or replace function private.bpay_next_query_producer_enter_v1(
 p_owner_kind text,p_cause_kind text,p_cause_key text,p_request_sha256 bytea,p_mode text
) returns table(producer_id uuid,is_outer boolean)
language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;epoch bigint;current_phase text;parent text;
begin
 if p_owner_kind is null or p_owner_kind not in ('MANUAL_OPEN','MANUAL_RESOLVE','SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE','QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_FIRST_AUTHORISE','SOURCE_PUBLISH','SOURCE_PENDING_RELEASE','SOURCE_LOCAL_COMPLETE','SOURCE_LATER_CHANGE','SOURCE_FINAL','SOURCE_CORRECT_FINAL','SOURCE_UPLOAD','SOURCE_ROWS_APPLY','SOURCE_PROTECTED_NEXT','SOURCE_C1_COMPLETE') or p_cause_kind is distinct from p_owner_kind
   or p_cause_key is null or pg_catalog.octet_length(p_cause_key) not between 1 and 256
   or p_request_sha256 is null or pg_catalog.octet_length(p_request_sha256)<>32
   or p_mode is null or p_mode not in ('STANDALONE_QUERY','SOURCE_PUBLICATION') then
   raise exception using errcode='22023',message='BPAY_NEXT_QUERY_PRODUCER_INPUT_INVALID';end if;
 select t.phase into current_phase from private.bpay_next_input_transaction t
   where t.preparing_transaction_id=pg_catalog.pg_current_xact_id();
 select p.* into owned from private.bpay_next_query_producer p
   where p.preparing_transaction_id=pg_catalog.pg_current_xact_id();
 if found then
   if owned.phase<>'BUSINESS_OPEN' or current_phase is distinct from 'OPEN' or pg_catalog.cardinality(owned.owner_stack)>=16 then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_NOT_OPEN';end if;
   parent:=owned.owner_stack[pg_catalog.cardinality(owned.owner_stack)];
   if not ((parent='CANDIDATE_APP_SUBMIT' and p_owner_kind in ('CANDIDATE_RESPONSE','CANDIDATE_MATERIALISE'))
     or (parent='CANDIDATE_SELF_SUBMIT' and p_owner_kind='CANDIDATE_MATERIALISE')
     or (parent='CANDIDATE_MATERIALISE' and p_owner_kind='QUERY_SYNC')
     or (parent in ('SOURCE_UPLOAD','SOURCE_ROWS_APPLY') and p_owner_kind='QUERY_SYNC')
     or (parent in ('SOURCE_FINAL','SOURCE_CORRECT_FINAL') and p_owner_kind='MANUAL_RESOLVE')
     or (parent='SOURCE_LOCAL_COMPLETE' and p_owner_kind in ('SOURCE_PUBLISH','MANUAL_RESOLVE'))
     or (parent in ('SOURCE_LATER_CHANGE','SOURCE_PENDING_RELEASE') and p_owner_kind='SOURCE_PUBLISH')
     or (parent='SOURCE_PUBLISH' and p_owner_kind='SOURCE_PUBLISH')
     or (parent='SYSTEM_HOURS_ACCEPT' and p_owner_kind='SYSTEM_HOURS_ACCEPT')) then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_EDGE_INVALID';end if;
   update private.bpay_next_query_producer p set owner_stack=pg_catalog.array_append(p.owner_stack,p_owner_kind)
     where p.id=owned.id and p.preparing_transaction_id=pg_catalog.pg_current_xact_id() and p.phase='BUSINESS_OPEN';
   if not found then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_NESTED_OWNER_CHANGED';end if;
   producer_id:=owned.id;is_outer:=false;return next;return;
 end if;
 if current_phase is not null and current_phase<>'OPEN' then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_TRANSACTION_NOT_FRESH';end if;
 -- True root is responsible for pure auth/shape/replay before this call.
 -- This stable module decision precedes its first business/session/FK lock.
 perform private.weekly_source_pay_query_admit_v2();
 select m.owner_epoch into epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
 if epoch is null then
   -- Preserve shared Source under the stable other-owner decision too.
   perform 1 from private.bpay_next_module_control m where m.id=1 for share;
   if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MODULE_DECISION_CHANGED';end if;
   producer_id:=null;is_outer:=false;return next;return;
 end if;
 if exists(select 1 from private.bpay_next_input_version_event e where e.preparing_transaction_id=pg_catalog.pg_current_xact_id())
   or exists(select 1 from public.banking_pay_scope_change_transactions x where x.state='PENDING'
     and x.xmin::text::numeric=mod(pg_catalog.pg_current_xact_id()::text::numeric,4294967296)) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MIXED_TRANSACTION_UNSUPPORTED';end if;
 insert into private.bpay_next_input_transaction(preparing_transaction_id,module_epoch,phase)
   values(pg_catalog.pg_current_xact_id(),epoch,'OPEN') on conflict(preparing_transaction_id) do nothing;
 if not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=pg_catalog.pg_current_xact_id()
   and t.module_epoch=epoch and t.phase='OPEN' and t.next_event_ordinal=0) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_TRANSACTION_OWNER_INVALID';end if;
 insert into private.bpay_next_query_producer(preparing_transaction_id,module_epoch,owner_kind,cause_kind,cause_key,
   request_sha256,mode,phase,owner_stack) values(pg_catalog.pg_current_xact_id(),epoch,p_owner_kind,p_cause_kind,p_cause_key,
   p_request_sha256,p_mode,'BUSINESS_OPEN',array[p_owner_kind]) returning id into producer_id;
 is_outer:=true;return next;
end
$function$;


create or replace function public.weekly_source_query_accept_system_hours_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_guard jsonb;
  v_requested uuid[];
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_count integer:=0;
  v_query_producer uuid;v_query_outer boolean;v_query_hash bytea;v_query_completion bytea;v_query_result jsonb;
  v_query_event public.weekly_discrepancy_events%rowtype;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('actor_user_id','source_cycle_id','projection_publication_id','incident_ids'))
     or pg_catalog.jsonb_typeof(p_request->'incident_ids')<>'array' then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    select pg_catalog.array_agg(value::uuid order by value::uuid) into v_requested
    from pg_catalog.jsonb_array_elements_text(p_request->'incident_ids');
  exception when others then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_REQUEST_INVALID' using errcode='22023';
  end;
  if coalesce(pg_catalog.array_length(v_requested,1),0)=0 then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_INVALID' using errcode='22023';
  end if;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(
    v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date
  );
  v_query_hash:=private.bpay_next_query_system_action_digest_v1(v_actor,v_cycle_id,v_publication_id,v_requested);
  select producer_id,is_outer into v_query_producer,v_query_outer from private.bpay_next_query_producer_enter_v1(
    'SYSTEM_HOURS_ACCEPT','SYSTEM_HOURS_ACCEPT','SYSTEM_HOURS_DIRECT:'||pg_catalog.encode(v_query_hash,'hex'),v_query_hash,'STANDALONE_QUERY');
  for v_incident in
    select incident.* from public.weekly_discrepancy_incidents incident
    where incident.id=any(v_requested) order by incident.id for update
  loop
    v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
    perform private.weekly_source_office_authority_v1(
      v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
      (v_guard->>'finalisation_week_ending')::date);
    if v_incident.source_cycle_id<>v_cycle_id or v_incident.state<>'OPEN' then
      raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_comparison from public.weekly_issue_comparison_revisions
    where id=v_incident.current_comparison_revision_id;
    update public.weekly_discrepancy_incidents
    set state='RESOLVED',reconciliation_state='RECONCILED',
        candidate_action_state='NOT_REQUIRED',manager_potential_state='NOT_REQUIRED',
        manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
        resolved_at_utc=pg_catalog.transaction_timestamp(),
        resolution_kind='OFFICE_ACCEPTED_SYSTEM_HOURS'
    where id=v_incident.id;
    update public.weekly_candidate_outreach_memberships set state='RESOLVED'
    where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
    update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
    where incident_id=v_incident.id and response_state='UNANSWERED';
    update public.office_action_notifications set operational_state='RESOLVED',
      resolved_at_utc=pg_catalog.transaction_timestamp()
    where issue_id=v_incident.id and operational_state='OPEN';
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'OFFICE_ACCEPTED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,actor_user_id,
      bounded_payload_json,idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'RESOLVED','OFFICE',v_actor,
      pg_catalog.jsonb_build_object('resolution','OFFICE_ACCEPTED_SYSTEM_HOURS'),
      'OFFICE_ACCEPTED_RESOLVED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex')
    ) on conflict (event_kind,idempotency_key) do nothing;
    if v_query_producer is not null then
      -- ON CONFLICT keeps the original row; qualify both actual immutable IDs
      -- against the original locked incident/episode, never a guessed replay.
      for v_query_event in select e.* from public.weekly_discrepancy_events e where
        (e.event_kind='OFFICE_ACCEPTED' and e.idempotency_key='OFFICE_ACCEPTED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex'))
        or (e.event_kind='RESOLVED' and e.idempotency_key='OFFICE_ACCEPTED_RESOLVED:'||v_incident.id::text||':'||pg_catalog.encode(v_comparison.material_comparison_fingerprint,'hex'))
      loop
        if (v_query_event.incident_id,v_query_event.issue_episode,v_query_event.projection_publication_id,
            v_query_event.expected_comparison_fingerprint,v_query_event.actor_kind,v_query_event.actor_user_id,v_query_event.bounded_payload_json->>'resolution')
          is distinct from (v_incident.id,v_incident.episode_number,v_publication_id,v_comparison.material_comparison_fingerprint,
            'OFFICE'::text,v_actor,'OFFICE_ACCEPTED_SYSTEM_HOURS'::text) then
          raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_ORIGINAL_EVENT_UNBOUND';end if;
      end loop;
    end if;
    v_count:=v_count+1;
  end loop;
  if v_count<>pg_catalog.array_length(v_requested,1) then
    raise exception 'WEEKLY_SOURCE_ACCEPT_SYSTEM_SELECTION_STALE' using errcode='40001';
  end if;
  perform public._audit_insert(
    'weekly_source_query_resolution',v_publication_id::text,
    'WEEKLY_SOURCE_SYSTEM_HOURS_ACCEPTED',null,
    pg_catalog.jsonb_build_object('incident_count',v_count),
    'Office accepted system hours',v_actor
  );
  v_query_result:=pg_catalog.jsonb_build_object('ok',true,'resolved_count',v_count);
  if v_query_producer is not null then
    -- Child qualifies the whole real pair/stage set before leaving; only the
    -- actual root may persist completion and receive the one metadata command.
    perform 1 from private.bpay_next_query_system_witnesses_v1(v_query_producer,v_cycle_id,v_publication_id,v_actor,v_requested);
    if v_query_outer then
      v_query_completion:=private.bpay_next_query_system_complete_v1(v_query_producer,'DIRECT_SYSTEM_HOURS',v_cycle_id,v_publication_id,v_actor,v_requested,v_query_hash);
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'SYSTEM_HOURS_ACCEPT',v_query_producer,v_query_completion);
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'SYSTEM_HOURS_ACCEPT');end if;
  end if;
  return v_query_result;
end;
$function$;


create or replace function public.weekly_source_office_bulk_query_action_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','projection_publication_id',
    'expected_workspace_version','action','selection'
  ]::text[];
  v_selection_allowed constant text[]:=array[
    'mode','group_keys','excluded_group_keys','incident_ids','filters',
    'sort_key','sort_direction','selection_proof','group_selection_proofs'
  ]::text[];
  v_filter_allowed constant text[]:=array['status','candidate','issue']::text[];
  v_unknown text;
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_expected_version text;
  v_current_version text;
  v_action text;
  v_selection jsonb;
  v_mode text;
  v_filters jsonb;
  v_group_selection_proofs jsonb;
  v_selection_proof text;
  v_expected_selection_proof text;
  v_group_keys text[]:='{}'::text[];
  v_excluded_keys text[]:='{}'::text[];
  v_current_keys text[]:='{}'::text[];
  v_selected_keys text[]:='{}'::text[];
  v_incident_ids uuid[]:='{}'::uuid[];
  v_cycle public.weekly_source_cycles%rowtype;
  v_guard jsonb;
  v_group record;
  v_candidate record;
  v_client record;
  v_route record;
  v_scopes jsonb;
  v_exact_incidents uuid[];
  v_owner_result jsonb;
  v_results jsonb:='[]'::jsonb;
  v_exclusions jsonb:='[]'::jsonb;
  v_selected_count integer:=0;
  v_included_count integer:=0;
  v_included_issue_count integer:=0;
  v_excluded_count integer:=0;
  v_candidate_available boolean;
  v_candidate_results jsonb;
  v_candidate_issue_count integer;
  v_candidate_failed boolean;
  v_message text;
  v_query_producer uuid;v_query_outer boolean;v_query_hash bytea;v_query_child_hash bytea;v_query_completion bytea;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_expected_version:=p_request->>'expected_workspace_version';
    v_action:=pg_catalog.upper(pg_catalog.btrim(p_request->>'action'));
    v_selection:=p_request->'selection';
  exception when others then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_publication_id is null
     or coalesce(v_expected_version,'')!~ '^[0-9a-f]{64}$'
     or v_action not in ('ASK_CANDIDATES','SEND_MANAGER_NOW','ACCEPT_SYSTEM_HOURS')
     or pg_catalog.jsonb_typeof(v_selection)<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(v_selection) key
  where not (key=any(v_selection_allowed)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  v_mode:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_selection->>'mode','')));
  if v_mode not in ('ALL_FILTERED','EXPLICIT')
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'group_keys','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'excluded_group_keys','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'incident_ids','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'group_selection_proofs','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'filters','{}'::jsonb))<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;
  v_filters:=coalesce(v_selection->'filters','{}'::jsonb);
  v_group_selection_proofs:=coalesce(v_selection->'group_selection_proofs','[]'::jsonb);
  v_selection_proof:=nullif(pg_catalog.lower(pg_catalog.btrim(v_selection->>'selection_proof')),'');
  select key into v_unknown from pg_catalog.jsonb_object_keys(v_filters) key
  where not (key=any(v_filter_allowed)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_FILTER_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if exists(
    select 1 from pg_catalog.jsonb_each(v_filters) item
    where pg_catalog.jsonb_typeof(item.value)<>'string'
  )
     or coalesce(v_filters->>'status','UNRESOLVED') not in ('UNRESOLVED','ALL')
     or coalesce(v_filters->>'issue','ALL') not in ('ALL','TIMESHEET_MISSING','HOURS_DIFFER','MISSING_SHIFT')
     or pg_catalog.char_length(coalesce(v_filters->>'candidate',''))>120
     or coalesce(v_selection->>'sort_key','candidate') not in (
       'candidate','client','issues','candidate_asked','manager_informed','status','age'
     )
     or pg_catalog.lower(coalesce(v_selection->>'sort_direction','asc')) not in ('asc','desc')
     or (v_selection_proof is not null and v_selection_proof!~ '^[0-9a-f]{64}$') then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;
  begin
    select coalesce(pg_catalog.array_agg(value order by value),'{}'::text[]) into v_group_keys
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'group_keys','[]'::jsonb)) value;
    select coalesce(pg_catalog.array_agg(value order by value),'{}'::text[]) into v_excluded_keys
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'excluded_group_keys','[]'::jsonb)) value;
    select coalesce(pg_catalog.array_agg(value::uuid order by value::uuid),'{}'::uuid[]) into v_incident_ids
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'incident_ids','[]'::jsonb)) value;
  exception when others then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end;
  if exists(select 1 from pg_catalog.unnest(v_group_keys) key where key!~ '^qg_[0-9a-f]{64}$')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'group_keys','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'excluded_group_keys','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'incident_ids','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) item
       where pg_catalog.jsonb_typeof(item)<>'object'
          or exists(select 1 from pg_catalog.jsonb_object_keys(item) key
            where key not in ('group_key','selection_proof'))
          or pg_catalog.jsonb_typeof(item->'group_key')<>'string'
          or pg_catalog.jsonb_typeof(item->'selection_proof')<>'string'
          or coalesce(item->>'group_key','')!~ '^qg_[0-9a-f]{64}$'
          or coalesce(item->>'selection_proof','')!~ '^[0-9a-f]{64}$')
     or exists(select 1 from pg_catalog.unnest(v_excluded_keys) key where key!~ '^qg_[0-9a-f]{64}$')
     or pg_catalog.cardinality(v_group_keys)<>(select pg_catalog.count(distinct key) from pg_catalog.unnest(v_group_keys) key)
     or pg_catalog.cardinality(v_excluded_keys)<>(select pg_catalog.count(distinct key) from pg_catalog.unnest(v_excluded_keys) key)
     or pg_catalog.cardinality(v_incident_ids)<>(select pg_catalog.count(distinct id) from pg_catalog.unnest(v_incident_ids) id)
     or (v_mode='ALL_FILTERED' and pg_catalog.cardinality(v_group_keys)<>0)
     or (v_mode='EXPLICIT' and pg_catalog.cardinality(v_group_keys)=0 and pg_catalog.cardinality(v_incident_ids)=0)
     or (v_mode='EXPLICIT' and pg_catalog.cardinality(v_excluded_keys)<>0)
     or (v_mode='ALL_FILTERED' and pg_catalog.cardinality(v_incident_ids)<>0)
     or (v_action<>'ACCEPT_SYSTEM_HOURS' and pg_catalog.cardinality(v_incident_ids)<>0)
     or (v_action<>'ACCEPT_SYSTEM_HOURS' and pg_catalog.jsonb_array_length(v_group_selection_proofs)<>0)
     or (v_action='ACCEPT_SYSTEM_HOURS' and (
       v_mode<>'EXPLICIT' or pg_catalog.cardinality(v_group_keys)=0
       or pg_catalog.cardinality(v_incident_ids)=0
       or pg_catalog.jsonb_array_length(v_group_selection_proofs)<>pg_catalog.cardinality(v_group_keys)
       or (select pg_catalog.count(distinct item->>'group_key')
           from pg_catalog.jsonb_array_elements(v_group_selection_proofs) item)
          <>pg_catalog.cardinality(v_group_keys)
     )) then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;

  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  if v_action='ACCEPT_SYSTEM_HOURS' then
    perform private.weekly_source_office_authority_v1(v_actor,'ACCEPT_SYSTEM_HOURS',
      (v_guard->>'source_group_id')::uuid,null,(v_guard->>'finalisation_week_ending')::date);
    v_query_hash:=private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SYSTEM_HOURS_BULK_V1',pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,'source_cycle_id',v_cycle_id,'projection_publication_id',v_publication_id,
      'action',v_action,'expected_workspace_version',v_expected_version,'selection',pg_catalog.jsonb_build_object(
        'mode',v_mode,'group_keys',to_jsonb(v_group_keys),'excluded_group_keys',to_jsonb(v_excluded_keys),
        'incident_ids',to_jsonb(v_incident_ids),'filters',v_filters,'sort_key',coalesce(v_selection->>'sort_key','candidate'),
        'sort_direction',pg_catalog.lower(coalesce(v_selection->>'sort_direction','asc')),'selection_proof',v_selection_proof,
        'group_selection_proofs',(select coalesce(jsonb_agg(item order by item->>'group_key'),'[]'::jsonb)
          from pg_catalog.jsonb_array_elements(v_group_selection_proofs) item))));
    select producer_id,is_outer into v_query_producer,v_query_outer from private.bpay_next_query_producer_enter_v1(
      'SYSTEM_HOURS_ACCEPT','SYSTEM_HOURS_ACCEPT','SYSTEM_HOURS_BULK:'||pg_catalog.encode(v_query_hash,'hex'),v_query_hash,'STANDALONE_QUERY');
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  v_current_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication_id);
  if v_current_version is distinct from v_expected_version then
    raise exception 'WEEKLY_SOURCE_BULK_WORKSPACE_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,case v_action when 'SEND_MANAGER_NOW' then 'SEND_MANAGER'
      when 'ACCEPT_SYSTEM_HOURS' then 'ACCEPT_SYSTEM_HOURS' else 'ASK_CANDIDATES' end,
    (v_guard->>'source_group_id')::uuid,null,(v_guard->>'finalisation_week_ending')::date
  );
  select coalesce(pg_catalog.array_agg(query.group_key order by query.group_key),'{}'::text[])
  into v_current_keys
  from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query;
  v_expected_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
      'action',v_action,'source_cycle_id',v_cycle_id,
      'projection_publication_id',v_publication_id,
      'workspace_version',v_current_version,'filters',v_filters,
      'group_keys',to_jsonb(v_current_keys)
    )),'hex');
  if v_action in ('ASK_CANDIDATES','SEND_MANAGER_NOW')
     and v_selection_proof is distinct from v_expected_selection_proof then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
  end if;
  if v_mode='ALL_FILTERED' then
    if exists(select 1 from pg_catalog.unnest(v_excluded_keys) key where not (key=any(v_current_keys))) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    select coalesce(pg_catalog.array_agg(key order by key),'{}'::text[]) into v_selected_keys
    from pg_catalog.unnest(v_current_keys) key where not (key=any(v_excluded_keys));
  else
    if exists(select 1 from pg_catalog.unnest(v_group_keys) key where not (key=any(v_current_keys))) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    v_selected_keys:=v_group_keys;
  end if;
  v_selected_count:=pg_catalog.cardinality(v_selected_keys);

  if v_action='ACCEPT_SYSTEM_HOURS' then
    if exists(
      select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) proof
      where not ((proof->>'group_key')=any(v_selected_keys))
    ) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    for v_group in
      select query.group_key,query.accept_incident_ids
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
      order by query.group_key
    loop
      if pg_catalog.cardinality(v_group.accept_incident_ids)=0
         or not (v_group.accept_incident_ids && v_incident_ids)
         or not exists(
           select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) proof
           where proof->>'group_key'=v_group.group_key
             and proof->>'selection_proof'=pg_catalog.encode(
               private.weekly_source_sha256_jsonb_v1(
                 'WEEKLY_SOURCE_ACCEPT_GROUP_SELECTION_V1',pg_catalog.jsonb_build_object(
                   'workspace_version',v_current_version,
                   'group_key',v_group.group_key,
                   'incident_ids',to_jsonb(v_group.accept_incident_ids)
                 )
               ),'hex'
             )
         ) then
        raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
      end if;
    end loop;
    if exists(
      select 1
      from pg_catalog.unnest(v_incident_ids) selected_incident(incident_id)
      left join (
        select eligible_group.group_key,eligible_incident.incident_id
        from private.weekly_source_office_query_groups_v1(
          v_cycle_id,v_publication_id,v_filters
        ) eligible_group
        cross join lateral pg_catalog.unnest(
          eligible_group.accept_incident_ids
        ) eligible_incident(incident_id)
        where eligible_group.group_key=any(v_selected_keys)
      ) eligible on eligible.incident_id=selected_incident.incident_id
      group by selected_incident.incident_id
      having pg_catalog.count(eligible.group_key)<>1
    ) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    v_owner_result:=public.weekly_source_query_accept_system_hours_atomic_v1(
      pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
        'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_incident_ids))
    );
    if v_query_producer is not null then
      if not v_query_outer or v_owner_result is distinct from pg_catalog.jsonb_build_object('ok',true,'resolved_count',pg_catalog.cardinality(v_incident_ids)) then
        raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_BULK_CHILD_INCOMPLETE';end if;
      v_query_child_hash:=private.bpay_next_query_system_action_digest_v1(v_actor,v_cycle_id,v_publication_id,v_incident_ids);
      v_query_completion:=private.bpay_next_query_system_complete_v1(v_query_producer,'BULK_SYSTEM_HOURS',v_cycle_id,v_publication_id,v_actor,v_incident_ids,v_query_child_hash);
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'SYSTEM_HOURS_ACCEPT',v_query_producer,v_query_completion);
    end if;
    return pg_catalog.jsonb_build_object('ok',true,'action',v_action,'status','COMPLETE',
      'selected_count',v_selected_count,'included_count',v_selected_count,
      'included_issue_count',pg_catalog.cardinality(v_incident_ids),'excluded_count',0,
      'excluded','[]'::jsonb,'result',v_owner_result);
  end if;

  if v_action='ASK_CANDIDATES' then
    for v_group in
      select query.group_key
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
        and not (query.outreach_eligible or not query.candidate_app_available)
      order by query.group_key
    loop
      v_excluded_count:=v_excluded_count+1;
      if pg_catalog.jsonb_array_length(v_exclusions)<100 then
        v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'group_key',v_group.group_key,'status','UNAVAILABLE_FOR_CANDIDATE_QUERIES'
        ));
      end if;
    end loop;
    for v_candidate in
      select query.candidate_id,
        bool_and(query.candidate_app_available) candidate_app_available,
        pg_catalog.array_agg(query.group_key order by query.group_key) group_keys
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
        and (query.outreach_eligible or not query.candidate_app_available)
      group by query.candidate_id order by query.candidate_id
    loop
      select pg_catalog.count(*)=1 into v_candidate_available
      from public.candidate_app_global_membership_links membership
      join public.candidate_app_accounts account on account.id=membership.account_id
      where membership.candidate_id=v_candidate.candidate_id
        and membership.state='ACTIVE' and account.status='ACTIVE';
      if not v_candidate_available then
        v_excluded_count:=v_excluded_count+pg_catalog.cardinality(v_candidate.group_keys);
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'candidate_id',v_candidate.candidate_id,'group_keys',to_jsonb(v_candidate.group_keys),
            'status','UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT'
          ));
        end if;
        continue;
      end if;

      v_candidate_results:='[]'::jsonb;
      v_candidate_issue_count:=0;
      v_candidate_failed:=false;
      begin
        select coalesce(pg_catalog.jsonb_agg(scope.value order by
          (scope.value->>'week_ending')::date,(scope.value->>'client_id')::uuid,(scope.value->>'contract_id')::uuid),'[]'::jsonb)
        into v_scopes
        from (
          select distinct on (value->>'week_ending',value->>'client_id',value->>'contract_id') value
          from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
          cross join lateral pg_catalog.jsonb_array_elements(query.missing_scopes) value
          where query.group_key=any(v_selected_keys) and query.candidate_id=v_candidate.candidate_id
            and (query.outreach_eligible or not query.candidate_app_available)
          order by value->>'week_ending',value->>'client_id',value->>'contract_id',value::text
        ) scope;
        if pg_catalog.jsonb_array_length(v_scopes)>0 then
          v_owner_result:=public.weekly_source_timesheet_submission_request_start_atomic_v1(
            pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
              'projection_publication_id',v_publication_id,'candidate_id',v_candidate.candidate_id,
              'scopes',v_scopes)
          );
          v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
          v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.jsonb_array_length(v_scopes);
        end if;

        for v_client in
          select query.client_id,
            pg_catalog.array_agg(query.group_key order by query.group_key) group_keys,
            bool_or(pg_catalog.jsonb_array_length(query.missing_scopes)>0) has_missing_scope
          from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
          where query.group_key=any(v_selected_keys) and query.candidate_id=v_candidate.candidate_id
            and (query.outreach_eligible or not query.candidate_app_available)
          group by query.client_id order by query.client_id
        loop
          if v_client.has_missing_scope then continue; end if;
          select pg_catalog.array_agg(incident.id order by incident.id) into v_exact_incidents
          from public.weekly_discrepancy_incidents incident
          join public.weekly_issue_comparison_revisions comparison
            on comparison.id=incident.current_comparison_revision_id
          where incident.source_cycle_id=v_cycle_id
            and incident.candidate_id=v_candidate.candidate_id
            and incident.client_id=v_client.client_id and incident.state='OPEN'
            and not private.weekly_source_covered_hours_incident_v1(incident.id)
            and incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED');
          if coalesce(pg_catalog.cardinality(v_exact_incidents),0)=0 then continue; end if;
          v_owner_result:=public.weekly_source_query_ask_candidate_atomic_v1(
            pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
              'projection_publication_id',v_publication_id,'candidate_id',v_candidate.candidate_id,
              'client_id',v_client.client_id,'incident_ids',to_jsonb(v_exact_incidents))
          );
          v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
          v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.cardinality(v_exact_incidents);
        end loop;
      exception when sqlstate '55000' then
        get stacked diagnostics v_message=message_text;
        if v_message='WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE' then
          v_candidate_failed:=true;
        else
          raise;
        end if;
      end;

      if v_candidate_failed then
        v_excluded_count:=v_excluded_count+pg_catalog.cardinality(v_candidate.group_keys);
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'candidate_id',v_candidate.candidate_id,'group_keys',to_jsonb(v_candidate.group_keys),
            'status','UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT'
          ));
        end if;
      else
        v_results:=v_results||v_candidate_results;
        v_included_count:=v_included_count+pg_catalog.cardinality(v_candidate.group_keys);
        v_included_issue_count:=v_included_issue_count+v_candidate_issue_count;
      end if;
    end loop;
  else
    for v_group in
      select query.*
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
      order by query.group_key
    loop
      if not v_group.manager_eligible or pg_catalog.cardinality(v_group.incident_ids)=0
         or pg_catalog.jsonb_array_length(v_group.missing_scopes)>0 then
        v_excluded_count:=v_excluded_count+1;
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'group_key',v_group.group_key,'status','UNAVAILABLE_FOR_MANAGER'
          ));
        end if;
      else
        v_included_count:=v_included_count+1;
        v_included_issue_count:=v_included_issue_count+pg_catalog.cardinality(v_group.incident_ids);
      end if;
    end loop;
    for v_route in
      select query.manager_recipient_route_key,
        pg_catalog.array_agg(distinct incident_id order by incident_id) incident_ids
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      cross join lateral pg_catalog.unnest(query.incident_ids) incident_id
      where query.group_key=any(v_selected_keys) and query.manager_eligible
        and pg_catalog.jsonb_array_length(query.missing_scopes)=0
      group by query.manager_recipient_route_key
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex')
    loop
      v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
        pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
          'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
      );
      v_results:=v_results||pg_catalog.jsonb_build_array(v_owner_result);
    end loop;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'action',v_action,
    'status',case when v_included_count=0 then 'NO_ELIGIBLE_SELECTION'
      when v_excluded_count>0 then 'PARTLY_COMPLETE' else 'COMPLETE' end,
    'selected_count',v_selected_count,'included_count',v_included_count,
    'included_issue_count',v_included_issue_count,'excluded_count',v_excluded_count,
    'excluded',v_exclusions,'excluded_omitted_count',greatest(v_excluded_count-pg_catalog.jsonb_array_length(v_exclusions),0),
    'results',v_results,'workspace_version',v_current_version
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
end;
$function$;


alter function private.bpay_next_query_system_action_digest_v1(uuid,uuid,uuid,uuid[]) owner to postgres;
revoke all on function private.bpay_next_query_system_action_digest_v1(uuid,uuid,uuid,uuid[]) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_system_witnesses_v1(uuid,uuid,uuid,uuid,uuid[]) owner to postgres;
revoke all on function private.bpay_next_query_system_witnesses_v1(uuid,uuid,uuid,uuid,uuid[]) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_root_completion_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_root_completion_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_completion_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_query_completion_insert_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_query_system_complete_v1(uuid,text,uuid,uuid,uuid,uuid[],bytea) owner to postgres;
revoke all on function private.bpay_next_query_system_complete_v1(uuid,text,uuid,uuid,uuid,uuid[],bytea) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) owner to postgres;
revoke all on function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_producer_enter_v1(text,text,text,bytea,text) owner to postgres;
revoke all on function private.bpay_next_query_producer_enter_v1(text,text,text,bytea,text) from public,anon,authenticated,service_role;
alter function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_query_accept_system_hours_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb) to service_role;
drop trigger if exists bpay_next_query_completion_insert_guard_v1 on private.bpay_next_query_root_completion;
create trigger bpay_next_query_completion_insert_guard_v1 before insert on private.bpay_next_query_root_completion for each row execute function private.bpay_next_query_completion_insert_guard_v1();
drop trigger if exists bpay_next_query_completion_insert_guard_v1 on private.bpay_next_query_root_completion_witness;
create trigger bpay_next_query_completion_insert_guard_v1 before insert on private.bpay_next_query_root_completion_witness for each row execute function private.bpay_next_query_completion_insert_guard_v1();
notify pgrst,'reload schema';
commit;
