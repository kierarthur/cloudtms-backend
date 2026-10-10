-- F-25 same-xid QuerySync certificate admission and complete immutable closure.
-- Design: F-25-query-sync-completion-build.md. Actual writer is a separate successor.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_sync_certificate_insert_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;header private.bpay_next_query_root_completion%rowtype;
 incident public.weekly_discrepancy_incidents%rowtype;comparison public.weekly_issue_comparison_revisions%rowtype;
 group_id uuid;
begin
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=new.completion_id;
 select h.* into strict header from private.bpay_next_query_root_completion h where h.id=owned.id;
 if owned.preparing_transaction_id<>pg_current_xact_id() or owned.phase<>'BUSINESS_OPEN' or owned.mode<>'STANDALONE_QUERY'
   or owned.owner_kind<>'QUERY_SYNC' or owned.owner_stack is distinct from array['QUERY_SYNC']::text[]
   or owned.completion_witness_id is not null
   or (header.preparing_transaction_id,header.module_epoch,header.owner_kind,header.root_variant,header.request_sha256)
      is distinct from (owned.preparing_transaction_id,owned.module_epoch,'QUERY_SYNC'::text,'QUERY_SYNC'::text,owned.request_sha256)
   or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
      and t.module_epoch=owned.module_epoch and t.phase='OPEN')
   or not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=owned.module_epoch) then
  raise exception using errcode='55000',message='BPAY_NEXT_QUERY_SYNC_CERTIFICATE_INSERT_NOT_OPEN';end if;
 -- These are plain preclock checks of the original owner's actual locked/new
 -- rows. Historical assertion never re-reads these mutable lifecycle fields.
 select c.source_group_id into strict group_id from public.weekly_source_cycles c where c.id=header.source_cycle_id;
 select i.* into strict incident from public.weekly_discrepancy_incidents i where i.id=new.incident_id;
 select c.* into strict comparison from public.weekly_issue_comparison_revisions c where c.id=new.comparison_revision_id;
 if (incident.work_event_id,incident.episode_number,incident.current_comparison_revision_id,incident.source_group_id,comparison.incident_id)
     is distinct from (new.work_event_id,new.issue_episode,new.comparison_revision_id,group_id,new.incident_id)
   or not exists(select 1 from public.weekly_work_events w where w.id=new.work_event_id and w.first_source_group_id=group_id
      and w.candidate_id=incident.candidate_id and w.client_id=incident.client_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_CERTIFICATE_ACTUAL_SCOPE_INVALID';end if;
 if tg_table_name='bpay_next_query_sync_input_completion' then
  if not(new.work_event_id=any(header.input_identities))
    or incident.state is distinct from (case when new.outcome='UNCHANGED_RESOLVED' then 'RESOLVED' else 'OPEN' end)
    or new.evidence_sha256 is distinct from private.bpay_next_query_sync_input_completion_digest_v1(new::private.bpay_next_query_sync_input_completion)
    or (select count(*) from private.bpay_next_query_sync_input_completion r where r.completion_id=owned.id)>=header.input_identity_count then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_INPUT_CERTIFICATE_INSERT_INVALID';end if;
 else
  if not(new.incident_id=any(header.omitted_incident_ids)) or new.work_event_id=any(header.input_identities)
    or incident.state<>'RESOLVED' or incident.resolution_kind is distinct from 'SOURCE_MATCHED'
    or new.evidence_sha256 is distinct from private.bpay_next_query_sync_omission_completion_digest_v1(new::private.bpay_next_query_sync_omission_completion)
    or (select count(*) from private.bpay_next_query_sync_omission_completion r where r.completion_id=owned.id)>=header.omitted_incident_count then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_OMISSION_CERTIFICATE_INSERT_INVALID';end if;
 end if;
 return new;
end
$function$;

create or replace function private.bpay_next_query_sync_root_completion_assert_v1(p_completion_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare header private.bpay_next_query_root_completion%rowtype;owned private.bpay_next_query_producer%rowtype;
 actual_count bigint;actual_sha bytea;expected_count bigint;expected_sha bytea;
begin
 select h.* into strict header from private.bpay_next_query_root_completion h where h.id=p_completion_id;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=header.id;
 if (header.preparing_transaction_id,header.module_epoch,header.owner_kind,header.root_variant,header.request_sha256)
    is distinct from (owned.preparing_transaction_id,owned.module_epoch,'QUERY_SYNC'::text,'QUERY_SYNC'::text,owned.request_sha256)
   or owned.owner_kind<>'QUERY_SYNC' or owned.mode<>'STANDALONE_QUERY'
   or owned.owner_stack is distinct from array['QUERY_SYNC']::text[]
   or owned.cause_kind<>'QUERY_SYNC' or owned.cause_key is distinct from 'QUERY_SYNC:'||encode(owned.request_sha256,'hex')
   or header.child_action_sha256 is not null
   or header.input_identities_sha256 is distinct from sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_INPUT_IDENTITIES_V1',header.input_identities)::text,'UTF8'))
   or header.omitted_incidents_sha256 is distinct from sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_OMITTED_INCIDENTS_V1',header.omitted_incident_ids)::text,'UTF8'))
   or header.completion_sha256 is distinct from private.bpay_next_query_root_completion_digest_v1(header) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_ROOT_COMPLETION_UNBOUND';end if;
 perform private.bpay_next_query_sync_certificate_evidence_assert_v1(header.id);
 perform private.bpay_next_query_sync_stages_assert_v1(header.id);
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,w.comparison_revision_id,
   w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into actual_count,actual_sha from private.bpay_next_query_root_completion_witness w where w.completion_id=header.id;
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,w.comparison_revision_id,
   w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into expected_count,expected_sha from private.bpay_next_query_sync_expected_witnesses_v1(header.id) w;
 if expected_count>4000 or (actual_count,actual_sha) is distinct from (expected_count,expected_sha)
   or (actual_count,actual_sha) is distinct from (header.original_witness_count,header.original_witnesses_sha256)
   or exists(select 1 from private.bpay_next_query_root_completion_witness w where w.completion_id=header.id and
     w.evidence_sha256 is distinct from private.bpay_next_query_original_evidence_digest_v1(w.witness_kind,w.comparison_revision_id,w.discrepancy_event_id)) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_WITNESS_SET_INVALID';end if;
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
 if header.owner_kind='QUERY_SYNC' then
  perform private.bpay_next_query_sync_root_completion_assert_v1(header.id);return;end if;
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
 elsif owned.owner_kind in ('SYSTEM_HOURS_ACCEPT','QUERY_SYNC') then
   if p_witness_id is distinct from owned.id or not exists(select 1 from private.bpay_next_query_root_completion h
     where h.id=owned.id and h.completion_sha256=p_witness_sha256) then
     raise exception using errcode='23514',message='BPAY_NEXT_SYSTEM_COMPLETION_WITNESS_UNBOUND';end if;
   perform private.bpay_next_query_root_completion_assert_v1(owned.id);
 else
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPLETION_OWNER_NOT_INSTALLED';
 end if;
end
$function$;

alter function private.bpay_next_query_sync_certificate_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_query_sync_certificate_insert_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_root_completion_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_root_completion_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_root_completion_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_root_completion_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) owner to postgres;
revoke all on function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_query_sync_input_completion_insert_guard on private.bpay_next_query_sync_input_completion;
create trigger bpay_next_query_sync_input_completion_insert_guard before insert on private.bpay_next_query_sync_input_completion
 for each row execute function private.bpay_next_query_sync_certificate_insert_guard_v1();
drop trigger if exists bpay_next_query_sync_omission_completion_insert_guard on private.bpay_next_query_sync_omission_completion;
create trigger bpay_next_query_sync_omission_completion_insert_guard before insert on private.bpay_next_query_sync_omission_completion
 for each row execute function private.bpay_next_query_sync_certificate_insert_guard_v1();
notify pgrst,'reload schema';
commit;
