-- F-25 exact response Query true tails; immutable original response-set witness.
-- Includes the real Candidate app submit parent; QuerySync and Source closure remain pending.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_response_completion_digest_v1(p_producer_id uuid)
returns bytea language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;kind text;actor text;request_key uuid;actual_count bigint;result bytea;
begin
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id;
 if owned.owner_kind not in ('MANAGER_RESPONSE','CANDIDATE_RESPONSE') then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESPONSE_OWNER_INVALID';end if;
 request_key:=owned.cause_key::uuid;
 kind:=case owned.owner_kind when 'MANAGER_RESPONSE' then 'MANAGER_RESPONDED' else 'CANDIDATE_RESPONDED' end;
 actor:=case owned.owner_kind when 'MANAGER_RESPONSE' then 'MANAGER' else 'CANDIDATE' end;
 select count(*) into actual_count from public.weekly_discrepancy_events e where e.event_kind=kind
   and e.bounded_payload_json->>'request_idempotency_key'=request_key::text;
 if actual_count not between 1 and 1000 or exists(select 1 from public.weekly_discrepancy_events e where e.event_kind=kind
   and e.bounded_payload_json->>'request_idempotency_key'=request_key::text and
   (e.actor_kind<>actor or e.bounded_payload_json->>'request_item_count' is distinct from actual_count::text
     or e.bounded_payload_json->>'request_semantic_hash' is distinct from pg_catalog.encode(owned.request_sha256,'hex')
     or coalesce(e.bounded_payload_json->>'response_fingerprint','')!~'^[0-9a-f]{64}$'
     or e.idempotency_key is distinct from (case owned.owner_kind when 'MANAGER_RESPONSE'
       then 'MANAGER_RESPONSE:'||request_key::text||':'||(e.bounded_payload_json->>'review_item_id')
       else 'CANDIDATE_RESPONSE:'||request_key::text||':'||e.incident_id::text end))) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESPONSE_COMPLETION_INCOMPLETE';end if;
 if exists(select 1 from private.bpay_next_query_event_owner eo join private.bpay_next_input_version_event event on event.id=eo.event_id
   join private.bpay_next_query_input_version q on q.event_id=event.id where eo.producer_id=owned.id and
   (event.owner_relation<>'public.weekly_discrepancy_incidents' or not event.old_present or not event.new_present
     or q.old_gate_tuple->>'state' is distinct from 'OPEN' or q.new_gate_tuple->>'state' is distinct from 'RESOLVED'
     or (q.old_gate_tuple-'state') is distinct from (q.new_gate_tuple-'state')
     or not exists(select 1 from public.weekly_discrepancy_events e where e.event_kind=kind
       and e.bounded_payload_json->>'request_idempotency_key'=request_key::text and e.incident_id=event.owner_row_id
       and exists(select 1 from public.weekly_issue_comparison_revisions comparison
         where comparison.id=(q.old_gate_tuple->>'current_comparison_revision_id')::uuid
           and comparison.incident_id=event.owner_row_id and comparison.material_comparison_fingerprint=e.expected_comparison_fingerprint)
       and exists(select 1 from public.weekly_discrepancy_events resolved where resolved.event_kind='RESOLVED'
         and resolved.idempotency_key=(case owned.owner_kind when 'MANAGER_RESPONSE' then 'MANAGER_CONFIRMED:' else 'CANDIDATE_CORRECTED:' end)||e.id::text
         and resolved.incident_id=e.incident_id and resolved.issue_episode=e.issue_episode
         and resolved.projection_publication_id=e.projection_publication_id and resolved.expected_comparison_fingerprint=e.expected_comparison_fingerprint
         and resolved.actor_kind=e.actor_kind and resolved.bounded_payload_json->>'resolution'=(case owned.owner_kind when 'MANAGER_RESPONSE'
           then 'MANAGER_CONFIRMED_SYSTEM_HOURS' else 'CANDIDATE_CORRECTED' end))))) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESPONSE_GATE_SCOPE_INVALID';end if;
 select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_RESPONSE_COMPLETION_V1',owned.owner_kind,
   request_key,pg_catalog.encode(owned.request_sha256,'hex'),actual_count,jsonb_agg(jsonb_build_array(e.id,e.incident_id,
     e.issue_episode,e.projection_publication_id,pg_catalog.encode(e.expected_comparison_fingerprint,'hex'),e.event_kind,
     e.actor_kind,e.idempotency_key,e.bounded_payload_json->>'request_item_count',e.bounded_payload_json->>'request_semantic_hash',
     e.bounded_payload_json->>'response_fingerprint',e.bounded_payload_json->>'review_item_id',
     e.bounded_payload_json->>'response_kind',e.bounded_payload_json->>'choice',
     (select jsonb_build_array(resolved.id,resolved.incident_id,resolved.issue_episode,resolved.projection_publication_id,
       pg_catalog.encode(resolved.expected_comparison_fingerprint,'hex'),resolved.actor_kind,resolved.idempotency_key,
       resolved.bounded_payload_json->>'resolution') from public.weekly_discrepancy_events resolved where resolved.event_kind='RESOLVED'
       and resolved.idempotency_key=(case owned.owner_kind when 'MANAGER_RESPONSE' then 'MANAGER_CONFIRMED:' else 'CANDIDATE_CORRECTED:' end)||e.id::text)) order by e.id))::text,'UTF8')) into result
 from public.weekly_discrepancy_events e where e.event_kind=kind and e.bounded_payload_json->>'request_idempotency_key'=request_key::text;
 return result;
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
 else
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPLETION_OWNER_NOT_INSTALLED';
 end if;
end
$function$;

create or replace function public.weekly_source_manager_review_respond_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_query_producer uuid;v_query_outer boolean;v_query_result jsonb;
  v_credential_hash bytea;
  v_control_ticket uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_review_batch_version integer;
  v_response_fingerprint bytea;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_receipt public.weekly_manager_route_receipts%rowtype;
  v_batch public.weekly_manager_review_batches%rowtype;
  v_response jsonb;
  v_item public.weekly_manager_review_items%rowtype;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_kind text;
  v_expected bytea;
  v_response_hash bytea;
  v_event_id uuid;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_summary text;
  v_answered integer:=0;
  v_notices integer:=0;
  v_intended_start timestamp without time zone;
  v_intended_end timestamp without time zone;
  v_intended_break integer;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'credential_hash','control_plane_ticket_id',
                 'request_idempotency_key','review_batch_version','response_fingerprint','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_credential_hash:=private.weekly_source_query_hex32_v1(
      p_request->>'credential_hash','WEEKLY_SOURCE_MANAGER_CREDENTIAL_HASH_INVALID'
    );
    v_control_ticket:=(p_request->>'control_plane_ticket_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_review_batch_version:=(p_request->>'review_batch_version')::integer;
    v_response_fingerprint:=private.weekly_source_query_hex32_v1(
      p_request->>'response_fingerprint','WEEKLY_SOURCE_MANAGER_RESPONSE_FINGERPRINT_INVALID'
    );
    v_responses:=(select pg_catalog.jsonb_agg(value order by (value->>'review_item_id')::uuid)
                 from pg_catalog.jsonb_array_elements(p_request->'responses'));
  exception when others then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REQUEST_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RESPONSE_REQUEST_V1',pg_catalog.jsonb_build_object(
      'control_plane_ticket_id',v_control_ticket,
      'review_batch_version',v_review_batch_version,
      'response_fingerprint',pg_catalog.encode(v_response_fingerprint,'hex'),
      'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');
  -- Exact response replay wins before credential expiry or mutable fact checks.
  select pg_catalog.count(*),min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='MANAGER_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;
  -- Pure credential/scope eligibility before stable module and business locks.
  select receipt.* into strict v_receipt from public.weekly_manager_route_receipts receipt
    join public.weekly_manager_review_batches batch on batch.id=receipt.review_batch_id
    where receipt.credential_hash=v_credential_hash and receipt.control_plane_ticket_id=v_control_ticket;
  select b.* into strict v_batch from public.weekly_manager_review_batches b where b.id=v_receipt.review_batch_id;
  if v_receipt.state<>'ACTIVE' or v_batch.state<>'ACTIVE' or pg_catalog.transaction_timestamp()>=v_receipt.expires_at_utc then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';end if;
  if v_review_batch_version<>v_batch.credential_generation or v_response_fingerprint<>v_batch.sent_content_hash then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_BATCH_STALE' using errcode='40001';end if;
  select entry.producer_id,entry.is_outer into v_query_producer,v_query_outer
    from private.bpay_next_query_producer_enter_v1('MANAGER_RESPONSE','MANAGER_RESPONSE',v_request_key::text,v_semantic_hash,'STANDALONE_QUERY') entry;
  select receipt.* into strict v_receipt
  from public.weekly_manager_route_receipts receipt
  join public.weekly_manager_review_batches batch on batch.id=receipt.review_batch_id
  where receipt.credential_hash=v_credential_hash
    and receipt.control_plane_ticket_id=v_control_ticket
  for update of receipt;
  select * into strict v_batch from public.weekly_manager_review_batches
  where id=v_receipt.review_batch_id for update;
  if v_receipt.state<>'ACTIVE' or v_batch.state<>'ACTIVE'
     or pg_catalog.transaction_timestamp()>=v_receipt.expires_at_utc then
    raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
  end if;
  if v_review_batch_version<>v_batch.credential_generation
     or v_response_fingerprint<>v_batch.sent_content_hash then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_BATCH_STALE' using errcode='40001';
  end if;
  for v_response in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'review_item_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_response)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_response) key
                 where key not in (
                   'review_item_id','incident_episode','current_fact_version','response_kind',
                   'intended_start','intended_end','intended_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_item from public.weekly_manager_review_items
      where id=(v_response->>'review_item_id')::uuid and review_batch_id=v_batch.id
        and response_state='UNANSWERED' for update;
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=v_item.incident_id for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_response->>'current_fact_version','WEEKLY_SOURCE_MANAGER_FACT_VERSION_INVALID'
    );
    if v_incident.state<>'OPEN' or v_incident.episode_number<>(v_response->>'incident_episode')::integer
       or v_item.incident_episode<>v_incident.episode_number
       or private.weekly_source_covered_hours_incident_v1(v_incident.id)
       or v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_response->>'response_kind','')));
    if v_kind not in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK','MANAGER_CORRECTED_SOURCE')
       or (v_kind='SYSTEM_CORRECT' and v_comparison.issue_family<>'SOURCE_HOURS_DIFFER')
       or (v_kind='CANDIDATE_DID_NOT_WORK' and v_comparison.issue_family='SOURCE_HOURS_DIFFER') then
      raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_KIND_INVALID' using errcode='22023';
    end if;
    v_intended_start:=null;
    v_intended_end:=null;
    v_intended_break:=null;
    if v_kind='MANAGER_CORRECTED_SOURCE' then
      begin
        if coalesce(v_response->>'intended_start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_response->>'intended_end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
          raise exception 'invalid time';
        end if;
        v_intended_break:=(v_response->>'intended_break_minutes')::integer;
        v_intended_start:=v_work.work_date+(v_response->>'intended_start')::time;
        v_intended_end:=v_work.work_date+(v_response->>'intended_end')::time;
        -- 14 section 5.3.4 for the source side: "If start equals end, the row
        -- blocks because neither accepted profile supplies an explicit 24-hour
        -- marker."  The overnight roll below is for a genuine end-before-start
        -- shift; start equal to end is not a 24-hour shift and must not be
        -- recorded as one, now that Office reads this value.
        if v_intended_end=v_intended_start then raise exception 'equal start and end'; end if;
        if v_intended_end<v_intended_start then v_intended_end:=v_intended_end+interval '1 day'; end if;
        if v_intended_break<0 or v_intended_break>720
           or v_intended_end<=v_intended_start then raise exception 'invalid hours'; end if;
      exception when others then
        raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
      end;
    elsif v_response ? 'intended_start' or v_response ? 'intended_end'
       or v_response ? 'intended_break_minutes' then
      raise exception 'WEEKLY_SOURCE_MANAGER_INTENDED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_MANAGER_RESPONSE_ITEM_V1',pg_catalog.jsonb_build_object(
        'review_item_id',v_item.id,'incident_id',v_incident.id,
        'incident_episode',v_incident.episode_number,
        'current_fact_version',pg_catalog.encode(v_expected,'hex'),
        'response_kind',v_kind,
        'intended_start_at_local',v_intended_start,
        'intended_end_at_local',v_intended_end,
        'intended_break_minutes',v_intended_break
      )
    );
    update public.weekly_manager_review_items
    set response_state='ANSWERED',response_kind=v_kind,
        intended_start_at_local=v_intended_start,
        intended_end_at_local=v_intended_end,
        intended_break_minutes=v_intended_break,
        response_fingerprint=v_response_hash,answered_at_utc=pg_catalog.transaction_timestamp()
    where id=v_item.id;
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
      v_expected,'MANAGER_RESPONDED','MANAGER',pg_catalog.jsonb_build_object(
        'response_kind',v_kind,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses),
        'review_item_id',v_item.id
      ),'MANAGER_RESPONSE:'||v_request_key::text||':'||v_item.id::text
    ) returning id into v_event_id;
    -- A manager saying the source is right does not settle a candidate's
    -- explicit assertion that their own hours are right. Keep the ordinary
    -- open-incident authorisation hold until Office accepts current source
    -- hours or protects pay for this existing work identity.
    if v_kind in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK')
       and not (v_incident.candidate_action_state='RESPONDED'
         and exists (
           select 1 from public.weekly_discrepancy_events candidate_answer
           where candidate_answer.incident_id=v_incident.id
             and candidate_answer.issue_episode=v_incident.episode_number
             and candidate_answer.event_kind='CANDIDATE_RESPONDED'
             and candidate_answer.bounded_payload_json->>'choice'='CANDIDATE_CORRECT'
         )) then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state=case when candidate_action_state='NOT_ASKED' then 'NOT_REQUIRED' else candidate_action_state end,
          manager_potential_state='NOT_REQUIRED',manager_action_state='RESPONDED',
          waiting_source_state='NOT_WAITING',resolved_at_utc=pg_catalog.transaction_timestamp(),
          resolution_kind='MANAGER_CONFIRMED_SYSTEM_HOURS'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where incident_id=v_incident.id and state in ('ACTIONABLE','ANSWERED');
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_comparison.projection_publication_id,
        v_expected,'RESOLVED','MANAGER',
        pg_catalog.jsonb_build_object('resolution','MANAGER_CONFIRMED_SYSTEM_HOURS'),
        'MANAGER_CONFIRMED:'||v_event_id::text
      );
    elsif v_kind='MANAGER_CORRECTED_SOURCE' then
      update public.weekly_discrepancy_incidents
      set reconciliation_state='WAITING_FOR_SOURCE',manager_action_state='RESPONDED',
          waiting_source_state='WAITING_REIMPORT'
      where id=v_incident.id;
    else
      update public.weekly_discrepancy_incidents
      set reconciliation_state='UNRESOLVED',manager_action_state='RESPONDED',
          waiting_source_state='NOT_WAITING'
      where id=v_incident.id;
    end if;
    select * into strict v_client from public.clients where id=v_incident.client_id;
    select * into strict v_candidate from public.candidates where id=v_incident.candidate_id;
    select * into strict v_group from public.weekly_source_groups where id=v_incident.source_group_id;
    v_summary:=case
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
        then 'Shift missing or not yet authorised'
      when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
        then 'Shift not shown in system'
      else 'System hours reviewed'
    end;
    -- 04A section 4: "A corrected/added/finalised response records the manager's
    -- intended start, end and break and waits for a later accepted source
    -- upload; it never creates source hours."  The record lives on
    -- weekly_manager_review_items and is deliberately left out of every
    -- calculation.  It is repeated here, as plain non-financial text, so the
    -- Office notice this response raises actually carries the figures the
    -- manager gave; without them Office cannot compare the manager's stated
    -- correction with the later source, which is what section 6 expects it to
    -- do.  These keys are hours only: the fan-out owner rejects any pay,
    -- charge, rate, margin, VAT, invoice or banking key.
    v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
      v_incident.id,v_event_id,
      case when v_kind='MANAGER_CORRECTED_SOURCE'
        then 'WEEKLY_MANAGER_SOURCE_CORRECTED' else 'WEEKLY_MANAGER_SYSTEM_CONFIRMED' end,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'candidate_name',coalesce(nullif(v_candidate.display_name,''),
          pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
        'client_name',v_client.name,'work_date',v_work.work_date,
        'issue_summary',v_summary,'manager_response',v_kind,
        'manager_intended_start',to_char(v_intended_start,'YYYY-MM-DD HH24:MI'),
        'manager_intended_end',to_char(v_intended_end,'YYYY-MM-DD HH24:MI'),
        'manager_intended_break_minutes',v_intended_break
      ))
    );
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_MANAGER_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.weekly_manager_review_items
                where review_batch_id=v_batch.id and response_state='UNANSWERED') then
    update public.weekly_manager_review_batches set state='COMPLETE',
      completed_at_utc=pg_catalog.transaction_timestamp() where id=v_batch.id;
    update public.weekly_manager_route_receipts set state='COMPLETE' where id=v_receipt.id;
  end if;
  v_query_result:=pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'office_notification_count',v_notices,
    'remaining_count',(select pg_catalog.count(*) from public.weekly_manager_review_items
                       where review_batch_id=v_batch.id and response_state='UNANSWERED')
  );
  if v_query_outer then
    -- Genuine immutable request response set, after all original notice/cohort/
    -- intent or batch/route completion; no business work remains after this.
    perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'MANAGER_RESPONSE',v_request_key,
      private.bpay_next_query_response_completion_digest_v1(v_query_producer));
  else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'MANAGER_RESPONSE');end if;
  return v_query_result;
exception when no_data_found then
  raise exception 'WEEKLY_SOURCE_MANAGER_REVIEW_UNAVAILABLE' using errcode='42501';
end;
$function$;

create or replace function public.weekly_source_candidate_response_submit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_query_producer uuid;v_query_outer boolean;v_query_result jsonb;
  v_candidate_id uuid;
  v_generation_id uuid;
  v_publication_id uuid;
  v_request_key uuid;
  v_responses jsonb;
  v_semantic_hash bytea;
  v_semantic_hex text;
  v_existing_count integer;
  v_existing_expected integer;
  v_existing_hash text;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cohort public.weekly_candidate_cohorts%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_item jsonb;
  v_incident public.weekly_discrepancy_incidents%rowtype;
  v_comparison public.weekly_issue_comparison_revisions%rowtype;
  v_membership public.weekly_candidate_outreach_memberships%rowtype;
  v_choice text;
  v_expected bytea;
  v_current_timesheet_hash bytea;
  v_response_hash bytea;
  v_draft public.weekly_candidate_response_drafts%rowtype;
  v_draft_version integer;
  v_event_id uuid;
  v_answered integer:=0;
  v_resolved integer:=0;
  v_notices integer:=0;
  v_work public.weekly_work_events%rowtype;
  v_client public.clients%rowtype;
  v_candidate public.candidates%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_summary text;
  v_recipient_generation_id uuid;
  v_recipient_route_id uuid;
  v_due_ids uuid[];
  v_intent_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in (
                 'candidate_id','candidate_generation_id','projection_publication_id',
                 'request_idempotency_key','responses'
               ))
     or pg_catalog.jsonb_typeof(p_request->'responses')<>'array'
     or pg_catalog.jsonb_array_length(p_request->'responses')=0 then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end if;
  begin
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_generation_id:=(p_request->>'candidate_generation_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_request_key:=(p_request->>'request_idempotency_key')::uuid;
    v_responses:=(
      select pg_catalog.jsonb_agg(value order by (value->>'incident_id')::uuid)
      from pg_catalog.jsonb_array_elements(p_request->'responses')
    );
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_INVALID' using errcode='22023';
  end;
  v_semantic_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_CANDIDATE_RESPONSE_REQUEST_V1',
    pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'candidate_generation_id',v_generation_id,
      'projection_publication_id',v_publication_id,'responses',v_responses
    )
  );
  v_semantic_hex:=pg_catalog.encode(v_semantic_hash,'hex');

  -- Exact durable replay is deliberately checked before any mutable generation,
  -- publication, Timesheet or incident test.
  select pg_catalog.count(*),
         min(event.bounded_payload_json->>'request_item_count'),
         min(event.bounded_payload_json->>'request_semantic_hash')
  into v_existing_count,v_existing_expected,v_existing_hash
  from public.weekly_discrepancy_events event
  where event.event_kind='CANDIDATE_RESPONDED'
    and event.bounded_payload_json->>'request_idempotency_key'=v_request_key::text;
  if v_existing_count>0 then
    if v_existing_hash<>v_semantic_hex
       or v_existing_expected::integer<>pg_catalog.jsonb_array_length(v_responses)
       or v_existing_count<>v_existing_expected then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_REPLAY_CONFLICT' using errcode='23505';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'replay',true,'request_idempotency_key',v_request_key,
      'answered_count',v_existing_count
    );
  end if;

  -- Pure Candidate/generation scope before stable module and original locks.
  select g.* into strict v_generation from public.weekly_candidate_outreach_generations g where g.id=v_generation_id;
  if v_generation.state<>'ACTIVE' or v_generation.request_kind<>'CHECK_HOURS'
    or v_generation.candidate_id<>v_candidate_id or pg_catalog.transaction_timestamp()>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';end if;
  select c.* into strict v_cohort from public.weekly_candidate_cohorts c where c.id=v_generation.candidate_cohort_id;
  if v_cohort.current_generation_id<>v_generation.id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';end if;
  select entry.producer_id,entry.is_outer into v_query_producer,v_query_outer
    from private.bpay_next_query_producer_enter_v1('CANDIDATE_RESPONSE','CANDIDATE_RESPONSE',v_request_key::text,v_semantic_hash,'STANDALONE_QUERY') entry;
  select * into strict v_generation
  from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;
  if v_generation.state<>'ACTIVE' or v_generation.request_kind<>'CHECK_HOURS'
     or v_generation.candidate_id<>v_candidate_id
     or pg_catalog.transaction_timestamp()>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cohort from public.weekly_candidate_cohorts
  where id=v_generation.candidate_cohort_id for update;
  if v_cohort.current_generation_id<>v_generation.id then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_GENERATION_STALE' using errcode='40001';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_candidate from public.candidates where id=v_candidate_id;
  select coalesce(pg_catalog.max(draft_version),0)+1 into v_draft_version
  from public.weekly_candidate_response_drafts where candidate_generation_id=v_generation.id;
  insert into public.weekly_candidate_response_drafts(
    candidate_generation_id,candidate_id,draft_version,current_projection_publication_id,
    state,draft_hash,submitted_at_utc
  ) values (
    v_generation.id,v_candidate_id,v_draft_version,v_publication_id,
    'SUBMITTED',v_semantic_hash,pg_catalog.transaction_timestamp()
  ) returning * into v_draft;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(v_responses)
    order by (value->>'incident_id')::uuid
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object'
       or exists(select 1 from pg_catalog.jsonb_object_keys(v_item) key
                 where key not in (
                   'incident_id','expected_comparison_fingerprint','expected_timesheet_hash',
                   'choice','corrected_start_at_local','corrected_end_at_local','corrected_break_minutes'
                 )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_INVALID' using errcode='22023';
    end if;
    begin
      select * into strict v_incident from public.weekly_discrepancy_incidents
      where id=(v_item->>'incident_id')::uuid for update;
      select * into strict v_membership from public.weekly_candidate_outreach_memberships
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id
        and state='ACTIONABLE' for update;
      select * into strict v_comparison from public.weekly_issue_comparison_revisions
      where id=v_incident.current_comparison_revision_id;
    exception when no_data_found or invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end;
    if v_incident.state<>'OPEN' or v_incident.candidate_id<>v_candidate_id
       or v_incident.source_cycle_id<>v_cycle.id
       or private.weekly_source_covered_hours_incident_v1(v_incident.id) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_expected:=private.weekly_source_query_hex32_v1(
      v_item->>'expected_comparison_fingerprint',
      'WEEKLY_SOURCE_CANDIDATE_RESPONSE_FINGERPRINT_INVALID'
    );
    if v_expected<>v_comparison.material_comparison_fingerprint then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_ITEM_STALE' using errcode='40001';
    end if;
    v_current_timesheet_hash:=private.weekly_source_query_candidate_timesheet_hash_v1(
      v_comparison.candidate_timesheet_id
    );
    if v_current_timesheet_hash is null
       or v_current_timesheet_hash<>private.weekly_source_query_hex32_v1(
         v_item->>'expected_timesheet_hash','WEEKLY_SOURCE_CANDIDATE_TIMESHEET_HASH_INVALID'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_TIMESHEET_STALE' using errcode='40001';
    end if;
    v_choice:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_item->>'choice','')));
    if v_choice not in ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT') then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_CHOICE_INVALID' using errcode='22023';
    end if;
    if (
      nullif(v_item->>'corrected_start_at_local','') is null
      or nullif(v_item->>'corrected_end_at_local','') is null
      or nullif(v_item->>'corrected_break_minutes','') is null
    ) and not (
      nullif(v_item->>'corrected_start_at_local','') is null
      and nullif(v_item->>'corrected_end_at_local','') is null
      and nullif(v_item->>'corrected_break_minutes','') is null
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is not null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_NOT_ALLOWED' using errcode='22023';
    end if;
    if v_choice='NEITHER_CORRECT' and
       nullif(v_item->>'corrected_start_at_local','') is null then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if v_choice='CANDIDATE_WRONG'
       and nullif(v_item->>'corrected_start_at_local','') is null
       and v_comparison.source_presence<>'ABSENT' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_REQUIRED' using errcode='22023';
    end if;
    if nullif(v_item->>'corrected_start_at_local','') is not null and (
      nullif(v_item->>'corrected_break_minutes','')::integer<0
      or nullif(v_item->>'corrected_end_at_local','')::timestamp
         <=nullif(v_item->>'corrected_start_at_local','')::timestamp
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_CORRECTED_HOURS_INVALID' using errcode='22023';
    end if;
    v_response_hash:=private.weekly_source_query_response_fingerprint_v1(
      v_incident.id,v_comparison.id,v_candidate_id,v_draft_version,v_request_key,
      private.weekly_source_sha256_jsonb_v1('WEEKLY_CANDIDATE_RESPONSE_ITEM_V1',v_item)
    );
    insert into public.weekly_candidate_response_draft_items(
      response_draft_id,incident_id,comparison_revision_id,choice,
      corrected_start_at_local,corrected_end_at_local,corrected_break_minutes,
      response_fingerprint
    ) values (
      v_draft.id,v_incident.id,v_comparison.id,v_choice,
      nullif(v_item->>'corrected_start_at_local','')::timestamp,
      nullif(v_item->>'corrected_end_at_local','')::timestamp,
      nullif(v_item->>'corrected_break_minutes','')::integer,v_response_hash
    );
    insert into public.weekly_discrepancy_events(
      incident_id,issue_episode,projection_publication_id,
      expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,
      idempotency_key
    ) values (
      v_incident.id,v_incident.episode_number,v_publication_id,
      v_comparison.material_comparison_fingerprint,'CANDIDATE_RESPONDED','CANDIDATE',
      pg_catalog.jsonb_build_object(
        'choice',v_choice,'response_fingerprint',pg_catalog.encode(v_response_hash,'hex'),
        'request_idempotency_key',v_request_key,
        'request_semantic_hash',v_semantic_hex,
        'request_item_count',pg_catalog.jsonb_array_length(v_responses)
      ),'CANDIDATE_RESPONSE:'||v_request_key::text||':'||v_incident.id::text
    ) returning id into v_event_id;
    if v_choice='CANDIDATE_WRONG' then
      update public.weekly_discrepancy_incidents
      set state='RESOLVED',reconciliation_state='RECONCILED',
          candidate_action_state='RESPONDED',manager_potential_state='NOT_REQUIRED',
          manager_action_state='NOT_REQUIRED',waiting_source_state='NOT_WAITING',
          resolved_at_utc=pg_catalog.transaction_timestamp(),resolution_kind='CANDIDATE_CORRECTED'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='RESOLVED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      update public.weekly_manager_review_items set response_state='FILTERED_RESOLVED'
      where incident_id=v_incident.id and response_state='UNANSWERED';
      insert into public.weekly_discrepancy_events(
        incident_id,issue_episode,projection_publication_id,
        expected_comparison_fingerprint,event_kind,actor_kind,bounded_payload_json,idempotency_key
      ) values (
        v_incident.id,v_incident.episode_number,v_publication_id,
        v_comparison.material_comparison_fingerprint,'RESOLVED','CANDIDATE',
        pg_catalog.jsonb_build_object('resolution','CANDIDATE_CORRECTED'),
        'CANDIDATE_CORRECTED:'||v_event_id::text
      );
      v_resolved:=v_resolved+1;
    else
      v_policy:=private.weekly_source_query_incident_policy_v1(v_incident.id);
      update public.weekly_discrepancy_incidents
      set candidate_action_state='RESPONDED',
          manager_potential_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'AVAILABLE'
            else 'NOT_AVAILABLE'
          end,
          manager_action_state=case
            when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
                 and nullif(v_policy->>'manager_query_recipient','') is not null
              then 'DUE'
            else 'NOT_REQUIRED'
          end,
          reconciliation_state='WAITING_FOR_SOURCE'
      where id=v_incident.id;
      update public.weekly_candidate_outreach_memberships set state='ANSWERED'
      where candidate_generation_id=v_generation.id and incident_id=v_incident.id;
      select * into strict v_work from public.weekly_work_events where id=v_incident.work_event_id;
      select * into strict v_client from public.clients where id=v_incident.client_id;
      v_summary:=case
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED' and v_group.source_family='NHSP'
          then 'Shift missing or not yet authorised'
        when v_comparison.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'
          then 'Shift missing from system hours'
        else 'System hours need review'
      end;
      v_notices:=v_notices+private.weekly_source_query_notice_fanout_v1(
        v_incident.id,v_event_id,'WEEKLY_CANDIDATE_SOURCE_DISPUTED',
        pg_catalog.jsonb_build_object(
          'candidate_name',coalesce(nullif(v_candidate.display_name,''),
            pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),
          'client_name',v_client.name,'work_date',v_work.work_date,
          'issue_summary',v_summary,'candidate_choice',v_choice
        )
      );
    end if;
    v_answered:=v_answered+1;
  end loop;
  if v_answered<>pg_catalog.jsonb_array_length(v_responses) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SELECTION_INVALID' using errcode='22023';
  end if;
  if not exists(
    select 1 from public.weekly_candidate_outreach_memberships
    where candidate_generation_id=v_generation.id and state='ACTIONABLE'
  ) then
    update public.weekly_candidate_outreach_generations set state='COMPLETE'
    where id=v_generation.id;
    update public.weekly_message_intents set state='RETIRED'
    where candidate_generation_id=v_generation.id and state in ('DUE','RENDERED');

    select generation.id,generation.recipient_route_id
      into v_recipient_generation_id,v_recipient_route_id
    from public.weekly_manager_recipient_memberships membership
    join public.weekly_manager_recipient_generations generation
      on generation.id=membership.recipient_generation_id and generation.state='ACTIVE'
    join public.weekly_manager_recipient_routes route
      on route.id=generation.recipient_route_id
     and route.current_generation_id=generation.id
    where membership.candidate_cohort_id=v_cohort.id
    order by generation.generation_number desc
    limit 1
    for update of generation;
    if v_recipient_generation_id is not null
       and not exists(
         select 1
         from public.weekly_manager_recipient_memberships route_membership
         join public.weekly_route_activations activation
           on activation.id=route_membership.route_activation_id
          and activation.route_mode='CANDIDATE_FIRST'
         join public.weekly_candidate_cohorts candidate_cohort
           on candidate_cohort.id=route_membership.candidate_cohort_id
         join public.weekly_candidate_outreach_memberships candidate_membership
           on candidate_membership.candidate_generation_id=candidate_cohort.current_generation_id
          and candidate_membership.state='ACTIONABLE'
         where route_membership.recipient_generation_id=v_recipient_generation_id
       ) and exists(
      -- 04A section 6.1: only a row that is unanswered AND unsent becomes due.
      -- A row an accepted batch on this route already owns is not work, so it
      -- must not raise an EARLY_ALL event whose render would then be empty.
      select 1 from public.weekly_manager_recipient_memberships membership
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
        and not private.weekly_source_query_manager_row_owned_v1(
          v_recipient_route_id,incident.id,incident.episode_number
        )
    ) then
      insert into public.weekly_manager_cohort_due_events(
        recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
        due_at_utc,state,trigger_hash
      ) select distinct
        v_recipient_generation_id,membership.candidate_cohort_id,'EARLY_ALL',
        candidate_generation.started_at_utc,pg_catalog.transaction_timestamp(),'PENDING',
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_MANAGER_COHORT_DUE_V1',pg_catalog.jsonb_build_object(
            'recipient_generation_id',v_recipient_generation_id,
            'candidate_cohort_id',membership.candidate_cohort_id,
            'event_kind','EARLY_ALL',
            'candidate_generation_id',candidate_generation.id
          )
        )
      from public.weekly_manager_recipient_memberships membership
      join public.weekly_route_activations activation
        on activation.id=membership.route_activation_id
       and activation.route_mode='CANDIDATE_FIRST'
      join public.weekly_candidate_cohorts candidate_cohort
        on candidate_cohort.id=membership.candidate_cohort_id
      join public.weekly_candidate_outreach_generations candidate_generation
        on candidate_generation.id=candidate_cohort.current_generation_id
      join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
      where membership.recipient_generation_id=v_recipient_generation_id
        and incident.state='OPEN' and incident.manager_action_state<>'RESPONDED'
      on conflict (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash) do nothing;
      select pg_catalog.array_agg(id order by id) into v_due_ids
      from public.weekly_manager_cohort_due_events
      where recipient_generation_id=v_recipient_generation_id
        and event_kind='EARLY_ALL' and state='PENDING';
      if coalesce(pg_catalog.array_length(v_due_ids,1),0)>0
         and not exists(
           select 1 from public.weekly_message_intents intent
           where intent.recipient_generation_id=v_recipient_generation_id
             and intent.state in ('DUE','RENDERED')
         ) then
        v_intent_id:=private.weekly_source_query_manager_intent_v1(
          (select recipient_route_id from public.weekly_manager_recipient_generations where id=v_recipient_generation_id),
          v_recipient_generation_id,v_due_ids,'MANAGER_EARLY_ALL',1,
          pg_catalog.transaction_timestamp()
        );
        update public.weekly_manager_cohort_due_events set state='CONSUMED'
        where id=any(v_due_ids);
      end if;
    end if;
  end if;
  v_query_result:=pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'request_idempotency_key',v_request_key,
    'answered_count',v_answered,'resolved_count',v_resolved,
    'office_notification_count',v_notices,'manager_message_intent_id',v_intent_id
  );
  if v_query_outer then
    -- Genuine immutable request response set, after all original notice/cohort/
    -- intent or batch/route completion; no business work remains after this.
    perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'CANDIDATE_RESPONSE',v_request_key,
      private.bpay_next_query_response_completion_digest_v1(v_query_producer));
  else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'CANDIDATE_RESPONSE');end if;
  return v_query_result;
end;
$function$;

create or replace function private.bpay_next_query_incident_row_stage_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare old_tuple jsonb:='{}'::jsonb;new_tuple jsonb:='{}'::jsonb;row_id uuid;candidate uuid;
begin
 if tg_op<>'INSERT' then old_tuple:=jsonb_build_object('candidate_id',old.candidate_id,'client_id',old.client_id,
   'work_event_id',old.work_event_id,'source_cycle_id',old.source_cycle_id,'source_group_id',old.source_group_id,
   'state',old.state,'current_comparison_revision_id',old.current_comparison_revision_id);row_id:=old.id;candidate:=old.candidate_id;end if;
 if tg_op<>'DELETE' then new_tuple:=jsonb_build_object('candidate_id',new.candidate_id,'client_id',new.client_id,
   'work_event_id',new.work_event_id,'source_cycle_id',new.source_cycle_id,'source_group_id',new.source_group_id,
   'state',new.state,'current_comparison_revision_id',new.current_comparison_revision_id);row_id:=new.id;candidate:=new.candidate_id;end if;
 if tg_op='UPDATE' and (new.id,new.candidate_id,new.client_id,new.work_event_id,new.source_cycle_id,new.source_group_id)
   is distinct from (old.id,old.candidate_id,old.client_id,old.work_event_id,old.source_cycle_id,old.source_group_id) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_INCIDENT_IDENTITY_IMMUTABLE';end if;
 if old_tuple=new_tuple then return null;end if;
 if not exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id()) then
   if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
     raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MATERIAL_WRITER_UNOWNED';end if;
   return null;
 end if;
 if not exists(select 1 from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_catalog.pg_current_xact_id()
   and p.owner_stack[pg_catalog.cardinality(p.owner_stack)] in ('SYSTEM_HOURS_ACCEPT','MANAGER_RESPONSE','CANDIDATE_RESPONSE',
     'QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','SOURCE_UPLOAD','SOURCE_ROWS_APPLY')) then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_INCIDENT_WRITER_INVALID';end if;
 -- Incident has known Candidate, but no canonical root column.
 perform private.bpay_next_query_version_stage_v1('public.weekly_discrepancy_incidents',row_id,candidate,null,
   tg_op<>'INSERT',tg_op<>'DELETE',old_tuple,new_tuple);
 return null;
end
$function$;
alter function private.bpay_next_query_incident_row_stage_v1() owner to postgres;
revoke all on function private.bpay_next_query_incident_row_stage_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_query_incident_row_stage_v1 on public.weekly_discrepancy_incidents;
create trigger bpay_next_query_incident_row_stage_v1 after insert or update or delete on public.weekly_discrepancy_incidents
 for each row execute function private.bpay_next_query_incident_row_stage_v1();

alter function private.bpay_next_query_response_completion_digest_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_response_completion_digest_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) owner to postgres;
revoke all on function private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea) from public,anon,authenticated,service_role;
alter function public.weekly_source_manager_review_respond_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manager_review_respond_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_manager_review_respond_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_candidate_response_submit_atomic_v1(jsonb) to service_role;
alter function private.bpay_next_query_incident_row_stage_v1() owner to postgres;
revoke all on function private.bpay_next_query_incident_row_stage_v1() from public,anon,authenticated,service_role;

create or replace function public.weekly_source_candidate_app_submit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_request_id uuid,
  p_body jsonb,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_query_producer uuid;v_query_outer boolean;v_query_receipt uuid;v_query_account uuid;
  v_context jsonb;
  v_candidate_id uuid;
  v_kind text;
  v_key uuid;
  v_hash bytea;
  v_replay jsonb;
  v_projection jsonb;
  v_scope jsonb;
  v_internal jsonb;
  v_generation public.weekly_candidate_outreach_generations%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_workflow_result jsonb;
  v_response_result jsonb;
  v_result jsonb;
  v_next_generation integer;
begin
  perform private.weekly_source_query_require_service_v1();
  v_context:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,false
  );
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then
    raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
  end if;
  if p_body is null or pg_catalog.jsonb_typeof(p_body)<>'object'
     or pg_catalog.jsonb_typeof(p_body->'responses')<>'array'
     or coalesce(p_body->>'request_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(p_body->>'scope_fingerprint','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  v_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_body->>'submission_kind','')));
  if v_kind='RESPONSES_ONLY' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','responses','idempotency_key'
              )) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  elsif v_kind='WHOLE_WEEK_REVISION' then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_body) key
              where key not in (
                'submission_kind','request_version','request_fingerprint','scope_id',
                'scope_version','scope_fingerprint','workflow_id','generation',
                'candidate_signature_component_id','candidate_signed_at_utc',
                'immutable_submission','responses','idempotency_key'
              )) or pg_catalog.jsonb_typeof(p_body->'immutable_submission')<>'object' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end if;
  begin
    v_key:=(p_body->>'idempotency_key')::uuid;
    perform (p_body->>'request_version')::integer;
    perform (p_body->>'scope_id')::uuid;
    perform (p_body->>'scope_version')::integer;
    if v_kind='WHOLE_WEEK_REVISION' then
      perform (p_body->>'workflow_id')::uuid;
      perform (p_body->>'generation')::integer;
      perform (p_body->>'candidate_signature_component_id')::uuid;
      perform (p_body->>'candidate_signed_at_utc')::timestamptz;
    end if;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_INVALID' using errcode='22023';
  end;
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CANDIDATE_APP_SUBMIT_V1',pg_catalog.jsonb_build_object(
      'candidate_id',v_candidate_id,'request_id',p_request_id,
      'body',p_body-'idempotency_key'
    )
  );
  -- Original immutable reply wins before the true session/business locks.
  v_replay:=private.weekly_source_candidate_app_receipt_v1(v_candidate_id,p_request_id,'FINAL_SUBMIT',v_key,v_hash);
  if v_replay is not null then
    return pg_catalog.jsonb_set(v_replay,'{idempotent_replay}','true'::jsonb,false);end if;
  v_query_account:=(v_context->>'account_id')::uuid;
  select entry.producer_id,entry.is_outer into v_query_producer,v_query_outer
    from private.bpay_next_query_producer_enter_v1('CANDIDATE_APP_SUBMIT','CANDIDATE_APP_SUBMIT',v_key::text,v_hash,'STANDALONE_QUERY') entry;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,true);
  if nullif(v_context->>'selected_candidate_id','')::uuid is distinct from v_candidate_id
    or (v_context->>'account_id')::uuid is distinct from v_query_account then
    raise exception using errcode='40001',message='BPAY_NEXT_QUERY_CANDIDATE_SESSION_SCOPE_CHANGED';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_CANDIDATE_APP:'||v_key::text,0
  ));
  v_replay:=private.weekly_source_candidate_app_receipt_v1(
    v_candidate_id,p_request_id,'FINAL_SUBMIT',v_key,v_hash
  );
  if v_replay is not null then
    if v_query_outer then
      select receipt.id into strict v_query_receipt from public.weekly_candidate_app_mutation_receipts receipt
        where receipt.candidate_id=v_candidate_id and receipt.candidate_generation_id=p_request_id
          and receipt.mutation_kind='FINAL_SUBMIT' and receipt.idempotency_key=v_key and receipt.request_hash=v_hash;
      perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'CANDIDATE_APP_SUBMIT',v_query_receipt,
        pg_catalog.sha256(pg_catalog.convert_to(v_replay::text,'UTF8')));
    else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'CANDIDATE_APP_SUBMIT');end if;
    return pg_catalog.jsonb_set(v_replay,'{idempotent_replay}','true'::jsonb,false);
  end if;

  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=p_request_id and candidate_id=v_candidate_id for update;
  if v_generation.state<>'ACTIVE' or p_now_utc>v_generation.deadline_at_utc then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_generation.source_cycle_id;
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
  if (p_body->>'request_version')::integer<>(v_projection->>'request_version')::integer
     or p_body->>'request_fingerprint'<>v_projection->>'request_fingerprint' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_REQUEST_STALE' using errcode='40001';
  end if;
  select value into v_scope from pg_catalog.jsonb_array_elements(v_projection->'scopes')
  where value->>'scope_id'=p_body->>'scope_id';
  if v_scope is null
     or (p_body->>'scope_version')::integer<>(v_scope->>'scope_version')::integer
     or p_body->>'scope_fingerprint'<>v_scope->>'scope_fingerprint'
     or v_scope->>'completion_state'='COMPLETE' then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_SCOPE_STALE' using errcode='40001';
  end if;
  v_internal:=private.weekly_source_candidate_app_responses_v1(
    v_scope,p_body->'responses',true
  );

  if v_kind='RESPONSES_ONLY' then
    if v_scope->>'request_kind'<>'CHECK_HOURS'
       or pg_catalog.jsonb_array_length(v_internal)=0
       or exists(
         select 1 from pg_catalog.jsonb_array_elements(v_internal) item
         where item->>'choice'<>'CANDIDATE_CORRECT'
       ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSES_ONLY_INVALID' using errcode='22023';
    end if;
    v_response_result:=public.weekly_source_candidate_response_submit_atomic_v1(
      pg_catalog.jsonb_build_object(
        'candidate_id',v_candidate_id,'candidate_generation_id',p_request_id,
        'projection_publication_id',v_cycle.current_projection_publication_id,
        'request_idempotency_key',v_key,'responses',v_internal
      )
    );
    v_next_generation:=null;
  else
    if v_scope->>'request_kind'='CHECK_HOURS' and (
      pg_catalog.jsonb_array_length(v_internal)=0
      or not exists(
        select 1 from pg_catalog.jsonb_array_elements(v_internal) item
        where item->>'choice'<>'CANDIDATE_CORRECT'
      )
    ) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WEEK_REVISION_NOT_REQUIRED' using errcode='22023';
    end if;
    if v_scope->>'request_kind'='SUBMIT_TIMESHEET'
       and pg_catalog.jsonb_array_length(p_body->'responses')<>0 then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_SUBMIT_TIMESHEET_RESPONSES_INVALID' using errcode='22023';
    end if;
    if v_scope->>'request_kind'='CHECK_HOURS' then
      perform private.weekly_source_candidate_app_assert_week_revision_v1(
        v_scope,p_body->'responses',v_internal,p_body->'immutable_submission'
      );
    else
      perform private.weekly_source_candidate_app_assert_new_week_submission_v1(
        v_scope,p_body->'immutable_submission'
      );
    end if;
    select * into strict v_workflow from public.candidate_submission_workflows
    where id=(p_body->>'workflow_id')::uuid
      and candidate_id=v_candidate_id
      and environment=pg_catalog.upper(pg_catalog.btrim(p_environment))
      and contract_id=(v_scope->>'contract_id')::uuid
      and week_ending_date=(v_scope->>'week_ending_date')::date;
    if v_workflow.generation<>(p_body->>'generation')::integer
       or v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_workflow_result:=public.weekly_source_candidate_check_materialise_atomic_v1(
      pg_catalog.jsonb_build_object(
        'account_id',(v_context->>'account_id')::uuid,
        'candidate_id',v_candidate_id,
        'candidate_generation_id',p_request_id,
        'projection_publication_id',v_cycle.current_projection_publication_id,
        'request_version',(p_body->>'request_version')::integer,
        'request_fingerprint',p_body->>'request_fingerprint',
        'scope_id',(p_body->>'scope_id')::uuid,
        'scope_version',(p_body->>'scope_version')::integer,
        'scope_fingerprint',p_body->>'scope_fingerprint',
        'request_kind',v_scope->>'request_kind',
        'workflow_id',v_workflow.id,
        'expected_workflow_generation',v_workflow.generation,
        'candidate_signature_component_id',
          (p_body->>'candidate_signature_component_id')::uuid,
        'candidate_signed_at_utc',(p_body->>'candidate_signed_at_utc')::timestamptz,
        'immutable_submission',p_body->'immutable_submission',
        'request_idempotency_key',v_key
      ),p_now_utc
    );
    v_next_generation:=(v_workflow_result->>'generation')::integer;
    if v_scope->>'request_kind'='CHECK_HOURS' then
      select coalesce(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_set(
          value,'{expected_timesheet_hash}',
          pg_catalog.to_jsonb(v_workflow_result->>'timesheet_hash'),false
        ) order by value->>'incident_id'
      ),'[]'::jsonb) into v_internal
      from pg_catalog.jsonb_array_elements(v_internal);
      v_response_result:=public.weekly_source_candidate_response_submit_atomic_v1(
        pg_catalog.jsonb_build_object(
          'candidate_id',v_candidate_id,'candidate_generation_id',p_request_id,
          'projection_publication_id',v_cycle.current_projection_publication_id,
          'request_idempotency_key',v_key,'responses',v_internal
        )
      );
    end if;
  end if;
  update public.weekly_candidate_response_drafts
  set state='SUPERSEDED',updated_at_utc=p_now_utc
  where candidate_generation_id=p_request_id and candidate_id=v_candidate_id
    and state='DRAFT';
  v_projection:=private.weekly_source_candidate_app_projection_v1(
    v_candidate_id,p_request_id,p_now_utc
  );
  perform private.weekly_source_candidate_app_assert_request_shape_v1(v_projection);
  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'idempotent_replay',false,
    'processed_scope_id',(p_body->>'scope_id')::uuid,
    'next_outstanding_scope_id',v_projection->'earliest_outstanding_scope_id',
    'workflow_id',case when v_kind='WHOLE_WEEK_REVISION'
      then (p_body->>'workflow_id')::uuid else null end,
    'generation',v_next_generation,'request',v_projection
  );
  if exists(select 1 from pg_catalog.jsonb_object_keys(v_result) key
            where key not in (
              'ok','idempotent_replay','processed_scope_id','next_outstanding_scope_id',
              'workflow_id','generation','request'
            )) then
    raise exception 'WEEKLY_SOURCE_CANDIDATE_RESPONSE_SHAPE_INVALID' using errcode='55000';
  end if;
  insert into public.weekly_candidate_app_mutation_receipts(
    candidate_generation_id,candidate_id,mutation_kind,idempotency_key,
    request_hash,response_json,created_at_utc
  ) values (
    p_request_id,v_candidate_id,'FINAL_SUBMIT',v_key,v_hash,v_result,p_now_utc
  ) returning id into v_query_receipt;
  if v_query_outer then
    perform private.bpay_next_query_producer_finish_standalone_v1(v_query_producer,'CANDIDATE_APP_SUBMIT',v_query_receipt,
      pg_catalog.sha256(pg_catalog.convert_to(v_result::text,'UTF8')));
  else perform private.bpay_next_query_producer_leave_v1(v_query_producer,'CANDIDATE_APP_SUBMIT');end if;
  return v_result;
end;
$function$;

alter function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone) owner to postgres;
revoke all on function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_candidate_app_submit_atomic_v1(uuid,text,uuid,jsonb,timestamp with time zone) to service_role;

notify pgrst, 'reload schema';
commit;
