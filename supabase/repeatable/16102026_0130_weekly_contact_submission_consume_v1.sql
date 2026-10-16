-- Repeatable CloudTMS function/view authority: weekly_contact_submission_consume_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Consumes an already admitted PUSH operation. It deliberately takes no
-- deployment/policy/scope locks: a later pause or QT edit cannot recall START.
-- It follows the existing result owner's attempt -> target order, followed by
-- command and permit. START never locks an existing attempt after its target.
create or replace function public.weekly_contact_submission_consume_v1(p_request jsonb)
returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
 v_keys text[]:=array['permit_id','provider_attempt_id','dispatch_target_id','dispatch_command_id',
  'target_snapshot_hash','provider','content_hash','agency_id','environment'];
 v_permit_id uuid;v_attempt_id uuid;v_target_id uuid;v_command_id uuid;v_agency uuid;
 v_key text;v_at timestamptz;
 v_permit public.weekly_contact_submission_permits%rowtype;
 v_target public.weekly_message_dispatch_targets%rowtype;
 v_command public.weekly_message_dispatch_commands%rowtype;
 v_attempt public.weekly_message_target_attempts%rowtype;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'PUSH_TIMING_REQUEST_INVALID');
 if p_request->>'provider' is null or p_request->>'provider' not in('APNS','FCM')
  or p_request->>'environment' is null or p_request->>'environment' not in('TEST','LIVE') then
  raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end if;
 foreach v_key in array array['target_snapshot_hash','content_hash'] loop
  if jsonb_typeof(p_request->v_key) is distinct from 'string'
   or p_request->>v_key !~ '^[0-9a-f]{64}$' then
   raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end if;
 end loop;
 begin
  v_permit_id:=private.weekly_contact_uuid_v1(p_request->'permit_id');
  v_attempt_id:=private.weekly_contact_uuid_v1(p_request->'provider_attempt_id');
  v_target_id:=private.weekly_contact_uuid_v1(p_request->'dispatch_target_id');
  v_command_id:=private.weekly_contact_uuid_v1(p_request->'dispatch_command_id');
  v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
 exception when sqlstate '22023' then raise exception 'PUSH_TIMING_REQUEST_INVALID' using errcode='22023';end;
 perform private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
 if not exists(select 1 from private.cloudtms_database_identity
  where singleton and environment=p_request->>'environment') then
  raise exception 'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;

 select * into v_attempt from public.weekly_message_target_attempts where id=v_attempt_id for update;
 if not found or v_attempt.dispatch_target_id is distinct from v_target_id then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_target from public.weekly_message_dispatch_targets where id=v_target_id for update;
 if not found or v_target.dispatch_command_id is distinct from v_command_id
  or v_target.channel<>'PUSH' or v_target.target_kind<>'CANDIDATE_DEVICE'
  or v_target.provider is distinct from p_request->>'provider'
  or v_target.target_snapshot_hash is distinct from decode(p_request->>'target_snapshot_hash','hex')
  or v_target.rendered_content_hash is distinct from decode(p_request->>'content_hash','hex') then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_command from public.weekly_message_dispatch_commands where id=v_command_id for update;
 if not found or v_command.agency_id is distinct from v_agency
  or v_command.environment is distinct from p_request->>'environment'
  or v_command.rendered_content_hash is distinct from v_target.rendered_content_hash
  or v_command.tranche_kind not in('CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER',
   'TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H')
  or not exists(select 1 from public.weekly_message_intents i
   join public.weekly_message_renders r on r.message_intent_id=i.id
   join public.weekly_candidate_outreach_generations g on g.id=i.candidate_generation_id
   where i.id=v_command.message_intent_id and r.id=v_command.message_render_id
    and i.audience_kind='CANDIDATE' and i.agency_id=v_command.agency_id and i.environment=v_command.environment
    and g.id=v_command.candidate_generation_id and g.source_cycle_id=v_command.source_cycle_id
    and r.rendered_content_hash=v_target.rendered_content_hash) then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 select * into v_permit from public.weekly_contact_submission_permits where id=v_permit_id for update;
 if not found then raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 if v_permit.owner_kind<>'WEEKLY_SOURCE_TARGET' or v_permit.channel<>'PUSH'
  or v_permit.provider_attempt_id is distinct from v_attempt_id
  or v_permit.dispatch_target_id is distinct from v_target_id
  or v_permit.dispatch_command_id is distinct from v_command_id
  or v_permit.agency_id is distinct from v_agency or v_permit.environment is distinct from p_request->>'environment'
  or v_permit.provider is distinct from v_target.provider
  or v_permit.content_hash is distinct from v_target.rendered_content_hash
  or v_permit.target_snapshot_hash is distinct from v_target.target_snapshot_hash
  or v_permit.safe_target_snapshot_json is distinct from v_target.safe_target_snapshot_json
  or v_permit.recipient_hash is distinct from v_target.keyed_target_fingerprint
  or v_permit.lease_owner is distinct from v_attempt.lease_owner
  or v_permit.lease_token is distinct from v_attempt.lease_token::text
  or v_permit.provider_idempotency_identity is distinct from v_attempt.provider_idempotency_key then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 -- Validate the captured snapshot against the actual immutable native target
 -- on both initial consumption and replay; a guessed permit is no authority.
 perform private.weekly_contact_assert_closed_v1(v_target.safe_target_snapshot_json,
  array['control_plane_snapshot_id','snapshot_device_id','provider','target_revision_hash'],
  array['control_plane_snapshot_id','snapshot_device_id','provider','target_revision_hash'],'PUSH_TIMING_DISPATCH_SCOPE_INVALID');
 if v_target.safe_target_snapshot_json->>'control_plane_snapshot_id' is distinct from v_command.control_plane_snapshot_id::text
  or v_target.safe_target_snapshot_json->>'snapshot_device_id' is distinct from v_target.external_target_id::text
  or v_target.safe_target_snapshot_json->>'provider' is distinct from v_target.provider
  or v_target.safe_target_snapshot_json->>'target_revision_hash' is distinct from encode(v_target.target_snapshot_hash,'hex') then
  raise exception 'PUSH_TIMING_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
 if v_permit.consumed_at_utc is not null then
  return jsonb_build_object('ok',false,'reason','ALREADY_CONSUMED',
   'provider_attempt_id',v_attempt.id,'call_once',false);
 end if;
 if v_permit.status<>'STARTED' or v_target.state<>'SUBMISSION_STARTED'
  or v_attempt.completed_at_utc is not null or v_permit.admitted_at_utc is null
  or v_permit.consume_before_utc is distinct from v_permit.admitted_at_utc+interval '30 seconds' then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 -- Time is sampled after every lock wait. Transaction start time would allow
 -- a stale capability after the permitted provider-call window had elapsed.
 v_at:=clock_timestamp();
 if v_at>=v_permit.consume_before_utc then
  raise exception 'CONTACT_PERMIT_EXPIRED' using errcode='P0001';end if;
 update public.weekly_contact_submission_permits set consumed_at_utc=v_at where id=v_permit.id;
 return jsonb_build_object('ok',true,'version','CONTACT_CONSUMED_V1','permit_id',v_permit.id,
  'provider_attempt_id',v_attempt.id,'call_once',true,
  'consumed_at_utc',to_char(v_at at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
end;
$function$;

-- Called only inside the original native result owner, after its current
-- attempt/target fence and before its once-only result write. This bridge
-- changes permit metadata; all native outcomes/backoff/budgets stay there.
create or replace function private.weekly_contact_source_result_v1(
 p_attempt_id uuid,p_outcome text,p_message_id text,p_receipt jsonb,p_error jsonb
) returns void
language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_permit public.weekly_contact_submission_permits%rowtype;
 v_attempt public.weekly_message_target_attempts%rowtype;
 v_target public.weekly_message_dispatch_targets%rowtype;
 v_no_network boolean;v_result jsonb;v_at timestamptz:=clock_timestamp();
begin
 perform private.weekly_source_query_require_service_v1();
 select * into strict v_attempt from public.weekly_message_target_attempts where id=p_attempt_id;
 select * into strict v_target from public.weekly_message_dispatch_targets where id=v_attempt.dispatch_target_id;
 select * into v_permit from public.weekly_contact_submission_permits where provider_attempt_id=p_attempt_id for update;
 v_no_network:=p_receipt->'proved_no_network'='true'::jsonb
  and p_error->>'error_code'='CONTACT_PERMIT_EXPIRED_NO_SUBMISSION';
 if not found then
  -- Pre-install attempts retain their original result owner, but cannot claim
  -- the new permit-only no-network proof or receive a replacement allowance.
  if p_receipt?'proved_no_network' or p_error->>'error_code'='CONTACT_PERMIT_EXPIRED_NO_SUBMISSION' then
   raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
  return;
 end if;
 if v_permit.owner_kind<>'WEEKLY_SOURCE_TARGET'
  or v_permit.dispatch_target_id is distinct from v_target.id
  or v_permit.dispatch_command_id is distinct from v_target.dispatch_command_id
  or v_permit.channel is distinct from v_target.channel or v_permit.provider is distinct from v_target.provider
  or v_permit.content_hash is distinct from v_target.rendered_content_hash
  or v_permit.recipient_hash is distinct from v_target.keyed_target_fingerprint
  or v_permit.lease_owner is distinct from v_attempt.lease_owner
  or v_permit.lease_token is distinct from v_attempt.lease_token::text
  or v_permit.provider_idempotency_identity is distinct from v_attempt.provider_idempotency_key
  or v_permit.status<>'STARTED' or v_attempt.completed_at_utc is not null
  or v_target.state<>'SUBMISSION_STARTED' or v_target.attempt_count<>v_attempt.attempt_number then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 if p_receipt?'proved_no_network' or p_error->>'error_code'='CONTACT_PERMIT_EXPIRED_NO_SUBMISSION' then
  if coalesce(v_no_network,false) is not true or p_outcome is distinct from 'TRANSIENT_FAILURE'
   or p_message_id is not null or v_permit.channel<>'PUSH'
   or v_permit.consume_before_utc is null or v_at<v_permit.consume_before_utc
   or p_receipt is distinct from '{"proved_no_network":true}'::jsonb
   or p_error is distinct from '{"error_code":"CONTACT_PERMIT_EXPIRED_NO_SUBMISSION"}'::jsonb then
   raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
  -- A never-consumed permit cannot have reached the sole guarded provider.
  -- A consumed one needs this exact late-response proof from that same sender.
  update public.weekly_contact_submission_permits set status='RETIRED',
   proof_classification='NOT_SUBMITTED',proved_no_network=true where id=v_permit.id;
  return;
 end if;
 if p_outcome='ACCEPTED' and v_permit.consumed_at_utc is null then
  raise exception 'CONTACT_PERMIT_INVALID' using errcode='P0001';end if;
 v_result:=jsonb_build_object('provider_attempt_id',p_attempt_id,'outcome',p_outcome,
  'provider_message_id',p_message_id,'bounded_provider_receipt',p_receipt,'bounded_error',p_error);
 update public.weekly_contact_submission_permits set
  status=case p_outcome when 'ACCEPTED' then 'ACCEPTED' when 'AMBIGUOUS' then 'AMBIGUOUS' else 'FAILED' end,
  accepted_at_utc=case when p_outcome='ACCEPTED' then v_at end,
  accepted_result_json=case when p_outcome='ACCEPTED' then v_result end where id=v_permit.id;
end;
$function$;

alter function public.weekly_contact_submission_consume_v1(jsonb) owner to postgres;
alter function private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb) owner to postgres;
revoke all on function public.weekly_contact_submission_consume_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_contact_submission_consume_v1(jsonb) to service_role;
notify pgrst,'reload schema';

commit;
