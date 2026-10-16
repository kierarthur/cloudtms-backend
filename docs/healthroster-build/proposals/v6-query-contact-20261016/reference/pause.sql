\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_contact_uuid_v1(p_value jsonb) returns uuid
language plpgsql immutable security invoker set search_path to 'pg_catalog','pg_temp'
as $function$
begin
 if p_value is null or jsonb_typeof(p_value)<>'string'
  or p_value#>>'{}' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 return(p_value#>>'{}')::uuid;
end;
$function$;

-- Private transaction-local argument transport for the exact four-argument
-- scope guard. Only trusted service entry owners set it, after deployment
-- validation; it is neither a browser credential nor durable configuration.
create or replace function private.weekly_contact_context_v1(p_agency uuid,p_environment text) returns void
language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
begin
 perform private.weekly_source_query_require_service_v1();
 perform private._weekly_source_settings_deployment_v1(p_agency,p_environment);
 if not exists(select 1 from private.cloudtms_database_identity where singleton and environment=p_environment) then
  raise exception 'CONTACT_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;
 perform set_config('cloudtms.weekly_contact_deployment',jsonb_build_object('agency_id',p_agency,'environment',p_environment)::text,true);
end;
$function$;

create or replace function private.weekly_contact_next_monday_v1(p_at_utc timestamptz) returns timestamptz
language plpgsql stable security invoker set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_date date;
begin
 if p_at_utc is null or not isfinite(p_at_utc) then raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 v_date:=(p_at_utc at time zone 'Europe/London')::date;
 return private.weekly_contact_london_boundary_v1(v_date+(8-extract(isodow from v_date)::integer),'00:00'::time,false);
end;
$function$;

create or replace function private.weekly_contact_scope_lock_v1(
 p_agency uuid,p_environment text,p_candidate uuid,p_client uuid
) returns uuid
language plpgsql volatile security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_id uuid;
begin
 -- Every caller takes deployment FIRST; repeating this transaction lock is
 -- harmless. Multi-scope callers iterate canonical candidate/client order.
 perform private.weekly_contact_deployment_lock_v1(p_agency,p_environment);
 perform private.weekly_contact_context_v1(p_agency,p_environment);
 if p_candidate is null or p_client is null then raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 perform pg_advisory_xact_lock(hashtextextended('WEEKLY_CONTACT_SCOPE_V1|'||p_environment||'|'||p_agency::text||'|'||p_candidate::text||'|'||p_client::text,0));
 insert into public.weekly_contact_pause_scopes(environment,agency_id,candidate_id,client_id)
  values(p_environment,p_agency,p_candidate,p_client) on conflict(environment,agency_id,candidate_id,client_id) do nothing;
 select id into strict v_id from public.weekly_contact_pause_scopes where environment=p_environment
  and agency_id=p_agency and candidate_id=p_candidate and client_id=p_client for update;
 return v_id;
end;
$function$;

create or replace function private.weekly_contact_scope_guard_v1(
 p_candidate_id uuid,p_client_id uuid,p_audience text,p_at_utc timestamptz
) returns jsonb
language plpgsql stable security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_context jsonb;v_scope public.weekly_contact_pause_scopes%rowtype;v_until timestamptz;
begin
 perform private.weekly_source_query_require_service_v1();
 if p_candidate_id is null or p_client_id is null or p_audience is null
  or p_audience not in ('WORKER','MANAGER','BOTH') or p_at_utc is null or not isfinite(p_at_utc) then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 v_context:=nullif(current_setting('cloudtms.weekly_contact_deployment',true),'')::jsonb;
 if v_context is null or v_context->>'agency_id' is null or v_context->>'environment' not in ('TEST','LIVE') then
  raise exception 'CONTACT_DEPLOYMENT_UNAVAILABLE' using errcode='P0001';end if;
 select * into v_scope from public.weekly_contact_pause_scopes where environment=v_context->>'environment'
  and agency_id=(v_context->>'agency_id')::uuid and candidate_id=p_candidate_id and client_id=p_client_id;
 if p_audience in ('WORKER','BOTH') and v_scope.worker_paused_until_utc>p_at_utc then v_until:=v_scope.worker_paused_until_utc;end if;
 if p_audience in ('MANAGER','BOTH') and v_scope.manager_paused_until_utc>p_at_utc then
  v_until:=greatest(v_until,v_scope.manager_paused_until_utc);end if;
 return jsonb_build_object('scope_id',v_scope.id,'revision',coalesce(v_scope.revision,0)::text,
  'allow',v_until is null,'next_allowed_utc',case when v_until is null then null else to_char(v_until at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end);
end;
$function$;

create or replace function private.weekly_contact_pause_immutable_guard_v1() returns trigger
language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
begin
 if TG_TABLE_NAME<>'weekly_contact_pause_scopes' or TG_OP='DELETE' then
  raise exception 'CONTACT_IMMUTABLE_HISTORY' using errcode='23514';end if;
 if(new.id,new.environment,new.agency_id,new.candidate_id,new.client_id) is distinct from
  (old.id,old.environment,old.agency_id,old.candidate_id,old.client_id)
  or new.revision<>old.revision+1 then raise exception 'CONTACT_SCOPE_REVISION_INVALID' using errcode='23514';end if;
 return new;
end;
$function$;
create or replace trigger weekly_contact_pause_scopes_immutable before update or delete on public.weekly_contact_pause_scopes
 for each row execute function private.weekly_contact_pause_immutable_guard_v1();
create or replace trigger weekly_contact_pause_commands_immutable before update or delete on public.weekly_contact_pause_commands
 for each row execute function private.weekly_contact_pause_immutable_guard_v1();
create or replace trigger weekly_contact_pause_events_immutable before update or delete on public.weekly_contact_pause_events
 for each row execute function private.weekly_contact_pause_immutable_guard_v1();

create or replace function private.weekly_contact_pause_inflight_v1(
 p_agency uuid,p_environment text,p_candidate uuid,p_client uuid
) returns jsonb
language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
 select coalesce(jsonb_agg(x.summary order by x.started_at,x.permit_id),'[]'::jsonb) from(
  select p.id permit_id,p.admitted_at_utc started_at,jsonb_build_object(
   'permit_id',p.id,'owner_kind',p.owner_kind,'delivery_id',coalesce(p.dispatch_target_id,p.query_delivery_id),
   'provider_attempt_id',p.provider_attempt_id,'channel',p.channel,'status',p.status,
   'started_at_utc',to_char(p.admitted_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'memberships',(select jsonb_agg(m order by m->>'scope_id',m->>'audience') from jsonb_array_elements(p.scope_revisions_json)m
    where m->>'candidate_id'=p_candidate::text and m->>'client_id'=p_client::text)
  )summary
  from public.weekly_contact_submission_permits p where p.environment=p_environment and p.agency_id=p_agency
   and p.status in ('STARTED','AMBIGUOUS') and exists(select 1 from jsonb_array_elements(p.scope_revisions_json)m
    where m->>'candidate_id'=p_candidate::text and m->>'client_id'=p_client::text)
 )x;
$function$;

create or replace function private.weekly_contact_pause_shape_v1(
 p_agency uuid,p_environment text,p_candidate uuid,p_client uuid,p_at_utc timestamptz
) returns jsonb
language plpgsql stable security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_scope public.weekly_contact_pause_scopes%rowtype;
 v_next_pause_until timestamptz:=private.weekly_contact_next_monday_v1(p_at_utc);
begin
 select * into v_scope from public.weekly_contact_pause_scopes where agency_id=p_agency and environment=p_environment
  and candidate_id=p_candidate and client_id=p_client;
 return jsonb_build_object('scope_id',v_scope.id,'candidate_id',p_candidate,'client_id',p_client,
  'revision',coalesce(v_scope.revision,0)::text,
  'next_pause_until_utc',to_char(v_next_pause_until at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'next_pause_until_local',to_char(v_next_pause_until at time zone 'Europe/London','YYYY-MM-DD HH24:MI')||' Europe/London',
  'worker',jsonb_build_object('paused',coalesce(v_scope.worker_paused_until_utc>p_at_utc,false),
   'paused_until_utc',case when v_scope.worker_paused_until_utc>p_at_utc then to_char(v_scope.worker_paused_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
   'paused_until_local',case when v_scope.worker_paused_until_utc>p_at_utc then to_char(v_scope.worker_paused_until_utc at time zone 'Europe/London','YYYY-MM-DD HH24:MI')||' Europe/London' end,
   'reason',v_scope.worker_reason),
  'manager',jsonb_build_object('paused',coalesce(v_scope.manager_paused_until_utc>p_at_utc,false),
   'paused_until_utc',case when v_scope.manager_paused_until_utc>p_at_utc then to_char(v_scope.manager_paused_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
   'paused_until_local',case when v_scope.manager_paused_until_utc>p_at_utc then to_char(v_scope.manager_paused_until_utc at time zone 'Europe/London','YYYY-MM-DD HH24:MI')||' Europe/London' end,
   'reason',v_scope.manager_reason),
  'can_pause',true,'can_resume',true,
  'in_flight',private.weekly_contact_pause_inflight_v1(p_agency,p_environment,p_candidate,p_client));
end;
$function$;

create or replace function public.weekly_source_contact_pause_get_v1(p_request jsonb) returns jsonb
language plpgsql volatile security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_keys text[]:=array['candidate_id','client_id','actor_user_id','agency_id','environment'];
 v_actor uuid;v_candidate uuid;v_client uuid;v_agency uuid;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'CONTACT_REQUEST_INVALID');
 v_actor:=private.weekly_contact_uuid_v1(p_request->'actor_user_id');v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
 v_candidate:=private.weekly_contact_uuid_v1(p_request->'candidate_id');v_client:=private.weekly_contact_uuid_v1(p_request->'client_id');
 perform private.weekly_source_office_authority_v1(v_actor,'ASK_CANDIDATES',null,null,null);
 perform private.weekly_contact_context_v1(v_agency,p_request->>'environment');
 if not exists(select 1 from public.candidates where id=v_candidate) or not exists(select 1 from public.clients where id=v_client) then
  raise exception 'CONTACT_SCOPE_NOT_FOUND' using errcode='42501';end if;
 return jsonb_build_object('ok',true,'version','CONTACT_PAUSE_STATE_V1','scope',
  private.weekly_contact_pause_shape_v1(v_agency,p_request->>'environment',v_candidate,v_client,clock_timestamp()));
end;
$function$;

create or replace function private.weekly_contact_pause_command_v1(p_request jsonb,p_operation text) returns jsonb
language plpgsql volatile security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare v_keys text[]:=array['candidate_id','client_id','actor_user_id','agency_id','environment','request_id','expected_revision','audience','note'];
 v_actor uuid;v_candidate uuid;v_client uuid;v_agency uuid;v_id uuid;v_request uuid;v_expected bigint;v_audience text;v_note text;
 v_scope public.weekly_contact_pause_scopes%rowtype;v_existing public.weekly_contact_pause_commands%rowtype;
 v_now timestamptz;v_expiry timestamptz;v_worker timestamptz;v_manager timestamptz;v_changed boolean;v_worker_changed boolean;v_manager_changed boolean;
 v_worker_reason text;v_manager_reason text;v_digest bytea;v_reply jsonb;v_task uuid;v_revision bigint;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,array_remove(v_keys,'note'),'CONTACT_REQUEST_INVALID');
 if p_operation is null or p_operation not in ('PAUSE','RESUME') or p_request->>'audience' is null
  or p_request->>'audience' not in ('WORKER','MANAGER','BOTH')
  or(p_request?'note' and(jsonb_typeof(p_request->'note') not in ('string','null') or char_length(p_request->>'note')>500)) then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 v_actor:=private.weekly_contact_uuid_v1(p_request->'actor_user_id');v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
 v_candidate:=private.weekly_contact_uuid_v1(p_request->'candidate_id');v_client:=private.weekly_contact_uuid_v1(p_request->'client_id');
 v_request:=private.weekly_contact_uuid_v1(p_request->'request_id');v_audience:=p_request->>'audience';v_note:=p_request->>'note';
 begin v_expected:=private.weekly_contact_revision_v1(p_request->'expected_revision',false);
 exception when sqlstate '22023' then raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end;
 perform private.weekly_source_office_authority_v1(v_actor,'ASK_CANDIDATES',null,null,null);
 perform private.weekly_contact_context_v1(v_agency,p_request->>'environment');
 if not exists(select 1 from public.candidates where id=v_candidate) or not exists(select 1 from public.clients where id=v_client) then
  raise exception 'CONTACT_SCOPE_NOT_FOUND' using errcode='42501';end if;
 v_id:=private.weekly_contact_scope_lock_v1(v_agency,p_request->>'environment',v_candidate,v_client);
 select * into strict v_scope from public.weekly_contact_pause_scopes where id=v_id for update;
 perform pg_advisory_xact_lock(hashtextextended('WEEKLY_CONTACT_COMMAND_V1|'||v_request::text,0));
 v_digest:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_PAUSE_COMMAND_V1',
  jsonb_build_object('operation',p_operation,'request',p_request));
 select * into v_existing from public.weekly_contact_pause_commands where request_id=v_request;
 if found then
  if v_existing.request_digest is distinct from v_digest or v_existing.actor_user_id<>v_actor or v_existing.scope_id<>v_id then
   raise exception 'CONTACT_IDEMPOTENCY_CONFLICT' using errcode='23505';end if;
  return v_existing.response_json;
 end if;
 if v_scope.revision<>v_expected then raise exception 'CONTACT_REVISION_CONFLICT' using errcode='40001';end if;
 v_now:=clock_timestamp();if p_operation='PAUSE' then v_expiry:=private.weekly_contact_next_monday_v1(v_now);end if;
 v_worker:=v_scope.worker_paused_until_utc;v_manager:=v_scope.manager_paused_until_utc;
 v_worker_reason:=v_scope.worker_reason;v_manager_reason:=v_scope.manager_reason;
 if v_audience in ('WORKER','BOTH') then
  if p_operation='PAUSE' then v_worker:=v_expiry;v_worker_reason:=v_note;
  elsif v_worker>v_now then v_worker:=null;v_worker_reason:=null;end if;
 end if;
 if v_audience in ('MANAGER','BOTH') then
  if p_operation='PAUSE' then v_manager:=v_expiry;v_manager_reason:=v_note;
  elsif v_manager>v_now then v_manager:=null;v_manager_reason:=null;end if;
 end if;
 v_worker_changed:=(v_worker,v_worker_reason) is distinct from(v_scope.worker_paused_until_utc,v_scope.worker_reason);
 v_manager_changed:=(v_manager,v_manager_reason) is distinct from(v_scope.manager_paused_until_utc,v_scope.manager_reason);
 v_changed:=v_worker_changed or v_manager_changed;v_revision:=v_scope.revision;
 if v_changed then
  if v_revision=9223372036854775807 then raise exception 'CONTACT_REVISION_EXHAUSTED' using errcode='54000';end if;
  v_revision:=v_revision+1;
  update public.weekly_contact_pause_scopes set revision=v_revision,worker_paused_until_utc=v_worker,
   manager_paused_until_utc=v_manager,worker_reason=v_worker_reason,manager_reason=v_manager_reason,
   updated_by_user_id=v_actor,updated_at_utc=v_now where id=v_id;
  if v_worker_changed then insert into public.weekly_contact_pause_events(scope_id,request_id,revision,audience,actor_user_id,action,previous_expiry_utc,new_expiry_utc,reason,recorded_at_utc)
   values(v_id,v_request,v_revision,'WORKER',v_actor,p_operation,v_scope.worker_paused_until_utc,v_worker,v_note,v_now);end if;
  if v_manager_changed then insert into public.weekly_contact_pause_events(scope_id,request_id,revision,audience,actor_user_id,action,previous_expiry_utc,new_expiry_utc,reason,recorded_at_utc)
   values(v_id,v_request,v_revision,'MANAGER',v_actor,p_operation,v_scope.manager_paused_until_utc,v_manager,v_note,v_now);end if;
 end if;
 if v_changed or p_operation='RESUME' then
  insert into public.weekly_contact_recheck_tasks(scope_id,request_id,scope_revision,due_at_utc,state)
   values(v_id,v_request,v_revision,coalesce(v_expiry,v_now),'READY') returning id into v_task;
 end if;
 v_reply:=jsonb_build_object('ok',true,'version','CONTACT_PAUSE_RECEIPT_V1','request_id',v_request,
  'operation',p_operation,'audience',v_audience,'changed',v_changed,'recheck_task_id',v_task,
  'automatic_send',false,'scope',private.weekly_contact_pause_shape_v1(v_agency,p_request->>'environment',v_candidate,v_client,v_now));
 insert into public.weekly_contact_pause_commands(request_id,actor_user_id,scope_id,request_digest,expected_revision,accepted_revision,
  operation,audience_mask,computed_expiry_utc,response_json,created_at_utc,completed_at_utc)
  values(v_request,v_actor,v_id,v_digest,v_expected,v_revision,p_operation,
   case v_audience when 'WORKER' then 1 when 'MANAGER' then 2 else 3 end,v_expiry,v_reply,v_now,clock_timestamp());
 return v_reply;
end;
$function$;

create or replace function public.weekly_source_contact_pause_atomic_v1(p_request jsonb) returns jsonb
language sql volatile security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$ select private.weekly_contact_pause_command_v1(p_request,'PAUSE'); $function$;
create or replace function public.weekly_source_contact_resume_atomic_v1(p_request jsonb) returns jsonb
language sql volatile security definer set search_path to 'pg_catalog','private','pg_temp'
as $function$ select private.weekly_contact_pause_command_v1(p_request,'RESUME'); $function$;

alter function private.weekly_contact_uuid_v1(jsonb) owner to postgres;
alter function private.weekly_contact_context_v1(uuid,text) owner to postgres;
alter function private.weekly_contact_next_monday_v1(timestamptz) owner to postgres;
alter function private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid) owner to postgres;
alter function private.weekly_contact_scope_guard_v1(uuid,uuid,text,timestamptz) owner to postgres;
alter function private.weekly_contact_pause_immutable_guard_v1() owner to postgres;
alter function private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid) owner to postgres;
alter function private.weekly_contact_pause_shape_v1(uuid,text,uuid,uuid,timestamptz) owner to postgres;
alter function private.weekly_contact_pause_command_v1(jsonb,text) owner to postgres;
alter function public.weekly_source_contact_pause_get_v1(jsonb) owner to postgres;
alter function public.weekly_source_contact_pause_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_contact_resume_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_uuid_v1(jsonb),private.weekly_contact_context_v1(uuid,text),
 private.weekly_contact_next_monday_v1(timestamptz),private.weekly_contact_scope_lock_v1(uuid,text,uuid,uuid),
 private.weekly_contact_scope_guard_v1(uuid,uuid,text,timestamptz),private.weekly_contact_pause_immutable_guard_v1(),
 private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid),private.weekly_contact_pause_shape_v1(uuid,text,uuid,uuid,timestamptz),
 private.weekly_contact_pause_command_v1(jsonb,text),public.weekly_source_contact_pause_get_v1(jsonb),
 public.weekly_source_contact_pause_atomic_v1(jsonb),public.weekly_source_contact_resume_atomic_v1(jsonb)
 from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_contact_pause_get_v1(jsonb),public.weekly_source_contact_pause_atomic_v1(jsonb),
 public.weekly_source_contact_resume_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
