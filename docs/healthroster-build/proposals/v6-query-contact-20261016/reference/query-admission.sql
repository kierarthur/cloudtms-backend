-- Repeatable CloudTMS function/view authority: weekly_contact_query_outbox_admission_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Real legacy query delivery facts. This does not create a weekly dispatch
-- target, command, provider attempt, contact allowance or financial authority.
create or replace function private.weekly_contact_query_content_v1(
 p_outbox_id uuid,p_delivery_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_mail public.mail_outbox%rowtype;v_delivery public.hr_issue_email_deliveries%rowtype;
 v_item public.hr_issue_email_delivery_items%rowtype;v_action public.import_review_decisions%rowtype;
 v_issue public.hr_issue_emails%rowtype;v_operation public.import_apply_operations%rowtype;
 v_import public.hr_imports%rowtype;v_ts public.timesheets%rowtype;
 v_current boolean;v_count integer:=0;v_route jsonb;v_evidence jsonb;
 v_route_hashes text[]:='{}';v_issue_witnesses jsonb:='[]';v_pairs jsonb:='[]';v_members jsonb:='[]';
 v_attachments jsonb;v_issue_set text;v_route_set text;v_content jsonb;v_content_hash bytea;v_recipient bytea;
begin
 perform private.weekly_source_query_require_service_v1();
 select * into strict v_mail from public.mail_outbox where id=p_outbox_id;
 select * into strict v_delivery from public.hr_issue_email_deliveries where id=p_delivery_id;
 if v_mail.type is distinct from 'TIMESHEET_QUERY' or v_delivery.mail_outbox_id is distinct from v_mail.id
  or v_mail.context_kind is distinct from 'TIMESHEET_QUERY_DELIVERY' or v_mail.context_id is distinct from v_delivery.id
  or v_mail.deterministic_outbox_key is distinct from v_delivery.deterministic_outbox_key
  or v_mail.reference is distinct from v_delivery.deterministic_outbox_key
  or v_mail.created_by is distinct from v_delivery.created_by_user_id
  or v_mail."to" is distinct from v_delivery.recipient_email
  or v_mail.email_type is distinct from 'TIMESHEET_QUERY' then
  raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
 perform public._import_review_assert_actor_v1(v_delivery.created_by_user_id);
 select * into strict v_operation from public.import_apply_operations where id=v_delivery.operation_id;
 select * into strict v_import from public.hr_imports where id=v_delivery.import_id;
 if v_operation.import_id is distinct from v_delivery.import_id or v_operation.committed_at_utc is null then
  raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
 v_current:=v_mail.status::text='QUEUED' and v_mail.sent_at is null and v_delivery.status='QUEUED'
  and v_import.pruned_at is null and v_operation.state in('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE');
 for v_item in select * from public.hr_issue_email_delivery_items
  where delivery_id=v_delivery.id order by action_id,id loop
  v_count:=v_count+1;
  if v_count>500 then raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
  select * into strict v_action from public.import_review_decisions where action_id=v_item.action_id;
  select * into strict v_issue from public.hr_issue_emails where id=v_item.issue_id;
  select * into strict v_ts from public.timesheets where timesheet_id=v_action.timesheet_id;
  if v_action.import_id is distinct from v_delivery.import_id or v_issue.import_id is distinct from v_delivery.import_id
   -- A first native enqueue creates the issue after the immutable action was
   -- catalogued, and fills only its native temporary issue projection. Its
   -- saved action may therefore genuinely have no issue_id yet. The real
   -- delivery item + exact immutable fingerprint still bind that new issue.
   or (v_action.issue_id is not null and v_action.issue_id is distinct from v_issue.id)
   or v_action.timesheet_id is distinct from v_issue.timesheet_id
   or v_action.client_id is distinct from v_issue.client_id or v_action.contract_id is distinct from v_issue.contract_id
   or v_action.candidate_id is null
   or v_action.client_id is null or v_item.issue_fingerprint is distinct from v_issue.issue_fingerprint
   or v_item.issue_fingerprint is distinct from coalesce(nullif(v_action.summary_json->>'issue_fingerprint',''),v_action.source_identity)
   or v_action.action_kind not in('EMAIL_ISSUE','EMAIL_REMINDER')
   or not exists(select 1 from public.import_review_action_outcomes o
    where o.import_id=v_delivery.import_id and o.operation_id=v_delivery.operation_id and o.action_id=v_action.action_id) then
   raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
  -- The physical Timesheet has no candidate_id column. Weekly identity is
  -- its real Contract; a genuine contractless Daily uses the same exact
  -- native import identity view that produced the reviewed action.
  if v_ts.contract_id is not null then
   if not exists(select 1 from public.contracts c where c.id=v_ts.contract_id
    and c.candidate_id=v_action.candidate_id and c.client_id=v_action.client_id) then
    raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
  elsif v_ts.sheet_scope::text<>'DAILY'
   or (select count(*) from public.v_timesheets_daily_match d where d.timesheet_id=v_ts.timesheet_id)<>1
   or not exists(select 1 from public.v_timesheets_daily_match d where d.timesheet_id=v_ts.timesheet_id
    and d.candidate_id=v_action.candidate_id and d.client_id=v_action.client_id) then
   raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';
  end if;
  v_route:=public._timesheet_query_recipient_resolve_core_v1(v_action.client_id,v_action.contract_id);
  v_evidence:=private.weekly_source_issued_signed_query_evidence_v1(v_action.timesheet_id,v_delivery.import_id);
  if v_route->>'recipient_email' is distinct from v_delivery.recipient_email
   or v_route->>'recipient_scope_key' is distinct from v_action.summary_json->>'recipient_scope_key'
   or v_route->>'route_fingerprint' is distinct from v_action.summary_json->>'recipient_route_fingerprint' then
   v_current:=false;end if;
  if v_action.is_current is distinct from true or v_action.selected is distinct from true
   or v_action.requires_reconfirmation is distinct from false or v_item.marked_sent_at_utc is not null
   or v_evidence->>'reason_code' is not null
   or v_evidence->'document_ready' is distinct from 'true'::jsonb
   or v_evidence->'source_complete' is distinct from 'true'::jsonb
   or v_evidence->>'evidence_fingerprint' is distinct from v_action.summary_json->>'attachment_fingerprint' then
   v_current:=false;end if;
  v_route_hashes:=array_append(v_route_hashes,v_route->>'route_fingerprint');
  v_issue_witnesses:=v_issue_witnesses||jsonb_build_array(jsonb_build_object('timesheet_id',v_action.timesheet_id,
   'issue_fingerprint',v_item.issue_fingerprint,'evidence_fingerprint',v_evidence->>'evidence_fingerprint',
   'r2_key',v_evidence->>'attachment_r2_key','filename',v_evidence->>'attachment_filename'));
  v_pairs:=v_pairs||jsonb_build_array(jsonb_build_object('candidate_id',v_action.candidate_id,'client_id',v_action.client_id));
  v_members:=v_members||jsonb_build_array(jsonb_build_object('action_id',v_action.action_id,'issue_id',v_issue.id,
   'issue_fingerprint',v_item.issue_fingerprint,'candidate_id',v_action.candidate_id,'client_id',v_action.client_id));
 end loop;
 select coalesce(jsonb_agg(x.pair order by x.pair->>'candidate_id',x.pair->>'client_id'),'[]'::jsonb)
  into v_pairs from(select distinct value pair from jsonb_array_elements(v_pairs))x;
 select coalesce(jsonb_agg(jsonb_build_object('r2_key',x.item->'r2_key','filename',x.item->'filename')
  order by x.item->>'timesheet_id'),'[]'::jsonb) into v_attachments from(
  select distinct on(value->>'timesheet_id')value item from jsonb_array_elements(v_issue_witnesses)
  order by value->>'timesheet_id',value->>'issue_fingerprint'
 )x;
 select public._import_review_hash_v1(string_agg(x.item->>'issue_fingerprint'||':'||(x.item->>'evidence_fingerprint'),
  '|' order by x.item->>'issue_fingerprint')) into v_issue_set from jsonb_array_elements(v_issue_witnesses)x(item);
 select public._import_review_hash_v1(string_agg(distinct h,'|' order by h)) into v_route_set from unnest(v_route_hashes)h;
 if v_count=0 or v_mail.attachments is distinct from v_attachments
  or v_delivery.issue_set_fingerprint is distinct from v_issue_set
  or v_delivery.recipient_route_fingerprint is distinct from v_route_set
  or v_delivery.recipient_scope_key is distinct from 'RECIPIENT_EMAIL:'||public._import_review_hash_v1(v_delivery.recipient_email) then
  v_current:=false;end if;
 -- The server-owned native enqueue freezes these fields. The trigger below
 -- forbids in-place body/recipient/attachment edits for this exact mail type;
 -- recomposition must retain this row and create the genuine new native row.
 v_content:=jsonb_build_object('mail_outbox_id',v_mail.id,'query_delivery_id',v_delivery.id,
  'type',v_mail.type,'to',v_mail."to",'cc',v_mail.cc,'bcc',v_mail.bcc,'reply_to',v_mail.reply_to,
  'subject',v_mail.subject,'body_html',v_mail.body_html,'body_text',v_mail.body_text,'attachments',v_mail.attachments,
  'importance',v_mail.importance,'attachment_delivery_policy',v_mail.attachment_delivery_policy,
  'recipient_kind',v_mail.recipient_kind,'recipient_id',v_mail.recipient_id,
  'context_kind',v_mail.context_kind,'context_id',v_mail.context_id,'email_type',v_mail.email_type,
  'reference',v_mail.reference,'deterministic_outbox_key',v_mail.deterministic_outbox_key,
  'created_by',v_mail.created_by,'delivery_items',v_members);
 v_content_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_CONTENT_V1',v_content);
 v_recipient:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_RECIPIENT_V1',jsonb_build_object(
  'to',v_mail."to",'cc',v_mail.cc,'bcc',v_mail.bcc,'reply_to',v_mail.reply_to,
  'recipient_route_fingerprint',v_delivery.recipient_route_fingerprint));
 return jsonb_build_object('mail_outbox_id',v_mail.id,'query_delivery_id',v_delivery.id,'scope_pairs',v_pairs,
  'content_hash',encode(v_content_hash,'hex'),'recipient_hash',encode(v_recipient,'hex'),
  'members',v_members,'current',coalesce(v_current,false));
end;
$function$;

create or replace function private.weekly_contact_query_payload_immutable_v1() returns trigger
language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
 if (old.type='TIMESHEET_QUERY' or new.type='TIMESHEET_QUERY') and (
  (old.type,old."to",old.cc,old.bcc,old.reply_to,old.subject,old.body_html,old.body_text,old.attachments,
   old.importance,old.attachment_delivery_policy,old.recipient_kind,old.recipient_id,old.context_kind,old.context_id,
   old.email_type,old.reference,old.deterministic_outbox_key,old.created_by)
  is distinct from
  (new.type,new."to",new.cc,new.bcc,new.reply_to,new.subject,new.body_html,new.body_text,new.attachments,
   new.importance,new.attachment_delivery_policy,new.recipient_kind,new.recipient_id,new.context_kind,new.context_id,
   new.email_type,new.reference,new.deterministic_outbox_key,new.created_by)) then
  raise exception 'CONTACT_QUERY_FROZEN_PAYLOAD_IMMUTABLE' using errcode='23514';end if;
 return new;
end;
$function$;

create or replace trigger weekly_contact_query_payload_immutable_v1 before update of
 type,"to",cc,bcc,reply_to,subject,body_html,body_text,attachments,importance,attachment_delivery_policy,
 recipient_kind,recipient_id,context_kind,context_id,email_type,reference,deterministic_outbox_key,created_by
 on public.mail_outbox for each row execute function private.weekly_contact_query_payload_immutable_v1();

alter function private.weekly_contact_query_content_v1(uuid,uuid) owner to postgres;
alter function private.weekly_contact_query_payload_immutable_v1() owner to postgres;
revoke all on function private.weekly_contact_query_content_v1(uuid,uuid),
 private.weekly_contact_query_payload_immutable_v1() from public,anon,authenticated,service_role;

create or replace function private.weekly_contact_query_admission_v1(p_request jsonb) returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_keys text[]:=array['version','owner_kind','mail_outbox_id','query_delivery_id',
 'attempt_lease_token','worker_id','agency_id','environment'];
 v_outbox uuid;v_delivery_id uuid;v_agency uuid;v_environment text;v_token text;v_worker text;
 v_mail public.mail_outbox%rowtype;v_delivery public.hr_issue_email_deliveries%rowtype;
 v_permit public.weekly_contact_submission_permits%rowtype;v_before jsonb;v_after jsonb;
 v_pair jsonb;v_member jsonb;v_guard jsonb;v_scope uuid;v_scope_row public.weekly_contact_pause_scopes%rowtype;
 v_scopes jsonb:='[]';v_until timestamptz;v_at timestamptz;v_logical bytea;v_permit_id uuid;
 v_base jsonb;v_ordinal integer;v_contact bytea;v_budget bytea;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_assert_closed_v1(p_request,v_keys,v_keys,'CONTACT_REQUEST_INVALID');
 if p_request->>'version' is distinct from 'CONTACT_START_V1'
  or p_request->>'owner_kind' is distinct from 'TIMESHEET_QUERY_OUTBOX'
  or jsonb_typeof(p_request->'attempt_lease_token') is distinct from 'string'
  or char_length(p_request->>'attempt_lease_token') not between 1 and 500
  or jsonb_typeof(p_request->'worker_id') is distinct from 'string'
  or char_length(p_request->>'worker_id') not between 1 and 200
  or p_request->>'environment' is null or p_request->>'environment' not in('TEST','LIVE') then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 v_outbox:=private.weekly_contact_uuid_v1(p_request->'mail_outbox_id');
 v_delivery_id:=private.weekly_contact_uuid_v1(p_request->'query_delivery_id');
 v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');v_environment:=p_request->>'environment';
 v_token:=p_request->>'attempt_lease_token';v_worker:=p_request->>'worker_id';
 perform private.weekly_contact_deployment_lock_v1(v_agency,v_environment);
 perform private.weekly_contact_context_v1(v_agency,v_environment);
 v_before:=private.weekly_contact_query_content_v1(v_outbox,v_delivery_id);
 for v_pair in select value from jsonb_array_elements(v_before->'scope_pairs')
  order by value->>'candidate_id',value->>'client_id' loop
  perform private.weekly_contact_scope_lock_v1(v_agency,v_environment,
   (v_pair->>'candidate_id')::uuid,(v_pair->>'client_id')::uuid);
 end loop;
 -- Same existing outbox -> delivery order as the native successful mark.
 -- There is exactly one of each; no later deployment/scope/config lock.
 select * into strict v_mail from public.mail_outbox where id=v_outbox for update;
 select * into strict v_delivery from public.hr_issue_email_deliveries where id=v_delivery_id for update;
 v_after:=private.weekly_contact_query_content_v1(v_outbox,v_delivery_id);
 if (v_before-'current') is distinct from (v_after-'current') then
  raise exception 'CONTACT_QUERY_CONTENT_CHANGED' using errcode='40001';end if;
 v_at:=clock_timestamp();
 if v_mail.attempt_lease_token is distinct from v_token or v_mail.attempt_leased_at_utc is null
  or v_mail.attempt_leased_at_utc>v_at or v_mail.attempt_lease_expires_at_utc is null
  or v_mail.attempt_lease_expires_at_utc<=v_at then
  raise exception 'CONTACT_QUERY_LEASE_STALE' using errcode='40001';end if;
 v_base:=jsonb_build_object('version','CONTACT_START_RESULT_V1','owner_kind','TIMESHEET_QUERY_OUTBOX',
  'channel','EMAIL','mail_outbox_id',v_outbox,'query_delivery_id',v_delivery_id);
 v_logical:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_DELIVERY_V1',jsonb_build_object(
  'mail_outbox_id',v_outbox,'query_delivery_id',v_delivery_id,'content_hash',v_after->>'content_hash'));
 select * into v_permit from public.weekly_contact_submission_permits
  where logical_delivery_key=v_logical and status in('PREPARED','STARTED','AMBIGUOUS','ACCEPTED') for update;
 if found then
  -- Lease expiry or a different claim token cannot create another logical
  -- submission while an original admitted/unknown/accepted attempt owns it.
  return v_base||jsonb_build_object('ok',false,'reason','CHECK_ORIGINAL_ATTEMPT',
   'call_once',false,'next_eligible_at_utc',null);end if;
 if v_after->'current' is distinct from 'true'::jsonb then
  return v_base||jsonb_build_object('ok',false,'reason','OBSOLETE','call_once',false,'next_eligible_at_utc',null);end if;
 for v_pair in select value from jsonb_array_elements(v_after->'scope_pairs')
  order by value->>'candidate_id',value->>'client_id' loop
  v_guard:=private.weekly_contact_scope_guard_v1((v_pair->>'candidate_id')::uuid,
   (v_pair->>'client_id')::uuid,'MANAGER',v_at);
  v_scope:=(v_guard->>'scope_id')::uuid;
  if v_scope is null or jsonb_typeof(v_guard->'allow') is distinct from 'boolean'
   or jsonb_typeof(v_guard->'revision') is distinct from 'string' then
   raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
  v_scopes:=v_scopes||jsonb_build_array(jsonb_build_object('scope_id',v_scope,
   'candidate_id',v_pair->'candidate_id','client_id',v_pair->'client_id','audience','MANAGER','revision',v_guard->>'revision'));
  if v_guard->'allow' is distinct from 'true'::jsonb then
   if v_guard->>'next_allowed_utc' is null then raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='42501';end if;
   v_until:=greatest(v_until,(v_guard->>'next_allowed_utc')::timestamptz);end if;
 end loop;
 if v_until is not null then
  return v_base||jsonb_build_object('ok',false,'reason','NOT_STARTED_PAUSED','call_once',false,
   'next_eligible_at_utc',to_char(v_until at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));end if;
 v_ordinal:=v_delivery.reminder_sequence+1;
 -- Native send ordinal is retained, independent of new request/claim tokens.
 -- If any frozen member was already accepted, retire/recompose the old body;
 -- never mark/send only its remaining members beneath that unchanged body.
 for v_member in select value from jsonb_array_elements(v_after->'members') loop
  v_contact:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_MEMBERSHIP_V1',
   jsonb_build_object('issue_id',v_member->'issue_id','purpose','TIMESHEET_QUERY'));
  if exists(select 1 from public.weekly_contact_delivery_memberships m
   where m.logical_contact_key=v_contact and m.send_ordinal=v_ordinal and m.accepted_at_utc is not null) then
   return v_base||jsonb_build_object('ok',false,'reason','OBSOLETE','call_once',false,'next_eligible_at_utc',null);end if;
 end loop;
 insert into public.weekly_contact_submission_permits(environment,agency_id,owner_kind,logical_delivery_key,
  mail_outbox_id,query_delivery_id,channel,provider,content_hash,recipient_hash,scope_revisions_json,
  lease_owner,lease_token,lease_expires_at_utc,status,admitted_at_utc,consumed_at_utc)
 values(v_environment,v_agency,'TIMESHEET_QUERY_OUTBOX',v_logical,v_outbox,v_delivery_id,'EMAIL','POWER_AUTOMATE',
  decode(v_after->>'content_hash','hex'),decode(v_after->>'recipient_hash','hex'),v_scopes,
  v_worker,v_token,v_mail.attempt_lease_expires_at_utc,'STARTED',v_at,v_at)
 returning id into v_permit_id;
 for v_member in select value from jsonb_array_elements(v_after->'members') loop
  select * into strict v_scope_row from public.weekly_contact_pause_scopes
   where environment=v_environment and agency_id=v_agency
    and candidate_id=(v_member->>'candidate_id')::uuid and client_id=(v_member->>'client_id')::uuid;
  v_contact:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_MEMBERSHIP_V1',
   jsonb_build_object('issue_id',v_member->'issue_id','purpose','TIMESHEET_QUERY'));
  v_budget:=private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_BUDGET_V1',
   jsonb_build_object('issue_id',v_member->'issue_id','purpose','TIMESHEET_QUERY'));
  insert into public.weekly_contact_delivery_memberships(logical_contact_key,purpose,audience,
   candidate_id,client_id,scope_id,scope_revision,delivery_id,payload_revision,content_hash,send_ordinal,budget_owner_key)
  values(v_contact,'TIMESHEET_QUERY','MANAGER',(v_member->>'candidate_id')::uuid,(v_member->>'client_id')::uuid,
   v_scope_row.id,v_scope_row.revision,v_delivery_id,1,decode(v_after->>'content_hash','hex'),v_ordinal,v_budget)
  on conflict(delivery_id,logical_contact_key,send_ordinal)do nothing;
  if not exists(select 1 from public.weekly_contact_delivery_memberships m where m.delivery_id=v_delivery_id
   and m.logical_contact_key=v_contact and m.send_ordinal=v_ordinal and m.content_hash=decode(v_after->>'content_hash','hex')
   and m.candidate_id=(v_member->>'candidate_id')::uuid and m.client_id=(v_member->>'client_id')::uuid
   and m.scope_id=v_scope_row.id and m.budget_owner_key=v_budget and m.accepted_at_utc is null) then
   raise exception 'CONTACT_QUERY_MEMBERSHIP_CONFLICT' using errcode='40001';end if;
 end loop;
 return v_base||jsonb_build_object('ok',true,'contact_permit_id',v_permit_id,
  'content_hash',v_after->>'content_hash','call_once',true);
end;
$function$;

alter function private.weekly_contact_query_admission_v1(jsonb) owner to postgres;
revoke all on function private.weekly_contact_query_admission_v1(jsonb) from public,anon,authenticated,service_role;

-- One public tagged entry, with both genuine native owners. The Worker supplies
-- only its trusted deployment/worker and the existing native lease identities.
create or replace function public.weekly_contact_submission_start_v1(p_request jsonb) returns jsonb
language plpgsql volatile security definer
set search_path to 'pg_catalog','private','pg_temp'
as $function$
begin
 perform private.weekly_source_query_require_service_v1();
 if p_request is null or jsonb_typeof(p_request) is distinct from 'object' then
  raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 case p_request->>'owner_kind'
  when 'WEEKLY_SOURCE_TARGET' then return private.weekly_contact_source_admission_v1(p_request);
  when 'TIMESHEET_QUERY_OUTBOX' then return private.weekly_contact_query_admission_v1(p_request);
  else raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';
 end case;
end;
$function$;

alter function public.weekly_contact_submission_start_v1(jsonb) owner to postgres;
revoke all on function public.weekly_contact_submission_start_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_contact_submission_start_v1(jsonb) to service_role;
\ir includes/weekly_contact_source_native_start_v1.sqlinc
notify pgrst,'reload schema';

commit;
