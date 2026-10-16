-- Reviewed Health contact lineage and rolling-upgrade claim fence; TEST release only.
-- Complete native snapshot, lease/result guards and exact ACL commit together.
-- Source proposal f601343afd2d05dcefb085239ae5559cf57c48c0.
\set ON_ERROR_STOP on
begin;

-- 010_existing_delivery_lineage.sqlinc
-- Minimal additive lineage in the existing native store. No new queue/store.
-- Serialize the snapshot/fence installation with native mail mutations. An
-- already executing old claim is also fenced at its actual lease UPDATE by
-- the new query-only trigger; replacing its function alone is insufficient.
lock table public.mail_outbox in share row exclusive mode;
alter table public.hr_issue_email_deliveries
 add column original_delivery_id uuid,
 add column payload_revision bigint not null default 1 check(payload_revision>=1),
 add column next_payload_revision bigint not null default 2 check(next_payload_revision>=2),
 add column contact_managed boolean not null default false,
 add column contact_rechecked_at_utc timestamptz,
 add column legacy_claim_lease_token text,
 add column legacy_claim_leased_at_utc timestamptz,
 add column legacy_claim_expires_at_utc timestamptz,
 add column contact_claim_backend_pid integer,
 add column contact_claim_transaction_id xid8,
 add column contact_claim_token text,
 add column contact_claim_at_utc timestamptz,
 add column contact_claim_expires_at_utc timestamptz,
 add column retired_at_utc timestamptz,
 add column retirement_reason text check(retirement_reason is null or retirement_reason in('CONTACT_PAUSED','CONTACT_OBSOLETE','CONTACT_RECOMPOSED','CONTACT_ACCEPTED_ELSEWHERE'));
-- Preserve genuine existing identities/timestamps; original is itself, never a
-- guessed latest row. Existing accepted legacy history remains legacy history.
update public.hr_issue_email_deliveries set original_delivery_id=id;
-- Snapshot only real native claim facts already present. A lease is not a
-- fabricated START, acceptance or NOT_SUBMITTED proof, even after expiry.
update public.hr_issue_email_deliveries d set legacy_claim_lease_token=o.attempt_lease_token,
 legacy_claim_leased_at_utc=o.attempt_leased_at_utc,legacy_claim_expires_at_utc=o.attempt_lease_expires_at_utc
 from public.mail_outbox o where o.id=d.mail_outbox_id and o.type='TIMESHEET_QUERY' and o.status::text<>'SENT' and o.sent_at is null
  and o.attempt_lease_token is not null;
update public.hr_issue_email_deliveries d set contact_managed=true
 where d.status<>'SENT' and d.legacy_claim_lease_token is null and not exists(select 1 from public.mail_outbox o where o.id=d.mail_outbox_id and(o.status::text='SENT' or o.sent_at is not null));
alter table public.hr_issue_email_deliveries alter column original_delivery_id set not null;
alter table public.hr_issue_email_deliveries alter column contact_managed set default true;
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_original_fk
 foreign key(original_delivery_id) references public.hr_issue_email_deliveries(id) on delete restrict;
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_root_revision_uq unique(original_delivery_id,payload_revision);
alter table public.hr_issue_email_deliveries drop constraint hr_issue_email_deliveries_scope_ck;
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_scope_ck check(
 recipient_scope in('CLIENT_DEFAULT','CONTRACT_OVERRIDE') and reminder_sequence>=0 and length(recipient_email) between 3 and 320
 and length(recipient_route_fingerprint)=64 and recipient_route_fingerprint~'^[0-9a-f]{64}$'
 and length(issue_set_fingerprint)=64 and issue_set_fingerprint~'^[0-9a-f]{64}$'
 and status in('QUEUED','ACCEPTED','SENT','FAILED','RETIRED')
 and ((status='RETIRED' and retired_at_utc is not null and retirement_reason is not null)
  or(status<>'RETIRED' and retired_at_utc is null and retirement_reason is null)));
-- Match the genuine old uniqueness by its exact column vector, retaining all
-- other constraints. Its generated name can be truncated by PostgreSQL.
do $migration$
declare constraint_name text;
begin
 select c.conname into strict constraint_name from pg_constraint c
 where c.conrelid='public.hr_issue_email_deliveries'::regclass and c.contype='u'
 and (select array_agg(a.attname::text order by key.ordinality) from unnest(c.conkey)with ordinality key(attnum,ordinality)
  join pg_attribute a on a.attrelid=c.conrelid and a.attnum=key.attnum)
 =array['operation_id','recipient_scope_key','issue_set_fingerprint','reminder_sequence'];
 execute format('alter table public.hr_issue_email_deliveries drop constraint %I',constraint_name);
end;
$migration$;
alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_content_revision_uq
 unique(operation_id,recipient_scope_key,issue_set_fingerprint,reminder_sequence,payload_revision);
create index hr_issue_email_deliveries_contact_retired_idx on public.hr_issue_email_deliveries(retired_at_utc,id)
 where status='RETIRED' and original_delivery_id=id;


-- 080_native_integrity.sqlinc
create or replace function private.weekly_contact_query_integrity_v1()
returns trigger language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare d public.hr_issue_email_deliveries%rowtype;r public.hr_issue_email_deliveries%rowtype;p public.weekly_contact_submission_permits%rowtype;
begin
 if tg_table_name='hr_issue_email_deliveries' then
  -- The original enqueue may run during DB-first runtime compatibility. Its
  -- genuine initial native INSERT owns this actual ID and revision1. It need
  -- not know the additive column to obtain the same explicit self lineage.
  if tg_op='INSERT' and new.original_delivery_id is null and new.payload_revision=1 then new.original_delivery_id:=new.id;end if;
  if tg_op='UPDATE' and (old.original_delivery_id,old.payload_revision,old.contact_managed,old.import_id,old.operation_id,old.recipient_scope,
    old.recipient_scope_key,old.recipient_route_fingerprint,old.recipient_email,old.reminder_sequence,old.issue_set_fingerprint,
    old.deterministic_outbox_key,old.mail_outbox_id,old.created_by_user_id,old.created_at_utc,old.legacy_claim_lease_token,old.legacy_claim_leased_at_utc,old.legacy_claim_expires_at_utc)
   is distinct from(new.original_delivery_id,new.payload_revision,new.contact_managed,new.import_id,new.operation_id,new.recipient_scope,
    new.recipient_scope_key,new.recipient_route_fingerprint,new.recipient_email,new.reminder_sequence,new.issue_set_fingerprint,
    new.deterministic_outbox_key,new.mail_outbox_id,new.created_by_user_id,new.created_at_utc,new.legacy_claim_lease_token,new.legacy_claim_leased_at_utc,new.legacy_claim_expires_at_utc) then
   raise exception 'CONTACT_QUERY_LINEAGE_IMMUTABLE' using errcode='23514';end if;
  if tg_op='UPDATE' and old.status='RETIRED' and(new.status,old.retired_at_utc,old.retirement_reason) is distinct from(old.status,new.retired_at_utc,new.retirement_reason) then
   raise exception 'CONTACT_QUERY_RETIRED_DELIVERY' using errcode='23514';end if;
  if new.original_delivery_id=new.id then
   if new.payload_revision<>1 then raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='23514';end if;
  else
   select * into strict r from public.hr_issue_email_deliveries where id=new.original_delivery_id;
   if r.original_delivery_id<>r.id or r.payload_revision<>1 or new.payload_revision<2 or new.import_id<>r.import_id
    or new.operation_id<>r.operation_id or new.reminder_sequence<>r.reminder_sequence or new.created_by_user_id is distinct from r.created_by_user_id then
    raise exception 'CONTACT_QUERY_SCOPE_INVALID' using errcode='23514';end if;
  end if;
  return new;
 elsif tg_table_name='hr_issue_email_delivery_items' then
  if tg_op='DELETE' or (old.delivery_id,old.issue_id,old.action_id,old.issue_fingerprint) is distinct from(new.delivery_id,new.issue_id,new.action_id,new.issue_fingerprint) then
   raise exception 'CONTACT_QUERY_FROZEN_ITEMS_IMMUTABLE' using errcode='23514';end if;
  select * into strict d from public.hr_issue_email_deliveries where id=new.delivery_id;
  if d.contact_managed and old.marked_sent_at_utc is distinct from new.marked_sent_at_utc and not exists(
   select 1 from public.weekly_contact_submission_permits x where x.query_delivery_id=d.id and x.status='ACCEPTED'
    and x.accepted_at_utc is not distinct from new.marked_sent_at_utc) then raise exception 'CONTACT_QUERY_ACCEPTED_PROOF_REQUIRED' using errcode='42501';end if;
  return new;
 elsif tg_table_name='mail_outbox' then
  if new.type<>'TIMESHEET_QUERY' then return new;end if;
  select * into strict d from public.hr_issue_email_deliveries where mail_outbox_id=new.id;
  if old.status::text='SENT' or old.sent_at is not null then
   if new.status::text<>'SENT' or new.sent_at is distinct from old.sent_at or new.provider_message_id is distinct from old.provider_message_id then
    raise exception 'CONTACT_QUERY_ACCEPTED_HISTORY_IMMUTABLE' using errcode='23514';end if;
   -- Genuine later delivery/read callbacks retain their existing metadata.
   return new;
  end if;
  if new.status::text<>'SENT' and new.sent_at is null then return new;end if;
  if not d.contact_managed then
   if d.legacy_claim_lease_token is null or d.legacy_claim_leased_at_utc is null or d.legacy_claim_expires_at_utc is null
    or d.legacy_claim_expires_at_utc<=d.legacy_claim_leased_at_utc or old.attempt_lease_token is distinct from d.legacy_claim_lease_token
    or old.attempt_leased_at_utc is distinct from d.legacy_claim_leased_at_utc
    or old.attempt_lease_expires_at_utc is distinct from d.legacy_claim_expires_at_utc
    or new.status::text<>'SENT' or new.sent_at is null or new.provider_status is distinct from 'ACCEPTED' then
    raise exception 'CONTACT_QUERY_LEGACY_CLAIM_PROOF_REQUIRED' using errcode='42501';end if;
   return new;
  end if;
  select * into p from public.weekly_contact_submission_permits x where x.query_delivery_id=d.id and x.mail_outbox_id=new.id and x.status='ACCEPTED';
  if p.id is null or d.status='RETIRED' or p.accepted_at_utc is distinct from new.sent_at or new.provider_status is distinct from 'ACCEPTED'
   or p.provider_idempotency_identity is distinct from new.provider_message_id
   or p.content_hash<>decode(private.weekly_contact_query_saved_payload_v1(old.id,d.id)->>'content_hash','hex') then
   raise exception 'CONTACT_QUERY_ACCEPTED_PROOF_REQUIRED' using errcode='42501';end if;
  return new;
 end if;
 raise exception 'CONTACT_QUERY_GUARD_SCOPE_INVALID' using errcode='23514';
end;
$function$;


-- 081_native_claim_guard.sqlinc
-- Existing native claim provenance only. This is not provider admission.
-- The real picked CTE has already locked these exact mail rows with the native
-- queue predicates/order/bounds. Fixed deployment/service checks are repeated.
create or replace function public._email_outbox_contact_claim_stamp_v1(p_ids uuid[],p_token text,p_at timestamptz,p_expires timestamptz,p_agency uuid,p_environment text)
returns boolean language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_context_v1(p_agency,p_environment);
 if cardinality(p_ids)>500 or nullif(btrim(p_token),'') is null or p_at is distinct from transaction_timestamp()
  or p_expires<=p_at or p_expires>p_at+interval '30 minutes' then raise exception 'CONTACT_QUERY_CLAIM_INVALID' using errcode='22023';end if;
 update public.hr_issue_email_deliveries d set contact_claim_backend_pid=pg_backend_pid(),contact_claim_transaction_id=pg_current_xact_id(),
  contact_claim_token=p_token,contact_claim_at_utc=p_at,contact_claim_expires_at_utc=p_expires
 from public.mail_outbox o,public.hr_issue_email_deliveries original
 where o.id=any(p_ids) and d.mail_outbox_id=o.id and o.type='TIMESHEET_QUERY' and d.contact_managed and d.legacy_claim_lease_token is null
  and d.status='QUEUED' and d.retired_at_utc is null and original.id=d.original_delivery_id and original.original_delivery_id=original.id and original.payload_revision=1
  and o.status::text='QUEUED' and o.sent_at is null and o.delivered_at is null and o.read_at is null
  and coalesce(o.next_attempt_at_utc,o.scheduled_for_utc,o.created_at_utc)<=p_at
  and(o.attempt_lease_token is null or o.attempt_lease_expires_at_utc is null or o.attempt_lease_expires_at_utc<=p_at)
  and d.contact_claim_backend_pid is null and d.contact_claim_transaction_id is null and d.contact_claim_token is null
  and d.contact_claim_at_utc is null and d.contact_claim_expires_at_utc is null;
 return true;
end;
$function$;

create or replace function public._email_outbox_contact_claim_cleanup_v1(p_token text)
returns void language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
begin
 perform private.weekly_source_query_require_service_v1();
 update public.hr_issue_email_deliveries set contact_claim_backend_pid=null,contact_claim_transaction_id=null,contact_claim_token=null,
  contact_claim_at_utc=null,contact_claim_expires_at_utc=null
 where contact_claim_backend_pid=pg_backend_pid() and contact_claim_transaction_id=pg_current_xact_id_if_assigned() and contact_claim_token=p_token;
end;
$function$;

create or replace function private.weekly_contact_query_claim_guard_v1()
returns trigger language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare d public.hr_issue_email_deliveries%rowtype;
begin
 if tg_table_name='hr_issue_email_deliveries' then
  -- Deferred final state, including update-then-clear within the same claim.
  if exists(select 1 from public.hr_issue_email_deliveries x where x.id=new.id and
   (x.contact_claim_backend_pid is not null or x.contact_claim_transaction_id is not null or x.contact_claim_token is not null
    or x.contact_claim_at_utc is not null or x.contact_claim_expires_at_utc is not null)) then
   raise exception 'CONTACT_QUERY_CLAIM_STAMP_SURVIVED' using errcode='23514';end if;
  return null;
 end if;
 if new.type<>'TIMESHEET_QUERY' or new.attempt_lease_token is null
  or(new.attempt_lease_token,new.attempt_leased_at_utc,new.attempt_lease_expires_at_utc)
   is not distinct from(old.attempt_lease_token,old.attempt_leased_at_utc,old.attempt_lease_expires_at_utc) then return new;end if;
 select * into strict d from public.hr_issue_email_deliveries where mail_outbox_id=new.id;
 if not d.contact_managed or d.legacy_claim_lease_token is not null or d.status<>'QUEUED' or d.retired_at_utc is not null
  or d.contact_claim_backend_pid is distinct from pg_backend_pid() or d.contact_claim_transaction_id is distinct from pg_current_xact_id_if_assigned()
  or d.contact_claim_token is distinct from new.attempt_lease_token or d.contact_claim_at_utc is distinct from new.attempt_leased_at_utc
  or d.contact_claim_expires_at_utc is distinct from new.attempt_lease_expires_at_utc or new.attempt_leased_at_utc is distinct from transaction_timestamp()
  or new.status::text<>'QUEUED' or new.sent_at is not null or new.delivered_at is not null or new.read_at is not null then
  raise exception 'CONTACT_QUERY_CURRENT_CLAIM_BRIDGE_REQUIRED' using errcode='42501';end if;
 return new;
end;
$function$;


-- 082_native_claim_kernel.sqlinc
create or replace function public._email_outbox_contact_claim_core_v1(
  p_limit integer,
  p_attempt_lease_token text,
  p_lease_minutes integer default 5,p_contact_bridge boolean default false,p_agency uuid default null,p_environment text default null
) returns setof public.mail_outbox
language plpgsql
security invoker
set search_path to 'public','pg_temp'
as $function$
declare
  v_now timestamptz:=now();
  v_limit integer:=greatest(0,least(coalesce(p_limit,0),500));
  v_lease integer:=greatest(1,least(coalesce(p_lease_minutes,5),30));
begin
  if coalesce(btrim(p_attempt_lease_token),'')='' then raise exception 'attempt_lease_token is required'; end if;
  if v_limit=0 then return; end if;

  return query
  with picked as materialized (
    select mo.id
    from public.mail_outbox mo
    where (mo.type<>'TIMESHEET_QUERY' or(p_contact_bridge and exists(
      select 1 from public.hr_issue_email_deliveries d join public.hr_issue_email_deliveries original on original.id=d.original_delivery_id
      where d.mail_outbox_id=mo.id and d.contact_managed and d.legacy_claim_lease_token is null
       and d.status='QUEUED' and d.retired_at_utc is null and original.original_delivery_id=original.id and original.payload_revision=1)))
      and mo.status='QUEUED' and mo.sent_at is null and mo.delivered_at is null and mo.read_at is null
      and coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc)<=v_now
      and (mo.attempt_lease_token is null or mo.attempt_lease_expires_at_utc is null
        or mo.attempt_lease_expires_at_utc<=v_now)
      and (
        not (
          nullif(btrim(coalesce(mo.payment_scope_json->>'candidate_workflow_id','')),'') is not null
          or mo.payment_scope_json ? 'candidate_workflow_generation'
          or mo.payment_scope_json ? 'paper_return_manifest_sha256'
          or mo.payment_scope_json ? 'candidate_paper_pack_ready'
          or mo.payment_scope_json ? 'candidate_complete_pack_storage_key'
          or upper(coalesce(mo.payment_scope_json->>'mail_hold_reason',''))
            ='CANDIDATE_PAPER_PACK_PENDING'
          or upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
        )
        or (
          coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.payment_scope_json->>'paper_return_manifest_sha256',''))
            ~ '^[0-9a-f]{64}$'
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in('true','t','1','yes')
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in('false','f','0','no')
          and lower(coalesce(mo.payment_scope_json->>'mail_held_until_pdf_rendered','true'))
            in('false','f','0','no')
          and nullif(btrim(coalesce(mo.payment_scope_json->>'mail_hold_reason','')),'') is null
          and jsonb_typeof(mo.attachments)='array'
          and jsonb_array_length(mo.attachments)=1
          and nullif(btrim(coalesce(mo.attachments->0->>'r2_key','')),'') is not null
          and lower(coalesce(mo.attachments->0->>'sha256','')) ~ '^[0-9a-f]{64}$'
          and coalesce(mo.attachments->0->>'size_bytes','') ~ '^[1-9][0-9]{0,18}$'
          and coalesce(mo.attachments->0->>'page_count','') ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.attachments->0->>'content_type',''))='application/pdf'
          and mo.attachments->0->>'r2_key'
            =mo.payment_scope_json->>'candidate_complete_pack_storage_key'
          and lower(mo.attachments->0->>'sha256')
            =lower(mo.payment_scope_json->>'candidate_complete_pack_sha256')
          and mo.attachments->0->>'size_bytes'
            =mo.payment_scope_json->>'candidate_complete_pack_size_bytes'
          and mo.attachments->0->>'page_count'
            =mo.payment_scope_json->>'candidate_complete_pack_page_count'
          and lower(coalesce(mo.payment_scope_json->>'candidate_complete_pack_media_type',''))
            ='application/pdf'
          and mo.attachments->0->>'candidate_workflow_id'
            =mo.payment_scope_json->>'candidate_workflow_id'
          and mo.attachments->0->>'candidate_workflow_generation'
            =mo.payment_scope_json->>'candidate_workflow_generation'
          and lower(mo.attachments->0->>'paper_return_manifest_sha256')
            =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
          and exists(
            select 1
            from public.candidate_submission_workflows workflow
            where workflow.id=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                then (mo.payment_scope_json->>'candidate_workflow_id')::uuid
                else null::uuid end
              and workflow.generation=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
                  ~ '^[1-9][0-9]{0,8}$'
                then (mo.payment_scope_json->>'candidate_workflow_generation')::integer
                else null::integer end
              and workflow.route='PAPER'
              and workflow.state='AWAITING_PAPER_RETURN'
              and encode(workflow.paper_return_manifest_sha256,'hex')
                =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
              and coalesce(workflow.target_timesheet_id,workflow.anchor_timesheet_id)=mo.context_id
          )
        )
        or (
          upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
          and upper(coalesce(mo.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL','CANCELLATION')
          and lower(coalesce(mo.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no')
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and (
            upper(mo.payment_scope_json->>'candidate_manager_mail_kind') in ('WITHDRAWAL','CANCELLATION')
            or (
              coalesce(mo.payment_scope_json->>'candidate_manager_route_receipt_id','') ~*
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              and coalesce(mo.payment_scope_json->>'candidate_manager_route_ticket_id','') ~*
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              and coalesce(mo.payment_scope_json->>'candidate_manager_route_revision','')
                ~ '^[1-9][0-9]{0,18}$'
              and lower(coalesce(
                    mo.payment_scope_json->>'candidate_manager_route_registration_sha256',''))
                ~ '^[0-9a-f]{64}$'
              and private._candidate_manager_email_claim_route_current_v1(
                (mo.payment_scope_json->>'candidate_manager_route_receipt_id')::uuid,
                (mo.payment_scope_json->>'candidate_manager_route_ticket_id')::uuid,
                (mo.payment_scope_json->>'candidate_manager_route_revision')::bigint,
                mo.payment_scope_json->>'candidate_manager_route_registration_sha256',
                mo.context_id,
                (mo.payment_scope_json->>'candidate_approval_request_id')::uuid,
                (mo.payment_scope_json->>'candidate_approval_request_generation')::integer
              )
            )
          )
          and mo.context_kind='CANDIDATE_WORKFLOW'
          and mo.context_id=(mo.payment_scope_json->>'candidate_manager_workflow_id')::uuid
          and exists (
            select 1
            from public.candidate_approval_requests request_row
            join public.candidate_submission_workflows workflow
              on workflow.id=request_row.workflow_id
            where request_row.id=(mo.payment_scope_json->>'candidate_approval_request_id')::uuid
              and request_row.workflow_id=mo.context_id
              and request_row.workflow_generation=
                    (mo.payment_scope_json->>'candidate_manager_workflow_generation')::integer
              and request_row.request_generation=
                    (mo.payment_scope_json->>'candidate_approval_request_generation')::integer
              and request_row.method='EMAIL'
              and request_row.manager_email_normalized=mo."to"
              and (
                (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind')
                    in ('INITIAL','REMINDER','RENEWAL')
                  and request_row.state='PENDING'
                  and request_row.expires_at_utc>v_now
                  and workflow.generation=request_row.workflow_generation
                  and workflow.route='EMAIL'
                  and workflow.state='AWAITING_MANAGER_APPROVAL'
                  and workflow.review_manifest_sha256=request_row.review_manifest_sha256
                )
                or (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind') in ('WITHDRAWAL','CANCELLATION')
                  and request_row.state in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED')
                )
              )
          )
        )
        or (
          -- The completed Weekly Timesheet copy is informational, not a
          -- manager-approval or paper-return command.  Admit only the exact
          -- immutable completion event and its single rendered PDF; leave
          -- every existing Candidate and payment-mail branch unchanged.
          mo.email_type='WEEKLY_COMPLETED_TIMESHEET_COPY'
          and mo.type='TIMESHEET_GENERAL'
          and mo.recipient_kind='CLIENT_INFORMATIONAL_COPY'
          and mo.context_kind='timesheets'
          and mo.attachments_ready=true
          and mo.attachment_delivery_policy='ATTACH'
          and mo.payment_scope_json->>'completed_pack_copy_authority'
            ='WEEKLY_COMPLETED_PACK_COPY_V1'
          and mo.payment_scope_json->>'informational_only'='true'
          and mo.payment_scope_json->>'changes_pay'='false'
          and mo.payment_scope_json->>'changes_invoice'='false'
          and nullif(btrim(coalesce(
            mo.payment_scope_json->>'candidate_mail_authority','')),'') is null
          and not (mo.payment_scope_json ? 'paper_return_manifest_sha256')
          and not (mo.payment_scope_json ? 'candidate_manager_mail_kind')
          and mo.body_html !~* 'href=|https?://'
          and mo.body_text !~* 'https?://'
          and jsonb_typeof(mo.attachments)='array'
          and jsonb_array_length(mo.attachments)=1
          and nullif(btrim(coalesce(mo.attachments->0->>'r2_key','')),'') is not null
          and lower(coalesce(mo.attachments->0->>'content_type',''))='application/pdf'
          and coalesce(mo.attachments->0->>'sha256','') ~ '^[0-9a-f]{64}$'
          and coalesce(mo.attachments->0->>'size_bytes','') ~ '^[1-9][0-9]{0,18}$'
          and coalesce(mo.attachments->0->>'page_count','') ~ '^[1-9][0-9]{0,8}$'
          and private._weekly_source_completed_pack_copy_claim_event_valid_v1(mo)
        )
      )
      and (
        upper(coalesce(mo.type,''))<>'INVOICE'
        or (
          mo.attachments_ready=true and mo.waiting_invoice_operation_id is null
          and jsonb_typeof(mo.attachments)='array' and jsonb_array_length(mo.attachments)>0
          and not exists (
            select 1 from jsonb_array_elements(mo.attachments) a
            left join lateral (
              select
                case when coalesce(a->>'document_version_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'document_version_id')::uuid end document_version_id,
                case when coalesce(a->>'invoice_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'invoice_id')::uuid end invoice_id,
                case when coalesce(a->>'size_bytes','') ~ '^[0-9]{1,18}$'
                  then(a->>'size_bytes')::bigint end size_bytes,
                case when coalesce(a->>'page_count','') ~ '^[0-9]{1,9}$'
                  then(a->>'page_count')::integer end page_count,
                upper(coalesce(nullif(a->>'delivery_mode',''),
                  'ATTACHMENT')) delivery_mode,
                lower(coalesce(a->>'secure_link_required','false'))
                  in('true','t','1','yes') secure_link_required
            ) parsed on true
            left join public.invoice_document_versions v
              on v.id=parsed.document_version_id
            left join public.invoices i on i.id=parsed.invoice_id
            where parsed.document_version_id is null or parsed.invoice_id is null
              or parsed.delivery_mode not in('ATTACHMENT','SECURE_LINK')
              or nullif(a->>'sha256','') is null
              or nullif(a->>'filename','') is null
              or coalesce(parsed.size_bytes,0)<=0 or coalesce(parsed.page_count,0)<=0
              or v.id is null or v.status<>'READY' or v.superseded_at_utc is not null
              or v.purpose<>'FINAL_ISSUE' or v.entity_type<>'INVOICE'
              or v.entity_id is distinct from parsed.invoice_id
              or i.id is null or i.status<>'ISSUED'
              or i.issued_document_version_id is distinct from v.id
              or v.sha256 is distinct from a->>'sha256'
              or v.size_bytes is distinct from parsed.size_bytes
              or v.page_count is distinct from parsed.page_count
              or(parsed.delivery_mode='ATTACHMENT' and(
                nullif(a->>'r2_key','') is null
                or nullif(a->>'mime_type','') is null
                or v.r2_key is distinct from a->>'r2_key'))
              or(parsed.delivery_mode='SECURE_LINK' and(
                parsed.secure_link_required is not true
                or nullif(a->>'r2_key','') is not null))
          )
        )
      )
    order by coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc),
      mo.created_at_utc,mo.id
    for update skip locked limit v_limit
  ),
  contact_claim as materialized (
    select case when p_contact_bridge then public._email_outbox_contact_claim_stamp_v1(
      coalesce(array_agg(p.id),'{}'::uuid[]),p_attempt_lease_token,v_now,v_now+make_interval(mins=>v_lease),p_agency,p_environment) else null end proof
    from picked p
  ),
  updated as (
    update public.mail_outbox mo set attempt_lease_token=p_attempt_lease_token,
      attempt_leased_at_utc=v_now,attempt_lease_expires_at_utc=v_now+make_interval(mins=>v_lease)
    from picked p cross join contact_claim proof where mo.id=p.id returning mo.*
  )
  select u.* from updated u
  order by coalesce(u.next_attempt_at_utc,u.scheduled_for_utc,u.created_at_utc),u.created_at_utc,u.id;
  if p_contact_bridge then perform public._email_outbox_contact_claim_cleanup_v1(p_attempt_lease_token);end if;
end;
$function$;


-- 083_native_claim_bridge.sqlinc
-- Preserve original generic signature, INVOKER role/RLS, bounds and non-query
-- behavior. The same selection/lease kernel excludes managed query work here.
create or replace function public.email_outbox_claim_ready_batch(p_limit integer,p_attempt_lease_token text,p_lease_minutes integer default 5)
returns setof public.mail_outbox language plpgsql security invoker set search_path='public','pg_temp'
as $function$
begin return query select * from public._email_outbox_contact_claim_core_v1(p_limit,p_attempt_lease_token,p_lease_minutes,false);end;
$function$;

-- This narrow read-only fixed-deployment assertion permits the claim bridge to
-- retain the original INVOKER queue/RLS semantics. It allocates no work/permit.
create or replace function public._email_outbox_contact_claim_deployment_v1(p_agency uuid,p_environment text)
returns void language plpgsql volatile security definer set search_path='pg_catalog','private','pg_temp'
as $function$
begin
 perform private.weekly_source_query_require_service_v1();
 perform private.weekly_contact_context_v1(p_agency,p_environment);
end;
$function$;

create or replace function public.email_outbox_claim_ready_batch_contact_v1(p_request jsonb)
returns setof public.mail_outbox language plpgsql security invoker set search_path='pg_catalog','public','pg_temp'
as $function$
begin
 -- Closed syntax here; actual authenticated fixed deployment is verified by
 -- the audited service-only context assertion. The mode is a native lease
 -- selector, never provider permission: Root START/SENT guards remain required.
 if p_request is null or jsonb_typeof(p_request)<>'object' or not(p_request?&array['version','limit','attempt_lease_token','lease_minutes','worker_id','agency_id','environment'])
  or p_request-array['version','limit','attempt_lease_token','lease_minutes','worker_id','agency_id','environment']<>'{}'::jsonb
  or p_request->>'version' is distinct from 'EMAIL_OUTBOX_CONTACT_CLAIM_V1'
  or jsonb_typeof(p_request->'limit') is distinct from 'number' or p_request->>'limit'!~'^[0-9]{1,3}$' or(p_request->>'limit')::integer>500
  or jsonb_typeof(p_request->'lease_minutes') is distinct from 'number' or p_request->>'lease_minutes'!~'^[0-9]{1,2}$' or(p_request->>'lease_minutes')::integer not between 1 and 30
  or jsonb_typeof(p_request->'attempt_lease_token') is distinct from 'string' or char_length(p_request->>'attempt_lease_token') not between 1 and 500
  or jsonb_typeof(p_request->'worker_id') is distinct from 'string' or char_length(p_request->>'worker_id') not between 1 and 200
  or jsonb_typeof(p_request->'agency_id') is distinct from 'string' or not coalesce(p_request->>'agency_id'~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',false)
  or not coalesce(p_request->>'environment' in('TEST','LIVE'),false) then raise exception 'CONTACT_REQUEST_INVALID' using errcode='22023';end if;
 perform public._email_outbox_contact_claim_deployment_v1((p_request->>'agency_id')::uuid,p_request->>'environment');
 return query select * from public._email_outbox_contact_claim_core_v1((p_request->>'limit')::integer,p_request->>'attempt_lease_token',(p_request->>'lease_minutes')::integer,true,(p_request->>'agency_id')::uuid,p_request->>'environment');
end;
$function$;

-- Preview/in-flight hook for root's closed union. A real prior native claim is
-- explicitly distinct from a START permit; expiry cannot retire its unknown.
create or replace function private.weekly_contact_query_legacy_claims_v1(p_candidate uuid,p_client uuid,p_agency uuid,p_environment text)
returns jsonb language plpgsql stable security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare result jsonb;
begin
 perform private.weekly_source_query_require_service_v1();
 select coalesce(jsonb_agg(jsonb_build_object('kind','LEGACY_QUERY_CLAIM','mail_outbox_id',x.mail_outbox_id,'query_delivery_id',x.id,
  'claim_identity_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_CONTACT_QUERY_LEGACY_CLAIM_V1',jsonb_build_object(
   'mail_outbox_id',x.mail_outbox_id,'query_delivery_id',x.id,'lease_token',x.legacy_claim_lease_token)),'hex'),
  'claimed_at_utc',case when x.legacy_claim_leased_at_utc is null then null else to_char(x.legacy_claim_leased_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
  'lease_expires_at_utc',case when x.legacy_claim_expires_at_utc is null then null else to_char(x.legacy_claim_expires_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') end,
  'status','CHECKING_ORIGINAL_LEGACY_CLAIM','provider_submission_established',false,'can_auto_resend',false) order by x.id),'[]'::jsonb)
 into result from public.hr_issue_email_deliveries x join public.mail_outbox o on o.id=x.mail_outbox_id
 where x.legacy_claim_lease_token is not null and not x.contact_managed and x.status<>'SENT' and o.sent_at is null and o.status::text<>'SENT'
  and exists(select 1 from public.hr_issue_email_delivery_items item join public.import_review_decisions a on a.action_id=item.action_id
   where item.delivery_id=x.id and a.candidate_id=p_candidate and a.client_id=p_client)
  and exists(select 1 from public.candidate_push_timing_policy policy where policy.agency_id=p_agency and policy.environment=p_environment and policy.deployment_slot);
 return result;
end;
$function$;


-- 091_claim_acl.sqlinc
alter function private.weekly_contact_query_integrity_v1() owner to postgres;
revoke all on function private.weekly_contact_query_integrity_v1() from public,anon,authenticated,service_role;
alter function public._email_outbox_contact_claim_stamp_v1(uuid[],text,timestamptz,timestamptz,uuid,text) owner to postgres;
revoke all on function public._email_outbox_contact_claim_stamp_v1(uuid[],text,timestamptz,timestamptz,uuid,text) from public,anon,authenticated,service_role;
grant execute on function public._email_outbox_contact_claim_stamp_v1(uuid[],text,timestamptz,timestamptz,uuid,text) to service_role;
alter function public._email_outbox_contact_claim_cleanup_v1(text) owner to postgres;
revoke all on function public._email_outbox_contact_claim_cleanup_v1(text) from public,anon,authenticated,service_role;
grant execute on function public._email_outbox_contact_claim_cleanup_v1(text) to service_role;
alter function private.weekly_contact_query_claim_guard_v1() owner to postgres;
revoke all on function private.weekly_contact_query_claim_guard_v1() from public,anon,authenticated,service_role;
alter function public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text) owner to postgres;
revoke all on function public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text) from public,anon,authenticated,service_role;
grant execute on function public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text) to service_role;
alter function public.email_outbox_claim_ready_batch(integer,text,integer) owner to postgres;
revoke all on function public.email_outbox_claim_ready_batch(integer,text,integer) from public,anon,authenticated,service_role;
grant execute on function public.email_outbox_claim_ready_batch(integer,text,integer) to service_role;
alter function public._email_outbox_contact_claim_deployment_v1(uuid,text) owner to postgres;
revoke all on function public._email_outbox_contact_claim_deployment_v1(uuid,text) from public,anon,authenticated,service_role;
grant execute on function public._email_outbox_contact_claim_deployment_v1(uuid,text) to service_role;
alter function public.email_outbox_claim_ready_batch_contact_v1(jsonb) owner to postgres;
revoke all on function public.email_outbox_claim_ready_batch_contact_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.email_outbox_claim_ready_batch_contact_v1(jsonb) to service_role;
alter function private.weekly_contact_query_legacy_claims_v1(uuid,uuid,uuid,text) owner to postgres;
revoke all on function private.weekly_contact_query_legacy_claims_v1(uuid,uuid,uuid,text) from public,anon,authenticated,service_role;


-- 084_native_claim_triggers.sqlinc
-- ONE-TIME protected migration only. Routine081 is independently repeatable.
create trigger weekly_contact_query_claim_guard_v1 before update of attempt_lease_token,attempt_leased_at_utc,attempt_lease_expires_at_utc on public.mail_outbox
 for each row execute function private.weekly_contact_query_claim_guard_v1();
create constraint trigger weekly_contact_query_claim_cleanup_v1 after insert or update on public.hr_issue_email_deliveries
 deferrable initially deferred for each row execute function private.weekly_contact_query_claim_guard_v1();


-- 085_native_integrity_triggers.sqlinc
-- ONE-TIME protected migration only. Routine080 is independently repeatable.
create trigger weekly_contact_query_delivery_integrity_v1 before insert or update on public.hr_issue_email_deliveries
 for each row execute function private.weekly_contact_query_integrity_v1();
create trigger weekly_contact_query_items_integrity_v1 before update or delete on public.hr_issue_email_delivery_items
 for each row execute function private.weekly_contact_query_integrity_v1();
create trigger weekly_contact_query_sent_integrity_v1 before update of status,sent_at,provider_status,provider_message_id on public.mail_outbox
 for each row execute function private.weekly_contact_query_integrity_v1();

commit;
