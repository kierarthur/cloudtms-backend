-- Unique trial marker is always rolled back and independently absent on a fresh connection.
create table public.healthroster_query_schema_trial_16102026(marker text primary key);
insert into public.healthroster_query_schema_trial_16102026 values('ROLLBACK_ONLY');
-- Schema/current read constructor only. No enqueue, claim, provider or result.
do $fixture$
declare name text;reply jsonb;signature text;
begin
 if current_database()<>'cloudtms_test_clone' then raise exception 'QUERY_SCHEMA_WRONG_DATABASE';end if;
 foreach name in array array['original_delivery_id','payload_revision','next_payload_revision','contact_managed',
  'legacy_claim_lease_token','legacy_claim_leased_at_utc','legacy_claim_expires_at_utc','contact_claim_backend_pid',
  'contact_claim_transaction_id','contact_claim_token','contact_claim_at_utc','contact_claim_expires_at_utc',
  'retired_at_utc','retirement_reason','contact_rechecked_at_utc'] loop
  if not exists(select 1 from pg_attribute where attrelid='public.hr_issue_email_deliveries'::regclass and attname=name and attnum>0 and not attisdropped) then raise exception 'QUERY_SCHEMA_COLUMN_MISSING: %',name;end if;
 end loop;
 if exists(select 1 from public.hr_issue_email_deliveries where original_delivery_id is distinct from id or payload_revision<>1 or next_payload_revision<>2) then raise exception 'QUERY_SCHEMA_ORIGINAL_HISTORY_REWRITTEN';end if;
 if exists(select 1 from public.hr_issue_email_deliveries d join public.mail_outbox o on o.id=d.mail_outbox_id
  where d.legacy_claim_lease_token is not null and(d.legacy_claim_lease_token is distinct from o.attempt_lease_token
   or d.legacy_claim_leased_at_utc is distinct from o.attempt_leased_at_utc or d.legacy_claim_expires_at_utc is distinct from o.attempt_lease_expires_at_utc)) then raise exception 'QUERY_SCHEMA_LEGACY_SNAPSHOT_NOT_ACTUAL';end if;
 if not exists(select 1 from pg_trigger where tgrelid='public.hr_issue_email_deliveries'::regclass and tgname='weekly_contact_query_claim_cleanup_v1' and tgdeferrable and tginitdeferred and tgenabled='O') then raise exception 'QUERY_SCHEMA_DEFERRED_CLEANUP_MISSING';end if;
 foreach name in array array['weekly_contact_query_claim_guard_v1','weekly_contact_query_delivery_integrity_v1','weekly_contact_query_items_integrity_v1','weekly_contact_query_sent_integrity_v1'] loop
  if(select count(*) from pg_trigger where tgname=name and not tgisinternal and tgenabled='O')<>1 then raise exception 'QUERY_SCHEMA_TRIGGER_MISSING_OR_DUPLICATE: %',name;end if;
 end loop;
 foreach signature in array array['public.email_outbox_claim_ready_batch(integer,text,integer)',
  'public.email_outbox_claim_ready_batch_contact_v1(jsonb)','public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text)'] loop
  if not has_function_privilege('service_role',signature,'EXECUTE') or has_function_privilege('anon',signature,'EXECUTE') or has_function_privilege('authenticated',signature,'EXECUTE')
   or(select prosecdef from pg_proc where oid=signature::regprocedure) then raise exception 'QUERY_SCHEMA_INVOKER_ACL_CHANGED: %',signature;end if;
 end loop;
 foreach signature in array array['private.weekly_contact_query_integrity_v1()','private.weekly_contact_query_claim_guard_v1()',
  'private.weekly_contact_query_legacy_claims_v1(uuid,uuid,uuid,text)','private.weekly_contact_pause_inflight_v1(uuid,text,uuid,uuid)',
  'private.weekly_contact_query_saved_payload_v1(uuid,uuid)','private.weekly_contact_query_member_v1(uuid,uuid)'] loop
  if has_function_privilege('service_role',signature,'EXECUTE') or has_function_privilege('anon',signature,'EXECUTE') or has_function_privilege('authenticated',signature,'EXECUTE') then raise exception 'QUERY_SCHEMA_PRIVATE_HELPER_EXPOSED: %',signature;end if;
 end loop;
 perform set_config('request.jwt.claim.role','service_role',true);
 reply:=public.weekly_source_contact_pause_get_v1(jsonb_build_object('candidate_id','f7160000-0000-4000-8000-000000000010',
  'client_id','f7160000-0000-4000-8000-000000000011','actor_user_id','f7160000-0000-4000-8000-000000000001',
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST'));
 if reply#>>'{scope,revision}' is distinct from '4' or reply#>'{scope,in_flight}' is distinct from '[]'::jsonb then raise exception 'QUERY_SCHEMA_CURRENT_READ_CHANGED';end if;
end;
$fixture$;
set constraints all immediate;
select jsonb_build_object('scope','QUERY_SCHEMA_CURRENT_READ_ONLY','assertions','PASS','database',current_database(),
 'enqueue_claim_provider_result_invoked',false,'genuine_query_business_and_concurrency','NOT_RUN');
