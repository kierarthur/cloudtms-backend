-- F5 actual OFF_RULE setup, post-VERIFIED mandatory pre-publication rollback journey.
-- SOURCE_ONLY / NATIVE_NOT_RUN; genuine registered authorisation required, features remain OFF.
-- No pay/finalisation/invoice/provider/comms owner; no fabricated READY/signature/release.
-- Fresh synthetic Source file comes from the actual past open cycle/window; all native clocks remain genuine.
\set ON_ERROR_STOP on
begin;
set local lock_timeout='5s';
set local statement_timeout='60s';
select set_config('request.jwt.claim.role','service_role',true);
create function pg_temp.f5_assert(p_ok boolean,p_name text) returns void language plpgsql as $f$
begin if p_ok is not true then raise exception 'F5_VERIFY_FAILED: %',p_name; end if; end; $f$;
create function pg_temp.f5_refuses(p_sql text,p_code text) returns void language plpgsql as $f$
begin begin execute p_sql; exception when others then if sqlerrm=p_code then return; end if;
raise exception 'F5_WRONG_REFUSAL: expected %, received %',p_code,sqlerrm; end;
raise exception 'F5_REFUSAL_MISSING: %',p_code; end; $f$;
create temporary table f5_generated_input(value jsonb not null);
-- F5_GENERATED_DATA_INSERT_REQUIRED
select pg_temp.f5_assert((select count(*)=1 from f5_generated_input),'actual parser-generated synthetic file required');
select pg_temp.f5_assert(((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)>(clock_timestamp() at time zone 'Europe/London')::date,'future fixture date');
select pg_temp.f5_assert(not exists(select 1 from public.tms_users where id::text like 'f5510000-%' or email like 'f5-%@example.invalid')
 and not exists(select 1 from public.clients where id::text like 'f5510000-%' or name='F5 fixture self bill')
 and not exists(select 1 from public.client_settings where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_groups where id::text like 'f5510000-%' or code='F5_VERIFY_MONEY')
 and not exists(select 1 from public.weekly_source_group_clients where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_client_policies where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_cycles where id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_uploads where original_filename='f5-verify-money.xlsx')
 and not exists(select 1 from public.weekly_source_rule_sets where code='F5_VERIFY_RULES' or created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_rule_set_versions where created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_group_rule_assignments where created_by_user_id::text like 'f5510000-%')
 and not exists(select 1 from public.weekly_source_invoice_hold_releases where actor_user_id::text like 'f5510000-%'),'exact fixture vacancy');
insert into public.tms_users(id,email,role,password_hash,display_name,is_active,payment_authoriser,payment_golden_key) values
 ('f5510000-0000-4000-8000-000000000001','f5-admin@example.invalid','admin','not-a-real-password','F5 admin',true,false,false),
 ('f5510000-0000-4000-8000-000000000002','f5-authoriser@example.invalid','user','not-a-real-password','F5 authoriser',true,true,false);
insert into public.clients(id,name,ts_queries_email,vat_chargeable) values ('f5510000-0000-4000-8000-000000000010','F5 fixture self bill','f5-manager@example.invalid',true);
insert into public.client_settings(id,client_id,effective_from,hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,no_timesheet_required,requires_hr,is_nhsp)
 values ('f5510000-0000-4000-8000-000000000020','f5510000-0000-4000-8000-000000000010',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)-60,true,true,true,true,true,false);
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
 values ('f5510000-0000-4000-8000-000000000050','TEST','f5510000-0000-4000-8000-000000000099','F5_VERIFY_MONEY','F5 money','ROSTER',3,'23:59');
insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,created_by_user_id)
 values ('f5510000-0000-4000-8000-000000000060','f5510000-0000-4000-8000-000000000050','f5510000-0000-4000-8000-000000000010',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)-60,'f5510000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(id,source_group_id,client_id,effective_from,authority_mode,document_mode,self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,created_by_user_id)
 values ('f5510000-0000-4000-8000-000000000070','f5510000-0000-4000-8000-000000000050','f5510000-0000-4000-8000-000000000010',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)-60,'SOURCE_AUTHORITY','CHECK_ONLY',true,false,false,'f5510000-0000-4000-8000-000000000001');
insert into public.weekly_source_cycles(id,source_group_id,scope_client_id,finalisation_week_ending,cutoff_at_utc)
 values ('f5510000-0000-4000-8000-000000000080','f5510000-0000-4000-8000-000000000050','f5510000-0000-4000-8000-000000000010',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7),((((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7)+3)+time '23:59') at time zone 'Europe/London');
-- Temporary test adapter. The actual public OFF_RULE owner revalidates the
-- registered immutable authorization, current release, actor, scope and parent.
create function pg_temp.f5_native(p_auth jsonb,p_operation text,p_request jsonb)
returns jsonb language plpgsql as $function$
begin
 if p_operation not in ('weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1',
  'weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1') then raise exception 'F5_NATIVE_OPERATION_REFUSED';end if;
 return public.weekly_source_group_rule_feature_set_atomic_v1(jsonb_build_object(
  'actor_user_id',p_auth->>'journey_actor_user_id','agency_id',p_auth->>'agency_id','environment','TEST',
  'qualification_action','OFF_RULE_COMMAND','source_cycle_id',p_auth->>'source_cycle_id',
  'native_operation',p_operation,'native_request',p_request));
end;
$function$;

-- Replace only fixture identities in the exact existing bounded source input.
-- This adapts fixture metadata; it does not create authorization or source money.
create function pg_temp.f5_native_request(p_request jsonb,p_auth jsonb)
returns jsonb language plpgsql as $function$
declare body text:=p_request::text;
begin
 body:=replace(body,'f5510000-0000-4000-8000-000000000001',p_auth->>'journey_actor_user_id');
 body:=replace(body,'f5510000-0000-4000-8000-000000000099',p_auth->>'agency_id');
 body:=replace(body,'f5510000-0000-4000-8000-000000000050',p_auth->>'source_group_id');
 body:=replace(body,'f5510000-0000-4000-8000-000000000080',p_auth->>'source_cycle_id');
 body:=replace(body,'f5510000-0000-4000-8000-000000000010',p_auth#>>'{fixture_manifest,client_ids,0}');
 p_request:=body::jsonb;
 if p_request ? 'coverage_confirmed_at_utc' then
  p_request:=jsonb_set(p_request,'{coverage_confirmed_at_utc}',to_jsonb(to_char(clock_timestamp() at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
 end if;
 return p_request;
end;
$function$;

-- Same exact native tables/columns as the retained query-prefix economics
-- witness. No TS/TF/invoice/document is constructed by this F5 journey.
create function pg_temp.f5_economics(p_timesheet_ids uuid[])
returns jsonb language sql stable as $function$
 select jsonb_build_object(
  'timesheets',(select jsonb_agg(to_jsonb(t) order by t.timesheet_id) from public.timesheets t where t.timesheet_id=any(p_timesheet_ids)),
  'workflows',(select jsonb_agg(to_jsonb(w) order by w.id) from public.candidate_submission_workflows w where w.id in(select candidate_workflow_id from public.timesheets where timesheet_id=any(p_timesheet_ids))),
  'approval_requests',(select jsonb_agg(to_jsonb(a) order by a.id) from public.candidate_approval_requests a where a.workflow_id in(select candidate_workflow_id from public.timesheets where timesheet_id=any(p_timesheet_ids))),
  'components',(select jsonb_agg(to_jsonb(c) order by c.id) from public.candidate_submission_components c where c.workflow_id in(select candidate_workflow_id from public.timesheets where timesheet_id=any(p_timesheet_ids))),
  'timesheet_evidence',(select jsonb_agg(to_jsonb(e) order by e.id) from public.timesheet_evidence e where e.timesheet_id=any(p_timesheet_ids)),
  'document_assets',(select jsonb_agg(to_jsonb(a) order by a.id) from public.invoice_document_assets a where a.id in(select manual_document_asset_id from public.timesheets where timesheet_id=any(p_timesheet_ids))
   or a.id in(select document_asset_id from public.timesheet_evidence where timesheet_id=any(p_timesheet_ids))),
  'financials',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f where f.timesheet_id=any(p_timesheet_ids)),
  'invoices',(select jsonb_agg(to_jsonb(i) order by i.id) from public.invoices i where i.id in(select invoice_id from public.invoice_lines where timesheet_id=any(p_timesheet_ids))),
  'invoice_lines',(select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l where l.invoice_id in(select invoice_id from public.invoice_lines where timesheet_id=any(p_timesheet_ids))),
  'document_versions',(select jsonb_agg(to_jsonb(d) order by d.id) from public.invoice_document_versions d where
   (d.entity_type='TIMESHEET' and d.entity_id=any(p_timesheet_ids)) or(d.entity_type='INVOICE' and d.entity_id in(select invoice_id from public.invoice_lines where timesheet_id=any(p_timesheet_ids)))));
$function$;

create temporary table f5_owner_proof(name text primary key,value jsonb);
do $verify$
declare base jsonb:='{"actor_user_id":"f5510000-0000-4000-8000-000000000001","agency_id":"f5510000-0000-4000-8000-000000000099","environment":"TEST","source_group_id":"f5510000-0000-4000-8000-000000000050"}'::jsonb;
 saved jsonb; assigned jsonb; enabled jsonb; begun jsonb; staged jsonb; sealed jsonb; released jsonb; request jsonb;
 row_id uuid; row_hash text; cycle_version bigint; latest private.cloudtms_database_releases%rowtype; sig text; role_name text; auth jsonb; n integer; actual_client uuid; protected_ids uuid[]; before_economics jsonb;
begin
 saved:=public.weekly_source_rule_set_version_save_atomic_v1(base||jsonb_build_object('request_id','f5510000-0000-4000-8000-000000000100',
   'rule_set',jsonb_build_object('id',null,'code','F5_VERIFY_RULES','display_name','F5 source money','expected_version',0),'values','{"checking_profile_kind":"HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1","invoice_sender":"CLIENT_SELF_BILL","invoice_amount_basis":"SOURCE_AMOUNT","source_amount_basis":"EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","line_vat_method":"LINE_GROSS_MINUS_NET_FROM_SOURCE","rate_difference_policy":"FLAG_ALLOW_IGNORE"}'::jsonb));
 assigned:=public.weekly_source_group_rule_assign_atomic_v1(base||jsonb_build_object('request_id','f5510000-0000-4000-8000-000000000110',
   'rule_set_version_id',saved->>'rule_set_version_id','effective_from_week_ending',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7),'expected_group_version',1));
 perform pg_temp.f5_assert(private.weekly_source_effective_rule_set_v1('f5510000-0000-4000-8000-000000000010','f5510000-0000-4000-8000-000000000080') is null,'saved assignment remains OFF');
  select * into strict latest from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1;
  perform pg_temp.f5_assert(latest.status='VERIFIED' and latest.completed_at_utc is not null,'actual latest release VERIFIED');
  select count(*),jsonb_agg(value)->0 into n,auth
   from jsonb_array_elements(latest.evidence_json->'weekly_source_protected_test_setup_authorizations') value
   where value->>'schema_version'='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1'
    and value->>'purpose'='SOURCE_FIXTURE_SETUP'
    and value->>'source_cycle_id'=(select value#>>'{proof,source_cycle_id}' from f5_generated_input)
    and value->>'week_ending'=(select value#>>'{proof,week_ending}' from f5_generated_input)
    and value->'allowed_operations' @> '["weekly_source_upload_stage_begin_atomic_v1","weekly_source_upload_stage_rows_atomic_v1","weekly_source_upload_seal_atomic_v1","weekly_source_invoice_hold_release_atomic_v1"]'::jsonb;
  perform pg_temp.f5_assert(n=1,'one genuinely registered current F5 OFF_RULE authorization; never manufacture one');
  auth:=private.weekly_source_rule_qualification_authorization_v1((auth->>'source_cycle_id')::uuid,(auth->>'journey_actor_user_id')::uuid,'SOURCE_FIXTURE_SETUP');
  perform private.weekly_source_rule_qualification_scope_assert_v1(auth,(auth->>'journey_actor_user_id')::uuid,true);
  perform pg_temp.f5_assert(exists(select 1 from public.weekly_source_cycles c cross join f5_generated_input g
   where c.id=(auth->>'source_cycle_id')::uuid and c.source_group_id=(auth->>'source_group_id')::uuid
    and c.finalisation_week_ending::text=g.value#>>'{proof,week_ending}'
    and c.cutoff_at_utc=(g.value#>>'{proof,window_end_at_utc}')::timestamptz
    and (g.value#>>'{proof,finalised_at_utc}')::timestamptz<=c.cutoff_at_utc
    and (g.value#>>'{proof,finalised_at_utc}')::timestamptz>coalesce((select max(prior.cutoff_at_utc) from public.weekly_source_cycles prior
      where prior.source_group_id=c.source_group_id and prior.cutoff_at_utc<c.cutoff_at_utc),
      ((c.cutoff_at_utc at time zone 'Europe/London')-interval '7 days') at time zone 'Europe/London')
    and c.cutoff_at_utc<clock_timestamp() and c.current_complete_upload_id is null and c.current_projection_publication_id is null
    and c.current_final_revision_id is null and c.state not in('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')),'actual past open native cycle and exact generated source window');
  perform pg_temp.f5_assert(jsonb_array_length(auth#>'{fixture_manifest,client_ids}')=1,'one actual authorized Client');
  actual_client:=(auth#>>'{fixture_manifest,client_ids,0}')::uuid;
  perform pg_temp.f5_assert(private.weekly_source_effective_rule_set_v1(actual_client,(auth->>'source_cycle_id')::uuid) is null,'actual fixture remains operationally OFF');
  perform pg_temp.f5_assert((select values_json->>'invoice_amount_basis'='SOURCE_AMOUNT'
    and values_json->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    from public.weekly_source_rule_set_versions where id=(auth->>'rule_set_version_id')::uuid),'genuine existing Source-money rule values');
  select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),'{}'::uuid[]) into protected_ids from public.timesheets t
   where t.contract_id in(select value::uuid from jsonb_array_elements_text(auth#>'{fixture_manifest,contract_ids}') value);
  before_economics:=pg_temp.f5_economics(protected_ids);
  enabled:=public.weekly_source_group_rule_feature_set_atomic_v1(jsonb_build_object(
   'actor_user_id',auth->>'setup_actor_user_id','agency_id',auth->>'agency_id','environment','TEST',
   'qualification_action','OFF_RULE_OPEN','source_cycle_id',auth->>'source_cycle_id',
   'journey_actor_user_id',auth->>'journey_actor_user_id','purpose','SOURCE_FIXTURE_SETUP','reason','F5 mandatory pre-publication rollback positive journey'));

  begun:=pg_temp.f5_native(auth,'weekly_source_upload_stage_begin_atomic_v1',pg_temp.f5_native_request((select value->'begin_request' from f5_generated_input),auth));
 perform pg_temp.f5_assert(begun->>'status'='STAGING','actual stage begin');
  staged:=pg_temp.f5_native(auth,'weekly_source_upload_stage_rows_atomic_v1',pg_temp.f5_native_request((select value->'stage_request' from f5_generated_input)||jsonb_build_object('actor_user_id',auth->>'journey_actor_user_id','upload_id',begun->>'logical_upload_id'),auth));
 perform pg_temp.f5_assert(staged->>'status'='STAGING','actual money evidence stage');
  sealed:=pg_temp.f5_native(auth,'weekly_source_upload_seal_atomic_v1',pg_temp.f5_native_request(jsonb_build_object('actor_user_id','f5510000-0000-4000-8000-000000000001','upload_id',begun->>'logical_upload_id'),auth));
 perform pg_temp.f5_assert(sealed->>'status'='CURRENT','actual immutable seal: '||sealed::text);
 select r.id,encode(r.normalised_row_hash,'hex') into strict row_id,row_hash from public.weekly_source_upload_rows r where r.upload_id=(begun->>'logical_upload_id')::uuid;
 select version into strict cycle_version from public.weekly_source_cycles where id=(auth->>'source_cycle_id')::uuid;
 perform pg_temp.f5_assert((select source_shift_charge_pence=10000 and source_vat_pence=2002 and source_gross_pence=12002 and finalised_at_utc=(select (value#>>'{proof,finalised_at_utc}')::timestamptz from f5_generated_input) from public.weekly_source_upload_rows where id=row_id),'native stored literal money and real timestamp');
 request:=jsonb_build_object('actor_user_id',auth->>'journey_actor_user_id','source_cycle_id',auth->>'source_cycle_id','upload_row_id',row_id,
   'expected_source_row_hash',row_hash,'expected_authority_scope_version',cycle_version,'hold_kind','VAT_FIT');
 perform pg_temp.f5_refuses(format('select public.weekly_source_invoice_hold_release_atomic_v1(%L::jsonb)',request||jsonb_build_object('actor_user_id','f5510000-0000-4000-8000-000000000002')),'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED');
 perform pg_temp.f5_refuses(format('select public.weekly_source_invoice_hold_release_atomic_v1(%L::jsonb)',request||jsonb_build_object('expected_authority_scope_version',cycle_version+1)),'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE');
  released:=pg_temp.f5_native(auth,'weekly_source_invoice_hold_release_atomic_v1',pg_temp.f5_native_request(request,auth));
 perform pg_temp.f5_assert(released->>'action'='BILL_AT_NET_PLUS_20' and released->>'net_pence'='10000' and released->>'vat_pence'='2000' and released->>'gross_pence'='12000','native bounded default hold release');
 perform pg_temp.f5_assert((pg_temp.f5_native(auth,'weekly_source_invoice_hold_release_atomic_v1',request)->>'idempotent')::boolean,'exact hold replay');
 perform pg_temp.f5_refuses(format('select pg_temp.f5_native(%L::jsonb,%L,%L::jsonb)',auth,'weekly_source_invoice_hold_release_atomic_v1',request||jsonb_build_object('action','BILL_AS_FILE')),'WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT');
 perform pg_temp.f5_refuses(format('update public.weekly_source_invoice_hold_releases set action=action where id=%L::uuid',released->>'release_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
 perform pg_temp.f5_refuses(format('delete from public.weekly_source_invoice_hold_releases where id=%L::uuid',released->>'release_id'),'WEEKLY_SOURCE_IMMUTABLE_RECORD');
 perform pg_temp.f5_assert((select count(*)=2 from public.weekly_source_money_cell_evidence where upload_id=(begun->>'logical_upload_id')::uuid),'two exact source evidence cells; no fee');
 perform pg_temp.f5_assert(not exists(select 1 from public.weekly_source_final_revisions where source_cycle_id='f5510000-0000-4000-8000-000000000080') and not exists(select 1 from public.weekly_source_billing_movements where actual_client_id='f5510000-0000-4000-8000-000000000010'),'no fabricated financial producer');
  perform pg_temp.f5_assert(before_economics=pg_temp.f5_economics(protected_ids),'exact original TS/TF/workflow/approval/component/invoice/document equality');
  perform pg_temp.f5_assert(protected_ids=(select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),'{}'::uuid[]) from public.timesheets t
   where t.contract_id in(select value::uuid from jsonb_array_elements_text(auth#>'{fixture_manifest,contract_ids}') value)),'no new native Timesheet');
  perform pg_temp.f5_assert(not exists(select 1 from public.weekly_source_final_revisions where source_cycle_id=(auth->>'source_cycle_id')::uuid)
   and not exists(select 1 from public.weekly_source_billing_movements where actual_client_id=actual_client),'no Source final/financial publication');
  perform pg_temp.f5_assert(not exists(select 1 from public.weekly_source_group_rule_assignments where source_group_id=(auth->>'source_group_id')::uuid and feature_enabled)
   and private.weekly_source_effective_rule_set_v1(actual_client,(auth->>'source_cycle_id')::uuid) is null,'features and ordinary effective rules remain OFF after native command');
  perform public.weekly_source_group_rule_feature_set_atomic_v1(jsonb_build_object(
   'actor_user_id',auth->>'setup_actor_user_id','agency_id',auth->>'agency_id','environment','TEST',
   'qualification_action','OFF_RULE_CLOSE','source_cycle_id',auth->>'source_cycle_id',
   'journey_actor_user_id',auth->>'journey_actor_user_id','purpose','SOURCE_FIXTURE_SETUP','reason','F5 rollback journey complete; no operational activation'));
  insert into f5_owner_proof values ('hold_release',released),('native_seal',sealed),('nonoperational_off_rule_scope',enabled);

end;
$verify$;
select * from f5_owner_proof order by name;
rollback;
