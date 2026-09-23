-- PostgreSQL 17 rollback verification: weekly_source_candidate_view_producer_v1
--
-- Gate 9 items G9-6 (the MyTMS payload producer) and G9-4 (the Office
-- `action_state` Unauthorise verdict).
--
-- Proves:
--   * the produced payload is exactly the MyTMS contract's
--     `CandidateWeeklySourceView` (nine required members, no others);
--   * `UI-019`, `UI-020` and `UI-021` payloads from database states that
--     genuinely produce them, including `NAI-MYT-001`'s rule that no approved
--     hours exist before Office authorisation;
--   * `NAI-MYT-002`'s expense modes;
--   * the payload carries no money and no internal vocabulary;
--   * an ordinary, non-Weekly-Source Timesheet detail is byte-identical before
--     and after the single additive call;
--   * `action_state` carries the withdrawal owner's own verdict, and reports an
--     explicit unavailable verdict when that owner is not installed.
--
-- Every fixture row is rolled back.

\set ON_ERROR_STOP on
\pset pager off

begin;

select pg_catalog.set_config('request.jwt.claim.role','service_role',true);

create or replace function pg_temp.assert_true(p_ok boolean,p_message text)
returns void language plpgsql as $verify$
begin
  if p_ok is not true then raise exception 'VERIFY_FAILED: %',p_message; end if;
end;
$verify$;

create or replace function pg_temp.assert_eq(p_left text,p_right text,p_message text)
returns void language plpgsql as $verify$
begin
  if p_left is distinct from p_right then
    raise exception 'VERIFY_FAILED: % (got %, expected %)',
      p_message,coalesce(p_left,'<null>'),coalesce(p_right,'<null>');
  end if;
end;
$verify$;

-- ---------------------------------------------------------------------------
-- 1. Structure, privileges and volatility.
-- ---------------------------------------------------------------------------
do $structure$
declare
  v_proc record;
begin
  for v_proc in
    select unnest(array[
      'private.weekly_source_candidate_week_context_v1(uuid)',
      'private.weekly_source_candidate_view_request_v1(jsonb)',
      'private.weekly_source_candidate_approved_hours_v1(jsonb)',
      'private.weekly_source_candidate_hours_shape_v1(jsonb)',
      'private.weekly_source_candidate_hours_differ_v1(jsonb,jsonb)',
      'private.weekly_source_candidate_view_v1(uuid,timestamptz)',
      'private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)',
      'private.weekly_source_office_authorisation_state_v1(uuid)',
      'private.weekly_source_office_unauthorise_action_state_v1(uuid)'
    ]::text[]) as ident
  loop
    perform pg_temp.assert_true(to_regprocedure(v_proc.ident) is not null,
      'function missing: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select p.provolatile from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        in ('i','s'),
      'producer must not be VOLATILE: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select r.rolname from pg_proc p join pg_roles r on r.oid=p.proowner
       where p.oid=to_regprocedure(v_proc.ident))='postgres',
      'wrong owner: '||v_proc.ident);
    perform pg_temp.assert_true(
      not exists (
        select 1 from pg_proc p,
          aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        left join pg_roles grantee on grantee.oid=acl.grantee
        where p.oid=to_regprocedure(v_proc.ident)
          and (acl.grantee=0
            or grantee.rolname in ('anon','authenticated','service_role'))),
      'private producer is executable by PUBLIC or a browser/service role: '||v_proc.ident);
  end loop;
end;
$structure$;

-- ---------------------------------------------------------------------------
-- 2. The producer chain reads no money and takes no lock.
--    (The Office bridge is checked separately: it is allowed to call the
--    withdrawal availability owner, which reads Banking Pay evidence itself.)
-- ---------------------------------------------------------------------------
do $prohibitions$
declare
  v_body text;
  v_term text;
begin
  v_body:=lower(
    pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_v1(uuid,timestamptz)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_week_context_v1(uuid)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_approved_hours_v1(jsonb)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_request_v1(jsonb)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_candidate_view_merge_v1(uuid,timestamptz)')));

  foreach v_term in array array[
    'pay_batch','pay_bank_transfer','timesheet_pay_state','timesheets_financials',
    'invoice','remittance','recovery','pay_advance','umbrella',
    'amount_ex_vat','amount_inc_vat','rates_json','pay_rate','charge_rate',
    'source_total_cost_pence','source_shift_charge_pence','source_commission_pence',
    'source_expense_pence',
    'for update','for share','pg_advisory',
    'insert into','update public.','delete from',
    'pg_catalog.coalesce(','pg_catalog.nullif(',
    'pg_catalog.least(','pg_catalog.greatest('
  ]::text[]
  loop
    perform pg_temp.assert_true(pg_catalog.strpos(v_body,v_term)=0,
      'MyTMS producer must not mention: '||v_term);
  end loop;
end;
$prohibitions$;

-- ---------------------------------------------------------------------------
-- 3. A real Weekly Source world.
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do nothing;

insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('c1000000-0000-4000-8000-000000000001','mytms-producer@example.invalid',
  'admin','not-a-real-password','MyTMS producer verifier',true);

insert into public.clients(id,name,ts_queries_email)
values
  ('c2000000-0000-4000-8000-000000000001','Producer Source Trust','manager@example.invalid'),
  ('c2000000-0000-4000-8000-000000000002','Producer Ordinary Trust','manager@example.invalid');
insert into public.client_settings(
  id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,
  no_timesheet_required,requires_hr
) values
  ('c2100000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   '2026-01-01','ELECTRONIC',0,true,false,false,false,true),
  ('c2100000-0000-4000-8000-000000000002','c2000000-0000-4000-8000-000000000002',
   '2026-01-01','ELECTRONIC',0,true,false,false,false,true);

insert into public.candidates(
  id,tms_ref,first_name,last_name,display_name,email,active,key_norm,opt_in_email
) values (
  'c3000000-0000-4000-8000-000000000001','MYT-90001','Jo','Nurse','Jo Nurse',
  'jo.producer@example.invalid',true,'JO-PRODUCER',true);

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  week_ending_weekday_snapshot,default_submission_mode,role
) values
  ('c4000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}',
   0,'ELECTRONIC','RMN'),
  ('c4000000-0000-4000-8000-000000000002','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}',
   0,'ELECTRONIC','RMN');

insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time
) values (
  'c5000000-0000-4000-8000-000000000001','TEST','c0000000-0000-4000-8000-000000000001',
  'MYTMS_PRODUCER_VERIFY','MyTMS producer verification Roster','ROSTER',3,'15:00');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  'c5000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  '2026-01-01','c1000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'c5000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  '2026-01-01','SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,
  'manager@example.invalid','c1000000-0000-4000-8000-000000000001');

insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'c6000000-0000-4000-8000-000000000001','c5000000-0000-4000-8000-000000000001',
  '2026-09-06','2026-09-09 15:00:00+00','OPEN',1,'NONE');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'c7000000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'mytms-producer-verify.xlsx',decode(repeat('71',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('72',32),'hex'),decode(repeat('73',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('74',32),'hex'),
  'CURRENT','c1000000-0000-4000-8000-000000000001');
update public.weekly_source_cycles
set current_complete_upload_id='c7000000-0000-4000-8000-000000000001'
where id='c6000000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'c8000000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'CYCLE','c7000000-0000-4000-8000-000000000001',1,
  decode(repeat('75',32),'hex'),decode(repeat('76',32),'hex'),'CURRENT',
  '2026-09-01 08:00:00+00');
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='c8000000-0000-4000-8000-000000000001'
where id='c6000000-0000-4000-8000-000000000001';

insert into public.weekly_work_events(
  id,candidate_id,client_id,work_date,identity_kind,profile_external_key,
  durable_identity_hash,first_source_group_id,source_format_profile_id
) values
  ('c9000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-09-01','PROFILE_EXTERNAL_KEY',
   'producer-shift-1',decode(repeat('81',32),'hex'),
   'c5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444'),
  ('c9000000-0000-4000-8000-000000000002','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','2026-09-02','PROFILE_EXTERNAL_KEY',
   'producer-shift-2',decode(repeat('82',32),'hex'),
   'c5000000-0000-4000-8000-000000000001','34444444-4444-4444-8444-444444444444');

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('ca000000-0000-4000-8000-000000000001','c7000000-0000-4000-8000-000000000001',
   1,'producer-shift-1','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
   '2026-09-01 09:00:00','2026-09-01 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('91',32),'hex')),
  ('ca000000-0000-4000-8000-000000000002','c7000000-0000-4000-8000-000000000001',
   2,'producer-shift-2','JO NURSE','PRODUCER SOURCE TRUST','2026-09-02',
   '2026-09-02 09:00:00','2026-09-02 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('92',32),'hex'));

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('cb000000-0000-4000-8000-000000000001','ca000000-0000-4000-8000-000000000001',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b1',32),'hex'),
   decode(repeat('a1',32),'hex'),decode(repeat('a2',32),'hex'),
   decode(repeat('a3',32),'hex')),
  ('cb000000-0000-4000-8000-000000000002','ca000000-0000-4000-8000-000000000002',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000002','AUTO_UNIQUE','NEW_PROFILE_KEY',
   decode(repeat('b2',32),'hex'),
   decode(repeat('a4',32),'hex'),decode(repeat('a5',32),'hex'),
   decode(repeat('a6',32),'hex'));

-- The Weekly Source root: the Candidate submitted exactly the source hours.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,
  actual_schedule_json,additional_units_week,additional_units_per_day
) values (
  'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE','jo-nurse',
  'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
  '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/jo.png',repeat('a',64),
  'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30},
    {"row_key":"row-2","date":"2026-09-02","start":"09:00","end":"17:00","break_minutes":60}]'::jsonb,
  '{}'::jsonb,'{}'::jsonb);

-- The ordinary, non-Weekly-Source root used for the differential.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,
  actual_schedule_json,additional_units_week,additional_units_per_day
) values (
  'cc000000-0000-4000-8000-000000000002','MYTMS-PRODUCER-ORDINARY','jo-nurse',
  'producer-ordinary-trust','ward-b','rmn','2026-09-01 08:00:00+00',
  '2026-09-01 17:00:00+00',30,510,'2026-09-06','verify/jo2.png',repeat('b',64),
  'c4000000-0000-4000-8000-000000000002','WEEKLY','HOURS',
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb,
  '{}'::jsonb,'{}'::jsonb);

insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id,
  day_entries_json,totals_json
) values
  ('cd000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   '2026-09-06','SUBMITTED','ELECTRONIC','cc000000-0000-4000-8000-000000000001',
   '[]'::jsonb,'{}'::jsonb),
  ('cd000000-0000-4000-8000-000000000002','c4000000-0000-4000-8000-000000000002',
   '2026-09-06','SUBMITTED','ELECTRONIC','cc000000-0000-4000-8000-000000000002',
   '[]'::jsonb,'{}'::jsonb);

-- The lineage rows are what bind the publication to this Timesheet, and are
-- what the Office presentation's publication lookup keys on.
insert into public.weekly_source_row_timesheet_lineages(
  id,row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
  contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
  week_ending_date,lineage_fingerprint
) values
  ('ce000000-0000-4000-8000-000000000001','cb000000-0000-4000-8000-000000000001',
   'c6000000-0000-4000-8000-000000000001','c9000000-0000-4000-8000-000000000001',
   'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   'c4000000-0000-4000-8000-000000000001','cd000000-0000-4000-8000-000000000001',
   'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',1,'2026-09-06',
   decode(repeat('c1',32),'hex')),
  ('ce000000-0000-4000-8000-000000000002','cb000000-0000-4000-8000-000000000002',
   'c6000000-0000-4000-8000-000000000001','c9000000-0000-4000-8000-000000000002',
   'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
   'c4000000-0000-4000-8000-000000000001','cd000000-0000-4000-8000-000000000001',
   'cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',1,'2026-09-06',
   decode(repeat('c2',32),'hex'));

-- ---------------------------------------------------------------------------
-- 3a. WP-11d F10: the committed entitlement head, which is the AUTHORITY for
--     approved hours.  Seeded exactly as the coordinator writes it - bundle,
--     STAGED head, components, then the commit update - so the shapes below are
--     the real relation contents, not a stub.
-- ---------------------------------------------------------------------------
create or replace function pg_temp.seed_committed_head(
  p_head uuid,
  p_bundle uuid,
  p_revision bigint,
  p_prior uuid,
  p_components jsonb,
  p_commit boolean default true,
  -- Declared component_count, overriding the number actually inserted.  The
  -- component rows are immutable once written (`WEEKLY_SOURCE_IMMUTABLE_RECORD`),
  -- so the count-mismatch shape can only be seeded, never produced by deletion.
  p_declared_count integer default null
) returns void
language plpgsql
as $seed_head$
declare
  v_component jsonb;
  v_ordinal integer:=0;
  v_count integer:=jsonb_array_length(coalesce(p_components,'[]'::jsonb));
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    p_bundle,1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001',p_bundle,
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    -- Unique per bundle: the relation enforces a unique request digest.
    sha256(convert_to('request:'||p_bundle::text,'UTF8')),
    sha256(convert_to('source:'||p_bundle::text,'UTF8')),
    sha256(convert_to('choice:'||p_bundle::text,'UTF8')),
    sha256(convert_to('before:'||p_bundle::text,'UTF8')),
    array[p_head]::uuid[],'PROPOSED');

  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id
  ) values (
    p_head,'LOCKED_FINAL_SOURCE','c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
    '2026-09-06','cc000000-0000-4000-8000-000000000001','MYTMS-PRODUCER-SOURCE',
    1,p_revision,p_prior,'STAGED',coalesce(p_declared_count,v_count)=0,
    coalesce(p_declared_count,v_count),
    decode(repeat('21',32),'hex'),decode(repeat('22',32),'hex'),
    decode(repeat('23',32),'hex'),p_bundle,1,p_bundle,
    'c1000000-0000-4000-8000-000000000001');

  for v_component in select value from jsonb_array_elements(coalesce(p_components,'[]'::jsonb))
  loop
    v_ordinal:=v_ordinal+1;
    insert into public.weekly_source_entitlement_head_components(
      id,head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,segment_id,segment_key,
      work_date,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_ex_vat,exclude_from_pay,origin,decision_bundle_id,bundle_revision,
      component_sha256
    ) values (
      gen_random_uuid(),p_head,v_ordinal,
      (v_component->>'component_id')::uuid,
      coalesce(v_component->>'component_kind','WORKED_TIME'),'SEGMENT',
      'seg-'||v_ordinal::text,
      v_component->>'member_identity','seg-'||v_ordinal::text,
      v_component->>'member_identity',
      (v_component->>'work_date')::date,
      (v_component->>'hours_day')::numeric,
      coalesce((v_component->>'hours_night')::numeric,0),
      coalesce((v_component->>'hours_sat')::numeric,0),
      coalesce((v_component->>'hours_sun')::numeric,0),
      coalesce((v_component->>'hours_bh')::numeric,0),
      -- A money column the producer must never read.  It is seeded non-zero on
      -- purpose so that reading it would show up in the payload scan.
      123.45,coalesce((v_component->>'exclude_from_pay')::boolean,false),
      'WEEKLY_SOURCE',p_bundle,1,
      sha256(convert_to(p_head::text||v_ordinal::text,'UTF8')));
  end loop;

  if p_commit then
    perform pg_temp.commit_head(p_head);
  end if;
end;
$seed_head$;

create or replace function pg_temp.commit_head(p_head uuid)
returns void language plpgsql as $commit_head$
begin
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',
         committed_at_utc=pg_catalog.transaction_timestamp(),
         publication_receipt_digest=decode(repeat('41',32),'hex'),
         scope_change_tx_token=gen_random_uuid()
   where id=p_head;
end;
$commit_head$;

-- Supersede a committed head with a staged successor, in the order the
-- coordinator does it: the successor must exist before it can be pointed at.
create or replace function pg_temp.supersede_head(p_old uuid,p_new uuid)
returns void language plpgsql as $supersede_head$
begin
  update public.weekly_source_entitlement_heads
     set state='SUPERSEDED',
         superseded_at_utc=pg_catalog.transaction_timestamp(),
         superseded_by_head_id=p_new
   where id=p_old;
end;
$supersede_head$;

-- The two source shifts, as hour figures: 09:00-19:00 break 30 = 9.5 h, and
-- 09:00-17:00 break 60 = 7 h.  A head that approves exactly the source.
create or replace function pg_temp.head_matching_source()
returns jsonb language sql immutable as $$
  select jsonb_build_array(
    jsonb_build_object('component_id','d1000000-0000-4000-8000-000000000001',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      'work_date','2026-09-01','hours_day',9.5),
    jsonb_build_object('component_id','d1000000-0000-4000-8000-000000000002',
      'member_identity','c9000000-0000-4000-8000-000000000002',
      'work_date','2026-09-02','hours_day',7));
$$;

-- ---------------------------------------------------------------------------
-- 4. NAI-MYT-001 first half: before Office authorisation there are no approved
--    hours, so the Candidate is shown nothing to be paid.
-- ---------------------------------------------------------------------------
do $before_auth$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v is not null,'a Weekly Source week must produce a view');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'NAI-MYT-001: no approved hours before Office authorisation');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false',
    'NAI-MYT-001: nothing differs before Office authorisation');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'2',
    'the Candidate submission is shown untouched');
end;
$before_auth$;

-- ---------------------------------------------------------------------------
-- 4.1 A SUBMIT_TIMESHEET request is addressed by the public outreach
--     generation id used in the notification/deep link, never by the private
--     submission-request row id.  The membership remains the scope id.
-- ---------------------------------------------------------------------------
savepoint before_submit_request_identity;

insert into public.weekly_route_activations(
  id,source_cycle_id,candidate_id,client_id,audience_route,route_mode,
  activated_by_user_id,activated_at_utc
) values (
  'c6100000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  'CANDIDATE','CANDIDATE_FIRST','c1000000-0000-4000-8000-000000000001',
  '2026-09-01 09:00:00+00'
);
insert into public.weekly_candidate_cohorts(
  id,source_cycle_id,candidate_id,client_id,manager_recipient_route_key
) values (
  'c6200000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c3000000-0000-4000-8000-000000000001','c2000000-0000-4000-8000-000000000001',
  decode(repeat('61',32),'hex')
);
insert into public.weekly_candidate_outreach_generations(
  id,source_cycle_id,candidate_cohort_id,candidate_id,client_id,
  generation_number,activation_id,trigger_kind,request_kind,route_mode,
  started_at_utc,reminder_due_at_utc,deadline_at_utc,
  manual_reminder_available_at_utc,state,membership_hash
) values (
  'c6300000-0000-4000-8000-000000000001','c6000000-0000-4000-8000-000000000001',
  'c6200000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'c2000000-0000-4000-8000-000000000001',1,
  'c6100000-0000-4000-8000-000000000001','OFFICE_ASK','SUBMIT_TIMESHEET',
  'CANDIDATE_FIRST','2026-09-01 09:00:00+00','2026-09-01 15:00:00+00',
  '2026-09-01 21:00:00+00','2026-09-01 10:00:00+00','ACTIVE',
  decode(repeat('62',32),'hex')
);
update public.weekly_candidate_cohorts
set current_generation_id='c6300000-0000-4000-8000-000000000001'
where id='c6200000-0000-4000-8000-000000000001';
insert into public.weekly_timesheet_submission_requests(
  id,environment,agency_id,source_cycle_id,candidate_id,candidate_cohort_id,
  request_generation,current_upload_id,current_projection_publication_id,state,
  started_at_utc,reminder_due_at_utc,deadline_at_utc,membership_hash
) values (
  'c6400000-0000-4000-8000-000000000001','TEST','c0000000-0000-4000-8000-000000000001',
  'c6000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'c6200000-0000-4000-8000-000000000001',1,
  'c7000000-0000-4000-8000-000000000001','c8000000-0000-4000-8000-000000000001',
  'ACTIVE','2026-09-01 09:00:00+00','2026-09-01 15:00:00+00',
  '2026-09-01 21:00:00+00',decode(repeat('63',32),'hex')
);
insert into public.weekly_timesheet_submission_request_memberships(
  id,submission_request_id,ordinal,week_ending,client_id,contract_id,
  expected_source_fingerprint,state
) values (
  'c6500000-0000-4000-8000-000000000001','c6400000-0000-4000-8000-000000000001',
  1,'2026-09-06','c2000000-0000-4000-8000-000000000001',
  'c4000000-0000-4000-8000-000000000001',decode(repeat('64',32),'hex'),'WAITING'
);

do $submit_request_identity$
declare
  v jsonb;
  v_contract_week_merge jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'request_kind','SUBMIT_TIMESHEET',
    'the candidate view exposes the submission request kind');
  perform pg_temp.assert_eq(v->>'request_id','c6300000-0000-4000-8000-000000000001',
    'request_id is the public outreach generation used by the deep link');
  perform pg_temp.assert_true(v->>'request_id' is distinct from
    'c6400000-0000-4000-8000-000000000001',
    'the private submission row id must never replace the public request id');
  perform pg_temp.assert_eq(v->>'scope_id','c6500000-0000-4000-8000-000000000001',
    'scope_id is the exact contract-week membership');

  -- The real first-submission shape has no Timesheet yet.  Removing only the
  -- Contract Week's Timesheet binding reproduces that state while retaining
  -- the same active server-owned request and scope identities.
  update public.contract_weeks
  set timesheet_id=null,status='OPEN'
  where id='cd000000-0000-4000-8000-000000000001';
  v_contract_week_merge:=
    private.weekly_source_candidate_contract_week_view_merge_v1(
      'cd000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,request_id}',
    'c6300000-0000-4000-8000-000000000001',
    'a first submission contract-week exposes the public request id');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,scope_id}',
    'c6500000-0000-4000-8000-000000000001',
    'a first submission contract-week exposes the exact membership scope');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,request_kind}',
    'SUBMIT_TIMESHEET',
    'only the server-owned first-submission request unlocks the week');
  perform pg_temp.assert_eq(
    v_contract_week_merge#>>'{weekly_source_candidate_view,expense_entry_mode}',
    'SEPARATE_TIMESHEET',
    'ordinary self-bill candidate expenses remain on a separate Timesheet');
end;
$submit_request_identity$;

rollback to savepoint before_submit_request_identity;

update public.timesheets set authorised_at_server=now()
where timesheet_id='cc000000-0000-4000-8000-000000000001';

-- ---------------------------------------------------------------------------
-- 4a. WP-11d F10.  The approved hours come from the COMMITTED ENTITLEMENT HEAD,
--     never from the source.
--
--     The independent review of WP-14 proved, on real data, that the previous
--     source-derived predicate told a Candidate about two shifts under a
--     CERTIFIED-ZERO head, and told them nothing at all when the approved hours
--     later changed, because the push deduplication key is a digest of this
--     payload and the unchanged source left it unchanged.  Each shape below is
--     executed, not inspected.
-- ---------------------------------------------------------------------------

-- 4a.1  Authorised, source authority, NO committed head: the Candidate is told
--       nothing.  The regression guard for F10 is the pair of assertions that
--       the source rows are NOT emitted even though they exist and resolve.
do $f10_no_head$
declare
  v jsonb;
  v_entitlement jsonb;
  v_source_rows integer;
begin
  select count(*)::integer into v_source_rows
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id
  where source_row.upload_id='c7000000-0000-4000-8000-000000000001'
    and resolution.mapping_state='RESOLVED';
  perform pg_temp.assert_eq(v_source_rows::text,'2',
    'the source rows the old predicate would have emitted are present');

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','NO_APPROVED_ENTITLEMENT',
    'F10 no committed head means nothing is approved');
  perform pg_temp.assert_eq(v_entitlement->>'reason','NO_COMMITTED_HEAD',
    'F10 no committed head reason');

  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'F10 the source rows must NOT be presented as approved hours');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false',
    'F10 nothing is approved, so nothing differs');
end;
$f10_no_head$;

-- 4a.2  A CERTIFIED-ZERO head.  The Office has decided that nothing is approved.
--       The WP-14 review's shape A9: this previously pushed two shifts.
savepoint before_f10_zero;
select pg_temp.seed_committed_head(
  'd0000000-0000-4000-8000-00000000000a','d0000000-0000-4000-8000-0000000000ba',
  1,null,'[]'::jsonb);
do $f10_zero$
declare
  v jsonb;
  v_entitlement jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','AVAILABLE',
    'F10 a certified-zero head is a decided fact, not an error');
  perform pg_temp.assert_eq(v_entitlement->>'certified_zero','true',
    'F10 certified zero is reported as such');
  perform pg_temp.assert_eq(v_entitlement->>'total_hours','0',
    'F10 a certified-zero head approves zero hours');

  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'F10/A9 a certified-zero head must show NO approved shifts (was: two)');
end;
$f10_zero$;
rollback to savepoint before_f10_zero;

-- 4a.3  A head that approves exactly the source, then a LATER head that approves
--       less.  The payload must CHANGE, because the push deduplication key is a
--       digest of it and a silent payload is why the WP-14 shape A9b pushed
--       nothing when the approved hours changed.
savepoint before_f10_change;
select pg_temp.seed_committed_head(
  'd0000000-0000-4000-8000-00000000001a','d0000000-0000-4000-8000-0000000000bb',
  1,null,pg_temp.head_matching_source());
do $f10_change$
declare
  v_first jsonb;
  v_second jsonb;
begin
  v_first:=private.weekly_source_candidate_approved_hours_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(jsonb_array_length(v_first)::text,'2',
    'F10 the head approving both shifts yields both rows');
  perform pg_temp.assert_eq(v_first#>>'{0,end}','19:00',
    'F10 the first approved row carries the recovered clock times');

  -- The Office decides differently: a new committed head approving one shift,
  -- staged first, then the old head superseded, then the new head committed -
  -- the order the coordinator uses, and the only order the schema allows.
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000002a','d0000000-0000-4000-8000-0000000000bc',
    2,'d0000000-0000-4000-8000-00000000001a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000002',
      'member_identity','c9000000-0000-4000-8000-000000000002',
      'work_date','2026-09-02','hours_day',7)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000001a',
    'd0000000-0000-4000-8000-00000000002a');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-00000000002a');

  v_second:=private.weekly_source_candidate_approved_hours_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(jsonb_array_length(v_second)::text,'1',
    'F10 the later head approving one shift yields one row');
  perform pg_temp.assert_true(v_first is distinct from v_second,
    'F10/A9b the payload MUST change when the approved hours change, or the '
    ||'deduplication key cannot change and no push is ever made');
end;
$f10_change$;
rollback to savepoint before_f10_change;

-- 4a.4  Two committed heads for one family: contradictory evidence, reported and
--       never resolved by picking one.  The array-returning wrapper RAISES, so a
--       caller that can only carry an array cannot mistake it for "nothing is
--       approved".
--
--       HONEST NOTE ON REACHABILITY.  The schema currently makes this shape
--       unreachable through real data: the partial unique indexes
--       `..._committed_current_uq (btrim(root_family_booking_id))` and
--       `..._committed_root_uq (root_timesheet_id)` allow one committed head per
--       family, and the head root-identity trigger forces
--       `root_family_booking_id` to equal the Timesheet's own `booking_id`.  The
--       branch is therefore defence in depth.  It is nevertheless EXECUTED here
--       rather than merely inspected (Part 1, executed-review rule 1), by
--       dropping those two indexes inside this savepoint and rolling them back
--       with it.  That relaxation lives inside the verifier, so it runs
--       identically at release time; it is not a hand patch of one clone.
savepoint before_f10_two_heads;
do $f10_two_heads$
declare
  v_entitlement jsonb;
  v_raised boolean:=false;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000003a','d0000000-0000-4000-8000-0000000000bd',
    1,null,pg_temp.head_matching_source());

  drop index public.weekly_source_entitlement_heads_committed_current_uq;
  drop index public.weekly_source_entitlement_heads_committed_root_uq;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current,
    actual_schedule_json,additional_units_week,additional_units_per_day
  ) values (
    'cc000000-0000-4000-8000-00000000000f','MYTMS-PRODUCER-SOURCE','jo-nurse',
    'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
    '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/jo0f.png',repeat('f',64),
    'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',2,false,
    '[]'::jsonb,'{}'::jsonb,'{}'::jsonb);
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'd0000000-0000-4000-8000-0000000000be',1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-00000000000f',
    'c4000000-0000-4000-8000-000000000001','d0000000-0000-4000-8000-0000000000be',
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    sha256(convert_to('request:second-head','UTF8')),
    sha256(convert_to('source:second-head','UTF8')),
    sha256(convert_to('choice:second-head','UTF8')),
    sha256(convert_to('before:second-head','UTF8')),
    array['d0000000-0000-4000-8000-00000000004a']::uuid[],'PROPOSED');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token
  ) values (
    'd0000000-0000-4000-8000-00000000004a','LOCKED_FINAL_SOURCE',
    'c0000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001','2026-09-06',
    'cc000000-0000-4000-8000-00000000000f','MYTMS-PRODUCER-SOURCE',2,2,
    'd0000000-0000-4000-8000-00000000003a','COMMITTED_CURRENT',true,0,
    decode(repeat('51',32),'hex'),decode(repeat('52',32),'hex'),
    decode(repeat('53',32),'hex'),'d0000000-0000-4000-8000-0000000000be',1,
    'd0000000-0000-4000-8000-0000000000be','c1000000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),decode(repeat('54',32),'hex'),
    gen_random_uuid());

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'F10 two committed heads for one family is contradictory evidence');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'MULTIPLE_COMMITTED_HEADS_FOR_FAMILY','F10 two heads reason');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 two heads produce no rows at all');

  begin
    perform private.weekly_source_candidate_approved_hours_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'));
  exception when others then
    v_raised:=true;
  end;
  perform pg_temp.assert_true(v_raised,
    'F10 the array wrapper must RAISE on UNAVAILABLE, never return an empty '
    ||'array that looks like "nothing is approved"');
end;
$f10_two_heads$;
rollback to savepoint before_f10_two_heads;

-- 4a.5  A head whose declared component_count disagrees with what is stored.
savepoint before_f10_count;
do $f10_count$
declare
  v_entitlement jsonb;
begin
  -- Two components stored, three declared.  The component rows are immutable
  -- once written, so this is seeded rather than produced by a later deletion.
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000005a','d0000000-0000-4000-8000-0000000000bf',
    1,null,pg_temp.head_matching_source(),true,3);
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason','HEAD_COMPONENT_COUNT_MISMATCH',
    'F10 a head whose stored components do not match its own count');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 count mismatch produces no rows');
end;
$f10_count$;
rollback to savepoint before_f10_count;

-- 4a.6  A head component naming a work event that resolves to no times at all.
savepoint before_f10_times;
do $f10_times$
declare
  v_entitlement jsonb;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000006a','d0000000-0000-4000-8000-0000000000c0',
    1,null,jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000003',
      'member_identity','not-a-resolvable-work-event',
      'work_date','2026-09-03','hours_day',5)));
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_COMPONENT_TIMES_UNRESOLVED',
    'F10 a component whose times cannot be recovered states no schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 unresolved times produce no rows');
end;
$f10_times$;
rollback to savepoint before_f10_times;

-- 4a.7  A head whose hours cannot be reconciled with the recovered times.  This
--       is the branch that stops the OPPOSITE error from F10: presenting clock
--       times that do not describe the hours the Office approved.
savepoint before_f10_reconcile;
do $f10_reconcile$
declare
  v_entitlement jsonb;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000007a','d0000000-0000-4000-8000-0000000000c1',
    1,null,jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000004',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      -- The head says 4 hours; the only times available for that work event are
      -- 09:00-19:00 break 30, which is 9.5 hours.
      'work_date','2026-09-01','hours_day',4)));
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_ENTITLEMENT_NOT_RECONCILED',
    'F10 times that do not describe the approved hours state no schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'F10 irreconcilable hours produce no rows');
end;
$f10_reconcile$;
rollback to savepoint before_f10_reconcile;

-- 4a.8  The Office's OWN protected statement supplies the times when it exists,
--       and is preferred over the source row.  This is the real WP-14 A9b shape:
--       the Office took the shift over and reduced it to four hours, and the
--       head's buckets were computed from those times, so they reconcile.
savepoint before_f10_office;
insert into public.weekly_exceptional_pay_target_families(
  id,agency_id,candidate_id,contract_id,week_start_date,week_ending_date,
  root_timesheet_id,root_family_booking_id,target_domain,target_domain_version,
  ownership_state,first_signed_evidence_fingerprint,current_generation_number,
  current_lifecycle_state,c1_publication_state,current_component_count,
  bound_version,creation_idempotency_key
) values (
  'd2000000-0000-4000-8000-000000000001','c0000000-0000-4000-8000-000000000001',
  'c3000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
  '2026-08-31','2026-09-06','cc000000-0000-4000-8000-000000000001',
  'MYTMS-PRODUCER-SOURCE','WEEKLY_PROTECTED_PAY','C1_V1','TARGET_MANAGED',
  decode(repeat('61',32),'hex'),1,'PROTECTED','NONE',1,1,
  'mytms-producer-family-1');

insert into public.weekly_exceptional_orchestration_runs(
  id,family_id,request_kind,idempotency_key,requested_by_user_id,state,
  before_state_fingerprint,started_at_utc
) values (
  'd3000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
  'APPROVE','mytms-producer-run-1','c1000000-0000-4000-8000-000000000001','COMPLETE',
  decode(repeat('62',32),'hex'),now());

insert into public.weekly_exceptional_payment_approvals(
  id,pay_target_family_id,work_event_id,candidate_id,client_id,contract_id,
  week_ending,protected_work_date,protected_start_at_local,protected_end_at_local,
  protected_break_minutes,contributing_issue_episode_ids,
  contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
  contract_rate_policy_source_fingerprint,approved_by_user_id,approval_reason,
  approved_at_utc,source_cycle_id,approved_target_pay_components_json,
  approved_target_gross,creation_orchestration_run_id,approval_hash,
  creation_idempotency_key
) values (
  'd4000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
  'c9000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
  '2026-09-06','2026-09-01','2026-09-01 09:00:00','2026-09-01 13:00:00',0,
  array[]::uuid[],decode(repeat('63',32),'hex'),decode(repeat('64',32),'hex'),
  decode(repeat('65',32),'hex'),'c1000000-0000-4000-8000-000000000001',
  'Office reduced the shift',now(),'c6000000-0000-4000-8000-000000000001',
  '{}'::jsonb,0,'d3000000-0000-4000-8000-000000000001',
  decode(repeat('66',32),'hex'),'mytms-producer-approval-1');

insert into public.weekly_exceptional_pay_family_events(
  id,family_id,event_sequence,durable_work_event_id,evidence_approval_id,
  work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,
  source_proposal_snapshot_json,source_proposal_hash,
  fixed_office_target_snapshot_json,fixed_office_target_hash,state,
  office_actor_user_id,office_reason,occurred_at_utc,event_hash
) values (
  'd5000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000001',
  1,'c9000000-0000-4000-8000-000000000001','d4000000-0000-4000-8000-000000000001',
  '2026-09-01','2026-09-01 09:00:00','2026-09-01 13:00:00',0,'{}'::jsonb,
  '{}'::jsonb,decode(repeat('67',32),'hex'),'{}'::jsonb,
  decode(repeat('68',32),'hex'),'WAIT','c1000000-0000-4000-8000-000000000001',
  'Office reduced the shift',now(),decode(repeat('69',32),'hex'));

do $f10_office$
declare
  v_entitlement jsonb;
  v jsonb;
begin
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-00000000008a','d0000000-0000-4000-8000-0000000000c2',
    1,null,jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000005',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      'work_date','2026-09-01','hours_day',4)));
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','AVAILABLE',
    'F10/A9b the Office statement supplies times that reconcile with the head');
  perform pg_temp.assert_eq(v_entitlement->>'total_hours','4.000000',
    'F10/A9b the approved total is the head''s four hours');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'1',
    'F10/A9b one approved row');
  perform pg_temp.assert_eq(v_entitlement#>>'{rows,0,end}','13:00',
    'F10/A9b the Office''s own end time is preferred over the source row''s 19:00');

  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'1',
    'F10/A9b the Candidate is shown the approved shift');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','true',
    'F10/A9b the approved hours differ from the two submitted shifts');
  -- The whole-payload scan, with approved rows PRESENT and a non-zero money
  -- column seeded on every component.
  perform pg_temp.assert_true(lower(v::text) !~ '\m(pay|amount|vat|rate|charge|money|pence)\M',
    'F10/A9b the payload carries no money vocabulary with approved rows present');
  perform pg_temp.assert_true(v::text !~ '123\.45',
    'F10/A9b the component money column is never read into the payload');
end;
$f10_office$;
rollback to savepoint before_f10_office;

-- ---------------------------------------------------------------------------
-- 5. Contract conformance and UI-019 (MYTMS_SAME).
--
--    WP-11d F10: UI-019 now needs a COMMITTED HEAD whose approved hours equal
--    the submission.  Without one there are no approved hours at all, which is
--    a different state (`NO_APPROVED_ENTITLEMENT`), not MYTMS_SAME.
-- ---------------------------------------------------------------------------
savepoint before_head_world;
select pg_temp.seed_committed_head(
  'd0000000-0000-4000-8000-00000000009a','d0000000-0000-4000-8000-0000000000c3',
  1,null,pg_temp.head_matching_source());

do $ui019$
declare
  v jsonb;
  v_keys text;
  v_hours jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v) as k(key);
  perform pg_temp.assert_eq(v_keys,
    'approved_hours_differ,approved_hours_to_be_paid,expense_entry_mode,'
    ||'request_id,request_kind,scope_id,submitted_additional_units_per_day,'
    ||'submitted_additional_units_week,submitted_timesheet',
    'CandidateWeeklySourceView members, exactly and only the nine required');

  perform pg_temp.assert_eq(jsonb_typeof(v->'submitted_timesheet'),'array','submitted is an array');
  perform pg_temp.assert_eq(jsonb_typeof(v->'approved_hours_to_be_paid'),'array','approved is an array');
  perform pg_temp.assert_eq(jsonb_typeof(v->'approved_hours_differ'),'boolean','differ is a boolean');
  perform pg_temp.assert_eq(jsonb_typeof(v->'request_id'),'null','no live request');
  perform pg_temp.assert_eq(jsonb_typeof(v->'scope_id'),'null','no live scope');
  perform pg_temp.assert_eq(jsonb_typeof(v->'request_kind'),'null','no live request kind');

  -- CandidateWeeklySourceHours: exactly the seven required members.
  select value into v_hours from jsonb_array_elements(v->'submitted_timesheet') limit 1;
  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v_hours) as k(key);
  perform pg_temp.assert_eq(v_keys,
    'additional_units,break_entry,date,end,row_key,start,worked',
    'CandidateWeeklySourceHours members');

  -- UI-019 MYTMS_SAME: the approved hours equal the submission, so no approved
  -- card is offered at all.
  perform pg_temp.assert_eq(v->>'approved_hours_differ','false','UI-019 no difference');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'0',
    'UI-019 no approved card');
  perform pg_temp.assert_eq(v->>'expense_entry_mode','SEPARATE_TIMESHEET',
    'NAI-MYT-002 ordinary source-authority expense mode');
end;
$ui019$;

-- ---------------------------------------------------------------------------
-- 6. The payload carries no money and no internal vocabulary.
-- ---------------------------------------------------------------------------
do $vocabulary$
declare
  v_text text;
  v_term text;
begin
  v_text:=lower(private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000001')::text);
  -- Word-boundary matching: `SEPARATE_TIMESHEET` legitimately contains the
  -- letters of "rate", and the contract fixes that enum value, so a bare
  -- substring test would be wrong rather than strict.
  foreach v_term in array array[
    'source','protected','exceptional','reconciliation','reconcile',
    'remittance','invoice','recovery','payment','money','vat','rate','rates',
    'pence','charge','umbrella','batch','settled','settlement','paid'
  ]::text[]
  loop
    perform pg_temp.assert_true(v_text !~ ('\m'||v_term||'\M'),
      'MyTMS payload must never contain the term: '||v_term);
  end loop;
  -- And no money-shaped value anywhere.
  perform pg_temp.assert_true(v_text !~ '[£$€]','MyTMS payload must carry no currency symbol');
end;
$vocabulary$;

-- ---------------------------------------------------------------------------
-- 7. UI-020 (MYTMS_DIFFERENT): the approved hours differ from the submission.
-- ---------------------------------------------------------------------------
-- Source rows are immutable (`WEEKLY_SOURCE_IMMUTABLE_RECORD`), which is the
-- schema doing its job, so the difference is introduced on the Candidate's own
-- submission: the Candidate claims an hour more than the client system shows.
savepoint before_ui020;
update public.timesheets
set actual_schedule_json=
  '[{"row_key":"row-1","date":"2026-09-01","start":"09:00","end":"19:00","break_minutes":30},
    {"row_key":"row-2","date":"2026-09-02","start":"09:00","end":"18:00","break_minutes":60}]'::jsonb
where timesheet_id='cc000000-0000-4000-8000-000000000001';

do $ui020$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','true','UI-020 difference detected');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'2',
    'UI-020 complete approved schedule');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'2',
    'UI-020 the Candidate submission is still shown untouched');
  perform pg_temp.assert_eq(v#>>'{approved_hours_to_be_paid,1,end}','17:00',
    'UI-020 approved end time comes from the client system row, not the claim');
  perform pg_temp.assert_eq(v#>>'{submitted_timesheet,1,end}','18:00',
    'UI-020 the Candidate claim is shown exactly as submitted');
end;
$ui020$;
rollback to savepoint before_ui020;

-- ---------------------------------------------------------------------------
-- 8. UI-021 / NAI-MYT-001 second half: a source-authority week with no
--    Candidate submission, after Office authorisation.
--
--    The frozen contract has `additionalProperties: false` and no
--    no-submission member, so the state is expressed as an EMPTY
--    `submitted_timesheet` beside a non-empty `approved_hours_to_be_paid`.
--    That pair is unambiguous and the hours are never placed in
--    `submitted_timesheet`, so they can never be labelled as a submission.
-- ---------------------------------------------------------------------------
savepoint before_ui021;
update public.timesheets
set r2_nurse_key=null,img_sha256_nurse=null,actual_schedule_json='[]'::jsonb
where timesheet_id='cc000000-0000-4000-8000-000000000001';

do $ui021$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_timesheet')::text,'0',
    'UI-021 the submitted fact stays empty and is never filled from the client system');
  perform pg_temp.assert_eq(jsonb_array_length(v->'approved_hours_to_be_paid')::text,'2',
    'UI-021 approved hours only');
  perform pg_temp.assert_eq(v->>'approved_hours_differ','true','UI-021 differs from nothing');
  perform pg_temp.assert_eq(jsonb_array_length(v->'submitted_additional_units_week')::text,'0',
    'UI-021 no submitted units');
  perform pg_temp.assert_eq(v#>>'{approved_hours_to_be_paid,0,worked}','true',
    'UI-021 approved rows carry hours');
  -- WP-11d: the whole-payload scan repeated in the UI-021 state, where approved
  -- rows are present.  The package's original scan only ever ran with an empty
  -- approved array.
  perform pg_temp.assert_true(
    lower(v::text) !~ ('\m(source|protected|exceptional|reconciliation|remittance'
      ||'|invoice|recovery|payment|money|vat|rate|rates|pence|charge|umbrella'
      ||'|batch|settled|settlement|paid)\M'),
    'UI-021 payload carries no internal vocabulary with approved rows present');
  perform pg_temp.assert_true(v::text !~ '123\.45',
    'UI-021 the head component money column is never read into the payload');
end;
$ui021$;
rollback to savepoint before_ui021;

-- ---------------------------------------------------------------------------
-- 9. NAI-MYT-002: the configured source-fixed expense mode hides Candidate
--    expense entry; every other source-authority week offers the separate
--    additional expense Timesheet.
-- ---------------------------------------------------------------------------
savepoint before_expense_mode;
update public.weekly_source_client_policies
set source_fixed_expenses_enabled=true
where source_group_id='c5000000-0000-4000-8000-000000000001'
  and client_id='c2000000-0000-4000-8000-000000000001';

do $expense_mode$
begin
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001')->>'expense_entry_mode',
    'NOT_AVAILABLE','NAI-MYT-002 source-fixed expense hides Candidate expense entry');
end;
$expense_mode$;
rollback to savepoint before_expense_mode;

-- ---------------------------------------------------------------------------
-- 9a. WP-11d F4: an integrity or configuration failure on a CONFIRMED Weekly
--     Source week is no longer indistinguishable from an ordinary week.
--
--     Before this fix the producer returned NULL for both, the detail RPC
--     omitted the member, and MyTMS silently rendered the ordinary Timesheet -
--     so a Candidate saw a blank week that should have shown hours, with no
--     signal anywhere.  The frozen contract has no error member, so the only
--     honest fail-closed route is to let the failure propagate.
-- ---------------------------------------------------------------------------
savepoint before_f4;
do $f4_policy$
declare
  v_raised boolean:=false;
  v_ordinary jsonb;
begin
  -- The effective-policy helper raises when the client policy row is gone.
  delete from public.weekly_source_client_policies
  where source_group_id='c5000000-0000-4000-8000-000000000001'
    and client_id='c2000000-0000-4000-8000-000000000001';
  begin
    perform private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001');
  exception when others then
    v_raised:=true;
  end;
  perform pg_temp.assert_true(v_raised,
    'F4 a configuration failure on a confirmed Weekly Source week must not be '
    ||'swallowed into NULL');

  -- And an ordinary Timesheet is still NULL, not an error: the two states are
  -- now distinguishable, which is the whole point.
  v_ordinary:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_true(v_ordinary is null,
    'F4 an ordinary Timesheet still produces NULL');
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_view_merge_v1(
      'cc000000-0000-4000-8000-000000000002')::text,'{}',
    'F4 an ordinary Timesheet still merges nothing');
end;
$f4_policy$;
rollback to savepoint before_f4;

savepoint before_f4;
do $f4_entitlement$
declare
  v_entitlement jsonb;
  v_view jsonb;
  v_raised boolean:=false;
  v_wrapper_raised boolean:=false;
  v_message text;
begin
  -- WP-11e G3.  THIS ASSERTION CHANGED, AND THE CHANGE IS THE POINT.
  --
  -- It used to require that an UNAVAILABLE entitlement head PROPAGATE as an
  -- error through `weekly_source_candidate_view_v1`, and therefore through
  -- `public.candidate_app_timesheet_detail_v2`.  Executed against an ordinary
  -- source re-upload, that meant the worker could not open their own week at
  -- all - not the approved card, the WEEK - while their own submitted evidence
  -- was perfectly sound.  An unresolvable head is a statement about the
  -- OFFICE'S approval, not about the Candidate's submission.
  --
  -- It now degrades to a STATED unavailable result.  What the original
  -- assertion was defending is asserted here in full and separately: the
  -- resolver still states the reason, the payload never falls back to the
  -- source, and the array wrapper WP-14 uses still raises.
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000aa','d0000000-0000-4000-8000-0000000000c4',
    2,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-000000000006',
      'member_identity','still-not-a-work-event',
      'work_date','2026-09-03','hours_day',5)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000aa');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000aa');

  -- 1. The reason is still STATED, in full, by the resolver the Office
  --    projection, the audit and WP-14 all read.
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 the unresolvable head is still an explicit unavailable state');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_COMPONENT_TIMES_UNRESOLVED',
    'G3 carrying its exact reason');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'0',
    'G3 and no rows');

  -- 2. The view DOES NOT raise, and the week opens.
  begin
    v_view:=private.weekly_source_candidate_view_v1(
      'cc000000-0000-4000-8000-000000000001');
  exception when others then
    v_raised:=true;
    get stacked diagnostics v_message=message_text;
  end;
  perform pg_temp.assert_true(not v_raised,
    'G3 an unresolvable head must NOT stop the worker opening their week, got '
    ||coalesce(v_message,'<no message>'));
  perform pg_temp.assert_true(v_view is not null,
    'G3 the payload is produced, so NULL still means only "not a Weekly Source '
    ||'week"');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'submitted_timesheet')::text,'2',
    'G3 the Candidate still sees their OWN submitted evidence, which is sound');

  -- 3. And it still never falls back to the source (WP-11d F10).
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'approved_hours_to_be_paid')::text,'0',
    'G3 no approved hours are shown, and in particular no source-derived ones');
  perform pg_temp.assert_eq(v_view->>'approved_hours_differ','false',
    'G3 nothing is presented as approved, so nothing differs');

  -- 4. The REAL RPC carries the week.  This is the assertion a worker cares
  --    about, driven through the real Candidate-app session in section 10a's
  --    world; here it is driven directly on the merge the RPC performs.
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_merge_v1(
      'cc000000-0000-4000-8000-000000000001') ? 'weekly_source_candidate_view',
    'G3 the additive call still returns the member rather than failing');

  -- 5. WP-14's array wrapper is UNCHANGED and still raises, so its push is
  --    still withheld on exactly this state.
  begin
    perform private.weekly_source_candidate_approved_hours_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'));
  exception when others then
    v_wrapper_raised:=true;
    get stacked diagnostics v_message=message_text;
  end;
  perform pg_temp.assert_true(v_wrapper_raised,
    'G3 the array wrapper WP-14 calls still raises on an unavailable head');
  perform pg_temp.assert_true(
    v_message like '%WEEKLY_SOURCE_APPROVED_ENTITLEMENT_UNAVAILABLE%'
    and v_message like '%APPROVED_COMPONENT_TIMES_UNRESOLVED%',
    'G3 and the raise still carries the exact reason');
end;
$f4_entitlement$;
rollback to savepoint before_f4;

-- ---------------------------------------------------------------------------
-- 9b. WP-11e G3: AN ORDINARY SOURCE RE-UPLOAD.
--
--     `weekly_source_uploads.state` has `SUPERSEDED` as a designed state and
--     the resolver has `EXACT_DURABLE_LINEAGE` precisely so that a corrected
--     re-upload of the same shift resolves to the SAME durable work event.
--     Before this fix, every approved component whose times come from the
--     source then had two matches, the head was `APPROVED_COMPONENT_TIMES_
--     UNRESOLVED`, and the real detail RPC RAISED: the worker could not open a
--     week whose head was sound and whose two source statements were identical.
--
--     The head here is the source-matching head seeded in section 4a.
-- ---------------------------------------------------------------------------
savepoint before_reupload;
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
  physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  'c7000000-0000-4000-8000-000000000002','c6000000-0000-4000-8000-000000000001',
  'mytms-producer-verify-corrected.xlsx',decode(repeat('61',32),'hex'),100,
  '34444444-4444-4444-8444-444444444444','verify','verify',
  decode(repeat('62',32),'hex'),decode(repeat('63',32),'hex'),
  'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',2,2,decode(repeat('64',32),'hex'),
  'CURRENT','c1000000-0000-4000-8000-000000000001');
update public.weekly_source_uploads set state='SUPERSEDED'
where id='c7000000-0000-4000-8000-000000000001';
update public.weekly_source_cycles
set current_complete_upload_id='c7000000-0000-4000-8000-000000000002'
where id='c6000000-0000-4000-8000-000000000001';

insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values
  ('ca000000-0000-4000-8000-000000000011','c7000000-0000-4000-8000-000000000002',
   1,'producer-shift-1','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
   '2026-09-01 09:00:00','2026-09-01 19:00:00',30,570,'SOURCE_WORKED',
   decode(repeat('93',32),'hex')),
  ('ca000000-0000-4000-8000-000000000012','c7000000-0000-4000-8000-000000000002',
   2,'producer-shift-2','JO NURSE','PRODUCER SOURCE TRUST','2026-09-02',
   '2026-09-02 09:00:00','2026-09-02 17:00:00',60,420,'SOURCE_WORKED',
   decode(repeat('94',32),'hex'));

insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values
  ('cb000000-0000-4000-8000-000000000011','ca000000-0000-4000-8000-000000000011',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
   decode(repeat('b3',32),'hex'),decode(repeat('a7',32),'hex'),
   decode(repeat('a8',32),'hex'),decode(repeat('a9',32),'hex')),
  ('cb000000-0000-4000-8000-000000000012','ca000000-0000-4000-8000-000000000012',
   1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
   'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
   'c9000000-0000-4000-8000-000000000002','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
   decode(repeat('b4',32),'hex'),decode(repeat('aa',32),'hex'),
   decode(repeat('ab',32),'hex'),decode(repeat('ac',32),'hex'));

do $reupload$
declare
  v_entitlement jsonb;
  v_view jsonb;
  v_rows integer;
begin
  select count(*)::integer into v_rows
  from public.weekly_source_row_resolutions as resolution
  join public.weekly_source_upload_rows as source_row
    on source_row.id=resolution.upload_row_id
  where resolution.work_event_id='c9000000-0000-4000-8000-000000000002'
    and resolution.mapping_state='RESOLVED';
  perform pg_temp.assert_eq(v_rows::text,'2',
    'G3 the shape really is two lineage-matched rows for one work event');

  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','AVAILABLE',
    'G3 an ordinary re-upload does not make a sound head unresolvable');
  perform pg_temp.assert_eq(jsonb_array_length(v_entitlement->'rows')::text,'2',
    'G3 both approved shifts are still presented');

  v_view:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v_view is not null,
    'G3 the worker can open the week after a re-upload');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'submitted_timesheet')::text,'2',
    'G3 with their own submitted evidence intact');
end;
$reupload$;

-- A re-upload that states DIFFERENT times for the same work event is a real
-- contradiction and must STILL fail closed.  The distinct-tuple count is what
-- separates the two cases, so it is executed, not argued.
savepoint before_contradictory_reupload;
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,normalised_row_hash
) values (
  'ca000000-0000-4000-8000-000000000013','c7000000-0000-4000-8000-000000000002',
  3,'producer-shift-1b','JO NURSE','PRODUCER SOURCE TRUST','2026-09-01',
  '2026-09-01 07:00:00','2026-09-01 15:00:00',30,450,'SOURCE_WORKED',
  decode(repeat('95',32),'hex'));
insert into public.weekly_source_row_resolutions(
  id,upload_row_id,generation,mapping_state,candidate_id,client_id,contract_id,
  work_event_id,contract_selection_method,work_event_match_kind,
  work_event_match_fingerprint,qualification_profile_fingerprint,
  qualifying_contract_set_hash,source_row_fingerprint
) values (
  'cb000000-0000-4000-8000-000000000013','ca000000-0000-4000-8000-000000000013',
  1,'RESOLVED','c3000000-0000-4000-8000-000000000001',
  'c2000000-0000-4000-8000-000000000001','c4000000-0000-4000-8000-000000000001',
  'c9000000-0000-4000-8000-000000000001','AUTO_UNIQUE','EXACT_DURABLE_LINEAGE',
  decode(repeat('b5',32),'hex'),decode(repeat('ad',32),'hex'),
  decode(repeat('ae',32),'hex'),decode(repeat('af',32),'hex'));
do $contradictory_reupload$
declare
  v_entitlement jsonb;
  v_view jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 two DIFFERENT statements of the same shift are still a contradiction');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_COMPONENT_TIMES_UNRESOLVED','G3 with the fail-closed reason');
  -- Fail-closed on the approved card, and still openable.
  v_view:=private.weekly_source_candidate_view_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    jsonb_array_length(v_view->'approved_hours_to_be_paid')::text,'0',
    'G3 a contradiction shows no approved hours and no source fallback');
end;
$contradictory_reupload$;
rollback to savepoint before_contradictory_reupload;

-- A SUPERSEDED upload's rows alone supply no times at all: the authority is the
-- cycle's current complete upload and nothing else.
savepoint before_only_superseded;
update public.weekly_source_cycles
set current_complete_upload_id=null
where id='c6000000-0000-4000-8000-000000000001';
do $no_authoritative_upload$
declare
  v_entitlement jsonb;
begin
  v_entitlement:=private.weekly_source_candidate_approved_entitlement_v1(
    private.weekly_source_candidate_week_context_v1(
      'cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v_entitlement->>'state','UNAVAILABLE',
    'G3 with no authoritative upload the head''s times cannot be recovered, and '
    ||'that fails closed rather than reading a superseded upload');
  perform pg_temp.assert_eq(v_entitlement->>'reason',
    'APPROVED_COMPONENT_TIMES_UNRESOLVED','G3 fail-closed reason');
end;
$no_authoritative_upload$;
rollback to savepoint before_only_superseded;
rollback to savepoint before_reupload;

-- ---------------------------------------------------------------------------
-- 9c. WP-11e G3: EVERY path that can raise through the real Candidate detail
--     RPC, enumerated and driven.  The rule the package now holds is:
--
--       * a failure to read the WEEK (client policy, family resolver) still
--         propagates - the caller cannot be handed a week that could not be
--         identified;
--       * a failure to resolve the OFFICE'S APPROVAL degrades to a stated
--         unavailable result, because the Candidate's own submitted evidence is
--         sound and they are entitled to see it.
-- ---------------------------------------------------------------------------
create or replace function pg_temp.view_outcome(p_timesheet uuid)
returns text
language plpgsql
as $outcome$
declare
  v jsonb;
begin
  v:=private.weekly_source_candidate_view_v1(p_timesheet);
  if v is null then return 'NULL'; end if;
  return 'PAYLOAD approved='
    ||jsonb_array_length(v->'approved_hours_to_be_paid')::text;
exception when others then
  -- `sqlstate` is a plpgsql special variable and must never be qualified.
  return 'RAISED '||sqlstate;
end;
$outcome$;

savepoint before_raise_census;
do $raise_census_heads$
declare
  v_head uuid;
  v_bundle uuid;
  v_revision bigint:=2;
begin
  -- (a) unresolvable component identity
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e1','d0000000-0000-4000-8000-0000000000f1',
    v_revision,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-0000000000e1',
      'member_identity','no-such-work-event',
      'work_date','2026-09-03','hours_day',5)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e1');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e1');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 APPROVED_COMPONENT_TIMES_UNRESOLVED degrades, it does not raise');
end;
$raise_census_heads$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_reconcile$
begin
  -- (b) a head whose hours cannot be reconciled with the recovered times
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e2','d0000000-0000-4000-8000-0000000000f2',
    2,'d0000000-0000-4000-8000-00000000009a',
    jsonb_build_array(jsonb_build_object(
      'component_id','d1000000-0000-4000-8000-0000000000e2',
      'member_identity','c9000000-0000-4000-8000-000000000001',
      'work_date','2026-09-01','hours_day',4)),false);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e2');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e2');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 APPROVED_ENTITLEMENT_NOT_RECONCILED degrades, it does not raise');
end;
$raise_census_reconcile$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_count$
begin
  -- (c) a head whose declared component_count disagrees with what is stored
  perform pg_temp.seed_committed_head(
    'd0000000-0000-4000-8000-0000000000e3','d0000000-0000-4000-8000-0000000000f3',
    2,'d0000000-0000-4000-8000-00000000009a',pg_temp.head_matching_source(),
    false,5);
  perform pg_temp.supersede_head('d0000000-0000-4000-8000-00000000009a',
    'd0000000-0000-4000-8000-0000000000e3');
  perform pg_temp.commit_head('d0000000-0000-4000-8000-0000000000e3');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 HEAD_COMPONENT_COUNT_MISMATCH degrades, it does not raise');
end;
$raise_census_count$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_two_heads$
begin
  -- (d) two committed heads for one family.  The relaxation of the two partial
  --     unique indexes lives inside this savepoint, exactly as section 4a.4
  --     does it, so it runs identically at release time.
  drop index public.weekly_source_entitlement_heads_committed_current_uq;
  drop index public.weekly_source_entitlement_heads_committed_root_uq;
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
    r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current,
    actual_schedule_json,additional_units_week,additional_units_per_day
  ) values (
    'cc000000-0000-4000-8000-0000000000e0','MYTMS-PRODUCER-SOURCE','jo-nurse',
    'producer-source-trust','ward-a','rmn','2026-09-01 08:00:00+00',
    '2026-09-02 17:00:00+00',90,1050,'2026-09-06','verify/joe0.png',repeat('e',64),
    'c4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',2,false,
    '[]'::jsonb,'{}'::jsonb,'{}'::jsonb);
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,
    bundle_kind,source_root_family_booking_id,source_root_timesheet_id,
    source_contract_id,decision_id,decided_by_user_id,publication_mode,
    request_digest,source_revision_digest,contract_choice_digest,
    before_inventory_digest,proposed_head_ids,state
  ) values (
    'd0000000-0000-4000-8000-0000000000f4',1,'c0000000-0000-4000-8000-000000000001',
    'c3000000-0000-4000-8000-000000000001','2026-09-06','SINGLE_ROOT',
    'MYTMS-PRODUCER-SOURCE','cc000000-0000-4000-8000-0000000000e0',
    'c4000000-0000-4000-8000-000000000001','d0000000-0000-4000-8000-0000000000f4',
    'c1000000-0000-4000-8000-000000000001','IMMEDIATE',
    sha256(convert_to('request:g3-second-head','UTF8')),
    sha256(convert_to('source:g3-second-head','UTF8')),
    sha256(convert_to('choice:g3-second-head','UTF8')),
    sha256(convert_to('before:g3-second-head','UTF8')),
    array['d0000000-0000-4000-8000-0000000000e4']::uuid[],'PROPOSED');
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
    prior_head_id,state,certified_zero,component_count,entitlement_digest,
    inventory_digest,source_generation_digest,decision_bundle_id,bundle_revision,
    decision_id,decided_by_user_id,committed_at_utc,publication_receipt_digest,
    scope_change_tx_token
  ) values (
    'd0000000-0000-4000-8000-0000000000e4','LOCKED_FINAL_SOURCE',
    'c0000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
    'c4000000-0000-4000-8000-000000000001','2026-09-06',
    'cc000000-0000-4000-8000-0000000000e0','MYTMS-PRODUCER-SOURCE',2,2,
    'd0000000-0000-4000-8000-00000000009a','COMMITTED_CURRENT',true,0,
    decode(repeat('55',32),'hex'),decode(repeat('56',32),'hex'),
    decode(repeat('57',32),'hex'),'d0000000-0000-4000-8000-0000000000f4',1,
    'd0000000-0000-4000-8000-0000000000f4','c1000000-0000-4000-8000-000000000001',
    pg_catalog.transaction_timestamp(),decode(repeat('58',32),'hex'),
    gen_random_uuid());
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_approved_entitlement_v1(
      private.weekly_source_candidate_week_context_v1(
        'cc000000-0000-4000-8000-000000000001'))->>'reason',
    'MULTIPLE_COMMITTED_HEADS_FOR_FAMILY','G3 the shape really is two heads');
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001'),'PAYLOAD approved=0',
    'G3 MULTIPLE_COMMITTED_HEADS_FOR_FAMILY degrades, it does not raise');
end;
$raise_census_two_heads$;
rollback to savepoint before_raise_census;

savepoint before_raise_census;
do $raise_census_policy$
declare
  v_outcome text;
begin
  -- (e) THE ONE THAT MUST STILL RAISE.  A client policy that cannot be resolved
  --     means the WEEK cannot be read at all, not that an approval is
  --     unresolved, and the F4 ruling stands for it unchanged.
  delete from public.weekly_source_client_policies
  where source_group_id='c5000000-0000-4000-8000-000000000001'
    and client_id='c2000000-0000-4000-8000-000000000001';
  v_outcome:=pg_temp.view_outcome('cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_true(v_outcome like 'RAISED%',
    'G3 a configuration failure still propagates, got '||v_outcome);
  perform pg_temp.assert_eq(
    pg_temp.view_outcome('cc000000-0000-4000-8000-000000000002'),'NULL',
    'G3 and an ordinary Timesheet is still NULL, so the two remain '
    ||'distinguishable');
end;
$raise_census_policy$;
rollback to savepoint before_raise_census;

rollback to savepoint before_head_world;

-- ---------------------------------------------------------------------------
-- 10. The ordinary-Timesheet differential.
--
--     The single additive call site merges `private.…_view_merge_v1(...)` into
--     the Candidate detail result.  For every Timesheet that is not a Weekly
--     Source week that call returns `{}`, and `x || '{}'::jsonb = x` for every
--     jsonb object, so the ordinary payload is byte-identical to before the
--     edit.  Both halves are proved here.
-- ---------------------------------------------------------------------------
do $ordinary$
declare
  v_merge jsonb;
begin
  v_merge:=private.weekly_source_candidate_view_merge_v1('cc000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v_merge::text,'{}',
    'an ordinary Timesheet merges nothing into the Candidate detail');
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_v1('cc000000-0000-4000-8000-000000000002') is null,
    'an ordinary Timesheet produces no Weekly Source view');

  -- The merge identity, on a realistic detail-shaped object.
  perform pg_temp.assert_true(
    ('{"ok":true,"timesheet":{"id":"x"},"hours":[1,2,3],"break_entry":null}'::jsonb
       ||'{}'::jsonb)::text
    ='{"ok":true,"timesheet":{"id":"x"},"hours":[1,2,3],"break_entry":null}'::jsonb::text,
    'merging an empty object is a no-op');

  -- A DAILY record never reaches the call site, and would produce nothing if it did.
  perform pg_temp.assert_true(
    private.weekly_source_candidate_view_merge_v1(
      '00000000-0000-4000-8000-0000000000ff')::text='{}',
    'an unknown Timesheet merges nothing');

  -- The single call site really is a single call, and it is on the weekly
  -- return path only, so the DAILY early return is untouched.
  perform pg_temp.assert_eq(
    (length(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')))
     -length(replace(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')),
       'weekly_source_candidate_view_merge_v1','')))::text,
    length('weekly_source_candidate_view_merge_v1')::text,
    'exactly one additive call in candidate_app_timesheet_detail_v2');
  perform pg_temp.assert_eq(
    (length(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')))
     -length(replace(pg_get_functiondef(to_regprocedure(
       'public.candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamptz)')),
       'weekly_source_candidate_contract_week_view_merge_v1','')))::text,
    length('weekly_source_candidate_contract_week_view_merge_v1')::text,
    'exactly one first-submission contract-week call in candidate_app_timesheet_detail_v2');
  perform pg_temp.assert_eq(
    private.weekly_source_candidate_contract_week_view_merge_v1(
      'cd000000-0000-4000-8000-000000000002')::text,'{}',
    'an ordinary bound Contract Week receives no source-request unlock');
end;
$ordinary$;

-- ---------------------------------------------------------------------------
-- 10a. The real Candidate detail RPC carries it, through the single call site.
--
--      This is the end-to-end proof that the additive call actually runs inside
--      `public.candidate_app_timesheet_detail_v2` and that an ordinary Timesheet
--      comes back without the member at all.
-- ---------------------------------------------------------------------------
update public.settings_defaults
set candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||pg_catalog.jsonb_build_object('candidate_app_reads',true,'candidate_app_writes',true)
where id=1;

insert into public.candidate_app_accounts(
  id,environment,email_normalized,status,password_scheme,
  password_scheme_version,password_salt,password_digest,password_changed_at_utc
) values (
  'cf000000-0000-4000-8000-000000000001','TEST','jo.producer@example.invalid',
  'ACTIVE','PBKDF2-HMAC-SHA256',1,decode(repeat('91',16),'hex'),
  decode(repeat('92',32),'hex'),'2026-09-01 08:00:00+00');
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values (
  'cf100000-0000-4000-8000-000000000001','cf000000-0000-4000-8000-000000000001',
  'TEST','c3000000-0000-4000-8000-000000000001','ACTIVE',
  extensions.digest('mytms-producer-session','sha256'),
  '2026-09-30 00:00:00+00','2026-10-01 00:00:00+00');
insert into public.candidate_app_global_membership_links(
  membership_id,global_account_identity_hmac,account_id,candidate_id,
  candidate_code,membership_generation,state,linked_at_utc,updated_at_utc
) values (
  'cf200000-0000-4000-8000-000000000001',
  extensions.digest('mytms-producer-membership','sha256'),
  'cf000000-0000-4000-8000-000000000001','c3000000-0000-4000-8000-000000000001',
  'MYT-90001',1,'ACTIVE','2026-09-01 08:00:00+00','2026-09-01 08:00:00+00');

do $rpc$
declare
  v_source jsonb;
  v_ordinary jsonb;
begin
  v_source:=public.candidate_app_timesheet_detail_v2(
    'cf100000-0000-4000-8000-000000000001','TEST',
    'cc000000-0000-4000-8000-000000000001',null,null,now());
  perform pg_temp.assert_true(
    v_source ? 'weekly_source_candidate_view',
    'the Candidate detail RPC carries the Weekly Source view for a source week');
  perform pg_temp.assert_eq(
    jsonb_typeof(v_source->'weekly_source_candidate_view'),'object',
    'the member is the view object');
  perform pg_temp.assert_eq(
    (select string_agg(key,',' order by key)
     from jsonb_object_keys(v_source->'weekly_source_candidate_view') as k(key)),
    'approved_hours_differ,approved_hours_to_be_paid,expense_entry_mode,'
    ||'request_id,request_kind,scope_id,submitted_additional_units_per_day,'
    ||'submitted_additional_units_week,submitted_timesheet',
    'the RPC emits the contract shape');

  v_ordinary:=public.candidate_app_timesheet_detail_v2(
    'cf100000-0000-4000-8000-000000000001','TEST',
    'cc000000-0000-4000-8000-000000000002',null,null,now());
  perform pg_temp.assert_true(
    not (v_ordinary ? 'weekly_source_candidate_view'),
    'an ordinary Timesheet payload does not carry the member at all');
end;
$rpc$;

-- ---------------------------------------------------------------------------
-- 11. G9-4: the Office `action_state` Unauthorise verdict.
-- ---------------------------------------------------------------------------
do $action_state$
declare
  v jsonb;
  v_keys text;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v) as k(key);
  perform pg_temp.assert_eq(v_keys,'unauthorise,unauthorise_allowed',
    'action_state additions');

  select string_agg(key,',' order by key) into v_keys
  from jsonb_object_keys(v->'unauthorise') as k(key);
  -- WP-11d F7 adds `owner_sqlstate` and `owner_error`, so the verdict now has
  -- eleven members.  Both are always present and never null ('NONE' when the
  -- owner answered), so `jsonb_strip_nulls` at the Office call site still cannot
  -- remove one.  This is a behavioural difference recorded for the approver.
  perform pg_temp.assert_eq(v_keys,
    'authorisation_state,availability_source,available,owner_error,'
    ||'owner_sqlstate,permanent,reason,refusal_code,refusal_nature,retryable,'
    ||'withdrawn',
    'unauthorise verdict members');

  -- No member may be null: the Office projection strips nulls.
  perform pg_temp.assert_true(
    not exists(select 1 from jsonb_each(v->'unauthorise') as e(key,value)
               where jsonb_typeof(e.value)='null'),
    'no null member may reach jsonb_strip_nulls');

  -- The verdict is the withdrawal owner's own.  This root was authorised by a
  -- direct column update and carries no Weekly Source root authorisation, so
  -- the owner's own answer is NOT_MANAGED_ROOT and the control is refused.
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'an unmanaged root is not withdrawable through this owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT',
    'the refusal code comes from the withdrawal availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_nature}','PERMANENT',
    'the refusal nature comes from the withdrawal availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,permanent}','true',
    'proof/36 section 6: a permanent refusal is reported as permanent');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}',
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAW_AVAILABLE_V1',
    'the verdict came from the real availability owner');
  perform pg_temp.assert_eq(v#>>'{unauthorise,authorisation_state}','NEVER_AUTHORISED',
    'the lifecycle fact is reported beside the verdict');
  perform pg_temp.assert_eq(v#>>'{unauthorise,withdrawn}','false','not withdrawn');
end;
$action_state$;

-- The projection carries it.
do $projection$
declare
  v jsonb;
begin
  v:=public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','c1000000-0000-4000-8000-000000000001',
    'timesheet_id','cc000000-0000-4000-8000-000000000001'));
  perform pg_temp.assert_eq(v->>'applicable','true','the presentation applies');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise_allowed}' is not null,
    'action_state carries unauthorise_allowed');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise,refusal_code}' is not null,
    'action_state carries the refusal kind');
  perform pg_temp.assert_true(v#>'{action_state,unauthorise,withdrawn}' is not null,
    'action_state carries the withdrawn state');
  -- The existing members are still there and unchanged in shape.
  perform pg_temp.assert_true(v#>'{action_state,authorise_allowed}' is not null,
    'the existing authorise_allowed member survives');
end;
$projection$;

-- A late-bound or failing availability owner is reported, never fatal.
savepoint before_owner_absent;
drop function public.weekly_source_first_authorisation_withdraw_available_v1(uuid);
do $owner_absent$
declare
  v jsonb;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'an absent availability owner never allows the control');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}','OWNER_ABSENT',
    'an absent availability owner is named');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_WITHDRAW_AVAILABILITY_UNAVAILABLE',
    'an absent availability owner yields an explicit refusal code');
  -- and the whole Office projection still answers.
  perform pg_temp.assert_eq(
    public.weekly_source_office_timesheet_presentation_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','c1000000-0000-4000-8000-000000000001',
      'timesheet_id','cc000000-0000-4000-8000-000000000001'))
    #>>'{action_state,unauthorise,availability_source}','OWNER_ABSENT',
    'the Office projection survives an absent availability owner');
end;
$owner_absent$;
rollback to savepoint before_owner_absent;

-- ---------------------------------------------------------------------------
-- 11a. WP-11d F3.  `available` read from the owner's JSON verdict is
--      THREE-VALUED, and Part 1 standing rule 4 requires absent, null AND
--      non-boolean to take the unsafe branch, with all four cases tested.
--      PostgreSQL accepts 'yes', 'on', '1' and 't' as boolean true, so the
--      previous bare cast granted the Unauthorise control on a string.
-- ---------------------------------------------------------------------------
do $f3_boolean_cast$
begin
  -- The positive control for the defect: PostgreSQL really does cast these.
  perform pg_temp.assert_eq(
    ('yes'::boolean)::text||','||('on'::boolean)::text||','||('1'::boolean)::text
      ||','||('t'::boolean)::text,
    'true,true,true,true',
    'PostgreSQL casts these strings to true, which is why a bare cast is unsafe');
end;
$f3_boolean_cast$;

savepoint before_f3;
create or replace function pg_temp.f3_case(p_verdict text)
returns text language plpgsql as $f3case$
declare
  v jsonb;
begin
  execute pg_catalog.format($stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language sql stable as
    $b$ select %L::jsonb $b$;
  $stub$,p_verdict);
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  return (v->>'unauthorise_allowed')||'/'||(v#>>'{unauthorise,available}');
end;
$f3case$;

do $f3$
begin
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 absent available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":null,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 JSON-null available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":"yes","code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 a NON-BOOLEAN available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":1,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 a numeric available takes the unsafe branch');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":false,"code":"NONE","refusal_nature":"NONE","retryable":false,"reason":"NONE"}'),
    'false/false','F3 an explicit false is refused');
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":true,"code":"NONE","refusal_nature":"NONE","retryable":true,"reason":"NONE"}'),
    'true/true','F3 a genuine boolean true still allows the control');
  -- `retryable` is the same three-valued JSON boolean.
  perform pg_temp.assert_eq(
    pg_temp.f3_case('{"available":true,"retryable":"yes","code":"NONE","refusal_nature":"NONE","reason":"NONE"}'),
    'true/true','F3 a non-boolean retryable does not break the verdict');
end;
$f3$;
rollback to savepoint before_f3;

-- ---------------------------------------------------------------------------
-- 11b. WP-11d F7.  An owner error keeps its cause.  The refusal code itself is
--      unchanged; the SQLSTATE and a bounded message are added so an Office user
--      and an operator are not left with "did not answer".
-- ---------------------------------------------------------------------------
savepoint before_f7;
do $f7$
declare
  v jsonb;
begin
  execute $stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language plpgsql stable as
    $b$ begin raise exception using errcode='42501',
      message='WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED'; end; $b$;
  $stub$;
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v->>'unauthorise_allowed','false',
    'F7 a raising owner never allows the control');
  perform pg_temp.assert_eq(v#>>'{unauthorise,availability_source}','OWNER_ERROR',
    'F7 the failure is still named OWNER_ERROR');
  perform pg_temp.assert_eq(v#>>'{unauthorise,refusal_code}',
    'WEEKLY_SOURCE_WITHDRAW_AVAILABILITY_UNAVAILABLE',
    'F7 the refusal code is unchanged');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_sqlstate}','42501',
    'F7 the dropped SQLSTATE is preserved');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_error}',
    'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED','F7 the owner message is preserved');
  perform pg_temp.assert_true(
    length(v#>>'{unauthorise,owner_error}')<=200,'F7 the message is bounded');
end;
$f7$;
rollback to savepoint before_f7;

savepoint before_f7;
do $f7_bounded$
declare
  v jsonb;
begin
  execute $stub$
    create or replace function public.weekly_source_first_authorisation_withdraw_available_v1(
      p_timesheet_id uuid) returns jsonb language plpgsql stable as
    $b$ begin raise exception using errcode='XX000',
      message=repeat('E',5000); end; $b$;
  $stub$;
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(
    length(v#>>'{unauthorise,owner_error}')::text,'200',
    'F7 a long owner error cannot bloat the projection');
end;
$f7_bounded$;
rollback to savepoint before_f7;

-- A healthy owner carries the NONE placeholders, so the object still has every
-- member non-null and jsonb_strip_nulls at the call site cannot remove one.
do $f7_healthy$
declare
  v jsonb;
  v_nulls integer;
begin
  v:=private.weekly_source_office_unauthorise_action_state_v1(
    'cc000000-0000-4000-8000-000000000001');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_sqlstate}','NONE',
    'a healthy owner reports no SQLSTATE');
  perform pg_temp.assert_eq(v#>>'{unauthorise,owner_error}','NONE',
    'a healthy owner reports no error');
  select count(*)::integer into v_nulls
  from jsonb_each(v->'unauthorise') as member(key,value)
  where jsonb_typeof(member.value)='null';
  perform pg_temp.assert_eq(v_nulls::text,'0',
    'every member of the verdict is non-null');
end;
$f7_healthy$;

rollback;
