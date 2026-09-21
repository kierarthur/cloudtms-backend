-- Rollback-only proof for the correction-session cancel owner,
-- public.weekly_source_correct_final_cancel_atomic_v1.
--
-- Every assertion below EXECUTES the owner. Nothing here inspects
-- pg_get_functiondef to decide whether a path works. The single catalogue
-- assertion (gate 0) exists only to fail loudly when the owner is absent --
-- which is exactly what it does against any database built before WP-59.
--
-- Two scopes are seeded on one cycle:
--   scope 1 (...050) carries the ABANDONED session and proves the release:
--           the Trust and cutoff are blocked before the cancel and free after.
--   scope 2 (...051) carries a fully furnished session and proves every
--           refusal limb, the effects on the staged upload and publication,
--           idempotency, and the two sentinels.
-- The fixture is self-contained: it chains no other verifier, so this gate
-- cannot inherit another package's fixture drift.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is not true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

create function pg_temp.cancel_call(
  p_actor uuid,p_session uuid,p_version bigint,p_key text,p_reason text
) returns text language plpgsql as $function$
declare v jsonb;
begin
  v:=public.weekly_source_correct_final_cancel_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_V1',
      'actor_user_id',p_actor,'correction_session_id',p_session,
      'expected_session_version',p_version,'idempotency_key',p_key,'reason',p_reason));
  return 'OK '||coalesce(v->>'status','?')||' v'||coalesce(v->>'version','?')
         ||' replay='||coalesce(v->>'idempotent_replay','?')
         ||' from='||coalesce(v->>'cancelled_from_state','?')
         ||' rejected='||coalesce(v->>'rejected_upload_count','?')
         ||' superseded='||coalesce(v->>'superseded_upload_count','?')
         ||' publications='||coalesce(v->>'staled_publication_count','?');
exception when others then
  return 'REFUSED '||SQLSTATE||' '||SQLERRM;
end;
$function$;

create function pg_temp.open_call(
  p_actor uuid,p_scope uuid,p_revision uuid,p_manifest_hex text,p_key text,p_reason text
) returns text language plpgsql as $function$
declare v jsonb;
begin
  v:=public.weekly_source_correct_final_open_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      'actor_user_id',p_actor,
      'source_cycle_id','59000000-0000-4000-8000-000000000040',
      'authority_scope_kind','NHSP_REPORT_SCOPE','report_scope_id',p_scope,
      'expected_current_final_revision_id',p_revision,
      'expected_final_manifest_hash',p_manifest_hex,
      'reason',p_reason,'idempotency_key',p_key));
  return 'OK '||coalesce(v->>'status','?')||' replay='
         ||coalesce(v->>'idempotent_replay','?');
exception when others then
  return 'REFUSED '||SQLSTATE||' '||SQLERRM;
end;
$function$;

create function pg_temp.open_session_id(
  p_actor uuid,p_scope uuid,p_revision uuid,p_manifest_hex text,p_key text,p_reason text
) returns uuid language plpgsql as $function$
declare v jsonb;
begin
  v:=public.weekly_source_correct_final_open_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      'actor_user_id',p_actor,
      'source_cycle_id','59000000-0000-4000-8000-000000000040',
      'authority_scope_kind','NHSP_REPORT_SCOPE','report_scope_id',p_scope,
      'expected_current_final_revision_id',p_revision,
      'expected_final_manifest_hash',p_manifest_hex,
      'reason',p_reason,'idempotency_key',p_key));
  return (v->>'correction_session_id')::uuid;
end;
$function$;

-- A census OF the partial unique index's own predicate for one authority
-- scope. One row means that Trust and cutoff are blocked; zero means free. No
-- decision in this file is taken by the index itself.
create function pg_temp.active_rows(p_scope uuid) returns bigint
language sql stable as $function$
  select pg_catalog.count(*)
  from public.weekly_final_source_correction_sessions
  where source_cycle_id='59000000-0000-4000-8000-000000000040'
    and authority_scope_kind='NHSP_REPORT_SCOPE'
    and report_scope_id=p_scope
    and state in ('DRAFT','STAGING','READY','REVIEWED','PREPARING','PREPARED','COMMITTING');
$function$;

-- SENTINEL A -- the one that MUST differ across a successful cancel.
create function pg_temp.session_fingerprint(p_session uuid) returns text
language sql stable as $function$
  select pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WP59_SESSION_SENTINEL',pg_catalog.jsonb_build_object(
      'state',session_row.state,
      'completed',(session_row.completed_at_utc is not null),
      'version',session_row.version,
      'result_hash',pg_catalog.encode(session_row.result_hash,'hex'),
      'uploads',(select coalesce(pg_catalog.string_agg(u.state,'|' order by u.id),'-')
                 from public.weekly_source_uploads u
                 where u.correction_session_id=session_row.id),
      'publications',(select coalesce(pg_catalog.string_agg(
                        p.state||':'||coalesce(p.failure_code,'-'),'|' order by p.id),'-')
                      from public.weekly_source_projection_publications p
                      where p.correction_session_id=session_row.id)
    )),'hex')
  from public.weekly_final_source_correction_sessions session_row
  where session_row.id=p_session;
$function$;

-- SENTINEL B -- the prior authority, which must not move at all.
create function pg_temp.authority_fingerprint(p_revision uuid) returns text
language sql stable as $function$
  select pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WP59_AUTHORITY_SENTINEL',pg_catalog.jsonb_build_object(
      'prior_state',revision.state,
      'prior_manifest',pg_catalog.encode(revision.manifest_hash,'hex'),
      'prior_upload_state',(select u.state from public.weekly_source_uploads u
                            where u.id=revision.upload_id),
      'scope_pointer',scope.current_final_revision_id,
      'scope_publication',scope.current_projection_publication_id,
      'scope_state',scope.state,
      'scope_version',scope.version,
      'cycle_pointer',cycle.current_final_revision_id,
      'cycle_state',cycle.state,
      'revision_count',(select pg_catalog.count(*)
                        from public.weekly_source_final_revisions r
                        where r.source_cycle_id=cycle.id),
      'manifest_count',(select pg_catalog.count(*)
                        from public.weekly_source_client_manifests m
                        where m.source_cycle_id=cycle.id),
      'movement_count',(select pg_catalog.count(*)
                        from public.weekly_source_billing_movements bm
                        where bm.final_revision_id=revision.id),
      'binding_count',(select pg_catalog.count(*)
                       from public.weekly_source_invoice_line_bindings b
                       join public.weekly_source_billing_movements bm
                         on bm.id=b.billing_movement_id
                       where bm.final_revision_id=revision.id),
      'placement_count',(select pg_catalog.count(*)
                         from public.weekly_source_invoice_placements pl
                         join public.weekly_source_billing_movements bm
                           on bm.id=pl.billing_movement_id
                         where bm.final_revision_id=revision.id)
    )),'hex')
  from public.weekly_source_final_revisions revision
  join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
  join public.weekly_source_report_scopes scope on scope.id=revision.report_scope_id
  where revision.id=p_revision;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 0. The owner must be installed and correctly sealed. This is the
-- assertion that fails against every pre-WP-59 database.
-- ---------------------------------------------------------------------------
select pg_temp.assert_true(
  pg_catalog.to_regprocedure(
    'public.weekly_source_correct_final_cancel_atomic_v1(jsonb)') is not null,
  'the correction-session cancel owner public.weekly_source_correct_final_cancel_atomic_v1(jsonb) '
  'is not installed: an abandoned correction session has no exit and holds its Trust and cutoff for ever'
);
select pg_temp.assert_true(
  (select p.prosecdef
     and exists(select 1 from pg_catalog.unnest(coalesce(p.proconfig,'{}'::text[])) setting
                where setting like 'search\_path=%' escape '\')
     and pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
     and not pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
     and not pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
   from pg_catalog.pg_proc p
   where p.oid=pg_catalog.to_regprocedure(
     'public.weekly_source_correct_final_cancel_atomic_v1(jsonb)')),
  'the cancel owner must be SECURITY DEFINER with a fixed search_path, executable by service_role only'
);

-- ---------------------------------------------------------------------------
-- Fixture
-- ---------------------------------------------------------------------------
insert into public.tms_users(id,email,role,is_active,password_hash) values
  ('59000000-0000-4000-8000-000000000001','wp59.actor.a@example.invalid','admin',true,'x'),
  ('59000000-0000-4000-8000-000000000002','wp59.actor.b@example.invalid','admin',true,'x'),
  ('59000000-0000-4000-8000-000000000003','wp59.actor.c@example.invalid','user',true,'x'),
  ('59000000-0000-4000-8000-000000000004','wp59.actor.d@example.invalid','admin',false,'x');
insert into public.clients(id,name) values
  ('59000000-0000-4000-8000-000000000010','WP59 Trust');
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
  cutoff_local_time,nhsp_report_heading_name
) values (
  '59000000-0000-4000-8000-000000000020','TEST','59000000-0000-4000-8000-0000000000aa',
  'WP59_NHSP','WP59 NHSP Group','NHSP',3,'15:00','WP59 Trust Backing Report');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from)
values ('59000000-0000-4000-8000-000000000020','59000000-0000-4000-8000-000000000010','2020-01-01');
insert into public.weekly_source_format_profiles(
  id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
  row_finalisation_capability,worked_duration_authority,report_number_required,
  fmc_must_equal_zero,physical_negative_meaning,profile_json,profile_sha256
) values (
  '59000000-0000-4000-8000-000000000030','WP59_NHSP_PROFILE',1,
  'NHSP_TRUST_BACKING_REPORT','XLSX','NO_INFERENCE','NONE','SOURCE_ACTUAL',true,
  true,'NHSP_FULL_REVERSAL','{}'::jsonb,pg_catalog.decode(pg_catalog.repeat('59',32),'hex'));
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  finalised_at_utc,finalised_by_user_id
) values (
  '59000000-0000-4000-8000-000000000040','59000000-0000-4000-8000-000000000020',
  '2026-03-08','2026-03-09T15:00:00Z','FINALISED',1,
  '2026-03-10T09:00:00Z','59000000-0000-4000-8000-000000000001');

insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
  version,state
) values
  ('59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000040','TEST',
   '59000000-0000-4000-8000-0000000000aa','59000000-0000-4000-8000-000000000020',
   '59000000-0000-4000-8000-000000000010','2026-03-09T15:00:00Z',1,'FINALISED'),
  ('59000000-0000-4000-8000-000000000051','59000000-0000-4000-8000-000000000040','TEST',
   '59000000-0000-4000-8000-0000000000aa','59000000-0000-4000-8000-000000000020',
   '59000000-0000-4000-8000-000000000010','2026-03-16T15:00:00Z',1,'FINALISED');

insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,purpose,state,uploaded_by_user_id
) values
  ('59000000-0000-4000-8000-000000000060','59000000-0000-4000-8000-000000000040',
   '59000000-0000-4000-8000-000000000050','wp59-prior-1.xlsx',
   pg_catalog.decode(pg_catalog.repeat('60',32),'hex'),1024,
   '59000000-0000-4000-8000-000000000030','p1','n1',
   pg_catalog.decode(pg_catalog.repeat('61',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('62',32),'hex'),
   'NHSP_TRUST_REPORT_SCOPE',1,1,
   pg_catalog.decode(pg_catalog.repeat('63',32),'hex'),
   'ORDINARY','CURRENT','59000000-0000-4000-8000-000000000001'),
  ('59000000-0000-4000-8000-000000000061','59000000-0000-4000-8000-000000000040',
   '59000000-0000-4000-8000-000000000051','wp59-prior-2.xlsx',
   pg_catalog.decode(pg_catalog.repeat('64',32),'hex'),1024,
   '59000000-0000-4000-8000-000000000030','p1','n1',
   pg_catalog.decode(pg_catalog.repeat('65',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('66',32),'hex'),
   'NHSP_TRUST_REPORT_SCOPE',1,1,
   pg_catalog.decode(pg_catalog.repeat('67',32),'hex'),
   'ORDINARY','CURRENT','59000000-0000-4000-8000-000000000001');

insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,report_scope_id,revision_number,upload_id,
  reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state
) values
  ('59000000-0000-4000-8000-000000000070','59000000-0000-4000-8000-000000000040',
   'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000050',1,
   '59000000-0000-4000-8000-000000000060','INITIAL_FINALISATION',
   '59000000-0000-4000-8000-000000000001',
   pg_catalog.decode(pg_catalog.repeat('70',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('71',32),'hex'),'CURRENT'),
  ('59000000-0000-4000-8000-000000000071','59000000-0000-4000-8000-000000000040',
   'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000051',1,
   '59000000-0000-4000-8000-000000000061','INITIAL_FINALISATION',
   '59000000-0000-4000-8000-000000000001',
   pg_catalog.decode(pg_catalog.repeat('72',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('73',32),'hex'),'CURRENT');

insert into public.weekly_source_client_manifests(
  id,final_revision_id,source_group_id,source_cycle_id,client_id,
  finalisation_week_ending,manifest_hash,movement_count
) values
  ('59000000-0000-4000-8000-000000000080','59000000-0000-4000-8000-000000000070',
   '59000000-0000-4000-8000-000000000020','59000000-0000-4000-8000-000000000040',
   '59000000-0000-4000-8000-000000000010','2026-03-08',
   pg_catalog.decode(pg_catalog.repeat('80',32),'hex'),0),
  ('59000000-0000-4000-8000-000000000081','59000000-0000-4000-8000-000000000071',
   '59000000-0000-4000-8000-000000000020','59000000-0000-4000-8000-000000000040',
   '59000000-0000-4000-8000-000000000010','2026-03-08',
   pg_catalog.decode(pg_catalog.repeat('81',32),'hex'),0);

insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values
  ('59000000-0000-4000-8000-000000000090','59000000-0000-4000-8000-000000000040',
   'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000050',
   '59000000-0000-4000-8000-000000000060',1,
   pg_catalog.decode(pg_catalog.repeat('90',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('91',32),'hex'),'CURRENT',
   pg_catalog.transaction_timestamp()),
  ('59000000-0000-4000-8000-000000000091','59000000-0000-4000-8000-000000000040',
   'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000051',
   '59000000-0000-4000-8000-000000000061',1,
   pg_catalog.decode(pg_catalog.repeat('92',32),'hex'),
   pg_catalog.decode(pg_catalog.repeat('93',32),'hex'),'CURRENT',
   pg_catalog.transaction_timestamp());

update public.weekly_source_report_scopes
set current_complete_upload_id='59000000-0000-4000-8000-000000000060',
    current_final_revision_id='59000000-0000-4000-8000-000000000070',
    current_projection_publication_id='59000000-0000-4000-8000-000000000090',
    projection_state='CURRENT'
where id='59000000-0000-4000-8000-000000000050';
update public.weekly_source_report_scopes
set current_complete_upload_id='59000000-0000-4000-8000-000000000061',
    current_final_revision_id='59000000-0000-4000-8000-000000000071',
    current_projection_publication_id='59000000-0000-4000-8000-000000000091',
    projection_state='CURRENT'
where id='59000000-0000-4000-8000-000000000051';

create temporary table pg_temp_wp59(label text primary key,value text) on commit drop;

-- ---------------------------------------------------------------------------
-- 1. The block, present. Actor A opens on scope 1 and walks away.
-- ---------------------------------------------------------------------------
insert into pg_temp_wp59(label,value)
values ('abandoned',pg_temp.open_session_id(
  '59000000-0000-4000-8000-000000000001','59000000-0000-4000-8000-000000000050',
  '59000000-0000-4000-8000-000000000070',pg_catalog.repeat('70',32),
  'wp59-open-1','WP59 abandoned correction')::text);

select pg_temp.assert_true(pg_temp.active_rows('59000000-0000-4000-8000-000000000050')=1,
  'BEFORE: the abandoned session must hold the active scope index for this Trust and cutoff');
select pg_temp.assert_true(
  pg_temp.open_call('59000000-0000-4000-8000-000000000002',
    '59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000070',
    pg_catalog.repeat('70',32),'wp59-open-2','WP59 second actor')
    ='REFUSED 55000 WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS',
  'BEFORE: a second active administrator must be blocked while the abandoned session lives');
select pg_temp.assert_true(
  pg_temp.open_call('59000000-0000-4000-8000-000000000001',
    '59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000070',
    pg_catalog.repeat('70',32),'wp59-open-3','WP59 retry by the opener')
    ='REFUSED 55000 WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS',
  'BEFORE: the opening actor must be blocked from re-opening while the abandoned session lives');

insert into pg_temp_wp59(label,value) values
  ('session_before',pg_temp.session_fingerprint(
     (select value::uuid from pg_temp_wp59 where label='abandoned'))),
  ('authority_before',pg_temp.authority_fingerprint(
     '59000000-0000-4000-8000-000000000070'));

-- ---------------------------------------------------------------------------
-- 2. Refusal limbs that do not need a furnished session. Each is driven, each
--    carries its own reason, and none of them may touch the session.
-- ---------------------------------------------------------------------------
do $refusals$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='abandoned');
  v_result text;
begin
  begin
    perform public.weekly_source_correct_final_cancel_atomic_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_V1',
        'actor_user_id','59000000-0000-4000-8000-000000000001',
        'correction_session_id',v_session,'expected_session_version',1,
        'idempotency_key','k','reason','r','unexpected_key','x'));
    raise exception 'ASSERTION_FAILED: an unexpected request key was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_CANCEL_REQUEST_INVALID' then raise; end if;
  end;
  begin
    perform public.weekly_source_correct_final_cancel_atomic_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_CANCEL_WRONG',
        'actor_user_id','59000000-0000-4000-8000-000000000001',
        'correction_session_id',v_session,'expected_session_version',1,
        'idempotency_key','k','reason','r'));
    raise exception 'ASSERTION_FAILED: a wrong schema_version was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_CANCEL_REQUEST_INVALID' then raise; end if;
  end;
  begin
    perform public.weekly_source_correct_final_cancel_atomic_v1('[]'::jsonb);
    raise exception 'ASSERTION_FAILED: a non-object request was accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_CANCEL_REQUEST_INVALID' then raise; end if;
  end;

  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,1,'k','');
  if v_result<>'REFUSED 22023 WEEKLY_SOURCE_CORRECTION_CANCEL_VALUE_INVALID' then
    raise exception 'ASSERTION_FAILED: an empty reason was not refused (%)',v_result;
  end if;
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,0,'k','r');
  if v_result<>'REFUSED 22023 WEEKLY_SOURCE_CORRECTION_CANCEL_VALUE_INVALID' then
    raise exception 'ASSERTION_FAILED: expected_session_version 0 was not refused (%)',v_result;
  end if;
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,1,'','r');
  if v_result<>'REFUSED 22023 WEEKLY_SOURCE_CORRECTION_CANCEL_VALUE_INVALID' then
    raise exception 'ASSERTION_FAILED: an empty idempotency key was not refused (%)',v_result;
  end if;

  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',
    '59000000-0000-4000-8000-0000000000ff',1,'k','r');
  if v_result<>'REFUSED 22023 WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' then
    raise exception 'ASSERTION_FAILED: an unknown session was not refused (%)',v_result;
  end if;

  -- compare-and-swap: a stale expected_session_version loses and is told why.
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,99,'k','r');
  if v_result<>'REFUSED 40001 WEEKLY_SOURCE_CORRECTION_SESSION_STALE' then
    raise exception 'ASSERTION_FAILED: a stale expected_session_version was not refused (%)',v_result;
  end if;

  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000003',v_session,1,'k','r');
  if v_result<>'REFUSED 42501 WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' then
    raise exception 'ASSERTION_FAILED: a non-admin actor was not refused (%)',v_result;
  end if;
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000004',v_session,1,'k','r');
  if v_result<>'REFUSED 42501 WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' then
    raise exception 'ASSERTION_FAILED: an inactive actor was not refused (%)',v_result;
  end if;

  -- D-WP59-1: a second active administrator who holds CORRECT_FINAL_SOURCE is
  -- still not the opening actor, and may not cancel.
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000002',v_session,1,'k','r');
  if v_result<>'REFUSED 42501 WEEKLY_SOURCE_CORRECTION_CANCEL_ACTOR_NOT_OPENER' then
    raise exception 'ASSERTION_FAILED: a second administrator was allowed to cancel (%)',v_result;
  end if;
end;
$refusals$;

select pg_temp.assert_true(
  pg_temp.session_fingerprint((select value::uuid from pg_temp_wp59 where label='abandoned'))
    =(select value from pg_temp_wp59 where label='session_before'),
  'no refused cancel may change the session');
select pg_temp.assert_true(pg_temp.active_rows('59000000-0000-4000-8000-000000000050')=1,
  'no refused cancel may release the scope');

-- ---------------------------------------------------------------------------
-- 3. Scope 2. A furnished session: a staged correction upload and a correction
--    projection publication, so the state limbs and the effects can both be
--    driven on a session that has real artefacts.
-- ---------------------------------------------------------------------------
insert into pg_temp_wp59(label,value)
values ('furnished',pg_temp.open_session_id(
  '59000000-0000-4000-8000-000000000001','59000000-0000-4000-8000-000000000051',
  '59000000-0000-4000-8000-000000000071',pg_catalog.repeat('72',32),
  'wp59-open-s2','WP59 furnished correction')::text);

insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,purpose,correction_session_id,state,uploaded_by_user_id
)
select '59000000-0000-4000-8000-0000000000c1','59000000-0000-4000-8000-000000000040',
  '59000000-0000-4000-8000-000000000051','wp59-replacement.xlsx',
  pg_catalog.decode(pg_catalog.repeat('c1',32),'hex'),2048,
  '59000000-0000-4000-8000-000000000030','p1','n1',
  pg_catalog.decode(pg_catalog.repeat('c2',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c3',32),'hex'),
  'NHSP_TRUST_REPORT_SCOPE',1,1,
  pg_catalog.decode(pg_catalog.repeat('c4',32),'hex'),
  'FINAL_SOURCE_CORRECTION',(select value::uuid from pg_temp_wp59 where label='furnished'),
  'CORRECTION_READY','59000000-0000-4000-8000-000000000001';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,correction_session_id,comparison_manifest_hash,
  issue_set_hash,state
)
select '59000000-0000-4000-8000-0000000000c5','59000000-0000-4000-8000-000000000040',
  'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000051',
  '59000000-0000-4000-8000-0000000000c1',1,
  (select value::uuid from pg_temp_wp59 where label='furnished'),
  pg_catalog.decode(pg_catalog.repeat('c6',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('c7',32),'hex'),'CORRECTION_READY';
-- an inactive PREPARED replacement revision, as PREPARE would leave it
insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,report_scope_id,revision_number,upload_id,
  predecessor_revision_id,reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state
) values (
  '59000000-0000-4000-8000-0000000000c8','59000000-0000-4000-8000-000000000040',
  'NHSP_REPORT_SCOPE','59000000-0000-4000-8000-000000000051',2,
  '59000000-0000-4000-8000-0000000000c1','59000000-0000-4000-8000-000000000071',
  'CORRECT_FINAL_SOURCE','59000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('c9',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('ca',32),'hex'),'PREPARED');
update public.weekly_final_source_correction_sessions
set replacement_correction_upload_id='59000000-0000-4000-8000-0000000000c1',
    replacement_projection_publication_id='59000000-0000-4000-8000-0000000000c5',
    review_idempotency_key='wp59-review',
    review_request_hash=pg_catalog.decode(pg_catalog.repeat('cb',32),'hex'),
    review_result_json='{"ok": true}'::jsonb,
    review_result_hash=pg_catalog.decode(pg_catalog.repeat('cc',32),'hex'),
    state='REVIEWED',version=version+1
where id=(select value::uuid from pg_temp_wp59 where label='furnished');

-- Each probe moves the furnished session into one state inside a subtransaction,
-- drives the REAL owner, remembers its answer in a plpgsql variable (which
-- survives the rollback), then discards the fixture change.
do $states$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='furnished');
  v_version bigint:=(select version from public.weekly_final_source_correction_sessions
                     where id=v_session);
  v_result text;
begin
  -- PREPARED: a replacement revision has been materialised.
  begin
    update public.weekly_final_source_correction_sessions
    set prepared_final_revision_id='59000000-0000-4000-8000-0000000000c8',
        prepare_idempotency_key='wp59-prepare',
        prepare_request_hash=pg_catalog.decode(pg_catalog.repeat('cd',32),'hex'),
        prepare_result_json='{"ok": true}'::jsonb,
        prepare_result_hash=pg_catalog.decode(pg_catalog.repeat('ce',32),'hex'),
        state='PREPARED'
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-prepared','WP59 cancel a prepared session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_PREPARED_REVISION_EXISTS' then
    raise exception 'ASSERTION_FAILED: a PREPARED session was not refused (%)',v_result;
  end if;

  -- A prepared revision recorded while the state has moved back: cancel must
  -- refuse rather than guess what the session already committed.
  begin
    update public.weekly_final_source_correction_sessions
    set prepared_final_revision_id='59000000-0000-4000-8000-0000000000c8'
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-prepid','WP59 cancel with a recorded prepared revision');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_PREPARED_REVISION_EXISTS' then
    raise exception 'ASSERTION_FAILED: a recorded prepared revision was not refused (%)',v_result;
  end if;

  -- PREPARING: in flight, and what it has materialised cannot be established.
  begin
    update public.weekly_final_source_correction_sessions
    set state='PREPARING' where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-preparing','WP59 cancel a preparing session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_PREPARE_IN_FLIGHT' then
    raise exception 'ASSERTION_FAILED: a PREPARING session was not refused (%)',v_result;
  end if;

  -- COMMITTING: a correction transaction is in flight.
  begin
    update public.weekly_final_source_correction_sessions
    set prepared_final_revision_id='59000000-0000-4000-8000-0000000000c8',
        prepare_idempotency_key='wp59-prepare',
        prepare_request_hash=pg_catalog.decode(pg_catalog.repeat('cd',32),'hex'),
        prepare_result_json='{"ok": true}'::jsonb,
        prepare_result_hash=pg_catalog.decode(pg_catalog.repeat('ce',32),'hex'),
        apply_idempotency_key='wp59-apply',
        apply_request_hash=pg_catalog.decode(pg_catalog.repeat('cf',32),'hex'),
        state='COMMITTING'
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-committing','WP59 cancel a committing session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_COMMIT_IN_FLIGHT' then
    raise exception 'ASSERTION_FAILED: a COMMITTING session was not refused (%)',v_result;
  end if;

  -- APPLIED: the correction has already replaced the authority.
  begin
    update public.weekly_source_final_revisions
    set state='SUPERSEDED' where id='59000000-0000-4000-8000-000000000071';
    update public.weekly_source_final_revisions
    set state='CURRENT' where id='59000000-0000-4000-8000-0000000000c8';
    update public.weekly_final_source_correction_sessions
    set prepared_final_revision_id='59000000-0000-4000-8000-0000000000c8',
        applied_final_revision_id='59000000-0000-4000-8000-0000000000c8',
        prepare_idempotency_key='wp59-prepare',
        prepare_request_hash=pg_catalog.decode(pg_catalog.repeat('cd',32),'hex'),
        prepare_result_json='{"ok": true}'::jsonb,
        prepare_result_hash=pg_catalog.decode(pg_catalog.repeat('ce',32),'hex'),
        apply_idempotency_key='wp59-apply',
        apply_request_hash=pg_catalog.decode(pg_catalog.repeat('cf',32),'hex'),
        result_json='{"ok": true}'::jsonb,
        result_hash=pg_catalog.decode(pg_catalog.repeat('d0',32),'hex'),
        state='APPLIED',completed_at_utc=pg_catalog.transaction_timestamp()
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-applied','WP59 cancel an applied session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_ALREADY_APPLIED' then
    raise exception 'ASSERTION_FAILED: an APPLIED session was not refused (%)',v_result;
  end if;

  -- CANCELLED, with a key that is not the one that cancelled it.
  begin
    update public.weekly_final_source_correction_sessions
    set state='CANCELLED',completed_at_utc=pg_catalog.transaction_timestamp()
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-other-key','WP59 cancel an already cancelled session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_ALREADY_CANCELLED' then
    raise exception 'ASSERTION_FAILED: an already cancelled session was not refused (%)',v_result;
  end if;

  -- FAILED.
  begin
    update public.weekly_final_source_correction_sessions
    set state='FAILED',completed_at_utc=pg_catalog.transaction_timestamp()
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-failed','WP59 cancel a failed session');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_ALREADY_TERMINAL' then
    raise exception 'ASSERTION_FAILED: a FAILED session was not refused (%)',v_result;
  end if;

  -- The prior authority has moved under the session.
  begin
    update public.weekly_source_report_scopes
    set current_final_revision_id=null where id='59000000-0000-4000-8000-000000000051';
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-stale','WP59 cancel against a moved authority');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 40001 WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' then
    raise exception 'ASSERTION_FAILED: a moved authority pointer was not refused (%)',v_result;
  end if;

  -- A correction descendant exists, so the cancel could not release the scope
  -- even if it succeeded. It says so instead of pretending otherwise.
  begin
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-descendant','WP59 cancel with a descendant present');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS' then
    raise exception 'ASSERTION_FAILED: an existing correction descendant was not refused (%)',v_result;
  end if;
end;
$states$;

-- The descendant revision was seeded only for the state probes above; the
-- remaining scope-2 proofs need the session to be cancellable, so it goes.
delete from public.weekly_source_final_revisions
where id='59000000-0000-4000-8000-0000000000c8';

do $replacement_stale$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='furnished');
  v_version bigint:=(select version from public.weekly_final_source_correction_sessions
                     where id=v_session);
  v_result text;
begin
  -- A staged correction upload that has been promoted past this session.
  begin
    update public.weekly_source_uploads
    set state='SEALED' where id='59000000-0000-4000-8000-0000000000c1';
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-upload','WP59 cancel with a promoted upload');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 40001 WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' then
    raise exception 'ASSERTION_FAILED: a promoted correction upload was not refused (%)',v_result;
  end if;

  -- A correction publication that has been promoted to CURRENT.
  begin
    update public.weekly_source_projection_publications
    set state='STALE' where id='59000000-0000-4000-8000-000000000091';
    update public.weekly_source_projection_publications
    set state='CURRENT',published_at_utc=pg_catalog.transaction_timestamp()
    where id='59000000-0000-4000-8000-0000000000c5';
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-publication','WP59 cancel with a promoted publication');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 40001 WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' then
    raise exception 'ASSERTION_FAILED: a promoted correction publication was not refused (%)',v_result;
  end if;

  -- A pointer that does not resolve to a row bound back to this session. The
  -- disposal loops key on correction_session_id, so this is the shape in which
  -- a cancel could report success while leaving live replacement work behind.
  begin
    update public.weekly_final_source_correction_sessions
    set replacement_correction_upload_id='59000000-0000-4000-8000-000000000061'
    where id=v_session;
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-unbound','WP59 cancel with an unbound replacement pointer');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 40001 WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' then
    raise exception 'ASSERTION_FAILED: an unbound replacement pointer was not refused (%)',v_result;
  end if;
end;
$replacement_stale$;

-- A cancel that removes nothing where something was expected is a refusal, not
-- a success. Injected fault: a trigger that silently swallows the update to the
-- staged correction upload, exactly the shape of a clean return that does
-- nothing. The owner must notice and refuse rather than report CANCELLED.
create function pg_temp.wp59_swallow_upload_update() returns trigger
language plpgsql as $function$
begin
  if OLD.id='59000000-0000-4000-8000-0000000000c1' then
    return null;
  end if;
  return NEW;
end;
$function$;

do $silent_success$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='furnished');
  v_version bigint:=(select version from public.weekly_final_source_correction_sessions
                     where id=v_session);
  v_result text;
begin
  begin
    execute 'create trigger wp59_swallow_upload_update before update on '
            'public.weekly_source_uploads for each row execute function '
            'pg_temp.wp59_swallow_upload_update()';
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
      v_version,'wp59-c-silent','WP59 cancel whose disposal is swallowed');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_CANCEL_INCOMPLETE' then
    raise exception 'ASSERTION_FAILED: a cancel that disposed of nothing reported success (%)',v_result;
  end if;
end;
$silent_success$;

-- ---------------------------------------------------------------------------
-- 4. The furnished session cancels from REVIEWED, and its staged work becomes
--    audit history exactly as pack 05 CFS-015 requires -- rejected and stale,
--    never deleted.
-- ---------------------------------------------------------------------------
do $cancel_furnished$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='furnished');
  v_version bigint:=(select version from public.weekly_final_source_correction_sessions
                     where id=v_session);
  v_result text;
begin
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
    v_version,'wp59-cancel-s2','WP59 abandon the furnished correction');
  if v_result<>'OK CANCELLED v'||(v_version+1)::text
       ||' replay=false from=REVIEWED rejected=0 superseded=1 publications=1' then
    raise exception 'ASSERTION_FAILED: cancel from REVIEWED did not succeed as stated (%)',v_result;
  end if;
  -- an exact replay returns the sealed result and writes nothing new
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
    v_version,'wp59-cancel-s2','WP59 abandon the furnished correction');
  if v_result<>'OK CANCELLED v'||(v_version+1)::text
       ||' replay=true from=REVIEWED rejected=0 superseded=1 publications=1' then
    raise exception 'ASSERTION_FAILED: the cancel was not idempotent on its own key (%)',v_result;
  end if;
  -- the same key with a different request is a collision, not a replay
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,
    v_version,'wp59-cancel-s2','WP59 a different reason under the same key');
  if v_result<>'REFUSED 22023 WEEKLY_SOURCE_CORRECTION_CANCEL_IDEMPOTENCY_COLLISION' then
    raise exception 'ASSERTION_FAILED: a changed cancel request reused its key (%)',v_result;
  end if;
end;
$cancel_furnished$;

select pg_temp.assert_true(
  (select state='SUPERSEDED' and row_manifest_hash is not null
   from public.weekly_source_uploads
   where id='59000000-0000-4000-8000-0000000000c1'),
  'the sealed correction upload must become audit-only SUPERSEDED with its row manifest hash intact, not be deleted and not have its evidence erased');
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_upload_attempts
   where logical_upload_id='59000000-0000-4000-8000-0000000000c1'
     and result='REJECTED' and reason_code='CORRECTION_SESSION_CANCELLED'),
  'the cancel must append exactly one typed CORRECTION_SESSION_CANCELLED upload attempt');
select pg_temp.assert_true(
  (select state='STALE' and failure_code='CORRECTION_SESSION_CANCELLED'
   from public.weekly_source_projection_publications
   where id='59000000-0000-4000-8000-0000000000c5'),
  'the correction projection publication must become STALE with its typed failure code');
select pg_temp.assert_true(
  (select state='CURRENT' from public.weekly_source_final_revisions
   where id='59000000-0000-4000-8000-000000000071'),
  'the prior final authority of the cancelled scope must still be CURRENT');
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.audit_events
   where object_type='weekly_final_source_correction_sessions'
     and object_id_text=(select value from pg_temp_wp59 where label='furnished')
     and action='WEEKLY_SOURCE_CORRECT_FINAL_CANCELLED'
     and reason='WP59 abandon the furnished correction'
     and actor_user_id='59000000-0000-4000-8000-000000000001'),
  'the cancel must append exactly one audit event naming the actor and the reason');
select pg_temp.assert_true(pg_temp.active_rows('59000000-0000-4000-8000-000000000051')=0,
  'the furnished session must release its own Trust and cutoff');

-- ---------------------------------------------------------------------------
-- 5. The release. The abandoned DRAFT session on scope 1 is cancelled, the
--    block disappears, and a new correction opens normally.
-- ---------------------------------------------------------------------------
do $release$
declare
  v_session uuid:=(select value::uuid from pg_temp_wp59 where label='abandoned');
  v_result text;
begin
  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,1,
    'wp59-cancel-1','WP59 the opener abandons the correction');
  if v_result<>'OK CANCELLED v2 replay=false from=DRAFT rejected=0 superseded=0 publications=0' then
    raise exception 'ASSERTION_FAILED: the abandoned session did not cancel (%)',v_result;
  end if;
end;
$release$;

select pg_temp.assert_true(pg_temp.active_rows('59000000-0000-4000-8000-000000000050')=0,
  'AFTER: the block must be gone -- no row remains inside the active scope index');
select pg_temp.assert_true(
  pg_temp.open_call('59000000-0000-4000-8000-000000000001',
    '59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000070',
    pg_catalog.repeat('70',32),'wp59-open-4','WP59 the correction that can now run')
    ='OK DRAFT replay=false',
  'AFTER: a new correction session must open for the released Trust and cutoff');
select pg_temp.assert_true(
  pg_temp.open_call('59000000-0000-4000-8000-000000000002',
    '59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000070',
    pg_catalog.repeat('70',32),'wp59-open-5','WP59 someone else again')
    ='REFUSED 55000 WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS',
  'AFTER: the newly opened session must hold the scope in its turn');

-- ---------------------------------------------------------------------------
-- 6. The sentinels. One is planted to differ and must differ; the other is the
--    prior money authority and must not have moved by one byte.
-- ---------------------------------------------------------------------------
select pg_temp.assert_true(
  pg_temp.session_fingerprint((select value::uuid from pg_temp_wp59 where label='abandoned'))
    <>(select value from pg_temp_wp59 where label='session_before'),
  'SENTINEL A: the cancelled session fingerprint must differ from its pre-cancel value '
  '-- if it does not, this gate is asserting nothing');
select pg_temp.assert_true(
  pg_temp.authority_fingerprint('59000000-0000-4000-8000-000000000070')
    =(select value from pg_temp_wp59 where label='authority_before'),
  'SENTINEL B: the prior final source authority, its pointers, revisions, manifests, '
  'movements, bindings and placements must be byte-identical across a cancel');

-- ---------------------------------------------------------------------------
-- 7. WP-59's change to the OPEN replay lookup. A terminal session is never
--    replayed as if it were live: re-sending the byte-identical request that
--    opened the now-cancelled session is refused by name instead of returning
--    that cancelled session as an idempotent replay.
-- ---------------------------------------------------------------------------
select pg_temp.assert_true(
  pg_temp.open_call('59000000-0000-4000-8000-000000000001',
    '59000000-0000-4000-8000-000000000050','59000000-0000-4000-8000-000000000070',
    pg_catalog.repeat('70',32),'wp59-open-1','WP59 abandoned correction')
    ='REFUSED 22023 WEEKLY_SOURCE_CORRECTION_OPEN_SESSION_TERMINAL',
  'the request that opened a cancelled session must not replay it as a live one');

-- ---------------------------------------------------------------------------
-- 8. The CYCLE authority scope. Everything above is NHSP_REPORT_SCOPE, which
--    leaves the owner's other branch -- the one that has no report scope row to
--    lock and has to resolve the client from the revision's manifest -- undriven.
--    Standing rule 17: a guard nobody drives is not a guard. This section drives
--    the CYCLE branch, its release, and its fail-closed manifest-cardinality
--    refusal.
-- ---------------------------------------------------------------------------
insert into public.clients(id,name) values
  ('59000000-0000-4000-8000-000000000011','WP59 Second Client'),
  ('59000000-0000-4000-8000-000000000012','WP59 Third Client');
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  '59000000-0000-4000-8000-000000000021','TEST','59000000-0000-4000-8000-0000000000aa',
  'WP59_ROSTER','WP59 Roster Group','ROSTER',3,'15:00');
-- a client belongs to at most one source group at a time
-- (weekly_source_group_clients_no_overlap), so the ROSTER group gets its own two.
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from) values
  ('59000000-0000-4000-8000-000000000021','59000000-0000-4000-8000-000000000011','2020-01-01'),
  ('59000000-0000-4000-8000-000000000021','59000000-0000-4000-8000-000000000012','2020-01-01');
insert into public.weekly_source_format_profiles(
  id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
  row_finalisation_capability,worked_duration_authority,profile_json,profile_sha256
) values (
  '59000000-0000-4000-8000-000000000031','WP59_ROSTER_PROFILE',1,
  'GENERIC_COMPLETE_SNAPSHOT','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE','NONE',
  'SOURCE_ACTUAL','{}'::jsonb,pg_catalog.decode(pg_catalog.repeat('31',32),'hex'));
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  finalised_at_utc,finalised_by_user_id
) values (
  '59000000-0000-4000-8000-000000000041','59000000-0000-4000-8000-000000000021',
  '2026-03-15','2026-03-16T15:00:00Z','FINALISED',1,
  '2026-03-17T09:00:00Z','59000000-0000-4000-8000-000000000001');
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,purpose,state,uploaded_by_user_id
) values (
  '59000000-0000-4000-8000-000000000068','59000000-0000-4000-8000-000000000041',
  'wp59-cycle-prior.xlsx',pg_catalog.decode(pg_catalog.repeat('68',32),'hex'),1024,
  '59000000-0000-4000-8000-000000000031','p1','n1',
  pg_catalog.decode(pg_catalog.repeat('69',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('6a',32),'hex'),
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,
  pg_catalog.decode(pg_catalog.repeat('6b',32),'hex'),
  'ORDINARY','CURRENT','59000000-0000-4000-8000-000000000001');
insert into public.weekly_source_final_revisions(
  id,source_cycle_id,authority_scope_kind,revision_number,upload_id,
  coverage_start_local_date,coverage_end_local_date,coverage_timezone,
  reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state
) values (
  '59000000-0000-4000-8000-000000000078','59000000-0000-4000-8000-000000000041',
  'CYCLE',1,'59000000-0000-4000-8000-000000000068',
  '2026-03-09','2026-03-15','Europe/London','INITIAL_FINALISATION',
  '59000000-0000-4000-8000-000000000001',
  pg_catalog.decode(pg_catalog.repeat('78',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('79',32),'hex'),'CURRENT');
insert into public.weekly_source_client_manifests(
  id,final_revision_id,source_group_id,source_cycle_id,client_id,
  finalisation_week_ending,manifest_hash,movement_count
) values (
  '59000000-0000-4000-8000-000000000088','59000000-0000-4000-8000-000000000078',
  '59000000-0000-4000-8000-000000000021','59000000-0000-4000-8000-000000000041',
  '59000000-0000-4000-8000-000000000011','2026-03-15',
  pg_catalog.decode(pg_catalog.repeat('88',32),'hex'),0);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  '59000000-0000-4000-8000-000000000098','59000000-0000-4000-8000-000000000041',
  'CYCLE','59000000-0000-4000-8000-000000000068',1,
  pg_catalog.decode(pg_catalog.repeat('98',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('99',32),'hex'),'CURRENT',
  pg_catalog.transaction_timestamp());
update public.weekly_source_cycles
set current_complete_upload_id='59000000-0000-4000-8000-000000000068',
    current_final_revision_id='59000000-0000-4000-8000-000000000078',
    current_projection_publication_id='59000000-0000-4000-8000-000000000098',
    projection_state='CURRENT'
where id='59000000-0000-4000-8000-000000000041';

create function pg_temp.cycle_open(p_key text,p_reason text) returns text
language plpgsql as $function$
declare v jsonb;
begin
  v:=public.weekly_source_correct_final_open_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      'actor_user_id','59000000-0000-4000-8000-000000000001',
      'source_cycle_id','59000000-0000-4000-8000-000000000041',
      'authority_scope_kind','CYCLE','report_scope_id',null,
      'expected_current_final_revision_id','59000000-0000-4000-8000-000000000078',
      'expected_final_manifest_hash',pg_catalog.repeat('78',32),
      'reason',p_reason,'idempotency_key',p_key));
  return coalesce(v->>'correction_session_id','?');
exception when others then
  return 'REFUSED '||SQLSTATE||' '||SQLERRM;
end;
$function$;

create function pg_temp.cycle_active_rows() returns bigint
language sql stable as $function$
  select pg_catalog.count(*)
  from public.weekly_final_source_correction_sessions
  where source_cycle_id='59000000-0000-4000-8000-000000000041'
    and authority_scope_kind='CYCLE' and report_scope_id is null
    and state in ('DRAFT','STAGING','READY','REVIEWED','PREPARING','PREPARED','COMMITTING');
$function$;

do $cycle_scope$
declare
  v_session uuid;
  v_result text;
begin
  v_session:=pg_temp.cycle_open('wp59-cycle-1','WP59 cycle-scope correction')::uuid;
  if pg_temp.cycle_active_rows()<>1 then
    raise exception 'ASSERTION_FAILED: the CYCLE-scope session did not take the scope';
  end if;

  -- fail closed when the revision's client scope is not exactly one manifest
  begin
    insert into public.weekly_source_client_manifests(
      id,final_revision_id,source_group_id,source_cycle_id,client_id,
      finalisation_week_ending,manifest_hash,movement_count
    ) values (
      '59000000-0000-4000-8000-000000000089','59000000-0000-4000-8000-000000000078',
      '59000000-0000-4000-8000-000000000021','59000000-0000-4000-8000-000000000041',
      '59000000-0000-4000-8000-000000000012','2026-03-15',
      pg_catalog.decode(pg_catalog.repeat('89',32),'hex'),0);
    v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,1,
      'wp59-cycle-scope','WP59 cancel with an ambiguous client scope');
    raise exception 'WP59_PROBE_ROLLBACK';
  exception when others then
    if sqlerrm<>'WP59_PROBE_ROLLBACK' then raise; end if;
  end;
  if v_result<>'REFUSED 55000 WEEKLY_SOURCE_CORRECTION_CLIENT_SCOPE_INVALID' then
    raise exception 'ASSERTION_FAILED: an ambiguous CYCLE client scope was not refused (%)',v_result;
  end if;

  v_result:=pg_temp.cancel_call('59000000-0000-4000-8000-000000000001',v_session,1,
    'wp59-cycle-cancel','WP59 abandon the cycle-scope correction');
  if v_result<>'OK CANCELLED v2 replay=false from=DRAFT rejected=0 superseded=0 publications=0' then
    raise exception 'ASSERTION_FAILED: the CYCLE-scope session did not cancel (%)',v_result;
  end if;
  if pg_temp.cycle_active_rows()<>0 then
    raise exception 'ASSERTION_FAILED: the CYCLE-scope cancel did not release the cycle';
  end if;
  if pg_temp.cycle_open('wp59-cycle-2','WP59 the cycle correction that can now run')
       !~*'^[0-9a-f]{8}-' then
    raise exception 'ASSERTION_FAILED: a new CYCLE-scope correction did not open after the cancel';
  end if;
end;
$cycle_scope$;

select pg_temp.assert_true(
  (select state='CURRENT' from public.weekly_source_final_revisions
   where id='59000000-0000-4000-8000-000000000078'),
  'the CYCLE-scope prior authority must still be CURRENT after its cancel');

rollback;
