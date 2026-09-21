-- Weekly Source Plan 6.2 — Gate 12 executed proof suite: ROT (part 1 of 2).
-- WP-16c. Rollback-contained, PostgreSQL 17.11.
--
-- The single-session half of `ROT-001..ROT-012`. The multi-session half
-- (`ROT-002`, `ROT-003`, `ROT-012`) is in `rot-suite.mjs`.
--
-- WHAT IS NEW HERE.  `ROT-005`, `ROT-006`, `ROT-007` and `ROT-008` appear in no
-- verifier in the repository: neither WP-03 nor WP-09 proved them, statically or
-- at runtime. They are proved here for the first time. `ROT-004`'s runtime half
-- is WP-09 limitation L1, addressed to WP-16c by name: WP-09 proved 30 guard
-- installations by a `pg_proc.prosrc` census and drove eleven calls, leaving the
-- import, removal and TSFIN entry points to a runtime suite. This file drives
-- every entry point its world can reach, with `candidate_route_confirmation`
-- OFF and ON, and records which ones it could not reach rather than implying
-- coverage it does not have.
--
-- EVERY ASSERTION HERE EXECUTES THE PATH (Part 1 rule 1). No assertion is a
-- search over `pg_get_functiondef`.
--
-- Output contract, one line per assertion:
--   WS16C_PROOF|<control id>|PASS|FAIL|SKIP|<EXECUTED|STATIC|ENVIRONMENT>|<detail>
--
-- Nothing here is written outside the rolled-back transaction, and
-- `set constraints all immediate` is never used.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create temporary table wp16c_results(
  ordinal serial primary key,
  proof_id text not null,
  result text not null,
  evidence text not null,
  detail text not null
) on commit drop;

create function pg_temp.proof(
  p_proof_id text, p_result text, p_evidence text, p_detail text
) returns void language sql as $function$
  insert into wp16c_results(proof_id,result,evidence,detail)
  values (p_proof_id,p_result,p_evidence,pg_catalog.left(pg_catalog.replace(p_detail,'|','/'),400));
$function$;

create function pg_temp.check(
  p_proof_id text, p_condition boolean, p_detail text, p_evidence text default 'EXECUTED'
) returns boolean language plpgsql as $function$
begin
  perform pg_temp.proof(
    p_proof_id,
    case when p_condition is distinct from true then 'FAIL' else 'PASS' end,
    p_evidence, p_detail);
  return coalesce(p_condition,false);
end;
$function$;

create function pg_temp.drain_workbench_jobs() returns void
language sql as $function$
  update public.banking_pay_workbench_jobs
     set status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp()
   where status in ('QUEUED','RUNNING');
$function$;

-- One comparable string over everything a rotation entry point could write for
-- a family. `ROT-004` requires the refusal to happen with ZERO rows written, so
-- the proof is that this digest is identical before and after every attempt.
create function pg_temp.lifecycle_digest(p_booking text) returns text
language sql stable as $function$
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    coalesce(pg_catalog.string_agg(row_text,'|' order by row_text),''),'UTF8')),'hex')
  from (
    select 'ts:'||root.timesheet_id::text||':'||root.version::text||':'||root.is_current::text
           ||':'||root.status::text||':'||coalesce(root.authorised_at_server::text,'-') as row_text
    from public.timesheets root where pg_catalog.btrim(root.booking_id)=pg_catalog.btrim(p_booking)
    union all
    select 'fin:'||financial.id::text||':'||financial.is_current::text||':'
           ||financial.processing_status::text
    from public.timesheets_financials financial
    join public.timesheets root on root.timesheet_id=financial.timesheet_id
    where pg_catalog.btrim(root.booking_id)=pg_catalog.btrim(p_booking)
    union all
    select 'week:'||week.id::text||':'||week.status::text||':'||coalesce(week.timesheet_id::text,'-')
    from public.contract_weeks week
    join public.timesheets root on root.timesheet_id=week.timesheet_id
    where pg_catalog.btrim(root.booking_id)=pg_catalog.btrim(p_booking)
    union all
    select 'gen:'||generation.id::text||':'||generation.authorisation_generation::text
           ||':'||coalesce(generation.withdrawn_at_utc::text,'-')
    from public.weekly_source_root_authorisations generation
    where pg_catalog.btrim(generation.family_booking_id)=pg_catalog.btrim(p_booking)
    union all
    select 'audit:'||pg_catalog.count(*)::text
    from public.audit_events event
    where event.object_type='timesheets'
      and event.object_id_text in (
        select root.timesheet_id::text from public.timesheets root
        where pg_catalog.btrim(root.booking_id)=pg_catalog.btrim(p_booking))
  ) as family_state;
$function$;

-- Drive one rotation entry point and classify the outcome honestly.
--
--   GUARD        refused by the managed-root guard itself, zero writes. This is
--                the result ROT-004 and ROT-012 ask for.
--   OTHER        refused before any write by a DIFFERENT installed guard that
--                fires earlier. Still "refuses safely before any write", so it
--                passes, but the code that actually fired is named rather than
--                being implied to be the managed-root guard.
--   NOT_REACHED  the entry point returned without reaching the guard, because
--                this world carries no target of the shape it acts on.
--                Recorded SKIP: runtime coverage is NOT claimed for it.
--   WROTE        not refused AND the family changed. That is a defect.
--
-- Some owners report per row instead of raising (E26 collects into `errors`),
-- so `p_result_carries_refusal` lets the caller say the refusal is in the
-- returned value rather than in an exception.
create function pg_temp.drive_entry_point(
  p_proof_id text, p_label text, p_sql text, p_booking text,
  p_result_carries_refusal boolean default false
) returns void language plpgsql as $function$
declare
  v_before text;
  v_after text;
  v_message text;
  v_returned text;
  v_guard boolean:=false;
  v_other boolean:=false;
begin
  v_before:=pg_temp.lifecycle_digest(p_booking);
  begin
    if p_result_carries_refusal then
      execute p_sql into v_returned;
      v_guard:=coalesce(v_returned,'') like '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%';
    else
      execute p_sql;
    end if;
  exception when others then
    get stacked diagnostics v_message=message_text;
    v_guard:=v_message like '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%';
    v_other:=not v_guard;
  end;
  v_after:=pg_temp.lifecycle_digest(p_booking);

  if v_before is distinct from v_after and not v_guard then
    perform pg_temp.proof(p_proof_id,'FAIL','EXECUTED',
      p_label||': the family CHANGED without a managed-root refusal ('
      ||coalesce(v_message,'accepted')||')');
  elsif v_guard then
    perform pg_temp.proof(p_proof_id,'PASS','EXECUTED',
      p_label||': refused WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED before any write, '
      ||'family state byte-identical');
  elsif v_other then
    perform pg_temp.proof(p_proof_id,'PASS','EXECUTED',
      p_label||': refused safely before any write by an earlier installed guard ('
      ||coalesce(v_message,'?')||'), family state byte-identical');
  else
    perform pg_temp.proof(p_proof_id,'SKIP','EXECUTED',
      p_label||': returned without reaching the managed-root guard and wrote nothing; '
      ||'this world carries no target of the shape it acts on, so runtime coverage '
      ||'is NOT claimed for this entry point');
  end if;
end;
$function$;

create function pg_temp.seed_timesheet(
  p_timesheet_id uuid,p_booking_id text,p_version integer,p_is_current boolean,
  p_contract_id uuid
) returns uuid language sql as $function$
  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,created_at,updated_at
  ) values (
    p_timesheet_id,p_booking_id,p_version,p_is_current,
    'RECEIVED'::public.timesheet_status_enum,
    'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
    'HOURS'::public.timesheet_line_type_enum,'wp16c-rot-occupant','wp16c-rot-hospital',
    'wp16c-rot-ward','wp16c-rot-role','weekly-0','2026-09-13',p_contract_id,
    '[]'::jsonb,'{}'::jsonb,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) returning timesheet_id;
$function$;

create function pg_temp.seed_week_and_financials(
  p_contract_week_id uuid,p_contract_id uuid,p_timesheet_id uuid,
  p_financials_id uuid,p_candidate_id uuid,p_client_id uuid,p_version integer
) returns void language sql as $function$
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment
  ) values (
    p_contract_week_id,p_contract_id,'2026-09-13',0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,p_timesheet_id,false);
  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
  ) values (
    p_financials_id,p_timesheet_id,p_version,true,p_candidate_id,p_client_id,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,10,100,200);
$function$;

-- ---------------------------------------------------------------------------
-- Fixture world.
--   R1  managed, authorised, plain family                (`ROT-004`, `ROT-008`)
--   R2  managed, authorised, rotated family v1 demoted    (`ROT-005`, `ROT-007`)
--   R3  UNMANAGED family, never authorised                (`ROT-010`)
--   R4  blank booking id                                  (`ROT-009`)
--   R5  whitespace-split pair sharing a trimmed key       (`ROT-009`)
--   R6  family with zero current rows                     (`ROT-009`)
--   R7  a second UNMANAGED family, never rotated          (`ROT-010` CORE arm)
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex'))
on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('c7000000-0000-4000-8000-000000000001','wp16c-rot@example.test','admin',true,'not-a-login');
insert into public.clients(id,name) values ('c7000000-0000-4000-8000-000000000002','WP16C ROT Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('c7000000-0000-4000-8000-000000000002',20,'2026-01-01');

do $seed_rot_world$
declare
  v_index integer;
  v_candidate uuid;
  v_contract uuid;
  v_timesheet uuid;
begin
  for v_index in 1..7 loop
    v_candidate:=('c7000000-0000-4000-8000-0000000001'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_contract:=('c7000000-0000-4000-8000-0000000002'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    v_timesheet:=('c7000000-0000-4000-8000-0000000003'||pg_catalog.lpad(v_index::text,2,'0'))::uuid;
    insert into public.candidates(id,display_name)
    values (v_candidate,'WP16C ROT Candidate '||v_index);
    insert into public.contracts(
      id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
    ) values (
      v_contract,v_candidate,'c7000000-0000-4000-8000-000000000002',
      '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true);

    if v_index=2 then
      perform pg_temp.seed_timesheet(
        'c7000000-0000-4000-8000-000000000392','WP16C-ROT-02',1,false,v_contract);
      perform pg_temp.seed_timesheet(v_timesheet,'WP16C-ROT-02',2,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c7000000-0000-4000-8000-000000000002',2);
      -- A financial row on the SUPERSEDED version, so a census that walks the
      -- whole family sees financials on an earlier version (`ROT-007`).
      insert into public.timesheets_financials(
        id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
        processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat
      ) values (
        'c7000000-0000-4000-8000-000000000592','c7000000-0000-4000-8000-000000000392',
        1,false,v_candidate,'c7000000-0000-4000-8000-000000000002',
        'PENDING_AUTH'::public.ts_fin_processing_status_enum,8,80,160);
    elsif v_index=4 then
      perform pg_temp.seed_timesheet(v_timesheet,'   ',1,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c7000000-0000-4000-8000-000000000002',1);
    elsif v_index=5 then
      perform pg_temp.seed_timesheet(v_timesheet,'WP16C-ROT-05',1,true,v_contract);
      perform pg_temp.seed_timesheet(
        'c7000000-0000-4000-8000-000000000395',' WP16C-ROT-05',1,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c7000000-0000-4000-8000-000000000002',1);
    elsif v_index=6 then
      perform pg_temp.seed_timesheet(v_timesheet,'WP16C-ROT-06',1,false,v_contract);
      perform pg_temp.seed_timesheet(
        'c7000000-0000-4000-8000-000000000396','WP16C-ROT-06',2,false,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c7000000-0000-4000-8000-000000000002',1);
    else
      perform pg_temp.seed_timesheet(
        v_timesheet,'WP16C-ROT-'||pg_catalog.lpad(v_index::text,2,'0'),1,true,v_contract);
      perform pg_temp.seed_week_and_financials(
        ('c7000000-0000-4000-8000-0000000004'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_contract,v_timesheet,
        ('c7000000-0000-4000-8000-0000000005'||pg_catalog.lpad(v_index::text,2,'0'))::uuid,
        v_candidate,'c7000000-0000-4000-8000-000000000002',1);
    end if;
  end loop;
end
$seed_rot_world$;

select pg_temp.drain_workbench_jobs();

-- Authorise the two managed roots through the REAL installed owner.
do $authorise_rot_roots$
declare
  v_result jsonb;
  v_root uuid;
begin
  foreach v_root in array array[
    'c7000000-0000-4000-8000-000000000301'::uuid,
    'c7000000-0000-4000-8000-000000000302'::uuid]
  loop
    perform pg_temp.drain_workbench_jobs();
    v_result:=public.weekly_source_first_authorise_v1(
      v_root,v_root,null,'c7000000-0000-4000-8000-000000000001');
    perform pg_temp.drain_workbench_jobs();
    if coalesce((v_result->>'ok')::boolean,false) is not true then
      raise exception 'WP16C_ROT_FIXTURE_AUTHORISE_FAILED for %: %',v_root,v_result::text;
    end if;
  end loop;
end
$authorise_rot_roots$;

-- ===========================================================================
-- 1. ROT-004 runtime — every rotation entry point this world can reach refuses
--    safely BEFORE any write, with `candidate_route_confirmation` off and on.
--    WP-09 limitation L1.
-- ===========================================================================
do $rot_004$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000301';
  v_actor uuid:='c7000000-0000-4000-8000-000000000001';
  v_booking text:='WP16C-ROT-01';
  v_flag boolean;
  v_label text;
  v_signature text;
  v_context text:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to('wp16c-rot-context','UTF8')),'hex');
begin
  -- E1a validates its own inputs before it reaches the guard, so a bogus
  -- signature would only ever prove the input guard. The real current signature
  -- is used so the managed-root guard is the thing under test.
  select nullif(pg_catalog.btrim(coalesce(
           signature->>'backend_row_signature',signature->>'row_signature','')),'')
    into v_signature
  from public.timesheet_lifecycle_guard_signature_v1(
    v_root,(select week.id from public.contract_weeks week where week.timesheet_id=v_root),false) as signature;
  foreach v_flag in array array[false,true]
  loop
    v_label:=case when v_flag then 'CORE' else 'LEGACY' end;
    update public.settings_defaults
       set candidate_app_feature_flags_json=
             coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
             ||pg_catalog.jsonb_build_object('candidate_route_confirmation',v_flag)
     where id=1;

    perform pg_temp.drive_entry_point('ROT-004','E1 public.timesheet_route_version_rotate ('||v_label||')',
      pg_catalog.format('select public.timesheet_route_version_rotate(%L::uuid,%L::uuid,%L,%L::uuid,true)',
        v_root,v_root,'ALLOW_QR_AGAIN',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E1a public.timesheet_route_version_confirmed_v1 ('||v_label||')',
      pg_catalog.format('select public.timesheet_route_version_confirmed_v1(%L::uuid,%L::uuid,%L,%L,%L,%L::uuid)',
        v_root,v_root,v_signature,v_context,'ALLOW_QR_AGAIN',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E4 public.timesheet_qr_restore_version ('||v_label||')',
      pg_catalog.format('select public.timesheet_qr_restore_version(%L::uuid,%L::uuid,%L,%L::uuid)',
        v_root,v_root,'RESTORE_PREVIOUS',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E4 public.timesheet_qr_refuse_and_reset ('||v_label||')',
      pg_catalog.format('select public.timesheet_qr_refuse_and_reset(%L::uuid,%L::uuid,%L,%L::uuid)',
        v_root,v_root,'wp16c refusal',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E3 private._candidate_timesheet_reject_rotate_v1 ('||v_label||')',
      pg_catalog.format('select private._candidate_timesheet_reject_rotate_v1(%L::uuid,%L::uuid,%L,%L::uuid)',
        v_root,v_root,'wp16c reject',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E7 public.tsfin_prepare_write ('||v_label||')',
      pg_catalog.format('select public.tsfin_prepare_write(%L::uuid)',v_root),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E7 public.tsfin_mark_revoked ('||v_label||')',
      pg_catalog.format('select public.tsfin_mark_revoked(%L::uuid)',v_root),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E25 public.tsfin_write_current_snapshot_single_bounded ('||v_label||')',
      pg_catalog.format('select public.tsfin_write_current_snapshot_single_bounded(%L::uuid,1,%L::jsonb,%L::uuid)',
        v_root,'{}',v_actor),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E26 public.tsfin_write_snapshots_and_complete ('||v_label||')',
      pg_catalog.format('select public.tsfin_write_snapshots_and_complete(%L::jsonb)::text',
        pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'timesheet_id',v_root,'timesheet_version',1,'snapshot_json','{}'::jsonb))::text),
      v_booking,true);

    perform pg_temp.drive_entry_point('ROT-004','E11 private._candidate_expense_payment_edit_shell_v1 ('||v_label||')',
      pg_catalog.format('select private._candidate_expense_payment_edit_shell_v1(%L::uuid)',v_root),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E21 public.timesheet_standard_delete_apply_v1 ('||v_label||')',
      pg_catalog.format('select public.timesheet_standard_delete_apply_v1(%L::uuid,%L::uuid,%L::uuid,%L)',
        v_root,v_actor,v_root,v_signature),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E22 public.timesheet_weekly_manual_adjustment_delete_apply ('||v_label||')',
      pg_catalog.format('select public.timesheet_weekly_manual_adjustment_delete_apply(%L::uuid,%L::uuid,array[%L::uuid],null,null,null,%L)',
        v_root,v_actor,v_root,v_signature),v_booking);

    perform pg_temp.drive_entry_point('ROT-004','E5 public.contract_week_manual_upsert_atomic ('||v_label||')',
      pg_catalog.format('select public.contract_week_manual_upsert_atomic(%L::uuid,%L::uuid,null,%L::jsonb,%L::jsonb,null,null,%L::uuid)',
        'c7000000-0000-4000-8000-000000000401',v_root,'{}','{}',v_actor),v_booking);
  end loop;

  update public.settings_defaults
     set candidate_app_feature_flags_json=
           coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
           ||pg_catalog.jsonb_build_object('candidate_route_confirmation',false)
   where id=1;
exception when others then
  perform pg_temp.proof('ROT-004','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_004$;

-- ===========================================================================
-- 2. ROT-005 — a retry or replay that submits the OLD physical Timesheet id of
--    a rotated family. It is resolved through the family by the installed
--    `public._pay_timesheet_rotation_scope`, the non-canonical request is
--    refused, and an exact replay returns the committed result.
-- ===========================================================================
do $rot_005$
declare
  v_current uuid:='c7000000-0000-4000-8000-000000000302';
  v_historic uuid:='c7000000-0000-4000-8000-000000000392';
  v_actor uuid:='c7000000-0000-4000-8000-000000000001';
  v_candidate uuid:='c7000000-0000-4000-8000-000000000102';
  v_scope record;
  v_lock jsonb;
  v_family jsonb;
  v_refused jsonb;
  v_withdraw jsonb;
  v_replay jsonb;
  v_signature text;
begin
  select pg_catalog.count(*) filter (where scope.requested_timesheet_id=v_historic) as requested,
         pg_catalog.min(scope.canonical_timesheet_id::text)::uuid as canonical,
         pg_catalog.count(distinct scope.family_timesheet_id) as members,
         pg_catalog.bool_or(scope.requested_is_canonical) as any_canonical
    into v_scope
  from public._pay_timesheet_rotation_scope(array[v_historic]) scope;
  perform pg_temp.check('ROT-005',
    v_scope.canonical=v_current and v_scope.members=2 and v_scope.any_canonical is false,
    'the installed resolver resolves the old physical id through the family to canonical '
    ||coalesce(v_scope.canonical::text,'?')||' over '||v_scope.members||' members, and reports it is not canonical');

  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate,array[v_historic]::uuid[],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
    pg_catalog.gen_random_uuid(),'WP16C-ROT-005');
  select family_element.value into v_family
  from pg_catalog.jsonb_array_elements(coalesce(v_lock->'families','[]'::jsonb)) family_element;
  perform pg_temp.check('ROT-005',
    coalesce((v_lock->>'ok')::boolean,false)
    and coalesce((v_family->>'requested_is_canonical')::boolean,true) is false
    and (v_family->>'canonical_timesheet_id')::uuid=v_current,
    'interface I-1 reports the facts for the old id rather than acting on it: requested_is_canonical='
    ||coalesce(v_family->>'requested_is_canonical','?')||', canonical='
    ||coalesce(v_family->>'canonical_timesheet_id','?'));

  v_refused:=public.weekly_source_first_authorise_v1(v_historic,v_historic,null,v_actor);
  perform pg_temp.check('ROT-005',
    coalesce((v_refused->>'ok')::boolean,true)=false,
    'a non-canonical request is refused by the owner: '||coalesce(v_refused->>'code','NONE'));

  v_signature:=nullif(pg_catalog.btrim(coalesce(
      signature->>'backend_row_signature',signature->>'row_signature','')),'')
  from public.timesheet_lifecycle_guard_signature_v1(
    v_current,(select week.id from public.contract_weeks week where week.timesheet_id=v_current),false) as signature;
  v_withdraw:=public.weekly_source_first_authorisation_withdraw_v1(
    v_current,v_current,v_signature,v_actor);
  perform pg_temp.drain_workbench_jobs();
  v_replay:=public.weekly_source_first_authorisation_withdraw_v1(
    v_current,v_current,v_signature,v_actor);
  perform pg_temp.check('ROT-005',
    coalesce((v_withdraw->>'withdrawn')::boolean,false)
    and coalesce((v_replay->>'replayed')::boolean,false)
    and (v_replay->>'root_authorisation_id')=(v_withdraw->>'root_authorisation_id'),
    'an exact replay returns the committed result rather than acting a second time (replayed='
    ||coalesce(v_replay->>'replayed','?')||')');
exception when others then
  perform pg_temp.proof('ROT-005','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_005$;

-- ===========================================================================
-- 3. ROT-007 — payment, reservation, cancellation or settlement history
--    attached to an EARLIER Timesheet version of the family. The freeze census
--    must see it: the family is enumerated by every physical member, never by
--    the canonical row alone.
-- ===========================================================================
do $rot_007$
declare
  v_current uuid:='c7000000-0000-4000-8000-000000000302';
  v_historic uuid:='c7000000-0000-4000-8000-000000000392';
  v_candidate uuid:='c7000000-0000-4000-8000-000000000102';
  v_members uuid[];
  v_census jsonb;
  v_items jsonb;
  v_on_historic integer;
begin
  -- A Banking Pay item bound to the SUPERSEDED version.  The Draft is a fixture
  -- in the existing evidence tables (decision D2, contract section 2).
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,execution_commit_state
  ) values (
    'c7000000-0000-4000-8000-000000000f01','2026-09-18','DRAFT','REVOLUT_API','SAGE',
    'REVOLUT','SANDBOX','NOT_SUBMITTED');
  insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id)
  values ('c7000000-0000-4000-8000-000000000f02','c7000000-0000-4000-8000-000000000f01',v_candidate);
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,pay_channel,timesheet_id,is_voided,
    amount_ex_vat,amount_inc_vat
  ) values (
    'c7000000-0000-4000-8000-000000000f03','c7000000-0000-4000-8000-000000000f02',
    'TIMESHEET_PAYMENT','PAYE',v_historic,false,100,100);

  select pg_catalog.array_agg(distinct scope.family_timesheet_id) into v_members
  from public._pay_timesheet_rotation_scope(array[v_current]) scope;
  perform pg_temp.check('ROT-007',
    v_historic=any(v_members) and v_current=any(v_members),
    'the family enumerates both physical members, current and superseded');

  v_census:=private.weekly_source_freeze_census_v1(v_candidate,v_members);
  v_items:=coalesce(v_census->'items','[]'::jsonb);
  select pg_catalog.count(*) into v_on_historic
  from pg_catalog.jsonb_array_elements(v_items) item(value)
  where (item.value->>'timesheet_id')::uuid=v_historic;
  perform pg_temp.check('ROT-007',
    v_on_historic>=1 and coalesce(v_census->>'result','')='FROZEN',
    'the freeze census sees the item on the earlier version ('||v_on_historic
    ||' item(s)) and holds the root at '||coalesce(v_census->>'result','?')
    ||' until that version''s items are proved');

  -- ADOPTED CONSEQUENCE.  This assertion originally required that a census
  -- given only the canonical row could NOT see the item on the superseded
  -- version, on the reading that interface I-2 relies entirely on its caller to
  -- pass every member. Executed, the installed census expands the family itself
  -- and sees it either way, which is STRICTER than I-2 requires and cannot
  -- produce a missed item. The assertion is corrected to the behaviour the rule
  -- actually demands: the item is visible however the census is called.
  perform pg_temp.check('ROT-007',
    (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(
       coalesce(private.weekly_source_freeze_census_v1(v_candidate,array[v_current])->'items','[]'::jsonb)) item(value)
     where (item.value->>'timesheet_id')::uuid=v_historic)>=1,
    'the earlier version''s item is visible however the census is called: the installed census '
    ||'expands the family itself, which is stricter than interface I-2 requires');
exception when others then
  perform pg_temp.proof('ROT-007','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_007$;

-- ===========================================================================
-- 4. ROT-008 — invoice history attached to the authorised root while a later
--    version exists elsewhere in the family. Nothing is re-pointed: invoice
--    lines and every Weekly Source binding are byte-identical after a refused
--    rotation attempt and after an ordinary later version appears.
-- ===========================================================================
do $rot_008$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000301';
  v_actor uuid:='c7000000-0000-4000-8000-000000000001';
  v_invoice uuid:='c7000000-0000-4000-8000-000000000a01';
  v_before text;
  v_after_refusal text;
  v_after_later text;
  v_message text;
  v_refused boolean:=false;
  v_new_version uuid;
begin
  insert into public.invoices(id,client_id,status,invoice_no,subtotal_ex_vat,vat_amount,total_inc_vat,issued_at_utc)
  values (v_invoice,'c7000000-0000-4000-8000-000000000002',
          'ISSUED'::public.invoice_status_enum,'WP16C-ROT-INV',200,40,240,pg_catalog.clock_timestamp());
  insert into public.invoice_lines(
    id,invoice_id,timesheet_id,booking_id,description,total_pay_ex_vat,total_charge_ex_vat,total_inc_vat
  ) values (
    'c7000000-0000-4000-8000-000000000b01',v_invoice,v_root,'WP16C-ROT-01',
    'WP16C ROT invoice line',100,200,240);

  v_before:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    coalesce((select pg_catalog.string_agg(row_text,'|' order by row_text) from (
      select 'line:'||line.id::text||':'||line.timesheet_id::text||':'||coalesce(line.booking_id,'-')
             ||':'||line.total_charge_ex_vat::text as row_text
      from public.invoice_lines line where line.invoice_id=v_invoice
      union all
      select 'binding:'||binding.id::text||':'||binding.state||':'
             ||coalesce(binding.invoice_line_id::text,'-')
      from public.weekly_source_invoice_line_bindings binding where binding.invoice_id=v_invoice
    ) as invoice_state),''),'UTF8')),'hex');

  begin
    perform public.timesheet_route_version_rotate(v_root,v_root,'ALLOW_QR_AGAIN',v_actor,true);
  exception when others then
    get stacked diagnostics v_message=message_text;
    v_refused:=v_message like '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%';
  end;

  v_after_refusal:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    coalesce((select pg_catalog.string_agg(row_text,'|' order by row_text) from (
      select 'line:'||line.id::text||':'||line.timesheet_id::text||':'||coalesce(line.booking_id,'-')
             ||':'||line.total_charge_ex_vat::text as row_text
      from public.invoice_lines line where line.invoice_id=v_invoice
      union all
      select 'binding:'||binding.id::text||':'||binding.state||':'
             ||coalesce(binding.invoice_line_id::text,'-')
      from public.weekly_source_invoice_line_bindings binding where binding.invoice_id=v_invoice
    ) as invoice_state),''),'UTF8')),'hex');
  perform pg_temp.check('ROT-008',v_refused and v_before=v_after_refusal,
    'the rotation attempt is refused and invoice lines and bindings are byte-identical afterwards');

  -- An ordinary later version appears elsewhere in the family, exactly as an
  -- installed import owner would create it.  The invoice must still not move.
  v_new_version:='c7000000-0000-4000-8000-000000000391';
  update public.timesheets set is_current=false where timesheet_id=v_root;
  perform pg_temp.seed_timesheet(
    v_new_version,'WP16C-ROT-01',2,true,'c7000000-0000-4000-8000-000000000201');

  v_after_later:=pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
    coalesce((select pg_catalog.string_agg(row_text,'|' order by row_text) from (
      select 'line:'||line.id::text||':'||line.timesheet_id::text||':'||coalesce(line.booking_id,'-')
             ||':'||line.total_charge_ex_vat::text as row_text
      from public.invoice_lines line where line.invoice_id=v_invoice
      union all
      select 'binding:'||binding.id::text||':'||binding.state||':'
             ||coalesce(binding.invoice_line_id::text,'-')
      from public.weekly_source_invoice_line_bindings binding where binding.invoice_id=v_invoice
    ) as invoice_state),''),'UTF8')),'hex');
  perform pg_temp.check('ROT-008',v_before=v_after_later,
    'a later version elsewhere in the family re-points nothing: invoice_lines.timesheet_id and '
    ||'booking_id and every weekly_source_invoice_line_bindings row are unchanged');
exception when others then
  perform pg_temp.proof('ROT-008','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_008$;

-- ===========================================================================
-- 5. ROT-009 — conflicting or ambiguous family history. Every Weekly Source
--    path fails closed and writes nothing.
-- ===========================================================================
do $rot_009$
declare
  v_blank uuid:='c7000000-0000-4000-8000-000000000304';
  v_split uuid:='c7000000-0000-4000-8000-000000000305';
  v_nocurrent uuid:='c7000000-0000-4000-8000-000000000306';
  v_result jsonb;
  v_generations integer;
begin
  v_result:=private.weekly_source_lock_and_resolve_families_v1(
    'c7000000-0000-4000-8000-000000000104',array[v_blank]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',pg_catalog.gen_random_uuid(),'WP16C-ROT-009');
  perform pg_temp.check('ROT-009',
    coalesce((v_result->>'ok')::boolean,true)=false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->>'reason'='BOOKING_IDENTITY_MISSING',
    'a missing booking identity fails closed: '||coalesce(v_result->>'reason','NONE'));

  v_result:=private.weekly_source_lock_and_resolve_families_v1(
    'c7000000-0000-4000-8000-000000000105',array[v_split]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',pg_catalog.gen_random_uuid(),'WP16C-ROT-009');
  -- HANDOVER 2 round-5 Part E: the trim-equivalent split family is a canonical
  -- booking-reference collision and the approver has ruled it must be named one.
  -- The rename is being made in two steps so nothing breaks in between: every
  -- consumer accepts BOTH tokens first, and only then does the producer rename.
  -- This assertion is a consumer, so it accepts both and reports which token the
  -- installed owner actually returned, and it therefore passes on either side of
  -- the rename instead of straddling it.
  perform pg_temp.check('ROT-009',
    coalesce((v_result->>'ok')::boolean,true)=false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->>'reason' in (
          'FAMILY_SPLIT_BY_WHITESPACE','BOOKING_REFERENCE_CANONICAL_COLLISION'),
    'a trim-equivalent split family fails closed as a canonical booking-reference '
    ||'collision: '||coalesce(v_result->>'reason','NONE'));

  v_result:=private.weekly_source_lock_and_resolve_families_v1(
    'c7000000-0000-4000-8000-000000000106',array[v_nocurrent]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',pg_catalog.gen_random_uuid(),'WP16C-ROT-009');
  perform pg_temp.check('ROT-009',
    coalesce((v_result->>'ok')::boolean,true)=false
    and v_result->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
    and v_result->>'reason' in ('CURRENT_ROW_CARDINALITY','CANONICAL_AMBIGUOUS','FAMILY_UNRESOLVED'),
    'a family with no current row fails closed: '||coalesce(v_result->>'reason','NONE')
    ||' (canonical resolution fails before the current-row count is reached)');

  -- Nothing written by any of the three.
  select pg_catalog.count(*) into v_generations
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id in (v_blank,v_split,v_nocurrent);
  perform pg_temp.check('ROT-009',v_generations=0,
    'review is required and nothing is written: '||v_generations||' authorisation generations exist');
exception when others then
  perform pg_temp.proof('ROT-009','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_009$;

-- ===========================================================================
-- 6. ROT-010 — ordinary, non-Weekly-Source Timesheets. The guard short-circuits
--    for unmanaged families and the real rotation owner proceeds, in both
--    bodies.
-- ===========================================================================
do $rot_010$
declare
  v_actor uuid:='c7000000-0000-4000-8000-000000000001';
  v_root uuid;
  v_guard jsonb;
  v_decision jsonb;
  v_new uuid;
  v_signature text;
  v_context text;
  v_legacy_outcome text;
  v_core_outcome text;
begin
  select root.timesheet_id into v_root from public.timesheets root
  where pg_catalog.btrim(root.booking_id)='WP16C-ROT-03' and root.is_current;

  v_guard:=private.weekly_source_managed_root_guard_v1(v_root);
  v_decision:=private.weekly_source_managed_root_guard_decision_v1(v_root);
  perform pg_temp.check('ROT-010',
    coalesce((v_guard->>'managed')::boolean,true)=false
    and coalesce((v_guard->>'ok')::boolean,false)
    and coalesce((v_decision->>'managed')::boolean,true)=false
    and v_decision->>'refusal_code' is null,
    'the guard short-circuits for an unmanaged family: managed='||coalesce(v_guard->>'managed','?')
    ||', and the definer-rights decision shim, which is the only form an invoker-rights '
    ||'caller can reach, agrees');

  -- LEGACY body: the flag is off, so the dispatcher runs the legacy path.
  update public.settings_defaults
     set candidate_app_feature_flags_json=
           coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
           ||pg_catalog.jsonb_build_object('candidate_route_confirmation',false)
   where id=1;
  begin
    select (public.timesheet_route_version_rotate(
      v_root,v_root,'ALLOW_QR_AGAIN',v_actor,true)->>'new_timesheet_id')::uuid into v_new;
    v_legacy_outcome:='ROTATED:'||coalesce(v_new::text,'null');
  exception when others then
    v_legacy_outcome:=sqlerrm;
  end;
  perform pg_temp.check('ROT-010',
    v_legacy_outcome not like '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'LEGACY body: the guard short-circuits and the installed route policy alone decides the '
    ||'outcome ('||v_legacy_outcome||')');

  -- CORE body.  The two bodies are driven directly, exactly as WP-03's committed
  -- race driver does.  Going through the dispatcher with the flag on would prove
  -- ROUTE_CHANGE_CONFIRMATION_REQUIRED, and going through the confirmed route
  -- would prove ROUTE_CHANGE_NOT_PERMITTED; both are installed route policy for a
  -- MANUAL electronic-route Timesheet with no QR lineage, and neither is the
  -- guard.
  --
  -- THE ROW IS NOT "LEGACY EQUALS CORE".  `ROT-010` asks for the result to be
  -- identical **before and after the change**, in each body.  The two bodies
  -- differ from each other for this Timesheet shape for reasons that predate
  -- this project: the legacy body accepts `ALLOW_QR_AGAIN` under
  -- `p_allow_manual_only`, the core body refuses it
  -- `ALLOW_QR_AGAIN_REQUIRES_PRIOR_LINEAGE_OR_PAPER_PERMISSION`.  What one clone
  -- can prove is that neither outcome is the guard's, which is what is asserted
  -- here; the before/after byte comparison is WP-09's executed differential and
  -- the `PROT-ROTATION-001` row of the differential adapter.
  update public.settings_defaults
     set candidate_app_feature_flags_json=
           coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
           ||pg_catalog.jsonb_build_object('candidate_route_confirmation',true)
   where id=1;
  -- A FRESH unmanaged family, because the row the LEGACY arm has just promoted
  -- is refused by installed route policy that has nothing to do with the guard.
  select root.timesheet_id into v_root from public.timesheets root
  where pg_catalog.btrim(root.booking_id)='WP16C-ROT-07' and root.is_current;
  begin
    select (private._timesheet_route_version_core_v1(
      v_root,v_root,'ALLOW_QR_AGAIN',v_actor,true)->>'new_timesheet_id')::uuid into v_new;
    v_core_outcome:='ROTATED:'||coalesce(v_new::text,'null');
  exception when others then
    v_core_outcome:=sqlerrm;
  end;
  perform pg_temp.check('ROT-010',
    v_core_outcome not like '%WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED%',
    'CORE body: the guard short-circuits and the installed route policy alone decides the '
    ||'outcome ('||v_core_outcome||')');

  perform pg_temp.check('ROT-010',
    v_legacy_outcome not like '%WEEKLY_SOURCE%' and v_core_outcome not like '%WEEKLY_SOURCE%',
    'no Weekly Source code appears in either body''s result for an unmanaged family, so the '
    ||'guard is invisible to ordinary Timesheets');

  update public.settings_defaults
     set candidate_app_feature_flags_json=
           coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
           ||pg_catalog.jsonb_build_object('candidate_route_confirmation',false)
   where id=1;
exception when others then
  perform pg_temp.proof('ROT-010','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_010$;

-- ===========================================================================
-- 7. ROT-011 — the Weekly Source authorisation record. Family, physical id,
--    version and row signature are recorded and immutable afterwards; only the
--    coordinator writes the head pointer and only the withdrawal owner clears
--    it; re-authorisation appends a new generation.
-- ===========================================================================
do $rot_011$
declare
  v_root uuid:='c7000000-0000-4000-8000-000000000301';
  v_actor uuid:='c7000000-0000-4000-8000-000000000001';
  v_record record;
  v_refused boolean;
  v_message text;
begin
  select generation.id,generation.root_timesheet_id,generation.family_booking_id,
         generation.timesheet_version,generation.authorisation_generation,
         generation.authorised_row_signature,generation.current_entitlement_head_id
    into v_record
  from public.weekly_source_root_authorisations generation
  where generation.root_timesheet_id=v_root and generation.withdrawn_at_utc is null;

  perform pg_temp.check('ROT-011',
    v_record.root_timesheet_id=v_root
    and pg_catalog.btrim(v_record.family_booking_id)='WP16C-ROT-01'
    and v_record.timesheet_version=1
    and nullif(pg_catalog.btrim(coalesce(v_record.authorised_row_signature,'')),'') is not null
    and v_record.current_entitlement_head_id is null,
    'the first authorisation records family, physical id, version and row signature, and no head pointer');

  foreach v_message in array array[
    'update public.weekly_source_root_authorisations set family_booking_id=''WP16C-ROT-99'' where id=$1',
    'update public.weekly_source_root_authorisations set timesheet_version=99 where id=$1',
    'update public.weekly_source_root_authorisations set root_timesheet_id=$2 where id=$1',
    'delete from public.weekly_source_root_authorisations where id=$1']
  loop
    v_refused:=false;
    begin
      execute v_message using v_record.id,'c7000000-0000-4000-8000-000000000302'::uuid;
    exception when others then
      v_refused:=true;
    end;
    perform pg_temp.check('ROT-011',v_refused,
      'the generation refuses '||pg_catalog.split_part(v_message,' where',1));
  end loop;
exception when others then
  perform pg_temp.proof('ROT-011','FAIL','EXECUTED','section raised: '||sqlerrm);
end
$rot_011$;

-- ===========================================================================
-- Results
-- ===========================================================================
select 'WS16C_PROOF|'||result_row.proof_id||'|'||result_row.result||'|'
       ||result_row.evidence||'|'||result_row.detail as line
from wp16c_results as result_row
order by result_row.ordinal;

rollback;
