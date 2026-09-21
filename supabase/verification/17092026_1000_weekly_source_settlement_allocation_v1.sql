-- PostgreSQL 17 rollback verification: weekly_source_settlement_allocation_v1
--
-- Gate 9 item G9-2.  Proves that `Hours paid`, `Hours paid to date` and
-- `current paid hours` are read only from the immutable per-root, per-shift
-- settlement allocation, across every physical Timesheet version, and that
-- malformed or contradictory evidence produces an explicit UNAVAILABLE state
-- instead of a figure.
--
-- Everything this file writes is rolled back.  It drives no Banking Pay owner:
-- Create Draft, execution, cancellation and settlement are on the contract
-- section 2 do-not-touch list, so the evidence shapes are seeded exactly as the
-- WP-16a fixture library seeds them, and the shapes are cited to the installed
-- writer whose output they reproduce.
--
-- WP-11d corrections to this file (18 September 2026):
--
--   * The seeds now SIGN as the installed writer signs.  They previously used
--     `sha256(pay_batch_id||timesheet_id)`, which no installed routine produces;
--     the one installed writer of `public.pay_batch_timesheet_snapshots` is
--     `public.pay_batch_create_timesheet_snapshots` and it signs
--     `md5(target_snapshot_json::text)` (installed definition line 329).  The
--     old seeds therefore did not reproduce the writer's output and could not
--     exercise the signature-binds-content check (F2).
--   * The multi-settlement seeds now RESTATE the position instead of encoding a
--     delta.  The installed writer restates the Timesheet's whole position in
--     every settlement snapshot and moves only the money residual, so the old
--     "same shift settled twice sums to 5.25" seed asserted a model the writer
--     does not have.  Sections 8 and 8a replace it (F1).
--   * `NO_SETTLEMENT` is asserted to carry NO numeric member at all (F8).

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
    select 'private.weekly_source_settlement_allocation_v1(uuid)' as ident,'s' as volatility
    union all
    select 'private.weekly_source_settlement_snapshot_shifts_v1(jsonb)','i'
    union all
    select 'private.weekly_source_settlement_bounds_v1()','i'
  loop
    perform pg_temp.assert_true(
      to_regprocedure(v_proc.ident) is not null,
      'function missing: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select p.provolatile from pg_proc p where p.oid=to_regprocedure(v_proc.ident))
        =v_proc.volatility,
      'wrong volatility: '||v_proc.ident);
    perform pg_temp.assert_true(
      (select r.rolname from pg_proc p join pg_roles r on r.oid=p.proowner
       where p.oid=to_regprocedure(v_proc.ident))='postgres',
      'wrong owner: '||v_proc.ident);
    perform pg_temp.assert_true(
      not exists (
        select 1
        from pg_proc p,
             aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        join pg_roles grantee on grantee.oid=acl.grantee
        where p.oid=to_regprocedure(v_proc.ident)
          and grantee.rolname in ('anon','authenticated','service_role')),
      'private helper is executable by a browser or service role: '||v_proc.ident);
    perform pg_temp.assert_true(
      not exists (
        select 1
        from pg_proc p,
             aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) as acl
        where p.oid=to_regprocedure(v_proc.ident)
          and acl.grantee=0),
      'private helper is executable by PUBLIC: '||v_proc.ident);
  end loop;

  perform pg_temp.assert_true(
    (select p.prosecdef from pg_proc p
     where p.oid=to_regprocedure('private.weekly_source_settlement_allocation_v1(uuid)')),
    'allocation reader is not SECURITY DEFINER');
end;
$structure$;

-- ---------------------------------------------------------------------------
-- 2. The two absolute prohibitions, proved against the INSTALLED definition.
--
--    (a) no currency-to-hours calculation: the reader names no money column;
--    (b) the one-row-per-Timesheet last-settled cache is never authority;
--    (c) no write and no row lock anywhere.
-- ---------------------------------------------------------------------------
do $prohibitions$
declare
  v_body text;
  v_term text;
begin
  v_body:=lower(
    pg_get_functiondef(to_regprocedure('private.weekly_source_settlement_allocation_v1(uuid)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_settlement_snapshot_shifts_v1(jsonb)'))
    ||pg_get_functiondef(to_regprocedure('private.weekly_source_settlement_bounds_v1()')));

  foreach v_term in array array[
    'amount_ex_vat','amount_vat','amount_inc_vat',
    'frozen_target_amount','frozen_source_amount',
    'gross_preview','net_bank_amount','total_bank_out','total_debt_created',
    'debt_created','loan_repayment_taken','overpayment_recovery_taken',
    'pay_rate','charge_rate','pay_day','charge_day','rates_json',
    'last_settled_signature','last_settled_pay_batch_id',
    'timesheet_pay_state.','public.timesheet_pay_state ',
    'ts_pay_adjustments','timesheets_financials',
    'for update','for share','for no key update','for key share',
    'insert into','update public.','delete from','nextval(',
    'pay_settle_rail','pay_batch_cancel','pay_workbench_scope_invalidate'
  ]::text[]
  loop
    perform pg_temp.assert_true(
      pg_catalog.strpos(v_body,v_term)=0,
      'settlement allocation reader must not mention: '||v_term);
  end loop;

  -- Desktop AGENTS.md conditional-expression qualification rule: COALESCE,
  -- NULLIF, LEAST and GREATEST are syntax constructs, never pg_catalog
  -- functions.  A routine can be created with the illegal prefix and then fail
  -- with SQLSTATE 42883 only on first execution, so the source guard is
  -- mandatory and this file also makes a real first-use call (sections 3 to 9).
  foreach v_term in array array[
    'pg_catalog.coalesce(','pg_catalog.nullif(',
    'pg_catalog.least(','pg_catalog.greatest('
  ]::text[]
  loop
    perform pg_temp.assert_true(
      pg_catalog.strpos(v_body,v_term)=0,
      'illegal pg_catalog prefix on a conditional expression: '||v_term);
  end loop;

  -- No advisory lock of any kind: this is a read projection.
  foreach v_term in array array[
    'pg_advisory_lock','pg_advisory_xact_lock','pg_try_advisory'
  ]::text[]
  loop
    perform pg_temp.assert_true(
      pg_catalog.strpos(v_body,v_term)=0,
      'settlement allocation reader must not take an advisory lock: '||v_term);
  end loop;

  -- The three evidence relations it is allowed to read, and no other Banking
  -- Pay relation beyond the terminal container and its Candidate row.
  foreach v_term in array array[
    'public.timesheet_pay_state_history',
    'public.pay_batch_timesheet_snapshots',
    'public.pay_batches',
    'public.pay_batch_candidates',
    'public._pay_timesheet_rotation_scope'
  ]::text[]
  loop
    perform pg_temp.assert_true(
      pg_catalog.strpos(v_body,v_term)>0,
      'settlement allocation reader must read: '||v_term);
  end loop;
end;
$prohibitions$;

-- ---------------------------------------------------------------------------
-- 3. Per-shift derivation from a frozen snapshot, in isolation.
-- ---------------------------------------------------------------------------
do $shifts$
declare
  v jsonb;
begin
  -- The hours-bearing structure of a frozen snapshot is `segments`, with the
  -- five CloudTMS buckets, exactly as the installed Umbrella reader uses it
  -- (26052026_2100HRS_NEW_FUNCTIONS.sql `ts_schedule_rows_all`).
  v:=private.weekly_source_settlement_snapshot_shifts_v1(
    '{"segments":[
       {"segment_id":"s1","date":"2026-03-16","start_utc":"2026-03-16T08:00:00Z",
        "end_utc":"2026-03-16T16:00:00Z","break_mins":30,
        "hours_day":7.5,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0},
       {"segment_id":"s2","date":"2026-03-21","start_utc":"2026-03-21T20:00:00Z",
        "end_utc":"2026-03-22T08:00:00Z","break_mins":60,
        "hours_day":0,"hours_night":5,"hours_sat":6,"hours_sun":0,"hours_bh":0}
     ]}'::jsonb);
  perform pg_temp.assert_true((v->>'ok')::boolean,'well-formed snapshot must derive');
  perform pg_temp.assert_eq(jsonb_array_length(v->'shifts')::text,'2','two shifts');
  perform pg_temp.assert_eq((v#>>'{shifts,0,hours}'),'7.5','shift 1 hours');
  perform pg_temp.assert_eq((v#>>'{shifts,1,hours}'),'11','shift 2 hours');

  -- exclude_from_pay is not paid.
  v:=private.weekly_source_settlement_snapshot_shifts_v1(
    '{"segments":[
       {"segment_id":"s1","date":"2026-03-16","hours_day":7.5},
       {"segment_id":"s2","date":"2026-03-17","hours_day":8,"exclude_from_pay":true}
     ]}'::jsonb);
  perform pg_temp.assert_true((v->>'ok')::boolean,'exclude_from_pay snapshot derives');
  perform pg_temp.assert_eq(jsonb_array_length(v->'shifts')::text,'1','excluded shift dropped');
  perform pg_temp.assert_eq((v->>'excluded_from_pay'),'1','excluded count reported');

  -- Negative buckets (an adjustment snapshot) are legal.
  v:=private.weekly_source_settlement_snapshot_shifts_v1(
    '{"segments":[{"segment_id":"s1","date":"2026-03-16","hours_day":-2}]}'::jsonb);
  perform pg_temp.assert_true((v->>'ok')::boolean,'negative bucket derives');
  perform pg_temp.assert_eq((v#>>'{shifts,0,hours}'),'-2','negative hours preserved');

  -- Every malformed shape is UNDERIVABLE, never a smaller number.
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(null)->>'reason',
    'SNAPSHOT_NOT_AN_OBJECT','null snapshot');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1('[]'::jsonb)->>'reason',
    'SNAPSHOT_NOT_AN_OBJECT','array snapshot');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(
      '{"fixture":"target"}'::jsonb)->>'reason',
    'SNAPSHOT_SEGMENTS_ABSENT','snapshot with no segments');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(
      '{"segments":[]}'::jsonb)->>'reason',
    'SNAPSHOT_SEGMENTS_EMPTY','snapshot with empty segments');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(
      '{"segments":[{"segment_id":"s1","date":"2026-03-16","hours_day":"seven"}]}'::jsonb)->>'reason',
    'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','non-numeric bucket');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(
      '{"segments":[{"segment_id":"s1","hours_day":7}]}'::jsonb)->>'reason',
    'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','missing shift date');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_snapshot_shifts_v1(
      '{"segments":["s1"]}'::jsonb)->>'reason',
    'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','segment not an object');
end;
$shifts$;

-- ---------------------------------------------------------------------------
-- 4. A real world: a rotated Timesheet family and its Banking Pay evidence.
--
-- Seed citations (the installed writer whose output each shape reproduces):
--   pay_batches / pay_batch_candidates / pay_batch_items
--       Workbench Draft create (call-only; contract section 2), seeded exactly
--       as tests/weekly-source/fixtures-banking/020_base_world.sql does.
--   pay_batch_timesheet_snapshots
--       private.pay_workbench_draft_snapshot_page_v1
--       (04092026_1440_banking_pay_draft_snapshot_pagination_v8.sql), whose
--       target_snapshot_json carries the display metadata including `segments`.
--   timesheet_pay_state_history
--       public.pay_settle_rail (04082026_1211_pay_settle_rail.sql:5292-5326):
--       distinct on (timesheet_id) order by created_at_utc desc, id, then
--       insert (timesheet_id, pay_batch_id, settled_at_utc, target_snapshot_json,
--       signature) where not exists.
--   pay_batches terminal / pay_batch_candidates settled
--       public.pay_settle_rail terminal SET lists.
-- ---------------------------------------------------------------------------
insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('e1000000-0000-4000-8000-000000000001','alloc-owner@example.invalid',
  'admin','not-a-real-password','Allocation verifier',true);

insert into public.clients(id,name,vat_chargeable)
values ('e2000000-0000-4000-8000-000000000001','Allocation Trust',true);
insert into public.client_settings(
  id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr
) values (
  'e2100000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000001',
  '2026-01-01',false,false,true,true,false
);

insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
values ('e3000000-0000-4000-8000-000000000001','ALLOC-001','Sam','Nurse','Sam Nurse',
  'sam.alloc@example.invalid');

insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json
) values (
  'e4000000-0000-4000-8000-000000000001','e3000000-0000-4000-8000-000000000001',
  'e2000000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}'
);

-- A rotated family: version 1 superseded, version 2 current, and a whitespace
-- padded booking id, exactly as the WP-16a base world's family A.
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current
) values
  ('ea000000-0000-4000-8000-000000000001','  alloc-booking-a  ','sam-nurse',
   'allocation-trust','ward-a','nurse','2026-03-16 08:00:00+00','2026-03-16 16:00:00+00',
   30,450,'2026-03-22','verify/sam1.png',repeat('a',64),
   'e4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',1,false),
  ('ea000000-0000-4000-8000-000000000002','  alloc-booking-a  ','sam-nurse',
   'allocation-trust','ward-a','nurse','2026-03-16 08:00:00+00','2026-03-16 16:00:00+00',
   30,450,'2026-03-22','verify/sam2.png',repeat('b',64),
   'e4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',2,true),
  -- An unrelated single-version root used for the no-settlement case.
  ('ea000000-0000-4000-8000-000000000003','alloc-booking-b','sam-nurse',
   'allocation-trust','ward-a','nurse','2026-03-16 08:00:00+00','2026-03-16 16:00:00+00',
   30,450,'2026-03-22','verify/sam3.png',repeat('c',64),
   'e4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',1,true);

create or replace function pg_temp.seed_settled_batch(
  p_batch_id uuid,
  p_timesheet_id uuid,
  p_snapshot jsonb,
  p_options jsonb default '{}'::jsonb
) returns void
language plpgsql
as $seed$
declare
  v_candidate uuid:='e3000000-0000-4000-8000-000000000001';
  v_batch_candidate uuid:=gen_random_uuid();
  -- The installed writer's own scheme: public.pay_batch_create_timesheet_snapshots
  -- stores `md5(snapshot_rows.target_snapshot_json::text)` (line 329 of its
  -- installed definition).  Nothing else writes this relation.
  v_signature text:=coalesce(p_options->>'signature',md5(p_snapshot::text));
  v_history_signature text:=coalesce(p_options->>'history_signature',v_signature);
  -- public.pay_settle_rail copies the CHOSEN snapshot row's target_snapshot_json
  -- verbatim into timesheet_pay_state_history.snapshot_json (:5293-5326), so the
  -- two are identical by construction.  `history_override` seeds the one shape
  -- the installed writer can never produce: a diverged history copy under an
  -- intact signature (WP-11d F2).
  v_history_snapshot jsonb:=coalesce(p_options->'history_override',p_snapshot);
  v_now timestamptz:=coalesce((p_options->>'settled_at_utc')::timestamptz,now());
begin
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id,
    execution_commit_state,execution_commit_ref,execution_committed_at_utc,
    completed_at_utc
  ) values (
    p_batch_id,date '2026-03-27',
    coalesce(p_options->>'batch_status','SETTLED'),
    'MONZO_CSV','CSV','CSV','SANDBOX','PAYE','e1000000-0000-4000-8000-000000000001',
    coalesce(p_options->>'execution_commit_state','COMMITTED'),
    'verify-commit:'||p_batch_id::text,
    case when coalesce(p_options->>'execution_commit_state','COMMITTED')='COMMITTED'
      then v_now end,
    case when coalesce(p_options->>'batch_status','SETTLED')<>'DRAFT' then v_now end);

  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
    paye_state,settlement_status,settled_at_utc
  ) values (
    v_batch_candidate,p_batch_id,v_candidate,'ALLOC-001','Sam Nurse','READY',
    coalesce(p_options->>'settlement_status','SETTLED'),
    case when coalesce(p_options->>'settlement_status','SETTLED')='SETTLED'
      then v_now end);

  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided
  ) values (
    gen_random_uuid(),v_batch_candidate,'TIMESHEET_PAYMENT',p_timesheet_id,'PAYE',false);

  if coalesce(p_options->>'snapshot_mode','PRESENT')<>'MISSING' then
    insert into public.pay_batch_timesheet_snapshots(
      id,pay_batch_id,timesheet_id,candidate_id,pay_channel,
      base_snapshot_json,target_snapshot_json,signature,created_at_utc
    ) values (
      gen_random_uuid(),p_batch_id,p_timesheet_id,v_candidate,'PAYE',
      '{}'::jsonb,p_snapshot,
      case when coalesce(p_options->>'snapshot_mode','PRESENT')='EMPTY_SIGNATURE'
        then '' else v_signature end,
      v_now-interval '2 minutes');

    if coalesce(p_options->>'snapshot_mode','PRESENT')='CONFLICTING_SECOND' then
      insert into public.pay_batch_timesheet_snapshots(
        id,pay_batch_id,timesheet_id,candidate_id,pay_channel,
        base_snapshot_json,target_snapshot_json,signature,created_at_utc
      ) values (
        gen_random_uuid(),p_batch_id,p_timesheet_id,v_candidate,'UMBRELLA',
        '{}'::jsonb,p_snapshot||jsonb_build_object('variant','conflict'),
        v_signature,v_now-interval '1 minute');
    end if;
  end if;

  insert into public.timesheet_pay_state_history(
    id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
  ) values (
    gen_random_uuid(),p_timesheet_id,p_batch_id,v_now,v_history_snapshot,
    v_history_signature);

  if coalesce(p_options->>'history_mode','SINGLE')='DUPLICATE' then
    insert into public.timesheet_pay_state_history(
      id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
    ) values (
      gen_random_uuid(),p_timesheet_id,p_batch_id,v_now,v_history_snapshot,
      v_history_signature);
  end if;
end;
$seed$;

-- ---------------------------------------------------------------------------
-- 5. UI-007: a single settled batch.  The figure is the settled allocation.
--    The settlement sits on the SUPERSEDED version 1 of the family and the
--    reader is called with the CURRENT version 2 (`proof/34 section 8`).
-- ---------------------------------------------------------------------------
select pg_temp.seed_settled_batch(
  'eb000000-0000-4000-8000-000000000001',
  'ea000000-0000-4000-8000-000000000001',
  '{"segments":[
     {"segment_id":"a1","date":"2026-03-16","start_utc":"2026-03-16T08:00:00Z",
      "end_utc":"2026-03-16T16:00:00Z","break_mins":30,
      "hours_day":7.5,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0},
     {"segment_id":"a2","date":"2026-03-21","start_utc":"2026-03-21T20:00:00Z",
      "end_utc":"2026-03-22T08:00:00Z","break_mins":60,
      "hours_day":0,"hours_night":5,"hours_sat":6,"hours_sun":0,"hours_bh":0}
   ]}'::jsonb);

do $ui007$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE','UI-007 available');
  perform pg_temp.assert_eq(v->>'total_hours','18.5','UI-007 Hours paid');
  perform pg_temp.assert_eq(v->>'batch_count','1','UI-007 one batch');
  perform pg_temp.assert_eq(jsonb_array_length(v->'shifts')::text,'2','UI-007 two shifts');
  perform pg_temp.assert_eq(v#>>'{hours_by_bucket,day}','7.5','UI-007 day bucket');
  perform pg_temp.assert_eq(v#>>'{hours_by_bucket,night}','5','UI-007 night bucket');
  perform pg_temp.assert_eq(v#>>'{hours_by_bucket,sat}','6','UI-007 saturday bucket');
  -- Rotation: the evidence is on version 1, the request named version 2.
  perform pg_temp.assert_eq(
    v#>>'{settlements,0,timesheet_id}','ea000000-0000-4000-8000-000000000001',
    'UI-007 settlement read from the superseded family member');
  perform pg_temp.assert_eq(
    v->>'canonical_timesheet_id','ea000000-0000-4000-8000-000000000002',
    'UI-007 canonical row is version 2');
  perform pg_temp.assert_eq(jsonb_array_length(v->'member_timesheet_ids')::text,'2',
    'UI-007 both physical members enumerated');
  -- The same answer when the request names the superseded version.
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_allocation_v1(
      'ea000000-0000-4000-8000-000000000001')->>'total_hours','18.5',
    'UI-007 same figure from either family member');
end;
$ui007$;

-- ---------------------------------------------------------------------------
-- 6. No row lock is taken, and nothing is written.
-- ---------------------------------------------------------------------------
create function private.weekly_source_settlement_stable_rowlock_control_v1()
returns void language plpgsql stable
set search_path to 'public','pg_catalog','pg_temp'
as $control$
begin
  perform 1 from public.pay_batches limit 1 for update;
end;
$control$;

create function private.weekly_source_settlement_stable_write_control_v1()
returns void language plpgsql stable
set search_path to 'public','pg_catalog','pg_temp'
as $control$
begin
  update public.pay_batches set last_status_checked_at_utc=now()
  where id='00000000-0000-4000-8000-0000000000ff';
end;
$control$;

do $nolock$
declare
  v_tuple_locks integer;
  v_before bigint;
  v_after bigint;
begin
  select count(*) into v_before from public.timesheet_pay_state_history;
  perform private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  select count(*) into v_after from public.timesheet_pay_state_history;
  perform pg_temp.assert_true(v_before=v_after,'reader wrote settlement history');

  select count(*) into v_tuple_locks
  from pg_locks
  where pid=pg_backend_pid() and locktype='tuple';
  perform pg_temp.assert_true(v_tuple_locks=0,'reader took a row lock');

  -- Positive control for the STABLE guarantee.  PostgreSQL refuses a row lock
  -- and refuses a write inside a non-volatile function, so a STABLE reader
  -- cannot take one even if a future edit tried to.  This proves the mechanism
  -- exists in this exact server, which is what makes the `STABLE` assertion in
  -- section 1 and the `for update` source guard in section 2 a complete proof
  -- of "no row lock on any Banking Pay table, no write anywhere".
  begin
    perform private.weekly_source_settlement_stable_rowlock_control_v1();
    perform pg_temp.assert_true(false,
      'PostgreSQL did not refuse a row lock inside a STABLE function');
  exception
    when feature_not_supported or invalid_transaction_state then
      null;
  end;
  begin
    perform private.weekly_source_settlement_stable_write_control_v1();
    perform pg_temp.assert_true(false,
      'PostgreSQL did not refuse a write inside a STABLE function');
  exception
    when feature_not_supported or invalid_transaction_state or read_only_sql_transaction then
      null;
  end;
end;
$nolock$;
drop function private.weekly_source_settlement_stable_rowlock_control_v1();
drop function private.weekly_source_settlement_stable_write_control_v1();

-- ---------------------------------------------------------------------------
-- 7. NO_SETTLEMENT is a normal result, not an error.
-- ---------------------------------------------------------------------------
do $nosettlement$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','NO_SETTLEMENT','no settlement state');
  perform pg_temp.assert_true((v->>'ok')::boolean,'no settlement is ok');
  perform pg_temp.assert_eq(v->>'reason',null,'no settlement carries no reason');
  -- WP-11d F8.  A week that has not been paid has NO paid figure; it does not
  -- have zero.  A consumer that reads a figure without branching on `state`
  -- must find nothing to print.
  perform pg_temp.assert_true(not (v ? 'total_hours'),
    'NO_SETTLEMENT must carry no total_hours member');
  perform pg_temp.assert_true(not (v ? 'hours_by_bucket'),
    'NO_SETTLEMENT must carry no hours_by_bucket member');
  perform pg_temp.assert_true(not (v ? 'shifts'),
    'NO_SETTLEMENT must carry no shifts member');
  -- Counts of financial EVENTS are not hour figures and are retained.
  perform pg_temp.assert_eq(v->>'batch_count','0','no settlement batch count');
  perform pg_temp.assert_eq(v->>'settlement_count','0','no settlement count');

  perform pg_temp.assert_eq(
    private.weekly_source_settlement_allocation_v1(
      'ea000000-0000-4000-8000-00000000ffff')->>'reason',
    'ROOT_NOT_FOUND','unknown root');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_allocation_v1(null)->>'reason',
    'ROOT_NOT_FOUND','null root');
end;
$nosettlement$;

-- ---------------------------------------------------------------------------
-- 7a. WP-11d F2: the signature must bind the content the hours come from.
--
--     The installed writer copies the chosen snapshot row's
--     `target_snapshot_json` verbatim into `timesheet_pay_state_history`
--     (`pay_settle_rail:5293-5326`) and signs it `md5(target::text)`
--     (`pay_batch_create_timesheet_snapshots` line 329), so a history copy that
--     differs from the signed snapshot is the one contradiction a signature
--     exists to expose.  Before this fix it produced a confident wrong figure.
-- ---------------------------------------------------------------------------
savepoint before_content_binding;
do $content_binding$
declare
  v jsonb;
begin
  -- History says 17.5 hours; the signed snapshot says 7.5; the signature string
  -- is intact and still equal on both rows.
  perform pg_temp.seed_settled_batch(
    'ef000000-0000-4000-8000-000000000001',
    'ea000000-0000-4000-8000-000000000003',
    '{"segments":[{"segment_id":"f1","date":"2026-03-16","hours_day":7.5}]}'::jsonb,
    jsonb_build_object('history_override',
      '{"segments":[{"segment_id":"f1","date":"2026-03-16","hours_day":17.5}]}'::jsonb));
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',
    'F2 diverged history copy must not produce a figure');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_SNAPSHOT_CONFLICT',
    'F2 diverged history copy reason');
  perform pg_temp.assert_true(v->'total_hours' is null,'F2 no figure');
end;
$content_binding$;
rollback to savepoint before_content_binding;

savepoint before_content_binding;
do $signature_binding$
declare
  v jsonb;
begin
  -- Both rows carry the SAME signature string, so the old string-to-string
  -- comparison passes, but that signature does not re-compute from the content
  -- it claims to attest.
  perform pg_temp.seed_settled_batch(
    'ef000000-0000-4000-8000-000000000002',
    'ea000000-0000-4000-8000-000000000003',
    '{"segments":[{"segment_id":"f2","date":"2026-03-16","hours_day":7.5}]}'::jsonb,
    '{"signature":"a-signature-that-does-not-bind-this-content"}'::jsonb);
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',
    'F2 signature that does not bind its content must not produce a figure');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_SNAPSHOT_CONFLICT',
    'F2 unbound signature reason');
end;
$signature_binding$;
rollback to savepoint before_content_binding;

-- ---------------------------------------------------------------------------
-- 8. UI-009 and UI-012: a root settled in more than one batch.
--
--    The installed Banking Pay writer RESTATES the whole position in every
--    settlement snapshot and moves only the money residual (see the reader's
--    gate function for the three installed-writer citations), so a sum across
--    settlements double-counts.  Until the finance approver confirms the
--    restatement semantics the reader states NO FIGURE for such a root.
--    Section 8a proves the restatement reading itself, by narrowing the gate
--    inside a savepoint exactly as the ruling will narrow it.
-- ---------------------------------------------------------------------------
savepoint before_multi_settlement;

-- A later batch RESTATES the position: the same two shifts as UI-007, with the
-- day shift adjusted from 7.5 to 8.5 hours, plus a new shift.  Under the
-- writer's model this root's position is 19.5 hours, NOT 18.5 + 19.5.
select pg_temp.seed_settled_batch(
  'eb000000-0000-4000-8000-000000000002',
  'ea000000-0000-4000-8000-000000000002',
  '{"segments":[
     {"segment_id":"a1","date":"2026-03-16","start_utc":"2026-03-16T08:00:00Z",
      "end_utc":"2026-03-16T16:00:00Z","break_mins":30,
      "hours_day":8.5,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0},
     {"segment_id":"a2","date":"2026-03-21","start_utc":"2026-03-21T20:00:00Z",
      "end_utc":"2026-03-22T08:00:00Z","break_mins":60,
      "hours_day":0,"hours_night":5,"hours_sat":6,"hours_sun":0,"hours_bh":0}
   ]}'::jsonb,
  jsonb_build_object('settled_at_utc',(now()+interval '1 day')::text));

do $ui009$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',
    'UI-009 fails closed because the settlement sequence cannot be proved');
  -- WP-11e G7.  Ruling B1a is MADE, so the old reason
  -- (`SETTLEMENT_POSITION_SEMANTICS_UNRULED`, "withheld until the settlement
  -- position rule is confirmed") is now a false statement.  The withholding
  -- stands, and its reason is the true one.
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_SEQUENCE_UNPROVABLE',
    'UI-009 withholds for the TRUE reason: no installed settlement sequence');
  perform pg_temp.assert_eq(v->>'unavailable_class','POSITION_WITHHELD',
    'UI-009 the evidence is sound, so this is a withholding, not a contradiction');
  perform pg_temp.assert_true(
    v->>'reason_detail' like '%complete and consistent%'
    and v->>'reason_detail' like '%cannot be proved%',
    'UI-009 the detail says the records are sound and the ordering is unprovable');
  perform pg_temp.assert_true(
    v->>'reason_detail' not like '%until%',
    'UI-009 the detail no longer says the figure is withheld UNTIL something, '
    ||'because the ruling has been made');
  perform pg_temp.assert_true(v->'total_hours' is null,
    'UI-009 states no figure at all');
  perform pg_temp.assert_true(v->'hours_by_bucket' is null,
    'UI-009 states no bucket figure');
  -- The audit facts are still complete: no financial event is collapsed.
  perform pg_temp.assert_eq(v->>'batch_count','2','UI-009 both batches reported');
  perform pg_temp.assert_eq(v->>'settlement_count','2','UI-009 both settlements reported');
  perform pg_temp.assert_eq(jsonb_array_length(v->'settlements')::text,'2',
    'UI-009 both settlements retained for audit');
end;
$ui009$;

-- ---------------------------------------------------------------------------
-- 8-G7. WP-11e G7, EXECUTED: `settled_at_utc` is not a sequence, and there is
--       no installed one to replace it with.  This is the evidence for keeping
--       the gate closed, and it is the test that must be re-run the day anyone
--       proposes opening it.
-- ---------------------------------------------------------------------------
do $g7_no_sequence$
declare
  v_cols text;
  v_writers text;
  v_id_default text;
begin
  -- No settlement sequence or revision exists on any evidence relation the
  -- reader can see.  `timesheet_pay_state_history` is the relation the ruling
  -- would have to carry it on.
  select coalesce(string_agg(attribute.attname,',' order by attribute.attname),'')
    into v_cols
  from pg_catalog.pg_attribute attribute
  join pg_catalog.pg_class relation on relation.oid=attribute.attrelid
  join pg_catalog.pg_namespace space on space.oid=relation.relnamespace
  where space.nspname='public' and relation.relname='timesheet_pay_state_history'
    and attribute.attnum>0 and not attribute.attisdropped;
  perform pg_temp.assert_eq(v_cols,
    'id,pay_batch_id,settled_at_utc,signature,snapshot_json,timesheet_id',
    'G7 the settlement history relation carries no sequence or revision column; '
    ||'if this assertion fails, a sequence contract may now exist and the gate '
    ||'must be re-examined against ruling B1a');

  select coalesce(pg_catalog.pg_get_expr(default_row.adbin,default_row.adrelid),
                  'NO DEFAULT')
    into v_id_default
  from pg_catalog.pg_attribute attribute
  join pg_catalog.pg_class relation on relation.oid=attribute.attrelid
  join pg_catalog.pg_namespace space on space.oid=relation.relnamespace
  left join pg_catalog.pg_attrdef default_row
    on default_row.adrelid=attribute.attrelid and default_row.adnum=attribute.attnum
  where space.nspname='public' and relation.relname='timesheet_pay_state_history'
    and attribute.attname='id';
  perform pg_temp.assert_eq(v_id_default,'gen_random_uuid()',
    'G7 the history id is a random uuid, so it orders nothing');

  select coalesce(string_agg(distinct routine.proname,',' order by routine.proname),
                  'NONE')
    into v_writers
  from pg_catalog.pg_proc routine
  join pg_catalog.pg_namespace space on space.oid=routine.pronamespace
  where space.nspname in ('public','private') and routine.prokind='f'
    and pg_catalog.pg_get_functiondef(routine.oid)
        ~* 'insert\s+into\s+(public\.)?timesheet_pay_state_history';
  perform pg_temp.assert_eq(v_writers,'pay_settle_rail',
    'G7 exactly one installed routine writes settlement history');
end;
$g7_no_sequence$;

-- A tie on the maximum settled_at_utc is never resolved by sort order.  This
-- rule must SURVIVE the narrowing, so section 8a re-runs it after narrowing.
savepoint before_tie;
select pg_temp.seed_settled_batch(
  'eb000000-0000-4000-8000-000000000009',
  'ea000000-0000-4000-8000-000000000001',
  '{"segments":[{"segment_id":"a9","date":"2026-03-19","hours_day":3}]}'::jsonb,
  jsonb_build_object('settled_at_utc',(now()+interval '1 day')::text));
do $tie$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_ORDER_AMBIGUOUS',
    'a tie on the maximum settled_at_utc is ambiguous, never resolved by sort order');
  perform pg_temp.assert_true(v->'total_hours' is null,'tie states no figure');
end;
$tie$;
rollback to savepoint before_tie;

-- ---------------------------------------------------------------------------
-- 8a. The RESTATEMENT READING, and the exact text change that would expose it.
--
--     WP-11e G1.  This section used to install a HAND-WRITTEN copy of the gate
--     and call that "the narrowing executed".  It was not: the copy added a
--     `return null;` that the prescribed instruction ("delete these two lines
--     and leave `return null;` as the fall-through") does not produce, because
--     the shipped gate has no fall-through return.  Both text changes are now
--     applied to `pg_get_functiondef` of the INSTALLED gate, so what is proved
--     here is what an implementer would actually do:
--
--       (a) the LITERAL DELETION raises `2F005 control reached end of function
--           without RETURN` on every multi-settlement root.  It must never be
--           prescribed, and this assertion exists so that nobody re-prescribes
--           it;
--       (b) the ONE-LINE SUBSTITUTION of `return null;` compiles and exposes
--           the restatement reading, which is what the remaining assertions
--           measure.
--
--     WP-11e G7.  (b) IS NOT A RELEASE INSTRUCTION.  Under ruling B1a the
--     substitution alone would select the position by `settled_at_utc`, which
--     is the arbitrary timestamp the ruling forbids - proved in section 8a-G7
--     below, where two batches one microsecond apart are separated by the stamp
--     and the batch created FIRST wins.  The substitution may be applied only
--     together with re-pointing the position pick and the tie test at an
--     installed settlement sequence contract.  Everything here is rolled back
--     and the shipped gate stays closed.
-- ---------------------------------------------------------------------------
savepoint before_narrowed_gate;

-- The installed gate's own text, so that nothing below can drift from it.
create or replace function pg_temp.installed_gate_source()
returns text
language sql
stable
as $gate_source$
  select pg_catalog.pg_get_functiondef(routine.oid)
  from pg_catalog.pg_proc routine
  join pg_catalog.pg_namespace space on space.oid=routine.pronamespace
  where space.nspname='private'
    and routine.proname='weekly_source_settlement_position_gate_v1';
$gate_source$;

-- (a) The literal deletion WP-11d prescribed: remove the trailing comment block
--     and the final `return`, leaving whatever fall-through exists.  There is
--     none.
create or replace function pg_temp.apply_gate_deletion()
returns text
language plpgsql
as $apply_deletion$
declare
  v_source text:=pg_temp.installed_gate_source();
  v_changed text;
begin
  v_changed:=pg_catalog.regexp_replace(v_source,
    '(\n[ \t]*--[^\n]*)*\n[ \t]*return[ \t]+''SETTLEMENT_SEQUENCE_UNPROVABLE'';','');
  if v_changed=v_source then
    return 'NOT_APPLIED';
  end if;
  execute v_changed;
  return 'APPLIED';
end;
$apply_deletion$;

-- (b) The one-line substitution, which is what the handoff now prescribes.
create or replace function pg_temp.apply_gate_substitution()
returns text
language plpgsql
as $apply_substitution$
declare
  v_source text:=pg_temp.installed_gate_source();
  v_changed text;
begin
  v_changed:=pg_catalog.regexp_replace(v_source,
    'return[ \t]+''SETTLEMENT_SEQUENCE_UNPROVABLE'';','return null;');
  if v_changed=v_source then
    return 'NOT_APPLIED';
  end if;
  execute v_changed;
  return 'APPLIED';
end;
$apply_substitution$;

savepoint before_literal_deletion;
do $literal_deletion$
declare
  v_state text;
  v_raised text:='NO RAISE';
begin
  perform pg_temp.assert_eq(pg_temp.apply_gate_deletion(),'APPLIED',
    'G1 the prescribed deletion matched the installed text');
  begin
    perform private.weekly_source_settlement_allocation_v1(
      'ea000000-0000-4000-8000-000000000002');
  exception when others then
    -- `sqlstate` is a plpgsql special variable, not a pg_catalog function, and
    -- must never be schema-qualified.
    v_raised:=sqlstate;
  end;
  perform pg_temp.assert_eq(v_raised,'2F005',
    'G1 the LITERAL two-line deletion leaves a function with no RETURN and '
    ||'raises on every multi-settlement root. Never prescribe the deletion.');
  -- The early return still works, which is exactly why the defect would have
  -- survived a shallow smoke test.
  v_state:=private.weekly_source_settlement_allocation_v1(
    'ea000000-0000-4000-8000-000000000003')->>'state';
  perform pg_temp.assert_eq(v_state,'NO_SETTLEMENT',
    'G1 the no-settlement early return still works after the deletion, so the '
    ||'defect is invisible to anything that does not drive a multi-settlement '
    ||'root');
end;
$literal_deletion$;
rollback to savepoint before_literal_deletion;

do $substitution$
begin
  perform pg_temp.assert_eq(pg_temp.apply_gate_substitution(),'APPLIED',
    'G1 the one-line substitution matched the installed text');
end;
$substitution$;

do $restated_up$
declare
  v jsonb;
  v_shift jsonb;
begin
  -- 18.5 h restated UPWARD to 19.5 h.  The truth is 19.5, not 38.
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE','restated up: available');
  perform pg_temp.assert_eq(v->>'total_hours','19.5',
    'restated up: the position is the latest restatement, never a sum');
  perform pg_temp.assert_eq(v->>'position_basis','LATEST_RESTATEMENT',
    'restated up: position basis named');
  perform pg_temp.assert_eq(v->>'position_pay_batch_id',
    'eb000000-0000-4000-8000-000000000002','restated up: the latest batch restates');
  perform pg_temp.assert_eq(jsonb_array_length(v->'shifts')::text,'2',
    'restated up: the position has two shifts');
  select value into v_shift from jsonb_array_elements(v->'shifts') as element(value)
  where element.value->>'segment_id'='a1';
  perform pg_temp.assert_eq(v_shift->>'hours','8.5',
    'restated up: the adjusted shift shows its restated hours, not 7.5+8.5');
  perform pg_temp.assert_eq(jsonb_array_length(v_shift->'settlements')::text,'1',
    'restated up: the shift names the one settlement that restated it');
  -- Audit is not collapsed.
  perform pg_temp.assert_eq(v->>'settlement_count','2',
    'restated up: every settlement is still retained for audit');
end;
$restated_up$;

-- A downward recovery: 18.5 h recovered to 11 h.  The truth is 11, not 29.5.
savepoint before_recovery;
select pg_temp.seed_settled_batch(
  'eb000000-0000-4000-8000-000000000004',
  'ea000000-0000-4000-8000-000000000002',
  '{"segments":[
     {"segment_id":"a1","date":"2026-03-16","start_utc":"2026-03-16T08:00:00Z",
      "end_utc":"2026-03-16T16:00:00Z","break_mins":30,
      "hours_day":6,"hours_night":0,"hours_sat":0,"hours_sun":0,"hours_bh":0},
     {"segment_id":"a2","date":"2026-03-21","start_utc":"2026-03-21T20:00:00Z",
      "end_utc":"2026-03-22T08:00:00Z","break_mins":60,
      "hours_day":0,"hours_night":5,"hours_sat":0,"hours_sun":0,"hours_bh":0}
   ]}'::jsonb,
  jsonb_build_object('settled_at_utc',(now()+interval '2 days')::text));
do $restated_down$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE','recovery: available');
  perform pg_temp.assert_eq(v->>'total_hours','11',
    'recovery: the position is the latest restatement, never a sum');
  perform pg_temp.assert_eq(v->>'settlement_count','3',
    'recovery: all three settlements retained for audit');
  perform pg_temp.assert_eq(v#>>'{hours_by_bucket,sat}','0',
    'recovery: a bucket removed by the restatement is removed from the position');
end;
$restated_down$;
rollback to savepoint before_recovery;

-- The tie rule must survive the narrowing.
savepoint before_tie_after_narrowing;
select pg_temp.seed_settled_batch(
  'eb000000-0000-4000-8000-000000000009',
  'ea000000-0000-4000-8000-000000000001',
  '{"segments":[{"segment_id":"a9","date":"2026-03-19","hours_day":3}]}'::jsonb,
  jsonb_build_object('settled_at_utc',(now()+interval '1 day')::text));
do $tie_after$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_ORDER_AMBIGUOUS',
    'the tie rule survives the narrowing');
  perform pg_temp.assert_true(v->'total_hours' is null,
    'the tie still states no figure after narrowing');
end;
$tie_after$;
rollback to savepoint before_tie_after_narrowing;

-- ---------------------------------------------------------------------------
-- 8a-G7. WHY THE SUBSTITUTION ALONE MUST NOT BE APPLIED.
--
--        Ruling B1a: "Select the latest authoritative settlement by the
--        installed settlement sequence/revision contract, not by an arbitrary
--        timestamp."  With the gate open and nothing else changed, the position
--        pick orders by `settled_at_utc`, which is the settle rail's own
--        transaction start time.  Two batches whose CREATION order and STAMP
--        order disagree by a single microsecond are separated by the stamp, and
--        the batch created FIRST wins.  That is the forbidden reading, executed.
-- ---------------------------------------------------------------------------
savepoint before_microsecond;
select pg_temp.seed_settled_batch(
  'ec000000-0000-4000-8000-000000000001',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"t1","date":"2026-03-16","hours_day":6}]}'::jsonb,
  jsonb_build_object('settled_at_utc','2026-03-30 10:00:00.000001+00'));
select pg_temp.seed_settled_batch(
  'ec000000-0000-4000-8000-000000000002',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"t1","date":"2026-03-16","hours_day":5}]}'::jsonb,
  jsonb_build_object('settled_at_utc','2026-03-30 10:00:00.000000+00'));
update public.pay_batches set created_at_utc='2026-03-20 09:00:00+00'
where id='ec000000-0000-4000-8000-000000000001';
update public.pay_batches set created_at_utc='2026-03-21 09:00:00+00'
where id='ec000000-0000-4000-8000-000000000002';

do $microsecond$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE',
    'G7 with the gate merely opened, a figure IS stated for two contending '
    ||'settlements');
  perform pg_temp.assert_eq(v->>'total_hours','6',
    'G7 and the figure is decided by ONE MICROSECOND of the settle rail''s own '
    ||'clock: 6 h from the batch created FIRST, not 5 h from the batch created '
    ||'last. Ruling B1a calls this an unprovable ordering and requires no '
    ||'figure, which is why the shipped gate stays closed.');
  perform pg_temp.assert_eq(v->>'position_pay_batch_id',
    'ec000000-0000-4000-8000-000000000001',
    'G7 the stamp, not the creation order, chose the settlement');
end;
$microsecond$;
rollback to savepoint before_microsecond;

-- ---------------------------------------------------------------------------
-- 8a-G8. THE POSITION UNIT: the two readings, and the single edit between them.
--
--        Settlement 1 restates shift A = 8 h and shift B = 4 h.
--        Settlement 2 restates shift A = 9 h only.
--        'ROOT_AND_SHIFT' (ruling B1a's words) reads 13 h; 'ROOT' reads 9 h.
--        Both are executed, and the only difference between them is the literal
--        returned by `weekly_source_settlement_position_unit_v1`.
-- ---------------------------------------------------------------------------
savepoint before_position_unit;
select pg_temp.seed_settled_batch(
  'ee000000-0000-4000-8000-000000000001',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"A","date":"2026-03-16","hours_day":8},
                {"segment_id":"B","date":"2026-03-17","hours_day":4}]}'::jsonb,
  jsonb_build_object('settled_at_utc','2026-03-30 10:00:00+00'));
select pg_temp.seed_settled_batch(
  'ee000000-0000-4000-8000-000000000002',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"A","date":"2026-03-16","hours_day":9}]}'::jsonb,
  jsonb_build_object('settled_at_utc','2026-03-31 10:00:00+00'));

do $unit_shipped$
declare
  v jsonb;
  v_a text;
  v_b text;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'position_unit','ROOT_AND_SHIFT',
    'G8 the shipped unit is the ruling''s own "root and shift"');
  perform pg_temp.assert_eq(v->>'total_hours','13',
    'G8 ROOT_AND_SHIFT: a shift restated once and absent from the later '
    ||'settlement keeps the hours the settlement that stated it gave it');
  select element.value->>'hours' into v_a
  from jsonb_array_elements(v->'shifts') as element(value)
  where element.value->>'segment_id'='A';
  select element.value->>'hours' into v_b
  from jsonb_array_elements(v->'shifts') as element(value)
  where element.value->>'segment_id'='B';
  perform pg_temp.assert_eq(v_a,'9','G8 shift A takes its latest restatement');
  perform pg_temp.assert_eq(v_b,'4','G8 shift B keeps its only restatement');
  perform pg_temp.assert_eq(v->>'position_settlement_count','2',
    'G8 the stated position draws on two settlements, and says so');
end;
$unit_shipped$;

savepoint before_unit_flip;
-- THE ONE EDIT.  Nothing else in the file changes.
create or replace function private.weekly_source_settlement_position_unit_v1()
returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $unit_root$
  select 'ROOT'::text;
$unit_root$;

do $unit_root_reading$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'position_unit','ROOT',
    'G8 the alternative unit is in effect');
  perform pg_temp.assert_eq(v->>'total_hours','9',
    'G8 ROOT: the latest settlement''s whole snapshot is the position, so shift '
    ||'B is read as no longer paid. 13 against 9 is the money the approver must '
    ||'choose between.');
  perform pg_temp.assert_eq(jsonb_array_length(v->'shifts')::text,'1',
    'G8 ROOT states one shift');
  perform pg_temp.assert_eq(v->>'position_settlement_count','1',
    'G8 ROOT draws on exactly one settlement');
end;
$unit_root_reading$;
rollback to savepoint before_unit_flip;
rollback to savepoint before_position_unit;

-- Restore the shipped, fail-closed gate.
rollback to savepoint before_narrowed_gate;

do $gate_restored$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_SEQUENCE_UNPROVABLE',
    'the shipped gate is fail-closed again after the narrowing proof');
  perform pg_temp.assert_true(v->'total_hours' is null,
    'and it states no figure');
end;
$gate_restored$;

-- Back to the single-settlement UI-007 world for the negative cases below.
rollback to savepoint before_multi_settlement;

do $single_again$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE','single settlement still readable');
  perform pg_temp.assert_eq(v->>'total_hours','18.5','single settlement figure');
end;
$single_again$;

-- ---------------------------------------------------------------------------
-- 8c. WP-11e G8: the unit choice is UNOBSERVABLE on everything that ships.
--     The two readings can only differ across more than one settlement, and the
--     G7 gate withholds every such root, so the whole shipped surface must be
--     byte-identical under either unit.  Proved by flipping the unit and
--     comparing the complete result, not by reasoning about it.
-- ---------------------------------------------------------------------------
savepoint before_unit_equivalence;
do $unit_equivalence_before$
declare
  v_root uuid;
begin
  create temporary table pg_temp_unit_baseline(root_timesheet_id uuid,
                                               result text) on commit drop;
  foreach v_root in array array[
    'ea000000-0000-4000-8000-000000000001'::uuid,
    'ea000000-0000-4000-8000-000000000002'::uuid,
    'ea000000-0000-4000-8000-000000000003'::uuid]
  loop
    insert into pg_temp_unit_baseline
    values (v_root,
            private.weekly_source_settlement_allocation_v1(v_root)::text);
  end loop;
end;
$unit_equivalence_before$;

create or replace function private.weekly_source_settlement_position_unit_v1()
returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $unit_root$
  select 'ROOT'::text;
$unit_root$;

do $unit_equivalence_after$
declare
  v_row record;
  v_now text;
  v_differences integer:=0;
begin
  for v_row in select * from pg_temp_unit_baseline loop
    v_now:=private.weekly_source_settlement_allocation_v1(
             v_row.root_timesheet_id)::text;
    -- The unit is named in the result, so it legitimately differs; nothing else
    -- may.
    if pg_catalog.replace(v_now,'"position_unit": "ROOT"',
                          '"position_unit": "ROOT_AND_SHIFT"')
       is distinct from v_row.result then
      v_differences:=v_differences+1;
    end if;
  end loop;
  perform pg_temp.assert_eq(v_differences::text,'0',
    'G8 the two position units produce identical results on every shipped '
    ||'shape, because the gate withholds every root where they could differ');
end;
$unit_equivalence_after$;
rollback to savepoint before_unit_equivalence;

-- ---------------------------------------------------------------------------
-- 8d. WP-11e G2: a negative stated position is never a figure.
--     Hours paid can never be negative.  A single settlement whose segments are
--     8 and -9 used to report MINUS ONE HOUR PAID as AVAILABLE.
-- ---------------------------------------------------------------------------
savepoint before_negative;
select pg_temp.seed_settled_batch(
  'ef000000-0000-4000-8000-000000000001',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"n1","date":"2026-03-16","hours_day":8},
                {"segment_id":"n2","date":"2026-03-17","hours_day":-9}]}'::jsonb);
do $negative_total$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',
    'G2 a negative position is not a figure');
  perform pg_temp.assert_eq(v->>'reason','SNAPSHOT_POSITION_NEGATIVE',
    'G2 named reason');
  perform pg_temp.assert_eq(v->>'unavailable_class','EVIDENCE_DAMAGED',
    'G2 a negative position is contradictory evidence, not a withholding');
  perform pg_temp.assert_true(v->'total_hours' is null,'G2 no total is stated');
  perform pg_temp.assert_true(v->'shifts' is null,'G2 no shifts are stated');
  perform pg_temp.assert_eq(jsonb_typeof(v->'settlement_count'),'number',
    'G2 the evidence counts are still present');
end;
$negative_total$;
rollback to savepoint before_negative;

-- A SINGLE negative shift, with a non-negative total, is refused too: the
-- position is wrong even when the sum happens to survive.
savepoint before_negative_shift;
select pg_temp.seed_settled_batch(
  'ef000000-0000-4000-8000-000000000002',
  'ea000000-0000-4000-8000-000000000003',
  '{"segments":[{"segment_id":"n1","date":"2026-03-16","hours_day":8},
                {"segment_id":"n2","date":"2026-03-17","hours_day":-1}]}'::jsonb);
do $negative_shift$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'reason','SNAPSHOT_POSITION_NEGATIVE',
    'G2 one negative shift is refused even though the total 7 is positive');
end;
$negative_shift$;
rollback to savepoint before_negative_shift;

-- ---------------------------------------------------------------------------
-- 8e. WP-11e G6: a snapshot settled under ANOTHER Candidate is contradictory
--     evidence.  The hours may be the hours paid against this Timesheet, but a
--     Timesheet paid under somebody else is a contradiction and the pack's
--     direction for contradictory evidence is fail-closed.
-- ---------------------------------------------------------------------------
savepoint before_foreign_candidate;
do $foreign_candidate$
declare
  v jsonb;
  v_other uuid:='e3000000-0000-4000-8000-0000000000ff';
  v_batch uuid:='ef000000-0000-4000-8000-000000000003';
  v_batch_candidate uuid:=gen_random_uuid();
  v_snapshot jsonb:=
    '{"segments":[{"segment_id":"x1","date":"2026-03-16","hours_day":8}]}'::jsonb;
begin
  insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
  values (v_other,'ALLOC-OTHER','Pat','Other','Pat Other',
    'pat.other@example.invalid');
  insert into public.pay_batches(
    id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
    rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id,
    execution_commit_state,execution_commit_ref,execution_committed_at_utc,
    completed_at_utc
  ) values (
    v_batch,date '2026-03-27','SETTLED','MONZO_CSV','CSV','CSV','SANDBOX','PAYE',
    'e1000000-0000-4000-8000-000000000001','COMMITTED','verify-commit:foreign',
    now(),now());
  insert into public.pay_batch_candidates(
    id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
    paye_state,settlement_status,settled_at_utc
  ) values (
    v_batch_candidate,v_batch,v_other,'ALLOC-OTHER','Pat Other','READY',
    'SETTLED',now());
  insert into public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided
  ) values (
    gen_random_uuid(),v_batch_candidate,'TIMESHEET_PAYMENT',
    'ea000000-0000-4000-8000-000000000003','PAYE',false);
  insert into public.pay_batch_timesheet_snapshots(
    id,pay_batch_id,timesheet_id,candidate_id,pay_channel,
    base_snapshot_json,target_snapshot_json,signature,created_at_utc
  ) values (
    gen_random_uuid(),v_batch,'ea000000-0000-4000-8000-000000000003',v_other,
    'PAYE','{}'::jsonb,v_snapshot,md5(v_snapshot::text),now()-interval '2 minutes');
  insert into public.timesheet_pay_state_history(
    id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature
  ) values (
    gen_random_uuid(),'ea000000-0000-4000-8000-000000000003',v_batch,now(),
    v_snapshot,md5(v_snapshot::text));

  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',
    'G6 a snapshot settled under another Candidate must not yield a figure');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_SNAPSHOT_CONFLICT',
    'G6 named reason');
  perform pg_temp.assert_true(v->'total_hours' is null,'G6 no figure is stated');
end;
$foreign_candidate$;
rollback to savepoint before_foreign_candidate;

-- ---------------------------------------------------------------------------
-- 8f. WP-11e G4: the evidence counts are JSON numbers on EVERY class, including
--     the four paths where they used to be absent, and the bound path no longer
--     reports raw history rows as a settlement count.
-- ---------------------------------------------------------------------------
do $counts_everywhere$
declare
  v jsonb;
begin
  foreach v in array array[
    private.weekly_source_settlement_allocation_v1(null),
    private.weekly_source_settlement_allocation_v1(
      '00000000-0000-4000-8000-0000000000ff'::uuid)]
  loop
    perform pg_temp.assert_eq(v->>'reason','ROOT_NOT_FOUND','G4 subject unknown');
    perform pg_temp.assert_eq(jsonb_typeof(v->'settlement_count'),'number',
      'G4 ROOT_NOT_FOUND carries a numeric settlement count');
    perform pg_temp.assert_eq(jsonb_typeof(v->'batch_count'),'number',
      'G4 ROOT_NOT_FOUND carries a numeric batch count');
    perform pg_temp.assert_eq(v->>'evidence_counts_basis','NO_EVIDENCE_READ',
      'G4 and says the counts came from no evidence, so a consumer cannot read '
      ||'zero as "this week has not been paid"');
  end loop;
end;
$counts_everywhere$;

savepoint before_counts_family;
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,
  r2_nurse_key,img_sha256_nurse,contract_id,sheet_scope,line_type,version,is_current
) values (
  'ea000000-0000-4000-8000-0000000000f1','   ','sam-nurse','allocation-trust',
  'ward-a','nurse','2026-03-16 08:00:00+00','2026-03-16 16:00:00+00',30,450,
  '2026-03-22','verify/samf1.png',repeat('f',64),
  'e4000000-0000-4000-8000-000000000001','WEEKLY','HOURS',1,true);
do $counts_family$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1(
       'ea000000-0000-4000-8000-0000000000f1');
  perform pg_temp.assert_eq(v->>'reason','FAMILY_UNRESOLVED',
    'G4 a blank booking identity does not resolve a family');
  perform pg_temp.assert_eq(jsonb_typeof(v->'settlement_count'),'number',
    'G4 FAMILY_UNRESOLVED carries a numeric settlement count');
  perform pg_temp.assert_eq(jsonb_typeof(v->'batch_count'),'number',
    'G4 FAMILY_UNRESOLVED carries a numeric batch count');
  perform pg_temp.assert_eq(v->>'evidence_counts_basis','NO_EVIDENCE_READ',
    'G4 FAMILY_UNRESOLVED names its counts basis');
end;
$counts_family$;
rollback to savepoint before_counts_family;

savepoint before_counts_bound;
do $counts_bound$
declare
  v jsonb;
  v_row integer;
begin
  -- 501 history rows, all for ONE (timesheet, batch) pair: one settlement, one
  -- batch, 501 rows.  The old result called that `settlement_count = 501`.
  for v_row in 1..501 loop
    insert into public.timesheet_pay_state_history(
      id,timesheet_id,pay_batch_id,settled_at_utc,snapshot_json,signature)
    values (gen_random_uuid(),'ea000000-0000-4000-8000-000000000001',
      'eb000000-0000-4000-8000-000000000001',now(),'{}'::jsonb,'x');
  end loop;
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'reason','EVIDENCE_EXCEEDS_BOUND','G4 bound hit');
  perform pg_temp.assert_eq(v->>'history_row_count','502',
    'G4 the raw history row count keeps its own name');
  perform pg_temp.assert_eq(v->>'settlement_count','1',
    'G4 the settlement count counts SETTLEMENTS, not history rows');
  perform pg_temp.assert_eq(v->>'batch_count','1',
    'G4 and the batch count is present, which it was not');
  perform pg_temp.assert_eq(jsonb_typeof(v->'batch_count'),'number',
    'G4 as a JSON number');
  perform pg_temp.assert_eq(v->>'evidence_counts_basis','FAMILY_EVIDENCE',
    'G4 counted from the family''s own evidence');
end;
$counts_bound$;
rollback to savepoint before_counts_bound;

-- ---------------------------------------------------------------------------
-- 8b. The WP-11b INTERFACE: a withheld position and damaged evidence are
--     distinguished in what the reader RETURNS, not only in its internal logic,
--     so the lifecycle resolver never has to guess.  WP-11b resolves the phase
--     from the evidence COUNTS without ever taking a figure, so those counts
--     must be present on both classes and must stay counts.
-- ---------------------------------------------------------------------------
savepoint before_interface;
select pg_temp.seed_settled_batch(
  'ed000000-0000-4000-8000-000000000001',
  'ea000000-0000-4000-8000-000000000002',
  '{"segments":[{"segment_id":"a1","date":"2026-03-16","hours_day":8.5}]}'::jsonb,
  jsonb_build_object('settled_at_utc',(now()+interval '1 day')::text));

do $interface_withheld$
declare
  v jsonb;
begin
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'unavailable_class','POSITION_WITHHELD',
    'a sound-evidence withholding is classed POSITION_WITHHELD, not a contradiction');
  perform pg_temp.assert_true(v->'total_hours' is null,'withheld carries no figure');
  -- The counts WP-11b resolves the phase from.
  perform pg_temp.assert_eq(v->>'settlement_count','2','withheld carries the settlement count');
  perform pg_temp.assert_eq(v->>'batch_count','2','withheld carries the batch count');
  perform pg_temp.assert_eq(jsonb_typeof(v->'settlement_count'),'number',
    'the settlement count is a number');
  perform pg_temp.assert_eq(jsonb_typeof(v->'batch_count'),'number',
    'the batch count is a number');
  -- Plain English an Office user can read, and it must be TRUE of this state:
  -- the evidence is sound, only the position is withheld.
  -- WP-11e G7: the sentence changed with the reason.  It must still say two
  -- true things in plain English - that no figure is being shown, and that the
  -- payment records themselves are sound - and it must no longer say the figure
  -- is held UNTIL a rule is confirmed, because the rule is confirmed.
  perform pg_temp.assert_true(
    v->>'reason_detail' like '%no figure is shown%',
    'the withheld detail says plainly that no figure is shown');
  perform pg_temp.assert_true(
    v->>'reason_detail' like '%complete and consistent%',
    'the withheld detail tells the truth: the payment records are sound');
  perform pg_temp.assert_true(
    v->>'reason_detail' like '%cannot be proved%',
    'the withheld detail names the true cause: the ordering cannot be proved');
  perform pg_temp.assert_true(
    v->>'reason_detail' not like '%until%',
    'the withheld detail no longer says "until", which was an untrue statement '
    ||'once ruling B1a was made');
end;
$interface_withheld$;
rollback to savepoint before_interface;

savepoint before_interface;
do $interface_damaged$
declare
  v jsonb;
begin
  perform pg_temp.seed_settled_batch(
    'ed000000-0000-4000-8000-000000000002',
    'ea000000-0000-4000-8000-000000000002',
    '{"segments":[{"segment_id":"a1","date":"2026-03-16","hours_day":8.5}]}'::jsonb,
    '{"history_mode":"DUPLICATE"}'::jsonb);
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'unavailable_class','EVIDENCE_DAMAGED',
    'contradictory evidence stays a contradiction');
  perform pg_temp.assert_eq(v->>'reason','SETTLEMENT_HISTORY_CONFLICT',
    'damaged evidence keeps its specific reason');
  perform pg_temp.assert_true(v->'total_hours' is null,'damaged carries no figure');
  perform pg_temp.assert_eq(jsonb_typeof(v->'settlement_count'),'number',
    'damaged carries the settlement count');
  perform pg_temp.assert_eq(jsonb_typeof(v->'batch_count'),'number',
    'damaged carries the batch count');
  perform pg_temp.assert_true(v->>'reason_detail' like '%contradictory%',
    'the damaged detail says so plainly');
end;
$interface_damaged$;
rollback to savepoint before_interface;

-- Every reason this reader can produce carries a class and a distinct, non-empty
-- plain-English detail, so no consumer can meet an unclassified reason.
do $interface_complete$
declare
  v_reason text;
  v_class jsonb;
  v_details text[]:='{}';
  v_withheld integer:=0;
begin
  -- WP-11e: `SETTLEMENT_SEQUENCE_UNPROVABLE` (G7) and
  -- `SNAPSHOT_POSITION_NEGATIVE` (G2) are new; the retired
  -- `SETTLEMENT_POSITION_SEMANTICS_UNRULED` stays in the list because the class
  -- function must still classify it for any caller still holding the literal,
  -- even though nothing produces it any more.
  foreach v_reason in array array[
    'ROOT_NOT_FOUND','FAMILY_UNRESOLVED','FAMILY_EXCEEDS_BOUND',
    'EVIDENCE_EXCEEDS_BOUND','SETTLEMENT_HISTORY_CONFLICT',
    'SETTLEMENT_SNAPSHOT_CONFLICT','SETTLEMENT_STATUS_CONTRADICTS_HISTORY',
    'SETTLEMENT_ORDER_AMBIGUOUS','SETTLEMENT_SEQUENCE_UNPROVABLE',
    'SETTLEMENT_POSITION_SEMANTICS_UNRULED','SNAPSHOT_POSITION_NEGATIVE',
    'SNAPSHOT_SEGMENTS_ABSENT','SNAPSHOT_SEGMENTS_EMPTY',
    'SNAPSHOT_SEGMENTS_EXCEED_BOUND','SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE',
    'SNAPSHOT_NOT_AN_OBJECT']::text[]
  loop
    v_class:=private.weekly_source_settlement_reason_class_v1(v_reason);
    perform pg_temp.assert_true(
      v_class->>'unavailable_class' in ('POSITION_WITHHELD','EVIDENCE_DAMAGED'),
      'every reason carries one of the two classes: '||v_reason);
    perform pg_temp.assert_true(
      length(coalesce(v_class->>'reason_detail',''))>20,
      'every reason carries a plain-English detail: '||v_reason);
    v_details:=v_details||(v_class->>'reason_detail');
    if v_class->>'unavailable_class'='POSITION_WITHHELD' then
      v_withheld:=v_withheld+1;
    end if;
  end loop;
  perform pg_temp.assert_eq(v_withheld::text,'3',
    'the position-gate reasons are withholdings - the two the reader produces '
    ||'plus the retired literal, which must keep its class; everything else is '
    ||'damaged evidence');
  perform pg_temp.assert_eq(
    (select count(distinct d)::text from unnest(v_details) as d),'16',
    'every reason reads differently, so an Office user is never told the same '
    ||'sentence for two different causes');
  -- WP-11e G7.  The retired reason must no longer claim the rule is unconfirmed,
  -- because the rule IS confirmed.  It is also produced by nothing: a static
  -- check is not enough here, so the executed proof is the UI-009 assertion in
  -- section 8, which drives a real multi-settlement root and reads the new
  -- reason back.
  perform pg_temp.assert_true(
    private.weekly_source_settlement_reason_class_v1(
      'SETTLEMENT_POSITION_SEMANTICS_UNRULED')->>'reason_detail'
      not like '%until the settlement position rule is confirmed%',
    'G7 the retired reason no longer states that the ruling has not been made');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_reason_class_v1(
      'SNAPSHOT_POSITION_NEGATIVE')->>'unavailable_class','EVIDENCE_DAMAGED',
    'G2 a negative position is damaged evidence, never a withholding');
  perform pg_temp.assert_eq(
    private.weekly_source_settlement_reason_class_v1(
      'SETTLEMENT_SEQUENCE_UNPROVABLE')->>'unavailable_class','POSITION_WITHHELD',
    'G7 an unprovable sequence is a withholding: the evidence is sound');
end;
$interface_complete$;

-- ---------------------------------------------------------------------------
-- 9. Every malformed or contradictory shape returns UNAVAILABLE with a reason,
--    and never a figure.  Each case is applied to its own fresh batch and then
--    rolled back to the savepoint, so the cases cannot mask one another.
-- ---------------------------------------------------------------------------
create or replace function pg_temp.expect_unavailable(
  p_options jsonb,
  p_expected_reason text,
  p_message text
) returns void
language plpgsql
as $case$
declare
  v jsonb;
begin
  perform pg_temp.seed_settled_batch(
    'ec000000-0000-4000-8000-000000000001',
    'ea000000-0000-4000-8000-000000000002',
    coalesce(p_options->'snapshot',
      '{"segments":[{"segment_id":"c1","date":"2026-03-19","hours_day":3}]}'::jsonb),
    p_options-'snapshot');
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000002');
  perform pg_temp.assert_eq(v->>'state','UNAVAILABLE',p_message||' (state)');
  perform pg_temp.assert_eq(v->>'reason',p_expected_reason,p_message||' (reason)');
  perform pg_temp.assert_true(v->'total_hours' is null,p_message||' (no figure)');
  perform pg_temp.assert_true((v->>'ok')::boolean is false,p_message||' (not ok)');
end;
$case$;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"history_mode":"DUPLICATE"}'::jsonb,
  'SETTLEMENT_HISTORY_CONFLICT','R20/R34 duplicate history row');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"snapshot_mode":"MISSING"}'::jsonb,
  'SETTLEMENT_SNAPSHOT_CONFLICT','R35 snapshot missing');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"snapshot_mode":"EMPTY_SIGNATURE"}'::jsonb,
  'SETTLEMENT_SNAPSHOT_CONFLICT','R35 empty snapshot signature');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"snapshot_mode":"CONFLICTING_SECOND"}'::jsonb,
  'SETTLEMENT_SNAPSHOT_CONFLICT','R35 conflicting second snapshot');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"history_signature":"not-the-snapshot-signature"}'::jsonb,
  'SETTLEMENT_SNAPSHOT_CONFLICT','history/snapshot signature mismatch');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"batch_status":"DRAFT"}'::jsonb,
  'SETTLEMENT_STATUS_CONTRADICTS_HISTORY','history row in a non-terminal batch');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"execution_commit_state":"NOT_SUBMITTED"}'::jsonb,
  'SETTLEMENT_STATUS_CONTRADICTS_HISTORY','history row without a committed execution');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"settlement_status":"FAILED"}'::jsonb,
  'SETTLEMENT_STATUS_CONTRADICTS_HISTORY','history row with an unsettled Candidate row');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable('{"snapshot":{"fixture":"target"}}'::jsonb,
  'SNAPSHOT_SEGMENTS_ABSENT','snapshot carries no shift structure');
rollback to savepoint before_negatives;

savepoint before_negatives;
select pg_temp.expect_unavailable(
  '{"snapshot":{"segments":[{"segment_id":"c1","date":"2026-03-19","hours_day":"three"}]}}'::jsonb,
  'SNAPSHOT_SHIFT_HOURS_NOT_DERIVABLE','snapshot hours not derivable');
rollback to savepoint before_negatives;

-- A FAILED-with-completed_at_utc container that settled this Candidate is a
-- terminal container (`proof/32 section 4.1` and section 5.2), so its hours are
-- readable.
savepoint before_negatives;
do $failedbatch$
declare
  v jsonb;
begin
  -- Seeded on the UNSETTLED root so it is that root's only settlement: the
  -- point of R43 is that a completed-with-failures container IS terminal, not
  -- that several settlements combine.
  perform pg_temp.seed_settled_batch(
    'ec000000-0000-4000-8000-000000000002',
    'ea000000-0000-4000-8000-000000000003',
    '{"segments":[{"segment_id":"c2","date":"2026-03-20","hours_day":2}]}'::jsonb,
    '{"batch_status":"FAILED"}'::jsonb);
  v:=private.weekly_source_settlement_allocation_v1('ea000000-0000-4000-8000-000000000003');
  perform pg_temp.assert_eq(v->>'state','AVAILABLE',
    'R43 completed-with-failures container is terminal');
  perform pg_temp.assert_eq(v->>'total_hours','2','R43 hours read from the failed container');
end;
$failedbatch$;
rollback to savepoint before_negatives;

rollback;
