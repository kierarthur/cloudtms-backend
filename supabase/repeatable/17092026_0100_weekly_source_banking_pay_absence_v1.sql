-- Repeatable CloudTMS authority: weekly_source_banking_pay_absence_v1
--
-- Package WP-21.  The reader for the setting
-- `weekly_source.banking_pay_integration_absent`, the boundary notice it makes
-- available, and the arrival detector that makes the declaration unable to
-- outlive its own truth.
--
-- ===========================================================================
-- WHAT THIS PACKAGE DOES NOT DO
-- ===========================================================================
--
-- Banking Pay is expected NOT to accept Weekly Source work while the other
-- workstream completes its activation gate.  IMPORTANT: this package is only a
-- diagnostic declaration.  It does not itself stop an existing Workbench claim
-- route, so it is never evidence that disconnected TEST authorisation is safe.
-- Safe parking is owned by HANDOVER 2's server-side activation gate.
--
--   * Nothing here produces a money-movement verdict.  No `cash_state`, no
--     `TERMINAL_NO_MONEY`, no `PENDING_NON_FINAL`, no typed result of any kind
--     is manufactured, supplied, defaulted or guessed.  The installed
--     classifier `public._pay_rail_state_money_movement_classify` is the only
--     thing that answers, and nothing here substitutes for it, wraps its
--     verdict, or is called by anything that consumes it.
--   * Nothing here reimplements, copies or interprets the classifier's marker
--     rules.  `BANKING_PAY_CLASSIFIER_ACCEPTANCE_R5.md` requires consumers to
--     use the typed result and forbids a private interpretation; WP-21 adds no
--     consumer at all.
--   * Nothing here writes to a Banking Pay-owned relation.  Not
--     `pay_batches`, not `pay_bank_transfers`, not `pay_batch_items`, not
--     `pay_advance_reservations`, not the unpay-batch relation, not any other.
--     The verification file for this package writes to none of them either.
--   * Nothing here is read by any owner that decides anything.  The freeze
--     census, the first-authorisation owner, the publication coordinator, the
--     release worker and every Office and Candidate route are BYTE-IDENTICAL
--     to their state before WP-21, and none of them can see this setting.
--   * Nothing here suppresses, defers or conditions the WP-07c
--     future-expectation tripwire.  That tripwire is not touched by this
--     package and must keep failing the day the real classifier ships.
--
-- ===========================================================================
-- WHAT IT DOES DO
-- ===========================================================================
--
-- It answers one question, for a human: "is this stop the known Banking Pay
-- boundary, or is something broken?"  It answers it from a durable declaration
-- rather than from anybody's memory, and it refuses to keep answering once the
-- integration has actually arrived.
--
-- ONE FINDING THIS PACKAGE OWES THE READER, because it is the reason the
-- surface here is so small.  An executed survey of every Weekly Source
-- repeatable, migration, verification file and broker route found that the
-- Banking Pay money-movement classifier is consulted in exactly TWO places --
-- the freeze census and the first-authorisation owner's W2 -- and both are at
-- the Banking Pay boundary, not upstream of it.  Nothing upstream depends on
-- that classifier, so import, review, finalisation, self-billing, Candidate and
-- Office presentation, audit and export can be tested independently.  This
-- finding does NOT prove that an existing Workbench claim route will stop.  A
-- Weekly Source authorisation is safe in disconnected TEST only after the
-- separately owned HANDOVER 2 claim gate has been installed, verified and left
-- FALSE so the item is parked as AWAITING_BANKING_PAY_ACTIVATION.
--
-- ===========================================================================
-- THE ARRIVAL DETECTOR
-- ===========================================================================
--
-- A declaration that the integration is absent must stop being made the moment
-- it stops being true.  `weekly_source_banking_pay_branch_probe_v1` below asks
-- the INSTALLED classifier, with the exact marker sets round-5 Part F fixes for
-- its two reason-specific branches, and returns its typed answer VERBATIM.  It
-- adds no interpretation, it is consulted by NO owner, and its only consumer is
-- this package's own gate, which RAISES once either branch answers
-- terminal-no-money.
--
-- This is deliberately the same shape as the WP-07c future-expectation tripwire
-- and deliberately NOT a replacement for it.  The tripwire is untouched: it
-- still builds its own probes and it still fails the day the classifier ships.
-- If both fire on the same day, that is the intended outcome -- one refuses to
-- let the blocked journey be forgotten, the other refuses to let a stale
-- declaration survive.

\set ON_ERROR_STOP on

begin;

-- ===========================================================================
-- 1. The arrival probe.
--
-- Marker sets, word for word from round-5 Part F:
--
--   PRE_BANK_CANCEL_VOIDED
--     "status = VOIDED, failed_reason = PRE_BANK_CANCEL_VOIDED,
--      pre_bank_cancel_applied = true ..."
--
--   CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED
--     "status = VOIDED; amount is zero;
--      failed_reason = CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED;
--      cancellation_reauthorisation_overlay_voided = true; ..."
--
-- These are the classifier's INPUTS, not its rules.  This function forms no
-- opinion about what they mean; it hands them over and reports what came back.
-- An unknown branch RAISES rather than returning no rows, because a detector
-- that silently detects nothing is worse than no detector.
-- ===========================================================================
create or replace function private.weekly_source_banking_pay_branch_probe_v1(
  p_branch text
) returns table (
  branch text,
  cash_state text,
  is_final_money_moved boolean,
  is_terminal_no_money boolean,
  is_pending_non_final boolean,
  reason text
)
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_branch text:=pg_catalog.btrim(coalesce(p_branch,''));
  v_meta jsonb;
begin
  v_meta:=case v_branch
    when 'PRE_BANK_CANCEL_VOIDED' then
      pg_catalog.jsonb_build_object(
        'failed_reason','PRE_BANK_CANCEL_VOIDED',
        'pre_bank_cancel_applied',true)
    when 'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED' then
      pg_catalog.jsonb_build_object(
        'failed_reason','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
        'cancellation_reauthorisation_overlay_voided',true,
        'amount',0)
    else null end;

  if v_meta is null then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_BRANCH_PROBE_UNKNOWN'
      using errcode='22023',
            detail='Unknown Banking Pay classifier branch '||coalesce(v_branch,'<null>')
                   ||'. The accepted contract fixes exactly two reason-specific '
                   ||'TERMINAL_NO_MONEY branches: PRE_BANK_CANCEL_VOIDED and '
                   ||'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED.';
  end if;

  return query
  select
    v_branch,
    classification.cash_state,
    coalesce(classification.is_final_money_moved,false),
    coalesce(classification.is_terminal_no_money,false),
    coalesce(classification.is_pending_non_final,false),
    classification.reason
  from public._pay_rail_state_money_movement_classify(
    'VOIDED',null,v_meta,v_meta) as classification;
end;
$function$;

-- ===========================================================================
-- 2. "Has the integration arrived?"
--
-- The typed-shape guard runs FIRST and raises loudly: if the installed
-- classifier ever loses one of the typed columns, anything depending on it must
-- fail noisily rather than quietly deciding nothing has changed.
-- ===========================================================================
create or replace function private.weekly_source_banking_pay_integration_arrived_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_shape integer;
  v_pre record;
  v_overlay record;
begin
  select pg_catalog.count(*)::integer into v_shape
  from pg_catalog.pg_proc as classifier
  cross join lateral pg_catalog.unnest(classifier.proargnames) as argument(name)
  where classifier.oid=pg_catalog.to_regprocedure(
          'public._pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)')
    and argument.name in ('cash_state','is_terminal_no_money','is_pending_non_final',
                          'is_final_money_moved','reason');
  if coalesce(v_shape,0)<>5 then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_TYPED_SHAPE_LOST'
      using errcode='55000',
            detail='The installed Banking Pay classifier must return a TYPED result carrying '
                   ||'cash_state, is_terminal_no_money, is_pending_non_final, '
                   ||'is_final_money_moved and reason; found '||coalesce(v_shape,0)||' of 5. '
                   ||'Anything that cannot read it proves nothing and must fail loudly.';
  end if;

  select * into v_pre
  from private.weekly_source_banking_pay_branch_probe_v1('PRE_BANK_CANCEL_VOIDED');
  select * into v_overlay
  from private.weekly_source_banking_pay_branch_probe_v1(
    'CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED');

  return pg_catalog.jsonb_build_object(
    'arrived',coalesce(v_pre.is_terminal_no_money,false)
              or coalesce(v_overlay.is_terminal_no_money,false),
    'typed_shape_columns',v_shape,
    'branches',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'branch','PRE_BANK_CANCEL_VOIDED',
        'cash_state',v_pre.cash_state,
        'is_terminal_no_money',coalesce(v_pre.is_terminal_no_money,false),
        'reason',v_pre.reason),
      pg_catalog.jsonb_build_object(
        'branch','CANCELLATION_REAUTHORISATION_OVERLAY_VOIDED',
        'cash_state',v_overlay.cash_state,
        'is_terminal_no_money',coalesce(v_overlay.is_terminal_no_money,false),
        'reason',v_overlay.reason)));
end;
$function$;

-- ===========================================================================
-- 3. The setting reader.  THE ONLY PLACE THE SETTING IS READ.
--
-- Read-only.  Returns the declaration's state and never makes it more
-- permissive than the stored evidence -- there is nothing for it to permit.
-- It raises in exactly one circumstance: the absence is declared AND the
-- integration has arrived, which is the declaration being untrue.
--
-- The probe is deliberately NOT run when the setting is off, so an ordinary
-- database pays nothing for this package existing.
-- ===========================================================================
create or replace function private.weekly_source_banking_pay_absence_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_rows integer;
  v_setting private.weekly_source_banking_pay_absence%rowtype;
  v_environment text;
  v_identity_rows integer;
  v_arrived jsonb;
begin
  -- Cardinality, never `limit 1`. Absent is off.
  select pg_catalog.count(*)::integer into v_rows
  from private.weekly_source_banking_pay_absence;
  if v_rows=0 then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','SETTING_ABSENT');
  end if;
  if v_rows<>1 then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_AMBIGUOUS'
      using errcode='55000',
            detail='The absence declaration is a singleton; found '||v_rows||' rows.';
  end if;

  select * into v_setting from private.weekly_source_banking_pay_absence;
  if coalesce(v_setting.declared,false) is not true then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','SETTING_OFF');
  end if;

  -- The declaration's OWN environment check, repeated at read time so that a
  -- row which somehow survived a restore, a clone or a promotion into a live
  -- database still reads as off. Fails closed on anything but TEST.
  select pg_catalog.count(*)::integer into v_identity_rows
  from private.cloudtms_database_identity;
  if v_identity_rows<>1 then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','ENVIRONMENT_UNKNOWN',
      'identity_rows',v_identity_rows);
  end if;
  select identity_row.environment into v_environment
  from private.cloudtms_database_identity as identity_row;
  if coalesce(v_environment,'')<>'TEST' then
    return pg_catalog.jsonb_build_object(
      'setting','weekly_source.banking_pay_integration_absent',
      'declared',false,'reason_code','ENVIRONMENT_NOT_TEST',
      'environment',v_environment);
  end if;

  -- The declaration cannot outlive its own truth.
  v_arrived:=private.weekly_source_banking_pay_integration_arrived_v1();
  if coalesce((v_arrived->>'arrived')::boolean,false) then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE'
      using errcode='55000',
            detail='The Banking Pay money-movement classifier now answers TERMINAL_NO_MONEY for '
                   ||'at least one of its two reason-specific branches, so the integration this '
                   ||'setting declares absent has arrived and the setting must be REMOVED rather '
                   ||'than left on. The WP-07c future-expectation tripwire fires on the same '
                   ||'event and independently; both are intended. Branch probes: '
                   ||v_arrived::text;
  end if;

  return pg_catalog.jsonb_build_object(
    'setting','weekly_source.banking_pay_integration_absent',
    'declared',true,
    'reason_code','DECLARED_ABSENT',
    'environment',v_environment,
    'declared_by',v_setting.declared_by,
    'declared_at_utc',v_setting.declared_at_utc,
    'reason',v_setting.reason,
    'classifier_branches',v_arrived->'branches');
end;
$function$;

-- ===========================================================================
-- 4. The boundary notice.
--
-- The one thing the setting makes available, and the only thing.  NULL when the
-- setting is off, so a caller that merges this into a payload adds NO KEY AT
-- ALL and its payload is byte-identical to the payload before WP-21.
--
-- No Weekly Source owner calls this today.  It exists so that a screen, a route
-- or a tester CAN state the boundary plainly, and so that the statement comes
-- from a durable declaration rather than from prose in a report.
-- ===========================================================================
create or replace function private.weekly_source_banking_pay_boundary_notice_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_state jsonb:=private.weekly_source_banking_pay_absence_v1();
begin
  if coalesce((v_state->>'declared')::boolean,false) is not true then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'kind','BANKING_PAY_INTEGRATION_ABSENT',
    'severity','RELEASE_BLOCKED_UNTIL_ACTIVATION_GATE',
    'headline','Banking Pay activation gate required',
    'message','Banking Pay is not connected yet. This declaration is informational and does not '
      ||'stop an existing Workbench claim route. Weekly Source authorisation is safe to test only '
      ||'after the HANDOVER 2 server-side activation gate is installed and verified with its '
      ||'switch off. Until then, test import, review, finalisation, self-billing and presentation, '
      ||'but do not treat this notice as a payment stop.',
    'declared_by',v_state->>'declared_by',
    'declared_at_utc',v_state->>'declared_at_utc',
    'reason',v_state->>'reason',
    'changes_no_outcome',true);
end;
$function$;

-- ===========================================================================
-- 5. The TEST control surface.
--
-- `private`, revoked from every API role, so there is no route to it from the
-- broker, PostgREST, the Office screen or the Candidate app. A session that can
-- call these already holds the database owner.
-- ===========================================================================
create or replace function private.weekly_source_banking_pay_absence_declare_v1(
  p_declared_by text,
  p_reason text
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_arrived jsonb;
begin
  v_arrived:=private.weekly_source_banking_pay_integration_arrived_v1();
  if coalesce((v_arrived->>'arrived')::boolean,false) then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_NO_LONGER_TRUE'
      using errcode='55000',
            detail='The Banking Pay integration has arrived on at least one of its two '
                   ||'reason-specific branches, so its absence cannot be declared. Branch '
                   ||'probes: '||v_arrived::text;
  end if;

  insert into private.weekly_source_banking_pay_absence(
    singleton,declared,declared_by,reason)
  values (true,true,p_declared_by,p_reason)
  on conflict (singleton) do update
    set declared=true,
        declared_by=excluded.declared_by,
        reason=excluded.reason,
        declared_at_utc=pg_catalog.transaction_timestamp();

  return private.weekly_source_banking_pay_absence_v1();
end;
$function$;

create or replace function private.weekly_source_banking_pay_absence_clear_v1()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
begin
  delete from private.weekly_source_banking_pay_absence;
  return private.weekly_source_banking_pay_absence_v1();
end;
$function$;

-- ---------------------------------------------------------------------------
-- 6. Ownership, privileges and comments.
-- ---------------------------------------------------------------------------
alter function private.weekly_source_banking_pay_branch_probe_v1(text) owner to postgres;
alter function private.weekly_source_banking_pay_integration_arrived_v1() owner to postgres;
alter function private.weekly_source_banking_pay_absence_v1() owner to postgres;
alter function private.weekly_source_banking_pay_boundary_notice_v1() owner to postgres;
alter function private.weekly_source_banking_pay_absence_declare_v1(text,text) owner to postgres;
alter function private.weekly_source_banking_pay_absence_clear_v1() owner to postgres;

revoke all on function private.weekly_source_banking_pay_branch_probe_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_banking_pay_integration_arrived_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_banking_pay_absence_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_banking_pay_boundary_notice_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_banking_pay_absence_declare_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_banking_pay_absence_clear_v1()
  from public,anon,authenticated,service_role;

comment on function private.weekly_source_banking_pay_branch_probe_v1(text) is
  'WP-21 arrival detector. Hands the INSTALLED classifier the exact marker set round-5 Part F fixes for a named branch and returns its typed answer verbatim, adding no interpretation. It produces no verdict of its own and is consulted by NO owner; its only consumer is this package own gate. It does not replace, weaken or condition the WP-07c future-expectation tripwire, which still builds its own probes and still fails the day the classifier ships.';
comment on function private.weekly_source_banking_pay_integration_arrived_v1() is
  'WP-21. The typed-shape guard, then both branch probes. arrived=true means the Banking Pay integration this package declares absent has landed, and the declaration must be removed.';
comment on function private.weekly_source_banking_pay_absence_v1() is
  'WP-21. THE ONLY reader of the setting weekly_source.banking_pay_integration_absent. Absent setting, setting off, an environment the release control plane does not record as TEST, or an unknown environment all return declared=false. An integration that has arrived RAISES. It decides nothing and no Weekly Source owner calls it.';
comment on function private.weekly_source_banking_pay_boundary_notice_v1() is
  'WP-21. Informational diagnostic only. It does not stop a Workbench claim route and is not deployment-safety evidence. Weekly Source authorisation is safe in disconnected TEST only after the HANDOVER 2 server-side activation gate is installed, verified and FALSE. NULL when the setting is off. It changes no outcome anywhere and is called by no owner.';
comment on function private.weekly_source_banking_pay_absence_declare_v1(text,text) is
  'WP-21 TEST-only control. Records the absence declaration. Refuses in any environment the release control plane does not record as TEST (through the relation own trigger) and refuses once the integration has arrived. Not granted to anon, authenticated or service_role, so no application route can call it.';
comment on function private.weekly_source_banking_pay_absence_clear_v1() is
  'WP-21 TEST-only control. Removes the declaration, returning the database to the state in which the setting is indistinguishable from absent.';

commit;
