-- One-time CloudTMS schema migration: weekly_source_banking_pay_absence
--
-- Package WP-21.  ONE singleton relation holding ONE declaration:
--
--     weekly_source.banking_pay_integration_absent
--
-- It is a DECLARATION, not a control.  It records that the Banking Pay
-- integration is knowingly and intentionally absent while the other workstream
-- finishes it, so that a boundary stop can be told apart from a fault.  It
-- decides nothing, answers nothing and changes no outcome anywhere.
--
-- WHAT THIS IS NOT, stated first because the distinction is the whole point:
--
--   * It is NOT a classifier, a stand-in classifier, or a source of Banking Pay
--     verdicts.  Nothing in WP-21 produces a `cash_state`, a
--     `TERMINAL_NO_MONEY`, a `PENDING_NON_FINAL` or any other typed money-movement
--     result.  The installed classifier
--     `public._pay_rail_state_money_movement_classify` remains the only thing
--     that answers, and WP-21 never substitutes for it.
--   * It is NOT a bypass.  It cannot make a refusal pass, a root releasable, a
--     head publishable or an authorisation withdrawable.  Every Weekly Source
--     outcome is byte-identical with this declaration present and absent.
--   * It does NOT write to `pay_batches`, `pay_bank_transfers`, `pay_batch_items`,
--     `pay_advance_reservations`, the unpay-batch relation or ANY other Banking
--     Pay-owned table, and neither does its verifier.  Nothing in WP-21
--     manufactures payment evidence of any kind.
--   * It does NOT suppress, defer or condition the WP-07c future-expectation
--     tripwire.  That tripwire is untouched by this package and must keep
--     failing the day the real classifier ships.
--
-- WHY IT EXISTS.  The Banking Pay integration is expected NOT to function while
-- the other workstream completes it, and that is the correct state.  The risk is
-- not that work is blocked -- WP-21's own executed survey found that nothing
-- UPSTREAM of the Banking Pay boundary depends on the classifier at all -- but
-- that a stop AT the boundary is indistinguishable, to a tester, from a defect in
-- our own code or data.  This declaration closes that gap, and closes it in the
-- one way that cannot drift into a simulation: by saying the integration is
-- absent, and saying nothing whatsoever about what it would have answered.
--
-- Three structural properties are enforced HERE.  The rest are in the repeatable
-- `17092026_0100_weekly_source_banking_pay_absence_v1.sql`.
--
--   1. OFF BY DEFAULT, AND ABSENT BY DEFAULT.  This migration inserts no row.
--      An empty relation is indistinguishable from the declaration not existing.
--
--   2. REFUSES IN A LIVE ENVIRONMENT, BY ITS OWN CHECK.  The release control
--      plane records the environment in
--      `private.cloudtms_database_identity.environment`
--      (`22082026_1507_cloudtms_database_release_control_plane.sql`).  The
--      trigger below reads that relation itself on every insert and update and
--      refuses unless it positively says `TEST`.  A missing, duplicated, NULL or
--      non-TEST environment refuses.  The refusal does not depend on a
--      convention and cannot be avoided by writing the row directly.
--
--   3. UNREACHABLE FROM THE APPLICATION.  The relation is `private`, owned by
--      `postgres`, and revoked from `anon`, `authenticated` AND `service_role`,
--      so no browser, broker, PostgREST or Candidate-app route can read or write
--      it in any environment.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- 1. The environment guard.
--
-- In the migration because it is the enforcement of a structural constraint on
-- this relation and must exist before the trigger that carries it.
-- ---------------------------------------------------------------------------
create or replace function private._weekly_source_banking_pay_absence_environment_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'private','pg_catalog','pg_temp'
as $function$
declare
  v_environment text;
  v_rows integer;
begin
  -- Cardinality, never `limit 1`: the identity relation is a singleton, and if
  -- it is not one this guard refuses rather than picking a row.
  select pg_catalog.count(*)::integer into v_rows
  from private.cloudtms_database_identity;
  if v_rows <> 1 then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_ENVIRONMENT_UNKNOWN'
      using errcode='42501',
            detail='The release control plane records no single database identity row, so the '
                   ||'deployment environment cannot be established. The Banking Pay absence '
                   ||'declaration refuses to be recorded unless the control plane positively '
                   ||'says TEST. Rows found: '||v_rows||'.';
  end if;

  select identity_row.environment into v_environment
  from private.cloudtms_database_identity as identity_row;

  if coalesce(v_environment,'') <> 'TEST' then
    raise exception 'WEEKLY_SOURCE_BANKING_PAY_ABSENCE_LIVE_REFUSED'
      using errcode='42501',
            detail='The Banking Pay absence declaration is a TEST affordance and refuses to be '
                   ||'recorded in this environment. The release control plane records '
                   ||'environment='||coalesce(v_environment,'<null>')||'. Nothing was changed. '
                   ||'No production behaviour depends on this declaration in any case: it '
                   ||'decides nothing.';
  end if;

  return case when pg_catalog.upper(tg_op)='DELETE' then old else new end;
end;
$function$;

alter function private._weekly_source_banking_pay_absence_environment_guard_v1()
  owner to postgres;
revoke all on function private._weekly_source_banking_pay_absence_environment_guard_v1()
  from public,anon,authenticated,service_role;

comment on function private._weekly_source_banking_pay_absence_environment_guard_v1() is
  'WP-21. Refuses any write to the Banking Pay absence declaration unless the release control plane (private.cloudtms_database_identity.environment) positively records TEST. A missing, duplicated, NULL or non-TEST environment refuses. This is the declaration own check, not a convention.';

-- ---------------------------------------------------------------------------
-- 2. The declaration.
--
-- Singleton, and DELIBERATELY EMPTY after this migration.
--
--   * no row            -> the setting is OFF (the default, and the state a
--                          release leaves behind)
--   * declared = false  -> the setting is OFF
--   * declared = true   -> the setting is ON: the Banking Pay integration is
--                          declared knowingly absent, and a boundary notice
--                          becomes available to anything that chooses to read
--                          it.  NOTHING ELSE CHANGES.
-- ---------------------------------------------------------------------------
create table private.weekly_source_banking_pay_absence (
  singleton boolean primary key default true check (singleton),
  declared boolean not null default false,
  declared_by text not null,
  reason text not null,
  declared_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  constraint weekly_source_banking_pay_absence_by_ck
    check (pg_catalog.btrim(declared_by) <> ''),
  constraint weekly_source_banking_pay_absence_reason_ck
    check (pg_catalog.btrim(reason) <> '')
);
alter table private.weekly_source_banking_pay_absence owner to postgres;

create trigger weekly_source_banking_pay_absence_environment_guard
  before insert or update on private.weekly_source_banking_pay_absence
  for each row
  execute function private._weekly_source_banking_pay_absence_environment_guard_v1();

revoke all on table private.weekly_source_banking_pay_absence
from public,anon,authenticated,service_role;

comment on table private.weekly_source_banking_pay_absence is
  'WP-21. The setting weekly_source.banking_pay_integration_absent. Default OFF: this migration inserts no row, and an empty relation is indistinguishable from the setting not existing. It records that the Banking Pay integration is knowingly absent while another workstream completes it, so a stop at the boundary can be told apart from a fault. It is a declaration and not a control: no Weekly Source owner reads it, it decides nothing, it produces no money-movement verdict and every outcome is byte-identical with it present and absent. Refuses to be written at all unless the release control plane records environment=TEST, and is unreachable from anon, authenticated and service_role.';
comment on column private.weekly_source_banking_pay_absence.declared is
  'ON means the absence is declared. It does NOT mean anything is bypassed, simulated or answered on Banking Pay behalf.';
comment on column private.weekly_source_banking_pay_absence.reason is
  'Why the absence is declared, in the declarer own words. Carried into the boundary notice so a tester can see who decided this and why.';

commit;
