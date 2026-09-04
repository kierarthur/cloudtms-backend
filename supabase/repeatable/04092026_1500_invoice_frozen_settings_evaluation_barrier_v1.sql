-- Frozen Timesheet settings must be read only for the source rows already
-- selected by the invoice command. PostgreSQL may otherwise move a STABLE
-- reader below the source join and evaluate an unrelated, legitimately
-- unprocessed Daily Timesheet whose authority has not frozen yet.

\set ON_ERROR_STOP on

begin;

alter function private._timesheet_settings_authority_frozen_v1(uuid) volatile;
alter function private._timesheet_settings_authority_frozen_v1(uuid) owner to postgres;
revoke all on function private._timesheet_settings_authority_frozen_v1(uuid)
  from public,anon,authenticated,service_role;

comment on function private._timesheet_settings_authority_frozen_v1(uuid) is
  'Fail-closed frozen Timesheet settings reader. VOLATILE is an evaluation barrier so invoice queries cannot inspect unrelated planned or unprocessed rows.';

notify pgrst, 'reload schema';

commit;
