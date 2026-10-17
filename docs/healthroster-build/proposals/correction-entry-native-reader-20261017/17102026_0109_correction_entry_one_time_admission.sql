-- ONE_TIME_ADMISSION only, not part of any rerunnable repeatable closure.
-- Root may use this collision check in the initial additive admission phase.
-- It must not mount it before later changed-closure/function reapplication.
do $guard$
begin
 if to_regprocedure('private.timesheet_correction_entry_subject_v1(uuid,uuid)') is not null
  or to_regprocedure('private.timesheet_correction_entry_v1(uuid,uuid)') is not null
  or to_regprocedure('public.timesheet_correction_entry_v1(jsonb)') is not null then
  raise exception 'CORRECTION_ENTRY_ADDITIVE_SOURCE_CONFLICT';end if;
end;
$guard$;
