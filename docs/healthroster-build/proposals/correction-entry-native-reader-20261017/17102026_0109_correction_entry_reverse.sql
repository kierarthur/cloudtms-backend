-- Full reverse for this additive proposal; no retained native definition changes.
drop function public.timesheet_correction_entry_v1(jsonb);
drop function private.timesheet_correction_entry_v1(uuid,uuid);
drop function private.timesheet_correction_entry_subject_v1(uuid,uuid);
notify pgrst,'reload schema';
