-- TEST-only rollback for the bounded operation target-set repair performed
-- after operation-bound protected-root exclusion was deployed.
BEGIN;

UPDATE public.import_apply_operations
SET response_json=jsonb_set(
      response_json,
      '{affected_timesheet_ids}',
      '["268a1afc-c800-4435-80ec-8630539499df","60548d68-50fd-4951-99ff-7fe17d778930","ef3d4713-f55a-4902-8200-912a06e3d86f"]'::jsonb,
      false
    ),
    updated_at_utc=now()
WHERE id='706d6132-b40f-4825-b820-bf1aa2b85fd7'::uuid
  AND import_id='7f6d1ddb-741d-42ef-893d-81817cd279b3'::uuid;

COMMIT;
