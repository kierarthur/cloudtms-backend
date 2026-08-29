-- Banking Pay performance acceptance completed with durable financial audit
-- authority unchanged. Disable only the temporary high-volume diagnostic log.

UPDATE public.settings_defaults
SET temp_log = false
WHERE id = 1
  AND temp_log IS DISTINCT FROM false;
