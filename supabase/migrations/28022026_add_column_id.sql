-- Add committed_by_user_id to ID consolidation runs (safe to rerun)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name   = 'id_consolidation_runs'
      AND c.column_name  = 'committed_by_user_id'
  ) THEN
    ALTER TABLE public.id_consolidation_runs
      ADD COLUMN committed_by_user_id uuid NULL;
  END IF;
END
$$;
