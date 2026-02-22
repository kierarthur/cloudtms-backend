BEGIN;

-- ---------------------------------------------------------------------------
-- public.contracts
--   + is_ad_hoc boolean not null default false
--   + ensure candidate_id remains nullable
-- ---------------------------------------------------------------------------

ALTER TABLE public.contracts
  ADD COLUMN IF NOT EXISTS is_ad_hoc boolean NOT NULL DEFAULT false;

-- If (for any reason) candidate_id has become NOT NULL, make it nullable again.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'contracts'
      AND column_name  = 'candidate_id'
      AND is_nullable  = 'NO'
  ) THEN
    EXECUTE 'ALTER TABLE public.contracts ALTER COLUMN candidate_id DROP NOT NULL';
  END IF;
END $$;

-- Defensive: if is_ad_hoc exists but is nullable / missing default, fix it.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'contracts'
      AND column_name  = 'is_ad_hoc'
  ) THEN
    EXECUTE 'ALTER TABLE public.contracts ALTER COLUMN is_ad_hoc SET DEFAULT false';

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name   = 'contracts'
        AND column_name  = 'is_ad_hoc'
        AND is_nullable  = 'YES'
    ) THEN
      EXECUTE 'UPDATE public.contracts SET is_ad_hoc = false WHERE is_ad_hoc IS NULL';
      EXECUTE 'ALTER TABLE public.contracts ALTER COLUMN is_ad_hoc SET NOT NULL';
    END IF;
  END IF;
END $$;


-- ---------------------------------------------------------------------------
-- public.contract_weeks
--   + enforce_day_partition boolean not null default false
--   + allowed_days_mask text null
--   + split_boundary_date date null
--   + worker_note text null
--   + split_group_key text null
-- ---------------------------------------------------------------------------

ALTER TABLE public.contract_weeks
  ADD COLUMN IF NOT EXISTS enforce_day_partition boolean NOT NULL DEFAULT false;

ALTER TABLE public.contract_weeks
  ADD COLUMN IF NOT EXISTS allowed_days_mask text;

ALTER TABLE public.contract_weeks
  ADD COLUMN IF NOT EXISTS split_boundary_date date;

ALTER TABLE public.contract_weeks
  ADD COLUMN IF NOT EXISTS worker_note text;

ALTER TABLE public.contract_weeks
  ADD COLUMN IF NOT EXISTS split_group_key text;

-- Defensive: if enforce_day_partition exists but is nullable / missing default, fix it.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'contract_weeks'
      AND column_name  = 'enforce_day_partition'
  ) THEN
    EXECUTE 'ALTER TABLE public.contract_weeks ALTER COLUMN enforce_day_partition SET DEFAULT false';

    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name   = 'contract_weeks'
        AND column_name  = 'enforce_day_partition'
        AND is_nullable  = 'YES'
    ) THEN
      EXECUTE 'UPDATE public.contract_weeks SET enforce_day_partition = false WHERE enforce_day_partition IS NULL';
      EXECUTE 'ALTER TABLE public.contract_weeks ALTER COLUMN enforce_day_partition SET NOT NULL';
    END IF;
  END IF;
END $$;

COMMIT;
