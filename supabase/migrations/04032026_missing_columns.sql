-- ============================================================================
-- CloudTMS — Add agreed missing columns (idempotent / safe to rerun)
-- ============================================================================

-- 1) public.pay_batch_candidates: add updated_at
ALTER TABLE public.pay_batch_candidates
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

ALTER TABLE public.pay_batch_candidates
  ALTER COLUMN updated_at SET DEFAULT now();

UPDATE public.pay_batch_candidates pbc
SET updated_at = now()
WHERE pbc.updated_at IS NULL;

ALTER TABLE public.pay_batch_candidates
  ALTER COLUMN updated_at SET NOT NULL;


-- 2) public.pay_batch_items: add created_at, updated_at, is_mismatch
ALTER TABLE public.pay_batch_items
  ADD COLUMN IF NOT EXISTS created_at timestamptz;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN created_at SET DEFAULT now();

UPDATE public.pay_batch_items pbi
SET created_at = now()
WHERE pbi.created_at IS NULL;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN created_at SET NOT NULL;


ALTER TABLE public.pay_batch_items
  ADD COLUMN IF NOT EXISTS updated_at timestamptz;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN updated_at SET DEFAULT now();

UPDATE public.pay_batch_items pbi
SET updated_at = now()
WHERE pbi.updated_at IS NULL;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN updated_at SET NOT NULL;


ALTER TABLE public.pay_batch_items
  ADD COLUMN IF NOT EXISTS is_mismatch boolean;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN is_mismatch SET DEFAULT false;

UPDATE public.pay_batch_items pbi
SET is_mismatch = false
WHERE pbi.is_mismatch IS NULL;

ALTER TABLE public.pay_batch_items
  ALTER COLUMN is_mismatch SET NOT NULL;
