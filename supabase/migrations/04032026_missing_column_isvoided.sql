ALTER TABLE public.pay_batch_items
ADD COLUMN IF NOT EXISTS is_voided boolean NOT NULL DEFAULT false;
