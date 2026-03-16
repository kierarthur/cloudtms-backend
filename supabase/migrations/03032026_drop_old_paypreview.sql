-- Drop the legacy 5-arg overload of public.pay_preview
-- Safe to rerun.
DROP FUNCTION IF EXISTS public.pay_preview(
  date,            -- p_pay_date
  date,            -- p_week_ending_cutoff
  uuid,            -- p_actor_user_id
  uuid,            -- p_candidate_id
  uuid             -- p_client_id
);
