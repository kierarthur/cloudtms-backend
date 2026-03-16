-- Drop the legacy overload (7-arg signature) so only the override-capable variant remains.
-- Safe to rerun.
DROP FUNCTION IF EXISTS public.pay_create_draft_batch(
  date,         -- p_pay_date
  date,         -- p_week_ending_cutoff
  text,         -- p_pay_channel_scope
  uuid,         -- p_actor_user_id
  jsonb,        -- p_preview_decisions_json
  uuid,         -- p_candidate_id
  uuid          -- p_client_id
);
