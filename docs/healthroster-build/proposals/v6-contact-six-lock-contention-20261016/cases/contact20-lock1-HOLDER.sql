begin read only;
set local lock_timeout='2s';
lock table public.weekly_manager_recipient_generations in access share mode nowait;
-- Retain until writer expected refusal, then ROLLBACK; never commit.
