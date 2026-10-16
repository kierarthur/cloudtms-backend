begin read write;
set local lock_timeout='2s';
lock table public.weekly_candidate_outreach_generations in row exclusive mode nowait;
-- Retain until writer expected refusal, then ROLLBACK; never commit.
