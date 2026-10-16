begin read write;
set local lock_timeout='2s';
lock table public.weekly_candidate_cohorts in row exclusive mode nowait;
-- Retain until writer expected refusal, then ROLLBACK; never commit.
