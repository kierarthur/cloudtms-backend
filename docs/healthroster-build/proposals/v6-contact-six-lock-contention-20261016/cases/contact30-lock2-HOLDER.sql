begin read only;
set local lock_timeout='2s';
lock table public.weekly_timesheet_submission_requests in access share mode nowait;
-- Retain until writer expected refusal, then ROLLBACK; never commit.
