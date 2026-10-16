begin read only;
set local lock_timeout='2s';
lock table public.weekly_message_dispatch_commands in access share mode nowait;
-- Retain until writer expected refusal, then ROLLBACK; never commit.
