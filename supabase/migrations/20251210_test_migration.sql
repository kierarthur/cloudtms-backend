-- 20251210_test_migration.sql
-- Simple test table to confirm GitHub → Supabase migrations work.

create table if not exists migration_test (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now()
);
