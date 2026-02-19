-- =============================================================================
-- CloudTMS: Multi-user freshness (heartbeat) + Candidate working-status rollup
-- =============================================================================
-- Sources (existing objects):
-- - candidates_summary view exists.  (public.candidates_summary) :contentReference[oaicite:1]{index=1}
-- - timesheets_financials + timesheets exist and are used for candidate_id + week_ending_date. 
-- - picker client delta uses clients.rev > since (so client_settings changes must bump clients.rev). :contentReference[oaicite:3]{index=3}
-- =============================================================================

begin;

-- -----------------------------------------------------------------------------
-- 1) HEARTBEAT COUNTERS TABLE
-- -----------------------------------------------------------------------------
create table if not exists public.app_change_counters (
  entity_key text primary key,
  seq bigint not null default 0,
  updated_at timestamptz not null default now()
);

-- Ensure baseline rows exist (idempotent)
insert into public.app_change_counters(entity_key, seq)
values
  ('candidates', 0),
  ('clients', 0),
  ('umbrellas', 0),
  ('contracts', 0),
  ('timesheets', 0),
  ('invoices', 0)
on conflict (entity_key) do nothing;

-- -----------------------------------------------------------------------------
-- 2) HEARTBEAT INTERNAL BUMP FUNCTION + GENERIC TRIGGER
-- -----------------------------------------------------------------------------
create or replace function public._change_bump(p_entity_key text)
returns void
language plpgsql
as $$
begin
  if p_entity_key is null or btrim(p_entity_key) = '' then
    return;
  end if;

  update public.app_change_counters c
     set seq = c.seq + 1,
         updated_at = now()
   where c.entity_key = p_entity_key;

  if not found then
    insert into public.app_change_counters(entity_key, seq, updated_at)
    values (p_entity_key, 1, now())
    on conflict (entity_key) do update
      set seq = public.app_change_counters.seq + 1,
          updated_at = now();
  end if;
end;
$$;

create or replace function public._trg_change_bump()
returns trigger
language plpgsql
as $$
declare
  v_entity_key text;
begin
  v_entity_key := nullif(btrim(coalesce(tg_argv[0], '')), '');

  perform public._change_bump(v_entity_key);

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$$;

-- -----------------------------------------------------------------------------
-- 3) HEARTBEAT RPC: tiny, fast, and safe to poll
--    Frontend can send last-seen map; RPC returns seqs + changed list.
-- -----------------------------------------------------------------------------
create or replace function public.rpc_changes_ping(p_last_seen jsonb default '{}'::jsonb)
returns jsonb
language plpgsql
stable
as $$
declare
  v_seqs jsonb := '{}'::jsonb;
  v_changed text[] := array[]::text[];
  v_prev bigint;
  v_cur  bigint;
  r record;
begin
  for r in
    select c.entity_key, c.seq
    from public.app_change_counters c
    order by c.entity_key
  loop
    v_cur := coalesce(r.seq, 0);
    v_prev := 0;

    begin
      v_prev := coalesce((p_last_seen ->> r.entity_key)::bigint, 0);
    exception when others then
      v_prev := 0;
    end;

    v_seqs := v_seqs || jsonb_build_object(r.entity_key, v_cur);

    if v_cur > v_prev then
      v_changed := array_append(v_changed, r.entity_key);
    end if;
  end loop;

  return jsonb_build_object(
    'server_utc', now(),
    'seqs', v_seqs,
    'changed', to_jsonb(v_changed)
  );
end;
$$;

-- -----------------------------------------------------------------------------
-- 4) TRIGGERS: bump counters when key entities change
--    (Keep this set lean; it’s statement/row-level but cheap.)
-- -----------------------------------------------------------------------------

-- CANDIDATES
drop trigger if exists trg_cc_candidates on public.candidates;
create trigger trg_cc_candidates
after insert or update or delete on public.candidates
for each row execute function public._trg_change_bump('candidates');

drop trigger if exists trg_cc_candidates_tombstones on public.candidates_tombstones;
create trigger trg_cc_candidates_tombstones
after insert or update or delete on public.candidates_tombstones
for each row execute function public._trg_change_bump('candidates');

-- If you want candidate list “updates available” when job titles mapping changes:
drop trigger if exists trg_cc_candidate_job_titles on public.candidate_job_titles;
create trigger trg_cc_candidate_job_titles
after insert or update or delete on public.candidate_job_titles
for each row execute function public._trg_change_bump('candidates');

-- CLIENTS
drop trigger if exists trg_cc_clients on public.clients;
create trigger trg_cc_clients
after insert or update or delete on public.clients
for each row execute function public._trg_change_bump('clients');

drop trigger if exists trg_cc_clients_tombstones on public.clients_tombstones;
create trigger trg_cc_clients_tombstones
after insert or update or delete on public.clients_tombstones
for each row execute function public._trg_change_bump('clients');

-- UMBRELLAS
drop trigger if exists trg_cc_umbrellas on public.umbrellas;
create trigger trg_cc_umbrellas
after insert or update or delete on public.umbrellas
for each row execute function public._trg_change_bump('umbrellas');

-- CONTRACTS (+ calendar-related child table)
drop trigger if exists trg_cc_contracts on public.contracts;
create trigger trg_cc_contracts
after insert or update or delete on public.contracts
for each row execute function public._trg_change_bump('contracts');

drop trigger if exists trg_cc_contract_weeks on public.contract_weeks;
create trigger trg_cc_contract_weeks
after insert or update or delete on public.contract_weeks
for each row execute function public._trg_change_bump('contracts');

-- TIMESHEETS
drop trigger if exists trg_cc_timesheets on public.timesheets;
create trigger trg_cc_timesheets
after insert or update or delete on public.timesheets
for each row execute function public._trg_change_bump('timesheets');

drop trigger if exists trg_cc_timesheets_financials on public.timesheets_financials;
create trigger trg_cc_timesheets_financials
after insert or update or delete on public.timesheets_financials
for each row execute function public._trg_change_bump('timesheets');

-- INVOICES (invoice_lines affects what user sees)
drop trigger if exists trg_cc_invoices on public.invoices;
create trigger trg_cc_invoices
after insert or update or delete on public.invoices
for each row execute function public._trg_change_bump('invoices');

drop trigger if exists trg_cc_invoice_lines on public.invoice_lines;
create trigger trg_cc_invoice_lines
after insert or update or delete on public.invoice_lines
for each row execute function public._trg_change_bump('invoices');

-- -----------------------------------------------------------------------------
-- 5) CRITICAL: keep picker client delta correct when client_settings changes
--    Your picker delta uses: clients.rev > since. :contentReference[oaicite:4]{index=4}
--    So changes to client_settings MUST bump the parent clients row rev/updated_at.
-- -----------------------------------------------------------------------------
create or replace function public._trg_touch_client_from_client_settings()
returns trigger
language plpgsql
as $$
declare
  v_client_id uuid;
begin
  v_client_id := coalesce(new.client_id, old.client_id);

  if v_client_id is not null then
    update public.clients cl
       set rev = coalesce(cl.rev, 0) + 1,
           updated_at = now()
     where cl.id = v_client_id;
  end if;

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$$;

drop trigger if exists trg_touch_client_from_client_settings on public.client_settings;
create trigger trg_touch_client_from_client_settings
after insert or update or delete on public.client_settings
for each row execute function public._trg_touch_client_from_client_settings();

-- -----------------------------------------------------------------------------
-- 6) Candidate working / recently worked rollup views (fast filters)
--    Uses contracts + timesheets_financials join timesheets for week_ending_date.
-- -----------------------------------------------------------------------------

create or replace view public.candidate_activity_rollup as
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
),
contract_active as (
  select
    con.candidate_id as candidate_id,
    bool_or(
      con.start_date <= (select a.anchor_ymd from anchor a)
      and coalesce(con.end_date, (select a.anchor_ymd from anchor a)) >= (select a.anchor_ymd from anchor a)
    ) as is_currently_working
  from public.contracts con
  where con.candidate_id is not null
  group by con.candidate_id
),
last_ts as (
  select
    tf.candidate_id as candidate_id,
    max(ts.week_ending_date)::date as last_timesheet_week_ending
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  where tf.is_current = true
    and tf.candidate_id is not null
  group by tf.candidate_id
)
select
  cand.id as candidate_id,
  coalesce(ca.is_currently_working, false) as is_currently_working,
  lt.last_timesheet_week_ending as last_timesheet_week_ending
from public.candidates cand
left join contract_active ca
  on ca.candidate_id = cand.id
left join last_ts lt
  on lt.candidate_id = cand.id;

-- Extend candidates_summary with rollup columns for fast server-side filtering
create or replace view public.candidates_summary_activity as
select
  cs.*,
  car.is_currently_working,
  car.last_timesheet_week_ending
from public.candidates_summary cs
left join public.candidate_activity_rollup car
  on car.candidate_id = cs.id;

-- -----------------------------------------------------------------------------
-- 7) Performance indexes for rollup (idempotent)
-- -----------------------------------------------------------------------------

-- Faster "currently working" lookup by candidate
create index if not exists idx_contracts_candidate_dates
  on public.contracts using btree (candidate_id, start_date, end_date);

-- Ensure we can group by candidate quickly for current TSFIN rows
create index if not exists idx_tsfin_candidate_current
  on public.timesheets_financials using btree (candidate_id)
  where is_current = true;

commit;
