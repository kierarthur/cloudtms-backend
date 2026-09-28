-- PROPOSED repeatable (Worker AI, backfill; made self-contained by Worker AU, REL-INCLUDE). NOT committed, NOT installed.
-- Repeatable CloudTMS completion authority: banking_pay_stage2_backfill_complete_v1
-- Runs the Stage 2 backfill again AFTER 26092026_0204 (R4) has installed the maintaining triggers and BEFORE
-- 26092026_0209 (R9 grants). The body detects the installed triggers and runs in COMPLETE mode:
--   * catches up rows written by pre-Stage-2 code between the INITIAL (migration-phase) pass and R4, with every
--     trigger enabled (they behave exactly like an ordinary write);
--   * adds the pay_finance_case_components pair (bpay_finance_component_work_presence_v1 / _reference_v1) NOT VALID,
--     then validates every Stage 2 constraint;
--   * fails closed (BPAY_STAGE2_BACKFILL_DRIFT) if any stored value still differs from its trigger derivation.
-- Idempotent: a rerun after success writes 0 rows. On a blank NEW database it only adds and validates the pair.
-- No explicit begin/commit: the body commits per bounded batch.
-- SELF-CONTAINED (Worker AU, REL-INCLUDE): the release engine refuses an include that leaves supabase/repeatable, so
-- the former "\ir ../migrations/26092026_0210_banking_pay_stage2_backfill.sql" is replaced by a VERBATIM copy of that
-- migration's executable body: every byte from its first "\set ON_ERROR_STOP on" line to the end of the file
-- (31099 bytes, sha256 7652b63b3547c22b0fd3b21f9dc2441c0d24c8a9b3ebf272c3e7db6a1d2e0f81; the migration's leading comment block is not copied). The two must stay
-- byte-identical; the migration is immutable once locked, so a later change belongs in a new repeatable.
-- [Worker CA amendment ORPHAN-V5, 28 Sep 2026] Re-copied after the unpublished 0210 was amended (orphan items: counted
-- and written as recorded UUID + NULL key/revision instead of refused). Neither file was ever installed or published.

\set ON_ERROR_STOP on
\if :{?bpay_backfill_batch}
\else
\set bpay_backfill_batch 5000
\endif

set lock_timeout = '3s';
set statement_timeout = 0;
set jit = off;

-- 0. Preconditions: M1 installed; refuse data that no trigger could ever have produced (read-only census).
do $bpay_bf_pre$
declare
  v_missing text[];
  v_orphans bigint; v_orphan_ids text; v_lineage bigint; v_lineage_ids text;
begin
  select array_agg(x.n) into v_missing from (values
    ('public.pay_batch_items.bpay_direct_source_timesheet_id_v1'),('public.pay_batch_items.bpay_direct_work_key_v1'),
    ('public.pay_batch_items.bpay_direct_work_revision_v1'),('public.pay_finance_case_components.bpay_work_key_v1'),
    ('public.pay_finance_case_components.bpay_work_revision_v1'),('public.timesheet_pay_state.bpay_work_key_v1'),
    ('public.timesheet_payment_overrides.bpay_work_key_v1'),('public.timesheet_payment_overrides.bpay_clock_family_id_v1'),
    ('public.ts_pay_adjustments.bpay_work_key_v1'),('public.timesheets.bpay_work_key_v1'),('public.timesheets.bpay_work_revision_v1')
  ) x(n)
  where not exists (select 1 from pg_catalog.pg_attribute a
    where a.attrelid = pg_catalog.to_regclass(pg_catalog.split_part(x.n,'.',1)||'.'||pg_catalog.split_part(x.n,'.',2))
      and a.attname = pg_catalog.split_part(x.n,'.',3) and not a.attisdropped);
  -- [Worker AU amendment REL-GENCOL] the key column must be M1's maintained ordinary column, not a GENERATED one.
  if exists (select 1 from pg_catalog.pg_attribute a where a.attrelid = 'public.timesheets'::regclass
               and a.attname = 'bpay_work_key_v1' and a.attgenerated <> '')
     or not exists (select 1 from pg_catalog.pg_trigger t where t.tgrelid = 'public.timesheets'::regclass
               and t.tgname = 'zz_bpay_timesheet_work_key_v1' and not t.tgisinternal and t.tgenabled = 'A') then
    v_missing := coalesce(v_missing, '{}'::text[]) || 'public.timesheets.bpay_work_key_v1 maintained by zz_bpay_timesheet_work_key_v1 (ENABLE ALWAYS)'::text;
  end if;
  if v_missing is not null or pg_catalog.to_regclass('private.bpay_clock_families_v1') is null
     or pg_catalog.to_regclass('private.bpay_clock_rows_v1') is null
     or pg_catalog.to_regclass('private.bpay_protected_reference_heads_v1') is null
     or pg_catalog.to_regclass('private.bpay_protected_reference_events_v1') is null then
    raise exception 'BPAY_STAGE2_BACKFILL_REQUIRES_M1'
      using detail = coalesce(pg_catalog.array_to_string(v_missing, ','), 'private Stage 2 tables missing'),
            hint = 'This backfill must run after 26092026_0200_banking_pay_stage2_work_reference_schema.sql.';
  end if;

  -- [Worker CA amendment ORPHAN-V5] A pay item whose only Timesheet reference is a frozen UUID that no longer exists
  -- (a deleted Timesheet; hosted TEST held 165) is not refused: exactly like the amended item trigger, the items phase
  -- stores that recorded UUID with key and revision NULL, which is how today's code already treats it (it maps to that
  -- UUID and to nothing else). The count is reported, never guessed around. Lineage mismatches stay fail-closed.
  select pg_catalog.count(*), pg_catalog.string_agg(q.id::text, ',') filter (where q.rn <= 5)
    into v_orphans, v_orphan_ids
  from (select i.id, pg_catalog.row_number() over (order by i.id) rn
        from public.pay_batch_items i
        cross join lateral (select nullif(btrim(coalesce(i.frozen_source_basis_json->>'timesheet_id','')),'') fz) f
        where i.timesheet_id is null
          and f.fz ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and not exists (select 1 from public.timesheets t where t.timesheet_id =
            case when f.fz ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then f.fz::uuid end)) q;
  -- A Source lineage whose Timesheet no longer carries the lineage family booking: bpay_lineage_work_binding_v1 would
  -- have refused it (BPAY_LINEAGE_WORK_IDENTITY_MISMATCH).
  select pg_catalog.count(*), pg_catalog.string_agg(q.timesheet_id::text, ',') filter (where q.rn <= 5)
    into v_lineage, v_lineage_ids
  from (select l.timesheet_id, pg_catalog.row_number() over (order by l.timesheet_id) rn
        from public.weekly_source_row_timesheet_lineages l
        join public.timesheets t on t.timesheet_id = l.timesheet_id
        where t.booking_id is distinct from l.family_booking_id) q;
  raise notice 'BPAY_STAGE2_BACKFILL_ORPHAN_ITEMS pay_batch_items_orphan_frozen_timesheet=% orphan_item_sample=%',
    v_orphans, coalesce(v_orphan_ids, '');
  if v_lineage > 0 then
    raise exception 'BPAY_STAGE2_BACKFILL_UNDERIVABLE_ROWS'
      using detail = pg_catalog.jsonb_build_object(
        'pay_batch_items_orphan_frozen_timesheet', v_orphans, 'orphan_item_sample', v_orphan_ids,
        'lineage_booking_mismatch', v_lineage, 'lineage_timesheet_sample', v_lineage_ids)::text,
            hint = 'No trigger-equal value exists for these rows; an owner decision is required before install.';
  end if;
end
$bpay_bf_pre$;

-- 1. Session-local helpers (pg_temp: no catalogue/contract footprint; gone when the session ends).
create or replace function pg_temp.bpay_bf_triggers(p_rel regclass, p_disable boolean)
returns void language plpgsql as $f$
declare r record;
begin
  if p_disable then
    for r in select t.tgname, t.tgenabled from pg_catalog.pg_trigger t
             where t.tgrelid = p_rel and not t.tgisinternal and t.tgenabled in ('O','A') order by t.tgname loop
      insert into pg_temp.bpay_bf_disabled values (p_rel, r.tgname, r.tgenabled);
      execute pg_catalog.format('alter table %s disable trigger %I', p_rel, r.tgname);
    end loop;
  else
    for r in select d.tgname, d.state from pg_temp.bpay_bf_disabled d where d.rel = p_rel order by d.tgname loop
      execute pg_catalog.format(case r.state when 'A' then 'alter table %s enable always trigger %I'
                                             else 'alter table %s enable trigger %I' end, p_rel, r.tgname);
    end loop;
    delete from pg_temp.bpay_bf_disabled d where d.rel = p_rel;
  end if;
end $f$;

create temp table if not exists bpay_bf_disabled (rel regclass, tgname name, state "char");
create temp table if not exists bpay_bf_log (phase text, batches integer, rows_changed bigint, started timestamptz,
  finished timestamptz, max_batch_ms numeric, retries integer);
truncate pg_temp.bpay_bf_log;

-- One bounded batch of one phase. Returns rows written. p_lo exclusive, p_hi inclusive (NULL = open end).
create or replace function pg_temp.bpay_bf_batch(p_phase text, p_lo uuid, p_hi uuid, p_initial boolean)
returns bigint language plpgsql as $f$
declare n bigint := 0; m bigint; fam uuid; h record; e record; d bytea;
begin
  if p_phase = 'clock' then
    -- bpay_clock_rebuild_v1 for every Timesheet in the range: family per exact booking_id; row for a non-blank booking;
    -- authorisation_at = greatest(authorised_at_server, revoked_at, latest TSFIN authorised_at_utc); consumed_at = cache.
    insert into private.bpay_clock_families_v1(booking_id)
    select distinct t.booking_id from public.timesheets t
    where t.timesheet_id > p_lo and (p_hi is null or t.timesheet_id <= p_hi)
      and t.booking_id is not null and btrim(t.booking_id) <> ''
      and not exists (select 1 from private.bpay_clock_families_v1 f where f.booking_id = t.booking_id)
    on conflict (booking_id) do nothing;
    get diagnostics m = row_count; n := n + m;
    insert into private.bpay_clock_rows_v1 as r (timesheet_id, family_id, authorisation_at, consumed_at)
    select t.timesheet_id, f.id,
           greatest(t.authorised_at_server, t.revoked_at,
                    (select x.authorised_at_utc from public.timesheets_financials x where x.timesheet_id = t.timesheet_id
                     order by x.authorised_at_utc desc nulls last limit 1)),
           c.advance_authorisation_consumed_at_utc
    from public.timesheets t
    join private.bpay_clock_families_v1 f on f.booking_id = t.booking_id
    left join public.timesheet_summary_pay_state_cache c on c.timesheet_id = t.timesheet_id
    where t.timesheet_id > p_lo and (p_hi is null or t.timesheet_id <= p_hi)
      and t.booking_id is not null and btrim(t.booking_id) <> ''
    on conflict (timesheet_id) do update set family_id = excluded.family_id,
      authorisation_at = excluded.authorisation_at, consumed_at = excluded.consumed_at
    where (r.family_id, r.authorisation_at, r.consumed_at)
          is distinct from (excluded.family_id, excluded.authorisation_at, excluded.consumed_at);
    get diagnostics m = row_count; n := n + m;
    delete from private.bpay_clock_rows_v1 r using public.timesheets t
    where r.timesheet_id = t.timesheet_id and t.timesheet_id > p_lo and (p_hi is null or t.timesheet_id <= p_hi)
      and (t.booking_id is null or btrim(t.booking_id) = '');
    get diagnostics m = row_count; n := n + m;

  elsif p_phase = 'overrides' then
    if p_initial then perform pg_temp.bpay_bf_triggers('public.timesheet_payment_overrides'::regclass, true); end if;
    insert into private.bpay_clock_families_v1(booking_id)
    select distinct t.bpay_work_key_v1 from public.timesheet_payment_overrides o
    join public.timesheets t on t.timesheet_id = o.timesheet_id
    where o.id > p_lo and (p_hi is null or o.id <= p_hi) and t.bpay_work_key_v1 is not null
      and not exists (select 1 from private.bpay_clock_families_v1 f where f.booking_id = t.bpay_work_key_v1)
    on conflict (booking_id) do nothing;
    get diagnostics m = row_count; n := n + m;
    update public.timesheet_payment_overrides o
       set bpay_work_key_v1 = d.k, bpay_work_revision_v1 = d.rv, bpay_clock_family_id_v1 = d.fam
    from (select o2.id, t.bpay_work_key_v1 k, t.bpay_work_revision_v1 rv, f.id fam
          from public.timesheet_payment_overrides o2
          join public.timesheets t on t.timesheet_id = o2.timesheet_id
          left join private.bpay_clock_families_v1 f on f.booking_id = t.bpay_work_key_v1
          where o2.id > p_lo and (p_hi is null or o2.id <= p_hi)) d
    where o.id = d.id
      and (o.bpay_work_key_v1, o.bpay_work_revision_v1, o.bpay_clock_family_id_v1) is distinct from (d.k, d.rv, d.fam);
    get diagnostics m = row_count; n := n + m;
    if p_initial then perform pg_temp.bpay_bf_triggers('public.timesheet_payment_overrides'::regclass, false); end if;

  elsif p_phase = 'adjustments' then
    if p_initial then perform pg_temp.bpay_bf_triggers('public.ts_pay_adjustments'::regclass, true); end if;
    update public.ts_pay_adjustments a set bpay_work_key_v1 = t.bpay_work_key_v1, bpay_work_revision_v1 = t.bpay_work_revision_v1
    from public.timesheets t
    where t.timesheet_id = a.timesheet_id and a.id > p_lo and (p_hi is null or a.id <= p_hi)
      and (a.bpay_work_key_v1, a.bpay_work_revision_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1);
    get diagnostics m = row_count; n := n + m;
    if p_initial then perform pg_temp.bpay_bf_triggers('public.ts_pay_adjustments'::regclass, false); end if;

  elsif p_phase = 'pay_state' then
    if p_initial then perform pg_temp.bpay_bf_triggers('public.timesheet_pay_state'::regclass, true); end if;
    update public.timesheet_pay_state s set bpay_work_key_v1 = t.bpay_work_key_v1, bpay_work_revision_v1 = t.bpay_work_revision_v1
    from public.timesheets t
    where t.timesheet_id = s.timesheet_id and s.timesheet_id > p_lo and (p_hi is null or s.timesheet_id <= p_hi)
      and (s.bpay_work_key_v1, s.bpay_work_revision_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1);
    get diagnostics m = row_count; n := n + m;
    if p_initial then perform pg_temp.bpay_bf_triggers('public.timesheet_pay_state'::regclass, false); end if;

  elsif p_phase = 'components' then
    if p_initial then perform pg_temp.bpay_bf_triggers('public.pay_finance_case_components'::regclass, true); end if;
    update public.pay_finance_case_components c set bpay_work_key_v1 = d.k, bpay_work_revision_v1 = d.rv
    from (select c2.id, t.bpay_work_key_v1 k, t.bpay_work_revision_v1 rv
          from public.pay_finance_case_components c2
          left join public.timesheets t on t.timesheet_id = c2.linked_timesheet_id
          where c2.id > p_lo and (p_hi is null or c2.id <= p_hi)) d
    where c.id = d.id and (c.bpay_work_key_v1, c.bpay_work_revision_v1) is distinct from (d.k, d.rv);
    get diagnostics m = row_count; n := n + m;
    if p_initial then perform pg_temp.bpay_bf_triggers('public.pay_finance_case_components'::regclass, false); end if;

  elsif p_phase = 'items' then
    -- Identical precedence to bpay_item_work_reference_v1: timesheet_id, else a UUID-shaped frozen basis timesheet_id.
    -- [Worker CA amendment ORPHAN-V5] An orphan (recorded reference, Timesheet row gone) gets exactly what the amended
    -- trigger stores: the recorded UUID, key NULL, revision NULL (the LEFT JOIN yields NULL for both).
    if p_initial then perform pg_temp.bpay_bf_triggers('public.pay_batch_items'::regclass, true); end if;
    update public.pay_batch_items i
       set bpay_direct_source_timesheet_id_v1 = d.ref, bpay_direct_work_key_v1 = d.k, bpay_direct_work_revision_v1 = d.rv
    from (select r.id, r.ref, t.bpay_work_key_v1 k, t.bpay_work_revision_v1 rv
          from (select i2.id, coalesce(i2.timesheet_id,
                   case when f.fz ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then f.fz::uuid end) ref
                from public.pay_batch_items i2
                cross join lateral (select nullif(btrim(coalesce(i2.frozen_source_basis_json->>'timesheet_id','')),'') fz) f
                where i2.id > p_lo and (p_hi is null or i2.id <= p_hi)) r
          left join public.timesheets t on t.timesheet_id = r.ref) d
    where i.id = d.id
      and (i.bpay_direct_source_timesheet_id_v1, i.bpay_direct_work_key_v1, i.bpay_direct_work_revision_v1)
          is distinct from (d.ref, d.k, d.rv);
    get diagnostics m = row_count; n := n + m;
    if p_initial then perform pg_temp.bpay_bf_triggers('public.pay_batch_items'::regclass, false); end if;

  elsif p_phase = 'protected' then
    -- bpay_protected_reference_capture_v1 replayed: one ACTIVE event per approval at approved_at_utc (approvals are only
    -- ever inserted un-withdrawn) and one INACTIVE event at withdrawn_at_utc; events that are already in the chain are
    -- skipped; missing ones are appended after the current head in (time, active-before-inactive, approval id) order.
    for fam in select distinct a.pay_target_family_id from public.weekly_exceptional_payment_approvals a
               where a.pay_target_family_id > p_lo and (p_hi is null or a.pay_target_family_id <= p_hi) order by 1 loop
      perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('BPAY_APPROVAL_REFERENCE_V2:'||fam::text, 0));
      insert into private.bpay_protected_reference_heads_v1 values
        (fam, 0, pg_catalog.sha256(pg_catalog.convert_to('BPAY_APPROVAL_REFERENCE_V2_GENESIS','UTF8')||pg_catalog.uuid_send(fam)))
      on conflict do nothing;
      select * into strict h from private.bpay_protected_reference_heads_v1 where family_id = fam for update;
      m := 0;
      for e in
        select x.approval_id, x.approval_hash, x.active from (
          select a.id approval_id, a.approval_hash, true active, a.approved_at_utc at, 0 k
          from public.weekly_exceptional_payment_approvals a where a.pay_target_family_id = fam
          union all
          select a.id, a.approval_hash, false, a.withdrawn_at_utc, 1
          from public.weekly_exceptional_payment_approvals a where a.pay_target_family_id = fam and a.withdrawn_at_utc is not null) x
        where not exists (select 1 from private.bpay_protected_reference_events_v1 ev
                          where ev.family_id = fam and ev.approval_id = x.approval_id and ev.active = x.active)
        order by x.at, x.k, x.approval_id
      loop
        d := pg_catalog.sha256(pg_catalog.convert_to('BPAY_APPROVAL_REFERENCE_V2_EVENT','UTF8')||pg_catalog.uuid_send(fam)||
             pg_catalog.int8send(h.ordinal+1)||pg_catalog.uuid_send(e.approval_id)||e.approval_hash||pg_catalog.boolsend(e.active)||h.digest);
        insert into private.bpay_protected_reference_events_v1 values (fam, h.ordinal+1, e.approval_id, e.approval_hash, e.active, h.digest, d);
        h.ordinal := h.ordinal + 1; h.digest := d; m := m + 1;
      end loop;
      if m > 0 then
        update private.bpay_protected_reference_heads_v1 set ordinal = h.ordinal, digest = h.digest where family_id = fam;
        n := n + m;
      end if;
    end loop;

  elsif p_phase = 'retire' then
    -- bpay_clock_retire_family_v1: a family with no clock row and no override reference is removed.
    delete from private.bpay_clock_families_v1 f
    where f.id in (select f2.id from private.bpay_clock_families_v1 f2
                   where f2.id > p_lo and (p_hi is null or f2.id <= p_hi)
                     and not exists (select 1 from private.bpay_clock_rows_v1 c where c.family_id = f2.id)
                     and not exists (select 1 from public.timesheet_payment_overrides o where o.bpay_clock_family_id_v1 = f2.id)
                   for update);
    get diagnostics m = row_count; n := n + m;
  else
    raise exception 'BPAY_STAGE2_BACKFILL_UNKNOWN_PHASE %', p_phase;
  end if;
  return n;
end $f$;

-- Next exclusive upper bound of a keyset batch (NULL = the rest of the table).
create or replace function pg_temp.bpay_bf_next(p_phase text, p_lo uuid, p_size integer)
returns uuid language plpgsql stable as $f$
declare v uuid; v_sql text;
begin
  v_sql := case p_phase
    when 'clock' then 'select timesheet_id from public.timesheets where timesheet_id > $1 order by timesheet_id offset $2 limit 1'
    when 'overrides' then 'select id from public.timesheet_payment_overrides where id > $1 order by id offset $2 limit 1'
    when 'adjustments' then 'select id from public.ts_pay_adjustments where id > $1 order by id offset $2 limit 1'
    when 'pay_state' then 'select timesheet_id from public.timesheet_pay_state where timesheet_id > $1 order by timesheet_id offset $2 limit 1'
    when 'components' then 'select id from public.pay_finance_case_components where id > $1 order by id offset $2 limit 1'
    when 'items' then 'select id from public.pay_batch_items where id > $1 order by id offset $2 limit 1'
    when 'protected' then 'select pay_target_family_id from (select distinct pay_target_family_id from public.weekly_exceptional_payment_approvals where pay_target_family_id > $1) q order by 1 offset $2 limit 1'
    when 'retire' then 'select id from private.bpay_clock_families_v1 where id > $1 order by id offset $2 limit 1'
  end;
  execute v_sql into v using p_lo, p_size - 1;
  return v;
end $f$;

create or replace procedure pg_temp.bpay_bf_run(p_batch integer)
language plpgsql as $p$
declare
  v_initial boolean;
  v_phase text; v_lo uuid; v_hi uuid; v_size integer; v_changed bigint; v_total bigint; v_batches integer;
  v_attempt integer; v_retries integer; v_started timestamptz; v_b0 timestamptz; v_max numeric;
begin
  if p_batch is null or p_batch < 25 or p_batch > 50000 then raise exception 'BPAY_STAGE2_BACKFILL_BATCH_OUT_OF_RANGE %', p_batch; end if;
  v_initial := not exists (select 1 from pg_catalog.pg_trigger where tgrelid = 'public.pay_batch_items'::regclass
                           and tgname = 'bpay_item_work_reference_v1' and not tgisinternal);
  raise notice 'BPAY_STAGE2_BACKFILL mode=% batch=%', case when v_initial then 'INITIAL' else 'COMPLETE' end, p_batch;
  -- clock first (it creates the families that overrides reference), retire last.
  foreach v_phase in array array['clock','overrides','adjustments','pay_state','components','items','protected','retire'] loop
    v_size := case when v_phase = 'protected' then greatest(p_batch / 100, 1) else p_batch end;
    v_lo := '00000000-0000-0000-0000-000000000000'::uuid; v_total := 0; v_batches := 0; v_retries := 0; v_max := 0;
    v_started := pg_catalog.clock_timestamp();
    loop
      v_hi := pg_temp.bpay_bf_next(v_phase, v_lo, v_size);
      v_attempt := 0;
      loop
        v_b0 := pg_catalog.clock_timestamp();
        begin
          v_changed := pg_temp.bpay_bf_batch(v_phase, v_lo, v_hi, v_initial);
          exit;
        exception when lock_not_available or deadlock_detected then
          v_attempt := v_attempt + 1; v_retries := v_retries + 1;
          if v_attempt >= 5 then raise; end if;
          perform pg_catalog.pg_sleep(v_attempt);
        end;
      end loop;
      v_max := greatest(v_max, extract(epoch from pg_catalog.clock_timestamp() - v_b0) * 1000);
      v_total := v_total + v_changed; v_batches := v_batches + 1;
      commit;
      exit when v_hi is null;
      v_lo := v_hi;
    end loop;
    insert into pg_temp.bpay_bf_log values (v_phase, v_batches, v_total, v_started, pg_catalog.clock_timestamp(), round(v_max, 1), v_retries);
    commit;
    raise notice 'BPAY_STAGE2_BACKFILL phase=% batches=% rows_written=% seconds=% max_batch_ms=% retries=%', v_phase, v_batches, v_total,
      round(extract(epoch from pg_catalog.clock_timestamp() - v_started)::numeric, 3), round(v_max, 1), v_retries;
  end loop;
end $p$;

call pg_temp.bpay_bf_run(:bpay_backfill_batch);

-- 2. COMPLETE mode only: the finance-component pair that M1 defers (old code inserting a linked component between M1 and
-- R4 would otherwise fail on the CHECK). Same definitions as the tested extract. NOT VALID first (brief lock, no scan).
do $bpay_bf_fc$
begin
  if exists (select 1 from pg_catalog.pg_trigger where tgrelid = 'public.pay_batch_items'::regclass
             and tgname = 'bpay_item_work_reference_v1' and not tgisinternal) then
    if not exists (select 1 from pg_catalog.pg_constraint where conrelid = 'public.pay_finance_case_components'::regclass
                   and conname = 'bpay_finance_component_work_presence_v1') then
      alter table public.pay_finance_case_components add constraint bpay_finance_component_work_presence_v1
        check (((linked_timesheet_id is null) = (bpay_work_revision_v1 is null))) not valid;
    end if;
    if not exists (select 1 from pg_catalog.pg_constraint where conrelid = 'public.pay_finance_case_components'::regclass
                   and conname = 'bpay_finance_component_work_reference_v1') then
      alter table public.pay_finance_case_components add constraint bpay_finance_component_work_reference_v1
        foreign key (linked_timesheet_id, bpay_work_revision_v1) references public.timesheets(timesheet_id, bpay_work_revision_v1)
        match full not valid;
    end if;
  end if;
end
$bpay_bf_fc$;

-- 3. Validate every Stage 2 constraint that is still NOT VALID (SHARE UPDATE EXCLUSIVE: ordinary DML continues).
select pg_catalog.format('alter table %s validate constraint %I', c.conrelid::regclass, c.conname)
from pg_catalog.pg_constraint c
where not c.convalidated and c.contype in ('c','f')
  and c.conname in ('bpay_item_work_reference_presence_v1','bpay_item_work_reference_v1',
    'bpay_finance_component_work_presence_v1','bpay_finance_component_work_reference_v1',
    'bpay_timesheet_pay_state_work_fk','bpay_timesheet_payment_overrides_work_fk',
    'timesheet_payment_overrides_bpay_clock_family_id_v1_fkey','bpay_ts_pay_adjustments_work_fk',
    'bpay_protected_root_work_v1','bpay_authorisation_work_reference_v1','bpay_evidence_shape_v1',
    'bpay_lineage_work_revision_fk_v1','bpay_timesheet_work_key_expression_v1')
order by c.conrelid::regclass::text, c.conname
\gexec

-- 4. Read-only verification: stored == derived for every row. COMPLETE mode fails closed on any drift; INITIAL mode
-- reports it (rows written by old code after their batch are expected and are caught up in COMPLETE mode).
do $bpay_bf_verify$
declare v jsonb; v_initial boolean;
begin
  v_initial := not exists (select 1 from pg_catalog.pg_trigger where tgrelid = 'public.pay_batch_items'::regclass
                           and tgname = 'bpay_item_work_reference_v1' and not tgisinternal);
  select pg_catalog.jsonb_build_object(
    'timesheet_key', (select count(*) from public.timesheets t
      where t.bpay_work_key_v1 is distinct from case when t.booking_id is not null and btrim(t.booking_id) <> '' then t.booking_id end),
    -- [Worker CA amendment ORPHAN-V5] an orphan's derivation is (recorded UUID, NULL, NULL) through the LEFT JOIN, so
    -- exactly that shape passes and any other stored value for it is drift.
    'items', (select count(*) from public.pay_batch_items i
      cross join lateral (select nullif(btrim(coalesce(i.frozen_source_basis_json->>'timesheet_id','')),'') fz) f
      cross join lateral (select coalesce(i.timesheet_id, case when f.fz ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then f.fz::uuid end) ref) r
      left join public.timesheets t on t.timesheet_id = r.ref
      where (i.bpay_direct_source_timesheet_id_v1, i.bpay_direct_work_key_v1, i.bpay_direct_work_revision_v1)
            is distinct from (r.ref, t.bpay_work_key_v1, t.bpay_work_revision_v1)),
    'components', (select count(*) from public.pay_finance_case_components c left join public.timesheets t on t.timesheet_id = c.linked_timesheet_id
      where (c.bpay_work_key_v1, c.bpay_work_revision_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1)),
    'pay_state', (select count(*) from public.timesheet_pay_state s join public.timesheets t using (timesheet_id)
      where (s.bpay_work_key_v1, s.bpay_work_revision_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1)),
    'adjustments', (select count(*) from public.ts_pay_adjustments a join public.timesheets t using (timesheet_id)
      where (a.bpay_work_key_v1, a.bpay_work_revision_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1)),
    'overrides', (select count(*) from public.timesheet_payment_overrides o join public.timesheets t using (timesheet_id)
      left join private.bpay_clock_families_v1 f on f.booking_id = t.bpay_work_key_v1
      where (o.bpay_work_key_v1, o.bpay_work_revision_v1, o.bpay_clock_family_id_v1) is distinct from (t.bpay_work_key_v1, t.bpay_work_revision_v1, f.id)),
    'clock_missing_or_wrong', (select count(*) from public.timesheets t
      left join private.bpay_clock_rows_v1 r on r.timesheet_id = t.timesheet_id
      left join private.bpay_clock_families_v1 f on f.id = r.family_id
      left join public.timesheet_summary_pay_state_cache c on c.timesheet_id = t.timesheet_id
      where t.booking_id is not null and btrim(t.booking_id) <> ''
        and (f.booking_id, r.authorisation_at, r.consumed_at) is distinct from (t.booking_id,
             greatest(t.authorised_at_server, t.revoked_at, (select x.authorised_at_utc from public.timesheets_financials x
               where x.timesheet_id = t.timesheet_id order by x.authorised_at_utc desc nulls last limit 1)),
             c.advance_authorisation_consumed_at_utc)),
    'clock_extra', (select count(*) from private.bpay_clock_rows_v1 r join public.timesheets t using (timesheet_id)
      where t.booking_id is null or btrim(t.booking_id) = ''),
    'clock_orphan_families', (select count(*) from private.bpay_clock_families_v1 f
      where not exists (select 1 from private.bpay_clock_rows_v1 r where r.family_id = f.id)
        and not exists (select 1 from public.timesheet_payment_overrides o where o.bpay_clock_family_id_v1 = f.id)),
    'protected_missing_events', (select count(*) from (
        select a.pay_target_family_id f, a.id, true act from public.weekly_exceptional_payment_approvals a
        union all select a.pay_target_family_id, a.id, false from public.weekly_exceptional_payment_approvals a where a.withdrawn_at_utc is not null) x
      where not exists (select 1 from private.bpay_protected_reference_events_v1 e where e.family_id = x.f and e.approval_id = x.id and e.active = x.act)),
    'protected_head_mismatch', (select count(*) from private.bpay_protected_reference_heads_v1 h
      cross join lateral (select coalesce(max(e.ordinal), 0) mo from private.bpay_protected_reference_events_v1 e where e.family_id = h.family_id) m
      cross join lateral (select coalesce((select e2.digest from private.bpay_protected_reference_events_v1 e2
          where e2.family_id = h.family_id order by e2.ordinal desc limit 1),
          pg_catalog.sha256(pg_catalog.convert_to('BPAY_APPROVAL_REFERENCE_V2_GENESIS','UTF8')||pg_catalog.uuid_send(h.family_id))) md) g
      where (h.ordinal, h.digest) is distinct from (m.mo, g.md)),
    'protected_chain_links', (select count(*) from private.bpay_protected_reference_events_v1 e
      where e.digest is distinct from pg_catalog.sha256(pg_catalog.convert_to('BPAY_APPROVAL_REFERENCE_V2_EVENT','UTF8')||pg_catalog.uuid_send(e.family_id)||
              pg_catalog.int8send(e.ordinal)||pg_catalog.uuid_send(e.approval_id)||e.approval_hash||pg_catalog.boolsend(e.active)||e.previous_digest)
         or e.previous_digest is distinct from coalesce((select p.digest from private.bpay_protected_reference_events_v1 p
              where p.family_id = e.family_id and p.ordinal = e.ordinal - 1),
              pg_catalog.sha256(pg_catalog.convert_to('BPAY_APPROVAL_REFERENCE_V2_GENESIS','UTF8')||pg_catalog.uuid_send(e.family_id)))),
    'revision_copies', (select count(*) from public.weekly_source_row_timesheet_lineages l join public.timesheets t using (timesheet_id)
        where l.bpay_lineage_work_revision_v1 <> t.bpay_work_revision_v1)
      + (select count(*) from public.weekly_source_root_authorisations a join public.timesheets t on t.timesheet_id = a.root_timesheet_id
        where a.bpay_root_work_revision_v1 <> t.bpay_work_revision_v1)
      + (select count(*) from public.weekly_exceptional_pay_target_families p join public.timesheets t on t.timesheet_id = p.root_timesheet_id
        where p.bpay_root_work_revision_v1 <> t.bpay_work_revision_v1),
    'not_valid_constraints', (select count(*) from pg_catalog.pg_constraint c where not c.convalidated
      and c.conname in ('bpay_item_work_reference_presence_v1','bpay_item_work_reference_v1',
        'bpay_finance_component_work_presence_v1','bpay_finance_component_work_reference_v1',
        'bpay_timesheet_pay_state_work_fk','bpay_timesheet_payment_overrides_work_fk',
        'timesheet_payment_overrides_bpay_clock_family_id_v1_fkey','bpay_ts_pay_adjustments_work_fk',
        'bpay_protected_root_work_v1','bpay_authorisation_work_reference_v1','bpay_evidence_shape_v1',
        'bpay_lineage_work_revision_fk_v1','bpay_timesheet_work_key_expression_v1'))
  ) into v;
  if exists (select 1 from pg_catalog.jsonb_each_text(v) e where e.value::bigint <> 0) then
    if v_initial then
      raise notice 'BPAY_STAGE2_BACKFILL_INITIAL_DRIFT %', v;
    else
      raise exception 'BPAY_STAGE2_BACKFILL_DRIFT' using detail = v::text;
    end if;
  else
    raise notice 'BPAY_STAGE2_BACKFILL_VERIFIED %', v;
  end if;
end
$bpay_bf_verify$;

select phase, batches, rows_changed, round(extract(epoch from finished - started)::numeric, 3) as seconds, max_batch_ms, retries
from pg_temp.bpay_bf_log order by started;
