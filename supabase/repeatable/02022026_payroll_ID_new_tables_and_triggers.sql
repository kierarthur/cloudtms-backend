CREATE OR REPLACE FUNCTION public.id_ledger_list(
  p_limit integer DEFAULT 50,
  p_offset integer DEFAULT 0,
  p_status text[] DEFAULT NULL,
  p_client_id uuid DEFAULT NULL,
  p_search text DEFAULT NULL,
  p_only_reportable boolean DEFAULT false,
  p_only_changed boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_limit int := greatest(1, least(coalesce(p_limit, 50), 500));
  v_offset int := greatest(coalesce(p_offset, 0), 0);

  v_statuses text[] := null;
  v_search text := nullif(btrim(coalesce(p_search, '')), '');

  v_total_count int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  -- Normalise status filter to uppercase trimmed values (ignore blanks)
  if p_status is not null then
    select
      array_agg(upper(btrim(x)) order by upper(btrim(x)))
    into v_statuses
    from unnest(p_status) as x
    where nullif(btrim(coalesce(x, '')), '') is not null;

    if v_statuses is not null and array_length(v_statuses, 1) = 0 then
      v_statuses := null;
    end if;
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      l.current_ex_vat,
      l.current_vat,
      l.current_inc_vat,

      l.last_reported_ex_vat,
      l.last_reported_vat,
      l.last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat, 0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat, 0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat, 0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  filtered0 as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat, 0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat, 0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat, 0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      (v_statuses is null or upper(coalesce(c.effective_invoice_status, '')) = any(v_statuses))
      and (p_client_id is null or c.client_id = p_client_id)
      and (
        v_search is null
        or coalesce(c.effective_invoice_number, '') ilike ('%' || v_search || '%')
        or coalesce(c.client_name, '') ilike ('%' || v_search || '%')
      )
      and (
        coalesce(p_only_reportable, false) = false
        or upper(coalesce(c.effective_invoice_status, '')) <> 'ON_HOLD'
      )
  ),
  filtered as (
    select
      f0.*
    from filtered0 f0
    where
      coalesce(p_only_changed, false) = false
      or (
        f0.delta_ex_vat <> 0::numeric(12,2)
        or f0.delta_vat <> 0::numeric(12,2)
        or f0.delta_inc_vat <> 0::numeric(12,2)
      )
  ),
  total as (
    select count(*)::int as total_count
    from filtered f
  ),
  page as (
    select
      f.*
    from filtered f
    order by
      f.sort_ts desc nulls last,
      nullif(btrim(coalesce(f.effective_invoice_number, '')), '') desc nulls last,
      f.invoice_id desc
    limit v_limit offset v_offset
  )
  select
    coalesce((select t.total_count from total t), 0),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', p.invoice_id::text,
          'invoice_number', p.effective_invoice_number,
          'invoice_status', p.effective_invoice_status,
          'invoice_type', p.effective_invoice_type,

          'client_id', case when p.client_id is null then null else p.client_id::text end,
          'client_name', p.client_name,

          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'paid_at_utc', p.paid_at_utc,
          'status_date_utc', p.status_date_utc,
          'credit_note_created_at_utc', p.credit_note_created_at_utc,

          'updated_at_utc', p.updated_at_utc,

          'current_ex_vat', coalesce(p.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(p.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(p.current_inc_vat, 0)::numeric(12,2),

          'last_reported_ex_vat', coalesce(p.last_reported_ex_vat, 0)::numeric(12,2),
          'last_reported_vat', coalesce(p.last_reported_vat, 0)::numeric(12,2),
          'last_reported_inc_vat', coalesce(p.last_reported_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat', p.reportable_current_ex_vat,
          'reportable_current_vat', p.reportable_current_vat,
          'reportable_current_inc_vat', p.reportable_current_inc_vat,

          'delta_ex_vat', p.delta_ex_vat,
          'delta_vat', p.delta_vat,
          'delta_inc_vat', p.delta_inc_vat,

          'line_kind', p.line_kind,
          'non_reportable_reason', p.non_reportable_reason
        )
        order by
          p.sort_ts desc nulls last,
          nullif(btrim(coalesce(p.effective_invoice_number, '')), '') desc nulls last,
          p.invoice_id desc
      ),
      '[]'::jsonb
    )
  into v_total_count, v_rows
  from page p;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total_count,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$function$;





create or replace function public.id_ledger_upsert_from_invoice_row(
  p_invoice_id uuid,
  p_set_zero boolean default false,
  p_invoice_no text default null,
  p_status_text text default null,
  p_type_text text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_inv record;

  v_invoice_no text;
  v_status_text text;
  v_type_text text;

  v_ex numeric := 0;
  v_vat numeric := 0;
  v_inc numeric := 0;
begin
  -- Defensive: avoid breaking triggers if called with NULL
  if p_invoice_id is null then
    return;
  end if;

  -- Prefer reading the current invoices row when present (normal path).
  select
    i.invoice_no,
    i.status::text as status_text,
    i.type::text as type_text,
    coalesce(i.subtotal_ex_vat,0)::numeric as subtotal_ex_vat,
    coalesce(i.vat_amount,0)::numeric as vat_amount,
    coalesce(i.total_inc_vat,0)::numeric as total_inc_vat
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if found then
    v_invoice_no := nullif(btrim(coalesce(v_inv.invoice_no, '')), '');
    v_status_text := nullif(btrim(coalesce(v_inv.status_text, '')), '');
    v_type_text := nullif(btrim(coalesce(v_inv.type_text, '')), '');

    if p_set_zero then
      v_ex := 0; v_vat := 0; v_inc := 0;
    else
      v_ex := coalesce(v_inv.subtotal_ex_vat,0);
      v_vat := coalesce(v_inv.vat_amount,0);
      v_inc := coalesce(v_inv.total_inc_vat,0);

      -- Safety: if a CREDIT_NOTE ever ends up stored as positive totals, force negative.
      -- (If credit notes are already stored as signed totals, this is a no-op.)
      if v_type_text = 'CREDIT_NOTE' then
        if v_ex > 0 then v_ex := -1 * v_ex; end if;
        if v_vat > 0 then v_vat := -1 * v_vat; end if;
        if v_inc > 0 then v_inc := -1 * v_inc; end if;
      end if;
    end if;

  else
    -- Invoice row not found (e.g. already deleted): use provided snapshots and zero totals.
    v_invoice_no := nullif(btrim(coalesce(p_invoice_no, '')), '');
    v_status_text := nullif(btrim(coalesce(p_status_text, '')), '');
    v_type_text := nullif(btrim(coalesce(p_type_text, '')), '');

    v_ex := 0; v_vat := 0; v_inc := 0;
  end if;

  insert into public.id_invoice_ledger (
    invoice_id,
    invoice_number,
    invoice_status,
    invoice_type,
    current_ex_vat,
    current_vat,
    current_inc_vat,
    updated_at_utc
  )
  values (
    p_invoice_id,
    v_invoice_no,
    v_status_text,
    v_type_text,
    round(coalesce(v_ex,0)::numeric,2),
    round(coalesce(v_vat,0)::numeric,2),
    round(coalesce(v_inc,0)::numeric,2),
    now()
  )
  on conflict (invoice_id) do update
  set
    invoice_number   = excluded.invoice_number,
    invoice_status   = excluded.invoice_status,
    invoice_type     = excluded.invoice_type,
    current_ex_vat   = excluded.current_ex_vat,
    current_vat      = excluded.current_vat,
    current_inc_vat  = excluded.current_inc_vat,
    updated_at_utc   = excluded.updated_at_utc;

end;
$$;


begin;

-- =========================================================
-- Helpers: Ledger upsert from invoices
-- =========================================================


-- Recompute totals then upsert ledger.
create or replace function public.id_ledger_recompute_and_sync_invoice(p_invoice_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Canonical totals recompute; this also clears invoice_pdf_r2_key. :contentReference[oaicite:3]{index=3}
  begin
    perform public.invoice_recompute_totals(p_invoice_id);
  exception when others then
    -- If invoice missing or recompute fails, fall back to a safe ledger upsert with zeros.
    -- (We do NOT raise: ledger must not break invoice_lines operations.)
    perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, true, null, null, null);
    return;
  end;

  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$$;

-- Metadata-only sync (no recompute). Used for invoices INSERT/UPDATE to keep status/type/invoice_no current.
create or replace function public.id_ledger_sync_invoice_metadata(p_invoice_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$$;

-- =========================================================
-- A2.1 invoice_lines triggers (AFTER INSERT/UPDATE/DELETE)
-- Statement-level triggers with transition tables so we recompute ONCE per invoice per statement.
-- =========================================================

create or replace function public.trg_id_invoice_lines_ai_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct nr.invoice_id
    from new_rows nr
    where nr.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

create or replace function public.trg_id_invoice_lines_au_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    with ids as (
      select invoice_id from new_rows where invoice_id is not null
      union
      select invoice_id from old_rows where invoice_id is not null
    )
    select distinct invoice_id from ids
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

create or replace function public.trg_id_invoice_lines_ad_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct orw.invoice_id
    from old_rows orw
    where orw.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

-- Drop+recreate triggers (safe to rerun)
drop trigger if exists trg_id_invoice_lines_ai on public.invoice_lines;
drop trigger if exists trg_id_invoice_lines_au on public.invoice_lines;
drop trigger if exists trg_id_invoice_lines_ad on public.invoice_lines;

-- Create triggers only if invoice_lines exists
do $$
begin
  if to_regclass('public.invoice_lines') is not null then

    create trigger trg_id_invoice_lines_ai
    after insert on public.invoice_lines
    referencing new table as new_rows
    for each statement
    execute function public.trg_id_invoice_lines_ai_stmt();

    create trigger trg_id_invoice_lines_au
    after update on public.invoice_lines
    referencing old table as old_rows new table as new_rows
    for each statement
    execute function public.trg_id_invoice_lines_au_stmt();

    create trigger trg_id_invoice_lines_ad
    after delete on public.invoice_lines
    referencing old table as old_rows
    for each statement
    execute function public.trg_id_invoice_lines_ad_stmt();

  end if;
end$$;

-- =========================================================
-- A2.2 invoices metadata sync trigger (AFTER INSERT/UPDATE)
-- Keeps invoice_no/status/type in ledger even when lines unchanged.
-- =========================================================

create or replace function public.trg_id_invoices_meta_aiu()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return new;
  end if;

  perform public.id_ledger_sync_invoice_metadata(new.id);
  return new;
end;
$$;

drop trigger if exists trg_id_invoices_meta_aiu on public.invoices;

do $$
begin
  if to_regclass('public.invoices') is not null then
    create trigger trg_id_invoices_meta_aiu
    after insert or update on public.invoices
    for each row
    execute function public.trg_id_invoices_meta_aiu();
  end if;
end$$;

-- =========================================================
-- A2.3 invoices delete semantics (AFTER DELETE)
-- Keep ledger row but set current_* = 0 (audit-safe).
-- =========================================================

create or replace function public.trg_id_invoices_after_delete()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return old;
  end if;

  -- Force ledger current_* to zero, keep snapshots from OLD row.
  perform public.id_ledger_upsert_from_invoice_row(
    old.id,
    true,
    old.invoice_no,
    old.status::text,
    old.type::text
  );

  return old;
end;
$$;

drop trigger if exists trg_id_invoices_ad on public.invoices;

do $$
begin
  if to_regclass('public.invoices') is not null then
    create trigger trg_id_invoices_ad
    after delete on public.invoices
    for each row
    execute function public.trg_id_invoices_after_delete();
  end if;
end$$;

-- Optional PostgREST schema reload (safe wrapper)
do $$
begin
  perform pg_notify('pgrst', 'reload schema');
exception when others then
  null;
end$$;

commit;



begin;

-- =========================================================
-- A3) Invoice Discounting RPCs
--  - id_consolidation_preview()
--  - id_consolidation_balance_now(p_actor_user_id uuid)
--  - id_consolidation_runs_list(p_limit int, p_offset int)
--  - id_consolidation_run_get(p_id_ref text)
--
-- Notes:
--  - All functions are CREATE OR REPLACE (safe to rerun).
--  - Balance-now locks the changed ledger rows FOR UPDATE to keep read/update consistent.
--  - Returns are JSONB so you get {total, lines[]} in a single RPC response.
-- =========================================================
CREATE OR REPLACE FUNCTION public.id_consolidation_runs_list(
  p_limit int DEFAULT 50,
  p_offset int DEFAULT 0
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,50), 500));
  v_offset int := greatest(coalesce(p_offset,0), 0);
  v_total_count int;
  v_runs jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_consolidation_runs') is null then
    raise exception 'ID_RUNS_TABLE_MISSING';
  end if;

  -- ✅ Only COMMITTED runs (exclude drafts)
  select count(*)::int into v_total_count
  from public.id_consolidation_runs r
  where r.bank_uploaded_at_utc is not null;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id_ref', r.id_ref,
          'state', case when r.bank_uploaded_at_utc is null then 'DRAFT' else 'COMMITTED' end,
          'created_at_utc', r.created_at_utc,
          'created_by_user_id', case when r.created_by_user_id is null then null else r.created_by_user_id::text end,
          'committed_by_user_id', case when r.committed_by_user_id is null then null else r.committed_by_user_id::text end,
          'total_delta_ex_vat', r.total_delta_ex_vat,
          'total_delta_vat', r.total_delta_vat,
          'total_delta_inc_vat', r.total_delta_inc_vat,
          'bank_upload_code', r.bank_upload_code,
          'bank_uploaded_at_utc', r.bank_uploaded_at_utc,
          'note', r.note
        )
        order by r.created_at_utc desc, r.id_ref desc
      ),
      '[]'::jsonb
    )
  into v_runs
  from (
    select r0.*
    from public.id_consolidation_runs r0
    where r0.bank_uploaded_at_utc is not null
    order by r0.created_at_utc desc, r0.id_ref desc
    limit v_limit offset v_offset
  ) r;

  return jsonb_build_object(
    'total_count', coalesce(v_total_count,0),
    'limit', v_limit,
    'offset', v_offset,
    'runs', v_runs
  );
end;
$$;


create or replace function public.id_consolidation_preview()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_lines jsonb := '[]'::jsonb;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_total_current_ex numeric(12,2) := 0;
  v_total_current_vat numeric(12,2) := 0;
  v_total_current_inc numeric(12,2) := 0;

  v_total_reportable_ex numeric(12,2) := 0;
  v_total_reportable_vat numeric(12,2) := 0;
  v_total_reportable_inc numeric(12,2) := 0;
begin
  -- Guard (gives an explicit error if migrations not applied)
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      coalesce(l.current_ex_vat,0)::numeric(12,2) as current_ex_vat,
      coalesce(l.current_vat,0)::numeric(12,2) as current_vat,
      coalesce(l.current_inc_vat,0)::numeric(12,2) as current_inc_vat,

      coalesce(l.last_reported_ex_vat,0)::numeric(12,2) as last_reported_ex_vat,
      coalesce(l.last_reported_vat,0)::numeric(12,2) as last_reported_vat,
      coalesce(l.last_reported_inc_vat,0)::numeric(12,2) as last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.invoice_no as inv_invoice_no,
      i.status::text as inv_status_text,
      i.type::text as inv_type_text,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat,0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat,0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat,0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  changed as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat,0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat,0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat,0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      c.reportable_current_ex_vat <> coalesce(c.last_reported_ex_vat,0)::numeric(12,2)
      or c.reportable_current_vat <> coalesce(c.last_reported_vat,0)::numeric(12,2)
      or c.reportable_current_inc_vat <> coalesce(c.last_reported_inc_vat,0)::numeric(12,2)
  )
  select
    coalesce(sum(ch.delta_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.delta_vat),0)::numeric(12,2),
    coalesce(sum(ch.delta_inc_vat),0)::numeric(12,2),

    coalesce(sum(ch.current_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.current_vat),0)::numeric(12,2),
    coalesce(sum(ch.current_inc_vat),0)::numeric(12,2),

    coalesce(sum(ch.reportable_current_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.reportable_current_vat),0)::numeric(12,2),
    coalesce(sum(ch.reportable_current_inc_vat),0)::numeric(12,2),

    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', ch.invoice_id::text,
          'invoice_number', ch.effective_invoice_number,
          'invoice_status', ch.effective_invoice_status,
          'invoice_type', ch.effective_invoice_type,

          'client_id', case when ch.client_id is null then null else ch.client_id::text end,
          'client_name', ch.client_name,

          'issued_at_utc', ch.issued_at_utc,
          'due_at_utc', ch.due_at_utc,
          'paid_at_utc', ch.paid_at_utc,
          'status_date_utc', ch.status_date_utc,
          'credit_note_created_at_utc', ch.credit_note_created_at_utc,

          'updated_at_utc', ch.updated_at_utc,

          'is_on_hold', ch.is_on_hold,
          'line_kind', ch.line_kind,
          'non_reportable_reason', ch.non_reportable_reason,

          'current_ex_vat', ch.current_ex_vat,
          'current_vat', ch.current_vat,
          'current_inc_vat', ch.current_inc_vat,

          'reportable_current_ex_vat', ch.reportable_current_ex_vat,
          'reportable_current_vat', ch.reportable_current_vat,
          'reportable_current_inc_vat', ch.reportable_current_inc_vat,

          'last_reported_ex_vat', ch.last_reported_ex_vat,
          'last_reported_vat', ch.last_reported_vat,
          'last_reported_inc_vat', ch.last_reported_inc_vat,

          'delta_ex_vat', ch.delta_ex_vat,
          'delta_vat', ch.delta_vat,
          'delta_inc_vat', ch.delta_inc_vat
        )
        order by
          ch.sort_ts desc nulls last,
          nullif(btrim(coalesce(ch.effective_invoice_number,'')),'') nulls last,
          ch.invoice_id
      ),
      '[]'::jsonb
    )
  into
    v_total_ex, v_total_vat, v_total_inc,
    v_total_current_ex, v_total_current_vat, v_total_current_inc,
    v_total_reportable_ex, v_total_reportable_vat, v_total_reportable_inc,
    v_lines
  from changed ch;

  return jsonb_build_object(
    'total_delta_ex_vat', v_total_ex,
    'total_delta_vat', v_total_vat,
    'total_delta_inc_vat', v_total_inc,

    'total_current_ex_vat', v_total_current_ex,
    'total_current_vat', v_total_current_vat,
    'total_current_inc_vat', v_total_current_inc,

    'total_reportable_current_ex_vat', v_total_reportable_ex,
    'total_reportable_current_vat', v_total_reportable_vat,
    'total_reportable_current_inc_vat', v_total_reportable_inc,

    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$$;

CREATE OR REPLACE FUNCTION public.id_consolidation_balance_now(
  p_actor_user_id uuid,
  p_bank_upload_code text,
  p_note text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
declare
  v_id_ref text;
  v_created_at timestamptz;
  v_bank_uploaded_at timestamptz;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_lines jsonb := '[]'::jsonb;

  v_bank_upload_code text;
  v_note text;

  v_draft jsonb;
  v_commit jsonb;

  v_is_on_hold boolean;
begin
  v_bank_upload_code := nullif(btrim(coalesce(p_bank_upload_code,'')), '');
  v_note := nullif(btrim(coalesce(p_note,'')), '');

  if v_bank_upload_code is null then
    raise exception 'BANK_UPLOAD_CODE_REQUIRED';
  end if;

  if to_regclass('public.id_ref_seq') is null then
    raise exception 'ID_REF_SEQ_MISSING';
  end if;
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  -- ✅ Two-phase wrapper:
  -- 1) Draft start (creates run header + snapshot lines, NO ledger updates)
  v_draft := public.id_consolidation_run_draft_start(p_actor_user_id, v_note);

  v_id_ref := nullif(btrim(coalesce(v_draft->>'id_ref','')), '');
  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'DRAFT_START_FAILED';
  end if;

  v_created_at := (v_draft->>'created_at_utc')::timestamptz;

  v_total_ex := coalesce((v_draft #>> '{totals,delta_ex_vat}')::numeric, 0)::numeric(12,2);
  v_total_vat := coalesce((v_draft #>> '{totals,delta_vat}')::numeric, 0)::numeric(12,2);
  v_total_inc := coalesce((v_draft #>> '{totals,delta_inc_vat}')::numeric, 0)::numeric(12,2);

  -- Rebuild legacy "lines" payload shape from snapshot run lines
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', rl.invoice_id::text,
          'invoice_number', rl.invoice_number,
          'invoice_status', rl.invoice_status,
          'invoice_type', rl.invoice_type,

          'is_on_hold', (upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD'),

          'delta_ex_vat', coalesce(rl.delta_ex_vat, 0)::numeric(12,2),
          'delta_vat', coalesce(rl.delta_vat, 0)::numeric(12,2),
          'delta_inc_vat', coalesce(rl.delta_inc_vat, 0)::numeric(12,2),

          'current_ex_vat', coalesce(rl.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(rl.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(rl.current_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_ex_vat, 0)::numeric(12,2)
            end),

          'reportable_current_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_vat, 0)::numeric(12,2)
            end),

          'reportable_current_inc_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_inc_vat, 0)::numeric(12,2)
            end),

          'last_reported_ex_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_ex_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_ex_vat, 0)::numeric(12,2)
            )::numeric(12,2),

          'last_reported_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_vat, 0)::numeric(12,2)
            )::numeric(12,2),

          'last_reported_inc_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_inc_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_inc_vat, 0)::numeric(12,2)
            )::numeric(12,2)
        )
        order by
          nullif(btrim(coalesce(rl.invoice_number,'')),'') nulls last,
          rl.invoice_id
      ),
      '[]'::jsonb
    )
  into v_lines
  from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref;

  -- 2) Commit (stores bank upload code + updates ledger baselines from snapshot)
  v_commit := public.id_consolidation_run_draft_commit(v_id_ref, v_bank_upload_code, p_actor_user_id);

  v_bank_uploaded_at := (v_commit->>'bank_uploaded_at_utc')::timestamptz;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'created_at_utc', v_created_at,
    'bank_upload_code', v_bank_upload_code,
    'bank_uploaded_at_utc', v_bank_uploaded_at,
    'note', v_note,
    'total_delta_ex_vat', v_total_ex,
    'total_delta_vat', v_total_vat,
    'total_delta_inc_vat', v_total_inc,
    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$$;


CREATE OR REPLACE FUNCTION public.id_consolidation_run_get(p_id_ref text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
declare
  v_run record;
  v_lines jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if p_id_ref is null or p_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  select
    r.id_ref,
    r.created_at_utc,
    r.created_by_user_id,
    r.committed_by_user_id,
    r.total_delta_ex_vat,
    r.total_delta_vat,
    r.total_delta_inc_vat,
    r.bank_upload_code,
    r.bank_uploaded_at_utc,
    r.note
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = p_id_ref
  limit 1;

  if not found then
    raise exception 'ID_RUN_NOT_FOUND';
  end if;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', rl.invoice_id::text,

          'invoice_number',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_number,'')),''),
              i.invoice_no
            ),

          'client_id', case when i.client_id is null then null else i.client_id::text end,
          'client_name', c.name,

          'invoice_status',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_status,'')),''),
              i.status::text
            ),

          'invoice_type',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_type,'')),''),
              i.type::text
            ),

          'delta_ex_vat', rl.delta_ex_vat,
          'delta_vat', rl.delta_vat,
          'delta_inc_vat', rl.delta_inc_vat,

          'current_ex_vat', rl.current_ex_vat,
          'current_vat', rl.current_vat,
          'current_inc_vat', rl.current_inc_vat
        )
        order by
          nullif(btrim(coalesce(rl.invoice_number,'')),'') nulls last,
          rl.invoice_id
      ),
      '[]'::jsonb
    )
  into v_lines
  from public.id_consolidation_run_lines rl
  left join public.invoices i
    on i.id = rl.invoice_id
  left join public.clients c
    on c.id = i.client_id
  where rl.id_ref = p_id_ref;

  return jsonb_build_object(
    'run', jsonb_build_object(
      'id_ref', v_run.id_ref,
      'state', case when v_run.bank_uploaded_at_utc is null then 'DRAFT' else 'COMMITTED' end,
      'created_at_utc', v_run.created_at_utc,
      'created_by_user_id', case when v_run.created_by_user_id is null then null else v_run.created_by_user_id::text end,
      'committed_by_user_id', case when v_run.committed_by_user_id is null then null else v_run.committed_by_user_id::text end,
      'bank_upload_code', v_run.bank_upload_code,
      'bank_uploaded_at_utc', v_run.bank_uploaded_at_utc,
      'note', v_run.note,
      'total_delta_ex_vat', v_run.total_delta_ex_vat,
      'total_delta_vat', v_run.total_delta_vat,
      'total_delta_inc_vat', v_run.total_delta_inc_vat
    ),
    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$$;

-- Optional PostgREST schema reload (safe wrapper)
do $$
begin
  perform pg_notify('pgrst', 'reload schema');
exception when others then
  null;
end$$;

commit;

CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_start(
  p_actor_user_id uuid,
  p_note text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_ref_num bigint;
  v_id_ref text;
  v_created_at timestamptz := now();

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;
  v_line_count int := 0;

  v_note text;
begin
  v_note := nullif(btrim(coalesce(p_note,'')), '');

  if to_regclass('public.id_ref_seq') is null then
    raise exception 'ID_REF_SEQ_MISSING';
  end if;
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  -- Allocate new sequential ref and format as 6 digits
  select nextval('public.id_ref_seq') into v_ref_num;
  v_id_ref := lpad(v_ref_num::text, 6, '0');

  -- Build snapshot of changed rows (same delta logic as id_consolidation_balance_now),
  -- and INSERT run header + run lines WITHOUT updating id_invoice_ledger baselines.
  with changed as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      (upper(coalesce(l.invoice_status,'')) = 'ON_HOLD') as is_on_hold,

      coalesce(l.current_ex_vat,0)::numeric(12,2) as current_ex_vat,
      coalesce(l.current_vat,0)::numeric(12,2) as current_vat,
      coalesce(l.current_inc_vat,0)::numeric(12,2) as current_inc_vat,

      coalesce(l.last_reported_ex_vat,0)::numeric(12,2) as last_reported_ex_vat,
      coalesce(l.last_reported_vat,0)::numeric(12,2) as last_reported_vat,
      coalesce(l.last_reported_inc_vat,0)::numeric(12,2) as last_reported_inc_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_ex_vat,0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_vat,0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_inc_vat,0)::numeric(12,2)
      end) as reportable_current_inc_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_ex_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_ex_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_ex_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_inc_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_inc_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_inc_vat
    from public.id_invoice_ledger l
    where
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_ex_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_ex_vat,0)::numeric(12,2)
      or
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_vat,0)::numeric(12,2)
      or
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_inc_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_inc_vat,0)::numeric(12,2)
    for update
  ),
  agg as (
    select
      coalesce(sum(c.delta_ex_vat),0)::numeric(12,2) as total_ex,
      coalesce(sum(c.delta_vat),0)::numeric(12,2) as total_vat,
      coalesce(sum(c.delta_inc_vat),0)::numeric(12,2) as total_inc,
      count(*)::int as line_count
    from changed c
  ),
  ins_run as (
    insert into public.id_consolidation_runs (
      id_ref,
      created_at_utc,
      created_by_user_id,
      total_delta_ex_vat,
      total_delta_vat,
      total_delta_inc_vat,
      bank_upload_code,
      bank_uploaded_at_utc,
      note
    )
    select
      v_id_ref,
      v_created_at,
      p_actor_user_id,
      a.total_ex,
      a.total_vat,
      a.total_inc,
      null::text,
      null::timestamptz,
      v_note
    from agg a
    returning 1
  ),
  ins_lines as (
    insert into public.id_consolidation_run_lines (
      id_ref,
      invoice_id,
      invoice_number,
      invoice_status,
      invoice_type,
      delta_ex_vat,
      delta_vat,
      delta_inc_vat,
      current_ex_vat,
      current_vat,
      current_inc_vat
    )
    select
      v_id_ref,
      c.invoice_id,
      c.invoice_number,
      c.invoice_status,
      c.invoice_type,
      c.delta_ex_vat,
      c.delta_vat,
      c.delta_inc_vat,
      c.current_ex_vat,
      c.current_vat,
      c.current_inc_vat
    from changed c
    returning 1
  )
  select
    a.total_ex,
    a.total_vat,
    a.total_inc,
    a.line_count
  into
    v_total_ex,
    v_total_vat,
    v_total_inc,
    v_line_count
  from agg a;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'created_at_utc', v_created_at,
    'line_count', coalesce(v_line_count, 0),
    'totals', jsonb_build_object(
      'delta_ex_vat', coalesce(v_total_ex, 0)::numeric(12,2),
      'delta_vat', coalesce(v_total_vat, 0)::numeric(12,2),
      'delta_inc_vat', coalesce(v_total_inc, 0)::numeric(12,2)
    )
  );
end;
$function$;
CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_commit(
  p_id_ref text,
  p_bank_upload_code text,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_id_ref text := nullif(btrim(coalesce(p_id_ref,'')), '');
  v_bank_upload_code text := nullif(btrim(coalesce(p_bank_upload_code,'')), '');
  v_now timestamptz := now();

  v_run record;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_line_count int := 0;
  v_ledger_rows_updated int := 0;
  v_header_updated int := 0;

  v_has_committed_by_col boolean := false;
  v_existing_code text;
begin
  if p_actor_user_id is null then
    raise exception 'ACTOR_USER_ID_REQUIRED';
  end if;

  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  if v_bank_upload_code is null then
    raise exception 'BANK_UPLOAD_CODE_REQUIRED';
  end if;

  -- Lock run row for idempotency + concurrency safety
  select
    r.id_ref,
    r.created_at_utc,
    r.created_by_user_id,
    r.total_delta_ex_vat,
    r.total_delta_vat,
    r.total_delta_inc_vat,
    r.bank_upload_code,
    r.bank_uploaded_at_utc,
    r.note
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = v_id_ref
  limit 1
  for update;

  if not found then
    raise exception 'ID_RUN_NOT_FOUND';
  end if;

  v_total_ex := coalesce(v_run.total_delta_ex_vat, 0)::numeric(12,2);
  v_total_vat := coalesce(v_run.total_delta_vat, 0)::numeric(12,2);
  v_total_inc := coalesce(v_run.total_delta_inc_vat, 0)::numeric(12,2);

  v_existing_code := nullif(btrim(coalesce(v_run.bank_upload_code,'')), '');

  -- Already committed (idempotent path)
  if v_run.bank_uploaded_at_utc is not null or v_existing_code is not null then
    if v_existing_code is not distinct from v_bank_upload_code then
      select count(*)::int
      into v_line_count
      from public.id_consolidation_run_lines rl
      where rl.id_ref = v_id_ref;

      return jsonb_build_object(
        'id_ref', v_id_ref,
        'state', 'COMMITTED',
        'bank_upload_code', v_existing_code,
        'bank_uploaded_at_utc', v_run.bank_uploaded_at_utc,
        'committed_by_user_id', p_actor_user_id::text,
        'line_count', coalesce(v_line_count, 0),
        'totals', jsonb_build_object(
          'delta_ex_vat', v_total_ex,
          'delta_vat', v_total_vat,
          'delta_inc_vat', v_total_inc
        ),
        'ledger_rows_updated', 0,
        'did_commit', false
      );
    else
      raise exception 'ID_RUN_ALREADY_COMMITTED_DIFFERENT_CODE';
    end if;
  end if;

  -- Detect optional column committed_by_user_id (NOT present in current schema dump)
  select exists (
    select 1
    from information_schema.columns c
    where c.table_schema = 'public'
      and c.table_name = 'id_consolidation_runs'
      and c.column_name = 'committed_by_user_id'
  )
  into v_has_committed_by_col;

  if v_has_committed_by_col then
    execute
      'update public.id_consolidation_runs
          set bank_upload_code = $1,
              bank_uploaded_at_utc = $2,
              committed_by_user_id = $3
        where id_ref = $4
          and bank_uploaded_at_utc is null'
    using v_bank_upload_code, v_now, p_actor_user_id, v_id_ref;

    get diagnostics v_header_updated = row_count;
  else
    update public.id_consolidation_runs r2
    set
      bank_upload_code = v_bank_upload_code,
      bank_uploaded_at_utc = v_now
    where r2.id_ref = v_id_ref
      and r2.bank_uploaded_at_utc is null;

    get diagnostics v_header_updated = row_count;
  end if;

  if v_header_updated = 0 then
    -- Another session committed concurrently; re-check for idempotency
    select
      r3.bank_upload_code,
      r3.bank_uploaded_at_utc
    into
      v_existing_code,
      v_now
    from public.id_consolidation_runs r3
    where r3.id_ref = v_id_ref
    limit 1;

    v_existing_code := nullif(btrim(coalesce(v_existing_code,'')), '');

    if v_now is not null and v_existing_code is not distinct from v_bank_upload_code then
      select count(*)::int
      into v_line_count
      from public.id_consolidation_run_lines rl
      where rl.id_ref = v_id_ref;

      return jsonb_build_object(
        'id_ref', v_id_ref,
        'state', 'COMMITTED',
        'bank_upload_code', v_existing_code,
        'bank_uploaded_at_utc', v_now,
        'committed_by_user_id', p_actor_user_id::text,
        'line_count', coalesce(v_line_count, 0),
        'totals', jsonb_build_object(
          'delta_ex_vat', v_total_ex,
          'delta_vat', v_total_vat,
          'delta_inc_vat', v_total_inc
        ),
        'ledger_rows_updated', 0,
        'did_commit', false
      );
    end if;

    raise exception 'ID_RUN_COMMIT_RACE';
  end if;

  -- Update ledger baselines from SNAPSHOT lines (not from current ledger state)
  update public.id_invoice_ledger l
  set
    last_reported_ex_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_ex_vat,0)::numeric(12,2)
      end
    ),
    last_reported_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_vat,0)::numeric(12,2)
      end
    ),
    last_reported_inc_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_inc_vat,0)::numeric(12,2)
      end
    ),
    updated_at_utc = v_now
  from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref
    and l.invoice_id = rl.invoice_id;

  get diagnostics v_ledger_rows_updated = row_count;

  select count(*)::int
  into v_line_count
  from public.id_consolidation_run_lines rl2
  where rl2.id_ref = v_id_ref;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'state', 'COMMITTED',
    'bank_upload_code', v_bank_upload_code,
    'bank_uploaded_at_utc', v_now,
    'committed_by_user_id', p_actor_user_id::text,
    'line_count', coalesce(v_line_count, 0),
    'totals', jsonb_build_object(
      'delta_ex_vat', v_total_ex,
      'delta_vat', v_total_vat,
      'delta_inc_vat', v_total_inc
    ),
    'ledger_rows_updated', coalesce(v_ledger_rows_updated, 0),
    'did_commit', true
  );
end;
$function$;


CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_cancel(
  p_id_ref text,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_id_ref text := nullif(btrim(coalesce(p_id_ref,'')), '');

  v_run record;

  v_deleted_lines int := 0;
  v_deleted_run int := 0;

  v_has_commit_marker boolean := false;
begin
  if p_actor_user_id is null then
    raise exception 'ACTOR_USER_ID_REQUIRED';
  end if;

  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  -- Lock run row (if present)
  select
    r.id_ref,
    r.bank_upload_code,
    r.bank_uploaded_at_utc
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = v_id_ref
  limit 1
  for update;

  if not found then
    return jsonb_build_object(
      'ok', true,
      'id_ref', v_id_ref,
      'cancelled', false,
      'already_missing', true
    );
  end if;

  v_has_commit_marker :=
    (v_run.bank_uploaded_at_utc is not null)
    or (nullif(btrim(coalesce(v_run.bank_upload_code,'')), '') is not null);

  if v_has_commit_marker then
    raise exception 'CANNOT_CANCEL_COMMITTED_RUN';
  end if;

  delete from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref;

  get diagnostics v_deleted_lines = row_count;

  delete from public.id_consolidation_runs r2
  where r2.id_ref = v_id_ref
    and r2.bank_uploaded_at_utc is null;

  get diagnostics v_deleted_run = row_count;

  if v_deleted_run = 0 then
    raise exception 'CANCEL_FAILED';
  end if;

  return jsonb_build_object(
    'ok', true,
    'id_ref', v_id_ref,
    'cancelled', true,
    'already_missing', false,
    'deleted_lines', coalesce(v_deleted_lines, 0)
  );
end;
$function$;




