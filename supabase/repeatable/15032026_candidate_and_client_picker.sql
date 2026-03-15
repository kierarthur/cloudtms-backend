CREATE OR REPLACE FUNCTION public.client_picker_search(
  p_query text,
  p_limit integer DEFAULT 25,
  p_offset integer DEFAULT 0,
  p_nhsp_only boolean DEFAULT false,
  p_hr_auto_only boolean DEFAULT false
)
RETURNS TABLE (
  id uuid,
  name text,
  cli_ref text,
  primary_invoice_email text,
  is_nhsp boolean,
  autoprocess_hr boolean,
  rev bigint,
  updated_at timestamptz,
  match_rank integer
)
LANGUAGE plpgsql
STABLE
AS $function$
declare
  v_query_raw text := coalesce(p_query, '');
  v_query text := btrim(v_query_raw);
  v_query_lc text := lower(btrim(v_query_raw));
  v_limit integer := greatest(1, least(coalesce(p_limit, 25), 100));
  v_offset integer := greatest(coalesce(p_offset, 0), 0);
begin
  if v_query = '' or char_length(v_query) < 2 then
    return;
  end if;

  return query
  with latest_settings as (
    select
      cs.client_id,
      cs.is_nhsp,
      cs.autoprocess_hr,
      row_number() over (
        partition by cs.client_id
        order by
          cs.effective_from desc nulls last,
          cs.updated_at desc nulls last,
          cs.created_at desc nulls last,
          cs.id desc
      ) as rn
    from public.client_settings as cs
  ),
  base as (
    select
      c.id,
      c.name,
      c.cli_ref,
      c.primary_invoice_email,
      coalesce(ls.is_nhsp, false) as is_nhsp,
      coalesce(ls.autoprocess_hr, false) as autoprocess_hr,
      c.rev,
      c.updated_at
    from public.clients as c
    left join latest_settings as ls
      on ls.client_id = c.id
     and ls.rn = 1
    where
      (p_nhsp_only is false or coalesce(ls.is_nhsp, false) is true)
      and (p_hr_auto_only is false or coalesce(ls.autoprocess_hr, false) is true)
      and (
        lower(coalesce(c.cli_ref, '')) = v_query_lc
        or lower(coalesce(c.primary_invoice_email, '')) = v_query_lc
        or lower(coalesce(c.name, '')) = v_query_lc
        or lower(coalesce(c.ap_phone, '')) = v_query_lc
        or lower(coalesce(c.contact_email, '')) = v_query_lc
        or lower(coalesce(c.cli_ref, '')) like v_query_lc || '%'
        or lower(coalesce(c.primary_invoice_email, '')) like v_query_lc || '%'
        or lower(coalesce(c.name, '')) like v_query_lc || '%'
        or lower(coalesce(c.ap_phone, '')) like v_query_lc || '%'
        or lower(coalesce(c.contact_email, '')) like v_query_lc || '%'
        or position(v_query_lc in lower(coalesce(c.cli_ref, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.primary_invoice_email, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.name, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.ap_phone, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.contact_email, ''))) > 0
      )
  ),
  ranked as (
    select
      b.id,
      b.name,
      b.cli_ref,
      b.primary_invoice_email,
      b.is_nhsp,
      b.autoprocess_hr,
      b.rev,
      b.updated_at,
      case
        when lower(coalesce(b.cli_ref, '')) = v_query_lc then 10
        when lower(coalesce(b.primary_invoice_email, '')) = v_query_lc then 20
        when lower(coalesce(b.name, '')) = v_query_lc then 30
        when lower(coalesce(b.cli_ref, '')) like v_query_lc || '%' then 40
        when lower(coalesce(b.primary_invoice_email, '')) like v_query_lc || '%' then 50
        when lower(coalesce(b.name, '')) like v_query_lc || '%' then 60
        when position(v_query_lc in lower(coalesce(b.cli_ref, ''))) > 0 then 70
        when position(v_query_lc in lower(coalesce(b.primary_invoice_email, ''))) > 0 then 80
        when position(v_query_lc in lower(coalesce(b.name, ''))) > 0 then 90
        else 999
      end as match_rank
    from base as b
  )
  select
    r.id,
    r.name,
    r.cli_ref,
    r.primary_invoice_email,
    r.is_nhsp,
    r.autoprocess_hr,
    r.rev,
    r.updated_at,
    r.match_rank
  from ranked as r
  order by
    r.match_rank asc,
    lower(coalesce(r.name, '')) asc,
    r.id asc
  offset v_offset
  limit v_limit;
end;
$function$;

CREATE OR REPLACE FUNCTION public.client_picker_search(
  p_query text,
  p_limit integer DEFAULT 25,
  p_offset integer DEFAULT 0,
  p_nhsp_only boolean DEFAULT false,
  p_hr_auto_only boolean DEFAULT false
)
RETURNS TABLE (
  id uuid,
  name text,
  cli_ref text,
  primary_invoice_email text,
  is_nhsp boolean,
  autoprocess_hr boolean,
  rev bigint,
  updated_at timestamptz,
  match_rank integer
)
LANGUAGE plpgsql
STABLE
AS $function$
declare
  v_query_raw text := coalesce(p_query, '');
  v_query text := btrim(v_query_raw);
  v_query_lc text := lower(btrim(v_query_raw));
  v_limit integer := greatest(1, least(coalesce(p_limit, 25), 100));
  v_offset integer := greatest(coalesce(p_offset, 0), 0);
begin
  if v_query = '' or char_length(v_query) < 2 then
    return;
  end if;

  return query
  with latest_settings as (
    select
      cs.client_id,
      cs.is_nhsp,
      cs.autoprocess_hr,
      row_number() over (
        partition by cs.client_id
        order by
          cs.effective_from desc nulls last,
          cs.updated_at desc nulls last,
          cs.created_at desc nulls last,
          cs.id desc
      ) as rn
    from public.client_settings as cs
  ),
  base as (
    select
      c.id,
      c.name,
      c.cli_ref,
      c.primary_invoice_email,
      coalesce(ls.is_nhsp, false) as is_nhsp,
      coalesce(ls.autoprocess_hr, false) as autoprocess_hr,
      c.rev,
      c.updated_at
    from public.clients as c
    left join latest_settings as ls
      on ls.client_id = c.id
     and ls.rn = 1
    where
      (p_nhsp_only is false or coalesce(ls.is_nhsp, false) is true)
      and (p_hr_auto_only is false or coalesce(ls.autoprocess_hr, false) is true)
      and (
        lower(coalesce(c.cli_ref, '')) = v_query_lc
        or lower(coalesce(c.primary_invoice_email, '')) = v_query_lc
        or lower(coalesce(c.name, '')) = v_query_lc
        or lower(coalesce(c.ap_phone, '')) = v_query_lc
        or lower(coalesce(c.contact_email, '')) = v_query_lc
        or lower(coalesce(c.cli_ref, '')) like v_query_lc || '%'
        or lower(coalesce(c.primary_invoice_email, '')) like v_query_lc || '%'
        or lower(coalesce(c.name, '')) like v_query_lc || '%'
        or lower(coalesce(c.ap_phone, '')) like v_query_lc || '%'
        or lower(coalesce(c.contact_email, '')) like v_query_lc || '%'
        or position(v_query_lc in lower(coalesce(c.cli_ref, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.primary_invoice_email, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.name, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.ap_phone, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.contact_email, ''))) > 0
      )
  ),
  ranked as (
    select
      b.id,
      b.name,
      b.cli_ref,
      b.primary_invoice_email,
      b.is_nhsp,
      b.autoprocess_hr,
      b.rev,
      b.updated_at,
      case
        when lower(coalesce(b.cli_ref, '')) = v_query_lc then 10
        when lower(coalesce(b.primary_invoice_email, '')) = v_query_lc then 20
        when lower(coalesce(b.name, '')) = v_query_lc then 30
        when lower(coalesce(b.cli_ref, '')) like v_query_lc || '%' then 40
        when lower(coalesce(b.primary_invoice_email, '')) like v_query_lc || '%' then 50
        when lower(coalesce(b.name, '')) like v_query_lc || '%' then 60
        when position(v_query_lc in lower(coalesce(b.cli_ref, ''))) > 0 then 70
        when position(v_query_lc in lower(coalesce(b.primary_invoice_email, ''))) > 0 then 80
        when position(v_query_lc in lower(coalesce(b.name, ''))) > 0 then 90
        else 999
      end as match_rank
    from base as b
  )
  select
    r.id,
    r.name,
    r.cli_ref,
    r.primary_invoice_email,
    r.is_nhsp,
    r.autoprocess_hr,
    r.rev,
    r.updated_at,
    r.match_rank
  from ranked as r
  order by
    r.match_rank asc,
    lower(coalesce(r.name, '')) asc,
    r.id asc
  offset v_offset
  limit v_limit;
end;
$function$;



