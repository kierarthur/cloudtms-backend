CREATE OR REPLACE FUNCTION public.candidate_picker_search(
  p_query text,
  p_limit integer DEFAULT 25,
  p_offset integer DEFAULT 0,
  p_include_inactive boolean DEFAULT false
)
RETURNS TABLE(
  id uuid,
  display_name text,
  first_name text,
  last_name text,
  email text,
  phone text,
  tms_ref text,
  roles jsonb,
  job_titles_display text,
  active boolean,
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
  v_q_digits text := regexp_replace(coalesce(p_query, ''), '[^0-9]+', '', 'g');
begin
  if v_query = '' or char_length(v_query) < 2 then
    return;
  end if;

  return query
  with base as (
    select
      cs.id,
      cs.display_name,
      cs.first_name,
      cs.last_name,
      cs.email,
      cs.phone,
      cs.tms_ref,
      cs.roles,
      cs.job_titles_display,
      cs.active,
      concat_ws(
        ' ',
        coalesce(cs.first_name, ''),
        coalesce(cs.last_name, '')
      ) as full_name
    from public.candidates_summary as cs
    where
      (p_include_inactive is true or cs.active is true)
      and (
        lower(coalesce(cs.tms_ref, '')) = v_query_lc
        or lower(coalesce(cs.email, '')) = v_query_lc
        or lower(coalesce(cs.display_name, '')) = v_query_lc
        or lower(
          concat_ws(
            ' ',
            coalesce(cs.first_name, ''),
            coalesce(cs.last_name, '')
          )
        ) = v_query_lc
        or lower(coalesce(cs.first_name, '')) = v_query_lc
        or lower(coalesce(cs.last_name, '')) = v_query_lc
        or lower(coalesce(cs.phone, '')) = v_query_lc
        or (
          v_q_digits <> ''
          and regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g') = v_q_digits
        )
        or lower(coalesce(cs.tms_ref, '')) like v_query_lc || '%'
        or lower(coalesce(cs.email, '')) like v_query_lc || '%'
        or lower(coalesce(cs.display_name, '')) like v_query_lc || '%'
        or lower(coalesce(cs.first_name, '')) like v_query_lc || '%'
        or lower(coalesce(cs.last_name, '')) like v_query_lc || '%'
        or lower(
          concat_ws(
            ' ',
            coalesce(cs.first_name, ''),
            coalesce(cs.last_name, '')
          )
        ) like v_query_lc || '%'
        or lower(coalesce(cs.phone, '')) like v_query_lc || '%'
        or (
          v_q_digits <> ''
          and regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g') like v_q_digits || '%'
        )
        or position(v_query_lc in lower(coalesce(cs.tms_ref, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.email, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.display_name, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.first_name, ''))) > 0
        or position(v_query_lc in lower(coalesce(cs.last_name, ''))) > 0
        or position(
          v_query_lc in lower(
            concat_ws(
              ' ',
              coalesce(cs.first_name, ''),
              coalesce(cs.last_name, '')
            )
          )
        ) > 0
        or position(v_query_lc in lower(coalesce(cs.job_titles_display, ''))) > 0
        or (
          cs.roles is not null
          and position(v_query_lc in lower(cs.roles::text)) > 0
        )
        or position(v_query_lc in lower(coalesce(cs.phone, ''))) > 0
        or (
          v_q_digits <> ''
          and position(v_q_digits in regexp_replace(coalesce(cs.phone, ''), '[^0-9]+', '', 'g')) > 0
        )
      )
  ),
  ranked as (
    select
      b.id,
      b.display_name,
      b.first_name,
      b.last_name,
      b.email,
      b.phone,
      b.tms_ref,
      b.roles,
      b.job_titles_display,
      b.active,
      case
        when lower(coalesce(b.tms_ref, '')) = v_query_lc then 10
        when lower(coalesce(b.email, '')) = v_query_lc then 20
        when lower(coalesce(b.display_name, '')) = v_query_lc then 30
        when lower(coalesce(b.full_name, '')) = v_query_lc then 40
        when lower(coalesce(b.first_name, '')) = v_query_lc then 50
        when lower(coalesce(b.last_name, '')) = v_query_lc then 60
        when lower(coalesce(b.tms_ref, '')) like v_query_lc || '%' then 70
        when lower(coalesce(b.email, '')) like v_query_lc || '%' then 80
        when lower(coalesce(b.display_name, '')) like v_query_lc || '%' then 90
        when lower(coalesce(b.full_name, '')) like v_query_lc || '%' then 100
        when lower(coalesce(b.first_name, '')) like v_query_lc || '%' then 110
        when lower(coalesce(b.last_name, '')) like v_query_lc || '%' then 120
        when position(v_query_lc in lower(coalesce(b.tms_ref, ''))) > 0 then 130
        when position(v_query_lc in lower(coalesce(b.email, ''))) > 0 then 140
        when position(v_query_lc in lower(coalesce(b.display_name, ''))) > 0 then 150
        when position(v_query_lc in lower(coalesce(b.full_name, ''))) > 0 then 160
        when position(v_query_lc in lower(coalesce(b.job_titles_display, ''))) > 0 then 170
        when b.roles is not null and position(v_query_lc in lower(b.roles::text)) > 0 then 180
        when v_q_digits <> '' and regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g') = v_q_digits then 190
        when v_q_digits <> '' and regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g') like v_q_digits || '%' then 200
        when v_q_digits <> '' and position(v_q_digits in regexp_replace(coalesce(b.phone, ''), '[^0-9]+', '', 'g')) > 0 then 210
        when position(v_query_lc in lower(coalesce(b.phone, ''))) > 0 then 220
        else 999
      end as match_rank
    from base as b
  )
  select
    r.id,
    r.display_name,
    r.first_name,
    r.last_name,
    r.email,
    r.phone,
    r.tms_ref,
    r.roles,
    r.job_titles_display,
    r.active,
    r.match_rank
  from ranked as r
  order by
    r.match_rank asc,
    lower(coalesce(r.display_name, '')) asc,
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



