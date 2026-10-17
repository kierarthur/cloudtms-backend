create or replace function public.timesheet_pdf_reference_sig(
  p_timesheet_id uuid
)
returns text
language plpgsql
security definer
set search_path = public
as $$
declare
  v_raw_sig text;
  v_sig text;
begin
  if p_timesheet_id is null then
    return null;
  end if;

  select
    coalesce(
      string_agg(
        (r.row_key || '=' || coalesce(r.current_reference, '')),
        '||'
        order by r.row_key
      ),
      ''
    )
  into v_raw_sig
  from public.timesheet_pdf_reference_rows(p_timesheet_id) r;

  select
    encode(
      extensions.digest(coalesce(v_raw_sig, ''), 'sha256'),
      'hex'
    )
  into v_sig;

  return v_sig;
end;
$$;