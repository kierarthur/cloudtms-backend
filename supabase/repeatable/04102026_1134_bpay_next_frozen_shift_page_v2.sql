-- Extended exact frozen shift detail. V1 remains unchanged and ungranted;
-- this new private signature avoids DROP/CASCADE and return-type replacement.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_simple_shift_page_v2(
  p_run_line_id uuid,p_after_detail_no integer,p_limit integer
) returns table (
  shift_detail_id uuid,detail_no integer,work_date date,
  shift_start_at timestamptz,shift_end_at timestamptz,
  shift_start_local text,shift_end_local text,shift_overnight boolean,
  submitted_minutes integer,approved_minutes integer,
  approved_hours numeric,detail_label text,
  segment_pay_ex_vat numeric,pay_excluded boolean,
  hours_day numeric,hours_night numeric,hours_sat numeric,
  hours_sun numeric,hours_bh numeric
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_line record;
begin
  if p_limit is null or p_limit not between 1 and 100
     or (p_after_detail_no is not null and p_after_detail_no<0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_SHIFT_PAGE_INPUT_INVALID';
  end if;
  select * into strict v_line
    from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id);
  return query
    select s.id,s.detail_no,s.work_date,s.shift_start_at,s.shift_end_at,
      s.shift_start_local,s.shift_end_local,s.shift_overnight,
      s.submitted_minutes,s.approved_minutes,s.approved_hours,s.detail_label,
      s.segment_pay_ex_vat,s.pay_excluded,
      s.hours_day,s.hours_night,s.hours_sat,s.hours_sun,s.hours_bh
    from private.bpay_next_shift_detail s
    where s.approved_line_id=v_line.approved_line_id
      and s.detail_no>coalesce(p_after_detail_no,0)
    order by s.detail_no,s.id
    limit p_limit;
end
$function$;

alter function private.bpay_next_simple_shift_page_v2(uuid,integer,integer)
  owner to postgres;
revoke all on function private.bpay_next_simple_shift_page_v2(uuid,integer,integer)
  from public,anon,authenticated,service_role;

commit;
