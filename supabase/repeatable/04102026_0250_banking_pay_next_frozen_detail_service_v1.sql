-- Service-only immutable detail transport. Broker supplies its validated
-- Office actor; browser roles have neither wrapper nor private-page grants.
-- Only one <=100 row page is serialised, with exact decimals as text.
\set ON_ERROR_STOP on
begin;
create or replace function public.bpay_next_frozen_detail_page_v1(
  p_actor_user_id uuid,p_kind text,p_run_line_id uuid default null,
  p_run_work_id uuid default null,p_shift_detail_id uuid default null,
  p_after_detail_no integer default null,p_after_break_no integer default null,
  p_after_bucket text default null,p_after_rate_family text default null,
  p_after_rate_code text default null,p_limit integer default 50
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_rows jsonb:='[]'::jsonb;
  v_sql text;
  v_page record;
  v_bytes bigint:=512;
  v_count integer:=0;
  v_more boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
                   and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_FROZEN_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_kind is null or p_kind not in ('SHIFTS','BREAKS','RATE_DETAILS','RATE_SCHEDULE')
     or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
  end if;
  if p_kind='RATE_SCHEDULE' then
    if p_run_work_id is null or p_run_line_id is not null or p_shift_detail_id is not null
       or p_after_detail_no is not null or p_after_break_no is not null or p_after_bucket is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
    end if;
    v_sql:=$query$select to_jsonb(page) row_value
    from (select rate_schedule_id,rate_family,rate_code,unit_label,
          paye_rate::text,umbrella_rate::text,charge_rate::text
          from private.bpay_next_simple_rate_schedule_page_v1($1,$5,$6,$7)) page$query$;
  else
    if p_run_line_id is null or p_run_work_id is not null
       or p_after_rate_family is not null or p_after_rate_code is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
    end if;
    if p_kind='SHIFTS' then
      if p_shift_detail_id is not null or p_after_break_no is not null or p_after_bucket is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from (select shift_detail_id,detail_no,work_date,shift_start_at,shift_end_at,
            shift_start_local,shift_end_local,shift_overnight,submitted_minutes,
            approved_minutes,approved_hours::text,detail_label,
            segment_pay_ex_vat::text,pay_excluded,
            hours_day::text,hours_night::text,hours_sat::text,hours_sun::text,hours_bh::text
            from private.bpay_next_simple_shift_page_v2($1,$3,$7)) page$query$;
    elsif p_kind='BREAKS' then
      if p_after_detail_no is not null or p_after_bucket is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from private.bpay_next_simple_break_page_v1($1,$2,$3,$7) page$query$;
    else
      if p_shift_detail_id is not null or p_after_detail_no is not null or p_after_break_no is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from (select rate_detail_id,bucket,approved_hours::text,source_pay_rate::text
            from private.bpay_next_simple_rate_detail_page_v1($1,$4,$7)) page$query$;
    end if;
  end if;
  -- Fixed SQL choices only: no caller-supplied identifier/query. Each source
  -- returns <=100 rows. Charge encoded bytes BEFORE adding each row to the
  -- response; a byte-short page is explicitly incomplete, not end-of-data.
  for v_page in execute v_sql using coalesce(p_run_line_id,p_run_work_id),p_shift_detail_id,
    coalesce(p_after_detail_no,p_after_break_no),p_after_bucket,p_after_rate_family,p_after_rate_code,p_limit
  loop
    if v_bytes+octet_length(v_page.row_value::text)+1>120000 then
      if v_count=0 then
        raise exception using errcode='54000',message='BPAY_NEXT_FROZEN_DETAIL_ROW_TOO_LARGE';
      end if;
      v_more:=true;
      exit;
    end if;
    v_bytes:=v_bytes+octet_length(v_page.row_value::text)+1;
    v_rows:=v_rows||jsonb_build_array(v_page.row_value);
    v_count:=v_count+1;
  end loop;
  -- Exact-full row pages may need one final empty request; no count scan.
  return jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;
alter function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) owner to postgres;
revoke all on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)
from public,anon,authenticated;
grant execute on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) to service_role;
notify pgrst,'reload schema';
commit;
