-- Repeatable CloudTMS function/view authority: weekly_source_candidate_approved_clock_row_v2
-- Display validation only: retain the owner's approved bucket hours. Never
-- recalculate money/rates or replace approval with elapsed wall-clock hours.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_candidate_approved_clock_row_v2(
  p_detail jsonb,p_buckets jsonb,p_row_key text
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_date date;
  v_start_text text;
  v_end_text text;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_start_utc timestamp with time zone;
  v_end_utc timestamp with time zone;
  v_has_utc boolean;
  v_overnight boolean;
  v_break integer;
  v_elapsed numeric;
  v_bucket numeric;
  v_bucket_minutes integer;
  v_paid_minutes integer:=0;
  v_hours numeric:=0;
  v_key text;
begin
  if jsonb_typeof(p_detail) is distinct from 'object'
    or jsonb_typeof(p_buckets) is distinct from 'object'
    or nullif(btrim(p_row_key),'') is null then return null; end if;
  v_date:=coalesce(nullif(p_detail->>'work_date',''),nullif(p_detail->>'date',''))::date;
  if v_date is null
    or (p_detail ? 'work_date' and p_detail ? 'date'
        and (p_detail->>'work_date')::date is distinct from (p_detail->>'date')::date)
    or (nullif(p_buckets->>'work_date','') is not null
        and (p_buckets->>'work_date')::date is distinct from v_date) then return null; end if;
  v_has_utc:=nullif(p_detail->>'start_utc','') is not null
    or nullif(p_detail->>'end_utc','') is not null;
  if v_has_utc then
    if coalesce(p_detail->>'start_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$'
      or coalesce(p_detail->>'end_utc','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$' then return null; end if;
    v_start_utc:=(p_detail->>'start_utc')::timestamptz;
    v_end_utc:=(p_detail->>'end_utc')::timestamptz;
    v_start:=v_start_utc at time zone 'Europe/London';
    v_end:=v_end_utc at time zone 'Europe/London';
    if date_trunc('minute',v_start) is distinct from v_start
      or date_trunc('minute',v_end) is distinct from v_end
      or v_start::date is distinct from v_date
      or v_end::date not between v_date and v_date+1 then return null; end if;
    v_start_text:=to_char(v_start,'HH24:MI');
    v_end_text:=to_char(v_end,'HH24:MI');
    v_overnight:=v_end::date>v_date;
    if (nullif(p_detail->>'start','') is not null and p_detail->>'start'<>v_start_text)
      or (nullif(p_detail->>'end','') is not null and p_detail->>'end'<>v_end_text)
      or (p_detail ? 'overnight' and p_detail->'overnight'<>'null'::jsonb
          and (jsonb_typeof(p_detail->'overnight') is distinct from 'boolean'
            or (p_detail->>'overnight')::boolean is distinct from v_overnight)) then return null; end if;
  else
    v_start_text:=p_detail->>'start'; v_end_text:=p_detail->>'end';
    if coalesce(v_start_text,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
      or coalesce(v_end_text,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
      or jsonb_typeof(p_detail->'overnight') is distinct from 'boolean' then return null; end if;
    v_overnight:=(p_detail->>'overnight')::boolean;
    v_start:=v_date+v_start_text::time;
    v_end:=v_date+v_end_text::time+case when v_overnight then interval '1 day' else interval '0 day' end;
    v_start_utc:=v_start at time zone 'Europe/London';
    v_end_utc:=v_end at time zone 'Europe/London';
    -- A nonexistent or repeated local endpoint without retained UTC is not
    -- reliable evidence. Do not guess an offset from the approved amount.
    if (v_start_utc at time zone 'Europe/London') is distinct from v_start
      or (v_end_utc at time zone 'Europe/London') is distinct from v_end
      or ((v_start_utc+interval '1 hour') at time zone 'Europe/London')=v_start
      or ((v_start_utc-interval '1 hour') at time zone 'Europe/London')=v_start
      or ((v_end_utc+interval '1 hour') at time zone 'Europe/London')=v_end
      or ((v_end_utc-interval '1 hour') at time zone 'Europe/London')=v_end then return null; end if;
  end if;
  if v_end<=v_start or v_end-v_start>interval '1 day' or v_end_utc<=v_start_utc then return null; end if;
  if coalesce(p_detail->>'break_minutes',p_detail->>'break_mins','') !~ '^[0-9]+$' then return null; end if;
  v_break:=coalesce(p_detail->>'break_minutes',p_detail->>'break_mins')::integer;
  if p_detail ? 'break_minutes' and p_detail ? 'break_mins'
    and (p_detail->>'break_minutes')::integer is distinct from (p_detail->>'break_mins')::integer then return null; end if;
  v_elapsed:=extract(epoch from (v_end_utc-v_start_utc))/60;
  if v_elapsed<>trunc(v_elapsed) or v_elapsed>1500
    or v_break>720 or v_break>v_elapsed then return null; end if;
  foreach v_key in array array['hours_day','hours_night','hours_sat','hours_sun','hours_bh'] loop
    v_bucket:=coalesce((p_buckets->>v_key)::numeric,0);
    if v_bucket<0 or v_bucket>25 then return null; end if;
    v_bucket_minutes:=round(v_bucket*60)::integer;
    -- Each genuine bucket represents whole minutes. The existing JS writer
    -- seals 2dp; Source SQL can seal 6dp. Recover minutes, not new paid hours.
    if v_bucket is distinct from round(v_bucket_minutes::numeric/60,2)
      and v_bucket is distinct from round(v_bucket_minutes::numeric/60,6) then return null; end if;
    v_paid_minutes:=v_paid_minutes+v_bucket_minutes;
    v_hours:=v_hours+v_bucket;
  end loop;
  if v_paid_minutes is distinct from (v_elapsed-v_break)::integer then return null; end if;
  return jsonb_build_object('hours',v_hours,'row',
    private.weekly_source_candidate_app_issue_hours_v1(v_date,v_start,v_end,v_break,p_row_key,'[]'::jsonb));
exception when invalid_text_representation or datetime_field_overflow
  or invalid_datetime_format or numeric_value_out_of_range then
  return null;
end;
$function$;
alter function private.weekly_source_candidate_approved_clock_row_v2(jsonb,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_candidate_approved_clock_row_v2(jsonb,jsonb,text)
  from public,anon,authenticated,service_role;
commit;
