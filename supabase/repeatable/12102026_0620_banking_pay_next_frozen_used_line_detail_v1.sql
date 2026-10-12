-- DR17 / BD137: captured source type and actual approved line operands, read only.
-- No live Timesheet/Contract/Client/rate lookup and no new route or command.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_payment_client_origin_v1(p_revision_id uuid)
returns jsonb
language plpgsql stable security definer
set search_path=pg_catalog,private
as $function$
declare
  v_revision private.bpay_next_work_revision%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_daily boolean;
  v_evidence jsonb;
  v_settings jsonb;
  v_client_id uuid;
  v_classification text:='UNKNOWN';
begin
  if p_revision_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REVISION_REQUIRED';
  end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=p_revision_id;
  select w.* into strict v_work from private.bpay_next_work w where w.id=v_revision.work_id;
  if v_revision.approved_at_utc is null or v_revision.sealed_at_utc is null then
    raise exception using errcode='55000',message='BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE';
  end if;
  v_daily:=v_work.origin_kind='DAILY' or (
    v_revision.daily_approval_evidence_json->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    and v_revision.daily_approval_evidence_json->>'sheet_scope'='DAILY');
  v_evidence:=v_revision.daily_approval_evidence_json;
  v_settings:=v_evidence->'settings_authority_json';
  -- Only sealed native Daily evidence can classify its original Client here.
  -- Missing proof keeps the payment visible; contradictions never infer a Client.
  if v_daily and jsonb_typeof(v_evidence)='object'
     and v_evidence ?& array['contract_version','sheet_scope','timesheet_id','timesheet_version',
       'financial_id','candidate_id','booking_id','contract_id','client_id','work_date',
       'financial_authorised_at_utc','timesheet_authorised_at_utc']
     and v_evidence->>'client_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and v_evidence->>'financial_authorised_at_utc' is not null
     and v_evidence->>'timesheet_authorised_at_utc' is not null
     and jsonb_typeof(v_settings)='object'
     and v_settings ?& array['authority_version','workflow','client_id','contract_id','relevant_date','authority_fingerprint'] then
    v_classification:='UNCLASSIFIED_MISMATCH';
    if v_evidence->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
       and v_evidence->>'sheet_scope'='DAILY'
       and v_evidence->>'client_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       and v_evidence->>'timesheet_id'=v_revision.physical_timesheet_id::text
       and v_evidence->>'timesheet_version'=v_revision.physical_timesheet_version::text
       and v_evidence->>'financial_id'=v_revision.financial_snapshot_id::text
       and v_evidence->>'candidate_id'=v_work.candidate_id::text
       and v_evidence->>'booking_id'=v_work.booking_id
       and v_evidence->'contract_id'=coalesce(to_jsonb(v_work.contract_id),'null'::jsonb)
       and (v_work.origin_kind<>'DAILY' or v_evidence->>'work_date'=v_work.origin_work_date::text)
       and jsonb_typeof(v_settings)='object'
       and v_settings->>'authority_version'='CONTRACT_SETTINGS_AUTHORITY_V1'
       and v_settings->>'workflow'='DAILY'
       and v_settings->>'client_id'=v_evidence->>'client_id'
       and v_settings->'contract_id'=v_evidence->'contract_id'
       and v_settings->>'relevant_date'=v_evidence->>'work_date'
       and v_settings->>'authority_fingerprint' ~ '^[0-9a-f]{64}$'
       and v_evidence->>'financial_authorised_at_utc' is not null
       and v_evidence->>'timesheet_authorised_at_utc' is not null then
      v_client_id:=(v_evidence->>'client_id')::uuid;v_classification:='CLASSIFIED';
    end if;
  end if;
  return jsonb_build_object(
    'revision_id',v_revision.id,'origin_kind',v_work.origin_kind,
    'source_kind',v_revision.source_kind,
    'sheet_scope',case when v_daily then 'DAILY' end,'contract_id',v_work.contract_id,
    'client_id',v_client_id,'classification',v_classification,
    'work_date',case when v_work.origin_kind='DAILY' then v_work.origin_work_date::text
      when v_daily then v_revision.daily_approval_evidence_json->>'work_date' end,
    'physical_timesheet_id',v_revision.physical_timesheet_id,
    'physical_timesheet_version',v_revision.physical_timesheet_version,
    'candidate_display_name',v_revision.candidate_display_name,
    'candidate_reference',v_revision.candidate_reference,
    'client_display_name',v_revision.client_display_name,'job_title',v_revision.job_title,
    'band_label',v_revision.band_label,'timesheet_reference',v_revision.timesheet_reference,
    'pay_channel',v_revision.source_pay_channel,
    'authorised_at_utc',case when v_daily then v_revision.daily_approval_evidence_json->'financial_authorised_at_utc'
      else to_jsonb(v_revision.approved_at_utc) end);
end
$function$;

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
  v_origin jsonb;
  v_revision_id uuid;
  v_used_line jsonb;
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
  -- One captured revision subject, never a current Client/Contract lookup.
  if p_kind='RATE_SCHEDULE' then
    select rw.captured_revision_id into strict v_revision_id
      from private.bpay_next_run_work rw where rw.id=p_run_work_id;
  else
    select g.captured_revision_id into strict v_revision_id
      from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id) g;
  end if;
  v_origin:=private.bpay_next_payment_client_origin_v1(v_revision_id);
  if p_kind='RATE_DETAILS' then
    -- The guarded immutable line/revision pair owns actual used quantities/rates.
    -- No live rate, amount division or capture/publisher change is permitted.
    select jsonb_build_object('component_kind',l.component_kind,
      'component_key',l.component_key,'unit_label',l.unit_label,
      'approved_quantity',l.approved_quantity::text,
      'approved_unit_rate',l.approved_unit_rate::text)
      into strict v_used_line
      from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id) g
      join private.bpay_next_approved_line l on l.id=g.approved_line_id
        and l.revision_id=g.captured_revision_id;
  end if;
  v_bytes:=v_bytes+octet_length(v_origin::text)+coalesce(octet_length(v_used_line::text),4);
  if v_bytes>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_FROZEN_DETAIL_ROW_TOO_LARGE';
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
  return jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit),'origin',v_origin,'used_line',v_used_line);
end
$function$;
alter function private.bpay_next_payment_client_origin_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_payment_client_origin_v1(uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) owner to postgres;
revoke all on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) from public,anon,authenticated;
grant execute on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) to service_role;
notify pgrst,'reload schema';
commit;
