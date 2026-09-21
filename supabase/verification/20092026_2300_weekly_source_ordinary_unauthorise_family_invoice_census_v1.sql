-- PHD-022 / PROT-UNAUTH-001 / PROT-ROTATION-001.
-- PostgreSQL 17.11 rollback-contained proof that both ordinary unauthorise
-- owners refuse before mutation when any physical member of a rotated
-- Timesheet family carries an invoice line, a TSFIN invoice id, or a segment
-- invoice lock. No Banking Pay or Workbench owner is defined or changed here.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create temporary table phd022_results(
  ordinal serial primary key,
  proof_id text not null,
  result text not null,
  detail text not null
) on commit drop;

create function pg_temp.record_result(p_proof_id text,p_ok boolean,p_detail text)
returns void language sql as $function$
  insert into phd022_results(proof_id,result,detail)
  values(p_proof_id,case when p_ok then 'PASS' else 'FAIL' end,
         pg_catalog.left(pg_catalog.replace(p_detail,'|','/'),500));
$function$;

create function pg_temp.seed_family(p_case integer,p_lock_kind text)
returns uuid language plpgsql as $function$
declare
  v_actor uuid:=md5('phd022-actor')::uuid;
  v_client uuid:=md5('phd022-client')::uuid;
  v_candidate uuid:=md5('phd022-candidate-'||p_case)::uuid;
  v_contract uuid:=md5('phd022-contract-'||p_case)::uuid;
  v_old uuid:=('70000000-0000-4000-8000-'||pg_catalog.lpad((p_case*2-1)::text,12,'0'))::uuid;
  v_current uuid:=('70000000-0000-4000-8000-'||pg_catalog.lpad((p_case*2)::text,12,'0'))::uuid;
  v_week uuid:=md5('phd022-week-'||p_case)::uuid;
  v_old_tf uuid:=md5('phd022-old-tf-'||p_case)::uuid;
  v_current_tf uuid:=md5('phd022-current-tf-'||p_case)::uuid;
  v_invoice uuid:=md5('phd022-invoice-'||p_case)::uuid;
  v_line uuid:=md5('phd022-line-'||p_case)::uuid;
  v_booking text:='PHD022-BK-'||pg_catalog.lpad(p_case::text,2,'0');
begin
  insert into public.tms_users(id,email,role,is_active,password_hash)
  values(v_actor,'phd022-office@example.test','admin',true,'not-a-login')
  on conflict(id) do nothing;
  insert into public.clients(id,name)
  values(v_client,'PHD-022 ordinary Client')
  on conflict(id) do nothing;
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  )
  select md5('phd022-client-settings')::uuid,v_client,'2026-01-01','MANUAL',0
  where not exists(
    select 1 from public.client_settings cs where cs.client_id=v_client
  );
  insert into public.candidates(id,display_name)
  values(v_candidate,'PHD-022 Candidate '||p_case);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
  ) values (
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31','PAYE','{}'::jsonb,
    'HEALTHROSTER',false,false,false,false
  );

  insert into public.timesheets(
    timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
    line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
    qr_payload_json,is_adjustment,authorised_at_server,created_at,updated_at
  ) values
  (v_old,v_booking,1,false,'RECEIVED'::public.timesheet_status_enum,
   'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
   'HOURS'::public.timesheet_line_type_enum,'phd022-'||p_case,'phd022-hospital',
   'phd022-ward','phd022-role','weekly-0','2026-09-13',v_contract,'[]'::jsonb,
   '{}'::jsonb,false,null,pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()),
  (v_current,v_booking,2,true,'RECEIVED'::public.timesheet_status_enum,
   'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
   'HOURS'::public.timesheet_line_type_enum,'phd022-'||p_case,'phd022-hospital',
   'phd022-ward','phd022-role','weekly-0','2026-09-13',v_contract,'[]'::jsonb,
   '{}'::jsonb,false,pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp());

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    timesheet_id,is_adjustment
  ) values (
    v_week,v_contract,'2026-09-13',0,'AUTHORISED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,v_current,false
  );

  insert into public.timesheets_financials(
    id,timesheet_id,timesheet_version,is_current,candidate_id,client_id,
    processing_status,total_hours,total_pay_ex_vat,total_charge_ex_vat,authorised_at_utc
  ) values
  (v_old_tf,v_old,1,false,v_candidate,v_client,
   'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,8,100,200,
   pg_catalog.statement_timestamp()),
  (v_current_tf,v_current,2,true,v_candidate,v_client,
   'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,8,100,200,
   pg_catalog.statement_timestamp());

  insert into public.invoices(id,client_id,status,invoice_no)
  values(v_invoice,v_client,'DRAFT'::public.invoice_status_enum,'PHD022-'||p_case);

  if p_lock_kind='LINE' then
    insert into public.invoice_lines(id,invoice_id,timesheet_id,booking_id,description)
    values(v_line,v_invoice,v_old,v_booking,'PHD-022 historical family line');
  elsif p_lock_kind='TSFIN' then
    update public.timesheets_financials
       set locked_by_invoice_id=v_invoice,
           locked_at_utc=pg_catalog.statement_timestamp()
     where id=v_old_tf;
  elsif p_lock_kind='SEGMENT' then
    update public.timesheets_financials
       set invoice_breakdown_json=pg_catalog.jsonb_build_array(
         pg_catalog.jsonb_build_object('invoice_locked_invoice_id',v_invoice::text))
     where id=v_old_tf;
  else
    raise exception 'PHD022_UNKNOWN_LOCK_KIND';
  end if;
  return v_current;
end
$function$;

create function pg_temp.prove_single(p_case integer,p_lock_kind text)
returns void language plpgsql as $function$
declare
  v_current uuid:=pg_temp.seed_family(p_case,p_lock_kind);
  v_actor uuid:=md5('phd022-actor')::uuid;
  v_refused boolean:=false;
  v_authorised timestamptz;
  v_financial_authorised timestamptz;
  v_week_status text;
begin
  begin
    perform public.timesheet_unauthorise_atomic(v_current,v_current,v_actor);
  exception when others then
    v_refused:=sqlerrm='TIMESHEET_LOCKED_BY_INVOICE';
  end;
  select ts.authorised_at_server,tf.authorised_at_utc,cw.status::text
    into v_authorised,v_financial_authorised,v_week_status
  from public.timesheets ts
  join public.timesheets_financials tf
    on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
  where ts.timesheet_id=v_current;
  perform pg_temp.record_result(
    'PHD-022-SINGLE-'||p_lock_kind,
    v_refused and v_authorised is not null and v_financial_authorised is not null
      and v_week_status='AUTHORISED',
    'single owner refused historical '||p_lock_kind||' evidence before changing the current Timesheet, TSFIN or contract week'
  );
end
$function$;

create function pg_temp.prove_bulk(p_case integer,p_lock_kind text)
returns void language plpgsql as $function$
declare
  v_current uuid:=pg_temp.seed_family(p_case,p_lock_kind);
  v_actor uuid:=md5('phd022-actor')::uuid;
  v_result jsonb;
  v_authorised timestamptz;
  v_financial_authorised timestamptz;
  v_week_status text;
  v_ok boolean;
begin
  v_result:=public.timesheet_unauthorise_bulk_atomic(
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'timesheet_id',v_current,
      'expected_timesheet_id',v_current
    )),
    v_actor
  );
  select ts.authorised_at_server,tf.authorised_at_utc,cw.status::text
    into v_authorised,v_financial_authorised,v_week_status
  from public.timesheets ts
  join public.timesheets_financials tf
    on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
  where ts.timesheet_id=v_current;
  v_ok:=coalesce(v_result#>>'{results,0,error_code}','')='TIMESHEET_LOCKED_BY_INVOICE'
      and coalesce((v_result->>'success_count')::integer,-1)=0
      and coalesce((v_result->>'failure_count')::integer,-1)=1
      and v_authorised is not null and v_financial_authorised is not null
      and v_week_status='AUTHORISED';
  perform pg_temp.record_result(
    'PHD-022-BULK-'||p_lock_kind,
    v_ok,
    'bulk owner refused historical '||p_lock_kind||' evidence before changing the current Timesheet, TSFIN or contract week'
      ||case when v_ok then '' else '; result='||v_result::text end
  );
end
$function$;

select pg_temp.prove_single(1,'LINE');
select pg_temp.prove_single(2,'TSFIN');
select pg_temp.prove_single(3,'SEGMENT');
select pg_temp.prove_bulk(4,'LINE');
select pg_temp.prove_bulk(5,'TSFIN');
select pg_temp.prove_bulk(6,'SEGMENT');

do $assertions$
declare
  v_failures text;
begin
  select pg_catalog.string_agg(proof_id||':'||detail,'; ' order by ordinal)
    into v_failures
  from phd022_results
  where result<>'PASS';
  if v_failures is not null then
    raise exception 'PHD022_ORDINARY_UNAUTHORISE_FAMILY_CENSUS_FAILED: %',v_failures using errcode='55000';
  end if;
  if (select count(*) from phd022_results)<>6 then
    raise exception 'PHD022_ORDINARY_UNAUTHORISE_FAMILY_CENSUS_INCOMPLETE' using errcode='55000';
  end if;
end
$assertions$;

select 'PHD022_PROOF|'||proof_id||'|'||result||'|'||detail as line
from phd022_results
order by ordinal;

rollback;
