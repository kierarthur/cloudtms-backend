\set ON_ERROR_STOP on

-- Disposable PostgreSQL fixture suite for
-- public._import_review_effective_invoice_balance_core_v1.
--
-- Load tests/import-authoritative-effective-balance-helper-runtime-schema.sql
-- into an empty disposable database first. Never run this fixture suite against
-- CloudTMS TEST or production: it intentionally truncates its disposable tables.

begin;
set search_path=public,extensions,pg_temp;

create or replace function public.fixture_uuid(p_value integer)
returns uuid
language sql
immutable
as $function$
  select ('00000000-0000-4000-8000-' || lpad(p_value::text,12,'0'))::uuid
$function$;

create or replace function public.fixture_reset()
returns void
language plpgsql
as $function$
begin
  truncate table
    public.audit_events,
    public.contract_weeks,
    public.import_review_action_outcomes,
    public.import_review_decisions,
    public.import_apply_operations,
    public.invoice_lines,
    public.invoices,
    public.timesheets_financials,
    public.timesheets,
    public.nhsp_shifts,
    public.hr_rows;
end
$function$;

create or replace function public.fixture_seed_source(
  p_source_system text,
  p_base integer
)
returns jsonb
language plpgsql
set search_path=public,extensions,pg_temp
as $function$
declare
  v_import uuid:=public.fixture_uuid(p_base);
  v_hr_row uuid:=public.fixture_uuid(p_base+1);
  v_shift uuid:=public.fixture_uuid(p_base+2);
  v_candidate uuid:=public.fixture_uuid(p_base+3);
  v_client uuid:=public.fixture_uuid(p_base+4);
  v_contract uuid:=public.fixture_uuid(p_base+5);
  v_timesheet uuid:=public.fixture_uuid(p_base+6);
  v_tsfin uuid:=public.fixture_uuid(p_base+7);
  v_key text:=case when p_source_system='NHSP' then 'NHSP-' else 'HR-' end||p_base::text;
  v_identity text:=p_source_system||'|'||v_key;
  v_stream text:='SELF_BILL';
  v_basis text:=case when p_source_system='NHSP' then 'NHSP' else 'HEALTHROSTER_SELF_BILL' end;
  v_schedule jsonb;
begin
  v_schedule:=jsonb_build_array(jsonb_build_object(
    'shift_id',v_shift::text,
    'external_row_key',v_key,
    'date','2026-07-27',
    'start','08:00',
    'end','20:00',
    'hours_day',12,
    'hours_night',0,
    'hours_sat',0,
    'hours_sun',0,
    'hours_bh',0
  ));

  insert into public.hr_rows(id,import_id,external_row_key)
  values(v_hr_row,v_import,v_key);

  insert into public.nhsp_shifts(
    id,external_row_key,source_system,candidate_id,client_id,contract_id,
    week_ending_date,timesheet_id,latest_import_id
  ) values(
    v_shift,v_key,p_source_system,v_candidate,v_client,v_contract,
    date '2026-08-02',v_timesheet,v_import
  );

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,actual_schedule_json,
    candidate_hint_text,correction_id,correction_kind,parent_timesheet_id,
    created_at,updated_at,is_current,archived_at_utc
  ) values(
    v_timesheet,v_contract,date '2026-08-02',v_schedule,
    '{}'::jsonb,null,null,null,now(),now(),true,null
  );

  insert into public.timesheets_financials(
    id,timesheet_id,is_current,computed_at_utc,invoice_breakdown_json,basis,
    policy_snapshot_json,candidate_id,locked_by_invoice_id,is_stale,
    has_rate_issue,has_pay_channel_issue,paid_at_utc,
    hours_day,hours_night,hours_sat,hours_sun,hours_bh,
    total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat
  ) values(
    v_tsfin,v_timesheet,true,now(),'{}'::jsonb,v_basis,
    '{}'::jsonb,v_candidate,null,false,false,false,null,
    12,0,0,0,0,120,144,24
  );

  insert into public.contract_weeks(timesheet_id,contract_id,week_ending_date,status)
  values(v_timesheet,v_contract,date '2026-08-02','OPEN');

  return jsonb_build_object(
    'source_identity',v_identity,
    'source_system',p_source_system,
    'external_row_key',v_key,
    'invoice_stream',v_stream,
    'source_shift_id',v_shift::text,
    'hr_row_id',v_hr_row::text,
    'source_timesheet_id',v_timesheet::text,
    'candidate_id',v_candidate::text,
    'client_id',v_client::text,
    'contract_id',v_contract::text,
    'authoritative_import_id',v_import::text,
    'week_ending_date','2026-08-02',
    'authoritative_schedule_json',v_schedule,
    'authoritative_hours',jsonb_build_object(
      'hours_day',12,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,'total_hours',12
    )
  );
end
$function$;

create or replace function public.fixture_assert(p_condition boolean,p_message text)
returns void
language plpgsql
as $function$
begin
  if not coalesce(p_condition,false) then
    raise exception 'FIXTURE_ASSERTION_FAILED: %',p_message;
  end if;
end
$function$;

-- Case group 1: a later import replaced latest_import_id, but an older
-- completed operation still proves the deleted invoiced members.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_identity text;
  v_correction text;
  v_reversal uuid;
  v_replacement uuid;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,1000),('HEALTHROSTER'::text,2000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_identity:=v_item->>'source_identity';
    v_correction:='CROSS-'||v_system;
    v_reversal:=public.fixture_uuid(v_base+30);
    v_replacement:=public.fixture_uuid(v_base+31);

    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+20),public.fixture_uuid(v_base+21),
      repeat(case when v_system='NHSP' then 'a' else 'b' end,64),
      v_correction,v_reversal,v_replacement,
      'CREATE_REVERSAL_REPLACEMENT','RETAIN_EXISTING_CORRECTION_ID',
      null,'[]'::jsonb,now()-interval '2 days'
    );

    insert into public.invoices(id,type,status,issued_at_utc,original_invoice_id,issue_state)
    values
      (public.fixture_uuid(v_base+40),'INVOICE','ISSUED',now()-interval '1 day',null,'ISSUED'),
      (public.fixture_uuid(v_base+41),'INVOICE','ISSUED',now()-interval '1 day',null,'ISSUED');

    insert into public.invoice_lines(
      id,invoice_id,timesheet_id,meta_json,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,created_at
    ) values
      (public.fixture_uuid(v_base+42),public.fixture_uuid(v_base+40),v_reversal,
       '{"line_type":"HOURS_WEEKLY"}'::jsonb,-10,0,0,0,0,-100,-120,-20,now()-interval '1 day'),
      (public.fixture_uuid(v_base+43),public.fixture_uuid(v_base+41),v_replacement,
       '{"line_type":"HOURS_WEEKLY"}'::jsonb,11,0,0,0,0,110,132,22,now()-interval '1 day');

    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      public.fixture_uuid(v_base),jsonb_build_array(v_item)
    ) h;

    perform public.fixture_assert(
      (v_balance->>'validated_completed_operation_evidence_count')::integer=1,
      v_system||' cross-import operation was not recovered'
    );
    perform public.fixture_assert(
      (v_balance#>>'{B_hours,total_hours}')::numeric=1,
      v_system||' deleted members did not contribute the expected signed B'
    );
    perform public.fixture_assert(
      v_balance->'fully_invoiced_generation_ids' @> jsonb_build_array(v_correction),
      v_system||' deleted pair was not classified as fully invoiced'
    );
    perform public.fixture_assert(
      jsonb_array_length(v_balance->'historical_missing_timesheet_ids')=2,
      v_system||' missing historical member IDs were not retained'
    );
    raise notice 'PASS cross-import deleted-member reconstruction: %',v_system;
  end loop;
end
$test$;

-- Case group 2: an archived-sibling repair legitimately re-keys the surviving
-- physical member. A later review must canonicalise the new assignment.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_identity text;
  v_old_correction text;
  v_new_correction text;
  v_old_reversal uuid;
  v_surviving_replacement uuid;
  v_new_reversal uuid;
  v_schedule jsonb;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,3000),('HEALTHROSTER'::text,4000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_identity:=v_item->>'source_identity';
    v_old_correction:='OLD-'||v_system;
    v_new_correction:='REPAIRED-'||v_system;
    v_old_reversal:=public.fixture_uuid(v_base+30);
    v_surviving_replacement:=public.fixture_uuid(v_base+31);
    v_new_reversal:=public.fixture_uuid(v_base+32);
    v_schedule:=v_item->'authoritative_schedule_json';

    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+20),public.fixture_uuid(v_base+21),
      repeat(case when v_system='NHSP' then 'c' else 'd' end,64),
      v_old_correction,v_old_reversal,v_surviving_replacement,
      'CREATE_REVERSAL_REPLACEMENT','RETAIN_EXISTING_CORRECTION_ID',
      null,'[]'::jsonb,now()-interval '3 days'
    );

    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+22),public.fixture_uuid(v_base+23),
      repeat(case when v_system='NHSP' then 'e' else 'f' end,64),
      v_new_correction,v_new_reversal,v_surviving_replacement,
      'AMEND_EXISTING_REPLACEMENT','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED',
      v_old_correction,jsonb_build_array(v_surviving_replacement::text),now()-interval '2 days'
    );

    insert into public.timesheets(
      timesheet_id,contract_id,week_ending_date,actual_schedule_json,candidate_hint_text,
      correction_id,correction_kind,parent_timesheet_id,created_at,updated_at,is_current,archived_at_utc
    ) values
      (v_old_reversal,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',
       v_old_correction,'CHANGED_HOURS_REVERSAL',public.fixture_uuid(v_base+6),
       now()-interval '4 days',now()-interval '2 days',true,now()-interval '2 days'),
      (v_surviving_replacement,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',
       v_new_correction,'CHANGED_HOURS_REPLACEMENT',public.fixture_uuid(v_base+6),
       now()-interval '4 days',now()-interval '2 days',true,null),
      (v_new_reversal,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',
       v_new_correction,'CHANGED_HOURS_REVERSAL',public.fixture_uuid(v_base+6),
       now()-interval '2 days',now()-interval '2 days',true,null);

    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      public.fixture_uuid(v_base),jsonb_build_array(v_item)
    ) h;

    perform public.fixture_assert(
      jsonb_array_length(v_balance->'correction_member_supersession_lineage')=1,
      v_system||' valid repair supersession edge was not retained'
    );
    perform public.fixture_assert(
      coalesce((v_balance->>'active_mutable_generation')::boolean,false),
      v_system||' repaired active pair was not selected as mutable'
    );
    perform public.fixture_assert(
      v_balance->>'reviewed_existing_correction_id'=v_new_correction,
      v_system||' repaired correction ID was not canonical'
    );
    perform public.fixture_assert(
      nullif(v_balance->>'blocking_code','') is null,
      v_system||' repaired lineage was incorrectly blocked'
    );
    raise notice 'PASS archived-member repair supersession: %',v_system;
  end loop;
end
$test$;

-- Case group 3: full credit of a two-source aggregate line. Each source must
-- receive only its exact frozen segment's hours and money.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_shift uuid;
  v_other_shift uuid;
  v_timesheet uuid;
  v_tsfin uuid;
  v_key text;
  v_other_key text;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,5000),('HEALTHROSTER'::text,6000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_shift:=public.fixture_uuid(v_base+2);
    v_other_shift:=public.fixture_uuid(v_base+99);
    v_timesheet:=public.fixture_uuid(v_base+6);
    v_tsfin:=public.fixture_uuid(v_base+7);
    v_key:=v_item->>'external_row_key';
    v_other_key:='OTHER-'||v_base::text;

    update public.timesheets
    set actual_schedule_json=jsonb_build_array(
      jsonb_build_object('shift_id',v_shift::text,'external_row_key',v_key),
      jsonb_build_object('shift_id',v_other_shift::text,'external_row_key',v_other_key)
    )
    where timesheet_id=v_timesheet;

    update public.timesheets_financials
    set invoice_breakdown_json=jsonb_build_object('segments',jsonb_build_array(
      jsonb_build_object(
        'shift_id',v_shift::text,'external_row_key',v_key,
        'hours_day',10,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
        'pay_amount',100,'charge_amount',120
      ),
      jsonb_build_object(
        'shift_id',v_other_shift::text,'external_row_key',v_other_key,
        'hours_day',5,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
        'pay_amount',50,'charge_amount',60
      )
    ))
    where id=v_tsfin;

    insert into public.invoices(id,type,status,issued_at_utc,original_invoice_id,issue_state)
    values
      (public.fixture_uuid(v_base+40),'INVOICE','ISSUED',now()-interval '2 days',null,'ISSUED'),
      (public.fixture_uuid(v_base+41),'CREDIT_NOTE','ISSUED',now()-interval '1 day',public.fixture_uuid(v_base+40),'ISSUED');

    insert into public.invoice_lines(
      id,invoice_id,timesheet_id,meta_json,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,created_at
    ) values
      (public.fixture_uuid(v_base+42),public.fixture_uuid(v_base+40),v_timesheet,
       jsonb_build_object('line_type','HOURS_WEEKLY','tsfin_id',v_tsfin::text),
       15,0,0,0,0,150,180,30,now()-interval '2 days'),
      (public.fixture_uuid(v_base+43),public.fixture_uuid(v_base+41),v_timesheet,
       jsonb_build_object('line_type','HOURS_WEEKLY','tsfin_id',v_tsfin::text,
         'original_invoice_line_id',public.fixture_uuid(v_base+42)::text),
       15,0,0,0,0,-150,-180,-30,now()-interval '1 day');

    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      public.fixture_uuid(v_base),jsonb_build_array(v_item)
    ) h;

    perform public.fixture_assert(
      (v_balance#>>'{B_hours,total_hours}')::numeric=0,
      v_system||' aggregate credit did not net exact source hours to zero'
    );
    perform public.fixture_assert(
      (v_balance#>>'{B_financials,pay_ex_vat}')::numeric=0
      and (v_balance#>>'{B_financials,charge_ex_vat}')::numeric=0
      and (v_balance#>>'{B_financials,margin_ex_vat}')::numeric=0,
      v_system||' aggregate credit allocated whole-line money to one source'
    );
    perform public.fixture_assert(
      coalesce((v_balance->>'effective_position_net_is_zero')::boolean,false),
      v_system||' exact source segment did not settle to zero'
    );
    raise notice 'PASS multi-source credit allocation: %',v_system;
  end loop;
end
$test$;

-- Case group 4: zero signed hours with a non-zero monetary residual must never
-- fall through to an ordinary amendment route.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_timesheet uuid;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,7000),('HEALTHROSTER'::text,8000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_timesheet:=public.fixture_uuid(v_base+6);

    insert into public.invoices(id,type,status,issued_at_utc,original_invoice_id,issue_state)
    values
      (public.fixture_uuid(v_base+40),'INVOICE','ISSUED',now()-interval '2 days',null,'ISSUED'),
      (public.fixture_uuid(v_base+41),'INVOICE','ISSUED',now()-interval '1 day',null,'ISSUED');

    insert into public.invoice_lines(
      id,invoice_id,timesheet_id,meta_json,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,created_at
    ) values
      (public.fixture_uuid(v_base+42),public.fixture_uuid(v_base+40),v_timesheet,
       '{"line_type":"HOURS_WEEKLY"}'::jsonb,10,0,0,0,0,100,120,20,now()-interval '2 days'),
      (public.fixture_uuid(v_base+43),public.fixture_uuid(v_base+41),v_timesheet,
       '{"line_type":"HOURS_WEEKLY"}'::jsonb,-10,0,0,0,0,-99,-120,-21,now()-interval '1 day');

    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      public.fixture_uuid(v_base),jsonb_build_array(v_item)
    ) h;

    perform public.fixture_assert(
      (v_balance#>>'{B_hours,total_hours}')::numeric=0,
      v_system||' residual fixture hours did not net to zero'
    );
    perform public.fixture_assert(
      (v_balance#>>'{B_financials,pay_ex_vat}')::numeric<>0,
      v_system||' residual fixture money was not retained'
    );
    perform public.fixture_assert(
      not coalesce((v_balance->>'B_standard_representable')::boolean,true),
      v_system||' zero-hour monetary residual was called representable'
    );
    perform public.fixture_assert(
      v_balance->>'blocking_code'='IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE',
      v_system||' zero-hour monetary residual did not fail closed'
    );
    raise notice 'PASS zero-hours non-zero-money blocker: %',v_system;
  end loop;
end
$test$;

rollback;