\set ON_ERROR_STOP on

-- Disposable PostgreSQL fixture suite for
-- public._import_review_effective_invoice_balance_core_v1.
--
-- Load tests/01082026_0150_import_authoritative_effective_balance_runtime_schema.sql
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
    public.import_review_states,
    public.import_apply_operations,
    public.hr_imports,
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
      'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',
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
      'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',
      null,'[]'::jsonb,now()-interval '3 days'
    );

    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+22),public.fixture_uuid(v_base+23),
      repeat(case when v_system='NHSP' then 'e' else 'f' end,64),
      v_new_correction,v_new_reversal,v_surviving_replacement,
      'AMEND_EXISTING_REPLACEMENT','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED',
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

-- Case group 5: successive archived-sibling repairs must canonicalise the
-- complete C1 -> C2 -> C3 chain for the surviving physical member.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_identity text;
  v_c1 text;
  v_c2 text;
  v_c3 text;
  v_r1 uuid;
  v_r2 uuid;
  v_r3 uuid;
  v_positive uuid;
  v_schedule jsonb;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,9000),('HEALTHROSTER'::text,10000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_identity:=v_item->>'source_identity';
    v_c1:='CHAIN-C1-'||v_system;
    v_c2:='CHAIN-C2-'||v_system;
    v_c3:='CHAIN-C3-'||v_system;
    v_r1:=public.fixture_uuid(v_base+30);
    v_positive:=public.fixture_uuid(v_base+31);
    v_r2:=public.fixture_uuid(v_base+32);
    v_r3:=public.fixture_uuid(v_base+33);
    v_schedule:=v_item->'authoritative_schedule_json';

    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+20),public.fixture_uuid(v_base+21),
      encode(digest(convert_to(v_system||' chain create','UTF8'),'sha256'),'hex'),
      v_c1,v_r1,v_positive,'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',
      null,'[]'::jsonb,now()-interval '4 days'
    );
    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+22),public.fixture_uuid(v_base+23),
      encode(digest(convert_to(v_system||' chain repair two','UTF8'),'sha256'),'hex'),
      v_c2,v_r2,v_positive,'AMEND_EXISTING_REPLACEMENT',
      'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED',
      v_c1,jsonb_build_array(v_positive::text),now()-interval '3 days'
    );
    perform public.fixture_add_completed_operation(
      v_identity,v_system,public.fixture_uuid(v_base+2),public.fixture_uuid(v_base+6),
      public.fixture_uuid(v_base+3),public.fixture_uuid(v_base+4),public.fixture_uuid(v_base+5),
      date '2026-08-02',public.fixture_uuid(v_base+24),public.fixture_uuid(v_base+25),
      encode(digest(convert_to(v_system||' chain repair three','UTF8'),'sha256'),'hex'),
      v_c3,v_r3,v_positive,'AMEND_EXISTING_REPLACEMENT',
      'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED',
      v_c2,jsonb_build_array(v_positive::text),now()-interval '2 days'
    );

    insert into public.timesheets(
      timesheet_id,contract_id,week_ending_date,actual_schedule_json,candidate_hint_text,
      correction_id,correction_kind,parent_timesheet_id,created_at,updated_at,is_current,archived_at_utc
    ) values
      (v_r1,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',v_c1,
       'CHANGED_HOURS_REVERSAL',public.fixture_uuid(v_base+6),now()-interval '5 days',now()-interval '3 days',true,now()-interval '3 days'),
      (v_r2,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',v_c2,
       'CHANGED_HOURS_REVERSAL',public.fixture_uuid(v_base+6),now()-interval '3 days',now()-interval '2 days',true,now()-interval '2 days'),
      (v_r3,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',v_c3,
       'CHANGED_HOURS_REVERSAL',public.fixture_uuid(v_base+6),now()-interval '2 days',now()-interval '2 days',true,null),
      (v_positive,public.fixture_uuid(v_base+5),date '2026-08-02',v_schedule,'{}',v_c3,
       'CHANGED_HOURS_REPLACEMENT',public.fixture_uuid(v_base+6),now()-interval '5 days',now()-interval '2 days',true,null);

    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      public.fixture_uuid(v_base),jsonb_build_array(v_item)
    ) h;

    perform public.fixture_assert(exists(
      select 1 from jsonb_array_elements(v_balance->'correction_member_supersession_lineage') edge
      where edge->>'member_timesheet_id'=v_positive::text
        and edge->>'superseded_correction_id'=v_c1
        and edge->>'canonical_correction_id'=v_c3
        and (edge->>'supersession_depth')::integer=2
    ),v_system||' transitive C1 to C3 supersession was not canonicalised');
    perform public.fixture_assert(v_balance->>'reviewed_existing_correction_id'=v_c3,
      v_system||' terminal C3 correction was not selected');
    perform public.fixture_assert(nullif(v_balance->>'blocking_code','') is null,
      v_system||' valid transitive repair chain was blocked');
    raise notice 'PASS transitive archived-member supersession: %',v_system;
  end loop;
end
$test$;

-- Case group 6: invalid route/mode pairs and altered frozen source authority
-- must be rejected for both authoritative Weekly sources.
do $test$
declare
  v_system text;
  v_base integer;
  v_case integer;
  v_route text;
  v_request_mode text;
  v_applied_mode text;
  v_item jsonb;
  v_balance jsonb;
  v_operation uuid;
  v_action text;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,11000),('HEALTHROSTER'::text,12000)) cases(source_system,base_value)
  loop
    for v_case,v_route,v_request_mode,v_applied_mode in
      select * from (values
        (1,'CREATE_REVERSAL_REPLACEMENT','RETAIN_EXISTING_CORRECTION_ID','RETAIN_EXISTING_CORRECTION_ID'),
        (2,'CREATE_REVERSAL_REPLACEMENT','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'),
        (3,'AMEND_EXISTING_REPLACEMENT','CREATE_NEW_GENERATION','CREATE_NEW_GENERATION')
      ) invalid(case_id,route,request_mode,applied_mode)
    loop
      perform public.fixture_reset();
      v_item:=public.fixture_seed_source(v_system,v_base+v_case*100);
      v_operation:=public.fixture_uuid(v_base+v_case*100+20);
      v_action:=encode(digest(convert_to(v_system||' invalid mode '||v_case,'UTF8'),'sha256'),'hex');
      perform public.fixture_add_completed_operation(
        v_item->>'source_identity',v_system,(v_item->>'source_shift_id')::uuid,(v_item->>'source_timesheet_id')::uuid,
        (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
        (v_item->>'week_ending_date')::date,public.fixture_uuid(v_base+v_case*100+40),v_operation,v_action,
        'INVALID-'||v_case,public.fixture_uuid(v_base+v_case*100+30),public.fixture_uuid(v_base+v_case*100+31),
        v_route,v_request_mode,v_applied_mode,'OLD-'||v_case,
        jsonb_build_array(public.fixture_uuid(v_base+v_case*100+31)::text),now()-interval '1 day'
      );
      select h.balance_json into strict v_balance
      from public._import_review_effective_invoice_balance_core_v1(
        (v_item->>'authoritative_import_id')::uuid,jsonb_build_array(v_item)
      ) h;
      perform public.fixture_assert(v_balance->>'blocking_code'='IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE',
        v_system||' invalid route/mode combination was accepted: '||v_case);
    end loop;

    for v_case in 4..6 loop
      perform public.fixture_reset();
      v_item:=public.fixture_seed_source(v_system,v_base+v_case*100);
      v_operation:=public.fixture_uuid(v_base+v_case*100+20);
      v_action:=encode(digest(convert_to(v_system||' invalid scope '||v_case,'UTF8'),'sha256'),'hex');
      perform public.fixture_add_completed_operation(
        v_item->>'source_identity',v_system,(v_item->>'source_shift_id')::uuid,(v_item->>'source_timesheet_id')::uuid,
        (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
        (v_item->>'week_ending_date')::date,public.fixture_uuid(v_base+v_case*100+40),v_operation,v_action,
        'INVALID-SCOPE-'||v_case,public.fixture_uuid(v_base+v_case*100+30),public.fixture_uuid(v_base+v_case*100+31),
        'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',null,'[]'::jsonb,now()-interval '1 day'
      );
      if v_case=4 then
        update public.import_apply_operations
        set response_json=jsonb_set(response_json,'{request_envelope,reconciliation_units,0,invoice_stream}','"NORMAL"'::jsonb)
        where id=v_operation;
      elsif v_case=5 then
        update public.import_apply_operations
        set response_json=jsonb_set(response_json,'{request_envelope,reconciliation_units,0,source_scope_fingerprint}',to_jsonb(repeat('0',64)))
        where id=v_operation;
      else
        update public.import_review_action_outcomes set evidence_fingerprint=repeat('0',64) where operation_id=v_operation;
      end if;
      select h.balance_json into strict v_balance
      from public._import_review_effective_invoice_balance_core_v1(
        (v_item->>'authoritative_import_id')::uuid,jsonb_build_array(v_item)
      ) h;
      perform public.fixture_assert(v_balance->>'blocking_code'='IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE',
        v_system||' altered frozen source authority was accepted: '||v_case);
    end loop;
    raise notice 'PASS invalid operation authority controls: %',v_system;
  end loop;
end
$test$;

-- Case group 7: malformed credit header or source provenance must fail closed.
do $test$
declare
  v_system text;
  v_base integer;
  v_case integer;
  v_item jsonb;
  v_balance jsonb;
  v_source_timesheet uuid;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,13000),('HEALTHROSTER'::text,14000)) cases(source_system,base_value)
  loop
    for v_case in 1..2 loop
      perform public.fixture_reset();
      v_item:=public.fixture_seed_source(v_system,v_base+v_case*100);
      v_source_timesheet:=(v_item->>'source_timesheet_id')::uuid;
      insert into public.invoices(id,client_id,type,status,issued_at_utc,original_invoice_id,issue_state)
      values
        (public.fixture_uuid(v_base+v_case*100+40),(v_item->>'client_id')::uuid,'INVOICE','ISSUED',now()-interval '2 days',null,'ISSUED'),
        (public.fixture_uuid(v_base+v_case*100+41),(v_item->>'client_id')::uuid,'CREDIT_NOTE','ISSUED',now()-interval '1 day',
          case when v_case=1 then public.fixture_uuid(v_base+v_case*100+49) else public.fixture_uuid(v_base+v_case*100+40) end,'ISSUED');
      insert into public.invoice_lines(
        id,invoice_id,timesheet_id,meta_json,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,created_at
      ) values
        (public.fixture_uuid(v_base+v_case*100+42),public.fixture_uuid(v_base+v_case*100+40),v_source_timesheet,
         '{"line_type":"HOURS_WEEKLY"}'::jsonb,10,0,0,0,0,100,120,20,now()-interval '2 days'),
        (public.fixture_uuid(v_base+v_case*100+43),public.fixture_uuid(v_base+v_case*100+41),
         case when v_case=2 then public.fixture_uuid(v_base+v_case*100+48) else v_source_timesheet end,
         jsonb_build_object('line_type','HOURS_WEEKLY','original_invoice_line_id',public.fixture_uuid(v_base+v_case*100+42)::text),
         10,0,0,0,0,-100,-120,-20,now()-interval '1 day');
      select h.balance_json into strict v_balance
      from public._import_review_effective_invoice_balance_core_v1(
        (v_item->>'authoritative_import_id')::uuid,jsonb_build_array(v_item)
      ) h;
      perform public.fixture_assert(v_balance->>'blocking_code'='IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE',
        v_system||' malformed credit provenance was accepted: '||v_case);
    end loop;
    raise notice 'PASS malformed credit provenance controls: %',v_system;
  end loop;
end
$test$;

-- Case group 8: all completed generations remain historical authority and B
-- is the signed sum of physical Weekly-hours components, not the latest row.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_balance jsonb;
  v_identity text;
  v_invoice uuid;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,15000),('HEALTHROSTER'::text,16000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_identity:=v_item->>'source_identity';
    perform public.fixture_add_completed_operation(
      v_identity,v_system,(v_item->>'source_shift_id')::uuid,(v_item->>'source_timesheet_id')::uuid,
      (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
      (v_item->>'week_ending_date')::date,public.fixture_uuid(v_base+20),public.fixture_uuid(v_base+21),
      encode(digest(convert_to(v_system||' generation one','UTF8'),'sha256'),'hex'),'G1-'||v_system,
      public.fixture_uuid(v_base+30),public.fixture_uuid(v_base+31),
      'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',null,'[]'::jsonb,now()-interval '6 days',11
    );
    perform public.fixture_add_completed_operation(
      v_identity,v_system,(v_item->>'source_shift_id')::uuid,(v_item->>'source_timesheet_id')::uuid,
      (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
      (v_item->>'week_ending_date')::date,public.fixture_uuid(v_base+22),public.fixture_uuid(v_base+23),
      encode(digest(convert_to(v_system||' generation two','UTF8'),'sha256'),'hex'),'G2-'||v_system,
      public.fixture_uuid(v_base+32),public.fixture_uuid(v_base+33),
      'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',null,'[]'::jsonb,now()-interval '4 days',12
    );
    perform public.fixture_add_completed_operation(
      v_identity,v_system,(v_item->>'source_shift_id')::uuid,(v_item->>'source_timesheet_id')::uuid,
      (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
      (v_item->>'week_ending_date')::date,public.fixture_uuid(v_base+24),public.fixture_uuid(v_base+25),
      encode(digest(convert_to(v_system||' generation three','UTF8'),'sha256'),'hex'),'G3-'||v_system,
      public.fixture_uuid(v_base+34),public.fixture_uuid(v_base+35),
      'CREATE_REVERSAL_REPLACEMENT',null,'CREATE_NEW_GENERATION',null,'[]'::jsonb,now()-interval '2 days',10
    );
    v_invoice:=public.fixture_uuid(v_base+40);
    insert into public.invoices(id,client_id,type,status,issued_at_utc,original_invoice_id,issue_state)
    values(v_invoice,(v_item->>'client_id')::uuid,'INVOICE','ISSUED',now()-interval '1 day',null,'ISSUED');
    insert into public.invoice_lines(
      id,invoice_id,timesheet_id,meta_json,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,created_at
    ) values
      (public.fixture_uuid(v_base+41),v_invoice,(v_item->>'source_timesheet_id')::uuid,'{"line_type":"HOURS_WEEKLY"}',10,0,0,0,0,100,120,20,now()-interval '7 days'),
      (public.fixture_uuid(v_base+42),v_invoice,public.fixture_uuid(v_base+30),'{"line_type":"HOURS_WEEKLY"}',-10,0,0,0,0,-100,-120,-20,now()-interval '6 days'),
      (public.fixture_uuid(v_base+43),v_invoice,public.fixture_uuid(v_base+31),'{"line_type":"HOURS_WEEKLY"}',11,0,0,0,0,110,132,22,now()-interval '6 days'),
      (public.fixture_uuid(v_base+44),v_invoice,public.fixture_uuid(v_base+32),'{"line_type":"HOURS_WEEKLY"}',-11,0,0,0,0,-110,-132,-22,now()-interval '4 days'),
      (public.fixture_uuid(v_base+45),v_invoice,public.fixture_uuid(v_base+33),'{"line_type":"HOURS_WEEKLY"}',12,0,0,0,0,120,144,24,now()-interval '4 days'),
      (public.fixture_uuid(v_base+46),v_invoice,public.fixture_uuid(v_base+34),'{"line_type":"HOURS_WEEKLY"}',-12,0,0,0,0,-120,-144,-24,now()-interval '2 days'),
      (public.fixture_uuid(v_base+47),v_invoice,public.fixture_uuid(v_base+35),'{"line_type":"HOURS_WEEKLY"}',10,0,0,0,0,100,120,20,now()-interval '2 days');
    select h.balance_json into strict v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      (v_item->>'authoritative_import_id')::uuid,jsonb_build_array(v_item)
    ) h;
    perform public.fixture_assert((v_balance->>'validated_completed_operation_evidence_count')::integer=3,
      v_system||' did not retain all completed generations');
    perform public.fixture_assert((v_balance#>>'{B_hours,total_hours}')::numeric=10
      and (v_balance#>>'{B_financials,pay_ex_vat}')::numeric=100
      and (v_balance#>>'{B_financials,charge_ex_vat}')::numeric=120,
      v_system||' repeated correction arithmetic did not net to 10');
    perform public.fixture_assert(v_balance->'fully_invoiced_generation_ids' @>
      jsonb_build_array('G1-'||v_system,'G2-'||v_system,'G3-'||v_system),
      v_system||' repeated generations were not all classified as historical');
    perform public.fixture_assert(nullif(v_balance->>'blocking_code','') is null,
      v_system||' repeated correction history was blocked');
    raise notice 'PASS repeated correction chain arithmetic: %',v_system;
  end loop;
end
$test$;

-- Case group 9: execute the real apply-envelope producer for both source
-- systems and prove its fresh-generation request shape is the one accepted by
-- the helper alongside the real phase-3 CREATE_NEW_GENERATION result contract.
do $test$
declare
  v_system text;
  v_base integer;
  v_item jsonb;
  v_import uuid;
  v_action text;
  v_evidence_fp text;
  v_recon_fp text;
  v_policy_fp text:=repeat('f',64);
  v_scope_fp text;
  v_envelope jsonb;
  v_unit jsonb;
  v_expected_unit_fp text;
begin
  for v_system,v_base in
    select * from (values ('NHSP'::text,17000),('HEALTHROSTER'::text,18000)) cases(source_system,base_value)
  loop
    perform public.fixture_reset();
    v_item:=public.fixture_seed_source(v_system,v_base);
    v_import:=(v_item->>'authoritative_import_id')::uuid;
    v_action:=encode(digest(convert_to(v_system||' apply envelope producer','UTF8'),'sha256'),'hex');
    v_evidence_fp:=encode(digest(convert_to('action-evidence|'||v_action,'UTF8'),'sha256'),'hex');
    v_recon_fp:=repeat(substr(v_action,2,1),64);
    v_scope_fp:=encode(digest(convert_to(concat_ws('|','source-scope-v1',v_item->>'source_identity',v_system,
      v_item->>'source_shift_id',v_item->>'external_row_key',v_item->>'source_timesheet_id',
      v_item->>'candidate_id',v_item->>'client_id',v_item->>'contract_id',v_item->>'week_ending_date','SELF_BILL'),'UTF8'),'sha256'),'hex');

    insert into public.import_review_states(import_id,preview_fingerprint)
    values(v_import,repeat('p',64));
    insert into public.hr_imports(id,source_system,coverage_fingerprint)
    values(v_import,v_system::public.hr_source_enum,repeat('c',64));
    insert into public.import_review_decisions(
      action_id,import_id,action_kind,source_identity,shift_id,candidate_id,client_id,contract_id,
      timesheet_id,hr_row_id,is_current,selected,selectable,summary_json,evidence_fingerprint
    ) values(
      v_action,v_import,'APPLY_AMENDMENT',v_item->>'source_identity',(v_item->>'source_shift_id')::uuid,
      (v_item->>'candidate_id')::uuid,(v_item->>'client_id')::uuid,(v_item->>'contract_id')::uuid,
      (v_item->>'source_timesheet_id')::uuid,(v_item->>'hr_row_id')::uuid,true,true,true,
      jsonb_build_object(
        'authority_mode','AUTHORITATIVE','is_daily',false,'existing_shift_id',v_item->>'source_shift_id',
        'week_ending_date',v_item->>'week_ending_date','invoice_stream','SELF_BILL',
        'source_scope_fingerprint',v_scope_fp,'amendment_route','CREATE_REVERSAL_REPLACEMENT',
        'reconciliation_mode','B_PLUS_M_TO_A','effective_invoice_ids','[]'::jsonb,
        'effective_invoice_line_ids','[]'::jsonb,'B_hours','{}'::jsonb,'B_financials','{}'::jsonb,
        'B_standard_schedule_json','[]'::jsonb,'active_mutable_member_ids','[]'::jsonb,
        'physically_missing_mutable_roles','[]'::jsonb,'M_hours','{}'::jsonb,
        'A_schedule_json',v_item->'authoritative_schedule_json','A_hours',v_item->'authoritative_hours',
        'review_policy_basis_kind','FROZEN_RECONCILIATION','review_policy_basis_fingerprint',v_policy_fp,
        'reconciliation_fingerprint',v_recon_fp
      ),v_evidence_fp
    );

    v_envelope:=public._import_review_apply_envelope_core_v1(v_import);
    v_unit:=v_envelope#>'{reconciliation_units,0}';
    v_expected_unit_fp:=public._import_review_hash_v1(concat_ws('|','unit-v2',v_action,v_item->>'source_identity',
      v_item->>'source_shift_id','CREATE_REVERSAL_REPLACEMENT','B_PLUS_M_TO_A',v_recon_fp,
      'FROZEN_RECONCILIATION',v_policy_fp,v_evidence_fp));
    perform public.fixture_assert(v_unit->>'route'='CREATE_REVERSAL_REPLACEMENT',
      v_system||' real apply envelope did not retain the reviewed route');
    perform public.fixture_assert(nullif(v_unit->>'repair_identity_mode','') is null,
      v_system||' real fresh-generation request unexpectedly supplied a repair mode');
    perform public.fixture_assert(v_unit->>'unit_fingerprint'=v_expected_unit_fp,
      v_system||' real apply envelope unit-v2 fingerprint was not exact');
    perform public.fixture_assert(v_unit->'expected_roles'=jsonb_build_array('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'),
      v_system||' real apply envelope omitted the two correction roles');
    raise notice 'PASS real apply-envelope producer contract: %',v_system;
  end loop;
end
$test$;

rollback;
