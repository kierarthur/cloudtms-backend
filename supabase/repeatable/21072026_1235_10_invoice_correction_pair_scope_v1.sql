create or replace function public.invoice_correction_pair_scope_v1(
  p_timesheet_id uuid,
  p_target_invoice_id uuid default null,
  p_actor_user_id uuid default null,
  p_lock_rows boolean default true,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_ready_count integer:=0;
  v_line_member_count integer:=0;
  v_line_invoice_count integer:=0;
  v_client_count integer:=0;
  v_contract_count integer:=0;
  v_week_count integer:=0;
  v_stream_count integer:=0;
  v_target public.invoices%rowtype;
  v_rows jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  r record;
  v_leg jsonb;
  v_policy_ready boolean;
  v_expected_stream text;
  v_current_stream text;
  v_target_stream text;
  v_line_policy_mismatch_count integer:=0;
  v_reversal_line_count integer:=0;
  v_replacement_line_count integer:=0;
  v_reversal_invoice_ids uuid[]:=array[]::uuid[];
  v_replacement_invoice_ids uuid[]:=array[]::uuid[];
  v_placement_state text:='MALFORMED_PAIR';
  v_placement_compatible boolean:=false;
  v_placement_invoices jsonb:='[]'::jsonb;
  v_compatibility_mode text;
  v_operation public.import_apply_operations%rowtype;
  v_operation_unit jsonb;
  v_operation_unit_count integer:=0;
  v_balance jsonb;
  v_balance_row_count integer:=0;
  v_timesheet public.timesheets%rowtype;
  v_pair_correction_id text;
  v_pair_parent_id uuid;
  v_pair_operation_id uuid;
  v_pair_unit_fingerprint text;
  v_pair_source_identity text;
  v_pair_envelope_count integer:=0;
  v_pair_parent_count integer:=0;
  v_pair_member_count integer:=0;
  v_pair_reversal_count integer:=0;
  v_pair_replacement_count integer:=0;
  v_missing_ids uuid[]:=array[]::uuid[];
  v_recomputed_unit_fingerprint text;
begin
  if p_timesheet_id is null then raise exception 'INVOICE_CORRECTION_TIMESHEET_ID_REQUIRED' using errcode='22023'; end if;
  if p_max_members<1 or p_max_members>100 then raise exception 'INVOICE_CORRECTION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_actor_user_id is not null then
    perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
    if not found then raise exception 'INVOICE_CORRECTION_ACTOR_INVALID' using errcode='42501'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    -- The ordinary whole-chain validator remains authoritative.  This narrowly
    -- gated fallback exists only for a current pair committed by the reviewed
    -- authoritative-import reconciliation path when an older issued row was
    -- physically removed but its frozen invoice evidence remains provable.
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current and archived_at_utc is null;
    if v_timesheet.timesheet_id is null
       or not coalesce(v_timesheet.is_adjustment,false)
       or upper(coalesce(v_timesheet.adjustment_origin,''))<>'IMPORT_CORRECTION'
       or v_timesheet.correction_kind not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
       or v_timesheet.correction_id is null
       or coalesce(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    v_pair_correction_id:=v_timesheet.correction_id;
    v_pair_operation_id:=(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}')::uuid;
    v_pair_unit_fingerprint:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}','');
    v_pair_source_identity:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}','');

    select * into v_operation from public.import_apply_operations o
    where o.id=v_pair_operation_id and o.state='COMPLETE' and o.committed_at_utc is not null;
    if v_operation.id is null then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    select count(*)::integer,min(u::text)::jsonb into v_operation_unit_count,v_operation_unit
    from jsonb_array_elements(case when jsonb_typeof(v_operation.response_json->'reconciliation_units')='array'
      then v_operation.response_json->'reconciliation_units' else '[]'::jsonb end) u
    where u->>'schema_version'='IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
      and u->>'correction_id'=v_pair_correction_id
      and u->>'source_identity'=v_pair_source_identity
      and u->>'unit_fingerprint'=v_pair_unit_fingerprint
      and u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    if v_operation_unit_count<>1 then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select public._import_review_hash_v1(concat_ws('|','unit-v1',
      v_operation_unit->>'action_id',v_operation_unit->>'source_identity',v_operation_unit->>'route',
      v_operation_unit->>'reconciliation_fingerprint',outcome.evidence_fingerprint))
    into v_recomputed_unit_fingerprint
    from public.import_review_action_outcomes outcome
    where outcome.operation_id=v_operation.id and outcome.action_id=v_operation_unit->>'action_id';
    if v_recomputed_unit_fingerprint is distinct from v_operation_unit->>'unit_fingerprint' then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select count(*)::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')::integer,
      count(distinct t.parent_timesheet_id)::integer,
      min(t.parent_timesheet_id),
      count(distinct coalesce(
        t.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
        tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
      ))::integer,
      coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[])
    into v_pair_member_count,v_pair_reversal_count,v_pair_replacement_count,
      v_pair_parent_count,v_pair_parent_id,v_pair_envelope_count,v_ids
    from public.timesheets t
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.correction_id=v_pair_correction_id and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.adjustment_origin='IMPORT_CORRECTION'
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'=v_operation.id::text
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'=v_pair_unit_fingerprint
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'=v_pair_source_identity;
    if v_pair_member_count<>2 or v_pair_reversal_count<>1 or v_pair_replacement_count<>1
       or v_pair_parent_count<>1 or v_pair_parent_id is null or v_pair_envelope_count<>1
       or v_pair_parent_id is distinct from (v_operation_unit->>'parent_timesheet_id')::uuid
       or not exists(select 1 from public.timesheets parent_ts where parent_ts.timesheet_id=v_pair_parent_id
          and parent_ts.is_current and parent_ts.archived_at_utc is null) then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    if exists(select 1 from public.timesheets t left join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        t.contract_id is distinct from (v_operation_unit->>'contract_id')::uuid
        or t.week_ending_date is distinct from (v_operation_unit->>'week_ending_date')::date
        or tf.candidate_id is distinct from (v_operation_unit->>'candidate_id')::uuid
        or tf.client_id is distinct from (v_operation_unit->>'client_id')::uuid
        or jsonb_typeof(t.actual_schedule_json)<>'array'
        or jsonb_array_length(t.actual_schedule_json)<>1
        or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
          'shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity'))
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select coalesce(array_agg(x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_missing_ids
    from jsonb_array_elements_text(coalesce(v_operation_unit->'historical_missing_timesheet_ids','[]'::jsonb)) x(value);
    if cardinality(v_missing_ids)=0
       or exists(select 1 from unnest(v_missing_ids) missing_id
          where exists(select 1 from public.timesheets t where t.timesheet_id=missing_id)
             or not exists(select 1 from public.invoice_lines il join public.invoices i on i.id=il.invoice_id
               where (il.timesheet_id=missing_id or il.meta_json->>'timesheet_id'=missing_id::text)
                 and i.status in ('ISSUED','PAID','ON_HOLD') and i.issued_at_utc is not null)
             or not exists(select 1 from public.audit_events ae where ae.object_type='timesheets'
               and ae.object_id_text=missing_id::text
               and ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')))
       or exists(select 1 from jsonb_array_elements(coalesce(v_chain->'errors','[]'::jsonb)) e
          where e->>'code'<>'CORRECTION_UNIT_INVALID') then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select count(*)::integer,min(b.balance_json::text)::jsonb into v_balance_row_count,v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      v_operation.import_id,
      jsonb_build_array(jsonb_build_object(
        'source_identity',v_operation_unit->>'source_identity','source_system',v_operation_unit->>'source_system',
        'source_shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity',
        'hr_row_id',v_operation_unit->>'hr_row_id','source_timesheet_id',v_operation_unit->>'source_timesheet_id',
        'candidate_id',v_operation_unit->>'candidate_id','client_id',v_operation_unit->>'client_id',
        'contract_id',v_operation_unit->>'contract_id','week_ending_date',v_operation_unit->>'week_ending_date',
        'invoice_stream',v_operation_unit->>'invoice_stream','authoritative_import_id',v_operation.import_id,
        'authoritative_schedule_json',v_operation_unit->'A_schedule_json','authoritative_hours',v_operation_unit->'A_hours'
      )),1,512,256,128
    ) b;
    if v_balance_row_count<>1 or nullif(v_balance->>'blocking_code','') is not null
       or v_balance->>'effective_invoice_fingerprint' is distinct from v_operation_unit->>'B_invoice_fingerprint'
       or v_balance->'historical_missing_timesheet_ids' is distinct from to_jsonb(v_missing_ids) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;
    if exists(select 1 from public.timesheets t join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false)
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          tf.hours_day<>-coalesce((v_operation_unit#>>'{B_hours,hours_day}')::numeric,0)
          or tf.hours_night<>-coalesce((v_operation_unit#>>'{B_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>-coalesce((v_operation_unit#>>'{B_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>-coalesce((v_operation_unit#>>'{B_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>-coalesce((v_operation_unit#>>'{B_hours,hours_bh}')::numeric,0)
          or tf.total_pay_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
          or tf.total_charge_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          or tf.margin_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          tf.hours_day<>coalesce((v_operation_unit#>>'{A_hours,hours_day}')::numeric,0)
          or tf.hours_night<>coalesce((v_operation_unit#>>'{A_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>coalesce((v_operation_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>coalesce((v_operation_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>coalesce((v_operation_unit#>>'{A_hours,hours_bh}')::numeric,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    v_envelope:=public._ctms_correction_policy_envelope_read_v1(p_timesheet_id);
    v_unit:=jsonb_build_object('valid',true,'correction_id',v_pair_correction_id,
      'correction_shape','REVERSAL_REPLACEMENT','expected_member_count',2,
      'member_ids',to_jsonb(v_ids),'policy_envelope',v_envelope);
    v_chain:=jsonb_build_object('root_timesheet_id',v_envelope->>'root_timesheet_id');
    v_compatibility_mode:='IMPORT_AUTHORITATIVE_RECONCILIATION_V1';
  else
    v_unit:=v_chain->'requested_correction_unit';
  end if;
  if jsonb_typeof(v_unit)<>'object' or coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_UNIT_INVALID' using errcode='P0001';
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_stream:=upper(btrim(coalesce(v_envelope->>'invoice_stream','')));
  if v_expected_stream not in ('NORMAL','SELF_BILL') then
    raise exception 'INVOICE_CORRECTION_FROZEN_STREAM_INVALID' using errcode='P0001';
  end if;
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then raise exception 'INVOICE_CORRECTION_MEMBER_COUNT_MISMATCH' using errcode='P0001'; end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids) order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.timesheet_id=any(v_ids) and tf.is_current=true
      order by tf.timesheet_id,tf.id for update;
  end if;

  if p_target_invoice_id is not null then
    if p_lock_rows then select * into v_target from public.invoices where id=p_target_invoice_id for update;
    else select * into v_target from public.invoices where id=p_target_invoice_id; end if;
    if not found then raise exception 'INVOICE_CORRECTION_TARGET_NOT_FOUND' using errcode='P0002'; end if;
    if upper(coalesce(v_target.status::text,''))<>'DRAFT' or v_target.issued_at_utc is not null then
      raise exception 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' using errcode='P0001',
        detail=jsonb_build_object('invoice_id',p_target_invoice_id,'status',v_target.status,'issued_at_utc',v_target.issued_at_utc)::text;
    end if;
    v_target_stream:=case
      when lower(coalesce(v_target.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
        then 'SELF_BILL' else 'NORMAL' end;
    if v_target_stream is distinct from v_expected_stream then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
        'code','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
        'expected_stream',v_expected_stream,
        'target_stream',v_target_stream
      ));
    end if;
  end if;

  for r in
    select ts.timesheet_id,ts.correction_kind,ts.contract_id,ts.week_ending_date,
      tf.id tsfin_id,tf.client_id,tf.basis,tf.processing_status,tf.is_stale,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      c.self_bill
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_current_stream:=case
      when upper(coalesce(r.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end;
    v_policy_ready:=
      coalesce(r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
               r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
               r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
        is not distinct from v_envelope->>'envelope_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_leg_fingerprint',r.rate_source_refs_json->>'correction_leg_fingerprint')
        is not distinct from v_leg->>'leg_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',r.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint',r.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
      and r.policy_snapshot_json->'correction_invoice_policy'
        is not distinct from v_leg->'invoice_policy'
      and upper(btrim(coalesce(r.policy_snapshot_json->>'correction_invoice_stream','')))
        is not distinct from v_expected_stream
      and upper(btrim(coalesce(v_leg#>>'{invoice_policy,invoice_stream}','')))
        is not distinct from v_expected_stream
      and v_current_stream is not distinct from v_expected_stream;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
       and v_policy_ready then v_ready_count:=v_ready_count+1; end if;
    if not v_policy_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id)); end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,'tsfin_id',r.tsfin_id,
      'client_id',r.client_id,'contract_id',r.contract_id,'week_ending_date',r.week_ending_date,
       'invoice_stream',v_expected_stream,'current_contract_stream',v_current_stream,
       'processing_status',r.processing_status,'policy_ready',v_policy_ready,
       'invoice_vat_chargeable',v_leg#>'{invoice_policy,invoice_vat_chargeable}',
       'invoice_vat_rate_pct',v_leg#>'{invoice_policy,applied_vat_rate_pct}',
       'invoice_policy_fingerprint',v_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
       'leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  select count(distinct tf.client_id),count(distinct ts.contract_id),count(distinct ts.week_ending_date),
    count(distinct case
      when upper(coalesce(tf.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end)
  into v_client_count,v_contract_count,v_week_count,v_stream_count
  from public.timesheets ts
  join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_ids);

  select count(distinct il.timesheet_id),count(distinct il.invoice_id)
  into v_line_member_count,v_line_invoice_count
  from public.invoice_lines il
  join public.invoices i on i.id=il.invoice_id
  where il.timesheet_id=any(v_ids)
    and upper(coalesce(i.type::text,''))<>'CREDIT_NOTE';

  select
    count(il.invoice_id) filter(where ts.correction_kind='CHANGED_HOURS_REVERSAL')::integer,
    count(il.invoice_id) filter(where ts.correction_kind='CHANGED_HOURS_REPLACEMENT')::integer,
    coalesce(array_agg(il.invoice_id order by il.invoice_id)
      filter(where ts.correction_kind='CHANGED_HOURS_REVERSAL'),array[]::uuid[]),
    coalesce(array_agg(il.invoice_id order by il.invoice_id)
      filter(where ts.correction_kind='CHANGED_HOURS_REPLACEMENT'),array[]::uuid[])
  into v_reversal_line_count,v_replacement_line_count,
       v_reversal_invoice_ids,v_replacement_invoice_ids
  from public.timesheets ts
  left join public.invoice_lines il on il.timesheet_id=ts.timesheet_id
    and exists(select 1 from public.invoices active_invoice where active_invoice.id=il.invoice_id
      and upper(coalesce(active_invoice.type::text,''))<>'CREDIT_NOTE')
  where ts.timesheet_id=any(v_ids);

  select coalesce(jsonb_agg(jsonb_build_object(
    'invoice_id',i.id,'status',i.status,'issued_at_utc',i.issued_at_utc,
    'client_id',i.client_id,
    'invoice_stream',case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
      then 'SELF_BILL' else 'NORMAL' end,
    'currency',upper(coalesce(nullif(i.header_snapshot_json->>'currency',''),nullif(i.header_snapshot_json#>>'{meta,currency}',''),'GBP')),
    'invoice_week',coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}',
      i.header_snapshot_json->>'week_ending_date',i.header_snapshot_json#>>'{meta,week_ending_date}'))
    order by i.id),'[]'::jsonb)
  into v_placement_invoices
  from public.invoices i
  where i.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids);

  if v_expected_count<>2 then
    v_placement_state:='MALFORMED_PAIR';
  elsif v_reversal_line_count>1 or v_replacement_line_count>1 then
    v_placement_state:='DUPLICATE_PLACEMENT';
  elsif v_reversal_line_count=0 and v_replacement_line_count=0 then
    v_placement_state:='UNPLACED';
    v_placement_compatible:=true;
  elsif (v_reversal_line_count=1 and v_replacement_line_count=0)
     or (v_reversal_line_count=0 and v_replacement_line_count=1) then
    v_placement_state:='INCOMPLETE_MOVE';
    v_placement_compatible:=true;
  elsif v_reversal_invoice_ids[1]=v_replacement_invoice_ids[1] then
    v_placement_state:='COMPLETE_SAME_INVOICE';
    v_placement_compatible:=true;
  else
    select count(distinct i.client_id)=1
       and count(distinct case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
         then 'SELF_BILL' else 'NORMAL' end)=1
       and count(distinct upper(coalesce(nullif(i.header_snapshot_json->>'currency',''),
         nullif(i.header_snapshot_json#>>'{meta,currency}',''),'GBP')))=1
       and count(distinct coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}',i.header_snapshot_json->>'week_ending_date',
         i.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text
           from public.timesheets ts where ts.timesheet_id=any(v_ids))))=1
    into v_placement_compatible
    from public.invoices i
    where i.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids);
    v_placement_state:=case when v_placement_compatible then 'COMPLETE_SPLIT_INVOICES'
      else 'INCOMPATIBLE_PLACEMENT' end;
  end if;

  select count(*)::integer into v_line_policy_mismatch_count
  from public.invoice_lines il
  cross join lateral (
    select public._ctms_correction_policy_leg_read_v1(il.timesheet_id) leg
  ) expected
  where il.timesheet_id=any(v_ids)
    and (p_target_invoice_id is null or il.invoice_id=p_target_invoice_id)
    and il.vat_rate_pct is distinct from
      (expected.leg#>>'{invoice_policy,applied_vat_rate_pct}')::numeric;

  if v_ready_count<>v_expected_count then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TSFIN_NOT_READY','ready_count',v_ready_count)); end if;
  if v_client_count<>1 or v_contract_count<>1 or v_week_count<>1 or v_stream_count<>1 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_SCOPE_MIXED'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    join public.timesheets_financials tf
      on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
      and (case
        when upper(coalesce(tf.basis::text,'')) in (
          'NHSP','NHSP_ADJUSTMENT',
          'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
        ) then 'SELF_BILL'
        else 'NORMAL'
      end)
        is distinct from v_expected_stream
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_FROZEN_STREAM_DRIFT',
      'expected_stream',v_expected_stream
    ));
  end if;
  if v_placement_state='DUPLICATE_PLACEMENT' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_DUPLICATE_PLACEMENT'));
  elsif v_placement_state='INCOMPATIBLE_PLACEMENT' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_INCOMPATIBLE_PLACEMENT'));
  elsif v_placement_state='MALFORMED_PAIR' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_MALFORMED'));
  end if;
  if v_line_policy_mismatch_count<>0 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_LINE_VAT_POLICY_MISMATCH',
      'mismatching_line_count',v_line_policy_mismatch_count
    ));
  end if;
  if p_target_invoice_id is not null and v_target.client_id is distinct from (select tf.client_id from public.timesheets_financials tf where tf.timesheet_id=v_ids[1] and tf.is_current=true) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH'));
  end if;
  if p_target_invoice_id is not null and v_placement_state='INCOMPLETE_MOVE'
     and exists (
       select 1
       from public.invoices placed
       where placed.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids)
         and placed.id<>p_target_invoice_id
         and (
           placed.client_id is distinct from v_target.client_id
           or (case when lower(coalesce(placed.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
                 then 'SELF_BILL' else 'NORMAL' end) is distinct from v_target_stream
           or upper(coalesce(nullif(placed.header_snapshot_json->>'currency',''),
                nullif(placed.header_snapshot_json#>>'{meta,currency}',''),'GBP'))
              is distinct from upper(coalesce(nullif(v_target.header_snapshot_json->>'currency',''),
                nullif(v_target.header_snapshot_json#>>'{meta,currency}',''),'GBP'))
           or coalesce(placed.header_snapshot_json#>>'{meta,invoice_week_start}',placed.header_snapshot_json->>'week_ending_date',
                placed.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text from public.timesheets ts where ts.timesheet_id=any(v_ids)))
              is distinct from coalesce(v_target.header_snapshot_json#>>'{meta,invoice_week_start}',v_target.header_snapshot_json->>'week_ending_date',
                v_target.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text from public.timesheets ts where ts.timesheet_id=any(v_ids)))
         )
     ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_TARGET_INCOMPATIBLE_WITH_PARTNER'));
  end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'target_invoice_id',p_target_invoice_id,'target_appendable',p_target_invoice_id is null or jsonb_array_length(v_errors)=0,
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'invoice_stream',v_expected_stream,
    'pair_rows',v_rows,'ready_count',v_ready_count,'existing_line_member_count',v_line_member_count,
    'existing_line_invoice_count',v_line_invoice_count,
    'placement_state',v_placement_state,
    'placement_complete',v_placement_state in ('COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES'),
    'placement_compatible',v_placement_compatible,
    'placement_invoices',v_placement_invoices,
    'reversal_invoice_ids',to_jsonb(v_reversal_invoice_ids),
    'replacement_invoice_ids',to_jsonb(v_replacement_invoice_ids),
    'missing_member_kind',case
      when v_placement_state='INCOMPLETE_MOVE' and v_reversal_line_count=0 then 'CHANGED_HOURS_REVERSAL'
      when v_placement_state='INCOMPLETE_MOVE' and v_replacement_line_count=0 then 'CHANGED_HOURS_REPLACEMENT' end,
    'line_policy_mismatch_count',v_line_policy_mismatch_count,'errors',v_errors,
    'compatibility_mode',v_compatibility_mode);
end;
$function$;

comment on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) is
  'Validates one complete import correction unit for invoice materialisation. Supports reversal-only, independent leg VAT, self-bill stream isolation, and DRAFT plus issued_at-null append eligibility.';
revoke all on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) from public,anon,authenticated;
grant execute on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) to service_role;
