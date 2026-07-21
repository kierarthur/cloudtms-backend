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
begin
  if p_timesheet_id is null then raise exception 'INVOICE_CORRECTION_TIMESHEET_ID_REQUIRED' using errcode='22023'; end if;
  if p_max_members<1 or p_max_members>100 then raise exception 'INVOICE_CORRECTION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_actor_user_id is not null then
    perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
    if not found then raise exception 'INVOICE_CORRECTION_ACTOR_INVALID' using errcode='42501'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
  end if;
  v_unit:=v_chain->'requested_correction_unit';
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
      tf.id tsfin_id,tf.client_id,tf.processing_status,tf.is_stale,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      c.self_bill
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_current_stream:=case when coalesce(r.self_bill,false) then 'SELF_BILL' else 'NORMAL' end;
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
    count(distinct case when coalesce(c.self_bill,false) then 'SELF_BILL' else 'NORMAL' end)
  into v_client_count,v_contract_count,v_week_count,v_stream_count
  from public.timesheets ts
  join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  left join public.contracts c on c.id=ts.contract_id
  where ts.timesheet_id=any(v_ids);

  select count(distinct il.timesheet_id),count(distinct il.invoice_id)
  into v_line_member_count,v_line_invoice_count
  from public.invoice_lines il where il.timesheet_id=any(v_ids);

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
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
      and (case when coalesce(c.self_bill,false) then 'SELF_BILL' else 'NORMAL' end)
        is distinct from v_expected_stream
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_FROZEN_STREAM_DRIFT',
      'expected_stream',v_expected_stream
    ));
  end if;
  if v_line_member_count not in (0,v_expected_count) or (v_line_member_count=v_expected_count and v_line_invoice_count<>1) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_UNIT_SPLIT'));
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
    'line_policy_mismatch_count',v_line_policy_mismatch_count,'errors',v_errors);
end;
$function$;

comment on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) is
  'Validates one complete import correction unit for invoice materialisation. Supports reversal-only, independent leg VAT, self-bill stream isolation, and DRAFT plus issued_at-null append eligibility.';
revoke all on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) from public,anon,authenticated;
grant execute on function public.invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer) to service_role;
