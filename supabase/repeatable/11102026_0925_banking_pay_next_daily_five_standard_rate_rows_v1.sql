-- DY-02: retain every Standard bucket and all fifteen dated rate cells.
-- Unknown amounts remain SQL/JSON null; all authorised provenance and ACLs retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_daily_approval_evidence_v1(p_financial_id uuid)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_tf public.timesheets_financials%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_context record;
  v_date date;
  v_rates jsonb;
  v_selected jsonb;
  v_paye jsonb;
  v_umbrella jsonb;
  v_rows jsonb;
  v_bucket record;
begin
  select tf.* into strict v_tf from public.timesheets_financials tf where tf.id=p_financial_id;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=v_tf.timesheet_id;
  if v_ts.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum
     or v_ts.line_type::text is distinct from 'HOURS'
     or v_ts.is_current is distinct from true or v_ts.archived_at_utc is not null or v_ts.revoked_at is not null
     or v_ts.is_adjustment is true or v_ts.parent_timesheet_id is not null or v_ts.correction_id is not null
     or nullif(btrim(v_ts.adjustment_origin),'') is not null or nullif(btrim(v_ts.correction_kind),'') is not null
     or nullif(btrim(v_ts.booking_id),'') is null
     or v_tf.is_current is distinct from true or v_tf.is_stale is distinct from false
     or v_tf.timesheet_version is distinct from v_ts.version
     or v_tf.authorised_at_utc is null or v_ts.authorised_at_server is null
     or v_tf.basis::text is distinct from 'SELF_REPORTED'
     or v_tf.candidate_id is null or v_tf.client_id is null or nullif(btrim(v_tf.role),'') is null
     or upper(v_tf.pay_method) not in ('PAYE','UMBRELLA') or v_tf.pay_method is null
     or v_tf.has_rate_issue is distinct from false or v_tf.has_pay_channel_issue is distinct from false
     or v_tf.invoice_breakdown_json->>'mode' is distinct from 'AGGREGATE'
     or jsonb_typeof(v_tf.policy_snapshot_json) is distinct from 'object'
     or jsonb_typeof(v_tf.rate_source_refs_json) is distinct from 'object'
     or v_tf.worked_start_iso is null or v_tf.worked_end_iso is null or v_tf.worked_end_iso<=v_tf.worked_start_iso
     or (v_tf.worked_start_iso,v_tf.worked_end_iso,v_tf.break_start_iso,v_tf.break_end_iso,v_tf.break_minutes)
        is distinct from (v_ts.worked_start_iso,v_ts.worked_end_iso,v_ts.break_start_iso,v_ts.break_end_iso,v_ts.break_minutes)
     or (v_tf.break_start_iso is null)<>(v_tf.break_end_iso is null)
     or (v_tf.break_start_iso is not null and v_tf.break_end_iso<=v_tf.break_start_iso)
     or v_tf.break_minutes<0
     or v_tf.additional_units_json is distinct from '{}'::jsonb
     or v_tf.additional_pay_ex_vat is distinct from 0::numeric
     or coalesce(v_tf.expenses_pay_ex_vat,0)<>0 or coalesce(v_tf.mileage_pay_ex_vat,0)<>0
     or coalesce(v_tf.mileage_units,0)<>0 or coalesce(v_tf.travel_pay_ex_vat,0)<>0
     or coalesce(v_tf.accommodation_pay_ex_vat,0)<>0 or coalesce(v_tf.other_pay_ex_vat,0)<>0 then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_APPROVAL_PROVENANCE_REQUIRED';
  end if;
  -- A real contracted DAILY retains its existing Contract identity. This
  -- does not turn a Contract edit into authorisation or manufacture a Contract
  -- for a contractless Daily. Both paths require the same authorised TF below.
  if v_ts.contract_id is not null then
    select c.* into strict v_contract from public.contracts c where c.id=v_ts.contract_id;
    if v_contract.candidate_id is distinct from v_tf.candidate_id
       or v_contract.client_id is distinct from v_tf.client_id then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CONTRACT_IDENTITY_CONFLICT';
    end if;
  end if;
  -- The existing canonical context resolves the factual Candidate and Office
  -- financial Client. No latest-history lookup or Rota re-admission.
  select ctx.* into strict v_context
    from public.tsfin_load_context_batch(array[v_ts.timesheet_id]) ctx;
  if v_context.effective_timesheet_id is distinct from v_ts.timesheet_id
     or (v_context.out_cur_fin->>'id') is distinct from v_tf.id::text
     or (v_context.out_candidate->>'id') is distinct from v_tf.candidate_id::text
     or v_context.out_client_id is distinct from v_tf.client_id then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CONTEXT_IDENTITY_CONFLICT';
  end if;
  v_date:=(v_tf.worked_start_iso at time zone 'Europe/London')::date;
  if v_ts.week_ending_date is distinct from v_date+(7-extract(isodow from v_date)::integer) then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_WORK_DATE_CONFLICT';
  end if;
  -- Both channels use the SAME existing dated override/default resolver.
  -- The opposite channel remains unavailable when that authority returns NULL.
  select jsonb_object_agg(r.rate_type,to_jsonb(r)) into v_rates
    from public.tsfin_resolve_rates_batch(jsonb_build_array(
      jsonb_build_object('k',v_ts.timesheet_id,'candidate_id',v_tf.candidate_id,
        'client_id',v_tf.client_id,'role',v_tf.role,'band',v_tf.band,'date',v_date,'rate_type','PAYE'),
      jsonb_build_object('k',v_ts.timesheet_id,'candidate_id',v_tf.candidate_id,
        'client_id',v_tf.client_id,'role',v_tf.role,'band',v_tf.band,'date',v_date,'rate_type','UMBRELLA')
    )) r;
  v_selected:=v_rates->upper(v_tf.pay_method);
  if v_selected is null or coalesce(v_selected->>'source_kind','') not in ('CANDIDATE_OVERRIDE','CLIENT_DEFAULT')
     or private._candidate_daily_rate_contract_v1(v_selected) is distinct from
        private._candidate_daily_rate_contract_v1(jsonb_build_object(
          'source_kind',v_tf.rate_source_refs_json->>'kind',
          'override_id',v_tf.rate_source_refs_json->>'override_id',
          'default_id',v_tf.rate_source_refs_json->>'default_id',
          'rate_type',v_tf.rate_source_refs_json->>'rate_type',
          'pay_day',v_tf.pay_day,'pay_night',v_tf.pay_night,'pay_sat',v_tf.pay_sat,'pay_sun',v_tf.pay_sun,'pay_bh',v_tf.pay_bh,
          'charge_day',v_tf.charge_day,'charge_night',v_tf.charge_night,'charge_sat',v_tf.charge_sat,
          'charge_sun',v_tf.charge_sun,'charge_bh',v_tf.charge_bh
        )) then
    raise exception using errcode='40001',message='BPAY_NEXT_DAILY_RATE_AUTHORITY_STALE';
  end if;
  v_paye:=v_rates->'PAYE'; v_umbrella:=v_rates->'UMBRELLA';
  if (v_paye-array['k','rate_type','source_kind','override_id','pay_day','pay_night','pay_sat','pay_sun','pay_bh'])
     is distinct from
     (v_umbrella-array['k','rate_type','source_kind','override_id','pay_day','pay_night','pay_sat','pay_sun','pay_bh']) then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CHANNEL_CONTEXT_CONFLICT';
  end if;
  for v_bucket in select x.value from jsonb_each(v_rates) channel
    cross join lateral jsonb_each(channel.value) x
    where x.key in ('pay_day','pay_night','pay_sat','pay_sun','pay_bh',
                    'charge_day','charge_night','charge_sat','charge_sun','charge_bh')
  loop
    if v_bucket.value<>'null'::jsonb and
       ((v_bucket.value#>>'{}')::numeric<>round((v_bucket.value#>>'{}')::numeric,6)
        or (v_bucket.value#>>'{}')::numeric<0
        or abs((v_bucket.value#>>'{}')::numeric)>=1000000000000) then
      raise exception using errcode='22003',message='BPAY_NEXT_DAILY_AVAILABLE_RATE_PRECISION_INVALID';
    end if;
  end loop;
  select coalesce(jsonb_agg(jsonb_build_object('rate_family','STANDARD','rate_code',b.code,
      'unit_label',nullif(btrim(v_contract.bucket_labels_json->>lower(b.code)),''),
      'paye_rate',(v_paye->>b.pay_key)::numeric,
      'umbrella_rate',(v_umbrella->>b.pay_key)::numeric,'charge_rate',(v_paye->>b.charge_key)::numeric)
      order by b.code),'[]'::jsonb) into v_rows
    from (values ('DAY'::text,'pay_day'::text,'charge_day'::text),
      ('NIGHT','pay_night','charge_night'),('SAT','pay_sat','charge_sat'),
      ('SUN','pay_sun','charge_sun'),('BH','pay_bh','charge_bh')) b(code,pay_key,charge_key);
  return jsonb_build_object('contract_version','BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1',
    'timesheet_id',v_ts.timesheet_id,'timesheet_version',v_ts.version,'financial_id',v_tf.id,
    'booking_id',v_ts.booking_id,'candidate_id',v_tf.candidate_id,'client_id',v_tf.client_id,
    'work_date',v_date,'role',v_tf.role,'band',v_tf.band,
    'policy_snapshot_json',v_tf.policy_snapshot_json,'rate_source_refs_json',v_tf.rate_source_refs_json,
    'resolved_channels',v_rates,'rates',v_rows);
end
$function$;
alter function private.bpay_next_daily_approval_evidence_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_daily_approval_evidence_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
