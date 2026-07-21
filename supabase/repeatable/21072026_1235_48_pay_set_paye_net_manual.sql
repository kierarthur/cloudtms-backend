-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: ed7d701ee557.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.pay_set_paye_net_manual(
  p_pay_batch_id uuid,
  p_entries_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_entries jsonb := coalesce(p_entries_json, '[]'::jsonb);

  v_dup jsonb := '[]'::jsonb;
  v_ambig jsonb := '[]'::jsonb;
  v_unmatched jsonb := '[]'::jsonb;

  v_updated_count int := 0;
  v_cleared_count int := 0;

  v_batch record;
  v_week_start date;
  v_batch_status_upper text := null;
  v_execution_commit_state text := 'NOT_SUBMITTED';
  v_execution_commit_ref text := null;
  v_execution_committed_at_utc timestamptz := null;

  v_fresh jsonb := null;
  v_is_stale boolean := false;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;

  v_deleted_ded_items int := 0;
  v_deleted_ded_breakdowns int := 0;
  v_ins_overpay int := 0;
  v_ins_loan int := 0;
  v_ins_overpay_bd int := 0;
  v_ins_loan_bd int := 0;
  v_deleted_ded_reservations int := 0;
  v_deleted_manual_debt_breakdowns int := 0;
  v_released_manual_debt_reservations int := 0;
  v_voided_manual_debt_items int := 0;
  v_ins_manual_debt int := 0;
  v_ins_manual_debt_bd int := 0;
  v_ins_finance_reservations int := 0;
  v_upd_candidates int := 0;
  v_candidate_summaries jsonb := '[]'::jsonb;
begin
  perform public._ctms_assert_pay_batch_mutable_v1(p_pay_batch_id,'PAY_SET_PAYE_NET_MANUAL');
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id is required';
  end if;

  if jsonb_typeof(v_entries) <> 'array' then
    raise exception 'entries must be an array';
  end if;

  select
    pb.id,
    pb.status,
    pb.pay_date,
    pb.batch_kind_fixed,
    pb.cancelled_at_utc,
    pb.execution_commit_state,
    pb.execution_commit_ref,
    pb.execution_committed_at_utc
  into v_batch
  from public.pay_batches pb
  where pb.id = p_pay_batch_id
  for update;

  if v_batch.id is null then
    raise exception 'pay_batch not found';
  end if;

  if upper(btrim(coalesce(v_batch.batch_kind_fixed,''))) = 'LOANS' then
    raise exception 'PAYE_NET_NOT_APPLICABLE_FOR_LOANS_BATCH';
  end if;

  if v_batch.pay_date is null then
    raise exception 'pay_batch pay_date is required';
  end if;

  v_week_start := public._pay_week_start_monday(v_batch.pay_date);

  v_batch_status_upper := upper(btrim(coalesce(v_batch.status, '')));
  v_execution_commit_state := upper(btrim(coalesce(v_batch.execution_commit_state, 'NOT_SUBMITTED')));
  if v_execution_commit_state not in ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') then
    v_execution_commit_state := 'NOT_SUBMITTED';
  end if;
  v_execution_commit_ref := nullif(btrim(coalesce(v_batch.execution_commit_ref, '')), '');
  v_execution_committed_at_utc := v_batch.execution_committed_at_utc;

  if v_batch.cancelled_at_utc is not null
     or v_batch_status_upper in ('CANCELLED', 'CANCELED', 'SCHEDULED', 'SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED', 'EXECUTED', 'PAID', 'SETTLED')
     or v_execution_commit_state <> 'NOT_SUBMITTED'
     or v_execution_commit_ref is not null
     or v_execution_committed_at_utc is not null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_SET_PAYE_NET_MANUAL',
      'code', 'PAYE_NET_MUTATION_BLOCKED_BY_LIFECYCLE',
      'message', 'pay_set_paye_net_manual: PAYE net cannot be changed after the payment batch has crossed the execution/cancellation lifecycle boundary',
      'pay_batch_id', p_pay_batch_id::text,
      'status', v_batch.status,
      'cancelled_at_utc', case when v_batch.cancelled_at_utc is null then null else v_batch.cancelled_at_utc::text end,
      'execution_commit_state', v_execution_commit_state,
      'execution_commit_ref', v_execution_commit_ref,
      'execution_committed_at_utc', case when v_execution_committed_at_utc is null then null else v_execution_committed_at_utc::text end
    )::text;
  end if;

  v_fresh := public.pay_batch_validate_freshness(
    p_pay_batch_id => p_pay_batch_id,
    p_actor_user_id => p_actor_user_id,
    p_allow_large_full_scan => false
  );
  v_is_stale := coalesce((v_fresh->>'is_stale')::boolean, false);
  v_stale_reasons := coalesce(v_fresh->'stale_reasons', '[]'::jsonb);

  if v_is_stale = true then
    select coalesce(jsonb_agg(x.elem), '[]'::jsonb)
      into v_diff_sample
    from (
      select elem
      from jsonb_array_elements(coalesce(v_fresh->'diff','[]'::jsonb)) as elem
      limit 50
    ) x;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_SET_PAYE_NET_MANUAL',
      'code', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION' else 'BATCH_STALE' end,
      'message', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'pay_set_paye_net_manual: chunked freshness validation is required before changing PAYE net' else 'pay_set_paye_net_manual: batch is stale; regenerate draft before setting PAYE net' end,
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  create temp table if not exists _tmp_manual_net (
    pay_batch_candidate_id uuid,
    candidate_id uuid,
    works_raw text,
    works_norm text,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_manual_net;

  insert into _tmp_manual_net(pay_batch_candidate_id, candidate_id, works_raw, works_norm, net_amount)
  select
    nullif(e->>'pay_batch_candidate_id','')::uuid as pay_batch_candidate_id,
    nullif(e->>'candidate_id','')::uuid as candidate_id,
    nullif(btrim(e->>'tms_ref'), '') as works_raw,
    case
      when nullif(btrim(e->>'tms_ref'), '') is null then null
      else upper(regexp_replace(btrim(e->>'tms_ref'), '\s+', '', 'g'))
    end as works_norm,
    case
      when nullif(btrim(coalesce(e->>'net_amount','')), '') is null then null
      else round((nullif(btrim(coalesce(e->>'net_amount','')), '')::numeric), 2)
    end as net_amount
  from jsonb_array_elements(v_entries) e
  where e is not null and jsonb_typeof(e)='object';

  if exists (
    select 1
    from _tmp_manual_net mn
    where mn.pay_batch_candidate_id is null
      and mn.candidate_id is null
      and (mn.works_norm is null or mn.works_norm = '')
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: each entry must include pay_batch_candidate_id, candidate_id or tms_ref/NI (Works Number)';
  end if;

  if exists (
    select 1
    from _tmp_manual_net t
    where t.net_amount is not null
      and t.net_amount < 0
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: net_amount must be non-negative';
  end if;

  select coalesce(jsonb_agg(x), '[]'::jsonb)
  into v_dup
  from (
    select to_jsonb(
      case
        when mn.pay_batch_candidate_id is not null then ('PBC:' || mn.pay_batch_candidate_id::text)
        when mn.candidate_id is not null then ('CAND:' || mn.candidate_id::text)
        else ('WORKS:' || mn.works_norm)
      end
    ) as x
    from _tmp_manual_net mn
    group by
      case
        when mn.pay_batch_candidate_id is not null then ('PBC:' || mn.pay_batch_candidate_id::text)
        when mn.candidate_id is not null then ('CAND:' || mn.candidate_id::text)
        else ('WORKS:' || mn.works_norm)
      end
    having count(*) > 1
  ) d;

  if jsonb_array_length(v_dup) > 0 then
    raise exception 'MANUAL_NET_INVALID: duplicate entries %', v_dup::text;
  end if;

  create temp table if not exists _tmp_batch_paye2 (
    pay_batch_candidate_id uuid,
    candidate_id uuid,
    tms_ref_norm text,
    ni_norm text
  ) on commit drop;

  truncate table _tmp_batch_paye2;

  insert into _tmp_batch_paye2(pay_batch_candidate_id, candidate_id, tms_ref_norm, ni_norm)
  select
    pbc.id,
    pbc.candidate_id,
    upper(regexp_replace(btrim(coalesce(pbc.candidate_tms_ref, c.tms_ref, '')), '\s+', '', 'g')) as tms_ref_norm,
    upper(regexp_replace(btrim(coalesce(c.ni_number, '')), '\s+', '', 'g')) as ni_norm
  from public.pay_batch_candidates pbc
  join public.candidates c
    on c.id = pbc.candidate_id
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', a.works_norm,
        'candidate_ids', a.candidate_ids
      )
    ),
    '[]'::jsonb
  )
  into v_ambig
  from (
    select
      mn.works_norm,
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids
    from _tmp_manual_net mn
    join _tmp_batch_paye2 bp
      on mn.pay_batch_candidate_id is null
     and mn.candidate_id is null
     and mn.works_norm is not null
     and mn.works_norm <> ''
     and (
       (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
       or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
     )
    group by mn.works_norm
    having count(distinct bp.candidate_id) > 1
  ) a;

  if jsonb_array_length(v_ambig) > 0 then
    raise exception 'MANUAL_NET_INVALID: ambiguous Works Number(s) %', v_ambig::text;
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', case when mn.pay_batch_candidate_id is null then null else mn.pay_batch_candidate_id::text end,
        'candidate_id', case when mn.candidate_id is null then null else mn.candidate_id::text end,
        'works_norm', mn.works_norm
      )
    ),
    '[]'::jsonb
  )
  into v_unmatched
  from _tmp_manual_net mn
  left join _tmp_batch_paye2 bp
    on (mn.pay_batch_candidate_id is not null and bp.pay_batch_candidate_id = mn.pay_batch_candidate_id)
    or (mn.pay_batch_candidate_id is null and mn.candidate_id is not null and bp.candidate_id = mn.candidate_id)
    or (
      mn.pay_batch_candidate_id is null
      and mn.candidate_id is null
      and mn.works_norm is not null
      and mn.works_norm <> ''
      and (
        (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
        or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
      )
    )
  where bp.pay_batch_candidate_id is null;

  if jsonb_array_length(v_unmatched) > 0 then
    raise exception 'MANUAL_NET_INVALID: entry candidate not found in pay batch PAYE candidates %', v_unmatched::text;
  end if;

  create temp table if not exists _tmp_manual_match (
    pay_batch_candidate_id uuid,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_manual_match;

  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is not null
   and bp.pay_batch_candidate_id = mn.pay_batch_candidate_id;

  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is null
   and mn.candidate_id is not null
   and bp.candidate_id = mn.candidate_id;

  insert into _tmp_manual_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    mn.net_amount
  from _tmp_manual_net mn
  join _tmp_batch_paye2 bp
    on mn.pay_batch_candidate_id is null
   and mn.candidate_id is null
   and mn.works_norm is not null
   and mn.works_norm <> ''
   and (
     (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = mn.works_norm)
     or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = mn.works_norm)
   );

  if exists (
    select 1
    from _tmp_manual_match mm
    group by mm.pay_batch_candidate_id
    having count(*) > 1
    limit 1
  ) then
    raise exception 'MANUAL_NET_INVALID: duplicate resolved candidates in request (internal match collision)';
  end if;

  select count(*)::int
  into v_updated_count
  from _tmp_manual_match mm
  where mm.net_amount is not null;

  select count(*)::int
  into v_cleared_count
  from _tmp_manual_match mm
  where mm.net_amount is null;

  delete from public.pay_batch_paye_net_inputs pni
  using _tmp_manual_match mm
  where pni.pay_batch_candidate_id = mm.pay_batch_candidate_id
    and pni.source = 'MANUAL_ENTRY';

  insert into public.pay_batch_paye_net_inputs(
    pay_batch_candidate_id, source, net_amount, imported_at_utc, file_name, file_hash
  )
  select
    mm.pay_batch_candidate_id,
    'MANUAL_ENTRY',
    mm.net_amount,
    now(),
    null,
    null
  from _tmp_manual_match mm
  where mm.net_amount is not null;

  update public.pay_batch_candidates pbc
  set paye_state = case
    when exists (
      select 1
      from public.pay_batch_paye_net_inputs pni2
      where pni2.pay_batch_candidate_id = pbc.id
      limit 1
    ) then 'READY'
    else 'PENDING_NET'
  end
  where pbc.pay_batch_id = p_pay_batch_id
    and pbc.paye_state is not null;

  ---------------------------------------------------------------------------
  -- ✅ Recompute deductions + net_bank_amount (PAYE candidates only)
  ---------------------------------------------------------------------------
  create temp table if not exists _tmp_paye_scope (
    pay_batch_candidate_id uuid primary key,
    candidate_id uuid not null
  ) on commit drop;

  truncate table _tmp_paye_scope;

  insert into _tmp_paye_scope(pay_batch_candidate_id, candidate_id)
  select
    pbc3.id,
    pbc3.candidate_id
  from public.pay_batch_candidates pbc3
  where pbc3.pay_batch_id = p_pay_batch_id
    and pbc3.paye_state is not null;


  create temp table if not exists _tmp_recovery_templates (
    item_type text not null,
    pay_batch_candidate_id uuid not null,
    candidate_id uuid not null,
    finance_case_id uuid null,
    source_ref text null,
    pay_channel text null,
    umbrella_id uuid null,
    is_mismatch boolean null,
    paye_treatment text null,
    finance_component_id uuid null,
    frozen_component_snapshot_json jsonb null,
    frozen_component_key_type text null,
    frozen_component_key_value text null,
    frozen_component_classification public.pay_finance_component_classification_enum null,
    frozen_source_basis_json jsonb null,
    frozen_source_pay_method text null,
    frozen_target_pay_method text null,
    frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
    frozen_resolution_payload_json jsonb null,
    frozen_resolution_result_json jsonb null,
    frozen_source_amount numeric(12,2) null,
    frozen_target_amount_ex_vat numeric(12,2) null,
    frozen_target_amount_vat numeric(12,2) null,
    frozen_target_amount_inc_vat numeric(12,2) null,
    payout_instruction_snapshot_json jsonb null,
    template_sort_at timestamptz null,
    sort_order integer not null,
    frozen_case_type text null,
    frozen_payout_status text null,
    frozen_remaining_source_amount numeric(12,2) not null,
    frozen_weekly_due_amount numeric(12,2) not null,
    frozen_next_due_week_start date null,
    minimum_earnings_threshold numeric(12,2) null,
    take_home_floor_override numeric(12,2) null,
    default_take_home_floor numeric(12,2) null
  ) on commit drop;

  truncate table _tmp_recovery_templates;

  insert into _tmp_recovery_templates (
    item_type,
    pay_batch_candidate_id,
    candidate_id,
    finance_case_id,
    source_ref,
    pay_channel,
    umbrella_id,
    is_mismatch,
    paye_treatment,
    finance_component_id,
    frozen_component_snapshot_json,
    frozen_component_key_type,
    frozen_component_key_value,
    frozen_component_classification,
    frozen_source_basis_json,
    frozen_source_pay_method,
    frozen_target_pay_method,
    frozen_resolution_mode,
    frozen_resolution_payload_json,
    frozen_resolution_result_json,
    frozen_source_amount,
    frozen_target_amount_ex_vat,
    frozen_target_amount_vat,
    frozen_target_amount_inc_vat,
    payout_instruction_snapshot_json,
    template_sort_at,
    sort_order,
    frozen_case_type,
    frozen_payout_status,
    frozen_remaining_source_amount,
    frozen_weekly_due_amount,
    frozen_next_due_week_start,
    minimum_earnings_threshold,
    take_home_floor_override,
    default_take_home_floor
  )
  select distinct on (
    pbi_rt.item_type,
    pbi_rt.pay_batch_candidate_id,
    coalesce(
      coalesce(
        pbi_rt.finance_case_id,
        case
          when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
          else null::uuid
        end
      )::text,
      ''
    ),
    coalesce(pbi_rt.finance_component_id::text, ''),
    coalesce(pbi_rt.source_ref, '')
  )
    pbi_rt.item_type,
    pbi_rt.pay_batch_candidate_id,
    pbc_rt.candidate_id,
    coalesce(
      pbi_rt.finance_case_id,
      case
        when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
        else null::uuid
      end
    ) as finance_case_id,
    pbi_rt.source_ref,
    pbi_rt.pay_channel,
    pbi_rt.umbrella_id,
    pbi_rt.is_mismatch,
    pbi_rt.paye_treatment,
    pbi_rt.finance_component_id,
    pbi_rt.frozen_component_snapshot_json,
    pbi_rt.frozen_component_key_type,
    pbi_rt.frozen_component_key_value,
    pbi_rt.frozen_component_classification,
    pbi_rt.frozen_source_basis_json,
    pbi_rt.frozen_source_pay_method,
    pbi_rt.frozen_target_pay_method,
    pbi_rt.frozen_resolution_mode,
    pbi_rt.frozen_resolution_payload_json,
    pbi_rt.frozen_resolution_result_json,
    round(coalesce(pbi_rt.frozen_source_amount, 0), 2)::numeric(12,2) as frozen_source_amount,
    round(coalesce(pbi_rt.frozen_target_amount_ex_vat, 0), 2)::numeric(12,2) as frozen_target_amount_ex_vat,
    round(coalesce(pbi_rt.frozen_target_amount_vat, 0), 2)::numeric(12,2) as frozen_target_amount_vat,
    round(coalesce(pbi_rt.frozen_target_amount_inc_vat, 0), 2)::numeric(12,2) as frozen_target_amount_inc_vat,
    pbi_rt.payout_instruction_snapshot_json,
    coalesce(pbi_rt.created_at, pbi_rt.updated_at, now()) as template_sort_at,
    case
      when nullif(btrim(coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order')), '') ~ '^-?[0-9]+$'
        then (coalesce(pbi_rt.frozen_source_basis_json->>'allocation_priority_order', pbi_rt.frozen_source_basis_json->>'sort_order', pbi_rt.frozen_resolution_payload_json->>'sort_order'))::integer
      else 2147483647
    end as sort_order,
    upper(coalesce(
      nullif(btrim(pbi_rt.frozen_source_basis_json->>'case_type'), ''),
      case pbi_rt.item_type
        when 'OVERPAYMENT_RECOVERY' then 'OVERPAYMENT'
        when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE'
        when 'MANUAL_DEBT_RECOVERY' then 'MANUAL_DEBT_ADJUSTMENT'
        else null
      end
    )) as frozen_case_type,
    upper(nullif(btrim(pbi_rt.frozen_source_basis_json->>'payout_status'), '')) as frozen_payout_status,
    round(greatest(
      coalesce(
        case
          when nullif(btrim(pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_component_snapshot_json->>'remaining_source_amount')::numeric
          when nullif(btrim(pbi_rt.frozen_source_basis_json->>'remaining_source_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'remaining_source_amount')::numeric
          when nullif(btrim(pbi_rt.frozen_source_basis_json->>'outstanding_amount'), '') is not null then (pbi_rt.frozen_source_basis_json->>'outstanding_amount')::numeric
          when nullif(btrim(pbi_rt.frozen_source_basis_json->>'amount'), '') is not null then abs((pbi_rt.frozen_source_basis_json->>'amount')::numeric)
          when pbi_rt.frozen_source_amount is not null then abs(pbi_rt.frozen_source_amount)
          else 0::numeric
        end,
        0::numeric
      ),
      0::numeric
    ), 2)::numeric(12,2) as frozen_remaining_source_amount,
    case
      when nullif(btrim(pbi_rt.frozen_source_basis_json->>'weekly_due'), '') is null then 0::numeric(12,2)
      else round(greatest(abs((pbi_rt.frozen_source_basis_json->>'weekly_due')::numeric), 0), 2)::numeric(12,2)
    end as frozen_weekly_due_amount,
    case
      when nullif(btrim(pbi_rt.frozen_source_basis_json->>'next_due_week_start'), '') is null then null::date
      else (pbi_rt.frozen_source_basis_json->>'next_due_week_start')::date
    end as frozen_next_due_week_start,
    case
      when nullif(btrim(pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold'), '') is null then null::numeric(12,2)
      else round(greatest((pbi_rt.frozen_source_basis_json->>'minimum_earnings_threshold')::numeric, 0), 2)::numeric(12,2)
    end as minimum_earnings_threshold,
    case
      when nullif(btrim(pbi_rt.frozen_source_basis_json->>'take_home_floor_override'), '') is null then null::numeric(12,2)
      else round(greatest((pbi_rt.frozen_source_basis_json->>'take_home_floor_override')::numeric, 0), 2)::numeric(12,2)
    end as take_home_floor_override,
    case
      when nullif(btrim(pbi_rt.frozen_source_basis_json->>'default_take_home_floor'), '') is null then null::numeric(12,2)
      else round(greatest((pbi_rt.frozen_source_basis_json->>'default_take_home_floor')::numeric, 0), 2)::numeric(12,2)
    end as default_take_home_floor
  from public.pay_batch_items pbi_rt
  join public.pay_batch_candidates pbc_rt
    on pbc_rt.id = pbi_rt.pay_batch_candidate_id
  where pbc_rt.pay_batch_id = p_pay_batch_id
    and pbi_rt.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
  order by
    pbi_rt.item_type,
    pbi_rt.pay_batch_candidate_id,
    coalesce(
      coalesce(
        pbi_rt.finance_case_id,
        case
          when pbi_rt.source_ref ~ '^advance:[0-9a-fA-F-]{36}$' then replace(pbi_rt.source_ref, 'advance:', '')::uuid
          else null::uuid
        end
      )::text,
      ''
    ),
    coalesce(pbi_rt.finance_component_id::text, ''),
    coalesce(pbi_rt.source_ref, ''),
    pbi_rt.is_voided desc,
    coalesce(pbi_rt.updated_at, pbi_rt.created_at) desc,
    pbi_rt.id desc;

  -- PAYE net is produced after gross-side deductions have already been sent
  -- through payroll. Reproject only net-side deductions here; the frozen gross
  -- items and their reservations must remain unchanged.
  create temp table if not exists _tmp_manual_debt_item_ids (
    id uuid primary key
  ) on commit drop;

  truncate table _tmp_manual_debt_item_ids;

  insert into _tmp_manual_debt_item_ids(id)
  select
    pbi_md_del.id
  from public.pay_batch_items pbi_md_del
  where pbi_md_del.pay_batch_candidate_id in (select sc_md_del.pay_batch_candidate_id from _tmp_paye_scope sc_md_del)
    and pbi_md_del.item_type = 'MANUAL_DEBT_RECOVERY'
    and upper(coalesce(pbi_md_del.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
    and pbi_md_del.is_voided = false;

  create temp table if not exists _tmp_ded_item_ids (
    id uuid primary key
  ) on commit drop;

  truncate table _tmp_ded_item_ids;

  insert into _tmp_ded_item_ids(id)
  select
    pbi_del.id
  from public.pay_batch_items pbi_del
  where pbi_del.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_del.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT')
    and upper(coalesce(pbi_del.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
    and pbi_del.is_voided = false;

  delete from public.pay_batch_item_breakdowns pbib_del
  using _tmp_ded_item_ids di
  where pbib_del.pay_batch_item_id = di.id;

  get diagnostics v_deleted_ded_breakdowns = row_count;

  delete from public.pay_batch_item_breakdowns pbib_md_del
  using _tmp_manual_debt_item_ids mdi_del
  where pbib_md_del.pay_batch_item_id = mdi_del.id;

  get diagnostics v_deleted_manual_debt_breakdowns = row_count;

  update public.pay_advance_reservations par_del
  set
    status = 'RELEASED',
    released_at_utc = now(),
    released_reason = 'PAYE_NET_REPROJECTION',
    updated_by_user_id = p_actor_user_id
  where par_del.pay_batch_item_id in (select di_res.id from _tmp_ded_item_ids di_res)
    and par_del.status = 'RESERVED';

  get diagnostics v_deleted_ded_reservations = row_count;

  update public.pay_advance_reservations par_md
  set
    status = 'RELEASED',
    released_at_utc = now(),
    released_reason = 'PAYE_NET_REPROJECTION',
    updated_by_user_id = p_actor_user_id
  where par_md.pay_batch_item_id in (select mdi_res.id from _tmp_manual_debt_item_ids mdi_res)
    and par_md.status = 'RESERVED';

  get diagnostics v_released_manual_debt_reservations = row_count;

  update public.pay_batch_items pbi_d_void
  set
    is_voided = true,
    reservation_id = null,
    updated_at = now()
  where pbi_d_void.id in (select di_void.id from _tmp_ded_item_ids di_void)
    and pbi_d_void.is_voided = false;

  v_deleted_ded_items := 0;

  update public.pay_batch_items pbi_md_void
  set
    is_voided = true,
    reservation_id = null,
    updated_at = now()
  where pbi_md_void.id in (select mdi_void.id from _tmp_manual_debt_item_ids mdi_void)
    and pbi_md_void.is_voided = false;

  get diagnostics v_voided_manual_debt_items = row_count;

  update public.pay_batch_candidates pbc_aw
  set
    awaiting_net_amount = not exists (
      select 1
      from public.pay_batch_paye_net_inputs pni_aw
      where pni_aw.pay_batch_candidate_id = pbc_aw.id
      limit 1
    ),
    updated_at = now()
  where pbc_aw.id in (select s.pay_batch_candidate_id from _tmp_paye_scope s);

  with ins_overpay as (
    insert into public.pay_batch_items (
      id,
      pay_batch_candidate_id,
      item_type,
      timesheet_id,
      segment_key,
      source_ref,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      repayment_week_start,
      pay_channel,
      umbrella_id,
      is_mismatch,
      is_voided,
      created_at,
      updated_at,
      finance_case_id,
      reservation_id,
      paye_treatment,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json
    )
    with cand_scope as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        pbc.awaiting_net_amount,
        greatest(coalesce(ni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount
      from public.pay_batch_candidates pbc
      join _tmp_paye_scope sc
        on sc.pay_batch_candidate_id = pbc.id
      left join lateral (
        select pni.net_amount
        from public.pay_batch_paye_net_inputs pni
        where pni.pay_batch_candidate_id = pbc.id
        order by pni.imported_at_utc desc
        limit 1
      ) ni on true
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from _tmp_recovery_templates rt
      where rt.item_type = 'OVERPAYMENT_RECOVERY'
        and rt.finance_case_id is not null
        and upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    alloc_base as (
      select
        cs.pay_batch_candidate_id,
        ct.finance_case_id,
        ct.source_ref,
        ct.pay_channel,
        ct.umbrella_id,
        ct.is_mismatch,
        ct.paye_treatment,
        ct.finance_component_id,
        ct.frozen_component_snapshot_json,
        ct.frozen_component_key_type,
        ct.frozen_component_key_value,
        ct.frozen_component_classification,
        ct.frozen_source_basis_json,
        ct.frozen_source_pay_method,
        ct.frozen_target_pay_method,
        ct.frozen_resolution_mode,
        ct.frozen_resolution_payload_json,
        ct.frozen_resolution_result_json,
        ct.frozen_source_amount,
        ct.frozen_target_amount_ex_vat,
        ct.frozen_target_amount_vat,
        ct.frozen_target_amount_inc_vat,
        ct.payout_instruction_snapshot_json,
        ct.frozen_remaining_source_amount,
        ct.sort_order,
        ct.template_sort_at,
        cs.paye_net_amount as available_pool_ex,
        sum(ct.frozen_remaining_source_amount) over (
          partition by cs.pay_batch_candidate_id
          order by ct.sort_order, ct.template_sort_at, ct.finance_case_id, ct.finance_component_id, coalesce(ct.source_ref, '')
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from cand_scope cs
      join case_templates ct
        on ct.pay_batch_candidate_id = cs.pay_batch_candidate_id
      where cs.awaiting_net_amount = false
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'OVERPAYMENT')) = 'OVERPAYMENT'
    ),
    alloc as (
      select
        ab.*,
        round(
          least(
            ab.frozen_remaining_source_amount,
            greatest(ab.available_pool_ex - coalesce(ab.cum_before_ex, 0), 0)
          ),
          2
        )::numeric(12,2) as take_ex
      from alloc_base ab
    )
    select
      gen_random_uuid() as id,
      a.pay_batch_candidate_id,
      'OVERPAYMENT_RECOVERY' as item_type,
      null::uuid as timesheet_id,
      null::text as segment_key,
      coalesce(a.source_ref, 'advance:' || a.finance_case_id::text) as source_ref,
      round(-a.take_ex, 2)::numeric(12,2) as amount_ex_vat,
      0::numeric(12,2) as amount_vat,
      round(-a.take_ex, 2)::numeric(12,2) as amount_inc_vat,
      v_week_start as repayment_week_start,
      coalesce(a.pay_channel, 'PAYE') as pay_channel,
      a.umbrella_id,
      coalesce(a.is_mismatch, false) as is_mismatch,
      false as is_voided,
      now() as created_at,
      now() as updated_at,
      a.finance_case_id,
      null::uuid as reservation_id,
      coalesce(a.paye_treatment, 'NET_DEDUCT') as paye_treatment,
      a.finance_component_id,
      a.frozen_component_snapshot_json,
      a.frozen_component_key_type,
      a.frozen_component_key_value,
      a.frozen_component_classification,
      a.frozen_source_basis_json,
      a.frozen_source_pay_method,
      coalesce(a.frozen_target_pay_method, coalesce(a.pay_channel, 'PAYE')) as frozen_target_pay_method,
      a.frozen_resolution_mode,
      a.frozen_resolution_payload_json,
      jsonb_strip_nulls(
        coalesce(a.frozen_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'target_amount_ex_vat', round(-a.take_ex, 2),
          'target_amount_vat', 0,
          'target_amount_inc_vat', round(-a.take_ex, 2)
        )
      ) as frozen_resolution_result_json,
      round(coalesce(a.take_ex, 0), 2)::numeric(12,2) as frozen_source_amount,
      round(-a.take_ex, 2)::numeric(12,2) as frozen_target_amount_ex_vat,
      0::numeric(12,2) as frozen_target_amount_vat,
      round(-a.take_ex, 2)::numeric(12,2) as frozen_target_amount_inc_vat,
      a.payout_instruction_snapshot_json
    from alloc a
    where a.take_ex > 0
    returning id, amount_ex_vat, amount_vat, amount_inc_vat
  )
  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  select
    io.id,
    'OVERPAYMENT_RECOVERY',
    null,
    'Overpayment recovery',
    null::numeric,
    null::numeric,
    io.amount_ex_vat,
    io.amount_vat,
    io.amount_inc_vat,
    '{}'::jsonb
  from ins_overpay io;

  get diagnostics v_ins_overpay_bd = row_count;

  select count(*)::int
  into v_ins_overpay
  from public.pay_batch_items pbi_ct
  where pbi_ct.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_ct.item_type = 'OVERPAYMENT_RECOVERY'
    and pbi_ct.is_voided = false
    and pbi_ct.repayment_week_start = v_week_start;

  with ins_loan as (
    insert into public.pay_batch_items (
      id,
      pay_batch_candidate_id,
      item_type,
      timesheet_id,
      segment_key,
      source_ref,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      repayment_week_start,
      pay_channel,
      umbrella_id,
      is_mismatch,
      is_voided,
      created_at,
      updated_at,
      finance_case_id,
      reservation_id,
      paye_treatment,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json
    )
    with cand_scope as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        pbc.awaiting_net_amount,
        greatest(coalesce(ni.net_amount, 0), 0)::numeric(12,2) as paye_net_amount
      from public.pay_batch_candidates pbc
      join _tmp_paye_scope sc
        on sc.pay_batch_candidate_id = pbc.id
      left join lateral (
        select pni.net_amount
        from public.pay_batch_paye_net_inputs pni
        where pni.pay_batch_candidate_id = pbc.id
        order by pni.imported_at_utc desc
        limit 1
      ) ni on true
    ),
    overpay_current as (
      select
        pbi.pay_batch_candidate_id,
        round(sum(-pbi.amount_ex_vat), 2)::numeric(12,2) as overpayment_recovery_taken_ex
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
        and pbi.is_voided = false
        and pbi.item_type = 'OVERPAYMENT_RECOVERY'
        and upper(coalesce(pbi.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
        and pbi.repayment_week_start = v_week_start
      group by pbi.pay_batch_candidate_id
    ),
    case_templates as (
      select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
        rt.*
      from _tmp_recovery_templates rt
      where rt.item_type = 'LOAN_REPAYMENT'
        and rt.finance_case_id is not null
        and upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
      order by
        rt.pay_batch_candidate_id,
        rt.finance_case_id,
        coalesce(rt.finance_component_id::text, ''),
        coalesce(rt.source_ref, ''),
        rt.sort_order,
        rt.template_sort_at,
        rt.finance_component_id nulls last,
        rt.source_ref nulls last
    ),
    loan_seed as (
      select
        cs.pay_batch_candidate_id,
        cs.candidate_id,
        ct.finance_case_id,
        ct.source_ref,
        ct.pay_channel,
        ct.umbrella_id,
        ct.is_mismatch,
        ct.paye_treatment,
        ct.finance_component_id,
        ct.frozen_component_snapshot_json,
        ct.frozen_component_key_type,
        ct.frozen_component_key_value,
        ct.frozen_component_classification,
        ct.frozen_source_basis_json,
        ct.frozen_source_pay_method,
        ct.frozen_target_pay_method,
        ct.frozen_resolution_mode,
        ct.frozen_resolution_payload_json,
        ct.frozen_resolution_result_json,
        ct.frozen_source_amount,
        ct.frozen_target_amount_ex_vat,
        ct.frozen_target_amount_vat,
        ct.frozen_target_amount_inc_vat,
        ct.payout_instruction_snapshot_json,
        ct.frozen_remaining_source_amount,
        ct.sort_order,
        ct.template_sort_at,
        round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2)::numeric(12,2) as nominal_due_amount,
        ct.minimum_earnings_threshold,
        ct.take_home_floor_override,
        round(greatest(cs.paye_net_amount - coalesce(oc.overpayment_recovery_taken_ex, 0), 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
        round(greatest(cs.paye_net_amount - coalesce(oc.overpayment_recovery_taken_ex, 0), 0), 2)::numeric(12,2) as run_take_home_before_ex
      from cand_scope cs
      join case_templates ct
        on ct.pay_batch_candidate_id = cs.pay_batch_candidate_id
      left join overpay_current oc
        on oc.pay_batch_candidate_id = cs.pay_batch_candidate_id
      where cs.awaiting_net_amount = false
        and ct.frozen_remaining_source_amount > 0
        and upper(coalesce(ct.frozen_case_type, 'PAYMENT_ADVANCE')) = 'PAYMENT_ADVANCE'
        and upper(coalesce(ct.frozen_payout_status, 'PAID')) = 'PAID'
        and (ct.frozen_next_due_week_start is null or ct.frozen_next_due_week_start <= v_week_start)
        and round(greatest(least(coalesce(ct.frozen_weekly_due_amount, ct.frozen_remaining_source_amount, 0::numeric(12,2)), ct.frozen_remaining_source_amount), 0), 2) > 0
    ),
    loan_inputs as (
      select
        ls.pay_batch_candidate_id,
        ls.candidate_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'sort_order', ls.sort_order,
              'finance_case_id', ls.finance_case_id::text,
              'case_type', 'PAYMENT_ADVANCE',
              'payout_status', 'PAID',
              'nominal_due_amount', ls.nominal_due_amount,
              'minimum_earnings_threshold', ls.minimum_earnings_threshold,
              'take_home_floor_override', ls.take_home_floor_override
            )
            order by ls.sort_order, ls.template_sort_at, ls.finance_case_id
          ),
          '[]'::jsonb
        ) as recovery_rows_json,
        max(ls.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
        max(ls.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex
      from loan_seed ls
      group by ls.pay_batch_candidate_id, ls.candidate_id
    ),
    loan_alloc as (
      select
        li.pay_batch_candidate_id,
        li.candidate_id,
        lra.finance_case_id,
        round(coalesce(lra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
      from loan_inputs li
      cross join lateral public._pay_finance_protected_recovery_allocate(
        li.recovery_rows_json,
        li.run_earnings_headroom_ex,
        li.run_take_home_before_ex,
        null::numeric
      ) lra
    )
    select
      gen_random_uuid() as id,
      ct.pay_batch_candidate_id,
      'LOAN_REPAYMENT' as item_type,
      null::uuid as timesheet_id,
      null::text as segment_key,
      coalesce(ct.source_ref, 'advance:' || la.finance_case_id::text) as source_ref,
      round(-la.take_ex, 2)::numeric(12,2) as amount_ex_vat,
      0::numeric(12,2) as amount_vat,
      round(-la.take_ex, 2)::numeric(12,2) as amount_inc_vat,
      v_week_start as repayment_week_start,
      coalesce(ct.pay_channel, 'PAYE') as pay_channel,
      ct.umbrella_id,
      coalesce(ct.is_mismatch, false) as is_mismatch,
      false as is_voided,
      now() as created_at,
      now() as updated_at,
      la.finance_case_id,
      null::uuid as reservation_id,
      coalesce(ct.paye_treatment, 'NET_DEDUCT') as paye_treatment,
      ct.finance_component_id,
      ct.frozen_component_snapshot_json,
      ct.frozen_component_key_type,
      ct.frozen_component_key_value,
      ct.frozen_component_classification,
      ct.frozen_source_basis_json,
      ct.frozen_source_pay_method,
      coalesce(ct.frozen_target_pay_method, coalesce(ct.pay_channel, 'PAYE')) as frozen_target_pay_method,
      ct.frozen_resolution_mode,
      ct.frozen_resolution_payload_json,
      jsonb_strip_nulls(
        coalesce(ct.frozen_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'target_amount_ex_vat', round(-la.take_ex, 2),
          'target_amount_vat', 0,
          'target_amount_inc_vat', round(-la.take_ex, 2)
        )
      ) as frozen_resolution_result_json,
      round(coalesce(la.take_ex, 0), 2)::numeric(12,2) as frozen_source_amount,
      round(-la.take_ex, 2)::numeric(12,2) as frozen_target_amount_ex_vat,
      0::numeric(12,2) as frozen_target_amount_vat,
      round(-la.take_ex, 2)::numeric(12,2) as frozen_target_amount_inc_vat,
      ct.payout_instruction_snapshot_json
    from loan_alloc la
    join case_templates ct
      on ct.pay_batch_candidate_id = la.pay_batch_candidate_id
     and ct.finance_case_id = la.finance_case_id
    where round(coalesce(la.take_ex, 0), 2) > 0
    returning id, amount_ex_vat, amount_vat, amount_inc_vat
  )
  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  select
    il.id,
    'LOAN_REPAYMENT',
    null,
    'Loan repayment',
    null::numeric,
    null::numeric,
    il.amount_ex_vat,
    il.amount_vat,
    il.amount_inc_vat,
    '{}'::jsonb
  from ins_loan il;

  get diagnostics v_ins_loan_bd = row_count;

  select count(*)::int
  into v_ins_loan
  from public.pay_batch_items pbi_ct2
  where pbi_ct2.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_ct2.item_type = 'LOAN_REPAYMENT'
    and pbi_ct2.is_voided = false
    and pbi_ct2.repayment_week_start = v_week_start;

  with manual_debt_scope as (
    select
      pbc_md.id as pay_batch_candidate_id,
      pbc_md.candidate_id,
      round(greatest(coalesce(pbc_md.gross_preview, 0), 0), 2)::numeric(12,2) as run_earnings_headroom_ex,
      round(
        greatest(
          coalesce(pni_md.net_amount, 0)
          - coalesce((
              select round(sum(-pbi_ov.amount_ex_vat), 2)
              from public.pay_batch_items pbi_ov
              where pbi_ov.pay_batch_candidate_id = pbc_md.id
                and pbi_ov.is_voided = false
                and pbi_ov.item_type = 'OVERPAYMENT_RECOVERY'
                and upper(coalesce(pbi_ov.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
                and pbi_ov.repayment_week_start = v_week_start
            ), 0)
          - coalesce((
              select round(sum(-pbi_ln.amount_ex_vat), 2)
              from public.pay_batch_items pbi_ln
              where pbi_ln.pay_batch_candidate_id = pbc_md.id
                and pbi_ln.is_voided = false
                and pbi_ln.item_type = 'LOAN_REPAYMENT'
                and upper(coalesce(pbi_ln.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
                and pbi_ln.repayment_week_start = v_week_start
            ), 0),
          0
        ),
        2
      )::numeric(12,2) as run_take_home_before_ex,
      round(greatest(coalesce((
        select max(rt.default_take_home_floor)
        from _tmp_recovery_templates rt
        where rt.pay_batch_candidate_id = pbc_md.id
          and rt.item_type = 'MANUAL_DEBT_RECOVERY'
      ), 0), 0), 2)::numeric(12,2) as default_take_home_floor
    from public.pay_batch_candidates pbc_md
    join _tmp_paye_scope sc_md
      on sc_md.pay_batch_candidate_id = pbc_md.id
    left join lateral (
      select pni_md_inner.net_amount
      from public.pay_batch_paye_net_inputs pni_md_inner
      where pni_md_inner.pay_batch_candidate_id = pbc_md.id
      order by pni_md_inner.imported_at_utc desc
      limit 1
    ) pni_md on true
    where pbc_md.awaiting_net_amount = false
  ),
  manual_debt_case_templates as (
    select distinct on (rt.pay_batch_candidate_id, rt.finance_case_id, coalesce(rt.finance_component_id::text, ''), coalesce(rt.source_ref, ''))
      rt.*
    from _tmp_recovery_templates rt
    where rt.item_type = 'MANUAL_DEBT_RECOVERY'
      and rt.finance_case_id is not null
      and upper(coalesce(rt.paye_treatment, 'NET_DEDUCT')) not in ('GROSS_DEDUCT', 'GROSS_DEDUCTION', 'DEDUCT_GROSS')
    order by
      rt.pay_batch_candidate_id,
      rt.finance_case_id,
      coalesce(rt.finance_component_id::text, ''),
      coalesce(rt.source_ref, ''),
      rt.sort_order,
      rt.template_sort_at,
      rt.finance_component_id nulls last,
      rt.source_ref nulls last
  ),
  manual_debt_template_values as (
    select
      mds.pay_batch_candidate_id,
      mds.candidate_id,
      mdt.finance_case_id,
      mdt.template_sort_at,
      mds.run_earnings_headroom_ex,
      mds.run_take_home_before_ex,
      nullif(mds.default_take_home_floor, 0)::numeric(12,2) as default_take_home_floor,
      upper(coalesce(mdt.frozen_case_type, 'MANUAL_DEBT_ADJUSTMENT')) as frozen_case_type,
      mdt.frozen_next_due_week_start,
      mdt.minimum_earnings_threshold,
      mdt.take_home_floor_override,
      mdt.frozen_weekly_due_amount,
      mdt.frozen_remaining_source_amount,
      mdt.sort_order,
      sched_md.scheduled_due_amount,
      mdt.source_ref,
      mdt.pay_channel,
      mdt.umbrella_id,
      mdt.is_mismatch,
      mdt.paye_treatment,
      mdt.finance_component_id,
      mdt.frozen_component_snapshot_json,
      mdt.frozen_component_key_type,
      mdt.frozen_component_key_value,
      mdt.frozen_component_classification,
      mdt.frozen_source_basis_json,
      mdt.frozen_source_pay_method,
      mdt.frozen_target_pay_method,
      mdt.frozen_resolution_mode,
      mdt.frozen_resolution_payload_json,
      mdt.frozen_resolution_result_json,
      mdt.frozen_source_amount,
      mdt.frozen_target_amount_ex_vat,
      mdt.frozen_target_amount_vat,
      mdt.frozen_target_amount_inc_vat,
      mdt.payout_instruction_snapshot_json
    from manual_debt_scope mds
    join manual_debt_case_templates mdt
      on mdt.pay_batch_candidate_id = mds.pay_batch_candidate_id
    left join lateral (
      select
        round(greatest(abs((sched_item.value->>'amount')::numeric), 0), 2)::numeric(12,2) as scheduled_due_amount
      from jsonb_array_elements(
        case
          when jsonb_typeof(coalesce(mdt.frozen_source_basis_json->'schedule_json', '[]'::jsonb)) = 'array'
            then coalesce(mdt.frozen_source_basis_json->'schedule_json', '[]'::jsonb)
          else '[]'::jsonb
        end
      ) as sched_item(value)
      where nullif(btrim(sched_item.value->>'week_start'), '') is not null
        and (sched_item.value->>'week_start')::date = v_week_start
      limit 1
    ) sched_md on true
  ),
  manual_debt_seed as (
    select
      mdtv.pay_batch_candidate_id,
      mdtv.candidate_id,
      mdtv.finance_case_id,
      mdtv.template_sort_at,
      mdtv.sort_order,
      round(
        greatest(
          least(
            coalesce(mdtv.scheduled_due_amount, mdtv.frozen_weekly_due_amount, 0::numeric(12,2)),
            mdtv.frozen_remaining_source_amount
          ),
          0
        ),
        2
      )::numeric(12,2) as nominal_due_amount,
      mdtv.minimum_earnings_threshold,
      mdtv.take_home_floor_override,
      mdtv.run_earnings_headroom_ex,
      mdtv.run_take_home_before_ex,
      mdtv.default_take_home_floor
    from manual_debt_template_values mdtv
    where mdtv.frozen_case_type = 'MANUAL_DEBT_ADJUSTMENT'
      and mdtv.frozen_remaining_source_amount > 0
      and (mdtv.frozen_next_due_week_start is null or mdtv.frozen_next_due_week_start <= v_week_start)
      and round(
        greatest(
          least(
            coalesce(mdtv.scheduled_due_amount, mdtv.frozen_weekly_due_amount, 0::numeric(12,2)),
            mdtv.frozen_remaining_source_amount
          ),
          0
        ),
        2
      ) > 0
  ),
  manual_debt_inputs as (
    select
      mds.candidate_id,
      mds.pay_batch_candidate_id,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'sort_order', mds.sort_order,
            'finance_case_id', mds.finance_case_id::text,
            'case_type', 'MANUAL_DEBT_ADJUSTMENT',
            'payout_status', null,
            'nominal_due_amount', mds.nominal_due_amount,
            'minimum_earnings_threshold', mds.minimum_earnings_threshold,
            'take_home_floor_override', mds.take_home_floor_override
          )
          order by mds.sort_order, mds.template_sort_at, mds.finance_case_id
        ),
        '[]'::jsonb
      ) as recovery_rows_json,
      max(mds.run_earnings_headroom_ex)::numeric(12,2) as run_earnings_headroom_ex,
      max(mds.run_take_home_before_ex)::numeric(12,2) as run_take_home_before_ex,
      nullif(max(mds.default_take_home_floor), 0)::numeric(12,2) as default_take_home_floor
    from manual_debt_seed mds
    group by mds.candidate_id, mds.pay_batch_candidate_id
  ),
  manual_debt_alloc as (
    select
      mdi.pay_batch_candidate_id,
      mdi.candidate_id,
      mdra.finance_case_id,
      round(coalesce(mdra.protected_recoverable_amount, 0), 2)::numeric(12,2) as take_ex
    from manual_debt_inputs mdi
    cross join lateral public._pay_finance_protected_recovery_allocate(
      mdi.recovery_rows_json,
      mdi.run_earnings_headroom_ex,
      mdi.run_take_home_before_ex,
      mdi.default_take_home_floor
    ) mdra
  ),
  ins_manual_debt as (
    insert into public.pay_batch_items (
      id,
      pay_batch_candidate_id,
      item_type,
      timesheet_id,
      segment_key,
      source_ref,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      repayment_week_start,
      pay_channel,
      umbrella_id,
      is_mismatch,
      is_voided,
      created_at,
      updated_at,
      finance_case_id,
      reservation_id,
      paye_treatment,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json
    )
    select
      gen_random_uuid() as id,
      mdt.pay_batch_candidate_id,
      'MANUAL_DEBT_RECOVERY' as item_type,
      null::uuid as timesheet_id,
      null::text as segment_key,
      coalesce(mdt.source_ref, 'advance:' || mda.finance_case_id::text) as source_ref,
      round(-mda.take_ex, 2)::numeric(12,2) as amount_ex_vat,
      0::numeric(12,2) as amount_vat,
      round(-mda.take_ex, 2)::numeric(12,2) as amount_inc_vat,
      v_week_start as repayment_week_start,
      coalesce(mdt.pay_channel, 'PAYE') as pay_channel,
      mdt.umbrella_id,
      coalesce(mdt.is_mismatch, false) as is_mismatch,
      false as is_voided,
      now() as created_at,
      now() as updated_at,
      mda.finance_case_id,
      null::uuid as reservation_id,
      coalesce(mdt.paye_treatment, 'NET_DEDUCT') as paye_treatment,
      mdt.finance_component_id,
      mdt.frozen_component_snapshot_json,
      mdt.frozen_component_key_type,
      mdt.frozen_component_key_value,
      mdt.frozen_component_classification,
      mdt.frozen_source_basis_json,
      mdt.frozen_source_pay_method,
      coalesce(mdt.frozen_target_pay_method, coalesce(mdt.pay_channel, 'PAYE')) as frozen_target_pay_method,
      mdt.frozen_resolution_mode,
      mdt.frozen_resolution_payload_json,
      jsonb_strip_nulls(
        coalesce(mdt.frozen_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'target_amount_ex_vat', round(-mda.take_ex, 2),
          'target_amount_vat', 0,
          'target_amount_inc_vat', round(-mda.take_ex, 2)
        )
      ) as frozen_resolution_result_json,
      round(coalesce(mda.take_ex, 0), 2)::numeric(12,2) as frozen_source_amount,
      round(-mda.take_ex, 2)::numeric(12,2) as frozen_target_amount_ex_vat,
      0::numeric(12,2) as frozen_target_amount_vat,
      round(-mda.take_ex, 2)::numeric(12,2) as frozen_target_amount_inc_vat,
      mdt.payout_instruction_snapshot_json
    from manual_debt_alloc mda
    join manual_debt_case_templates mdt
      on mdt.pay_batch_candidate_id = mda.pay_batch_candidate_id
     and mdt.finance_case_id = mda.finance_case_id
    where round(coalesce(mda.take_ex, 0), 2) > 0
    returning id, amount_ex_vat, amount_vat, amount_inc_vat
  )
  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  select
    imd.id,
    'MANUAL_DEBT_RECOVERY',
    null,
    'Manual debt recovery',
    null::numeric,
    null::numeric,
    imd.amount_ex_vat,
    imd.amount_vat,
    imd.amount_inc_vat,
    '{}'::jsonb
  from ins_manual_debt imd;

  get diagnostics v_ins_manual_debt_bd = row_count;

  select count(*)::int
  into v_ins_manual_debt
  from public.pay_batch_items pbi_ct3
  where pbi_ct3.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_ct3.item_type = 'MANUAL_DEBT_RECOVERY'
    and pbi_ct3.is_voided = false
    and pbi_ct3.repayment_week_start = v_week_start;

  with ins_finance_reservations as (
    insert into public.pay_advance_reservations (
      id,
      finance_case_id,
      pay_batch_id,
      pay_batch_candidate_id,
      pay_batch_item_id,
      reserved_amount,
      repayment_week_start,
      status,
      created_at_utc,
      committed_at_utc,
      settled_at_utc,
      released_at_utc,
      released_reason,
      created_by_user_id,
      updated_by_user_id,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      reserved_source_amount,
      frozen_rounded_target_amount
    )
    select
      gen_random_uuid() as id,
      pbi_fin.finance_case_id,
      p_pay_batch_id as pay_batch_id,
      pbi_fin.pay_batch_candidate_id,
      pbi_fin.id as pay_batch_item_id,
      round(abs(coalesce(pbi_fin.amount_ex_vat, 0)), 2)::numeric(12,2) as reserved_amount,
      pbi_fin.repayment_week_start,
      'RESERVED' as status,
      now() as created_at_utc,
      null::timestamptz as committed_at_utc,
      null::timestamptz as settled_at_utc,
      null::timestamptz as released_at_utc,
      null::text as released_reason,
      p_actor_user_id as created_by_user_id,
      p_actor_user_id as updated_by_user_id,
      pbi_fin.finance_component_id,
      pbi_fin.frozen_component_snapshot_json,
      pbi_fin.frozen_component_key_type,
      pbi_fin.frozen_component_key_value,
      pbi_fin.frozen_component_classification,
      pbi_fin.frozen_source_basis_json,
      pbi_fin.frozen_source_pay_method,
      pbi_fin.frozen_target_pay_method,
      pbi_fin.frozen_resolution_mode,
      pbi_fin.frozen_resolution_payload_json,
      pbi_fin.frozen_resolution_result_json,
      round(abs(coalesce(pbi_fin.frozen_source_amount, pbi_fin.amount_ex_vat, 0)), 2)::numeric(12,2) as reserved_source_amount,
      round(abs(coalesce(pbi_fin.frozen_target_amount_ex_vat, pbi_fin.amount_ex_vat, 0)), 2)::numeric(12,2) as frozen_rounded_target_amount
    from public.pay_batch_items pbi_fin
    where pbi_fin.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
      and pbi_fin.is_voided = false
      and pbi_fin.finance_case_id is not null
      and pbi_fin.reservation_id is null
      and pbi_fin.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
    returning id, pay_batch_item_id
  ),
  upd_finance_items as (
    update public.pay_batch_items pbi_upd
    set
      reservation_id = ifr.id,
      updated_at = now()
    from ins_finance_reservations ifr
    where pbi_upd.id = ifr.pay_batch_item_id
    returning pbi_upd.id
  )
  select count(*)::int
  into v_ins_finance_reservations
  from upd_finance_items;
  update public.pay_batch_candidates pbc_sum
  set
    awaiting_net_amount = not exists (
      select 1
      from public.pay_batch_paye_net_inputs pni4
      where pni4.pay_batch_candidate_id = pbc_sum.id
      limit 1
    ),
    overpayment_recovery_taken = coalesce((
      select round(sum(-pbi_s.amount_ex_vat), 2)
      from public.pay_batch_items pbi_s
      where pbi_s.pay_batch_candidate_id = pbc_sum.id
        and pbi_s.is_voided = false
        and pbi_s.item_type = 'OVERPAYMENT_RECOVERY'
    ), 0)::numeric(12,2),
    loan_repayment_taken = coalesce((
      select round(sum(-pbi_s2.amount_ex_vat), 2)
      from public.pay_batch_items pbi_s2
      where pbi_s2.pay_batch_candidate_id = pbc_sum.id
        and pbi_s2.is_voided = false
        and pbi_s2.item_type = 'LOAN_REPAYMENT'
    ), 0)::numeric(12,2),
    net_bank_amount = case
      when not exists (
        select 1
        from public.pay_batch_paye_net_inputs pni5
        where pni5.pay_batch_candidate_id = pbc_sum.id
        limit 1
      ) then null
      else greatest(
        round(
          coalesce((
            select pni_last.net_amount
            from public.pay_batch_paye_net_inputs pni_last
            where pni_last.pay_batch_candidate_id = pbc_sum.id
            order by pni_last.imported_at_utc desc
            limit 1
          ), 0)
          +
          coalesce((
            select round(coalesce(sum(
              case
                when pbi_d.is_voided = false and coalesce(pbi_d.paye_treatment, '') = 'NET_ADD'
                  then pbi_d.amount_ex_vat
                when pbi_d.is_voided = false and coalesce(pbi_d.paye_treatment, '') = 'NET_DEDUCT'
                  then pbi_d.amount_ex_vat
                else 0
              end
            ),0),2)
            from public.pay_batch_items pbi_d
            where pbi_d.pay_batch_candidate_id = pbc_sum.id
          ), 0),
          2
        ),
        0
      )::numeric(12,2)
    end,
    updated_at = now()
  where pbc_sum.id in (select s.pay_batch_candidate_id from _tmp_paye_scope s);

  get diagnostics v_upd_candidates = row_count;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', pbc_ret.id::text,
        'candidate_id', pbc_ret.candidate_id::text,
        'paye_state', pbc_ret.paye_state,
        'awaiting_net_amount', pbc_ret.awaiting_net_amount,
        'paye_net_amount', pni_ret.net_amount,
        'overpayment_recovery_taken', coalesce(pbc_ret.overpayment_recovery_taken, 0)::numeric(12,2),
        'loan_repayment_taken', coalesce(pbc_ret.loan_repayment_taken, 0)::numeric(12,2),
        'loan_payout_net_additions', coalesce((
          select round(sum(coalesce(pbi_np.amount_ex_vat, 0)), 2)
          from public.pay_batch_items pbi_np
          where pbi_np.pay_batch_candidate_id = pbc_ret.id
            and pbi_np.is_voided = false
            and pbi_np.item_type = 'LOAN_PAYOUT'
            and coalesce(pbi_np.paye_treatment, '') = 'NET_ADD'
        ), 0)::numeric(12,2),
        'manual_credit_net_additions', coalesce((
          select round(sum(coalesce(pbi_mc.amount_ex_vat, 0)), 2)
          from public.pay_batch_items pbi_mc
          where pbi_mc.pay_batch_candidate_id = pbc_ret.id
            and pbi_mc.is_voided = false
            and pbi_mc.item_type = 'MANUAL_CREDIT_PAYOUT'
            and coalesce(pbi_mc.paye_treatment, '') = 'NET_ADD'
        ), 0)::numeric(12,2),
        'manual_debt_net_deductions', coalesce((
          select round(sum(-pbi_mdn.amount_ex_vat), 2)
          from public.pay_batch_items pbi_mdn
          where pbi_mdn.pay_batch_candidate_id = pbc_ret.id
            and pbi_mdn.is_voided = false
            and pbi_mdn.item_type = 'MANUAL_DEBT_RECOVERY'
            and coalesce(pbi_mdn.paye_treatment, '') = 'NET_DEDUCT'
        ), 0)::numeric(12,2),
        'net_additions_total', coalesce((
          select round(sum(coalesce(pbi_na.amount_ex_vat, 0)), 2)
          from public.pay_batch_items pbi_na
          where pbi_na.pay_batch_candidate_id = pbc_ret.id
            and pbi_na.is_voided = false
            and coalesce(pbi_na.paye_treatment, '') = 'NET_ADD'
        ), 0)::numeric(12,2),
        'net_deductions_total', coalesce((
          select round(sum(-pbi_nd0.amount_ex_vat), 2)
          from public.pay_batch_items pbi_nd0
          where pbi_nd0.pay_batch_candidate_id = pbc_ret.id
            and pbi_nd0.is_voided = false
            and coalesce(pbi_nd0.paye_treatment, '') = 'NET_DEDUCT'
        ), 0)::numeric(12,2),
        'net_bank_amount', pbc_ret.net_bank_amount,
        'deductions_summary', jsonb_build_object(
          'gross_positive', greatest(coalesce(pni_ret.net_amount, 0), 0)::numeric(12,2),
          'overpayment_recovery', coalesce(pbc_ret.overpayment_recovery_taken, 0)::numeric(12,2),
          'loan_repayment', coalesce(pbc_ret.loan_repayment_taken, 0)::numeric(12,2),
          'loan_payout_net_additions', coalesce((
            select round(sum(coalesce(pbi_np2.amount_ex_vat, 0)), 2)
            from public.pay_batch_items pbi_np2
            where pbi_np2.pay_batch_candidate_id = pbc_ret.id
              and pbi_np2.is_voided = false
              and pbi_np2.item_type = 'LOAN_PAYOUT'
              and coalesce(pbi_np2.paye_treatment, '') = 'NET_ADD'
          ), 0)::numeric(12,2),
          'manual_credit_net_additions', coalesce((
            select round(sum(coalesce(pbi_mc2.amount_ex_vat, 0)), 2)
            from public.pay_batch_items pbi_mc2
            where pbi_mc2.pay_batch_candidate_id = pbc_ret.id
              and pbi_mc2.is_voided = false
              and pbi_mc2.item_type = 'MANUAL_CREDIT_PAYOUT'
              and coalesce(pbi_mc2.paye_treatment, '') = 'NET_ADD'
          ), 0)::numeric(12,2),
          'manual_debt_net_deductions', coalesce((
            select round(sum(-pbi_mdn2.amount_ex_vat), 2)
            from public.pay_batch_items pbi_mdn2
            where pbi_mdn2.pay_batch_candidate_id = pbc_ret.id
              and pbi_mdn2.is_voided = false
              and pbi_mdn2.item_type = 'MANUAL_DEBT_RECOVERY'
              and coalesce(pbi_mdn2.paye_treatment, '') = 'NET_DEDUCT'
          ), 0)::numeric(12,2),
          'net_additions_total', coalesce((
            select round(sum(coalesce(pbi_na2.amount_ex_vat, 0)), 2)
            from public.pay_batch_items pbi_na2
            where pbi_na2.pay_batch_candidate_id = pbc_ret.id
              and pbi_na2.is_voided = false
              and coalesce(pbi_na2.paye_treatment, '') = 'NET_ADD'
          ), 0)::numeric(12,2),
          'net_deductions_total', coalesce((
            select round(sum(-pbi_nd.amount_ex_vat), 2)
            from public.pay_batch_items pbi_nd
            where pbi_nd.pay_batch_candidate_id = pbc_ret.id
              and pbi_nd.is_voided = false
              and coalesce(pbi_nd.paye_treatment, '') = 'NET_DEDUCT'
          ), 0)::numeric(12,2),
          'final_payable', pbc_ret.net_bank_amount,
          'awaiting_net_amount', pbc_ret.awaiting_net_amount,
          'paye_net_amount', pni_ret.net_amount
        )
      )
      order by pbc_ret.candidate_id::text
    ),
    '[]'::jsonb
  )
  into v_candidate_summaries
  from public.pay_batch_candidates pbc_ret
  left join lateral (
    select pni_ret_inner.net_amount
    from public.pay_batch_paye_net_inputs pni_ret_inner
    where pni_ret_inner.pay_batch_candidate_id = pbc_ret.id
    order by pni_ret_inner.imported_at_utc desc
    limit 1
  ) pni_ret on true
  where pbc_ret.id in (select s.pay_batch_candidate_id from _tmp_paye_scope s);

  return jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'source', 'MANUAL_ENTRY',
    'updated_count', v_updated_count,
    'cleared_count', v_cleared_count,
    'unmatched', '[]'::jsonb,
    'candidate_summaries', v_candidate_summaries,
    'recompute', jsonb_build_object(
      'deleted_deduction_items', v_deleted_ded_items,
      'deleted_deduction_breakdowns', v_deleted_ded_breakdowns,
      'deleted_manual_debt_breakdowns', v_deleted_manual_debt_breakdowns,
      'deleted_net_deduct_reservations', v_deleted_ded_reservations,
      'released_manual_debt_reservations', v_released_manual_debt_reservations,
      'voided_manual_debt_items', v_voided_manual_debt_items,
      'inserted_overpayment_items', v_ins_overpay,
      'inserted_overpayment_breakdowns', v_ins_overpay_bd,
      'inserted_loan_items', v_ins_loan,
      'inserted_loan_breakdowns', v_ins_loan_bd,
      'inserted_manual_debt_items', v_ins_manual_debt,
      'inserted_manual_debt_breakdowns', v_ins_manual_debt_bd,
      'inserted_finance_reservations', v_ins_finance_reservations,
      'updated_candidates', v_upd_candidates
    )
  );
end;
$$;
