-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 0d17bc1c53fc.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.pay_set_paye_net_from_sage(
  p_pay_batch_id uuid,
  p_csv_raw text,
  p_actor_user_id uuid,
  p_source_filename text
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_lines text[];
  v_line text;
  v_fields text[];

  header_idx int := 0;
  i int := 0;

  works_col int := 0;
  net_col int := 0;

  works text;
  works_norm text;
  net_raw text;
  net_amt numeric;

  dup_check jsonb := '[]'::jsonb;

  v_matched jsonb := '[]'::jsonb;
  v_unknown jsonb := '[]'::jsonb;
  v_ambig jsonb := '[]'::jsonb;
  v_missing_net jsonb := '[]'::jsonb;

  v_applied_count int := 0;

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
  v_ins_loan_res int := 0;
  v_upd_candidates int := 0;
  v_candidate_summaries jsonb := '[]'::jsonb;
begin
  perform public._ctms_assert_pay_batch_mutable_v1(p_pay_batch_id,'PAY_SET_PAYE_NET_FROM_SAGE');
  if p_pay_batch_id is null then
    raise exception 'pay_batch_id is required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id is required';
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
      'error', 'PAY_SET_PAYE_NET_SAGE',
      'code', 'PAYE_NET_MUTATION_BLOCKED_BY_LIFECYCLE',
      'message', 'pay_set_paye_net_from_sage: PAYE net cannot be changed after the payment batch has crossed the execution/cancellation lifecycle boundary',
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
      'error', 'PAY_SET_PAYE_NET_SAGE',
      'code', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'FRESHNESS_REQUIRES_CHUNKED_VALIDATION' else 'BATCH_STALE' end,
      'message', case when coalesce((v_fresh->>'requires_chunked_freshness')::boolean, false) then 'pay_set_paye_net_from_sage: chunked freshness validation is required before changing PAYE net' else 'pay_set_paye_net_from_sage: batch is stale; regenerate draft before importing PAYE net' end,
      'pay_batch_id', p_pay_batch_id::text,
      'stale_reasons', v_stale_reasons,
      'diff', v_diff_sample
    )::text;
  end if;

  v_lines := regexp_split_to_array(coalesce(p_csv_raw,''), E'\\r?\\n');

  for i in 1..coalesce(array_length(v_lines,1),0) loop
    v_line := coalesce(v_lines[i],'');
    if btrim(v_line) = '' then continue; end if;

    v_fields := public._pay_csv_parse_line(v_line);

    works_col := 0;
    net_col := 0;

    for header_idx in 1..coalesce(array_length(v_fields,1),0) loop
      if lower(btrim(coalesce(v_fields[header_idx],''))) = lower('Works Number') then
        works_col := header_idx;
      end if;
      if lower(btrim(coalesce(v_fields[header_idx],''))) = lower('Net Pay') then
        net_col := header_idx;
      end if;
    end loop;

    if works_col > 0 and net_col > 0 then
      header_idx := i;
      exit;
    end if;
  end loop;

  if header_idx = 0 then
    raise exception 'SAGE_IMPORT_INVALID: header row with Works Number and Net Pay not found';
  end if;

  create temp table if not exists _tmp_sage_net (
    works_number text,
    works_norm text,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_sage_net;

  for i in (header_idx+1)..coalesce(array_length(v_lines,1),0) loop
    v_line := coalesce(v_lines[i],'');
    if btrim(v_line) = '' then continue; end if;

    v_fields := public._pay_csv_parse_line(v_line);

    works := public._pay_csv_trim_field(case when works_col <= array_length(v_fields,1) then v_fields[works_col] else null end);
    if works is null then
      continue;
    end if;

    if lower(works) = 'totals' then
      continue;
    end if;

    works_norm := upper(regexp_replace(btrim(coalesce(works,'')), '\s+', '', 'g'));
    if works_norm = '' then
      continue;
    end if;

    net_raw := public._pay_csv_trim_field(case when net_col <= array_length(v_fields,1) then v_fields[net_col] else null end);

    if net_raw is null or btrim(net_raw) = '' then
      insert into _tmp_sage_net(works_number, works_norm, net_amount)
      values (works, works_norm, null);
      continue;
    end if;

    net_raw := replace(net_raw, ',', '');
    net_raw := regexp_replace(net_raw, '[^0-9\\.-]', '', 'g');

    if net_raw is null or btrim(net_raw) = '' then
      insert into _tmp_sage_net(works_number, works_norm, net_amount)
      values (works, works_norm, null);
      continue;
    end if;

    begin
      net_amt := net_raw::numeric;
    exception when others then
      raise exception 'SAGE_IMPORT_INVALID: Net Pay not numeric for Works Number %', works;
    end;

    if net_amt < 0 then
      raise exception 'SAGE_IMPORT_INVALID: Net Pay negative for Works Number %', works;
    end if;

    insert into _tmp_sage_net(works_number, works_norm, net_amount)
    values (works, works_norm, round(net_amt,2));
  end loop;

  select coalesce(jsonb_agg(t.works_norm), '[]'::jsonb)
  into dup_check
  from (
    select sn.works_norm
    from _tmp_sage_net sn
    group by sn.works_norm
    having count(*) > 1
  ) t;

  if jsonb_array_length(dup_check) > 0 then
    raise exception 'SAGE_IMPORT_INVALID: duplicate Works Number(s) %', dup_check::text;
  end if;

  create temp table if not exists _tmp_batch_paye (
    candidate_id uuid,
    pay_batch_candidate_id uuid,
    tms_ref_norm text,
    ni_norm text
  ) on commit drop;

  truncate table _tmp_batch_paye;

  insert into _tmp_batch_paye(candidate_id, pay_batch_candidate_id, tms_ref_norm, ni_norm)
  select
    pbc.candidate_id,
    pbc.id,
    upper(regexp_replace(btrim(coalesce(pbc.candidate_tms_ref, c.tms_ref, '')), '\s+', '', 'g')) as tms_ref_norm,
    upper(regexp_replace(btrim(coalesce(c.ni_number, '')), '\s+', '', 'g')) as ni_norm
  from public.pay_batch_candidates pbc
  join public.candidates c on c.id = pbc.candidate_id
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
      sn.works_norm,
      jsonb_agg(distinct bp.candidate_id::text order by bp.candidate_id::text) as candidate_ids
    from _tmp_sage_net sn
    join _tmp_batch_paye bp
      on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    group by sn.works_norm
    having count(distinct bp.candidate_id) > 1
  ) a;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', sn.works_number,
        'works_norm', sn.works_norm
      )
    ),
    '[]'::jsonb
  )
  into v_unknown
  from _tmp_sage_net sn
  left join _tmp_batch_paye bp
    on (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
    or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
  where bp.candidate_id is null;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'works_number', sn.works_number,
        'works_norm', sn.works_norm,
        'candidate_id', bp.candidate_id::text,
        'pay_batch_candidate_id', bp.pay_batch_candidate_id::text
      )
    ),
    '[]'::jsonb
  )
  into v_missing_net
  from _tmp_sage_net sn
  join _tmp_batch_paye bp
    on (
      (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    )
  where sn.net_amount is null
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_ambig,'[]'::jsonb)) a
      where (a->>'works_number') = sn.works_norm
      limit 1
    );

  create temp table if not exists _tmp_sage_match (
    pay_batch_candidate_id uuid,
    net_amount numeric
  ) on commit drop;

  truncate table _tmp_sage_match;

  insert into _tmp_sage_match(pay_batch_candidate_id, net_amount)
  select
    bp.pay_batch_candidate_id,
    sn.net_amount
  from _tmp_sage_net sn
  join _tmp_batch_paye bp
    on (
      (bp.tms_ref_norm is not null and bp.tms_ref_norm <> '' and bp.tms_ref_norm = sn.works_norm)
      or (bp.ni_norm is not null and bp.ni_norm <> '' and bp.ni_norm = sn.works_norm)
    )
  where sn.net_amount is not null
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_ambig,'[]'::jsonb)) a
      where (a->>'works_number') = sn.works_norm
      limit 1
    )
    and not exists (
      select 1
      from jsonb_array_elements(coalesce(v_unknown,'[]'::jsonb)) u
      where (u->>'works_norm') = sn.works_norm
      limit 1
    );

  select count(*)::int
  into v_applied_count
  from _tmp_sage_match sm;

  delete from public.pay_batch_paye_net_inputs pni
  using _tmp_sage_match sm
  where pni.pay_batch_candidate_id = sm.pay_batch_candidate_id
    and pni.source = 'SAGE_IMPORT';

  insert into public.pay_batch_paye_net_inputs(
    pay_batch_candidate_id, source, net_amount, imported_at_utc, file_name, file_hash
  )
  select
    sm.pay_batch_candidate_id,
    'SAGE_IMPORT',
    sm.net_amount,
    now(),
    p_source_filename,
    null
  from _tmp_sage_match sm;

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

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'pay_batch_candidate_id', bp.pay_batch_candidate_id::text,
        'candidate_id', bp.candidate_id::text,
        'net_amount', sm.net_amount
      )
      order by bp.candidate_id::text
    ),
    '[]'::jsonb
  )
  into v_matched
  from _tmp_sage_match sm
  join _tmp_batch_paye bp
    on bp.pay_batch_candidate_id = sm.pay_batch_candidate_id;

  ---------------------------------------------------------------------------
  -- ✅ Recompute NET_DEDUCT items + net_bank_amount (PAYE candidates only)
  -- Gross-side recovery/addition items remain as created at draft time.
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

  create temp table if not exists _tmp_ded_item_ids (
    id uuid primary key
  ) on commit drop;

  truncate table _tmp_ded_item_ids;

  insert into _tmp_ded_item_ids(id)
  select
    pbi_del.id
  from public.pay_batch_items pbi_del
  where pbi_del.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_del.item_type = 'LOAN_REPAYMENT';

  delete from public.pay_batch_item_breakdowns pbib_del
  using _tmp_ded_item_ids di
  where pbib_del.pay_batch_item_id = di.id;

  get diagnostics v_deleted_ded_breakdowns = row_count;

  delete from public.pay_advance_reservations par_del
  using _tmp_ded_item_ids di_res
  where par_del.pay_batch_item_id = di_res.id;

  get diagnostics v_deleted_ded_reservations = row_count;

  delete from public.pay_batch_items pbi_del2
  using _tmp_ded_item_ids di2
  where pbi_del2.id = di2.id;

  get diagnostics v_deleted_ded_items = row_count;

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

  with ins_loan_items as (
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
      finance_case_id,
      reservation_id,
      paye_treatment,
      created_at,
      updated_at
    )
    with
    cand_scope as (
      select
        pbc.id as pay_batch_candidate_id,
        pbc.candidate_id,
        'PAYE'::text as pay_channel,
        null::uuid as umbrella_id,
        pbc.awaiting_net_amount,
        ni.net_amount::numeric(12,2) as paye_net_amount
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
    cand_limits as (
      select
        cs.pay_batch_candidate_id,
        cs.candidate_id,
        cs.pay_channel,
        cs.umbrella_id,
        greatest(coalesce(cs.paye_net_amount, 0), 0)::numeric(12,2) as earnings_before_payment_advance_ex
      from cand_scope cs
      where cs.awaiting_net_amount = false
    ),
    paid_wtd as (
      select
        pbc2.candidate_id,
        round(sum(
          case
            when pbc2.net_bank_amount is not null then pbc2.net_bank_amount
            when pbc2.gross_preview is not null then pbc2.gross_preview
            else 0
          end
        ), 2)::numeric(12,2) as paid_wtd_before_ex
      from public.pay_batch_candidates pbc2
      join public.pay_batches pb2
        on pb2.id = pbc2.pay_batch_id
      where pb2.cancelled_at_utc is null
        and pb2.id <> p_pay_batch_id
        and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
        and pb2.pay_date >= v_week_start
        and pb2.pay_date < (v_week_start + 7)
      group by pbc2.candidate_id
    ),
    cand_with_floor as (
      select
        cl.pay_batch_candidate_id,
        cl.candidate_id,
        cl.pay_channel,
        cl.umbrella_id,
        cl.earnings_before_payment_advance_ex,
        coalesce(pw.paid_wtd_before_ex, 0)::numeric(12,2) as paid_wtd_before_ex,
        coalesce(c.min_take_home_wtd, 0)::numeric(12,2) as floor_ex,
        round(
          greatest(
            least(
              cl.earnings_before_payment_advance_ex,
              (coalesce(pw.paid_wtd_before_ex, 0) + cl.earnings_before_payment_advance_ex) - coalesce(c.min_take_home_wtd, 0)
            ),
            0
          ),
          2
        )::numeric(12,2) as max_payment_advance_repayment_ex
      from cand_limits cl
      join public.candidates c
        on c.id = cl.candidate_id
      left join paid_wtd pw
        on pw.candidate_id = cl.candidate_id
      where cl.earnings_before_payment_advance_ex > 0
    ),
    payment_advances as (
      select
        pa.id as finance_case_id,
        pa.candidate_id,
        pa.outstanding_amount::numeric(12,2) as outstanding_amount,
        pa.weekly_due::numeric(12,2) as weekly_due,
        pa.start_week_start,
        pa.created_at
      from public.pay_advances pa
      where pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
        and pa.payout_status = 'PAID'::public.pay_advance_payout_status_enum
        and pa.status = 'ACTIVE'::public.pay_advance_status_enum
        and pa.outstanding_amount > 0
        and pa.weekly_due is not null
        and pa.weekly_due > 0
        and (pa.start_week_start is null or pa.start_week_start <= v_week_start)
    ),
    payment_advance_repaid_wtd as (
      select
        pbc2.candidate_id,
        replace(pbi2.source_ref, 'advance:', '')::uuid as finance_case_id,
        round(coalesce(sum(-pbi2.amount_ex_vat), 0), 2)::numeric(12,2) as repaid_wtd_ex
      from public.pay_batch_items pbi2
      join public.pay_batch_candidates pbc2
        on pbc2.id = pbi2.pay_batch_candidate_id
      join public.pay_batches pb2
        on pb2.id = pbc2.pay_batch_id
      where pbi2.item_type = 'LOAN_REPAYMENT'
        and pbi2.is_voided = false
        and pbi2.source_ref ~ '^advance:[0-9a-fA-F-]{36}$'
        and pbi2.repayment_week_start = v_week_start
        and pb2.cancelled_at_utc is null
        and pb2.id <> p_pay_batch_id
        and coalesce(pb2.batch_kind_fixed, '') <> 'LOANS'
      group by
        pbc2.candidate_id,
        replace(pbi2.source_ref, 'advance:', '')::uuid
    ),
    payment_advance_due as (
      select
        cwf.pay_batch_candidate_id,
        cwf.candidate_id,
        cwf.pay_channel,
        cwf.umbrella_id,
        cwf.max_payment_advance_repayment_ex,
        pa.finance_case_id,
        pa.outstanding_amount,
        pa.weekly_due,
        pa.start_week_start,
        pa.created_at,
        least(pa.weekly_due, pa.outstanding_amount)::numeric(12,2) as due_this_week_ex,
        greatest(
          least(pa.weekly_due, pa.outstanding_amount) - coalesce(parw.repaid_wtd_ex, 0),
          0
        )::numeric(12,2) as remaining_due_ex
      from cand_with_floor cwf
      join payment_advances pa
        on pa.candidate_id = cwf.candidate_id
      left join payment_advance_repaid_wtd parw
        on parw.candidate_id = cwf.candidate_id
       and parw.finance_case_id = pa.finance_case_id
      where cwf.max_payment_advance_repayment_ex > 0
        and greatest(
          least(pa.weekly_due, pa.outstanding_amount) - coalesce(parw.repaid_wtd_ex, 0),
          0
        ) > 0
    ),
    alloc_base as (
      select
        pad.candidate_id,
        pad.finance_case_id,
        pad.remaining_due_ex,
        pad.max_payment_advance_repayment_ex,
        sum(pad.remaining_due_ex) over (
          partition by pad.candidate_id
          order by pad.start_week_start nulls first, pad.created_at, pad.finance_case_id
          rows between unbounded preceding and 1 preceding
        )::numeric(12,2) as cum_before_ex
      from payment_advance_due pad
    ),
    alloc as (
      select
        pad2.pay_batch_candidate_id,
        pad2.pay_channel,
        pad2.umbrella_id,
        pad2.finance_case_id,
        least(
          pad2.remaining_due_ex,
          greatest(pad2.max_payment_advance_repayment_ex - coalesce(ab.cum_before_ex, 0), 0)
        )::numeric(12,2) as take_ex
      from payment_advance_due pad2
      join alloc_base ab
        on ab.finance_case_id = pad2.finance_case_id
    )
    select
      gen_random_uuid() as id,
      a.pay_batch_candidate_id,
      'LOAN_REPAYMENT' as item_type,
      null::uuid as timesheet_id,
      null::text as segment_key,
      ('advance:' || a.finance_case_id::text) as source_ref,
      (-a.take_ex)::numeric(12,2) as amount_ex_vat,
      (0)::numeric(12,2) as amount_vat,
      (-a.take_ex)::numeric(12,2) as amount_inc_vat,
      v_week_start as repayment_week_start,
      a.pay_channel as pay_channel,
      a.umbrella_id as umbrella_id,
      false as is_mismatch,
      false as is_voided,
      a.finance_case_id as finance_case_id,
      null::uuid as reservation_id,
      'NET_DEDUCT'::text as paye_treatment,
      now() as created_at,
      now() as updated_at
    from alloc a
    where a.take_ex > 0
    returning
      id,
      pay_batch_candidate_id,
      finance_case_id,
      repayment_week_start,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat
  ),
  ins_loan_reservations as (
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
      updated_by_user_id
    )
    select
      gen_random_uuid() as id,
      ili.finance_case_id,
      p_pay_batch_id,
      ili.pay_batch_candidate_id,
      ili.id as pay_batch_item_id,
      abs(ili.amount_ex_vat)::numeric(12,2) as reserved_amount,
      ili.repayment_week_start,
      'RESERVED' as status,
      now() as created_at_utc,
      null::timestamptz as committed_at_utc,
      null::timestamptz as settled_at_utc,
      null::timestamptz as released_at_utc,
      null::text as released_reason,
      p_actor_user_id as created_by_user_id,
      p_actor_user_id as updated_by_user_id
    from ins_loan_items ili
    returning id, pay_batch_item_id
  ),
  upd_loan_items as (
    update public.pay_batch_items pbi_upd
    set
      reservation_id = ilr.id,
      updated_at = now()
    from ins_loan_reservations ilr
    where pbi_upd.id = ilr.pay_batch_item_id
    returning pbi_upd.id
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
    ili.id,
    'LOAN_REPAYMENT',
    null,
    'Payment Advance repayment',
    null::numeric,
    null::numeric,
    ili.amount_ex_vat,
    ili.amount_vat,
    ili.amount_inc_vat,
    '{}'::jsonb
  from ins_loan_items ili;

  get diagnostics v_ins_loan_bd = row_count;

  select count(*)::int
  into v_ins_loan
  from public.pay_batch_items pbi_ct2
  where pbi_ct2.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_ct2.item_type = 'LOAN_REPAYMENT'
    and pbi_ct2.is_voided = false
    and pbi_ct2.repayment_week_start = v_week_start;

  select count(*)::int
  into v_ins_loan_res
  from public.pay_advance_reservations par_ct
  join public.pay_batch_items pbi_res
    on pbi_res.id = par_ct.pay_batch_item_id
  where par_ct.pay_batch_id = p_pay_batch_id
    and pbi_res.pay_batch_candidate_id in (select s.pay_batch_candidate_id from _tmp_paye_scope s)
    and pbi_res.item_type = 'LOAN_REPAYMENT'
    and pbi_res.is_voided = false
    and par_ct.status = 'RESERVED';

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
        'overpayment_recovery', coalesce(pbc_ret.overpayment_recovery_taken, 0)::numeric(12,2),
        'manual_debt_recovery', coalesce((
          select round(sum(-pbi_md.amount_ex_vat), 2)
          from public.pay_batch_items pbi_md
          where pbi_md.pay_batch_candidate_id = pbc_ret.id
            and pbi_md.is_voided = false
            and pbi_md.item_type = 'MANUAL_DEBT_RECOVERY'
        ), 0)::numeric(12,2),
        'loan_repayment', coalesce(pbc_ret.loan_repayment_taken, 0)::numeric(12,2),
        'payment_advance_repayment', coalesce(pbc_ret.loan_repayment_taken, 0)::numeric(12,2),
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
          'imported_paye_net', greatest(coalesce(pni_ret.net_amount, 0), 0)::numeric(12,2),
          'gross_side_overpayment_recovery', coalesce(pbc_ret.overpayment_recovery_taken, 0)::numeric(12,2),
          'gross_side_manual_debt_recovery', coalesce((
            select round(sum(-pbi_md2.amount_ex_vat), 2)
            from public.pay_batch_items pbi_md2
            where pbi_md2.pay_batch_candidate_id = pbc_ret.id
              and pbi_md2.is_voided = false
              and pbi_md2.item_type = 'MANUAL_DEBT_RECOVERY'
              and coalesce(pbi_md2.paye_treatment, '') = 'GROSS_DEDUCT'
          ), 0)::numeric(12,2),
          'payment_advance_repayment', coalesce(pbc_ret.loan_repayment_taken, 0)::numeric(12,2),
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
          'final_bank', pbc_ret.net_bank_amount,
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
    'source', 'SAGE_IMPORT',
    'applied_count', v_applied_count,
    'matched', v_matched,
    'missing_net', v_missing_net,
    'unknown', v_unknown,
    'ambiguous', v_ambig,
    'candidate_summaries', v_candidate_summaries,
    'recompute', jsonb_build_object(
      'deleted_deduction_items', v_deleted_ded_items,
      'deleted_deduction_breakdowns', v_deleted_ded_breakdowns,
      'deleted_net_deduct_reservations', v_deleted_ded_reservations,
      'inserted_overpayment_items', v_ins_overpay,
      'inserted_overpayment_breakdowns', v_ins_overpay_bd,
      'inserted_loan_items', v_ins_loan,
      'inserted_loan_breakdowns', v_ins_loan_bd,
      'inserted_loan_reservations', v_ins_loan_res,
      'updated_candidates', v_upd_candidates
    )
  );
end;
$$;
