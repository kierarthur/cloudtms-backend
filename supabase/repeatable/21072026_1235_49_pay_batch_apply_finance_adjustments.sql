-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: d7366616e4f6.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_batch_apply_finance_adjustments(
  p_pay_batch_id uuid,
  p_pay_channel_scope text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_vat_rate_pct numeric DEFAULT NULL::numeric,
  p_week_start date DEFAULT NULL::date,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_id uuid := p_pay_batch_id;
  v_scope text := upper(btrim(coalesce(p_pay_channel_scope, '')));
  v_vat_rate_pct numeric := p_vat_rate_pct;
  v_week_start date := p_week_start;
  v_now_utc timestamptz := now();
  v_stage text := NULL;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_client_filter_single uuid := NULL::uuid;
  v_rows_ins_loan_payout_items integer := 0;
  v_rows_ins_manual_credit_payout_items integer := 0;
  v_rows_ins_underpayment_payout_items integer := 0;
  v_rows_ins_overpayment_recovery_items integer := 0;
  v_rows_ins_operation_planned_overpayment_items integer := 0;
  v_rows_upd_candidates_overpayment_recovery_taken integer := 0;
  v_rows_ins_debt_items integer := 0;
  v_rows_upd_candidates_debt integer := 0;
  v_rows_ins_loan_items integer := 0;
  v_rows_upd_candidates_loan integer := 0;
  v_rows_staged_recovery_template_rows integer := 0;
  v_rows_staged_manual_debt_template_rows integer := 0;
  v_rows_staged_loan_template_rows integer := 0;
  v_rows_staged_overpayment_template_rows integer := 0;
  v_rows_staged_paye_manual_debt_template_rows integer := 0;
  v_rows_ins_dormant_recovery_template_items integer := 0;
  v_rows_ins_dormant_manual_debt_template_items integer := 0;
  v_rows_ins_dormant_loan_template_items integer := 0;
  v_rows_ins_dormant_overpayment_template_items integer := 0;
  v_rows_skipped_dormant_recovery_template_items integer := 0;
  v_rows_skipped_dormant_manual_debt_template_items integer := 0;
  v_rows_skipped_dormant_loan_template_items integer := 0;
  v_rows_skipped_dormant_overpayment_template_items integer := 0;
  v_missing_materialised_manual_debt_template_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_staged_manual_debt_template_rows_by_candidate jsonb := '{}'::jsonb;
  v_payout_instruction_freeze_rec record;
  v_operation_allocation_total integer := 0;
  v_operation_allocation_done integer := 0;
  v_operation_mismatch_details jsonb := '{}'::jsonb;
  v_operation_plan_mismatch_details jsonb := '{}'::jsonb;
  v_operation_plan_drift_details jsonb := '[]'::jsonb;
  v_canonical_provenance_mismatch_details jsonb := '[]'::jsonb;
BEGIN
  perform public._ctms_assert_pay_batch_mutable_v1(p_pay_batch_id,'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS');
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_operation_id IS NOT NULL OR p_candidate_scope_ids IS NOT NULL THEN
    IF p_operation_id IS NULL THEN
      RAISE EXCEPTION 'p_operation_id is required when p_candidate_scope_ids is supplied';
    END IF;

    IF p_candidate_scope_ids IS NULL OR jsonb_typeof(p_candidate_scope_ids) <> 'array' OR jsonb_array_length(p_candidate_scope_ids) = 0 THEN
      RAISE EXCEPTION 'p_candidate_scope_ids must be a non-empty JSON array when p_operation_id is supplied';
    END IF;

    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION 'p_actor_user_id is required when p_operation_id is supplied';
    END IF;

    PERFORM *
    FROM public.pay_batch_stage_operation_candidate_chunk_context(
      p_operation_id,
      p_pay_batch_id,
      p_candidate_scope_ids,
      p_actor_user_id
    );

    SELECT settings_context.vat_rate_pct, settings_context.pay_week_start
    INTO v_vat_rate_pct, v_week_start
    FROM pg_temp.tmp_pay_build_settings_context AS settings_context
    LIMIT 1;
  END IF;

  IF v_scope NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'pay_channel_scope must be PAYE or UMBRELLA';
  END IF;
  IF v_vat_rate_pct IS NULL THEN
    RAISE EXCEPTION 'vat_rate_pct is required';
  END IF;
  IF v_week_start IS NULL THEN
    RAISE EXCEPTION 'week_start is required';
  END IF;
  IF to_regclass('pg_temp.tmp_pay_build_finance_case_components_ctx') IS NULL THEN
    RAISE EXCEPTION 'tmp_pay_build_finance_case_components_ctx temp table is required';
  END IF;
  IF to_regclass('pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx') IS NULL THEN
    RAISE EXCEPTION 'tmp_pay_build_oneoff_payout_bank_details_ctx temp table is required';
  END IF;
  IF to_regclass('pg_temp.tmp_pay_build_timesheet_snapshots_ctx') IS NULL THEN
    RAISE EXCEPTION 'tmp_pay_build_timesheet_snapshots_ctx temp table is required';
  END IF;
  IF to_regclass('pg_temp.tmp_pay_build_recovery_template_rows') IS NULL THEN
    RAISE EXCEPTION 'tmp_pay_build_recovery_template_rows temp table is required';
  END IF;
  SELECT COALESCE(array_agg(distinct spr.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM pg_temp.tmp_pay_build_selected_preview_rows spr;

  IF p_operation_id IS NOT NULL THEN
    SELECT count(*)::integer,
           count(*) FILTER (
             WHERE allocation_row.status = 'ITEM_CREATED'
               AND allocation_row.pay_batch_item_id IS NOT NULL
               AND EXISTS (
                 SELECT 1
                 FROM public.pay_batch_items AS linked_done_item
                 JOIN public.pay_batch_candidates AS linked_done_candidate
                   ON linked_done_candidate.id = linked_done_item.pay_batch_candidate_id
                 WHERE linked_done_item.id = allocation_row.pay_batch_item_id
                   AND linked_done_candidate.pay_batch_id = v_batch_id
                   AND linked_done_candidate.candidate_id = allocation_row.candidate_id
                   AND coalesce(linked_done_item.is_voided, false) = false
                   AND (
                     (
                       UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
                       AND linked_done_item.item_type = UPPER(BTRIM(COALESCE(allocation_row.allocation_type, '')))
                     )
                     OR (
                       UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) NOT IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
                       AND linked_done_item.item_type IN ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT','MANUAL_DEBT_RECOVERY','LOAN_REPAYMENT')
                     )
                   )
               )
           )::integer
    INTO v_operation_allocation_total, v_operation_allocation_done
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_id = ANY(v_candidate_ids)
      AND allocation_row.pay_channel = v_scope
      AND allocation_row.finance_case_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN (
        'OVERPAYMENT_RECOVERY',
        'MANUAL_DEBT_RECOVERY',
        'PAYMENT_ADVANCE_REPAYMENT',
        'LOAN_REPAYMENT',
        'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
        'MANUAL_CREDIT_PAYOUT',
        'LOAN_PAYOUT',
        'UNDERPAYMENT_PAYMENT'
      );

    IF coalesce(v_operation_allocation_total, 0) > 0
       AND coalesce(v_operation_allocation_total, 0) = coalesce(v_operation_allocation_done, 0) THEN
      RETURN jsonb_build_object(
        'ok', true,
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'applied_count', 0,
        'reused_count', v_operation_allocation_done,
        'skipped_count', 0,
        'failed_count', 0,
        'mismatch_details', '{}'::jsonb
      );
    END IF;

    -- Register the exact transaction-local Draft finance authority before
    -- either the normal materialisation path or the bounded partial-retry
    -- cleanup can touch pay_batch_items.  ASSERT_COMPLETE at the end of this
    -- function must never depend on the partial-retry branch having run.
    PERFORM private.pay_workbench_draft_expected_effects_v1(
      p_operation_id,'APPLY_FINANCE_ADJUSTMENTS','REGISTER',
      jsonb_build_array(
        jsonb_build_object('relation_name','pay_batch_items','operation','INSERT'),
        jsonb_build_object('relation_name','pay_batch_items','operation','UPDATE'),
        jsonb_build_object('relation_name','pay_batch_items','operation','DELETE')
      ),jsonb_build_object('pay_batch_id',p_pay_batch_id)
    );

    IF coalesce(v_operation_allocation_done, 0) > 0
       AND coalesce(v_operation_allocation_done, 0) < coalesce(v_operation_allocation_total, 0) THEN
      IF EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
        JOIN public.pay_batch_items AS linked_item
          ON linked_item.id = allocation_row.pay_batch_item_id
        WHERE allocation_row.operation_id = p_operation_id
          AND allocation_row.candidate_id = ANY(v_candidate_ids)
          AND allocation_row.pay_channel = v_scope
          AND allocation_row.finance_case_id IS NOT NULL
          AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN (
            'OVERPAYMENT_RECOVERY',
            'MANUAL_DEBT_RECOVERY',
            'PAYMENT_ADVANCE_REPAYMENT',
            'LOAN_REPAYMENT',
            'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
            'MANUAL_CREDIT_PAYOUT',
            'LOAN_PAYOUT',
            'UNDERPAYMENT_PAYMENT'
          )
          AND allocation_row.status = 'ITEM_CREATED'
          AND allocation_row.pay_batch_item_id IS NOT NULL
          AND linked_item.reservation_id IS NOT NULL
        LIMIT 1
      ) THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS',
          'code', 'OPERATION_ALLOCATION_PARTIAL_RETRY_AFTER_RESERVATION',
          'message', 'pay_batch_apply_finance_adjustments cannot safely retry a partially-linked finance chunk after reservations have been created',
          'pay_batch_id', v_batch_id::text,
          'operation_id', p_operation_id::text,
          'pay_channel_scope', v_scope
        )::text;
      END IF;

      DELETE FROM public.pay_batch_items AS linked_item_delete
      USING public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.candidate_id = ANY(v_candidate_ids)
        AND allocation_row.pay_channel = v_scope
        AND allocation_row.finance_case_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN (
          'OVERPAYMENT_RECOVERY',
          'MANUAL_DEBT_RECOVERY',
          'PAYMENT_ADVANCE_REPAYMENT',
          'LOAN_REPAYMENT',
          'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
          'MANUAL_CREDIT_PAYOUT',
          'LOAN_PAYOUT',
        'UNDERPAYMENT_PAYMENT'
        )
        AND allocation_row.status = 'ITEM_CREATED'
        AND allocation_row.pay_batch_item_id = linked_item_delete.id
        AND linked_item_delete.reservation_id IS NULL;

      UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_reset
      SET
        status = 'PENDING',
        pay_batch_item_id = NULL,
        updated_at_utc = now()
      WHERE allocation_reset.operation_id = p_operation_id
        AND allocation_reset.candidate_id = ANY(v_candidate_ids)
        AND allocation_reset.pay_channel = v_scope
        AND allocation_reset.finance_case_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(allocation_reset.allocation_type, ''))) IN (
          'OVERPAYMENT_RECOVERY',
          'MANUAL_DEBT_RECOVERY',
          'PAYMENT_ADVANCE_REPAYMENT',
          'LOAN_REPAYMENT',
          'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
          'MANUAL_CREDIT_PAYOUT',
          'LOAN_PAYOUT',
        'UNDERPAYMENT_PAYMENT'
        )
        AND allocation_reset.status = 'ITEM_CREATED';

      v_operation_allocation_done := 0;
    END IF;
  END IF;


  IF EXISTS (
    WITH finance_case_draft_scope AS (
      SELECT DISTINCT
        spr.finance_case_id AS finance_case_id,
        spr.candidate_id AS candidate_id,
        upper(coalesce(nullif(spr.pay_channel, ''), v_scope)) AS target_pay_channel,
        spr.line_type AS source_line_type
      FROM pg_temp.tmp_pay_build_selected_preview_rows AS spr
      WHERE spr.draftable = true
        AND spr.finance_case_id IS NOT NULL
        AND spr.candidate_id = ANY(v_candidate_ids)
        AND (v_client_filter_single IS NULL OR spr.client_id = v_client_filter_single)
        AND spr.line_type IN (
          'OVERPAYMENT_RECOVERY',
          'MANUAL_DEBT_RECOVERY',
          'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
          'MANUAL_CREDIT_PAYOUT',
          'UNDERPAYMENT_PAYMENT'
        )

      UNION

      SELECT DISTINCT
        template_rows.finance_case_id AS finance_case_id,
        template_rows.candidate_id AS candidate_id,
        CASE
          WHEN upper(coalesce(template_rows.pay_channel, '')) IN ('PAYE', 'UMBRELLA') THEN upper(coalesce(template_rows.pay_channel, ''))
          WHEN coalesce(btrim(coalesce(template_rows.pay_channel, '')), '') = '' THEN v_scope
          ELSE upper(coalesce(template_rows.pay_channel, ''))
        END AS target_pay_channel,
        template_rows.recovery_family AS source_line_type
      FROM pg_temp.tmp_pay_build_recovery_template_rows AS template_rows
      WHERE template_rows.finance_case_id IS NOT NULL
        AND template_rows.candidate_id = ANY(v_candidate_ids)
        AND template_rows.recovery_family IN (
          'OVERPAYMENT_RECOVERY',
          'MANUAL_DEBT_RECOVERY'
        )
    )
    SELECT 1
    FROM finance_case_draft_scope AS draft_scope
    JOIN public.pay_advances AS finance_case
      ON finance_case.id = draft_scope.finance_case_id
     AND finance_case.case_type IN (
       'OVERPAYMENT'::public.pay_finance_case_type_enum,
       'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum,
       'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum,
       'UNDERPAYMENT'::public.pay_finance_case_type_enum
     )
     AND finance_case.status = 'ACTIVE'::public.pay_advance_status_enum
    JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.finance_case_id = draft_scope.finance_case_id
     AND finance_component.closed_at_utc IS NULL
    WHERE draft_scope.target_pay_channel IN ('PAYE', 'UMBRELLA')
      AND finance_component.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      AND round(coalesce(finance_component.remaining_source_amount, 0), 2) > 0
      AND upper(coalesce(finance_component.source_pay_method, '')) IN ('PAYE', 'UMBRELLA')
      AND upper(coalesce(finance_component.source_pay_method, '')) <> draft_scope.target_pay_channel
      AND (
        coalesce(finance_component.is_resolution_stale, false) = true
        OR nullif(btrim(coalesce(finance_component.saved_target_pay_method, '')), '') IS NULL
        OR upper(coalesce(finance_component.saved_target_pay_method, '')) <> draft_scope.target_pay_channel
        OR finance_component.saved_resolution_mode IS NULL
        OR jsonb_typeof(coalesce(finance_component.saved_resolution_payload_json, 'null'::jsonb)) <> 'object'
        OR jsonb_typeof(coalesce(finance_component.saved_resolution_result_json, 'null'::jsonb)) <> 'object'
        OR nullif(btrim(coalesce(finance_component.resolution_fingerprint, '')), '') IS NULL
      )
    LIMIT 1
  ) THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS',
      'code', 'TAXABLE_CHANNEL_RESTRUCTURE_REQUIRED',
      'message', 'pay_batch_apply_finance_adjustments: taxable PAYE/Umbrella channel restructure is required before finance items can be drafted',
      'pay_batch_id', v_batch_id::text,
      'pay_channel_scope', v_scope
    )::text;
  END IF;


v_stage := 'STAGE_12C_APPLY_LOAN_PAYOUTS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  insert into public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    is_mismatch,
    finance_case_id,
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
    paye_treatment
  )
  with safe_case_rows as (
    select
      spr.preview_row_id,
      spr.candidate_id,
      spr.finance_case_id,
      spr.client_id,
      spr.timesheet_id as linked_timesheet_id,
      upper(coalesce(spr.pay_channel,'')) as pay_channel,
      upper(coalesce(spr.paye_treatment,'')) as paye_treatment,
      nullif(btrim(coalesce(spr.routing_kind,'')), '') as routing_kind,
      nullif(btrim(coalesce(spr.destination_label,'')), '') as destination_label,
      nullif(btrim(coalesce(spr.taxability,'')), '') as taxability,
      nullif(btrim(coalesce(spr.beneficiary_name,'')), '') as beneficiary_name,
      nullif(btrim(coalesce(spr.masked_bank_account,'')), '') as masked_bank_account,
      nullif(btrim(coalesce(spr.bank_details_hash,'')), '') as bank_details_hash,
      coalesce(spr.is_candidate_directed_oneoff_payout, false) as is_candidate_directed_oneoff_payout,
      coalesce(spr.appears_on_umbrella_remittance, false) as appears_on_umbrella_remittance,
      coalesce(spr.generates_candidate_payment_advice, false) as generates_candidate_payment_advice,
      abs(round(coalesce(spr.preview_amount_ex_vat, 0), 2))::numeric(12,2) as payout_total_target_ex
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    where spr.draftable = true
      and spr.line_type = 'LOAN_PAYOUT'
      and spr.finance_case_id is not null
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
  ),
  candidate_due as (
    select
      pbc.id as pay_batch_candidate_id,
      scr.preview_row_id,
      scr.candidate_id,
      scr.pay_channel,
      case when scr.pay_channel = 'UMBRELLA' then c.umbrella_id else null end as umbrella_id,
      scr.finance_case_id,
      scr.linked_timesheet_id,
      scr.paye_treatment,
      scr.routing_kind,
      scr.destination_label,
      scr.taxability,
      scr.beneficiary_name,
      scr.masked_bank_account,
      scr.bank_details_hash,
      scr.is_candidate_directed_oneoff_payout,
      scr.appears_on_umbrella_remittance,
      scr.generates_candidate_payment_advice,
      scr.payout_total_target_ex
    from safe_case_rows scr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = scr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = scr.candidate_id
  ),
  case_component_base as (
    select
      cd.pay_batch_candidate_id,
      cd.preview_row_id,
      cd.candidate_id,
      cd.pay_channel,
      cd.umbrella_id,
      cd.finance_case_id,
      cd.linked_timesheet_id,
      cd.paye_treatment,
      cd.routing_kind,
      cd.destination_label,
      cd.taxability,
      cd.beneficiary_name,
      cd.masked_bank_account,
      cd.bank_details_hash,
      cd.is_candidate_directed_oneoff_payout,
      cd.appears_on_umbrella_remittance,
      cd.generates_candidate_payment_advice,
      pfc.id as finance_component_id,
      pfc.component_key_type,
      pfc.component_key_value,
      pfc.classification,
      upper(coalesce(pfc.source_pay_method,'')) as source_pay_method,
      pfc.source_basis_json,
      upper(coalesce(pfc.saved_target_pay_method,'')) as saved_target_pay_method,
      pfc.saved_resolution_mode,
      pfc.saved_resolution_payload_json,
      pfc.saved_resolution_result_json,
      round(greatest(coalesce(pfc.source_amount, pfc.remaining_source_amount, 0), 0), 2)::numeric(12,2) as source_amount,
      round(greatest(coalesce(pfc.remaining_source_amount, 0), 0), 2)::numeric(12,2) as remaining_source_amount,
      pfc.allocation_priority_group,
      pfc.allocation_priority_order,
      pfc.created_at_utc as finance_component_created_at,
      cd.payout_total_target_ex,
      case
        when cd.pay_channel = 'UMBRELLA'
         and pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then round((public._pay_umbrella_vat_calc(round(greatest(coalesce(pfc.remaining_source_amount,0),0),2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'ex')::numeric, 2)::numeric(12,2)
        else round(greatest(coalesce(pfc.remaining_source_amount,0),0), 2)::numeric(12,2)
      end as remaining_target_amount_ex_vat,
      case
        when cd.pay_channel = 'UMBRELLA'
         and pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then round((public._pay_umbrella_vat_calc(round(greatest(coalesce(pfc.remaining_source_amount,0),0),2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric, 2)::numeric(12,2)
        else 0::numeric(12,2)
      end as remaining_target_amount_vat,
      case
        when cd.pay_channel = 'UMBRELLA'
         and pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then round((public._pay_umbrella_vat_calc(round(greatest(coalesce(pfc.remaining_source_amount,0),0),2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric, 2)::numeric(12,2)
        else round(greatest(coalesce(pfc.remaining_source_amount,0),0), 2)::numeric(12,2)
      end as remaining_target_amount_inc_vat,
      obd.created_by_user_id as bank_details_created_by_user_id,
      obd.updated_by_user_id as bank_details_updated_by_user_id,
      obd.note as bank_details_note
    from candidate_due cd
    join pg_temp.tmp_pay_build_finance_case_components_ctx pfc
      on pfc.finance_case_id = cd.finance_case_id
     and coalesce(pfc.remaining_source_amount, 0) > 0
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = cd.umbrella_id
    left join pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx obd
      on obd.finance_case_id = cd.finance_case_id
  ),
  case_component_due as (
    select
      ccb.*,
      sum(ccb.remaining_target_amount_ex_vat) over (
        partition by ccb.finance_case_id
        order by ccb.allocation_priority_group, ccb.allocation_priority_order, ccb.finance_component_created_at, ccb.finance_component_id
        rows between unbounded preceding and 1 preceding
      )::numeric(12,2) as cum_before_case_target,
      least(
        ccb.remaining_target_amount_ex_vat,
        greatest(ccb.payout_total_target_ex - coalesce(sum(ccb.remaining_target_amount_ex_vat) over (
          partition by ccb.finance_case_id
          order by ccb.allocation_priority_group, ccb.allocation_priority_order, ccb.finance_component_created_at, ccb.finance_component_id
          rows between unbounded preceding and 1 preceding
        ), 0), 0)
      )::numeric(12,2) as take_target_ex
    from case_component_base ccb
  ),
  alloc as (
    select
      ccd.pay_batch_candidate_id,
      ccd.pay_channel,
      ccd.umbrella_id,
      ccd.finance_case_id,
      ccd.linked_timesheet_id,
      ccd.paye_treatment,
      ccd.routing_kind,
      ccd.destination_label,
      ccd.taxability,
      ccd.beneficiary_name,
      ccd.masked_bank_account,
      ccd.bank_details_hash,
      ccd.is_candidate_directed_oneoff_payout,
      ccd.appears_on_umbrella_remittance,
      ccd.generates_candidate_payment_advice,
      ccd.bank_details_created_by_user_id,
      ccd.bank_details_updated_by_user_id,
      ccd.bank_details_note,
      ccd.finance_component_id,
      ccd.component_key_type,
      ccd.component_key_value,
      ccd.classification,
      ccd.source_pay_method,
      ccd.source_basis_json,
      ccd.saved_target_pay_method,
      ccd.saved_resolution_mode,
      ccd.saved_resolution_payload_json,
      ccd.saved_resolution_result_json,
      ccd.source_amount,
      ccd.remaining_source_amount,
      ccd.remaining_target_amount_ex_vat,
      ccd.remaining_target_amount_vat,
      ccd.remaining_target_amount_inc_vat,
      round(ccd.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      case
        when round(coalesce(ccd.remaining_target_amount_ex_vat,0),2) > 0 then least(
          ccd.remaining_source_amount,
          round(ccd.remaining_source_amount * ccd.take_target_ex / ccd.remaining_target_amount_ex_vat, 2)
        )::numeric(12,2)
        else 0::numeric(12,2)
      end as take_source_amount,
      case
        when round(coalesce(ccd.remaining_target_amount_ex_vat,0),2) > 0 then round(coalesce(ccd.remaining_target_amount_vat,0) * ccd.take_target_ex / ccd.remaining_target_amount_ex_vat, 2)::numeric(12,2)
        else 0::numeric(12,2)
      end as take_target_vat,
      case
        when round(coalesce(ccd.remaining_target_amount_ex_vat,0),2) > 0 then round(coalesce(ccd.remaining_target_amount_inc_vat,0) * ccd.take_target_ex / ccd.remaining_target_amount_ex_vat, 2)::numeric(12,2)
        else 0::numeric(12,2)
      end as take_target_inc
    from case_component_due ccd
    where ccd.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    a.pay_batch_candidate_id,
    'LOAN_PAYOUT'::text as item_type,
    a.linked_timesheet_id as timesheet_id,
    null::text as segment_key,
    ('advance:' || a.finance_case_id::text) as source_ref,
    'Payment Advance'::text as description,
    round(a.take_target_ex, 2) as amount_ex_vat,
    round(a.take_target_vat, 2) as amount_vat,
    round(a.take_target_inc, 2) as amount_inc_vat,
    a.pay_channel,
    a.umbrella_id,
    false as is_mismatch,
    a.finance_case_id,
    a.finance_component_id,
    jsonb_build_object(
      'finance_case_id', a.finance_case_id::text,
      'finance_component_id', a.finance_component_id::text,
      'linked_timesheet_id', case when a.linked_timesheet_id is null then null else a.linked_timesheet_id::text end,
      'classification', a.classification::text,
      'source_pay_method', a.source_pay_method,
      'target_pay_method', a.pay_channel,
      'source_basis_json', a.source_basis_json,
      'saved_target_pay_method', a.saved_target_pay_method,
      'saved_resolution_mode', case when a.saved_resolution_mode is null then null else a.saved_resolution_mode::text end,
      'saved_resolution_payload_json', a.saved_resolution_payload_json,
      'saved_resolution_result_json', a.saved_resolution_result_json,
      'reserved_source_amount', round(a.take_source_amount, 2),
      'frozen_target_amount_ex_vat', round(a.take_target_ex, 2),
      'frozen_target_amount_vat', round(a.take_target_vat, 2),
      'frozen_target_amount_inc_vat', round(a.take_target_inc, 2)
    ) as frozen_component_snapshot_json,
    a.component_key_type,
    a.component_key_value,
    a.classification,
    a.source_basis_json,
    a.source_pay_method,
    a.pay_channel,
    a.saved_resolution_mode,
    a.saved_resolution_payload_json,
    a.saved_resolution_result_json,
    round(a.take_source_amount, 2),
    round(a.take_target_ex, 2),
    round(a.take_target_vat, 2),
    round(a.take_target_inc, 2),
    jsonb_strip_nulls(
      jsonb_build_object(
        'taxability', a.taxability,
        'routing_kind', a.routing_kind,
        'destination_label', a.destination_label,
        'pay_channel', a.pay_channel,
        'payee_entity_kind', case when a.is_candidate_directed_oneoff_payout then 'CANDIDATE' when a.routing_kind = 'UMBRELLA_COMPANY' then 'UMBRELLA' else 'CANDIDATE' end,
        'beneficiary_name', a.beneficiary_name,
        'masked_bank_account', a.masked_bank_account,
        'bank_details_hash', a.bank_details_hash,
        'bank_details_note', a.bank_details_note,
        'bank_details_created_by_user_id', case when a.bank_details_created_by_user_id is null then null else a.bank_details_created_by_user_id::text end,
        'bank_details_updated_by_user_id', case when a.bank_details_updated_by_user_id is null then null else a.bank_details_updated_by_user_id::text end,
        'appears_on_umbrella_remittance', a.appears_on_umbrella_remittance,
        'generates_candidate_payment_advice', a.generates_candidate_payment_advice,
        'is_candidate_directed_oneoff_payout', a.is_candidate_directed_oneoff_payout,
        'payee_entity_id', case
          when a.is_candidate_directed_oneoff_payout then (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = a.pay_batch_candidate_id
            limit 1
          )
          when a.routing_kind = 'UMBRELLA_COMPANY' then case when a.umbrella_id is null then null else a.umbrella_id::text end
          else (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = a.pay_batch_candidate_id
            limit 1
          )
        end
      )
    ) as payout_instruction_snapshot_json,
    case
      when a.pay_channel <> 'PAYE' then 'NONE'
      when a.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_ADD'
      when a.classification in ('REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum, 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum) then 'NET_ADD'
      else coalesce(nullif(a.paye_treatment,''), 'NET_ADD')
    end as paye_treatment
  from alloc a
  where a.take_target_ex > 0;

  get diagnostics v_rows_ins_loan_payout_items = row_count;


v_stage := 'STAGE_12D_APPLY_MANUAL_CREDIT_PAYOUTS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  insert into public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    is_mismatch,
    finance_case_id,
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
    paye_treatment
  )
  with selected_case_rows as (
    select distinct on (spr.finance_case_id)
      pbc.id as pay_batch_candidate_id,
      spr.candidate_id,
      spr.finance_case_id,
      spr.client_id,
      spr.timesheet_id as linked_timesheet_id,
      upper(coalesce(nullif(spr.pay_channel,''), v_scope)) as pay_channel,
      upper(coalesce(nullif(spr.paye_treatment,''), 'NONE')) as paye_treatment,
      case when upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = 'UMBRELLA' then c.umbrella_id else null::uuid end as umbrella_id,
      nullif(btrim(coalesce(spr.routing_kind,'')), '') as routing_kind,
      nullif(btrim(coalesce(spr.destination_label,'')), '') as destination_label,
      nullif(btrim(coalesce(spr.taxability,'')), '') as taxability,
      nullif(btrim(coalesce(spr.beneficiary_name,'')), '') as beneficiary_name,
      nullif(btrim(coalesce(spr.masked_bank_account,'')), '') as masked_bank_account,
      nullif(btrim(coalesce(spr.bank_details_hash,'')), '') as bank_details_hash,
      coalesce(spr.is_candidate_directed_oneoff_payout, false) as is_candidate_directed_oneoff_payout,
      coalesce(spr.appears_on_umbrella_remittance, false) as appears_on_umbrella_remittance,
      coalesce(spr.generates_candidate_payment_advice, false) as generates_candidate_payment_advice,
      round(abs(coalesce(spr.preview_amount_ex_vat, 0)), 2)::numeric(12,2) as due_amount_ex_vat,
      coalesce(spr.case_components_json, '[]'::jsonb) as case_components_json
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = spr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = spr.candidate_id
    where spr.draftable = true
      and spr.line_type = 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
      and spr.finance_case_id is not null
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
      and abs(coalesce(spr.preview_amount_ex_vat, 0)) > 0
    order by spr.finance_case_id, spr.preview_row_id
  ),
  component_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      scr.routing_kind,
      scr.destination_label,
      scr.taxability,
      scr.beneficiary_name,
      scr.masked_bank_account,
      scr.bank_details_hash,
      scr.is_candidate_directed_oneoff_payout,
      scr.appears_on_umbrella_remittance,
      scr.generates_candidate_payment_advice,
      scr.due_amount_ex_vat,
      comp.comp_json,
      case
        when nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), '') is null then null::uuid
        else (comp.comp_json->>'finance_component_id')::uuid
      end as finance_component_id,
      nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), '') as component_key_type,
      coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), 'TOTAL') as component_key_value,
      case
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        else null::public.pay_finance_component_classification_enum
      end as classification,
      upper(btrim(coalesce(comp.comp_json->>'source_pay_method',''))) as source_pay_method,
      coalesce(comp.comp_json->'source_basis_json', '{}'::jsonb) as source_basis_json,
      upper(nullif(btrim(coalesce(comp.comp_json->>'saved_target_pay_method','')), '')) as saved_target_pay_method,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_payload_json') = 'object' then comp.comp_json->'saved_resolution_payload_json'
        else null::jsonb
      end as saved_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_result_json') = 'object' then comp.comp_json->'saved_resolution_result_json'
        else null::jsonb
      end as saved_resolution_result_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_payload_json') = 'object' then comp.comp_json->'suggested_resolution_payload_json'
        else null::jsonb
      end as preview_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_result_json') = 'object' then comp.comp_json->'suggested_resolution_result_json'
        else null::jsonb
      end as preview_resolution_result_json,
      nullif(btrim(coalesce(comp.comp_json->>'saved_resolution_mode','')), '') as saved_resolution_mode_text,
      nullif(btrim(coalesce(comp.comp_json->'suggested_resolution_payload_json'->>'resolution_mode','')), '') as preview_resolution_mode_text,
      round(
        case
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'source_amount')::numeric)
          when coalesce(comp.comp_json->>'source_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'source_amount_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'frozen_source_amount','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'frozen_source_amount')::numeric)
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'remaining_source_amount')::numeric)
          when coalesce(comp.comp_json->>'remaining_source_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'remaining_source_amount_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'source_amount')::numeric)
          when coalesce(comp.comp_json->>'source_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'source_amount_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'frozen_source_amount','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'frozen_source_amount')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as remaining_source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as allocated_source_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as preview_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat')::numeric
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'target_pay_ex_vat')::numeric
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as target_pay_ex_vat,
      case
        when coalesce(comp.comp_json->>'source_units','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_units')::numeric, 6)
        else null::numeric
      end as source_units,
      case
        when coalesce(comp.comp_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_rate')::numeric, 2)
        else null::numeric
      end as source_rate,
      case
        when coalesce(comp.comp_json->>'target_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_rate')::numeric, 2)
        else null::numeric
      end as target_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_rate')::numeric, 2)
        else null::numeric
      end as source_charge_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as source_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_margin_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_margin_ex_vat')::numeric, 2)
        else null::numeric
      end as target_margin_ex_vat,
      case
        when coalesce(comp.comp_json->>'margin_delta_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'margin_delta_ex_vat')::numeric, 2)
        else null::numeric
      end as margin_delta_ex_vat,
      row_number() over (
        partition by scr.finance_case_id
        order by
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''), md5(comp.comp_json::text)),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), ''), ''),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), '')
      ) as component_rn,
      count(*) over (partition by scr.finance_case_id) as component_ct,
      round(sum(
        round(
          case
            when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'preview_due_amount_ex_vat')::numeric
            else 0
          end,
          2
        )
      ) over (partition by scr.finance_case_id), 2)::numeric(12,2) as component_preview_due_sum
    from selected_case_rows scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
      and round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'preview_due_amount_ex_vat')::numeric
          else 0
        end,
        2
      ) > 0
  ),
  normalized as (
    select
      cr.*,
      round(
        case
          when cr.component_ct = 1 then cr.due_amount_ex_vat
          when cr.component_rn = 1 then greatest(cr.due_amount_ex_vat - (coalesce(cr.component_preview_due_sum, 0) - coalesce(cr.preview_due_amount_ex_vat, 0)), 0)
          else cr.preview_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as take_target_ex
    from component_rows cr
  ),
  final_alloc as (
    select
      n.pay_batch_candidate_id,
      n.finance_case_id,
      n.pay_channel,
      n.paye_treatment,
      n.umbrella_id,
      n.routing_kind,
      n.destination_label,
      n.taxability,
      n.beneficiary_name,
      n.masked_bank_account,
      n.bank_details_hash,
      n.is_candidate_directed_oneoff_payout,
      n.appears_on_umbrella_remittance,
      n.generates_candidate_payment_advice,
      n.finance_component_id,
      n.component_key_type,
      n.component_key_value,
      n.classification,
      n.source_pay_method,
      n.source_basis_json,
      case
        when n.saved_resolution_mode_text is not null then n.saved_resolution_mode_text::public.pay_finance_component_resolution_mode_enum
        else null::public.pay_finance_component_resolution_mode_enum
      end as frozen_resolution_mode,
      case
        when jsonb_typeof(n.saved_resolution_payload_json) = 'object' then jsonb_strip_nulls(
          n.saved_resolution_payload_json
          || jsonb_build_object(
            'target_pay_method', n.pay_channel,
            'target_units', n.source_units,
            'suggested_target_rate', n.target_rate,
            'source_rate', n.source_rate,
            'source_charge_rate', n.source_charge_rate
          )
        )
        else jsonb_strip_nulls(jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_units', n.source_units,
          'suggested_target_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate
        ))
      end as frozen_resolution_payload_json,
      round(
        case
          when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
          when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
          else 0
        end,
        2
      )::numeric(12,2) as take_source_amount,
      round(n.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as take_target_vat,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
          else round(n.take_target_ex, 2)
        end,
        2
      )::numeric(12,2) as take_target_inc,
      jsonb_strip_nulls(
        coalesce(n.comp_json, '{}'::jsonb)
        || jsonb_build_object(
          'frozen_target_pay_method', n.pay_channel,
          'frozen_resolution_mode_text', n.saved_resolution_mode_text,
          'frozen_source_amount', round(
            case
              when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
              when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
              else 0
            end,
            2
          ),
          'frozen_target_amount_ex_vat', round(n.take_target_ex, 2),
          'frozen_target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'frozen_target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'selected_preview_due_amount_ex_vat', round(n.due_amount_ex_vat, 2),
          'component_preview_due_amount_ex_vat', round(n.take_target_ex, 2)
        )
      ) as frozen_component_snapshot_json,
      jsonb_strip_nulls(
        coalesce(n.saved_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_amount_ex_vat', round(n.take_target_ex, 2),
          'target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'replacement_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate,
          'source_charge_ex_vat', n.source_charge_ex_vat,
          'target_charge_ex_vat', n.target_charge_ex_vat,
          'target_margin_ex_vat', n.target_margin_ex_vat,
          'margin_delta_ex_vat', n.margin_delta_ex_vat
        )
      ) as frozen_resolution_result_json
    from normalized n
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = n.umbrella_id
    where n.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    fa.pay_batch_candidate_id,
    'MANUAL_CREDIT_PAYOUT'::text as item_type,
    null::uuid as timesheet_id,
    null::text as segment_key,
    ('advance:' || fa.finance_case_id::text) as source_ref,
    'Manual credit adjustment payment'::text as description,
    round(fa.take_target_ex, 2) as amount_ex_vat,
    round(fa.take_target_vat, 2) as amount_vat,
    round(fa.take_target_inc, 2) as amount_inc_vat,
    fa.pay_channel,
    fa.umbrella_id,
    (fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(fa.source_pay_method,'')) <> upper(coalesce(fa.pay_channel,''))) as is_mismatch,
    fa.finance_case_id,
    fa.finance_component_id,
    fa.frozen_component_snapshot_json,
    fa.component_key_type,
    fa.component_key_value,
    fa.classification,
    fa.source_basis_json,
    fa.source_pay_method,
    fa.pay_channel,
    fa.frozen_resolution_mode,
    fa.frozen_resolution_payload_json,
    fa.frozen_resolution_result_json,
    round(fa.take_source_amount, 2),
    round(fa.take_target_ex, 2),
    round(fa.take_target_vat, 2),
    round(fa.take_target_inc, 2),
    jsonb_strip_nulls(
      jsonb_build_object(
        'taxability', fa.taxability,
        'routing_kind', fa.routing_kind,
        'destination_label', fa.destination_label,
        'pay_channel', fa.pay_channel,
        'payee_entity_kind', case when fa.is_candidate_directed_oneoff_payout then 'CANDIDATE' when fa.routing_kind = 'UMBRELLA_COMPANY' then 'UMBRELLA' else 'CANDIDATE' end,
        'beneficiary_name', fa.beneficiary_name,
        'masked_bank_account', fa.masked_bank_account,
        'bank_details_hash', fa.bank_details_hash,
        'appears_on_umbrella_remittance', fa.appears_on_umbrella_remittance,
        'generates_candidate_payment_advice', fa.generates_candidate_payment_advice,
        'payee_entity_id', case
          when fa.is_candidate_directed_oneoff_payout then (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = fa.pay_batch_candidate_id
            limit 1
          )
          when fa.routing_kind = 'UMBRELLA_COMPANY' then case when fa.umbrella_id is null then null else fa.umbrella_id::text end
          else (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = fa.pay_batch_candidate_id
            limit 1
          )
        end
      )
    ) as payout_instruction_snapshot_json,
    case
      when fa.pay_channel <> 'PAYE' then 'NONE'
      when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_ADD'
      when fa.classification in ('REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum, 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum) then 'NET_ADD'
      else coalesce(nullif(fa.paye_treatment,''), 'NET_ADD')
    end as paye_treatment
  from final_alloc fa
  where fa.take_target_ex > 0;

  get diagnostics v_rows_ins_manual_credit_payout_items = row_count;


v_stage := 'STAGE_12E_APPLY_UNDERPAYMENT_PAYMENTS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  insert into public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    is_mismatch,
    finance_case_id,
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
    paye_treatment
  )
  with selected_case_rows as (
    select distinct on (spr.finance_case_id)
      pbc.id as pay_batch_candidate_id,
      spr.candidate_id,
      spr.finance_case_id,
      spr.client_id,
      spr.timesheet_id as linked_timesheet_id,
      upper(coalesce(nullif(spr.pay_channel,''), v_scope)) as pay_channel,
      upper(coalesce(nullif(spr.paye_treatment,''), 'NONE')) as paye_treatment,
      case when upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = 'UMBRELLA' then c.umbrella_id else null::uuid end as umbrella_id,
      nullif(btrim(coalesce(spr.routing_kind,'')), '') as routing_kind,
      nullif(btrim(coalesce(spr.destination_label,'')), '') as destination_label,
      nullif(btrim(coalesce(spr.taxability,'')), '') as taxability,
      nullif(btrim(coalesce(spr.beneficiary_name,'')), '') as beneficiary_name,
      nullif(btrim(coalesce(spr.masked_bank_account,'')), '') as masked_bank_account,
      nullif(btrim(coalesce(spr.bank_details_hash,'')), '') as bank_details_hash,
      coalesce(spr.is_candidate_directed_oneoff_payout, false) as is_candidate_directed_oneoff_payout,
      coalesce(spr.appears_on_umbrella_remittance, false) as appears_on_umbrella_remittance,
      coalesce(spr.generates_candidate_payment_advice, false) as generates_candidate_payment_advice,
      round(abs(coalesce(spr.preview_amount_ex_vat, 0)), 2)::numeric(12,2) as due_amount_ex_vat,
      coalesce(spr.case_components_json, '[]'::jsonb) as case_components_json
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = spr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = spr.candidate_id
    where spr.draftable = true
      and spr.line_type = 'UNDERPAYMENT_PAYMENT'
      and upper(coalesce(spr.case_type, '')) = 'UNDERPAYMENT'
      and spr.finance_case_id is not null
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
      and abs(coalesce(spr.preview_amount_ex_vat, 0)) > 0
    order by spr.finance_case_id, spr.preview_row_id
  ),
  component_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.linked_timesheet_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      scr.routing_kind,
      scr.destination_label,
      scr.taxability,
      scr.beneficiary_name,
      scr.masked_bank_account,
      scr.bank_details_hash,
      scr.is_candidate_directed_oneoff_payout,
      scr.appears_on_umbrella_remittance,
      scr.generates_candidate_payment_advice,
      scr.due_amount_ex_vat,
      comp.comp_json,
      case
        when nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), '') is null then null::uuid
        else (comp.comp_json->>'finance_component_id')::uuid
      end as finance_component_id,
      nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), '') as component_key_type,
      coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), 'TOTAL') as component_key_value,
      case
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        else null::public.pay_finance_component_classification_enum
      end as classification,
      upper(btrim(coalesce(comp.comp_json->>'source_pay_method',''))) as source_pay_method,
      coalesce(comp.comp_json->'source_basis_json', '{}'::jsonb) as source_basis_json,
      upper(nullif(btrim(coalesce(comp.comp_json->>'saved_target_pay_method','')), '')) as saved_target_pay_method,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_payload_json') = 'object' then comp.comp_json->'saved_resolution_payload_json'
        else null::jsonb
      end as saved_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_result_json') = 'object' then comp.comp_json->'saved_resolution_result_json'
        else null::jsonb
      end as saved_resolution_result_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_payload_json') = 'object' then comp.comp_json->'suggested_resolution_payload_json'
        else null::jsonb
      end as preview_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_result_json') = 'object' then comp.comp_json->'suggested_resolution_result_json'
        else null::jsonb
      end as preview_resolution_result_json,
      nullif(btrim(coalesce(comp.comp_json->>'saved_resolution_mode','')), '') as saved_resolution_mode_text,
      nullif(btrim(coalesce(comp.comp_json->'suggested_resolution_payload_json'->>'resolution_mode','')), '') as preview_resolution_mode_text,
      round(
        case
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as remaining_source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as allocated_source_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as preview_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat')::numeric
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'target_pay_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as target_pay_ex_vat,
      case
        when coalesce(comp.comp_json->>'source_units','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_units')::numeric, 6)
        else null::numeric
      end as source_units,
      case
        when coalesce(comp.comp_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_rate')::numeric, 2)
        else null::numeric
      end as source_rate,
      case
        when coalesce(comp.comp_json->>'target_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_rate')::numeric, 2)
        else null::numeric
      end as target_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_rate')::numeric, 2)
        else null::numeric
      end as source_charge_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as source_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_margin_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_margin_ex_vat')::numeric, 2)
        else null::numeric
      end as target_margin_ex_vat,
      case
        when coalesce(comp.comp_json->>'margin_delta_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'margin_delta_ex_vat')::numeric, 2)
        else null::numeric
      end as margin_delta_ex_vat,
      row_number() over (
        partition by scr.finance_case_id
        order by
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''), md5(comp.comp_json::text)),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), ''), ''),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), '')
      ) as component_rn,
      count(*) over (partition by scr.finance_case_id) as component_ct,
      round(sum(
        round(
          case
            when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
            else 0
          end,
          2
        )
      ) over (partition by scr.finance_case_id), 2)::numeric(12,2) as component_preview_due_sum
    from selected_case_rows scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
      and round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      ) > 0
  ),
  normalized as (
    select
      cr.*,
      round(
        case
          when cr.component_ct = 1 then cr.due_amount_ex_vat
          when cr.component_rn = 1 then greatest(cr.due_amount_ex_vat - (coalesce(cr.component_preview_due_sum, 0) - coalesce(cr.preview_due_amount_ex_vat, 0)), 0)
          else cr.preview_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as take_target_ex
    from component_rows cr
  ),
  final_alloc as (
    select
      n.pay_batch_candidate_id,
      n.finance_case_id,
      n.linked_timesheet_id,
      n.pay_channel,
      n.paye_treatment,
      n.umbrella_id,
      n.routing_kind,
      n.destination_label,
      n.taxability,
      n.beneficiary_name,
      n.masked_bank_account,
      n.bank_details_hash,
      n.is_candidate_directed_oneoff_payout,
      n.appears_on_umbrella_remittance,
      n.generates_candidate_payment_advice,
      n.finance_component_id,
      n.component_key_type,
      n.component_key_value,
      n.classification,
      n.source_pay_method,
      n.source_basis_json,
      case
        when n.saved_resolution_mode_text is not null then n.saved_resolution_mode_text::public.pay_finance_component_resolution_mode_enum
        else null::public.pay_finance_component_resolution_mode_enum
      end as frozen_resolution_mode,
      case
        when jsonb_typeof(n.saved_resolution_payload_json) = 'object' then jsonb_strip_nulls(
          n.saved_resolution_payload_json
          || jsonb_build_object(
            'target_pay_method', n.pay_channel,
            'target_units', n.source_units,
            'suggested_target_rate', n.target_rate,
            'source_rate', n.source_rate,
            'source_charge_rate', n.source_charge_rate
          )
        )
        else jsonb_strip_nulls(jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_units', n.source_units,
          'suggested_target_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate
        ))
      end as frozen_resolution_payload_json,
      round(
        case
          when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
          when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
          else 0
        end,
        2
      )::numeric(12,2) as take_source_amount,
      round(n.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as take_target_vat,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
          else round(n.take_target_ex, 2)
        end,
        2
      )::numeric(12,2) as take_target_inc,
      jsonb_strip_nulls(
        coalesce(n.comp_json, '{}'::jsonb)
        || jsonb_build_object(
          'frozen_target_pay_method', n.pay_channel,
          'frozen_resolution_mode_text', n.saved_resolution_mode_text,
          'frozen_source_amount', round(
            case
              when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
              when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
              else 0
            end,
            2
          ),
          'frozen_target_amount_ex_vat', round(n.take_target_ex, 2),
          'frozen_target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'frozen_target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'selected_preview_due_amount_ex_vat', round(n.due_amount_ex_vat, 2),
          'component_preview_due_amount_ex_vat', round(n.take_target_ex, 2)
        )
      ) as frozen_component_snapshot_json,
      jsonb_strip_nulls(
        coalesce(n.saved_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_amount_ex_vat', round(n.take_target_ex, 2),
          'target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'replacement_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate,
          'source_charge_ex_vat', n.source_charge_ex_vat,
          'target_charge_ex_vat', n.target_charge_ex_vat,
          'target_margin_ex_vat', n.target_margin_ex_vat,
          'margin_delta_ex_vat', n.margin_delta_ex_vat
        )
      ) as frozen_resolution_result_json
    from normalized n
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = n.umbrella_id
    where n.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    fa.pay_batch_candidate_id,
    'UNDERPAYMENT_PAYMENT'::text as item_type,
    fa.linked_timesheet_id as timesheet_id,
    null::text as segment_key,
    ('advance:' || fa.finance_case_id::text) as source_ref,
    'Underpayment payment'::text as description,
    round(fa.take_target_ex, 2) as amount_ex_vat,
    round(fa.take_target_vat, 2) as amount_vat,
    round(fa.take_target_inc, 2) as amount_inc_vat,
    fa.pay_channel,
    fa.umbrella_id,
    (fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(fa.source_pay_method,'')) <> upper(coalesce(fa.pay_channel,''))) as is_mismatch,
    fa.finance_case_id,
    fa.finance_component_id,
    fa.frozen_component_snapshot_json,
    fa.component_key_type,
    fa.component_key_value,
    fa.classification,
    fa.source_basis_json,
    fa.source_pay_method,
    fa.pay_channel,
    fa.frozen_resolution_mode,
    fa.frozen_resolution_payload_json,
    fa.frozen_resolution_result_json,
    round(fa.take_source_amount, 2),
    round(fa.take_target_ex, 2),
    round(fa.take_target_vat, 2),
    round(fa.take_target_inc, 2),
    jsonb_strip_nulls(
      jsonb_build_object(
        'taxability', fa.taxability,
        'routing_kind', fa.routing_kind,
        'destination_label', fa.destination_label,
        'pay_channel', fa.pay_channel,
        'payee_entity_kind', case when fa.is_candidate_directed_oneoff_payout then 'CANDIDATE' when fa.routing_kind = 'UMBRELLA_COMPANY' then 'UMBRELLA' else 'CANDIDATE' end,
        'beneficiary_name', fa.beneficiary_name,
        'masked_bank_account', fa.masked_bank_account,
        'bank_details_hash', fa.bank_details_hash,
        'appears_on_umbrella_remittance', fa.appears_on_umbrella_remittance,
        'generates_candidate_payment_advice', fa.generates_candidate_payment_advice,
        'payee_entity_id', case
          when fa.is_candidate_directed_oneoff_payout then (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = fa.pay_batch_candidate_id
            limit 1
          )
          when fa.routing_kind = 'UMBRELLA_COMPANY' then case when fa.umbrella_id is null then null else fa.umbrella_id::text end
          else (
            select pbc2.candidate_id::text
            from public.pay_batch_candidates pbc2
            where pbc2.id = fa.pay_batch_candidate_id
            limit 1
          )
        end
      )
    ) as payout_instruction_snapshot_json,
    case
      when fa.pay_channel <> 'PAYE' then 'NONE'
      when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_ADD'
      when fa.classification in ('REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum, 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum) then 'NET_ADD'
      when coalesce(nullif(fa.paye_treatment,''), 'NET_ADD') in ('GROSS_ADD','NET_ADD','GROSS_DEDUCT','NET_DEDUCT','NONE') then coalesce(nullif(fa.paye_treatment,''), 'NET_ADD')
      else 'NET_ADD'
    end as paye_treatment
  from final_alloc fa
  where fa.take_target_ex > 0;

  get diagnostics v_rows_ins_underpayment_payout_items = row_count;


v_stage := 'STAGE_16AA_APPLY_OVERPAYMENT_RECOVERY';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

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
    frozen_target_amount_inc_vat
  )
  with selected_case_rows_raw as (
    select
      pbc.id as pay_batch_candidate_id,
      spr.candidate_id,
      spr.finance_case_id,
      upper(coalesce(nullif(spr.pay_channel,''), v_scope)) as pay_channel,
      upper(coalesce(nullif(spr.paye_treatment,''), 'NONE')) as paye_treatment,
      case when upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = 'UMBRELLA' then c.umbrella_id else null::uuid end as umbrella_id,
      round(abs(coalesce(spr.preview_amount_ex_vat, 0)), 2)::numeric(12,2) as row_due_amount_ex_vat,
      coalesce(spr.case_components_json, '[]'::jsonb) as case_components_json,
      spr.preview_row_id
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = spr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = spr.candidate_id
    where spr.draftable = true
      and spr.line_type = 'OVERPAYMENT_RECOVERY'
      and p_operation_id is null
      and spr.finance_case_id is not null
      and upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = v_scope
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
      and abs(coalesce(spr.preview_amount_ex_vat, 0)) > 0
  ), selected_case_row_caps as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      round(sum(scr.row_due_amount_ex_vat), 2)::numeric(12,2) as selected_row_due_amount_ex_vat
    from selected_case_rows_raw scr
    group by
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id
  ), selected_case_component_rows_raw as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      scr.row_due_amount_ex_vat,
      scr.preview_row_id,
      comp.comp_json,
      coalesce(
        nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''),
        nullif(btrim(coalesce(comp.comp_json->>'component_fingerprint','')), ''),
        md5(comp.comp_json::text)
      ) as component_identity,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'target_pay_ex_vat')::numeric)
          else scr.row_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as component_due_amount_ex_vat,
      row_number() over (
        partition by scr.finance_case_id,
          coalesce(
            nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''),
            nullif(btrim(coalesce(comp.comp_json->>'component_fingerprint','')), ''),
            md5(comp.comp_json::text)
          )
        order by scr.preview_row_id
      ) as component_dedupe_rank
    from selected_case_rows_raw scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
  ), selected_case_component_rows_ranked as (
    select
      component_row.*,
      coalesce(
        sum(component_row.component_due_amount_ex_vat) over (
          partition by component_row.finance_case_id, component_row.preview_row_id
          order by component_row.component_identity
          rows between unbounded preceding and 1 preceding
        ),
        0
      )::numeric(12,2) as prior_component_due_amount_ex_vat
    from selected_case_component_rows_raw component_row
    where component_row.component_dedupe_rank = 1
  ), selected_case_component_rows as (
    select
      ranked_row.pay_batch_candidate_id,
      ranked_row.candidate_id,
      ranked_row.finance_case_id,
      ranked_row.pay_channel,
      ranked_row.paye_treatment,
      ranked_row.umbrella_id,
      ranked_row.row_due_amount_ex_vat,
      ranked_row.preview_row_id,
      ranked_row.component_identity,
      greatest(
        least(
          ranked_row.component_due_amount_ex_vat,
          greatest(
            ranked_row.row_due_amount_ex_vat - ranked_row.prior_component_due_amount_ex_vat,
            0
          )
        ),
        0
      )::numeric(12,2) as component_due_amount_ex_vat,
      jsonb_strip_nulls(
        ranked_row.comp_json
        || jsonb_build_object(
          'uncapped_preview_due_amount_ex_vat', round(ranked_row.component_due_amount_ex_vat, 2),
          'preview_due_amount_ex_vat', round(
            greatest(
              least(
                ranked_row.component_due_amount_ex_vat,
                greatest(
                  ranked_row.row_due_amount_ex_vat - ranked_row.prior_component_due_amount_ex_vat,
                  0
                )
              ),
              0
            ),
            2
          ),
          'allocated_source_due_amount_ex_vat', round(
            case
              when coalesce(ranked_row.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
               and coalesce(ranked_row.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
               and abs((ranked_row.comp_json->>'preview_due_amount_ex_vat')::numeric) > 0
              then
                abs((ranked_row.comp_json->>'allocated_source_due_amount_ex_vat')::numeric)
                * greatest(
                    least(
                      ranked_row.component_due_amount_ex_vat,
                      greatest(
                        ranked_row.row_due_amount_ex_vat - ranked_row.prior_component_due_amount_ex_vat,
                        0
                      )
                    ),
                    0
                  )
                / abs((ranked_row.comp_json->>'preview_due_amount_ex_vat')::numeric)
              else greatest(
                least(
                  ranked_row.component_due_amount_ex_vat,
                  greatest(
                    ranked_row.row_due_amount_ex_vat - ranked_row.prior_component_due_amount_ex_vat,
                    0
                  )
                ),
                0
              )
            end,
            2
          )
        )
      ) as comp_json
    from selected_case_component_rows_ranked ranked_row
  ), selected_case_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      least(
        round(sum(scr.component_due_amount_ex_vat), 2),
        max(src.selected_row_due_amount_ex_vat)
      )::numeric(12,2) as due_amount_ex_vat,
      coalesce(jsonb_agg(scr.comp_json order by scr.preview_row_id, scr.component_identity), '[]'::jsonb) as case_components_json
    from selected_case_component_rows scr
    join selected_case_row_caps src
      on src.pay_batch_candidate_id = scr.pay_batch_candidate_id
     and src.candidate_id = scr.candidate_id
     and src.finance_case_id = scr.finance_case_id
     and src.pay_channel = scr.pay_channel
     and src.paye_treatment = scr.paye_treatment
     and src.umbrella_id is not distinct from scr.umbrella_id
    where round(coalesce(scr.component_due_amount_ex_vat, 0), 2) > 0
    group by
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id
  ),
  component_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      scr.due_amount_ex_vat,
      comp.comp_json,
      case
        when nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), '') is null then null::uuid
        else (comp.comp_json->>'finance_component_id')::uuid
      end as finance_component_id,
      nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), '') as component_key_type,
      coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), 'TOTAL') as component_key_value,
      case
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        else null::public.pay_finance_component_classification_enum
      end as classification,
      upper(btrim(coalesce(comp.comp_json->>'source_pay_method',''))) as source_pay_method,
      coalesce(comp.comp_json->'source_basis_json', '{}'::jsonb) as source_basis_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_payload_json') = 'object' then comp.comp_json->'saved_resolution_payload_json'
        else null::jsonb
      end as saved_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_result_json') = 'object' then comp.comp_json->'saved_resolution_result_json'
        else null::jsonb
      end as saved_resolution_result_json,
      nullif(btrim(coalesce(comp.comp_json->>'saved_resolution_mode','')), '') as saved_resolution_mode_text,
      round(
        case
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as remaining_source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as allocated_source_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as preview_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'target_pay_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as target_pay_ex_vat,
      case
        when coalesce(comp.comp_json->>'source_units','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_units')::numeric, 6)
        else null::numeric
      end as source_units,
      case
        when coalesce(comp.comp_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_rate')::numeric, 2)
        else null::numeric
      end as source_rate,
      case
        when coalesce(comp.comp_json->>'target_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_rate')::numeric, 2)
        else null::numeric
      end as target_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_rate')::numeric, 2)
        else null::numeric
      end as source_charge_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as source_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_margin_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_margin_ex_vat')::numeric, 2)
        else null::numeric
      end as target_margin_ex_vat,
      case
        when coalesce(comp.comp_json->>'margin_delta_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'margin_delta_ex_vat')::numeric, 2)
        else null::numeric
      end as margin_delta_ex_vat,
      row_number() over (
        partition by scr.finance_case_id
        order by
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''), md5(comp.comp_json::text)),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), ''), ''),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), '')
      ) as component_rn,
      count(*) over (partition by scr.finance_case_id) as component_ct,
      round(sum(
        round(
          case
            when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
            else 0
          end,
          2
        )
      ) over (partition by scr.finance_case_id), 2)::numeric(12,2) as component_preview_due_sum
    from selected_case_rows scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
      and round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      ) > 0
  ),
  normalized as (
    select
      cr.*,
      round(
        case
          when cr.component_ct = 1 then cr.due_amount_ex_vat
          when cr.component_rn = 1 then greatest(cr.due_amount_ex_vat - (coalesce(cr.component_preview_due_sum, 0) - coalesce(cr.preview_due_amount_ex_vat, 0)), 0)
          else cr.preview_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as take_target_ex
    from component_rows cr
  ),
  final_alloc as (
    select
      n.pay_batch_candidate_id,
      n.finance_case_id,
      n.pay_channel,
      n.paye_treatment,
      n.umbrella_id,
      n.finance_component_id,
      n.component_key_type,
      n.component_key_value,
      n.classification,
      n.source_pay_method,
      n.source_basis_json,
      case
        when n.saved_resolution_mode_text is not null then n.saved_resolution_mode_text::public.pay_finance_component_resolution_mode_enum
        when n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
         and upper(coalesce(n.source_pay_method,'')) <> upper(coalesce(n.pay_channel,''))
        then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
        else null::public.pay_finance_component_resolution_mode_enum
      end as frozen_resolution_mode,
      case
        when jsonb_typeof(n.saved_resolution_payload_json) = 'object' then jsonb_strip_nulls(
          n.saved_resolution_payload_json
          || jsonb_build_object(
            'target_pay_method', n.pay_channel,
            'target_units', n.source_units,
            'suggested_target_rate', n.target_rate,
            'source_rate', n.source_rate,
            'source_charge_rate', n.source_charge_rate
          )
        )
        else jsonb_strip_nulls(jsonb_build_object(
          'target_pay_method', n.pay_channel,
          'target_units', n.source_units,
          'suggested_target_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate
        ))
      end as frozen_resolution_payload_json,
      round(
        case
          when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
          when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
          else 0
        end,
        2
      )::numeric(12,2) as take_source_amount,
      round(n.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as take_target_vat,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
          else round(n.take_target_ex, 2)
        end,
        2
      )::numeric(12,2) as take_target_inc,
      jsonb_strip_nulls(
        coalesce(n.comp_json, '{}'::jsonb)
        || jsonb_build_object(
          'frozen_target_pay_method', n.pay_channel,
          'frozen_source_amount', round(
            case
              when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
              when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
              else 0
            end,
            2
          ),
          'frozen_target_amount_ex_vat', round(-n.take_target_ex, 2),
          'frozen_target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'frozen_target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'selected_preview_due_amount_ex_vat', round(n.due_amount_ex_vat, 2),
          'component_preview_due_amount_ex_vat', round(n.take_target_ex, 2)
        )
      ) as frozen_component_snapshot_json,
      jsonb_strip_nulls(
        coalesce(n.saved_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'target_pay_method', n.pay_channel,
          'target_amount_ex_vat', round(n.take_target_ex, 2),
          'target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'replacement_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate,
          'source_charge_ex_vat', n.source_charge_ex_vat,
          'target_charge_ex_vat', n.target_charge_ex_vat,
          'target_margin_ex_vat', n.target_margin_ex_vat,
          'margin_delta_ex_vat', n.margin_delta_ex_vat
        )
      ) as frozen_resolution_result_json
    from normalized n
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = n.umbrella_id
    where n.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    fa.pay_batch_candidate_id,
    'OVERPAYMENT_RECOVERY' as item_type,
    null::uuid as timesheet_id,
    null::text as segment_key,
    ('advance:' || fa.finance_case_id::text) as source_ref,
    (-fa.take_target_ex)::numeric(12,2) as amount_ex_vat,
    (-fa.take_target_vat)::numeric(12,2) as amount_vat,
    (-fa.take_target_inc)::numeric(12,2) as amount_inc_vat,
    v_week_start as repayment_week_start,
    fa.pay_channel as pay_channel,
    fa.umbrella_id as umbrella_id,
    (fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(fa.source_pay_method,'')) <> upper(coalesce(fa.pay_channel,''))) as is_mismatch,
    false as is_voided,
    v_now_utc as created_at,
    v_now_utc as updated_at,
    fa.finance_case_id,
    null::uuid as reservation_id,
    case
      when fa.pay_channel <> 'PAYE' then 'NONE'
      when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_DEDUCT'
      when fa.classification in ('NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum, 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) then 'NET_DEDUCT'
      else coalesce(nullif(fa.paye_treatment,''), 'NET_DEDUCT')
    end as paye_treatment,
    fa.finance_component_id,
    fa.frozen_component_snapshot_json,
    fa.component_key_type,
    fa.component_key_value,
    fa.classification,
    fa.source_basis_json,
    fa.source_pay_method,
    fa.pay_channel,
    fa.frozen_resolution_mode,
    fa.frozen_resolution_payload_json,
    fa.frozen_resolution_result_json,
    fa.take_source_amount,
    round(-fa.take_target_ex, 2),
    round(-fa.take_target_vat, 2),
    round(-fa.take_target_inc, 2)
  from final_alloc fa
  where fa.take_target_ex > 0;

  get diagnostics v_rows_ins_overpayment_recovery_items = row_count;

  IF p_operation_id IS NOT NULL THEN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'allocation_source_key', plan_row.allocation_source_key,
             'frozen_plan_digest', allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}',
             'current_plan_digest', plan_row.plan_digest
           ) ORDER BY plan_row.allocation_source_key), '[]'::jsonb)
    INTO v_operation_plan_drift_details
    FROM private.pay_workbench_draft_finance_item_plan_v1(
      p_operation_id,
      p_candidate_scope_ids
    ) AS plan_row
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.operation_id = plan_row.operation_id
     AND allocation_row.operation_source_key = plan_row.allocation_source_key
    WHERE plan_row.pay_channel = v_scope
      AND NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}', '')
          IS DISTINCT FROM plan_row.plan_digest;

    IF jsonb_array_length(COALESCE(v_operation_plan_drift_details, '[]'::jsonb)) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS',
        'code', 'DRAFT_FINANCE_ITEM_PLAN_DRIFT',
        'message', 'Frozen Draft finance-item plan changed before materialisation',
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_scope,
        'mismatches', v_operation_plan_drift_details
      )::text;
    END IF;

    INSERT INTO public.pay_batch_items (
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
      operation_source_key
    )
    WITH operation_plan AS (
      SELECT plan_row.*
      FROM private.pay_workbench_draft_finance_item_plan_v1(
        p_operation_id,
        p_candidate_scope_ids
      ) AS plan_row
      WHERE plan_row.planned_item_type = 'OVERPAYMENT_RECOVERY'
        AND plan_row.pay_channel = v_scope
    ), operation_plan_context AS (
      SELECT
        operation_plan.*,
        pay_batch_candidate.id AS pay_batch_candidate_id,
        candidate_context.umbrella_id,
        COALESCE(operation_plan.plan_basis_json->'finance_component', '{}'::jsonb) AS component_json,
        CASE
          WHEN UPPER(BTRIM(COALESCE(operation_plan.plan_basis_json#>>'{finance_component,classification}', ''))) = 'TAXABLE_CHANNEL_SENSITIVE'
            THEN 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          WHEN UPPER(BTRIM(COALESCE(operation_plan.plan_basis_json#>>'{finance_component,classification}', ''))) = 'REIMBURSEMENT_GROSS_FIXED'
            THEN 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
          WHEN UPPER(BTRIM(COALESCE(operation_plan.plan_basis_json#>>'{finance_component,classification}', ''))) = 'NET_PAY_FIXED_RECOVERY'
            THEN 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
          ELSE NULL::public.pay_finance_component_classification_enum
        END AS classification
      FROM operation_plan
      JOIN public.pay_batch_candidates AS pay_batch_candidate
        ON pay_batch_candidate.pay_batch_id = v_batch_id
       AND pay_batch_candidate.candidate_id = operation_plan.candidate_id
      JOIN pg_temp.tmp_pay_build_candidates_ctx AS candidate_context
        ON candidate_context.id = operation_plan.candidate_id
    ), materialisation AS (
      SELECT
        operation_plan_context.*,
        CASE
          WHEN operation_plan_context.pay_channel = 'UMBRELLA'
           AND operation_plan_context.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            THEN ROUND((public._pay_umbrella_vat_calc(
                   ABS(operation_plan_context.planned_item_amount),
                   v_vat_rate_pct,
                   COALESCE(umbrella_context.vat_chargeable, false)
                 )->>'vat')::numeric, 2)
          ELSE 0::numeric
        END AS planned_vat_abs
      FROM operation_plan_context
      LEFT JOIN pg_temp.tmp_pay_build_umbrellas_ctx AS umbrella_context
        ON umbrella_context.id = operation_plan_context.umbrella_id
    )
    SELECT
      gen_random_uuid(),
      materialisation.pay_batch_candidate_id,
      materialisation.planned_item_type,
      NULL::uuid,
      NULL::text,
      'advance:' || materialisation.finance_case_id::text,
      ROUND(materialisation.planned_item_amount, 2)::numeric(12,2),
      (-materialisation.planned_vat_abs)::numeric(12,2),
      ROUND(materialisation.planned_item_amount - materialisation.planned_vat_abs, 2)::numeric(12,2),
      v_week_start,
      materialisation.pay_channel,
      CASE WHEN materialisation.pay_channel = 'UMBRELLA' THEN materialisation.umbrella_id ELSE NULL::uuid END,
      (
        materialisation.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND UPPER(COALESCE(materialisation.component_json->>'source_pay_method', '')) <> materialisation.pay_channel
      ),
      false,
      v_now_utc,
      v_now_utc,
      materialisation.finance_case_id,
      NULL::uuid,
      CASE
        WHEN materialisation.pay_channel <> 'PAYE' THEN 'NONE'
        WHEN materialisation.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum THEN 'GROSS_DEDUCT'
        WHEN materialisation.classification IN (
          'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
          'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        ) THEN 'NET_DEDUCT'
        ELSE COALESCE(NULLIF(BTRIM(materialisation.plan_basis_json#>>'{line,paye_treatment}'), ''), 'NET_DEDUCT')
      END,
      materialisation.finance_component_id,
      jsonb_strip_nulls(
        materialisation.component_json
        || jsonb_build_object(
          'draft_finance_item_plan_key', materialisation.planned_item_key,
          'draft_finance_item_plan_digest', materialisation.plan_digest,
          'frozen_target_pay_method', materialisation.pay_channel,
          'frozen_source_amount', ROUND(ABS(materialisation.contribution_amount), 2),
          'frozen_target_amount_ex_vat', ROUND(materialisation.planned_item_amount, 2),
          'frozen_target_amount_vat', ROUND(-materialisation.planned_vat_abs, 2),
          'frozen_target_amount_inc_vat', ROUND(materialisation.planned_item_amount - materialisation.planned_vat_abs, 2),
          'component_preview_due_amount_ex_vat', ROUND(ABS(materialisation.contribution_amount), 2)
        )
      ),
      NULLIF(BTRIM(materialisation.component_json->>'component_key_type'), ''),
      COALESCE(NULLIF(BTRIM(materialisation.component_json->>'component_key_value'), ''), 'TOTAL'),
      materialisation.classification,
      COALESCE(materialisation.component_json->'source_basis_json', '{}'::jsonb),
      UPPER(NULLIF(BTRIM(materialisation.component_json->>'source_pay_method'), '')),
      materialisation.pay_channel,
      CASE
        WHEN NULLIF(BTRIM(materialisation.component_json->>'saved_resolution_mode'), '') IS NULL THEN NULL::public.pay_finance_component_resolution_mode_enum
        ELSE (materialisation.component_json->>'saved_resolution_mode')::public.pay_finance_component_resolution_mode_enum
      END,
      CASE
        WHEN jsonb_typeof(materialisation.component_json->'saved_resolution_payload_json') = 'object'
          THEN materialisation.component_json->'saved_resolution_payload_json'
        ELSE NULL::jsonb
      END,
      CASE
        WHEN jsonb_typeof(materialisation.component_json->'saved_resolution_result_json') = 'object'
          THEN materialisation.component_json->'saved_resolution_result_json'
        ELSE NULL::jsonb
      END,
      ROUND(COALESCE(
        CASE
          WHEN COALESCE(materialisation.component_json->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN ABS((materialisation.component_json->>'allocated_source_due_amount_ex_vat')::numeric)
          ELSE NULL::numeric
        END,
        ABS(materialisation.contribution_amount)
      ), 2),
      ROUND(materialisation.planned_item_amount, 2),
      ROUND(-materialisation.planned_vat_abs, 2),
      ROUND(materialisation.planned_item_amount - materialisation.planned_vat_abs, 2),
      materialisation.planned_item_key
    FROM materialisation
    WHERE materialisation.planned_item_amount < 0
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS existing_planned_item
        WHERE existing_planned_item.pay_batch_candidate_id = materialisation.pay_batch_candidate_id
          AND existing_planned_item.operation_source_key = materialisation.planned_item_key
          AND COALESCE(existing_planned_item.is_voided, false) = false
      );

    GET DIAGNOSTICS v_rows_ins_operation_planned_overpayment_items = ROW_COUNT;
    v_rows_ins_overpayment_recovery_items :=
      COALESCE(v_rows_ins_overpayment_recovery_items, 0)
      + COALESCE(v_rows_ins_operation_planned_overpayment_items, 0);
  END IF;


  update public.pay_batch_candidates pbc
  set
    overpayment_recovery_taken = coalesce((
      select round(sum(-pbi.amount_ex_vat), 2)
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = pbc.id
        and pbi.is_voided = false
        and pbi.item_type = 'OVERPAYMENT_RECOVERY'
    ), 0)::numeric(12,2),
    updated_at = v_now_utc
  where pbc.pay_batch_id = v_batch_id;

  get diagnostics v_rows_upd_candidates_overpayment_recovery_taken = row_count;

  v_rows_ins_debt_items := 0;


v_stage := 'STAGE_16B_APPLY_MANUAL_DEBT_RECOVERY';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

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
    frozen_target_amount_inc_vat
  )
  with selected_case_rows as (
    select distinct on (spr.finance_case_id)
      pbc.id as pay_batch_candidate_id,
      spr.candidate_id,
      spr.finance_case_id,
      upper(coalesce(nullif(spr.pay_channel,''), v_scope)) as pay_channel,
      upper(coalesce(nullif(spr.paye_treatment,''), 'NONE')) as paye_treatment,
      exists (
        select 1
        from public.pay_batch_paye_net_inputs pni_md_exists
        where pni_md_exists.pay_batch_candidate_id = pbc.id
        limit 1
      ) as has_paye_net_input,
      case when upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = 'UMBRELLA' then c.umbrella_id else null::uuid end as umbrella_id,
      round(abs(coalesce(spr.preview_amount_ex_vat, 0)), 2)::numeric(12,2) as due_amount_ex_vat,
      coalesce(spr.case_components_json, '[]'::jsonb) as case_components_json
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = spr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = spr.candidate_id
    where spr.draftable = true
      and spr.line_type = 'MANUAL_DEBT_RECOVERY'
      and spr.finance_case_id is not null
      and upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = v_scope
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
      and abs(coalesce(spr.preview_amount_ex_vat, 0)) > 0
    order by spr.finance_case_id, spr.preview_row_id
  ),
  component_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.has_paye_net_input,
      scr.umbrella_id,
      scr.due_amount_ex_vat,
      comp.comp_json,
      case
        when nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), '') is null then null::uuid
        else (comp.comp_json->>'finance_component_id')::uuid
      end as finance_component_id,
      nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), '') as component_key_type,
      coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), 'TOTAL') as component_key_value,
      case
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        else null::public.pay_finance_component_classification_enum
      end as classification,
      upper(btrim(coalesce(comp.comp_json->>'source_pay_method',''))) as source_pay_method,
      coalesce(comp.comp_json->'source_basis_json', '{}'::jsonb) as source_basis_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_payload_json') = 'object' then comp.comp_json->'saved_resolution_payload_json'
        else null::jsonb
      end as saved_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_result_json') = 'object' then comp.comp_json->'saved_resolution_result_json'
        else null::jsonb
      end as saved_resolution_result_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_payload_json') = 'object' then comp.comp_json->'suggested_resolution_payload_json'
        else null::jsonb
      end as preview_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'suggested_resolution_result_json') = 'object' then comp.comp_json->'suggested_resolution_result_json'
        else null::jsonb
      end as preview_resolution_result_json,
      nullif(btrim(coalesce(comp.comp_json->>'saved_resolution_mode','')), '') as saved_resolution_mode_text,
      nullif(btrim(coalesce(comp.comp_json->'suggested_resolution_payload_json'->>'resolution_mode','')), '') as preview_resolution_mode_text,
      round(
        case
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as remaining_source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as allocated_source_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as preview_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->'suggested_resolution_result_json'->>'target_amount_ex_vat')::numeric
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'target_pay_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as target_pay_ex_vat,
      case
        when coalesce(comp.comp_json->>'source_units','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_units')::numeric, 6)
        else null::numeric
      end as source_units,
      case
        when coalesce(comp.comp_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_rate')::numeric, 2)
        else null::numeric
      end as source_rate,
      case
        when coalesce(comp.comp_json->>'target_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_rate')::numeric, 2)
        else null::numeric
      end as target_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_rate')::numeric, 2)
        else null::numeric
      end as source_charge_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as source_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_margin_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_margin_ex_vat')::numeric, 2)
        else null::numeric
      end as target_margin_ex_vat,
      case
        when coalesce(comp.comp_json->>'margin_delta_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'margin_delta_ex_vat')::numeric, 2)
        else null::numeric
      end as margin_delta_ex_vat,
      row_number() over (
        partition by scr.finance_case_id
        order by
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''), md5(comp.comp_json::text)),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), ''), ''),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), '')
      ) as component_rn,
      count(*) over (partition by scr.finance_case_id) as component_ct,
      round(sum(
        round(
          case
            when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
            else 0
          end,
          2
        )
      ) over (partition by scr.finance_case_id), 2)::numeric(12,2) as component_preview_due_sum
    from selected_case_rows scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
      and round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      ) > 0
  ),
  normalized as (
    select
      cr.*,
      round(
        case
          when cr.component_ct = 1 then cr.due_amount_ex_vat
          when cr.component_rn = 1 then greatest(cr.due_amount_ex_vat - (coalesce(cr.component_preview_due_sum, 0) - coalesce(cr.preview_due_amount_ex_vat, 0)), 0)
          else cr.preview_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as take_target_ex
    from component_rows cr
  ),
  final_alloc as (
    select
      n.pay_batch_candidate_id,
      n.finance_case_id,
      n.pay_channel,
      n.paye_treatment,
      n.has_paye_net_input,
      n.umbrella_id,
      n.finance_component_id,
      n.component_key_type,
      n.component_key_value,
      n.classification,
      n.source_pay_method,
      n.source_basis_json,
      case
        when n.saved_resolution_mode_text is not null then n.saved_resolution_mode_text::public.pay_finance_component_resolution_mode_enum
        else null::public.pay_finance_component_resolution_mode_enum
      end as frozen_resolution_mode,
      case
        when jsonb_typeof(n.saved_resolution_payload_json) = 'object' then jsonb_strip_nulls(
          n.saved_resolution_payload_json
          || jsonb_build_object(
            'target_pay_method', n.pay_channel,
            'target_units', n.source_units,
            'suggested_target_rate', n.target_rate,
            'source_rate', n.source_rate,
            'source_charge_rate', n.source_charge_rate
          )
        )
        else jsonb_strip_nulls(jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_units', n.source_units,
          'suggested_target_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate
        ))
      end as frozen_resolution_payload_json,
      round(
        case
          when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
          when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
          else 0
        end,
        2
      )::numeric(12,2) as take_source_amount,
      round(n.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as take_target_vat,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
          else round(n.take_target_ex, 2)
        end,
        2
      )::numeric(12,2) as take_target_inc,
      jsonb_strip_nulls(
        coalesce(n.comp_json, '{}'::jsonb)
        || jsonb_build_object(
          'frozen_target_pay_method', n.pay_channel,
          'frozen_resolution_mode_text', n.saved_resolution_mode_text,
          'frozen_source_amount', round(
            case
              when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
              when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
              else 0
            end,
            2
          ),
          'frozen_target_amount_ex_vat', round(n.take_target_ex, 2),
          'frozen_target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'frozen_target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'selected_preview_due_amount_ex_vat', round(n.due_amount_ex_vat, 2),
          'component_preview_due_amount_ex_vat', round(n.take_target_ex, 2)
        )
      ) as frozen_component_snapshot_json,
      jsonb_strip_nulls(
        coalesce(n.saved_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'resolution_mode', n.saved_resolution_mode_text,
          'target_pay_method', n.pay_channel,
          'target_amount_ex_vat', round(n.take_target_ex, 2),
          'target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'replacement_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate,
          'source_charge_ex_vat', n.source_charge_ex_vat,
          'target_charge_ex_vat', n.target_charge_ex_vat,
          'target_margin_ex_vat', n.target_margin_ex_vat,
          'margin_delta_ex_vat', n.margin_delta_ex_vat
        )
      ) as frozen_resolution_result_json
    from normalized n
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = n.umbrella_id
    where n.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    fa.pay_batch_candidate_id,
    'MANUAL_DEBT_RECOVERY' as item_type,
    null::uuid as timesheet_id,
    null::text as segment_key,
    ('advance:' || fa.finance_case_id::text) as source_ref,
    (-fa.take_target_ex)::numeric(12,2) as amount_ex_vat,
    (-fa.take_target_vat)::numeric(12,2) as amount_vat,
    (-fa.take_target_inc)::numeric(12,2) as amount_inc_vat,
    v_week_start as repayment_week_start,
    fa.pay_channel as pay_channel,
    fa.umbrella_id as umbrella_id,
    (fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(fa.source_pay_method,'')) <> upper(coalesce(fa.pay_channel,''))) as is_mismatch,
    false as is_voided,
    v_now_utc as created_at,
    v_now_utc as updated_at,
    fa.finance_case_id,
    null::uuid as reservation_id,
    case
      when fa.pay_channel <> 'PAYE' then 'NONE'
      when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_DEDUCT'
      when fa.classification in ('NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum, 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) then 'NET_DEDUCT'
      else coalesce(nullif(fa.paye_treatment,''), 'NET_DEDUCT')
    end as paye_treatment,
    fa.finance_component_id,
    fa.frozen_component_snapshot_json,
    fa.component_key_type,
    fa.component_key_value,
    fa.classification,
    fa.source_basis_json,
    fa.source_pay_method,
    fa.pay_channel,
    fa.frozen_resolution_mode,
    fa.frozen_resolution_payload_json,
    fa.frozen_resolution_result_json,
    fa.take_source_amount,
    round(-fa.take_target_ex, 2),
    round(-fa.take_target_vat, 2),
    round(-fa.take_target_inc, 2)
  from final_alloc fa
  where fa.take_target_ex > 0
    and not (
      upper(coalesce(fa.pay_channel, '')) = 'PAYE'
      and coalesce(fa.has_paye_net_input, false) = false
      and (
        case
          when fa.pay_channel <> 'PAYE' then 'NONE'
          when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_DEDUCT'
          when fa.classification in ('NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum, 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) then 'NET_DEDUCT'
          else coalesce(nullif(fa.paye_treatment,''), 'NET_DEDUCT')
        end
      ) = 'NET_DEDUCT'
    );

  get diagnostics v_rows_ins_debt_items = row_count;

v_stage := 'STAGE_16C_APPLY_PAYMENT_ADVANCE_REPAYMENTS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

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
    frozen_target_amount_inc_vat
  )
  with selected_case_rows as (
    select distinct on (spr.finance_case_id)
      pbc.id as pay_batch_candidate_id,
      spr.candidate_id,
      spr.finance_case_id,
      upper(coalesce(nullif(spr.pay_channel,''), v_scope)) as pay_channel,
      upper(coalesce(nullif(spr.paye_treatment,''), 'NONE')) as paye_treatment,
      case when upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = 'UMBRELLA' then c.umbrella_id else null::uuid end as umbrella_id,
      round(abs(coalesce(spr.preview_amount_ex_vat, 0)), 2)::numeric(12,2) as due_amount_ex_vat,
      coalesce(spr.case_components_json, '[]'::jsonb) as case_components_json
    from pg_temp.tmp_pay_build_selected_preview_rows spr
    join public.pay_batch_candidates pbc
      on pbc.pay_batch_id = v_batch_id
     and pbc.candidate_id = spr.candidate_id
    join pg_temp.tmp_pay_build_candidates_ctx c
      on c.id = spr.candidate_id
    where spr.draftable = true
      and spr.line_type = 'PAYMENT_ADVANCE_REPAYMENT'
      and spr.finance_case_id is not null
      and upper(coalesce(nullif(spr.pay_channel,''), v_scope)) = v_scope
      and spr.candidate_id = any(v_candidate_ids)
      and (v_client_filter_single is null or spr.client_id = v_client_filter_single)
      and abs(coalesce(spr.preview_amount_ex_vat, 0)) > 0
    order by spr.finance_case_id, spr.preview_row_id
  ),
  component_rows as (
    select
      scr.pay_batch_candidate_id,
      scr.candidate_id,
      scr.finance_case_id,
      scr.pay_channel,
      scr.paye_treatment,
      scr.umbrella_id,
      scr.due_amount_ex_vat,
      comp.comp_json,
      case
        when nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), '') is null then null::uuid
        else (comp.comp_json->>'finance_component_id')::uuid
      end as finance_component_id,
      nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), '') as component_key_type,
      coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), 'TOTAL') as component_key_value,
      case
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        when upper(btrim(coalesce(comp.comp_json->>'classification',''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        else null::public.pay_finance_component_classification_enum
      end as classification,
      upper(btrim(coalesce(comp.comp_json->>'source_pay_method',''))) as source_pay_method,
      coalesce(comp.comp_json->'source_basis_json', '{}'::jsonb) as source_basis_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_payload_json') = 'object' then comp.comp_json->'saved_resolution_payload_json'
        else null::jsonb
      end as saved_resolution_payload_json,
      case
        when jsonb_typeof(comp.comp_json->'saved_resolution_result_json') = 'object' then comp.comp_json->'saved_resolution_result_json'
        else null::jsonb
      end as saved_resolution_result_json,
      nullif(btrim(coalesce(comp.comp_json->>'saved_resolution_mode','')), '') as saved_resolution_mode_text,
      round(
        case
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'remaining_source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'remaining_source_amount')::numeric
          when coalesce(comp.comp_json->>'source_amount','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'source_amount')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as remaining_source_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'allocated_source_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (comp.comp_json->>'allocated_source_due_amount_ex_vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as allocated_source_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as preview_due_amount_ex_vat,
      round(
        case
          when coalesce(comp.comp_json->>'target_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'target_pay_ex_vat')::numeric)
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      )::numeric(12,2) as target_pay_ex_vat,
      case
        when coalesce(comp.comp_json->>'source_units','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_units')::numeric, 6)
        else null::numeric
      end as source_units,
      case
        when coalesce(comp.comp_json->>'source_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_rate')::numeric, 2)
        else null::numeric
      end as source_rate,
      case
        when coalesce(comp.comp_json->>'target_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_rate')::numeric, 2)
        else null::numeric
      end as target_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_rate','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_rate')::numeric, 2)
        else null::numeric
      end as source_charge_rate,
      case
        when coalesce(comp.comp_json->>'source_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'source_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as source_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_charge_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_charge_ex_vat')::numeric, 2)
        else null::numeric
      end as target_charge_ex_vat,
      case
        when coalesce(comp.comp_json->>'target_margin_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'target_margin_ex_vat')::numeric, 2)
        else null::numeric
      end as target_margin_ex_vat,
      case
        when coalesce(comp.comp_json->>'margin_delta_ex_vat','') ~ '^-?\d+(\.\d+)?$' then round((comp.comp_json->>'margin_delta_ex_vat')::numeric, 2)
        else null::numeric
      end as margin_delta_ex_vat,
      row_number() over (
        partition by scr.finance_case_id
        order by
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'finance_component_id','')), ''), md5(comp.comp_json::text)),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_type','')), ''), ''),
          coalesce(nullif(btrim(coalesce(comp.comp_json->>'component_key_value','')), ''), '')
      ) as component_rn,
      count(*) over (partition by scr.finance_case_id) as component_ct,
      round(sum(
        round(
          case
            when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
            else 0
          end,
          2
        )
      ) over (partition by scr.finance_case_id), 2)::numeric(12,2) as component_preview_due_sum
    from selected_case_rows scr
    cross join lateral jsonb_array_elements(coalesce(scr.case_components_json, '[]'::jsonb)) as comp(comp_json)
    where jsonb_typeof(comp.comp_json) = 'object'
      and round(
        case
          when coalesce(comp.comp_json->>'preview_due_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then abs((comp.comp_json->>'preview_due_amount_ex_vat')::numeric)
          else 0
        end,
        2
      ) > 0
  ),
  normalized as (
    select
      cr.*,
      round(
        case
          when cr.component_ct = 1 then cr.due_amount_ex_vat
          when cr.component_rn = 1 then greatest(cr.due_amount_ex_vat - (coalesce(cr.component_preview_due_sum, 0) - coalesce(cr.preview_due_amount_ex_vat, 0)), 0)
          else cr.preview_due_amount_ex_vat
        end,
        2
      )::numeric(12,2) as take_target_ex
    from component_rows cr
  ),
  final_alloc as (
    select
      n.pay_batch_candidate_id,
      n.finance_case_id,
      n.pay_channel,
      n.paye_treatment,
      n.umbrella_id,
      n.finance_component_id,
      n.component_key_type,
      n.component_key_value,
      n.classification,
      n.source_pay_method,
      n.source_basis_json,
      case
        when n.saved_resolution_mode_text is not null then n.saved_resolution_mode_text::public.pay_finance_component_resolution_mode_enum
        when n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
         and upper(coalesce(n.source_pay_method,'')) <> upper(coalesce(n.pay_channel,''))
        then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
        else null::public.pay_finance_component_resolution_mode_enum
      end as frozen_resolution_mode,
      case
        when jsonb_typeof(n.saved_resolution_payload_json) = 'object' then jsonb_strip_nulls(
          n.saved_resolution_payload_json
          || jsonb_build_object(
            'target_pay_method', n.pay_channel,
            'target_units', n.source_units,
            'suggested_target_rate', n.target_rate,
            'source_rate', n.source_rate,
            'source_charge_rate', n.source_charge_rate
          )
        )
        else jsonb_strip_nulls(jsonb_build_object(
          'target_pay_method', n.pay_channel,
          'target_units', n.source_units,
          'suggested_target_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate
        ))
      end as frozen_resolution_payload_json,
      round(
        case
          when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
          when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
          else 0
        end,
        2
      )::numeric(12,2) as take_source_amount,
      round(n.take_target_ex, 2)::numeric(12,2) as take_target_ex,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
          else 0
        end,
        2
      )::numeric(12,2) as take_target_vat,
      round(
        case
          when n.pay_channel = 'UMBRELLA'
           and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
          else round(n.take_target_ex, 2)
        end,
        2
      )::numeric(12,2) as take_target_inc,
      jsonb_strip_nulls(
        coalesce(n.comp_json, '{}'::jsonb)
        || jsonb_build_object(
          'frozen_target_pay_method', n.pay_channel,
          'frozen_source_amount', round(
            case
              when n.allocated_source_due_amount_ex_vat > 0 then n.allocated_source_due_amount_ex_vat
              when n.take_target_ex > 0 and n.target_pay_ex_vat > 0 then least(n.remaining_source_amount_ex_vat, round(n.remaining_source_amount_ex_vat * n.take_target_ex / n.target_pay_ex_vat, 2))
              else 0
            end,
            2
          ),
          'frozen_target_amount_ex_vat', round(-n.take_target_ex, 2),
          'frozen_target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'frozen_target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'selected_preview_due_amount_ex_vat', round(n.due_amount_ex_vat, 2),
          'component_preview_due_amount_ex_vat', round(n.take_target_ex, 2)
        )
      ) as frozen_component_snapshot_json,
      jsonb_strip_nulls(
        coalesce(n.saved_resolution_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'target_pay_method', n.pay_channel,
          'target_amount_ex_vat', round(n.take_target_ex, 2),
          'target_amount_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'vat')::numeric
              else 0
            end,
            2
          ),
          'target_amount_inc_vat', round(
            case
              when n.pay_channel = 'UMBRELLA'
               and n.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
              then (public._pay_umbrella_vat_calc(round(n.take_target_ex, 2), v_vat_rate_pct, coalesce(u.vat_chargeable,false))->>'inc')::numeric
              else round(n.take_target_ex, 2)
            end,
            2
          ),
          'replacement_rate', n.target_rate,
          'source_rate', n.source_rate,
          'source_charge_rate', n.source_charge_rate,
          'source_charge_ex_vat', n.source_charge_ex_vat,
          'target_charge_ex_vat', n.target_charge_ex_vat,
          'target_margin_ex_vat', n.target_margin_ex_vat,
          'margin_delta_ex_vat', n.margin_delta_ex_vat
        )
      ) as frozen_resolution_result_json
    from normalized n
    left join pg_temp.tmp_pay_build_umbrellas_ctx u
      on u.id = n.umbrella_id
    where n.take_target_ex > 0
  )
  select
    gen_random_uuid() as id,
    fa.pay_batch_candidate_id,
    'LOAN_REPAYMENT' as item_type,
    null::uuid as timesheet_id,
    null::text as segment_key,
    ('advance:' || fa.finance_case_id::text) as source_ref,
    (-fa.take_target_ex)::numeric(12,2) as amount_ex_vat,
    (-fa.take_target_vat)::numeric(12,2) as amount_vat,
    (-fa.take_target_inc)::numeric(12,2) as amount_inc_vat,
    v_week_start as repayment_week_start,
    fa.pay_channel as pay_channel,
    fa.umbrella_id as umbrella_id,
    (fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum and upper(coalesce(fa.source_pay_method,'')) <> upper(coalesce(fa.pay_channel,''))) as is_mismatch,
    false as is_voided,
    v_now_utc as created_at,
    v_now_utc as updated_at,
    fa.finance_case_id,
    null::uuid as reservation_id,
    case
      when fa.pay_channel <> 'PAYE' then 'NONE'
      when fa.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_DEDUCT'
      when fa.classification in ('NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum, 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) then 'NET_DEDUCT'
      else coalesce(nullif(fa.paye_treatment,''), 'NET_DEDUCT')
    end as paye_treatment,
    fa.finance_component_id,
    fa.frozen_component_snapshot_json,
    fa.component_key_type,
    fa.component_key_value,
    fa.classification,
    fa.source_basis_json,
    fa.source_pay_method,
    fa.pay_channel,
    fa.frozen_resolution_mode,
    fa.frozen_resolution_payload_json,
    fa.frozen_resolution_result_json,
    fa.take_source_amount,
    round(-fa.take_target_ex, 2),
    round(-fa.take_target_vat, 2),
    round(-fa.take_target_inc, 2)
  from final_alloc fa
  where fa.take_target_ex > 0;

  get diagnostics v_rows_ins_loan_items = row_count;


  update public.pay_batch_candidates pbc
  set
    loan_repayment_taken = coalesce((
      select round(sum(-pbi.amount_ex_vat), 2)
      from public.pay_batch_items pbi
      where pbi.pay_batch_candidate_id = pbc.id
        and pbi.is_voided = false
        and pbi.item_type = 'LOAN_REPAYMENT'
    ), 0)::numeric(12,2),
    updated_at = v_now_utc
  where pbc.pay_batch_id = v_batch_id;

  get diagnostics v_rows_upd_candidates_loan = row_count;

v_stage := 'STAGE_16BD_INSERT_DORMANT_RECOVERY_TEMPLATES';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  create temporary table if not exists pg_temp.tmp_pay_build_dormant_recovery_template_stage (
    pay_batch_candidate_id uuid not null,
    candidate_id uuid not null,
    finance_case_id uuid not null,
    recovery_family text not null,
    paye_treatment text null,
    normalized_pay_channel text null,
    umbrella_id uuid null,
    source_ref text null,
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
    frozen_outstanding_amount numeric(12,2) null,
    weekly_due numeric(12,2) null,
    minimum_earnings_threshold numeric null,
    take_home_floor_override numeric null,
    default_take_home_floor numeric null,
    payout_status text null,
    next_due_week_start date null,
    sort_order integer null,
    duplicate_exists boolean not null
  ) on commit drop;
  truncate table pg_temp.tmp_pay_build_dormant_recovery_template_stage;

  insert into pg_temp.tmp_pay_build_dormant_recovery_template_stage (
    pay_batch_candidate_id,
    candidate_id,
    finance_case_id,
    recovery_family,
    paye_treatment,
    normalized_pay_channel,
    umbrella_id,
    source_ref,
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
    frozen_outstanding_amount,
    weekly_due,
    minimum_earnings_threshold,
    take_home_floor_override,
    default_take_home_floor,
    payout_status,
    next_due_week_start,
    sort_order,
    duplicate_exists
  )
  select
    pbc.id as pay_batch_candidate_id,
    tr.candidate_id,
    tr.finance_case_id,
    tr.recovery_family,
    tr.paye_treatment,
    case
      when upper(coalesce(tr.pay_channel, '')) in ('PAYE', 'UMBRELLA') then upper(coalesce(tr.pay_channel, ''))
      when coalesce(btrim(coalesce(tr.pay_channel, '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
      else null::text
    end as normalized_pay_channel,
    tr.umbrella_id,
    coalesce(tr.source_ref, 'advance:' || tr.finance_case_id::text) as source_ref,
    tr.finance_component_id,
    tr.frozen_component_snapshot_json,
    tr.frozen_component_key_type,
    tr.frozen_component_key_value,
    tr.frozen_component_classification,
    tr.frozen_source_basis_json,
    tr.frozen_source_pay_method,
    coalesce(tr.frozen_target_pay_method,
      case
        when upper(coalesce(tr.pay_channel, '')) in ('PAYE', 'UMBRELLA') then upper(coalesce(tr.pay_channel, ''))
        when coalesce(btrim(coalesce(tr.pay_channel, '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
        else null::text
      end
    ) as frozen_target_pay_method,
    tr.frozen_resolution_mode,
    tr.frozen_resolution_payload_json,
    tr.frozen_resolution_result_json,
    tr.frozen_source_amount,
    tr.frozen_outstanding_amount,
    tr.weekly_due,
    tr.minimum_earnings_threshold,
    tr.take_home_floor_override,
    tr.default_take_home_floor,
    tr.payout_status,
    tr.next_due_week_start,
    tr.sort_order,
    exists (
      select 1
      from public.pay_batch_items as pbi_existing
      where pbi_existing.pay_batch_candidate_id = pbc.id
        and pbi_existing.finance_case_id = tr.finance_case_id
        and pbi_existing.item_type = tr.recovery_family
        and (
          (pbi_existing.finance_component_id is null and tr.finance_component_id is null)
          or pbi_existing.finance_component_id = tr.finance_component_id
        )
        and coalesce(pbi_existing.source_ref, '') = coalesce(coalesce(tr.source_ref, 'advance:' || tr.finance_case_id::text), '')
        and upper(coalesce(pbi_existing.pay_channel, '')) = upper(coalesce(
          case
            when upper(coalesce(tr.pay_channel, '')) in ('PAYE', 'UMBRELLA') then upper(coalesce(tr.pay_channel, ''))
            when coalesce(btrim(coalesce(tr.pay_channel, '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
            else null::text
          end,
          ''
        ))
    ) as duplicate_exists
  from pg_temp.tmp_pay_build_recovery_template_rows as tr
  join public.pay_batch_candidates as pbc
    on pbc.pay_batch_id = v_batch_id
   and pbc.candidate_id = tr.candidate_id
  where tr.candidate_id = any(v_candidate_ids)
    and coalesce(
      case
        when upper(coalesce(tr.pay_channel, '')) in ('PAYE', 'UMBRELLA') then upper(coalesce(tr.pay_channel, ''))
        when coalesce(btrim(coalesce(tr.pay_channel, '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
        else null::text
      end,
      ''
    ) = v_scope;

  select count(*)::integer
  into v_rows_staged_recovery_template_rows
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows;

  select count(*)::integer
  into v_rows_staged_manual_debt_template_rows
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY';

  select count(*)::integer
  into v_rows_staged_loan_template_rows
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.recovery_family = 'LOAN_REPAYMENT';

  select count(*)::integer
  into v_rows_staged_overpayment_template_rows
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.recovery_family = 'OVERPAYMENT_RECOVERY';

  select count(*)::integer
  into v_rows_staged_paye_manual_debt_template_rows
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY'
    and stage_rows.normalized_pay_channel = 'PAYE';

  select coalesce(
           jsonb_object_agg(stage_counts.candidate_id::text, to_jsonb(stage_counts.template_count) order by stage_counts.candidate_id::text),
           '{}'::jsonb
         )
  into v_staged_manual_debt_template_rows_by_candidate
  from (
    select stage_rows.candidate_id, count(*)::integer as template_count
    from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
    where stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY'
      and stage_rows.normalized_pay_channel = 'PAYE'
    group by stage_rows.candidate_id
  ) as stage_counts;

  select count(*)::integer
  into v_rows_skipped_dormant_recovery_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = true;

  select count(*)::integer
  into v_rows_skipped_dormant_manual_debt_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = true
    and stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY';

  select count(*)::integer
  into v_rows_skipped_dormant_loan_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = true
    and stage_rows.recovery_family = 'LOAN_REPAYMENT';

  select count(*)::integer
  into v_rows_skipped_dormant_overpayment_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = true
    and stage_rows.recovery_family = 'OVERPAYMENT_RECOVERY';

  insert into public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
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
    stage_rows.pay_batch_candidate_id,
    stage_rows.recovery_family as item_type,
    null::uuid as timesheet_id,
    null::text as segment_key,
    stage_rows.source_ref,
    null::text as description,
    0::numeric(12,2) as amount_ex_vat,
    0::numeric(12,2) as amount_vat,
    0::numeric(12,2) as amount_inc_vat,
    v_week_start as repayment_week_start,
    stage_rows.normalized_pay_channel as pay_channel,
    stage_rows.umbrella_id as umbrella_id,
    (
      stage_rows.frozen_component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      and upper(coalesce(stage_rows.frozen_source_pay_method, '')) <> upper(coalesce(stage_rows.frozen_target_pay_method, stage_rows.normalized_pay_channel, ''))
    ) as is_mismatch,
    true as is_voided,
    v_now_utc as created_at,
    v_now_utc as updated_at,
    stage_rows.finance_case_id,
    null::uuid as reservation_id,
    case
      when stage_rows.normalized_pay_channel <> 'PAYE' then 'NONE'
      when stage_rows.frozen_component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum then 'GROSS_DEDUCT'
      when stage_rows.frozen_component_classification in ('NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum, 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum) then 'NET_DEDUCT'
      else coalesce(stage_rows.paye_treatment, 'NET_DEDUCT')
    end as paye_treatment,
    stage_rows.finance_component_id,
    stage_rows.frozen_component_snapshot_json,
    stage_rows.frozen_component_key_type,
    stage_rows.frozen_component_key_value,
    stage_rows.frozen_component_classification,
    jsonb_strip_nulls(
      coalesce(stage_rows.frozen_source_basis_json, '{}'::jsonb)
      || jsonb_build_object(
        'recovery_family', stage_rows.recovery_family,
        'source_ref', stage_rows.source_ref,
        'pay_channel', stage_rows.normalized_pay_channel,
        'frozen_source_pay_method', stage_rows.frozen_source_pay_method,
        'frozen_target_pay_method', stage_rows.frozen_target_pay_method,
        'frozen_source_amount', stage_rows.frozen_source_amount,
        'frozen_outstanding_amount', stage_rows.frozen_outstanding_amount,
        'weekly_due', stage_rows.weekly_due,
        'minimum_earnings_threshold', stage_rows.minimum_earnings_threshold,
        'take_home_floor_override', stage_rows.take_home_floor_override,
        'default_take_home_floor', stage_rows.default_take_home_floor,
        'payout_status', stage_rows.payout_status,
        'next_due_week_start', case when stage_rows.next_due_week_start is null then null else stage_rows.next_due_week_start::text end,
        'sort_order', stage_rows.sort_order
      )
    ) as frozen_source_basis_json,
    stage_rows.frozen_source_pay_method,
    coalesce(stage_rows.frozen_target_pay_method, stage_rows.normalized_pay_channel) as frozen_target_pay_method,
    stage_rows.frozen_resolution_mode,
    stage_rows.frozen_resolution_payload_json,
    stage_rows.frozen_resolution_result_json,
    coalesce(nullif(stage_rows.frozen_outstanding_amount, 0), stage_rows.frozen_source_amount, 0::numeric(12,2)) as frozen_source_amount,
    null::numeric as frozen_target_amount_ex_vat,
    null::numeric as frozen_target_amount_vat,
    null::numeric as frozen_target_amount_inc_vat,
    null::jsonb as payout_instruction_snapshot_json
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = false;

  get diagnostics v_rows_ins_dormant_recovery_template_items = row_count;

  select count(*)::integer
  into v_rows_ins_dormant_manual_debt_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = false
    and stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY';

  select count(*)::integer
  into v_rows_ins_dormant_loan_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = false
    and stage_rows.recovery_family = 'LOAN_REPAYMENT';

  select count(*)::integer
  into v_rows_ins_dormant_overpayment_template_items
  from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
  where stage_rows.duplicate_exists = false
    and stage_rows.recovery_family = 'OVERPAYMENT_RECOVERY';

  if v_rows_staged_paye_manual_debt_template_rows > 0 then
    select coalesce(array_agg(missing_rows.candidate_id order by missing_rows.candidate_id), ARRAY[]::uuid[])
    into v_missing_materialised_manual_debt_template_candidate_ids
    from (
      select distinct stage_rows.candidate_id
      from pg_temp.tmp_pay_build_dormant_recovery_template_stage as stage_rows
      where stage_rows.recovery_family = 'MANUAL_DEBT_RECOVERY'
        and stage_rows.normalized_pay_channel = 'PAYE'
    ) as missing_rows
    left join (
      select distinct pbc.candidate_id
      from public.pay_batch_items as pbi_materialised
      join public.pay_batch_candidates as pbc
        on pbc.id = pbi_materialised.pay_batch_candidate_id
      where pbc.pay_batch_id = v_batch_id
        and pbi_materialised.item_type = 'MANUAL_DEBT_RECOVERY'
        and upper(coalesce(pbi_materialised.pay_channel, '')) = 'PAYE'
    ) as materialised_rows
      on materialised_rows.candidate_id = missing_rows.candidate_id
    where materialised_rows.candidate_id is null;

    if coalesce(array_length(v_missing_materialised_manual_debt_template_candidate_ids, 1), 0) > 0 then
      raise exception 'MANUAL_DEBT_TEMPLATE_NOT_MATERIALISED: staged PAYE manual-debt templates did not materialise into pay_batch_items for candidates %',
        array_to_string(v_missing_materialised_manual_debt_template_candidate_ids, ',');
    end if;
  end if;

v_stage := 'STAGE_16C0_FREEZE_CANONICAL_CORRECTION_PROVENANCE';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;

  with correction_items as (
    select
      batch_item.id as pay_batch_item_id,
      batch_candidate.candidate_id,
      batch.source_workbench_session_id,
      batch_item.source_ref,
      batch_item.pay_channel,
      batch_item.amount_ex_vat,
      coalesce(operation_line.match_count,0) as operation_line_match_count,
      coalesce(operation_line.canonical_line_json,'{}'::jsonb)
        as operation_canonical_line_json,
      coalesce(
        operation_line.canonical_line_json->'correction_chain_component',
        '{}'::jsonb
      ) as operation_correction_component_json,
      (
        coalesce(operation_line.match_count,0)=1
        and jsonb_typeof(
          operation_line.canonical_line_json
            ->'correction_chain_component'->'resolution_required'
        )='boolean'
        and operation_line.canonical_line_json
              ->'correction_chain_component'->'resolution_required'
            ='false'::jsonb
        and jsonb_typeof(
          operation_line.canonical_line_json->'case_needs_resolution'
        )='boolean'
        and operation_line.canonical_line_json->'case_needs_resolution'
            ='false'::jsonb
      ) as automatic_correction,
      coalesce(
        nullif(batch_item.frozen_component_snapshot_json
          ->>'canonical_correction_key',''),
        nullif(operation_line.canonical_line_json
          ->'correction_chain_component'->>'canonical_correction_key',''),
        nullif(operation_line.canonical_line_json
          ->>'canonical_correction_key',''),
        nullif(batch_item.frozen_resolution_payload_json
          ->>'canonical_correction_key',''),
        nullif(batch_item.frozen_resolution_payload_json
          ->>'resolution_identity_key','')
      ) as canonical_correction_key,
      coalesce(
        nullif(batch_item.frozen_component_snapshot_json
          ->>'resolution_economic_fingerprint',''),
        nullif(operation_line.canonical_line_json
          ->'correction_chain_component'->>'resolution_economic_fingerprint',''),
        nullif(operation_line.canonical_line_json
          ->>'resolution_economic_fingerprint',''),
        nullif(batch_item.frozen_resolution_payload_json
          ->>'resolution_economic_fingerprint','')
      ) as resolution_economic_fingerprint,
      batch_item.frozen_component_snapshot_json,
      batch_item.frozen_source_basis_json,
      batch_item.frozen_resolution_payload_json,
      batch_item.frozen_resolution_result_json
    from public.pay_batch_items batch_item
    join public.pay_batch_candidates batch_candidate
      on batch_candidate.id=batch_item.pay_batch_candidate_id
    join public.pay_batches batch
      on batch.id=batch_candidate.pay_batch_id
    left join public.banking_pay_operation_candidate_scope operation_scope
      on operation_scope.operation_id=p_operation_id
     and operation_scope.pay_batch_id=batch.id
     and operation_scope.candidate_id=batch_candidate.candidate_id
     and upper(btrim(operation_scope.pay_channel))=
         upper(btrim(batch_item.pay_channel))
    left join lateral (
      select count(*)::integer as match_count,
             (jsonb_agg(canonical_line.value
               order by canonical_line.ordinality)->0)
               as canonical_line_json
      from jsonb_array_elements(coalesce(
        operation_scope.selected_canonical_preview_lines_json,
        '[]'::jsonb
      )) with ordinality as canonical_line(value,ordinality)
      where coalesce(
        nullif(canonical_line.value->>'row_key',''),
        nullif(canonical_line.value->>'source_ref','')
      )=batch_item.source_ref
    ) operation_line on true
    where batch.id=v_batch_id
      and (
        coalesce(batch_item.frozen_source_basis_json
          ->>'source_family_key','') like 'correction-chain:%'
        or coalesce(batch_item.frozen_component_snapshot_json
          ->>'source_family_key','') like 'correction-chain:%'
        or coalesce(batch_item.frozen_resolution_payload_json
          ->>'source_family_key','') like 'correction-chain:%'
        or coalesce(batch_item.source_ref,'') like 'correction-chain:%'
      )
  ), authoritative_resolution as (
    select correction_item.*,
      resolution_row.id as target_resolution_id,
      resolution_row.resolution_origin_session_id,
      resolution_row.resolution_origin_pay_date,
      resolution_row.source_basis_fingerprint
        as current_source_basis_fingerprint,
      resolution_row.resolution_origin_source_basis_fingerprint,
      resolution_row.payload_json as current_resolution_payload_json,
      carry_row.id as carry_registration_id,
      carry_row.source_resolution_id,
      carry_row.source_resolution_identity_key
    from correction_items correction_item
    left join lateral (
      select saved_resolution.*
      from public.banking_pay_workbench_session_case_resolutions
        saved_resolution
      where saved_resolution.session_id=
          correction_item.source_workbench_session_id
        and saved_resolution.candidate_id=correction_item.candidate_id
        and saved_resolution.resolution_identity_key=
          correction_item.canonical_correction_key
      order by saved_resolution.updated_at_utc desc,
               saved_resolution.id desc
      limit 1
    ) resolution_row on true
    left join lateral (
      select registration.*
      from public.banking_pay_workbench_case_resolution_carry_registrations
        registration
      where registration.target_session_id=
          correction_item.source_workbench_session_id
        and registration.candidate_id=correction_item.candidate_id
        and registration.canonical_resolution_key=
          correction_item.canonical_correction_key
        and registration.status='CARRIED'
        and registration.target_resolution_id=resolution_row.id
      order by registration.completed_at_utc desc,
               registration.id desc
      limit 1
    ) carry_row on true
  ), prepared as (
    select authoritative_resolution.*,
      case when authoritative_resolution.automatic_correction then
        (
          coalesce(
            authoritative_resolution.frozen_resolution_result_json,
            '{}'::jsonb
          )-'resolution_result_fingerprint'
        ) || jsonb_build_object(
          'resolution_authority','AUTOMATIC_CORRECTION_CHAIN',
          'resolution_required',false,
          'resolution_complete',true,
          'canonical_correction_key',
            authoritative_resolution.canonical_correction_key,
          'resolution_identity_key',
            authoritative_resolution.canonical_correction_key,
          'resolution_identity_version','CORRECTION_CHAIN_V1',
          'resolution_economic_fingerprint',
            authoritative_resolution.resolution_economic_fingerprint,
          'target_pay_method',
            upper(btrim(authoritative_resolution.pay_channel)),
          'target_amount_ex_vat',
            round(authoritative_resolution.amount_ex_vat,2)
        )
      else
        coalesce(
          authoritative_resolution.frozen_resolution_result_json,
          '{}'::jsonb
        )-'resolution_result_fingerprint'
      end as resolution_result_base_json
    from authoritative_resolution
  ), frozen as (
    select prepared.*,
      encode(
        extensions.digest(
          convert_to(
            prepared.resolution_result_base_json::text,
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      ) as resolution_result_fingerprint
    from prepared
  )
  update public.pay_batch_items batch_item
  set frozen_component_snapshot_json=
        coalesce(batch_item.frozen_component_snapshot_json,'{}'::jsonb)
        ||jsonb_build_object(
          'canonical_correction_key',frozen.canonical_correction_key,
          'correction_identity_version','CORRECTION_CHAIN_V1',
          'correction_root_id',coalesce(
            batch_item.frozen_component_snapshot_json
              ->>'correction_root_id',
            frozen.operation_correction_component_json
              ->>'correction_root_id',
            frozen.operation_canonical_line_json
              ->>'correction_root_id',
            frozen.current_resolution_payload_json
              ->>'correction_root_id'
          ),
          'component_key_type',
            batch_item.frozen_component_key_type,
          'component_key_value',
            batch_item.frozen_component_key_value,
          'ordered_member_timesheet_ids',coalesce(
            batch_item.frozen_component_snapshot_json
              ->'ordered_member_timesheet_ids',
            frozen.operation_correction_component_json
              ->'ordered_member_timesheet_ids',
            frozen.operation_canonical_line_json
              ->'ordered_member_timesheet_ids',
            frozen.current_resolution_payload_json
              ->'ordered_member_timesheet_ids',
            '[]'::jsonb
          ),
          'component_lineage_fingerprint',coalesce(
            batch_item.frozen_component_snapshot_json
              ->>'component_lineage_fingerprint',
            frozen.operation_correction_component_json
              ->>'component_lineage_fingerprint',
            frozen.operation_canonical_line_json
              ->>'component_lineage_fingerprint',
            frozen.current_resolution_payload_json
              ->>'component_lineage_fingerprint'
          ),
          'carrier_source_line_id',coalesce(
            batch_item.frozen_component_snapshot_json
              ->>'carrier_source_line_id',
            batch_item.frozen_component_snapshot_json
              ->>'source_line_id',
            frozen.operation_canonical_line_json
              ->>'line_id'
          ),
          'resolution_economic_fingerprint',
            frozen.resolution_economic_fingerprint
        ),
      frozen_source_basis_json=
        coalesce(batch_item.frozen_source_basis_json,'{}'::jsonb)
        ||jsonb_build_object(
          'source_family_key',coalesce(
            batch_item.frozen_source_basis_json
              ->>'source_family_key',
            batch_item.frozen_component_snapshot_json
              ->>'source_family_key',
            frozen.operation_correction_component_json
              ->>'source_family_key',
            frozen.operation_canonical_line_json
              ->>'source_family_key',
            frozen.current_resolution_payload_json
              ->>'source_family_key'
          ),
          'source_basis_fingerprint',coalesce(
            batch_item.frozen_source_basis_json
              ->>'source_basis_fingerprint',
            batch_item.frozen_component_snapshot_json
              ->>'source_basis_fingerprint',
            frozen.operation_correction_component_json
              ->>'source_basis_fingerprint',
            frozen.operation_canonical_line_json
              ->>'source_basis_fingerprint',
            frozen.operation_canonical_line_json
              #>>'{source_basis_json,source_basis_fingerprint}',
            frozen.current_source_basis_fingerprint
          ),
          'correction_chain_fingerprint',coalesce(
            batch_item.frozen_source_basis_json
              ->>'correction_chain_fingerprint',
            frozen.current_resolution_payload_json
              ->>'correction_chain_fingerprint'
          ),
          'correction_residual_fingerprint',coalesce(
            batch_item.frozen_source_basis_json
              ->>'correction_residual_fingerprint',
            batch_item.frozen_source_basis_json
              ->>'correction_chain_residual_fingerprint',
            frozen.current_resolution_payload_json
              ->>'correction_chain_residual_fingerprint'
          ),
          'correction_financials_policy_envelope_fingerprint',
            coalesce(
              batch_item.frozen_source_basis_json
                ->>'correction_financials_policy_envelope_fingerprint',
              frozen.operation_correction_component_json
                ->>'correction_financials_policy_envelope_fingerprint',
              frozen.operation_canonical_line_json
                ->>'correction_financials_policy_envelope_fingerprint',
              frozen.current_resolution_payload_json
                ->>'correction_financials_policy_envelope_fingerprint'
            ),
          'represented_source_family_key',coalesce(
            batch_item.frozen_source_basis_json
              ->>'represented_source_family_key',
            batch_item.frozen_source_basis_json
              ->>'source_family_key'
          ),
          'source_build_run_id',coalesce(
            batch_item.frozen_source_basis_json
              ->>'source_build_run_id',
            batch_item.frozen_component_snapshot_json
              ->>'source_build_run_id',
            batch_item.frozen_resolution_payload_json
              ->>'source_build_run_id',
            frozen.current_resolution_payload_json
              ->>'source_build_run_id'
          ),
          'operation_source_ref',case
            when frozen.automatic_correction then frozen.source_ref
            else batch_item.frozen_source_basis_json->>'operation_source_ref'
          end
        ),
      frozen_resolution_payload_json=
        coalesce(batch_item.frozen_resolution_payload_json,'{}'::jsonb)
        ||jsonb_build_object(
          'resolution_identity_key',frozen.canonical_correction_key,
          'resolution_identity_version','CORRECTION_CHAIN_V1',
          'canonical_correction_key',frozen.canonical_correction_key,
          'resolution_economic_fingerprint',
            frozen.resolution_economic_fingerprint,
          'source_resolution_id',coalesce(
            frozen.source_resolution_id,
            frozen.target_resolution_id
          ),
          'source_resolution_identity_key',coalesce(
            frozen.source_resolution_identity_key,
            frozen.canonical_correction_key
          ),
          'target_resolution_id',frozen.target_resolution_id,
          'carry_registration_id',frozen.carry_registration_id,
          'resolution_origin_session_id',
            frozen.resolution_origin_session_id,
          'resolution_origin_pay_date',
            frozen.resolution_origin_pay_date,
          'resolution_origin_source_basis_fingerprint',
            frozen.resolution_origin_source_basis_fingerprint
        )
        || case when frozen.automatic_correction then
          jsonb_build_object(
            'resolution_authority','AUTOMATIC_CORRECTION_CHAIN',
            'resolution_required',false,
            'resolution_complete',true,
            'target_resolution_id',null,
            'operation_line_match_count',
              frozen.operation_line_match_count,
            'operation_source_ref',frozen.source_ref
          )
        else '{}'::jsonb end,
      frozen_resolution_result_json=
        frozen.resolution_result_base_json
        ||jsonb_build_object(
          'resolution_result_fingerprint',
            frozen.resolution_result_fingerprint
        ),
      updated_at=v_now_utc
  from frozen
  where batch_item.id=frozen.pay_batch_item_id;

  select coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
    'pay_batch_item_id',batch_item.id,
    'candidate_id',batch_candidate.candidate_id,
    'canonical_component_key',
      batch_item.frozen_component_snapshot_json
        ->>'canonical_correction_key',
    'canonical_resolution_key',
      batch_item.frozen_resolution_payload_json
        ->>'resolution_identity_key',
    'component_economic_fingerprint',
      batch_item.frozen_component_snapshot_json
        ->>'resolution_economic_fingerprint',
    'resolution_economic_fingerprint',
      batch_item.frozen_resolution_payload_json
        ->>'resolution_economic_fingerprint',
    'resolution_authority',
      batch_item.frozen_resolution_payload_json
        ->>'resolution_authority',
    'resolution_required',
      batch_item.frozen_resolution_payload_json
        ->>'resolution_required'
  )) order by batch_item.id),'[]'::jsonb)
  into v_canonical_provenance_mismatch_details
  from public.pay_batch_items batch_item
  join public.pay_batch_candidates batch_candidate
    on batch_candidate.id=batch_item.pay_batch_candidate_id
  where batch_candidate.pay_batch_id=v_batch_id
    and (
      coalesce(batch_item.frozen_source_basis_json
        ->>'source_family_key','') like 'correction-chain:%'
      or coalesce(batch_item.source_ref,'') like 'correction-chain:%'
    )
    and (
      nullif(batch_item.frozen_component_snapshot_json
        ->>'canonical_correction_key','') is null
      or nullif(batch_item.frozen_component_snapshot_json
        ->>'correction_identity_version','')
          is distinct from 'CORRECTION_CHAIN_V1'
      or nullif(batch_item.frozen_component_snapshot_json
        ->>'component_lineage_fingerprint','') is null
      or jsonb_typeof(batch_item.frozen_component_snapshot_json
        ->'ordered_member_timesheet_ids') is distinct from 'array'
      or nullif(batch_item.frozen_resolution_payload_json
        ->>'resolution_identity_key','') is null
      or nullif(batch_item.frozen_resolution_payload_json
        ->>'resolution_economic_fingerprint','') is null
      or nullif(batch_item.frozen_resolution_result_json
        ->>'resolution_result_fingerprint','') is null
      or batch_item.frozen_component_snapshot_json
          ->>'canonical_correction_key'
        is distinct from
          batch_item.frozen_resolution_payload_json
            ->>'resolution_identity_key'
      or batch_item.frozen_component_snapshot_json
          ->>'resolution_economic_fingerprint'
        is distinct from
          batch_item.frozen_resolution_payload_json
            ->>'resolution_economic_fingerprint'
      or (
        coalesce(batch_item.frozen_resolution_payload_json
          ->>'resolution_authority','')='AUTOMATIC_CORRECTION_CHAIN'
        and (
          batch_item.frozen_resolution_payload_json
            ->>'resolution_required' is distinct from 'false'
          or batch_item.frozen_resolution_payload_json
            ->>'resolution_complete' is distinct from 'true'
          or nullif(batch_item.frozen_resolution_payload_json
            ->>'target_resolution_id','') is not null
          or batch_item.frozen_resolution_payload_json
            ->>'operation_line_match_count' is distinct from '1'
          or batch_item.frozen_resolution_payload_json
            ->>'operation_source_ref' is distinct from batch_item.source_ref
          or batch_item.frozen_resolution_result_json
            ->>'resolution_authority'
              is distinct from 'AUTOMATIC_CORRECTION_CHAIN'
          or batch_item.frozen_resolution_result_json
            ->>'resolution_required' is distinct from 'false'
          or batch_item.frozen_resolution_result_json
            ->>'resolution_complete' is distinct from 'true'
          or batch_item.frozen_resolution_result_json
            ->>'canonical_correction_key'
              is distinct from batch_item.frozen_resolution_payload_json
                ->>'resolution_identity_key'
          or batch_item.frozen_resolution_result_json
            ->>'resolution_economic_fingerprint'
              is distinct from batch_item.frozen_resolution_payload_json
                ->>'resolution_economic_fingerprint'
          or nullif(batch_item.frozen_component_snapshot_json
            ->>'correction_root_id','') is null
          or case
            when jsonb_typeof(batch_item.frozen_component_snapshot_json
              ->'ordered_member_timesheet_ids')='array'
            then jsonb_array_length(batch_item.frozen_component_snapshot_json
              ->'ordered_member_timesheet_ids')
            else 0
          end=0
          or coalesce(batch_item.frozen_source_basis_json
            ->>'source_family_key','') not like 'correction-chain:%'
        )
      )
      or (
        coalesce(batch_item.frozen_resolution_payload_json
          ->>'resolution_authority','')<>'AUTOMATIC_CORRECTION_CHAIN'
        and nullif(batch_item.frozen_resolution_payload_json
          ->>'target_resolution_id','') is null
      )
    );

  if jsonb_array_length(v_canonical_provenance_mismatch_details)>0 then
    raise exception 'PAY_BATCH_CANONICAL_CORRECTION_PROVENANCE_INVALID'
      using errcode='P0001',
            detail=jsonb_build_object(
              'code','PAY_BATCH_CANONICAL_CORRECTION_PROVENANCE_INVALID',
              'pay_batch_id',v_batch_id,
              'mismatches',v_canonical_provenance_mismatch_details
            )::text;
  end if;

v_stage := 'STAGE_16C1_FREEZE_ALL_FINANCE_ITEM_PAYOUT_INSTRUCTIONS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text,
        'helper_exists', false,
        'mode', 'STAGED_PREVIEW_FINANCE_PAYOUT_AUTHORITY'
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;


  v_stage := 'STAGE_16C2_ENRICH_FINANCE_PAYOUT_INSTRUCTION_EXEC_FIELDS';
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'PAY_CREATE_DRAFT_BATCH:' || v_stage,
      jsonb_build_object(
        'stage', v_stage,
        'pay_batch_id', v_batch_id::text
      ),
      'pay_batches',
      v_batch_id::text,
      null, null, null, null, null
    );
  exception when others then null; end;


  update public.pay_batch_items as pbi_set
  set payout_instruction_snapshot_json = pbi_src.payout_instruction_snapshot_json
  from (
    with finance_case_preview_ctx as (
      select distinct on (spr.finance_case_id)
        spr.finance_case_id,
        spr.timesheet_id as linked_timesheet_id,
        upper(coalesce(spr.pay_channel, '')) as preview_pay_channel,
        nullif(btrim(coalesce(spr.routing_kind, '')), '') as routing_kind,
        nullif(btrim(coalesce(spr.destination_label, '')), '') as destination_label,
        nullif(btrim(coalesce(spr.taxability, '')), '') as taxability,
        nullif(btrim(coalesce(spr.beneficiary_name, '')), '') as beneficiary_name,
        nullif(btrim(coalesce(spr.masked_bank_account, '')), '') as masked_bank_account,
        nullif(btrim(coalesce(spr.bank_details_hash, '')), '') as bank_details_hash,
        coalesce(spr.is_candidate_directed_oneoff_payout, false) as is_candidate_directed_oneoff_payout,
        coalesce(spr.appears_on_umbrella_remittance, false) as appears_on_umbrella_remittance,
        coalesce(spr.generates_candidate_payment_advice, false) as generates_candidate_payment_advice
      from pg_temp.tmp_pay_build_selected_preview_rows spr
      where spr.finance_case_id is not null
      order by spr.finance_case_id, spr.preview_row_id
    )
    select
      pbi.id as pay_batch_item_id,
      jsonb_strip_nulls(
        coalesce(pbi.payout_instruction_snapshot_json, '{}'::jsonb)
        || jsonb_build_object(
          'taxability',
            coalesce(
              nullif(btrim(coalesce(fpc.taxability, '')), ''),
              case when pbi.frozen_component_classification = 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum then 'NON_TAXABLE' else 'TAXABLE' end
            ),
          'routing_kind',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', '')), ''),
              nullif(btrim(coalesce(fpc.routing_kind, '')), ''),
              case when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' then 'UMBRELLA_COMPANY' else 'NORMAL_PAY_ROUTE' end
            ),
          'destination_label',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'destination_label', '')), ''),
              nullif(btrim(coalesce(fpc.destination_label, '')), ''),
              case when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' then 'umbrella company' else 'normal PAYE route' end
            ),
          'pay_channel', upper(coalesce(pbi.pay_channel::text, '')),
          'payee_entity_kind',
            case
              when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then 'CANDIDATE'
              when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false then 'UMBRELLA'
              else 'CANDIDATE'
            end,
          'payee_entity_id',
            case
              when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then pbc.candidate_id::text
              when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false and coalesce(pbi.umbrella_id, c.umbrella_id) is not null then coalesce(pbi.umbrella_id, c.umbrella_id)::text
              else pbc.candidate_id::text
            end,
          'beneficiary_name',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'beneficiary_name', '')), ''),
              nullif(btrim(coalesce(fpc.beneficiary_name, '')), ''),
              case
                when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
                  then nullif(btrim(coalesce(obd.beneficiary_name, '')), '')
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false
                  then nullif(btrim(coalesce(u.name, '')), '')
                else nullif(btrim(coalesce(c.account_holder, c.display_name, concat_ws(' ', c.first_name, c.last_name))), '')
              end
            ),
          'sort_code',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'sort_code', '')), ''),
              case
                when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
                 and length(coalesce(obd.sort_code, '')) = 6
                  then substr(obd.sort_code, 1, 2) || '-' || substr(obd.sort_code, 3, 2) || '-' || substr(obd.sort_code, 5, 2)
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false and length(coalesce(u.sort_code, '')) = 6
                  then substr(u.sort_code, 1, 2) || '-' || substr(u.sort_code, 3, 2) || '-' || substr(u.sort_code, 5, 2)
                when length(coalesce(c.sort_code, '')) = 6
                  then substr(c.sort_code, 1, 2) || '-' || substr(c.sort_code, 3, 2) || '-' || substr(c.sort_code, 5, 2)
                else null
              end
            ),
          'account_number',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'account_number', '')), ''),
              case
                when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
                  then nullif(obd.account_number, '')
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false
                  then nullif(u.account_number, '')
                else nullif(c.account_number, '')
              end
            ),
          'account_type',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'account_type', '')), ''),
              case
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false then 'Business'
                else 'Personal'
              end
            ),
          'masked_bank_account',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'masked_bank_account', '')), ''),
              nullif(btrim(coalesce(fpc.masked_bank_account, '')), ''),
              case
                when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
                 and nullif(coalesce(obd.account_number, ''), '') is not null
                  then lpad(right(obd.account_number, 4), length(obd.account_number), '*')
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false and nullif(coalesce(u.account_number, ''), '') is not null
                  then lpad(right(u.account_number, 4), length(u.account_number), '*')
                when nullif(coalesce(c.account_number, ''), '') is not null
                  then lpad(right(c.account_number, 4), length(c.account_number), '*')
                else null
              end
            ),
          'bank_details_hash',
            coalesce(
              nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
              nullif(btrim(coalesce(fpc.bank_details_hash, '')), ''),
              case
                when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then obd.bank_details_hash
                when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false then u.bank_details_hash
                else c.bank_details_hash
              end
            ),
          'bank_details_note', coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'bank_details_note', '')), ''), obd.note),
          'bank_details_created_by_user_id', coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'bank_details_created_by_user_id', '')), ''), case when obd.created_by_user_id is null then null else obd.created_by_user_id::text end),
          'bank_details_updated_by_user_id', coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'bank_details_updated_by_user_id', '')), ''), case when obd.updated_by_user_id is null then null else obd.updated_by_user_id::text end),
          'week_ending_bucket',
            case
              when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA'
               and upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) <> 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
                then case
                  when pbi.item_type in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
                    then coalesce(
                      matching_positive_group.week_ending_bucket,
                      case when ts_ctx.week_ending_date is null then null else ts_ctx.week_ending_date::text end,
                      (v_week_start + 6)::text
                    )
                  else coalesce(
                    case when ts_ctx.week_ending_date is null then null else ts_ctx.week_ending_date::text end,
                    (v_week_start + 6)::text
                  )
                end
              else null
            end,
          'rail_provider', upper(btrim(coalesce(pb.rail_provider_snapshot, ''))),
          'rail_env', upper(btrim(coalesce(pb.rail_env_snapshot, ''))),
          'payee_id',
            case
              when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then case when c.payee_id is null then null else c.payee_id::text end
              when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false then case when u.payee_id is null then null else u.payee_id::text end
              else case when c.payee_id is null then null else c.payee_id::text end
            end,
          'payee_account_id',
            case
              when upper(coalesce(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind', fpc.routing_kind), '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' then case when c.payee_account_id is null then null else c.payee_account_id::text end
              when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' and coalesce(fpc.is_candidate_directed_oneoff_payout, false) = false then case when u.payee_account_id is null then null else u.payee_account_id::text end
              else case when c.payee_account_id is null then null else c.payee_account_id::text end
            end,
          'appears_on_umbrella_remittance',
            coalesce(
              (pbi.payout_instruction_snapshot_json->>'appears_on_umbrella_remittance')::boolean,
              fpc.appears_on_umbrella_remittance,
              case when upper(coalesce(pbi.pay_channel::text, '')) = 'UMBRELLA' then true else false end
            ),
          'generates_candidate_payment_advice',
            coalesce(
              (pbi.payout_instruction_snapshot_json->>'generates_candidate_payment_advice')::boolean,
              fpc.generates_candidate_payment_advice,
              false
            ),
          'is_candidate_directed_oneoff_payout',
            coalesce(
              (pbi.payout_instruction_snapshot_json->>'is_candidate_directed_oneoff_payout')::boolean,
              fpc.is_candidate_directed_oneoff_payout,
              false
            )
        )
      ) as payout_instruction_snapshot_json
    from public.pay_batch_items as pbi
    join public.pay_batch_candidates as pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches as pb
      on pb.id = pbc.pay_batch_id
    join pg_temp.tmp_pay_build_candidates_ctx as c
      on c.id = pbc.candidate_id
    left join pg_temp.tmp_pay_build_umbrellas_ctx as u
      on u.id = coalesce(pbi.umbrella_id, c.umbrella_id)
    left join finance_case_preview_ctx as fpc
      on fpc.finance_case_id = pbi.finance_case_id
    left join pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx as obd
      on obd.finance_case_id = pbi.finance_case_id
    left join pg_temp.tmp_pay_build_timesheet_snapshots_ctx as ts_ctx
      on ts_ctx.timesheet_id = coalesce(pbi.timesheet_id, fpc.linked_timesheet_id)
    left join lateral (
      select
        positive_item.payout_instruction_snapshot_json->>'week_ending_bucket' as week_ending_bucket
      from public.pay_batch_items as positive_item
      where positive_item.pay_batch_candidate_id = pbi.pay_batch_candidate_id
        and positive_item.id <> pbi.id
        and coalesce(positive_item.is_voided, false) = false
        and upper(coalesce(positive_item.pay_channel::text, '')) = upper(coalesce(pbi.pay_channel::text, ''))
        and round(coalesce(positive_item.amount_inc_vat, positive_item.amount_ex_vat, 0), 2) > 0
        and coalesce(positive_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
        and (
          upper(coalesce(pbi.pay_channel::text, '')) <> 'UMBRELLA'
          or coalesce(positive_item.umbrella_id, c.umbrella_id)
             is not distinct from coalesce(pbi.umbrella_id, c.umbrella_id)
        )
      order by
        (positive_item.payout_instruction_snapshot_json->>'week_ending_bucket')::date,
        positive_item.id
      limit 1
    ) as matching_positive_group
      on pbi.item_type in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
    where pbc.pay_batch_id = v_batch_id
      and coalesce(pbi.is_voided, false) = false
      and pbi.finance_case_id is not null
      and pbi.item_type in ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT','OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','LOAN_REPAYMENT')
  ) as pbi_src
  where pbi_set.id = pbi_src.pay_batch_item_id;


  IF p_operation_id IS NOT NULL THEN
    WITH allocation_matches AS (
      SELECT DISTINCT ON (allocation_row.id)
        allocation_row.id AS allocation_row_id,
        batch_item.id AS pay_batch_item_id,
        COALESCE(
          NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', ''),
          allocation_row.operation_source_key
        ) AS operation_source_key
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = v_batch_id
       AND batch_candidate.candidate_id = allocation_row.candidate_id
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
       AND batch_item.finance_case_id IS NOT DISTINCT FROM allocation_row.finance_case_id
       AND (
         allocation_row.finance_component_id IS NULL
         OR batch_item.finance_component_id IS NOT DISTINCT FROM allocation_row.finance_component_id
       )
       AND (
         (
           NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', '') IS NOT NULL
           AND batch_item.operation_source_key = allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}'
           AND ROUND(COALESCE(batch_item.amount_ex_vat, 0), 2) = ROUND(COALESCE(allocation_row.allocated_amount, 0), 2)
         )
         OR (
           NULLIF(allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', '') IS NULL
           AND ROUND(ABS(COALESCE(batch_item.amount_ex_vat, 0)), 2) = ROUND(ABS(COALESCE(allocation_row.allocated_amount, 0)), 2)
           AND (
             batch_item.operation_source_key IS NULL
             OR batch_item.operation_source_key = allocation_row.operation_source_key
           )
         )
       )
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.candidate_id = ANY(v_candidate_ids)
        AND allocation_row.pay_channel = v_scope
        AND allocation_row.finance_case_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN (
          'OVERPAYMENT_RECOVERY',
          'MANUAL_DEBT_RECOVERY',
          'PAYMENT_ADVANCE_REPAYMENT',
          'LOAN_REPAYMENT',
          'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
          'MANUAL_CREDIT_PAYOUT',
          'LOAN_PAYOUT',
        'UNDERPAYMENT_PAYMENT'
        )
        AND (
          (
            UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
            AND batch_item.item_type = UPPER(BTRIM(COALESCE(allocation_row.allocation_type, '')))
          )
          OR (
            UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) NOT IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
            AND batch_item.item_type IN ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT','MANUAL_DEBT_RECOVERY','LOAN_REPAYMENT')
          )
        )
        AND coalesce(batch_item.is_voided, false) = false
      ORDER BY
        allocation_row.id,
        CASE
          WHEN batch_item.operation_source_key = allocation_row.operation_source_key THEN 0
          WHEN batch_item.operation_source_key = NULLIF(
            allocation_row.allocation_basis_json#>>'{draft_finance_item_plan,planned_item_key}', ''
          ) THEN 0
          WHEN batch_item.id = allocation_row.pay_batch_item_id THEN 1
          ELSE 2
        END,
        batch_item.created_at NULLS LAST,
        batch_item.id
    ),
    updated_items AS (
      UPDATE public.pay_batch_items AS batch_item_update
      SET
        operation_source_key = allocation_matches.operation_source_key,
        updated_at = now()
      FROM allocation_matches
      WHERE batch_item_update.id = allocation_matches.pay_batch_item_id
        AND batch_item_update.operation_source_key IS DISTINCT FROM allocation_matches.operation_source_key
      RETURNING batch_item_update.id
    )
    UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_update
    SET
      status = 'ITEM_CREATED',
      pay_batch_id = v_batch_id,
      pay_batch_item_id = allocation_matches.pay_batch_item_id,
      updated_at_utc = now()
    FROM allocation_matches
    WHERE allocation_update.id = allocation_matches.allocation_row_id
      AND (allocation_update.status <> 'ITEM_CREATED' OR allocation_update.pay_batch_item_id IS DISTINCT FROM allocation_matches.pay_batch_item_id);

    WITH expected_plan AS (
      SELECT plan_row.*
      FROM private.pay_workbench_draft_finance_item_plan_v1(p_operation_id, p_candidate_scope_ids) AS plan_row
      WHERE plan_row.pay_channel = v_scope
        AND plan_row.planned_item_type = 'OVERPAYMENT_RECOVERY'
    ), created_plan AS (
      SELECT
        batch_item.operation_source_key AS planned_item_key,
        batch_item.amount_ex_vat,
        batch_item.item_type,
        batch_candidate.candidate_id,
        batch_item.finance_case_id,
        batch_item.finance_component_id,
        count(*) OVER (PARTITION BY batch_item.operation_source_key) AS key_count
      FROM public.pay_batch_items AS batch_item
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = batch_item.pay_batch_candidate_id
      WHERE batch_candidate.pay_batch_id = v_batch_id
        AND batch_candidate.candidate_id = ANY(v_candidate_ids)
        AND batch_item.item_type = 'OVERPAYMENT_RECOVERY'
        AND batch_item.finance_case_id IS NOT NULL
        AND batch_item.operation_source_key LIKE (p_operation_id::text || ':%')
        AND COALESCE(batch_item.is_voided, false) = false
    ), mismatch AS (
      SELECT
        (SELECT count(*)::integer FROM expected_plan) AS expected_key_count,
        (SELECT count(DISTINCT created_plan.planned_item_key)::integer FROM created_plan) AS created_key_count,
        (SELECT count(*)::integer
         FROM expected_plan
         WHERE NOT EXISTS (
           SELECT 1 FROM created_plan
           WHERE created_plan.planned_item_key = expected_plan.planned_item_key
         )) AS missing_key_count,
        (SELECT count(*)::integer
         FROM created_plan
         WHERE NOT EXISTS (
           SELECT 1 FROM expected_plan
           WHERE expected_plan.planned_item_key = created_plan.planned_item_key
         )) AS unexpected_key_count,
        (SELECT count(*)::integer FROM created_plan WHERE created_plan.key_count > 1) AS duplicate_key_count,
        (SELECT count(*)::integer
         FROM expected_plan
         JOIN created_plan ON created_plan.planned_item_key = expected_plan.planned_item_key
         WHERE ROUND(created_plan.amount_ex_vat, 2) IS DISTINCT FROM ROUND(expected_plan.planned_item_amount, 2)
            OR created_plan.item_type IS DISTINCT FROM expected_plan.planned_item_type
            OR created_plan.candidate_id IS DISTINCT FROM expected_plan.candidate_id
            OR created_plan.finance_case_id IS DISTINCT FROM expected_plan.finance_case_id
            OR created_plan.finance_component_id IS DISTINCT FROM expected_plan.finance_component_id) AS identity_or_amount_mismatch_count,
        COALESCE((
          SELECT jsonb_agg(missing_row.planned_item_key ORDER BY missing_row.planned_item_key)
          FROM (
            SELECT expected_plan.planned_item_key
            FROM expected_plan
            WHERE NOT EXISTS (
              SELECT 1 FROM created_plan
              WHERE created_plan.planned_item_key = expected_plan.planned_item_key
            )
            ORDER BY expected_plan.planned_item_key
            LIMIT 20
          ) AS missing_row
        ), '[]'::jsonb) AS missing_key_sample,
        COALESCE((
          SELECT jsonb_agg(unexpected_row.planned_item_key ORDER BY unexpected_row.planned_item_key)
          FROM (
            SELECT created_plan.planned_item_key
            FROM created_plan
            WHERE NOT EXISTS (
              SELECT 1 FROM expected_plan
              WHERE expected_plan.planned_item_key = created_plan.planned_item_key
            )
            ORDER BY created_plan.planned_item_key
            LIMIT 20
          ) AS unexpected_row
        ), '[]'::jsonb) AS unexpected_key_sample
    )
    SELECT jsonb_build_object(
      'expected_key_count', mismatch.expected_key_count,
      'created_key_count', mismatch.created_key_count,
      'missing_key_count', mismatch.missing_key_count,
      'unexpected_key_count', mismatch.unexpected_key_count,
      'duplicate_key_count', mismatch.duplicate_key_count,
      'identity_or_amount_mismatch_count', mismatch.identity_or_amount_mismatch_count,
      'missing_key_sample', mismatch.missing_key_sample,
      'unexpected_key_sample', mismatch.unexpected_key_sample
    )
    INTO v_operation_plan_mismatch_details
    FROM mismatch;

    IF COALESCE((v_operation_plan_mismatch_details->>'missing_key_count')::integer, 0) > 0
       OR COALESCE((v_operation_plan_mismatch_details->>'unexpected_key_count')::integer, 0) > 0
       OR COALESCE((v_operation_plan_mismatch_details->>'duplicate_key_count')::integer, 0) > 0
       OR COALESCE((v_operation_plan_mismatch_details->>'identity_or_amount_mismatch_count')::integer, 0) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS',
        'code', 'DRAFT_FINANCE_ITEM_PLAN_PARITY_FAILED',
        'message', 'Created Draft finance items do not exactly match the frozen canonical finance-item plan',
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_scope,
        'mismatch_details', v_operation_plan_mismatch_details
      )::text;
    END IF;

    SELECT jsonb_build_object(
             'allocation_rows_total', count(*)::integer,
             'allocation_rows_unlinked', count(*) FILTER (WHERE allocation_row.status <> 'ITEM_CREATED' OR allocation_row.pay_batch_item_id IS NULL)::integer
           )
    INTO v_operation_mismatch_details
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_id = ANY(v_candidate_ids)
      AND allocation_row.pay_channel = v_scope
      AND allocation_row.finance_case_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) IN (
        'OVERPAYMENT_RECOVERY',
        'MANUAL_DEBT_RECOVERY',
        'PAYMENT_ADVANCE_REPAYMENT',
        'LOAN_REPAYMENT',
        'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
        'MANUAL_CREDIT_PAYOUT',
        'LOAN_PAYOUT',
        'UNDERPAYMENT_PAYMENT'
      );

    IF coalesce((v_operation_mismatch_details->>'allocation_rows_unlinked')::integer, 0) > 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_BATCH_APPLY_FINANCE_ADJUSTMENTS',
        'code', 'OPERATION_ALLOCATION_ROWS_NOT_LINKED',
        'message', 'pay_batch_apply_finance_adjustments could not link every operation allocation row to a created pay batch item',
        'pay_batch_id', v_batch_id::text,
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_scope,
        'mismatch_details', v_operation_mismatch_details
      )::text;
    END IF;

    UPDATE public.banking_pay_operation_candidate_scope AS scope_update
    SET status = CASE
          WHEN NOT EXISTS (
            SELECT 1
            FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_remaining
            WHERE allocation_remaining.operation_id = p_operation_id
              AND allocation_remaining.candidate_scope_id = scope_update.id
              AND UPPER(BTRIM(COALESCE(allocation_remaining.status, ''))) IN ('PENDING', 'ITEM_PENDING')
          ) THEN 'DRAFTED'
          ELSE scope_update.status
        END,
        pay_batch_id = v_batch_id,
        updated_at_utc = now()
    WHERE scope_update.operation_id = p_operation_id
      AND scope_update.candidate_id = ANY(v_candidate_ids)
      AND scope_update.pay_channel = v_scope;
  END IF;

  IF p_operation_id IS NOT NULL THEN
    PERFORM private.pay_workbench_draft_expected_effects_v1(
      p_operation_id,'APPLY_FINANCE_ADJUSTMENTS','ASSERT_COMPLETE','[]'::jsonb,
      jsonb_build_object('pay_batch_id',p_pay_batch_id)
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', v_batch_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'pay_channel_scope', v_scope,
    'applied_count', (coalesce(v_rows_ins_loan_payout_items, 0) + coalesce(v_rows_ins_manual_credit_payout_items, 0) + coalesce(v_rows_ins_underpayment_payout_items, 0) + coalesce(v_rows_ins_overpayment_recovery_items, 0) + coalesce(v_rows_ins_debt_items, 0) + coalesce(v_rows_ins_loan_items, 0) + coalesce(v_rows_ins_dormant_recovery_template_items, 0) + coalesce(v_rows_ins_dormant_manual_debt_template_items, 0) + coalesce(v_rows_ins_dormant_loan_template_items, 0) + coalesce(v_rows_ins_dormant_overpayment_template_items, 0)),
    'reused_count', coalesce(v_operation_allocation_done, 0),
    'skipped_count', (coalesce(v_rows_skipped_dormant_recovery_template_items, 0) + coalesce(v_rows_skipped_dormant_manual_debt_template_items, 0) + coalesce(v_rows_skipped_dormant_loan_template_items, 0) + coalesce(v_rows_skipped_dormant_overpayment_template_items, 0)),
    'failed_count', 0,
    'mismatch_details', coalesce(v_operation_mismatch_details, '{}'::jsonb),
    'loan_payout_items_inserted', coalesce(v_rows_ins_loan_payout_items, 0),
    'manual_credit_payout_items_inserted', coalesce(v_rows_ins_manual_credit_payout_items, 0),
    'underpayment_payment_items_inserted', coalesce(v_rows_ins_underpayment_payout_items, 0),
    'overpayment_recovery_items_inserted', coalesce(v_rows_ins_overpayment_recovery_items, 0),
    'manual_debt_recovery_items_inserted', coalesce(v_rows_ins_debt_items, 0),
    'loan_repayment_items_inserted', coalesce(v_rows_ins_loan_items, 0),
    'staged_recovery_template_rows', coalesce(v_rows_staged_recovery_template_rows, 0),
    'staged_manual_debt_template_rows', coalesce(v_rows_staged_manual_debt_template_rows, 0),
    'staged_loan_template_rows', coalesce(v_rows_staged_loan_template_rows, 0),
    'staged_overpayment_template_rows', coalesce(v_rows_staged_overpayment_template_rows, 0),
    'staged_paye_manual_debt_template_rows', coalesce(v_rows_staged_paye_manual_debt_template_rows, 0),
    'staged_manual_debt_template_rows_by_candidate', coalesce(v_staged_manual_debt_template_rows_by_candidate, '{}'::jsonb),
    'dormant_recovery_template_items_inserted', coalesce(v_rows_ins_dormant_recovery_template_items, 0),
    'dormant_manual_debt_template_items_inserted', coalesce(v_rows_ins_dormant_manual_debt_template_items, 0),
    'dormant_loan_template_items_inserted', coalesce(v_rows_ins_dormant_loan_template_items, 0),
    'dormant_overpayment_template_items_inserted', coalesce(v_rows_ins_dormant_overpayment_template_items, 0),
    'dormant_recovery_template_items_skipped_as_duplicate', coalesce(v_rows_skipped_dormant_recovery_template_items, 0),
    'dormant_manual_debt_template_items_skipped_as_duplicate', coalesce(v_rows_skipped_dormant_manual_debt_template_items, 0),
    'dormant_loan_template_items_skipped_as_duplicate', coalesce(v_rows_skipped_dormant_loan_template_items, 0),
    'dormant_overpayment_template_items_skipped_as_duplicate', coalesce(v_rows_skipped_dormant_overpayment_template_items, 0)
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_batch_apply_finance_adjustments(
  uuid,
  text,
  uuid,
  numeric,
  date,
  uuid,
  jsonb
) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_apply_finance_adjustments(
  uuid,
  text,
  uuid,
  numeric,
  date,
  uuid,
  jsonb
) TO service_role;
