-- Banking Pay bounded-scope V1.2.4: set-based financial transition adapter.
-- Financial source triggers only identify exact impacted owners; they never run finance.

CREATE OR REPLACE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_reason text;
  v_candidate_ids uuid[];
  v_timesheet_ids uuid[];
  v_expected regclass:=pg_catalog.to_regclass('pg_temp._bpay_wb_expected_effects');
  v_delete_context regclass:=pg_catalog.to_regclass('pg_temp._bpay_candidate_delete_context_v1');
  v_relation_owner oid;
  v_duplicate boolean:=false;
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_transition_impacts_v1(
    relation_name text NOT NULL,
    operation text NOT NULL,
    source_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    timesheet_id uuid NULL,
    before_digest text NULL,
    after_digest text NULL,
    reason text NOT NULL,
    UNIQUE NULLS NOT DISTINCT(relation_name,operation,source_id,candidate_id,timesheet_id)
  ) ON COMMIT DROP;
  TRUNCATE pg_temp._bpay_wb_transition_impacts_v1;

  v_reason:=upper(TG_TABLE_NAME)||'_'||upper(TG_OP);

  IF TG_TABLE_NAME='pay_advances' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text),NULL,v_reason FROM old_rows r ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,CASE WHEN TG_OP='INSERT' THEN md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text)
        ELSE md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text) END,v_reason FROM new_rows r
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_finance_case_components' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text),NULL,v_reason FROM old_rows r ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,CASE WHEN TG_OP='INSERT' THEN md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text)
        ELSE md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text) END,v_reason FROM new_rows r
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_finance_case_events' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(component.candidate_id,finance_case.candidate_id),
        COALESCE(component.linked_timesheet_id,finance_case.linked_timesheet_id),
        md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text),NULL,v_reason FROM old_rows r
      LEFT JOIN public.pay_finance_case_components component ON component.id=r.finance_component_id
      LEFT JOIN public.pay_advances finance_case ON finance_case.id=r.finance_case_id
      WHERE COALESCE(component.candidate_id,finance_case.candidate_id) IS NOT NULL ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(component.candidate_id,finance_case.candidate_id),
        COALESCE(component.linked_timesheet_id,finance_case.linked_timesheet_id),
        NULL,CASE WHEN TG_OP='INSERT' THEN md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text)
        ELSE md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(r))::text) END,v_reason FROM new_rows r
      LEFT JOIN public.pay_finance_case_components component ON component.id=r.finance_component_id
      LEFT JOIN public.pay_advances finance_case ON finance_case.id=r.finance_case_id
      WHERE COALESCE(component.candidate_id,finance_case.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_batch_items' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id),
        COALESCE(r.timesheet_id,component.linked_timesheet_id),
        md5(jsonb_build_object(
          'pay_batch_candidate_id',r.pay_batch_candidate_id,'item_type',r.item_type,
          'timesheet_id',r.timesheet_id,'amount_ex_vat',r.amount_ex_vat,
          'amount_vat',r.amount_vat,'amount_inc_vat',r.amount_inc_vat,
          'pay_channel',r.pay_channel,'pay_bank_transfer_id',r.pay_bank_transfer_id,
          'is_voided',r.is_voided,'finance_case_id',r.finance_case_id,
          'reservation_id',r.reservation_id,'finance_component_id',r.finance_component_id,
          'frozen_component_snapshot_json',r.frozen_component_snapshot_json,
          'frozen_component_key_type',r.frozen_component_key_type,
          'frozen_component_key_value',r.frozen_component_key_value,
          'frozen_component_classification',r.frozen_component_classification,
          'frozen_source_basis_json',r.frozen_source_basis_json,
          'frozen_source_pay_method',r.frozen_source_pay_method,
          'frozen_target_pay_method',r.frozen_target_pay_method,
          'frozen_resolution_mode',r.frozen_resolution_mode,
          'frozen_resolution_payload_json',r.frozen_resolution_payload_json,
          'frozen_resolution_result_json',r.frozen_resolution_result_json,
          'frozen_source_amount',r.frozen_source_amount,
          'frozen_target_amount_ex_vat',r.frozen_target_amount_ex_vat,
          'frozen_target_amount_vat',r.frozen_target_amount_vat,
          'frozen_target_amount_inc_vat',r.frozen_target_amount_inc_vat,
          'operation_source_key',r.operation_source_key)::text),NULL,v_reason
      FROM old_rows AS r
      LEFT JOIN public.pay_batch_candidates AS batch_candidate ON batch_candidate.id=r.pay_batch_candidate_id
      LEFT JOIN public.pay_finance_case_components AS component ON component.id=r.finance_component_id
      LEFT JOIN public.timesheets_financials AS current_financial
        ON current_financial.timesheet_id=COALESCE(r.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id) IS NOT NULL
      ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id),
        COALESCE(r.timesheet_id,component.linked_timesheet_id),NULL,
        md5(jsonb_build_object(
          'pay_batch_candidate_id',r.pay_batch_candidate_id,'item_type',r.item_type,
          'timesheet_id',r.timesheet_id,'amount_ex_vat',r.amount_ex_vat,
          'amount_vat',r.amount_vat,'amount_inc_vat',r.amount_inc_vat,
          'pay_channel',r.pay_channel,'pay_bank_transfer_id',r.pay_bank_transfer_id,
          'is_voided',r.is_voided,'finance_case_id',r.finance_case_id,
          'reservation_id',r.reservation_id,'finance_component_id',r.finance_component_id,
          'frozen_component_snapshot_json',r.frozen_component_snapshot_json,
          'frozen_component_key_type',r.frozen_component_key_type,
          'frozen_component_key_value',r.frozen_component_key_value,
          'frozen_component_classification',r.frozen_component_classification,
          'frozen_source_basis_json',r.frozen_source_basis_json,
          'frozen_source_pay_method',r.frozen_source_pay_method,
          'frozen_target_pay_method',r.frozen_target_pay_method,
          'frozen_resolution_mode',r.frozen_resolution_mode,
          'frozen_resolution_payload_json',r.frozen_resolution_payload_json,
          'frozen_resolution_result_json',r.frozen_resolution_result_json,
          'frozen_source_amount',r.frozen_source_amount,
          'frozen_target_amount_ex_vat',r.frozen_target_amount_ex_vat,
          'frozen_target_amount_vat',r.frozen_target_amount_vat,
          'frozen_target_amount_inc_vat',r.frozen_target_amount_inc_vat,
          'operation_source_key',r.operation_source_key)::text),v_reason
      FROM new_rows AS r
      LEFT JOIN public.pay_batch_candidates AS batch_candidate ON batch_candidate.id=r.pay_batch_candidate_id
      LEFT JOIN public.pay_finance_case_components AS component ON component.id=r.finance_component_id
      LEFT JOIN public.timesheets_financials AS current_financial
        ON current_financial.timesheet_id=COALESCE(r.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_batch_item_breakdowns' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id),
        COALESCE(item.timesheet_id,component.linked_timesheet_id),
        md5(jsonb_build_object('pay_batch_item_id',r.pay_batch_item_id,'line_kind',r.line_kind,
          'bucket_code',r.bucket_code,'unit_name',r.unit_name,'units',r.units,'rate',r.rate,
          'amount_ex_vat',r.amount_ex_vat,'amount_vat',r.amount_vat,
          'amount_inc_vat',r.amount_inc_vat,'meta_json',r.meta_json,
          'operation_source_key',r.operation_source_key)::text),NULL,v_reason
      FROM old_rows AS r
      LEFT JOIN public.pay_batch_items AS item ON item.id=r.pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
      LEFT JOIN public.pay_finance_case_components AS component ON component.id=item.finance_component_id
      LEFT JOIN public.timesheets_financials AS current_financial
        ON current_financial.timesheet_id=COALESCE(item.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id) IS NOT NULL
      ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id),
        COALESCE(item.timesheet_id,component.linked_timesheet_id),NULL,
        md5(jsonb_build_object('pay_batch_item_id',r.pay_batch_item_id,'line_kind',r.line_kind,
          'bucket_code',r.bucket_code,'unit_name',r.unit_name,'units',r.units,'rate',r.rate,
          'amount_ex_vat',r.amount_ex_vat,'amount_vat',r.amount_vat,
          'amount_inc_vat',r.amount_inc_vat,'meta_json',r.meta_json,
          'operation_source_key',r.operation_source_key)::text),v_reason
      FROM new_rows AS r
      JOIN public.pay_batch_items AS item ON item.id=r.pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
      LEFT JOIN public.pay_finance_case_components AS component ON component.id=item.finance_component_id
      LEFT JOIN public.timesheets_financials AS current_financial
        ON current_financial.timesheet_id=COALESCE(item.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,current_financial.candidate_id,component.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_batch_timesheet_snapshots' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.timesheet_id,
        md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'timesheet_id',r.timesheet_id,
          'candidate_id',r.candidate_id,'pay_channel',r.pay_channel,
          'base_snapshot_json',r.base_snapshot_json,'target_snapshot_json',r.target_snapshot_json,
          'signature',r.signature)::text),NULL,v_reason FROM old_rows r ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.timesheet_id,NULL,
        md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'timesheet_id',r.timesheet_id,
          'candidate_id',r.candidate_id,'pay_channel',r.pay_channel,
          'base_snapshot_json',r.base_snapshot_json,'target_snapshot_json',r.target_snapshot_json,
          'signature',r.signature)::text),v_reason FROM new_rows r
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_bank_transfer_events' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(r.candidate_id,batch_candidate.candidate_id),item.timesheet_id,
        md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'pay_bank_transfer_id',r.pay_bank_transfer_id,
          'candidate_id',r.candidate_id,'provider_key',r.provider_key,
          'provider_event_id',r.provider_event_id,'provider_reference',r.provider_reference,
          'provider_state',r.provider_state,'normalised_state',r.normalised_state,
          'amount',r.amount,'currency',r.currency,'mapping_status',r.mapping_status,
          'movement_classification',r.movement_classification,
          'correction_disposition',r.correction_disposition,'mapping_method',r.mapping_method,
          'provider_event_type',r.provider_event_type,'provider_transaction_id',r.provider_transaction_id,
          'provider_failure_reason_code',r.provider_failure_reason_code,
          'provider_failure_reason_group',r.provider_failure_reason_group)::text),NULL,v_reason
      FROM old_rows r
      LEFT JOIN public.pay_batch_items item ON item.pay_bank_transfer_id=r.pay_bank_transfer_id
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
      WHERE COALESCE(r.candidate_id,batch_candidate.candidate_id) IS NOT NULL
      ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(r.candidate_id,batch_candidate.candidate_id),item.timesheet_id,NULL,
        md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'pay_bank_transfer_id',r.pay_bank_transfer_id,
          'candidate_id',r.candidate_id,'provider_key',r.provider_key,
          'provider_event_id',r.provider_event_id,'provider_reference',r.provider_reference,
          'provider_state',r.provider_state,'normalised_state',r.normalised_state,
          'amount',r.amount,'currency',r.currency,'mapping_status',r.mapping_status,
          'movement_classification',r.movement_classification,
          'correction_disposition',r.correction_disposition,'mapping_method',r.mapping_method,
          'provider_event_type',r.provider_event_type,'provider_transaction_id',r.provider_transaction_id,
          'provider_failure_reason_code',r.provider_failure_reason_code,
          'provider_failure_reason_group',r.provider_failure_reason_group)::text),v_reason
      FROM new_rows r
      LEFT JOIN public.pay_batch_items item ON item.pay_bank_transfer_id=r.pay_bank_transfer_id
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=item.pay_batch_candidate_id
      WHERE COALESCE(r.candidate_id,batch_candidate.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_advance_reservations' THEN
    IF TG_OP IN ('UPDATE','DELETE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,component.candidate_id,current_financial.candidate_id),
        COALESCE(item.timesheet_id,component.linked_timesheet_id),
        md5(jsonb_build_object('finance_case_id',r.finance_case_id,'pay_batch_id',r.pay_batch_id,
          'pay_batch_candidate_id',r.pay_batch_candidate_id,'pay_batch_item_id',r.pay_batch_item_id,
          'finance_component_id',r.finance_component_id,'reserved_amount',r.reserved_amount,
          'repayment_week_start',r.repayment_week_start,'status',r.status,
          'committed_at_utc',r.committed_at_utc,'settled_at_utc',r.settled_at_utc,
          'released_at_utc',r.released_at_utc,'frozen_component_snapshot_json',r.frozen_component_snapshot_json,
          'frozen_component_key_type',r.frozen_component_key_type,
          'frozen_component_key_value',r.frozen_component_key_value,
          'frozen_component_classification',r.frozen_component_classification,
          'frozen_source_basis_json',r.frozen_source_basis_json,
          'frozen_source_pay_method',r.frozen_source_pay_method,
          'frozen_target_pay_method',r.frozen_target_pay_method,
          'frozen_resolution_mode',r.frozen_resolution_mode,
          'frozen_resolution_payload_json',r.frozen_resolution_payload_json,
          'frozen_resolution_result_json',r.frozen_resolution_result_json,
          'reserved_source_amount',r.reserved_source_amount,
          'frozen_rounded_target_amount',r.frozen_rounded_target_amount)::text),NULL,v_reason
      FROM old_rows r
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=r.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_items item ON item.id=r.pay_batch_item_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=r.finance_component_id
      LEFT JOIN public.timesheets_financials current_financial
        ON current_financial.timesheet_id=COALESCE(item.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,component.candidate_id,current_financial.candidate_id) IS NOT NULL
      ON CONFLICT DO NOTHING;
    END IF;
    IF TG_OP IN ('INSERT','UPDATE') THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,
        COALESCE(batch_candidate.candidate_id,component.candidate_id,current_financial.candidate_id),
        COALESCE(item.timesheet_id,component.linked_timesheet_id),NULL,
        md5(jsonb_build_object('finance_case_id',r.finance_case_id,'pay_batch_id',r.pay_batch_id,
          'pay_batch_candidate_id',r.pay_batch_candidate_id,'pay_batch_item_id',r.pay_batch_item_id,
          'finance_component_id',r.finance_component_id,'reserved_amount',r.reserved_amount,
          'repayment_week_start',r.repayment_week_start,'status',r.status,
          'committed_at_utc',r.committed_at_utc,'settled_at_utc',r.settled_at_utc,
          'released_at_utc',r.released_at_utc,'frozen_component_snapshot_json',r.frozen_component_snapshot_json,
          'frozen_component_key_type',r.frozen_component_key_type,
          'frozen_component_key_value',r.frozen_component_key_value,
          'frozen_component_classification',r.frozen_component_classification,
          'frozen_source_basis_json',r.frozen_source_basis_json,
          'frozen_source_pay_method',r.frozen_source_pay_method,
          'frozen_target_pay_method',r.frozen_target_pay_method,
          'frozen_resolution_mode',r.frozen_resolution_mode,
          'frozen_resolution_payload_json',r.frozen_resolution_payload_json,
          'frozen_resolution_result_json',r.frozen_resolution_result_json,
          'reserved_source_amount',r.reserved_source_amount,
          'frozen_rounded_target_amount',r.frozen_rounded_target_amount)::text),v_reason
      FROM new_rows r
      LEFT JOIN public.pay_batch_candidates batch_candidate ON batch_candidate.id=r.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_items item ON item.id=r.pay_batch_item_id
      LEFT JOIN public.pay_finance_case_components component ON component.id=r.finance_component_id
      LEFT JOIN public.timesheets_financials current_financial
        ON current_financial.timesheet_id=COALESCE(item.timesheet_id,component.linked_timesheet_id)
       AND current_financial.is_current
      WHERE COALESCE(batch_candidate.candidate_id,component.candidate_id,current_financial.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    END IF;

  ELSIF TG_TABLE_NAME='pay_batch_candidates' AND TG_OP='DELETE' THEN
    INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
    SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,NULL,
      md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'candidate_id',r.candidate_id)::text),NULL,v_reason
    FROM old_rows r ON CONFLICT DO NOTHING;

  ELSIF TG_TABLE_NAME='pay_bank_transfers' AND TG_OP='DELETE' THEN
    INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
    SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,item.timesheet_id,
      md5(jsonb_build_object('pay_batch_id',r.pay_batch_id,'candidate_id',r.candidate_id,
        'pay_channel',r.pay_channel,'amount',r.amount,'currency',r.currency,'status',r.status,
        'rail_provider',r.rail_provider,'rail_env',r.rail_env,'rail_state',r.rail_state,
        'transfer_group_key',r.transfer_group_key,'grouping_mode_used',r.grouping_mode_used,
        'week_ending_bucket',r.week_ending_bucket)::text),NULL,v_reason
    FROM old_rows r LEFT JOIN public.pay_batch_items item ON item.pay_bank_transfer_id=r.id
    WHERE r.candidate_id IS NOT NULL ON CONFLICT DO NOTHING;

  ELSIF TG_TABLE_NAME='pay_batches' AND TG_OP='DELETE' THEN
    -- Exact child statement triggers carry candidate/timesheet effects. Empty batches
    -- have no Banking Pay economic scope, so this parent backstop intentionally emits none.
    NULL;
  ELSE
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_TRANSITION_IDENTITY_UNRESOLVED'
      USING ERRCODE='22023',DETAIL=TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME||':'||TG_OP;
  END IF;

  -- UPDATE rows are economically relevant only where the typed digest changed.
  DELETE FROM pg_temp._bpay_wb_transition_impacts_v1
  WHERE operation='UPDATE' AND before_digest IS NOT DISTINCT FROM after_digest;

  -- A candidate-delete cascade is suppressible only through the exact owner-created,
  -- backend/transaction-bound context installed by candidate_delete_apply.
  IF v_delete_context IS NOT NULL THEN
    SELECT relowner INTO v_relation_owner FROM pg_catalog.pg_class WHERE oid=v_delete_context;
    IF v_relation_owner=current_user::regrole::oid
       AND EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=v_delete_context AND relation.relpersistence='t'
           AND relation.relnamespace=pg_catalog.pg_my_temp_schema())
       AND (SELECT array_agg(attribute.attname||':'||pg_catalog.format_type(attribute.atttypid,attribute.atttypmod)
              ORDER BY attribute.attnum)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid=v_delete_context AND attribute.attnum>0 AND NOT attribute.attisdropped)
          =ARRAY['candidate_id:uuid','delete_operation_id:uuid','candidate_lock_key:bigint',
            'backend_pid:integer','transaction_id:bigint','created_at_utc:timestamp with time zone',
            'suppress:boolean'] THEN
      EXECUTE $sql$
        DELETE FROM pg_temp._bpay_wb_transition_impacts_v1 AS impact
        USING pg_temp._bpay_candidate_delete_context_v1 AS context
        WHERE impact.operation='DELETE'
          AND impact.candidate_id=context.candidate_id
          AND context.suppress
          AND context.candidate_lock_key=pg_catalog.hashtextextended(
            public._pay_workbench_candidate_serial_key(context.candidate_id),24062027)
          AND context.backend_pid=pg_catalog.pg_backend_pid()
          AND context.transaction_id=pg_catalog.txid_current()
          AND (SELECT count(*) FROM pg_temp._bpay_candidate_delete_context_v1)=1
      $sql$;
    END IF;
  END IF;

  -- Finance effects produced by the validated build are consumed only when their
  -- full identity and typed before/after digests match one owner-created row.
  IF v_expected IS NOT NULL THEN
    SELECT relowner INTO v_relation_owner FROM pg_catalog.pg_class WHERE oid=v_expected;
    IF v_relation_owner=current_user::regrole::oid
       AND EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=v_expected AND relation.relpersistence='t'
           AND relation.relnamespace=pg_catalog.pg_my_temp_schema())
       AND (SELECT array_agg(attribute.attname ORDER BY attribute.attnum)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid=v_expected AND attribute.attnum>0 AND NOT attribute.attisdropped)
          =ARRAY['build_token','candidate_id','timesheet_id','relation_name','operation','source_id',
            'actual_source_id','finance_case_id','finance_component_id','economic_key_type','economic_key_value',
            'proposed','expected_before_digest','expected_after_digest','observed'] THEN
      EXECUTE $sql$
        SELECT EXISTS(
          SELECT 1
          FROM pg_temp._bpay_wb_transition_impacts_v1 impact
          JOIN pg_temp._bpay_wb_expected_effects expected
            ON expected.relation_name=impact.relation_name
           AND expected.operation=impact.operation
           AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
           AND expected.candidate_id=impact.candidate_id
           AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
           AND expected.expected_before_digest IS NOT DISTINCT FROM impact.before_digest
           AND expected.expected_after_digest IS NOT DISTINCT FROM impact.after_digest
           AND expected.proposed IS TRUE
           AND expected.observed IS NOT TRUE
          GROUP BY impact.relation_name,impact.operation,impact.source_id,impact.candidate_id,impact.timesheet_id
          HAVING count(*)<>1
        )
      $sql$ INTO v_duplicate;
      IF v_duplicate THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      EXECUTE $sql$
        UPDATE pg_temp._bpay_wb_expected_effects expected SET observed=true
        FROM pg_temp._bpay_wb_transition_impacts_v1 impact
        WHERE expected.relation_name=impact.relation_name
          AND expected.operation=impact.operation
          AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
          AND expected.candidate_id=impact.candidate_id
          AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
          AND expected.expected_before_digest IS NOT DISTINCT FROM impact.before_digest
          AND expected.expected_after_digest IS NOT DISTINCT FROM impact.after_digest
          AND expected.proposed IS TRUE
          AND expected.observed IS NOT TRUE
      $sql$;
      EXECUTE $sql$
        DELETE FROM pg_temp._bpay_wb_transition_impacts_v1 impact
        USING pg_temp._bpay_wb_expected_effects expected
        WHERE expected.relation_name=impact.relation_name
          AND expected.operation=impact.operation
          AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
          AND expected.candidate_id=impact.candidate_id
          AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
          AND expected.expected_before_digest IS NOT DISTINCT FROM impact.before_digest
          AND expected.proposed IS TRUE
          AND expected.observed IS TRUE
          AND expected.expected_after_digest IS NOT DISTINCT FROM impact.after_digest
      $sql$;
      IF EXISTS(SELECT 1 FROM pg_temp._bpay_wb_transition_impacts_v1) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH'
          USING ERRCODE='23514';
      END IF;
    END IF;
  END IF;

  SELECT array_agg(candidate_id ORDER BY candidate_id,timesheet_id NULLS FIRST),
         array_agg(timesheet_id ORDER BY candidate_id,timesheet_id NULLS FIRST)
  INTO v_candidate_ids,v_timesheet_ids
  FROM (
    SELECT DISTINCT candidate_id,timesheet_id
    FROM pg_temp._bpay_wb_transition_impacts_v1
  ) AS impacted;

  IF COALESCE(cardinality(v_candidate_ids),0)>0 THEN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      v_candidate_ids,v_timesheet_ids,v_reason,NULL,
      jsonb_build_object('source_relation',TG_TABLE_NAME,'source_operation',TG_OP)
    );
  END IF;
  RETURN NULL;
END;
$function$;

ALTER FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() TO postgres;
