-- Banking Pay bounded-scope V1.2.4: set-based financial transition adapter.
-- Financial source triggers only identify exact impacted owners; they never run finance.

-- The repository applies repeatables in UK date order.  Install the later-dated
-- causal-context authority before any changed legacy caller is compiled so a
-- clean database and an existing TEST database follow the same dependency order.
\ir 10082026_2345_banking_pay_correction_owned_dirty_context.sql

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
  v_draft_context regclass:=pg_catalog.to_regclass('pg_temp._bpay_wb_draft_expected_effect_context_v1');
  v_draft_observed regclass:=pg_catalog.to_regclass('pg_temp._bpay_wb_draft_observed_effects_v1');
  v_delete_context regclass:=pg_catalog.to_regclass('pg_temp._bpay_candidate_delete_context_v1');
  v_relation_owner oid;
  v_duplicate boolean:=false;
  v_mismatch_detail jsonb:='{}'::jsonb;
  v_draft_operation_id uuid:=NULL::uuid;
  v_draft_phase text:=NULL::text;
  v_draft_context_token text:=NULL::text;
  v_correction_dirty_contexts jsonb:='{}'::jsonb;
  v_correction_context_count integer:=0;
  v_execution_overlay_context regclass:=pg_catalog.to_regclass(
    'pg_temp._bpay_wb_unsent_execution_overlay_context_v1');
  v_execution_overlay_contexts jsonb:='{}'::jsonb;
  v_execution_overlay_context_count integer:=0;
  v_execution_overlay_exact boolean:=false;
  v_execution_schedule_context regclass:=pg_catalog.to_regclass(
    'pg_temp._bpay_wb_unsent_execution_schedule_context_v2');
  v_execution_schedule_contexts jsonb:='{}'::jsonb;
  v_execution_schedule_context_count integer:=0;
  v_execution_schedule_exact boolean:=false;
  v_impacted_candidate_count integer:=0;
  v_scope_change_tx_token uuid:=NULL::uuid;
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
    IF TG_OP='INSERT' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text),v_reason FROM new_rows r
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    ELSIF TG_OP='UPDATE' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(old_row))::text),v_reason
      FROM new_rows r JOIN old_rows old_row ON old_row.id=r.id
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
    IF TG_OP='INSERT' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text),v_reason FROM new_rows r
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    ELSIF TG_OP='UPDATE' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,r.candidate_id,r.linked_timesheet_id,
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(old_row))::text),v_reason
      FROM new_rows r JOIN old_rows old_row ON old_row.id=r.id
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
    IF TG_OP='INSERT' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(component.candidate_id,finance_case.candidate_id),
        COALESCE(component.linked_timesheet_id,finance_case.linked_timesheet_id),
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),'{}'::jsonb)::text),v_reason FROM new_rows r
      LEFT JOIN public.pay_finance_case_components component ON component.id=r.finance_component_id
      LEFT JOIN public.pay_advances finance_case ON finance_case.id=r.finance_case_id
      WHERE COALESCE(component.candidate_id,finance_case.candidate_id) IS NOT NULL
      ON CONFLICT(relation_name,operation,source_id,candidate_id,timesheet_id) DO UPDATE
      SET after_digest=EXCLUDED.after_digest;
    ELSIF TG_OP='UPDATE' THEN
      INSERT INTO pg_temp._bpay_wb_transition_impacts_v1
      SELECT TG_TABLE_NAME,TG_OP,r.id,COALESCE(component.candidate_id,finance_case.candidate_id),
        COALESCE(component.linked_timesheet_id,finance_case.linked_timesheet_id),
        NULL,md5(private.pay_workbench_finance_effect_normalise_row_v1(
          TG_TABLE_NAME,TG_OP,to_jsonb(r),to_jsonb(old_row))::text),v_reason
      FROM new_rows r JOIN old_rows old_row ON old_row.id=r.id
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

  -- Canonical Draft writers install a transaction-local operation context.
  -- Only effects belonging to that exact operation/batch/candidate scope and
  -- an allow-listed relation/action are consumed.  Everything else remains in
  -- the normal dirty path and is recorded as an unmatched effect so the writer
  -- fails closed at its phase assertion.
  IF v_draft_context IS NOT NULL AND v_draft_observed IS NOT NULL THEN
    SELECT relowner INTO v_relation_owner FROM pg_catalog.pg_class WHERE oid=v_draft_context;
    IF v_relation_owner=current_user::regrole::oid
       AND EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=v_draft_context AND relation.relpersistence='t'
           AND relation.relnamespace=pg_catalog.pg_my_temp_schema())
       AND (SELECT array_agg(attribute.attname::text ORDER BY attribute.attnum)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid=v_draft_context AND attribute.attnum>0 AND NOT attribute.attisdropped)
          =ARRAY['operation_id','pay_batch_id','workbench_session_id','phase','backend_pid',
            'transaction_id','registered_at_utc','expected_effects_json','context_token']
       AND EXISTS(SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=v_draft_observed AND relation.relpersistence='t'
           AND relation.relnamespace=pg_catalog.pg_my_temp_schema())
       AND (SELECT array_agg(attribute.attname::text ORDER BY attribute.attnum)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid=v_draft_observed AND attribute.attnum>0 AND NOT attribute.attisdropped)
          =ARRAY['operation_id','pay_batch_id','phase','relation_name','operation','source_id',
            'candidate_id','timesheet_id','before_digest','after_digest','matched','observed_at_utc'] THEN
      SELECT context_row.operation_id,context_row.phase,context_row.context_token
      INTO v_draft_operation_id,v_draft_phase,v_draft_context_token
      FROM pg_temp._bpay_wb_draft_expected_effect_context_v1 AS context_row
      WHERE context_row.backend_pid=pg_catalog.pg_backend_pid()
        AND context_row.transaction_id=pg_catalog.txid_current()
      LIMIT 1;

      IF v_draft_operation_id IS NOT NULL THEN
        INSERT INTO pg_temp._bpay_wb_draft_observed_effects_v1(
          operation_id,pay_batch_id,phase,relation_name,operation,source_id,
          candidate_id,timesheet_id,before_digest,after_digest,matched,observed_at_utc
        )
        SELECT
          context_row.operation_id,context_row.pay_batch_id,context_row.phase,
          impact.relation_name,impact.operation,impact.source_id,impact.candidate_id,
          impact.timesheet_id,impact.before_digest,impact.after_digest,
          (
            EXISTS(
              SELECT 1
              FROM pg_catalog.jsonb_array_elements(context_row.expected_effects_json) AS expected(value)
              WHERE pg_catalog.lower(pg_catalog.btrim(COALESCE(expected.value->>'relation_name','')))=impact.relation_name
                AND pg_catalog.upper(pg_catalog.btrim(COALESCE(expected.value->>'operation','')))=impact.operation
            )
            AND EXISTS(
              SELECT 1
              FROM public.banking_pay_operation_candidate_scope AS operation_scope
              WHERE operation_scope.operation_id=context_row.operation_id
                AND operation_scope.pay_batch_id=context_row.pay_batch_id
                AND operation_scope.candidate_id=impact.candidate_id
                AND operation_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
            )
            AND CASE impact.relation_name
              WHEN 'pay_batch_items' THEN EXISTS(
                SELECT 1 FROM public.pay_batch_items item
                JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
                WHERE item.id=impact.source_id AND candidate.pay_batch_id=context_row.pay_batch_id
              ) OR impact.operation='DELETE'
              WHEN 'pay_batch_item_breakdowns' THEN EXISTS(
                SELECT 1 FROM public.pay_batch_item_breakdowns breakdown
                JOIN public.pay_batch_items item ON item.id=breakdown.pay_batch_item_id
                JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
                WHERE breakdown.id=impact.source_id AND candidate.pay_batch_id=context_row.pay_batch_id
              ) OR impact.operation='DELETE'
              WHEN 'pay_batch_timesheet_snapshots' THEN EXISTS(
                SELECT 1 FROM public.pay_batch_timesheet_snapshots snapshot
                WHERE snapshot.id=impact.source_id AND snapshot.pay_batch_id=context_row.pay_batch_id
              )
              WHEN 'pay_advance_reservations' THEN EXISTS(
                SELECT 1 FROM public.pay_advance_reservations reservation
                WHERE reservation.id=impact.source_id AND reservation.pay_batch_id=context_row.pay_batch_id
              ) OR impact.operation='DELETE'
              WHEN 'pay_finance_case_events' THEN EXISTS(
                SELECT 1 FROM public.pay_finance_case_events event
                WHERE event.id=impact.source_id AND event.pay_batch_id=context_row.pay_batch_id
              ) OR impact.operation='DELETE'
              WHEN 'pay_batch_candidates' THEN impact.operation='DELETE'
              ELSE impact.relation_name IN ('pay_advances','pay_finance_case_components')
            END
          ),
          pg_catalog.clock_timestamp()
        FROM pg_temp._bpay_wb_transition_impacts_v1 AS impact
        CROSS JOIN pg_temp._bpay_wb_draft_expected_effect_context_v1 AS context_row
        WHERE context_row.operation_id=v_draft_operation_id
        ON CONFLICT(operation_id,phase,relation_name,operation,source_id,candidate_id)
        DO UPDATE SET
          timesheet_id=EXCLUDED.timesheet_id,
          before_digest=EXCLUDED.before_digest,
          after_digest=EXCLUDED.after_digest,
          matched=EXCLUDED.matched,
          observed_at_utc=EXCLUDED.observed_at_utc;

        DELETE FROM pg_temp._bpay_wb_transition_impacts_v1 AS impact
        USING pg_temp._bpay_wb_draft_observed_effects_v1 AS observed
        WHERE observed.operation_id=v_draft_operation_id
          AND observed.phase=v_draft_phase
          AND observed.matched
          AND observed.relation_name=impact.relation_name
          AND observed.operation=impact.operation
          AND observed.source_id=impact.source_id
          AND observed.candidate_id=impact.candidate_id;
      END IF;
    END IF;
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
       AND (SELECT array_agg(attribute.attname::text ORDER BY attribute.attnum)
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
        SELECT jsonb_build_object(
          'impact_count',(SELECT count(*) FROM pg_temp._bpay_wb_transition_impacts_v1),
          'expected_count',(SELECT count(*) FROM pg_temp._bpay_wb_expected_effects),
          'identity_match_count',(
            SELECT count(*)
            FROM pg_temp._bpay_wb_transition_impacts_v1 impact
            WHERE EXISTS(
              SELECT 1 FROM pg_temp._bpay_wb_expected_effects expected
              WHERE expected.relation_name=impact.relation_name
                AND expected.operation=impact.operation
                AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
                AND expected.candidate_id=impact.candidate_id
                AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
            )
          ),
          'before_match_count',(
            SELECT count(*)
            FROM pg_temp._bpay_wb_transition_impacts_v1 impact
            WHERE EXISTS(
              SELECT 1 FROM pg_temp._bpay_wb_expected_effects expected
              WHERE expected.relation_name=impact.relation_name
                AND expected.operation=impact.operation
                AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
                AND expected.candidate_id=impact.candidate_id
                AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
                AND expected.expected_before_digest IS NOT DISTINCT FROM impact.before_digest
            )
          ),
          'after_match_count',(
            SELECT count(*)
            FROM pg_temp._bpay_wb_transition_impacts_v1 impact
            WHERE EXISTS(
              SELECT 1 FROM pg_temp._bpay_wb_expected_effects expected
              WHERE expected.relation_name=impact.relation_name
                AND expected.operation=impact.operation
                AND COALESCE(expected.actual_source_id,expected.source_id)=impact.source_id
                AND expected.candidate_id=impact.candidate_id
                AND expected.timesheet_id IS NOT DISTINCT FROM impact.timesheet_id
                AND expected.expected_after_digest IS NOT DISTINCT FROM impact.after_digest
            )
          ),
          'by_relation_operation',COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
              'relation_name',grouped.relation_name,
              'operation',grouped.operation,
              'impact_count',grouped.impact_count
            ) ORDER BY grouped.relation_name,grouped.operation)
            FROM (
              SELECT impact.relation_name,impact.operation,count(*) AS impact_count
              FROM pg_temp._bpay_wb_transition_impacts_v1 impact
              GROUP BY impact.relation_name,impact.operation
            ) grouped
          ),'[]'::jsonb)
        ) INTO v_mismatch_detail;
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH %',v_mismatch_detail::text
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
    SELECT pg_catalog.count(DISTINCT candidate_id)::integer
    INTO v_impacted_candidate_count
    FROM pg_temp._bpay_wb_transition_impacts_v1;

    -- The execution owner deliberately links otherwise frozen batch items to
    -- newly prepared local transfer rows.  Preserve normal invalidation, but
    -- stamp it only when the complete UPDATE statement is exactly represented
    -- by the owner-created transaction-local context and no other column moved.
    IF TG_TABLE_NAME='pay_batch_items' AND TG_OP='UPDATE'
       AND v_execution_overlay_context IS NOT NULL THEN
      SELECT relation.relowner
      INTO v_relation_owner
      FROM pg_catalog.pg_class AS relation
      WHERE relation.oid=v_execution_overlay_context;

      IF v_relation_owner=current_user::regrole::oid
         AND EXISTS (
           SELECT 1 FROM pg_catalog.pg_class AS relation
           WHERE relation.oid=v_execution_overlay_context
             AND relation.relpersistence='t'
             AND relation.relnamespace=pg_catalog.pg_my_temp_schema()
         )
         AND (SELECT pg_catalog.array_agg(
                attribute.attname||':'||pg_catalog.format_type(attribute.atttypid,attribute.atttypmod)
                ORDER BY attribute.attnum)
              FROM pg_catalog.pg_attribute AS attribute
              WHERE attribute.attrelid=v_execution_overlay_context
                AND attribute.attnum>0 AND NOT attribute.attisdropped)
             =ARRAY[
               'contract_version:text','execution_operation_id:uuid','pay_batch_id:uuid',
               'pay_batch_candidate_id:uuid','candidate_id:uuid','timesheet_id:uuid',
               'pay_batch_item_id:uuid','pay_bank_transfer_id:uuid','transfer_scope_id:uuid',
               'source_workbench_session_id:uuid','source_snapshot_run_id:uuid',
               'source_session_version:bigint','row_context_digest:text',
               'created_at_utc:timestamp with time zone'
             ] THEN
        SELECT
          NOT EXISTS (
            SELECT 1
            FROM new_rows AS new_row
            JOIN old_rows AS old_row ON old_row.id=new_row.id
            LEFT JOIN pg_temp._bpay_wb_unsent_execution_overlay_context_v1 AS context_row
              ON context_row.pay_batch_item_id=new_row.id
             AND context_row.pay_batch_candidate_id=new_row.pay_batch_candidate_id
             AND context_row.timesheet_id IS NOT DISTINCT FROM new_row.timesheet_id
             AND context_row.pay_bank_transfer_id=new_row.pay_bank_transfer_id
            WHERE old_row.pay_bank_transfer_id IS NOT NULL
               OR new_row.pay_bank_transfer_id IS NULL
               OR context_row.pay_batch_item_id IS NULL
               OR (pg_catalog.to_jsonb(new_row)-'pay_bank_transfer_id'-'updated_at')
                    IS DISTINCT FROM
                  (pg_catalog.to_jsonb(old_row)-'pay_bank_transfer_id'-'updated_at')
          )
          AND (SELECT pg_catalog.count(*) FROM new_rows)
              =(SELECT pg_catalog.count(*)
                FROM pg_temp._bpay_wb_unsent_execution_overlay_context_v1),
          (SELECT pg_catalog.count(*)::integer
           FROM pg_temp._bpay_wb_unsent_execution_overlay_context_v1)
        INTO v_execution_overlay_exact,v_execution_overlay_context_count;

        IF v_execution_overlay_exact AND v_execution_overlay_context_count>0 THEN
          SELECT COALESCE(pg_catalog.jsonb_object_agg(
                   candidate_group.candidate_id::text,
                   pg_catalog.jsonb_build_object(
                     'contract_version','EXECUTION_UNSENT_OVERLAY_CONTEXT_V1',
                     'execution_operation_id',candidate_group.execution_operation_id,
                     'pay_batch_id',candidate_group.pay_batch_id,
                     'candidate_id',candidate_group.candidate_id,
                     'pay_batch_candidate_ids',candidate_group.pay_batch_candidate_ids,
                     'pay_batch_item_ids',candidate_group.pay_batch_item_ids,
                     'timesheet_ids',candidate_group.timesheet_ids,
                     'transfer_scope_ids',candidate_group.transfer_scope_ids,
                     'pay_bank_transfer_ids',candidate_group.pay_bank_transfer_ids,
                     'source_workbench_session_id',candidate_group.source_workbench_session_id,
                     'source_snapshot_run_id',candidate_group.source_snapshot_run_id,
                     'source_session_version',candidate_group.source_session_version,
                     'context_digest',pg_catalog.md5(
                       candidate_group.execution_operation_id::text||'|'||
                       candidate_group.pay_batch_id::text||'|'||candidate_group.candidate_id::text||'|'||
                       candidate_group.pay_batch_candidate_ids::text||'|'||
                       candidate_group.pay_batch_item_ids::text||'|'||candidate_group.timesheet_ids::text||'|'||
                       candidate_group.transfer_scope_ids::text||'|'||
                       candidate_group.pay_bank_transfer_ids::text||'|'||
                       COALESCE(candidate_group.source_workbench_session_id::text,'')||'|'||
                       COALESCE(candidate_group.source_snapshot_run_id::text,'')||'|'||
                       COALESCE(candidate_group.source_session_version::text,'')||
                       '|EXECUTION_UNSENT_OVERLAY_CONTEXT_V1'
                     )
                   ) ORDER BY candidate_group.candidate_id
                 ),'{}'::jsonb)
          INTO v_execution_overlay_contexts
          FROM (
            SELECT context_row.candidate_id,
              pg_catalog.min(context_row.execution_operation_id::text)::uuid AS execution_operation_id,
              pg_catalog.min(context_row.pay_batch_id::text)::uuid AS pay_batch_id,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_batch_candidate_id::text
                ORDER BY context_row.pay_batch_candidate_id::text) AS pay_batch_candidate_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_batch_item_id::text
                ORDER BY context_row.pay_batch_item_id::text) AS pay_batch_item_ids,
              COALESCE(pg_catalog.jsonb_agg(DISTINCT context_row.timesheet_id::text
                ORDER BY context_row.timesheet_id::text)
                FILTER (WHERE context_row.timesheet_id IS NOT NULL),'[]'::jsonb) AS timesheet_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.transfer_scope_id::text
                ORDER BY context_row.transfer_scope_id::text) AS transfer_scope_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_bank_transfer_id::text
                ORDER BY context_row.pay_bank_transfer_id::text) AS pay_bank_transfer_ids,
              pg_catalog.min(context_row.source_workbench_session_id::text)::uuid AS source_workbench_session_id,
              pg_catalog.min(context_row.source_snapshot_run_id::text)::uuid AS source_snapshot_run_id,
              pg_catalog.min(context_row.source_session_version) AS source_session_version
            FROM pg_temp._bpay_wb_unsent_execution_overlay_context_v1 AS context_row
            GROUP BY context_row.candidate_id
            HAVING pg_catalog.count(DISTINCT context_row.execution_operation_id)=1
               AND pg_catalog.count(DISTINCT context_row.pay_batch_id)=1
               AND pg_catalog.count(DISTINCT context_row.source_workbench_session_id)<=1
               AND pg_catalog.count(DISTINCT context_row.source_snapshot_run_id)<=1
               AND pg_catalog.count(DISTINCT context_row.source_session_version)<=1
          ) AS candidate_group;

          IF (SELECT pg_catalog.count(*)
                FROM pg_catalog.jsonb_object_keys(v_execution_overlay_contexts))
               <>v_impacted_candidate_count THEN
            v_execution_overlay_exact:=false;
            v_execution_overlay_contexts:='{}'::jsonb;
          ELSE
            v_scope_change_tx_token:=public.pay_workbench_scope_change_tx_token_v1();
          END IF;
        END IF;
      END IF;
    END IF;

    -- The exact PAYMENT_EXECUTE scheduling owner may create one further
    -- finalized candidate generation while committing frozen reservations and
    -- their audit events.  Stamp only the two writer statements whose complete
    -- transition tables match that owner-created temporary scope.  Other
    -- finance mutations remain ordinary/unowned dirtiness and therefore make
    -- the later certified route fail closed.
    IF v_execution_schedule_context IS NOT NULL
       AND ((TG_TABLE_NAME='pay_advance_reservations' AND TG_OP='UPDATE')
         OR (TG_TABLE_NAME='pay_finance_case_events' AND TG_OP='INSERT')) THEN
      SELECT relation.relowner INTO v_relation_owner
      FROM pg_catalog.pg_class AS relation
      WHERE relation.oid=v_execution_schedule_context;

      IF v_relation_owner=current_user::regrole::oid
         AND EXISTS (
           SELECT 1 FROM pg_catalog.pg_class AS relation
           WHERE relation.oid=v_execution_schedule_context
             AND relation.relpersistence='t'
             AND relation.relnamespace=pg_catalog.pg_my_temp_schema()
         )
         AND (SELECT pg_catalog.array_agg(
                attribute.attname||':'||pg_catalog.format_type(attribute.atttypid,attribute.atttypmod)
                ORDER BY attribute.attnum)
              FROM pg_catalog.pg_attribute AS attribute
              WHERE attribute.attrelid=v_execution_schedule_context
                AND attribute.attnum>0 AND NOT attribute.attisdropped)
             =ARRAY[
               'contract_version:text','execution_operation_id:uuid','pay_batch_id:uuid',
               'actor_user_id:uuid','pay_batch_candidate_id:uuid','candidate_id:uuid',
               'timesheet_id:uuid','pay_batch_item_id:uuid','pay_bank_transfer_id:uuid',
               'transfer_scope_id:uuid','reservation_id:uuid','finance_case_id:uuid',
               'finance_component_id:uuid','source_workbench_session_id:uuid',
               'source_snapshot_run_id:uuid','source_session_version:bigint',
               'row_context_digest:text','created_at_utc:timestamp with time zone'
             ] THEN
        IF TG_TABLE_NAME='pay_advance_reservations' THEN
          SELECT NOT EXISTS (
              SELECT 1
              FROM new_rows AS new_row
              JOIN old_rows AS old_row ON old_row.id=new_row.id
              LEFT JOIN pg_temp._bpay_wb_unsent_execution_schedule_context_v2 AS context_row
                ON context_row.reservation_id=new_row.id
               AND context_row.pay_batch_id=new_row.pay_batch_id
               AND context_row.pay_batch_candidate_id=new_row.pay_batch_candidate_id
               AND context_row.pay_batch_item_id=new_row.pay_batch_item_id
               AND context_row.finance_case_id=new_row.finance_case_id
               AND context_row.finance_component_id IS NOT DISTINCT FROM new_row.finance_component_id
              WHERE context_row.reservation_id IS NULL
                 OR pg_catalog.upper(pg_catalog.btrim(COALESCE(old_row.status,'')))<>'RESERVED'
                 OR pg_catalog.upper(pg_catalog.btrim(COALESCE(new_row.status,'')))<>'COMMITTED'
                 OR new_row.committed_at_utc IS NULL
                 OR (pg_catalog.to_jsonb(new_row)-'status'-'committed_at_utc'-'updated_by_user_id')
                      IS DISTINCT FROM
                    (pg_catalog.to_jsonb(old_row)-'status'-'committed_at_utc'-'updated_by_user_id')
            )
            AND (SELECT pg_catalog.count(DISTINCT new_row.id) FROM new_rows AS new_row)
              =(SELECT pg_catalog.count(DISTINCT context_row.reservation_id)
                FROM pg_temp._bpay_wb_unsent_execution_schedule_context_v2 AS context_row
                WHERE context_row.reservation_id IS NOT NULL
                  AND EXISTS (SELECT 1 FROM new_rows AS new_row
                    WHERE new_row.id=context_row.reservation_id))
          INTO v_execution_schedule_exact;
        ELSE
          SELECT NOT EXISTS (
              SELECT 1
              FROM new_rows AS new_row
              LEFT JOIN pg_temp._bpay_wb_unsent_execution_schedule_context_v2 AS context_row
                ON context_row.reservation_id=new_row.reservation_id
               AND context_row.pay_batch_id=new_row.pay_batch_id
               AND context_row.finance_case_id=new_row.finance_case_id
              WHERE context_row.reservation_id IS NULL
                 OR pg_catalog.upper(pg_catalog.btrim(COALESCE(new_row.event_type,'')))
                      <>'RESERVATION_COMMITTED'
                 OR COALESCE(new_row.reason,'')<>'schedule_commit'
                 OR new_row.actor_user_id IS DISTINCT FROM context_row.actor_user_id
            )
            AND (SELECT pg_catalog.count(*) FROM new_rows)
              =(SELECT pg_catalog.count(DISTINCT context_row.reservation_id)
                FROM pg_temp._bpay_wb_unsent_execution_schedule_context_v2 AS context_row
                WHERE context_row.reservation_id IS NOT NULL
                  AND EXISTS (SELECT 1 FROM new_rows AS new_row
                    WHERE new_row.reservation_id=context_row.reservation_id))
          INTO v_execution_schedule_exact;
        END IF;

        IF v_execution_schedule_exact THEN
          SELECT COALESCE(pg_catalog.jsonb_object_agg(
                   candidate_group.candidate_id::text,
                   pg_catalog.jsonb_build_object(
                     'contract_version','EXECUTION_UNSENT_SCHEDULE_CONTEXT_V2',
                     'execution_operation_id',candidate_group.execution_operation_id,
                     'pay_batch_id',candidate_group.pay_batch_id,
                     'candidate_id',candidate_group.candidate_id,
                     'pay_batch_candidate_ids',candidate_group.pay_batch_candidate_ids,
                     'pay_batch_item_ids',candidate_group.pay_batch_item_ids,
                     'timesheet_ids',candidate_group.timesheet_ids,
                     'transfer_scope_ids',candidate_group.transfer_scope_ids,
                     'pay_bank_transfer_ids',candidate_group.pay_bank_transfer_ids,
                     'reservation_ids',candidate_group.reservation_ids,
                     'finance_case_ids',candidate_group.finance_case_ids,
                     'finance_component_ids',candidate_group.finance_component_ids,
                     'source_workbench_session_id',candidate_group.source_workbench_session_id,
                     'source_snapshot_run_id',candidate_group.source_snapshot_run_id,
                     'source_session_version',candidate_group.source_session_version,
                     'context_digest',pg_catalog.md5(
                       candidate_group.execution_operation_id::text||'|'||
                       candidate_group.pay_batch_id::text||'|'||candidate_group.candidate_id::text||'|'||
                       candidate_group.pay_batch_candidate_ids::text||'|'||
                       candidate_group.pay_batch_item_ids::text||'|'||candidate_group.timesheet_ids::text||'|'||
                       candidate_group.transfer_scope_ids::text||'|'||
                       candidate_group.pay_bank_transfer_ids::text||'|'||
                       candidate_group.reservation_ids::text||'|'||candidate_group.finance_case_ids::text||'|'||
                       candidate_group.finance_component_ids::text||'|'||
                       COALESCE(candidate_group.source_workbench_session_id::text,'')||'|'||
                       COALESCE(candidate_group.source_snapshot_run_id::text,'')||'|'||
                       COALESCE(candidate_group.source_session_version::text,'')||
                       '|EXECUTION_UNSENT_SCHEDULE_CONTEXT_V2'
                     )
                   ) ORDER BY candidate_group.candidate_id
                 ),'{}'::jsonb),pg_catalog.count(*)::integer
          INTO v_execution_schedule_contexts,v_execution_schedule_context_count
          FROM (
            SELECT context_row.candidate_id,
              pg_catalog.min(context_row.execution_operation_id::text)::uuid AS execution_operation_id,
              pg_catalog.min(context_row.pay_batch_id::text)::uuid AS pay_batch_id,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_batch_candidate_id::text
                ORDER BY context_row.pay_batch_candidate_id::text) AS pay_batch_candidate_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_batch_item_id::text
                ORDER BY context_row.pay_batch_item_id::text) AS pay_batch_item_ids,
              COALESCE(pg_catalog.jsonb_agg(DISTINCT context_row.timesheet_id::text
                ORDER BY context_row.timesheet_id::text)
                FILTER (WHERE context_row.timesheet_id IS NOT NULL),'[]'::jsonb) AS timesheet_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.transfer_scope_id::text
                ORDER BY context_row.transfer_scope_id::text) AS transfer_scope_ids,
              pg_catalog.jsonb_agg(DISTINCT context_row.pay_bank_transfer_id::text
                ORDER BY context_row.pay_bank_transfer_id::text) AS pay_bank_transfer_ids,
              COALESCE(pg_catalog.jsonb_agg(DISTINCT context_row.reservation_id::text
                ORDER BY context_row.reservation_id::text)
                FILTER (WHERE context_row.reservation_id IS NOT NULL),'[]'::jsonb) AS reservation_ids,
              COALESCE(pg_catalog.jsonb_agg(DISTINCT context_row.finance_case_id::text
                ORDER BY context_row.finance_case_id::text)
                FILTER (WHERE context_row.finance_case_id IS NOT NULL),'[]'::jsonb) AS finance_case_ids,
              COALESCE(pg_catalog.jsonb_agg(DISTINCT context_row.finance_component_id::text
                ORDER BY context_row.finance_component_id::text)
                FILTER (WHERE context_row.finance_component_id IS NOT NULL),'[]'::jsonb)
                AS finance_component_ids,
              pg_catalog.min(context_row.source_workbench_session_id::text)::uuid
                AS source_workbench_session_id,
              pg_catalog.min(context_row.source_snapshot_run_id::text)::uuid
                AS source_snapshot_run_id,
              pg_catalog.min(context_row.source_session_version) AS source_session_version
            FROM pg_temp._bpay_wb_unsent_execution_schedule_context_v2 AS context_row
            WHERE context_row.candidate_id=ANY(v_candidate_ids)
            GROUP BY context_row.candidate_id
            HAVING pg_catalog.count(DISTINCT context_row.execution_operation_id)=1
               AND pg_catalog.count(DISTINCT context_row.pay_batch_id)=1
          ) AS candidate_group;

          IF v_execution_schedule_context_count<>v_impacted_candidate_count THEN
            v_execution_schedule_exact:=false;
            v_execution_schedule_contexts:='{}'::jsonb;
          ELSE
            v_scope_change_tx_token:=public.pay_workbench_scope_change_tx_token_v1();
          END IF;
        END IF;
      END IF;
    END IF;

    -- Financial DML performed by the correction page remains genuine audit
    -- evidence.  Where every impacted candidate belongs to the exact
    -- transaction-local correction context, stamp (never suppress) that
    -- causality so its durable dirty job can wait for route election.
    IF pg_catalog.to_regclass('pg_temp._bpay_wb_correction_dirty_context_v1') IS NOT NULL THEN
      EXECUTE $context$
        SELECT COALESCE(
                 pg_catalog.jsonb_object_agg(
                   context_row.candidate_id::text,
                   pg_catalog.to_jsonb(context_row)-'created_at_utc'
                   ORDER BY context_row.candidate_id
                 ),
                 '{}'::jsonb
               ),
               pg_catalog.count(*)::integer
        FROM pg_temp._bpay_wb_correction_dirty_context_v1 AS context_row
        WHERE context_row.candidate_id=ANY($1)
      $context$
      INTO v_correction_dirty_contexts,v_correction_context_count
      USING v_candidate_ids;

      IF v_correction_context_count=v_impacted_candidate_count
         AND v_correction_context_count>0 THEN
        v_scope_change_tx_token:=COALESCE(
          v_scope_change_tx_token,public.pay_workbench_scope_change_tx_token_v1());
      ELSE
        v_correction_dirty_contexts:='{}'::jsonb;
        v_scope_change_tx_token:=NULL::uuid;
      END IF;
    END IF;

    -- A candidate scope generation is global and may legitimately have gaps
    -- caused by other candidates.  Count the exact execution-owned source
    -- events per candidate/transaction instead.  The later V2 sealer compares
    -- this closed count with the per-candidate source sequence, so a committed
    -- intervening change cannot hide merely because it did not retain a
    -- candidate dirty-job row.
    IF (v_execution_overlay_exact OR v_execution_schedule_exact)
       AND v_scope_change_tx_token IS NOT NULL THEN
      CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_execution_owned_event_counter_v2(
        scope_change_tx_token uuid NOT NULL,
        candidate_id uuid NOT NULL,
        owned_source_event_count integer NOT NULL,
        PRIMARY KEY(scope_change_tx_token,candidate_id)
      ) ON COMMIT DROP;

      INSERT INTO pg_temp._bpay_wb_execution_owned_event_counter_v2 AS owned_event(
        scope_change_tx_token,candidate_id,owned_source_event_count
      )
      SELECT v_scope_change_tx_token,candidate_id,1
      FROM (
        SELECT DISTINCT candidate_id
        FROM pg_catalog.unnest(v_candidate_ids) AS candidate_input(candidate_id)
        WHERE candidate_id IS NOT NULL
      ) AS exact_candidate
      ON CONFLICT(scope_change_tx_token,candidate_id) DO UPDATE
      SET owned_source_event_count=owned_event.owned_source_event_count+1;

      IF v_execution_overlay_exact THEN
        SELECT COALESCE(pg_catalog.jsonb_object_agg(
                 context_entry.key,
                 context_entry.value||pg_catalog.jsonb_build_object(
                   'owned_source_event_count',owned_event.owned_source_event_count,
                   'owned_source_event_count_digest',pg_catalog.md5(
                     COALESCE(context_entry.value->>'context_digest','')||'|'||
                     v_scope_change_tx_token::text||'|'||context_entry.key||'|'||
                     owned_event.owned_source_event_count::text||
                     '|EXECUTION_OWNED_SOURCE_EVENT_COUNT_V1'
                   )
                 ) ORDER BY context_entry.key
               ),'{}'::jsonb)
        INTO v_execution_overlay_contexts
        FROM pg_catalog.jsonb_each(v_execution_overlay_contexts) AS context_entry(key,value)
        JOIN pg_temp._bpay_wb_execution_owned_event_counter_v2 AS owned_event
          ON owned_event.scope_change_tx_token=v_scope_change_tx_token
         AND owned_event.candidate_id=context_entry.key::uuid;
      END IF;

      IF v_execution_schedule_exact THEN
        SELECT COALESCE(pg_catalog.jsonb_object_agg(
                 context_entry.key,
                 context_entry.value||pg_catalog.jsonb_build_object(
                   'owned_source_event_count',owned_event.owned_source_event_count,
                   'owned_source_event_count_digest',pg_catalog.md5(
                     COALESCE(context_entry.value->>'context_digest','')||'|'||
                     v_scope_change_tx_token::text||'|'||context_entry.key||'|'||
                     owned_event.owned_source_event_count::text||
                     '|EXECUTION_OWNED_SOURCE_EVENT_COUNT_V1'
                   )
                 ) ORDER BY context_entry.key
               ),'{}'::jsonb)
        INTO v_execution_schedule_contexts
        FROM pg_catalog.jsonb_each(v_execution_schedule_contexts) AS context_entry(key,value)
        JOIN pg_temp._bpay_wb_execution_owned_event_counter_v2 AS owned_event
          ON owned_event.scope_change_tx_token=v_scope_change_tx_token
         AND owned_event.candidate_id=context_entry.key::uuid;
      END IF;
    END IF;

    PERFORM private.pay_workbench_scope_invalidate_v1(
      v_candidate_ids,v_timesheet_ids,v_reason,v_scope_change_tx_token,
      jsonb_strip_nulls(jsonb_build_object(
        'source_relation',TG_TABLE_NAME,'source_operation',TG_OP,
        'draft_operation_id',v_draft_operation_id,
        'draft_phase',v_draft_phase,
        'draft_context_token',v_draft_context_token,
        'correction_dirty_contexts',CASE
          WHEN v_correction_dirty_contexts<>'{}'::jsonb
            THEN v_correction_dirty_contexts ELSE NULL::jsonb END,
        'request_owned_scope_change_tx_token',CASE
          WHEN v_correction_dirty_contexts<>'{}'::jsonb
            THEN v_scope_change_tx_token ELSE NULL::uuid END,
        'correction_dirty_causal_contract_version',CASE
          WHEN v_correction_dirty_contexts<>'{}'::jsonb
            THEN 'CORRECTION_OWNED_DIRTY_CAUSAL_V1' ELSE NULL::text END,
        'execution_overlay_contexts',CASE
          WHEN v_execution_overlay_exact THEN v_execution_overlay_contexts ELSE NULL::jsonb END,
        'execution_overlay_scope_change_tx_token',CASE
          WHEN v_execution_overlay_exact THEN v_scope_change_tx_token ELSE NULL::uuid END,
        'execution_overlay_causal_contract_version',CASE
          WHEN v_execution_overlay_exact THEN 'EXECUTION_UNSENT_OVERLAY_CAUSAL_V1'
          ELSE NULL::text END,
        'execution_overlay_schedule_contexts',CASE
          WHEN v_execution_schedule_exact THEN v_execution_schedule_contexts ELSE NULL::jsonb END,
        'execution_overlay_schedule_scope_change_tx_token',CASE
          WHEN v_execution_schedule_exact THEN v_scope_change_tx_token ELSE NULL::uuid END,
        'execution_overlay_schedule_causal_contract_version',CASE
          WHEN v_execution_schedule_exact THEN 'EXECUTION_UNSENT_SCHEDULE_CAUSAL_V2'
          ELSE NULL::text END
      ))
    );
  END IF;
  RETURN NULL;
END;
$function$;

ALTER FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1() TO postgres;
