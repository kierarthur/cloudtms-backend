-- Private Blocked presentation only. Preserve the existing renderer's special
-- outstanding-amount branch; the Ready scalar is not that Blocked amount.
-- Original full payloads/actions remain owned by the unchanged detail readers.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_blocked_presentation_v2(p_row jsonb,p_task jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
 n jsonb;summary jsonb;headroom jsonb;component jsonb;components jsonb;ss jsonb;
 line_type text;codes text[];state text;until_date text;reason text;message_id text;condition_text text;
 amount numeric;nominal numeric;outstanding numeric;original numeric;component_outstanding numeric;
 outstanding_total numeric:=0;component_count integer:=0;recoverable numeric;row_amount numeric;section_amount numeric;
 has_amount boolean;code text;
BEGIN
 IF jsonb_typeof(p_row) IS DISTINCT FROM 'object' OR jsonb_typeof(p_task) IS DISTINCT FROM 'object' THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 IF private.pay_workbench_modal_hidden_v2(p_row) THEN RETURN NULL;END IF;
 n:=CASE WHEN jsonb_typeof(p_row->'row_json')='object' THEN p_row->'row_json'
   WHEN jsonb_typeof(p_row->'rowJson')='object' THEN p_row->'rowJson' ELSE '{}'::jsonb END;
 summary:=CASE WHEN jsonb_typeof(p_row->'case_resolution_summary')='object' THEN p_row->'case_resolution_summary'
   WHEN jsonb_typeof(n->'case_resolution_summary')='object' THEN n->'case_resolution_summary' ELSE '{}'::jsonb END;
 headroom:=CASE WHEN jsonb_typeof(p_row->'selection_recovery_headroom_v1')='object' THEN p_row->'selection_recovery_headroom_v1'
   WHEN jsonb_typeof(n->'selection_recovery_headroom_v1')='object' THEN n->'selection_recovery_headroom_v1' ELSE '{}'::jsonb END;
 line_type:=UPPER(BTRIM(COALESCE(NULLIF(p_row->>'line_type',''),NULLIF(p_row->>'lineType',''),
   NULLIF(n->>'line_type',''),n->>'lineType','')));
 SELECT COALESCE(array_agg(DISTINCT upper(btrim(value))) FILTER(WHERE NULLIF(btrim(value),'') IS NOT NULL),ARRAY[]::text[])
 INTO codes FROM (
  SELECT unnest(private.pay_workbench_modal_bank_blockers_v2(p_row)||private.pay_workbench_modal_bank_blockers_v2(n)
    ||private.pay_workbench_modal_bank_blockers_v2(summary)) AS value
  UNION ALL SELECT unnest(ARRAY[p_row->>'presentation_reason',p_row->>'presentationReason',n->>'presentation_reason',n->>'presentationReason'])
 ) reasons;
 row_amount:=private.pay_workbench_modal_first_display_number_v2(private.pay_workbench_modal_display_fields_v2(p_row,ARRAY[
  'amount_display','amountDisplay','amount_ex_vat','amountExVat','preview_amount_ex_vat','previewAmountExVat',
  'ready_preview_amount_ex_vat','readyPreviewAmountExVat','component_amount_ex_vat','componentAmountExVat']));
 section_amount:=COALESCE(private.pay_workbench_modal_first_display_number_v2(
  private.pay_workbench_modal_display_fields_v2(p_row,ARRAY['section_amount_display','sectionAmountDisplay',
    'section_amount_ex_vat','sectionAmountExVat','section_non_segment_amount_ex_vat','sectionNonSegmentAmountExVat'])),row_amount);
 has_amount:=section_amount IS NOT NULL;
 nominal:=private.pay_workbench_modal_first_display_number_v2(ARRAY[p_row->'nominal_due_amount_ex_vat',p_row->'nominalDueAmountExVat',
  n->'nominal_due_amount_ex_vat',n->'nominalDueAmountExVat',summary->'nominal_due_amount_ex_vat',summary->'nominalDueAmountExVat'],true);
 IF has_amount THEN amount:=private.pay_workbench_modal_line_display_amount_v2(p_row);END IF;
 IF line_type='OVERPAYMENT_RECOVERY' THEN
  -- Same outstanding components, precedence and two-decimal display as
  -- getOverpaymentRecoveryPresentation. No allocation/write is performed.
  SELECT COALESCE(jsonb_agg(item.value ORDER BY src.ord,item.ord),'[]'::jsonb) INTO components
  FROM unnest(ARRAY[p_row->'case_components',p_row->'caseComponents',n->'case_components',n->'caseComponents'])
   WITH ORDINALITY src(value,ord)
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(src.value)='array' THEN src.value ELSE '[]'::jsonb END)
   WITH ORDINALITY item(value,ord) WHERE jsonb_typeof(item.value)='object';
  FOR component IN SELECT value FROM jsonb_array_elements(components) LOOP
   original:=ABS(ROUND(private.pay_workbench_modal_first_display_number_v2(ARRAY[
    component->'resolved_target_amount_ex_vat',component->'resolvedTargetAmountExVat',component->'target_pay_ex_vat',
    component->'targetPayExVat',component->'source_amount',component->'sourceAmount',component->'source_pay_ex_vat',
    component->'sourcePayExVat',component->'remaining_source_amount',component->'remainingSourceAmount'],true),2));
   component_outstanding:=ABS(ROUND(private.pay_workbench_modal_first_display_number_v2(ARRAY[
    component->'target_outstanding_ex_vat',component->'targetOutstandingExVat',component->'remaining_source_amount',
    component->'remainingSourceAmount',to_jsonb(original)],true),2));
   IF original IS NOT NULL OR component_outstanding IS NOT NULL THEN
    component_count:=component_count+1;outstanding_total:=outstanding_total+COALESCE(component_outstanding,0);
   END IF;
  END LOOP;
  recoverable:=ABS(ROUND(COALESCE(private.pay_workbench_modal_first_display_number_v2(ARRAY[
   p_row->'recoverable_this_pay_run_ex_vat',p_row->'recoverableThisPayRunExVat',n->'recoverable_this_pay_run_ex_vat',
   n->'recoverableThisPayRunExVat',headroom->'recoverable_amount_ex_vat',headroom->'recoverableAmountExVat',
   to_jsonb(row_amount),to_jsonb(section_amount),'0'::jsonb],true),0),2));
  outstanding:=CASE WHEN component_count>0 THEN outstanding_total ELSE ABS(ROUND(COALESCE(
   private.pay_workbench_modal_first_display_number_v2(ARRAY[p_row->'nominal_due_amount_ex_vat',p_row->'nominalDueAmountExVat',
    n->'nominal_due_amount_ex_vat',n->'nominalDueAmountExVat',p_row->'case_outstanding_amount',p_row->'caseOutstandingAmount',
    n->'case_outstanding_amount',n->'caseOutstandingAmount',p_row->'outstanding_amount',p_row->'outstandingAmount',
    n->'outstanding_amount',n->'outstandingAmount',summary->'nominal_due_amount_ex_vat',summary->'nominalDueAmountExVat',
    to_jsonb(recoverable)],true),0),2)) END;
  IF outstanding>0 AND recoverable=0 AND 'NO_PAY_HEADROOM'=ANY(codes) THEN amount:=outstanding;END IF;
 END IF;
 ss:=CASE WHEN jsonb_typeof(p_row->'snooze_state')='object' THEN p_row->'snooze_state' ELSE '{}'::jsonb END;
 state:=UPPER(BTRIM(COALESCE(NULLIF(ss->>'state',''),CASE WHEN jsonb_typeof(p_row->'snooze_state')='string'
   THEN NULLIF(p_row->>'snooze_state','') END,p_row->>'blocked_snooze_state','')));
 until_date:=BTRIM(COALESCE(NULLIF(ss->>'snooze_until_date',''),p_row->>'snooze_until_date',''));
 IF state NOT IN ('','NONE','NOT_SNOOZED','CLEARED') AND until_date<>'' THEN
  IF until_date !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR to_char(until_date::date,'YYYY-MM-DD')<>until_date THEN
   RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
  reason:='Snoozed until '||to_char(until_date::date,'DD/MM/YYYY')||'.';condition_text:=reason;
 ELSIF p_task->>'task_family'='SOURCE_PROGRESS' THEN
  IF p_task->>'state' IS DISTINCT FROM 'BLOCKED' OR p_task->>'title' IS NULL
   OR p_task->>'title' NOT IN ('Refresh failed','Payment preview needs refreshing.') THEN
   RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
  reason:=p_task->>'title';condition_text:='Payment preview needs refreshing.';amount:=NULL;
 ELSIF p_task->>'task_family'='BANK_ACCOUNT' THEN
  IF p_task->>'state' IS DISTINCT FROM 'BLOCKED' THEN
   RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
  message_id:=p_task->>'title_message_id';reason:=private.pay_workbench_modal_task_title_v2(message_id);
  IF reason IS NULL THEN
   code:=p_task->>'code';
   IF code IN ('BANK_TARGET_CHANGED','BANK_RESULT_CHANGED') THEN
    message_id:='MSG-096';reason:='The bank details changed or could not be confirmed. Refresh Banking Pay and try again.';
   ELSE message_id:='MSG-090';reason:='Payment issue not identified';END IF;
  END IF;
  condition_text:=reason;
 ELSIF 'NO_PAY_HEADROOM'=ANY(codes) THEN
  message_id:='MSG-031';reason:='Insufficient funds to deduct';
  condition_text:='There is not enough pay available in this run to make this deduction.';
 ELSIF line_type='DO_NOT_PAY' THEN
  reason:='This line is currently marked do not pay.';condition_text:=reason;
 ELSIF line_type='BLOCKED_TIMESHEET' THEN
  message_id:='MSG-093';
  reason:='This Timesheet payment cannot be included because CloudTMS could not confirm its current status. Open the Timesheet or refresh Banking Pay.';
  condition_text:=reason;
 ELSE message_id:='MSG-090';reason:='Payment issue not identified';END IF;
 IF message_id='MSG-090' THEN
  condition_text:='CloudTMS could not identify why this payment is blocked. It will not be included. Refresh Banking Pay; if it remains blocked, contact support.';
 END IF;
 RETURN jsonb_build_object('reason_message_id',message_id,'reason',reason,'clear_condition',condition_text,
  'clear_condition_message_id',CASE message_id WHEN 'MSG-031' THEN 'MSG-032' WHEN 'MSG-090' THEN 'MSG-091' ELSE message_id END,
  'affected_display_amount',CASE WHEN amount IS NOT NULL THEN (ROUND(amount,2)::numeric(20,2))::text END,
  'nominal_due_amount',CASE WHEN nominal IS NOT NULL THEN (ROUND(nominal,2)::numeric(20,2))::text END,
  'diagnostic_codes',to_jsonb(codes));
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_blocked_presentation_v2(jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_blocked_presentation_v2(jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role;

commit;
