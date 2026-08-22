-- Immutable CloudTMS TEST function snapshot, page 02.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- _pay_finance_case_freeze_payout_instruction_to_batch_item(uuid,uuid)
CREATE OR REPLACE FUNCTION public._pay_finance_case_freeze_payout_instruction_to_batch_item(p_pay_batch_item_id uuid, p_finance_case_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_item public.pay_batch_items%rowtype;
  v_case public.pay_advances%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_oneoff public.pay_finance_case_oneoff_payout_bank_details%rowtype;
  v_component_classification public.pay_finance_component_classification_enum := null;
  v_taxability public.pay_finance_taxability_enum := null;
  v_routing_kind public.pay_finance_routing_kind_enum := null;
  v_pay_method text := null;
  v_pay_channel text := null;
  v_destination_label text := null;
  v_payee_entity_kind text := null;
  v_payee_entity_id uuid := null;
  v_beneficiary_name text := null;
  v_masked_bank_account text := null;
  v_bank_details_hash text := null;
  v_bank_details_note text := null;
  v_bank_details_created_by_user_id uuid := null;
  v_bank_details_updated_by_user_id uuid := null;
  v_is_candidate_directed_oneoff_payout boolean := false;
  v_appears_on_umbrella_remittance boolean := false;
  v_generates_candidate_payment_advice boolean := false;
  v_snapshot jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_item_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'PAY_BATCH_ITEM_ID_REQUIRED',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: pay_batch_item_id is required'
    )::text;
  END IF;

  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: finance_case_id is required'
    )::text;
  END IF;

  SELECT pbi.*
  INTO v_item
  FROM public.pay_batch_items AS pbi
  WHERE pbi.id = p_pay_batch_item_id
  LIMIT 1
  FOR UPDATE;

  IF v_item.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'PAY_BATCH_ITEM_NOT_FOUND',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: pay batch item not found',
      'pay_batch_item_id', p_pay_batch_item_id::text
    )::text;
  END IF;

  IF v_item.finance_case_id IS NOT NULL AND v_item.finance_case_id <> p_finance_case_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'FINANCE_CASE_ID_MISMATCH',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: item finance_case_id does not match input finance_case_id',
      'pay_batch_item_id', p_pay_batch_item_id::text,
      'item_finance_case_id', v_item.finance_case_id::text,
      'input_finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  LIMIT 1;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT c.*
  INTO v_candidate
  FROM public.candidates AS c
  WHERE c.id = v_case.candidate_id
  LIMIT 1;

  IF v_candidate.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: candidate not found for finance case',
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_case.candidate_id::text
    )::text;
  END IF;

  v_pay_method := upper(coalesce(v_candidate.pay_method, ''));
  IF v_pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
      'code', 'PAY_METHOD_INVALID',
      'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: candidate pay_method must be PAYE or UMBRELLA',
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_case.candidate_id::text,
      'pay_method', v_pay_method
    )::text;
  END IF;

  IF v_candidate.umbrella_id IS NOT NULL THEN
    SELECT u.*
    INTO v_umbrella
    FROM public.umbrellas AS u
    WHERE u.id = v_candidate.umbrella_id
    LIMIT 1;
  END IF;

  SELECT pfc.classification
  INTO v_component_classification
  FROM public.pay_finance_case_components AS pfc
  WHERE pfc.finance_case_id = p_finance_case_id
    AND pfc.component_key_type = 'CASE_TOTAL'
    AND pfc.component_key_value = 'TOTAL'
    AND pfc.closed_at_utc IS NULL
  ORDER BY pfc.updated_at_utc DESC, pfc.created_at_utc DESC, pfc.id DESC
  LIMIT 1;

  v_taxability := v_case.taxability;
  IF v_taxability IS NULL THEN
    IF v_case.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum THEN
      v_taxability := 'NON_TAXABLE'::public.pay_finance_taxability_enum;
    ELSIF v_component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum THEN
      v_taxability := 'TAXABLE'::public.pay_finance_taxability_enum;
    ELSIF v_component_classification IN (
      'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum,
      'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
    ) THEN
      v_taxability := 'NON_TAXABLE'::public.pay_finance_taxability_enum;
    END IF;
  END IF;

  v_routing_kind := v_case.routing_kind;
  IF v_routing_kind IS NULL THEN
    IF v_case.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSE
        v_routing_kind := 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum;
      END IF;
    ELSIF v_case.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSIF v_taxability = 'TAXABLE'::public.pay_finance_taxability_enum THEN
        v_routing_kind := 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum;
      ELSE
        v_routing_kind := 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum;
      END IF;
    ELSIF v_case.case_type = 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum THEN
      IF v_pay_method = 'PAYE' THEN
        v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
      ELSE
        v_routing_kind := 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum;
      END IF;
    ELSE
      v_routing_kind := 'NORMAL_PAY_ROUTE'::public.pay_finance_routing_kind_enum;
    END IF;
  END IF;

  v_pay_channel := v_pay_method;

  IF v_routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::public.pay_finance_routing_kind_enum THEN
    SELECT d.*
    INTO v_oneoff
    FROM public.pay_finance_case_oneoff_payout_bank_details AS d
    WHERE d.finance_case_id = p_finance_case_id
    LIMIT 1;

    IF v_oneoff.finance_case_id IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_CASE_FREEZE_PAYOUT_INSTRUCTION_TO_BATCH_ITEM',
        'code', 'ONEOFF_BANK_DETAILS_MISSING',
        'message', '_pay_finance_case_freeze_payout_instruction_to_batch_item: one-off bank details are required for this finance case routing',
        'finance_case_id', p_finance_case_id::text,
        'routing_kind', v_routing_kind::text
      )::text;
    END IF;

    v_destination_label := 'one-off specified bank account';
    v_payee_entity_kind := 'CANDIDATE';
    v_payee_entity_id := v_candidate.id;
    v_beneficiary_name := v_oneoff.beneficiary_name;
    v_bank_details_hash := v_oneoff.bank_details_hash;
    v_bank_details_note := v_oneoff.note;
    v_bank_details_created_by_user_id := v_oneoff.created_by_user_id;
    v_bank_details_updated_by_user_id := v_oneoff.updated_by_user_id;
    IF nullif(coalesce(v_oneoff.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_oneoff.account_number, 4), length(v_oneoff.account_number), '*');
    END IF;
    v_is_candidate_directed_oneoff_payout := true;
    v_appears_on_umbrella_remittance := false;
    v_generates_candidate_payment_advice := true;
  ELSIF v_routing_kind = 'UMBRELLA_COMPANY'::public.pay_finance_routing_kind_enum THEN
    v_destination_label := 'umbrella company';
    v_payee_entity_kind := 'UMBRELLA';
    v_payee_entity_id := v_candidate.umbrella_id;
    v_beneficiary_name := nullif(btrim(coalesce(v_umbrella.name, '')), '');
    v_bank_details_hash := v_umbrella.bank_details_hash;
    IF nullif(coalesce(v_umbrella.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_umbrella.account_number, 4), length(v_umbrella.account_number), '*');
    END IF;
    v_is_candidate_directed_oneoff_payout := false;
    v_appears_on_umbrella_remittance := true;
    v_generates_candidate_payment_advice := false;
  ELSE
    v_destination_label := 'normal PAYE route';
    v_payee_entity_kind := 'CANDIDATE';
    v_payee_entity_id := v_candidate.id;
    v_beneficiary_name := nullif(btrim(coalesce(v_candidate.account_holder, v_candidate.display_name, concat_ws(' ', v_candidate.first_name, v_candidate.last_name))), '');
    v_bank_details_hash := v_candidate.bank_details_hash;
    IF nullif(coalesce(v_candidate.account_number, ''), '') IS NOT NULL THEN
      v_masked_bank_account := lpad(right(v_candidate.account_number, 4), length(v_candidate.account_number), '*');
    END IF;
    v_is_candidate_directed_oneoff_payout := false;
    v_appears_on_umbrella_remittance := false;
    v_generates_candidate_payment_advice := false;
  END IF;

  v_snapshot := jsonb_strip_nulls(
    jsonb_build_object(
      'taxability', case when v_taxability is null then null else v_taxability::text end,
      'routing_kind', case when v_routing_kind is null then null else v_routing_kind::text end,
      'destination_label', v_destination_label,
      'pay_channel', v_pay_channel,
      'payee_entity_kind', v_payee_entity_kind,
      'payee_entity_id', case when v_payee_entity_id is null then null else v_payee_entity_id::text end,
      'beneficiary_name', v_beneficiary_name,
      'masked_bank_account', v_masked_bank_account,
      'bank_details_hash', v_bank_details_hash,
      'bank_details_note', v_bank_details_note,
      'bank_details_created_by_user_id', case when v_bank_details_created_by_user_id is null then null else v_bank_details_created_by_user_id::text end,
      'bank_details_updated_by_user_id', case when v_bank_details_updated_by_user_id is null then null else v_bank_details_updated_by_user_id::text end,
      'appears_on_umbrella_remittance', v_appears_on_umbrella_remittance,
      'generates_candidate_payment_advice', v_generates_candidate_payment_advice,
      'is_candidate_directed_oneoff_payout', v_is_candidate_directed_oneoff_payout,
      'locked_at_draft', true,
      'finance_case_id', p_finance_case_id::text,
      'pay_batch_item_id', p_pay_batch_item_id::text
    )
  );

  UPDATE public.pay_batch_items AS pbi
  SET payout_instruction_snapshot_json = v_snapshot
  WHERE pbi.id = p_pay_batch_item_id;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_item_id', p_pay_batch_item_id::text,
    'finance_case_id', p_finance_case_id::text,
    'payout_instruction_snapshot_json', v_snapshot
  );
END;
$function$;

-- _pay_finance_case_oneoff_bank_remove(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public._pay_finance_case_oneoff_bank_remove(p_finance_case_id uuid, p_actor_user_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_case public.pay_advances%rowtype;
  v_existing public.pay_finance_case_oneoff_payout_bank_details%rowtype;

  v_has_noncancelled_batch_item boolean := false;
  v_before_json jsonb := null;
  v_after_json jsonb := null;
  v_preview_blocked_until_reentered boolean := false;
BEGIN
  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_REMOVE',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_finance_case_oneoff_bank_remove: finance_case_id is required'
    )::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_REMOVE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', '_pay_finance_case_oneoff_bank_remove: actor_user_id is required'
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  FOR UPDATE;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_REMOVE',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_finance_case_oneoff_bank_remove: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  IF v_case.case_type NOT IN (
    'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
    'MANUAL_CREDIT_ADJUSTMENT'::public.pay_finance_case_type_enum
  ) THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_REMOVE',
      'code', 'CASE_TYPE_NOT_ELIGIBLE',
      'message', '_pay_finance_case_oneoff_bank_remove: one-off bank details can only be removed for payment advances or manual credit adjustments',
      'finance_case_id', p_finance_case_id::text,
      'case_type', v_case.case_type::text
    )::text;
  END IF;

  SELECT exists (
    SELECT 1
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    WHERE (
        pbi.finance_case_id = p_finance_case_id
        OR pbi.source_ref = ('advance:'::text || p_finance_case_id::text)
      )
      AND coalesce(pbi.is_voided, false) = false
      AND pb.cancelled_at_utc IS NULL
      AND upper(coalesce(pb.status, '')) <> 'CANCELLED'
  )
  INTO v_has_noncancelled_batch_item;

  IF v_case.payout_pay_batch_id IS NOT NULL OR v_has_noncancelled_batch_item THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_REMOVE',
      'code', 'ONEOFF_BANK_DETAILS_LOCKED',
      'message', '_pay_finance_case_oneoff_bank_remove: one-off bank details can only be removed while the item is Not processed yet',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT d.*
  INTO v_existing
  FROM public.pay_finance_case_oneoff_payout_bank_details AS d
  WHERE d.finance_case_id = p_finance_case_id
  FOR UPDATE;

  v_preview_blocked_until_reentered := coalesce(v_case.oneoff_bank_details_required, false);

  IF v_existing.finance_case_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'finance_case_id', p_finance_case_id::text,
      'removed', false,
      'preview_blocked_until_reentered', v_preview_blocked_until_reentered
    );
  END IF;

  v_before_json := jsonb_build_object(
    'finance_case_id', p_finance_case_id::text,
    'candidate_id', v_case.candidate_id::text,
    'beneficiary_name', v_existing.beneficiary_name,
    'sort_code_masked', CASE WHEN v_existing.sort_code IS NULL THEN NULL ELSE 'XX-XX-' || right(v_existing.sort_code, 2) END,
    'account_number_masked', CASE WHEN v_existing.account_number IS NULL THEN NULL ELSE lpad(right(v_existing.account_number, 4), length(v_existing.account_number), '*') END,
    'bank_details_hash', v_existing.bank_details_hash,
    'note', v_existing.note
  );

  DELETE FROM public.pay_finance_case_oneoff_payout_bank_details AS d
  WHERE d.finance_case_id = p_finance_case_id;

  v_after_json := jsonb_build_object(
    'finance_case_id', p_finance_case_id::text,
    'candidate_id', v_case.candidate_id::text,
    'oneoff_bank_details_present', false,
    'preview_blocked_until_reentered', v_preview_blocked_until_reentered
  );

  PERFORM public._audit_insert(
    'finance_case',
    p_finance_case_id::text,
    'ONEOFF_BANK_DETAILS_REMOVED',
    v_before_json,
    v_after_json,
    coalesce(nullif(btrim(coalesce(p_reason, '')), ''), 'ONE_OFF_PAYOUT_BANK_DETAILS'),
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'finance_case_id', p_finance_case_id::text,
    'removed', true,
    'preview_blocked_until_reentered', v_preview_blocked_until_reentered
  );
END;
$function$;

-- _pay_finance_case_oneoff_bank_upsert(uuid,uuid,text,text,text,text,text)
CREATE OR REPLACE FUNCTION public._pay_finance_case_oneoff_bank_upsert(p_finance_case_id uuid, p_actor_user_id uuid, p_beneficiary_name text, p_sort_code text, p_account_number text, p_note text DEFAULT NULL::text, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_case public.pay_advances%rowtype;
  v_candidate public.candidates%rowtype;
  v_existing public.pay_finance_case_oneoff_payout_bank_details%rowtype;

  v_effective jsonb := '{}'::jsonb;

  v_beneficiary_name_norm text := null;
  v_sort_digits text := null;
  v_sort_code_norm text := null;
  v_account_number_norm text := null;
  v_bank_details_hash text := null;
  v_note_norm text := null;

  v_before_json jsonb := null;
  v_after_json jsonb := null;
  v_action text := null;

  v_has_noncancelled_batch_item boolean := false;
BEGIN
  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_finance_case_oneoff_bank_upsert: finance_case_id is required'
    )::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', '_pay_finance_case_oneoff_bank_upsert: actor_user_id is required'
    )::text;
  END IF;

  v_beneficiary_name_norm := nullif(regexp_replace(btrim(coalesce(p_beneficiary_name, '')), '\s+', ' ', 'g'), '');
  v_sort_digits := nullif(regexp_replace(coalesce(p_sort_code, ''), '[^0-9]+', '', 'g'), '');
  v_account_number_norm := nullif(regexp_replace(coalesce(p_account_number, ''), '[^0-9]+', '', 'g'), '');
  v_note_norm := nullif(btrim(coalesce(p_note, '')), '');

  IF v_beneficiary_name_norm IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'BENEFICIARY_NAME_REQUIRED',
      'message', '_pay_finance_case_oneoff_bank_upsert: beneficiary_name is required'
    )::text;
  END IF;

  IF v_sort_digits IS NULL OR length(v_sort_digits) <> 6 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'SORT_CODE_INVALID',
      'message', '_pay_finance_case_oneoff_bank_upsert: sort_code must contain exactly 6 digits'
    )::text;
  END IF;

  IF v_account_number_norm IS NULL OR length(v_account_number_norm) < 6 OR length(v_account_number_norm) > 10 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'ACCOUNT_NUMBER_INVALID',
      'message', '_pay_finance_case_oneoff_bank_upsert: account_number must contain between 6 and 10 digits'
    )::text;
  END IF;

  v_sort_code_norm := substr(v_sort_digits, 1, 2) || '-' || substr(v_sort_digits, 3, 2) || '-' || substr(v_sort_digits, 5, 2);
  v_bank_details_hash := public._bank_hash(v_sort_code_norm, v_account_number_norm, v_beneficiary_name_norm);

  IF nullif(btrim(coalesce(v_bank_details_hash, '')), '') IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'BANK_HASH_FAILED',
      'message', '_pay_finance_case_oneoff_bank_upsert: unable to compute bank_details_hash'
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  FOR UPDATE;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_finance_case_oneoff_bank_upsert: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT c.*
  INTO v_candidate
  FROM public.candidates AS c
  WHERE c.id = v_case.candidate_id
  LIMIT 1;

  IF v_candidate.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', '_pay_finance_case_oneoff_bank_upsert: candidate not found for finance case',
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_case.candidate_id::text
    )::text;
  END IF;

  v_effective := public._pay_finance_case_effective_payout_instruction(p_finance_case_id);

  IF upper(coalesce(v_candidate.pay_method, '')) <> 'UMBRELLA' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'PAY_METHOD_NOT_UMBRELLA',
      'message', '_pay_finance_case_oneoff_bank_upsert: one-off payout bank details are only valid for umbrella-routed one-off payouts',
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_candidate.id::text,
      'pay_method', v_candidate.pay_method
    )::text;
  END IF;

  IF coalesce(v_effective ->> 'routing_kind', '') <> 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'ROUTING_KIND_NOT_ONEOFF',
      'message', '_pay_finance_case_oneoff_bank_upsert: finance case is not currently routed to a one-off specified bank account',
      'finance_case_id', p_finance_case_id::text,
      'routing_kind', v_effective ->> 'routing_kind'
    )::text;
  END IF;

  SELECT exists (
    SELECT 1
    FROM public.pay_batch_items AS pbi
    JOIN public.pay_batch_candidates AS pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches AS pb
      ON pb.id = pbc.pay_batch_id
    WHERE (
        pbi.finance_case_id = p_finance_case_id
        OR pbi.source_ref = ('advance:'::text || p_finance_case_id::text)
      )
      AND coalesce(pbi.is_voided, false) = false
      AND pb.cancelled_at_utc IS NULL
      AND upper(coalesce(pb.status, '')) <> 'CANCELLED'
  )
  INTO v_has_noncancelled_batch_item;

  IF v_case.payout_pay_batch_id IS NOT NULL OR v_has_noncancelled_batch_item THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_CASE_ONEOFF_BANK_UPSERT',
      'code', 'ONEOFF_BANK_DETAILS_LOCKED',
      'message', '_pay_finance_case_oneoff_bank_upsert: one-off bank details can only be edited while the item is Not processed yet',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  SELECT d.*
  INTO v_existing
  FROM public.pay_finance_case_oneoff_payout_bank_details AS d
  WHERE d.finance_case_id = p_finance_case_id
  FOR UPDATE;

  IF v_existing.finance_case_id IS NOT NULL THEN
    v_before_json := jsonb_build_object(
      'finance_case_id', p_finance_case_id::text,
      'candidate_id', v_case.candidate_id::text,
      'beneficiary_name', v_existing.beneficiary_name,
      'sort_code_masked', CASE WHEN v_existing.sort_code IS NULL THEN NULL ELSE 'XX-XX-' || right(v_existing.sort_code, 2) END,
      'account_number_masked', CASE WHEN v_existing.account_number IS NULL THEN NULL ELSE lpad(right(v_existing.account_number, 4), length(v_existing.account_number), '*') END,
      'bank_details_hash', v_existing.bank_details_hash,
      'note', v_existing.note
    );
  END IF;

  INSERT INTO public.pay_finance_case_oneoff_payout_bank_details(
    finance_case_id,
    candidate_id,
    beneficiary_name,
    sort_code,
    account_number,
    bank_details_hash,
    note,
    created_at_utc,
    created_by_user_id,
    updated_at_utc,
    updated_by_user_id
  )
  VALUES (
    p_finance_case_id,
    v_case.candidate_id,
    v_beneficiary_name_norm,
    v_sort_code_norm,
    v_account_number_norm,
    v_bank_details_hash,
    v_note_norm,
    now(),
    p_actor_user_id,
    now(),
    p_actor_user_id
  )
  ON CONFLICT (finance_case_id) DO UPDATE
  SET candidate_id = EXCLUDED.candidate_id,
      beneficiary_name = EXCLUDED.beneficiary_name,
      sort_code = EXCLUDED.sort_code,
      account_number = EXCLUDED.account_number,
      bank_details_hash = EXCLUDED.bank_details_hash,
      note = EXCLUDED.note,
      updated_at_utc = EXCLUDED.updated_at_utc,
      updated_by_user_id = EXCLUDED.updated_by_user_id;

  SELECT d.*
  INTO v_existing
  FROM public.pay_finance_case_oneoff_payout_bank_details AS d
  WHERE d.finance_case_id = p_finance_case_id
  LIMIT 1;

  v_after_json := jsonb_build_object(
    'finance_case_id', p_finance_case_id::text,
    'candidate_id', v_case.candidate_id::text,
    'beneficiary_name', v_existing.beneficiary_name,
    'sort_code_masked', CASE WHEN v_existing.sort_code IS NULL THEN NULL ELSE 'XX-XX-' || right(v_existing.sort_code, 2) END,
    'account_number_masked', CASE WHEN v_existing.account_number IS NULL THEN NULL ELSE lpad(right(v_existing.account_number, 4), length(v_existing.account_number), '*') END,
    'bank_details_hash', v_existing.bank_details_hash,
    'note', v_existing.note
  );

  IF v_before_json IS NULL THEN
    v_action := 'ONEOFF_BANK_DETAILS_CREATED';
  ELSIF v_before_json IS DISTINCT FROM v_after_json THEN
    v_action := 'ONEOFF_BANK_DETAILS_UPDATED';
  ELSE
    v_action := 'ONEOFF_BANK_DETAILS_UNCHANGED';
  END IF;

  IF v_action <> 'ONEOFF_BANK_DETAILS_UNCHANGED' THEN
    PERFORM public._audit_insert(
      'finance_case',
      p_finance_case_id::text,
      v_action,
      v_before_json,
      v_after_json,
      coalesce(nullif(btrim(coalesce(p_reason, '')), ''), 'ONE_OFF_PAYOUT_BANK_DETAILS'),
      p_actor_user_id
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'finance_case_id', p_finance_case_id::text,
    'candidate_id', v_case.candidate_id::text,
    'action', v_action,
    'beneficiary_name', v_existing.beneficiary_name,
    'sort_code_masked', CASE WHEN v_existing.sort_code IS NULL THEN NULL ELSE 'XX-XX-' || right(v_existing.sort_code, 2) END,
    'account_number_masked', CASE WHEN v_existing.account_number IS NULL THEN NULL ELSE lpad(right(v_existing.account_number, 4), length(v_existing.account_number), '*') END,
    'bank_details_hash', v_existing.bank_details_hash,
    'note', v_existing.note,
    'preview_blocked_until_reentered', false
  );
END;
$function$;

-- _pay_finance_protected_recovery_allocate(jsonb,numeric,numeric,numeric)
CREATE OR REPLACE FUNCTION public._pay_finance_protected_recovery_allocate(p_recovery_rows jsonb, p_run_earnings_headroom numeric, p_run_take_home_headroom numeric DEFAULT NULL::numeric, p_default_take_home_floor numeric DEFAULT NULL::numeric)
 RETURNS TABLE(sort_order integer, finance_case_id uuid, case_type pay_finance_case_type_enum, payout_status pay_advance_payout_status_enum, nominal_due_amount numeric, minimum_earnings_threshold numeric, effective_take_home_floor numeric, headroom_before numeric, take_home_before numeric, threshold_cap_amount numeric, take_home_cap_amount numeric, protected_recoverable_amount numeric, headroom_after numeric, take_home_after numeric)
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
DECLARE
  v_row record;
  v_remaining_headroom numeric(12,2) := round(greatest(coalesce(p_run_earnings_headroom, 0), 0), 2)::numeric(12,2);
  v_remaining_take_home numeric(12,2) := CASE
    WHEN p_run_take_home_headroom IS NULL THEN NULL
    ELSE round(greatest(p_run_take_home_headroom, 0), 2)::numeric(12,2)
  END;
  v_default_take_home_floor numeric(12,2) := CASE
    WHEN p_default_take_home_floor IS NULL THEN NULL
    ELSE round(greatest(p_default_take_home_floor, 0), 2)::numeric(12,2)
  END;
  v_case_type_text text := NULL;
  v_payout_status_text text := NULL;
  v_nominal_due_amount numeric(12,2) := 0;
  v_minimum_earnings_threshold numeric(12,2) := NULL;
  v_effective_take_home_floor_local numeric(12,2) := NULL;
  v_headroom_before_local numeric(12,2) := 0;
  v_take_home_before_local numeric(12,2) := NULL;
  v_threshold_cap_amount_local numeric(12,2) := 0;
  v_take_home_cap_amount_local numeric(12,2) := NULL;
  v_effective_cap_amount_local numeric(12,2) := 0;
  v_protected_recoverable_amount_local numeric(12,2) := 0;
BEGIN
  IF p_recovery_rows IS NULL OR jsonb_typeof(p_recovery_rows) <> 'array' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
      'code', 'RECOVERY_ROWS_ARRAY_REQUIRED',
      'message', '_pay_finance_protected_recovery_allocate: p_recovery_rows must be a JSON array'
    )::text;
  END IF;

  FOR v_row IN
    WITH parsed_rows AS (
      SELECT
        CASE
          WHEN nullif(btrim(elem.value->>'sort_order'), '') IS NULL THEN elem.ordinality::integer
          ELSE (elem.value->>'sort_order')::integer
        END AS sort_order,
        elem.ordinality::integer AS input_ordinality,
        CASE
          WHEN nullif(btrim(elem.value->>'finance_case_id'), '') IS NULL THEN NULL::uuid
          ELSE (elem.value->>'finance_case_id')::uuid
        END AS finance_case_id,
        nullif(btrim(elem.value->>'case_type'), '') AS case_type_text,
        nullif(btrim(elem.value->>'payout_status'), '') AS payout_status_text,
        round(
          greatest(
            coalesce(
              CASE
                WHEN nullif(btrim(elem.value->>'nominal_due_amount'), '') IS NULL THEN 0::numeric
                ELSE (elem.value->>'nominal_due_amount')::numeric
              END,
              0::numeric
            ),
            0::numeric
          ),
          2
        )::numeric(12,2) AS nominal_due_amount,
        CASE
          WHEN nullif(btrim(elem.value->>'minimum_earnings_threshold'), '') IS NULL THEN NULL::numeric(12,2)
          ELSE round(greatest((elem.value->>'minimum_earnings_threshold')::numeric, 0::numeric), 2)::numeric(12,2)
        END AS minimum_earnings_threshold,
        CASE
          WHEN nullif(btrim(elem.value->>'take_home_floor_override'), '') IS NULL THEN NULL::numeric(12,2)
          ELSE round(greatest((elem.value->>'take_home_floor_override')::numeric, 0::numeric), 2)::numeric(12,2)
        END AS take_home_floor_override
      FROM jsonb_array_elements(p_recovery_rows) WITH ORDINALITY AS elem(value, ordinality)
    )
    SELECT
      pr.sort_order,
      pr.input_ordinality,
      pr.finance_case_id,
      pr.case_type_text,
      pr.payout_status_text,
      pr.nominal_due_amount,
      pr.minimum_earnings_threshold,
      pr.take_home_floor_override
    FROM parsed_rows AS pr
    ORDER BY pr.sort_order, pr.input_ordinality, pr.finance_case_id
  LOOP
    IF v_row.finance_case_id IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'FINANCE_CASE_ID_REQUIRED',
        'message', '_pay_finance_protected_recovery_allocate: each recovery row must include finance_case_id',
        'sort_order', v_row.sort_order,
        'input_ordinality', v_row.input_ordinality
      )::text;
    END IF;

    v_case_type_text := upper(coalesce(v_row.case_type_text, ''));

    IF v_case_type_text NOT IN ('PAYMENT_ADVANCE', 'MANUAL_DEBT_ADJUSTMENT') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'UNSUPPORTED_CASE_TYPE',
        'message', '_pay_finance_protected_recovery_allocate: only PAYMENT_ADVANCE repayments and MANUAL_DEBT_ADJUSTMENT recoveries are supported',
        'finance_case_id', v_row.finance_case_id::text,
        'case_type', v_row.case_type_text
      )::text;
    END IF;

    v_payout_status_text := upper(coalesce(v_row.payout_status_text, ''));
    IF v_case_type_text = 'PAYMENT_ADVANCE' AND v_payout_status_text <> 'PAID' THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'PAYMENT_ADVANCE_NOT_REPAYABLE',
        'message', '_pay_finance_protected_recovery_allocate: PAYMENT_ADVANCE rows must represent paid advances when allocating recoveries',
        'finance_case_id', v_row.finance_case_id::text,
        'payout_status', v_row.payout_status_text
      )::text;
    END IF;

    v_nominal_due_amount := round(greatest(coalesce(v_row.nominal_due_amount, 0), 0), 2)::numeric(12,2);
    v_minimum_earnings_threshold := v_row.minimum_earnings_threshold;
    v_effective_take_home_floor_local := coalesce(v_row.take_home_floor_override, v_default_take_home_floor);

    v_headroom_before_local := v_remaining_headroom;
    v_take_home_before_local := v_remaining_take_home;

    v_threshold_cap_amount_local := round(
      CASE
        WHEN v_minimum_earnings_threshold IS NULL THEN v_headroom_before_local
        ELSE greatest(v_headroom_before_local - v_minimum_earnings_threshold, 0)
      END,
      2
    )::numeric(12,2);

    v_take_home_cap_amount_local := CASE
      WHEN v_take_home_before_local IS NULL OR v_effective_take_home_floor_local IS NULL THEN NULL
      ELSE round(greatest(v_take_home_before_local - v_effective_take_home_floor_local, 0), 2)::numeric(12,2)
    END;

    v_effective_cap_amount_local := round(
      least(
        v_headroom_before_local,
        v_threshold_cap_amount_local,
        coalesce(v_take_home_cap_amount_local, v_headroom_before_local)
      ),
      2
    )::numeric(12,2);

    v_protected_recoverable_amount_local := round(
      least(v_nominal_due_amount, greatest(v_effective_cap_amount_local, 0)),
      2
    )::numeric(12,2);

    v_remaining_headroom := round(
      greatest(v_remaining_headroom - v_protected_recoverable_amount_local, 0),
      2
    )::numeric(12,2);

    IF v_remaining_take_home IS NOT NULL THEN
      v_remaining_take_home := round(
        greatest(v_remaining_take_home - v_protected_recoverable_amount_local, 0),
        2
      )::numeric(12,2);
    END IF;

    sort_order := v_row.sort_order;
    finance_case_id := v_row.finance_case_id;
    case_type := v_case_type_text::public.pay_finance_case_type_enum;
    payout_status := CASE
      WHEN v_case_type_text = 'PAYMENT_ADVANCE' THEN 'PAID'::public.pay_advance_payout_status_enum
      ELSE NULL::public.pay_advance_payout_status_enum
    END;
    nominal_due_amount := v_nominal_due_amount;
    minimum_earnings_threshold := v_minimum_earnings_threshold;
    effective_take_home_floor := v_effective_take_home_floor_local;
    headroom_before := v_headroom_before_local;
    take_home_before := v_take_home_before_local;
    threshold_cap_amount := v_threshold_cap_amount_local;
    take_home_cap_amount := v_take_home_cap_amount_local;
    protected_recoverable_amount := v_protected_recoverable_amount_local;
    headroom_after := v_remaining_headroom;
    take_home_after := v_remaining_take_home;

    RETURN NEXT;
  END LOOP;

  RETURN;
END;
$function$;

-- _pay_manual_adjustment_carry_forward_create(uuid[],jsonb,uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_create(p_source_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[], p_resolved_scope_json jsonb DEFAULT NULL::jsonb, p_source_correction_request_id uuid DEFAULT NULL::uuid, p_source_correction_work_item_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_source_pay_batch_item_ids uuid[] := COALESCE(p_source_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_item_ids uuid[] := ARRAY[]::uuid[];
  v_source_pay_batch_id uuid := NULL::uuid;
  v_detection_result jsonb := '{}'::jsonb;
  v_safe_source_item_ids uuid[] := ARRAY[]::uuid[];
  v_existing_source_item_ids uuid[] := ARRAY[]::uuid[];
  v_created_count integer := 0;
  v_existing_count integer := 0;
  v_skipped_count integer := 0;
  v_total_safe_count integer := 0;
  v_records_json jsonb := '[]'::jsonb;
  v_skipped_json jsonb := '[]'::jsonb;
BEGIN
  IF p_resolved_scope_json IS NOT NULL AND jsonb_typeof(p_resolved_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF p_resolved_scope_json IS NOT NULL AND jsonb_typeof(p_resolved_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(p_resolved_scope_json->'pay_batch_item_ids') = 'array' THEN p_resolved_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  v_source_pay_batch_item_ids := COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[]) || COALESCE(v_scope_item_ids, ARRAY[]::uuid[]);

  SELECT COALESCE(array_agg(DISTINCT source_item_values.source_item_id) FILTER (WHERE source_item_values.source_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_source_pay_batch_item_ids
  FROM unnest(COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[])) AS source_item_values(source_item_id);

  IF COALESCE(array_length(v_source_pay_batch_item_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'created_count', 0,
      'existing_count', 0,
      'skipped_count', 0,
      'carry_forward_records', '[]'::jsonb,
      'skipped_items', '[]'::jsonb,
      'message', 'No source pay_batch_item_ids supplied.'
    );
  END IF;

  SELECT batch_candidate_rows.pay_batch_id
  INTO v_source_pay_batch_id
  FROM public.pay_batch_items AS source_item_rows
  JOIN public.pay_batch_candidates AS batch_candidate_rows
    ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
  WHERE source_item_rows.id = ANY(v_source_pay_batch_item_ids)
  ORDER BY batch_candidate_rows.pay_batch_id
  LIMIT 1;

  IF v_source_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'SOURCE_PAY_BATCH_ITEMS_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SOURCE_PAY_BATCH_ITEMS_NOT_FOUND')::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_batch_items AS source_item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
    WHERE source_item_rows.id = ANY(v_source_pay_batch_item_ids)
      AND batch_candidate_rows.pay_batch_id IS DISTINCT FROM v_source_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'SOURCE_PAY_BATCH_ITEMS_MUST_BELONG_TO_ONE_BATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'SOURCE_PAY_BATCH_ITEMS_MUST_BELONG_TO_ONE_BATCH')::text;
  END IF;

  PERFORM 1
  FROM public.pay_batch_items AS lock_source_item_rows
  WHERE lock_source_item_rows.id = ANY(v_source_pay_batch_item_ids)
  ORDER BY lock_source_item_rows.id
  FOR UPDATE;

  v_detection_result := public._pay_detect_manual_adjustments_for_carry_forward(
    v_source_pay_batch_id,
    jsonb_build_object(
      'pay_batch_id', v_source_pay_batch_id::text,
      'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(item_id_values.source_item_id::text ORDER BY item_id_values.source_item_id::text) FROM unnest(v_source_pay_batch_item_ids) AS item_id_values(source_item_id)), '[]'::jsonb)
    ),
    p_actor_user_id
  );

  WITH safe_items AS (
    SELECT NULLIF(btrim(safe_item_values.safe_item_json->>'pay_batch_item_id'), '')::uuid AS pay_batch_item_id
    FROM jsonb_array_elements(COALESCE(v_detection_result->'manual_adjustments_to_carry_forward', '[]'::jsonb)) AS safe_item_values(safe_item_json)
    WHERE NULLIF(btrim(safe_item_values.safe_item_json->>'pay_batch_item_id'), '') ~ v_uuid_regex
  )
  SELECT COALESCE(array_agg(safe_items.pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_safe_source_item_ids
  FROM safe_items;

  v_total_safe_count := COALESCE(array_length(v_safe_source_item_ids, 1), 0);

  SELECT COALESCE(array_agg(existing_carry_forward_rows.source_pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_existing_source_item_ids
  FROM public.pay_manual_adjustment_carry_forwards AS existing_carry_forward_rows
  WHERE existing_carry_forward_rows.source_pay_batch_item_id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[]));

  WITH candidate_rows AS (
    SELECT
      source_item_rows.id AS source_pay_batch_item_id,
      batch_candidate_rows.pay_batch_id AS source_pay_batch_id,
      source_item_rows.pay_bank_transfer_id AS source_pay_bank_transfer_id,
      source_item_rows.pay_batch_candidate_id AS source_pay_batch_candidate_id,
      batch_candidate_rows.candidate_id AS candidate_id,
      COALESCE(source_item_rows.umbrella_id, transfer_rows.umbrella_id, CASE WHEN upper(COALESCE(source_item_rows.pay_channel, '')) = 'UMBRELLA' THEN candidate_record_rows.umbrella_id ELSE NULL::uuid END) AS umbrella_id,
      CASE
        WHEN NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'client_id', '')), '') ~ v_uuid_regex
             AND client_rows.id IS NOT NULL
          THEN client_rows.id
        ELSE NULL::uuid
      END AS client_id,
      source_item_rows.timesheet_id AS timesheet_id,
      upper(btrim(COALESCE(source_item_rows.pay_channel, ''))) AS pay_channel,
      CASE WHEN source_item_rows.amount_inc_vat > 0 THEN 'CREDIT' ELSE 'DEBIT' END AS adjustment_direction,
      source_item_rows.amount_ex_vat AS amount_ex_vat,
      source_item_rows.amount_vat AS amount_vat,
      source_item_rows.amount_inc_vat AS amount_inc_vat,
      COALESCE(NULLIF(btrim(source_item_rows.frozen_component_key_type), ''), source_item_rows.item_type) AS amount_basis,
      source_item_rows.paye_treatment AS paye_treatment,
      jsonb_build_object(
        'signed_amount_convention', 'SIGNED_AMOUNTS',
        'adjustment_direction_is_display_only', true,
        'pay_channel', source_item_rows.pay_channel,
        'paye_treatment', source_item_rows.paye_treatment,
        'frozen_component_classification', CASE WHEN source_item_rows.frozen_component_classification IS NULL THEN NULL ELSE source_item_rows.frozen_component_classification::text END,
        'frozen_source_pay_method', source_item_rows.frozen_source_pay_method,
        'frozen_target_pay_method', source_item_rows.frozen_target_pay_method,
        'amount_ex_vat', source_item_rows.amount_ex_vat,
        'amount_vat', source_item_rows.amount_vat,
        'amount_inc_vat', source_item_rows.amount_inc_vat
      ) AS tax_treatment_json,
      source_item_rows.description AS description,
      COALESCE(
        NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'reason', '')), ''),
        NULLIF(btrim(COALESCE(source_item_rows.frozen_resolution_payload_json->>'reason', '')), ''),
        NULLIF(btrim(COALESCE(source_item_rows.description, '')), '')
      ) AS reason,
      source_item_rows.source_ref AS source_ref,
      source_item_rows.operation_source_key AS source_operation_source_key,
      jsonb_build_object(
        'source_pay_batch_item', to_jsonb(source_item_rows),
        'source_pay_batch_candidate', to_jsonb(batch_candidate_rows),
        'source_pay_bank_transfer', CASE WHEN transfer_rows.id IS NULL THEN NULL ELSE to_jsonb(transfer_rows) END,
        'signed_amount_convention', 'SIGNED_AMOUNTS',
        'created_from_function', '_pay_manual_adjustment_carry_forward_create'
      ) AS source_snapshot_json
    FROM public.pay_batch_items AS source_item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = source_item_rows.pay_batch_candidate_id
    JOIN public.candidates AS candidate_record_rows
      ON candidate_record_rows.id = batch_candidate_rows.candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = source_item_rows.pay_bank_transfer_id
    LEFT JOIN public.clients AS client_rows
      ON client_rows.id = CASE
        WHEN NULLIF(btrim(COALESCE(source_item_rows.frozen_source_basis_json->>'client_id', '')), '') ~ v_uuid_regex
          THEN (source_item_rows.frozen_source_basis_json->>'client_id')::uuid
        ELSE NULL::uuid
      END
    WHERE source_item_rows.id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[]))
      AND source_item_rows.amount_inc_vat IS NOT NULL
      AND round(source_item_rows.amount_inc_vat, 2) <> 0
      AND NULLIF(btrim(COALESCE(source_item_rows.description, '')), '') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_manual_adjustment_carry_forwards AS target_carry_forward_rows
        WHERE target_carry_forward_rows.target_pay_batch_item_id = source_item_rows.id
      )
      AND upper(COALESCE(source_item_rows.source_ref, '')) NOT LIKE 'CARRY_FORWARD:%'
      AND upper(COALESCE(source_item_rows.operation_source_key, '')) NOT LIKE 'CARRY_FORWARD:%'
  ), upserted_rows AS (
    INSERT INTO public.pay_manual_adjustment_carry_forwards AS carry_forward_target_rows (
      source_pay_batch_id,
      source_pay_batch_item_id,
      source_pay_bank_transfer_id,
      source_pay_batch_candidate_id,
      source_correction_request_id,
      source_correction_work_item_id,
      candidate_id,
      umbrella_id,
      client_id,
      timesheet_id,
      pay_channel,
      adjustment_direction,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      amount_basis,
      paye_treatment,
      tax_treatment_json,
      description,
      reason,
      source_ref,
      source_operation_source_key,
      source_snapshot_json,
      status,
      created_by_user_id
    )
    SELECT
      candidate_rows.source_pay_batch_id,
      candidate_rows.source_pay_batch_item_id,
      candidate_rows.source_pay_bank_transfer_id,
      candidate_rows.source_pay_batch_candidate_id,
      p_source_correction_request_id,
      p_source_correction_work_item_id,
      candidate_rows.candidate_id,
      candidate_rows.umbrella_id,
      candidate_rows.client_id,
      candidate_rows.timesheet_id,
      candidate_rows.pay_channel,
      candidate_rows.adjustment_direction,
      candidate_rows.amount_ex_vat,
      candidate_rows.amount_vat,
      candidate_rows.amount_inc_vat,
      candidate_rows.amount_basis,
      candidate_rows.paye_treatment,
      candidate_rows.tax_treatment_json,
      candidate_rows.description,
      candidate_rows.reason,
      candidate_rows.source_ref,
      candidate_rows.source_operation_source_key,
      candidate_rows.source_snapshot_json,
      'PENDING_CARRY_FORWARD',
      p_actor_user_id
    FROM candidate_rows
    ON CONFLICT (source_pay_batch_item_id)
    DO UPDATE
    SET
      source_correction_request_id = COALESCE(carry_forward_target_rows.source_correction_request_id, EXCLUDED.source_correction_request_id),
      source_correction_work_item_id = COALESCE(carry_forward_target_rows.source_correction_work_item_id, EXCLUDED.source_correction_work_item_id),
      source_snapshot_json = COALESCE(carry_forward_target_rows.source_snapshot_json, '{}'::jsonb) || jsonb_build_object('last_seen_source_snapshot_json', EXCLUDED.source_snapshot_json),
      updated_at_utc = now()
    RETURNING *
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'id', upserted_rows.id::text,
    'source_pay_batch_id', upserted_rows.source_pay_batch_id::text,
    'source_pay_batch_item_id', upserted_rows.source_pay_batch_item_id::text,
    'source_pay_bank_transfer_id', CASE WHEN upserted_rows.source_pay_bank_transfer_id IS NULL THEN NULL ELSE upserted_rows.source_pay_bank_transfer_id::text END,
    'source_pay_batch_candidate_id', CASE WHEN upserted_rows.source_pay_batch_candidate_id IS NULL THEN NULL ELSE upserted_rows.source_pay_batch_candidate_id::text END,
    'candidate_id', upserted_rows.candidate_id::text,
    'umbrella_id', CASE WHEN upserted_rows.umbrella_id IS NULL THEN NULL ELSE upserted_rows.umbrella_id::text END,
    'pay_channel', upserted_rows.pay_channel,
    'adjustment_direction', upserted_rows.adjustment_direction,
    'amount_ex_vat', upserted_rows.amount_ex_vat,
    'amount_vat', upserted_rows.amount_vat,
    'amount_inc_vat', upserted_rows.amount_inc_vat,
    'description', upserted_rows.description,
    'status', upserted_rows.status,
    'was_existing', upserted_rows.source_pay_batch_item_id = ANY(COALESCE(v_existing_source_item_ids, ARRAY[]::uuid[])),
    'signed_amount_convention', 'SIGNED_AMOUNTS'
  ) ORDER BY upserted_rows.source_pay_batch_item_id::text), '[]'::jsonb)
  INTO v_records_json
  FROM upserted_rows;

  SELECT
    COALESCE((count(*) FILTER (WHERE COALESCE((record_values.record_json->>'was_existing')::boolean, false)))::integer, 0),
    COALESCE((count(*) FILTER (WHERE NOT COALESCE((record_values.record_json->>'was_existing')::boolean, false)))::integer, 0)
  INTO v_existing_count, v_created_count
  FROM jsonb_array_elements(COALESCE(v_records_json, '[]'::jsonb)) AS record_values(record_json);

  WITH recorded_items AS (
    SELECT NULLIF(btrim(record_values.record_json->>'source_pay_batch_item_id'), '')::uuid AS source_item_id
    FROM jsonb_array_elements(COALESCE(v_records_json, '[]'::jsonb)) AS record_values(record_json)
    WHERE NULLIF(btrim(record_values.record_json->>'source_pay_batch_item_id'), '') ~ v_uuid_regex
  ), skipped_items AS (
    SELECT source_item_values.source_item_id
    FROM unnest(COALESCE(v_source_pay_batch_item_ids, ARRAY[]::uuid[])) AS source_item_values(source_item_id)
    WHERE NOT EXISTS (
      SELECT 1
      FROM recorded_items AS recorded_item_rows
      WHERE recorded_item_rows.source_item_id = source_item_values.source_item_id
    )
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'pay_batch_item_id', skipped_items.source_item_id::text,
    'reason', CASE
      WHEN skipped_items.source_item_id = ANY(COALESCE(v_safe_source_item_ids, ARRAY[]::uuid[])) THEN 'SAFE_ITEM_NOT_INSERTED_OR_REUSED_TARGET_CARRY_FORWARD_SOURCE'
      ELSE 'NOT_SOURCE_LESS_CARRY_FORWARD_SAFE'
    END
  ) ORDER BY skipped_items.source_item_id::text), '[]'::jsonb)
  INTO v_skipped_json
  FROM skipped_items;

  v_skipped_count := COALESCE(jsonb_array_length(COALESCE(v_skipped_json, '[]'::jsonb)), 0);

  RETURN jsonb_build_object(
    'ok', true,
    'source_pay_batch_id', v_source_pay_batch_id::text,
    'source_correction_request_id', CASE WHEN p_source_correction_request_id IS NULL THEN NULL ELSE p_source_correction_request_id::text END,
    'source_correction_work_item_id', CASE WHEN p_source_correction_work_item_id IS NULL THEN NULL ELSE p_source_correction_work_item_id::text END,
    'created_count', COALESCE(v_created_count, 0),
    'existing_count', COALESCE(v_existing_count, 0),
    'skipped_count', COALESCE(v_skipped_count, 0),
    'carry_forward_records', COALESCE(v_records_json, '[]'::jsonb),
    'skipped_items', COALESCE(v_skipped_json, '[]'::jsonb),
    'detection_result', COALESCE(v_detection_result, '{}'::jsonb),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true
  );
END;
$function$;

-- _pay_manual_adjustment_carry_forward_freshness_check(uuid,uuid[],uuid[],jsonb,uuid)
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_freshness_check(p_pay_batch_id uuid DEFAULT NULL::uuid, p_candidate_ids uuid[] DEFAULT NULL::uuid[], p_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[], p_resolved_scope_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_candidate_ids uuid[] := COALESCE(p_candidate_ids, ARRAY[]::uuid[]);
  v_pay_batch_item_ids uuid[] := COALESCE(p_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_blockers jsonb := '[]'::jsonb;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_carry_forward_ids_json jsonb := '[]'::jsonb;
  v_blocker_count integer := 0;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_effective_pay_batch_id IS NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_effective_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_effective_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  WITH raw_candidate_values AS (
    SELECT candidate_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'candidate_ids') = 'array' THEN v_scope_json->'candidate_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,candidate_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,candidate_ids}'
        ELSE '[]'::jsonb
      END
    ) AS candidate_values(raw_value)
  ), raw_item_values AS (
    SELECT item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
        WHEN jsonb_typeof(v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}') = 'array' THEN v_scope_json #> '{resolved_full_payment_scope_json,pay_batch_item_ids}'
        ELSE '[]'::jsonb
      END
    ) AS item_values(raw_value)
  ), all_candidates AS (
    SELECT candidate_array_values.candidate_id AS candidate_id
    FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS candidate_array_values(candidate_id)
    WHERE candidate_array_values.candidate_id IS NOT NULL
    UNION ALL
    SELECT NULLIF(btrim(raw_candidate_values.raw_value), '')::uuid AS candidate_id
    FROM raw_candidate_values
    WHERE NULLIF(btrim(raw_candidate_values.raw_value), '') ~ v_uuid_regex
  ), all_items AS (
    SELECT item_array_values.item_id AS pay_batch_item_id
    FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS item_array_values(item_id)
    WHERE item_array_values.item_id IS NOT NULL
    UNION ALL
    SELECT NULLIF(btrim(raw_item_values.raw_value), '')::uuid AS pay_batch_item_id
    FROM raw_item_values
    WHERE NULLIF(btrim(raw_item_values.raw_value), '') ~ v_uuid_regex
  )
  SELECT
    COALESCE((SELECT array_agg(DISTINCT all_candidates.candidate_id) FROM all_candidates WHERE all_candidates.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE((SELECT array_agg(DISTINCT all_items.pay_batch_item_id) FROM all_items WHERE all_items.pay_batch_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_candidate_ids, v_pay_batch_item_ids;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0
     AND COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0 THEN
    SELECT COALESCE(array_agg(DISTINCT batch_candidate_rows.candidate_id) FILTER (WHERE batch_candidate_rows.candidate_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM public.pay_batch_candidates AS batch_candidate_rows
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
     AND COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    SELECT COALESCE(array_agg(DISTINCT item_rows.id), ARRAY[]::uuid[])
    INTO v_pay_batch_item_ids
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id;
  END IF;

  WITH target_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      batch_candidate_rows.pay_batch_id,
      batch_candidate_rows.candidate_id,
      item_rows.pay_channel,
      item_rows.operation_source_key,
      item_rows.source_ref,
      item_rows.amount_ex_vat,
      item_rows.amount_vat,
      item_rows.amount_inc_vat,
      item_rows.is_voided
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    WHERE batch_candidate_rows.pay_batch_id = v_effective_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
        OR item_rows.id = ANY(v_pay_batch_item_ids)
      )
      AND (
        COALESCE(array_length(v_candidate_ids, 1), 0) = 0
        OR batch_candidate_rows.candidate_id = ANY(v_candidate_ids)
      )
  ), relevant_carry_forwards AS (
    SELECT DISTINCT carry_forward_rows.*
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN target_items AS target_item_rows
      ON target_item_rows.pay_batch_item_id = carry_forward_rows.target_pay_batch_item_id
      OR lower(COALESCE(target_item_rows.operation_source_key, '')) = 'carry_forward:' || carry_forward_rows.id::text
      OR lower(COALESCE(target_item_rows.source_ref, '')) = 'carry_forward:' || carry_forward_rows.id::text
    WHERE carry_forward_rows.target_pay_batch_id = v_effective_pay_batch_id
       OR target_item_rows.pay_batch_item_id IS NOT NULL
       OR (
         COALESCE(array_length(v_candidate_ids, 1), 0) > 0
         AND carry_forward_rows.candidate_id = ANY(COALESCE(v_candidate_ids, ARRAY[]::uuid[]))
         AND carry_forward_rows.status IN ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH', 'CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW')
       )
  ), target_carry_forward_item_rows AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      target_batch_item_rows.pay_batch_item_id AS target_pay_batch_item_id,
      target_batch_item_rows.pay_batch_id AS target_pay_batch_id,
      target_batch_item_rows.candidate_id AS target_candidate_id,
      target_batch_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_batch_item_rows.amount_vat AS target_amount_vat,
      target_batch_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_batch_item_rows.is_voided AS target_is_voided,
      target_batch_item_rows.source_ref AS target_source_ref,
      target_batch_item_rows.operation_source_key AS target_operation_source_key
    FROM relevant_carry_forwards
    LEFT JOIN target_items AS target_batch_item_rows
      ON target_batch_item_rows.pay_batch_item_id = relevant_carry_forwards.target_pay_batch_item_id
      OR lower(COALESCE(target_batch_item_rows.operation_source_key, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
      OR lower(COALESCE(target_batch_item_rows.source_ref, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
  ), source_carry_forward_item_rows AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      source_batch_item_rows.id AS source_pay_batch_item_id,
      source_batch_item_rows.amount_ex_vat AS source_amount_ex_vat,
      source_batch_item_rows.amount_vat AS source_amount_vat,
      source_batch_item_rows.amount_inc_vat AS source_amount_inc_vat,
      source_batch_item_rows.pay_bank_transfer_id AS source_pay_bank_transfer_id,
      source_candidate_rows.settlement_status AS source_settlement_status,
      source_candidate_rows.settled_at_utc AS source_settled_at_utc,
      source_transfer_rows.status AS source_transfer_status,
      source_transfer_rows.rail_state AS source_transfer_rail_state,
      COALESCE(source_transfer_rows.rail_meta_json, '{}'::jsonb) AS source_transfer_meta_json
    FROM relevant_carry_forwards
    LEFT JOIN public.pay_batch_items AS source_batch_item_rows
      ON source_batch_item_rows.id = relevant_carry_forwards.source_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS source_candidate_rows
      ON source_candidate_rows.id = source_batch_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS source_transfer_rows
      ON source_transfer_rows.id = source_batch_item_rows.pay_bank_transfer_id
  ), source_classified_rows AS (
    SELECT
      source_carry_forward_item_rows.*,
      COALESCE(source_classification_rows.is_final_money_moved, false) AS source_is_final_money_moved
    FROM source_carry_forward_item_rows
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      source_carry_forward_item_rows.source_transfer_status,
      source_carry_forward_item_rows.source_transfer_rail_state,
      source_carry_forward_item_rows.source_transfer_meta_json,
      source_carry_forward_item_rows.source_transfer_meta_json
    ) AS source_classification_rows ON source_carry_forward_item_rows.source_pay_bank_transfer_id IS NOT NULL
  ), checked_carry_forwards AS (
    SELECT
      relevant_carry_forwards.id AS carry_forward_id,
      relevant_carry_forwards.status AS carry_forward_status,
      relevant_carry_forwards.candidate_id,
      relevant_carry_forwards.pay_channel,
      relevant_carry_forwards.amount_ex_vat AS carry_forward_amount_ex_vat,
      relevant_carry_forwards.amount_vat AS carry_forward_amount_vat,
      relevant_carry_forwards.amount_inc_vat AS carry_forward_amount_inc_vat,
      relevant_carry_forwards.source_pay_batch_id,
      relevant_carry_forwards.source_pay_batch_item_id,
      relevant_carry_forwards.target_pay_batch_id,
      relevant_carry_forwards.target_pay_batch_item_id,
      target_carry_forward_item_rows.target_pay_batch_item_id AS actual_target_pay_batch_item_id,
      target_carry_forward_item_rows.target_pay_batch_id AS actual_target_pay_batch_id,
      target_carry_forward_item_rows.target_amount_ex_vat,
      target_carry_forward_item_rows.target_amount_vat,
      target_carry_forward_item_rows.target_amount_inc_vat,
      target_carry_forward_item_rows.target_is_voided,
      source_classified_rows.source_pay_batch_item_id AS actual_source_pay_batch_item_id,
      source_classified_rows.source_amount_ex_vat,
      source_classified_rows.source_amount_vat,
      source_classified_rows.source_amount_inc_vat,
      source_classified_rows.source_settlement_status,
      source_classified_rows.source_settled_at_utc,
      source_classified_rows.source_is_final_money_moved,
      EXISTS (
        SELECT 1
        FROM target_items AS represented_target_items
        WHERE represented_target_items.candidate_id = relevant_carry_forwards.candidate_id
          AND upper(btrim(COALESCE(represented_target_items.pay_channel, ''))) = upper(btrim(COALESCE(relevant_carry_forwards.pay_channel, '')))
          AND COALESCE(represented_target_items.is_voided, false) = false
          AND (
            represented_target_items.pay_batch_item_id = relevant_carry_forwards.target_pay_batch_item_id
            OR lower(COALESCE(represented_target_items.operation_source_key, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
            OR lower(COALESCE(represented_target_items.source_ref, '')) = 'carry_forward:' || relevant_carry_forwards.id::text
          )
      ) AS is_represented_in_target_batch
    FROM relevant_carry_forwards
    LEFT JOIN target_carry_forward_item_rows
      ON target_carry_forward_item_rows.carry_forward_id = relevant_carry_forwards.id
    LEFT JOIN source_classified_rows
      ON source_classified_rows.carry_forward_id = relevant_carry_forwards.id
  ), blocker_rows AS (
    SELECT
      CASE
        WHEN checked_carry_forwards.carry_forward_status IN ('CANCELLED', 'SUPERSEDED', 'NEEDS_REVIEW') THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CHANGED'
        WHEN checked_carry_forwards.carry_forward_status = 'CONSUMED_IN_BATCH'
          AND checked_carry_forwards.target_pay_batch_id IS DISTINCT FROM v_effective_pay_batch_id THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_CONSUMED_ELSEWHERE'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id IS NOT NULL
          AND checked_carry_forwards.target_pay_batch_id IS DISTINCT FROM v_effective_pay_batch_id THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_RESERVED_ELSEWHERE'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND checked_carry_forwards.actual_target_pay_batch_item_id IS NULL THEN 'RESERVED_CARRY_FORWARD_MISSING_FROM_TARGET_BATCH'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND COALESCE(checked_carry_forwards.target_is_voided, false) THEN 'RESERVED_CARRY_FORWARD_TARGET_ITEM_VOIDED'
        WHEN checked_carry_forwards.carry_forward_status = 'RESERVED_IN_DRAFT'
          AND checked_carry_forwards.target_pay_batch_id = v_effective_pay_batch_id
          AND (
            round(COALESCE(checked_carry_forwards.carry_forward_amount_ex_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_ex_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_inc_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.target_amount_inc_vat, 0), 2)
          ) THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_AMOUNT_CHANGED'
        WHEN checked_carry_forwards.actual_source_pay_batch_item_id IS NOT NULL
          AND (
            round(COALESCE(checked_carry_forwards.carry_forward_amount_ex_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_ex_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_vat, 0), 2)
            OR round(COALESCE(checked_carry_forwards.carry_forward_amount_inc_vat, 0), 2) <> round(COALESCE(checked_carry_forwards.source_amount_inc_vat, 0), 2)
          ) THEN 'MANUAL_ADJUSTMENT_CARRY_FORWARD_SOURCE_AMOUNT_CHANGED'
        WHEN upper(COALESCE(checked_carry_forwards.source_settlement_status, '')) = 'SETTLED'
          OR checked_carry_forwards.source_settled_at_utc IS NOT NULL
          OR COALESCE(checked_carry_forwards.source_is_final_money_moved, false) THEN 'SOURCE_PAYMENT_SCOPE_BECAME_PAID_OR_SETTLED'
        WHEN checked_carry_forwards.carry_forward_status = 'PENDING_CARRY_FORWARD'
          AND COALESCE(array_length(v_candidate_ids, 1), 0) > 0
          AND checked_carry_forwards.candidate_id = ANY(COALESCE(v_candidate_ids, ARRAY[]::uuid[]))
          AND checked_carry_forwards.is_represented_in_target_batch = false THEN 'PENDING_CARRY_FORWARD_NOT_INCLUDED_IN_TARGET_BATCH'
        ELSE NULL::text
      END AS blocker_code,
      checked_carry_forwards.carry_forward_id,
      checked_carry_forwards.carry_forward_status,
      checked_carry_forwards.candidate_id,
      checked_carry_forwards.pay_channel,
      checked_carry_forwards.source_pay_batch_id,
      checked_carry_forwards.source_pay_batch_item_id,
      checked_carry_forwards.target_pay_batch_id,
      checked_carry_forwards.target_pay_batch_item_id,
      checked_carry_forwards.actual_target_pay_batch_item_id,
      checked_carry_forwards.carry_forward_amount_inc_vat,
      checked_carry_forwards.target_amount_inc_vat
    FROM checked_carry_forwards
  )
  SELECT
    COALESCE(jsonb_agg(jsonb_build_object(
      'code', blocker_rows.blocker_code,
      'carry_forward_id', blocker_rows.carry_forward_id::text,
      'status', blocker_rows.carry_forward_status,
      'candidate_id', CASE WHEN blocker_rows.candidate_id IS NULL THEN NULL ELSE blocker_rows.candidate_id::text END,
      'pay_channel', blocker_rows.pay_channel,
      'source_pay_batch_id', blocker_rows.source_pay_batch_id::text,
      'source_pay_batch_item_id', blocker_rows.source_pay_batch_item_id::text,
      'target_pay_batch_id', CASE WHEN blocker_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocker_rows.target_pay_batch_id::text END,
      'target_pay_batch_item_id', CASE WHEN blocker_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocker_rows.target_pay_batch_item_id::text END,
      'actual_target_pay_batch_item_id', CASE WHEN blocker_rows.actual_target_pay_batch_item_id IS NULL THEN NULL ELSE blocker_rows.actual_target_pay_batch_item_id::text END,
      'carry_forward_amount_inc_vat', blocker_rows.carry_forward_amount_inc_vat,
      'target_amount_inc_vat', blocker_rows.target_amount_inc_vat
    ) ORDER BY blocker_rows.carry_forward_id::text, blocker_rows.blocker_code) FILTER (WHERE blocker_rows.blocker_code IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(DISTINCT blocker_rows.blocker_code) FILTER (WHERE blocker_rows.blocker_code IS NOT NULL), '[]'::jsonb),
    COALESCE(jsonb_agg(DISTINCT blocker_rows.carry_forward_id::text) FILTER (WHERE blocker_rows.carry_forward_id IS NOT NULL), '[]'::jsonb)
  INTO v_blockers, v_stale_reasons, v_carry_forward_ids_json
  FROM blocker_rows;

  v_blocker_count := COALESCE(jsonb_array_length(COALESCE(v_blockers, '[]'::jsonb)), 0);

  RETURN jsonb_build_object(
    'ok', v_blocker_count = 0,
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'carry_forward_ids', COALESCE(v_carry_forward_ids_json, '[]'::jsonb),
    'stale_reasons', COALESCE(v_stale_reasons, '[]'::jsonb),
    'support_details_json', jsonb_build_object(
      'pay_batch_id', v_effective_pay_batch_id::text,
      'candidate_ids', COALESCE((SELECT jsonb_agg(candidate_values.candidate_id::text ORDER BY candidate_values.candidate_id::text) FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS candidate_values(candidate_id)), '[]'::jsonb),
      'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(item_values.pay_batch_item_id::text ORDER BY item_values.pay_batch_item_id::text) FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS item_values(pay_batch_item_id)), '[]'::jsonb),
      'blocker_count', v_blocker_count,
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true
    )
  );
END;
$function$;

-- _pay_manual_adjustment_carry_forward_mark_consumed(uuid,uuid[],jsonb,jsonb,uuid)
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_mark_consumed(p_target_pay_batch_id uuid DEFAULT NULL::uuid, p_target_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[], p_resolved_scope_json jsonb DEFAULT NULL::jsonb, p_final_paid_evidence_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_scope_item_ids uuid[] := COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_explicit_final_paid boolean := false;
  v_candidate_count integer := 0;
  v_consumed_count integer := 0;
  v_existing_consumed_count integer := 0;
  v_blocked_count integer := 0;
  v_consumed_ids jsonb := '[]'::jsonb;
  v_existing_consumed_ids jsonb := '[]'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_target_pay_batch_id IS NULL
     AND v_scope_json IS NOT NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_target_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;

    v_scope_item_ids := COALESCE(v_scope_item_ids, ARRAY[]::uuid[]) || COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);

    SELECT COALESCE(array_agg(DISTINCT item_id_values.item_id) FILTER (WHERE item_id_values.item_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM unnest(COALESCE(v_scope_item_ids, ARRAY[]::uuid[])) AS item_id_values(item_id);

    WITH raw_values AS (
      SELECT transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_scope_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  IF v_target_pay_batch_id IS NULL AND COALESCE(array_length(v_scope_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED')::text;
  END IF;

  IF p_final_paid_evidence_json IS NOT NULL AND jsonb_typeof(p_final_paid_evidence_json) = 'object' THEN
    v_explicit_final_paid := (
      lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'final_paid', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'is_final_money_moved', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'money_moved', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'payment_made', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR lower(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'settled', '')), '')) IN ('true','t','yes','y','1','paid','settled','completed','success','succeeded')
      OR upper(NULLIF(btrim(COALESCE(p_final_paid_evidence_json->>'cash_state', '')), '')) = 'FINAL_PAID'
    );
  END IF;

  PERFORM 1
  FROM public.pay_manual_adjustment_carry_forwards AS lock_carry_forward_rows
  WHERE lock_carry_forward_rows.status IN ('RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH')
    AND (
      (v_target_pay_batch_id IS NOT NULL AND lock_carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
      OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND lock_carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
    )
  ORDER BY lock_carry_forward_rows.id
  FOR UPDATE;

  WITH candidate_rows AS (
    SELECT
      carry_forward_rows.id AS carry_forward_id,
      carry_forward_rows.status AS carry_forward_status,
      carry_forward_rows.target_pay_batch_id,
      carry_forward_rows.target_pay_batch_item_id,
      carry_forward_rows.amount_ex_vat,
      carry_forward_rows.amount_vat,
      carry_forward_rows.amount_inc_vat,
      target_item_rows.pay_bank_transfer_id,
      target_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_item_rows.amount_vat AS target_amount_vat,
      target_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_item_rows.is_voided AS target_is_voided,
      batch_candidate_rows.settlement_status,
      batch_candidate_rows.settled_at_utc,
      target_transfer_rows.status AS transfer_status,
      target_transfer_rows.rail_state,
      target_transfer_rows.completed_at_utc,
      target_transfer_rows.rail_meta_json,
      classifier_rows.cash_state,
      classifier_rows.is_final_money_moved
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN public.pay_batch_items AS target_item_rows
      ON target_item_rows.id = carry_forward_rows.target_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS target_transfer_rows
      ON target_transfer_rows.id = target_item_rows.pay_bank_transfer_id
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      target_transfer_rows.status,
      target_transfer_rows.rail_state,
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb),
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb)
    ) AS classifier_rows
    WHERE carry_forward_rows.status IN ('RESERVED_IN_DRAFT', 'CONSUMED_IN_BATCH')
      AND (
        (v_target_pay_batch_id IS NOT NULL AND carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
        OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
        OR (COALESCE(array_length(v_scope_transfer_ids, 1), 0) > 0 AND target_item_rows.pay_bank_transfer_id = ANY(v_scope_transfer_ids))
      )
  ), classified_rows AS (
    SELECT
      candidate_rows.*,
      (
        COALESCE(candidate_rows.is_final_money_moved, false)
        OR upper(COALESCE(candidate_rows.cash_state, '')) = 'FINAL_PAID'
        OR upper(COALESCE(candidate_rows.settlement_status, '')) = 'SETTLED'
        OR candidate_rows.settled_at_utc IS NOT NULL
        OR COALESCE(v_explicit_final_paid, false)
      ) AS has_final_paid_or_settled_evidence,
      (
        candidate_rows.target_pay_batch_item_id IS NOT NULL
        AND candidate_rows.target_amount_inc_vat IS NOT NULL
        AND round(candidate_rows.target_amount_inc_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_inc_vat, 2)
        AND (
          (candidate_rows.amount_ex_vat IS NULL AND candidate_rows.target_amount_ex_vat IS NULL)
          OR (candidate_rows.amount_ex_vat IS NOT NULL AND candidate_rows.target_amount_ex_vat IS NOT NULL AND round(candidate_rows.target_amount_ex_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_ex_vat, 2))
        )
        AND (
          (candidate_rows.amount_vat IS NULL AND candidate_rows.target_amount_vat IS NULL)
          OR (candidate_rows.amount_vat IS NOT NULL AND candidate_rows.target_amount_vat IS NOT NULL AND round(candidate_rows.target_amount_vat, 2) IS NOT DISTINCT FROM round(candidate_rows.amount_vat, 2))
        )
      ) AS target_amount_matches
    FROM candidate_rows
  ), existing_consumed_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.carry_forward_status = 'CONSUMED_IN_BATCH'
  ), consumable_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.carry_forward_status = 'RESERVED_IN_DRAFT'
      AND classified_rows.has_final_paid_or_settled_evidence
      AND classified_rows.target_amount_matches
  ), blocked_rows AS (
    SELECT classified_rows.*,
      CASE
        WHEN classified_rows.carry_forward_status <> 'RESERVED_IN_DRAFT' THEN 'CARRY_FORWARD_STATUS_NOT_RESERVED'
        WHEN classified_rows.target_pay_batch_item_id IS NULL THEN 'CARRY_FORWARD_TARGET_ITEM_MISSING'
        WHEN classified_rows.target_amount_matches = false THEN 'CARRY_FORWARD_TARGET_AMOUNT_MISMATCH'
        WHEN classified_rows.has_final_paid_or_settled_evidence = false THEN 'FINAL_PAID_EVIDENCE_REQUIRED'
        ELSE 'CARRY_FORWARD_NOT_CONSUMABLE'
      END AS blocker_code
    FROM classified_rows
    WHERE classified_rows.carry_forward_status <> 'CONSUMED_IN_BATCH'
      AND NOT (
        classified_rows.carry_forward_status = 'RESERVED_IN_DRAFT'
        AND classified_rows.has_final_paid_or_settled_evidence
        AND classified_rows.target_amount_matches
      )
  ), updated_rows AS (
    UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_update_rows
    SET
      status = 'CONSUMED_IN_BATCH',
      consumed_at_utc = COALESCE(carry_forward_update_rows.consumed_at_utc, now()),
      status_reason = 'Consumed after final paid/settled evidence for target payment.',
      updated_at_utc = now()
    FROM consumable_rows
    WHERE carry_forward_update_rows.id = consumable_rows.carry_forward_id
    RETURNING carry_forward_update_rows.id
  )
  SELECT
    (SELECT count(*)::integer FROM candidate_rows),
    (SELECT count(*)::integer FROM updated_rows),
    (SELECT count(*)::integer FROM existing_consumed_rows),
    (SELECT count(*)::integer FROM blocked_rows),
    COALESCE((SELECT jsonb_agg(updated_rows.id::text ORDER BY updated_rows.id::text) FROM updated_rows), '[]'::jsonb),
    COALESCE((SELECT jsonb_agg(existing_consumed_rows.carry_forward_id::text ORDER BY existing_consumed_rows.carry_forward_id::text) FROM existing_consumed_rows), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'code', blocked_rows.blocker_code,
        'carry_forward_id', blocked_rows.carry_forward_id::text,
        'status', blocked_rows.carry_forward_status,
        'target_pay_batch_id', CASE WHEN blocked_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_id::text END,
        'target_pay_batch_item_id', CASE WHEN blocked_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_item_id::text END,
        'cash_state', blocked_rows.cash_state,
        'transfer_status', blocked_rows.transfer_status,
        'settlement_status', blocked_rows.settlement_status,
        'settled_at_utc', blocked_rows.settled_at_utc,
        'completed_at_utc', blocked_rows.completed_at_utc,
        'target_amount_matches', blocked_rows.target_amount_matches,
        'signed_amount_convention', 'SIGNED_AMOUNTS'
      ) ORDER BY blocked_rows.carry_forward_id::text)
      FROM blocked_rows
    ), '[]'::jsonb)
  INTO
    v_candidate_count,
    v_consumed_count,
    v_existing_consumed_count,
    v_blocked_count,
    v_consumed_ids,
    v_existing_consumed_ids,
    v_blockers;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_blocked_count, 0) = 0,
    'target_pay_batch_id', CASE WHEN v_target_pay_batch_id IS NULL THEN NULL ELSE v_target_pay_batch_id::text END,
    'candidate_count', COALESCE(v_candidate_count, 0),
    'consumed_count', COALESCE(v_consumed_count, 0),
    'existing_consumed_count', COALESCE(v_existing_consumed_count, 0),
    'consumed_carry_forward_ids', COALESCE(v_consumed_ids, '[]'::jsonb),
    'existing_consumed_carry_forward_ids', COALESCE(v_existing_consumed_ids, '[]'::jsonb),
    'blocked_count', COALESCE(v_blocked_count, 0),
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'explicit_final_paid_evidence', COALESCE(v_explicit_final_paid, false),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );
END;
$function$;

-- _pay_manual_adjustment_carry_forward_release_for_scope(uuid,jsonb,uuid[],uuid,text)
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_release_for_scope(p_target_pay_batch_id uuid DEFAULT NULL::uuid, p_resolved_scope_json jsonb DEFAULT NULL::jsonb, p_target_pay_batch_item_ids uuid[] DEFAULT NULL::uuid[], p_actor_user_id uuid DEFAULT NULL::uuid, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_scope_json jsonb := COALESCE(p_resolved_scope_json, '{}'::jsonb);
  v_scope_item_ids uuid[] := COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);
  v_scope_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_released_ids jsonb := '[]'::jsonb;
  v_blocked_items jsonb := '[]'::jsonb;
  v_candidate_count integer := 0;
  v_released_count integer := 0;
  v_blocked_count integer := 0;
BEGIN
  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) <> 'object' THEN
    RAISE EXCEPTION 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'RESOLVED_SCOPE_JSON_MUST_BE_OBJECT')::text;
  END IF;

  IF v_target_pay_batch_id IS NULL
     AND v_scope_json IS NOT NULL
     AND NULLIF(btrim(COALESCE(v_scope_json->>'pay_batch_id', '')), '') ~ v_uuid_regex THEN
    v_target_pay_batch_id := (v_scope_json->>'pay_batch_id')::uuid;
  END IF;

  IF v_scope_json IS NOT NULL AND jsonb_typeof(v_scope_json) = 'object' THEN
    WITH raw_values AS (
      SELECT item_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_batch_item_ids') = 'array' THEN v_scope_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;

    v_scope_item_ids := COALESCE(v_scope_item_ids, ARRAY[]::uuid[]) || COALESCE(p_target_pay_batch_item_ids, ARRAY[]::uuid[]);

    SELECT COALESCE(array_agg(DISTINCT item_id_values.item_id) FILTER (WHERE item_id_values.item_id IS NOT NULL), ARRAY[]::uuid[])
    INTO v_scope_item_ids
    FROM unnest(COALESCE(v_scope_item_ids, ARRAY[]::uuid[])) AS item_id_values(item_id);

    WITH raw_values AS (
      SELECT transfer_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_scope_json->'pay_bank_transfer_ids') = 'array' THEN v_scope_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_values(raw_value)
    ), clean_values AS (
      SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
      FROM raw_values
    )
    SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
    INTO v_scope_transfer_ids
    FROM clean_values
    WHERE clean_values.clean_value IS NOT NULL
      AND clean_values.clean_value ~ v_uuid_regex;
  END IF;

  IF v_target_pay_batch_id IS NULL AND COALESCE(array_length(v_scope_item_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_OR_ITEM_SCOPE_REQUIRED')::text;
  END IF;

  PERFORM 1
  FROM public.pay_manual_adjustment_carry_forwards AS lock_carry_forward_rows
  WHERE lock_carry_forward_rows.status = 'RESERVED_IN_DRAFT'
    AND (
      (v_target_pay_batch_id IS NOT NULL AND lock_carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
      OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND lock_carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
    )
  ORDER BY lock_carry_forward_rows.id
  FOR UPDATE;

  WITH candidate_rows AS (
    SELECT
      carry_forward_rows.id AS carry_forward_id,
      carry_forward_rows.target_pay_batch_id,
      carry_forward_rows.target_pay_batch_item_id,
      carry_forward_rows.status,
      carry_forward_rows.amount_ex_vat,
      carry_forward_rows.amount_vat,
      carry_forward_rows.amount_inc_vat,
      target_item_rows.pay_bank_transfer_id,
      target_item_rows.amount_ex_vat AS target_amount_ex_vat,
      target_item_rows.amount_vat AS target_amount_vat,
      target_item_rows.amount_inc_vat AS target_amount_inc_vat,
      target_item_rows.is_voided AS target_is_voided,
      batch_candidate_rows.settlement_status,
      batch_candidate_rows.settled_at_utc,
      target_transfer_rows.status AS transfer_status,
      target_transfer_rows.rail_state,
      target_transfer_rows.completed_at_utc,
      target_transfer_rows.rail_meta_json,
      classifier_rows.cash_state,
      classifier_rows.is_final_money_moved
    FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
    LEFT JOIN public.pay_batch_items AS target_item_rows
      ON target_item_rows.id = carry_forward_rows.target_pay_batch_item_id
    LEFT JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS target_transfer_rows
      ON target_transfer_rows.id = target_item_rows.pay_bank_transfer_id
    CROSS JOIN LATERAL public._pay_rail_state_money_movement_classify(
      target_transfer_rows.status,
      target_transfer_rows.rail_state,
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb),
      COALESCE(target_transfer_rows.rail_meta_json, '{}'::jsonb)
    ) AS classifier_rows
    WHERE carry_forward_rows.status = 'RESERVED_IN_DRAFT'
      AND (
        (v_target_pay_batch_id IS NOT NULL AND carry_forward_rows.target_pay_batch_id = v_target_pay_batch_id)
        OR (COALESCE(array_length(v_scope_item_ids, 1), 0) > 0 AND carry_forward_rows.target_pay_batch_item_id = ANY(v_scope_item_ids))
        OR (COALESCE(array_length(v_scope_transfer_ids, 1), 0) > 0 AND target_item_rows.pay_bank_transfer_id = ANY(v_scope_transfer_ids))
      )
  ), classified_rows AS (
    SELECT
      candidate_rows.*,
      (
        COALESCE(candidate_rows.is_final_money_moved, false)
        OR upper(COALESCE(candidate_rows.cash_state, '')) = 'FINAL_PAID'
        OR upper(COALESCE(candidate_rows.settlement_status, '')) = 'SETTLED'
        OR candidate_rows.settled_at_utc IS NOT NULL
      ) AS has_final_paid_or_settled_evidence
    FROM candidate_rows
  ), blocked_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.has_final_paid_or_settled_evidence
  ), releasable_rows AS (
    SELECT classified_rows.*
    FROM classified_rows
    WHERE classified_rows.has_final_paid_or_settled_evidence = false
  ), updated_rows AS (
    UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_update_rows
    SET
      status = 'PENDING_CARRY_FORWARD',
      target_pay_batch_id = NULL,
      target_pay_batch_item_id = NULL,
      target_operation_source_key = NULL,
      reserved_at_utc = NULL,
      released_at_utc = now(),
      status_reason = COALESCE(NULLIF(btrim(p_reason), ''), 'Released because target payment scope was cancelled or unwound before money moved.'),
      updated_at_utc = now()
    FROM releasable_rows
    WHERE carry_forward_update_rows.id = releasable_rows.carry_forward_id
    RETURNING carry_forward_update_rows.id
  )
  SELECT
    (SELECT count(*)::integer FROM candidate_rows),
    (SELECT count(*)::integer FROM updated_rows),
    (SELECT count(*)::integer FROM blocked_rows),
    COALESCE((SELECT jsonb_agg(updated_rows.id::text ORDER BY updated_rows.id::text) FROM updated_rows), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'code', 'CARRY_FORWARD_TARGET_HAS_FINAL_PAID_OR_SETTLED_EVIDENCE',
        'carry_forward_id', blocked_rows.carry_forward_id::text,
        'target_pay_batch_id', CASE WHEN blocked_rows.target_pay_batch_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_id::text END,
        'target_pay_batch_item_id', CASE WHEN blocked_rows.target_pay_batch_item_id IS NULL THEN NULL ELSE blocked_rows.target_pay_batch_item_id::text END,
        'pay_bank_transfer_id', CASE WHEN blocked_rows.pay_bank_transfer_id IS NULL THEN NULL ELSE blocked_rows.pay_bank_transfer_id::text END,
        'cash_state', blocked_rows.cash_state,
        'transfer_status', blocked_rows.transfer_status,
        'settlement_status', blocked_rows.settlement_status,
        'settled_at_utc', blocked_rows.settled_at_utc,
        'completed_at_utc', blocked_rows.completed_at_utc
      ) ORDER BY blocked_rows.carry_forward_id::text)
      FROM blocked_rows
    ), '[]'::jsonb)
  INTO
    v_candidate_count,
    v_released_count,
    v_blocked_count,
    v_released_ids,
    v_blocked_items;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_blocked_count, 0) = 0,
    'target_pay_batch_id', CASE WHEN v_target_pay_batch_id IS NULL THEN NULL ELSE v_target_pay_batch_id::text END,
    'candidate_count', COALESCE(v_candidate_count, 0),
    'released_count', COALESCE(v_released_count, 0),
    'released_carry_forward_ids', COALESCE(v_released_ids, '[]'::jsonb),
    'blocked_count', COALESCE(v_blocked_count, 0),
    'blockers', COALESCE(v_blocked_items, '[]'::jsonb),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );
END;
$function$;

-- _pay_manual_adjustment_carry_forward_reserve_for_batch_item(uuid,uuid,uuid,text,uuid)
CREATE OR REPLACE FUNCTION public._pay_manual_adjustment_carry_forward_reserve_for_batch_item(p_carry_forward_id uuid, p_target_pay_batch_id uuid, p_target_pay_batch_item_id uuid, p_target_operation_source_key text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_carry_forward_row public.pay_manual_adjustment_carry_forwards%ROWTYPE;
  v_target_item_id uuid := p_target_pay_batch_item_id;
  v_target_pay_batch_id uuid := p_target_pay_batch_id;
  v_target_operation_source_key text := NULL::text;
  v_target_item_pay_batch_id uuid := NULL::uuid;
  v_target_item_candidate_id uuid := NULL::uuid;
  v_target_item_pay_channel text := NULL::text;
  v_target_item_amount_ex_vat numeric := NULL::numeric;
  v_target_item_amount_vat numeric := NULL::numeric;
  v_target_item_amount_inc_vat numeric := NULL::numeric;
  v_target_item_operation_source_key text := NULL::text;
  v_target_item_source_ref text := NULL::text;
  v_target_item_is_voided boolean := false;
  v_existing_target_carry_forward_id uuid := NULL::uuid;
  v_blockers jsonb := '[]'::jsonb;
  v_result_status text := NULL::text;
  v_was_idempotent boolean := false;
BEGIN
  IF p_carry_forward_id IS NULL THEN
    RAISE EXCEPTION 'CARRY_FORWARD_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'CARRY_FORWARD_ID_REQUIRED')::text;
  END IF;

  IF p_target_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ID_REQUIRED', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  IF p_target_pay_batch_item_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ITEM_ID_REQUIRED', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  SELECT carry_forward_rows.*
  INTO v_carry_forward_row
  FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
  WHERE carry_forward_rows.id = p_carry_forward_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CARRY_FORWARD_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'CARRY_FORWARD_NOT_FOUND', 'carry_forward_id', p_carry_forward_id)::text;
  END IF;

  SELECT
    batch_candidate_rows.pay_batch_id,
    batch_candidate_rows.candidate_id,
    upper(btrim(COALESCE(target_item_rows.pay_channel, ''))),
    target_item_rows.amount_ex_vat,
    target_item_rows.amount_vat,
    target_item_rows.amount_inc_vat,
    target_item_rows.operation_source_key,
    target_item_rows.source_ref,
    COALESCE(target_item_rows.is_voided, false)
  INTO
    v_target_item_pay_batch_id,
    v_target_item_candidate_id,
    v_target_item_pay_channel,
    v_target_item_amount_ex_vat,
    v_target_item_amount_vat,
    v_target_item_amount_inc_vat,
    v_target_item_operation_source_key,
    v_target_item_source_ref,
    v_target_item_is_voided
  FROM public.pay_batch_items AS target_item_rows
  JOIN public.pay_batch_candidates AS batch_candidate_rows
    ON batch_candidate_rows.id = target_item_rows.pay_batch_candidate_id
  WHERE target_item_rows.id = p_target_pay_batch_item_id
  FOR UPDATE OF target_item_rows, batch_candidate_rows;

  IF v_target_item_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'TARGET_PAY_BATCH_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'TARGET_PAY_BATCH_ITEM_NOT_FOUND', 'target_pay_batch_item_id', p_target_pay_batch_item_id)::text;
  END IF;

  SELECT existing_target_rows.id
  INTO v_existing_target_carry_forward_id
  FROM public.pay_manual_adjustment_carry_forwards AS existing_target_rows
  WHERE existing_target_rows.target_pay_batch_item_id = p_target_pay_batch_item_id
    AND existing_target_rows.id <> p_carry_forward_id
  FOR UPDATE;

  IF v_existing_target_carry_forward_id IS NOT NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_PAY_BATCH_ITEM_ALREADY_RESERVED_BY_ANOTHER_CARRY_FORWARD',
      'carry_forward_id', p_carry_forward_id::text,
      'existing_carry_forward_id', v_existing_target_carry_forward_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  v_target_operation_source_key := COALESCE(
    NULLIF(btrim(COALESCE(p_target_operation_source_key, '')), ''),
    NULLIF(btrim(COALESCE(v_target_item_operation_source_key, '')), ''),
    'carry_forward:' || p_carry_forward_id::text
  );

  IF v_target_item_pay_batch_id IS DISTINCT FROM p_target_pay_batch_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_ITEM_BATCH_MISMATCH',
      'target_pay_batch_id', p_target_pay_batch_id::text,
      'actual_pay_batch_id', v_target_item_pay_batch_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_target_item_is_voided THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'TARGET_PAY_BATCH_ITEM_VOIDED',
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_carry_forward_row.status NOT IN ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT') THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_STATUS_NOT_RESERVABLE',
      'carry_forward_id', p_carry_forward_id::text,
      'status', v_carry_forward_row.status
    ));
  END IF;

  IF v_carry_forward_row.status = 'RESERVED_IN_DRAFT'
     AND (
       v_carry_forward_row.target_pay_batch_id IS DISTINCT FROM p_target_pay_batch_id
       OR v_carry_forward_row.target_pay_batch_item_id IS DISTINCT FROM p_target_pay_batch_item_id
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_ALREADY_RESERVED_FOR_ANOTHER_TARGET',
      'carry_forward_id', p_carry_forward_id::text,
      'existing_target_pay_batch_id', CASE WHEN v_carry_forward_row.target_pay_batch_id IS NULL THEN NULL ELSE v_carry_forward_row.target_pay_batch_id::text END,
      'existing_target_pay_batch_item_id', CASE WHEN v_carry_forward_row.target_pay_batch_item_id IS NULL THEN NULL ELSE v_carry_forward_row.target_pay_batch_item_id::text END,
      'requested_target_pay_batch_id', p_target_pay_batch_id::text,
      'requested_target_pay_batch_item_id', p_target_pay_batch_item_id::text
    ));
  END IF;

  IF v_carry_forward_row.status = 'RESERVED_IN_DRAFT'
     AND v_carry_forward_row.target_pay_batch_id IS NOT DISTINCT FROM p_target_pay_batch_id
     AND v_carry_forward_row.target_pay_batch_item_id IS NOT DISTINCT FROM p_target_pay_batch_item_id THEN
    v_was_idempotent := true;
  END IF;

  IF v_carry_forward_row.candidate_id IS DISTINCT FROM v_target_item_candidate_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_CANDIDATE_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'carry_forward_candidate_id', v_carry_forward_row.candidate_id::text,
      'target_candidate_id', CASE WHEN v_target_item_candidate_id IS NULL THEN NULL ELSE v_target_item_candidate_id::text END
    ));
  END IF;

  IF upper(btrim(COALESCE(v_carry_forward_row.pay_channel, ''))) IS DISTINCT FROM v_target_item_pay_channel THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_PAY_CHANNEL_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'carry_forward_pay_channel', v_carry_forward_row.pay_channel,
      'target_pay_channel', v_target_item_pay_channel
    ));
  END IF;

  IF v_target_item_amount_inc_vat IS NULL
     OR round(v_target_item_amount_inc_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_inc_vat, 2)
     OR (
       v_carry_forward_row.amount_ex_vat IS NULL
       AND v_target_item_amount_ex_vat IS NOT NULL
     )
     OR (
       v_carry_forward_row.amount_ex_vat IS NOT NULL
       AND (
         v_target_item_amount_ex_vat IS NULL
         OR round(v_target_item_amount_ex_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_ex_vat, 2)
       )
     )
     OR (
       v_carry_forward_row.amount_vat IS NULL
       AND v_target_item_amount_vat IS NOT NULL
     )
     OR (
       v_carry_forward_row.amount_vat IS NOT NULL
       AND (
         v_target_item_amount_vat IS NULL
         OR round(v_target_item_amount_vat, 2) IS DISTINCT FROM round(v_carry_forward_row.amount_vat, 2)
       )
     ) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'CARRY_FORWARD_TARGET_AMOUNT_MISMATCH',
      'carry_forward_id', p_carry_forward_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
      'carry_forward_amount_ex_vat', v_carry_forward_row.amount_ex_vat,
      'carry_forward_amount_vat', v_carry_forward_row.amount_vat,
      'carry_forward_amount_inc_vat', v_carry_forward_row.amount_inc_vat,
      'target_amount_ex_vat', v_target_item_amount_ex_vat,
      'target_amount_vat', v_target_item_amount_vat,
      'target_amount_inc_vat', v_target_item_amount_inc_vat,
      'signed_amount_convention', 'SIGNED_AMOUNTS'
    ));
  END IF;

  IF COALESCE(jsonb_array_length(v_blockers), 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'reserved', false,
      'idempotent', false,
      'carry_forward_id', p_carry_forward_id::text,
      'target_pay_batch_id', p_target_pay_batch_id::text,
      'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
      'blockers', v_blockers,
      'signed_amount_convention', 'SIGNED_AMOUNTS',
      'adjustment_direction_is_display_only', true
    );
  END IF;

  UPDATE public.pay_manual_adjustment_carry_forwards AS carry_forward_rows
  SET
    target_pay_batch_id = p_target_pay_batch_id,
    target_pay_batch_item_id = p_target_pay_batch_item_id,
    target_operation_source_key = v_target_operation_source_key,
    status = 'RESERVED_IN_DRAFT',
    reserved_at_utc = COALESCE(carry_forward_rows.reserved_at_utc, now()),
    released_at_utc = NULL,
    cancelled_at_utc = NULL,
    status_reason = CASE
      WHEN v_was_idempotent THEN COALESCE(carry_forward_rows.status_reason, 'Already reserved for this target item.')
      ELSE 'Reserved in draft target pay_batch_item.'
    END,
    updated_at_utc = now()
  WHERE carry_forward_rows.id = p_carry_forward_id
  RETURNING carry_forward_rows.status
  INTO v_result_status;

  RETURN jsonb_build_object(
    'ok', true,
    'reserved', true,
    'idempotent', v_was_idempotent,
    'carry_forward_id', p_carry_forward_id::text,
    'target_pay_batch_id', p_target_pay_batch_id::text,
    'target_pay_batch_item_id', p_target_pay_batch_item_id::text,
    'target_operation_source_key', v_target_operation_source_key,
    'status', COALESCE(v_result_status, 'RESERVED_IN_DRAFT'),
    'signed_amount_convention', 'SIGNED_AMOUNTS',
    'adjustment_direction_is_display_only', true
  );
END;
$function$;

-- _pay_outstanding_components(uuid[],uuid)
CREATE OR REPLACE FUNCTION public._pay_outstanding_components(p_timesheet_ids uuid[], p_exclude_pay_batch_id uuid)
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, truth_ex_vat numeric, baseline_ex_vat numeric, reserved_ex_vat numeric, outstanding_ex_vat numeric, truth_inc_vat numeric, baseline_inc_vat numeric, reserved_inc_vat numeric, outstanding_inc_vat numeric, reservation_overrun_detected boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with
inp as (
  select coalesce(
    (
      select array_agg(distinct t_input.x order by t_input.x)
      from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as t_input(x)
      where t_input.x is not null
    ),
    array[]::uuid[]
  ) as ts_ids
),
rotation_scope_rows as (
  select
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  from inp as input_scope
  join public._pay_timesheet_rotation_scope(input_scope.ts_ids) as scope_rows
    on true
),
rotation_scope_keyed as (
  select
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    coalesce(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) as scope_family_key
  from rotation_scope_rows
  where rotation_scope_rows.requested_timesheet_id is not null
),
projection_targets as (
  select
    rotation_scope_keyed.scope_family_key,
    coalesce(
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (
          where coalesce(rotation_scope_keyed.requested_is_canonical, false) = true
            and rotation_scope_keyed.canonical_timesheet_id is not null
        )
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.requested_timesheet_id order by rotation_scope_keyed.requested_timesheet_id)
        filter (where rotation_scope_keyed.requested_timesheet_id is not null)
      )[1],
      (
        array_agg(distinct rotation_scope_keyed.canonical_timesheet_id order by rotation_scope_keyed.canonical_timesheet_id)
        filter (where rotation_scope_keyed.canonical_timesheet_id is not null)
      )[1]
    ) as projected_timesheet_id
  from rotation_scope_keyed
  group by rotation_scope_keyed.scope_family_key
),
family_to_projection as (
  select distinct
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id
  from rotation_scope_keyed
  join projection_targets
    on projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  where rotation_scope_keyed.family_timesheet_id is not null
    and projection_targets.projected_timesheet_id is not null
),
timesheet_entitlement_components as (
  select
    entitlement_rows.timesheet_id,
    entitlement_rows.key_type,
    entitlement_rows.key_value,
    entitlement_rows.truth_ex_vat,
    entitlement_rows.baseline_ex_vat,
    entitlement_rows.truth_inc_vat,
    entitlement_rows.baseline_inc_vat
  from public._pay_current_timesheet_entitlement_components((select input_scope.ts_ids from inp as input_scope)) as entitlement_rows
),
reserved_components as (
  select
    reserved_component_rows.timesheet_id,
    reserved_component_rows.key_type,
    reserved_component_rows.key_value,
    reserved_component_rows.amount_ex_vat,
    reserved_component_rows.amount_inc_vat
  from public._pay_reserved_components(
    (select input_scope.ts_ids from inp as input_scope),
    p_exclude_pay_batch_id
  ) as reserved_component_rows
),
component_truth_raw as (
  select
    family_to_projection.projected_timesheet_id as timesheet_id,
    upper(nullif(btrim(coalesce(finance_component_rows.component_key_type,'')), '')) as raw_key_type,
    nullif(btrim(coalesce(finance_component_rows.component_key_value,'')), '') as raw_key_value,
    case
      when jsonb_typeof(coalesce(finance_component_rows.source_basis_json, '{}'::jsonb)) = 'object'
        then coalesce(finance_component_rows.source_basis_json, '{}'::jsonb)
      else '{}'::jsonb
    end as source_basis_json,
    round(
      case
        when pay_advance_rows.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum
          then coalesce(finance_component_rows.remaining_source_amount,0) * -1
        when pay_advance_rows.case_type = 'UNDERPAYMENT'::public.pay_finance_case_type_enum
          then coalesce(finance_component_rows.remaining_source_amount,0)
        else 0
      end,
      2
    ) as signed_amount
  from family_to_projection
  join public.pay_finance_case_components as finance_component_rows
    on finance_component_rows.linked_timesheet_id = family_to_projection.family_timesheet_id
  join public.pay_advances as pay_advance_rows
    on pay_advance_rows.id = finance_component_rows.finance_case_id
  where finance_component_rows.linked_timesheet_id is not null
    and family_to_projection.projected_timesheet_id is not null
    and finance_component_rows.closed_at_utc is null
    and coalesce(finance_component_rows.remaining_source_amount,0) > 0
    and pay_advance_rows.case_type in (
      'OVERPAYMENT'::public.pay_finance_case_type_enum,
      'UNDERPAYMENT'::public.pay_finance_case_type_enum
    )
),
component_truth_prepared as (
  select
    component_truth_raw_rows.timesheet_id,
    component_truth_raw_rows.raw_key_type,
    component_truth_raw_rows.raw_key_value,
    component_truth_raw_rows.source_basis_json,
    component_truth_raw_rows.signed_amount,
    coalesce(
      upper(nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'item_type', '')), '')),
      case
        when component_truth_raw_rows.raw_key_type in ('TS_DAY','TS_TOTAL','CASE_TOTAL')
          then 'SEGMENT_DELTA'
        when nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'work_date','')), '') ~ '^\d{4}-\d{2}-\d{2}$'
          then 'SEGMENT_DELTA'
        when component_truth_raw_rows.raw_key_type = 'ADJUSTMENT_CODE'
          then 'ADJUSTMENT_DELTA'
        when nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'adjustment_id','')), '') is not null
          then 'ADJUSTMENT_DELTA'
        when component_truth_raw_rows.raw_key_type = 'EXPENSE_CODE'
         and upper(coalesce(component_truth_raw_rows.raw_key_value, '')) = 'MILEAGE'
          then 'MILEAGE_DELTA'
        when component_truth_raw_rows.raw_key_type in ('ADDITIONAL_CODE','EXPENSE_CODE')
          then 'EXPENSE_DELTA'
        when nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'additional_code','')), '') is not null
          then 'EXPENSE_DELTA'
        when nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'expense_code','')), '') is not null
          then 'EXPENSE_DELTA'
        else null::text
      end
    ) as resolver_item_type,
    nullif(btrim(coalesce(component_truth_raw_rows.source_basis_json->>'work_date','')), '') as source_work_date_text
  from component_truth_raw as component_truth_raw_rows
),
component_truth_keyed as (
  select
    component_truth_prepared_rows.timesheet_id,
    resolved_component_keys.key_type,
    resolved_component_keys.key_value,
    component_truth_prepared_rows.signed_amount
  from component_truth_prepared as component_truth_prepared_rows
  join lateral public._pay_policy_x_resolve_pre_draft_economic_key(
    p_timesheet_id => component_truth_prepared_rows.timesheet_id,
    p_live_source_json => component_truth_prepared_rows.source_basis_json || jsonb_build_object(
      'timesheet_id', component_truth_prepared_rows.timesheet_id::text,
      'item_type', component_truth_prepared_rows.resolver_item_type,
      'component_key_type', component_truth_prepared_rows.raw_key_type,
      'component_key_value', component_truth_prepared_rows.raw_key_value,
      'work_date', component_truth_prepared_rows.source_work_date_text,
      'live_truth_used', true
    ),
    p_item_type => component_truth_prepared_rows.resolver_item_type,
    p_key_type_hint => component_truth_prepared_rows.raw_key_type,
    p_key_value_hint => component_truth_prepared_rows.raw_key_value,
    p_work_date => case
      when component_truth_prepared_rows.source_work_date_text ~ '^\d{4}-\d{2}-\d{2}$'
        then component_truth_prepared_rows.source_work_date_text::date
      when component_truth_prepared_rows.raw_key_type = 'TS_DAY'
       and component_truth_prepared_rows.raw_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        then component_truth_prepared_rows.raw_key_value::date
      else null::date
    end
  ) as resolved_component_keys
    on true
  where resolved_component_keys.key_resolution_failure_reason is null
),
component_truth as (
  select
    component_truth_keyed_rows.timesheet_id,
    component_truth_keyed_rows.key_type,
    component_truth_keyed_rows.key_value,
    round(sum(component_truth_keyed_rows.signed_amount), 2) as truth_ex_vat,
    round(sum(component_truth_keyed_rows.signed_amount), 2) as truth_inc_vat
  from component_truth_keyed as component_truth_keyed_rows
  where component_truth_keyed_rows.key_type is not null
    and btrim(component_truth_keyed_rows.key_type) <> ''
    and component_truth_keyed_rows.key_value is not null
    and btrim(component_truth_keyed_rows.key_value) <> ''
  group by
    component_truth_keyed_rows.timesheet_id,
    component_truth_keyed_rows.key_type,
    component_truth_keyed_rows.key_value
),
component_keys as (
  select distinct
    component_truth_keys.timesheet_id,
    component_truth_keys.key_type,
    component_truth_keys.key_value
  from component_truth as component_truth_keys
),
component_joined as (
  select
    component_keys.timesheet_id,
    component_keys.key_type,
    component_keys.key_value,
    coalesce(component_truth.truth_ex_vat,0) as truth_ex_vat,
    0::numeric as baseline_ex_vat,
    coalesce(reserved_components.amount_ex_vat,0) as reserved_ex_vat,
    coalesce(component_truth.truth_inc_vat,0) as truth_inc_vat,
    0::numeric as baseline_inc_vat,
    coalesce(reserved_components.amount_inc_vat,0) as reserved_inc_vat
  from component_keys
  left join component_truth
    on component_truth.timesheet_id = component_keys.timesheet_id
   and component_truth.key_type = component_keys.key_type
   and component_truth.key_value = component_keys.key_value
  left join reserved_components
    on reserved_components.timesheet_id = component_keys.timesheet_id
   and reserved_components.key_type = component_keys.key_type
   and reserved_components.key_value = component_keys.key_value
),
all_legacy_keys as (
  select distinct
    legacy_key_rows.timesheet_id,
    legacy_key_rows.key_type,
    legacy_key_rows.key_value
  from (
    select
      timesheet_entitlement_components.timesheet_id,
      timesheet_entitlement_components.key_type,
      timesheet_entitlement_components.key_value
    from timesheet_entitlement_components
    union all
    select
      reserved_components.timesheet_id,
      reserved_components.key_type,
      reserved_components.key_value
    from reserved_components
  ) as legacy_key_rows
),
legacy_joined as (
  select
    all_legacy_keys.timesheet_id,
    all_legacy_keys.key_type,
    all_legacy_keys.key_value,
    coalesce(timesheet_entitlement_components.truth_ex_vat,0) as truth_ex_vat,
    coalesce(timesheet_entitlement_components.baseline_ex_vat,0) as baseline_ex_vat,
    coalesce(reserved_components.amount_ex_vat,0) as reserved_ex_vat,
    coalesce(timesheet_entitlement_components.truth_inc_vat,0) as truth_inc_vat,
    coalesce(timesheet_entitlement_components.baseline_inc_vat,0) as baseline_inc_vat,
    coalesce(reserved_components.amount_inc_vat,0) as reserved_inc_vat
  from all_legacy_keys
  left join timesheet_entitlement_components
    on timesheet_entitlement_components.timesheet_id = all_legacy_keys.timesheet_id
   and timesheet_entitlement_components.key_type = all_legacy_keys.key_type
   and timesheet_entitlement_components.key_value = all_legacy_keys.key_value
  left join reserved_components
    on reserved_components.timesheet_id = all_legacy_keys.timesheet_id
   and reserved_components.key_type = all_legacy_keys.key_type
   and reserved_components.key_value = all_legacy_keys.key_value
),
final_rows as (
  select
    component_joined.timesheet_id,
    component_joined.key_type,
    component_joined.key_value,
    component_joined.truth_ex_vat,
    component_joined.baseline_ex_vat,
    component_joined.reserved_ex_vat,
    component_joined.truth_inc_vat,
    component_joined.baseline_inc_vat,
    component_joined.reserved_inc_vat
  from component_joined

  union all

  select
    legacy_joined.timesheet_id,
    legacy_joined.key_type,
    legacy_joined.key_value,
    legacy_joined.truth_ex_vat,
    legacy_joined.baseline_ex_vat,
    legacy_joined.reserved_ex_vat,
    legacy_joined.truth_inc_vat,
    legacy_joined.baseline_inc_vat,
    legacy_joined.reserved_inc_vat
  from legacy_joined
  where not exists (
    select 1
    from component_truth
    where component_truth.timesheet_id = legacy_joined.timesheet_id
      and component_truth.key_type = legacy_joined.key_type
      and component_truth.key_value = legacy_joined.key_value
  )
)
select
  final_rows.timesheet_id,
  final_rows.key_type,
  final_rows.key_value,
  round(final_rows.truth_ex_vat,2) as truth_ex_vat,
  round(final_rows.baseline_ex_vat,2) as baseline_ex_vat,
  round(final_rows.reserved_ex_vat,2) as reserved_ex_vat,
  round(final_rows.truth_ex_vat - final_rows.baseline_ex_vat - final_rows.reserved_ex_vat,2) as outstanding_ex_vat,
  round(final_rows.truth_inc_vat,2) as truth_inc_vat,
  round(final_rows.baseline_inc_vat,2) as baseline_inc_vat,
  round(final_rows.reserved_inc_vat,2) as reserved_inc_vat,
  round(final_rows.truth_inc_vat - final_rows.baseline_inc_vat - final_rows.reserved_inc_vat,2) as outstanding_inc_vat,
  (
    round(abs(final_rows.reserved_ex_vat),2) >
    round(abs(greatest(final_rows.truth_ex_vat - final_rows.baseline_ex_vat, 0)),2)
  ) as reservation_overrun_detected
from final_rows
where final_rows.timesheet_id is not null
  and final_rows.key_type is not null
  and final_rows.key_value is not null;
$function$;

-- _pay_outstanding_components(uuid[])
CREATE OR REPLACE FUNCTION public._pay_outstanding_components(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, truth_ex_vat numeric, baseline_ex_vat numeric, reserved_ex_vat numeric, outstanding_ex_vat numeric, truth_inc_vat numeric, baseline_inc_vat numeric, reserved_inc_vat numeric, outstanding_inc_vat numeric, reservation_overrun_detected boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
SELECT
  outstanding_component_rows.timesheet_id,
  outstanding_component_rows.key_type,
  outstanding_component_rows.key_value,
  outstanding_component_rows.truth_ex_vat,
  outstanding_component_rows.baseline_ex_vat,
  outstanding_component_rows.reserved_ex_vat,
  outstanding_component_rows.outstanding_ex_vat,
  outstanding_component_rows.truth_inc_vat,
  outstanding_component_rows.baseline_inc_vat,
  outstanding_component_rows.reserved_inc_vat,
  outstanding_component_rows.outstanding_inc_vat,
  outstanding_component_rows.reservation_overrun_detected
FROM public._pay_outstanding_components(p_timesheet_ids, NULL::uuid) AS outstanding_component_rows;
$function$;

-- _pay_payment_correction_apply_accepted_finance_resolution(uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public._pay_payment_correction_apply_accepted_finance_resolution(p_correction_request_id uuid, p_work_item_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_request public.pay_payment_correction_requests%rowtype;
  v_work_item public.pay_payment_correction_work_items%rowtype;
  v_batch public.pay_batches%rowtype;
  v_now timestamptz := now();
  v_effective_actor_user_id uuid := NULL::uuid;
  v_actor_kind text := 'SYSTEM';

  v_resolution_root jsonb := NULL::jsonb;
  v_resolution_body jsonb := NULL::jsonb;
  v_resolution_cases jsonb := '[]'::jsonb;

  v_sensitive_case_count integer := 0;
  v_case_record record;
  v_component_record record;
  v_accepted_case_json jsonb := NULL::jsonb;
  v_accepted_component_ids_json jsonb := '[]'::jsonb;
  v_accepted_fingerprints_json jsonb := '{}'::jsonb;
  v_accepted_surface text := NULL::text;
  v_accepted_effective_pay_date_text text := NULL::text;
  v_accepted_effective_pay_date date := NULL::date;
  v_resolution_path text := NULL::text;
  v_schedule_input_mode text := NULL::text;
  v_weeks_total integer := NULL::integer;
  v_weekly_due numeric := NULL::numeric;
  v_manual_total_remaining numeric := NULL::numeric;
  v_note text := NULL::text;
  v_regeneration_note text := NULL::text;
  v_component_resolutions jsonb := '[]'::jsonb;

  v_case_row public.pay_advances%rowtype;
  v_expected_fingerprint text := NULL::text;
  v_current_fingerprint text := NULL::text;
  v_regenerated_suggestion jsonb := NULL::jsonb;
  v_regenerated_suggestion_hash text := NULL::text;
  v_accepted_suggestion_hash text := NULL::text;
  v_plan_case_json jsonb := NULL::jsonb;
  v_plan_suggestion_hash text := NULL::text;
  v_plan_effective_pay_date_text text := NULL::text;
  v_accepted_hash_text text := NULL::text;
  v_accepted_hash_basis jsonb := '{}'::jsonb;
  v_accepted_basis_taxable_result jsonb := NULL::jsonb;
  v_regenerated_taxable_result jsonb := NULL::jsonb;
  v_apply_result jsonb := NULL::jsonb;
  v_apply_results jsonb := '[]'::jsonb;
  v_blocker jsonb := NULL::jsonb;
  v_open_overlap_count integer := 0;
  v_selected_component_count integer := 0;
  v_missing_component_count integer := 0;
  v_missing_fingerprint_count integer := 0;
  v_fingerprint_mismatch_count integer := 0;
  v_stale_component_count integer := 0;
  v_closed_unrecoverable_component_count integer := 0;
  v_total_selected_item_count integer := 0;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED')::text;
  END IF;

  IF p_work_item_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_WORK_ITEM_ID_REQUIRED')::text;
  END IF;

  SELECT public.pay_payment_correction_requests.*
  INTO v_request
  FROM public.pay_payment_correction_requests
  WHERE public.pay_payment_correction_requests.id = p_correction_request_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
              'correction_request_id', p_correction_request_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_work_items.*
  INTO v_work_item
  FROM public.pay_payment_correction_work_items
  WHERE public.pay_payment_correction_work_items.id = p_work_item_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_NOT_FOUND',
              'work_item_id', p_work_item_id
            )::text;
  END IF;

  IF v_work_item.correction_request_id IS DISTINCT FROM v_request.id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_REQUEST_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_REQUEST_MISMATCH',
              'correction_request_id', v_request.id,
              'work_item_id', v_work_item.id,
              'work_item_correction_request_id', v_work_item.correction_request_id
            )::text;
  END IF;

  IF v_work_item.pay_batch_id IS DISTINCT FROM v_request.pay_batch_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_WORK_ITEM_BATCH_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_WORK_ITEM_BATCH_MISMATCH',
              'correction_request_id', v_request.id,
              'work_item_id', v_work_item.id,
              'request_pay_batch_id', v_request.pay_batch_id,
              'work_item_pay_batch_id', v_work_item.pay_batch_id
            )::text;
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = v_request.pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', v_request.pay_batch_id
            )::text;
  END IF;

  SELECT public.pay_payment_correction_actions.actor_user_id
  INTO v_effective_actor_user_id
  FROM public.pay_payment_correction_actions
  WHERE public.pay_payment_correction_actions.correction_request_id = v_request.id
    AND public.pay_payment_correction_actions.action IN ('AUTHORISE', 'USE_GOLDEN_KEY')
    AND public.pay_payment_correction_actions.actor_user_id IS NOT NULL
  ORDER BY public.pay_payment_correction_actions.action_at_utc DESC, public.pay_payment_correction_actions.id DESC
  LIMIT 1;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_effective_actor_user_id, v_request.requested_by_user_id);
  v_actor_kind := CASE WHEN v_effective_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END;

  DROP TABLE IF EXISTS pg_temp._tmp_pcafr_selected_items;
  CREATE TEMP TABLE _tmp_pcafr_selected_items ON COMMIT DROP AS
  SELECT selected_items.*
  FROM public._pay_payment_correction_selected_items(
    v_request.pay_batch_id,
    v_work_item.selection_json,
    true
  ) AS selected_items;

  SELECT count(*)::integer
  INTO v_total_selected_item_count
  FROM pg_temp._tmp_pcafr_selected_items AS selected_count;

  DROP TABLE IF EXISTS pg_temp._tmp_pcafr_sensitive_cases;
  CREATE TEMP TABLE _tmp_pcafr_sensitive_cases ON COMMIT DROP AS
  SELECT
    selected_items.finance_case_id AS finance_case_id,
    COALESCE(pay_finance_case_components.candidate_id, selected_items.candidate_id) AS candidate_id,
    COALESCE(
      jsonb_agg(DISTINCT selected_items.finance_component_id ORDER BY selected_items.finance_component_id)
        FILTER (WHERE selected_items.finance_component_id IS NOT NULL),
      '[]'::jsonb
    ) AS selected_component_ids
  FROM pg_temp._tmp_pcafr_selected_items AS selected_items
  JOIN public.pay_batch_items AS pay_batch_items
    ON pay_batch_items.id = selected_items.pay_batch_item_id
  LEFT JOIN public.pay_finance_case_components AS pay_finance_case_components
    ON pay_finance_case_components.id = selected_items.finance_component_id
  LEFT JOIN public.pay_advances AS pay_advances
    ON pay_advances.id = selected_items.finance_case_id
  WHERE selected_items.finance_case_id IS NOT NULL
    AND selected_items.finance_component_id IS NOT NULL
    AND (
      COALESCE(pay_finance_case_components.classification::text, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR COALESCE(pay_batch_items.frozen_component_classification::text, '') = 'TAXABLE_CHANNEL_SENSITIVE'
      OR (
        COALESCE(pay_advances.taxability::text, '') = 'TAXABLE'
        AND (
          pay_batch_items.frozen_resolution_mode IS NOT NULL
          OR pay_finance_case_components.saved_resolution_mode IS NOT NULL
        )
      )
    )
  GROUP BY selected_items.finance_case_id, COALESCE(pay_finance_case_components.candidate_id, selected_items.candidate_id);

  SELECT count(*)::integer
  INTO v_sensitive_case_count
  FROM pg_temp._tmp_pcafr_sensitive_cases AS sensitive_case_count;

  IF v_sensitive_case_count = 0 THEN
    v_result := jsonb_build_object(
      'ok', true,
      'applied', false,
      'reason', 'NO_CHANNEL_SENSITIVE_FINANCE',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text,
      'selected_item_count', v_total_selected_item_count,
      'processed_at_utc', v_now,
      'processing_actor_kind', v_actor_kind,
      'actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END
    );

    UPDATE public.pay_payment_correction_work_items AS no_sensitive_work_item
    SET result_json = COALESCE(no_sensitive_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'accepted_finance_resolution', v_result
        )
    WHERE no_sensitive_work_item.id = v_work_item.id;

    RETURN v_result;
  END IF;

  IF v_request.accepted_resolution_json IS NULL
     OR COALESCE(jsonb_typeof(v_request.accepted_resolution_json), 'null') <> 'object' THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_REQUIRED',
      'message', 'Selected gross/taxable/channel-sensitive finance items require accepted_resolution_json before correction apply.',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text,
      'sensitive_finance_case_count', v_sensitive_case_count
    );

    UPDATE public.pay_payment_correction_work_items AS accepted_missing_work_item
    SET status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(accepted_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
          'processed_at_utc', v_now
        )
    WHERE accepted_missing_work_item.id = v_work_item.id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  v_resolution_root := v_request.accepted_resolution_json;

  v_resolution_body := CASE
    WHEN v_resolution_root ? 'suggested_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'suggested_resolution'), 'null') = 'object'
      THEN v_resolution_root->'suggested_resolution'
    WHEN v_resolution_root ? 'accepted_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'accepted_resolution'), 'null') = 'object'
      THEN v_resolution_root->'accepted_resolution'
    ELSE v_resolution_root
  END;

  v_resolution_cases := COALESCE(v_resolution_body->'finance_cases', v_resolution_root->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_resolution_cases), 'null') <> 'array' THEN
    v_blocker := jsonb_build_object(
      'code', 'ACCEPTED_RESOLUTION_FINANCE_CASES_INVALID',
      'message', 'accepted_resolution_json.finance_cases must be an array.',
      'correction_request_id', v_request.id::text,
      'work_item_id', v_work_item.id::text
    );

    UPDATE public.pay_payment_correction_work_items AS accepted_cases_invalid_work_item
    SET status = 'BLOCKED',
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_at_utc = v_now,
        last_error = v_blocker->>'message',
        result_json = COALESCE(accepted_cases_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', v_blocker,
          'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
          'processed_at_utc', v_now
        )
    WHERE accepted_cases_invalid_work_item.id = v_work_item.id;

    RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
  END IF;

  FOR v_case_record IN
    SELECT sensitive_cases.*
    FROM pg_temp._tmp_pcafr_sensitive_cases AS sensitive_cases
    ORDER BY sensitive_cases.finance_case_id
  LOOP
    SELECT finance_case_rows.*
    INTO v_case_row
    FROM public.pay_advances AS finance_case_rows
    WHERE finance_case_rows.id = v_case_record.finance_case_id
    FOR UPDATE;

    IF NOT FOUND THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_NOT_FOUND',
        'message', 'Selected finance case no longer exists.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS finance_case_missing_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(finance_case_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE finance_case_missing_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT accepted_case.value
    INTO v_accepted_case_json
    FROM jsonb_array_elements(v_resolution_cases) AS accepted_case(value)
    WHERE NULLIF(btrim(COALESCE(accepted_case.value->>'finance_case_id', '')), '') = v_case_record.finance_case_id::text
    LIMIT 1;

    IF v_accepted_case_json IS NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_CASE_MISSING',
        'message', 'accepted_resolution_json does not include the selected gross/channel-sensitive finance case.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_case_missing_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_case_missing_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_case_missing_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '') IS NOT NULL
       AND NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '') <> v_case_record.candidate_id::text THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_CANDIDATE_MISMATCH',
        'message', 'accepted_resolution_json candidate_id does not match the selected finance case candidate.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'accepted_candidate_id', v_accepted_case_json->>'candidate_id',
        'selected_candidate_id', v_case_record.candidate_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_candidate_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_candidate_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_candidate_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT plan_case.value
    INTO v_plan_case_json
    FROM jsonb_array_elements(
      CASE
        WHEN COALESCE(jsonb_typeof(v_request.plan_json#>'{suggested_resolution,finance_cases}'), 'null') = 'array'
          THEN v_request.plan_json#>'{suggested_resolution,finance_cases}'
        ELSE '[]'::jsonb
      END
    ) AS plan_case(value)
    WHERE NULLIF(btrim(COALESCE(plan_case.value->>'finance_case_id', '')), '') = v_case_record.finance_case_id::text
    LIMIT 1;

    v_plan_suggestion_hash := NULLIF(btrim(COALESCE(v_plan_case_json->>'suggestion_hash', '')), '');
    v_plan_effective_pay_date_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'effective_pay_date', '')), '');

    v_accepted_component_ids_json := COALESCE(
      v_accepted_case_json->'component_ids',
      v_accepted_case_json->'selected_component_ids',
      '[]'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_accepted_component_ids_json), 'null') <> 'array' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_IDS_INVALID',
        'message', 'accepted_resolution_json finance case component_ids must be an array.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS accepted_components_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(accepted_components_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE accepted_components_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT count(*)::integer
    INTO v_selected_component_count
    FROM public.pay_finance_case_components AS selected_component_count
    WHERE selected_component_count.finance_case_id = v_case_record.finance_case_id
      AND selected_component_count.id IN (
        SELECT (selected_component_ids.value)::uuid
        FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
        WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      );

    SELECT count(*)::integer
    INTO v_missing_component_count
    FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
    WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND NOT EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
        WHERE accepted_component_ids.value = selected_component_ids.value
      );

    IF v_selected_component_count = 0 OR v_missing_component_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
        'message', 'accepted_resolution_json does not cover all selected gross/channel-sensitive finance components.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'missing_component_count', v_missing_component_count,
        'selected_component_count', v_selected_component_count
      );

      UPDATE public.pay_payment_correction_work_items AS component_scope_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(component_scope_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE component_scope_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_fingerprints_json := COALESCE(
      v_accepted_case_json->'current_component_fingerprints',
      v_accepted_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_accepted_fingerprints_json), 'null') <> 'object' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_COMPONENT_FINGERPRINTS_INVALID',
        'message', 'accepted_resolution_json current_component_fingerprints must be an object.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS fingerprints_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(fingerprints_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE fingerprints_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_missing_fingerprint_count := 0;
    v_fingerprint_mismatch_count := 0;
    v_stale_component_count := 0;
    v_closed_unrecoverable_component_count := 0;

    FOR v_component_record IN
      SELECT public.pay_finance_case_components.*
      FROM public.pay_finance_case_components
      WHERE public.pay_finance_case_components.finance_case_id = v_case_record.finance_case_id
        AND public.pay_finance_case_components.id IN (
          SELECT (selected_component_ids.value)::uuid
          FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
          WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
      ORDER BY public.pay_finance_case_components.id
      FOR UPDATE
    LOOP
      v_expected_fingerprint := NULLIF(btrim(COALESCE(v_accepted_fingerprints_json->>v_component_record.id::text, '')), '');
      v_current_fingerprint := COALESCE(
        NULLIF(btrim(v_component_record.resolution_fingerprint), ''),
        md5(jsonb_build_object(
          'finance_component_id', v_component_record.id,
          'finance_case_id', v_component_record.finance_case_id,
          'classification', v_component_record.classification::text,
          'source_pay_method', v_component_record.source_pay_method,
          'source_amount', v_component_record.source_amount,
          'remaining_source_amount', v_component_record.remaining_source_amount,
          'saved_target_pay_method', v_component_record.saved_target_pay_method,
          'saved_resolution_mode', v_component_record.saved_resolution_mode::text,
          'saved_resolution_payload_json', v_component_record.saved_resolution_payload_json,
          'saved_resolution_result_json', v_component_record.saved_resolution_result_json,
          'is_resolution_stale', v_component_record.is_resolution_stale,
          'closed_at_utc', v_component_record.closed_at_utc,
          'updated_at_utc', v_component_record.updated_at_utc
        )::text)
      );

      IF v_expected_fingerprint IS NULL THEN
        v_missing_fingerprint_count := v_missing_fingerprint_count + 1;
      ELSIF v_expected_fingerprint <> v_current_fingerprint THEN
        v_fingerprint_mismatch_count := v_fingerprint_mismatch_count + 1;
      END IF;

      IF COALESCE(v_component_record.is_resolution_stale, false) THEN
        v_stale_component_count := v_stale_component_count + 1;
      END IF;

      IF v_component_record.closed_at_utc IS NOT NULL
         AND round(COALESCE(v_component_record.remaining_source_amount, 0), 2) <= 0 THEN
        v_closed_unrecoverable_component_count := v_closed_unrecoverable_component_count + 1;
      END IF;
    END LOOP;

    IF v_missing_fingerprint_count > 0 OR v_fingerprint_mismatch_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_STALE',
        'message', 'Accepted finance resolution is stale because one or more component fingerprints no longer match.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'missing_fingerprint_count', v_missing_fingerprint_count,
        'fingerprint_mismatch_count', v_fingerprint_mismatch_count
      );

      UPDATE public.pay_payment_correction_work_items AS stale_fingerprint_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(stale_fingerprint_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE stale_fingerprint_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_stale_component_count > 0 OR v_closed_unrecoverable_component_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_MANUAL_REVIEW_REQUIRED',
        'message', 'Selected finance components are stale or closed in a way that requires manual review before correction apply.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'stale_component_count', v_stale_component_count,
        'closed_unrecoverable_component_count', v_closed_unrecoverable_component_count
      );

      UPDATE public.pay_payment_correction_work_items AS manual_review_component_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(manual_review_component_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE manual_review_component_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_case_row.status <> 'ACTIVE'::public.pay_advance_status_enum
       OR v_case_row.written_off_at_utc IS NOT NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'FINANCE_CASE_NOT_ACTIVE_OR_WRITTEN_OFF',
        'message', 'Selected finance case is no longer active or has been written off.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'status', v_case_row.status::text,
        'written_off_at_utc', v_case_row.written_off_at_utc
      );

      UPDATE public.pay_payment_correction_work_items AS inactive_case_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(inactive_case_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE inactive_case_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    SELECT count(*)::integer
    INTO v_open_overlap_count
    FROM public.pay_batch_items AS overlapping_items
    JOIN public.pay_batch_candidates AS overlapping_candidates
      ON overlapping_candidates.id = overlapping_items.pay_batch_candidate_id
    JOIN public.pay_batches AS overlapping_batches
      ON overlapping_batches.id = overlapping_candidates.pay_batch_id
    WHERE overlapping_candidates.pay_batch_id <> v_request.pay_batch_id
      AND COALESCE(overlapping_items.is_voided, false) = false
      AND overlapping_batches.cancelled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(overlapping_batches.status)
      AND (
        overlapping_items.finance_case_id = v_case_record.finance_case_id
        OR overlapping_items.finance_component_id IN (
          SELECT (selected_component_ids.value)::uuid
          FROM jsonb_array_elements_text(v_case_record.selected_component_ids) AS selected_component_ids(value)
          WHERE selected_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        OR overlapping_items.reservation_id IN (
          SELECT selected_items.reservation_id
          FROM pg_temp._tmp_pcafr_selected_items AS selected_items
          WHERE selected_items.finance_case_id = v_case_record.finance_case_id
            AND selected_items.reservation_id IS NOT NULL
        )
      );

    IF v_open_overlap_count > 0 THEN
      v_blocker := jsonb_build_object(
        'code', 'DRAFT_BATCH_INTERFERENCE',
        'message', 'An open overlapping draft/reserved batch already references the selected finance case/component/reservation. Delete or cancel the overlapping draft first.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'overlap_count', v_open_overlap_count
      );

      UPDATE public.pay_payment_correction_work_items AS overlap_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(overlap_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE overlap_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_surface := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_resolution_body->>'apply_surface'), ''),
      'pay_finance_case_apply_taxable_channel_restructure'
    );

    IF v_accepted_surface NOT IN (
      'pay_finance_case_apply_taxable_channel_restructure',
      'pay_manual_debt_adjustment_resolve_taxable_channel_change',
      'pay_finance_component_resolutions_apply'
    ) THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_UNSUPPORTED',
        'message', 'accepted_resolution_json apply_surface is not supported by the payment correction finance helper.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'apply_surface', v_accepted_surface
      );

      UPDATE public.pay_payment_correction_work_items AS unsupported_surface_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(unsupported_surface_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE unsupported_surface_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_effective_pay_date_text := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'effective_pay_date'), ''),
      v_plan_effective_pay_date_text,
      NULLIF(btrim(v_resolution_body->>'effective_pay_date'), ''),
      COALESCE(v_batch.authoritative_payment_date, v_batch.pay_date)::text
    );

    IF v_accepted_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_INVALID',
        'message', 'accepted_resolution_json effective_pay_date must be YYYY-MM-DD.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'effective_pay_date', v_accepted_effective_pay_date_text
      );

      UPDATE public.pay_payment_correction_work_items AS effective_date_invalid_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(effective_date_invalid_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE effective_date_invalid_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF v_plan_effective_pay_date_text IS NOT NULL
       AND v_plan_effective_pay_date_text ~ '^\d{4}-\d{2}-\d{2}$'
       AND v_accepted_effective_pay_date_text <> v_plan_effective_pay_date_text THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_MISMATCH',
        'message', 'accepted_resolution_json effective_pay_date does not match the payment correction plan effective_pay_date.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'accepted_effective_pay_date', v_accepted_effective_pay_date_text,
        'planned_effective_pay_date', v_plan_effective_pay_date_text
      );

      UPDATE public.pay_payment_correction_work_items AS effective_date_mismatch_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(effective_date_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE effective_date_mismatch_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_effective_pay_date := v_accepted_effective_pay_date_text::date;
    v_resolution_path := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'resolution_path'), ''),
      NULLIF(btrim(v_resolution_body->>'resolution_path'), ''),
      NULLIF(btrim(v_accepted_case_json#>>'{suggestion,resolution_path}'), ''),
      'SUGGESTED'
    );
    v_schedule_input_mode := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_resolution_body->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_accepted_case_json#>>'{suggestion,schedule_input_mode}'), '')
    );

    v_weeks_total := CASE
      WHEN COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}') ~ '^-?\d+$'
        THEN COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}')::integer
      ELSE NULL::integer
    END;

    v_weekly_due := CASE
      WHEN COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}') ~ '^-?\d+(\.\d+)?$'
        THEN COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}')::numeric
      ELSE NULL::numeric
    END;

    v_manual_total_remaining := CASE
      WHEN COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining') ~ '^-?\d+(\.\d+)?$'
        THEN COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining')::numeric
      ELSE NULL::numeric
    END;

    v_note := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'note'), ''),
      NULLIF(btrim(v_resolution_body->>'note'), ''),
      'Applied accepted taxable channel finance resolution for payment correction request ' || v_request.id::text || ', work item ' || v_work_item.id::text
    );

    v_regeneration_note := CASE
      WHEN UPPER(COALESCE(v_resolution_path, 'SUGGESTED')) = 'MANUAL' THEN COALESCE(
        NULLIF(btrim(v_accepted_case_json->>'note'), ''),
        NULLIF(btrim(v_resolution_body->>'note'), '')
      )
      ELSE COALESCE(
        NULLIF(btrim(v_accepted_case_json->>'note'), ''),
        NULLIF(btrim(v_accepted_case_json#>>'{suggestion,note}'), ''),
        NULLIF(btrim(v_accepted_case_json#>>'{suggestion,request,note}'), ''),
        NULLIF(btrim(v_plan_case_json->>'note'), ''),
        NULLIF(btrim(v_plan_case_json#>>'{suggestion,note}'), ''),
        NULLIF(btrim(v_plan_case_json#>>'{suggestion,request,note}'), ''),
        'Generated for payment correction plan ' || v_request.pay_batch_id::text
      )
    END;

    IF v_effective_actor_user_id IS NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'ACTOR_USER_ID_REQUIRED_FOR_ACCEPTED_FINANCE_RESOLUTION',
        'message', 'Applying accepted gross/channel-sensitive finance resolution requires a user actor.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS actor_required_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(actor_required_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE actor_required_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    v_accepted_suggestion_hash := NULLIF(btrim(COALESCE(v_accepted_case_json->>'suggestion_hash', '')), '');
    v_accepted_hash_text := COALESCE(NULLIF(btrim(v_accepted_case_json#>>'{suggestion_hash_basis,hash_text}'), ''), NULLIF(btrim(v_resolution_body#>>'{suggestion_hash_basis,hash_text}'), ''));
    v_accepted_hash_basis := COALESCE(v_accepted_case_json->'suggestion_hash_basis', v_resolution_body->'suggestion_hash_basis', '{}'::jsonb);

    IF COALESCE(jsonb_typeof(v_accepted_hash_basis), 'null') <> 'object' THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_INVALID',
        'message', 'accepted_resolution_json suggestion_hash_basis must be an object.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS invalid_hash_basis_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(invalid_hash_basis_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE invalid_hash_basis_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    IF UPPER(COALESCE(v_resolution_path, 'SUGGESTED')) = 'MANUAL' AND v_accepted_hash_text IS NULL THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_TEXT_MISSING',
        'message', 'accepted_resolution_json manual resolution must include backend-generated suggestion_hash_basis.hash_text.',
        'finance_case_id', v_case_record.finance_case_id::text
      );

      UPDATE public.pay_payment_correction_work_items AS missing_hash_basis_text_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(missing_hash_basis_text_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE missing_hash_basis_text_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END IF;

    BEGIN
      v_regenerated_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
        p_finance_case_id => v_case_record.finance_case_id,
        p_actor_user_id => v_effective_actor_user_id,
        p_effective_pay_date => v_accepted_effective_pay_date,
        p_resolution_path => v_resolution_path,
        p_schedule_input_mode => v_schedule_input_mode,
        p_weeks_total => v_weeks_total,
        p_weekly_due => v_weekly_due,
        p_manual_total_remaining => v_manual_total_remaining,
        p_note => v_regeneration_note
      );

      v_regenerated_taxable_result := COALESCE(
        v_regenerated_suggestion->'taxable_channel_result',
        v_regenerated_suggestion->'result',
        v_regenerated_suggestion->'suggestion',
        v_regenerated_suggestion
      ) - 'generated_at'
        - 'generated_at_utc'
        - 'created_at'
        - 'created_at_utc'
        - 'updated_at'
        - 'updated_at_utc'
        - 'audit'
        - 'debug';

      IF UPPER(COALESCE(v_resolution_path, 'SUGGESTED')) = 'MANUAL' THEN
        v_accepted_basis_taxable_result := v_accepted_hash_basis->'taxable_channel_result';

        IF COALESCE(v_accepted_hash_basis->>'hash_version', '') <> 'payment_correction_finance_resolution_v1'
           OR COALESCE(v_accepted_hash_basis->>'finance_case_id', '') <> v_case_record.finance_case_id::text
           OR COALESCE(v_accepted_hash_basis->>'candidate_id', '') <> v_case_row.candidate_id::text
           OR COALESCE(v_accepted_hash_basis->>'apply_surface', '') <> v_accepted_surface
           OR COALESCE(v_accepted_hash_basis->>'effective_pay_date', '') <> v_accepted_effective_pay_date_text
           OR UPPER(COALESCE(v_accepted_hash_basis->>'resolution_path', '')) <> 'MANUAL'
           OR COALESCE(v_accepted_hash_basis->>'schedule_input_mode', '') <> COALESCE(v_schedule_input_mode, '')
           OR COALESCE(v_accepted_hash_basis->>'weeks_total', '') <> COALESCE(v_weeks_total::text, '')
           OR COALESCE(v_accepted_hash_basis->>'weekly_due', '') <> COALESCE(v_weekly_due::text, '')
           OR COALESCE(v_accepted_hash_basis->>'manual_total_remaining', '') <> COALESCE(v_manual_total_remaining::text, '')
           OR COALESCE(v_accepted_hash_basis->'selected_component_ids', '[]'::jsonb) IS DISTINCT FROM COALESCE(v_accepted_component_ids_json, '[]'::jsonb)
           OR COALESCE(v_accepted_hash_basis->'current_component_fingerprints', v_accepted_hash_basis->'component_fingerprints', '{}'::jsonb) IS DISTINCT FROM COALESCE(v_accepted_fingerprints_json, '{}'::jsonb)
           OR v_accepted_basis_taxable_result IS NULL
           OR v_accepted_basis_taxable_result IS DISTINCT FROM v_regenerated_taxable_result THEN
          v_blocker := jsonb_build_object(
            'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_MISMATCH',
            'message', 'accepted_resolution_json manual suggestion_hash_basis does not match the regenerated backend suggestion and selected correction scope.',
            'finance_case_id', v_case_record.finance_case_id::text
          );

          UPDATE public.pay_payment_correction_work_items AS hash_basis_mismatch_work_item
          SET status = 'BLOCKED',
              locked_at_utc = NULL,
              locked_by = NULL,
              processed_at_utc = v_now,
              last_error = v_blocker->>'message',
              result_json = COALESCE(hash_basis_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
                'ok', false,
                'status', 'BLOCKED',
                'blocker', v_blocker,
                'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
                'processed_at_utc', v_now
              )
          WHERE hash_basis_mismatch_work_item.id = v_work_item.id;

          RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
        END IF;

        v_regenerated_suggestion_hash := md5(v_accepted_hash_text);
      ELSE
        v_regenerated_suggestion_hash := md5(jsonb_build_object(
          'finance_case_id', v_case_record.finance_case_id,
          'candidate_id', v_case_row.candidate_id,
          'component_ids', v_case_record.selected_component_ids,
          'selected_component_ids', v_case_record.selected_component_ids,
          'component_fingerprints', v_accepted_fingerprints_json,
          'effective_pay_date', v_accepted_effective_pay_date,
          'apply_surface', v_accepted_surface,
          'resolution_path', COALESCE(v_regenerated_suggestion->>'resolution_path', v_regenerated_suggestion#>>'{request,resolution_path}', v_resolution_path, 'SUGGESTED'),
          'resolution_mode', COALESCE(v_regenerated_suggestion->>'resolution_mode', v_regenerated_suggestion#>>'{result,resolution_mode}', v_regenerated_suggestion#>>'{suggestion,resolution_mode}'),
          'weeks_total', COALESCE(v_regenerated_suggestion->>'weeks_total', v_regenerated_suggestion#>>'{result,weeks_total}', v_regenerated_suggestion#>>'{suggestion,weeks_total}'),
          'weekly_due', COALESCE(v_regenerated_suggestion->>'weekly_due', v_regenerated_suggestion#>>'{result,weekly_due}', v_regenerated_suggestion#>>'{suggestion,weekly_due}'),
          'manual_total_remaining', COALESCE(v_regenerated_suggestion->>'manual_total_remaining', v_regenerated_suggestion#>>'{result,manual_total_remaining}', v_regenerated_suggestion#>>'{suggestion,manual_total_remaining}'),
          'taxable_channel_result', v_regenerated_taxable_result
        )::text);
      END IF;

      IF v_accepted_suggestion_hash IS NULL THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_SUGGESTION_HASH_REQUIRED',
          'message', 'accepted_resolution_json must include suggestion_hash for every selected gross/channel-sensitive finance case.',
          'finance_case_id', v_case_record.finance_case_id::text
        );

        UPDATE public.pay_payment_correction_work_items AS missing_suggestion_hash_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(missing_suggestion_hash_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE missing_suggestion_hash_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;

      IF UPPER(COALESCE(v_resolution_path, 'SUGGESTED')) = 'SUGGESTED'
         AND v_plan_suggestion_hash IS NOT NULL
         AND v_accepted_suggestion_hash <> v_plan_suggestion_hash THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_PLAN_HASH_MISMATCH',
          'message', 'accepted_resolution_json suggestion_hash does not match the stored correction plan suggestion_hash.',
          'finance_case_id', v_case_record.finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'planned_suggestion_hash', v_plan_suggestion_hash
        );

        UPDATE public.pay_payment_correction_work_items AS plan_hash_mismatch_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(plan_hash_mismatch_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE plan_hash_mismatch_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;

      IF v_accepted_suggestion_hash <> v_regenerated_suggestion_hash THEN
        v_blocker := jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because the regenerated suggestion no longer matches the accepted suggestion hash.',
          'finance_case_id', v_case_record.finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'regenerated_suggestion_hash', v_regenerated_suggestion_hash
        );

        UPDATE public.pay_payment_correction_work_items AS stale_suggestion_work_item
        SET status = 'BLOCKED',
            locked_at_utc = NULL,
            locked_by = NULL,
            processed_at_utc = v_now,
            last_error = v_blocker->>'message',
            result_json = COALESCE(stale_suggestion_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
              'ok', false,
              'status', 'BLOCKED',
              'blocker', v_blocker,
              'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
              'processed_at_utc', v_now
            )
        WHERE stale_suggestion_work_item.id = v_work_item.id;

        RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_REGENERATION_FAILED',
        'message', 'Accepted finance resolution could not be regenerated using the existing suggested-resolution function.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      );

      UPDATE public.pay_payment_correction_work_items AS regeneration_failed_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(regeneration_failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE regeneration_failed_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END;

    BEGIN
      IF v_accepted_surface = 'pay_finance_case_apply_taxable_channel_restructure' THEN
        v_apply_result := public.pay_finance_case_apply_taxable_channel_restructure(
          p_finance_case_id => v_case_record.finance_case_id,
          p_actor_user_id => v_effective_actor_user_id,
          p_resolution_path => v_resolution_path,
          p_schedule_input_mode => v_schedule_input_mode,
          p_weeks_total => v_weeks_total,
          p_weekly_due => v_weekly_due,
          p_manual_total_remaining => v_manual_total_remaining,
          p_effective_pay_date => v_accepted_effective_pay_date,
          p_note => v_note
        );
      ELSIF v_accepted_surface = 'pay_manual_debt_adjustment_resolve_taxable_channel_change' THEN
        v_apply_result := public.pay_manual_debt_adjustment_resolve_taxable_channel_change(
          p_finance_case_id => v_case_record.finance_case_id,
          p_actor_user_id => v_effective_actor_user_id,
          p_resolution_path => v_resolution_path,
          p_schedule_input_mode => v_schedule_input_mode,
          p_weeks_total => v_weeks_total,
          p_weekly_due => v_weekly_due,
          p_manual_total_remaining => v_manual_total_remaining,
          p_effective_pay_date => v_accepted_effective_pay_date,
          p_note => v_note
        );
      ELSE
        v_component_resolutions := COALESCE(
          v_accepted_case_json->'component_resolutions',
          v_accepted_case_json#>'{suggestion,component_resolutions}',
          v_accepted_case_json#>'{suggestion,resolutions}',
          v_accepted_case_json#>'{suggestion,components}',
          '[]'::jsonb
        );

        IF COALESCE(jsonb_typeof(v_component_resolutions), 'null') <> 'array'
           OR jsonb_array_length(v_component_resolutions) = 0 THEN
          RAISE EXCEPTION 'COMPONENT_RESOLUTIONS_REQUIRED'
            USING DETAIL = jsonb_build_object(
              'code', 'COMPONENT_RESOLUTIONS_REQUIRED',
              'finance_case_id', v_case_record.finance_case_id::text,
              'apply_surface', v_accepted_surface
            )::text;
        END IF;

        v_apply_result := public.pay_finance_component_resolutions_apply(
          p_candidate_id => v_case_record.candidate_id,
          p_component_resolutions => v_component_resolutions,
          p_actor_user_id => v_effective_actor_user_id,
          p_finance_case_id => v_case_record.finance_case_id,
          p_reason => v_note
        );
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_blocker := jsonb_build_object(
        'code', 'ACCEPTED_FINANCE_RESOLUTION_APPLY_FAILED',
        'message', 'Accepted finance resolution failed to apply. The helper marked this work item blocked; caller must not mark the correction work item applied.',
        'finance_case_id', v_case_record.finance_case_id::text,
        'apply_surface', v_accepted_surface,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      );

      UPDATE public.pay_payment_correction_work_items AS apply_failed_work_item
      SET status = 'BLOCKED',
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_at_utc = v_now,
          last_error = v_blocker->>'message',
          result_json = COALESCE(apply_failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', 'BLOCKED',
            'blocker', v_blocker,
            'accepted_finance_resolution', jsonb_build_object('ok', false, 'blocker', v_blocker),
            'processed_at_utc', v_now
          )
      WHERE apply_failed_work_item.id = v_work_item.id;

      RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED', 'blocker', v_blocker);
    END;

    v_apply_results := v_apply_results || jsonb_build_array(jsonb_build_object(
      'finance_case_id', v_case_record.finance_case_id::text,
      'candidate_id', v_case_record.candidate_id::text,
      'selected_component_ids', v_case_record.selected_component_ids,
      'apply_surface', v_accepted_surface,
      'effective_pay_date', v_accepted_effective_pay_date::text,
      'regenerated_suggestion_hash', v_regenerated_suggestion_hash,
      'resolution_path', v_resolution_path,
      'accepted_suggestion_hash', v_accepted_suggestion_hash,
      'apply_result', v_apply_result
    ));
  END LOOP;

  v_result := jsonb_build_object(
    'ok', true,
    'applied', true,
    'correction_request_id', v_request.id::text,
    'work_item_id', v_work_item.id::text,
    'sensitive_finance_case_count', v_sensitive_case_count,
    'selected_item_count', v_total_selected_item_count,
    'apply_results', v_apply_results,
    'processed_at_utc', v_now,
    'processing_actor_kind', v_actor_kind,
    'actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END
  );

  UPDATE public.pay_payment_correction_work_items AS successful_work_item
  SET result_json = COALESCE(successful_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
        'accepted_finance_resolution', v_result
      )
  WHERE successful_work_item.id = v_work_item.id;

  RETURN v_result;
END;
$function$;

-- _pay_payment_correction_mail_scope_match(uuid,uuid,jsonb,jsonb,boolean)
CREATE OR REPLACE FUNCTION public._pay_payment_correction_mail_scope_match(p_mail_outbox_id uuid, p_pay_batch_id uuid, p_selection_json jsonb, p_selected_scope_json jsonb, p_allow_legacy_broad_match boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_mail_row public.mail_outbox%ROWTYPE;
  v_mail_scope jsonb := '{}'::jsonb;
  v_scope_type text := NULL::text;
  v_work_unit text := NULL::text;
  v_uuid_pattern text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

  v_selected_pay_batch_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_selected_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_selected_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_selected_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_selected_payout_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_group_keys text[] := ARRAY[]::text[];

  v_mail_pay_batch_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_mail_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_mail_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_mail_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_mail_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_mail_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_mail_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_mail_payout_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_mail_transfer_group_keys text[] := ARRAY[]::text[];

  v_mail_batch_matches boolean := false;
  v_is_whole_batch_scope boolean := false;
  v_selected_has_narrow_payment_scope boolean := false;
  v_selected_candidate_scope_complete boolean := false;
  v_selected_has_candidate_filter boolean := false;
  v_selected_has_umbrella_filter boolean := false;
  v_candidate_filter_requires_payment_match boolean := false;
  v_umbrella_filter_requires_payment_match boolean := false;
  v_mail_candidate_scope_matches boolean := false;
  v_mail_umbrella_scope_matches boolean := false;
  v_transfer_scope_candidate_safe boolean := false;
  v_transfer_scope_umbrella_safe boolean := false;
  v_transfer_scope_safe boolean := false;

  v_item_exact boolean := false;
  v_item_partial boolean := false;
  v_transfer_exact boolean := false;
  v_transfer_partial boolean := false;
  v_payout_transfer_exact boolean := false;
  v_payout_transfer_partial boolean := false;
  v_transfer_group_exact boolean := false;
  v_transfer_group_partial boolean := false;
  v_finance_case_exact boolean := false;
  v_finance_case_partial boolean := false;
  v_finance_component_exact boolean := false;
  v_finance_component_partial boolean := false;
  v_reservation_exact boolean := false;
  v_reservation_partial boolean := false;
  v_pay_batch_candidate_exact boolean := false;
  v_pay_batch_candidate_partial boolean := false;
  v_umbrella_group_exact boolean := false;
  v_umbrella_partial boolean := false;
  v_context_exact boolean := false;
  v_legacy_broad_match boolean := false;
  v_reference_broad_match boolean := false;
  v_recipient_broad_match boolean := false;
  v_payment_scope_broad_match boolean := false;
  v_partial_overlap_requires_review boolean := false;

  v_match_kind text := 'NONE';
  v_match_confidence text := 'NONE';
  v_safe_to_cancel boolean := false;
  v_requires_review boolean := false;
  v_reason text := 'NO_SCOPE_MATCH';
BEGIN
  IF p_mail_outbox_id IS NULL THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_OUTBOX_ID_REQUIRED'
    );
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'PAY_BATCH_ID_REQUIRED',
      'mail_outbox_id', p_mail_outbox_id
    );
  END IF;

  SELECT public.mail_outbox.*
  INTO v_mail_row
  FROM public.mail_outbox
  WHERE public.mail_outbox.id = p_mail_outbox_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_OUTBOX_NOT_FOUND',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id
    );
  END IF;

  v_mail_scope := COALESCE(v_mail_row.payment_scope_json, '{}'::jsonb);
  v_scope_type := upper(btrim(COALESCE(p_selected_scope_json->>'scope_type', p_selection_json->>'scope_type', '')));
  v_work_unit := upper(btrim(COALESCE(p_selected_scope_json->>'work_unit', p_selection_json->>'work_unit', '')));

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_id'), ('pay_batch_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_item_id'), ('pay_batch_item_ids'), ('selected_pay_batch_item_ids'), ('expected_pay_batch_item_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_item_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_candidate_id'), ('pay_batch_candidate_ids'), ('selected_pay_batch_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_batch_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('candidate_id'), ('candidate_ids'), ('selected_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_bank_transfer_id'), ('pay_bank_transfer_ids'), ('selected_pay_bank_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_pay_bank_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('umbrella_id'), ('umbrella_ids'), ('selected_umbrella_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_umbrella_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_case_id'), ('finance_case_ids'), ('selected_finance_case_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_finance_case_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_component_id'), ('finance_component_ids'), ('selected_finance_component_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_finance_component_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('reservation_id'), ('reservation_ids'), ('selected_reservation_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_reservation_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('payout_transfer_id'), ('payout_transfer_ids'), ('selected_payout_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_selected_payout_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH source_payloads(payload) AS (
    VALUES (COALESCE(p_selection_json, '{}'::jsonb)), (COALESCE(p_selected_scope_json, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('transfer_group_key'), ('transfer_group_keys'), ('selected_transfer_group_keys')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM source_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(source_payloads.payload -> key_names.key_name) = 'array'
          THEN source_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(source_payloads.payload ->> key_names.key_name)
    FROM source_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(source_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT text_values.text_value), ARRAY[]::text[])
  INTO v_selected_transfer_group_keys
  FROM (
    SELECT raw_values.raw_value AS text_value
    FROM raw_values
    WHERE NULLIF(raw_values.raw_value, '') IS NOT NULL
  ) AS text_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_id'), ('pay_batch_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_item_id'), ('pay_batch_item_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_item_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_batch_candidate_id'), ('pay_batch_candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_batch_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('candidate_id'), ('candidate_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_candidate_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('pay_bank_transfer_id'), ('pay_bank_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_pay_bank_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('umbrella_id'), ('umbrella_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_umbrella_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_case_id'), ('finance_case_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_finance_case_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('finance_component_id'), ('finance_component_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_finance_component_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('reservation_id'), ('reservation_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_reservation_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('payout_transfer_id'), ('payout_transfer_ids')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT uuid_values.uuid_value), ARRAY[]::uuid[])
  INTO v_mail_payout_transfer_ids
  FROM (
    SELECT raw_values.raw_value::uuid AS uuid_value
    FROM raw_values
    WHERE raw_values.raw_value ~ v_uuid_pattern
  ) AS uuid_values;

  WITH scope_payloads(payload) AS (
    VALUES (COALESCE(v_mail_scope, '{}'::jsonb))
  ), key_names(key_name) AS (
    VALUES ('transfer_group_key'), ('transfer_group_keys')
  ), raw_values(raw_value) AS (
    SELECT btrim(array_values.value_text)
    FROM scope_payloads
    JOIN key_names ON true
    CROSS JOIN LATERAL jsonb_array_elements_text(
      COALESCE(
        CASE WHEN jsonb_typeof(scope_payloads.payload -> key_names.key_name) = 'array'
          THEN scope_payloads.payload -> key_names.key_name
          ELSE '[]'::jsonb
        END,
        '[]'::jsonb
      )
    ) AS array_values(value_text)
    UNION ALL
    SELECT btrim(scope_payloads.payload ->> key_names.key_name)
    FROM scope_payloads
    JOIN key_names ON true
    WHERE jsonb_typeof(scope_payloads.payload -> key_names.key_name) IN ('string', 'number')
  )
  SELECT COALESCE(array_agg(DISTINCT text_values.text_value), ARRAY[]::text[])
  INTO v_mail_transfer_group_keys
  FROM (
    SELECT raw_values.raw_value AS text_value
    FROM raw_values
    WHERE NULLIF(raw_values.raw_value, '') IS NOT NULL
  ) AS text_values;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batches', 'pay_batch')
     AND v_mail_row.context_id = p_pay_batch_id THEN
    v_mail_pay_batch_ids := ARRAY(
      SELECT DISTINCT context_batch_ids.batch_id
      FROM unnest(v_mail_pay_batch_ids || ARRAY[p_pay_batch_id]) AS context_batch_ids(batch_id)
      WHERE context_batch_ids.batch_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_batch_item_ids := ARRAY(
      SELECT DISTINCT context_item_ids.item_id
      FROM unnest(v_mail_pay_batch_item_ids || ARRAY[v_mail_row.context_id]) AS context_item_ids(item_id)
      WHERE context_item_ids.item_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_bank_transfer_ids := ARRAY(
      SELECT DISTINCT context_transfer_ids.transfer_id
      FROM unnest(v_mail_pay_bank_transfer_ids || ARRAY[v_mail_row.context_id]) AS context_transfer_ids(transfer_id)
      WHERE context_transfer_ids.transfer_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_pay_batch_candidate_ids := ARRAY(
      SELECT DISTINCT context_candidate_ids.pay_batch_candidate_id
      FROM unnest(v_mail_pay_batch_candidate_ids || ARRAY[v_mail_row.context_id]) AS context_candidate_ids(pay_batch_candidate_id)
      WHERE context_candidate_ids.pay_batch_candidate_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advances', 'pay_advance', 'finance_case', 'finance_cases')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_finance_case_ids := ARRAY(
      SELECT DISTINCT context_finance_case_ids.finance_case_id
      FROM unnest(v_mail_finance_case_ids || ARRAY[v_mail_row.context_id]) AS context_finance_case_ids(finance_case_id)
      WHERE context_finance_case_ids.finance_case_id IS NOT NULL
    );
  END IF;

  IF lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations')
     AND v_mail_row.context_id IS NOT NULL THEN
    v_mail_reservation_ids := ARRAY(
      SELECT DISTINCT context_reservation_ids.reservation_id
      FROM unnest(v_mail_reservation_ids || ARRAY[v_mail_row.context_id]) AS context_reservation_ids(reservation_id)
      WHERE context_reservation_ids.reservation_id IS NOT NULL
    );
  END IF;

  v_mail_batch_matches := (
    p_pay_batch_id = ANY(v_mail_pay_batch_ids)
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batches', 'pay_batch')
      AND v_mail_row.context_id = p_pay_batch_id
    )
  );

  v_is_whole_batch_scope := (
    v_scope_type = 'BATCH'
    AND COALESCE(NULLIF(v_work_unit, ''), 'BATCH') = 'BATCH'
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) = 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) = 0
  ) OR (lower(btrim(COALESCE(p_selected_scope_json->>'is_whole_batch', ''))) IN ('true', 't', '1', 'yes'));

  v_selected_has_narrow_payment_scope := (
    COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    OR COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
  );

  v_selected_candidate_scope_complete := (
    (lower(btrim(COALESCE(p_selected_scope_json->>'selected_candidate_scope_complete', ''))) IN ('true', 't', '1', 'yes'))
    OR (lower(btrim(COALESCE(p_selected_scope_json->>'candidate_scope_complete', ''))) IN ('true', 't', '1', 'yes'))
    OR (
      v_scope_type = 'CANDIDATES'
      AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND v_selected_has_narrow_payment_scope = false
    )
  );

  v_selected_has_candidate_filter := (
    v_scope_type = 'CANDIDATES'
    OR COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    OR COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
  );

  v_selected_has_umbrella_filter := (
    v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
    OR COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
  );

  v_candidate_filter_requires_payment_match := (
    v_scope_type = 'CANDIDATES'
    AND v_selected_has_candidate_filter
    AND v_selected_has_narrow_payment_scope
  );

  v_umbrella_filter_requires_payment_match := (
    v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
    AND v_selected_has_umbrella_filter
    AND v_selected_has_narrow_payment_scope
  );

  v_mail_candidate_scope_matches := (
    (
      COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
      AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    )
    OR (
      COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_candidate_ids, 1), 0) > 0
      AND v_mail_candidate_ids && v_selected_candidate_ids
    )
    OR (
      COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'CANDIDATE'
      AND v_mail_row.recipient_id IS NOT NULL
      AND v_mail_row.recipient_id = ANY(v_selected_candidate_ids)
    )
  );

  v_mail_umbrella_scope_matches := (
    (
      COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
      AND v_mail_umbrella_ids && v_selected_umbrella_ids
    )
    OR (
      COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'UMBRELLA'
      AND v_mail_row.recipient_id IS NOT NULL
      AND v_mail_row.recipient_id = ANY(v_selected_umbrella_ids)
    )
  );

  v_transfer_scope_candidate_safe := (
    v_candidate_filter_requires_payment_match = false
    OR v_mail_candidate_scope_matches
  );

  v_transfer_scope_umbrella_safe := (
    v_umbrella_filter_requires_payment_match = false
    OR v_mail_umbrella_scope_matches
  );

  v_transfer_scope_safe := (
    v_transfer_scope_candidate_safe
    AND v_transfer_scope_umbrella_safe
  );

  IF v_mail_batch_matches = false THEN
    SELECT EXISTS(
      SELECT 1
      FROM unnest(v_selected_pay_batch_ids || ARRAY[p_pay_batch_id]) AS selected_batch_ids(batch_id)
      WHERE selected_batch_ids.batch_id IS NOT NULL
        AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_batch_ids.batch_id::text || '%'
    )
    INTO v_reference_broad_match;

    IF v_reference_broad_match THEN
      IF v_is_whole_batch_scope THEN
        RETURN jsonb_build_object(
          'matched', true,
          'match_kind', 'WHOLE_BATCH',
          'match_confidence', 'LEGACY_BROAD',
          'safe_to_cancel', true,
          'requires_review', false,
          'reason', 'WHOLE_BATCH_SCOPE_MATCH_BY_LEGACY_BATCH_REFERENCE',
          'mail_outbox_id', p_mail_outbox_id,
          'pay_batch_id', p_pay_batch_id,
          'mail_status', v_mail_row.status::text,
          'mail_type', v_mail_row.type,
          'recipient_kind', v_mail_row.recipient_kind,
          'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
          'context_kind', v_mail_row.context_kind,
          'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
          'reference', v_mail_row.reference,
          'payment_scope_json', v_mail_scope,
          'legacy_broad_match_allowed', true,
          'safe_rule', 'Whole-batch correction may cancel mail rows matched by pay_batch_id.'
        );
      END IF;

      RETURN jsonb_build_object(
        'matched', true,
        'match_kind', 'LEGACY_BROAD',
        'match_confidence', 'LEGACY_BROAD',
        'safe_to_cancel', false,
        'requires_review', true,
        'reason', 'MAIL_MATCHES_BATCH_ONLY_BY_LEGACY_REFERENCE',
        'mail_outbox_id', p_mail_outbox_id,
        'pay_batch_id', p_pay_batch_id,
        'mail_status', v_mail_row.status::text,
        'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false)
      );
    END IF;

    RETURN jsonb_build_object(
      'matched', false,
      'match_kind', 'NONE',
      'match_confidence', 'NONE',
      'safe_to_cancel', false,
      'requires_review', false,
      'reason', 'MAIL_ROW_NOT_IN_PAY_BATCH_SCOPE',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_context_kind', v_mail_row.context_kind,
      'mail_context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END
    );
  END IF;

  IF v_is_whole_batch_scope THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'WHOLE_BATCH',
      'match_confidence', 'EXACT',
      'safe_to_cancel', true,
      'requires_review', false,
      'reason', 'WHOLE_BATCH_SCOPE_MATCH',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'payment_scope_json', v_mail_scope
    );
  END IF;

  v_item_exact := (
    COALESCE(array_length(v_mail_pay_batch_item_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    AND v_mail_pay_batch_item_ids <@ v_selected_pay_batch_item_ids
  );

  v_item_partial := (
    COALESCE(array_length(v_mail_pay_batch_item_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0) > 0
    AND v_mail_pay_batch_item_ids && v_selected_pay_batch_item_ids
    AND v_item_exact = false
  );

  v_transfer_exact := (
    COALESCE(array_length(v_mail_pay_bank_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    AND v_mail_pay_bank_transfer_ids <@ v_selected_pay_bank_transfer_ids
  );

  v_transfer_partial := (
    COALESCE(array_length(v_mail_pay_bank_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
    AND v_mail_pay_bank_transfer_ids && v_selected_pay_bank_transfer_ids
    AND v_transfer_exact = false
  );

  v_payout_transfer_exact := (
    COALESCE(array_length(v_mail_payout_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
    AND v_mail_payout_transfer_ids <@ v_selected_payout_transfer_ids
  );

  v_payout_transfer_partial := (
    COALESCE(array_length(v_mail_payout_transfer_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0) > 0
    AND v_mail_payout_transfer_ids && v_selected_payout_transfer_ids
    AND v_payout_transfer_exact = false
  );

  v_transfer_group_exact := (
    COALESCE(array_length(v_mail_transfer_group_keys, 1), 0) > 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    AND v_mail_transfer_group_keys <@ v_selected_transfer_group_keys
  );

  v_transfer_group_partial := (
    COALESCE(array_length(v_mail_transfer_group_keys, 1), 0) > 0
    AND COALESCE(array_length(v_selected_transfer_group_keys, 1), 0) > 0
    AND v_mail_transfer_group_keys && v_selected_transfer_group_keys
    AND v_transfer_group_exact = false
  );

  v_finance_case_exact := (
    COALESCE(array_length(v_mail_finance_case_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    AND v_mail_finance_case_ids <@ v_selected_finance_case_ids
  );

  v_finance_case_partial := (
    COALESCE(array_length(v_mail_finance_case_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_case_ids, 1), 0) > 0
    AND v_mail_finance_case_ids && v_selected_finance_case_ids
    AND v_finance_case_exact = false
  );

  v_finance_component_exact := (
    COALESCE(array_length(v_mail_finance_component_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    AND v_mail_finance_component_ids <@ v_selected_finance_component_ids
  );

  v_finance_component_partial := (
    COALESCE(array_length(v_mail_finance_component_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_finance_component_ids, 1), 0) > 0
    AND v_mail_finance_component_ids && v_selected_finance_component_ids
    AND v_finance_component_exact = false
  );

  v_reservation_exact := (
    COALESCE(array_length(v_mail_reservation_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    AND v_mail_reservation_ids <@ v_selected_reservation_ids
  );

  v_reservation_partial := (
    COALESCE(array_length(v_mail_reservation_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_reservation_ids, 1), 0) > 0
    AND v_mail_reservation_ids && v_selected_reservation_ids
    AND v_reservation_exact = false
  );

  v_pay_batch_candidate_exact := (
    v_selected_candidate_scope_complete
    AND COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    AND v_mail_pay_batch_candidate_ids <@ v_selected_pay_batch_candidate_ids
  );

  v_pay_batch_candidate_partial := (
    COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
    AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    AND v_pay_batch_candidate_exact = false
  );

  v_umbrella_group_exact := (
    COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
    AND v_mail_umbrella_ids <@ v_selected_umbrella_ids
    AND (
      v_transfer_group_exact
      OR v_transfer_exact
      OR v_item_exact
      OR v_payout_transfer_exact
      OR v_finance_case_exact
      OR v_finance_component_exact
      OR v_reservation_exact
    )
  );

  v_umbrella_partial := (
    COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
    AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
    AND v_mail_umbrella_ids && v_selected_umbrella_ids
    AND v_umbrella_group_exact = false
  );

  v_context_exact := (
    (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item')
      AND v_mail_row.context_id = ANY(v_selected_pay_batch_item_ids)
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer')
      AND v_mail_row.context_id = ANY(v_selected_pay_bank_transfer_ids)
      AND v_transfer_scope_safe
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advances', 'pay_advance', 'finance_case', 'finance_cases')
      AND v_mail_row.context_id = ANY(v_selected_finance_case_ids)
    )
    OR (
      lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations')
      AND v_mail_row.context_id = ANY(v_selected_reservation_ids)
    )
    OR (
      v_selected_candidate_scope_complete
      AND lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate')
      AND v_mail_row.context_id = ANY(v_selected_pay_batch_candidate_ids)
    )
  );

  IF v_item_exact THEN
    v_match_kind := 'PAY_BATCH_ITEM';
    v_reason := 'PAY_BATCH_ITEM_SCOPE_MATCH';
  ELSIF v_transfer_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'TRANSFER_SCOPE_MATCH';
  ELSIF v_payout_transfer_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'PAYOUT_TRANSFER_SCOPE_MATCH';
  ELSIF v_umbrella_group_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'UMBRELLA_GROUP';
    v_reason := 'UMBRELLA_GROUP_SCOPE_MATCH';
  ELSIF v_finance_component_exact THEN
    v_match_kind := 'FINANCE_CASE';
    v_reason := 'FINANCE_COMPONENT_SCOPE_MATCH';
  ELSIF v_reservation_exact THEN
    v_match_kind := 'RESERVATION';
    v_reason := 'RESERVATION_SCOPE_MATCH';
  ELSIF v_finance_case_exact THEN
    v_match_kind := 'FINANCE_CASE';
    v_reason := 'FINANCE_CASE_SCOPE_MATCH';
  ELSIF v_transfer_group_exact AND v_transfer_scope_safe THEN
    v_match_kind := 'TRANSFER';
    v_reason := 'TRANSFER_GROUP_SCOPE_MATCH';
  ELSIF v_pay_batch_candidate_exact THEN
    v_match_kind := 'PAY_BATCH_CANDIDATE';
    v_reason := 'COMPLETE_PAY_BATCH_CANDIDATE_SCOPE_MATCH';
  ELSIF v_context_exact THEN
    v_match_kind := CASE
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_items', 'pay_batch_item') THEN 'PAY_BATCH_ITEM'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_bank_transfers', 'pay_bank_transfer') THEN 'TRANSFER'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_batch_candidates', 'pay_batch_candidate') THEN 'PAY_BATCH_CANDIDATE'
      WHEN lower(btrim(COALESCE(v_mail_row.context_kind, ''))) IN ('pay_advance_reservations', 'pay_advance_reservation', 'reservation', 'reservations') THEN 'RESERVATION'
      ELSE 'FINANCE_CASE'
    END;
    v_reason := 'CONTEXT_SCOPE_MATCH';
  END IF;

  v_partial_overlap_requires_review := (
    v_item_partial
    OR v_transfer_partial
    OR (v_transfer_exact AND v_transfer_scope_safe = false)
    OR v_payout_transfer_partial
    OR (v_payout_transfer_exact AND v_transfer_scope_safe = false)
    OR v_transfer_group_partial
    OR (v_transfer_group_exact AND v_transfer_scope_safe = false)
    OR (v_umbrella_group_exact AND v_transfer_scope_safe = false)
    OR v_finance_case_partial
    OR v_finance_component_partial
    OR v_reservation_partial
    OR (
      v_pay_batch_candidate_partial
      AND NOT (
        v_item_exact
        OR v_transfer_exact
        OR v_payout_transfer_exact
        OR v_transfer_group_exact
        OR v_finance_case_exact
        OR v_finance_component_exact
        OR v_reservation_exact
      )
    )
    OR (
      v_umbrella_partial
      AND NOT (
        v_item_exact
        OR v_transfer_exact
        OR v_payout_transfer_exact
        OR v_transfer_group_exact
        OR v_finance_case_exact
        OR v_finance_component_exact
        OR v_reservation_exact
      )
    )
  );

  IF v_match_kind <> 'NONE' AND v_partial_overlap_requires_review THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'LEGACY_BROAD',
      'match_confidence', 'LEGACY_BROAD',
      'safe_to_cancel', false,
      'requires_review', true,
      'reason', 'MAIL_SCOPE_PARTIAL_OVERLAP_REQUIRES_REVIEW',
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false),
      'partial_overlap', true,
      'would_otherwise_match_kind', v_match_kind,
      'would_otherwise_match_reason', v_reason,
      'safe_rule', 'Do not cancel selected-scope mail rows when a populated scope dimension only partially overlaps the selected scope.'
    );
  END IF;

  IF v_match_kind <> 'NONE' THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', v_match_kind,
      'match_confidence', 'EXACT',
      'safe_to_cancel', true,
      'requires_review', false,
      'reason', v_reason,
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'selected_scope_summary', jsonb_build_object(
        'scope_type', v_scope_type,
        'pay_batch_item_count', COALESCE(array_length(v_selected_pay_batch_item_ids, 1), 0),
        'pay_bank_transfer_count', COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0),
        'pay_batch_candidate_count', COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0),
        'finance_case_count', COALESCE(array_length(v_selected_finance_case_ids, 1), 0),
        'finance_component_count', COALESCE(array_length(v_selected_finance_component_ids, 1), 0),
        'reservation_count', COALESCE(array_length(v_selected_reservation_ids, 1), 0),
        'payout_transfer_count', COALESCE(array_length(v_selected_payout_transfer_ids, 1), 0),
        'transfer_group_count', COALESCE(array_length(v_selected_transfer_group_keys, 1), 0)
      )
    );
  END IF;

  v_partial_overlap_requires_review := (
    v_item_partial
    OR v_transfer_partial
    OR (v_transfer_exact AND v_transfer_scope_safe = false)
    OR v_payout_transfer_partial
    OR (v_payout_transfer_exact AND v_transfer_scope_safe = false)
    OR v_transfer_group_partial
    OR (v_transfer_group_exact AND v_transfer_scope_safe = false)
    OR (v_umbrella_group_exact AND v_transfer_scope_safe = false)
    OR v_finance_case_partial
    OR v_finance_component_partial
    OR v_reservation_partial
    OR v_pay_batch_candidate_partial
    OR v_umbrella_partial
  );

  SELECT EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_batch_item_ids) AS selected_reference_item_ids(item_id)
    WHERE selected_reference_item_ids.item_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_item_ids.item_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_bank_transfer_ids) AS selected_reference_transfer_ids(transfer_id)
    WHERE selected_reference_transfer_ids.transfer_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_transfer_ids.transfer_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_pay_batch_candidate_ids) AS selected_reference_candidate_ids(pay_batch_candidate_id)
    WHERE selected_reference_candidate_ids.pay_batch_candidate_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_candidate_ids.pay_batch_candidate_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_candidate_ids) AS selected_reference_candidates(candidate_id)
    WHERE selected_reference_candidates.candidate_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_candidates.candidate_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_umbrella_ids) AS selected_reference_umbrellas(umbrella_id)
    WHERE selected_reference_umbrellas.umbrella_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_umbrellas.umbrella_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_finance_case_ids) AS selected_reference_finance_cases(finance_case_id)
    WHERE selected_reference_finance_cases.finance_case_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_finance_cases.finance_case_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_finance_component_ids) AS selected_reference_finance_components(finance_component_id)
    WHERE selected_reference_finance_components.finance_component_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_finance_components.finance_component_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_reservation_ids) AS selected_reference_reservations(reservation_id)
    WHERE selected_reference_reservations.reservation_id IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_reservations.reservation_id::text || '%'
  ) OR EXISTS(
    SELECT 1
    FROM unnest(v_selected_transfer_group_keys) AS selected_reference_groups(transfer_group_key)
    WHERE NULLIF(selected_reference_groups.transfer_group_key, '') IS NOT NULL
      AND COALESCE(v_mail_row.reference, '') ILIKE '%' || selected_reference_groups.transfer_group_key || '%'
  )
  INTO v_reference_broad_match;

  v_recipient_broad_match := (
    (
      upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'CANDIDATE'
      AND v_mail_row.recipient_id IS NOT NULL
      AND (
        v_mail_row.recipient_id = ANY(v_selected_candidate_ids)
      )
    )
    OR (
      upper(btrim(COALESCE(v_mail_row.recipient_kind, ''))) = 'UMBRELLA'
      AND v_mail_row.recipient_id IS NOT NULL
      AND (
        v_mail_row.recipient_id = ANY(v_selected_umbrella_ids)
      )
    )
  );

  v_payment_scope_broad_match := (
    (
      COALESCE(array_length(v_mail_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_candidate_ids, 1), 0) > 0
      AND v_mail_candidate_ids && v_selected_candidate_ids
    )
    OR (
      COALESCE(array_length(v_mail_umbrella_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_umbrella_ids, 1), 0) > 0
      AND v_mail_umbrella_ids && v_selected_umbrella_ids
    )
    OR (
      COALESCE(array_length(v_mail_pay_batch_candidate_ids, 1), 0) > 0
      AND COALESCE(array_length(v_selected_pay_batch_candidate_ids, 1), 0) > 0
      AND v_mail_pay_batch_candidate_ids && v_selected_pay_batch_candidate_ids
    )
  );

  v_legacy_broad_match := (
    v_partial_overlap_requires_review
    OR v_reference_broad_match
    OR v_recipient_broad_match
    OR v_payment_scope_broad_match
  );

  IF v_legacy_broad_match THEN
    RETURN jsonb_build_object(
      'matched', true,
      'match_kind', 'LEGACY_BROAD',
      'match_confidence', 'LEGACY_BROAD',
      'safe_to_cancel', false,
      'requires_review', true,
      'reason', CASE
        WHEN v_partial_overlap_requires_review THEN 'MAIL_SCOPE_PARTIAL_OVERLAP_REQUIRES_REVIEW'
        WHEN v_reference_broad_match THEN 'MAIL_SCOPE_MATCHES_LEGACY_REFERENCE_ONLY'
        WHEN v_recipient_broad_match THEN 'MAIL_SCOPE_MATCHES_RECIPIENT_ONLY'
        ELSE 'MAIL_SCOPE_MATCHES_BROAD_PAYMENT_SCOPE_ONLY'
      END,
      'mail_outbox_id', p_mail_outbox_id,
      'pay_batch_id', p_pay_batch_id,
      'mail_status', v_mail_row.status::text,
      'mail_type', v_mail_row.type,
      'recipient_kind', v_mail_row.recipient_kind,
      'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
      'context_kind', v_mail_row.context_kind,
      'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
      'reference', v_mail_row.reference,
      'payment_scope_json', v_mail_scope,
      'legacy_broad_match_allowed', COALESCE(p_allow_legacy_broad_match, false),
      'partial_overlap', v_partial_overlap_requires_review,
      'safe_rule', 'Do not cancel selected-scope mail rows unless safe_to_cancel is true.'
    );
  END IF;

  RETURN jsonb_build_object(
    'matched', false,
    'match_kind', 'NONE',
    'match_confidence', 'NONE',
    'safe_to_cancel', false,
    'requires_review', false,
    'reason', 'NO_EXACT_OR_LEGACY_SCOPE_MATCH',
    'mail_outbox_id', p_mail_outbox_id,
    'pay_batch_id', p_pay_batch_id,
    'mail_status', v_mail_row.status::text,
    'mail_type', v_mail_row.type,
    'recipient_kind', v_mail_row.recipient_kind,
    'recipient_id', CASE WHEN v_mail_row.recipient_id IS NULL THEN NULL ELSE v_mail_row.recipient_id::text END,
    'context_kind', v_mail_row.context_kind,
    'context_id', CASE WHEN v_mail_row.context_id IS NULL THEN NULL ELSE v_mail_row.context_id::text END,
    'reference', v_mail_row.reference,
    'payment_scope_json', v_mail_scope
  );
END;
$function$;

-- _pay_payment_correction_selected_items(uuid,jsonb,boolean)
CREATE OR REPLACE FUNCTION public._pay_payment_correction_selected_items(p_pay_batch_id uuid, p_selection_json jsonb, p_include_already_corrected boolean DEFAULT false)
 RETURNS TABLE(pay_batch_id uuid, pay_batch_candidate_id uuid, candidate_id uuid, candidate_display_name text, candidate_tms_ref text, pay_batch_item_id uuid, item_type text, timesheet_id uuid, pay_bank_transfer_id uuid, transfer_status text, transfer_amount numeric, transfer_group_key text, payee_entity_kind text, payee_entity_id uuid, umbrella_id uuid, umbrella_name text, finance_case_id uuid, finance_component_id uuid, reservation_id uuid, pay_channel text, frozen_source_pay_method text, frozen_target_pay_method text, current_candidate_pay_method text, economic_key_type text, economic_key_value text, source_amount_ex_vat numeric, target_amount_ex_vat numeric, key_resolution_source text, key_resolution_failure_reason text, amount_ex_vat numeric, amount_vat numeric, amount_inc_vat numeric, is_voided boolean, already_corrected boolean, applied_correction_kinds text[])
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_type text;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_item_types text[] := ARRAY[]::text[];

  v_umbrella_id uuid;
  v_umbrella_id_text text;
  v_transfer_group_key text;
  v_expected_item_count integer := NULL::integer;
  v_expected_item_count_text text;
  v_work_unit text := NULL::text;
  v_source_correction_request_id_text text := NULL::text;
  v_explicit_item_ids_authoritative boolean := false;

  v_return_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_item_count integer := 0;
  v_selected_already_corrected_count integer := 0;
  v_return_item_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_returned_count integer := 0;
  v_sorted_return_item_ids uuid[] := ARRAY[]::uuid[];
  v_sorted_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
BEGIN
  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'selection_scope_type', CASE
        WHEN p_selection_json IS NULL THEN NULL
        ELSE p_selection_json->>'scope_type'
      END,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'selection_keys', CASE
        WHEN p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN '[]'::jsonb
        ELSE COALESCE((
          SELECT jsonb_agg(selection_keys.key_name ORDER BY selection_keys.key_name)
          FROM jsonb_object_keys(p_selection_json) AS selection_keys(key_name)
        ), '[]'::jsonb)
      END
    ),
    'pay_payment_correction',
    COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_check
    WHERE batch_check.id = p_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_scope_type := upper(nullif(btrim(coalesce(p_selection_json->>'scope_type', '')), ''));

  -- Compatibility is deliberately limited to the exact server-owned whole-Draft
  -- cancellation envelope produced by pay_batch_cancel before scope_type was
  -- frozen explicitly. Every other caller must still supply scope_type.
  IF v_scope_type IS NULL
     AND upper(btrim(coalesce(p_selection_json->>'requested_action', ''))) = 'DRAFT_CANCEL'
     AND upper(btrim(coalesce(p_selection_json->>'mode', ''))) = 'ALL_MATCHING'
     AND coalesce(p_selection_json->>'source_context', '') = 'pay_batch_cancel'
     AND coalesce(p_selection_json->'filter_json', '{}'::jsonb) = '{}'::jsonb
     AND coalesce(p_selection_json->'exclusions', '[]'::jsonb) = '[]'::jsonb THEN
    v_scope_type := 'BATCH';
  END IF;

  IF v_scope_type IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF v_scope_type = 'TIMESHEET'
     OR p_selection_json ? 'timesheet_id'
     OR p_selection_json ? 'timesheet_ids' THEN
    RAISE EXCEPTION 'TIMESHEET_SCOPE_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'TIMESHEET_SCOPE_NOT_ALLOWED',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF v_scope_type NOT IN ('BATCH', 'CANDIDATES', 'TRANSFER', 'UMBRELLA_PAYMENT_GROUP') THEN
    RAISE EXCEPTION 'UNSUPPORTED_PAYMENT_CORRECTION_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_PAYMENT_CORRECTION_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'supported_scope_types', jsonb_build_array(
                'BATCH',
                'CANDIDATES',
                'TRANSFER',
                'UMBRELLA_PAYMENT_GROUP'
              )
            )::text;
  END IF;

  IF p_selection_json ? 'pay_batch_candidate_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BATCH_CANDIDATE_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANDIDATE_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'pay_bank_transfer_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BANK_TRANSFER_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'pay_batch_item_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'expected_pay_batch_item_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'EXPECTED_PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'EXPECTED_PAY_BATCH_ITEM_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'finance_case_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'FINANCE_CASE_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'FINANCE_CASE_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'finance_component_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'FINANCE_COMPONENT_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'FINANCE_COMPONENT_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'reservation_ids'
     AND COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'RESERVATION_IDS_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'RESERVATION_IDS_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF p_selection_json ? 'item_types'
     AND COALESCE(jsonb_typeof(p_selection_json->'item_types'), 'null') <> 'array' THEN
    RAISE EXCEPTION 'ITEM_TYPES_MUST_BE_ARRAY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'ITEM_TYPES_MUST_BE_ARRAY', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT candidate_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_batch_candidate_ids'
        ELSE '[]'::jsonb
      END
    ) AS candidate_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_candidate_id'
    WHERE p_selection_json ? 'pay_batch_candidate_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_candidate_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;


  IF EXISTS (
    WITH raw_values AS (
      SELECT candidate_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_candidate_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_batch_candidate_ids'
          ELSE '[]'::jsonb
        END
      ) AS candidate_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_batch_candidate_id'
      WHERE p_selection_json ? 'pay_batch_candidate_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BATCH_CANDIDATE_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_CANDIDATE_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT transfer_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_bank_transfer_ids'
        ELSE '[]'::jsonb
      END
    ) AS transfer_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_bank_transfer_id'
    WHERE p_selection_json ? 'pay_bank_transfer_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_bank_transfer_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT transfer_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_bank_transfer_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_bank_transfer_ids'
          ELSE '[]'::jsonb
        END
      ) AS transfer_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_bank_transfer_id'
      WHERE p_selection_json ? 'pay_bank_transfer_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BANK_TRANSFER_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BANK_TRANSFER_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') = 'array'
          THEN p_selection_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS item_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_item_id'
    WHERE p_selection_json ? 'pay_batch_item_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT item_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'pay_batch_item_ids'), 'null') = 'array'
            THEN p_selection_json->'pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'pay_batch_item_id'
      WHERE p_selection_json ? 'pay_batch_item_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_PAY_BATCH_ITEM_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_PAY_BATCH_ITEM_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT expected_item_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
          THEN p_selection_json->'expected_pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS expected_item_array_values(raw_value)
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_expected_pay_batch_item_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT expected_item_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN p_selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS expected_item_array_values(raw_value)
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_EXPECTED_PAY_BATCH_ITEM_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_EXPECTED_PAY_BATCH_ITEM_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT finance_case_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') = 'array'
          THEN p_selection_json->'finance_case_ids'
        ELSE '[]'::jsonb
      END
    ) AS finance_case_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'finance_case_id'
    WHERE p_selection_json ? 'finance_case_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT finance_case_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_case_ids'), 'null') = 'array'
            THEN p_selection_json->'finance_case_ids'
          ELSE '[]'::jsonb
        END
      ) AS finance_case_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'finance_case_id'
      WHERE p_selection_json ? 'finance_case_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_FINANCE_CASE_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_FINANCE_CASE_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT finance_component_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') = 'array'
          THEN p_selection_json->'finance_component_ids'
        ELSE '[]'::jsonb
      END
    ) AS finance_component_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'finance_component_id'
    WHERE p_selection_json ? 'finance_component_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_finance_component_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT finance_component_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'finance_component_ids'), 'null') = 'array'
            THEN p_selection_json->'finance_component_ids'
          ELSE '[]'::jsonb
        END
      ) AS finance_component_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'finance_component_id'
      WHERE p_selection_json ? 'finance_component_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_FINANCE_COMPONENT_ID'
      USING ERRCODE = 'P0001',

            DETAIL = jsonb_build_object('code', 'INVALID_FINANCE_COMPONENT_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT reservation_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') = 'array'
          THEN p_selection_json->'reservation_ids'
        ELSE '[]'::jsonb
      END
    ) AS reservation_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'reservation_id'
    WHERE p_selection_json ? 'reservation_id'
  ),
  cleaned_values AS (
    SELECT nullif(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_reservation_ids
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL
    AND cleaned_values.clean_value ~ v_uuid_regex;

  IF EXISTS (
    WITH raw_values AS (
      SELECT reservation_array_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(p_selection_json->'reservation_ids'), 'null') = 'array'
            THEN p_selection_json->'reservation_ids'
          ELSE '[]'::jsonb
        END
      ) AS reservation_array_values(raw_value)
      UNION ALL
      SELECT p_selection_json->>'reservation_id'
      WHERE p_selection_json ? 'reservation_id'
    )
    SELECT 1
    FROM raw_values
    WHERE nullif(btrim(raw_values.raw_value), '') IS NOT NULL
      AND nullif(btrim(raw_values.raw_value), '') !~ v_uuid_regex
  ) THEN
    RAISE EXCEPTION 'INVALID_RESERVATION_ID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'INVALID_RESERVATION_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  WITH raw_values AS (
    SELECT item_type_array_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN COALESCE(jsonb_typeof(p_selection_json->'item_types'), 'null') = 'array'
          THEN p_selection_json->'item_types'
        ELSE '[]'::jsonb
      END
    ) AS item_type_array_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'item_type'
    WHERE p_selection_json ? 'item_type'
  ),
  cleaned_values AS (
    SELECT upper(nullif(btrim(raw_values.raw_value), '')) AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(DISTINCT cleaned_values.clean_value), ARRAY[]::text[])
  INTO v_item_types
  FROM cleaned_values
  WHERE cleaned_values.clean_value IS NOT NULL;

  v_umbrella_id_text := nullif(btrim(coalesce(p_selection_json->>'umbrella_id', '')), '');
  v_transfer_group_key := nullif(btrim(coalesce(p_selection_json->>'transfer_group_key', '')), '');

  IF v_umbrella_id_text IS NOT NULL THEN
    IF v_umbrella_id_text !~ v_uuid_regex THEN
      RAISE EXCEPTION 'INVALID_UMBRELLA_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'INVALID_UMBRELLA_ID', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
    END IF;

    v_umbrella_id := v_umbrella_id_text::uuid;
  END IF;

  v_expected_item_count_text := nullif(btrim(coalesce(p_selection_json->>'expected_item_count', '')), '');
  IF v_expected_item_count_text IS NOT NULL THEN
    BEGIN
      v_expected_item_count := v_expected_item_count_text::integer;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE EXCEPTION 'EXPECTED_ITEM_COUNT_MUST_BE_INTEGER'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'EXPECTED_ITEM_COUNT_MUST_BE_INTEGER',
                  'pay_batch_id', p_pay_batch_id,
                  'expected_item_count', v_expected_item_count_text
                )::text;
    END;

    IF v_expected_item_count < 0 THEN
      RAISE EXCEPTION 'EXPECTED_ITEM_COUNT_MUST_NOT_BE_NEGATIVE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'EXPECTED_ITEM_COUNT_MUST_NOT_BE_NEGATIVE',
                'pay_batch_id', p_pay_batch_id,
                'expected_item_count', v_expected_item_count
              )::text;
    END IF;
  END IF;

  v_work_unit := upper(NULLIF(btrim(COALESCE(p_selection_json->>'work_unit', '')), ''));
  v_source_correction_request_id_text := NULLIF(btrim(COALESCE(p_selection_json->>'source_correction_request_id', '')), '');

  v_explicit_item_ids_authoritative := (
    (
      COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0
      OR COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0
    )
    AND (
      v_source_correction_request_id_text IS NOT NULL
      OR v_work_unit IN ('BATCH', 'CANDIDATE', 'TRANSFER', 'CANDIDATE_TRANSFER', 'FINANCE_CASE')
    )
  );

  IF COALESCE(v_explicit_item_ids_authoritative, false)
     AND COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
     AND COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    v_pay_batch_item_ids := v_expected_pay_batch_item_ids;
  END IF;

  IF v_scope_type = 'CANDIDATES'
     AND COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_CANDIDATE_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF v_scope_type = 'TRANSFER'
     AND COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION 'PAY_BANK_TRANSFER_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BANK_TRANSFER_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
     AND v_umbrella_id IS NULL THEN
    RAISE EXCEPTION 'UMBRELLA_SELECTION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'UMBRELLA_SELECTION_REQUIRED', 'pay_batch_id', p_pay_batch_id, 'scope_type', v_scope_type)::text;
  END IF;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0 THEN
    IF EXISTS (
      SELECT 1
      FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
      LEFT JOIN public.pay_batch_items AS requested_pay_batch_items
        ON requested_pay_batch_items.id = requested_item_ids.requested_pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS requested_pay_batch_candidates
        ON requested_pay_batch_candidates.id = requested_pay_batch_items.pay_batch_candidate_id
      WHERE requested_pay_batch_items.id IS NULL
         OR requested_pay_batch_candidates.pay_batch_id IS DISTINCT FROM p_pay_batch_id
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'reason', 'One or more supplied pay_batch_item_ids do not exist in the selected batch.'
              )::text;
    END IF;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    IF EXISTS (
      SELECT 1
      FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id)
      LEFT JOIN public.pay_batch_items AS expected_pay_batch_items
        ON expected_pay_batch_items.id = expected_item_ids.expected_pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS expected_pay_batch_candidates
        ON expected_pay_batch_candidates.id = expected_pay_batch_items.pay_batch_candidate_id
      WHERE expected_pay_batch_items.id IS NULL
         OR expected_pay_batch_candidates.pay_batch_id IS DISTINCT FROM p_pay_batch_id
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'reason', 'One or more supplied expected_pay_batch_item_ids do not exist in the selected batch.'
              )::text;
    END IF;
  END IF;

  WITH applied_correction_rows AS (
    SELECT
      public.pay_payment_correction_items.pay_batch_item_id AS correction_pay_batch_item_id,
      public.pay_payment_correction_items.correction_item_kind AS correction_item_kind
    FROM public.pay_payment_correction_items
    WHERE public.pay_payment_correction_items.pay_batch_id = p_pay_batch_id
      AND public.pay_payment_correction_items.pay_batch_item_id IS NOT NULL
      AND public.pay_payment_correction_items.status = 'APPLIED'
  ),
  applied_corrections AS (
    SELECT
      applied_correction_rows.correction_pay_batch_item_id AS correction_pay_batch_item_id,
      array_agg(DISTINCT applied_correction_rows.correction_item_kind ORDER BY applied_correction_rows.correction_item_kind) AS applied_correction_kinds
    FROM applied_correction_rows
    GROUP BY applied_correction_rows.correction_pay_batch_item_id
  ),
  base_scope_items AS (
    SELECT DISTINCT
      public.pay_batch_items.id AS selected_pay_batch_item_id
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND (
        v_scope_type = 'BATCH'
        OR (
          v_scope_type = 'CANDIDATES'
          AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
        )
        OR (
          v_scope_type = 'TRANSFER'
          AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
        )
        OR (
          v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
          AND (
            public.pay_batch_items.umbrella_id = v_umbrella_id
            OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
            OR (
              upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
              AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
            )
          )
          AND (
            v_transfer_group_key IS NULL
            OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
          )
        )
      )
  ),
  raw_selected_items AS (
    SELECT DISTINCT

      public.pay_batch_items.id AS selected_pay_batch_item_id
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    JOIN base_scope_items
      ON base_scope_items.selected_pay_batch_item_id = public.pay_batch_items.id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND (
        COALESCE(array_length(v_pay_batch_item_ids, 1), 0) = 0
        OR public.pay_batch_items.id = ANY(v_pay_batch_item_ids)
      )
      AND (
        COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0) = 0
        OR public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_finance_case_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_case_id = ANY(v_finance_case_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_finance_component_ids, 1), 0) = 0
        OR public.pay_batch_items.finance_component_id = ANY(v_finance_component_ids)
      )
      AND (
        COALESCE(v_explicit_item_ids_authoritative, false)
        OR COALESCE(array_length(v_reservation_ids, 1), 0) = 0
        OR public.pay_batch_items.reservation_id = ANY(v_reservation_ids)
      )
      AND (
        COALESCE(array_length(v_item_types, 1), 0) = 0
        OR upper(coalesce(public.pay_batch_items.item_type, '')) = ANY(v_item_types)
      )
  ),
  requested_item_scope_violations AS (
    SELECT requested_item_ids.requested_pay_batch_item_id
    FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
    LEFT JOIN base_scope_items
      ON base_scope_items.selected_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id
    WHERE base_scope_items.selected_pay_batch_item_id IS NULL
  ),
  decorated_selected_items AS (
    SELECT
      raw_selected_items.selected_pay_batch_item_id AS selected_pay_batch_item_id,
      COALESCE(array_length(applied_corrections.applied_correction_kinds, 1), 0) > 0 AS selected_item_already_corrected
    FROM raw_selected_items
    LEFT JOIN applied_corrections
      ON applied_corrections.correction_pay_batch_item_id = raw_selected_items.selected_pay_batch_item_id
  ),
  selection_aggregate AS (
    SELECT
      COALESCE(
        array_agg(decorated_selected_items.selected_pay_batch_item_id ORDER BY decorated_selected_items.selected_pay_batch_item_id)
          FILTER (
            WHERE COALESCE(p_include_already_corrected, false)
               OR NOT decorated_selected_items.selected_item_already_corrected
          ),
        ARRAY[]::uuid[]
      ) AS return_item_ids,
      count(*)::integer AS selected_item_count,
      (count(*) FILTER (WHERE decorated_selected_items.selected_item_already_corrected))::integer AS selected_already_corrected_count
    FROM decorated_selected_items
  )
  SELECT
    selection_aggregate.return_item_ids,
    selection_aggregate.selected_item_count,
    selection_aggregate.selected_already_corrected_count
  INTO
    v_return_item_ids,
    v_selected_item_count,
    v_selected_already_corrected_count
  FROM selection_aggregate;

  IF COALESCE(array_length(v_pay_batch_item_ids, 1), 0) > 0
     AND EXISTS (
       WITH base_scope_items AS (
         SELECT DISTINCT
           public.pay_batch_items.id AS selected_pay_batch_item_id
         FROM public.pay_batch_items
         JOIN public.pay_batch_candidates
           ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
         LEFT JOIN public.pay_bank_transfers
           ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
         WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
           AND (
             v_scope_type = 'BATCH'
             OR (
               v_scope_type = 'CANDIDATES'
               AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
             )
             OR (
               v_scope_type = 'TRANSFER'
               AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
             )
             OR (
               v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
               AND (
                 public.pay_batch_items.umbrella_id = v_umbrella_id
                 OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
                 OR (
                   upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
                   AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
                 )
               )
               AND (
                 v_transfer_group_key IS NULL
                 OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
               )
             )
           )
       )
       SELECT 1
       FROM unnest(v_pay_batch_item_ids) AS requested_item_ids(requested_pay_batch_item_id)
       LEFT JOIN base_scope_items
         ON base_scope_items.selected_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id
       WHERE base_scope_items.selected_pay_batch_item_id IS NULL
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'pay_batch_item_ids', to_jsonb(v_pay_batch_item_ids)
            )::text;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0
     AND EXISTS (
       WITH base_scope_items AS (
         SELECT DISTINCT
           public.pay_batch_items.id AS selected_pay_batch_item_id
         FROM public.pay_batch_items
         JOIN public.pay_batch_candidates
           ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
         LEFT JOIN public.pay_bank_transfers
           ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
         WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
           AND (
             v_scope_type = 'BATCH'
             OR (
               v_scope_type = 'CANDIDATES'
               AND public.pay_batch_candidates.id = ANY(v_pay_batch_candidate_ids)
             )
             OR (
               v_scope_type = 'TRANSFER'
               AND public.pay_batch_items.pay_bank_transfer_id = ANY(v_pay_bank_transfer_ids)
             )
             OR (
               v_scope_type = 'UMBRELLA_PAYMENT_GROUP'
               AND (
                 public.pay_batch_items.umbrella_id = v_umbrella_id
                 OR public.pay_bank_transfers.umbrella_id = v_umbrella_id
                 OR (
                   upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
                   AND public.pay_bank_transfers.payee_entity_id = v_umbrella_id
                 )
               )
               AND (
                 v_transfer_group_key IS NULL
                 OR public.pay_bank_transfers.transfer_group_key = v_transfer_group_key
               )
             )
           )
       )
       SELECT 1
       FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id)
       LEFT JOIN base_scope_items
         ON base_scope_items.selected_pay_batch_item_id = expected_item_ids.expected_pay_batch_item_id
       WHERE base_scope_items.selected_pay_batch_item_id IS NULL
     ) THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAYMENT_CORRECTION_ITEM_SELECTION_OUT_OF_SCOPE',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'expected_pay_batch_item_ids', to_jsonb(v_expected_pay_batch_item_ids),
              'reason', 'One or more expected_pay_batch_item_ids are outside the base correction scope.'
            )::text;
  END IF;

  v_return_item_count := COALESCE(array_length(v_return_item_ids, 1), 0);

  IF v_expected_item_count IS NOT NULL
     AND v_return_item_count IS DISTINCT FROM v_expected_item_count THEN
    RAISE EXCEPTION 'WORK_SELECTION_DRIFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'WORK_SELECTION_DRIFT',
              'pay_batch_id', p_pay_batch_id,
              'scope_type', v_scope_type,
              'expected_item_count', v_expected_item_count,
              'resolved_item_count', v_return_item_count,
              'reason', 'Resolved correction item count no longer matches the expected work-item count.'
            )::text;
  END IF;

  IF COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0) > 0 THEN
    SELECT
      COALESCE(array_agg(DISTINCT returned_item_ids.returned_pay_batch_item_id ORDER BY returned_item_ids.returned_pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_sorted_return_item_ids
    FROM unnest(v_return_item_ids) AS returned_item_ids(returned_pay_batch_item_id);

    SELECT
      COALESCE(array_agg(DISTINCT expected_item_ids.expected_pay_batch_item_id ORDER BY expected_item_ids.expected_pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_sorted_expected_pay_batch_item_ids
    FROM unnest(v_expected_pay_batch_item_ids) AS expected_item_ids(expected_pay_batch_item_id);

    IF v_sorted_return_item_ids IS DISTINCT FROM v_sorted_expected_pay_batch_item_ids THEN
      RAISE EXCEPTION 'WORK_SELECTION_DRIFT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'WORK_SELECTION_DRIFT',
                'pay_batch_id', p_pay_batch_id,
                'scope_type', v_scope_type,
                'expected_pay_batch_item_ids', to_jsonb(v_sorted_expected_pay_batch_item_ids),
                'resolved_pay_batch_item_ids', to_jsonb(v_sorted_return_item_ids),
                'reason', 'Resolved correction item ids no longer exactly match the expected work-item ids.'
              )::text;
    END IF;
  END IF;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_RESOLVED_SCOPE',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_scope_type,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'pay_batch_candidate_id_count', COALESCE(array_length(v_pay_batch_candidate_ids, 1), 0),
      'pay_bank_transfer_id_count', COALESCE(array_length(v_pay_bank_transfer_ids, 1), 0),
      'pay_batch_item_id_count', COALESCE(array_length(v_pay_batch_item_ids, 1), 0),
      'expected_pay_batch_item_id_count', COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0),
      'finance_case_id_count', COALESCE(array_length(v_finance_case_ids, 1), 0),
      'finance_component_id_count', COALESCE(array_length(v_finance_component_ids, 1), 0),
      'reservation_id_count', COALESCE(array_length(v_reservation_ids, 1), 0),
      'item_type_count', COALESCE(array_length(v_item_types, 1), 0),
      'umbrella_id', v_umbrella_id,
      'transfer_group_key', v_transfer_group_key,
      'expected_item_count', v_expected_item_count,
      'work_unit', v_work_unit,
      'source_correction_request_id_present', v_source_correction_request_id_text IS NOT NULL,
      'explicit_item_ids_authoritative', COALESCE(v_explicit_item_ids_authoritative, false),
      'selected_item_count', v_selected_item_count,
      'selected_already_corrected_count', v_selected_already_corrected_count,
      'return_item_count', v_return_item_count

    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF v_return_item_count = 0 THEN
    RETURN;
  END IF;

  WITH requested_item_ids AS (
    SELECT unnest(v_return_item_ids) AS requested_pay_batch_item_id
  ),
  economic_components AS (
    SELECT
      economic_component_rows.pay_batch_item_id AS economic_pay_batch_item_id,
      economic_component_rows.key_resolution_failure_reason AS economic_key_resolution_failure_reason
    FROM public._pay_batch_item_economic_components(NULL::uuid, v_return_item_ids) AS economic_component_rows
  )
  SELECT
    (count(*) FILTER (
      WHERE economic_components.economic_pay_batch_item_id IS NULL
         OR economic_components.economic_key_resolution_failure_reason IS NOT NULL
    ))::integer
  INTO v_key_resolution_failure_count
  FROM requested_item_ids
  LEFT JOIN economic_components
    ON economic_components.economic_pay_batch_item_id = requested_item_ids.requested_pay_batch_item_id;

  RETURN QUERY
  WITH applied_correction_rows AS (
    SELECT
      public.pay_payment_correction_items.pay_batch_item_id AS correction_pay_batch_item_id,
      public.pay_payment_correction_items.correction_item_kind AS correction_item_kind
    FROM public.pay_payment_correction_items
    WHERE public.pay_payment_correction_items.pay_batch_id = p_pay_batch_id
      AND public.pay_payment_correction_items.pay_batch_item_id IS NOT NULL
      AND public.pay_payment_correction_items.status = 'APPLIED'
  ),
  applied_corrections AS (
    SELECT
      applied_correction_rows.correction_pay_batch_item_id AS correction_pay_batch_item_id,
      array_agg(DISTINCT applied_correction_rows.correction_item_kind ORDER BY applied_correction_rows.correction_item_kind) AS applied_correction_kinds
    FROM applied_correction_rows
    GROUP BY applied_correction_rows.correction_pay_batch_item_id
  ),
  economic_components AS (
    SELECT
      economic_component_rows.pay_batch_id AS economic_pay_batch_id,
      economic_component_rows.pay_batch_item_id AS economic_pay_batch_item_id,
      economic_component_rows.timesheet_id AS economic_timesheet_id,
      economic_component_rows.item_type AS economic_item_type,
      economic_component_rows.key_type AS economic_key_type,
      economic_component_rows.key_value AS economic_key_value,
      economic_component_rows.source_amount_ex_vat AS economic_source_amount_ex_vat,
      economic_component_rows.source_amount_inc_vat AS economic_source_amount_inc_vat,
      economic_component_rows.target_amount_ex_vat AS economic_target_amount_ex_vat,
      economic_component_rows.key_resolution_source AS economic_key_resolution_source,
      economic_component_rows.key_resolution_failure_reason AS economic_key_resolution_failure_reason
    FROM public._pay_batch_item_economic_components(NULL::uuid, v_return_item_ids) AS economic_component_rows
  ),
  selected_batch_rows AS (
    SELECT
      public.pay_batch_candidates.pay_batch_id AS selected_pay_batch_id,
      public.pay_batch_candidates.id AS selected_pay_batch_candidate_id,
      public.pay_batch_candidates.candidate_id AS selected_candidate_id,
      COALESCE(
        NULLIF(btrim(public.pay_batch_candidates.candidate_display_name), ''),
        NULLIF(btrim(public.candidates.display_name), ''),
        NULLIF(btrim(concat_ws(' ', public.candidates.first_name, public.candidates.last_name)), ''),
        NULLIF(btrim(public.candidates.tms_ref), ''),
        public.pay_batch_candidates.candidate_id::text
      ) AS selected_candidate_display_name,
      COALESCE(
        NULLIF(btrim(public.pay_batch_candidates.candidate_tms_ref), ''),
        NULLIF(btrim(public.candidates.tms_ref), '')
      ) AS selected_candidate_tms_ref,
      public.pay_batch_items.id AS selected_pay_batch_item_id,
      public.pay_batch_items.item_type AS selected_item_type,
      public.pay_batch_items.timesheet_id AS selected_timesheet_id,
      public.pay_batch_items.pay_bank_transfer_id AS selected_pay_bank_transfer_id,
      public.pay_bank_transfers.status AS selected_transfer_status,
      public.pay_bank_transfers.amount AS selected_transfer_amount,
      public.pay_bank_transfers.transfer_group_key AS selected_transfer_group_key,
      public.pay_bank_transfers.payee_entity_kind AS selected_payee_entity_kind,
      public.pay_bank_transfers.payee_entity_id AS selected_payee_entity_id,
      COALESCE(
        public.pay_batch_items.umbrella_id,
        public.pay_bank_transfers.umbrella_id,
        CASE
          WHEN upper(coalesce(public.pay_bank_transfers.payee_entity_kind, '')) IN ('UMBRELLA', 'UMBRELLA_COMPANY')
            THEN public.pay_bank_transfers.payee_entity_id
          ELSE NULL::uuid
        END
      ) AS selected_umbrella_id,
      public.pay_batch_items.finance_case_id AS selected_finance_case_id,
      public.pay_batch_items.finance_component_id AS selected_finance_component_id,
      public.pay_batch_items.reservation_id AS selected_reservation_id,
      public.pay_batch_items.pay_channel AS selected_pay_channel,
      public.pay_batch_items.frozen_source_pay_method AS selected_frozen_source_pay_method,
      public.pay_batch_items.frozen_target_pay_method AS selected_frozen_target_pay_method,
      public.candidates.pay_method AS selected_current_candidate_pay_method,
      public.pay_batch_items.amount_ex_vat AS selected_amount_ex_vat,
      public.pay_batch_items.amount_vat AS selected_amount_vat,
      public.pay_batch_items.amount_inc_vat AS selected_amount_inc_vat,
      public.pay_batch_items.is_voided AS selected_is_voided,
      COALESCE(applied_corrections.applied_correction_kinds, ARRAY[]::text[]) AS selected_applied_correction_kinds
    FROM public.pay_batch_items
    JOIN public.pay_batch_candidates
      ON public.pay_batch_candidates.id = public.pay_batch_items.pay_batch_candidate_id
    JOIN public.candidates
      ON public.candidates.id = public.pay_batch_candidates.candidate_id
    LEFT JOIN public.pay_bank_transfers
      ON public.pay_bank_transfers.id = public.pay_batch_items.pay_bank_transfer_id
    LEFT JOIN applied_corrections
      ON applied_corrections.correction_pay_batch_item_id = public.pay_batch_items.id
    WHERE public.pay_batch_candidates.pay_batch_id = p_pay_batch_id
      AND public.pay_batch_items.id = ANY(v_return_item_ids)
  )
  SELECT
    selected_batch_rows.selected_pay_batch_id AS pay_batch_id,
    selected_batch_rows.selected_pay_batch_candidate_id AS pay_batch_candidate_id,
    selected_batch_rows.selected_candidate_id AS candidate_id,
    selected_batch_rows.selected_candidate_display_name AS candidate_display_name,
    selected_batch_rows.selected_candidate_tms_ref AS candidate_tms_ref,
    selected_batch_rows.selected_pay_batch_item_id AS pay_batch_item_id,
    selected_batch_rows.selected_item_type AS item_type,
    selected_batch_rows.selected_timesheet_id AS timesheet_id,
    selected_batch_rows.selected_pay_bank_transfer_id AS pay_bank_transfer_id,
    selected_batch_rows.selected_transfer_status AS transfer_status,
    selected_batch_rows.selected_transfer_amount AS transfer_amount,
    selected_batch_rows.selected_transfer_group_key AS transfer_group_key,
    selected_batch_rows.selected_payee_entity_kind AS payee_entity_kind,
    selected_batch_rows.selected_payee_entity_id AS payee_entity_id,
    selected_batch_rows.selected_umbrella_id AS umbrella_id,
    public.umbrellas.name AS umbrella_name,
    selected_batch_rows.selected_finance_case_id AS finance_case_id,
    selected_batch_rows.selected_finance_component_id AS finance_component_id,
    selected_batch_rows.selected_reservation_id AS reservation_id,
    selected_batch_rows.selected_pay_channel AS pay_channel,
    selected_batch_rows.selected_frozen_source_pay_method AS frozen_source_pay_method,
    selected_batch_rows.selected_frozen_target_pay_method AS frozen_target_pay_method,
    selected_batch_rows.selected_current_candidate_pay_method AS current_candidate_pay_method,
    economic_components.economic_key_type AS economic_key_type,
    economic_components.economic_key_value AS economic_key_value,
    economic_components.economic_source_amount_ex_vat AS source_amount_ex_vat,
    economic_components.economic_target_amount_ex_vat AS target_amount_ex_vat,
    COALESCE(economic_components.economic_key_resolution_source, 'KEY_RESOLUTION_FAILED') AS key_resolution_source,
    CASE
      WHEN economic_components.economic_pay_batch_item_id IS NULL THEN 'ECONOMIC_COMPONENT_ROW_NOT_RETURNED'
      ELSE economic_components.economic_key_resolution_failure_reason
    END AS key_resolution_failure_reason,
    selected_batch_rows.selected_amount_ex_vat AS amount_ex_vat,
    selected_batch_rows.selected_amount_vat AS amount_vat,
    economic_components.economic_source_amount_inc_vat AS amount_inc_vat,
    selected_batch_rows.selected_is_voided AS is_voided,
    COALESCE(array_length(selected_batch_rows.selected_applied_correction_kinds, 1), 0) > 0 AS already_corrected,
    selected_batch_rows.selected_applied_correction_kinds AS applied_correction_kinds
  FROM selected_batch_rows
  LEFT JOIN economic_components
    ON economic_components.economic_pay_batch_item_id = selected_batch_rows.selected_pay_batch_item_id
  LEFT JOIN public.umbrellas
    ON public.umbrellas.id = selected_batch_rows.selected_umbrella_id
  ORDER BY
    selected_batch_rows.selected_candidate_display_name,
    selected_batch_rows.selected_transfer_group_key NULLS LAST,
    selected_batch_rows.selected_pay_bank_transfer_id NULLS LAST,
    selected_batch_rows.selected_pay_batch_item_id;

  GET DIAGNOSTICS v_returned_count = ROW_COUNT;

  PERFORM public._imp_debug_audit(
    NULL::uuid,
    'PAYMENT_CORRECTION_SELECTED_ITEMS_RETURNED',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'scope_type', v_scope_type,
      'include_already_corrected', COALESCE(p_include_already_corrected, false),
      'selected_item_count', v_selected_item_count,
      'selected_already_corrected_count', v_selected_already_corrected_count,
      'return_item_count', v_return_item_count,
      'returned_row_count', v_returned_count,
      'key_resolution_failure_count', v_key_resolution_failure_count,
      'expected_item_count', v_expected_item_count,
      'expected_pay_batch_item_id_count', COALESCE(array_length(v_expected_pay_batch_item_ids, 1), 0),
      'work_unit', v_work_unit,
      'source_correction_request_id_present', v_source_correction_request_id_text IS NOT NULL,
      'explicit_item_ids_authoritative', COALESCE(v_explicit_item_ids_authoritative, false)
    ),
    'pay_payment_correction',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN;

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      NULL::uuid,
      'PAYMENT_CORRECTION_SELECTED_ITEMS_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'scope_type', v_scope_type,
        'include_already_corrected', COALESCE(p_include_already_corrected, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM,
        'selection_json', p_selection_json
      ),
      'pay_payment_correction',
      COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

-- _pay_payment_correction_validate_accepted_finance_resolution(uuid,jsonb,jsonb,jsonb,uuid)
CREATE OR REPLACE FUNCTION public._pay_payment_correction_validate_accepted_finance_resolution(p_pay_batch_id uuid, p_selection_json jsonb, p_plan_json jsonb, p_accepted_resolution_json jsonb, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_batch public.pay_batches%rowtype;
  v_suggested_required boolean := false;
  v_plan_resolution jsonb := NULL::jsonb;
  v_plan_cases jsonb := '[]'::jsonb;
  v_resolution_root jsonb := NULL::jsonb;
  v_resolution_body jsonb := NULL::jsonb;
  v_resolution_cases jsonb := '[]'::jsonb;
  v_plan_case_json jsonb := NULL::jsonb;
  v_accepted_case_json jsonb := NULL::jsonb;
  v_case_row public.pay_advances%rowtype;
  v_component_record record;

  v_finance_case_id uuid := NULL::uuid;
  v_plan_candidate_id uuid := NULL::uuid;
  v_plan_candidate_id_text text := NULL::text;
  v_accepted_candidate_id_text text := NULL::text;
  v_plan_apply_surface text := NULL::text;
  v_accepted_apply_surface text := NULL::text;
  v_plan_effective_pay_date_text text := NULL::text;
  v_accepted_effective_pay_date_text text := NULL::text;
  v_plan_effective_pay_date date := NULL::date;

  v_plan_selected_component_ids_json jsonb := '[]'::jsonb;
  v_accepted_component_ids_json jsonb := '[]'::jsonb;
  v_plan_component_ids uuid[] := ARRAY[]::uuid[];
  v_accepted_component_ids uuid[] := ARRAY[]::uuid[];
  v_current_component_ids uuid[] := ARRAY[]::uuid[];
  v_plan_component_count integer := 0;
  v_accepted_component_count integer := 0;
  v_current_component_count integer := 0;
  v_invalid_component_id_count integer := 0;

  v_plan_fingerprints_json jsonb := '{}'::jsonb;
  v_accepted_fingerprints_json jsonb := '{}'::jsonb;
  v_expected_fingerprint text := NULL::text;
  v_plan_fingerprint text := NULL::text;
  v_current_fingerprint text := NULL::text;
  v_missing_fingerprint_count integer := 0;
  v_fingerprint_mismatch_count integer := 0;
  v_plan_fingerprint_mismatch_count integer := 0;
  v_stale_component_count integer := 0;
  v_closed_unrecoverable_component_count integer := 0;

  v_plan_suggestion_hash text := NULL::text;
  v_accepted_suggestion_hash text := NULL::text;
  v_regenerated_suggestion jsonb := NULL::jsonb;
  v_regenerated_suggestion_hash text := NULL::text;
  v_resolution_path text := NULL::text;
  v_resolution_mode text := NULL::text;
  v_weeks_total text := NULL::text;
  v_weekly_due text := NULL::text;
  v_manual_total_remaining text := NULL::text;
  v_schedule_input_mode text := NULL::text;
  v_note text := NULL::text;
  v_regeneration_note text := NULL::text;
  v_accepted_hash_text text := NULL::text;
  v_accepted_hash_basis jsonb := '{}'::jsonb;
  v_accepted_basis_taxable_result jsonb := NULL::jsonb;
  v_regenerated_taxable_result jsonb := NULL::jsonb;

  v_affected_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_open_overlap_count integer := 0;
  v_extra_accepted_case_count integer := 0;
  v_case_count integer := 0;
  v_blocker jsonb := NULL::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAY_BATCH_ID_REQUIRED',
        'message', 'p_pay_batch_id is required.'
      )
    );
  END IF;

  IF p_plan_json IS NULL OR COALESCE(jsonb_typeof(p_plan_json), 'null') <> 'object' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_PLAN_JSON_MUST_BE_OBJECT',
        'message', 'p_plan_json must be an object.'
      )
    );
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', false,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAY_BATCH_NOT_FOUND',
        'message', 'The selected pay batch does not exist.',
        'pay_batch_id', p_pay_batch_id::text
      )
    );
  END IF;

  v_suggested_required := COALESCE((p_plan_json->>'suggested_resolution_required')::boolean, false)
    OR COALESCE((p_plan_json#>>'{suggested_resolution,required}')::boolean, false);

  v_plan_resolution := COALESCE(p_plan_json->'suggested_resolution', '{}'::jsonb);
  v_plan_cases := COALESCE(v_plan_resolution->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_plan_cases), 'null') <> 'array' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', v_suggested_required,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'PAYMENT_CORRECTION_PLAN_FINANCE_CASES_INVALID',
        'message', 'p_plan_json.suggested_resolution.finance_cases must be an array when suggested resolution is required.'
      )
    );
  END IF;

  SELECT count(*)::integer
  INTO v_case_count
  FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value);

  IF NOT v_suggested_required OR v_case_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'required', false,
      'validated', false,
      'finance_case_count', 0,
      'message', 'No accepted gross/channel-sensitive finance resolution is required for this correction plan.'
    );
  END IF;

  IF p_accepted_resolution_json IS NULL
     OR COALESCE(jsonb_typeof(p_accepted_resolution_json), 'null') <> 'object' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_REQUIRED',
        'message', 'accepted_resolution_json is required for selected gross/taxable/channel-sensitive finance items.',
        'finance_case_count', v_case_count
      )
    );
  END IF;

  v_resolution_root := p_accepted_resolution_json;
  v_resolution_body := CASE
    WHEN v_resolution_root ? 'suggested_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'suggested_resolution'), 'null') = 'object'
      THEN v_resolution_root->'suggested_resolution'
    WHEN v_resolution_root ? 'accepted_resolution'
      AND COALESCE(jsonb_typeof(v_resolution_root->'accepted_resolution'), 'null') = 'object'
      THEN v_resolution_root->'accepted_resolution'
    ELSE v_resolution_root
  END;

  v_resolution_cases := COALESCE(v_resolution_body->'finance_cases', v_resolution_root->'finance_cases', '[]'::jsonb);

  IF COALESCE(jsonb_typeof(v_resolution_cases), 'null') <> 'array' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_FINANCE_CASES_INVALID',
        'message', 'accepted_resolution_json.finance_cases must be an array.'
      )
    );
  END IF;

  SELECT count(*)::integer
  INTO v_extra_accepted_case_count
  FROM jsonb_array_elements(v_resolution_cases) AS accepted_case_elements(value)
  WHERE NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '') IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value)
      WHERE NULLIF(btrim(COALESCE(plan_case_elements.value->>'finance_case_id', '')), '') = NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '')
    );

  IF v_extra_accepted_case_count > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'required', true,
      'validated', false,
      'blocker', jsonb_build_object(
        'code', 'ACCEPTED_RESOLUTION_EXTRA_FINANCE_CASE',
        'message', 'accepted_resolution_json includes finance cases that are not present in the current correction plan.',
        'extra_case_count', v_extra_accepted_case_count
      )
    );
  END IF;

  FOR v_plan_case_json IN
    SELECT plan_case_elements.value
    FROM jsonb_array_elements(v_plan_cases) AS plan_case_elements(value)
    ORDER BY NULLIF(btrim(COALESCE(plan_case_elements.value->>'finance_case_id', '')), '')
  LOOP
    IF NULLIF(btrim(COALESCE(v_plan_case_json->>'finance_case_id', '')), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_FINANCE_CASE_ID_INVALID',
          'message', 'The correction plan contains an invalid finance_case_id.',
          'finance_case_id', v_plan_case_json->>'finance_case_id'
        )
      );
    END IF;

    v_finance_case_id := (v_plan_case_json->>'finance_case_id')::uuid;
    v_plan_candidate_id_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'candidate_id', '')), '');
    v_plan_candidate_id := CASE
      WHEN v_plan_candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN v_plan_candidate_id_text::uuid
      ELSE NULL::uuid
    END;

    SELECT public.pay_advances.*
    INTO v_case_row
    FROM public.pay_advances
    WHERE public.pay_advances.id = v_finance_case_id;

    IF NOT FOUND THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_NOT_FOUND',
          'message', 'A selected finance case no longer exists.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_plan_candidate_id IS NOT NULL AND v_case_row.candidate_id IS DISTINCT FROM v_plan_candidate_id THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_FINANCE_CASE_CANDIDATE_MISMATCH',
          'message', 'The plan finance case candidate does not match the current finance case candidate.',
          'finance_case_id', v_finance_case_id::text,
          'planned_candidate_id', v_plan_candidate_id::text,
          'current_candidate_id', v_case_row.candidate_id::text
        )
      );
    END IF;

    SELECT accepted_case_elements.value
    INTO v_accepted_case_json
    FROM jsonb_array_elements(v_resolution_cases) AS accepted_case_elements(value)
    WHERE NULLIF(btrim(COALESCE(accepted_case_elements.value->>'finance_case_id', '')), '') = v_finance_case_id::text
    LIMIT 1;

    IF v_accepted_case_json IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_CASE_MISSING',
          'message', 'accepted_resolution_json does not include a selected gross/channel-sensitive finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    v_accepted_candidate_id_text := NULLIF(btrim(COALESCE(v_accepted_case_json->>'candidate_id', '')), '');

    IF v_accepted_candidate_id_text IS NOT NULL
       AND v_accepted_candidate_id_text <> v_case_row.candidate_id::text THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_CANDIDATE_MISMATCH',
          'message', 'accepted_resolution_json candidate_id does not match the selected finance case candidate.',
          'finance_case_id', v_finance_case_id::text,
          'accepted_candidate_id', v_accepted_candidate_id_text,
          'selected_candidate_id', v_case_row.candidate_id::text
        )
      );
    END IF;

    v_plan_selected_component_ids_json := COALESCE(
      v_plan_case_json->'selected_component_ids',
      v_plan_case_json->'component_ids',
      '[]'::jsonb
    );

    v_accepted_component_ids_json := COALESCE(
      v_accepted_case_json->'selected_component_ids',
      v_accepted_case_json->'component_ids',
      '[]'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_plan_selected_component_ids_json), 'null') <> 'array'
       OR COALESCE(jsonb_typeof(v_accepted_component_ids_json), 'null') <> 'array' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_IDS_INVALID',
          'message', 'Plan and accepted component ids must both be arrays.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    SELECT count(*)::integer
    INTO v_invalid_component_id_count
    FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
    WHERE accepted_component_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    IF v_invalid_component_id_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_ID_INVALID',
          'message', 'accepted_resolution_json contains invalid finance component ids.',
          'finance_case_id', v_finance_case_id::text,
          'invalid_component_id_count', v_invalid_component_id_count
        )
      );
    END IF;

    SELECT COALESCE(array_agg(DISTINCT plan_component_ids.value::uuid ORDER BY plan_component_ids.value::uuid), ARRAY[]::uuid[])
    INTO v_plan_component_ids
    FROM jsonb_array_elements_text(v_plan_selected_component_ids_json) AS plan_component_ids(value)
    WHERE plan_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT COALESCE(array_agg(DISTINCT accepted_component_ids.value::uuid ORDER BY accepted_component_ids.value::uuid), ARRAY[]::uuid[])
    INTO v_accepted_component_ids
    FROM jsonb_array_elements_text(v_accepted_component_ids_json) AS accepted_component_ids(value)
    WHERE accepted_component_ids.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    v_plan_component_count := COALESCE(array_length(v_plan_component_ids, 1), 0);
    v_accepted_component_count := COALESCE(array_length(v_accepted_component_ids, 1), 0);

    IF v_plan_component_count = 0 OR v_plan_component_ids IS DISTINCT FROM v_accepted_component_ids THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
          'message', 'accepted_resolution_json selected component ids do not exactly match the current correction plan.',
          'finance_case_id', v_finance_case_id::text,
          'planned_component_count', v_plan_component_count,
          'accepted_component_count', v_accepted_component_count
        )
      );
    END IF;

    SELECT COALESCE(array_agg(public.pay_finance_case_components.id ORDER BY public.pay_finance_case_components.id), ARRAY[]::uuid[])
    INTO v_current_component_ids
    FROM public.pay_finance_case_components
    WHERE public.pay_finance_case_components.finance_case_id = v_finance_case_id
      AND public.pay_finance_case_components.id = ANY(v_plan_component_ids);

    v_current_component_count := COALESCE(array_length(v_current_component_ids, 1), 0);

    IF v_current_component_ids IS DISTINCT FROM v_plan_component_ids THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_SCOPE_MISMATCH',
          'message', 'One or more selected finance components no longer belongs to the planned finance case.',
          'finance_case_id', v_finance_case_id::text,
          'planned_component_count', v_plan_component_count,
          'current_component_count', v_current_component_count
        )
      );
    END IF;

    v_plan_fingerprints_json := COALESCE(
      v_plan_case_json->'current_component_fingerprints',
      v_plan_case_json#>'{suggestion_hash_basis,component_fingerprints}',
      v_plan_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    v_accepted_fingerprints_json := COALESCE(
      v_accepted_case_json->'current_component_fingerprints',
      v_accepted_case_json->'component_fingerprints',
      '{}'::jsonb
    );

    IF COALESCE(jsonb_typeof(v_plan_fingerprints_json), 'null') <> 'object'
       OR COALESCE(jsonb_typeof(v_accepted_fingerprints_json), 'null') <> 'object' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_COMPONENT_FINGERPRINTS_INVALID',
          'message', 'Plan and accepted component fingerprints must both be objects.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    v_missing_fingerprint_count := 0;
    v_fingerprint_mismatch_count := 0;
    v_plan_fingerprint_mismatch_count := 0;
    v_stale_component_count := 0;
    v_closed_unrecoverable_component_count := 0;

    FOR v_component_record IN
      SELECT public.pay_finance_case_components.*
      FROM public.pay_finance_case_components
      WHERE public.pay_finance_case_components.finance_case_id = v_finance_case_id
        AND public.pay_finance_case_components.id = ANY(v_plan_component_ids)
      ORDER BY public.pay_finance_case_components.id
    LOOP
      v_current_fingerprint := COALESCE(
        NULLIF(btrim(v_component_record.resolution_fingerprint), ''),
        md5(jsonb_build_object(
          'finance_component_id', v_component_record.id,
          'finance_case_id', v_component_record.finance_case_id,
          'classification', v_component_record.classification::text,
          'source_pay_method', v_component_record.source_pay_method,
          'source_amount', v_component_record.source_amount,
          'remaining_source_amount', v_component_record.remaining_source_amount,
          'saved_target_pay_method', v_component_record.saved_target_pay_method,
          'saved_resolution_mode', v_component_record.saved_resolution_mode::text,
          'saved_resolution_payload_json', v_component_record.saved_resolution_payload_json,
          'saved_resolution_result_json', v_component_record.saved_resolution_result_json,
          'is_resolution_stale', v_component_record.is_resolution_stale,
          'closed_at_utc', v_component_record.closed_at_utc,
          'updated_at_utc', v_component_record.updated_at_utc
        )::text)
      );

      v_expected_fingerprint := NULLIF(btrim(COALESCE(v_accepted_fingerprints_json->>v_component_record.id::text, '')), '');
      v_plan_fingerprint := NULLIF(btrim(COALESCE(v_plan_fingerprints_json->>v_component_record.id::text, '')), '');

      IF v_expected_fingerprint IS NULL THEN
        v_missing_fingerprint_count := v_missing_fingerprint_count + 1;
      ELSIF v_expected_fingerprint <> v_current_fingerprint THEN
        v_fingerprint_mismatch_count := v_fingerprint_mismatch_count + 1;
      END IF;

      IF v_plan_fingerprint IS NOT NULL AND v_plan_fingerprint <> v_expected_fingerprint THEN
        v_plan_fingerprint_mismatch_count := v_plan_fingerprint_mismatch_count + 1;
      END IF;

      IF COALESCE(v_component_record.is_resolution_stale, false) THEN
        v_stale_component_count := v_stale_component_count + 1;
      END IF;

      IF v_component_record.closed_at_utc IS NOT NULL
         AND round(COALESCE(v_component_record.remaining_source_amount, 0), 2) <= 0 THEN
        v_closed_unrecoverable_component_count := v_closed_unrecoverable_component_count + 1;
      END IF;
    END LOOP;

    IF v_missing_fingerprint_count > 0
       OR v_fingerprint_mismatch_count > 0
       OR v_plan_fingerprint_mismatch_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because one or more component fingerprints no longer match.',
          'finance_case_id', v_finance_case_id::text,
          'missing_fingerprint_count', v_missing_fingerprint_count,
          'fingerprint_mismatch_count', v_fingerprint_mismatch_count,
          'plan_fingerprint_mismatch_count', v_plan_fingerprint_mismatch_count
        )
      );
    END IF;

    IF v_stale_component_count > 0 OR v_closed_unrecoverable_component_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_MANUAL_REVIEW_REQUIRED',
          'message', 'Selected finance components are stale or closed in a way that requires manual review before correction apply.',
          'finance_case_id', v_finance_case_id::text,
          'stale_component_count', v_stale_component_count,
          'closed_unrecoverable_component_count', v_closed_unrecoverable_component_count
        )
      );
    END IF;

    IF v_case_row.status <> 'ACTIVE'::public.pay_advance_status_enum
       OR v_case_row.written_off_at_utc IS NOT NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'FINANCE_CASE_NOT_ACTIVE_OR_WRITTEN_OFF',
          'message', 'Selected finance case is no longer active or has been written off.',
          'finance_case_id', v_finance_case_id::text,
          'status', v_case_row.status::text,
          'written_off_at_utc', v_case_row.written_off_at_utc
        )
      );
    END IF;

    SELECT COALESCE(array_agg(DISTINCT (affected_item_elements.value->>'reservation_id')::uuid ORDER BY (affected_item_elements.value->>'reservation_id')::uuid), ARRAY[]::uuid[])
    INTO v_affected_reservation_ids
    FROM jsonb_array_elements(COALESCE(p_plan_json->'affected_items', '[]'::jsonb)) AS affected_item_elements(value)
    WHERE NULLIF(btrim(COALESCE(affected_item_elements.value->>'finance_case_id', '')), '') = v_finance_case_id::text
      AND NULLIF(btrim(COALESCE(affected_item_elements.value->>'reservation_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    SELECT count(*)::integer
    INTO v_open_overlap_count
    FROM public.pay_batch_items AS overlapping_items
    JOIN public.pay_batch_candidates AS overlapping_candidates
      ON overlapping_candidates.id = overlapping_items.pay_batch_candidate_id
    JOIN public.pay_batches AS overlapping_batches
      ON overlapping_batches.id = overlapping_candidates.pay_batch_id
    WHERE overlapping_candidates.pay_batch_id <> p_pay_batch_id
      AND COALESCE(overlapping_items.is_voided, false) = false
      AND overlapping_batches.cancelled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(overlapping_batches.status)
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS overlap_corrections
        WHERE overlap_corrections.pay_batch_item_id = overlapping_items.id
          AND overlap_corrections.status = 'APPLIED'
      )
      AND (
        overlapping_items.finance_case_id = v_finance_case_id
        OR overlapping_items.finance_component_id = ANY(v_plan_component_ids)
        OR (
          COALESCE(array_length(v_affected_reservation_ids, 1), 0) > 0
          AND overlapping_items.reservation_id = ANY(v_affected_reservation_ids)
        )
      );

    IF v_open_overlap_count > 0 THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'DRAFT_BATCH_INTERFERENCE',
          'message', 'An open overlapping draft/reserved batch already references the selected finance case/component/reservation. Delete or cancel the overlapping draft first.',
          'finance_case_id', v_finance_case_id::text,
          'overlap_count', v_open_overlap_count
        )
      );
    END IF;

    v_plan_apply_surface := COALESCE(
      NULLIF(btrim(v_plan_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_plan_resolution->>'apply_surface'), ''),
      'pay_finance_case_apply_taxable_channel_restructure'
    );

    v_accepted_apply_surface := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'apply_surface'), ''),
      NULLIF(btrim(v_resolution_body->>'apply_surface'), ''),
      v_plan_apply_surface
    );

    IF v_accepted_apply_surface NOT IN (
      'pay_finance_case_apply_taxable_channel_restructure',
      'pay_manual_debt_adjustment_resolve_taxable_channel_change',
      'pay_finance_component_resolutions_apply'
    ) THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_UNSUPPORTED',
          'message', 'accepted_resolution_json apply_surface is not supported by the payment correction finance validation helper.',
          'finance_case_id', v_finance_case_id::text,
          'apply_surface', v_accepted_apply_surface
        )
      );
    END IF;

    IF v_plan_apply_surface IS NOT NULL AND v_accepted_apply_surface <> v_plan_apply_surface THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_APPLY_SURFACE_MISMATCH',
          'message', 'accepted_resolution_json apply_surface does not match the correction plan apply_surface.',
          'finance_case_id', v_finance_case_id::text,
          'planned_apply_surface', v_plan_apply_surface,
          'accepted_apply_surface', v_accepted_apply_surface
        )
      );
    END IF;

    v_plan_effective_pay_date_text := NULLIF(btrim(COALESCE(v_plan_case_json->>'effective_pay_date', p_plan_json#>>'{batch,effective_pay_date}', '')), '');
    v_accepted_effective_pay_date_text := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'effective_pay_date'), ''),
      NULLIF(btrim(v_resolution_body->>'effective_pay_date'), ''),
      v_plan_effective_pay_date_text
    );

    IF v_plan_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$'
       OR v_accepted_effective_pay_date_text !~ '^\d{4}-\d{2}-\d{2}$' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_INVALID',
          'message', 'Plan and accepted effective_pay_date must both be YYYY-MM-DD.',
          'finance_case_id', v_finance_case_id::text,
          'planned_effective_pay_date', v_plan_effective_pay_date_text,
          'accepted_effective_pay_date', v_accepted_effective_pay_date_text
        )
      );
    END IF;

    IF v_accepted_effective_pay_date_text <> v_plan_effective_pay_date_text THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_EFFECTIVE_PAY_DATE_MISMATCH',
          'message', 'accepted_resolution_json effective_pay_date does not match the correction plan effective_pay_date.',
          'finance_case_id', v_finance_case_id::text,
          'planned_effective_pay_date', v_plan_effective_pay_date_text,
          'accepted_effective_pay_date', v_accepted_effective_pay_date_text
        )
      );
    END IF;

    v_plan_effective_pay_date := v_plan_effective_pay_date_text::date;
    v_plan_suggestion_hash := NULLIF(btrim(COALESCE(v_plan_case_json->>'suggestion_hash', '')), '');
    v_accepted_suggestion_hash := COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'suggestion_hash'), ''),
      NULLIF(btrim(v_resolution_body->>'suggestion_hash'), '')
    );

    v_resolution_path := UPPER(COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'resolution_path'), ''),
      NULLIF(btrim(v_resolution_body->>'resolution_path'), ''),
      'SUGGESTED'
    ));
    IF v_resolution_path NOT IN ('SUGGESTED', 'MANUAL') THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_PATH_INVALID',
          'message', 'accepted_resolution_json resolution_path must be SUGGESTED or MANUAL.',
          'finance_case_id', v_finance_case_id::text,
          'resolution_path', v_resolution_path
        )
      );
    END IF;

    v_schedule_input_mode := UPPER(COALESCE(
      NULLIF(btrim(v_accepted_case_json->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_resolution_body->>'schedule_input_mode'), ''),
      NULLIF(btrim(v_accepted_case_json#>>'{suggestion,schedule_input_mode}'), '')
    ));
    v_weeks_total := COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}');
    v_weekly_due := COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}');
    v_manual_total_remaining := COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining');
    v_note := COALESCE(NULLIF(btrim(v_accepted_case_json->>'note'), ''), NULLIF(btrim(v_resolution_body->>'note'), ''));
    v_accepted_hash_text := COALESCE(NULLIF(btrim(v_accepted_case_json#>>'{suggestion_hash_basis,hash_text}'), ''), NULLIF(btrim(v_resolution_body#>>'{suggestion_hash_basis,hash_text}'), ''));
    v_accepted_hash_basis := COALESCE(v_accepted_case_json->'suggestion_hash_basis', v_resolution_body->'suggestion_hash_basis', '{}'::jsonb);

    IF COALESCE(jsonb_typeof(v_accepted_hash_basis), 'null') <> 'object' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_INVALID',
          'message', 'accepted_resolution_json suggestion_hash_basis must be an object.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_resolution_path = 'MANUAL' AND v_accepted_hash_text IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_TEXT_MISSING',
          'message', 'accepted_resolution_json manual resolution must include backend-generated suggestion_hash_basis.hash_text.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_plan_suggestion_hash IS NULL AND v_resolution_path = 'SUGGESTED' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'PLAN_SUGGESTION_HASH_MISSING',
          'message', 'The correction plan does not include a suggestion_hash for the selected finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_accepted_suggestion_hash IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_SUGGESTION_HASH_MISSING',
          'message', 'accepted_resolution_json does not include suggestion_hash for the selected finance case.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    IF v_resolution_path = 'SUGGESTED' AND v_accepted_suggestion_hash <> v_plan_suggestion_hash THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'accepted_resolution_json suggestion_hash does not match the correction plan suggestion_hash.',
          'finance_case_id', v_finance_case_id::text,
          'planned_suggestion_hash', v_plan_suggestion_hash,
          'accepted_suggestion_hash', v_accepted_suggestion_hash
        )
      );
    END IF;

    IF p_actor_user_id IS NULL THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACTOR_USER_ID_REQUIRED_FOR_SUGGESTED_RESOLUTION_VALIDATION',
          'message', 'A user id is required to regenerate and validate a gross/channel-sensitive finance suggestion.',
          'finance_case_id', v_finance_case_id::text
        )
      );
    END IF;

    v_regeneration_note := CASE
      WHEN v_resolution_path = 'MANUAL' THEN v_note
      ELSE COALESCE(
        NULLIF(btrim(v_accepted_case_json->>'note'), ''),
        NULLIF(btrim(v_accepted_case_json#>>'{suggestion,note}'), ''),
        NULLIF(btrim(v_accepted_case_json#>>'{suggestion,request,note}'), ''),
        NULLIF(btrim(v_plan_case_json->>'note'), ''),
        NULLIF(btrim(v_plan_case_json#>>'{suggestion,note}'), ''),
        NULLIF(btrim(v_plan_case_json#>>'{suggestion,request,note}'), ''),
        'Generated for payment correction plan ' || p_pay_batch_id::text
      )
    END;

    BEGIN
      v_regenerated_suggestion := public.pay_finance_case_taxable_channel_restructure_suggestion(
        p_finance_case_id => v_finance_case_id,
        p_actor_user_id => p_actor_user_id,
        p_effective_pay_date => v_plan_effective_pay_date,
        p_resolution_path => v_resolution_path,
        p_schedule_input_mode => CASE WHEN v_resolution_path = 'MANUAL' THEN NULLIF(v_schedule_input_mode, '') ELSE NULL::text END,
        p_weeks_total => CASE WHEN v_resolution_path = 'MANUAL' AND COALESCE(v_weeks_total, '') ~ '^\d+$' THEN v_weeks_total::integer ELSE NULL::integer END,
        p_weekly_due => CASE WHEN v_resolution_path = 'MANUAL' AND COALESCE(v_weekly_due, '') ~ '^\d+(\.\d+)?$' THEN v_weekly_due::numeric ELSE NULL::numeric END,
        p_manual_total_remaining => CASE WHEN v_resolution_path = 'MANUAL' AND COALESCE(v_manual_total_remaining, '') ~ '^\d+(\.\d+)?$' THEN v_manual_total_remaining::numeric ELSE NULL::numeric END,
        p_note => v_regeneration_note
      );
    EXCEPTION WHEN OTHERS THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'SUGGESTED_RESOLUTION_REGENERATION_FAILED',
          'message', 'The gross/channel-sensitive finance suggestion could not be regenerated during accepted-resolution validation.',
          'finance_case_id', v_finance_case_id::text,
          'sqlstate', SQLSTATE,
          'error_message', SQLERRM
        )
      );
    END;

    v_resolution_path := COALESCE(v_regenerated_suggestion->>'resolution_path', v_regenerated_suggestion#>>'{request,resolution_path}', 'SUGGESTED');
    v_resolution_mode := COALESCE(v_regenerated_suggestion->>'resolution_mode', v_regenerated_suggestion#>>'{result,resolution_mode}', v_regenerated_suggestion#>>'{suggestion,resolution_mode}');
    v_weeks_total := COALESCE(v_regenerated_suggestion->>'weeks_total', v_regenerated_suggestion#>>'{result,weeks_total}', v_regenerated_suggestion#>>'{suggestion,weeks_total}');
    v_weekly_due := COALESCE(v_regenerated_suggestion->>'weekly_due', v_regenerated_suggestion#>>'{result,weekly_due}', v_regenerated_suggestion#>>'{suggestion,weekly_due}');
    v_manual_total_remaining := COALESCE(v_regenerated_suggestion->>'manual_total_remaining', v_regenerated_suggestion#>>'{result,manual_total_remaining}', v_regenerated_suggestion#>>'{suggestion,manual_total_remaining}');

    v_regenerated_taxable_result := COALESCE(
      v_regenerated_suggestion->'taxable_channel_result',
      v_regenerated_suggestion->'result',
      v_regenerated_suggestion->'suggestion',
      v_regenerated_suggestion
    ) - 'generated_at'
      - 'generated_at_utc'
      - 'created_at'
      - 'created_at_utc'
      - 'updated_at'
      - 'updated_at_utc'
      - 'audit'
      - 'debug';

    IF v_resolution_path = 'MANUAL' THEN
      v_accepted_basis_taxable_result := v_accepted_hash_basis->'taxable_channel_result';

      IF COALESCE(v_accepted_hash_basis->>'hash_version', '') <> 'payment_correction_finance_resolution_v1'
         OR COALESCE(v_accepted_hash_basis->>'finance_case_id', '') <> v_finance_case_id::text
         OR COALESCE(v_accepted_hash_basis->>'candidate_id', '') <> v_case_row.candidate_id::text
         OR COALESCE(v_accepted_hash_basis->>'apply_surface', '') <> v_plan_apply_surface
         OR COALESCE(v_accepted_hash_basis->>'effective_pay_date', '') <> v_plan_effective_pay_date_text
         OR UPPER(COALESCE(v_accepted_hash_basis->>'resolution_path', '')) <> 'MANUAL'
         OR COALESCE(v_accepted_hash_basis->>'schedule_input_mode', '') <> COALESCE(NULLIF(btrim(v_accepted_case_json->>'schedule_input_mode'), ''), NULLIF(btrim(v_resolution_body->>'schedule_input_mode'), ''), '')
         OR COALESCE(v_accepted_hash_basis->>'weeks_total', '') <> COALESCE(v_accepted_case_json->>'weeks_total', v_resolution_body->>'weeks_total', v_accepted_case_json#>>'{suggestion,selected,weeks_total}', '')
         OR COALESCE(v_accepted_hash_basis->>'weekly_due', '') <> COALESCE(v_accepted_case_json->>'weekly_due', v_resolution_body->>'weekly_due', v_accepted_case_json#>>'{suggestion,selected,weekly_due}', '')
         OR COALESCE(v_accepted_hash_basis->>'manual_total_remaining', '') <> COALESCE(v_accepted_case_json->>'manual_total_remaining', v_resolution_body->>'manual_total_remaining', '')
         OR COALESCE(v_accepted_hash_basis->'selected_component_ids', '[]'::jsonb) IS DISTINCT FROM COALESCE(v_accepted_component_ids_json, '[]'::jsonb)
         OR COALESCE(v_accepted_hash_basis->'current_component_fingerprints', v_accepted_hash_basis->'component_fingerprints', '{}'::jsonb) IS DISTINCT FROM COALESCE(v_accepted_fingerprints_json, '{}'::jsonb)
         OR v_accepted_basis_taxable_result IS NULL
         OR v_accepted_basis_taxable_result IS DISTINCT FROM v_regenerated_taxable_result THEN
        RETURN jsonb_build_object(
          'ok', false,
          'required', true,
          'validated', false,
          'blocker', jsonb_build_object(
            'code', 'ACCEPTED_SUGGESTION_HASH_BASIS_MISMATCH',
            'message', 'accepted_resolution_json manual suggestion_hash_basis does not match the regenerated backend suggestion and selected correction scope.',
            'finance_case_id', v_finance_case_id::text
          )
        );
      END IF;

      v_regenerated_suggestion_hash := md5(v_accepted_hash_text);
    ELSE
      v_regenerated_suggestion_hash := md5(jsonb_build_object(
        'finance_case_id', v_finance_case_id,
        'candidate_id', v_case_row.candidate_id,
        'component_ids', COALESCE(v_plan_case_json->'component_ids', to_jsonb(v_plan_component_ids)),
        'selected_component_ids', COALESCE(v_plan_case_json->'selected_component_ids', to_jsonb(v_plan_component_ids)),
        'component_fingerprints', v_plan_fingerprints_json,
        'effective_pay_date', v_plan_effective_pay_date,
        'apply_surface', v_plan_apply_surface,
        'resolution_path', v_resolution_path,
        'resolution_mode', v_resolution_mode,
        'weeks_total', v_weeks_total,
        'weekly_due', v_weekly_due,
        'manual_total_remaining', v_manual_total_remaining,
        'taxable_channel_result', v_regenerated_taxable_result
      )::text);
    END IF;

    IF v_regenerated_suggestion_hash <> v_accepted_suggestion_hash THEN
      RETURN jsonb_build_object(
        'ok', false,
        'required', true,
        'validated', false,
        'blocker', jsonb_build_object(
          'code', 'ACCEPTED_RESOLUTION_STALE',
          'message', 'Accepted finance resolution is stale because the regenerated suggestion hash no longer matches the accepted suggestion hash.',
          'finance_case_id', v_finance_case_id::text,
          'accepted_suggestion_hash', v_accepted_suggestion_hash,
          'regenerated_suggestion_hash', v_regenerated_suggestion_hash
        )
      );
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'required', true,
    'validated', true,
    'finance_case_count', v_case_count,
    'pay_batch_id', p_pay_batch_id::text,
    'validated_at_utc', now()
  );
END;
$function$;

-- _pay_payment_movement_classify(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._pay_payment_movement_classify(p_pay_batch_id uuid, p_selection_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_selection_json jsonb := COALESCE(p_selection_json, '{}'::jsonb);
  v_diagnostic_json jsonb := '{}'::jsonb;
  v_lifecycle text := NULL::text;
  v_recommended_action text := NULL::text;
  v_classification text := NULL::text;
  v_blockers jsonb := '[]'::jsonb;
  v_manual_result jsonb := '{}'::jsonb;
  v_safe_to_auto_apply boolean := false;
  v_diagnostic_context text := NULL::text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_CORRECTION_SELECTION_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_diagnostic_context := CASE
    WHEN UPPER(BTRIM(COALESCE(v_selection_json->>'diagnostic_context', v_selection_json->>'diagnosticContext', ''))) IN (
      'CURRENT_PAYMENT_STATUS',
      'CURRENT_PAYMENT_STATUS_TAB',
      'PAYMENT_STATUS_TAB',
      'PAYMENT_ISSUES_TAB',
      'PAYMENT_ISSUE_REVIEW',
      'CANCELLATION_ACTION',
      'CANCEL_PAYMENT_ACTION',
      'CANCEL_WHOLE_BATCH_ACTION',
      'CORRECTION_REVIEW',
      'CORRECTION_ACTION',
      'PAYMENT_CORRECTION_PLAN',
      'USER_TRIGGERED_DIAGNOSTIC',
      'EXPLICIT_DIAGNOSTIC'
    ) THEN UPPER(BTRIM(COALESCE(v_selection_json->>'diagnostic_context', v_selection_json->>'diagnosticContext', '')))
    WHEN UPPER(BTRIM(COALESCE(v_selection_json->>'scope_type', v_selection_json->>'scopeType', ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'CANCEL_WHOLE_BATCH_ACTION'
    ELSE 'CANCEL_PAYMENT_ACTION'
  END;

  v_diagnostic_json := public.pay_payment_cancelability_diagnostic(
    p_pay_batch_id,
    v_selection_json,
    NULL::uuid,
    v_diagnostic_context
  );
  v_lifecycle := NULLIF(btrim(COALESCE(v_diagnostic_json->>'payment_lifecycle_state', '')), '');
  v_recommended_action := NULLIF(btrim(COALESCE(v_diagnostic_json->>'recommended_action', '')), '');
  v_manual_result := jsonb_build_object(
    'manual_adjustment_carry_forward_required', COALESCE(NULLIF(v_diagnostic_json->>'manual_adjustment_carry_forward_required', '')::boolean, false),
    'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json->'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'manual_adjustments_carried_forward_existing', COALESCE(v_diagnostic_json->'manual_adjustments_carried_forward_existing', '[]'::jsonb),
    'can_carry_forward_automatically', COALESCE(NULLIF(v_diagnostic_json->>'can_carry_forward_automatically', '')::boolean, true),
    'carry_forward_blockers', COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb),
    'manual_adjustment_support_details_json', COALESCE(v_diagnostic_json->'manual_adjustment_support_details_json', '{}'::jsonb)
  );

  v_blockers := COALESCE(v_diagnostic_json->'blockers', '[]'::jsonb);

  IF v_lifecycle = 'PAID_OR_SETTLED' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'NO_MONEY_UNWIND_HAS_SETTLEMENT_EVIDENCE',
      'message', 'Payment has paid/settled evidence; use overpayment recovery instead of no-money unwind.',
      'required_action', 'AMEND_AND_RECOVER_OVERPAYMENT'
    ));
  END IF;

  IF v_lifecycle = 'PROVIDER_OUTCOME_UNKNOWN' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PAYMENT_OUTCOME_UNKNOWN_CHECK_PROVIDER',
      'message', 'Provider outcome is unknown; check provider before retry or unwind.',
      'required_action', 'CHECK_PROVIDER_STATUS'
    ));
  END IF;

  IF v_lifecycle = 'PROVIDER_SUBMITTED_PENDING' THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'PROVIDER_CANCELLATION_REQUIRED_BEFORE_UNWIND',
      'message', 'Provider submission is pending; provider state must be checked before unwind.',
      'required_action', 'CHECK_PROVIDER_STATUS'
    ));
  END IF;

  IF COALESCE(NULLIF(v_diagnostic_json#>>'{manual_adjustment_support_details_json,source_less_ambiguous_count}', '')::integer, 0) > 0
     OR COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)), 0) > 0 THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS',
      'message', 'One or more source-less manual adjustments could not be carried forward automatically.',
      'carry_forward_blockers', COALESCE(v_diagnostic_json->'carry_forward_blockers', '[]'::jsonb)
    ));
  END IF;

  v_classification := CASE
    WHEN v_lifecycle = 'LOCAL_PREPARED_NOT_SENT' THEN 'LOCAL_PREPARED_NOT_SENT'
    WHEN v_lifecycle = 'SCHEDULED_LOCAL_NOT_SENT' THEN 'SCHEDULED_LOCAL_NOT_SENT'
    WHEN v_lifecycle = 'PROVIDER_SUBMITTED_PENDING' THEN 'PROVIDER_SUBMITTED_PENDING'
    WHEN v_lifecycle = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
    WHEN v_lifecycle = 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_OUTCOME_UNKNOWN'
    WHEN v_lifecycle = 'PROVIDER_CANCELLED_NO_MONEY' THEN 'PROVIDER_CANCELLED_NO_MONEY'
    WHEN v_lifecycle = 'PROVIDER_FAILED_NO_MONEY' THEN 'PROVIDER_FAILED_NO_MONEY'
    WHEN v_lifecycle = 'PAID_OR_SETTLED' THEN 'PAID_OR_SETTLED'
    WHEN v_lifecycle = 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION'
    WHEN v_lifecycle = 'CANCELLED_BEFORE_BANK_SUBMISSION' THEN 'CANCELLED_BEFORE_BANK_SUBMISSION'
    WHEN v_lifecycle = 'FINANCIALS_REWOUND' THEN 'FINANCIALS_REWOUND'
    ELSE 'PROVIDER_OUTCOME_UNKNOWN'
  END;

  v_safe_to_auto_apply := v_recommended_action IN ('PRE_PROVIDER_CANCEL_AND_RECALCULATE', 'NO_MONEY_UNWIND_AND_RECALCULATE')
    AND COALESCE(jsonb_array_length(COALESCE(v_blockers, '[]'::jsonb)), 0) = 0;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'diagnostic_context', v_diagnostic_context,
    'classification', v_classification,
    'payment_lifecycle_state', v_lifecycle,
    'recommended_action', v_recommended_action,
    'safe_to_auto_apply', v_safe_to_auto_apply,
    'can_pre_provider_cancel', COALESCE(NULLIF(v_diagnostic_json->>'can_pre_provider_cancel', '')::boolean, false),
    'can_no_money_unwind', COALESCE(NULLIF(v_diagnostic_json->>'can_no_money_unwind', '')::boolean, false),
    'can_recover_overpayment', COALESCE(NULLIF(v_diagnostic_json->>'can_recover_overpayment', '')::boolean, false),
    'requires_provider_cancel', COALESCE(NULLIF(v_diagnostic_json->>'requires_provider_cancel', '')::boolean, false),
    'requires_bank_check', COALESCE(NULLIF(v_diagnostic_json->>'requires_bank_check', '')::boolean, false),
    'requires_retry_later', COALESCE(NULLIF(v_diagnostic_json->>'requires_retry_later', '')::boolean, false),
    'blockers', COALESCE(v_blockers, '[]'::jsonb),
    'reasons', COALESCE(v_diagnostic_json->'warnings', '[]'::jsonb),
    'evidence', jsonb_build_object(
      'blocking_paid_evidence_json', COALESCE(v_diagnostic_json->'blocking_paid_evidence_json', '{}'::jsonb),
      'terminal_no_money_evidence_json', COALESCE(v_diagnostic_json->'terminal_no_money_evidence_json', '{}'::jsonb),
      'pending_provider_evidence_json', COALESCE(v_diagnostic_json->'pending_provider_evidence_json', '{}'::jsonb),
      'provider_evidence', COALESCE(v_diagnostic_json#>'{support_details_json,provider_evidence}', '{}'::jsonb)
    ),
    'manual_adjustment_carry_forward', v_manual_result,
    'resolved_full_payment_scope_json', COALESCE(v_diagnostic_json->'resolved_full_payment_scope_json', '{}'::jsonb),
    'finance_scope_json', COALESCE(v_diagnostic_json->'finance_scope_json', '{}'::jsonb),
    'diagnostic_payload', v_diagnostic_json,
    'policy_x_checked', true
  );
END;
$function$;

-- _pay_pct_to_frac(numeric)
CREATE OR REPLACE FUNCTION public._pay_pct_to_frac(p_pct numeric)
 RETURNS numeric
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
begin
  if p_pct is null then return 0; end if;
  if p_pct > 1 then return p_pct / 100; end if;
  return p_pct;
end;
$function$;

-- _pay_pct_to_mult(numeric)
CREATE OR REPLACE FUNCTION public._pay_pct_to_mult(p_pct numeric)
 RETURNS numeric
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  v_frac numeric;
begin
  v_frac := public._pay_pct_to_frac(p_pct);
  return 1 + coalesce(v_frac, 0);
end;
$function$;

-- _pay_policy_x_assert_economic_key(uuid,text,text,text,text,text,boolean,jsonb)
CREATE OR REPLACE FUNCTION public._pay_policy_x_assert_economic_key(p_timesheet_id uuid, p_key_type text, p_key_value text, p_context text DEFAULT NULL::text, p_authority_scope text DEFAULT NULL::text, p_resolution_source text DEFAULT NULL::text, p_required boolean DEFAULT true, p_source_json jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(ok boolean, timesheet_id uuid, key_type text, key_value text, failure_reason text, failure_detail text, resolution_source text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_timesheet_id uuid := p_timesheet_id;
  v_key_type text := UPPER(NULLIF(BTRIM(COALESCE(p_key_type, '')), ''));
  v_key_value text := NULLIF(BTRIM(COALESCE(p_key_value, '')), '');
  v_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_context, '')), ''), '-', '_'));
  v_authority_scope text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_authority_scope, '')), ''), '-', '_'));
  v_resolution_source text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_resolution_source, '')), ''), '-', '_'));
  v_required boolean := COALESCE(p_required, true);
  v_source_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_source_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_source_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_failure_reason text := NULL::text;
  v_failure_detail text := NULL::text;
  v_date_text text := NULL::text;
  v_source_live_component_flag text := NULL::text;
  v_source_live_tsfin_flag text := NULL::text;
  v_source_live_truth_flag text := NULL::text;
  v_source_live_segment_flag text := NULL::text;
  v_source_missing_snapshot_flag text := NULL::text;
BEGIN
  v_source_live_component_flag := LOWER(BTRIM(COALESCE(v_source_json->>'live_component_identity_used', '')));
  v_source_live_tsfin_flag := LOWER(BTRIM(COALESCE(v_source_json->>'live_tsfin_used', v_source_json->>'live_tsfin_identity_used', '')));
  v_source_live_truth_flag := LOWER(BTRIM(COALESCE(v_source_json->>'live_truth_used', '')));
  v_source_live_segment_flag := LOWER(BTRIM(COALESCE(v_source_json->>'live_segment_identity_used', '')));
  v_source_missing_snapshot_flag := LOWER(BTRIM(COALESCE(v_source_json->>'missing_deterministic_post_draft_snapshot', v_source_json->>'missing_frozen_snapshot', '')));

  IF v_authority_scope = 'POST_DRAFT'
     AND (
       v_resolution_source = 'LIVE'
       OR v_resolution_source LIKE 'LIVE_%'
       OR v_resolution_source LIKE '%_LIVE'
       OR v_resolution_source LIKE '%_LIVE_%'
       OR v_resolution_source LIKE '%LIVE_FINANCE%'
       OR v_resolution_source LIKE '%LIVE_COMPONENT%'
       OR v_resolution_source LIKE '%LIVE_TSFIN%'
       OR v_source_live_component_flag IN ('true', 't', 'yes', '1')
       OR v_source_live_tsfin_flag IN ('true', 't', 'yes', '1')
       OR v_source_live_truth_flag IN ('true', 't', 'yes', '1')
       OR v_source_live_segment_flag IN ('true', 't', 'yes', '1')
     ) THEN
    v_failure_reason := 'POST_DRAFT_LIVE_COMPONENT_KEY_REJECTED';
    v_failure_detail := 'Post-draft economic key resolution cannot use live truth, live TSFIN, live segment identity, or live finance-component identity.';

  ELSIF v_timesheet_id IS NULL AND v_required THEN
    v_failure_reason := CASE
      WHEN v_authority_scope = 'POST_DRAFT' THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
      ELSE 'TIMESHEET_ID_NOT_RESOLVED'
    END;
    v_failure_detail := 'timesheet_id is required for economic entitlement keyspace resolution.';

  ELSIF v_authority_scope = 'POST_DRAFT'
     AND v_required
     AND (v_key_type IS NULL OR v_key_value IS NULL)
     AND v_source_missing_snapshot_flag IN ('true', 't', 'yes', '1') THEN
    v_failure_reason := 'MISSING_DETERMINISTIC_POST_DRAFT_SNAPSHOT_EVIDENCE';
    v_failure_detail := 'Post-draft batch artefacts did not contain deterministic frozen snapshot evidence for the economic key.';

  ELSIF v_key_type IS NULL AND v_required THEN
    v_failure_reason := CASE
      WHEN v_authority_scope = 'POST_DRAFT' THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
      ELSE 'KEY_TYPE_NOT_RESOLVED'
    END;
    v_failure_detail := 'key_type is required and was not resolved.';

  ELSIF v_key_value IS NULL AND v_required THEN
    v_failure_reason := CASE
      WHEN v_authority_scope = 'POST_DRAFT' THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
      ELSE 'KEY_VALUE_NOT_RESOLVED'
    END;
    v_failure_detail := 'key_value is required and was not resolved.';

  ELSIF v_key_type IN (
    'SEGMENT_ID',
    'SEGMENT_KEY',
    'SEGMENT_STABLE_KEY',
    'STABLE_SEGMENT_KEY',
    'SEGMENT_STABLE_ID',
    'SEGMENT_HANDLE',
    'UI_SELECTION_ID',
    'SELECTION_ID',
    'SELECTION_HANDLE',
    'SNOOZE_ID',
    'SNOOZE_KEY',
    'PREVIEW_ROW_ID',
    'ROW_ID',
    'LINE_ID',
    'PAY_BATCH_ITEM_ID',
    'FINANCE_COMPONENT_ID',
    'COMPONENT_ID'
  ) THEN
    v_failure_reason := 'IDENTITY_HANDLE_NOT_ECONOMIC_KEY';
    v_failure_detail := 'Identity handles may locate evidence but cannot be used as entitlement buckets.';

  ELSIF v_key_type = 'TS_DAY' THEN
    IF v_key_value IS NULL THEN
      v_failure_reason := CASE
        WHEN v_authority_scope = 'POST_DRAFT' THEN 'POST_DRAFT_KEY_RESOLUTION_FAILED'
        ELSE 'KEY_VALUE_NOT_RESOLVED'
      END;
      v_failure_detail := 'TS_DAY requires a YYYY-MM-DD work-date key_value.';
    ELSIF v_key_value !~ '^\d{4}-\d{2}-\d{2}$' THEN
      v_failure_reason := CASE
        WHEN v_key_value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          OR v_key_value ~* '^(seg|segment|stable|snooze|row|selection)[:_\-]'
        THEN 'TS_DAY_KEY_VALUE_IDENTITY_HANDLE'
        ELSE 'TS_DAY_KEY_VALUE_NOT_DATE'
      END;
      v_failure_detail := 'TS_DAY must remain date-bucketed with key_value formatted as YYYY-MM-DD.';
    ELSE
      BEGIN
        v_date_text := to_char(v_key_value::date, 'YYYY-MM-DD');
        IF v_date_text IS DISTINCT FROM v_key_value THEN
          v_failure_reason := 'TS_DAY_KEY_VALUE_NOT_DATE';
          v_failure_detail := 'TS_DAY key_value is not a valid canonical YYYY-MM-DD date.';
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          v_failure_reason := 'TS_DAY_KEY_VALUE_NOT_DATE';
          v_failure_detail := 'TS_DAY key_value could not be cast to a valid date.';
      END;
    END IF;
  END IF;

  ok := v_failure_reason IS NULL;
  timesheet_id := v_timesheet_id;
  key_type := CASE WHEN v_failure_reason IS NULL THEN v_key_type ELSE NULL::text END;
  key_value := CASE WHEN v_failure_reason IS NULL THEN v_key_value ELSE NULL::text END;
  failure_reason := v_failure_reason;
  failure_detail := v_failure_detail;
  resolution_source := v_resolution_source;
  RETURN NEXT;
END;
$function$;

-- _pay_policy_x_resolve_post_draft_economic_key(uuid,uuid,uuid,text,text,text,jsonb,jsonb,jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._pay_policy_x_resolve_post_draft_economic_key(p_pay_batch_item_id uuid DEFAULT NULL::uuid, p_pay_batch_id uuid DEFAULT NULL::uuid, p_timesheet_id uuid DEFAULT NULL::uuid, p_item_type text DEFAULT NULL::text, p_frozen_key_type text DEFAULT NULL::text, p_frozen_key_value text DEFAULT NULL::text, p_frozen_component_snapshot_json jsonb DEFAULT '{}'::jsonb, p_frozen_source_basis_json jsonb DEFAULT '{}'::jsonb, p_breakdown_meta_json jsonb DEFAULT '{}'::jsonb, p_target_snapshot_json jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, key_resolution_source text, key_resolution_failure_reason text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
DECLARE
  v_pay_batch_id uuid := p_pay_batch_id;
  v_pay_batch_item_id uuid := p_pay_batch_item_id;
  v_timesheet_id uuid := p_timesheet_id;
  v_item_type text := UPPER(NULLIF(BTRIM(COALESCE(p_item_type, '')), ''));
  v_segment_key text := NULL::text;
  v_source_ref text := NULL::text;
  v_frozen_key_type text := UPPER(NULLIF(BTRIM(COALESCE(p_frozen_key_type, '')), ''));
  v_frozen_key_value text := NULLIF(BTRIM(COALESCE(p_frozen_key_value, '')), '');
  v_frozen_component_snapshot_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_frozen_component_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_frozen_component_snapshot_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_frozen_source_basis_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_frozen_source_basis_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_frozen_source_basis_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_breakdown_meta_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_breakdown_meta_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_breakdown_meta_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_target_snapshot_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_target_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_target_snapshot_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_breakdown_count integer := 0;
  v_snapshot_key_type text := NULL::text;
  v_snapshot_key_value text := NULL::text;
  v_work_date_text text := NULL::text;
  v_segment_id_text text := NULL::text;
  v_segment_key_text text := NULL::text;
  v_segment_stable_key_text text := NULL::text;
  v_ref_num_text text := NULL::text;
  v_adjustment_id_text text := NULL::text;
  v_additional_code_text text := NULL::text;
  v_expense_code_text text := NULL::text;
  v_segments_json jsonb := '[]'::jsonb;
  v_segment_match_found boolean := false;
  v_segment_match_date_text text := NULL::text;
  v_resolved_key_type text := NULL::text;
  v_resolved_key_value text := NULL::text;
  v_resolved_source text := NULL::text;
  v_assert_ok boolean := false;
  v_assert_key_type text := NULL::text;
  v_assert_key_value text := NULL::text;
  v_assert_failure_reason text := NULL::text;
  v_failure_reason text := NULL::text;
  v_source_json jsonb := '{}'::jsonb;
  v_frozen_timesheet_id_text text := NULL::text;
  v_frozen_timesheet_id_count integer := 0;
  v_frozen_linked_timesheet_id_text text := NULL::text;
  v_frozen_linked_timesheet_id_count integer := 0;
  v_frozen_direct_timesheet_id_text text := NULL::text;
  v_frozen_direct_timesheet_id_count integer := 0;
  v_frozen_carrier_timesheet_id_text text := NULL::text;
  v_frozen_carrier_timesheet_id_count integer := 0;
BEGIN
  IF v_pay_batch_item_id IS NOT NULL THEN
    SELECT
      batch_candidate_row.pay_batch_id,
      batch_item_row.timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item_row.item_type, '')), '')),
      batch_item_row.segment_key,
      batch_item_row.source_ref,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item_row.frozen_component_key_type, '')), '')),
      NULLIF(BTRIM(COALESCE(batch_item_row.frozen_component_key_value, '')), ''),
      CASE
        WHEN jsonb_typeof(COALESCE(batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)) = 'object' THEN COALESCE(batch_item_row.frozen_component_snapshot_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END,
      CASE
        WHEN jsonb_typeof(COALESCE(batch_item_row.frozen_source_basis_json, '{}'::jsonb)) = 'object' THEN COALESCE(batch_item_row.frozen_source_basis_json, '{}'::jsonb)
        ELSE '{}'::jsonb
      END
    INTO
      v_pay_batch_id,
      v_timesheet_id,
      v_item_type,
      v_segment_key,
      v_source_ref,
      v_frozen_key_type,
      v_frozen_key_value,
      v_frozen_component_snapshot_json,
      v_frozen_source_basis_json
    FROM public.pay_batch_items AS batch_item_row
    JOIN public.pay_batch_candidates AS batch_candidate_row
      ON batch_candidate_row.id = batch_item_row.pay_batch_candidate_id
    WHERE batch_item_row.id = v_pay_batch_item_id
      AND (p_pay_batch_id IS NULL OR batch_candidate_row.pay_batch_id = p_pay_batch_id)
    LIMIT 1;

    IF v_pay_batch_id IS NOT NULL THEN
      SELECT
        COUNT(*)::integer,
        CASE
          WHEN COUNT(*) = 1 THEN (ARRAY_AGG(breakdown_row.meta_json ORDER BY breakdown_row.id))[1]
          ELSE '{}'::jsonb
        END
      INTO
        v_breakdown_count,
        v_breakdown_meta_json
      FROM public.pay_batch_item_breakdowns AS breakdown_row
      WHERE breakdown_row.pay_batch_item_id = v_pay_batch_item_id;

      IF jsonb_typeof(COALESCE(v_breakdown_meta_json, '{}'::jsonb)) <> 'object' THEN
        v_breakdown_meta_json := '{}'::jsonb;
      END IF;

      IF v_timesheet_id IS NOT NULL
         AND (p_target_snapshot_json IS NULL OR jsonb_typeof(COALESCE(p_target_snapshot_json, '{}'::jsonb)) <> 'object' OR p_target_snapshot_json = '{}'::jsonb) THEN
        SELECT snapshot_row.target_snapshot_json
        INTO v_target_snapshot_json
        FROM public.pay_batch_timesheet_snapshots AS snapshot_row
        WHERE snapshot_row.pay_batch_id = v_pay_batch_id
          AND snapshot_row.timesheet_id = v_timesheet_id
        ORDER BY snapshot_row.created_at_utc DESC, snapshot_row.id DESC
        LIMIT 1;

        IF jsonb_typeof(COALESCE(v_target_snapshot_json, '{}'::jsonb)) <> 'object' THEN
          v_target_snapshot_json := '{}'::jsonb;
        END IF;
      END IF;
    END IF;
  END IF;

  -- Recovery rows freeze both the stable correction-root identity and the
  -- current carrier member inside the batch artefact. Those UUIDs may
  -- intentionally differ: linked_timesheet_id is the stable correction-root
  -- namespace, while carrier_timesheet_id identifies the member which carried
  -- the component when the draft was frozen. Do not treat that valid
  -- root/carrier pair as contradictory evidence.
  --
  -- Resolve each semantic role independently and fail closed only when frozen
  -- artefacts disagree within the same role. Prefer the stable linked/root
  -- identity, then a direct frozen timesheet identity, then the carrier.
  -- Policy X is preserved because every candidate below comes exclusively from
  -- the frozen batch artefact.
  IF v_timesheet_id IS NULL
     AND v_item_type = 'OVERPAYMENT_RECOVERY' THEN
    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_linked_timesheet_id_count,
      v_frozen_linked_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'linked_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'linked_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'linked_timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_direct_timesheet_id_count,
      v_frozen_direct_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    SELECT
      COUNT(DISTINCT frozen_timesheet_candidate.candidate_value)::integer,
      MIN(frozen_timesheet_candidate.candidate_value)
    INTO
      v_frozen_carrier_timesheet_id_count,
      v_frozen_carrier_timesheet_id_text
    FROM (
      VALUES
        (NULLIF(BTRIM(COALESCE(v_frozen_source_basis_json->>'carrier_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json#>>'{source_basis_json,carrier_timesheet_id}', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_frozen_component_snapshot_json->>'carrier_timesheet_id', '')), '')),
        (NULLIF(BTRIM(COALESCE(v_breakdown_meta_json->>'carrier_timesheet_id', '')), ''))
    ) AS frozen_timesheet_candidate(candidate_value)
    WHERE frozen_timesheet_candidate.candidate_value IS NOT NULL;

    IF COALESCE(v_frozen_linked_timesheet_id_count, 0) > 1
       OR COALESCE(v_frozen_direct_timesheet_id_count, 0) > 1
       OR COALESCE(v_frozen_carrier_timesheet_id_count, 0) > 1 THEN
      v_failure_reason := 'CONFLICTING_FROZEN_TIMESHEET_ID';
    ELSE
      v_frozen_timesheet_id_count :=
        COALESCE(v_frozen_linked_timesheet_id_count, 0)
        + COALESCE(v_frozen_direct_timesheet_id_count, 0)
        + COALESCE(v_frozen_carrier_timesheet_id_count, 0);
      v_frozen_timesheet_id_text := COALESCE(
        v_frozen_linked_timesheet_id_text,
        v_frozen_direct_timesheet_id_text,
        v_frozen_carrier_timesheet_id_text
      );

      IF v_frozen_timesheet_id_text IS NOT NULL
         AND pg_input_is_valid(v_frozen_timesheet_id_text, 'uuid') THEN
        v_timesheet_id := v_frozen_timesheet_id_text::uuid;
      ELSIF v_frozen_timesheet_id_text IS NOT NULL THEN
        v_failure_reason := 'INVALID_FROZEN_TIMESHEET_ID';
      END IF;
    END IF;

    IF v_failure_reason IS NULL
       AND COALESCE(v_frozen_timesheet_id_count, 0) > 0
       AND v_timesheet_id IS NULL THEN
      v_failure_reason := 'INVALID_FROZEN_TIMESHEET_ID';
    END IF;
  END IF;

  v_source_json := jsonb_build_object(
    'pay_batch_item_id', CASE WHEN v_pay_batch_item_id IS NULL THEN NULL ELSE v_pay_batch_item_id::text END,
    'pay_batch_id', CASE WHEN v_pay_batch_id IS NULL THEN NULL ELSE v_pay_batch_id::text END,
    'authority_scope', 'POST_DRAFT'
  );

  IF v_frozen_key_type IS NOT NULL OR v_frozen_key_value IS NOT NULL THEN
    SELECT
      key_check.ok,
      key_check.key_type,
      key_check.key_value,
      key_check.failure_reason
    INTO
      v_assert_ok,
      v_assert_key_type,
      v_assert_key_value,
      v_assert_failure_reason
    FROM public._pay_policy_x_assert_economic_key(
      p_timesheet_id => v_timesheet_id,
      p_key_type => v_frozen_key_type,
      p_key_value => v_frozen_key_value,
      p_context => 'POST_DRAFT_RESOLVE_FROZEN_ITEM_KEY',
      p_authority_scope => 'POST_DRAFT',
      p_resolution_source => 'FROZEN_ITEM_KEY',
      p_required => true,
      p_source_json => v_source_json
    ) AS key_check;

    IF COALESCE(v_assert_ok, false) THEN
      timesheet_id := v_timesheet_id;
      key_type := v_assert_key_type;
      key_value := v_assert_key_value;
      key_resolution_source := 'FROZEN_ITEM_KEY';
      key_resolution_failure_reason := NULL::text;
      RETURN NEXT;
      RETURN;
    ELSE
      v_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason);
    END IF;
  END IF;

  v_snapshot_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    v_frozen_component_snapshot_json->>'component_key_type',
    v_frozen_component_snapshot_json->>'key_type',
    v_frozen_component_snapshot_json#>>'{source_basis_json,component_key_type}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,key_type}',
    v_frozen_source_basis_json->>'component_key_type',
    v_frozen_source_basis_json->>'key_type',
    v_breakdown_meta_json->>'component_key_type',
    v_breakdown_meta_json->>'key_type',
    ''
  )), ''));

  v_snapshot_key_value := NULLIF(BTRIM(COALESCE(
    v_frozen_component_snapshot_json->>'component_key_value',
    v_frozen_component_snapshot_json->>'key_value',
    v_frozen_component_snapshot_json#>>'{source_basis_json,component_key_value}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,key_value}',
    v_frozen_source_basis_json->>'component_key_value',
    v_frozen_source_basis_json->>'key_value',
    v_breakdown_meta_json->>'component_key_value',
    v_breakdown_meta_json->>'key_value',
    ''
  )), '');

  IF v_snapshot_key_type IS NOT NULL OR v_snapshot_key_value IS NOT NULL THEN
    SELECT
      key_check.ok,
      key_check.key_type,
      key_check.key_value,
      key_check.failure_reason
    INTO
      v_assert_ok,
      v_assert_key_type,
      v_assert_key_value,
      v_assert_failure_reason
    FROM public._pay_policy_x_assert_economic_key(
      p_timesheet_id => v_timesheet_id,
      p_key_type => v_snapshot_key_type,
      p_key_value => v_snapshot_key_value,
      p_context => 'POST_DRAFT_RESOLVE_FROZEN_COMPONENT_OR_BASIS_KEY',
      p_authority_scope => 'POST_DRAFT',
      p_resolution_source => 'FROZEN_COMPONENT_OR_BASIS_KEY',
      p_required => true,
      p_source_json => v_source_json
    ) AS key_check;

    IF COALESCE(v_assert_ok, false) THEN
      timesheet_id := v_timesheet_id;
      key_type := v_assert_key_type;
      key_value := v_assert_key_value;
      key_resolution_source := 'FROZEN_COMPONENT_OR_BASIS_KEY';
      key_resolution_failure_reason := NULL::text;
      RETURN NEXT;
      RETURN;
    ELSE
      v_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason);
    END IF;
  END IF;

  v_work_date_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'work_date',
    v_frozen_source_basis_json->>'date',
    v_frozen_component_snapshot_json#>>'{source_basis_json,work_date}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,date}',
    v_frozen_component_snapshot_json->>'work_date',
    v_frozen_component_snapshot_json->>'date',
    v_breakdown_meta_json->>'work_date',
    v_breakdown_meta_json->>'date',
    ''
  )), '');

  v_segment_id_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_id',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_id}',
    v_frozen_component_snapshot_json->>'segment_id',
    v_breakdown_meta_json->>'segment_id',
    CASE
      WHEN v_source_ref IS NOT NULL AND BTRIM(v_source_ref) LIKE 'seg:%' THEN split_part(v_source_ref, ':', 2)
      ELSE NULL::text
    END,
    ''
  )), '');

  v_segment_key_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_key',
    v_frozen_source_basis_json->>'segment_stable_key',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_key}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_stable_key}',
    v_frozen_component_snapshot_json->>'segment_key',
    v_frozen_component_snapshot_json->>'segment_stable_key',
    v_breakdown_meta_json->>'segment_key',
    v_breakdown_meta_json->>'segment_stable_key',
    v_segment_key,
    ''
  )), '');

  v_segment_stable_key_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'segment_stable_key',
    v_frozen_component_snapshot_json#>>'{source_basis_json,segment_stable_key}',
    v_frozen_component_snapshot_json->>'segment_stable_key',
    v_breakdown_meta_json->>'segment_stable_key',
    ''
  )), '');

  v_ref_num_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'ref_num',
    v_frozen_component_snapshot_json#>>'{source_basis_json,ref_num}',
    v_frozen_component_snapshot_json->>'ref_num',
    v_breakdown_meta_json->>'ref_num',
    ''
  )), '');

  v_adjustment_id_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'adjustment_id',
    v_frozen_source_basis_json->>'adjustment_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,adjustment_id}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,adjustment_code}',
    v_frozen_component_snapshot_json->>'adjustment_id',
    v_frozen_component_snapshot_json->>'adjustment_code',
    v_breakdown_meta_json->>'adjustment_id',
    v_breakdown_meta_json->>'adjustment_code',
    ''
  )), '');

  v_additional_code_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'additional_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,additional_code}',
    v_frozen_component_snapshot_json->>'additional_code',
    v_breakdown_meta_json->>'additional_code',
    ''
  )), '');

  v_expense_code_text := NULLIF(BTRIM(COALESCE(
    v_frozen_source_basis_json->>'expense_code',
    v_frozen_source_basis_json->>'mileage_code',
    v_frozen_component_snapshot_json#>>'{source_basis_json,expense_code}',
    v_frozen_component_snapshot_json#>>'{source_basis_json,mileage_code}',
    v_frozen_component_snapshot_json->>'expense_code',
    v_frozen_component_snapshot_json->>'mileage_code',
    v_breakdown_meta_json->>'expense_code',
    v_breakdown_meta_json->>'mileage_code',
    ''
  )), '');

  IF v_item_type = 'SEGMENT_DELTA' AND v_work_date_text IS NOT NULL AND v_work_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
    v_resolved_key_type := 'TS_DAY';
    v_resolved_key_value := v_work_date_text;
    v_resolved_source := 'FROZEN_SOURCE_BASIS_DATE';

  ELSIF v_item_type = 'SEGMENT_DELTA' THEN
    IF jsonb_typeof(v_target_snapshot_json->'segments') = 'array' THEN
      v_segments_json := v_target_snapshot_json->'segments';
    ELSIF jsonb_typeof(v_target_snapshot_json#>'{actual_schedule_json,segments}') = 'array' THEN
      v_segments_json := v_target_snapshot_json#>'{actual_schedule_json,segments}';
    ELSIF jsonb_typeof(v_target_snapshot_json#>'{schedule_json,segments}') = 'array' THEN
      v_segments_json := v_target_snapshot_json#>'{schedule_json,segments}';
    ELSE
      v_segments_json := '[]'::jsonb;
    END IF;

    IF jsonb_array_length(v_segments_json) > 0 THEN
      SELECT
        NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), ''),
        true
      INTO
        v_segment_match_date_text,
        v_segment_match_found
      FROM jsonb_array_elements(v_segments_json) AS segment_choice(segment_json)
      WHERE segment_choice.segment_json IS NOT NULL
        AND jsonb_typeof(segment_choice.segment_json) = 'object'
        AND (
          (v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text)
          OR (v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text)
          OR (v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text)
          OR (v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text)
          OR (v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text)
        )
      ORDER BY
        CASE
          WHEN v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text THEN 0
          WHEN v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text THEN 1
          WHEN v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text THEN 2
          WHEN v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text THEN 3
          WHEN v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text THEN 4
          ELSE 9
        END
      LIMIT 1;

      IF COALESCE(v_segment_match_found, false)
         AND v_segment_match_date_text IS NOT NULL
         AND v_segment_match_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
        v_resolved_key_type := 'TS_DAY';
        v_resolved_key_value := v_segment_match_date_text;
        v_resolved_source := 'FROZEN_TIMESHEET_SNAPSHOT';
      ELSIF COALESCE(v_segment_match_found, false) THEN
        v_resolved_key_type := 'TS_TOTAL';
        v_resolved_key_value := 'TOTAL';
        v_resolved_source := 'FROZEN_TIMESHEET_SNAPSHOT_TOTAL_FALLBACK';
      END IF;
    END IF;

  ELSIF v_item_type = 'ADJUSTMENT_DELTA' AND v_adjustment_id_text IS NOT NULL THEN
    v_resolved_key_type := 'ADJUSTMENT_CODE';
    v_resolved_key_value := v_adjustment_id_text;
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';

  ELSIF v_item_type = 'EXPENSE_DELTA' AND v_additional_code_text IS NOT NULL THEN
    v_resolved_key_type := 'ADDITIONAL_CODE';
    v_resolved_key_value := UPPER(v_additional_code_text);
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';

  ELSIF v_item_type IN ('EXPENSE_DELTA', 'MILEAGE_DELTA') AND v_expense_code_text IS NOT NULL THEN
    v_resolved_key_type := 'EXPENSE_CODE';
    v_resolved_key_value := UPPER(v_expense_code_text);
    v_resolved_source := 'FROZEN_SOURCE_BASIS_KEY';
  END IF;

  IF v_resolved_key_type IS NULL AND v_item_type = 'SEGMENT_DELTA' THEN
    v_failure_reason := COALESCE(v_failure_reason, 'MISSING_DETERMINISTIC_POST_DRAFT_SNAPSHOT_EVIDENCE');
    v_source_json := v_source_json || jsonb_build_object('missing_deterministic_post_draft_snapshot', true);
  END IF;

  SELECT
    key_check.ok,
    key_check.key_type,
    key_check.key_value,
    key_check.failure_reason
  INTO
    v_assert_ok,
    v_assert_key_type,
    v_assert_key_value,
    v_assert_failure_reason
  FROM public._pay_policy_x_assert_economic_key(
    p_timesheet_id => v_timesheet_id,
    p_key_type => v_resolved_key_type,
    p_key_value => v_resolved_key_value,
    p_context => 'POST_DRAFT_RESOLVE_FINAL',
    p_authority_scope => 'POST_DRAFT',
    p_resolution_source => COALESCE(v_resolved_source, 'POST_DRAFT_KEY_RESOLUTION_FAILED'),
    p_required => true,
    p_source_json => v_source_json
  ) AS key_check;

  IF COALESCE(v_assert_ok, false) THEN
    timesheet_id := v_timesheet_id;
    key_type := v_assert_key_type;
    key_value := v_assert_key_value;
    key_resolution_source := v_resolved_source;
    key_resolution_failure_reason := NULL::text;
  ELSE
    timesheet_id := v_timesheet_id;
    key_type := NULL::text;
    key_value := NULL::text;
    key_resolution_source := 'KEY_RESOLUTION_FAILED';
    key_resolution_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason, 'POST_DRAFT_KEY_RESOLUTION_FAILED');
  END IF;

  RETURN NEXT;
END;
$function$;

-- _pay_policy_x_resolve_pre_draft_economic_key(uuid,jsonb,text,text,text,date)
CREATE OR REPLACE FUNCTION public._pay_policy_x_resolve_pre_draft_economic_key(p_timesheet_id uuid DEFAULT NULL::uuid, p_live_source_json jsonb DEFAULT '{}'::jsonb, p_item_type text DEFAULT NULL::text, p_key_type_hint text DEFAULT NULL::text, p_key_value_hint text DEFAULT NULL::text, p_work_date date DEFAULT NULL::date)
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, key_resolution_source text, key_resolution_failure_reason text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_source_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_live_source_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_live_source_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_timesheet_id uuid := p_timesheet_id;
  v_timesheet_id_text text;
  v_item_type text := UPPER(NULLIF(BTRIM(COALESCE(p_item_type, '')), ''));
  v_direct_key_type text;
  v_direct_key_value text;
  v_resolved_key_type text := NULL::text;
  v_resolved_key_value text := NULL::text;
  v_resolved_source text := NULL::text;
  v_failure_reason text := NULL::text;
  v_assert_ok boolean := false;
  v_assert_key_type text := NULL::text;
  v_assert_key_value text := NULL::text;
  v_assert_failure_reason text := NULL::text;
  v_work_date_text text := NULL::text;
  v_segment_id_text text := NULL::text;
  v_segment_key_text text := NULL::text;
  v_segment_stable_key_text text := NULL::text;
  v_ref_num_text text := NULL::text;
  v_adjustment_id_text text := NULL::text;
  v_additional_code_text text := NULL::text;
  v_expense_code_text text := NULL::text;
  v_segments_json jsonb := '[]'::jsonb;
  v_live_schedule_json jsonb := '{}'::jsonb;
  v_segment_match_found boolean := false;
  v_segment_match_date_text text := NULL::text;
  v_timesheet_start_date_text text := NULL::text;
BEGIN
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_source_json->>'timesheet_id', '')), '');
  IF v_timesheet_id IS NULL
     AND v_timesheet_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_timesheet_id := v_timesheet_id_text::uuid;
  END IF;

  IF v_item_type IS NULL THEN
    v_item_type := UPPER(NULLIF(BTRIM(COALESCE(
      v_source_json->>'item_type',
      v_source_json->>'line_type',
      v_source_json->>'component_type',
      v_source_json#>>'{source_basis_json,item_type}',
      ''
    )), ''));
  END IF;

  v_direct_key_type := UPPER(NULLIF(BTRIM(COALESCE(
    p_key_type_hint,
    v_source_json->>'component_key_type',
    v_source_json->>'key_type',
    v_source_json#>>'{source_basis_json,component_key_type}',
    v_source_json#>>'{source_basis_json,key_type}',
    ''
  )), ''));

  v_direct_key_value := NULLIF(BTRIM(COALESCE(
    p_key_value_hint,
    v_source_json->>'component_key_value',
    v_source_json->>'key_value',
    v_source_json#>>'{source_basis_json,component_key_value}',
    v_source_json#>>'{source_basis_json,key_value}',
    ''
  )), '');

  IF v_direct_key_type IS NOT NULL OR v_direct_key_value IS NOT NULL THEN
    SELECT
      key_check.ok,
      key_check.key_type,
      key_check.key_value,
      key_check.failure_reason
    INTO
      v_assert_ok,
      v_assert_key_type,
      v_assert_key_value,
      v_assert_failure_reason
    FROM public._pay_policy_x_assert_economic_key(
      p_timesheet_id => v_timesheet_id,
      p_key_type => v_direct_key_type,
      p_key_value => v_direct_key_value,
      p_context => 'PRE_DRAFT_RESOLVE_DIRECT_KEY',
      p_authority_scope => 'PRE_DRAFT',
      p_resolution_source => 'PRE_DRAFT_LIVE_DIRECT_KEY',
      p_required => true,
      p_source_json => v_source_json
    ) AS key_check;

    IF COALESCE(v_assert_ok, false) THEN
      v_resolved_key_type := v_assert_key_type;
      v_resolved_key_value := v_assert_key_value;
      v_resolved_source := 'PRE_DRAFT_LIVE_DIRECT_KEY';
    ELSE
      v_failure_reason := v_assert_failure_reason;
    END IF;
  END IF;

  IF v_resolved_key_type IS NULL THEN
    v_work_date_text := COALESCE(
      CASE WHEN p_work_date IS NULL THEN NULL::text ELSE p_work_date::text END,
      NULLIF(BTRIM(COALESCE(v_source_json->>'work_date', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json->>'date', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{source_basis_json,work_date}', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{source_basis_json,date}', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{segment,work_date}', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{segment,date}', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{segment_json,work_date}', '')), ''),
      NULLIF(BTRIM(COALESCE(v_source_json#>>'{segment_json,date}', '')), '')
    );

    v_segment_id_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'segment_id',
      v_source_json#>>'{source_basis_json,segment_id}',
      v_source_json#>>'{segment,segment_id}',
      v_source_json#>>'{segment_json,segment_id}',
      ''
    )), '');

    v_segment_key_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'segment_key',
      v_source_json#>>'{source_basis_json,segment_key}',
      v_source_json#>>'{segment,segment_key}',
      v_source_json#>>'{segment_json,segment_key}',
      ''
    )), '');

    v_segment_stable_key_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'segment_stable_key',
      v_source_json#>>'{source_basis_json,segment_stable_key}',
      v_source_json#>>'{segment,segment_stable_key}',
      v_source_json#>>'{segment_json,segment_stable_key}',
      ''
    )), '');

    v_ref_num_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'ref_num',
      v_source_json#>>'{source_basis_json,ref_num}',
      v_source_json#>>'{segment,ref_num}',
      v_source_json#>>'{segment_json,ref_num}',
      ''
    )), '');

    v_adjustment_id_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'adjustment_id',
      v_source_json->>'adjustment_code',
      v_source_json#>>'{source_basis_json,adjustment_id}',
      v_source_json#>>'{source_basis_json,adjustment_code}',
      ''
    )), '');

    v_additional_code_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'additional_code',
      v_source_json#>>'{source_basis_json,additional_code}',
      ''
    )), '');

    v_expense_code_text := NULLIF(BTRIM(COALESCE(
      v_source_json->>'expense_code',
      v_source_json->>'mileage_code',
      v_source_json#>>'{source_basis_json,expense_code}',
      v_source_json#>>'{source_basis_json,mileage_code}',
      ''
    )), '');

    IF v_item_type IN ('SEGMENT_DELTA', 'SEGMENT', 'TIMESHEET_SEGMENT', 'HOURS') THEN
      IF v_work_date_text IS NOT NULL AND v_work_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
        v_resolved_key_type := 'TS_DAY';
        v_resolved_key_value := v_work_date_text;
        v_resolved_source := 'PRE_DRAFT_LIVE_WORK_DATE';
      END IF;

      IF v_resolved_key_type IS NULL THEN
        IF jsonb_typeof(v_source_json->'segments') = 'array' THEN
          v_segments_json := v_source_json->'segments';
        ELSIF jsonb_typeof(v_source_json#>'{actual_schedule_json,segments}') = 'array' THEN
          v_segments_json := v_source_json#>'{actual_schedule_json,segments}';
        ELSIF jsonb_typeof(v_source_json#>'{schedule_json,segments}') = 'array' THEN
          v_segments_json := v_source_json#>'{schedule_json,segments}';
        ELSE
          v_segments_json := '[]'::jsonb;
        END IF;

        IF jsonb_array_length(v_segments_json) > 0 THEN
          SELECT
            NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), ''),
            true
          INTO
            v_segment_match_date_text,
            v_segment_match_found
          FROM jsonb_array_elements(v_segments_json) AS segment_choice(segment_json)
          WHERE segment_choice.segment_json IS NOT NULL
            AND jsonb_typeof(segment_choice.segment_json) = 'object'
            AND (
              (v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text)
              OR (v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text)
              OR (v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text)
              OR (v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text)
              OR (v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text)
            )
          ORDER BY
            CASE
              WHEN v_work_date_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), '') = v_work_date_text THEN 0
              WHEN v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text THEN 1
              WHEN v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text THEN 2
              WHEN v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text THEN 3
              WHEN v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text THEN 4
              ELSE 9
            END
          LIMIT 1;

          IF COALESCE(v_segment_match_found, false)
             AND v_segment_match_date_text IS NOT NULL
             AND v_segment_match_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
            v_resolved_key_type := 'TS_DAY';
            v_resolved_key_value := v_segment_match_date_text;
            v_resolved_source := 'PRE_DRAFT_LIVE_SEGMENT_DATE';
          ELSIF COALESCE(v_segment_match_found, false) THEN
            v_resolved_key_type := 'TS_TOTAL';
            v_resolved_key_value := 'TOTAL';
            v_resolved_source := 'PRE_DRAFT_LIVE_SEGMENT_TOTAL_FALLBACK';
          END IF;
        END IF;
      END IF;

      IF v_resolved_key_type IS NULL AND v_timesheet_id IS NOT NULL THEN
        SELECT COALESCE(latest_finance.actual_schedule_json, timesheet_row.actual_schedule_json, '{}'::jsonb)
        INTO v_live_schedule_json
        FROM public.timesheets AS timesheet_row
        LEFT JOIN LATERAL (
          SELECT finance_row.actual_schedule_json
          FROM public.timesheets_financials AS finance_row
          WHERE finance_row.timesheet_id = timesheet_row.timesheet_id
            AND finance_row.is_current = true
          ORDER BY finance_row.computed_at_utc DESC, finance_row.id DESC
          LIMIT 1
        ) AS latest_finance ON true
        WHERE timesheet_row.timesheet_id = v_timesheet_id
        LIMIT 1;

        IF jsonb_typeof(v_live_schedule_json->'segments') = 'array' THEN
          v_segments_json := v_live_schedule_json->'segments';
        ELSE
          v_segments_json := '[]'::jsonb;
        END IF;

        IF jsonb_array_length(v_segments_json) > 0 THEN
          v_segment_match_found := false;
          v_segment_match_date_text := NULL::text;

          SELECT
            NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'date', segment_choice.segment_json->>'work_date', '')), ''),
            true
          INTO
            v_segment_match_date_text,
            v_segment_match_found
          FROM jsonb_array_elements(v_segments_json) AS segment_choice(segment_json)
          WHERE segment_choice.segment_json IS NOT NULL
            AND jsonb_typeof(segment_choice.segment_json) = 'object'
            AND (
              (v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text)
              OR (v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text)
              OR (v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text)
              OR (v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text)
            )
          ORDER BY
            CASE
              WHEN v_ref_num_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'ref_num', '')), '') = v_ref_num_text THEN 1
              WHEN v_segment_id_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_id', '')), '') = v_segment_id_text THEN 2
              WHEN v_segment_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_key', segment_choice.segment_json->>'segment_id', '')), '') = v_segment_key_text THEN 3
              WHEN v_segment_stable_key_text IS NOT NULL AND NULLIF(BTRIM(COALESCE(segment_choice.segment_json->>'segment_stable_key', segment_choice.segment_json->>'segment_id', segment_choice.segment_json->>'segment_key', '')), '') = v_segment_stable_key_text THEN 4
              ELSE 9
            END
          LIMIT 1;

          IF COALESCE(v_segment_match_found, false)
             AND v_segment_match_date_text IS NOT NULL
             AND v_segment_match_date_text ~ '^\d{4}-\d{2}-\d{2}$' THEN
            v_resolved_key_type := 'TS_DAY';
            v_resolved_key_value := v_segment_match_date_text;
            v_resolved_source := 'PRE_DRAFT_LIVE_TIMESHEET_SEGMENT_DATE';
          ELSIF COALESCE(v_segment_match_found, false) THEN
            v_resolved_key_type := 'TS_TOTAL';
            v_resolved_key_value := 'TOTAL';
            v_resolved_source := 'PRE_DRAFT_LIVE_TIMESHEET_SEGMENT_TOTAL_FALLBACK';
          END IF;
        END IF;
      END IF;

      IF v_resolved_key_type IS NULL AND v_timesheet_id IS NOT NULL THEN
        SELECT COALESCE(timesheet_row.worked_start_iso::date::text, timesheet_row.scheduled_start_iso::date::text)
        INTO v_timesheet_start_date_text
        FROM public.timesheets AS timesheet_row
        WHERE timesheet_row.timesheet_id = v_timesheet_id
        LIMIT 1;

        IF v_timesheet_start_date_text IS NOT NULL THEN
          v_resolved_key_type := 'TS_DAY';
          v_resolved_key_value := v_timesheet_start_date_text;
          v_resolved_source := 'PRE_DRAFT_LIVE_TIMESHEET_START_DATE';
        END IF;
      END IF;

      IF v_resolved_key_type IS NULL THEN
        v_resolved_key_type := 'TS_TOTAL';
        v_resolved_key_value := 'TOTAL';
        v_resolved_source := 'PRE_DRAFT_LIVE_TOTAL_FALLBACK';
      END IF;

    ELSIF v_item_type = 'ADJUSTMENT_DELTA' AND v_adjustment_id_text IS NOT NULL THEN
      v_resolved_key_type := 'ADJUSTMENT_CODE';
      v_resolved_key_value := v_adjustment_id_text;
      v_resolved_source := 'PRE_DRAFT_LIVE_ADJUSTMENT_KEY';

    ELSIF v_item_type = 'EXPENSE_DELTA' AND v_additional_code_text IS NOT NULL THEN
      v_resolved_key_type := 'ADDITIONAL_CODE';
      v_resolved_key_value := UPPER(v_additional_code_text);
      v_resolved_source := 'PRE_DRAFT_LIVE_ADDITIONAL_CODE';

    ELSIF v_item_type IN ('EXPENSE_DELTA', 'MILEAGE_DELTA') AND v_expense_code_text IS NOT NULL THEN
      v_resolved_key_type := 'EXPENSE_CODE';
      v_resolved_key_value := UPPER(v_expense_code_text);
      v_resolved_source := 'PRE_DRAFT_LIVE_EXPENSE_CODE';
    END IF;
  END IF;

  SELECT
    key_check.ok,
    key_check.key_type,
    key_check.key_value,
    key_check.failure_reason
  INTO
    v_assert_ok,
    v_assert_key_type,
    v_assert_key_value,
    v_assert_failure_reason
  FROM public._pay_policy_x_assert_economic_key(
    p_timesheet_id => v_timesheet_id,
    p_key_type => v_resolved_key_type,
    p_key_value => v_resolved_key_value,
    p_context => 'PRE_DRAFT_RESOLVE_FINAL',
    p_authority_scope => 'PRE_DRAFT',
    p_resolution_source => COALESCE(v_resolved_source, 'PRE_DRAFT_KEY_RESOLUTION_FAILED'),
    p_required => true,
    p_source_json => v_source_json
  ) AS key_check;

  IF COALESCE(v_assert_ok, false) THEN
    timesheet_id := v_timesheet_id;
    key_type := v_assert_key_type;
    key_value := v_assert_key_value;
    key_resolution_source := v_resolved_source;
    key_resolution_failure_reason := NULL::text;
  ELSE
    timesheet_id := v_timesheet_id;
    key_type := NULL::text;
    key_value := NULL::text;
    key_resolution_source := 'KEY_RESOLUTION_FAILED';
    key_resolution_failure_reason := COALESCE(v_failure_reason, v_assert_failure_reason, 'PRE_DRAFT_KEY_RESOLUTION_FAILED');
  END IF;

  RETURN NEXT;
END;
$function$;

-- _pay_rail_state_money_movement_classify(text,text,jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._pay_rail_state_money_movement_classify(p_transfer_status text DEFAULT NULL::text, p_rail_state text DEFAULT NULL::text, p_event_payload_json jsonb DEFAULT '{}'::jsonb, p_provider_meta_json jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(cash_state text, normalised_transfer_status text, is_final_money_moved boolean, is_terminal_no_money boolean, is_pending_non_final boolean, completed_at_allowed boolean, reason text, support_details_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_event_payload_json jsonb := '{}'::jsonb;
  v_provider_meta_json jsonb := '{}'::jsonb;
  v_transfer_status_upper text := NULL::text;
  v_rail_state_upper text := NULL::text;
  v_event_status_upper text := NULL::text;
  v_provider_status_upper text := NULL::text;
  v_event_outcome_upper text := NULL::text;
  v_provider_outcome_upper text := NULL::text;
  v_error_code_upper text := NULL::text;
  v_reason_code_upper text := NULL::text;
  v_provider_key_upper text := NULL::text;
  v_provider_event_type_upper text := NULL::text;
  v_provider_event_transport_upper text := NULL::text;
  v_data_new_state_upper text := NULL::text;
  v_data_state_upper text := NULL::text;
  v_old_state_upper text := NULL::text;
  v_new_state_upper text := NULL::text;
  v_normalised_state_upper text := NULL::text;
  v_primary_status_upper text := NULL::text;
  v_status_terms text[] := ARRAY[]::text[];
  v_final_status_present boolean := false;
  v_terminal_status_present boolean := false;
  v_pending_status_present boolean := false;
  v_explicit_final_paid_evidence boolean := false;
  v_explicit_terminal_no_money_evidence boolean := false;
  v_explicit_pending_evidence boolean := false;
  v_ambiguous_commit_or_execute boolean := false;
  v_ambiguous_return_or_revert boolean := false;
  v_bool_true_terms text[] := ARRAY['true','t','yes','y','1','final','settled','paid','completed','success','succeeded'];
  v_bool_false_terms text[] := ARRAY['false','f','no','n','0'];
BEGIN
  IF p_event_payload_json IS NOT NULL AND jsonb_typeof(p_event_payload_json) = 'object' THEN
    v_event_payload_json := p_event_payload_json;
  END IF;

  IF p_provider_meta_json IS NOT NULL AND jsonb_typeof(p_provider_meta_json) = 'object' THEN
    v_provider_meta_json := p_provider_meta_json;
  END IF;

  v_transfer_status_upper := upper(NULLIF(btrim(COALESCE(p_transfer_status, '')), ''));
  v_rail_state_upper := upper(NULLIF(btrim(COALESCE(p_rail_state, '')), ''));

  v_event_status_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'status',
    v_event_payload_json->>'normalised_state',
    v_event_payload_json->>'normalized_state',
    v_event_payload_json->>'new_state',
    v_event_payload_json->>'state',
    v_event_payload_json->>'rail_state',
    v_event_payload_json->>'payment_status',
    v_event_payload_json->>'provider_status',
    v_event_payload_json #>> '{payment,status}',
    v_event_payload_json #>> '{provider,status}',
    v_event_payload_json #>> '{data,new_state}',
    v_event_payload_json #>> '{data,state}',
    v_event_payload_json #>> '{data,status}',
    ''
  )), ''));

  v_provider_status_upper := upper(NULLIF(btrim(COALESCE(
    v_provider_meta_json->>'status',
    v_provider_meta_json->>'normalised_state',
    v_provider_meta_json->>'normalized_state',
    v_provider_meta_json->>'new_state',
    v_provider_meta_json->>'state',
    v_provider_meta_json->>'rail_state',
    v_provider_meta_json->>'payment_status',
    v_provider_meta_json->>'provider_status',
    v_provider_meta_json #>> '{payment,status}',
    v_provider_meta_json #>> '{provider,status}',
    v_provider_meta_json #>> '{data,new_state}',
    v_provider_meta_json #>> '{data,state}',
    v_provider_meta_json #>> '{data,status}',
    ''
  )), ''));

  v_event_outcome_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'outcome',
    v_event_payload_json->>'cash_state',
    v_event_payload_json->>'money_movement_state',
    v_event_payload_json->>'mapping_status',
    v_event_payload_json #>> '{payment,outcome}',
    v_event_payload_json #>> '{provider,outcome}',
    v_event_payload_json #>> '{data,outcome}',
    ''
  )), ''));

  v_provider_outcome_upper := upper(NULLIF(btrim(COALESCE(
    v_provider_meta_json->>'outcome',
    v_provider_meta_json->>'cash_state',
    v_provider_meta_json->>'money_movement_state',
    v_provider_meta_json->>'mapping_status',
    v_provider_meta_json #>> '{payment,outcome}',
    v_provider_meta_json #>> '{provider,outcome}',
    v_provider_meta_json #>> '{data,outcome}',
    ''
  )), ''));

  v_error_code_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'error_code',
    v_event_payload_json->>'failure_code',
    v_event_payload_json->>'decline_code',
    v_event_payload_json #>> '{error,code}',
    v_event_payload_json #>> '{failure,code}',
    v_provider_meta_json->>'error_code',
    v_provider_meta_json->>'failure_code',
    v_provider_meta_json->>'decline_code',
    v_provider_meta_json #>> '{error,code}',
    v_provider_meta_json #>> '{failure,code}',
    ''
  )), ''));

  v_reason_code_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'reason_code',
    v_event_payload_json->>'reason',
    v_event_payload_json->>'failed_reason',
    v_event_payload_json #>> '{reason,code}',
    v_provider_meta_json->>'reason_code',
    v_provider_meta_json->>'reason',
    v_provider_meta_json->>'failed_reason',
    v_provider_meta_json #>> '{reason,code}',
    ''
  )), ''));


  v_provider_key_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'provider_key',
    v_event_payload_json->>'provider',
    v_event_payload_json->>'rail_provider',
    v_provider_meta_json->>'provider_key',
    v_provider_meta_json->>'provider',
    v_provider_meta_json->>'rail_provider',
    ''
  )), ''));

  v_provider_event_type_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'provider_event_type',
    v_event_payload_json->>'event_type',
    v_event_payload_json->>'event',
    v_provider_meta_json->>'provider_event_type',
    v_provider_meta_json->>'event_type',
    v_provider_meta_json->>'event',
    ''
  )), ''));

  v_provider_event_transport_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'provider_event_transport',
    v_event_payload_json->>'event_source',
    v_event_payload_json->>'source',
    v_provider_meta_json->>'provider_event_transport',
    v_provider_meta_json->>'event_source',
    v_provider_meta_json->>'source',
    ''
  )), ''));

  v_data_new_state_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json #>> '{data,new_state}',
    v_provider_meta_json #>> '{data,new_state}',
    v_event_payload_json->>'new_state',
    v_provider_meta_json->>'new_state',
    ''
  )), ''));

  v_data_state_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json #>> '{data,state}',
    v_provider_meta_json #>> '{data,state}',
    v_event_payload_json->>'state',
    v_provider_meta_json->>'state',
    ''
  )), ''));

  v_old_state_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json #>> '{data,old_state}',
    v_provider_meta_json #>> '{data,old_state}',
    ''
  )), ''));

  v_new_state_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json #>> '{data,new_state}',
    v_provider_meta_json #>> '{data,new_state}',
    v_event_payload_json->>'new_state',
    v_provider_meta_json->>'new_state',
    ''
  )), ''));

  v_normalised_state_upper := upper(NULLIF(btrim(COALESCE(
    v_event_payload_json->>'normalised_state',
    v_event_payload_json->>'normalized_state',
    v_provider_meta_json->>'normalised_state',
    v_provider_meta_json->>'normalized_state',
    ''
  )), ''));

  v_status_terms := ARRAY[
    v_transfer_status_upper,
    v_rail_state_upper,
    v_event_status_upper,
    v_provider_status_upper,
    v_event_outcome_upper,
    v_provider_outcome_upper,
    v_error_code_upper,
    v_reason_code_upper,
    v_data_new_state_upper,
    v_data_state_upper,
    v_old_state_upper,
    v_new_state_upper,
    v_normalised_state_upper
  ];

  SELECT COALESCE(status_terms.status_text, 'UNKNOWN')
  INTO v_primary_status_upper
  FROM unnest(v_status_terms) AS status_terms(status_text)
  WHERE status_terms.status_text IS NOT NULL
  LIMIT 1;

  v_final_status_present := EXISTS (
    SELECT 1
    FROM unnest(ARRAY[
      v_transfer_status_upper,
      v_rail_state_upper,
      v_event_status_upper,
      v_provider_status_upper,
      v_event_outcome_upper,
      v_provider_outcome_upper,
      v_error_code_upper,
      v_reason_code_upper,
      v_data_new_state_upper,
      v_data_state_upper,
      v_new_state_upper,
      v_normalised_state_upper
    ]) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'PAID',
      'SETTLED',
      'COMPLETED',
      'SUCCESS',
      'SUCCEEDED',
      'PAYMENT_PAID',
      'PAYMENT_SETTLED',
      'PAYMENT_COMPLETED',
      'FINAL_PAID',
      'MONEY_MOVED',
      'CONFIRMED_PAID',
      'CONFIRMED_SETTLED'
    )
  );

  v_terminal_status_present := EXISTS (
    SELECT 1
    FROM unnest(ARRAY[
      v_transfer_status_upper,
      v_rail_state_upper,
      v_event_status_upper,
      v_provider_status_upper,
      v_event_outcome_upper,
      v_provider_outcome_upper,
      v_error_code_upper,
      v_reason_code_upper,
      v_data_new_state_upper,
      v_data_state_upper,
      v_new_state_upper,
      v_normalised_state_upper
    ]) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'FAILED',
      'FAILURE',
      'REJECTED',
      'DECLINED',
      'CANCELLED',
      'CANCELED',
      'SUBMISSION_FAILED',
      'FAILED_BEFORE_COMMIT',
      'CANCELLED_BEFORE_RELEASE',
      'CANCELED_BEFORE_RELEASE',
      'WRONG_BANK',
      'WRONG_BANK_DETAILS',
      'NO_MONEY',
      'NO_PAYMENT_MADE',
      'NOT_PAID',
      'REVERSED_BEFORE_RELEASE',
      'INSUFFICIENT_FUNDS',
      'ACCOUNT_CLOSED',
      'INVALID_ACCOUNT',
      'INVALID_SORT_CODE',
      'BANK_REJECTED'
    )
  );

  v_pending_status_present := EXISTS (
    SELECT 1
    FROM unnest(ARRAY[
      v_transfer_status_upper,
      v_rail_state_upper,
      v_event_status_upper,
      v_provider_status_upper,
      v_event_outcome_upper,
      v_provider_outcome_upper,
      v_error_code_upper,
      v_reason_code_upper,
      v_data_new_state_upper,
      v_data_state_upper,
      v_new_state_upper,
      v_normalised_state_upper
    ]) AS status_terms(status_text)
    WHERE status_terms.status_text IN (
      'CREATED',
      'ACCEPTED',
      'SCHEDULED',
      'SUBMITTED',
      'SENT',
      'PROCESSING',
      'IN_FLIGHT',
      'IN-FLIGHT',
      'INFLIGHT',
      'QUEUED',
      'PENDING',
      'PENDING_SETTLEMENT',
      'PENDING_CONFIRMATION',
      'PENDING_SUBMISSION',
      'AUTHORISED',
      'AUTHORIZED',
      'AWAITING_CONFIRMATION',
      'AWAITING_SETTLEMENT'
    )
  );

  v_ambiguous_commit_or_execute := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN ('COMMITTED', 'EXECUTED')
  );

  v_ambiguous_return_or_revert := EXISTS (
    SELECT 1
    FROM unnest(v_status_terms) AS status_terms(status_text)
    WHERE status_terms.status_text IN ('RETURNED', 'REVERTED', 'REVERSED')
  );

  v_explicit_final_paid_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'final_paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'is_final_money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'settlement_confirmed', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'settled', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{money_movement,final_paid}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{settlement,confirmed}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'final_paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'is_final_money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'money_moved', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'settlement_confirmed', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'settled', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'paid', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{money_movement,final_paid}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{settlement,confirmed}', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_terminal_no_money_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'no_payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'terminal_no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'wrong_bank', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'cancelled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'canceled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'failed_before_commit', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json #>> '{money_movement,no_payment_made}', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'no_payment_made', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'terminal_no_money', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'wrong_bank', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'cancelled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'canceled_before_release', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'failed_before_commit', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json #>> '{money_movement,no_payment_made}', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_pending_evidence := (
    lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'pending', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'in_flight', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_event_payload_json->>'processing', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'pending', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'in_flight', '')), '')) = ANY(v_bool_true_terms)
    OR lower(NULLIF(btrim(COALESCE(v_provider_meta_json->>'processing', '')), '')) = ANY(v_bool_true_terms)
  );

  v_explicit_final_paid_evidence := COALESCE(v_explicit_final_paid_evidence, false);
  v_explicit_terminal_no_money_evidence := COALESCE(v_explicit_terminal_no_money_evidence, false);
  v_explicit_pending_evidence := COALESCE(v_explicit_pending_evidence, false);

  IF v_explicit_final_paid_evidence OR v_final_status_present THEN
    cash_state := 'FINAL_PAID';
    normalised_transfer_status := 'COMPLETED';
    is_final_money_moved := true;
    is_terminal_no_money := false;
    is_pending_non_final := false;
    completed_at_allowed := true;
    reason := 'Explicit final-paid/provider-settled evidence was found.';
  ELSIF v_explicit_terminal_no_money_evidence OR v_terminal_status_present THEN
    cash_state := 'TERMINAL_NO_MONEY';
    normalised_transfer_status := CASE
      WHEN v_primary_status_upper IN ('CANCELLED', 'CANCELED', 'CANCELLED_BEFORE_RELEASE', 'CANCELED_BEFORE_RELEASE') THEN 'CANCELLED'
      ELSE 'FAILED'
    END;
    is_final_money_moved := false;
    is_terminal_no_money := true;
    is_pending_non_final := false;
    completed_at_allowed := false;
    reason := 'Terminal no-money/failure/cancelled-before-release evidence was found.';
  ELSIF v_explicit_pending_evidence OR v_pending_status_present THEN
    cash_state := 'PENDING_NON_FINAL';
    normalised_transfer_status := CASE
      WHEN v_primary_status_upper IN ('CREATED', 'SCHEDULED', 'SUBMITTED', 'SENT', 'PROCESSING', 'QUEUED', 'ACCEPTED', 'PENDING') THEN CASE WHEN v_primary_status_upper = 'CREATED' THEN 'PENDING' ELSE v_primary_status_upper END
      ELSE 'PENDING'
    END;
    is_final_money_moved := false;
    is_terminal_no_money := false;
    is_pending_non_final := true;
    completed_at_allowed := false;
    reason := 'Pending/non-final provider or rail state was found.';
  ELSE
    cash_state := 'UNKNOWN';
    normalised_transfer_status := COALESCE(v_primary_status_upper, 'UNKNOWN');
    is_final_money_moved := false;
    is_terminal_no_money := false;
    is_pending_non_final := false;
    completed_at_allowed := false;
    reason := CASE
      WHEN v_ambiguous_commit_or_execute THEN 'Bare COMMITTED/EXECUTED evidence is ambiguous and is not final-paid evidence without explicit final settlement metadata.'
      WHEN v_ambiguous_return_or_revert THEN 'RETURNED/REVERTED/REVERSED evidence is ambiguous and is not terminal no-money without explicit no-payment-made evidence.'
      ELSE 'No explicit final-paid, terminal no-money, or pending non-final evidence was found.'
    END;
  END IF;

  support_details_json := jsonb_build_object(
    'transfer_status', p_transfer_status,
    'rail_state', p_rail_state,
    'transfer_status_upper', v_transfer_status_upper,
    'rail_state_upper', v_rail_state_upper,
    'event_status_upper', v_event_status_upper,
    'provider_status_upper', v_provider_status_upper,
    'event_outcome_upper', v_event_outcome_upper,
    'provider_outcome_upper', v_provider_outcome_upper,
    'error_code_upper', v_error_code_upper,
    'reason_code_upper', v_reason_code_upper,
    'primary_status_upper', v_primary_status_upper,
    'final_status_present', v_final_status_present,
    'terminal_status_present', v_terminal_status_present,
    'pending_status_present', v_pending_status_present,
    'explicit_final_paid_evidence', v_explicit_final_paid_evidence,
    'explicit_terminal_no_money_evidence', v_explicit_terminal_no_money_evidence,
    'explicit_pending_evidence', v_explicit_pending_evidence,
    'ambiguous_commit_or_execute', v_ambiguous_commit_or_execute,
    'ambiguous_return_or_revert', v_ambiguous_return_or_revert,
    'committed_or_executed_requires_explicit_final_paid_metadata', true,
    'returned_or_reverted_requires_explicit_no_money_metadata', true,
    'provider_key_upper', v_provider_key_upper,
    'provider_event_type_upper', v_provider_event_type_upper,
    'provider_event_transport_upper', v_provider_event_transport_upper,
    'data_new_state_upper', v_data_new_state_upper,
    'data_state_upper', v_data_state_upper,
    'old_state_upper', v_old_state_upper,
    'new_state_upper', v_new_state_upper,
    'normalised_state_upper', v_normalised_state_upper,
    'is_unknown_or_review', (cash_state = 'UNKNOWN'),
    'can_no_money_unwind', (cash_state = 'TERMINAL_NO_MONEY'),
    'reason_code', CASE
      WHEN cash_state = 'FINAL_PAID' THEN 'FINAL_PAID_EXPLICIT'
      WHEN cash_state = 'TERMINAL_NO_MONEY' THEN 'TERMINAL_NO_MONEY_EXPLICIT'
      WHEN cash_state = 'PENDING_NON_FINAL' THEN 'PENDING_NON_FINAL'
      WHEN v_ambiguous_commit_or_execute THEN 'AMBIGUOUS_COMMITTED_OR_EXECUTED'
      WHEN v_ambiguous_return_or_revert THEN 'AMBIGUOUS_RETURNED_OR_REVERTED'
      ELSE 'UNKNOWN_NO_EXPLICIT_PROVIDER_OUTCOME'
    END,
    'reason_label', reason,
    'success_only_status_update', (cash_state = 'FINAL_PAID'),
    'event_payload_keys', CASE
      WHEN jsonb_typeof(v_event_payload_json) = 'object' THEN COALESCE((SELECT jsonb_agg(event_keys.key_name ORDER BY event_keys.key_name) FROM jsonb_object_keys(v_event_payload_json) AS event_keys(key_name)), '[]'::jsonb)
      ELSE '[]'::jsonb
    END,
    'provider_meta_keys', CASE
      WHEN jsonb_typeof(v_provider_meta_json) = 'object' THEN COALESCE((SELECT jsonb_agg(meta_keys.key_name ORDER BY meta_keys.key_name) FROM jsonb_object_keys(v_provider_meta_json) AS meta_keys(key_name)), '[]'::jsonb)
      ELSE '[]'::jsonb
    END
  );

  RETURN NEXT;
END;
$function$;

-- _pay_repayment_schedule_rebase_for_snooze(uuid,date)
CREATE OR REPLACE FUNCTION public._pay_repayment_schedule_rebase_for_snooze(p_finance_case_id uuid, p_snooze_until_date date)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_case public.pay_advances%rowtype;
  v_before_schedule_json jsonb := '[]'::jsonb;
  v_after_schedule_json jsonb := '[]'::jsonb;
  v_before_next_due_week_start date := null;
  v_after_next_due_week_start date := null;
  v_requested_week_start date := null;
  v_target_week_start date := null;
  v_installment_count integer := 0;
BEGIN
  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_repayment_schedule_rebase_for_snooze: finance_case_id is required'
    )::text;
  END IF;

  IF p_snooze_until_date IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'SNOOZE_UNTIL_DATE_REQUIRED',
      'message', '_pay_repayment_schedule_rebase_for_snooze: snooze_until_date is required'
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  LIMIT 1
  FOR UPDATE;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_repayment_schedule_rebase_for_snooze: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  IF v_case.case_type NOT IN (
    'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
    'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
  ) THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'CASE_TYPE_NOT_SNOOZEABLE',
      'message', '_pay_repayment_schedule_rebase_for_snooze: only repayment-type finance cases may be rebased for snooze',
      'finance_case_id', p_finance_case_id::text,
      'case_type', v_case.case_type::text
    )::text;
  END IF;

  v_before_schedule_json := coalesce(v_case.schedule_json, '[]'::jsonb);
  v_before_next_due_week_start := v_case.next_due_week_start;
  v_requested_week_start := public._pay_week_start_monday(p_snooze_until_date);

  IF v_before_next_due_week_start IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'NEXT_DUE_MISSING',
      'message', '_pay_repayment_schedule_rebase_for_snooze: next_due_week_start is required on the finance case'
    )::text;
  END IF;

  IF v_requested_week_start < v_before_next_due_week_start THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
      'code', 'SNOOZE_DATE_BEFORE_NEXT_DUE',
      'message', '_pay_repayment_schedule_rebase_for_snooze: snooze_until_date cannot move the repayment schedule earlier than the current next due week',
      'finance_case_id', p_finance_case_id::text,
      'current_next_due_week_start', v_before_next_due_week_start::text,
      'requested_week_start', v_requested_week_start::text
    )::text;
  END IF;

  v_target_week_start := CASE
    WHEN v_requested_week_start = v_before_next_due_week_start THEN (v_before_next_due_week_start + 7)
    ELSE v_requested_week_start
  END;

  WITH schedule_rows AS (
    SELECT
      row_number() OVER (ORDER BY (elem.value ->> 'week_start')::date ASC, elem.ordinality ASC) AS seq_no,
      round(coalesce((elem.value ->> 'amount')::numeric, 0), 2) AS amount_value
    FROM jsonb_array_elements(v_before_schedule_json) WITH ORDINALITY AS elem(value, ordinality)
    WHERE coalesce((elem.value ->> 'amount')::numeric, 0) < 0
  )
  SELECT
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'week_start', (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date,
          'amount', sr.amount_value
        )
        ORDER BY (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date ASC
      ),
      '[]'::jsonb
    ),
    count(*)::integer
  INTO v_after_schedule_json, v_installment_count
  FROM schedule_rows AS sr;

  v_after_next_due_week_start := CASE WHEN v_installment_count > 0 THEN v_target_week_start ELSE null END;

  UPDATE public.pay_advances AS pa
  SET schedule_json = v_after_schedule_json,
      next_due_week_start = v_after_next_due_week_start,
      updated_at = now()
  WHERE pa.id = p_finance_case_id;

  RETURN jsonb_build_object(
    'ok', true,
    'finance_case_id', p_finance_case_id::text,
    'schedule_before_snooze_json', v_before_schedule_json,
    'next_due_week_start_before_snooze', case when v_before_next_due_week_start is null then null else v_before_next_due_week_start::text end,
    'requested_week_start', v_requested_week_start::text,
    'schedule_after_snooze_json', v_after_schedule_json,
    'next_due_week_start_after_snooze', case when v_after_next_due_week_start is null then null else v_after_next_due_week_start::text end,
    'installment_count', v_installment_count
  );
END;
$function$;

-- _pay_repayment_schedule_restore_after_snooze_clear(uuid,date)
CREATE OR REPLACE FUNCTION public._pay_repayment_schedule_restore_after_snooze_clear(p_finance_case_id uuid, p_effective_from_date date)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_case public.pay_advances%rowtype;
  v_before_schedule_json jsonb := '[]'::jsonb;
  v_after_schedule_json jsonb := '[]'::jsonb;
  v_before_next_due_week_start date := null;
  v_after_next_due_week_start date := null;
  v_requested_week_start date := null;
  v_current_operating_week_start date := null;
  v_target_week_start date := null;
  v_installment_count integer := 0;
BEGIN
  IF p_finance_case_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', '_pay_repayment_schedule_restore_after_snooze_clear: finance_case_id is required'
    )::text;
  END IF;

  IF p_effective_from_date IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
      'code', 'EFFECTIVE_FROM_DATE_REQUIRED',
      'message', '_pay_repayment_schedule_restore_after_snooze_clear: effective_from_date is required'
    )::text;
  END IF;

  SELECT pa.*
  INTO v_case
  FROM public.pay_advances AS pa
  WHERE pa.id = p_finance_case_id
  LIMIT 1
  FOR UPDATE;

  IF v_case.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', '_pay_repayment_schedule_restore_after_snooze_clear: finance case not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  END IF;

  IF v_case.case_type NOT IN (
    'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
    'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
  ) THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
      'code', 'CASE_TYPE_NOT_SNOOZEABLE',
      'message', '_pay_repayment_schedule_restore_after_snooze_clear: only repayment-type finance cases may be restored after snooze clear',
      'finance_case_id', p_finance_case_id::text,
      'case_type', v_case.case_type::text
    )::text;
  END IF;

  v_before_schedule_json := coalesce(v_case.schedule_json, '[]'::jsonb);
  v_before_next_due_week_start := v_case.next_due_week_start;
  v_requested_week_start := public._pay_week_start_monday(p_effective_from_date);
  v_current_operating_week_start := public._pay_week_start_monday((now() at time zone 'Europe/London')::date);

  IF v_requested_week_start < v_current_operating_week_start THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
      'code', 'EFFECTIVE_FROM_DATE_BEFORE_NOW',
      'message', '_pay_repayment_schedule_restore_after_snooze_clear: effective_from_date cannot restore the repayment schedule earlier than the current operating week',
      'finance_case_id', p_finance_case_id::text,
      'requested_week_start', v_requested_week_start::text,
      'current_operating_week_start', v_current_operating_week_start::text
    )::text;
  END IF;

  v_target_week_start := v_requested_week_start;

  WITH schedule_rows AS (
    SELECT
      row_number() OVER (ORDER BY (elem.value ->> 'week_start')::date ASC, elem.ordinality ASC) AS seq_no,
      round(coalesce((elem.value ->> 'amount')::numeric, 0), 2) AS amount_value
    FROM jsonb_array_elements(v_before_schedule_json) WITH ORDINALITY AS elem(value, ordinality)
    WHERE coalesce((elem.value ->> 'amount')::numeric, 0) < 0
  )
  SELECT
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'week_start', (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date,
          'amount', sr.amount_value
        )
        ORDER BY (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date ASC
      ),
      '[]'::jsonb
    ),
    count(*)::integer
  INTO v_after_schedule_json, v_installment_count
  FROM schedule_rows AS sr;

  v_after_next_due_week_start := CASE WHEN v_installment_count > 0 THEN v_target_week_start ELSE null END;

  UPDATE public.pay_advances AS pa
  SET schedule_json = v_after_schedule_json,
      next_due_week_start = v_after_next_due_week_start,
      updated_at = now()
  WHERE pa.id = p_finance_case_id;

  RETURN jsonb_build_object(
    'ok', true,
    'finance_case_id', p_finance_case_id::text,
    'schedule_before_restore_json', v_before_schedule_json,
    'next_due_week_start_before_restore', case when v_before_next_due_week_start is null then null else v_before_next_due_week_start::text end,
    'requested_week_start', v_requested_week_start::text,
    'current_operating_week_start', v_current_operating_week_start::text,
    'schedule_after_restore_json', v_after_schedule_json,
    'next_due_week_start_after_restore', case when v_after_next_due_week_start is null then null else v_after_next_due_week_start::text end,
    'installment_count', v_installment_count
  );
END;
$function$;

-- _pay_reserved_components(uuid[],uuid)
CREATE OR REPLACE FUNCTION public._pay_reserved_components(p_timesheet_ids uuid[], p_exclude_pay_batch_id uuid)
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, amount_ex_vat numeric, amount_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
WITH
inp AS (
  SELECT COALESCE(
    (
      SELECT array_agg(DISTINCT input_values.timesheet_id_value ORDER BY input_values.timesheet_id_value)
      FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_values(timesheet_id_value)
      WHERE input_values.timesheet_id_value IS NOT NULL
    ),
    ARRAY[]::uuid[]
  ) AS ts_ids
),
rotation_scope_rows AS (
  SELECT
    scope_rows.requested_timesheet_id,
    scope_rows.booking_id,
    scope_rows.canonical_timesheet_id,
    scope_rows.family_timesheet_id,
    scope_rows.family_is_current,
    scope_rows.family_version,
    scope_rows.requested_is_canonical
  FROM inp AS input_scope
  JOIN public._pay_timesheet_rotation_scope(input_scope.ts_ids) AS scope_rows
    ON true
),
rotation_scope_keyed AS (
  SELECT
    rotation_scope_rows.requested_timesheet_id,
    rotation_scope_rows.booking_id,
    rotation_scope_rows.canonical_timesheet_id,
    rotation_scope_rows.family_timesheet_id,
    rotation_scope_rows.family_is_current,
    rotation_scope_rows.family_version,
    rotation_scope_rows.requested_is_canonical,
    COALESCE(rotation_scope_rows.booking_id, rotation_scope_rows.requested_timesheet_id::text) AS scope_family_key
  FROM rotation_scope_rows
  WHERE rotation_scope_rows.requested_timesheet_id IS NOT NULL
),
projection_targets AS (
  SELECT
    rotation_scope_keyed.scope_family_key,
    COALESCE(
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (
          WHERE COALESCE(rotation_scope_keyed.requested_is_canonical, false) = true
            AND rotation_scope_keyed.canonical_timesheet_id IS NOT NULL
        )
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.requested_timesheet_id ORDER BY rotation_scope_keyed.requested_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.requested_timesheet_id IS NOT NULL)
      )[1],
      (
        ARRAY_AGG(DISTINCT rotation_scope_keyed.canonical_timesheet_id ORDER BY rotation_scope_keyed.canonical_timesheet_id)
        FILTER (WHERE rotation_scope_keyed.canonical_timesheet_id IS NOT NULL)
      )[1]
    ) AS projected_timesheet_id
  FROM rotation_scope_keyed
  GROUP BY rotation_scope_keyed.scope_family_key
),
family_to_projection AS (
  SELECT DISTINCT
    rotation_scope_keyed.family_timesheet_id,
    projection_targets.projected_timesheet_id
  FROM rotation_scope_keyed
  JOIN projection_targets
    ON projection_targets.scope_family_key = rotation_scope_keyed.scope_family_key
  WHERE rotation_scope_keyed.family_timesheet_id IS NOT NULL
    AND projection_targets.projected_timesheet_id IS NOT NULL
),
active_batch_item_ids AS (
  SELECT DISTINCT
    pay_batch_item_row.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS projected_timesheet_id
  FROM family_to_projection
  JOIN public.pay_batch_items AS pay_batch_item_row
    ON pay_batch_item_row.timesheet_id = family_to_projection.family_timesheet_id
  JOIN public.pay_batch_candidates AS pay_batch_candidate_row
    ON pay_batch_candidate_row.id = pay_batch_item_row.pay_batch_candidate_id
  JOIN public.pay_batches AS pay_batch_row
    ON pay_batch_row.id = pay_batch_candidate_row.pay_batch_id
  LEFT JOIN public.pay_bank_transfers AS pay_bank_transfer_row
    ON pay_bank_transfer_row.id = pay_batch_item_row.pay_bank_transfer_id
  WHERE pay_batch_item_row.timesheet_id IS NOT NULL
    AND COALESCE(pay_batch_item_row.is_voided, false) = false
    AND upper(coalesce(pay_batch_item_row.pay_channel, '')) IN ('PAYE','UMBRELLA')
    AND public._pay_batch_status_is_active_reservation(pay_batch_row.status)
    AND (p_exclude_pay_batch_id IS NULL OR pay_batch_row.id <> p_exclude_pay_batch_id)
    AND UPPER(BTRIM(COALESCE(pay_batch_item_row.item_type, ''))) IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_correction_row
      WHERE applied_correction_row.pay_batch_item_id = pay_batch_item_row.id
        AND applied_correction_row.status = 'APPLIED'
        AND applied_correction_row.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
    )
    AND NOT (
      upper(btrim(coalesce(pay_batch_candidate_row.settlement_status, ''))) = 'SETTLED'
      OR pay_batch_candidate_row.settled_at_utc IS NOT NULL
      OR upper(btrim(coalesce(pay_bank_transfer_row.status, ''))) = 'COMPLETED'
      OR pay_bank_transfer_row.completed_at_utc IS NOT NULL
    )
),
open_correction_exact_item_ids AS (
  SELECT DISTINCT
    open_correction_request_row.id AS correction_request_id,
    open_correction_request_row.pay_batch_id AS pay_batch_id,
    parsed_exact_item_ids.pay_batch_item_id AS pay_batch_item_id
  FROM public.pay_payment_correction_requests AS open_correction_request_row
  JOIN LATERAL (
    SELECT DISTINCT
      exact_item_text_values.item_id_text::uuid AS pay_batch_item_id
    FROM (
      SELECT item_array_values.item_id_text
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(open_correction_request_row.plan_json->'selected_pay_batch_item_ids') = 'array'
            THEN open_correction_request_row.plan_json->'selected_pay_batch_item_ids'
          WHEN jsonb_typeof(open_correction_request_row.plan_json#>'{selection,selected_pay_batch_item_ids}') = 'array'
            THEN open_correction_request_row.plan_json#>'{selection,selected_pay_batch_item_ids}'
          WHEN jsonb_typeof(open_correction_request_row.plan_json#>'{selection,pay_batch_item_ids}') = 'array'
            THEN open_correction_request_row.plan_json#>'{selection,pay_batch_item_ids}'
          WHEN jsonb_typeof(open_correction_request_row.plan_json#>'{work_expansion_plan,selected_pay_batch_item_ids}') = 'array'
            THEN open_correction_request_row.plan_json#>'{work_expansion_plan,selected_pay_batch_item_ids}'
          WHEN jsonb_typeof(open_correction_request_row.selection_json->'selected_pay_batch_item_ids') = 'array'
            THEN open_correction_request_row.selection_json->'selected_pay_batch_item_ids'
          WHEN jsonb_typeof(open_correction_request_row.selection_json->'pay_batch_item_ids') = 'array'
            THEN open_correction_request_row.selection_json->'pay_batch_item_ids'
          WHEN jsonb_typeof(open_correction_request_row.selection_json->'expected_pay_batch_item_ids') = 'array'
            THEN open_correction_request_row.selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS item_array_values(item_id_text)

      UNION ALL

      SELECT open_correction_request_row.selection_json->>'pay_batch_item_id'
      WHERE open_correction_request_row.selection_json ? 'pay_batch_item_id'

      UNION ALL

      SELECT open_correction_request_row.selection_json->>'expected_pay_batch_item_id'
      WHERE open_correction_request_row.selection_json ? 'expected_pay_batch_item_id'
    ) AS exact_item_text_values
    WHERE nullif(btrim(coalesce(exact_item_text_values.item_id_text, '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(exact_item_text_values.item_id_text, '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_exact_item_ids
    ON true
  JOIN public.pay_batch_items AS exact_pay_batch_item_row
    ON exact_pay_batch_item_row.id = parsed_exact_item_ids.pay_batch_item_id
  JOIN public.pay_batch_candidates AS exact_pay_batch_candidate_row
    ON exact_pay_batch_candidate_row.id = exact_pay_batch_item_row.pay_batch_candidate_id
   AND exact_pay_batch_candidate_row.pay_batch_id = open_correction_request_row.pay_batch_id
  WHERE open_correction_request_row.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING','BLOCKED')
    AND open_correction_request_row.correction_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')
    AND (p_exclude_pay_batch_id IS NULL OR open_correction_request_row.pay_batch_id <> p_exclude_pay_batch_id)
),
open_correction_hold_item_ids AS (
  SELECT DISTINCT
    open_correction_exact_item_ids.pay_batch_item_id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS projected_timesheet_id
  FROM open_correction_exact_item_ids
  JOIN public.pay_batch_items AS selected_pay_batch_item_row
    ON selected_pay_batch_item_row.id = open_correction_exact_item_ids.pay_batch_item_id
  JOIN family_to_projection
    ON family_to_projection.family_timesheet_id = selected_pay_batch_item_row.timesheet_id
  JOIN public.pay_batch_candidates AS selected_pay_batch_candidate_row
    ON selected_pay_batch_candidate_row.id = selected_pay_batch_item_row.pay_batch_candidate_id
   AND selected_pay_batch_candidate_row.pay_batch_id = open_correction_exact_item_ids.pay_batch_id
  LEFT JOIN public.pay_bank_transfers AS selected_pay_bank_transfer_row
    ON selected_pay_bank_transfer_row.id = selected_pay_batch_item_row.pay_bank_transfer_id
  WHERE selected_pay_batch_item_row.timesheet_id IS NOT NULL
    AND COALESCE(selected_pay_batch_item_row.is_voided, false) = false
    AND upper(coalesce(selected_pay_batch_item_row.pay_channel, '')) IN ('PAYE','UMBRELLA')
    AND (p_exclude_pay_batch_id IS NULL OR selected_pay_batch_candidate_row.pay_batch_id <> p_exclude_pay_batch_id)
    AND UPPER(BTRIM(COALESCE(selected_pay_batch_item_row.item_type, ''))) IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_correction_row
      WHERE applied_correction_row.pay_batch_item_id = selected_pay_batch_item_row.id
        AND applied_correction_row.status = 'APPLIED'
        AND applied_correction_row.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
    )
    AND NOT (
      upper(btrim(coalesce(selected_pay_batch_candidate_row.settlement_status, ''))) = 'SETTLED'
      OR selected_pay_batch_candidate_row.settled_at_utc IS NOT NULL
      OR upper(btrim(coalesce(selected_pay_bank_transfer_row.status, ''))) = 'COMPLETED'
      OR selected_pay_bank_transfer_row.completed_at_utc IS NOT NULL
    )
),
unresolved_terminal_failure_hold_item_ids AS (
  SELECT DISTINCT
    terminal_failure_item_row.id AS pay_batch_item_id,
    family_to_projection.projected_timesheet_id AS projected_timesheet_id
  FROM family_to_projection
  JOIN public.pay_batch_items AS terminal_failure_item_row
    ON terminal_failure_item_row.timesheet_id = family_to_projection.family_timesheet_id
  JOIN public.pay_batch_candidates AS terminal_failure_candidate_row
    ON terminal_failure_candidate_row.id = terminal_failure_item_row.pay_batch_candidate_id
  JOIN public.pay_bank_transfer_events AS terminal_failure_event_row
    ON terminal_failure_event_row.pay_batch_id = terminal_failure_candidate_row.pay_batch_id
   AND terminal_failure_event_row.pay_bank_transfer_id = terminal_failure_item_row.pay_bank_transfer_id
   AND terminal_failure_event_row.mapping_status = 'MATCHED'
   AND terminal_failure_event_row.normalised_state IN ('FAILED','DECLINED','REJECTED','CANCELLED')
  LEFT JOIN public.pay_bank_transfers AS terminal_failure_transfer_row
    ON terminal_failure_transfer_row.id = terminal_failure_item_row.pay_bank_transfer_id
  WHERE terminal_failure_item_row.timesheet_id IS NOT NULL
    AND COALESCE(terminal_failure_item_row.is_voided, false) = false
    AND upper(coalesce(terminal_failure_item_row.pay_channel, '')) IN ('PAYE','UMBRELLA')
    AND (p_exclude_pay_batch_id IS NULL OR terminal_failure_candidate_row.pay_batch_id <> p_exclude_pay_batch_id)
    AND UPPER(BTRIM(COALESCE(terminal_failure_item_row.item_type, ''))) IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS applied_correction_row
      WHERE applied_correction_row.pay_batch_item_id = terminal_failure_item_row.id
        AND applied_correction_row.status = 'APPLIED'
        AND applied_correction_row.correction_item_kind IN ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
    )
    AND NOT (
      upper(btrim(coalesce(terminal_failure_candidate_row.settlement_status, ''))) = 'SETTLED'
      OR terminal_failure_candidate_row.settled_at_utc IS NOT NULL
      OR upper(btrim(coalesce(terminal_failure_transfer_row.status, ''))) = 'COMPLETED'
      OR terminal_failure_transfer_row.completed_at_utc IS NOT NULL
    )
),
active_item_ids AS (
  SELECT active_batch_item_ids.pay_batch_item_id, active_batch_item_ids.projected_timesheet_id
  FROM active_batch_item_ids
  UNION
  SELECT open_correction_hold_item_ids.pay_batch_item_id, open_correction_hold_item_ids.projected_timesheet_id
  FROM open_correction_hold_item_ids
  UNION
  SELECT unresolved_terminal_failure_hold_item_ids.pay_batch_item_id, unresolved_terminal_failure_hold_item_ids.projected_timesheet_id
  FROM unresolved_terminal_failure_hold_item_ids
),
active_item_id_array AS (
  SELECT
    CASE
      WHEN COUNT(*) = 0 THEN ARRAY['00000000-0000-0000-0000-000000000000'::uuid]
      ELSE ARRAY_AGG(active_item_ids.pay_batch_item_id ORDER BY active_item_ids.pay_batch_item_id)
    END AS pay_batch_item_ids
  FROM active_item_ids
),
reserved_keyed_raw AS (
  SELECT
    active_item_rows.pay_batch_item_id,
    active_item_rows.projected_timesheet_id AS timesheet_id,
    UPPER(NULLIF(BTRIM(COALESCE(economic_component_rows.key_type, '')), '')) AS key_type,
    NULLIF(BTRIM(COALESCE(economic_component_rows.key_value, '')), '') AS key_value,
    ROUND(COALESCE(economic_component_rows.source_amount_ex_vat, 0), 2) AS amount_ex_vat,
    ROUND(COALESCE(economic_component_rows.source_amount_inc_vat, 0), 2) AS amount_inc_vat
  FROM active_item_id_array
  JOIN LATERAL public._pay_batch_item_economic_components(
    p_pay_batch_id => NULL::uuid,
    p_pay_batch_item_ids => active_item_id_array.pay_batch_item_ids
  ) AS economic_component_rows
    ON true
  JOIN active_item_ids AS active_item_rows
    ON active_item_rows.pay_batch_item_id = economic_component_rows.pay_batch_item_id
  WHERE active_item_rows.projected_timesheet_id IS NOT NULL
    AND UPPER(BTRIM(COALESCE(economic_component_rows.item_type, ''))) IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
    AND economic_component_rows.key_resolution_failure_reason IS NULL
    AND economic_component_rows.source_amount_ex_vat IS NOT NULL
    AND UPPER(BTRIM(COALESCE(economic_component_rows.key_type, ''))) IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE')
),
reserved_keyed AS (
  SELECT
    reserved_keyed_raw.pay_batch_item_id,
    reserved_keyed_raw.timesheet_id,
    reserved_keyed_raw.key_type,
    reserved_keyed_raw.key_value,
    ROUND(SUM(COALESCE(reserved_keyed_raw.amount_ex_vat, 0)), 2) AS amount_ex_vat,
    ROUND(SUM(COALESCE(reserved_keyed_raw.amount_inc_vat, 0)), 2) AS amount_inc_vat
  FROM reserved_keyed_raw
  WHERE reserved_keyed_raw.timesheet_id IS NOT NULL
    AND reserved_keyed_raw.key_type IS NOT NULL
    AND BTRIM(reserved_keyed_raw.key_type) <> ''
    AND reserved_keyed_raw.key_value IS NOT NULL
    AND BTRIM(reserved_keyed_raw.key_value) <> ''
    AND NOT (reserved_keyed_raw.key_type = 'TS_DAY' AND reserved_keyed_raw.key_value !~ '^\d{4}-\d{2}-\d{2}$')
  GROUP BY
    reserved_keyed_raw.pay_batch_item_id,
    reserved_keyed_raw.timesheet_id,
    reserved_keyed_raw.key_type,
    reserved_keyed_raw.key_value
),
reserved_components AS (
  SELECT
    reserved_keyed_rows.timesheet_id,
    reserved_keyed_rows.key_type,
    reserved_keyed_rows.key_value,
    ROUND(SUM(COALESCE(reserved_keyed_rows.amount_ex_vat, 0)), 2) AS amount_ex_vat,
    ROUND(SUM(COALESCE(reserved_keyed_rows.amount_inc_vat, 0)), 2) AS amount_inc_vat
  FROM reserved_keyed AS reserved_keyed_rows
  GROUP BY
    reserved_keyed_rows.timesheet_id,
    reserved_keyed_rows.key_type,
    reserved_keyed_rows.key_value
)
SELECT
  reserved_component_rows.timesheet_id,
  reserved_component_rows.key_type,
  reserved_component_rows.key_value,
  reserved_component_rows.amount_ex_vat,
  reserved_component_rows.amount_inc_vat
FROM reserved_components AS reserved_component_rows
WHERE reserved_component_rows.timesheet_id IS NOT NULL
  AND reserved_component_rows.key_type IS NOT NULL
  AND reserved_component_rows.key_value IS NOT NULL
  AND (
    ROUND(COALESCE(reserved_component_rows.amount_ex_vat, 0), 2) <> 0
    OR ROUND(COALESCE(reserved_component_rows.amount_inc_vat, 0), 2) <> 0
  );
$function$;

-- _pay_reserved_components(uuid[])
CREATE OR REPLACE FUNCTION public._pay_reserved_components(p_timesheet_ids uuid[])
 RETURNS TABLE(timesheet_id uuid, key_type text, key_value text, amount_ex_vat numeric, amount_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
SELECT
  reserved_component_rows.timesheet_id,
  reserved_component_rows.key_type,
  reserved_component_rows.key_value,
  reserved_component_rows.amount_ex_vat,
  reserved_component_rows.amount_inc_vat
FROM public._pay_reserved_components(p_timesheet_ids, NULL::uuid) AS reserved_component_rows;
$function$;

-- _pay_resolve_payment_scope_for_cancel_rewind(uuid,jsonb,boolean,uuid)
CREATE OR REPLACE FUNCTION public._pay_resolve_payment_scope_for_cancel_rewind(p_pay_batch_id uuid, p_selection_json jsonb, p_lock_mode boolean DEFAULT false, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_scope_type text := NULL::text;
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_selected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_selected_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_selected_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_selected_transfer_group_keys text[] := ARRAY[]::text[];
  v_selected_pay_channels text[] := ARRAY[]::text[];
  v_explicit_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_expected_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_authoritative_explicit_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_item_ids uuid[] := ARRAY[]::uuid[];
  v_pay_batch_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_umbrella_ids uuid[] := ARRAY[]::uuid[];
  v_pay_bank_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_finance_component_ids uuid[] := ARRAY[]::uuid[];
  v_reservation_ids uuid[] := ARRAY[]::uuid[];
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_pay_channels text[] := ARRAY[]::text[];
  v_transfer_group_keys text[] := ARRAY[]::text[];
  v_candidate_payment_group_keys text[] := ARRAY[]::text[];
  v_missing_explicit_item_ids uuid[] := ARRAY[]::uuid[];
  v_selected_item_count integer := 0;
  v_expanded_item_count integer := 0;
  v_is_full_scope boolean := true;
  v_partial_scope_blockers jsonb := '[]'::jsonb;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NULL OR jsonb_typeof(p_selection_json) <> 'object' THEN
    RAISE EXCEPTION 'PAYMENT_SCOPE_SELECTION_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_SCOPE_SELECTION_JSON_MUST_BE_OBJECT', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM public.pay_batches AS batch_check WHERE batch_check.id = p_pay_batch_id) THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  v_scope_type := upper(NULLIF(btrim(COALESCE(p_selection_json->>'scope_type', '')), ''));

  IF v_scope_type IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_SCOPE_TYPE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAYMENT_SCOPE_TYPE_REQUIRED', 'pay_batch_id', p_pay_batch_id)::text;
  END IF;

  WITH selected_items AS (
    SELECT selected_item_rows.*
    FROM public._pay_payment_correction_selected_items(p_pay_batch_id, p_selection_json, true) AS selected_item_rows
  )
  SELECT
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_item_id) FILTER (WHERE selected_items.pay_batch_item_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_batch_candidate_id) FILTER (WHERE selected_items.pay_batch_candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.candidate_id) FILTER (WHERE selected_items.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.pay_bank_transfer_id) FILTER (WHERE selected_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.umbrella_id) FILTER (WHERE selected_items.umbrella_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT selected_items.transfer_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(selected_items.transfer_group_key, '')), '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT upper(btrim(COALESCE(selected_items.pay_channel, '')))) FILTER (WHERE NULLIF(btrim(COALESCE(selected_items.pay_channel, '')), '') IS NOT NULL), ARRAY[]::text[]),
    count(*)::integer
  INTO
    v_selected_pay_batch_item_ids,
    v_selected_pay_batch_candidate_ids,
    v_selected_candidate_ids,
    v_selected_pay_bank_transfer_ids,
    v_selected_umbrella_ids,
    v_selected_transfer_group_keys,
    v_selected_pay_channels,
    v_selected_item_count
  FROM selected_items;

  WITH raw_values AS (
    SELECT explicit_item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_selection_json->'pay_batch_item_ids') = 'array' THEN p_selection_json->'pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS explicit_item_values(raw_value)
    UNION ALL
    SELECT p_selection_json->>'pay_batch_item_id'
    WHERE p_selection_json ? 'pay_batch_item_id'
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_explicit_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  WITH raw_values AS (
    SELECT expected_item_values.raw_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(p_selection_json->'expected_pay_batch_item_ids') = 'array' THEN p_selection_json->'expected_pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ) AS expected_item_values(raw_value)
  ), clean_values AS (
    SELECT NULLIF(btrim(raw_values.raw_value), '') AS clean_value
    FROM raw_values
  )
  SELECT COALESCE(array_agg(clean_values.clean_value::uuid), ARRAY[]::uuid[])
  INTO v_expected_pay_batch_item_ids
  FROM clean_values
  WHERE clean_values.clean_value IS NOT NULL
    AND clean_values.clean_value ~ v_uuid_regex;

  SELECT COALESCE(array_agg(DISTINCT explicit_items.explicit_item_id) FILTER (WHERE explicit_items.explicit_item_id IS NOT NULL), ARRAY[]::uuid[])
  INTO v_authoritative_explicit_item_ids
  FROM (
    SELECT unnest(COALESCE(v_explicit_pay_batch_item_ids, ARRAY[]::uuid[])) AS explicit_item_id
    UNION ALL
    SELECT unnest(COALESCE(v_expected_pay_batch_item_ids, ARRAY[]::uuid[])) AS explicit_item_id
  ) AS explicit_items;

  WITH selected_seed AS (
    SELECT selected_item_rows.*
    FROM public._pay_payment_correction_selected_items(p_pay_batch_id, p_selection_json, true) AS selected_item_rows
  ), expanded_items AS (
    SELECT DISTINCT candidate_item_rows.id AS pay_batch_item_id
    FROM public.pay_batch_items AS candidate_item_rows
    JOIN public.pay_batch_candidates AS candidate_batch_rows
      ON candidate_batch_rows.id = candidate_item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS candidate_transfer_rows
      ON candidate_transfer_rows.id = candidate_item_rows.pay_bank_transfer_id
    WHERE candidate_batch_rows.pay_batch_id = p_pay_batch_id
      AND COALESCE(candidate_item_rows.is_voided, false) IS NOT TRUE
      AND (
        v_scope_type = 'BATCH'
        OR candidate_item_rows.id = ANY(COALESCE(v_selected_pay_batch_item_ids, ARRAY[]::uuid[]))
        OR (
          COALESCE(array_length(v_selected_pay_bank_transfer_ids, 1), 0) > 0
          AND candidate_item_rows.pay_bank_transfer_id = ANY(v_selected_pay_bank_transfer_ids)
        )
        OR (
          v_scope_type = 'CANDIDATES'
          AND candidate_item_rows.pay_batch_candidate_id = ANY(COALESCE(v_selected_pay_batch_candidate_ids, ARRAY[]::uuid[]))
        )
        OR EXISTS (
          SELECT 1
          FROM selected_seed AS selected_seed_rows
          WHERE selected_seed_rows.pay_batch_candidate_id = candidate_item_rows.pay_batch_candidate_id
            AND (
              (
                selected_seed_rows.pay_bank_transfer_id IS NOT NULL
                AND candidate_item_rows.pay_bank_transfer_id = selected_seed_rows.pay_bank_transfer_id
              )
              OR (
                NULLIF(btrim(COALESCE(selected_seed_rows.transfer_group_key, '')), '') IS NOT NULL
                AND NULLIF(btrim(COALESCE(candidate_transfer_rows.transfer_group_key, '')), '') IS NOT NULL
                AND candidate_transfer_rows.transfer_group_key = selected_seed_rows.transfer_group_key
                AND upper(btrim(COALESCE(candidate_item_rows.pay_channel, ''))) = upper(btrim(COALESCE(selected_seed_rows.pay_channel, '')))
              )
              OR (
                selected_seed_rows.pay_bank_transfer_id IS NULL
                AND candidate_item_rows.pay_bank_transfer_id IS NULL
                AND upper(btrim(COALESCE(candidate_item_rows.pay_channel, ''))) = upper(btrim(COALESCE(selected_seed_rows.pay_channel, '')))
                AND candidate_item_rows.umbrella_id IS NOT DISTINCT FROM selected_seed_rows.umbrella_id
              )
            )
        )
      )
  )
  SELECT COALESCE(array_agg(expanded_items.pay_batch_item_id), ARRAY[]::uuid[])
  INTO v_pay_batch_item_ids
  FROM expanded_items;

  v_expanded_item_count := COALESCE(array_length(v_pay_batch_item_ids, 1), 0);

  WITH expanded_batch_items AS (
    SELECT
      item_rows.id AS pay_batch_item_id,
      item_rows.pay_batch_candidate_id,
      batch_candidate_rows.candidate_id,
      item_rows.pay_bank_transfer_id,
      COALESCE(item_rows.umbrella_id, transfer_rows.umbrella_id) AS umbrella_id,
      item_rows.finance_case_id,
      item_rows.finance_component_id,
      item_rows.reservation_id,
      item_rows.timesheet_id,
      upper(btrim(COALESCE(item_rows.pay_channel, ''))) AS pay_channel,
      transfer_rows.transfer_group_key,
      (
        batch_candidate_rows.candidate_id::text
        || ':' || upper(btrim(COALESCE(item_rows.pay_channel, '')))
        || ':' || COALESCE(item_rows.umbrella_id::text, transfer_rows.umbrella_id::text, 'NO_UMBRELLA')
        || ':' || COALESCE(transfer_rows.transfer_group_key, 'NO_TRANSFER_GROUP')
      ) AS candidate_payment_group_key
    FROM public.pay_batch_items AS item_rows
    JOIN public.pay_batch_candidates AS batch_candidate_rows
      ON batch_candidate_rows.id = item_rows.pay_batch_candidate_id
    LEFT JOIN public.pay_bank_transfers AS transfer_rows
      ON transfer_rows.id = item_rows.pay_bank_transfer_id
    WHERE item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
  ), reservation_rows AS (
    SELECT reservation_source_rows.*
    FROM public.pay_advance_reservations AS reservation_source_rows
    WHERE reservation_source_rows.pay_batch_id = p_pay_batch_id
      AND (
        reservation_source_rows.pay_batch_item_id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
        OR reservation_source_rows.pay_batch_candidate_id = ANY(COALESCE(v_selected_pay_batch_candidate_ids, ARRAY[]::uuid[]))
      )
  )
  SELECT
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_batch_candidate_id) FILTER (WHERE expanded_batch_items.pay_batch_candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.candidate_id) FILTER (WHERE expanded_batch_items.candidate_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.umbrella_id) FILTER (WHERE expanded_batch_items.umbrella_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_bank_transfer_id) FILTER (WHERE expanded_batch_items.pay_bank_transfer_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.finance_case_id, reservation_rows.finance_case_id)) FILTER (WHERE COALESCE(expanded_batch_items.finance_case_id, reservation_rows.finance_case_id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.finance_component_id, reservation_rows.finance_component_id)) FILTER (WHERE COALESCE(expanded_batch_items.finance_component_id, reservation_rows.finance_component_id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT COALESCE(expanded_batch_items.reservation_id, reservation_rows.id)) FILTER (WHERE COALESCE(expanded_batch_items.reservation_id, reservation_rows.id) IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.timesheet_id) FILTER (WHERE expanded_batch_items.timesheet_id IS NOT NULL), ARRAY[]::uuid[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.pay_channel) FILTER (WHERE NULLIF(expanded_batch_items.pay_channel, '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.transfer_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(expanded_batch_items.transfer_group_key, '')), '') IS NOT NULL), ARRAY[]::text[]),
    COALESCE(array_agg(DISTINCT expanded_batch_items.candidate_payment_group_key) FILTER (WHERE NULLIF(btrim(COALESCE(expanded_batch_items.candidate_payment_group_key, '')), '') IS NOT NULL), ARRAY[]::text[])
  INTO
    v_pay_batch_candidate_ids,
    v_candidate_ids,
    v_umbrella_ids,
    v_pay_bank_transfer_ids,
    v_finance_case_ids,
    v_finance_component_ids,
    v_reservation_ids,
    v_timesheet_ids,
    v_pay_channels,
    v_transfer_group_keys,
    v_candidate_payment_group_keys
  FROM expanded_batch_items
  LEFT JOIN reservation_rows
    ON reservation_rows.pay_batch_item_id = expanded_batch_items.pay_batch_item_id
    OR reservation_rows.pay_batch_candidate_id = expanded_batch_items.pay_batch_candidate_id;

  IF COALESCE(array_length(v_authoritative_explicit_item_ids, 1), 0) > 0 THEN
    SELECT COALESCE(array_agg(missing_item_rows.pay_batch_item_id), ARRAY[]::uuid[])
    INTO v_missing_explicit_item_ids
    FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS missing_item_rows(pay_batch_item_id)
    WHERE NOT (missing_item_rows.pay_batch_item_id = ANY(v_authoritative_explicit_item_ids));

    v_is_full_scope := COALESCE(array_length(v_missing_explicit_item_ids, 1), 0) = 0;
  ELSE
    v_is_full_scope := true;
  END IF;

  IF NOT v_is_full_scope THEN
    v_partial_scope_blockers := v_partial_scope_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'SELECT_FULL_UNPAID_PAYMENT_SCOPE_REQUIRED',
      'message', 'The selected rows do not include the whole unpaid payment scope. Select the full payment scope before cancellation or rewind.',
      'missing_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(missing_values.pay_batch_item_id::text ORDER BY missing_values.pay_batch_item_id::text) FROM unnest(v_missing_explicit_item_ids) AS missing_values(pay_batch_item_id)), '[]'::jsonb)
    ));
  END IF;

  IF COALESCE(p_lock_mode, false) THEN
    PERFORM 1
    FROM public.pay_batches AS lock_batch_rows
    WHERE lock_batch_rows.id = p_pay_batch_id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_bank_transfers AS lock_transfer_rows
    WHERE lock_transfer_rows.id = ANY(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[]))
    ORDER BY lock_transfer_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_batch_candidates AS lock_candidate_rows
    WHERE lock_candidate_rows.id = ANY(COALESCE(v_pay_batch_candidate_ids, ARRAY[]::uuid[]))
    ORDER BY lock_candidate_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_batch_items AS lock_item_rows
    WHERE lock_item_rows.id = ANY(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[]))
    ORDER BY lock_item_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_advance_reservations AS lock_reservation_rows
    WHERE lock_reservation_rows.id = ANY(COALESCE(v_reservation_ids, ARRAY[]::uuid[]))
    ORDER BY lock_reservation_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_finance_case_components AS lock_component_rows
    WHERE lock_component_rows.id = ANY(COALESCE(v_finance_component_ids, ARRAY[]::uuid[]))
    ORDER BY lock_component_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_payment_correction_requests AS lock_request_rows
    WHERE lock_request_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_request_rows.status, '')) IN ('REQUESTED', 'PENDING', 'AUTHORISED', 'AUTHORIZED', 'IN_PROGRESS', 'PROCESSING', 'EXPANDED')
    ORDER BY lock_request_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.pay_payment_correction_work_items AS lock_work_item_rows
    WHERE lock_work_item_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_work_item_rows.status, '')) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING')
    ORDER BY lock_work_item_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.banking_pay_operations AS lock_operation_rows
    WHERE lock_operation_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_operation_rows.status, '')) IN ('QUEUED', 'RUNNING', 'PROCESSING', 'IN_PROGRESS', 'REVIEW_REQUIRED')
    ORDER BY lock_operation_rows.id
    FOR UPDATE;

    PERFORM 1
    FROM public.banking_pay_operation_chunks AS lock_chunk_rows
    JOIN public.banking_pay_operations AS lock_chunk_operation_rows
      ON lock_chunk_operation_rows.id = lock_chunk_rows.operation_id
    WHERE lock_chunk_operation_rows.pay_batch_id = p_pay_batch_id
      AND upper(COALESCE(lock_chunk_rows.status, '')) IN ('PENDING', 'CLAIMED', 'LOCKED', 'RUNNING', 'PROCESSING')
    ORDER BY lock_chunk_rows.id
    FOR UPDATE;
  END IF;

  v_result := jsonb_build_object(
    'scope_type', v_scope_type,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_batch_candidate_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_batch_candidate_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'candidate_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'umbrella_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_umbrella_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_bank_transfer_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_bank_transfer_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'finance_case_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_finance_case_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'finance_component_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_finance_component_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'reservation_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_reservation_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'timesheet_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
    'pay_channels', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_pay_channels, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'transfer_group_keys', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_transfer_group_keys, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'candidate_payment_group_keys', COALESCE((SELECT jsonb_agg(scope_values.value_text ORDER BY scope_values.value_text) FROM unnest(COALESCE(v_candidate_payment_group_keys, ARRAY[]::text[])) AS scope_values(value_text)), '[]'::jsonb),
    'is_full_scope', v_is_full_scope,
    'partial_scope_blockers', COALESCE(v_partial_scope_blockers, '[]'::jsonb),
    'support_details_json', jsonb_build_object(
      'selected_item_count', v_selected_item_count,
      'expanded_item_count', v_expanded_item_count,
      'explicit_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_explicit_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
      'expected_pay_batch_item_ids', COALESCE((SELECT jsonb_agg(scope_values.value_id::text ORDER BY scope_values.value_id::text) FROM unnest(COALESCE(v_expected_pay_batch_item_ids, ARRAY[]::uuid[])) AS scope_values(value_id)), '[]'::jsonb),
      'lock_mode', COALESCE(p_lock_mode, false),
      'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    )
  );

  RETURN v_result;
END;
$function$;

-- _pay_timesheet_components(jsonb)
CREATE OR REPLACE FUNCTION public._pay_timesheet_components(p_snapshot_json jsonb)
 RETURNS TABLE(key_type text, key_value text, amount_ex_vat numeric, amount_inc_vat numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
with
seg_raw as (
  select
    nullif(btrim(coalesce(seg->>'segment_id','')), '') as segment_id,
    nullif(btrim(coalesce(seg->>'date','')), '') as seg_date_raw,
    coalesce(nullif(seg->>'exclude_from_pay','')::boolean, false) as exclude_from_pay,
    case
      when coalesce(seg->>'pay_amount','') ~ '^-?\d+(\.\d+)?$' then (seg->>'pay_amount')::numeric
      else 0::numeric
    end as pay_amount_ex
  from jsonb_array_elements(coalesce(p_snapshot_json->'segments','[]'::jsonb)) as seg
  where seg is not null and jsonb_typeof(seg) = 'object'
),
seg_norm as (
  select
    case
      when sr.seg_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then 'TS_DAY'
      else 'TS_TOTAL'
    end as key_type,
    case
      when sr.seg_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then sr.seg_date_raw
      else 'TOTAL'
    end as key_value,
    round(
      sum(
        case
          when sr.exclude_from_pay then 0::numeric
          else coalesce(sr.pay_amount_ex,0)
        end
      ),
      2
    ) as amount_ex_vat
  from seg_raw sr
  where sr.segment_id is not null
  group by 1,2
),
add_kv as (
  select
    upper(btrim(e.key)) as code,
    e.value as obj
  from jsonb_each(coalesce(p_snapshot_json->'additional_units_json','{}'::jsonb)) as e
  where e.key is not null
    and btrim(e.key) <> ''
    and e.value is not null
    and jsonb_typeof(e.value) = 'object'
),
add_by_code as (
  select
    'ADDITIONAL_CODE'::text as key_type,
    ak.code as key_value,
    round(
      coalesce(
        case when coalesce(ak.obj->>'pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'pay_ex_vat')::numeric end,
        case when coalesce(ak.obj->>'amount_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'amount_ex_vat')::numeric end,
        (
          coalesce(
            case when coalesce(ak.obj->>'unit_count','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'unit_count')::numeric end,
            case when coalesce(ak.obj->>'units_week','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'units_week')::numeric end,
            0::numeric
          )
          *
          coalesce(
            case when coalesce(ak.obj->>'pay_rate','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'pay_rate')::numeric end,
            case when coalesce(ak.obj->>'rate','') ~ '^-?\d+(\.\d+)?$' then (ak.obj->>'rate')::numeric end,
            0::numeric
          )
        ),
        0::numeric
      ),
      2
    ) as amount_ex_vat
  from add_kv ak
),
add_sum as (
  select
    round(coalesce(sum(abc.amount_ex_vat),0),2) as sum_ex
  from add_by_code abc
),
add_total_fallback as (
  select
    'ADDITIONAL_CODE'::text as key_type,
    'TOTAL'::text as key_value,
    round(
      case
        when coalesce(p_snapshot_json->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (p_snapshot_json->>'additional_pay_ex_vat')::numeric
        else 0::numeric
      end,
      2
    ) as amount_ex_vat
  where (select coalesce(a.sum_ex,0) from add_sum a) = 0
    and (
      coalesce(p_snapshot_json->>'additional_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      and (p_snapshot_json->>'additional_pay_ex_vat')::numeric <> 0
    )
),
exp_vals as (
  select
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,travel_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,travel_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as travel_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,accommodation_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as accom_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,other_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,other_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as other_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,mileage_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,mileage_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as mileage_ex,
    round(
      case when coalesce(p_snapshot_json #>> '{expenses,expenses_pay_ex_vat}','') ~ '^-?\d+(\.\d+)?$'
           then (p_snapshot_json #>> '{expenses,expenses_pay_ex_vat}')::numeric else 0::numeric end, 2
    ) as expenses_rollup_ex
),
exp_has_cats as (
  select
    (coalesce(ev.travel_ex,0) <> 0)
    or (coalesce(ev.accom_ex,0) <> 0)
    or (coalesce(ev.other_ex,0) <> 0)
    or (coalesce(ev.mileage_ex,0) <> 0) as has_any_cat
  from exp_vals ev
),
exp_components as (
  select 'EXPENSE_CODE'::text as key_type, 'TRAVEL'::text as key_value, ev.travel_ex as amount_ex_vat
  from exp_vals ev
  where coalesce(ev.travel_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'ACCOMMODATION'::text, ev.accom_ex
  from exp_vals ev
  where coalesce(ev.accom_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'OTHER'::text, ev.other_ex
  from exp_vals ev
  where coalesce(ev.other_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'MILEAGE'::text, ev.mileage_ex
  from exp_vals ev
  where coalesce(ev.mileage_ex,0) <> 0
  union all
  select 'EXPENSE_CODE'::text, 'EXPENSES'::text, ev.expenses_rollup_ex
  from exp_vals ev
  where (select eh.has_any_cat from exp_has_cats eh) = false
    and coalesce(ev.expenses_rollup_ex,0) <> 0
),
adj_components as (
  select
    'ADJUSTMENT_CODE'::text as key_type,
    nullif(btrim(coalesce(adj->>'id','')), '') as key_value,
    round(
      case
        when coalesce(adj->>'delta_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$' then (adj->>'delta_pay_ex_vat')::numeric
        else 0::numeric
      end,
      2
    ) as amount_ex_vat
  from jsonb_array_elements(coalesce(p_snapshot_json->'adjustments','[]'::jsonb)) as adj
  where adj is not null
    and jsonb_typeof(adj)='object'
    and nullif(btrim(coalesce(adj->>'id','')), '') is not null
    and (
      coalesce(adj->>'delta_pay_ex_vat','') ~ '^-?\d+(\.\d+)?$'
      and (adj->>'delta_pay_ex_vat')::numeric <> 0
    )
)
select
  s.key_type,
  s.key_value,
  s.amount_ex_vat,
  s.amount_ex_vat as amount_inc_vat
from seg_norm s

union all
select
  abc.key_type,
  abc.key_value,
  abc.amount_ex_vat,
  abc.amount_ex_vat
from add_by_code abc
where abc.amount_ex_vat <> 0

union all
select
  atf.key_type,
  atf.key_value,
  atf.amount_ex_vat,
  atf.amount_ex_vat
from add_total_fallback atf

union all
select
  ec.key_type,
  ec.key_value,
  ec.amount_ex_vat,
  ec.amount_ex_vat
from exp_components ec

union all
select
  ac.key_type,
  ac.key_value,
  ac.amount_ex_vat,
  ac.amount_ex_vat
from adj_components ac;
$function$;

-- _pay_timesheet_rotation_scope(uuid[])
CREATE OR REPLACE FUNCTION public._pay_timesheet_rotation_scope(p_timesheet_ids uuid[])
 RETURNS TABLE(requested_timesheet_id uuid, booking_id text, canonical_timesheet_id uuid, family_timesheet_id uuid, family_is_current boolean, family_version integer, requested_is_canonical boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
WITH input_timesheets AS (
  SELECT DISTINCT
    input_timesheet_values.timesheet_id_value AS requested_timesheet_id
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_timesheet_values(timesheet_id_value)
  WHERE input_timesheet_values.timesheet_id_value IS NOT NULL
),
requested_timesheets AS (
  SELECT
    input_timesheets.requested_timesheet_id AS requested_timesheet_id,
    public.timesheets.timesheet_id AS matched_timesheet_id,
    public.timesheets.booking_id AS matched_booking_id
  FROM input_timesheets
  LEFT JOIN public.timesheets
    ON public.timesheets.timesheet_id = input_timesheets.requested_timesheet_id
),
requested_bookings AS (
  SELECT DISTINCT
    requested_timesheets.matched_booking_id AS booking_id
  FROM requested_timesheets
  WHERE requested_timesheets.matched_timesheet_id IS NOT NULL
    AND requested_timesheets.matched_booking_id IS NOT NULL
    AND BTRIM(requested_timesheets.matched_booking_id) <> ''
),
canonical_timesheets AS (
  SELECT DISTINCT ON (current_timesheets.booking_id)
    current_timesheets.booking_id AS booking_id,
    current_timesheets.timesheet_id AS canonical_timesheet_id
  FROM public.timesheets AS current_timesheets
  JOIN requested_bookings
    ON requested_bookings.booking_id = current_timesheets.booking_id
  WHERE current_timesheets.is_current = true
  ORDER BY
    current_timesheets.booking_id,
    current_timesheets.version DESC,
    current_timesheets.updated_at DESC,
    current_timesheets.created_at DESC,
    current_timesheets.timesheet_id
),
family_timesheets AS (
  SELECT DISTINCT
    family_rows.booking_id AS booking_id,
    family_rows.timesheet_id AS family_timesheet_id,
    family_rows.is_current AS family_is_current,
    family_rows.version AS family_version
  FROM public.timesheets AS family_rows
  JOIN requested_bookings
    ON requested_bookings.booking_id = family_rows.booking_id
),
resolved_scope_rows AS (
  SELECT
    requested_timesheets.requested_timesheet_id AS requested_timesheet_id,
    requested_timesheets.matched_booking_id AS booking_id,
    canonical_timesheets.canonical_timesheet_id AS canonical_timesheet_id,
    family_timesheets.family_timesheet_id AS family_timesheet_id,
    family_timesheets.family_is_current AS family_is_current,
    family_timesheets.family_version AS family_version,
    COALESCE(requested_timesheets.requested_timesheet_id = canonical_timesheets.canonical_timesheet_id, false) AS requested_is_canonical
  FROM requested_timesheets
  JOIN family_timesheets
    ON family_timesheets.booking_id = requested_timesheets.matched_booking_id
  LEFT JOIN canonical_timesheets
    ON canonical_timesheets.booking_id = requested_timesheets.matched_booking_id
  WHERE requested_timesheets.matched_timesheet_id IS NOT NULL
),
defensive_unresolved_rows AS (
  SELECT
    requested_timesheets.requested_timesheet_id AS requested_timesheet_id,
    NULL::text AS booking_id,
    requested_timesheets.requested_timesheet_id AS canonical_timesheet_id,
    requested_timesheets.requested_timesheet_id AS family_timesheet_id,
    NULL::boolean AS family_is_current,
    NULL::integer AS family_version,
    false AS requested_is_canonical
  FROM requested_timesheets
  WHERE requested_timesheets.matched_timesheet_id IS NULL
),
rotation_scope_output AS (
  SELECT
    resolved_scope_rows.requested_timesheet_id AS requested_timesheet_id,
    resolved_scope_rows.booking_id AS booking_id,
    resolved_scope_rows.canonical_timesheet_id AS canonical_timesheet_id,
    resolved_scope_rows.family_timesheet_id AS family_timesheet_id,
    resolved_scope_rows.family_is_current AS family_is_current,
    resolved_scope_rows.family_version AS family_version,
    resolved_scope_rows.requested_is_canonical AS requested_is_canonical
  FROM resolved_scope_rows

  UNION ALL

  SELECT
    defensive_unresolved_rows.requested_timesheet_id AS requested_timesheet_id,
    defensive_unresolved_rows.booking_id AS booking_id,
    defensive_unresolved_rows.canonical_timesheet_id AS canonical_timesheet_id,
    defensive_unresolved_rows.family_timesheet_id AS family_timesheet_id,
    defensive_unresolved_rows.family_is_current AS family_is_current,
    defensive_unresolved_rows.family_version AS family_version,
    defensive_unresolved_rows.requested_is_canonical AS requested_is_canonical
  FROM defensive_unresolved_rows
)
SELECT
  rotation_scope_output.requested_timesheet_id,
  rotation_scope_output.booking_id,
  rotation_scope_output.canonical_timesheet_id,
  rotation_scope_output.family_timesheet_id,
  rotation_scope_output.family_is_current,
  rotation_scope_output.family_version,
  rotation_scope_output.requested_is_canonical
FROM rotation_scope_output
ORDER BY
  rotation_scope_output.requested_timesheet_id,
  rotation_scope_output.booking_id NULLS LAST,
  rotation_scope_output.family_is_current DESC NULLS LAST,
  rotation_scope_output.family_version DESC NULLS LAST,
  rotation_scope_output.family_timesheet_id;
$function$;

-- _pay_umbrella_vat_calc(numeric,numeric,boolean)
CREATE OR REPLACE FUNCTION public._pay_umbrella_vat_calc(p_ex numeric, p_vat_rate_pct numeric, p_vat_chargeable boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
declare
  v_ex numeric := coalesce(p_ex,0);
  v_vat numeric := 0;
  v_inc numeric := 0;
  v_frac numeric := public._pay_pct_to_frac(p_vat_rate_pct);
begin
  if coalesce(p_vat_chargeable,false) then
    v_vat := round(v_ex * v_frac, 2);
    v_inc := round(v_ex + v_vat, 2);
  else
    v_vat := 0;
    v_inc := round(v_ex, 2);
  end if;

  return jsonb_build_object(
    'ex', round(v_ex,2),
    'vat', round(v_vat,2),
    'inc', round(v_inc,2)
  );
end;
$function$;

-- _pay_week_start_monday(date)
CREATE OR REPLACE FUNCTION public._pay_week_start_monday(p_date date)
 RETURNS date
 LANGUAGE plpgsql
 IMMUTABLE
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
declare
  v_dow int;
  v_offset int;
begin
  if p_date is null then return null; end if;
  v_dow := extract(dow from p_date)::int;      -- 0=Sun..6=Sat
  v_offset := (v_dow + 6) % 7;                 -- days since Monday
  return (p_date - v_offset);
end;
$function$;

-- _pay_workbench_authoritative_scope_valid_v1(uuid,uuid,bigint,uuid,date,date,text)
CREATE OR REPLACE FUNCTION public._pay_workbench_authoritative_scope_valid_v1(p_session_id uuid, p_candidate_id uuid, p_session_version bigint, p_actor_user_id uuid, p_pay_date date, p_week_ending_cutoff date, p_pay_channel_scope text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS authoritative_session
    JOIN public.banking_pay_workbench_session_scope AS authoritative_scope
      ON authoritative_scope.session_id = authoritative_session.id
     AND authoritative_scope.candidate_id = p_candidate_id
    JOIN public.candidates AS authoritative_candidate
      ON authoritative_candidate.id = authoritative_scope.candidate_id
    WHERE authoritative_session.id = p_session_id
      AND UPPER(BTRIM(authoritative_session.status)) = 'OPEN'
      AND authoritative_session.discarded_at_utc IS NULL
      AND CASE
            WHEN authoritative_session.version IS NULL THEN 1
            ELSE authoritative_session.version
          END = p_session_version
      AND authoritative_session.actor_user_id = p_actor_user_id
      AND authoritative_session.pay_date = p_pay_date
      AND authoritative_session.week_ending_cutoff = p_week_ending_cutoff
      AND UPPER(BTRIM(authoritative_candidate.pay_method)) = UPPER(BTRIM(p_pay_channel_scope))
  );
$function$;

-- _pay_workbench_candidate_projection_contract()
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_projection_contract()
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog'
AS $function$
BEGIN
  RETURN jsonb_build_object(
    'projection_version', 4,
    'hidden_recovery_template_projection_version', 1,
    'requires_hidden_recovery_templates', true,
    'canonical_correction_carrier_version',
      'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_V1',
    'targeted_family_materialisation_version',
      'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1'
  );
END;
$function$;

-- _pay_workbench_candidate_serial_active_state(uuid,uuid,text,jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_active_state(p_job_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_job_type text DEFAULT NULL::text, p_payload_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, clock_timestamp());
  v_payload jsonb := CASE WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_candidate_id uuid := public._pay_workbench_candidate_serial_candidate_id(p_candidate_id, p_payload_json);
  v_serial_key text := public._pay_workbench_candidate_serial_key(public._pay_workbench_candidate_serial_candidate_id(p_candidate_id, p_payload_json));
  v_candidate_job boolean := false;
  v_is_chain_continuation boolean := false;
  v_projection_run_id_text text := NULL::text;
  v_projection_run_id uuid := NULL::uuid;
  v_active_job_id uuid := NULL::uuid;
  v_active_job_type text := NULL::text;
  v_active_job_status text := NULL::text;
  v_active_job_started_at_utc timestamptz := NULL::timestamptz;
  v_active_job_run_at_utc timestamptz := NULL::timestamptz;
  v_active_chain_id text := NULL::text;
  v_queued_chain_job_id uuid := NULL::uuid;
  v_queued_chain_job_type text := NULL::text;
  v_queued_chain_job_status text := NULL::text;
  v_queued_chain_started_at_utc timestamptz := NULL::timestamptz;
  v_queued_chain_run_at_utc timestamptz := NULL::timestamptz;
  v_queued_chain_id text := NULL::text;
  v_projection_blocker_id uuid := NULL::uuid;
  v_projection_blocker_status text := NULL::text;
  v_projection_blocker_started_at_utc timestamptz := NULL::timestamptz;
  v_projection_blocker_updated_at_utc timestamptz := NULL::timestamptz;
  v_blocked boolean := false;
  v_reason text := NULL::text;
BEGIN
  v_candidate_job := public._pay_workbench_candidate_serial_is_candidate_job(p_job_type, v_candidate_id, v_payload);

  IF v_candidate_id IS NULL OR v_candidate_job IS NOT TRUE THEN
    RETURN jsonb_build_object(
      'ok', true,
      'blocked', false,
      'candidate_serial_blocked', false,
      'reason', 'NO_CANDIDATE_SERIAL_SCOPE',
      'candidate_serial_wait_reason', 'NO_CANDIDATE_SERIAL_SCOPE',
      'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
      'candidate_serial_key', v_serial_key
    );
  END IF;

  v_is_chain_continuation := (
    lower(BTRIM(COALESCE(v_payload->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR UPPER(BTRIM(COALESCE(v_payload->>'run_mode', ''))) IN ('BOUNDED_CONTINUATION', 'CONTINUATION', 'STAGE_CONTINUATION')
    OR lower(BTRIM(COALESCE(v_payload->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR (
      lower(BTRIM(COALESCE(v_payload->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND NULLIF(BTRIM(COALESCE(v_payload->>'fallback_reason', '')), '') IS NOT NULL
    )
    OR (
      NULLIF(BTRIM(COALESCE(v_payload->>'source_job_id', v_payload->>'continuation_source_job_id', v_payload->>'bounded_continuation_source_job_id', '')), '') IS NOT NULL
      AND UPPER(BTRIM(COALESCE(v_payload->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')
    )
  );

  v_projection_run_id_text := COALESCE(
    NULLIF(BTRIM(COALESCE(v_payload->>'projection_run_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload#>>'{cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload#>>'{cursor,cursor,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload#>>'{cursor_json,projection_run_id}', '')), ''),
    NULLIF(BTRIM(COALESCE(v_payload#>>'{cursor_json,cursor,projection_run_id}', '')), '')
  );
  IF v_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_projection_run_id := v_projection_run_id_text::uuid;
  END IF;

  SELECT running_job.id,
         running_job.job_type,
         running_job.status,
         running_job.started_at_utc,
         running_job.run_at_utc,
         COALESCE(NULLIF(BTRIM(COALESCE(running_job.payload_json->>'candidate_serial_active_chain_id', '')), ''), NULLIF(BTRIM(COALESCE(running_job.payload_json->>'source_job_id', '')), ''), running_job.id::text)
  INTO v_active_job_id, v_active_job_type, v_active_job_status, v_active_job_started_at_utc, v_active_job_run_at_utc, v_active_chain_id
  FROM public.banking_pay_workbench_jobs AS running_job
  WHERE UPPER(BTRIM(COALESCE(running_job.status, ''))) = 'RUNNING'
    AND running_job.id IS DISTINCT FROM p_job_id
    AND public._pay_workbench_candidate_serial_candidate_id(running_job.candidate_id, running_job.payload_json) = v_candidate_id
    AND public._pay_workbench_candidate_serial_is_candidate_job(running_job.job_type, running_job.candidate_id, running_job.payload_json)
  ORDER BY running_job.started_at_utc ASC NULLS FIRST, running_job.created_at_utc ASC, running_job.id ASC
  LIMIT 1;

  IF v_active_job_id IS NOT NULL THEN
    v_blocked := true;
    v_reason := 'CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_JOB';
  END IF;

  IF v_blocked IS NOT TRUE AND v_is_chain_continuation IS NOT TRUE THEN
    SELECT queued_job.id,
           queued_job.job_type,
           queued_job.status,
           queued_job.started_at_utc,
           queued_job.run_at_utc,
           COALESCE(NULLIF(BTRIM(COALESCE(queued_job.payload_json->>'candidate_serial_active_chain_id', '')), ''), NULLIF(BTRIM(COALESCE(queued_job.payload_json->>'source_job_id', '')), ''), queued_job.id::text)
    INTO v_queued_chain_job_id, v_queued_chain_job_type, v_queued_chain_job_status, v_queued_chain_started_at_utc, v_queued_chain_run_at_utc, v_queued_chain_id
    FROM public.banking_pay_workbench_jobs AS queued_job
    WHERE UPPER(BTRIM(COALESCE(queued_job.status, ''))) = 'QUEUED'
      AND queued_job.id IS DISTINCT FROM p_job_id
      AND public._pay_workbench_candidate_serial_candidate_id(queued_job.candidate_id, queued_job.payload_json) = v_candidate_id
      AND public._pay_workbench_candidate_serial_is_candidate_job(queued_job.job_type, queued_job.candidate_id, queued_job.payload_json)
      AND (
        lower(BTRIM(COALESCE(queued_job.payload_json->>'continuation', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR UPPER(BTRIM(COALESCE(queued_job.payload_json->>'run_mode', ''))) IN ('BOUNDED_CONTINUATION', 'CONTINUATION', 'STAGE_CONTINUATION')
        OR lower(BTRIM(COALESCE(queued_job.payload_json->>'fallback_from_delta', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        OR (
          lower(BTRIM(COALESCE(queued_job.payload_json->>'source_build_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND NULLIF(BTRIM(COALESCE(queued_job.payload_json->>'fallback_reason', '')), '') IS NOT NULL
        )
        OR (
          NULLIF(BTRIM(COALESCE(queued_job.payload_json->>'source_job_id', queued_job.payload_json->>'continuation_source_job_id', queued_job.payload_json->>'bounded_continuation_source_job_id', '')), '') IS NOT NULL
          AND UPPER(BTRIM(COALESCE(queued_job.payload_json->>'run_mode', ''))) NOT IN ('LATEST_STATE_HEAD', 'LATEST_RERUN_AFTER_RUNNING')
        )
      )
    ORDER BY queued_job.run_at_utc ASC, queued_job.priority ASC, queued_job.created_at_utc ASC, queued_job.id ASC
    LIMIT 1;

    IF v_queued_chain_job_id IS NOT NULL THEN
      v_blocked := true;
      v_reason := 'CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_CONTINUATION';
    END IF;
  END IF;

  IF v_blocked IS NOT TRUE THEN
    SELECT projection_run.id, projection_run.status, projection_run.started_at_utc, projection_run.updated_at_utc
    INTO v_projection_blocker_id, v_projection_blocker_status, v_projection_blocker_started_at_utc, v_projection_blocker_updated_at_utc
    FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
    WHERE projection_run.candidate_id = v_candidate_id
      AND UPPER(BTRIM(COALESCE(projection_run.status, ''))) IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS')
      AND (v_projection_run_id IS NULL OR projection_run.id IS DISTINCT FROM v_projection_run_id)
    ORDER BY projection_run.started_at_utc ASC NULLS FIRST, projection_run.updated_at_utc ASC NULLS FIRST, projection_run.id ASC
    LIMIT 1;

    IF v_projection_blocker_id IS NOT NULL THEN
      v_blocked := true;
      v_reason := 'CANDIDATE_SERIAL_BLOCKED_BY_ACTIVE_PROJECTION_RUN';
    END IF;
  END IF;

  IF v_blocked IS TRUE THEN
    PERFORM public._pay_workbench_candidate_serial_audit(
      COALESCE(v_reason, 'CANDIDATE_SERIAL_BLOCKED'),
      p_job_id,
      v_candidate_id,
      jsonb_strip_nulls(jsonb_build_object(
        'candidate_serial_key', v_serial_key,
        'blocked_job_id', CASE WHEN v_active_job_id IS NULL THEN NULL ELSE v_active_job_id::text END,
        'blocked_job_type', v_active_job_type,
        'blocked_job_status', v_active_job_status,
        'blocked_job_started_at_utc', v_active_job_started_at_utc,
        'blocked_job_run_at_utc', v_active_job_run_at_utc,
        'blocked_chain_job_id', CASE WHEN v_queued_chain_job_id IS NULL THEN NULL ELSE v_queued_chain_job_id::text END,
        'blocked_chain_job_type', v_queued_chain_job_type,
        'blocked_chain_job_status', v_queued_chain_job_status,
        'blocked_chain_started_at_utc', v_queued_chain_started_at_utc,
        'blocked_chain_run_at_utc', v_queued_chain_run_at_utc,
        'active_chain_id', COALESCE(v_active_chain_id, v_queued_chain_id),
        'projection_run_id', CASE WHEN v_projection_blocker_id IS NULL THEN NULL ELSE v_projection_blocker_id::text END,
        'projection_status', v_projection_blocker_status,
        'projection_started_at_utc', v_projection_blocker_started_at_utc,
        'projection_updated_at_utc', v_projection_blocker_updated_at_utc,
        'candidate_serial_wait_reason', COALESCE(v_reason, 'CANDIDATE_SERIAL_BLOCKED')
      )),
      COALESCE(v_reason, 'CANDIDATE_SERIAL_BLOCKED'),
      NULL::uuid
    );
  END IF;

  RETURN jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'blocked', COALESCE(v_blocked, false),
    'candidate_serial_blocked', COALESCE(v_blocked, false),
    'reason', COALESCE(v_reason, 'CANDIDATE_SERIAL_CLEAR'),
    'candidate_serial_wait_reason', COALESCE(v_reason, 'CANDIDATE_SERIAL_CLEAR'),
    'candidate_id', v_candidate_id::text,
    'candidate_serial_key', v_serial_key,
    'is_chain_continuation', COALESCE(v_is_chain_continuation, false),
    'blocked_job_id', CASE WHEN v_active_job_id IS NULL THEN NULL ELSE v_active_job_id::text END,
    'blocking_job_id', CASE WHEN v_active_job_id IS NULL THEN NULL ELSE v_active_job_id::text END,
    'blocked_job_type', v_active_job_type,
    'blocked_job_status', v_active_job_status,
    'blocking_status', COALESCE(v_active_job_status, v_queued_chain_job_status, v_projection_blocker_status),
    'blocked_job_started_at_utc', v_active_job_started_at_utc,
    'blocked_job_run_at_utc', v_active_job_run_at_utc,
    'blocking_started_at_utc', COALESCE(v_active_job_started_at_utc, v_queued_chain_started_at_utc, v_projection_blocker_started_at_utc),
    'blocking_run_at_utc', COALESCE(v_active_job_run_at_utc, v_queued_chain_run_at_utc, v_projection_blocker_updated_at_utc),
    'blocked_chain_job_id', CASE WHEN v_queued_chain_job_id IS NULL THEN NULL ELSE v_queued_chain_job_id::text END,
    'blocked_chain_job_type', v_queued_chain_job_type,
    'blocked_chain_job_status', v_queued_chain_job_status,
    'blocked_chain_started_at_utc', v_queued_chain_started_at_utc,
    'blocked_chain_run_at_utc', v_queued_chain_run_at_utc,
    'active_chain_id', COALESCE(v_active_chain_id, v_queued_chain_id),
    'projection_run_id', CASE WHEN v_projection_blocker_id IS NULL THEN NULL ELSE v_projection_blocker_id::text END,
    'blocking_projection_run_id', CASE WHEN v_projection_blocker_id IS NULL THEN NULL ELSE v_projection_blocker_id::text END,
    'projection_status', v_projection_blocker_status,
    'projection_started_at_utc', v_projection_blocker_started_at_utc,
    'projection_updated_at_utc', v_projection_blocker_updated_at_utc
  ));
END;
$function$;

-- _pay_workbench_candidate_serial_audit(text,uuid,uuid,jsonb,text,uuid)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_audit(p_action text, p_job_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_after_json jsonb DEFAULT '{}'::jsonb, p_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_action text := COALESCE(NULLIF(BTRIM(p_action), ''), 'CANDIDATE_SERIAL_EVENT');
  v_candidate_id uuid := p_candidate_id;
  v_after jsonb := CASE WHEN jsonb_typeof(COALESCE(p_after_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_after_json, '{}'::jsonb) ELSE '{}'::jsonb END;
BEGIN
  IF v_candidate_id IS NULL THEN
    v_candidate_id := public._pay_workbench_candidate_serial_candidate_id(NULL::uuid, v_after);
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    COALESCE(p_job_id::text, CASE WHEN v_candidate_id IS NULL THEN NULL ELSE 'candidate:' || v_candidate_id::text END),
    v_action,
    NULL::jsonb,
    jsonb_strip_nulls(
      jsonb_build_object(
        'job_id', CASE WHEN p_job_id IS NULL THEN NULL ELSE p_job_id::text END,
        'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
        'candidate_serial_key', public._pay_workbench_candidate_serial_key(v_candidate_id)
      ) || v_after
    ),
    COALESCE(NULLIF(BTRIM(p_reason), ''), v_action),
    p_actor_user_id
  );
EXCEPTION WHEN OTHERS THEN
  RETURN;
END;
$function$;

-- _pay_workbench_candidate_serial_candidate_id(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_candidate_id(p_candidate_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_candidate_id uuid := NULL::uuid;
  v_candidate_count integer := 0;
BEGIN
  IF p_candidate_id IS NOT NULL THEN
    RETURN p_candidate_id;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_candidate_count
  FROM public._pay_workbench_candidate_serial_candidate_ids(p_candidate_id, p_payload_json) AS candidate_ids(candidate_id);

  IF COALESCE(v_candidate_count, 0) = 1 THEN
    SELECT candidate_ids.candidate_id
    INTO v_candidate_id
    FROM public._pay_workbench_candidate_serial_candidate_ids(p_candidate_id, p_payload_json) AS candidate_ids(candidate_id)
    LIMIT 1;

    RETURN v_candidate_id;
  END IF;

  RETURN NULL::uuid;
END;
$function$;

-- _pay_workbench_candidate_serial_candidate_ids(uuid,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_candidate_ids(p_candidate_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS SETOF uuid
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
      DECLARE
        v_payload jsonb := CASE
            WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object'
                  THEN COALESCE(p_payload_json, '{}'::jsonb)
                      ELSE '{}'::jsonb
                        END;
                          v_uuid_regex constant text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
                          BEGIN
                            /*
                                Compatibility rule used by the existing singular wrapper:

                                      _pay_workbench_candidate_serial_candidate_id(p_candidate_id, payload)

                                          already returns p_candidate_id immediately when supplied. Keep the plural
                                              helper aligned with that behaviour: an explicit argument is authoritative.
                                                */
                                                  IF p_candidate_id IS NOT NULL THEN
                                                      RETURN NEXT p_candidate_id;
                                                          RETURN;
                                                            END IF;

                                                              RETURN QUERY
                                                                WITH scalar_values(raw_value, source_rank) AS (
                                                                    SELECT v_payload->>'candidate_serial_candidate_id', 10
                                                                        UNION ALL SELECT v_payload->>'candidate_id', 20
                                                                            UNION ALL SELECT v_payload->>'candidateId', 30
                                                                                UNION ALL SELECT v_payload->>'candidate_uuid', 40
                                                                                    UNION ALL SELECT v_payload#>>'{candidate,id}', 50
                                                                                        UNION ALL SELECT v_payload#>>'{candidate,candidate_id}', 60
                                                                                            UNION ALL SELECT v_payload#>>'{candidate,candidate_uuid}', 70

                                                                                                UNION ALL SELECT v_payload#>>'{scope,candidate_id}', 80
                                                                                                    UNION ALL SELECT v_payload#>>'{scope,candidate,id}', 90

                                                                                                        UNION ALL SELECT v_payload#>>'{source,candidate_id}', 100
                                                                                                            UNION ALL SELECT v_payload#>>'{source,candidate,id}', 110

                                                                                                                UNION ALL SELECT v_payload#>>'{source_build,candidate_id}', 120
                                                                                                                    UNION ALL SELECT v_payload#>>'{classifier_result,candidate_id}', 130
                                                                                                                        UNION ALL SELECT v_payload#>>'{preview_decisions_json,candidate_id}', 140
                                                                                                                            UNION ALL SELECT v_payload#>>'{result,candidate_id}', 150

                                                                                                                                UNION ALL SELECT v_payload#>>'{cursor,candidate_id}', 160
                                                                                                                                    UNION ALL SELECT v_payload#>>'{cursor,candidate,id}', 170
                                                                                                                                        UNION ALL SELECT v_payload#>>'{cursor,cursor_candidate_id}', 180

                                                                                                                                            UNION ALL SELECT v_payload#>>'{cursor_json,candidate_id}', 190
                                                                                                                                                UNION ALL SELECT v_payload#>>'{cursor_json,candidate,id}', 200
                                                                                                                                                    UNION ALL SELECT v_payload#>>'{cursor_json,cursor_candidate_id}', 210

                                                                                                                                                        UNION ALL
                                                                                                                                                            SELECT
                                                                                                                                                                  CASE
                                                                                                                                                                          WHEN UPPER(BTRIM(COALESCE(v_payload->>'scope_kind', v_payload->>'scope_type', ''))) = 'CANDIDATE'
                                                                                                                                                                                    THEN v_payload->>'scope_id'
                                                                                                                                                                                            ELSE NULL::text
                                                                                                                                                                                                  END,
                                                                                                                                                                                                        220
                                                                                                                                                                                                          ),
                                                                                                                                                                                                            array_sources(key_name, source_rank) AS (
                                                                                                                                                                                                                VALUES
                                                                                                                                                                                                                      ('candidate_ids'::text, 1000),
                                                                                                                                                                                                                            ('candidateIds'::text, 1100),
                                                                                                                                                                                                                                  ('candidate_uuids'::text, 1200),
                                                                                                                                                                                                                                        ('targeted_candidate_ids'::text, 1300),
                                                                                                                                                                                                                                              ('targeted_refresh_candidate_ids'::text, 1400),
                                                                                                                                                                                                                                                    ('affected_candidate_ids'::text, 1500)
                                                                                                                                                                                                                                                      ),
                                                                                                                                                                                                                                                        array_values(raw_value, source_rank) AS (
                                                                                                                                                                                                                                                            SELECT
                                                                                                                                                                                                                                                                  CASE
                                                                                                                                                                                                                                                                          WHEN jsonb_typeof(array_item.value) IN ('string', 'number') THEN
                                                                                                                                                                                                                                                                                    array_item.value #>> '{}'
                                                                                                                                                                                                                                                                                            WHEN jsonb_typeof(array_item.value) = 'object' THEN
                                                                                                                                                                                                                                                                                                      COALESCE(
                                                                                                                                                                                                                                                                                                                  array_item.value->>'candidate_serial_candidate_id',
                                                                                                                                                                                                                                                                                                                              array_item.value->>'candidate_id',
                                                                                                                                                                                                                                                                                                                                          array_item.value->>'candidateId',
                                                                                                                                                                                                                                                                                                                                                      array_item.value->>'candidate_uuid',
                                                                                                                                                                                                                                                                                                                                                                  array_item.value->>'id',
                                                                                                                                                                                                                                                                                                                                                                              array_item.value#>>'{candidate,id}',
                                                                                                                                                                                                                                                                                                                                                                                          array_item.value#>>'{candidate,candidate_id}',
                                                                                                                                                                                                                                                                                                                                                                                                      array_item.value#>>'{candidate,candidate_uuid}'
                                                                                                                                                                                                                                                                                                                                                                                                                )
                                                                                                                                                                                                                                                                                                                                                                                                                        ELSE NULL::text
                                                                                                                                                                                                                                                                                                                                                                                                                              END AS raw_value,
                                                                                                                                                                                                                                                                                                                                                                                                                                    array_sources.source_rank + array_item.ordinality::integer AS source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                        FROM array_sources
                                                                                                                                                                                                                                                                                                                                                                                                                                            CROSS JOIN LATERAL jsonb_array_elements(
                                                                                                                                                                                                                                                                                                                                                                                                                                                  CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                          WHEN jsonb_typeof(v_payload -> array_sources.key_name) = 'array'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                    THEN v_payload -> array_sources.key_name
                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHEN jsonb_typeof(v_payload -> array_sources.key_name) IN ('string', 'number')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      THEN jsonb_build_array(v_payload -> array_sources.key_name)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              ELSE '[]'::jsonb
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    END
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        ) WITH ORDINALITY AS array_item(value, ordinality)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            raw_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                SELECT scalar_values.raw_value, scalar_values.source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    FROM scalar_values

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        UNION ALL

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            SELECT array_values.raw_value, array_values.source_rank
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                FROM array_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    cleaned_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        SELECT DISTINCT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              NULLIF(BTRIM(raw_values.raw_value), '') AS raw_value
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  FROM raw_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      WHERE raw_values.raw_value IS NOT NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        ),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          parsed_values AS (
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              SELECT
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    cleaned_values.raw_value::uuid AS candidate_uuid
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        FROM cleaned_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHERE cleaned_values.raw_value ~* v_uuid_regex
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              )
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                SELECT DISTINCT parsed_values.candidate_uuid
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  FROM parsed_values
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    WHERE parsed_values.candidate_uuid IS NOT NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ORDER BY parsed_values.candidate_uuid;

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        RETURN;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        END;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        $function$;

-- _pay_workbench_candidate_serial_is_candidate_job(text,uuid,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_is_candidate_job(p_job_type text, p_candidate_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_job_type text := UPPER(BTRIM(COALESCE(p_job_type, '')));
  v_candidate_id uuid := public._pay_workbench_candidate_serial_candidate_id(p_candidate_id, p_payload_json);
BEGIN
  IF v_candidate_id IS NULL THEN
    RETURN false;
  END IF;

  IF v_job_type IN ('WORKBENCH_SESSION_SCOPE_SEED', 'SESSION_SCOPE_SEED', 'WORKBENCH_SESSION_CLONE_REBASE', 'SESSION_CLONE_REBASE', 'CLONE_REBASE') THEN
    RETURN false;
  END IF;

  IF v_job_type IN (
    'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    'WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH',
    'WORKBENCH_CANDIDATE_SOURCE_BUILD', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK', 'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE', 'CANDIDATE_SOURCE_BUILD', 'CANDIDATE_SOURCE_BUILD_CHUNK', 'SOURCE_BUILD', 'SOURCE_BUILD_PAGE',
    'WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE', 'CANDIDATE_LINE_WORK_SEED', 'CANDIDATE_LINE_WORK_SEED_PAGE', 'LINE_WORK_SEED_PAGE', 'SNAPSHOT_CANDIDATE_REFRESH', 'CANDIDATE_REFRESH',
    'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'CANDIDATE_LINE_WORK_PROCESS', 'CANDIDATE_LINE_WORK_PROCESS_CHUNK', 'LINE_WORK_PROCESS', 'LINE_WORK_PROCESS_CHUNK',
    'WORKBENCH_PREVIEW_ROWS_MATERIALISE', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE', 'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK', 'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROWS_MATERIALISE', 'PREVIEW_ROWS_MATERIALIZE', 'PREVIEW_ROWS_MATERIALISE_CHUNK', 'PREVIEW_ROWS_MATERIALIZE_CHUNK', 'PREVIEW_ROW_MATERIALISE_CHUNK', 'PREVIEW_ROW_MATERIALIZE_CHUNK',
    'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
    'CONTRACT_CLIENT_DIRTY_FANOUT'
  ) THEN
    RETURN true;
  END IF;

  IF v_job_type LIKE E'WORKBENCH_CANDIDATE\\_%' ESCAPE E'\\' THEN
    RETURN true;
  END IF;

  IF v_job_type LIKE E'WORKBENCH\\_%' ESCAPE E'\\'
     AND (
       COALESCE(p_payload_json, '{}'::jsonb) ? 'candidate_id'
       OR COALESCE(p_payload_json, '{}'::jsonb) ? 'candidateId'
       OR COALESCE(p_payload_json, '{}'::jsonb) ? 'candidate_uuid'
       OR COALESCE(p_payload_json, '{}'::jsonb)#>>'{candidate,id}' IS NOT NULL
     ) THEN
    RETURN true;
  END IF;

  RETURN false;
END;
$function$;

-- _pay_workbench_candidate_serial_key(uuid)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_key(p_candidate_id uuid)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT CASE
    WHEN p_candidate_id IS NULL THEN NULL::text
    ELSE 'WORKBENCH_CANDIDATE_SERIAL:candidate:' || p_candidate_id::text
  END;
$function$;

-- _pay_workbench_candidate_serial_try_gate(uuid,uuid,text,jsonb,text)
CREATE OR REPLACE FUNCTION public._pay_workbench_candidate_serial_try_gate(p_job_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_job_type text DEFAULT NULL::text, p_payload_json jsonb DEFAULT '{}'::jsonb, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_payload jsonb := CASE WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_candidate_id uuid := public._pay_workbench_candidate_serial_candidate_id(p_candidate_id, p_payload_json);
  v_serial_key text := public._pay_workbench_candidate_serial_key(v_candidate_id);
  v_required boolean := false;
  v_locked boolean := false;
  v_state jsonb := '{}'::jsonb;
  v_blocked boolean := false;
  v_reason text := NULL::text;
  v_result jsonb := '{}'::jsonb;
BEGIN
  v_required := public._pay_workbench_candidate_serial_is_candidate_job(p_job_type, v_candidate_id, v_payload);

  IF v_candidate_id IS NULL OR v_serial_key IS NULL OR v_required IS NOT TRUE THEN
    v_state := public._pay_workbench_candidate_serial_active_state(p_job_id, v_candidate_id, p_job_type, v_payload, NULL::timestamptz);
    RETURN COALESCE(v_state, '{}'::jsonb) || jsonb_strip_nulls(jsonb_build_object(
      'ok', true, 'allowed', true, 'blocked', false, 'candidate_serial_blocked', false,
      'reason', COALESCE(v_state->>'reason', 'NO_CANDIDATE_SERIAL_SCOPE'),
      'candidate_serial_wait_reason', COALESCE(v_state->>'candidate_serial_wait_reason', v_state->>'reason', 'NO_CANDIDATE_SERIAL_SCOPE'),
      'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
      'candidate_serial_key', v_serial_key,
      'candidate_serial_gate_reason', NULLIF(BTRIM(COALESCE(p_reason, '')), ''),
      'candidate_serial_gate_decision', 'BYPASSED',
      'advisory_lock_attempted', false
    ));
  END IF;

  v_locked := COALESCE(pg_try_advisory_xact_lock(hashtextextended(v_serial_key, 24062027)), false);
  v_state := public._pay_workbench_candidate_serial_active_state(p_job_id, v_candidate_id, p_job_type, v_payload, NULL::timestamptz);
  v_blocked := v_locked IS NOT TRUE
    OR lower(BTRIM(COALESCE(v_state->>'blocked', v_state->>'candidate_serial_blocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_reason := CASE
    WHEN v_locked IS NOT TRUE THEN 'CANDIDATE_SERIAL_BLOCKED_BY_ADVISORY_LOCK'
    ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_state->>'reason', '')), ''), CASE WHEN v_blocked THEN 'CANDIDATE_SERIAL_BLOCKED' ELSE 'CANDIDATE_SERIAL_CLEAR' END)
  END;

  v_result := COALESCE(v_state, '{}'::jsonb) || jsonb_strip_nulls(jsonb_build_object(
    'ok', true, 'allowed', NOT COALESCE(v_blocked, false), 'blocked', COALESCE(v_blocked, false),
    'candidate_serial_blocked', COALESCE(v_blocked, false), 'reason', v_reason,
    'candidate_serial_wait_reason', v_reason, 'candidate_id', v_candidate_id::text,
    'candidate_serial_key', v_serial_key,
    'candidate_serial_gate_reason', NULLIF(BTRIM(COALESCE(p_reason, '')), ''),
    'candidate_serial_gate_decision', CASE WHEN v_blocked THEN 'BLOCKED' ELSE 'GRANTED' END,
    'advisory_lock_attempted', true, 'advisory_lock_granted', COALESCE(v_locked, false)
  ));

  PERFORM public._pay_workbench_candidate_serial_audit(
    CASE WHEN v_blocked THEN 'CANDIDATE_SERIAL_GATE_BLOCKED' ELSE 'CANDIDATE_SERIAL_GATE_GRANTED' END,
    p_job_id, v_candidate_id, v_result || jsonb_build_object('job_type', p_job_type),
    COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), v_reason), NULL::uuid
  );
  RETURN v_result;
END;
$function$;

-- _pay_workbench_case_resolution_carry_process_candidate_v1(uuid,uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public._pay_workbench_case_resolution_carry_process_candidate_v1(p_target_session_id uuid, p_candidate_id uuid, p_target_source_build_run_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_catalog', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
DECLARE
  v_registration record;
  v_snapshot jsonb;
  v_source_resolution jsonb;
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_target_fingerprint text;
  v_source_authority_fingerprint text;
  v_target_authority_fingerprint text;
  v_target_resolution_id uuid;
  v_existing_id uuid;
  v_existing_payload_json jsonb;
  v_existing_updated_at_utc timestamptz;
  v_status text;
  v_reason_code text;
  v_carried integer := 0;
  v_stale integer := 0;
  v_incompatible integer := 0;
  v_superseded integer := 0;
  v_deferred integer := 0;
  v_actor_user_id uuid;
  v_target_pay_date date;
BEGIN
  IF p_target_session_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  SELECT actor_user_id, pay_date
  INTO v_actor_user_id, v_target_pay_date
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_target_session_id
    AND status = 'OPEN'
    AND discarded_at_utc IS NULL
    AND replacement_session_id IS NULL;

  IF v_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CASE_RESOLUTION_CARRY_TARGET_OBSOLETE'
      USING ERRCODE = '55000';
  END IF;

  FOR v_registration IN
    SELECT registration_row.*
    FROM
      public.banking_pay_workbench_case_resolution_carry_registrations
        registration_row
    WHERE registration_row.target_session_id = p_target_session_id
      AND registration_row.candidate_id = p_candidate_id
      AND registration_row.status = 'PENDING'
    ORDER BY registration_row.source_priority,
             registration_row.created_at_utc,
             registration_row.id
    FOR UPDATE
  LOOP
    v_snapshot := v_registration.source_resolution_snapshot_json;
    v_source_resolution := v_snapshot -> 'resolution';
    v_status := NULL;
    v_reason_code := NULL;
    v_target_fingerprint := NULL;
    v_source_authority_fingerprint := NULL;
    v_target_authority_fingerprint := NULL;
    v_target_resolution_id := NULL;
    v_existing_id := NULL;
    v_existing_payload_json := NULL;
    v_existing_updated_at_utc := NULL;
    v_residual := NULL;
    v_component := NULL;

    IF v_registration.resolution_scope_kind = 'CORRECTION_COMPONENT' THEN
      v_residuals :=
        public._ctms_candidate_correction_residuals_v1(
          p_target_session_id,
          p_candidate_id,
          NULL::uuid,
          'PAY_WORKBENCH_CARRY_PROCESS'
        );

      SELECT residual.value, component.value
      INTO v_residual, v_component
      FROM jsonb_array_elements(
        coalesce(v_residuals, '[]'::jsonb)
      ) residual(value)
      CROSS JOIN LATERAL jsonb_array_elements(
        coalesce(residual.value -> 'components', '[]'::jsonb)
      ) component(value)
      WHERE component.value ->> 'canonical_correction_key'
        = v_registration.canonical_resolution_key
      LIMIT 1;

      IF v_component IS NULL THEN
        UPDATE
          public.banking_pay_workbench_case_resolution_carry_registrations
        SET attempt_count = attempt_count + 1,
            last_attempt_at_utc = coalesce(p_now_utc, now()),
            last_error_json = jsonb_build_object(
              'code', 'TARGET_EVIDENCE_NOT_READY',
              'target_source_build_run_id', p_target_source_build_run_id
            ),
            updated_at_utc = coalesce(p_now_utc, now())
        WHERE id = v_registration.id;
        v_deferred := v_deferred + 1;
        CONTINUE;
      END IF;

      v_target_fingerprint :=
        nullif(v_component ->> 'resolution_economic_fingerprint', '');
      v_source_authority_fingerprint :=
        nullif(
          v_snapshot
            ->> 'source_resolution_authority_fingerprint',
          ''
        );
      v_target_authority_fingerprint :=
        public._ctms_correction_resolution_authority_fingerprint_v1(
          v_residual,
          v_component
        );

      IF v_target_fingerprint IS NULL
         OR v_source_authority_fingerprint IS NULL
         OR v_target_authority_fingerprint IS NULL THEN
        v_status := 'INCOMPATIBLE';
        v_reason_code := 'AUTHORITY_OR_ECONOMIC_FINGERPRINT_MISSING';
      ELSIF v_target_authority_fingerprint
        IS DISTINCT FROM v_source_authority_fingerprint THEN
        v_status := 'STALE';
        v_reason_code := CASE
          WHEN v_component ->> 'component_lineage_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_component,component_lineage_fingerprint}'
            THEN 'MEMBER_SET_CHANGED'
          WHEN v_component ->> 'source_basis_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_component,source_basis_fingerprint}'
            THEN 'SOURCE_BASIS_CHANGED'
          WHEN v_residual
              ->> 'correction_financials_policy_envelope_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>>
                '{correction_residual,correction_financials_policy_envelope_fingerprint}'
            THEN 'POLICY_ENVELOPE_CHANGED'
          WHEN v_residual ->> 'target_pay_method'
            IS DISTINCT FROM
              v_snapshot #>> '{correction_residual,target_pay_method}'
            THEN 'TARGET_PAY_METHOD_CHANGED'
          WHEN v_residual ->> 'residual_fingerprint'
            IS DISTINCT FROM
              v_snapshot #>> '{correction_residual,residual_fingerprint}'
            THEN 'RESIDUAL_FINGERPRINT_CHANGED'
          ELSE 'RATE_OR_AMOUNT_CHANGED'
        END;
      ELSE
        SELECT target_resolution.id,
               target_resolution.payload_json,
               target_resolution.updated_at_utc
        INTO v_existing_id,
             v_existing_payload_json,
             v_existing_updated_at_utc
        FROM public.banking_pay_workbench_session_case_resolutions
          target_resolution
        WHERE target_resolution.session_id = p_target_session_id
          AND target_resolution.resolution_identity_key =
            v_registration.canonical_resolution_key
        ORDER BY target_resolution.updated_at_utc DESC,
                 target_resolution.id DESC
        LIMIT 1
        FOR UPDATE;

        IF v_existing_id IS NOT NULL
           AND coalesce(
             v_existing_payload_json ->> 'carry_registration_id',
             ''
           ) <> v_registration.id::text THEN
          v_status := 'SUPERSEDED';
          v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
          v_target_resolution_id := v_existing_id;
        ELSE
          INSERT INTO
            public.banking_pay_workbench_session_case_resolutions (
              id,
              session_id,
              candidate_id,
              case_key,
              resolution_family,
              resolution_identity_key,
              timesheet_id,
              source_basis_fingerprint,
              source_family_key,
              bucket_code,
              component_key_type,
              component_key_value,
              payload_json,
              resolution_origin_session_id,
              resolution_origin_pay_date,
              resolution_origin_source_basis_fingerprint,
              created_at_utc,
              updated_at_utc
            )
          VALUES (
            coalesce(v_existing_id, gen_random_uuid()),
            p_target_session_id,
            p_candidate_id,
            coalesce(
              v_source_resolution ->> 'case_key',
              'timesheet:' ||
                coalesce(v_component ->> 'carrier_timesheet_id', '')
            ),
            coalesce(
              v_source_resolution ->> 'resolution_family',
              'BUCKETED'
            ),
            v_registration.canonical_resolution_key,
            nullif(v_component ->> 'carrier_timesheet_id', '')::uuid,
            v_component ->> 'source_basis_fingerprint',
            v_residual ->> 'source_family_key',
            v_source_resolution ->> 'bucket_code',
            upper(v_component ->> 'component_key_type'),
            v_component ->> 'component_key_value',
            coalesce(v_source_resolution -> 'payload_json', '{}'::jsonb)
              || jsonb_build_object(
                'resolution_identity_key',
                  v_registration.canonical_resolution_key,
                'resolution_identity_version', 'CORRECTION_CHAIN_V1',
                'canonical_correction_key',
                  v_registration.canonical_resolution_key,
                'resolution_economic_fingerprint',
                  v_target_fingerprint,
                'source_resolution_id',
                  v_registration.source_resolution_id,
                'source_resolution_identity_key',
                  v_registration.source_resolution_identity_key,
                'carry_registration_id', v_registration.id,
                'resolution_origin_session_id',
                  v_registration.source_session_id,
                'resolution_origin_pay_date',
                  v_source_resolution ->> 'resolution_origin_pay_date',
                'resolution_origin_source_basis_fingerprint',
                  coalesce(
                    v_source_resolution
                      ->> 'resolution_origin_source_basis_fingerprint',
                    v_source_resolution ->> 'source_basis_fingerprint'
                  ),
                'clone_carried_forward', true,
                'clone_validation_status', 'VALID',
                'requires_review', false,
                'target_session_id', p_target_session_id,
                'target_source_build_run_id',
                  p_target_source_build_run_id,
                'carried_forward_at_utc',
                  coalesce(p_now_utc, now()),
                'correction_chain_fingerprint',
                  v_residual ->> 'chain_fingerprint',
                'correction_chain_residual_fingerprint',
                  v_residual ->> 'residual_fingerprint',
                'correction_financials_policy_envelope',
                  v_residual
                    -> 'correction_financials_policy_envelope',
                'correction_financials_policy_envelope_fingerprint',
                  v_residual
                    ->> 'correction_financials_policy_envelope_fingerprint',
                'policy_x_authority_scope',
                  'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
              ),
            coalesce(
              nullif(
                v_source_resolution ->> 'resolution_origin_session_id',
                ''
              )::uuid,
              v_registration.source_session_id
            ),
            coalesce(
              nullif(
                v_source_resolution ->> 'resolution_origin_pay_date',
                ''
              )::date,
              v_target_pay_date
            ),
            coalesce(
              v_source_resolution
                ->> 'resolution_origin_source_basis_fingerprint',
              v_source_resolution ->> 'source_basis_fingerprint'
            ),
            coalesce(p_now_utc, now()),
            coalesce(p_now_utc, now())
          )
          ON CONFLICT (session_id, resolution_identity_key)
          DO UPDATE
          SET candidate_id = EXCLUDED.candidate_id,
              case_key = EXCLUDED.case_key,
              resolution_family = EXCLUDED.resolution_family,
              timesheet_id = EXCLUDED.timesheet_id,
              source_basis_fingerprint =
                EXCLUDED.source_basis_fingerprint,
              source_family_key = EXCLUDED.source_family_key,
              bucket_code = EXCLUDED.bucket_code,
              component_key_type = EXCLUDED.component_key_type,
              component_key_value = EXCLUDED.component_key_value,
              payload_json = EXCLUDED.payload_json,
              updated_at_utc = EXCLUDED.updated_at_utc
          WHERE
            public.banking_pay_workbench_session_case_resolutions
              .payload_json ->> 'carry_registration_id'
              = v_registration.id::text
          RETURNING id
          INTO v_target_resolution_id;

          IF v_target_resolution_id IS NULL THEN
            v_status := 'SUPERSEDED';
            v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
          ELSE
            PERFORM
              public._ctms_normalise_correction_case_resolutions_v1(
                p_target_session_id,
                p_candidate_id,
                nullif(
                  v_component ->> 'carrier_timesheet_id',
                  ''
                )::uuid
              );

            v_residuals :=
              public._ctms_candidate_correction_residuals_v1(
                p_target_session_id,
                p_candidate_id,
                NULL::uuid,
                'PAY_WORKBENCH_CARRY_VALIDATE_DECISION'
              );

            v_residual := NULL;
            v_component := NULL;
            SELECT residual.value, component.value
            INTO v_residual, v_component
            FROM jsonb_array_elements(
              coalesce(v_residuals, '[]'::jsonb)
            ) residual(value)
            CROSS JOIN LATERAL jsonb_array_elements(
              coalesce(residual.value -> 'components', '[]'::jsonb)
            ) component(value)
            WHERE component.value ->> 'canonical_correction_key'
              = v_registration.canonical_resolution_key
            LIMIT 1;

            v_target_fingerprint :=
              nullif(
                v_component ->> 'resolution_economic_fingerprint',
                ''
              );

            IF v_target_fingerprint IS NULL
               OR v_target_fingerprint IS DISTINCT FROM
                    v_registration.source_economic_fingerprint THEN
              DELETE FROM
                public.banking_pay_workbench_session_case_resolutions
              WHERE session_id = p_target_session_id
                AND payload_json ->> 'carry_registration_id'
                  = v_registration.id::text;
              v_target_resolution_id := NULL;
              v_status := 'STALE';
              v_reason_code := 'DECISION_RESULT_CHANGED';
            ELSE
              v_status := 'CARRIED';
            END IF;
          END IF;
        END IF;
      END IF;
    ELSE
      IF EXISTS (
        SELECT 1
        FROM public.banking_pay_snapshot_case_component_state
          target_component
        JOIN public.banking_pay_workbench_sessions target_session
          ON target_session.id = p_target_session_id
         AND target_component.snapshot_run_id =
           target_session.source_snapshot_run_id
        WHERE target_component.candidate_id = p_candidate_id
          AND target_component.case_key
            IS NOT DISTINCT FROM
              v_source_resolution ->> 'case_key'
          AND target_component.source_basis_fingerprint
            IS NOT DISTINCT FROM
              v_source_resolution ->> 'source_basis_fingerprint'
      ) THEN
        INSERT INTO
          public.banking_pay_workbench_session_case_resolutions (
            session_id,
            candidate_id,
            case_key,
            resolution_family,
            resolution_identity_key,
            timesheet_id,
            source_basis_fingerprint,
            source_family_key,
            bucket_code,
            component_key_type,
            component_key_value,
            payload_json,
            resolution_origin_session_id,
            resolution_origin_pay_date,
            resolution_origin_source_basis_fingerprint,
            created_at_utc,
            updated_at_utc
          )
        VALUES (
          p_target_session_id,
          p_candidate_id,
          v_source_resolution ->> 'case_key',
          v_source_resolution ->> 'resolution_family',
          v_registration.canonical_resolution_key,
          nullif(v_source_resolution ->> 'timesheet_id', '')::uuid,
          v_source_resolution ->> 'source_basis_fingerprint',
          v_source_resolution ->> 'source_family_key',
          v_source_resolution ->> 'bucket_code',
          v_source_resolution ->> 'component_key_type',
          v_source_resolution ->> 'component_key_value',
          coalesce(v_source_resolution -> 'payload_json', '{}'::jsonb)
            || jsonb_build_object(
              'carry_registration_id', v_registration.id,
              'source_resolution_id', v_registration.source_resolution_id,
              'source_resolution_identity_key',
                v_registration.source_resolution_identity_key,
              'clone_carried_forward', true,
              'clone_validation_status', 'VALID',
              'requires_review', false,
              'target_source_build_run_id',
                p_target_source_build_run_id,
              'carried_forward_at_utc', coalesce(p_now_utc, now()),
              'policy_x_authority_scope',
                'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
            ),
          coalesce(
            nullif(
              v_source_resolution ->> 'resolution_origin_session_id',
              ''
            )::uuid,
            v_registration.source_session_id
          ),
          coalesce(
            nullif(
              v_source_resolution ->> 'resolution_origin_pay_date',
              ''
            )::date,
            v_target_pay_date
          ),
          coalesce(
            v_source_resolution
              ->> 'resolution_origin_source_basis_fingerprint',
            v_source_resolution ->> 'source_basis_fingerprint'
          ),
          coalesce(p_now_utc, now()),
          coalesce(p_now_utc, now())
        )
        ON CONFLICT (session_id, resolution_identity_key)
        DO NOTHING
        RETURNING id
        INTO v_target_resolution_id;

        IF v_target_resolution_id IS NULL THEN
          SELECT id
          INTO v_target_resolution_id
          FROM public.banking_pay_workbench_session_case_resolutions
          WHERE session_id = p_target_session_id
            AND resolution_identity_key =
              v_registration.canonical_resolution_key;
          v_status := 'SUPERSEDED';
          v_reason_code := 'TARGET_AUTHORITATIVE_DECISION_EXISTS';
        ELSE
          v_status := 'CARRIED';
          v_target_fingerprint :=
            v_registration.source_economic_fingerprint;
        END IF;
      ELSE
        UPDATE
          public.banking_pay_workbench_case_resolution_carry_registrations
        SET attempt_count = attempt_count + 1,
            last_attempt_at_utc = coalesce(p_now_utc, now()),
            last_error_json = jsonb_build_object(
              'code', 'TARGET_EVIDENCE_NOT_READY',
              'target_source_build_run_id', p_target_source_build_run_id
            ),
            updated_at_utc = coalesce(p_now_utc, now())
        WHERE id = v_registration.id;
        v_deferred := v_deferred + 1;
        CONTINUE;
      END IF;
    END IF;

    UPDATE public.banking_pay_workbench_case_resolution_carry_registrations
    SET status = v_status,
        state_reason_code = v_reason_code,
        target_source_build_run_id = p_target_source_build_run_id,
        target_resolution_id = v_target_resolution_id,
        target_economic_fingerprint = v_target_fingerprint,
        attempt_count = attempt_count + 1,
        last_attempt_at_utc = coalesce(p_now_utc, now()),
        last_error_json = CASE
          WHEN v_status IN ('STALE', 'INCOMPATIBLE')
            THEN jsonb_build_object('code', v_reason_code)
          ELSE NULL
        END,
        updated_at_utc = coalesce(p_now_utc, now()),
        completed_at_utc = coalesce(p_now_utc, now())
    WHERE id = v_registration.id;

    INSERT INTO public.audit_events (
      actor_user_id,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    VALUES (
      v_actor_user_id,
      'BANKING_PAY_CASE_RESOLUTION_CARRY',
      v_registration.id::text,
      'CARRY_' || v_status,
      jsonb_build_object('status', 'PENDING'),
      jsonb_strip_nulls(
        jsonb_build_object(
          'registration_id', v_registration.id,
          'source_session_id', v_registration.source_session_id,
          'target_session_id', p_target_session_id,
          'source_resolution_id',
            v_registration.source_resolution_id,
          'target_resolution_id', v_target_resolution_id,
          'candidate_id', p_candidate_id,
          'canonical_resolution_key',
            v_registration.canonical_resolution_key,
          'source_economic_fingerprint',
            v_registration.source_economic_fingerprint,
          'target_economic_fingerprint', v_target_fingerprint,
          'status', v_status,
          'state_reason_code', v_reason_code
        )
      ),
      v_registration.carry_reason
    );

    v_carried := v_carried + CASE WHEN v_status = 'CARRIED' THEN 1 ELSE 0 END;
    v_stale := v_stale + CASE WHEN v_status = 'STALE' THEN 1 ELSE 0 END;
    v_incompatible :=
      v_incompatible + CASE WHEN v_status = 'INCOMPATIBLE' THEN 1 ELSE 0 END;
    v_superseded :=
      v_superseded + CASE WHEN v_status = 'SUPERSEDED' THEN 1 ELSE 0 END;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'target_session_id', p_target_session_id,
    'candidate_id', p_candidate_id,
    'target_source_build_run_id', p_target_source_build_run_id,
    'carried_count', v_carried,
    'stale_count', v_stale,
    'incompatible_count', v_incompatible,
    'superseded_count', v_superseded,
    'deferred_count', v_deferred,
    'policy_x_authority_scope',
      'PRE_DRAFT_CASE_RESOLUTION_STATE_ONLY'
  );
END;
$function$;

-- _pay_workbench_delta_projection_terminalise_if_orphaned(uuid,uuid,uuid,uuid,bigint,bigint,bigint,bigint,text,timestamp with time zone,uuid)
CREATE OR REPLACE FUNCTION public._pay_workbench_delta_projection_terminalise_if_orphaned(p_projection_run_id uuid, p_session_id uuid, p_candidate_id uuid, p_superseded_job_id uuid, p_payload_source_change_seq bigint DEFAULT 0, p_cursor_source_change_seq bigint DEFAULT 0, p_projection_run_source_change_seq bigint DEFAULT 0, p_live_candidate_source_change_seq bigint DEFAULT 0, p_reason text DEFAULT 'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM'::text, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_reason text := COALESCE(NULLIF(BTRIM(p_reason), ''), 'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM');
  v_run public.banking_pay_workbench_candidate_delta_projection_runs%ROWTYPE;
  v_status_before text := NULL::text;
  v_status_after text := NULL::text;
  v_effective_projection_source_change_seq bigint := 0;
  v_effective_latest_source_seq bigint := 0;
  v_active_job_id uuid := NULL::uuid;
  v_active_job_type text := NULL::text;
  v_active_job_status text := NULL::text;
  v_skip_reason text := NULL::text;
BEGIN
  IF p_projection_run_id IS NULL
     OR p_session_id IS NULL
     OR p_candidate_id IS NULL
     OR p_superseded_job_id IS NULL THEN
    v_skip_reason := 'MISSING_REQUIRED_SCOPE';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', CASE WHEN p_projection_run_id IS NULL THEN NULL ELSE p_projection_run_id::text END,
      'projection_run_terminalised', false,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  SELECT projection_run.*
  INTO v_run
  FROM public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
  WHERE projection_run.id = p_projection_run_id
  FOR UPDATE SKIP LOCKED;

  IF NOT FOUND THEN
    v_skip_reason := 'PROJECTION_RUN_LOCKED_OR_NOT_FOUND';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  v_status_before := v_run.status;
  v_effective_projection_source_change_seq := GREATEST(COALESCE(p_projection_run_source_change_seq, 0), COALESCE(v_run.source_change_seq, 0));
  v_effective_latest_source_seq := GREATEST(COALESCE(p_payload_source_change_seq, 0), COALESCE(p_live_candidate_source_change_seq, 0));

  IF v_run.session_id IS DISTINCT FROM p_session_id
     OR v_run.candidate_id IS DISTINCT FROM p_candidate_id THEN
    v_skip_reason := 'PROJECTION_SCOPE_MISMATCH';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_status_before', v_status_before,
      'projection_run_status_after', v_status_before,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'projection_run_session_id', v_run.session_id::text,
      'projection_run_candidate_id', v_run.candidate_id::text,
      'job_session_id', p_session_id::text,
      'job_candidate_id', p_candidate_id::text,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  IF UPPER(BTRIM(COALESCE(v_run.status, ''))) NOT IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS') THEN
    v_skip_reason := 'PROJECTION_RUN_ALREADY_TERMINAL';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_status_before', v_status_before,
      'projection_run_status_after', v_status_before,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  IF NOT (
    (COALESCE(v_effective_projection_source_change_seq, 0) > 0 AND COALESCE(v_effective_latest_source_seq, 0) > COALESCE(v_effective_projection_source_change_seq, 0))
    OR (COALESCE(p_cursor_source_change_seq, 0) > 0 AND COALESCE(v_effective_latest_source_seq, 0) > COALESCE(p_cursor_source_change_seq, 0))
  ) THEN
    v_skip_reason := 'NOT_STALE_AGAINST_LIVE_SOURCE_SEQ';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_status_before', v_status_before,
      'projection_run_status_after', v_status_before,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'payload_source_change_seq', p_payload_source_change_seq,
      'cursor_source_change_seq', p_cursor_source_change_seq,
      'projection_run_source_change_seq', v_effective_projection_source_change_seq,
      'live_candidate_source_change_seq', p_live_candidate_source_change_seq,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  SELECT active_projection_job.id,
         active_projection_job.job_type,
         active_projection_job.status
  INTO v_active_job_id,
       v_active_job_type,
       v_active_job_status
  FROM public.banking_pay_workbench_jobs AS active_projection_job
  WHERE active_projection_job.id IS DISTINCT FROM p_superseded_job_id
    AND UPPER(BTRIM(COALESCE(active_projection_job.status, ''))) IN ('QUEUED', 'RUNNING')
    AND (
      COALESCE(active_projection_job.payload_json->>'projection_run_id', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{cursor,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{cursor,cursor,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{cursor_json,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{cursor_json,cursor,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{result_json,next_cursor,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{result_json,next_cursor,cursor,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{result_json,next_cursor_json,projection_run_id}', '') = p_projection_run_id::text
      OR COALESCE(active_projection_job.payload_json#>>'{result_json,next_cursor_json,cursor,projection_run_id}', '') = p_projection_run_id::text
    )
  ORDER BY CASE WHEN UPPER(BTRIM(COALESCE(active_projection_job.status, ''))) = 'RUNNING' THEN 0 ELSE 1 END,
           active_projection_job.run_at_utc ASC,
           active_projection_job.created_at_utc ASC,
           active_projection_job.id ASC
  LIMIT 1;

  IF v_active_job_id IS NOT NULL THEN
    v_skip_reason := 'ACTIVE_JOB_CAN_COMPLETE_PROJECTION';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_status_before', v_status_before,
      'projection_run_status_after', v_status_before,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'active_continuation_job_exists', true,
      'active_continuation_job_id', v_active_job_id::text,
      'active_continuation_job_type', v_active_job_type,
      'active_continuation_job_status', v_active_job_status,
      'no_active_continuation_job', false,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS projection_run
  SET status = 'FAILED',
      fallback_required = false,
      fallback_reason = 'STALE_CONTINUATION_SUPERSEDED_BEFORE_CLAIM',
      completed_at_utc = v_now,
      updated_at_utc = v_now,
      diagnostics_json = jsonb_strip_nulls(
        COALESCE(projection_run.diagnostics_json, '{}'::jsonb)
        || jsonb_build_object(
          'stale_continuation_superseded_before_claim', true,
          'terminalised_by', 'pay_workbench_claim_due_jobs',
          'terminalised_reason', v_reason,
          'terminalised_at_utc', v_now::text,
          'superseded_continuation_job_id', p_superseded_job_id::text,
          'payload_source_change_seq', p_payload_source_change_seq,
          'cursor_source_change_seq', p_cursor_source_change_seq,
          'projection_run_source_change_seq', v_effective_projection_source_change_seq,
          'live_candidate_source_change_seq', p_live_candidate_source_change_seq,
          'newer_source_change_seq', v_effective_latest_source_seq,
          'superseded_by_newer_source_change_seq', true,
          'projection_status_before', v_status_before,
          'projection_status_after', 'FAILED',
          'no_active_continuation_job', true,
          'candidate_serial_unblocked', true,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        )
      )
  WHERE projection_run.id = p_projection_run_id
    AND projection_run.session_id = p_session_id
    AND projection_run.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(projection_run.status, ''))) IN ('RUNNING', 'PROCESSING', 'IN_PROGRESS')
  RETURNING projection_run.status
  INTO v_status_after;

  IF NOT FOUND THEN
    v_skip_reason := 'PROJECTION_RUN_STATUS_CHANGED_BEFORE_TERMINALISE';
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'projection_run_id', p_projection_run_id::text,
      'projection_run_terminalised', false,
      'projection_run_status_before', v_status_before,
      'projection_run_status_after', v_status_before,
      'projection_run_terminalised_reason', v_reason,
      'skip_reason', v_skip_reason,
      'no_active_continuation_job', true,
      'superseded_continuation_job_id', p_superseded_job_id::text,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ));
  END IF;

  BEGIN
    PERFORM public._audit_insert(
      'banking_pay_workbench_candidate_delta_projection_run',
      p_projection_run_id::text,
      'STALE_CONTINUATION_PROJECTION_RUN_TERMINALISED_BEFORE_CLAIM',
      jsonb_strip_nulls(jsonb_build_object(
        'projection_run_id', p_projection_run_id::text,
        'status', v_status_before,
        'source_change_seq', v_effective_projection_source_change_seq
      )),
      jsonb_strip_nulls(jsonb_build_object(
        'projection_run_id', p_projection_run_id::text,
        'projection_run_terminalised', true,
        'projection_run_status_before', v_status_before,
        'projection_run_status_after', COALESCE(v_status_after, 'FAILED'),
        'projection_run_terminalised_reason', v_reason,
        'terminalised_by', 'pay_workbench_claim_due_jobs',
        'superseded_continuation_job_id', p_superseded_job_id::text,
        'payload_source_change_seq', p_payload_source_change_seq,
        'cursor_source_change_seq', p_cursor_source_change_seq,
        'projection_run_source_change_seq', v_effective_projection_source_change_seq,
        'live_candidate_source_change_seq', p_live_candidate_source_change_seq,
        'newer_source_change_seq', v_effective_latest_source_seq,
        'no_active_continuation_job', true,
        'candidate_serial_unblocked', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )),
      v_reason,
      p_actor_user_id
    );
  EXCEPTION WHEN OTHERS THEN
    -- Audit must not re-open the ghost projection-run blocker if the state transition succeeded.
    NULL;
  END;

  RETURN jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'projection_run_id', p_projection_run_id::text,
    'projection_run_terminalised', true,
    'projection_run_status_before', v_status_before,
    'projection_run_status_after', COALESCE(v_status_after, 'FAILED'),
    'projection_run_terminalised_reason', v_reason,
    'terminalised_by', 'pay_workbench_claim_due_jobs',
    'superseded_continuation_job_id', p_superseded_job_id::text,
    'payload_source_change_seq', p_payload_source_change_seq,
    'cursor_source_change_seq', p_cursor_source_change_seq,
    'projection_run_source_change_seq', v_effective_projection_source_change_seq,
    'live_candidate_source_change_seq', p_live_candidate_source_change_seq,
    'newer_source_change_seq', v_effective_latest_source_seq,
    'active_continuation_job_exists', false,
    'no_active_continuation_job', true,
    'candidate_serial_unblocked', true,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  ));
END;
$function$;

-- _pay_workbench_dirty_payload_merge(jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_dirty_payload_merge(p_existing jsonb, p_incoming jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
DECLARE
  v_existing jsonb := COALESCE(p_existing, '{}'::jsonb);
  v_incoming jsonb := COALESCE(p_incoming, '{}'::jsonb);
  v_merged jsonb := '{}'::jsonb;
  v_latest_source_change_seq bigint := 0;
  v_reason_count integer := 0;
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_trigger_sources jsonb := '[]'::jsonb;
  v_reasons jsonb := '[]'::jsonb;
  v_candidate_ids jsonb := '[]'::jsonb;
  v_finance_case_ids jsonb := '[]'::jsonb;
  v_existing_seq bigint := 0;
  v_incoming_seq bigint := 0;
  v_incoming_has_reason_event boolean := false;
  v_authorise_boundary_changed boolean := false;
  v_unauthorise_boundary_changed boolean := false;
  v_existing_authorise_boundary_changed boolean := false;
  v_incoming_authorise_boundary_changed boolean := false;
  v_existing_unauthorise_boundary_changed boolean := false;
  v_incoming_unauthorise_boundary_changed boolean := false;
  v_explicit_banking_pay_action boolean := false;
  v_existing_explicit_banking_pay_action boolean := false;
  v_incoming_explicit_banking_pay_action boolean := false;
  v_existing_dirty_required boolean := false;
  v_incoming_dirty_required boolean := false;
  v_existing_ordinary_no_dirty boolean := false;
  v_incoming_ordinary_no_dirty boolean := false;
  v_banking_pay_dirty_required boolean := false;
  v_ordinary_timesheet_edit_save_no_dirty boolean := false;
  v_existing_lifecycle_context text := NULL::text;
  v_incoming_lifecycle_context text := NULL::text;
  v_effective_lifecycle_context text := NULL::text;
  v_trigger_table text := NULL::text;
  v_trigger_op text := NULL::text;
  v_trigger_operation text := NULL::text;
  v_projection_class text := NULL::text;
  v_refresh_scope_kind text := NULL::text;
BEGIN
  IF jsonb_typeof(v_existing) IS DISTINCT FROM 'object' THEN
    v_existing := '{}'::jsonb;
  END IF;
  IF jsonb_typeof(v_incoming) IS DISTINCT FROM 'object' THEN
    v_incoming := '{}'::jsonb;
  END IF;

  v_existing_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_existing->>'latest_source_change_seq', '') ~ '^\d+$' THEN (v_existing->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_existing->>'source_change_seq', '') ~ '^\d+$' THEN (v_existing->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_existing->>'source_change_sequence', '') ~ '^\d+$' THEN (v_existing->>'source_change_sequence')::bigint END, 0)
  );
  v_incoming_seq := GREATEST(
    COALESCE(CASE WHEN COALESCE(v_incoming->>'latest_source_change_seq', '') ~ '^\d+$' THEN (v_incoming->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_incoming->>'source_change_seq', '') ~ '^\d+$' THEN (v_incoming->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(v_incoming->>'source_change_sequence', '') ~ '^\d+$' THEN (v_incoming->>'source_change_sequence')::bigint END, 0)
  );
  v_latest_source_change_seq := GREATEST(v_existing_seq, v_incoming_seq);

  v_existing_authorise_boundary_changed := lower(BTRIM(COALESCE(v_existing->>'authorise_boundary_changed', v_existing->>'timesheet_authorise_boundary_changed', v_existing#>>'{complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_authorise_boundary_changed := lower(BTRIM(COALESCE(v_incoming->>'authorise_boundary_changed', v_incoming->>'timesheet_authorise_boundary_changed', v_incoming#>>'{complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_unauthorise_boundary_changed := lower(BTRIM(COALESCE(v_existing->>'unauthorise_boundary_changed', v_existing->>'timesheet_unauthorise_boundary_changed', v_existing#>>'{complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_unauthorise_boundary_changed := lower(BTRIM(COALESCE(v_incoming->>'unauthorise_boundary_changed', v_incoming->>'timesheet_unauthorise_boundary_changed', v_incoming#>>'{complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_authorise_boundary_changed := v_existing_authorise_boundary_changed OR v_incoming_authorise_boundary_changed;
  v_unauthorise_boundary_changed := v_existing_unauthorise_boundary_changed OR v_incoming_unauthorise_boundary_changed;

  v_existing_lifecycle_context := NULLIF(BTRIM(COALESCE(v_existing->>'lifecycle_mutation_context', v_existing->>'mutation_context', v_existing->>'lifecycle_context', '')), '');
  v_incoming_lifecycle_context := NULLIF(BTRIM(COALESCE(v_incoming->>'lifecycle_mutation_context', v_incoming->>'mutation_context', v_incoming->>'lifecycle_context', '')), '');

  IF lower(BTRIM(COALESCE(v_existing_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet')
     OR lower(BTRIM(COALESCE(v_incoming_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet') THEN
    v_authorise_boundary_changed := true;
  END IF;
  IF lower(BTRIM(COALESCE(v_existing_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet')
     OR lower(BTRIM(COALESCE(v_incoming_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet') THEN
    v_unauthorise_boundary_changed := true;
  END IF;

  v_effective_lifecycle_context := CASE
    WHEN v_unauthorise_boundary_changed THEN 'timesheet_unauthorise'
    WHEN v_authorise_boundary_changed THEN 'timesheet_authorise'
    ELSE COALESCE(v_incoming_lifecycle_context, v_existing_lifecycle_context)
  END;

  v_existing_explicit_banking_pay_action := lower(BTRIM(COALESCE(v_existing->>'explicit_banking_pay_action', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_explicit_banking_pay_action := lower(BTRIM(COALESCE(v_incoming->>'explicit_banking_pay_action', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_dirty_required := lower(BTRIM(COALESCE(v_existing->>'banking_pay_dirty_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_dirty_required := lower(BTRIM(COALESCE(v_incoming->>'banking_pay_dirty_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_ordinary_no_dirty := lower(BTRIM(COALESCE(v_existing->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_ordinary_no_dirty := lower(BTRIM(COALESCE(v_incoming->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_explicit_banking_pay_action := v_existing_explicit_banking_pay_action
    OR v_incoming_explicit_banking_pay_action
    OR v_authorise_boundary_changed
    OR v_unauthorise_boundary_changed;
  v_ordinary_timesheet_edit_save_no_dirty := (v_existing_ordinary_no_dirty OR v_incoming_ordinary_no_dirty)
    AND v_explicit_banking_pay_action IS NOT TRUE
    AND v_existing_dirty_required IS NOT TRUE
    AND v_incoming_dirty_required IS NOT TRUE
    AND v_authorise_boundary_changed IS NOT TRUE
    AND v_unauthorise_boundary_changed IS NOT TRUE;
  v_banking_pay_dirty_required := v_ordinary_timesheet_edit_save_no_dirty IS NOT TRUE;

  v_trigger_table := CASE
    WHEN (v_authorise_boundary_changed OR v_unauthorise_boundary_changed)
         AND lower(BTRIM(COALESCE(v_existing->>'trigger_table', ''))) = 'timesheets' THEN 'timesheets'
    WHEN (v_authorise_boundary_changed OR v_unauthorise_boundary_changed)
         AND lower(BTRIM(COALESCE(v_incoming->>'trigger_table', ''))) = 'timesheets' THEN 'timesheets'
    ELSE NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_table', v_existing->>'trigger_table', '')), '')
  END;
  v_trigger_op := NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_op', v_incoming->>'trigger_operation', v_existing->>'trigger_op', v_existing->>'trigger_operation', '')), '');
  v_trigger_operation := NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_operation', v_incoming->>'trigger_op', v_existing->>'trigger_operation', v_existing->>'trigger_op', '')), '');

  v_incoming_has_reason_event := (
    NULLIF(BTRIM(COALESCE(v_incoming->>'reason_latest', v_incoming->>'reason', v_incoming->>'trigger_source', '')), '') IS NOT NULL
    OR jsonb_typeof(v_incoming->'reasons') = 'array'
    OR jsonb_typeof(v_incoming->'trigger_sources') = 'array'
  );

  v_reason_count := COALESCE(
      CASE WHEN COALESCE(v_existing->>'reason_count', '') ~ '^\d+$' THEN (v_existing->>'reason_count')::integer END,
      0
    )
    + COALESCE(
      CASE WHEN COALESCE(v_incoming->>'reason_count', '') ~ '^\d+$' THEN (v_incoming->>'reason_count')::integer END,
      CASE WHEN v_incoming_has_reason_event THEN 1 ELSE 0 END
    );

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'targeted_timesheet_ids') = 'array' THEN v_existing->'targeted_timesheet_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'targeted_timesheet_ids') = 'array' THEN v_incoming->'targeted_timesheet_ids' ELSE '[]'::jsonb END)
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_linked_timesheet_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'linked_timesheet_ids') = 'array' THEN v_existing->'linked_timesheet_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'linked_timesheet_ids') = 'array' THEN v_incoming->'linked_timesheet_ids' ELSE '[]'::jsonb END)
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_trigger_sources
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'trigger_sources') = 'array' THEN v_existing->'trigger_sources' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'trigger_sources') = 'array' THEN v_incoming->'trigger_sources' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT COALESCE(v_existing->>'trigger_source', '')
      UNION ALL
      SELECT COALESCE(v_incoming->>'trigger_source', '')
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_reasons
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'reasons') = 'array' THEN v_existing->'reasons' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'reasons') = 'array' THEN v_incoming->'reasons' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT COALESCE(v_existing->>'reason_latest', v_existing->>'reason', '')
      UNION ALL
      SELECT COALESCE(v_incoming->>'reason_latest', v_incoming->>'reason', '')
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_candidate_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'candidate_ids') = 'array' THEN v_existing->'candidate_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'candidate_ids') = 'array' THEN v_incoming->'candidate_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT COALESCE(v_existing->>'candidate_id', '')
      UNION ALL
      SELECT COALESCE(v_incoming->>'candidate_id', '')
      UNION ALL
      SELECT CASE WHEN UPPER(BTRIM(COALESCE(v_existing->>'scope_kind', ''))) = 'CANDIDATE' THEN COALESCE(v_existing->>'scope_id', '') ELSE '' END
      UNION ALL
      SELECT CASE WHEN UPPER(BTRIM(COALESCE(v_incoming->>'scope_kind', ''))) = 'CANDIDATE' THEN COALESCE(v_incoming->>'scope_id', '') ELSE '' END
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  SELECT COALESCE(jsonb_agg(to_jsonb(union_values.value) ORDER BY union_values.value), '[]'::jsonb)
  INTO v_finance_case_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(array_values.value), '') AS value
    FROM (
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'finance_case_ids') = 'array' THEN v_existing->'finance_case_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT value
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'finance_case_ids') = 'array' THEN v_incoming->'finance_case_ids' ELSE '[]'::jsonb END)
      UNION ALL
      SELECT COALESCE(v_existing->>'finance_case_id', '')
      UNION ALL
      SELECT COALESCE(v_incoming->>'finance_case_id', '')
      UNION ALL
      SELECT CASE WHEN UPPER(BTRIM(COALESCE(v_existing->>'scope_kind', ''))) = 'FINANCE_CASE' THEN COALESCE(v_existing->>'scope_id', '') ELSE '' END
      UNION ALL
      SELECT CASE WHEN UPPER(BTRIM(COALESCE(v_incoming->>'scope_kind', ''))) = 'FINANCE_CASE' THEN COALESCE(v_incoming->>'scope_id', '') ELSE '' END
    ) AS array_values(value)
    WHERE NULLIF(BTRIM(array_values.value), '') IS NOT NULL
  ) AS union_values;

  v_merged := v_existing || v_incoming;

  v_projection_class := CASE
    WHEN v_authorise_boundary_changed OR v_unauthorise_boundary_changed THEN 'NORMAL_TIMESHEET'
    ELSE NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'projection_class', v_existing->>'projection_class', ''))), '')
  END;
  v_refresh_scope_kind := CASE
    WHEN jsonb_array_length(COALESCE(v_targeted_timesheet_ids, '[]'::jsonb)) > 0 THEN 'TARGETED_TIMESHEETS'
    ELSE NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'refresh_scope_kind', v_existing->>'refresh_scope_kind', ''))), '')
  END;

  v_merged := jsonb_strip_nulls(
    v_merged
    || jsonb_build_object(
      'queue_class', 'DIRTY_TRIGGER_PRIORITY',
      'priority_class', 'DIRTY_TRIGGER_PRIORITY',
      'trigger_table', v_trigger_table,
      'trigger_op', v_trigger_op,
      'trigger_operation', v_trigger_operation,
      'mutation_context', v_effective_lifecycle_context,
      'lifecycle_mutation_context', v_effective_lifecycle_context,
      'authorise_boundary_changed', COALESCE(v_authorise_boundary_changed, false),
      'unauthorise_boundary_changed', COALESCE(v_unauthorise_boundary_changed, false),
      'explicit_banking_pay_action', COALESCE(v_explicit_banking_pay_action, false),
      'banking_pay_dirty_required', COALESCE(v_banking_pay_dirty_required, true),
      'ordinary_timesheet_edit_save_no_dirty', COALESCE(v_ordinary_timesheet_edit_save_no_dirty, false),
      'projection_class', v_projection_class,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', v_targeted_timesheet_ids,
      'linked_timesheet_ids', v_linked_timesheet_ids,
      'trigger_sources', v_trigger_sources,
      'reasons', v_reasons,
      'candidate_ids', v_candidate_ids,
      'finance_case_ids', v_finance_case_ids,
      'reason_count', v_reason_count,
      'latest_source_change_seq', v_latest_source_change_seq,
      'source_change_seq', v_latest_source_change_seq,
      'source_change_sequence', v_latest_source_change_seq,
      'latest_event_at_utc', COALESCE(v_incoming->>'latest_event_at_utc', v_incoming->>'event_at_utc', v_existing->>'latest_event_at_utc', v_existing->>'event_at_utc'),
      'reason_latest', COALESCE(v_incoming->>'reason_latest', v_incoming->>'reason', v_existing->>'reason_latest', v_existing->>'reason'),
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'policy_x_dirtying_only', true,
      'economic_truth_mutation_allowed', false
    )
  );

  RETURN v_merged;
END;
$function$;

-- _pay_workbench_merge_targeted_scope_payload(jsonb,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_merge_targeted_scope_payload(p_existing jsonb, p_incoming jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
DECLARE
  v_existing jsonb := CASE WHEN jsonb_typeof(COALESCE(p_existing, '{}'::jsonb)) = 'object' THEN COALESCE(p_existing, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_incoming jsonb := CASE WHEN jsonb_typeof(COALESCE(p_incoming, '{}'::jsonb)) = 'object' THEN COALESCE(p_incoming, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_result jsonb := '{}'::jsonb;
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_finance_case_ids jsonb := '[]'::jsonb;
  v_affected_economic_keys jsonb := '[]'::jsonb;
  v_patched_row_ids jsonb := '[]'::jsonb;
  v_targeted_refresh_candidate_ids jsonb := '[]'::jsonb;
  v_reasons_json jsonb := '[]'::jsonb;
  v_existing_scope_kind text := NULL::text;
  v_incoming_scope_kind text := NULL::text;
  v_effective_scope_kind text := NULL::text;
  v_existing_candidate_id text := NULL::text;
  v_incoming_candidate_id text := NULL::text;
  v_effective_candidate_id text := NULL::text;
  v_existing_reason text := NULL::text;
  v_incoming_reason text := NULL::text;
  v_effective_reason text := NULL::text;
  v_existing_fallback_reason text := NULL::text;
  v_incoming_fallback_reason text := NULL::text;
  v_effective_fallback_reason text := NULL::text;
  v_existing_projection_class text := NULL::text;
  v_incoming_projection_class text := NULL::text;
  v_effective_projection_class text := NULL::text;
  v_existing_projection_run_id text := NULL::text;
  v_incoming_projection_run_id text := NULL::text;
  v_effective_projection_run_id text := NULL::text;
  v_existing_projection_mode text := NULL::text;
  v_incoming_projection_mode text := NULL::text;
  v_existing_resolved_mode text := NULL::text;
  v_incoming_resolved_mode text := NULL::text;
  v_existing_job_type text := NULL::text;
  v_incoming_job_type text := NULL::text;
  v_existing_served_delta boolean := false;
  v_incoming_served_delta boolean := false;
  v_existing_source_change_seq_text text := NULL::text;
  v_incoming_source_change_seq_text text := NULL::text;
  v_existing_source_change_seq bigint := NULL::bigint;
  v_incoming_source_change_seq bigint := NULL::bigint;
  v_effective_source_change_seq bigint := NULL::bigint;
  v_existing_force_legacy boolean := false;
  v_incoming_force_legacy boolean := false;
  v_existing_force_broad_legacy boolean := false;
  v_incoming_force_broad_legacy boolean := false;
  v_existing_complex boolean := false;
  v_incoming_complex boolean := false;
  v_effective_force_legacy boolean := false;
  v_effective_force_broad_legacy boolean := false;
  v_existing_shadow_compare_required boolean := false;
  v_incoming_shadow_compare_required boolean := false;
  v_existing_shadow_compare_enforced boolean := false;
  v_incoming_shadow_compare_enforced boolean := false;
  v_effective_shadow_compare_required boolean := false;
  v_effective_shadow_compare_enforced boolean := false;
  v_authorise_boundary_changed boolean := false;
  v_unauthorise_boundary_changed boolean := false;
  v_explicit_banking_pay_action boolean := false;
  v_banking_pay_dirty_required boolean := false;
  v_ordinary_timesheet_edit_save_no_dirty boolean := false;
  v_existing_lifecycle_context text := NULL::text;
  v_incoming_lifecycle_context text := NULL::text;
  v_effective_lifecycle_context text := NULL::text;
  v_effective_trigger_table text := NULL::text;
  v_effective_trigger_op text := NULL::text;
  v_effective_trigger_operation text := NULL::text;
  v_complex_classes text[] := ARRAY[
    'FINANCE_CASE',
    'PAY_ADVANCE',
    'TIMESHEET_ADVANCE',
    'LOAN',
    'OVERPAYMENT',
    'UNDERPAYMENT',
    'MANUAL_DEBT_ADJUSTMENT',
    'MANUAL_CREDIT_ADJUSTMENT',
    'MANUAL_DEBIT',
    'REPAYMENT',
    'CASE_RESOLUTION',
    'PAYE_UMBRELLA_SWITCH',
    'UMBRELLA_ENTITY_CHANGE',
    'BANK_ROUTING_CHANGE',
    'ONE_OFF_BANK_ACCOUNT',
    'CONTRACT_CLIENT_DIRTY',
    'BROAD_SESSION_REFRESH',
    'TARGET_SCOPE_MISSING',
    'UNKNOWN_TRIGGER'
  ];
BEGIN
  v_result := v_existing || v_incoming;

  v_existing_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(v_existing->>'refresh_scope_kind', ''))), '');
  v_incoming_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'refresh_scope_kind', ''))), '');
  v_existing_candidate_id := NULLIF(BTRIM(COALESCE(v_existing->>'candidate_id', '')), '');
  v_incoming_candidate_id := NULLIF(BTRIM(COALESCE(v_incoming->>'candidate_id', '')), '');
  v_existing_reason := NULLIF(BTRIM(COALESCE(v_existing->>'reason', '')), '');
  v_incoming_reason := NULLIF(BTRIM(COALESCE(v_incoming->>'reason', '')), '');
  v_existing_fallback_reason := NULLIF(BTRIM(COALESCE(v_existing->>'fallback_reason', '')), '');
  v_incoming_fallback_reason := NULLIF(BTRIM(COALESCE(v_incoming->>'fallback_reason', '')), '');
  v_existing_projection_class := NULLIF(UPPER(BTRIM(COALESCE(v_existing->>'projection_class', ''))), '');
  v_incoming_projection_class := NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'projection_class', ''))), '');
  v_existing_projection_run_id := NULLIF(BTRIM(COALESCE(v_existing->>'projection_run_id', '')), '');
  v_incoming_projection_run_id := NULLIF(BTRIM(COALESCE(v_incoming->>'projection_run_id', '')), '');
  v_existing_projection_mode := NULLIF(UPPER(BTRIM(COALESCE(v_existing->>'projection_mode', ''))), '');
  v_incoming_projection_mode := NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'projection_mode', ''))), '');
  v_existing_resolved_mode := NULLIF(UPPER(BTRIM(COALESCE(v_existing->>'resolved_mode', ''))), '');
  v_incoming_resolved_mode := NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'resolved_mode', ''))), '');
  v_existing_job_type := NULLIF(UPPER(BTRIM(COALESCE(v_existing->>'job_type', v_existing->>'resolved_job_type', v_existing->>'canonical_job_type', ''))), '');
  v_incoming_job_type := NULLIF(UPPER(BTRIM(COALESCE(v_incoming->>'job_type', v_incoming->>'resolved_job_type', v_incoming->>'canonical_job_type', ''))), '');

  v_existing_force_legacy := lower(BTRIM(COALESCE(v_existing->>'force_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_force_legacy := lower(BTRIM(COALESCE(v_incoming->>'force_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_force_broad_legacy := lower(BTRIM(COALESCE(v_existing->>'force_broad_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_force_broad_legacy := lower(BTRIM(COALESCE(v_incoming->>'force_broad_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_shadow_compare_required := lower(BTRIM(COALESCE(v_existing->>'shadow_compare_required', v_existing->>'shadow_compare', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_shadow_compare_required := lower(BTRIM(COALESCE(v_incoming->>'shadow_compare_required', v_incoming->>'shadow_compare', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_shadow_compare_enforced := lower(BTRIM(COALESCE(v_existing->>'shadow_compare_enforced', v_existing->>'enforce_shadow_compare', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_shadow_compare_enforced := lower(BTRIM(COALESCE(v_incoming->>'shadow_compare_enforced', v_incoming->>'enforce_shadow_compare', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_authorise_boundary_changed := lower(BTRIM(COALESCE(v_existing->>'authorise_boundary_changed', v_existing->>'timesheet_authorise_boundary_changed', v_existing#>>'{complexity_flags,authorise_boundary_changed}', v_existing#>>'{classifier_result,complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_incoming->>'authorise_boundary_changed', v_incoming->>'timesheet_authorise_boundary_changed', v_incoming#>>'{complexity_flags,authorise_boundary_changed}', v_incoming#>>'{classifier_result,complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_unauthorise_boundary_changed := lower(BTRIM(COALESCE(v_existing->>'unauthorise_boundary_changed', v_existing->>'timesheet_unauthorise_boundary_changed', v_existing#>>'{complexity_flags,unauthorise_boundary_changed}', v_existing#>>'{classifier_result,complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_incoming->>'unauthorise_boundary_changed', v_incoming->>'timesheet_unauthorise_boundary_changed', v_incoming#>>'{complexity_flags,unauthorise_boundary_changed}', v_incoming#>>'{classifier_result,complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_existing_lifecycle_context := NULLIF(BTRIM(COALESCE(v_existing->>'lifecycle_mutation_context', v_existing->>'mutation_context', v_existing->>'lifecycle_context', '')), '');
  v_incoming_lifecycle_context := NULLIF(BTRIM(COALESCE(v_incoming->>'lifecycle_mutation_context', v_incoming->>'mutation_context', v_incoming->>'lifecycle_context', '')), '');
  IF lower(BTRIM(COALESCE(v_existing_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet')
     OR lower(BTRIM(COALESCE(v_incoming_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet') THEN
    v_authorise_boundary_changed := true;
  END IF;
  IF lower(BTRIM(COALESCE(v_existing_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet')
     OR lower(BTRIM(COALESCE(v_incoming_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet') THEN
    v_unauthorise_boundary_changed := true;
  END IF;
  v_effective_lifecycle_context := CASE
    WHEN v_unauthorise_boundary_changed THEN 'timesheet_unauthorise'
    WHEN v_authorise_boundary_changed THEN 'timesheet_authorise'
    ELSE COALESCE(v_incoming_lifecycle_context, v_existing_lifecycle_context)
  END;
  v_explicit_banking_pay_action := lower(BTRIM(COALESCE(v_existing->>'explicit_banking_pay_action', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_incoming->>'explicit_banking_pay_action', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_authorise_boundary_changed
    OR v_unauthorise_boundary_changed;
  v_ordinary_timesheet_edit_save_no_dirty := (
      lower(BTRIM(COALESCE(v_existing->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(BTRIM(COALESCE(v_incoming->>'ordinary_timesheet_edit_save_no_dirty', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    )
    AND v_explicit_banking_pay_action IS NOT TRUE
    AND lower(BTRIM(COALESCE(v_existing->>'banking_pay_dirty_required', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND lower(BTRIM(COALESCE(v_incoming->>'banking_pay_dirty_required', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on');
  v_banking_pay_dirty_required := v_ordinary_timesheet_edit_save_no_dirty IS NOT TRUE;
  v_effective_trigger_table := CASE
    WHEN (v_authorise_boundary_changed OR v_unauthorise_boundary_changed)
         AND lower(BTRIM(COALESCE(v_existing->>'trigger_table', ''))) = 'timesheets' THEN 'timesheets'
    WHEN (v_authorise_boundary_changed OR v_unauthorise_boundary_changed)
         AND lower(BTRIM(COALESCE(v_incoming->>'trigger_table', ''))) = 'timesheets' THEN 'timesheets'
    ELSE NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_table', v_existing->>'trigger_table', '')), '')
  END;
  v_effective_trigger_op := NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_op', v_incoming->>'trigger_operation', v_existing->>'trigger_op', v_existing->>'trigger_operation', '')), '');
  v_effective_trigger_operation := NULLIF(BTRIM(COALESCE(v_incoming->>'trigger_operation', v_incoming->>'trigger_op', v_existing->>'trigger_operation', v_existing->>'trigger_op', '')), '');

  v_existing_served_delta := (
      v_existing_projection_mode = 'DELTA'
      OR v_existing_resolved_mode = 'DELTA'
      OR v_existing_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    )
    AND v_existing_force_legacy IS NOT TRUE
    AND v_existing_force_broad_legacy IS NOT TRUE;

  v_incoming_served_delta := (
      v_incoming_projection_mode = 'DELTA'
      OR v_incoming_resolved_mode = 'DELTA'
      OR v_incoming_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    )
    AND v_incoming_force_legacy IS NOT TRUE
    AND v_incoming_force_broad_legacy IS NOT TRUE;

  IF v_incoming_served_delta IS TRUE THEN
    -- A fresh served-DELTA payload carries the current classifier/settings shadow decision.
    -- Do not allow stale queued payloads to resurrect mandatory shadow enforcement.
    v_effective_shadow_compare_required := COALESCE(v_incoming_shadow_compare_required, false);
    v_effective_shadow_compare_enforced := COALESCE(v_incoming_shadow_compare_required, false)
      AND COALESCE(v_incoming_shadow_compare_enforced, false);
  ELSE
    v_effective_shadow_compare_required := COALESCE(v_existing_shadow_compare_required, false)
      OR COALESCE(v_incoming_shadow_compare_required, false);
    v_effective_shadow_compare_enforced := COALESCE(v_effective_shadow_compare_required, false)
      AND (
        COALESCE(v_existing_shadow_compare_enforced, false)
        OR COALESCE(v_incoming_shadow_compare_enforced, false)
      );
  END IF;

  v_existing_complex := v_existing_projection_class = ANY(v_complex_classes) OR lower(BTRIM(COALESCE(v_existing->>'complex_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_incoming_complex := v_incoming_projection_class = ANY(v_complex_classes) OR lower(BTRIM(COALESCE(v_incoming->>'complex_refresh_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_effective_force_broad_legacy := v_existing_force_broad_legacy OR v_incoming_force_broad_legacy OR v_existing_scope_kind IN ('CANDIDATE_FULL_LIVE', 'SESSION_FULL_LIVE', 'BROAD_SESSION_REFRESH') OR v_incoming_scope_kind IN ('CANDIDATE_FULL_LIVE', 'SESSION_FULL_LIVE', 'BROAD_SESSION_REFRESH');
  v_effective_force_legacy := v_existing_force_legacy OR v_incoming_force_legacy OR v_existing_complex OR v_incoming_complex OR v_effective_force_broad_legacy;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value), '[]'::jsonb)
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(raw_values.value), '') AS value
    FROM (
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'targeted_timesheet_ids') = 'array' THEN v_existing->'targeted_timesheet_ids' WHEN jsonb_typeof(v_existing->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_existing->>'targeted_timesheet_ids') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'targeted_timesheet_ids') = 'array' THEN v_incoming->'targeted_timesheet_ids' WHEN jsonb_typeof(v_incoming->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_incoming->>'targeted_timesheet_ids') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE NULLIF(BTRIM(raw_values.value), '') IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value), '[]'::jsonb)
  INTO v_linked_timesheet_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(raw_values.value), '') AS value
    FROM (
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'linked_timesheet_ids') = 'array' THEN v_existing->'linked_timesheet_ids' WHEN jsonb_typeof(v_existing->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_existing->>'linked_timesheet_ids') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'linked_timesheet_ids') = 'array' THEN v_incoming->'linked_timesheet_ids' WHEN jsonb_typeof(v_incoming->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_incoming->>'linked_timesheet_ids') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE NULLIF(BTRIM(raw_values.value), '') IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value), '[]'::jsonb)
  INTO v_finance_case_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(raw_values.value), '') AS value
    FROM (
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'finance_case_ids') = 'array' THEN v_existing->'finance_case_ids' WHEN jsonb_typeof(v_existing->'finance_case_ids') = 'string' THEN jsonb_build_array(v_existing->>'finance_case_ids') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'finance_case_ids') = 'array' THEN v_incoming->'finance_case_ids' WHEN jsonb_typeof(v_incoming->'finance_case_ids') = 'string' THEN jsonb_build_array(v_incoming->>'finance_case_ids') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE NULLIF(BTRIM(raw_values.value), '') IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value::text), '[]'::jsonb)
  INTO v_affected_economic_keys
  FROM (
    SELECT DISTINCT raw_values.value
    FROM (
      SELECT jsonb_array_elements(CASE WHEN jsonb_typeof(v_existing->'affected_economic_keys') = 'array' THEN v_existing->'affected_economic_keys' WHEN v_existing ? 'affected_economic_keys' THEN jsonb_build_array(v_existing->'affected_economic_keys') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements(CASE WHEN jsonb_typeof(v_incoming->'affected_economic_keys') = 'array' THEN v_incoming->'affected_economic_keys' WHEN v_incoming ? 'affected_economic_keys' THEN jsonb_build_array(v_incoming->'affected_economic_keys') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE raw_values.value IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value), '[]'::jsonb)
  INTO v_patched_row_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(raw_values.value), '') AS value
    FROM (
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'patched_row_ids') = 'array' THEN v_existing->'patched_row_ids' WHEN jsonb_typeof(v_existing->'patched_row_ids') = 'string' THEN jsonb_build_array(v_existing->>'patched_row_ids') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'patched_row_ids') = 'array' THEN v_incoming->'patched_row_ids' WHEN jsonb_typeof(v_incoming->'patched_row_ids') = 'string' THEN jsonb_build_array(v_incoming->>'patched_row_ids') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE NULLIF(BTRIM(raw_values.value), '') IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(array_values.value ORDER BY array_values.value), '[]'::jsonb)
  INTO v_targeted_refresh_candidate_ids
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(raw_values.value), '') AS value
    FROM (
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'targeted_refresh_candidate_ids') = 'array' THEN v_existing->'targeted_refresh_candidate_ids' WHEN jsonb_typeof(v_existing->'targeted_refresh_candidate_ids') = 'string' THEN jsonb_build_array(v_existing->>'targeted_refresh_candidate_ids') ELSE '[]'::jsonb END) AS value
      UNION ALL
      SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'targeted_refresh_candidate_ids') = 'array' THEN v_incoming->'targeted_refresh_candidate_ids' WHEN jsonb_typeof(v_incoming->'targeted_refresh_candidate_ids') = 'string' THEN jsonb_build_array(v_incoming->>'targeted_refresh_candidate_ids') ELSE '[]'::jsonb END) AS value
    ) AS raw_values
    WHERE NULLIF(BTRIM(raw_values.value), '') IS NOT NULL
  ) AS array_values;

  SELECT COALESCE(jsonb_agg(reason_values.reason_value ORDER BY reason_values.reason_ord), '[]'::jsonb)
  INTO v_reasons_json
  FROM (
    SELECT deduped_reasons.reason_value, MIN(deduped_reasons.reason_ord) AS reason_ord
    FROM (
      SELECT v_existing_reason AS reason_value, 1::bigint AS reason_ord
      UNION ALL
      SELECT NULLIF(BTRIM(existing_reason_array.value), '') AS reason_value, 1000::bigint + existing_reason_array.ordinality AS reason_ord
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_existing->'reasons') = 'array' THEN v_existing->'reasons' WHEN jsonb_typeof(v_existing->'reasons') = 'string' THEN jsonb_build_array(v_existing->>'reasons') ELSE '[]'::jsonb END) WITH ORDINALITY AS existing_reason_array(value, ordinality)
      UNION ALL
      SELECT v_incoming_reason AS reason_value, 1000000::bigint AS reason_ord
      UNION ALL
      SELECT NULLIF(BTRIM(incoming_reason_array.value), '') AS reason_value, 1001000::bigint + incoming_reason_array.ordinality AS reason_ord
      FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_incoming->'reasons') = 'array' THEN v_incoming->'reasons' WHEN jsonb_typeof(v_incoming->'reasons') = 'string' THEN jsonb_build_array(v_incoming->>'reasons') ELSE '[]'::jsonb END) WITH ORDINALITY AS incoming_reason_array(value, ordinality)
    ) AS deduped_reasons
    WHERE deduped_reasons.reason_value IS NOT NULL
    GROUP BY deduped_reasons.reason_value
  ) AS reason_values;

  v_effective_candidate_id := COALESCE(v_incoming_candidate_id, v_existing_candidate_id);
  v_effective_reason := COALESCE(v_incoming_reason, v_existing_reason);
  v_effective_projection_run_id := COALESCE(v_incoming_projection_run_id, v_existing_projection_run_id);
  v_effective_projection_class := CASE
    WHEN v_authorise_boundary_changed OR v_unauthorise_boundary_changed THEN 'NORMAL_TIMESHEET'
    WHEN v_incoming_projection_class = 'BROAD_SESSION_REFRESH' OR v_existing_projection_class = 'BROAD_SESSION_REFRESH' OR v_effective_force_broad_legacy THEN 'BROAD_SESSION_REFRESH'
    WHEN v_incoming_projection_class = ANY(v_complex_classes) THEN v_incoming_projection_class
    WHEN v_existing_projection_class = ANY(v_complex_classes) THEN v_existing_projection_class
    ELSE COALESCE(v_incoming_projection_class, v_existing_projection_class)
  END;
  v_effective_fallback_reason := COALESCE(v_incoming_fallback_reason, v_existing_fallback_reason, CASE WHEN v_effective_force_broad_legacy THEN 'BROAD_SESSION_REFRESH' WHEN v_effective_force_legacy THEN 'LEGACY_REQUIRED_AFTER_PAYLOAD_MERGE' ELSE NULL::text END);
  v_effective_scope_kind := CASE
    WHEN v_effective_force_broad_legacy THEN 'CANDIDATE_FULL_LIVE'
    WHEN v_existing_scope_kind = 'CANDIDATE_FULL_LIVE' OR v_incoming_scope_kind = 'CANDIDATE_FULL_LIVE' THEN 'CANDIDATE_FULL_LIVE'
    WHEN jsonb_array_length(v_targeted_timesheet_ids) > 0 OR jsonb_array_length(v_linked_timesheet_ids) > 0 OR v_existing_scope_kind = 'TARGETED_TIMESHEETS' OR v_incoming_scope_kind = 'TARGETED_TIMESHEETS' THEN 'TARGETED_TIMESHEETS'
    ELSE COALESCE(v_incoming_scope_kind, v_existing_scope_kind)
  END;

  v_existing_source_change_seq_text := NULLIF(BTRIM(COALESCE(v_existing->>'source_change_seq', '')), '');
  v_incoming_source_change_seq_text := NULLIF(BTRIM(COALESCE(v_incoming->>'source_change_seq', '')), '');
  IF v_existing_source_change_seq_text ~ '^-?[0-9]+$' THEN v_existing_source_change_seq := v_existing_source_change_seq_text::bigint; END IF;
  IF v_incoming_source_change_seq_text ~ '^-?[0-9]+$' THEN v_incoming_source_change_seq := v_incoming_source_change_seq_text::bigint; END IF;
  v_effective_source_change_seq := GREATEST(COALESCE(v_existing_source_change_seq, 0), COALESCE(v_incoming_source_change_seq, 0));

  v_result := v_result
    || jsonb_build_object(
      'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids, '[]'::jsonb),
      'finance_case_ids', COALESCE(v_finance_case_ids, '[]'::jsonb),
      'affected_economic_keys', COALESCE(v_affected_economic_keys, '[]'::jsonb),
      'patched_row_ids', COALESCE(v_patched_row_ids, '[]'::jsonb),
      'targeted_refresh_candidate_ids', COALESCE(v_targeted_refresh_candidate_ids, '[]'::jsonb),
      'source_change_seq', COALESCE(v_effective_source_change_seq, 0),
      'source_change_sequence', COALESCE(v_effective_source_change_seq, 0)
    )
    || jsonb_build_object(
      'shadow_compare_required', COALESCE(v_effective_shadow_compare_required, false),
      'shadow_compare_enforced', COALESCE(v_effective_shadow_compare_enforced, false),
      'trigger_table', v_effective_trigger_table,
      'trigger_op', v_effective_trigger_op,
      'trigger_operation', v_effective_trigger_operation,
      'mutation_context', v_effective_lifecycle_context,
      'lifecycle_mutation_context', v_effective_lifecycle_context,
      'authorise_boundary_changed', COALESCE(v_authorise_boundary_changed, false),
      'unauthorise_boundary_changed', COALESCE(v_unauthorise_boundary_changed, false),
      'explicit_banking_pay_action', COALESCE(v_explicit_banking_pay_action, false),
      'banking_pay_dirty_required', COALESCE(v_banking_pay_dirty_required, true),
      'ordinary_timesheet_edit_save_no_dirty', COALESCE(v_ordinary_timesheet_edit_save_no_dirty, false)
    );

  IF v_effective_scope_kind IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('refresh_scope_kind', v_effective_scope_kind);
  ELSE
    v_result := v_result - 'refresh_scope_kind';
  END IF;

  IF v_effective_candidate_id IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('candidate_id', v_effective_candidate_id);
  END IF;

  IF v_effective_reason IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('reason', v_effective_reason);
  END IF;

  IF jsonb_array_length(v_reasons_json) > 0 THEN
    v_result := v_result || jsonb_build_object('reasons', v_reasons_json);
  ELSE
    v_result := v_result - 'reasons';
  END IF;

  IF v_effective_projection_run_id IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('projection_run_id', v_effective_projection_run_id);
  END IF;

  IF v_effective_projection_class IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('projection_class', v_effective_projection_class);
  END IF;

  IF v_effective_fallback_reason IS NOT NULL THEN
    v_result := v_result || jsonb_build_object('fallback_reason', v_effective_fallback_reason);
  END IF;

  IF v_effective_force_broad_legacy THEN
    v_result := v_result || jsonb_build_object('force_broad_legacy', true);
  END IF;

  IF v_effective_force_legacy THEN
    v_result := v_result
      || jsonb_build_object(
        'force_legacy', true,
        'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      )
      || jsonb_build_object(
        'source_build_required', true,
        'line_work_required', true,
        'delta_refresh_required', false,
        'projection_mode', 'LEGACY'
      );
  END IF;

  RETURN jsonb_strip_nulls(v_result);
END;
$function$;

-- _pay_workbench_normalise_timesheet_rotation_scope_payload(uuid[],uuid[])
CREATE OR REPLACE FUNCTION public._pay_workbench_normalise_timesheet_rotation_scope_payload(p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_linked_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_targeted uuid[] := ARRAY[]::uuid[];
  v_requested_linked uuid[] := ARRAY[]::uuid[];
  v_requested_all uuid[] := ARRAY[]::uuid[];
  v_targeted_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_semantic_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_family_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_canonical_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_canonical_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_canonical_timesheet_ids uuid[] := ARRAY[]::uuid[];
BEGIN
  SELECT COALESCE(array_agg(DISTINCT raw_id ORDER BY raw_id), ARRAY[]::uuid[])
  INTO v_requested_targeted
  FROM unnest(COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[])) AS raw(raw_id)
  WHERE raw.raw_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT raw_id ORDER BY raw_id), ARRAY[]::uuid[])
  INTO v_requested_linked
  FROM unnest(COALESCE(p_linked_timesheet_ids, ARRAY[]::uuid[])) AS raw(raw_id)
  WHERE raw.raw_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT raw_id ORDER BY raw_id), ARRAY[]::uuid[])
  INTO v_requested_all
  FROM (
    SELECT unnest(v_requested_targeted) AS raw_id
    UNION ALL
    SELECT unnest(v_requested_linked) AS raw_id
  ) AS raw
  WHERE raw.raw_id IS NOT NULL;

  IF COALESCE(array_length(v_requested_all, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'targeted_timesheet_ids', '[]'::jsonb,
      'linked_timesheet_ids', '[]'::jsonb,
      'family_timesheet_ids', '[]'::jsonb,
      'canonical_timesheet_ids', '[]'::jsonb,
      'queue_identity_targeted_timesheet_ids', '[]'::jsonb,
      'queue_identity_linked_timesheet_ids', '[]'::jsonb,
      'queue_identity_timesheet_ids', '[]'::jsonb,
      'requested_timesheet_ids', '[]'::jsonb,
      'requested_targeted_timesheet_ids', '[]'::jsonb,
      'requested_linked_timesheet_ids', '[]'::jsonb,
      'rotation_family_resolved', false,
      'queue_identity_preserves_targeted_linked_semantics', true,
      'linked_timesheets_promoted_to_targeted', false,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  IF COALESCE(array_length(v_requested_targeted, 1), 0) > 0 THEN
    SELECT
      COALESCE(array_agg(target_scope.family_id ORDER BY target_scope.family_id), ARRAY[]::uuid[]),
      COALESCE(array_agg(target_scope.canonical_id ORDER BY target_scope.canonical_id), ARRAY[]::uuid[])
    INTO v_targeted_family_timesheet_ids, v_targeted_canonical_timesheet_ids
    FROM (
      SELECT DISTINCT
        COALESCE(scope_row.family_timesheet_id, scope_row.canonical_timesheet_id, scope_row.requested_timesheet_id) AS family_id,
        COALESCE(scope_row.canonical_timesheet_id, scope_row.family_timesheet_id, scope_row.requested_timesheet_id) AS canonical_id
      FROM public._pay_timesheet_rotation_scope(v_requested_targeted) AS scope_row
      WHERE COALESCE(scope_row.family_timesheet_id, scope_row.canonical_timesheet_id, scope_row.requested_timesheet_id) IS NOT NULL
    ) AS target_scope;
  END IF;

  IF COALESCE(array_length(v_targeted_family_timesheet_ids, 1), 0) = 0 THEN
    v_targeted_family_timesheet_ids := v_requested_targeted;
  END IF;

  IF COALESCE(array_length(v_targeted_canonical_timesheet_ids, 1), 0) = 0 THEN
    v_targeted_canonical_timesheet_ids := v_targeted_family_timesheet_ids;
  END IF;

  IF COALESCE(array_length(v_requested_linked, 1), 0) > 0 THEN
    SELECT
      COALESCE(array_agg(linked_scope.family_id ORDER BY linked_scope.family_id), ARRAY[]::uuid[]),
      COALESCE(array_agg(linked_scope.canonical_id ORDER BY linked_scope.canonical_id), ARRAY[]::uuid[])
    INTO v_linked_family_timesheet_ids, v_linked_canonical_timesheet_ids
    FROM (
      SELECT DISTINCT
        COALESCE(scope_row.family_timesheet_id, scope_row.canonical_timesheet_id, scope_row.requested_timesheet_id) AS family_id,
        COALESCE(scope_row.canonical_timesheet_id, scope_row.family_timesheet_id, scope_row.requested_timesheet_id) AS canonical_id
      FROM public._pay_timesheet_rotation_scope(v_requested_linked) AS scope_row
      WHERE COALESCE(scope_row.family_timesheet_id, scope_row.canonical_timesheet_id, scope_row.requested_timesheet_id) IS NOT NULL
    ) AS linked_scope;
  END IF;

  IF COALESCE(array_length(v_linked_family_timesheet_ids, 1), 0) = 0 THEN
    v_linked_family_timesheet_ids := v_requested_linked;
  END IF;

  IF COALESCE(array_length(v_linked_canonical_timesheet_ids, 1), 0) = 0 THEN
    v_linked_canonical_timesheet_ids := v_linked_family_timesheet_ids;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT linked_only.linked_id ORDER BY linked_only.linked_id), ARRAY[]::uuid[])
  INTO v_semantic_linked_timesheet_ids
  FROM unnest(COALESCE(v_linked_family_timesheet_ids, ARRAY[]::uuid[])) AS linked_only(linked_id)
  WHERE linked_only.linked_id IS NOT NULL
    AND NOT (linked_only.linked_id = ANY(COALESCE(v_targeted_family_timesheet_ids, ARRAY[]::uuid[])));

  SELECT COALESCE(array_agg(DISTINCT family_scope.family_id ORDER BY family_scope.family_id), ARRAY[]::uuid[])
  INTO v_family_timesheet_ids
  FROM (
    SELECT unnest(COALESCE(v_targeted_family_timesheet_ids, ARRAY[]::uuid[])) AS family_id
    UNION ALL
    SELECT unnest(COALESCE(v_linked_family_timesheet_ids, ARRAY[]::uuid[])) AS family_id
  ) AS family_scope
  WHERE family_scope.family_id IS NOT NULL;

  IF COALESCE(array_length(v_family_timesheet_ids, 1), 0) = 0 THEN
    v_family_timesheet_ids := v_requested_all;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT canonical_scope.canonical_id ORDER BY canonical_scope.canonical_id), ARRAY[]::uuid[])
  INTO v_canonical_timesheet_ids
  FROM (
    SELECT unnest(COALESCE(v_targeted_canonical_timesheet_ids, ARRAY[]::uuid[])) AS canonical_id
    UNION ALL
    SELECT unnest(COALESCE(v_linked_canonical_timesheet_ids, ARRAY[]::uuid[])) AS canonical_id
  ) AS canonical_scope
  WHERE canonical_scope.canonical_id IS NOT NULL;

  IF COALESCE(array_length(v_canonical_timesheet_ids, 1), 0) = 0 THEN
    v_canonical_timesheet_ids := v_family_timesheet_ids;
  END IF;

  RETURN jsonb_build_object(
    'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_family_timesheet_ids), '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(to_jsonb(v_semantic_linked_timesheet_ids), '[]'::jsonb),
    'family_timesheet_ids', COALESCE(to_jsonb(v_family_timesheet_ids), '[]'::jsonb),
    'canonical_timesheet_ids', COALESCE(to_jsonb(v_canonical_timesheet_ids), '[]'::jsonb),
    'targeted_family_timesheet_ids', COALESCE(to_jsonb(v_targeted_family_timesheet_ids), '[]'::jsonb),
    'linked_family_timesheet_ids', COALESCE(to_jsonb(v_linked_family_timesheet_ids), '[]'::jsonb),
    'queue_identity_targeted_timesheet_ids', COALESCE(to_jsonb(v_family_timesheet_ids), '[]'::jsonb),
    'queue_identity_linked_timesheet_ids', '[]'::jsonb,
    'queue_identity_timesheet_ids', COALESCE(to_jsonb(v_family_timesheet_ids), '[]'::jsonb),
    'requested_timesheet_ids', COALESCE(to_jsonb(v_requested_all), '[]'::jsonb),
    'requested_targeted_timesheet_ids', COALESCE(to_jsonb(v_requested_targeted), '[]'::jsonb),
    'requested_linked_timesheet_ids', COALESCE(to_jsonb(v_requested_linked), '[]'::jsonb),
    'rotation_family_resolved', true,
    'rotation_family_scope_for_queue_identity', true,
    'queue_identity_preserves_targeted_linked_semantics', true,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

-- _pay_workbench_preview_selection_key_v1(uuid,text,uuid,text,text,text,jsonb)
CREATE OR REPLACE FUNCTION public._pay_workbench_preview_selection_key_v1(p_candidate_id uuid, p_section text, p_timesheet_id uuid, p_key_type text, p_key_value text, p_row_key text, p_row_json jsonb)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE
  v_row jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_row_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_row_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_canonical_key text;
  v_economic_key text;
  v_key_type text := upper(btrim(COALESCE(p_key_type, '')));
  v_key_value text := btrim(COALESCE(p_key_value, ''));
BEGIN
  IF p_candidate_id IS NULL THEN
    RETURN NULL;
  END IF;

  v_canonical_key := NULLIF(btrim(COALESCE(
    v_row ->> 'canonical_correction_key',
    v_row #>> '{source_basis_json,canonical_correction_key}',
    v_row #>> '{frozen_source_basis_json,canonical_correction_key}',
    v_row #>> '{resolution_payload_json,canonical_correction_key}',
    v_row #>> '{saved_resolution_payload_json,canonical_correction_key}',
    v_row #>> '{frozen_resolution_payload_json,canonical_correction_key}',
    v_row #>> '{case_resolution_summary,canonical_correction_key}',
    ''
  )), '');

  IF v_canonical_key IS NOT NULL THEN
    RETURN concat_ws('|', 'CORRECTION', p_candidate_id::text, v_canonical_key);
  END IF;

  v_economic_key := NULLIF(btrim(COALESCE(
    v_row ->> 'economic_key',
    v_row ->> 'stable_economic_key',
    v_row #>> '{economic_key_json,canonical_key}',
    ''
  )), '');

  IF v_economic_key IS NOT NULL THEN
    RETURN concat_ws('|', 'ECONOMIC', p_candidate_id::text, v_economic_key);
  END IF;

  IF p_timesheet_id IS NOT NULL
     AND v_key_type <> ''
     AND v_key_value <> '' THEN
    RETURN concat_ws(
      '|',
      'TIMESHEET_COMPONENT',
      p_candidate_id::text,
      p_timesheet_id::text,
      v_key_type,
      v_key_value
    );
  END IF;

  IF p_timesheet_id IS NOT NULL
     AND NULLIF(btrim(COALESCE(p_row_key, '')), '') IS NOT NULL THEN
    RETURN concat_ws(
      '|',
      'TIMESHEET_ROW',
      p_candidate_id::text,
      p_timesheet_id::text,
      upper(btrim(COALESCE(p_section, ''))),
      btrim(p_row_key)
    );
  END IF;

  RETURN NULL;
END;
$function$;

-- _pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)
CREATE OR REPLACE FUNCTION public._pay_workbench_refresh_dependency_closure_v1(p_candidate_id uuid, p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_linked_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_finance_case_ids uuid[] DEFAULT ARRAY[]::uuid[], p_max_timesheets integer DEFAULT 250, p_max_finance_cases integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_seed_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_effective_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_effective_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_invalid_seed_count integer := 0;
  v_invalid_finance_case_count integer := 0;
  v_iteration integer := 0;
  v_timesheet_count integer := 0;
  v_finance_case_count integer := 0;
  v_max_timesheets integer := LEAST(GREATEST(COALESCE(p_max_timesheets, 250), 1), 1000);
  v_max_finance_cases integer := LEAST(GREATEST(COALESCE(p_max_finance_cases, 100), 1), 500);
  v_requires_full_candidate boolean := false;
  v_fallback_reason text := NULL::text;
BEGIN
  IF p_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'coverage_complete', false,
      'requires_full_candidate', true,
      'fallback_reason', 'DEPENDENCY_CLOSURE_CANDIDATE_REQUIRED',
      'effective_targeted_timesheet_ids', '[]'::jsonb,
      'effective_linked_timesheet_ids', '[]'::jsonb,
      'effective_finance_case_ids', '[]'::jsonb
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT seed_id ORDER BY seed_id), ARRAY[]::uuid[])
  INTO v_seed_timesheet_ids
  FROM unnest(
    COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[])
    || COALESCE(p_linked_timesheet_ids, ARRAY[]::uuid[])
  ) AS seed(seed_id)
  WHERE seed_id IS NOT NULL;

  SELECT COUNT(*)::integer
  INTO v_invalid_seed_count
  FROM unnest(v_seed_timesheet_ids) AS seed(seed_id)
  WHERE NOT EXISTS (
      SELECT 1
      FROM public.timesheets_financials AS tsfin
      WHERE tsfin.timesheet_id = seed.seed_id
        AND tsfin.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS finance_case
      WHERE finance_case.linked_timesheet_id = seed.seed_id
        AND finance_case.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_finance_case_components AS component
      WHERE component.linked_timesheet_id = seed.seed_id
        AND component.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.timesheet_payment_overrides AS payment_override
      WHERE payment_override.timesheet_id = seed.seed_id
        AND payment_override.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.ts_pay_adjustments AS pay_adjustment
      WHERE pay_adjustment.timesheet_id = seed.seed_id
        AND pay_adjustment.candidate_id = p_candidate_id
    );

  SELECT COUNT(*)::integer
  INTO v_invalid_finance_case_count
  FROM unnest(COALESCE(p_finance_case_ids, ARRAY[]::uuid[])) AS requested_case(finance_case_id)
  WHERE requested_case.finance_case_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS finance_case
      WHERE finance_case.id = requested_case.finance_case_id
        AND finance_case.candidate_id = p_candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_finance_case_components AS component
      WHERE component.finance_case_id = requested_case.finance_case_id
        AND component.candidate_id = p_candidate_id
    );

  IF COALESCE(v_invalid_seed_count, 0) > 0
     OR COALESCE(v_invalid_finance_case_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'coverage_complete', false,
      'requires_full_candidate', true,
      'fallback_reason', CASE
        WHEN COALESCE(v_invalid_seed_count, 0) > 0
          THEN 'DEPENDENCY_CLOSURE_TIMESHEET_OWNERSHIP_UNPROVEN'
        ELSE 'DEPENDENCY_CLOSURE_FINANCE_CASE_OWNERSHIP_UNPROVEN'
      END,
      'invalid_seed_count', COALESCE(v_invalid_seed_count, 0),
      'invalid_finance_case_count', COALESCE(v_invalid_finance_case_count, 0),
      'effective_targeted_timesheet_ids', '[]'::jsonb,
      'effective_linked_timesheet_ids', '[]'::jsonb,
      'effective_finance_case_ids', '[]'::jsonb
    );
  END IF;

  /*
   * Close the direct graph in four bounded set-based passes. The relationship
   * types are deliberately constrained to timesheet rotation families and
   * finance-case/component links; no candidate-wide financial calculation
   * occurs.
   */
  v_effective_timesheet_ids := v_seed_timesheet_ids;
  SELECT COALESCE(array_agg(DISTINCT finance_case_id ORDER BY finance_case_id), ARRAY[]::uuid[])
  INTO v_effective_finance_case_ids
  FROM unnest(COALESCE(p_finance_case_ids, ARRAY[]::uuid[]))
    AS requested_case(finance_case_id)
  WHERE finance_case_id IS NOT NULL;

  FOR v_iteration IN 1..4 LOOP
    SELECT COALESCE(array_agg(DISTINCT timesheet_id ORDER BY timesheet_id), ARRAY[]::uuid[])
    INTO v_effective_timesheet_ids
    FROM (
      SELECT existing_id AS timesheet_id
      FROM unnest(v_effective_timesheet_ids) AS existing_timesheet(existing_id)
      UNION
      SELECT rotation_scope.family_timesheet_id
      FROM public._pay_timesheet_rotation_scope(v_effective_timesheet_ids)
        AS rotation_scope
      WHERE rotation_scope.family_timesheet_id IS NOT NULL
      UNION
      SELECT component.linked_timesheet_id
      FROM public.pay_finance_case_components AS component
      WHERE component.candidate_id = p_candidate_id
        AND component.finance_case_id = ANY(v_effective_finance_case_ids)
        AND component.linked_timesheet_id IS NOT NULL
      UNION
      SELECT finance_case.linked_timesheet_id
      FROM public.pay_advances AS finance_case
      WHERE finance_case.candidate_id = p_candidate_id
        AND finance_case.id = ANY(v_effective_finance_case_ids)
        AND finance_case.linked_timesheet_id IS NOT NULL
    ) AS expanded_timesheets
    WHERE timesheet_id IS NOT NULL;

    SELECT COALESCE(array_agg(DISTINCT finance_case_id ORDER BY finance_case_id), ARRAY[]::uuid[])
    INTO v_effective_finance_case_ids
    FROM (
      SELECT existing_id AS finance_case_id
      FROM unnest(v_effective_finance_case_ids) AS existing_case(existing_id)
      UNION
      SELECT component.finance_case_id
      FROM public.pay_finance_case_components AS component
      WHERE component.candidate_id = p_candidate_id
        AND component.linked_timesheet_id = ANY(v_effective_timesheet_ids)
      UNION
      SELECT finance_case.id
      FROM public.pay_advances AS finance_case
      WHERE finance_case.candidate_id = p_candidate_id
        AND finance_case.linked_timesheet_id = ANY(v_effective_timesheet_ids)
    ) AS expanded_cases
    WHERE finance_case_id IS NOT NULL;

    EXIT WHEN COALESCE(array_length(v_effective_timesheet_ids, 1), 0) > v_max_timesheets
           OR COALESCE(array_length(v_effective_finance_case_ids, 1), 0) > v_max_finance_cases;
  END LOOP;

  v_timesheet_count := COALESCE(array_length(v_effective_timesheet_ids, 1), 0);
  v_finance_case_count := COALESCE(array_length(v_effective_finance_case_ids, 1), 0);

  IF v_timesheet_count > v_max_timesheets THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_TIMESHEET_CAP_EXCEEDED';
  ELSIF v_finance_case_count > v_max_finance_cases THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_FINANCE_CASE_CAP_EXCEEDED';
  ELSIF COALESCE(array_length(v_seed_timesheet_ids, 1), 0) = 0
        AND v_finance_case_count = 0 THEN
    v_requires_full_candidate := true;
    v_fallback_reason := 'DEPENDENCY_CLOSURE_EMPTY_ROOT';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'coverage_complete', NOT v_requires_full_candidate,
    'requires_full_candidate', v_requires_full_candidate,
    'fallback_reason', v_fallback_reason,
    'candidate_id', p_candidate_id::text,
    'requested_timesheet_count', COALESCE(array_length(v_seed_timesheet_ids, 1), 0),
    'effective_timesheet_count', v_timesheet_count,
    'effective_finance_case_count', v_finance_case_count,
    'effective_targeted_timesheet_ids', CASE
      WHEN v_requires_full_candidate THEN '[]'::jsonb
      ELSE to_jsonb(v_effective_timesheet_ids)
    END,
    'effective_linked_timesheet_ids', '[]'::jsonb,
    'effective_finance_case_ids', CASE
      WHEN v_requires_full_candidate THEN '[]'::jsonb
      ELSE to_jsonb(v_effective_finance_case_ids)
    END,
    'closure_depth_cap', 4,
    'max_timesheets', v_max_timesheets,
    'max_finance_cases', v_max_finance_cases,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
    'economic_calculation_performed', false,
    'queue_mutation_performed', false
  );
END;
$function$;

-- _temp_diag_log(text,text,text,jsonb)
CREATE OR REPLACE FUNCTION public._temp_diag_log(p_action text, p_object_type text, p_object_id_text text DEFAULT NULL::text, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_enabled boolean := false;
  v_payload jsonb := '{}'::jsonb;
BEGIN
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      RETURN;
    WHEN OTHERS THEN
      RETURN;
  END;

  IF COALESCE(v_enabled, false) IS NOT TRUE THEN
    RETURN;
  END IF;

  v_payload := COALESCE(p_payload_json, '{}'::jsonb);

  IF LENGTH(v_payload::text) > 12000 THEN
    v_payload := jsonb_build_object(
      'truncated', true,
      'original_length', LENGTH(COALESCE(p_payload_json, '{}'::jsonb)::text)
    );
  END IF;

  BEGIN
    INSERT INTO public.audit_events (
      actor_user_id,
      actor_display,
      actor_role_at_time,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason,
      ip,
      user_agent,
      correlation_id
    )
    VALUES (
      NULL::uuid,
      NULL::text,
      NULL::text,
      COALESCE(NULLIF(BTRIM(p_object_type), ''), 'TEMP_DIAG'),
      NULLIF(BTRIM(COALESCE(p_object_id_text, '')), ''),
      COALESCE(NULLIF(BTRIM(p_action), ''), 'TEMP_DIAG_STAGE'),
      NULL::jsonb,
      v_payload,
      'TEMP_DIAG',
      NULL::text,
      NULL::text,
      NULL::text
    );
  EXCEPTION
    WHEN OTHERS THEN
      RETURN;
  END;
END;
$function$;

-- _timesheet_query_email_delivery_mark_core_v1(uuid,text,text,timestamp with time zone,uuid)
CREATE OR REPLACE FUNCTION public._timesheet_query_email_delivery_mark_core_v1(p_mail_outbox_id uuid, p_provider_message_id text, p_provider_status text, p_accepted_at_utc timestamp with time zone, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare d public.hr_issue_email_deliveries%rowtype; o public.mail_outbox%rowtype; v_marked integer:=0; v_state public.import_review_states%rowtype; v_reconcile jsonb;
begin
  select * into o from public.mail_outbox where id=p_mail_outbox_id for update;
  select * into d from public.hr_issue_email_deliveries where mail_outbox_id=p_mail_outbox_id for update;
  if o.id is null or d.id is null then raise exception 'TIMESHEET_QUERY_DELIVERY_NOT_FOUND' using errcode='P0002'; end if;
  if upper(coalesce(o.status::text,''))<>'SENT' and o.sent_at is null then raise exception 'TIMESHEET_QUERY_OUTBOX_NOT_SENT' using errcode='55000'; end if;
  if nullif(btrim(coalesce(o.provider_message_id,'')),'') is not null and nullif(btrim(coalesce(p_provider_message_id,'')),'') is not null
    and o.provider_message_id<>btrim(p_provider_message_id) then raise exception 'TIMESHEET_QUERY_PROVIDER_ID_MISMATCH' using errcode='22023'; end if;
  if d.status='SENT' then return jsonb_build_object('ok',true,'replay',true,'delivery_id',d.id,'marked_issue_count',0); end if;
  update public.hr_issue_email_delivery_items set marked_sent_at_utc=coalesce(p_accepted_at_utc,o.sent_at,now())
  where delivery_id=d.id and marked_sent_at_utc is null;
  get diagnostics v_marked=row_count;
  update public.hr_issue_emails e set last_sent_at=coalesce(p_accepted_at_utc,o.sent_at,now()),sent_count=e.sent_count+1,
    last_successful_delivery_id=d.id,delivery_history_status='SENT_VERIFIED',updated_at=now()
  where e.id in (select di.issue_id from public.hr_issue_email_delivery_items di where di.delivery_id=d.id and di.marked_sent_at_utc is not null)
    and e.last_successful_delivery_id is distinct from d.id;
  update public.hr_issue_email_deliveries set status='SENT',provider_message_id=coalesce(nullif(btrim(p_provider_message_id),''),o.provider_message_id),
    provider_status=coalesce(nullif(btrim(p_provider_status),''),o.provider_status,'ACCEPTED'),accepted_at_utc=coalesce(p_accepted_at_utc,o.sent_at,now()),marked_at_utc=now(),updated_at_utc=now()
  where id=d.id returning * into d;
  if not exists(select 1 from public.hr_issue_email_deliveries x where x.import_id=d.import_id and x.status<>'SENT') then
    update public.import_apply_operations set response_json=response_json||jsonb_build_object('review_email_follow_up_status','COMPLETE'),updated_at_utc=now()
    where id=d.operation_id;
    v_reconcile:=public._import_review_follow_up_reconcile_core_v1(d.import_id,d.operation_id,p_actor_user_id);
    select * into v_state from public.import_review_states where import_id=d.import_id;
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(d.import_id,v_state.state_version,d.operation_id,'QUERY_EMAIL_FOLLOW_UP_COMPLETE',p_actor_user_id,jsonb_build_object('delivery_id',d.id));
  end if;
  return jsonb_build_object('ok',true,'replay',false,'delivery_id',d.id,'marked_issue_count',v_marked,'status',d.status,
    'follow_up_status',coalesce(v_reconcile->>'follow_up_status',(select s.follow_up_status from public.import_review_states s where s.import_id=d.import_id)));
end $function$;

-- _timesheet_query_recipient_resolve_core_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION public._timesheet_query_recipient_resolve_core_v1(p_client_id uuid, p_contract_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare c public.clients%rowtype; k public.contracts%rowtype; v_scope text; v_key text; v_email text; v_hash text;
begin
  if p_client_id is null then raise exception 'TIMESHEET_QUERY_CLIENT_REQUIRED' using errcode='22023'; end if;
  select * into c from public.clients where id=p_client_id; if not found then raise exception 'TIMESHEET_QUERY_CLIENT_NOT_FOUND' using errcode='P0002'; end if;
  if p_contract_id is not null then select * into k from public.contracts where id=p_contract_id;
    if not found or k.client_id<>p_client_id then raise exception 'TIMESHEET_QUERY_CONTRACT_CLIENT_MISMATCH' using errcode='22023'; end if; end if;
  if p_contract_id is not null and coalesce(k.send_ts_queries_to_different_email,false) then
    v_scope:='CONTRACT_OVERRIDE'; v_key:='CONTRACT_OVERRIDE:'||k.id::text; v_email:=lower(btrim(coalesce(k.ts_queries_alt_email_address,'')));
    if length(v_email) not between 3 and 320 or v_email!~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' then
      raise exception 'TIMESHEET_QUERY_CONTRACT_OVERRIDE_INVALID' using errcode='22023'; end if;
    v_hash:=public._import_review_hash_v1(concat_ws('|','query-route-v1',v_key,v_email,k.updated_at));
  else
    v_scope:='CLIENT_DEFAULT'; v_key:='CLIENT_DEFAULT:'||c.id::text; v_email:=lower(btrim(coalesce(c.ts_queries_email,'')));
    if length(v_email) not between 3 and 320 or v_email!~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' then
      raise exception 'TIMESHEET_QUERY_CLIENT_EMAIL_INVALID' using errcode='22023'; end if;
    v_hash:=public._import_review_hash_v1(concat_ws('|','query-route-v1',v_key,v_email,c.rev,c.updated_at));
  end if;
  return jsonb_build_object('client_id',p_client_id,'contract_id',p_contract_id,'recipient_scope',v_scope,
    'recipient_scope_key',v_key,'recipient_email',v_email,'route_fingerprint',v_hash);
end $function$;

