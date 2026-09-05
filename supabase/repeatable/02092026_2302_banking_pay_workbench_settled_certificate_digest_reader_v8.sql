-- WORKBENCH_SETTLED_CERTIFICATION_V2 / V8 canonical digest and bounded readers.
-- Runtime authority is Miget; the supabase directory name is historical only.

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_stable_stringify_v8(
  p_value jsonb
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_type text;
  v_result text;
BEGIN
  v_type := pg_catalog.jsonb_typeof(p_value);
  IF v_type IS NULL OR v_type = 'null' THEN
    RETURN 'null';
  ELSIF v_type IN ('string', 'number', 'boolean') THEN
    RETURN p_value::text;
  ELSIF v_type = 'array' THEN
    SELECT '[' || COALESCE(
      pg_catalog.string_agg(
        private.pay_workbench_settled_certificate_stable_stringify_v8(element.value),
        ',' ORDER BY element.ordinality
      ),
      ''
    ) || ']'
    INTO v_result
    FROM pg_catalog.jsonb_array_elements(p_value) WITH ORDINALITY AS element(value, ordinality);
    RETURN v_result;
  ELSIF v_type = 'object' THEN
    SELECT '{' || COALESCE(
      pg_catalog.string_agg(
        pg_catalog.to_jsonb(member.key)::text || ':' ||
          private.pay_workbench_settled_certificate_stable_stringify_v8(member.value),
        ',' ORDER BY member.key COLLATE "C"
      ),
      ''
    ) || '}'
    INTO v_result
    FROM pg_catalog.jsonb_each(p_value) AS member(key, value);
    RETURN v_result;
  END IF;
  RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CANONICAL_VALUE_INVALID'
    USING ERRCODE = '22023';
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_sha256_init_v8()
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
  SELECT pg_catalog.jsonb_build_object(
    'h', pg_catalog.jsonb_build_array(
      1779033703, 3144134277, 1013904242, 2773480762,
      1359893119, 2600822924, 528734635, 1541459225
    ),
    'total_bytes', 0,
    'tail_hex', ''
  );
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_sha256_update_bytes_v8(
  p_state jsonb,
  p_bytes bytea,
  p_count_as_message boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_mask constant bigint := 4294967295;
  v_mod constant bigint := 4294967296;
  v_k constant bigint[] := ARRAY[
    1116352408,1899447441,3049323471,3921009573,961987163,1508970993,2453635748,2870763221,
    3624381080,310598401,607225278,1426881987,1925078388,2162078206,2614888103,3248222580,
    3835390401,4022224774,264347078,604807628,770255983,1249150122,1555081692,1996064986,
    2554220882,2821834349,2952996808,3210313671,3336571891,3584528711,113926993,338241895,
    666307205,773529912,1294757372,1396182291,1695183700,1986661051,2177026350,2456956037,
    2730485921,2820302411,3259730800,3345764771,3516065817,3600352804,4094571909,275423344,
    430227734,506948616,659060556,883997877,958139571,1322822218,1537002063,1747873779,
    1955562222,2024104815,2227730452,2361852424,2428436474,2756734187,3204031479,3329325298
  ]::bigint[];
  v_h bigint[] := ARRAY[]::bigint[];
  v_w bigint[] := pg_catalog.array_fill(0::bigint, ARRAY[64]);
  v_total bigint;
  v_tail bytea;
  v_buffer bytea;
  v_offset integer := 0;
  v_i integer;
  v_x bigint;
  v_s0 bigint;
  v_s1 bigint;
  v_ch bigint;
  v_maj bigint;
  v_t1 bigint;
  v_t2 bigint;
  v_a bigint;
  v_b bigint;
  v_c bigint;
  v_d bigint;
  v_e bigint;
  v_f bigint;
  v_g bigint;
  v_hh bigint;
BEGIN
  IF pg_catalog.jsonb_typeof(p_state) IS DISTINCT FROM 'object'
     OR pg_catalog.jsonb_typeof(p_state->'h') IS DISTINCT FROM 'array'
     OR pg_catalog.jsonb_array_length(p_state->'h') <> 8
     OR COALESCE(p_state->>'total_bytes', '') !~ '^[0-9]+$'
     OR COALESCE(p_state->>'tail_hex', '') !~ '^[0-9a-f]*$'
     OR pg_catalog.length(COALESCE(p_state->>'tail_hex', '')) % 2 <> 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
  END IF;

  v_total := (p_state->>'total_bytes')::bigint;
  v_tail := pg_catalog.decode(p_state->>'tail_hex', 'hex');
  IF pg_catalog.octet_length(v_tail) > 63
     OR v_total % 64 <> pg_catalog.octet_length(v_tail) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
  END IF;
  IF pg_catalog.octet_length(COALESCE(p_bytes, ''::bytea)) > 524288 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CANONICAL_PAGE_TOO_LARGE' USING ERRCODE = '54000';
  END IF;

  FOR v_i IN 0..7 LOOP
    IF COALESCE(p_state->'h'->>v_i, '') !~ '^[0-9]+$' THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
    END IF;
    v_x := (p_state->'h'->>v_i)::bigint;
    IF v_x < 0 OR v_x > v_mask THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
    END IF;
    v_h := pg_catalog.array_append(v_h, v_x);
  END LOOP;

  v_buffer := v_tail || COALESCE(p_bytes, ''::bytea);
  IF p_count_as_message THEN
    v_total := v_total + pg_catalog.octet_length(COALESCE(p_bytes, ''::bytea));
  END IF;

  WHILE v_offset + 64 <= pg_catalog.octet_length(v_buffer) LOOP
    FOR v_i IN 0..15 LOOP
      v_w[v_i + 1] := (
        (pg_catalog.get_byte(v_buffer, v_offset + v_i * 4)::bigint << 24) |
        (pg_catalog.get_byte(v_buffer, v_offset + v_i * 4 + 1)::bigint << 16) |
        (pg_catalog.get_byte(v_buffer, v_offset + v_i * 4 + 2)::bigint << 8) |
         pg_catalog.get_byte(v_buffer, v_offset + v_i * 4 + 3)::bigint
      ) & v_mask;
    END LOOP;
    FOR v_i IN 16..63 LOOP
      v_x := v_w[v_i - 15 + 1];
      v_s0 := (((v_x >> 7) | ((v_x << 25) & v_mask)) #
               ((v_x >> 18) | ((v_x << 14) & v_mask)) #
               (v_x >> 3)) & v_mask;
      v_x := v_w[v_i - 2 + 1];
      v_s1 := (((v_x >> 17) | ((v_x << 15) & v_mask)) #
               ((v_x >> 19) | ((v_x << 13) & v_mask)) #
               (v_x >> 10)) & v_mask;
      v_w[v_i + 1] := (v_w[v_i - 16 + 1] + v_s0 + v_w[v_i - 7 + 1] + v_s1) % v_mod;
    END LOOP;

    v_a := v_h[1]; v_b := v_h[2]; v_c := v_h[3]; v_d := v_h[4];
    v_e := v_h[5]; v_f := v_h[6]; v_g := v_h[7]; v_hh := v_h[8];
    FOR v_i IN 0..63 LOOP
      v_s1 := (((v_e >> 6) | ((v_e << 26) & v_mask)) #
               ((v_e >> 11) | ((v_e << 21) & v_mask)) #
               ((v_e >> 25) | ((v_e << 7) & v_mask))) & v_mask;
      v_ch := ((v_e & v_f) # ((v_mask # v_e) & v_g)) & v_mask;
      v_t1 := (v_hh + v_s1 + v_ch + v_k[v_i + 1] + v_w[v_i + 1]) % v_mod;
      v_s0 := (((v_a >> 2) | ((v_a << 30) & v_mask)) #
               ((v_a >> 13) | ((v_a << 19) & v_mask)) #
               ((v_a >> 22) | ((v_a << 10) & v_mask))) & v_mask;
      v_maj := ((v_a & v_b) # (v_a & v_c) # (v_b & v_c)) & v_mask;
      v_t2 := (v_s0 + v_maj) % v_mod;
      v_hh := v_g; v_g := v_f; v_f := v_e;
      v_e := (v_d + v_t1) % v_mod;
      v_d := v_c; v_c := v_b; v_b := v_a;
      v_a := (v_t1 + v_t2) % v_mod;
    END LOOP;
    v_h[1] := (v_h[1] + v_a) % v_mod;
    v_h[2] := (v_h[2] + v_b) % v_mod;
    v_h[3] := (v_h[3] + v_c) % v_mod;
    v_h[4] := (v_h[4] + v_d) % v_mod;
    v_h[5] := (v_h[5] + v_e) % v_mod;
    v_h[6] := (v_h[6] + v_f) % v_mod;
    v_h[7] := (v_h[7] + v_g) % v_mod;
    v_h[8] := (v_h[8] + v_hh) % v_mod;
    v_offset := v_offset + 64;
  END LOOP;

  v_tail := pg_catalog.substring(v_buffer, v_offset + 1);
  IF pg_catalog.octet_length(v_tail) > 63
     OR (p_count_as_message AND v_total % 64 <> pg_catalog.octet_length(v_tail)) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
  END IF;
  RETURN pg_catalog.jsonb_build_object(
    'h', pg_catalog.to_jsonb(v_h),
    'total_bytes', v_total,
    'tail_hex', pg_catalog.encode(v_tail, 'hex')
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_sha256_update_v8(
  p_state jsonb,
  p_text text
)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
  SELECT private.pay_workbench_settled_certificate_sha256_update_bytes_v8(
    p_state,
    pg_catalog.convert_to(COALESCE(p_text, ''), 'UTF8'),
    true
  );
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_sha256_final_v8(
  p_state jsonb
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_total bigint;
  v_tail_length integer;
  v_zero_count integer;
  v_padding bytea;
  v_length_bytes bytea := pg_catalog.decode('0000000000000000', 'hex');
  v_bits bigint;
  v_final jsonb;
  v_i integer;
  v_result text := '';
BEGIN
  IF pg_catalog.jsonb_typeof(p_state) IS DISTINCT FROM 'object'
     OR COALESCE(p_state->>'total_bytes', '') !~ '^[0-9]+$'
     OR COALESCE(p_state->>'tail_hex', '') !~ '^[0-9a-f]*$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_STATE_INVALID' USING ERRCODE = '22023';
  END IF;
  v_total := (p_state->>'total_bytes')::bigint;
  v_tail_length := pg_catalog.length(p_state->>'tail_hex') / 2;
  IF v_total > 1152921504606846975 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_MESSAGE_TOO_LARGE' USING ERRCODE = '54000';
  END IF;
  v_zero_count := (56 - ((v_tail_length + 1) % 64) + 64) % 64;
  v_bits := v_total * 8;
  FOR v_i IN 0..7 LOOP
    v_length_bytes := pg_catalog.set_byte(
      v_length_bytes,
      7 - v_i,
      ((v_bits >> (v_i * 8)) & 255)::integer
    );
  END LOOP;
  v_padding := pg_catalog.decode('80', 'hex') ||
    pg_catalog.decode(pg_catalog.repeat('00', v_zero_count), 'hex') ||
    v_length_bytes;
  v_final := private.pay_workbench_settled_certificate_sha256_update_bytes_v8(
    p_state,
    v_padding,
    false
  );
  IF COALESCE(v_final->>'tail_hex', '') <> '' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_FINALIZATION_INVALID' USING ERRCODE = '22023';
  END IF;
  FOR v_i IN 0..7 LOOP
    v_result := v_result || pg_catalog.lpad(pg_catalog.to_hex((v_final->'h'->>v_i)::bigint), 8, '0');
  END LOOP;
  RETURN v_result;
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_sha256_text_v8(
  p_text text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
  -- One-shot values use pgcrypto's native implementation.  This is exactly
  -- SHA-256 over the same UTF-8 stableStringify bytes as the restartable
  -- state owner, but avoids interpreting 64 compression rounds in PL/pgSQL
  -- for every constituent and page receipt.
  SELECT pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(COALESCE(p_text, ''), 'UTF8'), 'sha256'),
    'hex'
  );
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_money_v8(
  p_value jsonb
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_text text;
  v_amount numeric;
BEGIN
  IF p_value IS NULL OR pg_catalog.jsonb_typeof(p_value) = 'null' THEN
    RETURN NULL;
  END IF;
  IF pg_catalog.jsonb_typeof(p_value) NOT IN ('number', 'string') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MONEY_INVALID' USING ERRCODE = '22023';
  END IF;
  v_text := CASE WHEN pg_catalog.jsonb_typeof(p_value) = 'string'
                 THEN p_value #>> '{}'
                 ELSE p_value::text END;
  IF v_text !~ '^-?[0-9]+(?:\.[0-9]+)?$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MONEY_INVALID' USING ERRCODE = '22023';
  END IF;
  v_amount := pg_catalog.round(v_text::numeric, 2);
  IF pg_catalog.abs(v_amount) >= 1000000000000000::numeric THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MONEY_OUT_OF_RANGE' USING ERRCODE = '22003';
  END IF;
  IF v_amount = 0 THEN v_amount := 0; END IF;
  RETURN pg_catalog.to_char(v_amount, 'FM9999999999999990.00');
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_component_evidence_json_v8(
  p_certificate_uuid uuid,
  p_constituent_ordinal integer
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_all jsonb;
  v_full jsonb;
  v_decisive jsonb;
  v_all_ids jsonb;
  v_full_ids jsonb;
  v_decisive_ids jsonb;
BEGIN
  WITH projected AS (
    SELECT evidence.*,
      pg_catalog.jsonb_build_object(
        'frozen_component_ordinal', evidence.frozen_component_ordinal,
        'source_component_kind', evidence.source_component_kind,
        'economic_key_type', evidence.economic_key_type,
        'economic_key_value', evidence.economic_key_value,
        'component_fallback', evidence.component_fallback,
        'authoritative_truth_ex_vat', evidence.authoritative_truth_ex_vat,
        'authoritative_baseline_ex_vat', evidence.authoritative_baseline_ex_vat,
        'authoritative_reserved_ex_vat', evidence.authoritative_reserved_ex_vat,
        'authoritative_outstanding_ex_vat', evidence.authoritative_outstanding_ex_vat,
        'component_amount_ex_vat', evidence.component_amount_ex_vat,
        'source_pay_ex_vat', evidence.source_pay_ex_vat,
        'source_charge_ex_vat', evidence.source_charge_ex_vat,
        'financial_revision_digest', evidence.financial_revision_digest,
        'target_authority_digest', evidence.target_authority_digest,
        'conversion_context_digest', evidence.conversion_context_digest,
        'physical_bucket_key', evidence.physical_bucket_key,
        'physical_bucket_digest', evidence.physical_bucket_digest,
        'sealed_evidence_digest', evidence.sealed_evidence_digest,
        'source_pay_method', evidence.source_pay_method,
        'target_pay_method', evidence.target_pay_method
      ) AS bound_facts
    FROM private.banking_pay_workbench_settled_certificate_component_evidence_v8 evidence
    WHERE evidence.certificate_uuid = p_certificate_uuid
      AND evidence.constituent_ordinal = p_constituent_ordinal
  )
  SELECT
    COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'stable_component_id', projected.stable_component_id,
        'bound_facts', projected.bound_facts
      ) ORDER BY projected.frozen_component_ordinal, projected.stable_component_id
    ) FILTER (WHERE projected.evidence_kind = 'ALL_SAME_ECONOMIC_KEY'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'stable_component_id', projected.stable_component_id,
        'bound_facts', projected.bound_facts,
        'signed_pre_signature', pg_catalog.jsonb_build_object(
          'component_fallback', projected.component_fallback,
          'authoritative_truth_ex_vat', projected.authoritative_truth_ex_vat,
          'authoritative_baseline_ex_vat', projected.authoritative_baseline_ex_vat
        )
      ) ORDER BY projected.frozen_component_ordinal, projected.stable_component_id
    ) FILTER (WHERE projected.evidence_kind = 'FULL_SIGNED_PRE_SIGNATURE'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'stable_component_id', projected.stable_component_id,
        'bound_facts', projected.bound_facts,
        'signed_pre_signature', pg_catalog.jsonb_build_object(
          'component_fallback', projected.component_fallback,
          'authoritative_truth_ex_vat', projected.authoritative_truth_ex_vat,
          'authoritative_baseline_ex_vat', projected.authoritative_baseline_ex_vat
        ),
        'decisive_frozen_evidence', pg_catalog.jsonb_build_object(
          'authoritative_reserved_ex_vat', projected.authoritative_reserved_ex_vat,
          'authoritative_outstanding_ex_vat', projected.authoritative_outstanding_ex_vat,
          'component_amount_ex_vat', projected.component_amount_ex_vat,
          'source_pay_ex_vat', projected.source_pay_ex_vat,
          'source_charge_ex_vat', projected.source_charge_ex_vat,
          'financial_revision_digest', projected.financial_revision_digest,
          'target_authority_digest', projected.target_authority_digest,
          'conversion_context_digest', projected.conversion_context_digest,
          'physical_bucket_key', projected.physical_bucket_key,
          'physical_bucket_digest', projected.physical_bucket_digest,
          'sealed_evidence_digest', projected.sealed_evidence_digest,
          'source_pay_method', projected.source_pay_method,
          'target_pay_method', projected.target_pay_method
        )
      ) ORDER BY projected.frozen_component_ordinal, projected.stable_component_id
    ) FILTER (WHERE projected.evidence_kind = 'DECISIVE_SIGNED_EVIDENCE'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(projected.stable_component_id)
      ORDER BY projected.frozen_component_ordinal, projected.stable_component_id)
      FILTER (WHERE projected.evidence_kind = 'ALL_SAME_ECONOMIC_KEY'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(projected.stable_component_id)
      ORDER BY projected.frozen_component_ordinal, projected.stable_component_id)
      FILTER (WHERE projected.evidence_kind = 'FULL_SIGNED_PRE_SIGNATURE'), '[]'::jsonb),
    COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(projected.stable_component_id)
      ORDER BY projected.frozen_component_ordinal, projected.stable_component_id)
      FILTER (WHERE projected.evidence_kind = 'DECISIVE_SIGNED_EVIDENCE'), '[]'::jsonb)
  INTO v_all, v_full, v_decisive, v_all_ids, v_full_ids, v_decisive_ids
  FROM projected;

  IF pg_catalog.jsonb_array_length(v_full) > 1
     OR pg_catalog.jsonb_array_length(v_decisive) > 1
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_decisive_ids) decisive_id
       WHERE NOT v_full_ids @> pg_catalog.jsonb_build_array(decisive_id.value)
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_COMPONENT_EVIDENCE_CARDINALITY_INVALID'
      USING ERRCODE = '22023';
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ordering', 'frozen_component_ordinal asc, stable_component_id asc',
    'all_same_economic_key_component_count', pg_catalog.jsonb_array_length(v_all),
    'all_same_economic_key_component_ordered_ids', v_all_ids,
    'all_same_economic_key_components_digest_sha256',
      private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_all)
      ),
    'full_signed_pre_signature_match_count', pg_catalog.jsonb_array_length(v_full),
    'full_signed_pre_signature_match_ordered_ids', v_full_ids,
    'full_signed_pre_signature_matches_digest_sha256',
      private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_full)
      ),
    'decisive_signed_evidence_count', pg_catalog.jsonb_array_length(v_decisive),
    'decisive_signed_evidence_ordered_ids', v_decisive_ids,
    'decisive_signed_evidence_digest_sha256',
      private.pay_workbench_settled_certificate_sha256_text_v8(
        private.pay_workbench_settled_certificate_stable_stringify_v8(v_decisive)
      )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_constituent_unsigned_v8(
  p_certificate_uuid uuid,
  p_constituent_ordinal integer
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_entry private.banking_pay_workbench_settled_certificate_entries_v8%ROWTYPE;
  v_superseded jsonb;
  v_reservations jsonb;
BEGIN
  SELECT * INTO STRICT v_entry
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
  WHERE entry.certificate_uuid = p_certificate_uuid
    AND entry.constituent_ordinal = p_constituent_ordinal;

  SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(source.superseded_source_id)
           ORDER BY source.source_ordinal), '[]'::jsonb)
  INTO v_superseded
  FROM private.banking_pay_workbench_settled_certificate_superseded_sources_v8 source
  WHERE source.certificate_uuid = p_certificate_uuid
    AND source.constituent_ordinal = p_constituent_ordinal;

  SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(reservation.source_reservation_id)
           ORDER BY reservation.reservation_ordinal), '[]'::jsonb)
  INTO v_reservations
  FROM private.banking_pay_workbench_settled_cert_source_reservations_v8 reservation
  WHERE reservation.certificate_uuid = p_certificate_uuid
    AND reservation.constituent_ordinal = p_constituent_ordinal;

  RETURN pg_catalog.jsonb_build_object(
    'constituent_ordinal', v_entry.constituent_ordinal,
    'preview_row_id', v_entry.preview_row_id,
    'materialised_preview_row_id', v_entry.materialised_preview_row_id,
    'presentation_preview_row_id', v_entry.presentation_preview_row_id,
    'row_key', v_entry.row_key,
    'line_id', v_entry.line_id,
    'source_identity', pg_catalog.jsonb_build_object(
      'source_kind', v_entry.source_kind,
      'source_line_id', v_entry.source_line_id,
      'source_row_key', v_entry.source_row_key,
      'source_publication_id', v_entry.source_publication_id,
      'source_change_seq', v_entry.source_change_seq,
      'source_build_run_id', v_entry.source_build_run_id,
      'source_identity_digest_sha256', v_entry.source_identity_digest_sha256
    ),
    'candidate_publication_ordinal', v_entry.candidate_publication_ordinal,
    'candidate_id', v_entry.candidate_id,
    'client_id', v_entry.client_id,
    'timesheet_id', v_entry.timesheet_id,
    'resolved_pay_channel', v_entry.resolved_pay_channel,
    'resolved_payment_method', v_entry.resolved_payment_method,
    'amount_sign', v_entry.amount_sign,
    'semantic_kind', v_entry.semantic_kind,
    'economic_key', pg_catalog.jsonb_build_object(
      'keyspace', 'timesheet_id,key_type,key_value',
      'timesheet_id', v_entry.economic_key_timesheet_id,
      'key_type', v_entry.economic_key_type,
      'key_value', v_entry.economic_key_value
    ),
    'canonical_amount_ex_vat', v_entry.canonical_amount_ex_vat,
    'source_reservation_amount_ex_vat', v_entry.source_reservation_amount_ex_vat,
    'prior_payment', pg_catalog.jsonb_build_object(
      'treatment', v_entry.prior_payment_treatment,
      'prior_paid_amount_ex_vat', v_entry.prior_paid_amount_ex_vat,
      'evidence_digest_sha256', v_entry.prior_payment_evidence_digest_sha256
    ),
    'supersession', pg_catalog.jsonb_build_object(
      'treatment', v_entry.supersession_treatment,
      'ordered_superseded_source_ids', v_superseded,
      'evidence_digest_sha256', v_entry.supersession_evidence_digest_sha256
    ),
    'recovery_headroom', pg_catalog.jsonb_build_object(
      'contract_version', v_entry.recovery_contract_version,
      'nominal_due_amount_ex_vat', v_entry.recovery_nominal_due_amount_ex_vat,
      'recoverable_this_pay_run_ex_vat', v_entry.recovery_recoverable_this_pay_run_ex_vat,
      'headroom_amount_ex_vat', v_entry.recovery_headroom_amount_ex_vat,
      'allocated_recovery_amount_ex_vat', v_entry.recovery_allocated_amount_ex_vat,
      'result_kind', v_entry.recovery_result_kind,
      'overlay_digest_sha256', v_entry.recovery_overlay_digest_sha256
    ),
    'expected_pre_draft_facts', pg_catalog.jsonb_build_object(
      'recovery_headroom_allocation_expectation', pg_catalog.jsonb_build_object(
        'expected_allocation_basis_kind', v_entry.expected_allocation_basis_kind,
        'expected_allocated_recovery_amount_ex_vat', v_entry.expected_allocated_recovery_amount_ex_vat,
        'expected_allocation_result', v_entry.expected_allocation_result,
        'source_evidence_digest_sha256', v_entry.expected_allocation_source_digest_sha256
      ),
      'item_expectation', pg_catalog.jsonb_build_object(
        'expected_item_semantic_kind', v_entry.expected_item_semantic_kind,
        'expected_item_source_identity_digest_sha256', v_entry.expected_item_source_identity_digest_sha256,
        'expected_item_amount_ex_vat', v_entry.expected_item_amount_ex_vat,
        'source_evidence_digest_sha256', v_entry.expected_item_source_digest_sha256
      ),
      'source_reservation_expectation', pg_catalog.jsonb_build_object(
        'expected_reservation_applicability', v_entry.expected_reservation_applicability,
        'source_reservation_amount_ex_vat', v_entry.expected_reservation_amount_ex_vat,
        'ordered_active_source_reservation_ids', v_reservations,
        'source_evidence_digest_sha256', v_entry.expected_reservation_source_digest_sha256
      )
    ),
    'component_evidence', private.pay_workbench_settled_certificate_component_evidence_json_v8(
      p_certificate_uuid, p_constituent_ordinal
    ),
    'readiness_class', v_entry.readiness_class,
    'selection_state', v_entry.selection_state,
    'selected', v_entry.selected,
    'draftable', v_entry.draftable,
    'is_ready_for_draft', v_entry.is_ready_for_draft
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_constituent_json_v8(
  p_certificate_uuid uuid,
  p_constituent_ordinal integer
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_unsigned jsonb;
  v_digest text;
  v_stored text;
BEGIN
  v_unsigned := private.pay_workbench_settled_certificate_constituent_unsigned_v8(
    p_certificate_uuid, p_constituent_ordinal
  );
  v_digest := private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_unsigned)
  );
  SELECT entry.constituent_digest_sha256 INTO STRICT v_stored
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
  WHERE entry.certificate_uuid = p_certificate_uuid
    AND entry.constituent_ordinal = p_constituent_ordinal;
  IF v_stored IS DISTINCT FROM v_digest THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED' USING ERRCODE = '22023';
  END IF;
  RETURN v_unsigned || pg_catalog.jsonb_build_object('constituent_digest_sha256', v_digest);
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_canonical_stream_page_v8(
  p_certificate_uuid uuid,
  p_stream_kind text,
  p_pay_channel_scope text,
  p_after_ordinal integer,
  p_limit integer
)
RETURNS TABLE(
  canonical_fragment text,
  row_count integer,
  next_after_ordinal integer,
  has_more boolean
)
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = ''
AS $function$
BEGIN
  IF p_limit NOT BETWEEN 1 AND 256
     OR p_pay_channel_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_INVALID' USING ERRCODE = '22023';
  END IF;
  IF p_stream_kind = 'CONSTITUENT_DIGESTS' THEN
    RETURN QUERY
    WITH bounded AS (
      SELECT entry.constituent_ordinal, entry.constituent_digest_sha256
      FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
      WHERE entry.certificate_uuid = p_certificate_uuid
        AND (p_pay_channel_scope = 'ALL' OR entry.resolved_pay_channel = p_pay_channel_scope)
        AND (p_after_ordinal IS NULL OR entry.constituent_ordinal > p_after_ordinal)
      ORDER BY entry.constituent_ordinal
      LIMIT p_limit + 1
    ), page AS (
      SELECT * FROM bounded ORDER BY constituent_ordinal LIMIT p_limit
    )
    SELECT COALESCE(
             pg_catalog.string_agg(
               CASE WHEN p_after_ordinal IS NULL AND page.constituent_ordinal =
                          (SELECT MIN(p2.constituent_ordinal) FROM page p2)
                    THEN '' ELSE ',' END || pg_catalog.to_jsonb(page.constituent_digest_sha256)::text,
               '' ORDER BY page.constituent_ordinal
             ),
             ''
           ),
           COUNT(*)::integer,
           MAX(page.constituent_ordinal),
           (SELECT COUNT(*) > p_limit FROM bounded)
    FROM page;
    RETURN;
  END IF;
  RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STREAM_KIND_INVALID' USING ERRCODE = '22023';
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_overall_digest_advance_v8(
  p_certificate_uuid uuid,
  p_limit integer
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_run private.banking_pay_workbench_settled_certificate_digest_runs_v8%ROWTYPE;
  v_partition private.banking_pay_workbench_settled_certificate_partitions_v8%ROWTYPE;
  v_manifest record;
  v_row record;
  v_state jsonb;
  v_fragment text := '';
  v_piece text;
  v_phase text;
  v_kind text;
  v_next_phase text;
  v_next_member integer;
  v_next_partition integer;
  v_row_count integer := 0;
  v_byte_count integer := 0;
  v_has_more boolean;
  v_digest text;
  v_universe private.banking_pay_workbench_settled_certificate_universes_v8%ROWTYPE;
  v_authority jsonb;
  v_channels jsonb;
  v_gate jsonb;
  v_policy jsonb;
  v_owners jsonb;
  v_surfaces jsonb;
BEGIN
  IF p_certificate_uuid IS NULL OR p_limit NOT BETWEEN 1 AND 256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERALL_DIGEST_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT * INTO STRICT v_header FROM private.banking_pay_workbench_settled_certificates_v8
  WHERE certificate_uuid=p_certificate_uuid;
  IF v_header.lifecycle<>'BUILDING' OR v_header.certified_at_utc IS NULL
     OR v_header.selected_constituents_digest_sha256 IS NULL
     OR v_header.selected_partitions_digest_sha256 IS NULL
     OR v_header.manifests_digest_sha256 IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERALL_DIGEST_PREREQUISITES_MISSING' USING ERRCODE='55000';
  END IF;
  SELECT * INTO v_run FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8 digest_run
  WHERE digest_run.certificate_uuid=p_certificate_uuid AND digest_run.stream_kind='OVERALL_PAYLOAD'
    AND digest_run.pay_channel_scope='ALL' FOR UPDATE;
  IF NOT FOUND THEN
    SELECT pg_catalog.jsonb_build_object(
      'session_id',v_header.workbench_session_id,'session_version',v_header.session_version,
      'progress_counter_version',v_header.progress_counter_version,'progress_state',v_header.progress_state,
      'source_snapshot_run_id',v_header.source_snapshot_run_id,'session_signature',v_header.session_signature,
      'pay_date',v_header.pay_date,'week_ending_cutoff',v_header.week_ending_cutoff,
      'filters_digest_sha256',v_header.filters_digest_sha256,
      'scope_change_generation_target',v_header.scope_change_generation_target,
      'scope_change_generation_applied',v_header.scope_change_generation_applied,
      'scope_change_generation_shadow_checked',v_header.scope_change_generation_shadow_checked,
      'authority_fence_generation',v_header.authority_fence_generation
    ) INTO v_authority;
    SELECT COALESCE(
      pg_catalog.jsonb_object_agg(manifests.pay_channel_scope, manifests.full_manifest),
      '{}'::jsonb
    ) || pg_catalog.jsonb_build_object(
      'manifests_digest_sha256', v_header.manifests_digest_sha256
    ) INTO v_channels FROM (
      SELECT manifest.pay_channel_scope,pg_catalog.jsonb_build_object(
        'pay_channel_scope',manifest.pay_channel_scope,'constituent_count',manifest.constituent_count,
        'partition_count',manifest.partition_count,'canonical_amount_ex_vat_total',manifest.canonical_amount_ex_vat_total,
        'selected_constituents_digest_sha256',manifest.selected_constituents_digest_sha256,
        'selected_partitions_digest_sha256',manifest.selected_partitions_digest_sha256,
        'manifest_digest_sha256',manifest.manifest_digest_sha256) full_manifest
      FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
      WHERE manifest.certificate_uuid=p_certificate_uuid
    ) manifests;
    IF v_channels->'ALL'='null'::jsonb OR v_channels->'PAYE'='null'::jsonb OR v_channels->'UMBRELLA'='null'::jsonb THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CHANNEL_MANIFESTS_INCOMPLETE' USING ERRCODE='55000';
    END IF;
    v_gate:=pg_catalog.jsonb_build_object(
      'can_create_draft',v_header.can_create_draft,
      'selected_eligible_ready_row_count',v_header.selected_eligible_ready_row_count,
      'blocking_reason_count',v_header.blocking_reason_count,
      'gate_digest_sha256',v_header.gate_digest_sha256);
    v_fragment:='{"authority":'||private.pay_workbench_settled_certificate_stable_stringify_v8(v_authority)||
      ',"certified_at_utc":'||pg_catalog.to_jsonb(v_header.certified_at_utc)::text||
      ',"channel_manifests":'||private.pay_workbench_settled_certificate_stable_stringify_v8(v_channels)||
      ',"completeness":{"active_draft_rows_excluded":true,"all_selected_rows_loaded":true,"create_draft_gate":'||
      private.pay_workbench_settled_certificate_stable_stringify_v8(v_gate)||
      ',"exclusion_universes":{"active_draft":{"ordered_stable_identity_digests":[';
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(
      private.pay_workbench_settled_certificate_sha256_init_v8(),v_fragment);
    INSERT INTO private.banking_pay_workbench_settled_certificate_digest_runs_v8(
      certificate_uuid,stream_kind,pay_channel_scope,hash_contract,status,next_ordinal,
      stream_phase,current_member_ordinal,current_state_json
    ) VALUES (p_certificate_uuid,'OVERALL_PAYLOAD','ALL','V2_STABLE_STRINGIFY_OVERALL',
      'BUILDING',0,'U_ACTIVE_DRAFT',NULL,v_state)
    RETURNING * INTO v_run;
    -- The prefix has already been committed to v_state. Do not append the
    -- same local fragment again when this first call advances ACTIVE_DRAFT.
    v_fragment:='';
  END IF;
  IF v_run.status='COMPLETE' THEN
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',true,'overall_digest_sha256',v_run.final_digest_sha256);
  END IF;
  v_phase:=v_run.stream_phase;

  IF v_phase LIKE 'U_%' THEN
    v_kind:=CASE v_phase WHEN 'U_ACTIVE_DRAFT' THEN 'ACTIVE_DRAFT' WHEN 'U_INELIGIBLE' THEN 'INELIGIBLE'
      WHEN 'U_SNOOZED' THEN 'SNOOZED' WHEN 'U_ACTION_REQUIRED' THEN 'ACTION_REQUIRED'
      WHEN 'U_BLOCKED' THEN 'BLOCKED' WHEN 'U_READY' THEN 'READY' END;
    IF v_kind IS NULL THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERALL_PHASE_INVALID' USING ERRCODE='55000'; END IF;
    SELECT * INTO STRICT v_universe FROM private.banking_pay_workbench_settled_certificate_universes_v8
    WHERE certificate_uuid=p_certificate_uuid AND universe_kind=v_kind;
    FOR v_row IN
      SELECT member.member_ordinal,member.stable_identity_digest_sha256
      FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid AND member.universe_kind=v_kind
        AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
      ORDER BY member.member_ordinal LIMIT p_limit
    LOOP
      v_piece:=CASE WHEN v_run.next_ordinal+v_row_count=0 THEN '' ELSE ',' END||
        pg_catalog.to_jsonb(v_row.stable_identity_digest_sha256)::text;
      v_fragment:=v_fragment||v_piece;v_row_count:=v_row_count+1;v_next_member:=v_row.member_ordinal;
    END LOOP;
    v_has_more:=EXISTS(SELECT 1 FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid AND member.universe_kind=v_kind
        AND (v_next_member IS NULL OR member.member_ordinal>v_next_member));
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      v_piece:='],"row_count":'||v_universe.row_count::text||',"universe_digest_sha256":'||
        pg_catalog.to_jsonb(v_universe.universe_digest_sha256)::text||'}';
      IF v_phase='U_ACTIVE_DRAFT' THEN
        v_piece:=v_piece||',"ineligible":{"ordered_stable_identity_digests":[';v_next_phase:='U_INELIGIBLE';
      ELSIF v_phase='U_INELIGIBLE' THEN
        v_piece:=v_piece||',"snoozed":{"ordered_stable_identity_digests":[';v_next_phase:='U_SNOOZED';
      ELSIF v_phase='U_SNOOZED' THEN
        v_piece:=v_piece||'},"historical_terminal_rows_are_not_current_authority":true,"historical_terminal_rows_retained":true'||
          ',"ineligible_rows_excluded":true,"invalid_current_job_pointer_count":0'||
          ',"line_units_failed":0,"line_units_pending":0,"line_units_ready":'||v_header.line_units_ready::text||
          ',"line_units_total":'||v_header.line_units_total::text||',"queued_current_job_count":0'||
          ',"ready_action_required_blocked_pairwise_disjoint":true,"running_current_job_count":0'||
          ',"scope_failed_count":0,"scope_pending_count":0,"scope_ready_count":'||v_header.scope_ready_count::text||
          ',"scope_seeded_count":'||v_header.scope_seeded_count::text||',"scope_total_count":'||v_header.scope_total_count::text||
          ',"section_universes":{"action_required":{"ordered_stable_identity_digests":[';
        v_next_phase:='U_ACTION_REQUIRED';
      ELSIF v_phase='U_ACTION_REQUIRED' THEN
        v_piece:=v_piece||',"blocked":{"ordered_stable_identity_digests":[';v_next_phase:='U_BLOCKED';
      ELSIF v_phase='U_BLOCKED' THEN
        v_piece:=v_piece||',"ordering":"stable row ordinal asc, stable identity asc","ready":{"ordered_stable_identity_digests":[';
        v_next_phase:='U_READY';
      ELSE
        SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(owner.logical_owner_identity) ORDER BY owner.owner_ordinal),'[]'::jsonb)
          INTO v_owners FROM private.banking_pay_workbench_settled_certificate_policy_owners_v8 owner
          WHERE owner.certificate_uuid=p_certificate_uuid;
        SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(surface.compared_surface) ORDER BY surface.surface_ordinal),'[]'::jsonb)
          INTO v_surfaces FROM private.banking_pay_workbench_settled_certificate_policy_surfaces_v8 surface
          WHERE surface.certificate_uuid=p_certificate_uuid;
        v_policy:=pg_catalog.jsonb_build_object(
          'contract_version',v_header.policy_contract_version,'logical_owner_identities',v_owners,
          'before_policy_projection_digest_sha256',v_header.before_policy_projection_digest_sha256,
          'after_policy_projection_digest_sha256',v_header.after_policy_projection_digest_sha256,
          'digests_equal',v_header.policy_digests_equal,'execution_recovery_delta_only',v_header.execution_recovery_delta_only,
          'forbidden_policy_delta_count',v_header.forbidden_policy_delta_count,'compared_surfaces',v_surfaces,
          'no_payment_policy_change',v_header.no_payment_policy_change);
        v_piece:=v_piece||'},"selected_page_order":'||pg_catalog.to_jsonb(v_header.selected_page_order)::text||
          ',"selected_page_size_max":'||v_header.selected_page_size_max::text||
          ',"selected_pages_fetched":'||v_header.selected_pages_fetched::text||
          ',"selected_row_count":'||v_header.selected_row_count::text||
          ',"selected_sentinel_overflow":false,"selected_terminal_sentinel_seen":true'||
          ',"server_selected_ids_equal_materialised_selected_ids":true'||
          ',"server_selected_preview_row_ids_provided":true,"snoozed_rows_excluded":true'||
          ',"unloaded_selection_gap_count":0,"unresolved_current_job_count":0}'||
          ',"payment_policy_parity":'||private.pay_workbench_settled_certificate_stable_stringify_v8(v_policy)||
          ',"publication_set":{"ordering":"scope_ordinal asc, candidate_id asc","publication_count":'||
          v_header.publication_count::text||',"publications":[';
        v_next_phase:='PUBLICATIONS';
      END IF;
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,v_piece);
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        stream_phase=v_next_phase,next_ordinal=0,current_member_ordinal=NULL WHERE digest_run_uuid=v_run.digest_run_uuid;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        next_ordinal=next_ordinal+v_row_count,current_member_ordinal=v_next_member WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',false,'stage',v_phase,'page_row_count',v_row_count);
  END IF;

  IF v_phase='PUBLICATIONS' THEN
    FOR v_row IN SELECT publication.* FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
      WHERE publication.certificate_uuid=p_certificate_uuid
        AND (v_run.current_member_ordinal IS NULL OR publication.scope_ordinal>v_run.current_member_ordinal)
      ORDER BY publication.scope_ordinal LIMIT p_limit
    LOOP
      v_piece:=CASE WHEN v_run.next_ordinal+v_row_count=0 THEN '' ELSE ',' END||
        private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
          'scope_ordinal',v_row.scope_ordinal,'candidate_id',v_row.candidate_id,
          'candidate_state_id',v_row.candidate_state_id,'candidate_state_status',v_row.candidate_state_status,
          'source_change_seq',v_row.source_change_seq,'source_build_run_id',v_row.source_build_run_id,
          'source_publication_id',v_row.source_publication_id,
          'certified_publication_session_version',v_row.certified_publication_session_version,
          'publication_attestation_version',v_row.publication_attestation_version,
          'publication_attestation_digest_sha256',v_row.publication_attestation_digest_sha256,
          'publication_parity_ok',v_row.publication_parity_ok,'publication_attested_at_utc',v_row.publication_attested_at_utc));
      v_fragment:=v_fragment||v_piece;v_row_count:=v_row_count+1;v_next_member:=v_row.scope_ordinal;
    END LOOP;
    v_has_more:=EXISTS(SELECT 1 FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
      WHERE publication.certificate_uuid=p_certificate_uuid AND (v_next_member IS NULL OR publication.scope_ordinal>v_next_member));
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      v_piece:='],"publications_digest_sha256":'||pg_catalog.to_jsonb(v_header.publications_digest_sha256)::text||'}'||
        ',"selected_canonical_amount_ex_vat_total":'||pg_catalog.to_jsonb(v_header.selected_canonical_amount_ex_vat_total)::text||
        ',"selected_constituent_count":'||v_header.selected_constituent_count::text||',"selected_constituents":[';
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,v_piece);
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        stream_phase='CONSTITUENTS',next_ordinal=0,current_member_ordinal=NULL WHERE digest_run_uuid=v_run.digest_run_uuid;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        next_ordinal=next_ordinal+v_row_count,current_member_ordinal=v_next_member WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',false,'stage',v_phase,'page_row_count',v_row_count);
  END IF;

  IF v_phase='CONSTITUENTS' THEN
    FOR v_row IN SELECT entry.constituent_ordinal FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
      WHERE entry.certificate_uuid=p_certificate_uuid
        AND (v_run.current_member_ordinal IS NULL OR entry.constituent_ordinal>v_run.current_member_ordinal)
      ORDER BY entry.constituent_ordinal LIMIT LEAST(p_limit,64)
    LOOP
      v_piece:=CASE WHEN v_run.next_ordinal+v_row_count=0 THEN '' ELSE ',' END||
        private.pay_workbench_settled_certificate_stable_stringify_v8(
          private.pay_workbench_settled_certificate_constituent_json_v8(p_certificate_uuid,v_row.constituent_ordinal));
      -- Preserve the exact V1 stableStringify byte stream while keeping the
      -- pure PL/pgSQL SHA-256 work well inside the unchanged 15-second budget.
      -- The next keyset call resumes at the following whole constituent.
      IF v_row_count>0 AND v_byte_count+pg_catalog.octet_length(v_piece)>65536 THEN EXIT; END IF;
      v_fragment:=v_fragment||v_piece;v_byte_count:=v_byte_count+pg_catalog.octet_length(v_piece);
      v_row_count:=v_row_count+1;v_next_member:=v_row.constituent_ordinal;
    END LOOP;
    v_has_more:=EXISTS(SELECT 1 FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
      WHERE entry.certificate_uuid=p_certificate_uuid AND (v_next_member IS NULL OR entry.constituent_ordinal>v_next_member));
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      v_piece:='],"selected_constituents_digest_sha256":'||pg_catalog.to_jsonb(v_header.selected_constituents_digest_sha256)::text||
        ',"selected_partition_count":'||v_header.selected_partition_count::text||',"selected_partitions":[';
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,v_piece);
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        stream_phase='PARTITION_START',next_ordinal=0,current_member_ordinal=NULL,current_partition_ordinal=NULL
      WHERE digest_run_uuid=v_run.digest_run_uuid;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        next_ordinal=next_ordinal+v_row_count,current_member_ordinal=v_next_member WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',false,'stage',v_phase,
      'page_row_count',v_row_count,'canonical_byte_count',v_byte_count);
  END IF;

  IF v_phase='PARTITION_START' THEN
    SELECT MIN(partition_ordinal) INTO v_next_partition FROM private.banking_pay_workbench_settled_certificate_partitions_v8
    WHERE certificate_uuid=p_certificate_uuid AND (v_run.current_partition_ordinal IS NULL OR partition_ordinal>v_run.current_partition_ordinal);
    IF v_next_partition IS NULL THEN
      v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,
        '],"selected_partitions_digest_sha256":'||pg_catalog.to_jsonb(v_header.selected_partitions_digest_sha256)::text||
        ',"selected_partitions_ordering":'||pg_catalog.to_jsonb(v_header.selected_partitions_ordering)::text||'}');
      v_digest:=private.pay_workbench_settled_certificate_sha256_final_v8(v_state);
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        status='COMPLETE',stream_phase='COMPLETE',final_digest_sha256=v_digest WHERE digest_run_uuid=v_run.digest_run_uuid;
      RETURN pg_catalog.jsonb_build_object('ok',true,'complete',true,'overall_digest_sha256',v_digest);
    END IF;
    SELECT * INTO STRICT v_partition FROM private.banking_pay_workbench_settled_certificate_partitions_v8
    WHERE certificate_uuid=p_certificate_uuid AND partition_ordinal=v_next_partition;
    v_fragment:=CASE WHEN v_run.next_ordinal=0 THEN '' ELSE ',' END||
      '{"candidate_id":'||pg_catalog.to_jsonb(v_partition.candidate_id)::text||
      ',"canonical_amount_ex_vat_total":'||pg_catalog.to_jsonb(v_partition.canonical_amount_ex_vat_total)::text||
      ',"constituent_count":'||v_partition.constituent_count::text||',"ordered_constituent_identity_digests":[';
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
      stream_phase='PARTITION_IDENTITIES',current_partition_ordinal=v_next_partition,current_member_ordinal=NULL
    WHERE digest_run_uuid=v_run.digest_run_uuid;
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',false,'stage',v_phase,'partition_ordinal',v_next_partition);
  END IF;

  IF v_phase IN ('PARTITION_IDENTITIES','PARTITION_ORDINALS') THEN
    SELECT * INTO STRICT v_partition FROM private.banking_pay_workbench_settled_certificate_partitions_v8
    WHERE certificate_uuid=p_certificate_uuid AND partition_ordinal=v_run.current_partition_ordinal;
    FOR v_row IN SELECT member.* FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
        AND (v_run.current_member_ordinal IS NULL OR member.member_ordinal>v_run.current_member_ordinal)
      ORDER BY member.member_ordinal LIMIT p_limit
    LOOP
      v_piece:=CASE WHEN v_run.current_member_ordinal IS NULL AND v_row_count=0 THEN '' ELSE ',' END||
        CASE WHEN v_phase='PARTITION_IDENTITIES' THEN pg_catalog.to_jsonb(v_row.stable_identity_digest_sha256)::text
             ELSE v_row.constituent_ordinal::text END;
      v_fragment:=v_fragment||v_piece;v_row_count:=v_row_count+1;v_next_member:=v_row.member_ordinal;
    END LOOP;
    v_has_more:=EXISTS(SELECT 1 FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=p_certificate_uuid AND member.partition_ordinal=v_partition.partition_ordinal
        AND (v_next_member IS NULL OR member.member_ordinal>v_next_member));
    v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_run.current_state_json,v_fragment);
    IF NOT v_has_more THEN
      IF v_phase='PARTITION_IDENTITIES' THEN
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,'],"ordered_constituent_ordinals":[');
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
          stream_phase='PARTITION_ORDINALS',current_member_ordinal=NULL WHERE digest_run_uuid=v_run.digest_run_uuid;
      ELSE
        v_state:=private.pay_workbench_settled_certificate_sha256_update_v8(v_state,
          '],"partition_digest_sha256":'||pg_catalog.to_jsonb(v_partition.partition_digest_sha256)::text||
          ',"partition_ordinal":'||v_partition.partition_ordinal::text||
          ',"resolved_pay_channel":'||pg_catalog.to_jsonb(v_partition.resolved_pay_channel)::text||'}');
        UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
          stream_phase='PARTITION_START',current_member_ordinal=NULL,next_ordinal=next_ordinal+1
        WHERE digest_run_uuid=v_run.digest_run_uuid;
      END IF;
    ELSE
      UPDATE private.banking_pay_workbench_settled_certificate_digest_runs_v8 SET current_state_json=v_state,
        current_member_ordinal=v_next_member WHERE digest_run_uuid=v_run.digest_run_uuid;
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok',true,'complete',false,'stage',v_phase,
      'partition_ordinal',v_partition.partition_ordinal,'page_row_count',v_row_count);
  END IF;
  RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERALL_PHASE_INVALID' USING ERRCODE='55000';
END;
$function$;

CREATE OR REPLACE FUNCTION private.workbench_settled_cert_same_week_override_validate_v8(
  p_same_week_paye_override jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_continue boolean;
  v_verified boolean;
  v_used boolean;
  v_reason text;
  v_reauth_purpose text;
  v_guardrail_code text;
  v_verified_by_text text;
  v_verified_at_text text;
  v_verified_by uuid;
  v_verified_at timestamptz;
  v_pay_date date;
  v_pay_week_start date;
  v_pay_week_end date;
BEGIN
  IF jsonb_typeof(p_same_week_paye_override)<>'object'
     OR (SELECT COUNT(*) FROM pg_catalog.jsonb_object_keys(p_same_week_paye_override))<>11
     OR NOT (p_same_week_paye_override ?& ARRAY[
       'continue','verified','used','pay_date','pay_week_start','pay_week_end',
       'reason','verified_by_user_id','verified_at_utc','reauth_purpose','guardrail_code'])
     OR jsonb_typeof(p_same_week_paye_override->'continue')<>'boolean'
     OR jsonb_typeof(p_same_week_paye_override->'verified')<>'boolean'
     OR jsonb_typeof(p_same_week_paye_override->'used')<>'boolean'
     OR jsonb_typeof(p_same_week_paye_override->'pay_date')<>'string'
     OR jsonb_typeof(p_same_week_paye_override->'pay_week_start')<>'string'
     OR jsonb_typeof(p_same_week_paye_override->'pay_week_end')<>'string'
     OR jsonb_typeof(p_same_week_paye_override->'reason') NOT IN ('string','null')
     OR jsonb_typeof(p_same_week_paye_override->'reauth_purpose') NOT IN ('string','null')
     OR jsonb_typeof(p_same_week_paye_override->'guardrail_code') NOT IN ('string','null')
     OR jsonb_typeof(p_same_week_paye_override->'verified_by_user_id') NOT IN ('string','null')
     OR jsonb_typeof(p_same_week_paye_override->'verified_at_utc') NOT IN ('string','null') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_continue:=(p_same_week_paye_override->>'continue')::boolean;
  v_verified:=(p_same_week_paye_override->>'verified')::boolean;
  v_used:=(p_same_week_paye_override->>'used')::boolean;
  v_reason:=CASE WHEN jsonb_typeof(p_same_week_paye_override->'reason')='null' THEN NULL ELSE p_same_week_paye_override->>'reason' END;
  v_reauth_purpose:=CASE WHEN jsonb_typeof(p_same_week_paye_override->'reauth_purpose')='null' THEN NULL ELSE p_same_week_paye_override->>'reauth_purpose' END;
  v_guardrail_code:=CASE WHEN jsonb_typeof(p_same_week_paye_override->'guardrail_code')='null' THEN NULL ELSE p_same_week_paye_override->>'guardrail_code' END;
  v_verified_by_text:=CASE WHEN jsonb_typeof(p_same_week_paye_override->'verified_by_user_id')='null' THEN NULL ELSE p_same_week_paye_override->>'verified_by_user_id' END;
  v_verified_at_text:=CASE WHEN jsonb_typeof(p_same_week_paye_override->'verified_at_utc')='null' THEN NULL ELSE p_same_week_paye_override->>'verified_at_utc' END;

  IF (v_guardrail_code IS NOT NULL AND v_guardrail_code<>'PAYE_SAME_WEEK_OVERRIDE_REQUIRED')
     OR (v_verified_by_text IS NOT NULL AND v_verified_by_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  BEGIN
    v_pay_date:=(p_same_week_paye_override->>'pay_date')::date;
    v_pay_week_start:=(p_same_week_paye_override->>'pay_week_start')::date;
    v_pay_week_end:=(p_same_week_paye_override->>'pay_week_end')::date;
    v_verified_by:=v_verified_by_text::uuid;
    v_verified_at:=v_verified_at_text::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END;

  IF v_pay_week_start>v_pay_date OR v_pay_date>v_pay_week_end
     OR (v_used IS FALSE AND (
       v_verified IS TRUE OR v_reason IS NOT NULL OR v_reauth_purpose IS NOT NULL
       OR v_verified_by IS NOT NULL OR v_verified_at IS NOT NULL))
     OR (v_used IS TRUE AND (
       v_continue IS NOT TRUE OR v_verified IS NOT TRUE
       OR NULLIF(BTRIM(COALESCE(v_reason,'')),'') IS NULL
       OR char_length(v_reason)>2000
       OR v_reauth_purpose IS DISTINCT FROM 'PAYE_SAME_WEEK_OVERRIDE'
       OR v_verified_by IS NULL OR v_verified_at IS NULL)) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  RETURN p_same_week_paye_override;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_reference_issue_v8(
  p_certification_id text,
  p_pay_channel_scope text,
  p_idempotency_key text,
  p_same_week_paye_override jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
SET statement_timeout = '6000ms'
SET lock_timeout = '1000ms'
AS $function$
DECLARE
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_filter private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8%ROWTYPE;
  v_channel private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_certification_id text:=NULLIF(BTRIM(COALESCE(p_certification_id,'')),'');
  v_pay_channel_scope text:=UPPER(NULLIF(BTRIM(COALESCE(p_pay_channel_scope,'')),''));
  v_idempotency_key text:=NULLIF(BTRIM(COALESCE(p_idempotency_key,'')),'');
BEGIN
  IF v_certification_id IS NULL
     OR v_certification_id !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR v_pay_channel_scope NOT IN ('ALL','PAYE','UMBRELLA')
     OR v_idempotency_key IS NULL OR char_length(v_idempotency_key)>200
     OR jsonb_typeof(p_same_week_paye_override)<>'object'
     OR (SELECT COUNT(*) FROM pg_catalog.jsonb_object_keys(p_same_week_paye_override))<>11
     OR NOT (p_same_week_paye_override ?& ARRAY[
       'continue','verified','used','pay_date','pay_week_start','pay_week_end',
       'reason','verified_by_user_id','verified_at_utc','reauth_purpose','guardrail_code']) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID'
      USING ERRCODE='22023';
  END IF;
  PERFORM private.workbench_settled_cert_same_week_override_validate_v8(
    p_same_week_paye_override
  );

  SELECT certificate.* INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certification_id=v_certification_id
  FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE='P0001';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle<>'SEALED_CURRENT'
     OR v_header.overall_digest_sha256 IS NULL
     OR v_header.certification_id IS DISTINCT FROM
          'WORKBENCH_SETTLED_CERTIFICATION_V2:'||v_header.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;

  SELECT session.* INTO v_session
  FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id
  FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;

  SELECT manifest.* INTO STRICT v_filter
  FROM private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid;
  SELECT manifest.* INTO STRICT v_channel
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid
    AND manifest.pay_channel_scope=v_pay_channel_scope;

  IF v_filter.candidate_filter_id IS DISTINCT FROM v_header.candidate_filter_id
     OR v_filter.client_filter_id IS DISTINCT FROM v_header.client_filter_id
     OR v_filter.filter_context_digest_sha256 IS DISTINCT FROM v_header.filter_context_digest_sha256
     OR v_filter.manifest_digest_sha256 IS DISTINCT FROM v_header.filter_scope_manifest_digest_sha256
     OR v_channel.constituent_count NOT BETWEEN 1 AND 50000
     OR v_channel.partition_count<=0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MANIFEST_MISMATCH' USING ERRCODE='55000';
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'certification_id',v_header.certification_id,
    'overall_digest_sha256',v_header.overall_digest_sha256,
    'pay_channel_scope',v_channel.pay_channel_scope,
    'manifest_digest_sha256',v_channel.manifest_digest_sha256,
    'idempotency_key',v_idempotency_key,
    'same_week_paye_override',p_same_week_paye_override,
    'candidate_filter_id',v_filter.candidate_filter_id,
    'client_filter_id',v_filter.client_filter_id,
    'filter_context_digest_sha256',v_filter.filter_context_digest_sha256);
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_current_reference_issue_v8(
  p_workbench_session_id uuid,
  p_session_version bigint,
  p_progress_counter_version bigint,
  p_pay_channel_scope text,
  p_idempotency_key text,
  p_same_week_paye_override jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
SET statement_timeout = '6000ms'
SET lock_timeout = '1000ms'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_all_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_paye_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_umbrella_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_certificate_count integer;
  v_certificate_uuid uuid;
  v_certification_id text;
  v_certificate_reference jsonb;
  v_requested_count integer;
  v_requested_manifest_digest text;
  v_pay_week_start date;
BEGIN
  IF p_workbench_session_id IS NULL
     OR p_session_version IS NULL OR p_session_version<1
     OR p_progress_counter_version IS NULL OR p_progress_counter_version<1 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID'
      USING ERRCODE='22023';
  END IF;

  SELECT session.* INTO v_session
  FROM public.banking_pay_workbench_sessions session
  WHERE session.id=p_workbench_session_id
  FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE='P0001';
  END IF;
  IF v_session.version IS DISTINCT FROM p_session_version
     OR v_session.progress_counter_version IS DISTINCT FROM p_progress_counter_version
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;

  SELECT COUNT(*)::integer,MIN(certificate.certificate_uuid::text)::uuid,
         MIN(certificate.certification_id)
  INTO v_certificate_count,v_certificate_uuid,v_certification_id
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.workbench_session_id=p_workbench_session_id
    AND certificate.session_version=p_session_version
    AND certificate.progress_counter_version=p_progress_counter_version
    AND certificate.authority_fence_generation=v_session.authority_fence_generation
    AND certificate.lifecycle='SEALED_CURRENT';
  IF v_certificate_count=0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE='P0001';
  ELSIF v_certificate_count<>1 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MULTIPLE_CURRENT' USING ERRCODE='55000';
  END IF;

  IF v_certificate_uuid IS NULL OR v_certification_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH' USING ERRCODE='55000';
  END IF;

  SELECT certificate.* INTO STRICT v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=v_certificate_uuid
    AND certificate.certification_id=v_certification_id;
  SELECT manifest.* INTO STRICT v_all_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='ALL';
  SELECT manifest.* INTO STRICT v_paye_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='PAYE';
  SELECT manifest.* INTO STRICT v_umbrella_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='UMBRELLA';

  IF v_all_manifest.constituent_count IS DISTINCT FROM v_header.selected_constituent_count
     OR v_all_manifest.constituent_count IS DISTINCT FROM
          v_paye_manifest.constituent_count+v_umbrella_manifest.constituent_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CHANNEL_SCOPE_MISMATCH'
      USING ERRCODE='55000';
  END IF;

  v_certificate_reference:=public.pay_workbench_settled_certificate_reference_issue_v8(
    v_certification_id,p_pay_channel_scope,p_idempotency_key,p_same_week_paye_override
  );
  v_requested_count:=CASE UPPER(BTRIM(p_pay_channel_scope))
    WHEN 'ALL' THEN v_all_manifest.constituent_count
    WHEN 'PAYE' THEN v_paye_manifest.constituent_count
    WHEN 'UMBRELLA' THEN v_umbrella_manifest.constituent_count
  END;
  v_requested_manifest_digest:=CASE UPPER(BTRIM(p_pay_channel_scope))
    WHEN 'ALL' THEN v_all_manifest.manifest_digest_sha256
    WHEN 'PAYE' THEN v_paye_manifest.manifest_digest_sha256
    WHEN 'UMBRELLA' THEN v_umbrella_manifest.manifest_digest_sha256
  END;
  IF v_requested_count IS NULL
     OR v_requested_manifest_digest IS DISTINCT FROM
          v_certificate_reference->>'manifest_digest_sha256' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_CHANNEL_SCOPE_MISMATCH'
      USING ERRCODE='55000';
  END IF;

  v_pay_week_start:=public._pay_week_start_monday(v_header.pay_date);
  RETURN pg_catalog.jsonb_build_object(
    'certificate_reference',v_certificate_reference,
    'pre_admission_scope_facts',pg_catalog.jsonb_build_object(
      'scope_facts_contract','WORKBENCH_SETTLED_CERTIFICATE_PRE_ADMISSION_SCOPE_FACTS_V1',
      'certificate_uuid',v_header.certificate_uuid,
      'certification_id',v_header.certification_id,
      'overall_digest_sha256',v_header.overall_digest_sha256,
      'workbench_session_id',v_header.workbench_session_id,
      'pay_channel_scope',UPPER(BTRIM(p_pay_channel_scope)),
      'manifest_digest_sha256',v_requested_manifest_digest,
      'selected_ready_total',v_all_manifest.constituent_count,
      'selected_ready_for_request',v_requested_count,
      'selected_ready_paye',v_paye_manifest.constituent_count,
      'selected_ready_umbrella',v_umbrella_manifest.constituent_count,
      'pay_date',v_header.pay_date::text,
      'pay_week_start',v_pay_week_start::text,
      'pay_week_end',(v_pay_week_start+6)::text,
      'week_ending_cutoff',v_header.week_ending_cutoff::text,
      'session_version',v_header.session_version,
      'progress_counter_version',v_header.progress_counter_version,
      'authority_fence_generation',v_header.authority_fence_generation));
END;
$function$;

-- Supported owner-internal join for the H2 database-owned initializer. A
-- postgres-owned SECURITY DEFINER routine may call this function directly;
-- service_role/browser callers may not. The returned compact contract is the
-- only admission authority and must not be reconstructed from caller JSON.
-- The persisted operation-link state column is link_state. A later H2-owned
-- final-freeze owner may update exactly one identity-matching row from
-- link_state='STAGING' to link_state='FROZEN' after revalidation; an identical
-- already-FROZEN row is replay and every other identity/state fails closed.
-- The admission JSON field freeze_state is only the external readback alias for
-- this stored link_state and does not name a second state authority.
CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_operation_admit_v8(
  p_operation_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_filter private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8%ROWTYPE;
  v_channel private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_reference jsonb;
  v_override jsonb;
  v_certification_id text;
  v_overall_digest text;
  v_pay_channel_scope text;
  v_manifest_digest text;
  v_reference_idempotency_key text;
  v_candidate_filter_text text;
  v_client_filter_text text;
  v_candidate_filter_id uuid;
  v_client_filter_id uuid;
  v_filter_context_digest text;
  v_continue boolean;
  v_verified boolean;
  v_used boolean;
  v_reason text;
  v_reauth_purpose text;
  v_guardrail_code text;
  v_verified_by_text text;
  v_verified_by uuid;
  v_verified_at_text text;
  v_verified_at timestamptz;
  v_pay_date date;
  v_pay_week_start date;
  v_pay_week_end date;
  v_rail_provider_snapshot text;
  v_rail_env_snapshot text;
  v_override_digest text;
  v_request_digest text;
  v_compact_operation_projection jsonb;
  v_replayed boolean := false;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','6000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1000',true);

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_ADMISSION_INVALID'
      USING ERRCODE='22023';
  END IF;

  SELECT operation.* INTO v_operation
  FROM public.banking_pay_operations operation
  WHERE operation.id=p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR UPPER(BTRIM(COALESCE(v_operation.operation_type,'')))<>'DRAFT_CREATE'
     OR v_operation.workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_ADMISSION_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_reference:=v_operation.input_json->'workbench_settled_certificate_reference_v8';
  IF jsonb_typeof(v_reference)<>'object'
     OR NOT (v_reference ?& ARRAY[
       'certification_id','overall_digest_sha256','pay_channel_scope','manifest_digest_sha256',
       'idempotency_key','same_week_paye_override','candidate_filter_id','client_filter_id',
       'filter_context_digest_sha256'])
     OR (SELECT COUNT(*) FROM pg_catalog.jsonb_object_keys(v_reference))<>9
     OR jsonb_typeof(v_reference->'certification_id')<>'string'
     OR jsonb_typeof(v_reference->'overall_digest_sha256')<>'string'
     OR jsonb_typeof(v_reference->'pay_channel_scope')<>'string'
     OR jsonb_typeof(v_reference->'manifest_digest_sha256')<>'string'
     OR jsonb_typeof(v_reference->'idempotency_key')<>'string'
     OR jsonb_typeof(v_reference->'filter_context_digest_sha256')<>'string' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_certification_id:=NULLIF(BTRIM(COALESCE(v_reference->>'certification_id','')),'');
  v_overall_digest:=LOWER(NULLIF(BTRIM(COALESCE(v_reference->>'overall_digest_sha256','')),''));
  v_pay_channel_scope:=UPPER(NULLIF(BTRIM(COALESCE(v_reference->>'pay_channel_scope','')),''));
  v_manifest_digest:=LOWER(NULLIF(BTRIM(COALESCE(v_reference->>'manifest_digest_sha256','')),''));
  v_reference_idempotency_key:=NULLIF(BTRIM(COALESCE(v_reference->>'idempotency_key','')),'');
  v_filter_context_digest:=LOWER(NULLIF(BTRIM(COALESCE(v_reference->>'filter_context_digest_sha256','')),''));

  IF v_certification_id IS NULL
     OR v_overall_digest IS NULL
     OR v_pay_channel_scope IS NULL
     OR v_manifest_digest IS NULL
     OR v_reference_idempotency_key IS NULL
     OR v_filter_context_digest IS NULL
     OR v_certification_id !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR v_overall_digest !~ '^[0-9a-f]{64}$'
     OR v_certification_id IS DISTINCT FROM 'WORKBENCH_SETTLED_CERTIFICATION_V2:'||v_overall_digest
     OR v_pay_channel_scope NOT IN ('ALL','PAYE','UMBRELLA')
     OR v_manifest_digest !~ '^[0-9a-f]{64}$'
     OR v_filter_context_digest !~ '^[0-9a-f]{64}$'
     OR v_reference_idempotency_key IS DISTINCT FROM v_operation.idempotency_key THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID'
      USING ERRCODE='22023';
  END IF;

  IF jsonb_typeof(v_reference->'candidate_filter_id') NOT IN ('string','null')
     OR jsonb_typeof(v_reference->'client_filter_id') NOT IN ('string','null') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_MISMATCH'
      USING ERRCODE='22023';
  END IF;
  v_candidate_filter_text:=NULLIF(BTRIM(COALESCE(v_reference->>'candidate_filter_id','')),'');
  v_client_filter_text:=NULLIF(BTRIM(COALESCE(v_reference->>'client_filter_id','')),'');
  IF (v_candidate_filter_text IS NOT NULL AND v_candidate_filter_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
     OR (v_client_filter_text IS NOT NULL AND v_client_filter_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_MISMATCH'
      USING ERRCODE='22023';
  END IF;
  v_candidate_filter_id:=v_candidate_filter_text::uuid;
  v_client_filter_id:=v_client_filter_text::uuid;

  v_override:=v_reference->'same_week_paye_override';
  PERFORM private.workbench_settled_cert_same_week_override_validate_v8(v_override);
  IF jsonb_typeof(v_override)<>'object'
     OR (SELECT COUNT(*) FROM pg_catalog.jsonb_object_keys(v_override))<>11
     OR jsonb_typeof(v_override->'continue')<>'boolean'
     OR jsonb_typeof(v_override->'verified')<>'boolean'
     OR jsonb_typeof(v_override->'used')<>'boolean'
     OR jsonb_typeof(v_override->'pay_date')<>'string'
     OR jsonb_typeof(v_override->'pay_week_start')<>'string'
     OR jsonb_typeof(v_override->'pay_week_end')<>'string'
     OR NOT (v_override ?& ARRAY[
       'reason','verified_by_user_id','verified_at_utc','reauth_purpose','guardrail_code']) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_continue:=(v_override->>'continue')::boolean;
  v_verified:=(v_override->>'verified')::boolean;
  v_used:=(v_override->>'used')::boolean;
  v_reason:=CASE WHEN jsonb_typeof(v_override->'reason')='null' THEN NULL ELSE v_override->>'reason' END;
  v_reauth_purpose:=CASE WHEN jsonb_typeof(v_override->'reauth_purpose')='null' THEN NULL ELSE v_override->>'reauth_purpose' END;
  v_guardrail_code:=CASE WHEN jsonb_typeof(v_override->'guardrail_code')='null' THEN NULL ELSE v_override->>'guardrail_code' END;
  v_verified_by_text:=CASE WHEN jsonb_typeof(v_override->'verified_by_user_id')='null' THEN NULL ELSE v_override->>'verified_by_user_id' END;
  v_verified_at_text:=CASE WHEN jsonb_typeof(v_override->'verified_at_utc')='null' THEN NULL ELSE v_override->>'verified_at_utc' END;

  IF jsonb_typeof(v_override->'reason') NOT IN ('string','null')
     OR jsonb_typeof(v_override->'reauth_purpose') NOT IN ('string','null')
     OR jsonb_typeof(v_override->'guardrail_code') NOT IN ('string','null')
     OR jsonb_typeof(v_override->'verified_by_user_id') NOT IN ('string','null')
     OR jsonb_typeof(v_override->'verified_at_utc') NOT IN ('string','null')
     OR (v_guardrail_code IS NOT NULL AND v_guardrail_code<>'PAYE_SAME_WEEK_OVERRIDE_REQUIRED')
     OR (v_verified_by_text IS NOT NULL AND v_verified_by_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  BEGIN
    v_pay_date:=(v_override->>'pay_date')::date;
    v_pay_week_start:=(v_override->>'pay_week_start')::date;
    v_pay_week_end:=(v_override->>'pay_week_end')::date;
    v_verified_by:=v_verified_by_text::uuid;
    v_verified_at:=v_verified_at_text::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END;

  IF v_pay_week_start>v_pay_date OR v_pay_date>v_pay_week_end
     OR (v_used IS FALSE AND (
       v_verified IS TRUE OR v_reason IS NOT NULL OR v_reauth_purpose IS NOT NULL
       OR v_verified_by IS NOT NULL OR v_verified_at IS NOT NULL))
     OR (v_used IS TRUE AND (
       v_continue IS NOT TRUE OR v_verified IS NOT TRUE
       OR NULLIF(BTRIM(COALESCE(v_reason,'')),'') IS NULL
       OR char_length(v_reason)>2000
       OR v_reauth_purpose IS DISTINCT FROM 'PAYE_SAME_WEEK_OVERRIDE'
       OR v_verified_by IS NULL OR v_verified_at IS NULL)) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_override_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_override));
  v_request_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_reference));
  v_rail_provider_snapshot:=UPPER(BTRIM(NULLIF(
    v_operation.input_json->>'rail_provider_snapshot','')));
  v_rail_env_snapshot:=UPPER(BTRIM(NULLIF(
    v_operation.input_json->>'rail_env_snapshot','')));
  IF v_rail_provider_snapshot IS NULL OR v_rail_env_snapshot IS NULL THEN
    SELECT UPPER(BTRIM(NULLIF(settings.rail_provider_default,''))),
           UPPER(BTRIM(NULLIF(settings.rail_env_default,'')))
    INTO v_rail_provider_snapshot,v_rail_env_snapshot
    FROM public.settings_defaults AS settings
    ORDER BY settings.id
    LIMIT 1;
  END IF;
  IF v_rail_provider_snapshot IS NULL OR v_rail_env_snapshot IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAYMENT_RAIL_SNAPSHOT_MISSING'
      USING ERRCODE='55000';
  END IF;
  -- Preserve the established Draft input field names, but populate them only
  -- from the reference that this owner has independently validated.  The
  -- nested certificate reference and operation link remain the authority;
  -- these compact fields are a compatibility projection, not a second
  -- selection or payment-policy input.
  v_compact_operation_projection:=pg_catalog.jsonb_build_object(
    'pay_channel_scope',v_pay_channel_scope,
    'draft_scope',v_pay_channel_scope,
    'same_week_paye_override',v_override,
    'rail_provider_snapshot',v_rail_provider_snapshot,
    'rail_env_snapshot',v_rail_env_snapshot
  );

  SELECT certificate.* INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certification_id=v_certification_id
    AND certificate.overall_digest_sha256=v_overall_digest
  FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE='P0001';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle<>'SEALED_CURRENT'
     OR v_operation.workbench_session_id IS DISTINCT FROM v_header.workbench_session_id THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;

  SELECT session.* INTO v_session
  FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id
  FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;

  SELECT manifest.* INTO STRICT v_filter
  FROM private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid;
  SELECT manifest.* INTO STRICT v_channel
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid
    AND manifest.pay_channel_scope=v_pay_channel_scope;

  IF v_candidate_filter_id IS DISTINCT FROM v_header.candidate_filter_id
     OR v_client_filter_id IS DISTINCT FROM v_header.client_filter_id
     OR v_candidate_filter_id IS DISTINCT FROM v_filter.candidate_filter_id
     OR v_client_filter_id IS DISTINCT FROM v_filter.client_filter_id
     OR v_filter_context_digest IS DISTINCT FROM v_header.filter_context_digest_sha256
     OR v_filter_context_digest IS DISTINCT FROM v_filter.filter_context_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_CONTEXT_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_manifest_digest IS DISTINCT FROM v_channel.manifest_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MANIFEST_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_channel.constituent_count NOT BETWEEN 1 AND 50000
     OR v_channel.partition_count<=0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE' USING ERRCODE='55000';
  END IF;

  SELECT link.* INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  WHERE link.operation_id=p_operation_id
  FOR UPDATE;
  IF FOUND THEN
    IF v_link.certificate_uuid IS DISTINCT FROM v_header.certificate_uuid
       OR v_link.certification_id IS DISTINCT FROM v_certification_id
       OR v_link.overall_digest_sha256 IS DISTINCT FROM v_overall_digest
       OR v_link.pay_channel_scope IS DISTINCT FROM v_pay_channel_scope
       OR v_link.idempotency_key IS DISTINCT FROM v_operation.idempotency_key
       OR v_link.admission_request_digest_sha256 IS DISTINCT FROM v_request_digest
       OR v_link.channel_manifest_digest_sha256 IS DISTINCT FROM v_manifest_digest
       OR v_link.candidate_filter_id IS DISTINCT FROM v_candidate_filter_id
       OR v_link.client_filter_id IS DISTINCT FROM v_client_filter_id
       OR v_link.filter_context_digest_sha256 IS DISTINCT FROM v_filter_context_digest
       OR v_link.filter_scope_manifest_digest_sha256 IS DISTINCT FROM v_filter.manifest_digest_sha256
       OR v_link.same_week_paye_override_digest_sha256 IS DISTINCT FROM v_override_digest
       OR v_operation.input_json->>'pay_channel_scope' IS DISTINCT FROM v_pay_channel_scope
       OR v_operation.input_json->>'draft_scope' IS DISTINCT FROM v_pay_channel_scope
       OR v_operation.input_json->'same_week_paye_override' IS DISTINCT FROM v_override
       OR v_operation.input_json->>'rail_provider_snapshot' IS DISTINCT FROM v_rail_provider_snapshot
       OR v_operation.input_json->>'rail_env_snapshot' IS DISTINCT FROM v_rail_env_snapshot THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH'
        USING ERRCODE='23505';
    END IF;
    v_replayed:=true;
  ELSE
    IF UPPER(BTRIM(COALESCE(v_operation.status,''))) NOT IN ('QUEUED','RUNNING')
       OR UPPER(BTRIM(COALESCE(v_operation.phase,'')))<>'INITIALISE'
       OR v_operation.pay_batch_id IS NOT NULL
       OR v_operation.scope_frozen_at_utc IS NOT NULL
       OR COALESCE(v_operation.source_scope_seed_complete,false)
       OR COALESCE(v_operation.total_units,0)<>0
       OR COALESCE(v_operation.completed_units,0)<>0
       OR COALESCE(v_operation.failed_units,0)<>0 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_ALREADY_ADVANCED'
        USING ERRCODE='55000';
    END IF;
    INSERT INTO private.banking_pay_workbench_settled_certificate_operation_links_v8(
      operation_id,certificate_uuid,certification_id,overall_digest_sha256,pay_channel_scope,
      idempotency_key,admission_request_digest_sha256,channel_manifest_digest_sha256,
      link_state,candidate_filter_id,client_filter_id,filter_context_digest_sha256,
      filter_scope_manifest_digest_sha256,same_week_paye_override_reason,
      same_week_paye_override_reauth_purpose,same_week_paye_override_continue,
      same_week_paye_override_verified,same_week_paye_override_used,
      same_week_paye_override_pay_date,same_week_paye_override_pay_week_start,
      same_week_paye_override_pay_week_end,same_week_paye_override_guardrail_code,
      same_week_paye_override_digest_sha256,same_week_paye_override_verified_by_user_id,
      same_week_paye_override_verified_at_utc
    ) VALUES (
      p_operation_id,v_header.certificate_uuid,v_certification_id,v_overall_digest,v_pay_channel_scope,
      v_operation.idempotency_key,v_request_digest,v_manifest_digest,
      'STAGING',v_candidate_filter_id,v_client_filter_id,v_filter_context_digest,
      v_filter.manifest_digest_sha256,v_reason,v_reauth_purpose,v_continue,
      v_verified,v_used,v_pay_date,v_pay_week_start,v_pay_week_end,v_guardrail_code,
      v_override_digest,v_verified_by,v_verified_at
    );
    UPDATE public.banking_pay_operations operation
    SET input_json=COALESCE(operation.input_json,'{}'::jsonb)||v_compact_operation_projection,
        progress_json=pg_catalog.jsonb_strip_nulls(COALESCE(operation.progress_json,'{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'workbench_settled_certificate_admitted',true,
            'workbench_settled_certification_id',v_certification_id,
            'workbench_settled_overall_digest_sha256',v_overall_digest,
            'workbench_settled_pay_channel_scope',v_pay_channel_scope,
            'workbench_settled_channel_manifest_digest_sha256',v_manifest_digest,
            'workbench_settled_admitted_at_utc',pg_catalog.clock_timestamp()::text)),
        updated_at_utc=pg_catalog.clock_timestamp()
    WHERE operation.id=p_operation_id;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'contract','WORKBENCH_SETTLED_CERTIFICATE_OPERATION_ADMISSION_V8',
    'ok',true,'operation_id',p_operation_id,'certificate_uuid',v_header.certificate_uuid,
    'certification_id',v_certification_id,
    'overall_digest_sha256',v_overall_digest,'workbench_session_id',v_header.workbench_session_id,
    'session_version',v_header.session_version,'progress_counter_version',v_header.progress_counter_version,
    'authority_fence_generation',v_header.authority_fence_generation,
    'pay_channel_scope',v_pay_channel_scope,'manifest_digest_sha256',v_manifest_digest,
    'constituent_count',v_channel.constituent_count,'partition_count',v_channel.partition_count,
    'canonical_amount_ex_vat_total',v_channel.canonical_amount_ex_vat_total,
    'lifecycle',v_header.lifecycle,'freeze_state','STAGING','replayed',v_replayed,
    'candidate_filter_id',v_candidate_filter_id,'client_filter_id',v_client_filter_id,
    'filter_context_digest_sha256',v_filter_context_digest,
    'filter_scope_manifest_digest_sha256',v_filter.manifest_digest_sha256,
    'admission_request_digest_sha256',v_request_digest,
    'same_week_paye_override_digest_sha256',v_override_digest,
    'compact_operation_projection_contract','WORKBENCH_SETTLED_CERTIFICATE_OPERATION_PROJECTION_V1');
END;
$function$;

-- All bounded certificate readers below are pre-freeze staging readers. They
-- accept stored link_state ADMITTED/STAGING only; once H2 atomically records
-- link_state FROZEN, no further certificate paging or filter reads are allowed.
CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_entry_page_v8(
  p_operation_id uuid,
  p_certification_id text,
  p_after_ordinal integer,
  p_limit integer,
  p_expected_previous_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_previous private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_existing private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_rows jsonb := '[]'::jsonb;
  v_row jsonb;
  v_entry record;
  v_scope_key text;
  v_page_kind constant text := 'CERTIFICATE_ENTRIES';
  v_page_sequence integer;
  v_next_after integer;
  v_row_count integer := 0;
  v_row_bytes integer;
  v_page_bytes integer := 2;
  v_has_more boolean;
  v_replayed boolean := false;
  v_request_digest text;
  v_page_digest text;
  v_response_core jsonb;
  v_receipt_core jsonb;
  v_row_receipts jsonb := '[]'::jsonb;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','15000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1500',true);
  IF p_operation_id IS NULL OR COALESCE(p_certification_id,'') !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR p_limit NOT BETWEEN 1 AND 256
     OR (p_after_ordinal IS NOT NULL AND p_after_ordinal<0)
     OR (p_after_ordinal IS NULL AND p_expected_previous_receipt_sha256 IS NOT NULL)
     OR (p_after_ordinal IS NOT NULL AND COALESCE(p_expected_previous_receipt_sha256,'') !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT link.* INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  WHERE link.operation_id=p_operation_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  SELECT certificate.* INTO v_header
  FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=v_link.certificate_uuid FOR SHARE;
  IF NOT FOUND OR v_link.certification_id IS DISTINCT FROM p_certification_id
     OR v_header.certification_id IS DISTINCT FROM p_certification_id
     OR v_link.overall_digest_sha256 IS DISTINCT FROM v_header.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_link.link_state NOT IN ('ADMITTED','STAGING') OR v_header.lifecycle<>'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;
  SELECT session.* INTO v_session FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;
  SELECT * INTO STRICT v_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid
    AND manifest.pay_channel_scope=v_link.pay_channel_scope;

  v_scope_key:='OPERATION:'||p_operation_id::text;
  IF p_after_ordinal IS NULL THEN
    v_page_sequence:=0;
  ELSE
    SELECT receipt.* INTO v_previous
    FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
    WHERE receipt.certificate_uuid=v_header.certificate_uuid
      AND receipt.request_scope_key=v_scope_key AND receipt.page_kind=v_page_kind
      AND receipt.pay_channel_scope=v_link.pay_channel_scope
      AND receipt.next_after_ordinal=p_after_ordinal
      AND receipt.page_digest_sha256=p_expected_previous_receipt_sha256
    ORDER BY receipt.page_sequence DESC LIMIT 1;
    IF NOT FOUND OR NOT v_previous.has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH' USING ERRCODE='55000';
    END IF;
    v_page_sequence:=v_previous.page_sequence+1;
  END IF;
  v_request_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'operation_id',p_operation_id,'stage_kind',v_page_kind,'pay_channel_scope',v_link.pay_channel_scope,
      'after_ordinal',p_after_ordinal,'requested_limit',p_limit,
      'expected_previous_receipt_sha256',p_expected_previous_receipt_sha256)));
  SELECT receipt.* INTO v_existing
  FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
  WHERE receipt.certificate_uuid=v_header.certificate_uuid
    AND receipt.request_scope_key=v_scope_key AND receipt.page_kind=v_page_kind
    AND receipt.pay_channel_scope=v_link.pay_channel_scope
    AND receipt.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;
  IF FOUND THEN
    IF v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT' USING ERRCODE='23505';
    END IF;
    v_replayed:=true;
    v_page_sequence:=v_existing.page_sequence;
  END IF;

  FOR v_entry IN
    SELECT entry.constituent_ordinal
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
    WHERE entry.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR entry.resolved_pay_channel=v_link.pay_channel_scope)
      AND (p_after_ordinal IS NULL OR entry.constituent_ordinal>p_after_ordinal)
    ORDER BY entry.constituent_ordinal LIMIT p_limit
  LOOP
    v_row:=pg_catalog.jsonb_build_object(
      'constituent_ordinal',v_entry.constituent_ordinal,
      'constituent',private.pay_workbench_settled_certificate_constituent_json_v8(
        v_header.certificate_uuid,v_entry.constituent_ordinal));
    v_row_bytes:=pg_catalog.octet_length(private.pay_workbench_settled_certificate_stable_stringify_v8(v_row));
    IF v_row_bytes>65536 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED' USING ERRCODE='54000';
    END IF;
    IF v_row_count>0 AND v_page_bytes+1+v_row_bytes>524288 THEN EXIT; END IF;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);
    v_row_receipts:=v_row_receipts||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'constituent_ordinal',v_entry.constituent_ordinal,
      'constituent_digest_sha256',v_row#>>'{constituent,constituent_digest_sha256}'));
    v_page_bytes:=v_page_bytes+CASE WHEN v_row_count=0 THEN 0 ELSE 1 END+v_row_bytes;
    v_row_count:=v_row_count+1;
    v_next_after:=v_entry.constituent_ordinal;
  END LOOP;
  IF v_row_count=0 AND p_after_ordinal IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE' USING ERRCODE='55000';
  END IF;
  v_has_more:=EXISTS(
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
    WHERE entry.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR entry.resolved_pay_channel=v_link.pay_channel_scope)
      AND (v_next_after IS NULL OR entry.constituent_ordinal>v_next_after));
  v_response_core:=pg_catalog.jsonb_build_object(
    'operation_id',p_operation_id,'stage_kind',v_page_kind,'page_sequence',v_page_sequence,
    'after_ordinal',p_after_ordinal,'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_constituents_digest_sha256,'rows',v_rows);
  -- The rows are already individually bound by their constituent digests.  A
  -- compact receipt hashes those exact digests plus paging metadata instead of
  -- re-hashing up to 512 KiB of returned JSON in one call.  The response rows
  -- and V1 stream digest are unchanged.
  v_receipt_core:=pg_catalog.jsonb_build_object(
    'receipt_contract','WORKBENCH_CERTIFICATE_PAGE_RECEIPT_V2',
    'request_preimage_digest_sha256',v_request_digest,'page_sequence',v_page_sequence,
    'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_constituents_digest_sha256,
    'row_receipts',v_row_receipts);
  v_page_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_receipt_core));
  IF v_replayed THEN
    IF v_existing.page_digest_sha256 IS DISTINCT FROM v_page_digest
       OR v_existing.row_count<>v_row_count OR v_existing.canonical_byte_count<>v_page_bytes
       OR v_existing.next_after_ordinal IS DISTINCT FROM v_next_after
       OR v_existing.has_more IS DISTINCT FROM v_has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED' USING ERRCODE='55000';
    END IF;
  ELSE
    INSERT INTO private.banking_pay_workbench_settled_certificate_page_receipts_v8(
      certificate_uuid,request_scope_key,page_kind,pay_channel_scope,page_sequence,after_ordinal,
      requested_limit,expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
      canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,page_digest_sha256
    ) VALUES (
      v_header.certificate_uuid,v_scope_key,v_page_kind,v_link.pay_channel_scope,v_page_sequence,p_after_ordinal,
      p_limit,p_expected_previous_receipt_sha256,v_request_digest,v_row_count,v_page_bytes,v_next_after,
      v_has_more,true,v_page_digest);
  END IF;
  RETURN v_response_core||pg_catalog.jsonb_build_object(
    'page_receipt',v_receipt_core,'page_receipt_digest_sha256',v_page_digest,'replayed',v_replayed);
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_partition_page_v8(
  p_operation_id uuid,
  p_certification_id text,
  p_after_ordinal integer,
  p_limit integer,
  p_expected_previous_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_previous private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_existing private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_rows jsonb := '[]'::jsonb;
  v_row jsonb;
  v_member record;
  v_scope_key text;
  v_page_kind constant text := 'CERTIFICATE_PARTITION_MEMBERS';
  v_page_sequence integer;
  v_next_after integer;
  v_row_count integer := 0;
  v_row_bytes integer;
  v_page_bytes integer := 2;
  v_has_more boolean;
  v_replayed boolean := false;
  v_request_digest text;
  v_page_digest text;
  v_response_core jsonb;
  v_receipt_core jsonb;
  v_row_receipts jsonb := '[]'::jsonb;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','15000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1500',true);
  IF p_operation_id IS NULL OR COALESCE(p_certification_id,'') !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR p_limit NOT BETWEEN 1 AND 256
     OR (p_after_ordinal IS NOT NULL AND p_after_ordinal<0)
     OR (p_after_ordinal IS NULL AND p_expected_previous_receipt_sha256 IS NOT NULL)
     OR (p_after_ordinal IS NOT NULL AND COALESCE(p_expected_previous_receipt_sha256,'') !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT link.* INTO v_link FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  WHERE link.operation_id=p_operation_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  SELECT certificate.* INTO v_header FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=v_link.certificate_uuid FOR SHARE;
  IF NOT FOUND OR v_link.certification_id IS DISTINCT FROM p_certification_id
     OR v_header.certification_id IS DISTINCT FROM p_certification_id
     OR v_link.overall_digest_sha256 IS DISTINCT FROM v_header.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_link.link_state NOT IN ('ADMITTED','STAGING') OR v_header.lifecycle<>'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;
  SELECT session.* INTO v_session FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;
  SELECT * INTO STRICT v_manifest FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid AND manifest.pay_channel_scope=v_link.pay_channel_scope;
  v_scope_key:='OPERATION:'||p_operation_id::text;
  IF p_after_ordinal IS NULL THEN
    v_page_sequence:=0;
  ELSE
    SELECT receipt.* INTO v_previous FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
    WHERE receipt.certificate_uuid=v_header.certificate_uuid AND receipt.request_scope_key=v_scope_key
      AND receipt.page_kind=v_page_kind AND receipt.pay_channel_scope=v_link.pay_channel_scope
      AND receipt.next_after_ordinal=p_after_ordinal
      AND receipt.page_digest_sha256=p_expected_previous_receipt_sha256
    ORDER BY receipt.page_sequence DESC LIMIT 1;
    IF NOT FOUND OR NOT v_previous.has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH' USING ERRCODE='55000';
    END IF;
    v_page_sequence:=v_previous.page_sequence+1;
  END IF;
  v_request_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'operation_id',p_operation_id,'stage_kind',v_page_kind,'pay_channel_scope',v_link.pay_channel_scope,
      'after_ordinal',p_after_ordinal,'requested_limit',p_limit,
      'expected_previous_receipt_sha256',p_expected_previous_receipt_sha256)));
  SELECT receipt.* INTO v_existing FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
  WHERE receipt.certificate_uuid=v_header.certificate_uuid AND receipt.request_scope_key=v_scope_key
    AND receipt.page_kind=v_page_kind AND receipt.pay_channel_scope=v_link.pay_channel_scope
    AND receipt.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;
  IF FOUND THEN
    IF v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT' USING ERRCODE='23505';
    END IF;
    v_replayed:=true;v_page_sequence:=v_existing.page_sequence;
  END IF;
  FOR v_member IN
    SELECT member.stream_ordinal,member.partition_ordinal,member.member_ordinal,
           member.constituent_ordinal,member.stable_identity_digest_sha256,
           partition.candidate_id,partition.resolved_pay_channel,partition.constituent_count,
           partition.canonical_amount_ex_vat_total,partition.partition_digest_sha256
    FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
    JOIN private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      ON partition.certificate_uuid=member.certificate_uuid AND partition.partition_ordinal=member.partition_ordinal
    WHERE member.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR partition.resolved_pay_channel=v_link.pay_channel_scope)
      AND (p_after_ordinal IS NULL OR member.stream_ordinal>p_after_ordinal)
    ORDER BY member.stream_ordinal LIMIT p_limit
  LOOP
    v_row:=pg_catalog.to_jsonb(v_member);
    v_row_bytes:=pg_catalog.octet_length(private.pay_workbench_settled_certificate_stable_stringify_v8(v_row));
    IF v_row_bytes>65536 THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED' USING ERRCODE='54000'; END IF;
    IF v_row_count>0 AND v_page_bytes+1+v_row_bytes>524288 THEN EXIT; END IF;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);
    v_row_receipts:=v_row_receipts||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'stream_ordinal',v_member.stream_ordinal,'partition_ordinal',v_member.partition_ordinal,
      'member_ordinal',v_member.member_ordinal,'constituent_ordinal',v_member.constituent_ordinal,
      'stable_identity_digest_sha256',v_member.stable_identity_digest_sha256,
      'partition_digest_sha256',v_member.partition_digest_sha256));
    v_page_bytes:=v_page_bytes+CASE WHEN v_row_count=0 THEN 0 ELSE 1 END+v_row_bytes;
    v_row_count:=v_row_count+1;v_next_after:=v_member.stream_ordinal;
  END LOOP;
  IF v_row_count=0 AND p_after_ordinal IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE' USING ERRCODE='55000';
  END IF;
  v_has_more:=EXISTS(
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
    JOIN private.banking_pay_workbench_settled_certificate_partitions_v8 partition
      ON partition.certificate_uuid=member.certificate_uuid AND partition.partition_ordinal=member.partition_ordinal
    WHERE member.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR partition.resolved_pay_channel=v_link.pay_channel_scope)
      AND (v_next_after IS NULL OR member.stream_ordinal>v_next_after));
  v_response_core:=pg_catalog.jsonb_build_object(
    'operation_id',p_operation_id,'stage_kind',v_page_kind,'page_sequence',v_page_sequence,
    'after_ordinal',p_after_ordinal,'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'member_stream_total_count',v_manifest.constituent_count,
    'partition_count',v_manifest.partition_count,
    'selected_partitions_digest_sha256',v_manifest.selected_partitions_digest_sha256,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_partitions_digest_sha256,'rows',v_rows);
  v_receipt_core:=pg_catalog.jsonb_build_object(
    'receipt_contract','WORKBENCH_CERTIFICATE_PAGE_RECEIPT_V2',
    'request_preimage_digest_sha256',v_request_digest,'page_sequence',v_page_sequence,
    'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_partitions_digest_sha256,
    'row_receipts',v_row_receipts);
  v_page_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_receipt_core));
  IF v_replayed THEN
    IF v_existing.page_digest_sha256 IS DISTINCT FROM v_page_digest
       OR v_existing.row_count<>v_row_count OR v_existing.canonical_byte_count<>v_page_bytes
       OR v_existing.next_after_ordinal IS DISTINCT FROM v_next_after OR v_existing.has_more IS DISTINCT FROM v_has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED' USING ERRCODE='55000';
    END IF;
  ELSE
    INSERT INTO private.banking_pay_workbench_settled_certificate_page_receipts_v8(
      certificate_uuid,request_scope_key,page_kind,pay_channel_scope,page_sequence,after_ordinal,
      requested_limit,expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
      canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,page_digest_sha256
    ) VALUES (
      v_header.certificate_uuid,v_scope_key,v_page_kind,v_link.pay_channel_scope,v_page_sequence,p_after_ordinal,
      p_limit,p_expected_previous_receipt_sha256,v_request_digest,v_row_count,v_page_bytes,v_next_after,
      v_has_more,true,v_page_digest);
  END IF;
  RETURN v_response_core||pg_catalog.jsonb_build_object(
    'page_receipt',v_receipt_core,'page_receipt_digest_sha256',v_page_digest,'replayed',v_replayed);
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_component_page_v8(
  p_operation_id uuid,
  p_certification_id text,
  p_evidence_kind text,
  p_after_ordinal integer,
  p_limit integer,
  p_expected_previous_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_previous private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_existing private.banking_pay_workbench_settled_certificate_page_receipts_v8%ROWTYPE;
  v_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_rows jsonb := '[]'::jsonb;
  v_evidence jsonb;
  v_row jsonb;
  v_entry record;
  v_scope_key text;
  v_page_kind text;
  v_page_sequence integer;
  v_next_after integer;
  v_row_count integer := 0;
  v_row_bytes integer;
  v_page_bytes integer := 2;
  v_has_more boolean;
  v_replayed boolean := false;
  v_request_digest text;
  v_page_digest text;
  v_response_core jsonb;
  v_receipt_core jsonb;
  v_row_receipts jsonb := '[]'::jsonb;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','15000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1500',true);
  IF p_operation_id IS NULL OR COALESCE(p_certification_id,'') !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$'
     OR p_evidence_kind NOT IN ('ALL_SAME_ECONOMIC_KEY','FULL_SIGNED_PRE_SIGNATURE','DECISIVE_SIGNED_EVIDENCE')
     OR p_limit NOT BETWEEN 1 AND 256
     OR (p_after_ordinal IS NOT NULL AND p_after_ordinal<0)
     OR (p_after_ordinal IS NULL AND p_expected_previous_receipt_sha256 IS NOT NULL)
     OR (p_after_ordinal IS NOT NULL AND COALESCE(p_expected_previous_receipt_sha256,'') !~ '^[0-9a-f]{64}$') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_INVALID' USING ERRCODE='22023';
  END IF;
  v_page_kind:='CERTIFICATE_COMPONENTS:'||p_evidence_kind;
  SELECT link.* INTO v_link FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  WHERE link.operation_id=p_operation_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  SELECT certificate.* INTO v_header FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=v_link.certificate_uuid FOR SHARE;
  IF NOT FOUND OR v_link.certification_id IS DISTINCT FROM p_certification_id
     OR v_header.certification_id IS DISTINCT FROM p_certification_id
     OR v_link.overall_digest_sha256 IS DISTINCT FROM v_header.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_link.link_state NOT IN ('ADMITTED','STAGING') OR v_header.lifecycle<>'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;
  SELECT session.* INTO v_session FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;
  SELECT * INTO STRICT v_manifest FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid AND manifest.pay_channel_scope=v_link.pay_channel_scope;
  v_scope_key:='OPERATION:'||p_operation_id::text;
  IF p_after_ordinal IS NULL THEN
    v_page_sequence:=0;
  ELSE
    SELECT receipt.* INTO v_previous FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
    WHERE receipt.certificate_uuid=v_header.certificate_uuid AND receipt.request_scope_key=v_scope_key
      AND receipt.page_kind=v_page_kind AND receipt.pay_channel_scope=v_link.pay_channel_scope
      AND receipt.next_after_ordinal=p_after_ordinal
      AND receipt.page_digest_sha256=p_expected_previous_receipt_sha256
    ORDER BY receipt.page_sequence DESC LIMIT 1;
    IF NOT FOUND OR NOT v_previous.has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH' USING ERRCODE='55000';
    END IF;
    v_page_sequence:=v_previous.page_sequence+1;
  END IF;
  v_request_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(pg_catalog.jsonb_build_object(
      'operation_id',p_operation_id,'stage_kind',v_page_kind,'pay_channel_scope',v_link.pay_channel_scope,
      'after_ordinal',p_after_ordinal,'requested_limit',p_limit,
      'expected_previous_receipt_sha256',p_expected_previous_receipt_sha256)));
  SELECT receipt.* INTO v_existing FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
  WHERE receipt.certificate_uuid=v_header.certificate_uuid AND receipt.request_scope_key=v_scope_key
    AND receipt.page_kind=v_page_kind AND receipt.pay_channel_scope=v_link.pay_channel_scope
    AND receipt.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;
  IF FOUND THEN
    IF v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT' USING ERRCODE='23505';
    END IF;
    v_replayed:=true;v_page_sequence:=v_existing.page_sequence;
  END IF;
  FOR v_entry IN
    SELECT entry.constituent_ordinal,entry.all_same_key_count,entry.all_same_key_digest_sha256,
           entry.signed_match_count,entry.signed_match_digest_sha256,
           entry.decisive_signed_count,entry.decisive_signed_digest_sha256
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
    WHERE entry.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR entry.resolved_pay_channel=v_link.pay_channel_scope)
      AND (p_after_ordinal IS NULL OR entry.constituent_ordinal>p_after_ordinal)
    ORDER BY entry.constituent_ordinal LIMIT p_limit
  LOOP
    SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(evidence)-'certificate_uuid'
             ORDER BY evidence.evidence_ordinal),'[]'::jsonb)
    INTO v_evidence
    FROM private.banking_pay_workbench_settled_certificate_component_evidence_v8 evidence
    WHERE evidence.certificate_uuid=v_header.certificate_uuid
      AND evidence.constituent_ordinal=v_entry.constituent_ordinal
      AND evidence.evidence_kind=p_evidence_kind;
    v_row:=pg_catalog.jsonb_build_object(
      'constituent_ordinal',v_entry.constituent_ordinal,'evidence_kind',p_evidence_kind,
      'expected_count',CASE p_evidence_kind
        WHEN 'ALL_SAME_ECONOMIC_KEY' THEN v_entry.all_same_key_count
        WHEN 'FULL_SIGNED_PRE_SIGNATURE' THEN v_entry.signed_match_count
        ELSE v_entry.decisive_signed_count END,
      'expected_digest_sha256',CASE p_evidence_kind
        WHEN 'ALL_SAME_ECONOMIC_KEY' THEN v_entry.all_same_key_digest_sha256
        WHEN 'FULL_SIGNED_PRE_SIGNATURE' THEN v_entry.signed_match_digest_sha256
        ELSE v_entry.decisive_signed_digest_sha256 END,
      'evidence_rows',v_evidence);
    v_row_bytes:=pg_catalog.octet_length(private.pay_workbench_settled_certificate_stable_stringify_v8(v_row));
    IF v_row_bytes>65536 THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED' USING ERRCODE='54000'; END IF;
    IF v_row_count>0 AND v_page_bytes+1+v_row_bytes>524288 THEN EXIT; END IF;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);
    v_row_receipts:=v_row_receipts||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'constituent_ordinal',v_entry.constituent_ordinal,'evidence_kind',p_evidence_kind,
      'expected_count',v_row->'expected_count',
      'expected_digest_sha256',v_row->>'expected_digest_sha256'));
    v_page_bytes:=v_page_bytes+CASE WHEN v_row_count=0 THEN 0 ELSE 1 END+v_row_bytes;
    v_row_count:=v_row_count+1;v_next_after:=v_entry.constituent_ordinal;
  END LOOP;
  IF v_row_count=0 AND p_after_ordinal IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE' USING ERRCODE='55000';
  END IF;
  v_has_more:=EXISTS(SELECT 1 FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
    WHERE entry.certificate_uuid=v_header.certificate_uuid
      AND (v_link.pay_channel_scope='ALL' OR entry.resolved_pay_channel=v_link.pay_channel_scope)
      AND (v_next_after IS NULL OR entry.constituent_ordinal>v_next_after));
  v_response_core:=pg_catalog.jsonb_build_object(
    'operation_id',p_operation_id,'stage_kind',v_page_kind,'page_sequence',v_page_sequence,
    'after_ordinal',p_after_ordinal,'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_constituents_digest_sha256,'rows',v_rows);
  v_receipt_core:=pg_catalog.jsonb_build_object(
    'receipt_contract','WORKBENCH_CERTIFICATE_PAGE_RECEIPT_V2',
    'request_preimage_digest_sha256',v_request_digest,'page_sequence',v_page_sequence,
    'row_count',v_row_count,'canonical_byte_count',v_page_bytes,
    'next_after_ordinal',v_next_after,'has_more',v_has_more,'terminal_sentinel_present',true,
    'stage_total_count',v_manifest.constituent_count,
    'stage_total_digest_sha256',v_manifest.selected_constituents_digest_sha256,
    'row_receipts',v_row_receipts);
  v_page_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_receipt_core));
  IF v_replayed THEN
    IF v_existing.page_digest_sha256 IS DISTINCT FROM v_page_digest
       OR v_existing.row_count<>v_row_count OR v_existing.canonical_byte_count<>v_page_bytes
       OR v_existing.next_after_ordinal IS DISTINCT FROM v_next_after OR v_existing.has_more IS DISTINCT FROM v_has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED' USING ERRCODE='55000';
    END IF;
  ELSE
    INSERT INTO private.banking_pay_workbench_settled_certificate_page_receipts_v8(
      certificate_uuid,request_scope_key,page_kind,pay_channel_scope,page_sequence,after_ordinal,
      requested_limit,expected_previous_receipt_sha256,request_preimage_digest_sha256,row_count,
      canonical_byte_count,next_after_ordinal,has_more,terminal_sentinel_present,page_digest_sha256
    ) VALUES (
      v_header.certificate_uuid,v_scope_key,v_page_kind,v_link.pay_channel_scope,v_page_sequence,p_after_ordinal,
      p_limit,p_expected_previous_receipt_sha256,v_request_digest,v_row_count,v_page_bytes,v_next_after,
      v_has_more,true,v_page_digest);
  END IF;
  RETURN v_response_core||pg_catalog.jsonb_build_object(
    'page_receipt',v_receipt_core,'page_receipt_digest_sha256',v_page_digest,'replayed',v_replayed);
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_filter_manifest_v8(
  p_operation_id uuid,
  p_certification_id text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_filter private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8%ROWTYPE;
  v_channel private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
BEGIN
  PERFORM pg_catalog.set_config('statement_timeout','6000',true);
  PERFORM pg_catalog.set_config('lock_timeout','1000',true);
  IF p_operation_id IS NULL OR COALESCE(p_certification_id,'') !~ '^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REFERENCE_INVALID' USING ERRCODE='22023';
  END IF;
  SELECT link.* INTO v_link FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  WHERE link.operation_id=p_operation_id FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_LINK_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  SELECT certificate.* INTO v_header FROM private.banking_pay_workbench_settled_certificates_v8 certificate
  WHERE certificate.certificate_uuid=v_link.certificate_uuid FOR SHARE;
  IF NOT FOUND OR v_link.certification_id IS DISTINCT FROM p_certification_id
     OR v_header.certification_id IS DISTINCT FROM p_certification_id
     OR v_link.overall_digest_sha256 IS DISTINCT FROM v_header.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH' USING ERRCODE='55000';
  END IF;
  IF v_header.lifecycle='REVOKED_CORRUPT_OR_SECURITY' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_REVOKED' USING ERRCODE='55000';
  END IF;
  IF v_link.link_state NOT IN ('ADMITTED','STAGING') OR v_header.lifecycle<>'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_CURRENT' USING ERRCODE='55000';
  END IF;
  SELECT session.* INTO v_session FROM public.banking_pay_workbench_sessions session
  WHERE session.id=v_header.workbench_session_id FOR SHARE;
  IF NOT FOUND OR v_session.version IS DISTINCT FROM v_header.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_header.progress_counter_version
     OR v_session.authority_fence_generation IS DISTINCT FROM v_header.authority_fence_generation
     OR UPPER(v_session.status)<>'OPEN' OR UPPER(v_session.progress_state)<>'READY'
     OR v_session.discarded_at_utc IS NOT NULL OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED' USING ERRCODE='55000';
  END IF;
  SELECT * INTO STRICT v_filter FROM private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid;
  SELECT * INTO STRICT v_channel FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_header.certificate_uuid AND manifest.pay_channel_scope=v_link.pay_channel_scope;
  IF v_link.candidate_filter_id IS DISTINCT FROM v_filter.candidate_filter_id
     OR v_link.client_filter_id IS DISTINCT FROM v_filter.client_filter_id
     OR v_link.filter_context_digest_sha256 IS DISTINCT FROM v_filter.filter_context_digest_sha256
     OR v_link.filter_scope_manifest_digest_sha256 IS DISTINCT FROM v_filter.manifest_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FILTER_MANIFEST_MISMATCH' USING ERRCODE='55000';
  END IF;
  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'operation_id',p_operation_id,'certificate_uuid',v_header.certificate_uuid,
    'certification_id',p_certification_id,
    'overall_digest_sha256',v_header.overall_digest_sha256,
    'workbench_session_id',v_header.workbench_session_id,'session_version',v_header.session_version,
    'progress_counter_version',v_header.progress_counter_version,
    'authority_fence_generation',v_header.authority_fence_generation,
    'pay_channel_scope',v_link.pay_channel_scope,
    'candidate_filter_id',v_filter.candidate_filter_id,'client_filter_id',v_filter.client_filter_id,
    'filter_binding_mode',v_filter.filter_binding_mode,
    'filter_context_digest_sha256',v_filter.filter_context_digest_sha256,
    'filter_scope_manifest_digest_sha256',v_filter.manifest_digest_sha256,
    'constituent_count',v_channel.constituent_count,'partition_count',v_channel.partition_count,
    'canonical_amount_ex_vat_total',v_channel.canonical_amount_ex_vat_total,
    'selected_constituents_digest_sha256',v_channel.selected_constituents_digest_sha256,
    'selected_partitions_digest_sha256',v_channel.selected_partitions_digest_sha256,
    'channel_manifest_digest_sha256',v_channel.manifest_digest_sha256,
    'lifecycle',v_header.lifecycle,'freeze_state',v_link.link_state
  );
END;
$function$;

CREATE OR REPLACE FUNCTION private.pay_workbench_settled_certificate_digest_checkpoint_apply_v8(
  p_digest_run_uuid uuid,
  p_checkpoint_sequence integer,
  p_state_after_json jsonb,
  p_page_receipt_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_run private.banking_pay_workbench_settled_certificate_digest_runs_v8%ROWTYPE;
  v_previous private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8%ROWTYPE;
  v_existing private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8%ROWTYPE;
BEGIN
  IF p_checkpoint_sequence < 0
     OR pg_catalog.jsonb_typeof(p_state_after_json) IS DISTINCT FROM 'object'
     OR COALESCE(p_page_receipt_sha256, '') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_CHECKPOINT_INVALID' USING ERRCODE = '22023';
  END IF;
  PERFORM private.pay_workbench_settled_certificate_sha256_update_v8(p_state_after_json, '');
  SELECT * INTO v_run
  FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8
  WHERE digest_run_uuid = p_digest_run_uuid
  FOR UPDATE;
  IF NOT FOUND OR v_run.status <> 'BUILDING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_RUN_NOT_BUILDING' USING ERRCODE = '55000';
  END IF;
  SELECT * INTO v_existing
  FROM private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8
  WHERE digest_run_uuid = p_digest_run_uuid
    AND checkpoint_sequence = p_checkpoint_sequence;
  IF FOUND THEN
    IF v_existing.state_after_json IS DISTINCT FROM p_state_after_json
       OR v_existing.page_receipt_sha256 IS DISTINCT FROM p_page_receipt_sha256 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_CHECKPOINT_CONFLICT' USING ERRCODE = '23505';
    END IF;
    RETURN pg_catalog.jsonb_build_object('ok', true, 'replayed', true, 'state_after', p_state_after_json);
  END IF;
  IF p_checkpoint_sequence = 0 THEN
    v_previous.state_after_json := private.pay_workbench_settled_certificate_sha256_init_v8();
  ELSE
    SELECT * INTO v_previous
    FROM private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8
    WHERE digest_run_uuid = p_digest_run_uuid
      AND checkpoint_sequence = p_checkpoint_sequence - 1;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_DIGEST_CHECKPOINT_GAP' USING ERRCODE = '55000';
    END IF;
  END IF;
  INSERT INTO private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8(
    digest_run_uuid, checkpoint_sequence, after_ordinal,
    stream_phase, partition_ordinal, nested_array_kind, member_ordinal,
    state_before_json, state_after_json, page_receipt_sha256
  ) VALUES (
    p_digest_run_uuid, p_checkpoint_sequence, v_run.next_ordinal,
    v_run.stream_phase, v_run.current_partition_ordinal, NULL, v_run.current_member_ordinal,
    v_previous.state_after_json, p_state_after_json, p_page_receipt_sha256
  );
  RETURN pg_catalog.jsonb_build_object('ok', true, 'replayed', false, 'state_after', p_state_after_json);
END;
$function$;

ALTER FUNCTION private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_sha256_init_v8() OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_sha256_update_bytes_v8(jsonb,bytea,boolean) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_sha256_update_v8(jsonb,text) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_sha256_final_v8(jsonb) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_sha256_text_v8(text) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_money_v8(jsonb) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_component_evidence_json_v8(uuid,integer) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_constituent_unsigned_v8(uuid,integer) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_constituent_json_v8(uuid,integer) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_canonical_stream_page_v8(uuid,text,text,integer,integer) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_overall_digest_advance_v8(uuid,integer) OWNER TO postgres;
ALTER FUNCTION private.workbench_settled_cert_same_week_override_validate_v8(jsonb) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_reference_issue_v8(text,text,text,jsonb) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_current_reference_issue_v8(uuid,bigint,bigint,text,text,jsonb) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_operation_admit_v8(uuid) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_entry_page_v8(uuid,text,integer,integer,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_partition_page_v8(uuid,text,integer,integer,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_component_page_v8(uuid,text,text,integer,integer,text) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_settled_certificate_filter_manifest_v8(uuid,text) OWNER TO postgres;
ALTER FUNCTION private.pay_workbench_settled_certificate_digest_checkpoint_apply_v8(uuid,integer,jsonb,text) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_sha256_init_v8() FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_sha256_update_bytes_v8(jsonb,bytea,boolean) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_sha256_update_v8(jsonb,text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_sha256_final_v8(jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_sha256_text_v8(text) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_money_v8(jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_component_evidence_json_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_constituent_unsigned_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_constituent_json_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_canonical_stream_page_v8(uuid,text,text,integer,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_overall_digest_advance_v8(uuid,integer) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION private.workbench_settled_cert_same_week_override_validate_v8(jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_reference_issue_v8(text,text,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_current_reference_issue_v8(uuid,bigint,bigint,text,text,jsonb) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_operation_admit_v8(uuid) FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_entry_page_v8(uuid,text,integer,integer,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_partition_page_v8(uuid,text,integer,integer,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_component_page_v8(uuid,text,text,integer,integer,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_filter_manifest_v8(uuid,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_settled_certificate_digest_checkpoint_apply_v8(uuid,integer,jsonb,text) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_sha256_init_v8() TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_sha256_update_bytes_v8(jsonb,bytea,boolean) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_sha256_update_v8(jsonb,text) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_sha256_final_v8(jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_sha256_text_v8(text) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_money_v8(jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_component_evidence_json_v8(uuid,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_constituent_unsigned_v8(uuid,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_constituent_json_v8(uuid,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_canonical_stream_page_v8(uuid,text,text,integer,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_overall_digest_advance_v8(uuid,integer) TO postgres;
GRANT EXECUTE ON FUNCTION private.workbench_settled_cert_same_week_override_validate_v8(jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_reference_issue_v8(text,text,text,jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_current_reference_issue_v8(uuid,bigint,bigint,text,text,jsonb) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_operation_admit_v8(uuid) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_entry_page_v8(uuid,text,integer,integer,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_partition_page_v8(uuid,text,integer,integer,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_component_page_v8(uuid,text,text,integer,integer,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_filter_manifest_v8(uuid,text) TO postgres, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_settled_certificate_digest_checkpoint_apply_v8(uuid,integer,jsonb,text) TO postgres;

NOTIFY pgrst, 'reload schema';
