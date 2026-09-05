import fs from 'node:fs';
import path from 'node:path';

const root = process.cwd();
const sourcePath = path.join(
  root,
  'supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql'
);
const targetPath = process.env.H2_ONE_CANDIDATE_SCOPE_OUTPUT
  ? path.resolve(process.env.H2_ONE_CANDIDATE_SCOPE_OUTPUT)
  : path.join(
      root,
      'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql'
    );

let source = fs.readFileSync(sourcePath, 'utf8').replace(/\r\n/g, '\n');

function replaceExact(label, before, after) {
  const occurrences = source.split(before).length - 1;
  if (occurrences !== 1) {
    throw new Error(`${label}: expected one source occurrence, found ${occurrences}`);
  }
  source = source.replace(before, after);
}

replaceExact(
  'explicit-selection flag declaration',
  '    v_candidate_audit_hash text;\n',
  '    v_candidate_audit_hash text;\n    v_candidate_explicitly_selected boolean := false;\n'
);

replaceExact(
  'complete bounded Candidate scan',
  `          AND (v_cursor_candidate_id IS NULL OR candidate_row.id > v_cursor_candidate_id)
          AND (v_mode = 'ALL_MATCHING' OR candidate_row.id::text IN (
            SELECT explicit_token.value
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
          ))
        ORDER BY candidate_row.id`,
  `          AND (v_cursor_candidate_id IS NULL OR candidate_row.id > v_cursor_candidate_id)
        -- Both sides of the unchanged-unselected integrity proof must page the
        -- same complete frozen batch universe.  EXPLICIT still selects only
        -- the requested identities below; this scan merely preserves the
        -- untouched Candidate fingerprints that FINALISE independently reads.
        ORDER BY candidate_row.id`
);

replaceExact(
  'selected-only cancellation-reversion proof input',
  `            WHERE candidate_row.pay_batch_id=v_batch.id
              AND candidate_row.id::text IN (
                SELECT scan_token.value
                FROM pg_catalog.jsonb_array_elements_text(v_scan_candidate_tokens) AS scan_token(value)
              )`,
  `            WHERE candidate_row.pay_batch_id=v_batch.id
              AND candidate_row.id::text IN (
                SELECT scan_token.value
                FROM pg_catalog.jsonb_array_elements_text(v_scan_candidate_tokens) AS scan_token(value)
              )
              AND (
                v_mode <> 'EXPLICIT'
                OR candidate_row.id::text IN (
                  SELECT explicit_token.value
                  FROM pg_catalog.jsonb_array_elements_text(
                    COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
                  ) AS explicit_token(value)
                )
              )`
);

replaceExact(
  'explicit unselected Candidate audit branch',
  `        v_amount := pg_catalog.round(
            GREATEST(COALESCE(v_candidate.net_bank_amount, 0), 0),
            2
        )::numeric(14,2);

        IF v_item_count < 1 AND v_mode = 'EXPLICIT' THEN`,
  `        v_amount := pg_catalog.round(
            GREATEST(COALESCE(v_candidate.net_bank_amount, 0), 0),
            2
        )::numeric(14,2);

        v_candidate_explicitly_selected :=
          v_mode <> 'EXPLICIT'
          OR EXISTS (
            SELECT 1
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
            WHERE explicit_token.value = v_candidate.pay_batch_candidate_id::text
          );

        IF v_mode = 'EXPLICIT' AND NOT v_candidate_explicitly_selected THEN
          -- This is the exact audit shape recomputed by FINALISE.  It proves
          -- the unselected Candidate stayed unchanged without applying that
          -- Candidate's eligibility, correction or financial mutation path.
          v_candidate_audit_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
              'version', 1,
              'pay_batch_candidate_id', v_candidate.pay_batch_candidate_id,
              'candidate_id', v_candidate.candidate_id,
              'net_bank_amount_pence', pg_catalog.round(COALESCE(v_candidate.net_bank_amount, 0) * 100)::bigint,
              'settlement_status', v_candidate.settlement_status,
              'item_state', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                  item_row.id, item_row.item_type, COALESCE(item_row.is_voided, false),
                  pg_catalog.round(COALESCE(item_row.amount_ex_vat, 0) * 100)::bigint,
                  pg_catalog.round(COALESCE(item_row.amount_vat, 0) * 100)::bigint,
                  pg_catalog.round(COALESCE(item_row.amount_inc_vat, 0) * 100)::bigint,
                  item_row.reservation_id, item_row.finance_component_id,
                  item_row.pay_bank_transfer_id, item_row.operation_source_key
                ) ORDER BY item_row.id)
                FROM public.pay_batch_items AS item_row
                WHERE item_row.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
              ), '[]'::jsonb),
              'reservation_state', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                  reservation_row.id, reservation_row.pay_batch_item_id,
                  reservation_row.status, reservation_row.committed_at_utc,
                  reservation_row.settled_at_utc, reservation_row.released_at_utc
                ) ORDER BY reservation_row.id)
                FROM public.pay_advance_reservations AS reservation_row
                JOIN public.pay_batch_items AS reservation_item
                  ON reservation_item.id = reservation_row.pay_batch_item_id
                WHERE reservation_item.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
                  AND COALESCE(reservation_item.is_voided, false) IS NOT TRUE
              ), '[]'::jsonb),
              'transfer_state', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
                  transfer_row.id, transfer_row.status, transfer_row.rail_state,
                  transfer_row.request_id, transfer_row.rail_tx_id,
                  transfer_row.transfer_group_key
                ) ORDER BY transfer_row.id)
                FROM public.pay_bank_transfers AS transfer_row
                WHERE transfer_row.id IN (
                  SELECT transfer_item.pay_bank_transfer_id
                  FROM public.pay_batch_items AS transfer_item
                  WHERE transfer_item.pay_batch_candidate_id = v_candidate.pay_batch_candidate_id
                    AND transfer_item.pay_bank_transfer_id IS NOT NULL
                    AND COALESCE(transfer_item.is_voided, false) IS NOT TRUE
                )
              ), '[]'::jsonb)
            )
          );
          v_unselected_chain_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
              'prior', v_unselected_chain_hash,
              'candidate_hash', v_candidate_audit_hash
            )
          );
          CONTINUE;
        END IF;

        IF v_item_count < 1 AND v_mode = 'EXPLICIT' THEN`
);

replaceExact(
  'complete-universe continuation',
  `    IF v_mode = 'ALL_MATCHING' THEN
        SELECT EXISTS (
            SELECT 1
            FROM public.pay_batch_candidates AS remaining_candidate
            WHERE remaining_candidate.pay_batch_id = v_batch.id
              AND (
                  v_last_candidate_id IS NULL
                  OR remaining_candidate.id > v_last_candidate_id
              )
        )
        INTO v_has_more;
    ELSE
      SELECT EXISTS (
        SELECT 1 FROM public.pay_batch_candidates AS remaining_candidate
        WHERE remaining_candidate.pay_batch_id = v_batch.id
          AND (v_scan_last_candidate_id IS NULL OR remaining_candidate.id > v_scan_last_candidate_id)
          AND remaining_candidate.id::text IN (
            SELECT explicit_token.value
            FROM pg_catalog.jsonb_array_elements_text(
              COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)
            ) AS explicit_token(value)
          )
      ) INTO v_has_more;
    END IF;`,
  `    SELECT EXISTS (
      SELECT 1
      FROM public.pay_batch_candidates AS remaining_candidate
      WHERE remaining_candidate.pay_batch_id = v_batch.id
        AND (
          v_scan_last_candidate_id IS NULL
          OR remaining_candidate.id > v_scan_last_candidate_id
        )
    )
    INTO v_has_more;`
);

const header = [
  '-- CloudTMS Banking Pay cancellation — exact unselected-scope integrity.',
  '-- Generated from the complete current selection-prepare owner by',
  '-- scripts/generate-banking-pay-one-candidate-cancellation-scope-integrity-v1.mjs.',
  '-- Policy/economics are unchanged: this only gives EXPLICIT preparation and',
  '-- FINALISE the same complete bounded frozen Candidate universe to fingerprint.',
  ''
].join('\n');

fs.writeFileSync(targetPath, header + source, 'utf8');
console.log(targetPath);
