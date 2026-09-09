import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptPath = fileURLToPath(import.meta.url);
const repositoryRoot = path.resolve(path.dirname(scriptPath), '..');
const sourcePath = path.join(
  repositoryRoot,
  'supabase',
  'repeatable',
  '04082026_1207_pay_payment_correction_request_start.sql'
);
const targetPath = path.join(
  repositoryRoot,
  'supabase',
  'repeatable',
  '08092026_0510_banking_pay_payment_correction_prepare_idempotency_v1.sql'
);

const expectedSourceSha256 = 'e8b6ace569f4d2f9e48b54ba7c88b90d2b4fac47a140d398483c2e1d83080ccb';
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');
const source = fs.readFileSync(sourcePath, 'utf8');
const sourceSha256 = sha256(Buffer.from(source, 'utf8'));

if (sourceSha256 !== expectedSourceSha256) {
  throw new Error(`Historical owner drift: expected ${expectedSourceSha256}, received ${sourceSha256}`);
}

const eol = source.includes('\r\n') ? '\r\n' : '\n';
const lines = (value) => value.replaceAll('\n', eol);
const replaceOnce = (value, anchor, replacement, label) => {
  const first = value.indexOf(anchor);
  if (first < 0 || value.indexOf(anchor, first + anchor.length) >= 0) {
    throw new Error(`${label} anchor must occur exactly once`);
  }
  return value.slice(0, first) + replacement + value.slice(first + anchor.length);
};

let output = source;
output = output.replace(
  '-- CloudTMS Banking Pay cancellation — Stage 1 replacement.',
  '-- CloudTMS Banking Pay cancellation — exact PREPARE/START_PREPARED lost-response replay replacement.'
);
output = output.replace(
  '-- Exact installed identity retained. PREPARE is non-gating; START_PREPARED consumes proof atomically.',
  lines(`-- Exact installed identity retained. Payment policy and every fresh-request gate remain unchanged.
-- Exact already-created requests are identified from immutable request selection plus the operation key;
-- mutable planning output is deliberately excluded from PREPARE replay identity.`)
);

const declarationAnchor = lines(`  v_q_bound_authority_set_digest text;
BEGIN`);
const declarationReplacement = lines(`  v_q_bound_authority_set_digest text;
  v_replay_idempotency_key text;
  v_replay_operation_count integer := 0;
  v_replay_request_count integer := 0;
  v_replay_link_count integer := 0;
  v_replay_operation public.banking_pay_operations%rowtype;
  v_replay_request public.pay_payment_correction_requests%rowtype;
  v_replay_expected_selection jsonb;
  v_replay_stored_descriptor_hash text;
  v_replay_initial_descriptor_count integer := 0;
BEGIN`);
output = replaceOnce(output, declarationAnchor, declarationReplacement, 'declaration');

const prepareAnchor = lines(`    ELSIF p_source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION')::text;
    END IF;

    SELECT batch_row.*`);

const earlyPrepareReplay = lines(`    ELSIF p_source_bank_event_id IS NULL THEN
      RAISE EXCEPTION 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'SOURCE_BANK_EVENT_REQUIRED_FOR_AUTO_CORRECTION')::text;
    END IF;

    -- An explicit operation key is the durable serialisation point for an
    -- exact PREPARE retry.  This runs before mutable Workbench/batch freshness
    -- checks, but it can only return an already-created request after proving
    -- the complete immutable original intent.  A different intent never uses
    -- this bypass and continues through every existing freshness gate below.
    v_replay_idempotency_key := NULLIF(
      pg_catalog.btrim(coalesce(p_selection_json->>'idempotency_key', '')),
      ''
    );

    IF v_replay_idempotency_key IS NOT NULL THEN
      PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
          'banking_pay_operation_start:PAYMENT_CORRECTION:' || v_replay_idempotency_key,
          0
        )
      );

      SELECT pg_catalog.count(*)::integer
      INTO v_replay_operation_count
      FROM public.banking_pay_operations AS keyed_operation
      WHERE keyed_operation.idempotency_key = v_replay_idempotency_key;

      IF v_replay_operation_count > 1 THEN
        RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'IDEMPOTENCY_CONFLICT',
            'reason', 'OPERATION_IDEMPOTENCY_KEY_AMBIGUOUS',
            'idempotency_key', v_replay_idempotency_key
          )::text;
      END IF;

      IF v_replay_operation_count = 0 THEN
        SELECT pg_catalog.count(*)::integer
        INTO v_replay_request_count
        FROM public.pay_payment_correction_requests AS orphan_request
        WHERE orphan_request.pay_batch_id = p_pay_batch_id
          AND NULLIF(pg_catalog.btrim(coalesce(orphan_request.selection_json->>'idempotency_key', '')), '')
              = v_replay_idempotency_key;

        IF v_replay_request_count > 0 THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'REQUEST_OPERATION_LINK_MISSING',
              'idempotency_key', v_replay_idempotency_key
            )::text;
        END IF;
      ELSE
        SELECT keyed_operation.*
        INTO v_replay_operation
        FROM public.banking_pay_operations AS keyed_operation
        WHERE keyed_operation.idempotency_key = v_replay_idempotency_key
        FOR UPDATE;

        BEGIN
          v_request_id := NULLIF(
            pg_catalog.btrim(coalesce(v_replay_operation.input_json->>'correction_request_id', '')),
            ''
          )::uuid;
        EXCEPTION WHEN invalid_text_representation THEN
          v_request_id := NULL::uuid;
        END;

        IF v_request_id IS NULL THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'OPERATION_REQUEST_LINK_INVALID',
              'operation_id', v_replay_operation.id
            )::text;
        END IF;

        SELECT request_row.*
        INTO v_replay_request
        FROM public.pay_payment_correction_requests AS request_row
        WHERE request_row.id = v_request_id;

        IF NOT FOUND THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'CORRECTION_REQUEST_MISSING',
              'operation_id', v_replay_operation.id
            )::text;
        END IF;

        SELECT pg_catalog.count(*)::integer
        INTO v_replay_link_count
        FROM public.banking_pay_operations AS linked_operation
        WHERE linked_operation.operation_type = 'PAYMENT_CORRECTION'
          AND linked_operation.input_json->>'correction_request_id' = v_request_id::text;

        IF v_replay_link_count <> 1 THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'REQUEST_OPERATION_LINK_AMBIGUOUS',
              'correction_request_id', v_request_id,
              'operation_link_count', v_replay_link_count
            )::text;
        END IF;

        v_mode := pg_catalog.upper(coalesce(
          p_selection_json#>>'{selection,mode}', p_selection_json->>'mode', ''
        ));
        v_action := pg_catalog.upper(coalesce(
          p_selection_json#>>'{selection,action}', p_selection_json->>'requested_action', p_selection_json->>'action', ''
        ));
        v_filter := coalesce(
          p_selection_json#>'{selection,filter_json}', p_selection_json->'filter_json',
          p_selection_json#>'{selection,filter}', p_selection_json->'filter', '{}'::jsonb
        );
        v_sort_key := pg_catalog.upper(coalesce(
          p_selection_json#>>'{selection,sort_key}', p_selection_json->>'sort_key', 'STATUS'
        ));
        v_sort_direction := pg_catalog.upper(coalesce(
          p_selection_json#>>'{selection,sort_direction}', p_selection_json->>'sort_direction', 'ASC'
        ));

        IF v_mode NOT IN ('EXPLICIT', 'ALL_MATCHING')
           OR v_action NOT IN ('DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT', 'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND')
           OR pg_catalog.jsonb_typeof(v_filter) <> 'object'
           OR v_sort_key NOT IN ('STATUS', 'CANDIDATE', 'AMOUNT')
           OR v_sort_direction NOT IN ('ASC', 'DESC') THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT', 'reason', 'REPLAY_DESCRIPTOR_INVALID'
            )::text;
        END IF;

        IF v_action = 'DRAFT_CANCEL'
           AND NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '')
               IS DISTINCT FROM 'DRAFT_PAYMENT_CANCELLED_BY_USER' THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT', 'reason', 'REPLAY_REASON_MISMATCH'
            )::text;
        ELSIF v_action <> 'DRAFT_CANCEL'
           AND coalesce(p_auto_requested, false) IS NOT TRUE
           AND NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '') IS NULL THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT', 'reason', 'REPLAY_REASON_MISMATCH'
            )::text;
        END IF;

        v_explicit_tokens := '[]'::jsonb;
        v_canonical_explicit_tokens := '[]'::jsonb;
        v_requested_explicit_count := 0;
        v_unique_explicit_count := 0;
        v_requested_explicit_hash := NULL::text;

        IF v_mode = 'EXPLICIT' THEN
          v_explicit_tokens := coalesce(
            p_selection_json#>'{selection,explicit_candidate_tokens}',
            p_selection_json->'explicit_candidate_tokens',
            p_selection_json->'pay_batch_candidate_ids',
            '[]'::jsonb
          );

          IF pg_catalog.jsonb_typeof(v_explicit_tokens) <> 'array'
             OR EXISTS (
               SELECT 1
               FROM pg_catalog.jsonb_array_elements(v_explicit_tokens) AS supplied_token(value)
               WHERE pg_catalog.jsonb_typeof(supplied_token.value) <> 'string'
             )
             OR EXISTS (
               SELECT 1
               FROM pg_catalog.jsonb_array_elements_text(v_explicit_tokens) AS supplied_token(token_value)
               WHERE supplied_token.token_value
                 !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             ) THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
              USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'IDEMPOTENCY_CONFLICT', 'reason', 'REPLAY_EXPLICIT_SELECTION_INVALID'
              )::text;
          END IF;

          SELECT pg_catalog.count(*)::integer,
                 pg_catalog.count(DISTINCT token_value)::integer,
                 coalesce(pg_catalog.jsonb_agg(token_value ORDER BY token_value), '[]'::jsonb)
          INTO v_requested_explicit_count, v_unique_explicit_count, v_canonical_explicit_tokens
          FROM (
            SELECT pg_catalog.lower(explicit_token.token_value) AS token_value
            FROM pg_catalog.jsonb_array_elements_text(v_explicit_tokens) AS explicit_token(token_value)
          ) AS canonical_tokens;

          IF v_requested_explicit_count <> pg_catalog.jsonb_array_length(v_explicit_tokens)
             OR v_requested_explicit_count < 1
             OR v_requested_explicit_count > v_max_candidates
             OR v_unique_explicit_count <> v_requested_explicit_count THEN
            RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
              USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'IDEMPOTENCY_CONFLICT', 'reason', 'REPLAY_EXPLICIT_SELECTION_INVALID'
              )::text;
          END IF;

          v_requested_explicit_hash := private.pay_payment_correction_sha256_v1(
            pg_catalog.jsonb_build_object(
              'version', 1, 'pay_batch_id', p_pay_batch_id, 'chain', 'EXPLICIT_IDS'
            )
          );
          FOR v_explicit_token IN
            SELECT token.value
            FROM pg_catalog.jsonb_array_elements_text(v_canonical_explicit_tokens) AS token(value)
            ORDER BY token.value
          LOOP
            v_requested_explicit_hash := private.pay_payment_correction_sha256_v1(
              pg_catalog.jsonb_build_object(
                'prior', v_requested_explicit_hash,
                'pay_batch_candidate_id', v_explicit_token
              )
            );
          END LOOP;
        END IF;

        v_active_scope_hash := NULLIF(v_replay_request.selection_json->>'scope_fence_hash', '');
        v_snapshot_token := coalesce(
          p_selection_json#>>'{selection,snapshot_token}',
          p_selection_json->>'snapshot_token',
          CASE WHEN v_action = 'DRAFT_CANCEL' OR coalesce(p_auto_requested, false)
               THEN v_replay_request.selection_json->>'snapshot_token' ELSE NULL END
        );
        v_correction_kind := CASE WHEN v_action IN ('NO_MONEY_RELEASE', 'NO_MONEY_UNWIND')
                                  THEN 'NO_MONEY_UNWIND' ELSE 'PRE_BANK_CANCEL' END;
        v_reason_hash := private.pay_payment_correction_sha256_v1(
          coalesce(pg_catalog.to_jsonb(NULLIF(pg_catalog.btrim(p_reason), '')), 'null'::jsonb)
        );
        v_evidence_hash := CASE WHEN p_source_bank_event_id IS NULL THEN NULL
          ELSE private.pay_payment_correction_sha256_v1(pg_catalog.to_jsonb(p_source_bank_event_id)) END;
        v_outcome_hash := CASE WHEN p_accepted_resolution_json IS NULL THEN NULL
          ELSE private.pay_payment_correction_sha256_v1(p_accepted_resolution_json) END;

        v_replay_expected_selection := p_selection_json || pg_catalog.jsonb_build_object(
          'contract_version', 1,
          'mode', v_mode,
          'requested_action', v_action,
          'filter_json', v_filter,
          'sort_key', v_sort_key,
          'sort_direction', v_sort_direction,
          'snapshot_token', v_snapshot_token,
          'scope_fence_hash', v_active_scope_hash,
          'requested_explicit_count', v_requested_explicit_count,
          'requested_explicit_hash', v_requested_explicit_hash,
          'draft_overlay_fast_pre_request_authorities',
            coalesce(v_replay_request.selection_json->'draft_overlay_fast_pre_request_authorities', '{}'::jsonb),
          'cancellation_reversion_pre_request_authorities_v2',
            coalesce(v_replay_request.selection_json->'cancellation_reversion_pre_request_authorities_v2', '{}'::jsonb),
          'cancellation_reversion_pre_request_authorities_v3',
            coalesce(v_replay_request.selection_json->'cancellation_reversion_pre_request_authorities_v3', '{}'::jsonb),
          'canonical_explicit_candidate_tokens', CASE
            WHEN v_mode = 'EXPLICIT' THEN v_canonical_explicit_tokens ELSE '[]'::jsonb END,
          'selection', coalesce(p_selection_json->'selection', '{}'::jsonb) || pg_catalog.jsonb_build_object(
            'mode', v_mode,
            'action', v_action,
            'filter_json', v_filter,
            'sort_key', v_sort_key,
            'sort_direction', v_sort_direction,
            'snapshot_token', v_snapshot_token,
            'scope_fence_hash', v_active_scope_hash,
            'explicit_candidate_tokens', CASE WHEN v_mode = 'EXPLICIT' THEN v_canonical_explicit_tokens ELSE '[]'::jsonb END,
            'requested_explicit_count', v_requested_explicit_count,
            'requested_explicit_hash', v_requested_explicit_hash
          )
        );
        v_descriptor_hash := private.pay_payment_correction_sha256_v1(
          v_replay_expected_selection - 'command' - 'draft_overlay_fast_pre_request_authorities'
            - 'cancellation_reversion_pre_request_authorities_v2'
            - 'cancellation_reversion_pre_request_authorities_v3'
        );
        v_replay_stored_descriptor_hash := private.pay_payment_correction_sha256_v1(
          v_replay_request.selection_json - 'command' - 'draft_overlay_fast_pre_request_authorities'
            - 'cancellation_reversion_pre_request_authorities_v2'
            - 'cancellation_reversion_pre_request_authorities_v3'
        );

        SELECT pg_catalog.count(*)::integer
        INTO v_replay_initial_descriptor_count
        FROM public.pay_payment_correction_actions AS request_action
        WHERE request_action.correction_request_id = v_request_id
          AND request_action.action = 'REQUEST'
          AND request_action.metadata_json->>'descriptor_hash' = v_replay_stored_descriptor_hash;

        IF pg_catalog.jsonb_typeof(v_replay_request.selection_json) <> 'object'
           OR v_active_scope_hash !~ '^[0-9a-f]{64}$'
           OR v_replay_expected_selection IS DISTINCT FROM v_replay_request.selection_json
           OR v_descriptor_hash IS DISTINCT FROM v_replay_stored_descriptor_hash
           OR v_replay_initial_descriptor_count <> 1
           OR NULLIF(pg_catalog.btrim(coalesce(v_replay_request.selection_json->>'idempotency_key', '')), '')
                IS DISTINCT FROM v_replay_idempotency_key
           OR v_replay_operation.operation_type IS DISTINCT FROM 'PAYMENT_CORRECTION'
           OR v_replay_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
           OR v_replay_request.pay_batch_id IS DISTINCT FROM p_pay_batch_id
           OR v_replay_operation.actor_user_id IS DISTINCT FROM p_actor_user_id
           OR v_replay_request.requested_by_user_id IS DISTINCT FROM
                (CASE WHEN coalesce(p_auto_requested, false) THEN NULL::uuid ELSE p_actor_user_id END)
           OR v_replay_request.auto_requested IS DISTINCT FROM coalesce(p_auto_requested, false)
           OR v_replay_request.source_bank_event_id IS DISTINCT FROM p_source_bank_event_id
           OR v_replay_request.correction_kind IS DISTINCT FROM v_correction_kind
           OR NULLIF(pg_catalog.btrim(coalesce(v_replay_request.reason, '')), '')
                IS DISTINCT FROM NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '')
           OR v_replay_request.accepted_resolution_json IS DISTINCT FROM p_accepted_resolution_json
           OR v_replay_request.accepted_resolution_hash IS DISTINCT FROM v_outcome_hash
           OR v_replay_operation.input_json->>'correction_request_id' IS DISTINCT FROM v_request_id::text
           OR v_replay_operation.input_json->>'requested_action' IS DISTINCT FROM v_action
           OR v_replay_operation.input_json->'auto_requested'
                IS DISTINCT FROM pg_catalog.to_jsonb(coalesce(p_auto_requested, false))
           OR v_replay_operation.input_json->>'source_bank_event_id'
                IS DISTINCT FROM (CASE WHEN p_source_bank_event_id IS NULL THEN NULL ELSE p_source_bank_event_id::text END) THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'IMMUTABLE_REQUEST_IDENTITY_MISMATCH',
              'correction_request_id', v_request_id,
              'operation_id', v_replay_operation.id,
              'idempotency_key', v_replay_idempotency_key
            )::text;
        END IF;

        -- The unique request/operation link is not enough on its own: the two
        -- rows must also describe one source-supported lifecycle point.  The
        -- operation runner may legitimately be RUNNING, waiting, failed or in
        -- review at the same phase, but it must never be at a phase belonging
        -- to a different request state.
        IF NOT (
          (
            v_replay_request.status = 'PLANNING'
            AND v_replay_operation.phase = 'PREPARE_SELECTION'
            AND v_replay_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_replay_request.status IN ('PLANNED','REQUESTED','AWAITING_AUTHORISATION')
            AND v_replay_operation.phase IN ('AWAITING_REAUTHENTICATION','WAITING_AUTHORISATION')
            AND v_replay_operation.status IN ('WAITING_AUTHORISATION','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_replay_request.status = 'AUTHORISED'
            AND v_replay_operation.phase = 'EXPAND_WORK'
            AND v_replay_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_replay_request.status = 'EXPANDED'
            AND v_replay_operation.phase = 'PROCESS_CHUNKS'
            AND v_replay_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_replay_request.status = 'PROCESSING'
            AND v_replay_operation.phase IN ('PROCESS_CHUNKS','FINALISE')
            AND v_replay_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_replay_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
            AND (
              (
                v_replay_operation.phase = 'REFRESH_WORKBENCH'
                AND v_replay_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
              )
              OR (
                v_replay_operation.phase = 'COMPLETE'
                AND v_replay_operation.status IN ('COMPLETE','FAILED','CANCELLED','REVIEW_REQUIRED')
              )
            )
          )
        ) THEN
          RAISE EXCEPTION 'PAYMENT_CORRECTION_IDEMPOTENCY_CONFLICT'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'IDEMPOTENCY_CONFLICT',
              'reason', 'REQUEST_OPERATION_LIFECYCLE_MISMATCH',
              'correction_request_id', v_request_id,
              'operation_id', v_replay_operation.id,
              'request_status', v_replay_request.status,
              'operation_status', v_replay_operation.status,
              'operation_phase', v_replay_operation.phase
            )::text;
        END IF;

        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'existing_request', true, 'is_existing', true,
          'correction_request_id', v_replay_request.id,
          'operation_id', v_replay_operation.id,
          'request_status', v_replay_request.status,
          'operation_status', v_replay_operation.status,
          'phase', v_replay_operation.phase,
          'selection_ready', v_replay_request.status <> 'PLANNING',
          'gate_active', v_replay_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING'),
          'approved_count', coalesce(v_replay_request.approved_count, 0),
          'required_quantity', greatest(coalesce(v_replay_request.required_quantity, 1), 1),
          'requires_reauthentication', v_replay_request.status = 'PLANNED' AND coalesce(v_replay_request.auto_requested, false) IS NOT TRUE,
          'requires_authorisation', v_replay_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
          'display_status', CASE WHEN v_replay_request.status = 'PLANNING' THEN 'Preparing payment selection' ELSE 'Cancellation request already exists' END,
          'display_message', 'CloudTMS returned the existing request for this exact idempotency contract.',
          'continuation', pg_catalog.jsonb_build_object(
            'required', v_replay_request.status = 'PLANNING',
            'operation_id', v_replay_operation.id,
            'operation_type', 'PAYMENT_CORRECTION',
            'pay_batch_id', v_replay_request.pay_batch_id,
            'root_operation_id', v_replay_operation.root_operation_id,
            'phase', v_replay_operation.phase,
            'run_after_utc', v_replay_operation.run_after_utc,
            'reason', 'PAYMENT_CORRECTION_PREPARE',
            'successor_relation', CASE WHEN v_replay_request.status = 'PLANNING' THEN 'SELF' ELSE 'NONE' END,
            'requires_user_action', v_replay_request.status <> 'PLANNING',
            'terminal', v_replay_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
          ),
          'code', 'PAYMENT_CORRECTION_REQUEST_EXISTING'
        );
      END IF;
    END IF;

    SELECT batch_row.*`);
output = replaceOnce(output, prepareAnchor, earlyPrepareReplay, 'early PREPARE replay');

const startReplayAnchor = lines(`  v_proof_hash := NULLIF(p_selection_json->>'proof_hash', '');
  v_resume_reauthenticated_request :=
    v_command = 'START_PREPARED'
    AND coalesce(v_request.auto_requested, false) IS NOT TRUE
    AND v_request.status IN ('REQUESTED', 'AWAITING_AUTHORISATION')
    AND v_request.requested_by_user_id IS NOT DISTINCT FROM p_actor_user_id
    AND greatest(coalesce(v_request.required_quantity, 1), 1) = 1
    AND coalesce(v_request.approved_count, 0) = 0
    AND v_request.reauth_consumed_at_utc IS NOT NULL
    AND v_request.reauth_proof_hash IS NOT DISTINCT FROM v_proof_hash
    AND coalesce(v_request.plan_json->>'requested_action', '') IN (
      'DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT',
      'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND'
    );`);
const startReplayReplacement = lines(`  v_proof_hash := NULLIF(p_selection_json->>'proof_hash', '');

  -- Preserve the historical one-authoriser resume branch exactly.  Those
  -- REQUESTED/AWAITING rows must continue through the existing AUTHORISE
  -- transition; only already-committed states use the read-only replay below.
  v_resume_reauthenticated_request :=
    v_command = 'START_PREPARED'
    AND coalesce(v_request.auto_requested, false) IS NOT TRUE
    AND v_request.status IN ('REQUESTED', 'AWAITING_AUTHORISATION')
    AND v_request.requested_by_user_id IS NOT DISTINCT FROM p_actor_user_id
    AND greatest(coalesce(v_request.required_quantity, 1), 1) = 1
    AND coalesce(v_request.approved_count, 0) = 0
    AND v_request.reauth_consumed_at_utc IS NOT NULL
    AND v_request.reauth_proof_hash IS NOT DISTINCT FROM v_proof_hash
    AND coalesce(v_request.plan_json->>'requested_action', '') IN (
      'DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT',
      'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND'
    );

  -- Returning an already-started request is a read-only lost-response replay.
  -- It is allowed before mutable batch/currentness gates only when the exact
  -- consumed proof, request fields and unique linked operation all agree.
  IF v_command = 'START_PREPARED'
     AND v_request.status IN (
       'REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING',
       'APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED'
     )
     AND coalesce(v_resume_reauthenticated_request, false) IS NOT TRUE THEN
    SELECT pg_catalog.count(*)::integer
    INTO v_replay_link_count
    FROM public.banking_pay_operations AS linked_operation
    WHERE linked_operation.operation_type = 'PAYMENT_CORRECTION'
      AND linked_operation.input_json->>'correction_request_id' = v_request_id::text;

    IF v_replay_link_count <> 1 THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_START_REPLAY_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'START_PREPARED_REPLAY_CONFLICT',
          'reason', 'REQUEST_OPERATION_LINK_AMBIGUOUS',
          'correction_request_id', v_request_id,
          'operation_link_count', v_replay_link_count
        )::text;
    END IF;

    SELECT linked_operation.*
    INTO v_operation
    FROM public.banking_pay_operations AS linked_operation
    WHERE linked_operation.operation_type = 'PAYMENT_CORRECTION'
      AND linked_operation.input_json->>'correction_request_id' = v_request_id::text;

    IF coalesce(v_request.auto_requested, false)
       OR v_request.requested_by_user_id IS DISTINCT FROM p_actor_user_id
       OR v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id
       OR v_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id
       OR v_operation.input_json->>'requested_action'
            IS DISTINCT FROM coalesce(v_request.plan_json->>'requested_action', v_request.selection_json->>'requested_action')
       OR v_operation.input_json->'auto_requested' IS DISTINCT FROM 'false'::jsonb
       OR v_operation.input_json->>'source_bank_event_id' IS NOT NULL
       OR v_request.source_bank_event_id IS DISTINCT FROM p_source_bank_event_id
       OR NULLIF(pg_catalog.btrim(coalesce(v_request.reason, '')), '')
            IS DISTINCT FROM NULLIF(pg_catalog.btrim(coalesce(p_reason, '')), '')
       OR v_request.accepted_resolution_json IS DISTINCT FROM p_accepted_resolution_json
       OR NULLIF(p_selection_json->>'selection_hash', '') IS DISTINCT FROM v_request.selection_hash
       OR NULLIF(p_selection_json->>'plan_hash', '') IS DISTINCT FROM v_request.plan_hash
       OR v_proof_hash !~ '^[0-9a-f]{64}$'
       OR v_request.reauth_proof_hash IS DISTINCT FROM v_proof_hash
       OR v_request.reauth_consumed_at_utc IS NULL
       OR v_request.reauth_expires_at_utc IS NULL
       OR v_request.reauth_consumed_at_utc > v_request.reauth_expires_at_utc THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_START_REPLAY_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'START_PREPARED_REPLAY_CONFLICT',
          'reason', 'EXACT_CONSUMED_PROOF_OR_REQUEST_MISMATCH',
          'correction_request_id', v_request_id
        )::text;
    END IF;

    IF NOT (
      (
        v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION')
        AND v_operation.phase IN ('AWAITING_REAUTHENTICATION','WAITING_AUTHORISATION')
        AND v_operation.status IN ('WAITING_AUTHORISATION','WAITING','FAILED','REVIEW_REQUIRED')
      )
      OR (
        v_request.status = 'AUTHORISED'
        AND v_operation.phase = 'EXPAND_WORK'
        AND v_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
      )
      OR (
        v_request.status = 'EXPANDED'
        AND v_operation.phase = 'PROCESS_CHUNKS'
        AND v_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
      )
      OR (
        v_request.status = 'PROCESSING'
        AND v_operation.phase IN ('PROCESS_CHUNKS','FINALISE')
        AND v_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
      )
      OR (
        v_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
        AND (
          (
            v_operation.phase = 'REFRESH_WORKBENCH'
            AND v_operation.status IN ('RUNNING','WAITING','FAILED','REVIEW_REQUIRED')
          )
          OR (
            v_operation.phase = 'COMPLETE'
            AND v_operation.status IN ('COMPLETE','FAILED','CANCELLED','REVIEW_REQUIRED')
          )
        )
      )
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_START_REPLAY_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'START_PREPARED_REPLAY_CONFLICT',
          'reason', 'REQUEST_OPERATION_LIFECYCLE_MISMATCH',
          'correction_request_id', v_request_id,
          'operation_id', v_operation.id,
          'request_status', v_request.status,
          'operation_status', v_operation.status,
          'operation_phase', v_operation.phase
        )::text;
    END IF;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'is_existing', true,
      'correction_request_id', v_request_id, 'operation_id', v_operation.id,
      'request_status', v_request.status, 'operation_status', v_operation.status,
      'phase', v_operation.phase,
      'selection_ready', true,
      'gate_active', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION','AUTHORISED','EXPANDED','PROCESSING'),
      'approved_count', coalesce(v_request.approved_count, 0),
      'required_quantity', greatest(coalesce(v_request.required_quantity, 1), 1),
      'requires_reauthentication', false,
      'requires_authorisation', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
      'display_status', 'Cancellation request already started',
      'display_message', 'CloudTMS returned the already-committed cancellation request.',
      'continuation', pg_catalog.jsonb_build_object(
        'required', v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING'),
        'operation_id', v_operation.id, 'operation_type', 'PAYMENT_CORRECTION',
        'pay_batch_id', v_request.pay_batch_id, 'root_operation_id', v_operation.root_operation_id,
        'phase', v_operation.phase, 'run_after_utc', v_operation.run_after_utc,
        'reason', 'PAYMENT_CORRECTION_ALREADY_STARTED',
        'successor_relation', CASE WHEN v_request.status IN ('AUTHORISED','EXPANDED','PROCESSING') THEN 'SELF' ELSE 'NONE' END,
        'requires_user_action', v_request.status IN ('REQUESTED','AWAITING_AUTHORISATION'),
        'terminal', v_request.status IN ('APPLIED','APPLIED_WITH_BLOCKERS','BLOCKED','FAILED','REJECTED','CANCELLED')
      ),
      'code', 'REQUEST_ALREADY_STARTED'
    );
  END IF;`);
output = replaceOnce(output, startReplayAnchor, startReplayReplacement, 'START_PREPARED replay');

if (!output.includes("'reason', 'IMMUTABLE_REQUEST_IDENTITY_MISMATCH'")) {
  throw new Error('Generated owner is missing immutable PREPARE replay guard');
}
if (!output.includes("'reason', 'EXACT_CONSUMED_PROOF_OR_REQUEST_MISMATCH'")) {
  throw new Error('Generated owner is missing consumed START_PREPARED replay guard');
}
if ((output.match(/CREATE OR REPLACE FUNCTION public\.pay_payment_correction_request_start\(/g) || []).length !== 1) {
  throw new Error('Generated owner must contain exactly one replacement function');
}

const checkOnly = process.argv.includes('--check');
if (checkOnly) {
  if (!fs.existsSync(targetPath)) throw new Error(`Generated owner is missing: ${targetPath}`);
  const existing = fs.readFileSync(targetPath, 'utf8');
  if (existing !== output) throw new Error('Generated owner is stale; rerun the generator');
  process.stdout.write(`PASS ${path.relative(repositoryRoot, targetPath)} ${sha256(Buffer.from(existing, 'utf8'))}\n`);
} else {
  fs.writeFileSync(targetPath, output, 'utf8');
  process.stdout.write(`WROTE ${path.relative(repositoryRoot, targetPath)} ${sha256(Buffer.from(output, 'utf8'))}\n`);
}
