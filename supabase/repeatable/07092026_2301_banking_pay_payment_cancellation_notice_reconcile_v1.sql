-- Best-effort payment cancellation notice reconciliation.
--
-- This owner may only append one idempotent mail_outbox notice after an exact
-- provider-accepted original payment email and a completed communication-V2
-- cancellation prove the same bounded item scope.  It never changes financial,
-- cancellation, remittance, settlement or provider state.

\set ON_ERROR_STOP on

begin;

create or replace function public.pay_payment_cancellation_notice_reconcile_v1(
  p_correction_request_id uuid default null,
  p_original_mail_outbox_id uuid default null,
  p_after_created_at_utc timestamptz default null,
  p_after_mail_outbox_id uuid default null,
  p_limit integer default 50,
  p_template_version text default 'PAYMENT_CANCELLATION_NOTICE_V1'
)
returns jsonb
language plpgsql
volatile
parallel unsafe
security definer
set search_path to ''
set statement_timeout to '6000ms'
set lock_timeout to '1000ms'
as $function$
declare
  v_template_version constant text := 'PAYMENT_CANCELLATION_NOTICE_V1';
  v_mode text;
  v_bound_request public.pay_payment_correction_requests%rowtype;
  v_request public.pay_payment_correction_requests%rowtype;
  v_original public.mail_outbox%rowtype;
  v_scope jsonb;
  v_item_json jsonb;
  v_item_ids uuid[];
  v_pay_batch_candidate_ids uuid[];
  v_candidate_id uuid;
  v_message_shape text;
  v_match_ids uuid[];
  v_source_ids uuid[] := array[]::uuid[];
  v_notice_key text;
  v_inserted_id uuid;
  v_existing_notice public.mail_outbox%rowtype;
  v_notice_scope jsonb;
  v_notice_subject text;
  v_notice_body_html constant text := '<p>The payment described in our earlier email has been cancelled and will not be made as previously advised.</p><p>If a replacement payment is arranged, you will receive a separate notification.</p>';
  v_notice_body_text constant text := 'The payment described in our earlier email has been cancelled and will not be made as previously advised.' || pg_catalog.chr(10) || pg_catalog.chr(10) || 'If a replacement payment is arranged, you will receive a separate notification.';
  v_examined integer := 0;
  v_eligible integer := 0;
  v_queued integer := 0;
  v_already_present integer := 0;
  v_skipped integer := 0;
  v_has_more boolean := false;
  v_last_created_at_utc timestamptz;
  v_last_mail_outbox_id uuid;
  v_reason text;
  v_reason_counts jsonb := '{}'::jsonb;
  v_reason_total bigint := 0;
  v_effective_request_bound boolean := false;
  v_recovery_event_kind text;
  v_recovery_event_id uuid;
  v_recovery_selected_mail_id uuid;
  v_recovery_claim_contended boolean := false;
  v_source_page_more boolean := false;
  v_request_progress_after_created_at_utc timestamptz;
  v_request_progress_after_mail_outbox_id uuid;
  v_request_event_had_error boolean := false;
  v_event_had_error boolean := false;
  v_event_result_code text;
  v_before_examined integer;
  v_before_eligible integer;
  v_before_queued integer;
  v_before_already_present integer;
  v_before_skipped integer;
  v_before_reason_counts jsonb;
  v_request_result_json jsonb;
  v_cumulative_reason_counts jsonb := '{}'::jsonb;
  v_prior_examined integer := 0;
  v_prior_eligible integer := 0;
  v_prior_queued integer := 0;
  v_prior_already_present integer := 0;
  v_prior_skipped integer := 0;
  v_saved_progress_invalid boolean := false;
  v_allowed_reason_codes constant text[] := array[
    'CORRECTION_ITEM_COVERAGE_AMBIGUOUS',
    'CORRECTION_ITEM_COVERAGE_INCOMPLETE',
    'CORRECTION_ITEM_COVERAGE_NOT_FOUND',
    'CORRECTION_SOURCE_NOT_FOUND',
    'EXISTING_NOTICE_IDENTITY_CONFLICT',
    'EXISTING_NOTICE_AWAITS_MANUAL_RETRY',
    'EXISTING_NOTICE_STATE_CONFLICT',
    'ORIGINAL_CANDIDATE_INVALID',
    'ORIGINAL_CONTEXT_INVALID',
    'ORIGINAL_ITEM_COUNT_MISMATCH',
    'ORIGINAL_ITEM_SCOPE_DUPLICATE',
    'ORIGINAL_ITEM_SCOPE_INVALID',
    'ORIGINAL_ITEM_SCOPE_SIZE_INVALID',
    'ORIGINAL_MESSAGE_SHAPE_UNSUPPORTED',
    'ORIGINAL_NOT_PROVIDER_ACCEPTED',
    'ORIGINAL_PAY_BATCH_CANDIDATE_SCOPE_INVALID',
    'ORIGINAL_RECIPIENT_IDENTITY_MISMATCH',
    'UNEXPECTED_EVENT_ERROR'
  ]::text[];
begin
  -- Every mode/boundary error is rejected before the first possible write.
  if p_correction_request_id is not null
     and p_original_mail_outbox_id is not null then
    raise exception using errcode = '22023',
      message = 'PAYMENT_CANCELLATION_NOTICE_MODE_CONFLICT';
  end if;

  if (p_after_created_at_utc is null) <> (p_after_mail_outbox_id is null) then
    raise exception using errcode = '22023',
      message = 'PAYMENT_CANCELLATION_NOTICE_CURSOR_PAIR_REQUIRED';
  end if;

  -- Recovery progress belongs to locked database rows.  An external cursor
  -- could bypass a still-pending row, so every caller-supplied cursor is
  -- rejected even though the legacy-compatible signature remains stable.
  if p_after_created_at_utc is not null then
    raise exception using errcode = '22023',
      message = 'PAYMENT_CANCELLATION_NOTICE_EXTERNAL_CURSOR_PROHIBITED';
  end if;

  if p_limit is null or p_limit < 1 or p_limit > 100 then
    raise exception using errcode = '22023',
      message = 'PAYMENT_CANCELLATION_NOTICE_LIMIT_INVALID';
  end if;

  if p_template_version is distinct from v_template_version then
    raise exception using errcode = '22023',
      message = 'PAYMENT_CANCELLATION_NOTICE_TEMPLATE_UNSUPPORTED';
  end if;

  v_mode := case
    when p_correction_request_id is not null then 'CORRECTION'
    when p_original_mail_outbox_id is not null then 'ORIGINAL'
    else 'RECOVERY'
  end;

  if v_mode = 'CORRECTION' then
    select request_row.*
      into v_bound_request
    from public.pay_payment_correction_requests as request_row
    where request_row.id = p_correction_request_id
    for update;

    if not found
       or v_bound_request.cancel_notice_tracked is not true
       or v_bound_request.status not in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
       or v_bound_request.applied_at_utc is null
       or v_bound_request.correction_kind not in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
       or v_bound_request.plan_json->>'candidate_scope_contract_version' is distinct from '2'
       or v_bound_request.plan_json->>'candidate_scope_hash_version' is distinct from '2'
       or v_bound_request.plan_json->>'source_row_count_semantics' is distinct from 'FINANCIAL_ONLY'
       or v_bound_request.plan_json->>'communication_cleanup_contract_version' is distinct from '2' then
      return pg_catalog.jsonb_build_object(
        'ok', true,
        'mode', v_mode,
        'template_version', v_template_version,
        'examined', 1,
        'eligible', 0,
        'queued', 0,
        'already_present', 0,
        'skipped', 1,
        'reason_counts', pg_catalog.jsonb_build_object('CORRECTION_NOT_ELIGIBLE', 1),
        'has_more', false,
        'next_cursor', null,
        'progress_owner', 'SERVER_ROW',
        'recovery_claim_contended', false
      );
    end if;

    -- An exact nudge/lost-reply retry after this applied event was completely
    -- reconciled is terminal idempotency.  Never reopen page one: doing so on a
    -- request with more than p_limit source mails could create stranded progress
    -- while the completed event token remains excluded from recovery.
    if v_bound_request.cancel_notice_reconciled_applied_at_utc
         is not distinct from v_bound_request.applied_at_utc then
      if v_bound_request.cancel_notice_progress_applied_at_utc is not null
         or v_bound_request.cancel_notice_after_created_at_utc is not null
         or v_bound_request.cancel_notice_after_mail_outbox_id is not null
         or v_bound_request.cancel_notice_next_attempt_at_utc is not null then
        raise exception using
          errcode = '55000',
          message = 'PAYMENT_CANCELLATION_NOTICE_TERMINAL_PROGRESS_CONFLICT';
      end if;

      v_request_result_json := coalesce(
        v_bound_request.cancel_notice_result_json,
        '{}'::jsonb
      );
      v_saved_progress_invalid :=
        pg_catalog.jsonb_typeof(v_request_result_json) is distinct from 'object'
        or pg_catalog.pg_column_size(v_request_result_json) > 16384
        or (
          select pg_catalog.count(*) > 20
          from pg_catalog.jsonb_object_keys(
            case
              when pg_catalog.jsonb_typeof(v_request_result_json) = 'object'
                then v_request_result_json
              else '{}'::jsonb
            end
          ) as result_key(key)
        )
        or not (v_request_result_json ?& array[
          'template_version', 'reconciliation_authority',
          'event_applied_at_utc', 'examined', 'eligible', 'queued',
          'already_present', 'skipped', 'reason_counts', 'complete',
          'result_code'
        ])
        or v_request_result_json->>'template_version'
             is distinct from v_template_version
        or v_request_result_json->>'reconciliation_authority'
             is distinct from 'ASYNCHRONOUS_SENT_ONLY'
        or v_request_result_json->'event_applied_at_utc'
             is distinct from pg_catalog.to_jsonb(
               pg_catalog.to_char(
                 pg_catalog.timezone('UTC', v_bound_request.applied_at_utc),
                 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               )
             )
        or v_request_result_json->'complete' is distinct from 'true'::jsonb
        or v_request_result_json->>'result_code' not in (
             'EVENT_COMPLETE', 'NO_SENT_SOURCE_AT_RECONCILIATION'
           )
        or pg_catalog.jsonb_typeof(v_request_result_json->'reason_counts')
             is distinct from 'object';

      if not v_saved_progress_invalid then
        select exists (
                 select 1
                 from (
                   values ('examined'), ('eligible'), ('queued'),
                          ('already_present'), ('skipped')
                 ) as counter_key(key)
                 where pg_catalog.jsonb_typeof(
                         v_request_result_json->counter_key.key
                       ) is distinct from 'number'
                    or v_request_result_json->>counter_key.key
                         !~ '^(0|[1-9][0-9]{0,8})$'
                    or (v_request_result_json->>counter_key.key)::bigint > 50000
               )
            or exists (
                 select 1
                 from pg_catalog.jsonb_each(
                   v_request_result_json->'reason_counts'
                 ) as reason_entry(reason_code, reason_value)
                 where reason_entry.reason_code <> all(v_allowed_reason_codes)
                    or pg_catalog.jsonb_typeof(reason_entry.reason_value)
                         is distinct from 'number'
                    or reason_entry.reason_value #>> '{}'
                         !~ '^(0|[1-9][0-9]{0,8})$'
                    or (reason_entry.reason_value #>> '{}')::bigint > 50000
               )
          into v_saved_progress_invalid;
      end if;

      if not v_saved_progress_invalid then
        v_prior_examined := (v_request_result_json->>'examined')::integer;
        v_prior_eligible := (v_request_result_json->>'eligible')::integer;
        v_prior_queued := (v_request_result_json->>'queued')::integer;
        v_prior_already_present :=
          (v_request_result_json->>'already_present')::integer;
        v_prior_skipped := (v_request_result_json->>'skipped')::integer;
        select coalesce(pg_catalog.sum(reason_entry.value::bigint), 0::bigint)
          into v_reason_total
        from pg_catalog.jsonb_each_text(
          v_request_result_json->'reason_counts'
        ) as reason_entry(key, value);
        v_saved_progress_invalid :=
          v_prior_examined <> v_prior_eligible + v_prior_skipped
          or v_prior_eligible <>
               v_prior_queued + v_prior_already_present
          or v_reason_total <> v_prior_skipped;
      end if;

      if v_saved_progress_invalid then
        return pg_catalog.jsonb_build_object(
          'ok', true,
          'mode', v_mode,
          'template_version', v_template_version,
          'examined', 1,
          'eligible', 0,
          'queued', 0,
          'already_present', 0,
          'skipped', 1,
          'reason_counts', pg_catalog.jsonb_build_object(
            'SAVED_PROGRESS_INVALID', 1
          ),
          'has_more', false,
          'next_cursor', null,
          'progress_owner', 'SERVER_ROW',
          'recovery_claim_contended', false,
          'already_complete', true
        );
      end if;

      return pg_catalog.jsonb_build_object(
        'ok', true,
        'mode', v_mode,
        'template_version', v_template_version,
        'examined', 0,
        'eligible', 0,
        'queued', 0,
        'already_present', 0,
        'skipped', 0,
        'reason_counts', '{}'::jsonb,
        'has_more', false,
        'next_cursor', null,
        'progress_owner', 'SERVER_ROW',
        'recovery_claim_contended', false,
        'already_complete', true
      );
    end if;
    v_effective_request_bound := true;
  elsif v_mode = 'ORIGINAL' then
    v_source_ids := array[p_original_mail_outbox_id];
  else
    -- Pick exactly one oldest due event lane.  Historical rows are absent from
    -- both partial indexes because their cutover marker is NULL.
    select event_row.event_kind,
           event_row.event_id
      into v_recovery_event_kind,
           v_recovery_event_id
    from (
      select mail_event.*
      from (
        select 'MAIL'::text as event_kind,
               mail_row.id as event_id,
               coalesce(
                 mail_row.cancel_notice_next_attempt_at_utc,
                 mail_row.sent_at
               ) as event_order_utc
        from public.mail_outbox as mail_row
        where mail_row.cancel_notice_tracked is true
          and mail_row.type = 'REMITTANCE'
          and mail_row.status = 'SENT'::public.mail_status_enum
          and mail_row.sent_at is not null
          and mail_row.provider_status = 'ACCEPTED'
          and mail_row.cancel_notice_reconciled_sent_at_utc
                is distinct from mail_row.sent_at
          and (
            mail_row.cancel_notice_next_attempt_at_utc is null
            or mail_row.cancel_notice_next_attempt_at_utc
                 <= pg_catalog.statement_timestamp()
          )
        order by coalesce(
                   mail_row.cancel_notice_next_attempt_at_utc,
                   mail_row.sent_at
                 ),
                 mail_row.sent_at,
                 mail_row.id
        limit 1
      ) as mail_event
      union all
      select request_event.*
      from (
        select 'CORRECTION'::text as event_kind,
               request_row.id as event_id,
               coalesce(
                 request_row.cancel_notice_next_attempt_at_utc,
                 request_row.applied_at_utc
               ) as event_order_utc
        from public.pay_payment_correction_requests as request_row
        where request_row.cancel_notice_tracked is true
          and request_row.status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
          and request_row.applied_at_utc is not null
          and request_row.cancel_notice_reconciled_applied_at_utc
                is distinct from request_row.applied_at_utc
          and request_row.correction_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
          and request_row.plan_json->>'candidate_scope_contract_version' = '2'
          and request_row.plan_json->>'candidate_scope_hash_version' = '2'
          and request_row.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
          and request_row.plan_json->>'communication_cleanup_contract_version' = '2'
          and (
            request_row.cancel_notice_next_attempt_at_utc is null
            or request_row.cancel_notice_next_attempt_at_utc
                 <= pg_catalog.statement_timestamp()
          )
        order by coalesce(
                   request_row.cancel_notice_next_attempt_at_utc,
                   request_row.applied_at_utc
                 ),
                 request_row.applied_at_utc,
                 request_row.id
        limit 1
      ) as request_event
    ) as event_row
    order by event_row.event_order_utc,
             event_row.event_id,
             event_row.event_kind
    limit 1;

    if v_recovery_event_kind = 'CORRECTION' then
      select request_row.*
        into v_bound_request
      from public.pay_payment_correction_requests as request_row
      where request_row.id = v_recovery_event_id
        and request_row.cancel_notice_tracked is true
        and request_row.status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
        and request_row.applied_at_utc is not null
        and request_row.cancel_notice_reconciled_applied_at_utc
              is distinct from request_row.applied_at_utc
        and request_row.correction_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
        and request_row.plan_json->>'candidate_scope_contract_version' = '2'
        and request_row.plan_json->>'candidate_scope_hash_version' = '2'
        and request_row.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
        and request_row.plan_json->>'communication_cleanup_contract_version' = '2'
        and (
          request_row.cancel_notice_next_attempt_at_utc is null
          or request_row.cancel_notice_next_attempt_at_utc <= pg_catalog.statement_timestamp()
        )
      for update skip locked;
      if found then
        v_effective_request_bound := true;
      else
        select exists (
                 select 1
                 from public.pay_payment_correction_requests as request_row
                 where request_row.id = v_recovery_event_id
                   and request_row.cancel_notice_tracked is true
                   and request_row.status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
                   and request_row.applied_at_utc is not null
                   and request_row.cancel_notice_reconciled_applied_at_utc
                         is distinct from request_row.applied_at_utc
                   and request_row.correction_kind in (
                     'PRE_BANK_CANCEL', 'NO_MONEY_UNWIND'
                   )
                   and request_row.plan_json->>'candidate_scope_contract_version' = '2'
                   and request_row.plan_json->>'candidate_scope_hash_version' = '2'
                   and request_row.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
                   and request_row.plan_json->>'communication_cleanup_contract_version' = '2'
                   and (
                     request_row.cancel_notice_next_attempt_at_utc is null
                     or request_row.cancel_notice_next_attempt_at_utc <=
                          pg_catalog.statement_timestamp()
                   )
               )
          into v_recovery_claim_contended;

        -- The candidate seen by the non-locking two-lane comparison may have
        -- been claimed between selection and this lock.  Skip it and claim the
        -- next indexed due request instead; a busy oldest row must never make
        -- later independent work look terminal.
        select request_row.*
          into v_bound_request
        from public.pay_payment_correction_requests as request_row
        where request_row.cancel_notice_tracked is true
          and request_row.status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
          and request_row.applied_at_utc is not null
          and request_row.cancel_notice_reconciled_applied_at_utc
                is distinct from request_row.applied_at_utc
          and request_row.correction_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
          and request_row.plan_json->>'candidate_scope_contract_version' = '2'
          and request_row.plan_json->>'candidate_scope_hash_version' = '2'
          and request_row.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
          and request_row.plan_json->>'communication_cleanup_contract_version' = '2'
          and (
            request_row.cancel_notice_next_attempt_at_utc is null
            or request_row.cancel_notice_next_attempt_at_utc <= pg_catalog.statement_timestamp()
          )
        order by coalesce(
                   request_row.cancel_notice_next_attempt_at_utc,
                   request_row.applied_at_utc
                 ),
                 request_row.applied_at_utc,
                 request_row.id
        for update skip locked
        limit 1;

        if found then
          v_effective_request_bound := true;
          v_recovery_event_id := v_bound_request.id;
        else
          -- A due mail event can still advance while the only correction row is
          -- busy.  The mail claim below also uses SKIP LOCKED over the full due
          -- lane, so two recovery callers cannot lose or double-consume work.
          v_recovery_event_kind := 'MAIL';
        end if;
      end if;
    end if;

    if v_recovery_event_kind = 'MAIL' and not v_effective_request_bound then
      -- Capture the current oldest due mail without a lock.  The subsequent
      -- SKIP LOCKED claim may legitimately advance later mail, but if this
      -- exact row remains due and was not claimed the response must expose
      -- contention rather than falsely report a terminal recovery lane.
      select source_mail.id
        into v_recovery_selected_mail_id
      from public.mail_outbox as source_mail
      where source_mail.cancel_notice_tracked is true
        and source_mail.type = 'REMITTANCE'
        and source_mail.status = 'SENT'::public.mail_status_enum
        and source_mail.sent_at is not null
        and source_mail.provider_status = 'ACCEPTED'
        and source_mail.cancel_notice_reconciled_sent_at_utc
              is distinct from source_mail.sent_at
        and (
          source_mail.cancel_notice_next_attempt_at_utc is null
          or source_mail.cancel_notice_next_attempt_at_utc
               <= pg_catalog.statement_timestamp()
        )
      order by
        coalesce(
          source_mail.cancel_notice_next_attempt_at_utc,
          source_mail.sent_at
        ),
        source_mail.sent_at,
        source_mail.id
      limit 1;

      -- PostgreSQL forbids row locking in a SELECT that also contains window
      -- functions.  Claim the bounded physical rows first, then calculate the
      -- deterministic ordinal and cumulative item weight from that locked set.
      with claimed_source as materialized (
        select source_mail.id,
               source_mail.created_at_utc,
               source_mail.sent_at,
               source_mail.cancel_notice_next_attempt_at_utc,
               source_mail.payment_scope_json
        from public.mail_outbox as source_mail
        where source_mail.cancel_notice_tracked is true
          and source_mail.type = 'REMITTANCE'
          and source_mail.status = 'SENT'::public.mail_status_enum
          and source_mail.sent_at is not null
          and source_mail.provider_status = 'ACCEPTED'
          and source_mail.cancel_notice_reconciled_sent_at_utc
                is distinct from source_mail.sent_at
          and (
            source_mail.cancel_notice_next_attempt_at_utc is null
            or source_mail.cancel_notice_next_attempt_at_utc
                 <= pg_catalog.statement_timestamp()
          )
        order by
          coalesce(
            source_mail.cancel_notice_next_attempt_at_utc,
            source_mail.sent_at
          ),
          source_mail.sent_at,
          source_mail.id
        for update skip locked
        limit p_limit + 1
      ), locked_source as materialized (
        select claimed_source.id,
               claimed_source.created_at_utc,
               case
                 when pg_catalog.jsonb_typeof(
                   claimed_source.payment_scope_json->'pay_batch_item_ids'
                 ) = 'array' then
                   greatest(
                     1,
                     least(
                       pg_catalog.jsonb_array_length(
                         claimed_source.payment_scope_json->'pay_batch_item_ids'
                       ),
                       50001
                     )
                   )
                 when pg_catalog.jsonb_typeof(
                   claimed_source.payment_scope_json->'items'
                 ) = 'array' then
                   greatest(
                     1,
                     least(
                       pg_catalog.jsonb_array_length(
                         claimed_source.payment_scope_json->'items'
                       ),
                       50001
                     )
                   )
                 else 1
               end as item_weight,
               pg_catalog.row_number() over (
                 order by
                   coalesce(
                     claimed_source.cancel_notice_next_attempt_at_utc,
                     claimed_source.sent_at
                   ),
                   claimed_source.sent_at,
                   claimed_source.id
               ) as source_ordinal
        from claimed_source
      ), weighted_source as (
        select locked_source.*,
               pg_catalog.sum(locked_source.item_weight) over (
                 order by locked_source.source_ordinal
               ) as running_item_weight
        from locked_source
      )
      select coalesce(
               pg_catalog.array_agg(
                 weighted_source.id
                 order by weighted_source.source_ordinal
               ) filter (
                  where weighted_source.source_ordinal = 1
                     or (
                      weighted_source.source_ordinal <= p_limit
                      and weighted_source.running_item_weight <= 50000
                    )
               ),
               array[]::uuid[]
             ),
             coalesce(
               pg_catalog.bool_or(
                 not (
                    weighted_source.source_ordinal = 1
                    or (
                      weighted_source.source_ordinal <= p_limit
                     and weighted_source.running_item_weight <= 50000
                   )
                 )
               ),
               false
             )
        into v_source_ids,
             v_source_page_more
      from weighted_source;

      if v_recovery_selected_mail_id is not null
         and not (
           v_recovery_selected_mail_id = any(
             coalesce(v_source_ids, array[]::uuid[])
           )
         ) then
        select v_recovery_claim_contended or exists (
                 select 1
                 from public.mail_outbox as source_mail
                 where source_mail.id = v_recovery_selected_mail_id
                   and source_mail.cancel_notice_tracked is true
                   and source_mail.type = 'REMITTANCE'
                   and source_mail.status = 'SENT'::public.mail_status_enum
                   and source_mail.sent_at is not null
                   and source_mail.provider_status = 'ACCEPTED'
                   and source_mail.cancel_notice_reconciled_sent_at_utc
                         is distinct from source_mail.sent_at
                   and (
                     source_mail.cancel_notice_next_attempt_at_utc is null
                     or source_mail.cancel_notice_next_attempt_at_utc
                          <= pg_catalog.statement_timestamp()
                   )
               )
          into v_recovery_claim_contended;
      end if;
      v_source_page_more :=
        v_source_page_more or v_recovery_claim_contended;
    elsif not v_effective_request_bound then
      v_recovery_event_kind := null;
    end if;
  end if;

  if v_effective_request_bound then
    if v_bound_request.cancel_notice_progress_applied_at_utc
         is not distinct from v_bound_request.applied_at_utc
       and (
         (v_bound_request.cancel_notice_after_created_at_utc is null)
         <> (v_bound_request.cancel_notice_after_mail_outbox_id is null)
       ) then
      update public.pay_payment_correction_requests as request_row
         set cancel_notice_reconciled_applied_at_utc =
               v_bound_request.applied_at_utc,
             cancel_notice_progress_applied_at_utc = null,
             cancel_notice_after_created_at_utc = null,
             cancel_notice_after_mail_outbox_id = null,
             cancel_notice_next_attempt_at_utc = null,
             cancel_notice_result_json = pg_catalog.jsonb_build_object(
               'template_version', v_template_version,
               'event_applied_at_utc', pg_catalog.to_char(
                 pg_catalog.timezone('UTC', v_bound_request.applied_at_utc),
                 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ),
               'examined', 1,
               'eligible', 0,
               'queued', 0,
               'already_present', 0,
               'skipped', 1,
               'reason_counts', pg_catalog.jsonb_build_object(
                 'REQUEST_PROGRESS_CURSOR_CORRUPT', 1
               ),
               'complete', true,
               'result_code', 'SAVED_PROGRESS_INVALID'
             )
       where request_row.id = v_bound_request.id
         and request_row.applied_at_utc is not distinct from
             v_bound_request.applied_at_utc;

      return pg_catalog.jsonb_build_object(
        'ok', true,
        'mode', v_mode,
        'template_version', v_template_version,
        'examined', 1,
        'eligible', 0,
        'queued', 0,
        'already_present', 0,
        'skipped', 1,
        'reason_counts', pg_catalog.jsonb_build_object(
          'REQUEST_PROGRESS_CURSOR_CORRUPT', 1
        ),
        'has_more', v_recovery_claim_contended,
        'next_cursor', null,
        'progress_owner', 'SERVER_ROW',
        'recovery_claim_contended', v_recovery_claim_contended
      );
    end if;

    if v_bound_request.cancel_notice_progress_applied_at_utc
         is not distinct from v_bound_request.applied_at_utc
       and (
         (v_bound_request.cancel_notice_after_created_at_utc is null)
         = (v_bound_request.cancel_notice_after_mail_outbox_id is null)
       ) then
      v_request_progress_after_created_at_utc :=
        v_bound_request.cancel_notice_after_created_at_utc;
      v_request_progress_after_mail_outbox_id :=
        v_bound_request.cancel_notice_after_mail_outbox_id;
      v_request_result_json := coalesce(
        v_bound_request.cancel_notice_result_json,
        '{}'::jsonb
      );
    else
      v_request_progress_after_created_at_utc := null;
      v_request_progress_after_mail_outbox_id := null;
      v_request_result_json := '{}'::jsonb;
    end if;

    select pg_catalog.jsonb_typeof(v_request_result_json) is distinct from 'object'
        or pg_catalog.pg_column_size(v_request_result_json) > 16384
        or (
          select pg_catalog.count(*) > 20
          from pg_catalog.jsonb_object_keys(
            case
              when pg_catalog.jsonb_typeof(v_request_result_json) = 'object'
                then v_request_result_json
              else '{}'::jsonb
            end
          ) as result_key(key)
        )
        or (
          v_bound_request.cancel_notice_progress_applied_at_utc
            is not distinct from v_bound_request.applied_at_utc
          and (
            not (v_request_result_json ?& array[
            'template_version',
            'reconciliation_authority',
            'event_applied_at_utc',
            'examined',
            'eligible',
            'queued',
            'already_present',
            'skipped',
            'reason_counts',
            'complete',
            'last_source_created_at_utc',
            'last_source_mail_outbox_id',
            'result_code'
            ])
            or v_request_result_json->>'template_version'
                 is distinct from v_template_version
            or v_request_result_json->>'reconciliation_authority'
                 is distinct from 'ASYNCHRONOUS_SENT_ONLY'
            or v_request_result_json->'event_applied_at_utc'
                 is distinct from pg_catalog.to_jsonb(
                   pg_catalog.to_char(
                     pg_catalog.timezone('UTC', v_bound_request.applied_at_utc),
                     'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                   )
                 )
            or v_request_result_json->'complete' is distinct from 'false'::jsonb
            or v_request_result_json->'last_source_created_at_utc'
                 is distinct from coalesce(
                    pg_catalog.to_jsonb(
                      pg_catalog.to_char(
                        pg_catalog.timezone(
                          'UTC', v_request_progress_after_created_at_utc
                        ),
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                      )
                    ),
                   'null'::jsonb
                 )
            or v_request_result_json->'last_source_mail_outbox_id'
                 is distinct from coalesce(
                   pg_catalog.to_jsonb(v_request_progress_after_mail_outbox_id),
                   'null'::jsonb
                 )
            or (
              v_request_progress_after_created_at_utc is not null
              and v_request_result_json->>'result_code' not in (
                'SOURCE_PAGE_CONTINUES', 'UNEXPECTED_EVENT_ERROR_RETRY'
              )
            )
            or (
              v_request_progress_after_created_at_utc is null
              and v_request_result_json->>'result_code'
                    is distinct from 'UNEXPECTED_EVENT_ERROR_RETRY'
            )
          )
        )
        or exists (
          select 1
          from (
            values
              ('examined'),
              ('eligible'),
              ('queued'),
              ('already_present'),
              ('skipped')
          ) as counter_key(key)
          where v_request_result_json ? counter_key.key
            and case
              when pg_catalog.jsonb_typeof(
                     v_request_result_json->counter_key.key
                   ) is distinct from 'number'
                then true
              when v_request_result_json->>counter_key.key
                     !~ '^(0|[1-9][0-9]{0,8})$'
                then true
              else (v_request_result_json->>counter_key.key)::bigint > 50000
            end
        )
        or (
          v_request_result_json ? 'reason_counts'
          and pg_catalog.jsonb_typeof(
            v_request_result_json->'reason_counts'
          ) is distinct from 'object'
        )
        or exists (
          select 1
          from pg_catalog.jsonb_each(
            case
              when pg_catalog.jsonb_typeof(
                v_request_result_json->'reason_counts'
              ) = 'object'
                then v_request_result_json->'reason_counts'
              else '{}'::jsonb
            end
          ) as reason_entry(reason_code, reason_value)
          where reason_entry.reason_code <> all(v_allowed_reason_codes)
             or case
                  when pg_catalog.jsonb_typeof(reason_entry.reason_value)
                         is distinct from 'number'
                    then true
                  when reason_entry.reason_value #>> '{}'
                         !~ '^(0|[1-9][0-9]{0,8})$'
                    then true
                  else (reason_entry.reason_value #>> '{}')::bigint > 50000
                end
        )
      into v_saved_progress_invalid;

    if not v_saved_progress_invalid then
      v_prior_examined := coalesce(
        nullif(v_request_result_json->>'examined', '')::integer,
        0
      );
      v_prior_eligible := coalesce(
        nullif(v_request_result_json->>'eligible', '')::integer,
        0
      );
      v_prior_queued := coalesce(
        nullif(v_request_result_json->>'queued', '')::integer,
        0
      );
      v_prior_already_present := coalesce(
        nullif(v_request_result_json->>'already_present', '')::integer,
        0
      );
      v_prior_skipped := coalesce(
        nullif(v_request_result_json->>'skipped', '')::integer,
        0
      );
      select coalesce(
               pg_catalog.sum(reason_entry.value::bigint),
               0::bigint
             )
        into v_reason_total
      from pg_catalog.jsonb_each_text(
        coalesce(v_request_result_json->'reason_counts', '{}'::jsonb)
      ) as reason_entry(key, value);
      v_saved_progress_invalid :=
        v_prior_examined <> v_prior_eligible + v_prior_skipped
        or v_prior_eligible <>
             v_prior_queued + v_prior_already_present
        or v_reason_total <> v_prior_skipped;
    end if;

    if v_saved_progress_invalid then
      update public.pay_payment_correction_requests as request_row
         set cancel_notice_reconciled_applied_at_utc =
               v_bound_request.applied_at_utc,
             cancel_notice_progress_applied_at_utc = null,
             cancel_notice_after_created_at_utc = null,
             cancel_notice_after_mail_outbox_id = null,
             cancel_notice_next_attempt_at_utc = null,
             cancel_notice_result_json = pg_catalog.jsonb_build_object(
               'template_version', v_template_version,
               'event_applied_at_utc', pg_catalog.to_char(
                 pg_catalog.timezone('UTC', v_bound_request.applied_at_utc),
                 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ),
               'examined', 1,
               'eligible', 0,
               'queued', 0,
               'already_present', 0,
               'skipped', 1,
               'reason_counts', pg_catalog.jsonb_build_object(
                 'SAVED_PROGRESS_INVALID', 1
               ),
               'complete', true,
               'result_code', 'SAVED_PROGRESS_INVALID'
             )
       where request_row.id = v_bound_request.id
         and request_row.applied_at_utc is not distinct from
             v_bound_request.applied_at_utc;

      return pg_catalog.jsonb_build_object(
        'ok', true,
        'mode', v_mode,
        'template_version', v_template_version,
        'examined', 1,
        'eligible', 0,
        'queued', 0,
        'already_present', 0,
        'skipped', 1,
        'reason_counts', pg_catalog.jsonb_build_object(
          'SAVED_PROGRESS_INVALID', 1
        ),
        'has_more', v_recovery_claim_contended,
        'next_cursor', null,
        'progress_owner', 'SERVER_ROW',
        'recovery_claim_contended', v_recovery_claim_contended
      );
    end if;

    -- Page raw provider-accepted source messages before any item-array
    -- expansion.  The cumulative item weight prevents several individually
    -- bounded messages from becoming one unbounded call.
    with request_scope as materialized (
      -- Resolve the exact Candidate/PBC keys from this one request through the
      -- request index before touching mail.  The expression mail index can then
      -- seek those keys directly even when the batch contains thousands of
      -- unrelated Candidate messages.
      select distinct correction_item.candidate_id::text as candidate_id,
                      correction_item.pay_batch_candidate_id::text
                        as pay_batch_candidate_id
      from public.pay_payment_correction_items as correction_item
      where correction_item.correction_request_id = v_bound_request.id
        and correction_item.pay_batch_id = v_bound_request.pay_batch_id
        and correction_item.candidate_id is not null
        and correction_item.pay_batch_candidate_id is not null
        and correction_item.correction_item_kind = v_bound_request.correction_kind
        and correction_item.status = 'APPLIED'
        and correction_item.applied_at_utc is not null
      order by correction_item.candidate_id::text,
               correction_item.pay_batch_candidate_id::text
      limit 50001
    ), candidate_source_per_scope as materialized (
      -- Keep the exact Candidate/PBC predicates inside a bounded lateral seek.
      -- This prevents the planner from choosing the broader batch/time index
      -- and filtering thousands of other Candidates before finding the target.
      select exact_source.id,
             exact_source.created_at_utc,
             exact_source.payment_scope_json
      from request_scope
      cross join lateral (
        select source_mail.id,
               source_mail.created_at_utc,
               source_mail.payment_scope_json
        from public.mail_outbox as source_mail
        where source_mail.context_id = v_bound_request.pay_batch_id
          and source_mail.payment_scope_json->>'candidate_id' =
              request_scope.candidate_id
          and source_mail.payment_scope_json->>'pay_batch_candidate_id' =
              request_scope.pay_batch_candidate_id
          and source_mail.context_kind = 'pay_batches'
          and source_mail.type = 'REMITTANCE'
          and source_mail.status = 'SENT'::public.mail_status_enum
          and source_mail.sent_at is not null
          and source_mail.provider_status = 'ACCEPTED'
          and (
            v_request_progress_after_created_at_utc is null
            or (source_mail.created_at_utc, source_mail.id) >
               (
                 v_request_progress_after_created_at_utc,
                 v_request_progress_after_mail_outbox_id
               )
          )
        order by source_mail.created_at_utc, source_mail.id
        limit p_limit + 1
      ) as exact_source
    ), candidate_source as materialized (
      select candidate_source_per_scope.id,
             candidate_source_per_scope.created_at_utc,
             candidate_source_per_scope.payment_scope_json
      from candidate_source_per_scope
      order by candidate_source_per_scope.created_at_utc,
               candidate_source_per_scope.id
      limit p_limit + 1
    ), bounded_source as materialized (
      select source_mail.id,
             source_mail.created_at_utc,
             case
               when pg_catalog.jsonb_typeof(
                 source_mail.payment_scope_json->'pay_batch_item_ids'
               ) = 'array' then
                 greatest(
                   1,
                   least(
                     pg_catalog.jsonb_array_length(
                       source_mail.payment_scope_json->'pay_batch_item_ids'
                     ),
                     50001
                   )
                 )
               when pg_catalog.jsonb_typeof(
                 source_mail.payment_scope_json->'items'
               ) = 'array' then
                 greatest(
                   1,
                   least(
                     pg_catalog.jsonb_array_length(
                       source_mail.payment_scope_json->'items'
                     ),
                     50001
                   )
                 )
               else 1
             end as item_weight,
             pg_catalog.row_number() over (
               order by source_mail.created_at_utc, source_mail.id
             ) as source_ordinal
      from candidate_source as source_mail
      order by source_mail.created_at_utc, source_mail.id
    ), weighted_source as (
      select bounded_source.*,
             pg_catalog.sum(bounded_source.item_weight) over (
               order by bounded_source.source_ordinal
             ) as running_item_weight
      from bounded_source
    )
    select coalesce(
             pg_catalog.array_agg(
               weighted_source.id
               order by weighted_source.source_ordinal
             ) filter (
               where weighted_source.source_ordinal = 1
                  or (
                    weighted_source.source_ordinal <= p_limit
                    and weighted_source.running_item_weight <= 50000
                  )
             ),
             array[]::uuid[]
           ),
           coalesce(
             pg_catalog.bool_or(
               not (
                 weighted_source.source_ordinal = 1
                 or (
                    weighted_source.source_ordinal <= p_limit
                   and weighted_source.running_item_weight <= 50000
                 )
               )
             ),
             false
           )
      into v_source_ids,
           v_source_page_more
    from weighted_source;
  end if;

  for v_original in
    select source_mail.*
    from public.mail_outbox as source_mail
    where source_mail.id = any(v_source_ids)
      and (v_mode <> 'ORIGINAL' or source_mail.cancel_notice_tracked is true)
    order by source_mail.created_at_utc, source_mail.id
  loop
    if v_examined >= p_limit then
      v_has_more := true;
      exit;
    end if;

    v_before_examined := v_examined;
    v_before_eligible := v_eligible;
    v_before_queued := v_queued;
    v_before_already_present := v_already_present;
    v_before_skipped := v_skipped;
    v_before_reason_counts := v_reason_counts;
    v_event_had_error := false;
    v_event_result_code := null;

    -- The inner block is a savepoint.  A malformed/poison event can therefore
    -- be recorded and delayed without rolling back or blocking cancellation,
    -- while any notice write made by the failed event is undone atomically.
    begin
      v_examined := v_examined + 1;
      v_last_created_at_utc := v_original.created_at_utc;
      v_last_mail_outbox_id := v_original.id;
      v_reason := null;
      v_scope := coalesce(v_original.payment_scope_json, '{}'::jsonb);
      v_item_json := null;
      v_item_ids := null;
      v_pay_batch_candidate_ids := null;
      v_candidate_id := null;
      v_message_shape := null;
      v_request := null;
      v_match_ids := array[]::uuid[];

    if v_original.type is distinct from 'REMITTANCE'
       or v_original.status::text is distinct from 'SENT'
       or v_original.sent_at is null
       or v_original.provider_status is distinct from 'ACCEPTED' then
      v_reason := 'ORIGINAL_NOT_PROVIDER_ACCEPTED';
    elsif v_original.context_kind is distinct from 'pay_batches'
       or v_original.context_id is null
       or pg_catalog.jsonb_typeof(v_scope) is distinct from 'object'
       or v_scope->>'pay_batch_id' is distinct from v_original.context_id::text then
      v_reason := 'ORIGINAL_CONTEXT_INVALID';
    elsif v_scope->>'remittance_type' = 'CANDIDATE_REMITTANCE'
       and v_original.recipient_kind = 'candidate' then
      v_message_shape := 'CANDIDATE_REMITTANCE';
      v_item_json := v_scope->'pay_batch_item_ids';
    elsif v_scope->>'remittance_type' = 'UMBRELLA_REMITTANCE'
       and v_original.recipient_kind = 'umbrella' then
      v_message_shape := 'UMBRELLA_REMITTANCE';
      v_item_json := v_scope->'pay_batch_item_ids';
    elsif v_scope->>'message_kind' = 'PAYOUT_NOTICE'
       and v_scope->>'notice_scope' = 'PAYOUT_NOTICE_CANDIDATE'
       and v_original.recipient_kind = 'candidate'
       and coalesce(v_original.reference, '') like 'payout_notice:%' then
      v_message_shape := 'PAYOUT_NOTICE';
      v_item_json := v_scope->'items';
    else
      v_reason := 'ORIGINAL_MESSAGE_SHAPE_UNSUPPORTED';
    end if;

    if v_reason is null then
      if coalesce(v_scope->>'candidate_id', '')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
        v_reason := 'ORIGINAL_CANDIDATE_INVALID';
      else
        v_candidate_id := (v_scope->>'candidate_id')::uuid;
      end if;
    end if;

    if v_reason is null then
      if v_message_shape in ('CANDIDATE_REMITTANCE', 'PAYOUT_NOTICE')
         and v_original.recipient_id is distinct from v_candidate_id then
        v_reason := 'ORIGINAL_RECIPIENT_IDENTITY_MISMATCH';
      elsif v_message_shape = 'UMBRELLA_REMITTANCE'
        and (
          coalesce(v_scope->>'recipient_id', '')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or v_original.recipient_id::text is distinct from v_scope->>'recipient_id'
        ) then
        v_reason := 'ORIGINAL_RECIPIENT_IDENTITY_MISMATCH';
      end if;
    end if;

    if v_reason is null then
      -- All three current producers own one singular top-level batch-Candidate
      -- identity.  PAYOUT_NOTICE has plural identities only in unrelated
      -- queue_context shapes, never in its durable payment_scope_json.
      if v_message_shape in (
        'CANDIDATE_REMITTANCE', 'UMBRELLA_REMITTANCE', 'PAYOUT_NOTICE'
      ) then
        if coalesce(v_scope->>'pay_batch_candidate_id', '')
             !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          v_reason := 'ORIGINAL_PAY_BATCH_CANDIDATE_SCOPE_INVALID';
        else
          v_pay_batch_candidate_ids := array[(v_scope->>'pay_batch_candidate_id')::uuid];
        end if;
      else
        v_reason := 'ORIGINAL_PAY_BATCH_CANDIDATE_SCOPE_INVALID';
      end if;
    end if;

    if v_reason is null then
      if pg_catalog.jsonb_typeof(v_item_json) is distinct from 'array'
         or pg_catalog.jsonb_array_length(v_item_json) < 1
         or pg_catalog.jsonb_array_length(v_item_json) > 50000 then
        v_reason := 'ORIGINAL_ITEM_SCOPE_SIZE_INVALID';
      elsif v_message_shape in ('CANDIDATE_REMITTANCE', 'UMBRELLA_REMITTANCE')
        and exists (
          select 1
          from pg_catalog.jsonb_array_elements(v_item_json) as item_value(value)
          where pg_catalog.jsonb_typeof(item_value.value) is distinct from 'string'
             or (item_value.value #>> '{}')
                !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) then
        v_reason := 'ORIGINAL_ITEM_SCOPE_INVALID';
      elsif v_message_shape = 'PAYOUT_NOTICE'
        and exists (
          select 1
          from pg_catalog.jsonb_array_elements(v_item_json) as item_value(value)
          where pg_catalog.jsonb_typeof(item_value.value) is distinct from 'object'
             or coalesce(item_value.value->>'pay_batch_item_id', '')
                !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) then
        v_reason := 'ORIGINAL_ITEM_SCOPE_INVALID';
      elsif v_message_shape in ('CANDIDATE_REMITTANCE', 'UMBRELLA_REMITTANCE')
        and (
          pg_catalog.jsonb_typeof(v_scope->'item_count') is distinct from 'number'
          or coalesce(v_scope->>'item_count', '') !~ '^[1-9][0-9]*$'
          or v_scope->>'item_count'
             is distinct from pg_catalog.jsonb_array_length(v_item_json)::text
        ) then
        v_reason := 'ORIGINAL_ITEM_COUNT_MISMATCH';
      elsif v_message_shape = 'PAYOUT_NOTICE'
        and v_scope ? 'item_count'
        and (
          pg_catalog.jsonb_typeof(v_scope->'item_count') is distinct from 'number'
          or coalesce(v_scope->>'item_count', '') !~ '^[1-9][0-9]*$'
          or v_scope->>'item_count'
             is distinct from pg_catalog.jsonb_array_length(v_item_json)::text
        ) then
        v_reason := 'ORIGINAL_ITEM_COUNT_MISMATCH';
      else
        select pg_catalog.array_agg(item_id order by item_id)
          into v_item_ids
        from (
          select case
            when v_message_shape = 'PAYOUT_NOTICE'
              then (item_value.value->>'pay_batch_item_id')::uuid
            else (item_value.value #>> '{}')::uuid
          end as item_id
          from pg_catalog.jsonb_array_elements(v_item_json) as item_value(value)
        ) as parsed_items;

        if pg_catalog.cardinality(v_item_ids)
             <> (select pg_catalog.count(distinct item_id) from pg_catalog.unnest(v_item_ids) as distinct_item(item_id)) then
          v_reason := 'ORIGINAL_ITEM_SCOPE_DUPLICATE';
        end if;
      end if;
    end if;

    if v_reason is null then
      if v_effective_request_bound then
        v_request := v_bound_request;
        if v_request.pay_batch_id is distinct from v_original.context_id
           or exists (
             select 1
             from pg_catalog.unnest(v_item_ids) as required_item(pay_batch_item_id)
             where (
               select pg_catalog.count(*)
               from public.pay_payment_correction_items as correction_item
               where correction_item.correction_request_id = v_request.id
                 and correction_item.pay_batch_id = v_request.pay_batch_id
                 and correction_item.candidate_id = v_candidate_id
                 and correction_item.pay_batch_candidate_id = any(v_pay_batch_candidate_ids)
                 and correction_item.pay_batch_item_id = required_item.pay_batch_item_id
                 and correction_item.correction_item_kind = v_request.correction_kind
                 and correction_item.status = 'APPLIED'
                 and correction_item.applied_at_utc is not null
             ) <> 1
           )
           or exists (
             select 1
             from pg_catalog.unnest(v_pay_batch_candidate_ids) as required_candidate(pay_batch_candidate_id)
             where not exists (
               select 1
               from public.pay_payment_correction_items as correction_item
               where correction_item.correction_request_id = v_request.id
                 and correction_item.pay_batch_id = v_request.pay_batch_id
                 and correction_item.candidate_id = v_candidate_id
                 and correction_item.pay_batch_candidate_id = required_candidate.pay_batch_candidate_id
                 and correction_item.pay_batch_item_id = any(v_item_ids)
                 and correction_item.correction_item_kind = v_request.correction_kind
                 and correction_item.status = 'APPLIED'
                 and correction_item.applied_at_utc is not null
             )
           )
           then
          v_reason := 'CORRECTION_ITEM_COVERAGE_INCOMPLETE';
        end if;
      else
        with candidate_seed as materialized (
          -- The existing unique APPLIED item/kind index bounds this seed to at
          -- most the two accepted correction kinds.  MAIL recovery therefore
          -- never searches request history for the batch.
          select correction_item.correction_request_id,
                 correction_item.correction_item_kind
          from public.pay_payment_correction_items as correction_item
          where correction_item.pay_batch_item_id = v_item_ids[1]
            and correction_item.correction_item_kind in (
              'PRE_BANK_CANCEL', 'NO_MONEY_UNWIND'
            )
            and correction_item.status = 'APPLIED'
            and correction_item.applied_at_utc is not null
            and correction_item.pay_batch_id = v_original.context_id
            and correction_item.candidate_id = v_candidate_id
            and correction_item.pay_batch_candidate_id = any(
              v_pay_batch_candidate_ids
            )
          order by correction_item.correction_request_id,
                   correction_item.correction_item_kind
          limit 3
        )
        select coalesce(
                 pg_catalog.array_agg(
                   candidate_request.id order by candidate_request.id
                 ),
                 array[]::uuid[]
               )
          into v_match_ids
        from (
          select request_row.id
          from candidate_seed
          join public.pay_payment_correction_requests as request_row
            on request_row.id = candidate_seed.correction_request_id
           and request_row.correction_kind =
               candidate_seed.correction_item_kind
          where request_row.pay_batch_id = v_original.context_id
            -- MAIL mode may resolve the exact pre-cutover APPLIED request for a
            -- payment mail that was unsent/retryable at cutover and became SENT
            -- afterward.  The unique applied-item seed above is the authority;
            -- this does not admit that request to request-driven recovery.
            and request_row.status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
            and request_row.applied_at_utc is not null
            and request_row.correction_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
            and request_row.plan_json->>'candidate_scope_contract_version' = '2'
            and request_row.plan_json->>'candidate_scope_hash_version' = '2'
            and request_row.plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
            and request_row.plan_json->>'communication_cleanup_contract_version' = '2'
            and not exists (
              select 1
              from pg_catalog.unnest(v_item_ids) as required_item(pay_batch_item_id)
              where (
                select pg_catalog.count(*)
                from public.pay_payment_correction_items as correction_item
                where correction_item.correction_request_id = request_row.id
                  and correction_item.pay_batch_id = request_row.pay_batch_id
                  and correction_item.candidate_id = v_candidate_id
                  and correction_item.pay_batch_candidate_id = any(v_pay_batch_candidate_ids)
                  and correction_item.pay_batch_item_id = required_item.pay_batch_item_id
                  and correction_item.correction_item_kind = request_row.correction_kind
                  and correction_item.status = 'APPLIED'
                  and correction_item.applied_at_utc is not null
              ) <> 1
            )
            and not exists (
              select 1
              from pg_catalog.unnest(v_pay_batch_candidate_ids) as required_candidate(pay_batch_candidate_id)
              where not exists (
                select 1
                from public.pay_payment_correction_items as correction_item
                where correction_item.correction_request_id = request_row.id
                  and correction_item.pay_batch_id = request_row.pay_batch_id
                  and correction_item.candidate_id = v_candidate_id
                  and correction_item.pay_batch_candidate_id = required_candidate.pay_batch_candidate_id
                  and correction_item.pay_batch_item_id = any(v_item_ids)
                  and correction_item.correction_item_kind = request_row.correction_kind
                  and correction_item.status = 'APPLIED'
                  and correction_item.applied_at_utc is not null
              )
            )
          order by request_row.applied_at_utc desc nulls last,
                   request_row.id
          limit 2
        ) as candidate_request;

        if pg_catalog.cardinality(v_match_ids) = 0 then
          v_reason := 'CORRECTION_ITEM_COVERAGE_NOT_FOUND';
        elsif pg_catalog.cardinality(v_match_ids) > 1 then
          v_reason := 'CORRECTION_ITEM_COVERAGE_AMBIGUOUS';
        else
          select request_row.*
            into v_request
          from public.pay_payment_correction_requests as request_row
          where request_row.id = v_match_ids[1];
        end if;
      end if;
    end if;

    if v_reason is not null then
      v_skipped := v_skipped + 1;
      v_reason_counts := pg_catalog.jsonb_set(
        v_reason_counts,
        array[v_reason],
        pg_catalog.to_jsonb(coalesce((v_reason_counts->>v_reason)::integer, 0) + 1),
        true
      );
      v_event_result_code := v_reason;
    else
      v_notice_key := v_template_version || ':' || v_original.id::text;
    -- Fixed generic copy deliberately discloses no Candidate, Umbrella, batch,
    -- pay-date or payment context carried by the original remittance subject.
    v_notice_subject := 'Payment cancelled';
    v_notice_scope := pg_catalog.jsonb_build_object(
      'contract_version', v_template_version,
      'original_mail_outbox_id', v_original.id,
      'original_deterministic_outbox_key', v_original.deterministic_outbox_key,
      'original_sent_at_utc', v_original.sent_at,
      'pay_batch_id', v_request.pay_batch_id,
      'candidate_id', v_candidate_id,
      'correction_request_id', v_request.id,
      'correction_kind', v_request.correction_kind,
      'covered_item_count', pg_catalog.cardinality(v_item_ids)
    );
    v_inserted_id := null;
    v_existing_notice := null;

    insert into public.mail_outbox (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      created_at_utc,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      importance,
      email_type,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json,
      deterministic_outbox_key,
      attachments_ready,
      attachment_total_bytes,
      attachment_delivery_policy,
      cancel_notice_tracked
    ) values (
      'PAYMENT_CANCELLATION',
      v_original."to",
      v_original.cc,
      v_original.bcc,
      v_original.reply_to,
      v_notice_subject,
      v_notice_body_html,
      v_notice_body_text,
      '[]'::jsonb,
      'QUEUED'::public.mail_status_enum,
      pg_catalog.statement_timestamp(),
      v_original.created_by,
      v_notice_key,
      v_original.recipient_kind,
      v_original.recipient_id,
      'pay_payment_correction_requests',
      v_request.id,
      'Normal',
      'html',
      pg_catalog.statement_timestamp(),
      pg_catalog.statement_timestamp(),
      v_notice_scope,
      v_notice_key,
      true,
      0,
      null,
      false
    )
    on conflict (deterministic_outbox_key) do nothing
    returning id into v_inserted_id;

    if v_inserted_id is null then
      select existing_notice.*
        into v_existing_notice
      from public.mail_outbox as existing_notice
      where existing_notice.deterministic_outbox_key = v_notice_key;

      if found
         and v_existing_notice.type is not distinct from 'PAYMENT_CANCELLATION'
         and v_existing_notice."to" is not distinct from v_original."to"
         and v_existing_notice.cc is not distinct from v_original.cc
         and v_existing_notice.bcc is not distinct from v_original.bcc
         and v_existing_notice.reply_to is not distinct from v_original.reply_to
         and v_existing_notice.subject is not distinct from v_notice_subject
         and v_existing_notice.body_html is not distinct from v_notice_body_html
         and v_existing_notice.body_text is not distinct from v_notice_body_text
         and v_existing_notice.attachments is not distinct from '[]'::jsonb
         and v_existing_notice.created_by is not distinct from v_original.created_by
         and v_existing_notice.reference is not distinct from v_notice_key
         and v_existing_notice.recipient_kind is not distinct from v_original.recipient_kind
         and v_existing_notice.recipient_id is not distinct from v_original.recipient_id
         and v_existing_notice.context_kind is not distinct from 'pay_payment_correction_requests'
         and v_existing_notice.context_id is not distinct from v_request.id
         and v_existing_notice.importance is not distinct from 'Normal'
         and v_existing_notice.email_type is not distinct from 'html'
         and v_existing_notice.payment_scope_json is not distinct from v_notice_scope
         and v_existing_notice.attachments_ready is not distinct from true
         and v_existing_notice.attachment_total_bytes is not distinct from 0
         and v_existing_notice.attachment_delivery_policy is null
         and v_existing_notice.cancel_notice_tracked is not distinct from false then
        if v_existing_notice.status::text in ('QUEUED', 'SENT') then
          v_eligible := v_eligible + 1;
          v_already_present := v_already_present + 1;
          v_event_result_code := 'ALREADY_PRESENT';
        elsif v_existing_notice.status::text = 'FAILED'
           and v_existing_notice.sent_at is null then
          -- Delivery retry remains with the established outbox_unified_retry
          -- owner/UI.  This reconciler records the state but must not invent an
          -- automatic retry policy or couple mail delivery to cancellation.
          v_skipped := v_skipped + 1;
          v_reason := 'EXISTING_NOTICE_AWAITS_MANUAL_RETRY';
          v_reason_counts := pg_catalog.jsonb_set(
            v_reason_counts,
            array[v_reason],
            pg_catalog.to_jsonb(coalesce((v_reason_counts->>v_reason)::integer, 0) + 1),
            true
          );
          v_event_result_code := v_reason;
        else
          v_skipped := v_skipped + 1;
          v_reason := 'EXISTING_NOTICE_STATE_CONFLICT';
          v_reason_counts := pg_catalog.jsonb_set(
            v_reason_counts,
            array[v_reason],
            pg_catalog.to_jsonb(coalesce((v_reason_counts->>v_reason)::integer, 0) + 1),
            true
          );
          v_event_result_code := v_reason;
        end if;
      else
        v_skipped := v_skipped + 1;
        v_reason := 'EXISTING_NOTICE_IDENTITY_CONFLICT';
        v_reason_counts := pg_catalog.jsonb_set(
          v_reason_counts,
          array[v_reason],
          pg_catalog.to_jsonb(coalesce((v_reason_counts->>v_reason)::integer, 0) + 1),
          true
        );
        v_event_result_code := v_reason;
      end if;
    else
      v_eligible := v_eligible + 1;
      v_queued := v_queued + 1;
      v_event_result_code := 'QUEUED';
    end if;
    end if;
    exception when others then
      v_examined := v_before_examined + 1;
      v_eligible := v_before_eligible;
      v_queued := v_before_queued;
      v_already_present := v_before_already_present;
      v_skipped := v_before_skipped + 1;
      v_reason_counts := pg_catalog.jsonb_set(
        v_before_reason_counts,
        array['UNEXPECTED_EVENT_ERROR'],
        pg_catalog.to_jsonb(
          coalesce(
            (v_before_reason_counts->>'UNEXPECTED_EVENT_ERROR')::integer,
            0
          ) + 1
        ),
        true
      );
      v_event_had_error := true;
      v_event_result_code := 'UNEXPECTED_EVENT_ERROR';
    end;

    if v_event_had_error then
      update public.mail_outbox as source_mail
         set cancel_notice_next_attempt_at_utc =
               pg_catalog.statement_timestamp() + interval '5 minutes',
             cancel_notice_result_code = v_event_result_code
       where source_mail.id = v_original.id
         and source_mail.cancel_notice_tracked is true
         and source_mail.sent_at is not distinct from v_original.sent_at
         and source_mail.cancel_notice_reconciled_sent_at_utc
               is distinct from source_mail.sent_at;
      v_has_more := true;
      if v_effective_request_bound then
        v_request_event_had_error := true;
        exit;
      end if;
    else
      -- Deterministic success and deterministic rejection both consume this
      -- exact SENT event.  A later SENT token reopens it automatically.
      update public.mail_outbox as source_mail
         set cancel_notice_reconciled_sent_at_utc = source_mail.sent_at,
             cancel_notice_next_attempt_at_utc = null,
             cancel_notice_result_code = v_event_result_code
       where source_mail.id = v_original.id
         and source_mail.cancel_notice_tracked is true
         and source_mail.sent_at is not distinct from v_original.sent_at;

      if v_effective_request_bound then
        v_request_progress_after_created_at_utc := v_original.created_at_utc;
        v_request_progress_after_mail_outbox_id := v_original.id;
      end if;
    end if;
  end loop;

  v_has_more := v_has_more or v_source_page_more;

  if v_effective_request_bound and v_examined = 0 then
    v_examined := 1;
    v_skipped := 1;
    v_reason_counts := pg_catalog.jsonb_build_object(
      'CORRECTION_SOURCE_NOT_FOUND', 1
    );
    v_event_result_code := 'CORRECTION_SOURCE_NOT_FOUND';
  elsif v_mode = 'ORIGINAL' and v_examined = 0 then
    v_examined := 1;
    v_skipped := 1;
    v_reason_counts := pg_catalog.jsonb_build_object('ORIGINAL_NOT_FOUND', 1);
  end if;

  if v_effective_request_bound then
    v_prior_examined := coalesce(
      nullif(v_request_result_json->>'examined', '')::integer,
      0
    );
    v_prior_eligible := coalesce(
      nullif(v_request_result_json->>'eligible', '')::integer,
      0
    );
    v_prior_queued := coalesce(
      nullif(v_request_result_json->>'queued', '')::integer,
      0
    );
    v_prior_already_present := coalesce(
      nullif(v_request_result_json->>'already_present', '')::integer,
      0
    );
    v_prior_skipped := coalesce(
      nullif(v_request_result_json->>'skipped', '')::integer,
      0
    );

    -- The settled functional ceiling applies to the entire request event, not
    -- independently to each page.  Raise before the request progress write; the
    -- enclosing RPC transaction then rolls back every notice/marker touched by
    -- this over-ceiling page instead of persisting an impossible cursor that a
    -- later call would terminalise as corrupt.
    if v_prior_examined::bigint + v_examined::bigint > 50000
       or v_prior_eligible::bigint + v_eligible::bigint > 50000
       or v_prior_queued::bigint + v_queued::bigint > 50000
       or v_prior_already_present::bigint + v_already_present::bigint > 50000
       or v_prior_skipped::bigint + v_skipped::bigint > 50000 then
      raise exception using
        errcode = '54000',
        message = 'PAYMENT_CANCELLATION_NOTICE_REQUEST_EVENT_CEILING_EXCEEDED';
    end if;

    select coalesce(
             pg_catalog.jsonb_object_agg(
               merged_reason.reason_code,
               merged_reason.reason_count
             ),
             '{}'::jsonb
           )
      into v_cumulative_reason_counts
    from (
      select reason_source.reason_code,
             pg_catalog.sum(reason_source.reason_count)::integer as reason_count
      from (
        select prior_reason.key as reason_code,
               prior_reason.value::integer as reason_count
        from pg_catalog.jsonb_each_text(
          coalesce(v_request_result_json->'reason_counts', '{}'::jsonb)
        ) as prior_reason(key, value)
        union all
        select page_reason.key,
               page_reason.value::integer
        from pg_catalog.jsonb_each_text(v_reason_counts)
          as page_reason(key, value)
      ) as reason_source
      group by reason_source.reason_code
    ) as merged_reason;

    v_request_result_json := pg_catalog.jsonb_build_object(
      'template_version', v_template_version,
      'reconciliation_authority', 'ASYNCHRONOUS_SENT_ONLY',
      'event_applied_at_utc', pg_catalog.to_char(
        pg_catalog.timezone('UTC', v_bound_request.applied_at_utc),
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
      ),
      'examined', v_prior_examined + v_examined,
      'eligible', v_prior_eligible + v_eligible,
      'queued', v_prior_queued + v_queued,
      'already_present', v_prior_already_present + v_already_present,
      'skipped', v_prior_skipped + v_skipped,
      'reason_counts', v_cumulative_reason_counts,
      'complete', not v_has_more,
      'last_source_created_at_utc', case
        when v_request_progress_after_created_at_utc is null then null
        else pg_catalog.to_char(
          pg_catalog.timezone('UTC', v_request_progress_after_created_at_utc),
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
      end,
      'last_source_mail_outbox_id', v_request_progress_after_mail_outbox_id,
      'result_code', case
        when v_request_event_had_error then 'UNEXPECTED_EVENT_ERROR_RETRY'
        when v_has_more then 'SOURCE_PAGE_CONTINUES'
        when v_event_result_code = 'CORRECTION_SOURCE_NOT_FOUND'
          then 'NO_SENT_SOURCE_AT_RECONCILIATION'
        else 'EVENT_COMPLETE'
      end
    );

    update public.pay_payment_correction_requests as request_row
       set cancel_notice_reconciled_applied_at_utc = case
             when not v_has_more then v_bound_request.applied_at_utc
             else request_row.cancel_notice_reconciled_applied_at_utc
           end,
           cancel_notice_progress_applied_at_utc = case
             when v_has_more then v_bound_request.applied_at_utc
             else null
           end,
           cancel_notice_after_created_at_utc = case
             when v_has_more then v_request_progress_after_created_at_utc
             else null
           end,
           cancel_notice_after_mail_outbox_id = case
             when v_has_more then v_request_progress_after_mail_outbox_id
             else null
           end,
           cancel_notice_next_attempt_at_utc = case
             when v_request_event_had_error
               then pg_catalog.statement_timestamp() + interval '5 minutes'
             when v_has_more then pg_catalog.statement_timestamp()
             else null
           end,
           cancel_notice_result_json = v_request_result_json
     where request_row.id = v_bound_request.id
       and request_row.cancel_notice_tracked is true
       and request_row.applied_at_utc is not distinct from
           v_bound_request.applied_at_utc;
  end if;

  select coalesce(pg_catalog.sum(reason_entry.value::integer), 0)::integer
    into v_reason_total
  from pg_catalog.jsonb_each_text(v_reason_counts) as reason_entry(key, value);

  if v_examined <> v_eligible + v_skipped
     or v_eligible <> v_queued + v_already_present
     or v_reason_total <> v_skipped then
    raise exception using
      errcode = 'XX000',
      message = 'PAYMENT_CANCELLATION_NOTICE_RESULT_INVARIANT_VIOLATION';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok', true,
    'mode', v_mode,
    'template_version', v_template_version,
    'examined', v_examined,
    'eligible', v_eligible,
    'queued', v_queued,
    'already_present', v_already_present,
    'skipped', v_skipped,
    'reason_counts', v_reason_counts,
    -- Contention belongs only to this response envelope.  It must not make a
    -- different request persist false continuation state, but the caller must
    -- still be told that the recovery lane was not terminal.
    'has_more', v_has_more or v_recovery_claim_contended,
    'next_cursor', null,
    'progress_owner', 'SERVER_ROW',
    'recovery_claim_contended', v_recovery_claim_contended
  );
end
$function$;

alter function public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamptz,uuid,integer,text)
  owner to postgres;

-- CREATE OR REPLACE preserves a function's existing ACL.  Restore the exact
-- service-only boundary on every reapply instead of trusting a finite list of
-- known browser/provider roles: any unexpected direct EXECUTE grantee (and any
-- delegated grant option) is removed before the intended grants are restored.
do $acl$
declare
  v_function_identity constant text :=
    'public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamptz,uuid,integer,text)';
  v_function_oid oid := pg_catalog.to_regprocedure(
    'public.pay_payment_cancellation_notice_reconcile_v1(uuid,uuid,timestamp with time zone,uuid,integer,text)'
  );
  v_function_owner_oid oid;
  v_service_role_oid oid := pg_catalog.to_regrole('service_role');
  v_acl record;
begin
  if v_function_oid is null or v_service_role_oid is null then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_ACL_PREREQUISITE_MISSING';
  end if;

  select function_row.proowner
    into strict v_function_owner_oid
  from pg_catalog.pg_proc as function_row
  where function_row.oid = v_function_oid;

  for v_acl in
    select distinct acl_row.grantee
    from pg_catalog.pg_proc as function_row
    cross join lateral pg_catalog.aclexplode(
      coalesce(
        function_row.proacl,
        pg_catalog.acldefault('f', function_row.proowner)
      )
    ) as acl_row
    where function_row.oid = v_function_oid
      and acl_row.privilege_type = 'EXECUTE'
      and acl_row.grantee not in (
        v_function_owner_oid,
        v_service_role_oid
      )
  loop
    if v_acl.grantee = 0 then
      execute pg_catalog.format(
        'revoke all on function %s from public cascade',
        v_function_identity
      );
    else
      execute pg_catalog.format(
        'revoke all on function %s from %I cascade',
        v_function_identity,
        pg_catalog.pg_get_userbyid(v_acl.grantee)
      );
    end if;
  end loop;

  for v_acl in
    select distinct acl_row.grantee
    from pg_catalog.pg_proc as function_row
    cross join lateral pg_catalog.aclexplode(
      coalesce(
        function_row.proacl,
        pg_catalog.acldefault('f', function_row.proowner)
      )
    ) as acl_row
    where function_row.oid = v_function_oid
      and acl_row.privilege_type = 'EXECUTE'
      and acl_row.is_grantable is true
      and acl_row.grantee in (
        v_function_owner_oid,
        v_service_role_oid
      )
  loop
    execute pg_catalog.format(
      'revoke grant option for execute on function %s from %I cascade',
      v_function_identity,
      pg_catalog.pg_get_userbyid(v_acl.grantee)
    );
  end loop;

  execute pg_catalog.format(
    'grant execute on function %s to %I',
    v_function_identity,
    pg_catalog.pg_get_userbyid(v_function_owner_oid)
  );
  execute pg_catalog.format(
    'grant execute on function %s to %I',
    v_function_identity,
    pg_catalog.pg_get_userbyid(v_service_role_oid)
  );
end
$acl$;

notify pgrst, 'reload schema';

commit;
