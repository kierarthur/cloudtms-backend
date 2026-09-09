-- Admit a dedicated, backward-compatible mail-outbox type for payment
-- cancellation notices and provide a bounded recovery-page access path.
-- This migration changes no payment, cancellation, remittance or provider state.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_constraint_definition text;
  v_constraint_validated boolean;
  v_normalized_definition text;
  v_unexpected_stored_type text;
  v_expected_preimage constant text := 'CHECK (((type = ANY (ARRAY[''INVOICE''::text, ''REMITTANCE''::text, ''TSO_FAILURE''::text, ''BROADCAST''::text, ''TIMESHEET_QR''::text, ''TIMESHEET_REFUSAL''::text, ''TIMESHEET_GENERAL''::text, ''TIMESHEET_QUERY''::text])) OR (type = ''MAILSHOT_EMAIL''::text)))';
  v_expected_postimage constant text := 'CHECK (((type = ANY (ARRAY[''INVOICE''::text, ''REMITTANCE''::text, ''TSO_FAILURE''::text, ''BROADCAST''::text, ''TIMESHEET_QR''::text, ''TIMESHEET_REFUSAL''::text, ''TIMESHEET_GENERAL''::text, ''TIMESHEET_QUERY''::text, ''PAYMENT_CANCELLATION''::text])) OR (type = ''MAILSHOT_EMAIL''::text)))';
  v_expected_after constant text[] := array[
    'BROADCAST',
    'INVOICE',
    'MAILSHOT_EMAIL',
    'PAYMENT_CANCELLATION',
    'REMITTANCE',
    'TIMESHEET_GENERAL',
    'TIMESHEET_QR',
    'TIMESHEET_QUERY',
    'TIMESHEET_REFUSAL',
    'TSO_FAILURE'
  ];
begin
  -- Preserve the established correction-owner lock order.  A correction takes
  -- its request row before any mail work, so this release must fence the
  -- request relation before mail_outbox to avoid a request->mail / mail->request
  -- deadlock during cutover.  Both locks are retained until commit.
  lock table only public.pay_payment_correction_requests
    in share row exclusive mode;
  lock table only public.mail_outbox in share row exclusive mode;

  select pg_catalog.pg_get_constraintdef(constraint_row.oid),
         constraint_row.convalidated
    into v_constraint_definition,
         v_constraint_validated
  from pg_catalog.pg_constraint as constraint_row
  where constraint_row.conrelid = 'public.mail_outbox'::pg_catalog.regclass
    and constraint_row.conname = 'mail_outbox_type_check'
    and constraint_row.contype = 'c';

  if v_constraint_definition is null or v_constraint_validated is not true then
    raise exception using
      errcode = '23514',
      message = 'PAYMENT_CANCELLATION_NOTICE_MAIL_TYPE_CONSTRAINT_MISSING_OR_UNVALIDATED';
  end if;

  v_normalized_definition := pg_catalog.btrim(
    pg_catalog.regexp_replace(v_constraint_definition, '[[:space:]]+', ' ', 'g')
  );

  if v_normalized_definition not in (v_expected_preimage, v_expected_postimage) then
    raise exception using
      errcode = '23514',
      message = 'PAYMENT_CANCELLATION_NOTICE_MAIL_TYPE_CONSTRAINT_UNEXPECTED',
      detail = v_constraint_definition;
  end if;

  select stored_row.type
    into v_unexpected_stored_type
  from only public.mail_outbox as stored_row
  where stored_row.type <> all(v_expected_after)
  order by stored_row.type
  limit 1;

  if v_unexpected_stored_type is not null then
    raise exception using
      errcode = '23514',
      message = 'PAYMENT_CANCELLATION_NOTICE_STORED_MAIL_TYPE_UNEXPECTED',
      detail = v_unexpected_stored_type;
  end if;

  if v_normalized_definition = v_expected_preimage then
    alter table public.mail_outbox
      drop constraint mail_outbox_type_check;

    alter table public.mail_outbox
      add constraint mail_outbox_type_check
      check (
        type = any (
          array[
            'INVOICE'::text,
            'REMITTANCE'::text,
            'TSO_FAILURE'::text,
            'BROADCAST'::text,
            'TIMESHEET_QR'::text,
            'TIMESHEET_REFUSAL'::text,
            'TIMESHEET_GENERAL'::text,
            'TIMESHEET_QUERY'::text,
            'PAYMENT_CANCELLATION'::text
          ]
        )
        or type = 'MAILSHOT_EMAIL'::text
      ) not valid;

    alter table public.mail_outbox
      validate constraint mail_outbox_type_check;
  end if;

  select pg_catalog.btrim(
           pg_catalog.regexp_replace(
             pg_catalog.pg_get_constraintdef(constraint_row.oid),
             '[[:space:]]+',
             ' ',
             'g'
           )
         ),
         constraint_row.convalidated
    into v_normalized_definition,
         v_constraint_validated
  from pg_catalog.pg_constraint as constraint_row
  where constraint_row.conrelid = 'public.mail_outbox'::pg_catalog.regclass
    and constraint_row.conname = 'mail_outbox_type_check'
    and constraint_row.contype = 'c';

  if v_normalized_definition is distinct from v_expected_postimage
     or v_constraint_validated is not true then
    raise exception using
      errcode = '23514',
      message = 'PAYMENT_CANCELLATION_NOTICE_MAIL_TYPE_POSTCONDITION_FAILED',
      detail = coalesce(v_normalized_definition, 'NULL');
  end if;
end
$migration$;

-- Recovery is driven only by post-cutover row-local events.  Adding these
-- nullable columns without defaults is metadata-only for historical rows.
-- Historical NULL means deliberately outside this V1 recovery cutover; future
-- rows receive TRUE only after the locked zero-active cutover below.
alter table public.mail_outbox
  add column if not exists cancel_notice_tracked boolean,
  add column if not exists cancel_notice_reconciled_sent_at_utc timestamptz,
  add column if not exists cancel_notice_next_attempt_at_utc timestamptz,
  add column if not exists cancel_notice_result_code text;

alter table public.pay_payment_correction_requests
  add column if not exists cancel_notice_tracked boolean,
  add column if not exists cancel_notice_reconciled_applied_at_utc timestamptz,
  add column if not exists cancel_notice_progress_applied_at_utc timestamptz,
  add column if not exists cancel_notice_after_created_at_utc timestamptz,
  add column if not exists cancel_notice_after_mail_outbox_id uuid,
  add column if not exists cancel_notice_next_attempt_at_utc timestamptz,
  add column if not exists cancel_notice_result_json jsonb;

do $recovery_cutover$
declare
  v_nonterminal_request_count bigint;
begin
  -- Both relations are already locked above, in the same request-before-mail
  -- order used by the correction owners, and remain locked in this transaction.

  -- APPLIED_WITH_BLOCKERS is deliberately included: current same-request
  -- resumption can reset it to EXPANDED and later assign a new applied_at_utc.
  select pg_catalog.count(*)
    into v_nonterminal_request_count
  from only public.pay_payment_correction_requests as request_row
  where request_row.status is null
     or request_row.status not in (
       'APPLIED', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
     );

  if v_nonterminal_request_count <> 0 then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_CUTOVER_ACTIVE_REQUESTS',
      detail = v_nonterminal_request_count::text;
  end if;

  -- Existing history remains NULL/untracked.  Only an exact payment email that
  -- is still retryable or leased in the QUEUED state is opted into the new
  -- SENT-event lane, so a later provider acceptance cannot be lost.
  update only public.mail_outbox as mail_row
     set cancel_notice_tracked = true
   where mail_row.cancel_notice_tracked is null
     and mail_row.type = 'REMITTANCE'
     and mail_row.status in (
       'QUEUED'::public.mail_status_enum,
       'FAILED'::public.mail_status_enum
     )
     and mail_row.sent_at is null
     and mail_row.context_kind = 'pay_batches'
     and mail_row.context_id is not null
     and pg_catalog.jsonb_typeof(mail_row.payment_scope_json) = 'object'
     and (
       mail_row.payment_scope_json->>'remittance_type' in (
         'CANDIDATE_REMITTANCE', 'UMBRELLA_REMITTANCE'
       )
       or (
         mail_row.payment_scope_json->>'message_kind' = 'PAYOUT_NOTICE'
         and mail_row.payment_scope_json->>'notice_scope' = 'PAYOUT_NOTICE_CANDIDATE'
       )
     );

  alter table public.mail_outbox
    alter column cancel_notice_tracked set default true;
  alter table public.pay_payment_correction_requests
    alter column cancel_notice_tracked set default true,
    alter column cancel_notice_result_json set default '{}'::jsonb;
end
$recovery_cutover$;

create index if not exists mail_outbox_payment_cancellation_sent_source_idx
  on public.mail_outbox (context_id, created_at_utc, id)
  where context_kind = 'pay_batches'
    and type = 'REMITTANCE'
    and status = 'SENT'::public.mail_status_enum
    and sent_at is not null
    and provider_status = 'ACCEPTED';

create index if not exists mail_outbox_payment_cancellation_sent_candidate_source_idx
  on public.mail_outbox (
    context_id,
    (payment_scope_json->>'candidate_id'),
    (payment_scope_json->>'pay_batch_candidate_id'),
    created_at_utc,
    id
  )
  where context_kind = 'pay_batches'
    and type = 'REMITTANCE'
    and status = 'SENT'::public.mail_status_enum
    and sent_at is not null
    and provider_status = 'ACCEPTED';

create index if not exists mail_outbox_payment_cancellation_pending_idx
  on public.mail_outbox (
    (coalesce(cancel_notice_next_attempt_at_utc, sent_at)),
    sent_at,
    id
  )
  where cancel_notice_tracked is true
    and type = 'REMITTANCE'
    and status = 'SENT'::public.mail_status_enum
    and sent_at is not null
    and provider_status = 'ACCEPTED'
    and cancel_notice_reconciled_sent_at_utc is distinct from sent_at;

create index if not exists pay_payment_cancellation_notice_recovery_request_idx
  on public.pay_payment_correction_requests (
    (coalesce(cancel_notice_next_attempt_at_utc, applied_at_utc)),
    applied_at_utc,
    id
  )
  where cancel_notice_tracked is true
    and applied_at_utc is not null
    and cancel_notice_reconciled_applied_at_utc is distinct from applied_at_utc
    and status in ('APPLIED', 'APPLIED_WITH_BLOCKERS')
    and correction_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND')
    and plan_json->>'candidate_scope_contract_version' = '2'
    and plan_json->>'candidate_scope_hash_version' = '2'
    and plan_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'
    and plan_json->>'communication_cleanup_contract_version' = '2';

do $index_verification$
declare
  v_index_definition text;
  v_index_valid boolean;
  v_index_ready boolean;
  v_expected_index_definition constant text :=
    'CREATE INDEX mail_outbox_payment_cancellation_sent_source_idx ON public.mail_outbox USING btree (context_id, created_at_utc, id) WHERE ((context_kind = ''pay_batches''::text) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text))';
begin
  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid,
         index_row.indisready
    into v_index_definition,
         v_index_valid,
         v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid =
    pg_catalog.to_regclass('public.mail_outbox_payment_cancellation_sent_source_idx');

  if v_index_definition is distinct from v_expected_index_definition
     or v_index_valid is not true
     or v_index_ready is not true then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_RECOVERY_INDEX_POSTCONDITION_FAILED',
      detail = coalesce(v_index_definition, 'NULL');
  end if;
end
$index_verification$;

do $candidate_source_index_verification$
declare
  v_index_definition text;
  v_index_valid boolean;
  v_index_ready boolean;
  v_expected_index_definition constant text :=
    'CREATE INDEX mail_outbox_payment_cancellation_sent_candidate_source_idx ON public.mail_outbox USING btree (context_id, ((payment_scope_json ->> ''candidate_id''::text)), ((payment_scope_json ->> ''pay_batch_candidate_id''::text)), created_at_utc, id) WHERE ((context_kind = ''pay_batches''::text) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text))';
begin
  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid,
         index_row.indisready
    into v_index_definition,
         v_index_valid,
         v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid = pg_catalog.to_regclass(
    'public.mail_outbox_payment_cancellation_sent_candidate_source_idx'
  );

  if v_index_definition is distinct from v_expected_index_definition
     or v_index_valid is not true
     or v_index_ready is not true then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_CANDIDATE_SOURCE_INDEX_POSTCONDITION_FAILED',
      detail = coalesce(v_index_definition, 'NULL');
  end if;
end
$candidate_source_index_verification$;

do $pending_mail_index_verification$
declare
  v_index_definition text;
  v_index_valid boolean;
  v_index_ready boolean;
begin
  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid,
         index_row.indisready
    into v_index_definition,
         v_index_valid,
         v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid =
    pg_catalog.to_regclass('public.mail_outbox_payment_cancellation_pending_idx');

  if v_index_definition is distinct from
       'CREATE INDEX mail_outbox_payment_cancellation_pending_idx ON public.mail_outbox USING btree (COALESCE(cancel_notice_next_attempt_at_utc, sent_at), sent_at, id) WHERE ((cancel_notice_tracked IS TRUE) AND (type = ''REMITTANCE''::text) AND (status = ''SENT''::mail_status_enum) AND (sent_at IS NOT NULL) AND (provider_status = ''ACCEPTED''::text) AND (cancel_notice_reconciled_sent_at_utc IS DISTINCT FROM sent_at))'
     or v_index_valid is not true
     or v_index_ready is not true then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_PENDING_MAIL_INDEX_POSTCONDITION_FAILED',
      detail = coalesce(v_index_definition, 'NULL');
  end if;
end
$pending_mail_index_verification$;

do $request_index_verification$
declare
  v_index_definition text;
  v_index_valid boolean;
  v_index_ready boolean;
begin
  select pg_catalog.pg_get_indexdef(index_row.indexrelid),
         index_row.indisvalid,
         index_row.indisready
    into v_index_definition,
         v_index_valid,
         v_index_ready
  from pg_catalog.pg_index as index_row
  where index_row.indexrelid =
    pg_catalog.to_regclass('public.pay_payment_cancellation_notice_recovery_request_idx');

  if v_index_definition is distinct from
       'CREATE INDEX pay_payment_cancellation_notice_recovery_request_idx ON public.pay_payment_correction_requests USING btree (COALESCE(cancel_notice_next_attempt_at_utc, applied_at_utc), applied_at_utc, id) WHERE ((cancel_notice_tracked IS TRUE) AND (applied_at_utc IS NOT NULL) AND (cancel_notice_reconciled_applied_at_utc IS DISTINCT FROM applied_at_utc) AND (status = ANY (ARRAY[''APPLIED''::text, ''APPLIED_WITH_BLOCKERS''::text])) AND (correction_kind = ANY (ARRAY[''PRE_BANK_CANCEL''::text, ''NO_MONEY_UNWIND''::text])) AND ((plan_json ->> ''candidate_scope_contract_version''::text) = ''2''::text) AND ((plan_json ->> ''candidate_scope_hash_version''::text) = ''2''::text) AND ((plan_json ->> ''source_row_count_semantics''::text) = ''FINANCIAL_ONLY''::text) AND ((plan_json ->> ''communication_cleanup_contract_version''::text) = ''2''::text))'
     or v_index_valid is not true
     or v_index_ready is not true then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_REQUEST_INDEX_POSTCONDITION_FAILED',
      detail = coalesce(v_index_definition, 'NULL');
  end if;
end
$request_index_verification$;

do $recovery_column_verification$
declare
  v_invalid_column_count integer;
begin
  select pg_catalog.count(*)::integer
    into v_invalid_column_count
  from (
    values
      ('mail_outbox', 'cancel_notice_tracked', 'boolean', 'true'),
      ('mail_outbox', 'cancel_notice_reconciled_sent_at_utc', 'timestamp with time zone', null),
      ('mail_outbox', 'cancel_notice_next_attempt_at_utc', 'timestamp with time zone', null),
      ('mail_outbox', 'cancel_notice_result_code', 'text', null),
      ('pay_payment_correction_requests', 'cancel_notice_tracked', 'boolean', 'true'),
      ('pay_payment_correction_requests', 'cancel_notice_reconciled_applied_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_progress_applied_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_after_created_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_after_mail_outbox_id', 'uuid', null),
      ('pay_payment_correction_requests', 'cancel_notice_next_attempt_at_utc', 'timestamp with time zone', null),
      ('pay_payment_correction_requests', 'cancel_notice_result_json', 'jsonb', '''{}''::jsonb')
  ) as expected(table_name, column_name, data_type, column_default)
  left join information_schema.columns as actual
    on actual.table_schema = 'public'
   and actual.table_name = expected.table_name
   and actual.column_name = expected.column_name
  where actual.column_name is null
     or actual.data_type is distinct from expected.data_type
     or actual.is_nullable is distinct from 'YES'
     or actual.column_default is distinct from expected.column_default;

  if v_invalid_column_count <> 0 then
    raise exception using
      errcode = '55000',
      message = 'PAYMENT_CANCELLATION_NOTICE_RECOVERY_COLUMN_POSTCONDITION_FAILED',
      detail = v_invalid_column_count::text;
  end if;
end
$recovery_column_verification$;

commit;
