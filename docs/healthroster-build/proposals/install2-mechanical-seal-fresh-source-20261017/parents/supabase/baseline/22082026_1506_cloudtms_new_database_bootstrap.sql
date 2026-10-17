-- Safe system-only bootstrap for a genuinely new CloudTMS database.
-- No customer, candidate, timesheet, finance, provider, or banking data is copied.
-- Candidate/MyTMS features and autonomous Workbench activity start disabled.

\set ON_ERROR_STOP on
\if :{?cloudtms_environment}
\else
  \echo 'cloudtms_environment is required (TEST or LIVE)'
  \quit 3
\endif

select pg_catalog.set_config(
  'cloudtms.release.environment',
  pg_catalog.upper(:'cloudtms_environment'),
  false
);

do $environment_guard$
begin
  if pg_catalog.current_setting('cloudtms.release.environment') not in ('TEST', 'LIVE') then
    raise exception 'CLOUDTMS_RELEASE_ENVIRONMENT_INVALID';
  end if;
end
$environment_guard$;

insert into public.settings_defaults (
  id,
  candidate_app_environment,
  rail_env_default,
  banking_pay_workbench_cron_enabled,
  banking_pay_workbench_nudge_enabled,
  candidate_app_feature_flags_json
)
values (
  1,
  pg_catalog.current_setting('cloudtms.release.environment'),
  case
    when pg_catalog.current_setting('cloudtms.release.environment') = 'LIVE' then 'PROD'
    else 'SANDBOX'
  end,
  false,
  false,
  '{"candidate_paper_qr":false,"candidate_settings":false,"candidate_app_reads":false,"candidate_app_writes":false,"candidate_notifications":false,"candidate_manager_approval":false,"candidate_daily_finalisation":false,"candidate_route_confirmation":false,"candidate_account_registration":false,"candidate_expense_atomic_placement":false,"candidate_record_role_capabilities":false,"candidate_expense_invoice_routing_v1":false}'::jsonb
)
on conflict (id) do nothing;

insert into public.banking_pay_operation_config (
  operation_type, phase, chunk_type, default_chunk_size, min_chunk_size,
  max_chunk_size, max_advance_ms, lock_seconds, enabled
)
values
  ('DRAFT_CREATE','APPLY_FINANCE_ADJUSTMENTS','CANDIDATE_SCOPE',50,1,250,15000,60,true),
  ('DRAFT_CREATE','BUILD_ITEM_BREAKDOWNS','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','CREATE_TIMESHEET_SNAPSHOTS','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','DRAIN_TSFIN','TSFIN',100,1,250,15000,60,true),
  ('DRAFT_CREATE','ENSURE_PAYEE_READINESS','PAYEE_READINESS',50,1,250,15000,60,true),
  ('DRAFT_CREATE','FINALISE_RESERVATIONS','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','INSERT_CANDIDATES','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','INSERT_ITEMS','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','POPULATE_CANDIDATE_SUMMARIES','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('DRAFT_CREATE','SEED_ALLOCATION_ROWS','CANDIDATE_SCOPE',50,1,250,15000,60,true),
  ('DRAFT_CREATE','SEED_DRAFT_CHUNKS','CANDIDATE_SCOPE',100,1,250,15000,60,true),
  ('PAYMENT_CORRECTION','EXPAND_WORK','CANDIDATE_SCOPE',50,1,100,7500,60,true),
  ('PAYMENT_CORRECTION','FINALISE','CANDIDATE_SCOPE',100,1,100,7500,60,true),
  ('PAYMENT_CORRECTION','PREPARE_SELECTION','CANDIDATE_SCOPE',50,1,100,7500,60,true),
  ('PAYMENT_CORRECTION','PROCESS_CHUNKS','CANDIDATE_SCOPE',10,1,25,7500,60,true),
  ('PAYMENT_CORRECTION','REFRESH_WORKBENCH','CANDIDATE_SCOPE',100,1,100,7500,60,true),
  ('PAYMENT_EXECUTE','APPLY_RAIL_UPDATES','RAIL_UPDATE',100,1,250,15000,60,true),
  ('PAYMENT_EXECUTE','PREPARE_TRANSFER_CHUNKS','TRANSFER_GROUP',100,1,250,15000,60,true),
  ('PAYMENT_EXECUTE','PREPARE_TRANSFER_SCOPE','TRANSFER_GROUP',100,1,250,15000,60,true),
  ('PAYMENT_EXECUTE','SUBMIT_PROVIDER_TRANSFERS','TRANSFER_SUBMIT',50,1,250,15000,60,true),
  ('PAYMENT_EXECUTE','VALIDATE_FRESHNESS','FRESHNESS_VALIDATE',50,1,250,15000,60,true),
  ('PAYMENT_SETTLEMENT','APPLY_SETTLEMENT_CHUNKS','SETTLEMENT',100,1,250,15000,60,true),
  ('PREVIEW_REFRESH','PAGE','PREVIEW_PAGE',100,1,500,15000,60,true),
  ('REMITTANCE_QUEUE','QUEUE_PAYOUT_NOTICE_CHUNKS','PAYOUT_NOTICE',100,1,250,15000,60,true),
  ('REMITTANCE_QUEUE','QUEUE_REMITTANCE_CHUNKS','REMITTANCE',100,1,250,15000,60,true)
on conflict (operation_type, phase, chunk_type) do nothing;

do $bootstrap_hmac$
declare
  v_key_id text;
  v_secret_name text;
  v_secret_id uuid;
begin
  if not exists (
    select 1
    from private.invoice_async_snapshot_hmac_keys k
    where k.is_current
      and k.active_from_utc <= pg_catalog.statement_timestamp()
      and (k.active_to_utc is null or k.active_to_utc > pg_catalog.statement_timestamp())
  ) then
    v_key_id := 'baseline-' || pg_catalog.substr(extensions.gen_random_uuid()::text, 1, 16);
    v_secret_name := 'cloudtms_invoice_snapshot_' || pg_catalog.replace(extensions.gen_random_uuid()::text, '-', '');
    v_secret_id := vault.create_secret(
      pg_catalog.encode(extensions.gen_random_bytes(48), 'base64'),
      v_secret_name,
      'CloudTMS invoice snapshot HMAC generated during safe database bootstrap'
    );
    insert into private.invoice_async_snapshot_hmac_keys (
      key_id, vault_secret_id, active_from_utc, is_current
    )
    values (v_key_id, v_secret_id, pg_catalog.statement_timestamp(), true);
  end if;
end
$bootstrap_hmac$;
