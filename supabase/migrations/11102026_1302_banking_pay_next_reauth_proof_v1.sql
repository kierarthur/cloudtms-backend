-- One-time CloudTMS schema/data migration: banking_pay_next_reauth_proof_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- EX-01 / BD110: audit-only proof; no financial backfill or raw credentials.
create table private.bpay_next_reauth_proof (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  action_kind text not null check(action_kind in ('EXECUTE_REQUEST','EXECUTION_APPROVE',
    'CSV_FILE_ISSUE','CSV_ISSUE','CSV_DISPOSITION','CANCEL_SELECTION','CANCEL_REQUEST',
    'RELEASE_REFUSED','CASH_REISSUE','REISSUE_CANCEL','RETURN_SELECTION','CSV_RETURN',
    'OUTCOME_REVIEW_RESOLVE','WRITE_OFF','PAYE_SAME_WEEK_OVERRIDE')),
  scope_sha256 text not null check(scope_sha256 ~ '^[0-9a-f]{64}$'),
  token_sha256 text not null unique check(token_sha256 ~ '^[0-9a-f]{64}$'),
  second_factor_used boolean not null,
  verified_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  -- CSV_ISSUE uses the native instruction UUID, not a command-table row.
  consumed_command_id uuid unique,
  check(pg_catalog.isfinite(verified_at_utc) and pg_catalog.isfinite(expires_at_utc)
    and expires_at_utc>verified_at_utc
    and expires_at_utc<=verified_at_utc+interval '300 seconds')
);
alter table private.bpay_next_reauth_proof owner to postgres;
alter table private.bpay_next_reauth_proof enable row level security;
alter table private.bpay_next_reauth_proof force row level security;
revoke all on private.bpay_next_reauth_proof from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_reauth_proof
  for all to CURRENT_USER using(true) with check(true);

alter table public.tms_login_2fa_challenges
  add column action_kind text,
  add column scope_sha256 text;
alter table public.tms_login_2fa_challenges
  drop constraint tms_login_2fa_challenges_purpose_chk,
  add constraint tms_login_2fa_challenges_purpose_chk check(purpose in
    ('PAYMENT_SCHEDULE','PAYMENT_REVERSAL','PAYE_SAME_WEEK_OVERRIDE','BPAY_NEXT_ACTION')),
  add constraint tms_login_2fa_challenges_next_scope_chk check(
    (purpose<>'BPAY_NEXT_ACTION' and action_kind is null and scope_sha256 is null)
    or (purpose='BPAY_NEXT_ACTION' and action_kind is not null and scope_sha256 is not null
      and scope_sha256 ~ '^[0-9a-f]{64}$'
      and action_kind in ('EXECUTE_REQUEST','EXECUTION_APPROVE','CSV_FILE_ISSUE',
        'CSV_ISSUE','CSV_DISPOSITION','CANCEL_SELECTION','CANCEL_REQUEST','RELEASE_REFUSED',
        'CASH_REISSUE','REISSUE_CANCEL','RETURN_SELECTION','CSV_RETURN',
        'OUTCOME_REVIEW_RESOLVE','WRITE_OFF','PAYE_SAME_WEEK_OVERRIDE')));

commit;
