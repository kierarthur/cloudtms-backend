-- One-time CloudTMS schema/data migration: weekly_contact_native_attempt_permit_unique
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- One original native attempt can own only one contact admission. Logical
-- delivery uniqueness separately governs retries across distinct attempts.
-- A duplicate fails closed; no existing permit or result is rewritten.
create unique index weekly_contact_submission_permits_native_attempt_uq
 on public.weekly_contact_submission_permits(provider_attempt_id)
 where provider_attempt_id is not null;

commit;
