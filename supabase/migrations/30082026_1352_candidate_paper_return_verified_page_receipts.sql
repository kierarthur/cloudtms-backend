-- Durable server proof that an immutable returned PAPER page's stored JPEG
-- contained the exact signed TSQ2 identity for its current manifest page.

\set ON_ERROR_STOP on

begin;

alter table public.candidate_submission_components
  add column if not exists paper_return_proof_receipt_sha256 bytea,
  add column if not exists paper_return_qr_payload_sha256 bytea,
  add column if not exists paper_return_verified_at_utc timestamptz;

alter table public.candidate_submission_components
  drop constraint if exists candidate_submission_components_paper_qr_receipt_ck;
alter table public.candidate_submission_components
  add constraint candidate_submission_components_paper_qr_receipt_ck check (
    (paper_return_proof_receipt_sha256 is null
      and paper_return_qr_payload_sha256 is null
      and paper_return_verified_at_utc is null)
    or
    (component_kind='SIGNED_RETURN'
      and state='IMMUTABLE'
      and paper_return_proof_receipt_sha256 is not null
      and octet_length(paper_return_proof_receipt_sha256)=32
      and paper_return_qr_payload_sha256 is not null
      and octet_length(paper_return_qr_payload_sha256)=32
      and paper_return_verified_at_utc is not null)
  ) not valid;

alter table public.candidate_submission_components
  validate constraint candidate_submission_components_paper_qr_receipt_ck;

comment on column public.candidate_submission_components.paper_return_proof_receipt_sha256 is
  'Server-side exact current-manifest proof receipt for a returned PAPER page; null for legacy V1 pages.';
comment on column public.candidate_submission_components.paper_return_qr_payload_sha256 is
  'Digest of the independently verified signed TSQ2 payload read from the stored JPEG bytes.';
comment on column public.candidate_submission_components.paper_return_verified_at_utc is
  'Server time at which the stored returned-page JPEG passed exact TSQ2 verification.';

commit;
