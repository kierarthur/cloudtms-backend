-- One-time CloudTMS schema/data migration: banking_pay_next_accepted_case_week_terms_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Only an empty first-install capture can gain a previously missing term.
-- No DEFAULT or backfilled date/proof is historical economic authority.
do $capture_empty$
begin
  if exists(select 1 from private.bpay_next_accepted_week_input) then
    raise exception using errcode='55000',message='BPAY_NEXT_ACCEPTED_TERMS_INSTALL_REQUIRES_EMPTY';
  end if;
end
$capture_empty$;
-- One finite original-worker origin, never agency history. The bound includes
-- up to1000 exact immutable instruction/result ID and output-digest tuples.
alter table private.bpay_next_accepted_week_input
  drop constraint bpay_next_accepted_week_input_economic_origin_proof_check,
  add constraint bpay_next_accepted_week_input_economic_origin_proof_check
    check(pg_catalog.jsonb_typeof(economic_origin_proof)='object'
      and pg_catalog.octet_length(economic_origin_proof::text)<=262144);

commit;
