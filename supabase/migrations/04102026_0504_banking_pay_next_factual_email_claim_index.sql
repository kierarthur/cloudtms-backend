-- A8: exclude retained financial mail from the bounded factual claim lane.
-- No owner activation, row rewrite, historical chooser, or browser permission.
-- Exact metadata counterpart of broker/src/bpay-next-legacy-email.js.
\set ON_ERROR_STOP on
begin;

do $preflight$
begin
  if pg_catalog.to_regprocedure('private.bpay_next_legacy_email_is_factual_v1(text,text,text)') is not null
     or pg_catalog.to_regclass('public.bpay_next_factual_email_ready_idx') is not null then
    raise exception using errcode='55000',message='BPAY_NEXT_EMAIL_CLAIM_OBJECT_CONFLICT';
  end if;
end;
$preflight$;

-- JS String.trim's complete whitespace set, followed by ASCII-only case
-- folding, is shared exactly. SQL must NOT be STRICT: NULL optional context or
-- reference is a legitimate factual row, while NULL/unknown type is not proof.
create function private.bpay_next_legacy_email_is_factual_v1(
  p_type text,p_context_kind text,p_reference text
) returns boolean language sql immutable parallel safe security invoker
called on null input
set search_path=pg_catalog
as $function$
  with normalized as (
    select
      pg_catalog.translate(pg_catalog.btrim(coalesce(p_type,''),U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF'),
        'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ') collate "C" as row_type,
      pg_catalog.translate(pg_catalog.btrim(coalesce(p_context_kind,''),U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF'),
        'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ') collate "C" as row_context,
      pg_catalog.translate(pg_catalog.btrim(coalesce(p_reference,''),U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF'),
        'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ') collate "C" as row_reference
  )
  select row_type=any(array[
    'INVOICE','TSO_FAILURE','BROADCAST','TIMESHEET_QR',
    'TIMESHEET_REFUSAL','TIMESHEET_GENERAL','TIMESHEET_QUERY','MAILSHOT_EMAIL'
  ]::text[])
  and not(row_context=any(array[
    'PAY_BATCH','PAY_BATCHES','PAY_BATCH_REMITTANCE','PAY_PAYMENT_CORRECTION_REQUESTS'
  ]::text[]))
  and not(
    pg_catalog.starts_with(row_reference,'REMITTANCE')
    or pg_catalog.starts_with(row_reference,'PAYOUT_NOTICE:')
    or pg_catalog.starts_with(row_reference,'PAY_AUTH_INVITE:')
    or pg_catalog.starts_with(row_reference,'PAY_BATCH_PROCESSING:')
    or pg_catalog.starts_with(row_reference,'PAY_BATCH_SCHEDULED:')
    or pg_catalog.starts_with(row_reference,'PAY_BATCH_COMPLETED:')
    or pg_catalog.starts_with(row_reference,'PAYMENT_CANCELLATION_NOTICE_V1:')
  )
  from normalized;
$function$;

alter function private.bpay_next_legacy_email_is_factual_v1(text,text,text) owner to postgres;
revoke all on function private.bpay_next_legacy_email_is_factual_v1(text,text,text)
  from public,anon,authenticated,service_role;
grant execute on function private.bpay_next_legacy_email_is_factual_v1(text,text,text) to service_role;

-- The non-LEGACY query has this exact constant predicate (not a parameterized
-- owner OR factual clause). Its ORDER BY matches these keys. Due clock and lease
-- expiry are current residual checks, never part of an immutable index predicate.
-- Native EXPLAIN is required before claiming the planner selected this index.
create index bpay_next_factual_email_ready_idx on public.mail_outbox(
  (coalesce(next_attempt_at_utc,scheduled_for_utc,created_at_utc)),created_at_utc,id
) where status='QUEUED' and sent_at is null and delivered_at is null and read_at is null
  and private.bpay_next_legacy_email_is_factual_v1(type,context_kind,reference) is true;

commit;

