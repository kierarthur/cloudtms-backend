-- One-time CloudTMS schema/data migration: banking_pay_next_resolution_command_receipts
-- WB29: existing immutable W2 receipt, action-specific finite resolution variant.
-- Native retry fields/limits remain exact; no new job, receipt table or data rewrite.

\set ON_ERROR_STOP on

begin;

-- Discover the two generated checks by their audited PG17 expressions. Require
-- the unique catalogue result and exact observed name before the static DDL.
-- Never drop all checks mentioning a column or guess a generated suffix.
do $function$
declare v_names text[];
begin
 select array_agg(c.conname::text order by c.conname) into v_names from pg_catalog.pg_constraint c
  where c.conrelid='private.bpay_next_workbench_command_receipt'::pg_catalog.regclass and c.contype='c'
   and pg_catalog.pg_get_expr(c.conbin,c.conrelid)=$expr$((jsonb_typeof(request_body) = 'object'::text) AND (octet_length((request_body)::text) <= 4096))$expr$;
 if v_names is distinct from array['bpay_next_workbench_command_receipt_request_body_check']::text[] then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RECEIPT_REQUEST_CHECK_CHANGED';end if;
 select array_agg(c.conname::text order by c.conname) into v_names from pg_catalog.pg_constraint c
  where c.conrelid='private.bpay_next_workbench_command_receipt'::pg_catalog.regclass and c.contype='c'
   and pg_catalog.pg_get_expr(c.conbin,c.conrelid)=$expr$((jsonb_typeof(result) = 'object'::text) AND (octet_length((result)::text) <= 8192))$expr$;
 if v_names is distinct from array['bpay_next_workbench_command_receipt_result_check']::text[] then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RECEIPT_RESULT_CHECK_CHANGED';end if;
 if (select pg_catalog.pg_get_constraintdef(c.oid) from pg_catalog.pg_constraint c
   where c.conrelid='private.bpay_next_workbench_command_receipt'::pg_catalog.regclass
    and c.conname='bpay_next_workbench_retry_work_ck') is distinct from
  $expr$CHECK (((work_id IS NOT NULL) OR (NOT ((result ->> 'retry_kind'::text) IS DISTINCT FROM 'PREPARE'::text))))$expr$ then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_RECEIPT_RETRY_CHECK_CHANGED';end if;
end
$function$;

alter table private.bpay_next_workbench_command_receipt
 add column action text not null default 'RETRY_WORK_JOB'
  check(action in ('RETRY_WORK_JOB','RESOLUTION_SAVE','RESOLUTION_CLEAR')),
 alter column job_id drop not null,
 drop constraint bpay_next_workbench_command_receipt_request_body_check,
 drop constraint bpay_next_workbench_command_receipt_result_check,
 drop constraint bpay_next_workbench_retry_work_ck;

alter table private.bpay_next_workbench_command_receipt
 add constraint bpay_next_workbench_command_request_size_ck check(
  jsonb_typeof(request_body)='object' and octet_length(request_body::text)<=
   case when action='RETRY_WORK_JOB' then 4096 else 32768 end),
 add constraint bpay_next_workbench_command_result_size_ck check(
  jsonb_typeof(result)='object' and octet_length(result::text)<=
   case when action='RETRY_WORK_JOB' then 8192 else 65536 end),
 add constraint bpay_next_workbench_retry_work_ck check(action<>'RETRY_WORK_JOB'
  or(work_id is not null or(result->>'retry_kind') is not distinct from 'PREPARE')),
 add constraint bpay_next_workbench_command_variant_ck check((
  (action='RETRY_WORK_JOB' and job_id is not null)
  or(action in ('RESOLUTION_SAVE','RESOLUTION_CLEAR') and job_id is null
   and retry_run_worker_id is null and retry_input_job_id is null and retry_captured_revision is null
   and retry_expected_revision is null and retry_target_revision is null
   and result->>'action'=action
   and case when jsonb_typeof(request_body->'works')='array' and jsonb_typeof(result->'works')='array' then
    jsonb_array_length(request_body->'works') between 1 and 100
    and jsonb_array_length(result->'works')=jsonb_array_length(request_body->'works')
    and case when jsonb_array_length(request_body->'works')=1 then
     work_id is not null and request_body#>>'{works,0,work_id}'=work_id::text
      and result#>>'{works,0,work_id}'=work_id::text
     else work_id is null end
    else false end)
 ) is true);
-- Existing execution CK, PK/FKs, owner/RLS/policy and immutable triggers retained.

commit;
