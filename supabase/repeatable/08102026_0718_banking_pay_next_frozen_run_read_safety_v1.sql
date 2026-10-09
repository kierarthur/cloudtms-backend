-- PS-C09/F15 and PS-C10: frozen execution inputs and complete cancellation lookup.
-- Reuse the existing immutable effect/Case-event guard. Captured run_work and
-- run_line are built as complete INSERT-only records; workflow progress lives
-- on their worker, holds, requests and outcomes, never by repricing these rows.
-- No new table, function, ledger, calculator, permission or browser RPC.
\set ON_ERROR_STOP on
begin;

-- Exact-name idempotency must not overwrite a different attachment/index.
do $frozen_capture_preflight$
declare v record;
begin
  for v in select * from (values
    ('private.bpay_next_run_work','bpay_next_run_work_frozen_immutable_v1',27),
    ('private.bpay_next_run_work','bpay_next_run_work_frozen_no_truncate_v1',34),
    ('private.bpay_next_run_line','bpay_next_run_line_frozen_immutable_v1',27),
    ('private.bpay_next_run_line','bpay_next_run_line_frozen_no_truncate_v1',34)
  ) as expected(relation_name,trigger_name,trigger_type)
  loop
    if exists(select 1 from pg_catalog.pg_trigger t
      where t.tgrelid=v.relation_name::regclass and t.tgname=v.trigger_name
      and (t.tgfoid<>'private.bpay_next_effect_immutable_v1()'::regprocedure
        or t.tgtype<>v.trigger_type or t.tgenabled<>'O' or t.tgisinternal
        or t.tgnargs<>0 or t.tgattr::text<>'' or t.tgqual is not null
        or t.tgconstraint<>0 or t.tgdeferrable or t.tginitdeferred)) then
      raise exception using errcode='23514',message='BPAY_NEXT_FROZEN_CAPTURE_TRIGGER_CONFLICT';
    end if;
  end loop;
  if pg_catalog.to_regclass('private.bpay_next_cancel_request_worker_history_idx') is not null
     and not exists(select 1 from pg_catalog.pg_index i
       where i.indexrelid=pg_catalog.to_regclass('private.bpay_next_cancel_request_worker_history_idx')
       and i.indrelid='private.bpay_next_cancel_request'::regclass
       and i.indisvalid and i.indisready and not i.indisunique
       and i.indpred is null and i.indexprs is null
       and pg_catalog.pg_get_indexdef(i.indexrelid)=
         'CREATE INDEX bpay_next_cancel_request_worker_history_idx ON private.bpay_next_cancel_request USING btree (run_worker_id, command_id)') then
    raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_HISTORY_INDEX_CONFLICT';
  end if;
end
$frozen_capture_preflight$;

drop trigger if exists bpay_next_run_work_frozen_immutable_v1 on private.bpay_next_run_work;
create trigger bpay_next_run_work_frozen_immutable_v1
before update or delete on private.bpay_next_run_work
for each row execute function private.bpay_next_effect_immutable_v1();

drop trigger if exists bpay_next_run_work_frozen_no_truncate_v1 on private.bpay_next_run_work;
create trigger bpay_next_run_work_frozen_no_truncate_v1
before truncate on private.bpay_next_run_work
for each statement execute function private.bpay_next_effect_immutable_v1();

drop trigger if exists bpay_next_run_line_frozen_immutable_v1 on private.bpay_next_run_line;
create trigger bpay_next_run_line_frozen_immutable_v1
before update or delete on private.bpay_next_run_line
for each row execute function private.bpay_next_effect_immutable_v1();

drop trigger if exists bpay_next_run_line_frozen_no_truncate_v1 on private.bpay_next_run_line;
create trigger bpay_next_run_line_frozen_no_truncate_v1
before truncate on private.bpay_next_run_line
for each statement execute function private.bpay_next_effect_immutable_v1();

-- The existing unique pending-worker index remains the admission guard.
-- This separate non-unique lookup includes CANCELLED and BLOCKED history.
create index if not exists bpay_next_cancel_request_worker_history_idx
on private.bpay_next_cancel_request(run_worker_id,command_id);

commit;
