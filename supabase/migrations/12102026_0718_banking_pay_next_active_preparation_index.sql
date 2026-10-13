-- DR07 / BD139: native agency-wide and actor-scoped newest-first active preparations.
\set ON_ERROR_STOP on
begin;
create index bpay_next_active_preparation_created_idx on private.bpay_next_pay_run(created_at_utc desc,id desc)
  where status in ('PREPARING','REVIEW');
create index bpay_next_active_preparation_actor_created_idx on private.bpay_next_pay_run(actor_user_id,created_at_utc desc,id desc)
  where status in ('PREPARING','REVIEW');
commit;
