-- WB23 review F1: preserve PF04's actual terminal cleanup under its root lock.
-- Installed2232 stays immutable. No cleanup or monetary owner replacement.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_selection_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if tg_op='DELETE' then
  perform 1 from private.bpay_next_pay_run where id=old.run_id and status in ('COMPLETE','CANCELLED') for update;
  if found then return old;end if;
 end if;
 raise exception using errcode='23514',message='BPAY_NEXT_SELECTION_PAGE_CONFLICT';
end
$function$;
alter function private.bpay_next_selection_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_selection_receipt_guard_v1() from public,anon,authenticated,service_role;
notify pgrst,'reload schema';

commit;
