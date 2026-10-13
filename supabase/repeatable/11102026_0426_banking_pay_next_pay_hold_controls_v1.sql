-- WB40 D08: only the two explicitly named native hold/currentness bindings.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_pay_hold_control_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_bookings text[];v_work_ids uuid[];v_candidate uuid;v_work private.bpay_next_work%rowtype;
 v_canonical record;v_held boolean;v_reason text;v_sheet uuid;v_financial uuid;
begin
 if (select active_owner from private.bpay_next_module_control where id=1) is distinct from 'NEXT' then return new;end if;
 if tg_table_schema<>'public' or tg_table_name not in ('timesheets','timesheets_financials') then
  raise exception using errcode='23514',message='BPAY_NEXT_PAY_HOLD_TRIGGER_RELATION_INVALID';end if;
 if tg_table_name='timesheets' then
  if tg_op='UPDATE' then
   if (old.is_current,old.booking_id,old.timesheet_id) is not distinct from (new.is_current,new.booking_id,new.timesheet_id)
     or not(old.is_current or new.is_current) then return new;end if;
   v_bookings:=array[old.booking_id,new.booking_id];
  else
   if new.is_current is not true then return new;end if;
   v_bookings:=array[new.booking_id];
  end if;
 else
  if tg_op='UPDATE' then
   if (old.is_current,old.timesheet_id,old.pay_on_hold,old.pay_on_hold_reason)
     is not distinct from (new.is_current,new.timesheet_id,new.pay_on_hold,new.pay_on_hold_reason)
     or not(old.is_current or new.is_current) then return new;end if;
   select array_agg(distinct t.booking_id) into v_bookings from public.timesheets t
    where t.is_current and t.timesheet_id=any(array[old.timesheet_id,new.timesheet_id]);
  else
   if new.is_current is not true then return new;end if;
   select array_agg(t.booking_id) into v_bookings from public.timesheets t
    where t.is_current and t.timesheet_id=new.timesheet_id;
  end if;
 end if;
 select array_agg(w.id order by w.id) into v_work_ids from private.bpay_next_work w where w.booking_id=any(v_bookings);
 if v_work_ids is null then return new;end if;
 -- Native mutation already owns its public row. No public lock is acquired
 -- after the established Candidate -> Work private order.
 for v_candidate in select distinct w.candidate_id from private.bpay_next_work w
   where w.id=any(v_work_ids) order by w.candidate_id loop
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
 end loop;
 perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
 for v_work in select w.* from private.bpay_next_work w where w.id=any(v_work_ids) order by w.id loop
  select t.timesheet_id,tf.id financial_id,tf.pay_on_hold,tf.pay_on_hold_reason into v_canonical
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_sheet:=v_canonical.timesheet_id;v_financial:=v_canonical.financial_id;
  v_held:=coalesce(v_canonical.pay_on_hold,false);
  v_reason:=case when v_held then v_canonical.pay_on_hold_reason end;
  if (v_work.pay_hold_physical_timesheet_id,v_work.pay_hold_financial_id,v_work.pay_hold,v_work.pay_hold_reason)
    is not distinct from (v_sheet,v_financial,v_held,v_reason) then continue;end if;
  update private.bpay_next_work set pay_hold_control_version=pay_hold_control_version+1,
    pay_hold_changed_at_utc=clock_timestamp(),pay_hold_physical_timesheet_id=v_sheet,pay_hold_financial_id=v_financial,
    pay_hold=v_held,pay_hold_reason=v_reason where id=v_work.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1 where candidate_id=v_work.candidate_id;
  -- The existing wrapper owns contribution replacement and ORIGIN/Processing.
  perform private.bpay_next_workbench_refresh_work_v1(v_work.id);
 end loop;
 return new;
end
$function$;
alter function private.bpay_next_pay_hold_control_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_pay_hold_control_trigger_v1() from public,anon,authenticated,service_role;
do $bindings$
begin
 if not exists(select 1 from pg_trigger where tgrelid='public.timesheets'::regclass and tgname='bpay_next_pay_hold_timesheet_control_v1') then
  create trigger bpay_next_pay_hold_timesheet_control_v1 after insert or update of is_current,booking_id,timesheet_id
   on public.timesheets for each row execute function private.bpay_next_pay_hold_control_trigger_v1();
 end if;
 if not exists(select 1 from pg_trigger where tgrelid='public.timesheets_financials'::regclass and tgname='bpay_next_pay_hold_financial_control_v1') then
  create trigger bpay_next_pay_hold_financial_control_v1 after insert or update of is_current,timesheet_id,pay_on_hold,pay_on_hold_reason
   on public.timesheets_financials for each row execute function private.bpay_next_pay_hold_control_trigger_v1();
 end if;
 if (select count(*) from pg_trigger where not tgisinternal and tgname in ('bpay_next_pay_hold_timesheet_control_v1','bpay_next_pay_hold_financial_control_v1')
   and tgfoid='private.bpay_next_pay_hold_control_trigger_v1()'::regprocedure
   and tgrelid in ('public.timesheets'::regclass,'public.timesheets_financials'::regclass) and tgenabled='O')<>2 then
  raise exception 'BPAY_NEXT_PAY_HOLD_BINDING_MISMATCH';end if;
end
$bindings$;

commit;
